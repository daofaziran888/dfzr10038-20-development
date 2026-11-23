"""血缘下游重算全链路验收（端到端，仅依赖标准库）。

场景：同一次上游变动经左、右两个分支汇入汇合特征 D，
左支已换新版、右支仍为旧版（两支实际版本都有快照）。
调度员查询受波及节点 → 并发提交只保留一份有效计划 → 调度编排激活并派发任务 →
执行已放行任务 → 下游试读汇合结果（临时发布）→ 右支追赶完成 → 只交付一个齐版结果。

运行：cd backend && .venv/bin/python tests/test_lineage_recompute.py
"""
from __future__ import annotations

import json
import os
import subprocess
import sys
import threading
import time
import urllib.error
import urllib.request

BACKEND_DIR = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
sys.path.insert(0, BACKEND_DIR)

from app.services.lineage_recompute import (  # noqa: E402
    AUX_FEATURE,
    CONFLUENCE_FEATURE,
    LEFT_FEATURE,
    RIGHT_FEATURE,
    UPSTREAM_FEATURE,
)

PORT = 8765
BASE = f"http://127.0.0.1:{PORT}"

FAILURES: list[str] = []


def check(name: str, cond: bool, detail: str = "") -> None:
    print(f"{'PASS' if cond else 'FAIL'}  {name}" + ("" if cond else f"  -> {detail}"))
    if not cond:
        FAILURES.append(name)


def api(method: str, path: str, payload: dict | None = None) -> dict:
    data = None if payload is None else json.dumps(payload).encode("utf-8")
    req = urllib.request.Request(
        BASE + path, data=data, method=method,
        headers={"Content-Type": "application/json"},
    )
    with urllib.request.urlopen(req) as resp:
        return json.loads(resp.read().decode("utf-8"))


def wait_ready() -> None:
    for _ in range(100):
        try:
            if api("GET", "/api/health").get("ok"):
                return
        except Exception:
            time.sleep(0.1)
    raise RuntimeError("后端服务启动超时")


def main() -> int:
    proc = subprocess.Popen(
        [sys.executable, "-m", "uvicorn", "app.main:app",
         "--host", "127.0.0.1", "--port", str(PORT)],
        cwd=BACKEND_DIR,
        stdout=subprocess.DEVNULL,
        stderr=subprocess.DEVNULL,
    )
    try:
        wait_ready()
        run_scenario()
    finally:
        proc.terminate()
        proc.wait(timeout=10)

    print()
    if FAILURES:
        print(f"共 {len(FAILURES)} 项未通过：{FAILURES}")
        return 1
    print("全部验收项通过")
    return 0


def run_scenario() -> None:
    # ---------- 1. 查询下游重算数据：边界、废弃边排除、环路提示 ----------
    query = api("POST", "/api/lineage/recompute/query",
                {"inputs": {UPSTREAM_FEATURE: "v2"}})
    check("查询下游重算数据成功", query.get("ok") is True, json.dumps(query, ensure_ascii=False))
    boundary = query["boundary"]
    node_names = {n["feature"] for n in boundary["nodes"]}
    check("边界覆盖左右支、汇合与辅助特征",
          node_names == {LEFT_FEATURE, RIGHT_FEATURE, CONFLUENCE_FEATURE, AUX_FEATURE},
          str(node_names))
    confl = [c for c in boundary["confluences"] if c["feature"] == CONFLUENCE_FEATURE]
    check("汇合节点及其左右分支识别正确",
          bool(confl) and sorted(confl[0]["branches"]) == sorted([LEFT_FEATURE, RIGHT_FEATURE]),
          str(boundary["confluences"]))
    used = {(e["上游特征"], e["下游特征"]) for e in boundary["edges"]}
    check("已废弃直连边不参与检索",
          (UPSTREAM_FEATURE, CONFLUENCE_FEATURE) not in used
          and any(e["上游特征"] == UPSTREAM_FEATURE and e["下游特征"] == CONFLUENCE_FEATURE
                  for e in boundary["excluded_deprecated"]),
          json.dumps(boundary["excluded_deprecated"], ensure_ascii=False))
    check("环路提示实际闭合的关系且不无限扩展",
          len(boundary["cycles"]) == 1
          and boundary["cycles"][0]["closing_relation"] == f"{AUX_FEATURE}→{LEFT_FEATURE}"
          and boundary["cycles"][0]["path"][-1] == LEFT_FEATURE,
          json.dumps(boundary["cycles"], ensure_ascii=False))
    needs = {n["feature"]: n["needs_recompute"] for n in boundary["nodes"]}
    check("左支已是新版无需重算、右支旧版需要重算",
          needs.get(LEFT_FEATURE) is False and needs.get(RIGHT_FEATURE) is True,
          str(needs))

    bad = api("POST", "/api/lineage/recompute/query",
              {"inputs": {UPSTREAM_FEATURE: "v9"}})
    check("确认版本没有快照时拒绝作为起点", bad.get("ok") is False, str(bad))

    # ---------- 2. 两支实际版本都有快照（左新右旧） ----------
    v_left = api("GET", f"/api/lineage/versions/{LEFT_FEATURE}")
    v_right = api("GET", f"/api/lineage/versions/{RIGHT_FEATURE}")
    check("左支已换新版且有快照",
          v_left["current"] == "v2" and len(v_left["snapshots"]) >= 2, str(v_left))
    check("右支仍为旧版且有快照",
          v_right["current"] == "v1" and len(v_right["snapshots"]) >= 1, str(v_right))

    # ---------- 3. 并发提交：同一汇合节点只保留一份有效计划 ----------
    results: list[dict] = []
    barrier = threading.Barrier(2)

    def submit() -> None:
        barrier.wait()
        results.append(api("POST", "/api/lineage/recompute/plans",
                           {"inputs": {UPSTREAM_FEATURE: "v2"},
                            "confluence": CONFLUENCE_FEATURE}))

    threads = [threading.Thread(target=submit) for _ in range(2)]
    for t in threads:
        t.start()
    for t in threads:
        t.join()
    valid = [r for r in results if r.get("ok") and r.get("valid")]
    dup = [r for r in results if not r.get("ok")]
    check("并发提交只保留一份有效计划", len(valid) == 1 and len(dup) == 1,
          json.dumps(results, ensure_ascii=False))
    plan = valid[0]["plan"]
    check("有效计划标明关联的不同数据版本",
          plan["associated_versions"] == {LEFT_FEATURE: "v2", RIGHT_FEATURE: "v1"},
          str(plan["associated_versions"]))
    check("重复提交指向已存在的有效计划",
          bool(dup) and dup[0]["existing_plan"]["计划编号"] == plan["计划编号"],
          json.dumps(dup, ensure_ascii=False))
    plans = api("GET", f"/api/lineage/recompute/plans?confluence={CONFLUENCE_FEATURE}")["plans"]
    check("汇合节点仅一份有效计划在册",
          len([p for p in plans if p["valid"]]) == 1, str(len(plans)))
    tasks = {t["type"]: t for t in plan["tasks"]}
    check("派发实际重算和追赶任务",
          set(tasks) == {"重算", "追赶"}
          and tasks["追赶"]["feature"] == RIGHT_FEATURE
          and tasks["追赶"]["to_version"] == "v2",
          json.dumps(plan["tasks"], ensure_ascii=False))

    # ---------- 4. 调度编排：激活计划并派发任务 ----------
    created = api("POST", "/api/schedule_job", {"values": {
        "计划编号": "SCHE-9001", "任务类型": "血缘下游重算",
        "关联对象": plan["计划编号"], "Cron表达式": "0 2 * * *",
        "时区设置": "Asia/Shanghai", "依赖检查": "上游版本确认",
        "失败通知": "调度员", "下次执行": "2026-11-24",
    }})
    check("调度计划登记成功", created.get("ok") is True, str(created))
    sid = created["entry"]["id"]
    activated = api("POST", f"/api/schedule_job/{sid}/actions", {"action": "激活计划"})
    check("调度员激活计划成功", activated.get("ok") is True, str(activated))
    effect_names = [e["name"] for e in (activated.get("flow") or {}).get("effects", [])]
    check("调度编排按下游重算数据派发任务", "派发重算与追赶任务" in effect_names,
          str(effect_names))

    detail = api("GET", f"/api/lineage/recompute/plans/{plan['id']}")["plan"]
    check("重算计划进入执行中", detail["status"] == "执行中", detail["status"])
    task_by_type = {t["type"]: t for t in detail["tasks"]}
    check("重算任务已放行", task_by_type["重算"]["status"] == "已放行",
          str(task_by_type["重算"]))
    check("追赶任务保持可领",
          task_by_type["追赶"]["status"] == "可领取" and task_by_type["追赶"]["claimable"],
          str(task_by_type["追赶"]))

    waiting = api("GET", f"/api/lineage/confluence/{CONFLUENCE_FEATURE}/read")
    check("执行前汇合结果等待齐版，前一次齐版交付仍可读取",
          waiting["state"] == "等待齐版"
          and waiting["delivered"]["versions"] == {LEFT_FEATURE: "v1", RIGHT_FEATURE: "v1"},
          json.dumps(waiting, ensure_ascii=False))

    # ---------- 5. 执行所有已放行的任务，下游试读汇合节点新结果 ----------
    executed = api("POST", f"/api/lineage/recompute/plans/{plan['id']}/execute")
    check("已放行任务执行完成",
          executed.get("ok") is True
          and len(executed["executed"]) == 1
          and all(t["status"] == "已完成" for t in executed["executed"]),
          json.dumps(executed, ensure_ascii=False))
    check("未完成的追赶任务保持可领",
          len(executed["skipped_catchup"]) == 1
          and executed["skipped_catchup"][0]["status"] == "可领取",
          json.dumps(executed["skipped_catchup"], ensure_ascii=False))

    read1 = api("GET", f"/api/lineage/confluence/{CONFLUENCE_FEATURE}/read")
    prov = read1["provisional"]
    check("汇合结果临时发布", read1["state"] == "临时发布", read1["state"])
    check("临时结果注明来源（左新右旧）",
          prov["versions"] == {LEFT_FEATURE: "v2", RIGHT_FEATURE: "v1"}
          and any(p["branch"] == RIGHT_FEATURE and p["state"] == "旧版"
                  for p in prov["provenance"]),
          json.dumps(prov, ensure_ascii=False))
    check("临时结果记录实际开放时间", bool(prov.get("open_time")), str(prov))
    check("前一次齐版交付仍可继续读取",
          read1["delivered"]["versions"] == {LEFT_FEATURE: "v1", RIGHT_FEATURE: "v1"},
          json.dumps(read1["delivered"], ensure_ascii=False))

    # ---------- 6. 临时结果不得把右支误记为完成 ----------
    detail2 = api("GET", f"/api/lineage/recompute/plans/{plan['id']}")["plan"]
    catchup = [t for t in detail2["tasks"] if t["type"] == "追赶"][0]
    check("有临时结果时右支任务未被误记为完成", catchup["status"] == "可领取",
          str(catchup))
    check("右支实际版本仍为旧版",
          api("GET", f"/api/lineage/versions/{RIGHT_FEATURE}")["current"] == "v1", "")

    # ---------- 7. 右支追赶：领取并完成，只交付一个齐版结果 ----------
    tid = catchup["id"]
    claimed = api("POST", f"/api/lineage/recompute/tasks/{tid}/claim",
                  {"worker": "调度员A"})
    check("追赶任务可领取", claimed.get("ok") is True
          and claimed["task"]["status"] == "已领取", str(claimed))
    done = api("POST", f"/api/lineage/recompute/tasks/{tid}/complete")
    check("追赶任务完成且右支追平新版",
          done.get("ok") is True
          and api("GET", f"/api/lineage/versions/{RIGHT_FEATURE}")["current"] == "v2",
          json.dumps(done, ensure_ascii=False))
    check("右支旧版快照仍可复查",
          len(api("GET", f"/api/lineage/versions/{RIGHT_FEATURE}")["snapshots"]) >= 2, "")

    read2 = api("GET", f"/api/lineage/confluence/{CONFLUENCE_FEATURE}/read")
    delivered = read2["delivered"]
    check("右支就绪后状态为已交付", read2["state"] == "已交付", read2["state"])
    check("齐版结果采用两支新版",
          delivered["versions"] == {LEFT_FEATURE: "v2", RIGHT_FEATURE: "v2"},
          json.dumps(delivered, ensure_ascii=False))
    check("齐版结果实际开放时间不早于临时结果",
          delivered["open_time"] >= prov["open_time"] and delivered["seq"] > prov["seq"],
          f"delivered={delivered['open_time']} provisional={prov['open_time']}")
    check("临时结果已被齐版交付取代", read2["provisional"] is None,
          json.dumps(read2, ensure_ascii=False))

    again = api("POST", f"/api/lineage/recompute/tasks/{tid}/complete")
    check("重复完成追赶任务被拒绝", again.get("ok") is False, str(again))
    api("POST", f"/api/lineage/recompute/plans/{plan['id']}/execute")
    read3 = api("GET", f"/api/lineage/confluence/{CONFLUENCE_FEATURE}/read")
    check("只交付一个齐版结果",
          read3["delivered"]["seq"] == delivered["seq"]
          and read3["delivered"]["open_time"] == delivered["open_time"],
          json.dumps(read3["delivered"], ensure_ascii=False))

    final = api("GET", f"/api/lineage/recompute/plans/{plan['id']}")["plan"]
    check("计划全部完成",
          final["status"] == "已完成"
          and all(t["status"] == "已完成" for t in final["tasks"]),
          final["status"])


if __name__ == "__main__":
    sys.exit(main())
