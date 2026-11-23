"""特征血缘下游重算领域服务。

在已确认的血缘边集合上检索受波及的下游节点，给出需要重算的边界；
围绕汇合节点登记唯一有效的重算计划，派发实际重算和追赶任务；
管理汇合结果的可用状态（等待齐版 / 临时发布 / 已交付），
保证最终结果两支齐版、同一对齐只交付一个齐版结果，
临时结果不得把未完成的分支误记为完成。
"""
from __future__ import annotations

import datetime
import itertools
import threading
from typing import Any

from app.store import store

# 血缘边状态：只有已确认边参与检索，已废弃边必须排除
EDGE_CONFIRMED = "已确认"
EDGE_DEPRECATED = "已废弃"

# 重算计划状态
PLAN_PENDING = "待激活"
PLAN_ACTIVE = "执行中"
PLAN_DONE = "已完成"

# 任务类型与状态
TASK_RECOMPUTE = "重算"
TASK_CATCHUP = "追赶"
TASK_WAITING = "待放行"
TASK_RELEASED = "已放行"
TASK_CLAIMABLE = "可领取"
TASK_CLAIMED = "已领取"
TASK_DONE = "已完成"

# 汇合结果可用状态
RESULT_WAITING = "等待齐版"
RESULT_PROVISIONAL = "临时发布"
RESULT_DELIVERED = "已交付"
RESULT_EMPTY = "无数据"

# 演示场景（钻石形血缘）使用的特征名
UPSTREAM_FEATURE = "user_click_cnt_1d"
LEFT_FEATURE = "user_click_smooth_7d"
RIGHT_FEATURE = "user_click_burst_7d"
CONFLUENCE_FEATURE = "user_click_score_7d"
AUX_FEATURE = "user_click_aux_7d"


def _now() -> str:
    return datetime.datetime.now().isoformat()


class VersionStore:
    """特征数据版本与快照：每个特征保留实际当前版本和全部快照，旧版可复查。"""

    def __init__(self) -> None:
        self._records: dict[str, dict[str, Any]] = {}
        self._lock = threading.Lock()

    def register(
        self, feature: str, version: str, *, source: str, at: str | None = None
    ) -> dict[str, Any]:
        with self._lock:
            record = self._records.setdefault(
                feature, {"feature": feature, "current": None, "snapshots": []}
            )
            snapshot = {
                "seq": len(record["snapshots"]) + 1,
                "feature": feature,
                "version": version,
                "source": source,
                "snapshot_at": at or _now(),
            }
            record["snapshots"].append(snapshot)
            record["current"] = version
            return snapshot

    def current(self, feature: str) -> str | None:
        record = self._records.get(feature)
        return None if record is None else record["current"]

    def has_version(self, feature: str, version: str) -> bool:
        record = self._records.get(feature)
        return bool(record) and any(s["version"] == version for s in record["snapshots"])

    def detail(self, feature: str) -> dict[str, Any] | None:
        record = self._records.get(feature)
        if record is None:
            return None
        return {
            "feature": feature,
            "current": record["current"],
            "snapshots": list(record["snapshots"]),
        }


class ConfluenceResultStore:
    """汇合特征结果：前一次齐版交付 + 本次临时/齐版结果，以及被取代的历史。"""

    def __init__(self) -> None:
        self._results: dict[str, dict[str, Any]] = {}
        self._lock = threading.Lock()
        self._seq = itertools.count(1)

    def _entry(self, feature: str) -> dict[str, Any]:
        return self._results.setdefault(
            feature,
            {
                "feature": feature,
                "pending_plan": None,  # 进行中的计划编号 → 等待齐版
                "provisional": None,  # 本次临时结果（注明来源）
                "delivered": None,  # 当前生效的齐版交付
                "history": [],  # 被取代的结果
            },
        )

    @staticmethod
    def _alignment_key(plan_no: str | None, versions: dict[str, str]) -> str:
        body = ",".join(f"{k}@{v}" for k, v in sorted(versions.items()))
        return f"{plan_no or 'seed'}:{body}"

    def seed_delivered(
        self, feature: str, *, versions: dict[str, str], open_time: str, note: str
    ) -> None:
        with self._lock:
            entry = self._entry(feature)
            entry["delivered"] = {
                "seq": next(self._seq),
                "plan": None,
                "kind": "齐版交付",
                "alignment_key": self._alignment_key(None, versions),
                "versions": dict(versions),
                "open_time": open_time,
                "note": note,
            }

    def mark_pending(self, feature: str, plan_no: str) -> None:
        with self._lock:
            self._entry(feature)["pending_plan"] = plan_no

    def publish_provisional(
        self,
        feature: str,
        *,
        plan_no: str,
        versions: dict[str, str],
        provenance: list[dict[str, Any]],
    ) -> dict[str, Any]:
        """发布注明来源的临时结果；不触碰分支任务与版本，未完成分支保持可领。"""
        with self._lock:
            entry = self._entry(feature)
            result = {
                "seq": next(self._seq),
                "plan": plan_no,
                "kind": "临时结果",
                "versions": dict(versions),
                "provenance": list(provenance),
                "open_time": _now(),
                "note": "临时结果，注明来源；齐版交付后自动取代",
            }
            if entry["provisional"] is not None:
                entry["history"].append({**entry["provisional"], "closed": "被新临时结果取代"})
            entry["provisional"] = result
            entry["pending_plan"] = None
            return result

    def deliver(
        self, feature: str, *, plan_no: str, versions: dict[str, str]
    ) -> tuple[dict[str, Any], bool]:
        """交付齐版结果；同一对齐只交付一个齐版结果，返回 (结果, 是否新建)。"""
        with self._lock:
            entry = self._entry(feature)
            alignment_key = self._alignment_key(plan_no, versions)
            if entry["delivered"] and entry["delivered"].get("alignment_key") == alignment_key:
                return entry["delivered"], False
            result = {
                "seq": next(self._seq),
                "plan": plan_no,
                "kind": "齐版交付",
                "alignment_key": alignment_key,
                "versions": dict(versions),
                "open_time": _now(),
            }
            if entry["delivered"] is not None:
                entry["history"].append({**entry["delivered"], "closed": "被新齐版交付取代"})
            if entry["provisional"] is not None:
                entry["history"].append({**entry["provisional"], "closed": "齐版交付后取代临时结果"})
                entry["provisional"] = None
            entry["delivered"] = result
            entry["pending_plan"] = None
            return result, True

    def read(self, feature: str) -> dict[str, Any] | None:
        """下游读取视图：按本次结果的可用状态区分临时发布/等待齐版/已交付。"""
        with self._lock:
            entry = self._results.get(feature)
            if entry is None:
                return None
            if entry["provisional"] is not None:
                state = RESULT_PROVISIONAL
                message = "本次结果为临时发布（已注明来源），前一次齐版交付仍可继续读取"
            elif entry["pending_plan"] is not None:
                state = RESULT_WAITING
                message = "重算计划进行中，本次结果等待齐版；可继续读取前一次齐版交付"
            elif entry["delivered"] is not None:
                state = RESULT_DELIVERED
                message = "两支齐版，已交付唯一齐版结果"
            else:
                state = RESULT_EMPTY
                message = "暂无可用结果"
            return {
                "feature": feature,
                "state": state,
                "provisional": entry["provisional"],
                "delivered": entry["delivered"],
                "pending_plan": entry["pending_plan"],
                "history": list(entry["history"]),
                "message": message,
            }


class LineageRecomputeService:
    """下游重算编排：检索边界、登记计划、放行任务、管理汇合结果。"""

    def __init__(self) -> None:
        self.versions = VersionStore()
        self.results = ConfluenceResultStore()
        self._plans: list[dict[str, Any]] = []
        self._tasks: list[dict[str, Any]] = []
        self._lock = threading.RLock()
        self._plan_seq = itertools.count(1)
        self._task_seq = itertools.count(1)
        self.seed_demo_state()

    # ------------------------------------------------------------------
    # 演示数据：钻石形血缘的版本快照（左支已换新版、右支仍为旧版）
    # ------------------------------------------------------------------

    def seed_demo_state(self) -> None:
        self.versions.register(UPSTREAM_FEATURE, "v1", source="初始交付", at="2026-11-20T09:00:00")
        self.versions.register(UPSTREAM_FEATURE, "v2", source="上游变更确认", at="2026-11-22T09:00:00")
        self.versions.register(LEFT_FEATURE, "v1", source="初始交付", at="2026-11-20T09:30:00")
        self.versions.register(LEFT_FEATURE, "v2", source="左支重算完成", at="2026-11-22T10:00:00")
        self.versions.register(RIGHT_FEATURE, "v1", source="初始交付", at="2026-11-20T09:40:00")
        self.versions.register(CONFLUENCE_FEATURE, "v1", source="前一次齐版交付", at="2026-11-20T11:00:00")
        self.results.seed_delivered(
            CONFLUENCE_FEATURE,
            versions={LEFT_FEATURE: "v1", RIGHT_FEATURE: "v1"},
            open_time="2026-11-20T11:00:00",
            note="前一次齐版交付",
        )

    # ------------------------------------------------------------------
    # 检索：在已确认血缘边上给出需要重算的边界
    # ------------------------------------------------------------------

    def query_downstream(self, inputs: dict[str, str]) -> dict[str, Any]:
        inputs = {str(f): str(v) for f, v in (inputs or {}).items() if str(f).strip() and str(v).strip()}
        if not inputs:
            return {"ok": False, "message": "请选择至少一个输入特征及其确认版本", "boundary": None}
        unknown = [f"{f}@{v}" for f, v in inputs.items() if not self.versions.has_version(f, v)]
        if unknown:
            return {
                "ok": False,
                "message": f"输入特征的确认版本没有快照，无法作为重算起点：{'、'.join(unknown)}",
                "boundary": None,
            }

        rows = store.rows("feature_line")
        confirmed = [r for r in rows if r.get("status") == EDGE_CONFIRMED]
        deprecated = [r for r in rows if r.get("status") == EDGE_DEPRECATED]
        adj: dict[str, list[dict[str, Any]]] = {}
        for row in confirmed:
            adj.setdefault(str(row.get("上游特征", "")), []).append(row)

        visited: dict[str, int] = {}
        used_edges: list[dict[str, Any]] = []
        cycles: list[dict[str, Any]] = []
        seen_cycle_keys: set[tuple[str, str]] = set()

        def walk(feature: str, depth: int, path: list[str]) -> None:
            for row in adj.get(feature, []):
                down = str(row.get("下游特征", ""))
                edge_info = {
                    "link_id": row.get("id"),
                    "链路编号": row.get("链路编号"),
                    "上游特征": feature,
                    "下游特征": down,
                }
                if down in path:
                    # 环路：提示实际闭合的关系，不再继续扩展
                    key = (feature, down)
                    if key not in seen_cycle_keys:
                        seen_cycle_keys.add(key)
                        loop = path + [down]
                        cycles.append({
                            **edge_info,
                            "closing_relation": f"{feature}→{down}",
                            "path": loop,
                            "message": (
                                f"检测到环路：{'→'.join(loop)}，"
                                f"实际闭合关系为 {feature}→{down}，已停止沿该边扩展"
                            ),
                        })
                    continue
                used_edges.append(edge_info)
                if down not in visited:
                    visited[down] = depth + 1
                    walk(down, depth + 1, path + [down])

        for feat in inputs:
            visited.setdefault(feat, 0)
        for feat in inputs:
            walk(feat, 0, [feat])

        versions = {v for v in inputs.values()}
        target_version = versions.pop() if len(versions) == 1 else None

        nodes = [
            {
                "feature": feat,
                "depth": depth,
                "current_version": self.versions.current(feat),
                "needs_recompute": (
                    None if target_version is None
                    else self.versions.current(feat) != target_version
                ),
            }
            for feat, depth in sorted(visited.items(), key=lambda kv: (kv[1], kv[0]))
            if feat not in inputs
        ]

        indegree: dict[str, set[str]] = {}
        for edge in used_edges:
            indegree.setdefault(edge["下游特征"], set()).add(edge["上游特征"])
        confluences = [
            {"feature": node, "branches": sorted(ups)}
            for node, ups in sorted(indegree.items())
            if len(ups) >= 2
        ]

        excluded = [
            {
                "link_id": r.get("id"),
                "链路编号": r.get("链路编号"),
                "上游特征": r.get("上游特征"),
                "下游特征": r.get("下游特征"),
                "原因": "已废弃边不参与检索",
            }
            for r in deprecated
            if str(r.get("上游特征", "")) in visited
        ]

        return {
            "ok": True,
            "message": (
                f"从 {len(inputs)} 个输入特征出发，在 {len(confirmed)} 条已确认血缘边上检索，"
                f"波及 {len(nodes)} 个下游节点、{len(confluences)} 个汇合节点、"
                f"{len(cycles)} 处环路、排除 {len(excluded)} 条已废弃边"
            ),
            "boundary": {
                "inputs": inputs,
                "target_version": target_version,
                "nodes": nodes,
                "edges": used_edges,
                "confluences": confluences,
                "cycles": cycles,
                "excluded_deprecated": excluded,
                "confirmed_edge_total": len(confirmed),
            },
        }

    # ------------------------------------------------------------------
    # 计划：同一汇合节点只保留一份有效计划
    # ------------------------------------------------------------------

    def submit_plan(
        self,
        inputs: dict[str, str],
        *,
        confluence: str | None = None,
        target_version: str | None = None,
    ) -> dict[str, Any]:
        query = self.query_downstream(inputs)
        if not query["ok"]:
            return {"ok": False, "valid": False, "plan": None, "message": query["message"]}
        boundary = query["boundary"]
        target = target_version or boundary["target_version"]
        if not target:
            return {
                "ok": False,
                "valid": False,
                "plan": None,
                "message": "输入特征的确认版本不一致，请显式指定对齐目标版本",
            }
        confluences = boundary["confluences"]
        if confluence is None:
            if not confluences:
                return {
                    "ok": False,
                    "valid": False,
                    "plan": None,
                    "message": "波及范围内没有汇合节点，无需编排齐版重算",
                }
            confluence = confluences[0]["feature"]
        hit = next((c for c in confluences if c["feature"] == confluence), None)
        if hit is None:
            return {
                "ok": False,
                "valid": False,
                "plan": None,
                "message": f"{confluence} 不在本次波及的汇合节点内",
            }
        branches = hit["branches"]
        associated = {b: self.versions.current(b) for b in branches}

        with self._lock:
            existing = self._valid_plan_for(confluence)
            if existing is not None:
                return {
                    "ok": False,
                    "valid": False,
                    "plan": None,
                    "existing_plan": self._plan_view(existing),
                    "message": (
                        f"汇合节点 {confluence} 已存在有效重算计划 {existing['计划编号']}，"
                        f"本次提交不生效；该计划关联数据版本 {existing['associated_versions']}"
                    ),
                }
            plan_no = f"RCPLAN-{next(self._plan_seq):04d}"
            plan: dict[str, Any] = {
                "id": len(self._plans) + 1,
                "计划编号": plan_no,
                "confluence": confluence,
                "inputs": dict(inputs),
                "target_version": target,
                "associated_versions": associated,
                "boundary": boundary["nodes"],
                "cycles": boundary["cycles"],
                "branches": [
                    {
                        "feature": b,
                        "current_version": associated[b],
                        "aligned": associated[b] == target,
                    }
                    for b in branches
                ],
                "status": PLAN_PENDING,
                "valid": True,
                "created_at": _now(),
                "activated_at": None,
                "activated_by": None,
                "finished_at": None,
            }
            self._plans.append(plan)
            tasks = []
            for branch in branches:
                if associated[branch] != target:
                    tasks.append(self._new_task(
                        plan, TASK_CATCHUP, branch,
                        from_version=associated[branch], to_version=target,
                    ))
            tasks.append(self._new_task(
                plan, TASK_RECOMPUTE, confluence,
                from_version=self.versions.current(confluence), to_version=target,
            ))
            plan["tasks"] = [t["id"] for t in tasks]
            return {
                "ok": True,
                "valid": True,
                "plan": self._plan_view(plan),
                "message": (
                    f"已为汇合节点 {confluence} 登记重算计划 {plan_no}，"
                    f"关联数据版本 {associated}，派发任务 {len(tasks)} 个"
                ),
            }

    def list_plans(self, confluence: str | None = None) -> list[dict[str, Any]]:
        with self._lock:
            plans = self._plans
            if confluence:
                plans = [p for p in plans if p["confluence"] == confluence]
            return [self._plan_view(p) for p in plans]

    def get_plan(self, plan_id: int) -> dict[str, Any] | None:
        with self._lock:
            plan = self._find_plan(plan_id)
            return None if plan is None else self._plan_view(plan)

    # ------------------------------------------------------------------
    # 激活与执行：调度编排按下游重算数据派发任务
    # ------------------------------------------------------------------

    def activate_plan(self, plan_id: int, *, by: str = "调度员") -> tuple[dict[str, Any] | None, str]:
        with self._lock:
            plan = self._find_plan(plan_id)
            if plan is None:
                return None, f"重算计划 {plan_id} 不存在"
            if plan["status"] != PLAN_PENDING:
                return None, f"重算计划 {plan['计划编号']} 当前状态 {plan['status']}，不能重复激活"
            plan["status"] = PLAN_ACTIVE
            plan["activated_at"] = _now()
            plan["activated_by"] = by
            released: list[str] = []
            claimable: list[str] = []
            for task in self._plan_tasks(plan["id"]):
                task["released_at"] = _now()
                if task["type"] == TASK_RECOMPUTE:
                    task["status"] = TASK_RELEASED
                    released.append(task["任务编号"])
                else:
                    # 追赶任务保持可领，由执行方认领后完成
                    task["status"] = TASK_CLAIMABLE
                    task["claimable"] = True
                    claimable.append(task["任务编号"])
            self.results.mark_pending(plan["confluence"], plan["计划编号"])
            return plan, (
                f"计划 {plan['计划编号']} 已激活：放行重算任务 {released or '无'}，"
                f"追赶任务可领取 {claimable or '无'}"
            )

    def activate_by_schedule(self, ref: str) -> tuple[dict[str, Any] | None, str]:
        """调度编排入口：按计划编号或汇合特征匹配待激活的重算计划。"""
        with self._lock:
            pending = [p for p in self._plans if p["valid"] and p["status"] == PLAN_PENDING]
        if not pending:
            return None, "没有待激活的重算计划"
        chosen = None
        for plan in pending:
            if ref and (plan["计划编号"] in ref or plan["confluence"] in ref):
                chosen = plan
                break
        if chosen is None and len(pending) == 1:
            chosen = pending[0]
        if chosen is None:
            return None, "调度计划未指明要激活的重算计划"
        return self.activate_plan(chosen["id"], by=f"调度编排/{ref or '未指明'}")

    def execute_released(self, plan_id: int) -> dict[str, Any]:
        """执行所有已放行的重算任务；追赶任务需认领后执行，保持可领。"""
        with self._lock:
            plan = self._find_plan(plan_id)
            if plan is None:
                return {"ok": False, "message": f"重算计划 {plan_id} 不存在"}
            if plan["status"] != PLAN_ACTIVE:
                return {"ok": False, "message": f"计划 {plan['计划编号']} 当前状态 {plan['status']}，请先激活"}
            executed: list[dict[str, Any]] = []
            skipped: list[dict[str, Any]] = []
            for task in self._plan_tasks(plan["id"]):
                if task["type"] == TASK_RECOMPUTE and task["status"] == TASK_RELEASED:
                    self._execute_recompute(plan, task)
                    executed.append(dict(task))
                elif task["status"] in (TASK_CLAIMABLE, TASK_CLAIMED):
                    skipped.append({**dict(task), "原因": "追赶任务需领取后执行，保持可领"})
            self._maybe_finish_plan(plan)
            return {
                "ok": True,
                "executed": executed,
                "skipped_catchup": skipped,
                "read": self.results.read(plan["confluence"]),
                "message": (
                    f"已执行放行任务 {len(executed)} 个；"
                    f"追赶任务 {len(skipped)} 个保持可领"
                ),
            }

    # ------------------------------------------------------------------
    # 追赶任务：领取与完成
    # ------------------------------------------------------------------

    def claim_task(self, task_id: int, worker: str) -> tuple[dict[str, Any] | None, str]:
        with self._lock:
            task = self._find_task(task_id)
            if task is None:
                return None, f"任务 {task_id} 不存在"
            if task["type"] != TASK_CATCHUP:
                return None, f"任务 {task['任务编号']} 是{task['type']}任务，无需领取"
            if task["status"] != TASK_CLAIMABLE:
                return None, f"任务 {task['任务编号']} 当前状态 {task['status']}，不可领取"
            task["status"] = TASK_CLAIMED
            task["claimable"] = False
            task["claimed_by"] = worker
            return dict(task), f"{worker} 已领取追赶任务 {task['任务编号']}"

    def complete_task(self, task_id: int) -> dict[str, Any]:
        with self._lock:
            task = self._find_task(task_id)
            if task is None:
                return {"ok": False, "message": f"任务 {task_id} 不存在"}
            if task["status"] != TASK_CLAIMED:
                return {"ok": False, "message": f"任务 {task['任务编号']} 当前状态 {task['status']}，需先领取"}
            plan = self._find_plan(task["plan_id"])
            assert plan is not None
            # 分支版本追平并留快照，旧版仍可复查
            self.versions.register(
                task["feature"], task["to_version"],
                source=f"追赶完成/{plan['计划编号']}",
            )
            task["status"] = TASK_DONE
            task["finished_at"] = _now()
            for branch in plan["branches"]:
                if branch["feature"] == task["feature"]:
                    branch["current_version"] = task["to_version"]
                    branch["aligned"] = True
            delivered = None
            if all(b["aligned"] for b in plan["branches"]):
                adopted = {b["feature"]: self.versions.current(b["feature"]) for b in plan["branches"]}
                result, created = self._deliver_aligned(plan, adopted)
                delivered = {"created": created, "result": result}
            self._maybe_finish_plan(plan)
            return {
                "ok": True,
                "task": dict(task),
                "delivered": delivered,
                "read": self.results.read(plan["confluence"]),
                "message": (
                    f"追赶任务 {task['任务编号']} 完成，{task['feature']} 已追平 {task['to_version']}"
                    + ("，两支齐版已交付" if delivered and delivered["created"] else "")
                ),
            }

    # ------------------------------------------------------------------
    # 读取：下游按可用状态读取汇合特征
    # ------------------------------------------------------------------

    def read_confluence(self, feature: str) -> dict[str, Any] | None:
        return self.results.read(feature)

    def version_detail(self, feature: str) -> dict[str, Any] | None:
        return self.versions.detail(feature)

    # ------------------------------------------------------------------
    # 内部工具
    # ------------------------------------------------------------------

    def _new_task(
        self,
        plan: dict[str, Any],
        task_type: str,
        feature: str,
        *,
        from_version: str | None,
        to_version: str,
    ) -> dict[str, Any]:
        task_id = next(self._task_seq)
        task = {
            "id": task_id,
            "任务编号": f"RCTASK-{task_id:04d}",
            "plan_id": plan["id"],
            "计划编号": plan["计划编号"],
            "type": task_type,
            "feature": feature,
            "from_version": from_version,
            "to_version": to_version,
            "status": TASK_WAITING,
            "claimable": False,
            "claimed_by": None,
            "released_at": None,
            "finished_at": None,
        }
        self._tasks.append(task)
        return task

    def _execute_recompute(self, plan: dict[str, Any], task: dict[str, Any]) -> None:
        confluence = plan["confluence"]
        target = plan["target_version"]
        branches = [b["feature"] for b in plan["branches"]]
        adopted = {b: self.versions.current(b) for b in branches}
        if all(v == target for v in adopted.values()):
            _, created = self._deliver_aligned(plan, adopted)
            task["note"] = "两支齐版，交付齐版结果" if created else "齐版结果已交付，不重复交付"
        else:
            provenance = [
                {
                    "branch": b,
                    "version": adopted[b],
                    "state": "新版" if adopted[b] == target else "旧版",
                    "note": "" if adopted[b] == target else "追赶任务未完成，仍为可复查旧版",
                }
                for b in branches
            ]
            self.results.publish_provisional(
                confluence, plan_no=plan["计划编号"],
                versions=adopted, provenance=provenance,
            )
            task["note"] = "分支未齐版，发布注明来源的临时结果"
        task["status"] = TASK_DONE
        task["finished_at"] = _now()
        task["adopted_versions"] = adopted

    def _deliver_aligned(
        self, plan: dict[str, Any], adopted: dict[str, str]
    ) -> tuple[dict[str, Any], bool]:
        result, created = self.results.deliver(
            plan["confluence"], plan_no=plan["计划编号"], versions=adopted
        )
        if created:
            self.versions.register(
                plan["confluence"], plan["target_version"],
                source=f"齐版交付/{plan['计划编号']}",
            )
        return result, created

    def _maybe_finish_plan(self, plan: dict[str, Any]) -> None:
        if plan["status"] != PLAN_ACTIVE:
            return
        tasks = self._plan_tasks(plan["id"])
        if not all(t["status"] == TASK_DONE for t in tasks):
            return
        read = self.results.read(plan["confluence"])
        delivered = (read or {}).get("delivered")
        if delivered and all(
            v == plan["target_version"] for v in delivered["versions"].values()
        ):
            plan["status"] = PLAN_DONE
            plan["finished_at"] = _now()

    def _valid_plan_for(self, confluence: str) -> dict[str, Any] | None:
        for plan in self._plans:
            if (
                plan["valid"]
                and plan["confluence"] == confluence
                and plan["status"] in (PLAN_PENDING, PLAN_ACTIVE)
            ):
                return plan
        return None

    def _find_plan(self, plan_id: int) -> dict[str, Any] | None:
        for plan in self._plans:
            if plan["id"] == plan_id:
                return plan
        return None

    def _find_task(self, task_id: int) -> dict[str, Any] | None:
        for task in self._tasks:
            if task["id"] == task_id:
                return task
        return None

    def _plan_tasks(self, plan_id: int) -> list[dict[str, Any]]:
        return [t for t in self._tasks if t["plan_id"] == plan_id]

    def _plan_view(self, plan: dict[str, Any]) -> dict[str, Any]:
        view = dict(plan)
        view["tasks"] = [dict(t) for t in self._plan_tasks(plan["id"])]
        return view


recompute_service = LineageRecomputeService()
