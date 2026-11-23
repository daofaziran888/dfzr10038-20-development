"""特征血缘下游重算入口：调度员查询受波及节点、提交重算计划、派发与执行、下游读取汇合结果。"""
from __future__ import annotations

from typing import Any

from fastapi import APIRouter, HTTPException
from pydantic import BaseModel, Field

from app.services.lineage_recompute import recompute_service

router = APIRouter(prefix="/api/lineage", tags=["血缘下游重算"])


class RecomputeQueryPayload(BaseModel):
    """选中的输入特征及其确认版本：{特征名: 版本}。"""

    inputs: dict[str, str] = Field(default_factory=dict)


class PlanSubmitPayload(BaseModel):
    inputs: dict[str, str] = Field(default_factory=dict)
    confluence: str | None = None
    target_version: str | None = None


class TaskClaimPayload(BaseModel):
    worker: str = "调度员"


@router.post("/recompute/query")
def query_recompute(payload: RecomputeQueryPayload) -> dict[str, Any]:
    """在已确认血缘边集合上查询下游重算数据，给出需要重算的边界。

    已废弃边不参与检索；遇到环路时提示实际闭合的关系并停止扩展。
    """
    return recompute_service.query_downstream(payload.inputs)


@router.post("/recompute/plans")
def submit_plan(payload: PlanSubmitPayload) -> dict[str, Any]:
    """提交下游重算计划；同一汇合节点只保留一份有效计划，并标明关联的不同数据版本。"""
    return recompute_service.submit_plan(
        payload.inputs,
        confluence=payload.confluence,
        target_version=payload.target_version,
    )


@router.get("/recompute/plans")
def list_plans(confluence: str | None = None) -> dict[str, Any]:
    return {"plans": recompute_service.list_plans(confluence)}


@router.get("/recompute/plans/{plan_id}")
def get_plan(plan_id: int) -> dict[str, Any]:
    plan = recompute_service.get_plan(plan_id)
    if plan is None:
        raise HTTPException(status_code=404, detail=f"重算计划 {plan_id} 不存在")
    return {"ok": True, "plan": plan}


@router.post("/recompute/plans/{plan_id}/activate")
def activate_plan(plan_id: int) -> dict[str, Any]:
    """激活计划：放行重算任务，追赶任务保持可领。"""
    plan, message = recompute_service.activate_plan(plan_id)
    if plan is None:
        return {"ok": False, "message": message}
    return {"ok": True, "message": message, "plan": recompute_service.get_plan(plan_id)}


@router.post("/recompute/plans/{plan_id}/execute")
def execute_released(plan_id: int) -> dict[str, Any]:
    """执行所有已放行的重算任务；未完成的追赶任务保持可领。"""
    return recompute_service.execute_released(plan_id)


@router.post("/recompute/tasks/{task_id}/claim")
def claim_task(task_id: int, payload: TaskClaimPayload) -> dict[str, Any]:
    task, message = recompute_service.claim_task(task_id, payload.worker)
    return {"ok": task is not None, "message": message, "task": task}


@router.post("/recompute/tasks/{task_id}/complete")
def complete_task(task_id: int) -> dict[str, Any]:
    return recompute_service.complete_task(task_id)


@router.get("/confluence/{feature}/read")
def read_confluence(feature: str) -> dict[str, Any]:
    """下游按本次结果的可用状态读取汇合特征：临时发布 / 等待齐版 / 已交付。"""
    read = recompute_service.read_confluence(feature)
    if read is None:
        raise HTTPException(status_code=404, detail=f"汇合特征 {feature} 暂无结果")
    return read


@router.get("/versions/{feature}")
def version_detail(feature: str) -> dict[str, Any]:
    """特征实际版本与全部快照（旧版可复查）。"""
    detail = recompute_service.version_detail(feature)
    if detail is None:
        raise HTTPException(status_code=404, detail=f"特征 {feature} 没有版本快照")
    return detail
