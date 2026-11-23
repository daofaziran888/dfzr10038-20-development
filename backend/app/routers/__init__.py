"""业务路由汇总：血缘下游重算走专用路由，其余模块共用一套资源路由。"""
from __future__ import annotations

from app.routers import lineage, resource

# 专用路由放在通用资源路由之前，避免被 /api/{module} 动态路径抢先匹配。
ROUTERS = [lineage, resource]
