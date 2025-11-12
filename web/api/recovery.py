#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
恢复管理API
Recovery Management API
"""

import logging
from typing import List, Dict, Any, Optional
from datetime import datetime
from fastapi import APIRouter, HTTPException, Depends, BackgroundTasks, Request
from pydantic import BaseModel

logger = logging.getLogger(__name__)
router = APIRouter()


class RecoveryRequest(BaseModel):
    """恢复请求模型"""
    backup_set_id: str
    files: List[Dict[str, Any]]
    target_path: str


# 注意：路由顺序很重要，更具体的路径应该放在前面
# 先定义带路径参数的具体路由，再定义通用路由

@router.get("/backup-sets/{backup_set_id}/top-level")
async def get_top_level_directories(backup_set_id: str, request: Request):
    """获取备份集的顶层目录结构（优化性能，避免一次性加载所有文件）"""
    try:
        logger.info(f"收到获取顶层目录请求: backup_set_id={backup_set_id}")
        system = request.app.state.system
        if not system:
            logger.error("系统未初始化")
            raise HTTPException(status_code=500, detail="系统未初始化")

        directories = await system.recovery_engine.get_top_level_directories(backup_set_id)
        logger.info(f"返回 {len(directories)} 个顶层目录项")
        return {"items": directories}

    except HTTPException:
        raise
    except Exception as e:
        logger.error(f"获取顶层目录结构失败: {str(e)}", exc_info=True)
        raise HTTPException(status_code=500, detail=str(e))


@router.get("/backup-sets/{backup_set_id}/directory")
async def get_directory_contents(
    backup_set_id: str, 
    path: str = "",
    request: Request = None
):
    """获取指定目录下的文件和子目录列表"""
    try:
        system = request.app.state.system
        if not system:
            raise HTTPException(status_code=500, detail="系统未初始化")

        contents = await system.recovery_engine.get_directory_contents(backup_set_id, path)
        return {"items": contents}

    except Exception as e:
        logger.error(f"获取目录内容失败: {str(e)}")
        raise HTTPException(status_code=500, detail=str(e))


@router.get("/backup-sets/{backup_set_id}/files")
async def get_backup_set_files(backup_set_id: str, request: Request):
    """获取备份集文件列表"""
    try:
        system = request.app.state.system
        if not system:
            raise HTTPException(status_code=500, detail="系统未初始化")

        files = await system.recovery_engine.get_backup_set_files(backup_set_id)
        return {"files": files}

    except Exception as e:
        logger.error(f"获取备份集文件列表失败: {str(e)}")
        raise HTTPException(status_code=500, detail=str(e))


@router.get("/backup-sets")
async def search_backup_sets(
    request: Request,
    backup_group: Optional[str] = None,
    tape_id: Optional[str] = None,
    date_from: Optional[datetime] = None,
    date_to: Optional[datetime] = None
):
    """搜索备份集"""
    try:
        # 获取系统实例
        system = request.app.state.system
        if not system:
            return {"backup_sets": []}

        filters = {}
        if backup_group:
            filters['backup_group'] = backup_group
        if tape_id:
            filters['tape_id'] = tape_id
        if date_from:
            filters['date_from'] = date_from
        if date_to:
            filters['date_to'] = date_to

        backup_sets = await system.recovery_engine.search_backup_sets(filters)
        return {"backup_sets": backup_sets}

    except Exception as e:
        logger.error(f"搜索备份集失败: {str(e)}")
        return {"backup_sets": []}


@router.post("/tasks")
async def create_recovery_task(
    recovery_request: RecoveryRequest,
    background_tasks: BackgroundTasks,
    request: Request
):
    """创建恢复任务"""
    try:
        system = request.app.state.system
        if not system:
            raise HTTPException(status_code=500, detail="系统未初始化")

        recovery_id = await system.recovery_engine.create_recovery_task(
            backup_set_id=recovery_request.backup_set_id,
            files=recovery_request.files,
            target_path=recovery_request.target_path
        )

        if not recovery_id:
            raise HTTPException(status_code=500, detail="创建恢复任务失败")

        # 添加到后台任务
        background_tasks.add_task(
            system.recovery_engine.execute_recovery,
            recovery_id
        )

        return {
            "success": True,
            "recovery_id": recovery_id,
            "message": "恢复任务创建成功"
        }

    except Exception as e:
        logger.error(f"创建恢复任务失败: {str(e)}")
        raise HTTPException(status_code=500, detail=str(e))


@router.get("/tasks/{recovery_id}/status")
async def get_recovery_status(recovery_id: str, request: Request):
    """获取恢复状态"""
    try:
        system = request.app.state.system
        if not system:
            raise HTTPException(status_code=500, detail="系统未初始化")

        status = await system.recovery_engine.get_recovery_status(recovery_id)
        if status:
            return status
        else:
            raise HTTPException(status_code=404, detail="恢复任务不存在")

    except HTTPException:
        raise
    except Exception as e:
        logger.error(f"获取恢复状态失败: {str(e)}")
        raise HTTPException(status_code=500, detail=str(e))


@router.get("/backup-groups")
async def get_backup_groups(request: Request):
    """获取备份组列表"""
    try:
        system = request.app.state.system
        if not system:
            return {"backup_groups": []}

        groups = await system.recovery_engine.get_backup_groups()
        return {"backup_groups": groups}

    except Exception as e:
        logger.error(f"获取备份组列表失败: {str(e)}")
        return {"backup_groups": []}