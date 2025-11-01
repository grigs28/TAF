#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
备份管理API
Backup Management API
"""

import logging
from typing import List, Dict, Any, Optional
from datetime import datetime
from fastapi import APIRouter, HTTPException, Depends, BackgroundTasks, Request
from pydantic import BaseModel, Field

from models.backup import BackupTask, BackupTaskType
from config.settings import get_settings
from utils.logger import get_logger

logger = get_logger(__name__)
router = APIRouter()
settings = get_settings()


def get_system_instance(request: Request):
    """获取系统实例"""
    return request.app.state.system


class BackupTaskRequest(BaseModel):
    """备份任务请求模型"""
    task_name: str = Field(..., description="任务名称")
    source_paths: List[str] = Field(..., description="源路径列表")
    task_type: BackupTaskType = Field(BackupTaskType.FULL, description="任务类型")
    exclude_patterns: List[str] = Field(default_factory=list, description="排除模式")
    compression_enabled: bool = Field(True, description="是否启用压缩")
    encryption_enabled: bool = Field(False, description="是否启用加密")
    retention_days: int = Field(180, description="保留天数")
    description: str = Field("", description="任务描述")
    scheduled_time: Optional[datetime] = Field(None, description="计划执行时间")


class BackupTaskResponse(BaseModel):
    """备份任务响应模型"""
    task_id: int
    task_name: str
    task_type: str
    status: str
    progress_percent: float
    total_files: int
    processed_files: int
    total_bytes: int
    processed_bytes: int
    created_at: datetime
    started_at: Optional[datetime]
    completed_at: Optional[datetime]
    error_message: Optional[str]


@router.post("/tasks", response_model=Dict[str, Any])
async def create_backup_task(
    request: BackupTaskRequest,
    background_tasks: BackgroundTasks,
    http_request: Request
):
    """创建备份任务"""
    try:
        # 获取系统实例
        system = get_system_instance(http_request)
        if not system:
            raise HTTPException(status_code=500, detail="系统未初始化")

        # 创建备份任务
        backup_task = await system.backup_engine.create_backup_task(
            task_name=request.task_name,
            source_paths=request.source_paths,
            task_type=request.task_type,
            exclude_patterns=request.exclude_patterns,
            compression_enabled=request.compression_enabled,
            encryption_enabled=request.encryption_enabled,
            retention_days=request.retention_days,
            description=request.description,
            scheduled_time=request.scheduled_time
        )

        if not backup_task:
            raise HTTPException(status_code=500, detail="创建备份任务失败")

        # 如果是立即执行，添加到后台任务
        if not request.scheduled_time:
            background_tasks.add_task(
                system.backup_engine.execute_backup_task,
                backup_task
            )

        return {
            "success": True,
            "task_id": backup_task.id,
            "message": "备份任务创建成功"
        }

    except Exception as e:
        logger.error(f"创建备份任务失败: {str(e)}")
        raise HTTPException(status_code=500, detail=str(e))


@router.get("/tasks", response_model=List[BackupTaskResponse])
async def get_backup_tasks(
    status: Optional[str] = None,
    task_type: Optional[str] = None,
    limit: int = 50,
    offset: int = 0
):
    """获取备份任务列表"""
    try:
        # 这里应该从数据库查询任务列表
        # 暂时返回示例数据
        sample_tasks = [
            {
                "task_id": 1,
                "task_name": "月度备份-2024-01",
                "task_type": "monthly_full",
                "status": "completed",
                "progress_percent": 100.0,
                "total_files": 1500,
                "processed_files": 1500,
                "total_bytes": 10737418240,
                "processed_bytes": 10737418240,
                "created_at": datetime.now(),
                "started_at": datetime.now(),
                "completed_at": datetime.now(),
                "error_message": None
            },
            {
                "task_id": 2,
                "task_name": "增量备份-2024-01-15",
                "task_type": "incremental",
                "status": "running",
                "progress_percent": 45.5,
                "total_files": 500,
                "processed_files": 227,
                "total_bytes": 2147483648,
                "processed_bytes": 976562500,
                "created_at": datetime.now(),
                "started_at": datetime.now(),
                "completed_at": None,
                "error_message": None
            }
        ]

        # 应用过滤条件
        if status:
            sample_tasks = [t for t in sample_tasks if t["status"] == status]

        if task_type:
            sample_tasks = [t for t in sample_tasks if t["task_type"] == task_type]

        # 应用分页
        total = len(sample_tasks)
        tasks = sample_tasks[offset:offset + limit]

        return tasks

    except Exception as e:
        logger.error(f"获取备份任务列表失败: {str(e)}")
        raise HTTPException(status_code=500, detail=str(e))


@router.get("/tasks/{task_id}", response_model=BackupTaskResponse)
async def get_backup_task(task_id: int):
    """获取备份任务详情"""
    try:
        # 这里应该从数据库查询任务详情
        # 暂时返回示例数据
        if task_id == 1:
            return {
                "task_id": 1,
                "task_name": "月度备份-2024-01",
                "task_type": "monthly_full",
                "status": "completed",
                "progress_percent": 100.0,
                "total_files": 1500,
                "processed_files": 1500,
                "total_bytes": 10737418240,
                "processed_bytes": 10737418240,
                "created_at": datetime.now(),
                "started_at": datetime.now(),
                "completed_at": datetime.now(),
                "error_message": None
            }
        else:
            raise HTTPException(status_code=404, detail="任务不存在")

    except HTTPException:
        raise
    except Exception as e:
        logger.error(f"获取备份任务详情失败: {str(e)}")
        raise HTTPException(status_code=500, detail=str(e))


@router.put("/tasks/{task_id}/cancel")
async def cancel_backup_task(task_id: int):
    """取消备份任务"""
    try:
        # 获取系统实例
        system = request.app.state.system
        if not system:
            raise HTTPException(status_code=500, detail="系统未初始化")

        success = await system.backup_engine.cancel_task(task_id)
        if success:
            return {"success": True, "message": "任务已取消"}
        else:
            raise HTTPException(status_code=404, detail="任务不存在或无法取消")

    except HTTPException:
        raise
    except Exception as e:
        logger.error(f"取消备份任务失败: {str(e)}")
        raise HTTPException(status_code=500, detail=str(e))


@router.get("/tasks/{task_id}/status")
async def get_task_status(task_id: int):
    """获取任务状态"""
    try:
        # 获取系统实例
        system = request.app.state.system
        if not system:
            raise HTTPException(status_code=500, detail="系统未初始化")

        status = await system.backup_engine.get_task_status(task_id)
        if status:
            return status
        else:
            raise HTTPException(status_code=404, detail="任务不存在")

    except HTTPException:
        raise
    except Exception as e:
        logger.error(f"获取任务状态失败: {str(e)}")
        raise HTTPException(status_code=500, detail=str(e))


@router.get("/statistics")
async def get_backup_statistics():
    """获取备份统计信息"""
    try:
        # 这里应该从数据库计算统计信息
        # 暂时返回示例数据
        return {
            "total_tasks": 25,
            "completed_tasks": 20,
            "failed_tasks": 2,
            "running_tasks": 1,
            "pending_tasks": 2,
            "success_rate": 80.0,
            "total_data_backed_up": 107374182400,  # 100GB
            "compression_ratio": 0.65,
            "average_task_duration": 3600,  # 秒
            "recent_24h": {
                "total_tasks": 5,
                "completed_tasks": 4,
                "failed_tasks": 0,
                "data_backed_up": 21474836480  # 20GB
            }
        }

    except Exception as e:
        logger.error(f"获取备份统计信息失败: {str(e)}")
        raise HTTPException(status_code=500, detail=str(e))


@router.get("/backup-sets")
async def get_backup_sets(
    backup_group: Optional[str] = None,
    limit: int = 50,
    offset: int = 0
):
    """获取备份集列表"""
    try:
        # 这里应该从数据库查询备份集
        # 暂时返回示例数据
        sample_backup_sets = [
            {
                "set_id": "2024-01_000001",
                "set_name": "月度备份_2024-01_000001",
                "backup_group": "2024-01",
                "backup_type": "monthly_full",
                "backup_time": datetime.now().isoformat(),
                "total_files": 1500,
                "total_bytes": 10737418240,
                "tape_id": "TAPE001",
                "status": "active"
            },
            {
                "set_id": "2024-02_000001",
                "set_name": "月度备份_2024-02_000001",
                "backup_group": "2024-02",
                "backup_type": "monthly_full",
                "backup_time": datetime.now().isoformat(),
                "total_files": 1600,
                "total_bytes": 12884901888,
                "tape_id": "TAPE002",
                "status": "active"
            }
        ]

        # 应用过滤条件
        if backup_group:
            sample_backup_sets = [s for s in sample_backup_sets if s["backup_group"] == backup_group]

        # 应用分页
        total = len(sample_backup_sets)
        backup_sets = sample_backup_sets[offset:offset + limit]

        return {
            "backup_sets": backup_sets,
            "total": total,
            "limit": limit,
            "offset": offset
        }

    except Exception as e:
        logger.error(f"获取备份集列表失败: {str(e)}")
        raise HTTPException(status_code=500, detail=str(e))