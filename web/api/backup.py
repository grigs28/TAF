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
from sqlalchemy import select, func, desc

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
    """备份任务请求模型（创建模板配置）"""
    task_name: str = Field(..., description="任务名称")
    source_paths: List[str] = Field(..., description="源路径列表")
    task_type: BackupTaskType = Field(BackupTaskType.FULL, description="任务类型")
    exclude_patterns: List[str] = Field(default_factory=list, description="排除模式")
    compression_enabled: bool = Field(True, description="是否启用压缩")
    encryption_enabled: bool = Field(False, description="是否启用加密")
    retention_days: int = Field(180, description="保留天数")
    description: str = Field("", description="任务描述")
    tape_device: Optional[str] = Field(None, description="目标磁带机设备（可选）")


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
    http_request: Request
):
    """创建备份任务配置（模板）
    
    此接口创建备份任务配置模板，不立即执行。
    创建的模板可以在计划任务模块中选择并执行。
    """
    try:
        from config.database import db_manager
        from models.backup import BackupTask, BackupTaskStatus
        
        # 获取系统实例
        system = get_system_instance(http_request)
        if not system:
            raise HTTPException(status_code=500, detail="系统未初始化")

        # 验证源路径
        for path in request.source_paths:
            if not path or not path.strip():
                raise HTTPException(status_code=400, detail="源路径不能为空")

        # 创建备份任务模板
        async with db_manager.AsyncSessionLocal() as session:
            backup_task = BackupTask(
                task_name=request.task_name,
                task_type=request.task_type,
                source_paths=request.source_paths,
                exclude_patterns=request.exclude_patterns,
                compression_enabled=request.compression_enabled,
                encryption_enabled=request.encryption_enabled,
                retention_days=request.retention_days,
                description=request.description,
                tape_device=request.tape_device,  # 保存磁带设备配置
                status=BackupTaskStatus.PENDING,
                is_template=True,  # 标记为模板
                created_by='backup_api'
            )
            
            session.add(backup_task)
            await session.commit()
            await session.refresh(backup_task)
            
            return {
                "success": True,
                "task_id": backup_task.id,
                "message": "备份任务配置已创建",
                "task_name": backup_task.task_name,
                "is_template": True
            }

    except HTTPException:
        raise
    except Exception as e:
        logger.error(f"创建备份任务配置失败: {str(e)}")
        raise HTTPException(status_code=500, detail=str(e))


@router.get("/tasks", response_model=List[BackupTaskResponse])
async def get_backup_tasks(
    status: Optional[str] = None,
    task_type: Optional[str] = None,
    limit: int = 50,
    offset: int = 0,
    http_request: Request = None
):
    """获取备份任务列表（执行记录）
    
    此接口返回所有备份任务的执行记录，包括：
    - 通过计划任务模块创建的备份任务
    - 通过备份管理模块立即执行的备份任务
    """
    try:
        from config.database import db_manager
        from sqlalchemy import select, desc
        from models.backup import BackupTask, BackupTaskStatus, BackupTaskType
        
        async with db_manager.AsyncSessionLocal() as session:
            # 构建查询
            stmt = select(BackupTask)
            
            # 应用过滤条件
            if is_template is not None:
                stmt = stmt.where(BackupTask.is_template == is_template)
            
            if status:
                try:
                    status_enum = BackupTaskStatus(status)
                    stmt = stmt.where(BackupTask.status == status_enum)
                except ValueError:
                    pass  # 无效的状态值，忽略过滤
            
            if task_type:
                try:
                    task_type_enum = BackupTaskType(task_type)
                    stmt = stmt.where(BackupTask.task_type == task_type_enum)
                except ValueError:
                    pass  # 无效的类型值，忽略过滤
            
            # 按创建时间倒序排列
            stmt = stmt.order_by(desc(BackupTask.created_at))
            
            # 应用分页
            stmt = stmt.limit(limit).offset(offset)
            
            result = await session.execute(stmt)
            backup_tasks = result.scalars().all()
            
            # 转换为响应格式
            tasks = []
            for task in backup_tasks:
                tasks.append({
                    "task_id": task.id,
                    "task_name": task.task_name,
                    "task_type": task.task_type.value,
                    "status": task.status.value,
                    "progress_percent": task.progress_percent or 0.0,
                    "total_files": task.total_files or 0,
                    "processed_files": task.processed_files or 0,
                    "total_bytes": task.total_bytes or 0,
                    "processed_bytes": task.processed_bytes or 0,
                    "created_at": task.created_at,
                    "started_at": task.started_at,
                    "completed_at": task.completed_at,
                    "error_message": task.error_message,
                    "is_template": task.is_template or False,
                    "tape_device": task.tape_device  # 添加磁带设备信息
                })
            
            return tasks

    except Exception as e:
        logger.error(f"获取备份任务列表失败: {str(e)}")
        raise HTTPException(status_code=500, detail=str(e))


@router.get("/tasks/{task_id}", response_model=BackupTaskResponse)
async def get_backup_task(task_id: int):
    """获取备份任务详情"""
    try:
        from config.database import db_manager
        from sqlalchemy import select
        from models.backup import BackupTask
        
        async with db_manager.AsyncSessionLocal() as session:
            stmt = select(BackupTask).where(BackupTask.id == task_id)
            result = await session.execute(stmt)
            task = result.scalar_one_or_none()
            
            if not task:
                raise HTTPException(status_code=404, detail="备份任务不存在")
            
            return {
                "task_id": task.id,
                "task_name": task.task_name,
                "task_type": task.task_type.value,
                "status": task.status.value,
                "progress_percent": task.progress_percent or 0.0,
                "total_files": task.total_files or 0,
                "processed_files": task.processed_files or 0,
                "total_bytes": task.total_bytes or 0,
                "processed_bytes": task.processed_bytes or 0,
                "created_at": task.created_at,
                "started_at": task.started_at,
                "completed_at": task.completed_at,
                "error_message": task.error_message,
                "is_template": task.is_template or False,
                "tape_device": task.tape_device
            }

    except HTTPException:
        raise
    except Exception as e:
        logger.error(f"获取备份任务详情失败: {str(e)}")
        raise HTTPException(status_code=500, detail=str(e))


@router.get("/templates", response_model=List[Dict[str, Any]])
async def get_backup_templates(
    limit: int = 50,
    offset: int = 0,
    http_request: Request = None
):
    """获取备份任务模板列表（配置）
    
    返回所有备份任务配置模板，供计划任务模块选择。
    """
    try:
        from config.database import db_manager
        from sqlalchemy import select, desc
        from models.backup import BackupTask
        
        async with db_manager.AsyncSessionLocal() as session:
            # 查询所有模板
            stmt = select(BackupTask).where(BackupTask.is_template == True)
            stmt = stmt.order_by(desc(BackupTask.created_at))
            stmt = stmt.limit(limit).offset(offset)
            
            result = await session.execute(stmt)
            templates = result.scalars().all()
            
            # 转换为响应格式
            template_list = []
            for template in templates:
                template_list.append({
                    "task_id": template.id,
                    "task_name": template.task_name,
                    "task_type": template.task_type.value,
                    "description": template.description,
                    "source_paths": template.source_paths or [],
                    "tape_device": template.tape_device,
                    "compression_enabled": template.compression_enabled,
                    "encryption_enabled": template.encryption_enabled,
                    "retention_days": template.retention_days,
                    "exclude_patterns": template.exclude_patterns or [],
                    "created_at": template.created_at
                })
            
            return template_list

    except Exception as e:
        logger.error(f"获取备份任务模板列表失败: {str(e)}")
        raise HTTPException(status_code=500, detail=str(e))


@router.put("/tasks/{task_id}/cancel")
async def cancel_backup_task(task_id: int, http_request: Request = None):
    """取消备份任务（仅限执行记录）"""
    try:
        # 获取系统实例
        system = get_system_instance(http_request)
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
async def get_task_status(task_id: int, http_request: Request = None):
    """获取任务状态"""
    try:
        # 获取系统实例
        system = get_system_instance(http_request)
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
        from config.database import db_manager
        from sqlalchemy import select, func, and_
        from models.backup import BackupTask, BackupTaskStatus
        from datetime import datetime, timedelta
        
        async with db_manager.AsyncSessionLocal() as session:
            # 总任务数
            total_stmt = select(func.count(BackupTask.id))
            total_result = await session.execute(total_stmt)
            total_tasks = total_result.scalar() or 0
            
            # 按状态统计
            completed_stmt = select(func.count(BackupTask.id)).where(BackupTask.status == BackupTaskStatus.COMPLETED)
            completed_result = await session.execute(completed_stmt)
            completed_tasks = completed_result.scalar() or 0
            
            failed_stmt = select(func.count(BackupTask.id)).where(BackupTask.status == BackupTaskStatus.FAILED)
            failed_result = await session.execute(failed_stmt)
            failed_tasks = failed_result.scalar() or 0
            
            running_stmt = select(func.count(BackupTask.id)).where(BackupTask.status == BackupTaskStatus.RUNNING)
            running_result = await session.execute(running_stmt)
            running_tasks = running_result.scalar() or 0
            
            pending_stmt = select(func.count(BackupTask.id)).where(BackupTask.status == BackupTaskStatus.PENDING)
            pending_result = await session.execute(pending_stmt)
            pending_tasks = pending_result.scalar() or 0
            
            # 成功率
            success_rate = (completed_tasks / total_tasks * 100) if total_tasks > 0 else 0.0
            
            # 总备份数据量
            bytes_stmt = select(func.sum(BackupTask.processed_bytes)).where(BackupTask.status == BackupTaskStatus.COMPLETED)
            bytes_result = await session.execute(bytes_stmt)
            total_data_backed_up = bytes_result.scalar() or 0
            
            # 最近24小时统计
            twenty_four_hours_ago = datetime.now() - timedelta(hours=24)
            recent_stmt = select(
                func.count(BackupTask.id),
                func.sum(func.case((BackupTask.status == BackupTaskStatus.COMPLETED, 1), else_=0)),
                func.sum(func.case((BackupTask.status == BackupTaskStatus.FAILED, 1), else_=0)),
                func.sum(BackupTask.processed_bytes)
            ).where(BackupTask.created_at >= twenty_four_hours_ago)
            recent_result = await session.execute(recent_stmt)
            recent_row = recent_result.first()
            
            recent_total = recent_row[0] or 0
            recent_completed = recent_row[1] or 0
            recent_failed = recent_row[2] or 0
            recent_data = recent_row[3] or 0
            
            # 平均任务时长（简化计算）
            avg_duration = 3600  # 暂时使用默认值，后续可以从completed_at - started_at计算
            
            return {
                "total_tasks": total_tasks,
                "completed_tasks": completed_tasks,
                "failed_tasks": failed_tasks,
                "running_tasks": running_tasks,
                "pending_tasks": pending_tasks,
                "success_rate": round(success_rate, 2),
                "total_data_backed_up": total_data_backed_up,
                "compression_ratio": 0.65,  # 可以从BackupSet计算
                "average_task_duration": avg_duration,
                "recent_24h": {
                    "total_tasks": recent_total,
                    "completed_tasks": recent_completed,
                    "failed_tasks": recent_failed,
                    "data_backed_up": recent_data
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
        from config.database import db_manager
        from models.backup import BackupSet
        
        async with db_manager.AsyncSessionLocal() as session:
            # 构建查询
            stmt = select(BackupSet)
            
            # 应用过滤条件
            if backup_group:
                stmt = stmt.where(BackupSet.backup_group == backup_group)
            
            # 按备份时间倒序排列
            stmt = stmt.order_by(desc(BackupSet.backup_time))
            
            # 获取总数
            count_stmt = select(func.count(BackupSet.id))
            if backup_group:
                count_stmt = count_stmt.where(BackupSet.backup_group == backup_group)
            count_result = await session.execute(count_stmt)
            total = count_result.scalar() or 0
            
            # 应用分页
            stmt = stmt.limit(limit).offset(offset)
            result = await session.execute(stmt)
            backup_sets = result.scalars().all()
            
            # 转换为响应格式
            sets_list = []
            for backup_set in backup_sets:
                sets_list.append({
                    "set_id": backup_set.set_id,
                    "set_name": backup_set.set_name,
                    "backup_group": backup_set.backup_group,
                    "backup_type": backup_set.backup_type.value,
                    "backup_time": backup_set.backup_time.isoformat() if backup_set.backup_time else None,
                    "total_files": backup_set.total_files or 0,
                    "total_bytes": backup_set.total_bytes or 0,
                    "tape_id": backup_set.tape_id,
                    "status": backup_set.status.value if backup_set.status else "active"
                })
            
            return {
                "backup_sets": sets_list,
                "total": total,
                "limit": limit,
                "offset": offset
            }

    except Exception as e:
        logger.error(f"获取备份集列表失败: {str(e)}")
        raise HTTPException(status_code=500, detail=str(e))