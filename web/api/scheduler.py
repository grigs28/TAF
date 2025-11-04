#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
计划任务管理API
Scheduled Task Management API
"""

import logging
from typing import List, Optional, Dict, Any
from fastapi import APIRouter, HTTPException, Request
from pydantic import BaseModel, Field
from datetime import datetime

from models.scheduled_task import ScheduledTask, ScheduledTaskLog, ScheduleType, ScheduledTaskStatus, TaskActionType
from utils.scheduler import TaskScheduler

logger = logging.getLogger(__name__)
router = APIRouter()


# ===== 请求/响应模型 =====

class ScheduleConfigBase(BaseModel):
    """调度配置基类"""
    pass


class OnceScheduleConfig(ScheduleConfigBase):
    """一次性任务配置"""
    datetime: str = Field(..., description="执行时间，格式: YYYY-MM-DD HH:MM:SS")


class IntervalScheduleConfig(ScheduleConfigBase):
    """间隔任务配置"""
    interval: int = Field(..., description="间隔数值")
    unit: str = Field(..., description="时间单位: minutes/hours/days")


class DailyScheduleConfig(ScheduleConfigBase):
    """每日任务配置"""
    time: str = Field(..., description="执行时间，格式: HH:MM:SS")


class WeeklyScheduleConfig(ScheduleConfigBase):
    """每周任务配置"""
    day_of_week: int = Field(..., ge=0, le=6, description="星期几 (0=Monday, 6=Sunday)")
    time: str = Field(..., description="执行时间，格式: HH:MM:SS")


class MonthlyScheduleConfig(ScheduleConfigBase):
    """每月任务配置"""
    day_of_month: int = Field(..., ge=1, le=31, description="每月几号")
    time: str = Field(..., description="执行时间，格式: HH:MM:SS")


class YearlyScheduleConfig(ScheduleConfigBase):
    """每年任务配置"""
    month: int = Field(..., ge=1, le=12, description="月份")
    day: int = Field(..., ge=1, le=31, description="日期")
    time: str = Field(..., description="执行时间，格式: HH:MM:SS")


class CronScheduleConfig(ScheduleConfigBase):
    """Cron表达式配置"""
    cron: str = Field(..., description="Cron表达式")


class ScheduledTaskCreate(BaseModel):
    """创建计划任务请求"""
    task_name: str = Field(..., description="任务名称")
    description: Optional[str] = Field(None, description="任务描述")
    schedule_type: str = Field(..., description="调度类型: once/interval/daily/weekly/monthly/yearly/cron")
    schedule_config: Dict[str, Any] = Field(..., description="调度配置")
    action_type: str = Field(..., description="任务动作类型: backup/recovery/cleanup/health_check/retention_check/custom")
    action_config: Dict[str, Any] = Field(default_factory=dict, description="任务动作配置")
    backup_task_id: Optional[int] = Field(None, description="备份任务模板ID（当action_type=backup时使用）")
    enabled: bool = Field(True, description="是否启用")
    tags: Optional[List[str]] = Field(None, description="标签列表")
    task_metadata: Optional[Dict[str, Any]] = Field(None, description="任务元数据")


class ScheduledTaskUpdate(BaseModel):
    """更新计划任务请求"""
    task_name: Optional[str] = Field(None, description="任务名称")
    description: Optional[str] = Field(None, description="任务描述")
    schedule_type: Optional[str] = Field(None, description="调度类型")
    schedule_config: Optional[Dict[str, Any]] = Field(None, description="调度配置")
    action_type: Optional[str] = Field(None, description="任务动作类型")
    action_config: Optional[Dict[str, Any]] = Field(None, description="任务动作配置")
    enabled: Optional[bool] = Field(None, description="是否启用")
    tags: Optional[List[str]] = Field(None, description="标签列表")
    task_metadata: Optional[Dict[str, Any]] = Field(None, description="任务元数据")


class ScheduledTaskResponse(BaseModel):
    """计划任务响应"""
    id: int
    task_name: str
    description: Optional[str]
    schedule_type: str
    schedule_config: Dict[str, Any]
    action_type: str
    action_config: Dict[str, Any]
    status: str
    enabled: bool
    next_run_time: Optional[datetime]
    last_run_time: Optional[datetime]
    last_success_time: Optional[datetime]
    last_failure_time: Optional[datetime]
    total_runs: int
    success_runs: int
    failure_runs: int
    average_duration: Optional[int]
    last_error: Optional[str]
    tags: Optional[List[str]]
    task_metadata: Optional[Dict[str, Any]]
    created_at: datetime
    updated_at: datetime

    class Config:
        from_attributes = True


# ===== API端点 =====

@router.get("/tasks", response_model=List[ScheduledTaskResponse])
async def get_scheduled_tasks(
    enabled_only: bool = False,
    request: Request = None
):
    """获取所有计划任务"""
    try:
        system = request.app.state.system
        if not system:
            raise HTTPException(status_code=500, detail="系统未初始化")
        
        # 使用TaskScheduler获取任务
        scheduler: TaskScheduler = system.scheduler
        tasks = await scheduler.get_tasks(enabled_only=enabled_only)
        
        return [ScheduledTaskResponse(**task.to_dict()) for task in tasks]
        
    except HTTPException:
        raise
    except Exception as e:
        logger.error(f"获取计划任务列表失败: {str(e)}")
        raise HTTPException(status_code=500, detail=str(e))


@router.get("/tasks/{task_id}", response_model=ScheduledTaskResponse)
async def get_scheduled_task(task_id: int, request: Request = None):
    """获取单个计划任务"""
    try:
        system = request.app.state.system
        if not system:
            raise HTTPException(status_code=500, detail="系统未初始化")
        
        scheduler: TaskScheduler = system.scheduler
        task = await scheduler.get_task(task_id)
        
        if not task:
            raise HTTPException(status_code=404, detail="计划任务不存在")
        
        return ScheduledTaskResponse(**task.to_dict())
        
    except HTTPException:
        raise
    except Exception as e:
        logger.error(f"获取计划任务失败: {str(e)}")
        raise HTTPException(status_code=500, detail=str(e))


@router.post("/tasks", response_model=ScheduledTaskResponse)
async def create_scheduled_task(task: ScheduledTaskCreate, request: Request = None):
    """创建计划任务
    
    当action_type=backup时，如果提供了backup_task_id，将从备份任务模板加载配置。
    """
    try:
        system = request.app.state.system
        if not system:
            raise HTTPException(status_code=500, detail="系统未初始化")
        
        scheduler: TaskScheduler = system.scheduler
        
        # 验证备份任务模板（如果提供了backup_task_id）
        if task.action_type == "backup" and task.backup_task_id:
            from config.database import db_manager
            from sqlalchemy import select
            from models.backup import BackupTask
            
            async with db_manager.AsyncSessionLocal() as session:
                from sqlalchemy import and_
                stmt = select(BackupTask).where(
                    and_(
                        BackupTask.id == task.backup_task_id,
                        BackupTask.is_template == True
                    )
                )
                result = await session.execute(stmt)
                template = result.scalar_one_or_none()
                
                if not template:
                    raise HTTPException(
                        status_code=404,
                        detail=f"备份任务模板不存在: {task.backup_task_id}"
                    )
                
                # 将backup_task_id保存到task_metadata中
                if task.task_metadata is None:
                    task.task_metadata = {}
                task.task_metadata['backup_task_id'] = task.backup_task_id
        
        # 验证调度类型
        try:
            schedule_type = ScheduleType(task.schedule_type)
        except ValueError:
            raise HTTPException(status_code=400, detail=f"无效的调度类型: {task.schedule_type}")
        
        # 验证任务动作类型
        try:
            action_type = TaskActionType(task.action_type)
        except ValueError:
            raise HTTPException(status_code=400, detail=f"无效的任务动作类型: {task.action_type}")
        
        # 创建计划任务对象
        scheduled_task = ScheduledTask(
            task_name=task.task_name,
            description=task.description,
            schedule_type=schedule_type,
            schedule_config=task.schedule_config,
            action_type=action_type,
            action_config=task.action_config,
            enabled=task.enabled,
            status=ScheduledTaskStatus.ACTIVE if task.enabled else ScheduledTaskStatus.INACTIVE,
            tags=task.tags,
            task_metadata=task.task_metadata
        )
        
        # 添加任务
        success = await scheduler.add_task(scheduled_task)
        
        if not success:
            raise HTTPException(status_code=500, detail="创建计划任务失败")
        
        # 重新获取任务（包含ID和时间信息）
        created_task = await scheduler.get_task(scheduled_task.id)
        
        return ScheduledTaskResponse(**created_task.to_dict())
        
    except HTTPException:
        raise
    except Exception as e:
        logger.error(f"创建计划任务失败: {str(e)}")
        raise HTTPException(status_code=500, detail=str(e))


# 更具体的路径必须在通用路径之前定义，以确保路由匹配正确
@router.post("/tasks/{task_id}/run")
async def run_scheduled_task(task_id: int, request: Request = None):
    """立即运行计划任务"""
    try:
        system = request.app.state.system
        if not system:
            raise HTTPException(status_code=500, detail="系统未初始化")
        
        scheduler: TaskScheduler = system.scheduler
        
        success = await scheduler.run_task(task_id)
        
        if not success:
            raise HTTPException(status_code=404, detail="计划任务不存在")
        
        return {"success": True, "message": "计划任务已开始执行"}
        
    except HTTPException:
        raise
    except Exception as e:
        logger.error(f"运行计划任务失败: {str(e)}")
        raise HTTPException(status_code=500, detail=str(e))


@router.post("/tasks/{task_id}/stop")
async def stop_scheduled_task(task_id: int, request: Request = None):
    """停止正在运行的计划任务"""
    try:
        system = request.app.state.system
        if not system:
            raise HTTPException(status_code=500, detail="系统未初始化")
        
        scheduler: TaskScheduler = system.scheduler
        
        success = await scheduler.stop_task(task_id)
        
        if not success:
            raise HTTPException(status_code=404, detail="计划任务不存在或未在运行")
        
        return {"success": True, "message": "计划任务已停止"}
        
    except HTTPException:
        raise
    except Exception as e:
        logger.error(f"停止计划任务失败: {str(e)}")
        raise HTTPException(status_code=500, detail=str(e))


@router.post("/tasks/{task_id}/enable")
async def enable_scheduled_task(task_id: int, request: Request = None):
    """启用计划任务"""
    try:
        system = request.app.state.system
        if not system:
            raise HTTPException(status_code=500, detail="系统未初始化")
        
        scheduler: TaskScheduler = system.scheduler
        
        success = await scheduler.enable_task(task_id)
        
        if not success:
            raise HTTPException(status_code=404, detail="计划任务不存在")
        
        return {"success": True, "message": "计划任务已启用"}
        
    except HTTPException:
        raise
    except Exception as e:
        logger.error(f"启用计划任务失败: {str(e)}")
        raise HTTPException(status_code=500, detail=str(e))


@router.post("/tasks/{task_id}/disable")
async def disable_scheduled_task(task_id: int, request: Request = None):
    """禁用计划任务"""
    try:
        system = request.app.state.system
        if not system:
            raise HTTPException(status_code=500, detail="系统未初始化")
        
        scheduler: TaskScheduler = system.scheduler
        
        success = await scheduler.disable_task(task_id)
        
        if not success:
            raise HTTPException(status_code=404, detail="计划任务不存在")
        
        return {"success": True, "message": "计划任务已禁用"}
        
    except HTTPException:
        raise
    except Exception as e:
        logger.error(f"禁用计划任务失败: {str(e)}")
        raise HTTPException(status_code=500, detail=str(e))


@router.put("/tasks/{task_id}", response_model=ScheduledTaskResponse)
async def update_scheduled_task(
    task_id: int,
    task: ScheduledTaskUpdate,
    request: Request = None
):
    """更新计划任务"""
    try:
        system = request.app.state.system
        if not system:
            raise HTTPException(status_code=500, detail="系统未初始化")
        
        scheduler: TaskScheduler = system.scheduler
        
        # 构建更新字典
        updates = {}
        
        if task.task_name is not None:
            updates['task_name'] = task.task_name
        if task.description is not None:
            updates['description'] = task.description
        if task.schedule_type is not None:
            try:
                updates['schedule_type'] = ScheduleType(task.schedule_type)
            except ValueError:
                raise HTTPException(status_code=400, detail=f"无效的调度类型: {task.schedule_type}")
        if task.schedule_config is not None:
            updates['schedule_config'] = task.schedule_config
        if task.action_type is not None:
            try:
                updates['action_type'] = TaskActionType(task.action_type)
            except ValueError:
                raise HTTPException(status_code=400, detail=f"无效的任务动作类型: {task.action_type}")
        if task.action_config is not None:
            updates['action_config'] = task.action_config
        if task.enabled is not None:
            updates['enabled'] = task.enabled
            # 更新状态
            if task.enabled:
                updates['status'] = ScheduledTaskStatus.ACTIVE
            else:
                updates['status'] = ScheduledTaskStatus.INACTIVE
        if task.tags is not None:
            updates['tags'] = task.tags
        if task.task_metadata is not None:
            updates['task_metadata'] = task.task_metadata
        
        # 更新任务
        success = await scheduler.update_task(task_id, updates)
        
        if not success:
            raise HTTPException(status_code=404, detail="计划任务不存在")
        
        # 重新获取任务
        updated_task = await scheduler.get_task(task_id)
        
        return ScheduledTaskResponse(**updated_task.to_dict())
        
    except HTTPException:
        raise
    except Exception as e:
        logger.error(f"更新计划任务失败: {str(e)}")
        raise HTTPException(status_code=500, detail=str(e))


@router.delete("/tasks/{task_id}")
async def delete_scheduled_task(task_id: int, request: Request = None):
    """删除计划任务"""
    try:
        system = request.app.state.system
        if not system:
            raise HTTPException(status_code=500, detail="系统未初始化")
        
        scheduler: TaskScheduler = system.scheduler
        
        success = await scheduler.delete_task(task_id)
        
        if not success:
            raise HTTPException(status_code=404, detail="计划任务不存在")
        
        return {"success": True, "message": "计划任务已删除"}
        
    except HTTPException:
        raise
    except Exception as e:
        logger.error(f"删除计划任务失败: {str(e)}")
        raise HTTPException(status_code=500, detail=str(e))


@router.get("/tasks/{task_id}/logs")
async def get_task_logs(
    task_id: int,
    limit: int = 100,
    offset: int = 0,
    request: Request = None
):
    """获取计划任务执行日志"""
    try:
        from config.database import db_manager
        from sqlalchemy import select, desc
        
        async with db_manager.AsyncSessionLocal() as session:
            stmt = (
                select(ScheduledTaskLog)
                .where(ScheduledTaskLog.scheduled_task_id == task_id)
                .order_by(desc(ScheduledTaskLog.started_at))
                .limit(limit)
                .offset(offset)
            )
            result = await session.execute(stmt)
            logs = result.scalars().all()
            
            total_stmt = select(ScheduledTaskLog).where(ScheduledTaskLog.scheduled_task_id == task_id)
            total_result = await session.execute(total_stmt)
            total = len(list(total_result.scalars().all()))
            
            return {
                "logs": [log.to_dict() for log in logs],
                "total": total,
                "limit": limit,
                "offset": offset
            }
            
    except Exception as e:
        logger.error(f"获取任务日志失败: {str(e)}")
        raise HTTPException(status_code=500, detail=str(e))


@router.get("/scheduler/status")
async def get_scheduler_status(request: Request = None):
    """获取调度器状态"""
    try:
        system = request.app.state.system
        if not system:
            raise HTTPException(status_code=500, detail="系统未初始化")
        
        scheduler: TaskScheduler = system.scheduler
        
        return {
            "running": scheduler.running,
            "total_tasks": len(scheduler.tasks),
            "enabled_tasks": len([t for t in scheduler.tasks.values() if t.get('task', {}).enabled]),
            "running_executions": len(scheduler._running_executions)
        }
        
    except Exception as e:
        logger.error(f"获取调度器状态失败: {str(e)}")
        raise HTTPException(status_code=500, detail=str(e))

