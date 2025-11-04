#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
任务执行器
Task Executor
"""

import asyncio
import logging
import uuid
from datetime import datetime
from typing import Dict, Any, Callable

from models.scheduled_task import ScheduledTask, ScheduledTaskLog, ScheduledTaskStatus, TaskActionType
from models.system_log import OperationType, LogLevel, LogCategory
from config.database import db_manager
from sqlalchemy import select
from .action_handlers import get_action_handler
from .schedule_calculator import calculate_next_run_time
from utils.log_utils import log_operation, log_system
from .task_storage import record_run_start, record_run_end

logger = logging.getLogger(__name__)


def create_task_executor(scheduled_task: ScheduledTask, system_instance) -> Callable:
    """创建任务执行函数"""
    async def executor():
        execution_id = str(uuid.uuid4())
        start_time = datetime.now()
        
        try:
            # 记录任务执行开始日志
            await log_operation(
                operation_type=OperationType.SCHEDULER_RUN,
                resource_type="scheduler",
                resource_id=str(scheduled_task.id),
                resource_name=scheduled_task.task_name,
                operation_name="执行计划任务",
                operation_description=f"开始执行计划任务: {scheduled_task.task_name}",
                category="scheduler",
                success=True,
                result_message=f"任务执行开始 (执行ID: {execution_id})"
            )
            
            # 更新任务状态为运行中
            async with db_manager.AsyncSessionLocal() as session:
                scheduled_task.status = ScheduledTaskStatus.RUNNING
                scheduled_task.last_run_time = start_time
                session.add(scheduled_task)
                await session.commit()
            
            # 创建执行日志
            task_log = ScheduledTaskLog(
                scheduled_task_id=scheduled_task.id,
                execution_id=execution_id,
                started_at=start_time,
                status='running'
            )
            
            async with db_manager.AsyncSessionLocal() as session:
                session.add(task_log)
                await session.commit()

            # openGauss 原生记录运行开始
            try:
                await record_run_start(scheduled_task.id, execution_id, start_time)
            except Exception:
                pass
            
            # 执行任务动作
            action_type = scheduled_task.action_type
            action_config = scheduled_task.action_config or {}
            
            # 从task_metadata中获取backup_task_id
            backup_task_id = None
            if scheduled_task.task_metadata and isinstance(scheduled_task.task_metadata, dict):
                backup_task_id = scheduled_task.task_metadata.get('backup_task_id')
            
            # 获取动作处理器
            handler = get_action_handler(action_type, system_instance)
            
            # 根据动作类型执行
            if action_type == TaskActionType.BACKUP:
                # 备份动作需要传递backup_task_id参数
                result = await handler.execute(action_config, backup_task_id=backup_task_id, scheduled_task=scheduled_task)
            else:
                result = await handler.execute(action_config, scheduled_task=scheduled_task)
            
            end_time = datetime.now()
            duration = int((end_time - start_time).total_seconds() * 1000)  # 转换为毫秒
            
            # 更新执行日志
            async with db_manager.AsyncSessionLocal() as session:
                stmt = select(ScheduledTaskLog).where(ScheduledTaskLog.execution_id == execution_id)
                log_result = await session.execute(stmt)
                task_log = log_result.scalar_one()
                
                task_log.completed_at = end_time
                task_log.duration = duration // 1000  # 秒
                task_log.status = 'success'
                task_log.result = result
                
                # 更新任务统计
                scheduled_task.total_runs = (scheduled_task.total_runs or 0) + 1
                scheduled_task.success_runs = (scheduled_task.success_runs or 0) + 1
                scheduled_task.last_success_time = end_time
                scheduled_task.status = ScheduledTaskStatus.ACTIVE
                
                # 计算平均执行时长
                if scheduled_task.average_duration:
                    scheduled_task.average_duration = int(
                        (scheduled_task.average_duration + duration // 1000) / 2
                    )
                else:
                    scheduled_task.average_duration = duration // 1000
                
                # 计算下次执行时间
                next_run = calculate_next_run_time(scheduled_task)
                scheduled_task.next_run_time = next_run
                
                session.add(scheduled_task)
                await session.commit()

            # openGauss 原生记录结束（成功）
            try:
                await record_run_end(execution_id, end_time, 'success', result=result)
            except Exception:
                pass
            
            logger.info(f"任务执行成功: {scheduled_task.task_name} (执行ID: {execution_id})")
            
            # 记录任务执行成功日志
            await log_operation(
                operation_type=OperationType.SCHEDULER_RUN,
                resource_type="scheduler",
                resource_id=str(scheduled_task.id),
                resource_name=scheduled_task.task_name,
                operation_name="执行计划任务",
                operation_description=f"计划任务执行成功: {scheduled_task.task_name}",
                category="scheduler",
                success=True,
                result_message=f"任务执行成功 (执行ID: {execution_id}, 耗时: {duration}ms)",
                duration_ms=duration
            )
            
            # 记录系统日志
            await log_system(
                level=LogLevel.INFO,
                category=LogCategory.SYSTEM,
                message=f"计划任务执行成功: {scheduled_task.task_name}",
                module="scheduler",
                function="task_executor",
                task_id=scheduled_task.id,
                details={
                    "execution_id": execution_id,
                    "task_name": scheduled_task.task_name,
                    "task_id": scheduled_task.id,
                    "action_type": scheduled_task.action_type.value if scheduled_task.action_type else None,
                    "duration_ms": duration,
                    "result": result
                },
                duration_ms=duration
            )
            
        except Exception as e:
            end_time = datetime.now()
            duration = int((end_time - start_time).total_seconds() * 1000)  # 转换为毫秒
            error_msg = str(e)
            import traceback
            stack_trace = traceback.format_exc()
            
            logger.error(f"任务执行失败 {scheduled_task.task_name}: {error_msg}")
            
            # 更新执行日志
            try:
                async with db_manager.AsyncSessionLocal() as session:
                    stmt = select(ScheduledTaskLog).where(ScheduledTaskLog.execution_id == execution_id)
                    log_result = await session.execute(stmt)
                    task_log = log_result.scalar_one()
                    
                    task_log.completed_at = end_time
                    task_log.duration = duration // 1000  # 秒
                    task_log.status = 'failed'
                    task_log.error_message = error_msg
                    
                    # 更新任务统计
                    scheduled_task.total_runs = (scheduled_task.total_runs or 0) + 1
                    scheduled_task.failure_runs = (scheduled_task.failure_runs or 0) + 1
                    scheduled_task.last_failure_time = end_time
                    scheduled_task.last_error = error_msg
                    scheduled_task.status = ScheduledTaskStatus.ERROR
                    
                    session.add(scheduled_task)
                    await session.commit()
            except Exception as db_error:
                logger.error(f"更新任务日志失败: {str(db_error)}")

            # openGauss 原生记录结束（失败）
            try:
                await record_run_end(execution_id, end_time, 'failed', result=None, error_message=error_msg)
            except Exception:
                pass
            
            # 记录任务执行失败日志
            await log_operation(
                operation_type=OperationType.SCHEDULER_RUN,
                resource_type="scheduler",
                resource_id=str(scheduled_task.id),
                resource_name=scheduled_task.task_name,
                operation_name="执行计划任务",
                operation_description=f"计划任务执行失败: {scheduled_task.task_name}",
                category="scheduler",
                success=False,
                error_message=error_msg,
                duration_ms=duration
            )
            
            # 记录系统日志（错误）
            await log_system(
                level=LogLevel.ERROR,
                category=LogCategory.SYSTEM,
                message=f"计划任务执行失败: {scheduled_task.task_name}",
                module="scheduler",
                function="task_executor",
                task_id=scheduled_task.id,
                details={
                    "execution_id": execution_id,
                    "task_name": scheduled_task.task_name,
                    "task_id": scheduled_task.id,
                    "action_type": scheduled_task.action_type.value if scheduled_task.action_type else None,
                    "duration_ms": duration,
                    "error": error_msg
                },
                exception_type=type(e).__name__,
                stack_trace=stack_trace,
                duration_ms=duration
            )
    
    return executor

