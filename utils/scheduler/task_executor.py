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
from .task_storage import record_run_start, record_run_end, acquire_task_lock, release_task_lock
from .db_utils import is_opengauss, get_opengauss_connection

logger = logging.getLogger(__name__)


def create_task_executor(scheduled_task: ScheduledTask, system_instance, manual_run: bool = False) -> Callable:
    """创建任务执行函数
    
    Args:
        scheduled_task: 计划任务对象
        system_instance: 系统实例
        manual_run: 是否为手动运行（Web界面点击运行），默认为False
    """
    async def executor():
        execution_id = str(uuid.uuid4())
        start_time = datetime.now()
        lock_acquired = False
        
        try:
            # 获取任务并发锁（openGauss原生），获取失败则跳过
            got_lock = await acquire_task_lock(scheduled_task.id, execution_id)
            if not got_lock:
                await log_system(
                    level=LogLevel.INFO,
                    category=LogCategory.SCHEDULER,
                    message="任务已在执行中，跳过",
                    module="scheduler",
                    function="task_executor",
                    task_id=scheduled_task.id,
                    details={"execution_id": execution_id}
                )
                return
            
            lock_acquired = True

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
            
            # 更新任务状态为运行中（openGauss使用原生SQL，其他使用SQLAlchemy）
            if is_opengauss():
                conn = await get_opengauss_connection()
                try:
                    await conn.execute(
                        """
                        UPDATE scheduled_tasks
                        SET status = $1::scheduledtaskstatus, last_run_time = $2
                        WHERE id = $3
                        """,
                        'running', start_time, scheduled_task.id
                    )
                finally:
                    await conn.close()
            else:
                async with db_manager.AsyncSessionLocal() as session:
                    scheduled_task.status = ScheduledTaskStatus.RUNNING
                    scheduled_task.last_run_time = start_time
                    session.add(scheduled_task)
                    await session.commit()
            
            # 创建执行日志（openGauss使用原生SQL，其他使用SQLAlchemy）
            if is_opengauss():
                # openGauss 原生记录运行开始
                try:
                    await record_run_start(scheduled_task.id, execution_id, start_time)
                except Exception:
                    pass
            else:
                task_log = ScheduledTaskLog(
                    scheduled_task_id=scheduled_task.id,
                    execution_id=execution_id,
                    started_at=start_time,
                    status='running'
                )
                async with db_manager.AsyncSessionLocal() as session:
                    session.add(task_log)
                    await session.commit()
            
            # 执行任务动作
            action_type = scheduled_task.action_type
            action_config = scheduled_task.action_config or {}
            
            # 检查action_type是否有效
            if not action_type:
                raise ValueError(f"任务动作类型为空: {scheduled_task.task_name}")
            
            # 如果action_type是字符串，尝试转换为枚举
            if isinstance(action_type, str):
                try:
                    action_type = TaskActionType(action_type)
                except ValueError:
                    raise ValueError(f"无效的任务动作类型: {action_type} (任务: {scheduled_task.task_name})")
            
            logger.info(f"执行任务动作: {action_type.value if hasattr(action_type, 'value') else action_type} (任务: {scheduled_task.task_name})")
            
            # 从task_metadata中获取backup_task_id
            backup_task_id = None
            if scheduled_task.task_metadata and isinstance(scheduled_task.task_metadata, dict):
                backup_task_id = scheduled_task.task_metadata.get('backup_task_id')
            
            # 获取动作处理器
            try:
                handler = get_action_handler(action_type, system_instance)
            except Exception as e:
                logger.error(f"获取动作处理器失败: {str(e)}, action_type: {action_type}, type: {type(action_type)}")
                raise
            
            # 根据动作类型执行
            if action_type == TaskActionType.BACKUP:
                # 备份动作需要传递backup_task_id参数和manual_run参数
                result = await handler.execute(action_config, backup_task_id=backup_task_id, scheduled_task=scheduled_task, manual_run=manual_run)
            else:
                result = await handler.execute(action_config, scheduled_task=scheduled_task, manual_run=manual_run)
            
            end_time = datetime.now()
            duration = int((end_time - start_time).total_seconds() * 1000)  # 转换为毫秒
            
            # 更新执行日志和任务统计（openGauss使用原生SQL，其他使用SQLAlchemy）
            if is_opengauss():
                # openGauss 原生记录结束（成功）
                try:
                    await record_run_end(execution_id, end_time, 'success', result=result)
                except Exception:
                    pass
                
                # 更新任务统计（使用原生SQL）
                conn = await get_opengauss_connection()
                try:
                    # 获取当前统计值
                    current_task = await conn.fetchrow(
                        """
                        SELECT total_runs, success_runs, average_duration
                        FROM scheduled_tasks
                        WHERE id = $1
                        """,
                        scheduled_task.id
                    )
                    
                    total_runs = (current_task['total_runs'] or 0) + 1
                    success_runs = (current_task['success_runs'] or 0) + 1
                    
                    # 计算平均执行时长
                    avg_duration = duration // 1000  # 秒
                    if current_task['average_duration']:
                        avg_duration = int((current_task['average_duration'] + avg_duration) / 2)
                    
                    # 计算下次执行时间
                    next_run = calculate_next_run_time(scheduled_task)
                    
                    # 更新任务
                    await conn.execute(
                        """
                        UPDATE scheduled_tasks
                        SET status = $1::scheduledtaskstatus,
                            last_success_time = $2,
                            total_runs = $3,
                            success_runs = $4,
                            average_duration = $5,
                            next_run_time = $6
                        WHERE id = $7
                        """,
                        'active', end_time, total_runs, success_runs, avg_duration, next_run, scheduled_task.id
                    )
                finally:
                    await conn.close()
            else:
                # 使用SQLAlchemy更新
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
            
        except KeyboardInterrupt:
            # 处理 Ctrl+C 中断
            # 注意：finally 块会在 raise 之前执行，确保锁被释放
            end_time = datetime.now()
            duration = int((end_time - start_time).total_seconds() * 1000)
            error_msg = "任务被用户中断（Ctrl+C）"
            logger.warning(f"任务执行被中断: {scheduled_task.task_name}")
            
            # 更新任务状态为错误（使用 try-except 确保即使失败也不影响 finally 执行）
            try:
                if is_opengauss():
                    conn = await get_opengauss_connection()
                    try:
                        await conn.execute(
                            """
                            UPDATE scheduled_tasks
                            SET status = $1::scheduledtaskstatus,
                                last_error = $2
                            WHERE id = $3
                            """,
                            'error', error_msg, scheduled_task.id
                        )
                    finally:
                        await conn.close()
            except Exception as update_error:
                logger.error(f"更新任务状态失败（忽略继续）: {str(update_error)}")
            
            # 重新抛出异常，让上层处理
            # finally 块会在 raise 之前执行
            raise
            
        except Exception as e:
            end_time = datetime.now()
            duration = int((end_time - start_time).total_seconds() * 1000)  # 转换为毫秒
            error_msg = str(e)
            import traceback
            stack_trace = traceback.format_exc()
            
            # 记录详细的错误信息，包括堆栈跟踪
            logger.error(f"任务执行失败 {scheduled_task.task_name}: {error_msg}")
            logger.error(f"异常类型: {type(e).__name__}")
            logger.error(f"异常堆栈:\n{stack_trace}")
            
            # 更新执行日志和任务统计（openGauss使用原生SQL，其他使用SQLAlchemy）
            if is_opengauss():
                # openGauss 原生记录结束（失败）
                try:
                    await record_run_end(execution_id, end_time, 'failed', result=None, error_message=error_msg)
                except Exception:
                    pass
                
                # 更新任务统计（使用原生SQL）
                try:
                    conn = await get_opengauss_connection()
                    try:
                        # 获取当前统计值
                        current_task = await conn.fetchrow(
                            """
                            SELECT total_runs, failure_runs
                            FROM scheduled_tasks
                            WHERE id = $1
                            """,
                            scheduled_task.id
                        )
                        
                        total_runs = (current_task['total_runs'] or 0) + 1
                        failure_runs = (current_task['failure_runs'] or 0) + 1
                        
                        # 更新任务
                        await conn.execute(
                            """
                            UPDATE scheduled_tasks
                            SET status = $1::scheduledtaskstatus,
                                total_runs = $2,
                                failure_runs = $3,
                                last_failure_time = $4,
                                last_error = $5
                            WHERE id = $6
                            """,
                            'error', total_runs, failure_runs, end_time, error_msg, scheduled_task.id
                        )
                    finally:
                        await conn.close()
                except Exception as db_error:
                    logger.error(f"更新任务日志失败: {str(db_error)}")
            else:
                # 使用SQLAlchemy更新
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
        
        finally:
            # 确保无论成功还是失败，都释放任务锁
            # 这个 finally 块会在所有退出路径（return、raise、正常结束）之前执行
            if lock_acquired:
                try:
                    await release_task_lock(scheduled_task.id, execution_id)
                    logger.debug(f"任务锁已释放: {scheduled_task.task_name} (执行ID: {execution_id})")
                except Exception as lock_error:
                    # 即使释放锁失败，也要记录错误，但不影响其他流程
                    logger.error(f"释放任务锁失败: {scheduled_task.task_name} (执行ID: {execution_id}), 错误: {str(lock_error)}")
                    # 注意：即使释放锁失败，我们也不应该重新抛出异常，因为这会掩盖原始错误
    
    return executor

