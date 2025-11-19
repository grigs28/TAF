#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
计划任务存储 - SQLite 实现
Scheduled Task Storage - SQLite Implementation
"""

import logging
import json
from datetime import datetime
from typing import Dict, Any, Optional, List
from config.database import db_manager
from models.scheduled_task import ScheduledTask, ScheduledTaskStatus, ScheduledTaskLog
from sqlalchemy import select, func, desc
from utils.scheduler.sqlite_utils import get_sqlite_connection

logger = logging.getLogger(__name__)


async def load_tasks_from_db_sqlite(enabled_only: bool = True) -> List[ScheduledTask]:
    """从数据库加载计划任务（SQLite 版本）"""
    try:
        async with db_manager.AsyncSessionLocal() as session:
            stmt = select(ScheduledTask)
            if enabled_only:
                stmt = stmt.where(ScheduledTask.enabled == True)
            stmt = stmt.order_by(ScheduledTask.id)
            result = await session.execute(stmt)
            return list(result.scalars().all())
    except Exception as e:
        logger.error(f"从数据库加载任务失败: {str(e)}", exc_info=True)
        return []


async def record_run_start_sqlite(task_id: int, execution_id: str, started_at: datetime) -> None:
    """记录任务开始运行（SQLite 版本）"""
    try:
        async with db_manager.AsyncSessionLocal() as session:
            task_log = ScheduledTaskLog(
                scheduled_task_id=task_id,
                execution_id=execution_id,
                started_at=started_at,
                status='running'
            )
            session.add(task_log)
            await session.commit()
    except Exception as e:
        logger.warning(f"记录任务开始失败（忽略继续）: {str(e)}")


async def record_run_end_sqlite(
    execution_id: str,
    completed_at: datetime,
    status: str,
    result: Optional[Dict[str, Any]] = None,
    error_message: Optional[str] = None
) -> None:
    """记录任务结束（SQLite 版本）"""
    try:
        async with db_manager.AsyncSessionLocal() as session:
            stmt = select(ScheduledTaskLog).where(ScheduledTaskLog.execution_id == execution_id)
            log_result = await session.execute(stmt)
            task_log = log_result.scalar_one_or_none()
            
            if task_log:
                task_log.completed_at = completed_at
                task_log.status = status
                task_log.result = result
                task_log.error_message = error_message
                if task_log.started_at:
                    duration = (completed_at - task_log.started_at).total_seconds()
                    task_log.duration = int(duration)
                session.add(task_log)
                await session.commit()
    except Exception as e:
        logger.warning(f"记录任务结束失败（忽略继续）: {str(e)}")


async def acquire_task_lock_sqlite(task_id: int, execution_id: str) -> bool:
    """尝试获取任务锁（SQLite 版本）"""
    try:
        async with get_sqlite_connection() as conn:
            # 确保锁表存在
            await conn.execute("""
                CREATE TABLE IF NOT EXISTS task_locks (
                    task_id INTEGER PRIMARY KEY,
                    execution_id TEXT NOT NULL,
                    locked_at TIMESTAMP NOT NULL,
                    is_active INTEGER DEFAULT 1
                )
            """)
            
            # 检查是否有活跃锁
            cursor = await conn.execute("""
                SELECT task_id, is_active FROM task_locks WHERE task_id = ?
            """, (task_id,))
            row = await cursor.fetchone()
            
            if row:
                is_active = row[1] if len(row) > 1 else 0
                if is_active:
                    logger.warning(f"任务 {task_id} 的锁已被占用")
                    return False
                else:
                    # 更新失效的锁
                    await conn.execute("""
                        UPDATE task_locks
                        SET execution_id = ?, locked_at = ?, is_active = 1
                        WHERE task_id = ?
                    """, (execution_id, datetime.now(), task_id))
                    logger.info(f"任务 {task_id} 的锁已重新激活")
                    await conn.commit()
                    return True
            else:
                # 插入新锁记录
                await conn.execute("""
                    INSERT INTO task_locks (task_id, execution_id, locked_at, is_active)
                    VALUES (?, ?, ?, 1)
                """, (task_id, execution_id, datetime.now()))
                logger.info(f"任务 {task_id} 的新锁已创建")
                await conn.commit()
                return True
    except Exception as e:
        logger.warning(f"获取任务锁失败（忽略并继续）: {str(e)}")
        return True


async def release_task_lock_sqlite(task_id: int, execution_id: str) -> None:
    """释放任务锁（SQLite 版本）"""
    try:
        async with get_sqlite_connection() as conn:
            await conn.execute("""
                UPDATE task_locks
                SET is_active = 0
                WHERE task_id = ? AND execution_id = ?
            """, (task_id, execution_id))
            await conn.commit()
    except Exception as e:
        logger.warning(f"释放任务锁失败（忽略继续）: {str(e)}")


async def release_task_locks_by_task_sqlite(task_id: int) -> None:
    """释放指定任务的所有活跃锁（SQLite 版本）"""
    try:
        async with get_sqlite_connection() as conn:
            # 先查询有多少锁被释放
            cursor = await conn.execute("""
                SELECT COUNT(*) as count
                FROM task_locks
                WHERE task_id = ? AND is_active = 1
            """, (task_id,))
            row = await cursor.fetchone()
            lock_count = row[0] if row else 0
            
            # 执行解锁
            await conn.execute("""
                UPDATE task_locks
                SET is_active = 0
                WHERE task_id = ? AND is_active = 1
            """, (task_id,))
            await conn.commit()
            
            if lock_count > 0:
                logger.info(f"已释放任务 {task_id} 的 {lock_count} 个活跃锁")
            else:
                logger.info(f"任务 {task_id} 没有活跃锁需要释放")
    except Exception as e:
        logger.warning(f"释放指定任务锁失败（忽略继续）: {str(e)}")


async def release_all_active_locks_sqlite() -> None:
    """释放所有活跃的任务锁（SQLite 版本）"""
    try:
        async with get_sqlite_connection() as conn:
            # 先查询有多少锁被释放
            cursor = await conn.execute("""
                SELECT COUNT(*) as count
                FROM task_locks
                WHERE is_active = 1
            """)
            row = await cursor.fetchone()
            lock_count = row[0] if row else 0
            
            # 执行解锁
            await conn.execute("""
                UPDATE task_locks
                SET is_active = 0
                WHERE is_active = 1
            """)
            await conn.commit()
            
            if lock_count > 0:
                logger.info(f"已释放所有活跃的任务锁，共 {lock_count} 个")
            else:
                logger.info("没有活跃的任务锁需要释放")
    except Exception as e:
        logger.warning(f"释放所有任务锁失败（忽略继续）: {str(e)}")


async def get_task_by_id_sqlite(task_id: int) -> Optional[ScheduledTask]:
    """根据ID获取计划任务（SQLite 版本）"""
    try:
        async with db_manager.AsyncSessionLocal() as session:
            stmt = select(ScheduledTask).where(ScheduledTask.id == task_id)
            result = await session.execute(stmt)
            return result.scalar_one_or_none()
    except Exception as e:
        logger.error(f"获取计划任务失败: {str(e)}", exc_info=True)
        return None


async def get_all_tasks_sqlite(enabled_only: bool = False) -> List[ScheduledTask]:
    """获取所有计划任务（SQLite 版本）"""
    try:
        async with db_manager.AsyncSessionLocal() as session:
            stmt = select(ScheduledTask)
            if enabled_only:
                stmt = stmt.where(ScheduledTask.enabled == True)
            stmt = stmt.order_by(ScheduledTask.id)
            result = await session.execute(stmt)
            return list(result.scalars().all())
    except Exception as e:
        logger.error(f"获取所有计划任务失败: {str(e)}", exc_info=True)
        return []


async def add_task_sqlite(scheduled_task: ScheduledTask) -> bool:
    """添加计划任务（SQLite 版本）"""
    try:
        async with db_manager.AsyncSessionLocal() as session:
            session.add(scheduled_task)
            await session.commit()
            await session.refresh(scheduled_task)
            logger.info(f"使用SQLAlchemy插入计划任务成功: {scheduled_task.task_name} (ID: {scheduled_task.id})")
            
            # 记录操作日志
            from utils.log_utils import log_operation
            from models.system_log import OperationType
            await log_operation(
                operation_type=OperationType.SCHEDULER_CREATE,
                resource_type="scheduler",
                resource_id=str(scheduled_task.id),
                resource_name=scheduled_task.task_name,
                operation_name="创建计划任务",
                operation_description=f"创建计划任务: {scheduled_task.task_name}",
                category="scheduler",
                success=True,
                result_message=f"计划任务创建成功 (ID: {scheduled_task.id})",
                new_values={
                    "task_name": scheduled_task.task_name,
                    "description": scheduled_task.description,
                    "schedule_type": scheduled_task.schedule_type.value if scheduled_task.schedule_type else None,
                    "action_type": scheduled_task.action_type.value if scheduled_task.action_type else None,
                    "enabled": scheduled_task.enabled
                }
            )
            
            return True
    except Exception as e:
        import traceback
        error_detail = traceback.format_exc()
        logger.error(f"添加计划任务失败: {str(e)}")
        logger.error(f"错误详情:\n{error_detail}")
        
        # 记录操作日志（失败）
        from utils.log_utils import log_operation
        from models.system_log import OperationType
        task_name = getattr(scheduled_task, 'task_name', '未知任务')
        await log_operation(
            operation_type=OperationType.SCHEDULER_CREATE,
            resource_type="scheduler",
            resource_name=task_name,
            operation_name="创建计划任务",
            operation_description=f"创建计划任务失败: {task_name}",
            category="scheduler",
            success=False,
            error_message=str(e)
        )
        
        return False


async def update_task_sqlite(task_id: int, updates: Dict[str, Any], next_run_time: Optional[datetime] = None) -> Optional[ScheduledTask]:
    """更新计划任务（SQLite 版本）"""
    try:
        async with db_manager.AsyncSessionLocal() as session:
            stmt = select(ScheduledTask).where(ScheduledTask.id == task_id)
            result = await session.execute(stmt)
            task = result.scalar_one_or_none()
            
            if not task:
                logger.warning(f"未找到任务 ID: {task_id}")
                return None
            
            # 记录旧值（用于日志）
            from utils.log_utils import log_operation
            from models.system_log import OperationType
            old_values = {
                "task_name": task.task_name,
                "description": task.description,
                "schedule_type": task.schedule_type.value if task.schedule_type else None,
                "action_type": task.action_type.value if task.action_type else None,
                "enabled": task.enabled,
                "status": task.status.value if task.status else None
            }
            
            # 更新字段
            for key, value in updates.items():
                if hasattr(task, key):
                    setattr(task, key, value)
            
            # 如果提供了next_run_time，使用它
            if next_run_time is not None:
                task.next_run_time = next_run_time
            
            session.add(task)
            await session.commit()
            await session.refresh(task)
            logger.info(f"使用SQLAlchemy更新计划任务成功: {task.task_name} (ID: {task_id})")
            
            # 记录操作日志
            await log_operation(
                operation_type=OperationType.SCHEDULER_UPDATE,
                resource_type="scheduler",
                resource_id=str(task_id),
                resource_name=task.task_name,
                operation_name="更新计划任务",
                operation_description=f"更新计划任务: {task.task_name}",
                category="scheduler",
                success=True,
                result_message=f"计划任务更新成功 (ID: {task_id})",
                old_values=old_values,
                new_values={
                    "task_name": task.task_name,
                    "description": task.description,
                    "schedule_type": task.schedule_type.value if task.schedule_type else None,
                    "action_type": task.action_type.value if task.action_type else None,
                    "enabled": task.enabled,
                    "status": task.status.value if task.status else None
                },
                changed_fields=list(updates.keys())
            )
            
            return task
    except Exception as e:
        import traceback
        error_detail = traceback.format_exc()
        logger.error(f"更新计划任务失败: {str(e)}")
        logger.error(f"错误详情:\n{error_detail}")
        
        # 记录操作日志（失败）
        from utils.log_utils import log_operation
        from models.system_log import OperationType
        await log_operation(
            operation_type=OperationType.SCHEDULER_UPDATE,
            resource_type="scheduler",
            resource_id=str(task_id),
            operation_name="更新计划任务",
            operation_description=f"更新计划任务失败 (ID: {task_id})",
            category="scheduler",
            success=False,
            error_message=str(e)
        )
        
        return None


async def delete_task_sqlite(task_id: int) -> bool:
    """删除计划任务（SQLite 版本）"""
    try:
        async with db_manager.AsyncSessionLocal() as session:
            stmt = select(ScheduledTask).where(ScheduledTask.id == task_id)
            result = await session.execute(stmt)
            task = result.scalar_one_or_none()
            
            if not task:
                logger.warning(f"未找到任务 ID: {task_id}")
                return False
            
            task_name = task.task_name
            await session.delete(task)
            await session.commit()
            logger.info(f"删除计划任务成功: {task_name} (ID: {task_id})")
            
            # 记录操作日志
            from utils.log_utils import log_operation
            from models.system_log import OperationType
            await log_operation(
                operation_type=OperationType.SCHEDULER_DELETE,
                resource_type="scheduler",
                resource_id=str(task_id),
                resource_name=task_name,
                operation_name="删除计划任务",
                operation_description=f"删除计划任务: {task_name}",
                category="scheduler",
                success=True,
                result_message=f"计划任务删除成功 (ID: {task_id})"
            )
            
            return True
    except Exception as e:
        import traceback
        error_detail = traceback.format_exc()
        logger.error(f"删除计划任务失败: {str(e)}")
        logger.error(f"错误详情:\n{error_detail}")
        
        # 记录操作日志（失败）
        from utils.log_utils import log_operation
        from models.system_log import OperationType
        await log_operation(
            operation_type=OperationType.SCHEDULER_DELETE,
            resource_type="scheduler",
            resource_id=str(task_id),
            operation_name="删除计划任务",
            operation_description=f"删除计划任务失败 (ID: {task_id})",
            category="scheduler",
            success=False,
            error_message=str(e)
        )
        
        return False

