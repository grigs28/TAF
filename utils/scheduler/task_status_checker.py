#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
计划任务状态检查器 - SQLite 支持
Scheduled Task Status Checker - SQLite Support
"""

import logging
from typing import Optional
from config.database import db_manager
from models.scheduled_task import ScheduledTask, ScheduledTaskStatus
from utils.scheduler.db_utils import is_opengauss, get_opengauss_connection

logger = logging.getLogger(__name__)


async def check_task_status(task_id: int) -> Optional[ScheduledTaskStatus]:
    """从数据库重新加载任务状态（支持 SQLite）"""
    try:
        if is_opengauss():
            async with get_opengauss_connection() as conn:
                row = await conn.fetchrow(
                    "SELECT status FROM scheduled_tasks WHERE id = $1",
                    task_id
                )
                if row:
                    status_str = row['status']
                    if isinstance(status_str, str):
                        # 尝试解析枚举值
                        try:
                            return ScheduledTaskStatus(status_str.lower())
                        except ValueError:
                            logger.warning(f"无法解析任务状态: {status_str}")
                            return None
                    return status_str
                return None
        else:
            # 使用 SQLAlchemy 查询 SQLite
            from sqlalchemy import select
            async with db_manager.AsyncSessionLocal() as session:
                stmt = select(ScheduledTask).where(ScheduledTask.id == task_id)
                result = await session.execute(stmt)
                task = result.scalar_one_or_none()
                if task:
                    return task.status
                return None
    except Exception as e:
        logger.error(f"检查任务状态失败: {str(e)}", exc_info=True)
        return None


async def is_task_running(task_id: int) -> bool:
    """检查任务是否正在运行（支持 SQLite）"""
    status = await check_task_status(task_id)
    if status is None:
        return False
    return status == ScheduledTaskStatus.RUNNING

