#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
备份统计 API - SQLite 支持
Backup Statistics API - SQLite Support
"""

import logging
from typing import Dict, Any
from datetime import datetime, timedelta
from utils.scheduler.sqlite_utils import get_sqlite_connection
from models.backup import BackupTask, BackupTaskStatus
from models.scheduled_task import ScheduledTask, TaskActionType
from utils.scheduler.db_utils import is_opengauss, get_opengauss_connection

logger = logging.getLogger(__name__)


async def get_backup_statistics() -> Dict[str, Any]:
    """获取备份统计信息（支持 SQLite）"""
    try:
        if is_opengauss():
            return await _get_backup_statistics_opengauss()
        else:
            return await _get_backup_statistics_sqlite()
    except Exception as e:
        logger.error(f"获取备份统计信息失败: {str(e)}", exc_info=True)
        raise


async def _get_backup_statistics_opengauss() -> Dict[str, Any]:
    """获取备份统计信息（openGauss 版本）"""
    async with get_opengauss_connection() as conn:
        # 总任务数（包含模板与执行记录）
        total_row = await conn.fetchrow("SELECT COUNT(*) as total FROM backup_tasks")
        total_tasks = total_row["total"] if total_row else 0
        
        # 计划任务中的备份任务数量（计入总任务与pending）
        sched_total_row = await conn.fetchrow(
            "SELECT COUNT(*) as total FROM scheduled_tasks WHERE LOWER(action_type::text)=LOWER('BACKUP')"
        )
        sched_total = sched_total_row["total"] if sched_total_row else 0
        total_tasks += sched_total
        
        # 按状态统计（执行记录与模板均统计各自status）
        completed_row = await conn.fetchrow(
            "SELECT COUNT(*) as total FROM backup_tasks WHERE is_template = false AND LOWER(status::text)=LOWER($1)",
            BackupTaskStatus.COMPLETED.value
        )
        completed_tasks = completed_row["total"] if completed_row else 0
        
        failed_row = await conn.fetchrow(
            "SELECT COUNT(*) as total FROM backup_tasks WHERE is_template = false AND LOWER(status::text)=LOWER($1)",
            BackupTaskStatus.FAILED.value
        )
        failed_tasks = failed_row["total"] if failed_row else 0
        
        running_row = await conn.fetchrow(
            "SELECT COUNT(*) as total FROM backup_tasks WHERE is_template = false AND LOWER(status::text)=LOWER($1)",
            BackupTaskStatus.RUNNING.value
        )
        running_tasks = running_row["total"] if running_row else 0
        
        pending_row = await conn.fetchrow(
            "SELECT COUNT(*) as total FROM backup_tasks WHERE LOWER(status::text)=LOWER($1)",
            BackupTaskStatus.PENDING.value
        )
        pending_tasks = (pending_row["total"] if pending_row else 0) + sched_total
        
        # 成功率
        success_rate = (completed_tasks / total_tasks * 100) if total_tasks > 0 else 0.0
        
        # 总备份数据量
        bytes_row = await conn.fetchrow(
            """
            SELECT COALESCE(SUM(processed_bytes), 0) as total 
            FROM backup_tasks 
            WHERE is_template = false AND LOWER(status::text)=LOWER($1)
            """,
            BackupTaskStatus.COMPLETED.value
        )
        total_data_backed_up = bytes_row["total"] if bytes_row else 0
        
        # 最近24小时统计
        twenty_four_hours_ago = datetime.now() - timedelta(hours=24)
        recent_row = await conn.fetchrow(
            """
            SELECT 
                COUNT(*) as total,
                COUNT(*) FILTER (WHERE LOWER(status::text)=LOWER($1)) as completed,
                COUNT(*) FILTER (WHERE LOWER(status::text)=LOWER($2)) as failed,
                COALESCE(SUM(processed_bytes), 0) as data
            FROM backup_tasks
            WHERE is_template = false AND created_at >= $3
            """,
            BackupTaskStatus.COMPLETED.value,
            BackupTaskStatus.FAILED.value,
            twenty_four_hours_ago
        )
        
        recent_total = recent_row["total"] if recent_row else 0
        recent_completed = recent_row["completed"] if recent_row else 0
        recent_failed = recent_row["failed"] if recent_row else 0
        recent_data = recent_row["data"] if recent_row else 0
        
        # 平均任务时长
        avg_duration_row = await conn.fetchrow(
            """
            SELECT AVG(EXTRACT(EPOCH FROM (completed_at - started_at))) as avg_duration
            FROM backup_tasks
            WHERE is_template = false 
              AND LOWER(status::text)=LOWER($1)
              AND completed_at IS NOT NULL 
              AND started_at IS NOT NULL
            """,
            BackupTaskStatus.COMPLETED.value
        )
        avg_duration = int(avg_duration_row["avg_duration"]) if avg_duration_row and avg_duration_row["avg_duration"] else 3600
        
        # 压缩比
        compression_row = await conn.fetchrow(
            """
            SELECT 
                COALESCE(SUM(processed_bytes), 0) as processed,
                COALESCE(SUM(compressed_bytes), 0) as compressed
            FROM backup_tasks
            WHERE is_template = false 
              AND LOWER(status::text)=LOWER($1)
              AND compressed_bytes > 0
            """,
            BackupTaskStatus.COMPLETED.value
        )
        if compression_row and compression_row["processed"] > 0:
            compression_ratio = float(compression_row["compressed"]) / float(compression_row["processed"])
        else:
            compression_ratio = 0.65  # 默认值
        
        return {
            "total_tasks": total_tasks,
            "completed_tasks": completed_tasks,
            "failed_tasks": failed_tasks,
            "running_tasks": running_tasks,
            "pending_tasks": pending_tasks,
            "success_rate": round(success_rate, 2),
            "total_data_backed_up": total_data_backed_up,
            "compression_ratio": round(compression_ratio, 2),
            "average_task_duration": avg_duration,
            "recent_24h": {
                "total_tasks": recent_total,
                "completed_tasks": recent_completed,
                "failed_tasks": recent_failed,
                "data_backed_up": recent_data
            }
        }


async def _get_backup_statistics_sqlite() -> Dict[str, Any]:
    """获取备份统计信息（SQLite 版本）"""
    async with get_sqlite_connection() as conn:
        # 总任务数（只查询非模板任务）
        cursor = await conn.execute("SELECT COUNT(*) FROM backup_tasks WHERE is_template = 0")
        row = await cursor.fetchone()
        total_tasks = row[0] if row else 0
        
        # 计划任务中的备份任务数量
        cursor = await conn.execute("""
            SELECT COUNT(*) FROM scheduled_tasks
            WHERE LOWER(action_type) = LOWER(?)
        """, (TaskActionType.BACKUP.value,))
        row = await cursor.fetchone()
        sched_total = row[0] if row else 0
        total_tasks += sched_total
        
        # 按状态统计
        cursor = await conn.execute("""
            SELECT COUNT(*) FROM backup_tasks
            WHERE is_template = 0 AND LOWER(status) = LOWER(?)
        """, (BackupTaskStatus.COMPLETED.value,))
        row = await cursor.fetchone()
        completed_tasks = row[0] if row else 0
        
        cursor = await conn.execute("""
            SELECT COUNT(*) FROM backup_tasks
            WHERE is_template = 0 AND LOWER(status) = LOWER(?)
        """, (BackupTaskStatus.FAILED.value,))
        row = await cursor.fetchone()
        failed_tasks = row[0] if row else 0
        
        cursor = await conn.execute("""
            SELECT COUNT(*) FROM backup_tasks
            WHERE is_template = 0 AND LOWER(status) = LOWER(?)
        """, (BackupTaskStatus.RUNNING.value,))
        row = await cursor.fetchone()
        running_tasks = row[0] if row else 0
        
        cursor = await conn.execute("""
            SELECT COUNT(*) FROM backup_tasks
            WHERE is_template = 0 AND LOWER(status) = LOWER(?)
        """, (BackupTaskStatus.PENDING.value,))
        row = await cursor.fetchone()
        pending_tasks = row[0] if row else 0
        pending_tasks += sched_total
        
        # 成功率
        success_rate = (completed_tasks / total_tasks * 100) if total_tasks > 0 else 0.0
        
        # 总备份数据量
        cursor = await conn.execute("""
            SELECT COALESCE(SUM(processed_bytes), 0) FROM backup_tasks
            WHERE is_template = 0 AND LOWER(status) = LOWER(?)
        """, (BackupTaskStatus.COMPLETED.value,))
        row = await cursor.fetchone()
        total_data_backed_up = row[0] if row else 0
        
        # 最近24小时统计
        twenty_four_hours_ago = datetime.now() - timedelta(hours=24)
        cursor = await conn.execute("""
            SELECT 
                COUNT(*) as total,
                SUM(CASE WHEN LOWER(status) = LOWER(?) THEN 1 ELSE 0 END) as completed,
                SUM(CASE WHEN LOWER(status) = LOWER(?) THEN 1 ELSE 0 END) as failed,
                COALESCE(SUM(processed_bytes), 0) as data
            FROM backup_tasks
            WHERE is_template = 0 AND created_at >= ?
        """, (BackupTaskStatus.COMPLETED.value, BackupTaskStatus.FAILED.value, twenty_four_hours_ago))
        row = await cursor.fetchone()
        
        recent_total = row[0] if row else 0
        recent_completed = row[1] if row else 0
        recent_failed = row[2] if row else 0
        recent_data = row[3] if row else 0
        
        # 平均任务时长（使用秒数计算）
        cursor = await conn.execute("""
            SELECT AVG((julianday(completed_at) - julianday(started_at)) * 86400) as avg_duration
            FROM backup_tasks
            WHERE is_template = 0 
              AND LOWER(status) = LOWER(?)
              AND completed_at IS NOT NULL
              AND started_at IS NOT NULL
        """, (BackupTaskStatus.COMPLETED.value,))
        row = await cursor.fetchone()
        avg_duration = int(row[0]) if row and row[0] else 3600
        
        # 压缩比
        cursor = await conn.execute("""
            SELECT 
                COALESCE(SUM(processed_bytes), 0) as total_bytes,
                COALESCE(SUM(compressed_bytes), 0) as compressed_bytes
            FROM backup_tasks
            WHERE is_template = 0 
              AND LOWER(status) = LOWER(?)
              AND compressed_bytes > 0
        """, (BackupTaskStatus.COMPLETED.value,))
        row = await cursor.fetchone()
        
        if row and row[0] and row[0] > 0:
            compression_ratio = float(row[1] or 0) / float(row[0])
        else:
            compression_ratio = 0.65  # 默认值
        
        return {
            "total_tasks": total_tasks,
            "completed_tasks": completed_tasks,
            "failed_tasks": failed_tasks,
            "running_tasks": running_tasks,
            "pending_tasks": pending_tasks,
            "success_rate": round(success_rate, 2),
            "total_data_backed_up": total_data_backed_up or 0,
            "compression_ratio": round(compression_ratio, 2),
            "average_task_duration": avg_duration,
            "recent_24h": {
                "total_tasks": recent_total,
                "completed_tasks": recent_completed,
                "failed_tasks": recent_failed,
                "data_backed_up": recent_data or 0
            }
        }

