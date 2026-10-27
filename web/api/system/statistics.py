#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
系统管理API - statistics
System Management API - statistics
"""

import logging
from typing import Dict, Any, Optional, List
from datetime import datetime, timedelta
from fastapi import APIRouter, HTTPException, Request
from pydantic import BaseModel, Field

from utils.scheduler.db_utils import get_opengauss_connection

logger = logging.getLogger(__name__)
router = APIRouter()

@router.get("/statistics")
async def get_system_statistics(request: Request):
    """获取系统统计信息（从数据库查询真实数据）"""
    try:
        system = request.app.state.system
        
        # 1. 备份任务统计
        backup_tasks_stats = await _get_backup_tasks_statistics()
        
        # 2. 磁带库存统计
        tape_inventory_stats = await _get_tape_inventory_statistics()
        
        # 3. 存储统计
        storage_stats = await _get_storage_statistics()
        
        # 4. 最近备份活动
        recent_backups = await _get_recent_backups(limit=5)
        
        # 5. 存储使用趋势（最近12个月按月统计）
        storage_trend = await _get_storage_trend(days=365)
        
        # 6. 成功率统计
        success_rate_stats = await _get_success_rate_statistics()
        
        # 7. 系统运行时间
        uptime = await _get_system_uptime()
        
        return {
            "uptime": uptime,
            "backup_tasks": backup_tasks_stats,
            "tape_inventory": tape_inventory_stats,
            "storage": storage_stats,
            "recent_backups": recent_backups,
            "storage_trend": storage_trend,
            "success_rate": success_rate_stats
        }

    except Exception as e:
        logger.error(f"获取系统统计信息失败: {str(e)}")
        import traceback
        logger.error(traceback.format_exc())
        raise HTTPException(status_code=500, detail=str(e))


async def _get_backup_tasks_statistics() -> Dict[str, Any]:
    """获取备份任务统计"""
    try:
        async with get_opengauss_connection() as conn:
            # 统计运行中的任务
            running_count = await conn.fetchval(
                "SELECT COUNT(*) FROM backup_tasks WHERE LOWER(status::text) = LOWER('RUNNING') AND is_template = FALSE"
            ) or 0

            # 统计已完成的任务
            completed_count = await conn.fetchval(
                "SELECT COUNT(*) FROM backup_tasks WHERE LOWER(status::text) = LOWER('COMPLETED') AND is_template = FALSE"
            ) or 0

            # 统计失败的任务
            failed_count = await conn.fetchval(
                "SELECT COUNT(*) FROM backup_tasks WHERE LOWER(status::text) = LOWER('FAILED') AND is_template = FALSE"
            ) or 0

            # 总任务数（执行记录，不包括模板）
            total_count = await conn.fetchval(
                "SELECT COUNT(*) FROM backup_tasks WHERE is_template = FALSE"
            ) or 0

            return {
                "total": total_count,
                "running": running_count,
                "completed": completed_count,
                "failed": failed_count
            }
    except Exception as e:
        logger.error(f"获取备份任务统计失败: {str(e)}")
        return {"total": 0, "running": 0, "completed": 0, "failed": 0}


async def _get_tape_inventory_statistics() -> Dict[str, Any]:
    """获取磁带库存统计"""
    try:
        async with get_opengauss_connection() as conn:
            total_count = await conn.fetchval("SELECT COUNT(*) FROM tape_cartridges") or 0
            available_count = await conn.fetchval(
                "SELECT COUNT(*) FROM tape_cartridges WHERE LOWER(status::text) = LOWER('AVAILABLE')"
            ) or 0
            in_use_count = await conn.fetchval(
                "SELECT COUNT(*) FROM tape_cartridges WHERE LOWER(status::text) = LOWER('IN_USE')"
            ) or 0
            expired_count = await conn.fetchval(
                "SELECT COUNT(*) FROM tape_cartridges WHERE LOWER(status::text) = LOWER('EXPIRED')"
            ) or 0

            # 计算在线和离线数量（可用+使用中=在线，其他=离线）
            online_count = available_count + in_use_count
            offline_count = total_count - online_count

            return {
                "total": total_count,
                "available": available_count,
                "in_use": in_use_count,
                "expired": expired_count,
                "online": online_count,
                "offline": offline_count
            }
    except Exception as e:
        logger.error(f"获取磁带库存统计失败: {str(e)}")
        return {"total": 0, "available": 0, "in_use": 0, "expired": 0, "online": 0, "offline": 0}


async def _get_storage_statistics() -> Dict[str, Any]:
    """获取存储统计（仅统计保留月数内、成功完成的备份任务）"""
    try:
        # 计算保留期截止日（默认保留月数）
        from config.settings import get_settings
        retention_months = int(getattr(get_settings(), 'DEFAULT_RETENTION_MONTHS', 6) or 6)
        now = datetime.now()
        year = now.year + (now.month - retention_months - 1) // 12
        month = (now.month - retention_months - 1) % 12 + 1
        retention_cutoff = now.replace(year=year, month=month, day=1)

        async with get_opengauss_connection() as conn:
            # 总容量仍取磁带库实际容量（仅用于计算使用百分比）
            tape_result = await conn.fetchrow(
                """
                SELECT
                    COALESCE(SUM(capacity_bytes), 0) as total_capacity,
                    COALESCE(SUM(used_bytes), 0) as used_capacity
                FROM tape_cartridges
                """
            )
            total_capacity = tape_result['total_capacity'] or 0
            tape_used_bytes = tape_result['used_capacity'] or 0

            # 已用存储：保留期内、成功完成任务的备份集压缩后总量
            set_result = await conn.fetchrow(
                """
                SELECT COALESCE(SUM(s.compressed_bytes), 0) as total_compressed
                FROM backup_sets s
                LEFT JOIN backup_tasks t ON s.backup_task_id = t.id
                WHERE s.backup_time >= $1
                  AND LOWER(t.status::text) = 'completed'
                """,
                retention_cutoff
            )
            compressed_from_sets = set_result['total_compressed'] or 0

            if compressed_from_sets > 0:
                used_capacity = compressed_from_sets
            else:
                # 回退：保留期内、成功完成任务的原始总量
                task_result = await conn.fetchrow(
                    """
                    SELECT COALESCE(SUM(total_bytes), 0) as total_bytes
                    FROM backup_tasks
                    WHERE is_template = FALSE
                      AND started_at >= $1
                      AND total_bytes > 0
                      AND LOWER(status::text) = 'completed'
                    """,
                    retention_cutoff
                )
                used_capacity = task_result['total_bytes'] or 0

            # 统计数据不完整时，回退到磁带库实际写入量（物理用量）
            if used_capacity == 0:
                used_capacity = tape_used_bytes

            # 如果没有磁带容量数据，使用已用存储估算
            if total_capacity == 0:
                total_capacity = used_capacity * 2 if used_capacity > 0 else 0

            usage_percent = (used_capacity / total_capacity * 100) if total_capacity > 0 else 0

            return {
                "total_capacity": total_capacity,
                "used_capacity": used_capacity,
                "usage_percent": round(usage_percent, 1),
                "retention_months": retention_months,
            }
    except Exception as e:
        logger.error(f"获取存储统计失败: {str(e)}")
        return {"total_capacity": 0, "used_capacity": 0, "usage_percent": 0.0}


async def _get_recent_backups(limit: int = 5) -> List[Dict[str, Any]]:
    """获取最近备份活动"""
    try:
        async with get_opengauss_connection() as conn:
            rows = await conn.fetch(
                """
                SELECT id, task_name, task_type, status, total_bytes, started_at, completed_at, error_message
                FROM backup_tasks
                WHERE is_template = FALSE
                ORDER BY COALESCE(started_at, created_at) DESC
                LIMIT $1
                """,
                limit
            )

            backups = []
            for row in rows:
                status = row['status'].value if hasattr(row['status'], 'value') else str(row['status'])
                task_type = row['task_type'].value if hasattr(row['task_type'], 'value') else str(row['task_type'])

                backups.append({
                    "id": row['id'],
                    "task_name": row['task_name'],
                    "task_type": task_type,
                    "status": status,
                    "size_bytes": row['total_bytes'] or 0,
                    "started_at": row['started_at'].isoformat() if row['started_at'] else None,
                    "completed_at": row['completed_at'].isoformat() if row['completed_at'] else None,
                    "error_message": row['error_message']
                })

            return backups
    except Exception as e:
        logger.error(f"获取最近备份活动失败: {str(e)}")
        return []


async def _get_storage_trend(days: int = 365) -> List[Dict[str, Any]]:
    """获取存储使用趋势（按月统计备份量，按**全量/原始大小**）。

    口径是"每月备份了多少数据"，不是压缩后的占用。
    历史的 backup_sets.total_bytes 没有回写（旧月份全 0），
    所以以 backup_tasks.total_bytes 为主，backup_sets 仅作该月缺失时的兜底。
    """
    try:
        async with get_opengauss_connection() as conn:
            start_date = datetime.now() - timedelta(days=days)

            # 主数据源：各月全量大小
            task_rows = await conn.fetch(
                """
                SELECT
                    DATE_TRUNC('month', started_at)::date as backup_month,
                    SUM(total_bytes) as monthly_bytes
                FROM backup_tasks
                WHERE is_template = FALSE
                  AND started_at >= $1
                  AND total_bytes > 0
                  AND LOWER(status::text) NOT IN ('cancelled', 'interrupted')
                GROUP BY DATE_TRUNC('month', started_at)
                ORDER BY backup_month ASC
                """,
                start_date
            )

            # 兜底：backup_tasks 该月没有记录时用备份集的全量大小
            set_rows = await conn.fetch(
                """
                SELECT
                    DATE_TRUNC('month', s.backup_time)::date as backup_month,
                    SUM(s.total_bytes) as monthly_bytes
                FROM backup_sets s
                LEFT JOIN backup_tasks t ON s.backup_task_id = t.id
                WHERE s.backup_time >= $1
                  AND s.total_bytes > 0
                  AND (t.status IS NULL OR LOWER(t.status::text) NOT IN ('cancelled', 'interrupted'))
                GROUP BY DATE_TRUNC('month', s.backup_time)
                ORDER BY backup_month ASC
                """,
                start_date
            )

            def _key(v):
                return v.isoformat() if isinstance(v, datetime) else str(v)

            merged: Dict[str, int] = {}
            for row in set_rows:           # 先铺底：备份集全量
                merged[_key(row['backup_month'])] = int(row['monthly_bytes'] or 0)
            for row in task_rows:          # 任务表为准（同一份数据的源头）
                merged[_key(row['backup_month'])] = int(row['monthly_bytes'] or 0)

            return [{"date": k, "bytes": merged[k]} for k in sorted(merged)]
    except Exception as e:
        logger.error(f"获取存储使用趋势失败: {str(e)}")
        return []


async def _get_success_rate_statistics() -> Dict[str, Any]:
    """获取成功率统计（已完成的任务：completed=成功, failed/cancelled=未成功）"""
    try:
        async with get_opengauss_connection() as conn:
            # 已结束的任务状态（排除 running/pending 等进行中状态，
            # 也排除 cancelled/interrupted 等被停止的任务——停止的任务不计入成功率与失败统计）
            finished_statuses = "('completed', 'failed')"

            total = await conn.fetchval(
                f"SELECT COUNT(*) FROM backup_tasks WHERE is_template = FALSE AND LOWER(status::text) IN {finished_statuses}"
            ) or 0

            success = await conn.fetchval(
                "SELECT COUNT(*) FROM backup_tasks WHERE is_template = FALSE AND LOWER(status::text) = 'completed'"
            ) or 0

            # 本月统计
            this_month_start = datetime.now().replace(day=1, hour=0, minute=0, second=0, microsecond=0)
            this_month_total = await conn.fetchval(
                f"""
                SELECT COUNT(*) FROM backup_tasks
                WHERE is_template = FALSE
                  AND LOWER(status::text) IN {finished_statuses}
                  AND started_at >= $1
                """,
                this_month_start
            ) or 0

            this_month_success = await conn.fetchval(
                """
                SELECT COUNT(*) FROM backup_tasks
                WHERE is_template = FALSE
                  AND LOWER(status::text) = 'completed'
                  AND started_at >= $1
                """,
                this_month_start
            ) or 0

            # 上月统计
            last_month_start = (this_month_start - timedelta(days=1)).replace(day=1)
            last_month_total = await conn.fetchval(
                f"""
                SELECT COUNT(*) FROM backup_tasks
                WHERE is_template = FALSE
                  AND LOWER(status::text) IN {finished_statuses}
                  AND started_at >= $1 AND started_at < $2
                """,
                last_month_start, this_month_start
            ) or 0

            last_month_success = await conn.fetchval(
                """
                SELECT COUNT(*) FROM backup_tasks
                WHERE is_template = FALSE
                  AND LOWER(status::text) = 'completed'
                  AND started_at >= $1 AND started_at < $2
                """,
                last_month_start, this_month_start
            ) or 0

            success_rate = (success / total * 100) if total > 0 else 0
            this_month_rate = (this_month_success / this_month_total * 100) if this_month_total > 0 else 0
            last_month_rate = (last_month_success / last_month_total * 100) if last_month_total > 0 else 0

            rate_change = this_month_rate - last_month_rate

            return {
                "overall": round(success_rate, 1),
                "this_month": round(this_month_rate, 1),
                "last_month": round(last_month_rate, 1),
                "change": round(rate_change, 1),
                "this_month_count": this_month_success,
                "last_month_count": last_month_success
            }
    except Exception as e:
        logger.error(f"获取成功率统计失败: {str(e)}")
        return {"overall": 0.0, "this_month": 0.0, "last_month": 0.0, "change": 0.0, "this_month_count": 0, "last_month_count": 0}


async def _get_system_uptime() -> int:
    """获取系统运行时间（秒）"""
    try:
        from web.api.system.info import _SYSTEM_START_TIME
        delta = datetime.now() - _SYSTEM_START_TIME
        return int(delta.total_seconds())
    except Exception as e:
        logger.error(f"获取系统运行时间失败: {str(e)}")
        return 0

