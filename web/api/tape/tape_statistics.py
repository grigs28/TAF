#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
磁带管理API - tape_statistics
Tape Management API - tape_statistics
"""

import logging
import traceback
import json
import re
import os
import asyncio
from typing import List, Dict, Any, Optional
from datetime import datetime
from fastapi import APIRouter, HTTPException, Request, Depends, BackgroundTasks
from pydantic import BaseModel

from .models import CreateTapeRequest, UpdateTapeRequest
from .tape_utils import normalize_tape_label, check_tape_exists_sqlite, count_serial_numbers_sqlite, parse_expiry_date_for_inventory
from models.system_log import OperationType, LogCategory, LogLevel
from utils.log_utils import log_operation, log_system
from utils.scheduler.db_utils import is_opengauss, get_opengauss_connection
from utils.tape_tools import tape_tools_manager
from config.database import db_manager

logger = logging.getLogger(__name__)
router = APIRouter()


def parse_expiry_date_for_inventory(expiry_date):
    """解析过期日期（用于库存统计）"""
    from datetime import date, datetime
    if isinstance(expiry_date, date):
        return expiry_date
    if isinstance(expiry_date, datetime):
        return expiry_date.date()
    if isinstance(expiry_date, str):
        try:
            dt = datetime.fromisoformat(expiry_date.replace('Z', '+00:00'))
            return dt.date()
        except:
            return date.today()
    return date.today()


async def check_tape_exists_sqlite(db_manager, tape_id: str, label: str) -> tuple[bool, bool]:
    """检查磁带是否存在（SQLite版本）"""
    from utils.scheduler.sqlite_utils import get_sqlite_connection
    
    async with get_sqlite_connection() as conn:
        # 检查 tape_id
        cursor = await conn.execute("SELECT COUNT(*) FROM tape_cartridges WHERE tape_id = ?", (tape_id,))
        row = await cursor.fetchone()
        tape_exists = (row[0] > 0) if row else False
        
        # 检查 label
        cursor = await conn.execute("SELECT COUNT(*) FROM tape_cartridges WHERE label = ?", (label,))
        row = await cursor.fetchone()
        label_exists = (row[0] > 0) if row else False
        
        return tape_exists, label_exists


async def count_serial_numbers_sqlite(db_manager, pattern: str) -> int:
    """统计序列号数量（SQLite版本）"""
    from utils.scheduler.sqlite_utils import get_sqlite_connection, is_sqlite
    from utils.scheduler.db_utils import is_redis
    
    # 检查数据库类型
    if is_redis():
        raise ValueError("Redis模式下不支持磁带管理功能")
    if not is_sqlite():
        raise ValueError("当前数据库类型不支持磁带管理功能")
    
    async with get_sqlite_connection() as conn:
        cursor = await conn.execute("""
            SELECT COUNT(*) FROM tape_cartridges
            WHERE serial_number IS NOT NULL AND serial_number LIKE ?
        """, (pattern,))
        row = await cursor.fetchone()
        return row[0] if row else 0


def normalize_tape_label(label: Optional[str], year: int, month: int) -> str:
    target_year = f"{year:04d}"
    target_month = f"{month:02d}"
    default_seq = "01"
    default_label = f"TP{target_year}{target_month}{default_seq}"

    if not label:
        return default_label

    clean_label = label.strip().upper()

    def build_label(seq: str, suffix: str = "") -> str:
        seq = (seq if seq and seq.isdigit() else default_seq).zfill(2)[:2]
        return f"TP{target_year}{target_month}{seq}{suffix}"

    match = re.match(r'^TP(\d{4})(\d{2})(\d{2})(.*)$', clean_label)
    if match:
        return build_label(match.group(3), match.group(4))

    match = re.match(r'^TP(\d{4})(\d{2})(\d+)(.*)$', clean_label)
    if match:
        return build_label(match.group(3), match.group(4))

    match = re.match(r'^TAPE(\d{4})(\d{2})(\d{2})(.*)$', clean_label)
    if match:
        return build_label(match.group(3), match.group(4))

    match = re.match(r'^TAPE(\d{4})(\d{2})(\d+)(.*)$', clean_label)
    if match:
        return build_label(match.group(3), match.group(4))

    match = re.search(r'(\d{4})(\d{2})(\d{2})', clean_label)
    if match:
        return build_label(match.group(3))

    return default_label



@router.get("/inventory")
async def get_tape_inventory(request: Request):
    """获取磁带库存统计（从数据库获取真实数据）"""
    start_time = datetime.now()
    try:
        from config.settings import get_settings
        from datetime import date
        from utils.scheduler.db_utils import is_redis
        
        settings = get_settings()
        database_url = settings.DATABASE_URL
        
        # 检查是否为 Redis
        if is_redis():
            # Redis模式：使用Redis查询磁带库存
            from backup.redis_tape_db import list_tapes_redis
            
            tapes_redis = await list_tapes_redis()
            
            total_tapes = len(tapes_redis)
            available_tapes = sum(1 for t in tapes_redis if t.get('status', '').upper() == 'AVAILABLE')
            in_use_tapes = sum(1 for t in tapes_redis if t.get('status', '').upper() == 'IN_USE')
            full_tapes = sum(1 for t in tapes_redis if t.get('status', '').upper() == 'FULL')
            expired_tapes = sum(1 for t in tapes_redis if t.get('expiry_date') and parse_expiry_date_for_inventory(t.get('expiry_date')) < date.today())
            error_tapes = sum(1 for t in tapes_redis if t.get('status', '').upper() == 'ERROR')
            excellent_tapes = sum(1 for t in tapes_redis if (t.get('health_score') or 0) >= 80)
            good_tapes = sum(1 for t in tapes_redis if 60 <= (t.get('health_score') or 0) < 80)
            warning_tapes = sum(1 for t in tapes_redis if 40 <= (t.get('health_score') or 0) < 60)
            critical_tapes = sum(1 for t in tapes_redis if (t.get('health_score') or 0) < 40)
            total_capacity_bytes = sum(t.get('capacity_bytes', 0) or 0 for t in tapes_redis)
            total_used_bytes = sum(t.get('used_bytes', 0) or 0 for t in tapes_redis)
            total_available_bytes = total_capacity_bytes - total_used_bytes
            
            inventory = {
                "total_tapes": total_tapes,
                "available_tapes": available_tapes,
                "in_use_tapes": in_use_tapes,
                "full_tapes": full_tapes,
                "expired_tapes": expired_tapes,
                "error_tapes": error_tapes,
                "excellent_tapes": excellent_tapes,
                "good_tapes": good_tapes,
                "warning_tapes": warning_tapes,
                "critical_tapes": critical_tapes,
                "total_capacity_bytes": total_capacity_bytes,
                "total_used_bytes": total_used_bytes,
                "total_available_bytes": total_available_bytes,
                "usage_percent": (total_used_bytes / total_capacity_bytes * 100) if total_capacity_bytes > 0 else 0
            }
            
            duration_ms = int((datetime.now() - start_time).total_seconds() * 1000)
            await log_system(
                level=LogLevel.INFO,
                category=LogCategory.TAPE,
                message=f"[Redis模式] 获取磁带库存统计成功: 总计 {total_tapes} 个磁带",
                module="web.api.tape.crud",
                function="get_tape_inventory",
                duration_ms=duration_ms
            )
            return inventory
        
        # 检查是否为 SQLite
        is_sqlite = database_url.startswith("sqlite:///") or database_url.startswith("sqlite+aiosqlite:///")
        
        if is_sqlite:
            # 使用原生SQL查询 SQLite
            from utils.scheduler.sqlite_utils import get_sqlite_connection
            
            async with get_sqlite_connection() as conn:
                cursor = await conn.execute("SELECT * FROM tape_cartridges")
                rows = await cursor.fetchall()
                columns = [desc[0] for desc in cursor.description] if cursor.description else []
                tapes = [dict(zip(columns, row)) for row in rows]
                
                # 计算统计信息
                from models.tape import TapeStatus
                from datetime import date
                
                total_tapes = len(tapes)
                available_tapes = sum(1 for t in tapes if (t.get('status') == TapeStatus.AVAILABLE.value if isinstance(t.get('status'), str) else t.get('status') == TapeStatus.AVAILABLE))
                in_use_tapes = sum(1 for t in tapes if (t.get('status') == TapeStatus.IN_USE.value if isinstance(t.get('status'), str) else t.get('status') == TapeStatus.IN_USE))
                full_tapes = sum(1 for t in tapes if (t.get('status') == TapeStatus.FULL.value if isinstance(t.get('status'), str) else t.get('status') == TapeStatus.FULL))
                expired_tapes = sum(1 for t in tapes if t.get('expiry_date') and (t.get('expiry_date').date() if hasattr(t.get('expiry_date'), 'date') else date.fromisoformat(str(t.get('expiry_date'))[:10])) < date.today())
                error_tapes = sum(1 for t in tapes if (t.get('status') == TapeStatus.ERROR.value if isinstance(t.get('status'), str) else t.get('status') == TapeStatus.ERROR))
                excellent_tapes = sum(1 for t in tapes if (t.get('health_score') or 0) >= 80)
                good_tapes = sum(1 for t in tapes if 60 <= (t.get('health_score') or 0) < 80)
                warning_tapes = sum(1 for t in tapes if 40 <= (t.get('health_score') or 0) < 60)
                critical_tapes = sum(1 for t in tapes if (t.get('health_score') or 0) < 40)
                total_capacity_bytes = sum(t.get('capacity_bytes') or 0 for t in tapes)
                total_used_bytes = sum(t.get('used_bytes') or 0 for t in tapes)
                total_available_bytes = total_capacity_bytes - total_used_bytes
                
                inventory = {
                    "total_tapes": total_tapes,
                    "available_tapes": available_tapes,
                    "in_use_tapes": in_use_tapes,
                    "full_tapes": full_tapes,
                    "expired_tapes": expired_tapes,
                    "error_tapes": error_tapes,
                    "excellent_tapes": excellent_tapes,
                    "good_tapes": good_tapes,
                    "warning_tapes": warning_tapes,
                    "critical_tapes": critical_tapes,
                    "total_capacity_bytes": total_capacity_bytes,
                    "total_used_bytes": total_used_bytes,
                    "total_available_bytes": total_available_bytes,
                    "usage_percent": (total_used_bytes / total_capacity_bytes * 100) if total_capacity_bytes > 0 else 0
                }
        else:
            # 使用 psycopg2 查询 PostgreSQL/openGauss
            import psycopg2
            
            # 解析URL
            if database_url.startswith("opengauss://"):
                database_url = database_url.replace("opengauss://", "postgresql://", 1)
            
            pattern = r'postgresql://([^:]+):([^@]+)@([^:]+):(\d+)/(.+)'
            match = re.match(pattern, database_url)
            
            if not match:
                raise ValueError("无法解析数据库连接URL")
            
            username, password, host, port, database = match.groups()
            
            # 连接数据库
            conn = psycopg2.connect(
                host=host,
                port=port,
                user=username,
                password=password,
                database=database
            )
            
            try:
                with conn.cursor() as cur:
                    # 从数据库获取真实统计数据
                    # 注意：available_tapes 只统计 AVAILABLE 状态，排除 MAINTENANCE（格式化中）状态
                    cur.execute("""
                        SELECT 
                            COUNT(*) as total_tapes,
                            COUNT(*) FILTER (WHERE status = 'AVAILABLE') as available_tapes,
                            COUNT(*) FILTER (WHERE status = 'IN_USE') as in_use_tapes,
                            COUNT(*) FILTER (WHERE status = 'FULL') as full_tapes,
                            COUNT(*) FILTER (WHERE expiry_date < CURRENT_DATE) as expired_tapes,
                            COUNT(*) FILTER (WHERE status = 'ERROR') as error_tapes,
                            COUNT(*) FILTER (WHERE health_score >= 80) as excellent_tapes,
                            COUNT(*) FILTER (WHERE health_score >= 60 AND health_score < 80) as good_tapes,
                            COUNT(*) FILTER (WHERE health_score >= 40 AND health_score < 60) as warning_tapes,
                            COUNT(*) FILTER (WHERE health_score < 40) as critical_tapes,
                            COALESCE(SUM(capacity_bytes), 0) as total_capacity_bytes,
                            COALESCE(SUM(used_bytes), 0) as total_used_bytes
                        FROM tape_cartridges
                    """)
                    
                    row = cur.fetchone()
                    
                    if row:
                        total_capacity_bytes = row[10] or 0
                        total_used_bytes = row[11] or 0
                        total_available_bytes = total_capacity_bytes - total_used_bytes
                        
                        inventory = {
                            "total_tapes": row[0] or 0,
                            "available_tapes": row[1] or 0,
                            "in_use_tapes": row[2] or 0,
                            "full_tapes": row[3] or 0,
                            "expired_tapes": row[4] or 0,
                            "error_tapes": row[5] or 0,
                            "excellent_tapes": row[6] or 0,
                            "good_tapes": row[7] or 0,
                            "warning_tapes": row[8] or 0,
                            "critical_tapes": row[9] or 0,
                            "total_capacity_bytes": total_capacity_bytes,
                            "total_used_bytes": total_used_bytes,
                            "total_available_bytes": total_available_bytes,
                            "usage_percent": (total_used_bytes / total_capacity_bytes * 100) if total_capacity_bytes > 0 else 0
                        }
                    else:
                        # 如果没有数据，返回空统计
                        inventory = {
                            "total_tapes": 0,
                            "available_tapes": 0,
                            "in_use_tapes": 0,
                            "full_tapes": 0,
                            "expired_tapes": 0,
                            "error_tapes": 0,
                            "excellent_tapes": 0,
                            "good_tapes": 0,
                            "warning_tapes": 0,
                            "critical_tapes": 0,
                            "total_capacity_bytes": 0,
                            "total_used_bytes": 0,
                            "total_available_bytes": 0,
                            "usage_percent": 0
                        }
            
            finally:
                conn.close()
        
        duration_ms = int((datetime.now() - start_time).total_seconds() * 1000)
        await log_system(
            level=LogLevel.INFO,
            category=LogCategory.TAPE,
            message=f"获取磁带库存统计成功: 总计 {inventory['total_tapes']} 个磁带",
            module="web.api.tape.crud",
            function="get_tape_inventory",
            duration_ms=duration_ms
        )
        
        return inventory

    except Exception as e:
        duration_ms = int((datetime.now() - start_time).total_seconds() * 1000)
        error_msg = f"获取磁带库存失败: {str(e)}"
        logger.error(error_msg, exc_info=True)
        await log_system(
            level=LogLevel.ERROR,
            category=LogCategory.TAPE,
            message=error_msg,
            module="web.api.tape.crud",
            function="get_tape_inventory",
            exception_type=type(e).__name__,
            stack_trace=traceback.format_exc(),
            duration_ms=duration_ms
        )
        raise HTTPException(status_code=500, detail=str(e))


