#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
磁带管理API - tape_history
Tape Management API - tape_history
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



@router.get("/history")
async def get_tape_history(request: Request, limit: int = 50, offset: int = 0):
    """获取磁带操作历史（从新的日志系统获取，使用openGauss原生SQL）"""
    start_time = datetime.now()
    try:
        from config.settings import get_settings
        from datetime import timedelta
        
        settings = get_settings()
        is_opengauss_db = False
        if hasattr(db_manager, "is_opengauss_database"):
            try:
                is_opengauss_db = db_manager.is_opengauss_database()
            except Exception:
                is_opengauss_db = False
        
        if not is_opengauss_db:
            # 检查是否为Redis数据库，Redis不支持操作日志表
            from utils.scheduler.db_utils import is_redis
            if is_redis():
                # Redis模式下不返回操作日志（Redis没有对应的表结构）
                return {"success": True, "data": [], "total": 0}
            
            # 非openGauss数据库，使用原生SQL（SQLite）
            from utils.scheduler.sqlite_utils import get_sqlite_connection, is_sqlite
            
            # 再次检查是否为SQLite
            if not is_sqlite():
                return {"success": True, "data": [], "total": 0}
            
            async with get_sqlite_connection() as conn:
                cursor = await conn.execute("""
                    SELECT * FROM operation_logs
                    WHERE resource_type = ?
                    ORDER BY operation_time DESC
                    LIMIT ? OFFSET ?
                """, ("tape", limit, offset))
                rows = await cursor.fetchall()
                columns = [desc[0] for desc in cursor.description] if cursor.description else []
                operation_logs = [dict(zip(columns, row)) for row in rows]
                
                history = []
                for log in operation_logs:
                    operation_time = log.get('operation_time')
                    if operation_time and hasattr(operation_time, 'isoformat'):
                        operation_time_str = operation_time.isoformat()
                    elif operation_time:
                        operation_time_str = str(operation_time)
                    else:
                        operation_time_str = None
                    
                    history.append({
                        "id": log.get('id'),
                        "time": operation_time_str,
                        "tape_id": log.get('resource_id'),
                        "tape_label": log.get('resource_name'),
                        "operation": log.get('operation_name') or log.get('operation_description') or "",
                        "username": log.get('username') or "system",
                        "success": log.get('success', False),
                        "message": log.get('result_message') or log.get('error_message') or log.get('operation_description') or "",
                        "operation_type": log.get('operation_type') if isinstance(log.get('operation_type'), str) else (log.get('operation_type').value if hasattr(log.get('operation_type'), 'value') else str(log.get('operation_type')))
                    })
                
                return {
                    "success": True,
                    "history": history,
                    "count": len(history)
                }
        else:
            # 使用openGauss原生SQL
            async with get_opengauss_connection() as conn:
                # 查询磁带相关操作日志（resource_type = 'tape'）
                sql = """
                    SELECT 
                        id, operation_time, resource_id, resource_name, 
                        operation_name, operation_description, username,
                        success, result_message, error_message, operation_type
                    FROM operation_logs
                    WHERE resource_type = $1
                    ORDER BY operation_time DESC
                    LIMIT $2 OFFSET $3
                """
                
                rows = await conn.fetch(sql, "tape", limit, offset)
                
                history = []
                for row in rows:
                    operation_name = row['operation_name'] or row['operation_description'] or ""
                    # 如果operation_type是枚举值，转换为字符串
                    operation_type = row['operation_type']
                    if hasattr(operation_type, 'value'):
                        operation_type = operation_type.value
                    else:
                        operation_type = str(operation_type) if operation_type else ""
                    
                    # 格式化操作名称（中文友好）
                    operation_map = {
                        "tape_load": "加载磁带",
                        "tape_unload": "卸载磁带",
                        "tape_erase": "擦除磁带",
                        "tape_format": "格式化磁带",
                        "tape_read_label": "读取卷标",
                        "tape_write_label": "写入卷标",
                        "tape_rewind": "倒带",
                        "tape_position": "定位磁带",
                        "create": "创建磁带",
                        "update": "更新磁带",
                        "delete": "删除磁带"
                    }
                    
                    operation_display = operation_map.get(operation_type, operation_name or operation_type or "操作")
                    
                    message = row['result_message'] or row['error_message'] or row['operation_description'] or ""
                    
                    history.append({
                        "id": row['id'],
                        "time": row['operation_time'].isoformat() if row['operation_time'] else None,
                        "tape_id": row['resource_id'],
                        "tape_label": row['resource_name'],
                        "operation": operation_display,
                        "username": row['username'] or "system",
                        "success": row['success'],
                        "message": message,
                        "operation_type": operation_type
                    })
                
                duration_ms = int((datetime.now() - start_time).total_seconds() * 1000)
                await log_system(
                    level=LogLevel.INFO,
                    category=LogCategory.TAPE,
                    message=f"获取磁带操作历史成功: {len(history)} 条记录",
                    module="web.api.tape.crud",
                    function="get_tape_history",
                    duration_ms=duration_ms
                )
                
                return {
                    "success": True,
                    "history": history,
                    "count": len(history)
                }
    
    except Exception as e:
        duration_ms = int((datetime.now() - start_time).total_seconds() * 1000)
        error_msg = f"获取磁带操作历史失败: {str(e)}"
        logger.error(error_msg, exc_info=True)
        await log_system(
            level=LogLevel.ERROR,
            category=LogCategory.TAPE,
            message=error_msg,
            module="web.api.tape.crud",
            function="get_tape_history",
            exception_type=type(e).__name__,
            stack_trace=traceback.format_exc(),
            duration_ms=duration_ms
        )
        raise HTTPException(status_code=500, detail=str(e))


