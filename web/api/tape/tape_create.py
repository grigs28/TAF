#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
磁带管理API - tape_create
Tape Management API - tape_create
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



@router.post("/create")
async def create_tape(request: CreateTapeRequest, http_request: Request, background_tasks: BackgroundTasks):
    """创建或更新磁带记录，并使用LtfsCmdFormat.exe格式化磁带"""
    start_time = datetime.now()
    ip_address = http_request.client.host if http_request.client else None
    request_method = "POST"
    request_url = str(http_request.url)
    
    try:
        system = http_request.app.state.system
        if not system:
            raise HTTPException(status_code=500, detail="系统未初始化")

        from config.settings import get_settings
        from utils.scheduler.db_utils import is_redis
        
        settings = get_settings()
        database_url = settings.DATABASE_URL
        
        # 检查是否为 Redis
        is_redis_mode = is_redis()
        
        # 检查是否为 SQLite
        is_sqlite = database_url.startswith("sqlite:///") or database_url.startswith("sqlite+aiosqlite:///")
        
        # 统一生成卷标与盘符
        current_datetime = datetime.now()
        target_year = request.create_year or current_datetime.year
        target_month = request.create_month or current_datetime.month
        target_month = max(1, min(12, target_month))
        final_label = normalize_tape_label(request.label or request.tape_id, target_year, target_month)
        tape_id_value = final_label
        drive_letter = (settings.TAPE_DRIVE_LETTER or "O").strip().upper()
        if drive_letter.endswith(":"):
            drive_letter = drive_letter[:-1]
        if not drive_letter:
            drive_letter = "O"
        
        # 检查磁带是否已存在（以卷标为基准）
        tape_exists = False
        label_exists = False
        
        if is_redis_mode:
            # Redis模式：使用Redis查询
            from backup.redis_tape_db import check_tape_exists_redis, check_tape_label_exists_redis
            tape_exists = await check_tape_exists_redis(tape_id_value)
            label_exists = await check_tape_label_exists_redis(final_label)
        elif is_sqlite:
            # 使用 SQLAlchemy 查询 SQLite
            tape_exists, label_exists = await check_tape_exists_sqlite(db_manager, tape_id_value, final_label)
        else:
            # 使用 psycopg2 查询 PostgreSQL/openGauss
            import psycopg2
            import psycopg2.extras
            
            # 解析URL
            if database_url.startswith("opengauss://"):
                database_url = database_url.replace("opengauss://", "postgresql://", 1)
            pattern = r'postgresql://([^:]+):([^@]+)@([^:]+):(\d+)/(.+)'
            match = re.match(pattern, database_url)
            
            if not match:
                raise ValueError("无法解析数据库连接URL")
            
            username, password, host, port, database = match.groups()
            db_connect_kwargs = {
                "host": host,
                "port": port,
                "user": username,
                "password": password,
                "database": database
            }
            
            conn = psycopg2.connect(**db_connect_kwargs)
            try:
                with conn.cursor() as cur:
                    # 检查tape_id是否存在
                    cur.execute("SELECT 1 FROM tape_cartridges WHERE tape_id = %s", (tape_id_value,))
                    tape_exists = cur.fetchone() is not None
                    # 检查label是否存在
                    cur.execute("SELECT 1 FROM tape_cartridges WHERE label = %s", (final_label,))
                    label_exists = cur.fetchone() is not None
            finally:
                conn.close()
        
        # 如果数据库中没有该卷标，需要格式化磁盘并生成SN
        # 序列号生成优先级：1. 创建年份和月份（request.create_year/create_month） 2. 从卷标中提取 3. 当前年月
        # 如果用户没有提供序列号，自动生成（TPMMNN格式：TP + 月份 + 序号）
        if not request.serial_number:
            # 优先使用创建年份和月份
            year = target_year
            month = target_month
            
            # 如果卷标中包含年月信息，验证是否与创建年月一致
            match = re.search(r'(\d{4})(\d{2})', final_label)
            if match:
                label_year = int(match.group(1))
                label_month = int(match.group(2))
                # 如果卷标中的年月与创建年月不一致，使用卷标中的年月（卷标优先）
                if label_year != year or label_month != month:
                    logger.warning(f"卷标中的年月({label_year}{label_month:02d})与创建年月({year}{month:02d})不一致，使用卷标中的年月")
                    year = label_year
                    month = label_month
            
            # 生成序列号（TPMMNN格式：TP + 月份 + 序号）
            mm = month
            if is_redis_mode:
                # Redis模式：使用Redis统计序列号
                from backup.redis_tape_db import count_serial_numbers_redis
                count = await count_serial_numbers_redis(f"TP{mm:02d}%")
                sequence = count + 1
                generated_serial = f"TP{mm:02d}{sequence:02d}"
                logger.info(f"[Redis模式] 自动生成序列号: {generated_serial} (创建年份={year}, 创建月份={month}, 序号={sequence}, 卷标={final_label})")
            elif is_sqlite:
                count = await count_serial_numbers_sqlite(db_manager, f"TP{mm:02d}%")
                sequence = count + 1
                generated_serial = f"TP{mm:02d}{sequence:02d}"
                logger.info(f"自动生成序列号: {generated_serial} (创建年份={year}, 创建月份={month}, 序号={sequence}, 卷标={final_label})")
            else:
                conn = psycopg2.connect(**db_connect_kwargs)
                try:
                    with conn.cursor() as cur:
                        # 查询当前月份已有多少张磁盘（查询TP + 月份开头的序列号）
                        cur.execute("""
                            SELECT COUNT(*) FROM tape_cartridges 
                            WHERE serial_number IS NOT NULL AND serial_number LIKE %s
                        """, (f"TP{mm:02d}%",))
                        count = cur.fetchone()[0] or 0
                        sequence = count + 1
                        generated_serial = f"TP{mm:02d}{sequence:02d}"
                        logger.info(f"自动生成序列号: {generated_serial} (创建年份={year}, 创建月份={month}, 序号={sequence}, 卷标={final_label})")
                finally:
                    conn.close()
            
            # 使用生成的序列号
            serial_param = generated_serial
            request.serial_number = generated_serial
        else:
            # 用户提供了序列号，验证格式和月份一致性
            candidate = request.serial_number.strip().upper()
            # 验证格式：TPMMNN（TP + 2位月份 + 2位序号）
            if len(candidate) == 6 and candidate.startswith('TP') and candidate[2:4].isdigit() and candidate[4:6].isdigit():
                # 验证序列号中的月份是否与创建月份一致
                serial_month = int(candidate[2:4])
                
                # 优先使用创建年份和月份
                expected_year = target_year
                expected_month = target_month
                
                # 如果卷标中包含年月信息，验证是否与创建年月一致
                match = re.search(r'(\d{4})(\d{2})', final_label)
                if match:
                    label_year = int(match.group(1))
                    label_month = int(match.group(2))
                    # 如果卷标中的年月与创建年月不一致，使用卷标中的年月
                    if label_year != expected_year or label_month != expected_month:
                        expected_year = label_year
                        expected_month = label_month
                
                    # 验证序列号中的月份是否与期望的月份一致
                if serial_month != expected_month:
                    logger.warning(f"序列号中的月份({serial_month:02d})与创建月份({expected_month:02d})不一致，将重新生成")
                    # 重新生成序列号
                    mm = expected_month
                    if is_redis_mode:
                        # Redis模式：使用Redis统计序列号
                        from backup.redis_tape_db import count_serial_numbers_redis
                        count = await count_serial_numbers_redis(f"TP{mm:02d}%")
                        sequence = count + 1
                        generated_serial = f"TP{mm:02d}{sequence:02d}"
                        serial_param = generated_serial
                        request.serial_number = generated_serial
                        logger.info(f"[Redis模式] 重新生成序列号: {generated_serial} (创建年份={expected_year}, 创建月份={expected_month}, 序号={sequence})")
                    elif is_sqlite:
                        count = await count_serial_numbers_sqlite(db_manager, f"TP{mm:02d}%")
                        sequence = count + 1
                        generated_serial = f"TP{mm:02d}{sequence:02d}"
                        serial_param = generated_serial
                        request.serial_number = generated_serial
                        logger.info(f"重新生成序列号: {generated_serial} (创建年份={expected_year}, 创建月份={expected_month}, 序号={sequence})")
                    else:
                        conn = psycopg2.connect(**db_connect_kwargs)
                        try:
                            with conn.cursor() as cur:
                                cur.execute("""
                                    SELECT COUNT(*) FROM tape_cartridges 
                                    WHERE serial_number IS NOT NULL AND serial_number LIKE %s
                                """, (f"TP{mm:02d}%",))
                                count = cur.fetchone()[0] or 0
                                sequence = count + 1
                                generated_serial = f"TP{mm:02d}{sequence:02d}"
                                serial_param = generated_serial
                                request.serial_number = generated_serial
                                logger.info(f"重新生成序列号: {generated_serial} (创建年份={expected_year}, 创建月份={expected_month}, 序号={sequence})")
                        finally:
                            conn.close()
                else:
                    serial_param = candidate
            else:
                serial_param = None
                logger.warning(f"提供的序列号格式不正确: {request.serial_number}，将自动生成")
                # 如果格式不正确，重新生成
                # 优先使用创建年份和月份
                year = target_year
                month = target_month
                
                # 如果卷标中包含年月信息，验证是否与创建年月一致
                match = re.search(r'(\d{4})(\d{2})', final_label)
                if match:
                    label_year = int(match.group(1))
                    label_month = int(match.group(2))
                    if label_year != year or label_month != month:
                        logger.warning(f"卷标中的年月({label_year}{label_month:02d})与创建年月({year}{month:02d})不一致，使用卷标中的年月")
                        year = label_year
                        month = label_month
                
                mm = month
                if is_redis_mode:
                    # Redis模式：使用Redis统计序列号
                    from backup.redis_tape_db import count_serial_numbers_redis
                    count = await count_serial_numbers_redis(f"TP{mm:02d}%")
                    sequence = count + 1
                    generated_serial = f"TP{mm:02d}{sequence:02d}"
                    serial_param = generated_serial
                    request.serial_number = generated_serial
                    logger.info(f"[Redis模式] 自动生成序列号: {generated_serial} (创建年份={year}, 创建月份={month}, 序号={sequence}, 卷标={final_label})")
                elif is_sqlite:
                    count = await count_serial_numbers_sqlite(db_manager, f"TP{mm:02d}%")
                    sequence = count + 1
                    generated_serial = f"TP{mm:02d}{sequence:02d}"
                    serial_param = generated_serial
                    request.serial_number = generated_serial
                else:
                    conn = psycopg2.connect(**db_connect_kwargs)
                    try:
                        with conn.cursor() as cur:
                            cur.execute("""
                                SELECT COUNT(*) FROM tape_cartridges 
                                WHERE serial_number IS NOT NULL AND serial_number LIKE %s
                            """, (f"TP{mm:02d}%",))
                            count = cur.fetchone()[0] or 0
                            sequence = count + 1
                            generated_serial = f"TP{mm:02d}{sequence:02d}"
                            serial_param = generated_serial
                            request.serial_number = generated_serial
                    finally:
                        conn.close()
        
        # 如果数据库中没有该卷标，必须格式化磁盘
        format_tape = getattr(request, 'format_tape', True)  # 默认为True保持向后兼容
        if not label_exists:
            # 数据库中没有该卷标，必须格式化磁盘
            format_tape = True
            logger.info(f"数据库中没有卷标 {final_label}，将格式化磁盘并添加卷标和SN: {serial_param}")
        
        # 先更新数据库，格式化在后台执行（避免阻塞前端）
        if format_tape:
            operation_desc = "更新已有记录并" if tape_exists else "创建新记录并"
            logger.info(
                "%s使用 LtfsCmdFormat.exe 格式化磁带（后台执行）: tape_id=%s, drive=%s, label=%s", 
                operation_desc, tape_id_value, drive_letter, final_label
            )
            
            # 在后台执行格式化任务，不阻塞API响应
            async def format_tape_background():
                """后台格式化任务"""
                try:
                    format_result = await tape_tools_manager.format_tape_ltfs(
                        drive_letter=drive_letter,
                        volume_label=final_label,
                        serial=serial_param,
                        eject_after=False
                    )
                    
                    if format_result.get("success"):
                        logger.info("LtfsCmdFormat 格式化成功: tape_id=%s, label=%s", tape_id_value, final_label)
                        
                        # 格式化成功后，从磁盘读取实际的卷标和SN，然后更新数据库
                        try:
                            import asyncio
                            # 等待几秒，确保 LTFS 自动挂载完成
                            await asyncio.sleep(3)
                            
                            # 确认盘符挂载，如果尚未挂载则尝试重新分配
                            drive_with_colon = drive_letter if drive_letter.endswith(':') else f"{drive_letter}:"
                            if not os.path.exists(drive_with_colon):
                                logger.info(f"LTFS盘符 {drive_with_colon} 暂未挂载，尝试重新分配")
                                assign_result = await tape_tools_manager.assign_tape_ltfs(drive_letter)
                                if not assign_result.get("success"):
                                    logger.warning(f"重新分配 {drive_with_colon} 失败，错误: {assign_result.get('error')}, 暂不读取卷标")
                                    label_result = None
                                else:
                                    await asyncio.sleep(1)  # 再给 1 秒钟完成挂载
                                    try:
                                        label_result = await asyncio.wait_for(
                                            tape_tools_manager.read_tape_label_windows(),
                                            timeout=60.0
                                        )
                                    except asyncio.TimeoutError:
                                        logger.warning("读取磁带卷标超时（60秒）")
                                        label_result = {"success": False, "error": "读取卷标超时（60秒）"}
                            else:
                                try:
                                    label_result = await asyncio.wait_for(
                                        tape_tools_manager.read_tape_label_windows(),
                                        timeout=60.0
                                    )
                                except asyncio.TimeoutError:
                                    logger.warning("读取磁带卷标超时（60秒）")
                                    label_result = {"success": False, "error": "读取卷标超时（60秒）"}
                            
                            if label_result and label_result.get("success"):
                                actual_label = label_result.get("volume_name", "").strip()
                                actual_serial = label_result.get("serial_number", "").strip()
                                
                                logger.info(f"从磁盘读取到实际值: 卷标={actual_label}, SN={actual_serial}")
                                
                                # 如果读取到的值与预期值不同，更新数据库
                                if actual_label or actual_serial:
                                    # 重新连接数据库
                                    if is_redis_mode:
                                        # Redis模式：更新磁带记录
                                        from backup.redis_tape_db import update_tape_redis
                                        update_data = {}
                                        if actual_label and actual_label != final_label:
                                            update_data["label"] = actual_label
                                            logger.info(f"[Redis模式] 更新数据库卷标: {final_label} -> {actual_label}")
                                        if actual_serial and actual_serial != request.serial_number:
                                            update_data["serial_number"] = actual_serial
                                            logger.info(f"[Redis模式] 更新数据库序列号: {request.serial_number} -> {actual_serial}")
                                        if update_data:
                                            await update_tape_redis(tape_id=tape_id_value, **update_data)
                                            logger.info(f"[Redis模式] 已根据磁盘实际值更新数据库: tape_id={tape_id_value}")
                                    elif is_sqlite:
                                        # 使用原生SQL更新 SQLite
                                        from utils.scheduler.sqlite_utils import get_sqlite_connection
                                        
                                        async with get_sqlite_connection() as db_conn:
                                            # 查询磁带是否存在
                                            cursor = await db_conn.execute(
                                                "SELECT tape_id FROM tape_cartridges WHERE tape_id = ?",
                                                (tape_id_value,)
                                            )
                                            row = await cursor.fetchone()
                                            
                                            if row:
                                                # 更新卷标（如果读取到的值与数据库中的不同）
                                                if actual_label and actual_label != final_label:
                                                    await db_conn.execute(
                                                        "UPDATE tape_cartridges SET label = ? WHERE tape_id = ?",
                                                        (actual_label, tape_id_value)
                                                    )
                                                    logger.info(f"更新数据库卷标: {final_label} -> {actual_label}")
                                                
                                                # 更新序列号（如果读取到的值与数据库中的不同）
                                                if actual_serial and actual_serial != request.serial_number:
                                                    await db_conn.execute(
                                                        "UPDATE tape_cartridges SET serial_number = ? WHERE tape_id = ?",
                                                        (actual_serial, tape_id_value)
                                                    )
                                                    logger.info(f"更新数据库序列号: {request.serial_number} -> {actual_serial}")
                                                
                                                await db_conn.commit()
                                                logger.info(f"已根据磁盘实际值更新数据库: tape_id={tape_id_value}")
                                    else:
                                        db_conn = psycopg2.connect(**db_connect_kwargs)
                                        try:
                                            with db_conn.cursor() as db_cur:
                                                update_fields = []
                                                update_values = []
                                                
                                                # 更新卷标（如果读取到的值与数据库中的不同）
                                                if actual_label and actual_label != final_label:
                                                    update_fields.append("label = %s")
                                                    update_values.append(actual_label)
                                                    logger.info(f"更新数据库卷标: {final_label} -> {actual_label}")
                                                
                                                # 更新序列号（如果读取到的值与数据库中的不同）
                                                if actual_serial and actual_serial != request.serial_number:
                                                    update_fields.append("serial_number = %s")
                                                    update_values.append(actual_serial)
                                                    logger.info(f"更新数据库序列号: {request.serial_number} -> {actual_serial}")
                                                
                                                # 如果有需要更新的字段，执行更新
                                                if update_fields:
                                                    update_values.append(tape_id_value)
                                                    update_sql = f"""
                                                        UPDATE tape_cartridges
                                                        SET {', '.join(update_fields)}, updated_at = NOW()
                                                        WHERE tape_id = %s
                                                    """
                                                    db_cur.execute(update_sql, update_values)
                                                    db_conn.commit()
                                                    logger.info(f"已根据磁盘实际值更新数据库: tape_id={tape_id_value}")
                                        finally:
                                            db_conn.close()
                                else:
                                    logger.warning("从磁盘读取到的卷标或序列号为空，跳过数据库更新")
                            else:
                                logger.warning(f"读取磁盘卷标失败: {label_result.get('error', '未知错误')}")
                        except Exception as read_error:
                            logger.error(f"读取磁盘卷标并更新数据库时出错: {str(read_error)}", exc_info=True)
                        
                        await log_operation(
                            operation_type=OperationType.UPDATE if tape_exists else OperationType.CREATE,
                            resource_type="tape",
                            resource_id=tape_id_value,
                            resource_name=final_label,
                            operation_name="磁带格式化",
                            operation_description=f"LtfsCmdFormat 格式化磁带 {tape_id_value} 成功（后台执行）",
                            category="tape",
                            success=True,
                            ip_address=ip_address,
                            request_method=request_method,
                            request_url=request_url
                        )
                    else:
                        error_detail = format_result.get("stderr") or format_result.get("stdout") or "LtfsCmdFormat执行失败"
                        logger.error("LtfsCmdFormat 格式化磁带失败（后台执行）: %s", error_detail)
                        
                        # 发送钉钉通知
                        try:
                            if system and hasattr(system, 'dingtalk_notifier') and system.dingtalk_notifier:
                                await system.dingtalk_notifier.send_tape_format_notification(
                                    tape_id=tape_id_value,
                                    status="failed",
                                    error_detail=error_detail,
                                    volume_label=final_label,
                                    serial_number=serial_param
                                )
                        except Exception as notify_error:
                            logger.error(f"发送格式化失败钉钉通知异常: {str(notify_error)}", exc_info=True)
                        
                        await log_operation(
                            operation_type=OperationType.UPDATE if tape_exists else OperationType.CREATE,
                            resource_type="tape",
                            resource_id=tape_id_value,
                            resource_name=final_label,
                            operation_name="磁带格式化",
                            operation_description=f"LtfsCmdFormat 格式化磁带 {tape_id_value} 失败（后台执行）",
                            category="tape",
                            success=False,
                            error_message=error_detail,
                            ip_address=ip_address,
                            request_method=request_method,
                            request_url=request_url
                        )
                except Exception as e:
                    logger.error(f"后台格式化任务异常: {str(e)}", exc_info=True)
            
            # 启动后台任务，不等待完成
            background_tasks.add_task(format_tape_background)
            
            logger.info("格式化任务已在后台启动，API立即返回")
        else:
            # 用户选择不格式化，只更新数据库
            if tape_exists:
                logger.info("磁带 %s 已存在，用户选择不格式化，仅更新数据库记录", tape_id_value)
            else:
                logger.info("创建新磁带记录，用户选择不格式化，仅写入数据库")
        
        # 计算容量与有效期
        capacity_bytes = request.capacity_gb * (1024 ** 3) if request.capacity_gb else 18 * 1024 * (1024 ** 3)
        created_date = datetime(target_year, target_month, 1)
        
        expiry_year = created_date.year
        expiry_month = created_date.month + request.retention_months
        while expiry_month > 12:
            expiry_year += 1
            expiry_month -= 12
        expiry_date = datetime(expiry_year, expiry_month, 1)
        
        new_values = {
            "tape_id": tape_id_value,
            "label": final_label,
            "status": "AVAILABLE",
            "media_type": request.media_type,
            "generation": request.generation,
            "serial_number": request.serial_number,
            "location": request.location,
            "capacity_bytes": capacity_bytes,
            "retention_months": request.retention_months,
            "notes": request.notes,
            "manufactured_date": created_date.isoformat(),
            "expiry_date": expiry_date.isoformat()
        }
        
        # 写入或更新数据库
        if is_redis_mode:
            # Redis模式：使用Redis创建/更新磁带
            from backup.redis_tape_db import create_tape_redis, update_tape_redis
            
            media_type_str = request.media_type.value if hasattr(request.media_type, 'value') else str(request.media_type)
            
            if tape_exists:
                logger.info("[Redis模式] 磁带 %s 已存在，%s更新数据库记录", 
                          tape_id_value, "后台格式化任务已启动，" if format_tape else "跳过格式化，直接")
                success = await update_tape_redis(
                    tape_id=tape_id_value,
                    label=final_label,
                    status="AVAILABLE",
                    media_type=media_type_str,
                    generation=request.generation,
                    serial_number=request.serial_number,
                    location=request.location,
                    capacity_bytes=capacity_bytes,
                    retention_months=request.retention_months,
                    notes=request.notes,
                    manufactured_date=created_date,
                    expiry_date=expiry_date
                )
            else:
                logger.info("[Redis模式] 磁带 %s 不存在，创建新数据库记录", tape_id_value)
                result = await create_tape_redis(
                    tape_id=tape_id_value,
                    label=final_label,
                    status="AVAILABLE",
                    media_type=media_type_str,
                    generation=request.generation,
                    serial_number=request.serial_number,
                    location=request.location,
                    capacity_bytes=capacity_bytes,
                    retention_months=request.retention_months,
                    notes=request.notes,
                    manufactured_date=created_date,
                    expiry_date=expiry_date,
                    auto_erase=True,
                    health_score=100
                )
                success = result.get("success", False)
            
            if not success:
                raise Exception(f"[Redis模式] 创建/更新磁带失败: {tape_id_value}")
        elif is_sqlite:
            # 使用原生SQL操作 SQLite
            from utils.scheduler.sqlite_utils import get_sqlite_connection
            from models.tape import TapeStatus
            
            async with get_sqlite_connection() as conn:
                if tape_exists:
                    logger.info("磁带 %s 已存在，%s更新数据库记录", 
                              tape_id_value, "后台格式化任务已启动，" if format_tape else "跳过格式化，直接")
                    await conn.execute("""
                        UPDATE tape_cartridges
                        SET label = ?, status = ?, media_type = ?, generation = ?,
                            serial_number = ?, location = ?, capacity_bytes = ?,
                            retention_months = ?, notes = ?, manufactured_date = ?,
                            expiry_date = ?
                        WHERE tape_id = ?
                    """, (
                        final_label,
                        TapeStatus.AVAILABLE.value,
                        request.media_type.value if hasattr(request.media_type, 'value') else str(request.media_type),
                        request.generation,
                        request.serial_number,
                        request.location,
                        capacity_bytes,
                        request.retention_months,
                        request.notes,
                        created_date,
                        expiry_date,
                        tape_id_value
                    ))
                else:
                    logger.info("磁带 %s 不存在，创建新数据库记录", tape_id_value)
                    await conn.execute("""
                        INSERT INTO tape_cartridges (
                            tape_id, label, status, media_type, generation,
                            serial_number, location, capacity_bytes, used_bytes,
                            retention_months, notes, manufactured_date, expiry_date,
                            auto_erase, health_score
                        ) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
                    """, (
                        tape_id_value,
                        final_label,
                        TapeStatus.AVAILABLE.value,
                        request.media_type.value if hasattr(request.media_type, 'value') else str(request.media_type),
                        request.generation,
                        request.serial_number,
                        request.location,
                        capacity_bytes,
                        0,  # used_bytes
                        request.retention_months,
                        request.notes,
                        created_date,
                        expiry_date,
                        True,  # auto_erase
                        100  # health_score
                    ))
                
                await conn.commit()
        else:
            conn = psycopg2.connect(**db_connect_kwargs)
            try:
                with conn.cursor() as cur:
                    if tape_exists:
                        logger.info("磁带 %s 已存在，%s更新数据库记录", 
                                  tape_id_value, "后台格式化任务已启动，" if format_tape else "跳过格式化，直接")
                        cur.execute(
                            """
                            UPDATE tape_cartridges
                            SET label = %s,
                                status = %s,
                                media_type = %s,
                                generation = %s,
                                serial_number = %s,
                                location = %s,
                                capacity_bytes = %s,
                                retention_months = %s,
                                notes = %s,
                                manufactured_date = %s,
                                expiry_date = %s
                            WHERE tape_id = %s
                            """,
                            (
                                final_label,
                                'AVAILABLE',
                                request.media_type,
                                request.generation,
                                request.serial_number,
                                request.location,
                                capacity_bytes,
                                request.retention_months,
                                request.notes,
                                created_date,
                                expiry_date,
                                tape_id_value
                            )
                        )
                    else:
                        logger.info("磁带 %s 不存在，创建新数据库记录", tape_id_value)
                        cur.execute(
                            """
                            INSERT INTO tape_cartridges 
                            (tape_id, label, status, media_type, generation, serial_number, location,
                             capacity_bytes, used_bytes, retention_months, notes, manufactured_date, expiry_date, auto_erase, health_score)
                            VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s)
                            """,
                            (
                                tape_id_value,
                                final_label,
                                'AVAILABLE',
                                request.media_type,
                                request.generation,
                                request.serial_number,
                                request.location,
                                capacity_bytes,
                                0,
                                request.retention_months,
                                request.notes,
                                created_date,
                                expiry_date,
                                True,
                                100
                            )
                        )
                conn.commit()
            finally:
                conn.close()
        
        duration_ms = int((datetime.now() - start_time).total_seconds() * 1000)
        operation_type = OperationType.UPDATE if tape_exists else OperationType.CREATE
        operation_name = "更新磁带" if tape_exists else "创建磁带"
        operation_description = f"磁带 {tape_id_value} {'更新' if tape_exists else '创建'}成功，{'格式化任务已在后台启动' if format_tape else '未格式化（用户选择跳过）'}"
        result_message = f"磁带 {tape_id_value} {'更新' if tape_exists else '创建'}成功，卷标 {final_label}" + ("（格式化任务已在后台执行）" if format_tape else "")
        
        await log_operation(
            operation_type=operation_type,
            resource_type="tape",
            resource_id=tape_id_value,
            resource_name=final_label,
            operation_name=operation_name,
            operation_description=operation_description,
            category="tape",
            success=True,
            result_message=result_message,
            new_values=new_values,
            ip_address=ip_address,
            request_method=request_method,
            request_url=request_url,
            duration_ms=duration_ms
        )
        
        return {
            "success": True,
            "message": result_message,
            "tape_id": tape_id_value,
            "label": final_label,
            "formatted": format_tape,
            "updated": tape_exists
        }
        
    except HTTPException:
        raise
    except Exception as e:
        duration_ms = int((datetime.now() - start_time).total_seconds() * 1000)
        error_msg = f"创建/更新磁带失败: {str(e)}"
        logger.error(error_msg, exc_info=True)
        await log_operation(
            operation_type=OperationType.UPDATE,
            resource_type="tape",
            resource_id=tape_id_value if 'tape_id_value' in locals() else getattr(request, 'tape_id', None),
            resource_name=final_label if 'final_label' in locals() else getattr(request, 'label', None),
            operation_name="创建/更新磁带",
            operation_description="磁带创建/更新失败",
            category="tape",
            success=False,
            error_message=str(e),
            ip_address=ip_address,
            request_method=request_method,
            request_url=request_url,
            duration_ms=duration_ms
        )
        await log_system(
            level=LogLevel.ERROR,
            category=LogCategory.TAPE,
            message=error_msg,
            module="web.api.tape.crud",
            function="create_tape",
            exception_type=type(e).__name__,
            stack_trace=traceback.format_exc(),
            duration_ms=duration_ms
        )
        raise HTTPException(status_code=500, detail=str(e))


