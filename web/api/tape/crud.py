#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
磁带管理API - crud
Tape Management API - crud
"""

import logging
import traceback
import json
import re
import asyncio
from typing import List, Dict, Any, Optional
from datetime import datetime
from fastapi import APIRouter, HTTPException, Request, Depends, BackgroundTasks
from pydantic import BaseModel

from .models import CreateTapeRequest, UpdateTapeRequest
from models.system_log import OperationType, LogCategory, LogLevel
from utils.log_utils import log_operation, log_system
from utils.scheduler.db_utils import is_opengauss, get_opengauss_connection
from utils.tape_tools import tape_tools_manager

logger = logging.getLogger(__name__)
router = APIRouter()


def _normalize_tape_label(label: Optional[str], year: int, month: int) -> str:
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

        # 使用psycopg2直接连接，避免openGauss版本解析问题
        import psycopg2
        import psycopg2.extras
        from config.settings import get_settings
        
        settings = get_settings()
        database_url = settings.DATABASE_URL
        
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
        
        # 统一生成卷标与盘符
        current_datetime = datetime.now()
        target_year = request.create_year or current_datetime.year
        target_month = request.create_month or current_datetime.month
        target_month = max(1, min(12, target_month))
        final_label = _normalize_tape_label(request.label or request.tape_id, target_year, target_month)
        tape_id_value = final_label
        drive_letter = (settings.TAPE_DRIVE_LETTER or "O").strip().upper()
        if drive_letter.endswith(":"):
            drive_letter = drive_letter[:-1]
        if not drive_letter:
            drive_letter = "O"
        
        # 检查磁带是否已存在（以卷标为基准）
        tape_exists = False
        label_exists = False
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
                            # 等待一小段时间，确保格式化操作完全完成
                            import asyncio
                            await asyncio.sleep(2)
                            
                            # 读取磁盘上的实际卷标和序列号
                            label_result = await tape_tools_manager.read_tape_label_windows()
                            
                            if label_result.get("success"):
                                actual_label = label_result.get("volume_name", "").strip()
                                actual_serial = label_result.get("serial_number", "").strip()
                                
                                logger.info(f"从磁盘读取到实际值: 卷标={actual_label}, SN={actual_serial}")
                                
                                # 如果读取到的值与预期值不同，更新数据库
                                if actual_label or actual_serial:
                                    # 重新连接数据库
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


@router.get("/show/{tape_id}")
async def get_tape(tape_id: str, request: Request):
    """获取磁带详情"""
    try:
        # 使用psycopg2直接连接
        import psycopg2
        import psycopg2.extras
        from config.settings import get_settings
        
        settings = get_settings()
        database_url = settings.DATABASE_URL
        
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
                # 查询磁带信息
                cur.execute("""
                    SELECT tape_id, label, status, media_type, generation, serial_number, location,
                           capacity_bytes, used_bytes, retention_months, notes, manufactured_date, 
                           expiry_date, auto_erase, health_score
                    FROM tape_cartridges 
                    WHERE tape_id = %s
                """, (tape_id,))
                
                row = cur.fetchone()
                
                if not row:
                    return {
                        "success": False,
                        "message": f"磁带 {tape_id} 不存在"
                    }
                
                # 构建返回数据
                tape = {
                    "tape_id": row[0],
                    "label": row[1],
                    "status": row[2],
                    "media_type": row[3],
                    "generation": row[4],
                    "serial_number": row[5],
                    "location": row[6],
                    "capacity_bytes": row[7],
                    "used_bytes": row[8],
                    "retention_months": row[9],
                    "notes": row[10],
                    "manufactured_date": row[11].isoformat() if row[11] else None,
                    "expiry_date": row[12].isoformat() if row[12] else None,
                    "auto_erase": row[13],
                    "health_score": row[14]
                }
                
                return {
                    "success": True,
                    "tape": tape
                }
                
        finally:
            conn.close()
            
    except Exception as e:
        logger.error(f"获取磁带详情失败: {str(e)}")
        raise HTTPException(status_code=500, detail=str(e))


@router.put("/update/{tape_id}")
async def update_tape(tape_id: str, request: UpdateTapeRequest, http_request: Request):
    """更新磁带记录"""
    start_time = datetime.now()
    ip_address = http_request.client.host if http_request.client else None
    request_method = "PUT"
    request_url = str(http_request.url)
    
    try:
        system = http_request.app.state.system
        if not system:
            raise HTTPException(status_code=500, detail="系统未初始化")

        # 使用psycopg2直接连接，避免openGauss版本解析问题
        import psycopg2
        import psycopg2.extras
        from config.settings import get_settings
        
        settings = get_settings()
        database_url = settings.DATABASE_URL
        
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
        
        old_values = {}
        new_values = {}
        changed_fields = []
        
        try:
            with conn.cursor() as cur:
                # 检查磁带是否存在并获取旧值（包括manufactured_date用于序列号生成）
                cur.execute("""
                    SELECT tape_id, label, serial_number, media_type, generation, capacity_bytes, location, notes, manufactured_date
                    FROM tape_cartridges WHERE tape_id = %s
                """, (tape_id,))
                existing = cur.fetchone()
                
                if not existing:
                    await log_operation(
                        operation_type=OperationType.UPDATE,
                        resource_type="tape",
                        resource_id=tape_id,
                        operation_name="更新磁带",
                        operation_description=f"更新磁带 {tape_id}",
                        category="tape",
                        success=False,
                        error_message=f"磁带 {tape_id} 不存在",
                        ip_address=ip_address,
                        request_method=request_method,
                        request_url=request_url,
                        duration_ms=int((datetime.now() - start_time).total_seconds() * 1000)
                    )
                    return {
                        "success": False,
                        "message": f"磁带 {tape_id} 不存在"
                    }
                
                # 保存旧值
                old_values = {
                    "tape_id": existing[0],
                    "label": existing[1],
                    "serial_number": existing[2],
                    "media_type": existing[3],
                    "generation": existing[4],
                    "capacity_bytes": existing[5],
                    "location": existing[6],
                    "notes": existing[7],
                    "manufactured_date": existing[8].isoformat() if existing[8] else None
                }
                
                # 构建更新字段和值
                update_fields = []
                update_values = []
                
                if request.serial_number is not None and request.serial_number != old_values["serial_number"]:
                    update_fields.append("serial_number = %s")
                    update_values.append(request.serial_number)
                    new_values["serial_number"] = request.serial_number
                    changed_fields.append("serial_number")
                if request.media_type is not None and request.media_type != old_values["media_type"]:
                    update_fields.append("media_type = %s")
                    update_values.append(request.media_type)
                    new_values["media_type"] = request.media_type
                    changed_fields.append("media_type")
                if request.generation is not None and request.generation != old_values["generation"]:
                    update_fields.append("generation = %s")
                    update_values.append(request.generation)
                    new_values["generation"] = request.generation
                    changed_fields.append("generation")
                if request.capacity_gb is not None:
                    capacity_bytes = request.capacity_gb * (1024 ** 3)
                    if capacity_bytes != old_values["capacity_bytes"]:
                        update_fields.append("capacity_bytes = %s")
                        update_values.append(capacity_bytes)
                        new_values["capacity_bytes"] = capacity_bytes
                        changed_fields.append("capacity_bytes")
                if request.location is not None and request.location != old_values["location"]:
                    update_fields.append("location = %s")
                    update_values.append(request.location)
                    new_values["location"] = request.location
                    changed_fields.append("location")
                if request.notes is not None and request.notes != old_values["notes"]:
                    update_fields.append("notes = %s")
                    update_values.append(request.notes)
                    new_values["notes"] = request.notes
                    changed_fields.append("notes")
                
                # 如果没有需要更新的字段，返回错误
                if not update_fields:
                    await log_operation(
                        operation_type=OperationType.UPDATE,
                        resource_type="tape",
                        resource_id=tape_id,
                        resource_name=existing[1],
                        operation_name="更新磁带",
                        operation_description=f"更新磁带 {tape_id}",
                        category="tape",
                        success=False,
                        error_message="没有提供需要更新的字段",
                        ip_address=ip_address,
                        request_method=request_method,
                        request_url=request_url,
                        duration_ms=int((datetime.now() - start_time).total_seconds() * 1000)
                    )
                    return {
                        "success": False,
                        "message": "没有提供需要更新的字段"
                    }
                
                # 构建并执行更新SQL
                update_sql = f"""
                    UPDATE tape_cartridges
                    SET {', '.join(update_fields)}
                    WHERE tape_id = %s
                """
                update_values.append(tape_id)
                cur.execute(update_sql, update_values)
                
                conn.commit()
                logger.info(f"更新磁带记录: {tape_id}")
                
                # 如果请求格式化磁盘，执行格式化操作
                format_tape = getattr(request, 'format_tape', False)  # 默认False，需要显式指定
                if format_tape:
                    # 获取卷标和序列号用于格式化
                    # 如果请求中提供了新卷标，使用新卷标；否则使用旧卷标
                    label = getattr(request, 'label', None) or old_values.get("label") or tape_id
                    serial_number = new_values.get("serial_number") or old_values.get("serial_number")
                    
                    # 如果请求中提供了新卷标，需要先更新数据库中的卷标（在格式化之前）
                    if getattr(request, 'label', None) and getattr(request, 'label', None) != old_values.get("label"):
                        # 先更新卷标到数据库
                        cur.execute("UPDATE tape_cartridges SET label = %s WHERE tape_id = %s", 
                                  (getattr(request, 'label'), tape_id))
                        conn.commit()
                        new_values["label"] = getattr(request, 'label')
                        changed_fields.append("label")
                        logger.info(f"更新时将同时更新卷标: {old_values.get('label')} -> {getattr(request, 'label')}")
                        # 更新label变量，用于后续格式化
                        label = getattr(request, 'label')
                    
                    # 如果请求中提供了新序列号，也需要先更新数据库中的序列号（在格式化之前）
                    if serial_number and serial_number != old_values.get("serial_number"):
                        # 如果序列号还没有在update_fields中，需要单独更新
                        if "serial_number" not in changed_fields:
                            cur.execute("UPDATE tape_cartridges SET serial_number = %s WHERE tape_id = %s", 
                                      (serial_number, tape_id))
                            conn.commit()
                            logger.info(f"更新时将同时更新序列号: {old_values.get('serial_number')} -> {serial_number}")
                    
                    # 如果没有序列号，生成一个（使用新规则：TPMMNN）
                    # 序列号生成优先级：1. 从manufactured_date获取创建年月 2. 从卷标中提取 3. 当前年月
                    if not serial_number:
                        # 优先从manufactured_date获取创建年月
                        year = None
                        month = None
                        if old_values.get("manufactured_date"):
                            # manufactured_date 在 old_values 中已经是字符串格式
                            if isinstance(old_values["manufactured_date"], str):
                                manufactured_date = datetime.fromisoformat(old_values["manufactured_date"].replace('Z', '+00:00'))
                            else:
                                # 如果是 datetime 对象，直接使用
                                manufactured_date = old_values["manufactured_date"]
                            year = manufactured_date.year
                            month = manufactured_date.month
                        
                        # 如果manufactured_date中没有年月，尝试从卷标中提取
                        if year is None or month is None:
                            match = re.search(r'(\d{4})(\d{2})', label)
                            if match:
                                year = int(match.group(1))
                                month = int(match.group(2))
                        
                        # 如果仍然没有年月，使用当前年月
                        if year is None or month is None:
                            now = datetime.now()
                            year = now.year
                            month = now.month
                        
                        # 生成序列号（TPMMNN格式：TP + 月份 + 序号）
                        mm = month
                        # 查询当前月份已有多少张磁盘（查询TP + 月份开头的序列号）
                        cur.execute("""
                            SELECT COUNT(*) FROM tape_cartridges 
                            WHERE serial_number IS NOT NULL AND serial_number LIKE %s
                        """, (f"TP{mm:02d}%",))
                        count = cur.fetchone()[0] or 0
                        sequence = count + 1
                        serial_number = f"TP{mm:02d}{sequence:02d}"
                        logger.info(f"更新时自动生成序列号: {serial_number} (创建年份={year}, 创建月份={month}, 序号={sequence}, 卷标={label})")
                    
                    # 验证序列号格式（TPMMNN格式）
                    serial_param = None
                    if serial_number:
                        serial_number_upper = serial_number.strip().upper()
                        # 验证格式：TPMMNN（TP + 2位月份 + 2位序号）
                        if len(serial_number_upper) == 6 and serial_number_upper.startswith('TP') and serial_number_upper[2:4].isdigit() and serial_number_upper[4:6].isdigit():
                            serial_param = serial_number_upper
                        else:
                            logger.warning(f"序列号格式不正确，将不使用: {serial_number}")
                    
                    # 格式化磁盘（后台执行）
                    # 使用BackgroundTasks需要在函数参数中传递，这里使用asyncio.create_task
                    import asyncio
                    
                    # 保存变量供后台任务使用
                    background_label = label
                    background_serial = serial_param
                    background_tape_id = tape_id
                    background_old_serial = old_values.get("serial_number")
                    background_system = system  # 保存system实例供后台任务使用
                    
                    # 在格式化开始前，将状态设置为MAINTENANCE（维护中）
                    cur.execute("UPDATE tape_cartridges SET status = %s WHERE tape_id = %s", 
                              ('MAINTENANCE', tape_id))
                    conn.commit()
                    logger.info(f"格式化开始前，将磁带 {tape_id} 状态设置为 MAINTENANCE")
                    
                    async def format_tape_background():
                        try:
                            from utils.tape_tools import tape_tools_manager
                            # 重新连接数据库（因为原连接已关闭）
                            import psycopg2
                            db_conn = psycopg2.connect(
                                host=host,
                                port=port,
                                user=username,
                                password=password,
                                database=database
                            )
                            
                            try:
                                # 使用保存的system实例
                                drive_letter = background_system.settings.TAPE_DRIVE_LETTER or "O"
                                if drive_letter.endswith(":"):
                                    drive_letter = drive_letter[:-1]
                                drive_letter = drive_letter.strip().upper()
                                
                                logger.info(f"后台格式化磁盘: 卷标={background_label}, SN={background_serial}, 盘符={drive_letter}")
                                format_result = await tape_tools_manager.format_tape_ltfs(
                                    drive_letter=drive_letter,
                                    volume_label=background_label,
                                    serial=background_serial,
                                    eject_after=False
                                )
                                
                                logger.info(f"格式化命令执行完成 - 成功: {format_result.get('success')}, 返回码: {format_result.get('returncode')}, "
                                          f"stdout长度: {len(format_result.get('stdout', ''))}, stderr长度: {len(format_result.get('stderr', ''))}")
                                
                                if format_result.get("success"):
                                    logger.info(f"磁盘格式化成功: 卷标={background_label}, SN={background_serial}")
                                    
                                    # 格式化成功后，从磁盘读取实际的卷标和SN，然后更新数据库
                                    try:
                                        # 等待一小段时间，确保格式化操作完全完成
                                        import asyncio
                                        await asyncio.sleep(2)
                                        
                                        # 读取磁盘上的实际卷标和序列号
                                        label_result = await tape_tools_manager.read_tape_label_windows()
                                        
                                        if label_result.get("success"):
                                            actual_label = label_result.get("volume_name", "").strip()
                                            actual_serial = label_result.get("serial_number", "").strip()
                                            
                                            logger.info(f"从磁盘读取到实际值: 卷标={actual_label}, SN={actual_serial}")
                                            
                                            # 如果读取到的值与预期值不同，更新数据库
                                            if actual_label or actual_serial:
                                                update_fields = []
                                                update_values = []
                                                
                                                # 更新卷标（如果读取到的值与数据库中的不同）
                                                if actual_label and actual_label != background_label:
                                                    update_fields.append("label = %s")
                                                    update_values.append(actual_label)
                                                    logger.info(f"更新数据库卷标: {background_label} -> {actual_label}")
                                                
                                                # 更新序列号（如果读取到的值与数据库中的不同）
                                                if actual_serial and actual_serial != background_serial:
                                                    update_fields.append("serial_number = %s")
                                                    update_values.append(actual_serial)
                                                    logger.info(f"更新数据库序列号: {background_serial} -> {actual_serial}")
                                                
                                                # 如果有需要更新的字段，执行更新
                                                if update_fields:
                                                    update_values.append(background_tape_id)
                                                    update_sql = f"""
                                                        UPDATE tape_cartridges
                                                        SET {', '.join(update_fields)}, updated_at = NOW()
                                                        WHERE tape_id = %s
                                                    """
                                                    with db_conn.cursor() as db_cur:
                                                        db_cur.execute(update_sql, update_values)
                                                        db_conn.commit()
                                                        logger.info(f"已根据磁盘实际值更新数据库: tape_id={background_tape_id}, 更新字段={update_fields}")
                                                else:
                                                    logger.info(f"磁盘实际值与数据库一致，无需更新: 卷标={actual_label}, SN={actual_serial}")
                                            else:
                                                logger.warning("从磁盘读取到的卷标或序列号为空，跳过数据库更新")
                                            
                                            # 格式化成功，将状态改回AVAILABLE（可用）
                                            with db_conn.cursor() as db_cur:
                                                db_cur.execute("UPDATE tape_cartridges SET status = %s WHERE tape_id = %s", 
                                                            ('AVAILABLE', background_tape_id))
                                                db_conn.commit()
                                                logger.info(f"格式化完成，将磁带 {background_tape_id} 状态设置为 AVAILABLE")
                                        else:
                                            logger.warning(f"读取磁盘卷标失败: {label_result.get('error', '未知错误')}")
                                            # 即使读取失败，格式化成功也应该将状态改回AVAILABLE
                                            with db_conn.cursor() as db_cur:
                                                db_cur.execute("UPDATE tape_cartridges SET status = %s WHERE tape_id = %s", 
                                                            ('AVAILABLE', background_tape_id))
                                                db_conn.commit()
                                                logger.info(f"格式化完成（读取卷标失败），将磁带 {background_tape_id} 状态设置为 AVAILABLE")
                                    except Exception as read_error:
                                        logger.error(f"读取磁盘卷标并更新数据库时出错: {str(read_error)}", exc_info=True)
                                        # 即使读取异常，格式化成功也应该将状态改回AVAILABLE
                                        try:
                                            with db_conn.cursor() as db_cur:
                                                db_cur.execute("UPDATE tape_cartridges SET status = %s WHERE tape_id = %s", 
                                                            ('AVAILABLE', background_tape_id))
                                                db_conn.commit()
                                                logger.info(f"格式化完成（读取卷标异常），将磁带 {background_tape_id} 状态设置为 AVAILABLE")
                                        except Exception as db_update_error:
                                            logger.error(f"更新状态失败: {str(db_update_error)}", exc_info=True)
                                else:
                                    # 格式化失败，将状态改为ERROR（故障）
                                    error_detail = format_result.get("stderr") or format_result.get("stdout") or "格式化失败"
                                    returncode = format_result.get("returncode", -1)
                                    logger.error(f"格式化磁盘失败 - 返回码: {returncode}, 错误详情: {error_detail}")
                                    
                                    # 格式化失败时，将状态改为ERROR
                                    with db_conn.cursor() as db_cur:
                                        db_cur.execute("UPDATE tape_cartridges SET status = %s WHERE tape_id = %s", 
                                                    ('ERROR', background_tape_id))
                                        db_conn.commit()
                                        logger.warning(f"格式化失败，将磁带 {background_tape_id} 状态设置为 ERROR")
                                    
                                    # 发送钉钉通知
                                    try:
                                        if background_system and hasattr(background_system, 'dingtalk_notifier') and background_system.dingtalk_notifier:
                                            await background_system.dingtalk_notifier.send_tape_format_notification(
                                                tape_id=background_tape_id,
                                                status="failed",
                                                error_detail=f"返回码: {returncode}, {error_detail}",
                                                volume_label=background_label,
                                                serial_number=background_serial
                                            )
                                    except Exception as notify_error:
                                        logger.error(f"发送格式化失败钉钉通知异常: {str(notify_error)}", exc_info=True)
                            finally:
                                db_conn.close()
                        except Exception as e:
                            logger.error(f"后台格式化磁盘异常: {str(e)}", exc_info=True)
                            # 异常时也要将状态改为ERROR
                            try:
                                import psycopg2
                                db_conn = psycopg2.connect(
                                    host=host,
                                    port=port,
                                    user=username,
                                    password=password,
                                    database=database
                                )
                                with db_conn.cursor() as db_cur:
                                    db_cur.execute("UPDATE tape_cartridges SET status = %s WHERE tape_id = %s", 
                                                ('ERROR', background_tape_id))
                                    db_conn.commit()
                                    logger.error(f"格式化异常，将磁带 {background_tape_id} 状态设置为 ERROR")
                                db_conn.close()
                            except Exception as db_error:
                                logger.error(f"更新磁带状态失败: {str(db_error)}", exc_info=True)
                    
                    # 在后台执行格式化任务
                    # FastAPI已经提供了事件循环，直接创建任务即可
                    asyncio.create_task(format_tape_background())
                    logger.info(f"格式化磁盘任务已在后台启动: 卷标={label}, SN={serial_param}")
                    
                    # 返回格式化标志，供前端显示提示
                    duration_ms = int((datetime.now() - start_time).total_seconds() * 1000)
                    await log_operation(
                        operation_type=OperationType.UPDATE,
                        resource_type="tape",
                        resource_id=tape_id,
                        resource_name=old_values.get("label"),
                        operation_name="更新磁带",
                        operation_description=f"更新磁带 {tape_id}，格式化任务已在后台启动",
                        category="tape",
                        success=True,
                        result_message=f"磁带 {tape_id} 更新成功，格式化任务已在后台启动",
                        old_values=old_values,
                        new_values=new_values,
                        changed_fields=changed_fields,
                        ip_address=ip_address,
                        request_method=request_method,
                        request_url=request_url,
                        duration_ms=duration_ms
                    )
                    
                    return {
                        "success": True,
                        "message": f"磁带 {tape_id} 更新成功，格式化任务已在后台启动",
                        "tape_id": tape_id,
                        "formatted": True
                    }
        
        finally:
            conn.close()
        
        duration_ms = int((datetime.now() - start_time).total_seconds() * 1000)
        await log_operation(
            operation_type=OperationType.UPDATE,
            resource_type="tape",
            resource_id=tape_id,
            resource_name=old_values.get("label"),
            operation_name="更新磁带",
            operation_description=f"更新磁带 {tape_id}",
            category="tape",
            success=True,
            result_message=f"磁带 {tape_id} 更新成功",
            old_values=old_values,
            new_values=new_values,
            changed_fields=changed_fields,
            ip_address=ip_address,
            request_method=request_method,
            request_url=request_url,
            duration_ms=duration_ms
        )
        
        return {
            "success": True,
            "message": f"磁带 {tape_id} 更新成功",
            "tape_id": tape_id,
            "formatted": False
        }
        
    except HTTPException:
        raise
    except Exception as e:
        duration_ms = int((datetime.now() - start_time).total_seconds() * 1000)
        error_msg = f"更新磁带记录失败: {str(e)}"
        logger.error(error_msg, exc_info=True)
        await log_operation(
            operation_type=OperationType.UPDATE,
            resource_type="tape",
            resource_id=tape_id,
            operation_name="更新磁带",
            operation_description=f"更新磁带 {tape_id}",
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
            function="update_tape",
            exception_type=type(e).__name__,
            stack_trace=traceback.format_exc(),
            duration_ms=duration_ms
        )
        raise HTTPException(status_code=500, detail=str(e))


@router.get("/check/{tape_id}")
async def check_tape_exists(tape_id: str, request: Request):
    """检查磁带是否存在"""
    try:
        # 使用psycopg2直接连接，避免openGauss版本解析问题
        import psycopg2
        import psycopg2.extras
        from config.settings import get_settings
        from datetime import datetime, timezone
        
        settings = get_settings()
        database_url = settings.DATABASE_URL
        
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
                # 查询磁带是否存在
                cur.execute("""
                    SELECT tape_id, label, status, expiry_date
                    FROM tape_cartridges
                    WHERE tape_id = %s
                """, (tape_id,))
                
                row = cur.fetchone()
                
                if row:
                    # 检查是否过期（仅比较年月）
                    is_expired = False
                    if row[3]:  # expiry_date
                        # 使用timezone-aware datetime进行比较
                        now = datetime.now(timezone.utc)
                        expiry_date = row[3]
                        # 比较年月
                        if (now.year > expiry_date.year) or (now.year == expiry_date.year and now.month >= expiry_date.month):
                            is_expired = True
                    
                    return {
                        "exists": True,
                        "tape_id": row[0],
                        "label": row[1],
                        "status": row[2] if isinstance(row[2], str) else row[2].value,
                        "is_expired": is_expired,
                        "expiry_date": row[3].isoformat() if row[3] else None
                    }
                else:
                    return {
                        "exists": False
                    }
        
        finally:
            conn.close()
        
    except Exception as e:
        logger.error(f"检查磁带存在性失败: {str(e)}")
        raise HTTPException(status_code=500, detail=str(e))


@router.get("/list")
async def list_tapes(request: Request):
    """获取所有磁带列表
    
    注意：此接口在调用时会自动执行保留期检查，检查过期磁带并更新状态。
    """
    try:
        # 执行保留期检查（在打开磁带管理页面时检查）
        system = request.app.state.system
        if system and hasattr(system, 'tape_manager') and system.tape_manager:
            try:
                await system.tape_manager.check_retention_periods()
            except Exception as e:
                logger.warning(f"执行保留期检查失败: {str(e)}")
        
        import psycopg2
        from config.settings import get_settings
        
        settings = get_settings()
        database_url = settings.DATABASE_URL
        
        # 解析URL
        if database_url.startswith("opengauss://"):
            database_url = database_url.replace("opengauss://", "postgresql://", 1)
        
        pattern = r'postgresql://([^:]+):([^@]+)@([^:]+):(\d+)/(.+)'
        match = re.match(pattern, database_url)
        
        if not match:
            raise ValueError("无法解析数据库连接URL")
        
        username, password, host, port, database = match.groups()
        
        # 直接用psycopg2查询
        conn = psycopg2.connect(
            host=host,
            port=port,
            user=username,
            password=password,
            database=database
        )
        
        tapes = []
        try:
            with conn.cursor() as cur:
                # 查询所有磁带
                cur.execute("""
                    SELECT 
                        tape_id, label, status, media_type, generation,
                        serial_number, location, capacity_bytes, used_bytes,
                        write_count, read_count, load_count, health_score,
                        first_use_date, last_erase_date, expiry_date,
                        retention_months, backup_set_count, notes
                    FROM tape_cartridges
                    ORDER BY tape_id
                """)
                
                rows = cur.fetchall()
                
                for row in rows:
                    tapes.append({
                        "tape_id": row[0],
                        "label": row[1],
                        "status": row[2] if isinstance(row[2], str) else row[2].value,
                        "media_type": row[3],
                        "generation": row[4],
                        "serial_number": row[5],
                        "location": row[6],
                        "capacity_bytes": row[7],
                        "used_bytes": row[8],
                        "usage_percent": (row[8] / row[7] * 100) if row[7] > 0 else 0,
                        "write_count": row[9],
                        "read_count": row[10],
                        "load_count": row[11],
                        "health_score": row[12],
                        "first_use_date": row[13].isoformat() if row[13] else None,
                        "last_erase_date": row[14].isoformat() if row[14] else None,
                        "expiry_date": row[15].isoformat() if row[15] else None,
                        "retention_months": row[16],
                        "backup_set_count": row[17],
                        "notes": row[18]
                    })
        finally:
            conn.close()
            
        return {
            "success": True,
            "tapes": tapes,
            "count": len(tapes)
        }
        
    except Exception as e:
        logger.error(f"获取磁带列表失败: {str(e)}")
        raise HTTPException(status_code=500, detail=str(e))


@router.get("/inventory")
async def get_tape_inventory(request: Request):
    """获取磁带库存统计（从数据库获取真实数据）"""
    start_time = datetime.now()
    try:
        # 使用psycopg2直接连接，从数据库获取真实数据
        import psycopg2
        from config.settings import get_settings
        
        settings = get_settings()
        database_url = settings.DATABASE_URL
        
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
                else:
                    # 如果没有数据，返回空统计
                    return {
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


@router.get("/current")
async def get_current_tape(request: Request):
    """获取当前磁带信息"""
    start_time = datetime.now()
    try:
        system = request.app.state.system
        if not system:
            raise HTTPException(status_code=500, detail="系统未初始化")

        tape_info = await system.tape_manager.get_tape_info()
        duration_ms = int((datetime.now() - start_time).total_seconds() * 1000)
        
        if tape_info:
            await log_system(
                level=LogLevel.INFO,
                category=LogCategory.TAPE,
                message=f"获取当前磁带信息成功: {tape_info.get('tape_id', 'N/A')}",
                module="web.api.tape.crud",
                function="get_current_tape",
                duration_ms=duration_ms
            )
            return tape_info
        else:
            await log_system(
                level=LogLevel.INFO,
                category=LogCategory.TAPE,
                message="获取当前磁带信息: 当前没有加载的磁带",
                module="web.api.tape.crud",
                function="get_current_tape",
                duration_ms=duration_ms
            )
            return {"message": "当前没有加载的磁带"}

    except HTTPException:
        raise
    except Exception as e:
        duration_ms = int((datetime.now() - start_time).total_seconds() * 1000)
        error_msg = f"获取当前磁带信息失败: {str(e)}"
        logger.error(error_msg, exc_info=True)
        await log_system(
            level=LogLevel.ERROR,
            category=LogCategory.TAPE,
            message=error_msg,
            module="web.api.tape.crud",
            function="get_current_tape",
            exception_type=type(e).__name__,
            stack_trace=traceback.format_exc(),
            duration_ms=duration_ms
        )
        raise HTTPException(status_code=500, detail=str(e))


@router.get("/history")
async def get_tape_history(request: Request, limit: int = 50, offset: int = 0):
    """获取磁带操作历史（从新的日志系统获取，使用openGauss原生SQL）"""
    start_time = datetime.now()
    try:
        from config.settings import get_settings
        from datetime import timedelta
        
        settings = get_settings()
        database_url = settings.DATABASE_URL
        
        # 检查是否为openGauss
        is_opengauss = "opengauss" in database_url.lower()
        
        if not is_opengauss:
            # 非openGauss数据库，使用SQLAlchemy
            from config.database import db_manager
            from models.system_log import OperationLog
            from sqlalchemy import select, desc
            
            async with db_manager.AsyncSessionLocal() as session:
                # 查询磁带相关操作日志
                query = select(OperationLog).where(
                    OperationLog.resource_type == "tape"
                ).order_by(desc(OperationLog.operation_time)).limit(limit).offset(offset)
                
                result = await session.execute(query)
                operation_logs = result.scalars().all()
                
                history = []
                for log in operation_logs:
                    history.append({
                        "id": log.id,
                        "time": log.operation_time.isoformat() if log.operation_time else None,
                        "tape_id": log.resource_id,
                        "tape_label": log.resource_name,
                        "operation": log.operation_name or log.operation_description or "",
                        "username": log.username or "system",
                        "success": log.success,
                        "message": log.result_message or log.error_message or log.operation_description or "",
                        "operation_type": log.operation_type.value if hasattr(log.operation_type, 'value') else str(log.operation_type)
                    })
                
                return {
                    "success": True,
                    "history": history,
                    "count": len(history)
                }
        else:
            # 使用openGauss原生SQL
            import asyncpg
            import re
            
            # 解析URL
            url = database_url.replace("opengauss://", "postgresql://")
            pattern = r'postgresql://([^:]+):([^@]+)@([^:]+):(\d+)/(.+)'
            match = re.match(pattern, url)
            
            if not match:
                raise ValueError("无法解析openGauss数据库URL")
            
            username, password, host, port, database = match.groups()
            
            conn = await asyncpg.connect(
                host=host,
                port=int(port),
                user=username,
                password=password,
                database=database
            )
            
            try:
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
            
            finally:
                await conn.close()
    
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


@router.delete("/delete/{tape_id}")
async def delete_tape(tape_id: str, http_request: Request):
    """删除磁带记录"""
    start_time = datetime.now()
    ip_address = http_request.client.host if http_request.client else None
    request_method = "DELETE"
    request_url = str(http_request.url)
    
    try:
        # 使用psycopg2直接连接，避免openGauss版本解析问题
        import psycopg2
        import psycopg2.extras
        from config.settings import get_settings
        
        settings = get_settings()
        database_url = settings.DATABASE_URL
        
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
                # 检查磁带是否存在
                cur.execute("""
                    SELECT tape_id, label FROM tape_cartridges WHERE tape_id = %s
                """, (tape_id,))
                existing = cur.fetchone()
                
                if not existing:
                    await log_operation(
                        operation_type=OperationType.DELETE,
                        resource_type="tape",
                        resource_id=tape_id,
                        operation_name="删除磁带",
                        operation_description=f"删除磁带 {tape_id}",
                        category="tape",
                        success=False,
                        error_message=f"磁带 {tape_id} 不存在",
                        ip_address=ip_address,
                        request_method=request_method,
                        request_url=request_url,
                        duration_ms=int((datetime.now() - start_time).total_seconds() * 1000)
                    )
                    return {
                        "success": False,
                        "message": f"磁带 {tape_id} 不存在"
                    }
                
                tape_label = existing[1]
                
                # 删除磁带记录
                cur.execute("""
                    DELETE FROM tape_cartridges WHERE tape_id = %s
                """, (tape_id,))
                
                conn.commit()
                logger.info(f"删除磁带记录: {tape_id}")
        
        finally:
            conn.close()
        
        duration_ms = int((datetime.now() - start_time).total_seconds() * 1000)
        await log_operation(
            operation_type=OperationType.DELETE,
            resource_type="tape",
            resource_id=tape_id,
            resource_name=tape_label,
            operation_name="删除磁带",
            operation_description=f"删除磁带 {tape_id}",
            category="tape",
            success=True,
            result_message=f"磁带 {tape_id} 删除成功",
            ip_address=ip_address,
            request_method=request_method,
            request_url=request_url,
            duration_ms=duration_ms
        )
        
        return {
            "success": True,
            "message": f"磁带 {tape_id} 删除成功",
            "tape_id": tape_id
        }
        
    except HTTPException:
        raise
    except Exception as e:
        duration_ms = int((datetime.now() - start_time).total_seconds() * 1000)
        error_msg = f"删除磁带记录失败: {str(e)}"
        logger.error(error_msg, exc_info=True)
        await log_operation(
            operation_type=OperationType.DELETE,
            resource_type="tape",
            resource_id=tape_id,
            operation_name="删除磁带",
            operation_description=f"删除磁带 {tape_id}",
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
            function="delete_tape",
            exception_type=type(e).__name__,
            stack_trace=traceback.format_exc(),
            duration_ms=duration_ms
        )
        raise HTTPException(status_code=500, detail=str(e))
