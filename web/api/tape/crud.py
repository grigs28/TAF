#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
磁带管理API - crud
Tape Management API - crud
"""

import logging
import traceback
import json
from typing import List, Dict, Any, Optional
from datetime import datetime
from fastapi import APIRouter, HTTPException, Request, Depends
from pydantic import BaseModel

from .models import CreateTapeRequest, UpdateTapeRequest
from models.system_log import OperationType, LogCategory, LogLevel
from utils.log_utils import log_operation, log_system
from utils.scheduler.db_utils import is_opengauss, get_opengauss_connection

logger = logging.getLogger(__name__)
router = APIRouter()

@router.post("/create")
async def create_tape(request: CreateTapeRequest, http_request: Request):
    """创建新磁带记录"""
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
        from datetime import timedelta
        
        settings = get_settings()
        database_url = settings.DATABASE_URL
        
        # 解析URL
        if database_url.startswith("opengauss://"):
            database_url = database_url.replace("opengauss://", "postgresql://", 1)
        
        import re
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
                # 检查磁带ID是否已存在
                cur.execute("SELECT tape_id FROM tape_cartridges WHERE tape_id = %s", (request.tape_id,))
                existing = cur.fetchone()
                
                if existing:
                    await log_operation(
                        operation_type=OperationType.CREATE,
                        resource_type="tape",
                        resource_id=request.tape_id,
                        resource_name=request.label,
                        operation_name="创建磁带",
                        operation_description=f"创建磁带 {request.tape_id}",
                        category="tape",
                        success=False,
                        error_message=f"磁带 {request.tape_id} 已存在",
                        ip_address=ip_address,
                        request_method=request_method,
                        request_url=request_url,
                        duration_ms=int((datetime.now() - start_time).total_seconds() * 1000)
                    )
                    return {
                        "success": False,
                        "message": f"磁带 {request.tape_id} 已存在"
                    }
                
                # 计算容量
                # 前端发送的是 capacity_gb，单位是GB（二进制：1TB=1024GB）
                # 例如：18TB = 18 * 1024 = 18432 GB
                # capacity_bytes = capacity_gb * (1024 ** 3)
                if request.capacity_gb:
                    capacity_bytes = request.capacity_gb * (1024 ** 3)
                else:
                    capacity_bytes = 18 * 1024 * (1024 ** 3)  # 默认18TB = 18432GB
                
                # 计算创建日期和过期日期（仅年月）
                if request.create_year and request.create_month:
                    # 使用指定的年月，日期设为1号
                    created_date = datetime(request.create_year, request.create_month, 1)
                else:
                    # 默认使用当前年月
                    now = datetime.now()
                    created_date = datetime(now.year, now.month, 1)
                
                # 计算过期日期：创建日期 + retention_months个月
                expiry_year = created_date.year
                expiry_month = created_date.month + request.retention_months
                
                # 处理跨年
                while expiry_month > 12:
                    expiry_year += 1
                    expiry_month -= 12
                
                expiry_date = datetime(expiry_year, expiry_month, 1)
                
                # 准备新值
                new_values = {
                    "tape_id": request.tape_id,
                    "label": request.label,
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
                
                # 插入新磁带
                cur.execute("""
                    INSERT INTO tape_cartridges 
                    (tape_id, label, status, media_type, generation, serial_number, location,
                     capacity_bytes, used_bytes, retention_months, notes, manufactured_date, expiry_date, auto_erase, health_score)
                    VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s)
                """, (
                    request.tape_id,
                    request.label,
                    'AVAILABLE',  # 使用'AVAILABLE'状态（全大写）
                    request.media_type,
                    request.generation,
                    request.serial_number,
                    request.location,
                    capacity_bytes,
                    0,
                    request.retention_months,
                    request.notes,
                    created_date,  # 使用计算出的创建日期（仅年月）
                    expiry_date,
                    True,
                    100  # 默认健康分数100
                ))
                
                conn.commit()
                logger.info(f"创建磁带记录: {request.tape_id}")
        
        finally:
            conn.close()
        
        duration_ms = int((datetime.now() - start_time).total_seconds() * 1000)
        
        # 尝试写入物理磁带标签（如果磁带机中有磁带）
        try:
            # 准备标签数据
            tape_info = {
                "tape_id": request.tape_id,
                "label": request.label,
                "serial_number": request.serial_number,
                "created_date": created_date,
                "expiry_date": expiry_date
            }
            
            # 写入物理磁带标签
            write_result = await system.tape_manager.tape_operations._write_tape_label(tape_info)
            if write_result:
                logger.info(f"磁带标签已写入物理磁带: {request.tape_id}")
                await log_operation(
                    operation_type=OperationType.CREATE,
                    resource_type="tape",
                    resource_id=request.tape_id,
                    resource_name=request.label,
                    operation_name="创建磁带",
                    operation_description=f"创建磁带 {request.tape_id}，标签已写入",
                    category="tape",
                    success=True,
                    result_message=f"磁带 {request.tape_id} 创建成功，标签已写入",
                    new_values=new_values,
                    ip_address=ip_address,
                    request_method=request_method,
                    request_url=request_url,
                    duration_ms=duration_ms
                )
                return {
                    "success": True,
                    "message": f"磁带 {request.tape_id} 创建成功，标签已写入",
                    "tape_id": request.tape_id
                }
            else:
                logger.warning(f"磁带记录创建成功，但物理标签写入失败（可能磁带机中无磁带）")
                await log_operation(
                    operation_type=OperationType.CREATE,
                    resource_type="tape",
                    resource_id=request.tape_id,
                    resource_name=request.label,
                    operation_name="创建磁带",
                    operation_description=f"创建磁带 {request.tape_id}（但未写入物理磁带）",
                    category="tape",
                    success=True,
                    result_message=f"磁带 {request.tape_id} 创建成功（但未写入物理磁带）",
                    new_values=new_values,
                    ip_address=ip_address,
                    request_method=request_method,
                    request_url=request_url,
                    duration_ms=duration_ms
                )
                return {
                    "success": True,
                    "message": f"磁带 {request.tape_id} 创建成功（但未写入物理磁带，请确保磁带机中已装入磁带）",
                    "tape_id": request.tape_id
                }
        except Exception as e:
            logger.warning(f"写入物理磁带标签时出错: {str(e)}")
            await log_operation(
                operation_type=OperationType.CREATE,
                resource_type="tape",
                resource_id=request.tape_id,
                resource_name=request.label,
                operation_name="创建磁带",
                operation_description=f"创建磁带 {request.tape_id}（标签写入失败）",
                category="tape",
                success=True,
                result_message=f"磁带 {request.tape_id} 创建成功（但未写入物理磁带）",
                error_message=f"标签写入失败: {str(e)}",
                new_values=new_values,
                ip_address=ip_address,
                request_method=request_method,
                request_url=request_url,
                duration_ms=duration_ms
            )
            return {
                "success": True,
                "message": f"磁带 {request.tape_id} 创建成功（但未写入物理磁带，请确保磁带机中已装入磁带）",
                "tape_id": request.tape_id
            }
        
    except HTTPException:
        raise
    except Exception as e:
        duration_ms = int((datetime.now() - start_time).total_seconds() * 1000)
        error_msg = f"创建磁带记录失败: {str(e)}"
        logger.error(error_msg, exc_info=True)
        await log_operation(
            operation_type=OperationType.CREATE,
            resource_type="tape",
            resource_id=getattr(request, 'tape_id', None),
            resource_name=getattr(request, 'label', None),
            operation_name="创建磁带",
            operation_description=f"创建磁带失败",
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
        
        import re
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
        
        import re
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
                # 检查磁带是否存在并获取旧值
                cur.execute("""
                    SELECT tape_id, label, serial_number, media_type, generation, capacity_bytes, location, notes
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
                    "notes": existing[7]
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
            "tape_id": tape_id
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
        
        import re
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
    """获取所有磁带列表"""
    try:
        import psycopg2
        from config.settings import get_settings
        
        settings = get_settings()
        database_url = settings.DATABASE_URL
        
        # 解析URL
        if database_url.startswith("opengauss://"):
            database_url = database_url.replace("opengauss://", "postgresql://", 1)
        
        import re
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
        
        import re
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
                        "tape_read_label": "读取标签",
                        "tape_write_label": "写入标签",
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
