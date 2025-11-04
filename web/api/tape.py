#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
磁带管理API
Tape Management API
"""

import logging
import uuid
import base64
import json
from typing import List, Dict, Any, Optional
from fastapi import APIRouter, HTTPException, Request, Depends
from pydantic import BaseModel
from config.database import get_db

logger = logging.getLogger(__name__)
router = APIRouter()

# 添加一个测试路由，验证路由系统是否工作
@router.get("/test-read-label")
async def test_read_label(request: Request):
    """测试路由"""
    logger.critical("========== 测试路由被调用 ==========")
    raise HTTPException(status_code=500, detail="测试路由工作正常！如果你看到这个错误，说明路由系统正常")


class TapeConfigRequest(BaseModel):
    """磁带配置请求模型"""
    retention_months: int = 6
    auto_erase: bool = True


class CreateTapeRequest(BaseModel):
    """创建磁带请求模型"""
    tape_id: str
    label: str
    serial_number: Optional[str] = None
    media_type: str = "LTO"
    generation: int = 8
    capacity_gb: Optional[int] = None
    location: Optional[str] = None
    notes: Optional[str] = None
    retention_months: int = 6
    create_year: Optional[int] = None  # 创建年份
    create_month: Optional[int] = None  # 创建月份


class UpdateTapeRequest(BaseModel):
    """更新磁带请求模型"""
    serial_number: Optional[str] = None
    media_type: Optional[str] = None
    generation: Optional[int] = None
    capacity_gb: Optional[int] = None
    location: Optional[str] = None
    notes: Optional[str] = None


class WriteTapeLabelRequest(BaseModel):
    """写入磁带标签请求模型"""
    tape_id: str
    label: str
    serial_number: Optional[str] = None


@router.post("/create")
async def create_tape(request: CreateTapeRequest, http_request: Request):
    """创建新磁带记录"""
    try:
        system = http_request.app.state.system
        if not system:
            raise HTTPException(status_code=500, detail="系统未初始化")

        # 使用psycopg2直接连接，避免openGauss版本解析问题
        import psycopg2
        import psycopg2.extras
        from config.settings import get_settings
        from datetime import datetime, timedelta
        
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
                from datetime import date
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
                return {
                    "success": True,
                    "message": f"磁带 {request.tape_id} 创建成功，标签已写入",
                    "tape_id": request.tape_id
                }
            else:
                logger.warning(f"磁带记录创建成功，但物理标签写入失败（可能磁带机中无磁带）")
                return {
                    "success": True,
                    "message": f"磁带 {request.tape_id} 创建成功（但未写入物理磁带，请确保磁带机中已装入磁带）",
                    "tape_id": request.tape_id
                }
        except Exception as e:
            logger.warning(f"写入物理磁带标签时出错: {str(e)}")
            return {
                "success": True,
                "message": f"磁带 {request.tape_id} 创建成功（但未写入物理磁带，请确保磁带机中已装入磁带）",
                "tape_id": request.tape_id
            }
        
    except Exception as e:
        logger.error(f"创建磁带记录失败: {str(e)}")
        raise HTTPException(status_code=500, detail=str(e))


@router.get("/read-label")
async def read_tape_label(request: Request):
    """读取磁带标签"""
    logger.info("========== 读取磁带标签API被调用 ==========")
    try:
        logger.info("检查系统实例...")
        system = request.app.state.system
        if not system:
            logger.error("系统未初始化")
            raise HTTPException(status_code=500, detail="系统未初始化")
        
        logger.info("系统实例检查通过，准备调用tape_operations._read_tape_label")
        
        # 通过磁带操作读取标签
        metadata = await system.tape_manager.tape_operations._read_tape_label()
        
        logger.info(f"读取标签完成，结果: {metadata is not None}")
        if metadata:
            logger.info(f"成功读取标签: {metadata.get('tape_id', 'N/A')}")
            return {
                "success": True,
                "metadata": metadata
            }
        else:
            logger.warning("无法读取磁带标签或磁带为空")
            return {
                "success": False,
                "message": "无法读取磁带标签或磁带为空"
            }
        
    except HTTPException:
        raise
    except Exception as e:
        logger.error(f"读取磁带标签异常: {str(e)}", exc_info=True)
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
        
        try:
            with conn.cursor() as cur:
                # 检查磁带是否存在
                cur.execute("SELECT tape_id FROM tape_cartridges WHERE tape_id = %s", (tape_id,))
                existing = cur.fetchone()
                
                if not existing:
                    return {
                        "success": False,
                        "message": f"磁带 {tape_id} 不存在"
                    }
                
                # 构建更新字段和值
                update_fields = []
                update_values = []
                
                if request.serial_number is not None:
                    update_fields.append("serial_number = %s")
                    update_values.append(request.serial_number)
                if request.media_type is not None:
                    update_fields.append("media_type = %s")
                    update_values.append(request.media_type)
                if request.generation is not None:
                    update_fields.append("generation = %s")
                    update_values.append(request.generation)
                if request.capacity_gb is not None:
                    capacity_bytes = request.capacity_gb * (1024 ** 3)
                    update_fields.append("capacity_bytes = %s")
                    update_values.append(capacity_bytes)
                if request.location is not None:
                    update_fields.append("location = %s")
                    update_values.append(request.location)
                if request.notes is not None:
                    update_fields.append("notes = %s")
                    update_values.append(request.notes)
                
                # 如果没有需要更新的字段，返回错误
                if not update_fields:
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
        
        return {
            "success": True,
            "message": f"磁带 {tape_id} 更新成功",
            "tape_id": tape_id
        }
        
    except Exception as e:
        logger.error(f"更新磁带记录失败: {str(e)}")
        raise HTTPException(status_code=500, detail=str(e))




@router.post("/write-label")
async def write_tape_label(request: WriteTapeLabelRequest, http_request: Request):
    """写入磁带标签"""
    try:
        from datetime import datetime
        system = http_request.app.state.system
        if not system:
            raise HTTPException(status_code=500, detail="系统未初始化")
        
        # 从数据库中获取磁带的过期时间等信息
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
        conn = psycopg2.connect(
            host=host,
            port=port,
            database=database,
            user=username,
            password=password
        )
        try:
            cur = conn.cursor()
            
            # 查询磁带信息
            cur.execute(
                "SELECT expiry_date, created_date FROM tape_cartridges WHERE tape_id = %s",
                (request.tape_id,)
            )
            result = cur.fetchone()
            
            if not result:
                conn.close()
                raise HTTPException(status_code=404, detail=f"未找到磁带: {request.tape_id}")
            
            expiry_date, created_date = result
            
            # 准备磁带信息
            tape_info = {
                "tape_id": request.tape_id,
                "label": request.label,
                "serial_number": request.serial_number,
                "created_date": created_date or datetime.now(),
                "expiry_date": expiry_date or datetime.now(),
            }
            
            # 写入物理磁带标签
            write_result = await system.tape_manager.tape_operations._write_tape_label(tape_info)
            
            conn.close()
            
            if write_result:
                return {
                    "success": True,
                    "message": f"磁带标签写入成功: {request.label}"
                }
            else:
                return {
                    "success": False,
                    "message": "磁带标签写入失败"
                }
        
        except Exception as e:
            conn.close()
            raise
    
    except HTTPException:
        raise
    except Exception as e:
        logger.error(f"写入磁带标签失败: {str(e)}")
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
    """获取磁带库存统计"""
    try:
        system = request.app.state.system
        if not system:
            raise HTTPException(status_code=500, detail="系统未初始化")

        inventory = await system.tape_manager.get_inventory_status()
        return inventory

    except Exception as e:
        logger.error(f"获取磁带库存失败: {str(e)}")
        raise HTTPException(status_code=500, detail=str(e))


@router.get("/current")
async def get_current_tape(request: Request):
    """获取当前磁带信息"""
    try:
        system = request.app.state.system
        if not system:
            raise HTTPException(status_code=500, detail="系统未初始化")

        tape_info = await system.tape_manager.get_tape_info()
        if tape_info:
            return tape_info
        else:
            return {"message": "当前没有加载的磁带"}

    except Exception as e:
        logger.error(f"获取当前磁带信息失败: {str(e)}")
        raise HTTPException(status_code=500, detail=str(e))


@router.post("/load")
async def load_tape(request: Request, tape_id: str):
    """加载磁带"""
    try:
        system = request.app.state.system
        if not system:
            raise HTTPException(status_code=500, detail="系统未初始化")

        success = await system.tape_manager.load_tape(tape_id)
        if success:
            return {"success": True, "message": f"磁带 {tape_id} 加载成功"}
        else:
            raise HTTPException(status_code=500, detail="磁带加载失败")

    except HTTPException:
        raise
    except Exception as e:
        logger.error(f"加载磁带失败: {str(e)}")
        raise HTTPException(status_code=500, detail=str(e))


@router.post("/unload")
async def unload_tape(request: Request):
    """卸载磁带"""
    try:
        system = request.app.state.system
        if not system:
            raise HTTPException(status_code=500, detail="系统未初始化")

        success = await system.tape_manager.unload_tape()
        if success:
            return {"success": True, "message": "磁带卸载成功"}
        else:
            raise HTTPException(status_code=500, detail="磁带卸载失败")

    except Exception as e:
        logger.error(f"卸载磁带失败: {str(e)}")
        raise HTTPException(status_code=500, detail=str(e))


@router.post("/erase")
async def erase_tape(request: Request, tape_id: str):
    """擦除磁带"""
    try:
        system = request.app.state.system
        if not system:
            raise HTTPException(status_code=500, detail="系统未初始化")

        success = await system.tape_manager.erase_tape(tape_id)
        if success:
            return {"success": True, "message": f"磁带 {tape_id} 擦除成功"}
        else:
            raise HTTPException(status_code=500, detail="磁带擦除失败")

    except Exception as e:
        logger.error(f"擦除磁带失败: {str(e)}")
        raise HTTPException(status_code=500, detail=str(e))


@router.get("/check-format")
async def check_tape_format(request: Request):
    """检查磁带是否已格式化"""
    try:
        system = request.app.state.system
        if not system:
            return {
                "success": False,
                "formatted": False,
                "message": "系统未初始化"
            }
        
        # 检查是否有磁带设备
        if not system.tape_manager.scsi_interface.tape_devices or len(system.tape_manager.scsi_interface.tape_devices) == 0:
            return {
                "success": False,
                "formatted": False,
                "message": "未检测到磁带设备"
            }
        
        # 尝试读取磁带标签，如果成功则认为已格式化
        try:
            metadata = await system.tape_manager.tape_operations._read_tape_label()
            
            return {
                "success": True,
                "formatted": metadata is not None,
                "metadata": metadata if metadata else None
            }
        except Exception as e:
            # 读取失败通常意味着未格式化或磁带为空
            logger.debug(f"读取磁带标签失败（可能未格式化）: {str(e)}")
            return {
                "success": True,
                "formatted": False,
                "metadata": None
            }
    
    except Exception as e:
        # 其他错误
        logger.error(f"检查磁带格式异常: {str(e)}")
        return {
            "success": False,
            "formatted": False,
            "message": str(e)
        }


class FormatRequest(BaseModel):
    """格式化请求模型"""
    force: bool = False


@router.post("/format")
async def format_tape(request: Request, format_request: FormatRequest = FormatRequest()):
    """格式化磁带"""
    try:
        system = request.app.state.system
        if not system:
            return {
                "success": False,
                "message": "系统未初始化"
            }
        
        # 检查是否有磁带设备
        if not system.tape_manager.scsi_interface.tape_devices or len(system.tape_manager.scsi_interface.tape_devices) == 0:
            return {
                "success": False,
                "message": "未检测到磁带设备"
            }
        
        # 先读取现有标签（如果有），格式化后重新写入以保持标签不变
        existing_label = None
        
        # 检查是否已格式化
        try:
            existing_label = await system.tape_manager.tape_operations._read_tape_label()
            if existing_label and not format_request.force:
                # 已格式化且不强制，拒绝
                return {
                    "success": False,
                    "message": "磁带已格式化，如需强制格式化请使用force=true参数"
                }
        except Exception as e:
            logger.debug(f"读取磁带标签失败（继续格式化）: {str(e)}")
        
        # 使用SCSI接口格式化
        success = await system.tape_manager.scsi_interface.format_tape(format_type=0)
        if success:
            # 如果格式化前有标签，重新写入以保持标签不变
            if existing_label:
                try:
                    write_success = await system.tape_manager.tape_operations._write_tape_label(existing_label)
                    if write_success:
                        logger.info(f"格式化后重新写入磁带标签: {existing_label.get('tape_id')}")
                        return {"success": True, "message": "磁带格式化成功，标签已保留"}
                    else:
                        logger.warning("格式化成功，但重新写入标签失败")
                        return {"success": True, "message": "磁带格式化成功（但标签未重写）"}
                except Exception as e:
                    logger.warning(f"重新写入标签时出错: {str(e)}")
                    return {"success": True, "message": "磁带格式化成功（但标签未重写）"}
            
            return {"success": True, "message": "磁带格式化成功"}
        else:
            return {
                "success": False,
                "message": "磁带格式化失败，请检查设备状态和磁带是否正确加载"
            }

    except Exception as e:
        logger.error(f"格式化磁带异常: {str(e)}")
        return {
            "success": False,
            "message": f"格式化失败: {str(e)}"
        }


@router.post("/rewind")
async def rewind_tape(request: Request, tape_id: str = None):
    """倒带"""
    try:
        system = request.app.state.system
        if not system:
            raise HTTPException(status_code=500, detail="系统未初始化")

        # 使用SCSI接口倒带
        success = await system.tape_manager.scsi_interface.rewind_tape()
        if success:
            return {"success": True, "message": "磁带倒带成功"}
        else:
            raise HTTPException(status_code=500, detail="磁带倒带失败")

    except Exception as e:
        logger.error(f"倒带失败: {str(e)}")
        raise HTTPException(status_code=500, detail=str(e))


@router.post("/space")
async def space_tape_blocks(request: Request, blocks: int = 1, direction: str = "forward"):
    """按块定位磁带"""
    try:
        system = request.app.state.system
        if not system:
            raise HTTPException(status_code=500, detail="系统未初始化")

        # 使用SCSI接口定位
        success = await system.tape_manager.scsi_interface.space_blocks(blocks=blocks, direction=direction)
        if success:
            return {"success": True, "message": f"磁带定位成功：{blocks} 块 (方向: {direction})"}
        else:
            raise HTTPException(status_code=500, detail="磁带定位失败")

    except Exception as e:
        logger.error(f"磁带定位失败: {str(e)}")
        raise HTTPException(status_code=500, detail=str(e))


@router.get("/health")
async def check_tape_health(request: Request):
    """检查磁带健康状态"""
    try:
        system = request.app.state.system
        if not system:
            raise HTTPException(status_code=500, detail="系统未初始化")

        health = await system.tape_manager.health_check()
        return {"healthy": health}

    except Exception as e:
        logger.error(f"检查磁带健康状态失败: {str(e)}")
        raise HTTPException(status_code=500, detail=str(e))


@router.get("/devices")
async def get_tape_devices(request: Request):
    """获取磁带设备列表"""
    try:
        system = request.app.state.system
        if not system:
            raise HTTPException(status_code=500, detail="系统未初始化")

        devices = await system.tape_manager.scsi_interface.scan_tape_devices()
        return {"devices": devices}

    except Exception as e:
        logger.error(f"获取磁带设备列表失败: {str(e)}")
        raise HTTPException(status_code=500, detail=str(e))


# IBM LTO特定功能API端点
@router.get("/ibm/alerts")
async def get_ibm_tape_alerts(request: Request):
    """获取IBM磁带警报信息"""
    try:
        system = request.app.state.system
        if not system:
            raise HTTPException(status_code=500, detail="系统未初始化")

        alerts = await system.tape_manager.tape_operations.get_ibm_tape_alerts()
        return alerts

    except Exception as e:
        logger.error(f"获取IBM磁带警报失败: {str(e)}")
        raise HTTPException(status_code=500, detail=str(e))


@router.get("/ibm/performance")
async def get_ibm_performance_stats(request: Request):
    """获取IBM磁带性能统计"""
    try:
        system = request.app.state.system
        if not system:
            raise HTTPException(status_code=500, detail="系统未初始化")

        performance = await system.tape_manager.tape_operations.get_ibm_performance_stats()
        return performance

    except Exception as e:
        logger.error(f"获取IBM性能统计失败: {str(e)}")
        raise HTTPException(status_code=500, detail=str(e))


@router.get("/ibm/usage")
async def get_ibm_tape_usage(request: Request):
    """获取IBM磁带使用统计"""
    try:
        system = request.app.state.system
        if not system:
            raise HTTPException(status_code=500, detail="系统未初始化")

        usage = await system.tape_manager.tape_operations.get_ibm_tape_usage()
        return usage

    except Exception as e:
        logger.error(f"获取IBM磁带使用统计失败: {str(e)}")
        raise HTTPException(status_code=500, detail=str(e))


@router.get("/ibm/temperature")
async def get_ibm_temperature_status(request: Request):
    """获取IBM磁带机温度状态"""
    try:
        system = request.app.state.system
        if not system:
            raise HTTPException(status_code=500, detail="系统未初始化")

        temperature = await system.tape_manager.tape_operations.get_ibm_temperature_status()
        return temperature

    except Exception as e:
        logger.error(f"获取IBM温度状态失败: {str(e)}")
        raise HTTPException(status_code=500, detail=str(e))


@router.get("/ibm/serial")
async def get_ibm_drive_serial(request: Request):
    """获取IBM磁带机序列号"""
    try:
        system = request.app.state.system
        if not system:
            raise HTTPException(status_code=500, detail="系统未初始化")

        serial = await system.tape_manager.tape_operations.get_ibm_drive_serial_number()
        return serial

    except Exception as e:
        logger.error(f"获取IBM序列号失败: {str(e)}")
        raise HTTPException(status_code=500, detail=str(e))


@router.get("/ibm/firmware")
async def get_ibm_firmware_version(request: Request):
    """获取IBM磁带机固件版本"""
    try:
        system = request.app.state.system
        if not system:
            raise HTTPException(status_code=500, detail="系统未初始化")

        firmware = await system.tape_manager.tape_operations.get_ibm_firmware_version()
        return firmware

    except Exception as e:
        logger.error(f"获取IBM固件版本失败: {str(e)}")
        raise HTTPException(status_code=500, detail=str(e))


@router.post("/ibm/self-test")
async def run_ibm_self_test(request: Request):
    """运行IBM磁带机自检"""
    try:
        system = request.app.state.system
        if not system:
            raise HTTPException(status_code=500, detail="系统未初始化")

        result = await system.tape_manager.tape_operations.run_ibm_self_test()
        return result

    except Exception as e:
        logger.error(f"运行IBM自检失败: {str(e)}")
        raise HTTPException(status_code=500, detail=str(e))


@router.post("/ibm/encryption/enable")
async def enable_ibm_encryption(request: Request, encryption_key: Optional[str] = None):
    """启用IBM磁带加密"""
    try:
        system = request.app.state.system
        if not system:
            raise HTTPException(status_code=500, detail="系统未初始化")

        result = await system.tape_manager.tape_operations.enable_ibm_encryption(encryption_key)
        return result

    except Exception as e:
        logger.error(f"启用IBM加密失败: {str(e)}")
        raise HTTPException(status_code=500, detail=str(e))


@router.post("/ibm/encryption/disable")
async def disable_ibm_encryption(request: Request):
    """禁用IBM磁带加密"""
    try:
        system = request.app.state.system
        if not system:
            raise HTTPException(status_code=500, detail="系统未初始化")

        result = await system.tape_manager.tape_operations.disable_ibm_encryption()
        return result

    except Exception as e:
        logger.error(f"禁用IBM加密失败: {str(e)}")
        raise HTTPException(status_code=500, detail=str(e))


@router.post("/ibm/worm/enable")
async def enable_ibm_worm_mode(request: Request):
    """启用IBM WORM模式"""
    try:
        system = request.app.state.system
        if not system:
            raise HTTPException(status_code=500, detail="系统未初始化")

        result = await system.tape_manager.tape_operations.set_ibm_worm_mode(enable=True)
        return result

    except Exception as e:
        logger.error(f"启用IBM WORM模式失败: {str(e)}")
        raise HTTPException(status_code=500, detail=str(e))


@router.post("/ibm/worm/disable")
async def disable_ibm_worm_mode(request: Request):
    """禁用IBM WORM模式"""
    try:
        system = request.app.state.system
        if not system:
            raise HTTPException(status_code=500, detail="系统未初始化")

        result = await system.tape_manager.tape_operations.set_ibm_worm_mode(enable=False)
        return result

    except Exception as e:
        logger.error(f"禁用IBM WORM模式失败: {str(e)}")
        raise HTTPException(status_code=500, detail=str(e))


@router.get("/ibm/position")
async def get_ibm_tape_position(request: Request):
    """获取IBM磁带位置信息"""
    try:
        system = request.app.state.system
        if not system:
            raise HTTPException(status_code=500, detail="系统未初始化")

        position = await system.tape_manager.scsi_interface.get_tape_position()
        return position

    except Exception as e:
        logger.error(f"获取IBM磁带位置失败: {str(e)}")
        raise HTTPException(status_code=500, detail=str(e))


@router.get("/ibm/sense")
async def get_ibm_sense_data(request: Request):
    """获取IBM Sense数据"""
    try:
        system = request.app.state.system
        if not system:
            raise HTTPException(status_code=500, detail="系统未初始化")

        sense_data = await system.tape_manager.scsi_interface.request_sense()
        return sense_data

    except Exception as e:
        logger.error(f"获取IBM Sense数据失败: {str(e)}")
        raise HTTPException(status_code=500, detail=str(e))


@router.post("/ibm/log-sense")
async def send_ibm_log_sense(request: Request, page_code: int = 0x00, subpage_code: int = 0x00):
    """发送IBM LOG SENSE命令"""
    try:
        system = request.app.state.system
        if not system:
            raise HTTPException(status_code=500, detail="系统未初始化")

        parameters = {
            'page_code': page_code,
            'subpage_code': subpage_code
        }
        result = await system.tape_manager.scsi_interface.send_ibm_specific_command(
            device_path=None,
            command_type="log_sense",
            parameters=parameters
        )
        return result

    except Exception as e:
        logger.error(f"发送IBM LOG SENSE失败: {str(e)}")
        raise HTTPException(status_code=500, detail=str(e))


@router.post("/ibm/mode-sense")
async def send_ibm_mode_sense(request: Request, page_code: int = 0x3F, subpage_code: int = 0x00):
    """发送IBM MODE SENSE命令"""
    try:
        system = request.app.state.system
        if not system:
            raise HTTPException(status_code=500, detail="系统未初始化")

        parameters = {
            'page_code': page_code,
            'subpage_code': subpage_code
        }
        result = await system.tape_manager.scsi_interface.send_ibm_specific_command(
            device_path=None,
            command_type="mode_sense",
            parameters=parameters
        )
        return result

    except Exception as e:
        logger.error(f"发送IBM MODE SENSE失败: {str(e)}")
        raise HTTPException(status_code=500, detail=str(e))


@router.post("/ibm/inquiry-vpd")
async def send_ibm_inquiry_vpd(request: Request, page_code: int = 0x00):
    """发送IBM INQUIRY VPD命令"""
    try:
        system = request.app.state.system
        if not system:
            raise HTTPException(status_code=500, detail="系统未初始化")

        parameters = {'page_code': page_code}
        result = await system.tape_manager.scsi_interface.send_ibm_specific_command(
            device_path=None,
            command_type="inquiry_vpd",
            parameters=parameters
        )
        return result

    except Exception as e:
        logger.error(f"发送IBM INQUIRY VPD失败: {str(e)}")
        raise HTTPException(status_code=500, detail=str(e))


@router.get("/generate-uuid")
async def generate_uuid(request: Request):
    """生成UUID序列号"""
    try:
        u = uuid.uuid4()
        
        # 使用HEX格式（全大写无连字符，32字符）作为序列号
        serial_number = u.hex.upper()
        
        # 返回多种格式的UUID
        result = {
            "success": True,
            "serial_number": serial_number,  # 主要返回：全大写无连字符格式（32字符）
            "uuid": {
                "str": str(u),                    # 标准带连字符 36 字符
                "hex": u.hex,                     # 去掉连字符 32 字符（小写）
                "hex_upper": serial_number,       # 全大写无连字符 32 字符（推荐用于序列号）
                "str_upper": str(u).upper(),      # 全大写有连字符
                "braces": f"{{{u}}}",            # 花括号格式
                "urn": u.urn,                     # URN 标准格式
                "base64": base64.urlsafe_b64encode(u.bytes).decode().rstrip('='),  # base64 短串（22 字符）
                "int": u.int,                     # 128 bit 大整数
                "bits": format(u.int, '0128b'),   # 比特串 128 位
            }
        }
        
        logger.info(f"生成UUID序列号: {serial_number}")
        return result
        
    except Exception as e:
        logger.error(f"生成UUID失败: {str(e)}", exc_info=True)
        raise HTTPException(status_code=500, detail=str(e))