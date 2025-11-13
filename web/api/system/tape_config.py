#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
系统管理API - tape_config
System Management API - tape_config
"""

import logging
import traceback
from typing import Dict, Any, Optional
from datetime import datetime
from fastapi import APIRouter, HTTPException, Request
from pydantic import BaseModel, Field

from .models import TapeConfig
from models.system_log import OperationType, LogCategory, LogLevel
from utils.log_utils import log_operation, log_system

logger = logging.getLogger(__name__)
router = APIRouter()

@router.get("/tape/config")
async def get_tape_config(request: Request):
    """获取磁带机配置"""
    try:
        from config.settings import get_settings
        settings = get_settings()
        
        # 返回当前磁带机配置
        config = {
            "tape_device_path": settings.TAPE_DEVICE_PATH,
            "tape_drive_letter": settings.TAPE_DRIVE_LETTER,
            "default_block_size": settings.DEFAULT_BLOCK_SIZE,
            "max_volume_size": settings.MAX_VOLUME_SIZE,
            "tape_pool_size": settings.TAPE_POOL_SIZE,
            "tape_check_interval": settings.TAPE_CHECK_INTERVAL,
            "auto_tape_cleanup": settings.AUTO_TAPE_CLEANUP,
            # 新增：工具路径配置
            "itdt_path": getattr(settings, 'ITDT_PATH', ''),
            "ltfs_tools_dir": getattr(settings, 'LTFS_TOOLS_DIR', ''),
            "default_device_address": "0.0.24.0"  # 固定默认值
        }
        
        # 检查设备状态（优先使用缓存）
        status = {"connected": False, "device_info": "未检测"}
        try:
            system = request.app.state.system
            if system:
                devices = await system.tape_manager.get_cached_devices()
                if devices:
                    status = {
                        "connected": True,
                        "device_info": devices[0].get('model', devices[0].get('path', 'Unknown')),
                        "device_path": devices[0].get('path', '')
                    }
        except Exception as e:
            logger.debug(f"检查设备状态失败: {str(e)}")
        
        return {"success": True, "config": config, "status": status}
        
    except Exception as e:
        logger.error(f"获取磁带机配置失败: {str(e)}")
        return {"success": False, "message": str(e)}


@router.post("/tape/test")
async def test_tape_connection(config: TapeConfig, request: Request):
    """测试磁带机连接"""
    start_time = datetime.now()
    ip_address = request.client.host if request.client else None
    request_method = "POST"
    request_url = str(request.url)
    
    try:
        from config.settings import get_settings
        settings = get_settings()
        
        # 使用 ITDT 测试设备连接（优先使用缓存）
        try:
            system = request.app.state.system
            if not system:
                raise HTTPException(status_code=500, detail="系统未初始化")
            
            # 优先使用缓存设备
            devices = await system.tape_manager.get_cached_devices()
            
            duration_ms = int((datetime.now() - start_time).total_seconds() * 1000)
            
            # 如果找到设备，测试连接
            if devices and len(devices) > 0:
                device = devices[0]
                device_path = device.get('path', '')
                
                # 测试设备就绪状态
                try:
                    ready = await system.tape_manager.itdt_interface.test_unit_ready(device_path)
                    status_msg = "设备已就绪" if ready else "设备已检测到但未就绪"
                except Exception as e:
                    logger.warning(f"测试设备就绪状态失败: {str(e)}")
                    ready = False
                    status_msg = f"设备已检测到（{str(e)}）"
                
                device_info = f"{device.get('vendor', 'Unknown')} {device.get('model', 'Unknown')}"
                
                await log_operation(
                    operation_type=OperationType.EXECUTE,
                    resource_type="tape_drive",
                    resource_name=device_info,
                    operation_name="测试磁带机连接",
                    operation_description=f"测试磁带机连接: {device_path}",
                    category="tape",
                    success=True,
                    result_message=f"磁带机连接测试成功: {device_info}",
                    ip_address=ip_address,
                    request_method=request_method,
                    request_url=request_url,
                    duration_ms=duration_ms
                )
                
                # 只要扫描到设备就认为连接成功
                return {
                    "success": True,
                    "message": "磁带机连接测试成功",
                    "connected": True,
                    "device_info": device_info,
                    "status": status_msg,
                    "device_path": device_path
                }
            else:
                device_path = config.tape_device_path if config.tape_device_path else settings.TAPE_DEVICE_PATH
                await log_operation(
                    operation_type=OperationType.EXECUTE,
                    resource_type="tape_drive",
                    operation_name="测试磁带机连接",
                    operation_description=f"测试磁带机连接: {device_path}",
                    category="tape",
                    success=False,
                    error_message="未检测到磁带设备",
                    ip_address=ip_address,
                    request_method=request_method,
                    request_url=request_url,
                    duration_ms=duration_ms
                )
                return {
                    "success": False,
                    "message": "未检测到磁带设备",
                    "connected": False,
                    "error": "请检查设备是否连接"
                }
                
        except Exception as itdt_error:
            duration_ms = int((datetime.now() - start_time).total_seconds() * 1000)
            error_msg = f"ITDT接口测试失败: {str(itdt_error)}"
            logger.error(error_msg, exc_info=True)
            device_path = config.tape_device_path if config.tape_device_path else settings.TAPE_DEVICE_PATH
            await log_operation(
                operation_type=OperationType.EXECUTE,
                resource_type="tape_drive",
                operation_name="测试磁带机连接",
                operation_description=f"测试磁带机连接失败",
                category="tape",
                success=False,
                error_message=str(itdt_error),
                ip_address=ip_address,
                request_method=request_method,
                request_url=request_url,
                duration_ms=duration_ms
            )
            await log_system(
                level=LogLevel.ERROR,
                category=LogCategory.TAPE,
                message=error_msg,
                module="web.api.system.tape_config",
                function="test_tape_connection",
                exception_type=type(itdt_error).__name__,
                stack_trace=traceback.format_exc(),
                duration_ms=duration_ms
            )
            return {
                "success": False,
                "message": f"连接测试失败: {str(itdt_error)}",
                "connected": False,
                "error": str(itdt_error)
            }
        
    except Exception as e:
        duration_ms = int((datetime.now() - start_time).total_seconds() * 1000)
        error_msg = f"测试磁带机连接失败: {str(e)}"
        logger.error(error_msg, exc_info=True)
        await log_operation(
            operation_type=OperationType.EXECUTE,
            resource_type="tape_drive",
            operation_name="测试磁带机连接",
            operation_description="测试磁带机连接失败",
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
            module="web.api.system.tape_config",
            function="test_tape_connection",
            exception_type=type(e).__name__,
            stack_trace=traceback.format_exc(),
            duration_ms=duration_ms
        )
        return {
            "success": False,
            "message": f"测试失败: {str(e)}",
            "connected": False,
            "error": str(e)
        }


@router.put("/tape/config")
async def update_tape_config(config: TapeConfig, request: Request):
    """更新磁带机配置"""
    start_time = datetime.now()
    ip_address = request.client.host if request.client else None
    request_method = "PUT"
    request_url = str(request.url)
    
    try:
        import os
        from pathlib import Path
        from config.settings import get_settings
        
        # 获取旧配置用于日志记录
        settings = get_settings()
        old_values = {
            "tape_device_path": settings.TAPE_DEVICE_PATH,
            "tape_drive_letter": settings.TAPE_DRIVE_LETTER,
            "default_block_size": settings.DEFAULT_BLOCK_SIZE,
            "max_volume_size": settings.MAX_VOLUME_SIZE,
            "tape_pool_size": settings.TAPE_POOL_SIZE,
            "tape_check_interval": settings.TAPE_CHECK_INTERVAL,
            "auto_tape_cleanup": settings.AUTO_TAPE_CLEANUP
        }
        
        # 保存配置到.env文件
        env_file = Path(".env")
        env_lines = []
        
        if env_file.exists():
            with open(env_file, "r", encoding="utf-8") as f:
                env_lines = f.readlines()
        
        # 更新或添加磁带机配置
        config_keys = {
            "TAPE_DEVICE_PATH": config.tape_device_path,
            "TAPE_DRIVE_LETTER": config.tape_drive_letter,
            "DEFAULT_BLOCK_SIZE": str(config.default_block_size),
            "MAX_VOLUME_SIZE": str(config.max_volume_size),
            "TAPE_POOL_SIZE": str(config.tape_pool_size),
            "TAPE_CHECK_INTERVAL": str(config.tape_check_interval),
            "AUTO_TAPE_CLEANUP": "true" if config.auto_tape_cleanup else "false"
        }
        
        # 更新现有配置或添加新配置
        updated_keys = set()
        for i, line in enumerate(env_lines):
            line_stripped = line.strip()
            for key, value in config_keys.items():
                if line_stripped.startswith(key + "="):
                    env_lines[i] = f"{key}={value}\n"
                    updated_keys.add(key)
        
        # 添加未更新的配置
        for key, value in config_keys.items():
            if key not in updated_keys:
                env_lines.append(f"{key}={value}\n")
        
        # 写入文件
        with open(env_file, "w", encoding="utf-8") as f:
            f.writelines(env_lines)
        
        logger.info("磁带机配置已更新")
        
        # 准备新值
        new_values = {
            "tape_device_path": config.tape_device_path,
            "tape_drive_letter": config.tape_drive_letter,
            "default_block_size": config.default_block_size,
            "max_volume_size": config.max_volume_size,
            "tape_pool_size": config.tape_pool_size,
            "tape_check_interval": config.tape_check_interval,
            "auto_tape_cleanup": config.auto_tape_cleanup
        }
        
        # 计算变更字段
        changed_fields = []
        for key in old_values:
            if old_values[key] != new_values[key]:
                changed_fields.append(key)
        
        duration_ms = int((datetime.now() - start_time).total_seconds() * 1000)
        
        await log_operation(
            operation_type=OperationType.CONFIG,
            resource_type="tape_drive",
            resource_name="磁带机配置",
            operation_name="更新磁带机配置",
            operation_description="更新磁带机配置",
            category="tape",
            success=True,
            result_message="磁带机配置更新成功",
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
            "message": "磁带机配置更新成功"
        }
        
    except HTTPException:
        raise
    except Exception as e:
        duration_ms = int((datetime.now() - start_time).total_seconds() * 1000)
        error_msg = f"更新磁带机配置失败: {str(e)}"
        logger.error(error_msg, exc_info=True)
        await log_operation(
            operation_type=OperationType.CONFIG,
            resource_type="tape_drive",
            resource_name="磁带机配置",
            operation_name="更新磁带机配置",
            operation_description="更新磁带机配置",
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
            module="web.api.system.tape_config",
            function="update_tape_config",
            exception_type=type(e).__name__,
            stack_trace=traceback.format_exc(),
            duration_ms=duration_ms
        )
        raise HTTPException(status_code=500, detail=str(e))


@router.get("/tape/scan")
async def scan_tape_devices(request: Request, force_generic: bool = True, show_all_paths: bool = True, force_rescan: bool = False):
    """扫描磁带设备（默认使用缓存，force_rescan=true时强制重新扫描）"""
    start_time = datetime.now()
    ip_address = request.client.host if request.client else None
    request_method = "GET"
    request_url = str(request.url)
    
    try:
        system = request.app.state.system
        if not system:
            raise HTTPException(status_code=500, detail="系统未初始化")
        
        # 如果不需要强制重新扫描，优先使用缓存
        if not force_rescan:
            cached_devices = await system.tape_manager.get_cached_devices()
            if cached_devices:
                logger.info(f"使用缓存设备列表: {len(cached_devices)} 个设备")
                duration_ms = int((datetime.now() - start_time).total_seconds() * 1000)
                return {
                    "success": True,
                    "devices": cached_devices,
                    "count": len(cached_devices),
                    "cached": True,
                    "message": "使用缓存设备列表"
                }
        
        # 强制重新扫描或缓存为空
        try:
            from tape.itdt_interface import ITDTInterface
            itdt = ITDTInterface()
            await itdt.initialize()
            # 根据参数临时调整扫描行为
            settings = None
            try:
                from config.settings import get_settings
                settings = get_settings()
                settings.ITDT_FORCE_GENERIC_DD = bool(force_generic)
                settings.ITDT_SCAN_SHOW_ALL_PATHS = bool(show_all_paths)
            except Exception:
                pass
            # 扫描设备（ITDT 不需要 -f）
            devices = await itdt.scan_devices()
            
            # 更新缓存
            if devices:
                system.tape_manager._save_cached_devices(devices)
                system.tape_manager.cached_devices = devices
            
            duration_ms = int((datetime.now() - start_time).total_seconds() * 1000)
            
            if devices and len(devices) > 0:
                device_info = f"检测到 {len(devices)} 个设备"
                await log_operation(
                    operation_type=OperationType.TAPE_SCAN,
                    resource_type="tape_drive",
                    operation_name="扫描磁带设备",
                    operation_description="扫描磁带设备",
                    category="tape",
                    success=True,
                    result_message=device_info,
                    ip_address=ip_address,
                    request_method=request_method,
                    request_url=request_url,
                    duration_ms=duration_ms
                )
                return {
                    "success": True,
                    "devices": devices,
                    "count": len(devices)
                }
            else:
                await log_operation(
                    operation_type=OperationType.TAPE_SCAN,
                    resource_type="tape_drive",
                    operation_name="扫描磁带设备",
                    operation_description="扫描磁带设备",
                    category="tape",
                    success=True,
                    result_message="未检测到磁带设备",
                    ip_address=ip_address,
                    request_method=request_method,
                    request_url=request_url,
                    duration_ms=duration_ms
                )
                return {
                    "success": True,
                    "devices": [],
                    "count": 0,
                    "message": "未检测到磁带设备"
                }
                
        except Exception as itdt_error:
            duration_ms = int((datetime.now() - start_time).total_seconds() * 1000)
            error_msg = f"扫描磁带设备失败: {str(itdt_error)}"
            logger.error(error_msg, exc_info=True)
            await log_operation(
                operation_type=OperationType.TAPE_SCAN,
                resource_type="tape_drive",
                operation_name="扫描磁带设备",
                operation_description="扫描磁带设备",
                category="tape",
                success=False,
                error_message=str(itdt_error),
                ip_address=ip_address,
                request_method=request_method,
                request_url=request_url,
                duration_ms=duration_ms
            )
            await log_system(
                level=LogLevel.ERROR,
                category=LogCategory.TAPE,
                message=error_msg,
                module="web.api.system.tape_config",
                function="scan_tape_devices",
                exception_type=type(itdt_error).__name__,
                stack_trace=traceback.format_exc(),
                duration_ms=duration_ms
            )
            return {
                "success": False,
                "devices": [],
                "count": 0,
                "message": f"扫描失败: {str(itdt_error)}"
            }
        
    except Exception as e:
        duration_ms = int((datetime.now() - start_time).total_seconds() * 1000)
        error_msg = f"扫描磁带设备失败: {str(e)}"
        logger.error(error_msg, exc_info=True)
        await log_operation(
            operation_type=OperationType.TAPE_SCAN,
            resource_type="tape_drive",
            operation_name="扫描磁带设备",
            operation_description="扫描磁带设备",
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
            module="web.api.system.tape_config",
            function="scan_tape_devices",
            exception_type=type(e).__name__,
            stack_trace=traceback.format_exc(),
            duration_ms=duration_ms
        )
        return {
            "success": False,
            "devices": [],
            "count": 0,
            "message": str(e)
        }


@router.get("/tape/history")
async def get_tape_drive_history(request: Request, limit: int = 50, offset: int = 0):
    """获取磁带机操作历史（从新的日志系统获取，使用openGauss原生SQL）"""
    start_time = datetime.now()
    try:
        from config.settings import get_settings
        
        settings = get_settings()
        database_url = settings.DATABASE_URL
        
        # 检查是否为openGauss
        from utils.scheduler.db_utils import is_opengauss, get_opengauss_connection
        
        if not is_opengauss():
            # 非openGauss数据库，使用SQLAlchemy
            from config.database import db_manager
            from models.system_log import OperationLog
            from sqlalchemy import select, desc, or_
            
            async with db_manager.AsyncSessionLocal() as session:
                # 查询磁带机相关操作日志（resource_type = 'tape_drive' 或操作名称包含'磁带机'）
                query = select(OperationLog).where(
                    or_(
                        OperationLog.resource_type == "tape_drive",
                        OperationLog.operation_name.like("%磁带机%"),
                        OperationLog.operation_description.like("%磁带机%")
                    )
                ).order_by(desc(OperationLog.operation_time)).limit(limit).offset(offset)
                
                result = await session.execute(query)
                operation_logs = result.scalars().all()
                
                history = []
                for log in operation_logs:
                    history.append({
                        "id": log.id,
                        "time": log.operation_time.isoformat() if log.operation_time else None,
                        "operation": log.operation_name or log.operation_description or "",
                        "device_name": log.resource_name or "",
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
            # 使用连接池
            async with get_opengauss_connection() as conn:
                # 查询磁带机相关操作日志（resource_type = 'tape_drive' 或操作名称包含'磁带机'）
                sql = """
                    SELECT 
                        id, operation_time, resource_name, 
                        operation_name, operation_description, username,
                        success, result_message, error_message, operation_type
                    FROM operation_logs
                    WHERE resource_type = $1 
                       OR operation_name LIKE $2 
                       OR operation_description LIKE $3
                    ORDER BY operation_time DESC
                    LIMIT $4 OFFSET $5
                """
                
                rows = await conn.fetch(sql, "tape_drive", "%磁带机%", "%磁带机%", limit, offset)
                
                history = []
                for row in rows:
                    operation_name = row['operation_name'] or row['operation_description'] or ""
                    # 如果operation_type是枚举值，转换为字符串
                    operation_type = row['operation_type']
                    if hasattr(operation_type, 'value'):
                        operation_type = operation_type.value
                    else:
                        operation_type = str(operation_type) if operation_type else ""
                    
                    message = row['result_message'] or row['error_message'] or row['operation_description'] or ""
                    
                    history.append({
                        "id": row['id'],
                        "time": row['operation_time'].isoformat() if row['operation_time'] else None,
                        "operation": operation_name,
                        "device_name": row['resource_name'] or "",
                        "username": row['username'] or "system",
                        "success": row['success'],
                        "message": message,
                        "operation_type": operation_type
                    })
                
                duration_ms = int((datetime.now() - start_time).total_seconds() * 1000)
                await log_system(
                    level=LogLevel.INFO,
                    category=LogCategory.TAPE,
                    message=f"获取磁带机操作历史成功: {len(history)} 条记录",
                    module="web.api.system.tape_config",
                    function="get_tape_drive_history",
                    duration_ms=duration_ms
                )
                
                return {
                    "success": True,
                    "history": history,
                    "count": len(history)
                }
    
    except Exception as e:
        duration_ms = int((datetime.now() - start_time).total_seconds() * 1000)
        error_msg = f"获取磁带机操作历史失败: {str(e)}"
        logger.error(error_msg, exc_info=True)
        await log_system(
            level=LogLevel.ERROR,
            category=LogCategory.TAPE,
            message=error_msg,
            module="web.api.system.tape_config",
            function="get_tape_drive_history",
            exception_type=type(e).__name__,
            stack_trace=traceback.format_exc(),
            duration_ms=duration_ms
        )
        raise HTTPException(status_code=500, detail=str(e))

