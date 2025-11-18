#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
系统管理API - database
System Management API - database
"""

import logging
import traceback
from typing import Dict, Any, Optional
from datetime import datetime
from fastapi import APIRouter, HTTPException, Request
from pydantic import BaseModel, Field

from .models import DatabaseConfig
from models.system_log import OperationType, LogCategory, LogLevel
from utils.log_utils import log_operation, log_system

logger = logging.getLogger(__name__)
router = APIRouter()

@router.get("/database/config")
async def get_database_config():
    """获取数据库配置"""
    try:
        from config.settings import get_settings
        settings = get_settings()
        
        # 解析当前数据库URL
        db_url = settings.DATABASE_URL
        db_info = {
            "db_type": "sqlite",
            "pool_size": settings.DB_POOL_SIZE,
            "max_overflow": settings.DB_MAX_OVERFLOW
        }
        
        # 优先使用 DB_FLAVOR 配置获取数据库类型
        if settings.DB_FLAVOR:
            db_info["db_type"] = settings.DB_FLAVOR
        # 否则从 DATABASE_URL 推断数据库类型
        elif db_url.startswith("sqlite"):
            db_info["db_type"] = "sqlite"
        elif db_url.startswith("postgresql://") or db_url.startswith("opengauss://"):
            db_info["db_type"] = "opengauss" if db_url.startswith("opengauss") else "postgresql"
        
        # 根据数据库类型设置相应参数
        if db_info["db_type"] == "sqlite":
            db_info["db_path"] = db_url.replace("sqlite:///", "")
        else:
            # 提取连接参数
            db_info["db_host"] = settings.DB_HOST
            db_info["db_port"] = settings.DB_PORT
            db_info["db_user"] = settings.DB_USER
            db_info["db_database"] = settings.DB_DATABASE
            db_info["db_password"] = settings.DB_PASSWORD or ""  # 添加密码字段，如果为空则返回空字符串
        
        return db_info
        
    except Exception as e:
        logger.error(f"获取数据库配置失败: {str(e)}")
        return {
            "db_type": "sqlite",
            "db_path": "./data/backup_system.db",
            "pool_size": 10,
            "max_overflow": 20
        }


@router.post("/database/test")
async def test_database_connection(config: DatabaseConfig):
    """测试数据库连接"""
    try:
        from config.database import DatabaseManager
        
        # 构建数据库URL
        if config.db_type == "sqlite":
            if not config.db_path:
                raise ValueError("SQLite数据库需要指定路径")
            db_url = f"sqlite:///{config.db_path}"
        elif config.db_type in ["postgresql", "opengauss"]:
            if not all([config.db_host, config.db_port, config.db_user, config.db_database]):
                raise ValueError("PostgreSQL/openGauss数据库需要完整的连接参数")
            # 将opengauss转换为postgresql协议（兼容）
            db_protocol = "postgresql://" if config.db_type == "opengauss" else f"{config.db_type}://"
            db_url = f"{db_protocol}{config.db_user}:{config.db_password}@{config.db_host}:{config.db_port}/{config.db_database}"
        else:
            raise ValueError(f"不支持的数据库类型: {config.db_type}")
        
        # 创建临时数据库管理器进行测试
        temp_db = DatabaseManager()
        # 手动设置URL进行测试
        temp_db.settings.DATABASE_URL = db_url
        temp_db.settings.DB_POOL_SIZE = config.pool_size
        temp_db.settings.DB_MAX_OVERFLOW = config.max_overflow
        
        # 尝试初始化连接
        await temp_db.initialize()
        
        # 测试查询
        success = await temp_db.health_check()
        
        # 关闭连接
        await temp_db.close()
        
        if success:
            return {
                "success": True,
                "message": "数据库连接测试成功",
                "db_type": config.db_type
            }
        else:
            return {
                "success": False,
                "message": "数据库连接测试失败"
            }
            
    except Exception as e:
        logger.error(f"测试数据库连接失败: {str(e)}")
        return {
            "success": False,
            "message": f"连接失败: {str(e)}"
        }


@router.put("/database/config")
async def update_database_config(config: DatabaseConfig, request: Request):
    """更新数据库配置"""
    start_time = datetime.now()
    old_values = {}
    new_values = {}
    
    try:
        import os
        from pathlib import Path
        from config.settings import get_settings
        
        # 获取当前配置，用于填充缺失的密码和记录旧值
        current_settings = get_settings()
        old_values = {
            "db_type": "sqlite" if current_settings.DATABASE_URL.startswith("sqlite") else ("opengauss" if current_settings.DATABASE_URL.startswith("opengauss") else "postgresql"),
            "db_host": current_settings.DB_HOST or "",
            "db_port": current_settings.DB_PORT or 0,
            "db_user": current_settings.DB_USER or "",
            "db_database": current_settings.DB_DATABASE or "",
            "pool_size": current_settings.DB_POOL_SIZE,
            "max_overflow": current_settings.DB_MAX_OVERFLOW
        }
        
        logger.info(f"更新数据库配置: type={config.db_type}, host={config.db_host}, user={config.db_user}")
        
        # 验证配置
        if config.db_type == "sqlite":
            if not config.db_path:
                raise ValueError("SQLite数据库需要指定路径")
            # 创建目录
            Path(config.db_path).parent.mkdir(parents=True, exist_ok=True)
            db_url = f"sqlite:///{config.db_path}"
        elif config.db_type in ["postgresql", "opengauss"]:
            # 如果密码为空，使用当前配置的密码
            if not config.db_password:
                # 从当前URL提取密码
                current_url = current_settings.DATABASE_URL
                if "@" in current_url and (current_url.startswith("postgresql://") or current_url.startswith("opengauss://")):
                    try:
                        # 解析当前URL获取密码
                        parts = current_url.split("@")
                        auth_part = parts[0].split("://")[1]
                        if ":" in auth_part:
                            _, existing_password = auth_part.split(":", 1)
                            config.db_password = existing_password
                    except:
                        pass
                
                # 如果仍然没有密码，使用当前设置的DB_PASSWORD
                if not config.db_password:
                    config.db_password = current_settings.DB_PASSWORD
            
            # 验证必需参数（允许空字符串，将使用当前配置）
            if not all([config.db_host, config.db_port, config.db_user, config.db_database]):
                raise ValueError("PostgreSQL/openGauss数据库需要主机、端口、用户名和数据库名")
            
            # 确保有密码
            if not config.db_password:
                raise ValueError("PostgreSQL/openGauss数据库密码不能为空")
            
            # 将opengauss转换为postgresql协议（兼容）
            db_protocol = "postgresql://" if config.db_type == "opengauss" else f"{config.db_type}://"
            db_url = f"{db_protocol}{config.db_user}:{config.db_password}@{config.db_host}:{config.db_port}/{config.db_database}"
        else:
            raise ValueError(f"不支持的数据库类型: {config.db_type}")
        
        # 保存配置到.env文件
        env_file = Path(".env")
        env_lines = []
        
        if env_file.exists():
            with open(env_file, "r", encoding="utf-8") as f:
                env_lines = f.readlines()
        
        # 更新或添加数据库配置
        config_keys = {
            "DATABASE_URL": db_url,
            "DB_FLAVOR": config.db_type,  # 保存数据库类型
            "DB_HOST": config.db_host or "",
            "DB_PORT": str(config.db_port or ""),
            "DB_USER": config.db_user or "",
            "DB_PASSWORD": config.db_password or "",
            "DB_DATABASE": config.db_database or "",
            "DB_POOL_SIZE": str(config.pool_size),
            "DB_MAX_OVERFLOW": str(config.max_overflow)
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
        
        # 记录新值（隐藏密码）
        new_values = {
            "db_type": config.db_type,
            "db_host": config.db_host or "",
            "db_port": config.db_port or 0,
            "db_user": config.db_user or "",
            "db_database": config.db_database or "",
            "pool_size": config.pool_size,
            "max_overflow": config.max_overflow
        }
        
        # 计算持续时间
        duration_ms = int((datetime.now() - start_time).total_seconds() * 1000)
        
        # 记录操作日志
        changed_fields = []
        for key in new_values:
            if old_values.get(key) != new_values.get(key):
                changed_fields.append(key)
        
        # 获取客户端IP
        client_ip = request.client.host if request.client else None
        
        await log_operation(
            operation_type=OperationType.CONFIG,
            resource_type="database",
            resource_name="数据库配置",
            operation_name="更新数据库配置",
            operation_description=f"更新数据库配置: {config.db_type}",
            category="system",
            success=True,
            result_message="数据库配置更新成功",
            old_values=old_values,
            new_values=new_values,
            changed_fields=changed_fields,
            ip_address=client_ip,
            request_method="PUT",
            request_url=str(request.url),
            duration_ms=duration_ms
        )
        
        # 记录系统日志
        await log_system(
            level=LogLevel.INFO,
            category=LogCategory.SYSTEM,
            message=f"数据库配置已更新: {config.db_type}",
            details={
                "db_type": config.db_type,
                "db_host": config.db_host,
                "db_port": config.db_port,
                "db_user": config.db_user,
                "db_database": config.db_database,
                "pool_size": config.pool_size,
                "max_overflow": config.max_overflow
            },
            module="web.api.system.database",
            function="update_database_config"
        )
        
        logger.info(f"数据库配置已更新: {config.db_type}")
        
        return {
            "success": True,
            "message": "数据库配置更新成功",
            "db_url": db_url.split("@")[0] + "@***" if "@" in db_url else db_url  # 隐藏密码
        }
        
    except Exception as e:
        error_msg = str(e)
        duration_ms = int((datetime.now() - start_time).total_seconds() * 1000)
        
        # 记录失败的操作日志
        client_ip = request.client.host if request.client else None
        await log_operation(
            operation_type=OperationType.CONFIG,
            resource_type="database",
            resource_name="数据库配置",
            operation_name="更新数据库配置",
            operation_description=f"更新数据库配置失败: {error_msg}",
            category="system",
            success=False,
            error_message=error_msg,
            old_values=old_values,
            ip_address=client_ip,
            request_method="PUT",
            request_url=str(request.url),
            duration_ms=duration_ms
        )
        
        # 记录系统错误日志
        await log_system(
            level=LogLevel.ERROR,
            category=LogCategory.SYSTEM,
            message=f"更新数据库配置失败: {error_msg}",
            details={
                "error": error_msg,
                "config": {
                    "db_type": config.db_type,
                    "db_host": config.db_host,
                    "db_port": config.db_port,
                    "db_user": config.db_user,
                    "db_database": config.db_database
                }
            },
            module="web.api.system.database",
            function="update_database_config",
            exception_type=type(e).__name__,
            stack_trace=traceback.format_exc()
        )
        
        logger.error(f"更新数据库配置失败: {error_msg}", exc_info=True)
        raise HTTPException(status_code=500, detail=error_msg)


@router.get("/database/status")
async def get_database_status(request: Request):
    """获取数据库状态"""
    try:
        system = request.app.state.system
        if not system:
            return {
                "status": "unknown",
                "message": "系统未初始化"
            }
        
        from config.settings import get_settings
        settings = get_settings()
        
        # 检查数据库连接状态
        db_healthy = await system.db_manager.health_check()
        
        db_info = {
            "status": "online" if db_healthy else "offline",
            "db_type": "unknown",
            "pool_size": settings.DB_POOL_SIZE,
            "max_overflow": settings.DB_MAX_OVERFLOW
        }
        
        # 解析数据库类型
        db_url = settings.DATABASE_URL
        if db_url.startswith("sqlite"):
            db_info["db_type"] = "SQLite"
            db_info["db_path"] = db_url.replace("sqlite:///", "")
        elif db_url.startswith("opengauss://"):
            db_info["db_type"] = "openGauss"
        elif db_url.startswith("postgresql://"):
            # 需要检查是否是从opengauss转换来的
            # 通过检查DB_HOST等参数是否匹配来判断
            db_info["db_type"] = "PostgreSQL"
        
        return db_info
        
    except Exception as e:
        logger.error(f"获取数据库状态失败: {str(e)}")
        return {
            "status": "error",
            "message": str(e)
        }

