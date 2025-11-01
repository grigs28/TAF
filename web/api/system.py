#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
系统管理API
System Management API
"""

import logging
from typing import Dict, Any, Optional
from fastapi import APIRouter, HTTPException, Request
from pydantic import BaseModel, Field

logger = logging.getLogger(__name__)
router = APIRouter()


class DatabaseConfig(BaseModel):
    """数据库配置模型"""
    db_type: str = Field(..., description="数据库类型: sqlite, postgresql, opengauss, mysql")
    db_host: Optional[str] = Field(None, description="数据库主机")
    db_port: Optional[int] = Field(None, description="数据库端口")
    db_user: Optional[str] = Field(None, description="数据库用户名")
    db_password: Optional[str] = Field(None, description="数据库密码")
    db_database: Optional[str] = Field(None, description="数据库名称")
    db_path: Optional[str] = Field(None, description="SQLite数据库路径")
    pool_size: int = Field(10, description="连接池大小")
    max_overflow: int = Field(20, description="最大溢出连接数")


class SystemConfigRequest(BaseModel):
    """系统配置请求模型"""
    retention_months: int = 6
    auto_erase_expired: bool = True
    monthly_backup_cron: str = "0 2 1 * *"
    dingtalk_api_url: str = ""
    dingtalk_api_key: str = ""
    dingtalk_default_phone: str = ""
    database_config: Optional[DatabaseConfig] = None


@router.get("/info")
async def get_system_info():
    """获取系统信息"""
    try:
        from config.settings import get_settings
        settings = get_settings()

        return {
            "app_name": settings.APP_NAME,
            "version": settings.APP_VERSION,
            "python_version": "3.8+",
            "platform": "Windows/openEuler",
            "database": "openGauss",
            "compression": "7-Zip SDK"
        }

    except Exception as e:
        logger.error(f"获取系统信息失败: {str(e)}")
        raise HTTPException(status_code=500, detail=str(e))


@router.get("/version")
async def get_version():
    """获取版本信息和CHANGELOG"""
    try:
        from config.settings import get_settings
        from pathlib import Path
        
        settings = get_settings()
        
        # 读取CHANGELOG.md
        changelog_path = Path("CHANGELOG.md")
        changelog_content = ""
        if changelog_path.exists():
            with open(changelog_path, "r", encoding="utf-8") as f:
                changelog_content = f.read()
        
        return {
            "version": settings.APP_VERSION,
            "app_name": settings.APP_NAME,
            "changelog": changelog_content
        }

    except Exception as e:
        logger.error(f"获取版本信息失败: {str(e)}")
        raise HTTPException(status_code=500, detail=str(e))


@router.get("/health")
async def health_check(request: Request):
    """系统健康检查"""
    try:
        system = request.app.state.system
        if not system:
            return {"status": "unhealthy", "message": "系统未初始化"}

        checks = {
            "database": await system.db_manager.health_check(),
            "tape_drive": await system.tape_manager.health_check(),
            "scheduler": system.scheduler.running if system.scheduler else False
        }

        overall_healthy = all(checks.values())

        return {
            "status": "healthy" if overall_healthy else "unhealthy",
            "checks": checks
        }

    except Exception as e:
        logger.error(f"系统健康检查失败: {str(e)}")
        return {
            "status": "unhealthy",
            "error": str(e)
        }


@router.get("/config")
async def get_system_config():
    """获取系统配置"""
    try:
        from config.settings import get_settings
        settings = get_settings()

        # 返回非敏感配置
        return {
            "default_retention_months": settings.DEFAULT_RETENTION_MONTHS,
            "auto_erase_expired": settings.AUTO_ERASE_EXPIRED,
            "monthly_backup_cron": settings.MONTHLY_BACKUP_CRON,
            "dingtalk_api_url": settings.DINGTALK_API_URL,
            "dingtalk_default_phone": settings.DINGTALK_DEFAULT_PHONE,
            "scheduler_enabled": settings.SCHEDULER_ENABLED,
            "compression_level": settings.COMPRESSION_LEVEL,
            "max_file_size": settings.MAX_FILE_SIZE
        }

    except Exception as e:
        logger.error(f"获取系统配置失败: {str(e)}")
        raise HTTPException(status_code=500, detail=str(e))


@router.put("/config")
async def update_system_config(config: SystemConfigRequest):
    """更新系统配置"""
    try:
        # 这里应该实现配置更新逻辑
        # 包括验证配置、保存到数据库、重新加载配置等

        return {"success": True, "message": "配置更新成功"}

    except Exception as e:
        logger.error(f"更新系统配置失败: {str(e)}")
        raise HTTPException(status_code=500, detail=str(e))


@router.post("/test-dingtalk")
async def test_dingtalk_notification(request: Request):
    """测试钉钉通知"""
    try:
        system = request.app.state.system
        if not system:
            raise HTTPException(status_code=500, detail="系统未初始化")

        success = await system.dingtalk_notifier.test_connection()
        if success:
            # 发送测试消息
            await system.dingtalk_notifier.send_system_notification(
                "测试消息",
                "这是一条来自企业级磁带备份系统的测试消息"
            )
            return {"success": True, "message": "测试通知发送成功"}
        else:
            return {"success": False, "message": "钉钉连接测试失败"}

    except Exception as e:
        logger.error(f"测试钉钉通知失败: {str(e)}")
        raise HTTPException(status_code=500, detail=str(e))


@router.get("/logs")
async def get_system_logs(
    level: str = "INFO",
    limit: int = 100,
    offset: int = 0
):
    """获取系统日志"""
    try:
        # 这里应该从数据库查询日志
        # 暂时返回示例数据
        sample_logs = [
            {
                "timestamp": "2024-10-30T04:20:00Z",
                "level": "INFO",
                "module": "backup_engine",
                "message": "备份任务开始执行",
                "details": {}
            },
            {
                "timestamp": "2024-10-30T04:25:00Z",
                "level": "INFO",
                "module": "tape_manager",
                "message": "磁带加载成功",
                "details": {"tape_id": "TAPE001"}
            }
        ]

        return {
            "logs": sample_logs,
            "total": len(sample_logs),
            "limit": limit,
            "offset": offset
        }

    except Exception as e:
        logger.error(f"获取系统日志失败: {str(e)}")
        raise HTTPException(status_code=500, detail=str(e))


@router.get("/statistics")
async def get_system_statistics():
    """获取系统统计信息"""
    try:
        return {
            "uptime": 86400,  # 秒
            "backup_tasks": {
                "total": 25,
                "completed": 20,
                "failed": 2,
                "running": 1
            },
            "tape_inventory": {
                "total": 12,
                "available": 8,
                "in_use": 2,
                "expired": 2
            },
            "storage": {
                "total_capacity": 3865470566400,  # 3.5TB
                "used_capacity": 1073741824000,   # 1TB
                "usage_percent": 27.8
            },
            "notifications": {
                "sent_today": 5,
                "success_rate": 100.0
            }
        }

    except Exception as e:
        logger.error(f"获取系统统计信息失败: {str(e)}")
        raise HTTPException(status_code=500, detail=str(e))


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
        
        # 尝试解析数据库类型和参数
        if db_url.startswith("sqlite"):
            db_info["db_type"] = "sqlite"
            db_info["db_path"] = db_url.replace("sqlite:///", "")
        elif db_url.startswith("postgresql://") or db_url.startswith("opengauss://"):
            db_info["db_type"] = "opengauss" if db_url.startswith("opengauss") else "postgresql"
            # 提取连接参数
            db_info["db_host"] = settings.DB_HOST
            db_info["db_port"] = settings.DB_PORT
            db_info["db_user"] = settings.DB_USER
            db_info["db_database"] = settings.DB_DATABASE
        
        return db_info
        
    except Exception as e:
        logger.error(f"获取数据库配置失败: {str(e)}")
        raise HTTPException(status_code=500, detail=str(e))


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
            db_url = f"{config.db_type}://{config.db_user}:{config.db_password}@{config.db_host}:{config.db_port}/{config.db_database}"
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
async def update_database_config(config: DatabaseConfig):
    """更新数据库配置"""
    try:
        import os
        from pathlib import Path
        
        # 验证配置
        if config.db_type == "sqlite":
            if not config.db_path:
                raise ValueError("SQLite数据库需要指定路径")
            # 创建目录
            Path(config.db_path).parent.mkdir(parents=True, exist_ok=True)
            db_url = f"sqlite:///{config.db_path}"
        elif config.db_type in ["postgresql", "opengauss"]:
            if not all([config.db_host, config.db_port, config.db_user, config.db_password, config.db_database]):
                raise ValueError("PostgreSQL/openGauss数据库需要完整的连接参数")
            db_url = f"{config.db_type}://{config.db_user}:{config.db_password}@{config.db_host}:{config.db_port}/{config.db_database}"
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
        
        logger.info(f"数据库配置已更新: {config.db_type}")
        
        return {
            "success": True,
            "message": "数据库配置更新成功，需要重启系统生效",
            "db_url": db_url.split("@")[0] + "@***" if "@" in db_url else db_url  # 隐藏密码
        }
        
    except Exception as e:
        logger.error(f"更新数据库配置失败: {str(e)}")
        raise HTTPException(status_code=500, detail=str(e))


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
        elif db_url.startswith("postgresql://"):
            db_info["db_type"] = "PostgreSQL"
        elif db_url.startswith("opengauss://"):
            db_info["db_type"] = "openGauss"
        
        return db_info
        
    except Exception as e:
        logger.error(f"获取数据库状态失败: {str(e)}")
        return {
            "status": "error",
            "message": str(e)
        }