#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
系统配置管理模块
System Configuration Management Module
"""

import os
import re
from pathlib import Path
from typing import Optional
from pydantic import Field, field_validator

try:
    from pydantic_settings import BaseSettings
except ImportError:
    # 如果 pydantic-settings 没有安装，提供错误信息
    raise ImportError(
        "需要安装 pydantic-settings 包。请运行: pip install pydantic-settings\n"
        "Pydantic v2 将 BaseSettings 移动到了单独的包中。"
    )


def _read_version_from_changelog() -> str:
    """从CHANGELOG.md读取最新版本号"""
    try:
        changelog_path = Path("CHANGELOG.md")
        if changelog_path.exists():
            with open(changelog_path, "r", encoding="utf-8") as f:
                content = f.read()
                # 匹配 ## [版本号] 格式
                match = re.search(r'^##\s+\[([\d.]+)\]', content, re.MULTILINE)
                if match:
                    return match.group(1)
    except Exception:
        pass
    # 默认版本
    return "0.0.2"


class Settings(BaseSettings):
    """系统配置类"""

    # 应用配置
    APP_NAME: str = "企业级磁带备份系统"
    APP_VERSION: str = Field(default_factory=_read_version_from_changelog, description="版本号从CHANGELOG.md自动读取")
    DEBUG: bool = False
    WEB_PORT: int = 8080

    # 数据库配置
    DATABASE_URL: str = "opengauss://username:password@localhost:5432/backup_db"
    DB_POOL_SIZE: int = 10
    DB_MAX_OVERFLOW: int = 20

    # 数据库配置（兼容格式）
    DB_HOST: Optional[str] = "localhost"
    DB_PORT: Optional[int] = 5432
    DB_USER: Optional[str] = "username"
    DB_PASSWORD: Optional[str] = "password"
    DB_DATABASE: Optional[str] = "backup_db"

    @field_validator('DB_PORT', mode='before')
    @classmethod
    def validate_db_port(cls, v):
        """验证DB_PORT，将空字符串转换为None"""
        if v == '':
            return None
        return v

    # 安全配置
    SECRET_KEY: str = "your-jwt-secret-key-change-in-production"
    ALGORITHM: str = "HS256"
    ACCESS_TOKEN_EXPIRE_MINUTES: int = 30

    # 磁带配置
    TAPE_DRIVE_LETTER: str = "O"  # Windows盘符（大写，不带冒号，LTFS命令使用）
    DEFAULT_BLOCK_SIZE: int = 262144  # 256KB
    MAX_VOLUME_SIZE: int = 322122547200  # 300GB
    # ITDT 接口配置
    TAPE_INTERFACE_TYPE: str = "itdt"  # 仅使用 ITDT
    ITDT_PATH: str = "C:\\itdt\\itdt.exe" if os.name == "nt" else "/usr/local/itdt/itdt"
    ITDT_LOG_LEVEL: str = "Information"  # Errors|Warnings|Information|Debug
    ITDT_LOG_PATH: str = "output"
    
    # LTFS 工具目录配置（必须在LTFS程序目录下执行命令）
    LTFS_TOOLS_DIR: str = "D:\\APP\\TAF\\ITDT" if os.name == "nt" else "/usr/local/ltfs"
    ITDT_RESULT_PATH: str = "output"
    ITDT_DEVICE_PATH: str | None = None
    ITDT_FORCE_GENERIC_DD: bool = True  # 允许在无专用驱动时强制使用通用驱动
    ITDT_SCAN_SHOW_ALL_PATHS: bool = True  # 扫描时显示所有路径

    # 压缩配置
    COMPRESSION_LEVEL: int = 9
    SOLID_BLOCK_SIZE: int = 67108864  # 64MB
    MAX_FILE_SIZE: int = 3221225472  # 3GB

    # 计划任务配置
    SCHEDULER_ENABLED: bool = True
    MONTHLY_BACKUP_CRON: str = "0 2 1 * *"  # 每月1号02:00
    RETENTION_CHECK_CRON: str = "0 3 * * *"  # 每天03:00

    # 系统配置
    DEFAULT_RETENTION_MONTHS: int = 6
    AUTO_ERASE_EXPIRED: bool = True

    # 日志配置
    LOG_LEVEL: str = "INFO"
    LOG_FILE: str = "logs/application.log"

    # 钉钉通知配置
    DINGTALK_API_URL: str = "http://localhost:5555"
    DINGTALK_API_KEY: str = "your-dingtalk-api-key"
    DINGTALK_DEFAULT_PHONE: str = "13800000000"

    # 备份配置
    BACKUP_TEMP_DIR: str = "temp/backup"
    RECOVERY_TEMP_DIR: str = "temp/recovery"
    COMPRESSION_THREADS: int = 4
    SCAN_BATCH_SIZE: int = 1000  # 扫描批次大小：扫描到多少文件后开始压缩（默认1000个文件）
    SCAN_BATCH_SIZE_BYTES: int = 1073741824  # 扫描批次大小（字节）：扫描到多少字节后开始压缩（默认1GB）

    # 磁带管理配置
    TAPE_POOL_SIZE: int = 12  # 磁带池大小
    TAPE_CHECK_INTERVAL: int = 3600  # 磁带状态检查间隔（秒）
    AUTO_TAPE_CLEANUP: bool = True

    # Web界面配置
    WEB_STATIC_DIR: str = "web/static"
    WEB_TEMPLATE_DIR: str = "web/templates"
    MAX_UPLOAD_SIZE: int = 1073741824  # 1GB

    # 监控配置
    METRICS_ENABLED: bool = True
    HEALTH_CHECK_INTERVAL: int = 300  # 5分钟

    # 高级配置
    ENVIRONMENT: str = "production"
    WEB_HOST: str = "0.0.0.0"
    WEB_WORKERS: int = 4
    ENABLE_CORS: bool = True
    CORS_ORIGINS: str = "*"
    ENABLE_GZIP: bool = True
    TAPE_DEVICE_PATH: str = "/dev/nst0"
    LOG_BACKUP_COUNT: int = 30
    ASYNC_POOL_SIZE: int = 20
    ASYNC_MAX_OVERFLOW: int = 40
    ENABLE_QUERY_CACHE: bool = True
    QUERY_CACHE_TTL: int = 300
    WEBSOCKET_HEARTBEAT: int = 30
    SESSION_TIMEOUT: int = 3600
    
    # 数据目录配置
    DATA_DIR: str = "data"
    SQLITE_DB_FILE: str = "data/taf_backup.db"

    class Config:
        env_file = ".env"
        env_file_encoding = "utf-8"
        case_sensitive = True
        extra = "allow"  # 允许额外的字段


# 全局配置实例
settings = Settings()


def get_settings() -> Settings:
    """获取配置实例"""
    return settings


def reload_settings() -> Settings:
    """重新加载配置"""
    global settings
    settings = Settings()
    return settings