#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
SQLite 数据库工具函数
SQLite Database Utility Functions

提供 SQLite 数据库连接和操作工具，与 openGauss 工具函数接口保持一致
"""

import asyncio
import logging
import aiosqlite
from typing import Optional
from contextlib import asynccontextmanager
from pathlib import Path
from config.database import db_manager

logger = logging.getLogger(__name__)

# 全局连接池（SQLite 使用单个连接，但提供连接池接口）
_sqlite_connection: Optional[aiosqlite.Connection] = None
_connection_lock = asyncio.Lock()


def is_sqlite() -> bool:
    """检查当前数据库是否为 SQLite"""
    if hasattr(db_manager, "settings"):
        database_url = db_manager.settings.DATABASE_URL
        return database_url.startswith("sqlite:///") or database_url.startswith("sqlite+aiosqlite:///")
    return False


def _get_sqlite_path() -> str:
    """从 DATABASE_URL 获取 SQLite 数据库文件路径"""
    database_url = db_manager.settings.DATABASE_URL
    
    # 检查是否为 SQLite URL
    if not database_url.startswith("sqlite:///") and not database_url.startswith("sqlite+aiosqlite:///"):
        from utils.scheduler.db_utils import is_redis, is_opengauss
        
        # 确定当前数据库类型
        if is_redis():
            current_db_type = "Redis"
        elif is_opengauss():
            current_db_type = "openGauss"
        else:
            current_db_type = "未知类型"
        
        error_msg = (
            f"[数据库类型错误] 当前数据库类型是 {current_db_type}，无法获取 SQLite 路径。"
            f"请检查代码逻辑，确保在 {current_db_type} 模式下不使用 SQLite 连接。"
            f"DATABASE_URL: {database_url[:50]}..."
        )
        logger.error(error_msg)
        raise ValueError(error_msg)
    
    # 移除 sqlite:/// 或 sqlite+aiosqlite:/// 前缀
    path = database_url.replace("sqlite+aiosqlite:///", "").replace("sqlite:///", "")
    return path


async def _ensure_sqlite_connection():
    """确保 SQLite 连接已创建"""
    global _sqlite_connection
    
    if _sqlite_connection is None:
        async with _connection_lock:
            # 双重检查
            if _sqlite_connection is None:
                db_path = _get_sqlite_path()
                
                # 确保目录存在
                db_file = Path(db_path)
                db_file.parent.mkdir(parents=True, exist_ok=True)
                
                # 从配置读取超时时间
                timeout = getattr(db_manager.settings, 'SQLITE_TIMEOUT', 30.0)
                
                # 创建连接
                _sqlite_connection = await aiosqlite.connect(
                    db_path,
                    timeout=timeout,  # 连接超时（从配置读取）
                    check_same_thread=False,  # 允许在不同线程中使用
                    isolation_level=None,  # 显式启用 autocommit 模式，减少游标复位问题
                )
                
                # 从配置读取 SQLite 参数
                journal_mode = getattr(db_manager.settings, 'SQLITE_JOURNAL_MODE', 'WAL')
                synchronous = getattr(db_manager.settings, 'SQLITE_SYNCHRONOUS', 'NORMAL')
                cache_size = getattr(db_manager.settings, 'SQLITE_CACHE_SIZE', 10000)
                timeout = getattr(db_manager.settings, 'SQLITE_TIMEOUT', 30.0)
                
                # 启用 WAL 模式提升性能（根据配置）
                await _sqlite_connection.execute(f"PRAGMA journal_mode={journal_mode}")
                await _sqlite_connection.execute(f"PRAGMA synchronous={synchronous}")
                await _sqlite_connection.execute(f"PRAGMA cache_size=-{cache_size}")  # 负数表示 KB
                await _sqlite_connection.execute("PRAGMA temp_store=memory")
                await _sqlite_connection.commit()
                
                logger.info(f"SQLite 连接已创建: {db_path}, journal_mode={journal_mode}, synchronous={synchronous}, cache_size={cache_size}KB, timeout={timeout}s")
    
    return _sqlite_connection


@asynccontextmanager
async def get_sqlite_connection():
    """
    获取 SQLite 数据库连接（使用连接池，自动管理）
    
    用法：
    async with get_sqlite_connection() as conn:
        # 使用 conn
        cursor = await conn.execute("SELECT * FROM backup_tasks")
        rows = await cursor.fetchall()
        # 连接自动释放回连接池
    """
    # 检查当前数据库类型是否为 SQLite
    if not is_sqlite():
        from utils.scheduler.db_utils import is_redis, is_opengauss
        
        # 确定当前数据库类型
        if is_redis():
            current_db_type = "Redis"
            suggestion = "在 Redis 模式下请使用 Redis 相关函数"
        elif is_opengauss():
            current_db_type = "openGauss"
            suggestion = "在 openGauss 模式下请使用 get_opengauss_connection() 函数"
        else:
            current_db_type = "未知类型"
            suggestion = "请检查数据库配置"
        
        error_msg = (
            f"[数据库类型错误] 当前数据库类型是 {current_db_type}，无法使用 SQLite 连接。"
            f"{suggestion}。"
        )
        logger.error(error_msg)
        raise ValueError(error_msg)
    
    conn = await _ensure_sqlite_connection()
    try:
        yield conn
    finally:
        # SQLite 连接保持打开，不需要释放
        pass


async def close_sqlite_connection():
    """关闭 SQLite 连接"""
    global _sqlite_connection
    if _sqlite_connection:
        try:
            await _sqlite_connection.close()
            logger.info("SQLite 连接已关闭")
        except Exception as e:
            logger.error(f"关闭 SQLite 连接失败: {str(e)}", exc_info=True)
        finally:
            _sqlite_connection = None


async def get_database_connection():
    """
    获取数据库连接（自动选择 SQLite 或 openGauss）
    
    这是一个统一的接口，根据当前数据库类型自动选择正确的连接方式
    
    用法：
    async with get_database_connection() as conn:
        if is_sqlite():
            # SQLite 连接
            cursor = await conn.execute("SELECT * FROM backup_tasks")
            rows = await cursor.fetchall()
        else:
            # openGauss 连接（asyncpg）
            rows = await conn.fetch("SELECT * FROM backup_tasks")
    """
    if is_sqlite():
        from utils.scheduler.sqlite_utils import get_sqlite_connection
        async with get_sqlite_connection() as conn:
            yield conn
    else:
        from utils.scheduler.db_utils import get_opengauss_connection
        async with get_opengauss_connection() as conn:
            yield conn

