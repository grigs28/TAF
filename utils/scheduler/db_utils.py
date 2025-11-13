#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
数据库工具函数
Database Utility Functions
"""

import asyncio
import logging
from typing import Optional
from contextlib import asynccontextmanager
from config.database import db_manager

logger = logging.getLogger(__name__)

# 全局连接池
_opengauss_pool: Optional[object] = None
_pool_lock = asyncio.Lock()


def is_opengauss() -> bool:
    """检查当前数据库是否为openGauss"""
    database_url = db_manager.settings.DATABASE_URL
    return "opengauss" in database_url.lower()


async def _create_opengauss_pool():
    """创建openGauss连接池"""
    import asyncpg
    import re
    
    database_url = db_manager.settings.DATABASE_URL
    url = database_url.replace("opengauss://", "postgresql://")
    pattern = r'postgresql://([^:]+):([^@]+)@([^:]+):(\d+)/(.+)'
    match = re.match(pattern, url)
    if not match:
        raise ValueError("无法解析openGauss数据库URL")
    
    username, password, host, port, database = match.groups()
    
    # 从配置获取连接池参数
    pool_size = getattr(db_manager.settings, 'DB_POOL_SIZE', 10)
    max_overflow = getattr(db_manager.settings, 'DB_MAX_OVERFLOW', 20)
    pool_timeout = getattr(db_manager.settings, 'DB_POOL_TIMEOUT', 30.0)
    command_timeout = getattr(db_manager.settings, 'DB_COMMAND_TIMEOUT', 60.0)
    min_size = max(1, pool_size // 2)  # 最小连接数
    max_size = pool_size + max_overflow  # 最大连接数
    
    try:
        pool = await asyncpg.create_pool(
            host=host,
            port=int(port),
            user=username,
            password=password,
            database=database,
            min_size=min_size,
            max_size=max_size,
            timeout=pool_timeout,  # 连接超时时间（秒）
            command_timeout=command_timeout,  # 命令超时时间（秒）
            max_queries=50000,  # 每个连接的最大查询数
            max_inactive_connection_lifetime=300.0,  # 非活跃连接的最大生命周期（秒）
        )
        logger.info(f"openGauss连接池创建成功: min_size={min_size}, max_size={max_size}, timeout={pool_timeout}s, command_timeout={command_timeout}s")
        return pool
    except Exception as e:
        logger.error(f"创建openGauss连接池失败: {str(e)}", exc_info=True)
        raise


async def get_opengauss_pool():
    """获取openGauss连接池（单例模式）"""
    global _opengauss_pool
    
    if _opengauss_pool is None:
        async with _pool_lock:
            # 双重检查
            if _opengauss_pool is None:
                _opengauss_pool = await _create_opengauss_pool()
    
    # 检查连接池是否已关闭
    if _opengauss_pool.is_closing():
        async with _pool_lock:
            if _opengauss_pool.is_closing():
                _opengauss_pool = await _create_opengauss_pool()
    
    return _opengauss_pool


class ConnectionWrapper:
    """连接包装器，用于自动管理连接池连接"""
    def __init__(self, pool, conn):
        self.pool = pool
        self.conn = conn
        self._released = False
    
    def __getattr__(self, name):
        """代理所有属性访问到实际连接"""
        return getattr(self.conn, name)
    
    async def release(self):
        """释放连接回连接池"""
        if not self._released and self.conn:
            try:
                await self.pool.release(self.conn)
                self._released = True
            except Exception as e:
                logger.warning(f"释放连接失败: {str(e)}")
    
    async def __aenter__(self):
        """异步上下文管理器入口"""
        return self.conn
    
    async def __aexit__(self, exc_type, exc_val, exc_tb):
        """异步上下文管理器出口，自动释放连接"""
        await self.release()


async def _acquire_connection():
    """从连接池获取连接（内部函数）"""
    import asyncpg
    
    pool = await get_opengauss_pool()
    conn = None
    retry_count = 0
    max_retries = 3
    
    # 从配置获取获取连接的超时时间
    acquire_timeout = getattr(db_manager.settings, 'DB_ACQUIRE_TIMEOUT', 10.0)
    
    while retry_count < max_retries:
        try:
            # 从连接池获取连接
            conn = await pool.acquire(timeout=acquire_timeout)  # 获取连接的超时时间
            return pool, conn
        except asyncio.TimeoutError:
            retry_count += 1
            if retry_count >= max_retries:
                logger.error(f"获取数据库连接超时，已重试{retry_count}次")
                raise
            logger.warning(f"获取数据库连接超时，重试 {retry_count}/{max_retries}")
            await asyncio.sleep(0.5 * retry_count)  # 指数退避
        except asyncpg.exceptions.ConnectionDoesNotExistError:
            # 连接不存在，可能需要重新创建连接池
            retry_count += 1
            if retry_count >= max_retries:
                logger.error(f"数据库连接不存在，已重试{retry_count}次")
                raise
            logger.warning(f"数据库连接不存在，重新创建连接池，重试 {retry_count}/{max_retries}")
            global _opengauss_pool
            async with _pool_lock:
                try:
                    if _opengauss_pool and not _opengauss_pool.is_closing():
                        await _opengauss_pool.close()
                except Exception:
                    pass
                _opengauss_pool = None
            await asyncio.sleep(1.0 * retry_count)
        except Exception as e:
            logger.error(f"获取数据库连接失败: {str(e)}", exc_info=True)
            raise


@asynccontextmanager
async def get_opengauss_connection():
    """
    获取openGauss数据库连接（使用连接池，自动管理）
    
    用法：
    async with get_opengauss_connection() as conn:
        # 使用 conn
        rows = await conn.fetch("SELECT * FROM backup_tasks")
        # 连接自动释放回连接池
    """
    pool, conn = await _acquire_connection()
    try:
        yield conn
    finally:
        try:
            await pool.release(conn)
        except Exception as e:
            # openGauss 不支持 UNLISTEN 语句，这是 asyncpg 在释放连接时尝试执行的
            # 可以安全忽略这个错误，不影响连接释放
            error_msg = str(e)
            if "UNLISTEN" in error_msg or "not yet supported" in error_msg:
                # 这是 openGauss 的限制，不是真正的错误，使用 DEBUG 级别记录
                logger.debug(f"释放连接时遇到 openGauss 限制（可忽略）: {error_msg}")
            else:
                # 其他错误使用 WARNING 级别
                logger.warning(f"释放连接失败: {error_msg}")


async def close_opengauss_pool():
    """关闭openGauss连接池"""
    global _opengauss_pool
    if _opengauss_pool and not _opengauss_pool.is_closing():
        try:
            await _opengauss_pool.close()
            logger.info("openGauss连接池已关闭")
        except Exception as e:
            logger.error(f"关闭openGauss连接池失败: {str(e)}", exc_info=True)
        finally:
            _opengauss_pool = None

