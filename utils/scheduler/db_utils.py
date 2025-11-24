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
from utils.opengauss.guard import get_opengauss_monitor

logger = logging.getLogger(__name__)

# 全局连接池
_opengauss_pool: Optional[object] = None
_pool_lock = asyncio.Lock()


def is_opengauss() -> bool:
    """检查当前数据库是否为openGauss"""
    if hasattr(db_manager, "is_opengauss_database"):
        try:
            return db_manager.is_opengauss_database()
        except Exception:
            pass
    database_url = db_manager.settings.DATABASE_URL
    return "opengauss" in str(database_url).lower()


def is_redis() -> bool:
    """检查当前数据库是否为Redis"""
    database_url = db_manager.settings.DATABASE_URL
    db_flavor = getattr(db_manager.settings, 'DB_FLAVOR', None)
    # 优先使用DB_FLAVOR配置
    if db_flavor and db_flavor.lower() == "redis":
        return True
    # 从DATABASE_URL判断
    url_lower = str(database_url).lower()
    return url_lower.startswith("redis://") or url_lower.startswith("rediss://")


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
    max_inactive_lifetime = getattr(db_manager.settings, 'DB_MAX_INACTIVE_CONNECTION_LIFETIME', 600.0)
    query_dop = getattr(db_manager.settings, 'DB_QUERY_DOP', 16)  # openGauss 查询并行度
    min_size = max(1, pool_size // 2)  # 最小连接数
    max_size = pool_size + max_overflow  # 最大连接数
    
    monitor = get_opengauss_monitor()

    # 连接初始化函数：设置 query_dop
    async def init_connection(conn):
        """连接初始化函数：设置 openGauss 查询并行度"""
        try:
            await conn.execute(f"SET query_dop = {query_dop};")
            logger.debug(f"已设置 openGauss 查询并行度: query_dop = {query_dop}")
        except Exception as e:
            logger.warning(f"设置 query_dop 失败（可能不是 openGauss 数据库）: {str(e)}")
            # 不影响连接创建，继续执行

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
            max_inactive_connection_lifetime=max_inactive_lifetime,  # 非活跃连接的最大生命周期（秒，可配置）
            init=init_connection,  # 连接初始化回调：设置 query_dop
        )
        logger.info(
            f"openGauss连接池创建成功: min_size={min_size}, max_size={max_size}, "
            f"timeout={pool_timeout}s, command_timeout={command_timeout}s, "
            f"max_inactive_lifetime={max_inactive_lifetime}s, query_dop={query_dop}"
        )
        if monitor.enabled:
            logger.debug(
                "openGauss 连接池参数: host=%s port=%s db=%s min=%s max=%s",
                host,
                port,
                database,
                min_size,
                max_size,
            )
            monitor.ensure_running()
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
    global _opengauss_pool
    
    pool = await get_opengauss_pool()
    monitor = get_opengauss_monitor()
    conn = None
    retry_count = 0
    max_retries = 3
    
    # 从配置获取获取连接的超时时间
    acquire_timeout = getattr(db_manager.settings, 'DB_ACQUIRE_TIMEOUT', 10.0)
    
    while retry_count < max_retries:
        try:
            # 从连接池获取连接
            acquire_coro = pool.acquire(timeout=acquire_timeout)
            if monitor.enabled:
                monitor.ensure_running()
                conn = await monitor.watch(
                    acquire_coro,
                    operation="pool.acquire",
                    timeout=acquire_timeout + 1,
                    metadata={"retry": retry_count},
                    critical=True,
                )
            else:
                conn = await acquire_coro
            return pool, conn
        except asyncio.TimeoutError:
            retry_count += 1
            if retry_count >= max_retries:
                logger.error(f"获取数据库连接超时，已重试{retry_count}次")
                raise
            logger.warning(f"获取数据库连接超时，重试 {retry_count}/{max_retries}")
            await asyncio.sleep(0.5 * retry_count)  # 指数退避
        except (ConnectionError, OSError) as e:
            # 连接丢失或网络错误，重置连接池并重试
            retry_count += 1
            error_msg = str(e)
            if retry_count >= max_retries:
                logger.error(f"数据库连接丢失，已重试{retry_count}次: {error_msg}")
                raise
            logger.warning(f"数据库连接丢失，重置连接池并重试 {retry_count}/{max_retries}: {error_msg}")
            async with _pool_lock:
                try:
                    if _opengauss_pool and not _opengauss_pool.is_closing():
                        await _opengauss_pool.close()
                except Exception:
                    pass
                _opengauss_pool = None
            await asyncio.sleep(1.0 * retry_count)  # 等待后重试
        except asyncpg.exceptions.ConnectionDoesNotExistError:
            # 连接不存在，可能需要重新创建连接池
            retry_count += 1
            if retry_count >= max_retries:
                logger.error(f"数据库连接不存在，已重试{retry_count}次")
                raise
            logger.warning(f"数据库连接不存在，重新创建连接池，重试 {retry_count}/{max_retries}")
            async with _pool_lock:
                try:
                    if _opengauss_pool and not _opengauss_pool.is_closing():
                        await _opengauss_pool.close()
                except Exception:
                    pass
                _opengauss_pool = None
            await asyncio.sleep(1.0 * retry_count)
        except Exception as e:
            # 其他异常，记录并重新抛出
            error_msg = str(e)
            # 检查是否是 connection_lost 相关错误
            if "connection_lost" in error_msg.lower() or "unexpected connection" in error_msg.lower():
                retry_count += 1
                if retry_count >= max_retries:
                    logger.error(f"数据库连接异常，已重试{retry_count}次: {error_msg}")
                    raise
                logger.warning(f"数据库连接异常，重置连接池并重试 {retry_count}/{max_retries}: {error_msg}")
                async with _pool_lock:
                    try:
                        if _opengauss_pool and not _opengauss_pool.is_closing():
                            await _opengauss_pool.close()
                    except Exception:
                        pass
                    _opengauss_pool = None
                await asyncio.sleep(1.0 * retry_count)
            else:
                logger.error(f"获取数据库连接失败: {error_msg}", exc_info=True)
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
            monitor = get_opengauss_monitor()
            release_coro = pool.release(conn)
            if monitor.enabled:
                try:
                    await monitor.watch(
                        release_coro,
                        operation="pool.release",
                        timeout=5.0,
                    )
                except Exception as watch_error:
                    # 监控可能会捕获异常，但我们需要检查是否是 UNLISTEN 错误
                    error_msg = str(watch_error)
                    if "UNLISTEN" not in error_msg and "not yet supported" not in error_msg:
                        raise
                    # UNLISTEN 错误可以忽略
                    logger.debug(f"释放连接时遇到 openGauss 限制（可忽略）: {error_msg}")
            else:
                await release_coro
        except Exception as e:
            # openGauss 不支持 UNLISTEN 语句，这是 asyncpg 在释放连接时尝试执行的
            # 可以安全忽略这个错误，不影响连接释放
            error_msg = str(e)
            import asyncpg
            if isinstance(e, asyncpg.exceptions.FeatureNotSupportedError) or "UNLISTEN" in error_msg or "not yet supported" in error_msg:
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

