#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
SQLite 操作队列管理器 - 统一处理所有 SQLite 写操作，避免并发问题
SQLite Queue Manager - Unified handling of all SQLite write operations to avoid concurrency issues
"""

import asyncio
import logging
from typing import Callable, Any, Optional, Dict
from enum import Enum
from utils.scheduler.sqlite_utils import get_sqlite_connection

logger = logging.getLogger(__name__)


class OperationPriority(Enum):
    """操作优先级"""
    HIGH = 1      # 高优先级（写操作）
    NORMAL = 2    # 普通优先级（同步操作）


class SQLiteQueueManager:
    """SQLite 操作队列管理器 - 单例模式"""
    
    _instance = None
    _lock = asyncio.Lock()
    
    def __new__(cls):
        if cls._instance is None:
            cls._instance = super().__new__(cls)
        return cls._instance
    
    def __init__(self):
        if hasattr(self, '_initialized'):
            return
        
        self._initialized = True
        self._write_queue = asyncio.Queue()  # 高优先级队列（写操作）
        self._sync_queue = asyncio.Queue()  # 普通优先级队列（同步操作）
        self._worker_task = None
        self._is_running = False
        self._stats = {
            'write_operations': 0,
            'sync_operations': 0,
            'write_errors': 0,
            'sync_errors': 0
        }
    
    async def start(self):
        """启动队列管理器"""
        if self._is_running:
            return
        
        self._is_running = True
        self._worker_task = asyncio.create_task(self._worker_loop())
        logger.info("SQLite 操作队列管理器已启动")
    
    async def stop(self):
        """停止队列管理器"""
        if not self._is_running:
            return
        
        self._is_running = False
        
        # 等待队列中的操作完成
        while not self._write_queue.empty() or not self._sync_queue.empty():
            await asyncio.sleep(0.1)
        
        if self._worker_task:
            self._worker_task.cancel()
            try:
                await self._worker_task
            except asyncio.CancelledError:
                pass
        
        logger.info("SQLite 操作队列管理器已停止")
    
    async def execute_write(self, operation: Callable, *args, **kwargs) -> Any:
        """
        执行写操作（高优先级）
        
        Args:
            operation: 要执行的操作函数（async）
            *args, **kwargs: 操作函数的参数
        
        Returns:
            操作函数的返回值
        """
        future = asyncio.Future()
        await self._write_queue.put((operation, args, kwargs, future))
        return await future
    
    async def execute_sync(self, operation: Callable, *args, **kwargs) -> Any:
        """
        执行同步操作（普通优先级）
        
        Args:
            operation: 要执行的操作函数（async）
            *args, **kwargs: 操作函数的参数
        
        Returns:
            操作函数的返回值
        """
        future = asyncio.Future()
        await self._sync_queue.put((operation, args, kwargs, future))
        return await future
    
    async def _worker_loop(self):
        """工作循环 - 优先处理写操作，但给同步操作留出机会"""
        logger.info("SQLite 操作队列工作循环已启动")
        
        write_operations_processed = 0  # 连续处理的写操作数
        
        while self._is_running:
            try:
                # 优先处理写操作队列，但每处理 10 个写操作后，检查一次同步队列
                if not self._write_queue.empty() and write_operations_processed < 10:
                    operation, args, kwargs, future = await asyncio.wait_for(
                        self._write_queue.get(), timeout=0.1
                    )
                    await self._execute_operation(operation, args, kwargs, future, is_write=True)
                    write_operations_processed += 1
                    continue
                
                # 如果写操作队列为空，或者已处理 10 个写操作，处理同步操作队列
                if not self._sync_queue.empty():
                    operation, args, kwargs, future = await asyncio.wait_for(
                        self._sync_queue.get(), timeout=0.1
                    )
                    await self._execute_operation(operation, args, kwargs, future, is_write=False)
                    write_operations_processed = 0  # 重置计数器
                    continue
                
                # 如果同步队列也为空，重置计数器并继续处理写操作
                if self._write_queue.empty():
                    write_operations_processed = 0
                    await asyncio.sleep(0.01)
                else:
                    # 写操作队列不为空，但已处理 10 个，重置计数器继续处理
                    write_operations_processed = 0
                    continue
                
            except asyncio.TimeoutError:
                write_operations_processed = 0  # 重置计数器
                continue
            except asyncio.CancelledError:
                logger.info("SQLite 操作队列工作循环被取消")
                break
            except Exception as e:
                logger.error(f"SQLite 操作队列工作循环异常: {e}", exc_info=True)
                write_operations_processed = 0  # 重置计数器
                await asyncio.sleep(0.1)
    
    async def _execute_operation(self, operation: Callable, args: tuple, kwargs: dict, 
                                 future: asyncio.Future, is_write: bool):
        """执行操作"""
        try:
            # 检查操作函数是否需要 conn 参数
            import inspect
            sig = inspect.signature(operation)
            params = list(sig.parameters.keys())
            needs_conn = len(params) > 0 and params[0] == 'conn'
            
            if needs_conn:
                # 使用 SQLite 连接执行操作（写操作使用队列连接，确保串行执行）
                async with get_sqlite_connection() as conn:
                    result = await operation(conn, *args, **kwargs)
                    future.set_result(result)
            else:
                # 操作函数不需要 conn 参数，直接执行
                # 注意：这些操作（如 insert_backup_files_sqlite）使用 SQLAlchemy，
                # SQLAlchemy 会管理自己的连接，但可能仍然会被队列中的其他操作阻塞
                result = await operation(*args, **kwargs)
                future.set_result(result)
            
            if is_write:
                self._stats['write_operations'] += 1
            else:
                self._stats['sync_operations'] += 1
                    
        except Exception as e:
            logger.error(f"SQLite 操作执行失败: {e}", exc_info=True)
            future.set_exception(e)
            
            if is_write:
                self._stats['write_errors'] += 1
            else:
                self._stats['sync_errors'] += 1
    
    def get_stats(self) -> Dict[str, int]:
        """获取统计信息"""
        return self._stats.copy()
    
    def get_queue_size(self) -> Dict[str, int]:
        """获取队列大小"""
        return {
            'write_queue': self._write_queue.qsize(),
            'sync_queue': self._sync_queue.qsize()
        }


# 全局单例实例
_sqlite_queue_manager: Optional[SQLiteQueueManager] = None


def get_sqlite_queue_manager() -> SQLiteQueueManager:
    """获取 SQLite 操作队列管理器单例"""
    global _sqlite_queue_manager
    if _sqlite_queue_manager is None:
        _sqlite_queue_manager = SQLiteQueueManager()
    return _sqlite_queue_manager


async def execute_sqlite_write(operation: Callable, *args, **kwargs) -> Any:
    """
    执行 SQLite 写操作（高优先级）
    
    Args:
        operation: 要执行的操作函数，第一个参数必须是 conn (SQLite 连接)
        *args, **kwargs: 操作函数的其他参数
    
    Returns:
        操作函数的返回值
    
    Example:
        async def my_write_operation(conn, table_name, data):
            await conn.execute("INSERT INTO ...", ...)
            await conn.commit()
            return True
        
        result = await execute_sqlite_write(my_write_operation, "users", {"name": "test"})
    """
    manager = get_sqlite_queue_manager()
    if not manager._is_running:
        await manager.start()
    return await manager.execute_write(operation, *args, **kwargs)


async def execute_sqlite_sync(operation: Callable, *args, **kwargs) -> Any:
    """
    执行 SQLite 同步操作（普通优先级）
    
    Args:
        operation: 要执行的操作函数，第一个参数必须是 conn (SQLite 连接)
        *args, **kwargs: 操作函数的其他参数
    
    Returns:
        操作函数的返回值
    """
    manager = get_sqlite_queue_manager()
    if not manager._is_running:
        await manager.start()
    return await manager.execute_sync(operation, *args, **kwargs)

