#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
psycopg3 兼容层
将 psycopg3 的接口包装成类似 asyncpg 的接口，以便现有代码可以无缝切换
"""

import logging
import re
from typing import List, Dict, Any, Optional, Tuple
from psycopg import AsyncConnection
from psycopg.rows import dict_row

logger = logging.getLogger(__name__)


def convert_asyncpg_to_psycopg3_query(query: str) -> str:
    """将 asyncpg 风格的占位符 ($1, $2, ...) 转换为 psycopg3 风格 (%s)"""
    # 匹配 $1, $2, $3 等占位符
    pattern = r'\$(\d+)'
    
    def replace_placeholder(match):
        # 提取占位符编号
        placeholder_num = int(match.group(1))
        # 转换为 %s（psycopg3 使用位置参数，按顺序替换）
        return '%s'
    
    # 替换所有 $1, $2, $3 为 %s
    converted_query = re.sub(pattern, replace_placeholder, query)
    return converted_query


class AsyncPGCompatConnection:
    """将 psycopg3 AsyncConnection 包装成类似 asyncpg 的接口"""
    
    def __init__(self, conn: AsyncConnection, pool=None):
        self._conn = conn
        self._pool = pool  # 保存连接池引用，用于释放连接
        self._released = False
    
    async def execute(self, query: str, *args):
        """执行 SQL 语句（类似 asyncpg.execute）"""
        # 转换占位符：$1, $2 -> %s
        converted_query = convert_asyncpg_to_psycopg3_query(query)
        # 如果连接处于错误状态，先回滚
        if self._conn.info.transaction_status == 3:  # INERROR = 3
            try:
                await self._conn.rollback()
                logger.debug("execute: 连接处于错误状态，已回滚")
            except Exception as rollback_error:
                logger.debug(f"execute: 回滚失败（可能已自动回滚）: {str(rollback_error)}")
        
        # 直接执行 SQL，不创建事务上下文管理器（让调用者管理事务，或使用 autocommit）
        async with self._conn.cursor() as cur:
            await cur.execute(converted_query, args)
            # psycopg3 默认 autocommit=False，如果不在事务中（IDLE），需要显式提交
            # 如果已经在事务中（INTRANS），让调用者管理事务
            transaction_status = self._conn.info.transaction_status
            if transaction_status == 0:  # IDLE = 0 (不在事务中，需要提交)
                try:
                    await self._conn.commit()
                except Exception as commit_error:
                    # 如果提交失败（可能已经自动提交），记录但不抛出异常
                    logger.debug(f"execute: 提交失败（可能已自动提交）: {str(commit_error)}")
            # transaction_status == 1 (INTRANS) 表示在事务中，不提交，让调用者管理
            return cur.rowcount
    
    async def fetch(self, query: str, *args) -> List[Dict[str, Any]]:
        """执行查询并返回所有行（类似 asyncpg.fetch）"""
        # 转换占位符：$1, $2 -> %s
        converted_query = convert_asyncpg_to_psycopg3_query(query)
        # 如果连接处于错误状态，先回滚
        if self._conn.info.transaction_status == 3:  # INERROR = 3
            try:
                await self._conn.rollback()
                logger.debug("fetch: 连接处于错误状态，已回滚")
            except Exception as rollback_error:
                logger.debug(f"fetch: 回滚失败（可能已自动回滚）: {str(rollback_error)}")
        
        # 查询操作不需要事务，直接执行
        async with self._conn.cursor(row_factory=dict_row) as cur:
            await cur.execute(converted_query, args)
            rows = await cur.fetchall()
        
        # 查询完成后，如果连接处于 INTRANS 状态，提交事务（只读查询应该自动提交）
        transaction_status = self._conn.info.transaction_status
        if transaction_status == 1:  # INTRANS: 在事务中
            try:
                await self._conn.commit()
                logger.debug("fetch: 只读查询后已提交事务")
            except Exception as commit_error:
                logger.debug(f"fetch: 提交失败（可能已自动提交）: {str(commit_error)}")
        
        # psycopg3 的 dict_row 已经返回字典，直接返回
        return list(rows) if rows else []
    
    async def fetchrow(self, query: str, *args) -> Optional[Dict[str, Any]]:
        """执行查询并返回第一行（类似 asyncpg.fetchrow）"""
        # 转换占位符：$1, $2 -> %s
        converted_query = convert_asyncpg_to_psycopg3_query(query)
        # 如果连接处于错误状态，先回滚
        if self._conn.info.transaction_status == 3:  # INERROR = 3
            try:
                await self._conn.rollback()
                logger.debug("fetchrow: 连接处于错误状态，已回滚")
            except Exception as rollback_error:
                logger.debug(f"fetchrow: 回滚失败（可能已自动回滚）: {str(rollback_error)}")
        
        # 查询操作不需要事务，直接执行
        async with self._conn.cursor(row_factory=dict_row) as cur:
            await cur.execute(converted_query, args)
            row = await cur.fetchone()
        
        # 查询完成后，如果连接处于 INTRANS 状态，提交事务（只读查询应该自动提交）
        transaction_status = self._conn.info.transaction_status
        if transaction_status == 1:  # INTRANS: 在事务中
            try:
                await self._conn.commit()
                logger.debug("fetchrow: 只读查询后已提交事务")
            except Exception as commit_error:
                logger.debug(f"fetchrow: 提交失败（可能已自动提交）: {str(commit_error)}")
        
        return dict(row) if row else None
    
    async def fetchval(self, query: str, *args, column: int = 0) -> Any:
        """执行查询并返回第一行的指定列（类似 asyncpg.fetchval）"""
        # 转换占位符：$1, $2 -> %s
        converted_query = convert_asyncpg_to_psycopg3_query(query)
        # 如果连接处于错误状态，先回滚
        if self._conn.info.transaction_status == 3:  # INERROR = 3
            try:
                await self._conn.rollback()
                logger.debug("fetchval: 连接处于错误状态，已回滚")
            except Exception as rollback_error:
                logger.debug(f"fetchval: 回滚失败（可能已自动回滚）: {str(rollback_error)}")
        
        # 查询操作不需要事务，直接执行
        async with self._conn.cursor() as cur:
            await cur.execute(converted_query, args)
            row = await cur.fetchone()
        
        # 查询完成后，如果连接处于 INTRANS 状态，提交事务（只读查询应该自动提交）
        transaction_status = self._conn.info.transaction_status
        if transaction_status == 1:  # INTRANS: 在事务中
            try:
                await self._conn.commit()
                logger.debug("fetchval: 只读查询后已提交事务")
            except Exception as commit_error:
                logger.debug(f"fetchval: 提交失败（可能已自动提交）: {str(commit_error)}")
        
        if row:
            return row[column] if isinstance(row, (list, tuple)) else row
        return None
    
    async def executemany(self, query: str, args_list: List[Tuple]):
        """批量执行 SQL 语句（类似 asyncpg.executemany）"""
        import logging
        logger = logging.getLogger(__name__)
        
        # 转换占位符：$1, $2 -> %s
        converted_query = convert_asyncpg_to_psycopg3_query(query)
        logger.debug(f"[psycopg3_compat] executemany 开始，数据量: {len(args_list)} 条")
        
        # 验证参数数量（仅用于调试）
        if args_list:
            first_args = args_list[0]
            # 统计原始 SQL 中的占位符数量
            import re
            placeholder_count = len(re.findall(r'\$(\d+)', query))
            if len(first_args) != placeholder_count:
                logger.warning(
                    f"[psycopg3_compat] ⚠️ 参数数量不匹配: SQL占位符数={placeholder_count}, "
                    f"实际参数数={len(first_args)}, SQL片段: {query[:200]}..."
                )
        
        # psycopg3 需要显式事务，但为了确保数据正确提交，我们手动管理事务
        # 不使用事务上下文管理器，而是手动提交，确保数据持久化
        try:
            import time
            cursor_start = time.time()
            async with self._conn.cursor() as cur:
                await cur.executemany(converted_query, args_list)
                rowcount = cur.rowcount
            cursor_time = time.time() - cursor_start
            logger.debug(f"[psycopg3_compat] executemany cursor 执行完成: 影响行数={rowcount}, 耗时={cursor_time:.2f}秒")
            
            # 检查执行前的事务状态
            status_before = self._conn.info.transaction_status
            logger.debug(f"[psycopg3_compat] executemany 执行前事务状态: {status_before} (0=IDLE, 1=INTRANS, 3=INERROR)")
            
            # 显式提交事务（psycopg3 binary protocol 需要显式提交）
            commit_start = time.time()
            try:
                await self._conn.commit()
                commit_time = time.time() - commit_start
                logger.debug(f"[psycopg3_compat] commit() 调用完成，耗时={commit_time:.3f}秒")
            except Exception as commit_err:
                commit_time = time.time() - commit_start
                logger.error(f"[psycopg3_compat] ❌ commit() 调用失败: {str(commit_err)}, 耗时={commit_time:.3f}秒", exc_info=True)
                raise  # 重新抛出异常
            
            # 验证事务状态，确保已提交（应该是 IDLE = 0）
            # 注意：可能需要等待一小段时间让状态更新
            import asyncio
            await asyncio.sleep(0.001)  # 等待1ms让状态更新
            
            transaction_status = self._conn.info.transaction_status
            if transaction_status == 0:  # IDLE: 不在事务中，已提交
                logger.info(f"[psycopg3_compat] ✅ executemany 事务已提交: {rowcount} 行，commit耗时={commit_time:.3f}秒，事务状态=IDLE")
            elif transaction_status == 1:  # INTRANS: 仍在事务中，提交失败
                logger.error(f"[psycopg3_compat] ❌ executemany commit() 调用后事务状态仍为 INTRANS！这可能导致数据被回滚！")
                # 尝试再次提交
                try:
                    logger.warning(f"[psycopg3_compat] 重试 commit()...")
                    await self._conn.commit()
                    await asyncio.sleep(0.001)  # 再次等待状态更新
                    transaction_status_retry = self._conn.info.transaction_status
                    if transaction_status_retry == 0:
                        logger.info(f"[psycopg3_compat] ✅ 重试 commit() 成功，事务状态=IDLE")
                    else:
                        logger.error(f"[psycopg3_compat] ❌ 重试 commit() 后事务状态仍为 {transaction_status_retry}")
                        # 如果重试后仍然失败，尝试强制刷新连接状态
                        try:
                            # 执行一个简单的查询来刷新连接状态
                            async with self._conn.cursor() as cur:
                                await cur.execute("SELECT 1")
                        except Exception as refresh_err:
                            logger.debug(f"[psycopg3_compat] 刷新连接状态失败: {str(refresh_err)}")
                except Exception as retry_err:
                    logger.error(f"[psycopg3_compat] ❌ 重试 commit() 失败: {str(retry_err)}", exc_info=True)
            else:
                logger.warning(f"[psycopg3_compat] ⚠️ executemany commit() 后事务状态: {transaction_status} (0=IDLE, 1=INTRANS, 3=INERROR)")
            
            return rowcount
        except Exception as e:
            # 如果出错，回滚事务
            logger.error(f"[psycopg3_compat] ❌ executemany 执行失败: {str(e)}", exc_info=True)
            try:
                await self._conn.rollback()
                logger.debug("[psycopg3_compat] 事务已回滚")
            except:
                pass
            raise
    
    def is_closed(self) -> bool:
        """检查连接是否已关闭"""
        return self._conn.closed
    
    async def release(self):
        """释放连接回连接池（psycopg3 使用 putconn）"""
        if not self._released and self._pool and hasattr(self._pool, 'putconn'):
            try:
                await self._pool.putconn(self._conn)
                self._released = True
            except Exception as e:
                logger.warning(f"释放 psycopg3 连接失败: {str(e)}")
    
    def __getattr__(self, name):
        """代理其他属性到实际连接"""
        return getattr(self._conn, name)
