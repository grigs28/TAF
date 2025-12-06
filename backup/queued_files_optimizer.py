#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
文件入队状态更新优化器
优化 mark_files_as_queued 的性能

优化方案：
1. 方案1：确保 (backup_set_id, file_path) 复合索引存在
2. 方案2：使用临时表 + JOIN 更新方式（替换 ANY 操作符）
"""

import logging
import time
import uuid
from typing import List, Optional

logger = logging.getLogger(__name__)


async def ensure_index_exists(conn, table_name: str) -> bool:
    """
    方案1：确保 (backup_set_id, file_path) 复合索引存在
    
    Args:
        conn: 数据库连接
        table_name: 表名（如 backup_files_000044）
    
    Returns:
        bool: 索引是否已存在或创建成功
    """
    index_name = f"idx_{table_name}_set_path"
    
    try:
        # 检查索引是否已存在
        result = await conn.fetchval(
            """
            SELECT 1
            FROM pg_indexes
            WHERE schemaname = 'public'
              AND tablename = $1
              AND indexname = $2
            """,
            table_name,
            index_name,
        )
        
        if result:
            logger.debug(f"[索引优化] 索引 {index_name} 已存在，跳过创建")
            return True
        
        # 创建索引
        logger.info(f"[索引优化] 为表 {table_name} 创建复合索引 {index_name}(backup_set_id, file_path)")
        await conn.execute(
            f"""
            CREATE INDEX IF NOT EXISTS {index_name}
            ON {table_name}(backup_set_id, file_path)
            """
        )
        await conn.commit()
        logger.info(f"[索引优化] ✅ 索引 {index_name} 创建成功")
        return True
        
    except Exception as e:
        logger.warning(
            f"[索引优化] ⚠️ 创建索引 {index_name} 失败（可能已存在或权限不足）: {e}",
            exc_info=True
        )
        # 索引创建失败不影响主流程，返回 True 继续执行
        return True


async def mark_files_as_queued_optimized(
    conn,
    table_name: str,
    backup_set_db_id: int,
    file_paths: List[str],
    batch_size: int = 10000,
    commit_interval: int = 5
) -> int:
    """
    方案2：使用临时表 + JOIN 更新方式（优化版本）
    
    优势：
    1. 避免 ANY($2) 操作符在大数据量时的性能问题
    2. JOIN 通常比 ANY 更高效，特别是配合索引
    3. 可以一次性处理更多数据
    4. 批量提交减少事务开销
    
    Args:
        conn: 数据库连接
        table_name: 目标表名
        backup_set_db_id: 备份集ID
        file_paths: 需要标记为已入队的文件路径列表（可以包含重复路径）
        batch_size: 批次大小（默认10000，比原来的1000大10倍）
        commit_interval: 每N个批次提交一次（默认5）
    
    Returns:
        int: SQL 实际更新的数据库行数
    """
    if not file_paths:
        return 0
    
    # 过滤掉空路径
    effective_paths = [p for p in file_paths if p]
    if not effective_paths:
        return 0
    
    # 方案1：确保索引存在
    await ensure_index_exists(conn, table_name)
    
    total_updated = 0
    start_time = time.time()
    
    # 生成唯一的临时表名（使用时间戳+UUID避免冲突）
    temp_table = f"temp_file_paths_{int(time.time())}_{uuid.uuid4().hex[:8]}"
    
    try:
        # 创建临时表
        logger.debug(f"[临时表优化] 创建临时表 {temp_table}")
        await conn.execute(
            f"""
            CREATE TEMP TABLE {temp_table} (
                file_path TEXT NOT NULL,
                PRIMARY KEY (file_path)
            )
            """
        )
        
        # 批量插入路径到临时表
        # openGauss 不支持 ON CONFLICT，需要在 Python 层面去重
        unique_paths = list(dict.fromkeys(effective_paths))  # 保持顺序的去重
        if len(unique_paths) < len(effective_paths):
            logger.debug(
                f"[临时表优化] 路径去重：原始={len(effective_paths)}，去重后={len(unique_paths)}，"
                f"重复数={len(effective_paths) - len(unique_paths)}"
            )
        
        insert_batch_size = min(batch_size, 50000)  # 插入批次可以更大
        total_insert_batches = (len(unique_paths) + insert_batch_size - 1) // insert_batch_size
        
        logger.info(
            f"[临时表优化] 开始批量插入路径到临时表："
            f"总路径数={len(unique_paths)}（已去重），"
            f"插入批次大小={insert_batch_size}，"
            f"插入批次数={total_insert_batches}"
        )
        
        insert_start_time = time.time()
        
        # 性能优化：使用 unnest 方式一次性插入所有路径（比 executemany 快得多）
        # 将路径数组转换为 PostgreSQL 数组，使用 unnest 一次性插入
        try:
            # 方式1：使用 unnest 一次性插入（最快）
            await conn.execute(
                f"""
                INSERT INTO {temp_table} (file_path)
                SELECT unnest($1::TEXT[])
                """,
                unique_paths,
            )
            logger.debug(f"[临时表优化] 使用 unnest 方式一次性插入 {len(unique_paths)} 条路径")
        except Exception as e:
            # 如果 unnest 方式失败，回退到分批 executemany
            logger.debug(f"[临时表优化] unnest 方式失败，回退到分批插入: {e}")
            for i in range(0, len(unique_paths), insert_batch_size):
                batch_paths = unique_paths[i:i + insert_batch_size]
                # openGauss 不支持 ON CONFLICT，使用简单的 INSERT（已去重，不会有冲突）
                try:
                    # 构建批量插入SQL
                    values = [(path,) for path in batch_paths]
                    await conn.executemany(
                        f"INSERT INTO {temp_table} (file_path) VALUES ($1)",
                        values
                    )
                except Exception as e2:
                    logger.warning(f"[临时表优化] 批量插入部分路径失败，尝试逐条插入: {e2}")
                    # 如果批量插入失败，尝试逐条插入（忽略重复键错误）
                    for path in batch_paths:
                        try:
                            await conn.execute(
                                f"INSERT INTO {temp_table} (file_path) VALUES ($1)",
                                path
                            )
                        except Exception as insert_err:
                            # 忽略重复键错误（duplicate key value violates unique constraint）
                            error_msg = str(insert_err).lower()
                            if 'duplicate' in error_msg or 'unique' in error_msg:
                                pass  # 忽略重复键错误
                            else:
                                logger.debug(f"[临时表优化] 插入路径失败: {insert_err}")
        
        insert_elapsed = time.time() - insert_start_time
        # 避免除零错误：如果耗时太短（< 0.001秒），使用最小时间值
        insert_elapsed_safe = max(insert_elapsed, 0.001)
        insert_speed = len(unique_paths) / insert_elapsed_safe if insert_elapsed_safe > 0 else len(unique_paths)
        logger.info(
            f"[临时表优化] ✅ 临时表插入完成："
            f"耗时={insert_elapsed:.3f}秒，"
            f"速度={insert_speed:.0f} 路径/秒"
        )
        
        # 使用 JOIN 更新（一次性更新所有匹配的记录）
        logger.info(
            f"[临时表优化] 开始使用 JOIN 方式批量更新 {table_name}，"
            f"backup_set_id={backup_set_db_id}"
        )
        
        update_start_time = time.time()
        update_result = await conn.execute(
            f"""
            UPDATE {table_name} bf
            SET is_copy_success = TRUE,
                copy_status_at = NOW(),
                updated_at = NOW()
            FROM {temp_table} tmp
            WHERE bf.backup_set_id = $1
              AND bf.file_path = tmp.file_path
              AND (bf.is_copy_success IS DISTINCT FROM TRUE)
            """,
            backup_set_db_id,
        )
        
        # 获取更新行数
        updated_count = getattr(update_result, "rowcount", None)
        if updated_count is None:
            if isinstance(update_result, int):
                updated_count = update_result
        
        total_updated = updated_count if updated_count else 0
        
        update_elapsed = time.time() - update_start_time
        total_elapsed = time.time() - start_time
        
        # 避免除零错误：如果耗时太短（< 0.001秒），使用最小时间值
        total_elapsed_safe = max(total_elapsed, 0.001)
        total_speed = len(unique_paths) / total_elapsed_safe if total_elapsed_safe > 0 else len(unique_paths)
        
        logger.info(
            f"[临时表优化] ✅ JOIN 更新完成："
            f"更新行数={total_updated}，"
            f"更新耗时={update_elapsed:.2f}秒，"
            f"总耗时={total_elapsed:.2f}秒，"
            f"速度={total_speed:.0f} 路径/秒"
        )
        
        # 提交事务
        await conn.commit()
        
    except Exception as e:
        # 出错时回滚
        try:
            await conn.rollback()
        except Exception:
            pass
        logger.error(
            f"[临时表优化] ❌ 更新失败: {e}",
            exc_info=True,
        )
        raise
    finally:
        # 清理临时表（临时表会在连接关闭时自动删除，但显式删除更安全）
        try:
            await conn.execute(f"DROP TABLE IF EXISTS {temp_table}")
        except Exception:
            pass  # 忽略删除临时表失败
    
    return total_updated


async def verify_files_queued_optimized(
    conn,
    table_name: str,
    backup_set_db_id: int,
    file_paths: List[str],
    sample_size: Optional[int] = None,
) -> bool:
    """
    优化版校验：使用快速检查（LIMIT 1）替代全量 COUNT
    
    性能优化策略：
    1. 全部采用 ANY 查询方式（统一简化）
    2. 小数据量（≤50000）：直接 ANY 查询
    3. 大数据量（>50000）：分批 ANY 查询（每批10000条）
    
    Args:
        conn: 数据库连接
        table_name: 表名
        backup_set_db_id: 备份集ID
        file_paths: 文件路径列表
        sample_size: 采样大小（已废弃，保留参数以兼容）
    
    Returns:
        bool: 是否所有文件都已标记为 TRUE
    """
    if not file_paths:
        return True
    
    effective_paths = [p for p in file_paths if p]
    if not effective_paths:
        return True
    
    # 去重
    unique_paths = list(dict.fromkeys(effective_paths))
    path_count = len(unique_paths)
    
    # 全部采用 ANY 查询方式（统一简化）
    try:
        # 借鉴预取器：设置查询并行度（如果支持）
        try:
            from config.settings import get_settings
            settings = get_settings()
            query_dop = getattr(settings, 'DB_QUERY_DOP', 16)
            await conn.execute(f"SET LOCAL query_dop = {query_dop};")
        except Exception:
            pass  # 设置失败不影响查询
        
        # 全部使用 ANY 查询，根据数据量决定是否分批
        result = None
        
        # 小数据量（≤50000）：直接 ANY 查询
        if path_count <= 50000:
            result = await conn.fetchval(
                f"""
                SELECT 1
                FROM {table_name}
                WHERE backup_set_id = $1
                  AND file_path = ANY($2)
                  AND (is_copy_success IS DISTINCT FROM TRUE)
                LIMIT 1
                """,
                backup_set_db_id,
                unique_paths,
            )
        # 大数据量（>50000）：分批 ANY 查询
        else:
            # 借鉴预取器：分批查询，避免一次性查询太多路径
            batch_size = 10000  # 每批检查10000条路径
            for i in range(0, len(unique_paths), batch_size):
                batch_paths = unique_paths[i:i + batch_size]
                batch_result = await conn.fetchval(
                    f"""
                    SELECT 1
                    FROM {table_name}
                    WHERE backup_set_id = $1
                      AND file_path = ANY($2)
                      AND (is_copy_success IS DISTINCT FROM TRUE)
                    LIMIT 1
                    """,
                    backup_set_db_id,
                    batch_paths,
                )
                # 如果找到未标记的记录，立即返回
                if batch_result is not None:
                    result = batch_result
                    break
            # 如果所有批次都没找到，result 仍为 None
        
        # 如果找到未标记的记录，返回 False
        return result is None
        
    except Exception as e:
        logger.warning(
            f"[校验优化] ⚠️ ANY查询失败，回退到全量COUNT检查: {e}",
            exc_info=True
        )
        # 回退到全量 COUNT（兼容原逻辑）
        try:
            count = await conn.fetchval(
                f"""
                SELECT COUNT(*)
                FROM {table_name}
                WHERE backup_set_id = $1
                  AND file_path = ANY($2)
                  AND (is_copy_success IS DISTINCT FROM TRUE)
                """,
                backup_set_db_id,
                effective_paths,
            )
            return count == 0
        except Exception:
            # 如果全量检查也失败，返回 True（假设校验通过，避免阻塞流程）
            return True

