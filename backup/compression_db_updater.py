#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
openGauss 数据库统一调度器 - 异步批量更新压缩完成后的文件信息和内存数据库同步
OpenGauss Database Unified Scheduler - Asynchronous batch update of compressed file information and memory database sync
"""

import asyncio
import logging
from typing import List, Dict, Optional, Tuple, Any
from datetime import datetime
import time
import json

from utils.scheduler.db_utils import is_opengauss, get_opengauss_connection
from backup.utils import format_bytes

logger = logging.getLogger(__name__)


class OpenGaussDBScheduler:
    """openGauss 数据库统一调度器 - 异步批量更新压缩完成后的文件信息和内存数据库同步
    
    功能：
    1. 接收压缩完成的文件信息（通过队列），每3000个文件批量更新一次数据库
    2. 接收内存数据库同步请求（通过队列），每3000个文件批量插入一次数据库
    3. 使用无限队列，支持多压缩线程并发提交
    4. 统一处理事务提交和回滚，避免长事务锁表
    5. 处理压缩取消后的剩余文件
    """
    
    def __init__(self, backup_set_db_id: int, batch_size: int = 3000):
        """
        Args:
            backup_set_db_id: 备份集数据库ID
            batch_size: 批量更新大小（默认3000个文件）
        """
        self.backup_set_db_id = backup_set_db_id
        self.batch_size = batch_size
        
        # 无限队列，用于接收压缩完成的文件信息
        # 格式: ('compression', group_idx, file_paths, chunk_number, compressed_size, original_size)
        # 或: ('sync', file_data_map)
        self.update_queue = asyncio.Queue(maxsize=0)
        
        # 压缩更新缓冲区
        self.compression_buffer: List[Tuple[int, List[str], int, int, int]] = []  # (group_idx, file_paths, chunk_number, compressed_size, original_size)
        self.compression_buffer_file_count = 0  # 压缩缓冲区中的文件总数
        
        # 内存数据库同步缓冲区
        self.sync_buffer: List[Tuple] = []  # file_data_map 中的项
        self.sync_buffer_file_count = 0  # 同步缓冲区中的文件总数
        
        self.buffer_lock = asyncio.Lock()  # 保护缓冲区的锁
        
        # 运行状态
        self._running = False
        self._update_task: Optional[asyncio.Task] = None
        
        # 统计信息
        self.total_compression_received = 0
        self.total_compression_updated = 0
        self.total_compression_batches = 0
        self.total_sync_received = 0
        self.total_sync_inserted = 0
        self.total_sync_batches = 0
        
        # 表存在性缓存（避免每次检查）
        self._backup_files_table_exists: Optional[bool] = None
        
    def start(self):
        """启动调度器"""
        if self._running:
            logger.warning("[openGauss调度器] 已在运行中")
            return
        
        if not is_opengauss():
            logger.warning("[openGauss调度器] 非 openGauss 模式，不启动")
            return
        
        self._running = True
        self._update_task = asyncio.create_task(self._update_loop())
        logger.info(f"[openGauss调度器] 已启动 (backup_set_db_id={self.backup_set_db_id}, batch_size={self.batch_size})")
    
    async def stop(self):
        """停止调度器，处理剩余文件"""
        if not self._running:
            return
        
        self._running = False
        
        # 等待队列中的任务处理完成
        if self._update_task:
            # 发送停止信号
            await self.update_queue.put(('stop', None, None, None, None, None))
            
            try:
                # 等待任务完成，最多等待60秒
                await asyncio.wait_for(self._update_task, timeout=60.0)
            except asyncio.TimeoutError:
                logger.warning("[openGauss调度器] 停止超时，取消任务")
                self._update_task.cancel()
                try:
                    await self._update_task
                except asyncio.CancelledError:
                    pass
        
        # 处理缓冲区中剩余的文件
        async with self.buffer_lock:
            if self.compression_buffer:
                logger.info(f"[压缩DB更新器] 处理剩余压缩更新 {len(self.compression_buffer)} 个批次...")
                await self._flush_compression_buffer()
            if self.sync_buffer:
                logger.info(f"[openGauss同步] 处理剩余内存同步 {len(self.sync_buffer)} 个文件...")
                await self._flush_sync_buffer()
        
        logger.info(
            f"[统一调度器] 已停止 - "
            f"压缩: 接收={self.total_compression_received}, 更新={self.total_compression_updated}, 批次={self.total_compression_batches}; "
            f"同步: 接收={self.total_sync_received}, 插入={self.total_sync_inserted}, 批次={self.total_sync_batches}"
        )
    
    async def submit_compressed_files(
        self,
        group_idx: int,
        file_paths: List[str],
        chunk_number: int,
        compressed_size: int,
        original_size: int
    ):
        """提交压缩完成的文件信息
        
        Args:
            group_idx: 文件组索引
            file_paths: 文件路径列表（不能为空）
            chunk_number: 块编号
            compressed_size: 压缩后大小（整个文件组的总大小）
            original_size: 原始大小（整个文件组的总大小）
        """
        # 空列表检查：避免执行无意义的 SQL
        if not file_paths:
            logger.debug(f"[压缩DB更新器] 文件组 #{group_idx} 为空，跳过提交")
            return
        
        if not self._running:
            logger.warning("[压缩DB更新器] 调度器未运行，无法提交文件信息")
            return
        
        try:
            await self.update_queue.put((
                'compression',
                group_idx,
                file_paths,
                chunk_number,
                compressed_size,
                original_size
            ))
            self.total_compression_received += len(file_paths)
            logger.info(
                f"[压缩DB更新器] ✅ 已提交压缩文件组 #{group_idx}: "
                f"{len(file_paths)} 个文件, chunk_number={chunk_number}, "
                f"压缩大小={format_bytes(compressed_size)}"
            )
        except Exception as e:
            logger.error(f"[openGauss调度器] 提交压缩文件信息失败: {str(e)}", exc_info=True)
    
    async def submit_sync_files(self, file_data_map: List[Tuple]):
        """提交内存数据库同步请求
        
        Args:
            file_data_map: 文件数据映射列表 [(file_record, data_tuple), ...]
        """
        # 空列表检查：避免执行无意义的 SQL
        if not file_data_map:
            logger.debug("[openGauss同步] 同步文件列表为空，跳过提交")
            return
        
        if not self._running:
            logger.warning("[openGauss同步] 调度器未运行，无法提交同步请求")
            return
        
        try:
            await self.update_queue.put(('sync', None, None, None, None, file_data_map))
            self.total_sync_received += len(file_data_map)
            logger.info(
                f"[openGauss同步] ✅ 已提交内存同步请求: {len(file_data_map)} 个文件"
            )
        except Exception as e:
            logger.error(f"[openGauss调度器] 提交同步请求失败: {str(e)}", exc_info=True)
    
    async def _update_loop(self):
        """更新循环 - 从队列获取文件信息并批量更新"""
        logger.info("[统一调度器] 更新循环已启动（处理压缩更新和内存同步）")
        
        try:
            while self._running:
                try:
                    # 从队列获取文件信息（带超时，避免无限等待）
                    item = await asyncio.wait_for(self.update_queue.get(), timeout=5.0)
                    
                    # 检查是否是停止信号
                    if isinstance(item, tuple) and len(item) >= 1 and item[0] == 'stop':
                        logger.debug("[openGauss调度器] 收到停止信号")
                        break
                    
                    # 解析任务类型
                    task_type = item[0] if isinstance(item, tuple) and len(item) > 0 else None
                    
                    if task_type == 'compression':
                        # 压缩更新任务（取消 batch_size 限制：每次收到就立即刷一次）
                        _, group_idx, file_paths, chunk_number, compressed_size, original_size = item
                        # 空列表检查
                        if not file_paths:
                            continue
                        
                        file_count = len(file_paths)
                        async with self.buffer_lock:
                            self.compression_buffer.append((group_idx, file_paths, chunk_number, compressed_size, original_size))
                            self.compression_buffer_file_count += file_count
                            
                            logger.debug(
                                f"[压缩DB更新器] 接收文件组 #{group_idx}: {file_count} 个文件, "
                                f"缓冲区累计: {self.compression_buffer_file_count}（已配置为每次立即刷新）"
                            )
                            
                            # 不再依赖 batch_size 条件，有数据就立即批量更新
                            await self._flush_compression_buffer()
                    
                    elif task_type == 'sync':
                        # 内存数据库同步任务（取消 batch_size 限制：每次收到就立即刷一次）
                        _, _, _, _, _, file_data_map = item
                        # 空列表检查
                        if not file_data_map:
                            continue
                        
                        file_count = len(file_data_map)
                        async with self.buffer_lock:
                            self.sync_buffer.extend(file_data_map)
                            self.sync_buffer_file_count += file_count
                            
                            logger.debug(
                                f"[openGauss同步] 接收同步请求: {file_count} 个文件, "
                                f"缓冲区累计: {self.sync_buffer_file_count}（已配置为每次立即刷新）"
                            )
                            
                            # 不再依赖 batch_size 条件，有数据就立即批量同步
                            await self._flush_sync_buffer()
                    
                except asyncio.TimeoutError:
                    # 超时：检查缓冲区是否有数据需要处理
                    async with self.buffer_lock:
                        if self.compression_buffer:
                            logger.debug(f"[压缩DB更新器] 超时，处理压缩缓冲区中的 {len(self.compression_buffer)} 个批次")
                            await self._flush_compression_buffer()
                        if self.sync_buffer:
                            logger.debug(f"[openGauss同步] 超时，处理同步缓冲区中的 {len(self.sync_buffer)} 个文件")
                            await self._flush_sync_buffer()
                    continue
                except Exception as e:
                    logger.error(f"[openGauss调度器] 更新循环异常: {str(e)}", exc_info=True)
                    await asyncio.sleep(1)  # 错误后短暂等待
            
            # 循环结束前，处理剩余的缓冲区数据
            async with self.buffer_lock:
                if self.compression_buffer:
                    logger.info(f"[压缩DB更新器] 处理剩余压缩缓冲区中的 {len(self.compression_buffer)} 个批次")
                    await self._flush_compression_buffer()
                if self.sync_buffer:
                    logger.info(f"[openGauss同步] 处理剩余同步缓冲区中的 {len(self.sync_buffer)} 个文件")
                    await self._flush_sync_buffer()
        
        except asyncio.CancelledError:
            logger.debug("[统一调度器] 更新循环被取消")
            # 取消时也处理剩余数据
            async with self.buffer_lock:
                if self.compression_buffer:
                    logger.info(f"[压缩DB更新器] 取消时处理剩余压缩缓冲区中的 {len(self.compression_buffer)} 个批次")
                    await self._flush_compression_buffer()
                if self.sync_buffer:
                    logger.info(f"[openGauss同步] 取消时处理剩余同步缓冲区中的 {len(self.sync_buffer)} 个文件")
                    await self._flush_sync_buffer()
        except Exception as e:
            logger.error(f"[统一调度器] 更新循环异常: {str(e)}", exc_info=True)
        finally:
            logger.info("[统一调度器] 更新循环已结束")
    
    async def _flush_compression_buffer(self):
        """刷新压缩更新缓冲区 - 批量更新数据库"""
        if not self.compression_buffer:
            return
        
        # 提取要处理的批次（累积文件数达到 batch_size）
        batches_to_process = []
        files_to_process = 0
        
        for item in self.compression_buffer:
            group_idx, file_paths, chunk_number, compressed_size, original_size = item
            file_count = len(file_paths)
            
            if files_to_process + file_count <= self.batch_size:
                batches_to_process.append(item)
                files_to_process += file_count
            else:
                # 如果加上这个批次会超过 batch_size，停止
                break
        
        if not batches_to_process:
            return
        
        # 从缓冲区移除已处理的批次
        self.compression_buffer = self.compression_buffer[len(batches_to_process):]
        self.compression_buffer_file_count -= files_to_process
        
        # 统计信息
        total_files = 0
        total_compressed_size = 0
        total_original_size = 0
        
        # 合并所有批次的文件信息
        all_file_updates: Dict[str, Dict] = {}  # {file_path: {chunk_number, compressed_size}}
        
        for group_idx, file_paths, chunk_number, compressed_size, original_size in batches_to_process:
            total_files += len(file_paths)
            total_compressed_size += compressed_size
            total_original_size += original_size
            
            # 计算每个文件的压缩大小（平均分配）
            per_file_compressed_size = compressed_size // len(file_paths) if file_paths else 0
            
            # 合并到更新字典（如果同一个文件在多个批次中，使用最新的信息）
            for file_path in file_paths:
                all_file_updates[file_path] = {
                    'chunk_number': chunk_number,
                    'compressed_size': per_file_compressed_size
                }
        
        if not all_file_updates:
            logger.warning("[压缩DB更新器] 没有压缩文件需要更新")
            return
        
        logger.info(
            f"[压缩DB更新器] 📦 开始批量更新压缩信息: {len(batches_to_process)} 个批次, "
            f"{len(all_file_updates)} 个文件, "
            f"压缩大小={format_bytes(total_compressed_size)}"
        )
        
        # 批量更新数据库
        update_start_time = time.time()
        try:
            await self._update_compression_opengauss(all_file_updates)
            
            update_time = time.time() - update_start_time
            self.total_compression_updated += len(all_file_updates)
            self.total_compression_batches += 1
            
            logger.info(
                f"[压缩DB更新器] ✅ 批量更新压缩信息完成: {len(all_file_updates)} 个文件, "
                f"耗时={update_time:.2f}秒, "
                f"速度={len(all_file_updates)/update_time:.1f} 文件/秒"
            )
        
        except Exception as e:
            logger.error(f"[压缩DB更新器] ❌ 批量更新压缩信息失败: {str(e)}", exc_info=True)
            # 更新失败时，将批次重新放回缓冲区（避免数据丢失）
            async with self.buffer_lock:
                self.compression_buffer = batches_to_process + self.compression_buffer
                self.compression_buffer_file_count += files_to_process
            raise
    
    async def _flush_sync_buffer(self):
        """刷新内存数据库同步缓冲区 - 批量插入数据库"""
        if not self.sync_buffer:
            return
        
        # 提取要处理的文件（累积文件数达到 batch_size）
        files_to_process = []
        files_count = 0
        
        for item in self.sync_buffer:
            file_count = 1  # 每个 item 是一个文件
            if files_count + file_count <= self.batch_size:
                files_to_process.append(item)
                files_count += file_count
            else:
                break
        
        if not files_to_process:
            return
        
        # 从缓冲区移除已处理的文件
        self.sync_buffer = self.sync_buffer[len(files_to_process):]
        self.sync_buffer_file_count -= files_count
        
        logger.info(
            f"[openGauss同步] 📥 开始批量同步内存数据库: {len(files_to_process)} 个文件"
        )
        
        # 批量插入数据库
        sync_start_time = time.time()
        try:
            synced_file_ids = await self._insert_sync_files_opengauss(files_to_process)
            
            sync_time = time.time() - sync_start_time
            self.total_sync_inserted += len(synced_file_ids)
            self.total_sync_batches += 1
            
            logger.info(
                f"[openGauss同步] ✅ 批量同步内存数据库完成: {len(synced_file_ids)} 个文件, "
                f"耗时={sync_time:.2f}秒, "
                f"速度={len(synced_file_ids)/sync_time:.1f} 文件/秒"
            )
        
        except Exception as e:
            logger.error(f"[openGauss同步] ❌ 批量同步内存数据库失败: {str(e)}", exc_info=True)
            # 同步失败时，将文件重新放回缓冲区（避免数据丢失）
            async with self.buffer_lock:
                self.sync_buffer = files_to_process + self.sync_buffer
                self.sync_buffer_file_count += files_count
            raise
    
    async def _update_compression_opengauss(self, file_updates: Dict[str, Dict]):
        """更新 openGauss 数据库 - 压缩信息更新
        
        Args:
            file_updates: {file_path: {chunk_number, compressed_size}}
        """
        # 空列表检查：避免执行无意义的 SQL
        if not file_updates:
            return
        
        # 准备批量更新参数
        update_params = []
        for file_path, update_info in file_updates.items():
            update_params.append((
                update_info['chunk_number'],
                update_info['compressed_size'],
                self.backup_set_db_id,
                file_path
            ))
        
        # 复用连接，避免连接泄漏
        async with get_opengauss_connection() as conn:
            actual_conn = None
            try:
                # 获取实际连接对象（用于事务管理）
                actual_conn = conn._conn if hasattr(conn, '_conn') else conn
                
                # 执行批量更新
                # 注意：如果记录不存在（可能还在内存数据库中未同步），更新会失败（rowcount = 0）
                # 这是正常的竞争条件，不会影响数据一致性
                rowcount = await conn.executemany(
                    """
                    UPDATE backup_files
                    SET chunk_number = $1,
                        compressed_size = $2,
                        updated_at = NOW()
                    WHERE backup_set_id = $3
                      AND file_path = $4
                      AND (is_copy_success = TRUE OR is_copy_success IS NULL OR is_copy_success = FALSE)
                    """,
                    update_params
                )
                
                # 显式提交事务（openGauss 模式需要显式提交）
                try:
                    await actual_conn.commit()
                    logger.debug(f"[openGauss调度器] 压缩更新事务已提交: {rowcount} 个文件")
                except Exception as commit_err:
                    logger.warning(f"[openGauss调度器] 提交事务失败（可能已自动提交）: {commit_err}")
                
                # 验证更新结果
                if rowcount < len(update_params):
                    missing_count = len(update_params) - rowcount
                    logger.debug(
                        f"[openGauss调度器] ⚠️ 部分文件未更新: "
                        f"期望={len(update_params)}, 实际={rowcount}, 缺失={missing_count}。"
                        f"这可能是正常的竞争条件（记录还在内存数据库中未同步）。"
                    )
                
            except Exception as e:
                # 异常时显式回滚，避免长事务锁表
                if actual_conn and hasattr(actual_conn, 'info'):
                    try:
                        transaction_status = actual_conn.info.transaction_status
                        if transaction_status in (1, 3):  # INTRANS or INERROR
                            await actual_conn.rollback()
                            logger.debug("[openGauss调度器] 异常时事务已回滚")
                    except Exception as rollback_err:
                        logger.warning(f"[openGauss调度器] 回滚事务失败: {str(rollback_err)}")
                raise
    
    async def _insert_sync_files_opengauss(self, file_data_map: List[Tuple]) -> List[int]:
        """插入内存数据库同步文件到 openGauss
        
        Args:
            file_data_map: 文件数据映射列表 [(file_record, data_tuple), ...]
            
        Returns:
            成功插入的文件ID列表
        """
        # 空列表检查：避免执行无意义的 SQL
        if not file_data_map:
            return []
        
        synced_file_ids: List[int] = []
        
        # 准备批量插入数据
        insert_data = []
        for file_record, _ in file_data_map:
            if not file_record:
                continue
            
            file_id = file_record[0]
            backup_set_id = file_record[1]
            
            # 验证 backup_set_id
            if backup_set_id != self.backup_set_db_id:
                logger.error(
                    f"[openGauss调度器] ⚠️ 文件 backup_set_id={backup_set_id} "
                    f"与调度器的 backup_set_db_id={self.backup_set_db_id} 不匹配！"
                )
                continue
            
            # 提取文件数据
            file_path = file_record[2]
            file_name = file_record[3]
            directory_path = file_record[4]
            display_name = file_record[5]
            file_type = file_record[6] or "file"
            file_size = file_record[7] or 0
            compressed_size = file_record[8]
            file_permissions = file_record[9]
            file_owner = file_record[10]
            file_group = file_record[11]
            created_time = self._parse_datetime_from_sqlite(file_record[12])
            modified_time = self._parse_datetime_from_sqlite(file_record[13])
            accessed_time = self._parse_datetime_from_sqlite(file_record[14])
            tape_block_start = file_record[15]
            tape_block_count = file_record[16]
            compressed = bool(file_record[17])
            encrypted = bool(file_record[18])
            checksum = file_record[19]
            is_copy_success = bool(file_record[20])
            copy_status_at = self._parse_datetime_from_sqlite(file_record[21])
            backup_time = self._parse_datetime_from_sqlite(file_record[22])
            chunk_number = file_record[23]
            version = file_record[24]
            file_metadata = file_record[25]
            tags = file_record[26]
            
            # 准备插入数据元组
            data_tuple = (
                backup_set_id, file_path, file_name,
                directory_path, display_name, file_type,
                file_size, compressed_size, file_permissions,
                file_owner, file_group, created_time,
                modified_time, accessed_time, tape_block_start,
                tape_block_count, compressed, encrypted,
                checksum, is_copy_success, copy_status_at,
                backup_time, chunk_number, version,
                file_metadata, tags
            )
            insert_data.append(data_tuple)
            synced_file_ids.append(file_id)
        
        if not insert_data:
            logger.warning("[openGauss调度器] 没有有效的数据可以插入")
            return []
        
        # 复用连接，避免连接泄漏
        async with get_opengauss_connection() as conn:
            actual_conn = None
            try:
                # 获取实际连接对象（用于事务管理）
                actual_conn = conn._conn if hasattr(conn, '_conn') else conn
                
                # 检查表是否存在（使用缓存，避免每次检查）
                if self._backup_files_table_exists is None:
                    try:
                        await conn.fetchrow("SELECT 1 FROM backup_files LIMIT 1")
                        self._backup_files_table_exists = True
                    except Exception as table_check_err:
                        error_msg = str(table_check_err)
                        # 异常时显式回滚
                        if hasattr(actual_conn, 'info'):
                            transaction_status = actual_conn.info.transaction_status
                            if transaction_status in (1, 3):
                                await actual_conn.rollback()
                        
                        if "does not exist" in error_msg.lower() or "relation" in error_msg.lower():
                            self._backup_files_table_exists = False
                            logger.warning("[openGauss调度器] backup_files 表不存在，跳过插入")
                            return []
                        else:
                            logger.warning(f"[openGauss调度器] 检查表时出错: {error_msg}")
                            return []
                
                if self._backup_files_table_exists is False:
                    return []
                
                # 执行批量插入
                rowcount = await conn.executemany(
                    """
                    INSERT INTO backup_files (
                        backup_set_id, file_path, file_name, directory_path, display_name,
                        file_type, file_size, compressed_size, file_permissions, file_owner,
                        file_group, created_time, modified_time, accessed_time, tape_block_start,
                        tape_block_count, compressed, encrypted, checksum, is_copy_success,
                        copy_status_at, backup_time, chunk_number, version, file_metadata, tags,
                        created_at, updated_at
                    ) VALUES (
                        $1, $2, $3, $4, $5, $6, $7, $8, $9, $10,
                        $11, $12, $13, $14, $15, $16, $17, $18, $19, $20,
                        $21, $22, $23, $24, $25::jsonb, $26::jsonb, NOW(), NOW()
                    )
                    """,
                    insert_data
                )
                
                # 显式提交事务（openGauss 模式需要显式提交）
                try:
                    await actual_conn.commit()
                    logger.debug(f"[openGauss调度器] 内存同步事务已提交: {rowcount} 个文件")
                except Exception as commit_err:
                    logger.warning(f"[openGauss调度器] 提交事务失败（可能已自动提交）: {commit_err}")
                
                # 验证插入结果
                if rowcount != len(insert_data):
                    logger.warning(
                        f"[openGauss调度器] ⚠️ 部分文件未插入: "
                        f"期望={len(insert_data)}, 实际={rowcount}"
                    )
                    if rowcount > 0:
                        synced_file_ids = synced_file_ids[:rowcount]
                    else:
                        synced_file_ids = []
                
            except Exception as e:
                # 异常时显式回滚，避免长事务锁表
                if actual_conn and hasattr(actual_conn, 'info'):
                    try:
                        transaction_status = actual_conn.info.transaction_status
                        if transaction_status in (1, 3):  # INTRANS or INERROR
                            await actual_conn.rollback()
                            logger.debug("[openGauss调度器] 异常时事务已回滚")
                    except Exception as rollback_err:
                        logger.warning(f"[openGauss调度器] 回滚事务失败: {str(rollback_err)}")
                raise
        
        return synced_file_ids
    
    def _parse_datetime_from_sqlite(self, dt_value) -> Optional[datetime]:
        """将SQLite的datetime值转换为Python datetime对象"""
        if dt_value is None:
            return None
        if isinstance(dt_value, datetime):
            return dt_value
        if isinstance(dt_value, str):
            try:
                return datetime.fromisoformat(dt_value.replace('Z', '+00:00'))
            except:
                try:
                    return datetime.strptime(dt_value, '%Y-%m-%d %H:%M:%S')
                except:
                    return None
        return None
    
    def get_stats(self) -> Dict[str, Any]:
        """获取统计信息"""
        return {
            'compression': {
                'total_received': self.total_compression_received,
                'total_updated': self.total_compression_updated,
                'total_batches': self.total_compression_batches,
                'buffer_size': len(self.compression_buffer),
                'buffer_file_count': self.compression_buffer_file_count
            },
            'sync': {
                'total_received': self.total_sync_received,
                'total_inserted': self.total_sync_inserted,
                'total_batches': self.total_sync_batches,
                'buffer_size': len(self.sync_buffer),
                'buffer_file_count': self.sync_buffer_file_count
            },
            'queue_size': self.update_queue.qsize()
        }


# 为了向后兼容，保留 CompressionDBUpdater 作为别名
CompressionDBUpdater = OpenGaussDBScheduler

