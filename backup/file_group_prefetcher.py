#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
文件组预取器（openGauss模式）
在压缩任务开始前预取文件组，实现压缩和搜索并行执行
"""

import asyncio
import logging
from typing import Optional, Tuple, List, Dict, Any

from models.backup import BackupSet, BackupTask
from backup.backup_db import BackupDB
from config.settings import get_settings

logger = logging.getLogger(__name__)


class FileGroupPrefetcher:
    """文件组预取器（openGauss模式专用）
    
    功能：
    1. 在任务集开始时运行，预取文件组放入队列
    2. 压缩任务从队列获取文件组，不再直接查询数据库
    3. 压缩开始后，预取线程继续工作获取下一组文件
    4. 内存中同时存在2组文件：正在压缩的，待压缩的
    """
    
    def __init__(
        self,
        backup_db: BackupDB,
        backup_set: BackupSet,
        backup_task: BackupTask
    ):
        self.backup_db = backup_db
        self.backup_set = backup_set
        self.backup_task = backup_task
        
        # 文件组队列：容量为2（正在压缩的+待压缩的）
        self.file_group_queue: asyncio.Queue = asyncio.Queue(maxsize=2)
        
        # 预取任务
        self.prefetch_task: Optional[asyncio.Task] = None
        self._running = False
        
        # 最后处理的文件ID（用于连续查询）
        self.last_processed_file_id = 0
        
        # 统计信息
        self.prefetched_groups = 0
        self.total_retrieval_time = 0.0
        
    def start(self):
        """启动预取任务"""
        if self._running:
            logger.warning("[文件组预取器] 预取任务已在运行")
            return
        
        self._running = True
        self.prefetch_task = asyncio.create_task(self._prefetch_loop())
        logger.info("[文件组预取器] 预取任务已启动（openGauss模式）")
    
    async def stop(self):
        """停止预取任务"""
        if not self._running:
            return
        
        logger.info("[文件组预取器] 收到停止信号，正在停止预取任务...")
        self._running = False
        
        if self.prefetch_task:
            self.prefetch_task.cancel()
            try:
                await self.prefetch_task
            except asyncio.CancelledError:
                logger.info("[文件组预取器] 预取任务已取消")
            except Exception as e:
                logger.error(f"[文件组预取器] 停止预取任务时出错: {e}", exc_info=True)
        
        logger.info("[文件组预取器] 预取任务已停止")
    
    async def get_file_group(self, timeout: Optional[float] = None) -> Optional[Tuple[List[List[Dict]], int]]:
        """从队列获取文件组
        
        Args:
            timeout: 超时时间（秒），None表示无限等待
        
        Returns:
            (file_groups, last_processed_id) 或 None（如果超时或停止）
        """
        try:
            if timeout is None:
                result = await self.file_group_queue.get()
            else:
                result = await asyncio.wait_for(self.file_group_queue.get(), timeout=timeout)
            
            # 更新最后处理的文件ID
            if result and isinstance(result, tuple) and len(result) == 2:
                file_groups, last_processed_id = result
                if last_processed_id > self.last_processed_file_id:
                    self.last_processed_file_id = last_processed_id
                return result
            return result
        except asyncio.TimeoutError:
            logger.debug(f"[文件组预取器] 获取文件组超时（{timeout}秒）")
            return None
        except asyncio.CancelledError:
            logger.debug("[文件组预取器] 获取文件组被取消")
            return None
    
    async def _prefetch_loop(self):
        """预取循环：持续从数据库获取文件组并放入队列"""
        logger.info(
            f"[文件组预取器] ========== 预取循环已启动 ==========\n"
            f"backup_set_id={self.backup_set.id}, backup_task_id={self.backup_task.id}"
        )
        
        settings = get_settings()
        wait_retry_count = 0
        max_wait_retries = 6
        prefetch_loop_count = 0  # 预取循环计数
        
        try:
            while self._running:
                prefetch_loop_count += 1
                try:
                    # 检查队列是否已满（容量为2）
                    if self.file_group_queue.full():
                        # 队列已满，等待压缩任务消费
                        logger.debug(
                            f"[文件组预取器] 队列已满（{self.file_group_queue.qsize()}/2），"
                            f"等待压缩任务消费..."
                        )
                        await asyncio.sleep(1.0)  # 短暂等待后继续检查
                        continue
                    
                    # 从数据库获取文件组
                    import time
                    retrieval_start_time = time.time()
                    
                    logger.info(
                        f"[文件组预取器] [循环 #{prefetch_loop_count}] 开始检索文件组："
                        f"队列大小={self.file_group_queue.qsize()}/2, "
                        f"last_processed_id={self.last_processed_file_id}, "
                        f"max_file_size={settings.MAX_FILE_SIZE}"
                    )
                    
                    result = await self.backup_db.fetch_pending_files_grouped_by_size(
                        self.backup_set.id,
                        settings.MAX_FILE_SIZE,
                        self.backup_task.id,
                        should_wait_if_small=(wait_retry_count < max_wait_retries),
                        start_from_id=self.last_processed_file_id
                    )
                    
                    retrieval_elapsed = time.time() - retrieval_start_time
                    self.total_retrieval_time += retrieval_elapsed
                    
                    # 处理返回格式
                    if isinstance(result, tuple) and len(result) == 2:
                        file_groups, last_processed_id = result
                    else:
                        # 兼容旧格式
                        file_groups = result
                        last_processed_id = 0
                    
                    # 更新最后处理的文件ID
                    old_last_processed_id = self.last_processed_file_id
                    # 如果返回的 last_processed_id 为 0，说明检测到异常，需要重置查询起点
                    if last_processed_id == 0 and old_last_processed_id > 0:
                        logger.warning(
                            f"[文件组预取器] 检测到异常返回，重置 last_processed_id: "
                            f"{old_last_processed_id} -> 0（下次将从第一个未压缩文件开始查询）"
                        )
                        self.last_processed_file_id = 0
                    elif last_processed_id > self.last_processed_file_id:
                        self.last_processed_file_id = last_processed_id
                        logger.debug(
                            f"[文件组预取器] 更新 last_processed_id: "
                            f"{old_last_processed_id} -> {self.last_processed_file_id}"
                        )
                    
                    # 计算文件组中的文件总数
                    total_files_in_groups = sum(len(group) for group in file_groups) if file_groups else 0
                    
                    logger.info(
                        f"[文件组预取器] [循环 #{prefetch_loop_count}] 检索完成："
                        f"耗时={retrieval_elapsed:.2f}秒, "
                        f"文件组数量={len(file_groups) if file_groups else 0}, "
                        f"文件总数={total_files_in_groups}, "
                        f"last_processed_id={last_processed_id}, "
                        f"累计预取组数={self.prefetched_groups}, "
                        f"累计检索时间={self.total_retrieval_time:.2f}秒"
                    )
                    
                    # 将文件组放入队列
                    if file_groups:
                        # 有文件组，放入队列（即使扫描已完成，也要继续预读取直到没有文件）
                        await self.file_group_queue.put((file_groups, last_processed_id))
                        self.prefetched_groups += len(file_groups)
                        wait_retry_count = 0  # 重置等待计数
                        logger.info(
                            f"[文件组预取器] [循环 #{prefetch_loop_count}] 已预取 {len(file_groups)} 个文件组"
                            f"（共 {total_files_in_groups} 个文件），"
                            f"队列大小: {self.file_group_queue.qsize()}/2"
                        )
                        
                        # 检查是否完成2个队列读取（队列容量为2）
                        # 注意：使用队列大小而不是累计预取组数来判断是否需要等待
                        # 因为 prefetched_groups 会一直累加，不能准确反映队列状态
                        if self.file_group_queue.qsize() >= 2:
                            # 队列已满，等待压缩任务消费
                            logger.info(
                                f"[文件组预取器] 队列已满（{self.file_group_queue.qsize()}/2），"
                                f"等待压缩任务消费..."
                            )
                            # 检查压缩任务是否完成（通过检查队列是否被消费）
                            while self.file_group_queue.full():
                                await asyncio.sleep(1.0)
                            logger.info(
                                f"[文件组预取器] 压缩任务已消费队列，继续预取..."
                            )
                    else:
                        # 没有收到文件，先进行全库扫描，防止遗漏
                        logger.info(
                            f"[文件组预取器] [循环 #{prefetch_loop_count}] 没有收到文件，"
                            f"进行全库扫描检查是否有遗漏的未压缩文件..."
                        )
                        
                        # 全库扫描：查询所有未压缩文件
                        from utils.scheduler.db_utils import get_opengauss_connection
                        try:
                            async with get_opengauss_connection() as conn:
                                full_search_timeout = 1000.0  # 1000秒超时
                                await conn.execute(f"SET LOCAL statement_timeout = '{int(full_search_timeout)}s'")
                                
                                logger.info(
                                    f"[文件组预取器] [循环 #{prefetch_loop_count}] 开始全库检索所有未压缩文件"
                                    f"（超时时间：{full_search_timeout}秒）..."
                                )
                                all_pending_rows = await asyncio.wait_for(
                                    conn.fetch(
                                        """
                                        SELECT id, file_path, file_name, directory_path, display_name, file_type,
                                               file_size, file_permissions, modified_time, accessed_time
                                        FROM backup_files
                                        WHERE backup_set_id = $1
                                          AND (is_copy_success = FALSE OR is_copy_success IS NULL)
                                          AND file_type = 'file'::backupfiletype
                                        ORDER BY id
                                        """,
                                        self.backup_set.id
                                    ),
                                    timeout=full_search_timeout
                                )
                                
                                logger.info(
                                    f"[文件组预取器] [循环 #{prefetch_loop_count}] 全库检索完成，"
                                    f"找到 {len(all_pending_rows)} 个未压缩文件"
                                )
                                
                                if all_pending_rows:
                                    # 全库扫描找到文件，重置 last_processed_file_id 为 0，重新开始检索
                                    logger.warning(
                                        f"[文件组预取器] [循环 #{prefetch_loop_count}] ⚠️ 全库扫描发现遗漏的未压缩文件："
                                        f"{len(all_pending_rows)} 个，重置 last_processed_file_id=0，重新检索..."
                                    )
                                    self.last_processed_file_id = 0
                                    await asyncio.sleep(1.0)  # 短暂等待后继续检索
                                    continue
                                else:
                                    # 全库扫描也没有文件，查看任务集的完成标记（由文件扫描程序设置）
                                    scan_status = await self.backup_db.get_scan_status(self.backup_task.id)
                                    
                                    logger.info(
                                        f"[文件组预取器] [循环 #{prefetch_loop_count}] 全库扫描未找到文件，"
                                        f"查看任务集的完成标记（扫描状态={scan_status}，由文件扫描程序设置）"
                                    )
                                    
                                    if scan_status == 'completed':
                                        # 文件扫描已完成，且全库扫描确认没有未压缩文件，放入结束标记
                                        logger.info(
                                            "[文件组预取器] 文件扫描已完成且全库扫描确认没有未压缩文件，放入结束标记"
                                        )
                                        await self.file_group_queue.put(([], -1))  # -1 表示结束
                                        break
                                    else:
                                        # 文件扫描未完成，可能还有文件在同步，等待后重复查找
                                        logger.info(
                                            f"[文件组预取器] [循环 #{prefetch_loop_count}] 文件扫描未完成"
                                            f"（扫描状态={scan_status}），可能还有文件在同步，5秒后重复查找文件成组..."
                                        )
                                        await asyncio.sleep(5.0)
                                        continue
                        except asyncio.TimeoutError:
                            logger.error(
                                f"[文件组预取器] [循环 #{prefetch_loop_count}] 全库扫描超时，"
                                f"5秒后重试..."
                            )
                            await asyncio.sleep(5.0)
                            continue
                        except Exception as full_search_error:
                            logger.error(
                                f"[文件组预取器] [循环 #{prefetch_loop_count}] 全库扫描失败: {full_search_error}，"
                                f"5秒后重试...",
                                exc_info=True
                            )
                            await asyncio.sleep(5.0)
                            continue
                
                except asyncio.CancelledError:
                    logger.info(f"[文件组预取器] [循环 #{prefetch_loop_count}] 预取循环被取消")
                    break
                except Exception as e:
                    error_type = type(e).__name__
                    error_msg = str(e)
                    
                    # 检查是否是 BufferError 相关错误
                    is_buffer_error = (
                        error_type == 'BufferError' or 
                        'unexpected trailing' in error_msg.lower() or
                        'buffer' in error_msg.lower() or
                        'AssertionError' in error_type
                    )
                    
                    if is_buffer_error:
                        logger.warning(
                            f"[文件组预取器] [循环 #{prefetch_loop_count}] 数据库缓冲区错误："
                            f"{error_type}: {error_msg}\n"
                            f"这通常是由于查询结果太大或网络传输问题导致的。"
                            f"数据库查询函数会自动重试并减小批次大小。"
                        )
                    else:
                        logger.error(
                            f"[文件组预取器] [循环 #{prefetch_loop_count}] 预取文件组时出错："
                            f"{error_type}: {error_msg}",
                            exc_info=True
                        )
                    
                    # 出错后等待一段时间再重试
                    logger.info(
                        f"[文件组预取器] [循环 #{prefetch_loop_count}] 5秒后重试..."
                    )
                    await asyncio.sleep(5.0)
        
        except Exception as e:
            logger.error(
                f"[文件组预取器] 预取循环异常（循环 #{prefetch_loop_count}）: {e}",
                exc_info=True
            )
        finally:
            avg_retrieval_time = (
                self.total_retrieval_time / prefetch_loop_count 
                if prefetch_loop_count > 0 else 0
            )
            logger.info(
                f"[文件组预取器] ========== 预取循环已结束 ==========\n"
                f"总循环次数: {prefetch_loop_count}\n"
                f"预取文件组数: {self.prefetched_groups}\n"
                f"总检索时间: {self.total_retrieval_time:.2f}秒\n"
                f"平均检索时间: {avg_retrieval_time:.2f}秒/次\n"
                f"最后处理的文件ID: {self.last_processed_file_id}\n"
                f"队列剩余大小: {self.file_group_queue.qsize()}/2"
            )
    
    def get_stats(self) -> Dict[str, Any]:
        """获取统计信息"""
        return {
            'prefetched_groups': self.prefetched_groups,
            'total_retrieval_time': self.total_retrieval_time,
            'queue_size': self.file_group_queue.qsize(),
            'last_processed_file_id': self.last_processed_file_id
        }

