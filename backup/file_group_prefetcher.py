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
from backup.utils import format_bytes
from config.settings import get_settings

logger = logging.getLogger(__name__)


class FileGroupPrefetcher:
    """文件组预取器（openGauss模式专用）
    
    功能：
    1. 在任务集开始时运行，预取文件组放入队列
    2. 压缩任务从队列获取文件组，不再直接查询数据库
    3. 压缩开始后，预取线程继续工作获取下一组文件
    4. 内存中同时存在N+1组文件：N个正在压缩的，1个待压缩的（N=COMPRESSION_PARALLEL_BATCHES）
    """
    
    def __init__(
        self,
        backup_db: BackupDB,
        backup_set: BackupSet,
        backup_task: BackupTask,
        parallel_batches: Optional[int] = None
    ):
        self.backup_db = backup_db
        self.backup_set = backup_set
        self.backup_task = backup_task
        
        # 获取并行批次数量（默认从配置读取）
        if parallel_batches is None:
            settings = get_settings()
            parallel_batches = getattr(settings, 'COMPRESSION_PARALLEL_BATCHES', 2)
        
        self.parallel_batches = parallel_batches
        # 文件组队列：容量为 parallel_batches + 1（N个正在压缩的 + 1个待压缩的）
        self.queue_maxsize = parallel_batches + 1
        self.file_group_queue: asyncio.Queue = asyncio.Queue(maxsize=self.queue_maxsize)
        
        # 预取任务
        self.prefetch_task: Optional[asyncio.Task] = None
        self._running = False
        
        # 最后处理的文件ID（用于连续查询）
        self.last_processed_file_id = 0
        
        # 统计信息
        self.prefetched_groups = 0
        self.total_retrieval_time = 0.0
        
        # 队列中的文件总数（用于进度显示）- 使用计数器维护
        self.queued_files_count = 0  # 当前队列中所有文件组的总文件数
        self.total_queued_files_count = 0  # 累计放入队列的总文件数（用于进度显示）
        self.total_queued_size = 0  # 累计放入队列的总文件大小（字节）
        
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
            
            # 更新最后处理的文件ID和队列文件数
            if result and isinstance(result, tuple) and len(result) == 2:
                file_groups, last_processed_id = result
                if last_processed_id > self.last_processed_file_id:
                    self.last_processed_file_id = last_processed_id
                
                # 从队列中取出文件组后，减少队列中的文件数（任务完成时移除）
                if file_groups:
                    files_in_removed_groups = sum(len(group) for group in file_groups)
                    self.queued_files_count = max(0, self.queued_files_count - files_in_removed_groups)
                    logger.debug(
                        f"[文件组预取器] 从队列取出文件组，减少 {files_in_removed_groups} 个文件，"
                        f"队列中剩余文件数: {self.queued_files_count}"
                    )
                
                return result
            return result
        except asyncio.TimeoutError:
            logger.debug(f"[文件组预取器] 获取文件组超时（{timeout}秒）")
            return None
        except asyncio.CancelledError:
            logger.debug("[文件组预取器] 获取文件组被取消")
            return None
    
    def get_queued_files_count(self) -> int:
        """获取队列中的总文件数（用于进度显示）
        
        返回累计放入队列的总文件数，而不是当前队列中的文件数。
        这样可以正确显示进度：已处理文件数/累计放入队列的总文件数。
        
        Returns:
            累计放入队列的总文件数（用于进度显示）
        """
        # 优先使用累计放入队列的总文件数
        if self.total_queued_files_count > 0:
            return self.total_queued_files_count
        # 如果累计数为0，可能是还没有文件放入队列，使用当前队列中的文件数
        # 但这种情况应该很少见，因为进度更新通常在文件放入队列后才开始
        return self.queued_files_count
    
    async def _prefetch_loop(self):
        """预取循环：持续从数据库获取文件组并放入队列
        
        逻辑：
        1. 保证队列始终是 3/3（满状态）
        2. 6秒轮询检测队列大小
        3. 如果文件大小不足无法组成新文件组，队列保持当前状态，直到有新文件组能入队
        4. 扫描任务完成后，如果不够一组，全库扫描避免遗漏，还不够1组，停止线程
        5. 预取器不能阻塞其他线程（所有操作都是异步的）
        """
        logger.info(
            f"[文件组预取器] ========== 预取循环已启动 ==========\n"
            f"backup_set_id={self.backup_set.id}, backup_task_id={self.backup_task.id}, "
            f"队列容量={self.queue_maxsize}"
        )
        
        settings = get_settings()
        wait_retry_count = 0
        max_wait_retries = 6
        prefetch_loop_count = 0  # 预取循环计数
        queue_check_interval = 6.0  # 6秒轮询检测队列大小
        
        try:
            while self._running:
                prefetch_loop_count += 1
                try:
                    # 6秒轮询检测队列大小
                    current_queue_size = self.file_group_queue.qsize()
                    
                    # 如果队列未满，尝试填充
                    if current_queue_size < self.queue_maxsize:
                        # 队列未满，继续预取
                        logger.debug(
                            f"[文件组预取器] 队列未满（{current_queue_size}/{self.queue_maxsize}），"
                            f"继续预取文件组..."
                        )
                    else:
                        # 队列已满（3/3），6秒后再次检查
                        logger.debug(
                            f"[文件组预取器] 队列已满（{current_queue_size}/{self.queue_maxsize}），"
                            f"{queue_check_interval}秒后再次检查..."
                        )
                        await asyncio.sleep(queue_check_interval)
                        continue
                    
                    # 从数据库获取文件组
                    import time
                    retrieval_start_time = time.time()
                    
                    logger.info(
                        f"[文件组预取器] [循环 #{prefetch_loop_count}] 开始检索文件组："
                        f"队列大小✅={self.file_group_queue.qsize()}/{self.queue_maxsize}, "
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
                    
                    # 计算文件组中的文件总数和总大小
                    total_files_in_groups = sum(len(group) for group in file_groups) if file_groups else 0
                    total_size_in_groups = sum(
                        sum(file_info.get('size', 0) for file_info in group)
                        for group in file_groups
                    ) if file_groups else 0
                    
                    # 获取扫描状态用于日志输出
                    scan_status = await self.backup_db.get_scan_status(self.backup_task.id) if not file_groups else None
                    
                    logger.info(
                        f"[文件组预取器] [循环 #{prefetch_loop_count}] 检索完成："
                        f"耗时={retrieval_elapsed:.2f}秒, "
                        f"文件组数量={len(file_groups) if file_groups else 0}, "
                        f"文件总数={total_files_in_groups}, "
                        f"当前组大小={format_bytes(total_size_in_groups)}, "
                        f"累计总大小={format_bytes(self.total_queued_size)}, "
                        f"last_processed_id={last_processed_id}, "
                        f"扫描状态={scan_status if scan_status else 'N/A'}, "
                        f"累计预取组数={self.prefetched_groups}, "
                        f"累计检索时间={self.total_retrieval_time:.2f}秒"
                    )
                    
                    # 将文件组放入队列
                    if file_groups:
                        # 在放入队列前，直接设置 is_copy_success = TRUE
                        try:
                            logger.info(
                                f"[文件组预取器] [循环 #{prefetch_loop_count}] 开始标记文件为已入队："
                                f"{len(file_groups)} 个文件组，共 {total_files_in_groups} 个文件，"
                                f"总大小={format_bytes(total_size_in_groups)}"
                            )
                            await self.backup_db.mark_files_as_queued(
                                backup_set=self.backup_set,
                                file_groups=file_groups
                            )
                            logger.info(
                                f"[文件组预取器] [循环 #{prefetch_loop_count}] ✅ 文件标记完成，"
                                f"is_copy_success 已设置为 TRUE"
                            )
                        except Exception as mark_error:
                            logger.error(
                                f"[文件组预取器] [循环 #{prefetch_loop_count}] ⚠️ 标记文件失败: {str(mark_error)}",
                                exc_info=True
                            )
                            # 即使标记失败，也继续放入队列，避免阻塞流程
                        
                        # 有文件组，放入队列（非阻塞，如果队列满则等待）
                        try:
                            # 使用 put_nowait 尝试非阻塞放入，如果队列满则等待
                            if self.file_group_queue.full():
                                logger.debug(
                                    f"[文件组预取器] 队列已满，等待放入文件组..."
                                )
                            await self.file_group_queue.put((file_groups, last_processed_id))
                            self.prefetched_groups += len(file_groups)
                            # 更新队列中的文件总数（放入队列时增加）
                            self.queued_files_count += total_files_in_groups
                            # 更新累计放入队列的总文件数（用于进度显示，不减少）
                            self.total_queued_files_count += total_files_in_groups
                            # 更新累计放入队列的总文件大小（用于进度显示，不减少）
                            self.total_queued_size += total_size_in_groups
                            wait_retry_count = 0  # 重置等待计数
                            logger.info(
                                f"[文件组预取器] [循环 #{prefetch_loop_count}] 已预取 {len(file_groups)} 个文件组"
                                f"（共 {total_files_in_groups} 个文件，当前组大小={format_bytes(total_size_in_groups)}，累计总大小={format_bytes(self.total_queued_size)}），"
                                f"✅队列大小: {self.file_group_queue.qsize()}/{self.queue_maxsize}, "
                                f"当前队列文件数: {self.queued_files_count}, "
                                f"累计队列文件数: {self.total_queued_files_count}"
                            )
                        except Exception as put_error:
                            logger.error(
                                f"[文件组预取器] [循环 #{prefetch_loop_count}] 放入队列失败: {put_error}",
                                exc_info=True
                            )
                    else:
                        # 没有收到文件，检查扫描状态（如果之前没有获取，这里获取）
                        if scan_status is None:
                            scan_status = await self.backup_db.get_scan_status(self.backup_task.id)
                        
                        if scan_status == 'completed':
                            # 扫描任务已完成，如果不够一组，先进行全库扫描避免遗漏
                            logger.info(
                                f"[文件组预取器] [循环 #{prefetch_loop_count}] 扫描任务已完成，"
                                f"但未检索到文件组，进行全库扫描检查是否有遗漏..."
                            )
                            
                            # 全库扫描：查询所有未压缩文件
                            from utils.scheduler.db_utils import get_opengauss_connection
                            try:
                                async with get_opengauss_connection() as conn:
                                    full_search_timeout = 7200.0  # 7200秒超时（两小时）
                                    await conn.execute(f"SET LOCAL statement_timeout = '{int(full_search_timeout)}s'")
                                    
                                    logger.info(
                                        f"[文件组预取器] [循环 #{prefetch_loop_count}] 开始全库检索所有未压缩文件"
                                        f"（超时时间：{full_search_timeout}秒）..."
                                    )
                                    # 多表方案：根据 backup_set_db_id 决定物理表名，避免直接访问基础表 backup_files
                                    from utils.scheduler.db_utils import get_backup_files_table_by_set_id
                                    table_name = await get_backup_files_table_by_set_id(conn, self.backup_set.id)

                                    all_pending_rows = await asyncio.wait_for(
                                        conn.fetch(
                                            f"""
                                            SELECT id, file_path, file_name, directory_path, display_name, file_type,
                                                   file_size, file_permissions, modified_time, accessed_time
                                            FROM {table_name}
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
                                        # 全库扫描也没有文件，再次调用 fetch_pending_files_grouped_by_size
                                        # 这次应该会返回文件组（即使大小不足，因为扫描已完成）
                                        logger.info(
                                            f"[文件组预取器] [循环 #{prefetch_loop_count}] 全库扫描确认没有遗漏文件，"
                                            f"再次调用 fetch_pending_files_grouped_by_size 获取文件组（即使大小不足）..."
                                        )
                                        # 再次查询，这次应该会返回文件组（因为扫描已完成）
                                        result = await self.backup_db.fetch_pending_files_grouped_by_size(
                                            self.backup_set.id,
                                            settings.MAX_FILE_SIZE,
                                            self.backup_task.id,
                                            should_wait_if_small=False,  # 不再等待
                                            start_from_id=self.last_processed_file_id
                                        )
                                        
                                        if isinstance(result, tuple) and len(result) == 2:
                                            file_groups, last_processed_id = result
                                        else:
                                            file_groups = result
                                            last_processed_id = self.last_processed_file_id
                                        
                                        if file_groups:
                                            # 有文件组，标记并放入队列
                                            total_files_in_groups = sum(len(group) for group in file_groups)
                                            total_size_in_groups = sum(
                                                sum(file_info.get('size', 0) for file_info in group)
                                                for group in file_groups
                                            )
                                            try:
                                                logger.info(
                                                    f"[文件组预取器] [循环 #{prefetch_loop_count}] 开始标记文件为已入队："
                                                    f"{len(file_groups)} 个文件组，共 {total_files_in_groups} 个文件，"
                                                    f"总大小={format_bytes(total_size_in_groups)}"
                                                )
                                                await self.backup_db.mark_files_as_queued(
                                                    backup_set=self.backup_set,
                                                    file_groups=file_groups
                                                )
                                                logger.info(
                                                    f"[文件组预取器] [循环 #{prefetch_loop_count}] ✅ 文件标记完成，"
                                                    f"is_copy_success 已设置为 TRUE"
                                                )
                                            except Exception as mark_error:
                                                logger.error(
                                                    f"[文件组预取器] [循环 #{prefetch_loop_count}] ⚠️ 标记文件失败: {str(mark_error)}",
                                                    exc_info=True
                                                )
                                            
                                            await self.file_group_queue.put((file_groups, last_processed_id))
                                            self.prefetched_groups += len(file_groups)
                                            self.last_processed_file_id = last_processed_id
                                            # 计算当前组大小
                                            total_size_in_groups = sum(
                                                sum(file_info.get('size', 0) for file_info in group)
                                                for group in file_groups
                                            )
                                            # 更新队列中的文件总数（放入队列时增加）
                                            self.queued_files_count += total_files_in_groups
                                            # 更新累计放入队列的总文件数（用于进度显示，不减少）
                                            self.total_queued_files_count += total_files_in_groups
                                            # 更新累计放入队列的总文件大小（用于进度显示，不减少）
                                            self.total_queued_size += total_size_in_groups
                                            logger.info(
                                                f"[文件组预取器] [循环 #{prefetch_loop_count}] 已预取 {len(file_groups)} 个文件组"
                                                f"（共 {total_files_in_groups} 个文件，当前组大小={format_bytes(total_size_in_groups)}，累计总大小={format_bytes(self.total_queued_size)}），"
                                                f"✅队列大小: {self.file_group_queue.qsize()}/{self.queue_maxsize}, "
                                                f"当前队列文件数: {self.queued_files_count}, "
                                                f"累计队列文件数: {self.total_queued_files_count}"
                                            )
                                            continue
                                        else:
                                            # 全库扫描后仍然没有文件组，停止线程
                                            logger.info(
                                                f"[文件组预取器] [循环 #{prefetch_loop_count}] 全库扫描后仍然没有文件组，"
                                                f"不够1组，停止预取线程"
                                            )
                                            # 更新任务状态，标记分组完成
                                            try:
                                                await self.backup_db.update_task_stage_with_description(
                                                    self.backup_task,
                                                    "prefetch",
                                                    "[分组完成] 所有文件已分组完成"
                                                )
                                                logger.info(f"[文件组预取器] ✅ 已更新任务状态为分组完成")
                                            except Exception as update_error:
                                                logger.warning(f"[文件组预取器] ⚠️ 更新任务状态失败: {str(update_error)}")
                                            await self.file_group_queue.put(([], -1))  # -1 表示结束
                                            break
                            except asyncio.TimeoutError:
                                logger.error(
                                    f"[文件组预取器] [循环 #{prefetch_loop_count}] 全库扫描超时，"
                                    f"停止预取线程"
                                )
                                await self.file_group_queue.put(([], -1))  # -1 表示结束
                                break
                            except Exception as full_search_error:
                                logger.error(
                                    f"[文件组预取器] [循环 #{prefetch_loop_count}] 全库扫描失败: {full_search_error}，"
                                    f"停止预取线程",
                                    exc_info=True
                                )
                                await self.file_group_queue.put(([], -1))  # -1 表示结束
                                break
                        else:
                            # 扫描任务未完成，文件大小不足无法组成新文件组，队列保持当前状态
                            # 6秒后再次检查队列大小和扫描状态
                            logger.info(
                                f"[文件组预取器] [循环 #{prefetch_loop_count}] 扫描任务未完成"
                                f"（扫描状态={scan_status}），文件大小不足无法组成新文件组，"
                                f"队列保持当前状态（{current_queue_size}/{self.queue_maxsize}），"
                                f"{queue_check_interval}秒后再次检查..."
                            )
                            await asyncio.sleep(queue_check_interval)
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
                f"队列剩余大小: {self.file_group_queue.qsize()}/{self.queue_maxsize}"
            )
    
    def get_stats(self) -> Dict[str, Any]:
        """获取统计信息"""
        return {
            'prefetched_groups': self.prefetched_groups,
            'total_retrieval_time': self.total_retrieval_time,
            'queue_size': self.file_group_queue.qsize(),
            'last_processed_file_id': self.last_processed_file_id
        }

