#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
压缩工作线程
"""

import asyncio
import logging
import os
from typing import List, Dict, Optional
import tempfile

from utils.scheduler.db_utils import is_opengauss
from config.settings import get_settings
from backup.utils import format_bytes

logger = logging.getLogger(__name__)


class CompressionWorker:
    """压缩工作线程"""

    def __init__(
        self,
        backup_db,
        compressor,
        backup_set,
        backup_task,
        settings,
        file_move_worker=None,
        backup_notifier=None,
        tape_file_mover=None,
        file_group_prefetcher=None
    ):
        self.backup_task = backup_task
        self.settings = settings
        self.backup_set = backup_set
        self.backup_db = backup_db
        self.compressor = compressor
        self.file_move_worker = file_move_worker
        self.backup_notifier = backup_notifier
        self.tape_file_mover = tape_file_mover
        self.file_group_prefetcher = file_group_prefetcher

        # 检查是否使用预取模式（openGauss模式且有预取器）
        self.use_prefetcher = is_opengauss() and file_group_prefetcher is not None
        logger.info(f"[压缩循环] 初始化: is_opengauss={is_opengauss()}, file_group_prefetcher={file_group_prefetcher is not None}, use_prefetcher={self.use_prefetcher}")

        self.compression_task: Optional[asyncio.Task] = None
        self._running = False

        # 压缩循环统计
        self.processed_files = 0
        self.total_size = 0  # 压缩后的总大小
        self.total_original_size = 0  # 原始文件的总大小（未压缩）
        self.group_idx = 0  # 文件组索引

        # 并行批次设置
        if self.use_prefetcher:
            settings = get_settings()
            self.parallel_batches = getattr(settings, 'COMPRESSION_PARALLEL_BATCHES', 2)
        else:
            self.parallel_batches = 1  # 非openGauss模式，顺序执行
        
        # 并发控制：跟踪正在运行的压缩任务
        self.running_compression_futures: List[asyncio.Task] = []
        
        # 压缩进度更新任务（定期从数据库查询聚合进度）
        self.progress_update_task: Optional[asyncio.Task] = None

    def start(self):
        """启动压缩处理任务"""
        if self._running:
            return

        try:
            # 检查是否有运行中的事件循环
            try:
                loop = asyncio.get_running_loop()
            except RuntimeError:
                logger.error("❌ 没有运行中的事件循环，无法创建任务")
                raise

            self.compression_task = asyncio.create_task(self._compression_loop())
            self._running = True
            logger.info("压缩处理任务已启动")
        except Exception as e:
            logger.error(f"❌ 启动压缩处理任务失败: {str(e)}", exc_info=True)
            raise

    async def stop(self):
        """停止压缩处理任务"""
        if not self._running:
            return

        logger.info("收到停止信号，正在停止压缩处理...")
        self._running = False

        if self.compression_task:
            try:
                await self.compression_task
                logger.info("压缩处理任务已自然结束")
            except asyncio.CancelledError:
                logger.info("压缩处理任务被取消")
            except KeyboardInterrupt:
                logger.warning("压缩处理收到KeyboardInterrupt")
                raise
            except Exception as e:
                logger.error(f"压缩处理任务异常: {str(e)}", exc_info=True)

        logger.info("压缩处理任务已停止")

    async def _compression_loop(self):
        """简化的压缩处理"""
        logger.info("========== 压缩处理开始 ==========")

        if not self.backup_set.id:
            logger.error(f"⚠️⚠️ 错误：backup_set.id 为空！")
            return

        try:
            # 启动压缩进度更新任务（定期从数据库查询聚合进度）
            self.progress_update_task = asyncio.create_task(
                self._update_compression_progress_periodically()
            )
            
            if self.use_prefetcher:
                await self._process_prefetched_file_groups()
            else:
                await self._process_database_file_groups()
        except Exception as e:
            logger.error(f"❌ 压缩处理异常: {str(e)}", exc_info=True)
            raise
        finally:
            # 停止进度更新任务
            if self.progress_update_task:
                self.progress_update_task.cancel()
                try:
                    await self.progress_update_task
                except asyncio.CancelledError:
                    pass
                logger.info("压缩进度更新任务已停止")

    async def _process_prefetched_file_groups(self):
        """处理预取的文件组（openGauss模式）- 并发控制"""
        logger.info(f"开始处理预取的文件组（并发控制模式，parallel_batches={self.parallel_batches}）")

        while self._running:
            try:
                # 并发控制：如果达到并行限制，等待部分任务完成
                if len(self.running_compression_futures) >= self.parallel_batches:
                    logger.debug(f"达到并行限制 ({self.parallel_batches})，等待部分任务完成...")
                    # 等待至少一个任务完成
                    done, pending = await asyncio.wait(
                        self.running_compression_futures,
                        return_when=asyncio.FIRST_COMPLETED
                    )
                    # 移除已完成的任务并处理异常
                    for task in done:
                        if task in self.running_compression_futures:
                            self.running_compression_futures.remove(task)
                        try:
                            await task  # 获取任务结果（如果有异常会抛出）
                        except Exception as e:
                            logger.error(f"压缩任务异常: {str(e)}", exc_info=True)
                    logger.debug(f"当前运行中的压缩任务数: {len(self.running_compression_futures)}/{self.parallel_batches}")

                # 获取文件组（消费队列）
                result = await self.file_group_prefetcher.get_file_group(timeout=2.0)

                if result is None:
                    # 检查扫描状态，如果扫描未完成，继续等待
                    scan_status = await self.backup_db.get_scan_status(self.backup_task.id)
                    if scan_status != 'completed':
                        logger.info(f"无更多文件组，但扫描未完成（状态={scan_status}），继续等待...")
                        # 如果有正在运行的任务，等待它们完成
                        if self.running_compression_futures:
                            logger.info(f"等待 {len(self.running_compression_futures)} 个正在运行的压缩任务完成...")
                            done, _ = await asyncio.wait(self.running_compression_futures)
                            # 处理所有已完成的任务（包括异常）
                            for task in done:
                                try:
                                    await task
                                except Exception as e:
                                    logger.error(f"压缩任务异常: {str(e)}", exc_info=True)
                            self.running_compression_futures.clear()
                        await asyncio.sleep(2)
                        continue
                    else:
                        # 扫描已完成，等待所有正在运行的任务完成
                        if self.running_compression_futures:
                            logger.info(f"扫描已完成，等待 {len(self.running_compression_futures)} 个正在运行的压缩任务完成...")
                            done, _ = await asyncio.wait(self.running_compression_futures)
                            # 处理所有已完成的任务（包括异常）
                            for task in done:
                                try:
                                    await task
                                except Exception as e:
                                    logger.error(f"压缩任务异常: {str(e)}", exc_info=True)
                            self.running_compression_futures.clear()
                        logger.info("所有文件组处理完成，退出压缩循环")
                        break

                if isinstance(result, tuple) and len(result) == 2:
                    # 预取器返回格式：(file_groups, last_processed_id)
                    # file_groups 是一个列表，通常是 [[file1, file2, ..., fileN]]
                    file_groups, last_processed_id = result
                    # 检查是否是结束信号（空文件组且 last_processed_id == -1）
                    if not file_groups and last_processed_id == -1:
                        # 等待所有正在运行的任务完成
                        if self.running_compression_futures:
                            logger.info(f"收到结束信号，等待 {len(self.running_compression_futures)} 个正在运行的压缩任务完成...")
                            done, _ = await asyncio.wait(self.running_compression_futures)
                            # 处理所有已完成的任务（包括异常）
                            for task in done:
                                try:
                                    await task
                                except Exception as e:
                                    logger.error(f"压缩任务异常: {str(e)}", exc_info=True)
                            self.running_compression_futures.clear()
                        logger.info("收到预取器结束信号，退出压缩循环")
                        break
                    
                    # 取第一个文件组（通常只有一个文件组）
                    if file_groups and len(file_groups) > 0:
                        file_group = file_groups[0]
                        logger.info(f"[压缩循环] 从预取器获取文件组：{len(file_groups)} 个文件组，第一个文件组包含 {len(file_group)} 个文件")
                    else:
                        logger.warning("预取器返回的文件组列表为空，跳过")
                        continue
                    current_group_idx = self.group_idx
                else:
                    # 兼容旧格式（非元组）
                    logger.warning(f"收到非标准格式的文件组：{type(result)}，尝试直接使用")
                    file_group = result if isinstance(result, list) else []
                    current_group_idx = self.group_idx

                # 启动压缩任务（并发执行，但每个任务内部顺序执行：压缩 → 标注 → 移动）
                compression_task = asyncio.create_task(
                    self._compress_file_group(file_group, current_group_idx)
                )
                self.running_compression_futures.append(compression_task)
                logger.info(f"[压缩循环] 启动压缩任务 #{current_group_idx + 1}，当前运行中的任务数: {len(self.running_compression_futures)}/{self.parallel_batches}")

                self.group_idx += 1
                
                # 顺序启动：启动一个任务后，等待2秒，再启动下一个（启动后的任务并行执行）
                await asyncio.sleep(2.0)
                # 继续循环，获取下一个文件组（不等待当前任务完成）

            except asyncio.TimeoutError:
                # 超时时检查扫描状态，如果扫描未完成，继续等待
                scan_status = await self.backup_db.get_scan_status(self.backup_task.id)
                if scan_status != 'completed':
                    logger.info(f"获取文件组超时，但扫描未完成（状态={scan_status}），继续等待...")
                    # 如果有正在运行的任务，等待它们完成
                    if self.running_compression_futures:
                        logger.info(f"等待 {len(self.running_compression_futures)} 个正在运行的压缩任务完成...")
                        await asyncio.wait(self.running_compression_futures)
                        self.running_compression_futures.clear()
                    await asyncio.sleep(2)
                    continue
                else:
                    # 扫描已完成，等待所有正在运行的任务完成
                    if self.running_compression_futures:
                        logger.info(f"扫描已完成，等待 {len(self.running_compression_futures)} 个正在运行的压缩任务完成...")
                        await asyncio.wait(self.running_compression_futures)
                        self.running_compression_futures.clear()
                    logger.info("获取文件组超时，扫描已完成，退出压缩循环")
                    break
            except Exception as e:
                logger.error(f"处理文件组失败: {str(e)}", exc_info=True)
                # 发生异常时也检查扫描状态
                scan_status = await self.backup_db.get_scan_status(self.backup_task.id)
                if scan_status != 'completed':
                    logger.info(f"处理文件组异常，但扫描未完成（状态={scan_status}），继续等待...")
                    # 如果有正在运行的任务，等待它们完成
                    if self.running_compression_futures:
                        logger.info(f"等待 {len(self.running_compression_futures)} 个正在运行的压缩任务完成...")
                        await asyncio.wait(self.running_compression_futures)
                        self.running_compression_futures.clear()
                    await asyncio.sleep(2)
                    continue
                else:
                    # 扫描已完成，等待所有正在运行的任务完成
                    if self.running_compression_futures:
                        logger.info(f"扫描已完成，等待 {len(self.running_compression_futures)} 个正在运行的压缩任务完成...")
                        await asyncio.wait(self.running_compression_futures)
                        self.running_compression_futures.clear()
                    logger.error("处理文件组异常，扫描已完成，退出压缩循环")
                    break
        
        # 确保所有任务都已完成
        if self.running_compression_futures:
            logger.info(f"等待剩余的 {len(self.running_compression_futures)} 个压缩任务完成...")
            done, _ = await asyncio.wait(self.running_compression_futures)
            # 处理所有已完成的任务（包括异常）
            for task in done:
                try:
                    await task
                except Exception as e:
                    logger.error(f"压缩任务异常: {str(e)}", exc_info=True)
            self.running_compression_futures.clear()
        logger.info("所有压缩任务已完成")

    async def _update_compression_progress_periodically(self):
        """定期更新压缩进度（从数据库查询聚合进度，支持多进程）
        
        每5秒查询一次数据库，获取所有已压缩文件数（is_copy_success = TRUE），
        更新 description 字段，显示聚合进度。
        """
        try:
            # 等待压缩开始（避免在压缩开始前频繁查询）
            await asyncio.sleep(5.0)
            
            # 获取总文件数（从 backup_task 获取）
            total_files = getattr(self.backup_task, 'total_files', 0) or 0
            if total_files == 0:
                logger.debug("[压缩进度更新] 总文件数为0，跳过进度更新")
                return
            
            update_interval = 5.0  # 每5秒更新一次
            last_update_time = 0.0
            
            while self._running:
                try:
                    current_time = asyncio.get_event_loop().time()
                    
                    # 检查是否需要更新（每5秒更新一次）
                    if current_time - last_update_time >= update_interval:
                        # 从数据库查询已压缩文件数（聚合所有进程的进度）
                        compressed_count = await self.backup_db.get_compressed_files_count(
                            self.backup_set.id
                        )
                        
                        # 计算进度百分比
                        if total_files > 0:
                            progress_percent = (compressed_count / total_files) * 100.0
                            progress_percent = min(100.0, max(0.0, progress_percent))
                            
                            # 更新 description 字段
                            description = f"[压缩文件中...] {compressed_count}/{total_files} 个文件 ({progress_percent:.1f}%)"
                            await self.backup_db.update_task_stage_with_description(
                                self.backup_task,
                                "compress",
                                description
                            )
                            
                            logger.debug(
                                f"[压缩进度更新] 已压缩: {compressed_count}/{total_files} "
                                f"({progress_percent:.1f}%)"
                            )
                        
                        last_update_time = current_time
                    
                    # 等待1秒后再次检查
                    await asyncio.sleep(1.0)
                    
                except asyncio.CancelledError:
                    logger.info("[压缩进度更新] 收到取消信号，停止进度更新")
                    break
                except Exception as e:
                    logger.warning(f"[压缩进度更新] 更新进度失败: {str(e)}", exc_info=True)
                    # 出错后等待更长时间再重试
                    await asyncio.sleep(5.0)
                    
        except asyncio.CancelledError:
            logger.info("[压缩进度更新] 进度更新任务被取消")
        except Exception as e:
            logger.error(f"[压缩进度更新] 进度更新任务异常: {str(e)}", exc_info=True)

    async def _process_database_file_groups(self):
        """处理数据库中的文件组（非openGauss模式）"""
        logger.info("开始处理数据库中的文件组")

        last_processed_id = 0

        while self._running:
            try:
                # 从数据库获取文件组
                file_groups = await self.backup_db.get_next_compression_group(
                    backup_set_id=self.backup_set.id,
                    batch_size=1,
                    last_processed_id=last_processed_id
                )

                if not file_groups:
                    scan_status = await self.backup_db.get_scan_status(self.backup_task.id)
                    if scan_status == 'completed':
                        logger.info("所有文件组处理完成")
                        break
                    logger.debug("无文件组，等待...")
                    await asyncio.sleep(2)
                    continue

                file_group = file_groups[0]

                # 获取最后一个文件的ID作为下次查询的起点
                if file_group:
                    last_processed_id = 0
                    for f in file_group:
                        if isinstance(f, dict):
                            last_processed_id = max(last_processed_id, f.get('id', 0))
                        elif isinstance(f, list) and len(f) > 0:
                            # 处理不同的列表格式
                            if isinstance(f[0], (int, float)):
                                # 标准格式：[id, file_path, relative_path, file_size, ...]
                                last_processed_id = max(last_processed_id, int(f[0]))
                            elif len(f) >= 8 and isinstance(f[7], dict) and 'id' in f[7]:
                                # 可能的嵌套格式：[..., ..., ..., ..., ..., ..., ..., {'id': id, ...}]
                                last_processed_id = max(last_processed_id, int(f[7]['id']))
                            else:
                                logger.warning(f"[数据库模式] 无法从列表格式获取文件ID: {f}")
                        else:
                            logger.warning(f"[数据库模式] 未知文件格式: {f}")

                # 顺序处理文件组
                await self._compress_file_group(file_group, self.group_idx)

                self.group_idx += 1

            except Exception as e:
                logger.error(f"处理文件组失败: {str(e)}", exc_info=True)
                # 发生异常时检查扫描状态，如果扫描未完成，继续等待
                scan_status = await self.backup_db.get_scan_status(self.backup_task.id)
                if scan_status != 'completed':
                    logger.info(f"处理文件组异常，但扫描未完成（状态={scan_status}），继续等待...")
                    await asyncio.sleep(2)
                    continue
                else:
                    logger.error("处理文件组异常，扫描已完成，退出压缩循环")
                    break

        logger.info("数据库文件组处理完成")

    async def _compress_file_group(self, file_group: List[Dict], group_idx: int):
        """压缩单个文件组（内部顺序执行：压缩完成 → 标注完成 → 移动到final → 返回，队列已消费）"""
        if not file_group:
            logger.warning(f"[#{group_idx + 1}] 文件组为空，跳过")
            return

        try:
            total_files = len(file_group)

            # 处理文件数据格式并转换为压缩器期望的格式
            total_size = 0
            processed_file_group = []

            for f in file_group:
                file_dict = None

                if isinstance(f, dict):
                    # 字典格式 - 直接使用
                    file_dict = f
                    total_size += file_dict.get('file_size', 0)
                elif isinstance(f, list):
                    # 检查是否是嵌套字典格式（最后一个元素是字典）
                    if f and isinstance(f[-1], dict):
                        # 嵌套字典格式：[..., {...dict...}]
                        file_dict = f[-1]
                        total_size += file_dict.get('size', 0)
                    elif len(f) >= 4 and isinstance(f[3], (int, float)):
                        # 标准格式：[id, file_path, relative_path, file_size, ...]
                        # 转换为字典格式
                        file_dict = {
                            'id': f[0] if len(f) > 0 else 0,
                            'file_path': f[1] if len(f) > 1 else '',
                            'relative_path': f[2] if len(f) > 2 else '',
                            'file_size': f[3] if len(f) > 3 else 0,
                            'modified_time': f[4] if len(f) > 4 else None,
                            'md5': f[5] if len(f) > 5 else '',
                            'is_copy_success': f[6] if len(f) > 6 else False,
                            'stage': f[7] if len(f) > 7 else 'pending'
                        }
                        total_size += int(f[3])
                    else:
                        logger.warning(f"[#{group_idx + 1}] 无法从列表格式获取文件大小: {f}")
                        continue
                else:
                    logger.warning(f"[#{group_idx + 1}] 未知文件格式: {f}")
                    continue

                processed_file_group.append(file_dict)

            logger.info(f"[#{group_idx + 1}] 开始压缩 {total_files} 个文件，总大小 {format_bytes(total_size)}")

            # 更新进度
            await self.backup_db.update_task_stage_with_description(
                self.backup_task,
                "compress",
                f"[压缩文件中...] 0/{total_files} 个文件 (0.0%)"
            )

            # 准备压缩目录
            with tempfile.TemporaryDirectory() as temp_dir:
                # 压缩文件组（顺序执行：等待压缩完成、标注完成、移动到final后才返回）
                compressed_info = await self.compressor.compress_file_group(
                    processed_file_group, 
                    self.backup_set, 
                    self.backup_task, 
                    self.processed_files, 
                    total_files
                )
                
                # 压缩完成后，更新统计
                if compressed_info:
                    self.processed_files += total_files
                    logger.info(f"[#{group_idx + 1}] ✅ 压缩完成并已移动到final: {total_files} 个文件")
                
                archive_path = compressed_info.get('path') if compressed_info else None

                # 注意：文件移动到磁带由FinalDirMonitor独立线程监控final目录处理

        except Exception as e:
            logger.error(f"[#{group_idx + 1}] ❌ 压缩失败: {str(e)}", exc_info=True)
            raise