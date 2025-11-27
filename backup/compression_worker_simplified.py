#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
压缩工作线程（简化版）
"""

import asyncio
import logging
import os
from typing import List, Dict, Optional, Tuple
import tempfile

from utils.scheduler.db_utils import is_opengauss
from backup.compressor import Compressor

logger = logging.getLogger(__name__)


class CompressionWorker:
    """简化的压缩工作线程"""

    def __init__(
        self,
        backup_task,
        settings,
        backup_set,
        backup_db,
        file_move_worker=None,
        backup_notifier=None,
        tape_file_mover=None,
        file_group_prefetcher=None
    ):
        self.backup_task = backup_task
        self.settings = settings
        self.backup_set = backup_set
        self.backup_db = backup_db
        self.file_move_worker = file_move_worker
        self.backup_notifier = backup_notifier
        self.tape_file_mover = tape_file_mover
        self.file_group_prefetcher = file_group_prefetcher

        # 检查是否使用预取模式（openGauss模式且有预取器）
        self.use_prefetcher = is_opengauss() and file_group_prefetcher is not None
        logger.info(f"[压缩工作器] 初始化: use_prefetcher={self.use_prefetcher}")

        self.compression_task: Optional[asyncio.Task] = None
        self._running = False

        # 统计数据
        self.processed_files = 0

    def start(self):
        """启动压缩处理"""
        if self._running:
            return

        try:
            loop = asyncio.get_running_loop()
        except RuntimeError:
            logger.error("❌ 没有运行中的事件循环，无法创建任务")
            raise

        self.compression_task = asyncio.create_task(self._compression_loop())
        self._running = True
        logger.info("压缩处理任务已启动")

    async def stop(self):
        """停止压缩处理"""
        if not self._running:
            return

        logger.info("停止压缩处理...")
        self._running = False

        if self.compression_task:
            try:
                await self.compression_task
                logger.info("压缩处理任务已自然结束")
            except asyncio.CancelledError:
                logger.info("压缩处理任务被取消")
            except Exception as e:
                logger.error(f"压缩处理任务异常: {str(e)}", exc_info=True)

    async def _compression_loop(self):
        """简化的压缩处理"""
        logger.info("========== 压缩处理开始 ==========")

        if not self.backup_set.id:
            logger.error(f"⚠️⚠️ 错误：backup_set.id 为空！")
            return

        try:
            if self.use_prefetcher:
                await self._process_prefetched_file_groups()
            else:
                await self._process_database_file_groups()
        except Exception as e:
            logger.error(f"❌ 压缩处理异常: {str(e)}", exc_info=True)
            raise

    async def _process_prefetched_file_groups(self):
        """处理预取的文件组（openGauss模式）"""
        logger.info("开始处理预取的文件组")

        tasks = []
        group_idx = 0

        while self._running:
            try:
                result = await self.file_group_prefetcher.get_next_file_group(timeout=2.0)

                if result is None:
                    logger.debug("无更多文件组")
                    break

                if isinstance(result, tuple):
                    file_group, current_group_idx = result
                else:
                    file_group = result
                    current_group_idx = group_idx

                # 启动压缩任务
                task = asyncio.create_task(self._compress_file_group(file_group, current_group_idx))
                tasks.append(task)

                group_idx += 1

            except asyncio.TimeoutError:
                break
            except Exception as e:
                logger.error(f"处理文件组失败: {str(e)}", exc_info=True)
                break

        # 等待所有任务完成
        if tasks:
            logger.info(f"等待 {len(tasks)} 个压缩任务完成...")
            await asyncio.gather(*tasks, return_exceptions=True)
            logger.info("所有压缩任务已完成")

    async def _process_database_file_groups(self):
        """处理数据库中的文件组（非openGauss模式）"""
        logger.info("开始处理数据库中的文件组")

        group_idx = 0
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
                    last_processed_id = max(f.get('id', 0) for f in file_group)

                # 顺序处理文件组
                await self._compress_file_group(file_group, group_idx)

                group_idx += 1

            except Exception as e:
                logger.error(f"处理文件组失败: {str(e)}", exc_info=True)
                await asyncio.sleep(1)
                break

        logger.info("数据库文件组处理完成")

    async def _compress_file_group(self, file_group: List[Dict], group_idx: int):
        """压缩单个文件组"""
        if not file_group:
            logger.warning(f"[#{group_idx + 1}] 文件组为空，跳过")
            return

        try:
            total_files = len(file_group)
            total_size = sum(f.get('file_size', 0) for f in file_group)

            logger.info(f"[#{group_idx + 1}] 开始压缩 {total_files} 个文件，总大小 {self._format_bytes(total_size)}")

            # 更新进度
            await self.backup_db.update_task_stage_with_description(
                self.backup_task,
                "compress",
                f"[压缩文件中...] 0/{total_files} 个文件 (0.0%)"
            )

            # 获取压缩参数
            compression_method = getattr(self.settings, 'COMPRESSION_METHOD', 'zstd')
            compression_level = getattr(self.settings, 'COMPRESSION_LEVEL', 3)

            # 创建压缩器
            compressor = Compressor(
                method=compression_method,
                level=compression_level,
                max_file_size=getattr(self.settings, 'MAX_FILE_SIZE', 3 * 1024**3)
            )

            # 准备压缩目录
            with tempfile.TemporaryDirectory() as temp_dir:
                # 压缩文件组
                archive_path = await compressor.compress_files(file_group, temp_dir, self.backup_task.id, group_idx)

                # 如果有磁带文件移动器，移动到磁带
                if self.tape_file_mover and archive_path:
                    await self.tape_file_mover.move_to_tape(archive_path, self.backup_task.id)

            self.processed_files += total_files

            # 更新完成状态
            await self.backup_db.update_task_stage_with_description(
                self.backup_task,
                "compress",
                f"[压缩完成] {total_files}/{total_files} 个文件 (100.0%)"
            )

            logger.info(f"[#{group_idx + 1}] ✅ 压缩完成: {total_files} 个文件")

        except Exception as e:
            logger.error(f"[#{group_idx + 1}] ❌ 压缩失败: {str(e)}", exc_info=True)
            raise

    def _format_bytes(self, bytes_size: int) -> str:
        """格式化字节大小"""
        for unit in ['B', 'KB', 'MB', 'GB', 'TB']:
            if bytes_size < 1024:
                return f"{bytes_size:.1f} {unit}"
            bytes_size /= 1024
        return f"{bytes_size:.1f} PB"