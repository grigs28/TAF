#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
压缩循环工作线程
独立的后台线程，负责：检索文件 → 压缩 → 更新数据库 → 循环
"""

import asyncio
import logging
import os
from typing import Optional, Any, Dict, List
from pathlib import Path

from models.backup import BackupSet, BackupTask
from backup.compressor import Compressor
from backup.backup_db import BackupDB
from backup.backup_notifier import BackupNotifier
from backup.utils import format_bytes
from config.settings import get_settings

logger = logging.getLogger(__name__)


class CompressionWorker:
    """压缩循环工作线程管理器"""
    
    def __init__(
        self,
        backup_db: BackupDB,
        compressor: Compressor,
        backup_set: BackupSet,
        backup_task: BackupTask,
        settings: Any,
        file_move_worker: Any = None,  # FileMoveWorker
        backup_notifier: Optional[BackupNotifier] = None,
        tape_file_mover: Any = None  # TapeFileMover
    ):
        self.backup_db = backup_db
        self.compressor = compressor
        self.backup_set = backup_set
        self.backup_task = backup_task
        self.settings = settings
        self.file_move_worker = file_move_worker
        self.backup_notifier = backup_notifier
        self.tape_file_mover = tape_file_mover
        
        self.compression_task: Optional[asyncio.Task] = None
        self._running = False
        
        # 压缩循环统计
        self.processed_files = 0
        self.total_size = 0  # 压缩后的总大小
        self.total_original_size = 0  # 原始文件的总大小（未压缩）
        self.group_idx = 0
        
        self.idle_checks = 0
        self.max_idle_checks = 12  # 约1分钟
        self.wait_retry_count = 0
        self.max_wait_retries = 6  # 最多循环6次等待文件

    def start(self):
        """启动压缩循环后台任务"""
        if self._running:
            logger.warning("[压缩循环线程] 压缩循环任务已在运行")
            return
        
        self.compression_task = asyncio.create_task(self._compression_loop())
        self._running = True
        logger.info("[压缩循环线程] 压缩循环后台任务已启动")

    async def stop(self):
        """停止压缩循环后台任务"""
        if not self._running:
            return
        
        self._running = False
        if self.compression_task:
            self.compression_task.cancel()
            try:
                await self.compression_task
            except asyncio.CancelledError:
                pass
        logger.info("[压缩循环线程] 压缩循环后台任务已停止")

    async def _compression_loop(self):
        """独立的压缩循环后台任务
        
        流程顺序（严格按照以下顺序执行）：
        1. 检索所有非is_copy_success的文件，超阈值的跳过不修改is_copy，其他条件不变
        2. 压缩
        3. 修改数据库is_copy_success
        4. 循环到1
        
        注意：
        - fetch_pending_files_grouped_by_size 会自动跳过超阈值的文件，不修改is_copy_success
        - 压缩完成后立即更新is_copy_success，确保数据一致性
        - 所有参数、日志、错误处理等细节与backup_engine.py中的原始逻辑完全一致
        """
        logger.info("[压缩循环线程] ========== 压缩循环后台任务已启动 ==========")
        
        try:
            while self._running:
                try:
                    current_task = asyncio.current_task()
                    if current_task and current_task.cancelled():
                        logger.warning("压缩循环：检测到任务已被取消")
                        break
                except RuntimeError:
                    logger.warning("压缩循环：检测到任务可能已被取消")
                    break

                # ========== 步骤1：检索所有非is_copy_success的文件，超阈值的跳过不修改is_copy ==========
                # 新策略：调用数据库函数获取压缩组
                # 数据库函数内部已处理6次重试机制
                # fetch_pending_files_grouped_by_size 会自动：
                #   - 检索所有 is_copy_success = FALSE 的文件
                #   - 累积文件直到达到 max_file_size 阈值
                #   - 超过阈值的文件跳过（保持 FALSE 状态，下次仍可检索，不修改is_copy_success）
                logger.info(f"[压缩循环] [步骤1-检索文件] 开始检索下一批待压缩文件（文件组索引: {self.group_idx + 1}）...")
                file_groups = await self.backup_db.fetch_pending_files_grouped_by_size(
                    self.backup_set.id,
                    self.settings.MAX_FILE_SIZE,
                    self.backup_task.id,
                    should_wait_if_small=(self.wait_retry_count < self.max_wait_retries)
                )
                logger.info(f"[压缩循环] [步骤1-检索文件] 检索完成，返回文件组数量: {len(file_groups) if file_groups else 0}")
                
                if not file_groups:
                    # 新策略：返回空列表说明没有待压缩文件或需要等待
                    # 数据库函数已处理6次重试逻辑，这里只需检查最终状态
                    scan_status = await self.backup_db.get_scan_status(self.backup_task.id)
                    total_files_from_db = await self.backup_db.get_total_files_from_db(self.backup_task.id)

                    logger.info(
                        f"[新策略] 文件组为空，扫描状态={scan_status}, "
                        f"已处理={self.processed_files}, 总文件={total_files_from_db}"
                    )

                    if scan_status == 'completed' or self.processed_files >= total_files_from_db:
                        # 扫描完成或已处理所有文件，退出循环
                        logger.info("所有文件已压缩完毕，退出压缩循环")
                        break

                    # 否则等待更多文件
                    self.idle_checks += 1
                    if self.idle_checks >= self.max_idle_checks:
                        logger.warning(
                            f"等待压缩文件超时，扫描状态={scan_status}，"
                            f"已等待 {self.idle_checks * 5} 秒，继续等待..."
                        )

                    await asyncio.sleep(5)
                    continue

                # 成功获取到文件组，重置等待计数
                self.idle_checks = 0
                
                # file_groups 现在只包含一个文件组（每次调用只返回一个组）
                file_group = file_groups[0]
                total_files_in_group = len(file_group)
                total_size_in_group = sum(f.get('size', 0) or 0 for f in file_group)
                
                logger.info(
                    f"构建文件组完成：{total_files_in_group} 个文件，"
                    f"总大小 {format_bytes(total_size_in_group)}"
                )
                
                total_files_from_db = await self.backup_db.get_total_files_from_db(self.backup_task.id)
                if total_files_from_db > 0 and self.total_original_size > 0 and self.processed_files > 0:
                    avg_file_size = self.total_original_size / self.processed_files if self.processed_files > 0 else 0
                    if avg_file_size > 0:
                        files_per_archive = min(self.settings.MAX_FILE_SIZE / avg_file_size, total_files_from_db)
                        if files_per_archive > 0:
                            estimated_archive_count = max(self.group_idx + 1, int(total_files_from_db / files_per_archive))
                    else:
                        estimated_archive_count = max(self.group_idx + 1, int(total_files_from_db / 1000))
                elif total_files_from_db > 0:
                    estimated_archive_count = max(self.group_idx + 1, int(total_files_from_db / 1000))
                else:
                    estimated_archive_count = max(self.group_idx + 1, 1)
                
                # 构建压缩进度信息
                compression_status = "[压缩文件中...]"
                if hasattr(self.backup_task, 'current_compression_progress') and self.backup_task.current_compression_progress:
                    comp_prog = self.backup_task.current_compression_progress
                    compression_status = f"[压缩文件中...] {comp_prog['current']}/{comp_prog['total']} 个文件 ({comp_prog['percent']:.1f}%)"
                
                await self.backup_db.update_scan_progress(
                    self.backup_task,
                    self.processed_files,
                    self.processed_files + total_files_in_group,
                    compression_status
                )
                
                # 记录关键阶段：开始压缩（附带当前压缩参数）
                compression_method = getattr(self.settings, 'COMPRESSION_METHOD', 'pgzip')
                compression_level = getattr(self.settings, 'COMPRESSION_LEVEL', 9)
                compression_threads = getattr(self.settings, 'COMPRESSION_THREADS', 4)
                # PGZip 专用参数
                pgzip_block_size = getattr(self.settings, 'PGZIP_BLOCK_SIZE', '1G')
                pgzip_threads = getattr(self.settings, 'PGZIP_THREADS', compression_threads)
                try:
                    pgzip_threads = int(pgzip_threads)
                except (ValueError, TypeError):
                    pgzip_threads = int(compression_threads)
                
                # 7-Zip 命令行线程数（用于非 pgzip 场景）
                compression_command_threads = getattr(self.settings, 'COMPRESSION_COMMAND_THREADS', None)
                if compression_command_threads is None:
                    compression_command_threads = getattr(self.settings, 'WEB_WORKERS', compression_threads)
                try:
                    compression_command_threads = int(compression_command_threads)
                except (ValueError, TypeError):
                    compression_command_threads = int(compression_threads)
                
                zstd_threads = getattr(self.settings, 'ZSTD_THREADS', compression_threads)
                try:
                    zstd_threads = int(zstd_threads)
                except (ValueError, TypeError):
                    zstd_threads = int(compression_threads)
                
                if compression_method == 'pgzip':
                    compression_params = (
                        f"(压缩: 方法=pgzip, 等级={compression_level}, "
                        f"线程={pgzip_threads}, 块大小={pgzip_block_size})"
                    )
                elif compression_method == '7zip_command':
                    compression_params = (
                        f"(压缩: 方法=7zip_command, 等级={compression_level}, "
                        f"线程={compression_command_threads})"
                    )
                elif compression_method == 'tar':
                    compression_params = (
                        f"(压缩: 方法=tar, 不压缩，仅打包)"
                    )
                elif compression_method == 'zstd':
                    compression_params = (
                        f"(压缩: 方法=zstd, 等级={compression_level}, "
                        f"线程={zstd_threads})"
                    )
                else:
                    # py7zr 或其他方法，使用通用描述
                    compression_params = (
                        f"(压缩: 方法={compression_method}, 等级={compression_level}, "
                        f"线程={compression_threads})"
                    )
                
                self.backup_db._log_operation_stage_event(
                    self.backup_task,
                    f"[开始压缩] 文件组包含 {total_files_in_group} 个文件，总大小 {format_bytes(total_size_in_group)} {compression_params}"
                )
                # 处理这个文件组（只有一个组）
                current_group_idx = self.group_idx
                logger.info(f"处理文件组 {current_group_idx + 1}，包含 {total_files_in_group} 个文件")

                # 更新operation_stage和description
                await self.backup_db.update_task_stage_with_description(
                    self.backup_task,
                    "compress",
                    f"[开始压缩] 文件组 {current_group_idx + 1}：{total_files_in_group} 个文件，{format_bytes(total_size_in_group)}"
                )
                
                # 初始化压缩进度跟踪
                if not hasattr(self.backup_task, 'current_compression_progress'):
                    self.backup_task.current_compression_progress = None
                
                # 创建后台任务来定期更新压缩进度
                async def update_compression_progress_periodically():
                    """定期更新压缩进度到数据库"""
                    while True:
                        await asyncio.sleep(2)  # 每2秒更新一次
                        if hasattr(self.backup_task, 'current_compression_progress') and self.backup_task.current_compression_progress:
                            comp_prog = self.backup_task.current_compression_progress
                            compression_status = f"[压缩文件中...] {comp_prog['current']}/{comp_prog['total']} 个文件 ({comp_prog['percent']:.1f}%)"
                            try:
                                await self.backup_db.update_scan_progress(
                                    self.backup_task,
                                    self.processed_files,
                                    self.processed_files + total_files_in_group,
                                    compression_status
                                )
                            except Exception as e:
                                logger.debug(f"更新压缩进度失败（忽略继续）: {str(e)}")
                        else:
                            # 如果没有压缩进度信息，停止更新
                            break
                
                progress_update_task = asyncio.create_task(update_compression_progress_periodically())
                
                # ========== 步骤2：压缩文件组（顺序执行） ==========
                try:
                    compressed_file = await self.compressor.compress_file_group(
                        file_group,
                        self.backup_set,
                        self.backup_task,
                        base_processed_files=self.processed_files,
                        total_files=total_files_from_db
                    )
                    
                    # 压缩完成，取消进度更新任务
                    progress_update_task.cancel()
                    try:
                        await progress_update_task
                    except asyncio.CancelledError:
                        pass
                    if not compressed_file:
                        logger.warning(f"文件组 {current_group_idx + 1} 压缩失败，跳过该组")
                        continue

                    # 检查是否直接压缩到磁带
                    compress_directly_to_tape = getattr(self.settings, 'COMPRESS_DIRECTLY_TO_TAPE', True)
                    
                    logger.info(f"压缩完成，文件路径: {compressed_file['path']}, 直接压缩到磁带: {compress_directly_to_tape}")
                    
                    # 记录关键阶段：压缩完成
                    self.backup_db._log_operation_stage_event(
                        self.backup_task,
                        f"[压缩完成] 文件组 {current_group_idx + 1}，大小: {format_bytes(compressed_file['compressed_size'])}"
                    )
                    # 更新operation_stage和description
                    await self.backup_db.update_task_stage_with_description(
                        self.backup_task,
                        "compress",
                        f"[压缩完成] 文件组 {current_group_idx + 1}：{format_bytes(compressed_file['compressed_size'])}"
                    )
                    
                    if compress_directly_to_tape:
                        # 直接压缩到磁带，不需要移动队列，但仍需要更新状态
                        logger.info(f"文件已直接压缩到磁带机: {compressed_file['path']}")
                        tape_file_path = compressed_file['path']

                        # 记录关键阶段：开始写入磁带（直接压缩时）
                        self.backup_db._log_operation_stage_event(
                            self.backup_task,
                            f"[写入磁带中...] 文件组 {current_group_idx + 1}（直接压缩）"
                        )
                        # 更新operation_stage和description
                        await self.backup_db.update_task_stage_with_description(
                            self.backup_task,
                            "copy",
                            f"[写入磁带中] 文件组 {current_group_idx + 1}：{os.path.basename(compressed_file['path'])}"
                        )

                        # 记录关键阶段：文件已写入磁带机（直接压缩时）
                        self.backup_db._log_operation_stage_event(
                            self.backup_task,
                            f"[文件已移动到磁带机] {os.path.basename(compressed_file['path'])} (文件组 {current_group_idx + 1}，直接压缩)"
                        )
                        # 更新为完成状态
                        await self.backup_db.update_task_stage_with_description(
                            self.backup_task,
                            "finalize",
                            f"[写入完成] 文件组 {current_group_idx + 1}：{os.path.basename(compressed_file['path'])}"
                        )
                    else:
                        # 将文件加入移动队列，由后台线程顺序移动到磁带机
                        # 定义回调函数，在移动完成后记录日志（数据库已在保存时记录）
                        def move_callback(source_path: str, tape_file_path: Optional[str], success: bool, error: Optional[str]):
                            """文件移动完成后的回调函数"""
                            if success and tape_file_path:
                                logger.info(f"文件已成功移动到磁带机: {tape_file_path}")
                                # 记录关键阶段：文件已移动到磁带机（统一格式）
                                self.backup_db._log_operation_stage_event(
                                    self.backup_task,
                                    f"[文件已移动到磁带机] {os.path.basename(tape_file_path)} (文件组 {current_group_idx + 1})"
                                )
                            elif not success:
                                logger.error(f"文件移动到磁带机失败: {source_path}, 错误: {error}")
                                # 记录关键阶段：移动失败
                                self.backup_db._log_operation_stage_event(
                                    self.backup_task,
                                    f"[移动到磁带机失败] 文件组 {current_group_idx + 1}，错误: {error}"
                                )
                        
                        # 将文件加入移动队列（后台任务，不阻塞压缩循环）
                        if self.file_move_worker:
                            temp_archive_path = compressed_file.get('temp_path')
                            final_archive_path = compressed_file.get('final_path')
                            
                            # 将文件加入文件移动worker队列（非阻塞）
                            await self.file_move_worker.add_file_move_task(
                                temp_path=temp_archive_path,
                                final_path=final_archive_path,
                                backup_set=self.backup_set,
                                chunk_number=current_group_idx,
                                callback=move_callback,
                                backup_task=self.backup_task
                            )
                            logger.info(f"[文件移动] 文件移动任务已提交到队列，压缩循环继续执行")
                            
                            # 暂时使用源路径作为磁带路径（移动完成后会更新）
                            tape_file_path = compressed_file['path']
                        else:
                            logger.error("文件移动工作线程未初始化，无法将文件加入队列！")
                            tape_file_path = compressed_file['path']

                    # ========== 步骤3：修改数据库is_copy_success（顺序执行，必须在压缩完成后立即执行） ==========
                    # 重要：压缩完成后立即更新数据库（is_copy_success = TRUE），顺序执行
                    # 必须等待数据库更新完成后再继续下一组压缩，确保数据一致性
                    # 只有成功压缩的文件才会更新is_copy_success，超阈值跳过的文件保持FALSE状态
                    try:
                        logger.info(f"[压缩循环] [步骤3-数据库更新] 开始更新文件复制状态：文件组 {current_group_idx + 1}，包含 {len(file_group)} 个文件")
                        await self.backup_db.mark_files_as_copied(
                            backup_set=self.backup_set,
                            file_group=file_group,
                            compressed_file=compressed_file,
                            tape_file_path=tape_file_path or compressed_file['path'],
                            chunk_number=current_group_idx
                        )
                        logger.info(f"[压缩循环] [步骤3-数据库更新] ✅ 文件复制状态更新成功：文件组 {current_group_idx + 1}，{len(file_group)} 个文件的 is_copy_success 已设置为 TRUE")
                    except Exception as db_error:
                        logger.error(f"⚠️ [数据库更新] 更新文件复制状态失败: {str(db_error)}，继续执行", exc_info=True)
                        # 即使更新失败，也继续执行，避免阻塞整个流程

                    # ========== 更新进度统计（必须在数据库更新之前） ==========
                    self.processed_files += len(file_group)
                    self.total_size += compressed_file['compressed_size']  # 压缩后的总大小
                    self.total_original_size += compressed_file['original_size']  # 原始文件的总大小
                    
                    # 立即更新 backup_task 对象的值，确保后续数据库更新能读取到正确的值
                    self.backup_task.processed_files = self.processed_files
                    self.backup_task.processed_bytes = self.total_original_size  # 原始文件的总大小（未压缩）
                    self.backup_task.compressed_bytes = self.total_size  # 压缩后的总大小
                    
                    logger.debug(f"[进度统计] 文件组 {current_group_idx + 1} 处理完成：processed_files={self.processed_files}, processed_bytes={self.total_original_size}, compressed_bytes={self.total_size}")
                    
                    # 注意：不再更新 total_files 字段（压缩包数量）
                    # 压缩包数量存储在 result_summary.estimated_archive_count 中
                    # total_files 字段由后台扫描任务更新（总文件数）
                    
                    # 从数据库读取总文件数（由后台扫描任务更新）
                    total_files_from_db = await self.backup_db.get_total_files_from_db(self.backup_task.id)
                    
                    # 重新估算预计的压缩包总数（基于已处理的文件数和平均文件大小）
                    if self.processed_files > 0 and self.total_original_size > 0 and total_files_from_db > 0:
                        avg_file_size = self.total_original_size / self.processed_files
                        if avg_file_size > 0:
                            # 计算每个压缩包能容纳的文件数（基于MAX_FILE_SIZE）
                            files_per_archive = min(self.settings.MAX_FILE_SIZE / avg_file_size, total_files_from_db)
                            if files_per_archive > 0:
                                # 基于总扫描文件数估算压缩包总数
                                estimated_archive_count = max(current_group_idx + 1, int(total_files_from_db / files_per_archive))
                            else:
                                # 如果文件很大，每个压缩包只能容纳很少文件，使用保守估算
                                estimated_archive_count = max(current_group_idx + 1, int(total_files_from_db / 100))
                        else:
                            # 无法计算平均文件大小，使用保守估算
                            estimated_archive_count = max(current_group_idx + 1, int(total_files_from_db / 1000))
                    elif total_files_from_db > 0:
                        # 如果还没有处理文件，但已扫描了文件，使用保守估算
                        estimated_archive_count = max(current_group_idx + 1, int(total_files_from_db / 1000))
                    else:
                        # 如果还没有扫描文件，使用已生成的压缩包数
                        estimated_archive_count = max(current_group_idx + 1, 1)
                    
                    logger.debug(f"预计压缩包总数更新: {estimated_archive_count} (已生成: {current_group_idx + 1}, 总扫描文件: {total_files_from_db}, 已处理文件: {self.processed_files})")
                    
                    # 将预计的压缩包总数存储到 result_summary（JSON字段）
                    if not hasattr(self.backup_task, 'result_summary') or self.backup_task.result_summary is None:
                        self.backup_task.result_summary = {}
                    if isinstance(self.backup_task.result_summary, dict):
                        self.backup_task.result_summary['estimated_archive_count'] = estimated_archive_count
                    else:
                        import json
                        self.backup_task.result_summary = {'estimated_archive_count': estimated_archive_count}
                    
                    # 更新进度百分比
                    # 进度百分比基于：已处理文件数 / 总扫描文件数（从数据库读取）
                    # 扫描阶段占10%，压缩阶段占90%，当文件处理完成时进度为100%
                    # 注意：使用之前读取的 total_files_from_db（避免重复读取）
                    if total_files_from_db > 0:
                        # 基于已处理文件数和总扫描文件数计算进度
                        file_progress_ratio = self.processed_files / total_files_from_db
                        # 扫描阶段占10%，压缩阶段占90%
                        self.backup_task.progress_percent = min(100.0, 10.0 + (file_progress_ratio * 90.0))
                    elif self.processed_files > 0:
                        # 如果还没有扫描完，但已处理了一些文件，使用估算进度
                        # 估算：假设已处理的文件占总文件的很小一部分
                        self.backup_task.progress_percent = min(95.0, 10.0 + (self.processed_files / max(self.processed_files * 100, 1)) * 85.0)
                    else:
                        # 还没有处理任何文件，进度为10%（扫描阶段）
                        self.backup_task.progress_percent = 10.0
                    
                    # ========== 重要：压缩完成后立即更新数据库状态（包括 processed_files 和 processed_bytes），顺序执行 ==========
                    # 必须等待数据库更新完成后再继续下一组压缩，确保数据一致性
                    # 注意：backup_task 对象的值已在上面设置，这里直接使用
                    logger.info(f"[数据库更新] 压缩完成，更新数据库状态：已处理 {self.processed_files} 个文件，{format_bytes(self.total_original_size)} 原始数据，{format_bytes(self.total_size)} 压缩后数据")
                    logger.info(f"[数据库更新] backup_task 对象值验证：processed_files={self.backup_task.processed_files}, processed_bytes={self.backup_task.processed_bytes}, compressed_bytes={self.backup_task.compressed_bytes}")
                    
                    await self.backup_db.update_scan_progress(
                        self.backup_task, 
                        self.processed_files, 
                        self.backup_task.total_files, 
                        f"[压缩完成] 文件组 {current_group_idx + 1}：{format_bytes(compressed_file['compressed_size'])}"
                    )
                    logger.info(f"[数据库更新] 数据库状态更新完成：processed_files={self.processed_files}, processed_bytes={self.total_original_size}, compressed_bytes={self.total_size}")
                    
                    # 通知进度更新
                    await self.backup_notifier.notify_progress(self.backup_task)
                    
                    logger.info(f"[压缩循环] 文件组 {current_group_idx + 1} 处理完成，数据库已更新，准备继续下一组压缩")
                
                except Exception as group_error:
                    # 文件组处理失败，记录错误但继续处理下一个文件组
                    logger.error(f"⚠️ 处理文件组 {current_group_idx + 1} 时发生错误: {str(group_error)}，跳过该文件组，继续处理下一个文件组")
                    import traceback
                    logger.error(f"错误堆栈:\n{traceback.format_exc()}")
                
                # ========== 步骤4：循环到步骤1（更新文件组索引，准备下一轮检索） ==========
                # 更新文件组索引（每次只处理一个文件组）
                self.group_idx += 1
                if hasattr(self.backup_task, 'result_summary') and isinstance(self.backup_task.result_summary, dict):
                    estimated_count = self.backup_task.result_summary.get('estimated_archive_count', 'N/A')
                    logger.info(f"文件组处理完成，estimated_archive_count={estimated_count}")
                
                logger.info(f"[压缩循环] [步骤4-循环] 准备更新扫描进度，状态: [等待下一批文件...]")
                await self.backup_db.update_scan_progress(
                    self.backup_task,
                    self.processed_files,
                    self.processed_files,
                    "[等待下一批文件...]"
                )
                logger.info(f"[压缩循环] [步骤4-循环] 扫描进度已更新，准备继续循环检索下一批文件（回到步骤1）...")
            
        except (KeyboardInterrupt, asyncio.CancelledError):
            logger.warning("========== 压缩循环被中止 ==========")
            logger.warning(f"任务ID: {self.backup_task.id if self.backup_task else 'N/A'}")
            logger.warning(f"已处理文件组: {self.group_idx}")
            raise
        except Exception as e:
            logger.error(f"[压缩循环线程] 压缩循环任务异常: {str(e)}", exc_info=True)
        finally:
            logger.info("[压缩循环线程] 压缩循环后台任务已退出")
