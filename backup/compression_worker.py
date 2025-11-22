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
        
        # 关键修复：维护最后处理的文件ID，避免重复查询相同的文件
        self.last_processed_file_id = 0  # 上次处理的最后一个文件ID

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
        
        logger.info("[压缩循环线程] 收到停止信号，正在停止压缩循环...")
        self._running = False
        if self.compression_task:
            # 取消任务
            self.compression_task.cancel()
            try:
                await self.compression_task
            except asyncio.CancelledError:
                logger.info("[压缩循环线程] 压缩循环任务已取消")
            except KeyboardInterrupt:
                logger.warning("[压缩循环线程] 压缩循环收到KeyboardInterrupt")
                raise
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
        # 使用全局settings统一读取配置
        settings = get_settings()
        # 使用全局settings统一读取配置（不缓存，总是获取最新的）
        settings = get_settings()
        logger.info(
            f"[压缩循环线程] 初始化参数: backup_set_id={self.backup_set.id}, "
            f"backup_set.set_id={getattr(self.backup_set, 'set_id', 'N/A')}, "
            f"backup_task_id={self.backup_task.id}, max_file_size={settings.MAX_FILE_SIZE}"
        )
        
        # 验证 backup_set.id 是否正确
        if not self.backup_set.id:
            logger.error(f"[压缩循环线程] ⚠️⚠️ 错误：backup_set.id 为空！backup_set={self.backup_set}")
        
        try:
            loop_iteration = 0
            while self._running:
                loop_iteration += 1
                logger.debug(f"[压缩循环] 开始第 {loop_iteration} 次循环迭代")
                
                # 检查是否被取消
                try:
                    current_task = asyncio.current_task()
                    if current_task and current_task.cancelled():
                        logger.warning("压缩循环：检测到任务已被取消")
                        break
                except RuntimeError:
                    logger.warning("压缩循环：检测到任务可能已被取消")
                    break
                
                # 检查运行标志
                if not self._running:
                    logger.info("压缩循环：收到停止信号")
                    break

                # ========== 步骤1：检索所有非is_copy_success的文件，超阈值的跳过不修改is_copy ==========
                # 新策略：调用数据库函数获取压缩组
                # 数据库函数内部已处理6次重试机制
                # fetch_pending_files_grouped_by_size 会自动：
                #   - 检索所有 is_copy_success = FALSE 的文件
                #   - 累积文件直到达到 max_file_size 阈值
                #   - 超过阈值的文件跳过（保持 FALSE 状态，下次仍可检索，不修改is_copy_success）
                logger.info(f"[压缩循环] [步骤1-检索文件] 开始检索下一批待压缩文件（文件组索引: {self.group_idx + 1}）...")
                # 实时读取最新配置
                settings = get_settings()
                logger.info(
                    f"[压缩循环] [步骤1-检索文件] 检索参数: backup_set_id={self.backup_set.id}, "
                    f"max_file_size={settings.MAX_FILE_SIZE}, "
                    f"should_wait={self.wait_retry_count < self.max_wait_retries}, "
                    f"wait_retry_count={self.wait_retry_count}/{self.max_wait_retries}"
                )
                
                try:
                    import time
                    retrieval_start_time = time.time()
                    # 添加日志：记录查询参数
                    logger.info(
                        f"[压缩循环] [步骤1-检索文件] 开始检索下一批待压缩文件（文件组索引: {self.group_idx + 1}）... "
                        f"start_from_id={self.last_processed_file_id}, "
                        f"wait_retry_count={self.wait_retry_count}/{self.max_wait_retries}"
                    )
                    result = await self.backup_db.fetch_pending_files_grouped_by_size(
                        self.backup_set.id,
                        settings.MAX_FILE_SIZE,
                        self.backup_task.id,
                        should_wait_if_small=(self.wait_retry_count < self.max_wait_retries),
                        start_from_id=self.last_processed_file_id  # 关键修复：从上次处理的文件ID开始查询，避免重复
                    )
                    # 处理新的返回格式：(file_groups, last_processed_id)
                    if isinstance(result, tuple) and len(result) == 2:
                        file_groups, last_processed_id = result
                        # 更新最后处理的文件ID，即使返回空列表也要更新
                        old_last_processed_id = self.last_processed_file_id
                        
                        # 关键修复：如果返回的 last_processed_id 为 0，说明应该重置查询
                        if last_processed_id == 0 and self.last_processed_file_id > 0:
                            logger.warning(
                                f"[压缩循环] [步骤1-检索文件] ⚠️ 检测到需要重置查询："
                                f"返回的 last_processed_id=0，当前值={self.last_processed_file_id}，"
                                f"可能存在ID更小的未压缩文件，重置为0重新查询"
                            )
                            self.last_processed_file_id = 0
                        elif last_processed_id > self.last_processed_file_id:
                            self.last_processed_file_id = last_processed_id
                            logger.info(
                                f"[压缩循环] [步骤1-检索文件] ✅ 更新最后处理的文件ID: "
                                f"{old_last_processed_id} -> {self.last_processed_file_id}"
                            )
                        else:
                            logger.warning(
                                f"[压缩循环] [步骤1-检索文件] ⚠️ 返回的 last_processed_id ({last_processed_id}) "
                                f"不大于当前值 ({self.last_processed_file_id})，未更新"
                            )
                    else:
                        # 兼容旧格式（如果没有返回元组，说明是旧版本）
                        logger.warning(
                            f"[压缩循环] [步骤1-检索文件] ⚠️ 返回格式不是元组，可能是旧版本代码，"
                            f"result类型: {type(result)}"
                        )
                        file_groups = result
                        last_processed_id = 0
                    retrieval_elapsed = time.time() - retrieval_start_time
                    logger.info(
                        f"[压缩循环] [步骤1-检索文件] 检索完成，耗时: {retrieval_elapsed:.2f}秒，"
                        f"返回文件组数量: {len(file_groups) if file_groups else 0}, "
                        f"last_processed_id={last_processed_id}, "
                        f"当前 self.last_processed_file_id={self.last_processed_file_id}"
                    )
                except Exception as retrieval_error:
                    logger.error(
                        f"[压缩循环] [步骤1-检索文件] 检索失败: {str(retrieval_error)}",
                        exc_info=True
                    )
                    # 检索失败，等待后重试
                    await asyncio.sleep(5)
                    continue
                
                if not file_groups:
                    # 新策略：返回空列表说明没有待压缩文件或需要等待
                    # 增加重试计数（每次返回空列表时递增）
                    if self.wait_retry_count < self.max_wait_retries:
                        self.wait_retry_count += 1
                    
                    scan_status = await self.backup_db.get_scan_status(self.backup_task.id)
                    total_files_from_db = await self.backup_db.get_total_files_from_db(self.backup_task.id)

                    logger.info(
                        f"[压缩循环] [步骤1-检索文件] 文件组为空，扫描状态={scan_status}, "
                        f"已处理={self.processed_files}, 总文件={total_files_from_db}, "
                        f"idle_checks={self.idle_checks}/{self.max_idle_checks}, "
                        f"wait_retry_count={self.wait_retry_count}/{self.max_wait_retries}"
                    )

                    if scan_status == 'completed' or self.processed_files >= total_files_from_db:
                        # 扫描完成或已处理所有文件，退出循环
                        logger.info(
                            f"[压缩循环] 所有文件已压缩完毕，退出压缩循环。"
                            f"扫描状态={scan_status}, 已处理={self.processed_files}, 总文件={total_files_from_db}"
                        )
                        break

                    # 否则等待更多文件
                    self.idle_checks += 1
                    if self.idle_checks >= self.max_idle_checks:
                        logger.warning(
                            f"[压缩循环] 等待压缩文件超时，扫描状态={scan_status}，"
                            f"已等待 {self.idle_checks * 5} 秒，继续等待..."
                        )
                    else:
                        logger.info(
                            f"[压缩循环] 等待更多文件，idle_checks={self.idle_checks}/{self.max_idle_checks}，"
                            f"将在5秒后重试..."
                        )

                    await asyncio.sleep(5)
                    continue
                
                # 成功获取到文件组，重置等待计数
                self.wait_retry_count = 0
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
                        files_per_archive = min(settings.MAX_FILE_SIZE / avg_file_size, total_files_from_db)
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
                # 使用全局settings获取最新配置（仅在循环开始时读取一次，避免频繁调用）
                settings = get_settings()
                compression_method = getattr(settings, 'COMPRESSION_METHOD', 'pgzip')
                compression_level = getattr(settings, 'COMPRESSION_LEVEL', 9)
                compression_threads = getattr(settings, 'COMPRESSION_THREADS', 4)
                # PGZip 专用参数
                pgzip_block_size = getattr(settings, 'PGZIP_BLOCK_SIZE', '1G')
                pgzip_threads = getattr(settings, 'PGZIP_THREADS', compression_threads)
                try:
                    pgzip_threads = int(pgzip_threads)
                except (ValueError, TypeError):
                    pgzip_threads = int(compression_threads)
                
                # 7-Zip 命令行线程数（用于非 pgzip 场景）
                compression_command_threads = getattr(settings, 'COMPRESSION_COMMAND_THREADS', None)
                if compression_command_threads is None:
                    compression_command_threads = getattr(settings, 'WEB_WORKERS', compression_threads)
                try:
                    compression_command_threads = int(compression_command_threads)
                except (ValueError, TypeError):
                    compression_command_threads = int(compression_threads)
                
                zstd_threads = getattr(settings, 'ZSTD_THREADS', compression_threads)
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
                    # 再次检查是否被取消
                    if not self._running:
                        logger.info("压缩循环：在压缩前检测到停止信号，中止压缩")
                        progress_update_task.cancel()
                        try:
                            await progress_update_task
                        except asyncio.CancelledError:
                            pass
                        break
                    
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
                    
                    # 检查是否在压缩过程中被取消
                    if not self._running:
                        logger.info("压缩循环：在压缩过程中检测到停止信号，中止循环")
                        break
                    
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
                    
                    # ========== 步骤3：修改数据库is_copy_success（顺序执行，必须在压缩完成后立即执行，在文件移动之前） ==========
                    # 重要：压缩完成后立即更新数据库（is_copy_success = TRUE），顺序执行
                    # 必须等待数据库更新完成后再继续下一组压缩，确保数据一致性
                    # 否则下一组检索时可能还会检索到已压缩但is_copy_success未更新的文件，导致重复压缩
                    # 只有成功压缩的文件才会更新is_copy_success，超阈值跳过的文件保持FALSE状态
                    # 注意：数据库更新必须在文件移动之前执行，确保压缩完成后立即更新数据库状态
                    if compress_directly_to_tape:
                        tape_file_path = compressed_file['path']
                    else:
                        # 暂时使用源路径作为磁带路径（移动完成后会更新）
                        tape_file_path = compressed_file['path']
                    
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
                    
                    # ========== 步骤4：文件移动（在数据库更新完成后执行，不阻塞压缩循环） ==========
                    if compress_directly_to_tape:
                        # 直接压缩到磁带，不需要移动队列，但仍需要更新状态
                        logger.info(f"文件已直接压缩到磁带机: {compressed_file['path']}")

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
                        # 非直接压缩模式：文件已经在压缩线程中移动到final目录
                        # file_move_worker 会独立扫描 final 目录并移动到磁带，不需要向它发送消息
                        logger.info(f"文件已在压缩线程中移动到final目录: {compressed_file.get('final_path')}")
                        logger.debug(f"file_move_worker 将独立扫描 final 目录并处理文件移动到磁带")

                    # ========== 更新进度统计（在数据库更新之后） ==========
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
                            # 使用全局settings获取最新配置
                            settings = get_settings()
                            files_per_archive = min(settings.MAX_FILE_SIZE / avg_file_size, total_files_from_db)
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
                    
                    # 验证 is_copy_success 是否已正确更新（全量验证，使用批量查询优化性能）
                    try:
                        # 提取所有文件路径进行验证
                        all_paths = [f.get("file_path") or f.get("path") for f in file_group if f.get("file_path") or f.get("path")]
                        if all_paths:
                            from utils.scheduler.db_utils import is_opengauss, is_redis, get_opengauss_connection
                            from utils.scheduler.sqlite_utils import get_sqlite_connection
                            
                            if is_redis():
                                # Redis 模式：全量验证，使用批量查询优化性能
                                from config.redis_db import get_redis_client
                                from backup.redis_backup_db import KEY_PREFIX_BACKUP_FILE, KEY_INDEX_BACKUP_FILE_BY_PATH, _get_redis_key
                                
                                redis = await get_redis_client()
                                path_index_key = KEY_INDEX_BACKUP_FILE_BY_PATH
                                
                                verified_count = 0
                                total_checked = 0
                                
                                # 分批验证，每批1000个文件，避免单个pipeline过大导致超时
                                batch_size = 1000
                                for batch_start in range(0, len(all_paths), batch_size):
                                    batch_paths = all_paths[batch_start:batch_start + batch_size]
                                    
                                    # 构建路径索引键: backup_set_id:file_path
                                    path_keys = [f"{self.backup_set.id}:{path}" for path in batch_paths]
                                    
                                    # 批量获取文件ID
                                    pipe = redis.pipeline()
                                    for path_key in path_keys:
                                        pipe.hget(path_index_key, path_key)
                                    
                                    file_id_results = await pipe.execute()
                                    
                                    # 根据文件ID批量获取文件状态
                                    file_ids_to_check = []
                                    for file_id_str in file_id_results:
                                        if file_id_str:
                                            try:
                                                file_id = int(file_id_str)
                                                file_ids_to_check.append(file_id)
                                            except (ValueError, TypeError):
                                                pass
                                    
                                    if file_ids_to_check:
                                        # 批量获取文件状态
                                        pipe2 = redis.pipeline()
                                        for file_id in file_ids_to_check:
                                            file_key = _get_redis_key(KEY_PREFIX_BACKUP_FILE, file_id)
                                            pipe2.hget(file_key, 'is_copy_success')
                                        
                                        status_results = await pipe2.execute()
                                        
                                        # 统计验证结果
                                        for is_copy_success in status_results:
                                            if is_copy_success == '1':
                                                verified_count += 1
                                            total_checked += 1
                                
                                if total_checked > 0:
                                    logger.info(f"[压缩循环] [验证] 文件组 {current_group_idx + 1} 验证完成: {verified_count}/{total_checked} 个文件的 is_copy_success=1 (总文件数: {len(all_paths)})")
                                    if verified_count < total_checked:
                                        logger.warning(f"[压缩循环] [验证] ⚠️ 验证失败: 期望 {total_checked} 个文件 is_copy_success=1，实际只有 {verified_count} 个 (失败率: {(total_checked - verified_count) / total_checked * 100:.1f}%)")
                                else:
                                    logger.warning(f"[压缩循环] [验证] ⚠️ 无法验证：未找到文件记录（路径可能不匹配）")
                            elif is_opengauss():
                                # openGauss 模式：全量验证（使用原生 openGauss SQL）
                                async with get_opengauss_connection() as conn:
                                    # 分批验证，每批1000个文件
                                    batch_size = 1000
                                    verified_count = 0
                                    total_checked = 0
                                    
                                    for batch_start in range(0, len(all_paths), batch_size):
                                        batch_paths = all_paths[batch_start:batch_start + batch_size]
                                        # 使用原生 openGauss SQL：ANY($2) 方式查询
                                        # 分别查询总记录数和 is_copy_success=TRUE 的数量，避免 FILTER 语法兼容性问题
                                        try:
                                            # 查询总记录数
                                            total_result = await conn.fetchrow(
                                                """
                                                SELECT COUNT(*)::BIGINT as count
                                                FROM backup_files 
                                                WHERE backup_set_id = $1 AND file_path = ANY($2)
                                                """,
                                                self.backup_set.id, batch_paths
                                            )
                                            batch_total = total_result['count'] if total_result else 0
                                            
                                            # 查询 is_copy_success=TRUE 的数量
                                            verified_result = await conn.fetchrow(
                                                """
                                                SELECT COUNT(*)::BIGINT as count
                                                FROM backup_files 
                                                WHERE backup_set_id = $1 
                                                  AND file_path = ANY($2)
                                                  AND is_copy_success = TRUE
                                                """,
                                                self.backup_set.id, batch_paths
                                            )
                                            batch_verified = verified_result['count'] if verified_result else 0
                                            
                                            verified_count += batch_verified
                                            total_checked += batch_total
                                        except Exception as verify_batch_error:
                                            logger.error(
                                                f"[压缩循环] [验证] 批次验证失败: {str(verify_batch_error)}，"
                                                f"批次 {batch_start // batch_size + 1}，跳过该批次",
                                                exc_info=True
                                            )
                                            # 跳过该批次，继续验证其他批次
                                            continue
                                    
                                    logger.info(f"[压缩循环] [验证] 文件组 {current_group_idx + 1} 验证完成: {verified_count}/{total_checked} 个文件的 is_copy_success=TRUE (总文件数: {len(all_paths)})")
                                    if verified_count < total_checked:
                                        logger.warning(f"[压缩循环] [验证] ⚠️ 验证失败: 期望 {total_checked} 个文件 is_copy_success=TRUE，实际只有 {verified_count} 个")
                            else:
                                # SQLite 模式：全量验证
                                async with get_sqlite_connection() as conn:
                                    # 分批验证，每批1000个文件
                                    batch_size = 1000
                                    verified_count = 0
                                    total_checked = 0
                                    
                                    for batch_start in range(0, len(all_paths), batch_size):
                                        batch_paths = all_paths[batch_start:batch_start + batch_size]
                                        placeholders = ','.join(['?' for _ in batch_paths])
                                        verify_cursor = await conn.execute(f"""
                                            SELECT file_path, is_copy_success FROM backup_files 
                                            WHERE backup_set_id = ? AND file_path IN ({placeholders})
                                        """, (self.backup_set.id,) + tuple(batch_paths))
                                        verify_rows = await verify_cursor.fetchall()
                                        verified_count += sum(1 for row in verify_rows if row[1] == 1)
                                        total_checked += len(verify_rows)
                                    
                                    logger.info(f"[压缩循环] [验证] 文件组 {current_group_idx + 1} 验证完成: {verified_count}/{total_checked} 个文件的 is_copy_success=1 (总文件数: {len(all_paths)})")
                                    if verified_count < total_checked:
                                        logger.warning(f"[压缩循环] [验证] ⚠️ 验证失败: 期望 {total_checked} 个文件 is_copy_success=1，实际只有 {verified_count} 个")
                        else:
                            logger.warning(f"[压缩循环] [验证] ⚠️ 无法提取文件路径进行验证，file_group示例: {file_group[:3] if file_group else '空'}")
                    except Exception as verify_error:
                        logger.error(f"[压缩循环] [验证] ❌ 验证 is_copy_success 时出错: {str(verify_error)}", exc_info=True)
                
                except (KeyboardInterrupt, asyncio.CancelledError) as cancel_error:
                    # 收到中断信号，立即停止
                    logger.warning(f"========== 压缩循环收到中断信号，正在中止 ==========")
                    logger.warning(f"任务ID: {self.backup_task.id if self.backup_task else 'N/A'}")
                    logger.warning(f"已处理文件组: {self.group_idx}")
                    # 取消进度更新任务
                    if 'progress_update_task' in locals():
                        progress_update_task.cancel()
                        try:
                            await progress_update_task
                        except asyncio.CancelledError:
                            pass
                    self._running = False
                    raise
                except Exception as group_error:
                    # 文件组处理失败，记录错误但继续处理下一个文件组
                    logger.error(f"⚠️ 处理文件组 {current_group_idx + 1} 时发生错误: {str(group_error)}，跳过该文件组，继续处理下一个文件组")
                    import traceback
                    logger.error(f"错误堆栈:\n{traceback.format_exc()}")
                
                # 在循环结束前检查是否被取消
                if not self._running:
                    logger.info("压缩循环：检测到停止信号，退出循环")
                    break
                
                # ========== 步骤4：循环到步骤1（更新文件组索引，准备下一轮检索） ==========
                # 更新文件组索引（每次只处理一个文件组）
                logger.info(f"[压缩循环] [步骤4-循环] ========== 开始准备下一轮循环 ==========")
                logger.info(f"[压缩循环] [步骤4-循环] 当前文件组索引: {self.group_idx}，准备更新为: {self.group_idx + 1}")
                self.group_idx += 1
                logger.info(f"[压缩循环] [步骤4-循环] ✅ 文件组索引已更新: {self.group_idx}")
                
                # 关键修复：重置等待计数，确保立即查询下一批文件，而不是等待
                # 因为新文件可能已经同步到数据库，或者正在同步中
                self.wait_retry_count = 0
                self.idle_checks = 0
                logger.info(f"[压缩循环] [步骤4-循环] ✅ 已重置等待计数（wait_retry_count=0, idle_checks=0），准备立即查询下一批文件")
                
                if hasattr(self.backup_task, 'result_summary') and isinstance(self.backup_task.result_summary, dict):
                    estimated_count = self.backup_task.result_summary.get('estimated_archive_count', 'N/A')
                    logger.info(f"[压缩循环] [步骤4-循环] 文件组处理完成，estimated_archive_count={estimated_count}")

                logger.info(f"[压缩循环] [步骤4-循环] 准备更新扫描进度...")
                try:
                    import time
                    progress_update_start = time.time()
                    await self.backup_db.update_scan_progress(
                        self.backup_task,
                        self.processed_files,
                        self.processed_files,
                        "[检索下一批文件...]"
                    )
                    progress_update_elapsed = time.time() - progress_update_start
                    logger.info(f"[压缩循环] [步骤4-循环] ✅ 扫描进度已更新，耗时: {progress_update_elapsed:.2f}秒")
                    logger.info(f"[压缩循环] [步骤4-循环] 准备继续循环检索下一批文件（回到步骤1）...")
                except Exception as progress_error:
                    logger.error(f"[压缩循环] [步骤4-循环] ❌ 更新扫描进度失败: {str(progress_error)}，继续循环", exc_info=True)
                    # 即使更新失败，也继续循环
                
                logger.info(f"[压缩循环] [步骤4-循环] ✅ 准备开始下一轮循环（文件组索引: {self.group_idx + 1}）...")
                logger.info(f"[压缩循环] [步骤4-循环] ========== 准备回到步骤1（检索下一批文件） ==========")
                # 注意：这里不 sleep，立即继续循环到步骤1，查询下一批文件
            
        except (KeyboardInterrupt, asyncio.CancelledError):
            logger.warning("========== 压缩循环被中止 ==========")
            logger.warning(f"任务ID: {self.backup_task.id if self.backup_task else 'N/A'}")
            logger.warning(f"已处理文件组: {self.group_idx}")
            self._running = False
            raise
        except Exception as e:
            logger.error(f"[压缩循环线程] 压缩循环任务异常: {str(e)}", exc_info=True)
        finally:
            logger.info("[压缩循环线程] 压缩循环后台任务已退出")
