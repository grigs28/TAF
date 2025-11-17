#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
后台扫描任务模块
Background Scanner Module

独立的后台扫描任务，专门更新卡片中的总文件数和总字节数
"""

import asyncio
import logging
import os
import time
from pathlib import Path
from typing import List, Optional

from models.backup import BackupTask, BackupSet
from backup.utils import format_bytes
from config.settings import get_settings

logger = logging.getLogger(__name__)


class BackupScanner:
    """后台扫描任务类"""
    
    def __init__(self, file_scanner, backup_db):
        """初始化后台扫描任务
        
        Args:
            file_scanner: 文件扫描器对象
            backup_db: 数据库操作对象
        """
        self.file_scanner = file_scanner
        self.backup_db = backup_db
        self.settings = get_settings()
    
    async def scan_for_progress_update(
        self,
        backup_task: BackupTask,
        source_paths: List[str],
        exclude_patterns: List[str],
        backup_set: BackupSet,
        restart: bool = False
    ):
        """独立的后台扫描任务，专门更新卡片中的总文件数和总字节数
        
        这个任务：
        1. 独立扫描目录，统计文件数和字节数
        2. 定期（每100个文件）更新数据库中的 total_files 和 total_bytes
        3. 扫描完成后任务退出
        4. 不影响压缩流程
        5. 支持 KeyboardInterrupt 和 CancelledError，能够被正确取消
        
        Args:
            backup_task: 备份任务对象
            source_paths: 源路径列表
            exclude_patterns: 排除模式列表
        """
        backup_set_db_id = getattr(backup_set, 'id', None)

        try:
            if backup_task and backup_task.id:
                await self.backup_db.update_scan_status(backup_task.id, 'running')
            if restart and backup_set_db_id:
                await self.backup_db.clear_backup_files_for_set(backup_set_db_id)
            
            # 记录关键阶段：扫描文件开始
            self.backup_db._log_operation_stage_event(backup_task, "[扫描文件中...]")
            # 更新operation_stage和description
            await self.backup_db.update_task_stage_with_description(
                backup_task,
                "scan",
                "[扫描文件中] 正在扫描源文件系统..."
            )
            
            logger.info("========== 后台扫描任务启动：专门更新卡片中的总文件数和总字节数 ==========")
            logger.info(f"任务ID: {backup_task.id if backup_task else 'N/A'}")
            logger.info(f"源路径列表: {source_paths}")
            logger.info(f"排除规则: {exclude_patterns}")
            
            # 确保 source_paths 不为 None
            if source_paths is None:
                logger.warning("后台扫描任务：source_paths 为 None，使用空列表")
                source_paths = []
            
            if not source_paths:
                logger.warning("后台扫描任务：source_paths 为空列表，没有文件要扫描")
                # 即使没有源路径，也要更新数据库（设置为0）
                await self.backup_db.update_scan_progress_only(backup_task, 0, 0)
                logger.info("========== 后台扫描任务完成：没有源路径 ==========")
                return
            
            # 使用流式扫描模式：一边扫描一边通过队列提交文件信息，由专门的后台worker写入 backup_files，并按间隔更新统计
            # 非流式分支仅用于兼容旧逻辑（只统计、不写入文件列表）
            use_streaming = True
            if use_streaming:
                total_files = 0
                total_bytes = 0
                # 从配置读取扫描进度更新间隔（.env 中可通过 SCAN_UPDATE_INTERVAL 覆盖）
                update_interval = self.settings.SCAN_UPDATE_INTERVAL
                # 进度日志时间间隔（秒），控制“后台扫描任务：已扫描 N 个文件...”的输出频率
                log_interval_seconds = getattr(self.settings, 'SCAN_LOG_INTERVAL_SECONDS', 60)
                # 统计用时间基准（用于计算扫描速度）
                scan_start_time = time.time()
                last_log_time = scan_start_time
                last_log_files = 0

                # 使用批量数据库写入器提升写入性能

                if backup_set_db_id:
                    # 使用内存数据库写入器，实现极速写入 + 异步同步
                    # 从配置读取内存数据库参数
                    use_memory_db = getattr(self.settings, 'USE_MEMORY_DB', True)

                    if use_memory_db:
                        sync_batch_size = getattr(self.settings, 'MEMORY_DB_SYNC_BATCH_SIZE', 5000)
                        sync_interval = getattr(self.settings, 'MEMORY_DB_SYNC_INTERVAL', 30)
                        max_memory_files = getattr(self.settings, 'MEMORY_DB_MAX_FILES', 100000)

                        from backup.memory_db_writer import MemoryDBWriter
                        memory_writer = MemoryDBWriter(
                            backup_set_db_id=backup_set_db_id,
                            sync_batch_size=sync_batch_size,
                            sync_interval=sync_interval,
                            max_memory_files=max_memory_files
                        )
                        await memory_writer.initialize()
                        logger.info(f"内存数据库写入器已启动 (sync_batch={sync_batch_size}, interval={sync_interval}s)")
                    else:
                        # 回退到批量写入器
                        batch_size = getattr(self.settings, 'DB_BATCH_SIZE', 1000)
                        max_queue_size = getattr(self.settings, 'DB_QUEUE_MAX_SIZE', 5000)

                        from backup.backup_db import BatchDBWriter
                        batch_writer = BatchDBWriter(
                            backup_set_db_id=backup_set_db_id,
                            batch_size=batch_size,
                            max_queue_size=max_queue_size
                        )
                        await batch_writer.start()  # 启动批量写入器
                        logger.info(f"批量写入器已启动 (batch_size={batch_size}, max_queue={max_queue_size})")
                
                async for file_batch in self.file_scanner.scan_source_files_streaming(
                    source_paths,
                    exclude_patterns,
                    backup_task,
                    log_context="[后台扫描]"
                ):
                    for file_info in file_batch:
                        file_size = file_info.get('size', 0) or 0
                        total_files += 1
                        total_bytes += file_size
                        
                        # 扫描线程将文件信息提交到内存数据库或批量写入器
                        if backup_set_db_id:
                            try:
                                if use_memory_db and 'memory_writer' in locals():
                                    # 写入内存数据库（极速）
                                    await memory_writer.add_file(file_info)
                                else:
                                    # 写入批量写入器
                                    await batch_writer.add_file(file_info)
                            except asyncio.TimeoutError:
                                # 队列已满，记录警告但继续扫描
                                logger.warning(f"写入队列已满，跳过文件: {file_info.get('path', 'unknown')}")
                            except Exception as e:
                                # 其他错误，记录但不中断扫描
                                logger.error(f"写入文件失败: {e}, 文件: {file_info.get('path', 'unknown')}")
                        
                        if total_files % update_interval == 0:
                            # 先更新数据库中的统计字段
                            await self.backup_db.update_scan_progress_only(backup_task, total_files, total_bytes)
                            # 再根据时间间隔决定是否输出一条统计日志
                            now = time.time()
                            elapsed_since_last_log = now - last_log_time
                            if elapsed_since_last_log >= log_interval_seconds:
                                elapsed_total = max(now - scan_start_time, 0.001)
                                elapsed_window = max(elapsed_since_last_log, 0.001)
                                files_total_rate = total_files / elapsed_total
                                files_window = total_files - last_log_files
                                files_window_rate = files_window / elapsed_window if files_window > 0 else 0.0
                                logger.info(
                                    f"后台扫描任务：已扫描 {total_files} 个文件，总大小 {format_bytes(total_bytes)}，"
                                    f"平均速度 {files_total_rate:.1f} 个文件/秒，"
                                    f"最近 {files_window} 个文件用时 {elapsed_window:.1f} 秒，速度 {files_window_rate:.1f} 个文件/秒"
                                )
                                last_log_time = now
                                last_log_files = total_files

                # 等待写入器完成所有文件写入
                if backup_set_db_id:
                    try:
                        if use_memory_db and 'memory_writer' in locals():
                            # 停止内存数据库写入器（会自动完成最终同步）
                            stats = memory_writer.get_stats()
                            sync_status = await memory_writer.get_sync_status()

                            logger.info(f"内存数据库统计: 处理 {stats['total_files']} 个文件，"
                                       f"已同步 {stats['synced_files']} 个文件，同步进度 {stats['sync_progress']:.1f}%")

                            logger.info(f"同步状态: 总计 {sync_status['total_files']}, "
                                       f"已同步 {sync_status['synced_files']}, "
                                       f"待同步 {sync_status['pending_files']}, "
                                       f"错误 {sync_status['error_files']}")

                            await memory_writer.stop()

                        elif 'batch_writer' in locals():
                            # 停止批量写入器
                            stats = batch_writer.get_stats()
                            logger.info(f"批量写入统计: 处理 {stats['total_files']} 个文件，"
                                       f"完成 {stats['batch_count']} 个批次，耗时 {stats['total_time']:.1f}s")

                            await batch_writer.stop()

                    except Exception as e:
                        logger.error(f"后台扫描任务：停止写入器时出错: {str(e)}", exc_info=True)
                
                # 扫描完成后做最后一次进度更新
                await self.backup_db.update_scan_progress_only(backup_task, total_files, total_bytes)
                # 结束时再输出一次总平均速度
                end_time = time.time()
                elapsed_total = max(end_time - scan_start_time, 0.001)
                files_total_rate = total_files / elapsed_total
                logger.info(
                    f"后台扫描任务完成，共扫描 {total_files} 个文件，总大小 {format_bytes(total_bytes)}，"
                    f"平均速度 {files_total_rate:.1f} 个文件/秒"
                )
                
                # 记录关键阶段：扫描完成
                self.backup_db._log_operation_stage_event(backup_task, f"[扫描完成] 共 {total_files} 个文件，总大小 {format_bytes(total_bytes)}")
                # 更新operation_stage和description
                await self.backup_db.update_task_stage_with_description(
                    backup_task,
                    "scan",
                    f"[扫描完成] 共 {total_files} 个文件，总大小 {format_bytes(total_bytes)}"
                )
                
                if backup_task and backup_task.id:
                    await self.backup_db.update_scan_status(backup_task.id, 'completed')
                return
            
            total_files = 0  # 总文件数
            total_bytes = 0  # 总字节数
            # 从配置读取扫描进度更新间隔（.env 中可通过 SCAN_UPDATE_INTERVAL 覆盖）
            update_interval = self.settings.SCAN_UPDATE_INTERVAL
            
            # 处理网络路径（UNC路径）
            from utils.network_path import is_unc_path, normalize_unc_path
            
            for source_path_str in source_paths:
                logger.info(f"后台扫描任务：扫描源路径 {source_path_str}")
                
                # 处理 UNC 网络路径
                if is_unc_path(source_path_str):
                    normalized_path = normalize_unc_path(source_path_str)
                    source_path = Path(normalized_path)
                    logger.debug(f"后台扫描任务：检测到 UNC 路径，规范化后: {normalized_path}")
                else:
                    source_path = Path(source_path_str)
                
                if not source_path.exists():
                    logger.warning(f"后台扫描任务：源路径不存在，跳过: {source_path_str}")
                    continue
                
                try:
                    if source_path.is_file():
                        # 单个文件
                        try:
                            # 检查是否应该排除
                            if self.file_scanner.should_exclude_file(str(source_path), exclude_patterns):
                                continue
                            
                            # 获取文件信息
                            file_info = await self.file_scanner.get_file_info(source_path)
                            if file_info:
                                if backup_set_db_id:
                                    await self.backup_db.upsert_scanned_file_record(backup_set_db_id, file_info)
                                total_files += 1
                                total_bytes += file_info['size']
                                
                                # 每100个文件更新一次数据库
                                if total_files % update_interval == 0:
                                    await self.backup_db.update_scan_progress_only(backup_task, total_files, total_bytes)
                                    logger.info(f"后台扫描任务：已扫描 {total_files} 个文件，总大小 {format_bytes(total_bytes)}")
                        except (PermissionError, OSError, FileNotFoundError, IOError) as e:
                            logger.warning(f"后台扫描任务：跳过无法访问的文件: {source_path_str} (错误: {str(e)})")
                            continue
                        except Exception as e:
                            logger.warning(f"后台扫描任务：跳过出错的文件: {source_path_str} (错误: {str(e)})")
                            continue
                    
                    elif source_path.is_dir():
                        # 目录：递归扫描
                        logger.info(f"后台扫描任务：扫描目录 {source_path_str}")
                        
                        # 检查目录本身是否应该排除
                        if self.file_scanner.should_exclude_file(str(source_path), exclude_patterns):
                            logger.info(f"后台扫描任务：目录匹配排除规则，跳过整个目录: {source_path_str}")
                            continue
                        
                        # 使用队列来传递扫描结果（每100个文件一批）
                        scan_queue = asyncio.Queue(maxsize=0)  # 无限制队列
                        
                        # 重要：获取当前事件循环（主事件循环），在启动后台线程之前
                        # 因为后台线程中无法使用 get_running_loop()
                        main_loop = asyncio.get_running_loop()
                        
                        # 创建停止标志（用于响应 Ctrl+C）
                        import threading
                        stop_event = threading.Event()
                        
                        def sync_scan_worker():
                            """在线程池中执行同步遍历，统计文件数和字节数，根据目录数量智能匹配批次阈值（10、25、50、100个文件）提交一次，或者每20分钟强制提交一次"""
                            batch_files = 0  # 当前批次的文件数
                            batch_bytes = 0  # 当前批次的字节数
                            last_log_count = 0  # 上次输出日志的文件数
                            last_log_time = None  # 上次输出日志的时间
                            last_batch_submit_time = None  # 上次提交批次的时间
                            last_progress_log_time = None  # 上次输出进度日志的时间
                            last_dir_log_time = None  # 上次输出目录日志的时间
                            current_dir = None  # 当前正在扫描的目录
                            scan_failed = False  # 扫描是否失败
                            scan_error = None  # 扫描错误信息
                            file_count = 0  # 文件计数（在finally中使用）
                            dir_count = 0  # 目录计数
                            permission_error_count = 0  # 权限错误计数
                            path_too_long_count = 0  # 路径过长错误计数
                            BATCH_FORCE_INTERVAL = 1200.0  # 强制提交批次的时间间隔（秒）- 20分钟
                            PROGRESS_LOG_INTERVAL = 60.0  # 进度日志输出间隔（秒）- 1分钟
                            DIR_LOG_INTERVAL = 120.0  # 目录日志输出间隔（秒）- 2分钟
                            
                            def get_batch_threshold(dir_count: int) -> int:
                                """根据目录数量智能匹配批次阈值
                                
                                使用 settings.SCAN_BATCH_SIZE 作为基础值，根据目录数量进行比例调整
                                
                                Args:
                                    dir_count: 待扫描目录数量
                                
                                Returns:
                                    int: 批次阈值（文件数）
                                """
                                # 使用 settings.SCAN_BATCH_SIZE 作为基础值
                                base_batch_size = self.settings.SCAN_BATCH_SIZE
                                
                                if dir_count >= 50000:
                                    # 目录数量很多（>50000），使用基础值的约1.7%（300/180000）
                                    return int(base_batch_size * 0.0017)
                                elif dir_count >= 10000:
                                    # 目录数量较多（10000-50000），使用基础值的约0.28%（500/180000）
                                    return int(base_batch_size * 0.0028)
                                elif dir_count >= 1000:
                                    # 目录数量中等（1000-10000），使用基础值的约0.44%（800/180000）
                                    return int(base_batch_size * 0.0044)
                                else:
                                    # 目录数量少（<1000），使用基础值的约0.56%（1000/180000）
                                    return int(base_batch_size * 0.0056)
                            
                            # 获取批次字节数阈值（与压缩扫描使用相同的参数）
                            batch_bytes_threshold = self.settings.SCAN_BATCH_SIZE_BYTES
                            MAX_PATH_LENGTH = 260  # Windows路径最大长度（字符）
                            MAX_PATH_DISPLAY = 200  # 日志中显示的最大路径长度（字符）
                            
                            def truncate_path(path_str: str, max_len: int = MAX_PATH_DISPLAY) -> str:
                                """截断路径以便在日志中显示"""
                                if len(path_str) <= max_len:
                                    return path_str
                                # 保留开头和结尾
                                prefix_len = max_len - 50
                                return f"{path_str[:prefix_len]}...{path_str[-(max_len-prefix_len-3):]}"
                            
                            def format_path_for_log(path_str: str) -> str:
                                """格式化路径以便在日志中显示，考虑长度限制"""
                                try:
                                    # 检查路径长度
                                    path_len = len(path_str)
                                    if path_len > MAX_PATH_LENGTH:
                                        return f"{truncate_path(path_str)} (路径长度: {path_len} 字符，超过Windows限制 {MAX_PATH_LENGTH} 字符)"
                                    return truncate_path(path_str)
                                except Exception:
                                    return str(path_str)[:MAX_PATH_DISPLAY]
                            
                            try:
                                logger.info(f"后台扫描任务：开始遍历目录 {source_path_str}（线程中）")
                                file_count = 0
                                dir_count = 0
                                start_time = time.time()
                                last_log_time = start_time
                                last_batch_submit_time = start_time
                                last_progress_log_time = start_time
                                last_dir_log_time = start_time
                                
                                # 使用 os.scandir() 替代 rglob() 以提高性能（特别是对于大量目录）
                                # os.scandir() 在 Windows 上比 rglob() 更快，且内存占用更少
                                # 使用迭代方式遍历目录（避免 rglob 的内存问题）
                                dirs_to_scan = [source_path]  # 待扫描的目录队列
                                scanned_dirs = set()  # 已扫描的目录集合（避免重复扫描）
                                
                                # 对于大量目录，调整日志频率
                                LARGE_DIR_THRESHOLD = 10000  # 超过1万个目录时，使用更频繁的日志
                                is_large_dir_structure = False
                                
                                try:
                                    while dirs_to_scan:
                                        # 检查停止标志（响应 Ctrl+C）
                                        if stop_event.is_set():
                                            logger.warning(f"后台扫描任务：检测到停止信号，中止扫描（目录: {source_path_str}），已扫描 {file_count} 个文件，{dir_count} 个目录")
                                            scan_failed = True
                                            scan_error = "用户中断（Ctrl+C）"
                                            break
                                        
                                        try:
                                            current_scan_dir = dirs_to_scan.pop(0)
                                            
                                            # 避免重复扫描
                                            try:
                                                current_scan_dir_str = str(current_scan_dir.resolve())
                                            except Exception:
                                                current_scan_dir_str = str(current_scan_dir)
                                            
                                            if current_scan_dir_str in scanned_dirs:
                                                continue
                                            
                                            scanned_dirs.add(current_scan_dir_str)
                                            current_dir = current_scan_dir_str
                                            dir_count += 1
                                            
                                            # 检测是否为大型目录结构（超过1万个目录）
                                            if dir_count >= LARGE_DIR_THRESHOLD and not is_large_dir_structure:
                                                is_large_dir_structure = True
                                                # 根据待扫描目录数量获取批次阈值
                                                current_batch_threshold = get_batch_threshold(len(dirs_to_scan))
                                                logger.info(f"后台扫描任务：检测到大型目录结构（已扫描 {dir_count} 个目录，待扫描目录: {len(dirs_to_scan)}），批次阈值: {current_batch_threshold}，将使用更频繁的进度日志（目录: {source_path_str}）")
                                            
                                            # 每2分钟输出一次当前扫描的目录（大型目录结构时每30秒输出一次）
                                            current_time = time.time()
                                            elapsed_since_dir_log = current_time - last_dir_log_time
                                            dir_log_interval = 30.0 if is_large_dir_structure else DIR_LOG_INTERVAL
                                            if current_dir and elapsed_since_dir_log >= dir_log_interval:
                                                logger.info(f"后台扫描任务：正在扫描目录 {format_path_for_log(current_dir)}，已扫描 {dir_count} 个目录，{file_count} 个文件，待扫描目录: {len(dirs_to_scan)}（目录: {source_path_str}）")
                                                last_dir_log_time = current_time
                                            
                                            # 检查停止标志（在扫描目录前检查）
                                            if stop_event.is_set():
                                                logger.warning(f"后台扫描任务：检测到停止信号，中止扫描（目录: {source_path_str}），已扫描 {file_count} 个文件，{dir_count} 个目录")
                                                scan_failed = True
                                                scan_error = "用户中断（Ctrl+C）"
                                                break
                                            
                                            try:
                                                # 使用 os.scandir() 扫描当前目录
                                                with os.scandir(current_scan_dir_str) as entries:
                                                    for entry in entries:
                                                        # 检查停止标志（在每次迭代时检查，提高响应速度）
                                                        if stop_event.is_set():
                                                            logger.warning(f"后台扫描任务：检测到停止信号，中止扫描（目录: {source_path_str}），已扫描 {file_count} 个文件，{dir_count} 个目录")
                                                            scan_failed = True
                                                            scan_error = "用户中断（Ctrl+C）"
                                                            break
                                                        try:
                                                            # 获取路径
                                                            try:
                                                                entry_path = Path(entry.path)
                                                                current_path_str = str(entry_path)
                                                                path_len = len(current_path_str)
                                                                
                                                                # 检查路径长度
                                                                if path_len > MAX_PATH_LENGTH:
                                                                    path_too_long_count += 1
                                                                    if path_too_long_count <= 10:  # 只记录前10个路径过长的情况
                                                                        logger.warning(f"后台扫描任务：路径过长（{path_len} 字符 > {MAX_PATH_LENGTH} 字符）: {format_path_for_log(current_path_str)}")
                                                                    continue
                                                            except Exception as path_str_err:
                                                                # 路径字符串化失败（可能是编码问题）
                                                                logger.debug(f"后台扫描任务：路径字符串化失败: {str(path_str_err)}")
                                                                continue
                                                            
                                                            # 检查是否应该排除
                                                            if current_path_str and self.file_scanner.should_exclude_file(current_path_str, exclude_patterns):
                                                                continue
                                                            
                                                            # 处理目录和文件
                                                            try:
                                                                if entry.is_dir(follow_symlinks=False):
                                                                    # 目录：添加到待扫描队列
                                                                    dirs_to_scan.append(entry_path)
                                                                elif entry.is_file(follow_symlinks=False):
                                                                    # 检查停止标志（在处理文件前检查）
                                                                    if stop_event.is_set():
                                                                        logger.warning(f"后台扫描任务：检测到停止信号，中止扫描（目录: {source_path_str}），已扫描 {file_count} 个文件，{dir_count} 个目录")
                                                                        scan_failed = True
                                                                        scan_error = "用户中断（Ctrl+C）"
                                                                        break
                                                                    
                                                                    # 文件：统计文件大小
                                                                    try:
                                                                        stat = entry_path.stat()
                                                                        file_size = stat.st_size
                                                                        
                                                                        # 在线程中统计（本地变量，线程安全）
                                                                        batch_files += 1
                                                                        batch_bytes += file_size
                                                                        file_count += 1
                                                                        
                                                                        current_time = time.time()
                                                                        
                                                                        # 每处理1000个文件检查一次停止标志（提高响应速度）
                                                                        if file_count % 1000 == 0:
                                                                            if stop_event.is_set():
                                                                                logger.warning(f"后台扫描任务：检测到停止信号，中止扫描（目录: {source_path_str}），已扫描 {file_count} 个文件，{dir_count} 个目录")
                                                                                scan_failed = True
                                                                                scan_error = "用户中断（Ctrl+C）"
                                                                                break
                                                                        
                                                                        # 检查是否需要强制提交批次（即使没有达到阈值，也要定期提交）
                                                                        elapsed_since_last_batch = current_time - last_batch_submit_time
                                                                        # 根据待扫描目录数量智能匹配批次阈值
                                                                        batch_threshold = get_batch_threshold(len(dirs_to_scan))
                                                                        
                                                                        if batch_files > 0 and elapsed_since_last_batch >= BATCH_FORCE_INTERVAL:
                                                                            # 强制提交当前批次（即使没有达到阈值，每20分钟强制提交一次）
                                                                            try:
                                                                                asyncio.run_coroutine_threadsafe(
                                                                                    scan_queue.put((batch_files, batch_bytes)),
                                                                                    main_loop
                                                                                )
                                                                                current_dir_display = format_path_for_log(current_dir) if current_dir else "未知"
                                                                                logger.info(f"后台扫描任务：强制提交批次到队列（超过{BATCH_FORCE_INTERVAL}秒），文件数={batch_files}, 字节数={format_bytes(batch_bytes)}，累计已扫描={file_count}个文件，{dir_count}个目录，待扫描目录: {len(dirs_to_scan)}，批次阈值: {batch_threshold}，当前目录: {current_dir_display}（目录: {source_path_str}）")
                                                                            except Exception as e:
                                                                                logger.error(f"后台扫描任务：强制提交批次失败: {str(e)}", exc_info=True)
                                                                            batch_files = 0
                                                                            batch_bytes = 0
                                                                            last_batch_submit_time = current_time
                                                                        # 根据目录数量智能匹配批次阈值提交批次（正常提交）
                                                                        # 检查文件数或字节数是否达到阈值（与压缩扫描使用相同的逻辑）
                                                                        elif batch_files >= batch_threshold or batch_bytes >= batch_bytes_threshold:
                                                                            try:
                                                                                # 使用主事件循环（在启动线程前获取的）
                                                                                asyncio.run_coroutine_threadsafe(
                                                                                    scan_queue.put((batch_files, batch_bytes)),
                                                                                    main_loop
                                                                                )
                                                                                current_dir_display = format_path_for_log(current_dir) if current_dir else "未知"
                                                                                logger.info(f"后台扫描任务：已提交批次到队列，文件数={batch_files}, 字节数={format_bytes(batch_bytes)}，累计已扫描={file_count}个文件，{dir_count}个目录，待扫描目录: {len(dirs_to_scan)}，批次阈值: {batch_threshold}，当前目录: {current_dir_display}（目录: {source_path_str}）")
                                                                            except Exception as e:
                                                                                logger.error(f"后台扫描任务：提交批次失败: {str(e)}", exc_info=True)
                                                                                # 提交批次失败不算扫描失败，继续扫描
                                                                            batch_files = 0
                                                                            batch_bytes = 0
                                                                            last_batch_submit_time = current_time
                                                                        
                                                                        # 每10000个文件或每1分钟输出一次进度日志（大型目录结构时每30秒输出一次）
                                                                        elapsed_since_last_progress = current_time - last_progress_log_time
                                                                        progress_log_interval = 30.0 if is_large_dir_structure else PROGRESS_LOG_INTERVAL
                                                                        if file_count - last_log_count >= 10000 or elapsed_since_last_progress >= progress_log_interval:
                                                                            # 检查停止标志（在输出日志时也检查，提高响应速度）
                                                                            if stop_event.is_set():
                                                                                logger.warning(f"后台扫描任务：检测到停止信号，中止扫描（目录: {source_path_str}），已扫描 {file_count} 个文件，{dir_count} 个目录")
                                                                                scan_failed = True
                                                                                scan_error = "用户中断（Ctrl+C）"
                                                                                break
                                                                            
                                                                            elapsed = current_time - last_log_time if last_log_time else 0
                                                                            rate = (file_count - last_log_count) / elapsed_since_last_progress if elapsed_since_last_progress > 0 else 0
                                                                            current_dir_display = format_path_for_log(current_dir) if current_dir else "未知"
                                                                            logger.info(f"后台扫描任务：正在扫描目录 {source_path_str}，已扫描 {file_count} 个文件，{dir_count} 个目录，待扫描目录: {len(dirs_to_scan)}，批次阈值: {batch_threshold}，当前批次 {batch_files} 个文件 {format_bytes(batch_bytes)}，耗时 {elapsed:.1f} 秒，速度 {rate:.0f} 文件/秒，距上次提交 {elapsed_since_last_batch:.1f} 秒，当前目录: {current_dir_display}，权限错误: {permission_error_count}，路径过长: {path_too_long_count}（线程运行中）")
                                                                            last_log_count = file_count
                                                                            last_log_time = current_time
                                                                            last_progress_log_time = current_time
                                                                            
                                                                    except (PermissionError, OSError) as file_err:
                                                                        # 权限错误：记录详细路径信息
                                                                        permission_error_count += 1
                                                                        try:
                                                                            file_path_display = format_path_for_log(current_path_str)
                                                                            if permission_error_count <= 20:  # 只记录前20个权限错误
                                                                                logger.warning(f"后台扫描任务：权限错误（文件 #{permission_error_count}）: {file_path_display}，错误: {str(file_err)}")
                                                                        except Exception:
                                                                            logger.warning(f"后台扫描任务：权限错误（文件 #{permission_error_count}）: 无法获取路径，错误: {str(file_err)}")
                                                                        continue
                                                                    except (FileNotFoundError, IOError) as file_err:
                                                                        # 文件不存在或IO错误：跳过
                                                                        continue
                                                                    except Exception as file_err:
                                                                        # 记录文件错误但继续扫描
                                                                        try:
                                                                            file_path_display = format_path_for_log(current_path_str)
                                                                            logger.debug(f"后台扫描任务：跳过文件错误 {file_path_display}: {str(file_err)}")
                                                                        except Exception:
                                                                            logger.debug(f"后台扫描任务：跳过文件错误: {str(file_err)}")
                                                                        continue
                                                            except (OSError, PermissionError) as entry_err:
                                                                # 无法判断类型，尝试作为文件处理
                                                                try:
                                                                    if entry_path.is_file():
                                                                        stat = entry_path.stat()
                                                                        file_size = stat.st_size
                                                                        batch_files += 1
                                                                        batch_bytes += file_size
                                                                        file_count += 1
                                                                    elif entry_path.is_dir():
                                                                        dirs_to_scan.append(entry_path)
                                                                except Exception:
                                                                    permission_error_count += 1
                                                                    if permission_error_count <= 20:
                                                                        logger.debug(f"后台扫描任务：无法访问路径: {format_path_for_log(current_path_str)}，错误: {str(entry_err)}")
                                                                    continue
                                                        except (PermissionError, OSError) as entry_err:
                                                            # 路径权限错误：记录详细路径信息
                                                            permission_error_count += 1
                                                            try:
                                                                path_str = str(entry.path) if hasattr(entry, 'path') else "未知路径"
                                                                path_display = format_path_for_log(path_str)
                                                                if permission_error_count <= 20:  # 只记录前20个权限错误
                                                                    logger.warning(f"后台扫描任务：路径权限错误（路径 #{permission_error_count}）: {path_display}，错误: {str(entry_err)}")
                                                            except Exception:
                                                                logger.warning(f"后台扫描任务：路径权限错误（路径 #{permission_error_count}）: 无法获取路径，错误: {str(entry_err)}")
                                                            continue
                                                        except (FileNotFoundError, IOError) as entry_err:
                                                            # 路径不存在或IO错误：跳过
                                                            continue
                                                        except Exception as entry_err:
                                                            # 记录路径错误但继续扫描
                                                            try:
                                                                path_str = str(entry.path) if hasattr(entry, 'path') else "未知路径"
                                                                path_display = format_path_for_log(path_str)
                                                                logger.debug(f"后台扫描任务：跳过路径错误 {path_display}: {str(entry_err)}")
                                                            except Exception:
                                                                logger.debug(f"后台扫描任务：跳过路径错误: {str(entry_err)}")
                                                            continue
                                            except (PermissionError, OSError) as scan_dir_err:
                                                # 目录权限错误：记录并跳过该目录
                                                permission_error_count += 1
                                                try:
                                                    path_display = format_path_for_log(current_scan_dir_str)
                                                    if permission_error_count <= 20:  # 只记录前20个权限错误
                                                        logger.warning(f"后台扫描任务：目录权限错误（目录 #{permission_error_count}）: {path_display}，错误: {str(scan_dir_err)}")
                                                except Exception:
                                                    logger.warning(f"后台扫描任务：目录权限错误（目录 #{permission_error_count}）: 无法获取路径，错误: {str(scan_dir_err)}")
                                                continue
                                            except (FileNotFoundError, IOError) as scan_dir_err:
                                                # 目录不存在或IO错误：跳过
                                                continue
                                            except Exception as scan_dir_err:
                                                # 记录目录错误但继续扫描
                                                try:
                                                    path_display = format_path_for_log(current_scan_dir_str)
                                                    logger.debug(f"后台扫描任务：跳过目录错误 {path_display}: {str(scan_dir_err)}")
                                                except Exception:
                                                    logger.debug(f"后台扫描任务：跳过目录错误: {str(scan_dir_err)}")
                                                continue
                                        except KeyboardInterrupt:
                                            logger.warning(f"后台扫描任务：遍历目录被中断 {source_path_str}，已扫描 {file_count} 个文件，{dir_count} 个目录")
                                            # 设置停止标志，通知其他部分停止
                                            stop_event.set()
                                            scan_failed = True
                                            scan_error = "用户中断（KeyboardInterrupt）"
                                            break
                                        except Exception as dir_scan_err:
                                            # 目录扫描错误：记录但继续
                                            logger.debug(f"后台扫描任务：目录扫描错误: {str(dir_scan_err)}")
                                            continue
                                    
                                    # 如果还有待扫描的目录，记录警告
                                    if dirs_to_scan and not scan_failed:
                                        logger.warning(f"后台扫描任务：还有 {len(dirs_to_scan)} 个目录待扫描，但主循环已退出（目录: {source_path_str}）")
                                    
                                    total_time = time.time() - start_time
                                    logger.info(f"后台扫描任务：目录遍历完成 {source_path_str}，共扫描 {file_count} 个文件，{dir_count} 个目录，权限错误: {permission_error_count} 个，路径过长: {path_too_long_count} 个，总耗时 {total_time:.1f} 秒")
                                except KeyboardInterrupt:
                                    logger.warning(f"后台扫描任务：遍历目录被中断 {source_path_str}，已扫描 {file_count} 个文件，{dir_count} 个目录")
                                    scan_failed = True
                                    scan_error = "用户中断（KeyboardInterrupt）"
                                    raise
                                except Exception as scan_err:
                                    # 扫描过程出错（可能是文件系统错误、内存不足等）
                                    scan_failed = True
                                    scan_error = str(scan_err)
                                    logger.error(f"后台扫描任务：扫描目录失败 {source_path_str}，已扫描 {file_count} 个文件，{dir_count} 个目录: {scan_error}", exc_info=True)
                                    # 不 raise，继续执行 finally 块提交已扫描的批次
                                
                            except KeyboardInterrupt:
                                logger.warning(f"后台扫描任务：线程被中断 {source_path_str}，已扫描 {file_count} 个文件")
                                scan_failed = True
                                scan_error = "用户中断（KeyboardInterrupt）"
                                # 设置停止标志
                                stop_event.set()
                                raise
                            except Exception as e:
                                # 记录所有其他异常，包括完整的堆栈跟踪
                                scan_failed = True
                                scan_error = str(e)
                                logger.error(f"后台扫描任务：遍历目录失败 {source_path_str}，已扫描 {file_count} 个文件: {scan_error}", exc_info=True)
                            finally:
                                # 重要：无论是否异常，都确保发送信号到队列，让主循环知道线程已退出
                                try:
                                    # 提交剩余的批次
                                    if batch_files > 0:
                                        try:
                                            # 使用主事件循环（在启动线程前获取的）
                                            asyncio.run_coroutine_threadsafe(
                                                scan_queue.put((batch_files, batch_bytes)),
                                                main_loop
                                            )
                                            logger.info(f"后台扫描任务：提交剩余批次到队列，文件数={batch_files}, 字节数={format_bytes(batch_bytes)}（目录: {source_path_str}）")
                                        except Exception as e:
                                            logger.warning(f"后台扫描任务：提交剩余批次失败: {str(e)}")
                                    
                                    # 发送完成/退出信号
                                    # 如果扫描失败，发送错误信号；否则发送完成信号
                                    if scan_failed:
                                        # 发送错误信号：使用特殊标记 ('ERROR', error_message, file_count)
                                        try:
                                            asyncio.run_coroutine_threadsafe(
                                                scan_queue.put(('ERROR', scan_error, file_count)),
                                                main_loop
                                            )
                                            logger.warning(f"后台扫描任务：发送错误信号到队列（目录: {source_path_str}），错误: {scan_error}，已扫描: {file_count} 个文件")
                                        except Exception as e:
                                            logger.error(f"后台扫描任务：发送错误信号失败: {str(e)}", exc_info=True)
                                    else:
                                        # 发送完成信号：None 表示正常完成
                                        try:
                                            asyncio.run_coroutine_threadsafe(
                                                scan_queue.put(None),  # None 表示正常完成
                                                main_loop
                                            )
                                            logger.info(f"后台扫描任务：发送完成信号到队列（目录: {source_path_str}），共扫描 {file_count} 个文件")
                                        except Exception as e:
                                            logger.error(f"后台扫描任务：发送完成信号失败: {str(e)}", exc_info=True)
                                except Exception as finally_err:
                                    # finally 块中的异常不应该阻止信号发送，但需要记录
                                    logger.error(f"后台扫描任务：finally 块中发生异常（目录: {source_path_str}）: {str(finally_err)}", exc_info=True)
                                    # 尝试最后一次发送信号（使用最简单的方式）
                                    try:
                                        asyncio.run_coroutine_threadsafe(
                                            scan_queue.put(('ERROR', f"finally块异常: {str(finally_err)}", file_count)),
                                            main_loop
                                        )
                                    except Exception:
                                        pass
                        
                        # 启动扫描任务
                        logger.info(f"后台扫描任务：启动后台线程扫描目录 {source_path_str}")
                        scan_task = asyncio.create_task(asyncio.to_thread(sync_scan_worker))
                        scan_start_time = time.time()  # 记录扫描开始时间
                        last_heartbeat_time = time.time()  # 上次心跳时间
                        
                        # 从队列中获取批次并更新统计
                        logger.info(f"后台扫描任务：开始从队列获取批次并更新统计（目录: {source_path_str}）")
                        batch_received_count = 0
                        timeout_count = 0  # 超时计数
                        last_timeout_log_time = None  # 上次超时日志时间
                        try:
                            while True:
                                try:
                                    # 检查任务是否被取消（Ctrl+C）
                                    try:
                                        current_task = asyncio.current_task()
                                        if current_task and current_task.cancelled():
                                            logger.warning("后台扫描任务：检测到任务已被取消（Ctrl+C）")
                                            # 设置停止标志，通知后台线程停止
                                            stop_event.set()
                                            # 取消后台扫描任务
                                            if not scan_task.done():
                                                scan_task.cancel()
                                            break
                                    except RuntimeError:
                                        # 如果没有当前任务，可能已经被取消
                                        logger.warning("后台扫描任务：检测到任务可能已被取消")
                                        # 设置停止标志，通知后台线程停止
                                        stop_event.set()
                                        if not scan_task.done():
                                            scan_task.cancel()
                                        break
                                    
                                    # 检查停止标志（在主循环中也要检查）
                                    if stop_event.is_set():
                                        logger.warning("后台扫描任务：检测到停止信号，退出主循环")
                                        if not scan_task.done():
                                            scan_task.cancel()
                                        break
                                    
                                    # 等待批次或完成信号（带超时，避免无限等待）
                                    try:
                                        item = await asyncio.wait_for(scan_queue.get(), timeout=1200.0)  # 超时时间：20分钟（1200秒）
                                        timeout_count = 0  # 收到数据，重置超时计数
                                    except asyncio.CancelledError:
                                        # 任务被取消（Ctrl+C）
                                        logger.warning("后台扫描任务：任务被取消（CancelledError）")
                                        # 设置停止标志，通知后台线程停止
                                        stop_event.set()
                                        if not scan_task.done():
                                            scan_task.cancel()
                                        raise
                                    except asyncio.TimeoutError:
                                        timeout_count += 1
                                        # 超时检查扫描任务是否完成
                                        if scan_task.done():
                                            logger.info(f"后台扫描任务：扫描任务已完成，获取队列中剩余的批次（目录: {source_path_str}）")
                                            # 尝试获取队列中剩余的所有批次
                                            remaining_count = 0
                                            while not scan_queue.empty():
                                                try:
                                                    item = scan_queue.get_nowait()
                                                    if item is None:
                                                        break
                                                    batch_files, batch_bytes = item
                                                    total_files += batch_files
                                                    total_bytes += batch_bytes
                                                    remaining_count += 1
                                                    # 更新数据库
                                                    await self.backup_db.update_scan_progress_only(backup_task, total_files, total_bytes)
                                                    logger.info(f"后台扫描任务：已扫描 {total_files} 个文件，总大小 {format_bytes(total_bytes)}（目录: {source_path_str}，剩余批次: {remaining_count}）")
                                                except asyncio.QueueEmpty:
                                                    break
                                            if remaining_count > 0:
                                                logger.info(f"后台扫描任务：已处理 {remaining_count} 个剩余批次（目录: {source_path_str}）")
                                            break
                                        else:
                                            # 如果扫描任务还在运行，继续等待（输出调试信息）
                                            # 每20分钟输出一次日志，避免日志过多，同时确保知道任务还在运行
                                            current_time = time.time()
                                            elapsed_since_start = current_time - scan_start_time
                                            elapsed_since_last_heartbeat = current_time - last_heartbeat_time
                                            
                                            # 检查任务是否真的在运行（如果任务已完成但没有发送完成信号，可能是异常退出）
                                            if scan_task.done():
                                                # 任务已完成，但可能没有发送完成信号（异常退出）
                                                logger.warning(f"后台扫描任务：扫描任务已完成但未发送完成信号（目录: {source_path_str}），已接收批次: {batch_received_count}，耗时: {elapsed_since_start:.1f} 秒")
                                                # 尝试获取任务异常
                                                try:
                                                    exception = scan_task.exception()
                                                    if exception:
                                                        logger.error(f"后台扫描任务：扫描任务异常（目录: {source_path_str}）: {str(exception)}", exc_info=True)
                                                except Exception:
                                                    pass
                                                # 获取队列中剩余的所有批次
                                                remaining_count = 0
                                                while not scan_queue.empty():
                                                    try:
                                                        item = scan_queue.get_nowait()
                                                        if item is None:
                                                            continue
                                                        batch_files, batch_bytes = item
                                                        total_files += batch_files
                                                        total_bytes += batch_bytes
                                                        remaining_count += 1
                                                        await self.backup_db.update_scan_progress_only(backup_task, total_files, total_bytes)
                                                        logger.info(f"后台扫描任务：已扫描 {total_files} 个文件，总大小 {format_bytes(total_bytes)}（目录: {source_path_str}，剩余批次: {remaining_count}）")
                                                    except asyncio.QueueEmpty:
                                                        break
                                                if remaining_count > 0:
                                                    logger.info(f"后台扫描任务：已处理 {remaining_count} 个剩余批次（目录: {source_path_str}）")
                                                break
                                            
                                            # 每20分钟输出一次心跳日志（与超时时间一致）
                                            if last_timeout_log_time is None or current_time - last_timeout_log_time >= 1200.0:
                                                logger.info(f"后台扫描任务：等待批次中...（目录: {source_path_str}，已接收批次: {batch_received_count}，超时次数: {timeout_count}，已运行: {elapsed_since_start:.1f} 秒，扫描任务状态: 运行中）")
                                                last_timeout_log_time = current_time
                                                last_heartbeat_time = current_time
                                            
                                            # 如果超过30分钟没有收到批次，输出警告（可能线程卡住了）
                                            if elapsed_since_last_heartbeat > 1800.0:
                                                logger.warning(f"后台扫描任务：超过30分钟没有收到批次（目录: {source_path_str}），已接收批次: {batch_received_count}，耗时: {elapsed_since_start:.1f} 秒")
                                                last_heartbeat_time = current_time
                                            
                                            continue
                                    
                                    # 成功获取 item，检查是否是完成信号或错误信号（只有在成功获取 item 时才执行）
                                    if item is None:
                                        # 正常完成信号
                                        logger.info(f"后台扫描任务：收到完成信号，获取队列中剩余的批次（目录: {source_path_str}）")
                                        # 尝试获取队列中剩余的所有批次
                                        remaining_count = 0
                                        while not scan_queue.empty():
                                            try:
                                                batch_item = scan_queue.get_nowait()
                                                if batch_item is None:
                                                    continue
                                                # 检查是否是错误信号
                                                if isinstance(batch_item, tuple) and len(batch_item) >= 2 and batch_item[0] == 'ERROR':
                                                    error_msg, error_file_count = batch_item[1], batch_item[2] if len(batch_item) > 2 else 0
                                                    logger.error(f"后台扫描任务：队列中发现错误信号（目录: {source_path_str}），错误: {error_msg}，已扫描: {error_file_count} 个文件")
                                                    break
                                                batch_files, batch_bytes = batch_item
                                                total_files += batch_files
                                                total_bytes += batch_bytes
                                                remaining_count += 1
                                                # 更新数据库
                                                await self.backup_db.update_scan_progress_only(backup_task, total_files, total_bytes)
                                                logger.info(f"后台扫描任务：已扫描 {total_files} 个文件，总大小 {format_bytes(total_bytes)}（目录: {source_path_str}，剩余批次: {remaining_count}）")
                                            except asyncio.QueueEmpty:
                                                break
                                        if remaining_count > 0:
                                            logger.info(f"后台扫描任务：已处理 {remaining_count} 个剩余批次（目录: {source_path_str}）")
                                        break
                                    elif isinstance(item, tuple) and len(item) >= 2 and item[0] == 'ERROR':
                                        # 错误信号：('ERROR', error_message, file_count)
                                        error_msg = item[1]
                                        error_file_count = item[2] if len(item) > 2 else total_files
                                        logger.error(f"后台扫描任务：收到错误信号（目录: {source_path_str}），错误: {error_msg}，已扫描: {error_file_count} 个文件")
                                        # 更新数据库（使用已扫描的文件数和字节数）
                                        if total_files > 0:
                                            await self.backup_db.update_scan_progress_only(backup_task, total_files, total_bytes)
                                            logger.info(f"后台扫描任务：已更新数据库，累计 {total_files} 个文件，总大小 {format_bytes(total_bytes)}（目录: {source_path_str}）")
                                        # 获取队列中剩余的所有批次（如果有）
                                        remaining_count = 0
                                        while not scan_queue.empty():
                                            try:
                                                batch_item = scan_queue.get_nowait()
                                                if batch_item is None or (isinstance(batch_item, tuple) and len(batch_item) >= 2 and batch_item[0] == 'ERROR'):
                                                    continue
                                                batch_files, batch_bytes = batch_item
                                                total_files += batch_files
                                                total_bytes += batch_bytes
                                                remaining_count += 1
                                                await self.backup_db.update_scan_progress_only(backup_task, total_files, total_bytes)
                                                logger.info(f"后台扫描任务：已扫描 {total_files} 个文件，总大小 {format_bytes(total_bytes)}（目录: {source_path_str}，剩余批次: {remaining_count}）")
                                            except asyncio.QueueEmpty:
                                                break
                                        if remaining_count > 0:
                                            logger.info(f"后台扫描任务：已处理 {remaining_count} 个剩余批次（目录: {source_path_str}）")
                                        # 错误信号也意味着扫描任务结束
                                        break
                                    else:
                                        # 处理正常批次
                                        try:
                                            batch_files, batch_bytes = item
                                            total_files += batch_files
                                            total_bytes += batch_bytes
                                            batch_received_count += 1
                                            
                                            # 更新数据库
                                            await self.backup_db.update_scan_progress_only(backup_task, total_files, total_bytes)
                                            logger.info(f"后台扫描任务：已扫描 {total_files} 个文件，总大小 {format_bytes(total_bytes)}（目录: {source_path_str}，批次: {batch_received_count}）")
                                        except (ValueError, TypeError) as batch_err:
                                            # 批次数据格式错误（可能是未知的信号类型）
                                            logger.error(f"后台扫描任务：批次数据格式错误（目录: {source_path_str}）: {str(batch_err)}, 数据: {item}")
                                            # 检查是否是错误信号（但没有正确识别）
                                            if isinstance(item, tuple) and len(item) >= 2 and item[0] == 'ERROR':
                                                error_msg = item[1]
                                                error_file_count = item[2] if len(item) > 2 else total_files
                                                logger.error(f"后台扫描任务：检测到错误信号（目录: {source_path_str}），错误: {error_msg}，已扫描: {error_file_count} 个文件")
                                                if total_files > 0:
                                                    await self.backup_db.update_scan_progress_only(backup_task, total_files, total_bytes)
                                                break
                                            # 检查扫描任务是否已完成
                                            if scan_task.done():
                                                logger.info(f"后台扫描任务：扫描任务已完成，退出循环（目录: {source_path_str}）")
                                                break
                                            continue
                                        
                                except asyncio.CancelledError:
                                    # 任务被取消（Ctrl+C）
                                    logger.warning("后台扫描任务：任务被取消（CancelledError）")
                                    if not scan_task.done():
                                        scan_task.cancel()
                                    raise
                                except Exception as e:
                                    logger.error(f"后台扫描任务：处理批次失败（目录: {source_path_str}）: {str(e)}", exc_info=True)
                                    # 检查扫描任务状态
                                    if scan_task.done():
                                        logger.info(f"后台扫描任务：扫描任务已完成，退出循环（目录: {source_path_str}）")
                                        # 尝试获取任务异常
                                        try:
                                            exception = scan_task.exception()
                                            if exception:
                                                logger.error(f"后台扫描任务：扫描任务异常（目录: {source_path_str}）: {str(exception)}", exc_info=True)
                                        except Exception:
                                            pass
                                        break
                                    # 如果任务还在运行，继续等待
                                    continue
                                    
                        except KeyboardInterrupt:
                            # 用户中断（Ctrl+C）
                            logger.warning("后台扫描任务：用户中断（KeyboardInterrupt）")
                            # 设置停止标志，通知后台线程停止
                            stop_event.set()
                            if not scan_task.done():
                                scan_task.cancel()
                            raise
                        
                        # 确保扫描任务完成
                        try:
                            await scan_task
                            logger.info(f"后台扫描任务：扫描任务正常完成（目录: {source_path_str}）")
                        except Exception as task_err:
                            logger.error(f"后台扫描任务：扫描任务异常退出（目录: {source_path_str}）: {str(task_err)}", exc_info=True)
                        
                        # 检查任务是否有异常
                        if scan_task.done():
                            try:
                                exception = scan_task.exception()
                                if exception:
                                    logger.error(f"后台扫描任务：扫描任务内部异常（目录: {source_path_str}）: {str(exception)}", exc_info=True)
                            except Exception:
                                pass
                        
                        # 扫描完一个目录后，最后更新一次数据库
                        if total_files > 0:
                            await self.backup_db.update_scan_progress_only(backup_task, total_files, total_bytes)
                            logger.info(f"后台扫描任务：目录扫描完成 {source_path_str}，累计 {total_files} 个文件，总大小 {format_bytes(total_bytes)}")
                        else:
                            logger.warning(f"后台扫描任务：目录扫描完成但没有扫描到文件（目录: {source_path_str}）")
                            
                except Exception as e:
                    logger.error(f"后台扫描任务：处理源路径失败 {source_path_str}: {str(e)}")
                    continue
            
            # 扫描完成，最后一次更新数据库
            if total_files > 0:
                await self.backup_db.update_scan_progress_only(backup_task, total_files, total_bytes)
                logger.info(f"========== 后台扫描任务完成：共扫描 {total_files} 个文件，总大小 {format_bytes(total_bytes)} ==========")
            else:
                logger.info("========== 后台扫描任务完成：未扫描到文件 ==========")
        
        except KeyboardInterrupt:
            # 用户按 Ctrl+C 中止任务
            logger.warning("========== 后台扫描任务被用户中止（Ctrl+C） ==========")
            logger.warning(f"任务ID: {backup_task.id if backup_task else 'N/A'}")
            # 即使被中止，也更新已扫描的文件数和字节数
            if 'total_files' in locals() and total_files > 0:
                try:
                    await self.backup_db.update_scan_progress_only(backup_task, total_files, total_bytes)
                    logger.info(f"后台扫描任务：已更新已扫描的文件数 {total_files} 和总大小 {format_bytes(total_bytes)}")
                except Exception as update_error:
                    logger.error(f"更新扫描进度失败: {str(update_error)}")
            # 重新抛出 KeyboardInterrupt，让上层处理
            raise
            
        except asyncio.CancelledError:
            # 任务被取消
            logger.warning("========== 后台扫描任务被取消 ==========")
            logger.warning(f"任务ID: {backup_task.id if backup_task else 'N/A'}")
            # 即使被取消，也更新已扫描的文件数和字节数
            if 'total_files' in locals() and total_files > 0:
                try:
                    await self.backup_db.update_scan_progress_only(backup_task, total_files, total_bytes)
                    logger.info(f"后台扫描任务：已更新已扫描的文件数 {total_files} 和总大小 {format_bytes(total_bytes)}")
                except Exception as update_error:
                    logger.error(f"更新扫描进度失败: {str(update_error)}")
            # 重新抛出 CancelledError，让上层处理
            raise
        
        except Exception as e:
            logger.error(f"后台扫描任务失败: {str(e)}")
            import traceback
            logger.error(f"错误堆栈:\n{traceback.format_exc()}")
            # 即使失败，也尝试更新已扫描的文件数和字节数
            if 'total_files' in locals() and total_files > 0:
                try:
                    await self.backup_db.update_scan_progress_only(backup_task, total_files, total_bytes)
                    logger.info(f"后台扫描任务：已更新已扫描的文件数 {total_files} 和总大小 {format_bytes(total_bytes)}")
                except Exception as update_error:
                    logger.error(f"更新扫描进度失败: {str(update_error)}")
            if backup_task and backup_task.id:
                await self.backup_db.update_scan_status(backup_task.id, 'failed')

