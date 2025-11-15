#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
文件扫描模块
File Scanner Module
"""

import logging
import fnmatch
import asyncio
import os
from datetime import datetime
from pathlib import Path
from typing import List, Dict, Optional, AsyncGenerator, Callable, Awaitable

logger = logging.getLogger(__name__)


class FileScanner:
    """文件扫描器"""
    
    def __init__(self, settings=None, update_progress_callback: Optional[Callable] = None):
        """初始化文件扫描器
        
        Args:
            settings: 系统设置对象
            update_progress_callback: 更新进度回调函数 (backup_task, scanned_count, valid_count, operation_status)
        """
        self.settings = settings
        self.update_progress_callback = update_progress_callback
    
    async def get_file_info(self, file_path: Path) -> Optional[Dict]:
        """获取文件信息
        
        如果遇到权限错误、访问错误等，返回None，调用者应该跳过该文件。
        
        Args:
            file_path: 文件路径
            
        Returns:
            文件信息字典，如果无法访问则返回None
        """
        try:
            stat = file_path.stat()
            return {
                'path': str(file_path),
                'name': file_path.name,
                'size': stat.st_size,
                'modified_time': datetime.fromtimestamp(stat.st_mtime),
                'permissions': oct(stat.st_mode)[-3:],
                'is_file': file_path.is_file(),
                'is_dir': file_path.is_dir(),
                'is_symlink': file_path.is_symlink()
            }
        except (PermissionError, OSError, FileNotFoundError, IOError) as e:
            # 权限错误、访问错误等，返回None，让调用者跳过该文件
            logger.debug(f"无法获取文件信息（权限/访问错误）: {file_path} (错误: {str(e)})")
            return None
        except Exception as e:
            # 其他错误，也返回None
            logger.warning(f"获取文件信息失败 {file_path}: {str(e)}")
            return None
    
    def should_exclude_file(self, file_path: str, exclude_patterns: List[str]) -> bool:
        """检查文件或目录是否应该被排除
        
        排除规则匹配文件路径或其任何父目录路径时，文件/目录都会被排除。
        例如：如果排除规则匹配 "D:\temp"，则 "D:\temp\file.txt" 和 "D:\temp\subdir\file.txt" 都会被排除。
        
        Args:
            file_path: 文件或目录路径
            exclude_patterns: 排除模式列表（从计划任务 action_config 获取）
            
        Returns:
            bool: 如果文件/目录应该被排除返回 True
        """
        if not exclude_patterns:
            return False
        
        # 将路径标准化（统一使用正斜杠或反斜杠）
        normalized_path = file_path.replace('\\', '/')
        
        # 检查文件/目录路径本身是否匹配排除规则
        for pattern in exclude_patterns:
            normalized_pattern = pattern.replace('\\', '/')
            if fnmatch.fnmatch(normalized_path, normalized_pattern):
                return True
        
        # 检查文件/目录路径的父目录是否匹配排除规则
        # 例如：如果排除规则是 "D:/temp/*"，则 "D:/temp/subdir/file.txt" 应该被排除
        path_parts = normalized_path.split('/')
        for i in range(len(path_parts)):
            # 构建父目录路径（从根目录到当前层级）
            parent_path = '/'.join(path_parts[:i+1])
            if not parent_path:
                continue
            
            for pattern in exclude_patterns:
                normalized_pattern = pattern.replace('\\', '/')
                # 检查父目录路径是否匹配排除规则
                if fnmatch.fnmatch(parent_path, normalized_pattern):
                    return True
                # 检查父目录路径是否匹配通配符模式（如 "D:/temp/*"）
                if fnmatch.fnmatch(parent_path + '/*', normalized_pattern):
                    return True
        
        return False
    
    async def scan_source_files_streaming(
        self, 
        source_paths: List[str], 
        exclude_patterns: List[str], 
        backup_task: Optional[object] = None,
        batch_size: int = 100,
        log_context: Optional[str] = None
    ) -> AsyncGenerator[List[Dict], None]:
        """流式扫描源文件（异步生成器，分批返回文件）
        
        支持网络路径（UNC路径）：
        - \\192.168.0.79\yz - 指定共享路径
        - 自动处理 UNC 路径的文件和目录扫描
        
        Args:
            source_paths: 源路径列表
            exclude_patterns: 排除模式列表
            backup_task: 备份任务对象（可选，用于进度更新）
            batch_size: 每批返回的文件数
            log_context: 日志上下文前缀（用于区分调用场景）
            
        Yields:
            List[Dict]: 每批文件列表
        """
        if not source_paths:
            logger.warning("源路径列表为空")
            return
        
        context_prefix = f"{log_context} " if log_context else ""
        
        # 不再估算总文件数，直接使用后台扫描任务提供的 total_files
        # 后台扫描任务会独立扫描并更新数据库中的 total_files 和 total_bytes
        # 这样可以避免重复扫描，特别是对于包含大量目录的场景（如40.5万个目录）
        
        total_scanned = 0
        total_scanned_size = 0  # 所有扫描到的文件总字节数
        current_batch = []
        total_valid_files = 0  # 累计的有效文件总数
        
        for idx, source_path_str in enumerate(source_paths):
            logger.info(f"扫描源路径 {idx + 1}/{len(source_paths)}: {source_path_str}")
            
            # 处理 UNC 网络路径
            from utils.network_path import is_unc_path, normalize_unc_path
            if is_unc_path(source_path_str):
                # UNC 路径需要使用规范化后的路径
                normalized_path = normalize_unc_path(source_path_str)
                source_path = Path(normalized_path)
                logger.debug(f"检测到 UNC 路径，规范化后: {normalized_path}")
            else:
                source_path = Path(source_path_str)
            
            if not source_path.exists():
                logger.warning(f"源路径不存在，跳过: {source_path_str}")
                continue
            
            try:
                if source_path.is_file():
                    logger.debug(f"扫描文件: {source_path_str}")
                    try:
                        file_info = await self.get_file_info(source_path)
                        if file_info:
                            # 排除规则从计划任务获取（scheduled_task.action_config.exclude_patterns）
                            if not self.should_exclude_file(file_info['path'], exclude_patterns):
                                current_batch.append(file_info)
                                total_valid_files += 1  # 累计有效文件数
                                total_scanned_size += file_info['size']
                                # 注意：不再在这里更新 total_bytes_actual
                                # 这些统计由独立的后台扫描任务 _scan_for_progress_update 负责更新
                        total_scanned += 1
                        
                        # 更新扫描进度（使用后台扫描任务提供的 total_files，如果没有则只更新状态）
                        # 后台扫描任务会独立扫描并更新数据库中的 total_files，这里只需要更新已扫描的文件数
                        if backup_task and self.update_progress_callback:
                            # 从数据库读取最新的 total_files（由后台扫描任务更新）
                            if hasattr(backup_task, 'total_files') and backup_task.total_files and backup_task.total_files > 0:
                                # 使用后台扫描任务提供的 total_files 计算进度
                                scan_progress = min(10.0, (total_scanned / backup_task.total_files) * 10.0)
                                backup_task.progress_percent = scan_progress
                            else:
                                # 如果后台扫描任务还没有更新 total_files，只更新状态，不显示具体进度
                                backup_task.progress_percent = 0.0
                            await self.update_progress_callback(backup_task, total_scanned, len(current_batch), "[扫描文件中...]")
                        
                        # 达到批次大小，yield当前批次
                        if len(current_batch) >= batch_size:
                            yield current_batch
                            current_batch = []
                    except (PermissionError, OSError, FileNotFoundError, IOError) as file_error:
                        # 文件访问错误，跳过该文件，继续扫描
                        logger.warning(f"⚠️ 跳过无法访问的文件: {source_path_str} (错误: {str(file_error)})")
                        continue
                    except Exception as file_error:
                        # 其他错误，也跳过该文件
                        logger.warning(f"⚠️ 跳过出错的文件: {source_path_str} (错误: {str(file_error)})")
                        continue
                        
                elif source_path.is_dir():
                    logger.info(f"扫描目录: {source_path_str}")
                    
                    # 检查目录本身是否匹配排除规则
                    if self.should_exclude_file(str(source_path), exclude_patterns):
                        logger.info(f"目录匹配排除规则，跳过整个目录: {source_path_str}")
                        continue
                    
                    scanned_count = 0
                    excluded_count = 0
                    skipped_dirs = 0
                    error_count = 0
                    error_paths = []  # 记录出错的路径
                    
                    try:
                        # 异步生成器遍历目录（逐步yield，避免一次性加载所有路径）
                        async def async_rglob_generator(path: Path):
                            """异步递归遍历目录生成器（逐步yield路径，避免阻塞）"""
                            # 使用无限制队列来缓冲路径（处理海量文件）
                            path_queue = asyncio.Queue(maxsize=0)  # maxsize=0 表示无限制
                            stop_signal = object()  # 停止信号
                            scan_done = False
                            total_paths_scanned = 0  # 总路径数（用于统计）
                            
                            def sync_rglob_worker():
                                """在线程池中执行同步遍历，将路径放入队列
                                
                                职责：专注于遍历整个目录树，根据目录数量智能匹配批次阈值（10、25、50、100个路径）提交一次，或者每20分钟强制提交一次，中间不停，直到扫描完成
                                """
                                nonlocal scan_done, total_paths_scanned
                                import time
                                
                                # 防错机制配置
                                BATCH_FORCE_INTERVAL = 1200.0  # 强制提交批次的时间间隔（秒）- 20分钟
                                PROGRESS_LOG_INTERVAL = 60.0  # 进度日志输出间隔（秒）- 1分钟
                                DIR_LOG_INTERVAL = 120.0  # 目录日志输出间隔（秒）- 2分钟
                                MAX_PATH_LENGTH = 260  # Windows路径最大长度（字符）
                                MAX_PATH_DISPLAY = 200  # 日志中显示的最大路径长度（字符）
                                
                                def get_batch_threshold(dir_count: int) -> int:
                                    """根据目录数量智能匹配批次阈值
                                    
                                    Args:
                                        dir_count: 待扫描目录数量
                                    
                                    Returns:
                                        int: 批次阈值（路径数）
                                    """
                                    if dir_count >= 50000:
                                        # 目录数量很多（>50000），使用10个路径作为批次阈值
                                        return 300
                                    elif dir_count >= 10000:
                                        # 目录数量较多（10000-50000），使用25个路径作为批次阈值
                                        return 500
                                    elif dir_count >= 1000:
                                        # 目录数量中等（1000-10000），使用50个路径作为批次阈值
                                        return 800
                                    else:
                                        # 目录数量少（<1000），使用100个路径作为批次阈值
                                        return 1000
                                
                                # 统计变量
                                dir_count = 0  # 目录计数
                                permission_error_count = 0  # 权限错误计数
                                path_too_long_count = 0  # 路径过长错误计数
                                last_batch_submit_time = None  # 上次提交批次的时间
                                last_progress_log_time = None  # 上次输出进度日志的时间
                                last_dir_log_time = None  # 上次输出目录日志的时间
                                last_log_count = 0  # 上次输出日志的路径数
                                current_dir = None  # 当前正在扫描的目录
                                
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
                                
                                # 用于检测取消的标志（在主线程中设置）
                                scan_cancelled = False
                                scan_error_info = None
                                
                                try:
                                    start_time = time.time()
                                    last_batch_submit_time = start_time
                                    last_progress_log_time = start_time
                                    last_dir_log_time = start_time
                                    
                                    # 获取主事件循环（在启动线程前获取）
                                    try:
                                        main_loop = asyncio.get_running_loop()
                                    except RuntimeError:
                                        main_loop = None
                                    
                                    # 使用 os.scandir() 替代 rglob() 以提高性能（特别是对于大量目录）
                                    # os.scandir() 在 Windows 上比 rglob() 更快，且内存占用更少
                                    batch = []
                                    dirs_to_scan = [path]  # 待扫描的目录队列
                                    scanned_dirs = set()  # 已扫描的目录集合（避免重复扫描）
                                    
                                    # 对于大量目录，调整日志频率
                                    LARGE_DIR_THRESHOLD = 10000  # 超过1万个目录时，使用更频繁的日志
                                    is_large_dir_structure = False
                                    
                                    try:
                                        # 使用迭代方式遍历目录（避免 rglob 的内存问题）
                                        while dirs_to_scan:
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
                                                    logger.info(f"{context_prefix}流式扫描：检测到大型目录结构（已扫描 {dir_count} 个目录，待扫描目录: {len(dirs_to_scan)}），批次阈值: {current_batch_threshold}，将使用更频繁的进度日志（源路径: {path}）")
                                                
                                                # 每2分钟输出一次当前扫描的目录（大型目录结构时每30秒输出一次）
                                                current_time = time.time()
                                                elapsed_since_dir_log = current_time - last_dir_log_time
                                                dir_log_interval = 30.0 if is_large_dir_structure else DIR_LOG_INTERVAL
                                                if current_dir and elapsed_since_dir_log >= dir_log_interval:
                                                    logger.info(f"{context_prefix}流式扫描：正在扫描目录 {format_path_for_log(current_dir)}，已扫描 {dir_count} 个目录，{total_paths_scanned} 个路径（源路径: {path}）")
                                                    last_dir_log_time = current_time
                                                
                                                try:
                                                    # 使用 os.scandir() 扫描当前目录
                                                    with os.scandir(current_scan_dir_str) as entries:
                                                        for entry in entries:
                                                            try:
                                                                # 检查是否被取消
                                                                if scan_cancelled:
                                                                    break
                                                                
                                                                # 获取路径
                                                                try:
                                                                    entry_path = Path(entry.path)
                                                                    current_path_str = str(entry_path)
                                                                    path_len = len(current_path_str)
                                                                    
                                                                    # 检查路径长度
                                                                    if path_len > MAX_PATH_LENGTH:
                                                                        path_too_long_count += 1
                                                                        if path_too_long_count <= 10:  # 只记录前10个路径过长的情况
                                                                            logger.warning(f"{context_prefix}流式扫描：路径过长（{path_len} 字符 > {MAX_PATH_LENGTH} 字符）: {format_path_for_log(current_path_str)}")
                                                                        continue
                                                                except Exception as path_str_err:
                                                                    # 路径字符串化失败（可能是编码问题）
                                                                    logger.debug(f"{context_prefix}流式扫描：路径字符串化失败: {str(path_str_err)}")
                                                                    continue
                                                                
                                                                # 处理目录和文件
                                                                try:
                                                                    if entry.is_dir(follow_symlinks=False):
                                                                        # 目录：添加到待扫描队列
                                                                        dirs_to_scan.append(entry_path)
                                                                    elif entry.is_file(follow_symlinks=False):
                                                                        # 文件：添加到批次
                                                                        batch.append(entry_path)
                                                                        total_paths_scanned += 1
                                                                except (OSError, PermissionError) as entry_err:
                                                                    # 无法判断类型，尝试作为文件处理
                                                                    try:
                                                                        if entry_path.is_file():
                                                                            batch.append(entry_path)
                                                                            total_paths_scanned += 1
                                                                        elif entry_path.is_dir():
                                                                            dirs_to_scan.append(entry_path)
                                                                    except Exception:
                                                                        permission_error_count += 1
                                                                        if permission_error_count <= 20:
                                                                            logger.debug(f"{context_prefix}流式扫描：无法访问路径: {format_path_for_log(current_path_str)}，错误: {str(entry_err)}")
                                                                        continue
                                                                
                                                                # 检查是否需要强制提交批次（即使没有达到阈值，也要定期提交）
                                                                current_time = time.time()
                                                                elapsed_since_last_batch = current_time - last_batch_submit_time
                                                                # 根据待扫描目录数量智能匹配批次阈值
                                                                batch_threshold = get_batch_threshold(len(dirs_to_scan))
                                                                
                                                                if len(batch) > 0 and elapsed_since_last_batch >= BATCH_FORCE_INTERVAL:
                                                                    # 强制提交当前批次（即使没有达到阈值，每20分钟强制提交一次）
                                                                    try:
                                                                        if main_loop:
                                                                            future = asyncio.run_coroutine_threadsafe(
                                                                                path_queue.put(batch.copy()),
                                                                                main_loop
                                                                            )
                                                                            future.result(timeout=10.0)
                                                                        else:
                                                                            asyncio.run(path_queue.put(batch.copy()))
                                                                        current_dir_display = format_path_for_log(current_dir) if current_dir else "未知"
                                                                        logger.info(f"{context_prefix}流式扫描：强制提交批次到队列（超过{BATCH_FORCE_INTERVAL}秒），路径数={len(batch)}，累计已扫描={total_paths_scanned}个路径，{dir_count}个目录，待扫描目录: {len(dirs_to_scan)}，批次阈值: {batch_threshold}，当前目录: {current_dir_display}（源路径: {path}）")
                                                                    except Exception as e:
                                                                        logger.error(f"{context_prefix}流式扫描：强制提交批次失败: {str(e)}", exc_info=True)
                                                                    batch = []
                                                                    last_batch_submit_time = current_time
                                                                
                                                                # 根据目录数量智能匹配批次阈值提交批次（正常提交）
                                                                elif len(batch) >= batch_threshold:
                                                                    try:
                                                                        if main_loop:
                                                                            future = asyncio.run_coroutine_threadsafe(
                                                                                path_queue.put(batch.copy()),
                                                                                main_loop
                                                                            )
                                                                            future.result(timeout=10.0)
                                                                        else:
                                                                            asyncio.run(path_queue.put(batch.copy()))
                                                                    except Exception as e:
                                                                        logger.warning(f"{context_prefix}流式扫描：放入路径队列失败: {str(e)}")
                                                                    batch = []
                                                                    last_batch_submit_time = current_time
                                                                
                                                                # 每10000个路径或每1分钟输出一次进度日志（大型目录结构时每30秒输出一次）
                                                                elapsed_since_last_progress = current_time - last_progress_log_time
                                                                progress_log_interval = 30.0 if is_large_dir_structure else PROGRESS_LOG_INTERVAL
                                                                if total_paths_scanned - last_log_count >= 10000 or elapsed_since_last_progress >= progress_log_interval:
                                                                    elapsed = current_time - start_time if start_time else 0
                                                                    rate = (total_paths_scanned - last_log_count) / elapsed_since_last_progress if elapsed_since_last_progress > 0 else 0
                                                                    current_dir_display = format_path_for_log(current_dir) if current_dir else "未知"
                                                                    logger.info(f"{context_prefix}流式扫描：已扫描 {total_paths_scanned} 个路径（包含目录），{dir_count} 个目录，待扫描目录: {len(dirs_to_scan)}，批次阈值: {batch_threshold}，当前批次 {len(batch)} 个路径，耗时 {elapsed:.1f} 秒，速度 {rate:.0f} 路径/秒，距上次提交 {elapsed_since_last_batch:.1f} 秒，当前目录: {current_dir_display}，权限错误: {permission_error_count}，路径过长: {path_too_long_count}（线程运行中）")
                                                                    last_log_count = total_paths_scanned
                                                                    last_progress_log_time = current_time
                                                                    
                                                            except (PermissionError, OSError) as entry_err:
                                                                # 路径权限错误：记录详细路径信息
                                                                permission_error_count += 1
                                                                try:
                                                                    path_str = str(entry.path) if hasattr(entry, 'path') else "未知路径"
                                                                    path_display = format_path_for_log(path_str)
                                                                    if permission_error_count <= 20:  # 只记录前20个权限错误
                                                                        logger.warning(f"{context_prefix}流式扫描：路径权限错误（路径 #{permission_error_count}）: {path_display}，错误: {str(entry_err)}")
                                                                except Exception:
                                                                    logger.warning(f"{context_prefix}流式扫描：路径权限错误（路径 #{permission_error_count}）: 无法获取路径，错误: {str(entry_err)}")
                                                                continue
                                                            except (FileNotFoundError, IOError) as entry_err:
                                                                # 路径不存在或IO错误：跳过
                                                                continue
                                                            except Exception as entry_err:
                                                                # 记录路径错误但继续扫描
                                                                try:
                                                                    path_str = str(entry.path) if hasattr(entry, 'path') else "未知路径"
                                                                    path_display = format_path_for_log(path_str)
                                                                    logger.debug(f"{context_prefix}流式扫描：跳过路径错误 {path_display}: {str(entry_err)}")
                                                                except Exception:
                                                                    logger.debug(f"{context_prefix}流式扫描：跳过路径错误: {str(entry_err)}")
                                                                continue
                                                except (PermissionError, OSError) as scan_dir_err:
                                                    # 目录权限错误：记录并跳过该目录
                                                    permission_error_count += 1
                                                    try:
                                                        path_display = format_path_for_log(current_scan_dir_str)
                                                        if permission_error_count <= 20:  # 只记录前20个权限错误
                                                            logger.warning(f"{context_prefix}流式扫描：目录权限错误（目录 #{permission_error_count}）: {path_display}，错误: {str(scan_dir_err)}")
                                                    except Exception:
                                                        logger.warning(f"{context_prefix}流式扫描：目录权限错误（目录 #{permission_error_count}）: 无法获取路径，错误: {str(scan_dir_err)}")
                                                    continue
                                                except (FileNotFoundError, IOError) as scan_dir_err:
                                                    # 目录不存在或IO错误：跳过
                                                    continue
                                                except Exception as scan_dir_err:
                                                    # 记录目录错误但继续扫描
                                                    try:
                                                        path_display = format_path_for_log(current_scan_dir_str)
                                                        logger.debug(f"{context_prefix}流式扫描：跳过目录错误 {path_display}: {str(scan_dir_err)}")
                                                    except Exception:
                                                        logger.debug(f"{context_prefix}流式扫描：跳过目录错误: {str(scan_dir_err)}")
                                                    continue
                                                
                                                # 检查是否被取消
                                                if scan_cancelled:
                                                    break
                                                    
                                            except KeyboardInterrupt:
                                                # 在主线程中捕获 KeyboardInterrupt 时，会通过任务取消来通知
                                                # 这里记录但不抛出，继续执行 finally 块
                                                scan_cancelled = True
                                                scan_error_info = "用户中断（KeyboardInterrupt）"
                                                logger.warning(f"{context_prefix}流式扫描：遍历目录被中断 {path}，已扫描 {total_paths_scanned} 个路径，{dir_count} 个目录")
                                                break
                                            except Exception as dir_scan_err:
                                                # 目录扫描错误：记录但继续
                                                logger.debug(f"{context_prefix}流式扫描：目录扫描错误: {str(dir_scan_err)}")
                                                continue
                                        
                                        # 如果还有待扫描的目录，记录警告
                                        if dirs_to_scan and not scan_cancelled:
                                            logger.warning(f"{context_prefix}流式扫描：还有 {len(dirs_to_scan)} 个目录待扫描，但主循环已退出（源路径: {path}）")
                                            
                                    except KeyboardInterrupt:
                                        # 在主线程中捕获 KeyboardInterrupt 时，会通过任务取消来通知
                                        # 这里记录但不抛出，继续执行 finally 块
                                        scan_cancelled = True
                                        scan_error_info = "用户中断（KeyboardInterrupt）"
                                        logger.warning(f"{context_prefix}流式扫描：遍历目录被中断 {path}，已扫描 {total_paths_scanned} 个路径，{dir_count} 个目录")
                                    except Exception as scan_err:
                                        # 扫描过程出错
                                        scan_error_info = str(scan_err)
                                        logger.error(f"{context_prefix}流式扫描：扫描目录失败 {path}，已扫描 {total_paths_scanned} 个路径，{dir_count} 个目录: {scan_error_info}", exc_info=True)
                                    
                                    # 放入剩余的路径
                                    if batch:
                                        try:
                                            if main_loop:
                                                future = asyncio.run_coroutine_threadsafe(
                                                    path_queue.put(batch),
                                                    main_loop
                                                )
                                                future.result(timeout=10.0)
                                            else:
                                                asyncio.run(path_queue.put(batch))
                                        except Exception:
                                            pass
                                    
                                    total_time = time.time() - start_time
                                    if scan_cancelled:
                                        logger.warning(f"{context_prefix}流式扫描：目录遍历被中断 {path}，共扫描 {total_paths_scanned} 个路径，{dir_count} 个目录，权限错误: {permission_error_count} 个，路径过长: {path_too_long_count} 个，总耗时 {total_time:.1f} 秒")
                                    else:
                                        logger.info(f"{context_prefix}流式扫描：目录树遍历完成，共扫描 {total_paths_scanned} 个路径，{dir_count} 个目录，权限错误: {permission_error_count} 个，路径过长: {path_too_long_count} 个，总耗时 {total_time:.1f} 秒")
                                except KeyboardInterrupt:
                                    # 线程级别的 KeyboardInterrupt（很少发生，因为主线程会先捕获）
                                    scan_cancelled = True
                                    scan_error_info = "用户中断（KeyboardInterrupt）"
                                    logger.warning(f"{context_prefix}流式扫描：线程被中断 {path}，已扫描 {total_paths_scanned} 个路径")
                                except Exception as e:
                                    scan_error_info = str(e)
                                    logger.error(f"{context_prefix}流式扫描：遍历目录失败 {path}: {str(e)}", exc_info=True)
                                finally:
                                    # 重要：无论是否异常，都确保发送停止信号到队列，让主循环知道线程已退出
                                    scan_done = True
                                    try:
                                        # 发送停止信号（包含总路径数和取消标志）
                                        try:
                                            if main_loop:
                                                # 如果被取消，发送特殊的停止信号
                                                if scan_cancelled:
                                                    # 发送取消信号
                                                    future = asyncio.run_coroutine_threadsafe(
                                                        path_queue.put(('CANCELLED', scan_error_info, total_paths_scanned)),
                                                        main_loop
                                                    )
                                                    future.result(timeout=10.0)
                                                    logger.warning(f"{context_prefix}流式扫描：发送取消信号到队列（源路径: {path}），错误: {scan_error_info}，已扫描: {total_paths_scanned} 个路径")
                                                else:
                                                    # 正常完成信号
                                                    future = asyncio.run_coroutine_threadsafe(
                                                        path_queue.put((stop_signal, total_paths_scanned)),
                                                        main_loop
                                                    )
                                                    future.result(timeout=10.0)
                                            else:
                                                if scan_cancelled:
                                                    asyncio.run(path_queue.put(('CANCELLED', scan_error_info, total_paths_scanned)))
                                                else:
                                                    asyncio.run(path_queue.put((stop_signal, total_paths_scanned)))
                                        except Exception as signal_err:
                                            logger.error(f"{context_prefix}流式扫描：发送停止信号失败: {str(signal_err)}", exc_info=True)
                                    except Exception as finally_err:
                                        # finally 块中的异常不应该阻止信号发送，但需要记录
                                        logger.error(f"{context_prefix}流式扫描：finally 块中发生异常（源路径: {path}）: {str(finally_err)}", exc_info=True)
                                        # 尝试最后一次发送信号（使用最简单的方式）
                                        try:
                                            if main_loop:
                                                asyncio.run_coroutine_threadsafe(
                                                    path_queue.put(('CANCELLED', f"finally块异常: {str(finally_err)}", total_paths_scanned)),
                                                    main_loop
                                                )
                                        except Exception:
                                            pass
                            
                            # 在线程池中启动遍历任务
                            scan_task = asyncio.create_task(
                                asyncio.to_thread(sync_rglob_worker)
                            )
                            
                            # 从队列中逐步获取路径并yield
                            total_paths_count = 0  # 统计总路径数
                            try:
                                while True:
                                    try:
                                        # 检查任务是否被取消（Ctrl+C）
                                        try:
                                            current_task = asyncio.current_task()
                                            if current_task and current_task.cancelled():
                                                logger.warning(f"{context_prefix}流式扫描循环：检测到任务已被取消（Ctrl+C）")
                                                # 取消后台扫描任务
                                                if not scan_task.done():
                                                    scan_task.cancel()
                                                break
                                        except RuntimeError:
                                            # 如果没有当前任务，可能已经被取消
                                            logger.warning(f"{context_prefix}流式扫描循环：检测到任务可能已被取消")
                                            if not scan_task.done():
                                                scan_task.cancel()
                                            break
                                        
                                        # 等待路径或停止信号（带超时，避免无限等待）- 超时时间：20分钟（1200秒）
                                        try:
                                            item = await asyncio.wait_for(path_queue.get(), timeout=1200.0)
                                        except asyncio.TimeoutError:
                                            # 超时检查扫描任务是否完成
                                            if scan_task.done() or scan_done:
                                                # 尝试获取队列中剩余的所有路径
                                                while not path_queue.empty():
                                                    try:
                                                        item = path_queue.get_nowait()
                                                        # 检查是否是停止信号或取消信号
                                                        if isinstance(item, tuple):
                                                            if item[0] is stop_signal:
                                                                total_paths_count = item[1] if len(item) > 1 else total_paths_count
                                                                break
                                                            elif item[0] == 'CANCELLED':
                                                                # 取消信号
                                                                error_msg = item[1] if len(item) > 1 else "用户中断"
                                                                scanned_count = item[2] if len(item) > 2 else total_paths_count
                                                                logger.warning(f"{context_prefix}流式扫描：收到取消信号，错误: {error_msg}，已扫描: {scanned_count} 个路径")
                                                                break
                                                        elif item is stop_signal:
                                                            break
                                                        # item 是一个路径列表（批次）
                                                        for file_path in item:
                                                            yield file_path
                                                            total_paths_count += 1
                                                    except asyncio.QueueEmpty:
                                                        break
                                                break
                                            # 如果扫描任务还在运行，继续等待
                                            continue
                                        except asyncio.CancelledError:
                                            # 任务被取消（Ctrl+C）
                                            logger.warning(f"{context_prefix}流式扫描循环：任务被取消（CancelledError）")
                                            if not scan_task.done():
                                                scan_task.cancel()
                                            raise
                                        
                                        # 检查是否是停止信号或取消信号（可能是元组格式：(stop_signal, total_paths) 或 ('CANCELLED', error_msg, count)）
                                        if isinstance(item, tuple):
                                            if item[0] is stop_signal:
                                                total_paths_count = item[1] if len(item) > 1 else total_paths_count
                                                break
                                            elif item[0] == 'CANCELLED':
                                                # 取消信号
                                                error_msg = item[1] if len(item) > 1 else "用户中断"
                                                scanned_count = item[2] if len(item) > 2 else total_paths_count
                                                logger.warning(f"{context_prefix}流式扫描：收到取消信号，错误: {error_msg}，已扫描: {scanned_count} 个路径")
                                                # 取消后台扫描任务
                                                if not scan_task.done():
                                                    scan_task.cancel()
                                                break
                                        elif item is stop_signal:
                                            break
                                        
                                        # item 是一个路径列表（批次）
                                        for file_path in item:
                                            yield file_path
                                            total_paths_count += 1
                                            
                                    except asyncio.CancelledError:
                                        # 任务被取消（Ctrl+C）
                                        logger.warning(f"{context_prefix}流式扫描循环：任务被取消（CancelledError）")
                                        if not scan_task.done():
                                            scan_task.cancel()
                                        raise
                                    except Exception as e:
                                        logger.error(f"{context_prefix}流式扫描：处理批次失败: {str(e)}", exc_info=True)
                                        # 检查扫描任务状态
                                        if scan_task.done():
                                            break
                                        continue
                            except asyncio.CancelledError:
                                # 任务被取消（Ctrl+C）
                                logger.warning(f"{context_prefix}流式扫描：任务被取消")
                                if not scan_task.done():
                                    scan_task.cancel()
                                raise
                            except KeyboardInterrupt:
                                # 用户中断
                                logger.warning(f"{context_prefix}流式扫描：用户中断（KeyboardInterrupt）")
                                if not scan_task.done():
                                    scan_task.cancel()
                                raise
                            finally:
                                # 确保扫描任务完成
                                if not scan_task.done():
                                    scan_task.cancel()
                                    try:
                                        await scan_task
                                    except (asyncio.CancelledError, Exception):
                                        pass
                            
                            # 返回总路径数（用于统计）
                            logger.info(f"目录遍历完成，共处理 {total_paths_count} 个路径")
                        
                        # 防错机制配置（与后台扫描任务一致）
                        MAX_PATH_LENGTH = 260  # Windows路径最大长度（字符）
                        MAX_PATH_DISPLAY = 200  # 日志中显示的最大路径长度（字符）
                        path_too_long_count = 0  # 路径过长计数
                        permission_error_count = 0  # 权限错误计数
                        
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
                        
                        # 使用异步生成器逐步遍历目录
                        async for file_path in async_rglob_generator(source_path):
                            try:
                                # 路径长度检查和字符串化
                                file_path_str = None
                                try:
                                    file_path_str = str(file_path)
                                    path_len = len(file_path_str)
                                    
                                    # 检查路径长度（Windows限制260字符）
                                    if path_len > MAX_PATH_LENGTH:
                                        path_too_long_count += 1
                                        if path_too_long_count <= 10:  # 只记录前10个路径过长的情况
                                            logger.warning(f"压缩扫描：路径过长（{path_len} 字符 > {MAX_PATH_LENGTH} 字符）: {format_path_for_log(file_path_str)}")
                                        continue
                                except Exception as path_str_err:
                                    # 路径字符串化失败（可能是编码问题）
                                    logger.debug(f"压缩扫描：路径字符串化失败: {str(path_str_err)}")
                                    continue
                                
                                # 检查文件路径的父目录是否匹配排除规则
                                # 如果父目录匹配，跳过该文件
                                if file_path_str and self.should_exclude_file(file_path_str, exclude_patterns):
                                    skipped_dirs += 1
                                    continue
                                
                                if file_path.is_file():
                                    scanned_count += 1
                                    total_scanned += 1
                                    
                                    # 每扫描10000个文件输出一次进度（避免日志过多）
                                    if scanned_count % 10000 == 0:
                                        logger.info(f"{context_prefix}压缩扫描：已扫描 {scanned_count} 个文件（当前源路径，仅为当前源路径的文件数），找到 {total_valid_files} 个有效文件（当前批次: {len(current_batch)} 个）...")
                                    
                                    # 每扫描50个文件更新一次进度（使用后台扫描任务提供的 total_files）
                                    if total_scanned % 50 == 0 and backup_task and self.update_progress_callback:
                                        # 从数据库读取最新的 total_files（由后台扫描任务更新）
                                        if hasattr(backup_task, 'total_files') and backup_task.total_files and backup_task.total_files > 0:
                                            # 使用后台扫描任务提供的 total_files 计算进度
                                            scan_progress = min(10.0, (total_scanned / backup_task.total_files) * 10.0)
                                            backup_task.progress_percent = scan_progress
                                        else:
                                            # 如果后台扫描任务还没有更新 total_files，只更新状态，不显示具体进度
                                            backup_task.progress_percent = 0.0
                                        await self.update_progress_callback(backup_task, total_scanned, len(current_batch), "[扫描文件中...]")
                                    
                                    try:
                                        file_info = await self.get_file_info(file_path)
                                        if file_info:
                                            path_str = file_info.get('path', '')
                                            if len(path_str) > MAX_PATH_LENGTH:
                                                path_too_long_count += 1
                                                if path_too_long_count <= 20:
                                                    logger.warning(f"压缩扫描：跳过路径过长文件（{len(path_str)} 字符）: {format_path_for_log(path_str)}")
                                                continue
                                            if not self.should_exclude_file(path_str, exclude_patterns):
                                                current_batch.append(file_info)
                                                total_valid_files += 1  # 累计有效文件数
                                                total_scanned_size += file_info['size']
                                                if len(current_batch) >= batch_size:
                                                    yield current_batch
                                                    current_batch = []
                                            else:
                                                excluded_count += 1
                                    except (PermissionError, OSError) as file_error:
                                        # 文件权限错误：记录详细路径信息
                                        permission_error_count += 1
                                        error_count += 1
                                        path_display = format_path_for_log(file_path_str) if file_path_str else "未知路径"
                                        if permission_error_count <= 20:  # 只记录前20个权限错误
                                            logger.warning(f"压缩扫描：权限错误（文件 #{permission_error_count}）: {path_display}，错误: {str(file_error)}")
                                        if len(error_paths) < 50:  # 只记录前50个错误路径
                                            error_paths.append(file_path_str if file_path_str else str(file_path))
                                        continue
                                    except (FileNotFoundError, IOError) as file_error:
                                        # 文件不存在或IO错误：跳过该文件，继续扫描
                                        error_count += 1
                                        path_display = format_path_for_log(file_path_str) if file_path_str else "未知路径"
                                        logger.debug(f"压缩扫描：文件不存在或IO错误: {path_display}，错误: {str(file_error)}")
                                        if len(error_paths) < 50:  # 只记录前50个错误路径
                                            error_paths.append(file_path_str if file_path_str else str(file_path))
                                        continue
                                    except Exception as file_error:
                                        # 其他错误，也跳过该文件
                                        error_count += 1
                                        path_display = format_path_for_log(file_path_str) if file_path_str else "未知路径"
                                        logger.warning(f"压缩扫描：跳过出错的文件: {path_display}，错误: {str(file_error)}")
                                        if len(error_paths) < 50:  # 只记录前50个错误路径
                                            error_paths.append(file_path_str if file_path_str else str(file_path))
                                        continue
                                elif file_path.is_dir():
                                    try:
                                        # 检查目录是否匹配排除规则（使用已字符串化的路径）
                                        if file_path_str and self.should_exclude_file(file_path_str, exclude_patterns):
                                            skipped_dirs += 1
                                            # 跳过该目录下的所有文件（rglob会继续，但我们在文件检查时会跳过）
                                            continue
                                    except (PermissionError, OSError) as dir_error:
                                        # 目录权限错误：记录详细路径信息
                                        permission_error_count += 1
                                        error_count += 1
                                        path_display = format_path_for_log(file_path_str) if file_path_str else "未知路径"
                                        if permission_error_count <= 20:  # 只记录前20个权限错误
                                            logger.warning(f"压缩扫描：目录权限错误（目录 #{permission_error_count}）: {path_display}，错误: {str(dir_error)}")
                                        if len(error_paths) < 50:  # 只记录前50个错误路径
                                            error_paths.append(file_path_str if file_path_str else str(file_path))
                                        continue
                                    except (FileNotFoundError, IOError) as dir_error:
                                        # 目录不存在或IO错误：跳过该目录
                                        error_count += 1
                                        path_display = format_path_for_log(file_path_str) if file_path_str else "未知路径"
                                        logger.debug(f"压缩扫描：目录不存在或IO错误: {path_display}，错误: {str(dir_error)}")
                                        if len(error_paths) < 50:  # 只记录前50个错误路径
                                            error_paths.append(file_path_str if file_path_str else str(file_path))
                                        continue
                            except (PermissionError, OSError) as path_error:
                                # 路径权限错误：记录详细路径信息
                                permission_error_count += 1
                                error_count += 1
                                try:
                                    path_str = str(file_path) if 'file_path' in locals() else "未知路径"
                                    path_display = format_path_for_log(path_str)
                                    if permission_error_count <= 20:  # 只记录前20个权限错误
                                        logger.warning(f"压缩扫描：路径权限错误（路径 #{permission_error_count}）: {path_display}，错误: {str(path_error)}")
                                except Exception:
                                    logger.warning(f"压缩扫描：路径权限错误（路径 #{permission_error_count}）: 无法获取路径，错误: {str(path_error)}")
                                if len(error_paths) < 50:  # 只记录前50个错误路径
                                    try:
                                        error_paths.append(str(file_path) if 'file_path' in locals() else "未知路径")
                                    except Exception:
                                        pass
                                continue
                            except (FileNotFoundError, IOError) as path_error:
                                # 路径不存在或IO错误：跳过该路径
                                error_count += 1
                                try:
                                    path_str = str(file_path) if 'file_path' in locals() else "未知路径"
                                    path_display = format_path_for_log(path_str)
                                    logger.debug(f"压缩扫描：路径不存在或IO错误: {path_display}，错误: {str(path_error)}")
                                except Exception:
                                    logger.debug(f"压缩扫描：路径不存在或IO错误: 无法获取路径，错误: {str(path_error)}")
                                if len(error_paths) < 50:  # 只记录前50个错误路径
                                    try:
                                        error_paths.append(str(file_path) if 'file_path' in locals() else "未知路径")
                                    except Exception:
                                        pass
                                continue
                            except Exception as path_error:
                                # 其他错误，也跳过该路径
                                error_count += 1
                                try:
                                    path_str = str(file_path) if 'file_path' in locals() else "未知路径"
                                    path_display = format_path_for_log(path_str)
                                    logger.warning(f"压缩扫描：跳过出错的路径: {path_display}，错误: {str(path_error)}")
                                except Exception:
                                    logger.warning(f"压缩扫描：跳过出错的路径: 无法获取路径，错误: {str(path_error)}")
                                if len(error_paths) < 50:  # 只记录前50个错误路径
                                    try:
                                        error_paths.append(str(file_path) if 'file_path' in locals() else "未知路径")
                                    except Exception:
                                        pass
                                continue
                            
                            # 每处理100个文件后，yield控制权，避免长时间阻塞
                            if total_scanned % 100 == 0:
                                await asyncio.sleep(0)  # 让出控制权
                    except (PermissionError, OSError, FileNotFoundError, IOError) as scan_error:
                        # 扫描目录时的访问错误，记录但继续扫描其他目录
                        error_count += 1
                        error_paths.append(str(source_path_str))
                        logger.error(f"⚠️ 扫描目录时发生访问错误 {source_path_str}: {str(scan_error)}，跳过该目录，继续扫描其他路径")
                        continue
                    except Exception as e:
                        # 其他扫描错误，记录但继续
                        error_count += 1
                        error_paths.append(str(source_path_str))
                        logger.error(f"⚠️ 扫描目录时发生错误 {source_path_str}: {str(e)}，跳过该目录，继续扫描其他路径")
                        continue
                    
                    logger.info(f"目录扫描完成: {source_path_str}, 扫描 {scanned_count} 个文件, 累计有效 {total_valid_files} 个（当前批次: {len(current_batch)} 个）, 排除 {excluded_count} 个文件, 跳过 {skipped_dirs} 个目录/文件, 错误 {error_count} 个, 权限错误: {permission_error_count} 个, 路径过长: {path_too_long_count} 个")
                    if excluded_count > 0 or skipped_dirs > 0:
                        logger.warning(f"⚠️ 注意：已排除 {excluded_count} 个文件，跳过 {skipped_dirs} 个目录/文件（排除规则: {exclude_patterns if exclude_patterns else '无'}）")
                    if error_count > 0 or permission_error_count > 0 or path_too_long_count > 0:
                        logger.warning(f"⚠️ 注意：遇到 {error_count} 个错误（权限错误: {permission_error_count} 个, 路径过长: {path_too_long_count} 个），已跳过这些文件/目录")
                        if len(error_paths) > 0:
                            if len(error_paths) <= 10:
                                logger.warning(f"⚠️ 错误路径示例: {', '.join(error_paths[:10])}")
                            else:
                                logger.warning(f"⚠️ 错误路径示例（前10个）: {', '.join(error_paths[:10])}... 共 {len(error_paths)} 个")
                else:
                    logger.warning(f"源路径既不是文件也不是目录: {source_path_str}")
            except Exception as e:
                logger.error(f"处理源路径失败 {source_path_str}: {str(e)}")
                continue
        
        # 返回最后一批文件
        if current_batch:
            yield current_batch
        
        # 扫描完成，更新进度
        # 注意：不再在这里更新 total_bytes_actual 和 total_scanned_files
        # 这些统计由独立的后台扫描任务 _scan_for_progress_update 负责更新

        if backup_task and self.update_progress_callback:
            backup_task.progress_percent = 10.0
            await self.update_progress_callback(backup_task, total_scanned, total_scanned, "[准备压缩...]")
        
        logger.info(f"========== 扫描完成 ==========")
        logger.info(f"共扫描 {total_scanned} 个文件，找到 {total_valid_files} 个有效文件，总大小 {total_scanned_size:,} 字节")
        if exclude_patterns:
            logger.info(f"排除规则: {exclude_patterns}")
        logger.info(f"========== 扫描完成 ==========")
    
    async def scan_source_files(
        self, 
        source_paths: List[str], 
        exclude_patterns: List[str], 
        backup_task: Optional[object] = None
    ) -> List[Dict]:
        """扫描源文件（兼容旧接口，收集所有文件后返回）
        
        Args:
            source_paths: 源路径列表
            exclude_patterns: 排除模式列表
            backup_task: 备份任务对象（可选，用于进度更新）
            
        Returns:
            List[Dict]: 文件列表
        """
        file_list = []
        
        if not source_paths:
            logger.warning("源路径列表为空")
            return file_list

        # 不再估算总文件数，直接使用后台扫描任务提供的 total_files
        # 后台扫描任务会独立扫描并更新数据库中的 total_files 和 total_bytes
        # 这样可以避免重复扫描，特别是对于包含大量目录的场景（如40.5万个目录）
        
        total_scanned = 0
        
        for idx, source_path_str in enumerate(source_paths):
            logger.info(f"扫描源路径 {idx + 1}/{len(source_paths)}: {source_path_str}")
            source_path = Path(source_path_str)
            
            # 检查路径是否存在
            if not source_path.exists():
                logger.warning(f"源路径不存在，跳过: {source_path_str}")
                continue
            
            try:
                if source_path.is_file():
                    logger.debug(f"扫描文件: {source_path_str}")
                    try:
                        file_info = await self.get_file_info(source_path)
                        if file_info:
                            # 排除规则从计划任务获取（scheduled_task.action_config.exclude_patterns）
                            if not self.should_exclude_file(file_info['path'], exclude_patterns):
                                file_list.append(file_info)
                                logger.debug(f"已添加文件: {file_info['path']}")
                        
                        total_scanned += 1
                        # 更新扫描进度（使用后台扫描任务提供的 total_files，如果没有则只更新状态）
                        # 后台扫描任务会独立扫描并更新数据库中的 total_files，这里只需要更新已扫描的文件数
                        if backup_task and self.update_progress_callback:
                            # 从数据库读取最新的 total_files（由后台扫描任务更新）
                            if hasattr(backup_task, 'total_files') and backup_task.total_files and backup_task.total_files > 0:
                                # 使用后台扫描任务提供的 total_files 计算进度
                                scan_progress = min(10.0, (total_scanned / backup_task.total_files) * 10.0)
                                backup_task.progress_percent = scan_progress
                            else:
                                # 如果后台扫描任务还没有更新 total_files，只更新状态，不显示具体进度
                                backup_task.progress_percent = 0.0
                            await self.update_progress_callback(backup_task, total_scanned, len(file_list))
                    except (PermissionError, OSError, FileNotFoundError, IOError) as file_error:
                        # 文件访问错误，跳过该文件，继续扫描
                        logger.warning(f"⚠️ 跳过无法访问的文件: {source_path_str} (错误: {str(file_error)})")
                        continue
                    except Exception as file_error:
                        # 其他错误，也跳过该文件
                        logger.warning(f"⚠️ 跳过出错的文件: {source_path_str} (错误: {str(file_error)})")
                        continue
                        
                elif source_path.is_dir():
                    logger.info(f"扫描目录: {source_path_str}")
                    
                    # 检查目录本身是否匹配排除规则
                    if self.should_exclude_file(str(source_path), exclude_patterns):
                        logger.info(f"目录匹配排除规则，跳过整个目录: {source_path_str}")
                        continue
                    
                    scanned_count = 0
                    excluded_count = 0
                    skipped_dirs = 0
                    error_count = 0
                    error_paths = []
                    
                    # 使用 os.scandir() 替代 rglob() 以提高性能（特别是对于大量目录）
                    # os.scandir() 在 Windows 上比 rglob() 更快，且内存占用更少
                    MAX_PATH_LENGTH = 260  # Windows路径最大长度（字符）
                    MAX_PATH_DISPLAY = 200  # 日志中显示的最大路径长度（字符）
                    
                    def truncate_path(path_str: str, max_len: int = MAX_PATH_DISPLAY) -> str:
                        """截断路径以便在日志中显示"""
                        if len(path_str) <= max_len:
                            return path_str
                        prefix_len = max_len - 50
                        return f"{path_str[:prefix_len]}...{path_str[-(max_len-prefix_len-3):]}"
                    
                    def format_path_for_log(path_str: str) -> str:
                        """格式化路径以便在日志中显示，考虑长度限制"""
                        try:
                            path_len = len(path_str)
                            if path_len > MAX_PATH_LENGTH:
                                return f"{truncate_path(path_str)} (路径长度: {path_len} 字符，超过Windows限制 {MAX_PATH_LENGTH} 字符)"
                            return truncate_path(path_str)
                        except Exception:
                            return str(path_str)[:MAX_PATH_DISPLAY]
                    
                    try:
                        # 使用迭代方式遍历目录（避免 rglob 的内存问题）
                        dirs_to_scan = [source_path]  # 待扫描的目录队列
                        scanned_dirs = set()  # 已扫描的目录集合（避免重复扫描）
                        
                        while dirs_to_scan:
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
                                
                                # 检查目录是否匹配排除规则
                                if self.should_exclude_file(current_scan_dir_str, exclude_patterns):
                                    skipped_dirs += 1
                                    continue
                                
                                try:
                                    # 使用 os.scandir() 扫描当前目录
                                    with os.scandir(current_scan_dir_str) as entries:
                                        for entry in entries:
                                            try:
                                                # 获取路径
                                                try:
                                                    entry_path = Path(entry.path)
                                                    current_path_str = str(entry_path)
                                                    path_len = len(current_path_str)
                                                    
                                                    # 检查路径长度
                                                    if path_len > MAX_PATH_LENGTH:
                                                        error_count += 1
                                                        if error_count <= 10:
                                                            logger.warning(f"扫描：路径过长（{path_len} 字符 > {MAX_PATH_LENGTH} 字符）: {format_path_for_log(current_path_str)}")
                                                        continue
                                                except Exception as path_str_err:
                                                    logger.debug(f"扫描：路径字符串化失败: {str(path_str_err)}")
                                                    continue
                                                
                                                # 处理目录和文件
                                                try:
                                                    if entry.is_dir(follow_symlinks=False):
                                                        # 目录：添加到待扫描队列
                                                        dirs_to_scan.append(entry_path)
                                                    elif entry.is_file(follow_symlinks=False):
                                                        # 文件：检查排除规则并处理
                                                        # 检查文件路径的父目录是否匹配排除规则
                                                        if self.should_exclude_file(str(entry_path.parent), exclude_patterns):
                                                            skipped_dirs += 1
                                                            continue
                                                        
                                                        scanned_count += 1
                                                        total_scanned += 1
                                                        
                                                        # 每扫描100个文件输出一次进度并更新数据库
                                                    if scanned_count % 10000 == 0:
                                                        logger.info(f"{context_prefix}压缩扫描：已扫描 {scanned_count} 个文件（当前源路径，仅为当前源路径的文件数），找到 {len(file_list)} 个有效文件...")
                                                        
                                                        # 每扫描50个文件更新一次进度（避免过于频繁）
                                                        if total_scanned % 50 == 0 and backup_task and self.update_progress_callback:
                                                            # 更新扫描进度（使用后台扫描任务提供的 total_files，如果没有则只更新状态）
                                                            if hasattr(backup_task, 'total_files') and backup_task.total_files and backup_task.total_files > 0:
                                                                scan_progress = min(10.0, (total_scanned / backup_task.total_files) * 10.0)
                                                                backup_task.progress_percent = scan_progress
                                                            else:
                                                                backup_task.progress_percent = 0.0
                                                            await self.update_progress_callback(backup_task, total_scanned, len(file_list))
                                                        
                                                        try:
                                                            file_info = await self.get_file_info(entry_path)
                                                            if file_info:
                                                                # 排除规则从计划任务获取
                                                                if not self.should_exclude_file(file_info['path'], exclude_patterns):
                                                                    file_list.append(file_info)
                                                                else:
                                                                    excluded_count += 1
                                                        except (PermissionError, OSError, FileNotFoundError, IOError) as file_error:
                                                            error_count += 1
                                                            error_paths.append(current_path_str)
                                                            if error_count <= 20:
                                                                logger.warning(f"⚠️ 跳过无法访问的文件: {format_path_for_log(current_path_str)} (错误: {str(file_error)})")
                                                            continue
                                                        except Exception as file_error:
                                                            error_count += 1
                                                            error_paths.append(current_path_str)
                                                            if error_count <= 20:
                                                                logger.warning(f"⚠️ 跳过出错的文件: {format_path_for_log(current_path_str)} (错误: {str(file_error)})")
                                                            continue
                                                except (OSError, PermissionError) as entry_err:
                                                    # 无法判断类型，尝试作为文件处理
                                                    try:
                                                        if entry_path.is_file():
                                                            scanned_count += 1
                                                            total_scanned += 1
                                                            file_info = await self.get_file_info(entry_path)
                                                            if file_info and not self.should_exclude_file(file_info['path'], exclude_patterns):
                                                                file_list.append(file_info)
                                                        elif entry_path.is_dir():
                                                            dirs_to_scan.append(entry_path)
                                                    except Exception:
                                                        error_count += 1
                                                        if error_count <= 20:
                                                            logger.debug(f"扫描：无法访问路径: {format_path_for_log(current_path_str)}，错误: {str(entry_err)}")
                                                        continue
                                                    
                                            except (PermissionError, OSError) as entry_err:
                                                error_count += 1
                                                try:
                                                    path_str = str(entry.path) if hasattr(entry, 'path') else "未知路径"
                                                    if error_count <= 20:
                                                        logger.warning(f"扫描：路径权限错误（路径 #{error_count}）: {format_path_for_log(path_str)}，错误: {str(entry_err)}")
                                                except Exception:
                                                    if error_count <= 20:
                                                        logger.warning(f"扫描：路径权限错误（路径 #{error_count}）: 无法获取路径，错误: {str(entry_err)}")
                                                continue
                                            except (FileNotFoundError, IOError) as entry_err:
                                                continue
                                            except Exception as entry_err:
                                                error_count += 1
                                                try:
                                                    path_str = str(entry.path) if hasattr(entry, 'path') else "未知路径"
                                                    if error_count <= 20:
                                                        logger.debug(f"扫描：跳过路径错误 {format_path_for_log(path_str)}: {str(entry_err)}")
                                                except Exception:
                                                    pass
                                                continue
                                except (PermissionError, OSError) as scan_dir_err:
                                    # 目录权限错误：记录并跳过该目录
                                    error_count += 1
                                    error_paths.append(current_scan_dir_str)
                                    if error_count <= 20:
                                        logger.warning(f"扫描：目录权限错误（目录 #{error_count}）: {format_path_for_log(current_scan_dir_str)}，错误: {str(scan_dir_err)}")
                                    continue
                                except (FileNotFoundError, IOError) as scan_dir_err:
                                    continue
                                except Exception as scan_dir_err:
                                    error_count += 1
                                    error_paths.append(current_scan_dir_str)
                                    if error_count <= 20:
                                        logger.debug(f"扫描：跳过目录错误 {format_path_for_log(current_scan_dir_str)}: {str(scan_dir_err)}")
                                    continue
                                    
                            except Exception as dir_scan_err:
                                logger.debug(f"扫描：目录扫描错误: {str(dir_scan_err)}")
                                continue
                                
                    except (PermissionError, OSError, FileNotFoundError, IOError) as scan_error:
                        # 扫描目录时的访问错误，记录但继续扫描其他目录
                        error_count += 1
                        error_paths.append(str(source_path_str))
                        logger.error(f"⚠️ 扫描目录时发生访问错误 {source_path_str}: {str(scan_error)}，跳过该目录，继续扫描其他路径")
                        continue
                    except Exception as e:
                        # 其他扫描错误，记录但继续
                        error_count += 1
                        error_paths.append(str(source_path_str))
                        logger.error(f"⚠️ 扫描目录时发生错误 {source_path_str}: {str(e)}，跳过该目录，继续扫描其他路径")
                        continue
                    
                    logger.info(f"目录扫描完成: {source_path_str}, 扫描 {scanned_count} 个文件, 有效 {len(file_list)} 个, 排除 {excluded_count} 个文件, 跳过 {skipped_dirs} 个目录/文件, 错误 {error_count} 个")
                    if excluded_count > 0 or skipped_dirs > 0:
                        logger.warning(f"⚠️ 注意：已排除 {excluded_count} 个文件，跳过 {skipped_dirs} 个目录/文件（排除规则: {exclude_patterns if exclude_patterns else '无'}）")
                    if error_count > 0:
                        logger.warning(f"⚠️ 注意：遇到 {error_count} 个错误（权限、访问等），已跳过这些文件/目录")
                        if len(error_paths) <= 10:
                            logger.warning(f"⚠️ 错误路径示例: {', '.join(error_paths[:10])}")
                        else:
                            logger.warning(f"⚠️ 错误路径示例（前10个）: {', '.join(error_paths[:10])}... 共 {len(error_paths)} 个")
                else:
                    logger.warning(f"源路径既不是文件也不是目录: {source_path_str}")
            except Exception as e:
                logger.error(f"处理源路径失败 {source_path_str}: {str(e)}")
                continue

        return file_list

