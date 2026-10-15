#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""PGZip 并行压缩实现（自 compressor.py 拆分，逻辑未变）"""

import logging
import time
import os
import subprocess
import shutil
import tarfile
from pathlib import Path
from typing import List, Dict, Optional, Callable, Awaitable, Any

from models.backup import BackupSet, BackupTask
from utils.datetime_utils import now, format_datetime
from backup.utils import format_bytes
from backup.compress_helpers import (
    _parse_size_to_bytes,
    _finalize_compression_progress,
    _apply_tar_copybufsize,
)

logger = logging.getLogger(__name__)

import pgzip  # noqa: F401

def _compress_with_pgzip(
    archive_path: Path,
    file_group: List[Dict],
    backup_task: BackupTask,
    compression_level: int,
    pgzip_threads: int,
    block_size: str,
    compress_progress: Dict,
    total_files: int,
    base_processed_files: int,
) -> Dict:
    """使用PGZip压缩文件"""
    successful_files: List[str] = []
    failed_files: List[Dict[str, str]] = []
    archive_path_abs = archive_path.absolute()
    archive_path_abs.parent.mkdir(parents=True, exist_ok=True)

    block_size_bytes = _parse_size_to_bytes(block_size, 1024 * 1024 * 1024)
    threads = max(1, min(pgzip_threads or 1, 64))
    compresslevel = max(0, min(int(compression_level), 9))

    source_paths = getattr(backup_task, 'source_paths', None) or []

    close_start_time = None
    try:
        logger.debug(f"[PGZip] 开始打开压缩文件: {archive_path_abs}")
        with pgzip.open(
            archive_path_abs,
            'wb',
            thread=threads,
            blocksize=block_size_bytes,
            compresslevel=compresslevel or 5
        ) as gz_source:
            logger.debug(f"[PGZip] 压缩文件已打开，开始创建tar文件")
            with tarfile.open(fileobj=gz_source, mode='w') as tar:
                _apply_tar_copybufsize(tar)
                total_files_in_group = len(file_group)
                last_log_time = time.time()
                log_interval = 10.0  # 每10秒输出一次进度
                
                for file_idx, file_info in enumerate(file_group):
                    file_path = Path(file_info['path'])
                    
                    # 每10秒输出一次进度（不再按文件数量）
                    current_time = time.time()
                    if (current_time - last_log_time) >= log_interval:
                        current_progress = file_idx + 1
                        progress_percent = (current_progress / total_files_in_group * 100) if total_files_in_group > 0 else 0
                        logger.debug(f"[PGZip] 压缩进度: {current_progress}/{total_files_in_group} 个文件 ({progress_percent:.1f}%)")
                        # 将压缩进度信息存储到 compress_progress 和 backup_task 中
                        compress_progress['current_file_index'] = current_progress
                        compress_progress['total_files_in_group'] = total_files_in_group
                        # 计算当前文件组的原始总大小（压缩前）
                        total_group_size = sum(f.get('size', 0) or 0 for f in file_group)
                        if hasattr(backup_task, 'current_compression_progress'):
                            backup_task.current_compression_progress = {
                                'current': current_progress,
                                'total': total_files_in_group,
                                'percent': progress_percent,
                                'group_size_bytes': total_group_size  # 文件组的原始总大小（压缩前）
                            }
                        last_log_time = current_time
                    
                    if not file_path.exists():
                        logger.warning(f"文件不存在，跳过: {file_path}")
                        failed_files.append({'path': str(file_path), 'reason': '文件不存在'})
                        continue

                    arcname = file_path.name
                    for src_path in source_paths:
                        src = Path(src_path)
                        try:
                            if file_path.is_relative_to(src):
                                arcname = str(file_path.relative_to(src))
                                break
                        except (ValueError, AttributeError):
                            continue

                    try:
                        # 记录开始添加文件（仅对前10个和每1000个文件，或大文件）
                        file_size = file_info.get('size', 0) or 0
                        is_large_file = file_size > 100 * 1024 * 1024  # 大于100MB的文件
                        
                        if file_idx < 10 or file_idx % 1000 == 0 or is_large_file:
                            file_size_mb = file_size / (1024 * 1024) if file_size > 0 else 0
                            logger.debug(f"[PGZip] 开始添加文件 {file_idx + 1}/{total_files_in_group}: {file_path.name} (大小: {file_size_mb:.1f} MB)")
                        
                        # 对于大文件，记录开始时间
                        if is_large_file:
                            add_start_time = time.time()
                        
                        # 检查文件路径是否包含特殊字符，可能导致问题
                        # Windows上，某些特殊字符可能导致文件操作阻塞或需要输入
                        file_path_str = str(file_path)
                        if any(char in file_path_str for char in [' ', '-', '(', ')', '[', ']', '{', '}', '&', '|', '<', '>', '^', '%', '!', '@', '#', '$', '^', '*', '+', '=', ';', ':', '"', "'", '?', '*']):
                            logger.debug(f"[PGZip] 文件路径包含特殊字符: {file_path_str}")
                        
                        # 使用 filter 参数处理特殊文件名，避免阻塞
                        # filter 参数可以自定义文件元数据，避免某些文件系统操作
                        try:
                            tar.add(file_path, arcname=arcname, filter=None)
                        except Exception as tar_add_error:
                            # 如果 tar.add 失败，尝试使用 filter 参数
                            logger.warning(f"[PGZip] tar.add 失败，尝试使用 filter 参数: {file_path}, 错误: {tar_add_error}")
                            # 定义一个简单的 filter 函数，只保留基本信息
                            def safe_filter(tarinfo):
                                # 只保留基本信息，避免触发文件系统的某些操作
                                return tarinfo
                            tar.add(file_path, arcname=arcname, filter=safe_filter)
                        
                        # 对于大文件，记录耗时
                        if is_large_file:
                            add_elapsed = time.time() - add_start_time
                            logger.debug(f"[PGZip] 大文件添加完成: {file_path.name} (耗时: {add_elapsed:.1f}秒)")
                        
                        successful_files.append(str(file_path))
                        if total_files > 0:
                            current_processed = base_processed_files + file_idx + 1
                            compress_progress_value = 10.0 + (current_processed / total_files) * 90.0
                            compress_progress['bytes_written'] = archive_path_abs.stat().st_size if archive_path_abs.exists() else 0
                            backup_task.progress_percent = min(100.0, compress_progress_value)
                    except Exception as add_error:
                        logger.warning(f"添加文件到PGZip压缩包失败: {file_path}, 错误: {add_error}")
                        failed_files.append({'path': str(file_path), 'reason': f'写入失败: {add_error}'})
                        continue
                
                logger.debug(f"[PGZip] 所有文件已添加到tar，共 {len(successful_files)} 个成功，{len(failed_files)} 个失败")
            
            logger.debug(f"[PGZip] tar文件已关闭，准备关闭PGZip文件")
            # tar 文件已关闭，现在需要关闭 PGZip 文件
            
            # 在 with 语句退出前，显式刷新缓冲区（如果支持）
            # 这可以确保所有数据都写入文件，减少 close() 时的等待时间
            try:
                if hasattr(gz_source, 'flush'):
                    logger.debug(f"[PGZip] 显式刷新PGZip缓冲区（在关闭前）...")
                    flush_start_time = time.time()
                    gz_source.flush()
                    flush_elapsed = time.time() - flush_start_time
                    logger.debug(f"[PGZip] PGZip缓冲区已刷新（耗时: {flush_elapsed:.2f}秒）")
            except Exception as flush_error:
                logger.warning(f"[PGZip] 刷新PGZip缓冲区失败（可能不支持）: {flush_error}")
            
            logger.debug(f"[PGZip] 准备退出with语句，PGZip将自动调用close()...")
            # with 语句退出时会自动调用 gz_source.close()
            # 如果 PGZip 内部使用了多线程，close() 可能需要等待所有线程完成
            # 记录关闭开始时间，以便定位阻塞点
            close_start_time = time.time()
        
        # with 语句已退出，gz_source.close() 应该已经完成
        if close_start_time is not None:
            close_elapsed = time.time() - close_start_time
            logger.debug(f"[PGZip] PGZip文件已关闭（耗时: {close_elapsed:.2f}秒），检查文件是否存在")
        else:
            logger.debug(f"[PGZip] PGZip文件已关闭，检查文件是否存在")
        if archive_path_abs.exists():
            logger.info(
                f"PGZip压缩完成: {len(successful_files)} 个文件成功, "
                f"压缩包大小: {format_bytes(archive_path_abs.stat().st_size)}"
            )
            compress_progress['bytes_written'] = archive_path_abs.stat().st_size

        # 标记压缩完成（关键修复）
        logger.debug(f"[PGZip] 标记压缩进度为完成")
        _finalize_compression_progress(compress_progress, archive_path_abs)

        successful_original_size = sum(
            f['size'] for f in file_group if str(f['path']) in successful_files
        )

        return {
            'successful_files': successful_files,
            'failed_files': failed_files,
            'successful_original_size': successful_original_size,
            'archive_path': str(archive_path_abs)
        }
    except Exception as e:
        logger.error(f"PGZip压缩操作失败: {str(e)}")
        import traceback
        logger.error(traceback.format_exc())
        _finalize_compression_progress(compress_progress)
        return {
            'successful_files': [],
            'failed_files': failed_files,
            'successful_original_size': 0,
            'archive_path': str(archive_path_abs)
        }
