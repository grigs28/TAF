#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""纯 tar 打包实现（自 compressor.py 拆分，逻辑未变）"""

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


def _compress_with_tar(
    archive_path: Path,
    file_group: List[Dict],
    backup_task: BackupTask,
    compression_level: int,
    compress_progress: Dict,
    total_files: int,
    base_processed_files: int,
) -> Dict:
    """使用 tar 打包文件（不压缩）"""
    archive_path_abs = archive_path.absolute()
    archive_path_abs.parent.mkdir(parents=True, exist_ok=True)

    successful_files: List[str] = []
    failed_files: List[Dict[str, str]] = []
    source_paths = getattr(backup_task, 'source_paths', None) or []
    total_files_in_group = len(file_group)
    last_log_time = time.time()
    log_interval = 5.0

    logger.debug(f"[tar] 开始创建tar归档文件: {archive_path_abs}")
    try:
        with tarfile.open(archive_path_abs, 'w') as tar:
            _apply_tar_copybufsize(tar)
            for file_idx, file_info in enumerate(file_group):
                file_path = Path(file_info['path'])

                current_time = time.time()
                if file_idx % 100 == 0 or (current_time - last_log_time) >= log_interval:
                    logger.debug(f"[tar] 打包进度: {file_idx + 1}/{total_files_in_group} 个文件 ({((file_idx + 1) / max(total_files_in_group, 1) * 100):.1f}%)")
                    last_log_time = current_time

                if not file_path.exists():
                    logger.warning(f"[tar] 文件不存在，跳过: {file_path}")
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
                    tar.add(file_path, arcname=arcname)
                    successful_files.append(str(file_path))

                    if total_files > 0:
                        current_processed = base_processed_files + file_idx + 1
                        compress_progress_value = 10.0 + (current_processed / total_files) * 90.0
                        compress_progress['bytes_written'] = archive_path_abs.stat().st_size if archive_path_abs.exists() else 0
                        backup_task.progress_percent = min(100.0, compress_progress_value)
                except Exception as add_error:
                    if isinstance(add_error, BrokenPipeError) or 'Broken pipe' in str(add_error):
                        logger.info(f"[tar] 管道已断开，停止压缩")
                        break
                    logger.warning(f"[tar] 添加文件失败: {file_path}, 错误: {add_error}")
                    failed_files.append({'path': str(file_path), 'reason': f'写入失败: {add_error}'})
                    continue

        logger.info(f"[tar] 打包完成：{len(successful_files)} 个文件成功，{len(failed_files)} 个失败")
        compress_progress['completed'] = True
        compress_progress['running'] = False
        compress_progress['bytes_written'] = archive_path_abs.stat().st_size if archive_path_abs.exists() else 0

        successful_original_size = sum(f.get('size', 0) or 0 for f in file_group if str(f.get('path')) in successful_files)

        return {
            'successful_files': successful_files,
            'failed_files': failed_files,
            'successful_original_size': successful_original_size,
            'archive_path': str(archive_path_abs)
        }
    except Exception as tar_error:
        logger.error(f"tar打包失败: {tar_error}")
        import traceback
        logger.error(traceback.format_exc())
        for file_path in successful_files:
            failed_files.append({'path': file_path, 'reason': 'tar打包失败'})

        # 标记压缩完成（即使失败也要标记，避免无限等待）
        _finalize_compression_progress(compress_progress, archive_path_abs)

        return {
            'successful_files': [],
            'failed_files': failed_files,
            'successful_original_size': 0,
            'archive_path': str(archive_path_abs)
        }
