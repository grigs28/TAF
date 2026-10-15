#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""压缩公共辅助函数（自 compressor.py 拆分，逻辑未变）"""

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

logger = logging.getLogger(__name__)

def _parse_size_to_bytes(size_str: Optional[str], default_bytes: int = 1024 * 1024 * 1024) -> int:
    """解析带单位的块大小字符串为字节"""
    if not size_str:
        return default_bytes
    value = str(size_str).strip().lower()
    try:
        if value.endswith('g'):
            return int(float(value[:-1]) * (1024 ** 3))
        if value.endswith('m'):
            return int(float(value[:-1]) * (1024 ** 2))
        if value.endswith('k'):
            return int(float(value[:-1]) * 1024)
        return int(float(value))
    except ValueError:
        logger.warning(f"无法解析块大小 {size_str}，使用默认 {default_bytes} 字节")
        return default_bytes


def _finalize_compression_progress(compress_progress: Dict, archive: Optional[Path] = None):
    """统一更新压缩进度，避免调用方一直等待"""
    archive_abs = None
    try:
        if archive is not None:
            archive_abs = archive.absolute()
    except Exception:
        archive_abs = None
    compress_progress['running'] = False
    compress_progress['completed'] = True
    if archive_abs and archive_abs.exists():
        try:
            compress_progress['bytes_written'] = archive_abs.stat().st_size
        except Exception:
            compress_progress['bytes_written'] = 0
    else:
        compress_progress['bytes_written'] = 0


# 尝试导入psutil用于内存检查
try:
    import psutil
    PSUTIL_AVAILABLE = True
except ImportError:
    PSUTIL_AVAILABLE = False
    logger.warning("psutil未安装，无法检查系统内存，将使用默认内存分配策略")


def _apply_tar_copybufsize(tar) -> None:
    try:
        from config.settings import get_settings
        mb = int(getattr(get_settings(), 'COMPRESSION_TAR_COPYBUF_MB', 4) or 0)
        if mb > 0:
            tar.copybufsize = mb * 1024 * 1024
    except Exception:
        pass
