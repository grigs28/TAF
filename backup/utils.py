#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
备份工具函数模块
Backup Utility Functions Module
"""

import logging
import re
import hashlib
from typing import Optional, Dict
from pathlib import Path

# 曾经是 `logger = None  # 将在模块导入时设置`，但从未真的设置过：
# kill_compression_processes() 在"无残留进程"这条正常路径上必走 logger.debug，
# 直接抛 AttributeError: 'NoneType' object has no attribute 'debug'，
# 被上层 except 捕获后误报成"终止压缩进程失败"（每次停任务都出现）。
logger = logging.getLogger(__name__)


def normalize_volume_label(label: Optional[str], year: int, month: int) -> str:
    """标准化磁带卷标
    
    Args:
        label: 原始卷标
        year: 目标年份
        month: 目标月份
        
    Returns:
        标准化后的卷标，格式：TPYYYYMMNN
    """
    target_year = f"{year:04d}"
    target_month = f"{month:02d}"
    default_seq = "01"
    default_label = f"TP{target_year}{target_month}{default_seq}"

    if not label:
        return default_label

    clean_label = label.strip().upper()

    def build_label(seq: str, suffix: str = "") -> str:
        seq = (seq if seq and seq.isdigit() else default_seq).zfill(2)[:2]
        return f"TP{target_year}{target_month}{seq}{suffix}"

    match = re.match(r'^TP(\d{4})(\d{2})(\d{2})(.*)$', clean_label)
    if match:
        return build_label(match.group(3), match.group(4))

    match = re.match(r'^TP(\d{4})(\d{2})(\d+)(.*)$', clean_label)
    if match:
        return build_label(match.group(3), match.group(4))

    match = re.match(r'^TAPE(\d{4})(\d{2})(\d{2})(.*)$', clean_label)
    if match:
        return build_label(match.group(3), match.group(4))

    match = re.match(r'^TAPE(\d{4})(\d{2})(\d+)(.*)$', clean_label)
    if match:
        return build_label(match.group(3), match.group(4))

    match = re.search(r'(\d{4})(\d{2})(\d{2})', clean_label)
    if match:
        return build_label(match.group(3))

    return default_label


def extract_label_year_month(label: Optional[str]) -> Optional[Dict[str, int]]:
    """从卷标中提取年月信息
    
    Args:
        label: 磁带卷标
        
    Returns:
        包含year和month的字典，如果无法提取则返回None
    """
    if not label:
        return None

    clean_label = label.strip().upper()

    match = re.search(r'TP(\d{4})(\d{2})', clean_label)
    if match:
        return {
            "year": int(match.group(1)),
            "month": int(match.group(2))
        }

    match = re.search(r'TAPE(\d{4})(\d{2})', clean_label)
    if match:
        return {
            "year": int(match.group(1)),
            "month": int(match.group(2))
        }

    match = re.search(r'(\d{4})(\d{2})', clean_label)
    if match:
        return {
            "year": int(match.group(1)),
            "month": int(match.group(2))
        }

    return None


def format_bytes(bytes_size: int) -> str:
    """格式化字节大小
    
    Args:
        bytes_size: 字节大小
        
    Returns:
        格式化后的字符串，如 "1.23 GB"
    """
    for unit in ['B', 'KB', 'MB', 'GB', 'TB']:
        if bytes_size < 1024.0:
            return f"{bytes_size:.2f} {unit}"
        bytes_size /= 1024.0
    return f"{bytes_size:.2f} PB"


def calculate_file_checksum(file_path: Path) -> str:
    """计算文件SHA256校验和
    
    Args:
        file_path: 文件路径
        
    Returns:
        SHA256校验和（十六进制字符串）
    """
    sha256_hash = hashlib.sha256()
    with open(file_path, 'rb') as f:
        for chunk in iter(lambda: f.read(4096), b""):
            sha256_hash.update(chunk)
    return sha256_hash.hexdigest()



def kill_compression_processes(reason: str = "任务停止") -> int:
    """强制终止残留的压缩子进程（zstd 与配套的 tar）。

    背景：压缩跑在 CompressionProcessPool 的 executor 里（同步函数 + spawn 的子进程）。
    取消 asyncio future **既不会中断**这些同步函数，**也不会**让它们走到自身的
    except 分支（CancelledError 在 3.10 是 BaseException，且根本传不进同步函数），
    于是 tar/zstd 成为孤儿：继续从 NAS 读数据、继续写磁盘。
    实测：任务取消后仍残留 2~7 个 zstd 进程（其中 tar 还在占 NAS 带宽）。

    匹配策略：
      - zstd：命令行含 "zstd"（TAF 只在本功能里调用 zstd）
      - tar ：必须限定 "taf_zstdcli"（TAF 为每组创建的临时清单目录名，
              用它精确匹配本功能的 tar，避免误杀系统上其他 tar 命令）
    """
    import subprocess

    killed = 0
    for pattern in ('zstd', 'taf_zstdcli'):
        try:
            r = subprocess.run(['pkill', '-9', '-f', pattern],
                               capture_output=True, text=True, timeout=5)
            if r.returncode == 0:
                killed += 1
        except FileNotFoundError:
            # pkill 不存在，回退 killall（只能按进程名，覆盖不了 tar 的完整命令行）
            try:
                subprocess.run(['killall', '-9', 'zstd'],
                               capture_output=True, text=True, timeout=5)
                killed += 1
            except Exception:
                pass
            break
        except Exception:
            continue

    if killed:
        logger.warning(f"[{reason}] 已终止残留的压缩子进程（zstd/tar）")
    else:
        logger.debug(f"[{reason}] 无残留压缩子进程")
    return killed
