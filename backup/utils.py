#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
备份工具函数模块
Backup Utility Functions Module
"""

import re
import hashlib
from typing import Optional, Dict
from pathlib import Path

logger = None  # 将在模块导入时设置


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

