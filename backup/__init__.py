#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
备份处理模块
Backup Processing Module
"""

from .backup_engine import BackupEngine
# 向后兼容：导出 normalize_volume_label 和 extract_label_year_month
# 这些函数现在从 backup.utils 模块导入，但为了向后兼容，仍然从 backup_engine 导出
from backup.utils import normalize_volume_label, extract_label_year_month

__all__ = [
    'BackupEngine',
    'normalize_volume_label',
    'extract_label_year_month',
]