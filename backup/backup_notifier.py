#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
备份通知和配置管理模块
Backup Notifier and Configuration Management Module
"""

import json
import logging
from datetime import datetime, timedelta
from pathlib import Path
from typing import Dict, Any, Optional, List, Callable

from utils.datetime_utils import now

logger = logging.getLogger(__name__)


class BackupNotifier:
    """备份通知和配置管理类"""
    
    def __init__(self, dingtalk_notifier=None):
        """初始化通知器
        
        Args:
            dingtalk_notifier: 钉钉通知器对象
        """
        self.dingtalk_notifier = dingtalk_notifier
        self._notification_events_cache: Optional[Dict[str, bool]] = None
        self._notification_events_cache_time: Optional[datetime] = None
        self._progress_callbacks: List[Callable] = []
    
    def add_progress_callback(self, callback: Callable):
        """添加进度回调函数"""
        if callback not in self._progress_callbacks:
            self._progress_callbacks.append(callback)
    
    async def get_notification_events(self) -> Dict[str, bool]:
        """获取通知事件配置（带缓存）
        
        Returns:
            通知事件配置字典，包含各个事件的启用状态
        """
        # 缓存5分钟
        cache_timeout = timedelta(minutes=5)
        current_time = now()
        
        if (self._notification_events_cache and 
            self._notification_events_cache_time and 
            (current_time - self._notification_events_cache_time) < cache_timeout):
            return self._notification_events_cache
        
        try:
            # 从.env文件读取通知事件配置
            env_file = Path(".env")
            if env_file.exists():
                with open(env_file, "r", encoding="utf-8") as f:
                    for line in f:
                        line = line.strip()
                        if line.startswith("NOTIFICATION_EVENTS="):
                            events_json = line.split("=", 1)[1]
                            events_dict = json.loads(events_json)
                            self._notification_events_cache = events_dict
                            self._notification_events_cache_time = current_time
                            return events_dict
            
            # 如果.env中没有，返回默认配置（所有事件都启用）
            default_events = {
                "notify_backup_success": True,
                "notify_backup_started": True,
                "notify_backup_failed": True,
                "notify_recovery_success": True,
                "notify_recovery_failed": True,
                "notify_tape_change": True,
                "notify_tape_expired": True,
                "notify_tape_error": True,
                "notify_capacity_warning": True,
                "notify_system_error": True,
                "notify_system_started": True
            }
            self._notification_events_cache = default_events
            self._notification_events_cache_time = current_time
            return default_events
            
        except Exception as e:
            logger.warning(f"获取通知事件配置失败: {str(e)}，使用默认配置")
            # 返回默认配置（所有事件都启用）
            default_events = {
                "notify_backup_success": True,
                "notify_backup_started": True,
                "notify_backup_failed": True,
                "notify_recovery_success": True,
                "notify_recovery_failed": True,
                "notify_tape_change": True,
                "notify_tape_expired": True,
                "notify_tape_error": True,
                "notify_capacity_warning": True,
                "notify_system_error": True,
                "notify_system_started": True
            }
            return default_events
    
    async def get_backup_policy_parameters(self, settings) -> Dict[str, Any]:
        """获取备份策略参数（从tapedrive和system配置）
        
        Args:
            settings: 系统设置对象
            
        Returns:
            备份策略参数字典，包含：
            - compression_level: 压缩级别
            - max_file_size: 最大文件大小
            - solid_block_size: 固体块大小
            - retention_days: 保留天数
            - tape_drive_letter: 磁带盘符
            - default_block_size: 默认块大小
            - max_volume_size: 最大卷大小
        """
        try:
            # 从.env文件读取备份策略参数
            env_file = Path(".env")
            if env_file.exists():
                with open(env_file, "r", encoding="utf-8") as f:
                    for line in f:
                        line = line.strip()
                        if line.startswith("BACKUP_POLICY_PARAMETERS="):
                            policy_json = line.split("=", 1)[1]
                            policy_dict = json.loads(policy_json)
                            return policy_dict
            
            # 如果.env中没有，从settings获取
            policy = {
                'compression_level': getattr(settings, 'COMPRESSION_LEVEL', 9),
                'max_file_size': getattr(settings, 'MAX_FILE_SIZE', 3221225472),  # 3GB
                'solid_block_size': getattr(settings, 'SOLID_BLOCK_SIZE', 67108864),  # 64MB
                'retention_days': getattr(settings, 'RETENTION_DAYS', 180),  # 6个月
                'tape_drive_letter': getattr(settings, 'TAPE_DRIVE_LETTER', 'o'),
                'default_block_size': getattr(settings, 'DEFAULT_BLOCK_SIZE', 262144),  # 256KB
                'max_volume_size': getattr(settings, 'MAX_VOLUME_SIZE', 322122547200)  # 300GB
            }
            return policy
            
        except Exception as e:
            logger.warning(f"获取备份策略参数失败: {str(e)}，使用默认配置")
            # 返回默认配置
            return {
                'compression_level': 9,
                'max_file_size': 3221225472,  # 3GB
                'solid_block_size': 67108864,  # 64MB
                'retention_days': 180,  # 6个月
                'tape_drive_letter': 'o',
                'default_block_size': 262144,  # 256KB
                'max_volume_size': 322122547200  # 300GB
            }
    
    async def notify_progress(self, backup_task):
        """通知进度更新
        
        Args:
            backup_task: 备份任务对象
        """
        try:
            import asyncio
            for callback in self._progress_callbacks:
                if asyncio.iscoroutinefunction(callback):
                    await callback(backup_task)
                else:
                    callback(backup_task)
        except Exception as e:
            logger.error(f"进度通知失败: {str(e)}")

