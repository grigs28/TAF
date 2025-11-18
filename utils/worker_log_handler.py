#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
压缩和移动线程专属日志处理器
Worker Thread Dedicated Log Handler

为压缩线程和移动线程创建专属日志目录，只记录WARNING及以上级别的日志
根据备份周期自动清理过期日志（日备份保留12天，月备份保留12个月）
"""

import os
import logging
import logging.handlers
from datetime import datetime, timedelta
from pathlib import Path
from typing import Optional
from enum import Enum

from config.settings import get_settings


class ScheduleType(Enum):
    """调度类型"""
    DAILY = "daily"
    MONTHLY = "monthly"
    UNKNOWN = "unknown"


class WorkerLogHandler:
    """压缩和移动线程专属日志处理器"""
    
    def __init__(self):
        self.settings = get_settings()
        self.log_dir = Path(self.settings.LOG_FILE).parent
        
        # 创建专属日志目录
        self.compression_log_dir = self.log_dir / "compression"
        self.file_move_log_dir = self.log_dir / "file_move"
        
        self.compression_log_dir.mkdir(exist_ok=True)
        self.file_move_log_dir.mkdir(exist_ok=True)
        
        # 日志格式器
        self.formatter = logging.Formatter(
            '%(asctime)s - %(name)s - %(levelname)s - %(funcName)s:%(lineno)d - %(message)s',
            datefmt='%Y-%m-%d %H:%M:%S'
        )
        
        # 详细格式器（包含堆栈跟踪）
        self.detailed_formatter = logging.Formatter(
            '%(asctime)s - %(name)s - %(levelname)s - %(funcName)s:%(lineno)d - %(pathname)s - %(message)s\n%(exc_info)s',
            datefmt='%Y-%m-%d %H:%M:%S'
        )
    
    def setup_worker_loggers(self, schedule_type: Optional[ScheduleType] = None):
        """设置压缩和移动线程的专属日志处理器
        
        Args:
            schedule_type: 备份周期类型（DAILY/MONTHLY），用于确定日志保留时间
        """
        # 确定日志保留时间
        retention_days = self._get_retention_days(schedule_type)
        
        # 压缩线程日志处理器（使用模块的完整名称）
        compression_logger = logging.getLogger('backup.compression_worker')
        # 保持 propagate = True，让日志同时出现在主日志和专属日志中
        self._add_worker_handler(
            compression_logger,
            self.compression_log_dir,
            'compression',
            retention_days
        )
        
        # 移动线程日志处理器（使用模块的完整名称）
        file_move_logger = logging.getLogger('backup.file_move_worker')
        # 保持 propagate = True，让日志同时出现在主日志和专属日志中
        self._add_worker_handler(
            file_move_logger,
            self.file_move_log_dir,
            'file_move',
            retention_days
        )
        
        # 启动日志清理任务
        self._schedule_log_cleanup(schedule_type)
    
    def _add_worker_handler(
        self,
        logger: logging.Logger,
        log_dir: Path,
        worker_name: str,
        retention_days: int
    ):
        """为工作线程添加专属日志处理器
        
        Args:
            logger: 日志器对象
            worker_name: 工作线程名称（用于日志文件名）
            log_dir: 日志目录
            retention_days: 日志保留天数
        """
        # 创建按日期轮转的日志文件
        log_file = log_dir / f"{worker_name}_{datetime.now().strftime('%Y%m%d')}.log"
        
        # 使用 TimedRotatingFileHandler，每天轮转一次
        handler = logging.handlers.TimedRotatingFileHandler(
            filename=str(log_file),
            when='midnight',
            interval=1,
            backupCount=retention_days,  # 保留指定天数的日志文件
            encoding='utf-8'
        )
        
        # 只记录 WARNING 及以上级别
        handler.setLevel(logging.WARNING)
        handler.setFormatter(self.formatter)
        
        # 为 ERROR 及以上级别使用详细格式器
        error_handler = logging.handlers.TimedRotatingFileHandler(
            filename=str(log_dir / f"{worker_name}_error_{datetime.now().strftime('%Y%m%d')}.log"),
            when='midnight',
            interval=1,
            backupCount=retention_days,
            encoding='utf-8'
        )
        error_handler.setLevel(logging.ERROR)
        error_handler.setFormatter(self.detailed_formatter)
        
        # 添加到日志器（避免重复添加）
        if not any(isinstance(h, logging.handlers.TimedRotatingFileHandler) 
                   and worker_name in h.baseFilename for h in logger.handlers):
            logger.addHandler(handler)
            logger.addHandler(error_handler)
            logger.setLevel(logging.WARNING)  # 确保日志器级别不低于 WARNING
    
    def _get_retention_days(self, schedule_type: Optional[ScheduleType]) -> int:
        """根据备份周期类型确定日志保留天数
        
        Args:
            schedule_type: 备份周期类型
            
        Returns:
            保留天数（用于 TimedRotatingFileHandler 的 backupCount，每天轮转一次）
        """
        if schedule_type == ScheduleType.DAILY:
            # 日备份：保留12天（12个日志文件）
            return 12
        elif schedule_type == ScheduleType.MONTHLY:
            # 月备份：保留12个月（约365天，365个日志文件）
            return 365
        else:
            # 未知类型：默认保留12天
            return 12
    
    def _schedule_log_cleanup(self, schedule_type: Optional[ScheduleType]):
        """安排日志清理任务
        
        Args:
            schedule_type: 备份周期类型
        """
        # 立即执行一次清理
        self._cleanup_old_logs(schedule_type)
        
        # 注意：定期清理任务应该在主程序中安排，这里只提供清理方法
    
    def _cleanup_old_logs(self, schedule_type: Optional[ScheduleType]):
        """清理过期的日志文件
        
        Args:
            schedule_type: 备份周期类型
        """
        retention_days = self._get_retention_days(schedule_type)
        cutoff_date = datetime.now() - timedelta(days=retention_days)
        
        # 清理压缩线程日志
        self._cleanup_directory(self.compression_log_dir, cutoff_date)
        
        # 清理移动线程日志
        self._cleanup_directory(self.file_move_log_dir, cutoff_date)
    
    def _cleanup_directory(self, log_dir: Path, cutoff_date: datetime):
        """清理指定目录中的过期日志文件
        
        Args:
            log_dir: 日志目录
            cutoff_date: 截止日期（早于此日期的文件将被删除）
        """
        if not log_dir.exists():
            return
        
        deleted_count = 0
        for log_file in log_dir.glob("*.log*"):
            try:
                # 获取文件修改时间
                file_mtime = datetime.fromtimestamp(log_file.stat().st_mtime)
                
                # 如果文件修改时间早于截止日期，删除文件
                if file_mtime < cutoff_date:
                    log_file.unlink()
                    deleted_count += 1
            except Exception as e:
                # 删除失败，记录但不中断
                logger = logging.getLogger(__name__)
                logger.warning(f"清理日志文件失败: {log_file}, 错误: {str(e)}")
        
        if deleted_count > 0:
            logger = logging.getLogger(__name__)
            logger.info(f"已清理 {log_dir.name} 目录中的 {deleted_count} 个过期日志文件（保留时间: {cutoff_date.strftime('%Y-%m-%d')} 之后）")
    
    @staticmethod
    def get_schedule_type_from_task(backup_task) -> ScheduleType:
        """从备份任务中获取调度类型
        
        Args:
            backup_task: 备份任务对象
            
        Returns:
            调度类型
        """
        # 尝试从备份任务中获取调度类型
        # 注意：backup_task 可能没有直接的 schedule_type 字段
        # 需要通过关联的 scheduled_task 获取
        
        try:
            # 如果 backup_task 有 scheduled_task 关联
            if hasattr(backup_task, 'scheduled_task') and backup_task.scheduled_task:
                schedule_type_str = backup_task.scheduled_task.schedule_type
                if schedule_type_str == 'daily':
                    return ScheduleType.DAILY
                elif schedule_type_str == 'monthly':
                    return ScheduleType.MONTHLY
            
            # 如果 backup_task 有 task_type 字段，尝试推断
            if hasattr(backup_task, 'task_type'):
                task_type = str(backup_task.task_type)
                if 'monthly' in task_type.lower():
                    return ScheduleType.MONTHLY
                elif 'daily' in task_type.lower():
                    return ScheduleType.DAILY
        except Exception:
            pass
        
        # 默认返回 UNKNOWN
        return ScheduleType.UNKNOWN

