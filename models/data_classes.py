#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
数据类定义模块（备用实现）
Data Classes Module (Backup Implementation)
"""

from dataclasses import dataclass, field
from datetime import datetime
from typing import Optional, List, Dict, Any
import enum


class LogLevel(enum.Enum):
    """日志级别"""
    DEBUG = "DEBUG"
    INFO = "INFO"
    WARNING = "WARNING"
    ERROR = "ERROR"
    CRITICAL = "CRITICAL"


class ConfigType(enum.Enum):
    """配置类型"""
    SYSTEM = "system"
    BACKUP = "backup"
    TAPE = "tape"
    SCHEDULER = "scheduler"


class BackupTaskType(enum.Enum):
    """备份任务类型"""
    FULL = "full"
    INCREMENTAL = "incremental"
    DIFFERENTIAL = "differential"
    MONTHLY_FULL = "monthly_full"


class BackupTaskStatus(enum.Enum):
    """备份任务状态"""
    PENDING = "pending"
    RUNNING = "running"
    COMPLETED = "completed"
    FAILED = "failed"
    CANCELLED = "cancelled"
    PAUSED = "paused"


class ScheduleType(enum.Enum):
    """调度类型"""
    ONCE = "once"
    INTERVAL = "interval"
    DAILY = "daily"
    WEEKLY = "weekly"
    MONTHLY = "monthly"
    YEARLY = "yearly"
    CRON = "cron"


class TaskActionType(enum.Enum):
    """任务动作类型"""
    BACKUP = "backup"
    RECOVERY = "recovery"
    CLEANUP = "cleanup"
    HEALTH_CHECK = "health_check"
    RETENTION_CHECK = "retention_check"
    CUSTOM = "custom"


class ScheduledTaskStatus(enum.Enum):
    """计划任务状态"""
    ACTIVE = "active"
    INACTIVE = "inactive"
    PAUSED = "paused"
    ERROR = "error"


@dataclass
class User:
    """用户数据类"""
    id: int = 0
    username: str = ""
    email: str = ""
    full_name: str = ""
    is_active: bool = True
    is_admin: bool = False
    last_login: Optional[datetime] = None
    created_at: Optional[datetime] = None

    def to_dict(self) -> Dict[str, Any]:
        """转换为字典"""
        return {
            'id': self.id,
            'username': self.username,
            'email': self.email,
            'full_name': self.full_name,
            'is_active': self.is_active,
            'is_admin': self.is_admin,
            'last_login': self.last_login.isoformat() if self.last_login else None,
            'created_at': self.created_at.isoformat() if self.created_at else None
        }

    @classmethod
    def from_dict(cls, data: Dict[str, Any]) -> 'User':
        """从字典创建实例"""
        return cls(
            id=data.get('id', 0),
            username=data.get('username', ''),
            email=data.get('email', ''),
            full_name=data.get('full_name', ''),
            is_active=data.get('is_active', True),
            is_admin=data.get('is_admin', False),
            last_login=datetime.fromisoformat(data['last_login']) if data.get('last_login') else None,
            created_at=datetime.fromisoformat(data['created_at']) if data.get('created_at') else None
        )


@dataclass
class BackupTask:
    """备份任务数据类"""
    id: int = 0
    task_name: str = ""
    task_type: BackupTaskType = BackupTaskType.FULL
    description: str = ""
    status: BackupTaskStatus = BackupTaskStatus.PENDING
    is_template: bool = False
    template_id: Optional[int] = None

    # 备份配置
    source_paths: List[str] = field(default_factory=list)
    exclude_patterns: List[str] = field(default_factory=list)
    compression_enabled: bool = True
    encryption_enabled: bool = False
    retention_days: int = 180
    enable_simple_scan: bool = True

    # 时间信息
    scheduled_time: Optional[datetime] = None
    started_at: Optional[datetime] = None
    completed_at: Optional[datetime] = None

    # 执行信息
    executed_by: str = ""
    worker_id: str = ""

    # 统计信息
    total_files: int = 0
    processed_files: int = 0
    total_bytes: int = 0
    processed_bytes: int = 0
    compressed_bytes: int = 0

    # 磁带信息
    tape_device: str = ""
    tape_id: str = ""
    backup_set_id: str = ""

    # 结果信息
    progress_percent: float = 0.0
    error_message: str = ""
    result_summary: Dict[str, Any] = field(default_factory=dict)
    scan_status: str = "pending"
    scan_completed_at: Optional[datetime] = None

    # 系统字段
    created_by: str = ""
    created_at: Optional[datetime] = None
    updated_at: Optional[datetime] = None

    def to_dict(self) -> Dict[str, Any]:
        """转换为字典"""
        return {
            'id': self.id,
            'task_name': self.task_name,
            'task_type': self.task_type.value if self.task_type else None,
            'description': self.description,
            'status': self.status.value if self.status else None,
            'is_template': self.is_template,
            'template_id': self.template_id,
            'source_paths': self.source_paths,
            'exclude_patterns': self.exclude_patterns,
            'compression_enabled': self.compression_enabled,
            'encryption_enabled': self.encryption_enabled,
            'retention_days': self.retention_days,
            'scheduled_time': self.scheduled_time.isoformat() if self.scheduled_time else None,
            'started_at': self.started_at.isoformat() if self.started_at else None,
            'completed_at': self.completed_at.isoformat() if self.completed_at else None,
            'executed_by': self.executed_by,
            'worker_id': self.worker_id,
            'total_files': self.total_files,
            'processed_files': self.processed_files,
            'total_bytes': self.total_bytes,
            'processed_bytes': self.processed_bytes,
            'compressed_bytes': self.compressed_bytes,
            'tape_device': self.tape_device,
            'tape_id': self.tape_id,
            'backup_set_id': self.backup_set_id,
            'progress_percent': self.progress_percent,
            'error_message': self.error_message,
            'result_summary': self.result_summary,
            'scan_status': self.scan_status,
            'scan_completed_at': self.scan_completed_at.isoformat() if self.scan_completed_at else None,
            'created_by': self.created_by,
            'created_at': self.created_at.isoformat() if self.created_at else None,
            'updated_at': self.updated_at.isoformat() if self.updated_at else None
        }

    @classmethod
    def from_dict(cls, data: Dict[str, Any]) -> 'BackupTask':
        """从字典创建实例"""
        return cls(
            id=data.get('id', 0),
            task_name=data.get('task_name', ''),
            task_type=BackupTaskType(data['task_type']) if data.get('task_type') else BackupTaskType.FULL,
            description=data.get('description', ''),
            status=BackupTaskStatus(data['status']) if data.get('status') else BackupTaskStatus.PENDING,
            is_template=data.get('is_template', False),
            template_id=data.get('template_id'),
            source_paths=data.get('source_paths', []),
            exclude_patterns=data.get('exclude_patterns', []),
            compression_enabled=data.get('compression_enabled', True),
            encryption_enabled=data.get('encryption_enabled', False),
            retention_days=data.get('retention_days', 180),
            enable_simple_scan=data.get('enable_simple_scan', True),
            scheduled_time=datetime.fromisoformat(data['scheduled_time']) if data.get('scheduled_time') else None,
            started_at=datetime.fromisoformat(data['started_at']) if data.get('started_at') else None,
            completed_at=datetime.fromisoformat(data['completed_at']) if data.get('completed_at') else None,
            executed_by=data.get('executed_by', ''),
            worker_id=data.get('worker_id', ''),
            total_files=data.get('total_files', 0),
            processed_files=data.get('processed_files', 0),
            total_bytes=data.get('total_bytes', 0),
            processed_bytes=data.get('processed_bytes', 0),
            compressed_bytes=data.get('compressed_bytes', 0),
            tape_device=data.get('tape_device', ''),
            tape_id=data.get('tape_id', ''),
            backup_set_id=data.get('backup_set_id', ''),
            progress_percent=data.get('progress_percent', 0.0),
            error_message=data.get('error_message', ''),
            result_summary=data.get('result_summary', {}),
            scan_status=data.get('scan_status', 'pending'),
            scan_completed_at=datetime.fromisoformat(data['scan_completed_at']) if data.get('scan_completed_at') else None,
            created_by=data.get('created_by', ''),
            created_at=datetime.fromisoformat(data['created_at']) if data.get('created_at') else None,
            updated_at=datetime.fromisoformat(data['updated_at']) if data.get('updated_at') else None
        )


@dataclass
class ScheduledTask:
    """计划任务数据类"""
    id: int = 0
    task_name: str = ""
    description: str = ""
    schedule_type: ScheduleType = ScheduleType.ONCE
    schedule_config: Dict[str, Any] = field(default_factory=dict)
    action_type: TaskActionType = TaskActionType.BACKUP
    action_config: Dict[str, Any] = field(default_factory=dict)
    status: ScheduledTaskStatus = ScheduledTaskStatus.ACTIVE
    enabled: bool = True

    # 运行时间信息
    next_run_time: Optional[datetime] = None
    last_run_time: Optional[datetime] = None
    last_success_time: Optional[datetime] = None
    last_failure_time: Optional[datetime] = None

    # 统计信息
    total_runs: int = 0
    success_runs: int = 0
    failure_runs: int = 0
    average_duration: Optional[int] = None

    # 错误信息
    last_error: str = ""

    # 元数据
    tags: List[str] = field(default_factory=list)
    task_metadata: Dict[str, Any] = field(default_factory=dict)

    # 系统字段
    created_at: Optional[datetime] = None
    updated_at: Optional[datetime] = None

    def to_dict(self) -> Dict[str, Any]:
        """转换为字典"""
        return {
            'id': self.id,
            'task_name': self.task_name,
            'description': self.description,
            'schedule_type': self.schedule_type.value if self.schedule_type else None,
            'schedule_config': self.schedule_config,
            'action_type': self.action_type.value if self.action_type else None,
            'action_config': self.action_config,
            'status': self.status.value if self.status else None,
            'enabled': self.enabled,
            'next_run_time': self.next_run_time.isoformat() if self.next_run_time else None,
            'last_run_time': self.last_run_time.isoformat() if self.last_run_time else None,
            'last_success_time': self.last_success_time.isoformat() if self.last_success_time else None,
            'last_failure_time': self.last_failure_time.isoformat() if self.last_failure_time else None,
            'total_runs': self.total_runs,
            'success_runs': self.success_runs,
            'failure_runs': self.failure_runs,
            'average_duration': self.average_duration,
            'last_error': self.last_error,
            'tags': self.tags,
            'task_metadata': self.task_metadata,
            'created_at': self.created_at.isoformat() if self.created_at else None,
            'updated_at': self.updated_at.isoformat() if self.updated_at else None
        }

    @classmethod
    def from_dict(cls, data: Dict[str, Any]) -> 'ScheduledTask':
        """从字典创建实例"""
        return cls(
            id=data.get('id', 0),
            task_name=data.get('task_name', ''),
            description=data.get('description', ''),
            schedule_type=ScheduleType(data['schedule_type']) if data.get('schedule_type') else ScheduleType.ONCE,
            schedule_config=data.get('schedule_config', {}),
            action_type=TaskActionType(data['action_type']) if data.get('action_type') else TaskActionType.BACKUP,
            action_config=data.get('action_config', {}),
            status=ScheduledTaskStatus(data['status']) if data.get('status') else ScheduledTaskStatus.ACTIVE,
            enabled=data.get('enabled', True),
            next_run_time=datetime.fromisoformat(data['next_run_time']) if data.get('next_run_time') else None,
            last_run_time=datetime.fromisoformat(data['last_run_time']) if data.get('last_run_time') else None,
            last_success_time=datetime.fromisoformat(data['last_success_time']) if data.get('last_success_time') else None,
            last_failure_time=datetime.fromisoformat(data['last_failure_time']) if data.get('last_failure_time') else None,
            total_runs=data.get('total_runs', 0),
            success_runs=data.get('success_runs', 0),
            failure_runs=data.get('failure_runs', 0),
            average_duration=data.get('average_duration'),
            last_error=data.get('last_error', ''),
            tags=data.get('tags', []),
            task_metadata=data.get('task_metadata', {}),
            created_at=datetime.fromisoformat(data['created_at']) if data.get('created_at') else None,
            updated_at=datetime.fromisoformat(data['updated_at']) if data.get('updated_at') else None
        )