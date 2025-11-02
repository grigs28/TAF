#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
磁带盒类
Tape Cartridge Class
"""

from dataclasses import dataclass, field
from datetime import datetime, timedelta
from typing import Optional, Dict, Any
from enum import Enum

from config.settings import get_settings


class TapeStatus(Enum):
    """磁带状态枚举"""
    NEW = "new"                   # 新磁带
    AVAILABLE = "available"       # 可用
    IN_USE = "in_use"            # 使用中
    FULL = "full"                # 已满
    EXPIRED = "expired"          # 已过期
    ERROR = "error"              # 错误
    MAINTENANCE = "maintenance"   # 维护中


@dataclass
class TapeCartridge:
    """磁带盒类"""

    # 基本信息
    tape_id: str
    label: str
    status: TapeStatus = TapeStatus.NEW

    # 容量信息
    capacity_bytes: int = 0
    used_bytes: int = 0
    free_bytes: int = field(init=False)

    # 时间信息
    created_date: Optional[datetime] = None
    expiry_date: Optional[datetime] = None
    last_used_date: Optional[datetime] = None
    last_erase_date: Optional[datetime] = None
    last_access_date: Optional[datetime] = None

    # 物理信息
    location: str = ""
    media_type: str = "LTO"  # LTO, DDS, AIT等
    generation: int = 8      # LTO-8, LTO-9等
    serial_number: str = ""
    manufacturer: str = ""

    # 使用统计
    write_count: int = 0
    read_count: int = 0
    pass_count: int = 0      # 磁带通过次数
    load_count: int = 0      # 加载次数

    # 健康状态
    health_score: int = 100  # 0-100
    error_count: int = 0
    warning_count: int = 0

    # 备份组信息
    backup_group: Optional[str] = None  # YYYY-MM格式的备份组
    backup_sets: list = field(default_factory=list)

    def __post_init__(self):
        """初始化后处理"""
        if self.created_date is None:
            self.created_date = datetime.now()

        if self.expiry_date is None:
            settings = get_settings()
            self.expiry_date = self.created_date + timedelta(days=settings.DEFAULT_RETENTION_MONTHS * 30)

        self.free_bytes = self.capacity_bytes - self.used_bytes

        # 确保status是TapeStatus枚举
        if isinstance(self.status, str):
            self.status = TapeStatus(status)

    @property
    def usage_percent(self) -> float:
        """使用率百分比"""
        if self.capacity_bytes == 0:
            return 0.0
        return (self.used_bytes / self.capacity_bytes) * 100

    @property
    def is_full(self) -> bool:
        """是否已满"""
        return self.used_bytes >= self.capacity_bytes or self.usage_percent >= 95.0

    @property
    def is_expired(self) -> bool:
        """是否已过期（仅比较年月）"""
        if self.expiry_date is None:
            return False
        
        # 仅比较年月，忽略日
        now = datetime.now()
        expiry_year = self.expiry_date.year
        expiry_month = self.expiry_date.month
        current_year = now.year
        current_month = now.month
        
        # 判断是否过期：当前年月 >= 过期年月
        return (current_year > expiry_year) or (current_year == expiry_year and current_month >= expiry_month)

    @property
    def days_until_expiry(self) -> int:
        """距离过期天数"""
        if self.expiry_date is None:
            return -1
        delta = self.expiry_date - datetime.now()
        return delta.days

    @property
    def age_days(self) -> int:
        """磁带年龄（天数）"""
        if self.created_date is None:
            return 0
        delta = datetime.now() - self.created_date
        return delta.days

    def is_available_for_backup(self, required_space: int = 0) -> bool:
        """检查是否可用于备份"""
        if self.status != TapeStatus.AVAILABLE:
            return False

        if self.is_expired:
            return False

        if self.is_full:
            return False

        if required_space > 0 and self.free_bytes < required_space:
            return False

        return True

    def update_usage(self, bytes_written: int):
        """更新使用量"""
        self.used_bytes += bytes_written
        self.free_bytes = self.capacity_bytes - self.used_bytes
        self.last_used_date = datetime.now()
        self.last_access_date = datetime.now()
        self.write_count += 1

        # 如果写满，更新状态
        if self.is_full:
            self.status = TapeStatus.FULL

    def update_read_access(self):
        """更新读取访问"""
        self.last_access_date = datetime.now()
        self.read_count += 1

    def mark_as_used(self, backup_group: str = None):
        """标记为使用中"""
        self.status = TapeStatus.IN_USE
        self.last_used_date = datetime.now()
        self.load_count += 1

        if backup_group:
            self.backup_group = backup_group

    def mark_as_available(self):
        """标记为可用"""
        if not self.is_expired and not self.is_full:
            self.status = TapeStatus.AVAILABLE

    def mark_as_expired(self):
        """标记为过期"""
        self.status = TapeStatus.EXPIRED

    def mark_error(self):
        """标记为错误状态"""
        self.status = TapeStatus.ERROR
        self.error_count += 1

    def reset_usage(self):
        """重置使用量（擦除后调用）"""
        self.used_bytes = 0
        self.free_bytes = self.capacity_bytes
        self.last_erase_date = datetime.now()
        self.backup_group = None
        self.backup_sets.clear()
        self.mark_as_available()

    def assign_to_backup_group(self, backup_group: str):
        """分配到备份组"""
        self.backup_group = backup_group
        if backup_group not in self.backup_sets:
            self.backup_sets.append(backup_group)

    def update_health_score(self, new_score: int):
        """更新健康分数"""
        self.health_score = max(0, min(100, new_score))

    def increment_error_count(self):
        """增加错误计数"""
        self.error_count += 1
        if self.error_count > 5:
            self.status = TapeStatus.ERROR

    def increment_warning_count(self):
        """增加警告计数"""
        self.warning_count += 1

    def get_summary(self) -> Dict[str, Any]:
        """获取磁带摘要信息"""
        return {
            'tape_id': self.tape_id,
            'label': self.label,
            'status': self.status.value,
            'location': self.location,
            'capacity_gb': round(self.capacity_bytes / (1024**3), 2),
            'used_gb': round(self.used_bytes / (1024**3), 2),
            'free_gb': round(self.free_bytes / (1024**3), 2),
            'usage_percent': round(self.usage_percent, 2),
            'created_date': self.created_date.isoformat() if self.created_date else None,
            'expiry_date': self.expiry_date.isoformat() if self.expiry_date else None,
            'days_until_expiry': self.days_until_expiry,
            'age_days': self.age_days,
            'health_score': self.health_score,
            'error_count': self.error_count,
            'backup_group': self.backup_group,
            'media_type': self.media_type,
            'generation': self.generation
        }

    def get_detailed_info(self) -> Dict[str, Any]:
        """获取详细信息"""
        return {
            **self.get_summary(),
            'last_used_date': self.last_used_date.isoformat() if self.last_used_date else None,
            'last_erase_date': self.last_erase_date.isoformat() if self.last_erase_date else None,
            'last_access_date': self.last_access_date.isoformat() if self.last_access_date else None,
            'write_count': self.write_count,
            'read_count': self.read_count,
            'pass_count': self.pass_count,
            'load_count': self.load_count,
            'warning_count': self.warning_count,
            'manufacturer': self.manufacturer,
            'serial_number': self.serial_number,
            'backup_sets': self.backup_sets.copy(),
            'is_full': self.is_full,
            'is_expired': self.is_expired,
            'is_available_for_backup': self.is_available_for_backup()
        }

    def __str__(self) -> str:
        """字符串表示"""
        return f"TapeCartridge(id={self.tape_id}, label={self.label}, status={self.status.value}, usage={self.usage_percent:.1f}%)"

    def __repr__(self) -> str:
        """详细字符串表示"""
        return (f"TapeCartridge(id={self.tape_id}, label={self.label}, status={self.status.value}, "
                f"capacity={self.capacity_bytes}, used={self.used_bytes}, "
                f"location={self.location}, backup_group={self.backup_group})")