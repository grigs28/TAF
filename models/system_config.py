#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
系统配置数据模型
System Configuration Model
"""

from datetime import datetime
from sqlalchemy import Column, Integer, String, DateTime, Text, Boolean, Enum
from sqlalchemy.orm import validates
import enum

from .base import BaseModel


class ConfigType(enum.Enum):
    """配置类型"""
    STRING = "string"
    INTEGER = "integer"
    FLOAT = "float"
    BOOLEAN = "boolean"
    JSON = "json"
    ENCRYPTED = "encrypted"


class ConfigCategory(enum.Enum):
    """配置分类"""
    APPLICATION = "application"
    DATABASE = "database"
    WEB = "web"
    SECURITY = "security"
    TAPE = "tape"
    BACKUP = "backup"
    SCHEDULER = "scheduler"
    NOTIFICATION = "notification"
    PERFORMANCE = "performance"
    MONITORING = "monitoring"
    STORAGE = "storage"


class SystemConfig(BaseModel):
    """系统配置表"""

    __tablename__ = "system_config"

    # 配置标识
    config_key = Column(String(100), unique=True, nullable=False, comment="配置键")
    config_value = Column(Text, nullable=False, comment="配置值")
    config_type = Column(Enum(ConfigType), nullable=False, comment="配置类型")
    category = Column(Enum(ConfigCategory), comment="配置分类")
    description = Column(Text, comment="配置说明")

    # 配置属性
    is_sensitive = Column(Boolean, default=False, comment="是否为敏感信息")
    can_modify = Column(Boolean, default=True, comment="是否可修改")
    default_value = Column(Text, comment="默认值")

    # 配置约束
    min_value = Column(String(50), comment="最小值（类型检查）")
    max_value = Column(String(50), comment="最大值（类型检查）")
    allowed_values = Column(Text, comment="允许的值（JSON格式）")

    # 统计信息
    modification_count = Column(Integer, default=0, comment="修改次数")
    last_read_at = Column(DateTime(timezone=True), comment="最后读取时间")
    read_count = Column(Integer, default=0, comment="读取次数")

    def __repr__(self):
        return f"<SystemConfig(key={self.config_key}, type={self.config_type.value})>"

    @validates('config_value')
    def validate_config_value(self, key, value):
        """验证配置值"""
        if self.config_type == ConfigType.INTEGER:
            try:
                int(value)
            except ValueError:
                raise ValueError(f"配置 {self.config_key} 必须为整数")
        elif self.config_type == ConfigType.FLOAT:
            try:
                float(value)
            except ValueError:
                raise ValueError(f"配置 {self.config_key} 必须为浮点数")
        elif self.config_type == ConfigType.BOOLEAN:
            if value not in ['true', 'false', 'True', 'False', '1', '0']:
                raise ValueError(f"配置 {self.config_key} 必须为布尔值")
        return value

    def get_value(self):
        """获取配置值（根据类型转换）"""
        if self.config_type == ConfigType.INTEGER:
            return int(self.config_value)
        elif self.config_type == ConfigType.FLOAT:
            return float(self.config_value)
        elif self.config_type == ConfigType.BOOLEAN:
            return self.config_value.lower() in ['true', '1']
        elif self.config_type == ConfigType.JSON:
            import json
            return json.loads(self.config_value)
        else:
            return self.config_value

    def set_value(self, value):
        """设置配置值"""
        if self.config_type == ConfigType.JSON:
            import json
            self.config_value = json.dumps(value, ensure_ascii=False)
        else:
            self.config_value = str(value)

