#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
配置管理器
Configuration Manager
"""

import logging
from typing import Dict, Any, Optional
from .settings import get_settings
from models.system_config import SystemConfig, ConfigType, ConfigCategory

logger = logging.getLogger(__name__)


class SystemConfigManager:
    """系统配置管理器"""

    def __init__(self):
        self.settings = get_settings()
        self._cache: Dict[str, Any] = {}
        self._cache_valid = False

    async def initialize(self, db_session):
        """初始化配置管理器"""
        try:
            # 从数据库加载配置
            await self._load_from_database(db_session)
            logger.info("配置管理器初始化完成")
        except Exception as e:
            logger.error(f"配置管理器初始化失败: {str(e)}")
            raise

    async def _load_from_database(self, db_session):
        """从数据库加载配置"""
        try:
            result = await db_session.execute(
                "SELECT config_key, config_value, config_type FROM system_config"
            )
            rows = result.fetchall()
            
            for row in rows:
                config_key, config_value, config_type = row
                # 根据类型转换值
                if config_type == ConfigType.INTEGER.value:
                    self._cache[config_key] = int(config_value)
                elif config_type == ConfigType.FLOAT.value:
                    self._cache[config_key] = float(config_value)
                elif config_type == ConfigType.BOOLEAN.value:
                    self._cache[config_key] = config_value.lower() in ['true', '1']
                else:
                    self._cache[config_key] = config_value
            
            self._cache_valid = True
            logger.info(f"从数据库加载了 {len(self._cache)} 个配置项")

        except Exception as e:
            logger.error(f"从数据库加载配置失败: {str(e)}")
            # 如果表不存在或失败，使用默认配置
            self._cache_valid = False

    def get_config(self, key: str, default: Any = None):
        """获取配置值"""
        # 优先从缓存读取
        if self._cache_valid and key in self._cache:
            return self._cache[key]
        
        # 从settings读取默认值
        if hasattr(self.settings, key):
            return getattr(self.settings, key)
        
        # 返回默认值
        return default

    async def set_config(self, key: str, value: Any, config_type: ConfigType, 
                        db_session, user: str = None):
        """设置配置"""
        try:
            # 验证配置值
            self._validate_config_value(key, value, config_type)
            
            # 检查配置是否存在
            result = await db_session.execute(
                "SELECT id FROM system_config WHERE config_key = :key",
                {"key": key}
            )
            row = result.first()
            
            if row:
                # 更新现有配置
                await db_session.execute(
                    """UPDATE system_config 
                       SET config_value = :value, updated_at = NOW(), updated_by = :user
                       WHERE config_key = :key""",
                    {"value": str(value), "user": user, "key": key}
                )
            else:
                # 插入新配置
                await db_session.execute(
                    """INSERT INTO system_config (config_key, config_value, config_type)
                       VALUES (:key, :value, :type)""",
                    {"key": key, "value": str(value), "type": config_type.value}
                )
            
            await db_session.commit()
            
            # 更新缓存
            self._cache[key] = value
            
            logger.info(f"配置已更新: {key} = {value}")
            return True

        except Exception as e:
            logger.error(f"设置配置失败 {key}: {str(e)}")
            await db_session.rollback()
            return False

    def _validate_config_value(self, key: str, value: Any, config_type: ConfigType):
        """验证配置值"""
        if config_type == ConfigType.INTEGER:
            if not isinstance(value, int):
                raise ValueError(f"配置 {key} 必须为整数")
        elif config_type == ConfigType.FLOAT:
            if not isinstance(value, (int, float)):
                raise ValueError(f"配置 {key} 必须为数字")
        elif config_type == ConfigType.BOOLEAN:
            if not isinstance(value, bool):
                raise ValueError(f"配置 {key} 必须为布尔值")


# 全局配置管理器实例
config_manager = SystemConfigManager()


def get_config_manager() -> SystemConfigManager:
    """获取配置管理器实例"""
    return config_manager

