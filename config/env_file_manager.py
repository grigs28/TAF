#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
环境变量文件管理模块
Environment File Manager Module
"""

import os
import re
import logging
from pathlib import Path
from typing import Dict, Optional, Any, List
from collections import OrderedDict

from .settings import get_settings

logger = logging.getLogger(__name__)


class EnvFileManager:
    """环境变量文件管理器"""

    def __init__(self, env_file: str = ".env"):
        """
        初始化环境变量文件管理器
        
        Args:
            env_file: 环境变量文件路径
        """
        self.env_file = Path(env_file)
        self._cache: Optional[Dict[str, str]] = None

    def read_env_file(self, include_defaults: bool = True) -> Dict[str, str]:
        """
        读取 .env 文件，并可选择性地合并默认设置
        
        Args:
            include_defaults: 是否合并 settings 中的默认值
            
        Returns:
            包含环境变量的字典
        """
        env_vars = OrderedDict()
        
        # 如果需要，先加载默认设置
        if include_defaults:
            settings = get_settings()
            # 从 settings 获取所有配置项的默认值
            # 使用 settings 的 model_fields 来获取所有字段（更可靠）
            try:
                # Pydantic Settings 模型的所有字段
                if hasattr(settings, 'model_fields'):
                    for key, field_info in settings.model_fields.items():
                        if key.isupper():
                            try:
                                value = getattr(settings, key, None)
                                if value is not None:
                                    # 将值转换为字符串
                                    if isinstance(value, bool):
                                        env_vars[key] = str(value).lower()
                                    elif isinstance(value, (int, float)):
                                        env_vars[key] = str(value)
                                    else:
                                        env_vars[key] = str(value)
                            except Exception:
                                pass
                else:
                    # 回退到 dir() 方法
                    for key in dir(settings):
                        if key.isupper() and not key.startswith('_'):
                            try:
                                value = getattr(settings, key)
                                if value is not None:
                                    # 将值转换为字符串
                                    if isinstance(value, bool):
                                        env_vars[key] = str(value).lower()
                                    elif isinstance(value, (int, float)):
                                        env_vars[key] = str(value)
                                    else:
                                        env_vars[key] = str(value)
                            except Exception:
                                pass
            except Exception as e:
                logger.warning(f"加载settings默认值时出错: {str(e)}")
                # 继续执行，即使加载默认值失败
        
        # 从 .env 文件读取配置（会覆盖默认值）
        if self.env_file.exists():
            try:
                with open(self.env_file, 'r', encoding='utf-8') as f:
                    for line_num, line in enumerate(f, 1):
                        # 保留原始行（包括换行符）
                        original_line = line
                        line = line.rstrip('\n\r')
                        
                        # 跳过空行和注释
                        line_stripped = line.strip()
                        if not line_stripped or line_stripped.startswith('#'):
                            continue
                        
                        # 解析 KEY=VALUE 格式
                        # 支持值中包含等号的情况（使用第一个等号分割）
                        if '=' in line_stripped:
                            key, value = line_stripped.split('=', 1)
                            key = key.strip()
                            value = value.strip()
                            
                            # 移除值的引号（如果存在）
                            if (value.startswith('"') and value.endswith('"')) or \
                               (value.startswith("'") and value.endswith("'")):
                                value = value[1:-1]
                            
                            env_vars[key] = value
                        else:
                            logger.warning(f"第 {line_num} 行格式不正确: {line}")
            
            except Exception as e:
                logger.error(f"读取环境变量文件失败: {str(e)}", exc_info=True)
                raise
        else:
            logger.info(f"环境变量文件不存在: {self.env_file}，使用默认配置")
        
        self._cache = env_vars.copy()
        return env_vars

    def write_env_file(self, updates: Dict[str, str], backup: bool = True) -> bool:
        """
        更新 .env 文件
        
        Args:
            updates: 要更新的配置字典 {key: value}
            backup: 是否备份原文件
            
        Returns:
            是否成功
        """
        try:
            # 读取现有文件内容
            lines: List[str] = []
            existing_keys = set()
            
            if self.env_file.exists():
                with open(self.env_file, 'r', encoding='utf-8') as f:
                    lines = f.readlines()
                
                # 备份原文件
                if backup:
                    backup_file = self.env_file.with_suffix('.env.backup')
                    with open(backup_file, 'w', encoding='utf-8') as f:
                        f.writelines(lines)
                    logger.info(f"已备份环境变量文件: {backup_file}")
            else:
                # 文件不存在，创建新文件
                logger.info(f"环境变量文件不存在，将创建新文件: {self.env_file}")
            
            # 更新现有行
            updated_keys = set()
            for i, line in enumerate(lines):
                line_stripped = line.strip()
                
                # 保留注释和空行
                if not line_stripped or line_stripped.startswith('#'):
                    continue
                
                # 检查是否是配置行
                if '=' in line_stripped:
                    key = line_stripped.split('=', 1)[0].strip()
                    
                    # 如果这个键需要更新
                    if key in updates:
                        value = updates[key]
                        # 特殊处理：NOTIFICATION_EVENTS 和 TAPE_DEVICES_CACHE 等 JSON 格式的值
                        # 这些值需要正确转义内部的双引号
                        if key in ['NOTIFICATION_EVENTS', 'TAPE_DEVICES_CACHE']:
                            # JSON 值：需要转义内部双引号并用双引号包裹
                            import json
                            try:
                                # 如果已经是有效的 JSON 字符串，转义内部双引号
                                if isinstance(value, str):
                                    # 尝试解析以确保是有效的 JSON
                                    try:
                                        json.loads(value)
                                        # 转义内部的双引号
                                        escaped_value = value.replace('"', '\\"')
                                        lines[i] = f'{key}="{escaped_value}"\n'
                                    except json.JSONDecodeError:
                                        # 如果不是有效的 JSON，直接转义双引号
                                        escaped_value = value.replace('"', '\\"')
                                        lines[i] = f'{key}="{escaped_value}"\n'
                                else:
                                    # 如果不是字符串，转换为 JSON 字符串
                                    json_str = json.dumps(value, ensure_ascii=False)
                                    escaped_value = json_str.replace('"', '\\"')
                                    lines[i] = f'{key}="{escaped_value}"\n'
                            except Exception as e:
                                logger.warning(f"处理 {key} 的值时出错: {e}，使用简单转义")
                                escaped_value = str(value).replace('"', '\\"')
                                lines[i] = f'{key}="{escaped_value}"\n'
                        elif ' ' in str(value) or '#' in str(value) or '=' in str(value) or '"' in str(value):
                            # 如果值包含空格、特殊字符或双引号，使用引号并转义双引号
                            escaped_value = str(value).replace('"', '\\"')
                            lines[i] = f"{key}=\"{escaped_value}\"\n"
                        else:
                            lines[i] = f"{key}={value}\n"
                        updated_keys.add(key)
                        existing_keys.add(key)
            
            # 添加新配置（不在现有文件中的）
            for key, value in updates.items():
                if key not in updated_keys:
                    # 特殊处理：NOTIFICATION_EVENTS 和 TAPE_DEVICES_CACHE 等 JSON 格式的值
                    if key in ['NOTIFICATION_EVENTS', 'TAPE_DEVICES_CACHE']:
                        # JSON 值：需要转义内部双引号并用双引号包裹
                        import json
                        try:
                            # 如果已经是有效的 JSON 字符串，转义内部双引号
                            if isinstance(value, str):
                                # 尝试解析以确保是有效的 JSON
                                try:
                                    json.loads(value)
                                    # 转义内部的双引号
                                    escaped_value = value.replace('"', '\\"')
                                    lines.append(f'{key}="{escaped_value}"\n')
                                except json.JSONDecodeError:
                                    # 如果不是有效的 JSON，直接转义双引号
                                    escaped_value = value.replace('"', '\\"')
                                    lines.append(f'{key}="{escaped_value}"\n')
                            else:
                                # 如果不是字符串，转换为 JSON 字符串
                                json_str = json.dumps(value, ensure_ascii=False)
                                escaped_value = json_str.replace('"', '\\"')
                                lines.append(f'{key}="{escaped_value}"\n')
                        except Exception as e:
                            logger.warning(f"处理 {key} 的值时出错: {e}，使用简单转义")
                            escaped_value = str(value).replace('"', '\\"')
                            lines.append(f'{key}="{escaped_value}"\n')
                    elif ' ' in str(value) or '#' in str(value) or '=' in str(value) or '"' in str(value):
                        # 如果值包含空格、特殊字符或双引号，使用引号并转义双引号
                        escaped_value = str(value).replace('"', '\\"')
                        lines.append(f"{key}=\"{escaped_value}\"\n")
                    else:
                        lines.append(f"{key}={value}\n")
            
            # 写入文件
            with open(self.env_file, 'w', encoding='utf-8') as f:
                f.writelines(lines)
            
            logger.info(f"已更新环境变量文件: {self.env_file}, 更新了 {len(updates)} 个配置项")
            
            # 更新缓存
            if self._cache is None:
                self._cache = OrderedDict()
            self._cache.update(updates)
            
            return True
        
        except Exception as e:
            logger.error(f"写入环境变量文件失败: {str(e)}", exc_info=True)
            return False

    def get_value(self, key: str, default: Optional[str] = None, use_settings_default: bool = True) -> Optional[str]:
        """
        获取环境变量值
        
        Args:
            key: 环境变量键
            default: 默认值（如果 use_settings_default 为 False 时使用）
            use_settings_default: 是否从 settings 获取默认值
            
        Returns:
            环境变量值
        """
        if self._cache is None:
            self.read_env_file(include_defaults=use_settings_default)
        
        # 先从缓存中获取
        value = self._cache.get(key)
        if value is not None:
            return value
        
        # 如果缓存中没有，且允许使用 settings 默认值，则从 settings 获取
        if use_settings_default:
            try:
                settings = get_settings()
                if hasattr(settings, key):
                    settings_value = getattr(settings, key)
                    if settings_value is not None:
                        return str(settings_value)
            except Exception:
                pass
        
        # 返回提供的默认值
        return default

    def set_value(self, key: str, value: str, write_immediately: bool = True) -> bool:
        """
        设置环境变量值
        
        Args:
            key: 环境变量键
            value: 环境变量值
            write_immediately: 是否立即写入文件
            
        Returns:
            是否成功
        """
        if write_immediately:
            return self.write_env_file({key: value})
        else:
            if self._cache is None:
                self.read_env_file(include_defaults=True)
            self._cache[key] = value
            return True

    def update_values(self, updates: Dict[str, str], write_immediately: bool = True) -> bool:
        """
        批量更新环境变量值
        
        Args:
            updates: 包含更新的字典
            write_immediately: 是否立即写入文件
            
        Returns:
            是否成功
        """
        if write_immediately:
            return self.write_env_file(updates)
        else:
            if self._cache is None:
                self.read_env_file(include_defaults=True)
            self._cache.update(updates)
            return True

    def delete_value(self, key: str, write_immediately: bool = True) -> bool:
        """
        删除环境变量（仅从.env文件删除，不影响settings默认值）
        
        Args:
            key: 环境变量键
            write_immediately: 是否立即写入文件
            
        Returns:
            是否成功
        """
        try:
            if self._cache is None:
                self.read_env_file(include_defaults=True)
            
            # 读取文件内容
            lines: List[str] = []
            if self.env_file.exists():
                with open(self.env_file, 'r', encoding='utf-8') as f:
                    lines = f.readlines()
            
            # 移除包含该键的行
            new_lines = []
            key_found = False
            for line in lines:
                line_stripped = line.strip()
                if line_stripped and '=' in line_stripped:
                    line_key = line_stripped.split('=', 1)[0].strip()
                    if line_key == key:
                        key_found = True
                        continue  # 跳过这一行
                new_lines.append(line)
            
            # 如果键不存在于文件中，直接返回成功
            if not key_found:
                return True
            
            # 写入文件
            if write_immediately:
                with open(self.env_file, 'w', encoding='utf-8') as f:
                    f.writelines(new_lines)
                logger.info(f"已从.env文件删除配置: {key}")
            
            # 更新缓存（如果键在缓存中且来自.env文件）
            # 注意：如果键来自settings默认值，我们不从缓存中删除它
            if key in self._cache:
                # 重新加载以获取settings默认值
                if write_immediately:
                    self.reload()
            
            return True
        
        except Exception as e:
            logger.error(f"删除环境变量失败: {str(e)}", exc_info=True)
            return False

    def reload(self, include_defaults: bool = True) -> Dict[str, str]:
        """
        重新加载环境变量文件
        
        Args:
            include_defaults: 是否合并 settings 中的默认值
            
        Returns:
            包含环境变量的字典
        """
        self._cache = None
        return self.read_env_file(include_defaults=include_defaults)


# 全局实例
_env_manager: Optional[EnvFileManager] = None


def get_env_manager(env_file: str = ".env") -> EnvFileManager:
    """
    获取环境变量文件管理器实例
    
    Args:
        env_file: 环境变量文件路径
        
    Returns:
        环境变量文件管理器实例
    """
    global _env_manager
    if _env_manager is None or str(_env_manager.env_file) != env_file:
        _env_manager = EnvFileManager(env_file)
    return _env_manager

