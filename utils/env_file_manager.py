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

    def read_env_file(self) -> Dict[str, str]:
        """
        读取 .env 文件
        
        Returns:
            包含环境变量的字典
        """
        env_vars = OrderedDict()
        
        if not self.env_file.exists():
            logger.warning(f"环境变量文件不存在: {self.env_file}")
            return env_vars
        
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
                        # 如果值包含空格或特殊字符，使用引号
                        if ' ' in str(value) or '#' in str(value) or '=' in str(value):
                            lines[i] = f"{key}=\"{value}\"\n"
                        else:
                            lines[i] = f"{key}={value}\n"
                        updated_keys.add(key)
                        existing_keys.add(key)
            
            # 添加新配置（不在现有文件中的）
            for key, value in updates.items():
                if key not in updated_keys:
                    # 如果值包含空格或特殊字符，使用引号
                    if ' ' in str(value) or '#' in str(value) or '=' in str(value):
                        lines.append(f"{key}=\"{value}\"\n")
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

    def get_value(self, key: str, default: Optional[str] = None) -> Optional[str]:
        """
        获取环境变量值
        
        Args:
            key: 环境变量键
            default: 默认值
            
        Returns:
            环境变量值
        """
        if self._cache is None:
            self.read_env_file()
        
        return self._cache.get(key, default)

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
                self.read_env_file()
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
                self.read_env_file()
            self._cache.update(updates)
            return True

    def delete_value(self, key: str, write_immediately: bool = True) -> bool:
        """
        删除环境变量
        
        Args:
            key: 环境变量键
            write_immediately: 是否立即写入文件
            
        Returns:
            是否成功
        """
        try:
            if self._cache is None:
                self.read_env_file()
            
            if key not in self._cache:
                return True
            
            # 读取文件内容
            lines: List[str] = []
            if self.env_file.exists():
                with open(self.env_file, 'r', encoding='utf-8') as f:
                    lines = f.readlines()
            
            # 移除包含该键的行
            new_lines = []
            for line in lines:
                line_stripped = line.strip()
                if line_stripped and '=' in line_stripped:
                    line_key = line_stripped.split('=', 1)[0].strip()
                    if line_key == key:
                        continue  # 跳过这一行
                new_lines.append(line)
            
            # 写入文件
            if write_immediately:
                with open(self.env_file, 'w', encoding='utf-8') as f:
                    f.writelines(new_lines)
            
            # 更新缓存
            if key in self._cache:
                del self._cache[key]
            
            return True
        
        except Exception as e:
            logger.error(f"删除环境变量失败: {str(e)}", exc_info=True)
            return False

    def reload(self) -> Dict[str, str]:
        """
        重新加载环境变量文件
        
        Returns:
            包含环境变量的字典
        """
        self._cache = None
        return self.read_env_file()


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

