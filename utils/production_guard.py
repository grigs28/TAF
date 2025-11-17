#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
生产环境保护器
Production Environment Guard
"""

import os
import sys
from functools import wraps
from typing import Callable, Any

logger = __import__('utils.logger').get_logger(__name__)


class ProductionGuard:
    """生产环境保护器 - 防止测试代码在生产环境运行"""

    PRODUCTION_ENV_VAR = 'PRODUCTION_ENV'
    UNATTENDED_MODE_VAR = 'UNATTENDED_MODE'

    @classmethod
    def is_production(cls) -> bool:
        """检查是否为生产环境"""
        return os.getenv(cls.PRODUCTION_ENV_VAR, '').lower() in ('true', '1', 'yes')

    @classmethod
    def is_unattended_mode(cls) -> bool:
        """检查是否为无人值守模式"""
        return os.getenv(cls.UNATTENDED_MODE_VAR, '').lower() in ('true', '1', 'yes')

    @classmethod
    def block_interactive_input(cls, func: Callable) -> Callable:
        """装饰器：在生产环境和无人值守模式下阻止交互式输入"""
        @wraps(func)
        def wrapper(*args, **kwargs):
            if cls.is_production() or cls.is_unattended_mode():
                raise RuntimeError(
                    f"交互式输入被阻止 - 环境: PRODUCTION_ENV={cls.is_production()}, "
                    f"UNATTENDED_MODE={cls.is_unattended_mode()}. "
                    f"函数: {func.__module__}.{func.__name__}"
                )
            return func(*args, **kwargs)
        return wrapper

    @classmethod
    def safe_input(cls, prompt: str = "", default: str = None) -> str:
        """安全的输入函数 - 在生产环境返回默认值"""
        if cls.is_production() or cls.is_unattended_mode():
            if default is not None:
                logger.info(f"无人值守模式: 使用默认值 '{default}' 替代交互输入 '{prompt}'")
                return default
            else:
                raise RuntimeError(
                    f"无人值守模式不允许交互输入，且没有提供默认值。提示: '{prompt}'"
                )

        # 开发/测试环境允许正常输入
        try:
            return input(prompt)
        except (EOFError, KeyboardInterrupt):
            if default is not None:
                logger.warning(f"输入中断，使用默认值: {default}")
                return default
            raise


# 全局替换input函数
def install_production_guard():
    """安装生产环境保护器"""
    import builtins

    original_input = builtins.input

    def safe_input_wrapper(prompt: str = "") -> str:
        return ProductionGuard.safe_input(prompt)

    # 替换内置input函数
    builtins.input = safe_input_wrapper

    logger.info("生产环境保护器已安装")


# 自动安装保护器
if ProductionGuard.is_production() or ProductionGuard.is_unattended_mode():
    install_production_guard()