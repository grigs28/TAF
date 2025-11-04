#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
日志管理模块
Logging Management Module
"""

import os
import logging
import logging.handlers
from datetime import datetime
from pathlib import Path

from config.settings import get_settings


def setup_logging():
    """设置日志系统"""
    settings = get_settings()

    # 创建日志目录
    log_dir = Path(settings.LOG_FILE).parent
    log_dir.mkdir(exist_ok=True)

    # 配置根日志器
    root_logger = logging.getLogger()
    root_logger.setLevel(getattr(logging, settings.LOG_LEVEL.upper()))

    # 清除现有处理器
    root_logger.handlers.clear()

    # 创建格式器（包含更详细的信息）
    formatter = logging.Formatter(
        '%(asctime)s - %(name)s - %(levelname)s - %(funcName)s:%(lineno)d - %(message)s',
        datefmt='%Y-%m-%d %H:%M:%S'
    )
    
    # 详细格式器（用于错误日志，包含堆栈跟踪）
    detailed_formatter = logging.Formatter(
        '%(asctime)s - %(name)s - %(levelname)s - %(funcName)s:%(lineno)d - %(pathname)s - %(message)s\n%(exc_info)s',
        datefmt='%Y-%m-%d %H:%M:%S'
    )

    # 控制台处理器
    console_handler = logging.StreamHandler()
    console_handler.setLevel(logging.INFO)
    console_handler.setFormatter(formatter)
    root_logger.addHandler(console_handler)

    # 文件处理器（按日期轮转）
    file_handler = logging.handlers.TimedRotatingFileHandler(
        filename=settings.LOG_FILE,
        when='midnight',
        interval=1,
        backupCount=30,
        encoding='utf-8'
    )
    file_handler.setLevel(getattr(logging, settings.LOG_LEVEL.upper()))
    file_handler.setFormatter(formatter)
    root_logger.addHandler(file_handler)

    # 错误日志文件处理器
    error_log_file = log_dir / 'error.log'
    error_handler = logging.handlers.TimedRotatingFileHandler(
        filename=error_log_file,
        when='midnight',
        interval=1,
        backupCount=30,
        encoding='utf-8'
    )
    error_handler.setLevel(logging.ERROR)
    error_handler.setFormatter(formatter)
    root_logger.addHandler(error_handler)
    
    # 设置错误日志格式，包含异常堆栈
    error_handler.formatter = detailed_formatter

    # 设置第三方库日志级别
    logging.getLogger('sqlalchemy.engine').setLevel(logging.WARNING)
    logging.getLogger('sqlalchemy.pool').setLevel(logging.WARNING)
    logging.getLogger('hypercorn').setLevel(logging.WARNING)
    logging.getLogger('websockets').setLevel(logging.WARNING)

    # 记录启动日志
    logger = logging.getLogger(__name__)
    logger.info("=" * 60)
    logger.info("企业级磁带备份系统 - 日志系统初始化完成")
    logger.info(f"日志级别: {settings.LOG_LEVEL}")
    logger.info(f"日志文件: {settings.LOG_FILE}")
    logger.info(f"启动时间: {datetime.now().strftime('%Y-%m-%d %H:%M:%S')}")
    logger.info("=" * 60)


def get_logger(name: str) -> logging.Logger:
    """获取日志器"""
    return logging.getLogger(name)