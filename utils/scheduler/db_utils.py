#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
数据库工具函数
Database Utility Functions
"""

import logging
from config.database import db_manager

logger = logging.getLogger(__name__)


def is_opengauss() -> bool:
    """检查当前数据库是否为openGauss"""
    database_url = db_manager.settings.DATABASE_URL
    return "opengauss" in database_url.lower()


async def get_opengauss_connection():
    """获取openGauss数据库连接"""
    import asyncpg
    import re
    
    database_url = db_manager.settings.DATABASE_URL
    url = database_url.replace("opengauss://", "postgresql://")
    pattern = r'postgresql://([^:]+):([^@]+)@([^:]+):(\d+)/(.+)'
    match = re.match(pattern, url)
    if not match:
        raise ValueError("无法解析openGauss数据库URL")
    
    username, password, host, port, database = match.groups()
    
    return await asyncpg.connect(
        host=host,
        port=int(port),
        user=username,
        password=password,
        database=database
    )

