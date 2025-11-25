#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
数据库初始化模块
Database Initialization Module
"""

import logging
import re
from typing import Optional

from .settings import get_settings
from utils.db_connection_helper import (
    get_psycopg_connection_from_url,
    parse_database_url,
    set_autocommit
)

logger = logging.getLogger(__name__)


class DatabaseInitializer:
    """数据库初始化器"""
    
    def __init__(self):
        self.settings = get_settings()
    
    async def ensure_database_exists(self) -> bool:
        """确保数据库存在，如果不存在则创建"""
        try:
            database_url = self.settings.DATABASE_URL
            
            # 只处理PostgreSQL/openGauss数据库
            if not (database_url.startswith("postgresql://") or database_url.startswith("opengauss://")):
                logger.debug(f"非PostgreSQL/openGauss数据库，跳过数据库创建: {database_url}")
                return True
            
            # 解析URL
            username, password, host, port, database = parse_database_url(database_url)
            
            logger.info(f"检查数据库 {database} 是否存在...")
            
            # 连接到 postgres 数据库（默认数据库）
            from utils.db_connection_helper import get_psycopg_connection
            conn, is_psycopg3 = get_psycopg_connection(
                host=host,
                port=port,
                user=username,
                password=password,
                database='postgres',
                prefer_psycopg3=True
            )
            set_autocommit(conn, is_psycopg3, autocommit=True)
            cursor = conn.cursor()
            
            # 检查数据库是否已存在
            cursor.execute("SELECT 1 FROM pg_database WHERE datname = %s", (database,))
            exists = cursor.fetchone()
            
            if not exists:
                # 创建数据库，指定所有者和模板
                cursor.execute(f'CREATE DATABASE "{database}" OWNER "{username}" TEMPLATE template0')
                logger.info(f"数据库 {database} 创建成功，所有者为 {username}")
            else:
                logger.info(f"数据库 {database} 已存在")
            
            # 关闭到 postgres 的连接
            cursor.close()
            conn.close()
            
            # 连接到目标数据库并授予权限
            conn, is_psycopg3 = get_psycopg_connection(
                host=host,
                port=port,
                user=username,
                password=password,
                database=database,
                prefer_psycopg3=True
            )
            set_autocommit(conn, is_psycopg3, autocommit=True)
            cursor = conn.cursor()
            
            # 授予用户在 public schema 上的权限
            try:
                # 先尝试将public schema的所有者设置为当前用户
                cursor.execute(f'ALTER SCHEMA public OWNER TO "{username}"')
                logger.info(f"已将 public schema 的所有者设置为 {username}")
            except Exception as e:
                logger.warning(f"设置public schema所有者时出现警告: {str(e)}")
            
            try:
                cursor.execute(f'GRANT ALL ON SCHEMA public TO "{username}"')
                cursor.execute(f'GRANT ALL PRIVILEGES ON ALL TABLES IN SCHEMA public TO "{username}"')
                cursor.execute(f'GRANT ALL PRIVILEGES ON ALL SEQUENCES IN SCHEMA public TO "{username}"')
                
                # 设置默认权限
                cursor.execute(f'ALTER DEFAULT PRIVILEGES IN SCHEMA public GRANT ALL ON TABLES TO "{username}"')
                cursor.execute(f'ALTER DEFAULT PRIVILEGES IN SCHEMA public GRANT ALL ON SEQUENCES TO "{username}"')
                
                logger.info(f"已为用户 {username} 授予数据库 {database} 的权限")
            except Exception as e:
                logger.warning(f"授予权限时出现警告: {str(e)}")
            
            cursor.close()
            conn.close()
            
            return True
                
        except Exception as e:
            # 检查是否是权限错误（兼容 psycopg2 和 psycopg3）
            error_str = str(e).lower()
            if 'insufficient' in error_str and 'privilege' in error_str:
                logger.error(f"无法创建数据库，可能是权限不足: {str(e)}")
                logger.info(f"请手动创建数据库并授权")
                return False
            logger.error(f"数据库初始化失败: {str(e)}")
            return False
    

