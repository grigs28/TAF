#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
数据库初始化模块
Database Initialization Module
"""

import logging
import re
from typing import Optional
import psycopg2
from psycopg2.extensions import ISOLATION_LEVEL_AUTOCOMMIT

from .settings import get_settings

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
                logger.debug(f"SQLite数据库无需创建: {database_url}")
                return True
            
            # 处理opengauss URL
            if database_url.startswith("opengauss://"):
                database_url = database_url.replace("opengauss://", "postgresql://", 1)
            
            # 使用正则表达式解析URL
            pattern = r'postgresql://([^:]+):([^@]+)@([^:]+):(\d+)/(.+)'
            match = re.match(pattern, database_url)
            
            if not match:
                logger.error("无法解析数据库连接URL")
                return False
            
            username, password, host, port, database = match.groups()
            
            logger.info(f"检查数据库 {database} 是否存在...")
            
            # 连接到 postgres 数据库（默认数据库）
            conn = psycopg2.connect(
                host=host,
                port=port,
                user=username,
                password=password,
                database='postgres'
            )
            conn.set_isolation_level(ISOLATION_LEVEL_AUTOCOMMIT)
            cursor = conn.cursor()
            
            # 检查数据库是否已存在
            cursor.execute("SELECT 1 FROM pg_database WHERE datname = %s", (database,))
            exists = cursor.fetchone()
            
            if not exists:
                # 创建数据库
                cursor.execute(f'CREATE DATABASE "{database}"')
                logger.info(f"数据库 {database} 创建成功")
            else:
                logger.info(f"数据库 {database} 已存在")
            
            # 关闭到 postgres 的连接
            cursor.close()
            conn.close()
            
            # 连接到目标数据库并授予权限
            conn = psycopg2.connect(
                host=host,
                port=port,
                user=username,
                password=password,
                database=database
            )
            conn.set_isolation_level(ISOLATION_LEVEL_AUTOCOMMIT)
            cursor = conn.cursor()
            
            # 授予用户在 public schema 上的权限
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
                
        except psycopg2.errors.InsufficientPrivilege as e:
            # 可能是权限问题
            logger.error(f"无法创建数据库，可能是权限不足: {str(e)}")
            logger.info(f"请手动创建数据库并授权")
            return False
        except Exception as e:
            logger.error(f"数据库初始化失败: {str(e)}")
            return False
    

