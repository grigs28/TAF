#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
数据库初始化模块
Database Initialization Module
"""

import logging
from typing import Optional
import psycopg2

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
            
            # 解析URL获取数据库连接信息
            db_info = self._parse_database_url(database_url)
            if not db_info:
                logger.warning("无法从DATABASE_URL中解析数据库信息")
                return False
            
            database_name = db_info['database']
            
            logger.info(f"检查数据库 {database_name} 是否存在...")
            
            # 先连接到默认的postgres数据库
            conn = psycopg2.connect(
                host=db_info['host'],
                port=db_info['port'],
                user=db_info['user'],
                password=db_info['password'],
                database='postgres'
            )
            conn.autocommit = True
            cur = conn.cursor()
            
            # 检查数据库是否存在
            cur.execute("SELECT 1 FROM pg_database WHERE datname = %s", (database_name,))
            db_exists = cur.fetchone() is not None
            
            if not db_exists:
                # 创建数据库
                logger.info(f"创建数据库 {database_name}...")
                cur.execute(f'CREATE DATABASE "{database_name}"')
                logger.info(f"数据库 {database_name} 创建成功")
            else:
                logger.info(f"数据库 {database_name} 已存在")
            
            cur.close()
            conn.close()
            
            # 如果数据库不存在或刚创建，需要设置权限
            if not db_exists or True:  # 总是检查并设置权限
                await self._setup_database_permissions(db_info, database_name)
            
            return True
                
        except psycopg2.errors.InsufficientPrivilege as e:
            # 可能是权限问题
            logger.error(f"无法创建数据库，可能是权限不足: {str(e)}")
            logger.info(f"请手动创建数据库: CREATE DATABASE {database_name};")
            return False
        except Exception as e:
            logger.error(f"数据库初始化失败: {str(e)}")
            return False
    
    def _parse_database_url(self, database_url: str) -> Optional[dict]:
        """从数据库URL中解析连接信息"""
        try:
            # 格式: postgresql://user:password@host:port/database
            # 或:    opengauss://user:password@host:port/database
            
            # 移除协议前缀
            url = database_url
            if url.startswith("opengauss://"):
                url = url.replace("opengauss://", "", 1)
            elif url.startswith("postgresql://"):
                url = url.replace("postgresql://", "", 1)
            else:
                logger.error(f"不支持的数据库URL协议: {database_url}")
                return None
            
            # 解析认证信息
            if "@" not in url:
                logger.error(f"无效的数据库URL格式: {database_url}")
                return None
            
            auth_part, server_part = url.split("@", 1)
            
            # 解析用户名和密码
            if ":" not in auth_part:
                logger.error(f"无法解析用户名和密码: {database_url}")
                return None
            
            user, password = auth_part.split(":", 1)
            
            # 解析服务器信息和数据库名
            if "/" not in server_part:
                logger.error(f"无法解析数据库名: {database_url}")
                return None
            
            server_part, database = server_part.split("/", 1)
            
            # 移除可能的查询参数
            if "?" in database:
                database = database.split("?")[0]
            
            # 解析主机和端口
            if ":" in server_part:
                host, port = server_part.split(":", 1)
                port = int(port)
            else:
                host = server_part
                port = 5432  # 默认端口
            
            return {
                'host': host,
                'port': port,
                'user': user,
                'password': password,
                'database': database
            }
        except Exception as e:
            logger.error(f"解析数据库URL失败: {str(e)}")
            return None
    
    async def _setup_database_permissions(self, db_info: dict, database_name: str):
        """设置数据库权限"""
        try:
            # 连接到目标数据库
            conn = psycopg2.connect(
                host=db_info['host'],
                port=db_info['port'],
                user=db_info['user'],
                password=db_info['password'],
                database=database_name
            )
            conn.autocommit = True
            cur = conn.cursor()
            
            # 授权给当前用户
            username = db_info['user']
            logger.info(f"设置数据库 {database_name} 的权限给用户 {username}...")
            
            # 授予SCHEMA权限
            cur.execute("GRANT ALL ON SCHEMA public TO \"{}\"".format(username))
            cur.execute("ALTER SCHEMA public OWNER TO \"{}\"".format(username))
            
            # 授予所有表的权限
            cur.execute("GRANT ALL PRIVILEGES ON ALL TABLES IN SCHEMA public TO \"{}\"".format(username))
            cur.execute("GRANT ALL PRIVILEGES ON ALL SEQUENCES IN SCHEMA public TO \"{}\"".format(username))
            
            # 设置默认权限
            cur.execute("ALTER DEFAULT PRIVILEGES IN SCHEMA public GRANT ALL ON TABLES TO \"{}\"".format(username))
            cur.execute("ALTER DEFAULT PRIVILEGES IN SCHEMA public GRANT ALL ON SEQUENCES TO \"{}\"".format(username))
            
            logger.info(f"数据库权限设置完成")
            
            cur.close()
            conn.close()
            
        except Exception as e:
            logger.warning(f"设置数据库权限失败: {str(e)}，可能权限不足，但不影响使用")
            # 不抛出异常，允许继续运行

