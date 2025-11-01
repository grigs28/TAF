#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
数据库初始化模块
Database Initialization Module
"""

import logging
from typing import Optional
from sqlalchemy import create_engine, text
from sqlalchemy.exc import OperationalError, ProgrammingError

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
            
            # 解析URL获取数据库名
            database_name = self._extract_database_name(database_url)
            if not database_name:
                logger.warning("无法从DATABASE_URL中提取数据库名")
                return False
            
            # 连接到默认的postgres数据库
            admin_url = self._get_admin_url(database_url, database_name)
            
            logger.info(f"检查数据库 {database_name} 是否存在...")
            
            # 使用同步引擎创建数据库
            engine = create_engine(
                admin_url,
                isolation_level="AUTOCOMMIT",
                connect_args={"application_name": "tape_backup_init"}
            )
            
            try:
                with engine.connect() as conn:
                    # 检查数据库是否存在
                    result = conn.execute(
                        text(
                            "SELECT 1 FROM pg_database WHERE datname = :db_name"
                        ),
                        {"db_name": database_name}
                    )
                    exists = result.fetchone() is not None
                    
                    if exists:
                        logger.info(f"数据库 {database_name} 已存在")
                        return True
                    
                    # 创建数据库
                    logger.info(f"创建数据库 {database_name}...")
                    conn.execute(
                        text(f'CREATE DATABASE "{database_name}"')
                    )
                    logger.info(f"数据库 {database_name} 创建成功")
                    return True
                    
            except ProgrammingError as e:
                # 可能是权限问题
                logger.error(f"无法创建数据库，可能是权限不足: {str(e)}")
                logger.info(f"请手动创建数据库: CREATE DATABASE {database_name};")
                return False
            except Exception as e:
                logger.error(f"检查或创建数据库时发生错误: {str(e)}")
                return False
            finally:
                engine.dispose()
                
        except Exception as e:
            logger.error(f"数据库初始化失败: {str(e)}")
            return False
    
    def _extract_database_name(self, database_url: str) -> Optional[str]:
        """从数据库URL中提取数据库名"""
        try:
            # 格式: postgresql://user:password@host:port/database
            # 或:    opengauss://user:password@host:port/database
            if "@" in database_url and "/" in database_url:
                # 获取@之后的部分
                after_at = database_url.split("@")[1]
                # 获取最后一个/之后的部分
                parts = after_at.split("/")
                if len(parts) > 1:
                    database_name = parts[-1]
                    # 移除可能的查询参数
                    if "?" in database_name:
                        database_name = database_name.split("?")[0]
                    return database_name
        except Exception as e:
            logger.error(f"解析数据库名失败: {str(e)}")
        return None
    
    def _get_admin_url(self, database_url: str, current_db: str) -> str:
        """获取管理员的URL（连接到postgres数据库）"""
        # 将URL中的数据库名替换为postgres
        admin_url = database_url.replace(f"/{current_db}", "/postgres")
        # 确保使用postgresql协议
        if admin_url.startswith("opengauss://"):
            admin_url = admin_url.replace("opengauss://", "postgresql://")
        return admin_url

