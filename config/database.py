#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
数据库管理模块
Database Management Module
"""

import asyncio
import logging
from typing import AsyncGenerator, Optional
from sqlalchemy import create_engine, MetaData
from sqlalchemy.ext.asyncio import AsyncSession, create_async_engine, async_sessionmaker
from sqlalchemy.orm import sessionmaker, Session
from sqlalchemy.pool import StaticPool

from .settings import get_settings
from models.base import Base

logger = logging.getLogger(__name__)

# 元数据
metadata = MetaData()


class DatabaseManager:
    """数据库管理器"""

    def __init__(self):
        self.settings = get_settings()
        self.engine = None
        self.async_engine = None
        self.SessionLocal = None
        self.AsyncSessionLocal = None
        self._initialized = False

    async def initialize(self):
        """初始化数据库连接"""
        try:
            # 构建数据库URL
            database_url = self._build_database_url()
            async_database_url = self._build_async_database_url()

            # 根据数据库类型创建引擎
            if database_url.startswith("sqlite"):
                # SQLite不支持连接池
                self.engine = create_engine(
                    database_url,
                    echo=self.settings.DEBUG,
                    pool_pre_ping=True
                )
                self.async_engine = create_async_engine(
                    async_database_url,
                    echo=self.settings.DEBUG,
                    pool_pre_ping=True
                )
            else:
                # PostgreSQL/openGauss支持连接池
                self.engine = create_engine(
                    database_url,
                    pool_size=self.settings.DB_POOL_SIZE,
                    max_overflow=self.settings.DB_MAX_OVERFLOW,
                    echo=self.settings.DEBUG,
                    pool_pre_ping=True
                )
                self.async_engine = create_async_engine(
                    async_database_url,
                    pool_size=self.settings.DB_POOL_SIZE,
                    max_overflow=self.settings.DB_MAX_OVERFLOW,
                    echo=self.settings.DEBUG,
                    pool_pre_ping=True
                )

            # 创建会话工厂
            self.SessionLocal = sessionmaker(
                autocommit=False,
                autoflush=False,
                bind=self.engine
            )

            self.AsyncSessionLocal = async_sessionmaker(
                self.async_engine,
                class_=AsyncSession,
                expire_on_commit=False
            )

            # 创建表
            await self.create_tables()

            self._initialized = True
            logger.info("数据库连接初始化成功")

        except Exception as e:
            logger.error(f"数据库初始化失败: {str(e)}")
            raise

    def _build_database_url(self) -> str:
        """构建同步数据库URL"""
        url = self.settings.DATABASE_URL
        # 将opengauss URL转换为postgresql URL（兼容）
        if url.startswith("opengauss://"):
            return url.replace("opengauss://", "postgresql://")
        return url

    def _build_async_database_url(self) -> str:
        """构建异步数据库URL"""
        # 将同步URL转换为异步URL
        url = self.settings.DATABASE_URL
        if url.startswith("sqlite:///"):
            return url.replace("sqlite:///", "sqlite+aiosqlite:///")
        elif url.startswith("postgresql://"):
            return url.replace("postgresql://", "postgresql+asyncpg://")
        elif url.startswith("opengauss://"):
            return url.replace("opengauss://", "postgresql+asyncpg://")
        else:
            return url

    async def create_tables(self):
        """创建数据库表"""
        try:
            # 导入所有模型以确保它们被注册
            from models import backup, tape, user, system_log, system_config, scheduled_task
            import psycopg2
            import re
            
            # 对于openGauss，使用psycopg2直接创建表，避免版本检查问题
            database_url = self.settings.DATABASE_URL
            if "opengauss" in database_url.lower():
                logger.info("检测到openGauss数据库，使用psycopg2创建表...")
                await self._create_tables_with_psycopg2()
            else:
                # PostgreSQL/SQLite使用SQLAlchemy引擎来创建表
                with self.engine.begin() as conn:
                    Base.metadata.create_all(conn)
                logger.info("数据库表创建完成")

        except Exception as e:
            logger.error(f"创建数据库表失败: {str(e)}")
            raise
    
    async def _create_tables_with_psycopg2(self):
        """使用psycopg2直接连接创建表（解决openGauss版本解析问题）"""
        import psycopg2
        import re
        
        # 解析数据库URL获取连接信息
        database_url = self.settings.DATABASE_URL
        if database_url.startswith("opengauss://"):
            database_url = database_url.replace("opengauss://", "postgresql://", 1)
        
        pattern = r'postgresql://([^:]+):([^@]+)@([^:]+):(\d+)/(.+)'
        match = re.match(pattern, database_url)
        if not match:
            raise ValueError("无法解析数据库连接URL")
        
        username, password, host, port, database = match.groups()
        
        # 使用psycopg2直接连接
        conn = psycopg2.connect(
            host=host,
            port=port,
            user=username,
            password=password,
            database=database
        )
        
        try:
            # 使用SQLAlchemy的Base.metadata.create_all，但通过psycopg2连接
            # 这样可以避免版本检查，同时保留SQLAlchemy的所有特性
            from sqlalchemy import create_engine
            from sqlalchemy.schema import CreateTable
            
            # 创建一个临时的PostgreSQL引擎用于生成SQL（指定PostgreSQL dialect）
            temp_engine = create_engine("postgresql://", pool_pre_ping=False)
            
            with conn.cursor() as cur:
                # 先创建枚举类型
                for table in Base.metadata.tables.values():
                    for column in table.columns:
                        if hasattr(column.type, 'enums'):
                            # 这是一个枚举类型
                            enum_name = column.type.name
                            # 获取枚举值：可能是Enum对象列表，也可能是字符串列表
                            try:
                                enum_values = [e.value for e in column.type.enums]
                            except AttributeError:
                                # 如果已经是字符串列表
                                enum_values = list(column.type.enums)
                            
                            # 检查枚举类型是否已存在
                            cur.execute("""
                                SELECT 1 FROM pg_type WHERE typname = %s
                            """, (enum_name,))
                            if not cur.fetchone():
                                # 创建枚举类型
                                quoted_values = ', '.join([f"'{v}'" for v in enum_values])
                                enum_sql = f"CREATE TYPE {enum_name} AS ENUM ({quoted_values})"
                                cur.execute(enum_sql)
                                logger.info(f"创建枚举类型: {enum_name} with values: {enum_values}")
                            else:
                                # 如果已存在，检查其值
                                cur.execute("""
                                    SELECT enumlabel FROM pg_enum WHERE enumtypid = 
                                    (SELECT oid FROM pg_type WHERE typname = %s)
                                    ORDER BY enumsortorder
                                """, (enum_name,))
                                existing_values = [row[0] for row in cur.fetchall()]
                                logger.info(f"枚举类型 {enum_name} 已存在，包含值: {existing_values}")
                
                # 创建表
                for table in Base.metadata.sorted_tables:
                    # 检查表是否已存在
                    cur.execute("""
                        SELECT 1 FROM information_schema.tables WHERE table_name = %s
                    """, (table.name,))
                    if not cur.fetchone():
                        create_sql = str(CreateTable(table).compile(compile_kwargs={"literal_binds": True}, dialect=temp_engine.dialect))
                        cur.execute(create_sql)
                        logger.info(f"创建表: {table.name}")
            
            conn.commit()
            logger.info("使用psycopg2成功创建数据库表")
        except Exception as e:
            conn.rollback()
            raise
        finally:
            conn.close()

    def get_sync_session(self) -> Session:
        """获取同步数据库会话"""
        if not self._initialized:
            raise RuntimeError("数据库未初始化")
        return self.SessionLocal()

    async def get_async_session(self) -> AsyncGenerator[AsyncSession, None]:
        """获取异步数据库会话"""
        if not self._initialized:
            raise RuntimeError("数据库未初始化")

        async with self.AsyncSessionLocal() as session:
            try:
                yield session
            except Exception:
                await session.rollback()
                raise
            finally:
                await session.close()

    async def execute_sql(self, sql: str, params: dict = None):
        """执行原生SQL"""
        async with self.async_engine.begin() as conn:
            result = await conn.execute(sql, params or {})
            return result

    async def close(self):
        """关闭数据库连接"""
        try:
            if self.async_engine:
                await self.async_engine.dispose()
            if self.engine:
                self.engine.dispose()
            logger.info("数据库连接已关闭")
        except Exception as e:
            logger.error(f"关闭数据库连接时发生错误: {str(e)}")

    async def health_check(self) -> bool:
        """数据库健康检查"""
        try:
            # 对于openGauss，使用psycopg2直接连接避免版本解析问题
            database_url = self.settings.DATABASE_URL
            if "opengauss" in database_url.lower():
                import psycopg2
                import re
                
                # 解析数据库URL
                if database_url.startswith("opengauss://"):
                    database_url = database_url.replace("opengauss://", "postgresql://", 1)
                
                pattern = r'postgresql://([^:]+):([^@]+)@([^:]+):(\d+)/(.+)'
                match = re.match(pattern, database_url)
                if not match:
                    return False
                
                username, password, host, port, database = match.groups()
                
                # 使用psycopg2直接连接测试
                conn = psycopg2.connect(
                    host=host,
                    port=port,
                    user=username,
                    password=password,
                    database=database,
                    connect_timeout=5
                )
                cur = conn.cursor()
                cur.execute("SELECT 1")
                cur.close()
                conn.close()
            else:
                from sqlalchemy import text
                async with self.async_engine.begin() as conn:
                    await conn.execute(text("SELECT 1"))
            return True
        except Exception as e:
            logger.error(f"数据库健康检查失败: {str(e)}")
            return False


# 全局数据库管理器实例
db_manager = DatabaseManager()


async def get_db() -> AsyncGenerator[AsyncSession, None]:
    """依赖注入：获取数据库会话"""
    async for session in db_manager.get_async_session():
        yield session


def get_sync_db():
    """依赖注入：获取同步数据库会话"""
    return db_manager.get_sync_session()