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
                # PostgreSQL/openGauss支持连接池，处理版本检测问题
                try:
                    # 首先尝试使用标准配置创建引擎
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
                except Exception as version_error:
                    if "Could not determine version" in str(version_error):
                        logger.warning("检测到openGauss版本解析问题，使用简化配置...")
                        # 使用简化配置，禁用一些特性
                        self.engine = create_engine(
                            database_url,
                            echo=self.settings.DEBUG,
                            connect_args={"application_name": "enterprise_tape_backup"}
                        )
                        self.async_engine = create_async_engine(
                            async_database_url,
                            echo=self.settings.DEBUG,
                            connect_args={"application_name": "enterprise_tape_backup"}
                        )
                    else:
                        raise

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
            from models import backup, tape, user, system_log, system_config

            # 处理openGauss版本检测问题
            try:
                async with self.async_engine.begin() as conn:
                    await conn.run_sync(Base.metadata.create_all)
                logger.info("数据库表创建完成")
            except Exception as version_error:
                if "Could not determine version" in str(version_error):
                    # 忽略版本检测错误，继续创建表
                    logger.warning("检测到openGauss版本解析问题，尝试继续创建表...")
                    try:
                        # 使用同步引擎创建表
                        with self.engine.begin() as conn:
                            Base.metadata.create_all(conn)
                        logger.info("使用同步引擎成功创建数据库表")
                    except Exception as sync_error:
                        logger.error(f"同步创建表失败: {str(sync_error)}")
                        raise version_error
                else:
                    raise

        except Exception as e:
            logger.error(f"创建数据库表失败: {str(e)}")
            raise

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