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
        self._is_opengauss_cache: Optional[bool] = None

    def is_opengauss_database(self) -> bool:
        """对外提供是否为 openGauss 的统一判断"""
        if self._is_opengauss_cache is None:
            self._is_opengauss_cache = self._detect_opengauss()
        return bool(self._is_opengauss_cache)

    def _detect_opengauss(self) -> bool:
        """根据URL/显式配置/服务器版本自动识别openGauss"""
        raw_url = (self.settings.DATABASE_URL or "").lower()
        if "opengauss" in raw_url:
            return True

        flavor = getattr(self.settings, "DB_FLAVOR", None)
        if flavor and "opengauss" in flavor.lower():
            logger.info("通过 DB_FLAVOR 显式配置识别为 openGauss 数据库")
            return True

        try:
            import psycopg2
        except ImportError:
            logger.debug("psycopg2 未安装，无法自动检测 openGauss，默认为非 openGauss")
            return False

        conn = None
        try:
            conn = psycopg2.connect(self._build_database_url())
            with conn.cursor() as cur:
                cur.execute("SELECT version()")
                row = cur.fetchone()
                version_str = row[0] if row else ""
                if version_str and "opengauss" in version_str.lower():
                    logger.info(f"自动检测到 openGauss 数据库: {version_str}")
                    return True
        except Exception as detect_error:
            logger.debug(f"自动检测 openGauss 数据库失败: {detect_error}")
        finally:
            if conn:
                try:
                    conn.close()
                except Exception:
                    pass
        return False

    async def initialize(self):
        """初始化数据库连接"""
        try:
            # 构建数据库URL
            raw_database_url = self.settings.DATABASE_URL
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
                # 对于openGauss，需要特殊处理以避免版本解析错误
                is_opengauss = self.is_opengauss_database()
                
                # 对于openGauss，完全不创建SQLAlchemy引擎，避免版本解析错误
                if is_opengauss:
                    logger.warning("检测到openGauss数据库，跳过SQLAlchemy引擎创建，将使用原生SQL查询")
                    self.engine = None
                    self.async_engine = None
                    self.AsyncSessionLocal = None
                    self.SessionLocal = None
                else:
                    connect_args = {}
                    self.engine = create_engine(
                        database_url,
                        pool_size=self.settings.DB_POOL_SIZE,
                        max_overflow=self.settings.DB_MAX_OVERFLOW,
                        echo=self.settings.DEBUG,
                        pool_pre_ping=True,
                        connect_args=connect_args
                    )
                    self.async_engine = create_async_engine(
                        async_database_url,
                        pool_size=self.settings.DB_POOL_SIZE,
                        max_overflow=self.settings.DB_MAX_OVERFLOW,
                        echo=self.settings.DEBUG,
                        pool_pre_ping=True,
                        connect_args=connect_args
                    )
                    self.AsyncSessionLocal = async_sessionmaker(
                        self.async_engine,
                        class_=AsyncSession,
                        expire_on_commit=False
                    )
                    self.SessionLocal = sessionmaker(
                        autocommit=False,
                        autoflush=False,
                        bind=self.engine
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
            if self.is_opengauss_database():
                logger.info("检测到openGauss数据库，使用psycopg2创建表...")
                await self._create_tables_with_psycopg2()
            else:
                # PostgreSQL/SQLite使用SQLAlchemy引擎来创建表
                with self.engine.begin() as conn:
                    Base.metadata.create_all(conn)
                logger.info("数据库表创建完成")
                
                # 检查并添加缺失的字段（字段迁移）- 仅对PostgreSQL/openGauss
                if not database_url.startswith("sqlite"):
                    await self._migrate_missing_columns_postgresql()

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
                # 先创建枚举类型（明确定义所有枚举类型）
                from models.scheduled_task import ScheduleType, ScheduledTaskStatus, TaskActionType
                from models.system_log import LogLevel, LogCategory, OperationType, ErrorLevel
                from models.backup import BackupTaskType, BackupTaskStatus, BackupFileType
                
                # 定义所有需要的枚举类型（确保名称和值正确）
                enum_definitions = {
                    'scheduletype': [e.value for e in ScheduleType],  # ['once', 'interval', 'daily', 'weekly', 'monthly', 'yearly', 'cron']
                    'scheduledtaskstatus': [e.value for e in ScheduledTaskStatus],  # ['active', 'inactive', 'running', 'paused', 'error']
                    'taskactiontype': [e.value for e in TaskActionType],  # ['backup', 'recovery', 'cleanup', 'health_check', 'retention_check', 'custom']
                    'loglevel': [e.value for e in LogLevel],  # ['debug', 'info', 'warning', 'error', 'critical']
                    'logcategory': [e.value for e in LogCategory],  # ['system', 'backup', 'recovery', 'tape', 'user', 'security', 'performance', 'api', 'web', 'database']
                    'operationtype': [e.value for e in OperationType],  # 所有操作类型
                    'errorlevel': [e.value for e in ErrorLevel],  # ['low', 'medium', 'high', 'critical']
                    'backuptasktype': [e.value for e in BackupTaskType],  # ['full', 'incremental', 'differential', 'monthly_full']
                    'backuptaskstatus': [e.value for e in BackupTaskStatus],  # ['pending', 'running', 'completed', 'failed', 'cancelled', 'paused']
                    'backupfiletype': [e.value for e in BackupFileType],  # ['file', 'directory', 'symlink']
                }
                
                # 初始化统计列表
                created_enums = []
                existing_enums = []
                
                for enum_name, enum_values in enum_definitions.items():
                    # 检查枚举类型是否已存在
                    cur.execute("""
                        SELECT 1 FROM pg_type WHERE typname = %s
                    """, (enum_name,))
                    exists = cur.fetchone()
                    
                    if not exists:
                        # 创建枚举类型
                        quoted_values = ', '.join([f"'{v}'" for v in enum_values])
                        enum_sql = f'CREATE TYPE {enum_name} AS ENUM ({quoted_values})'
                        cur.execute(enum_sql)
                        created_enums.append(enum_name)
                    else:
                        existing_enums.append(enum_name)
                
                # 汇总输出，减少日志刷屏
                if created_enums:
                    logger.info(f"创建了 {len(created_enums)} 个新枚举类型")
                if existing_enums:
                    logger.debug(f"跳过 {len(existing_enums)} 个已存在的枚举类型")
                
                # 也处理其他表中的枚举类型（从SQLAlchemy元数据获取）
                for table in Base.metadata.tables.values():
                    for column in table.columns:
                        if hasattr(column.type, 'enums'):
                            enum_name = column.type.name
                            # 如果已经在上面定义过，跳过
                            if enum_name.lower() in enum_definitions:
                                continue
                            
                            # 获取枚举值
                            try:
                                enum_values = [e.value for e in column.type.enums]
                            except AttributeError:
                                enum_values = list(column.type.enums)
                            
                            # 检查枚举类型是否已存在
                            cur.execute("""
                                SELECT 1 FROM pg_type WHERE typname = %s
                            """, (enum_name,))
                            if not cur.fetchone():
                                quoted_values = ', '.join([f"'{v}'" for v in enum_values])
                                enum_sql = f"CREATE TYPE {enum_name} AS ENUM ({quoted_values})"
                                cur.execute(enum_sql)
                                created_enums.append(enum_name)
                            else:
                                existing_enums.append(enum_name)
                
                # 创建表
                created_tables = []
                existing_tables = []
                
                for table in Base.metadata.sorted_tables:
                    # 检查表是否已存在
                    cur.execute("""
                        SELECT 1 FROM information_schema.tables WHERE table_name = %s
                    """, (table.name,))
                    if not cur.fetchone():
                        create_sql = str(CreateTable(table).compile(compile_kwargs={"literal_binds": True}, dialect=temp_engine.dialect))
                        cur.execute(create_sql)
                        created_tables.append(table.name)
                    else:
                        existing_tables.append(table.name)
                
                # 汇总输出，减少日志刷屏
                if created_tables:
                    logger.info(f"创建了 {len(created_tables)} 个新表: {', '.join(created_tables[:5])}{'...' if len(created_tables) > 5 else ''}")
                if existing_tables:
                    logger.debug(f"跳过 {len(existing_tables)} 个已存在的表")
                
                # 检查并添加缺失的字段（字段迁移）
                self._migrate_missing_columns(cur)
            
            conn.commit()
            logger.info("使用psycopg2成功创建数据库表")
        except Exception as e:
            conn.rollback()
            raise
        finally:
            conn.close()
    
    def _migrate_missing_columns(self, cur):
        """检查并添加缺失的字段（字段迁移）
        
        Args:
            cur: psycopg2 cursor对象
        """
        try:
            from models.backup import BackupTask, BackupSet
            from sqlalchemy.sql import sqltypes
            
            # 定义需要迁移的字段（表名 -> [(字段名, SQL类型, 默认值), ...]）
            migrations = {
                'backup_tasks': [
                    ('compressed_bytes', 'BIGINT', '0', '压缩后字节数'),
                    ('scan_status', 'VARCHAR(50)', "'pending'", '扫描状态'),
                    ('scan_completed_at', 'TIMESTAMPTZ', 'NULL', '扫描完成时间'),
                    ('operation_stage', 'VARCHAR(50)', 'NULL', '操作阶段（scan/compress/copy/finalize）'),
                ],
                'backup_sets': [
                    ('compressed_bytes', 'BIGINT', '0', '压缩后字节数'),
                    ('compression_ratio', 'REAL', 'NULL', '压缩比'),
                ],
                'backup_files': [
                    ('directory_path', 'VARCHAR(1000)', 'NULL', '目录路径'),
                    ('display_name', 'VARCHAR(255)', 'NULL', '展示名称'),
                    ('is_copy_success', 'BOOLEAN', 'FALSE', '是否复制成功'),
                    ('copy_status_at', 'TIMESTAMPTZ', 'NULL', '复制状态更新时间'),
                ],
            }
            
            added_columns = []
            existing_columns = []
            
            for table_name, columns in migrations.items():
                # 检查表是否存在
                cur.execute("""
                    SELECT 1 FROM information_schema.tables WHERE table_name = %s
                """, (table_name,))
                if not cur.fetchone():
                    # 表不存在，跳过
                    continue
                
                # 获取表中现有的所有列名
                cur.execute("""
                    SELECT column_name FROM information_schema.columns 
                    WHERE table_name = %s
                """, (table_name,))
                existing_cols = {row[0] for row in cur.fetchall()}
                
                # 检查每个需要迁移的字段
                for col_name, col_type, default_value, comment in columns:
                    if col_name not in existing_cols:
                        # 字段不存在，需要添加
                        default_clause = f"DEFAULT {default_value}" if default_value != 'NULL' else ""
                        comment_clause = f"COMMENT ON COLUMN {table_name}.{col_name} IS '{comment}'" if comment else ""
                        
                        alter_sql = f"ALTER TABLE {table_name} ADD COLUMN {col_name} {col_type} {default_clause}"
                        cur.execute(alter_sql)
                        added_columns.append(f"{table_name}.{col_name}")
                        
                        # 添加注释（如果提供）
                        if comment_clause:
                            try:
                                cur.execute(comment_clause)
                            except Exception as comment_err:
                                # 注释添加失败不影响主流程
                                logger.debug(f"添加字段注释失败 {table_name}.{col_name}: {str(comment_err)}")
                    else:
                        existing_columns.append(f"{table_name}.{col_name}")
            
            # 汇总输出
            if added_columns:
                logger.info(f"添加了 {len(added_columns)} 个缺失字段: {', '.join(added_columns)}")
            if existing_columns:
                logger.debug(f"跳过 {len(existing_columns)} 个已存在的字段")
                
        except Exception as e:
            logger.warning(f"字段迁移检查失败: {str(e)}，但不影响表创建流程")
            # 不抛出异常，避免影响主流程
    
    async def _migrate_missing_columns_postgresql(self):
        """检查并添加缺失的字段（字段迁移）- PostgreSQL/非openGauss数据库
        
        使用SQLAlchemy引擎执行迁移
        """
        try:
            from sqlalchemy import text, inspect
            from models.backup import BackupTask, BackupSet
            
            # 定义需要迁移的字段（表名 -> [(字段名, SQL类型, 默认值, 注释), ...]）
            migrations = {
                'backup_tasks': [
                    ('compressed_bytes', 'BIGINT', '0', '压缩后字节数'),
                    ('scan_status', 'VARCHAR(50)', "'pending'", '扫描状态'),
                    ('scan_completed_at', 'TIMESTAMPTZ', None, '扫描完成时间'),
                    ('operation_stage', 'VARCHAR(50)', None, '操作阶段（scan/compress/copy/finalize）'),
                ],
                'backup_sets': [
                    ('compressed_bytes', 'BIGINT', '0', '压缩后字节数'),
                    ('compression_ratio', 'REAL', None, '压缩比'),
                ],
                'backup_files': [
                    ('directory_path', 'VARCHAR(1000)', None, '目录路径'),
                    ('display_name', 'VARCHAR(255)', None, '展示名称'),
                    ('is_copy_success', 'BOOLEAN', 'FALSE', '是否复制成功'),
                    ('copy_status_at', 'TIMESTAMPTZ', None, '复制状态更新时间'),
                ],
            }
            
            if self.engine is None:
                return
            
            inspector = inspect(self.engine)
            added_columns = []
            existing_columns = []
            
            async with self.async_engine.begin() as conn:
                for table_name, columns in migrations.items():
                    # 检查表是否存在
                    if not inspector.has_table(table_name):
                        continue
                    
                    # 获取表中现有的所有列名
                    existing_cols = {col['name'] for col in inspector.get_columns(table_name)}
                    
                    # 检查每个需要迁移的字段
                    for col_name, col_type, default_value, comment in columns:
                        if col_name not in existing_cols:
                            # 字段不存在，需要添加
                            default_clause = f"DEFAULT {default_value}" if default_value is not None else ""
                            alter_sql = f"ALTER TABLE {table_name} ADD COLUMN {col_name} {col_type} {default_clause}"
                            await conn.execute(text(alter_sql))
                            added_columns.append(f"{table_name}.{col_name}")
                            
                            # 添加注释（如果提供且数据库支持）
                            if comment and not self.settings.DATABASE_URL.startswith("sqlite"):
                                try:
                                    comment_sql = text(f"COMMENT ON COLUMN {table_name}.{col_name} IS '{comment}'")
                                    await conn.execute(comment_sql)
                                except Exception as comment_err:
                                    logger.debug(f"添加字段注释失败 {table_name}.{col_name}: {str(comment_err)}")
                        else:
                            existing_columns.append(f"{table_name}.{col_name}")
                
                # 汇总输出
                if added_columns:
                    logger.info(f"添加了 {len(added_columns)} 个缺失字段: {', '.join(added_columns)}")
                if existing_columns:
                    logger.debug(f"跳过 {len(existing_columns)} 个已存在的字段")
                    
        except Exception as e:
            logger.warning(f"字段迁移检查失败: {str(e)}，但不影响表创建流程")
            # 不抛出异常，避免影响主流程

    def get_sync_session(self) -> Session:
        """获取同步数据库会话"""
        if not self._initialized:
            raise RuntimeError("数据库未初始化")
        if self.SessionLocal is None:
            raise RuntimeError("openGauss数据库不支持SQLAlchemy同步会话，请使用原生SQL查询")
        return self.SessionLocal()

    async def get_async_session(self) -> AsyncGenerator[AsyncSession, None]:
        """获取异步数据库会话"""
        if not self._initialized:
            raise RuntimeError("数据库未初始化")
        if self.AsyncSessionLocal is None:
            raise RuntimeError("openGauss数据库不支持SQLAlchemy异步会话，请使用原生SQL查询（asyncpg）")

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
        if self.async_engine is None:
            raise RuntimeError("openGauss数据库不支持SQLAlchemy，请使用asyncpg直接执行SQL查询")
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
            if self.is_opengauss_database():
                import asyncpg
                import re

                database_url = self.settings.DATABASE_URL
                if database_url.startswith("opengauss://"):
                    database_url = database_url.replace("opengauss://", "postgresql://", 1)
                pattern = r'postgresql://([^:]+):([^@]+)@([^:]+):(\d+)/(.+)'
                match = re.match(pattern, database_url)
                if not match:
                    return False
                username, password, host, port, database = match.groups()
                conn = await asyncpg.connect(
                    host=host,
                    port=int(port),
                    user=username,
                    password=password,
                    database=database,
                    timeout=5
                )
                try:
                    await conn.execute("SELECT 1")
                finally:
                    await conn.close()
            else:
                from sqlalchemy import text
                if not self.async_engine:
                    raise RuntimeError("异步引擎未初始化")
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