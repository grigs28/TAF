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
from sqlalchemy.pool import StaticPool, NullPool

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
            # 对于openGauss，需要特殊处理以避免版本解析错误
            is_opengauss = self.is_opengauss_database()
            is_sqlite = raw_database_url.startswith("sqlite:///") or raw_database_url.startswith("sqlite+aiosqlite:///")
            is_redis = raw_database_url.startswith("redis://") or raw_database_url.startswith("rediss://")
            
            # 对于Redis，不创建SQLAlchemy引擎，使用原生Redis客户端
            if is_redis:
                logger.info("检测到Redis数据库，跳过SQLAlchemy引擎创建，将使用原生Redis客户端")
                self.engine = None
                self.async_engine = None
                self.AsyncSessionLocal = None
                self.SessionLocal = None
                
                # 初始化Redis管理器
                from config.redis_db import get_redis_manager
                redis_manager = get_redis_manager()
                if redis_manager:
                    await redis_manager.initialize()
                    logger.info("Redis连接初始化成功")
                else:
                    logger.warning("Redis管理器创建失败，请检查DATABASE_URL配置")
                
                # Redis不需要创建表
                self._initialized = True
                logger.info("Redis数据库初始化完成（跳过表创建）")
                return
            # 对于openGauss，完全不创建SQLAlchemy引擎，避免版本解析错误
            elif is_opengauss:
                logger.warning("检测到openGauss数据库，跳过SQLAlchemy引擎创建，将使用原生SQL查询")
                self.engine = None
                self.async_engine = None
                self.AsyncSessionLocal = None
                self.SessionLocal = None
            elif is_sqlite:
                # SQLite 需要特殊处理
                logger.info("检测到SQLite数据库，创建SQLAlchemy异步引擎")
                # 为避免单连接被多个协程共享导致游标重置，SQLite 使用 NullPool（每次请求新连接）
                connect_args = {"check_same_thread": False}
                pool_class = NullPool
                # 同步引擎（用于创建表等操作）
                self.engine = create_engine(
                    database_url,
                    echo=self.settings.DEBUG,
                    poolclass=pool_class,
                    connect_args=connect_args
                )
                # 异步引擎（使用 aiosqlite）
                self.async_engine = create_async_engine(
                    async_database_url,
                    echo=self.settings.DEBUG,
                    poolclass=pool_class,
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
            else:
                # PostgreSQL支持连接池
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
        # 将 SQLite 异步 URL 转换为同步 URL（用于同步引擎）
        if url.startswith("sqlite+aiosqlite:///"):
            return url.replace("sqlite+aiosqlite:///", "sqlite:///")
        return url

    def _build_async_database_url(self) -> str:
        """构建异步数据库URL"""
        # 将同步URL转换为异步URL
        url = self.settings.DATABASE_URL
        if url.startswith("postgresql://"):
            return url.replace("postgresql://", "postgresql+asyncpg://")
        elif url.startswith("opengauss://"):
            return url.replace("opengauss://", "postgresql+asyncpg://")
        elif url.startswith("sqlite:///"):
            # SQLite 需要使用 aiosqlite 作为异步驱动
            return url.replace("sqlite:///", "sqlite+aiosqlite:///")
        elif url.startswith("sqlite+aiosqlite:///"):
            # 已经是异步 URL，直接返回
            return url
        else:
            return url

    async def create_tables(self):
        """创建数据库表"""
        try:
            # 导入所有模型以确保它们被注册
            from models import backup, tape, user, system_log, system_config, scheduled_task
            
            database_url = self.settings.DATABASE_URL
            
            # Redis不需要创建表，它是键值存储
            is_redis = database_url.startswith("redis://") or database_url.startswith("rediss://")
            if is_redis:
                logger.info("检测到Redis数据库，跳过表创建（Redis是键值存储）")
                return
            
            # 检查是否为 SQLite
            if database_url.startswith("sqlite:///") or database_url.startswith("sqlite+aiosqlite:///"):
                # SQLite 使用专门的初始化器
                from config.sqlite_init import SQLiteInitializer
                sqlite_init = SQLiteInitializer()
                logger.info("检测到 SQLite 数据库，使用 SQLite 初始化器创建表...")
                await sqlite_init.create_tables()
            elif self.is_opengauss_database():
                # 对于openGauss，使用psycopg2直接创建表，避免版本检查问题
                logger.info("检测到openGauss数据库，使用psycopg2创建表...")
                await self._create_tables_with_psycopg2()
            else:
                # PostgreSQL使用SQLAlchemy引擎来创建表
                with self.engine.begin() as conn:
                    Base.metadata.create_all(conn)
                logger.info("数据库表创建完成")
                
                # 检查并添加缺失的字段（字段迁移）
                await self._migrate_missing_columns_postgresql()
                # 关键修复：确保路径、文件名字段是 TEXT 类型（对于已存在的表）
                await self._ensure_text_fields_postgresql()

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
                        
                        # 关键修复：将路径、文件名相关字段从 VARCHAR 改为 TEXT
                        # 确保在创建表时就使用 TEXT 类型，而不是 VARCHAR
                        if table.name == 'backup_files':
                            # 替换 file_name, file_path, directory_path, display_name 的 VARCHAR 为 TEXT
                            create_sql = create_sql.replace('file_name VARCHAR', 'file_name TEXT')
                            create_sql = create_sql.replace('file_name CHARACTER VARYING', 'file_name TEXT')
                            create_sql = create_sql.replace('file_path VARCHAR', 'file_path TEXT')
                            create_sql = create_sql.replace('file_path CHARACTER VARYING', 'file_path TEXT')
                            create_sql = create_sql.replace('directory_path VARCHAR', 'directory_path TEXT')
                            create_sql = create_sql.replace('directory_path CHARACTER VARYING', 'directory_path TEXT')
                            create_sql = create_sql.replace('display_name VARCHAR', 'display_name TEXT')
                            create_sql = create_sql.replace('display_name CHARACTER VARYING', 'display_name TEXT')
                            # 处理可能的长度限制（如 VARCHAR(4096) 等）
                            import re
                            create_sql = re.sub(r'file_name VARCHAR\([^)]+\)', 'file_name TEXT', create_sql, flags=re.IGNORECASE)
                            create_sql = re.sub(r'file_name CHARACTER VARYING\([^)]+\)', 'file_name TEXT', create_sql, flags=re.IGNORECASE)
                            create_sql = re.sub(r'file_path VARCHAR\([^)]+\)', 'file_path TEXT', create_sql, flags=re.IGNORECASE)
                            create_sql = re.sub(r'file_path CHARACTER VARYING\([^)]+\)', 'file_path TEXT', create_sql, flags=re.IGNORECASE)
                            create_sql = re.sub(r'directory_path VARCHAR\([^)]+\)', 'directory_path TEXT', create_sql, flags=re.IGNORECASE)
                            create_sql = re.sub(r'directory_path CHARACTER VARYING\([^)]+\)', 'directory_path TEXT', create_sql, flags=re.IGNORECASE)
                            create_sql = re.sub(r'display_name VARCHAR\([^)]+\)', 'display_name TEXT', create_sql, flags=re.IGNORECASE)
                            create_sql = re.sub(r'display_name CHARACTER VARYING\([^)]+\)', 'display_name TEXT', create_sql, flags=re.IGNORECASE)
                            logger.info(f"已将 backup_files 表的路径、文件名、展示名称相关字段设置为 TEXT 类型")
                        
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
                
                # 关键修复：无论表是新创建还是已存在，都强制检查并修改字段类型为 TEXT
                # 在提交前执行，确保在同一事务中完成
                logger.info("========== 开始检查 backup_files 表字段类型 ==========")
                try:
                    cur.execute("""
                        SELECT 1 FROM information_schema.tables WHERE table_name = 'backup_files'
                    """)
                    if cur.fetchone():
                        # 表已存在，强制检查并修改字段类型为 TEXT
                        logger.info("backup_files 表已存在，强制检查并修改字段类型为 TEXT...")
                        logger.info("确保路径、文件名、展示名称字段为 TEXT 类型（无长度限制）...")
                        text_fields = ['file_name', 'file_path', 'directory_path', 'display_name']
                        modified_fields = []
                        skipped_fields = []
                        error_fields = []
                        
                        for field_name in text_fields:
                            try:
                                # 检查字段当前类型
                                cur.execute("""
                                    SELECT data_type, character_maximum_length
                                    FROM information_schema.columns 
                                    WHERE table_name = 'backup_files' AND column_name = %s
                                """, (field_name,))
                                result = cur.fetchone()
                                if not result:
                                    logger.warning(f"字段 backup_files.{field_name} 不存在，跳过")
                                    continue
                                
                                current_type, max_length = result
                                if current_type == 'character varying':
                                    # 字段是 VARCHAR，强制改为 TEXT
                                    logger.info(f"将 backup_files.{field_name} 从 VARCHAR({max_length}) 改为 TEXT...")
                                    try:
                                        cur.execute(f"ALTER TABLE backup_files ALTER COLUMN {field_name} TYPE TEXT USING {field_name}::TEXT")
                                        # 再次验证修改是否成功
                                        cur.execute("""
                                            SELECT data_type FROM information_schema.columns 
                                            WHERE table_name = 'backup_files' AND column_name = %s
                                        """, (field_name,))
                                        verify_result = cur.fetchone()
                                        if verify_result and verify_result[0] == 'text':
                                            modified_fields.append(field_name)
                                            logger.info(f"✅ 成功将 backup_files.{field_name} 改为 TEXT 类型（已验证）")
                                        else:
                                            error_fields.append(f"{field_name} (修改后验证失败)")
                                            logger.error(f"❌ backup_files.{field_name} 修改后验证失败，当前类型: {verify_result[0] if verify_result else 'unknown'}")
                                    except Exception as alter_err:
                                        error_fields.append(f"{field_name} ({str(alter_err)})")
                                        logger.error(f"❌ 修改 backup_files.{field_name} 失败: {str(alter_err)}", exc_info=True)
                                elif current_type == 'text':
                                    skipped_fields.append(field_name)
                                    logger.debug(f"backup_files.{field_name} 已是 TEXT 类型，无需修改")
                                else:
                                    logger.warning(f"backup_files.{field_name} 类型为 {current_type}，不是 VARCHAR 或 TEXT")
                            except Exception as check_err:
                                error_fields.append(f"{field_name} (检查失败: {str(check_err)})")
                                logger.error(f"❌ 检查 backup_files.{field_name} 失败: {str(check_err)}", exc_info=True)
                    
                        # 汇总输出
                        if modified_fields:
                            logger.info(f"========== ✅ 成功修改了 {len(modified_fields)} 个字段为 TEXT: {', '.join(modified_fields)} ==========")
                        if skipped_fields:
                            logger.info(f"跳过 {len(skipped_fields)} 个已是 TEXT 类型的字段: {', '.join(skipped_fields)}")
                        if error_fields:
                            logger.error(f"========== ❌ {len(error_fields)} 个字段修改失败: ==========")
                            for err_field in error_fields:
                                logger.error(f"   - {err_field}")
                            logger.error(f"这会导致长文件名/路径无法同步到数据库！")
                        else:
                            logger.info("========== 字段类型检查完成，所有字段都是 TEXT 类型 ==========")
                    else:
                        logger.info("backup_files 表不存在，将在创建时自动设置为 TEXT 类型")
                except Exception as text_check_err:
                    logger.error(f"========== 字段类型检查和修改过程发生异常 ==========")
                    logger.error(f"错误信息: {str(text_check_err)}", exc_info=True)
                    logger.error(f"这可能导致长文件名/路径无法同步到数据库！")
                    # 不抛出异常，避免影响表创建流程，但记录详细错误信息
                
                # 检查并修改 backup_tasks 表的 total_bytes 字段类型为 NUMERIC（避免 int64 溢出）
                logger.info("========== 开始检查 backup_tasks 表 total_bytes 字段类型 ==========")
                try:
                    cur.execute("""
                        SELECT 1 FROM information_schema.tables WHERE table_name = 'backup_tasks'
                    """)
                    if cur.fetchone():
                        # 检查 total_bytes 字段类型
                        cur.execute("""
                            SELECT data_type FROM information_schema.columns 
                            WHERE table_name = 'backup_tasks' AND column_name = 'total_bytes'
                        """)
                        result = cur.fetchone()
                        if result:
                            current_type = result[0]
                            if current_type in ('bigint', 'integer'):
                                # 字段是 BIGINT 或 INTEGER，需要改为 NUMERIC
                                logger.info(f"将 backup_tasks.total_bytes 从 {current_type.upper()} 改为 NUMERIC...")
                                try:
                                    cur.execute("""
                                        ALTER TABLE backup_tasks 
                                        ALTER COLUMN total_bytes TYPE NUMERIC USING total_bytes::NUMERIC
                                    """)
                                    # 验证修改是否成功
                                    cur.execute("""
                                        SELECT data_type FROM information_schema.columns 
                                        WHERE table_name = 'backup_tasks' AND column_name = 'total_bytes'
                                    """)
                                    verify_result = cur.fetchone()
                                    if verify_result and verify_result[0] == 'numeric':
                                        logger.info("✅ 成功将 backup_tasks.total_bytes 改为 NUMERIC 类型（已验证）")
                                    else:
                                        logger.error(f"❌ backup_tasks.total_bytes 修改后验证失败，当前类型: {verify_result[0] if verify_result else 'unknown'}")
                                except Exception as alter_err:
                                    logger.error(f"❌ 修改 backup_tasks.total_bytes 失败: {str(alter_err)}", exc_info=True)
                            elif current_type == 'numeric':
                                logger.debug("backup_tasks.total_bytes 已是 NUMERIC 类型，无需修改")
                            else:
                                logger.warning(f"backup_tasks.total_bytes 类型为 {current_type}，不是 BIGINT 或 NUMERIC")
                        else:
                            logger.warning("backup_tasks.total_bytes 字段不存在，跳过类型检查")
                    else:
                        logger.info("backup_tasks 表不存在，将在创建时自动设置 total_bytes 为 NUMERIC 类型")
                except Exception as total_bytes_check_err:
                    logger.error(f"========== total_bytes 字段类型检查过程发生异常 ==========")
                    logger.error(f"错误信息: {str(total_bytes_check_err)}", exc_info=True)
                    # 不抛出异常，避免影响表创建流程
                
                # 创建索引（优化查询性能）- 必须在 with 块内，在 cursor 关闭之前
                self._create_indexes_for_backup_files(cur)
            
            # 提交所有修改（表创建、字段迁移、字段类型修改、索引创建）
            conn.commit()
            logger.info("使用psycopg2成功创建数据库表、字段迁移、字段类型检查和索引创建完成")
            
        except Exception as e:
            conn.rollback()
            logger.error(f"数据库初始化失败: {str(e)}", exc_info=True)
            raise
        finally:
            # 关闭连接（表创建和字段类型检查已完成）
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
    
    def _create_indexes_for_backup_files(self, cur):
        """为 backup_files 表创建索引（优化查询性能）
        
        索引说明：
        1. idx_backup_files_set_path: 优化 WHERE backup_set_id = ? AND file_path = ANY(?)
           用于验证查询和 mark_files_as_copied 查询
        
        2. idx_backup_files_set_copy_status: 优化 WHERE backup_set_id = ? AND is_copy_success = FALSE
           用于 fetch_pending_files_grouped_by_size 查询
        
        3. idx_backup_files_set_copy_type_id: 复合索引，优化待压缩文件查询
           用于 fetch_pending_files_grouped_by_size 的分批查询和排序
        
        Args:
            cur: psycopg2 cursor对象
        """
        try:
            # 检查 backup_files 表是否存在
            cur.execute("""
                SELECT 1 FROM information_schema.tables WHERE table_name = 'backup_files'
            """)
            if not cur.fetchone():
                logger.debug("backup_files 表不存在，跳过索引创建")
                return
            
            # 定义需要创建的索引
            indexes = [
                {
                    'name': 'idx_backup_files_set_path',
                    'sql': """
                        CREATE INDEX IF NOT EXISTS idx_backup_files_set_path 
                        ON backup_files(backup_set_id, file_path)
                    """,
                    'description': '优化验证查询和 mark_files_as_copied 查询（backup_set_id + file_path）'
                },
                {
                    'name': 'idx_backup_files_set_copy_status',
                    'sql': """
                        CREATE INDEX IF NOT EXISTS idx_backup_files_set_copy_status 
                        ON backup_files(backup_set_id, is_copy_success)
                        WHERE is_copy_success = FALSE OR is_copy_success IS NULL
                    """,
                    'description': '部分索引：优化待压缩文件查询（只索引未压缩文件，节省空间）'
                },
                {
                    'name': 'idx_backup_files_set_copy_type_id',
                    'sql': """
                        CREATE INDEX IF NOT EXISTS idx_backup_files_set_copy_type_id 
                        ON backup_files(backup_set_id, is_copy_success, file_type, id)
                        WHERE (is_copy_success = FALSE OR is_copy_success IS NULL) AND file_type = 'file'::backupfiletype
                    """,
                    'description': '部分复合索引：优化 fetch_pending_files_grouped_by_size 的分批查询和排序'
                }
            ]
            
            created_indexes = []
            existing_indexes = []
            error_indexes = []
            
            for index_def in indexes:
                try:
                    # 检查索引是否已存在
                    cur.execute("""
                        SELECT 1 FROM pg_indexes 
                        WHERE tablename = 'backup_files' AND indexname = %s
                    """, (index_def['name'],))
                    if cur.fetchone():
                        existing_indexes.append(index_def['name'])
                        logger.debug(f"索引 {index_def['name']} 已存在，跳过")
                        continue
                    
                    # 创建索引
                    cur.execute(index_def['sql'])
                    created_indexes.append(index_def['name'])
                    logger.info(f"✅ 创建索引 {index_def['name']}: {index_def['description']}")
                    
                except Exception as index_err:
                    error_indexes.append(f"{index_def['name']} ({str(index_err)})")
                    logger.warning(f"创建索引 {index_def['name']} 失败: {str(index_err)}")
                    # 继续创建其他索引，不中断流程
            
            # 汇总输出
            if created_indexes:
                logger.info(f"========== 成功创建 {len(created_indexes)} 个索引: {', '.join(created_indexes)} ==========")
            if existing_indexes:
                logger.debug(f"跳过 {len(existing_indexes)} 个已存在的索引: {', '.join(existing_indexes)}")
            if error_indexes:
                logger.warning(f"========== {len(error_indexes)} 个索引创建失败: ==========")
                for err_index in error_indexes:
                    logger.warning(f"   - {err_index}")
                logger.warning("这可能会影响查询性能，但不会影响功能")
            else:
                logger.info("========== 索引创建完成，查询性能已优化 ==========")
                
        except Exception as e:
            logger.warning(f"索引创建过程发生异常: {str(e)}，但不影响表创建流程", exc_info=True)
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
                            
                            # 添加注释（如果提供）
                            if comment:
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
    
    def _migrate_column_lengths(self, cur):
        """修改现有字段的类型（字段类型迁移）- openGauss
        将路径、文件名相关字段从 VARCHAR 改为 TEXT 类型（无长度限制）
        
        Args:
            cur: psycopg2 cursor对象
        """
        try:
            # 定义需要修改类型的字段（表名 -> [(字段名, 注释), ...]）
            text_migrations = {
                'backup_files': [
                    ('file_name', '文件名'),
                    ('file_path', '文件路径'),
                    ('directory_path', '目录路径'),
                ],
            }
            
            modified_columns = []
            skipped_columns = []
            error_columns = []
            
            logger.info("========== 开始检查字段类型迁移（VARCHAR -> TEXT）==========")
            logger.info("注意：如果数据库字段仍然是 VARCHAR(255)，需要执行此迁移将字段改为 TEXT 类型")
            
            for table_name, columns in text_migrations.items():
                # 检查表是否存在
                cur.execute("""
                    SELECT 1 FROM information_schema.tables WHERE table_name = %s
                """, (table_name,))
                if not cur.fetchone():
                    # 表不存在，跳过
                    logger.debug(f"表 {table_name} 不存在，跳过字段类型迁移")
                    continue
                
                # 获取表中现有字段的类型信息
                cur.execute("""
                    SELECT column_name, data_type, character_maximum_length
                    FROM information_schema.columns 
                    WHERE table_name = %s AND (data_type = 'character varying' OR data_type = 'text')
                """, (table_name,))
                existing_cols = {}
                for row in cur.fetchall():
                    col_name, data_type, max_length = row
                    existing_cols[col_name] = {'type': data_type, 'length': max_length}
                
                # 检查每个需要修改类型的字段
                for col_name, comment in columns:
                    if col_name not in existing_cols:
                        # 字段不存在，跳过
                        logger.debug(f"字段 {table_name}.{col_name} 不存在，跳过")
                        continue
                    
                    current_type = existing_cols[col_name]['type']
                    current_length = existing_cols[col_name]['length']
                    
                    # 如果已经是 TEXT 类型，跳过
                    if current_type == 'text':
                        skipped_columns.append(f"{table_name}.{col_name} (已是 TEXT 类型)")
                        logger.debug(f"字段 {table_name}.{col_name} 已是 TEXT 类型，跳过")
                        continue
                    
                    # 如果是 VARCHAR 类型，改为 TEXT
                    if current_type == 'character varying':
                        try:
                            logger.info(f"正在修改字段类型: {table_name}.{col_name} (VARCHAR({current_length}) -> TEXT)")
                            # 使用 USING 子句确保数据转换成功
                            alter_sql = f"ALTER TABLE {table_name} ALTER COLUMN {col_name} TYPE TEXT USING {col_name}::TEXT"
                            cur.execute(alter_sql)
                            modified_columns.append(f"{table_name}.{col_name} (VARCHAR({current_length}) -> TEXT)")
                            logger.info(f"✅ 成功修改字段类型: {table_name}.{col_name} (VARCHAR({current_length}) -> TEXT)")
                            
                            # 更新注释（如果提供）
                            if comment:
                                try:
                                    comment_sql = f"COMMENT ON COLUMN {table_name}.{col_name} IS '{comment}'"
                                    cur.execute(comment_sql)
                                except Exception as comment_err:
                                    logger.debug(f"更新字段注释失败 {table_name}.{col_name}: {str(comment_err)}")
                        except Exception as alter_err:
                            error_msg = f"{table_name}.{col_name}: {str(alter_err)}"
                            error_columns.append(error_msg)
                            logger.error(f"❌ 修改字段类型失败: {error_msg}")
            
            # 汇总输出
            if modified_columns:
                logger.info(f"✅ 成功修改了 {len(modified_columns)} 个字段类型: {', '.join(modified_columns)}")
            if skipped_columns:
                logger.info(f"跳过 {len(skipped_columns)} 个已是 TEXT 类型的字段（无需迁移）")
            if error_columns:
                logger.error(f"❌ {len(error_columns)} 个字段类型修改失败:")
                for error_col in error_columns:
                    logger.error(f"   - {error_col}")
                logger.error(f"这些字段可能仍然是 VARCHAR(255)，长文件名/路径可能无法同步")
                
            # 如果没有任何字段被修改，且表存在，记录信息
            if not modified_columns and not skipped_columns and not error_columns:
                logger.info("未找到需要迁移的字段（表可能不存在或字段已迁移）")
                
        except Exception as e:
            logger.error(f"========== 字段类型迁移检查失败 ==========")
            logger.error(f"错误信息: {str(e)}", exc_info=True)
            logger.error(f"这可能导致长文件名/路径无法同步到数据库")
            # 不抛出异常，避免影响主流程，但记录详细错误信息
    
    async def _ensure_text_fields_postgresql(self):
        """确保路径、文件名字段是 TEXT 类型（PostgreSQL初始化时调用）"""
        try:
            from sqlalchemy import text, inspect
            
            if self.async_engine is None:
                logger.debug("异步引擎未初始化，跳过字段类型检查")
                return
            
            inspector = inspect(self.engine)
            
            # 检查 backup_files 表是否存在
            if not inspector.has_table('backup_files'):
                logger.debug("backup_files 表不存在，跳过字段类型检查")
                return
            
            logger.info("========== 强制检查 backup_files 表字段类型（PostgreSQL）==========")
            logger.info("确保路径、文件名、展示名称字段为 TEXT 类型（无长度限制）...")
            
            # 需要检查的字段
            text_fields = [
                ('file_name', '文件名'),
                ('file_path', '文件路径'),
                ('directory_path', '目录路径'),
                ('display_name', '展示名称'),
            ]
            
            modified_fields = []
            skipped_fields = []
            error_fields = []
            
            async with self.async_engine.begin() as conn:
                # 获取表中现有字段的类型信息
                existing_cols = {}
                for col in inspector.get_columns('backup_files'):
                    col_name = col['name']
                    col_type = col['type']
                    type_name = col_type.__class__.__name__
                    if type_name == 'VARCHAR':
                        existing_cols[col_name] = {'type': 'VARCHAR', 'length': col_type.length}
                    elif type_name == 'TEXT' or str(col_type) == 'TEXT':
                        existing_cols[col_name] = {'type': 'TEXT', 'length': None}
                
                # 检查每个字段
                for field_name, comment in text_fields:
                    try:
                        if field_name not in existing_cols:
                            logger.warning(f"字段 backup_files.{field_name} 不存在，跳过")
                            continue
                        
                        current_type = existing_cols[field_name]['type']
                        current_length = existing_cols[field_name]['length']
                        
                        if current_type == 'TEXT':
                            skipped_fields.append(field_name)
                            logger.debug(f"backup_files.{field_name} 已是 TEXT 类型，无需修改")
                            continue
                        
                        if current_type == 'VARCHAR':
                            logger.info(f"将 backup_files.{field_name} 从 VARCHAR({current_length}) 改为 TEXT...")
                            try:
                                alter_sql = text(f"ALTER TABLE backup_files ALTER COLUMN {field_name} TYPE TEXT USING {field_name}::TEXT")
                                await conn.execute(alter_sql)
                                modified_fields.append(field_name)
                                logger.info(f"✅ 成功将 backup_files.{field_name} 改为 TEXT 类型")
                            except Exception as alter_err:
                                error_fields.append(f"{field_name} ({str(alter_err)})")
                                logger.error(f"❌ 修改 backup_files.{field_name} 失败: {str(alter_err)}")
                    except Exception as check_err:
                        error_fields.append(f"{field_name} (检查失败: {str(check_err)})")
                        logger.error(f"❌ 检查 backup_files.{field_name} 失败: {str(check_err)}")
            
            # 汇总输出
            if modified_fields:
                logger.info(f"========== ✅ 成功修改了 {len(modified_fields)} 个字段为 TEXT: {', '.join(modified_fields)} ==========")
            if skipped_fields:
                logger.info(f"跳过 {len(skipped_fields)} 个已是 TEXT 类型的字段: {', '.join(skipped_fields)}")
            if error_fields:
                logger.error(f"========== ❌ {len(error_fields)} 个字段修改失败: ==========")
                for err_field in error_fields:
                    logger.error(f"   - {err_field}")
                logger.error(f"这会导致长文件名/路径无法同步到数据库！")
            else:
                logger.info("========== 字段类型检查完成，所有字段都是 TEXT 类型 ==========")
                
        except Exception as e:
            logger.error(f"字段类型检查失败: {str(e)}", exc_info=True)
            # 不抛出异常，避免影响应用启动
    
    async def _migrate_column_lengths_postgresql(self):
        """修改现有字段的类型（字段类型迁移）- PostgreSQL/非openGauss数据库
        将路径、文件名相关字段从 VARCHAR 改为 TEXT 类型（无长度限制）
        
        使用SQLAlchemy引擎执行迁移
        """
        try:
            from sqlalchemy import text, inspect
            
            # 定义需要修改类型的字段（表名 -> [(字段名, 注释), ...]）
            text_migrations = {
                'backup_files': [
                    ('file_name', '文件名'),
                    ('file_path', '文件路径'),
                    ('directory_path', '目录路径'),
                ],
            }
            
            if self.engine is None:
                logger.debug("引擎未初始化，跳过字段类型迁移")
                return
            
            inspector = inspect(self.engine)
            modified_columns = []
            skipped_columns = []
            error_columns = []
            
            logger.info("========== 开始检查字段类型迁移（VARCHAR -> TEXT，PostgreSQL）==========")
            logger.info("注意：如果数据库字段仍然是 VARCHAR(255)，需要执行此迁移将字段改为 TEXT 类型")
            
            async with self.async_engine.begin() as conn:
                for table_name, columns in text_migrations.items():
                    # 检查表是否存在
                    if not inspector.has_table(table_name):
                        logger.debug(f"表 {table_name} 不存在，跳过字段类型迁移")
                        continue
                    
                    # 获取表中现有字段的类型信息
                    existing_cols = {}
                    for col in inspector.get_columns(table_name):
                        col_name = col['name']
                        col_type = col['type']
                        type_name = col_type.__class__.__name__
                        if type_name == 'VARCHAR':
                            existing_cols[col_name] = {'type': 'VARCHAR', 'length': col_type.length}
                        elif type_name == 'TEXT' or str(col_type) == 'TEXT':
                            existing_cols[col_name] = {'type': 'TEXT', 'length': None}
                    
                    # 检查每个需要修改类型的字段
                    for col_name, comment in columns:
                        if col_name not in existing_cols:
                            # 字段不存在，跳过
                            logger.debug(f"字段 {table_name}.{col_name} 不存在，跳过")
                            continue
                        
                        current_type = existing_cols[col_name]['type']
                        current_length = existing_cols[col_name]['length']
                        
                        # 如果已经是 TEXT 类型，跳过
                        if current_type == 'TEXT':
                            skipped_columns.append(f"{table_name}.{col_name} (已是 TEXT 类型)")
                            logger.debug(f"字段 {table_name}.{col_name} 已是 TEXT 类型，跳过")
                            continue
                        
                        # 如果是 VARCHAR 类型，改为 TEXT
                        if current_type == 'VARCHAR':
                            try:
                                logger.info(f"正在修改字段类型: {table_name}.{col_name} (VARCHAR({current_length}) -> TEXT)")
                                # 使用 USING 子句确保数据转换成功
                                alter_sql = text(f"ALTER TABLE {table_name} ALTER COLUMN {col_name} TYPE TEXT USING {col_name}::TEXT")
                                await conn.execute(alter_sql)
                                modified_columns.append(f"{table_name}.{col_name} (VARCHAR({current_length}) -> TEXT)")
                                logger.info(f"✅ 成功修改字段类型: {table_name}.{col_name} (VARCHAR({current_length}) -> TEXT)")
                                
                                # 更新注释（如果提供）
                                if comment:
                                    try:
                                        comment_sql = text(f"COMMENT ON COLUMN {table_name}.{col_name} IS '{comment}'")
                                        await conn.execute(comment_sql)
                                    except Exception as comment_err:
                                        logger.debug(f"更新字段注释失败 {table_name}.{col_name}: {str(comment_err)}")
                            except Exception as alter_err:
                                error_msg = f"{table_name}.{col_name}: {str(alter_err)}"
                                error_columns.append(error_msg)
                                logger.error(f"❌ 修改字段类型失败: {error_msg}")
                
                # 汇总输出
                if modified_columns:
                    logger.info(f"✅ 成功修改了 {len(modified_columns)} 个字段类型: {', '.join(modified_columns)}")
                if skipped_columns:
                    logger.info(f"跳过 {len(skipped_columns)} 个已是 TEXT 类型的字段（无需迁移）")
                if error_columns:
                    logger.error(f"❌ {len(error_columns)} 个字段类型修改失败:")
                    for error_col in error_columns:
                        logger.error(f"   - {error_col}")
                    logger.error(f"这些字段可能仍然是 VARCHAR(255)，长文件名/路径可能无法同步")
                
                # 如果没有任何字段被修改，且表存在，记录信息
                if not modified_columns and not skipped_columns and not error_columns:
                    logger.info("未找到需要迁移的字段（表可能不存在或字段已迁移）")
                    
        except Exception as e:
            logger.error(f"========== 字段类型迁移检查失败（PostgreSQL）==========")
            logger.error(f"错误信息: {str(e)}", exc_info=True)
            logger.error(f"这可能导致长文件名/路径无法同步到数据库")
            # 不抛出异常，避免影响主流程，但记录详细错误信息

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
            # Redis使用专门的健康检查
            raw_database_url = self.settings.DATABASE_URL
            is_redis = raw_database_url.startswith("redis://") or raw_database_url.startswith("rediss://")
            if is_redis:
                # 使用全局Redis管理器进行健康检查，而不是创建临时实例
                from config.redis_db import get_redis_manager
                redis_manager = get_redis_manager()
                if not redis_manager:
                    logger.warning("Redis管理器未初始化，无法进行健康检查")
                    return False
                try:
                    # 如果未初始化，先初始化
                    if not redis_manager._initialized:
                        await redis_manager.initialize()
                    # 使用管理器的健康检查方法（只ping，不关闭连接）
                    return await redis_manager.health_check()
                except Exception as e:
                    logger.error(f"Redis健康检查失败: {str(e)}")
                    return False
            
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