#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
SQLite 数据库初始化模块
SQLite Database Initialization Module
"""

import logging
import aiosqlite
from pathlib import Path
from typing import Optional
from config.database import db_manager
from models.base import Base
from sqlalchemy import create_engine, text

logger = logging.getLogger(__name__)


class SQLiteInitializer:
    """SQLite 数据库初始化器"""
    
    def __init__(self):
        self.settings = db_manager.settings
    
    def _get_sqlite_path(self) -> str:
        """从 DATABASE_URL 获取 SQLite 数据库文件路径"""
        database_url = self.settings.DATABASE_URL
        # 移除 sqlite:/// 或 sqlite+aiosqlite:/// 前缀
        path = database_url.replace("sqlite+aiosqlite:///", "").replace("sqlite:///", "")
        return path
    
    async def ensure_database_exists(self) -> bool:
        """确保 SQLite 数据库文件存在，如果不存在则创建，如果损坏则修复"""
        try:
            db_path = self._get_sqlite_path()
            db_file = Path(db_path)
            
            # 确保目录存在
            db_file.parent.mkdir(parents=True, exist_ok=True)
            
            # 如果文件不存在，创建一个空文件
            if not db_file.exists():
                db_file.touch()
                logger.info(f"SQLite 数据库文件已创建: {db_path}")
            else:
                logger.debug(f"SQLite 数据库文件已存在: {db_path}")
                # 验证数据库文件是否有效
                if not await self._verify_database_file(db_path):
                    logger.warning(f"SQLite 数据库文件可能已损坏: {db_path}，尝试修复...")
                    if await self._repair_database_file(db_path):
                        logger.info(f"SQLite 数据库文件修复成功: {db_path}")
                    else:
                        logger.error(f"SQLite 数据库文件修复失败: {db_path}，将重新创建")
                        # 备份损坏的文件
                        backup_path = f"{db_path}.corrupted"
                        if db_file.exists():
                            import shutil
                            shutil.move(str(db_file), backup_path)
                            logger.info(f"已备份损坏的数据库文件到: {backup_path}")
                        # 创建新的数据库文件
                        db_file.touch()
                        logger.info(f"已重新创建 SQLite 数据库文件: {db_path}")
            
            return True
        except Exception as e:
            logger.error(f"创建 SQLite 数据库文件失败: {str(e)}")
            return False
    
    async def _verify_database_file(self, db_path: str) -> bool:
        """验证 SQLite 数据库文件是否有效"""
        try:
            import sqlite3
            # 尝试打开数据库文件
            conn = sqlite3.connect(db_path, timeout=5.0)
            try:
                # 尝试执行一个简单的查询
                cursor = conn.cursor()
                cursor.execute("SELECT name FROM sqlite_master WHERE type='table' LIMIT 1")
                cursor.fetchone()
                conn.close()
                return True
            except sqlite3.DatabaseError:
                conn.close()
                return False
        except Exception as e:
            logger.debug(f"验证数据库文件时出错: {str(e)}")
            return False
    
    async def _repair_database_file(self, db_path: str) -> bool:
        """尝试修复 SQLite 数据库文件"""
        try:
            import sqlite3
            import shutil
            from datetime import datetime
            
            # 创建备份
            backup_path = f"{db_path}.backup_{datetime.now().strftime('%Y%m%d_%H%M%S')}"
            shutil.copy2(db_path, backup_path)
            logger.info(f"已创建数据库备份: {backup_path}")
            
            # 尝试使用 SQLite 的修复功能
            # SQLite 没有内置的修复功能，但我们可以尝试重新创建数据库
            # 首先尝试从损坏的数据库中恢复数据
            try:
                # 尝试打开损坏的数据库
                corrupted_conn = sqlite3.connect(db_path)
                corrupted_conn.execute("PRAGMA integrity_check")
                corrupted_conn.close()
                # 如果能够执行完整性检查，说明数据库可能没问题
                return True
            except sqlite3.DatabaseError:
                # 数据库确实损坏，无法修复
                logger.warning("数据库文件已损坏，无法自动修复")
                return False
        except Exception as e:
            logger.error(f"修复数据库文件时出错: {str(e)}")
            return False
    
    async def create_tables(self):
        """使用 SQLAlchemy 创建 SQLite 数据库表"""
        try:
            # 导入所有模型以确保它们被注册
            from models import backup, tape, user, system_log, system_config, scheduled_task
            
            database_url = self.settings.DATABASE_URL
            # 确保使用同步 URL（SQLAlchemy 需要）
            if database_url.startswith("sqlite+aiosqlite:///"):
                database_url = database_url.replace("sqlite+aiosqlite:///", "sqlite:///")
            
            # 创建同步引擎用于创建表
            engine = create_engine(
                database_url,
                echo=self.settings.DEBUG,
                connect_args={"check_same_thread": False}  # SQLite 允许多线程
            )
            
            # 创建所有表
            with engine.begin() as conn:
                Base.metadata.create_all(conn)
            
            logger.info("SQLite 数据库表创建完成")
            
            # 检查并添加缺失的字段（字段迁移）
            await self._migrate_missing_columns()
            
            # 设置 SQLite 优化参数
            await self._configure_sqlite_settings()
            
        except Exception as e:
            logger.error(f"创建 SQLite 数据库表失败: {str(e)}")
            raise
    
    async def _configure_sqlite_settings(self):
        """配置 SQLite 性能优化参数"""
        try:
            from utils.scheduler.sqlite_utils import get_sqlite_connection
            
            async with get_sqlite_connection() as conn:
                # 设置缓存大小（可根据配置调整）
                cache_size = getattr(self.settings, 'SQLITE_CACHE_SIZE', 10000)
                await conn.execute(f"PRAGMA cache_size=-{cache_size}")  # 负数表示 KB
                
                # 使用内存存储临时表
                await conn.execute("PRAGMA temp_store=memory")
                
                # 设置页面大小（如果数据库是新创建的）
                page_size = getattr(self.settings, 'SQLITE_PAGE_SIZE', 4096)
                await conn.execute(f"PRAGMA page_size={page_size}")
                
                # 设置日志模式（可根据配置调整）
                journal_mode = getattr(self.settings, 'SQLITE_JOURNAL_MODE', 'WAL')
                await conn.execute(f"PRAGMA journal_mode={journal_mode}")
                
                # 设置同步模式（可根据配置调整）
                synchronous = getattr(self.settings, 'SQLITE_SYNCHRONOUS', 'NORMAL')
                await conn.execute(f"PRAGMA synchronous={synchronous}")
                
                # 设置外键约束
                await conn.execute("PRAGMA foreign_keys=ON")
                
                await conn.commit()
                
                logger.info(f"SQLite 性能参数已配置: cache_size={cache_size}KB, page_size={page_size}, journal_mode={journal_mode}, synchronous={synchronous}")
        except Exception as e:
            logger.warning(f"配置 SQLite 性能参数失败: {str(e)}")
    
    async def _migrate_missing_columns(self):
        """检查并添加缺失的字段（字段迁移）"""
        try:
            from utils.scheduler.sqlite_utils import get_sqlite_connection
            
            async with get_sqlite_connection() as conn:
                total_added = 0
                
                # 迁移 backup_tasks 表
                total_added += await self._migrate_table_columns(conn, 'backup_tasks', [
                    ('is_template', 'INTEGER DEFAULT 0'),
                    ('template_id', 'INTEGER'),
                    ('tape_device', 'VARCHAR(200)'),
                    ('tape_id', 'VARCHAR(50)'),
                    ('backup_set_id', 'VARCHAR(50)'),
                    ('operation_stage', 'VARCHAR(50)'),
                    ('scan_status', 'VARCHAR(50)'),
                    ('scan_completed_at', 'DATETIME'),
                ])
                
                # 迁移 backup_files 表
                total_added += await self._migrate_table_columns(conn, 'backup_files', [
                    ('directory_path', 'TEXT'),
                    ('display_name', 'TEXT'),
                    ('is_copy_success', 'INTEGER DEFAULT 0'),  # SQLite uses INTEGER for BOOLEAN
                    ('copy_status_at', 'DATETIME'),
                ])
                
                # 迁移 operation_logs 表
                total_added += await self._migrate_table_columns(conn, 'operation_logs', [
                    ('username', 'VARCHAR(100)'),
                    ('resource_type', 'VARCHAR(100)'),
                    ('resource_id', 'VARCHAR(100)'),
                    ('resource_name', 'VARCHAR(200)'),
                    ('operation_name', 'VARCHAR(200)'),
                    ('operation_description', 'TEXT'),
                    ('category', 'VARCHAR(50)'),
                    ('duration_ms', 'INTEGER'),
                    ('request_method', 'VARCHAR(10)'),
                    ('request_url', 'VARCHAR(1000)'),
                    ('request_params', 'TEXT'),  # JSON stored as TEXT in SQLite
                    ('request_body', 'TEXT'),  # JSON stored as TEXT in SQLite
                    ('response_status', 'INTEGER'),
                    ('response_body', 'TEXT'),  # JSON stored as TEXT in SQLite
                    ('response_size', 'INTEGER'),
                    ('success', 'INTEGER DEFAULT 1'),  # SQLite uses INTEGER for BOOLEAN
                    ('result_message', 'TEXT'),
                    ('error_code', 'VARCHAR(50)'),
                    ('error_message', 'TEXT'),
                    ('ip_address', 'VARCHAR(45)'),
                    ('user_agent', 'TEXT'),
                    ('referer', 'VARCHAR(1000)'),
                    ('old_values', 'TEXT'),  # JSON stored as TEXT in SQLite
                    ('new_values', 'TEXT'),  # JSON stored as TEXT in SQLite
                    ('changed_fields', 'TEXT'),  # JSON stored as TEXT in SQLite
                ])
                
                if total_added > 0:
                    await conn.commit()
                    logger.info(f"SQLite 字段迁移完成: 总共添加了 {total_added} 个字段")
        except Exception as e:
            logger.warning(f"SQLite 字段迁移失败: {str(e)}")
    
    async def _migrate_table_columns(self, conn, table_name: str, columns_to_check: list) -> int:
        """迁移单个表的缺失字段"""
        try:
            # 检查表是否存在
            cursor = await conn.execute("""
                SELECT name FROM sqlite_master 
                WHERE type='table' AND name=?
            """, (table_name,))
            table_exists = await cursor.fetchone()
            
            if not table_exists:
                return 0
            
            # 获取现有列
            cursor = await conn.execute(f"PRAGMA table_info({table_name})")
            columns = await cursor.fetchall()
            existing_columns = {col[1] for col in columns}  # col[1] 是列名
            
            # 添加缺失的字段
            added_count = 0
            for col_name, col_def in columns_to_check:
                if col_name not in existing_columns:
                    try:
                        await conn.execute(f"ALTER TABLE {table_name} ADD COLUMN {col_name} {col_def}")
                        logger.info(f"已添加字段 {table_name}.{col_name}")
                        added_count += 1
                    except Exception as e:
                        logger.warning(f"添加字段 {table_name}.{col_name} 失败: {str(e)}")
            
            return added_count
        except Exception as e:
            logger.warning(f"迁移表 {table_name} 字段失败: {str(e)}")
            return 0

