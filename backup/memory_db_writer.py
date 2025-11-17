#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
内存数据库写入器 - 完全按照openGauss BackupFile模型重写
Memory Database Writer - Rewritten to match openGauss BackupFile model exactly
"""

import asyncio
import logging
import json
import sqlite3
import tempfile
import time
from datetime import datetime, timezone
from pathlib import Path
from typing import List, Dict, Optional, Tuple
import aiosqlite

from utils.scheduler.db_utils import get_opengauss_connection
from utils.datetime_utils import now, format_datetime

logger = logging.getLogger(__name__)


class MemoryDBWriter:
    """内存数据库写入器 - 与openGauss BackupFile模型完全一致"""

    def __init__(self, backup_set_db_id: int,
                 sync_batch_size: int = 5000,           # 同步批次大小
                 sync_interval: int = 30,                # 同步间隔(秒)
                 max_memory_files: int = 100000,         # 内存中最大文件数
                 checkpoint_interval: int = 300,         # 检查点间隔(秒)
                 checkpoint_retention_hours: int = 24):  # 检查点保留时间(小时)

        self.backup_set_db_id = backup_set_db_id
        self.sync_batch_size = sync_batch_size
        self.sync_interval = sync_interval
        self.max_memory_files = max_memory_files
        self.checkpoint_interval = checkpoint_interval
        self.checkpoint_retention_hours = checkpoint_retention_hours

        # 内存数据库
        self.memory_db = None
        self.db_connection = None

        # 同步相关
        self._is_syncing = False
        self._sync_task = None
        self._checkpoint_task = None
        self._last_sync_time = 0
        self._last_checkpoint_time = 0
        self._last_trigger_time = 0  # 防止频繁触发同步
        self._last_file_added_time = time.time()  # 记录最后添加文件的时间
        self._checkpoint_files = []  # 记录创建的检查点文件列表

        # 统计信息
        self._stats = {
            'total_files': 0,
            'synced_files': 0,
            'sync_batches': 0,
            'total_time': 0,
            'sync_time': 0,
            'memory_usage': 0
        }

    async def initialize(self):
        """初始化内存数据库和同步任务"""
        await self._setup_memory_database()
        await self._start_sync_tasks()
        logger.info(f"内存数据库写入器已初始化 (backup_set_id={self.backup_set_db_id})")

    async def _setup_memory_database(self):
        """设置内存数据库 - 完全按照openGauss BackupFile模型"""
        # 创建内存数据库连接
        self.db_connection = await aiosqlite.connect(":memory:")
        self.memory_db = self.db_connection

        # 创建表结构 - 与openGauss BackupFile模型完全一致
        await self._create_tables()

        # 启用WAL模式提升性能
        await self.memory_db.execute("PRAGMA journal_mode=WAL")
        await self.memory_db.execute("PRAGMA synchronous=NORMAL")
        await self.memory_db.execute("PRAGMA cache_size=10000")
        await self.memory_db.execute("PRAGMA temp_store=memory")

    async def _create_tables(self):
        """创建内存表结构 - 与openGauss BackupFile模型字段完全一致"""
        # 文件表 - 与models.backup.BackupFile完全一致的字段顺序和类型
        await self.memory_db.execute("""
            CREATE TABLE IF NOT EXISTS backup_files (
                id INTEGER PRIMARY KEY AUTOINCREMENT,
                backup_set_id INTEGER NOT NULL,
                file_path TEXT NOT NULL,
                file_name TEXT NOT NULL,
                directory_path TEXT,
                display_name TEXT,
                file_type TEXT NOT NULL DEFAULT 'file',
                file_size BIGINT NOT NULL,
                compressed_size BIGINT,
                file_permissions TEXT,
                file_owner TEXT,
                file_group TEXT,
                created_time TIMESTAMP,
                modified_time TIMESTAMP,
                accessed_time TIMESTAMP,
                tape_block_start BIGINT,
                tape_block_count INTEGER,
                compressed BOOLEAN DEFAULT FALSE,
                encrypted BOOLEAN DEFAULT FALSE,
                checksum TEXT,
                is_copy_success BOOLEAN DEFAULT FALSE,
                copy_status_at TIMESTAMP,
                backup_time TIMESTAMP NOT NULL,
                chunk_number INTEGER,
                version INTEGER DEFAULT 1,
                file_metadata TEXT,
                tags TEXT,
                synced_to_opengauss BOOLEAN DEFAULT FALSE,
                sync_error TEXT,
                created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
                updated_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
            )
        """)

        # 创建索引提升查询性能
        await self.memory_db.execute("CREATE INDEX IF NOT EXISTS idx_backup_files_path ON backup_files(file_path)")
        await self.memory_db.execute("CREATE INDEX IF NOT EXISTS idx_backup_files_synced ON backup_files(synced_to_opengauss)")
        await self.memory_db.execute("CREATE INDEX IF NOT EXISTS idx_backup_files_backup_set ON backup_files(backup_set_id)")

        await self.memory_db.commit()

    async def _start_sync_tasks(self):
        """启动同步任务"""
        # 启动定期同步任务
        self._sync_task = asyncio.create_task(self._sync_loop())

        # 启动检查点任务
        self._checkpoint_task = asyncio.create_task(self._checkpoint_loop())

    async def add_file(self, file_info: Dict):
        """添加文件到内存数据库 - 根据文件扫描器输出正确映射"""
        if not self.memory_db:
            await self.initialize()

        try:
            # 准备插入数据 - 根据文件扫描器输出格式映射到BackupFile模型
            insert_data = self._prepare_insert_data_from_scanner(file_info)

            # 插入到内存数据库 - 字段顺序与BackupFile模型一致
            await self.memory_db.execute("""
                INSERT INTO backup_files (
                    backup_set_id, file_path, file_name, directory_path, display_name,
                    file_type, file_size, compressed_size, file_permissions, file_owner,
                    file_group, created_time, modified_time, accessed_time, tape_block_start,
                    tape_block_count, compressed, encrypted, checksum, is_copy_success,
                    copy_status_at, backup_time, chunk_number, version, file_metadata, tags
                ) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
            """, insert_data)

            await self.memory_db.commit()

            self._stats['total_files'] += 1
            self._last_file_added_time = time.time()  # 更新最后添加文件时间

            # 检查是否需要立即同步
            await self._check_sync_need()

        except Exception as e:
            logger.error(f"添加文件到内存数据库失败: {e}, 文件: {file_info.get('path')}")
            raise

    def _prepare_insert_data_from_scanner(self, file_info: Dict) -> tuple:
        """
        根据文件扫描器输出格式准备插入数据
        文件扫描器输出格式:
        {
            'path': str(file_path),
            'name': file_path.name,
            'size': stat.st_size,
            'modified_time': datetime.fromtimestamp(stat.st_mtime),
            'permissions': oct(stat.st_mode)[-3:],
            'is_file': file_path.is_file(),
            'is_dir': file_path.is_dir(),
            'is_symlink': file_path.is_symlink()
        }
        """
        # 基本路径信息 - 来自文件扫描器
        file_path = file_info.get('path', '')
        file_name = file_info.get('name') or Path(file_path).name

        # 目录路径
        directory_path = str(Path(file_path).parent) if file_path and Path(file_path).parent != Path(file_path).anchor else None

        # 显示名称（暂时与文件名相同）
        display_name = file_name

        # 文件类型 - 根据扫描器输出判断
        if file_info.get('is_file', True):
            file_type = 'file'
        elif file_info.get('is_dir', False):
            file_type = 'directory'
        elif file_info.get('is_symlink', False):
            file_type = 'symlink'
        else:
            file_type = 'file'

        # 文件大小 - 关键字段！直接从扫描器的size字段获取
        file_size = file_info.get('size', 0) or 0

        # 压缩大小（初始为None，压缩时更新）
        compressed_size = None

        # 文件权限 - 来自扫描器
        file_permissions = file_info.get('permissions')

        # 文件所有者和组（初始为None，Linux环境下可扩展）
        file_owner = None
        file_group = None

        # 时间戳处理 - 优先使用扫描器提供的modified_time
        modified_time = file_info.get('modified_time')
        if isinstance(modified_time, datetime):
            modified_time = modified_time.replace(tzinfo=timezone.utc)
        else:
            modified_time = datetime.now(timezone.utc)

        # 创建时间和访问时间（暂时使用修改时间作为默认值）
        created_time = modified_time
        accessed_time = modified_time

        # 磁带相关信息（初始为None，压缩时更新）
        tape_block_start = None
        tape_block_count = None
        compressed = False
        encrypted = False
        checksum = None
        is_copy_success = False
        copy_status_at = None

        # 备份时间
        backup_time = datetime.now(timezone.utc)

        # 其他字段
        chunk_number = None
        version = 1

        # 元数据（记录扫描时信息）
        file_metadata = json.dumps({
            'scanned_at': datetime.now(timezone.utc).isoformat(),
            'scanner_source': 'file_scanner',
            'original_permissions': file_permissions,
            'file_type_detected': file_info.get('is_file', True)
        })

        # 标签
        tags = json.dumps({'status': 'scanned'})

        return (
            self.backup_set_db_id,     # backup_set_id
            file_path,                 # file_path
            file_name,                 # file_name
            directory_path,            # directory_path
            display_name,              # display_name
            file_type,                 # file_type
            file_size,                 # file_size - 关键字段！
            compressed_size,           # compressed_size
            file_permissions,          # file_permissions
            file_owner,                # file_owner
            file_group,                # file_group
            created_time,              # created_time
            modified_time,             # modified_time
            accessed_time,             # accessed_time
            tape_block_start,          # tape_block_start
            tape_block_count,          # tape_block_count
            compressed,                # compressed
            encrypted,                 # encrypted
            checksum,                  # checksum
            is_copy_success,           # is_copy_success
            copy_status_at,            # copy_status_at
            backup_time,               # backup_time
            chunk_number,              # chunk_number
            version,                   # version
            file_metadata,             # file_metadata
            tags                       # tags
        )

    async def _check_sync_need(self):
        """检查是否需要同步 - 增加超时机制处理剩余少量文件"""
        current_time = time.time()
        pending_files = await self._get_pending_sync_count()

        # 条件1：文件数量达到批次大小
        if pending_files >= self.sync_batch_size:
            await self._trigger_sync("batch_size_reached")
            return

        # 条件2：达到同步间隔时间
        if current_time - self._last_sync_time >= self.sync_interval:
            await self._trigger_sync("interval_reached")
            return

        # 条件3：内存中文件过多，且有足够待同步文件
        # 优化：只有在待同步文件超过批次大小的50%时才触发，避免频繁同步少量文件
        memory_threshold = min(self.max_memory_files, self.sync_batch_size * 2)
        if (self._stats['total_files'] >= memory_threshold and
            pending_files >= self.sync_batch_size // 2):
            await self._trigger_sync("memory_limit_reached")
            return

        # 条件4：超时机制 - 扫描完成但没有达到批量大小的剩余文件
        # 如果超过60秒没有新文件添加，且有待同步文件，强制同步
        time_since_last_file = current_time - self._last_file_added_time
        if (time_since_last_file >= 60 and pending_files > 0):
            await self._trigger_sync("scan_completed_timeout")
            return

        # 条件5：检查扫描是否可能完成 - 通过待同步文件占总文件的比例判断
        if pending_files > 0:
            # 如果98%以上的文件都已同步，且距离上次同步超过30秒，强制同步剩余文件
            sync_ratio = (self._stats['synced_files'] / max(1, self._stats['total_files']))
            if (sync_ratio >= 0.98 and
                current_time - self._last_sync_time >= 30):
                await self._trigger_sync("almost_complete")
                return

    async def _get_pending_sync_count(self) -> int:
        """获取待同步文件数量"""
        async with self.memory_db.execute(
            "SELECT COUNT(*) FROM backup_files WHERE synced_to_opengauss = FALSE"
        ) as cursor:
            result = await cursor.fetchone()
            return result[0] if result else 0

    async def _trigger_sync(self, reason: str):
        """触发同步 - 增加防抖动机制"""
        current_time = time.time()

        # 防抖动：避免1秒内频繁触发同步
        if current_time - self._last_trigger_time < 1.0:
            logger.debug(f"同步触发过于频繁，跳过 (原因: {reason})")
            return

        if self._is_syncing:
            logger.debug(f"同步已在进行中，跳过触发 (原因: {reason})")
            return

        self._last_trigger_time = current_time
        logger.info(f"触发同步到openGauss (原因: {reason})")
        await self._sync_to_opengauss()

    async def _sync_loop(self):
        """定期同步循环"""
        while True:
            try:
                await asyncio.sleep(self.sync_interval)

                if not self._is_syncing:
                    await self._sync_to_opengauss("scheduled")

            except asyncio.CancelledError:
                break
            except Exception as e:
                logger.error(f"同步循环异常: {e}")
                await asyncio.sleep(5)  # 错误后短暂等待

    async def _checkpoint_loop(self):
        """检查点循环 - 持久化保护"""
        while True:
            try:
                await asyncio.sleep(self.checkpoint_interval)
                await self._create_checkpoint()

            except asyncio.CancelledError:
                break
            except Exception as e:
                logger.error(f"检查点循环异常: {e}")
                await asyncio.sleep(30)  # 错误后等待更长时间

    async def _sync_to_opengauss(self, reason: str = "manual"):
        """同步文件到openGauss - 使用原生SQL，严禁SQLAlchemy解析openGauss"""
        if self._is_syncing:
            return

        self._is_syncing = True
        sync_start_time = time.time()

        try:
            # 获取待同步的文件批次
            files_to_sync = await self._get_files_to_sync()

            if not files_to_sync:
                logger.debug("没有文件需要同步")
                return

            logger.info(f"开始同步 {len(files_to_sync)} 个文件到openGauss (原因: {reason})")

            # 批量同步到openGauss
            synced_count = await self._batch_sync_to_opengauss(files_to_sync)

            # 更新同步状态
            await self._mark_files_synced([f[0] for f in files_to_sync[:synced_count]])

            # 更新统计
            sync_time = time.time() - sync_start_time
            self._stats['synced_files'] += synced_count
            self._stats['sync_batches'] += 1
            self._stats['sync_time'] += sync_time
            self._last_sync_time = time.time()

            logger.info(f"同步完成: {synced_count}/{len(files_to_sync)} 个文件，耗时 {sync_time:.2f}s")

        except Exception as e:
            logger.error(f"同步到openGauss失败: {e}", exc_info=True)
            # 记录同步错误
            await self._mark_sync_error(files_to_sync, str(e))

        finally:
            self._is_syncing = False

    async def _get_files_to_sync(self) -> List[Tuple]:
        """获取待同步的文件 - 按照BackupFile模型字段顺序"""
        async with self.memory_db.execute("""
            SELECT id, backup_set_id, file_path, file_name, directory_path, display_name,
                   file_type, file_size, compressed_size, file_permissions, file_owner,
                   file_group, created_time, modified_time, accessed_time, tape_block_start,
                   tape_block_count, compressed, encrypted, checksum, is_copy_success,
                   copy_status_at, backup_time, chunk_number, version, file_metadata, tags
            FROM backup_files
            WHERE synced_to_opengauss = FALSE
            ORDER BY id
            LIMIT ?
        """, (self.sync_batch_size,)) as cursor:
            return await cursor.fetchall()

    def _parse_datetime_from_sqlite(self, dt_value) -> datetime:
        """将SQLite的datetime值转换为Python datetime对象"""
        if dt_value is None:
            return None

        if isinstance(dt_value, datetime):
            return dt_value

        if isinstance(dt_value, str):
            try:
                # SQLite返回的字符串格式："2025-04-27 06:04:31.136616+00:00"
                # 或 "2025-04-27 06:04:31"
                if '+' in dt_value:
                    # 处理带时区的格式
                    return datetime.fromisoformat(dt_value.replace('Z', '+00:00'))
                else:
                    # 处理不带时区的格式
                    naive_dt = datetime.fromisoformat(dt_value)
                    return naive_dt.replace(tzinfo=timezone.utc)
            except ValueError:
                # 如果解析失败，返回当前时间
                return datetime.now(timezone.utc)

        # 其他情况，返回当前时间
        return datetime.now(timezone.utc)

    async def _batch_sync_to_opengauss(self, files: List[Tuple]) -> int:
        """批量同步到openGauss - 使用原生SQL，严禁SQLAlchemy解析openGauss"""
        if not files:
            return 0

        synced_count = 0
        async with get_opengauss_connection() as conn:
            for file_record in files:
                try:
                    # 转换数据格式，按照内存数据库字段顺序映射到openGauss
                    # file_record字段顺序：id, backup_set_id, file_path, file_name, directory_path, display_name,
                    # file_type, file_size, compressed_size, file_permissions, file_owner,
                    # file_group, created_time, modified_time, accessed_time, tape_block_start,
                    # tape_block_count, compressed, encrypted, checksum, is_copy_success,
                    # copy_status_at, backup_time, chunk_number, version, file_metadata, tags

                    # 修复datetime字段转换
                    backup_set_id = file_record[1]
                    file_path = file_record[2]
                    file_name = file_record[3]
                    directory_path = file_record[4]
                    display_name = file_record[5]
                    file_type = file_record[6]
                    file_size = file_record[7]  # 关键字段！
                    compressed_size = file_record[8]
                    file_permissions = file_record[9]
                    file_owner = file_record[10]
                    file_group = file_record[11]

                    # 修复：正确转换datetime字段
                    created_time = self._parse_datetime_from_sqlite(file_record[12])
                    modified_time = self._parse_datetime_from_sqlite(file_record[13])
                    accessed_time = self._parse_datetime_from_sqlite(file_record[14])
                    copy_status_at = self._parse_datetime_from_sqlite(file_record[21])
                    backup_time = self._parse_datetime_from_sqlite(file_record[22])

                    tape_block_start = file_record[15]
                    tape_block_count = file_record[16]
                    compressed = bool(file_record[17])
                    encrypted = bool(file_record[18])
                    checksum = file_record[19]
                    is_copy_success = bool(file_record[20])
                    chunk_number = file_record[23]
                    version = file_record[24]
                    file_metadata = file_record[25]
                    tags = file_record[26]

                    # 使用原生SQL插入，确保datetime字段正确传递
                    await conn.execute("""
                        INSERT INTO backup_files (
                            backup_set_id, file_path, file_name, directory_path, display_name,
                            file_type, file_size, compressed_size, file_permissions, file_owner,
                            file_group, created_time, modified_time, accessed_time, tape_block_start,
                            tape_block_count, compressed, encrypted, checksum, is_copy_success,
                            copy_status_at, backup_time, chunk_number, version, file_metadata, tags,
                            created_at, updated_at
                        ) VALUES (
                            $1, $2, $3, $4, $5, $6, $7, $8, $9, $10,
                            $11, $12, $13, $14, $15, $16, $17, $18, $19, $20,
                            $21, $22, $23, $24, $25::json, $26::json, NOW(), NOW()
                        )
                    """,
                    backup_set_id, file_path, file_name,
                    directory_path, display_name, file_type,
                    file_size, compressed_size, file_permissions,
                    file_owner, file_group, created_time,  # 现在是datetime对象
                    modified_time, accessed_time, tape_block_start,
                    tape_block_count, compressed, encrypted,
                    checksum, is_copy_success, copy_status_at,  # 现在是datetime对象
                    backup_time, chunk_number, version,  # backup_time现在是datetime对象
                    file_metadata, tags
                    )

                    synced_count += 1

                except Exception as e:
                    logger.error(f"同步单个文件失败: {e}, 文件: {file_record[2] if len(file_record) > 2 else 'unknown'}")
                    # 继续处理其他文件
                    continue

        return synced_count

    async def _mark_files_synced(self, file_ids: List[int]):
        """标记文件已同步"""
        if not file_ids:
            return

        placeholders = ','.join(['?' for _ in file_ids])
        await self.memory_db.execute(
            f"UPDATE backup_files SET synced_to_opengauss = TRUE, sync_error = NULL WHERE id IN ({placeholders})",
            file_ids
        )
        await self.memory_db.commit()

    async def _mark_sync_error(self, files: List[Tuple], error_message: str):
        """标记同步错误"""
        file_ids = [f[0] for f in files]

        placeholders = ','.join(['?' for _ in file_ids])
        await self.memory_db.execute(
            f"UPDATE backup_files SET sync_error = ? WHERE id IN ({placeholders})",
            [error_message] + file_ids
        )
        await self.memory_db.commit()

    async def _create_checkpoint(self):
        """创建检查点 - 持久化保护"""
        try:
            # 导出内存数据库到临时文件
            with tempfile.NamedTemporaryFile(mode='w', suffix='.sql', delete=False, encoding='utf-8') as f:
                checkpoint_file = f.name

            # 备份内存数据库
            with open(checkpoint_file, 'w', encoding='utf-8') as f:
                async for line in self.memory_db.iterdump():
                    f.write(f"{line}\n")

            self._last_checkpoint_time = time.time()
            self._checkpoint_files.append((checkpoint_file, self._last_checkpoint_time))
            logger.info(f"检查点已创建: {checkpoint_file}")
            
            # 清理过期的检查点文件
            await self._cleanup_old_checkpoints()

        except Exception as e:
            logger.error(f"创建检查点失败: {e}")
    
    async def _cleanup_old_checkpoints(self):
        """清理过期的检查点文件"""
        try:
            import os
            current_time = time.time()
            retention_seconds = self.checkpoint_retention_hours * 3600
            
            # 清理记录列表中的过期文件
            files_to_remove = []
            for checkpoint_file, create_time in self._checkpoint_files[:]:
                if current_time - create_time > retention_seconds:
                    try:
                        if os.path.exists(checkpoint_file):
                            os.remove(checkpoint_file)
                            logger.debug(f"已删除过期检查点文件: {checkpoint_file}")
                        files_to_remove.append((checkpoint_file, create_time))
                    except Exception as e:
                        logger.warning(f"删除检查点文件失败: {checkpoint_file}, 错误: {e}")
            
            # 从列表中移除已删除的文件
            for item in files_to_remove:
                if item in self._checkpoint_files:
                    self._checkpoint_files.remove(item)
            
            # 同时清理临时目录中可能遗留的检查点文件（通过文件名模式匹配）
            try:
                import glob
                temp_dir = tempfile.gettempdir()
                pattern = os.path.join(temp_dir, 'tmp*.sql')
                for old_file in glob.glob(pattern):
                    try:
                        file_stat = os.stat(old_file)
                        file_age = current_time - file_stat.st_mtime
                        if file_age > retention_seconds:
                            os.remove(old_file)
                            logger.debug(f"已删除临时目录中的过期检查点文件: {old_file}")
                    except Exception as e:
                        logger.debug(f"清理临时文件时出错（忽略）: {old_file}, {e}")
            except Exception as e:
                logger.debug(f"清理临时目录检查点文件失败（忽略）: {e}")
                
        except Exception as e:
            logger.warning(f"清理过期检查点文件失败: {e}")
    
    async def _cleanup_all_checkpoints(self):
        """清理所有检查点文件（停止时调用）"""
        try:
            import os
            for checkpoint_file, _ in self._checkpoint_files[:]:
                try:
                    if os.path.exists(checkpoint_file):
                        os.remove(checkpoint_file)
                        logger.debug(f"已删除检查点文件: {checkpoint_file}")
                except Exception as e:
                    logger.warning(f"删除检查点文件失败: {checkpoint_file}, 错误: {e}")
            self._checkpoint_files.clear()
        except Exception as e:
            logger.warning(f"清理所有检查点文件失败: {e}")

    async def force_sync(self):
        """强制同步所有待同步文件"""
        logger.info("强制同步所有待同步文件")
        await self._sync_to_opengauss("force_sync")

    async def stop(self):
        """停止内存数据库写入器"""
        logger.info("停止内存数据库写入器")

        # 停止同步任务
        if self._sync_task:
            self._sync_task.cancel()
            try:
                await self._sync_task
            except asyncio.CancelledError:
                pass

        if self._checkpoint_task:
            self._checkpoint_task.cancel()
            try:
                await self._checkpoint_task
            except asyncio.CancelledError:
                pass

        # 最后一次同步
        if self.memory_db:
            try:
                await self.force_sync()
                await self._create_checkpoint()
            except Exception as e:
                logger.error(f"最终同步失败: {e}")
        
        # 清理所有检查点文件（停止时）
        await self._cleanup_all_checkpoints()

        # 关闭数据库连接
        if self.memory_db:
            await self.memory_db.close()

    def get_stats(self) -> Dict:
        """获取统计信息"""
        stats = self._stats.copy()
        stats['memory_usage'] = self._stats['total_files'] * 2  # 估算内存使用(KB)
        stats['pending_sync'] = self._stats['total_files'] - self._stats['synced_files']
        stats['sync_progress'] = (self._stats['synced_files'] / max(1, self._stats['total_files'])) * 100

        return stats

    async def get_sync_status(self) -> Dict:
        """获取同步状态详情"""
        if not self.memory_db:
            return {'status': 'not_initialized'}

        async with self.memory_db.execute("""
            SELECT
                COUNT(*) as total_files,
                COUNT(CASE WHEN synced_to_opengauss = TRUE THEN 1 END) as synced_files,
                COUNT(CASE WHEN synced_to_opengauss = FALSE AND sync_error IS NOT NULL THEN 1 END) as error_files,
                COUNT(CASE WHEN synced_to_opengauss = FALSE THEN 1 END) as pending_files,
                SUM(file_size) as total_size
            FROM backup_files
        """) as cursor:
            result = await cursor.fetchone()

            return {
                'total_files': result[0],
                'synced_files': result[1],
                'error_files': result[2],
                'pending_files': result[3],
                'total_size': result[4] or 0,
                'sync_progress': (result[1] / max(1, result[0])) * 100,
                'is_syncing': self._is_syncing,
                'last_sync_time': self._last_sync_time,
                'last_checkpoint_time': self._last_checkpoint_time
            }