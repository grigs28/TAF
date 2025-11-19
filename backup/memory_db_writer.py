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
                 max_memory_files: int = 5000000,        # 内存中最大文件数（500万）
                 checkpoint_interval: int = 300,         # 检查点间隔(秒)
                 checkpoint_retention_hours: int = 24,   # 检查点保留时间(小时)
                 enable_checkpoint: bool = False):        # 是否启用检查点，默认不启用

        self.backup_set_db_id = backup_set_db_id
        self.sync_batch_size = sync_batch_size
        self.sync_interval = sync_interval
        self.max_memory_files = max_memory_files
        self.checkpoint_interval = checkpoint_interval
        self.checkpoint_retention_hours = checkpoint_retention_hours
        self.enable_checkpoint = enable_checkpoint  # 是否启用检查点

        # 检查点目录：使用项目根目录下的 temp/checkpoints 目录（仅在启用检查点时创建）
        if self.enable_checkpoint:
            project_root = Path(__file__).parent.parent  # backup -> 项目根目录
            self.checkpoint_dir = project_root / "temp" / "checkpoints"
            self.checkpoint_dir.mkdir(parents=True, exist_ok=True)
        else:
            self.checkpoint_dir = None

        # 内存数据库
        self.memory_db = None
        self.db_connection = None

        # 同步相关
        self._is_syncing = False
        self._sync_task = None
        self._checkpoint_task = None
        self._last_sync_time = 0
        self._sync_start_time = 0  # 当前同步开始时间
        self._last_checkpoint_time = 0
        self._last_trigger_time = 0  # 防止频繁触发同步
        self._last_file_added_time = time.time()  # 记录最后添加文件的时间
        self._checkpoint_files = []  # 记录创建的检查点文件列表 [(文件路径, 创建时间, 最大未同步文件ID), ...]

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
        # 启动时清理过期的检查点文件（仅在启用检查点时）
        if self.enable_checkpoint:
            await self._cleanup_old_checkpoints_on_startup()
        await self._setup_memory_database()
        await self._start_sync_tasks()
        logger.info(f"内存数据库写入器已初始化 (backup_set_id={self.backup_set_db_id}, checkpoint={self.enable_checkpoint})")

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
        logger.info(f"内存数据库同步任务已启动 (同步间隔: {self.sync_interval}秒, 批次大小: {self.sync_batch_size})")

        # 仅在启用检查点时启动检查点任务
        if self.enable_checkpoint:
            self._checkpoint_task = asyncio.create_task(self._checkpoint_loop())
            logger.info(f"检查点任务已启动 (间隔: {self.checkpoint_interval}秒)")
        else:
            self._checkpoint_task = None
            logger.info("检查点功能已禁用")

    async def add_file(self, file_info: Dict):
        """添加文件到内存数据库 - 根据文件扫描器输出正确映射（单个文件）"""
        if not self.memory_db:
            await self.initialize()

        try:
            # 准备插入数据 - 根据文件扫描器输出格式映射到BackupFile模型
            insert_data = self._prepare_insert_data_from_scanner(file_info)

            # 插入到内存数据库 - 字段顺序与BackupFile模型一致
            # 注意：显式包含 synced_to_opengauss 和 sync_error 字段，确保数据一致性
            # 验证 backup_set_id 是否正确
            backup_set_id_in_data = insert_data[0] if insert_data else None
            if backup_set_id_in_data != self.backup_set_db_id:
                logger.error(
                    f"[内存数据库] ⚠️⚠️ 错误：文件数据的 backup_set_id={backup_set_id_in_data} "
                    f"与 MemoryDBWriter 的 backup_set_db_id={self.backup_set_db_id} 不匹配！"
                )
            
            await self.memory_db.execute("""
                INSERT INTO backup_files (
                    backup_set_id, file_path, file_name, directory_path, display_name,
                    file_type, file_size, compressed_size, file_permissions, file_owner,
                    file_group, created_time, modified_time, accessed_time, tape_block_start,
                    tape_block_count, compressed, encrypted, checksum, is_copy_success,
                    copy_status_at, backup_time, chunk_number, version, file_metadata, tags,
                    synced_to_opengauss, sync_error
                ) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
            """, insert_data + (False, None))  # 显式设置 synced_to_opengauss = FALSE, sync_error = NULL

            await self.memory_db.commit()

            self._stats['total_files'] += 1
            self._last_file_added_time = time.time()  # 更新最后添加文件时间

            # 检查是否需要立即同步
            await self._check_sync_need()

        except Exception as e:
            file_path = file_info.get('path', 'unknown')
            logger.error(
                f"添加文件到内存数据库失败: {e}, "
                f"文件路径: {file_path[:200]}, "
                f"file_info键: {list(file_info.keys())}, "
                f"file_info值: {dict((k, type(v).__name__ if not isinstance(v, (str, int, bool, type(None))) else v) for k, v in file_info.items())}"
            )
            raise

    async def add_files_batch(self, file_info_list: List[Dict]):
        """批量添加文件到内存数据库 - 使用批量插入优化性能
        
        Args:
            file_info_list: 文件信息列表
        """
        if not file_info_list:
            return
        
        if not self.memory_db:
            await self.initialize()

        try:
            # 准备批量插入数据
            insert_data_list = []
            for file_info in file_info_list:
                try:
                    insert_data = self._prepare_insert_data_from_scanner(file_info)
                    # 添加 synced_to_opengauss 和 sync_error 字段
                    insert_data_list.append(insert_data + (False, None))
                except Exception as e:
                    file_path = file_info.get('path', 'unknown')
                    logger.warning(f"准备批量插入数据失败: {file_path[:200]}, 错误: {str(e)}")
                    continue
            
            if not insert_data_list:
                logger.warning("批量插入：没有有效的数据可以插入")
                return
            
            # 使用 executemany 批量插入（性能优化：一次插入多个文件，只提交一次）
            await self.memory_db.executemany("""
                INSERT INTO backup_files (
                    backup_set_id, file_path, file_name, directory_path, display_name,
                    file_type, file_size, compressed_size, file_permissions, file_owner,
                    file_group, created_time, modified_time, accessed_time, tape_block_start,
                    tape_block_count, compressed, encrypted, checksum, is_copy_success,
                    copy_status_at, backup_time, chunk_number, version, file_metadata, tags,
                    synced_to_opengauss, sync_error
                ) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
            """, insert_data_list)

            # 只提交一次（批量插入的关键优化）
            await self.memory_db.commit()

            # 更新统计信息
            added_count = len(insert_data_list)
            self._stats['total_files'] += added_count
            self._last_file_added_time = time.time()  # 更新最后添加文件时间

            # 数据写入保证机制：
            # 1. executemany 执行成功（如果失败会抛出异常）
            # 2. commit() 成功提交事务（如果失败会抛出异常）
            # 3. 如果任何步骤失败，异常会被捕获并向上抛出，扫描器会处理（回退到逐个添加）
            # 因此，如果方法正常返回（没有抛出异常），数据已经成功写入并持久化
            logger.debug(f"批量插入完成：成功插入 {added_count} 个文件到内存数据库（已提交事务）")

            # 检查是否需要立即同步（批量添加后只检查一次）
            await self._check_sync_need()

        except Exception as e:
            logger.error(
                f"批量添加文件到内存数据库失败: {e}, "
                f"文件数量: {len(file_info_list)}",
                exc_info=True
            )
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
        # 如果同步正在进行中，直接返回，避免重复触发和产生大量日志
        if self._is_syncing:
            return
        
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
        """获取待同步文件数量（仅当前备份集）"""
        async with self.memory_db.execute(
            "SELECT COUNT(*) FROM backup_files WHERE backup_set_id = ? AND synced_to_opengauss = FALSE",
            (self.backup_set_db_id,)
        ) as cursor:
            result = await cursor.fetchone()
            return result[0] if result else 0

    async def _trigger_sync(self, reason: str):
        """触发同步 - 增加防抖动机制，异步执行不阻塞扫描线程"""
        current_time = time.time()

        # 防抖动：避免1秒内频繁触发同步
        if current_time - self._last_trigger_time < 1.0:
            logger.debug(f"同步触发过于频繁，跳过 (原因: {reason})")
            return

        if self._is_syncing:
            # 降低日志级别，避免同步进行时产生大量重复日志
            logger.debug(f"同步已在进行中，跳过触发 (原因: {reason})")
            return

        self._last_trigger_time = current_time
        
        # 检查数据库类型以显示正确的日志
        from utils.scheduler.db_utils import is_opengauss
        db_type = "openGauss" if is_opengauss() else "SQLite"
        logger.info(f"触发同步到{db_type} (原因: {reason})")
        
        # 创建异步任务执行同步，不阻塞当前线程（扫描线程）
        # 这样扫描和同步可以并行执行，互不阻塞
        asyncio.create_task(self._sync_to_opengauss(reason))

    async def _sync_loop(self):
        """定期同步循环"""
        logger.info("内存数据库同步循环已启动，等待同步间隔...")
        while True:
            try:
                await asyncio.sleep(self.sync_interval)
                
                logger.info(f"定期同步触发（间隔: {self.sync_interval}秒）")

                if not self._is_syncing:
                    await self._sync_to_opengauss("scheduled")
                else:
                    # 获取同步信息（只在跳过时输出）
                    pending_count = await self._get_pending_sync_count()
                    total_scanned = self._stats['total_files']
                    total_synced = self._stats['synced_files']
                    # 计算当前同步已持续的时间
                    if self._sync_start_time > 0:
                        sync_duration = time.time() - self._sync_start_time
                    else:
                        # 如果没有记录开始时间，使用上次完成时间作为参考
                        sync_duration = time.time() - self._last_sync_time if self._last_sync_time > 0 else 0
                    
                    logger.warning(
                        f"⚠️ 同步正在进行中，跳过本次定期同步 - "
                        f"待同步: {pending_count} 个，"
                        f"累计总扫描: {total_scanned} 个，累计总同步: {total_synced} 个，"
                        f"当前同步已持续: {sync_duration:.1f}秒"
                    )
                    # 如果同步状态持续超过5分钟，记录警告（可能是卡住了）
                    if sync_duration > 300:
                        logger.error(
                            f"⚠️⚠️ 警告：同步状态已持续 {sync_duration:.1f} 秒（超过5分钟），"
                            f"可能已卡住！待同步: {pending_count} 个文件。"
                            f"建议检查 SQLite 队列管理器是否正常工作。"
                    )

            except asyncio.CancelledError:
                logger.info("内存数据库同步循环被取消")
                break
            except Exception as e:
                logger.error(f"同步循环异常: {e}", exc_info=True)
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
        """同步文件到主数据库（openGauss 或 SQLite）
        
        每次同步时，循环处理所有未同步的文件，直到全部同步完成（分批处理）
        """
        if self._is_syncing:
            return

        # 检查数据库类型
        from utils.scheduler.db_utils import is_opengauss
        if not is_opengauss():
            # SQLite 模式：使用队列同步到主数据库（写操作优先）
            await self._sync_to_sqlite_via_queue(reason)
            return

        self._is_syncing = True
        self._sync_start_time = time.time()  # 记录同步开始时间（用于计算持续时间）
        sync_start_time = self._sync_start_time
        total_synced_count = 0
        batch_number = 0

        try:
            # 记录同步开始时的待同步文件数
            initial_pending_count = await self._get_pending_sync_count()
            if initial_pending_count > 0:
                logger.info(f"[同步开始] 待同步文件数: {initial_pending_count} 个 (原因: {reason})")
            
            # 循环同步，直到所有未同步的文件都处理完成
            while True:
                # 获取待同步的文件批次（每次获取一批）
                files_to_sync = await self._get_files_to_sync()

                if not files_to_sync:
                    # 没有更多文件需要同步
                    if batch_number == 0:
                        logger.info("内存数据库中没有文件需要同步到openGauss")
                    break

                batch_number += 1
                logger.debug(f"[批次 {batch_number}] 开始同步 {len(files_to_sync)} 个文件到openGauss (原因: {reason})")

                # 批量同步到openGauss
                synced_count, synced_file_ids = await self._batch_sync_to_opengauss(files_to_sync)

                # 更新同步状态（只标记成功同步的文件）
                if synced_file_ids:
                    await self._mark_files_synced(synced_file_ids)

                # 更新统计
                total_synced_count += synced_count
                self._stats['synced_files'] += synced_count
                self._stats['sync_batches'] += 1

                logger.debug(f"[批次 {batch_number}] ✅ 同步完成: {synced_count}/{len(files_to_sync)} 个文件已成功同步到openGauss")
                
                # 检查剩余待同步文件数（用于确认是否所有文件都被同步）
                pending_count = await self._get_pending_sync_count()
                if pending_count > 0:
                    logger.debug(f"[批次 {batch_number}] 内存数据库中还有 {pending_count} 个文件待同步，将在下次同步时处理")
                
                # 如果当前批次中还有未同步的文件，记录警告
                if synced_count < len(files_to_sync):
                    remaining = len(files_to_sync) - synced_count
                    logger.info(f"[批次 {batch_number}] ⚠️ 还有 {remaining} 个文件同步失败，将在下次同步时重试")

            # 所有批次同步完成
            if batch_number > 0:
                sync_time = time.time() - sync_start_time
                self._stats['sync_time'] += sync_time
                self._last_sync_time = time.time()
                
                # 检查是否还有未同步的文件
                final_pending_count = await self._get_pending_sync_count()
                
                # 获取累计统计信息
                total_scanned = self._stats['total_files']  # 总扫描数（从任务开始到现在）
                total_synced_accumulated = self._stats['synced_files']  # 累计总同步数（从任务开始到现在）
                
                logger.info(
                    f"✅ 全部同步完成: 共 {batch_number} 个批次，总耗时 {sync_time:.2f}秒，"
                    f"同步开始时待同步: {initial_pending_count} 个，"
                    f"同步完成后剩余: {final_pending_count} 个，"
                    f"本次同步: {total_synced_count} 个，"
                    f"累计总扫描: {total_scanned} 个，"
                    f"累计总同步: {total_synced_accumulated} 个"
                )
                
                # 检查总扫描数和总同步数是否一致
                if total_scanned > 0:
                    sync_ratio = (total_synced_accumulated / total_scanned) * 100
                    if total_synced_accumulated < total_scanned:
                        logger.info(
                            f"⚠️ 同步进度: {sync_ratio:.1f}% "
                            f"（总扫描: {total_scanned} 个，总同步: {total_synced_accumulated} 个，"
                            f"待同步: {total_scanned - total_synced_accumulated} 个）"
                        )
                    elif total_synced_accumulated == total_scanned:
                        logger.info(f"✅ 同步完成: 总扫描 {total_scanned} 个文件已全部同步到openGauss数据库")
                    else:
                        logger.warning(
                            f"⚠️ 异常: 总同步数 ({total_synced_accumulated}) 大于总扫描数 ({total_scanned})，"
                            f"可能存在数据不一致"
                        )
                
                if final_pending_count > 0:
                    # 计算新增的文件数（同步过程中ES扫描器添加的新文件）
                    new_files_during_sync = final_pending_count - (initial_pending_count - total_synced_count)
                    if new_files_during_sync > 0:
                        logger.info(f"📊 同步过程中新增了 {new_files_during_sync} 个文件（ES扫描器持续添加）")
                    logger.info(f"⚠️ 仍有 {final_pending_count} 个文件未同步，将在下次同步时重试")

        except Exception as e:
            logger.error(f"同步到openGauss数据库失败: {e}", exc_info=True)
            # 记录同步错误（如果有）
            if 'files_to_sync' in locals() and files_to_sync:
                await self._mark_sync_error(files_to_sync, str(e))

        finally:
            self._is_syncing = False
            self._sync_start_time = 0  # 重置同步开始时间

    async def _get_files_to_sync(self) -> List[Tuple]:
        """获取待同步的文件 - 按照BackupFile模型字段顺序（仅当前备份集）"""
        # 先检查内存数据库中有多少文件
        async with self.memory_db.execute("""
            SELECT COUNT(*) FROM backup_files
            WHERE backup_set_id = ? AND synced_to_opengauss = FALSE
        """, (self.backup_set_db_id,)) as cursor:
            pending_count = (await cursor.fetchone())[0]
        
        # 检查是否有其他 backup_set_id 的文件
        async with self.memory_db.execute("""
            SELECT DISTINCT backup_set_id, COUNT(*) as cnt
            FROM backup_files
            WHERE synced_to_opengauss = FALSE
            GROUP BY backup_set_id
            LIMIT 5
        """) as cursor:
            all_pending = await cursor.fetchall()
        
        if pending_count > 0:
            logger.debug(
                f"[同步] 内存数据库中待同步文件: backup_set_id={self.backup_set_db_id}, "
                f"数量={pending_count}, 其他备份集: {all_pending}"
            )
        
        async with self.memory_db.execute("""
            SELECT id, backup_set_id, file_path, file_name, directory_path, display_name,
                   file_type, file_size, compressed_size, file_permissions, file_owner,
                   file_group, created_time, modified_time, accessed_time, tape_block_start,
                   tape_block_count, compressed, encrypted, checksum, is_copy_success,
                   copy_status_at, backup_time, chunk_number, version, file_metadata, tags
            FROM backup_files
            WHERE backup_set_id = ? AND synced_to_opengauss = FALSE
            ORDER BY id
            LIMIT ?
        """, (self.backup_set_db_id, self.sync_batch_size)) as cursor:
            files = await cursor.fetchall()
            if files:
                # 验证第一个文件的 backup_set_id
                first_file_backup_set_id = files[0][1] if len(files[0]) > 1 else None
                if first_file_backup_set_id != self.backup_set_db_id:
                    logger.error(
                        f"[同步] ⚠️⚠️ 错误：待同步文件的 backup_set_id={first_file_backup_set_id} "
                        f"与 MemoryDBWriter 的 backup_set_db_id={self.backup_set_db_id} 不匹配！"
                    )
            return files

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

    async def _batch_sync_to_opengauss(self, files: List[Tuple]) -> Tuple[int, List[int]]:
        """批量同步到openGauss - 使用原生SQL批量插入，严禁SQLAlchemy解析openGauss
        
        优化：使用 executemany 实现真正的批量插入，大幅提升性能
        """
        if not files:
            return 0, []

        # 检查数据库类型
        from utils.scheduler.db_utils import is_opengauss
        if not is_opengauss():
            synced_file_ids = await self._insert_files_to_sqlite(file_data_map)
            return len(synced_file_ids), synced_file_ids

        logger.debug(f"正在批量同步 {len(files)} 个文件到openGauss数据库（使用批量插入优化）...")
        
        # 准备批量插入数据
        insert_data = []
        file_data_map = []  # 保存文件记录和数据的对应关系 [(file_record, data_tuple), ...]
        failed_files = []  # 记录失败的文件索引和错误信息
        
        for idx, file_record in enumerate(files):
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

                # 注意：数据库字段已改为 TEXT 类型，无长度限制，不需要截断
                # 如果数据库迁移未执行，字段仍然是 VARCHAR(255)，会在插入时报错
                # 这种情况下，需要执行数据库迁移将字段类型改为 TEXT

                # 准备批量插入的数据元组（按照 VALUES 子句的顺序）
                data_tuple = (
                    backup_set_id, file_path, file_name,
                    directory_path, display_name, file_type,
                    file_size, compressed_size, file_permissions,
                    file_owner, file_group, created_time,
                    modified_time, accessed_time, tape_block_start,
                    tape_block_count, compressed, encrypted,
                    checksum, is_copy_success, copy_status_at,
                    backup_time, chunk_number, version,
                    file_metadata, tags
                )
                insert_data.append(data_tuple)
                file_data_map.append((file_record, data_tuple))  # 保存对应关系

            except Exception as e:
                # 数据准备阶段失败，记录错误
                file_path_str = file_record[2] if len(file_record) > 2 else 'unknown'
                logger.error(f"准备批量插入数据失败（索引 {idx}）: {e}, 文件: {file_path_str}")
                failed_files.append((idx, file_path_str, str(e)))
                continue

        if not insert_data:
            logger.info(f"没有有效的数据可以批量插入，所有 {len(files)} 个文件都在数据准备阶段失败")
            return 0, []

        # 执行批量插入
        synced_file_ids = []  # 成功同步的文件ID列表
        try:
            async with get_opengauss_connection() as conn:
                # 使用 executemany 实现真正的批量插入
                # 注意：asyncpg 的 executemany 会自动处理批量插入
                await conn.executemany("""
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
                """, insert_data)
                
                # 批量插入成功，所有文件都已同步
                synced_count = len(insert_data)
                # 提取成功同步的文件ID（file_record[0] 是文件ID）
                synced_file_ids = [file_record[0] for file_record, _ in file_data_map]
                logger.debug(f"批量插入成功: {synced_count} 个文件已同步到openGauss数据库")

        except Exception as e:
            # 批量插入失败，尝试逐个插入以确定哪些文件失败
            logger.warning(f"批量插入失败: {e}，尝试逐个插入以确定失败的文件...")
            synced_count, synced_file_ids = await self._fallback_individual_insert(file_data_map, failed_files)
        
        # 记录失败的文件
        if failed_files:
            logger.warning(f"数据准备阶段失败的文件数: {len(failed_files)}")
            for idx, file_path_str, error_msg in failed_files[:10]:  # 只记录前10个
                logger.debug(f"  失败文件 [{idx}]: {file_path_str}, 错误: {error_msg}")
            if len(failed_files) > 10:
                logger.debug(f"  ... 还有 {len(failed_files) - 10} 个失败文件未显示")

        return synced_count, synced_file_ids

    async def _fallback_individual_insert(self, file_data_map: List[Tuple], failed_files: List[Tuple]) -> Tuple[int, List[int]]:
        """批量插入失败时的回退方案：逐个插入以确定失败的文件
        
        Args:
            file_data_map: 文件记录和数据的对应关系列表 [(file_record, data_tuple), ...]
            failed_files: 失败文件列表，用于追加新的失败记录
            
        Returns:
            (成功同步的文件数, 成功同步的文件ID列表)
        """
        # 检查数据库类型
        from utils.scheduler.db_utils import is_opengauss
        if not is_opengauss():
            synced_file_ids = await self._insert_files_to_sqlite(file_data_map)
            return len(synced_file_ids), synced_file_ids

        synced_count = 0
        synced_file_ids = []
        async with get_opengauss_connection() as conn:
            for idx, (file_record, data_tuple) in enumerate(file_data_map):
                try:
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
                    """, *data_tuple)
                    synced_count += 1
                    # 记录成功同步的文件ID（file_record[0] 是文件ID）
                    synced_file_ids.append(file_record[0])
                except Exception as e:
                    file_path_str = file_record[2] if len(file_record) > 2 else 'unknown'
                    logger.error(f"回退逐个插入失败（索引 {idx}）: {e}, 文件: {file_path_str}")
                    # 注意：这里不记录原始索引，因为 file_data_map 中只包含成功准备数据的文件
                    failed_files.append((idx, file_path_str, str(e)))
        
        return synced_count, synced_file_ids

    async def _mark_files_synced(self, file_ids: List[int]):
        """标记文件已同步（仅当前备份集）"""
        if not file_ids:
            return

        placeholders = ','.join(['?' for _ in file_ids])
        await self.memory_db.execute(
            f"UPDATE backup_files SET synced_to_opengauss = TRUE, sync_error = NULL WHERE backup_set_id = ? AND id IN ({placeholders})",
            [self.backup_set_db_id] + file_ids
        )
        await self.memory_db.commit()
        
        # 同步成功后，清理已完全同步的检查点文件
        await self._cleanup_synced_checkpoints()

    async def _mark_sync_error(self, files: List[Tuple], error_message: str):
        """标记同步错误（仅当前备份集）"""
        file_ids = [f[0] for f in files]

        placeholders = ','.join(['?' for _ in file_ids])
        await self.memory_db.execute(
            f"UPDATE backup_files SET sync_error = ? WHERE backup_set_id = ? AND id IN ({placeholders})",
            [error_message, self.backup_set_db_id] + file_ids
        )
        await self.memory_db.commit()

    async def _insert_files_to_sqlite(self, file_data_map: List[Tuple]) -> List[int]:
        """将扫描文件同步到 SQLite 主库（调用方负责队列和串行执行）"""
        from backup.sqlite_backup_db import insert_backup_files_sqlite

        files_payload: List[Dict] = []
        synced_file_ids: List[int] = []

        for file_record, _ in file_data_map:
            if not file_record:
                continue

            file_id = file_record[0]
            backup_set_id = file_record[1]
            file_path = file_record[2]
            file_name = file_record[3]
            directory_path = file_record[4]
            display_name = file_record[5]
            file_type = file_record[6] or "file"
            file_size = file_record[7] or 0
            compressed_size = file_record[8]
            file_permissions = file_record[9]
            file_owner = file_record[10]
            file_group = file_record[11]
            created_time = self._parse_datetime_from_sqlite(file_record[12])
            modified_time = self._parse_datetime_from_sqlite(file_record[13])
            accessed_time = self._parse_datetime_from_sqlite(file_record[14])
            tape_block_start = file_record[15]
            tape_block_count = file_record[16]
            compressed = bool(file_record[17])
            encrypted = bool(file_record[18])
            checksum = file_record[19]
            is_copy_success = bool(file_record[20])
            copy_status_at = self._parse_datetime_from_sqlite(file_record[21])
            backup_time = self._parse_datetime_from_sqlite(file_record[22])
            chunk_number = file_record[23]
            version = file_record[24]
            file_metadata = file_record[25]
            tags = file_record[26]

            files_payload.append(
                {
                    "backup_set_id": backup_set_id,
                    "file_path": file_path,
                    "file_name": file_name,
                    "directory_path": directory_path,
                    "display_name": display_name,
                    "file_type": file_type,
                    "file_size": file_size,
                    "compressed_size": compressed_size,
                    "file_permissions": file_permissions,
                    "file_owner": file_owner,
                    "file_group": file_group,
                    "created_time": created_time,
                    "modified_time": modified_time,
                    "accessed_time": accessed_time,
                    "tape_block_start": tape_block_start,
                    "tape_block_count": tape_block_count,
                    "compressed": compressed,
                    "encrypted": encrypted,
                    "checksum": checksum,
                    "is_copy_success": is_copy_success,
                    "copy_status_at": copy_status_at,
                    "backup_time": backup_time,
                    "chunk_number": chunk_number,
                    "version": version,
                    "file_metadata": file_metadata,
                    "tags": tags,
                }
            )
            synced_file_ids.append(file_id)

        if files_payload:
            # 直接写入 SQLite（调用方负责确保串行执行，例如通过 sqlite_queue_manager）
            inserted_ids = await insert_backup_files_sqlite(files_payload)
            # insert_backup_files_sqlite 返回数据库中新生成的自增ID，但我们需要内存数据库的文件ID
            # 因此仍然返回 synced_file_ids（内存数据库ID），用于标记内存数据库状态
            if not inserted_ids:
                logger.warning("insert_backup_files_sqlite 未返回任何 ID，可能所有文件已存在")

        return synced_file_ids
    
    async def _sync_to_sqlite_via_queue(self, reason: str = "manual"):
        """通过队列同步文件到 SQLite 主库（同步操作，普通优先级）"""
        from backup.sqlite_queue_manager import execute_sqlite_sync
        
        if self._is_syncing:
            logger.warning(
                f"[SQLite同步] 同步已在进行中，跳过本次同步请求 (原因: {reason})。"
                f"如果此状态持续，可能是之前的同步未正确完成。"
            )
            return
        
        logger.info(f"[SQLite同步] 开始同步 (原因: {reason})，设置 _is_syncing = True")
        self._is_syncing = True
        self._sync_start_time = time.time()  # 记录同步开始时间
        sync_start_time = self._sync_start_time
        total_synced_count = 0
        batch_number = 0

        try:
            # 记录同步开始时的待同步文件数
            initial_pending_count = await self._get_pending_sync_count()
            if initial_pending_count > 0:
                logger.info(f"[SQLite同步开始] 待同步文件数: {initial_pending_count} 个 (原因: {reason})")
            else:
                logger.info(f"[SQLite同步开始] 没有待同步文件 (原因: {reason})")
            
            # 循环同步，直到所有未同步的文件都处理完成
            max_batches = 1000  # 防止无限循环
            while batch_number < max_batches:
                # 获取待同步的文件批次（每次获取一批）
                files_to_sync = await self._get_files_to_sync()

                if not files_to_sync:
                    # 没有更多文件需要同步
                    if batch_number == 0:
                        logger.info("内存数据库中没有文件需要同步到SQLite")
                    break

                batch_number += 1
                logger.debug(
                    f"[SQLite批次 {batch_number}] 开始同步 {len(files_to_sync)} 个文件 "
                    f"(原因: {reason}, backup_set_db_id={self.backup_set_db_id})"
                )

                # 准备批量插入数据
                file_data_map = []
                for file_record in files_to_sync:
                    file_data_map.append((file_record, None))  # 第二个参数在 SQLite 模式下不需要

                # 通过队列同步到 SQLite（同步操作，普通优先级）
                # _insert_files_to_sqlite 返回内存数据库中的文件ID列表
                batch_sync_start = time.time()
                try:
                    synced_file_ids = await execute_sqlite_sync(self._insert_files_to_sqlite, file_data_map)
                    batch_sync_time = time.time() - batch_sync_start
                except Exception as batch_error:
                    batch_sync_time = time.time() - batch_sync_start
                    logger.error(
                        f"[SQLite批次 {batch_number}] 同步失败: {str(batch_error)}，"
                        f"耗时: {batch_sync_time:.2f}秒",
                        exc_info=True
                    )
                    # 继续处理下一批，不中断整个同步流程
                    continue

                # 更新同步状态（只标记成功同步的文件）
                if synced_file_ids:
                    await self._mark_files_synced(synced_file_ids)

                # 更新统计
                synced_count = len(synced_file_ids)
                total_synced_count += synced_count
                self._stats['synced_files'] += synced_count
                self._stats['sync_batches'] += 1

                logger.info(
                    f"[SQLite批次 {batch_number}] ✅ 同步完成: {synced_count}/{len(files_to_sync)} 个文件已成功同步，"
                    f"耗时: {batch_sync_time:.2f}秒"
                )

            if batch_number >= max_batches:
                logger.warning(
                    f"[SQLite同步] 达到最大批次限制 ({max_batches})，停止同步。"
                    f"可能还有文件未同步，将在下次同步时继续。"
                )

            # 所有批次同步完成
            if batch_number > 0:
                sync_time = time.time() - sync_start_time
                self._stats['sync_time'] += sync_time
                self._last_sync_time = time.time()
                
                # 检查是否还有未同步的文件
                final_pending_count = await self._get_pending_sync_count()
                
                # 获取累计统计信息
                total_scanned = self._stats['total_files']
                total_synced_accumulated = self._stats['synced_files']
                
                logger.info(
                    f"✅ SQLite同步完成: 共 {batch_number} 个批次，总耗时 {sync_time:.2f}秒，"
                    f"同步开始时待同步: {initial_pending_count} 个，"
                    f"同步完成后剩余: {final_pending_count} 个，"
                    f"本次同步: {total_synced_count} 个，"
                    f"累计总扫描: {total_scanned} 个，"
                    f"累计总同步: {total_synced_accumulated} 个"
                )

        except Exception as e:
            logger.error(f"[SQLite同步] 同步过程异常: {e}", exc_info=True)
        finally:
            logger.info(f"[SQLite同步] 同步结束，设置 _is_syncing = False")
            self._is_syncing = False
            self._sync_start_time = 0  # 重置同步开始时间

    async def _create_checkpoint(self):
        """创建检查点 - 持久化保护"""
        if not self.enable_checkpoint:
            return  # 检查点功能已禁用，直接返回
        
        try:
            # 确保检查点目录存在
            if self.checkpoint_dir:
                self.checkpoint_dir.mkdir(parents=True, exist_ok=True)
            
            # 获取创建检查点时的最大未同步文件ID
            max_unsynced_id = await self._get_max_unsynced_file_id()
            
            # 在项目目录下的 temp/checkpoints 目录中创建检查点文件
            checkpoint_filename = f"tmp{int(time.time() * 1000)}.sql"
            checkpoint_file = str(self.checkpoint_dir / checkpoint_filename)

            # 备份内存数据库
            with open(checkpoint_file, 'w', encoding='utf-8') as f:
                async for line in self.memory_db.iterdump():
                    f.write(f"{line}\n")

            self._last_checkpoint_time = time.time()
            # 记录检查点文件：(文件路径, 创建时间, 最大未同步文件ID)
            self._checkpoint_files.append((checkpoint_file, self._last_checkpoint_time, max_unsynced_id))
            logger.info(f"检查点已创建: {checkpoint_file} (最大未同步文件ID: {max_unsynced_id})")
            
            # 清理过期的检查点文件
            await self._cleanup_old_checkpoints()

        except Exception as e:
            logger.error(f"创建检查点失败: {e}")
    
    async def _get_max_unsynced_file_id(self) -> int:
        """获取当前最大未同步文件ID（仅当前备份集）"""
        try:
            async with self.memory_db.execute("""
                SELECT MAX(id) FROM backup_files WHERE backup_set_id = ? AND synced_to_opengauss = FALSE
            """, (self.backup_set_db_id,)) as cursor:
                result = await cursor.fetchone()
                return result[0] if result and result[0] is not None else 0
        except Exception as e:
            logger.debug(f"获取最大未同步文件ID失败: {e}")
            return 0
    
    async def _cleanup_old_checkpoints_on_startup(self):
        """启动时清理所有过期的检查点文件"""
        if not self.enable_checkpoint:
            return  # 检查点功能已禁用，直接返回
        
        try:
            import os
            current_time = time.time()
            retention_seconds = self.checkpoint_retention_hours * 3600
            
            # 清理检查点目录中的所有过期文件
            if self.checkpoint_dir and self.checkpoint_dir.exists():
                import glob
                pattern = str(self.checkpoint_dir / 'tmp*.sql')
                cleaned_count = 0
                for old_file in glob.glob(pattern):
                    try:
                        file_stat = os.stat(old_file)
                        file_age = current_time - file_stat.st_mtime
                        if file_age > retention_seconds:
                            os.remove(old_file)
                            cleaned_count += 1
                            logger.debug(f"启动时已删除过期检查点文件: {old_file}")
                    except Exception as e:
                        logger.debug(f"清理检查点文件时出错（忽略）: {old_file}, {e}")
                
                if cleaned_count > 0:
                    logger.info(f"启动时已清理 {cleaned_count} 个过期检查点文件")
            else:
                # 如果目录不存在，创建它
                self.checkpoint_dir.mkdir(parents=True, exist_ok=True)
                
        except Exception as e:
            logger.warning(f"启动时清理检查点文件失败: {e}")
    
    async def _cleanup_synced_checkpoints(self):
        """清理已完全同步到openGauss的检查点文件"""
        if not self.enable_checkpoint:
            return  # 检查点功能已禁用，直接返回
        try:
            import os
            # 获取当前已同步的最大文件ID
            async with self.memory_db.execute("""
                SELECT MAX(id) FROM backup_files WHERE synced_to_opengauss = TRUE
            """) as cursor:
                result = await cursor.fetchone()
                max_synced_id = result[0] if result and result[0] is not None else 0
            
            if max_synced_id <= 0:
                return  # 还没有同步任何文件
            
            # 清理所有已完全同步的检查点文件
            # 如果检查点创建时的最大未同步文件ID <= 当前已同步的最大文件ID，说明该检查点的所有数据都已同步
            files_to_remove = []
            for checkpoint_info in self._checkpoint_files[:]:
                if len(checkpoint_info) >= 3:
                    checkpoint_file, create_time, max_unsynced_id = checkpoint_info
                    # 如果检查点创建时的最大未同步文件ID <= 当前已同步的最大文件ID，说明该检查点的所有数据都已同步
                    if max_unsynced_id <= max_synced_id:
                        try:
                            if os.path.exists(checkpoint_file):
                                os.remove(checkpoint_file)
                                logger.info(f"检查点文件已完全同步到openGauss，已删除: {checkpoint_file} (检查点最大未同步ID: {max_unsynced_id}, 当前已同步最大ID: {max_synced_id})")
                            files_to_remove.append(checkpoint_info)
                        except Exception as e:
                            logger.warning(f"删除已同步的检查点文件失败: {checkpoint_file}, 错误: {e}")
                else:
                    # 兼容旧格式：(文件路径, 创建时间)
                    checkpoint_file, create_time = checkpoint_info
                    # 旧格式的检查点文件无法判断是否已完全同步，跳过
                    pass
            
            # 从列表中移除已删除的文件
            for item in files_to_remove:
                if item in self._checkpoint_files:
                    self._checkpoint_files.remove(item)
                    
        except Exception as e:
            logger.warning(f"清理已同步的检查点文件失败: {e}")
    
    async def _cleanup_old_checkpoints(self):
        """清理过期的检查点文件（定期调用）"""
        if not self.enable_checkpoint:
            return  # 检查点功能已禁用，直接返回
        try:
            import os
            current_time = time.time()
            retention_seconds = self.checkpoint_retention_hours * 3600
            
            # 清理记录列表中的过期文件（仅清理未同步的过期文件）
            files_to_remove = []
            for checkpoint_info in self._checkpoint_files[:]:
                if len(checkpoint_info) >= 3:
                    checkpoint_file, create_time, max_unsynced_id = checkpoint_info
                else:
                    # 兼容旧格式：(文件路径, 创建时间)
                    checkpoint_file, create_time = checkpoint_info
                    max_unsynced_id = None
                
                if current_time - create_time > retention_seconds:
                    try:
                        if os.path.exists(checkpoint_file):
                            os.remove(checkpoint_file)
                            logger.debug(f"已删除过期检查点文件: {checkpoint_file}")
                        files_to_remove.append(checkpoint_info)
                    except Exception as e:
                        logger.warning(f"删除检查点文件失败: {checkpoint_file}, 错误: {e}")
            
            # 从列表中移除已删除的文件
            for item in files_to_remove:
                if item in self._checkpoint_files:
                    self._checkpoint_files.remove(item)
            
            # 同时清理检查点目录中可能遗留的过期文件（通过文件名模式匹配）
            try:
                import glob
                if self.checkpoint_dir.exists():
                    pattern = str(self.checkpoint_dir / 'tmp*.sql')
                    for old_file in glob.glob(pattern):
                        try:
                            # 如果文件不在记录列表中，检查是否过期
                            file_in_list = any(old_file == (cf[0] if isinstance(cf, tuple) else cf) for cf in self._checkpoint_files)
                            if not file_in_list:
                                file_stat = os.stat(old_file)
                                file_age = current_time - file_stat.st_mtime
                                if file_age > retention_seconds:
                                    os.remove(old_file)
                                    logger.debug(f"已删除检查点目录中的过期文件: {old_file}")
                        except Exception as e:
                            logger.debug(f"清理检查点文件时出错（忽略）: {old_file}, {e}")
            except Exception as e:
                logger.debug(f"清理检查点目录文件失败（忽略）: {e}")
                
        except Exception as e:
            logger.warning(f"清理过期检查点文件失败: {e}")
    
    async def _cleanup_all_checkpoints(self):
        """清理所有检查点文件（停止时调用）"""
        if not self.enable_checkpoint:
            return  # 检查点功能已禁用，直接返回
        try:
            import os
            for checkpoint_info in self._checkpoint_files[:]:
                # 兼容新旧格式
                if len(checkpoint_info) >= 3:
                    checkpoint_file = checkpoint_info[0]
                else:
                    checkpoint_file = checkpoint_info[0]
                
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

        # 如果检查点任务还在运行，等待它完成（仅在启用检查点时）
        if self.enable_checkpoint and self._checkpoint_task:
            self._checkpoint_task.cancel()
            try:
                await self._checkpoint_task
            except asyncio.CancelledError:
                pass

        # 最后一次同步
        if self.memory_db:
            try:
                await self.force_sync()
                # 创建最终检查点（仅在启用检查点时）
                if self.enable_checkpoint:
                    await self._create_checkpoint()
            except Exception as e:
                logger.error(f"最终同步失败: {e}")
        
        # 清理所有检查点文件（停止时，仅在启用检查点时）
        if self.enable_checkpoint:
            await self._cleanup_all_checkpoints()

        # 关闭数据库连接
        if self.memory_db:
            await self.memory_db.close()

    async def clear_database(self):
        """清空内存数据库中的所有数据（仅当前备份集）"""
        if not self.memory_db:
            logger.warning("内存数据库未初始化，无法清空")
            return
        
        try:
            # 删除当前备份集的所有文件记录
            async with self.memory_db.execute(
                "DELETE FROM backup_files WHERE backup_set_id = ?",
                (self.backup_set_db_id,)
            ) as cursor:
                deleted_count = cursor.rowcount
            
            await self.memory_db.commit()
            
            # 重置统计信息
            self._stats = {
                'total_files': 0,
                'synced_files': 0,
                'sync_batches': 0,
                'total_time': 0,
                'sync_time': 0,
                'memory_usage': 0
            }
            
            logger.info(f"已清空内存数据库（备份集ID: {self.backup_set_db_id}），删除了 {deleted_count} 条记录")
            
        except Exception as e:
            logger.error(f"清空内存数据库失败: {e}", exc_info=True)
            raise

    async def check_database_schema(self):
        """检查内存数据库的字段设置"""
        if not self.memory_db:
            logger.warning("内存数据库未初始化，无法检查")
            return
        
        try:
            # 获取表结构
            async with self.memory_db.execute("PRAGMA table_info(backup_files)") as cursor:
                columns = await cursor.fetchall()
            
            logger.info("========== 内存数据库字段检查 ==========")
            logger.info(f"表名: backup_files")
            logger.info(f"字段数量: {len(columns)}")
            logger.info("字段列表:")
            for col in columns:
                col_id, col_name, col_type, not_null, default_val, pk = col
                logger.info(f"  [{col_id}] {col_name}: {col_type} (NOT NULL: {not_null}, DEFAULT: {default_val}, PK: {pk})")
            
            # 检查关键字段的默认值
            synced_col = next((c for c in columns if c[1] == 'synced_to_opengauss'), None)
            if synced_col:
                logger.info(f"synced_to_opengauss 字段: 类型={synced_col[2]}, 默认值={synced_col[4]}")
            else:
                logger.warning("未找到 synced_to_opengauss 字段！")
            
            # 检查当前数据状态
            async with self.memory_db.execute("""
                SELECT 
                    COUNT(*) as total,
                    COUNT(CASE WHEN synced_to_opengauss = TRUE THEN 1 END) as synced,
                    COUNT(CASE WHEN synced_to_opengauss = FALSE THEN 1 END) as pending,
                    COUNT(CASE WHEN synced_to_opengauss IS NULL THEN 1 END) as null_synced
                FROM backup_files
                WHERE backup_set_id = ?
            """, (self.backup_set_db_id,)) as cursor:
                result = await cursor.fetchone()
                if result:
                    total, synced, pending, null_synced = result
                    logger.info(f"当前数据状态（备份集ID: {self.backup_set_db_id}）:")
                    logger.info(f"  总文件数: {total}")
                    logger.info(f"  已同步: {synced}")
                    logger.info(f"  待同步: {pending}")
                    logger.info(f"  synced_to_opengauss 为 NULL: {null_synced}")
                    if null_synced > 0:
                        logger.warning(f"⚠️ 发现 {null_synced} 个文件的 synced_to_opengauss 字段为 NULL，这可能导致同步问题！")
            
            logger.info("=========================================")
            
        except Exception as e:
            logger.error(f"检查内存数据库字段设置失败: {e}", exc_info=True)
            raise

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