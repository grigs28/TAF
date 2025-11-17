#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
内存数据库写入器 + 异步同步到openGauss
Memory Database Writer + Async Sync to openGauss
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

from utils.scheduler.db_utils import get_opengauss_connection, is_opengauss
from utils.datetime_utils import now, format_datetime

logger = logging.getLogger(__name__)


class MemoryDBWriter:
    """内存数据库写入器 - 极速写入 + 异步同步"""

    def __init__(self, backup_set_db_id: int,
                 sync_batch_size: int = 5000,           # 同步批次大小
                 sync_interval: int = 30,                # 同步间隔(秒)
                 max_memory_files: int = 100000,         # 内存中最大文件数
                 checkpoint_interval: int = 300):        # 检查点间隔(秒)

        self.backup_set_db_id = backup_set_db_id
        self.sync_batch_size = sync_batch_size
        self.sync_interval = sync_interval
        self.max_memory_files = max_memory_files
        self.checkpoint_interval = checkpoint_interval

        # 内存数据库
        self.memory_db = None
        self.db_connection = None

        # 同步相关
        self._is_syncing = False
        self._sync_task = None
        self._checkpoint_task = None
        self._last_sync_time = 0
        self._last_checkpoint_time = 0

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
        """设置内存数据库"""
        # 创建内存数据库连接
        self.db_connection = await aiosqlite.connect(":memory:")
        self.memory_db = self.db_connection

        # 创建表结构（与openGauss兼容）
        await self._create_tables()

        # 启用WAL模式提升性能
        await self.memory_db.execute("PRAGMA journal_mode=WAL")
        await self.memory_db.execute("PRAGMA synchronous=NORMAL")
        await self.memory_db.execute("PRAGMA cache_size=10000")
        await self.memory_db.execute("PRAGMA temp_store=memory")

    async def _create_tables(self):
        """创建内存表结构（与BackupFile模型完全一致）"""
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
        """添加文件到内存数据库（极速写入）"""
        if not self.memory_db:
            await self.initialize()

        try:
            # 准备插入数据
            insert_data = self._prepare_insert_data(file_info)

            # 插入到内存数据库（极快） - 字段顺序与BackupFile模型一致
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

            # 检查是否需要立即同步
            await self._check_sync_need()

        except Exception as e:
            logger.error(f"添加文件到内存数据库失败: {e}, 文件: {file_info.get('path')}")
            raise

    def _prepare_insert_data(self, file_info: Dict) -> tuple:
        """准备插入数据"""
        file_path = file_info.get('path', '')

        # 处理文件大小 - 修复：优先使用size字段（来自file_scanner），其次使用file_stat
        file_size = 0
        if 'size' in file_info:
            file_size = file_info['size'] or 0
        elif 'file_stat' in file_info and file_info['file_stat']:
            file_size = getattr(file_info['file_stat'], 'st_size', 0) or 0

        # 处理文件名
        file_name = file_info.get('name') or Path(file_path).name
        directory_path = str(Path(file_path).parent) if file_path else None

        # 处理文件类型 - 从路径扩展名获取
        file_type = Path(file_path).suffix.lower().lstrip('.')
        file_type = file_type if file_type else 'file'

        # 处理文件权限 - 如果有的话
        file_permissions = file_info.get('permissions')
        if not file_permissions and 'file_stat' in file_info and file_info['file_stat']:
            try:
                file_permissions = oct(file_info['file_stat'].st_mode)[-3:]
            except:
                file_permissions = None

        # 处理时间戳 - 优先使用modified_time，其次从file_stat或当前时间
        modified_time = file_info.get('modified_time')
        if not modified_time and 'file_stat' in file_info and file_info['file_stat']:
            try:
                modified_time = datetime.fromtimestamp(file_info['file_stat'].st_mtime, tz=timezone.utc)
            except:
                modified_time = datetime.now(timezone.utc)
        elif not modified_time:
            modified_time = datetime.now(timezone.utc)

        # 处理创建时间
        created_time = modified_time  # 默认使用修改时间作为创建时间
        if 'file_stat' in file_info and file_info['file_stat']:
            try:
                created_time = datetime.fromtimestamp(file_info['file_stat'].st_ctime, tz=timezone.utc)
            except:
                pass

        # 元数据处理
        metadata = file_info.get('file_metadata') or {}
        metadata.update({'scanned_at': datetime.now().isoformat()})

        current_time = datetime.now(timezone.utc)

        return (
            self.backup_set_db_id,                                   # backup_set_id
            file_path,                                               # file_path
            file_name,                                               # file_name
            directory_path,                                          # directory_path
            None,                                                    # display_name
            file_type,                                               # file_type - 修复：从路径获取
            file_size,                                               # file_size - 修复：使用size字段
            None,                                                    # compressed_size
            file_permissions,                                        # file_permissions
            None,                                                    # file_owner
            None,                                                    # file_group
            created_time,                                            # created_time
            modified_time,                                           # modified_time
            None,                                                    # accessed_time
            None,                                                    # tape_block_start
            None,                                                    # tape_block_count
            False,                                                   # compressed
            False,                                                   # encrypted
            None,                                                    # checksum
            False,                                                   # is_copy_success
            None,                                                    # copy_status_at
            current_time,                                            # backup_time
            None,                                                    # chunk_number
            1,                                                       # version
            json.dumps(metadata),                                    # file_metadata
            None                                                     # tags
        )

    async def _check_sync_need(self):
        """检查是否需要同步"""
        current_time = time.time()

        # 条件1：文件数量达到批次大小
        pending_files = await self._get_pending_sync_count()
        if pending_files >= self.sync_batch_size:
            await self._trigger_sync("batch_size_reached")
            return

        # 条件2：达到同步间隔时间
        if current_time - self._last_sync_time >= self.sync_interval:
            await self._trigger_sync("interval_reached")
            return

        # 条件3：内存中文件过多
        if self._stats['total_files'] >= self.max_memory_files:
            await self._trigger_sync("memory_limit_reached")

    async def _get_pending_sync_count(self) -> int:
        """获取待同步文件数量"""
        async with self.memory_db.execute(
            "SELECT COUNT(*) FROM backup_files WHERE synced_to_opengauss = FALSE"
        ) as cursor:
            result = await cursor.fetchone()
            return result[0] if result else 0

    async def _trigger_sync(self, reason: str):
        """触发同步"""
        if self._is_syncing:
            logger.debug(f"同步已在进行中，跳过触发 (原因: {reason})")
            return

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
        """同步文件到openGauss"""
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
        """获取待同步的文件"""
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

    async def _batch_sync_to_opengauss(self, files: List[Tuple]) -> int:
        """批量同步到openGauss"""
        from utils.scheduler.db_utils import get_opengauss_connection

        if not files:
            return 0

        async with get_opengauss_connection() as conn:
            # 准备批量插入数据
            insert_data = []
            for file_record in files:
                # 转换数据格式
                file_info = self._convert_record_to_file_info(file_record)
                insert_params = self._prepare_opengauss_insert_params(file_info)
                insert_data.append(insert_params)

            # 批量插入到openGauss (兼容openGauss语法，不支持ON CONFLICT)
            for params in insert_data:
                # 先检查记录是否存在
                existing = await conn.fetchval("""
                    SELECT COUNT(*) FROM backup_files
                    WHERE backup_set_id = $1 AND file_path = $2
                """, params[0], params[1])

                if existing > 0:
                    # 记录存在，执行更新 - 26个参数，包含所有字段
                    await conn.execute("""
                        UPDATE backup_files SET
                            file_name = $3,
                            directory_path = $4,
                            display_name = $5,
                            file_type = $6::backupfiletype,
                            file_size = $7,
                            compressed_size = $8,
                            file_permissions = $9,
                            file_owner = $10,
                            file_group = $11,
                            created_time = $12,
                            modified_time = $13,
                            accessed_time = $14,
                            tape_block_start = $15,
                            tape_block_count = $16,
                            compressed = $17,
                            encrypted = $18,
                            checksum = $19,
                            is_copy_success = $20,
                            copy_status_at = $21,
                            backup_time = $22,
                            chunk_number = $23,
                            version = $24,
                            file_metadata = $25::json,
                            tags = $26::json,
                            updated_at = NOW()
                        WHERE backup_set_id = $1 AND file_path = $2
                    """, *params)
                else:
                    # 记录不存在，执行插入 - 26个字段，26个参数，完全匹配
                    await conn.execute("""
                        INSERT INTO backup_files (
                            backup_set_id, file_path, file_name, directory_path, display_name,
                            file_type, file_size, compressed_size, file_permissions, file_owner,
                            file_group, created_time, modified_time, accessed_time, tape_block_start,
                            tape_block_count, compressed, encrypted, checksum, is_copy_success,
                            copy_status_at, backup_time, chunk_number, version, file_metadata, tags
                        ) VALUES (
                            $1, $2, $3, $4, $5, $6::backupfiletype, $7, $8, $9, $10,
                            $11, $12, $13, $14, $15, $16, $17, $18, $19, $20,
                            $21, $22, $23, $24, $25::json, $26::json
                        )
                    """, *params)

            return len(insert_data)

    def _convert_record_to_file_info(self, file_record: Tuple) -> Dict:
        """转换数据库记录为文件信息格式"""
        return {
            'path': file_record[2],           # file_path
            'name': file_record[3],           # file_name
            'directory_path': file_record[4],  # directory_path
            'display_name': file_record[5],   # display_name
            'file_type': file_record[6],      # file_type
            'size': file_record[7],           # file_size - 关键字段，确保正确映射！
            'compressed_size': file_record[8], # compressed_size
            'file_permissions': file_record[9], # file_permissions
            'file_owner': file_record[10],    # file_owner
            'file_group': file_record[11],    # file_group
            'created_time': file_record[12],  # created_time - 保持为datetime对象
            'modified_time': file_record[13], # modified_time - 保持为datetime对象
            'accessed_time': file_record[14], # accessed_time - 保持为datetime对象
            'tape_block_start': file_record[15], # tape_block_start
            'tape_block_count': file_record[16], # tape_block_count
            'compressed': bool(file_record[17]), # compressed - 转换为布尔类型
            'encrypted': bool(file_record[18]), # encrypted - 转换为布尔类型
            'checksum': file_record[19],      # checksum
            'is_copy_success': bool(file_record[20]), # is_copy_success - 转换为布尔类型
            'copy_status_at': file_record[21], # copy_status_at - 保持为datetime对象
            'backup_time': file_record[22],   # backup_time - 保持为datetime对象
            'chunk_number': file_record[23],  # chunk_number
            'version': file_record[24],       # version
            'file_metadata': json.loads(file_record[25]) if file_record[25] else {}, # file_metadata
            'tags': json.loads(file_record[26]) if file_record[26] else None # tags
        }

    def _prepare_opengauss_insert_params(self, file_info: Dict) -> tuple:
        """准备openGauss插入参数"""
        # 确保datetime字段是正确的datetime对象
        created_time = file_info.get('created_time')
        if isinstance(created_time, str):
            try:
                from datetime import datetime
                # 尝试解析字符串为datetime对象
                created_time = datetime.fromisoformat(created_time.replace('Z', '+00:00'))
            except (ValueError, AttributeError):
                created_time = None

        modified_time = file_info.get('modified_time')
        if isinstance(modified_time, str):
            try:
                from datetime import datetime
                modified_time = datetime.fromisoformat(modified_time.replace('Z', '+00:00'))
            except (ValueError, AttributeError):
                modified_time = None

        accessed_time = file_info.get('accessed_time')
        if isinstance(accessed_time, str):
            try:
                from datetime import datetime
                accessed_time = datetime.fromisoformat(accessed_time.replace('Z', '+00:00'))
            except (ValueError, AttributeError):
                accessed_time = None

        backup_time = file_info.get('backup_time')
        if isinstance(backup_time, str):
            try:
                from datetime import datetime
                backup_time = datetime.fromisoformat(backup_time.replace('Z', '+00:00'))
            except (ValueError, AttributeError):
                backup_time = None

        copy_status_at = file_info.get('copy_status_at')
        if isinstance(copy_status_at, str):
            try:
                from datetime import datetime
                copy_status_at = datetime.fromisoformat(copy_status_at.replace('Z', '+00:00'))
            except (ValueError, AttributeError):
                copy_status_at = None

        return (
            self.backup_set_db_id,                    # $1 backup_set_id
            file_info.get('path'),                    # $2 file_path
            file_info.get('name'),                    # $3 file_name
            file_info.get('directory_path'),           # $4 directory_path
            file_info.get('display_name'),             # $5 display_name
            file_info.get('file_type', 'file'),        # $6 file_type
            file_info.get('size', 0),                 # $7 file_size - 关键字段！
            file_info.get('compressed_size'),          # $8 compressed_size
            file_info.get('file_permissions'),         # $9 file_permissions
            file_info.get('file_owner'),               # $10 file_owner
            file_info.get('file_group'),               # $11 file_group
            created_time,                              # $12 created_time - 确保是datetime对象
            modified_time,                             # $13 modified_time - 确保是datetime对象
            accessed_time,                             # $14 accessed_time - 确保是datetime对象
            file_info.get('tape_block_start'),        # $15 tape_block_start
            file_info.get('tape_block_count'),        # $16 tape_block_count
            bool(file_info.get('compressed', False)), # $17 compressed - 必须是布尔类型
            bool(file_info.get('encrypted', False)),  # $18 encrypted - 必须是布尔类型
            file_info.get('checksum'),                # $19 checksum
            bool(file_info.get('is_copy_success', False)), # $20 is_copy_success - 必须是布尔类型
            copy_status_at,                            # $21 copy_status_at - 确保是datetime对象
            backup_time,                               # $22 backup_time - 确保是datetime对象
            file_info.get('chunk_number'),            # $23 chunk_number
            file_info.get('version', 1),              # $24 version
            json.dumps(file_info.get('file_metadata', {})), # $25 file_metadata
            json.dumps(file_info.get('tags', {}))     # $26 tags
        )

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
            with tempfile.NamedTemporaryFile(mode='w', suffix='.sql', delete=False) as f:
                checkpoint_file = f.name

            # 备份内存数据库
            with open(checkpoint_file, 'w') as f:
                for line in self.memory_db.iterdump():
                    f.write(f"{line}\n")

            self._last_checkpoint_time = time.time()
            logger.info(f"检查点已创建: {checkpoint_file}")

            # TODO: 可以将检查点文件上传到安全存储
            # 这里只是演示，实际可以保存到云存储或备份位置

        except Exception as e:
            logger.error(f"创建检查点失败: {e}")

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

            # 关闭数据库连接
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
                COUNT(CASE WHEN synced_to_opengauss = FALSE THEN 1 END) as pending_files
            FROM backup_files
        """) as cursor:
            result = await cursor.fetchone()

            return {
                'total_files': result[0],
                'synced_files': result[1],
                'error_files': result[2],
                'pending_files': result[3],
                'sync_progress': (result[1] / max(1, result[0])) * 100,
                'is_syncing': self._is_syncing,
                'last_sync_time': self._last_sync_time,
                'last_checkpoint_time': self._last_checkpoint_time
            }