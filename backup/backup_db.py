#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
数据库操作模块
Database Operations Module
"""

import asyncio
import logging
import json
import re
from datetime import datetime, timedelta
from pathlib import Path
from typing import List, Dict, Optional

from models.backup import BackupTask, BackupSet, BackupFile, BackupTaskStatus, BackupFileType, BackupSetStatus
from utils.datetime_utils import now, format_datetime

logger = logging.getLogger(__name__)


class BatchDBWriter:
    """批量数据库写入器 - 保持所有原有字段，提升写入性能"""

    def __init__(self, backup_set_db_id: int, batch_size: int = 1000, max_queue_size: int = 5000, timeout: int = 5):
        self.backup_set_db_id = backup_set_db_id
        self.batch_size = batch_size
        self.max_queue_size = max_queue_size
        self.timeout = timeout

        # 使用限制大小的队列，防止内存溢出
        self.file_queue = asyncio.Queue(maxsize=max_queue_size)
        self._batch_buffer = []
        self._is_running = False
        self._worker_task = None
        self._stats = {
            'total_files': 0,
            'batch_count': 0,
            'total_time': 0
        }

    async def start(self):
        """启动批量写入器"""
        if self._is_running:
            return

        self._is_running = True
        self._worker_task = asyncio.create_task(self._batch_worker())
        logger.info(f"批量数据库写入器已启动 (batch_size={self.batch_size}, max_queue={self.max_queue_size})")

    async def add_file(self, file_info: Dict):
        """添加文件到批量写入队列（可阻塞）"""
        if not self._is_running:
            await self.start()

        try:
            # 等待队列有空位，这里会产生背压
            await asyncio.wait_for(self.file_queue.put(file_info), timeout=self.timeout)
            self._stats['total_files'] += 1
        except asyncio.TimeoutError:
            logger.warning(f"批量写入队列已满，丢弃文件: {file_info.get('path', 'unknown')}")
            raise

    async def _batch_worker(self):
        """批量写入worker"""
        from utils.scheduler.db_utils import get_opengauss_connection, is_opengauss
        from utils.datetime_utils import now

        start_time = now()

        try:
            while self._is_running or not self.file_queue.empty():
                batch = []

                # 收集批次数据
                try:
                    # 等待第一个文件
                    first_file = await asyncio.wait_for(
                        self.file_queue.get(), timeout=1.0
                    )
                    batch.append(first_file)
                    self.file_queue.task_done()

                    # 快速收集更多文件（非阻塞）
                    while len(batch) < self.batch_size:
                        try:
                            file_info = self.file_queue.get_nowait()
                            batch.append(file_info)
                            self.file_queue.task_done()
                        except asyncio.QueueEmpty:
                            break

                except asyncio.TimeoutError:
                    # 超时但没有文件，检查是否应该退出
                    if not self._is_running:
                        break
                    continue

                # 处理批次
                if batch:
                    batch_start = now()
                    await self._process_batch(batch)
                    batch_time = (now() - batch_start).total_seconds()
                    self._stats['batch_count'] += 1

                    logger.debug(f"批量处理 {len(batch)} 个文件，耗时 {batch_time:.2f}s")

        except Exception as e:
            logger.error(f"批量写入worker异常: {e}")
            raise
        finally:
            total_time = (now() - start_time).total_seconds()
            self._stats['total_time'] = total_time
            logger.info(f"批量写入器停止，处理了 {self._stats['total_files']} 个文件，"
                       f"完成 {self._stats['batch_count']} 个批次，总耗时 {total_time:.1f}s")

    async def _process_batch(self, file_batch: List[Dict]):
        """处理一个文件批次"""
        from utils.scheduler.db_utils import get_opengauss_connection, is_opengauss
        from utils.datetime_utils import now
        from datetime import timezone

        if not file_batch:
            return

        async with get_opengauss_connection() as conn:
            # 准备数据分类
            insert_data = []
            update_data = []

            # 提取文件路径用于查询
            file_paths = [f.get('path', '') for f in file_batch]

            # 批量查询已存在的文件
            if file_paths:
                existing_files = await conn.fetch(
                    """
                    SELECT id, file_path, is_copy_success
                    FROM backup_files
                    WHERE backup_set_id = $1 AND file_path = ANY($2)
                    """,
                    self.backup_set_db_id, file_paths
                )
                existing_map = {row['file_path']: row for row in existing_files}
            else:
                existing_map = {}

            # 分类处理文件
            for file_info in file_batch:
                file_path = file_info.get('path', '')

                if file_path in existing_map:
                    existing = existing_map[file_path]
                    if existing['is_copy_success']:
                        continue  # 跳过已成功复制的文件

                    # 准备更新参数
                    update_params = self._prepare_update_params(file_info, existing['id'])
                    update_data.append(update_params)
                else:
                    # 准备插入参数
                    insert_params = self._prepare_insert_params(file_info)
                    insert_data.append(insert_params)

            # 执行批量操作
            if insert_data:
                await self._batch_insert(conn, insert_data)

            if update_data:
                await self._batch_update(conn, update_data)

    def _prepare_insert_params(self, file_info: Dict) -> tuple:
        """准备插入参数（保持所有原有字段）"""
        from datetime import timezone

        file_path = file_info.get('path', '')
        file_stat = file_info.get('file_stat')
        file_name = file_info.get('name') or Path(file_path).name
        directory_path = str(Path(file_path).parent) if file_path else None

        # 保持所有原有的元数据处理逻辑
        metadata = file_info.get('file_metadata') or {}
        metadata.update({'scanned_at': datetime.now().isoformat()})

        current_time = datetime.now()
        current_time_tz = current_time.replace(tzinfo=timezone.utc)

        # 完整的18个字段参数（与原upsert_scanned_file_record保持一致）
        return (
            self.backup_set_db_id,                                      # $1 backup_set_id
            file_path,                                                  # $2 file_path
            file_name,                                                  # $3 file_name
            'file',                                                     # $4 file_type (as enum)
            file_stat.st_size if file_stat else 0,                      # $5 file_size
            None,                                                       # $6 compressed_size
            oct(file_stat.st_mode)[-3:] if file_stat else None,         # $7 file_permissions
            datetime.fromtimestamp(file_stat.st_ctime, tz=timezone.utc) if file_stat else None,  # $8 created_time
            datetime.fromtimestamp(file_stat.st_mtime, tz=timezone.utc) if file_stat else None,  # $9 modified_time
            datetime.fromtimestamp(file_stat.st_atime, tz=timezone.utc) if file_stat else None,  # $10 accessed_time
            False,                                                      # $11 compressed
            None,                                                       # $12 checksum
            current_time_tz,                                           # $13 backup_time
            None,                                                       # $14 chunk_number
            None,                                                       # $15 tape_block_start
            json.dumps(metadata),                                      # $16 file_metadata (JSON)
            False,                                                      # $17 is_copy_success
            None                                                        # $18 copy_status_at
        )

    def _prepare_update_params(self, file_info: Dict, existing_id: int) -> tuple:
        """准备更新参数（保持所有原有字段）"""
        # 获取插入参数，但替换ID用于WHERE条件
        insert_params = self._prepare_insert_params(file_info)
        return (existing_id, *insert_params[1:])  # id + 其他17个字段

    async def _batch_insert(self, conn, insert_data: List[tuple]):
        """批量插入文件记录"""
        await conn.executemany(
            """
            INSERT INTO backup_files (
                backup_set_id, file_path, file_name, file_type, file_size,
                compressed_size, file_permissions, created_time, modified_time,
                accessed_time, compressed, checksum, backup_time, chunk_number,
                tape_block_start, file_metadata, is_copy_success, copy_status_at
            ) VALUES (
                $1, $2, $3, $4::backupfiletype, $5, $6, $7, $8, $9,
                $10, $11, $12, $13, $14, $15, $16::json, $17, $18
            )
            """,
            insert_data
        )
        logger.debug(f"批量插入 {len(insert_data)} 个文件记录")

    async def _batch_update(self, conn, update_data: List[tuple]):
        """批量更新文件记录"""
        await conn.executemany(
            """
            UPDATE backup_files
            SET file_name = $2, directory_path = $3, file_type = $4::backupfiletype,
                file_size = $5, compressed_size = $6, file_permissions = $7,
                created_time = $8, modified_time = $9, accessed_time = $10,
                compressed = $11, checksum = $12, backup_time = $13, chunk_number = $14,
                tape_block_start = $15, file_metadata = $16::json, copy_status_at = $17,
                updated_at = $18
            WHERE id = $1
            """,
            update_data
        )
        logger.debug(f"批量更新 {len(update_data)} 个文件记录")

    async def stop(self):
        """停止批量写入器"""
        self._is_running = False

        if self._worker_task:
            # 等待worker完成
            await self._worker_task
            self._worker_task = None

        logger.info("批量数据库写入器已停止")

    async def flush(self):
        """强制刷新剩余文件"""
        # 等待队列清空
        while not self.file_queue.empty():
            await asyncio.sleep(0.1)

        # 等待最后一个批次完成
        if self._worker_task:
            await asyncio.sleep(0.1)

    def get_stats(self) -> Dict:
        """获取统计信息"""
        return self._stats.copy()


def _parse_enum(enum_cls, value, default=None):
    if value is None:
        return default
    try:
        if isinstance(value, enum_cls):
            return value
        return enum_cls(value) if isinstance(value, str) else enum_cls(value.value)  # type: ignore[arg-type]
    except Exception:
        return default


class BackupDB:
    """备份数据库操作类"""
    
    def __init__(self):
        """初始化数据库操作类"""
        self._last_operation_status: Dict[int, str] = {}

    def _log_operation_stage_event(self, backup_task: Optional[BackupTask], operation_status: str):
        """记录关键阶段日志（即使全局日志级别较高也能看到）"""
        try:
            if not backup_task or not getattr(backup_task, 'id', None):
                return

            normalized = (operation_status or '').strip()
            if not normalized:
                return

            task_id = backup_task.id
            previous = self._last_operation_status.get(task_id)
            if previous == normalized:
                return

            self._last_operation_status[task_id] = normalized
            task_name = getattr(backup_task, 'task_name', '') or ''
            stage_label = normalized.strip('[]') or normalized
            logger.warning(f"[关键阶段] 任务 {task_name or task_id}: {stage_label}")

            if any(keyword in stage_label for keyword in ("完成", "成功", "失败", "终止", "结束")):
                self._last_operation_status.pop(task_id, None)
        except Exception:
            logger.debug("记录关键阶段日志失败", exc_info=True)
    
    async def create_backup_set(self, backup_task: BackupTask, tape) -> BackupSet:
        """创建备份集
        
        Args:
            backup_task: 备份任务对象
            tape: 磁带对象
            
        Returns:
            BackupSet: 备份集对象
        """
        try:
            # 生成备份集ID
            backup_group = format_datetime(now(), '%Y-%m')
            set_id = f"{backup_group}_{backup_task.id:06d}"
            backup_time = datetime.now()
            retention_until = backup_time + timedelta(days=backup_task.retention_days)
            
            # 使用原生 openGauss SQL，避免 SQLAlchemy 版本解析
            from utils.scheduler.db_utils import is_opengauss, get_opengauss_connection
            
            if is_opengauss():
                # 使用连接池
                async with get_opengauss_connection() as conn:
                    # 准备 source_info JSON
                    source_info_json = json.dumps({'paths': backup_task.source_paths}) if backup_task.source_paths else None
                    
                    # 插入备份集
                    await conn.execute(
                        """
                        INSERT INTO backup_sets 
                        (set_id, set_name, backup_group, status, backup_task_id, tape_id,
                         backup_type, backup_time, source_info, retention_until, auto_delete,
                         created_at, updated_at)
                        VALUES ($1, $2, $3, $4::backupsetstatus, $5, $6, $7::backuptasktype, $8, $9::json, $10, $11, $12, $13)
                        RETURNING id
                        """,
                        set_id,
                        f"{backup_task.task_name}_{set_id}",
                        backup_group,
                        'ACTIVE',  # BackupSetStatus.ACTIVE
                        backup_task.id,
                        tape.tape_id,
                        backup_task.task_type.value,  # BackupTaskType enum value
                        backup_time,
                        source_info_json,
                        retention_until,
                        True,  # auto_delete
                        backup_time,  # created_at
                        backup_time   # updated_at
                    )
                    
                    # 查询插入的记录
                    row = await conn.fetchrow(
                        """
                        SELECT id, set_id, set_name, backup_group, status, backup_task_id, tape_id,
                               backup_type, backup_time, source_info, retention_until, created_at, updated_at
                        FROM backup_sets
                        WHERE set_id = $1
                        """,
                        set_id
                    )
                    
                    if row:
                        # 创建 BackupSet 对象（用于返回）
                        backup_set = BackupSet(
                            id=row['id'],
                            set_id=row['set_id'],
                            set_name=row['set_name'],
                            backup_group=row['backup_group'],
                            status=row['status'],
                            backup_task_id=row['backup_task_id'],
                            tape_id=row['tape_id'],
                            backup_type=backup_task.task_type,
                            backup_time=row['backup_time'],
                            source_info={'paths': backup_task.source_paths},
                            retention_until=row['retention_until']
                        )
                    else:
                        raise RuntimeError(f"备份集插入后查询失败: {set_id}")
            else:
                # 非 openGauss 使用 SQLAlchemy（其他数据库）- 但当前项目只支持 openGauss
                raise RuntimeError("当前项目仅支持 openGauss 数据库")

            backup_task.backup_set_id = set_id
            logger.info(f"创建备份集: {set_id}")

            return backup_set

        except Exception as e:
            logger.error(f"创建备份集失败: {str(e)}")
            raise
    
    async def finalize_backup_set(self, backup_set: BackupSet, file_count: int, total_size: int):
        """完成备份集
        
        Args:
            backup_set: 备份集对象
            file_count: 文件数量
            total_size: 总大小
        """
        try:
            backup_set.total_files = file_count
            backup_set.total_bytes = total_size
            backup_set.compressed_bytes = total_size
            backup_set.compression_ratio = total_size / backup_set.total_bytes if backup_set.total_bytes > 0 else 1.0
            backup_set.chunk_count = 1  # 简化处理

            # 保存更新 - 使用原生 openGauss SQL
            from utils.scheduler.db_utils import is_opengauss, get_opengauss_connection
            
            if is_opengauss():
                # 使用连接池
                async with get_opengauss_connection() as conn:
                    await conn.execute(
                        """
                        UPDATE backup_sets
                        SET total_files = $1,
                            total_bytes = $2,
                            compressed_bytes = $3,
                            compression_ratio = $4,
                            chunk_count = $5,
                            updated_at = $6
                        WHERE set_id = $7
                        """,
                        file_count,
                        total_size,
                        total_size,
                        backup_set.compression_ratio,
                        backup_set.chunk_count,
                        datetime.now(),
                        backup_set.set_id
                    )
            else:
                # 非 openGauss 使用 SQLAlchemy
                from config.database import get_db
                async for db in get_db():
                    await db.commit()

            logger.info(f"备份集完成: {backup_set.set_id}")

        except Exception as e:
            logger.error(f"完成备份集失败: {str(e)}")
    
    async def save_backup_files_to_db(
        self, 
        file_group: List[Dict], 
        backup_set: BackupSet, 
        compressed_file: Dict, 
        tape_file_path: str, 
        chunk_number: int
    ):
        """保存备份文件信息到数据库（便于恢复）
        
        注意：如果数据库保存失败，不会中断备份流程，只会记录警告日志。
        
        Args:
            file_group: 文件组列表
            backup_set: 备份集对象
            compressed_file: 压缩文件信息字典
            tape_file_path: 磁带文件路径
            chunk_number: 块编号
        """
        try:
            from utils.scheduler.db_utils import is_opengauss, get_opengauss_connection
            
            if not is_opengauss():
                logger.warning("非openGauss数据库，跳过保存备份文件信息")
                return
            
            # 使用连接池
            async with get_opengauss_connection() as conn:
                try:
                    # 获取备份集的数据库ID
                    backup_set_row = await conn.fetchrow(
                        """
                        SELECT id FROM backup_sets WHERE set_id = $1
                        """,
                        backup_set.set_id
                    )
                    
                    if not backup_set_row:
                        logger.warning(f"找不到备份集: {backup_set.set_id}，跳过保存文件信息")
                        return
                    
                    backup_set_db_id = backup_set_row['id']
                    backup_time = datetime.now()
                    
                    # 在线程池中批量处理文件信息，避免阻塞事件循环
                    loop = asyncio.get_event_loop()
                    
                    def _process_file_info(file_info):
                        """在线程中处理单个文件信息"""
                        try:
                            file_path = Path(file_info['path'])
                            
                            # 确定文件类型（使用枚举值）
                            if file_info.get('is_dir', False):
                                file_type = BackupFileType.DIRECTORY.value
                            elif file_info.get('is_symlink', False):
                                if hasattr(BackupFileType, 'SYMLINK'):
                                    file_type = BackupFileType.SYMLINK.value
                                else:
                                    file_type = BackupFileType.FILE.value
                            else:
                                file_type = BackupFileType.FILE.value
                            
                            # 获取文件元数据（同步操作，在线程中执行）
                            try:
                                file_stat = file_path.stat() if file_path.exists() else None
                            except (PermissionError, OSError, FileNotFoundError, IOError) as stat_error:
                                logger.debug(f"无法获取文件统计信息: {file_path} (错误: {str(stat_error)})")
                                file_stat = None
                            except Exception as stat_error:
                                logger.warning(f"获取文件统计信息失败: {file_path} (错误: {str(stat_error)})")
                                file_stat = None
                            
                            # 跳过单个文件的校验和计算（文件已压缩，压缩包本身有校验和，避免阻塞）
                            file_checksum = None
                            
                            # 获取文件权限（Windows上可能不可用）
                            file_permissions = None
                            if file_stat:
                                try:
                                    file_permissions = oct(file_stat.st_mode)[-3:]
                                except:
                                    pass
                            
                            return {
                                'file_path': str(file_path),
                                'file_name': file_path.name,
                                'file_type': file_type,
                                'file_size': file_info.get('size', 0),
                                'file_stat': file_stat,
                                'file_permissions': file_permissions,
                                'file_checksum': file_checksum
                            }
                        except Exception as process_error:
                            logger.warning(f"处理文件信息失败: {file_info.get('path', 'unknown')} (错误: {str(process_error)})")
                            return None
                    
                    # 批量处理文件信息（在线程池中执行）
                    processed_files = await asyncio.gather(*[
                        loop.run_in_executor(None, _process_file_info, file_info)
                        for file_info in file_group
                    ], return_exceptions=True)
                    
                    # 过滤掉处理失败的文件（返回None或异常）
                    valid_processed_files = [f for f in processed_files if f is not None and not isinstance(f, Exception)]
                    
                    if len(valid_processed_files) < len(file_group):
                        failed_count = len(file_group) - len(valid_processed_files)
                        logger.warning(f"⚠️ 处理文件信息时，{failed_count} 个文件失败，继续保存其他文件")
                    
                    # 批量插入文件记录（使用事务）
                    success_count = 0
                    failed_count = 0
                    await self._mark_files_as_copied(
                        conn=conn,
                        backup_set_db_id=backup_set_db_id,
                        processed_files=valid_processed_files,
                        compressed_file=compressed_file,
                        tape_file_path=tape_file_path,
                        chunk_number=chunk_number,
                        backup_time=backup_time
                    )
                        
                except Exception as db_conn_error:
                    logger.warning(f"⚠️ 数据库连接或查询失败，跳过保存文件信息: {str(db_conn_error)}")
                    # 数据库错误不影响备份流程，继续执行
                    
        except Exception as e:
            logger.warning(f"⚠️ 保存备份文件信息到数据库失败: {str(e)}，但备份流程继续")
            import traceback
            logger.debug(f"错误堆栈:\n{traceback.format_exc()}")
            # 不抛出异常，因为文件已经写入磁带，数据库记录失败不应该影响备份流程

    async def mark_files_as_copied(
        self,
        backup_set: BackupSet,
        file_group: List[Dict],
        compressed_file: Dict,
        tape_file_path: str,
        chunk_number: int
    ):
        """标记文件为复制成功"""
        from utils.scheduler.db_utils import is_opengauss, get_opengauss_connection
        if not is_opengauss():
            return

        backup_set_db_id = getattr(backup_set, 'id', None)
        if not backup_set_db_id:
            async with get_opengauss_connection() as conn:
                row = await conn.fetchrow(
                    "SELECT id FROM backup_sets WHERE set_id = $1",
                    backup_set.set_id
                )
            if not row:
                logger.warning(f"找不到备份集: {backup_set.set_id}，无法标记文件成功")
                return
            backup_set_db_id = row['id']

        async with get_opengauss_connection() as conn:
            await self._mark_files_as_copied(
                conn=conn,
                backup_set_db_id=backup_set_db_id,
                processed_files=file_group,
                compressed_file=compressed_file,
                tape_file_path=tape_file_path,
                chunk_number=chunk_number,
                backup_time=datetime.now()
            )

    async def get_backup_set_by_set_id(self, set_id: str) -> Optional[BackupSet]:
        """根据 set_id 获取备份集"""
        from utils.scheduler.db_utils import is_opengauss, get_opengauss_connection
        if not set_id:
            return None
        
        if is_opengauss():
            async with get_opengauss_connection() as conn:
                row = await conn.fetchrow(
                    """
                    SELECT id, set_id, set_name, backup_group, status, backup_task_id,
                           tape_id, backup_type, backup_time, total_files, total_bytes,
                           compressed_bytes, compression_ratio, chunk_count, created_at,
                           updated_at
                    FROM backup_sets
                    WHERE set_id = $1
                    """,
                    set_id
                )
        else:
            async with db_manager.AsyncSessionLocal() as session:
                stmt = select(BackupSet).where(BackupSet.set_id == set_id)
                result = await session.execute(stmt)
                backup_set = result.scalar_one_or_none()
                return backup_set
        
        if not row:
            return None
        
        backup_set_obj = BackupSet(
            set_id=row['set_id'],
            set_name=row['set_name'],
            backup_group=row['backup_group'],
            backup_type=_parse_enum(BackupTaskType, row.get('backup_type'), BackupTaskType.FULL),
            backup_time=row['backup_time'],
            total_files=row['total_files'],
            total_bytes=row['total_bytes'],
            compressed_bytes=row['compressed_bytes'],
            compression_ratio=row['compression_ratio'],
            chunk_count=row['chunk_count']
        )
        backup_set_obj.id = row['id']
        backup_set_obj.status = _parse_enum(BackupSetStatus, row.get('status'), BackupSetStatus.ACTIVE)
        backup_set_obj.backup_task_id = row['backup_task_id']
        backup_set_obj.tape_id = row['tape_id']
        backup_set_obj.created_at = row['created_at']
        backup_set_obj.updated_at = row['updated_at']
        return backup_set_obj

    async def clear_backup_files_for_set(self, backup_set_db_id: int):
        """清理指定备份集的文件记录"""
        from utils.scheduler.db_utils import is_opengauss, get_opengauss_connection
        if not is_opengauss():
            return
        async with get_opengauss_connection() as conn:
            await conn.execute(
                "DELETE FROM backup_files WHERE backup_set_id = $1",
                backup_set_db_id
            )

    async def upsert_scanned_file_record(self, backup_set_db_id: int, file_info: Dict):
        """保存扫描阶段的文件/目录信息"""
        from utils.scheduler.db_utils import is_opengauss, get_opengauss_connection
        if not is_opengauss():
            return
        
        file_path = file_info.get('path') or file_info.get('file_path')
        if not file_path:
            return

        directory_path = str(Path(file_path).parent) if Path(file_path).parent else None
        display_name = file_info.get('name') or file_info.get('file_name') or Path(file_path).name
        file_size = file_info.get('size') or file_info.get('file_size') or 0
        file_permissions = file_info.get('permissions') or file_info.get('file_permissions')
        modified_time = file_info.get('modified_time')
        accessed_time = file_info.get('accessed_time')
        created_time = file_info.get('created_time') or modified_time

        if isinstance(modified_time, str):
            modified_time = datetime.fromisoformat(modified_time)
        if isinstance(accessed_time, str):
            accessed_time = datetime.fromisoformat(accessed_time)
        if isinstance(created_time, str):
            created_time = datetime.fromisoformat(created_time)

        if file_info.get('is_dir'):
            file_type = BackupFileType.DIRECTORY.value
        elif file_info.get('is_symlink'):
            file_type = BackupFileType.SYMLINK.value if hasattr(BackupFileType, 'SYMLINK') else BackupFileType.FILE.value
        else:
            file_type = BackupFileType.FILE.value

        async with get_opengauss_connection() as conn:
            existing = await conn.fetchrow(
                """
                SELECT id, is_copy_success 
                FROM backup_files 
                WHERE backup_set_id = $1 AND file_path = $2
                """,
                backup_set_db_id,
                file_path
            )

            metadata = file_info.get('file_metadata') or {}
            metadata.update({'scanned_at': datetime.now().isoformat()})

            if existing and existing['is_copy_success']:
                # 已复制成功的文件不覆盖
                return

            if existing:
                await conn.execute(
                    """
                    UPDATE backup_files
                    SET file_name = $3,
                        directory_path = $4,
                        display_name = $5,
                        file_type = $6::backupfiletype,
                        file_size = $7,
                        file_permissions = $8,
                        created_time = $9,
                        modified_time = $10,
                        accessed_time = $11,
                        file_metadata = $12::json
                    WHERE id = $13
                    """,
                    backup_set_db_id,
                    file_path,
                    display_name,
                    directory_path,
                    display_name,
                    file_type,
                    file_size,
                    file_permissions,
                    created_time,
                    modified_time,
                    accessed_time,
                    json.dumps(metadata),
                    existing['id']
                )
            else:
                await conn.execute(
                    """
                    INSERT INTO backup_files (
                        backup_set_id, file_path, file_name, directory_path, display_name,
                        file_type, file_size, compressed_size, file_permissions,
                        created_time, modified_time, accessed_time, compressed,
                        checksum, backup_time, chunk_number, tape_block_start,
                        file_metadata, is_copy_success, copy_status_at
                    ) VALUES (
                        $1, $2, $3, $4, $5,
                        $6::backupfiletype, $7, $8, $9,
                        $10, $11, $12, $13,
                        $14, $15, $16, $17,
                        $18::json, FALSE, NULL
                    )
                    """,
                    backup_set_db_id,
                    file_path,
                    display_name,
                    directory_path,
                    display_name,
                    file_type,
                    file_size,
                    0,
                    file_permissions,
                    created_time,
                    modified_time,
                    accessed_time,
                    False,
                    None,
                    datetime.now(),
                    0,
                    0,
                    json.dumps(metadata)
                )

    async def fetch_pending_backup_files(self, backup_set_db_id: int, limit: int = 500) -> List[Dict]:
        """获取待复制的文件列表（保留以兼容旧代码，但不再使用）"""
        from utils.scheduler.db_utils import is_opengauss, get_opengauss_connection
        if not is_opengauss():
            return []
        
        async with get_opengauss_connection() as conn:
            rows = await conn.fetch(
                """
                SELECT id, file_path, file_name, directory_path, display_name, file_type,
                       file_size, file_permissions, modified_time, accessed_time
                FROM backup_files
                WHERE backup_set_id = $1
                  AND (is_copy_success = FALSE OR is_copy_success IS NULL)
                  AND file_type = 'file'::backupfiletype
                ORDER BY id
                LIMIT $2
                """,
                backup_set_db_id,
                limit
            )
        
        pending_files = []
        for row in rows:
            file_type = row['file_type']
            pending_files.append({
                'id': row['id'],
                'path': row['file_path'],
                'file_path': row['file_path'],
                'name': row['file_name'],
                'file_name': row['file_name'],
                'directory_path': row['directory_path'],
                'display_name': row['display_name'],
                'size': row['file_size'] or 0,
                'permissions': row['file_permissions'],
                'modified_time': row['modified_time'],
                'accessed_time': row['accessed_time'],
                'is_dir': str(file_type).lower() == 'directory',
                'is_file': str(file_type).lower() == 'file',
                'is_symlink': str(file_type).lower() == 'symlink'
            })
        return pending_files
    
    async def fetch_pending_files_grouped_by_size(
        self,
        backup_set_db_id: int,
        max_file_size: int,
        backup_task_id: int = None,
        should_wait_if_small: bool = True
    ) -> List[List[Dict]]:
        """
        新策略：从数据库检索所有未压缩文件，构建压缩组

        策略说明：
        1. 每次检索所有 is_copy_success = FALSE 的文件
        2. 累积文件直到达到 max_file_size 阈值
        3. 超过阈值的文件跳过（保持 FALSE 状态，下次仍可检索）
        4. 如果一轮达不到阈值，最多重试6次后强制压缩
        5. 超大文件（超过容差上限）单独成组

        Args:
            backup_set_db_id: 备份集数据库ID
            max_file_size: 单个文件组的最大大小（字节）
            backup_task_id: 夀份任务ID，用于获取重试计数
            should_wait_if_small: 是否在组大小不足时等待

        Returns:
            List[List[Dict]]: 包含一个文件组的列表，空列表表示等待或无文件
        """
        from utils.scheduler.db_utils import is_opengauss, get_opengauss_connection
        from backup.utils import format_bytes

        if not is_opengauss():
            return []

        # 获取重试计数（如果没有backup_task_id则使用0）
        retry_count = 0
        max_retries = 6
        if backup_task_id:
            # 从备份引擎获取重试计数（需要通过外部传递或数据库存储）
            # 这里简化处理，通过should_wait_if_small判断是否应该继续等待
            retry_count = 0 if should_wait_if_small else max_retries

        # 从数据库检索所有未压缩的文件
        async with get_opengauss_connection() as conn:
            rows = await conn.fetch(
                """
                SELECT id, file_path, file_name, directory_path, display_name, file_type,
                       file_size, file_permissions, modified_time, accessed_time
                FROM backup_files
                WHERE backup_set_id = $1
                  AND (is_copy_success = FALSE OR is_copy_success IS NULL)
                  AND file_type = 'file'::backupfiletype
                ORDER BY id
                """,
                backup_set_db_id
            )
        
        if not rows:
            return []

        # 转换为文件信息字典
        all_files = []
        for row in rows:
            file_type = row['file_type']
            all_files.append({
                'id': row['id'],
                'path': row['file_path'],
                'file_path': row['file_path'],
                'name': row['file_name'],
                'file_name': row['file_name'],
                'directory_path': row['directory_path'],
                'display_name': row['display_name'],
                'size': row['file_size'] or 0,
                'permissions': row['file_permissions'],
                'modified_time': row['modified_time'],
                'accessed_time': row['accessed_time'],
                'is_dir': str(file_type).lower() == 'directory',
                'is_file': str(file_type).lower() == 'file',
                'is_symlink': str(file_type).lower() == 'symlink'
            })

        # 新策略参数：使用容差范围
        tolerance = max_file_size * 0.05  # 5% 容差
        min_group_size = max_file_size - tolerance  # 最小目标大小
        max_group_size = max_file_size + tolerance  # 超大文件阈值（含容差）

        current_group = []
        current_group_size = 0
        skipped_files = []  # 超过容差上限的文件，保持FALSE状态

        logger.info(
            f"[新策略] 检索到 {len(all_files)} 个未压缩文件，"
            f"目标范围：{format_bytes(min_group_size)} - {format_bytes(max_file_size)} "
            f"(含容差上限：{format_bytes(max_group_size)})，"
            f"重试次数：{retry_count}/{max_retries}"
        )

        # 按顺序处理所有文件
        for file_info in all_files:
            file_size = file_info['size']

            # 处理超大文件：超过容差上限，单独成组
            if file_size > max_group_size:
                # 如果当前组已有文件，先返回当前组
                if current_group:
                    logger.info(
                        f"[新策略] 返回当前组：{len(current_group)} 个文件，"
                        f"总大小 {format_bytes(current_group_size)}，"
                        f"发现超大文件将单独处理"
                    )
                    return [current_group]

                # 超大文件单独成组（特殊文件处理方式）
                logger.warning(
                    f"[新策略] 发现超大文件单独成组：{format_bytes(file_size)} "
                    f"(超过最大大小 {format_bytes(max_file_size)} 含容差)"
                )
                return [[file_info]]

            # 检查加入当前组是否会超过最大大小（不含容差）
            new_group_size = current_group_size + file_size

            if new_group_size > max_file_size:
                # 超过最大大小，跳过此文件（保持FALSE状态）
                skipped_files.append(file_info)
                logger.debug(
                    f"[新策略] 跳过文件（超过最大大小）：{file_info['name']} "
                    f"({format_bytes(file_size)})，当前组：{format_bytes(current_group_size)}"
                )
                continue

            # 加入当前组
            current_group.append(file_info)
            current_group_size = new_group_size

        # 处理最终的文件组
        if not current_group:
            # 没有文件加入当前组
            if skipped_files:
                logger.warning(
                    f"[新策略] 所有文件都超过最大大小，跳过了 {len(skipped_files)} 个文件"
                )
            else:
                logger.info("[新策略] 没有待压缩文件")
            return []

        # 检查当前组大小是否在容差范围内
        size_ratio = current_group_size / max_file_size if max_file_size > 0 else 0
        scan_status = await self.get_scan_status(backup_task_id) if backup_task_id else None

        if current_group_size < min_group_size and scan_status != 'completed' and retry_count < max_retries:
            # 组大小低于容差下限且扫描未完成，继续等待
            logger.warning(
                f"[新策略] 文件组大小低于容差下限：{format_bytes(current_group_size)} "
                f"(需要 ≥ {format_bytes(min_group_size)} = {size_ratio*100:.1f}% of 目标)，"
                f"扫描状态：{scan_status}，等待更多文件...（重试 {retry_count}/{max_retries}）"
            )
            return []

        # 压缩条件：
        # 1. 达到或超过最小目标大小（在容差范围内）
        # 2. 扫描已完成（即使未达到最小大小）
        # 3. 达到重试上限（强制压缩）
        if current_group_size >= min_group_size:
            logger.info(
                f"[新策略] 达到容差范围内：{format_bytes(current_group_size)} "
                f"({size_ratio*100:.1f}% of 目标，≥ {format_bytes(min_group_size)})，"
                f"跳过了 {len(skipped_files)} 个文件"
            )
        else:
            # 强制压缩情况
            reason = '扫描已完成' if scan_status == 'completed' else '达到重试上限'
            logger.warning(
                f"[新策略] 强制压缩：文件组大小 {format_bytes(current_group_size)} "
                f"({size_ratio*100:.1f}% of 目标，< {format_bytes(min_group_size)})，"
                f"原因：{reason}"
            )

        # 收集所有文件分组
        all_groups = []
        remaining_files = sorted_files.copy()

        # 继续分组直到没有剩余文件
        while remaining_files:
            # 取最大的文件作为新组的基准
            max_file = remaining_files[0]
            max_file_size = max_file['size']

            tolerance = max_file_size * 0.05  # 5% tolerance
            min_group_size = max_file_size - tolerance
            max_group_size = max_file_size + tolerance

            # 创建新组
            current_group = []
            files_to_remove = []

            # 将符合条件的文件加入当前组
            for file in remaining_files:
                file_size = file['size']
                if min_group_size <= file_size <= max_group_size:
                    current_group.append(file)
                    files_to_remove.append(file)

            # 从剩余文件列表中移除已分组的文件
            for file in files_to_remove:
                remaining_files.remove(file)

            # 添加到结果列表
            all_groups.append(current_group)

        return all_groups

    async def get_scan_status(self, backup_task_id: int) -> Optional[str]:
        """获取扫描状态"""
        from utils.scheduler.db_utils import is_opengauss, get_opengauss_connection
        if not is_opengauss():
            return None
        async with get_opengauss_connection() as conn:
            row = await conn.fetchrow(
                "SELECT scan_status FROM backup_tasks WHERE id = $1",
                backup_task_id
            )
        return row['scan_status'] if row else None

    async def update_scan_status(self, backup_task_id: int, status: str):
        """更新扫描状态"""
        from utils.scheduler.db_utils import is_opengauss, get_opengauss_connection
        if not is_opengauss():
            return
        current_time = datetime.now()
        async with get_opengauss_connection() as conn:
            if status == 'completed':
                await conn.execute(
                    """
                    UPDATE backup_tasks
                    SET scan_status = $1,
                        scan_completed_at = $2,
                        updated_at = $2
                    WHERE id = $3
                    """,
                    status,
                    current_time,
                    backup_task_id
                )
            else:
                await conn.execute(
                    """
                    UPDATE backup_tasks
                    SET scan_status = $1,
                        updated_at = $2
                    WHERE id = $3
                    """,
                    status,
                    current_time,
                    backup_task_id
                )

    async def _mark_files_as_copied(
        self,
        conn,
        backup_set_db_id: int,
        processed_files: List[Dict],
        compressed_file: Dict,
        tape_file_path: str,
        chunk_number: int,
        backup_time: datetime
    ):
        """Mark files as copied in the database"""
        success_count = 0
        failed_count = 0
        per_file_compressed_size = compressed_file.get('compressed_size', 0)
        per_file_compressed_size = per_file_compressed_size // len(processed_files) if processed_files else 0
        is_compressed = compressed_file.get('compression_enabled', True)
        checksum = compressed_file.get('checksum')
        copy_time = datetime.now()

        for processed_file in processed_files:
            try:
                file_path = processed_file.get('file_path')
                file_stat = processed_file.get('file_stat')
                metadata = processed_file.get('file_metadata') or {}
                metadata.update({
                    'tape_file_path': tape_file_path,
                    'chunk_number': chunk_number,
                    'original_path': file_path
                })

                result = await conn.execute(
                    """
                    UPDATE backup_files
                    SET compressed_size = $3,
                        compressed = $4,
                        checksum = $5,
                        backup_time = $6,
                        chunk_number = $7,
                        tape_block_start = $8,
                        file_metadata = $9::json,
                        is_copy_success = TRUE,
                        copy_status_at = $10
                    WHERE backup_set_id = $1 AND file_path = $2
                    """,
                    backup_set_db_id,
                    file_path,
                    per_file_compressed_size,
                    is_compressed,
                    checksum,
                    backup_time,
                    chunk_number,
                    0,
                    json.dumps(metadata),
                    copy_time
                )

                if result and result.startswith('UPDATE 0'):
                    # UPDATE 0 表示没有匹配的记录，需要插入新记录
                    try:
                        await conn.execute(
                            """
                            INSERT INTO backup_files (
                                backup_set_id, file_path, file_name, file_type, file_size,
                                compressed_size, file_permissions, created_time, modified_time,
                                accessed_time, compressed, checksum, backup_time, chunk_number,
                                tape_block_start, file_metadata, is_copy_success, copy_status_at
                            ) VALUES (
                                $1, $2, $3, $4::backupfiletype, $5,
                                $6, $7, $8, $9,
                                $10, $11, $12, $13, $14,
                                $15, $16::json, TRUE, $17
                            )
                            """,
                            backup_set_db_id,
                            file_path,
                            processed_file.get('file_name', Path(file_path).name),
                            processed_file.get('file_type', BackupFileType.FILE.value),
                            processed_file.get('file_size', 0),
                            per_file_compressed_size,
                            processed_file.get('file_permissions'),
                            datetime.fromtimestamp(file_stat.st_ctime) if file_stat else None,
                            datetime.fromtimestamp(file_stat.st_mtime) if file_stat else None,
                            datetime.fromtimestamp(file_stat.st_atime) if file_stat else None,
                            is_compressed,
                            checksum,
                            backup_time,
                            chunk_number,
                            0,
                            json.dumps(metadata),
                            copy_time
                        )
                        success_count += 1
                    except Exception as insert_error:
                        failed_count += 1
                        logger.warning(
                            f"[mark_files_as_copied] Failed to insert {processed_file.get('file_path', 'unknown')}: {insert_error}"
                        )
                else:
                    # UPDATE 成功
                    success_count += 1
            except Exception as e:
                failed_count += 1
                logger.warning(
                    f"[mark_files_as_copied] Failed to update {processed_file.get('file_path', 'unknown')}: {e}"
                )
                continue
        
        if success_count > 0:
            logger.debug(f"[mark_files_as_copied] Updated {success_count} files as copied")
        if failed_count > 0:
            logger.warning(
                f"[mark_files_as_copied] {failed_count} files failed to update, continuing backup flow"
            )
    
    async def update_scan_progress(
        self, 
        backup_task: BackupTask, 
        scanned_count: int, 
        valid_count: int, 
        operation_status: str = None
    ):
        """更新扫描进度到数据库
        
        Args:
            backup_task: 备份任务对象
            scanned_count: 已处理文件数（processed_files）
            valid_count: 压缩包数量（total_files），仅在压缩/写入阶段使用；扫描阶段传入的值会被忽略，使用 backup_task.total_files
            operation_status: 操作状态（如"[扫描文件中...]"、"[压缩文件中...]"等）
        """
        try:
            if not backup_task or not backup_task.id:
                return
            
            normalized_status = operation_status.strip() if isinstance(operation_status, str) else operation_status
            if normalized_status:
                self._log_operation_stage_event(backup_task, normalized_status)
                operation_status = normalized_status
            
            from utils.scheduler.db_utils import is_opengauss, get_opengauss_connection
            if is_opengauss():
                # 使用连接池
                async with get_opengauss_connection() as conn:
                    # 重要：不再更新 total_files 字段（压缩包数量）
                    # total_files 字段由独立的后台扫描任务 _scan_for_progress_update 负责更新（总文件数）
                    # 压缩包数量存储在 result_summary.estimated_archive_count 中
                    
                    if operation_status:
                        # 先获取当前description、total_files、total_bytes和result_summary
                        row = await conn.fetchrow(
                            "SELECT description, total_files, total_bytes, result_summary FROM backup_tasks WHERE id = $1",
                            backup_task.id
                        )
                        current_desc = row['description'] if row and row['description'] else ''
                        # 从数据库读取当前的 total_files 值（总文件数），保持不变
                        total_files_value = row['total_files'] if row and row['total_files'] else 0
                        # 从数据库读取当前的 total_bytes 值（总字节数），保持不变
                        total_bytes_from_db = row['total_bytes'] if row and row['total_bytes'] else 0
                        # 保持数据库中的 result_summary 不变（由后台扫描任务更新）
                        result_summary = row['result_summary'] if row and row['result_summary'] else {}
                        if isinstance(result_summary, str):
                            try:
                                result_summary = json.loads(result_summary)
                            except json.JSONDecodeError:
                                result_summary = {}
                        elif not isinstance(result_summary, dict):
                            result_summary = {}
                        
                        # 移除所有操作状态标记（保留格式化状态）
                        # 移除所有 [操作状态...] 格式的标记，但保留 [格式化中]
                        if '[格式化中]' in current_desc:
                            # 保留格式化状态，移除其他操作状态
                            cleaned_desc = re.sub(r'\[(?!格式化中)[^\]]+\.\.\.\]', '', current_desc)
                            cleaned_desc = cleaned_desc.replace('  ', ' ').strip()
                            new_desc = cleaned_desc + ' ' + operation_status if cleaned_desc else operation_status
                        else:
                            # 移除所有操作状态标记
                            cleaned_desc = re.sub(r'\[[^\]]+\.\.\.\]', '', current_desc)
                            cleaned_desc = cleaned_desc.replace('  ', ' ').strip()
                            new_desc = cleaned_desc + ' ' + operation_status if cleaned_desc else operation_status
                        
                        # 获取compressed_bytes和processed_bytes用于更新
                        compressed_bytes = getattr(backup_task, 'compressed_bytes', 0) or 0
                        processed_bytes = getattr(backup_task, 'processed_bytes', 0) or 0
                        
                        # 重要：不从 backup_task 对象读取 total_bytes 和 total_bytes_actual
                        # 这些字段由独立的后台扫描任务 _scan_for_progress_update 负责更新
                        # 压缩流程只更新 processed_files 和 processed_bytes，不更新总文件数和总字节数
                        
                        # 只更新 result_summary 中的 estimated_archive_count（如果备份任务对象中有）
                        if hasattr(backup_task, 'result_summary') and backup_task.result_summary:
                            if isinstance(backup_task.result_summary, dict):
                                if 'estimated_archive_count' in backup_task.result_summary:
                                    result_summary['estimated_archive_count'] = backup_task.result_summary['estimated_archive_count']
                        
                        # 将result_summary转换为JSON字符串
                        result_summary_json = json.dumps(result_summary) if result_summary else None
                        
                        await conn.execute(
                            """
                            UPDATE backup_tasks
                            SET progress_percent = $1,
                                processed_files = $2,
                                total_files = $3,
                                processed_bytes = $4,
                                compressed_bytes = $5,
                                result_summary = $6::json,
                                description = $7,
                                updated_at = $8
                            WHERE id = $9
                            """,
                            backup_task.progress_percent,
                            scanned_count,  # processed_files: 已处理文件数
                            total_files_value,  # total_files: 压缩包数量
                            processed_bytes,  # processed_bytes: 已处理字节数
                            compressed_bytes,  # compressed_bytes: 压缩后字节数
                            result_summary_json,
                            new_desc,
                            datetime.now(),
                            backup_task.id
                        )
                        # 注意：total_bytes 字段不更新，由后台扫描任务负责更新
                    else:
                        # 没有操作状态，只更新进度和字节数
                        # 从数据库读取当前的 total_files、total_bytes 和 result_summary，保持不变
                        current_row = await conn.fetchrow(
                            "SELECT total_files, total_bytes, result_summary FROM backup_tasks WHERE id = $1",
                            backup_task.id
                        )
                        # 从数据库读取当前的 total_files 值（总文件数），保持不变
                        total_files_value = current_row['total_files'] if current_row and current_row['total_files'] else 0
                        # 从数据库读取当前的 total_bytes 值（总字节数），保持不变（不更新）
                        total_bytes_from_db = current_row['total_bytes'] if current_row and current_row['total_bytes'] else 0
                        # 保持数据库中的 result_summary 不变（由后台扫描任务更新）
                        result_summary = current_row['result_summary'] if current_row and current_row['result_summary'] else {}
                        if isinstance(result_summary, str):
                            try:
                                result_summary = json.loads(result_summary)
                            except json.JSONDecodeError:
                                result_summary = {}
                        elif not isinstance(result_summary, dict):
                            result_summary = {}
                        
                        # 获取compressed_bytes和processed_bytes用于更新
                        compressed_bytes = getattr(backup_task, 'compressed_bytes', 0) or 0
                        processed_bytes = getattr(backup_task, 'processed_bytes', 0) or 0
                        
                        # 重要：不从 backup_task 对象读取 total_bytes 和 total_bytes_actual
                        # 这些字段由独立的后台扫描任务 _scan_for_progress_update 负责更新
                        # 压缩流程只更新 processed_files 和 processed_bytes，不更新总文件数和总字节数
                        
                        # 只更新 result_summary 中的 estimated_archive_count（如果备份任务对象中有）
                        if hasattr(backup_task, 'result_summary') and backup_task.result_summary:
                            if isinstance(backup_task.result_summary, dict):
                                if 'estimated_archive_count' in backup_task.result_summary:
                                    result_summary['estimated_archive_count'] = backup_task.result_summary['estimated_archive_count']
                        
                        # 将result_summary转换为JSON字符串
                        result_summary_json = json.dumps(result_summary) if result_summary else None
                        
                        await conn.execute(
                            """
                            UPDATE backup_tasks
                            SET progress_percent = $1,
                                processed_files = $2,
                                total_files = $3,
                                processed_bytes = $4,
                                compressed_bytes = $5,
                                result_summary = $6::json,
                                updated_at = $7
                            WHERE id = $8
                            """,
                            backup_task.progress_percent,
                            scanned_count,  # processed_files: 已处理文件数
                            total_files_value,  # total_files: 总文件数（从数据库读取，保持不变）
                            processed_bytes,  # processed_bytes: 已处理字节数
                            compressed_bytes,  # compressed_bytes: 压缩后字节数
                            result_summary_json,
                            datetime.now(),
                            backup_task.id
                        )
                        # 注意：total_bytes 字段不更新，由后台扫描任务负责更新
            else:
                # 非 openGauss 使用 SQLAlchemy（但当前项目仅支持 openGauss）
                logger.warning("非 openGauss 数据库，跳过进度更新")
        except Exception as e:
            logger.debug(f"更新扫描进度失败（忽略继续）: {str(e)}")
    
    async def update_task_status(self, backup_task: BackupTask, status: BackupTaskStatus):
        """更新任务状态
        
        Args:
            backup_task: 备份任务对象
            status: 任务状态
        """
        try:
            backup_task.status = status
            
            # 根据状态更新 started_at 和 completed_at
            current_time = datetime.now()
            update_fields = ['status', 'updated_at']
            update_values = [status.value, current_time]
            
            if status == BackupTaskStatus.RUNNING:
                # 运行状态：更新 started_at
                update_fields.append('started_at')
                update_values.append(current_time)
                
                # 同时更新 source_paths 和 tape_id（如果有的话），以便任务卡片正确显示
                if hasattr(backup_task, 'source_paths') and backup_task.source_paths:
                    update_fields.append('source_paths')
                    update_values.append(json.dumps(backup_task.source_paths) if isinstance(backup_task.source_paths, list) else backup_task.source_paths)
                
                if hasattr(backup_task, 'tape_id') and backup_task.tape_id:
                    update_fields.append('tape_id')
                    update_values.append(backup_task.tape_id)
            elif status in (BackupTaskStatus.COMPLETED, BackupTaskStatus.FAILED, BackupTaskStatus.CANCELLED):
                # 完成/失败/取消状态：更新 completed_at
                update_fields.append('completed_at')
                update_values.append(current_time)
            
            # 使用原生 openGauss SQL
            from utils.scheduler.db_utils import is_opengauss, get_opengauss_connection
            
            if is_opengauss():
                # 使用连接池
                async with get_opengauss_connection() as conn:
                    # 动态构建UPDATE语句
                    set_clauses = []
                    params = []
                    param_index = 1
                    
                    for field in update_fields:
                        if field == 'status':
                            set_clauses.append(f"status = ${param_index}::backuptaskstatus")
                        elif field == 'source_paths':
                            set_clauses.append(f"source_paths = ${param_index}::json")
                        else:
                            set_clauses.append(f"{field} = ${param_index}")
                        params.append(update_values[update_fields.index(field)])
                        param_index += 1
                    
                    params.append(backup_task.id)
                    
                    await conn.execute(
                        f"""
                        UPDATE backup_tasks
                        SET {', '.join(set_clauses)}
                        WHERE id = ${param_index}
                        """,
                        *params
                    )
            else:
                # 非 openGauss 使用 SQLAlchemy
                from config.database import get_db
                async for db in get_db():
                    await db.commit()
        except Exception as e:
            logger.error(f"更新任务状态失败: {str(e)}")
    
    async def update_task_stage_async(self, backup_task: BackupTask, stage_code: str, description: str = None):
        """更新任务的操作阶段（异步方法）

        Args:
            backup_task: 备份任务对象
            stage_code: 阶段代码（scan/compress/copy/finalize）
            description: 可选的阶段描述，如果提供则同时更新description字段
        """
        try:
            if not backup_task or not getattr(backup_task, 'id', None):
                logger.warning("无效的任务对象，无法更新阶段")
                return

            task_id = backup_task.id

            # 使用原生 openGauss SQL
            from utils.scheduler.db_utils import is_opengauss, get_opengauss_connection

            current_time = datetime.now()

            if is_opengauss():
                async with get_opengauss_connection() as conn:
                    if description:
                        # 同时更新operation_stage和description
                        await conn.execute("""
                            UPDATE backup_tasks
                            SET operation_stage = $1,
                                description = $2,
                                updated_at = $3
                            WHERE id = $4
                        """, stage_code, description, current_time, task_id)
                    else:
                        # 只更新operation_stage
                        await conn.execute("""
                            UPDATE backup_tasks
                            SET operation_stage = $1,
                                updated_at = $2
                            WHERE id = $3
                        """, stage_code, current_time, task_id)
            else:
                # 非 openGauss 数据库更新
                from config.database import get_db
                async for db in get_db():
                    # 这里假设模型有operation_stage字段
                    if hasattr(backup_task, 'operation_stage'):
                        backup_task.operation_stage = stage_code
                    if description and hasattr(backup_task, 'description'):
                        backup_task.description = description
                    backup_task.updated_at = current_time
                    await db.commit()
                    break

            logger.info(f"任务 {task_id} 阶段更新为: {stage_code}" + (f", 描述: {description}" if description else ""))

        except Exception as e:
            logger.error(f"更新任务阶段失败: {str(e)}")

    async def update_task_stage_with_description(self, backup_task: BackupTask, stage_code: str, description: str):
        """更新任务阶段和描述的便捷方法

        Args:
            backup_task: 备份任务对象
            stage_code: 阶段代码（scan/compress/copy/finalize）
            description: 阶段描述
        """
        await self.update_task_stage_async(backup_task, stage_code, description)

    def update_task_stage(self, backup_task: BackupTask, stage_code: str, main_loop=None, description: str = None):
        """更新任务的操作阶段（同步方法，在线程中调用时需要使用主事件循环）

        Args:
            backup_task: 备份任务对象
            stage_code: 阶段代码（scan/compress/copy/finalize）
            main_loop: 主事件循环（如果在线程中调用，必须提供）
            description: 可选的阶段描述，如果提供则同时更新description字段
        """
        try:
            if not backup_task or not getattr(backup_task, 'id', None):
                logger.warning("无效的任务对象，无法更新阶段")
                return

            import asyncio

            # 如果提供了主事件循环，使用它（从线程中调用）
            if main_loop:
                try:
                    future = asyncio.run_coroutine_threadsafe(
                        self.update_task_stage_async(backup_task, stage_code, description),
                        main_loop
                    )
                    # 等待完成（设置超时避免阻塞）
                    future.result(timeout=5.0)
                except Exception as e:
                    logger.warning(f"通过主事件循环更新任务阶段失败: {str(e)}")
                return

            # 否则，尝试在当前事件循环中执行
            try:
                loop = asyncio.get_running_loop()
                # 如果事件循环正在运行，创建任务
                loop.create_task(self.update_task_stage_async(backup_task, stage_code, description))
            except RuntimeError:
                # 没有运行的事件循环，创建新的
                loop = asyncio.new_event_loop()
                try:
                    loop.run_until_complete(self.update_task_stage_async(backup_task, stage_code, description))
                finally:
                    loop.close()

        except Exception as e:
            logger.error(f"更新任务阶段失败: {str(e)}")
    
    async def update_task_fields(self, backup_task: BackupTask, **fields):
        """更新任务的特定字段
        
        Args:
            backup_task: 备份任务对象
            **fields: 要更新的字段（键值对）
        """
        try:
            if not fields:
                return
            
            # 更新对象属性
            for field, value in fields.items():
                if hasattr(backup_task, field):
                    setattr(backup_task, field, value)
            
            # 使用原生 openGauss SQL
            from utils.scheduler.db_utils import is_opengauss, get_opengauss_connection
            
            if is_opengauss():
                # 使用连接池
                async with get_opengauss_connection() as conn:
                    # 动态构建UPDATE语句
                    set_clauses = []
                    params = []
                    param_index = 1
                    
                    for field, value in fields.items():
                        if field == 'status':
                            set_clauses.append(f"status = ${param_index}::backuptaskstatus")
                        elif field == 'source_paths':
                            set_clauses.append(f"source_paths = ${param_index}::json")
                        else:
                            set_clauses.append(f"{field} = ${param_index}")
                        
                        # 处理 JSON 字段
                        if field == 'source_paths' and isinstance(value, list):
                            params.append(json.dumps(value))
                        else:
                            params.append(value)
                        param_index += 1
                    
                    params.append(backup_task.id)
                    
                    await conn.execute(
                        f"""
                        UPDATE backup_tasks
                        SET {', '.join(set_clauses)}, updated_at = CURRENT_TIMESTAMP
                        WHERE id = ${param_index}
                        """,
                        *params
                    )
            else:
                # 非 openGauss 使用 SQLAlchemy
                from config.database import get_db
                async for db in get_db():
                    await db.commit()
        except Exception as e:
            logger.error(f"更新任务字段失败: {str(e)}")
    
    async def get_task_status(self, task_id: int) -> Optional[Dict]:
        """获取任务状态
        
        Args:
            task_id: 任务ID
            
        Returns:
            任务状态字典，如果不存在则返回None
        """
        try:
            # 使用原生 openGauss SQL
            from utils.scheduler.db_utils import is_opengauss, get_opengauss_connection
            
            if is_opengauss():
                # 使用连接池
                async with get_opengauss_connection() as conn:
                    row = await conn.fetchrow(
                        """
                        SELECT id, status, progress_percent, processed_files, total_files,
                               processed_bytes, total_bytes, compressed_bytes, description,
                               source_paths, tape_device, tape_id, result_summary, started_at, completed_at
                        FROM backup_tasks
                        WHERE id = $1
                        """,
                        task_id
                    )
                    
                    if row:
                        source_paths = None
                        if row['source_paths']:
                            try:
                                if isinstance(row['source_paths'], str):
                                    source_paths = json.loads(row['source_paths'])
                                else:
                                    source_paths = row['source_paths']
                            except:
                                source_paths = None
                        
                        # 计算压缩率
                        compression_ratio = 0.0
                        if row['processed_bytes'] and row['processed_bytes'] > 0 and row['compressed_bytes']:
                            compression_ratio = float(row['compressed_bytes']) / float(row['processed_bytes'])
                        
                        # 解析result_summary获取预计的压缩包总数
                        estimated_archive_count = None
                        total_scanned_bytes = None
                        if row['result_summary']:
                            try:
                                result_summary_dict = None
                                if isinstance(row['result_summary'], str):
                                    result_summary_dict = json.loads(row['result_summary'])
                                elif isinstance(row['result_summary'], dict):
                                    result_summary_dict = row['result_summary']
                                
                                if isinstance(result_summary_dict, dict):
                                    estimated_archive_count = result_summary_dict.get('estimated_archive_count')
                                    total_scanned_bytes = result_summary_dict.get('total_scanned_bytes')
                            except Exception:
                                pass
                        
                        return {
                            'task_id': task_id,
                            'status': row['status'],
                            'progress_percent': row['progress_percent'] or 0.0,
                            'processed_files': row['processed_files'] or 0,
                            'total_files': row['total_files'] or 0,  # 总文件数（由后台扫描任务更新）
                            'total_bytes': row['total_bytes'] or 0,  # 总字节数（由后台扫描任务更新）
                            'processed_bytes': row['processed_bytes'] or 0,
                            'compressed_bytes': row['compressed_bytes'] or 0,
                            'compression_ratio': compression_ratio,
                            'estimated_archive_count': estimated_archive_count,  # 压缩包数量（从 result_summary.estimated_archive_count 读取）
                            'description': row['description'] or '',
                            'source_paths': source_paths,
                            'tape_device': row['tape_device'],
                            'tape_id': row['tape_id'],
                            'started_at': row['started_at'],
                            'completed_at': row['completed_at']
                        }
            
            return None
        except Exception as e:
            logger.error(f"获取任务状态失败: {str(e)}")
            return None
    
    async def get_total_files_from_db(self, task_id: int) -> int:
        """从数据库读取总文件数（由后台扫描任务更新）
        
        Args:
            task_id: 任务ID
            
        Returns:
            总文件数，如果不存在则返回0
        """
        try:
            from utils.scheduler.db_utils import is_opengauss, get_opengauss_connection
            
            if is_opengauss():
                # 使用连接池
                async with get_opengauss_connection() as conn:
                    row = await conn.fetchrow(
                        "SELECT total_files FROM backup_tasks WHERE id = $1",
                        task_id
                    )
                    if row:
                        return row['total_files'] or 0
            return 0
        except Exception as e:
            logger.debug(f"读取总文件数失败（忽略继续）: {str(e)}")
            return 0
    
    async def update_scan_progress_only(self, backup_task: BackupTask, total_files: int, total_bytes: int):
        """仅更新扫描进度（总文件数和总字节数），不更新已处理文件数
        
        Args:
            backup_task: 备份任务对象
            total_files: 总文件数
            total_bytes: 总字节数
        """
        try:
            if not backup_task or not backup_task.id:
                return
            
            # 更新备份任务对象的统计信息
            backup_task.total_files = total_files  # total_files: 总文件数
            backup_task.total_bytes = total_bytes  # total_bytes: 总字节数
            
            # 更新数据库
            from utils.scheduler.db_utils import is_opengauss, get_opengauss_connection
            
            if is_opengauss():
                # 使用连接池
                async with get_opengauss_connection() as conn:
                    # 获取当前的 result_summary
                    row = await conn.fetchrow(
                        "SELECT result_summary FROM backup_tasks WHERE id = $1",
                        backup_task.id
                    )
                    result_summary = {}
                    if row and row['result_summary']:
                        try:
                            if isinstance(row['result_summary'], str):
                                result_summary = json.loads(row['result_summary'])
                            elif isinstance(row['result_summary'], dict):
                                result_summary = row['result_summary']
                        except Exception:
                            result_summary = {}
                    
                    # 更新 result_summary 中的总文件数和总字节数（作为备份存储）
                    result_summary['total_scanned_files'] = total_files
                    result_summary['total_scanned_bytes'] = total_bytes
                    
                    # 更新数据库：使用正确的字段存储总文件数和总字节数
                    await conn.execute(
                        """
                        UPDATE backup_tasks
                        SET total_files = $1,
                            total_bytes = $2,
                            result_summary = $3::json,
                            updated_at = $4
                        WHERE id = $5
                        """,
                        total_files,  # total_files: 总文件数
                        total_bytes,   # total_bytes: 总字节数
                        json.dumps(result_summary),
                        datetime.now(),
                        backup_task.id
                    )
        except Exception as e:
            logger.debug(f"更新扫描进度失败（忽略继续）: {str(e)}")

