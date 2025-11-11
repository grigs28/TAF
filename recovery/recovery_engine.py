#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
恢复引擎模块
Recovery Engine Module
"""

import os
import asyncio
import logging
import hashlib
from datetime import datetime
from pathlib import Path
from typing import List, Dict, Any, Optional, Callable
import py7zr

from config.settings import get_settings
from config.database import get_db, db_manager
from models.backup import BackupSet, BackupFile, BackupSetStatus, BackupTaskType, BackupFileType
from models.system_log import OperationLog, OperationType
from tape.tape_manager import TapeManager
from utils.dingtalk_notifier import DingTalkNotifier
from utils.scheduler.db_utils import is_opengauss, get_opengauss_connection
from sqlalchemy import select, and_, or_, func
from datetime import datetime, timedelta
import json

logger = logging.getLogger(__name__)


class RecoveryEngine:
    """恢复引擎"""

    def __init__(self):
        self.settings = get_settings()
        self.tape_manager: Optional[TapeManager] = None
        self.dingtalk_notifier: Optional[DingTalkNotifier] = None
        self._initialized = False
        self._current_recovery: Optional[Dict] = None
        self._progress_callbacks: List[Callable] = []

    async def initialize(self):
        """初始化恢复引擎"""
        try:
            # 创建恢复临时目录
            Path(self.settings.RECOVERY_TEMP_DIR).mkdir(parents=True, exist_ok=True)

            self._initialized = True
            logger.info("恢复引擎初始化完成")

        except Exception as e:
            logger.error(f"恢复引擎初始化失败: {str(e)}")
            raise

    def set_dependencies(self, tape_manager: TapeManager, dingtalk_notifier: DingTalkNotifier):
        """设置依赖组件"""
        self.tape_manager = tape_manager
        self.dingtalk_notifier = dingtalk_notifier

    def add_progress_callback(self, callback: Callable):
        """添加进度回调"""
        self._progress_callbacks.append(callback)

    async def search_backup_sets(self, filters: Dict[str, Any] = None) -> List[Dict]:
        """搜索备份集（从数据库查询真实数据）"""
        try:
            backup_sets = []
            filters = filters or {}

            if is_opengauss():
                # openGauss 原生SQL查询
                conn = await get_opengauss_connection()
                try:
                    # 构建WHERE子句
                    where_clauses = []
                    params = []
                    param_index = 1

                    # 只查询活跃状态的备份集
                    where_clauses.append("LOWER(status::text) = LOWER('ACTIVE')")

                    # 应用过滤条件
                    if 'backup_group' in filters and filters['backup_group']:
                        where_clauses.append(f"backup_group = ${param_index}")
                        params.append(filters['backup_group'])
                        param_index += 1

                    if 'tape_id' in filters and filters['tape_id']:
                        where_clauses.append(f"tape_id = ${param_index}")
                        params.append(filters['tape_id'])
                        param_index += 1

                    if 'date_from' in filters and filters['date_from']:
                        where_clauses.append(f"backup_time >= ${param_index}")
                        params.append(filters['date_from'])
                        param_index += 1

                    if 'date_to' in filters and filters['date_to']:
                        where_clauses.append(f"backup_time <= ${param_index}")
                        params.append(filters['date_to'])
                        param_index += 1

                    where_sql = " AND ".join(where_clauses) if where_clauses else "1=1"

                    # 查询备份集
                    sql = f"""
                        SELECT id, set_id, set_name, backup_group, backup_type, backup_time,
                               total_files, total_bytes, compressed_bytes, compression_ratio,
                               tape_id, status, created_at
                        FROM backup_sets
                        WHERE {where_sql}
                        ORDER BY backup_time DESC
                        LIMIT 100
                    """
                    rows = await conn.fetch(sql, *params)

                    # 转换为字典格式
                    for row in rows:
                        backup_set = {
                            'id': row['id'],
                            'set_id': row['set_id'],
                            'set_name': row['set_name'],
                            'backup_group': row['backup_group'],
                            'backup_type': row['backup_type'].value if hasattr(row['backup_type'], 'value') else str(row['backup_type']),
                            'backup_time': row['backup_time'].isoformat() if isinstance(row['backup_time'], datetime) else str(row['backup_time']),
                            'total_files': row['total_files'] or 0,
                            'total_bytes': row['total_bytes'] or 0,
                            'compressed_bytes': row['compressed_bytes'] or 0,
                            'compression_ratio': float(row['compression_ratio']) if row['compression_ratio'] else None,
                            'tape_id': row['tape_id'],
                            'status': row['status'].value if hasattr(row['status'], 'value') else str(row['status']),
                            'created_at': row['created_at'].isoformat() if isinstance(row['created_at'], datetime) else str(row['created_at'])
                        }
                        backup_sets.append(backup_set)
                finally:
                    await conn.close()
            else:
                # 使用SQLAlchemy查询
                async with db_manager.AsyncSessionLocal() as session:
                    stmt = select(BackupSet).where(BackupSet.status == BackupSetStatus.ACTIVE)

                    # 应用过滤条件
                    if 'backup_group' in filters and filters['backup_group']:
                        stmt = stmt.where(BackupSet.backup_group == filters['backup_group'])

                    if 'tape_id' in filters and filters['tape_id']:
                        stmt = stmt.where(BackupSet.tape_id == filters['tape_id'])

                    if 'date_from' in filters and filters['date_from']:
                        stmt = stmt.where(BackupSet.backup_time >= filters['date_from'])

                    if 'date_to' in filters and filters['date_to']:
                        stmt = stmt.where(BackupSet.backup_time <= filters['date_to'])

                    stmt = stmt.order_by(BackupSet.backup_time.desc()).limit(100)
                    result = await session.execute(stmt)
                    sets = result.scalars().all()

                    # 转换为字典格式
                    for backup_set in sets:
                        backup_sets.append({
                            'id': backup_set.id,
                            'set_id': backup_set.set_id,
                            'set_name': backup_set.set_name,
                            'backup_group': backup_set.backup_group,
                            'backup_type': backup_set.backup_type.value if hasattr(backup_set.backup_type, 'value') else str(backup_set.backup_type),
                            'backup_time': backup_set.backup_time.isoformat() if backup_set.backup_time else None,
                            'total_files': backup_set.total_files or 0,
                            'total_bytes': backup_set.total_bytes or 0,
                            'compressed_bytes': backup_set.compressed_bytes or 0,
                            'compression_ratio': float(backup_set.compression_ratio) if backup_set.compression_ratio else None,
                            'tape_id': backup_set.tape_id,
                            'status': backup_set.status.value if hasattr(backup_set.status, 'value') else str(backup_set.status),
                            'created_at': backup_set.created_at.isoformat() if backup_set.created_at else None
                        })

            logger.info(f"查询到 {len(backup_sets)} 个备份集")
            return backup_sets

        except Exception as e:
            logger.error(f"搜索备份集失败: {str(e)}")
            import traceback
            logger.error(traceback.format_exc())
            return []

    async def get_backup_set_files(self, backup_set_id: str) -> List[Dict]:
        """获取备份集文件列表（从数据库查询真实数据）"""
        try:
            files = []

            if is_opengauss():
                # openGauss 原生SQL查询
                conn = await get_opengauss_connection()
                try:
                    # 首先根据 set_id 查找备份集的 id
                    backup_set_row = await conn.fetchrow(
                        "SELECT id FROM backup_sets WHERE set_id = $1",
                        backup_set_id
                    )
                    
                    if not backup_set_row:
                        logger.warning(f"备份集不存在: {backup_set_id}")
                        return []

                    backup_set_db_id = backup_set_row['id']

                    # 查询该备份集的所有文件
                    sql = """
                        SELECT id, file_path, file_name, file_type, file_size, compressed_size,
                               file_permissions, created_time, modified_time, accessed_time,
                               compressed, checksum, backup_time, chunk_number
                        FROM backup_files
                        WHERE backup_set_id = $1
                        ORDER BY file_path ASC
                    """
                    rows = await conn.fetch(sql, backup_set_db_id)

                    # 转换为字典格式
                    for row in rows:
                        file_info = {
                            'id': row['id'],
                            'file_path': row['file_path'],
                            'file_name': row['file_name'],
                            'file_type': row['file_type'].value if hasattr(row['file_type'], 'value') else str(row['file_type']),
                            'file_size': row['file_size'] or 0,
                            'compressed_size': row['compressed_size'] or 0,
                            'file_permissions': row['file_permissions'],
                            'created_time': row['created_time'].isoformat() if row['created_time'] and isinstance(row['created_time'], datetime) else (str(row['created_time']) if row['created_time'] else None),
                            'modified_time': row['modified_time'].isoformat() if row['modified_time'] and isinstance(row['modified_time'], datetime) else (str(row['modified_time']) if row['modified_time'] else None),
                            'accessed_time': row['accessed_time'].isoformat() if row['accessed_time'] and isinstance(row['accessed_time'], datetime) else (str(row['accessed_time']) if row['accessed_time'] else None),
                            'compressed': row['compressed'] or False,
                            'checksum': row['checksum'],
                            'backup_time': row['backup_time'].isoformat() if row['backup_time'] and isinstance(row['backup_time'], datetime) else str(row['backup_time']),
                            'chunk_number': row['chunk_number']
                        }
                        files.append(file_info)
                finally:
                    await conn.close()
            else:
                # 使用SQLAlchemy查询
                async with db_manager.AsyncSessionLocal() as session:
                    # 首先查找备份集
                    stmt = select(BackupSet).where(BackupSet.set_id == backup_set_id)
                    result = await session.execute(stmt)
                    backup_set = result.scalar_one_or_none()

                    if not backup_set:
                        logger.warning(f"备份集不存在: {backup_set_id}")
                        return []

                    # 查询该备份集的所有文件
                    stmt = select(BackupFile).where(BackupFile.backup_set_id == backup_set.id).order_by(BackupFile.file_path)
                    result = await session.execute(stmt)
                    backup_files = result.scalars().all()

                    # 转换为字典格式
                    for file in backup_files:
                        files.append({
                            'id': file.id,
                            'file_path': file.file_path,
                            'file_name': file.file_name,
                            'file_type': file.file_type.value if hasattr(file.file_type, 'value') else str(file.file_type),
                            'file_size': file.file_size or 0,
                            'compressed_size': file.compressed_size or 0,
                            'file_permissions': file.file_permissions,
                            'created_time': file.created_time.isoformat() if file.created_time else None,
                            'modified_time': file.modified_time.isoformat() if file.modified_time else None,
                            'accessed_time': file.accessed_time.isoformat() if file.accessed_time else None,
                            'compressed': file.compressed or False,
                            'checksum': file.checksum,
                            'backup_time': file.backup_time.isoformat() if file.backup_time else None,
                            'chunk_number': file.chunk_number
                        })

            logger.info(f"查询到 {len(files)} 个文件 (备份集: {backup_set_id})")
            return files

        except Exception as e:
            logger.error(f"获取备份集文件列表失败: {str(e)}")
            import traceback
            logger.error(traceback.format_exc())
            return []

    async def search_files(self, backup_set_id: str, search_term: str,
                         file_type: str = None) -> List[Dict]:
        """搜索文件"""
        try:
            all_files = await self.get_backup_set_files(backup_set_id)
            filtered_files = []

            search_term_lower = search_term.lower()

            for file_info in all_files:
                # 搜索文件名或路径
                if (search_term_lower in file_info['file_name'].lower() or
                    search_term_lower in file_info['file_path'].lower()):

                    # 文件类型过滤
                    if file_type and file_info['file_type'] != file_type:
                        continue

                    filtered_files.append(file_info)

            return filtered_files

        except Exception as e:
            logger.error(f"搜索文件失败: {str(e)}")
            return []

    async def create_recovery_task(self, backup_set_id: str, files: List[Dict],
                                 target_path: str, **kwargs) -> Optional[str]:
        """创建恢复任务"""
        try:
            if not backup_set_id or not files or not target_path:
                raise ValueError("备份集ID、文件列表和目标路径不能为空")

            # 验证目标路径
            target_dir = Path(target_path)
            target_dir.mkdir(parents=True, exist_ok=True)

            # 生成恢复任务ID
            recovery_id = f"recovery_{datetime.now().strftime('%Y%m%d_%H%M%S')}"

            # 创建恢复任务信息
            recovery_info = {
                'recovery_id': recovery_id,
                'backup_set_id': backup_set_id,
                'files': files,
                'target_path': target_path,
                'status': 'pending',
                'created_at': datetime.now(),
                'started_at': None,
                'completed_at': None,
                'progress_percent': 0.0,
                'processed_files': 0,
                'total_files': len(files),
                'total_bytes': sum(f.get('file_size', 0) for f in files),
                'processed_bytes': 0,
                'error_message': None,
                'created_by': kwargs.get('created_by', 'system')
            }

            self._current_recovery = recovery_info

            logger.info(f"创建恢复任务成功: {recovery_id}")
            return recovery_id

        except Exception as e:
            logger.error(f"创建恢复任务失败: {str(e)}")
            return None

    async def execute_recovery(self, recovery_id: str) -> bool:
        """执行恢复操作"""
        try:
            if not self._initialized:
                raise RuntimeError("恢复引擎未初始化")

            if not self._current_recovery or self._current_recovery['recovery_id'] != recovery_id:
                raise RuntimeError("恢复任务不存在")

            recovery_info = self._current_recovery
            logger.info(f"开始执行恢复任务: {recovery_id}")

            # 更新状态
            recovery_info['status'] = 'running'
            recovery_info['started_at'] = datetime.now()

            # 发送开始通知
            if self.dingtalk_notifier:
                await self.dingtalk_notifier.send_recovery_notification(
                    recovery_id,
                    "started"
                )

            # 执行恢复流程
            success = await self._perform_recovery(recovery_info)

            # 更新完成状态
            recovery_info['completed_at'] = datetime.now()
            if success:
                recovery_info['status'] = 'completed'
                if self.dingtalk_notifier:
                    await self.dingtalk_notifier.send_recovery_notification(
                        recovery_id,
                        "success",
                        {
                            'file_count': recovery_info['processed_files'],
                            'size': self._format_bytes(recovery_info['processed_bytes']),
                            'duration': str(recovery_info['completed_at'] - recovery_info['started_at'])
                        }
                    )
            else:
                recovery_info['status'] = 'failed'
                if self.dingtalk_notifier:
                    await self.dingtalk_notifier.send_recovery_notification(
                        recovery_id,
                        "failed",
                        {'error': recovery_info['error_message']}
                    )

            logger.info(f"恢复任务执行完成: {recovery_id}, 成功: {success}")
            return success

        except Exception as e:
            logger.error(f"执行恢复任务失败: {str(e)}")
            if self._current_recovery:
                self._current_recovery['error_message'] = str(e)
                self._current_recovery['status'] = 'failed'
            return False
        finally:
            self._current_recovery = None

    async def _perform_recovery(self, recovery_info: Dict) -> bool:
        """执行恢复流程"""
        try:
            backup_set_id = recovery_info['backup_set_id']
            target_path = Path(recovery_info['target_path'])
            files = recovery_info['files']

            # 1. 获取备份集信息
            backup_set_info = await self._get_backup_set_info(backup_set_id)
            if not backup_set_info:
                raise RuntimeError(f"备份集不存在: {backup_set_id}")

            # 2. 获取磁带信息
            tape_id = backup_set_info['tape_id']
            logger.info(f"需要加载磁带: {tape_id}")

            # 3. 加载磁带
            if not await self.tape_manager.load_tape(tape_id):
                raise RuntimeError(f"加载磁带失败: {tape_id}")

            # 4. 读取并恢复文件
            processed_files = 0
            processed_bytes = 0

            for file_info in files:
                try:
                    # 读取文件数据
                    file_data = await self._read_file_from_tape(file_info)
                    if not file_data:
                        logger.warning(f"无法读取文件: {file_info['file_path']}")
                        continue

                    # 解压文件（如果需要）
                    if file_info.get('compressed_size', 0) > 0:
                        file_data = await self._decompress_file_data(file_data, file_info)

                    # 写入目标位置
                    target_file_path = target_path / Path(file_info['file_path']).name
                    target_file_path.parent.mkdir(parents=True, exist_ok=True)

                    with open(target_file_path, 'wb') as f:
                        f.write(file_data)

                    # 验证文件完整性
                    if await self._verify_file_integrity(target_file_path, file_info):
                        processed_files += 1
                        processed_bytes += len(file_data)
                        logger.info(f"文件恢复成功: {file_info['file_path']}")
                    else:
                        logger.error(f"文件完整性验证失败: {file_info['file_path']}")

                    # 更新进度
                    recovery_info['processed_files'] = processed_files
                    recovery_info['processed_bytes'] = processed_bytes
                    recovery_info['progress_percent'] = (processed_files / recovery_info['total_files']) * 100

                    # 通知进度更新
                    await self._notify_progress(recovery_info)

                except Exception as e:
                    logger.error(f"恢复文件失败 {file_info['file_path']}: {str(e)}")
                    continue

            # 5. 卸载磁带
            await self.tape_manager.unload_tape()

            return processed_files > 0

        except Exception as e:
            logger.error(f"恢复流程执行失败: {str(e)}")
            recovery_info['error_message'] = str(e)
            return False

    async def _get_backup_set_info(self, backup_set_id: str) -> Optional[Dict]:
        """获取备份集信息（从数据库查询真实数据）"""
        try:
            if is_opengauss():
                # openGauss 原生SQL查询
                conn = await get_opengauss_connection()
                try:
                    row = await conn.fetchrow(
                        """
                        SELECT id, set_id, set_name, backup_group, backup_type, backup_time,
                               total_files, total_bytes, compressed_bytes, tape_id, status
                        FROM backup_sets
                        WHERE set_id = $1
                        """,
                        backup_set_id
                    )
                    
                    if not row:
                        return None
                    
                    return {
                        'id': row['id'],
                        'set_id': row['set_id'],
                        'set_name': row['set_name'],
                        'backup_group': row['backup_group'],
                        'backup_type': row['backup_type'].value if hasattr(row['backup_type'], 'value') else str(row['backup_type']),
                        'backup_time': row['backup_time'].isoformat() if isinstance(row['backup_time'], datetime) else str(row['backup_time']),
                        'total_files': row['total_files'] or 0,
                        'total_bytes': row['total_bytes'] or 0,
                        'compressed_bytes': row['compressed_bytes'] or 0,
                        'tape_id': row['tape_id'],
                        'status': row['status'].value if hasattr(row['status'], 'value') else str(row['status'])
                    }
                finally:
                    await conn.close()
            else:
                # 使用SQLAlchemy查询
                async with db_manager.AsyncSessionLocal() as session:
                    stmt = select(BackupSet).where(BackupSet.set_id == backup_set_id)
                    result = await session.execute(stmt)
                    backup_set = result.scalar_one_or_none()
                    
                    if not backup_set:
                        return None
                    
                    return {
                        'id': backup_set.id,
                        'set_id': backup_set.set_id,
                        'set_name': backup_set.set_name,
                        'backup_group': backup_set.backup_group,
                        'backup_type': backup_set.backup_type.value if hasattr(backup_set.backup_type, 'value') else str(backup_set.backup_type),
                        'backup_time': backup_set.backup_time.isoformat() if backup_set.backup_time else None,
                        'total_files': backup_set.total_files or 0,
                        'total_bytes': backup_set.total_bytes or 0,
                        'compressed_bytes': backup_set.compressed_bytes or 0,
                        'tape_id': backup_set.tape_id,
                        'status': backup_set.status.value if hasattr(backup_set.status, 'value') else str(backup_set.status)
                    }
        except Exception as e:
            logger.error(f"获取备份集信息失败: {str(e)}")
            import traceback
            logger.error(traceback.format_exc())
            return None

    async def _read_file_from_tape(self, file_info: Dict) -> Optional[bytes]:
        """从磁带读取文件数据"""
        try:
            # 这里应该根据文件信息从磁带读取数据
            # 暂时返回示例数据
            return b"sample file data"
        except Exception as e:
            logger.error(f"从磁带读取文件失败: {str(e)}")
            return None

    async def _decompress_file_data(self, compressed_data: bytes, file_info: Dict) -> bytes:
        """解压文件数据"""
        try:
            # 这里应该根据压缩算法解压数据
            # 暂时直接返回原数据
            return compressed_data
        except Exception as e:
            logger.error(f"解压文件数据失败: {str(e)}")
            return compressed_data

    async def _verify_file_integrity(self, file_path: Path, file_info: Dict) -> bool:
        """验证文件完整性"""
        try:
            if not file_path.exists():
                return False

            # 检查文件大小
            actual_size = file_path.stat().st_size
            expected_size = file_info.get('file_size', 0)
            if actual_size != expected_size:
                logger.warning(f"文件大小不匹配: 期望 {expected_size}, 实际 {actual_size}")
                return False

            # 检查校验和
            if file_info.get('checksum'):
                actual_checksum = self._calculate_file_checksum(file_path)
                if actual_checksum != file_info['checksum']:
                    logger.warning(f"文件校验和不匹配: {file_path}")
                    return False

            return True

        except Exception as e:
            logger.error(f"验证文件完整性失败: {str(e)}")
            return False

    def _calculate_file_checksum(self, file_path: Path) -> str:
        """计算文件校验和"""
        sha256_hash = hashlib.sha256()
        with open(file_path, 'rb') as f:
            for chunk in iter(lambda: f.read(4096), b""):
                sha256_hash.update(chunk)
        return sha256_hash.hexdigest()

    async def _notify_progress(self, recovery_info: Dict):
        """通知进度更新"""
        try:
            for callback in self._progress_callbacks:
                if asyncio.iscoroutinefunction(callback):
                    await callback(recovery_info)
                else:
                    callback(recovery_info)
        except Exception as e:
            logger.error(f"进度通知失败: {str(e)}")

    def _format_bytes(self, bytes_size: int) -> str:
        """格式化字节大小"""
        for unit in ['B', 'KB', 'MB', 'GB', 'TB']:
            if bytes_size < 1024.0:
                return f"{bytes_size:.2f} {unit}"
            bytes_size /= 1024.0
        return f"{bytes_size:.2f} PB"

    async def get_recovery_status(self, recovery_id: str) -> Optional[Dict]:
        """获取恢复状态"""
        try:
            if self._current_recovery and self._current_recovery['recovery_id'] == recovery_id:
                recovery_info = self._current_recovery.copy()
                # 转换datetime对象为字符串
                for key in ['created_at', 'started_at', 'completed_at']:
                    if recovery_info.get(key):
                        recovery_info[key] = recovery_info[key].isoformat()
                return recovery_info
            return None
        except Exception as e:
            logger.error(f"获取恢复状态失败: {str(e)}")
            return None

    async def cancel_recovery(self, recovery_id: str) -> bool:
        """取消恢复任务"""
        try:
            if self._current_recovery and self._current_recovery['recovery_id'] == recovery_id:
                self._current_recovery['status'] = 'cancelled'
                self._current_recovery = None
                logger.info(f"恢复任务已取消: {recovery_id}")
                return True
            return False
        except Exception as e:
            logger.error(f"取消恢复任务失败: {str(e)}")
            return False

    async def get_backup_groups(self) -> List[str]:
        """获取备份组列表（从数据库查询真实数据）"""
        try:
            groups = []

            if is_opengauss():
                # openGauss 原生SQL查询
                conn = await get_opengauss_connection()
                try:
                    # 查询所有不重复的备份组，按时间倒序
                    sql = """
                        SELECT DISTINCT backup_group
                        FROM backup_sets
                        WHERE LOWER(status::text) = LOWER('ACTIVE')
                        ORDER BY backup_group DESC
                        LIMIT 12
                    """
                    rows = await conn.fetch(sql)
                    groups = [row['backup_group'] for row in rows]
                finally:
                    await conn.close()
            else:
                # 使用SQLAlchemy查询
                async with db_manager.AsyncSessionLocal() as session:
                    stmt = select(BackupSet.backup_group).where(
                        BackupSet.status == BackupSetStatus.ACTIVE
                    ).distinct().order_by(BackupSet.backup_group.desc()).limit(12)
                    result = await session.execute(stmt)
                    groups = [row[0] for row in result.all()]

            # 如果没有查询到数据，返回最近6个月的默认组
            if not groups:
                current_date = datetime.now()
                for i in range(6):
                    date = current_date.replace(month=((current_date.month - i - 1) % 12) + 1,
                                               year=current_date.year - ((current_date.month - i - 1) // 12))
                    group_name = date.strftime('%Y-%m')
                    groups.append(group_name)

            logger.info(f"查询到 {len(groups)} 个备份组")
            return groups
        except Exception as e:
            logger.error(f"获取备份组列表失败: {str(e)}")
            import traceback
            logger.error(traceback.format_exc())
            # 返回默认的最近6个月
            current_date = datetime.now()
            groups = []
            for i in range(6):
                date = current_date.replace(month=((current_date.month - i - 1) % 12) + 1,
                                           year=current_date.year - ((current_date.month - i - 1) // 12))
                group_name = date.strftime('%Y-%m')
                groups.append(group_name)
            return groups