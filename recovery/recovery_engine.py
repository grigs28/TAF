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
from config.database import get_db
from models.backup import BackupSet, BackupFile, BackupSetStatus
from models.system_log import OperationLog, OperationType
from tape.tape_manager import TapeManager
from utils.dingtalk_notifier import DingTalkNotifier

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
        """搜索备份集"""
        try:
            backup_sets = []

            # 这里应该从数据库查询备份集
            # 暂时返回示例数据
            sample_backup_sets = [
                {
                    'set_id': '2024-01_000001',
                    'set_name': '月度备份_2024-01_000001',
                    'backup_group': '2024-01',
                    'backup_type': 'monthly_full',
                    'backup_time': datetime.now().isoformat(),
                    'total_files': 1500,
                    'total_bytes': 10737418240,  # 10GB
                    'tape_id': 'TAPE001',
                    'status': 'active'
                },
                {
                    'set_id': '2024-02_000001',
                    'set_name': '月度备份_2024-02_000001',
                    'backup_group': '2024-02',
                    'backup_type': 'monthly_full',
                    'backup_time': datetime.now().isoformat(),
                    'total_files': 1600,
                    'total_bytes': 12884901888,  # 12GB
                    'tape_id': 'TAPE002',
                    'status': 'active'
                }
            ]

            # 应用过滤条件
            if filters:
                filtered_sets = []
                for backup_set in sample_backup_sets:
                    match = True

                    if 'backup_group' in filters:
                        if backup_set['backup_group'] != filters['backup_group']:
                            match = False

                    if 'tape_id' in filters:
                        if backup_set['tape_id'] != filters['tape_id']:
                            match = False

                    if 'date_from' in filters:
                        backup_time = datetime.fromisoformat(backup_set['backup_time'])
                        if backup_time < filters['date_from']:
                            match = False

                    if 'date_to' in filters:
                        backup_time = datetime.fromisoformat(backup_set['backup_time'])
                        if backup_time > filters['date_to']:
                            match = False

                    if match:
                        filtered_sets.append(backup_set)

                backup_sets = filtered_sets
            else:
                backup_sets = sample_backup_sets

            return backup_sets

        except Exception as e:
            logger.error(f"搜索备份集失败: {str(e)}")
            return []

    async def get_backup_set_files(self, backup_set_id: str) -> List[Dict]:
        """获取备份集文件列表"""
        try:
            # 这里应该从数据库查询文件列表
            # 暂时返回示例数据
            sample_files = [
                {
                    'id': 1,
                    'file_path': '/data/documents/report.pdf',
                    'file_name': 'report.pdf',
                    'file_type': 'file',
                    'file_size': 1048576,  # 1MB
                    'compressed_size': 524288,  # 512KB
                    'backup_time': datetime.now().isoformat(),
                    'checksum': 'abc123def456'
                },
                {
                    'id': 2,
                    'file_path': '/data/images/photo.jpg',
                    'file_name': 'photo.jpg',
                    'file_type': 'file',
                    'file_size': 2097152,  # 2MB
                    'compressed_size': 1572864,  # 1.5MB
                    'backup_time': datetime.now().isoformat(),
                    'checksum': 'def456abc123'
                },
                {
                    'id': 3,
                    'file_path': '/data/projects/',
                    'file_name': 'projects',
                    'file_type': 'directory',
                    'file_size': 0,
                    'compressed_size': 0,
                    'backup_time': datetime.now().isoformat(),
                    'checksum': None
                }
            ]

            return sample_files

        except Exception as e:
            logger.error(f"获取备份集文件列表失败: {str(e)}")
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
        """获取备份集信息"""
        try:
            # 这里应该从数据库查询
            # 暂时返回示例信息
            return {
                'set_id': backup_set_id,
                'tape_id': 'TAPE001',
                'backup_time': datetime.now().isoformat()
            }
        except Exception as e:
            logger.error(f"获取备份集信息失败: {str(e)}")
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
        """获取备份组列表"""
        try:
            # 这里应该从数据库查询
            # 暂时返回示例数据
            current_date = datetime.now()
            groups = []

            for i in range(6):  # 最近6个月
                date = current_date.replace(month=((current_date.month - i - 1) % 12) + 1,
                                           year=current_date.year - ((current_date.month - i - 1) // 12))
                group_name = date.strftime('%Y-%m')
                groups.append(group_name)

            return groups
        except Exception as e:
            logger.error(f"获取备份组列表失败: {str(e)}")
            return []