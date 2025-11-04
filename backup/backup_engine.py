#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
备份引擎模块
Backup Engine Module
"""

import os
import asyncio
import logging
import hashlib
from datetime import datetime, timedelta
from pathlib import Path
from typing import List, Dict, Any, Optional, Callable
import py7zr
import psutil

from config.settings import get_settings
from config.database import get_db
from models.backup import BackupTask, BackupSet, BackupFile, BackupTaskStatus, BackupTaskType, BackupFileType
from models.system_log import OperationLog, OperationType
from tape.tape_manager import TapeManager
from tape.tape_cartridge import TapeCartridge
from utils.dingtalk_notifier import DingTalkNotifier

logger = logging.getLogger(__name__)


class BackupEngine:
    """备份引擎"""

    def __init__(self):
        self.settings = get_settings()
        self.tape_manager: Optional[TapeManager] = None
        self.dingtalk_notifier: Optional[DingTalkNotifier] = None
        self._initialized = False
        self._current_task: Optional[BackupTask] = None
        self._progress_callbacks: List[Callable] = []

    async def initialize(self):
        """初始化备份引擎"""
        try:
            # 创建临时目录
            temp_dirs = [
                self.settings.BACKUP_TEMP_DIR,
                self.settings.RECOVERY_TEMP_DIR
            ]
            for temp_dir in temp_dirs:
                Path(temp_dir).mkdir(parents=True, exist_ok=True)

            self._initialized = True
            logger.info("备份引擎初始化完成")

        except Exception as e:
            logger.error(f"备份引擎初始化失败: {str(e)}")
            raise

    def set_dependencies(self, tape_manager: TapeManager, dingtalk_notifier: DingTalkNotifier):
        """设置依赖组件"""
        self.tape_manager = tape_manager
        self.dingtalk_notifier = dingtalk_notifier

    def add_progress_callback(self, callback: Callable):
        """添加进度回调"""
        self._progress_callbacks.append(callback)

    async def create_backup_task(self, task_name: str, source_paths: List[str],
                               task_type: BackupTaskType = BackupTaskType.FULL,
                               **kwargs) -> Optional[BackupTask]:
        """创建备份任务"""
        try:
            # 检查参数
            if not task_name or not source_paths:
                raise ValueError("任务名称和源路径不能为空")

            # 验证源路径
            for path in source_paths:
                if not os.path.exists(path):
                    raise ValueError(f"源路径不存在: {path}")

            # 创建备份任务
            backup_task = BackupTask(
                task_name=task_name,
                task_type=task_type,
                source_paths=source_paths,
                exclude_patterns=kwargs.get('exclude_patterns', []),
                compression_enabled=kwargs.get('compression_enabled', True),
                encryption_enabled=kwargs.get('encryption_enabled', False),
                retention_days=kwargs.get('retention_days', self.settings.DEFAULT_RETENTION_MONTHS * 30),
                description=kwargs.get('description', ''),
                scheduled_time=kwargs.get('scheduled_time'),
                created_by=kwargs.get('created_by', 'system')
            )

            # 保存到数据库
            async for db in get_db():
                db.add(backup_task)
                await db.commit()
                await db.refresh(backup_task)

            logger.info(f"创建备份任务成功: {task_name}")
            return backup_task

        except Exception as e:
            logger.error(f"创建备份任务失败: {str(e)}")
            return None

    async def execute_backup_task(self, backup_task: BackupTask) -> bool:
        """执行备份任务"""
        try:
            if not self._initialized:
                raise RuntimeError("备份引擎未初始化")

            self._current_task = backup_task
            task_id = backup_task.id

            logger.info(f"开始执行备份任务: {backup_task.task_name} (ID: {task_id})")

            # 更新任务状态
            await self._update_task_status(backup_task, BackupTaskStatus.RUNNING)
            backup_task.started_at = datetime.now()

            # 发送开始通知
            if self.dingtalk_notifier:
                await self.dingtalk_notifier.send_backup_notification(
                    backup_task.task_name,
                    "started"
                )

            # 执行备份流程
            success = await self._perform_backup(backup_task)

            # 更新任务完成状态
            backup_task.completed_at = datetime.now()
            if success:
                await self._update_task_status(backup_task, BackupTaskStatus.COMPLETED)
                if self.dingtalk_notifier:
                    await self.dingtalk_notifier.send_backup_notification(
                        backup_task.task_name,
                        "success",
                        {
                            'size': self._format_bytes(backup_task.processed_bytes),
                            'file_count': backup_task.processed_files,
                            'duration': str(backup_task.completed_at - backup_task.started_at)
                        }
                    )
            else:
                await self._update_task_status(backup_task, BackupTaskStatus.FAILED)
                if self.dingtalk_notifier:
                    await self.dingtalk_notifier.send_backup_notification(
                        backup_task.task_name,
                        "failed",
                        {'error': backup_task.error_message}
                    )

            # 保存任务结果
            async for db in get_db():
                await db.commit()

            logger.info(f"备份任务执行完成: {backup_task.task_name}, 成功: {success}")
            return success

        except Exception as e:
            logger.error(f"执行备份任务失败: {str(e)}")
            if backup_task:
                backup_task.error_message = str(e)
                await self._update_task_status(backup_task, BackupTaskStatus.FAILED)
            return False
        finally:
            self._current_task = None

    async def _perform_backup(self, backup_task: BackupTask) -> bool:
        """执行备份流程"""
        try:
            # 1. 扫描源文件
            logger.info("扫描源文件...")
            file_list = await self._scan_source_files(backup_task.source_paths, backup_task.exclude_patterns)
            backup_task.total_files = len(file_list)
            backup_task.total_bytes = sum(f['size'] for f in file_list)

            # 2. 获取可用磁带
            logger.info("获取可用磁带...")
            available_tape = await self.tape_manager.get_available_tape()
            if not available_tape:
                raise RuntimeError("没有可用的磁带")

            # 3. 加载磁带
            logger.info(f"加载磁带: {available_tape.tape_id}")
            if not await self.tape_manager.load_tape(available_tape.tape_id):
                raise RuntimeError(f"加载磁带失败: {available_tape.tape_id}")

            backup_task.tape_id = available_tape.tape_id

            # 4. 创建备份集
            backup_set = await self._create_backup_set(backup_task, available_tape)

            # 5. 分组压缩文件
            logger.info("分组压缩文件...")
            file_groups = await self._group_files_for_compression(file_list)

            # 6. 处理每个文件组
            processed_files = 0
            total_size = 0

            for group_idx, file_group in enumerate(file_groups):
                logger.info(f"处理文件组 {group_idx + 1}/{len(file_groups)}")

                # 压缩文件组
                compressed_file = await self._compress_file_group(file_group, backup_set)
                if not compressed_file:
                    continue

                # 写入磁带
                compressed_data = Path(compressed_file['path']).read_bytes()
                success = await self.tape_manager.write_data(compressed_data)
                if not success:
                    raise RuntimeError("写入磁带失败")

                # 更新进度
                processed_files += len(file_group)
                total_size += compressed_file['compressed_size']
                backup_task.processed_files = processed_files
                backup_task.processed_bytes = total_size
                backup_task.compressed_bytes = total_size
                backup_task.progress_percent = (processed_files / backup_task.total_files) * 100

                # 通知进度更新
                await self._notify_progress(backup_task)

                # 清理临时文件
                os.unlink(compressed_file['path'])

            # 7. 完成备份集
            await self._finalize_backup_set(backup_set, processed_files, total_size)

            # 8. 卸载磁带
            await self.tape_manager.unload_tape()

            return True

        except Exception as e:
            logger.error(f"备份流程执行失败: {str(e)}")
            backup_task.error_message = str(e)
            return False

    async def _scan_source_files(self, source_paths: List[str], exclude_patterns: List[str]) -> List[Dict]:
        """扫描源文件"""
        file_list = []
        
        if not source_paths:
            logger.warning("源路径列表为空")
            return file_list

        for idx, source_path_str in enumerate(source_paths):
            logger.info(f"扫描源路径 {idx + 1}/{len(source_paths)}: {source_path_str}")
            source_path = Path(source_path_str)
            
            # 检查路径是否存在
            if not source_path.exists():
                logger.warning(f"源路径不存在，跳过: {source_path_str}")
                continue
            
            try:
                if source_path.is_file():
                    logger.debug(f"扫描文件: {source_path_str}")
                    file_info = await self._get_file_info(source_path)
                    if file_info and not self._should_exclude_file(file_info['path'], exclude_patterns):
                        file_list.append(file_info)
                        logger.debug(f"已添加文件: {file_info['path']}")
                elif source_path.is_dir():
                    logger.info(f"扫描目录: {source_path_str}")
                    scanned_count = 0
                    excluded_count = 0
                    
                    # 使用 rglob 递归扫描，但需要处理可能的异常
                    try:
                        for file_path in source_path.rglob('*'):
                            if file_path.is_file():
                                scanned_count += 1
                                # 每扫描100个文件输出一次进度
                                if scanned_count % 100 == 0:
                                    logger.info(f"已扫描 {scanned_count} 个文件，找到 {len(file_list)} 个有效文件...")
                                
                                file_info = await self._get_file_info(file_path)
                                if file_info:
                                    if not self._should_exclude_file(file_info['path'], exclude_patterns):
                                        file_list.append(file_info)
                                    else:
                                        excluded_count += 1
                    except Exception as e:
                        logger.error(f"扫描目录时发生错误 {source_path_str}: {str(e)}")
                        # 继续扫描其他路径
                        continue
                    
                    logger.info(f"目录扫描完成: {source_path_str}, 扫描 {scanned_count} 个文件, 有效 {len(file_list)} 个, 排除 {excluded_count} 个")
                else:
                    logger.warning(f"源路径既不是文件也不是目录: {source_path_str}")
            except Exception as e:
                logger.error(f"处理源路径失败 {source_path_str}: {str(e)}")
                continue

        logger.info(f"扫描完成，共找到 {len(file_list)} 个文件")
        return file_list

    async def _get_file_info(self, file_path: Path) -> Optional[Dict]:
        """获取文件信息"""
        try:
            stat = file_path.stat()
            return {
                'path': str(file_path),
                'name': file_path.name,
                'size': stat.st_size,
                'modified_time': datetime.fromtimestamp(stat.st_mtime),
                'permissions': oct(stat.st_mode)[-3:],
                'is_file': file_path.is_file(),
                'is_dir': file_path.is_dir(),
                'is_symlink': file_path.is_symlink()
            }
        except Exception as e:
            logger.warning(f"获取文件信息失败 {file_path}: {str(e)}")
            return None

    def _should_exclude_file(self, file_path: str, exclude_patterns: List[str]) -> bool:
        """检查文件是否应该被排除"""
        import fnmatch

        for pattern in exclude_patterns:
            if fnmatch.fnmatch(file_path, pattern):
                return True
        return False

    async def _group_files_for_compression(self, file_list: List[Dict]) -> List[List[Dict]]:
        """将文件分组以进行压缩"""
        max_size = self.settings.MAX_FILE_SIZE
        groups = []
        current_group = []
        current_size = 0

        for file_info in file_list:
            # 如果单个文件超过最大大小，单独成组
            if file_info['size'] > max_size:
                if current_group:
                    groups.append(current_group)
                    current_group = []
                    current_size = 0
                groups.append([file_info])
                continue

            # 检查是否超过组大小限制
            if current_size + file_info['size'] > max_size and current_group:
                groups.append(current_group)
                current_group = []
                current_size = 0

            current_group.append(file_info)
            current_size += file_info['size']

        if current_group:
            groups.append(current_group)

        return groups

    async def _compress_file_group(self, file_group: List[Dict], backup_set: BackupSet) -> Optional[Dict]:
        """压缩文件组"""
        try:
            # 创建临时压缩文件
            timestamp = datetime.now().strftime('%Y%m%d_%H%M%S')
            temp_file = Path(self.settings.BACKUP_TEMP_DIR) / f"backup_{backup_set.set_id}_{timestamp}.7z"

            # 创建7z压缩文件
            with py7zr.SevenZipFile(temp_file, 'w', filters=[{'id': py7zr.FILTER_LZMA2}]) as archive:
                for file_info in file_group:
                    file_path = Path(file_info['path'])
                    if file_path.exists():
                        # 计算相对路径
                        archive_name = str(file_path.relative_to(file_path.anchor))
                        archive.write(file_path, arcname=archive_name)

            # 计算校验和
            checksum = self._calculate_file_checksum(temp_file)

            compressed_info = {
                'path': str(temp_file),
                'original_size': sum(f['size'] for f in file_group),
                'compressed_size': temp_file.stat().st_size,
                'file_count': len(file_group),
                'checksum': checksum
            }

            logger.debug(f"压缩完成: {len(file_group)} 个文件, "
                        f"原始大小: {self._format_bytes(compressed_info['original_size'])}, "
                        f"压缩后: {self._format_bytes(compressed_info['compressed_size'])}")

            return compressed_info

        except Exception as e:
            logger.error(f"压缩文件组失败: {str(e)}")
            return None

    def _calculate_file_checksum(self, file_path: Path) -> str:
        """计算文件校验和"""
        sha256_hash = hashlib.sha256()
        with open(file_path, 'rb') as f:
            for chunk in iter(lambda: f.read(4096), b""):
                sha256_hash.update(chunk)
        return sha256_hash.hexdigest()

    async def _create_backup_set(self, backup_task: BackupTask, tape: TapeCartridge) -> BackupSet:
        """创建备份集"""
        try:
            # 生成备份集ID
            backup_group = datetime.now().strftime('%Y-%m')
            set_id = f"{backup_group}_{backup_task.id:06d}"

            backup_set = BackupSet(
                set_id=set_id,
                set_name=f"{backup_task.task_name}_{set_id}",
                backup_group=backup_group,
                backup_task_id=backup_task.id,
                tape_id=tape.tape_id,
                backup_type=backup_task.task_type,
                backup_time=datetime.now(),
                source_info={'paths': backup_task.source_paths},
                retention_until=datetime.now() + timedelta(days=backup_task.retention_days)
            )

            # 保存到数据库
            async for db in get_db():
                db.add(backup_set)
                await db.commit()
                await db.refresh(backup_set)

            backup_task.backup_set_id = set_id
            logger.info(f"创建备份集: {set_id}")

            return backup_set

        except Exception as e:
            logger.error(f"创建备份集失败: {str(e)}")
            raise

    async def _finalize_backup_set(self, backup_set: BackupSet, file_count: int, total_size: int):
        """完成备份集"""
        try:
            backup_set.total_files = file_count
            backup_set.total_bytes = total_size
            backup_set.compressed_bytes = total_size
            backup_set.compression_ratio = total_size / backup_set.total_bytes if backup_set.total_bytes > 0 else 1.0
            backup_set.chunk_count = 1  # 简化处理

            # 保存更新
            async for db in get_db():
                await db.commit()

            logger.info(f"备份集完成: {backup_set.set_id}")

        except Exception as e:
            logger.error(f"完成备份集失败: {str(e)}")

    async def _update_task_status(self, backup_task: BackupTask, status: BackupTaskStatus):
        """更新任务状态"""
        try:
            backup_task.status = status
            async for db in get_db():
                await db.commit()
        except Exception as e:
            logger.error(f"更新任务状态失败: {str(e)}")

    async def _notify_progress(self, backup_task: BackupTask):
        """通知进度更新"""
        try:
            for callback in self._progress_callbacks:
                if asyncio.iscoroutinefunction(callback):
                    await callback(backup_task)
                else:
                    callback(backup_task)
        except Exception as e:
            logger.error(f"进度通知失败: {str(e)}")

    def _format_bytes(self, bytes_size: int) -> str:
        """格式化字节大小"""
        for unit in ['B', 'KB', 'MB', 'GB', 'TB']:
            if bytes_size < 1024.0:
                return f"{bytes_size:.2f} {unit}"
            bytes_size /= 1024.0
        return f"{bytes_size:.2f} PB"

    async def get_task_status(self, task_id: int) -> Optional[Dict]:
        """获取任务状态"""
        try:
            async for db in get_db():
                # 这里应该查询数据库获取任务状态
                # 暂时返回当前任务信息
                if self._current_task and self._current_task.id == task_id:
                    return {
                        'task_id': task_id,
                        'status': self._current_task.status.value,
                        'progress_percent': self._current_task.progress_percent,
                        'processed_files': self._current_task.processed_files,
                        'total_files': self._current_task.total_files,
                        'processed_bytes': self._current_task.processed_bytes,
                        'total_bytes': self._current_task.total_bytes
                    }
            return None
        except Exception as e:
            logger.error(f"获取任务状态失败: {str(e)}")
            return None

    async def cancel_task(self, task_id: int) -> bool:
        """取消任务"""
        try:
            if self._current_task and self._current_task.id == task_id:
                await self._update_task_status(self._current_task, BackupTaskStatus.CANCELLED)
                self._current_task = None
                logger.info(f"任务已取消: {task_id}")
                return True
            return False
        except Exception as e:
            logger.error(f"取消任务失败: {str(e)}")
            return False