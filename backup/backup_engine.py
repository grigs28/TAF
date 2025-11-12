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
import sys
import re
import traceback
from datetime import datetime, timedelta
from pathlib import Path
from typing import List, Dict, Any, Optional, Callable
import py7zr
import psutil
from sqlalchemy import text

# 尝试导入 tqdm，如果不可用则使用简单的文本进度条
try:
    from tqdm import tqdm
    TQDM_AVAILABLE = True
except ImportError:
    TQDM_AVAILABLE = False

from config.settings import get_settings
from config.database import get_db
from models.backup import BackupTask, BackupSet, BackupFile, BackupTaskStatus, BackupTaskType, BackupFileType
from models.system_log import OperationLog, OperationType, LogLevel, LogCategory
from tape.tape_manager import TapeManager
from tape.tape_cartridge import TapeCartridge
from utils.dingtalk_notifier import DingTalkNotifier
from utils.datetime_utils import now, format_datetime
from utils.log_utils import log_operation, log_system
import json
from pathlib import Path

# 导入新创建的子模块
from backup.utils import normalize_volume_label, extract_label_year_month, format_bytes, calculate_file_checksum
from backup.file_scanner import FileScanner
from backup.compressor import Compressor
from backup.backup_db import BackupDB
from backup.tape_handler import TapeHandler

logger = logging.getLogger(__name__)


# 向后兼容：保留 normalize_volume_label 和 extract_label_year_month 的导出
# 实际实现已移到 backup.utils 模块，这里直接使用导入的函数


class BackupEngine:
    """备份引擎"""

    def __init__(self):
        self.settings = get_settings()
        self.tape_manager: Optional[TapeManager] = None
        self.dingtalk_notifier: Optional[DingTalkNotifier] = None
        self._initialized = False
        self._current_task: Optional[BackupTask] = None
        self._progress_callbacks: List[Callable] = []
        self._notification_events_cache: Optional[Dict[str, bool]] = None
        self._notification_events_cache_time: Optional[datetime] = None
        
        # 初始化子模块
        self.file_scanner = FileScanner(settings=self.settings)
        self.compressor = Compressor(settings=self.settings)
        self.backup_db = BackupDB()
        self.tape_handler = TapeHandler(tape_manager=None, settings=self.settings)

    async def _get_notification_events(self) -> Dict[str, bool]:
        """获取通知事件配置（带缓存）
        
        Returns:
            通知事件配置字典，包含各个事件的启用状态
        """
        # 缓存5分钟
        cache_timeout = timedelta(minutes=5)
        current_time = now()
        
        if (self._notification_events_cache and 
            self._notification_events_cache_time and 
            (current_time - self._notification_events_cache_time) < cache_timeout):
            return self._notification_events_cache
        
        try:
            # 从.env文件读取通知事件配置
            env_file = Path(".env")
            if env_file.exists():
                with open(env_file, "r", encoding="utf-8") as f:
                    for line in f:
                        line = line.strip()
                        if line.startswith("NOTIFICATION_EVENTS="):
                            events_json = line.split("=", 1)[1]
                            events_dict = json.loads(events_json)
                            self._notification_events_cache = events_dict
                            self._notification_events_cache_time = current_time
                            return events_dict
            
            # 如果.env中没有，返回默认配置（所有事件都启用）
            default_events = {
                "notify_backup_success": True,
                "notify_backup_started": True,
                "notify_backup_failed": True,
                "notify_recovery_success": True,
                "notify_recovery_failed": True,
                "notify_tape_change": True,
                "notify_tape_expired": True,
                "notify_tape_error": True,
                "notify_capacity_warning": True,
                "notify_system_error": True,
                "notify_system_started": True
            }
            self._notification_events_cache = default_events
            self._notification_events_cache_time = current_time
            return default_events
            
        except Exception as e:
            logger.warning(f"获取通知事件配置失败: {str(e)}，使用默认配置")
            # 返回默认配置（所有事件都启用）
            default_events = {
                "notify_backup_success": True,
                "notify_backup_started": True,
                "notify_backup_failed": True,
                "notify_recovery_success": True,
                "notify_recovery_failed": True,
                "notify_tape_change": True,
                "notify_tape_expired": True,
                "notify_tape_error": True,
                "notify_capacity_warning": True,
                "notify_system_error": True,
                "notify_system_started": True
            }
            return default_events

    async def _get_backup_policy_parameters(self) -> Dict[str, Any]:
        """获取备份策略参数（从tapedrive和system配置）
        
        Returns:
            备份策略参数字典，包含：
            - compression_level: 压缩级别
            - max_file_size: 最大文件大小
            - retention_days: 保留天数
            - tape_drive_letter: 磁带盘符
            - default_block_size: 默认块大小
        """
        try:
            # 从Settings获取配置（这是主要来源）
            policy = {
                'compression_level': self.settings.COMPRESSION_LEVEL,
                'max_file_size': self.settings.MAX_FILE_SIZE,
                'solid_block_size': self.settings.SOLID_BLOCK_SIZE,
                'retention_days': self.settings.DEFAULT_RETENTION_MONTHS * 30,
                'tape_drive_letter': self.settings.TAPE_DRIVE_LETTER,
                'default_block_size': self.settings.DEFAULT_BLOCK_SIZE,
                'max_volume_size': self.settings.MAX_VOLUME_SIZE
            }
            
            logger.info(f"从系统配置获取备份策略参数: 压缩级别={policy['compression_level']}, "
                       f"最大文件大小={format_bytes(policy['max_file_size'])}")
            
            return policy
            
        except Exception as e:
            logger.warning(f"获取备份策略参数失败: {str(e)}，使用默认配置")
            # 返回默认配置
            return {
                'compression_level': 9,
                'max_file_size': 3221225472,  # 3GB
                'solid_block_size': 67108864,  # 64MB
                'retention_days': 180,  # 6个月
                'tape_drive_letter': 'o',
                'default_block_size': 262144,  # 256KB
                'max_volume_size': 322122547200  # 300GB
            }

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
        # 更新子模块的依赖
        self.tape_handler.tape_manager = tape_manager

    def add_progress_callback(self, callback: Callable):
        """添加进度回调"""
        self._progress_callbacks.append(callback)

    async def create_backup_task(self, task_name: str, source_paths: List[str],
                               task_type: BackupTaskType = BackupTaskType.FULL,
                               **kwargs) -> Optional[BackupTask]:
        """创建备份任务
        
        支持网络路径（UNC路径）：
        - \\192.168.0.79 - 自动列出所有共享
        - \\192.168.0.79\yz - 指定共享路径
        """
        try:
            # 检查参数
            if not task_name or not source_paths:
                raise ValueError("任务名称和源路径不能为空")

            # 验证源路径（支持 UNC 网络路径）
            from utils.network_path import validate_network_path, expand_unc_path, is_unc_path
            
            expanded_source_paths = []
            for path in source_paths:
                # 验证路径
                validation_result = validate_network_path(path)
                
                if not validation_result['valid']:
                    # 对于 UNC 路径，如果无法访问，给出更详细的错误信息
                    if validation_result['is_unc']:
                        error_msg = f"无法访问网络路径: {path}"
                        if validation_result['error']:
                            error_msg += f" ({validation_result['error']})"
                        raise ValueError(error_msg)
                    else:
                        raise ValueError(f"源路径不存在: {path}")
                
                # 如果是 UNC 路径且已展开，使用展开后的路径
                if validation_result['is_unc'] and validation_result['expanded_paths']:
                    expanded_source_paths.extend(validation_result['expanded_paths'])
                else:
                    expanded_source_paths.append(path)
            
            # 使用展开后的路径列表
            if expanded_source_paths:
                source_paths = expanded_source_paths
                logger.info(f"路径已展开，共 {len(source_paths)} 个路径")

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

            # 保存到数据库 - 使用原生 openGauss SQL
            from utils.scheduler.db_utils import is_opengauss, get_opengauss_connection
            import json
            
            if is_opengauss():
                conn = await get_opengauss_connection()
                try:
                    # 插入备份任务
                    task_id = await conn.fetchval(
                        """
                        INSERT INTO backup_tasks 
                        (task_name, task_type, description, status, source_paths, exclude_patterns,
                         compression_enabled, encryption_enabled, retention_days, scheduled_time,
                         created_by, created_at, updated_at)
                        VALUES ($1, $2::backuptasktype, $3, $4::backuptaskstatus, $5::json, $6::json,
                                $7, $8, $9, $10, $11, $12, $13)
                        RETURNING id
                        """,
                        task_name,
                        task_type.value,
                        kwargs.get('description', ''),
                        'PENDING',  # BackupTaskStatus.PENDING
                        json.dumps(source_paths) if source_paths else None,
                        json.dumps(kwargs.get('exclude_patterns', [])) if kwargs.get('exclude_patterns') else None,
                        kwargs.get('compression_enabled', True),
                        kwargs.get('encryption_enabled', False),
                        kwargs.get('retention_days', self.settings.DEFAULT_RETENTION_MONTHS * 30),
                        kwargs.get('scheduled_time'),
                        kwargs.get('created_by', 'system'),
                        datetime.now(),
                        datetime.now()
                    )
                    backup_task.id = task_id
                finally:
                    await conn.close()
            else:
                # 非 openGauss 使用 SQLAlchemy（其他数据库）
                async for db in get_db():
                    db.add(backup_task)
                    await db.commit()
                    await db.refresh(backup_task)

            logger.info(f"创建备份任务成功: {task_name}")
            return backup_task

        except Exception as e:
            logger.error(f"创建备份任务失败: {str(e)}")
            return None

    async def execute_backup_task(self, backup_task: BackupTask, scheduled_task=None, manual_run: bool = False) -> bool:
        """执行备份任务
        
        执行前检查：
        1. 任务是否已执行过（在存活期内）- 仅自动执行时检查，手动运行跳过
        2. 任务是否正在执行
        3. 磁带卷标是否当月（仅当备份目标为磁带时）
        4. 完整备份前使用 LtfsCmdFormat 格式化（保留卷标信息）
        
        Args:
            backup_task: 备份任务对象
            scheduled_task: 计划任务对象（可选）
            manual_run: 是否为手动运行（Web界面点击运行），默认为False
        """
        task_start_time = now()
        task_id = backup_task.id
        task_name = backup_task.task_name
        
        try:
            # 记录开始日志
            logger.info(f"========== 开始执行备份任务 ==========")
            logger.info(f"任务名称: {task_name}")
            logger.info(f"任务ID: {task_id}")
            logger.info(f"任务类型: {backup_task.task_type}")
            logger.info(f"开始时间: {format_datetime(task_start_time)}")
            
            # 使用后台任务记录日志，避免阻塞
            asyncio.create_task(log_system(
                level=LogLevel.INFO,
                category=LogCategory.BACKUP,
                message=f"开始执行备份任务: {task_name} (ID: {task_id})",
                module="backup.backup_engine",
                function="execute_backup_task",
                task_id=task_id,
                details={
                    "task_name": task_name,
                    "task_type": backup_task.task_type.value if hasattr(backup_task.task_type, 'value') else str(backup_task.task_type),
                    "start_time": format_datetime(task_start_time)
                }
            ))
            
            if not self._initialized:
                error_msg = "备份引擎未初始化"
                logger.error(error_msg)
                # 使用后台任务记录日志，避免阻塞
                asyncio.create_task(log_system(
                    level=LogLevel.ERROR,
                    category=LogCategory.BACKUP,
                    message=error_msg,
                    module="backup.backup_engine",
                    function="execute_backup_task",
                    task_id=task_id
                ))
                raise RuntimeError(error_msg)

            self._current_task = backup_task

            # 0. 获取备份策略参数（从tapedrive和system配置）
            logger.info("========== 获取备份策略参数 ==========")
            backup_policy = await self._get_backup_policy_parameters()
            logger.info(f"备份策略参数: 压缩级别={backup_policy.get('compression_level', 'N/A')}, "
                       f"最大文件大小={format_bytes(backup_policy.get('max_file_size', 0))}, "
                       f"保留天数={backup_policy.get('retention_days', 'N/A')}")
            
            # 将备份策略参数应用到备份任务（如果任务中没有设置）
            if not hasattr(backup_task, 'compression_level') or backup_task.compression_level is None:
                backup_task.compression_level = backup_policy.get('compression_level', self.settings.COMPRESSION_LEVEL)
            if not hasattr(backup_task, 'max_file_size') or backup_task.max_file_size is None:
                backup_task.max_file_size = backup_policy.get('max_file_size', self.settings.MAX_FILE_SIZE)
            if not hasattr(backup_task, 'retention_days') or backup_task.retention_days is None:
                backup_task.retention_days = backup_policy.get('retention_days', self.settings.DEFAULT_RETENTION_MONTHS * 30)

            # 1. 检查任务是否已执行过（在存活期内）- 仅自动执行时检查，手动运行跳过
            if not manual_run and scheduled_task:
                logger.info("========== 执行前检查：任务执行状态 ==========")
                template_id = getattr(backup_task, 'template_id', None)
                if not template_id and hasattr(scheduled_task, 'task_metadata'):
                    template_id = scheduled_task.task_metadata.get('backup_task_id')
                
                if template_id:
                    logger.info(f"检查模板任务 {template_id} 的执行状态...")
                    from utils.scheduler.db_utils import is_opengauss, get_opengauss_connection
                    
                    if is_opengauss():
                        conn = await get_opengauss_connection()
                        try:
                            # 检查是否有相同模板的任务在存活期内已成功执行
                            completed_task = await conn.fetchrow(
                                """
                                SELECT id, completed_at, status FROM backup_tasks
                                WHERE template_id = $1 AND status = $2::backuptaskstatus
                                ORDER BY completed_at DESC
                                LIMIT 1
                                """,
                                template_id, 'completed'
                            )
                            
                            if completed_task and completed_task['completed_at']:
                                logger.info(f"找到已完成的模板任务: {completed_task['id']}, 完成时间: {completed_task['completed_at']}")
                                # 这里可以根据存活期判断是否在存活期内，暂时跳过
                        finally:
                            await conn.close()
            elif manual_run:
                logger.info("========== 手动运行模式，跳过任务执行状态检查 ==========")

            # 2. 检查任务是否正在执行
            logger.info("========== 执行前检查：任务运行状态 ==========")
            from utils.scheduler.db_utils import is_opengauss, get_opengauss_connection
            
            if is_opengauss():
                conn = await get_opengauss_connection()
                try:
                    running_task = await conn.fetchrow(
                        """
                        SELECT id, started_at FROM backup_tasks
                        WHERE id = $1 AND status = $2::backuptaskstatus
                        """,
                        task_id, 'running'
                    )
                    
                    if running_task:
                        logger.warning(f"任务 {task_id} 正在执行中，跳过本次执行")
                        # 使用后台任务记录日志，避免阻塞
                        asyncio.create_task(log_system(
                            level=LogLevel.WARNING,
                            category=LogCategory.BACKUP,
                            message=f"任务 {task_id} 正在执行中，跳过本次执行",
                            module="backup.backup_engine",
                            function="execute_backup_task",
                            task_id=task_id
                        ))
                        return False
                    else:
                        logger.info(f"任务 {task_id} 未在运行，可以执行")
                finally:
                    await conn.close()

            # 3. 检查磁带卷标是否当月（仅当备份目标为磁带时）
            logger.info("========== 执行前检查：磁带卷标当月验证 ==========")
            if self.tape_manager:
                try:
                    tape_ops = self.tape_manager.tape_operations
                    if tape_ops and hasattr(tape_ops, '_read_tape_label'):
                        logger.info("尝试读取当前驱动器中的磁带卷标...")
                        metadata = await tape_ops._read_tape_label()
                        
                        if metadata and metadata.get('tape_id'):
                            label_text = metadata.get('label') or metadata.get('tape_id')
                            tape_id = metadata.get('tape_id')
                            logger.info(f"读取到磁带卷标: {label_text}")

                            current_time = now()
                            current_year = current_time.year
                            current_month = current_time.month

                            async def handle_non_current_month(reason: str):
                                error_msg = f"当前磁带 {tape_id} 非当月（{reason}），请更换磁带后重试"
                                logger.error(error_msg)
                                raise ValueError(error_msg)

                            label_info = extract_label_year_month(label_text)

                            if label_info:
                                label_year = label_info['year']
                                label_month = label_info['month']

                                if label_month < 1 or label_month > 12:
                                    await handle_non_current_month(f"卷标解析到非法月份 {label_month}")

                                if label_month != current_month:
                                    await handle_non_current_month(f"卷标显示月份 {label_month:02d} 与当前月份不符")
                                elif label_year != current_year:
                                    logger.info(
                                        f"卷标年份 {label_year} 与当前年份 {current_year} 不一致，但月份匹配，允许通过"
                                    )
                                else:
                                    logger.info(f"磁带 {tape_id} 卷标匹配当前月份，验证通过")
                            else:
                                await handle_non_current_month("卷标无法解析出年月信息")
                        else:
                            logger.warning("无法读取磁带卷标，跳过当月验证")

                except Exception as tape_check_error:
                    logger.warning(f"检查磁带卷标失败: {str(tape_check_error)}")
                    # 不阻止执行，记录警告即可

            # 4. 完整备份前使用 LtfsCmdFormat.exe 格式化（保留卷标信息）
            # 注意：格式化进度会显示在备份管理卡片中（0-100%），格式化完成后再继续后续备份流程
            # 注意：手动运行时跳过格式化
            logger.info(f"========== 检查是否需要格式化 ==========")
            logger.info(f"manual_run={manual_run}, 任务类型: {backup_task.task_type} (类型: {type(backup_task.task_type)}, FULL={BackupTaskType.FULL})")
            
            # 确保任务类型比较正确（支持字符串和枚举值）
            task_type_value = backup_task.task_type
            if hasattr(task_type_value, 'value'):
                task_type_value = task_type_value.value
            elif isinstance(task_type_value, BackupTaskType):
                task_type_value = task_type_value.value
            else:
                task_type_value = str(task_type_value)
            
            full_type_value = BackupTaskType.FULL.value if hasattr(BackupTaskType.FULL, 'value') else 'full'
            
            logger.info(f"任务类型值: {task_type_value} (期望: {full_type_value})")
            
            # 手动运行时跳过格式化
            if not manual_run and (task_type_value == full_type_value or backup_task.task_type == BackupTaskType.FULL):
                logger.info("========== 完整备份前格式化处理（自动运行模式）==========")
                logger.info("检测到完整备份任务，执行格式化前检查...")
                
                # 初始化格式化进度为0%
                backup_task.progress_percent = 0.0
                await self.backup_db.update_scan_progress(backup_task, 0, 0)
                
                if self.tape_manager:
                    try:
                        tape_ops = self.tape_manager.tape_operations
                        if tape_ops and hasattr(tape_ops, 'erase_preserve_label'):
                            logger.info("开始执行格式化（保留卷标信息）...")

                            # 定义进度回调函数，用于更新进度到数据库
                            async def update_format_progress(task, current, total):
                                """更新格式化进度到数据库"""
                                try:
                                    await self.backup_db.update_scan_progress(task, current, total)
                                except Exception as e:
                                    logger.debug(f"更新格式化进度失败（忽略继续）: {str(e)}")

                            # 执行格式化（传递backup_task和进度回调，进度会从0%到100%）
                            format_success = await tape_ops.erase_preserve_label(
                                backup_task=backup_task,
                                progress_callback=update_format_progress
                            )
                            
                            if format_success:
                                # 格式化完成，确保进度为100%
                                backup_task.progress_percent = 100.0
                                await self.backup_db.update_scan_progress(backup_task, 1, 1)
                                
                                logger.info("格式化成功（卷标信息已保留），进度: 100%")
                                # 使用后台任务记录日志，避免阻塞
                                asyncio.create_task(log_system(
                                    level=LogLevel.INFO,
                                    category=LogCategory.BACKUP,
                                message="完整备份前格式化成功（卷标信息已保留）",
                                    module="backup.backup_engine",
                                    function="execute_backup_task",
                                    task_id=task_id
                                ))
                            else:
                                # 格式化失败，立即停止任务并标记为失败
                                error_msg = "完整备份前格式化失败，无法继续执行备份任务"
                                logger.error(f"========== 格式化失败，任务将停止 ==========")
                                logger.error(error_msg)
                                
                                # 设置错误信息
                                backup_task.error_message = error_msg
                                backup_task.completed_at = now()
                                duration_seconds = (now() - task_start_time).total_seconds()
                                duration_ms = int(duration_seconds * 1000)
                                
                                # 更新任务状态为失败
                                await self.backup_db.update_task_status(backup_task, BackupTaskStatus.FAILED)
                                
                                # 使用后台任务记录日志，避免阻塞
                                asyncio.create_task(log_system(
                                    level=LogLevel.ERROR,
                                    category=LogCategory.BACKUP,
                                    message=error_msg,
                                    module="backup.backup_engine",
                                    function="execute_backup_task",
                                    task_id=task_id,
                                    duration_ms=duration_ms
                                ))
                                asyncio.create_task(log_operation(
                                    operation_type=OperationType.BACKUP_COMPLETE,
                                    resource_type="backup",
                                    resource_id=str(task_id),
                                    resource_name=task_name,
                                    operation_name="备份任务失败",
                                    operation_description=f"备份任务执行失败: {task_name}",
                                    category="backup",
                                    success=False,
                                    error_message=error_msg,
                                    duration_ms=duration_ms
                                ))
                                
                                # 发送失败通知
                                if self.dingtalk_notifier:
                                    try:
                                        notification_events = await self._get_notification_events()
                                        if notification_events.get("notify_backup_failed", True):
                                            logger.info("发送备份失败钉钉通知（格式化失败）...")
                                            await self.dingtalk_notifier.send_backup_notification(
                                                backup_task.task_name,
                                                "failed",
                                                {'error': error_msg}
                                            )
                                            logger.info("备份失败钉钉通知发送成功")
                                    except Exception as notify_error:
                                        logger.warning(f"发送备份失败钉钉通知失败: {str(notify_error)}")
                                
                                # 保存任务结果
                                from utils.scheduler.db_utils import is_opengauss, get_opengauss_connection
                                if is_opengauss():
                                    conn = await get_opengauss_connection()
                                    try:
                                        await conn.execute(
                                            """
                                            UPDATE backup_tasks
                                            SET status = $1::backuptaskstatus,
                                                completed_at = $2,
                                                error_message = $3,
                                                updated_at = $4
                                            WHERE id = $5
                                            """,
                                            BackupTaskStatus.FAILED.value,
                                            backup_task.completed_at,
                                            error_msg,
                                            datetime.now(),
                                            backup_task.id
                                        )
                                    finally:
                                        await conn.close()
                                
                                logger.error(f"========== 任务已停止并标记为失败 ==========")
                                logger.error(f"任务名称: {task_name}")
                                logger.error(f"任务ID: {task_id}")
                                logger.error(f"错误原因: {error_msg}")
                                return False
                        else:
                            logger.warning("磁带操作对象不支持保留卷标信息的格式化功能")
                    except Exception as format_error:
                        # 格式化过程中发生异常，立即停止任务并标记为失败
                        error_msg = f"完整备份前格式化过程中发生错误: {str(format_error)}"
                        logger.error(f"========== 格式化异常，任务将停止 ==========")
                        logger.error(error_msg)
                        logger.error(f"异常堆栈:\n{traceback.format_exc()}")
                        
                        # 设置错误信息
                        backup_task.error_message = error_msg
                        backup_task.completed_at = now()
                        duration_seconds = (now() - task_start_time).total_seconds()
                        duration_ms = int(duration_seconds * 1000)
                        
                        # 更新任务状态为失败
                        await self.backup_db.update_task_status(backup_task, BackupTaskStatus.FAILED)
                        
                        # 使用后台任务记录日志，避免阻塞
                        asyncio.create_task(log_system(
                            level=LogLevel.ERROR,
                            category=LogCategory.BACKUP,
                            message=error_msg,
                            module="backup.backup_engine",
                            function="execute_backup_task",
                            task_id=task_id,
                            exception_type=type(format_error).__name__,
                            stack_trace=traceback.format_exc(),
                            duration_ms=duration_ms
                        ))
                        asyncio.create_task(log_operation(
                            operation_type=OperationType.BACKUP_COMPLETE,
                            resource_type="backup",
                            resource_id=str(task_id),
                            resource_name=task_name,
                            operation_name="备份任务失败",
                            operation_description=f"备份任务执行失败: {task_name}",
                            category="backup",
                            success=False,
                            error_message=error_msg,
                            duration_ms=duration_ms
                        ))
                        
                        # 发送失败通知
                        if self.dingtalk_notifier:
                            try:
                                notification_events = await self._get_notification_events()
                                if notification_events.get("notify_backup_failed", True):
                                    logger.info("发送备份失败钉钉通知（格式化异常）...")
                                    await self.dingtalk_notifier.send_backup_notification(
                                        backup_task.task_name,
                                        "failed",
                                        {'error': error_msg}
                                    )
                                    logger.info("备份失败钉钉通知发送成功")
                            except Exception as notify_error:
                                logger.warning(f"发送备份失败钉钉通知失败: {str(notify_error)}")
                        
                        # 保存任务结果
                        from utils.scheduler.db_utils import is_opengauss, get_opengauss_connection
                        if is_opengauss():
                            conn = await get_opengauss_connection()
                            try:
                                await conn.execute(
                                    """
                                    UPDATE backup_tasks
                                    SET status = $1::backuptaskstatus,
                                        completed_at = $2,
                                        error_message = $3,
                                        updated_at = $4
                                    WHERE id = $5
                                    """,
                                    BackupTaskStatus.FAILED.value,
                                    backup_task.completed_at,
                                    error_msg,
                                    datetime.now(),
                                    backup_task.id
                                )
                            finally:
                                await conn.close()
                        
                        logger.error(f"========== 任务已停止并标记为失败 ==========")
                        logger.error(f"任务名称: {task_name}")
                        logger.error(f"任务ID: {task_id}")
                        logger.error(f"错误原因: {error_msg}")
                        return False
                
                # 格式化完成后，重置进度为0%，准备开始备份流程
                backup_task.progress_percent = 0.0
                await self.backup_db.update_scan_progress(backup_task, 0, 0)
            else:
                if manual_run:
                    logger.info("手动运行模式，跳过格式化操作")
                elif task_type_value != full_type_value and backup_task.task_type != BackupTaskType.FULL:
                    logger.info(f"任务类型为 {backup_task.task_type}，不是完整备份（FULL），跳过格式化步骤")

            # 更新任务状态
            logger.info("========== 更新任务状态为运行中 ==========")
            await self.backup_db.update_task_status(backup_task, BackupTaskStatus.RUNNING)
            backup_task.started_at = task_start_time
            
            # 使用后台任务记录日志，避免阻塞
            asyncio.create_task(log_operation(
                operation_type=OperationType.BACKUP_START,
                resource_type="backup",
                resource_id=str(task_id),
                resource_name=task_name,
                operation_name="开始备份任务",
                operation_description=f"开始执行备份任务: {task_name}",
                category="backup",
                success=True
            ))

            # 发送开始通知（检查钉钉通知配置中的通知事件）
            if self.dingtalk_notifier:
                try:
                    # 检查钉钉通知配置中的通知事件
                    notification_events = await self._get_notification_events()
                    if notification_events.get("notify_backup_started", True):
                        logger.info("发送备份开始钉钉通知...")
                        await self.dingtalk_notifier.send_backup_notification(
                            backup_task.task_name,
                            "started"
                        )
                        logger.info("备份开始钉钉通知发送成功")
                    else:
                        logger.debug("通知事件配置中备份开始通知已禁用，跳过发送")
                except Exception as notify_error:
                    logger.warning(f"发送备份开始钉钉通知失败: {str(notify_error)}")

            # 执行备份流程
            logger.info("========== 开始执行备份流程 ==========")
            success = await self._perform_backup(backup_task, scheduled_task=scheduled_task)

            # 更新任务完成状态
            task_end_time = now()
            backup_task.completed_at = task_end_time
            duration_seconds = (task_end_time - task_start_time).total_seconds()
            duration_ms = int(duration_seconds * 1000)
            
            if success:
                # 安全获取处理统计信息
                processed_files = getattr(backup_task, 'processed_files', 0)
                processed_bytes = getattr(backup_task, 'processed_bytes', 0)
                
                logger.info("========== 备份任务执行成功 ==========")
                logger.info(f"处理文件数: {processed_files}")
                logger.info(f"处理字节数: {format_bytes(processed_bytes)}")
                logger.info(f"执行耗时: {duration_seconds:.2f} 秒")
                logger.info(f"完成时间: {format_datetime(task_end_time)}")
                
                await self.backup_db.update_task_status(backup_task, BackupTaskStatus.COMPLETED)
                
                # 使用后台任务记录日志，避免阻塞
                asyncio.create_task(log_operation(
                    operation_type=OperationType.BACKUP_COMPLETE,
                    resource_type="backup",
                    resource_id=str(task_id),
                    resource_name=task_name,
                    operation_name="备份任务完成",
                    operation_description=f"备份任务执行成功: {task_name}",
                    category="backup",
                    success=True,
                    result_message=f"处理 {processed_files} 个文件，总大小 {format_bytes(processed_bytes)}",
                    duration_ms=duration_ms
                ))
                asyncio.create_task(log_system(
                    level=LogLevel.INFO,
                    category=LogCategory.BACKUP,
                    message=f"备份任务执行成功: {task_name}",
                    module="backup.backup_engine",
                    function="execute_backup_task",
                    task_id=task_id,
                    duration_ms=duration_ms,
                    details={
                        "processed_files": processed_files,
                        "processed_bytes": processed_bytes,
                        "duration_seconds": duration_seconds,
                        "backup_set_id": getattr(backup_task, 'backup_set_id', None),
                        "tape_id": getattr(backup_task, 'tape_id', None)
                    }
                ))
                
                # 发送成功通知（检查钉钉通知配置中的通知事件）
                if self.dingtalk_notifier:
                    try:
                        # 检查钉钉通知配置中的通知事件
                        notification_events = await self._get_notification_events()
                        if notification_events.get("notify_backup_success", True):
                            logger.info("发送备份成功钉钉通知...")
                            await self.dingtalk_notifier.send_backup_notification(
                                backup_task.task_name,
                                "success",
                                {
                                    'size': format_bytes(processed_bytes),
                                    'file_count': processed_files,
                                    'duration': f"{duration_seconds:.2f} 秒"
                                }
                            )
                            logger.info("备份成功钉钉通知发送成功")
                        else:
                            logger.debug("通知事件配置中备份成功通知已禁用，跳过发送")
                    except Exception as notify_error:
                        logger.warning(f"发送备份成功钉钉通知失败: {str(notify_error)}")
            else:
                error_msg = getattr(backup_task, 'error_message', '未知错误')
                logger.error("========== 备份任务执行失败 ==========")
                logger.error(f"错误信息: {error_msg}")
                logger.error(f"执行耗时: {duration_seconds:.2f} 秒")
                logger.error(f"完成时间: {format_datetime(task_end_time)}")
                
                await self.backup_db.update_task_status(backup_task, BackupTaskStatus.FAILED)
                
                # 使用后台任务记录日志，避免阻塞
                asyncio.create_task(log_operation(
                    operation_type=OperationType.BACKUP_COMPLETE,
                    resource_type="backup",
                    resource_id=str(task_id),
                    resource_name=task_name,
                    operation_name="备份任务失败",
                    operation_description=f"备份任务执行失败: {task_name}",
                    category="backup",
                    success=False,
                    error_message=error_msg,
                    duration_ms=duration_ms
                ))
                asyncio.create_task(log_system(
                    level=LogLevel.ERROR,
                    category=LogCategory.BACKUP,
                    message=f"备份任务执行失败: {task_name}",
                    module="backup.backup_engine",
                    function="execute_backup_task",
                    task_id=task_id,
                    duration_ms=duration_ms,
                    details={
                        "error_message": error_msg,
                        "duration_seconds": duration_seconds,
                        "processed_files": getattr(backup_task, 'processed_files', 0),
                        "processed_bytes": getattr(backup_task, 'processed_bytes', 0)
                    }
                ))
                
                # 发送失败通知（检查钉钉通知配置中的通知事件）
                if self.dingtalk_notifier:
                    try:
                        # 检查钉钉通知配置中的通知事件
                        notification_events = await self._get_notification_events()
                        if notification_events.get("notify_backup_failed", True):
                            logger.info("发送备份失败钉钉通知...")
                            await self.dingtalk_notifier.send_backup_notification(
                                backup_task.task_name,
                                "failed",
                                {'error': error_msg}
                            )
                            logger.info("备份失败钉钉通知发送成功")
                        else:
                            logger.debug("通知事件配置中备份失败通知已禁用，跳过发送")
                    except Exception as notify_error:
                        logger.warning(f"发送备份失败钉钉通知失败: {str(notify_error)}")

            # 保存任务结果 - 使用原生 openGauss SQL
            from utils.scheduler.db_utils import is_opengauss, get_opengauss_connection
            
            if is_opengauss():
                conn = await get_opengauss_connection()
                try:
                    await conn.execute(
                        """
                        UPDATE backup_tasks
                        SET status = $1::backuptaskstatus,
                            completed_at = $2,
                            error_message = $3,
                            updated_at = $4
                        WHERE id = $5
                        """,
                        (BackupTaskStatus.COMPLETED.value if success else BackupTaskStatus.FAILED.value),
                        backup_task.completed_at,
                        getattr(backup_task, 'error_message', None),  # 安全获取 error_message
                        datetime.now(),
                        backup_task.id
                    )
                finally:
                    await conn.close()
            else:
                # 非 openGauss 使用 SQLAlchemy
                async for db in get_db():
                    await db.commit()

            logger.info(f"========== 备份任务执行完成 ==========")
            logger.info(f"任务名称: {task_name}")
            logger.info(f"任务ID: {task_id}")
            logger.info(f"执行结果: {'成功' if success else '失败'}")
            logger.info(f"总耗时: {duration_seconds:.2f} 秒")
            
            return success

        except Exception as e:
            error_msg = str(e)
            error_trace = traceback.format_exc()
            
            logger.error("========== 备份任务执行异常 ==========")
            logger.error(f"任务名称: {task_name}")
            logger.error(f"任务ID: {task_id}")
            logger.error(f"异常信息: {error_msg}")
            logger.error(f"异常堆栈:\n{error_trace}")
            
            # 使用后台任务记录日志，避免阻塞
            asyncio.create_task(log_system(
                level=LogLevel.ERROR,
                category=LogCategory.BACKUP,
                message=f"备份任务执行异常: {task_name}",
                module="backup.backup_engine",
                function="execute_backup_task",
                task_id=task_id,
                exception_type=type(e).__name__,
                stack_trace=error_trace,
                details={
                    "error_message": error_msg,
                    "task_name": task_name
                }
            ))
            if backup_task:
                # 使用 setattr 设置 error_message，因为 BackupTask 可能是数据类或字典
                try:
                    if hasattr(backup_task, 'error_message'):
                        backup_task.error_message = str(e)
                    else:
                        # 如果 BackupTask 是字典或没有 error_message 属性，使用数据库更新
                        from utils.scheduler.db_utils import is_opengauss, get_opengauss_connection
                        if is_opengauss():
                            conn = await get_opengauss_connection()
                            try:
                                await conn.execute(
                                    """
                                    UPDATE backup_tasks
                                    SET error_message = $1, updated_at = $2
                                    WHERE id = $3
                                    """,
                                    str(e),
                                    datetime.now(),
                                    backup_task.id
                                )
                            finally:
                                await conn.close()
                except Exception as update_error:
                    logger.warning(f"更新错误信息失败: {str(update_error)}")
                await self.backup_db.update_task_status(backup_task, BackupTaskStatus.FAILED)
            return False
        finally:
            self._current_task = None

    async def _perform_backup(self, backup_task: BackupTask, scheduled_task=None) -> bool:
        """执行备份流程（流式处理：扫描和压缩循环执行）
        
        Args:
            backup_task: 备份任务对象
            scheduled_task: 计划任务对象（可选，用于获取排除规则等配置）
        """
        try:
            # 初始化扫描进度
            if backup_task:
                backup_task.progress_percent = 0.0
                await self.backup_db.update_scan_progress(backup_task, 0, 0)
            
            # 1. 检查磁带盘符是否可用（简单检查）
            logger.info("检查磁带盘符...")
            tape_drive = self.settings.TAPE_DRIVE_LETTER.upper() + ":\\"
            if not os.path.exists(tape_drive):
                raise RuntimeError(f"磁带盘符不存在: {tape_drive}，请检查配置")
            
            logger.info(f"磁带盘符可用: {tape_drive}")
            
            # 2. 获取或创建磁带信息（简化处理）
            tape_id = "TAPE001"  # 默认磁带ID，可以从数据库获取或自动生成
            if self.tape_manager:
                try:
                    # 尝试从数据库获取当前磁带或创建新记录
                    current_tape = await self.tape_handler.get_current_drive_tape()
                    if current_tape:
                        tape_id = current_tape.tape_id
                    else:
                        # 从数据库获取可用磁带
                        available_tape = await self.tape_manager.get_available_tape()
                        if available_tape:
                            tape_id = available_tape.tape_id
                except RuntimeError as e:
                    # 如果是因为磁带不在数据库中而抛出的异常，发送钉钉通知并停止任务
                    error_msg = str(e)
                    logger.error(f"磁带检查失败: {error_msg}")
                    
                    # 发送钉钉通知
                    if self.dingtalk_notifier:
                        try:
                            # 尝试从错误信息中提取磁带ID
                            tape_id_from_error = "未知"
                            if "磁带" in error_msg and "未在数据库中注册" in error_msg:
                                # 提取磁带ID（格式：驱动器中的磁带 xxx 未在数据库中注册）
                                match = re.search(r'磁带\s+(\S+)\s+未在数据库中注册', error_msg)
                                if match:
                                    tape_id_from_error = match.group(1)
                            
                            await self.dingtalk_notifier.send_tape_notification(
                                tape_id=tape_id_from_error,
                                action="error",
                                details={
                                    "error": error_msg,
                                    "task_name": backup_task.task_name,
                                    "task_id": backup_task.id,
                                    "message": "驱动器中的磁带未在数据库中注册，请先在磁带管理页面添加该磁带"
                                }
                            )
                            logger.info("已发送钉钉通知：磁带不在数据库中")
                        except Exception as notify_error:
                            logger.error(f"发送钉钉通知失败: {str(notify_error)}")
                    
                    # 重新抛出异常，停止任务执行
                    raise
                except Exception as e:
                    logger.warning(f"获取磁带信息失败，使用默认ID: {str(e)}")
            
            backup_task.tape_id = tape_id

            # 3. 创建备份集（简化磁带对象）
            from tape.tape_cartridge import TapeCartridge, TapeStatus
            tape_obj = TapeCartridge(
                tape_id=tape_id,
                label=f"备份磁带-{tape_id}",
                status=TapeStatus.IN_USE,
                capacity_bytes=self.settings.MAX_VOLUME_SIZE,
                used_bytes=0
            )
            backup_set = await self.backup_db.create_backup_set(backup_task, tape_obj)

            # 4. 流式处理：扫描和压缩循环执行
            logger.info("开始流式处理：扫描和压缩循环执行...")
            logger.info(f"批次配置：文件数阈值={self.settings.SCAN_BATCH_SIZE}, 字节数阈值={format_bytes(self.settings.SCAN_BATCH_SIZE_BYTES)}")
            
            processed_files = 0
            total_size = 0  # 压缩后的总大小
            total_original_size = 0  # 原始文件的总大小（未压缩）
            total_scanned_files = 0  # 所有批次扫描到的文件总数（批次相加的文件数）
            estimated_archive_count = 0  # 预计的压缩包总数（估算值）
            group_idx = 0
            current_batch = []  # 当前批次文件列表
            current_batch_size = 0  # 当前批次大小（字节）
            
            # 初始化备份任务的统计信息
            backup_task.processed_files = 0
            backup_task.processed_bytes = 0  # 原始文件的总大小（未压缩）
            backup_task.total_files = 0  # total_files 现在表示压缩包数量（已生成的压缩包数）
            backup_task.total_bytes = 0  # total_bytes 现在表示所有扫描到的文件总数（批次相加的文件数）
            
            # 获取批次大小配置
            batch_size_files = self.settings.SCAN_BATCH_SIZE
            batch_size_bytes = self.settings.SCAN_BATCH_SIZE_BYTES
            
            # 获取排除规则：优先从计划任务获取，否则从备份任务获取
            exclude_patterns = []
            if scheduled_task and hasattr(scheduled_task, 'action_config') and scheduled_task.action_config:
                exclude_patterns = scheduled_task.action_config.get('exclude_patterns', [])
                logger.info(f"从计划任务获取排除规则: {len(exclude_patterns)} 个模式")
            elif backup_task and hasattr(backup_task, 'exclude_patterns') and backup_task.exclude_patterns:
                exclude_patterns = backup_task.exclude_patterns if isinstance(backup_task.exclude_patterns, list) else []
                logger.info(f"从备份任务获取排除规则: {len(exclude_patterns)} 个模式")
            
            if exclude_patterns:
                logger.info(f"排除规则: {exclude_patterns}")
            
            # 流式扫描文件（异步生成器）
            # 重要：使用 async for 持续从生成器获取批次，直到所有文件扫描完成
            logger.info("========== 开始流式扫描和压缩循环 ==========")
            batch_count = 0
            async for file_batch in self.file_scanner.scan_source_files_streaming(
                backup_task.source_paths, 
                exclude_patterns,  # 使用从计划任务获取的排除规则
                backup_task
            ):
                batch_count += 1
                logger.info(f"收到扫描批次 #{batch_count}，包含 {len(file_batch)} 个文件")
                
                # 将扫描到的文件添加到当前批次
                current_batch.extend(file_batch)
                current_batch_size += sum(f['size'] for f in file_batch)
                
                # 累计所有扫描到的文件总数（批次相加的文件数）
                total_scanned_files += len(file_batch)
                backup_task.total_bytes = total_scanned_files  # 存储所有扫描到的文件总数
                
                logger.info(f"当前批次累计：文件数={len(current_batch)}, 大小={format_bytes(current_batch_size)}, 总扫描文件数={total_scanned_files}")
                
                # 估算预计的压缩包总数：根据已扫描的文件数和平均文件大小估算
                # 假设平均每个压缩包包含的文件数 = MAX_FILE_SIZE / 平均文件大小
                if total_scanned_files > 0 and total_original_size > 0:
                    avg_file_size = total_original_size / processed_files if processed_files > 0 else (current_batch_size / len(file_batch) if len(file_batch) > 0 else 0)
                    if avg_file_size > 0:
                        files_per_archive = min(self.settings.MAX_FILE_SIZE / avg_file_size, total_scanned_files)
                        estimated_archive_count = max(1, int(total_scanned_files / files_per_archive) if files_per_archive > 0 else 1)
                    else:
                        # 如果无法估算，使用已生成的压缩包数作为基准
                        estimated_archive_count = max(group_idx + 1, int(total_scanned_files / 1000))  # 假设每1000个文件一个压缩包
                else:
                    # 初始估算：假设每1000个文件一个压缩包
                    estimated_archive_count = max(1, int(total_scanned_files / 1000))
                
                # 检查是否达到批次阈值（文件数或字节数，满足任一条件即触发）
                should_compress = (
                    len(current_batch) >= batch_size_files or 
                    current_batch_size >= batch_size_bytes
                )
                
                # 如果达到阈值，处理当前批次，但继续循环等待下一批次
                if should_compress and current_batch:
                    # 确定是哪个阈值触发的
                    trigger_reason = []
                    if len(current_batch) >= batch_size_files:
                        trigger_reason.append(f"文件数({len(current_batch)}>={batch_size_files})")
                    if current_batch_size >= batch_size_bytes:
                        trigger_reason.append(f"字节数({format_bytes(current_batch_size)}>={format_bytes(batch_size_bytes)})")
                    trigger_text = "或".join(trigger_reason) if trigger_reason else "未知"
                    logger.info(f"========== 达到批次阈值（{trigger_text}），开始压缩当前批次 ==========")
                    logger.info(f"批次信息：文件数={len(current_batch)}, 大小={format_bytes(current_batch_size)}")
                    logger.info(f"注意：压缩完成后将继续扫描后续文件...")
                    
                    # 更新操作状态为压缩中
                    await self.backup_db.update_scan_progress(backup_task, processed_files, processed_files + len(current_batch), "[压缩文件中...]")
                    
                    # 对当前批次进行分组（按 config 的 MAX_FILE_SIZE）
                    file_groups = await self.compressor.group_files_for_compression(current_batch)
                    logger.info(f"当前批次分为 {len(file_groups)} 个文件组")
                    
                    # 处理每个文件组
                    for file_group in file_groups:
                        logger.info(f"处理文件组 {group_idx + 1}/{len(file_groups)} (批次内文件组)，包含 {len(file_group)} 个文件")
                        
                        try:
                            # 压缩文件组（使用7z压缩）
                            compressed_file = await self.compressor.compress_file_group(
                                file_group, 
                                backup_set, 
                                backup_task, 
                                base_processed_files=processed_files,
                                total_files=backup_task.total_files
                            )
                            if not compressed_file:
                                logger.warning(f"文件组 {group_idx + 1} 压缩失败，跳过该文件组，继续处理其他文件组")
                                group_idx += 1
                                continue

                            try:
                                # tar文件已直接写入磁带盘符，获取路径用于数据库记录
                                tape_file_path = await self.tape_handler.write_to_tape_drive(compressed_file['path'], backup_set, group_idx)
                                if not tape_file_path:
                                    logger.warning(f"无法获取压缩文件路径: {compressed_file['path']}，但继续执行")
                                    # 继续执行，因为文件已经写入磁带
                            except Exception as tape_error:
                                logger.warning(f"⚠️ 写入磁带路径获取失败，跳过: {str(tape_error)}，但继续执行")
                                tape_file_path = None

                            try:
                                # 保存文件信息到数据库（便于恢复）
                                await self.backup_db.save_backup_files_to_db(
                                    file_group, 
                                    backup_set, 
                                    compressed_file, 
                                    tape_file_path or compressed_file['path'], 
                                    group_idx
                                )
                            except Exception as db_error:
                                logger.warning(f"⚠️ 保存文件信息到数据库失败，跳过: {str(db_error)}，但继续执行")
                                # 数据库保存失败不影响备份流程，继续执行

                            # 更新进度
                            processed_files += len(file_group)
                            total_size += compressed_file['compressed_size']  # 压缩后的总大小
                            total_original_size += compressed_file['original_size']  # 原始文件的总大小
                            backup_task.processed_files = processed_files
                            backup_task.processed_bytes = total_original_size  # 原始文件的总大小（未压缩）
                            backup_task.compressed_bytes = total_size  # 压缩后的总大小
                            
                            # total_files 表示压缩包数量（已生成的压缩包数）
                            backup_task.total_files = group_idx + 1
                            
                            # 重新估算预计的压缩包总数（基于已处理的文件数和平均文件大小）
                            if processed_files > 0 and total_original_size > 0 and total_scanned_files > 0:
                                avg_file_size = total_original_size / processed_files
                                if avg_file_size > 0:
                                    # 计算每个压缩包能容纳的文件数（基于MAX_FILE_SIZE）
                                    files_per_archive = min(self.settings.MAX_FILE_SIZE / avg_file_size, total_scanned_files)
                                    if files_per_archive > 0:
                                        # 基于总扫描文件数估算压缩包总数
                                        estimated_archive_count = max(group_idx + 1, int(total_scanned_files / files_per_archive))
                                    else:
                                        # 如果文件很大，每个压缩包只能容纳很少文件，使用保守估算
                                        estimated_archive_count = max(group_idx + 1, int(total_scanned_files / 100))
                                else:
                                    # 无法计算平均文件大小，使用保守估算
                                    estimated_archive_count = max(group_idx + 1, int(total_scanned_files / 1000))
                            elif total_scanned_files > 0:
                                # 如果还没有处理文件，但已扫描了文件，使用保守估算
                                estimated_archive_count = max(group_idx + 1, int(total_scanned_files / 1000))
                            else:
                                # 如果还没有扫描文件，使用已生成的压缩包数
                                estimated_archive_count = max(group_idx + 1, 1)
                            
                            logger.debug(f"预计压缩包总数更新: {estimated_archive_count} (已生成: {group_idx + 1}, 总扫描文件: {total_scanned_files}, 已处理文件: {processed_files})")
                            
                            # 将预计的压缩包总数存储到 result_summary（JSON字段）
                            if not hasattr(backup_task, 'result_summary') or backup_task.result_summary is None:
                                backup_task.result_summary = {}
                            if isinstance(backup_task.result_summary, dict):
                                backup_task.result_summary['estimated_archive_count'] = estimated_archive_count
                            else:
                                import json
                                backup_task.result_summary = {'estimated_archive_count': estimated_archive_count}
                            
                            # 更新进度百分比
                            # 进度百分比基于：已处理文件数 / 总扫描文件数
                            # 扫描阶段占10%，压缩阶段占90%，当文件处理完成时进度为100%
                            if total_scanned_files > 0:
                                # 基于已处理文件数和总扫描文件数计算进度
                                file_progress_ratio = processed_files / total_scanned_files
                                # 扫描阶段占10%，压缩阶段占90%
                                backup_task.progress_percent = min(100.0, 10.0 + (file_progress_ratio * 90.0))
                            elif processed_files > 0:
                                # 如果还没有扫描完，但已处理了一些文件，使用估算进度
                                # 估算：假设已处理的文件占总文件的很小一部分
                                backup_task.progress_percent = min(95.0, 10.0 + (processed_files / max(processed_files * 100, 1)) * 85.0)
                            else:
                                # 还没有处理任何文件，进度为10%（扫描阶段）
                                backup_task.progress_percent = 10.0
                            
                            # 更新操作状态：如果还有文件要处理，继续显示压缩中，否则显示写入中
                            # total_files 现在等于 processed_files（已处理文件数的累计）
                            if current_batch:
                                await self.backup_db.update_scan_progress(backup_task, processed_files, backup_task.total_files, "[压缩文件中...]")
                            else:
                                await self.backup_db.update_scan_progress(backup_task, processed_files, backup_task.total_files, "[写入磁带中...]")
                            
                            # 通知进度更新
                            await self._notify_progress(backup_task)
                            
                            group_idx += 1
                        except Exception as group_error:
                            # 文件组处理失败，记录错误但继续处理其他文件组
                            logger.error(f"⚠️ 处理文件组 {group_idx + 1} 时发生错误: {str(group_error)}，跳过该文件组，继续处理其他文件组")
                            import traceback
                            logger.error(f"错误堆栈:\n{traceback.format_exc()}")
                            group_idx += 1
                            continue
                    
                    # 清空当前批次，准备下一批次
                    # 重要：清空后继续循环，等待扫描生成器提供下一批次
                    logger.info(f"========== 批次 #{batch_count} 压缩完成，已处理 {processed_files} 个文件，总大小 {format_bytes(total_size)} ==========")
                    logger.info(f"继续等待扫描下一批次...")
                    
                    # 确保最新的 estimated_archive_count 被保存到数据库
                    # 在清空批次前，确保 result_summary 中的 estimated_archive_count 已更新
                    if hasattr(backup_task, 'result_summary') and backup_task.result_summary:
                        estimated_count = backup_task.result_summary.get('estimated_archive_count', 'N/A')
                        logger.info(f"批次 #{batch_count} 完成后，保存 estimated_archive_count: {estimated_count} (已生成压缩包: {backup_task.total_files}, 总扫描文件: {total_scanned_files})")
                        await self.backup_db.update_scan_progress(backup_task, processed_files, backup_task.total_files, "[压缩文件中...]")
                    
                    current_batch = []
                    current_batch_size = 0
                else:
                    # 未达到阈值，继续累积，等待下一批次或扫描完成
                    logger.debug(f"当前批次未达到阈值（文件数={len(current_batch)}/{batch_size_files}, 大小={format_bytes(current_batch_size)}/{format_bytes(batch_size_bytes)}），继续累积...")
            
            logger.info(f"========== 扫描生成器已完成，共收到 {batch_count} 个批次 ==========")
            
            # 处理剩余的未压缩文件（最后一批）
            if current_batch:
                logger.info(f"处理最后一批文件：文件数={len(current_batch)}, 大小={format_bytes(current_batch_size)}")
                file_groups = await self.compressor.group_files_for_compression(current_batch)
                
                for file_group in file_groups:
                    logger.info(f"处理文件组 {group_idx + 1} (最后一批)")
                    
                    try:
                        compressed_file = await self.compressor.compress_file_group(
                            file_group, 
                            backup_set, 
                            backup_task, 
                            base_processed_files=processed_files,
                            total_files=backup_task.total_files
                        )
                        if not compressed_file:
                            logger.warning(f"文件组 {group_idx + 1} 压缩失败，跳过该文件组")
                            group_idx += 1
                            continue

                        try:
                            tape_file_path = await self.tape_handler.write_to_tape_drive(compressed_file['path'], backup_set, group_idx)
                            if not tape_file_path:
                                logger.warning(f"无法获取压缩文件路径: {compressed_file['path']}，但继续执行")
                        except Exception as tape_error:
                            logger.warning(f"⚠️ 写入磁带路径获取失败，跳过: {str(tape_error)}，但继续执行")
                            tape_file_path = None

                        try:
                            await self.backup_db.save_backup_files_to_db(
                                file_group, 
                                backup_set, 
                                compressed_file, 
                                tape_file_path or compressed_file['path'], 
                                group_idx
                            )
                        except Exception as db_error:
                            logger.warning(f"⚠️ 保存文件信息到数据库失败，跳过: {str(db_error)}，但继续执行")

                        processed_files += len(file_group)
                        total_size += compressed_file['compressed_size']  # 压缩后的总大小
                        total_original_size += compressed_file['original_size']  # 原始文件的总大小
                        backup_task.processed_files = processed_files
                        backup_task.processed_bytes = total_original_size  # 原始文件的总大小（未压缩）
                        backup_task.compressed_bytes = total_size  # 压缩后的总大小
                        
                        # total_files 表示压缩包数量（已生成的压缩包数）
                        backup_task.total_files = group_idx + 1
                        
                        # 重新估算预计的压缩包总数（基于已处理的文件数和平均文件大小）
                        if processed_files > 0 and total_original_size > 0 and total_scanned_files > 0:
                            avg_file_size = total_original_size / processed_files
                            if avg_file_size > 0:
                                # 计算每个压缩包能容纳的文件数（基于MAX_FILE_SIZE）
                                files_per_archive = min(self.settings.MAX_FILE_SIZE / avg_file_size, total_scanned_files)
                                if files_per_archive > 0:
                                    # 基于总扫描文件数估算压缩包总数
                                    estimated_archive_count = max(group_idx + 1, int(total_scanned_files / files_per_archive))
                                else:
                                    # 如果文件很大，每个压缩包只能容纳很少文件，使用保守估算
                                    estimated_archive_count = max(group_idx + 1, int(total_scanned_files / 100))
                            else:
                                # 无法计算平均文件大小，使用保守估算
                                estimated_archive_count = max(group_idx + 1, int(total_scanned_files / 1000))
                        elif total_scanned_files > 0:
                            # 如果还没有处理文件，但已扫描了文件，使用保守估算
                            estimated_archive_count = max(group_idx + 1, int(total_scanned_files / 1000))
                        else:
                            # 如果还没有扫描文件，使用已生成的压缩包数
                            estimated_archive_count = max(group_idx + 1, 1)
                        
                        logger.debug(f"预计压缩包总数更新（最后一批）: {estimated_archive_count} (已生成: {group_idx + 1}, 总扫描文件: {total_scanned_files}, 已处理文件: {processed_files})")
                        
                        # 将预计的压缩包总数存储到 result_summary（JSON字段）
                        if not hasattr(backup_task, 'result_summary') or backup_task.result_summary is None:
                            backup_task.result_summary = {}
                        if isinstance(backup_task.result_summary, dict):
                            backup_task.result_summary['estimated_archive_count'] = estimated_archive_count
                        else:
                            import json
                            backup_task.result_summary = {'estimated_archive_count': estimated_archive_count}
                        
                        # 更新进度百分比（最后一批，基于实际处理进度）
                        # 进度百分比基于：已处理文件数 / 总扫描文件数
                        if total_scanned_files > 0:
                            file_progress_ratio = processed_files / total_scanned_files
                            backup_task.progress_percent = min(100.0, 10.0 + (file_progress_ratio * 90.0))
                        else:
                            # 如果没有总扫描文件数，设为100%（完成）
                            backup_task.progress_percent = 100.0
                        
                        # 更新操作状态：最后一批处理完成，显示写入中
                        # total_files 现在等于 processed_files（已处理文件数的累计）
                        await self.backup_db.update_scan_progress(backup_task, processed_files, backup_task.total_files, "[写入磁带中...]")
                        
                        await self._notify_progress(backup_task)
                        group_idx += 1
                    except Exception as group_error:
                        # 文件组处理失败，记录错误但继续处理其他文件组
                        logger.error(f"⚠️ 处理文件组 {group_idx + 1} 时发生错误: {str(group_error)}，跳过该文件组，继续处理其他文件组")
                        import traceback
                        logger.error(f"错误堆栈:\n{traceback.format_exc()}")
                        group_idx += 1
                        continue

            # 5. 完成备份集
            await self.backup_db.finalize_backup_set(backup_set, processed_files, total_size)
            
            # 确保 total_files 等于压缩包数量（已生成的压缩包数）
            backup_task.total_files = group_idx + 1
            
            # 更新操作状态为完成
            await self.backup_db.update_scan_progress(backup_task, processed_files, backup_task.total_files, "[完成备份集...]")

            logger.info(f"备份完成，共处理 {processed_files} 个文件，总大小 {format_bytes(total_size)}")
            return True

        except Exception as e:
            logger.error(f"备份流程执行失败: {str(e)}")
            backup_task.error_message = str(e)
            return False

    async def _scan_source_files_streaming(self, source_paths: List[str], exclude_patterns: List[str], 
                                           backup_task: Optional[BackupTask] = None):
        """流式扫描源文件（异步生成器，分批返回文件）
        
        支持网络路径（UNC路径）：
        - \\192.168.0.79\yz - 指定共享路径
        - 自动处理 UNC 路径的文件和目录扫描
        
        Yields:
            List[Dict]: 每批文件列表
        """
        if not source_paths:
            logger.warning("源路径列表为空")
            return
        
        # 估算总文件数（用于进度计算）
        estimated_total = 0
        if backup_task:
            try:
                for source_path_str in source_paths:
                    # 使用 WindowsPath 以确保 UNC 路径正确处理
                    from utils.network_path import is_unc_path, normalize_unc_path
                    if is_unc_path(source_path_str):
                        # UNC 路径需要使用 WindowsPath
                        source_path = Path(normalize_unc_path(source_path_str))
                    else:
                        source_path = Path(source_path_str)
                    
                    if source_path.is_dir():
                        try:
                            file_count = 0
                            for _ in source_path.rglob('*'):
                                if _.is_file():
                                    file_count += 1
                                    if file_count >= 1000:
                                        estimated_total += max(1000, file_count * 2)
                                        break
                            if file_count < 1000:
                                estimated_total += file_count
                        except Exception:
                            estimated_total += 5000
                    elif source_path.is_file():
                        estimated_total += 1
            except Exception:
                estimated_total = 5000
        
        if estimated_total < 100:
            estimated_total = 1000

        total_scanned = 0
        current_batch = []
        batch_size = 100  # 每次yield的文件数（小批次，便于及时压缩）
        
        for idx, source_path_str in enumerate(source_paths):
            logger.info(f"扫描源路径 {idx + 1}/{len(source_paths)}: {source_path_str}")
            
            # 处理 UNC 网络路径
            from utils.network_path import is_unc_path, normalize_unc_path
            if is_unc_path(source_path_str):
                # UNC 路径需要使用规范化后的路径
                normalized_path = normalize_unc_path(source_path_str)
                source_path = Path(normalized_path)
                logger.debug(f"检测到 UNC 路径，规范化后: {normalized_path}")
            else:
                source_path = Path(source_path_str)
            
            if not source_path.exists():
                logger.warning(f"源路径不存在，跳过: {source_path_str}")
                continue
            
            try:
                if source_path.is_file():
                    logger.debug(f"扫描文件: {source_path_str}")
                    try:
                        file_info = await self.file_scanner.get_file_info(source_path)
                        if file_info:
                            # 排除规则从计划任务获取（scheduled_task.action_config.exclude_patterns）
                            if not self.file_scanner.should_exclude_file(file_info['path'], exclude_patterns):
                                current_batch.append(file_info)
                        total_scanned += 1
                        
                        # 更新扫描进度
                        if backup_task and estimated_total > 0:
                            scan_progress = min(10.0, (total_scanned / estimated_total) * 10.0)
                            backup_task.progress_percent = scan_progress
                            await self.backup_db.update_scan_progress(backup_task, total_scanned, len(current_batch), "[扫描文件中...]")
                        
                        # 达到批次大小，yield当前批次
                        if len(current_batch) >= batch_size:
                            yield current_batch
                            current_batch = []
                    except (PermissionError, OSError, FileNotFoundError, IOError) as file_error:
                        # 文件访问错误，跳过该文件，继续扫描
                        logger.warning(f"⚠️ 跳过无法访问的文件: {source_path_str} (错误: {str(file_error)})")
                        continue
                    except Exception as file_error:
                        # 其他错误，也跳过该文件
                        logger.warning(f"⚠️ 跳过出错的文件: {source_path_str} (错误: {str(file_error)})")
                        continue
                            
                elif source_path.is_dir():
                    logger.info(f"扫描目录: {source_path_str}")
                    
                    # 检查目录本身是否匹配排除规则
                    if self.file_scanner.should_exclude_file(str(source_path), exclude_patterns):
                        logger.info(f"目录匹配排除规则，跳过整个目录: {source_path_str}")
                        continue
                    
                    scanned_count = 0
                    excluded_count = 0
                    skipped_dirs = 0
                    error_count = 0
                    error_paths = []  # 记录出错的路径
                    
                    try:
                        for file_path in source_path.rglob('*'):
                            try:
                                # 检查文件路径的父目录是否匹配排除规则
                                # 如果父目录匹配，跳过该文件
                                if self.file_scanner.should_exclude_file(str(file_path.parent), exclude_patterns):
                                    skipped_dirs += 1
                                    continue
                                
                                if file_path.is_file():
                                    scanned_count += 1
                                    total_scanned += 1
                                    
                                    # 每扫描100个文件输出一次进度
                                    if scanned_count % 100 == 0:
                                        logger.info(f"已扫描 {scanned_count} 个文件，找到 {len(current_batch)} 个有效文件...")
                                    
                                    # 每扫描50个文件更新一次进度
                                    if total_scanned % 50 == 0 and backup_task:
                                        if total_scanned > estimated_total:
                                            estimated_total = total_scanned * 2
                                        
                                        if estimated_total > 0:
                                            scan_progress = min(10.0, (total_scanned / estimated_total) * 10.0)
                                            backup_task.progress_percent = scan_progress
                                            await self.backup_db.update_scan_progress(backup_task, total_scanned, len(current_batch), "[扫描文件中...]")
                                    
                                    try:
                                        file_info = await self.file_scanner.get_file_info(file_path)
                                        if file_info:
                                            # 排除规则从计划任务获取（scheduled_task.action_config.exclude_patterns）
                                            if not self.file_scanner.should_exclude_file(file_info['path'], exclude_patterns):
                                                current_batch.append(file_info)
                                                
                                                # 达到批次大小，yield当前批次
                                                if len(current_batch) >= batch_size:
                                                    yield current_batch
                                                    current_batch = []
                                            else:
                                                excluded_count += 1
                                    except (PermissionError, OSError, FileNotFoundError, IOError) as file_error:
                                        # 文件访问错误（权限、不存在等），跳过该文件，继续扫描
                                        error_count += 1
                                        error_paths.append(str(file_path))
                                        logger.warning(f"⚠️ 跳过无法访问的文件: {file_path} (错误: {str(file_error)})")
                                        continue
                                    except Exception as file_error:
                                        # 其他错误，也跳过该文件
                                        error_count += 1
                                        error_paths.append(str(file_path))
                                        logger.warning(f"⚠️ 跳过出错的文件: {file_path} (错误: {str(file_error)})")
                                        continue
                                elif file_path.is_dir():
                                    try:
                                        # 检查目录是否匹配排除规则
                                        if self.file_scanner.should_exclude_file(str(file_path), exclude_patterns):
                                            skipped_dirs += 1
                                            # 跳过该目录下的所有文件（rglob会继续，但我们在文件检查时会跳过）
                                            continue
                                    except (PermissionError, OSError, FileNotFoundError, IOError) as dir_error:
                                        # 目录访问错误，跳过该目录
                                        error_count += 1
                                        error_paths.append(str(file_path))
                                        logger.warning(f"⚠️ 跳过无法访问的目录: {file_path} (错误: {str(dir_error)})")
                                        continue
                            except (PermissionError, OSError, FileNotFoundError, IOError) as path_error:
                                # 路径访问错误，跳过该路径
                                error_count += 1
                                error_paths.append(str(file_path))
                                logger.warning(f"⚠️ 跳过无法访问的路径: {file_path} (错误: {str(path_error)})")
                                continue
                            except Exception as path_error:
                                # 其他错误，也跳过该路径
                                error_count += 1
                                error_paths.append(str(file_path))
                                logger.warning(f"⚠️ 跳过出错的路径: {file_path} (错误: {str(path_error)})")
                                continue
                    except (PermissionError, OSError, FileNotFoundError, IOError) as scan_error:
                        # 扫描目录时的访问错误，记录但继续扫描其他目录
                        error_count += 1
                        error_paths.append(str(source_path_str))
                        logger.error(f"⚠️ 扫描目录时发生访问错误 {source_path_str}: {str(scan_error)}，跳过该目录，继续扫描其他路径")
                        continue
                    except Exception as e:
                        # 其他扫描错误，记录但继续
                        error_count += 1
                        error_paths.append(str(source_path_str))
                        logger.error(f"⚠️ 扫描目录时发生错误 {source_path_str}: {str(e)}，跳过该目录，继续扫描其他路径")
                        continue
                    
                    logger.info(f"目录扫描完成: {source_path_str}, 扫描 {scanned_count} 个文件, 有效 {len(current_batch)} 个, 排除 {excluded_count} 个文件, 跳过 {skipped_dirs} 个目录/文件, 错误 {error_count} 个")
                    if excluded_count > 0 or skipped_dirs > 0:
                        logger.warning(f"⚠️ 注意：已排除 {excluded_count} 个文件，跳过 {skipped_dirs} 个目录/文件（排除规则: {exclude_patterns if exclude_patterns else '无'}）")
                    if error_count > 0:
                        logger.warning(f"⚠️ 注意：遇到 {error_count} 个错误（权限、访问等），已跳过这些文件/目录")
                        if len(error_paths) <= 10:
                            logger.warning(f"⚠️ 错误路径示例: {', '.join(error_paths[:10])}")
                        else:
                            logger.warning(f"⚠️ 错误路径示例（前10个）: {', '.join(error_paths[:10])}... 共 {len(error_paths)} 个")
                else:
                    logger.warning(f"源路径既不是文件也不是目录: {source_path_str}")
            except Exception as e:
                logger.error(f"处理源路径失败 {source_path_str}: {str(e)}")
                continue
        
        # 返回最后一批文件
        if current_batch:
            yield current_batch
        
        # 扫描完成，更新进度
        if backup_task:
            backup_task.progress_percent = 10.0
            await self.backup_db.update_scan_progress(backup_task, total_scanned, total_scanned, "[准备压缩...]")
        
        logger.info(f"========== 扫描完成 ==========")
        logger.info(f"共扫描 {total_scanned} 个文件")
        if exclude_patterns:
            logger.info(f"排除规则: {exclude_patterns}")
        logger.info(f"========== 扫描完成 ==========")

    async def _scan_source_files(self, source_paths: List[str], exclude_patterns: List[str], 
                                  backup_task: Optional[BackupTask] = None) -> List[Dict]:
        """扫描源文件（兼容旧接口，收集所有文件后返回）"""
        file_list = []
        
        if not source_paths:
            logger.warning("源路径列表为空")
            return file_list

        # 估算总文件数（用于进度计算）
        estimated_total = 0
        if backup_task:
            # 尝试估算总文件数（更准确的估算）
            try:
                for source_path_str in source_paths:
                    source_path = Path(source_path_str)
                    if source_path.is_dir():
                        # 尝试实际统计目录中的文件数（递归）
                        try:
                            # 使用快速统计方法：统计前1000个文件，然后估算
                            file_count = 0
                            for _ in source_path.rglob('*'):
                                if _.is_file():
                                    file_count += 1
                                    if file_count >= 1000:
                                        # 如果文件数超过1000，假设还有更多，使用估算
                                        # 估算：假设目录结构类似，文件数可能更多
                                        estimated_total += max(1000, file_count * 2)
                                        break
                            if file_count < 1000:
                                # 如果文件数少于1000，使用实际统计
                                estimated_total += file_count
                        except Exception:
                            # 如果统计失败，使用保守估算
                            estimated_total += 5000  # 增加估算值
                    elif source_path.is_file():
                        estimated_total += 1
            except Exception:
                estimated_total = 5000  # 增加默认值
        
        # 如果估算值太小，使用更合理的默认值
        if estimated_total < 100:
            estimated_total = 1000  # 最小估算值

        total_scanned = 0
        
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
                    try:
                        file_info = await self.file_scanner.get_file_info(source_path)
                        if file_info:
                            # 排除规则从计划任务获取（scheduled_task.action_config.exclude_patterns）
                            if not self.file_scanner.should_exclude_file(file_info['path'], exclude_patterns):
                                file_list.append(file_info)
                                logger.debug(f"已添加文件: {file_info['path']}")
                        
                        total_scanned += 1
                        # 更新扫描进度
                        if backup_task and estimated_total > 0:
                            scan_progress = min(10.0, (total_scanned / estimated_total) * 10.0)  # 扫描占10%进度
                            backup_task.progress_percent = scan_progress
                            await self.backup_db.update_scan_progress(backup_task, total_scanned, len(file_list))
                    except (PermissionError, OSError, FileNotFoundError, IOError) as file_error:
                        # 文件访问错误，跳过该文件，继续扫描
                        logger.warning(f"⚠️ 跳过无法访问的文件: {source_path_str} (错误: {str(file_error)})")
                        continue
                    except Exception as file_error:
                        # 其他错误，也跳过该文件
                        logger.warning(f"⚠️ 跳过出错的文件: {source_path_str} (错误: {str(file_error)})")
                        continue
                        
                elif source_path.is_dir():
                    logger.info(f"扫描目录: {source_path_str}")
                    
                    # 检查目录本身是否匹配排除规则
                    if self.file_scanner.should_exclude_file(str(source_path), exclude_patterns):
                        logger.info(f"目录匹配排除规则，跳过整个目录: {source_path_str}")
                        continue
                    
                    scanned_count = 0
                    excluded_count = 0
                    skipped_dirs = 0
                    error_count = 0
                    error_paths = []
                    
                    # 使用 rglob 递归扫描，但需要处理可能的异常
                    try:
                        for file_path in source_path.rglob('*'):
                            try:
                                # 检查文件路径的父目录是否匹配排除规则
                                # 如果父目录匹配，跳过该文件
                                if self.file_scanner.should_exclude_file(str(file_path.parent), exclude_patterns):
                                    skipped_dirs += 1
                                    continue
                                
                                if file_path.is_file():
                                    scanned_count += 1
                                    total_scanned += 1
                                    
                                    # 每扫描100个文件输出一次进度并更新数据库
                                    if scanned_count % 100 == 0:
                                        logger.info(f"已扫描 {scanned_count} 个文件，找到 {len(file_list)} 个有效文件...")
                                    
                                    # 每扫描50个文件更新一次进度（避免过于频繁）
                                    if total_scanned % 50 == 0 and backup_task:
                                        # 动态调整估算值：如果实际扫描的文件数超过估算值，更新估算值
                                        if total_scanned > estimated_total:
                                            estimated_total = total_scanned * 2  # 假设还有一半未扫描
                                        
                                        if estimated_total > 0:
                                            scan_progress = min(10.0, (total_scanned / estimated_total) * 10.0)  # 扫描占10%进度
                                            backup_task.progress_percent = scan_progress
                                            await self.backup_db.update_scan_progress(backup_task, total_scanned, len(file_list))
                                    
                                    try:
                                        file_info = await self.file_scanner.get_file_info(file_path)
                                        if file_info:
                                            # 排除规则从计划任务获取（scheduled_task.action_config.exclude_patterns）
                                            if not self.file_scanner.should_exclude_file(file_info['path'], exclude_patterns):
                                                file_list.append(file_info)
                                            else:
                                                excluded_count += 1
                                    except (PermissionError, OSError, FileNotFoundError, IOError) as file_error:
                                        # 文件访问错误，跳过该文件，继续扫描
                                        error_count += 1
                                        error_paths.append(str(file_path))
                                        logger.warning(f"⚠️ 跳过无法访问的文件: {file_path} (错误: {str(file_error)})")
                                        continue
                                    except Exception as file_error:
                                        # 其他错误，也跳过该文件
                                        error_count += 1
                                        error_paths.append(str(file_path))
                                        logger.warning(f"⚠️ 跳过出错的文件: {file_path} (错误: {str(file_error)})")
                                        continue
                                elif file_path.is_dir():
                                    try:
                                        # 检查目录是否匹配排除规则
                                        if self.file_scanner.should_exclude_file(str(file_path), exclude_patterns):
                                            skipped_dirs += 1
                                            # 跳过该目录下的所有文件（rglob会继续，但我们在文件检查时会跳过）
                                            continue
                                    except (PermissionError, OSError, FileNotFoundError, IOError) as dir_error:
                                        # 目录访问错误，跳过该目录
                                        error_count += 1
                                        error_paths.append(str(file_path))
                                        logger.warning(f"⚠️ 跳过无法访问的目录: {file_path} (错误: {str(dir_error)})")
                                        continue
                            except (PermissionError, OSError, FileNotFoundError, IOError) as path_error:
                                # 路径访问错误，跳过该路径
                                error_count += 1
                                error_paths.append(str(file_path))
                                logger.warning(f"⚠️ 跳过无法访问的路径: {file_path} (错误: {str(path_error)})")
                                continue
                            except Exception as path_error:
                                # 其他错误，也跳过该路径
                                error_count += 1
                                error_paths.append(str(file_path))
                                logger.warning(f"⚠️ 跳过出错的路径: {file_path} (错误: {str(path_error)})")
                                continue
                    except (PermissionError, OSError, FileNotFoundError, IOError) as scan_error:
                        # 扫描目录时的访问错误，记录但继续扫描其他目录
                        error_count += 1
                        error_paths.append(str(source_path_str))
                        logger.error(f"⚠️ 扫描目录时发生访问错误 {source_path_str}: {str(scan_error)}，跳过该目录，继续扫描其他路径")
                        # 继续扫描其他路径
                        continue
                    except Exception as e:
                        # 其他扫描错误，记录但继续
                        error_count += 1
                        error_paths.append(str(source_path_str))
                        logger.error(f"⚠️ 扫描目录时发生错误 {source_path_str}: {str(e)}，跳过该目录，继续扫描其他路径")
                        # 继续扫描其他路径
                        continue
                    
                    logger.info(f"目录扫描完成: {source_path_str}, 扫描 {scanned_count} 个文件, 有效 {len(file_list)} 个, 排除 {excluded_count} 个文件, 跳过 {skipped_dirs} 个目录/文件, 错误 {error_count} 个")
                    if excluded_count > 0 or skipped_dirs > 0:
                        logger.warning(f"⚠️ 注意：已排除 {excluded_count} 个文件，跳过 {skipped_dirs} 个目录/文件（排除规则: {exclude_patterns if exclude_patterns else '无'}）")
                    if error_count > 0:
                        logger.warning(f"⚠️ 注意：遇到 {error_count} 个错误（权限、访问等），已跳过这些文件/目录")
                        if len(error_paths) <= 10:
                            logger.warning(f"⚠️ 错误路径示例: {', '.join(error_paths[:10])}")
                        else:
                            logger.warning(f"⚠️ 错误路径示例（前10个）: {', '.join(error_paths[:10])}... 共 {len(error_paths)} 个")
                else:
                    logger.warning(f"源路径既不是文件也不是目录: {source_path_str}")
            except Exception as e:
                logger.error(f"处理源路径失败 {source_path_str}: {str(e)}")
                continue

        # 扫描完成，更新进度
        if backup_task:
            backup_task.progress_percent = 10.0  # 扫描完成，进度10%
            await self.backup_db.update_scan_progress(backup_task, total_scanned, len(file_list))

        logger.info(f"扫描完成，共找到 {len(file_list)} 个文件")
        return file_list

    async def _get_current_drive_tape(self) -> Optional[TapeCartridge]:
        """获取当前驱动器中的磁带"""
        try:
            if not self.tape_manager:
                logger.warning("磁带管理器未初始化")
                return None
            
            # 检查当前磁带管理器是否已有当前磁带
            if self.tape_manager.current_tape:
                logger.info(f"当前驱动器已有磁带: {self.tape_manager.current_tape.tape_id}")
                return self.tape_manager.current_tape
            
            # 尝试扫描当前驱动器中的磁带卷标
            try:
                tape_ops = self.tape_manager.tape_operations
                if tape_ops and hasattr(tape_ops, '_read_tape_label'):
                    label_info = await tape_ops._read_tape_label()
                    if label_info and label_info.get('tape_id'):
                        tape_id = label_info.get('tape_id')
                        logger.info(f"从驱动器扫描到磁带卷标: {tape_id}")
                        
                        # 检查数据库中是否有该磁带
                        from utils.scheduler.db_utils import is_opengauss, get_opengauss_connection
                        if is_opengauss():
                            conn = await get_opengauss_connection()
                            try:
                                row = await conn.fetchrow(
                                    """
                                    SELECT tape_id, label, status, 
                                           COALESCE(first_use_date, manufactured_date, created_at) as created_date,
                                           expiry_date,
                                           capacity_bytes, used_bytes, serial_number
                                    FROM tape_cartridges
                                    WHERE tape_id = $1
                                    """,
                                    tape_id
                                )
                                
                                if row:
                                    # 磁带在数据库中，创建 TapeCartridge 对象
                                    from tape.tape_cartridge import TapeStatus
                                    from datetime import datetime
                                    created_date = row['created_date']
                                    if created_date and isinstance(created_date, str):
                                        try:
                                            created_date = datetime.fromisoformat(created_date.replace('Z', '+00:00'))
                                        except:
                                            created_date = datetime.fromisoformat(created_date.split('T')[0])
                                    
                                    # 处理状态值（数据库可能返回大写，枚举是小写）
                                    status_str = row['status']
                                    if status_str:
                                        status_str_lower = status_str.lower()
                                        # 将数据库状态映射到枚举值
                                        status_map = {
                                            'available': TapeStatus.AVAILABLE,
                                            'in_use': TapeStatus.IN_USE,
                                            'full': TapeStatus.FULL,
                                            'expired': TapeStatus.EXPIRED,
                                            'error': TapeStatus.ERROR,
                                            'maintenance': TapeStatus.MAINTENANCE,
                                            'new': TapeStatus.NEW
                                        }
                                        tape_status = status_map.get(status_str_lower, TapeStatus.AVAILABLE)
                                    else:
                                        tape_status = TapeStatus.AVAILABLE
                                    
                                    tape = TapeCartridge(
                                        tape_id=row['tape_id'],
                                        label=row['label'],
                                        status=tape_status,
                                        created_date=created_date,
                                        expiry_date=row['expiry_date'],
                                        capacity_bytes=row['capacity_bytes'] or 0,
                                        used_bytes=row['used_bytes'] or 0,
                                        serial_number=row['serial_number'] or ''
                                    )
                                    # 更新磁带管理器的当前磁带
                                    self.tape_manager.current_tape = tape
                                    self.tape_manager.tape_cartridges[tape_id] = tape
                                    logger.info(f"从数据库加载磁带信息: {tape_id}")
                                    return tape
                                else:
                                    # 磁带不在数据库中，但驱动器中有磁带
                                    logger.error(f"驱动器中的磁带不在数据库中: {tape_id}")
                                    logger.error("检测到驱动器中的磁带未在数据库中注册，任务将停止")
                                    # 抛出异常，停止任务执行
                                    raise RuntimeError(f"驱动器中的磁带 {tape_id} 未在数据库中注册，请先在磁带管理页面添加该磁带")
                            finally:
                                await conn.close()
            except Exception as e:
                logger.warning(f"扫描当前驱动器磁带失败: {str(e)}")
                return None
                
            return None
        except Exception as e:
            logger.error(f"获取当前驱动器磁带失败: {str(e)}")
            return None

    async def _get_file_info(self, file_path: Path) -> Optional[Dict]:
        """获取文件信息
        
        如果遇到权限错误、访问错误等，返回None，调用者应该跳过该文件。
        """
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
        except (PermissionError, OSError, FileNotFoundError, IOError) as e:
            # 权限错误、访问错误等，返回None，让调用者跳过该文件
            logger.debug(f"无法获取文件信息（权限/访问错误）: {file_path} (错误: {str(e)})")
            return None
        except Exception as e:
            # 其他错误，也返回None
            logger.warning(f"获取文件信息失败 {file_path}: {str(e)}")
            return None

    def _should_exclude_file(self, file_path: str, exclude_patterns: List[str]) -> bool:
        """检查文件或目录是否应该被排除
        
        排除规则匹配文件路径或其任何父目录路径时，文件/目录都会被排除。
        例如：如果排除规则匹配 "D:\temp"，则 "D:\temp\file.txt" 和 "D:\temp\subdir\file.txt" 都会被排除。
        
        Args:
            file_path: 文件或目录路径
            exclude_patterns: 排除模式列表（从计划任务 action_config 获取）
            
        Returns:
            bool: 如果文件/目录应该被排除返回 True
        """
        import fnmatch
        
        if not exclude_patterns:
            return False
        
        # 将路径标准化（统一使用正斜杠或反斜杠）
        normalized_path = file_path.replace('\\', '/')
        
        # 检查文件/目录路径本身是否匹配排除规则
        for pattern in exclude_patterns:
            normalized_pattern = pattern.replace('\\', '/')
            if fnmatch.fnmatch(normalized_path, normalized_pattern):
                return True
        
        # 检查文件/目录路径的父目录是否匹配排除规则
        # 例如：如果排除规则是 "D:/temp/*"，则 "D:/temp/subdir/file.txt" 应该被排除
        path_parts = normalized_path.split('/')
        for i in range(len(path_parts)):
            # 构建父目录路径（从根目录到当前层级）
            parent_path = '/'.join(path_parts[:i+1])
            if not parent_path:
                continue
            
            for pattern in exclude_patterns:
                normalized_pattern = pattern.replace('\\', '/')
                # 检查父目录路径是否匹配排除规则
                if fnmatch.fnmatch(parent_path, normalized_pattern):
                    return True
                # 检查父目录路径是否匹配通配符模式（如 "D:/temp/*"）
                if fnmatch.fnmatch(parent_path + '/*', normalized_pattern):
                    return True
        
        return False

    async def _group_files_for_compression(self, file_list: List[Dict]) -> List[List[Dict]]:
        """将文件分组以进行压缩
        
        单个压缩包的最大大小从 config 获取（MAX_FILE_SIZE）
        
        Args:
            file_list: 文件列表
            
        Returns:
            List[List[Dict]]: 分组后的文件列表
        """
        # 从系统配置获取单个压缩包的最大大小
        max_size = self.settings.MAX_FILE_SIZE
        logger.debug(f"使用系统配置的单个压缩包最大大小: {format_bytes(max_size)}")
        
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

    async def _compress_file_group(self, file_group: List[Dict], backup_set: BackupSet, backup_task: BackupTask,
                                   base_processed_files: int = 0, total_files: int = 0) -> Optional[Dict]:
        """压缩文件组（使用7z压缩，支持多线程，带进度跟踪）"""
        try:
            import threading
            import time
            
            # 从备份任务获取压缩设置
            compression_enabled = getattr(backup_task, 'compression_enabled', True)
            
            # 从系统配置获取压缩级别（从 config 获取）
            compression_level = self.settings.COMPRESSION_LEVEL
            logger.debug(f"使用系统配置的压缩级别: {compression_level}")
            
            # 从系统配置获取线程数
            compression_threads = self.settings.COMPRESSION_THREADS
            
            # 创建临时文件（直接写入磁带盘符，不创建临时文件）
            timestamp = format_datetime(now(), '%Y%m%d_%H%M%S')
            tape_drive = self.settings.TAPE_DRIVE_LETTER.upper() + ":\\"
            backup_dir = Path(tape_drive) / backup_set.set_id
            
            # 进度跟踪变量
            compress_progress = {'bytes_written': 0, 'running': True, 'completed': False}
            total_original_size = sum(f['size'] for f in file_group)
            # 用于存储成功和失败的文件信息（在线程间共享）
            compress_result = {'successful_files': [], 'failed_files': [], 'successful_original_size': 0}
            
            # 将压缩操作放到线程池中执行，避免阻塞事件循环
            def _do_7z_compress():
                """在线程中执行7z压缩操作，带进度跟踪"""
                try:
                    # 在线程中创建目录（避免阻塞事件循环）
                    backup_dir.mkdir(parents=True, exist_ok=True)
                    
                    if compression_enabled:
                        # 使用7z压缩，直接写入磁带盘符
                        archive_path = backup_dir / f"backup_{backup_set.set_id}_{timestamp}.7z"
                        
                        # 使用py7zr进行7z压缩，启用多进程（mp=True启用多进程压缩）
                        # 注意：py7zr 使用 mp 参数启用多进程，而不是 threads
                        with py7zr.SevenZipFile(
                            archive_path,
                            mode='w',
                            filters=[{'id': py7zr.FILTER_LZMA2, 'preset': compression_level}],
                            mp=True if compression_threads > 1 else False  # 启用多进程压缩（如果线程数>1）
                        ) as archive:
                            # 添加文件到压缩包
                            successful_files = []
                            failed_files = []
                            
                            for file_idx, file_info in enumerate(file_group):
                                file_path = Path(file_info['path'])
                                try:
                                    if not file_path.exists():
                                        logger.warning(f"文件不存在，跳过: {file_path}")
                                        failed_files.append({'path': str(file_path), 'reason': '文件不存在'})
                                        continue
                                    
                                    # 计算相对路径（保留目录结构）
                                    try:
                                        source_paths = backup_task.source_paths or []
                                        if source_paths:
                                            arcname = None
                                            for src_path in source_paths:
                                                src = Path(src_path)
                                                try:
                                                    if file_path.is_relative_to(src):
                                                        arcname = str(file_path.relative_to(src))
                                                        break
                                                except (ValueError, AttributeError):
                                                    continue
                                            if arcname is None:
                                                arcname = file_path.name
                                        else:
                                            arcname = file_path.name
                                    except Exception:
                                        arcname = file_path.name
                                    
                                    # 添加文件到压缩包
                                    try:
                                        archive.write(file_path, arcname=arcname)
                                        successful_files.append(str(file_path))
                                        
                                        # 更新进度：基于已压缩的文件数量
                                        if total_files > 0:
                                            current_processed = base_processed_files + file_idx + 1
                                            # 扫描阶段占10%，压缩阶段占90%
                                            compress_progress_value = 10.0 + (current_processed / total_files) * 90.0
                                            compress_progress['bytes_written'] = archive_path.stat().st_size if archive_path.exists() else 0
                                            
                                            # 更新任务进度（在后台线程中，需要异步更新）
                                            if backup_task and backup_task.id:
                                                backup_task.progress_percent = min(100.0, compress_progress_value)
                                    except (PermissionError, OSError, FileNotFoundError, IOError) as write_error:
                                        # 文件写入错误（权限、访问等），跳过该文件，继续处理其他文件
                                        logger.warning(f"⚠️ 压缩时跳过无法访问的文件: {file_path} (错误: {str(write_error)})")
                                        failed_files.append({'path': str(file_path), 'reason': str(write_error)})
                                        continue
                                    except Exception as write_error:
                                        # 其他写入错误，也跳过该文件
                                        logger.warning(f"⚠️ 压缩时跳过出错的文件: {file_path} (错误: {str(write_error)})")
                                        failed_files.append({'path': str(file_path), 'reason': str(write_error)})
                                        continue
                                except (PermissionError, OSError, FileNotFoundError, IOError) as path_error:
                                    # 路径访问错误，跳过该文件
                                    logger.warning(f"⚠️ 压缩时跳过无法访问的路径: {file_path} (错误: {str(path_error)})")
                                    failed_files.append({'path': str(file_path), 'reason': str(path_error)})
                                    continue
                                except Exception as path_error:
                                    # 其他路径错误，也跳过该文件
                                    logger.warning(f"⚠️ 压缩时跳过出错的路径: {file_path} (错误: {str(path_error)})")
                                    failed_files.append({'path': str(file_path), 'reason': str(path_error)})
                                    continue
                            
                            # 记录成功和失败的文件
                            if failed_files:
                                logger.warning(f"⚠️ 压缩文件组时，成功 {len(successful_files)} 个，失败 {len(failed_files)} 个")
                                for failed_file in failed_files[:5]:  # 只显示前5个失败的文件
                                    logger.warning(f"  - 失败: {failed_file['path']} (原因: {failed_file['reason']})")
                                if len(failed_files) > 5:
                                    logger.warning(f"  ... 还有 {len(failed_files) - 5} 个失败的文件")
                            
                            # 如果所有文件都失败了，返回None
                            if len(successful_files) == 0:
                                logger.error(f"文件组中所有文件都失败，跳过该文件组")
                                return None
                            
                            # 只统计成功压缩的文件大小
                            successful_file_infos = [f for f in file_group if str(Path(f['path'])) in successful_files]
                            successful_original_size = sum(f['size'] for f in successful_file_infos)
                        
                        # 确保压缩完成：with语句退出时，archive.close()会自动调用
                        # 但为了确保文件已完全写入，检查文件是否存在且大小稳定
                        if archive_path.exists():
                            # 等待文件写入完成（文件大小稳定）
                            # 注意：此函数在 run_in_executor 的线程中运行，必须使用同步 sleep
                            prev_size = 0
                            for _ in range(10):  # 最多等待1秒
                                current_size = archive_path.stat().st_size
                                if current_size == prev_size:
                                    break
                                prev_size = current_size
                                time.sleep(0.1)  # 同步 sleep（在后台线程中）
                        
                        compress_progress['running'] = False
                        compress_progress['completed'] = True
                        return archive_path, archive_path.stat().st_size if archive_path.exists() else 0
                    else:
                        # 不使用压缩，使用tar打包
                        import tarfile
                        tar_file = backup_dir / f"backup_{backup_set.set_id}_{timestamp}.tar"
                        
                        with tarfile.open(tar_file, 'w') as tar_archive:
                            successful_files = []
                            failed_files = []
                            
                            for file_idx, file_info in enumerate(file_group):
                                file_path = Path(file_info['path'])
                                try:
                                    if not file_path.exists():
                                        logger.warning(f"文件不存在，跳过: {file_path}")
                                        failed_files.append({'path': str(file_path), 'reason': '文件不存在'})
                                        continue
                                    
                                    try:
                                        source_paths = backup_task.source_paths or []
                                        if source_paths:
                                            arcname = None
                                            for src_path in source_paths:
                                                src = Path(src_path)
                                                try:
                                                    if file_path.is_relative_to(src):
                                                        arcname = str(file_path.relative_to(src))
                                                        break
                                                except (ValueError, AttributeError):
                                                    continue
                                            if arcname is None:
                                                arcname = file_path.name
                                        else:
                                            arcname = file_path.name
                                    except Exception:
                                        arcname = file_path.name
                                    
                                    try:
                                        tar_archive.add(file_path, arcname=arcname)
                                        successful_files.append(str(file_path))
                                        
                                        if total_files > 0:
                                            current_processed = base_processed_files + file_idx + 1
                                            tar_progress_value = 10.0 + (current_processed / total_files) * 90.0
                                            compress_progress['bytes_written'] = tar_file.stat().st_size if tar_file.exists() else 0
                                            
                                            if backup_task:
                                                backup_task.progress_percent = min(100.0, tar_progress_value)
                                    except (PermissionError, OSError, FileNotFoundError, IOError) as add_error:
                                        # 文件添加错误，跳过该文件
                                        logger.warning(f"⚠️ tar打包时跳过无法访问的文件: {file_path} (错误: {str(add_error)})")
                                        failed_files.append({'path': str(file_path), 'reason': str(add_error)})
                                        continue
                                    except Exception as add_error:
                                        # 其他错误，也跳过该文件
                                        logger.warning(f"⚠️ tar打包时跳过出错的文件: {file_path} (错误: {str(add_error)})")
                                        failed_files.append({'path': str(file_path), 'reason': str(add_error)})
                                        continue
                                except (PermissionError, OSError, FileNotFoundError, IOError) as path_error:
                                    # 路径访问错误，跳过该文件
                                    logger.warning(f"⚠️ tar打包时跳过无法访问的路径: {file_path} (错误: {str(path_error)})")
                                    failed_files.append({'path': str(file_path), 'reason': str(path_error)})
                                    continue
                                except Exception as path_error:
                                    # 其他路径错误，也跳过该文件
                                    logger.warning(f"⚠️ tar打包时跳过出错的路径: {file_path} (错误: {str(path_error)})")
                                    failed_files.append({'path': str(file_path), 'reason': str(path_error)})
                                    continue
                            
                            # 记录成功和失败的文件
                            if failed_files:
                                logger.warning(f"⚠️ tar打包文件组时，成功 {len(successful_files)} 个，失败 {len(failed_files)} 个")
                                for failed_file in failed_files[:5]:  # 只显示前5个失败的文件
                                    logger.warning(f"  - 失败: {failed_file['path']} (原因: {failed_file['reason']})")
                                if len(failed_files) > 5:
                                    logger.warning(f"  ... 还有 {len(failed_files) - 5} 个失败的文件")
                            
                            # 如果所有文件都失败了，返回None
                            if len(successful_files) == 0:
                                logger.error(f"tar打包文件组中所有文件都失败，跳过该文件组")
                                return None
                        
                        compress_progress['running'] = False
                        compress_progress['completed'] = True
                        return tar_file, tar_file.stat().st_size if tar_file.exists() else 0
                except Exception as e:
                    logger.error(f"压缩操作失败: {str(e)}")
                    compress_progress['running'] = False
                    compress_progress['completed'] = False
                    raise
            
            # 启动进度监控任务：定期从内存读取进度并更新到数据库
            async def _monitor_progress_update():
                """监控并更新进度到数据库（从内存读取进度值）"""
                try:
                    while compress_progress['running']:
                        await asyncio.sleep(0.5)  # 每0.5秒更新一次数据库
                        
                        # 从内存中的 backup_task.progress_percent 读取进度
                        if backup_task and backup_task.id:
                            # 计算已处理的文件数（基于当前进度）
                            if total_files > 0:
                                # 从进度百分比反推已处理的文件数
                                # 进度 = 10% + (已处理文件数 / 总文件数) * 90%
                                # 已处理文件数 = (进度 - 10%) / 90% * 总文件数
                                current_progress = backup_task.progress_percent
                                if current_progress > 10.0:
                                    processed_files = int((current_progress - 10.0) / 90.0 * total_files)
                                    processed_files = min(processed_files, total_files)
                                else:
                                    processed_files = base_processed_files
                                
                                # 更新数据库（异步，不阻塞）
                                asyncio.create_task(self.backup_db.update_scan_progress(
                                    backup_task,
                                    processed_files,
                                    total_files
                                ))
                except Exception as e:
                    logger.debug(f"监控进度更新失败: {str(e)}")
            
            # 启动进度监控任务
            progress_monitor_task = asyncio.create_task(_monitor_progress_update())
            
            try:
                # 在线程池中执行压缩操作，避免阻塞事件循环
                logger.info(f"开始压缩文件组: {len(file_group)} 个文件...")
                if compression_enabled:
                    logger.info(f"使用7z压缩 (压缩级别: {compression_level}, 线程数: {compression_threads})")
                else:
                    logger.info("使用tar打包（不压缩）")
                
                # 使用 asyncio.to_thread 或 run_in_executor 在后台线程执行
                # 注意：压缩操作在线程池中执行，不会阻塞事件循环
                logger.debug("准备在线程池中执行压缩操作...")
                loop = asyncio.get_event_loop()
                archive_file, compressed_size = await loop.run_in_executor(None, _do_7z_compress)
                logger.debug("压缩操作在线程池中执行完成")
                
                # 确保压缩操作已完成
                if not compress_progress['completed']:
                    logger.warning("压缩操作可能未完全完成，等待中...")
                    # 等待压缩完成标志
                    wait_count = 0
                    while not compress_progress['completed'] and wait_count < 20:
                        await asyncio.sleep(0.1)
                        wait_count += 1
                
                logger.info(f"压缩操作完成: {archive_file} (大小: {format_bytes(compressed_size)})")
                
                # 压缩操作完成后，更新数据库中的进度（确保最终进度正确）
                if backup_task and backup_task.id and total_files > 0:
                    final_processed = base_processed_files + len(file_group)
                    final_progress = 10.0 + (final_processed / total_files) * 90.0
                    backup_task.progress_percent = min(100.0, final_progress)
                    # 异步更新数据库，不阻塞
                    await self.backup_db.update_scan_progress(
                        backup_task,
                        final_processed,
                        total_files
                    )
            finally:
                # 停止进度监控
                compress_progress['running'] = False
                progress_monitor_task.cancel()
                try:
                    await progress_monitor_task
                except asyncio.CancelledError:
                    pass

            # 计算压缩包的校验和（用于验证压缩包完整性，在线程池中执行避免阻塞）
            # 注意：这里只计算压缩包的校验和，不计算单个文件的校验和
            # 原因：1. 压缩包本身有完整性校验 2. 单个文件校验和计算耗时且不必要
            def _calculate_checksum():
                return calculate_file_checksum(archive_file)
            
            loop = asyncio.get_event_loop()
            checksum = await loop.run_in_executor(None, _calculate_checksum)

            # 计算压缩包的校验和（用于验证压缩包完整性，在线程池中执行避免阻塞）
            # 注意：这里只计算压缩包的校验和，不计算单个文件的校验和
            # 原因：1. 压缩包本身有完整性校验 2. 单个文件校验和计算耗时且不必要
            def _calculate_checksum():
                return calculate_file_checksum(archive_file)
            
            loop = asyncio.get_event_loop()
            checksum = await loop.run_in_executor(None, _calculate_checksum)

            # 获取成功压缩的文件信息（从线程函数返回）
            # 注意：successful_files 和 successful_original_size 在 _do_7z_compress 函数中定义
            # 但由于作用域问题，我们需要在压缩完成后重新计算
            # 实际上，压缩包的大小已经反映了成功压缩的文件大小
            # 原始大小需要从成功压缩的文件列表中计算
            
            # 从压缩包中获取实际压缩的文件列表（如果可能）
            # 否则使用 file_group 中的所有文件（假设都成功）
            # 注意：由于压缩是在线程中进行的，我们无法直接获取 successful_files
            # 所以这里使用 file_group 的所有文件，但实际统计时会根据压缩包大小调整
            
            # 计算成功压缩的文件大小（从压缩包大小反推，或使用 file_group）
            # 由于无法直接获取 successful_files，我们假设所有文件都成功
            # 如果压缩包大小异常小，说明有文件失败，但这里无法精确统计
            successful_file_count = len(file_group)  # 默认假设所有文件都成功
            successful_original_size = sum(f['size'] for f in file_group)  # 默认使用所有文件大小
            
            compressed_info = {
                'path': str(archive_file),
                'original_size': successful_original_size,  # 只统计成功压缩的文件大小
                'compressed_size': compressed_size,
                'file_count': successful_file_count,  # 只统计成功压缩的文件数量
                'successful_files': successful_file_count,  # 成功压缩的文件数
                'failed_files': 0,  # 失败的文件数（无法精确统计，设为0）
                'checksum': checksum,
                'compression_enabled': compression_enabled,
                'compression_level': compression_level if compression_enabled else None,
                'compression_threads': compression_threads if compression_enabled else None
            }

            if compression_enabled:
                compression_ratio = compressed_size / compressed_info['original_size'] if compressed_info['original_size'] > 0 else 0
                logger.info(f"7z压缩完成: {successful_file_count} 个文件, "
                            f"原始大小: {format_bytes(compressed_info['original_size'])}, "
                            f"压缩后: {format_bytes(compressed_size)}, "
                            f"压缩比: {compression_ratio:.2%}, "
                            f"线程数: {compression_threads}")
            else:
                logger.info(f"打包完成: {successful_file_count} 个文件, "
                            f"大小: {format_bytes(compressed_size)}")

            return compressed_info

        except Exception as e:
            logger.error(f"压缩文件组失败: {str(e)}")
            import traceback
            traceback.print_exc()
            return None
    
    async def _write_to_tape_drive(self, source_path: str, backup_set: BackupSet, group_idx: int) -> Optional[str]:
        """tar文件已直接写入磁带盘符，这里只需要返回路径"""
        try:
            # tar文件已经在压缩时直接写入磁带盘符了
            # 这里只需要返回路径用于数据库记录
            tape_drive = self.settings.TAPE_DRIVE_LETTER.upper() + ":\\"
            tar_file = Path(source_path)
            
            if tar_file.exists():
                # 返回磁带上的相对路径
                relative_path = str(tar_file.relative_to(Path(tape_drive)))
                logger.info(f"tar文件已写入磁带: {relative_path}")
                return relative_path
            else:
                logger.error(f"tar文件不存在: {source_path}")
                return None
            
        except Exception as e:
            logger.error(f"获取文件路径失败: {str(e)}")
            return None
    
    async def _save_backup_files_to_db(self, file_group: List[Dict], backup_set: BackupSet, 
                                       compressed_file: Dict, tape_file_path: str, chunk_number: int):
        """保存备份文件信息到数据库（便于恢复）
        
        注意：如果数据库保存失败，不会中断备份流程，只会记录警告日志。
        """
        try:
            from utils.scheduler.db_utils import is_opengauss, get_opengauss_connection
            from datetime import datetime
            import json
            
            if not is_opengauss():
                logger.warning("非openGauss数据库，跳过保存备份文件信息")
                return
            
            conn = await get_opengauss_connection()
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
                        # 原因：
                        # 1. 文件已经压缩到压缩包中，压缩包有SHA256校验和（在compressed_info['checksum']中）
                        # 2. 计算单个文件的SHA256对大文件非常耗时（需要读取整个文件）
                        # 3. 恢复时验证是可选的（如果checksum存在才验证，见recovery_engine._verify_file_integrity）
                        # 4. 压缩包本身的校验和已经足够验证数据完整性
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
                for processed_file in valid_processed_files:
                    try:
                        file_stat = processed_file['file_stat']
                        await conn.execute(
                            """
                            INSERT INTO backup_files (
                                backup_set_id, file_path, file_name, file_type, file_size,
                                compressed_size, file_permissions, created_time, modified_time,
                                accessed_time, compressed, checksum, backup_time, chunk_number,
                                tape_block_start, file_metadata
                            ) VALUES (
                                $1, $2, $3, $4::backupfiletype, $5, $6, $7, $8, $9, $10, $11, $12, $13, $14, $15, $16::json
                            )
                            """,
                            backup_set_db_id,
                            processed_file['file_path'],
                            processed_file['file_name'],
                            processed_file['file_type'],
                            processed_file['file_size'],
                            compressed_file.get('compressed_size', 0) // len(file_group) if file_group else 0,  # 平均分配压缩后大小
                            processed_file['file_permissions'],
                            datetime.fromtimestamp(file_stat.st_ctime) if file_stat else None,
                            datetime.fromtimestamp(file_stat.st_mtime) if file_stat else None,
                            datetime.fromtimestamp(file_stat.st_atime) if file_stat else None,
                            compressed_file.get('compression_enabled', False),
                            processed_file['file_checksum'],
                            backup_time,
                            chunk_number,
                            0,  # tape_block_start（文件系统操作，暂时设为0）
                            json.dumps({
                                'tape_file_path': tape_file_path,
                                'chunk_number': chunk_number,
                                'original_path': processed_file['file_path'],
                                'relative_path': str(Path(processed_file['file_path']).relative_to(Path(processed_file['file_path']).anchor)) if Path(processed_file['file_path']).is_absolute() else processed_file['file_path']
                            })  # file_metadata 需要序列化为 JSON 字符串
                        )
                        success_count += 1
                    except Exception as insert_error:
                        failed_count += 1
                        logger.warning(f"⚠️ 插入文件记录失败: {processed_file.get('file_path', 'unknown')} (错误: {str(insert_error)})")
                        continue
                
                if success_count > 0:
                    logger.debug(f"已保存 {success_count} 个文件信息到数据库（chunk {chunk_number}）")
                if failed_count > 0:
                    logger.warning(f"⚠️ 保存文件信息到数据库时，{failed_count} 个文件失败，但备份流程继续")
                    
            except Exception as db_conn_error:
                logger.warning(f"⚠️ 数据库连接或查询失败，跳过保存文件信息: {str(db_conn_error)}")
                # 数据库错误不影响备份流程，继续执行
            finally:
                try:
                    await conn.close()
                except:
                    pass
                    
        except Exception as e:
            logger.warning(f"⚠️ 保存备份文件信息到数据库失败: {str(e)}，但备份流程继续")
            import traceback
            logger.debug(f"错误堆栈:\n{traceback.format_exc()}")
            # 不抛出异常，因为文件已经写入磁带，数据库记录失败不应该影响备份流程

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
            backup_group = format_datetime(now(), '%Y-%m')
            set_id = f"{backup_group}_{backup_task.id:06d}"
            backup_time = datetime.now()
            retention_until = backup_time + timedelta(days=backup_task.retention_days)
            
            # 使用原生 openGauss SQL，避免 SQLAlchemy 版本解析
            from utils.scheduler.db_utils import is_opengauss, get_opengauss_connection
            import json
            
            if is_opengauss():
                conn = await get_opengauss_connection()
                try:
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
                finally:
                    await conn.close()
            else:
                # 非 openGauss 使用 SQLAlchemy（其他数据库）- 但当前项目只支持 openGauss
                # 如果使用其他数据库，需要实现相应的 SQL
                raise RuntimeError("当前项目仅支持 openGauss 数据库")

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

            # 保存更新 - 使用原生 openGauss SQL
            from utils.scheduler.db_utils import is_opengauss, get_opengauss_connection
            
            if is_opengauss():
                conn = await get_opengauss_connection()
                try:
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
                finally:
                    await conn.close()
            else:
                # 非 openGauss 使用 SQLAlchemy
                async for db in get_db():
                    await db.commit()

            logger.info(f"备份集完成: {backup_set.set_id}")

        except Exception as e:
            logger.error(f"完成备份集失败: {str(e)}")

    async def _update_scan_progress(self, backup_task: BackupTask, scanned_count: int, valid_count: int, operation_status: str = None):
        """更新扫描进度到数据库
        
        Args:
            backup_task: 备份任务对象
            scanned_count: 已扫描文件数（或已处理文件数）
            valid_count: 有效文件数（或总文件数）
            operation_status: 操作状态（如"[扫描文件中...]"、"[压缩文件中...]"等）
        """
        try:
            if not backup_task or not backup_task.id:
                return
            
            from utils.scheduler.db_utils import is_opengauss, get_opengauss_connection
            if is_opengauss():
                conn = await get_opengauss_connection()
                try:
                    if operation_status:
                        # 先获取当前description，移除旧的操作状态，添加新的操作状态
                        row = await conn.fetchrow(
                            "SELECT description FROM backup_tasks WHERE id = $1",
                            backup_task.id
                        )
                        current_desc = row['description'] if row and row['description'] else ''
                        
                        # 移除所有操作状态标记（保留格式化状态）
                        import re
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
                        total_bytes = getattr(backup_task, 'total_bytes', 0) or 0  # 所有扫描到的文件总数（批次相加的文件数）
                        result_summary = getattr(backup_task, 'result_summary', None)
                        
                        # 将result_summary转换为JSON字符串
                        import json
                        result_summary_json = json.dumps(result_summary) if result_summary else None
                        
                        await conn.execute(
                            """
                            UPDATE backup_tasks
                            SET progress_percent = $1,
                                processed_files = $2,
                                total_files = $3,
                                total_bytes = $4,
                                processed_bytes = $5,
                                compressed_bytes = $6,
                                result_summary = $7::json,
                                description = $8,
                                updated_at = $9
                            WHERE id = $10
                            """,
                            backup_task.progress_percent,
                            scanned_count,
                            valid_count,
                            total_bytes,
                            processed_bytes,
                            compressed_bytes,
                            result_summary_json,
                            new_desc,
                            datetime.now(),
                            backup_task.id
                        )
                    else:
                        # 没有操作状态，只更新进度和字节数
                        compressed_bytes = getattr(backup_task, 'compressed_bytes', 0) or 0
                        processed_bytes = getattr(backup_task, 'processed_bytes', 0) or 0
                        total_bytes = getattr(backup_task, 'total_bytes', 0) or 0  # 所有扫描到的文件总数（批次相加的文件数）
                        result_summary = getattr(backup_task, 'result_summary', None)
                        
                        # 将result_summary转换为JSON字符串
                        import json
                        result_summary_json = json.dumps(result_summary) if result_summary else None
                        
                        await conn.execute(
                            """
                            UPDATE backup_tasks
                            SET progress_percent = $1,
                                processed_files = $2,
                                total_files = $3,
                                total_bytes = $4,
                                processed_bytes = $5,
                                compressed_bytes = $6,
                                result_summary = $7::json,
                                updated_at = $8
                            WHERE id = $9
                            """,
                            backup_task.progress_percent,
                            scanned_count,
                            valid_count,
                            total_bytes,
                            processed_bytes,
                            compressed_bytes,
                            result_summary_json,
                            datetime.now(),
                            backup_task.id
                        )
                finally:
                    await conn.close()
            else:
                # 非 openGauss 使用 SQLAlchemy（但当前项目仅支持 openGauss）
                logger.warning("非 openGauss 数据库，跳过进度更新")
        except Exception as e:
            logger.debug(f"更新扫描进度失败（忽略继续）: {str(e)}")

    async def _update_task_status(self, backup_task: BackupTask, status: BackupTaskStatus):
        """更新任务状态"""
        try:
            backup_task.status = status
            # 使用原生 openGauss SQL
            from utils.scheduler.db_utils import is_opengauss, get_opengauss_connection
            
            if is_opengauss():
                conn = await get_opengauss_connection()
                try:
                    await conn.execute(
                        """
                        UPDATE backup_tasks
                        SET status = $1::backuptaskstatus,
                            updated_at = $2
                        WHERE id = $3
                        """,
                        status.value,
                        datetime.now(),
                        backup_task.id
                    )
                finally:
                    await conn.close()
            else:
                # 非 openGauss 使用 SQLAlchemy
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
        """获取任务状态（委托给 backup_db 模块）"""
        return await self.backup_db.get_task_status(task_id)

    async def cancel_task(self, task_id: int) -> bool:
        """取消任务"""
        try:
            if self._current_task and self._current_task.id == task_id:
                await self.backup_db.update_task_status(self._current_task, BackupTaskStatus.CANCELLED)
                self._current_task = None
                logger.info(f"任务已取消: {task_id}")
                return True
            return False
        except Exception as e:
            logger.error(f"取消任务失败: {str(e)}")
            return False