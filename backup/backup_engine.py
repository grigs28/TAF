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

logger = logging.getLogger(__name__)


def normalize_volume_label(label: Optional[str], year: int, month: int) -> str:
    target_year = f"{year:04d}"
    target_month = f"{month:02d}"
    default_seq = "01"
    default_label = f"TP{target_year}{target_month}{default_seq}"

    if not label:
        return default_label

    clean_label = label.strip().upper()

    def build_label(seq: str, suffix: str = "") -> str:
        seq = (seq if seq and seq.isdigit() else default_seq).zfill(2)[:2]
        return f"TP{target_year}{target_month}{seq}{suffix}"

    match = re.match(r'^TP(\d{4})(\d{2})(\d{2})(.*)$', clean_label)
    if match:
        return build_label(match.group(3), match.group(4))

    match = re.match(r'^TP(\d{4})(\d{2})(\d+)(.*)$', clean_label)
    if match:
        return build_label(match.group(3), match.group(4))

    match = re.match(r'^TAPE(\d{4})(\d{2})(\d{2})(.*)$', clean_label)
    if match:
        return build_label(match.group(3), match.group(4))

    match = re.match(r'^TAPE(\d{4})(\d{2})(\d+)(.*)$', clean_label)
    if match:
        return build_label(match.group(3), match.group(4))

    match = re.search(r'(\d{4})(\d{2})(\d{2})', clean_label)
    if match:
        return build_label(match.group(3))

    return default_label


def extract_label_year_month(label: Optional[str]) -> Optional[Dict[str, int]]:
    if not label:
        return None

    clean_label = label.strip().upper()

    match = re.search(r'TP(\d{4})(\d{2})', clean_label)
    if match:
        return {
            "year": int(match.group(1)),
            "month": int(match.group(2))
        }

    match = re.search(r'TAPE(\d{4})(\d{2})', clean_label)
    if match:
        return {
            "year": int(match.group(1)),
            "month": int(match.group(2))
        }

    match = re.search(r'(\d{4})(\d{2})', clean_label)
    if match:
        return {
            "year": int(match.group(1)),
            "month": int(match.group(2))
        }

    return None


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
                       f"最大文件大小={self._format_bytes(policy['max_file_size'])}")
            
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
                       f"最大文件大小={self._format_bytes(backup_policy.get('max_file_size', 0))}, "
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
            logger.info(f"========== 检查是否需要格式化 ==========")
            logger.info(f"任务类型: {backup_task.task_type} (类型: {type(backup_task.task_type)}, FULL={BackupTaskType.FULL})")
            
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
            
            if task_type_value == full_type_value or backup_task.task_type == BackupTaskType.FULL:
                logger.info("========== 完整备份前格式化处理 ==========")
                logger.info("检测到完整备份任务，执行格式化前检查...")
                
                # 初始化格式化进度为0%
                backup_task.progress_percent = 0.0
                await self._update_scan_progress(backup_task, 0, 0)
                
                if self.tape_manager:
                    try:
                        tape_ops = self.tape_manager.tape_operations
                        if tape_ops and hasattr(tape_ops, 'erase_preserve_label'):
                            logger.info("开始执行格式化（保留卷标信息）...")

                            # 定义进度回调函数，用于更新进度到数据库
                            async def update_format_progress(task, current, total):
                                """更新格式化进度到数据库"""
                                try:
                                    await self._update_scan_progress(task, current, total)
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
                                await self._update_scan_progress(backup_task, 1, 1)
                                
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
                                await self._update_task_status(backup_task, BackupTaskStatus.FAILED)
                                
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
                        await self._update_task_status(backup_task, BackupTaskStatus.FAILED)
                        
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
                await self._update_scan_progress(backup_task, 0, 0)
            else:
                logger.info(f"任务类型为 {backup_task.task_type}，不是完整备份（FULL），跳过格式化步骤")

            # 更新任务状态
            logger.info("========== 更新任务状态为运行中 ==========")
            await self._update_task_status(backup_task, BackupTaskStatus.RUNNING)
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
            success = await self._perform_backup(backup_task)

            # 更新任务完成状态
            task_end_time = now()
            backup_task.completed_at = task_end_time
            duration_seconds = (task_end_time - task_start_time).total_seconds()
            duration_ms = int(duration_seconds * 1000)
            
            if success:
                logger.info("========== 备份任务执行成功 ==========")
                logger.info(f"处理文件数: {backup_task.processed_files}")
                logger.info(f"处理字节数: {self._format_bytes(backup_task.processed_bytes)}")
                logger.info(f"执行耗时: {duration_seconds:.2f} 秒")
                logger.info(f"完成时间: {format_datetime(task_end_time)}")
                
                await self._update_task_status(backup_task, BackupTaskStatus.COMPLETED)
                
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
                    result_message=f"处理 {backup_task.processed_files} 个文件，总大小 {self._format_bytes(backup_task.processed_bytes)}",
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
                        "processed_files": backup_task.processed_files,
                        "processed_bytes": backup_task.processed_bytes,
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
                                    'size': self._format_bytes(backup_task.processed_bytes),
                                    'file_count': backup_task.processed_files,
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
                
                await self._update_task_status(backup_task, BackupTaskStatus.FAILED)
                
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
                await self._update_task_status(backup_task, BackupTaskStatus.FAILED)
            return False
        finally:
            self._current_task = None

    async def _perform_backup(self, backup_task: BackupTask) -> bool:
        """执行备份流程"""
        try:
            # 1. 扫描源文件
            logger.info("扫描源文件...")
            # 初始化扫描进度
            if backup_task:
                backup_task.progress_percent = 0.0
                await self._update_scan_progress(backup_task, 0, 0)
            
            file_list = await self._scan_source_files(backup_task.source_paths, backup_task.exclude_patterns, backup_task)
            backup_task.total_files = len(file_list)
            backup_task.total_bytes = sum(f['size'] for f in file_list)

            # 2. 检查磁带盘符是否可用（简单检查）
            logger.info("检查磁带盘符...")
            tape_drive = self.settings.TAPE_DRIVE_LETTER.upper() + ":\\"
            if not os.path.exists(tape_drive):
                raise RuntimeError(f"磁带盘符不存在: {tape_drive}，请检查配置")
            
            logger.info(f"磁带盘符可用: {tape_drive}")
            
            # 3. 获取或创建磁带信息（简化处理）
            tape_id = "TAPE001"  # 默认磁带ID，可以从数据库获取或自动生成
            if self.tape_manager:
                try:
                    # 尝试从数据库获取当前磁带或创建新记录
                    current_tape = await self._get_current_drive_tape()
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

            # 4. 创建备份集（简化磁带对象）
            from tape.tape_cartridge import TapeCartridge, TapeStatus
            tape_obj = TapeCartridge(
                tape_id=tape_id,
                label=f"备份磁带-{tape_id}",
                status=TapeStatus.IN_USE,
                capacity_bytes=self.settings.MAX_VOLUME_SIZE,
                used_bytes=0
            )
            backup_set = await self._create_backup_set(backup_task, tape_obj)

            # 5. 分组压缩文件
            logger.info("分组压缩文件...")
            file_groups = await self._group_files_for_compression(file_list)

            # 6. 处理每个文件组
            processed_files = 0
            total_size = 0

            for group_idx, file_group in enumerate(file_groups):
                logger.info(f"处理文件组 {group_idx + 1}/{len(file_groups)}")

                # 压缩文件组（根据备份任务的压缩设置，带进度跟踪）
                compressed_file = await self._compress_file_group(
                    file_group, 
                    backup_set, 
                    backup_task, 
                    base_processed_files=processed_files,
                    total_files=backup_task.total_files
                )
                if not compressed_file:
                    continue

                # tar文件已直接写入磁带盘符，获取路径用于数据库记录
                tape_file_path = await self._write_to_tape_drive(compressed_file['path'], backup_set, group_idx)
                if not tape_file_path:
                    logger.warning(f"无法获取tar文件路径: {compressed_file['path']}")
                    # 继续执行，因为文件已经写入磁带

                # 保存文件信息到数据库（便于恢复）
                await self._save_backup_files_to_db(
                    file_group, 
                    backup_set, 
                    compressed_file, 
                    tape_file_path or compressed_file['path'], 
                    group_idx
                )

                # 更新进度（基于文件数量和tar文件大小）
                processed_files += len(file_group)
                total_size += compressed_file['compressed_size']
                backup_task.processed_files = processed_files
                backup_task.processed_bytes = total_size
                backup_task.compressed_bytes = total_size
                backup_task.progress_percent = (processed_files / backup_task.total_files) * 100

                # 通知进度更新
                await self._notify_progress(backup_task)

            # 7. 完成备份集
            await self._finalize_backup_set(backup_set, processed_files, total_size)

            logger.info("备份完成")
            return True

        except Exception as e:
            logger.error(f"备份流程执行失败: {str(e)}")
            backup_task.error_message = str(e)
            return False

    async def _scan_source_files(self, source_paths: List[str], exclude_patterns: List[str], 
                                  backup_task: Optional[BackupTask] = None) -> List[Dict]:
        """扫描源文件"""
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
                    file_info = await self._get_file_info(source_path)
                    if file_info and not self._should_exclude_file(file_info['path'], exclude_patterns):
                        file_list.append(file_info)
                        logger.debug(f"已添加文件: {file_info['path']}")
                    
                    total_scanned += 1
                    # 更新扫描进度
                    if backup_task and estimated_total > 0:
                        scan_progress = min(10.0, (total_scanned / estimated_total) * 10.0)  # 扫描占10%进度
                        backup_task.progress_percent = scan_progress
                        await self._update_scan_progress(backup_task, total_scanned, len(file_list))
                        
                elif source_path.is_dir():
                    logger.info(f"扫描目录: {source_path_str}")
                    scanned_count = 0
                    excluded_count = 0
                    
                    # 使用 rglob 递归扫描，但需要处理可能的异常
                    try:
                        for file_path in source_path.rglob('*'):
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
                                        await self._update_scan_progress(backup_task, total_scanned, len(file_list))
                                
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

        # 扫描完成，更新进度
        if backup_task:
            backup_task.progress_percent = 10.0  # 扫描完成，进度10%
            await self._update_scan_progress(backup_task, total_scanned, len(file_list))

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

    async def _compress_file_group(self, file_group: List[Dict], backup_set: BackupSet, backup_task: BackupTask,
                                   base_processed_files: int = 0, total_files: int = 0) -> Optional[Dict]:
        """压缩文件组（简单的tar备份，带进度跟踪）"""
        try:
            import tarfile
            import threading
            import time
            
            # 从备份任务获取压缩设置
            compression_enabled = getattr(backup_task, 'compression_enabled', True)
            
            # 从系统配置获取压缩级别
            compression_level = self.settings.COMPRESSION_LEVEL
            
            # 创建临时文件（直接写入磁带盘符，不创建临时文件）
            timestamp = format_datetime(now(), '%Y%m%d_%H%M%S')
            tape_drive = self.settings.TAPE_DRIVE_LETTER.upper() + ":\\"
            backup_dir = Path(tape_drive) / backup_set.set_id
            backup_dir.mkdir(parents=True, exist_ok=True)
            
            # 进度跟踪变量
            tar_progress = {'bytes_written': 0, 'running': True}
            total_original_size = sum(f['size'] for f in file_group)
            
            # 将tar操作放到线程池中执行，避免阻塞事件循环
            def _do_tar_compress():
                """在线程中执行tar压缩操作，带进度跟踪"""
                if compression_enabled:
                    # 使用tar.gz，直接写入磁带盘符
                    tar_file = backup_dir / f"backup_{backup_set.set_id}_{timestamp}.tar.gz"
                    compresslevel = max(1, min(9, compression_level))
                    
                    with tarfile.open(tar_file, 'w:gz', compresslevel=compresslevel) as tar_archive:
                        for file_idx, file_info in enumerate(file_group):
                            file_path = Path(file_info['path'])
                            if file_path.exists():
                                # 计算相对路径（保留目录结构）
                                try:
                                    # 尝试从源路径计算相对路径
                                    source_paths = backup_task.source_paths or []
                                    if source_paths:
                                        # 找到匹配的源路径
                                        for src_path in source_paths:
                                            src = Path(src_path)
                                            try:
                                                if file_path.is_relative_to(src):
                                                    arcname = str(file_path.relative_to(src))
                                                    break
                                            except (ValueError, AttributeError):
                                                continue
                                        else:
                                            arcname = file_path.name
                                    else:
                                        arcname = file_path.name
                                except Exception:
                                    arcname = file_path.name
                                
                                tar_archive.add(file_path, arcname=arcname)
                                
                                # 更新进度：基于已打包的文件数量（已打包文件数/总文件数）
                                if total_files > 0:
                                    current_processed = base_processed_files + file_idx + 1
                                    # 扫描阶段占10%，tar压缩阶段占90%
                                    # 在tar阶段，进度从10%到100%
                                    tar_progress_value = 10.0 + (current_processed / total_files) * 90.0
                                    tar_progress['bytes_written'] = tar_file.stat().st_size if tar_file.exists() else 0
                                    
                                    # 更新任务进度（在后台线程中，需要异步更新）
                                    if backup_task and backup_task.id:
                                        # 这里不能直接更新数据库，需要在主线程中更新
                                        # 但可以更新内存中的进度值
                                        backup_task.progress_percent = min(100.0, tar_progress_value)
                    
                    tar_progress['running'] = False
                    return tar_file, tar_file.stat().st_size
                else:
                    # 使用tar，不压缩，直接写入磁带盘符
                    tar_file = backup_dir / f"backup_{backup_set.set_id}_{timestamp}.tar"
                    
                    with tarfile.open(tar_file, 'w') as tar_archive:
                        for file_idx, file_info in enumerate(file_group):
                            file_path = Path(file_info['path'])
                            if file_path.exists():
                                try:
                                    source_paths = backup_task.source_paths or []
                                    if source_paths:
                                        for src_path in source_paths:
                                            src = Path(src_path)
                                            try:
                                                if file_path.is_relative_to(src):
                                                    arcname = str(file_path.relative_to(src))
                                                    break
                                            except (ValueError, AttributeError):
                                                continue
                                        else:
                                            arcname = file_path.name
                                    else:
                                        arcname = file_path.name
                                except Exception:
                                    arcname = file_path.name
                                
                                tar_archive.add(file_path, arcname=arcname)
                                
                                # 更新进度：基于已打包的文件数量（已打包文件数/总文件数）
                                if total_files > 0:
                                    current_processed = base_processed_files + file_idx + 1
                                    # 扫描阶段占10%，tar压缩阶段占90%
                                    tar_progress_value = 10.0 + (current_processed / total_files) * 90.0
                                    tar_progress['bytes_written'] = tar_file.stat().st_size if tar_file.exists() else 0
                                    
                                    if backup_task:
                                        backup_task.progress_percent = min(100.0, tar_progress_value)
                    
                    tar_progress['running'] = False
                    return tar_file, tar_file.stat().st_size
            
            # 启动进度监控任务：定期从内存读取进度并更新到数据库
            async def _monitor_progress_update():
                """监控并更新进度到数据库（从内存读取进度值）"""
                try:
                    while tar_progress['running']:
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
                                asyncio.create_task(self._update_scan_progress(
                                    backup_task,
                                    processed_files,
                                    total_files
                                ))
                except Exception as e:
                    logger.debug(f"监控进度更新失败: {str(e)}")
            
            # 启动进度监控任务
            progress_monitor_task = asyncio.create_task(_monitor_progress_update())
            
            try:
                # 在线程池中执行tar操作，避免阻塞事件循环
                logger.info(f"开始压缩文件组: {len(file_group)} 个文件...")
                if compression_enabled:
                    logger.info(f"使用tar.gz压缩 (压缩级别: {max(1, min(9, compression_level))})")
                else:
                    logger.info("使用tar打包（不压缩）")
                
                # 使用 asyncio.to_thread 或 run_in_executor 在后台线程执行
                loop = asyncio.get_event_loop()
                tar_file, compressed_size = await loop.run_in_executor(None, _do_tar_compress)
                
                logger.info(f"tar操作完成: {tar_file} (大小: {self._format_bytes(compressed_size)})")
                
                # tar操作完成后，更新数据库中的进度（确保最终进度正确）
                if backup_task and backup_task.id and total_files > 0:
                    final_processed = base_processed_files + len(file_group)
                    final_progress = 10.0 + (final_processed / total_files) * 90.0
                    backup_task.progress_percent = min(100.0, final_progress)
                    # 异步更新数据库，不阻塞
                    await self._update_scan_progress(
                        backup_task,
                        final_processed,
                        total_files
                    )
            finally:
                # 停止进度监控
                tar_progress['running'] = False
                progress_monitor_task.cancel()
                try:
                    await progress_monitor_task
                except asyncio.CancelledError:
                    pass

            # 计算校验和（也在线程池中执行，避免阻塞）
            def _calculate_checksum():
                return self._calculate_file_checksum(tar_file)
            
            loop = asyncio.get_event_loop()
            checksum = await loop.run_in_executor(None, _calculate_checksum)

            compressed_info = {
                'path': str(tar_file),
                'original_size': sum(f['size'] for f in file_group),
                'compressed_size': compressed_size,
                'file_count': len(file_group),
                'checksum': checksum,
                'compression_enabled': compression_enabled,
                'compression_level': compression_level if compression_enabled else None
            }

            if compression_enabled:
                compression_ratio = compressed_size / compressed_info['original_size'] if compressed_info['original_size'] > 0 else 0
                logger.info(f"压缩完成: {len(file_group)} 个文件, "
                            f"原始大小: {self._format_bytes(compressed_info['original_size'])}, "
                            f"压缩后: {self._format_bytes(compressed_size)}, "
                            f"压缩比: {compression_ratio:.2%}")
            else:
                logger.info(f"打包完成: {len(file_group)} 个文件, "
                            f"大小: {self._format_bytes(compressed_size)}")

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
        """保存备份文件信息到数据库（便于恢复）"""
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
                    logger.error(f"找不到备份集: {backup_set.set_id}")
                    return
                
                backup_set_db_id = backup_set_row['id']
                backup_time = datetime.now()
                
                # 批量插入文件信息
                for file_info in file_group:
                    file_path = Path(file_info['path'])
                    
                    # 确定文件类型（使用枚举值）
                    if file_info.get('is_dir', False):
                        file_type = BackupFileType.DIRECTORY.value
                    elif file_info.get('is_symlink', False):
                        # 符号链接：如果枚举中有 SYMLINK 则使用，否则视为文件
                        if hasattr(BackupFileType, 'SYMLINK'):
                            file_type = BackupFileType.SYMLINK.value
                        else:
                            file_type = BackupFileType.FILE.value
                    else:
                        file_type = BackupFileType.FILE.value
                    
                    # 获取文件元数据
                    file_stat = file_path.stat() if file_path.exists() else None
                    
                    # 计算文件校验和（仅对文件，不对目录）
                    file_checksum = None
                    if file_type == 'file' and file_path.exists():
                        try:
                            file_checksum = self._calculate_file_checksum(file_path)
                        except Exception as e:
                            logger.warning(f"计算文件校验和失败 {file_path}: {str(e)}")
                    
                    # 获取文件权限（Windows上可能不可用）
                    file_permissions = None
                    if file_stat:
                        try:
                            file_permissions = oct(file_stat.st_mode)[-3:]
                        except:
                            pass
                    
                    # 插入文件记录
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
                        str(file_path),
                        file_path.name,
                        file_type,
                        file_info.get('size', 0),
                        compressed_file.get('compressed_size', 0) // len(file_group) if file_group else 0,  # 平均分配压缩后大小
                        file_permissions,
                        datetime.fromtimestamp(file_stat.st_ctime) if file_stat else None,
                        datetime.fromtimestamp(file_stat.st_mtime) if file_stat else None,
                        datetime.fromtimestamp(file_stat.st_atime) if file_stat else None,
                        compressed_file.get('compression_enabled', False),
                        file_checksum,
                        backup_time,
                        chunk_number,
                        0,  # tape_block_start（文件系统操作，暂时设为0）
                        json.dumps({
                            'tape_file_path': tape_file_path,
                            'chunk_number': chunk_number,
                            'original_path': str(file_path),
                            'relative_path': str(file_path.relative_to(file_path.anchor)) if file_path.is_absolute() else str(file_path)
                        })  # file_metadata 需要序列化为 JSON 字符串
                    )
                
                logger.debug(f"已保存 {len(file_group)} 个文件信息到数据库（chunk {chunk_number}）")
                
            finally:
                await conn.close()
                
        except Exception as e:
            logger.error(f"保存备份文件信息到数据库失败: {str(e)}")
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

    async def _update_scan_progress(self, backup_task: BackupTask, scanned_count: int, valid_count: int):
        """更新扫描进度到数据库"""
        try:
            if not backup_task or not backup_task.id:
                return
            
            from utils.scheduler.db_utils import is_opengauss, get_opengauss_connection
            if is_opengauss():
                conn = await get_opengauss_connection()
                try:
                    await conn.execute(
                        """
                        UPDATE backup_tasks
                        SET progress_percent = $1,
                            processed_files = $2,
                            total_files = $3,
                            updated_at = $4
                        WHERE id = $5
                        """,
                        backup_task.progress_percent,
                        scanned_count,
                        valid_count,
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
        """获取任务状态"""
        try:
            # 使用原生 openGauss SQL
            from utils.scheduler.db_utils import is_opengauss, get_opengauss_connection
            
            if is_opengauss():
                conn = await get_opengauss_connection()
                try:
                    row = await conn.fetchrow(
                        """
                        SELECT id, status, progress_percent, processed_files, total_files,
                               processed_bytes, total_bytes
                        FROM backup_tasks
                        WHERE id = $1
                        """,
                        task_id
                    )
                    
                    if row:
                        return {
                            'task_id': task_id,
                            'status': row['status'],
                            'progress_percent': row['progress_percent'] or 0.0,
                            'processed_files': row['processed_files'] or 0,
                            'total_files': row['total_files'] or 0,
                            'processed_bytes': row['processed_bytes'] or 0,
                            'total_bytes': row['total_bytes'] or 0
                        }
                finally:
                    await conn.close()
            else:
                # 非 openGauss - 返回当前任务信息（如果存在）
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