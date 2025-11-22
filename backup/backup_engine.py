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
import time
from datetime import datetime, timedelta
from pathlib import Path
from typing import List, Dict, Any, Optional, Callable
import py7zr
import psutil

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
from backup.backup_notifier import BackupNotifier
from backup.backup_scanner import BackupScanner
from backup.backup_task_manager import BackupTaskManager
from backup.tape_file_mover import TapeFileMover
from backup.compression_worker import CompressionWorker
from backup.file_move_worker import FileMoveWorker

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
        
        # 初始化子模块
        self.file_scanner = FileScanner(settings=self.settings)
        self.compressor = Compressor(settings=self.settings)
        self.backup_db = BackupDB()
        self.tape_handler = TapeHandler(tape_manager=None, settings=self.settings)
        self.backup_notifier = BackupNotifier(dingtalk_notifier=None)
        self.backup_scanner = BackupScanner(file_scanner=self.file_scanner, backup_db=self.backup_db)
        self.task_manager = BackupTaskManager(settings=self.settings)
        
        # 初始化文件移动队列管理器（延迟初始化，需要tape_handler）
        self.tape_file_mover: Optional[TapeFileMover] = None

    async def _get_notification_events(self) -> Dict[str, bool]:
        """获取通知事件配置（带缓存）- 委托给 BackupNotifier"""
        return await self.backup_notifier.get_notification_events()

    async def _get_backup_policy_parameters(self) -> Dict[str, Any]:
        """获取备份策略参数（从tapedrive和system配置）- 委托给 BackupNotifier"""
        return await self.backup_notifier.get_backup_policy_parameters(self.settings)
    
    async def _update_tape_path_in_db(self, backup_set: BackupSet, compressed_file: Dict, 
                                     tape_file_path: str, group_idx: int):
        """更新数据库中的磁带路径（在文件移动完成后调用）"""
        try:
            # 这里可以更新数据库中的磁带路径
            # 由于backup_db.save_backup_files_to_db已经保存了初始路径，这里可以更新为最终路径
            logger.debug(f"更新数据库中的磁带路径: {tape_file_path} (组索引: {group_idx})")
            # 如果需要更新，可以在这里实现
        except Exception as e:
            logger.warning(f"更新数据库中的磁带路径失败: {str(e)}")

    async def initialize(self):
        """初始化备份引擎"""
        try:
            import shutil
            
            # 先清除临时目录（启动时清理残留文件）
            compress_dir = Path(self.settings.BACKUP_COMPRESS_DIR)
            temp_dirs = [
                self.settings.BACKUP_TEMP_DIR,
                self.settings.RECOVERY_TEMP_DIR,
                self.settings.BACKUP_COMPRESS_DIR,
                compress_dir / "temp",  # temp临时目录
                compress_dir / "final",  # final正式目录
            ]
            
            # 清除所有临时目录内容（但不删除目录本身）
            for temp_dir_path in temp_dirs:
                temp_dir = Path(temp_dir_path)
                if temp_dir.exists():
                    try:
                        # 删除目录下的所有文件和子目录
                        for item in temp_dir.iterdir():
                            if item.is_dir():
                                shutil.rmtree(item, ignore_errors=True)
                            else:
                                item.unlink(missing_ok=True)
                        logger.info(f"已清除临时目录: {temp_dir}")
                    except Exception as e:
                        logger.warning(f"清除临时目录失败 {temp_dir}: {str(e)}")
            
            # 重新创建临时目录（确保目录存在）
            for temp_dir in temp_dirs:
                Path(temp_dir).mkdir(parents=True, exist_ok=True)
            
            # 初始化文件移动队列管理器
            # 获取主事件循环，以便在线程中使用
            try:
                main_loop = asyncio.get_running_loop()
            except RuntimeError:
                main_loop = None
            
            self.tape_file_mover = TapeFileMover(
                tape_handler=self.tape_handler,
                settings=self.settings,
                main_loop=main_loop
            )
            self.tape_file_mover.start()
            logger.info("文件移动队列管理器已启动")

            self._initialized = True
            logger.info("备份引擎初始化完成")

        except Exception as e:
            logger.error(f"备份引擎初始化失败: {str(e)}")
            raise
    
    async def shutdown(self):
        """关闭备份引擎，停止文件移动队列管理器"""
        try:
            if self.tape_file_mover:
                logger.info("正在停止文件移动队列管理器...")
                self.tape_file_mover.stop()
                self.tape_file_mover = None
                logger.info("文件移动队列管理器已停止")
        except Exception as e:
            logger.error(f"关闭备份引擎时发生错误: {str(e)}")

    def set_dependencies(self, tape_manager: TapeManager, dingtalk_notifier: DingTalkNotifier):
        """设置依赖组件"""
        self.tape_manager = tape_manager
        self.dingtalk_notifier = dingtalk_notifier
        # 更新子模块的依赖
        self.tape_handler.tape_manager = tape_manager
        self.backup_notifier.dingtalk_notifier = dingtalk_notifier

    def add_progress_callback(self, callback: Callable):
        """添加进度回调 - 委托给 BackupNotifier"""
        self.backup_notifier.add_progress_callback(callback)

    async def create_backup_task(self, task_name: str, source_paths: List[str],
                               task_type: BackupTaskType = BackupTaskType.FULL,
                               **kwargs) -> Optional[BackupTask]:
        """创建备份任务 - 委托给 BackupTaskManager"""
        return await self.task_manager.create_backup_task(task_name, source_paths, task_type, **kwargs)

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
                        # 使用连接池
                        async with get_opengauss_connection() as conn:
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
            elif manual_run:
                logger.info("========== 手动运行模式，跳过任务执行状态检查 ==========")

            # 2. 检查任务是否正在执行
            logger.info("========== 执行前检查：任务运行状态 ==========")
            from utils.scheduler.db_utils import is_opengauss, get_opengauss_connection
            
            if is_opengauss():
                # 使用连接池
                async with get_opengauss_connection() as conn:
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
                                    # 使用连接池
                                    async with get_opengauss_connection() as conn:
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
                        await self.backup_db.update_task_stage_with_description(
                            backup_task,
                            "failed",
                            f"[任务失败] {error_msg[:100]}{'...' if len(error_msg) > 100 else ''}"
                        )
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
                            # 使用连接池
                            async with get_opengauss_connection() as conn:
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

            # 更新任务状态（同时更新 source_paths 和 tape_id，以便任务卡片正确显示）
            logger.info("========== 更新任务状态为运行中 ==========")
            # 注意：此时 tape_id 还未设置，将在 _perform_backup 中设置后再次更新
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

            # 初始化任务状态
            await self.backup_db.update_task_stage_with_description(
                backup_task,
                "scan",
                "[准备开始] 正在初始化备份任务..."
            )

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
                
                # 更新最终状态
                await self.backup_db.update_task_stage_with_description(
                    backup_task,
                    "finalize",
                    f"[备份完成] 处理了 {processed_files} 个文件，总大小 {format_bytes(processed_bytes)}"
                )
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
                
                # 更新失败状态
                await self.backup_db.update_task_stage_with_description(
                    backup_task,
                    "failed",
                    f"[任务失败] {error_msg[:100]}{'...' if len(error_msg) > 100 else ''}"
                )
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
            from utils.scheduler.db_utils import is_opengauss, is_redis, get_opengauss_connection
            from utils.scheduler.sqlite_utils import is_sqlite
            
            if is_opengauss():
                # 使用连接池
                async with get_opengauss_connection() as conn:
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
            elif is_redis():
                # Redis模式：不需要提交事务，直接跳过
                pass
            elif is_sqlite():
                # SQLite模式：使用 SQLAlchemy
                from utils.scheduler.sqlite_utils import is_sqlite
                from config.database import get_db
                if is_sqlite() and db_manager.AsyncSessionLocal and callable(db_manager.AsyncSessionLocal):
                    async for db in get_db():
                        await db.commit()
                # 其他情况跳过

            logger.info(f"========== 备份任务执行完成 ==========")
            logger.info(f"任务名称: {task_name}")
            logger.info(f"任务ID: {task_id}")
            logger.info(f"执行结果: {'成功' if success else '失败'}")
            logger.info(f"总耗时: {duration_seconds:.2f} 秒")
            
            return success

        except KeyboardInterrupt:
            # 用户按 Ctrl+C 中止任务
            logger.warning("========== 用户中止备份任务执行（Ctrl+C） ==========")
            logger.warning(f"任务名称: {task_name}")
            logger.warning(f"任务ID: {task_id}")
            
            # 更新任务状态为取消
            if backup_task:
                try:
                    backup_task.error_message = "用户中止任务（Ctrl+C）"
                    await self.backup_db.update_task_status(backup_task, BackupTaskStatus.CANCELLED)
                    await self.backup_db.update_scan_progress(backup_task, 
                                                            backup_task.processed_files if hasattr(backup_task, 'processed_files') else 0,
                                                            backup_task.total_files if hasattr(backup_task, 'total_files') else 0, 
                                                            "[已取消]")
                    logger.info("任务状态已更新为取消")
                except Exception as update_error:
                    logger.error(f"更新任务状态失败: {str(update_error)}")
            
            # 重新抛出 KeyboardInterrupt，让上层处理
            raise
            
        except asyncio.CancelledError:
            # 任务被取消
            logger.warning("========== 备份任务执行被取消 ==========")
            logger.warning(f"任务名称: {task_name}")
            logger.warning(f"任务ID: {task_id}")
            
            # 更新任务状态为取消
            if backup_task:
                try:
                    backup_task.error_message = "任务被取消"
                    await self.backup_db.update_task_status(backup_task, BackupTaskStatus.CANCELLED)
                    await self.backup_db.update_scan_progress(backup_task, 
                                                            backup_task.processed_files if hasattr(backup_task, 'processed_files') else 0,
                                                            backup_task.total_files if hasattr(backup_task, 'total_files') else 0, 
                                                            "[已取消]")
                    logger.info("任务状态已更新为取消")
                except Exception as update_error:
                    logger.error(f"更新任务状态失败: {str(update_error)}")
            
            # 重新抛出 CancelledError，让上层处理
            raise

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
                            # 使用连接池
                            async with get_opengauss_connection() as conn:
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
        scan_progress_task = None  # 后台扫描任务
        try:
            # 初始化扫描进度
            if backup_task:
                backup_task.progress_percent = 0.0
                await self.backup_db.update_scan_progress(backup_task, 0, 0)
            
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
            
            # 初始化备份任务的统计信息
            backup_task.processed_files = 0
            backup_task.processed_bytes = 0  # 原始文件的总大小（未压缩）
            backup_task.total_files = 0  # total_files: 总文件数（由后台扫描任务更新）
            backup_task.total_bytes = 0  # total_bytes: 总字节数（由后台扫描任务更新）
            
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
            
            # 更新数据库中的 tape_id，以便任务卡片正确显示
            logger.info(f"更新数据库中的 tape_id: {tape_id}")
            await self.backup_db.update_task_fields(backup_task, tape_id=tape_id)

            # 3. 创建备份集（简化磁带对象）
            from tape.tape_cartridge import TapeCartridge, TapeStatus
            tape_obj = TapeCartridge(
                tape_id=tape_id,
                label=f"备份磁带-{tape_id}",
                status=TapeStatus.IN_USE,
                capacity_bytes=self.settings.MAX_VOLUME_SIZE,
                used_bytes=0
            )
            backup_set = None
            if getattr(backup_task, 'backup_set_id', None):
                backup_set = await self.backup_db.get_backup_set_by_set_id(backup_task.backup_set_id)
                if backup_set:
                    logger.info(f"检测到已有备份集 {backup_task.backup_set_id}，继续使用")
            if not backup_set:
                backup_set = await self.backup_db.create_backup_set(backup_task, tape_obj)

            # 4. 流式处理：扫描和压缩循环执行
            logger.info("开始流式处理：扫描和压缩循环执行...")
            logger.info(f"压缩配置：最大文件大小={format_bytes(self.settings.MAX_FILE_SIZE)}")
            
            scan_status = getattr(backup_task, 'scan_status', 'pending') or 'pending'
            force_rescan = getattr(backup_task, 'force_rescan', False)
            should_run_scanner = force_rescan or scan_status != 'completed'
            restart_scan = force_rescan or scan_status in (None, '', 'pending', 'failed', 'cancelled')
            
            if should_run_scanner:
                logger.info("========== 启动后台扫描任务，写入 backup_files ==========")

                # 记录关键阶段：扫描文件开始
                self.backup_db._log_operation_stage_event(
                    backup_task,
                    "[扫描文件中...]"
                )
                # 更新operation_stage和description
                await self.backup_db.update_task_stage_with_description(
                    backup_task,
                    "scan",
                    "[扫描文件中] 正在扫描源路径..."
                )

                scan_progress_task = asyncio.create_task(
                    self.backup_scanner.scan_for_progress_update(
                        backup_task,
                        backup_task.source_paths,
                        exclude_patterns,
                        backup_set,
                        restart=restart_scan
                    )
                )
                logger.info("后台扫描任务已启动")
                logger.info("等待后台扫描写入文件记录（90秒）...")
                await asyncio.sleep(90)
            else:
                logger.info("扫描状态为 completed，跳过扫描阶段")
            
            logger.info("========== 开始从数据库读取待压缩文件 ==========")

            # 记录关键阶段：开始压缩
            self.backup_db._log_operation_stage_event(
                backup_task,
                "[准备压缩...]"
            )
            # 更新operation_stage和description
            await self.backup_db.update_task_stage_with_description(
                backup_task,
                "compress",
                "[准备压缩] 正在构建文件组..."
            )

            # ========== 设置压缩和移动线程的专属日志处理器 ==========
            try:
                from utils.worker_log_handler import WorkerLogHandler, ScheduleType
                worker_log_handler = WorkerLogHandler()
                
                # 从 scheduled_task 中获取调度类型
                schedule_type = ScheduleType.UNKNOWN
                if scheduled_task and hasattr(scheduled_task, 'schedule_type'):
                    schedule_type_str = str(scheduled_task.schedule_type).lower()
                    if 'daily' in schedule_type_str:
                        schedule_type = ScheduleType.DAILY
                    elif 'monthly' in schedule_type_str:
                        schedule_type = ScheduleType.MONTHLY
                else:
                    # 尝试从 backup_task 推断
                    schedule_type = WorkerLogHandler.get_schedule_type_from_task(backup_task)
                
                # 设置专属日志处理器
                worker_log_handler.setup_worker_loggers(schedule_type)
                logger.info(f"已设置压缩和移动线程专属日志处理器（备份周期: {schedule_type.value}）")
            except Exception as log_setup_error:
                logger.warning(f"设置专属日志处理器失败: {str(log_setup_error)}，将使用默认日志")
            
            # ========== 创建文件移动后台任务（独立扫描 final 目录） ==========
            file_move_worker = FileMoveWorker(tape_file_mover=self.tape_file_mover)
            file_move_worker.start()
            
            # ========== 创建压缩循环后台任务（独立线程，顺序执行） ==========
            compression_worker = CompressionWorker(
                backup_db=self.backup_db,
                compressor=self.compressor,
                backup_set=backup_set,
                backup_task=backup_task,
                settings=self.settings,
                file_move_worker=None,  # 不再向 file_move_worker 发送消息，它独立扫描 final 目录
                backup_notifier=self.backup_notifier,
                tape_file_mover=self.tape_file_mover
            )
            compression_worker.start()

            # ========== 等待压缩循环完成 ==========
            processed_files = 0
            total_size = 0
            try:
                await compression_worker.compression_task
                # 压缩完成，获取统计信息
                processed_files = compression_worker.processed_files
                total_size = compression_worker.total_size
            except (KeyboardInterrupt, asyncio.CancelledError):
                logger.warning("========== 压缩循环被中止 ==========")
                logger.warning(f"任务ID: {backup_task.id if backup_task else 'N/A'}")
                # 即使被中断，也尝试获取已处理的统计信息
                try:
                    processed_files = compression_worker.processed_files
                    total_size = compression_worker.total_size
                except:
                    pass
                raise
            finally:
                # 停止压缩循环和文件移动任务
                await compression_worker.stop()
                await file_move_worker.stop()
            
            logger.info(f"========== 数据库压缩完成，共处理 {compression_worker.group_idx} 个文件组 ==========")

            # 5. 完成备份集
            await self.backup_db.finalize_backup_set(backup_set, processed_files, total_size)
            
            # 注意：不再更新 total_files 字段（压缩包数量）
            # 压缩包数量存储在 result_summary.estimated_archive_count 中
            # total_files 和 total_bytes 字段由独立的后台扫描任务 _scan_for_progress_update 负责更新（总文件数和总字节数）
            
            # 更新操作状态为完成
            await self.backup_db.update_scan_progress(backup_task, processed_files, backup_task.total_files, "[完成备份集...]")

            logger.info(f"备份完成，共处理 {processed_files} 个文件，总大小 {format_bytes(total_size)}")
            return True

        except KeyboardInterrupt:
            # 用户按 Ctrl+C 中止任务
            logger.warning("========== 用户中止备份任务（Ctrl+C） ==========")
            logger.warning(f"任务ID: {backup_task.id if backup_task else 'N/A'}")
            
            # 更新任务状态为取消
            if backup_task:
                try:
                    backup_task.error_message = "用户中止任务（Ctrl+C）"
                    await self.backup_db.update_task_stage_with_description(
                        backup_task,
                        "cancelled",
                        "[任务取消] 用户中止任务（Ctrl+C）"
                    )
                    await self.backup_db.update_task_status(backup_task, BackupTaskStatus.CANCELLED)
                    await self.backup_db.update_scan_progress(backup_task, processed_files if 'processed_files' in locals() else 0,
                                                            backup_task.total_files if hasattr(backup_task, 'total_files') else 0,
                                                            "[已取消]")
                    logger.info("任务状态已更新为取消")
                except Exception as update_error:
                    logger.error(f"更新任务状态失败: {str(update_error)}")
            
            # 重新抛出 KeyboardInterrupt，让上层处理
            raise
            
        except asyncio.CancelledError:
            # 任务被取消
            logger.warning("========== 备份任务被取消 ==========")
            logger.warning(f"任务ID: {backup_task.id if backup_task else 'N/A'}")
            
            # 更新任务状态为取消
            if backup_task:
                try:
                    backup_task.error_message = "任务被取消"
                    await self.backup_db.update_task_stage_with_description(
                        backup_task,
                        "cancelled",
                        "[任务取消] 任务被系统取消"
                    )
                    await self.backup_db.update_task_status(backup_task, BackupTaskStatus.CANCELLED)
                    await self.backup_db.update_scan_progress(backup_task, processed_files if 'processed_files' in locals() else 0,
                                                            backup_task.total_files if hasattr(backup_task, 'total_files') else 0,
                                                            "[已取消]")
                    logger.info("任务状态已更新为取消")
                except Exception as update_error:
                    logger.error(f"更新任务状态失败: {str(update_error)}")
            
            # 重新抛出 CancelledError，让上层处理
            raise
            
        except Exception as e:
            logger.error(f"备份流程执行失败: {str(e)}")
            import traceback
            logger.error(f"错误堆栈:\n{traceback.format_exc()}")
            if backup_task:
                backup_task.error_message = str(e)
            return False
            
        finally:
            # 清理资源：取消后台扫描任务
            if scan_progress_task and not scan_progress_task.done():
                logger.info("========== 取消后台扫描任务 ==========")
                try:
                    scan_progress_task.cancel()
                    # 等待任务取消完成（带超时）
                    try:
                        await asyncio.wait_for(scan_progress_task, timeout=5.0)
                    except asyncio.TimeoutError:
                        logger.warning("后台扫描任务取消超时，强制终止")
                    except asyncio.CancelledError:
                        logger.info("后台扫描任务已成功取消")
                    except Exception as cancel_error:
                        logger.warning(f"取消后台扫描任务时发生错误: {str(cancel_error)}")
                except Exception as cleanup_error:
                    logger.error(f"清理后台扫描任务失败: {str(cleanup_error)}")
                finally:
                    logger.info("后台扫描任务清理完成")

    async def get_task_status(self, task_id: int) -> Optional[Dict]:
        """获取任务状态 - 委托给 BackupTaskManager"""
        return await self.task_manager.get_task_status(task_id)

    async def cancel_task(self, task_id: int) -> bool:
        """取消任务 - 委托给 BackupTaskManager"""
        try:
            # 取消当前任务
            if self._current_task and self._current_task.id == task_id:
                result = await self.task_manager.cancel_task(task_id)
                if result:
                    self._current_task = None
                return result
            else:
                # 取消其他任务
                return await self.task_manager.cancel_task(task_id)
        except Exception as e:
            logger.error(f"取消任务失败: {str(e)}")
            return False
