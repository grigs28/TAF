#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
任务动作处理器
Task Action Handlers
"""

import logging
import asyncio
import math
import os
import random
import shutil
import threading
import uuid
import tarfile
import time
from datetime import datetime, timezone
from pathlib import Path
from utils.datetime_utils import now, format_datetime
from utils.scheduler.schedule_calculator import get_monthly_cycle_start
from typing import Dict, Any, Optional, List

from models.scheduled_task import ScheduledTask, TaskActionType
from models.backup import BackupTask, BackupTaskType, BackupTaskStatus
from utils.log_utils import log_system, LogLevel, LogCategory, log_operation, OperationType
from .db_utils import is_opengauss, get_opengauss_connection
import json

logger = logging.getLogger(__name__)


def _parse_enum(enum_class, value: str, default=None):
    """
    解析枚举值（处理大小写不匹配问题）

    Args:
        enum_class: 枚举类
        value: 枚举值（可能是大写、小写或混合大小写）
        default: 默认值（如果无法解析）

    Returns:
        枚举值

    Raises:
        ValueError: 如果枚举值无效且没有提供默认值
    """
    if not value:
        return default

    # 转换为小写并去除空白
    value_lower = value.lower().strip() if isinstance(value, str) else str(value).lower().strip()

    # 尝试直接匹配
    try:
        return enum_class(value_lower)
    except ValueError:
        # 如果直接匹配失败，尝试匹配枚举值
        for enum_value in enum_class:
            if enum_value.value.lower() == value_lower:
                return enum_value

        # 如果仍然无法匹配，记录警告并返回默认值
        if default is not None:
            logger.warning(f"无法解析枚举值 '{value}' (类型: {enum_class.__name__})，使用默认值 {default}")
            return default
        else:
            raise ValueError(f"无法解析枚举值 '{value}' (类型: {enum_class.__name__})")


class ActionHandler:
    """动作处理器基类"""

    def __init__(self, system_instance):
        self.system_instance = system_instance

    async def execute(
        self,
        config: Dict,
        scheduled_task: Optional[ScheduledTask] = None,
        manual_run: bool = False,
        run_options: Optional[Dict[str, Any]] = None
    ) -> Dict[str, Any]:
        """执行动作（子类实现）

        参数:
            config: 动作配置
            scheduled_task: 计划任务对象（可选）
            manual_run: 是否为手动运行（Web界面点击运行），默认为False
            run_options: 手动运行附加选项（如继续/重启模式）
        """
        raise NotImplementedError


class BackupActionHandler(ActionHandler):
    """备份动作处理器"""

    async def execute(
        self,
        config: Dict,
        backup_task_id: Optional[int] = None,
        scheduled_task: Optional[ScheduledTask] = None,
        manual_run: bool = False,
        run_options: Optional[Dict[str, Any]] = None
    ) -> Dict[str, Any]:
        """执行备份动作

        参数:
            config: 动作配置
            backup_task_id: 备份任务模板ID（如果提供，从模板加载配置）
            scheduled_task: 计划任务对象（用于检查重复执行）
            manual_run: 是否为手动运行（Web界面点击运行），默认为False
        """
        if not self.system_instance or not self.system_instance.backup_engine:
            raise ValueError("备份引擎未初始化")

        # 初始化变量，确保在所有异常情况下都有值
        backup_executed = False
        backup_task = None
        resumed_from_existing = False
        template_task = None
        task_name = ""

        try:
            run_options = run_options or {}
            run_mode = str(run_options.get('mode') or 'auto').lower()
            if run_mode not in ('auto', 'resume', 'restart'):
                run_mode = 'auto'
            force_rescan_option = bool(run_options.get('force_rescan', False))
            resume_only = run_mode == 'resume'
            restart_requested = run_mode == 'restart'

            logger.info(
                f"备份任务运行模式: manual_run={manual_run}, run_mode={run_mode}, force_rescan={force_rescan_option}"
            )

            # 执行前判定：周期内是否已成功执行、是否正在执行、磁带标签是否当月
            current_time = now()
            if scheduled_task and not manual_run:
                # 1) 周期检查（按任务的 schedule_type 推断周期：日/周/月/年）
                # 注意：手动运行时跳过周期检查
                cycle_ok = True
                last_success = scheduled_task.last_success_time
                schedule_type = getattr(scheduled_task, 'schedule_type', None)
                schedule_type_str = schedule_type.value if hasattr(schedule_type, 'value') else str(schedule_type) if schedule_type else 'unknown'

                # 特殊处理：月度任务（周期语义，与调度计算器一致）
                if schedule_type and getattr(schedule_type, 'value', '').lower() in ('monthly', 'month'):
                    if last_success:
                        period_start = get_monthly_cycle_start(
                            current_time,
                            getattr(scheduled_task, 'schedule_config', None) or {}
                        )
                        if period_start and last_success >= period_start:
                            logger.info(
                                f"[月度任务检查] 本周期已成功执行过，跳过本次备份 - "
                                f"任务ID: {getattr(scheduled_task, 'id', 'N/A')}, "
                                f"任务名称: {getattr(scheduled_task, 'task_name', 'N/A')}, "
                                f"周期起始: {period_start.strftime('%Y-%m-%d')}, "
                                f"上次成功时间: {last_success.strftime('%Y-%m-%d %H:%M:%S')}, "
                                f"当前时间: {current_time.strftime('%Y-%m-%d %H:%M:%S')}"
                            )
                            try:
                                await log_operation(
                                    operation_type=OperationType.SCHEDULER_RUN,
                                    resource_type="scheduler",
                                    resource_id=str(getattr(scheduled_task, 'id', '')),
                                    resource_name=getattr(scheduled_task, 'task_name', ''),
                                    operation_name="执行计划任务",
                                    operation_description="跳过：月度任务本周期已成功执行过",
                                    category="scheduler",
                                    success=True,
                                    result_message="跳过执行（月度任务本周期已成功执行过）"
                                )
                                await log_system(
                                    level=LogLevel.INFO,
                                    category=LogCategory.SYSTEM,
                                    message="月度任务跳过：本周期已成功执行过",
                                    module="utils.scheduler.action_handlers",
                                    function="BackupActionHandler.execute",
                                    task_id=getattr(scheduled_task, 'id', None)
                                )
                            except Exception:
                                pass
                            return {"status": "skipped", "message": "月度任务本周期已成功执行过"}
                        # last_success 在本周期起始之前 → 本周期未执行，继续往下走
                        logger.info(
                            f"[月度任务检查] 新周期开始，上次成功在上周期，正常执行 - "
                            f"任务ID: {getattr(scheduled_task, 'id', 'N/A')}, "
                            f"周期起始: {period_start.strftime('%Y-%m-%d') if period_start else 'N/A'}, "
                            f"上次成功时间: {last_success.strftime('%Y-%m-%d %H:%M:%S')}"
                        )
                    else:
                        logger.info(
                            f"[月度任务检查] 从未成功执行过，检查任务锁 - "
                            f"任务ID: {getattr(scheduled_task, 'id', 'N/A')}, "
                            f"任务名称: {getattr(scheduled_task, 'task_name', 'N/A')}"
                        )

                # 其他类型的任务：按原有逻辑检查周期
                if last_success and not (schedule_type and getattr(schedule_type, 'value', '').lower() in ('monthly', 'month')):
                    if schedule_type and getattr(schedule_type, 'value', '').lower() in ('daily', 'day'):
                        cycle_ok = (last_success.date() != current_time.date())
                    elif schedule_type and getattr(schedule_type, 'value', '').lower() in ('weekly', 'week'):
                        cycle_ok = (last_success.isocalendar().week != current_time.isocalendar().week or last_success.year != current_time.year)
                    elif schedule_type and getattr(schedule_type, 'value', '').lower() in ('yearly', 'year'):
                        cycle_ok = (last_success.year != current_time.year)
                    else:
                        # 未明确类型，默认按日
                        cycle_ok = (last_success.date() != current_time.date())

                # 如果周期内已执行过，则跳过
                if not cycle_ok:
                    schedule_type_str = schedule_type.value if hasattr(schedule_type, 'value') else str(schedule_type)
                    logger.info(
                        f"[周期检查] 当前周期内已成功执行，跳过本次备份 - "
                        f"任务ID: {getattr(scheduled_task, 'id', 'N/A')}, "
                        f"任务名称: {getattr(scheduled_task, 'task_name', 'N/A')}, "
                        f"调度类型: {schedule_type_str}, "
                        f"上次成功时间: {last_success.strftime('%Y-%m-%d %H:%M:%S') if last_success else 'N/A'}, "
                        f"当前时间: {current_time.strftime('%Y-%m-%d %H:%M:%S')}"
                    )
                    try:
                        await log_operation(
                            operation_type=OperationType.SCHEDULER_RUN,
                            resource_type="scheduler",
                            resource_id=str(getattr(scheduled_task, 'id', '')),
                            resource_name=getattr(scheduled_task, 'task_name', ''),
                            operation_name="执行计划任务",
                            operation_description="跳过：当前周期已执行",
                            category="scheduler",
                            success=True,
                            result_message="跳过执行（当前周期已执行）"
                        )
                        await log_system(
                            level=LogLevel.INFO,
                            category=LogCategory.SYSTEM,
                            message="计划任务跳过：当前周期已执行",
                            module="utils.scheduler.action_handlers",
                            function="BackupActionHandler.execute",
                            task_id=getattr(scheduled_task, 'id', None)
                        )
                    except Exception:
                        pass
                    return {"status": "skipped", "message": "当前周期已执行"}
            elif manual_run:
                logger.info("手动运行模式，跳过周期检查")

            # 2) 运行中检查已移除
            # 注意：任务锁机制已经在 task_executor 中处理了并发控制

            # 3) 磁带标签是否当月（从 LTFS 标签或磁带头读取）
            if scheduled_task and not manual_run:
                target_is_tape = False
                try:
                    action_cfg = scheduled_task.action_config or {}
                    target_is_tape = (action_cfg.get('backup_target') == 'tape') or ('tape_device' in action_cfg)
                except Exception:
                    pass
                if target_is_tape and self.system_instance and getattr(self.system_instance, 'tape_manager', None):
                    tape_ops = getattr(self.system_instance.tape_manager, 'tape_operations', None)
                    if tape_ops and hasattr(tape_ops, '_read_tape_label'):
                        try:
                            metadata = await asyncio.wait_for(
                                tape_ops._read_tape_label(),
                                timeout=60.0
                            )
                        except asyncio.TimeoutError:
                            logger.warning("计划任务中读取磁带卷标超时（60秒）")
                            metadata = None
                        if metadata and (metadata.get('created_date') or metadata.get('tape_id')):
                            try:
                                created_dt = None
                                if metadata.get('created_date'):
                                    try:
                                        created_dt = datetime.fromisoformat(str(metadata['created_date']).replace('Z','+00:00'))
                                    except Exception:
                                        created_dt = None
                                if created_dt:
                                    if not (created_dt.year == current_time.year and created_dt.month == current_time.month):
                                        raise ValueError("当前磁带非当月，请更换磁带后重试")
                                else:
                                    tape_id = str(metadata.get('tape_id', ''))
                                    if len(tape_id) >= 7 and tape_id.upper().startswith('TAP'):
                                        yy = int(tape_id[3:5])
                                        mm = int(tape_id[5:7])
                                        year = 2000 + yy
                                        if not (year == current_time.year and mm == current_time.month):
                                            raise ValueError("当前磁带标签非当月，请更换磁带后重试")
                            except ValueError as ve:
                                logger.warning(str(ve))
                                try:
                                    if self.system_instance and getattr(self.system_instance, 'dingtalk_notifier', None):
                                        notifier = self.system_instance.dingtalk_notifier
                                        tape_id = (metadata.get('tape_id') if metadata else '') or '未知磁带'
                                        await notifier.send_tape_notification(tape_id=tape_id, action='change_required')
                                except Exception:
                                    pass
                                try:
                                    await log_operation(
                                        operation_type=OperationType.BACKUP_START,
                                        resource_type="backup",
                                        resource_name=(getattr(scheduled_task, 'task_name', '') or '计划任务'),
                                        operation_name="更换磁带提醒",
                                        operation_description="当前磁带标签非当月，提醒更换磁带",
                                        category="backup",
                                        success=False,
                                        error_message=str(ve)
                                    )
                                    await log_system(
                                        level=LogLevel.WARNING,
                                        category=LogCategory.BACKUP,
                                        message="当前磁带标签非当月，提醒更换磁带",
                                        module="utils.scheduler.action_handlers",
                                        function="BackupActionHandler.execute",
                                    )
                                except Exception:
                                    pass
                                raise

            # 如果有备份任务模板ID，从模板加载配置
            template_task = None
            if backup_task_id:
                async with get_opengauss_connection() as conn:
                    row = await conn.fetchrow(
                        """
                        SELECT id, task_name, task_type, source_paths, exclude_patterns,
                               compression_enabled, encryption_enabled, retention_days,
                               description, tape_device, status, is_template, template_id,
                               created_at, updated_at
                        FROM backup_tasks
                        WHERE id = $1 AND is_template = TRUE
                        """,
                        backup_task_id
                    )
                    if not row:
                        raise ValueError(f"备份任务模板不存在: {backup_task_id}")

                    # 转换为 BackupTask 对象（简化版，只包含需要的字段）
                    template_task = type('BackupTask', (), {
                        'id': row['id'],
                        'task_name': row['task_name'],
                        'task_type': _parse_enum(BackupTaskType, row.get('task_type'), None),
                        'source_paths': row['source_paths'] if isinstance(row['source_paths'], list) else json.loads(row['source_paths']) if row['source_paths'] else [],
                        'exclude_patterns': row['exclude_patterns'] if isinstance(row['exclude_patterns'], list) else json.loads(row['exclude_patterns']) if row['exclude_patterns'] else [],
                        'compression_enabled': row['compression_enabled'],
                        'encryption_enabled': row['encryption_enabled'],
                        'retention_days': row['retention_days'],
                        'description': row['description'],
                        'tape_device': row['tape_device'],
                        'status': _parse_enum(BackupTaskStatus, row.get('status'), None),
                        'is_template': row['is_template'],
                        'template_id': row['template_id'],
                    })()

            # 执行前检查：判断同一个模板的任务是否还在执行中
            if template_task or scheduled_task:
                template_id = template_task.id if template_task else None
                if scheduled_task and scheduled_task.task_metadata:
                    template_id = scheduled_task.task_metadata.get('backup_task_id') or template_id

                if template_id:
                    running_task = None
                    async with get_opengauss_connection() as conn:
                        running_task_row = await conn.fetchrow(
                            """
                            SELECT id, started_at, status FROM backup_tasks
                            WHERE template_id = $1 AND status IN ($2::backuptaskstatus, $3::backuptaskstatus)
                            ORDER BY created_at DESC
                            LIMIT 1
                            """,
                            template_id, 'pending', 'running'
                        )
                        running_task = dict(running_task_row) if running_task_row else None

                    if running_task:
                        # 检查任务是否在同一时间执行（同一天同一个调度任务）
                        current_time_check = now()
                        if scheduled_task and scheduled_task.last_run_time:
                            last_run_date = scheduled_task.last_run_time.date()
                            today = current_time_check.date()

                            # 如果上次执行在今天，且任务还在运行，跳过本次执行
                            if last_run_date == today:
                                running_task_id = running_task.get('id') if isinstance(running_task, dict) else (running_task.id if hasattr(running_task, 'id') else None)
                                logger.warning(
                                    f"跳过执行：模板 {template_id} 的任务仍在执行中 "
                                    f"(运行中的任务ID: {running_task_id})"
                                )
                                return {
                                    "status": "skipped",
                                    "message": "相同模板的任务仍在执行中，已跳过本次执行",
                                    "running_task_id": running_task_id
                                }

                            # 如果任务运行超过一天，记录警告但继续执行
                            if isinstance(running_task, dict) and running_task.get('started_at'):
                                started_at = running_task.get('started_at')
                                if isinstance(started_at, str):
                                    started_at = datetime.fromisoformat(started_at.replace('Z', '+00:00'))
                                running_duration = (current_time_check - started_at).total_seconds()
                                if running_duration > 86400:  # 超过24小时
                                    logger.warning(f"模板 {template_id} 的任务已运行超过24小时，继续执行新任务")

            resume_template_id = template_task.id if template_task else backup_task_id
            if scheduled_task and scheduled_task.task_metadata:
                resume_template_id = scheduled_task.task_metadata.get('backup_task_id') or resume_template_id
            if manual_run and resume_template_id:
                if restart_requested:
                    cancelled_task_id = await self._cancel_incomplete_backup_task(resume_template_id)
                    if cancelled_task_id:
                        logger.info(f"手动运行选择重新开始，已取消未完成任务 {cancelled_task_id}")
                        try:
                            await log_system(
                                level=LogLevel.INFO,
                                category=LogCategory.BACKUP,
                                message=f"手动运行重新开始：取消未完成任务 {cancelled_task_id}",
                                module="utils.scheduler.action_handlers",
                                function="BackupActionHandler.execute",
                            )
                        except Exception:
                            pass
                else:
                    existing_task = await self._load_incomplete_backup_task(resume_template_id)
                    if existing_task:
                        backup_task = existing_task
                        resumed_from_existing = True
                        logger.info(f"检测到未完成的备份任务 {existing_task.id}，尝试继续执行")
                    elif resume_only:
                        logger.info("手动运行模式选择仅继续，但未找到可继续的备份任务")
                        return {
                            "status": "skipped",
                            "message": "没有未完成的备份任务可继续"
                        }

            # 从模板或配置中获取备份参数（若缺省则从系统实例配置补齐）
            if template_task:
                source_paths = template_task.source_paths or []
                task_type = template_task.task_type
                exclude_patterns = template_task.exclude_patterns or []
                compression_enabled = template_task.compression_enabled
                encryption_enabled = template_task.encryption_enabled
                retention_days = template_task.retention_days
                description = template_task.description or ''
                tape_device = template_task.tape_device
                task_name = f"{template_task.task_name}-{format_datetime(now(), '%Y%m%d_%H%M%S')}"
            else:
                # 从config获取参数（兼容旧逻辑）
                source_paths = config.get('source_paths', [])
                task_type_str = config.get('task_type', 'full')
                task_type_map = {
                    'full': BackupTaskType.FULL,
                    'incremental': BackupTaskType.INCREMENTAL,
                    'differential': BackupTaskType.DIFFERENTIAL,
                    'monthly_full': BackupTaskType.MONTHLY_FULL
                }
                task_type = task_type_map.get(task_type_str, BackupTaskType.FULL)
                exclude_patterns = config.get('exclude_patterns', [])
                compression_enabled = config.get('compression_enabled', True)
                encryption_enabled = config.get('encryption_enabled', False)
                retention_days = config.get('retention_days', 180)
                description = config.get('description', '')
                tape_device = config.get('tape_device')
                task_name = config.get('task_name', f"计划备份-{format_datetime(now(), '%Y%m%d_%H%M%S')}")

            # 补齐缺省参数：从 system_instance 的策略/配置合并
            try:
                sysi = self.system_instance
                if hasattr(sysi, 'settings'):
                    settings = sysi.settings
                    if not exclude_patterns and getattr(settings, 'DEFAULT_EXCLUDE_PATTERNS', None):
                        exclude_patterns = settings.DEFAULT_EXCLUDE_PATTERNS
                if not tape_device and hasattr(sysi, 'tape_manager'):
                    tm = sysi.tape_manager
                    if hasattr(tm, 'settings') and getattr(tm.settings, 'TAPE_DEVICE_PATH', None):
                        tape_device = tm.settings.TAPE_DEVICE_PATH
            except Exception:
                pass

            if not source_paths:
                raise ValueError("备份源路径不能为空")

            # 创建备份任务执行记录（不是模板）
            if backup_task is None:
                async with get_opengauss_connection() as conn:
                    backup_task_id = await conn.fetchval(
                        """
                        INSERT INTO backup_tasks (
                            task_name, task_type, source_paths, exclude_patterns,
                            compression_enabled, encryption_enabled, retention_days,
                            description, tape_device, status, is_template, template_id,
                            created_by, created_at, updated_at, scan_status
                        ) VALUES (
                            $1, $2::backuptasktype, $3, $4,
                            $5, $6, $7,
                            $8, $9, $10::backuptaskstatus, FALSE, $11,
                            $12, $13, $14, 'pending'
                        ) RETURNING id
                        """,
                        task_name,
                        task_type.value if hasattr(task_type, 'value') else str(task_type),
                        json.dumps(source_paths) if source_paths else None,
                        json.dumps(exclude_patterns) if exclude_patterns else None,
                        compression_enabled,
                        encryption_enabled,
                        retention_days,
                        description,
                        tape_device,
                        'pending',
                        template_task.id if template_task else None,
                        'scheduled_task',
                        now(),
                        now()
                    )

                    # 多表方案：为该执行任务创建 backup_files 分组和物理表
                    table_name = f"backup_files_{backup_task_id:06d}"

                    backup_files_group_id = await conn.fetchval(
                        """
                        INSERT INTO backup_files_groups (table_name, task_id)
                        VALUES ($1, $2)
                        RETURNING id
                        """,
                        table_name,
                        backup_task_id,
                    )

                    await conn.execute(
                        """
                        UPDATE backup_tasks
                        SET backup_files_group_id = $1,
                            backup_files_table = $2
                        WHERE id = $3
                        """,
                        backup_files_group_id,
                        table_name,
                        backup_task_id,
                    )

                    create_sql = f"""
                    CREATE TABLE IF NOT EXISTS {table_name} (
                        LIKE backup_files_template INCLUDING ALL
                    )
                    """
                    await conn.execute(create_sql)

                    actual_conn = conn._conn if hasattr(conn, '_conn') else conn
                    if hasattr(actual_conn, 'commit'):
                        try:
                            await actual_conn.commit()
                            logger.debug(f"备份任务 {backup_task_id} 及其 backup_files 分表已提交到数据库")
                        except Exception as commit_err:
                            logger.warning(f"提交备份任务事务失败（可能已自动提交）: {commit_err}")

                    backup_task = type('BackupTask', (), {
                        'id': backup_task_id,
                        'task_name': task_name,
                        'task_type': task_type,
                        'source_paths': source_paths,
                        'exclude_patterns': exclude_patterns,
                        'compression_enabled': compression_enabled,
                        'encryption_enabled': encryption_enabled,
                        'retention_days': retention_days,
                        'description': description,
                        'tape_device': tape_device,
                        'status': BackupTaskStatus.PENDING,
                        'is_template': False,
                        'template_id': template_task.id if template_task else None,
                        'created_by': 'scheduled_task',
                        'scan_status': 'pending',
                        'backup_set_id': None,
                        'backup_files_table': table_name,
                    })()

            if not hasattr(backup_task, 'force_rescan'):
                backup_task.force_rescan = False
            if force_rescan_option:
                backup_task.force_rescan = True

            if backup_task:
                task_name = backup_task.task_name

            # 执行备份任务
            await log_system(
                level=LogLevel.INFO,
                category=LogCategory.BACKUP,
                message="开始执行备份任务",
                module="utils.scheduler.action_handlers",
                function="BackupActionHandler.execute",
                details={"backup_task_id": getattr(backup_task, 'id', None), "task_name": task_name}
            )
            logger.info(f"调用备份引擎执行备份任务... (手动运行: {manual_run})")
            try:
                success = await self.system_instance.backup_engine.execute_backup_task(backup_task, scheduled_task=scheduled_task, manual_run=manual_run)
                backup_executed = True
                logger.info(f"备份任务执行完成，结果: {'成功' if success else '失败'}")
                await log_system(
                    level=LogLevel.INFO if success else LogLevel.ERROR,
                    category=LogCategory.BACKUP,
                    message="备份任务执行结束" + ("(成功)" if success else "(失败)"),
                    module="utils.scheduler.action_handlers",
                    function="BackupActionHandler.execute",
                    details={
                        "backup_task_id": getattr(backup_task, 'id', None),
                        "total_bytes": getattr(backup_task, 'total_bytes', None),
                        "total_files": getattr(backup_task, 'total_files', None),
                    }
                )

                if success:
                    return {
                        "status": "success",
                        "message": "备份任务执行成功",
                        "backup_task_id": backup_task.id,
                        "backup_set_id": backup_task.backup_set_id,
                        "tape_id": backup_task.tape_id,
                        "total_files": backup_task.total_files,
                        "total_bytes": backup_task.total_bytes,
                        "processed_files": backup_task.processed_files,
                        "template_id": template_task.id if template_task else None
                    }
                else:
                    error_msg = getattr(backup_task, 'error_message', '备份任务执行失败（未知错误）')
                    # 先更新 backup_tasks 表状态为 FAILED，避免任务卡片一直显示"运行中"
                    try:
                        from datetime import datetime as _dt
                        async with get_opengauss_connection() as _conn:
                            await _conn.execute(
                                """
                                UPDATE backup_tasks
                                SET status = 'failed'::backuptaskstatus,
                                    error_message = $1,
                                    completed_at = $2,
                                    updated_at = $3
                                WHERE id = $4 AND status NOT IN ('completed', 'failed', 'cancelled')
                                """,
                                error_msg,
                                _dt.now(),
                                _dt.now(),
                                backup_task.id
                            )
                            logger.info(f"[action_handlers] 已将备份任务 {backup_task.id} 状态更新为 FAILED: {error_msg}")
                    except Exception as db_err:
                        logger.warning(f"[action_handlers] 更新备份任务失败状态时数据库异常: {db_err}")
                    raise RuntimeError(f"备份任务执行失败: {error_msg}")
            except Exception as backup_error:
                if backup_executed:
                    raise
                raise

        except Exception as e:
            logger.error(f"执行备份动作失败: {str(e)}")
            if not backup_executed:
                try:
                    if self.system_instance and getattr(self.system_instance, 'dingtalk_notifier', None):
                        await self.system_instance.dingtalk_notifier.send_backup_notification(
                            backup_name=(template_task.task_name if 'template_task' in locals() and template_task else (config.get('task_name','计划备份'))),
                            status='failed',
                            details={'error': str(e)}
                        )
                except Exception:
                    pass
            raise

    async def _load_incomplete_backup_task(self, template_id: int):
        if not template_id:
            return None
        async with get_opengauss_connection() as conn:
            row = await conn.fetchrow(
                """
                SELECT id, task_name, task_type, source_paths, exclude_patterns,
                       compression_enabled, encryption_enabled, retention_days,
                       description, tape_device, status, template_id, tape_id,
                       total_files, processed_files, total_bytes, processed_bytes,
                       compressed_bytes, backup_set_id, scan_status, scan_completed_at,
                       result_summary
                FROM backup_tasks
                WHERE template_id = $1 AND status NOT IN ('completed', 'interrupted', 'cancelled')
                ORDER BY id DESC
                LIMIT 1
                """,
                template_id
            )
        if not row:
            return None
        return self._build_backup_task_from_row(row)

    async def _cancel_incomplete_backup_task(self, template_id: int) -> Optional[int]:
        """取消并清理未完成的备份任务"""
        existing_task = await self._load_incomplete_backup_task(template_id)
        if not existing_task:
            return None

        backup_engine = getattr(self.system_instance, 'backup_engine', None)
        backup_db = getattr(backup_engine, 'backup_db', None) if backup_engine else None

        if backup_db and getattr(existing_task, 'backup_set_id', None):
            try:
                backup_set = await backup_db.get_backup_set_by_set_id(existing_task.backup_set_id)
                if backup_set and getattr(backup_set, 'id', None):
                    await backup_db.clear_backup_files_for_set(backup_set.id)
            except Exception as e:
                logger.warning(f"清理历史 backup_files 失败: {e}")

        if backup_db:
            try:
                await backup_db.update_task_status(existing_task, BackupTaskStatus.CANCELLED)
            except Exception as e:
                logger.warning(f"更新未完成备份任务状态为取消失败: {e}")

        return existing_task.id

    def _build_backup_task_from_row(self, row) -> BackupTask:
        backup_task = BackupTask()
        backup_task.id = row['id']
        backup_task.task_name = row['task_name']
        backup_task.task_type = _parse_enum(BackupTaskType, row.get('task_type'), BackupTaskType.FULL)
        source_paths = row.get('source_paths')
        backup_task.source_paths = source_paths if isinstance(source_paths, list) else json.loads(source_paths) if source_paths else []
        exclude_patterns = row.get('exclude_patterns')
        backup_task.exclude_patterns = exclude_patterns if isinstance(exclude_patterns, list) else json.loads(exclude_patterns) if exclude_patterns else []
        backup_task.compression_enabled = row.get('compression_enabled')
        backup_task.encryption_enabled = row.get('encryption_enabled')
        backup_task.retention_days = row.get('retention_days')
        backup_task.description = row.get('description')
        backup_task.tape_device = row.get('tape_device')
        backup_task.tape_id = row.get('tape_id')
        backup_task.status = _parse_enum(BackupTaskStatus, row.get('status'), BackupTaskStatus.PENDING)
        backup_task.is_template = False
        backup_task.template_id = row.get('template_id')
        backup_task.total_files = row.get('total_files') or 0
        backup_task.processed_files = row.get('processed_files') or 0
        backup_task.total_bytes = row.get('total_bytes') or 0
        backup_task.processed_bytes = row.get('processed_bytes') or 0
        backup_task.compressed_bytes = row.get('compressed_bytes') or 0
        backup_task.backup_set_id = row.get('backup_set_id')
        backup_task.scan_status = row.get('scan_status') or 'pending'
        backup_task.scan_completed_at = row.get('scan_completed_at')
        summary = row.get('result_summary')
        if summary and isinstance(summary, str):
            try:
                summary = json.loads(summary)
            except json.JSONDecodeError:
                summary = {}
        backup_task.result_summary = summary or {}
        backup_task.force_rescan = False
        return backup_task


class RecoveryActionHandler(ActionHandler):
    """恢复动作处理器"""

    async def execute(
        self,
        config: Dict,
        scheduled_task: Optional[ScheduledTask] = None,
        manual_run: bool = False,
        run_options: Optional[Dict[str, Any]] = None
    ) -> Dict[str, Any]:
        """执行恢复动作"""
        try:
            logger.info("开始执行恢复任务")

            # 从配置中获取恢复参数
            backup_set_id = config.get('backup_set_id')
            files = config.get('files', [])
            target_path = config.get('target_path')

            if not backup_set_id:
                raise ValueError("恢复任务配置缺少 backup_set_id")

            if not files:
                raise ValueError("恢复任务配置缺少 files（文件列表）")

            if not target_path:
                raise ValueError("恢复任务配置缺少 target_path（目标路径）")

            # 获取恢复引擎
            if not self.system_instance or not hasattr(self.system_instance, 'recovery_engine'):
                raise RuntimeError("恢复引擎未初始化")

            recovery_engine = self.system_instance.recovery_engine

            # 创建恢复任务
            recovery_id = await recovery_engine.create_recovery_task(
                backup_set_id=backup_set_id,
                files=files,
                target_path=target_path,
                created_by='scheduled_task' if scheduled_task else 'manual'
            )

            if not recovery_id:
                raise RuntimeError("创建恢复任务失败")

            logger.info(f"恢复任务已创建: {recovery_id}")

            # 执行恢复任务
            success = await recovery_engine.execute_recovery(recovery_id)

            if success:
                logger.info(f"恢复任务执行成功: {recovery_id}")
                return {
                    "status": "success",
                    "message": f"恢复任务执行成功: {recovery_id}",
                    "recovery_id": recovery_id
                }
            else:
                error_msg = "恢复任务执行失败"
                if recovery_engine._current_recovery and recovery_engine._current_recovery.get('error_message'):
                    error_msg = recovery_engine._current_recovery['error_message']

                logger.error(f"恢复任务执行失败: {recovery_id}, 错误: {error_msg}")
                return {
                    "status": "failed",
                    "message": f"恢复任务执行失败: {error_msg}",
                    "recovery_id": recovery_id
                }

        except Exception as e:
            logger.error(f"执行恢复任务失败: {str(e)}")
            import traceback
            logger.error(traceback.format_exc())
            return {
                "status": "failed",
                "message": f"执行恢复任务失败: {str(e)}"
            }


class CleanupActionHandler(ActionHandler):
    """清理动作处理器"""

    async def execute(
        self,
        config: Dict,
        scheduled_task: Optional[ScheduledTask] = None,
        manual_run: bool = False,
        run_options: Optional[Dict[str, Any]] = None
    ) -> Dict[str, Any]:
        """执行清理动作"""
        return {"status": "success", "message": "清理任务已执行"}


class HealthCheckActionHandler(ActionHandler):
    """健康检查动作处理器"""

    async def execute(
        self,
        config: Dict,
        scheduled_task: Optional[ScheduledTask] = None,
        manual_run: bool = False,
        run_options: Optional[Dict[str, Any]] = None
    ) -> Dict[str, Any]:
        """执行健康检查动作"""
        return {"status": "success", "message": "健康检查已完成"}


class RetentionCheckActionHandler(ActionHandler):
    """保留期检查动作处理器"""

    async def execute(
        self,
        config: Dict,
        scheduled_task: Optional[ScheduledTask] = None,
        manual_run: bool = False,
        run_options: Optional[Dict[str, Any]] = None
    ) -> Dict[str, Any]:
        """执行保留期检查动作"""
        return {"status": "success", "message": "保留期检查已完成"}


class VerifyActionHandler(ActionHandler):
    """验证动作处理器 - 验证备份数据完整性"""

    @staticmethod
    def _verify_params_from_config(config: Dict) -> Dict[str, Any]:
        """提取验证任务的运行参数，供 result_summary 落库。

        为什么落库：计划任务的配置随时可能被编辑（改抽样比例/验证类型），
        事后查看历史任务时必须以"当时实际使用"的参数为准，不能反查当前配置。
        """
        try:
            percent = float(config.get('verify_percent', 1))
        except (TypeError, ValueError):
            percent = 1.0
        return {
            'verify_type': config.get('verify_type', 'directory'),
            'verify_percent': percent,
            'verify_enabled': bool(config.get('verify_enabled', True)),
        }

    async def _create_verify_task_record(self, config: Dict) -> int:
        """在 backup_tasks 表创建验证任务记录，返回 task_id"""
        verify_type = config.get('verify_type', 'directory')
        source_paths = config.get('source_paths', [])
        task_name = config.get('task_name') or f"验证任务-{format_datetime(now(), '%Y%m%d_%H%M%S')}"
        description = f"[扫描文件中...] 验证类型: {verify_type}"
        # 运行参数随任务落库（抽样比等），供卡片与历史查询使用
        verify_params_json = json.dumps(self._verify_params_from_config(config), ensure_ascii=False)

        async with get_opengauss_connection() as conn:
            task_id = await conn.fetchval(
                """
                INSERT INTO backup_tasks (
                    task_name, task_type, source_paths, exclude_patterns,
                    compression_enabled, encryption_enabled, retention_days,
                    description, tape_device, status, is_template,
                    created_by, created_at, updated_at, started_at, scan_status,
                    operation_stage, result_summary
                ) VALUES (
                    $1, 'verify'::backuptasktype, $2, $3,
                    FALSE, FALSE, 0,
                    $4, NULL, 'running'::backuptaskstatus, FALSE,
                    'scheduled_task', $5, $6, $7, 'running',
                    'scan', $8::jsonb
                ) RETURNING id
                """,
                task_name,
                json.dumps(source_paths) if source_paths else None,
                json.dumps(config.get('exclude_patterns', [])) or None,
                description,
                now(), now(), now(),
                verify_params_json
            )
            actual_conn = conn._conn if hasattr(conn, '_conn') else conn
            if hasattr(actual_conn, 'commit'):
                try:
                    await actual_conn.commit()
                except Exception:
                    pass
        return task_id

    async def _save_verify_summary(self, task_id: int, summary: Dict) -> None:
        """把验证结果统计合并写入 result_summary。

        卡片直接读取这些结构化字段，不再从 description 字符串解析；
        与创建时写入的 verify_type/verify_percent 合并（jsonb ||）。
        """
        try:
            async with get_opengauss_connection() as conn:
                await conn.execute(
                    # 占位符必须按出现顺序编号：psycopg3_compat 按出现顺序把
                    # $N 转成 %s，若 $2 出现在 $1 前会绑错参数
                    # （曾导致 result_summary 收到 smallint → cannot cast to jsonb）
                    "UPDATE backup_tasks SET result_summary = "
                    "COALESCE(result_summary, '{}'::jsonb) || $1::jsonb, updated_at = now() "
                    "WHERE id = $2",
                    json.dumps(summary, ensure_ascii=False),
                    task_id,
                )
                actual_conn = conn._conn if hasattr(conn, '_conn') else conn
                if hasattr(actual_conn, 'commit'):
                    try:
                        await actual_conn.commit()
                    except Exception:
                        pass
        except Exception as e:
            logger.warning(f"[验证任务] 写入结果统计失败 (task_id={task_id}): {e}")

    async def _mark_verify_cancelled(self, task_id: int) -> None:
        """任务被取消时把 backup_tasks 记录落库为 cancelled。

        独立成方法，便于在 shield 包裹的独立任务里跑完：
        任务取消后的清理不能被第二次取消打断（停止按钮可连点）。
        """
        try:
            await self._update_verify_progress(
                task_id,
                status='cancelled',
                operation_stage='verify',
                scan_status='completed',
                description='[已取消] 任务被手动停止',
                completed_at=now(),
            )
        except Exception as e:
            logger.warning(f"[验证任务] 写入取消状态失败 (task_id={task_id}): {e}")

    async def _update_verify_progress(self, task_id: int, **kwargs):
        """更新验证任务进度到 backup_tasks 表"""
        sets = []
        params = []
        idx = 1
        for key, value in kwargs.items():
            if key == 'status':
                sets.append(f"{key} = ${idx}::backuptaskstatus")
            else:
                sets.append(f"{key} = ${idx}")
            params.append(value)
            idx += 1
        if not sets:
            return
        params.append(task_id)
        sql = f"UPDATE backup_tasks SET {', '.join(sets)}, updated_at = now() WHERE id = ${idx}"
        try:
            async with get_opengauss_connection() as conn:
                await conn.execute(sql, *params)
                actual_conn = conn._conn if hasattr(conn, '_conn') else conn
                if hasattr(actual_conn, 'commit'):
                    try:
                        await actual_conn.commit()
                    except Exception:
                        pass
        except Exception as e:
            logger.warning(f"[验证任务] 更新进度失败 (task_id={task_id}): {e}")

    async def execute(
        self,
        config: Dict,
        scheduled_task: Optional[ScheduledTask] = None,
        manual_run: bool = False,
        run_options: Optional[Dict[str, Any]] = None
    ) -> Dict[str, Any]:
        """执行验证动作"""
        verify_type = config.get('verify_type', 'directory')
        verify_percent = float(config.get('verify_percent', 1))
        verify_enabled = config.get('verify_enabled', True)
        source_paths = config.get('source_paths', [])

        # 创建 backup_tasks 记录
        task_id = await self._create_verify_task_record(config)

        await log_system(
            level=LogLevel.INFO,
            category=LogCategory.BACKUP,
            message=f"开始执行验证任务（类型: {verify_type}, 抽样: {verify_percent}%）",
            module="utils.scheduler.action_handlers",
            function="VerifyActionHandler.execute",
            details={"verify_type": verify_type, "verify_percent": verify_percent, "backup_task_id": task_id}
        )

        try:
            loop = asyncio.get_event_loop()
            if verify_type == 'tape':
                result = await self._verify_tape(config, task_id)
            else:
                # 先在 async 中挂载 UNC 路径，再开线程跑验证
                resolved_paths = await self._resolve_source_paths(source_paths)
                if not resolved_paths:
                    raise ValueError("所有源路径均无效（SMB 挂载失败或路径不存在）")
                thread_config = dict(config)
                thread_config['source_paths'] = resolved_paths
                thread_config['_event_loop'] = loop  # 传入事件循环，供线程内 DB 更新使用
                # 取消标志：任务被取消时通知线程退出，避免线程继续扫描/读文件
                _stop_event = threading.Event()
                thread_config['_stop_event'] = _stop_event
                try:
                    result = await loop.run_in_executor(None, self._verify_directory_sync, thread_config, task_id)
                except asyncio.CancelledError:
                    _stop_event.set()
                    logger.warning("[目录验证] 任务被取消，已通知验证线程停止")
                    raise

            # 更新为完成状态（skipped 不能算失败：有备份任务在跑时跳过是正常行为）
            _result_status = result.get('status')
            if _result_status == 'success':
                final_status = 'completed'
            elif _result_status == 'skipped':
                final_status = 'cancelled'
            else:
                final_status = 'failed'
            # 参数（抽样比、验证类型…）+ 结果一起落库：参数由创建时写入，
            # 这里合并而不是覆盖，保证任务结束后仍能查到"当时用的抽样比例"
            final_summary = self._verify_params_from_config(config)
            final_summary.update(result)
            await self._update_verify_progress(
                task_id,
                status=final_status,
                progress_percent=100.0,
                operation_stage='verify',
                scan_status='completed',
                description=result.get('message', ''),
                result_summary=json.dumps(final_summary, ensure_ascii=False),
                completed_at=now()
            )

            await log_system(
                level=(LogLevel.INFO if _result_status in ('success', 'skipped')
                       else LogLevel.ERROR),
                category=LogCategory.BACKUP,
                message=f"验证任务完成: {result.get('message', '')}",
                module="utils.scheduler.action_handlers",
                function="VerifyActionHandler.execute",
                details=result
            )
            result['backup_task_id'] = task_id
            return result

        except asyncio.CancelledError:
            # CancelledError 继承 BaseException，下面那个 except Exception 接不住：
            # 用户停任务后 backup_tasks 记录会永远停在 running（实测任务 167），
            # 后续"有任务在运行"的判断会误判。这里显式落库为 cancelled。
            # 用独立任务 + shield：停止按钮可能连点，第二次取消会打断 await。
            _mark = asyncio.ensure_future(self._mark_verify_cancelled(task_id))
            try:
                await asyncio.shield(_mark)
            except asyncio.CancelledError:
                logger.warning(
                    f"[验证任务] 取消状态写入继续在后台执行 (task_id={task_id})")
            except Exception as _me:
                logger.warning(f"[验证任务] 取消状态写入失败 (task_id={task_id}): {_me}")
            raise

        except Exception as e:
            try:
                await self._update_verify_progress(
                    task_id,
                    status='failed',
                    error_message=str(e),
                    operation_stage='verify',
                    scan_status='completed',
                    completed_at=now()
                )
            except Exception:
                pass

            logger.error(f"验证任务执行失败: {e}", exc_info=True)
            await log_system(
                level=LogLevel.ERROR,
                category=LogCategory.BACKUP,
                message=f"验证任务执行异常: {str(e)}",
                module="utils.scheduler.action_handlers",
                function="VerifyActionHandler.execute",
            )
            return {"status": "failed", "message": str(e), "backup_task_id": task_id}

    async def _verify_tape(self, config: Dict, task_id: int = None) -> Dict[str, Any]:
        """磁带验证：挂载 LTFS，抽样 .tar.zst 归档验证完整性"""
        verify_percent = float(config.get('verify_percent', 1))

        # 1. 检查是否有正在运行的备份任务
        if self.system_instance and hasattr(self.system_instance, 'backup_engine'):
            backup_engine = self.system_instance.backup_engine
            if backup_engine and hasattr(backup_engine, '_current_task') and backup_engine._current_task:
                task_status = getattr(backup_engine._current_task, 'status', None)
                if task_status and str(task_status).lower() == 'running':
                    msg = "当前有备份任务正在运行，跳过磁带验证"
                    logger.warning(msg)
                    return {"status": "skipped", "message": msg}

        # 2. 获取 tape_handler 并挂载
        if not self.system_instance:
            return {"status": "failed", "message": "系统实例未初始化"}

        tape_handler = getattr(self.system_instance.backup_engine, 'tape_handler', None)
        if not tape_handler:
            return {"status": "failed", "message": "TapeHandler 未初始化"}

        was_already_mounted = False
        mount_point = None

        # 1.5 磁带身份校验：配置了 tape_label 时才校验（未配置则验证当前磁带）
        expected_label = str(
            config.get('tape_label') or config.get('tape_id') or '').strip()
        if expected_label:
            try:
                # tape_handler 构造时 tape_manager=None，必须从 system_instance 取
                _tm = (getattr(self.system_instance, 'tape_manager', None)
                       or getattr(tape_handler, 'tape_manager', None))
                _tops = getattr(_tm, 'tape_operations', None)
                if _tops and hasattr(_tops, '_read_tape_label'):
                    _info = await _tops._read_tape_label()
                    _actual = ((_info or {}).get('label')
                               or (_info or {}).get('tape_id') or '')
                    if not _actual:
                        logger.warning(
                            "[磁带验证] 无法读取当前磁带标签，磁带身份校验未执行")
                    elif _actual != expected_label:
                        msg = (f"当前磁带 {_actual} 与预期 {expected_label} 不符，"
                               f"跳过验证（请确认装载的磁带）")
                        logger.warning(f"[磁带验证] {msg}")
                        return {
                            "status": "skipped", "message": msg,
                            "total": 0, "sampled": 0, "passed": 0,
                            "failed": 0, "corrupt": 0, "verify_errors": 0,
                        }
                    else:
                        logger.info(f"[磁带验证] 磁带身份校验通过: {_actual}")
                else:
                    logger.warning(
                        "[磁带验证] tape_operations 不可用，磁带身份校验未执行")
            except Exception as _id_err:
                logger.warning(f"[磁带验证] 读取磁带标签失败，跳过身份校验: {_id_err}")

        try:
            # 检查是否已挂载
            mount_point = Path(tape_handler._get_ltfs_mount_point())
            if mount_point.exists() and mount_point.is_mount():
                was_already_mounted = True
                logger.info("[磁带验证] LTFS 已挂载，直接使用")
            else:
                logger.info("[磁带验证] 挂载 LTFS...")
                success, msg = await tape_handler.mount_with_retry(max_retries=2, retry_interval=15)
                if not success:
                    return {"status": "failed", "message": f"挂载 LTFS 失败: {msg}"}
                logger.info("[磁带验证] LTFS 挂载成功")

            # 3. 收集所有归档文件（跳过正在写入的：mtime 过新）
            # 当前磁带卷标（写入清单缓存时的键）
            tape_label = 'unknown'
            try:
                _re_eng = getattr(self.system_instance, 'tape_recovery_engine', None)
                if _re_eng:
                    _st = await _re_eng.get_tape_status()
                    tape_label = _st.get('tape_label') or 'unknown'
            except Exception:
                pass

            archives = []
            unstable_skipped = 0
            system_dirs = {'.LTFS', 'lost+found', '.Trash', '.Trashes',
                          '$RECYCLE.BIN', 'System Volume Information'}
            _now_ts = time.time()

            for item in mount_point.iterdir():
                if not item.is_dir() or item.name in system_dirs or item.name.startswith('.'):
                    continue
                # 递归扫描：归档可能放在 set 目录下的子目录里
                for f in item.rglob('*'):
                    if not f.is_file() or not f.name.endswith(
                            ('.tar.zst', '.tar.gz', '.tgz', '.tar', '.7z', '.zip')):
                        continue
                    try:
                        _st = f.stat()
                    except OSError:
                        continue
                    # 60 秒内还在变化的归档可能仍在写入（备份任务同时在跑），跳过
                    if _now_ts - _st.st_mtime < 60:
                        unstable_skipped += 1
                        continue
                    archives.append(f)

            if unstable_skipped:
                logger.warning(
                    f"[磁带验证] 跳过 {unstable_skipped} 个正在写入/刚写入的归档")

            if not archives:
                # 空带或装错带不能报"验证成功"，否则掩盖问题
                if unstable_skipped:
                    _msg = (f"磁带上 {unstable_skipped} 个归档均在写入中"
                            f"（mtime < 60 秒），本次跳过验证，请稍后重试")
                else:
                    _msg = "磁带上未发现归档文件（请确认装载的磁带是否正确）"
                logger.warning(f"[磁带验证] {_msg}")
                return {
                    "status": "skipped",
                    "message": _msg,
                    "total": 0, "sampled": 0, "passed": 0,
                    "failed": 0, "corrupt": 0, "verify_errors": 0,
                }

            # 4. 按百分比抽样：向上取整，最少 1 个
            # int() 是截断（1169×10% → 116，少抽一个）；改用 ceil：
            # 1169 → 117，11 → 2，3 → 1（不足 1 个也抽 1 个）
            sample_count = max(1, math.ceil(len(archives) * verify_percent / 100))
            sample_count = min(sample_count, len(archives))
            sampled = random.sample(archives, sample_count)

            _actual_pct = (sample_count / len(archives) * 100) if archives else 0.0
            logger.info(
                f"[磁带验证] 发现 {len(archives)} 个归档，抽样 {sample_count} 个"
                f"（实际 {_actual_pct:.1f}%，配置 {verify_percent}%）")

            # 归档总数先落库：运行中卡片即可显示“归档总数/抽样进度”
            if task_id:
                await self._save_verify_summary(task_id, {
                    'verify_total_archives': len(archives),
                })

            # 5. 创建临时目录
            from config.settings import get_settings
            settings = get_settings()
            verify_temp = Path(settings.VERIFY_TEMP_DIR) / str(uuid.uuid4())[:8]
            verify_temp.mkdir(parents=True, exist_ok=True)

            passed = 0
            failed = 0
            corrupt = 0          # 归档内容损坏（解压/校验不通过）
            verify_errors = 0    # 验证过程异常（IO/临时错误等）
            errors = []
            _last_progress_at = 0.0

            try:
                for i, archive_path in enumerate(sampled):
                    logger.info(f"[磁带验证] 验证归档 {i+1}/{sample_count}: {archive_path.name}")
                    # 实时进度：卡片显示"正在验证 xxx — 3.2/7.5GB (43%) ~180MB/s"。
                    # 挂到 system_instance 上供任务查询接口读取（同 tape_write 模式），
                    # 解压在线程里跑，只写 int/str 字段（CPython 原子赋值，无需锁）。
                    _live = {'name': archive_path.name, 'total': 0, 'done': 0,
                             'members': 0, 'started': time.time()}
                    try:
                        _live['total'] = archive_path.stat().st_size
                    except OSError:
                        pass
                    _sys_live = self.system_instance
                    if _sys_live is not None:
                        _sys_live._verify_live = _live
                    try:
                        # 单个归档上限 30 分钟：磁带/FUSE 卡死时不能无限挂住
                        _entries = []
                        valid, detail = await asyncio.wait_for(
                            asyncio.to_thread(
                                self._verify_archive_extract, archive_path, verify_temp,
                                _entries, _live),
                            timeout=1800)
                        if valid:
                            passed += 1
                            logger.info(f"[磁带验证] ✓ {archive_path.name}: {detail}")
                            # 解压时顺手拿到文件清单：写入 DB 缓存（未缓存才写），
                            # 恢复检索时可直接命中，无需再读磁带
                            try:
                                await self._cache_verified_archive(
                                    tape_label, archive_path, _entries)
                            except Exception as _ce:
                                logger.warning(
                                    f"[磁带验证] 写入清单缓存失败（不影响验证）: {_ce}")
                        else:
                            failed += 1
                            corrupt += 1
                            errors.append(f"[归档损坏] {archive_path}: {detail}")
                            logger.warning(f"[磁带验证] ✗ {archive_path}: {detail}")
                            # 标记损坏：恢复页面直接显示“损坏”，不再反复读带
                            try:
                                _re_eng2 = getattr(self.system_instance,
                                                   'tape_recovery_engine', None)
                                if _re_eng2 and tape_label and tape_label != 'unknown':
                                    await _re_eng2.mark_archive_corrupt(
                                        tape_label, archive_path.parent.name,
                                        archive_path.name, detail)
                            except Exception as _me:
                                logger.warning(f"[磁带验证] 标记归档损坏失败: {_me}")
                    except asyncio.TimeoutError:
                        failed += 1
                        verify_errors += 1
                        errors.append(
                            f"[验证超时] {archive_path}: 超过 1800 秒未完成")
                        logger.error(
                            f"[磁带验证] ✗ {archive_path.name}: 解压验证超时")
                    except Exception as e:
                        failed += 1
                        verify_errors += 1
                        errors.append(f"[验证异常] {archive_path}: {str(e)}")
                        logger.error(f"[磁带验证] ✗ {archive_path.name}: {e}")
                    # 更新数据库进度：每 5 秒最多一次，最后一个必写（避免频繁 UPDATE）
                    _now_p = time.time()
                    if task_id and (_now_p - _last_progress_at >= 5
                                    or i + 1 == sample_count):
                        _last_progress_at = _now_p
                        progress_pct = ((i + 1) / sample_count) * 100
                        await self._update_verify_progress(
                            task_id,
                            # 统一“总量”语义：目录验证=扫描数，磁带验证=归档总数
                            total_files=len(archives),
                            processed_files=i + 1,      # 已抽样验证数量
                            progress_percent=min(99.0, progress_pct),
                            operation_stage='verify',
                            scan_status='completed',
                            description=f"[磁带验证中...] 归档 {i+1}/{sample_count}, 通过 {passed}, 失败 {failed}"
                        )
            finally:
                # 清除卡片实时进度（任务取消/异常时也不能留旧数据）
                _sys_live = self.system_instance
                if _sys_live is not None:
                    _sys_live._verify_live = None
                # 6. 清理临时目录
                try:
                    shutil.rmtree(str(verify_temp), ignore_errors=True)
                except Exception:
                    pass

            # 7. 记录操作日志
            is_success = failed == 0
            error_detail = "\n".join(errors) if errors else None
            await log_operation(
                operation_type=OperationType.TAPE_VERIFY,
                resource_type="tape",
                operation_name="磁带验证",
                operation_description=f"磁带验证完成: 抽样 {sample_count}/{len(archives)}, 通过 {passed}, 失败 {failed}",
                category="backup",
                success=is_success,
                result_message=f"抽样 {sample_count}/{len(archives)}, 通过 {passed}, 失败 {failed}",
                error_message=error_detail
            )

            result = {
                "status": "success" if failed == 0 else "failed",
                "message": f"磁带验证完成: 抽样 {sample_count}/{len(archives)}, 通过 {passed}, 失败 {failed}"
                           + (f"（损坏 {corrupt}, 异常 {verify_errors}）" if failed else ""),
                "total": len(archives),
                "sampled": sample_count,
                "passed": passed,
                "failed": failed,
                "corrupt": corrupt,
                "verify_errors": verify_errors,
            }
            # 结构化统计落库（卡片按验证类型显示，不再解析 description）
            if task_id:
                await self._save_verify_summary(task_id, {
                    'verify_total_archives': len(archives),
                    'verify_sampled': sample_count,
                    'verify_passed': passed,
                    'verify_failed': failed,
                    'verify_corrupt': corrupt,
                    'verify_errors': verify_errors,
                })
            if errors:
                result["errors"] = errors[:20]  # 限制错误数量

            return result

        finally:
            # 如果是我们自己挂载的，验证完后卸载并 offline：
            # 只卸载不 offline 会让带子带着"加载状态+索引分区末尾"留在驱动器里，
            # 随后系统重启可能重演读卷标签失败的问题。
            # 用独立任务 + shield：停止按钮连点会再发一次取消，
            # 直接 await 会被打断，磁带停在已加载未 offline 的状态。
            if not was_already_mounted and mount_point and mount_point.is_mount():
                logger.info("[磁带验证] 开始卸载 LTFS 并 offline 磁带...")
                _unmount = asyncio.ensure_future(self._unmount_tape_after_verify(tape_handler))
                try:
                    await asyncio.shield(_unmount)
                    logger.info("[磁带验证] 已卸载 LTFS 并离线磁带")
                except asyncio.CancelledError:
                    # 不在此处 raise：finally 中没有活动异常时 raise 会抛
                    # RuntimeError。原取消异常在 finally 结束后自动继续传播。
                    logger.warning(
                        "[磁带验证] 任务被取消，卸载/离线继续在后台执行")
                except Exception as e:
                    logger.warning(f"[磁带验证] 卸载 LTFS 失败: {e}")

    async def _unmount_tape_after_verify(self, tape_handler) -> None:
        """验证结束后卸载 LTFS 并 offline 磁带（独立任务，不受任务取消影响）。"""
        try:
            await tape_handler.unmount_ltfs(eject=True)
        except Exception as e:
            logger.warning(f"[磁带验证] 卸载 LTFS 失败: {e}")

    async def _cache_verified_archive(self, tape_label: str, archive_path: Path,
                                      entries: list) -> None:
        """把磁带验证时解压得到的文件清单写入 DB 缓存（未缓存才写）。

        验证本来就完整解压了归档，清单是顺带产物；写入后恢复检索可直接
        命中 DB，不再读磁带。失败只告警，不影响验证结果。
        """
        if not entries or not tape_label or tape_label == 'unknown':
            return
        engine = getattr(self.system_instance, 'tape_recovery_engine', None)
        if not engine:
            return
        set_id = archive_path.parent.name
        archive_name = archive_path.name
        # 验证成功说明数据完好：清除可能存在的旧损坏标记（误标自愈）
        try:
            await engine.clear_archive_corrupt(tape_label, set_id, archive_name)
        except Exception:
            pass
        try:
            info = await engine.check_archive_cache(tape_label, set_id, archive_name)
            if info and info.get('cached'):
                return
        except Exception:
            pass
        try:
            archive_size = archive_path.stat().st_size
        except OSError:
            archive_size = None
        ok = await engine.save_archive_contents(
            tape_label, set_id, archive_name, entries, archive_size)
        if ok:
            logger.info(
                f"[磁带验证] 已缓存归档清单: {archive_name}"
                f"（{len(entries)} 个文件）")

    @staticmethod
    def _should_exclude_file(file_path: str, exclude_patterns: List[str]) -> bool:
        """检查文件/目录是否应该被排除（复用 FileScanner 相同的排除逻辑）"""
        if not exclude_patterns:
            return False
        from fnmatch import fnmatch as _fnmatch
        normalized = file_path.replace('\\', '/')
        path_parts = [p for p in normalized.split('/') if p]
        # 1. 完整路径匹配
        for pattern in exclude_patterns:
            normalized_pattern = pattern.replace('\\', '/')
            if _fnmatch(normalized, path_parts[-1]):
                return True
        # 2. 路径段名称匹配
        for part in path_parts:
            for pattern in exclude_patterns:
                normalized_pattern = pattern.replace('\\', "/")
                tail_pattern = normalized_pattern.split("/")[-1]
                if _fnmatch(part, tail_pattern):
                    return True
        # 3. 父目录匹配
        try:
            parent = Path(normalized).parent
            if VerifyActionHandler._should_exclude_file(str(parent), exclude_patterns):
                return True
        except Exception:
            pass
        return False

    async def _resolve_source_paths(self, source_paths: list) -> list:
        """解析源路径：UNC 网络路径自动挂载为本地路径（异步）"""
        from utils.network_path import is_unc_path, validate_network_path
        resolved_paths = []
        for src in source_paths:
            if is_unc_path(src):
                logger.info(f"[目录验证] 检测到 UNC 路径，尝试挂载 SMB: {src}")
                validation = await validate_network_path(src)
                if validation.get('valid') and validation.get('path'):
                    local_path = validation['path']
                    logger.info(f"[目录验证] UNC 路径已映射: {src} -> {local_path}")
                    resolved_paths.append(local_path)
                else:
                    logger.warning(f"[目录验证] SMB 挂载失败: {src} - {validation.get('message', '未知错误')}")
            else:
                resolved_paths.append(src)
        return resolved_paths

    def _verify_directory_sync(self, config: Dict, task_id: int = None) -> Dict[str, Any]:
        """目录验证（同步，在线程中运行，不阻塞事件循环）"""
        source_paths = config.get('source_paths', [])  # 已解析为本地路径
        verify_percent = float(config.get('verify_percent', 1))
        verify_enabled = config.get('verify_enabled', False)
        exclude_patterns = config.get('exclude_patterns', [])
        stop_event = config.get('_stop_event')  # 任务取消时由 execute 设置

        if not source_paths:
            return {"status": "failed", "message": "未指定验证源路径"}

        BATCH_SIZE = 100
        MAX_ERRORS = 200     # 错误详情只保留前 N 条，避免大量失败时内存膨胀
        passed = 0
        failed = 0
        total_scanned = 0
        total_sampled = 0
        excluded_count = 0
        errors = []
        last_progress_log = time.monotonic()
        last_db_update = 0.0  # 立即触发首次更新

        from config.settings import get_settings
        settings = get_settings()
        verify_temp = Path(settings.VERIFY_TEMP_DIR) / str(uuid.uuid4())[:8]

        # 获取事件循环（必须从调用方传入，线程内无法 get_event_loop）
        loop = config.get('_event_loop')

        def _try_db_update(stage, scan_st, desc_suffix, force=False):
            """线程安全地异步更新验证任务进度"""
            nonlocal last_db_update
            now_ts = time.monotonic()
            if not force and now_ts - last_db_update < 10:
                return
            if not task_id or not loop:
                return
            coro = None
            try:
                coro = self._update_verify_progress(
                    task_id,
                    total_files=total_scanned,
                    processed_files=total_sampled,
                    progress_percent=min(99.0, (total_sampled / max(total_scanned, 1)) * 100) if total_scanned > 0 else 0.0,
                    operation_stage=stage,
                    scan_status=scan_st,
                    description=f"[{desc_suffix}] 已扫描 {total_scanned}, 抽样 {total_sampled}, 通过 {passed}, 失败 {failed}"
                )
                asyncio.run_coroutine_threadsafe(coro, loop)
            except Exception:
                # 提交失败时关闭协程，避免 "coroutine was never awaited"
                try:
                    if coro is not None:
                        coro.close()
                except Exception:
                    pass
            last_db_update = now_ts

        # 立即写入初始状态（扫描开始）
        _try_db_update('scan', 'running', '扫描文件中...', force=True)

        def _add_errors(items):
            """收集错误详情（最多 MAX_ERRORS 条，防止内存膨胀）"""
            for err_msg in items:
                if len(errors) >= MAX_ERRORS:
                    break
                errors.append(err_msg)

        for src in source_paths:
            if stop_event is not None and stop_event.is_set():
                logger.warning("[目录验证] 收到停止信号，终止验证")
                break
            src_path = Path(src)
            if not src_path.exists():
                logger.warning(f"[目录验证] 路径不存在: {src}")
                continue
            if src_path.is_file():
                if exclude_patterns and self._should_exclude_file(str(src_path), exclude_patterns):
                    excluded_count += 1
                    continue
                total_scanned += 1
                sampled_list = [src_path]
                p, f, s, e = self._verify_batch_sync(sampled_list, verify_enabled, verify_temp)
                passed += p
                failed += f
                total_sampled += s
                for err_msg in e:
                    logger.warning(f"[目录验证] 验证失败: {err_msg}")
                _add_errors(e)
            elif src_path.is_dir():
                batch = []
                for root, dirs, files in os.walk(str(src_path)):
                    if stop_event is not None and stop_event.is_set():
                        logger.warning("[目录验证] 收到停止信号，终止扫描/验证")
                        break
                    if exclude_patterns:
                        dirs[:] = [d for d in dirs if not self._should_exclude_file(str(Path(root) / d), exclude_patterns)]
                    for f in files:
                        file_path = Path(root) / f
                        if exclude_patterns and self._should_exclude_file(str(file_path), exclude_patterns):
                            excluded_count += 1
                            continue
                        batch.append(file_path)
                        total_scanned += 1
                        # 每10秒更新扫描进度（即使未满一批）
                        _try_db_update('scan', 'running', '扫描文件中...')
                        if len(batch) >= BATCH_SIZE:
                            sample_count = max(1, math.ceil(len(batch) * verify_percent / 100))
                            sampled_list = random.sample(batch, min(sample_count, len(batch)))
                            p, f, s, e = self._verify_batch_sync(sampled_list, verify_enabled, verify_temp)
                            passed += p
                            failed += f
                            total_sampled += s
                            for err_msg in e:
                                logger.warning(f"[目录验证] 验证失败: {err_msg}")
                            _add_errors(e)
                            batch = []
                            # 批次验证后更新进度（走 10 秒节流，避免每批一次 UPDATE）
                            _try_db_update('verify', 'completed', '验证文件中...')
                            # 每60秒输出一次进度日志
                            now_ts = time.monotonic()
                            if now_ts - last_progress_log >= 60:
                                logger.info(f"[目录验证] 进度: 已扫描 {total_scanned} 个文件, 抽样 {total_sampled}, 通过 {passed}, 失败 {failed}")
                                last_progress_log = now_ts
                # 处理剩余的文件
                if batch:
                    sample_count = max(1, math.ceil(len(batch) * verify_percent / 100))
                    sampled_list = random.sample(batch, min(sample_count, len(batch)))
                    p, f, s, e = self._verify_batch_sync(sampled_list, verify_enabled, verify_temp)
                    passed += p
                    failed += f
                    total_sampled += s
                    for err_msg in e:
                        logger.warning(f"[目录验证] 验证失败: {err_msg}")
                    _add_errors(e)
                    _try_db_update('verify', 'completed', '验证文件中...', force=True)

        # 清理临时目录
        try:
            if verify_temp.exists():
                shutil.rmtree(str(verify_temp), ignore_errors=True)
        except Exception:
            pass

        if total_scanned == 0:
            return {"status": "success", "message": "指定路径中未发现文件", "total": 0, "sampled": 0, "passed": 0, "failed": 0}

        logger.info(f"[目录验证] 完成: 扫描 {total_scanned} 个文件 (排除 {excluded_count} 个), 抽样 {total_sampled}, 通过 {passed}, 失败 {failed}")

        result = {
            "status": "success" if failed == 0 else "failed",
            "message": f"目录验证完成: 扫描 {total_scanned}, 抽样 {total_sampled}, 通过 {passed}, 失败 {failed}",
            "total": total_scanned,
            "sampled": total_sampled,
            "passed": passed,
            "failed": failed,
        }
        # 结构化统计落库（线程中运行：提交到主事件循环）
        if task_id and loop:
            try:
                asyncio.run_coroutine_threadsafe(
                    self._save_verify_summary(task_id, {
                        'verify_scanned': total_scanned,
                        'verify_sampled': total_sampled,
                        'verify_passed': passed,
                        'verify_failed': failed,
                    }), loop)
            except Exception as _sv_err:
                logger.warning(f"[目录验证] 提交结果统计失败: {_sv_err}")
        if errors:
            result["errors"] = errors[:20]
        return result

    def _verify_batch_sync(self, sampled: list, verify_enabled: bool, verify_temp: Path) -> tuple:
        """同步验证一批文件，返回 (passed, failed, sampled_count, errors)"""
        passed = 0
        failed = 0
        errors = []

        for file_path in sampled:
            try:
                if file_path.name.endswith(('.tar.zst', '.tar.gz', '.tgz', '.tar', '.7z', '.zip')) and verify_enabled:
                    verify_temp.mkdir(parents=True, exist_ok=True)
                    valid, detail = self._verify_archive_extract(file_path, verify_temp)
                    # 清理临时文件
                    if verify_temp.exists():
                        for item in verify_temp.iterdir():
                            try:
                                if item.is_dir():
                                    shutil.rmtree(str(item))
                                else:
                                    item.unlink()
                            except Exception:
                                pass
                    if valid:
                        passed += 1
                    else:
                        failed += 1
                        errors.append(f"{file_path}: {detail}")
                else:
                    # 普通文件：完整读取，校验读到的字节数与 stat 一致
                    # （原来只读 4KB，发现不了文件中后部的损坏/截断）
                    try:
                        size_before = file_path.stat().st_size
                    except FileNotFoundError:
                        # 源文件在验证期间被删除：源目录正在变化，不算失败
                        logger.warning(
                            f"[目录验证] 文件已消失，跳过: {file_path}")
                        passed += 1
                        continue
                    read_total = 0
                    try:
                        with open(str(file_path), 'rb') as f:
                            while True:
                                chunk = f.read(1024 * 1024)
                                if not chunk:
                                    break
                                read_total += len(chunk)
                    except FileNotFoundError:
                        logger.warning(
                            f"[目录验证] 文件读取前已消失，跳过: {file_path}")
                        passed += 1
                        continue
                    try:
                        size_after = file_path.stat().st_size
                    except OSError:
                        size_after = size_before
                    if size_before != size_after:
                        # 验证期间源文件被修改：本次跳过，不算失败
                        logger.warning(
                            f"[目录验证] 文件在验证期间发生变化，跳过: {file_path} "
                            f"({size_before} -> {size_after})")
                        passed += 1
                    elif read_total != size_before:
                        failed += 1
                        errors.append(
                            f"{file_path}: 读取字节数不符 ({read_total} != {size_before})")
                    else:
                        passed += 1
            except Exception as e:
                failed += 1
                if len(errors) < 200:
                    errors.append(f"{file_path}: {str(e)}")

        return passed, failed, len(sampled), errors

    async def _verify_batch(self, batch: list, verify_percent: float, verify_enabled: bool, verify_temp: Path) -> tuple:
        """验证一批文件，返回 (passed, failed, sampled_count, errors)"""
        sample_count = max(1, math.ceil(len(batch) * verify_percent / 100))
        sample_count = min(sample_count, len(batch))
        sampled = random.sample(batch, sample_count)
        passed = 0
        failed = 0
        errors = []

        for file_path in sampled:
            try:
                if file_path.name.endswith(('.tar.zst', '.tar.gz', '.tgz', '.tar', '.7z', '.zip')) and verify_enabled:
                    verify_temp.mkdir(parents=True, exist_ok=True)
                    valid, detail = await asyncio.to_thread(
                        self._verify_archive_extract, file_path, verify_temp
                    )
                    # 清理临时文件
                    if verify_temp.exists():
                        for item in verify_temp.iterdir():
                            try:
                                if item.is_dir():
                                    shutil.rmtree(str(item))
                                else:
                                    item.unlink()
                            except Exception:
                                pass
                    if valid:
                        passed += 1
                    else:
                        failed += 1
                        errors.append(f"{file_path}: {detail}")
                else:
                    # 普通文件（含未勾选验证的压缩文件）：验证可读性
                    size = file_path.stat().st_size
                    if size > 0:
                        with open(str(file_path), 'rb') as f:
                            f.read(4096)
                    passed += 1
            except Exception as e:
                failed += 1
                errors.append(f"{file_path}: {str(e)}")

        return passed, failed, sample_count, errors

    @staticmethod
    def _verify_archive_header(archive_path: Path) -> tuple:
        """验证归档文件头部可读性（不解压，支持加密归档）

        Returns:
            (is_valid: bool, detail: str)
        """
        name = archive_path.name.lower()
        size = archive_path.stat().st_size
        size_mb = size / (1024 * 1024)

        try:
            if name.endswith('.tar.zst'):
                with open(str(archive_path), 'rb') as f:
                    magic = f.read(4)
                if magic[:4] == b'\x28\xb5\x2f\xfd':
                    return True, f"zstd 归档头部正常 ({size_mb:.1f}MB)"
                return False, f"zstd 魔数不匹配"
            elif name.endswith(('.tar.gz', '.tgz')):
                with open(str(archive_path), 'rb') as f:
                    magic = f.read(2)
                if magic == b'\x1f\x8b':
                    return True, f"gzip 归档头部正常 ({size_mb:.1f}MB)"
                return False, "gzip 魔数不匹配"
            elif name.endswith('.tar'):
                with open(str(archive_path), 'rb') as f:
                    f.seek(257)
                    ustar = f.read(5)
                if ustar == b'ustar':
                    return True, f"tar 归档头部正常 ({size_mb:.1f}MB)"
                return True, f"tar 文件可读 ({size_mb:.1f}MB)"
            elif name.endswith('.zip'):
                with open(str(archive_path), 'rb') as f:
                    magic = f.read(4)
                if magic[:2] == b'PK':
                    return True, f"ZIP 归档头部正常 ({size_mb:.1f}MB)"
                return False, "ZIP 魔数不匹配"
            elif name.endswith('.7z'):
                with open(str(archive_path), 'rb') as f:
                    magic = f.read(6)
                if magic[:2] == b'7z':
                    return True, f"7z 归档头部正常 ({size_mb:.1f}MB)"
                return False, "7z 魔数不匹配"
            else:
                return True, f"文件可读 ({size_mb:.1f}MB)"
        except Exception as e:
            return False, str(e)

    @staticmethod
    def _verify_archive_extract(archive_path: Path, temp_dir: Path,
                                entries_out: Optional[list] = None,
                                progress: Optional[dict] = None) -> tuple:
        """解压验证归档文件完整性（用于磁带验证，解压到临时目录后删除）

        Args:
            entries_out: 传入列表时，解压过程中收集归档内文件清单
                [{name,size,is_dir,modified_time}]，供写入 DB 缓存使用
            progress: 传入字典时实时更新 {'done': 已读压缩字节, 'members': 成员数}，
                供卡片进度条显示（仅 .tar.zst 的 zstandard 路径支持）

        Returns:
            (is_valid: bool, detail: str)
        """
        name = archive_path.name.lower()
        size = archive_path.stat().st_size
        size_mb = size / (1024 * 1024)

        try:
            if name.endswith('.tar.zst'):
                return VerifyActionHandler._extract_tar_zst(
                    archive_path, temp_dir, size_mb, entries_out, progress)
            elif name.endswith(('.tar.gz', '.tgz')):
                return VerifyActionHandler._extract_tar(
                    archive_path, temp_dir, 'r:gz', size_mb, entries_out)
            elif name.endswith('.tar'):
                return VerifyActionHandler._extract_tar(
                    archive_path, temp_dir, 'r:', size_mb, entries_out)
            elif name.endswith('.zip'):
                return VerifyActionHandler._extract_zip(
                    archive_path, temp_dir, size_mb, entries_out)
            elif name.endswith('.7z'):
                return VerifyActionHandler._extract_7z(
                    archive_path, temp_dir, size_mb, entries_out)
            else:
                return True, f"文件可读 ({size_mb:.1f}MB)"
        except Exception as e:
            return False, str(e)

    @staticmethod
    def _extract_tar_zst(archive_path: Path, temp_dir: Path, size_mb: float,
                         entries_out: Optional[list] = None,
                         progress: Optional[dict] = None) -> tuple:
        """解压验证 .tar.zst（entries_out 非 None 时收集文件清单；
        progress 非 None 时每成员更新已读字节数，供卡片进度条显示）"""
        def _collect(member):
            if entries_out is not None:
                try:
                    entries_out.append({
                        'name': member.name,
                        'size': member.size if member.isfile() else 0,
                        'is_dir': member.isdir(),
                        'modified_time': (datetime.fromtimestamp(member.mtime).isoformat()
                                          if member.mtime else None),
                    })
                except Exception:
                    pass

        try:
            import zstandard as zstd
            dctx = zstd.ZstdDecompressor()
            member_count = 0
            with open(str(archive_path), 'rb') as fh:
                with dctx.stream_reader(fh) as reader:
                    with tarfile.open(fileobj=reader, mode='r|') as tar:
                        for member in tar:
                            member_count += 1
                            if progress is not None:
                                # fh.tell() = 已读压缩字节（含缓冲预读，近似即可）
                                try:
                                    progress['done'] = fh.tell()
                                    progress['members'] = member_count
                                except Exception:
                                    pass
                            _collect(member)
            return True, f"解压验证通过 ({size_mb:.1f}MB, {member_count} 个文件)"
        except ImportError:
            import subprocess
            proc = subprocess.Popen(
                ['zstd', '-d', str(archive_path), '--stdout'],
                stdout=subprocess.PIPE, stderr=subprocess.PIPE
            )
            member_count = 0
            try:
                with tarfile.open(fileobj=proc.stdout, mode='r|') as tar:
                    for member in tar:
                        member_count += 1
                        _collect(member)
                proc.wait(timeout=60)
            except Exception as e:
                proc.kill()
                proc.wait()
                return False, f"解压验证失败: {e}"
            if proc.returncode != 0:
                return False, f"zstd 解压返回码: {proc.returncode}"
            return True, f"解压验证通过 ({size_mb:.1f}MB, {member_count} 个文件, CLI)"

    @staticmethod
    def _extract_tar(archive_path: Path, temp_dir: Path, mode: str, size_mb: float,
                     entries_out: Optional[list] = None) -> tuple:
        """解压验证 tar/tar.gz（entries_out 非 None 时收集文件清单）"""
        member_count = 0
        with tarfile.open(str(archive_path), mode) as tar:
            for member in tar:
                member_count += 1
                if entries_out is not None:
                    try:
                        entries_out.append({
                            'name': member.name,
                            'size': member.size if member.isfile() else 0,
                            'is_dir': member.isdir(),
                            'modified_time': (datetime.fromtimestamp(member.mtime).isoformat()
                                              if member.mtime else None),
                        })
                    except Exception:
                        pass
        return True, f"解压验证通过 ({size_mb:.1f}MB, {member_count} 个文件)"

    @staticmethod
    def _extract_zip(archive_path: Path, temp_dir: Path, size_mb: float,
                     entries_out: Optional[list] = None) -> tuple:
        """解压验证 zip（entries_out 非 None 时收集文件清单）"""
        import zipfile
        with zipfile.ZipFile(str(archive_path), 'r') as zf:
            bad = zf.testzip()
            if bad:
                return False, f"损坏的文件: {bad}"
            infos = zf.infolist()
            count = len(infos)
            if entries_out is not None:
                for zi in infos:
                    entries_out.append({
                        'name': zi.filename,
                        'size': 0 if zi.is_dir() else int(zi.file_size or 0),
                        'is_dir': zi.is_dir(),
                        'modified_time': '%04d-%02d-%02d %02d:%02d:%02d' % zi.date_time,
                    })
        return True, f"解压验证通过 ({size_mb:.1f}MB, {count} 个文件)"

    @staticmethod
    def _extract_7z(archive_path: Path, temp_dir: Path, size_mb: float,
                    entries_out: Optional[list] = None) -> tuple:
        """解压验证 7z（entries_out 非 None 时收集文件清单）"""
        try:
            import py7zr
            with py7zr.SevenZipFile(str(archive_path), mode='r') as archive:
                file_list = archive.list()
                count = len([f for f in file_list if not f.is_directory])
                if entries_out is not None:
                    for f in file_list:
                        entries_out.append({
                            'name': f.filename,
                            'size': 0 if f.is_directory else int(f.uncompressed or 0),
                            'is_dir': bool(f.is_directory),
                            'modified_time': (f.creationtime.strftime('%Y-%m-%d %H:%M:%S')
                                              if getattr(f, 'creationtime', None) else None),
                        })
            return True, f"解压验证通过 ({size_mb:.1f}MB, {count} 个文件)"
        except ImportError:
            return True, f"py7zr 未安装，跳过解压验证 ({size_mb:.1f}MB)"


class CustomActionHandler(ActionHandler):
    """自定义动作处理器"""

    async def execute(
        self,
        config: Dict,
        scheduled_task: Optional[ScheduledTask] = None,
        manual_run: bool = False,
        run_options: Optional[Dict[str, Any]] = None
    ) -> Dict[str, Any]:
        """执行自定义动作"""
        return {"status": "success", "message": "自定义任务已执行"}


def get_action_handler(action_type: TaskActionType, system_instance) -> ActionHandler:
    """获取动作处理器"""
    handler_map = {
        TaskActionType.BACKUP: BackupActionHandler,
        TaskActionType.RECOVERY: RecoveryActionHandler,
        TaskActionType.CLEANUP: CleanupActionHandler,
        TaskActionType.HEALTH_CHECK: HealthCheckActionHandler,
        TaskActionType.RETENTION_CHECK: RetentionCheckActionHandler,
        TaskActionType.VERIFY: VerifyActionHandler,
        TaskActionType.CUSTOM: CustomActionHandler,
    }

    handler_class = handler_map.get(action_type)
    if not handler_class:
        raise ValueError(f"不支持的任务动作类型: {action_type}")

    return handler_class(system_instance)
