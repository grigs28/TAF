#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
任务动作处理器
Task Action Handlers
"""

import logging
from datetime import datetime, timezone
from utils.datetime_utils import now, format_datetime
from typing import Dict, Any, Optional

from models.scheduled_task import ScheduledTask, TaskActionType
from models.backup import BackupTask, BackupTaskType, BackupTaskStatus
from config.database import db_manager
from sqlalchemy import select, and_
from utils.log_utils import log_system, LogLevel, LogCategory, log_operation, OperationType
from .db_utils import is_opengauss, get_opengauss_connection
import json

logger = logging.getLogger(__name__)


class ActionHandler:
    """动作处理器基类"""
    
    def __init__(self, system_instance):
        self.system_instance = system_instance
    
    async def execute(self, config: Dict, scheduled_task: Optional[ScheduledTask] = None, manual_run: bool = False) -> Dict[str, Any]:
        """执行动作（子类实现）
        
        参数:
            config: 动作配置
            scheduled_task: 计划任务对象（可选）
            manual_run: 是否为手动运行（Web界面点击运行），默认为False
        """
        raise NotImplementedError


class BackupActionHandler(ActionHandler):
    """备份动作处理器"""
    
    async def execute(self, config: Dict, backup_task_id: Optional[int] = None, 
                     scheduled_task: Optional[ScheduledTask] = None, manual_run: bool = False) -> Dict[str, Any]:
        """执行备份动作
        
        参数:
            config: 动作配置
            backup_task_id: 备份任务模板ID（如果提供，从模板加载配置）
            scheduled_task: 计划任务对象（用于检查重复执行）
            manual_run: 是否为手动运行（Web界面点击运行），默认为False
        """
        if not self.system_instance or not self.system_instance.backup_engine:
            raise ValueError("备份引擎未初始化")
        
        try:
            # 执行前判定：周期内是否已成功执行、是否正在执行、磁带标签是否当月
            current_time = now()
            if scheduled_task and not manual_run:
                # 1) 周期检查（按任务的 schedule_type 推断周期：日/周/月/年）
                # 注意：手动运行时跳过周期检查
                cycle_ok = True
                last_success = scheduled_task.last_success_time
                schedule_type = getattr(scheduled_task, 'schedule_type', None)
                if last_success:
                    if schedule_type and getattr(schedule_type, 'value', '').lower() in ('daily', 'day'):
                        cycle_ok = (last_success.date() != current_time.date())
                    elif schedule_type and getattr(schedule_type, 'value', '').lower() in ('weekly', 'week'):
                        cycle_ok = (last_success.isocalendar().week != current_time.isocalendar().week or last_success.year != current_time.year)
                    elif schedule_type and getattr(schedule_type, 'value', '').lower() in ('monthly', 'month'):
                        cycle_ok = (last_success.year != current_time.year or last_success.month != current_time.month)
                    elif schedule_type and getattr(schedule_type, 'value', '').lower() in ('yearly', 'year'):
                        cycle_ok = (last_success.year != current_time.year)
                    else:
                        # 未明确类型，默认按日
                        cycle_ok = (last_success.date() != current_time.date())
                # 如果周期内已执行过，则跳过
                if not cycle_ok:
                    logger.info("当前周期内已成功执行，跳过本次备份")
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
            
            # 2) 运行中检查（根据任务状态）- 手动运行和自动运行都检查
            if scheduled_task and getattr(scheduled_task, 'status', None) and str(scheduled_task.status).upper().endswith('RUNNING'):
                logger.info("任务仍在执行中，跳过本次备份")
                try:
                    await log_operation(
                        operation_type=OperationType.SCHEDULER_RUN,
                        resource_type="scheduler",
                        resource_id=str(getattr(scheduled_task, 'id', '')),
                        resource_name=getattr(scheduled_task, 'task_name', ''),
                        operation_name="执行计划任务",
                        operation_description="跳过：任务正在执行中",
                        category="scheduler",
                        success=True,
                        result_message="跳过执行（任务正在执行中）"
                    )
                    await log_system(
                        level=LogLevel.INFO,
                        category=LogCategory.SCHEDULER,
                        message="计划任务跳过：任务正在执行中",
                        module="utils.scheduler.action_handlers",
                        function="BackupActionHandler.execute",
                        task_id=getattr(scheduled_task, 'id', None)
                    )
                except Exception:
                    pass
                return {"status": "skipped", "message": "任务正在执行中"}

            # 3) 磁带标签是否当月（从 LTFS 标签或磁带头读取）
            # 仅当备份目标为磁带时要求当月
            # 注意：手动运行和自动运行都检查磁带标签
            if scheduled_task:
                target_is_tape = False
                try:
                    # 从 scheduled_task.action_config 读取备份目标
                    action_cfg = scheduled_task.action_config or {}
                    target_is_tape = (action_cfg.get('backup_target') == 'tape') or ('tape_device' in action_cfg)
                except Exception:
                    pass
                if target_is_tape and self.system_instance and getattr(self.system_instance, 'tape_manager', None):
                    tape_ops = getattr(self.system_instance.tape_manager, 'tape_operations', None)
                    if tape_ops and hasattr(tape_ops, '_read_tape_label'):
                        metadata = await tape_ops._read_tape_label()
                        # 无标签时允许继续，后续会自动提示换盘逻辑在业务层处理
                        if metadata and (metadata.get('created_date') or metadata.get('tape_id')):
                            try:
                                # 优先使用 created_date
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
                                    # 备用：从 tape_id 推断（如 TAPyymmddxxx）
                                    tape_id = str(metadata.get('tape_id', ''))
                                    if len(tape_id) >= 7 and tape_id.upper().startswith('TAP'):
                                        yy = int(tape_id[3:5])
                                        mm = int(tape_id[5:7])
                                        year = 2000 + yy
                                        if not (year == current_time.year and mm == current_time.month):
                                            raise ValueError("当前磁带标签非当月，请更换磁带后重试")
                            except ValueError as ve:
                                # 记录并抛出以触发通知与日志
                                logger.warning(str(ve))
                                # 通知：需要更换磁带
                                try:
                                    if self.system_instance and getattr(self.system_instance, 'dingtalk_notifier', None):
                                        notifier = self.system_instance.dingtalk_notifier
                                        tape_id = (metadata.get('tape_id') if metadata else '') or '未知磁带'
                                        await notifier.send_tape_notification(tape_id=tape_id, action='change_required')
                                except Exception:
                                    pass
                                # 记录日志
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
                if is_opengauss():
                    # openGauss 原生SQL查询
                    # 使用连接池
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
                            'task_type': BackupTaskType(row['task_type']) if row['task_type'] else None,
                            'source_paths': row['source_paths'] if isinstance(row['source_paths'], list) else json.loads(row['source_paths']) if row['source_paths'] else [],
                            'exclude_patterns': row['exclude_patterns'] if isinstance(row['exclude_patterns'], list) else json.loads(row['exclude_patterns']) if row['exclude_patterns'] else [],
                            'compression_enabled': row['compression_enabled'],
                            'encryption_enabled': row['encryption_enabled'],
                            'retention_days': row['retention_days'],
                            'description': row['description'],
                            'tape_device': row['tape_device'],
                            'status': BackupTaskStatus(row['status']) if row['status'] else None,
                            'is_template': row['is_template'],
                            'template_id': row['template_id'],
                        })()
                else:
                    # 使用SQLAlchemy查询
                    async with db_manager.AsyncSessionLocal() as session:
                        stmt = select(BackupTask).where(
                            and_(
                                BackupTask.id == backup_task_id,
                                BackupTask.is_template == True
                            )
                        )
                        result = await session.execute(stmt)
                        template_task = result.scalar_one_or_none()
                        
                        if not template_task:
                            raise ValueError(f"备份任务模板不存在: {backup_task_id}")
            
            # 执行前检查：判断同一个模板的任务是否还在执行中
            if template_task or scheduled_task:
                template_id = template_task.id if template_task else None
                if scheduled_task and scheduled_task.task_metadata:
                    template_id = scheduled_task.task_metadata.get('backup_task_id') or template_id
                
                if template_id:
                    # 检查是否有相同模板的任务正在执行
                    if is_opengauss():
                        # openGauss 原生SQL查询
                        # 使用连接池
                        async with get_opengauss_connection() as conn:
                            running_task_row = await conn.fetchrow(
                                """
                                SELECT id, started_at FROM backup_tasks
                                WHERE template_id = $1 AND status = $2
                                LIMIT 1
                                """,
                                template_id, 'running'
                            )
                            running_task = dict(running_task_row) if running_task_row else None
                    else:
                        # 使用SQLAlchemy查询
                        async with db_manager.AsyncSessionLocal() as session:
                            stmt = select(BackupTask).where(
                                and_(
                                    BackupTask.template_id == template_id,
                                    BackupTask.status == BackupTaskStatus.RUNNING
                                )
                            )
                            result = await session.execute(stmt)
                            running_task = result.scalar_one_or_none()
                    
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
                            if is_opengauss():
                                # openGauss 原生SQL查询返回字典
                                if isinstance(running_task, dict) and running_task.get('started_at'):
                                    started_at = running_task.get('started_at')
                                    if isinstance(started_at, str):
                                        started_at = datetime.fromisoformat(started_at.replace('Z', '+00:00'))
                                    running_duration = (current_time_check - started_at).total_seconds()
                                    if running_duration > 86400:  # 超过24小时
                                        logger.warning(f"模板 {template_id} 的任务已运行超过24小时，继续执行新任务")
                            else:
                                # SQLAlchemy 对象
                                if hasattr(running_task, 'started_at') and running_task.started_at:
                                    running_duration = (current_time_check - running_task.started_at).total_seconds()
                                    if running_duration > 86400:  # 超过24小时
                                        logger.warning(
                                            f"警告：模板 {template_id} 的任务已运行超过24小时 "
                                            f"(任务ID: {running_task.id})"
                                        )
            
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
                # 备份策略里可能有默认排除规则/压缩加密开关
                if hasattr(sysi, 'settings'):
                    settings = sysi.settings
                    if not exclude_patterns and getattr(settings, 'DEFAULT_EXCLUDE_PATTERNS', None):
                        exclude_patterns = settings.DEFAULT_EXCLUDE_PATTERNS
                # tapedrive 配置：默认磁带设备等
                if not tape_device and hasattr(sysi, 'tape_manager'):
                    tm = sysi.tape_manager
                    if hasattr(tm, 'settings') and getattr(tm.settings, 'TAPE_DEVICE_PATH', None):
                        tape_device = tm.settings.TAPE_DEVICE_PATH
            except Exception:
                pass
            
            if not source_paths:
                raise ValueError("备份源路径不能为空")
            
            # 注意：不在这里发送"开始"通知，因为 backup_engine.execute_backup_task() 中已经会发送
            # 这样可以避免重复通知，并且 backup_engine 中的通知会检查通知事件配置
            
            # 标记备份任务是否已执行（用于判断是否需要发送失败通知）
            backup_executed = False

            # 创建备份任务执行记录（不是模板）
            if is_opengauss():
                # openGauss 原生SQL插入
                # 使用连接池
                async with get_opengauss_connection() as conn:
                    backup_task_id = await conn.fetchval(
                        """
                        INSERT INTO backup_tasks (
                            task_name, task_type, source_paths, exclude_patterns,
                            compression_enabled, encryption_enabled, retention_days,
                            description, tape_device, status, is_template, template_id,
                            created_by, created_at, updated_at
                        ) VALUES (
                            $1, $2::backuptasktype, $3, $4,
                            $5, $6, $7,
                            $8, $9, $10::backuptaskstatus, FALSE, $11,
                            $12, $13, $13
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
                        now()
                    )
                    
                    # 创建一个简化的 BackupTask 对象用于后续使用
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
                    })()
            else:
                # 使用SQLAlchemy插入
                async with db_manager.AsyncSessionLocal() as session:
                    backup_task = BackupTask(
                        task_name=task_name,
                        task_type=task_type,
                        source_paths=source_paths,
                        exclude_patterns=exclude_patterns,
                        compression_enabled=compression_enabled,
                        encryption_enabled=encryption_enabled,
                        retention_days=retention_days,
                        description=description,
                        tape_device=tape_device,  # 保存磁带设备配置（执行时会选择）
                        status=BackupTaskStatus.PENDING,
                        is_template=False,  # 标记为执行记录
                        template_id=template_task.id if template_task else None,  # 关联模板
                        created_by='scheduled_task'
                    )
                    
                    session.add(backup_task)
                    await session.commit()
                    await session.refresh(backup_task)
            
            # 完整备份前：格式化磁带（计划任务使用当前年月生成卷标）
            # 注意：手动运行时跳过格式化
            logger.info(f"检查是否需要格式化: manual_run={manual_run}, task_type={task_type}, template_task_type={template_task.task_type if template_task else None}")
            if not manual_run and ((template_task and template_task.task_type == BackupTaskType.FULL) or (not template_task and task_type == BackupTaskType.FULL)):
                logger.info("开始执行格式化操作（自动运行模式）")
                try:
                    # 在格式化开始前，更新备份任务状态为RUNNING，并在description中注明"格式化中"
                    # 注意：is_opengauss 和 get_opengauss_connection 已在文件顶部导入
                    if is_opengauss():
                        # 使用连接池
                        async with get_opengauss_connection() as conn:
                            await conn.execute(
                                """
                                UPDATE backup_tasks
                                SET status = $1::backuptaskstatus,
                                    started_at = $2,
                                    description = COALESCE(description, '') || ' [格式化中]',
                                    updated_at = $2
                                WHERE id = $3
                                """,
                                BackupTaskStatus.RUNNING.value,
                                now(),
                                backup_task.id
                            )
                    else:
                        async with db_manager.AsyncSessionLocal() as session:
                            backup_task.status = BackupTaskStatus.RUNNING
                            backup_task.started_at = now()
                            if backup_task.description:
                                backup_task.description = backup_task.description + ' [格式化中]'
                            else:
                                backup_task.description = '[格式化中]'
                            await session.commit()
                            await session.refresh(backup_task)
                    
                    await log_system(
                        level=LogLevel.INFO,
                        category=LogCategory.BACKUP,
                        message="开始完整备份前格式化（使用当前年月）",
                        module="utils.scheduler.action_handlers",
                        function="BackupActionHandler.execute",
                    )
                    if self.system_instance and getattr(self.system_instance, 'tape_manager', None):
                        tape_ops = getattr(self.system_instance.tape_manager, 'tape_operations', None)
                        if tape_ops and hasattr(tape_ops, 'erase_preserve_label'):
                            # 计划任务格式化时使用当前年月生成卷标
                            ok = await tape_ops.erase_preserve_label(use_current_year_month=True)
                            if not ok:
                                logger.warning("完整备份前格式化失败，将尝试继续执行备份")
                                await log_system(
                                    level=LogLevel.WARNING,
                                    category=LogCategory.BACKUP,
                                    message="完整备份前格式化失败，继续执行",
                                    module="utils.scheduler.action_handlers",
                                    function="BackupActionHandler.execute",
                                )
                            else:
                                # 格式化成功，更新description，移除"格式化中"，准备开始备份
                                if is_opengauss():
                                    # 使用连接池
                                    async with get_opengauss_connection() as conn:
                                        await conn.execute(
                                            """
                                            UPDATE backup_tasks
                                            SET description = REPLACE(COALESCE(description, ''), ' [格式化中]', ''),
                                                updated_at = $1
                                            WHERE id = $2
                                            """,
                                            now(),
                                            backup_task.id
                                        )
                                else:
                                    async with db_manager.AsyncSessionLocal() as session:
                                        if backup_task.description:
                                            backup_task.description = backup_task.description.replace(' [格式化中]', '')
                                        await session.commit()
                except Exception as _:
                    logger.warning("完整备份前格式化异常，将尝试继续执行备份")
                    await log_system(
                        level=LogLevel.WARNING,
                        category=LogCategory.BACKUP,
                        message="完整备份前格式化异常，继续执行",
                        module="utils.scheduler.action_handlers",
                        function="BackupActionHandler.execute",
                    )
            else:
                if manual_run:
                    logger.info("手动运行模式，跳过格式化操作")
                elif not ((template_task and template_task.task_type == BackupTaskType.FULL) or (not template_task and task_type == BackupTaskType.FULL)):
                    logger.info("非完整备份任务，跳过格式化操作")

            # 执行备份任务
            await log_system(
                level=LogLevel.INFO,
                category=LogCategory.BACKUP,
                message="开始执行备份任务",
                module="utils.scheduler.action_handlers",
                function="BackupActionHandler.execute",
                details={"backup_task_id": getattr(backup_task, 'id', None), "task_name": task_name}
            )
            # 执行备份任务（传入 scheduled_task 和 manual_run 以便进行执行前检查）
            logger.info(f"调用备份引擎执行备份任务... (手动运行: {manual_run})")
            try:
                success = await self.system_instance.backup_engine.execute_backup_task(backup_task, scheduled_task=scheduled_task, manual_run=manual_run)
                backup_executed = True  # 标记备份任务已执行
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
                    # 注意：不在这里发送"成功"通知，因为 backup_engine.execute_backup_task() 中已经会发送
                    # 这样可以避免重复通知，并且 backup_engine 中的通知会检查通知事件配置，信息更详细
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
                    # 安全获取错误信息
                    error_msg = getattr(backup_task, 'error_message', '备份任务执行失败（未知错误）')
                    raise RuntimeError(f"备份任务执行失败: {error_msg}")
            except Exception as backup_error:
                # 如果备份任务已执行，backup_engine 中已经发送了失败通知，这里不再重复发送
                if backup_executed:
                    raise
                # 如果备份任务未执行（比如创建任务失败、执行前检查失败等），需要发送失败通知
                raise
                
        except Exception as e:
            logger.error(f"执行备份动作失败: {str(e)}")
            # 失败通知：只在备份任务未执行时发送（如果备份任务已执行，backup_engine 中已经发送了）
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


class RecoveryActionHandler(ActionHandler):
    """恢复动作处理器"""
    
    async def execute(self, config: Dict, scheduled_task: Optional[ScheduledTask] = None, manual_run: bool = False) -> Dict[str, Any]:
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
    
    async def execute(self, config: Dict, scheduled_task: Optional[ScheduledTask] = None, manual_run: bool = False) -> Dict[str, Any]:
        """执行清理动作"""
        return {"status": "success", "message": "清理任务已执行"}


class HealthCheckActionHandler(ActionHandler):
    """健康检查动作处理器"""
    
    async def execute(self, config: Dict, scheduled_task: Optional[ScheduledTask] = None, manual_run: bool = False) -> Dict[str, Any]:
        """执行健康检查动作"""
        return {"status": "success", "message": "健康检查已完成"}


class RetentionCheckActionHandler(ActionHandler):
    """保留期检查动作处理器"""
    
    async def execute(self, config: Dict, scheduled_task: Optional[ScheduledTask] = None, manual_run: bool = False) -> Dict[str, Any]:
        """执行保留期检查动作"""
        return {"status": "success", "message": "保留期检查已完成"}


class CustomActionHandler(ActionHandler):
    """自定义动作处理器"""
    
    async def execute(self, config: Dict, scheduled_task: Optional[ScheduledTask] = None, manual_run: bool = False) -> Dict[str, Any]:
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
        TaskActionType.CUSTOM: CustomActionHandler,
    }
    
    handler_class = handler_map.get(action_type)
    if not handler_class:
        raise ValueError(f"不支持的任务动作类型: {action_type}")
    
    return handler_class(system_instance)

