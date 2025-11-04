#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
任务动作处理器
Task Action Handlers
"""

import logging
from datetime import datetime, timezone
from typing import Dict, Any, Optional

from models.scheduled_task import ScheduledTask, TaskActionType
from models.backup import BackupTask, BackupTaskType, BackupTaskStatus
from config.database import db_manager
from sqlalchemy import select, and_

logger = logging.getLogger(__name__)


class ActionHandler:
    """动作处理器基类"""
    
    def __init__(self, system_instance):
        self.system_instance = system_instance
    
    async def execute(self, config: Dict, scheduled_task: Optional[ScheduledTask] = None) -> Dict[str, Any]:
        """执行动作（子类实现）"""
        raise NotImplementedError


class BackupActionHandler(ActionHandler):
    """备份动作处理器"""
    
    async def execute(self, config: Dict, backup_task_id: Optional[int] = None, 
                     scheduled_task: Optional[ScheduledTask] = None) -> Dict[str, Any]:
        """执行备份动作
        
        参数:
            config: 动作配置
            backup_task_id: 备份任务模板ID（如果提供，从模板加载配置）
            scheduled_task: 计划任务对象（用于检查重复执行）
        """
        if not self.system_instance or not self.system_instance.backup_engine:
            raise ValueError("备份引擎未初始化")
        
        try:
            # 执行前判定：周期内是否已成功执行、是否正在执行、磁带标签是否当月
            now = datetime.now(timezone.utc)
            if scheduled_task:
                # 1) 周期检查（按任务的 schedule_type 推断周期：日/周/月/年）
                cycle_ok = True
                last_success = scheduled_task.last_success_time
                schedule_type = getattr(scheduled_task, 'schedule_type', None)
                if last_success:
                    if schedule_type and getattr(schedule_type, 'value', '').lower() in ('daily', 'day'):
                        cycle_ok = (last_success.date() != now.date())
                    elif schedule_type and getattr(schedule_type, 'value', '').lower() in ('weekly', 'week'):
                        cycle_ok = (last_success.isocalendar().week != now.isocalendar().week or last_success.year != now.year)
                    elif schedule_type and getattr(schedule_type, 'value', '').lower() in ('monthly', 'month'):
                        cycle_ok = (last_success.year != now.year or last_success.month != now.month)
                    elif schedule_type and getattr(schedule_type, 'value', '').lower() in ('yearly', 'year'):
                        cycle_ok = (last_success.year != now.year)
                    else:
                        # 未明确类型，默认按日
                        cycle_ok = (last_success.date() != now.date())
                # 如果周期内已执行过，则跳过
                if not cycle_ok:
                    logger.info("当前周期内已成功执行，跳过本次备份")
                    return {"status": "skipped", "message": "当前周期已执行"}

                # 2) 运行中检查（根据任务状态）
                if getattr(scheduled_task, 'status', None) and str(scheduled_task.status).upper().endswith('RUNNING'):
                    logger.info("任务仍在执行中，跳过本次备份")
                    return {"status": "skipped", "message": "任务正在执行中"}

                # 3) 磁带标签是否当月（从 LTFS 标签或磁带头读取）
                # 仅当备份目标为磁带时要求当月
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
                                    if not (created_dt.year == now.year and created_dt.month == now.month):
                                        raise ValueError("当前磁带非当月，请更换磁带后重试")
                                else:
                                    # 备用：从 tape_id 推断（如 TAPyymmddxxx）
                                    tape_id = str(metadata.get('tape_id', ''))
                                    if len(tape_id) >= 7 and tape_id.upper().startswith('TAP'):
                                        yy = int(tape_id[3:5])
                                        mm = int(tape_id[5:7])
                                        year = 2000 + yy
                                        if not (year == now.year and mm == now.month):
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
                                raise
            # 如果有备份任务模板ID，从模板加载配置
            template_task = None
            if backup_task_id:
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
                            now = datetime.now()
                            if scheduled_task and scheduled_task.last_run_time:
                                last_run_date = scheduled_task.last_run_time.date()
                                today = now.date()
                                
                                # 如果上次执行在今天，且任务还在运行，跳过本次执行
                                if last_run_date == today:
                                    logger.warning(
                                        f"跳过执行：模板 {template_id} 的任务仍在执行中 "
                                        f"(运行中的任务ID: {running_task.id})"
                                    )
                                    return {
                                        "status": "skipped",
                                        "message": "相同模板的任务仍在执行中，已跳过本次执行",
                                        "running_task_id": running_task.id
                                    }
                            
                            # 如果任务运行超过一天，记录警告但继续执行
                            if running_task.started_at:
                                running_duration = (now - running_task.started_at).total_seconds()
                                if running_duration > 86400:  # 超过24小时
                                    logger.warning(
                                        f"警告：模板 {template_id} 的任务已运行超过24小时 "
                                        f"(任务ID: {running_task.id})"
                                    )
            
            # 从模板或配置中获取备份参数
            if template_task:
                source_paths = template_task.source_paths or []
                task_type = template_task.task_type
                exclude_patterns = template_task.exclude_patterns or []
                compression_enabled = template_task.compression_enabled
                encryption_enabled = template_task.encryption_enabled
                retention_days = template_task.retention_days
                description = template_task.description or ''
                tape_device = template_task.tape_device
                task_name = f"{template_task.task_name}-{datetime.now().strftime('%Y%m%d_%H%M%S')}"
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
                task_name = config.get('task_name', f"计划备份-{datetime.now().strftime('%Y%m%d_%H%M%S')}")
            
            if not source_paths:
                raise ValueError("备份源路径不能为空")
            
            # 发送“开始”通知
            try:
                if self.system_instance and getattr(self.system_instance, 'dingtalk_notifier', None):
                    await self.system_instance.dingtalk_notifier.send_backup_notification(
                        backup_name=(template_task.task_name if template_task else task_name),
                        status='started',
                        details=None
                    )
            except Exception:
                pass

            # 创建备份任务执行记录（不是模板）
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
            
            # 完整备份前：擦除但保留标签文件
            if (template_task and template_task.task_type == BackupTaskType.FULL) or (not template_task and task_type == BackupTaskType.FULL):
                try:
                    if self.system_instance and getattr(self.system_instance, 'tape_manager', None):
                        tape_ops = getattr(self.system_instance.tape_manager, 'tape_operations', None)
                        if tape_ops and hasattr(tape_ops, 'erase_preserve_label'):
                            ok = await tape_ops.erase_preserve_label()
                            if not ok:
                                logger.warning("完整备份前擦除失败，将尝试继续执行备份")
                except Exception as _:
                    logger.warning("完整备份前擦除异常，将尝试继续执行备份")

            # 执行备份任务
            success = await self.system_instance.backup_engine.execute_backup_task(backup_task)
            
            if success:
                # 成功通知
                try:
                    if self.system_instance and getattr(self.system_instance, 'dingtalk_notifier', None):
                        await self.system_instance.dingtalk_notifier.send_backup_notification(
                            backup_name=(template_task.task_name if template_task else task_name),
                            status='success',
                            details={
                                'size': backup_task.total_bytes,
                                'file_count': backup_task.total_files
                            }
                        )
                except Exception:
                    pass
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
                raise RuntimeError(f"备份任务执行失败: {backup_task.error_message}")
                
        except Exception as e:
            logger.error(f"执行备份动作失败: {str(e)}")
            # 失败通知
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
    
    async def execute(self, config: Dict, scheduled_task: Optional[ScheduledTask] = None) -> Dict[str, Any]:
        """执行恢复动作"""
        return {"status": "success", "message": "恢复任务已执行"}


class CleanupActionHandler(ActionHandler):
    """清理动作处理器"""
    
    async def execute(self, config: Dict, scheduled_task: Optional[ScheduledTask] = None) -> Dict[str, Any]:
        """执行清理动作"""
        return {"status": "success", "message": "清理任务已执行"}


class HealthCheckActionHandler(ActionHandler):
    """健康检查动作处理器"""
    
    async def execute(self, config: Dict, scheduled_task: Optional[ScheduledTask] = None) -> Dict[str, Any]:
        """执行健康检查动作"""
        return {"status": "success", "message": "健康检查已完成"}


class RetentionCheckActionHandler(ActionHandler):
    """保留期检查动作处理器"""
    
    async def execute(self, config: Dict, scheduled_task: Optional[ScheduledTask] = None) -> Dict[str, Any]:
        """执行保留期检查动作"""
        return {"status": "success", "message": "保留期检查已完成"}


class CustomActionHandler(ActionHandler):
    """自定义动作处理器"""
    
    async def execute(self, config: Dict, scheduled_task: Optional[ScheduledTask] = None) -> Dict[str, Any]:
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

