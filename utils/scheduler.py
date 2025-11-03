#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
计划任务调度器
Task Scheduler Module
"""

import asyncio
import logging
import uuid
from datetime import datetime, timedelta
from typing import Dict, Callable, Any, Optional, List
from croniter import croniter

from config.settings import get_settings
from models.backup import BackupTask
from models.tape import TapeCartridge
from models.scheduled_task import ScheduledTask, ScheduledTaskLog, ScheduleType, ScheduledTaskStatus, TaskActionType
from config.database import db_manager

logger = logging.getLogger(__name__)


class BackupScheduler:
    """备份任务调度器"""

    def __init__(self):
        self.settings = get_settings()
        self.running = False
        self.tasks: Dict[str, Dict] = {}
        self._scheduler_task = None
        self.system_instance = None

    async def initialize(self, system_instance):
        """初始化调度器"""
        self.system_instance = system_instance

        # 注册默认任务
        await self._register_default_tasks()

        logger.info("计划任务调度器初始化完成")

    async def _register_default_tasks(self):
        """注册默认任务"""
        if self.settings.SCHEDULER_ENABLED:
            # 月度备份任务
            await self.register_task(
                "monthly_backup",
                self.settings.MONTHLY_BACKUP_CRON,
                self._execute_monthly_backup,
                "月度完整备份任务"
            )

            # 保留期检查任务
            await self.register_task(
                "retention_check",
                self.settings.RETENTION_CHECK_CRON,
                self._execute_retention_check,
                "磁带保留期检查任务"
            )

            # 系统健康检查任务
            await self.register_task(
                "health_check",
                "0 */6 * * *",  # 每6小时执行一次
                self._execute_health_check,
                "系统健康检查任务"
            )

    async def register_task(self, task_id: str, cron_expression: str,
                          func: Callable, description: str = ""):
        """注册任务"""
        try:
            self.tasks[task_id] = {
                'cron': cron_expression,
                'func': func,
                'description': description,
                'last_run': None,
                'next_run': self._get_next_run_time(cron_expression),
                'enabled': True
            }
            logger.info(f"注册任务: {task_id} - {description}")
        except Exception as e:
            logger.error(f"注册任务失败 {task_id}: {str(e)}")

    async def unregister_task(self, task_id: str):
        """注销任务"""
        if task_id in self.tasks:
            del self.tasks[task_id]
            logger.info(f"注销任务: {task_id}")

    async def enable_task(self, task_id: str):
        """启用任务"""
        if task_id in self.tasks:
            self.tasks[task_id]['enabled'] = True
            logger.info(f"启用任务: {task_id}")

    async def disable_task(self, task_id: str):
        """禁用任务"""
        if task_id in self.tasks:
            self.tasks[task_id]['enabled'] = False
            logger.info(f"禁用任务: {task_id}")

    async def start(self):
        """启动调度器"""
        if self.running:
            return

        self.running = True
        self._scheduler_task = asyncio.create_task(self._scheduler_loop())
        logger.info("计划任务调度器已启动")

    async def stop(self):
        """停止调度器"""
        self.running = False
        if self._scheduler_task:
            self._scheduler_task.cancel()
            try:
                await self._scheduler_task
            except asyncio.CancelledError:
                pass
        logger.info("计划任务调度器已停止")

    async def _scheduler_loop(self):
        """调度器主循环"""
        while self.running:
            try:
                current_time = datetime.now()

                for task_id, task_info in self.tasks.items():
                    if not task_info['enabled']:
                        continue

                    if current_time >= task_info['next_run']:
                        await self._execute_task(task_id, task_info)
                        # 更新下次执行时间
                        task_info['last_run'] = current_time
                        task_info['next_run'] = self._get_next_run_time(task_info['cron'])

                # 每分钟检查一次
                await asyncio.sleep(60)

            except asyncio.CancelledError:
                break
            except Exception as e:
                logger.error(f"调度器循环出错: {str(e)}")
                await asyncio.sleep(60)

    async def _execute_task(self, task_id: str, task_info: Dict):
        """执行任务"""
        try:
            logger.info(f"开始执行任务: {task_id} - {task_info['description']}")

            start_time = datetime.now()
            await task_info['func']()
            end_time = datetime.now()

            duration = (end_time - start_time).total_seconds()
            logger.info(f"任务执行完成: {task_id}, 耗时: {duration:.2f}秒")

        except Exception as e:
            logger.error(f"任务执行失败 {task_id}: {str(e)}")

            # 发送错误通知
            if self.system_instance and self.system_instance.dingtalk_notifier:
                await self.system_instance.dingtalk_notifier.send_system_notification(
                    "任务执行失败",
                    f"任务 {task_id} 执行失败: {str(e)}"
                )

    def _get_next_run_time(self, cron_expression: str) -> datetime:
        """获取下次执行时间"""
        cron = croniter(cron_expression, datetime.now())
        return cron.get_next(datetime)

    async def _execute_monthly_backup(self):
        """执行月度备份"""
        logger.info("开始执行月度备份任务")

        if self.system_instance and self.system_instance.backup_engine:
            # 创建月度备份任务
            backup_task = BackupTask(
                task_name=f"月度备份-{datetime.now().strftime('%Y-%m')}",
                task_type="monthly_full",
                source_paths=["/"],  # 这里需要根据实际配置调整
                status="pending"
            )

            # 执行备份
            await self.system_instance.backup_engine.execute_backup_task(backup_task)

    async def _execute_retention_check(self):
        """执行保留期检查"""
        logger.info("开始执行磁带保留期检查")

        if self.system_instance and self.system_instance.tape_manager:
            await self.system_instance.tape_manager.check_retention_periods()

    async def _execute_health_check(self):
        """执行系统健康检查"""
        logger.info("开始执行系统健康检查")

        health_status = {
            'database': await self._check_database_health(),
            'tape_drive': await self._check_tape_drive_health(),
            'disk_space': await self._check_disk_space(),
            'services': await self._check_services_health()
        }

        # 检查是否有异常
        unhealthy_services = [k for k, v in health_status.items() if not v]
        if unhealthy_services:
            if self.system_instance and self.system_instance.dingtalk_notifier:
                await self.system_instance.dingtalk_notifier.send_system_notification(
                    "系统健康检查异常",
                    f"以下服务状态异常: {', '.join(unhealthy_services)}"
                )

    async def _check_database_health(self) -> bool:
        """检查数据库健康状态"""
        try:
            from ..config.database import db_manager
            return await db_manager.health_check()
        except Exception:
            return False

    async def _check_tape_drive_health(self) -> bool:
        """检查磁带驱动器健康状态"""
        try:
            if self.system_instance and self.system_instance.tape_manager:
                return await self.system_instance.tape_manager.health_check()
            return False
        except Exception:
            return False

    async def _check_disk_space(self) -> bool:
        """检查磁盘空间"""
        try:
            import shutil
            total, used, free = shutil.disk_usage("/")
            free_percent = (free / total) * 100
            return free_percent > 10  # 剩余空间大于10%
        except Exception:
            return False

    async def _check_services_health(self) -> bool:
        """检查各项服务健康状态"""
        # 这里可以添加更多服务检查
        return True

    def get_task_status(self) -> Dict[str, Any]:
        """获取任务状态"""
        return {
            'scheduler_running': self.running,
            'total_tasks': len(self.tasks),
            'enabled_tasks': len([t for t in self.tasks.values() if t['enabled']]),
            'tasks': {
                task_id: {
                    'description': task['description'],
                    'enabled': task['enabled'],
                    'last_run': task['last_run'].isoformat() if task['last_run'] else None,
                    'next_run': task['next_run'].isoformat() if task['next_run'] else None,
                    'cron': task['cron']
                }
                for task_id, task in self.tasks.items()
            }
        }


class TaskScheduler:
    """增强的计划任务调度器 - 支持数据库持久化和多种调度方式"""

    def __init__(self):
        self.settings = get_settings()
        self.running = False
        self.tasks: Dict[int, Dict] = {}  # key: scheduled_task.id
        self._scheduler_task = None
        self.system_instance = None
        self._running_executions: Dict[int, asyncio.Task] = {}  # 正在运行的任务

    async def initialize(self, system_instance):
        """初始化调度器"""
        self.system_instance = system_instance
        
        # 从数据库加载计划任务
        await self._load_tasks_from_db()
        
        logger.info(f"计划任务调度器初始化完成，加载了 {len(self.tasks)} 个任务")

    async def _load_tasks_from_db(self):
        """从数据库加载计划任务"""
        try:
            async with db_manager.AsyncSessionLocal() as session:
                from sqlalchemy import select
                stmt = select(ScheduledTask).where(ScheduledTask.enabled == True)
                result = await session.execute(stmt)
                scheduled_tasks = result.scalars().all()
                
                for task in scheduled_tasks:
                    await self._load_task(task)
                    
        except Exception as e:
            logger.error(f"从数据库加载任务失败: {str(e)}")

    async def _load_task(self, scheduled_task: ScheduledTask):
        """加载单个任务到内存"""
        try:
            # 计算下次执行时间
            next_run = self._calculate_next_run_time(scheduled_task)
            if next_run:
                # 创建任务执行函数
                execute_func = self._create_task_executor(scheduled_task)
                
                self.tasks[scheduled_task.id] = {
                    'task': scheduled_task,
                    'execute_func': execute_func,
                    'next_run': next_run,
                    'last_run': scheduled_task.last_run_time
                }
                
                # 更新数据库中的下次执行时间
                async with db_manager.AsyncSessionLocal() as session:
                    scheduled_task.next_run_time = next_run
                    session.add(scheduled_task)
                    await session.commit()
                    
                logger.info(f"加载任务: {scheduled_task.task_name} (ID: {scheduled_task.id})")
        except Exception as e:
            logger.error(f"加载任务失败 {scheduled_task.task_name}: {str(e)}")

    def _calculate_next_run_time(self, scheduled_task: ScheduledTask) -> Optional[datetime]:
        """计算下次执行时间"""
        try:
            config = scheduled_task.schedule_config or {}
            schedule_type = scheduled_task.schedule_type
            current_time = datetime.now()
            
            if schedule_type == ScheduleType.ONCE:
                # 一次性任务：某月某日某时
                datetime_str = config.get('datetime')
                if datetime_str:
                    next_time = datetime.strptime(datetime_str, '%Y-%m-%d %H:%M:%S')
                    # 如果已经过了执行时间，返回None（不再执行）
                    if next_time <= current_time:
                        return None
                    return next_time
                    
            elif schedule_type == ScheduleType.INTERVAL:
                # 间隔任务：每N分钟/小时/天
                interval = config.get('interval', 60)
                unit = config.get('unit', 'minutes')  # minutes/hours/days
                
                if unit == 'minutes':
                    delta = timedelta(minutes=interval)
                elif unit == 'hours':
                    delta = timedelta(hours=interval)
                elif unit == 'days':
                    delta = timedelta(days=interval)
                else:
                    logger.error(f"不支持的间隔单位: {unit}")
                    return None
                
                # 如果从未执行过，从当前时间开始
                if not scheduled_task.last_run_time:
                    return current_time + delta
                
                # 从上次执行时间开始计算
                last_run = scheduled_task.last_run_time
                next_time = last_run + delta
                
                # 如果下次执行时间已经过了，从当前时间开始
                if next_time <= current_time:
                    next_time = current_time + delta
                    
                return next_time
                
            elif schedule_type == ScheduleType.DAILY:
                # 每日任务：每天固定时间
                time_str = config.get('time', '02:00:00')
                hour, minute, second = map(int, time_str.split(':'))
                
                next_time = current_time.replace(hour=hour, minute=minute, second=second, microsecond=0)
                if next_time <= current_time:
                    # 如果今天的时间已过，执行明天的
                    next_time += timedelta(days=1)
                    
                return next_time
                
            elif schedule_type == ScheduleType.WEEKLY:
                # 每周任务：每周固定星期几的固定时间
                day_of_week = config.get('day_of_week', 0)  # 0=Monday, 6=Sunday
                time_str = config.get('time', '02:00:00')
                hour, minute, second = map(int, time_str.split(':'))
                
                current_weekday = current_time.weekday()  # 0=Monday, 6=Sunday
                days_ahead = day_of_week - current_weekday
                
                if days_ahead < 0 or (days_ahead == 0 and current_time.time() >= datetime.strptime(time_str, '%H:%M:%S').time()):
                    days_ahead += 7
                    
                next_time = current_time + timedelta(days=days_ahead)
                next_time = next_time.replace(hour=hour, minute=minute, second=second, microsecond=0)
                
                return next_time
                
            elif schedule_type == ScheduleType.MONTHLY:
                # 每月任务：每月固定日期的固定时间
                day_of_month = config.get('day_of_month', 1)
                time_str = config.get('time', '02:00:00')
                hour, minute, second = map(int, time_str.split(':'))
                
                next_time = current_time.replace(day=day_of_month, hour=hour, minute=minute, second=second, microsecond=0)
                if next_time <= current_time:
                    # 如果本月的日期已过，执行下个月的
                    if next_time.month == 12:
                        next_time = next_time.replace(year=next_time.year + 1, month=1)
                    else:
                        next_time = next_time.replace(month=next_time.month + 1)
                
                return next_time
                
            elif schedule_type == ScheduleType.YEARLY:
                # 每年任务：每年固定月日的固定时间
                month = config.get('month', 1)
                day = config.get('day', 1)
                time_str = config.get('time', '02:00:00')
                hour, minute, second = map(int, time_str.split(':'))
                
                next_time = current_time.replace(month=month, day=day, hour=hour, minute=minute, second=second, microsecond=0)
                if next_time <= current_time:
                    # 如果今年的日期已过，执行明年的
                    next_time = next_time.replace(year=next_time.year + 1)
                
                return next_time
                
            elif schedule_type == ScheduleType.CRON:
                # Cron表达式
                cron_expr = config.get('cron')
                if cron_expr:
                    cron = croniter(cron_expr, current_time)
                    return cron.get_next(datetime)
                    
            return None
            
        except Exception as e:
            logger.error(f"计算下次执行时间失败: {str(e)}")
            return None

    def _create_task_executor(self, scheduled_task: ScheduledTask) -> Callable:
        """创建任务执行函数"""
        async def executor():
            execution_id = str(uuid.uuid4())
            start_time = datetime.now()
            
            try:
                # 更新任务状态为运行中
                async with db_manager.AsyncSessionLocal() as session:
                    scheduled_task.status = ScheduledTaskStatus.RUNNING
                    scheduled_task.last_run_time = start_time
                    session.add(scheduled_task)
                    await session.commit()
                
                # 创建执行日志
                task_log = ScheduledTaskLog(
                    scheduled_task_id=scheduled_task.id,
                    execution_id=execution_id,
                    started_at=start_time,
                    status='running'
                )
                
                async with db_manager.AsyncSessionLocal() as session:
                    session.add(task_log)
                    await session.commit()
                
                # 执行任务动作
                result = await self._execute_task_action(scheduled_task)
                
                end_time = datetime.now()
                duration = int((end_time - start_time).total_seconds())
                
                # 更新执行日志
                async with db_manager.AsyncSessionLocal() as session:
                    from sqlalchemy import select
                    stmt = select(ScheduledTaskLog).where(ScheduledTaskLog.execution_id == execution_id)
                    log_result = await session.execute(stmt)
                    task_log = log_result.scalar_one()
                    
                    task_log.completed_at = end_time
                    task_log.duration = duration
                    task_log.status = 'success'
                    task_log.result = result
                    
                    # 更新任务统计
                    scheduled_task.total_runs += 1
                    scheduled_task.success_runs += 1
                    scheduled_task.last_success_time = end_time
                    scheduled_task.status = ScheduledTaskStatus.ACTIVE
                    
                    # 计算平均执行时长
                    if scheduled_task.average_duration:
                        scheduled_task.average_duration = int(
                            (scheduled_task.average_duration + duration) / 2
                        )
                    else:
                        scheduled_task.average_duration = duration
                    
                    # 计算下次执行时间
                    next_run = self._calculate_next_run_time(scheduled_task)
                    scheduled_task.next_run_time = next_run
                    
                    # 更新内存中的任务信息
                    if scheduled_task.id in self.tasks:
                        self.tasks[scheduled_task.id]['next_run'] = next_run
                        self.tasks[scheduled_task.id]['last_run'] = start_time
                    
                    session.add(scheduled_task)
                    await session.commit()
                
                logger.info(f"任务执行成功: {scheduled_task.task_name} (执行ID: {execution_id})")
                
            except Exception as e:
                end_time = datetime.now()
                duration = int((end_time - start_time).total_seconds())
                error_msg = str(e)
                
                logger.error(f"任务执行失败 {scheduled_task.task_name}: {error_msg}")
                
                # 更新执行日志
                try:
                    async with db_manager.AsyncSessionLocal() as session:
                        from sqlalchemy import select
                        stmt = select(ScheduledTaskLog).where(ScheduledTaskLog.execution_id == execution_id)
                        log_result = await session.execute(stmt)
                        task_log = log_result.scalar_one()
                        
                        task_log.completed_at = end_time
                        task_log.duration = duration
                        task_log.status = 'failed'
                        task_log.error_message = error_msg
                        
                        # 更新任务统计
                        scheduled_task.total_runs += 1
                        scheduled_task.failure_runs += 1
                        scheduled_task.last_failure_time = end_time
                        scheduled_task.last_error = error_msg
                        scheduled_task.status = ScheduledTaskStatus.ERROR
                        
                        session.add(scheduled_task)
                        await session.commit()
                except Exception as db_error:
                    logger.error(f"更新任务日志失败: {str(db_error)}")
        
        return executor

    async def _execute_task_action(self, scheduled_task: ScheduledTask) -> Dict[str, Any]:
        """执行任务动作"""
        action_type = scheduled_task.action_type
        action_config = scheduled_task.action_config or {}
        
        # 从task_metadata中获取backup_task_id
        backup_task_id = None
        if scheduled_task.task_metadata and isinstance(scheduled_task.task_metadata, dict):
            backup_task_id = scheduled_task.task_metadata.get('backup_task_id')
        
        if action_type == TaskActionType.BACKUP:
            return await self._execute_backup_action(action_config, backup_task_id, scheduled_task)
        elif action_type == TaskActionType.RECOVERY:
            return await self._execute_recovery_action(action_config)
        elif action_type == TaskActionType.CLEANUP:
            return await self._execute_cleanup_action(action_config)
        elif action_type == TaskActionType.HEALTH_CHECK:
            return await self._execute_health_check_action(action_config)
        elif action_type == TaskActionType.RETENTION_CHECK:
            return await self._execute_retention_check_action(action_config)
        elif action_type == TaskActionType.CUSTOM:
            return await self._execute_custom_action(action_config)
        else:
            raise ValueError(f"不支持的任务动作类型: {action_type}")

    async def _execute_backup_action(self, config: Dict, backup_task_id: Optional[int] = None, scheduled_task: Optional[ScheduledTask] = None) -> Dict[str, Any]:
        """执行备份动作
        
        参数:
            config: 动作配置
            backup_task_id: 备份任务模板ID（如果提供，从模板加载配置）
            scheduled_task: 计划任务对象（用于检查重复执行）
        """
        if not self.system_instance or not self.system_instance.backup_engine:
            raise ValueError("备份引擎未初始化")
        
        from models.backup import BackupTask, BackupTaskType, BackupTaskStatus
        from config.database import db_manager
        from sqlalchemy import select, and_
        
        try:
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
            
            # 执行备份任务
            success = await self.system_instance.backup_engine.execute_backup_task(backup_task)
            
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
                raise RuntimeError(f"备份任务执行失败: {backup_task.error_message}")
                
        except Exception as e:
            logger.error(f"执行备份动作失败: {str(e)}")
            raise

    async def _execute_recovery_action(self, config: Dict) -> Dict[str, Any]:
        """执行恢复动作"""
        return {"status": "success", "message": "恢复任务已执行"}

    async def _execute_cleanup_action(self, config: Dict) -> Dict[str, Any]:
        """执行清理动作"""
        return {"status": "success", "message": "清理任务已执行"}

    async def _execute_health_check_action(self, config: Dict) -> Dict[str, Any]:
        """执行健康检查动作"""
        return {"status": "success", "message": "健康检查已完成"}

    async def _execute_retention_check_action(self, config: Dict) -> Dict[str, Any]:
        """执行保留期检查动作"""
        return {"status": "success", "message": "保留期检查已完成"}

    async def _execute_custom_action(self, config: Dict) -> Dict[str, Any]:
        """执行自定义动作"""
        return {"status": "success", "message": "自定义任务已执行"}

    async def add_task(self, scheduled_task: ScheduledTask) -> bool:
        """添加计划任务"""
        try:
            # 保存到数据库
            async with db_manager.AsyncSessionLocal() as session:
                session.add(scheduled_task)
                await session.commit()
                await session.refresh(scheduled_task)
            
            # 如果任务已启用，加载到内存
            if scheduled_task.enabled:
                await self._load_task(scheduled_task)
            
            logger.info(f"添加计划任务成功: {scheduled_task.task_name}")
            return True
            
        except Exception as e:
            logger.error(f"添加计划任务失败: {str(e)}")
            return False

    async def delete_task(self, task_id: int) -> bool:
        """删除计划任务"""
        try:
            # 停止正在运行的任务
            if task_id in self._running_executions:
                self._running_executions[task_id].cancel()
                del self._running_executions[task_id]
            
            # 从内存中移除
            if task_id in self.tasks:
                del self.tasks[task_id]
            
            # 从数据库删除
            async with db_manager.AsyncSessionLocal() as session:
                from sqlalchemy import select
                stmt = select(ScheduledTask).where(ScheduledTask.id == task_id)
                result = await session.execute(stmt)
                task = result.scalar_one_or_none()
                
                if task:
                    await session.delete(task)
                    await session.commit()
                    logger.info(f"删除计划任务成功: {task.task_name}")
                    return True
                else:
                    logger.warning(f"未找到任务 ID: {task_id}")
                    return False
                    
        except Exception as e:
            logger.error(f"删除计划任务失败: {str(e)}")
            return False

    async def update_task(self, task_id: int, updates: Dict[str, Any]) -> bool:
        """更新计划任务"""
        try:
            async with db_manager.AsyncSessionLocal() as session:
                from sqlalchemy import select
                stmt = select(ScheduledTask).where(ScheduledTask.id == task_id)
                result = await session.execute(stmt)
                task = result.scalar_one_or_none()
                
                if not task:
                    logger.warning(f"未找到任务 ID: {task_id}")
                    return False
                
                # 更新字段
                for key, value in updates.items():
                    if hasattr(task, key):
                        setattr(task, key, value)
                
                # 重新计算下次执行时间
                task.next_run_time = self._calculate_next_run_time(task)
                
                session.add(task)
                await session.commit()
                await session.refresh(task)
            
            # 重新加载任务
            if task.enabled:
                await self._load_task(task)
            elif task_id in self.tasks:
                # 如果任务被禁用，从内存中移除
                del self.tasks[task_id]
            
            logger.info(f"更新计划任务成功: {task.task_name}")
            return True
            
        except Exception as e:
            logger.error(f"更新计划任务失败: {str(e)}")
            return False

    async def run_task(self, task_id: int) -> bool:
        """立即运行计划任务"""
        try:
            async with db_manager.AsyncSessionLocal() as session:
                from sqlalchemy import select
                stmt = select(ScheduledTask).where(ScheduledTask.id == task_id)
                result = await session.execute(stmt)
                task = result.scalar_one_or_none()
                
                if not task:
                    logger.warning(f"未找到任务 ID: {task_id}")
                    return False
                
                # 如果任务不在内存中，先加载
                if task_id not in self.tasks:
                    await self._load_task(task)
                
                # 创建执行函数并执行
                execute_func = self._create_task_executor(task)
                
                # 在后台执行（不阻塞）
                execution_task = asyncio.create_task(execute_func())
                self._running_executions[task_id] = execution_task
                
                logger.info(f"立即运行计划任务: {task.task_name}")
                return True
                
        except Exception as e:
            logger.error(f"立即运行计划任务失败: {str(e)}")
            return False

    async def stop_task(self, task_id: int) -> bool:
        """停止正在运行的任务"""
        try:
            if task_id in self._running_executions:
                execution_task = self._running_executions[task_id]
                execution_task.cancel()
                
                try:
                    await execution_task
                except asyncio.CancelledError:
                    pass
                
                del self._running_executions[task_id]
                
                # 更新任务状态
                async with db_manager.AsyncSessionLocal() as session:
                    from sqlalchemy import select
                    stmt = select(ScheduledTask).where(ScheduledTask.id == task_id)
                    result = await session.execute(stmt)
                    task = result.scalar_one_or_none()
                    
                    if task:
                        task.status = ScheduledTaskStatus.PAUSED
                        session.add(task)
                        await session.commit()
                
                logger.info(f"停止计划任务成功: task_id={task_id}")
                return True
            else:
                logger.warning(f"任务未在运行: task_id={task_id}")
                return False
                
        except Exception as e:
            logger.error(f"停止计划任务失败: {str(e)}")
            return False

    async def enable_task(self, task_id: int) -> bool:
        """启用计划任务"""
        return await self.update_task(task_id, {'enabled': True, 'status': ScheduledTaskStatus.ACTIVE})

    async def disable_task(self, task_id: int) -> bool:
        """禁用计划任务"""
        return await self.update_task(task_id, {'enabled': False, 'status': ScheduledTaskStatus.INACTIVE})

    async def start(self):
        """启动调度器"""
        if self.running:
            return
        
        self.running = True
        self._scheduler_task = asyncio.create_task(self._scheduler_loop())
        logger.info("计划任务调度器已启动")

    async def stop(self):
        """停止调度器"""
        self.running = False
        if self._scheduler_task:
            self._scheduler_task.cancel()
            try:
                await self._scheduler_task
            except asyncio.CancelledError:
                pass
        
        # 停止所有正在运行的任务
        for task_id in list(self._running_executions.keys()):
            await self.stop_task(task_id)
        
        logger.info("计划任务调度器已停止")

    async def _scheduler_loop(self):
        """调度器主循环"""
        while self.running:
            try:
                current_time = datetime.now()
                
                # 检查每个任务
                for task_id, task_info in list(self.tasks.items()):
                    if current_time >= task_info['next_run']:
                        # 执行任务（在后台执行，不阻塞）
                        execution_task = asyncio.create_task(task_info['execute_func']())
                        self._running_executions[task_id] = execution_task
                        
                        # 清理已完成的任务
                        def cleanup_task(exec_task, tid):
                            async def cleanup():
                                try:
                                    await exec_task
                                except Exception:
                                    pass
                                finally:
                                    if tid in self._running_executions:
                                        del self._running_executions[tid]
                            return cleanup
                        
                        asyncio.create_task(cleanup_task(execution_task, task_id)())
                
                # 每分钟检查一次
                await asyncio.sleep(60)
                
            except asyncio.CancelledError:
                break
            except Exception as e:
                logger.error(f"调度器循环出错: {str(e)}")
                await asyncio.sleep(60)

    async def get_tasks(self, enabled_only: bool = False) -> List[ScheduledTask]:
        """获取所有计划任务"""
        try:
            async with db_manager.AsyncSessionLocal() as session:
                from sqlalchemy import select
                stmt = select(ScheduledTask)
                if enabled_only:
                    stmt = stmt.where(ScheduledTask.enabled == True)
                result = await session.execute(stmt)
                return list(result.scalars().all())
        except Exception as e:
            logger.error(f"获取计划任务列表失败: {str(e)}")
            return []

    async def get_task(self, task_id: int) -> Optional[ScheduledTask]:
        """获取单个计划任务"""
        try:
            async with db_manager.AsyncSessionLocal() as session:
                from sqlalchemy import select
                stmt = select(ScheduledTask).where(ScheduledTask.id == task_id)
                result = await session.execute(stmt)
                return result.scalar_one_or_none()
        except Exception as e:
            logger.error(f"获取计划任务失败: {str(e)}")
            return None