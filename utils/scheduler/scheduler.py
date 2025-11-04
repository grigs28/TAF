#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
计划任务调度器
Task Scheduler Module
"""

import asyncio
import logging
from datetime import datetime, timedelta
from typing import Dict, Callable, Any, Optional, List
from croniter import croniter

from config.settings import get_settings
from models.backup import BackupTask
from models.tape import TapeCartridge
from models.scheduled_task import ScheduledTask, ScheduledTaskStatus
from config.database import db_manager

from .db_utils import is_opengauss, get_opengauss_connection
from .schedule_calculator import calculate_next_run_time
from .task_executor import create_task_executor
from .task_storage import (
    load_tasks_from_db, get_task_by_id, get_all_tasks,
    add_task as storage_add_task, delete_task as storage_delete_task,
    update_task as storage_update_task
)

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
            from config.database import db_manager
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
            scheduled_tasks = await load_tasks_from_db(enabled_only=True)
            
            for task in scheduled_tasks:
                await self._load_task(task)
                    
        except Exception as e:
            import traceback
            error_detail = traceback.format_exc()
            logger.error(f"从数据库加载任务失败: {str(e)}")
            logger.error(f"错误详情:\n{error_detail}")

    async def _load_task(self, scheduled_task: ScheduledTask):
        """加载单个任务到内存"""
        try:
            # 计算下次执行时间
            next_run = calculate_next_run_time(scheduled_task)
            if next_run:
                # 创建任务执行函数
                execute_func = create_task_executor(scheduled_task, self.system_instance)
                
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

    async def add_task(self, scheduled_task: ScheduledTask) -> bool:
        """添加计划任务"""
        try:
            # 计算下次执行时间
            next_run = calculate_next_run_time(scheduled_task)
            scheduled_task.next_run_time = next_run
            
            # 保存到数据库
            success = await storage_add_task(scheduled_task)
            
            if not success:
                return False
            
            # 如果任务已启用，加载到内存
            if scheduled_task.enabled:
                await self._load_task(scheduled_task)
            
            logger.info(f"添加计划任务成功: {scheduled_task.task_name} (ID: {scheduled_task.id})")
            return True
            
        except Exception as e:
            import traceback
            error_detail = traceback.format_exc()
            logger.error(f"添加计划任务失败: {str(e)}")
            logger.error(f"错误详情:\n{error_detail}")
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
            success = await storage_delete_task(task_id)
            
            if success:
                logger.info(f"删除计划任务成功: task_id={task_id}")
                return True
            else:
                logger.warning(f"未找到任务 ID: {task_id}")
                return False
                    
        except Exception as e:
            import traceback
            error_detail = traceback.format_exc()
            logger.error(f"删除计划任务失败: {str(e)}")
            logger.error(f"错误详情:\n{error_detail}")
            return False

    async def update_task(self, task_id: int, updates: Dict[str, Any]) -> bool:
        """更新计划任务"""
        try:
            # 获取任务
            task = await get_task_by_id(task_id)
            if not task:
                logger.warning(f"未找到任务 ID: {task_id}")
                return False
            
            # 更新字段
            for key, value in updates.items():
                if hasattr(task, key):
                    setattr(task, key, value)
            
            # 重新计算下次执行时间
            next_run = calculate_next_run_time(task)
            
            # 更新到数据库
            updated_task = await storage_update_task(task_id, updates, next_run_time=next_run)
            
            if not updated_task:
                return False
            
            # 重新加载任务
            if updated_task.enabled:
                await self._load_task(updated_task)
            elif task_id in self.tasks:
                # 如果任务被禁用，从内存中移除
                del self.tasks[task_id]
            
            logger.info(f"更新计划任务成功: {updated_task.task_name}")
            return True
            
        except Exception as e:
            import traceback
            error_detail = traceback.format_exc()
            logger.error(f"更新计划任务失败: {str(e)}")
            logger.error(f"错误详情:\n{error_detail}")
            return False

    async def run_task(self, task_id: int) -> bool:
        """立即运行计划任务"""
        try:
            task = await get_task_by_id(task_id)
            
            if not task:
                logger.warning(f"未找到任务 ID: {task_id}")
                return False
            
            # 如果任务不在内存中，先加载
            if task_id not in self.tasks:
                await self._load_task(task)
            
            # 创建执行函数并执行
            execute_func = create_task_executor(task, self.system_instance)
            
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
                await storage_update_task(task_id, {'status': ScheduledTaskStatus.PAUSED})
                
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
            return await get_all_tasks(enabled_only=enabled_only)
        except Exception as e:
            import traceback
            error_detail = traceback.format_exc()
            logger.error(f"获取计划任务列表失败: {str(e)}")
            logger.error(f"错误详情:\n{error_detail}")
            return []

    async def get_task(self, task_id: int) -> Optional[ScheduledTask]:
        """获取单个计划任务"""
        try:
            return await get_task_by_id(task_id)
        except Exception as e:
            import traceback
            error_detail = traceback.format_exc()
            logger.error(f"获取计划任务失败: {str(e)}")
            logger.error(f"错误详情:\n{error_detail}")
            return None

