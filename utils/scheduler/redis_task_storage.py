#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
计划任务存储 - Redis 实现
Scheduled Task Storage - Redis Implementation
"""

import logging
import json
from datetime import datetime
from typing import Dict, Any, Optional, List
from models.scheduled_task import ScheduledTask, ScheduledTaskStatus, ScheduledTaskLog, ScheduleType, TaskActionType
from config.redis_db import get_redis_client

logger = logging.getLogger(__name__)


# Redis键前缀
KEY_PREFIX_SCHEDULED_TASK = "scheduled_task"
KEY_PREFIX_SCHEDULED_TASK_LOG = "scheduled_task_log"
KEY_PREFIX_TASK_LOCK = "task_lock"
KEY_INDEX_SCHEDULED_TASKS = "scheduled_tasks:index"  # Set: 所有任务ID
KEY_INDEX_SCHEDULED_TASKS_ENABLED = "scheduled_tasks:enabled"  # Set: 启用的任务ID
KEY_INDEX_SCHEDULED_TASK_LOGS = "scheduled_task_logs:index"  # Set: 所有日志ID


def _get_redis_key(entity: str, identifier: Any) -> str:
    """生成Redis键"""
    return f"{entity}:{identifier}"


def _parse_datetime_value(value):
    """将字符串转换为 datetime 对象"""
    if not value:
        return None
    if isinstance(value, datetime):
        return value
    if isinstance(value, str):
        try:
            # 首先尝试 ISO 格式（支持微秒和时区）
            try:
                # 处理 Z 后缀
                iso_str = value.replace('Z', '+00:00') if value.endswith('Z') else value
                # 尝试直接解析 ISO 格式
                return datetime.fromisoformat(iso_str)
            except (ValueError, AttributeError):
                pass
            
            # 如果 ISO 格式失败，尝试其他格式
            for fmt in [
                "%Y-%m-%d %H:%M:%S.%f%z",
                "%Y-%m-%d %H:%M:%S.%f",
                "%Y-%m-%d %H:%M:%S%z",
                "%Y-%m-%d %H:%M:%S",
                "%Y-%m-%dT%H:%M:%S.%f%z",
                "%Y-%m-%dT%H:%M:%S%z",
                "%Y-%m-%dT%H:%M:%S",
            ]:
                try:
                    return datetime.strptime(value, fmt)
                except ValueError:
                    continue
            logger.warning(f"无法解析时间格式: {value}")
            return None
        except Exception as e:
            logger.warning(f"解析时间失败: {value}, 错误: {e}")
            return None
    return None


def _parse_enum(enum_class, value: str, default=None):
    """解析枚举值"""
    if not value:
        return default
    
    value_lower = value.lower().strip() if isinstance(value, str) else str(value).lower().strip()
    
    try:
        return enum_class(value_lower)
    except ValueError:
        for enum_value in enum_class:
            if enum_value.value.lower() == value_lower:
                return enum_value
        if default is not None:
            logger.warning(f"无法解析枚举值 '{value}' (类型: {enum_class.__name__})，使用默认值 {default}")
            return default
        else:
            raise ValueError(f"无法解析枚举值 '{value}' (类型: {enum_class.__name__})")


def _task_from_redis(data: Dict[str, Any]) -> ScheduledTask:
    """从Redis数据构建ScheduledTask对象"""
    task = ScheduledTask()
    task.id = int(data.get('id', 0))
    task.task_name = data.get('task_name', '')
    task.description = data.get('description') or ''
    task.schedule_type = _parse_enum(ScheduleType, data.get('schedule_type'), None)
    task.action_type = _parse_enum(TaskActionType, data.get('action_type'), None)
    task.status = _parse_enum(ScheduledTaskStatus, data.get('status'), ScheduledTaskStatus.INACTIVE)
    task.enabled = data.get('enabled', True) in (True, 'true', 'True', '1', 1)
    
    # 解析JSON字段
    schedule_config_str = data.get('schedule_config')
    if schedule_config_str:
        if isinstance(schedule_config_str, str):
            try:
                task.schedule_config = json.loads(schedule_config_str)
            except json.JSONDecodeError:
                task.schedule_config = {}
        else:
            task.schedule_config = schedule_config_str
    else:
        task.schedule_config = {}
    
    action_config_str = data.get('action_config')
    if action_config_str:
        if isinstance(action_config_str, str):
            try:
                task.action_config = json.loads(action_config_str)
            except json.JSONDecodeError:
                task.action_config = {}
        else:
            task.action_config = action_config_str
    else:
        task.action_config = {}
    
    task_metadata_str = data.get('task_metadata')
    if task_metadata_str:
        if isinstance(task_metadata_str, str):
            try:
                task.task_metadata = json.loads(task_metadata_str)
            except json.JSONDecodeError:
                task.task_metadata = {}
        else:
            task.task_metadata = task_metadata_str
    else:
        task.task_metadata = {}
    
    tags_str = data.get('tags')
    if tags_str:
        if isinstance(tags_str, str):
            try:
                task.tags = json.loads(tags_str)
            except json.JSONDecodeError:
                task.tags = []
        else:
            task.tags = tags_str
    else:
        task.tags = []
    
    # 时间字段
    task.next_run_time = _parse_datetime_value(data.get('next_run_time'))
    task.last_run_time = _parse_datetime_value(data.get('last_run_time'))
    task.last_success_time = _parse_datetime_value(data.get('last_success_time'))
    task.last_failure_time = _parse_datetime_value(data.get('last_failure_time'))
    
    # 统计信息
    task.total_runs = int(data.get('total_runs', 0) or 0)
    task.success_runs = int(data.get('success_runs', 0) or 0)
    task.failure_runs = int(data.get('failure_runs', 0) or 0)
    avg_duration = data.get('average_duration')
    task.average_duration = int(avg_duration) if avg_duration else None
    
    # 错误信息
    task.last_error = data.get('last_error') or ''
    
    # 其他字段
    task.backup_task_id = data.get('backup_task_id')
    
    # 解析时间字段，如果为 None 则使用当前时间作为默认值
    created_at = _parse_datetime_value(data.get('created_at'))
    updated_at = _parse_datetime_value(data.get('updated_at'))
    task.created_at = created_at if created_at else datetime.now()
    task.updated_at = updated_at if updated_at else datetime.now()
    
    return task


async def load_tasks_from_db_redis(enabled_only: bool = True) -> List[ScheduledTask]:
    """从Redis加载计划任务"""
    try:
        redis = await get_redis_client()
        
        # 获取任务ID集合
        if enabled_only:
            task_ids = await redis.smembers(KEY_INDEX_SCHEDULED_TASKS_ENABLED)
        else:
            task_ids = await redis.smembers(KEY_INDEX_SCHEDULED_TASKS)
        
        tasks = []
        for task_id_bytes in task_ids:
            task_id_str = task_id_bytes if isinstance(task_id_bytes, str) else (task_id_bytes.decode('utf-8') if isinstance(task_id_bytes, bytes) else str(task_id_bytes))
            task_id = int(task_id_str)
            task_key = _get_redis_key(KEY_PREFIX_SCHEDULED_TASK, task_id)
            
            # 获取任务数据
            task_data = await redis.hgetall(task_key)
            if not task_data:
                continue
            
            # 转换为字典（由于 Redis 连接设置了 decode_responses=True，键值已经是字符串）
            task_dict = {k if isinstance(k, str) else k.decode('utf-8'): 
                        v if isinstance(v, str) else (v.decode('utf-8') if isinstance(v, bytes) else str(v))
                        for k, v in task_data.items()}
            task_dict['id'] = task_id
            
            # 构建ScheduledTask对象
            task = _task_from_redis(task_dict)
            tasks.append(task)
        
        # 按ID排序
        tasks.sort(key=lambda x: x.id)
        
        return tasks
    except Exception as e:
        logger.error(f"[Redis模式] 从Redis加载任务失败: {str(e)}", exc_info=True)
        return []


async def record_run_start_redis(task_id: int, execution_id: str, started_at: datetime) -> None:
    """记录任务开始运行（Redis版本）"""
    try:
        redis = await get_redis_client()
        
        log_key = _get_redis_key(KEY_PREFIX_SCHEDULED_TASK_LOG, execution_id)
        log_data = {
            'scheduled_task_id': str(task_id),
            'execution_id': execution_id,
            'started_at': started_at.isoformat(),
            'status': 'running'
        }
        
        await redis.hset(log_key, mapping=log_data)
        await redis.sadd(KEY_INDEX_SCHEDULED_TASK_LOGS, execution_id)
    except Exception as e:
        logger.warning(f"[Redis模式] 记录任务开始失败（忽略继续）: {str(e)}")


async def record_run_end_redis(
    execution_id: str,
    completed_at: datetime,
    status: str,
    result: Optional[Dict[str, Any]] = None,
    error_message: Optional[str] = None
) -> None:
    """记录任务结束（Redis版本）"""
    try:
        redis = await get_redis_client()
        
        log_key = _get_redis_key(KEY_PREFIX_SCHEDULED_TASK_LOG, execution_id)
        
        updates = {
            'completed_at': completed_at.isoformat(),
            'status': status
        }
        
        if result:
            updates['result'] = json.dumps(result)
        if error_message:
            updates['error_message'] = error_message
        
        await redis.hset(log_key, mapping=updates)
    except Exception as e:
        logger.warning(f"[Redis模式] 记录任务结束失败（忽略继续）: {str(e)}")


async def acquire_task_lock_redis(task_id: int, execution_id: str) -> bool:
    """尝试获取任务锁（Redis版本）"""
    try:
        redis = await get_redis_client()
        
        lock_key = _get_redis_key(KEY_PREFIX_TASK_LOCK, task_id)
        
        # 检查锁是否存在且活跃
        lock_data = await redis.hgetall(lock_key)
        if lock_data:
            is_active = lock_data.get(b'is_active', b'0') == b'1'
            if is_active:
                logger.warning(f"[Redis模式] 任务 {task_id} 的锁已被占用")
                return False
            else:
                # 锁已失效，更新为活跃状态
                await redis.hset(lock_key, mapping={
                    'execution_id': execution_id,
                    'locked_at': datetime.now().isoformat(),
                    'is_active': '1'
                })
                logger.info(f"[Redis模式] 任务 {task_id} 的锁已重新激活")
                return True
        else:
            # 插入新锁记录
            await redis.hset(lock_key, mapping={
                'task_id': str(task_id),
                'execution_id': execution_id,
                'locked_at': datetime.now().isoformat(),
                'is_active': '1'
            })
            logger.info(f"[Redis模式] 任务 {task_id} 的新锁已创建")
            return True
    except Exception as e:
        logger.error(f"[Redis模式] 获取任务锁失败: {str(e)}", exc_info=True)
        # 关键修复：获取锁失败时应该返回 False，而不是 True
        return False


async def release_task_lock_redis(task_id: int, execution_id: str) -> None:
    """释放任务锁（Redis版本）"""
    try:
        redis = await get_redis_client()
        
        lock_key = _get_redis_key(KEY_PREFIX_TASK_LOCK, task_id)
        await redis.hset(lock_key, 'is_active', '0')
    except Exception as e:
        logger.warning(f"[Redis模式] 释放任务锁失败（忽略继续）: {str(e)}")


async def release_task_locks_by_task_redis(task_id: int) -> None:
    """释放指定任务的所有活跃锁（Redis版本）"""
    try:
        redis = await get_redis_client()
        
        lock_key = _get_redis_key(KEY_PREFIX_TASK_LOCK, task_id)
        await redis.hset(lock_key, 'is_active', '0')
        logger.info(f"[Redis模式] 已释放任务 {task_id} 的锁")
    except Exception as e:
        logger.warning(f"[Redis模式] 释放指定任务锁失败（忽略继续）: {str(e)}")


async def release_all_active_locks_redis() -> None:
    """释放所有活跃的任务锁（Redis版本）"""
    try:
        redis = await get_redis_client()
        
        # 查找所有锁键（通过模式匹配）
        lock_keys = []
        async for key in redis.scan_iter(match=f"{KEY_PREFIX_TASK_LOCK}:*"):
            lock_keys.append(key)
        
        # 批量设置为非活跃
        for lock_key in lock_keys:
            await redis.hset(lock_key, 'is_active', '0')
        
        logger.info(f"[Redis模式] 已释放所有活跃的任务锁，共 {len(lock_keys)} 个")
    except Exception as e:
        logger.warning(f"[Redis模式] 释放所有任务锁失败（忽略继续）: {str(e)}")


async def get_task_by_id_redis(task_id: int) -> Optional[ScheduledTask]:
    """根据ID获取计划任务（Redis版本）"""
    try:
        redis = await get_redis_client()
        
        task_key = _get_redis_key(KEY_PREFIX_SCHEDULED_TASK, task_id)
        task_data = await redis.hgetall(task_key)
        
        if not task_data:
            return None
        
        # 转换为字典（由于 Redis 连接设置了 decode_responses=True，键值已经是字符串）
        task_dict = {k if isinstance(k, str) else k.decode('utf-8'): 
                    v if isinstance(v, str) else (v.decode('utf-8') if isinstance(v, bytes) else str(v))
                    for k, v in task_data.items()}
        task_dict['id'] = task_id
        
        return _task_from_redis(task_dict)
    except Exception as e:
        logger.error(f"[Redis模式] 获取计划任务失败: {str(e)}", exc_info=True)
        return None


async def get_all_tasks_redis(enabled_only: bool = False) -> List[ScheduledTask]:
    """获取所有计划任务（Redis版本）"""
    return await load_tasks_from_db_redis(enabled_only)


async def add_task_redis(scheduled_task: ScheduledTask) -> bool:
    """添加计划任务（Redis版本）"""
    try:
        redis = await get_redis_client()
        
        # 获取下一个ID
        from config.redis_db import get_redis_manager
        redis_manager = get_redis_manager()
        if not redis_manager:
            logger.error("[Redis模式] Redis管理器未初始化")
            return False
        task_id = await redis_manager.get_next_id(KEY_PREFIX_SCHEDULED_TASK)
        
        scheduled_task.id = task_id
        task_key = _get_redis_key(KEY_PREFIX_SCHEDULED_TASK, task_id)
        
        # 准备数据
        task_data = {
            'id': str(task_id),
            'task_name': scheduled_task.task_name,
            'description': scheduled_task.description or '',
            'schedule_type': scheduled_task.schedule_type.value if scheduled_task.schedule_type else '',
            'action_type': scheduled_task.action_type.value if scheduled_task.action_type else '',
            'status': scheduled_task.status.value if scheduled_task.status else ScheduledTaskStatus.INACTIVE.value,
            'enabled': '1' if scheduled_task.enabled else '0',
            'schedule_config': json.dumps(scheduled_task.schedule_config) if scheduled_task.schedule_config else '{}',
            'action_config': json.dumps(scheduled_task.action_config) if scheduled_task.action_config else '{}',
            'task_metadata': json.dumps(scheduled_task.task_metadata) if scheduled_task.task_metadata else '{}',
            'tags': json.dumps(scheduled_task.tags) if scheduled_task.tags else '[]',
            'backup_task_id': str(scheduled_task.backup_task_id) if scheduled_task.backup_task_id else '',
            'total_runs': str(scheduled_task.total_runs or 0),
            'success_runs': str(scheduled_task.success_runs or 0),
            'failure_runs': str(scheduled_task.failure_runs or 0),
            'average_duration': str(scheduled_task.average_duration) if scheduled_task.average_duration else '',
            'last_error': scheduled_task.last_error or '',
        }
        
        # 时间字段
        if scheduled_task.next_run_time:
            task_data['next_run_time'] = scheduled_task.next_run_time.isoformat()
        if scheduled_task.last_run_time:
            task_data['last_run_time'] = scheduled_task.last_run_time.isoformat()
        if scheduled_task.last_success_time:
            task_data['last_success_time'] = scheduled_task.last_success_time.isoformat()
        if scheduled_task.last_failure_time:
            task_data['last_failure_time'] = scheduled_task.last_failure_time.isoformat()
        
        now_time = datetime.now()
        task_data['created_at'] = now_time.isoformat()
        task_data['updated_at'] = now_time.isoformat()
        
        # 保存到Redis
        await redis.hset(task_key, mapping=task_data)
        
        # 添加到索引
        await redis.sadd(KEY_INDEX_SCHEDULED_TASKS, str(task_id))
        if scheduled_task.enabled:
            await redis.sadd(KEY_INDEX_SCHEDULED_TASKS_ENABLED, str(task_id))
        
        logger.info(f"[Redis模式] 添加计划任务成功: {scheduled_task.task_name} (ID: {task_id})")
        
        # 记录操作日志
        from utils.log_utils import log_operation
        from models.system_log import OperationType
        await log_operation(
            operation_type=OperationType.SCHEDULER_CREATE,
            resource_type="scheduler",
            resource_id=str(task_id),
            resource_name=scheduled_task.task_name,
            operation_name="创建计划任务",
            operation_description=f"创建计划任务: {scheduled_task.task_name}",
            category="scheduler",
            success=True,
            result_message=f"计划任务创建成功 (ID: {task_id})",
            new_values={
                "task_name": scheduled_task.task_name,
                "description": scheduled_task.description,
                "schedule_type": scheduled_task.schedule_type.value if scheduled_task.schedule_type else None,
                "action_type": scheduled_task.action_type.value if scheduled_task.action_type else None,
                "enabled": scheduled_task.enabled
            }
        )
        
        return True
    except Exception as e:
        logger.error(f"[Redis模式] 添加计划任务失败: {str(e)}", exc_info=True)
        
        # 记录操作日志（失败）
        from utils.log_utils import log_operation
        from models.system_log import OperationType
        task_name = getattr(scheduled_task, 'task_name', '未知任务')
        await log_operation(
            operation_type=OperationType.SCHEDULER_CREATE,
            resource_type="scheduler",
            resource_name=task_name,
            operation_name="创建计划任务",
            operation_description=f"创建计划任务失败: {task_name}",
            category="scheduler",
            success=False,
            error_message=str(e)
        )
        
        return False


async def update_task_redis(task_id: int, updates: Dict[str, Any], next_run_time: Optional[datetime] = None) -> Optional[ScheduledTask]:
    """更新计划任务（Redis版本）"""
    try:
        redis = await get_redis_client()
        
        task_key = _get_redis_key(KEY_PREFIX_SCHEDULED_TASK, task_id)
        
        # 检查任务是否存在
        exists = await redis.exists(task_key)
        if not exists:
            logger.warning(f"[Redis模式] 未找到任务 ID: {task_id}")
            return None
        
        # 获取现有任务数据（由于 Redis 连接设置了 decode_responses=True，键值已经是字符串）
        task_data = await redis.hgetall(task_key)
        task_dict = {k if isinstance(k, str) else k.decode('utf-8'): 
                    v if isinstance(v, str) else (v.decode('utf-8') if isinstance(v, bytes) else str(v))
                    for k, v in task_data.items()}
        
        # 记录旧值（用于日志）
        old_values = {
            "task_name": task_dict.get('task_name', ''),
            "description": task_dict.get('description', ''),
            "schedule_type": task_dict.get('schedule_type', ''),
            "action_type": task_dict.get('action_type', ''),
            "enabled": task_dict.get('enabled', '0') == '1',
            "status": task_dict.get('status', '')
        }
        
        # 准备更新数据
        update_data = {}
        
        # 更新字段
        for key, value in updates.items():
            if key == 'schedule_config' or key == 'action_config' or key == 'task_metadata':
                update_data[key] = json.dumps(value) if value else '{}'
            elif key == 'tags':
                update_data[key] = json.dumps(value) if value else '[]'
            elif key == 'schedule_type' or key == 'action_type' or key == 'status':
                # 枚举值
                if hasattr(value, 'value'):
                    update_data[key] = value.value
                else:
                    update_data[key] = str(value)
            elif key in ('next_run_time', 'last_run_time', 'last_success_time', 'last_failure_time'):
                # 时间字段
                if value:
                    update_data[key] = value.isoformat() if isinstance(value, datetime) else str(value)
            elif key == 'enabled':
                update_data[key] = '1' if value else '0'
            else:
                update_data[key] = str(value)
        
        # 如果提供了next_run_time，使用它
        if next_run_time is not None:
            update_data['next_run_time'] = next_run_time.isoformat()
        
        # 更新时间戳
        update_data['updated_at'] = datetime.now().isoformat()
        
        # 更新Redis
        await redis.hset(task_key, mapping=update_data)
        
        # 更新启用索引
        if 'enabled' in updates:
            if updates['enabled']:
                await redis.sadd(KEY_INDEX_SCHEDULED_TASKS_ENABLED, str(task_id))
            else:
                await redis.srem(KEY_INDEX_SCHEDULED_TASKS_ENABLED, str(task_id))
        
        logger.info(f"[Redis模式] 更新计划任务成功: {task_dict.get('task_name')} (ID: {task_id})")
        
        # 记录操作日志
        from utils.log_utils import log_operation
        from models.system_log import OperationType
        
        # 获取新值（由于 Redis 连接设置了 decode_responses=True，键值已经是字符串）
        new_task_data = await redis.hgetall(task_key)
        new_task_dict = {k if isinstance(k, str) else k.decode('utf-8'): 
                        v if isinstance(v, str) else (v.decode('utf-8') if isinstance(v, bytes) else str(v))
                        for k, v in new_task_data.items()}
        
        await log_operation(
            operation_type=OperationType.SCHEDULER_UPDATE,
            resource_type="scheduler",
            resource_id=str(task_id),
            resource_name=new_task_dict.get('task_name', ''),
            operation_name="更新计划任务",
            operation_description=f"更新计划任务: {new_task_dict.get('task_name', '')}",
            category="scheduler",
            success=True,
            result_message=f"计划任务更新成功 (ID: {task_id})",
            old_values=old_values,
            new_values={
                "task_name": new_task_dict.get('task_name', ''),
                "description": new_task_dict.get('description', ''),
                "schedule_type": new_task_dict.get('schedule_type', ''),
                "action_type": new_task_dict.get('action_type', ''),
                "enabled": new_task_dict.get('enabled', '0') == '1',
                "status": new_task_dict.get('status', '')
            },
            changed_fields=list(updates.keys())
        )
        
        # 返回更新后的任务
        return await get_task_by_id_redis(task_id)
    except Exception as e:
        logger.error(f"[Redis模式] 更新计划任务失败: {str(e)}", exc_info=True)
        
        # 记录操作日志（失败）
        from utils.log_utils import log_operation
        from models.system_log import OperationType
        await log_operation(
            operation_type=OperationType.SCHEDULER_UPDATE,
            resource_type="scheduler",
            resource_id=str(task_id),
            operation_name="更新计划任务",
            operation_description=f"更新计划任务失败 (ID: {task_id})",
            category="scheduler",
            success=False,
            error_message=str(e)
        )
        
        return None


async def delete_task_redis(task_id: int) -> bool:
    """删除计划任务（Redis版本）"""
    try:
        redis = await get_redis_client()
        
        task_key = _get_redis_key(KEY_PREFIX_SCHEDULED_TASK, task_id)
        
        # 获取任务名称（用于日志）
        task_data = await redis.hgetall(task_key)
        if not task_data:
            logger.warning(f"[Redis模式] 未找到任务 ID: {task_id}")
            return False
        
        task_dict = {k if isinstance(k, str) else k.decode('utf-8'): 
                    v if isinstance(v, str) else (v.decode('utf-8') if isinstance(v, bytes) else str(v))
                    for k, v in task_data.items()}
        task_name = task_dict.get('task_name', '')
        
        # 记录操作日志（删除前）
        from utils.log_utils import log_operation
        from models.system_log import OperationType
        await log_operation(
            operation_type=OperationType.SCHEDULER_DELETE,
            resource_type="scheduler",
            resource_id=str(task_id),
            resource_name=task_name,
            operation_name="删除计划任务",
            operation_description=f"删除计划任务: {task_name}",
            category="scheduler",
            success=True,
            result_message=f"计划任务删除成功 (ID: {task_id})",
            old_values={
                "task_name": task_name,
                "task_id": task_id
            }
        )
        
        # 从Redis删除
        await redis.delete(task_key)
        
        # 从索引中移除
        await redis.srem(KEY_INDEX_SCHEDULED_TASKS, str(task_id))
        await redis.srem(KEY_INDEX_SCHEDULED_TASKS_ENABLED, str(task_id))
        
        logger.info(f"[Redis模式] 删除计划任务成功: {task_name} (ID: {task_id})")
        return True
    except Exception as e:
        logger.error(f"[Redis模式] 删除计划任务失败: {str(e)}", exc_info=True)
        
        # 记录操作日志（失败）
        from utils.log_utils import log_operation
        from models.system_log import OperationType
        await log_operation(
            operation_type=OperationType.SCHEDULER_DELETE,
            resource_type="scheduler",
            resource_id=str(task_id),
            operation_name="删除计划任务",
            operation_description=f"删除计划任务失败 (ID: {task_id})",
            category="scheduler",
            success=False,
            error_message=str(e)
        )
        
        return False

