#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
备份数据库操作 - Redis 实现（原生实现，不使用SQLAlchemy）
Backup Database Operations - Redis Implementation (Native, No SQLAlchemy)
"""

import logging
import json
import re
import time
from datetime import datetime, timedelta
from pathlib import Path
from typing import Optional, Dict, List, Any
from models.backup import (
    BackupSet,
    BackupSetStatus,
    BackupTaskType,
    BackupTask,
    BackupTaskStatus,
    BackupFile,
    BackupFileType,
)
from utils.datetime_utils import format_datetime, now
from config.redis_db import get_redis_client, get_redis_manager

logger = logging.getLogger(__name__)


# Redis键前缀
KEY_PREFIX_BACKUP_TASK = "backup_task"
KEY_PREFIX_BACKUP_SET = "backup_set"
KEY_PREFIX_BACKUP_FILE = "backup_file"
KEY_INDEX_BACKUP_TASKS = "backup_tasks:index"  # Set: 所有任务ID
KEY_INDEX_BACKUP_SETS = "backup_sets:index"  # Set: 所有备份集set_id
KEY_INDEX_BACKUP_FILES = "backup_files:index"  # Set: 所有文件ID
KEY_INDEX_BACKUP_SET_BY_SET_ID = "backup_sets:by_set_id"  # Hash: set_id -> id
KEY_INDEX_BACKUP_SET_BY_TASK_ID = "backup_sets:by_task_id"  # Set: task_id -> set_id[]
KEY_INDEX_BACKUP_FILE_BY_SET_ID = "backup_files:by_set_id"  # Set: backup_set_id -> file_id[]
KEY_INDEX_BACKUP_FILE_PENDING = "backup_files:pending"  # Sorted Set: backup_set_id -> {file_id: file_size} (未压缩文件，按大小排序)
KEY_INDEX_BACKUP_FILE_BY_PATH = "backup_files:by_path"  # Hash: backup_set_id -> {file_path: file_id}
KEY_COUNTER_BACKUP_SET = "backup_set:id"  # Counter for backup_set IDs
KEY_COUNTER_BACKUP_FILE = "backup_file:id"  # Counter for backup_file IDs


def _get_redis_key(entity: str, identifier: Any) -> str:
    """生成Redis键"""
    return f"{entity}:{identifier}"


def _parse_datetime_value(value):
    """将多种时间格式转换为 datetime 对象"""
    if not value:
        return None
    if isinstance(value, datetime):
        return value
    if isinstance(value, (int, float)):
        try:
            return datetime.fromtimestamp(value)
        except Exception:
            return None
    if isinstance(value, str):
        text = value.strip()
        if not text:
            return None
        try:
            if text.endswith("Z"):
                text = text.replace("Z", "+00:00")
            return datetime.fromisoformat(text)
        except ValueError:
            try:
                return datetime.fromtimestamp(float(text))
            except (ValueError, TypeError):
                return None
    return None


def _datetime_to_str(dt: Optional[datetime]) -> Optional[str]:
    """将datetime转换为ISO格式字符串"""
    if dt is None:
        return None
    if isinstance(dt, str):
        return dt
    return dt.isoformat()


def _str_to_datetime(s: Optional[str]) -> Optional[datetime]:
    """将ISO格式字符串转换为datetime"""
    if not s:
        return None
    return _parse_datetime_value(s)


def _ensure_dict(value) -> Dict:
    """确保值为字典"""
    if isinstance(value, dict):
        return value
    if isinstance(value, str):
        try:
            parsed = json.loads(value)
            return parsed if isinstance(parsed, dict) else {}
        except json.JSONDecodeError:
            return {}
    return {}


def _ensure_list(value) -> List:
    """确保值为列表"""
    if isinstance(value, list):
        return value
    if isinstance(value, str):
        try:
            parsed = json.loads(value)
            return parsed if isinstance(parsed, list) else []
        except json.JSONDecodeError:
            return []
    return []


def _normalize_file_type(processed_file: Dict) -> BackupFileType:
    """根据 processed_file 中的信息确定 BackupFileType"""
    file_type_value = processed_file.get("file_type")
    if isinstance(file_type_value, BackupFileType):
        return file_type_value
    if isinstance(file_type_value, str):
        try:
            return BackupFileType(file_type_value.lower())
        except ValueError:
            pass
    if processed_file.get("is_dir"):
        return BackupFileType.DIRECTORY
    if processed_file.get("is_symlink"):
        return BackupFileType.SYMLINK
    return BackupFileType.FILE


async def _get_next_id(redis, counter_key: str) -> int:
    """获取下一个ID（使用Redis INCR）"""
    return await redis.incr(counter_key)


async def create_backup_set_redis(backup_task: BackupTask, tape) -> BackupSet:
    """创建备份集（Redis版本）"""
    try:
        redis = await get_redis_client()
        redis_manager = get_redis_manager()
        
        # 生成备份集ID
        backup_group = format_datetime(now(), '%Y-%m')
        set_id = f"{backup_group}_{backup_task.id:06d}"
        backup_time = now()
        retention_until = backup_time + timedelta(days=backup_task.retention_days)
        
        # 获取下一个ID
        backup_set_id = await redis_manager.get_next_id('backup_set:id')
        
        # 准备数据
        task_type_value = backup_task.task_type.value if hasattr(backup_task.task_type, 'value') else str(backup_task.task_type)
        source_info = {'paths': backup_task.source_paths} if backup_task.source_paths else {}
        
        # 保存备份集数据
        backup_set_key = _get_redis_key(KEY_PREFIX_BACKUP_SET, backup_set_id)
        backup_set_data = {
            'id': str(backup_set_id),
            'set_id': set_id,
            'set_name': f"{backup_task.task_name}_{set_id}",
            'backup_group': backup_group,
            'status': BackupSetStatus.ACTIVE.value,
            'backup_task_id': str(backup_task.id),
            'tape_id': tape.tape_id if hasattr(tape, 'tape_id') else None,
            'backup_type': task_type_value,
            'backup_time': backup_time.isoformat(),
            'source_info': json.dumps(source_info) if source_info else '',
            'retention_until': retention_until.isoformat() if retention_until else '',
            'total_files': '0',
            'total_bytes': '0',
            'compressed_bytes': '0',
            'compression_ratio': '0.0',
            'chunk_count': '0',
            'created_at': backup_time.isoformat(),
            'updated_at': backup_time.isoformat(),
        }
        
        await redis.hset(backup_set_key, mapping=backup_set_data)
        
        # 添加到索引
        await redis.sadd(KEY_INDEX_BACKUP_SETS, set_id)
        await redis.hset(KEY_INDEX_BACKUP_SET_BY_SET_ID, set_id, str(backup_set_id))
        await redis.sadd(f"{KEY_INDEX_BACKUP_SET_BY_TASK_ID}:{backup_task.id}", set_id)
        
        # 创建 BackupSet 对象返回
        backup_set = BackupSet(
            id=backup_set_id,
            set_id=set_id,
            set_name=backup_set_data['set_name'],
            backup_group=backup_group,
            status=BackupSetStatus.ACTIVE,
            backup_task_id=backup_task.id,
            tape_id=tape.tape_id if hasattr(tape, 'tape_id') else None,
            backup_type=backup_task.task_type,
            backup_time=backup_time,
            source_info=source_info,
            retention_until=retention_until
        )
        
        logger.info(
            f"[Redis模式] 创建备份集成功: set_id={set_id}, "
            f"backup_set.id={backup_set.id}, "
            f"backup_set.set_id={backup_set.set_id}, "
            f"backup_task_id={backup_set.backup_task_id}"
        )
        
        return backup_set
        
    except Exception as e:
        logger.error(f"[Redis模式] 创建备份集失败: {str(e)}", exc_info=True)
        raise


async def get_backup_tasks_redis(
    status: Optional[str] = None,
    task_type: Optional[str] = None,
    q: Optional[str] = None,
    limit: int = 50,
    offset: int = 0
) -> List[Dict[str, Any]]:
    """获取备份任务列表（Redis版本）"""
    try:
        redis = await get_redis_client()
        
        # 状态规范化（在循环外部定义，避免循环未执行时变量未定义）
        normalized_status = (status or '').lower()
        
        # 获取所有任务ID（Redis 客户端设置了 decode_responses=True，所以返回值已经是字符串）
        task_ids_bytes = await redis.smembers(KEY_INDEX_BACKUP_TASKS)
        task_ids = [int(tid if isinstance(tid, str) else (tid.decode('utf-8') if isinstance(tid, bytes) else str(tid))) for tid in task_ids_bytes]
        
        tasks = []
        for task_id in task_ids:
            task_key = _get_redis_key(KEY_PREFIX_BACKUP_TASK, task_id)
            task_data = await redis.hgetall(task_key)
            
            if not task_data:
                continue
            
            # 转换为字典（Redis 客户端设置了 decode_responses=True，所以键值已经是字符串）
            task_dict = {k if isinstance(k, str) else k.decode('utf-8'): 
                        v if isinstance(v, str) else (v.decode('utf-8') if isinstance(v, bytes) else str(v))
                        for k, v in task_data.items()}
            
            # 检查 is_template（只返回非模板任务）
            is_template = task_dict.get('is_template', '0') == '1'
            if is_template:
                continue
            
            # 状态过滤
            task_status = task_dict.get('status', 'pending').lower()
            
            if status:
                if normalized_status in ('all',):
                    pass  # 不过滤
                elif normalized_status in ('not_run', '未运行'):
                    # 未运行：started_at为空的pending
                    started_at = task_dict.get('started_at')
                    if started_at or task_status != 'pending':
                        continue
                else:
                    # 精确匹配状态
                    if task_status != normalized_status:
                        continue
            
            # 任务类型过滤
            normalized_type = (task_type or '').lower()
            if task_type and normalized_type != 'all':
                task_type_val = task_dict.get('task_type', '').lower()
                if task_type_val != normalized_type:
                    continue
            
            # 搜索过滤
            if q and q.strip():
                task_name = task_dict.get('task_name', '')
                if q.strip().lower() not in task_name.lower():
                    continue
            
            # 解析JSON字段
            source_paths_str = task_dict.get('source_paths', '[]')
            source_paths = _ensure_list(source_paths_str)
            
            # 计算压缩率
            processed_bytes = float(task_dict.get('processed_bytes', 0) or 0)
            compressed_bytes = float(task_dict.get('compressed_bytes', 0) or 0)
            compression_ratio = 0.0
            if processed_bytes > 0 and compressed_bytes > 0:
                compression_ratio = compressed_bytes / processed_bytes
            
            # 解析result_summary
            result_summary_str = task_dict.get('result_summary', '{}')
            result_summary = _ensure_dict(result_summary_str)
            estimated_archive_count = result_summary.get('estimated_archive_count')
            total_bytes_actual = (
                result_summary.get('total_scanned_bytes') or
                result_summary.get('total_bytes_actual') or
                0
            )
            
            # 构建stage_info
            description = task_dict.get('description', '')
            scan_status = task_dict.get('scan_status')
            
            # 简化版_build_stage_info
            stage_info = {
                "operation_status": task_status,
                "operation_stage": "scan" if task_status == "scanning" else "pending",
                "operation_stage_label": description or "待开始",
                "stage_steps": []
            }
            
            # 构建任务响应
            tasks.append({
                "task_id": task_id,
                "task_name": task_dict.get('task_name', ''),
                "task_type": task_dict.get('task_type', 'full'),
                "status": task_status,
                "progress_percent": float(task_dict.get('progress_percent', 0) or 0),
                "total_files": int(task_dict.get('total_files', 0) or 0),
                "processed_files": int(task_dict.get('processed_files', 0) or 0),
                "total_bytes": int(task_dict.get('total_bytes', 0) or 0),
                "total_bytes_actual": int(total_bytes_actual) if total_bytes_actual else 0,
                "processed_bytes": int(processed_bytes),
                "compressed_bytes": int(compressed_bytes),
                "compression_ratio": compression_ratio,
                "estimated_archive_count": estimated_archive_count,
                "created_at": _parse_datetime_value(task_dict.get('created_at')),
                "started_at": _parse_datetime_value(task_dict.get('started_at')),
                "completed_at": _parse_datetime_value(task_dict.get('completed_at')),
                "error_message": task_dict.get('error_message'),
                "is_template": False,
                "tape_device": task_dict.get('tape_device'),
                "source_paths": source_paths,
                "description": description,
                "from_scheduler": False,
                "operation_status": stage_info["operation_status"],
                "operation_stage": stage_info["operation_stage"],
                "operation_stage_label": stage_info["operation_stage_label"],
                "stage_steps": stage_info["stage_steps"]
            })
        
        # 追加计划任务（未运行模板）
        include_sched = (not status) or (normalized_status in ("all", "pending", 'not_run', '未运行'))
        if include_sched:
            # 获取计划任务
            from utils.scheduler.redis_task_storage import KEY_INDEX_SCHEDULED_TASKS, KEY_PREFIX_SCHEDULED_TASK
            sched_ids_bytes = await redis.smembers(KEY_INDEX_SCHEDULED_TASKS)
            
            for sched_id_bytes in sched_ids_bytes:
                # 转换为字符串（由于 Redis 连接设置了 decode_responses=True，键值已经是字符串）
                sched_id_str = sched_id_bytes if isinstance(sched_id_bytes, str) else (sched_id_bytes.decode('utf-8') if isinstance(sched_id_bytes, bytes) else str(sched_id_bytes))
                sched_id = int(sched_id_str)
                sched_key = f"{KEY_PREFIX_SCHEDULED_TASK}:{sched_id}"
                sched_data = await redis.hgetall(sched_key)
                
                if not sched_data:
                    continue
                
                # 转换为字典（由于 Redis 连接设置了 decode_responses=True，键值已经是字符串）
                sched_dict = {k if isinstance(k, str) else k.decode('utf-8'): 
                             v if isinstance(v, str) else (v.decode('utf-8') if isinstance(v, bytes) else str(v))
                             for k, v in sched_data.items()}
                
                # 检查action_type是否为BACKUP
                action_type = sched_dict.get('action_type', '').lower()
                if action_type != 'backup':
                    continue
                
                # 检查enabled
                enabled = sched_dict.get('enabled', '1') == '1'
                if not enabled:
                    continue
                
                # 搜索过滤
                if q and q.strip():
                    task_name = sched_dict.get('task_name', '')
                    if q.strip().lower() not in task_name.lower():
                        continue
                
                # 解析action_config
                action_config_str = sched_dict.get('action_config', '{}')
                action_config = _ensure_dict(action_config_str)
                
                # 从action_config中提取信息
                atype = action_config.get('task_type', 'full')
                tdev = action_config.get('tape_device')
                spaths = _ensure_list(action_config.get('source_paths', []))
                
                # 任务类型过滤
                if task_type and normalized_type != 'all':
                    if atype.lower() != normalized_type:
                        continue
                
                # 构建stage_info
                stage_info = {
                    "operation_status": "pending",
                    "operation_stage": "pending",
                    "operation_stage_label": "待开始",
                    "stage_steps": []
                }
                
                tasks.append({
                    "task_id": sched_id,
                    "task_name": sched_dict.get('task_name', ''),
                    "task_type": atype,
                    "status": "pending",
                    "progress_percent": 0.0,
                    "total_files": 0,
                    "processed_files": 0,
                    "total_bytes": 0,
                    "total_bytes_actual": 0,
                    "processed_bytes": 0,
                    "compressed_bytes": 0,
                    "compression_ratio": 0.0,
                    "created_at": _parse_datetime_value(sched_dict.get('created_at')),
                    "started_at": None,
                    "completed_at": None,
                    "error_message": None,
                    "is_template": True,
                    "tape_device": tdev,
                    "source_paths": spaths or [],
                    "from_scheduler": True,
                    "enabled": enabled,
                    "description": "",
                    "estimated_archive_count": None,
                    "operation_status": stage_info["operation_status"],
                    "operation_stage": stage_info["operation_stage"],
                    "operation_stage_label": stage_info["operation_stage_label"],
                    "stage_steps": stage_info["stage_steps"]
                })
        
        # 按created_at排序（降序）
        def _ts(val):
            try:
                if not val:
                    return 0.0
                if isinstance(val, datetime):
                    return val.timestamp()
                if isinstance(val, (int, float)):
                    return float(val)
                return 0.0
            except Exception:
                return 0.0
        
        tasks.sort(key=lambda x: _ts(x.get('created_at')), reverse=True)
        
        # 分页
        return tasks[offset:offset+limit]
        
    except Exception as e:
        logger.error(f"[Redis模式] 获取备份任务列表失败: {str(e)}", exc_info=True)
        return []


async def get_task_status_redis(task_id: int) -> Optional[Dict]:
    """获取任务状态（Redis版本）
    
    Args:
        task_id: 任务ID
        
    Returns:
        任务状态字典，如果不存在则返回None
    """
    try:
        redis = await get_redis_client()
        task_key = _get_redis_key(KEY_PREFIX_BACKUP_TASK, task_id)
        
        # 检查任务是否存在
        exists = await redis.exists(task_key)
        if not exists:
            logger.warning(f"[Redis模式] 找不到备份任务: {task_id}")
            return None
        
        # 获取任务数据
        task_data = await redis.hgetall(task_key)
        if not task_data:
            logger.warning(f"[Redis模式] 无法获取备份任务数据: {task_id}")
            return None
        
        # 转换为字典
        task_dict = {k if isinstance(k, str) else k.decode('utf-8'): 
                    v if isinstance(v, str) else (v.decode('utf-8') if isinstance(v, bytes) else str(v))
                    for k, v in task_data.items()}
        
        # 解析JSON字段
        source_paths = None
        if task_dict.get('source_paths'):
            try:
                source_paths = _ensure_list(task_dict.get('source_paths'))
            except Exception:
                source_paths = None
        
        # 计算压缩率
        compression_ratio = 0.0
        processed_bytes = float(task_dict.get('processed_bytes', 0) or 0)
        compressed_bytes = float(task_dict.get('compressed_bytes', 0) or 0)
        if processed_bytes > 0 and compressed_bytes > 0:
            compression_ratio = float(compressed_bytes) / float(processed_bytes)
        
        # 解析result_summary获取预计的压缩包总数
        estimated_archive_count = None
        total_scanned_bytes = None
        result_summary_str = task_dict.get('result_summary', '{}')
        try:
            result_summary = _ensure_dict(result_summary_str)
            if isinstance(result_summary, dict):
                estimated_archive_count = result_summary.get('estimated_archive_count')
                total_scanned_bytes = result_summary.get('total_scanned_bytes')
        except Exception:
            pass
        
        return {
            'task_id': task_id,
            'status': task_dict.get('status', 'pending'),
            'progress_percent': float(task_dict.get('progress_percent', 0) or 0),
            'processed_files': int(task_dict.get('processed_files', 0) or 0),
            'total_files': int(task_dict.get('total_files', 0) or 0),
            'total_bytes': int(task_dict.get('total_bytes', 0) or 0),
            'processed_bytes': int(processed_bytes),
            'compressed_bytes': int(compressed_bytes),
            'compression_ratio': compression_ratio,
            'estimated_archive_count': estimated_archive_count,
            'description': task_dict.get('description', ''),
            'source_paths': source_paths or [],
            'tape_device': task_dict.get('tape_device'),
            'tape_id': task_dict.get('tape_id'),
            'started_at': _parse_datetime_value(task_dict.get('started_at')),
            'completed_at': _parse_datetime_value(task_dict.get('completed_at'))
        }
        
    except Exception as e:
        logger.error(f"[Redis模式] 获取任务状态失败: {str(e)}", exc_info=True)
        return None


async def update_task_status_redis(backup_task: BackupTask, status: BackupTaskStatus):
    """更新任务状态（Redis版本）"""
    try:
        redis = await get_redis_client()
        task_key = _get_redis_key(KEY_PREFIX_BACKUP_TASK, backup_task.id)
        
        # 检查任务是否存在
        exists = await redis.exists(task_key)
        if not exists:
            logger.warning(f"[Redis模式] 找不到备份任务: {backup_task.id}")
            return
        
        current_time = now()
        update_mapping = {
            'status': status.value if hasattr(status, 'value') else str(status),
            'updated_at': current_time.isoformat()
        }
        
        if status == BackupTaskStatus.RUNNING:
            update_mapping['started_at'] = current_time.isoformat()
            if hasattr(backup_task, 'source_paths') and backup_task.source_paths:
                update_mapping['source_paths'] = json.dumps(backup_task.source_paths) if isinstance(backup_task.source_paths, list) else backup_task.source_paths
            if hasattr(backup_task, 'tape_id') and backup_task.tape_id:
                update_mapping['tape_id'] = backup_task.tape_id
        elif status in (BackupTaskStatus.COMPLETED, BackupTaskStatus.FAILED, BackupTaskStatus.CANCELLED):
            update_mapping['completed_at'] = current_time.isoformat()
        
        await redis.hset(task_key, mapping=update_mapping)
        logger.info(f"[Redis模式] 更新任务状态成功: task_id={backup_task.id}, status={status.value if hasattr(status, 'value') else status}")
    except Exception as e:
        logger.error(f"[Redis模式] 更新任务状态失败: {str(e)}", exc_info=True)


async def update_task_fields_redis(backup_task: BackupTask, **fields):
    """更新任务的特定字段（Redis版本）"""
    try:
        if not fields:
            return
        
        redis = await get_redis_client()
        task_key = _get_redis_key(KEY_PREFIX_BACKUP_TASK, backup_task.id)
        
        # 检查任务是否存在
        exists = await redis.exists(task_key)
        if not exists:
            logger.warning(f"[Redis模式] 找不到备份任务: {backup_task.id}")
            return
        
        # 更新对象属性
        for field, value in fields.items():
            if hasattr(backup_task, field):
                setattr(backup_task, field, value)
        
        # 准备更新映射
        update_mapping = {
            'updated_at': now().isoformat()
        }
        
        for field, value in fields.items():
            # 处理 JSON 字段
            if field in ('source_paths', 'exclude_patterns', 'result_summary'):
                update_mapping[field] = json.dumps(value) if value else ''
            # 处理枚举类型
            elif hasattr(value, 'value'):
                update_mapping[field] = value.value
            # 处理日期时间
            elif isinstance(value, datetime):
                update_mapping[field] = value.isoformat()
            else:
                update_mapping[field] = str(value) if value is not None else ''
        
        await redis.hset(task_key, mapping=update_mapping)
        logger.debug(f"[Redis模式] 更新任务字段成功: task_id={backup_task.id}, fields={list(fields.keys())}")
    except Exception as e:
        logger.error(f"[Redis模式] 更新任务字段失败: {str(e)}", exc_info=True)


async def finalize_backup_set_redis(backup_set: BackupSet, file_count: int, total_size: int):
    """完成备份集（Redis版本）
    
    Args:
        backup_set: 备份集对象
        file_count: 文件总数
        total_size: 总字节数
    """
    try:
        redis = await get_redis_client()
        
        # 通过set_id获取backup_set_id（因为key是用backup_set_id构建的）
        backup_set_id = backup_set.id if backup_set.id else None
        
        # 如果没有id，尝试通过set_id查找
        if not backup_set_id:
            backup_set_id_str = await redis.hget(KEY_INDEX_BACKUP_SET_BY_SET_ID, backup_set.set_id)
            if backup_set_id_str:
                backup_set_id = int(backup_set_id_str)
            else:
                logger.warning(f"[Redis模式] 找不到备份集的ID: set_id={backup_set.set_id}，无法更新")
                return
        else:
            backup_set_id = int(backup_set_id) if isinstance(backup_set_id, str) else backup_set_id
        
        # 使用backup_set_id构建key（与创建时保持一致）
        backup_set_key = _get_redis_key(KEY_PREFIX_BACKUP_SET, backup_set_id)
        
        # 检查备份集是否存在
        exists = await redis.exists(backup_set_key)
        if not exists:
            logger.warning(f"[Redis模式] 找不到备份集: backup_set_id={backup_set_id}, set_id={backup_set.set_id}，无法更新")
            return
        
        # 计算压缩比率
        compression_ratio = total_size / backup_set.total_bytes if backup_set.total_bytes > 0 else 1.0
        
        # 更新备份集信息
        current_time = now()
        update_mapping = {
            'total_files': str(file_count),
            'total_bytes': str(total_size),
            'compressed_bytes': str(total_size),
            'compression_ratio': str(compression_ratio),
            'chunk_count': str(backup_set.chunk_count or 1),
            'updated_at': current_time.isoformat()
        }
        
        await redis.hset(backup_set_key, mapping=update_mapping)
        
        # 更新备份集对象属性
        backup_set.total_files = file_count
        backup_set.total_bytes = total_size
        backup_set.compressed_bytes = total_size
        backup_set.compression_ratio = compression_ratio
        backup_set.chunk_count = backup_set.chunk_count or 1
        
        logger.info(f"[Redis模式] 备份集完成: {backup_set.set_id}, 文件数={file_count}, 总大小={format_bytes(total_size)}")
        
    except Exception as e:
        logger.error(f"[Redis模式] 完成备份集失败: {str(e)}", exc_info=True)
        raise


async def delete_backup_task_redis(task_id: int) -> bool:
    """删除备份任务（Redis版本）
    
    Args:
        task_id: 任务ID
        
    Returns:
        bool: 如果删除成功返回True，否则返回False
    """
    try:
        redis = await get_redis_client()
        task_key = _get_redis_key(KEY_PREFIX_BACKUP_TASK, task_id)
        
        # 检查任务是否存在
        exists = await redis.exists(task_key)
        if not exists:
            logger.warning(f"[Redis模式] 备份任务不存在: {task_id}")
            return False
        
        # 获取任务信息（decode_responses=True，所以返回的是字符串字典）
        task_data = await redis.hgetall(task_key)
        if not task_data:
            logger.warning(f"[Redis模式] 无法获取备份任务数据: {task_id}")
            return False
        
        is_template = task_data.get('is_template', '0') == '1'
        
        # 1. 删除关联的备份集和文件
        # 获取所有关联的备份集set_id（使用SSCAN迭代，避免超时）
        set_ids = []
        set_index_key = f"{KEY_INDEX_BACKUP_SET_BY_TASK_ID}:{task_id}"
        cursor = 0
        while True:
            cursor, batch = await redis.sscan(set_index_key, cursor=cursor, count=1000)
            set_ids.extend(batch)
            if cursor == 0:
                break
        
        total_files_deleted = 0
        import time
        import asyncio
        delete_start_time = time.time()
        
        # 优化：并行删除多个备份集的文件（大幅提升性能）
        async def delete_backup_set_files(set_db_id: int, set_id: str) -> int:
            """删除单个备份集的所有文件，返回删除的文件数"""
            files_deleted = 0
            file_index_key = f"{KEY_INDEX_BACKUP_FILE_BY_SET_ID}:{set_db_id}"
            path_index_key = f"{KEY_INDEX_BACKUP_FILE_BY_PATH}:{set_db_id}"
            
            cursor = 0
            batch_delete_size = 10000  # 每批删除10000个文件（进一步增大）
            
            while True:
                # 使用SSCAN获取一批文件ID
                cursor, file_ids_batch = await redis.sscan(file_index_key, cursor=cursor, count=batch_delete_size)
                
                if not file_ids_batch:
                    if cursor == 0:
                        break
                    continue
                
                # 批量删除文件（优化：不需要获取路径，最后直接删除整个路径索引Hash）
                pipe = redis.pipeline()
                
                # 转换为字符串列表（确保Redis可以正确处理）
                file_ids_str = []
                for fid in file_ids_batch:
                    if isinstance(fid, bytes):
                        file_ids_str.append(fid.decode('utf-8') if hasattr(fid, 'decode') else str(fid))
                    else:
                        file_ids_str.append(str(fid))
                
                # 批量删除文件Hash
                for file_id in file_ids_str:
                    file_key = _get_redis_key(KEY_PREFIX_BACKUP_FILE, file_id)
                    pipe.delete(file_key)
                
                # 批量删除索引（使用SREM一次性删除多个成员）
                if file_ids_str:
                    pipe.srem(KEY_INDEX_BACKUP_FILES, *file_ids_str)
                    pipe.srem(file_index_key, *file_ids_str)
                
                # 执行批量删除
                await pipe.execute()
                files_deleted += len(file_ids_batch)
                
                if cursor == 0:
                    break
            
            # 删除备份集（使用pipeline合并操作）
            set_key = _get_redis_key(KEY_PREFIX_BACKUP_SET, set_db_id)
            pipe = redis.pipeline()
            pipe.delete(set_key)
            pipe.srem(KEY_INDEX_BACKUP_SETS, set_id)
            set_index_key = f"{KEY_INDEX_BACKUP_SET_BY_TASK_ID}:{task_id}"
            pipe.srem(set_index_key, set_id)
            pipe.hdel(KEY_INDEX_BACKUP_SET_BY_SET_ID, set_id)
            # 删除路径索引Hash（整个备份集的路径索引，不需要逐个删除）
            pipe.delete(path_index_key)
            # 阶段1优化：删除未压缩文件索引（Sorted Set）
            pending_index_key = f"{KEY_INDEX_BACKUP_FILE_PENDING}:{set_db_id}"
            pipe.delete(pending_index_key)
            await pipe.execute()
            
            return files_deleted
        
        # 先收集所有备份集ID
        backup_set_tasks = []
        for set_id in set_ids:
            set_db_id = await redis.hget(KEY_INDEX_BACKUP_SET_BY_SET_ID, set_id)
            if set_db_id:
                try:
                    set_db_id_int = int(set_db_id)
                    backup_set_tasks.append(delete_backup_set_files(set_db_id_int, set_id))
                except (ValueError, TypeError):
                    logger.warning(f"[Redis模式] 无效的备份集ID: {set_db_id}")
                    continue
        
        # 并行删除所有备份集的文件（大幅提升性能）
        if backup_set_tasks:
            files_deleted_results = await asyncio.gather(*backup_set_tasks, return_exceptions=True)
            for result in files_deleted_results:
                if isinstance(result, Exception):
                    logger.error(f"[Redis模式] 删除备份集文件失败: {str(result)}", exc_info=True)
                else:
                    total_files_deleted += result
        
        delete_total_time = time.time() - delete_start_time
        delete_avg_speed = total_files_deleted / delete_total_time if delete_total_time > 0 else 0
        
        if total_files_deleted > 0:
            logger.info(
                f"[Redis模式] ✅ 已删除 {total_files_deleted} 个备份文件记录，"
                f"耗时={delete_total_time:.2f}秒，平均速度={delete_avg_speed:.0f} 个/秒"
            )
        if set_ids:
            logger.info(f"[Redis模式] ✅ 已删除 {len(set_ids)} 个关联的备份集")
        
        # 2. 如果是模板，删除关联的执行记录
        if is_template:
            # 优化：使用批量查询，减少HGETALL调用
            child_task_ids = []
            cursor = 0
            batch_check_size = 2000  # 每批检查2000个任务
            
            while True:
                cursor, task_ids_batch = await redis.sscan(KEY_INDEX_BACKUP_TASKS, cursor=cursor, count=batch_check_size)
                
                if not task_ids_batch:
                    if cursor == 0:
                        break
                    continue
                
                # 批量获取任务的template_id字段（只获取需要的字段，而不是整个Hash）
                pipe = redis.pipeline()
                valid_task_ids = []
                
                for tid_str in task_ids_batch:
                    try:
                        tid = int(tid_str)
                        if tid == task_id:
                            continue  # 跳过自己
                        
                        valid_task_ids.append(tid)
                        child_task_key = _get_redis_key(KEY_PREFIX_BACKUP_TASK, tid)
                        pipe.hget(child_task_key, 'template_id')
                    except (ValueError, TypeError):
                        continue  # 跳过无效的任务ID
                
                # 执行批量查询
                if valid_task_ids:
                    template_ids_result = await pipe.execute()
                    
                    # 检查哪些任务的template_id匹配
                    for tid, template_id in zip(valid_task_ids, template_ids_result):
                        if template_id and template_id == str(task_id):
                            child_task_ids.append(tid)
                
                if cursor == 0:
                    break
            
            # 递归删除子任务
            if child_task_ids:
                logger.info(f"[Redis模式] 找到 {len(child_task_ids)} 个子任务，开始递归删除...")
                for idx, child_task_id in enumerate(child_task_ids, 1):
                    logger.debug(f"[Redis模式] 删除子任务 {idx}/{len(child_task_ids)}: task_id={child_task_id}")
                    await delete_backup_task_redis(child_task_id)
                logger.info(f"[Redis模式] 已删除 {len(child_task_ids)} 个关联的执行记录")
        
        # 3. 删除任务本身
        await redis.delete(task_key)
        await redis.srem(KEY_INDEX_BACKUP_TASKS, str(task_id))
        
        logger.info(f"[Redis模式] 删除备份任务成功: task_id={task_id}")
        return True
        
    except Exception as e:
        logger.error(f"[Redis模式] 删除备份任务失败: {str(e)}", exc_info=True)
        return False


async def update_scan_status_redis(backup_task_id: int, status: str):
    """更新扫描状态（Redis版本）
    
    Args:
        backup_task_id: 备份任务ID
        status: 扫描状态（如 'running', 'completed'）
    """
    try:
        redis = await get_redis_client()
        task_key = _get_redis_key(KEY_PREFIX_BACKUP_TASK, backup_task_id)
        
        # 检查任务是否存在
        exists = await redis.exists(task_key)
        if not exists:
            logger.warning(f"[Redis模式] 找不到备份任务: {backup_task_id}")
            return
        
        current_time = now()
        update_mapping = {
            'scan_status': status,
            'updated_at': current_time.isoformat()
        }
        
        if status == 'completed':
            update_mapping['scan_completed_at'] = current_time.isoformat()
        
        await redis.hset(task_key, mapping=update_mapping)
        logger.info(f"[Redis模式] 更新扫描状态成功: task_id={backup_task_id}, status={status}")
    except Exception as e:
        logger.error(f"[Redis模式] 更新扫描状态失败: {str(e)}", exc_info=True)


async def update_task_stage_async_redis(backup_task: BackupTask, stage_code: str, description: str = None):
    """更新任务的操作阶段（Redis版本）
    
    Args:
        backup_task: 备份任务对象
        stage_code: 阶段代码（scan/compress/copy/finalize）
        description: 可选的阶段描述，如果提供则同时更新description字段
    """
    try:
        if not backup_task or not getattr(backup_task, 'id', None):
            logger.warning(f"[Redis模式] 无效的任务对象，无法更新阶段")
            return
        
        redis = await get_redis_client()
        task_key = _get_redis_key(KEY_PREFIX_BACKUP_TASK, backup_task.id)
        
        # 检查任务是否存在
        exists = await redis.exists(task_key)
        if not exists:
            logger.warning(f"[Redis模式] 找不到备份任务: {backup_task.id}")
            return
        
        current_time = now()
        update_mapping = {
            'operation_stage': stage_code,
            'updated_at': current_time.isoformat()
        }
        
        if description:
            update_mapping['description'] = description
        
        await redis.hset(task_key, mapping=update_mapping)
        logger.info(f"[Redis模式] 更新任务阶段成功: task_id={backup_task.id}, stage={stage_code}" + (f", description={description}" if description else ""))
    except Exception as e:
        logger.error(f"[Redis模式] 更新任务阶段失败: {str(e)}", exc_info=True)


async def update_scan_progress_redis(
    backup_task: BackupTask,
    scanned_count: int,
    valid_count: int,
    operation_status: str = None
):
    """更新扫描进度（Redis版本）
    
    Args:
        backup_task: 备份任务对象
        scanned_count: 已处理文件数（processed_files）
        valid_count: 压缩包数量（total_files），仅在压缩/写入阶段使用；扫描阶段传入的值会被忽略，使用 backup_task.total_files
        operation_status: 操作状态（如"[扫描文件中...]"、"[压缩文件中...]"等）
    """
    try:
        if not backup_task or not backup_task.id:
            return
        
        import re
        
        redis = await get_redis_client()
        task_key = _get_redis_key(KEY_PREFIX_BACKUP_TASK, backup_task.id)
        
        # 检查任务是否存在
        exists = await redis.exists(task_key)
        if not exists:
            logger.warning(f"[Redis模式] 找不到备份任务: {backup_task.id}")
            return
        
        # 获取当前任务数据
        task_data = await redis.hgetall(task_key)
        
        # 读取当前值（保持不变）
        total_files_value_str = task_data.get('total_files', '0')
        try:
            total_files_value = int(total_files_value_str) if total_files_value_str else 0
        except (ValueError, TypeError):
            total_files_value = 0
        
        # 获取 result_summary
        result_summary_str = task_data.get('result_summary', '{}') if task_data else '{}'
        try:
            result_summary = _ensure_dict(result_summary_str)
        except Exception:
            result_summary = {}
        
        # 获取当前 description
        current_desc = task_data.get('description', '') if task_data else ''
        
        # 获取压缩字节数和已处理字节数
        compressed_bytes = getattr(backup_task, 'compressed_bytes', None) or 0
        processed_bytes = getattr(backup_task, 'processed_bytes', None) or 0
        
        # 更新 result_summary 中的 estimated_archive_count
        if hasattr(backup_task, 'result_summary') and backup_task.result_summary:
            if isinstance(backup_task.result_summary, dict):
                if 'estimated_archive_count' in backup_task.result_summary:
                    result_summary['estimated_archive_count'] = backup_task.result_summary['estimated_archive_count']
        
        # 处理描述
        if operation_status:
            if '[格式化中]' in current_desc:
                cleaned_desc = re.sub(r'\[(?!格式化中)[^\]]+\.\.\.\]', '', current_desc)
                cleaned_desc = cleaned_desc.replace('  ', ' ').strip()
                new_desc = cleaned_desc + ' ' + operation_status if cleaned_desc else operation_status
            else:
                cleaned_desc = re.sub(r'\[[^\]]+\.\.\.\]', '', current_desc)
                cleaned_desc = cleaned_desc.replace('  ', ' ').strip()
                new_desc = cleaned_desc + ' ' + operation_status if cleaned_desc else operation_status
        else:
            new_desc = current_desc
        
        # 更新任务数据
        current_time = now()
        update_mapping = {
            'progress_percent': str(backup_task.progress_percent) if hasattr(backup_task, 'progress_percent') and backup_task.progress_percent is not None else '0.0',
            'processed_files': str(scanned_count),
            'total_files': str(total_files_value),  # 保持不变
            'processed_bytes': str(processed_bytes),
            'compressed_bytes': str(compressed_bytes),
            'result_summary': json.dumps(result_summary) if result_summary else '{}',
            'description': new_desc,
            'updated_at': current_time.isoformat()
        }
        
        await redis.hset(task_key, mapping=update_mapping)
        logger.debug(f"[Redis模式] 更新扫描进度成功: task_id={backup_task.id}, processed_files={scanned_count}, processed_bytes={processed_bytes}, compressed_bytes={compressed_bytes}")
    except Exception as e:
        logger.error(f"[Redis模式] 更新扫描进度失败: {str(e)}", exc_info=True)


async def update_scan_progress_only_redis(backup_task: BackupTask, total_files: int, total_bytes: int):
    """仅更新扫描进度（总文件数和总字节数），不更新已处理文件数（Redis版本）
    
    Args:
        backup_task: 备份任务对象
        total_files: 总文件数
        total_bytes: 总字节数
    """
    try:
        if not backup_task or not backup_task.id:
            return
        
        # 更新备份任务对象的统计信息
        backup_task.total_files = total_files
        backup_task.total_bytes = total_bytes
        
        redis = await get_redis_client()
        task_key = _get_redis_key(KEY_PREFIX_BACKUP_TASK, backup_task.id)
        
        # 检查任务是否存在
        exists = await redis.exists(task_key)
        if not exists:
            logger.warning(f"[Redis模式] 找不到备份任务: {backup_task.id}")
            return
        
        # 获取当前的 result_summary
        task_data = await redis.hgetall(task_key)
        result_summary_str = task_data.get('result_summary', '{}') if task_data else '{}'
        
        try:
            result_summary = _ensure_dict(result_summary_str)
        except Exception:
            result_summary = {}
        
        # 更新 result_summary 中的总文件数和总字节数（作为备份存储）
        result_summary['total_scanned_files'] = total_files
        result_summary['total_scanned_bytes'] = total_bytes
        
        current_time = now()
        update_mapping = {
            'total_files': str(total_files),
            'total_bytes': str(total_bytes),
            'result_summary': json.dumps(result_summary) if result_summary else '{}',
            'updated_at': current_time.isoformat()
        }
        
        await redis.hset(task_key, mapping=update_mapping)
        logger.debug(f"[Redis模式] 更新扫描进度成功: task_id={backup_task.id}, total_files={total_files}, total_bytes={total_bytes}")
    except Exception as e:
        logger.error(f"[Redis模式] 更新扫描进度失败: {str(e)}", exc_info=True)


async def get_scan_status_redis(backup_task_id: int) -> Optional[str]:
    """获取扫描状态（Redis版本）
    
    Args:
        backup_task_id: 备份任务ID
        
    Returns:
        扫描状态字符串，如果不存在则返回None
    """
    try:
        redis = await get_redis_client()
        task_key = _get_redis_key(KEY_PREFIX_BACKUP_TASK, backup_task_id)
        
        scan_status = await redis.hget(task_key, 'scan_status')
        return scan_status if scan_status else None
    except Exception as e:
        logger.error(f"[Redis模式] 获取扫描状态失败: {str(e)}", exc_info=True)
        return None


async def fetch_pending_files_grouped_by_size_redis(
    backup_set_db_id: int,
    max_file_size: int,
    backup_task_id: int = None,
    should_wait_if_small: bool = True
) -> List[List[Dict]]:
    """Redis 版本：获取待压缩文件组
    
    Args:
        backup_set_db_id: 备份集数据库ID
        max_file_size: 最大文件组大小（字节）
        backup_task_id: 备份任务ID，用于获取重试计数
        should_wait_if_small: 是否在组大小不足时等待
        
    Returns:
        List[List[Dict]]: 包含一个文件组的列表，空列表表示等待或无文件
    """
    try:
        from backup.utils import format_bytes
        
        max_retries = 6
        # retry_count 应该由调用方传递，这里只用于日志显示
        # 实际的重试逻辑由 compression_worker 管理
        retry_count = 0 if should_wait_if_small else max_retries
        
        logger.info(
            f"[Redis压缩检索] 开始检索待压缩文件: backup_set_id={backup_set_db_id}, "
            f"max_file_size={format_bytes(max_file_size)}, backup_task_id={backup_task_id}, "
            f"should_wait_if_small={should_wait_if_small}, retry_count={retry_count}/{max_retries}"
        )
        
        redis = await get_redis_client()
        
        # 阶段1优化：使用未压缩文件索引（Sorted Set）直接检索，避免全量扫描
        pending_index_key = f"{KEY_INDEX_BACKUP_FILE_PENDING}:{backup_set_db_id}"
        pending_files_data = []
        batch_size = 1000  # 每批处理1000个文件
        total_scanned = 0
        
        # 检查索引是否存在
        index_exists = await redis.exists(pending_index_key)
        if not index_exists:
            logger.warning(f"[Redis压缩检索] 未压缩文件索引不存在: {pending_index_key}，可能所有文件已压缩或索引未建立")
            # 回退到旧逻辑：使用全量扫描（兼容旧数据）
            logger.info("[Redis压缩检索] 回退到全量扫描模式（兼容旧数据）")
            file_index_key = f"{KEY_INDEX_BACKUP_FILE_BY_SET_ID}:{backup_set_db_id}"
            cursor = 0
            while True:
                cursor, file_ids_batch = await redis.sscan(file_index_key, cursor=cursor, count=batch_size)
                if not file_ids_batch:
                    if cursor == 0:
                        break
                    continue
                
                total_scanned += len(file_ids_batch)
                pipe = redis.pipeline()
                for file_id in file_ids_batch:
                    file_key = _get_redis_key(KEY_PREFIX_BACKUP_FILE, file_id)
                    pipe.hgetall(file_key)
                
                batch_results = await pipe.execute()
                
                # 过滤未压缩文件并构建文件信息
                for file_id, file_data in zip(file_ids_batch, batch_results):
                    if not file_data:
                        continue
                    file_type = file_data.get('file_type', '').lower()
                    if file_type != 'file':
                        continue
                    is_copy_success = file_data.get('is_copy_success', '0')
                    if is_copy_success in ('1', 'True', 'true'):
                        continue
                    
                    try:
                        file_size_str = file_data.get('file_size', '') or ''
                        file_size = int(file_size_str) if file_size_str and file_size_str != '0' else 0
                    except (ValueError, TypeError):
                        file_size = 0
                    
                    file_info = {
                        'id': int(file_id) if file_id.isdigit() else file_id,
                        'path': file_data.get('file_path', ''),
                        'file_path': file_data.get('file_path', ''),
                        'name': file_data.get('file_name', ''),
                        'file_name': file_data.get('file_name', ''),
                        'size': file_size,
                        'file_size': file_size,
                        'permissions': file_data.get('file_permissions', ''),
                        'file_permissions': file_data.get('file_permissions', ''),
                        'modified_time': _parse_datetime_value(file_data.get('modified_time')),
                        'accessed_time': _parse_datetime_value(file_data.get('accessed_time')),
                        'created_time': _parse_datetime_value(file_data.get('created_time')),
                        'is_dir': False,
                        'is_file': True,
                        'is_symlink': False,
                    }
                    pending_files_data.append(file_info)
                
                if cursor == 0:
                    break
                
                if total_scanned % 10000 == 0:
                    logger.info(f"[Redis压缩检索] 全量扫描模式：已扫描 {total_scanned} 个文件，找到 {len(pending_files_data)} 个待压缩文件...")
        else:
            # 阶段2优化：流式加载和实时分组
            # 使用ZSCAN流式加载，边加载边分组，一旦形成组就立即返回
            tolerance = max_file_size * 0.05
            min_group_size = max_file_size - tolerance
            max_group_size = max_file_size + tolerance
            
            current_group: List[Dict] = []
            current_group_size = 0
            skipped_files: List[Dict] = []
            cursor = 0
            
            # 流式加载并实时分组
            while True:
                # 使用ZSCAN获取一批文件ID和大小（withscores=True同时获取文件大小）
                cursor, items = await redis.zscan(pending_index_key, cursor=cursor, count=batch_size)
                
                if not items:
                    if cursor == 0:
                        break
                    continue
                
                total_scanned += len(items)
                
                # 批量获取文件详细信息（只获取必要字段）
                file_ids_batch = []
                file_sizes_map = {}  # {file_id: file_size} 从Sorted Set的score获取
                
                for item in items:
                    if len(item) >= 2:
                        file_id_str = str(item[0])
                        file_size = int(item[1])  # 从Sorted Set的score获取文件大小
                        file_ids_batch.append(file_id_str)
                        file_sizes_map[file_id_str] = file_size
                
                # 批量获取文件详细信息（只获取必要字段）
                pipe = redis.pipeline()
                for file_id in file_ids_batch:
                    file_key = _get_redis_key(KEY_PREFIX_BACKUP_FILE, file_id)
                    # 只获取必要字段，减少数据传输量
                    pipe.hmget(file_key, 'file_path', 'file_name', 'file_type', 'file_permissions', 
                               'modified_time', 'accessed_time', 'created_time', 'is_copy_success')
                
                batch_results = await pipe.execute()
                
                # 实时分组：边加载边分组，一旦达到目标大小就返回
                for file_id, file_fields in zip(file_ids_batch, batch_results):
                    if not file_fields or len(file_fields) < 8:
                        continue
                    
                    file_path, file_name, file_type, file_permissions, modified_time, accessed_time, created_time, is_copy_success = file_fields
                    
                    # 双重检查：如果已压缩，跳过（索引可能未及时更新）
                    if is_copy_success in ('1', 'True', 'true'):
                        logger.debug(f"[Redis压缩检索] 文件已压缩但仍在索引中，跳过: file_id={file_id}")
                        continue
                    
                    file_type_lower = (file_type or '').lower()
                    if file_type_lower != 'file':
                        continue
                    
                    # 使用Sorted Set中的文件大小（更可靠）
                    file_size = file_sizes_map.get(file_id, 0)
                    
                    # 处理超大文件：超过容差上限，单独成组
                    if file_size > max_group_size:
                        if current_group:
                            logger.info(
                                f"[Redis策略] 流式加载：返回当前组：{len(current_group)} 个文件，总大小 {format_bytes(current_group_size)}，"
                                f"发现超大文件将单独处理"
                            )
                            return [current_group]
                        
                        logger.warning(
                            f"[Redis策略] 发现超大文件单独成组：{format_bytes(file_size)} "
                            f"(超过最大大小 {format_bytes(max_file_size)} 含容差)"
                        )
                        # 构建超大文件信息并立即返回
                        file_info = {
                            'id': int(file_id) if file_id.isdigit() else file_id,
                            'path': file_path or '',
                            'file_path': file_path or '',
                            'name': file_name or '',
                            'file_name': file_name or '',
                            'size': file_size,
                            'file_size': file_size,
                            'permissions': file_permissions or '',
                            'file_permissions': file_permissions or '',
                            'modified_time': _parse_datetime_value(modified_time),
                            'accessed_time': _parse_datetime_value(accessed_time),
                            'created_time': _parse_datetime_value(created_time),
                            'is_dir': False,
                            'is_file': True,
                            'is_symlink': False,
                        }
                        return [[file_info]]
                    
                    # 检查加入当前组是否会超过最大大小
                    new_group_size = current_group_size + file_size
                    
                    if new_group_size > max_file_size:
                        skipped_files.append({
                            'id': int(file_id) if file_id.isdigit() else file_id,
                            'path': file_path or '',
                            'file_path': file_path or '',
                            'name': file_name or '',
                            'size': file_size,
                        })
                        logger.debug(
                            f"[Redis策略] 跳过文件（超过最大大小）：{file_name or 'unknown'} "
                            f"({format_bytes(file_size)})，当前组：{format_bytes(current_group_size)}"
                        )
                        continue
                    
                    # 构建文件信息并添加到当前组
                    file_info = {
                        'id': int(file_id) if file_id.isdigit() else file_id,
                        'path': file_path or '',
                        'file_path': file_path or '',
                        'name': file_name or '',
                        'file_name': file_name or '',
                        'size': file_size,
                        'file_size': file_size,
                        'permissions': file_permissions or '',
                        'file_permissions': file_permissions or '',
                        'modified_time': _parse_datetime_value(modified_time),
                        'accessed_time': _parse_datetime_value(accessed_time),
                        'created_time': _parse_datetime_value(created_time),
                        'is_dir': False,
                        'is_file': True,
                        'is_symlink': False,
                    }
                    
                    current_group.append(file_info)
                    current_group_size = new_group_size
                    
                    # 阶段2优化：一旦组大小达到目标大小，立即返回（不等待所有文件加载）
                    # 这样可以显著减少等待时间，压缩可以立即开始
                    if current_group_size >= max_file_size:
                        logger.info(
                            f"[Redis策略] 流式加载：组大小达到目标，立即返回：{len(current_group)} 个文件，"
                            f"总大小={format_bytes(current_group_size)}，已扫描 {total_scanned} 个文件"
                        )
                        return [current_group]
                
                if cursor == 0:
                    break
                
                # 每处理1万个文件记录一次进度
                if total_scanned % 10000 == 0:
                    logger.info(f"[Redis压缩检索] 流式加载：已扫描 {total_scanned} 个未压缩文件，当前组：{len(current_group)} 个文件，总大小 {format_bytes(current_group_size)}...")
            
            # 阶段2优化：流式加载完成，处理剩余的文件组
            # 如果current_group不为空，继续处理；如果为空，说明所有文件都已处理或跳过
            if not current_group:
                # 检查是否有跳过的文件（超大文件）
                if skipped_files:
                    logger.warning(
                        f"[Redis策略] 流式加载：所有文件都超过最大大小，跳过了 {len(skipped_files)} 个文件"
                    )
                else:
                    logger.info("[Redis策略] 流式加载：没有待压缩文件")
                return []
            
            # 流式加载模式下，current_group已经分组，直接使用
            pending_files_data = current_group
        
        # 处理全量扫描模式的结果（索引不存在时）
        if not index_exists:
            # 全量扫描模式：应用分组逻辑
            if total_scanned == 0:
                logger.warning(f"[Redis压缩检索] 未找到任何文件（backup_set_id={backup_set_db_id}）")
                return []
            
            logger.info(f"[Redis压缩检索] 扫描完成: 共扫描 {total_scanned} 个文件，找到 {len(pending_files_data)} 个待压缩文件")
            
            if not pending_files_data:
                logger.warning(f"[Redis压缩检索] 未找到任何待压缩文件（backup_set_id={backup_set_db_id}），所有文件已压缩")
                return []
            
            # 按 id 排序（保持顺序）
            pending_files_data.sort(key=lambda x: x['id'] if isinstance(x['id'], int) else 0)
            
            # 应用分组逻辑（与 SQLite 版本相同）
            tolerance = max_file_size * 0.05
            min_group_size = max_file_size - tolerance
            max_group_size = max_file_size + tolerance
            
            current_group: List[Dict] = []
            current_group_size = 0
            skipped_files: List[Dict] = []
            
            logger.info(
                f"[Redis策略] 检索到 {len(pending_files_data)} 个未压缩文件，"
                f"目标范围：{format_bytes(min_group_size)} - {format_bytes(max_file_size)} "
                f"(含容差上限：{format_bytes(max_group_size)})，重试次数：{retry_count}/{max_retries}"
            )
            
            for file_info in pending_files_data:
                file_size = file_info['size']
                
                # 处理超大文件：超过容差上限，单独成组
                if file_size > max_group_size:
                    if current_group:
                        logger.info(
                            f"[Redis策略] 返回当前组：{len(current_group)} 个文件，总大小 {format_bytes(current_group_size)}，"
                            f"发现超大文件将单独处理"
                        )
                        return [current_group]
                    
                    logger.warning(
                        f"[Redis策略] 发现超大文件单独成组：{format_bytes(file_size)} "
                        f"(超过最大大小 {format_bytes(max_file_size)} 含容差)"
                    )
                    return [[file_info]]
                
                # 检查加入当前组是否会超过最大大小
                new_group_size = current_group_size + file_size
                
                if new_group_size > max_file_size:
                    skipped_files.append(file_info)
                    logger.debug(
                        f"[Redis策略] 跳过文件（超过最大大小）：{file_info['name']} "
                        f"({format_bytes(file_size)})，当前组：{format_bytes(current_group_size)}"
                    )
                    continue
                
                current_group.append(file_info)
                current_group_size = new_group_size
        
        if not current_group:
            if skipped_files:
                logger.warning(
                    f"[Redis策略] 所有文件都超过最大大小，跳过了 {len(skipped_files)} 个文件"
                )
            else:
                logger.info("[Redis策略] 没有待压缩文件")
            return []
        
        # 检查文件组大小和扫描状态
        size_ratio = current_group_size / max_file_size if max_file_size > 0 else 0
        scan_status = await get_scan_status_redis(backup_task_id) if backup_task_id else None
        
        logger.info(
            f"[Redis策略] 文件组构建完成: {len(current_group)} 个文件，"
            f"总大小={format_bytes(current_group_size)}, 大小比例={size_ratio*100:.1f}%, "
            f"扫描状态={scan_status}, retry_count={retry_count}/{max_retries}"
        )
        
        if current_group_size < min_group_size and scan_status != 'completed' and retry_count < max_retries:
            logger.warning(
                f"[Redis策略] 文件组大小低于容差下限：{format_bytes(current_group_size)} "
                f"(需要 ≥ {format_bytes(min_group_size)} = {size_ratio*100:.1f}% of 目标)，"
                f"扫描状态：{scan_status}，等待更多文件...（重试 {retry_count}/{max_retries}）"
            )
            return []
        
        if current_group_size >= min_group_size:
            logger.info(
                f"[Redis策略] ✅ 达到容差范围内：{format_bytes(current_group_size)} "
                f"({size_ratio*100:.1f}% of 目标，≥ {format_bytes(min_group_size)})，"
                f"跳过了 {len(skipped_files)} 个文件，返回文件组（{len(current_group)} 个文件）"
            )
        else:
            reason = '扫描已完成' if scan_status == 'completed' else '达到重试上限'
            logger.warning(
                f"[Redis策略] ⚠️ 强制压缩：文件组大小 {format_bytes(current_group_size)} "
                f"({size_ratio*100:.1f}% of 目标，< {format_bytes(min_group_size)})，原因：{reason}，"
                f"返回文件组（{len(current_group)} 个文件）"
            )
        
        logger.info(
            f"[Redis策略] 最终返回: 1个文件组，包含 {len(current_group)} 个文件，"
            f"总大小={format_bytes(current_group_size)}"
        )
        return [current_group]
    except Exception as e:
        logger.error(f"[Redis模式] 获取待压缩文件组失败: {str(e)}", exc_info=True)
        return []


async def get_compressed_files_count_redis(backup_set_db_id: int) -> int:
    """Redis 版本：查询已压缩文件数（聚合所有进程的进度）
    
    Args:
        backup_set_db_id: 备份集数据库ID
        
    Returns:
        已压缩文件数（is_copy_success = '1' 的文件数）
    """
    try:
        redis = await get_redis_client()
        
        # 使用备份集索引来查找所有文件
        backup_set_key = f"{KEY_PREFIX_BACKUP_SET}:{backup_set_db_id}"
        file_list_key = f"{backup_set_key}:files"
        
        # 获取所有文件ID
        file_ids = await redis.smembers(file_list_key)
        if not file_ids:
            return 0
        
        # 统计 is_copy_success = '1' 的文件数
        compressed_count = 0
        for file_id_bytes in file_ids:
            try:
                file_id = int(file_id_bytes) if isinstance(file_id_bytes, bytes) else int(file_id_bytes)
                file_key = _get_redis_key(KEY_PREFIX_BACKUP_FILE, file_id)
                
                # 检查文件类型和 is_copy_success 状态
                file_type = await redis.hget(file_key, 'file_type')
                is_copy_success = await redis.hget(file_key, 'is_copy_success')
                
                if file_type == 'file' and is_copy_success in ('1', 'True', 'true'):
                    compressed_count += 1
            except (ValueError, TypeError):
                continue
        
        return compressed_count
    except Exception as e:
        logger.error(f"[Redis模式] 查询已压缩文件数失败: {str(e)}", exc_info=True)
        return 0

async def mark_files_as_queued_redis(
    backup_set_db_id: int,
    file_paths: List[str]
):
    """Redis 版本：标记文件为已入队（仅设置 is_copy_success = TRUE）"""
    logger.info(f"[Redis模式] 开始标记 {len(file_paths)} 个文件为已入队（is_copy_success = TRUE）")
    
    if not file_paths:
        logger.warning("[Redis模式] ❌ 没有可更新的文件，跳过 mark_files_as_queued")
        return
    
    try:
        redis = await get_redis_client()
        path_index_key = f"{KEY_INDEX_BACKUP_FILE_BY_PATH}:{backup_set_db_id}"
        
        # 批量获取文件ID
        file_ids = []
        for file_path in file_paths:
            file_id = await redis.hget(path_index_key, file_path)
            if file_id:
                try:
                    file_ids.append(int(file_id))
                except (ValueError, TypeError):
                    continue
        
        if not file_ids:
            logger.warning(f"[Redis模式] ⚠️ 未找到任何文件ID，跳过更新")
            return
        
        # 批量更新 is_copy_success
        updated_count = 0
        for file_id in file_ids:
            file_key = _get_redis_key("backup_file", file_id)
            current_value = await redis.hget(file_key, 'is_copy_success')
            if current_value in ('0', 'False', 'false', None):
                await redis.hset(file_key, 'is_copy_success', '1')
                await redis.hset(file_key, 'copy_status_at', datetime.now().isoformat())
                await redis.hset(file_key, 'updated_at', datetime.now().isoformat())
                updated_count += 1
        
        logger.info(f"[Redis模式] ✅ 已更新 {updated_count} 个文件的 is_copy_success 状态")
    except Exception as e:
        logger.error(f"[Redis模式] ❌ 标记文件为已入队失败: {str(e)}", exc_info=True)
        raise

async def mark_files_as_copied_redis(
    backup_set_db_id: int,
    processed_files: List[Dict],
    compressed_file: Dict,
    tape_file_path: Optional[str],
    chunk_number: int,
    backup_time: Optional[datetime] = None
):
    """Redis 版本：标记压缩完成的文件，避免重复压缩/误操作"""
    logger.info(f"[Redis模式] ========== 开始标记文件为复制成功 ==========")
    logger.info(f"[Redis模式] 参数: backup_set_db_id={backup_set_db_id}, 文件数={len(processed_files)}, chunk_number={chunk_number}")
    
    if not processed_files:
        logger.warning("[Redis模式] ❌ 没有可更新的文件，跳过 mark_files_as_copied")
        return

    try:
        redis = await get_redis_client()
        backup_time = backup_time or datetime.now()
        per_file_compressed_size = int(
            (compressed_file.get("compressed_size") or 0) / max(len(processed_files), 1)
        )
        is_compressed = bool(compressed_file.get("compression_enabled", True))
        checksum = compressed_file.get("checksum")
        copy_time = datetime.now()

        file_paths: List[str] = []
        for item in processed_files:
            path_value = item.get("file_path") or item.get("path")
            if path_value:
                file_paths.append(path_value)

        logger.info(f"[Redis模式] 提取到 {len(file_paths)} 个文件路径（总文件数={len(processed_files)}）")
        if not file_paths:
            logger.error(f"[Redis模式] ❌ 处理中缺少 file_path，无法更新 is_copy_success！")
            return

        # 关键优化：直接使用传入的 file_paths 批量查询，避免遍历整个备份集
        path_index_key = f"{KEY_INDEX_BACKUP_FILE_BY_PATH}:{backup_set_db_id}"
        
        # 批量获取文件信息，构建 file_path -> file_id 映射
        logger.info(f"[Redis模式] 开始查询数据库中已存在的文件（backup_set_id={backup_set_db_id}，待查询文件数={len(file_paths)}）")
        existing_map: Dict[str, int] = {}  # {file_path: file_id}
        
        start_query_time = time.time()
        
        # 优化策略：直接批量查询 file_path -> file_id 反向索引（如果有）
        # 如果没有索引，回退到遍历方式（但这种情况应该在写入时就建立索引）
        
        # 尝试使用反向索引批量查询
        batch_size = 2000  # 批次大小
        total_found = 0
        
        # 分批查询 file_paths
        for i in range(0, len(file_paths), batch_size):
            batch_paths = file_paths[i:i + batch_size]
            
            # 批量查询 file_path -> file_id 映射（使用 HMGET）
            pipe = redis.pipeline()
            for file_path in batch_paths:
                pipe.hget(path_index_key, file_path)
            
            file_ids_result = await pipe.execute()
            
            # 处理查询结果
            for file_path, file_id_str in zip(batch_paths, file_ids_result):
                if file_id_str:
                    try:
                        file_id = int(file_id_str)
                        existing_map[file_path] = file_id
                        total_found += 1
                    except (ValueError, TypeError):
                        logger.debug(f"[Redis模式] 无效的file_id: {file_id_str} for path: {file_path}")
        
        query_time = time.time() - start_query_time
        
        # 如果通过索引找到的文件数少于传入的文件数，可能是索引未建立，回退到遍历方式
        if total_found < len(file_paths) and len(existing_map) < len(file_paths):
            logger.debug(
                f"[Redis模式] 通过索引找到 {len(existing_map)}/{len(file_paths)} 个文件，"
                f"尝试遍历备份集查找剩余文件（备份集可能尚未建立 file_path 索引）"
            )
            
            # 回退方案：遍历备份集查找剩余的文件路径
            set_file_index_key = f"{KEY_INDEX_BACKUP_FILE_BY_SET_ID}:{backup_set_db_id}"
            missing_paths = set(file_paths) - set(existing_map.keys())
            
            if missing_paths:
                batch_size_scan = 2000
                cursor = 0
                batch_num = 0
                total_scanned = 0
                
                while len(missing_paths) > 0:
                    # 使用SSCAN获取一批文件ID
                    cursor, file_ids_batch = await redis.sscan(set_file_index_key, cursor=cursor, count=batch_size_scan)
                    
                    if not file_ids_batch:
                        if cursor == 0:
                            break
                        continue
                    
                    batch_num += 1
                    total_scanned += len(file_ids_batch)
                    
                    # 批量获取文件路径
                    pipe = redis.pipeline()
                    for file_id in file_ids_batch:
                        file_key = _get_redis_key(KEY_PREFIX_BACKUP_FILE, file_id)
                        pipe.hget(file_key, 'file_path')
                    
                    file_paths_result = await pipe.execute()
                    
                    # 检查是否匹配缺失的文件路径
                    for file_id, file_path in zip(file_ids_batch, file_paths_result):
                        if file_path and file_path in missing_paths:
                            existing_map[file_path] = int(file_id)
                            missing_paths.remove(file_path)
                            # 同时更新索引（下次查询更快）
                            await redis.hset(path_index_key, file_path, str(file_id))
                    
                    # 如果找到所有缺失的文件，提前退出
                    if not missing_paths:
                        break
                    
                    if cursor == 0:
                        break
        
        query_time_total = time.time() - start_query_time
        
        logger.info(
            f"[Redis模式] 查询完成: 找到 {len(existing_map)} 个已存在的文件（需要更新），"
            f"{len(file_paths) - len(existing_map)} 个新文件（需要插入），"
            f"耗时={query_time_total*1000:.1f}ms"
        )

        # 准备批量更新和插入
        update_operations = []
        insert_operations = []
        skipped = 0

        for processed_file in processed_files:
            file_path = processed_file.get("file_path") or processed_file.get("path")
            if not file_path:
                skipped += 1
                continue

            file_name = processed_file.get("file_name") or Path(file_path).name
            file_size = processed_file.get("file_size") or processed_file.get("size") or 0
            file_stat = processed_file.get("file_stat")
            
            metadata = _ensure_dict(processed_file.get("file_metadata"))
            metadata["tape_file_path"] = tape_file_path
            metadata["chunk_number"] = chunk_number
            metadata.setdefault("original_path", file_path)
            metadata_json = json.dumps(metadata) if metadata else "{}"

            created_time = _parse_datetime_value(processed_file.get("created_time"))
            modified_time = _parse_datetime_value(processed_file.get("modified_time"))
            accessed_time = _parse_datetime_value(processed_file.get("accessed_time"))
            
            if file_stat:
                if not created_time and hasattr(file_stat, 'st_ctime'):
                    created_time = datetime.fromtimestamp(file_stat.st_ctime)
                if not modified_time and hasattr(file_stat, 'st_mtime'):
                    modified_time = datetime.fromtimestamp(file_stat.st_mtime)
                if not accessed_time and hasattr(file_stat, 'st_atime'):
                    accessed_time = datetime.fromtimestamp(file_stat.st_atime)

            if file_path in existing_map:
                # 更新现有文件
                file_id = existing_map[file_path]
                file_key = _get_redis_key(KEY_PREFIX_BACKUP_FILE, file_id)
                update_mapping = {
                    'file_name': file_name,
                    'file_size': str(file_size),
                    'compressed_size': str(per_file_compressed_size),
                    'compressed': '1' if is_compressed else '0',
                    'checksum': checksum or '',
                    'backup_time': backup_time.isoformat(),
                    'chunk_number': str(chunk_number),
                    'tape_block_start': '0',
                    'file_metadata': metadata_json,
                    'is_copy_success': '1',
                    'copy_status_at': copy_time.isoformat(),
                    'updated_at': datetime.now().isoformat()
                }
                if created_time:
                    update_mapping['created_time'] = created_time.isoformat()
                if modified_time:
                    update_mapping['modified_time'] = modified_time.isoformat()
                if accessed_time:
                    update_mapping['accessed_time'] = accessed_time.isoformat()
                
                update_operations.append((file_key, update_mapping))
            else:
                # 插入新文件（需要获取新ID）
                insert_operations.append({
                    'file_path': file_path,
                    'file_name': file_name,
                    'file_size': file_size,
                    'created_time': created_time,
                    'modified_time': modified_time,
                    'accessed_time': accessed_time,
                    'metadata': metadata_json
                })

        # 批量执行更新
        success_updates = 0
        if update_operations:
            update_start_time = time.time()
            logger.info(f"[Redis模式] 开始批量更新 {len(update_operations)} 个文件")
            
            # 阶段1优化：收集需要从未压缩文件索引中移除的文件ID
            file_ids_to_remove_from_pending = []
            for file_key, update_mapping in update_operations:
                # 从file_key中提取file_id（格式：backup_file:{file_id}）
                file_id_str = file_key.split(':')[-1]
                file_ids_to_remove_from_pending.append(file_id_str)
            
            # 优化：分批更新，每批最多5000个（避免单个Pipeline过大）
            batch_update_size = 5000
            total_updated = 0
            
            for i in range(0, len(update_operations), batch_update_size):
                batch_ops = update_operations[i:i + batch_update_size]
                batch_file_ids = file_ids_to_remove_from_pending[i:i + batch_update_size]
                pipe = redis.pipeline()
                for file_key, update_mapping in batch_ops:
                    pipe.hset(file_key, mapping=update_mapping)
                # 阶段1优化：从未压缩文件索引中批量移除已压缩的文件
                pending_index_key = f"{KEY_INDEX_BACKUP_FILE_PENDING}:{backup_set_db_id}"
                if batch_file_ids:
                    pipe.zrem(pending_index_key, *batch_file_ids)
                await pipe.execute()
                total_updated += len(batch_ops)
            
            update_time = time.time() - update_start_time
            success_updates = total_updated
            update_speed = success_updates / update_time if update_time > 0 else 0
            
            logger.info(
                f"[Redis模式] ✅ 批量更新完成: {success_updates} 个文件，"
                f"耗时={update_time*1000:.1f}ms，速度={update_speed:.0f} 个/秒"
            )

        # 批量执行插入
        success_inserts = 0
        if insert_operations:
            logger.info(f"[Redis模式] 开始批量插入 {len(insert_operations)} 个文件")
            # 先获取所有新文件的ID
            pipe = redis.pipeline()
            for _ in insert_operations:
                pipe.incr(KEY_COUNTER_BACKUP_FILE)
            new_ids = await pipe.execute()
            
            # 准备插入操作
            pipe = redis.pipeline()
            for insert_data, file_id in zip(insert_operations, new_ids):
                file_key = _get_redis_key(KEY_PREFIX_BACKUP_FILE, file_id)
                insert_mapping = {
                    'backup_set_id': str(backup_set_db_id),
                    'file_path': insert_data['file_path'],
                    'file_name': insert_data['file_name'],
                    'file_type': 'file',
                    'file_size': str(insert_data['file_size']),
                    'compressed_size': str(per_file_compressed_size),
                    'compressed': '1' if is_compressed else '0',
                    'checksum': checksum or '',
                    'backup_time': backup_time.isoformat(),
                    'chunk_number': str(chunk_number),
                    'tape_block_start': '0',
                    'file_metadata': insert_data['metadata'],
                    'is_copy_success': '1',
                    'copy_status_at': copy_time.isoformat()
                }
                if insert_data.get('created_time'):
                    insert_mapping['created_time'] = insert_data['created_time'].isoformat()
                if insert_data.get('modified_time'):
                    insert_mapping['modified_time'] = insert_data['modified_time'].isoformat()
                if insert_data.get('accessed_time'):
                    insert_mapping['accessed_time'] = insert_data['accessed_time'].isoformat()
                
                pipe.hset(file_key, mapping=insert_mapping)
                # 添加到索引
                pipe.sadd(KEY_INDEX_BACKUP_FILES, str(file_id))
                pipe.sadd(set_file_index_key, str(file_id))
                # 关键优化：添加 file_path -> file_id 反向索引（用于快速查询）
                path_index_key = f"{KEY_INDEX_BACKUP_FILE_BY_PATH}:{backup_set_db_id}"
                pipe.hset(path_index_key, insert_data['file_path'], str(file_id))
                # 阶段1优化：插入的新文件已经是已压缩状态（is_copy_success='1'），不需要添加到未压缩文件索引
            
            await pipe.execute()
            success_inserts = len(insert_operations)
            logger.info(f"[Redis模式] ✅ 批量插入完成: {success_inserts} 个文件")

        if success_updates or success_inserts:
            logger.info(
                f"[Redis模式] ✅ 已更新 {success_updates} 个文件、插入 {success_inserts} 个文件的压缩状态 "
                f"(is_copy_success=1, backup_set_id={backup_set_db_id}, chunk_number={chunk_number})"
            )
        else:
            logger.error(f"[Redis模式] ❌ 没有任何文件的 is_copy_success 状态被更新 (backup_set_id={backup_set_db_id}, 文件数={len(processed_files)}, 跳过={skipped})")

        if skipped:
            logger.warning(f"[Redis模式] 有 {skipped} 个文件缺少 file_path，已跳过")

    except Exception as e:
        logger.error(f"[Redis模式] 更新文件压缩状态失败: {str(e)}", exc_info=True)
        raise