#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
Redis操作日志存储模块
Redis Operation Log Storage Module
"""

import json
import logging
from datetime import datetime
from typing import Optional, Dict, Any, List

from config.redis_db import get_redis_client, get_redis_manager
from models.system_log import OperationType

logger = logging.getLogger(__name__)

# Redis键前缀
KEY_PREFIX_OPERATION_LOG = "operation_log"
KEY_INDEX_OPERATION_LOGS = "operation_logs:index"
KEY_INDEX_OPERATION_LOG_BY_RESOURCE_TYPE = "operation_logs:by_resource_type"
KEY_INDEX_OPERATION_LOG_BY_TIME = "operation_logs:by_time"
KEY_COUNTER_OPERATION_LOG = "operation_log:id"


def _get_redis_key(log_id: int) -> str:
    """获取操作日志的Redis键"""
    return f"{KEY_PREFIX_OPERATION_LOG}:{log_id}"


async def create_operation_log_redis(
    operation_type: OperationType,
    resource_type: str,
    resource_id: Optional[str] = None,
    resource_name: Optional[str] = None,
    operation_name: Optional[str] = None,
    operation_description: Optional[str] = None,
    category: Optional[str] = None,
    user_id: Optional[int] = None,
    username: Optional[str] = None,
    success: bool = True,
    result_message: Optional[str] = None,
    error_message: Optional[str] = None,
    duration_ms: Optional[int] = None,
    old_values: Optional[Dict[str, Any]] = None,
    new_values: Optional[Dict[str, Any]] = None,
    changed_fields: Optional[List[str]] = None,
    ip_address: Optional[str] = None,
    request_method: Optional[str] = None,
    request_url: Optional[str] = None,
    **kwargs
) -> int:
    """创建操作日志（Redis版本）"""
    try:
        redis = await get_redis_client()
        redis_manager = get_redis_manager()
        
        # 获取下一个ID
        log_id = await redis_manager.get_next_id(KEY_COUNTER_OPERATION_LOG)
        
        # 准备日志数据
        operation_time = datetime.now()
        log_data = {
            'id': str(log_id),
            'user_id': str(user_id) if user_id else '',
            'username': username or '',
            'operation_type': operation_type.value if isinstance(operation_type, OperationType) else str(operation_type),
            'resource_type': resource_type or '',
            'resource_id': resource_id or '',
            'resource_name': resource_name or '',
            'operation_name': operation_name or '',
            'operation_description': operation_description or '',
            'category': category or resource_type or '',
            'operation_time': operation_time.isoformat(),
            'duration_ms': str(duration_ms) if duration_ms else '',
            'request_method': request_method or '',
            'request_url': request_url or '',
            'success': '1' if success else '0',
            'result_message': result_message or '',
            'error_message': error_message or '',
            'ip_address': ip_address or '',
            'old_values': json.dumps(old_values) if old_values else '',
            'new_values': json.dumps(new_values) if new_values else '',
            'changed_fields': json.dumps(changed_fields) if changed_fields else '',
        }
        
        # 存储到Redis Hash
        log_key = _get_redis_key(log_id)
        await redis.hset(log_key, mapping=log_data)
        
        # 添加到索引
        await redis.sadd(KEY_INDEX_OPERATION_LOGS, str(log_id))
        if resource_type:
            await redis.sadd(f"{KEY_INDEX_OPERATION_LOG_BY_RESOURCE_TYPE}:{resource_type}", str(log_id))
        
        # 按时间索引（使用有序集合，分数为时间戳）
        timestamp = operation_time.timestamp()
        await redis.zadd(KEY_INDEX_OPERATION_LOG_BY_TIME, {str(log_id): timestamp})
        
        logger.debug(f"[Redis模式] 创建操作日志成功: log_id={log_id}, operation={operation_name or operation_type}")
        return log_id
    except Exception as e:
        logger.error(f"[Redis模式] 创建操作日志失败: {str(e)}", exc_info=True)
        raise


async def query_operation_logs_redis(
    resource_type: Optional[str] = None,
    operation_name_pattern: Optional[str] = None,
    operation_description_pattern: Optional[str] = None,
    limit: int = 100,
    offset: int = 0
) -> List[Dict]:
    """查询操作日志（Redis版本）"""
    try:
        redis = await get_redis_client()
        
        # 获取所有日志ID（从时间索引获取，按时间倒序）
        log_ids = await redis.zrevrange(KEY_INDEX_OPERATION_LOG_BY_TIME, offset, offset + limit - 1)
        
        if not log_ids:
            return []
        
        # 批量获取日志数据
        pipe = redis.pipeline()
        for log_id in log_ids:
            log_key = _get_redis_key(log_id)
            pipe.hgetall(log_key)
        
        log_data_list = await pipe.execute()
        
        # 转换为字典列表并应用过滤
        logs = []
        for log_id, log_data in zip(log_ids, log_data_list):
            if not log_data:
                continue
            
            # 转换为字典（Redis客户端配置了decode_responses=True，键值都是字符串）
            log_dict = {k: v for k, v in log_data.items()}
            
            # 应用过滤条件
            if resource_type and log_dict.get('resource_type') != resource_type:
                continue
            
            if operation_name_pattern and operation_name_pattern not in (log_dict.get('operation_name') or ''):
                continue
            
            if operation_description_pattern and operation_description_pattern not in (log_dict.get('operation_description') or ''):
                continue
            
            # 解析字段
            logs.append({
                'id': int(log_id),
                'user_id': int(log_dict.get('user_id', 0) or 0) if log_dict.get('user_id') else None,
                'username': log_dict.get('username', ''),
                'operation_type': log_dict.get('operation_type', ''),
                'resource_type': log_dict.get('resource_type', ''),
                'resource_id': log_dict.get('resource_id', ''),
                'resource_name': log_dict.get('resource_name', ''),
                'operation_name': log_dict.get('operation_name', ''),
                'operation_description': log_dict.get('operation_description', ''),
                'category': log_dict.get('category', ''),
                'operation_time': log_dict.get('operation_time', ''),
                'duration_ms': int(log_dict.get('duration_ms', 0) or 0) if log_dict.get('duration_ms') else None,
                'request_method': log_dict.get('request_method', ''),
                'request_url': log_dict.get('request_url', ''),
                'success': log_dict.get('success') == '1',
                'result_message': log_dict.get('result_message', ''),
                'error_message': log_dict.get('error_message', ''),
                'ip_address': log_dict.get('ip_address', ''),
                'old_values': json.loads(log_dict.get('old_values', '{}')) if log_dict.get('old_values') else None,
                'new_values': json.loads(log_dict.get('new_values', '{}')) if log_dict.get('new_values') else None,
                'changed_fields': json.loads(log_dict.get('changed_fields', '[]')) if log_dict.get('changed_fields') else None,
            })
        
        return logs
    except Exception as e:
        logger.error(f"[Redis模式] 查询操作日志失败: {str(e)}", exc_info=True)
        return []

