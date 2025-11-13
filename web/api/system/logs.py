#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
系统管理API - logs
System Management API - logs
"""

import logging
from typing import Dict, Any, Optional
from datetime import datetime
from fastapi import APIRouter, HTTPException, Request
from pydantic import BaseModel, Field

# 系统日志相关路由，无需导入模型

logger = logging.getLogger(__name__)
router = APIRouter()

@router.get("/logs")
async def get_system_logs(
    category: Optional[str] = None,
    level: Optional[str] = None,
    operation_type: Optional[str] = None,
    resource_type: Optional[str] = None,
    user_id: Optional[int] = None,
    start_time: Optional[datetime] = None,
    end_time: Optional[datetime] = None,
    limit: int = 100,
    offset: int = 0,
    request: Request = None
):
    """获取系统日志
    
    Args:
        category: 日志分类（system/backup/recovery/tape/user/security等）
        level: 日志级别（debug/info/warning/error/critical）
        operation_type: 操作类型（login/backup/recovery/tape_load等）
        resource_type: 资源类型（tape/backup/recovery/scheduler/user/system）
        user_id: 用户ID
        start_time: 开始时间
        end_time: 结束时间
        limit: 返回数量限制
        offset: 偏移量
    """
    try:
        from config.database import db_manager
        from datetime import datetime, timedelta
        import json
        
        # 如果没有指定时间范围，默认查询最近24小时的日志
        if not start_time:
            start_time = datetime.now() - timedelta(days=1)
        if not end_time:
            end_time = datetime.now()
        
        logs = []
        
        # 检查是否为 openGauss 数据库
        from utils.scheduler.db_utils import is_opengauss, get_opengauss_connection
        
        if is_opengauss():
            # 使用原生SQL查询（openGauss）
            # 使用连接池
            async with get_opengauss_connection() as conn:
                # 查询操作日志
                operation_where = []
                params = []
                param_idx = 1
                
                operation_where.append(f"operation_time >= ${param_idx}")
                params.append(start_time)
                param_idx += 1
                
                operation_where.append(f"operation_time <= ${param_idx}")
                params.append(end_time)
                param_idx += 1
                
                if category:
                    operation_where.append(f"category = ${param_idx}")
                    params.append(category)
                    param_idx += 1
                if operation_type:
                    operation_where.append(f"operation_type = ${param_idx}::operationtype")
                    params.append(operation_type)
                    param_idx += 1
                if resource_type:
                    operation_where.append(f"resource_type = ${param_idx}")
                    params.append(resource_type)
                    param_idx += 1
                if user_id:
                    operation_where.append(f"user_id = ${param_idx}")
                    params.append(user_id)
                    param_idx += 1
                
                # 添加LIMIT和OFFSET参数
                limit_param_idx = param_idx
                offset_param_idx = param_idx + 1
                params.extend([limit, offset])
                
                operation_sql = f"""
                    SELECT * FROM operation_logs
                    WHERE {' AND '.join(operation_where)}
                    ORDER BY operation_time DESC
                    LIMIT ${limit_param_idx} OFFSET ${offset_param_idx}
                """
                
                operation_rows = await conn.fetch(operation_sql, *params)
                
                # 查询系统日志
                system_where = []
                system_params = []
                system_param_idx = 1
                
                system_where.append(f"log_time >= ${system_param_idx}")
                system_params.append(start_time)
                system_param_idx += 1
                
                system_where.append(f"log_time <= ${system_param_idx}")
                system_params.append(end_time)
                system_param_idx += 1
                
                if category:
                    system_where.append(f"category = ${system_param_idx}::logcategory")
                    system_params.append(category)
                    system_param_idx += 1
                if level:
                    system_where.append(f"log_level = ${system_param_idx}::loglevel")
                    system_params.append(level.lower())
                    system_param_idx += 1
                
                # 添加LIMIT和OFFSET参数
                system_limit_param_idx = system_param_idx
                system_offset_param_idx = system_param_idx + 1
                system_params.extend([limit, offset])
                
                system_sql = f"""
                    SELECT * FROM system_logs
                    WHERE {' AND '.join(system_where)}
                    ORDER BY log_time DESC
                    LIMIT ${system_limit_param_idx} OFFSET ${system_offset_param_idx}
                """
                
                system_rows = await conn.fetch(system_sql, *system_params)
                
                # 格式化操作日志
                for row in operation_rows:
                    logs.append({
                        "id": row['id'],
                        "type": "operation",
                        "timestamp": row['operation_time'].isoformat() if row['operation_time'] else None,
                        "level": "info" if row.get('success', True) else "error",
                        "category": row.get('category') or "operation",
                        "operation_type": row.get('operation_type') if isinstance(row.get('operation_type'), str) else (row.get('operation_type').value if hasattr(row.get('operation_type'), 'value') else None),
                        "resource_type": row.get('resource_type'),
                        "resource_id": row.get('resource_id'),
                        "resource_name": row.get('resource_name'),
                        "user_id": row.get('user_id'),
                        "username": row.get('username'),
                        "operation_name": row.get('operation_name'),
                        "operation_description": row.get('operation_description'),
                        "success": row.get('success', True),
                        "result_message": row.get('result_message'),
                        "error_message": row.get('error_message'),
                        "ip_address": row.get('ip_address'),
                        "duration_ms": row.get('duration_ms'),
                        "details": {
                            "request_method": row.get('request_method'),
                            "request_url": row.get('request_url'),
                            "response_status": row.get('response_status'),
                            "old_values": json.loads(row['old_values']) if isinstance(row.get('old_values'), str) else row.get('old_values'),
                            "new_values": json.loads(row['new_values']) if isinstance(row.get('new_values'), str) else row.get('new_values')
                        }
                    })
                
                # 格式化系统日志
                for row in system_rows:
                    logs.append({
                        "id": row['id'],
                        "type": "system",
                        "timestamp": row['log_time'].isoformat() if row['log_time'] else None,
                        "level": row.get('log_level') if isinstance(row.get('log_level'), str) else (row.get('log_level').value if hasattr(row.get('log_level'), 'value') else "info"),
                        "category": row.get('category') if isinstance(row.get('category'), str) else (row.get('category').value if hasattr(row.get('category'), 'value') else "system"),
                        "message": row.get('message'),
                        "module": row.get('module'),
                        "function": row.get('function'),
                        "file_path": row.get('file_path'),
                        "line_number": row.get('line_number'),
                        "user_id": row.get('user_id'),
                        "task_id": row.get('task_id'),
                        "details": json.loads(row['details']) if isinstance(row.get('details'), str) else row.get('details'),
                        "exception_type": row.get('exception_type'),
                        "stack_trace": row.get('stack_trace'),
                        "duration_ms": row.get('duration_ms'),
                        "memory_usage_mb": row.get('memory_usage_mb'),
                        "cpu_usage_percent": row.get('cpu_usage_percent')
                    })
                
        else:
            # 使用SQLAlchemy查询（其他数据库）
            # 仅在非openGauss时导入SQLAlchemy模型，避免解析openGauss
            from models.system_log import SystemLog, OperationLog, LogLevel, LogCategory, OperationType
            from sqlalchemy import select, and_, or_, desc
            
            # 查询操作日志（OperationLog）
            operation_logs_query = select(OperationLog)
            operation_conditions = [
                OperationLog.operation_time >= start_time,
                OperationLog.operation_time <= end_time
            ]
            
            if category:
                operation_conditions.append(OperationLog.category == category)
            if operation_type:
                try:
                    operation_conditions.append(OperationLog.operation_type == OperationType(operation_type))
                except ValueError:
                    pass  # 无效的操作类型，忽略
            if resource_type:
                operation_conditions.append(OperationLog.resource_type == resource_type)
            if user_id:
                operation_conditions.append(OperationLog.user_id == user_id)
            
            operation_logs_query = operation_logs_query.where(and_(*operation_conditions))
            operation_logs_query = operation_logs_query.order_by(desc(OperationLog.operation_time))
            operation_logs_query = operation_logs_query.limit(limit).offset(offset)
            
            # 查询系统日志（SystemLog）
            system_logs_query = select(SystemLog)
            system_conditions = [
                SystemLog.log_time >= start_time,
                SystemLog.log_time <= end_time
            ]
            
            if category:
                try:
                    system_conditions.append(SystemLog.category == LogCategory(category))
                except ValueError:
                    pass  # 无效的分类，忽略
            if level:
                try:
                    system_conditions.append(SystemLog.log_level == LogLevel(level.lower()))
                except ValueError:
                    pass  # 无效的级别，忽略
            
            system_logs_query = system_logs_query.where(and_(*system_conditions))
            system_logs_query = system_logs_query.order_by(desc(SystemLog.log_time))
            system_logs_query = system_logs_query.limit(limit).offset(offset)
            
            # 执行查询
            async with db_manager.AsyncSessionLocal() as session:
                # 查询操作日志
                operation_result = await session.execute(operation_logs_query)
                operation_logs = operation_result.scalars().all()
                
                # 查询系统日志
                system_result = await session.execute(system_logs_query)
                system_logs = system_result.scalars().all()
            
            # 格式化操作日志
            for log in operation_logs:
                logs.append({
                    "id": log.id,
                    "type": "operation",
                    "timestamp": log.operation_time.isoformat() if log.operation_time else None,
                    "level": "info" if log.success else "error",
                    "category": log.category or "operation",
                    "operation_type": log.operation_type.value if log.operation_type else None,
                    "resource_type": log.resource_type,
                    "resource_id": log.resource_id,
                    "resource_name": log.resource_name,
                    "user_id": log.user_id,
                    "username": log.username,
                    "operation_name": log.operation_name,
                    "operation_description": log.operation_description,
                    "success": log.success,
                    "result_message": log.result_message,
                    "error_message": log.error_message,
                    "ip_address": log.ip_address,
                    "duration_ms": log.duration_ms,
                    "details": {
                        "request_method": log.request_method,
                        "request_url": log.request_url,
                        "response_status": log.response_status,
                        "old_values": log.old_values,
                        "new_values": log.new_values
                    }
                })
            
            # 格式化系统日志
            for log in system_logs:
                logs.append({
                    "id": log.id,
                    "type": "system",
                    "timestamp": log.log_time.isoformat() if log.log_time else None,
                    "level": log.log_level.value if log.log_level else "info",
                    "category": log.category.value if log.category else "system",
                    "message": log.message,
                    "module": log.module,
                    "function": log.function,
                    "file_path": log.file_path,
                    "line_number": log.line_number,
                    "user_id": log.user_id,
                    "task_id": log.task_id,
                    "details": log.details,
                    "exception_type": log.exception_type,
                    "stack_trace": log.stack_trace,
                    "duration_ms": log.duration_ms,
                    "memory_usage_mb": log.memory_usage_mb,
                    "cpu_usage_percent": log.cpu_usage_percent
                })
        
        # 按时间排序（最新的在前）
        logs.sort(key=lambda x: x.get("timestamp") or "", reverse=True)
        
        # 限制返回数量
        logs = logs[:limit]

        return {
            "success": True,
            "total": len(logs),
            "logs": logs,
            "pagination": {
            "limit": limit,
                "offset": offset,
                "has_more": len(logs) == limit
            }
        }

    except Exception as e:
        logger.error(f"获取系统日志失败: {str(e)}", exc_info=True)
        raise HTTPException(status_code=500, detail=str(e))

