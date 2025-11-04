#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
日志记录工具函数
Logging utility functions for OperationLog and SystemLog
"""

import logging
import json
import traceback
from datetime import datetime
from typing import Optional, Dict, Any, List
from enum import Enum

from models.system_log import OperationLog, SystemLog, LogLevel, LogCategory, OperationType
from config.database import db_manager
from .scheduler.db_utils import is_opengauss, get_opengauss_connection

logger = logging.getLogger(__name__)


async def log_operation(
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
) -> bool:
    """记录操作日志
    
    Args:
        operation_type: 操作类型（OperationType枚举）
        resource_type: 资源类型（如'scheduler', 'tape', 'backup'等）
        resource_id: 资源ID（字符串）
        resource_name: 资源名称
        operation_name: 操作名称
        operation_description: 操作描述
        category: 操作分类（如'scheduler', 'backup'等）
        user_id: 用户ID
        username: 用户名
        success: 是否成功
        result_message: 结果消息
        error_message: 错误消息
        duration_ms: 持续时间（毫秒）
        old_values: 修改前的值
        new_values: 修改后的值
        changed_fields: 变更的字段列表
        ip_address: IP地址
        request_method: 请求方法（GET/POST等）
        request_url: 请求URL
        **kwargs: 其他字段
        
    Returns:
        是否成功记录日志
    """
    try:
        operation_time = datetime.now()
        
        if is_opengauss():
            # 使用原生SQL插入操作日志
            conn = await get_opengauss_connection()
            try:
                # 构建SQL语句
                sql = """
                    INSERT INTO operation_logs (
                        user_id, username, operation_type, resource_type, resource_id, resource_name,
                        operation_name, operation_description, category, operation_time, duration_ms,
                        request_method, request_url, success, result_message, error_message,
                        old_values, new_values, changed_fields, ip_address
                    ) VALUES (
                        $1, $2, $3::operationtype, $4, $5, $6, $7, $8, $9, $10, $11,
                        $12, $13, $14, $15, $16, $17, $18, $19, $20
                    )
                """
                
                # 准备参数
                params = [
                    user_id,
                    username,
                    operation_type.value if isinstance(operation_type, Enum) else str(operation_type),
                    resource_type,
                    resource_id,
                    resource_name,
                    operation_name,
                    operation_description,
                    category,
                    operation_time,
                    duration_ms,
                    request_method,
                    request_url,
                    success,
                    result_message,
                    error_message,
                    json.dumps(old_values) if old_values else None,
                    json.dumps(new_values) if new_values else None,
                    json.dumps(changed_fields) if changed_fields else None,
                    ip_address
                ]
                
                await conn.execute(sql, *params)
                return True
            finally:
                await conn.close()
        else:
            # 使用SQLAlchemy插入操作日志
            async with db_manager.AsyncSessionLocal() as session:
                operation_log = OperationLog(
                    user_id=user_id,
                    username=username,
                    operation_type=operation_type,
                    resource_type=resource_type,
                    resource_id=str(resource_id) if resource_id else None,
                    resource_name=resource_name,
                    operation_name=operation_name,
                    operation_description=operation_description,
                    category=category or resource_type,
                    operation_time=operation_time,
                    duration_ms=duration_ms,
                    request_method=request_method,
                    request_url=request_url,
                    success=success,
                    result_message=result_message,
                    error_message=error_message,
                    old_values=old_values,
                    new_values=new_values,
                    changed_fields=changed_fields,
                    ip_address=ip_address
                )
                session.add(operation_log)
                await session.commit()
                return True
                
    except Exception as e:
        logger.error(f"记录操作日志失败: {str(e)}")
        logger.error(f"错误详情:\n{traceback.format_exc()}")
        return False


async def log_system(
    level: LogLevel,
    category: LogCategory,
    message: str,
    module: Optional[str] = None,
    function: Optional[str] = None,
    file_path: Optional[str] = None,
    line_number: Optional[int] = None,
    user_id: Optional[int] = None,
    task_id: Optional[int] = None,
    details: Optional[Dict[str, Any]] = None,
    exception_type: Optional[str] = None,
    stack_trace: Optional[str] = None,
    duration_ms: Optional[int] = None,
    memory_usage_mb: Optional[float] = None,
    cpu_usage_percent: Optional[float] = None,
    **kwargs
) -> bool:
    """记录系统日志
    
    Args:
        level: 日志级别（LogLevel枚举）
        category: 日志分类（LogCategory枚举）
        message: 日志消息
        module: 模块名
        function: 函数名
        file_path: 文件路径
        line_number: 行号
        user_id: 用户ID
        task_id: 任务ID
        details: 详细信息（字典）
        exception_type: 异常类型
        stack_trace: 堆栈跟踪
        duration_ms: 持续时间（毫秒）
        memory_usage_mb: 内存使用（MB）
        cpu_usage_percent: CPU使用率（百分比）
        **kwargs: 其他字段
        
    Returns:
        是否成功记录日志
    """
    try:
        log_time = datetime.now()
        
        if is_opengauss():
            # 使用原生SQL插入系统日志
            conn = await get_opengauss_connection()
            try:
                # 构建SQL语句
                sql = """
                    INSERT INTO system_logs (
                        log_level, category, message, module, function, file_path, line_number,
                        user_id, task_id, log_time, details, exception_type, stack_trace,
                        duration_ms, memory_usage_mb, cpu_usage_percent
                    ) VALUES (
                        $1::loglevel, $2::logcategory, $3, $4, $5, $6, $7,
                        $8, $9, $10, $11, $12, $13, $14, $15, $16
                    )
                """
                
                # 准备参数
                params = [
                    level.value if isinstance(level, Enum) else str(level),
                    category.value if isinstance(category, Enum) else str(category),
                    message,
                    module,
                    function,
                    file_path,
                    line_number,
                    user_id,
                    task_id,
                    log_time,
                    json.dumps(details) if details else None,
                    exception_type,
                    stack_trace,
                    duration_ms,
                    memory_usage_mb,
                    cpu_usage_percent
                ]
                
                await conn.execute(sql, *params)
                return True
            finally:
                await conn.close()
        else:
            # 使用SQLAlchemy插入系统日志
            async with db_manager.AsyncSessionLocal() as session:
                system_log = SystemLog(
                    log_level=level,
                    category=category,
                    message=message,
                    module=module,
                    function=function,
                    file_path=file_path,
                    line_number=line_number,
                    user_id=user_id,
                    task_id=task_id,
                    log_time=log_time,
                    details=details,
                    exception_type=exception_type,
                    stack_trace=stack_trace,
                    duration_ms=duration_ms,
                    memory_usage_mb=memory_usage_mb,
                    cpu_usage_percent=cpu_usage_percent
                )
                session.add(system_log)
                await session.commit()
                return True
                
    except Exception as e:
        logger.error(f"记录系统日志失败: {str(e)}")
        logger.error(f"错误详情:\n{traceback.format_exc()}")
        return False

