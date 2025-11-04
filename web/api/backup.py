#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
备份管理API
Backup Management API
"""

import logging
import traceback
from typing import List, Dict, Any, Optional
from datetime import datetime, timedelta
from fastapi import APIRouter, HTTPException, Depends, BackgroundTasks, Request
from pydantic import BaseModel, Field

from models.backup import BackupTask, BackupTaskType, BackupTaskStatus
from config.settings import get_settings
from utils.logger import get_logger
from utils.scheduler.db_utils import is_opengauss, get_opengauss_connection
from models.system_log import OperationType, LogCategory, LogLevel
from utils.log_utils import log_operation, log_system

logger = get_logger(__name__)
router = APIRouter()
settings = get_settings()


def get_system_instance(request: Request):
    """获取系统实例"""
    return request.app.state.system


class BackupTaskRequest(BaseModel):
    """备份任务请求模型（创建模板配置）"""
    task_name: str = Field(..., description="任务名称")
    source_paths: List[str] = Field(..., description="源路径列表")
    task_type: BackupTaskType = Field(BackupTaskType.FULL, description="任务类型")
    exclude_patterns: List[str] = Field(default_factory=list, description="排除模式")
    compression_enabled: bool = Field(True, description="是否启用压缩")
    encryption_enabled: bool = Field(False, description="是否启用加密")
    retention_days: int = Field(180, description="保留天数")
    description: str = Field("", description="任务描述")
    tape_device: Optional[str] = Field(None, description="目标磁带机设备（可选）")


class BackupTaskResponse(BaseModel):
    """备份任务响应模型"""
    task_id: int
    task_name: str
    task_type: str
    status: str
    progress_percent: float
    total_files: int
    processed_files: int
    total_bytes: int
    processed_bytes: int
    created_at: datetime
    started_at: Optional[datetime]
    completed_at: Optional[datetime]
    error_message: Optional[str]


@router.post("/tasks", response_model=Dict[str, Any])
async def create_backup_task(
    request: BackupTaskRequest,
    http_request: Request
):
    """创建备份任务配置（模板）
    
    此接口创建备份任务配置模板，不立即执行。
    创建的模板可以在计划任务模块中选择并执行。
    """
    start_time = datetime.now()
    
    try:
        # 获取系统实例
        system = get_system_instance(http_request)
        if not system:
            raise HTTPException(status_code=500, detail="系统未初始化")

        # 验证源路径
        for path in request.source_paths:
            if not path or not path.strip():
                raise HTTPException(status_code=400, detail="源路径不能为空")

        import json
        
        # 创建备份任务模板
        if is_opengauss():
            # 使用原生SQL插入
            conn = await get_opengauss_connection()
            try:
                task_id = await conn.fetchval(
                    """
                    INSERT INTO backup_tasks (
                        task_name, task_type, status, is_template, source_paths, exclude_patterns,
                        compression_enabled, encryption_enabled, retention_days, description,
                        tape_device, created_at, updated_at, created_by
                    ) VALUES (
                        $1, CAST($2 AS backuptasktype), CAST($3 AS backuptaskstatus), $4, $5, $6,
                        $7, $8, $9, $10,
                        $11, $12, $13, $14
                    )
                    RETURNING id
                    """,
                    request.task_name,
                    request.task_type.value,
                    BackupTaskStatus.PENDING.value,
                    True,  # is_template
                    json.dumps(request.source_paths) if request.source_paths else None,
                    json.dumps(request.exclude_patterns) if request.exclude_patterns else None,
                    request.compression_enabled,
                    request.encryption_enabled,
                    request.retention_days,
                    request.description,
                    request.tape_device,
                    datetime.now(),
                    datetime.now(),
                    'backup_api'
                )
                
                # 记录操作日志
                client_ip = http_request.client.host if http_request.client else None
                duration_ms = int((datetime.now() - start_time).total_seconds() * 1000)
                await log_operation(
                    operation_type=OperationType.CREATE,
                    resource_type="backup",
                    resource_id=str(task_id),
                    resource_name=f"备份任务模板: {request.task_name}",
                    operation_name="创建备份任务模板",
                    operation_description=f"创建备份任务配置模板: {request.task_name}",
                    category="backup",
                    success=True,
                    result_message="备份任务配置已创建",
                    new_values={
                        "task_name": request.task_name,
                        "task_type": request.task_type.value,
                        "source_paths": request.source_paths,
                        "tape_device": request.tape_device
                    },
                    ip_address=client_ip,
                    request_method="POST",
                    request_url=str(http_request.url),
                    duration_ms=duration_ms
                )
                
                return {
                    "success": True,
                    "task_id": task_id,
                    "message": "备份任务配置已创建",
                    "task_name": request.task_name,
                    "is_template": True
                }
            finally:
                await conn.close()
        else:
            # 使用SQLAlchemy插入
            from config.database import db_manager
            
            async with db_manager.AsyncSessionLocal() as session:
                backup_task = BackupTask(
                    task_name=request.task_name,
                    task_type=request.task_type,
                    source_paths=request.source_paths,
                    exclude_patterns=request.exclude_patterns,
                    compression_enabled=request.compression_enabled,
                    encryption_enabled=request.encryption_enabled,
                    retention_days=request.retention_days,
                    description=request.description,
                    tape_device=request.tape_device,
                    status=BackupTaskStatus.PENDING,
                    is_template=True,
                    created_by='backup_api'
                )
                
                session.add(backup_task)
                await session.commit()
                await session.refresh(backup_task)
                
                # 记录操作日志
                client_ip = http_request.client.host if http_request.client else None
                duration_ms = int((datetime.now() - start_time).total_seconds() * 1000)
                await log_operation(
                    operation_type=OperationType.CREATE,
                    resource_type="backup",
                    resource_id=str(backup_task.id),
                    resource_name=f"备份任务模板: {request.task_name}",
                    operation_name="创建备份任务模板",
                    operation_description=f"创建备份任务配置模板: {request.task_name}",
                    category="backup",
                    success=True,
                    result_message="备份任务配置已创建",
                    new_values={
                        "task_name": request.task_name,
                        "task_type": request.task_type.value,
                        "source_paths": request.source_paths,
                        "tape_device": request.tape_device
                    },
                    ip_address=client_ip,
                    request_method="POST",
                    request_url=str(http_request.url),
                    duration_ms=duration_ms
                )
                
                return {
                    "success": True,
                    "task_id": backup_task.id,
                    "message": "备份任务配置已创建",
                    "task_name": backup_task.task_name,
                    "is_template": True
                }

    except HTTPException:
        raise
    except Exception as e:
        error_msg = str(e)
        duration_ms = int((datetime.now() - start_time).total_seconds() * 1000)
        
        # 记录失败的操作日志
        client_ip = http_request.client.host if http_request.client else None
        await log_operation(
            operation_type=OperationType.CREATE,
            resource_type="backup",
            resource_name=f"备份任务模板: {request.task_name}",
            operation_name="创建备份任务模板",
            operation_description=f"创建备份任务配置模板失败: {error_msg}",
            category="backup",
            success=False,
            error_message=error_msg,
            ip_address=client_ip,
            request_method="POST",
            request_url=str(http_request.url),
            duration_ms=duration_ms
        )
        
        logger.error(f"创建备份任务配置失败: {error_msg}", exc_info=True)
        raise HTTPException(status_code=500, detail=error_msg)


@router.get("/tasks", response_model=List[BackupTaskResponse])
async def get_backup_tasks(
    status: Optional[str] = None,
    task_type: Optional[str] = None,
    q: Optional[str] = None,
    limit: int = 50,
    offset: int = 0,
    http_request: Request = None
):
    """获取备份任务列表（执行记录）
    
    此接口返回所有备份任务的执行记录，包括：
    - 通过计划任务模块创建的备份任务
    - 通过备份管理模块立即执行的备份任务
    """
    try:
        if is_opengauss():
            # 使用原生SQL查询
            conn = await get_opengauss_connection()
            try:
                # 构建WHERE子句
                where_clauses = []
                params = []
                param_index = 1
                
                # 默认返回所有记录（模板+执行记录）；当 status/task_type 为 'all' 或空时不加过滤
                normalized_status = (status or '').lower()
                include_not_run = normalized_status in ('not_run', '未运行')
                if status and normalized_status not in ('all', 'not_run', '未运行'):
                    # 以文本方式匹配，避免依赖枚举类型存在
                    where_clauses.append(f"LOWER(status::text) = LOWER(${param_index})")
                    params.append(status)
                    param_index += 1
                # 未运行：仅限从 backup_tasks 侧筛选“未启动”的pending记录
                if include_not_run:
                    where_clauses.append("(started_at IS NULL) AND LOWER(status::text)=LOWER('PENDING')")
                
                normalized_type = (task_type or '').lower()
                if task_type and normalized_type != 'all':
                    # 以文本方式匹配，避免依赖枚举类型存在
                    where_clauses.append(f"LOWER(task_type::text) = LOWER(${param_index})")
                    params.append(task_type)
                    param_index += 1

                if q and q.strip():
                    where_clauses.append(f"task_name ILIKE ${param_index}")
                    params.append(f"%{q.strip()}%")
                    param_index += 1
                
                where_sql = " AND ".join(where_clauses) if where_clauses else "1=1"
                
                # 构建查询（包含模板与执行记录）- 不在SQL层做分页，合并后在内存分页
                sql = f"""
                    SELECT id, task_name, task_type, status, progress_percent, total_files, 
                           processed_files, total_bytes, processed_bytes, created_at, started_at, 
                           completed_at, error_message, is_template, tape_device, source_paths
                    FROM backup_tasks
                    WHERE {where_sql}
                    ORDER BY created_at DESC
                """
                rows = await conn.fetch(sql, *params)
                
                # 转换为响应格式
                import json
                tasks = []
                for row in rows:
                    # 解析JSON字段
                    source_paths = None
                    if row["source_paths"]:
                        try:
                            if isinstance(row["source_paths"], str):
                                source_paths = json.loads(row["source_paths"])
                            else:
                                source_paths = row["source_paths"]
                        except:
                            source_paths = None
                    
                    tasks.append({
                        "task_id": row["id"],
                        "task_name": row["task_name"],
                        "task_type": row["task_type"].value if hasattr(row["task_type"], "value") else str(row["task_type"]),
                        "status": row["status"].value if hasattr(row["status"], "value") else str(row["status"]),
                        "progress_percent": float(row["progress_percent"]) if row["progress_percent"] else 0.0,
                        "total_files": row["total_files"] or 0,
                        "processed_files": row["processed_files"] or 0,
                        "total_bytes": row["total_bytes"] or 0,
                        "processed_bytes": row["processed_bytes"] or 0,
                        "created_at": row["created_at"],
                        "started_at": row["started_at"],
                        "completed_at": row["completed_at"],
                        "error_message": row["error_message"],
                        "is_template": row["is_template"] or False,
                        "tape_device": row["tape_device"],
                        "source_paths": source_paths
                    })
                # 追加计划任务（未运行模板）
                # 仅当无状态过滤或过滤为pending/all时返回
                include_sched = (not status) or (normalized_status in ("all", "pending", 'not_run', '未运行'))
                if include_sched:
                    sched_where = ["LOWER(action_type::text)=LOWER('BACKUP')"]
                    sched_params = []
                    if q and q.strip():
                        sched_where.append("task_name ILIKE $1")
                        sched_params.append(f"%{q.strip()}%")
                    # 任务类型筛选
                    if task_type and normalized_type != 'all':
                        # 从 action_config->task_type 里匹配（字符串包含）
                        # openGauss json 提取可后续增强，这里简化为 ILIKE 检测
                        if sched_params:
                            sched_where.append("action_config ILIKE $2")
                            sched_params.append(f"%\"task_type\": \"{task_type}\"%")
                        else:
                            sched_where.append("action_config ILIKE $1")
                            sched_params.append(f"%\"task_type\": \"{task_type}\"%")
                    # 未运行：计划任务自然视作未运行
                    sched_sql = f"""
                        SELECT id, task_name, status, created_at, started_at, completed_at, action_config
                        FROM scheduled_tasks
                        WHERE {' AND '.join(sched_where)}
                        ORDER BY created_at DESC
                    """
                    sched_rows = await conn.fetch(sched_sql, *sched_params)
                    for srow in sched_rows:
                        # 从action_config中提取task_type/tape_device
                        atype = 'full'
                        tdev = None
                        try:
                            acfg = srow["action_config"]
                            if isinstance(acfg, str):
                                acfg = json.loads(acfg)
                            if isinstance(acfg, dict):
                                atype = acfg.get('task_type') or atype
                                tdev = acfg.get('tape_device')
                        except:
                            pass
                        tasks.append({
                            "task_id": srow["id"],
                            "task_name": srow["task_name"],
                            "task_type": atype,
                            "status": "pending",  # 计划任务视为未运行
                            "progress_percent": 0.0,
                            "total_files": 0,
                            "processed_files": 0,
                            "total_bytes": 0,
                            "processed_bytes": 0,
                            "created_at": srow["created_at"],
                            "started_at": None,
                            "completed_at": None,
                            "error_message": None,
                            "is_template": True,
                            "tape_device": tdev,
                            "source_paths": None
                        })
                # 合并后排序与分页
                tasks.sort(key=lambda x: x.get('created_at') or datetime.min, reverse=True)
                return tasks[offset:offset+limit]
            finally:
                await conn.close()
        else:
            # 使用SQLAlchemy查询
            from config.database import db_manager
            from sqlalchemy import select, desc
            
            async with db_manager.AsyncSessionLocal() as session:
                # 构建查询
                stmt = select(BackupTask)
                
                if status and status.lower() != 'all':
                    try:
                        status_enum = BackupTaskStatus(status)
                        stmt = stmt.where(BackupTask.status == status_enum)
                    except ValueError:
                        pass
                
                if task_type and task_type.lower() != 'all':
                    try:
                        task_type_enum = BackupTaskType(task_type)
                        stmt = stmt.where(BackupTask.task_type == task_type_enum)
                    except ValueError:
                        pass
                if q and q.strip():
                    from sqlalchemy import or_
                    stmt = stmt.where(BackupTask.task_name.ilike(f"%{q.strip()}%"))
                
                # 按创建时间倒序排列
                stmt = stmt.order_by(desc(BackupTask.created_at))
                
                # 应用分页
                stmt = stmt.limit(limit).offset(offset)
                
                result = await session.execute(stmt)
                backup_tasks = result.scalars().all()
                
                # 转换为响应格式
                tasks = []
                for task in backup_tasks:
                    tasks.append({
                        "task_id": task.id,
                        "task_name": task.task_name,
                        "task_type": task.task_type.value,
                        "status": task.status.value,
                        "progress_percent": task.progress_percent or 0.0,
                        "total_files": task.total_files or 0,
                        "processed_files": task.processed_files or 0,
                        "total_bytes": task.total_bytes or 0,
                        "processed_bytes": task.processed_bytes or 0,
                        "created_at": task.created_at,
                        "started_at": task.started_at,
                        "completed_at": task.completed_at,
                        "error_message": task.error_message,
                        "is_template": task.is_template or False,
                        "tape_device": task.tape_device,
                        "source_paths": task.source_paths
                    })
                
                return tasks

    except Exception as e:
        logger.error(f"获取备份任务列表失败: {str(e)}", exc_info=True)
        raise HTTPException(status_code=500, detail=str(e))


@router.get("/tasks/{task_id}", response_model=BackupTaskResponse)
async def get_backup_task(task_id: int, http_request: Request):
    """获取备份任务详情"""
    try:
        if is_opengauss():
            # 使用原生SQL查询
            conn = await get_opengauss_connection()
            try:
                row = await conn.fetchrow(
                    """
                    SELECT id, task_name, task_type, status, progress_percent, total_files, 
                           processed_files, total_bytes, processed_bytes, created_at, started_at, 
                           completed_at, error_message, is_template, tape_device, source_paths
                    FROM backup_tasks
                    WHERE id = $1
                    """,
                    task_id
                )
                
                if not row:
                    raise HTTPException(status_code=404, detail="备份任务不存在")
                
                # 解析JSON字段
                import json
                source_paths = None
                if row["source_paths"]:
                    try:
                        if isinstance(row["source_paths"], str):
                            source_paths = json.loads(row["source_paths"])
                        else:
                            source_paths = row["source_paths"]
                    except:
                        source_paths = None
                
                return {
                    "task_id": row["id"],
                    "task_name": row["task_name"],
                    "task_type": row["task_type"].value if hasattr(row["task_type"], "value") else str(row["task_type"]),
                    "status": row["status"].value if hasattr(row["status"], "value") else str(row["status"]),
                    "progress_percent": float(row["progress_percent"]) if row["progress_percent"] else 0.0,
                    "total_files": row["total_files"] or 0,
                    "processed_files": row["processed_files"] or 0,
                    "total_bytes": row["total_bytes"] or 0,
                    "processed_bytes": row["processed_bytes"] or 0,
                    "created_at": row["created_at"],
                    "started_at": row["started_at"],
                    "completed_at": row["completed_at"],
                    "error_message": row["error_message"],
                    "is_template": row["is_template"] or False,
                    "tape_device": row["tape_device"],
                    "source_paths": source_paths
                }
            finally:
                await conn.close()
        else:
            # 使用SQLAlchemy查询
            from config.database import db_manager
            from sqlalchemy import select
            
            async with db_manager.AsyncSessionLocal() as session:
                stmt = select(BackupTask).where(BackupTask.id == task_id)
                result = await session.execute(stmt)
                task = result.scalar_one_or_none()
                
                if not task:
                    raise HTTPException(status_code=404, detail="备份任务不存在")
                
                return {
                    "task_id": task.id,
                    "task_name": task.task_name,
                    "task_type": task.task_type.value,
                    "status": task.status.value,
                    "progress_percent": task.progress_percent or 0.0,
                    "total_files": task.total_files or 0,
                    "processed_files": task.processed_files or 0,
                    "total_bytes": task.total_bytes or 0,
                    "processed_bytes": task.processed_bytes or 0,
                    "created_at": task.created_at,
                    "started_at": task.started_at,
                    "completed_at": task.completed_at,
                    "error_message": task.error_message,
                    "is_template": task.is_template or False,
                    "tape_device": task.tape_device,
                    "source_paths": task.source_paths
                }

    except HTTPException:
        raise
    except Exception as e:
        logger.error(f"获取备份任务详情失败: {str(e)}", exc_info=True)
        raise HTTPException(status_code=500, detail=str(e))


@router.get("/templates", response_model=List[Dict[str, Any]])
async def get_backup_templates(
    limit: int = 50,
    offset: int = 0,
    http_request: Request = None
):
    """获取备份任务模板列表（配置）
    
    返回所有备份任务配置模板，供计划任务模块选择。
    """
    try:
        from config.database import db_manager
        from sqlalchemy import select, desc
        from models.backup import BackupTask
        
        async with db_manager.AsyncSessionLocal() as session:
            # 查询所有模板
            stmt = select(BackupTask).where(BackupTask.is_template == True)
            stmt = stmt.order_by(desc(BackupTask.created_at))
            stmt = stmt.limit(limit).offset(offset)
            
            result = await session.execute(stmt)
            templates = result.scalars().all()
            
            # 转换为响应格式
            template_list = []
            for template in templates:
                template_list.append({
                    "task_id": template.id,
                    "task_name": template.task_name,
                    "task_type": template.task_type.value,
                    "description": template.description,
                    "source_paths": template.source_paths or [],
                    "tape_device": template.tape_device,
                    "compression_enabled": template.compression_enabled,
                    "encryption_enabled": template.encryption_enabled,
                    "retention_days": template.retention_days,
                    "exclude_patterns": template.exclude_patterns or [],
                    "created_at": template.created_at
                })
            
            return template_list

    except Exception as e:
        logger.error(f"获取备份任务模板列表失败: {str(e)}")
        raise HTTPException(status_code=500, detail=str(e))


@router.put("/tasks/{task_id}/cancel")
async def cancel_backup_task(task_id: int, http_request: Request):
    """取消备份任务（仅限执行记录）"""
    start_time = datetime.now()
    
    try:
        # 获取系统实例
        system = get_system_instance(http_request)
        if not system:
            raise HTTPException(status_code=500, detail="系统未初始化")

        # 先获取任务信息用于日志
        task_info = None
        if is_opengauss():
            conn = await get_opengauss_connection()
            try:
                row = await conn.fetchrow(
                    "SELECT task_name, status FROM backup_tasks WHERE id = $1",
                    task_id
                )
                if row:
                    task_info = {
                        "task_name": row["task_name"],
                        "status": row["status"].value if hasattr(row["status"], "value") else str(row["status"])
                    }
            finally:
                await conn.close()
        else:
            from config.database import db_manager
            from sqlalchemy import select
            async with db_manager.AsyncSessionLocal() as session:
                stmt = select(BackupTask).where(BackupTask.id == task_id)
                result = await session.execute(stmt)
                task = result.scalar_one_or_none()
                if task:
                    task_info = {
                        "task_name": task.task_name,
                        "status": task.status.value
                    }

        success = await system.backup_engine.cancel_task(task_id)
        
        # 记录操作日志
        client_ip = http_request.client.host if http_request.client else None
        duration_ms = int((datetime.now() - start_time).total_seconds() * 1000)
        
        if success:
            await log_operation(
                operation_type=OperationType.EXECUTE,
                resource_type="backup",
                resource_id=str(task_id),
                resource_name=f"备份任务: {task_info.get('task_name') if task_info else '未知'}" if task_info else f"备份任务: {task_id}",
                operation_name="取消备份任务",
                operation_description=f"取消备份任务: {task_info.get('task_name') if task_info else task_id}",
                category="backup",
                success=True,
                result_message="任务已取消",
                ip_address=client_ip,
                request_method="PUT",
                request_url=str(http_request.url),
                duration_ms=duration_ms
            )
            
            return {"success": True, "message": "任务已取消"}
        else:
            await log_operation(
                operation_type=OperationType.EXECUTE,
                resource_type="backup",
                resource_id=str(task_id),
                resource_name=f"备份任务: {task_info.get('task_name') if task_info else '未知'}" if task_info else f"备份任务: {task_id}",
                operation_name="取消备份任务",
                operation_description=f"取消备份任务失败: 任务不存在或无法取消",
                category="backup",
                success=False,
                error_message="任务不存在或无法取消",
                ip_address=client_ip,
                request_method="PUT",
                request_url=str(http_request.url),
                duration_ms=duration_ms
            )
            
            raise HTTPException(status_code=404, detail="任务不存在或无法取消")

    except HTTPException:
        raise
    except Exception as e:
        error_msg = str(e)
        duration_ms = int((datetime.now() - start_time).total_seconds() * 1000)
        
        # 记录失败的操作日志
        client_ip = http_request.client.host if http_request.client else None
        await log_operation(
            operation_type=OperationType.EXECUTE,
            resource_type="backup",
            resource_id=str(task_id),
            resource_name=f"备份任务: {task_id}",
            operation_name="取消备份任务",
            operation_description=f"取消备份任务失败: {error_msg}",
            category="backup",
            success=False,
            error_message=error_msg,
            ip_address=client_ip,
            request_method="PUT",
            request_url=str(http_request.url),
            duration_ms=duration_ms
        )
        
        logger.error(f"取消备份任务失败: {error_msg}", exc_info=True)
        raise HTTPException(status_code=500, detail=error_msg)


@router.get("/tasks/{task_id}/status")
async def get_task_status(task_id: int, http_request: Request = None):
    """获取任务状态"""
    try:
        # 获取系统实例
        system = get_system_instance(http_request)
        if not system:
            raise HTTPException(status_code=500, detail="系统未初始化")

        status = await system.backup_engine.get_task_status(task_id)
        if status:
            return status
        else:
            raise HTTPException(status_code=404, detail="任务不存在")

    except HTTPException:
        raise
    except Exception as e:
        logger.error(f"获取任务状态失败: {str(e)}")
        raise HTTPException(status_code=500, detail=str(e))


@router.get("/statistics")
async def get_backup_statistics(http_request: Request):
    """获取备份统计信息（使用真实数据）"""
    try:
        if is_opengauss():
            # 使用原生SQL查询
            conn = await get_opengauss_connection()
            try:
                # 总任务数（包含模板与执行记录）
                total_row = await conn.fetchrow("SELECT COUNT(*) as total FROM backup_tasks")
                total_tasks = total_row["total"] if total_row else 0
                # 计划任务中的备份任务数量（计入总任务与pending）
                sched_total_row = await conn.fetchrow(
                    "SELECT COUNT(*) as total FROM scheduled_tasks WHERE LOWER(action_type::text)=LOWER('BACKUP')"
                )
                sched_total = sched_total_row["total"] if sched_total_row else 0
                total_tasks += sched_total
                
                # 按状态统计（执行记录与模板均统计各自status）
                completed_row = await conn.fetchrow(
                    "SELECT COUNT(*) as total FROM backup_tasks WHERE is_template = false AND LOWER(status::text)=LOWER($1)",
                    BackupTaskStatus.COMPLETED.value
                )
                completed_tasks = completed_row["total"] if completed_row else 0
                
                failed_row = await conn.fetchrow(
                    "SELECT COUNT(*) as total FROM backup_tasks WHERE is_template = false AND LOWER(status::text)=LOWER($1)",
                    BackupTaskStatus.FAILED.value
                )
                failed_tasks = failed_row["total"] if failed_row else 0
                
                running_row = await conn.fetchrow(
                    "SELECT COUNT(*) as total FROM backup_tasks WHERE is_template = false AND LOWER(status::text)=LOWER($1)",
                    BackupTaskStatus.RUNNING.value
                )
                running_tasks = running_row["total"] if running_row else 0
                
                pending_row = await conn.fetchrow(
                    "SELECT COUNT(*) as total FROM backup_tasks WHERE LOWER(status::text)=LOWER($1)",
                    BackupTaskStatus.PENDING.value
                )
                pending_tasks = (pending_row["total"] if pending_row else 0) + sched_total
                
                # 成功率
                success_rate = (completed_tasks / total_tasks * 100) if total_tasks > 0 else 0.0
                
                # 总备份数据量
                bytes_row = await conn.fetchrow(
                    """
                    SELECT COALESCE(SUM(processed_bytes), 0) as total 
                    FROM backup_tasks 
                    WHERE is_template = false AND LOWER(status::text)=LOWER($1)
                    """,
                    BackupTaskStatus.COMPLETED.value
                )
                total_data_backed_up = bytes_row["total"] if bytes_row else 0
                
                # 最近24小时统计
                twenty_four_hours_ago = datetime.now() - timedelta(hours=24)
                recent_row = await conn.fetchrow(
                    """
                    SELECT 
                        COUNT(*) as total,
                        COUNT(*) FILTER (WHERE LOWER(status::text)=LOWER($1)) as completed,
                        COUNT(*) FILTER (WHERE LOWER(status::text)=LOWER($2)) as failed,
                        COALESCE(SUM(processed_bytes), 0) as data
                    FROM backup_tasks
                    WHERE is_template = false AND created_at >= $3
                    """,
                    BackupTaskStatus.COMPLETED.value,
                    BackupTaskStatus.FAILED.value,
                    twenty_four_hours_ago
                )
                
                recent_total = recent_row["total"] if recent_row else 0
                recent_completed = recent_row["completed"] if recent_row else 0
                recent_failed = recent_row["failed"] if recent_row else 0
                recent_data = recent_row["data"] if recent_row else 0
                
                # 平均任务时长（从completed_at - started_at计算）
                avg_duration_row = await conn.fetchrow(
                    """
                    SELECT AVG(EXTRACT(EPOCH FROM (completed_at - started_at))) as avg_duration
                    FROM backup_tasks
                    WHERE is_template = false 
                      AND LOWER(status::text)=LOWER($1)
                      AND completed_at IS NOT NULL 
                      AND started_at IS NOT NULL
                    """,
                    BackupTaskStatus.COMPLETED.value
                )
                avg_duration = int(avg_duration_row["avg_duration"]) if avg_duration_row and avg_duration_row["avg_duration"] else 3600
                
                # 压缩比（从compressed_bytes和processed_bytes计算）
                compression_row = await conn.fetchrow(
                    """
                    SELECT 
                        COALESCE(SUM(processed_bytes), 0) as processed,
                        COALESCE(SUM(compressed_bytes), 0) as compressed
                    FROM backup_tasks
                    WHERE is_template = false 
                      AND LOWER(status::text)=LOWER($1)
                      AND compressed_bytes > 0
                    """,
                    BackupTaskStatus.COMPLETED.value
                )
                if compression_row and compression_row["processed"] > 0:
                    compression_ratio = float(compression_row["compressed"]) / float(compression_row["processed"])
                else:
                    compression_ratio = 0.65  # 默认值
                
                return {
                    "total_tasks": total_tasks,
                    "completed_tasks": completed_tasks,
                    "failed_tasks": failed_tasks,
                    "running_tasks": running_tasks,
                    "pending_tasks": pending_tasks,
                    "success_rate": round(success_rate, 2),
                    "total_data_backed_up": total_data_backed_up,
                    "compression_ratio": round(compression_ratio, 2),
                    "average_task_duration": avg_duration,
                    "recent_24h": {
                        "total_tasks": recent_total,
                        "completed_tasks": recent_completed,
                        "failed_tasks": recent_failed,
                        "data_backed_up": recent_data
                    }
                }
            finally:
                await conn.close()
        else:
            # 使用SQLAlchemy查询
            from config.database import db_manager
            from sqlalchemy import select, func, and_
            
            async with db_manager.AsyncSessionLocal() as session:
                # 总任务数（只查询非模板任务）
                total_stmt = select(func.count(BackupTask.id)).where(BackupTask.is_template == False)
                total_result = await session.execute(total_stmt)
                total_tasks = total_result.scalar() or 0
                
                # 按状态统计
                completed_stmt = select(func.count(BackupTask.id)).where(
                    BackupTask.is_template == False,
                    BackupTask.status == BackupTaskStatus.COMPLETED
                )
                completed_result = await session.execute(completed_stmt)
                completed_tasks = completed_result.scalar() or 0
                
                failed_stmt = select(func.count(BackupTask.id)).where(
                    BackupTask.is_template == False,
                    BackupTask.status == BackupTaskStatus.FAILED
                )
                failed_result = await session.execute(failed_stmt)
                failed_tasks = failed_result.scalar() or 0
                
                running_stmt = select(func.count(BackupTask.id)).where(
                    BackupTask.is_template == False,
                    BackupTask.status == BackupTaskStatus.RUNNING
                )
                running_result = await session.execute(running_stmt)
                running_tasks = running_result.scalar() or 0
                
                pending_stmt = select(func.count(BackupTask.id)).where(
                    BackupTask.is_template == False,
                    BackupTask.status == BackupTaskStatus.PENDING
                )
                pending_result = await session.execute(pending_stmt)
                pending_tasks = pending_result.scalar() or 0
                
                # 成功率
                success_rate = (completed_tasks / total_tasks * 100) if total_tasks > 0 else 0.0
                
                # 总备份数据量
                bytes_stmt = select(func.sum(BackupTask.processed_bytes)).where(
                    BackupTask.is_template == False,
                    BackupTask.status == BackupTaskStatus.COMPLETED
                )
                bytes_result = await session.execute(bytes_stmt)
                total_data_backed_up = bytes_result.scalar() or 0
                
                # 最近24小时统计
                twenty_four_hours_ago = datetime.now() - timedelta(hours=24)
                recent_stmt = select(
                    func.count(BackupTask.id),
                    func.sum(func.case((BackupTask.status == BackupTaskStatus.COMPLETED, 1), else_=0)),
                    func.sum(func.case((BackupTask.status == BackupTaskStatus.FAILED, 1), else_=0)),
                    func.sum(BackupTask.processed_bytes)
                ).where(
                    BackupTask.is_template == False,
                    BackupTask.created_at >= twenty_four_hours_ago
                )
                recent_result = await session.execute(recent_stmt)
                recent_row = recent_result.first()
                
                recent_total = recent_row[0] or 0
                recent_completed = recent_row[1] or 0
                recent_failed = recent_row[2] or 0
                recent_data = recent_row[3] or 0
                
                # 平均任务时长
                avg_duration_stmt = select(
                    func.avg(func.extract('epoch', BackupTask.completed_at - BackupTask.started_at))
                ).where(
                    BackupTask.is_template == False,
                    BackupTask.status == BackupTaskStatus.COMPLETED,
                    BackupTask.completed_at.isnot(None),
                    BackupTask.started_at.isnot(None)
                )
                avg_duration_result = await session.execute(avg_duration_stmt)
                avg_duration = int(avg_duration_result.scalar() or 3600)
                
                # 压缩比
                compression_stmt = select(
                    func.sum(BackupTask.processed_bytes),
                    func.sum(BackupTask.compressed_bytes)
                ).where(
                    BackupTask.is_template == False,
                    BackupTask.status == BackupTaskStatus.COMPLETED,
                    BackupTask.compressed_bytes > 0
                )
                compression_result = await session.execute(compression_stmt)
                compression_row = compression_result.first()
                if compression_row and compression_row[0] and compression_row[0] > 0:
                    compression_ratio = float(compression_row[1] or 0) / float(compression_row[0])
                else:
                    compression_ratio = 0.65
                
                return {
                    "total_tasks": total_tasks,
                    "completed_tasks": completed_tasks,
                    "failed_tasks": failed_tasks,
                    "running_tasks": running_tasks,
                    "pending_tasks": pending_tasks,
                    "success_rate": round(success_rate, 2),
                    "total_data_backed_up": total_data_backed_up,
                    "compression_ratio": round(compression_ratio, 2),
                    "average_task_duration": avg_duration,
                    "recent_24h": {
                        "total_tasks": recent_total,
                        "completed_tasks": recent_completed,
                        "failed_tasks": recent_failed,
                        "data_backed_up": recent_data
                    }
                }

    except Exception as e:
        logger.error(f"获取备份统计信息失败: {str(e)}", exc_info=True)
        raise HTTPException(status_code=500, detail=str(e))


@router.get("/backup-sets")
async def get_backup_sets(
    backup_group: Optional[str] = None,
    limit: int = 50,
    offset: int = 0
):
    """获取备份集列表"""
    try:
        from config.database import db_manager
        from models.backup import BackupSet
        
        async with db_manager.AsyncSessionLocal() as session:
            # 构建查询
            stmt = select(BackupSet)
            
            # 应用过滤条件
            if backup_group:
                stmt = stmt.where(BackupSet.backup_group == backup_group)
            
            # 按备份时间倒序排列
            stmt = stmt.order_by(desc(BackupSet.backup_time))
            
            # 获取总数
            count_stmt = select(func.count(BackupSet.id))
            if backup_group:
                count_stmt = count_stmt.where(BackupSet.backup_group == backup_group)
            count_result = await session.execute(count_stmt)
            total = count_result.scalar() or 0
            
            # 应用分页
            stmt = stmt.limit(limit).offset(offset)
            result = await session.execute(stmt)
            backup_sets = result.scalars().all()
            
            # 转换为响应格式
            sets_list = []
            for backup_set in backup_sets:
                sets_list.append({
                    "set_id": backup_set.set_id,
                    "set_name": backup_set.set_name,
                    "backup_group": backup_set.backup_group,
                    "backup_type": backup_set.backup_type.value,
                    "backup_time": backup_set.backup_time.isoformat() if backup_set.backup_time else None,
                    "total_files": backup_set.total_files or 0,
                    "total_bytes": backup_set.total_bytes or 0,
                    "tape_id": backup_set.tape_id,
                    "status": backup_set.status.value if backup_set.status else "active"
                })
            
            return {
                "backup_sets": sets_list,
                "total": total,
                "limit": limit,
                "offset": offset
            }

    except Exception as e:
        logger.error(f"获取备份集列表失败: {str(e)}")
        raise HTTPException(status_code=500, detail=str(e))