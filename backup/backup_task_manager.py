#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
备份任务管理模块
Backup Task Manager Module

负责备份任务的创建、状态查询和取消
"""

import asyncio
import logging
import json
from datetime import datetime
from typing import List, Optional, Dict
from pathlib import Path

from config.database import get_db
from config.settings import get_settings
from models.backup import BackupTask, BackupTaskStatus, BackupTaskType
from utils.network_path import validate_network_path

logger = logging.getLogger(__name__)


class BackupTaskManager:
    """备份任务管理器"""
    
    def __init__(self, settings=None):
        """初始化任务管理器
        
        Args:
            settings: 系统设置对象
        """
        self.settings = settings or get_settings()
        self._current_task: Optional[BackupTask] = None
    
    async def create_backup_task(self, task_name: str, source_paths: List[str],
                                  task_type: BackupTaskType = BackupTaskType.FULL,
                                  **kwargs) -> Optional[BackupTask]:
        """创建备份任务
        
        支持网络路径（UNC路径）：
        - \\192.168.0.79 - 自动列出所有共享
        - \\192.168.0.79\yz - 指定共享路径
        
        Args:
            task_name: 任务名称
            source_paths: 源路径列表
            task_type: 任务类型
            **kwargs: 其他参数
            
        Returns:
            BackupTask: 创建的备份任务对象，如果失败返回None
        """
        try:
            # 检查参数
            if not task_name or not source_paths:
                raise ValueError("任务名称和源路径不能为空")

            # 验证源路径（支持 UNC 网络路径）
            expanded_source_paths = []
            for path in source_paths:
                # 验证路径
                validation_result = validate_network_path(path)
                
                if not validation_result['valid']:
                    # 对于 UNC 路径，如果无法访问，给出更详细的错误信息
                    if validation_result['is_unc']:
                        error_msg = f"无法访问网络路径: {path}"
                        if validation_result['error']:
                            error_msg += f" ({validation_result['error']})"
                        raise ValueError(error_msg)
                    else:
                        raise ValueError(f"源路径不存在: {path}")
                
                # 如果是 UNC 路径且已展开，使用展开后的路径
                if validation_result['is_unc'] and validation_result['expanded_paths']:
                    expanded_source_paths.extend(validation_result['expanded_paths'])
                else:
                    expanded_source_paths.append(path)
            
            # 使用展开后的路径列表
            if expanded_source_paths:
                source_paths = expanded_source_paths
                logger.info(f"路径已展开，共 {len(source_paths)} 个路径")

            # 创建备份任务
            backup_task = BackupTask(
                task_name=task_name,
                task_type=task_type,
                source_paths=source_paths,
                exclude_patterns=kwargs.get('exclude_patterns', []),
                compression_enabled=kwargs.get('compression_enabled', True),
                encryption_enabled=kwargs.get('encryption_enabled', False),
                retention_days=kwargs.get('retention_days', self.settings.DEFAULT_RETENTION_MONTHS * 30),
                description=kwargs.get('description', ''),
                scheduled_time=kwargs.get('scheduled_time'),
                created_by=kwargs.get('created_by', 'system')
            )

            # 保存到数据库 - 使用原生 openGauss SQL
            from utils.scheduler.db_utils import is_opengauss, get_opengauss_connection
            
            if is_opengauss():
                # 使用连接池
                async with get_opengauss_connection() as conn:
                    # 插入备份任务
                    task_id = await conn.fetchval(
                        """
                        INSERT INTO backup_tasks 
                        (task_name, task_type, description, status, source_paths, exclude_patterns,
                         compression_enabled, encryption_enabled, retention_days, scheduled_time,
                         created_by, created_at, updated_at)
                        VALUES ($1, $2::backuptasktype, $3, $4::backuptaskstatus, $5::json, $6::json,
                                $7, $8, $9, $10, $11, $12, $13)
                        RETURNING id
                        """,
                        task_name,
                        task_type.value,
                        kwargs.get('description', ''),
                        'PENDING',  # BackupTaskStatus.PENDING
                        json.dumps(source_paths) if source_paths else None,
                        json.dumps(kwargs.get('exclude_patterns', [])) if kwargs.get('exclude_patterns') else None,
                        kwargs.get('compression_enabled', True),
                        kwargs.get('encryption_enabled', False),
                        kwargs.get('retention_days', self.settings.DEFAULT_RETENTION_MONTHS * 30),
                        kwargs.get('scheduled_time'),
                        kwargs.get('created_by', 'system'),
                        datetime.now(),
                        datetime.now()
                    )
                    backup_task.id = task_id
            else:
                # 非 openGauss 使用 SQLAlchemy（其他数据库）
                async for db in get_db():
                    db.add(backup_task)
                    await db.commit()
                    await db.refresh(backup_task)

            logger.info(f"创建备份任务成功: {task_name}")
            return backup_task

        except Exception as e:
            logger.error(f"创建备份任务失败: {str(e)}")
            return None
    
    async def get_task_status(self, task_id: int) -> Optional[Dict]:
        """获取任务状态
        
        Args:
            task_id: 任务ID
            
        Returns:
            Dict: 任务状态字典，如果任务不存在返回None
        """
        try:
            from utils.scheduler.db_utils import is_opengauss, get_opengauss_connection
            
            if is_opengauss():
                # 使用连接池
                async with get_opengauss_connection() as conn:
                    row = await conn.fetchrow(
                        """
                        SELECT id, task_name, task_type, status, progress_percent, 
                               total_files, total_bytes, processed_files, processed_bytes,
                               started_at, completed_at, error_message, result_summary,
                               source_paths, tape_device, tape_id, description
                        FROM backup_tasks
                        WHERE id = $1
                        """,
                        task_id
                    )
                    
                    if row:
                        # 解析 source_paths
                        source_paths = None
                        if row['source_paths']:
                            try:
                                if isinstance(row['source_paths'], str):
                                    source_paths = json.loads(row['source_paths'])
                                else:
                                    source_paths = row['source_paths']
                            except:
                                source_paths = None
                        
                        return {
                            'id': row['id'],
                            'task_name': row['task_name'],
                            'task_type': row['task_type'],
                            'status': row['status'],
                            'progress_percent': row['progress_percent'],
                            'total_files': row['total_files'],
                            'total_bytes': row['total_bytes'],
                            'processed_files': row['processed_files'],
                            'processed_bytes': row['processed_bytes'],
                            'started_at': row['started_at'],
                            'completed_at': row['completed_at'],
                            'error_message': row['error_message'],
                            'result_summary': row['result_summary'],
                            'source_paths': source_paths,
                            'tape_device': row['tape_device'],
                            'tape_id': row['tape_id'],
                            'description': row['description']
                        }
            else:
                # 非 openGauss 使用 SQLAlchemy
                async for db in get_db():
                    backup_task = await db.get(BackupTask, task_id)
                    if backup_task:
                        # 解析 source_paths
                        source_paths = None
                        if backup_task.source_paths:
                            try:
                                if isinstance(backup_task.source_paths, str):
                                    source_paths = json.loads(backup_task.source_paths)
                                else:
                                    source_paths = backup_task.source_paths
                            except:
                                source_paths = None
                        
                        return {
                            'id': backup_task.id,
                            'task_name': backup_task.task_name,
                            'task_type': backup_task.task_type,
                            'status': backup_task.status,
                            'progress_percent': backup_task.progress_percent,
                            'total_files': backup_task.total_files,
                            'total_bytes': backup_task.total_bytes,
                            'processed_files': backup_task.processed_files,
                            'processed_bytes': backup_task.processed_bytes,
                            'started_at': backup_task.started_at,
                            'completed_at': backup_task.completed_at,
                            'error_message': backup_task.error_message,
                            'result_summary': backup_task.result_summary,
                            'source_paths': source_paths,
                            'tape_device': backup_task.tape_device,
                            'tape_id': backup_task.tape_id,
                            'description': backup_task.description
                        }
            
            return None
        except Exception as e:
            logger.error(f"获取任务状态失败: {str(e)}")
            return None
    
    async def cancel_task(self, task_id: int) -> bool:
        """取消任务
        
        Args:
            task_id: 任务ID
            
        Returns:
            bool: 如果取消成功返回True，否则返回False
        """
        try:
            from utils.scheduler.db_utils import is_opengauss, get_opengauss_connection
            
            if is_opengauss():
                # 使用连接池
                async with get_opengauss_connection() as conn:
                    # 更新任务状态为取消
                    await conn.execute(
                        """
                        UPDATE backup_tasks
                        SET status = $1::backuptaskstatus, updated_at = $2
                        WHERE id = $3 AND status = $4::backuptaskstatus
                        """,
                        'CANCELLED',  # BackupTaskStatus.CANCELLED
                        datetime.now(),
                        task_id,
                        'RUNNING'  # BackupTaskStatus.RUNNING
                    )
                    
                    # 检查是否更新成功
                    row = await conn.fetchrow(
                        """
                        SELECT id FROM backup_tasks WHERE id = $1 AND status = $2::backuptaskstatus
                        """,
                        task_id,
                        'CANCELLED'
                    )
                    
                    if row:
                        logger.info(f"任务已取消: {task_id}")
                        return True
            else:
                # 非 openGauss 使用 SQLAlchemy
                async for db in get_db():
                    backup_task = await db.get(BackupTask, task_id)
                    if backup_task and backup_task.status == BackupTaskStatus.RUNNING:
                        backup_task.status = BackupTaskStatus.CANCELLED
                        backup_task.updated_at = datetime.now()
                        await db.commit()
                        logger.info(f"任务已取消: {task_id}")
                        return True
            
            return False
        except Exception as e:
            logger.error(f"取消任务失败: {str(e)}")
            return False

