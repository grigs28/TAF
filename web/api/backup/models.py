#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
备份管理API - 数据模型
Backup Management API - Data Models
"""

from typing import List, Dict, Any, Optional
from datetime import datetime
from pydantic import BaseModel, Field
from models.backup import BackupTaskType


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


class BackupTaskUpdate(BaseModel):
    """备份任务更新模型（更新模板配置）"""
    task_name: Optional[str] = Field(None, description="任务名称")
    source_paths: Optional[List[str]] = Field(None, description="源路径列表")
    task_type: Optional[BackupTaskType] = Field(None, description="任务类型")
    exclude_patterns: Optional[List[str]] = Field(None, description="排除模式")
    compression_enabled: Optional[bool] = Field(None, description="是否启用压缩")
    encryption_enabled: Optional[bool] = Field(None, description="是否启用加密")
    retention_days: Optional[int] = Field(None, description="保留天数")
    description: Optional[str] = Field(None, description="任务描述")
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
    total_bytes_actual: int = 0
    processed_bytes: int
    compressed_bytes: int = 0
    compression_ratio: float = 0.0
    created_at: datetime
    started_at: Optional[datetime]
    completed_at: Optional[datetime]
    error_message: Optional[str]
    source_paths: List[str] = Field(default_factory=list)
    tape_device: Optional[str] = None
    tape_id: Optional[str] = None
    is_template: bool = False
    from_scheduler: bool = False
    enabled: Optional[bool] = True
    description: Optional[str] = ""
    estimated_archive_count: Optional[int] = None
    operation_status: Optional[str] = None
    operation_stage: Optional[str] = None
    operation_stage_label: Optional[str] = None
    stage_steps: List[Dict[str, Any]] = Field(default_factory=list)
    current_compression_progress: Optional[Dict[str, Any]] = Field(None, description="当前压缩进度信息（运行时）")

