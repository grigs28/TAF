#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
磁带管理API数据模型
Tape Management API Models
"""

from typing import Optional
from pydantic import BaseModel


class TapeConfigRequest(BaseModel):
    """磁带配置请求模型"""
    retention_months: int = 6
    auto_erase: bool = True


class CreateTapeRequest(BaseModel):
    """创建磁带请求模型"""
    tape_id: str
    label: str
    serial_number: Optional[str] = None
    media_type: str = "LTO"
    generation: int = 8
    capacity_gb: Optional[int] = None
    location: Optional[str] = None
    notes: Optional[str] = None
    retention_months: int = 6
    create_year: Optional[int] = None  # 创建年份
    create_month: Optional[int] = None  # 创建月份


class UpdateTapeRequest(BaseModel):
    """更新磁带请求模型"""
    serial_number: Optional[str] = None
    media_type: Optional[str] = None
    generation: Optional[int] = None
    capacity_gb: Optional[int] = None
    location: Optional[str] = None
    notes: Optional[str] = None


class WriteTapeLabelRequest(BaseModel):
    """写入磁带标签请求模型"""
    tape_id: str
    label: str
    serial_number: Optional[str] = None


class FormatRequest(BaseModel):
    """格式化磁带请求模型"""
    quick_format: bool = False
    verify: bool = True
