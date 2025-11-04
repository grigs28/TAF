#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
通知人员数据模型
Notification User Data Model
"""

from datetime import datetime
from sqlalchemy import Column, Integer, String, DateTime, Text, Boolean

from .base import BaseModel


class NotificationUser(BaseModel):
    """通知人员表"""

    __tablename__ = "notification_users"

    # 基本信息
    phone = Column(String(20), unique=True, nullable=False, comment="手机号")
    name = Column(String(100), nullable=False, comment="姓名")
    remark = Column(Text, comment="备注")
    enabled = Column(Boolean, default=True, nullable=False, comment="是否启用")

    def __repr__(self):
        return f"<NotificationUser(id={self.id}, phone={self.phone}, name={self.name})>"

