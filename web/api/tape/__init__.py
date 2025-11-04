#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
磁带管理API模块
Tape Management API Module
"""

from fastapi import APIRouter

# 导入所有子模块
from . import models, crud, label, operations, device, ibm, utils

# 创建主路由器
router = APIRouter()

# 注册所有子模块的路由
router.include_router(crud.router, tags=["磁带CRUD"])
router.include_router(label.router, tags=["磁带标签"])
router.include_router(operations.router, tags=["磁带操作"])
router.include_router(device.router, tags=["磁带设备"])
router.include_router(ibm.router, tags=["IBM磁带机"])
router.include_router(utils.router, tags=["磁带工具"])

__all__ = ['router', 'models']

