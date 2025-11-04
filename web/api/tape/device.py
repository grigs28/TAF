#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
磁带管理API - device
Tape Management API - device
"""

import logging
import traceback
from typing import List, Dict, Any, Optional
from datetime import datetime
from fastapi import APIRouter, HTTPException, Request, Depends
from pydantic import BaseModel

# 设备相关路由，无需导入模型
from models.system_log import OperationType, LogCategory, LogLevel
from utils.log_utils import log_operation, log_system

logger = logging.getLogger(__name__)
router = APIRouter()

@router.get("/health")
async def check_tape_health(request: Request):
    """检查磁带健康状态"""
    try:
        system = request.app.state.system
        if not system:
            raise HTTPException(status_code=500, detail="系统未初始化")

        health = await system.tape_manager.health_check()
        return {"healthy": health}

    except Exception as e:
        logger.error(f"检查磁带健康状态失败: {str(e)}")
        raise HTTPException(status_code=500, detail=str(e))


@router.get("/devices")
async def get_tape_devices(request: Request):
    """获取磁带设备列表"""
    try:
        system = request.app.state.system
        if not system:
            raise HTTPException(status_code=500, detail="系统未初始化")

        devices = await system.tape_manager.scsi_interface.scan_tape_devices()
        return {"devices": devices}

    except Exception as e:
        logger.error(f"获取磁带设备列表失败: {str(e)}")
        raise HTTPException(status_code=500, detail=str(e))
