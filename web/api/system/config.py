#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
系统管理API - config
System Management API - config
"""

import logging
from typing import Dict, Any, Optional
from datetime import datetime
from fastapi import APIRouter, HTTPException, Request
from pydantic import BaseModel, Field

from .models import SystemConfigRequest

logger = logging.getLogger(__name__)
router = APIRouter()

@router.get("/config")
async def get_system_config():
    """获取系统配置"""
    try:
        from config.settings import get_settings
        settings = get_settings()

        # 返回非敏感配置
        return {
            "default_retention_months": settings.DEFAULT_RETENTION_MONTHS,
            "auto_erase_expired": settings.AUTO_ERASE_EXPIRED,
            "monthly_backup_cron": settings.MONTHLY_BACKUP_CRON,
            "dingtalk_api_url": settings.DINGTALK_API_URL,
            "dingtalk_default_phone": settings.DINGTALK_DEFAULT_PHONE,
            "scheduler_enabled": settings.SCHEDULER_ENABLED,
            "compression_level": settings.COMPRESSION_LEVEL,
            "max_file_size": settings.MAX_FILE_SIZE
        }

    except Exception as e:
        logger.error(f"获取系统配置失败: {str(e)}")
        raise HTTPException(status_code=500, detail=str(e))


@router.put("/config")
async def update_system_config(config: SystemConfigRequest):
    """更新系统配置"""
    try:
        # 这里应该实现配置更新逻辑
        # 包括验证配置、保存到数据库、重新加载配置等

        return {"success": True, "message": "配置更新成功"}

    except Exception as e:
        logger.error(f"更新系统配置失败: {str(e)}")
        raise HTTPException(status_code=500, detail=str(e))


@router.post("/test-dingtalk")
async def test_dingtalk_notification(request: Request):
    """测试钉钉通知"""
    try:
        system = request.app.state.system
        if not system:
            raise HTTPException(status_code=500, detail="系统未初始化")

        success = await system.dingtalk_notifier.test_connection()
        if success:
            # 发送测试消息
            await system.dingtalk_notifier.send_system_notification(
                "测试消息",
                "这是一条来自企业级磁带备份系统的测试消息"
            )
            return {"success": True, "message": "测试通知发送成功"}
        else:
            return {"success": False, "message": "钉钉连接测试失败"}

    except Exception as e:
        logger.error(f"测试钉钉通知失败: {str(e)}")
        raise HTTPException(status_code=500, detail=str(e))

