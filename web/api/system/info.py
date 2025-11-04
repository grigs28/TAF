#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
系统管理API - info
System Management API - info
"""

import logging
from typing import Dict, Any, Optional
from datetime import datetime
from fastapi import APIRouter, HTTPException, Request
from pydantic import BaseModel, Field

# 系统信息相关路由，无需导入模型

logger = logging.getLogger(__name__)
router = APIRouter()

@router.get("/info")
async def get_system_info():
    """获取系统信息"""
    try:
        from config.settings import get_settings
        from pathlib import Path
        import re
        
        settings = get_settings()

        return {
            "app_name": settings.APP_NAME,
            "version": settings.APP_VERSION,
            "python_version": "3.8+",
            "platform": "Windows/openEuler",
            "database": "openGauss",
            "compression": "7-Zip SDK"
        }

    except Exception as e:
        logger.error(f"获取系统信息失败: {str(e)}")
        raise HTTPException(status_code=500, detail=str(e))


@router.get("/version")
async def get_version():
    """获取版本信息和CHANGELOG"""
    try:
        from config.settings import get_settings
        from pathlib import Path
        import re
        
        settings = get_settings()
        
        # 读取CHANGELOG.md
        changelog_path = Path("CHANGELOG.md")
        changelog_content = ""
        
        if changelog_path.exists():
            with open(changelog_path, "r", encoding="utf-8") as f:
                changelog_content = f.read()
        
        return {
            "version": settings.APP_VERSION,
            "app_name": settings.APP_NAME,
            "changelog": changelog_content
        }

    except Exception as e:
        logger.error(f"获取版本信息失败: {str(e)}")
        raise HTTPException(status_code=500, detail=str(e))


@router.get("/health")
async def health_check(request: Request):
    """系统健康检查"""
    try:
        system = request.app.state.system
        if not system:
            return {"status": "unhealthy", "message": "系统未初始化"}

        checks = {
            "database": await system.db_manager.health_check(),
            "tape_drive": await system.tape_manager.health_check(),
            "scheduler": system.scheduler.running if system.scheduler else False
        }

        overall_healthy = all(checks.values())

        return {
            "status": "healthy" if overall_healthy else "unhealthy",
            "checks": checks
        }

    except Exception as e:
        logger.error(f"系统健康检查失败: {str(e)}")
        return {
            "status": "unhealthy",
            "error": str(e)
        }

