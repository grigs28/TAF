#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
备份设置 API
"""

import logging
from fastapi import APIRouter, HTTPException
from pydantic import BaseModel

from config.env_file_manager import get_env_manager
from config.settings import get_settings, reload_settings

logger = logging.getLogger(__name__)

router = APIRouter(prefix="/settings")


class BackgroundCopyConfig(BaseModel):
    enabled: bool


@router.get("/background-copy")
async def get_background_copy_setting():
    """获取后台标记 is_copy_success 的开关状态"""
    settings = get_settings()
    return {
        "enabled": bool(getattr(settings, "ENABLE_BACKGROUND_COPY_UPDATE", False))
    }


@router.post("/background-copy")
async def update_background_copy_setting(config: BackgroundCopyConfig):
    """更新后台标记 is_copy_success 的开关状态（写入 .env 并立即生效）"""
    try:
        env_manager = get_env_manager()
        success = env_manager.write_env_file(
            {"ENABLE_BACKGROUND_COPY_UPDATE": str(config.enabled).lower()},
            backup=True
        )
        if not success:
            raise HTTPException(status_code=500, detail="写入配置失败")

        reload_settings()
        logger.info(
            "[备份设置] 后台标记 is_copy_success 已%s",
            "开启" if config.enabled else "关闭"
        )
        return {
            "enabled": bool(getattr(get_settings(), "ENABLE_BACKGROUND_COPY_UPDATE", False))
        }
    except HTTPException:
        raise
    except Exception as e:
        logger.error(f"更新后台标记配置失败: {str(e)}", exc_info=True)
        raise HTTPException(status_code=500, detail=f"更新配置失败: {str(e)}")

