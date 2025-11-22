#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
备份管理API - 统计信息
Backup Management API - Statistics
"""

import logging
from fastapi import APIRouter, HTTPException, Request

logger = logging.getLogger(__name__)
router = APIRouter()


@router.get("/statistics")
async def get_backup_statistics(http_request: Request):
    """获取备份统计信息（使用真实数据）"""
    try:
        from web.api import backup_statistics
        get_stats = backup_statistics.get_backup_statistics
        return await get_stats()
    except Exception as e:
        logger.error(f"获取备份统计信息失败: {str(e)}", exc_info=True)
        raise HTTPException(status_code=500, detail=str(e))

