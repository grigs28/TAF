#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
磁带管理API - utils
Tape Management API - utils
"""

import logging
from typing import List, Dict, Any, Optional
from fastapi import APIRouter, HTTPException, Request, Depends
from pydantic import BaseModel

import uuid
import base64
import json

logger = logging.getLogger(__name__)
router = APIRouter()

@router.get("/test-read-label")
async def test_read_label(request: Request):
    """测试路由"""
    logger.critical("========== 测试路由被调用 ==========")
    raise HTTPException(status_code=500, detail="测试路由工作正常！如果你看到这个错误，说明路由系统正常")


@router.get("/generate-uuid")
async def generate_uuid(request: Request):
    """生成UUID序列号"""
    try:
        u = uuid.uuid4()
        
        # 使用HEX格式（全大写无连字符，32字符）作为序列号
        serial_number = u.hex.upper()
        
        # 返回多种格式的UUID
        result = {
            "success": True,
            "serial_number": serial_number,  # 主要返回：全大写无连字符格式（32字符）
            "uuid": {
                "str": str(u),                    # 标准带连字符 36 字符
                "hex": u.hex,                     # 去掉连字符 32 字符（小写）
                "hex_upper": serial_number,       # 全大写无连字符 32 字符（推荐用于序列号）
                "str_upper": str(u).upper(),      # 全大写有连字符
                "braces": f"{{{u}}}",            # 花括号格式
                "urn": u.urn,                     # URN 标准格式
                "base64": base64.urlsafe_b64encode(u.bytes).decode().rstrip('='),  # base64 短串（22 字符）
                "int": u.int,                     # 128 bit 大整数
                "bits": format(u.int, '0128b'),   # 比特串 128 位
            }
        }
        
        logger.info(f"生成UUID序列号: {serial_number}")
        return result
        
    except Exception as e:
        logger.error(f"生成UUID失败: {str(e)}", exc_info=True)
        raise HTTPException(status_code=500, detail=str(e))
