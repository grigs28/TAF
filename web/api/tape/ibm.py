#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
磁带管理API - ibm
Tape Management API - ibm
"""

import logging
from typing import List, Dict, Any, Optional
from fastapi import APIRouter, HTTPException, Request, Depends
from pydantic import BaseModel

# IBM磁带机特定操作，无需导入模型

logger = logging.getLogger(__name__)
router = APIRouter()

@router.get("/ibm/alerts")
async def get_ibm_tape_alerts(request: Request):
    """获取IBM磁带警报信息"""
    try:
        system = request.app.state.system
        if not system:
            raise HTTPException(status_code=500, detail="系统未初始化")

        alerts = await system.tape_manager.tape_operations.get_ibm_tape_alerts()
        return alerts

    except Exception as e:
        logger.error(f"获取IBM磁带警报失败: {str(e)}")
        raise HTTPException(status_code=500, detail=str(e))


@router.get("/ibm/performance")
async def get_ibm_performance_stats(request: Request):
    """获取IBM磁带性能统计"""
    try:
        system = request.app.state.system
        if not system:
            raise HTTPException(status_code=500, detail="系统未初始化")

        performance = await system.tape_manager.tape_operations.get_ibm_performance_stats()
        return performance

    except Exception as e:
        logger.error(f"获取IBM性能统计失败: {str(e)}")
        raise HTTPException(status_code=500, detail=str(e))


@router.get("/ibm/usage")
async def get_ibm_tape_usage(request: Request):
    """获取IBM磁带使用统计"""
    try:
        system = request.app.state.system
        if not system:
            raise HTTPException(status_code=500, detail="系统未初始化")

        usage = await system.tape_manager.tape_operations.get_ibm_tape_usage()
        return usage

    except Exception as e:
        logger.error(f"获取IBM磁带使用统计失败: {str(e)}")
        raise HTTPException(status_code=500, detail=str(e))


@router.get("/ibm/temperature")
async def get_ibm_temperature_status(request: Request):
    """获取IBM磁带机温度状态"""
    try:
        system = request.app.state.system
        if not system:
            raise HTTPException(status_code=500, detail="系统未初始化")

        temperature = await system.tape_manager.tape_operations.get_ibm_temperature_status()
        return temperature

    except Exception as e:
        logger.error(f"获取IBM温度状态失败: {str(e)}")
        raise HTTPException(status_code=500, detail=str(e))


@router.get("/ibm/serial")
async def get_ibm_drive_serial(request: Request):
    """获取IBM磁带机序列号"""
    try:
        system = request.app.state.system
        if not system:
            raise HTTPException(status_code=500, detail="系统未初始化")

        serial = await system.tape_manager.tape_operations.get_ibm_drive_serial_number()
        return serial

    except Exception as e:
        logger.error(f"获取IBM序列号失败: {str(e)}")
        raise HTTPException(status_code=500, detail=str(e))


@router.get("/ibm/firmware")
async def get_ibm_firmware_version(request: Request):
    """获取IBM磁带机固件版本"""
    try:
        system = request.app.state.system
        if not system:
            raise HTTPException(status_code=500, detail="系统未初始化")

        firmware = await system.tape_manager.tape_operations.get_ibm_firmware_version()
        return firmware

    except Exception as e:
        logger.error(f"获取IBM固件版本失败: {str(e)}")
        raise HTTPException(status_code=500, detail=str(e))


@router.post("/ibm/self-test")
async def run_ibm_self_test(request: Request):
    """运行IBM磁带机自检"""
    try:
        system = request.app.state.system
        if not system:
            raise HTTPException(status_code=500, detail="系统未初始化")

        result = await system.tape_manager.tape_operations.run_ibm_self_test()
        return result

    except Exception as e:
        logger.error(f"运行IBM自检失败: {str(e)}")
        raise HTTPException(status_code=500, detail=str(e))


@router.post("/ibm/encryption/enable")
async def enable_ibm_encryption(request: Request, encryption_key: Optional[str] = None):
    """启用IBM磁带加密"""
    try:
        system = request.app.state.system
        if not system:
            raise HTTPException(status_code=500, detail="系统未初始化")

        result = await system.tape_manager.tape_operations.enable_ibm_encryption(encryption_key)
        return result

    except Exception as e:
        logger.error(f"启用IBM加密失败: {str(e)}")
        raise HTTPException(status_code=500, detail=str(e))


@router.post("/ibm/encryption/disable")
async def disable_ibm_encryption(request: Request):
    """禁用IBM磁带加密"""
    try:
        system = request.app.state.system
        if not system:
            raise HTTPException(status_code=500, detail="系统未初始化")

        result = await system.tape_manager.tape_operations.disable_ibm_encryption()
        return result

    except Exception as e:
        logger.error(f"禁用IBM加密失败: {str(e)}")
        raise HTTPException(status_code=500, detail=str(e))


@router.post("/ibm/worm/enable")
async def enable_ibm_worm_mode(request: Request):
    """启用IBM WORM模式"""
    try:
        system = request.app.state.system
        if not system:
            raise HTTPException(status_code=500, detail="系统未初始化")

        result = await system.tape_manager.tape_operations.set_ibm_worm_mode(enable=True)
        return result

    except Exception as e:
        logger.error(f"启用IBM WORM模式失败: {str(e)}")
        raise HTTPException(status_code=500, detail=str(e))


@router.post("/ibm/worm/disable")
async def disable_ibm_worm_mode(request: Request):
    """禁用IBM WORM模式"""
    try:
        system = request.app.state.system
        if not system:
            raise HTTPException(status_code=500, detail="系统未初始化")

        result = await system.tape_manager.tape_operations.set_ibm_worm_mode(enable=False)
        return result

    except Exception as e:
        logger.error(f"禁用IBM WORM模式失败: {str(e)}")
        raise HTTPException(status_code=500, detail=str(e))


@router.get("/ibm/position")
async def get_ibm_tape_position(request: Request):
    """获取IBM磁带位置信息"""
    try:
        system = request.app.state.system
        if not system:
            raise HTTPException(status_code=500, detail="系统未初始化")

        # ITDT 无直接读取块位置命令，暂返回未支持
        raise HTTPException(status_code=501, detail="ITDT暂不支持读取精确位置")
        return None

    except Exception as e:
        logger.error(f"获取IBM磁带位置失败: {str(e)}")
        raise HTTPException(status_code=500, detail=str(e))


@router.get("/ibm/sense")
async def get_ibm_sense_data(request: Request):
    """获取IBM Sense数据"""
    try:
        system = request.app.state.system
        if not system:
            raise HTTPException(status_code=500, detail="系统未初始化")

        # ITDT 无直接 Request Sense 输出，暂返回未支持
        raise HTTPException(status_code=501, detail="ITDT暂不支持Request Sense")

    except Exception as e:
        logger.error(f"获取IBM Sense数据失败: {str(e)}")
        raise HTTPException(status_code=500, detail=str(e))


@router.post("/ibm/log-sense")
async def send_ibm_log_sense(request: Request, page_code: int = 0x00, subpage_code: int = 0x00):
    """发送IBM LOG SENSE命令"""
    try:
        system = request.app.state.system
        if not system:
            raise HTTPException(status_code=500, detail="系统未初始化")

        parameters = {
            'page_code': page_code,
            'subpage_code': subpage_code
        }
        raise HTTPException(status_code=501, detail="ITDT暂不支持LOG SENSE直通")

    except Exception as e:
        logger.error(f"发送IBM LOG SENSE失败: {str(e)}")
        raise HTTPException(status_code=500, detail=str(e))


@router.post("/ibm/mode-sense")
async def send_ibm_mode_sense(request: Request, page_code: int = 0x3F, subpage_code: int = 0x00):
    """发送IBM MODE SENSE命令"""
    try:
        system = request.app.state.system
        if not system:
            raise HTTPException(status_code=500, detail="系统未初始化")

        parameters = {
            'page_code': page_code,
            'subpage_code': subpage_code
        }
        raise HTTPException(status_code=501, detail="ITDT暂不支持MODE SENSE直通")

    except Exception as e:
        logger.error(f"发送IBM MODE SENSE失败: {str(e)}")
        raise HTTPException(status_code=500, detail=str(e))


@router.post("/ibm/inquiry-vpd")
async def send_ibm_inquiry_vpd(request: Request, page_code: int = 0x00):
    """发送IBM INQUIRY VPD命令"""
    try:
        system = request.app.state.system
        if not system:
            raise HTTPException(status_code=500, detail="系统未初始化")

        parameters = {
            'page_code': page_code
        }
        raise HTTPException(status_code=501, detail="ITDT暂不支持INQUIRY VPD直通")

    except Exception as e:
        logger.error(f"发送IBM INQUIRY VPD失败: {str(e)}")
        raise HTTPException(status_code=500, detail=str(e))
