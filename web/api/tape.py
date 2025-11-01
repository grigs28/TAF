#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
磁带管理API
Tape Management API
"""

import logging
from typing import List, Dict, Any, Optional
from fastapi import APIRouter, HTTPException, Request
from pydantic import BaseModel

logger = logging.getLogger(__name__)
router = APIRouter()


class TapeConfigRequest(BaseModel):
    """磁带配置请求模型"""
    retention_months: int = 6
    auto_erase: bool = True


@router.get("/inventory")
async def get_tape_inventory(request: Request):
    """获取磁带库存"""
    try:
        system = request.app.state.system
        if not system:
            raise HTTPException(status_code=500, detail="系统未初始化")

        inventory = await system.tape_manager.get_inventory_status()
        return inventory

    except Exception as e:
        logger.error(f"获取磁带库存失败: {str(e)}")
        raise HTTPException(status_code=500, detail=str(e))


@router.get("/current")
async def get_current_tape(request: Request):
    """获取当前磁带信息"""
    try:
        system = request.app.state.system
        if not system:
            raise HTTPException(status_code=500, detail="系统未初始化")

        tape_info = await system.tape_manager.get_tape_info()
        if tape_info:
            return tape_info
        else:
            return {"message": "当前没有加载的磁带"}

    except Exception as e:
        logger.error(f"获取当前磁带信息失败: {str(e)}")
        raise HTTPException(status_code=500, detail=str(e))


@router.post("/load")
async def load_tape(request: Request, tape_id: str):
    """加载磁带"""
    try:
        system = request.app.state.system
        if not system:
            raise HTTPException(status_code=500, detail="系统未初始化")

        success = await system.tape_manager.load_tape(tape_id)
        if success:
            return {"success": True, "message": f"磁带 {tape_id} 加载成功"}
        else:
            raise HTTPException(status_code=500, detail="磁带加载失败")

    except HTTPException:
        raise
    except Exception as e:
        logger.error(f"加载磁带失败: {str(e)}")
        raise HTTPException(status_code=500, detail=str(e))


@router.post("/unload")
async def unload_tape(request: Request):
    """卸载磁带"""
    try:
        system = request.app.state.system
        if not system:
            raise HTTPException(status_code=500, detail="系统未初始化")

        success = await system.tape_manager.unload_tape()
        if success:
            return {"success": True, "message": "磁带卸载成功"}
        else:
            raise HTTPException(status_code=500, detail="磁带卸载失败")

    except Exception as e:
        logger.error(f"卸载磁带失败: {str(e)}")
        raise HTTPException(status_code=500, detail=str(e))


@router.post("/erase")
async def erase_tape(request: Request, tape_id: str):
    """擦除磁带"""
    try:
        system = request.app.state.system
        if not system:
            raise HTTPException(status_code=500, detail="系统未初始化")

        success = await system.tape_manager.erase_tape(tape_id)
        if success:
            return {"success": True, "message": f"磁带 {tape_id} 擦除成功"}
        else:
            raise HTTPException(status_code=500, detail="磁带擦除失败")

    except Exception as e:
        logger.error(f"擦除磁带失败: {str(e)}")
        raise HTTPException(status_code=500, detail=str(e))


@router.post("/format")
async def format_tape(request: Request, tape_id: str, format_type: int = 0):
    """格式化磁带"""
    try:
        system = request.app.state.system
        if not system:
            raise HTTPException(status_code=500, detail="系统未初始化")

        # 使用SCSI接口格式化
        success = await system.tape_manager.scsi_interface.format_tape(format_type=format_type)
        if success:
            return {"success": True, "message": f"磁带 {tape_id} 格式化成功"}
        else:
            raise HTTPException(status_code=500, detail="磁带格式化失败")

    except Exception as e:
        logger.error(f"格式化磁带失败: {str(e)}")
        raise HTTPException(status_code=500, detail=str(e))


@router.post("/rewind")
async def rewind_tape(request: Request, tape_id: str = None):
    """倒带"""
    try:
        system = request.app.state.system
        if not system:
            raise HTTPException(status_code=500, detail="系统未初始化")

        # 使用SCSI接口倒带
        success = await system.tape_manager.scsi_interface.rewind_tape()
        if success:
            return {"success": True, "message": "磁带倒带成功"}
        else:
            raise HTTPException(status_code=500, detail="磁带倒带失败")

    except Exception as e:
        logger.error(f"倒带失败: {str(e)}")
        raise HTTPException(status_code=500, detail=str(e))


@router.post("/space")
async def space_tape_blocks(request: Request, blocks: int = 1, direction: str = "forward"):
    """按块定位磁带"""
    try:
        system = request.app.state.system
        if not system:
            raise HTTPException(status_code=500, detail="系统未初始化")

        # 使用SCSI接口定位
        success = await system.tape_manager.scsi_interface.space_blocks(blocks=blocks, direction=direction)
        if success:
            return {"success": True, "message": f"磁带定位成功：{blocks} 块 (方向: {direction})"}
        else:
            raise HTTPException(status_code=500, detail="磁带定位失败")

    except Exception as e:
        logger.error(f"磁带定位失败: {str(e)}")
        raise HTTPException(status_code=500, detail=str(e))


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


# IBM LTO特定功能API端点
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

        position = await system.tape_manager.scsi_interface.get_tape_position()
        return position

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

        sense_data = await system.tape_manager.scsi_interface.request_sense()
        return sense_data

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
        result = await system.tape_manager.scsi_interface.send_ibm_specific_command(
            device_path=None,
            command_type="log_sense",
            parameters=parameters
        )
        return result

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
        result = await system.tape_manager.scsi_interface.send_ibm_specific_command(
            device_path=None,
            command_type="mode_sense",
            parameters=parameters
        )
        return result

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

        parameters = {'page_code': page_code}
        result = await system.tape_manager.scsi_interface.send_ibm_specific_command(
            device_path=None,
            command_type="inquiry_vpd",
            parameters=parameters
        )
        return result

    except Exception as e:
        logger.error(f"发送IBM INQUIRY VPD失败: {str(e)}")
        raise HTTPException(status_code=500, detail=str(e))