#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
磁带工具管理API
Tape Tools Management API
"""

import os
import logging
from typing import Optional
from datetime import datetime
from fastapi import APIRouter, HTTPException, Depends, Request
from pydantic import BaseModel, Field

from utils.tape_tools import tape_tools_manager
from utils.libltfs_wrapper import get_libltfs_wrapper
from models.system_log import OperationType, LogCategory, LogLevel
from config.database import get_db
from sqlalchemy.ext.asyncio import AsyncSession

logger = logging.getLogger(__name__)

router = APIRouter(prefix="/api/tools", tags=["tools"])


# ===== Pydantic 模型 =====
class ITDTLoadRequest(BaseModel):
    """ITDT加载磁带请求"""
    pass


class ITDTEraseRequest(BaseModel):
    """ITDT擦除磁带请求"""
    quick: bool = Field(default=True, description="是否快速擦除")


class ITDTChangePartitionRequest(BaseModel):
    """ITDT切换分区请求"""
    partition_number: int = Field(..., ge=0, le=1, description="分区号（0或1）")


class LTFSLoadRequest(BaseModel):
    """LTFS加载磁带请求"""
    drive_id: str = Field(..., description="驱动器ID，如：0.0.24.0")


class LTFSEjectRequest(BaseModel):
    """LTFS弹出磁带请求"""
    drive_id: str = Field(..., description="驱动器ID，如：0.0.24.0")


class LTFSFormatRequest(BaseModel):
    """LTFS格式化磁带请求 (LtfsCmdFormat.exe)"""
    drive_letter: Optional[str] = Field(None, description="盘符（大写，不带冒号，如：O）")
    volume_label: Optional[str] = Field(None, description="卷标名称")
    serial: Optional[str] = Field(None, description="序列号（6位大写字母数字）")
    eject_after: bool = Field(False, description="格式化后是否弹出")


class MkltfsFormatRequest(BaseModel):
    """mkltfs格式化磁带请求"""
    device_id: str = Field(..., description="设备地址，如：0.0.24.0")
    volume_label: Optional[str] = Field(None, description="卷标名称")


class LTFSAssignRequest(BaseModel):
    """LTFS分配磁带请求"""
    drive_id: str = Field(..., description="驱动器ID，如：0.0.24.0")


class LTFSUnassignRequest(BaseModel):
    """LTFS卸载磁带请求"""
    drive_id: str = Field(..., description="驱动器ID，如：0.0.24.0")


class LTFSCheckRequest(BaseModel):
    """LTFS检查磁带请求"""
    drive_id: str = Field(..., description="驱动器ID，如：0.0.24.0")


class MountCompleteRequest(BaseModel):
    """完整挂载流程请求"""
    drive_id: str = Field(..., description="驱动器ID，如：0.0.24.0")
    volume_label: Optional[str] = Field(None, description="卷标名称（如果格式化）")
    format_tape: bool = Field(True, description="是否格式化磁带")


class UnmountCompleteRequest(BaseModel):
    """完整卸载流程请求"""
    drive_id: str = Field(..., description="驱动器ID，如：0.0.24.0")


# 记录操作日志的辅助函数
async def log_tool_operation(
    db: AsyncSession,
    operation_type: OperationType,
    operation_name: str,
    success: bool,
    details: dict = None,
    error_message: str = None
):
    """记录工具操作日志"""
    try:
        from models.system_log import OperationLog
        
        log_entry = OperationLog(
            operation_type=operation_type,
            resource_type="tool",
            operation_name=operation_name,
            operation_description=f"工具管理: {operation_name}",
            category="tape",
            operation_time=datetime.now(),
            success=success,
            request_params=details or {},
            error_message=error_message
        )
        db.add(log_entry)
        await db.commit()
    except Exception as e:
        logger.error(f"记录工具操作日志失败: {str(e)}")


# ===== ITDT 工具API =====
@router.post("/itdt/load")
async def itdt_load_tape(request: ITDTLoadRequest, db: AsyncSession = Depends(get_db)):
    """使用ITDT加载磁带"""
    try:
        result = await tape_tools_manager.load_tape_itdt()
        await log_tool_operation(
            db, OperationType.TAPE_LOAD, "ITDT加载磁带",
            result.get("success", False), {"returncode": result.get("returncode")},
            result.get("stderr") if not result.get("success") else None
        )
        return result
    except Exception as e:
        logger.error(f"ITDT加载磁带失败: {str(e)}")
        await log_tool_operation(db, OperationType.TAPE_LOAD, "ITDT加载磁带", False, error_message=str(e))
        raise HTTPException(status_code=500, detail=str(e))


@router.get("/itdt/status")
async def itdt_check_status():
    """检查磁带设备状态"""
    try:
        result = await tape_tools_manager.check_tape_status_itdt()
        return result
    except Exception as e:
        logger.error(f"检查磁带设备状态失败: {str(e)}")
        raise HTTPException(status_code=500, detail=str(e))


@router.get("/itdt/partition")
async def itdt_get_partition_info():
    """获取分区信息"""
    try:
        result = await tape_tools_manager.get_partition_info_itdt()
        return result
    except Exception as e:
        logger.error(f"获取分区信息失败: {str(e)}")
        raise HTTPException(status_code=500, detail=str(e))


@router.post("/itdt/partition")
async def itdt_change_partition(request: ITDTChangePartitionRequest):
    """切换分区"""
    try:
        result = await tape_tools_manager.change_partition_itdt(request.partition_number)
        return result
    except Exception as e:
        logger.error(f"切换分区失败: {str(e)}")
        raise HTTPException(status_code=500, detail=str(e))


@router.post("/itdt/erase")
async def itdt_erase_tape(request: ITDTEraseRequest, db: AsyncSession = Depends(get_db)):
    """擦除磁带"""
    try:
        result = await tape_tools_manager.erase_tape_itdt(quick=request.quick)
        await log_tool_operation(
            db, OperationType.TAPE_ERASE, 
            f"ITDT{'快速' if request.quick else '完全'}擦除磁带",
            result.get("success", False), 
            {"quick": request.quick, "returncode": result.get("returncode")},
            result.get("stderr") if not result.get("success") else None
        )
        return result
    except Exception as e:
        logger.error(f"擦除磁带失败: {str(e)}")
        await log_tool_operation(
            db, OperationType.TAPE_ERASE, 
            f"ITDT{'快速' if request.quick else '完全'}擦除磁带", 
            False, error_message=str(e)
        )
        raise HTTPException(status_code=500, detail=str(e))


@router.get("/itdt/usage")
async def itdt_get_tape_usage():
    """获取磁带使用统计"""
    try:
        result = await tape_tools_manager.get_tape_usage_itdt()
        return result
    except Exception as e:
        logger.error(f"获取磁带使用统计失败: {str(e)}")
        raise HTTPException(status_code=500, detail=str(e))


@router.get("/itdt/list")
async def itdt_list_tape():
    """列出磁带内容"""
    try:
        result = await tape_tools_manager.list_tape_itdt()
        return result
    except Exception as e:
        logger.error(f"列出磁带内容失败: {str(e)}")
        raise HTTPException(status_code=500, detail=str(e))


@router.get("/itdt/mam")
async def itdt_read_mam_attributes(
    partition: Optional[int] = None,
    attribute_id: Optional[str] = None
):
    """读取MAM属性（序列号、二维码等）
    
    Args:
        partition: 分区号（0-3），可选
        attribute_id: 属性标识符（如 "0x0001", "0x0002", "0x0009"），可选
                     - 0x0001: Media Manufacturer (制造商)
                     - 0x0002: Media Serial Number (序列号)
                     - 0x0009: Media Barcode (二维码)
                     如果不指定，默认读取序列号（0x0002）
    """
    try:
        result = await tape_tools_manager.read_mam_attributes_itdt(
            partition=partition,
            attribute_id=attribute_id
        )
        return result
    except ValueError as e:
        logger.error(f"读取MAM属性参数错误: {str(e)}")
        raise HTTPException(status_code=400, detail=str(e))
    except Exception as e:
        logger.error(f"读取MAM属性失败: {str(e)}")
        raise HTTPException(status_code=500, detail=str(e))


class MAMWriteRequest(BaseModel):
    """写入MAM属性请求"""
    partition: int = Field(default=0, ge=0, le=3, description="分区号（0-3）")
    attribute_id: str = Field(..., description="属性标识符（如 0x0002）")
    attribute_value: str = Field(..., description="要写入的属性值")


@router.post("/itdt/mam")
async def itdt_write_mam_attribute(request: MAMWriteRequest, db: AsyncSession = Depends(get_db)):
    """写入MAM属性
    
    Args:
        request: 写入MAM属性请求
            - partition: 分区号（0-3），默认0
            - attribute_id: 属性标识符（如 "0x0002"）
            - attribute_value: 要写入的属性值
    """
    try:
        result = await tape_tools_manager.write_mam_attribute_itdt(
            attribute_id=request.attribute_id,
            attribute_value=request.attribute_value,
            partition=request.partition
        )
        
        await log_tool_operation(
            db, OperationType.UPDATE, "写入MAM属性",
            result.get("success", False),
            {
                "partition": request.partition,
                "attribute_id": request.attribute_id,
                "attribute_value": request.attribute_value,
                "returncode": result.get("returncode")
            },
            result.get("error_detail") if not result.get("success") else None
        )
        
        return result
    except ValueError as e:
        logger.error(f"写入MAM属性参数错误: {str(e)}")
        raise HTTPException(status_code=400, detail=str(e))
    except FileNotFoundError as e:
        logger.error(f"写入MAM属性文件错误: {str(e)}")
        raise HTTPException(status_code=404, detail=str(e))
    except Exception as e:
        logger.error(f"写入MAM属性失败: {str(e)}")
        await log_tool_operation(
            db, OperationType.UPDATE, "写入MAM属性",
            False, error_message=str(e)
        )
        raise HTTPException(status_code=500, detail=str(e))


# ===== LTFS 工具API =====
@router.get("/ltfs/drives")
async def ltfs_list_drives():
    """列出LTFS驱动器"""
    try:
        result = await tape_tools_manager.list_drives_ltfs()
        return result
    except Exception as e:
        logger.error(f"列出LTFS驱动器失败: {str(e)}")
        raise HTTPException(status_code=500, detail=str(e))


@router.post("/ltfs/load")
async def ltfs_load_tape(request: LTFSLoadRequest):
    """物理加载磁带"""
    try:
        result = await tape_tools_manager.load_tape_ltfs(request.drive_id)
        return result
    except Exception as e:
        logger.error(f"LTFS加载磁带失败: {str(e)}")
        raise HTTPException(status_code=500, detail=str(e))


@router.post("/ltfs/eject")
async def ltfs_eject_tape(request: LTFSEjectRequest):
    """物理弹出磁带"""
    try:
        result = await tape_tools_manager.eject_tape_ltfs(request.drive_id)
        return result
    except Exception as e:
        logger.error(f"LTFS弹出磁带失败: {str(e)}")
        raise HTTPException(status_code=500, detail=str(e))


@router.post("/ltfs/format")
async def ltfs_format_tape(request: LTFSFormatRequest, db: AsyncSession = Depends(get_db)):
    """格式化磁带为LTFS格式 (LtfsCmdFormat.exe)"""
    try:
        result = await tape_tools_manager.format_tape_ltfs(
            drive_letter=request.drive_letter,
            volume_label=request.volume_label,
            serial=request.serial,
            eject_after=request.eject_after
        )
        await log_tool_operation(
            db, OperationType.TAPE_FORMAT, "LTFS格式化磁带(LtfsCmdFormat)",
            result.get("success", False),
            {
                "drive_letter": request.drive_letter,
                "volume_label": request.volume_label,
                "serial": request.serial,
                "returncode": result.get("returncode")
            },
            result.get("stderr") if not result.get("success") else None
        )
        return result
    except Exception as e:
        logger.error(f"LTFS格式化磁带失败: {str(e)}")
        await log_tool_operation(
            db, OperationType.TAPE_FORMAT, "LTFS格式化磁带(LtfsCmdFormat)", 
            False, {"drive_letter": request.drive_letter}, str(e)
        )
        raise HTTPException(status_code=500, detail=str(e))


@router.post("/mkltfs/format")
async def mkltfs_format_tape(request: MkltfsFormatRequest, db: AsyncSession = Depends(get_db)):
    """使用mkltfs格式化磁带（备用方式）"""
    try:
        result = await tape_tools_manager.format_tape_mkltfs(
            device_id=request.device_id,
            volume_label=request.volume_label
        )
        await log_tool_operation(
            db, OperationType.TAPE_FORMAT, "mkltfs格式化磁带",
            result.get("success", False),
            {
                "device_id": request.device_id,
                "volume_label": request.volume_label,
                "returncode": result.get("returncode")
            },
            result.get("stderr") if not result.get("success") else None
        )
        return result
    except Exception as e:
        logger.error(f"mkltfs格式化磁带失败: {str(e)}")
        await log_tool_operation(
            db, OperationType.TAPE_FORMAT, "mkltfs格式化磁带",
            False, {"device_id": request.device_id}, str(e)
        )
        raise HTTPException(status_code=500, detail=str(e))


@router.post("/ltfs/assign")
async def ltfs_assign_tape(request: LTFSAssignRequest, db: AsyncSession = Depends(get_db)):
    """分配磁带到盘符"""
    try:
        result = await tape_tools_manager.assign_tape_ltfs(request.drive_id)
        await log_tool_operation(
            db, OperationType.TAPE_MOUNT, "LTFS分配磁带到盘符",
            result.get("success", False),
            {"drive_id": request.drive_id, "returncode": result.get("returncode")},
            result.get("stderr") if not result.get("success") else None
        )
        return result
    except Exception as e:
        logger.error(f"LTFS分配磁带失败: {str(e)}")
        await log_tool_operation(
            db, OperationType.TAPE_MOUNT, "LTFS分配磁带到盘符",
            False, {"drive_id": request.drive_id}, str(e)
        )
        raise HTTPException(status_code=500, detail=str(e))


@router.post("/ltfs/unassign")
async def ltfs_unassign_tape(request: LTFSUnassignRequest, db: AsyncSession = Depends(get_db)):
    """从盘符卸载磁带"""
    try:
        result = await tape_tools_manager.unassign_tape_ltfs(request.drive_id)
        await log_tool_operation(
            db, OperationType.TAPE_UNMOUNT, "LTFS从盘符卸载磁带",
            result.get("success", False),
            {"drive_id": request.drive_id, "returncode": result.get("returncode")},
            result.get("stderr") if not result.get("success") else None
        )
        return result
    except Exception as e:
        logger.error(f"LTFS卸载磁带失败: {str(e)}")
        await log_tool_operation(
            db, OperationType.TAPE_UNMOUNT, "LTFS从盘符卸载磁带",
            False, {"drive_id": request.drive_id}, str(e)
        )
        raise HTTPException(status_code=500, detail=str(e))


@router.post("/ltfs/check")
async def ltfs_check_tape(request: LTFSCheckRequest):
    """检查磁带完整性"""
    try:
        result = await tape_tools_manager.check_tape_ltfs(request.drive_id)
        return result
    except Exception as e:
        logger.error(f"LTFS检查磁带失败: {str(e)}")
        raise HTTPException(status_code=500, detail=str(e))


# ===== 卷信息和卷标API =====
@router.get("/volume/info")
async def get_volume_info():
    """获取磁带卷信息（详细）"""
    try:
        result = await tape_tools_manager.get_volume_info()
        return result
    except Exception as e:
        logger.error(f"获取卷信息失败: {str(e)}")
        raise HTTPException(status_code=500, detail=str(e))


@router.get("/label/read")
async def read_tape_label(db: AsyncSession = Depends(get_db)):
    """读取磁带卷标（Windows系统）"""
    try:
        result = await tape_tools_manager.read_tape_label_windows()
        
        # 记录操作日志
        await log_tool_operation(
            db, OperationType.TAPE_READ_LABEL, "读取磁带卷标",
            result.get("success", False),
            {
                "volume_name": result.get("volume_name"),
                "serial_number": result.get("serial_number"),
                "file_system": result.get("file_system")
            },
            result.get("error") if not result.get("success") else None
        )
        
        return result
    except Exception as e:
        logger.error(f"读取磁带卷标失败: {str(e)}")
        await log_tool_operation(
            db, OperationType.TAPE_READ_LABEL, "读取磁带卷标",
            False, error_message=str(e)
        )
        raise HTTPException(status_code=500, detail=str(e))


# ===== 组合流程API =====
@router.post("/mount/complete")
async def mount_tape_complete(request: MountCompleteRequest, db: AsyncSession = Depends(get_db)):
    """完整挂载流程：加载->格式化(可选)->分配"""
    try:
        result = await tape_tools_manager.mount_tape_complete(
            drive_id=request.drive_id,
            volume_label=request.volume_label,
            format_tape=request.format_tape
        )
        await log_tool_operation(
            db, OperationType.TAPE_MOUNT, "完整挂载流程",
            result.get("success", False),
            {
                "drive_id": request.drive_id,
                "format_tape": request.format_tape,
                "volume_label": request.volume_label,
                "steps": result.get("steps", [])
            },
            result.get("error") if not result.get("success") else None
        )
        return result
    except Exception as e:
        logger.error(f"完整挂载流程失败: {str(e)}")
        await log_tool_operation(
            db, OperationType.TAPE_MOUNT, "完整挂载流程",
            False, {"drive_id": request.drive_id}, str(e)
        )
        raise HTTPException(status_code=500, detail=str(e))


@router.post("/unmount/complete")
async def unmount_tape_complete(request: UnmountCompleteRequest, db: AsyncSession = Depends(get_db)):
    """完整卸载流程：卸载->弹出"""
    try:
        result = await tape_tools_manager.unmount_tape_complete(request.drive_id)
        await log_tool_operation(
            db, OperationType.TAPE_UNMOUNT, "完整卸载流程",
            result.get("success", False),
            {
                "drive_id": request.drive_id,
                "steps": result.get("steps", [])
            },
            result.get("error") if not result.get("success") else None
        )
        return result
    except Exception as e:
        logger.error(f"完整卸载流程失败: {str(e)}")
        await log_tool_operation(
            db, OperationType.TAPE_UNMOUNT, "完整卸载流程",
            False, {"drive_id": request.drive_id}, str(e)
        )
        raise HTTPException(status_code=500, detail=str(e))


# ===== 工具检查API =====
@router.get("/check")
async def check_tools_availability():
    """检查工具可用性"""
    try:
        result = tape_tools_manager.check_tools_availability()
        # 检查libltfs.dll可用性
        libltfs_available = False
        try:
            wrapper = get_libltfs_wrapper()
            libltfs_available = wrapper is not None
        except Exception:
            pass
        result["libltfs_available"] = libltfs_available
        return result
    except Exception as e:
        logger.error(f"检查工具可用性失败: {str(e)}")
        raise HTTPException(status_code=500, detail=str(e))


# ===== libltfs.dll 直接调用API =====

class LibLTFSVolumeLabelRequest(BaseModel):
    """libltfs.dll 设置卷标请求"""
    volume_label: str = Field(..., description="卷标名称")


class LibLTFSSerialNumberRequest(BaseModel):
    """libltfs.dll 设置序列号请求"""
    serial_number: str = Field(..., description="序列号")


class LibLTFSBarcodeRequest(BaseModel):
    """libltfs.dll 设置条码请求"""
    barcode: str = Field(..., description="条码")


@router.get("/libltfs/volume-label/{drive_letter}")
async def libltfs_get_volume_label(drive_letter: str, db: AsyncSession = Depends(get_db)):
    """通过libltfs.dll读取卷标"""
    try:
        logger.info(f"开始读取卷标，驱动器标识: {drive_letter}")
        
        wrapper = get_libltfs_wrapper()
        if not wrapper:
            logger.warning("libltfs.dll不可用")
            return {
                "success": False,
                "error": "libltfs.dll不可用"
            }
        
        logger.debug(f"尝试使用libltfs.dll读取卷标...")
        # 尝试使用libltfs.dll
        volume_label = wrapper.get_volume_label(drive_letter)
        logger.debug(f"libltfs.dll返回结果: {volume_label}")
        
        # 如果DLL方法失败，尝试使用tape_tools_manager（备用方法）
        if not volume_label and len(drive_letter) == 1 and drive_letter.isalpha():
            logger.info("libltfs.dll方法失败，尝试使用tape_tools_manager读取卷标...")
            try:
                result = await tape_tools_manager.read_tape_label_windows()
                logger.debug(f"tape_tools_manager读取卷标结果: {result}")
                if result.get("success") and result.get("volume_name"):
                    volume_label = result.get("volume_name")
                    logger.info(f"通过tape_tools_manager成功读取卷标: {volume_label}")
            except Exception as e:
                logger.error(f"使用tape_tools_manager获取卷标失败: {str(e)}", exc_info=True)
        
        if volume_label:
            logger.info(f"成功读取卷标: {volume_label}")
            return {
                "success": True,
                "volume_label": volume_label
            }
        else:
            logger.warning(f"未读取到卷标，驱动器标识: {drive_letter}")
            return {
                "success": False,
                "error": "未读取到卷标",
                "details": {
                    "drive_identifier": drive_letter,
                    "libltfs_available": wrapper is not None,
                    "suggestion": "请检查磁带是否已加载并挂载"
                }
            }
    except Exception as e:
        logger.error(f"读取卷标失败: {str(e)}", exc_info=True)
        return {
            "success": False,
            "error": str(e)
        }


@router.post("/libltfs/volume-label/{drive_letter}")
async def libltfs_set_volume_label(
    drive_letter: str,
    request: LibLTFSVolumeLabelRequest,
    db: AsyncSession = Depends(get_db)
):
    """通过libltfs.dll设置卷标"""
    try:
        wrapper = get_libltfs_wrapper()
        if not wrapper:
            raise HTTPException(status_code=503, detail="libltfs.dll不可用")
        
        success = wrapper.set_volume_label(drive_letter, request.volume_label)
        
        if success:
            return {
                "success": True,
                "message": f"卷标设置成功: {request.volume_label}"
            }
        else:
            return {
                "success": False,
                "error": "卷标设置失败"
            }
    except Exception as e:
        logger.error(f"设置卷标失败: {str(e)}")
        raise HTTPException(status_code=500, detail=str(e))


@router.get("/libltfs/serial-number/{drive_letter}")
async def libltfs_get_serial_number(drive_letter: str, db: AsyncSession = Depends(get_db)):
    """通过libltfs.dll读取序列号（MAM 0x0002）"""
    try:
        logger.info(f"开始读取序列号，驱动器标识: {drive_letter}")
        
        wrapper = get_libltfs_wrapper()
        if not wrapper:
            logger.warning("libltfs.dll不可用")
            return {
                "success": False,
                "error": "libltfs.dll不可用"
            }
        
        logger.debug(f"尝试使用libltfs.dll读取序列号...")
        # 尝试使用libltfs.dll
        serial_number = wrapper.get_serial_number(drive_letter)
        logger.debug(f"libltfs.dll返回结果: {serial_number}")
        
        # 如果DLL方法失败，尝试使用ITDT（备用方法）
        if not serial_number:
            logger.info("libltfs.dll方法失败，尝试使用ITDT读取MAM属性...")
            try:
                result = await tape_tools_manager.read_mam_attributes_itdt(partition=0, attribute_id="0x0002")
                logger.debug(f"ITDT读取MAM属性结果: {result}")
                
                if result.get("success"):
                    # 优先使用解析后的序列号字段
                    serial_number = result.get("serial_number")
                    logger.debug(f"从serial_number字段获取: {serial_number}")
                    
                    # 如果序列号字段为空字符串，说明检测到空值模式，不应该返回
                    if serial_number == "":
                        logger.debug("序列号字段为空字符串（检测到空值模式），不返回序列号")
                        serial_number = None
                    
                    # 如果序列号字段为空，尝试从MAM属性中提取
                    if not serial_number:
                        mam_data = result.get("mam_data", {})
                        mam_attributes = result.get("mam_attributes", {})
                        mam_data_text = result.get("mam_data_text", "")
                        
                        logger.debug(f"MAM数据内容: {mam_data}")
                        logger.debug(f"MAM属性字典: {mam_attributes}")
                        logger.debug(f"MAM数据文本: {mam_data_text}")
                        
                        # 尝试多个可能的键名
                        serial_number = (
                            mam_attributes.get("0x0002") or
                            mam_data.get("serial_number") or
                            mam_data.get("Serial Number") or
                            mam_data.get("serial") or
                            mam_data.get("Serial") or
                            mam_data.get("0x0002")
                        )
                        
                        # 如果还是没有，检查mam_data_text是否是有效的序列号格式
                        if not serial_number and mam_data_text:
                            # 如果mam_data_text是十六进制字符串，可能不是有效的序列号
                            # 检查是否是有效的序列号格式（字母数字，长度合理）
                            if len(mam_data_text) <= 32 and any(c.isalnum() for c in mam_data_text):
                                # 排除纯十六进制字符串（全部是0-9A-F）
                                hex_chars_only = all(c in '0123456789ABCDEFabcdef' for c in mam_data_text.replace(' ', ''))
                                
                                # 排除已知的空值或默认值模式
                                known_empty_patterns = [
                                    '00028000080040000000000000',
                                    '00000000000000000000000000',
                                    '0002800008004000',
                                    '00000000'
                                ]
                                
                                if mam_data_text.upper().replace(' ', '') in [p.upper() for p in known_empty_patterns]:
                                    logger.debug(f"检测到已知的空值模式，不返回序列号")
                                    serial_number = None
                                elif not hex_chars_only:
                                    # 不是纯十六进制，可能是有效序列号
                                    serial_number = mam_data_text
                                    logger.debug(f"从mam_data_text提取序列号: {serial_number}")
                                elif len(mam_data_text) <= 8:
                                    # 短十六进制字符串可能是有效的（如短ID）
                                    serial_number = mam_data_text
                                    logger.debug(f"短十六进制字符串作为序列号: {serial_number}")
                                else:
                                    # 长十六进制字符串可能是二进制数据，不返回
                                    logger.debug(f"长十六进制字符串可能是二进制数据，不返回: {mam_data_text}")
                        
                        logger.debug(f"从MAM属性提取的序列号: {serial_number}")
            except Exception as e:
                logger.error(f"使用ITDT读取MAM属性失败: {str(e)}", exc_info=True)
        
        if serial_number:
            logger.info(f"成功读取序列号: {serial_number}")
            return {
                "success": True,
                "serial_number": serial_number
            }
        else:
            logger.warning(f"未读取到序列号，驱动器标识: {drive_letter}")
            return {
                "success": False,
                "error": "未读取到序列号",
                "details": {
                    "drive_identifier": drive_letter,
                    "libltfs_available": wrapper is not None,
                    "suggestion": "请检查磁带是否已加载，或尝试使用ITDT工具直接读取MAM属性"
                }
            }
    except Exception as e:
        logger.error(f"读取序列号失败: {str(e)}", exc_info=True)
        return {
            "success": False,
            "error": str(e)
        }


@router.post("/libltfs/serial-number/{drive_letter}")
async def libltfs_set_serial_number(
    drive_letter: str,
    request: LibLTFSSerialNumberRequest,
    db: AsyncSession = Depends(get_db)
):
    """通过libltfs.dll设置序列号"""
    try:
        wrapper = get_libltfs_wrapper()
        if not wrapper:
            raise HTTPException(status_code=503, detail="libltfs.dll不可用")
        
        success = wrapper.set_serial_number(drive_letter, request.serial_number)
        
        if success:
            return {
                "success": True,
                "message": f"序列号设置成功: {request.serial_number}"
            }
        else:
            return {
                "success": False,
                "error": "序列号设置失败"
            }
    except Exception as e:
        logger.error(f"设置序列号失败: {str(e)}")
        raise HTTPException(status_code=500, detail=str(e))


@router.get("/libltfs/barcode/{drive_letter}")
async def libltfs_get_barcode(drive_letter: str, db: AsyncSession = Depends(get_db)):
    """通过libltfs.dll读取条码"""
    try:
        wrapper = get_libltfs_wrapper()
        if not wrapper:
            raise HTTPException(status_code=503, detail="libltfs.dll不可用")
        
        barcode = wrapper.get_barcode(drive_letter)
        
        if barcode:
            return {
                "success": True,
                "barcode": barcode
            }
        else:
            return {
                "success": False,
                "error": "未读取到条码"
            }
    except Exception as e:
        logger.error(f"读取条码失败: {str(e)}")
        raise HTTPException(status_code=500, detail=str(e))


@router.get("/libltfs/explore")
async def libltfs_explore_functions(keywords: str = None, db: AsyncSession = Depends(get_db)):
    """探索libltfs.dll的导出函数"""
    try:
        from utils.dll_explorer import explore_libltfs_dll
        
        keyword_list = keywords.split(',') if keywords else None
        if keyword_list:
            keyword_list = [k.strip() for k in keyword_list]
        
        results = explore_libltfs_dll()
        
        # 如果指定了关键词，只返回匹配的函数
        if keyword_list:
            from utils.dll_explorer import DLLExplorer
            base_dir = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
            dll_path = os.path.join(base_dir, "ITDT", "libltfs.dll")
            if os.path.exists(dll_path):
                explorer = DLLExplorer(dll_path)
                matched = explorer.search_functions(keyword_list)
                results['matched_functions'] = matched
        
        return {
            "success": True,
            "results": results
        }
    except Exception as e:
        logger.error(f"探索DLL函数失败: {str(e)}", exc_info=True)
        return {
            "success": False,
            "error": str(e)
        }


@router.post("/libltfs/barcode/{drive_letter}")
async def libltfs_set_barcode(
    drive_letter: str,
    request: LibLTFSBarcodeRequest,
    db: AsyncSession = Depends(get_db)
):
    """通过libltfs.dll设置条码"""
    try:
        wrapper = get_libltfs_wrapper()
        if not wrapper:
            raise HTTPException(status_code=503, detail="libltfs.dll不可用")
        
        success = wrapper.set_barcode(drive_letter, request.barcode)
        
        if success:
            return {
                "success": True,
                "message": f"条码设置成功: {request.barcode}"
            }
        else:
            return {
                "success": False,
                "error": "条码设置失败"
            }
    except Exception as e:
        logger.error(f"设置条码失败: {str(e)}")
        raise HTTPException(status_code=500, detail=str(e))

