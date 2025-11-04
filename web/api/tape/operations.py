#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
磁带管理API - operations
Tape Management API - operations
"""

import logging
import traceback
from typing import List, Dict, Any, Optional
from datetime import datetime
from fastapi import APIRouter, HTTPException, Request, Depends
from pydantic import BaseModel

from .models import FormatRequest
from models.system_log import OperationType, LogCategory, LogLevel
from utils.log_utils import log_operation, log_system

logger = logging.getLogger(__name__)
router = APIRouter()

@router.post("/load")
async def load_tape(request: Request, tape_id: str):
    """加载磁带"""
    start_time = datetime.now()
    ip_address = request.client.host if request.client else None
    request_method = "POST"
    request_url = str(request.url)
    
    try:
        system = request.app.state.system
        if not system:
            raise HTTPException(status_code=500, detail="系统未初始化")

        success = await system.tape_manager.load_tape(tape_id)
        duration_ms = int((datetime.now() - start_time).total_seconds() * 1000)
        
        if success:
            await log_operation(
                operation_type=OperationType.TAPE_LOAD,
                resource_type="tape",
                resource_id=tape_id,
                operation_name="加载磁带",
                operation_description=f"加载磁带 {tape_id}",
                category="tape",
                success=True,
                result_message=f"磁带 {tape_id} 加载成功",
                ip_address=ip_address,
                request_method=request_method,
                request_url=request_url,
                duration_ms=duration_ms
            )
            return {"success": True, "message": f"磁带 {tape_id} 加载成功"}
        else:
            await log_operation(
                operation_type=OperationType.TAPE_LOAD,
                resource_type="tape",
                resource_id=tape_id,
                operation_name="加载磁带",
                operation_description=f"加载磁带 {tape_id}",
                category="tape",
                success=False,
                error_message="磁带加载失败",
                ip_address=ip_address,
                request_method=request_method,
                request_url=request_url,
                duration_ms=duration_ms
            )
            raise HTTPException(status_code=500, detail="磁带加载失败")

    except HTTPException:
        raise
    except Exception as e:
        duration_ms = int((datetime.now() - start_time).total_seconds() * 1000)
        error_msg = f"加载磁带失败: {str(e)}"
        logger.error(error_msg, exc_info=True)
        await log_operation(
            operation_type=OperationType.TAPE_LOAD,
            resource_type="tape",
            resource_id=tape_id,
            operation_name="加载磁带",
            operation_description=f"加载磁带 {tape_id}",
            category="tape",
            success=False,
            error_message=str(e),
            ip_address=ip_address,
            request_method=request_method,
            request_url=request_url,
            duration_ms=duration_ms
        )
        await log_system(
            level=LogLevel.ERROR,
            category=LogCategory.TAPE,
            message=error_msg,
            module="web.api.tape.operations",
            function="load_tape",
            exception_type=type(e).__name__,
            stack_trace=traceback.format_exc(),
            duration_ms=duration_ms
        )
        raise HTTPException(status_code=500, detail=str(e))


@router.post("/unload")
async def unload_tape(request: Request):
    """卸载磁带"""
    start_time = datetime.now()
    ip_address = request.client.host if request.client else None
    request_method = "POST"
    request_url = str(request.url)
    
    try:
        system = request.app.state.system
        if not system:
            raise HTTPException(status_code=500, detail="系统未初始化")

        success = await system.tape_manager.unload_tape()
        duration_ms = int((datetime.now() - start_time).total_seconds() * 1000)
        
        if success:
            await log_operation(
                operation_type=OperationType.TAPE_UNLOAD,
                resource_type="tape",
                operation_name="卸载磁带",
                operation_description="卸载磁带",
                category="tape",
                success=True,
                result_message="磁带卸载成功",
                ip_address=ip_address,
                request_method=request_method,
                request_url=request_url,
                duration_ms=duration_ms
            )
            return {"success": True, "message": "磁带卸载成功"}
        else:
            await log_operation(
                operation_type=OperationType.TAPE_UNLOAD,
                resource_type="tape",
                operation_name="卸载磁带",
                operation_description="卸载磁带",
                category="tape",
                success=False,
                error_message="磁带卸载失败",
                ip_address=ip_address,
                request_method=request_method,
                request_url=request_url,
                duration_ms=duration_ms
            )
            raise HTTPException(status_code=500, detail="磁带卸载失败")

    except HTTPException:
        raise
    except Exception as e:
        duration_ms = int((datetime.now() - start_time).total_seconds() * 1000)
        error_msg = f"卸载磁带失败: {str(e)}"
        logger.error(error_msg, exc_info=True)
        await log_operation(
            operation_type=OperationType.TAPE_UNLOAD,
            resource_type="tape",
            operation_name="卸载磁带",
            operation_description="卸载磁带",
            category="tape",
            success=False,
            error_message=str(e),
            ip_address=ip_address,
            request_method=request_method,
            request_url=request_url,
            duration_ms=duration_ms
        )
        await log_system(
            level=LogLevel.ERROR,
            category=LogCategory.TAPE,
            message=error_msg,
            module="web.api.tape.operations",
            function="unload_tape",
            exception_type=type(e).__name__,
            stack_trace=traceback.format_exc(),
            duration_ms=duration_ms
        )
        raise HTTPException(status_code=500, detail=str(e))


@router.post("/erase")
async def erase_tape(request: Request, tape_id: str):
    """擦除磁带"""
    start_time = datetime.now()
    ip_address = request.client.host if request.client else None
    request_method = "POST"
    request_url = str(request.url)
    
    try:
        system = request.app.state.system
        if not system:
            raise HTTPException(status_code=500, detail="系统未初始化")

        success = await system.tape_manager.erase_tape(tape_id)
        duration_ms = int((datetime.now() - start_time).total_seconds() * 1000)
        
        if success:
            await log_operation(
                operation_type=OperationType.TAPE_ERASE,
                resource_type="tape",
                resource_id=tape_id,
                operation_name="擦除磁带",
                operation_description=f"擦除磁带 {tape_id}",
                category="tape",
                success=True,
                result_message=f"磁带 {tape_id} 擦除成功",
                ip_address=ip_address,
                request_method=request_method,
                request_url=request_url,
                duration_ms=duration_ms
            )
            return {"success": True, "message": f"磁带 {tape_id} 擦除成功"}
        else:
            await log_operation(
                operation_type=OperationType.TAPE_ERASE,
                resource_type="tape",
                resource_id=tape_id,
                operation_name="擦除磁带",
                operation_description=f"擦除磁带 {tape_id}",
                category="tape",
                success=False,
                error_message="磁带擦除失败",
                ip_address=ip_address,
                request_method=request_method,
                request_url=request_url,
                duration_ms=duration_ms
            )
            raise HTTPException(status_code=500, detail="磁带擦除失败")

    except HTTPException:
        raise
    except Exception as e:
        duration_ms = int((datetime.now() - start_time).total_seconds() * 1000)
        error_msg = f"擦除磁带失败: {str(e)}"
        logger.error(error_msg, exc_info=True)
        await log_operation(
            operation_type=OperationType.TAPE_ERASE,
            resource_type="tape",
            resource_id=tape_id,
            operation_name="擦除磁带",
            operation_description=f"擦除磁带 {tape_id}",
            category="tape",
            success=False,
            error_message=str(e),
            ip_address=ip_address,
            request_method=request_method,
            request_url=request_url,
            duration_ms=duration_ms
        )
        await log_system(
            level=LogLevel.ERROR,
            category=LogCategory.TAPE,
            message=error_msg,
            module="web.api.tape.operations",
            function="erase_tape",
            exception_type=type(e).__name__,
            stack_trace=traceback.format_exc(),
            duration_ms=duration_ms
        )
        raise HTTPException(status_code=500, detail=str(e))


@router.get("/check-format")
async def check_tape_format(request: Request):
    """检查磁带是否已格式化"""
    try:
        system = request.app.state.system
        if not system:
            return {
                "success": False,
                "formatted": False,
                "message": "系统未初始化"
            }
        
        # 检查是否有磁带设备
        if not system.tape_manager.scsi_interface.tape_devices or len(system.tape_manager.scsi_interface.tape_devices) == 0:
            return {
                "success": False,
                "formatted": False,
                "message": "未检测到磁带设备"
            }
        
        # 尝试读取磁带标签，如果成功则认为已格式化
        try:
            metadata = await system.tape_manager.tape_operations._read_tape_label()
            
            return {
                "success": True,
                "formatted": metadata is not None,
                "metadata": metadata if metadata else None
            }
        except Exception as e:
            # 读取失败通常意味着未格式化或磁带为空
            logger.debug(f"读取磁带标签失败（可能未格式化）: {str(e)}")
            return {
                "success": True,
                "formatted": False,
                "metadata": None
            }
    
    except Exception as e:
        # 其他错误
        logger.error(f"检查磁带格式异常: {str(e)}")
        return {
            "success": False,
            "formatted": False,
            "message": str(e)
        }


class FormatRequest(BaseModel):
    """格式化请求模型"""
    force: bool = False


@router.post("/format")
async def format_tape(request: Request, format_request: FormatRequest = FormatRequest()):
    """格式化磁带"""
    start_time = datetime.now()
    ip_address = request.client.host if request.client else None
    request_method = "POST"
    request_url = str(request.url)
    
    try:
        system = request.app.state.system
        if not system:
            await log_operation(
                operation_type=OperationType.TAPE_FORMAT,
                resource_type="tape",
                operation_name="格式化磁带",
                operation_description="格式化磁带",
                category="tape",
                success=False,
                error_message="系统未初始化",
                ip_address=ip_address,
                request_method=request_method,
                request_url=request_url,
                duration_ms=int((datetime.now() - start_time).total_seconds() * 1000)
            )
            return {
                "success": False,
                "message": "系统未初始化"
            }
        
        # 检查是否有磁带设备
        if not system.tape_manager.scsi_interface.tape_devices or len(system.tape_manager.scsi_interface.tape_devices) == 0:
            await log_operation(
                operation_type=OperationType.TAPE_FORMAT,
                resource_type="tape",
                operation_name="格式化磁带",
                operation_description="格式化磁带",
                category="tape",
                success=False,
                error_message="未检测到磁带设备",
                ip_address=ip_address,
                request_method=request_method,
                request_url=request_url,
                duration_ms=int((datetime.now() - start_time).total_seconds() * 1000)
            )
            return {
                "success": False,
                "message": "未检测到磁带设备"
            }
        
        # 先读取现有标签（如果有），格式化后重新写入以保持标签不变
        existing_label = None
        
        # 检查是否已格式化
        try:
            existing_label = await system.tape_manager.tape_operations._read_tape_label()
            if existing_label and not format_request.force:
                # 已格式化且不强制，拒绝
                await log_operation(
                    operation_type=OperationType.TAPE_FORMAT,
                    resource_type="tape",
                    resource_id=existing_label.get('tape_id'),
                    operation_name="格式化磁带",
                    operation_description=f"格式化磁带 {existing_label.get('tape_id')}",
                    category="tape",
                    success=False,
                    error_message="磁带已格式化，如需强制格式化请使用force=true参数",
                    ip_address=ip_address,
                    request_method=request_method,
                    request_url=request_url,
                    duration_ms=int((datetime.now() - start_time).total_seconds() * 1000)
                )
                return {
                    "success": False,
                    "message": "磁带已格式化，如需强制格式化请使用force=true参数"
                }
        except Exception as e:
            logger.debug(f"读取磁带标签失败（继续格式化）: {str(e)}")
        
        # 使用SCSI接口格式化
        success = await system.tape_manager.scsi_interface.format_tape(format_type=0)
        duration_ms = int((datetime.now() - start_time).total_seconds() * 1000)
        
        if success:
            # 如果格式化前有标签，重新写入以保持标签不变
            if existing_label:
                try:
                    write_success = await system.tape_manager.tape_operations._write_tape_label(existing_label)
                    if write_success:
                        logger.info(f"格式化后重新写入磁带标签: {existing_label.get('tape_id')}")
                        await log_operation(
                            operation_type=OperationType.TAPE_FORMAT,
                            resource_type="tape",
                            resource_id=existing_label.get('tape_id'),
                            operation_name="格式化磁带",
                            operation_description=f"格式化磁带 {existing_label.get('tape_id')}，标签已保留",
                            category="tape",
                            success=True,
                            result_message="磁带格式化成功，标签已保留",
                            ip_address=ip_address,
                            request_method=request_method,
                            request_url=request_url,
                            duration_ms=duration_ms
                        )
                        return {"success": True, "message": "磁带格式化成功，标签已保留"}
                    else:
                        logger.warning("格式化成功，但重新写入标签失败")
                        await log_operation(
                            operation_type=OperationType.TAPE_FORMAT,
                            resource_type="tape",
                            resource_id=existing_label.get('tape_id'),
                            operation_name="格式化磁带",
                            operation_description=f"格式化磁带 {existing_label.get('tape_id')}（但标签未重写）",
                            category="tape",
                            success=True,
                            result_message="磁带格式化成功（但标签未重写）",
                            ip_address=ip_address,
                            request_method=request_method,
                            request_url=request_url,
                            duration_ms=duration_ms
                        )
                        return {"success": True, "message": "磁带格式化成功（但标签未重写）"}
                except Exception as e:
                    logger.warning(f"重新写入标签时出错: {str(e)}")
                    await log_operation(
                        operation_type=OperationType.TAPE_FORMAT,
                        resource_type="tape",
                        resource_id=existing_label.get('tape_id'),
                        operation_name="格式化磁带",
                        operation_description=f"格式化磁带 {existing_label.get('tape_id')}（但标签未重写）",
                        category="tape",
                        success=True,
                        result_message="磁带格式化成功（但标签未重写）",
                        error_message=f"重新写入标签失败: {str(e)}",
                        ip_address=ip_address,
                        request_method=request_method,
                        request_url=request_url,
                        duration_ms=duration_ms
                    )
                    return {"success": True, "message": "磁带格式化成功（但标签未重写）"}
            
            await log_operation(
                operation_type=OperationType.TAPE_FORMAT,
                resource_type="tape",
                operation_name="格式化磁带",
                operation_description="格式化磁带",
                category="tape",
                success=True,
                result_message="磁带格式化成功",
                ip_address=ip_address,
                request_method=request_method,
                request_url=request_url,
                duration_ms=duration_ms
            )
            return {"success": True, "message": "磁带格式化成功"}
        else:
            await log_operation(
                operation_type=OperationType.TAPE_FORMAT,
                resource_type="tape",
                operation_name="格式化磁带",
                operation_description="格式化磁带",
                category="tape",
                success=False,
                error_message="磁带格式化失败，请检查设备状态和磁带是否正确加载",
                ip_address=ip_address,
                request_method=request_method,
                request_url=request_url,
                duration_ms=duration_ms
            )
            return {
                "success": False,
                "message": "磁带格式化失败，请检查设备状态和磁带是否正确加载"
            }

    except Exception as e:
        duration_ms = int((datetime.now() - start_time).total_seconds() * 1000)
        error_msg = f"格式化磁带异常: {str(e)}"
        logger.error(error_msg, exc_info=True)
        await log_operation(
            operation_type=OperationType.TAPE_FORMAT,
            resource_type="tape",
            operation_name="格式化磁带",
            operation_description="格式化磁带",
            category="tape",
            success=False,
            error_message=str(e),
            ip_address=ip_address,
            request_method=request_method,
            request_url=request_url,
            duration_ms=duration_ms
        )
        await log_system(
            level=LogLevel.ERROR,
            category=LogCategory.TAPE,
            message=error_msg,
            module="web.api.tape.operations",
            function="format_tape",
            exception_type=type(e).__name__,
            stack_trace=traceback.format_exc(),
            duration_ms=duration_ms
        )
        return {
            "success": False,
            "message": f"格式化失败: {str(e)}"
        }


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


