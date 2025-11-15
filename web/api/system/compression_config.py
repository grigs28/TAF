#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
系统管理API - 压缩配置
System Management API - Compression Configuration
"""

import logging
import os
import re
import shutil
from pathlib import Path
from typing import Dict, Any, Optional
from fastapi import APIRouter, HTTPException, Request
from pydantic import BaseModel, Field

from config.env_file_manager import EnvFileManager
from config.settings import get_settings

logger = logging.getLogger(__name__)
router = APIRouter()


class CompressionConfigRequest(BaseModel):
    """压缩配置请求"""
    compression_method: Optional[str] = Field(None, description="压缩方法: pgzip、py7zr 或 7zip_command")
    sevenzip_path: Optional[str] = Field(None, description="7-Zip程序路径")
    compression_threads: Optional[int] = Field(None, description="py7zr线程数")
    compression_command_threads: Optional[int] = Field(None, description="7-Zip命令行线程数")
    compression_level: Optional[int] = Field(None, description="压缩级别 (0-9)")
    compress_directly_to_tape: Optional[bool] = Field(None, description="是否直接压缩到磁带机")
    pgzip_block_size: Optional[str] = Field(None, description="PGZip块大小（如 512M、1G）")
    pgzip_threads: Optional[int] = Field(None, description="PGZip线程数")


@router.get("/compression")
async def get_compression_config():
    """获取压缩配置"""
    try:
        settings = get_settings()
        
        # 读取.env文件中的值（如果有）
        env_manager = EnvFileManager()
        env_values = env_manager.read_env_file()
        
        # 获取压缩级别（统一使用 COMPRESSION_LEVEL）
        compression_level = int(env_values.get("COMPRESSION_LEVEL", settings.COMPRESSION_LEVEL))
        
        # 获取7-Zip命令行线程数（优先使用 COMPRESSION_COMMAND_THREADS，否则使用 WEB_WORKERS）
        compression_command_threads = env_values.get("COMPRESSION_COMMAND_THREADS")
        if compression_command_threads is None:
            # 如果未设置 COMPRESSION_COMMAND_THREADS，使用 WEB_WORKERS
            compression_command_threads = env_values.get("WEB_WORKERS", settings.WEB_WORKERS)
        compression_command_threads = int(compression_command_threads)
        
        # 获取PGZip配置
        pgzip_block_size = env_values.get("PGZIP_BLOCK_SIZE", settings.PGZIP_BLOCK_SIZE)
        pgzip_threads = int(env_values.get("PGZIP_THREADS", env_values.get("COMPRESSION_THREADS", settings.PGZIP_THREADS)))

        # 获取直接压缩到磁带配置
        compress_directly_to_tape_str = env_values.get("COMPRESS_DIRECTLY_TO_TAPE")
        if compress_directly_to_tape_str is not None:
            compress_directly_to_tape = compress_directly_to_tape_str.lower() in ("true", "1", "yes", "on")
        else:
            compress_directly_to_tape = getattr(settings, 'COMPRESS_DIRECTLY_TO_TAPE', True)
        
        return {
            "compression_method": env_values.get("COMPRESSION_METHOD", settings.COMPRESSION_METHOD),
            "sevenzip_path": env_values.get("SEVENZIP_PATH", settings.SEVENZIP_PATH),
            "compression_threads": int(env_values.get("COMPRESSION_THREADS", settings.COMPRESSION_THREADS)),
            "compression_command_threads": compression_command_threads,
            "compression_level": compression_level,
            "compress_directly_to_tape": compress_directly_to_tape,
            "pgzip_block_size": pgzip_block_size,
            "pgzip_threads": pgzip_threads,
            # 额外的信息
            "available_methods": ["pgzip", "py7zr", "7zip_command"],
            "default_sevenzip_paths": [
                r"C:\Program Files\7-Zip\7z.exe",
                r"C:\Program Files (x86)\7-Zip\7z.exe"
            ]
        }
    except Exception as e:
        logger.error(f"获取压缩配置失败: {str(e)}")
        import traceback
        logger.error(traceback.format_exc())
        raise HTTPException(status_code=500, detail=str(e))


@router.put("/compression")
async def update_compression_config(config: CompressionConfigRequest, request: Request):
    """更新压缩配置"""
    try:
        env_manager = EnvFileManager()
        
        # 准备更新的配置
        updates = {}
        
        if config.compression_method is not None:
            if config.compression_method not in ["pgzip", "py7zr", "7zip_command"]:
                raise HTTPException(
                    status_code=400,
                    detail=f"无效的压缩方法: {config.compression_method}，必须是 'pgzip'、'py7zr' 或 '7zip_command'"
                )
            updates["COMPRESSION_METHOD"] = config.compression_method
        
        if config.sevenzip_path is not None:
            # 验证路径是否存在（如果提供了路径）
            if config.sevenzip_path.strip():
                path = Path(config.sevenzip_path)
                if not path.exists():
                    raise HTTPException(
                        status_code=400,
                        detail=f"指定的7-Zip路径不存在: {config.sevenzip_path}"
                    )
                if path.name.lower() != "7z.exe":
                    raise HTTPException(
                        status_code=400,
                        detail=f"指定的文件不是7z.exe: {config.sevenzip_path}"
                    )
                updates["SEVENZIP_PATH"] = str(path.absolute())
            else:
                # 清空路径，尝试自动查找
                updates["SEVENZIP_PATH"] = ""
        
        if config.compression_threads is not None:
            if config.compression_threads < 1 or config.compression_threads > 64:
                raise HTTPException(
                    status_code=400,
                    detail="compression_threads 必须在 1-64 之间"
                )
            updates["COMPRESSION_THREADS"] = str(config.compression_threads)
        
        if config.compression_command_threads is not None:
            if config.compression_command_threads < 1 or config.compression_command_threads > 64:
                raise HTTPException(
                    status_code=400,
                    detail="compression_command_threads 必须在 1-64 之间"
                )
            updates["COMPRESSION_COMMAND_THREADS"] = str(config.compression_command_threads)
        
        if config.compression_level is not None:
            if config.compression_level < 0 or config.compression_level > 9:
                raise HTTPException(
                    status_code=400,
                    detail="compression_level 必须在 0-9 之间"
                )
            updates["COMPRESSION_LEVEL"] = str(config.compression_level)
        
        if config.compress_directly_to_tape is not None:
            updates["COMPRESS_DIRECTLY_TO_TAPE"] = "true" if config.compress_directly_to_tape else "false"

        if config.pgzip_threads is not None:
            if config.pgzip_threads < 1 or config.pgzip_threads > 64:
                raise HTTPException(
                    status_code=400,
                    detail="pgzip_threads 必须在 1-64 之间"
                )
            updates["PGZIP_THREADS"] = str(config.pgzip_threads)

        if config.pgzip_block_size is not None:
            block_value = config.pgzip_block_size.strip()
            if not block_value:
                raise HTTPException(status_code=400, detail="pgzip_block_size 不能为空")
            if not re.match(r'^\d+(\.\d+)?[kKmMgG]?$', block_value):
                raise HTTPException(
                    status_code=400,
                    detail="pgzip_block_size 格式不正确，应为数字加可选单位（K/M/G）"
                )
            updates["PGZIP_BLOCK_SIZE"] = block_value.upper()
        
        # 如果没有提供sevenzip_path，但选择了7zip_command方法，尝试自动查找
        if config.compression_method == "7zip_command" and config.sevenzip_path is None:
            settings = get_settings()
            current_path = settings.SEVENZIP_PATH
            if not current_path or not Path(current_path).exists():
                # 尝试自动查找
                possible_paths = [
                    r"C:\Program Files\7-Zip\7z.exe",
                    r"C:\Program Files (x86)\7-Zip\7z.exe",
                ]
                # 也检查PATH中
                which_path = shutil.which("7z.exe")
                if which_path:
                    possible_paths.insert(0, which_path)
                
                found_path = None
                for path in possible_paths:
                    if Path(path).exists() if path else False:
                        found_path = path
                        break
                
                if found_path:
                    updates["SEVENZIP_PATH"] = found_path
                    logger.info(f"自动找到7-Zip路径: {found_path}")
                else:
                    logger.warning("未找到7-Zip程序，请手动指定路径")
        
        # 写入.env文件
        if updates:
            env_manager.write_env_file(updates)
            logger.info(f"更新压缩配置: {updates}")
        
        return {
            "success": True,
            "message": "压缩配置更新成功",
            "updated": updates
        }
        
    except HTTPException:
        raise
    except Exception as e:
        logger.error(f"更新压缩配置失败: {str(e)}")
        import traceback
        logger.error(traceback.format_exc())
        raise HTTPException(status_code=500, detail=str(e))


@router.get("/compression/check-sevenzip")
async def check_sevenzip_path(path: Optional[str] = None):
    """检查7-Zip路径是否有效"""
    try:
        if path:
            # 检查指定路径
            path_obj = Path(path)
            exists = path_obj.exists()
            is_7z = path_obj.name.lower() == "7z.exe" if exists else False
            
            return {
                "path": str(path_obj.absolute()),
                "exists": exists,
                "is_valid": exists and is_7z,
                "message": "路径有效" if (exists and is_7z) else ("文件不存在" if not exists else "不是7z.exe文件")
            }
        else:
            # 自动查找
            possible_paths = [
                r"C:\Program Files\7-Zip\7z.exe",
                r"C:\Program Files (x86)\7-Zip\7z.exe",
            ]
            which_path = shutil.which("7z.exe")
            if which_path:
                possible_paths.insert(0, which_path)
            
            found_paths = []
            for p in possible_paths:
                if Path(p).exists():
                    found_paths.append({
                        "path": p,
                        "exists": True,
                        "is_valid": True
                    })
            
            return {
                "found_paths": found_paths,
                "auto_detected": len(found_paths) > 0,
                "message": f"找到 {len(found_paths)} 个可能的7-Zip安装路径" if found_paths else "未找到7-Zip安装路径"
            }
            
    except Exception as e:
        logger.error(f"检查7-Zip路径失败: {str(e)}")
        raise HTTPException(status_code=500, detail=str(e))

