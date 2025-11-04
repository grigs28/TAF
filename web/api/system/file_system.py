#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
系统管理API - file_system
System Management API - file_system
"""

import logging
from typing import Dict, Any, Optional
from datetime import datetime
from fastapi import APIRouter, HTTPException, Request
from pydantic import BaseModel, Field

# 文件系统相关路由，无需导入模型

logger = logging.getLogger(__name__)
router = APIRouter()

@router.get("/file-system/drives")
async def get_file_system_drives():
    """获取服务器端的驱动器列表（Windows盘符或Linux挂载点）"""
    try:
        import os
        import platform
        
        drives = []
        system = platform.system()
        
        if system == "Windows":
            # Windows系统：获取所有盘符
            import string
            try:
                # 使用Windows API获取驱动器类型
                import ctypes
                from ctypes import wintypes
                
                kernel32 = ctypes.windll.kernel32
                GetDriveType = kernel32.GetDriveTypeW
                
                for letter in string.ascii_uppercase:
                    drive_path = f"{letter}:\\"
                    drive_type = GetDriveType(drive_path)
                    
                    # 驱动器类型：2=可移动, 3=固定, 4=网络, 5=CD-ROM
                    # 只显示可访问的本地驱动器
                    if drive_type in (2, 3, 5) and os.path.exists(drive_path):
                        drive_info = {
                            "path": drive_path,
                            "name": f"{letter}盘",
                            "type": "local",
                            "available": True
                        }
                        drives.append(drive_info)
            except Exception:
                # 如果API调用失败，使用简单的方法
                import string
                for letter in string.ascii_uppercase:
                    drive_path = f"{letter}:\\"
                    if os.path.exists(drive_path):
                        drive_info = {
                            "path": drive_path,
                            "name": f"{letter}盘",
                            "type": "local",
                            "available": True
                        }
                        drives.append(drive_info)
        elif system == "Linux":
            # Linux系统：获取挂载点
            import subprocess
            try:
                # 获取所有挂载点
                result = subprocess.run(['df', '-h'], capture_output=True, text=True)
                lines = result.stdout.strip().split('\n')[1:]  # 跳过标题行
                for line in lines:
                    parts = line.split()
                    if len(parts) >= 6:
                        mount_point = parts[5]
                        if mount_point.startswith('/') and os.path.isdir(mount_point):
                            drive_info = {
                                "path": mount_point,
                                "name": os.path.basename(mount_point) or mount_point,
                                "type": "local",
                                "available": True
                            }
                            drives.append(drive_info)
            except Exception:
                # 如果df命令不可用，使用常见挂载点
                common_mounts = ['/', '/home', '/var', '/usr', '/tmp']
                for mount in common_mounts:
                    if os.path.exists(mount):
                        drive_info = {
                            "path": mount,
                            "name": os.path.basename(mount) or mount,
                            "type": "local",
                            "available": True
                        }
                        drives.append(drive_info)
        
        # 如果没有找到驱动器，至少返回根目录
        if not drives:
            root_path = "C:\\" if system == "Windows" else "/"
            if os.path.exists(root_path):
                drives.append({
                    "path": root_path,
                    "name": "根目录" if system == "Linux" else "C盘",
                    "type": "local",
                    "available": True
                })
        
        return {"drives": drives}
        
    except Exception as e:
        logger.error(f"获取驱动器列表失败: {str(e)}")
        raise HTTPException(status_code=500, detail=str(e))


@router.get("/file-system/list")
async def list_file_system(path: str = None):
    """列出指定路径下的目录和文件
    
    参数:
        path: 要列出的路径（如果为空，返回驱动器列表）
    """
    try:
        import os
        import platform
        from pathlib import Path
        
        if not path:
            # 返回驱动器列表
            return await get_file_system_drives()
        
        # 验证路径
        if not os.path.exists(path):
            raise HTTPException(status_code=404, detail=f"路径不存在: {path}")
        
        if not os.path.isdir(path):
            raise HTTPException(status_code=400, detail=f"不是目录: {path}")
        
        # 列出目录内容
        items = []
        try:
            entries = os.listdir(path)
            for entry in sorted(entries):
                entry_path = os.path.join(path, entry)
                try:
                    stat = os.stat(entry_path)
                    is_dir = os.path.isdir(entry_path)
                    
                    items.append({
                        "name": entry,
                        "path": entry_path,
                        "type": "directory" if is_dir else "file",
                        "size": stat.st_size if not is_dir else None,
                        "modified": stat.st_mtime,
                        "readable": os.access(entry_path, os.R_OK),
                        "writable": os.access(entry_path, os.W_OK)
                    })
                except (OSError, PermissionError):
                    # 跳过无法访问的条目
                    continue
            
            # 如果有父目录，添加".."
            parent_path = os.path.dirname(path)
            if parent_path and parent_path != path:
                items.insert(0, {
                    "name": "..",
                    "path": parent_path,
                    "type": "directory",
                    "size": None,
                    "modified": None,
                    "readable": True,
                    "writable": False
                })
                
        except PermissionError:
            raise HTTPException(status_code=403, detail=f"无权限访问路径: {path}")
        except Exception as e:
            raise HTTPException(status_code=500, detail=f"列出目录失败: {str(e)}")
        
        return {
            "current_path": path,
            "items": items,
            "parent_path": os.path.dirname(path) if os.path.dirname(path) != path else None
        }
        
    except HTTPException:
        raise
    except Exception as e:
        logger.error(f"列出文件系统失败: {str(e)}")
        raise HTTPException(status_code=500, detail=str(e))
