#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
网络路径工具模块
Network Path Utilities
"""

import os
import logging
import platform
from typing import List, Optional, Dict
from pathlib import Path, WindowsPath

logger = logging.getLogger(__name__)


def is_unc_path(path: str) -> bool:
    """检查路径是否为 UNC 网络路径
    
    Args:
        path: 路径字符串
        
    Returns:
        bool: 如果是 UNC 路径返回 True
    """
    if not path:
        return False
    
    # Windows UNC 路径格式: \\server\share 或 \\server\share\path
    path_normalized = path.replace('/', '\\')
    return path_normalized.startswith('\\\\') and not path_normalized.startswith('\\\\?\\')


def normalize_unc_path(path: str) -> str:
    """规范化 UNC 路径
    
    Args:
        path: 路径字符串
        
    Returns:
        str: 规范化后的路径（统一使用反斜杠）
    """
    if not path:
        return path
    
    # 将正斜杠转换为反斜杠（UNC 路径使用反斜杠）
    return path.replace('/', '\\')


def get_unc_server_and_share(path: str) -> Optional[Dict[str, str]]:
    """从 UNC 路径中提取服务器和共享名称
    
    Args:
        path: UNC 路径，如 \\192.168.0.79 或 \\192.168.0.79\yz
        
    Returns:
        Dict[str, str]: 包含 'server' 和 'share' 的字典，如果路径无效返回 None
    """
    if not is_unc_path(path):
        return None
    
    path_normalized = normalize_unc_path(path)
    # 移除开头的 \\
    path_parts = path_normalized[2:].split('\\', 1)
    
    if len(path_parts) < 1:
        return None
    
    server = path_parts[0]
    share = path_parts[1] if len(path_parts) > 1 else None
    
    return {
        'server': server,
        'share': share,
        'full_path': path_normalized
    }


def list_network_shares(server: str) -> List[Dict[str, str]]:
    """列出指定服务器的所有共享
    
    Args:
        server: 服务器地址，如 '192.168.0.79'
        
    Returns:
        List[Dict[str, str]]: 共享列表，每个字典包含 'name' 和 'path'
    """
    if platform.system() != 'Windows':
        logger.warning("列出网络共享功能仅在 Windows 系统上支持")
        return []
    
    try:
        import win32net
        import win32netcon
        
        shares = []
        resume_handle = 0
        
        while True:
            # 枚举共享
            result, share_list, total, resume_handle = win32net.NetShareEnum(
                f"\\\\{server}",
                0,  # SHARE_INFO_0
                resume_handle
            )
            
            for share_info in share_list:
                share_name = share_info['netname']
                share_type = share_info['type']
                
                # 只返回磁盘共享（排除 IPC$, ADMIN$ 等系统共享）
                # share_type & 0x1 == 0 表示磁盘共享
                # 0x80000000 是 STYPE_DISKTREE
                if share_type & 0x1 == 0:  # 磁盘共享
                    share_path = f"\\\\{server}\\{share_name}"
                    shares.append({
                        'name': share_name,
                        'path': share_path,
                        'type': 'disk'
                    })
            
            if resume_handle == 0:
                break
        
        logger.info(f"在服务器 {server} 上找到 {len(shares)} 个共享")
        return shares
        
    except ImportError as e:
        logger.warning(f"win32net 模块未安装，无法列出网络共享: {str(e)}。请安装 pywin32: pip install pywin32")
        return []
    except Exception as e:
        error_msg = str(e)
        # 常见错误：访问被拒绝、网络不可达等
        if '拒绝访问' in error_msg or 'Access denied' in error_msg:
            logger.warning(f"无法访问服务器 {server} 的共享列表（权限不足）")
        elif '网络路径未找到' in error_msg or 'Network path not found' in error_msg:
            logger.warning(f"无法找到服务器 {server}（网络不可达）")
        else:
            logger.error(f"列出网络共享失败 {server}: {error_msg}")
        return []


def expand_unc_path(path: str) -> List[str]:
    """展开 UNC 路径
    
    如果路径是 \\server（没有指定共享），则列出所有共享并返回完整路径列表
    如果路径是 \\server\share，则直接返回该路径
    
    Args:
        path: UNC 路径
        
    Returns:
        List[str]: 展开后的路径列表
    """
    if not is_unc_path(path):
        return [path]
    
    path_info = get_unc_server_and_share(path)
    if not path_info:
        return [path]
    
    server = path_info['server']
    share = path_info['share']
    
    # 如果指定了共享，直接返回
    if share:
        return [normalize_unc_path(path)]
    
    # 如果没有指定共享，列出所有共享
    logger.info(f"未指定共享名称，列出服务器 {server} 的所有共享...")
    shares = list_network_shares(server)
    
    if not shares:
        logger.warning(f"无法列出服务器 {server} 的共享，尝试直接访问根路径")
        return [normalize_unc_path(path)]
    
    # 返回所有共享的完整路径
    expanded_paths = [share_info['path'] for share_info in shares]
    logger.info(f"展开路径 {path} 为 {len(expanded_paths)} 个共享路径")
    return expanded_paths


def check_path_exists(path: str) -> bool:
    """检查路径是否存在（支持 UNC 路径）
    
    Args:
        path: 路径字符串
        
    Returns:
        bool: 如果路径存在返回 True
    """
    if not path:
        return False
    
    try:
        # 对于 UNC 路径，使用 os.path.exists 应该可以工作
        # 但可能需要先确保网络连接
        normalized_path = normalize_unc_path(path) if is_unc_path(path) else path
        
        # 尝试使用 Path.exists()，它应该支持 UNC 路径
        path_obj = Path(normalized_path)
        
        # 对于 UNC 路径，可能需要特殊处理
        if is_unc_path(normalized_path):
            # 尝试访问路径的根目录
            try:
                # 检查是否可以访问路径
                return path_obj.exists()
            except Exception as e:
                logger.debug(f"检查 UNC 路径存在性时出错 {normalized_path}: {str(e)}")
                # 尝试使用 os.path.exists 作为备选
                return os.path.exists(normalized_path)
        else:
            return path_obj.exists()
            
    except Exception as e:
        logger.error(f"检查路径存在性失败 {path}: {str(e)}")
        return False


def validate_network_path(path: str) -> Dict[str, any]:
    """验证网络路径
    
    Args:
        path: 路径字符串
        
    Returns:
        Dict: 验证结果，包含 'valid', 'exists', 'is_unc', 'expanded_paths' 等字段
    """
    result = {
        'valid': False,
        'exists': False,
        'is_unc': False,
        'expanded_paths': [],
        'error': None
    }
    
    try:
        if not path:
            result['error'] = '路径为空'
            return result
        
        result['is_unc'] = is_unc_path(path)
        
        if result['is_unc']:
            # 展开 UNC 路径
            expanded_paths = expand_unc_path(path)
            result['expanded_paths'] = expanded_paths
            
            # 检查至少有一个路径存在
            for expanded_path in expanded_paths:
                if check_path_exists(expanded_path):
                    result['exists'] = True
                    result['valid'] = True
                    break
            
            if not result['exists']:
                result['error'] = f'无法访问网络路径: {path}'
        else:
            # 普通路径验证
            result['exists'] = check_path_exists(path)
            result['valid'] = result['exists']
            result['expanded_paths'] = [path]
            
            if not result['exists']:
                result['error'] = f'路径不存在: {path}'
                
    except Exception as e:
        result['error'] = str(e)
        logger.error(f"验证路径失败 {path}: {str(e)}")
    
    return result

