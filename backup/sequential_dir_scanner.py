#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
顺序目录扫描模块
Sequential Directory Scanner Module

专用于openGauss模式下的高性能顺序目录扫描
使用os.scandir顺序执行，优化速度，去掉不必要的检测，保留排除项
"""

import os
import logging
import time
from collections import deque
from pathlib import Path
from typing import List, Dict, Optional, Callable

logger = logging.getLogger(__name__)


class SequentialDirScanner:
    """顺序目录扫描器
    
    使用os.scandir顺序执行，优化速度：
    1. 去掉不必要的检测（如路径解析缓存、大型目录结构检测等）
    2. 保留排除项检查
    3. 简化错误处理
    4. 直接使用os.scandir，避免额外开销
    """
    
    def __init__(self, context_prefix: str = "[顺序扫描]"):
        """初始化顺序目录扫描器
        
        Args:
            context_prefix: 日志上下文前缀
        """
        self.context_prefix = context_prefix
        logger.debug(f"{self.context_prefix} 顺序目录扫描器已初始化")
    
    def scan_directory_tree(
        self,
        root_path: Path,
        exclude_check_func: Optional[Callable[[str], bool]] = None,
        file_callback: Optional[Callable[[Path], None]] = None
    ) -> int:
        """顺序扫描目录树
        
        Args:
            root_path: 根目录路径
            exclude_check_func: 排除检查函数，接受文件路径字符串，返回True表示排除
            file_callback: 文件回调函数，每发现一个文件时调用
            
        Returns:
            int: 扫描到的文件总数
        """
        if not root_path.exists():
            logger.warning(f"{self.context_prefix} 根目录不存在: {root_path}")
            return 0
        
        if not root_path.is_dir():
            logger.warning(f"{self.context_prefix} 根路径不是目录: {root_path}")
            return 0
        
        file_count = 0
        dirs_to_scan = deque([str(root_path.resolve())])
        scanned_dirs = set()
        
        try:
            while dirs_to_scan:
                current_dir_str = dirs_to_scan.popleft()
                
                # 检查是否已扫描（避免重复）
                if current_dir_str in scanned_dirs:
                    continue
                scanned_dirs.add(current_dir_str)
                
                # 检查目录是否被排除
                if exclude_check_func and exclude_check_func(current_dir_str):
                    continue
                
                try:
                    # 使用os.scandir扫描目录（性能最优）
                    with os.scandir(current_dir_str) as entries:
                        for entry in entries:
                            try:
                                entry_path = Path(entry.path)
                                entry_path_str = str(entry_path)
                                
                                if entry.is_dir(follow_symlinks=False):
                                    # 目录：添加到待扫描队列
                                    # 先检查是否被排除
                                    if not exclude_check_func or not exclude_check_func(entry_path_str):
                                        dirs_to_scan.append(entry_path_str)
                                
                                elif entry.is_file(follow_symlinks=False):
                                    # 文件：检查是否被排除
                                    if exclude_check_func and exclude_check_func(entry_path_str):
                                        continue
                                    
                                    # 调用文件回调
                                    if file_callback:
                                        file_callback(entry_path)
                                    
                                    file_count += 1
                                
                                # 忽略符号链接和其他类型
                                
                            except (OSError, PermissionError) as e:
                                # 单个条目错误，记录但继续
                                logger.debug(f"{self.context_prefix} 处理条目失败: {entry.path}, 错误: {str(e)}")
                                continue
                
                except (PermissionError, OSError, FileNotFoundError) as e:
                    # 目录无法打开（权限不足、不存在等）：记录并跳过
                    logger.debug(f"{self.context_prefix} 无法打开目录: {current_dir_str}, 错误: {str(e)}")
                    continue
                except Exception as e:
                    # 其他错误：记录并跳过
                    logger.warning(f"{self.context_prefix} 扫描目录时出错: {current_dir_str}, 错误: {str(e)}")
                    continue
        
        except Exception as e:
            logger.error(f"{self.context_prefix} 扫描过程异常: {str(e)}", exc_info=True)
            raise
        
        return file_count
    
    async def scan_directory_tree_async(
        self,
        root_path: Path,
        exclude_check_func: Optional[Callable[[str], bool]] = None,
        file_callback: Optional[Callable[[Path], None]] = None,
        batch_callback: Optional[Callable[[List[Path]], None]] = None,
        batch_size: int = 2000
    ) -> int:
        """异步顺序扫描目录树（支持批次回调）
        
        Args:
            root_path: 根目录路径
            exclude_check_func: 排除检查函数
            file_callback: 单个文件回调函数
            batch_callback: 批次回调函数，每收集到batch_size个文件时调用
            batch_size: 批次大小
            
        Returns:
            int: 扫描到的文件总数
        """
        import asyncio
        
        if not root_path.exists():
            logger.warning(f"{self.context_prefix} 根目录不存在: {root_path}")
            return 0
        
        if not root_path.is_dir():
            logger.warning(f"{self.context_prefix} 根路径不是目录: {root_path}")
            return 0
        
        file_count = 0
        dirs_to_scan = deque([str(root_path.resolve())])
        scanned_dirs = set()
        current_batch = []
        
        try:
            while dirs_to_scan:
                # 定期让出控制权，避免阻塞事件循环
                if file_count % 1000 == 0:
                    await asyncio.sleep(0)
                
                current_dir_str = dirs_to_scan.popleft()
                
                # 检查是否已扫描
                if current_dir_str in scanned_dirs:
                    continue
                scanned_dirs.add(current_dir_str)
                
                # 检查目录是否被排除
                if exclude_check_func and exclude_check_func(current_dir_str):
                    continue
                
                try:
                    # 使用os.scandir扫描目录
                    with os.scandir(current_dir_str) as entries:
                        for entry in entries:
                            try:
                                entry_path = Path(entry.path)
                                entry_path_str = str(entry_path)
                                
                                if entry.is_dir(follow_symlinks=False):
                                    # 目录：添加到待扫描队列
                                    if not exclude_check_func or not exclude_check_func(entry_path_str):
                                        dirs_to_scan.append(entry_path_str)
                                
                                elif entry.is_file(follow_symlinks=False):
                                    # 文件：检查是否被排除
                                    if exclude_check_func and exclude_check_func(entry_path_str):
                                        continue
                                    
                                    # 添加到批次
                                    current_batch.append(entry_path)
                                    
                                    # 调用单个文件回调
                                    if file_callback:
                                        file_callback(entry_path)
                                    
                                    file_count += 1
                                    
                                    # 批次达到阈值，调用批次回调
                                    if batch_callback and len(current_batch) >= batch_size:
                                        batch_callback(current_batch)
                                        current_batch = []
                                
                            except (OSError, PermissionError) as e:
                                logger.debug(f"{self.context_prefix} 处理条目失败: {entry.path}, 错误: {str(e)}")
                                continue
                
                except (PermissionError, OSError, FileNotFoundError) as e:
                    logger.debug(f"{self.context_prefix} 无法打开目录: {current_dir_str}, 错误: {str(e)}")
                    continue
                except Exception as e:
                    logger.warning(f"{self.context_prefix} 扫描目录时出错: {current_dir_str}, 错误: {str(e)}")
                    continue
            
            # 处理剩余的批次
            if batch_callback and current_batch:
                batch_callback(current_batch)
        
        except Exception as e:
            logger.error(f"{self.context_prefix} 扫描过程异常: {str(e)}", exc_info=True)
            raise
        
        return file_count

