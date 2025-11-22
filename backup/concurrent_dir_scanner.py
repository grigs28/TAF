#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
并发目录扫描模块
Concurrent Directory Scanner Module

专用于openGauss模式下的高性能并发目录扫描
使用ThreadPoolExecutor实现多线程并发扫描，提升扫描速度
"""

import os
import logging
import threading
import time
import queue
import asyncio
import inspect
from collections import deque
from concurrent.futures import ThreadPoolExecutor, as_completed
from pathlib import Path
from typing import List, Dict, Optional, Tuple, Callable

from config.settings import get_settings

logger = logging.getLogger(__name__)


def scan_single_directory(dir_path: str, dir_path_cache: Dict) -> Tuple[List[str], List[Path]]:
    """扫描单个目录，返回子目录列表和文件列表
    
    Args:
        dir_path: 目录路径（字符串）
        dir_path_cache: 路径缓存字典（用于优化路径解析）
    
    Returns:
        (subdirs, files): 子目录列表和文件列表
    """
    subdirs = []
    files = []
    
    try:
        entries = os.scandir(dir_path)
        with entries:
            for entry in entries:
                try:
                    entry_path = Path(entry.path)
                    
                    if entry.is_dir(follow_symlinks=False):
                        # 目录：添加到子目录列表
                        subdirs.append(entry_path)
                    elif entry.is_file(follow_symlinks=False):
                        # 文件：添加到文件列表
                        files.append(entry_path)
                except (OSError, PermissionError) as entry_err:
                    # 无法判断类型，尝试作为文件处理
                    try:
                        if entry_path.is_file():
                            files.append(entry_path)
                        elif entry_path.is_dir():
                            subdirs.append(entry_path)
                    except Exception:
                        # 无法访问，跳过
                        continue
                except Exception:
                    # 其他错误，跳过
                    continue
    except (PermissionError, OSError, FileNotFoundError, IOError) as scandir_err:
        # 目录无法打开（权限不足、不存在等）
        raise scandir_err
    except Exception as scandir_err:
        # 其他错误
        raise scandir_err
    
    return subdirs, files


class ConcurrentDirScanner:
    """并发目录扫描器
    
    使用ThreadPoolExecutor实现多线程并发扫描目录，提升扫描速度
    专用于openGauss模式下的高性能目录扫描
    """
    
    def __init__(self, max_workers: Optional[int] = None, context_prefix: str = "[并发扫描]"):
        """初始化并发目录扫描器
        
        Args:
            max_workers: 最大工作线程数，如果为None则从配置读取SCAN_THREADS
            context_prefix: 日志上下文前缀
        """
        settings = get_settings()
        self.max_workers = max_workers if max_workers is not None else getattr(settings, 'SCAN_THREADS', 4)
        self.context_prefix = context_prefix
        
        # 线程安全的数据结构
        self.dirs_to_scan = queue.Queue()  # 待扫描目录队列（线程安全）
        self.scanned_dirs_lock = threading.Lock()  # 已扫描目录集合的锁
        self.scanned_dirs = set()  # 已扫描的目录集合（避免重复扫描）
        self.files_batch_lock = threading.Lock()  # 文件批次的锁
        self.files_batch = []  # 当前文件批次
        
        # 统计信息（线程安全）
        self.stats_lock = threading.Lock()
        self.total_dirs_scanned = 0  # 已扫描目录数
        self.total_files_found = 0  # 已发现文件数
        self.permission_error_count = 0  # 权限错误计数
        
        # 路径缓存（线程安全）
        self.dir_path_cache_lock = threading.Lock()
        self.dir_path_cache = {}  # {Path对象: 字符串路径} 缓存
        
        # 控制标志
        self.scan_cancelled = False
        self.scan_done = False
        
        logger.info(f"{self.context_prefix} 并发目录扫描器已初始化 (线程数: {self.max_workers})")
    
    def _resolve_path(self, path: Path) -> str:
        """解析路径并缓存（线程安全）
        
        Args:
            path: 路径对象
        
        Returns:
            字符串路径
        """
        with self.dir_path_cache_lock:
            if path in self.dir_path_cache:
                return self.dir_path_cache[path]
            
            # 解析路径
            try:
                if isinstance(path, str):
                    path_str = path
                else:
                    path_str = str(path.resolve())
                
                # 缓存结果
                if not isinstance(path, str):
                    self.dir_path_cache[path] = path_str
                
                return path_str
            except Exception:
                # 解析失败，使用字符串表示
                path_str = str(path)
                if not isinstance(path, str):
                    self.dir_path_cache[path] = path_str
                return path_str
    
    def _is_dir_scanned(self, dir_path_str: str) -> bool:
        """检查目录是否已扫描（线程安全）
        
        Args:
            dir_path_str: 目录路径字符串
        
        Returns:
            是否已扫描
        """
        with self.scanned_dirs_lock:
            return dir_path_str in self.scanned_dirs
    
    def _mark_dir_scanned(self, dir_path_str: str):
        """标记目录已扫描（线程安全）
        
        Args:
            dir_path_str: 目录路径字符串
        """
        with self.scanned_dirs_lock:
            self.scanned_dirs.add(dir_path_str)
    
    def _put_to_queue(self, path_queue, batch: List, main_loop=None):
        """将批次放入队列（自动检测队列类型）
        
        Args:
            path_queue: 队列（asyncio.Queue 或 queue.Queue）
            batch: 要放入的批次数据
            main_loop: 主事件循环（用于 asyncio.Queue）
        """
        if main_loop:
            try:
                # 检查是否为 asyncio.Queue（put 是协程函数）
                if hasattr(path_queue, 'put'):
                    put_method = getattr(path_queue, 'put', None)
                    if put_method and inspect.iscoroutinefunction(put_method):
                        # asyncio.Queue：使用 run_coroutine_threadsafe
                        future = asyncio.run_coroutine_threadsafe(
                            path_queue.put(batch),
                            main_loop
                        )
                        future.result(timeout=10.0)
                        return
            except Exception as e:
                # 异步提交失败，尝试同步方式
                logger.debug(f"{self.context_prefix} 异步提交失败，尝试同步: {str(e)}")
        
        # queue.Queue 或其他类型的队列：直接 put
        try:
            path_queue.put(batch)
        except Exception as e:
            logger.warning(f"{self.context_prefix} 同步提交失败: {str(e)}")
            raise
    
    def _add_files_to_batch(self, files: List[Path], batch_threshold: int, 
                            path_queue, main_loop, batch_force_interval: float):
        """添加文件到批次（线程安全）
        
        Args:
            files: 文件列表
            batch_threshold: 批次阈值
            path_queue: 路径队列（asyncio.Queue 或 queue.Queue）
            main_loop: 主事件循环
            batch_force_interval: 强制提交批次的时间间隔
        """
        with self.files_batch_lock:
            self.files_batch.extend(files)
            
            # 更新统计信息
            with self.stats_lock:
                self.total_files_found += len(files)
            
            # 检查是否需要提交批次
            current_time = time.time()
            batch_size = len(self.files_batch)
            
            # 如果批次达到阈值，提交批次
            if batch_size >= batch_threshold:
                try:
                    batch_to_submit = self.files_batch.copy()
                    self.files_batch.clear()
                    
                    # 提交到队列（自动检测队列类型）
                    self._put_to_queue(path_queue, batch_to_submit, main_loop)
                    
                    logger.debug(f"{self.context_prefix} 已提交批次: {batch_size} 个文件")
                except Exception as e:
                    logger.warning(f"{self.context_prefix} 提交批次失败: {str(e)}")
    
    def _scan_directory_worker(self, dir_path: Path, batch_threshold: int,
                               path_queue, main_loop, batch_force_interval: float) -> Optional[Tuple[List[Path], int]]:
        """扫描单个目录的工作函数（在线程池中执行）
        
        Args:
            dir_path: 目录路径
            batch_threshold: 批次阈值
            path_queue: 路径队列
            main_loop: 主事件循环
            batch_force_interval: 强制提交批次的时间间隔
        
        Returns:
            (subdirs, file_count): 子目录列表和文件数，如果扫描失败返回None
        """
        if self.scan_cancelled:
            return None
        
        # 解析路径
        try:
            dir_path_str = self._resolve_path(dir_path)
        except Exception:
            return None
        
        # 检查是否已扫描
        if self._is_dir_scanned(dir_path_str):
            return None
        
        # 标记已扫描
        self._mark_dir_scanned(dir_path_str)
        
        # 更新统计
        with self.stats_lock:
            self.total_dirs_scanned += 1
            dir_count = self.total_dirs_scanned
        
        # 扫描目录
        try:
            subdirs, files = scan_single_directory(dir_path_str, self.dir_path_cache)
            
            # 添加文件到批次
            if files:
                self._add_files_to_batch(files, batch_threshold, path_queue, main_loop, batch_force_interval)
            
            return (subdirs, len(files))
            
        except (PermissionError, OSError, FileNotFoundError, IOError) as scandir_err:
            # 权限错误
            with self.stats_lock:
                self.permission_error_count += 1
                error_count = self.permission_error_count
            
            if error_count <= 20:
                logger.warning(f"{self.context_prefix} 无法打开目录: {dir_path_str[:200]}, 错误: {str(scandir_err)}")
            
            return None
        except Exception as scandir_err:
            # 其他错误
            with self.stats_lock:
                self.permission_error_count += 1
                error_count = self.permission_error_count
            
            if error_count <= 20:
                logger.warning(f"{self.context_prefix} 扫描目录时出错: {dir_path_str[:200]}, 错误: {str(scandir_err)}")
            
            return None
    
    def scan_directory_tree(self, root_path: Path, path_queue, 
                           main_loop, batch_threshold: int = 1000,
                           batch_force_interval: float = 1200.0,
                           log_interval: float = 60.0) -> int:
        """并发扫描目录树
        
        Args:
            root_path: 根目录路径
            path_queue: 路径队列（asyncio.Queue 或 queue.Queue，用于提交文件批次）
            main_loop: 主事件循环（用于异步操作，可为None）
            batch_threshold: 批次阈值（文件数）
            batch_force_interval: 强制提交批次的时间间隔（秒）
            log_interval: 日志输出间隔（秒）
        
        Returns:
            总路径数
        """
        # 添加根目录到队列
        self.dirs_to_scan.put(root_path)
        
        start_time = time.time()
        last_log_time = start_time
        last_batch_submit_time = start_time
        
        # 使用ThreadPoolExecutor并发扫描
        with ThreadPoolExecutor(max_workers=self.max_workers) as executor:
            futures = {}
            
            try:
                while not self.scan_done:
                    # 检查是否取消
                    if self.scan_cancelled:
                        break
                    
                    # 提交新的扫描任务（最多同时运行max_workers个任务）
                    while len(futures) < self.max_workers:
                        try:
                            # 从队列获取待扫描目录（非阻塞）
                            dir_path = self.dirs_to_scan.get_nowait()
                            
                            # 提交扫描任务
                            future = executor.submit(
                                self._scan_directory_worker,
                                dir_path,
                                batch_threshold,
                                path_queue,
                                main_loop,
                                batch_force_interval
                            )
                            futures[future] = dir_path
                            
                        except queue.Empty:
                            # 队列为空，退出内层循环
                            break
                    
                    # 检查已完成的任务
                    if not futures:
                        # 没有正在运行的任务，检查是否还有待扫描的目录
                        try:
                            dir_path = self.dirs_to_scan.get(timeout=0.1)
                            # 有新目录，继续循环
                            continue
                        except queue.Empty:
                            # 没有新目录，扫描完成
                            break
                    
                    # 等待至少一个任务完成
                    if futures:
                        try:
                            # 使用 as_completed 等待任务完成（超时1秒）
                            done_futures = as_completed(futures.keys(), timeout=1.0)
                            future = next(done_futures, None)
                            
                            if future and future in futures:
                                # 处理已完成的任务
                                dir_path = futures.pop(future, None)
                                
                                try:
                                    result = future.result(timeout=0.1)
                                    
                                    if result:
                                        subdirs, file_count = result
                                        
                                        # 添加子目录到待扫描队列
                                        for subdir in subdirs:
                                            # 解析路径并检查是否已扫描
                                            try:
                                                subdir_str = self._resolve_path(subdir)
                                                if not self._is_dir_scanned(subdir_str):
                                                    self.dirs_to_scan.put(subdir)
                                            except Exception:
                                                continue
                                
                                except Exception as e:
                                    logger.debug(f"{self.context_prefix} 扫描任务异常: {str(e)}")
                        except StopIteration:
                            # 没有任务完成，继续循环
                            pass
                        except Exception:
                            # as_completed 超时或其他异常，继续循环
                            pass
                    
                    # 检查是否需要输出日志
                    current_time = time.time()
                    if current_time - last_log_time >= log_interval:
                        with self.stats_lock:
                            dirs_scanned = self.total_dirs_scanned
                            files_found = self.total_files_found
                            errors = self.permission_error_count
                        
                        elapsed = current_time - start_time
                        logger.info(
                            f"{self.context_prefix} 进度: 已扫描 {dirs_scanned} 个目录, "
                            f"发现 {files_found} 个文件, 错误 {errors} 个, "
                            f"待扫描队列: {self.dirs_to_scan.qsize()} 个目录, "
                            f"耗时: {elapsed:.1f} 秒"
                        )
                        last_log_time = current_time
                    
                    # 检查是否需要强制提交批次
                    if current_time - last_batch_submit_time >= batch_force_interval:
                        with self.files_batch_lock:
                            if self.files_batch:
                                batch_to_submit = self.files_batch.copy()
                                self.files_batch.clear()
                                
                                try:
                                    # 提交到队列（自动检测队列类型）
                                    self._put_to_queue(path_queue, batch_to_submit, main_loop)
                                    logger.debug(f"{self.context_prefix} 强制提交批次: {len(batch_to_submit)} 个文件")
                                except Exception as e:
                                    logger.warning(f"{self.context_prefix} 强制提交批次失败: {str(e)}")
                                
                                last_batch_submit_time = current_time
                
                # 等待所有任务完成
                if futures:
                    for future in as_completed(futures.keys()):
                        try:
                            future.result(timeout=1.0)
                        except Exception:
                            pass
                
            except KeyboardInterrupt:
                logger.warning(f"{self.context_prefix} 收到中断信号，停止扫描")
                self.scan_cancelled = True
                # 取消所有未完成的任务
                for future in futures:
                    future.cancel()
        
        # 提交剩余的文件批次
        with self.files_batch_lock:
            if self.files_batch:
                try:
                    batch_to_submit = self.files_batch.copy()
                    self.files_batch.clear()
                    
                    # 提交到队列（自动检测队列类型）
                    self._put_to_queue(path_queue, batch_to_submit, main_loop)
                except Exception as e:
                    logger.warning(f"{self.context_prefix} 提交剩余批次失败: {str(e)}")
        
        # 返回统计信息
        with self.stats_lock:
            total_files = self.total_files_found
        
        return total_files

