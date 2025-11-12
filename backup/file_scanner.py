#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
文件扫描模块
File Scanner Module
"""

import logging
import fnmatch
from datetime import datetime
from pathlib import Path
from typing import List, Dict, Optional, AsyncGenerator, Callable, Awaitable

logger = logging.getLogger(__name__)


class FileScanner:
    """文件扫描器"""
    
    def __init__(self, settings=None, update_progress_callback: Optional[Callable] = None):
        """初始化文件扫描器
        
        Args:
            settings: 系统设置对象
            update_progress_callback: 更新进度回调函数 (backup_task, scanned_count, valid_count, operation_status)
        """
        self.settings = settings
        self.update_progress_callback = update_progress_callback
    
    async def get_file_info(self, file_path: Path) -> Optional[Dict]:
        """获取文件信息
        
        如果遇到权限错误、访问错误等，返回None，调用者应该跳过该文件。
        
        Args:
            file_path: 文件路径
            
        Returns:
            文件信息字典，如果无法访问则返回None
        """
        try:
            stat = file_path.stat()
            return {
                'path': str(file_path),
                'name': file_path.name,
                'size': stat.st_size,
                'modified_time': datetime.fromtimestamp(stat.st_mtime),
                'permissions': oct(stat.st_mode)[-3:],
                'is_file': file_path.is_file(),
                'is_dir': file_path.is_dir(),
                'is_symlink': file_path.is_symlink()
            }
        except (PermissionError, OSError, FileNotFoundError, IOError) as e:
            # 权限错误、访问错误等，返回None，让调用者跳过该文件
            logger.debug(f"无法获取文件信息（权限/访问错误）: {file_path} (错误: {str(e)})")
            return None
        except Exception as e:
            # 其他错误，也返回None
            logger.warning(f"获取文件信息失败 {file_path}: {str(e)}")
            return None
    
    def should_exclude_file(self, file_path: str, exclude_patterns: List[str]) -> bool:
        """检查文件或目录是否应该被排除
        
        排除规则匹配文件路径或其任何父目录路径时，文件/目录都会被排除。
        例如：如果排除规则匹配 "D:\temp"，则 "D:\temp\file.txt" 和 "D:\temp\subdir\file.txt" 都会被排除。
        
        Args:
            file_path: 文件或目录路径
            exclude_patterns: 排除模式列表（从计划任务 action_config 获取）
            
        Returns:
            bool: 如果文件/目录应该被排除返回 True
        """
        if not exclude_patterns:
            return False
        
        # 将路径标准化（统一使用正斜杠或反斜杠）
        normalized_path = file_path.replace('\\', '/')
        
        # 检查文件/目录路径本身是否匹配排除规则
        for pattern in exclude_patterns:
            normalized_pattern = pattern.replace('\\', '/')
            if fnmatch.fnmatch(normalized_path, normalized_pattern):
                return True
        
        # 检查文件/目录路径的父目录是否匹配排除规则
        # 例如：如果排除规则是 "D:/temp/*"，则 "D:/temp/subdir/file.txt" 应该被排除
        path_parts = normalized_path.split('/')
        for i in range(len(path_parts)):
            # 构建父目录路径（从根目录到当前层级）
            parent_path = '/'.join(path_parts[:i+1])
            if not parent_path:
                continue
            
            for pattern in exclude_patterns:
                normalized_pattern = pattern.replace('\\', '/')
                # 检查父目录路径是否匹配排除规则
                if fnmatch.fnmatch(parent_path, normalized_pattern):
                    return True
                # 检查父目录路径是否匹配通配符模式（如 "D:/temp/*"）
                if fnmatch.fnmatch(parent_path + '/*', normalized_pattern):
                    return True
        
        return False
    
    async def scan_source_files_streaming(
        self, 
        source_paths: List[str], 
        exclude_patterns: List[str], 
        backup_task: Optional[object] = None,
        batch_size: int = 100
    ) -> AsyncGenerator[List[Dict], None]:
        """流式扫描源文件（异步生成器，分批返回文件）
        
        支持网络路径（UNC路径）：
        - \\192.168.0.79\yz - 指定共享路径
        - 自动处理 UNC 路径的文件和目录扫描
        
        Args:
            source_paths: 源路径列表
            exclude_patterns: 排除模式列表
            backup_task: 备份任务对象（可选，用于进度更新）
            batch_size: 每批返回的文件数
            
        Yields:
            List[Dict]: 每批文件列表
        """
        if not source_paths:
            logger.warning("源路径列表为空")
            return
        
        # 估算总文件数（用于进度计算）
        estimated_total = 0
        if backup_task:
            try:
                for source_path_str in source_paths:
                    # 使用 WindowsPath 以确保 UNC 路径正确处理
                    from utils.network_path import is_unc_path, normalize_unc_path
                    if is_unc_path(source_path_str):
                        # UNC 路径需要使用 WindowsPath
                        source_path = Path(normalize_unc_path(source_path_str))
                    else:
                        source_path = Path(source_path_str)
                    
                    if source_path.is_dir():
                        try:
                            file_count = 0
                            for _ in source_path.rglob('*'):
                                if _.is_file():
                                    file_count += 1
                                    if file_count >= 1000:
                                        estimated_total += max(1000, file_count * 2)
                                        break
                            if file_count < 1000:
                                estimated_total += file_count
                        except Exception:
                            estimated_total += 5000
                    elif source_path.is_file():
                        estimated_total += 1
            except Exception:
                estimated_total = 5000
        
        if estimated_total < 100:
            estimated_total = 1000

        total_scanned = 0
        current_batch = []
        total_valid_files = 0  # 累计的有效文件总数
        
        for idx, source_path_str in enumerate(source_paths):
            logger.info(f"扫描源路径 {idx + 1}/{len(source_paths)}: {source_path_str}")
            
            # 处理 UNC 网络路径
            from utils.network_path import is_unc_path, normalize_unc_path
            if is_unc_path(source_path_str):
                # UNC 路径需要使用规范化后的路径
                normalized_path = normalize_unc_path(source_path_str)
                source_path = Path(normalized_path)
                logger.debug(f"检测到 UNC 路径，规范化后: {normalized_path}")
            else:
                source_path = Path(source_path_str)
            
            if not source_path.exists():
                logger.warning(f"源路径不存在，跳过: {source_path_str}")
                continue
            
            try:
                if source_path.is_file():
                    logger.debug(f"扫描文件: {source_path_str}")
                    try:
                        file_info = await self.get_file_info(source_path)
                        if file_info:
                            # 排除规则从计划任务获取（scheduled_task.action_config.exclude_patterns）
                            if not self.should_exclude_file(file_info['path'], exclude_patterns):
                                current_batch.append(file_info)
                                total_valid_files += 1  # 累计有效文件数
                        total_scanned += 1
                        
                        # 更新扫描进度
                        if backup_task and estimated_total > 0 and self.update_progress_callback:
                            scan_progress = min(10.0, (total_scanned / estimated_total) * 10.0)
                            backup_task.progress_percent = scan_progress
                            await self.update_progress_callback(backup_task, total_scanned, len(current_batch), "[扫描文件中...]")
                        
                        # 达到批次大小，yield当前批次
                        if len(current_batch) >= batch_size:
                            yield current_batch
                            current_batch = []
                    except (PermissionError, OSError, FileNotFoundError, IOError) as file_error:
                        # 文件访问错误，跳过该文件，继续扫描
                        logger.warning(f"⚠️ 跳过无法访问的文件: {source_path_str} (错误: {str(file_error)})")
                        continue
                    except Exception as file_error:
                        # 其他错误，也跳过该文件
                        logger.warning(f"⚠️ 跳过出错的文件: {source_path_str} (错误: {str(file_error)})")
                        continue
                        
                elif source_path.is_dir():
                    logger.info(f"扫描目录: {source_path_str}")
                    
                    # 检查目录本身是否匹配排除规则
                    if self.should_exclude_file(str(source_path), exclude_patterns):
                        logger.info(f"目录匹配排除规则，跳过整个目录: {source_path_str}")
                        continue
                    
                    scanned_count = 0
                    excluded_count = 0
                    skipped_dirs = 0
                    error_count = 0
                    error_paths = []  # 记录出错的路径
                    
                    try:
                        for file_path in source_path.rglob('*'):
                            try:
                                # 检查文件路径的父目录是否匹配排除规则
                                # 如果父目录匹配，跳过该文件
                                if self.should_exclude_file(str(file_path.parent), exclude_patterns):
                                    skipped_dirs += 1
                                    continue
                                
                                if file_path.is_file():
                                    scanned_count += 1
                                    total_scanned += 1
                                    
                                    # 每扫描100个文件输出一次进度
                                    if scanned_count % 100 == 0:
                                        logger.info(f"已扫描 {scanned_count} 个文件，找到 {total_valid_files} 个有效文件（当前批次: {len(current_batch)} 个）...")
                                    
                                    # 每扫描50个文件更新一次进度
                                    if total_scanned % 50 == 0 and backup_task and self.update_progress_callback:
                                        if total_scanned > estimated_total:
                                            estimated_total = total_scanned * 2
                                        
                                        if estimated_total > 0:
                                            scan_progress = min(10.0, (total_scanned / estimated_total) * 10.0)
                                            backup_task.progress_percent = scan_progress
                                            await self.update_progress_callback(backup_task, total_scanned, len(current_batch), "[扫描文件中...]")
                                    
                                    try:
                                        file_info = await self.get_file_info(file_path)
                                        if file_info:
                                            # 排除规则从计划任务获取（scheduled_task.action_config.exclude_patterns）
                                            if not self.should_exclude_file(file_info['path'], exclude_patterns):
                                                current_batch.append(file_info)
                                                total_valid_files += 1  # 累计有效文件数
                                                
                                                # 达到批次大小，yield当前批次
                                                if len(current_batch) >= batch_size:
                                                    yield current_batch
                                                    current_batch = []
                                            else:
                                                excluded_count += 1
                                    except (PermissionError, OSError, FileNotFoundError, IOError) as file_error:
                                        # 文件访问错误（权限、不存在等），跳过该文件，继续扫描
                                        error_count += 1
                                        error_paths.append(str(file_path))
                                        logger.warning(f"⚠️ 跳过无法访问的文件: {file_path} (错误: {str(file_error)})")
                                        continue
                                    except Exception as file_error:
                                        # 其他错误，也跳过该文件
                                        error_count += 1
                                        error_paths.append(str(file_path))
                                        logger.warning(f"⚠️ 跳过出错的文件: {file_path} (错误: {str(file_error)})")
                                        continue
                                elif file_path.is_dir():
                                    try:
                                        # 检查目录是否匹配排除规则
                                        if self.should_exclude_file(str(file_path), exclude_patterns):
                                            skipped_dirs += 1
                                            # 跳过该目录下的所有文件（rglob会继续，但我们在文件检查时会跳过）
                                            continue
                                    except (PermissionError, OSError, FileNotFoundError, IOError) as dir_error:
                                        # 目录访问错误，跳过该目录
                                        error_count += 1
                                        error_paths.append(str(file_path))
                                        logger.warning(f"⚠️ 跳过无法访问的目录: {file_path} (错误: {str(dir_error)})")
                                        continue
                            except (PermissionError, OSError, FileNotFoundError, IOError) as path_error:
                                # 路径访问错误，跳过该路径
                                error_count += 1
                                error_paths.append(str(file_path))
                                logger.warning(f"⚠️ 跳过无法访问的路径: {file_path} (错误: {str(path_error)})")
                                continue
                            except Exception as path_error:
                                # 其他错误，也跳过该路径
                                error_count += 1
                                error_paths.append(str(file_path))
                                logger.warning(f"⚠️ 跳过出错的路径: {file_path} (错误: {str(path_error)})")
                                continue
                    except (PermissionError, OSError, FileNotFoundError, IOError) as scan_error:
                        # 扫描目录时的访问错误，记录但继续扫描其他目录
                        error_count += 1
                        error_paths.append(str(source_path_str))
                        logger.error(f"⚠️ 扫描目录时发生访问错误 {source_path_str}: {str(scan_error)}，跳过该目录，继续扫描其他路径")
                        continue
                    except Exception as e:
                        # 其他扫描错误，记录但继续
                        error_count += 1
                        error_paths.append(str(source_path_str))
                        logger.error(f"⚠️ 扫描目录时发生错误 {source_path_str}: {str(e)}，跳过该目录，继续扫描其他路径")
                        continue
                    
                    logger.info(f"目录扫描完成: {source_path_str}, 扫描 {scanned_count} 个文件, 累计有效 {total_valid_files} 个（当前批次: {len(current_batch)} 个）, 排除 {excluded_count} 个文件, 跳过 {skipped_dirs} 个目录/文件, 错误 {error_count} 个")
                    if excluded_count > 0 or skipped_dirs > 0:
                        logger.warning(f"⚠️ 注意：已排除 {excluded_count} 个文件，跳过 {skipped_dirs} 个目录/文件（排除规则: {exclude_patterns if exclude_patterns else '无'}）")
                    if error_count > 0:
                        logger.warning(f"⚠️ 注意：遇到 {error_count} 个错误（权限、访问等），已跳过这些文件/目录")
                        if len(error_paths) <= 10:
                            logger.warning(f"⚠️ 错误路径示例: {', '.join(error_paths[:10])}")
                        else:
                            logger.warning(f"⚠️ 错误路径示例（前10个）: {', '.join(error_paths[:10])}... 共 {len(error_paths)} 个")
                else:
                    logger.warning(f"源路径既不是文件也不是目录: {source_path_str}")
            except Exception as e:
                logger.error(f"处理源路径失败 {source_path_str}: {str(e)}")
                continue
        
        # 返回最后一批文件
        if current_batch:
            yield current_batch
        
        # 扫描完成，更新进度
        if backup_task and self.update_progress_callback:
            backup_task.progress_percent = 10.0
            await self.update_progress_callback(backup_task, total_scanned, total_scanned, "[准备压缩...]")
        
        logger.info(f"========== 扫描完成 ==========")
        logger.info(f"共扫描 {total_scanned} 个文件，找到 {total_valid_files} 个有效文件")
        if exclude_patterns:
            logger.info(f"排除规则: {exclude_patterns}")
        logger.info(f"========== 扫描完成 ==========")
    
    async def scan_source_files(
        self, 
        source_paths: List[str], 
        exclude_patterns: List[str], 
        backup_task: Optional[object] = None
    ) -> List[Dict]:
        """扫描源文件（兼容旧接口，收集所有文件后返回）
        
        Args:
            source_paths: 源路径列表
            exclude_patterns: 排除模式列表
            backup_task: 备份任务对象（可选，用于进度更新）
            
        Returns:
            List[Dict]: 文件列表
        """
        file_list = []
        
        if not source_paths:
            logger.warning("源路径列表为空")
            return file_list

        # 估算总文件数（用于进度计算）
        estimated_total = 0
        if backup_task:
            # 尝试估算总文件数（更准确的估算）
            try:
                for source_path_str in source_paths:
                    source_path = Path(source_path_str)
                    if source_path.is_dir():
                        # 尝试实际统计目录中的文件数（递归）
                        try:
                            # 使用快速统计方法：统计前1000个文件，然后估算
                            file_count = 0
                            for _ in source_path.rglob('*'):
                                if _.is_file():
                                    file_count += 1
                                    if file_count >= 1000:
                                        # 如果文件数超过1000，假设还有更多，使用估算
                                        # 估算：假设目录结构类似，文件数可能更多
                                        estimated_total += max(1000, file_count * 2)
                                        break
                            if file_count < 1000:
                                # 如果文件数少于1000，使用实际统计
                                estimated_total += file_count
                        except Exception:
                            # 如果统计失败，使用保守估算
                            estimated_total += 5000  # 增加估算值
                    elif source_path.is_file():
                        estimated_total += 1
            except Exception:
                estimated_total = 5000  # 增加默认值
        
        # 如果估算值太小，使用更合理的默认值
        if estimated_total < 100:
            estimated_total = 1000  # 最小估算值

        total_scanned = 0
        
        for idx, source_path_str in enumerate(source_paths):
            logger.info(f"扫描源路径 {idx + 1}/{len(source_paths)}: {source_path_str}")
            source_path = Path(source_path_str)
            
            # 检查路径是否存在
            if not source_path.exists():
                logger.warning(f"源路径不存在，跳过: {source_path_str}")
                continue
            
            try:
                if source_path.is_file():
                    logger.debug(f"扫描文件: {source_path_str}")
                    try:
                        file_info = await self.get_file_info(source_path)
                        if file_info:
                            # 排除规则从计划任务获取（scheduled_task.action_config.exclude_patterns）
                            if not self.should_exclude_file(file_info['path'], exclude_patterns):
                                file_list.append(file_info)
                                logger.debug(f"已添加文件: {file_info['path']}")
                        
                        total_scanned += 1
                        # 更新扫描进度
                        if backup_task and estimated_total > 0 and self.update_progress_callback:
                            scan_progress = min(10.0, (total_scanned / estimated_total) * 10.0)  # 扫描占10%进度
                            backup_task.progress_percent = scan_progress
                            await self.update_progress_callback(backup_task, total_scanned, len(file_list))
                    except (PermissionError, OSError, FileNotFoundError, IOError) as file_error:
                        # 文件访问错误，跳过该文件，继续扫描
                        logger.warning(f"⚠️ 跳过无法访问的文件: {source_path_str} (错误: {str(file_error)})")
                        continue
                    except Exception as file_error:
                        # 其他错误，也跳过该文件
                        logger.warning(f"⚠️ 跳过出错的文件: {source_path_str} (错误: {str(file_error)})")
                        continue
                        
                elif source_path.is_dir():
                    logger.info(f"扫描目录: {source_path_str}")
                    
                    # 检查目录本身是否匹配排除规则
                    if self.should_exclude_file(str(source_path), exclude_patterns):
                        logger.info(f"目录匹配排除规则，跳过整个目录: {source_path_str}")
                        continue
                    
                    scanned_count = 0
                    excluded_count = 0
                    skipped_dirs = 0
                    error_count = 0
                    error_paths = []
                    
                    # 使用 rglob 递归扫描，但需要处理可能的异常
                    try:
                        for file_path in source_path.rglob('*'):
                            try:
                                # 检查文件路径的父目录是否匹配排除规则
                                # 如果父目录匹配，跳过该文件
                                if self.should_exclude_file(str(file_path.parent), exclude_patterns):
                                    skipped_dirs += 1
                                    continue
                                
                                if file_path.is_file():
                                    scanned_count += 1
                                    total_scanned += 1
                                    
                                    # 每扫描100个文件输出一次进度并更新数据库
                                    if scanned_count % 100 == 0:
                                        logger.info(f"已扫描 {scanned_count} 个文件，找到 {len(file_list)} 个有效文件...")
                                    
                                    # 每扫描50个文件更新一次进度（避免过于频繁）
                                    if total_scanned % 50 == 0 and backup_task and self.update_progress_callback:
                                        # 动态调整估算值：如果实际扫描的文件数超过估算值，更新估算值
                                        if total_scanned > estimated_total:
                                            estimated_total = total_scanned * 2  # 假设还有一半未扫描
                                        
                                        if estimated_total > 0:
                                            scan_progress = min(10.0, (total_scanned / estimated_total) * 10.0)  # 扫描占10%进度
                                            backup_task.progress_percent = scan_progress
                                            await self.update_progress_callback(backup_task, total_scanned, len(file_list))
                                    
                                    try:
                                        file_info = await self.get_file_info(file_path)
                                        if file_info:
                                            # 排除规则从计划任务获取（scheduled_task.action_config.exclude_patterns）
                                            if not self.should_exclude_file(file_info['path'], exclude_patterns):
                                                file_list.append(file_info)
                                            else:
                                                excluded_count += 1
                                    except (PermissionError, OSError, FileNotFoundError, IOError) as file_error:
                                        # 文件访问错误，跳过该文件，继续扫描
                                        error_count += 1
                                        error_paths.append(str(file_path))
                                        logger.warning(f"⚠️ 跳过无法访问的文件: {file_path} (错误: {str(file_error)})")
                                        continue
                                    except Exception as file_error:
                                        # 其他错误，也跳过该文件
                                        error_count += 1
                                        error_paths.append(str(file_path))
                                        logger.warning(f"⚠️ 跳过出错的文件: {file_path} (错误: {str(file_error)})")
                                        continue
                                elif file_path.is_dir():
                                    try:
                                        # 检查目录是否匹配排除规则
                                        if self.should_exclude_file(str(file_path), exclude_patterns):
                                            skipped_dirs += 1
                                            # 跳过该目录下的所有文件（rglob会继续，但我们在文件检查时会跳过）
                                            continue
                                    except (PermissionError, OSError, FileNotFoundError, IOError) as dir_error:
                                        # 目录访问错误，跳过该目录
                                        error_count += 1
                                        error_paths.append(str(file_path))
                                        logger.warning(f"⚠️ 跳过无法访问的目录: {file_path} (错误: {str(dir_error)})")
                                        continue
                            except (PermissionError, OSError, FileNotFoundError, IOError) as path_error:
                                # 路径访问错误，跳过该路径
                                error_count += 1
                                error_paths.append(str(file_path))
                                logger.warning(f"⚠️ 跳过无法访问的路径: {file_path} (错误: {str(path_error)})")
                                continue
                            except Exception as path_error:
                                # 其他错误，也跳过该路径
                                error_count += 1
                                error_paths.append(str(file_path))
                                logger.warning(f"⚠️ 跳过出错的路径: {file_path} (错误: {str(path_error)})")
                                continue
                    except (PermissionError, OSError, FileNotFoundError, IOError) as scan_error:
                        # 扫描目录时的访问错误，记录但继续扫描其他目录
                        error_count += 1
                        error_paths.append(str(source_path_str))
                        logger.error(f"⚠️ 扫描目录时发生访问错误 {source_path_str}: {str(scan_error)}，跳过该目录，继续扫描其他路径")
                        # 继续扫描其他路径
                        continue
                    except Exception as e:
                        # 其他扫描错误，记录但继续
                        error_count += 1
                        error_paths.append(str(source_path_str))
                        logger.error(f"⚠️ 扫描目录时发生错误 {source_path_str}: {str(e)}，跳过该目录，继续扫描其他路径")
                        # 继续扫描其他路径
                        continue
                    
                    logger.info(f"目录扫描完成: {source_path_str}, 扫描 {scanned_count} 个文件, 有效 {len(file_list)} 个, 排除 {excluded_count} 个文件, 跳过 {skipped_dirs} 个目录/文件, 错误 {error_count} 个")
                    if excluded_count > 0 or skipped_dirs > 0:
                        logger.warning(f"⚠️ 注意：已排除 {excluded_count} 个文件，跳过 {skipped_dirs} 个目录/文件（排除规则: {exclude_patterns if exclude_patterns else '无'}）")
                    if error_count > 0:
                        logger.warning(f"⚠️ 注意：遇到 {error_count} 个错误（权限、访问等），已跳过这些文件/目录")
                        if len(error_paths) <= 10:
                            logger.warning(f"⚠️ 错误路径示例: {', '.join(error_paths[:10])}")
                        else:
                            logger.warning(f"⚠️ 错误路径示例（前10个）: {', '.join(error_paths[:10])}... 共 {len(error_paths)} 个")
                else:
                    logger.warning(f"源路径既不是文件也不是目录: {source_path_str}")
            except Exception as e:
                logger.error(f"处理源路径失败 {source_path_str}: {str(e)}")
                continue

        return file_list

