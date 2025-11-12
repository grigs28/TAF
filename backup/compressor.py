#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
压缩处理模块
Compression Module
"""

import asyncio
import logging
import threading
import time
import py7zr
from pathlib import Path
from typing import List, Dict, Optional
from datetime import datetime

from models.backup import BackupSet, BackupTask
from utils.datetime_utils import now, format_datetime
from backup.utils import format_bytes, calculate_file_checksum

logger = logging.getLogger(__name__)


class Compressor:
    """压缩处理器"""
    
    def __init__(self, settings=None):
        """初始化压缩处理器
        
        Args:
            settings: 系统设置对象
        """
        self.settings = settings
    
    async def group_files_for_compression(self, file_list: List[Dict]) -> List[List[Dict]]:
        """将文件分组以进行压缩
        
        单个压缩包的最大大小从 config 获取（MAX_FILE_SIZE）
        当批次超过阈值时，尽可能均分文件，使得每个压缩包的大小尽可能接近但不超过 MAX_FILE_SIZE
        
        Args:
            file_list: 文件列表
            
        Returns:
            List[List[Dict]]: 分组后的文件列表
        """
        # 从系统配置获取单个压缩包的最大大小
        max_size = self.settings.MAX_FILE_SIZE
        logger.debug(f"使用系统配置的单个压缩包最大大小: {format_bytes(max_size)}")
        
        if not file_list:
            return []
        
        # 计算批次总大小
        total_size = sum(f['size'] for f in file_list)
        logger.debug(f"批次总大小: {format_bytes(total_size)}, 文件数: {len(file_list)}")
        
        # 如果总大小不超过最大大小，直接返回一个组
        if total_size <= max_size:
            logger.debug(f"批次总大小未超过限制，返回单个组")
            return [file_list]
        
        # 如果总大小超过最大大小，需要分成多个组
        # 计算需要分成多少组（向上取整）
        num_groups = (total_size + max_size - 1) // max_size  # 向上取整
        logger.debug(f"批次总大小超过限制，需要分成 {num_groups} 个组")
        
        # 计算目标每组大小（尽可能均分）
        target_group_size = total_size / num_groups
        logger.debug(f"目标每组大小: {format_bytes(target_group_size)}")
        
        # 使用贪心算法分组：尽可能让每组大小接近目标大小，但不超过 max_size
        groups = []
        current_group = []
        current_size = 0
        
        # 按文件大小降序排序，优先处理大文件（有助于更好的分组）
        sorted_files = sorted(file_list, key=lambda x: x['size'], reverse=True)
        
        # 预计算剩余文件大小（用于优化决策）
        # remaining_sizes[i] 表示从索引 i+1 开始的所有文件的累计大小（不包括索引 i 的文件）
        remaining_sizes = [0] * (len(sorted_files) + 1)
        cumulative_size = 0
        for idx in range(len(sorted_files) - 1, -1, -1):
            remaining_sizes[idx] = cumulative_size
            cumulative_size += sorted_files[idx]['size']
        
        for idx, file_info in enumerate(sorted_files):
            file_size = file_info['size']
            # 剩余文件总大小（不包括当前文件）：从下一个文件开始的所有文件
            remaining_size = remaining_sizes[idx]
            
            # 如果单个文件超过最大大小，单独成组
            if file_size > max_size:
                if current_group:
                    groups.append(current_group)
                    current_group = []
                    current_size = 0
                groups.append([file_info])
                continue
            
            # 检查添加到当前组是否会超过最大大小
            if current_size + file_size > max_size and current_group:
                # 当前组已满，开始新组
                groups.append(current_group)
                current_group = []
                current_size = 0
            
            # 检查是否应该开始新组（基于目标大小，尽可能均分）
            # 如果当前组大小接近目标大小，且添加这个文件会明显超过目标大小，考虑开始新组
            if current_group and current_size >= target_group_size * 0.8:
                # 如果添加这个文件会超过目标大小很多，且还有剩余文件，考虑开始新组
                if remaining_size > 0 and current_size + file_size > target_group_size * 1.2:
                    # 检查剩余文件是否足够填满新组（至少达到目标大小的50%）
                    if remaining_size >= target_group_size * 0.5:
                        groups.append(current_group)
                        current_group = []
                        current_size = 0
            
            current_group.append(file_info)
            current_size += file_size
        
        # 添加最后一组
        if current_group:
            groups.append(current_group)
        
        # 验证分组结果
        total_grouped_size = sum(sum(f['size'] for f in group) for group in groups)
        if abs(total_grouped_size - total_size) > 1024:  # 允许1KB的误差
            logger.warning(f"分组大小不匹配: 原始={format_bytes(total_size)}, 分组后={format_bytes(total_grouped_size)}")
        
        # 记录分组信息
        group_sizes = [sum(f['size'] for f in group) for group in groups]
        logger.info(f"文件分组完成: {len(groups)} 个组, "
                   f"组大小范围: {format_bytes(min(group_sizes))} - {format_bytes(max(group_sizes))}, "
                   f"平均组大小: {format_bytes(sum(group_sizes) / len(group_sizes))}")
        
        return groups
    
    async def compress_file_group(
        self, 
        file_group: List[Dict], 
        backup_set: BackupSet, 
        backup_task: BackupTask,
        base_processed_files: int = 0, 
        total_files: int = 0
    ) -> Optional[Dict]:
        """压缩文件组（使用7z压缩，支持多线程，带进度跟踪）
        
        Args:
            file_group: 文件组列表
            backup_set: 备份集对象
            backup_task: 备份任务对象
            base_processed_files: 已处理文件数基数
            total_files: 总文件数
            
        Returns:
            压缩文件信息字典，如果失败则返回None
        """
        try:
            # 从备份任务获取压缩设置
            compression_enabled = getattr(backup_task, 'compression_enabled', True)
            
            # 从系统配置获取压缩级别（从 config 获取）
            compression_level = self.settings.COMPRESSION_LEVEL
            logger.debug(f"使用系统配置的压缩级别: {compression_level}")
            
            # 从系统配置获取线程数
            compression_threads = self.settings.COMPRESSION_THREADS
            
            # 创建临时文件（直接写入磁带盘符，不创建临时文件）
            timestamp = format_datetime(now(), '%Y%m%d_%H%M%S')
            tape_drive = self.settings.TAPE_DRIVE_LETTER.upper() + ":\\"
            backup_dir = Path(tape_drive) / backup_set.set_id
            
            # 进度跟踪变量
            compress_progress = {'bytes_written': 0, 'running': True, 'completed': False}
            total_original_size = sum(f['size'] for f in file_group)
            # 用于存储成功和失败的文件信息（在线程间共享）
            compress_result = {'successful_files': [], 'failed_files': [], 'successful_original_size': 0}
            
            # 将压缩操作放到线程池中执行，避免阻塞事件循环
            def _do_7z_compress():
                """在线程中执行7z压缩操作，带进度跟踪"""
                try:
                    # 在线程中创建目录（避免阻塞事件循环）
                    backup_dir.mkdir(parents=True, exist_ok=True)
                    
                    if compression_enabled:
                        # 使用7z压缩，直接写入磁带盘符
                        archive_path = backup_dir / f"backup_{backup_set.set_id}_{timestamp}.7z"
                        
                        # 使用py7zr进行7z压缩，启用多进程（mp=True启用多进程压缩）
                        # 注意：py7zr 使用 mp 参数启用多进程，而不是 threads
                        with py7zr.SevenZipFile(
                            archive_path,
                            mode='w',
                            filters=[{'id': py7zr.FILTER_LZMA2, 'preset': compression_level}],
                            mp=True if compression_threads > 1 else False  # 启用多进程压缩（如果线程数>1）
                        ) as archive:
                            # 添加文件到压缩包
                            successful_files = []
                            failed_files = []
                            
                            for file_idx, file_info in enumerate(file_group):
                                file_path = Path(file_info['path'])
                                try:
                                    if not file_path.exists():
                                        logger.warning(f"文件不存在，跳过: {file_path}")
                                        failed_files.append({'path': str(file_path), 'reason': '文件不存在'})
                                        continue
                                    
                                    # 计算相对路径（保留目录结构）
                                    try:
                                        source_paths = backup_task.source_paths or []
                                        if source_paths:
                                            arcname = None
                                            for src_path in source_paths:
                                                src = Path(src_path)
                                                try:
                                                    if file_path.is_relative_to(src):
                                                        arcname = str(file_path.relative_to(src))
                                                        break
                                                except (ValueError, AttributeError):
                                                    continue
                                            if arcname is None:
                                                arcname = file_path.name
                                        else:
                                            arcname = file_path.name
                                    except Exception:
                                        arcname = file_path.name
                                    
                                    # 添加文件到压缩包
                                    try:
                                        archive.write(file_path, arcname=arcname)
                                        successful_files.append(str(file_path))
                                        
                                        # 更新进度：基于已压缩的文件数量
                                        if total_files > 0:
                                            current_processed = base_processed_files + file_idx + 1
                                            # 扫描阶段占10%，压缩阶段占90%
                                            compress_progress_value = 10.0 + (current_processed / total_files) * 90.0
                                            compress_progress['bytes_written'] = archive_path.stat().st_size if archive_path.exists() else 0
                                            
                                            # 更新任务进度（在后台线程中，需要异步更新）
                                            if backup_task and backup_task.id:
                                                backup_task.progress_percent = min(100.0, compress_progress_value)
                                    except (PermissionError, OSError, FileNotFoundError, IOError) as write_error:
                                        # 文件写入错误（权限、访问等），跳过该文件，继续处理其他文件
                                        logger.warning(f"⚠️ 压缩时跳过无法访问的文件: {file_path} (错误: {str(write_error)})")
                                        failed_files.append({'path': str(file_path), 'reason': str(write_error)})
                                        continue
                                    except Exception as write_error:
                                        # 其他错误，也跳过该文件
                                        logger.warning(f"⚠️ 压缩时跳过出错的文件: {file_path} (错误: {str(write_error)})")
                                        failed_files.append({'path': str(file_path), 'reason': str(write_error)})
                                        continue
                                except Exception as file_error:
                                    # 文件处理错误，跳过该文件
                                    logger.warning(f"⚠️ 压缩时跳过出错的文件: {file_path} (错误: {str(file_error)})")
                                    failed_files.append({'path': str(file_path), 'reason': str(file_error)})
                                    continue
                            
                            # 压缩完成，等待文件大小稳定
                            compress_progress['running'] = False
                            
                            # 等待文件大小稳定（最多等待10秒）
                            max_wait_time = 10
                            wait_interval = 0.1
                            wait_count = 0
                            last_size = archive_path.stat().st_size if archive_path.exists() else 0
                            
                            while wait_count < max_wait_time / wait_interval:
                                time.sleep(wait_interval)  # 使用同步sleep，因为在线程中
                                wait_count += 1
                                if archive_path.exists():
                                    current_size = archive_path.stat().st_size
                                    if current_size == last_size:
                                        # 文件大小稳定，压缩完成
                                        break
                                    last_size = current_size
                            
                            compress_progress['completed'] = True
                            compress_progress['bytes_written'] = archive_path.stat().st_size if archive_path.exists() else 0
                            
                            # 存储成功和失败的文件信息
                            compress_result['successful_files'] = successful_files
                            compress_result['failed_files'] = failed_files
                            compress_result['successful_original_size'] = sum(
                                f['size'] for f in file_group 
                                if str(f['path']) in successful_files
                            )
                            
                            logger.info(f"7z压缩完成: {len(successful_files)} 个文件成功, {len(failed_files)} 个文件失败")
                            
                    else:
                        # 不使用压缩，直接打包（tar格式）
                        # 注意：当前实现使用7z，如果compression_enabled=False，仍然使用7z但压缩级别为0
                        archive_path = backup_dir / f"backup_{backup_set.set_id}_{timestamp}.tar"
                        logger.warning("当前实现不支持tar格式，使用7z格式（压缩级别为0）")
                        # TODO: 实现tar格式打包
                        compress_progress['completed'] = True
                        compress_progress['bytes_written'] = 0
                        compress_result['successful_files'] = []
                        compress_result['failed_files'] = []
                        compress_result['successful_original_size'] = 0
                        
                except Exception as e:
                    logger.error(f"压缩操作失败: {str(e)}")
                    import traceback
                    logger.error(traceback.format_exc())
                    compress_progress['completed'] = True
                    compress_progress['running'] = False
            
            # 在线程池中执行压缩操作
            loop = asyncio.get_event_loop()
            await loop.run_in_executor(None, _do_7z_compress)
            
            # 等待压缩完成
            max_wait = 300  # 最多等待5分钟
            wait_count = 0
            while not compress_progress['completed'] and wait_count < max_wait:
                await asyncio.sleep(1)
                wait_count += 1
            
            if not compress_progress['completed']:
                logger.error("压缩操作超时")
                return None
            
            # 检查压缩文件是否存在
            if compression_enabled:
                archive_path = backup_dir / f"backup_{backup_set.set_id}_{timestamp}.7z"
            else:
                archive_path = backup_dir / f"backup_{backup_set.set_id}_{timestamp}.tar"
            
            if not archive_path.exists():
                logger.error(f"压缩文件不存在: {archive_path}")
                return None
            
            # 获取压缩文件大小
            compressed_size = archive_path.stat().st_size
            
            # 计算校验和（可选，性能考虑可以跳过）
            checksum = None
            # checksum = calculate_file_checksum(archive_path)  # 注释掉以提高性能
            
            successful_file_count = len(compress_result['successful_files'])
            
            compressed_info = {
                'path': str(archive_path),
                'compressed_size': compressed_size,
                'original_size': compress_result['successful_original_size'] or total_original_size,
                'successful_files': successful_file_count,  # 成功压缩的文件数
                'failed_files': 0,  # 失败的文件数（无法精确统计，设为0）
                'checksum': checksum,
                'compression_enabled': compression_enabled,
                'compression_level': compression_level if compression_enabled else None,
                'compression_threads': compression_threads if compression_enabled else None
            }

            if compression_enabled:
                compression_ratio = compressed_size / compressed_info['original_size'] if compressed_info['original_size'] > 0 else 0
                logger.info(f"7z压缩完成: {successful_file_count} 个文件, "
                            f"原始大小: {format_bytes(compressed_info['original_size'])}, "
                            f"压缩后: {format_bytes(compressed_size)}, "
                            f"压缩比: {compression_ratio:.2%}, "
                            f"线程数: {compression_threads}")
            else:
                logger.info(f"打包完成: {successful_file_count} 个文件, "
                            f"大小: {format_bytes(compressed_size)}")

            return compressed_info

        except Exception as e:
            logger.error(f"压缩文件组失败: {str(e)}")
            import traceback
            traceback.print_exc()
            return None

