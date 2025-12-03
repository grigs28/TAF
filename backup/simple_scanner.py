#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
简洁扫描模块 - 完全按照 memory_db_writer.py 的方法扫描和写数据库
Simple Scanner Module - Scan and write to database using the same method as memory_db_writer.py

功能：
1. 扫描目录（使用 os.scandir + FileScanner）
2. 直接批量写入 openGauss 数据库（使用 memory_db_writer 的方法）
3. 数据库表名通过内存获取（backup_task.backup_files_table）
4. 只完成扫描和写数据库任务，独立模块
"""

import asyncio
import logging
import os
import time
from pathlib import Path
from typing import List, Dict, Optional, Any
from datetime import datetime, timezone

from models.backup import BackupTask, BackupSet
from backup.utils import format_bytes
from backup.file_scanner import FileScanner
from utils.scheduler.db_utils import get_opengauss_connection, is_opengauss
from config.settings import get_settings

logger = logging.getLogger(__name__)


class SimpleScanner:
    """简洁扫描器 - 完全按照 memory_db_writer.py 的方法"""
    
    def __init__(self, backup_db):
        """初始化简洁扫描器
        
        Args:
            backup_db: 数据库操作对象
        """
        self.backup_db = backup_db
        self.settings = get_settings()
        self.file_scanner = FileScanner()
    
    async def scan_and_write(
        self,
        backup_task: BackupTask,
        source_paths: List[str],
        exclude_patterns: List[str],
        backup_set: BackupSet,
        restart: bool = False,
    ):
        """
        简洁扫描和写入 - 完全按照 memory_db_writer.py 的方法
        
        Args:
            backup_task: 备份任务对象
            source_paths: 源路径列表
            exclude_patterns: 排除模式列表
            backup_set: 备份集对象
            restart: 是否重新扫描（清理旧数据）
        """
        backup_set_db_id = getattr(backup_set, "id", None)
        logger.info(
            f"[简洁扫描] backup_task_id={getattr(backup_task, 'id', 'N/A')}, "
            f"backup_set_id={backup_set_db_id}, source_paths={source_paths}, exclude_patterns={exclude_patterns}"
        )
        
        # 初始化/清理状态：同时在内存和数据库中设置 scan_status = 'running'
        try:
            if backup_task and backup_task.id:
                backup_task.scan_status = "running"
                await self.backup_db.update_scan_status(backup_task.id, "running")
            if restart and backup_set_db_id:
                # 清理原有 backup_files 记录，重新扫描
                await self.backup_db.clear_backup_files_for_set(backup_set_db_id)
        except Exception as e:
            logger.warning(f"[简洁扫描] 初始化扫描状态失败（忽略继续）: {e}")
        
        logger.info("[简洁扫描] 开始扫描文件（使用简洁扫描模式）")
        
        # 处理源路径为空的情况
        if not source_paths:
            logger.warning("[简洁扫描] source_paths 为空列表，没有文件要扫描")
            if backup_task:
                backup_task.total_files = 0
                backup_task.total_bytes = 0
            return
        
        # 统计信息
        stats: Dict[str, Any] = {
            "total_scanned": 0,  # 扫描到的文件数
            "total_written": 0,  # 成功写入的文件数
            "total_failed": 0,  # 写入失败的文件数
            "total_bytes": 0,  # 成功写入的总字节数
            "excluded_count": 0,  # 被排除的文件数
            "excluded_dirs": 0,  # 被排除的目录数
            "error_count": 0,  # 文件错误数
            "error_dirs": 0,  # 目录错误数
            "dirs_scanned": 0,  # 扫描到的目录数
            "dirs_skipped": 0,  # 跳过的目录数
            "symlinks_skipped": 0,  # 跳过的符号链接数
            "start_time": time.time(),
        }
        
        # 从内存获取分表名（backup_task.backup_files_table）
        table_name = None
        if backup_task:
            table_name = getattr(backup_task, "backup_files_table", None)
            if table_name and isinstance(table_name, str) and table_name.startswith("backup_files_"):
                logger.debug(f"[简洁扫描] 从内存获取分表名: {table_name}")
            else:
                # 如果内存中没有，回退到查询数据库（仅一次）
                if backup_set_db_id:
                    try:
                        from utils.scheduler.db_utils import get_backup_files_table_by_set_id
                        async with get_opengauss_connection() as conn:
                            table_name = await get_backup_files_table_by_set_id(conn, backup_set_db_id)
                            # 同时更新内存中的 backup_task，供后续使用
                            if backup_task and table_name:
                                backup_task.backup_files_table = table_name
                            logger.debug(f"[简洁扫描] 从数据库获取分表名: {table_name}")
                    except Exception as e:
                        logger.warning(f"[简洁扫描] 获取分表名失败: {e}")
        
        if not table_name:
            logger.error("[简洁扫描] 无法获取分表名，扫描无法继续")
            return
        
        # 批次大小：使用 SCAN_UPDATE_INTERVAL
        batch_size = getattr(self.settings, "SCAN_UPDATE_INTERVAL", 1000) or 1000
        current_batch: List[Dict[str, Any]] = []
        batch_number = 0
        
        # 进度输出相关
        last_progress_time = time.time()
        progress_interval = getattr(self.settings, "SCAN_LOG_INTERVAL_SECONDS", 60) or 60
        last_log_count = 0
        
        # 优化：打开数据库连接后持续复用，不关闭（提高速度）
        scan_conn = None
        try:
            # 手动打开连接，不使用 context manager（避免自动关闭）
            conn_context = get_opengauss_connection()
            scan_conn = await conn_context.__aenter__()
            logger.debug("[简洁扫描] 已打开数据库连接，将在整个扫描过程中复用")
            
            async def flush_batch(current_dir_str: str = ""):
                """将当前批次写入数据库（使用 memory_db_writer 的方法）"""
                nonlocal current_batch, batch_number, stats, last_progress_time, last_log_count, scan_conn
                if not current_batch or not backup_set_db_id or not table_name or not scan_conn:
                    current_batch = []
                    return
                
                # 准备批量插入数据（完全按照 memory_db_writer._prepare_insert_data_for_opengauss 的方法）
                insert_data = []
                for file_info in current_batch:
                    try:
                        data_tuple = self._prepare_insert_data_for_opengauss(file_info, backup_set_db_id)
                        insert_data.append(data_tuple)
                    except Exception as e:
                        file_path = file_info.get("path", "unknown")
                        logger.warning(
                            f"[简洁扫描] 准备插入数据失败: {file_path[:200]}, 错误: {e}"
                        )
                        stats["total_failed"] += 1
                
                if not insert_data:
                    current_batch = []
                    return
                
                # 批量插入（使用复用的连接，完全按照 memory_db_writer 的方法）
                try:
                    await scan_conn.executemany(
                        f"""
                        INSERT INTO {table_name} (
                            backup_set_id, file_path, file_name, directory_path, display_name,
                            file_type, file_size, compressed_size, file_permissions, file_owner,
                            file_group, created_time, modified_time, accessed_time, tape_block_start,
                            tape_block_count, compressed, encrypted, checksum, is_copy_success,
                            copy_status_at, backup_time, chunk_number, version,
                            created_at, updated_at
                        ) VALUES (
                            $1, $2, $3, $4, $5, $6, $7, $8, $9, $10,
                            $11, $12, $13, $14, $15, $16, $17, $18, $19, $20,
                            $21, $22, $23, $24, NOW(), NOW()
                        )
                        """,
                        insert_data,
                    )
                    
                    # psycopg3 binary protocol 需要显式提交事务
                    actual_conn = scan_conn._conn if hasattr(scan_conn, "_conn") else scan_conn
                    try:
                        await actual_conn.commit()
                    except Exception as commit_err:
                        logger.warning(
                            f"[简洁扫描] 提交批次事务失败（可能已自动提交）: {commit_err}"
                        )
                        try:
                            await actual_conn.rollback()
                        except Exception:
                            pass
                        # 提交失败，这批文件未写入，计入失败统计
                        stats["total_failed"] += len(insert_data)
                        current_batch = []
                        return
                    
                    # 只有成功提交后才统计
                    written = len(insert_data)
                    stats["total_written"] += written
                    stats["total_bytes"] += sum(
                        (fi.get("size", 0) or 0) for fi in current_batch[:written]
                    )
                    batch_number += 1
                    
                    # 更新内存中的任务对象统计信息（供 UI 使用）
                    if backup_task:
                        backup_task.total_files = stats["total_written"]
                        backup_task.total_bytes = stats["total_bytes"]
                    
                    # 进度日志
                    now_ts = time.time()
                    elapsed = now_ts - stats["start_time"]
                    files_per_sec = stats["total_written"] / elapsed if elapsed > 0 else 0
                    # 只在批次较大或达到日志间隔时输出
                    if written >= 5000 or (now_ts - last_progress_time >= progress_interval):
                        current_dir_display = current_dir_str[:80] + "..." if current_dir_str and len(current_dir_str) > 80 else (current_dir_str or "")
                        logger.info(
                            f"[简洁扫描] 批次 {batch_number}: 已写入 {stats['total_written']:,} 个文件, "
                            f"总容量: {format_bytes(stats['total_bytes'])}, 速度: {files_per_sec:.0f} 文件/秒"
                            + (f", 当前目录: {current_dir_display}" if current_dir_display else "")
                        )
                        last_progress_time = now_ts
                    
                    # 不再在扫描过程中同步数据库，只更新内存数据供 UI 使用
                    # UI 会从内存中的 backup_task 对象获取 total_files 和 total_bytes
                    
                    last_log_count = stats["total_written"]
                except Exception as e:
                    logger.error(
                        f"[简洁扫描] 批次写入失败: {e}", exc_info=True
                    )
                    # 异常时回滚
                    try:
                        actual_conn = scan_conn._conn if hasattr(scan_conn, "_conn") else scan_conn
                        if hasattr(actual_conn, "rollback"):
                            await actual_conn.rollback()
                    except Exception:
                        pass
                    # 写入失败，这批文件未写入，计入失败统计
                    stats["total_failed"] += len(insert_data)
                
                finally:
                    current_batch = []
            
            # 主扫描循环（os.scandir + FileScanner，完全按照原扫描文件的方法）
            for idx, source_path_str in enumerate(source_paths):
                logger.info(
                    f"[简洁扫描] 扫描源路径 {idx + 1}/{len(source_paths)}: {source_path_str}"
                )
                
                if not os.path.exists(source_path_str):
                    logger.warning(
                        f"[简洁扫描] 源路径不存在，跳过: {source_path_str}"
                    )
                    continue
                
                # 单个文件
                if os.path.isfile(source_path_str):
                    try:
                        if self.file_scanner.should_exclude_file(
                            source_path_str, exclude_patterns
                        ):
                            stats["excluded_count"] += 1
                            continue
                        file_info = await self.file_scanner.get_file_info(Path(source_path_str))
                        if file_info:
                            current_batch.append(file_info)
                            stats["total_scanned"] += 1
                            if len(current_batch) >= batch_size:
                                await flush_batch(source_path_str)
                    except Exception as e:
                        logger.warning(
                            f"[简洁扫描] 处理单个文件失败: {source_path_str}, 错误: {e}"
                        )
                        stats["error_count"] += 1
                    continue
                
                # 目录：使用 os.scandir 递归扫描
                if os.path.isdir(source_path_str):
                    logger.info(
                        f"[简洁扫描] 扫描目录: {source_path_str}（os.scandir 模式）"
                    )
                    
                    # 检查目录本身是否被排除
                    if self.file_scanner.should_exclude_file(
                        source_path_str, exclude_patterns
                    ):
                        logger.info(
                            f"[简洁扫描] 目录匹配排除规则，跳过整个目录: {source_path_str}"
                        )
                        stats["excluded_dirs"] += 1
                        continue
                    
                    # 使用绝对路径字符串，避免 Path 对象开销
                    dirs_to_scan = [os.path.abspath(source_path_str)]
                    scanned_dirs = set()
                    
                    try:
                        while dirs_to_scan:
                            current_dir_str = dirs_to_scan.pop(0)
                            # 确保使用绝对路径（避免重复解析）
                            if not os.path.isabs(current_dir_str):
                                current_dir_str = os.path.abspath(current_dir_str)
                            
                            if current_dir_str in scanned_dirs:
                                stats["dirs_skipped"] += 1
                                continue
                            scanned_dirs.add(current_dir_str)
                            stats["dirs_scanned"] += 1
                            
                            # 检查目录排除
                            if self.file_scanner.should_exclude_file(
                                current_dir_str, exclude_patterns
                            ):
                                stats["excluded_dirs"] += 1
                                continue
                            
                            try:
                                with os.scandir(current_dir_str) as entries:
                                    for entry in entries:
                                        try:
                                            # 直接使用 entry.path 字符串，避免 Path 对象开销
                                            entry_path_str = entry.path
                                            
                                            if self.file_scanner.should_exclude_file(
                                                entry_path_str, exclude_patterns
                                            ):
                                                stats["excluded_count"] += 1
                                                continue
                                            
                                            # 目录：加入队列（使用绝对路径字符串）
                                            if entry.is_dir(follow_symlinks=False):
                                                dirs_to_scan.append(os.path.abspath(entry_path_str))
                                                continue
                                            
                                            # 文件：获取文件信息
                                            if entry.is_file(follow_symlinks=False):
                                                file_info = self.file_scanner.get_file_info_from_entry(
                                                    entry
                                                )
                                                if file_info:
                                                    current_batch.append(file_info)
                                                    stats["total_scanned"] += 1
                                                    
                                                    if len(current_batch) >= batch_size:
                                                        await flush_batch(current_dir_str)
                                                else:
                                                    stats["error_count"] += 1
                                                continue
                                            
                                            # 其他情况（符号链接等）简单跳过并计数
                                            if entry.is_symlink():
                                                stats["symlinks_skipped"] += 1
                                            else:
                                                stats["error_count"] += 1
                                        except (PermissionError, OSError, FileNotFoundError):
                                            stats["error_count"] += 1
                                            continue
                            except (PermissionError, OSError) as e:
                                logger.warning(
                                    f"[简洁扫描] 无法访问目录: {current_dir_str}, 错误: {e}"
                                )
                                stats["error_dirs"] += 1
                                continue
                    except Exception as e:
                        logger.warning(
                            f"[简洁扫描] 扫描目录失败: {source_path_str}, 错误: {e}"
                        )
                        stats["error_count"] += 1
                    continue
            
            # 写入最后一个批次
            if current_batch:
                logger.info(
                    f"[简洁扫描] 写入最后批次 ({len(current_batch)} 个文件)..."
                )
                await flush_batch("")
            
            # 扫描结束：更新内存状态，并做一次最终数据库同步
            if backup_task:
                backup_task.total_files = stats["total_written"]
                backup_task.total_bytes = stats["total_bytes"]
                try:
                    if hasattr(self.backup_db, "update_scan_progress_only"):
                        await self.backup_db.update_scan_progress_only(
                            backup_task,
                            stats["total_written"],
                            stats["total_bytes"],
                        )
                except Exception as sync_err:
                    logger.debug(
                        f"[简洁扫描] 扫描结束时同步扫描统计到数据库失败（忽略继续）: {sync_err}"
                    )
            
            elapsed = time.time() - stats["start_time"]
            files_per_sec = (
                stats["total_written"] / elapsed if elapsed > 0 else 0.0
            )
            logger.info(
                f"[简洁扫描] 扫描完成：成功写入 {stats['total_written']:,} 个文件，"
                f"总大小 {format_bytes(stats['total_bytes'])}, 平均速度 {files_per_sec:.1f} 文件/秒, "
                f"失败 {stats['total_failed']:,} 个, 排除 {stats['excluded_count']:,} 个"
            )
            
            # 扫描全部结束后，在内存和数据库中设置 scan_status = 'completed'
            if backup_task and backup_task.id:
                try:
                    backup_task.scan_status = "completed"
                    await self.backup_db.update_scan_status(backup_task.id, "completed")
                except Exception as e:
                    logger.warning(f"[简洁扫描] 更新扫描状态为 completed 失败（忽略继续）: {e}")
        
        finally:
            # 关闭数据库连接（整个扫描过程结束）
            if scan_conn:
                try:
                    actual_conn = scan_conn._conn if hasattr(scan_conn, "_conn") else scan_conn
                    if hasattr(actual_conn, "close"):
                        await actual_conn.close()
                    logger.debug("[简洁扫描] 已关闭数据库连接")
                except Exception as e:
                    logger.warning(f"[简洁扫描] 关闭数据库连接失败: {e}")
    
    def _prepare_insert_data_for_opengauss(self, file_info: Dict, backup_set_id: int) -> tuple:
        """
        为openGauss准备插入数据（完全按照 memory_db_writer._prepare_insert_data_for_opengauss 的方法）
        
        Args:
            file_info: 文件信息字典（来自 FileScanner）
            backup_set_id: 备份集ID
            
        Returns:
            插入数据元组
        """
        # 基本路径信息 - 来自文件扫描器
        file_path = file_info.get('path', '')
        # 使用 os.path.basename 替代 Path().name，性能更好
        file_name = file_info.get('name') or (os.path.basename(file_path) if file_path else '')
        
        # 目录路径：使用 os.path.dirname 替代 Path().parent，性能更好
        if file_path:
            directory_path = os.path.dirname(file_path)
            # 如果目录路径是根路径（如 "C:\" 或 "/"）或为空，设为 None
            if not directory_path or directory_path == os.path.dirname(directory_path):
                directory_path = None
        else:
            directory_path = None
        
        # 显示名称（暂时与文件名相同）
        display_name = file_name
        
        # 文件类型 - 根据扫描器输出判断
        if file_info.get('is_file', True):
            file_type = 'file'
        elif file_info.get('is_dir', False):
            file_type = 'directory'
        elif file_info.get('is_symlink', False):
            file_type = 'symlink'
        else:
            file_type = 'file'
        
        # 文件大小 - 关键字段！直接从扫描器的size字段获取
        file_size = file_info.get('size', 0) or 0
        
        # 压缩大小（初始为None，压缩时更新）
        compressed_size = None
        
        # 文件权限 - 来自扫描器
        file_permissions = file_info.get('permissions')
        
        # 文件所有者和组（初始为None，Linux环境下可扩展）
        file_owner = None
        file_group = None
        
        # 时间戳处理 - 优先使用扫描器提供的modified_time
        modified_time = file_info.get('modified_time')
        if isinstance(modified_time, datetime):
            modified_time = modified_time.replace(tzinfo=timezone.utc)
        else:
            modified_time = datetime.now(timezone.utc)
        
        # 创建时间和访问时间（暂时使用修改时间作为默认值）
        created_time = modified_time
        accessed_time = modified_time
        
        # 磁带相关信息（初始为None，压缩时更新）
        tape_block_start = None
        tape_block_count = None
        compressed = False
        encrypted = False
        checksum = None
        is_copy_success = False
        copy_status_at = None
        
        # 备份时间
        backup_time = datetime.now(timezone.utc)
        
        # 其他字段
        chunk_number = None
        version = 1
        
        # 返回格式：按照openGauss INSERT语句的字段顺序（不包含created_at和updated_at，它们在SQL中使用NOW()）
        # 注意：file_metadata 和 tags 字段已从扫描阶段删除，压缩阶段会更新 file_metadata
        return (
            backup_set_id,     # backup_set_id
            file_path,         # file_path
            file_name,         # file_name
            directory_path,    # directory_path
            display_name,      # display_name
            file_type,         # file_type
            file_size,         # file_size
            compressed_size,   # compressed_size
            file_permissions,  # file_permissions
            file_owner,        # file_owner
            file_group,        # file_group
            created_time,      # created_time
            modified_time,     # modified_time
            accessed_time,      # accessed_time
            tape_block_start,  # tape_block_start
            tape_block_count,  # tape_block_count
            compressed,        # compressed
            encrypted,         # encrypted
            checksum,          # checksum
            is_copy_success,   # is_copy_success
            copy_status_at,    # copy_status_at
            backup_time,       # backup_time
            chunk_number,      # chunk_number
            version            # version
        )

