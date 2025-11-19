#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
文件移动后台工作线程
独立的后台线程，负责：扫描 final 目录 → 移动到磁带（独立运行，不与其他程序关联）
"""

import asyncio
import os
import shutil
import logging
import time
from pathlib import Path
from typing import Optional, Dict, Any
from config.settings import get_settings

logger = logging.getLogger(__name__)


class FileMoveWorker:
    """文件移动后台任务管理器 - 独立扫描 final 目录并移动到磁带"""
    
    def __init__(self, tape_file_mover=None):
        self.tape_file_mover = tape_file_mover
        self.settings = get_settings()
        self.file_move_task: Optional[asyncio.Task] = None
        self._running = False
        self._scan_interval = 5  # 扫描间隔（秒）
        self._processed_files = set()  # 已处理文件的集合（文件名）
    
    def start(self):
        """启动文件移动后台任务"""
        if self._running:
            logger.warning("[文件移动线程] 文件移动任务已在运行")
            return
        
        self.file_move_task = asyncio.create_task(self._file_move_worker())
        self._running = True
        logger.info("[文件移动线程] 文件移动后台任务已启动（独立扫描 final 目录）")
    
    async def stop(self):
        """停止文件移动后台任务"""
        if not self._running:
            return
        
        self._running = False
        if self.file_move_task:
            self.file_move_task.cancel()
            try:
                await self.file_move_task
            except asyncio.CancelledError:
                pass
        logger.info("[文件移动线程] 文件移动后台任务已停止")

    def _get_final_dir(self) -> Path:
        """获取 final 目录路径"""
        compress_dir = Path(self.settings.BACKUP_COMPRESS_DIR)
        final_dir = compress_dir / "final"
        return final_dir

    def _extract_backup_set_id_from_filename(self, filename: str) -> Optional[str]:
        """从文件名提取 backup_set.set_id
        
        文件名格式: backup_{set_id}_{timestamp}.7z 或 backup_{set_id}_{timestamp}.tar.gz 等
        """
        try:
            # 文件名格式: backup_{set_id}_{timestamp}.{ext}
            if filename.startswith("backup_"):
                parts = filename.split("_")
                if len(parts) >= 2:
                    return parts[1]  # set_id
            return None
        except Exception as e:
            logger.debug(f"提取 backup_set_id 失败: {filename}, 错误: {str(e)}")
            return None

    async def _file_move_worker(self):
        """独立的文件移动后台任务：扫描 final 目录，发现文件后移动到磁带"""
        logger.info("[文件移动线程] ========== 文件移动后台任务已启动 ==========")
        
        try:
            while self._running:
                try:
                    # 扫描 final 目录
                    final_dir = self._get_final_dir()
                    
                    if not final_dir.exists():
                        logger.debug(f"[文件移动线程] final 目录不存在: {final_dir}，等待 {self._scan_interval} 秒后重试")
                        await asyncio.sleep(self._scan_interval)
                        continue
                    
                    # 扫描 final 目录下的所有子目录（每个 backup_set.set_id 一个子目录）
                    found_files = []
                    for set_id_dir in final_dir.iterdir():
                        if not set_id_dir.is_dir():
                            continue
                        
                        # 扫描该备份集目录下的所有压缩文件
                        for file_path in set_id_dir.iterdir():
                            if not file_path.is_file():
                                continue
                            
                            # 检查是否是压缩文件（.7z, .tar.gz, .tar, .tar.zst 等）
                            if file_path.suffix in ['.7z', '.gz', '.tar', '.zst'] or file_path.name.endswith('.tar.gz'):
                                # 检查是否已处理过（避免重复处理）
                                file_key = f"{set_id_dir.name}/{file_path.name}"
                                if file_key not in self._processed_files:
                                    found_files.append((set_id_dir.name, file_path))
                    
                    # 处理找到的文件
                    if found_files:
                        logger.info(f"[文件移动线程] 扫描到 {len(found_files)} 个新文件待移动到磁带")
                        
                        for set_id, file_path in found_files:
                            if not self._running:
                                break
                            
                            file_key = f"{set_id}/{file_path.name}"
                            
                            try:
                                # 检查文件是否仍然存在（可能在其他线程中被删除）
                                if not file_path.exists():
                                    logger.debug(f"[文件移动线程] 文件已不存在，跳过: {file_path.name}")
                                    self._processed_files.add(file_key)
                                    continue
                                
                                logger.info(f"[文件移动线程] 开始处理文件: {file_path.name} (backup_set: {set_id})")
                                
                                # 将文件加入磁带移动队列
                                if self.tape_file_mover:
                                    # 尝试获取 backup_set 对象（需要从数据库查询）
                                    # 这里简化处理，直接使用 set_id 字符串
                                    # tape_file_mover 可能需要 backup_set 对象，这里暂时传入 None 或简化处理
                                    try:
                                        from models.backup import BackupSet, BackupTaskType, BackupSetStatus
                                        from utils.scheduler.db_utils import is_opengauss, get_opengauss_connection
                                        from backup.backup_db import BackupDB
                                        
                                        backup_set = None
                                        if is_opengauss():
                                            async with get_opengauss_connection() as conn:
                                                row = await conn.fetchrow(
                                                    """
                                                    SELECT id, set_id, set_name, backup_group, status, backup_task_id, tape_id,
                                                           backup_type, backup_time, source_info, retention_until, auto_delete,
                                                           total_files, total_bytes, compressed_bytes, compression_ratio, chunk_count,
                                                           checksum, verified, verified_at, created_at, updated_at
                                                    FROM backup_sets
                                                    WHERE set_id = $1
                                                    """,
                                                    set_id
                                                )
                                                
                                                if row:
                                                    # 解析枚举值
                                                    backup_type_str = row['backup_type']
                                                    # BackupTaskType 的值是小写（如 "full", "incremental"）
                                                    if backup_type_str:
                                                        try:
                                                            backup_type = BackupTaskType(backup_type_str.lower())
                                                        except ValueError:
                                                            backup_type = BackupTaskType.FULL
                                                    else:
                                                        backup_type = BackupTaskType.FULL
                                                    
                                                    status_str = row['status']
                                                    # BackupSetStatus 的值是小写（如 "active", "archived"）
                                                    if status_str:
                                                        try:
                                                            status = BackupSetStatus(status_str.lower())
                                                        except ValueError:
                                                            status = BackupSetStatus.ACTIVE
                                                    else:
                                                        status = BackupSetStatus.ACTIVE
                                                    
                                                    # 解析 source_info JSON
                                                    import json
                                                    source_info = json.loads(row['source_info']) if row['source_info'] else None
                                                    
                                                    # 创建 BackupSet 对象
                                                    backup_set = BackupSet(
                                                        id=row['id'],
                                                        set_id=row['set_id'],
                                                        set_name=row['set_name'],
                                                        backup_group=row['backup_group'],
                                                        status=status,
                                                        backup_task_id=row['backup_task_id'],
                                                        tape_id=row['tape_id'],
                                                        backup_type=backup_type,
                                                        backup_time=row['backup_time'],
                                                        source_info=source_info,
                                                        retention_until=row['retention_until'],
                                                        auto_delete=row.get('auto_delete', True),
                                                        total_files=row.get('total_files', 0),
                                                        total_bytes=row.get('total_bytes', 0),
                                                        compressed_bytes=row.get('compressed_bytes', 0),
                                                        compression_ratio=row.get('compression_ratio'),
                                                        chunk_count=row.get('chunk_count', 0),
                                                        checksum=row.get('checksum'),
                                                        verified=row.get('verified', False),
                                                        verified_at=row.get('verified_at')
                                                    )
                                        else:
                                            # SQLite 版本：使用 BackupDB 的方法
                                            backup_db = BackupDB()
                                            backup_set = await backup_db.get_backup_set_by_set_id(set_id)
                                        
                                        if backup_set:
                                            # 从文件名提取 chunk_number（如果可能）
                                            # 文件名格式: backup_{set_id}_{timestamp}.{ext}
                                            # chunk_number 可能需要在文件名中编码，或者从数据库查询
                                            # 这里暂时使用 0 作为默认值
                                            chunk_number = 0
                                            
                                            # 定义回调函数
                                            def move_callback(source_path: str, tape_file_path: Optional[str], success: bool, error: Optional[str]):
                                                """文件移动完成后的回调函数"""
                                                if success and tape_file_path:
                                                    logger.info(f"[文件移动线程] 文件已成功移动到磁带机: {tape_file_path}")
                                                elif not success:
                                                    logger.error(f"[文件移动线程] 文件移动到磁带机失败: {source_path}, 错误: {error}")
                                            
                                            # 将文件加入磁带移动队列
                                            added = self.tape_file_mover.add_file(
                                                str(file_path),
                                                backup_set,
                                                chunk_number,
                                                callback=move_callback,
                                                backup_task=None  # 暂时不传递 backup_task
                                            )
                                            
                                            if added:
                                                logger.info(f"[文件移动线程] ✅ 文件已加入磁带移动队列: {file_path.name}")
                                                self._processed_files.add(file_key)
                                            else:
                                                logger.error(f"[文件移动线程] ❌ 文件加入磁带移动队列失败: {file_path.name}")
                                        else:
                                            logger.warning(f"[文件移动线程] 未找到 backup_set (set_id={set_id})，跳过文件: {file_path.name}")
                                            # 即使找不到 backup_set，也标记为已处理，避免重复扫描
                                            self._processed_files.add(file_key)
                                    except Exception as e:
                                        logger.error(f"[文件移动线程] 处理文件时发生错误: {file_path.name}, 错误: {str(e)}", exc_info=True)
                                        # 发生错误时，标记为已处理，避免无限重试
                                        self._processed_files.add(file_key)
                                else:
                                    logger.warning(f"[文件移动线程] 磁带文件移动器未初始化，跳过文件: {file_path.name}")
                                    # 即使没有 tape_file_mover，也标记为已处理
                                    self._processed_files.add(file_key)
                                
                            except Exception as file_error:
                                logger.error(f"[文件移动线程] 处理文件失败: {file_path.name}, 错误: {str(file_error)}", exc_info=True)
                                # 发生错误时，标记为已处理，避免无限重试
                                self._processed_files.add(file_key)
                    else:
                        # 没有找到新文件，等待后继续扫描
                        await asyncio.sleep(self._scan_interval)
                        
                except asyncio.CancelledError:
                    raise
                except Exception as scan_error:
                    logger.error(f"[文件移动线程] 扫描 final 目录时发生错误: {str(scan_error)}", exc_info=True)
                    await asyncio.sleep(self._scan_interval)
                    
        except asyncio.CancelledError:
            logger.warning("[文件移动线程] 文件移动任务被取消")
            raise
        except Exception as e:
            logger.error(f"[文件移动线程] 文件移动任务异常: {str(e)}", exc_info=True)
        finally:
            logger.info("[文件移动线程] 文件移动后台任务已退出")

