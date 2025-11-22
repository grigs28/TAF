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
        """
        独立的文件移动后台任务：检索final目录及其子目录，按目录结构移动到磁带
        
        新逻辑：
        1. 检索final目录及其所有子目录
        2. 找到文件就按目录结构移动到磁带
        3. 没有其他判断条件（不查询数据库、不验证backup_set等）
        4. 一一对应，按目录操作（保持目录结构）
        """
        logger.info("[文件移动线程] ========== 文件移动后台任务已启动 ==========")
        logger.info("[文件移动线程] 模式：检索final目录及其子目录，按目录结构移动到磁带（无其他判断条件）")
        
        try:
            while self._running:
                try:
                    # 扫描 final 目录及其所有子目录
                    final_dir = self._get_final_dir()
                    
                    if not final_dir.exists():
                        logger.debug(f"[文件移动线程] final 目录不存在: {final_dir}，等待 {self._scan_interval} 秒后重试")
                        await asyncio.sleep(self._scan_interval)
                        continue
                    
                    # 检索final目录及其所有子目录中的文件（按目录结构）
                    found_files = []  # [(相对路径, 文件路径), ...]
                    
                    # 递归扫描所有子目录
                    for root, dirs, files in os.walk(final_dir):
                        root_path = Path(root)
                        
                        # 计算相对于final_dir的相对路径
                        try:
                            relative_path = root_path.relative_to(final_dir)
                        except ValueError:
                            # 如果无法计算相对路径，使用绝对路径的一部分
                            relative_path = Path(root_path.name)
                        
                        # 扫描该目录下的所有文件
                        for file_name in files:
                            file_path = root_path / file_name
                            
                            if not file_path.is_file():
                                continue
                            
                            # 检查是否是压缩文件（.7z, .tar.gz, .tar, .tar.zst 等）
                            if file_path.suffix in ['.7z', '.gz', '.tar', '.zst'] or file_path.name.endswith('.tar.gz'):
                                # 检查是否已处理过（避免重复处理）
                                # 使用相对路径作为key，保持目录结构
                                file_key = str(relative_path / file_name) if relative_path != Path('.') else file_name
                                if file_key not in self._processed_files:
                                    found_files.append((relative_path, file_path, file_key))
                    
                    # 处理找到的文件（按目录结构移动）
                    if found_files:
                        logger.info(f"[文件移动线程] 扫描到 {len(found_files)} 个新文件待移动到磁带（按目录结构）")
                        
                        for relative_dir, file_path, file_key in found_files:
                            if not self._running:
                                break
                            
                            try:
                                # 检查文件是否仍然存在（可能在其他线程中被删除）
                                if not file_path.exists():
                                    logger.debug(f"[文件移动线程] 文件已不存在，跳过: {file_key}")
                                    self._processed_files.add(file_key)
                                    continue
                                
                                logger.info(f"[文件移动线程] 开始处理文件: {file_key}")
                                
                                # 将文件按目录结构移动到磁带（无其他判断条件）
                                if self.tape_file_mover:
                                    # 直接移动文件，不需要查询数据库、不需要验证backup_set等
                                    # 按目录结构：保持final目录下的目录结构
                                    
                                    # 创建磁带上的目标路径（保持目录结构）
                                    # 磁带盘符: O:\ 或配置的TAPE_DRIVE_LETTER
                                    tape_drive = Path(f"{self.settings.TAPE_DRIVE_LETTER}:\\")
                                    
                                    # 目标路径：磁带盘符 + 相对路径（保持目录结构）
                                    if relative_dir != Path('.'):
                                        tape_target_dir = tape_drive / relative_dir
                                        tape_target_path = tape_target_dir / file_path.name
                                    else:
                                        tape_target_dir = tape_drive
                                        tape_target_path = tape_target_dir / file_path.name
                                    
                                    logger.info(f"[文件移动线程] 准备移动到磁带: {file_key}")
                                    logger.info(f"[文件移动线程] 源路径: {file_path}")
                                    logger.info(f"[文件移动线程] 目标路径: {tape_target_path} (保持目录结构: {relative_dir})")
                                    
                                    # 确保目标目录存在
                                    try:
                                        tape_target_dir.mkdir(parents=True, exist_ok=True)
                                    except Exception as mkdir_error:
                                        logger.error(f"[文件移动线程] 创建目标目录失败: {tape_target_dir}, 错误: {mkdir_error}")
                                        self._processed_files.add(file_key)
                                        continue
                                    
                                    # 使用tape_file_mover移动文件
                                    # 注意：由于不再需要backup_set，需要修改tape_file_mover或使用shutil直接移动
                                    # 这里使用简化的移动方式：直接复制到磁带，然后删除源文件
                                    try:
                                        loop = asyncio.get_event_loop()
                                        await loop.run_in_executor(None, shutil.copy2, str(file_path), str(tape_target_path))
                                        
                                        # 验证移动是否成功（检查目标文件是否存在）
                                        if tape_target_path.exists():
                                            # 删除源文件
                                            try:
                                                file_path.unlink()
                                                logger.info(f"[文件移动线程] ✅ 文件已成功移动到磁带: {file_key} -> {tape_target_path}")
                                                self._processed_files.add(file_key)
                                            except Exception as del_error:
                                                logger.error(f"[文件移动线程] 删除源文件失败: {file_path}, 错误: {del_error}")
                                                # 即使删除失败，也标记为已处理（文件已复制到磁带）
                                                self._processed_files.add(file_key)
                                        else:
                                            logger.error(f"[文件移动线程] ❌ 文件复制到磁带后目标文件不存在: {tape_target_path}")
                                            self._processed_files.add(file_key)
                                    except Exception as move_error:
                                        logger.error(f"[文件移动线程] 移动文件到磁带失败: {file_key}, 错误: {str(move_error)}", exc_info=True)
                                        self._processed_files.add(file_key)
                                else:
                                    logger.warning(f"[文件移动线程] 磁带文件移动器未初始化，跳过文件: {file_key}")
                                    self._processed_files.add(file_key)
                                
                            except Exception as file_error:
                                logger.error(f"[文件移动线程] 处理文件失败: {file_key}, 错误: {str(file_error)}", exc_info=True)
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

