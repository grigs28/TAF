#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
文件移动后台工作线程
独立的后台线程，负责：temp → final → 磁带（顺序执行，不阻塞压缩）
"""

import asyncio
import os
import shutil
import logging
from pathlib import Path
from typing import Optional, Dict, Any, Callable

logger = logging.getLogger(__name__)


class FileMoveWorker:
    """文件移动后台任务管理器"""
    
    def __init__(self, tape_file_mover=None):
        self.tape_file_mover = tape_file_mover
        self.file_move_queue: Optional[asyncio.Queue] = None
        self.file_move_task: Optional[asyncio.Task] = None
        self._running = False
    
    def start(self):
        """启动文件移动后台任务"""
        if self._running:
            logger.warning("[文件移动线程] 文件移动任务已在运行")
            return
        
        self.file_move_queue = asyncio.Queue()
        self.file_move_task = asyncio.create_task(self._file_move_worker())
        self._running = True
        logger.info("[文件移动线程] 文件移动后台任务已启动（独立线程，不阻塞压缩）")
    
    async def stop(self):
        """停止文件移动后台任务"""
        if not self._running:
            return
        
        self._running = False
        if self.file_move_queue:
            # 发送停止信号
            await self.file_move_queue.put(None)
            # 等待任务完成
            if self.file_move_task:
                try:
                    await self.file_move_task
                except asyncio.CancelledError:
                    pass
        logger.info("[文件移动线程] 文件移动后台任务已停止")

    async def add_file_move_task(
        self,
        temp_path: Path,
        final_path: Path,
        backup_set: Any,  # BackupSet object
        chunk_number: int,
        callback: Optional[Callable] = None,
        backup_task: Optional[Any] = None
    ) -> bool:
        """添加文件移动任务到队列（非阻塞）"""
        if not self.file_move_queue:
            logger.error("[文件移动线程] 文件移动队列未初始化")
            return False
        
        move_info = {
            'temp_path': temp_path,
            'final_path': final_path,
            'backup_set': backup_set,
            'chunk_number': chunk_number,
            'callback': callback,
            'backup_task': backup_task
        }
        
        try:
            await self.file_move_queue.put(move_info)
            logger.debug(f"[文件移动线程] 文件移动任务已加入队列: {os.path.basename(final_path)}")
            return True
        except Exception as e:
            logger.error(f"[文件移动线程] 添加文件移动任务失败: {str(e)}")
            return False
    
    async def _file_move_worker(self):
        """独立的文件移动后台任务：顺序执行移动到final → 移动到磁带（严格顺序）
        
        注意：支持多个文件分次提交，即使第二个文件提交时第一个文件还在移动中，
        也能正确排队并顺序处理。每个文件都要等待前一个文件完全处理完成（移动到final + 加入磁带队列）
        后才能处理下一个。
        """
        logger.info("[文件移动线程] ========== 文件移动后台任务已启动 ==========")
        
        try:
            while self._running:
                # ========== 从队列中获取文件移动任务（支持多个文件分次提交） ==========
                try:
                    move_info = await asyncio.wait_for(self.file_move_queue.get(), timeout=1.0)
                    if move_info is None:
                        # 收到停止信号
                        logger.info("[文件移动线程] 收到停止信号，退出")
                        break
                    
                    # 获取文件路径
                    temp_path = Path(move_info['temp_path'])
                    final_path = Path(move_info['final_path'])
                    
                    logger.info(f"[文件移动线程] 开始处理文件移动任务: {final_path.name} (队列中还有 {self.file_move_queue.qsize()} 个任务)")
                    
                    try:
                        # ========== 阶段1：移动到final目录（顺序执行，等待完成） ==========
                        if not final_path.exists():
                            if temp_path.exists():
                                logger.info(f"[文件移动线程] [阶段1] 开始移动文件到final目录: {final_path.name}")
                                await asyncio.to_thread(shutil.move, str(temp_path), str(final_path))
                                logger.info(f"[文件移动线程] [阶段1] ✅ 文件已移动到final目录: {final_path.name}")
                            else:
                                logger.warning(f"[文件移动线程] [阶段1] 源文件不存在，跳过: {temp_path}")
                                self.file_move_queue.task_done()
                                continue
                        else:
                            logger.info(f"[文件移动线程] [阶段1] 文件已存在于final目录: {final_path.name}")
                        
                        # ========== 阶段2：将文件加入磁带移动队列（顺序执行，一个完成后再处理下一个） ==========
                        if self.tape_file_mover and final_path.exists():
                            logger.info(f"[文件移动线程] [阶段2] 开始将文件加入磁带移动队列: {final_path.name}")
                            added = self.tape_file_mover.add_file(
                                str(final_path),
                                move_info['backup_set'],
                                move_info['chunk_number'],
                                callback=move_info.get('callback'),
                                backup_task=move_info.get('backup_task')
                            )
                            if added:
                                logger.info(f"[文件移动线程] [阶段2] ✅ 文件已加入磁带移动队列: {final_path.name}")
                            else:
                                logger.error(f"[文件移动线程] [阶段2] ❌ 文件加入磁带移动队列失败: {final_path.name}")
                        else:
                            if not self.tape_file_mover:
                                logger.warning(f"[文件移动线程] [阶段2] 磁带文件移动器未初始化，跳过")
                            elif not final_path.exists():
                                logger.warning(f"[文件移动线程] [阶段2] 文件不存在于final目录，跳过: {final_path}")
                        
                        logger.info(f"[文件移动线程] ✅ 文件移动任务处理完成: {final_path.name}")
                    except Exception as move_error:
                        logger.error(f"[文件移动线程] 移动文件失败: {str(move_error)}", exc_info=True)
                    finally:
                        # 标记任务完成，无论成功或失败
                        self.file_move_queue.task_done()
                        
                except asyncio.TimeoutError:
                    # 超时，继续循环等待新任务
                    continue
        except asyncio.CancelledError:
            logger.warning("[文件移动线程] 文件移动任务被取消")
            raise
        except Exception as e:
            logger.error(f"[文件移动线程] 文件移动任务异常: {str(e)}", exc_info=True)
        finally:
            logger.info("[文件移动线程] 文件移动后台任务已退出")

