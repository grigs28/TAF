#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
备份引擎模块
Backup Engine Module
"""

import os
import asyncio
import logging
import hashlib
import sys
import re
import traceback
import time
from datetime import datetime, timedelta
from pathlib import Path
from typing import List, Dict, Any, Optional, Callable
import py7zr
import psutil

# 尝试导入 tqdm，如果不可用则使用简单的文本进度条
try:
    from tqdm import tqdm
    TQDM_AVAILABLE = True
except ImportError:
    TQDM_AVAILABLE = False

from config.settings import get_settings
from models.backup import BackupTask, BackupSet, BackupFile, BackupTaskStatus, BackupTaskType, BackupFileType
from models.system_log import OperationLog, OperationType, LogLevel, LogCategory
from tape.tape_manager import TapeManager
from tape.tape_cartridge import TapeCartridge
from utils.dingtalk_notifier import DingTalkNotifier
from utils.datetime_utils import now, format_datetime
from utils.log_utils import log_operation, log_system
import json
from pathlib import Path

# 导入新创建的子模块
from backup.utils import normalize_volume_label, extract_label_year_month, format_bytes, calculate_file_checksum
from backup.file_scanner import FileScanner
from backup.compressor import Compressor
from backup.backup_db import BackupDB
from backup.tape_handler import TapeHandler
from backup.backup_notifier import BackupNotifier
from backup.backup_scanner import BackupScanner
from backup.backup_task_manager import BackupTaskManager
from backup.final_dir_monitor import FinalDirMonitor
from backup.compression_worker import CompressionWorker

logger = logging.getLogger(__name__)


def kill_zstd_processes():
    """强制终止残留的压缩子进程（zstd 与配套 tar）。

    保留本函数名以向后兼容旧调用点；实现已下沉到 backup.utils，
    并补上了此前漏掉的 tar（它才是持续占用 NAS 带宽的那个）。
    """
    try:
        from backup.utils import kill_compression_processes
        kill_compression_processes(reason="关闭")
    except Exception as e:
        logger.warning(f"[关闭] 终止压缩进程失败: {e}")


# 向后兼容：保留 normalize_volume_label 和 extract_label_year_month 的导出
# 实际实现已移到 backup.utils 模块，这里直接使用导入的函数


class BackupEngine:
    """备份引擎"""

    def __init__(self):
        self.settings = get_settings()
        self.tape_manager: Optional[TapeManager] = None
        self.dingtalk_notifier: Optional[DingTalkNotifier] = None
        self._initialized = False
        self._current_task: Optional[BackupTask] = None
        self._current_compression_worker: Optional['CompressionWorker'] = None  # 存储当前运行的压缩工作线程
        
        # 初始化子模块
        self.file_scanner = FileScanner(settings=self.settings)
        self.compressor = Compressor(settings=self.settings)
        self.backup_db = BackupDB()
        self.tape_handler = TapeHandler(tape_manager=None, settings=self.settings)
        self.backup_notifier = BackupNotifier(dingtalk_notifier=None)
        self.backup_scanner = BackupScanner(file_scanner=self.file_scanner, backup_db=self.backup_db)
        self.task_manager = BackupTaskManager(settings=self.settings)
        
        # 初始化Final目录监控器（延迟初始化，需要tape_handler）
        self.final_dir_monitor: Optional[FinalDirMonitor] = None

    async def _get_notification_events(self) -> Dict[str, bool]:
        """获取通知事件配置（带缓存）- 委托给 BackupNotifier"""
        return await self.backup_notifier.get_notification_events()

    async def _get_backup_policy_parameters(self) -> Dict[str, Any]:
        """获取备份策略参数（从tapedrive和system配置）- 委托给 BackupNotifier"""
        return await self.backup_notifier.get_backup_policy_parameters(self.settings)
    
    async def _update_tape_path_in_db(self, backup_set: BackupSet, compressed_file: Dict, 
                                     tape_file_path: str, group_idx: int):
        """更新数据库中的磁带路径（在文件移动完成后调用）"""
        try:
            # 这里可以更新数据库中的磁带路径
            # 由于backup_db.save_backup_files_to_db已经保存了初始路径，这里可以更新为最终路径
            logger.debug(f"更新数据库中的磁带路径: {tape_file_path} (组索引: {group_idx})")
            # 如果需要更新，可以在这里实现
        except Exception as e:
            logger.warning(f"更新数据库中的磁带路径失败: {str(e)}")

    async def initialize(self):
        """初始化备份引擎"""
        try:
            import shutil

            # 先清除临时目录（启动时清理残留文件）
            compress_dir = Path(self.settings.BACKUP_COMPRESS_DIR)
            temp_dirs = [
                self.settings.BACKUP_TEMP_DIR,
                self.settings.RECOVERY_TEMP_DIR,
                self.settings.BACKUP_COMPRESS_DIR,
                compress_dir / "temp",  # temp临时目录
                compress_dir / "final",  # final正式目录
            ]

            # 清除所有临时目录内容（但不删除目录本身）
            for temp_dir_path in temp_dirs:
                temp_dir = Path(temp_dir_path)
                if temp_dir.exists():
                    try:
                        # 删除目录下的所有文件和子目录
                        for item in temp_dir.iterdir():
                            if item.is_dir():
                                shutil.rmtree(item, ignore_errors=True)
                            else:
                                item.unlink(missing_ok=True)
                        logger.info(f"已清除临时目录: {temp_dir}")
                    except Exception as e:
                        logger.warning(f"清除临时目录失败 {temp_dir}: {str(e)}")

            # 重新创建临时目录（确保目录存在）
            for temp_dir in temp_dirs:
                Path(temp_dir).mkdir(parents=True, exist_ok=True)

            # 初始化Final目录监控器（独立线程，10秒轮询扫描）
            self.final_dir_monitor = FinalDirMonitor(
                tape_handler=self.tape_handler,
                settings=self.settings,
                dingtalk_notifier=self.dingtalk_notifier  # 传递钉钉通知器
            )
            self.final_dir_monitor.start()
            logger.info("Final目录监控器已启动（独立线程，10秒轮询扫描）")

            # 清理上次非正常退出遗留的僵尸任务（status=running 但进程已死）
            try:
                from utils.scheduler.db_utils import get_opengauss_connection
                from datetime import datetime as _dt
                async with get_opengauss_connection() as conn:
                    result = await conn.execute(
                        """
                        UPDATE backup_tasks
                        SET status = 'interrupted'::backuptaskstatus,
                            error_message = '上次运行意外中断',
                            updated_at = $1
                        WHERE status = 'running'::backuptaskstatus
                        """,
                        _dt.now()
                    )
                    logger.info(f"[启动] 已清理僵尸任务（running→interrupted）: {result}")
            except Exception as e:
                logger.warning(f"[启动] 清理僵尸任务失败: {str(e)}")

            self._initialized = True
            logger.info("备份引擎初始化完成")

        except Exception as e:
            logger.error(f"备份引擎初始化失败: {str(e)}")
            raise
    
    async def shutdown(self):
        """关闭备份引擎

        关闭策略（不等压缩完成，保护磁带）：
        1. kill zstd 进程（立即停止压缩）
        2. 停压缩线程（不等，直接取消）
        3. 等当前磁带写入完成（最多等5分钟，保护磁带数据完整性）
        4. 清理 final 目录（删除未写入磁带的文件，保留正在写入的）
        5. 标记任务为中断状态
        6. sync + 卸载LTFS
        7. 卸载SMB
        """
        try:
            # 1. 立即终止所有 zstd 压缩进程
            logger.info("[关闭] 1/7 终止zstd压缩进程...")
            kill_zstd_processes()

            # 2. 停止压缩工作线程（不等，直接取消）
            if self._current_compression_worker:
                try:
                    logger.info("[关闭] 2/7 停止压缩工作线程（不等待完成）...")
                    self._current_compression_worker._running = False
                    # 取消所有正在运行的压缩任务
                    if self._current_compression_worker.running_compression_futures:
                        for task in self._current_compression_worker.running_compression_futures:
                            if not task.done():
                                task.cancel()
                        self._current_compression_worker.running_compression_futures.clear()
                    # 取消主压缩循环
                    if (self._current_compression_worker.compression_task
                            and not self._current_compression_worker.compression_task.done()):
                        self._current_compression_worker.compression_task.cancel()
                    self._current_compression_worker = None
                    logger.info("[关闭] 压缩工作线程已停止")
                except Exception as e:
                    logger.warning(f"[关闭] 停止压缩工作线程失败: {str(e)}")

            # 3. 等待当前磁带写入完成（保护正在写入的文件，最多等5分钟）
            if self.final_dir_monitor:
                current_file = self.final_dir_monitor._current_tape_file
                if current_file:
                    logger.info(f"[关闭] 3/7 等待磁带写入完成: {current_file}（最多5分钟）...")
                else:
                    logger.info("[关闭] 3/7 无正在写入的磁带文件，跳过等待")

                # 停止监控线程（等待当前写入完成）
                try:
                    self.final_dir_monitor.stop()
                    logger.info("[关闭] Final目录监控器已停止")
                except Exception as e:
                    logger.warning(f"[关闭] 停止Final目录监控器失败: {str(e)}")

            # 4. 清理 final 目录（残留压缩文件移入隔离目录，不删除）
            if self.final_dir_monitor:
                try:
                    logger.info("[关闭] 4/7 清理final目录...")
                    deleted = await asyncio.get_event_loop().run_in_executor(
                        None, self.final_dir_monitor.cleanup_final_dir)
                    logger.info(f"[关闭] final目录已清理，删除 {deleted} 个残留文件")
                    # 停机意味着本次任务重新开始：隔离目录中的失败文件也不再保留
                    q_deleted = await asyncio.get_event_loop().run_in_executor(
                        None, self.final_dir_monitor.cleanup_quarantine_dir)
                    logger.info(f"[关闭] 隔离目录已清理，删除 {q_deleted} 个文件")
                except Exception as e:
                    logger.warning(f"[关闭] 清理final目录失败: {str(e)}")

            # 5. 标记任务为中断状态
            logger.info("[关闭] 5/7 标记任务为中断状态...")
            if self._current_task:
                logger.info(f"[关闭] 取消当前备份任务 (id={self._current_task.id})")
                self._current_task = None
            try:
                from utils.scheduler.db_utils import get_opengauss_connection
                from datetime import datetime as _dt
                async with get_opengauss_connection() as conn:
                    result = await conn.execute(
                        """
                        UPDATE backup_tasks
                        SET status = 'cancelled'::backuptaskstatus,
                            error_message = '系统关闭，任务被中断',
                            updated_at = $1
                        WHERE status IN ('running'::backuptaskstatus, 'pending'::backuptaskstatus)
                        """,
                        _dt.now()
                    )
                    logger.info(f"[关闭] 已将运行中/等待中的备份任务标记为中断: {result}")
            except Exception as e:
                logger.warning(f"[关闭] 标记中断任务失败: {str(e)}")

            # 6. sync + 卸载LTFS（sync确保数据写入磁带）
            logger.info("[关闭] 6/7 卸载LTFS...")
            if self.tape_handler:
                try:
                    if self.tape_handler._ltfs_mounted:
                        # eject=True：卸载后执行 mt offline，让带子退出读写通路。
                        # 否则带子带着"加载状态 + 停在索引分区末尾"随系统关机，
                        # 重启后读卷标签可能失败（2026-10-22 已发生一次）。
                        await self.tape_handler.unmount_ltfs(eject=True)
                        logger.info("[关闭] LTFS已安全卸载")
                    else:
                        # 未挂载但带子可能仍加载，同样 offline 收尾
                        await self.tape_handler.offline_tape()
                except Exception as e:
                    logger.warning(f"[关闭] 卸载LTFS失败: {str(e)}")

            # 7. 卸载SMB挂载
            logger.info("[关闭] 7/7 卸载SMB挂载...")
            try:
                from utils.network_path import cleanup_mounts
                cleanup_mounts()
                logger.info("[关闭] SMB挂载已卸载")
            except Exception as e:
                logger.warning(f"[关闭] 卸载SMB挂载失败: {str(e)}")

        except Exception as e:
            logger.error(f"[关闭] 关闭备份引擎时发生错误: {str(e)}")

    async def _offline_tape_after_task(self, reason: str = '任务结束'):
        """任务结束后卸载 LTFS 并 offline。

        避免磁带以"加载状态 + 停在索引分区末尾"跟随系统关机/掉电，
        防止重启后卷标签读取失败。下次挂载时由 mt load 兜底或 LTFS
        自身 LOAD 重新加载。
        """
        try:
            th = getattr(self, 'tape_handler', None)
            if not th:
                logger.info(f"[备份引擎] {reason}：无 tape_handler，跳过卸载/offline")
                return
            # 延迟 20 秒：等写带收尾、文件系统缓存、残留操作完全结束再卸载
            logger.info(f"[备份引擎] {reason}：20 秒后卸载 LTFS 并 offline 磁带")
            await asyncio.sleep(20)
            if getattr(th, '_ltfs_mounted', False):
                logger.info("[备份引擎] LTFS 已挂载，开始卸载（fusermount → 等索引写入 → mt offline）...")
                ok = await th.unmount_ltfs(eject=True)
                logger.info(f"[备份引擎] {reason}：卸载并 offline 完成（结果={ok}）")
            else:
                logger.info("[备份引擎] LTFS 未挂载，检查带子是否仍加载并 offline...")
                try:
                    await th.offline_tape()
                except Exception as _oe:
                    logger.warning(f"[备份引擎] offline_tape 异常: {_oe}")
                logger.info(f"[备份引擎] {reason}：offline 检查完成")
        except Exception as e:
            logger.warning(f"[备份引擎] {reason}：离线磁带失败（非致命）: {e}")

    async def _send_tape_error_notification(self, error_msg: str):
        """发送磁带错误通知"""
        if self.dingtalk_notifier:
            try:
                await self.dingtalk_notifier.send_backup_notification(
                    "磁带检查",
                    "failed",
                    {'error': error_msg}
                )
                logger.info("[启动] 钉钉通知已发送")
            except Exception as e:
                logger.warning(f"[启动] 发送钉钉通知失败: {e}")

    def set_dependencies(self, tape_manager: TapeManager, dingtalk_notifier: DingTalkNotifier):
        """设置依赖组件"""
        self.tape_manager = tape_manager
        self.dingtalk_notifier = dingtalk_notifier
        # 更新子模块的依赖
        self.tape_handler.tape_manager = tape_manager
        self.tape_handler.dingtalk_notifier = dingtalk_notifier
        self.backup_notifier.dingtalk_notifier = dingtalk_notifier
        # 更新FinalDirMonitor的钉钉通知器
        if self.final_dir_monitor:
            self.final_dir_monitor.dingtalk_notifier = dingtalk_notifier

    def add_progress_callback(self, callback: Callable):
        """添加进度回调 - 委托给 BackupNotifier"""
        self.backup_notifier.add_progress_callback(callback)

    async def create_backup_task(self, task_name: str, source_paths: List[str],
                               task_type: BackupTaskType = BackupTaskType.FULL,
                               **kwargs) -> Optional[BackupTask]:
        """创建备份任务 - 委托给 BackupTaskManager"""
        return await self.task_manager.create_backup_task(task_name, source_paths, task_type, **kwargs)

    async def execute_backup_task(self, backup_task: BackupTask, scheduled_task=None, manual_run: bool = False) -> bool:
        """执行备份任务

        执行前检查：
        1. 任务是否已执行过（在存活期内）- 仅自动执行时检查，手动运行跳过
        2. 任务是否正在执行
        3. 磁带卷标是否当月（仅当备份目标为磁带时）
        4. 完整备份前自动格式化磁带（受 ENABLE_TAPE_FORMAT_BEFORE_FULL 控制）

        Args:
            backup_task: 备份任务对象
            scheduled_task: 计划任务对象（可选）
            manual_run: 是否为手动运行（Web界面点击运行），默认为False
        """
        task_start_time = now()
        task_id = backup_task.id
        task_name = backup_task.task_name

        # 备份开始前清理恢复临时副本（/mnt/SSD/recovery_tmp）：
        # 恢复与备份互斥（备份期间恢复 tab 变灰），此刻不会有进行中的恢复；
        # 这些副本会占用 SSD 空间、影响压缩守卫，必须先清掉。
        try:
            _re_engine = getattr(self, 'recovery_engine', None)
            if _re_engine:
                _n = _re_engine.cleanup_recovery_tmp()
                if _n:
                    logger.info(f"[备份引擎] 已清理恢复临时副本 {_n} 个")
        except Exception as _ct_err:
            logger.warning(f"[备份引擎] 清理恢复临时副本失败: {_ct_err}")

        # 任务开始：重置在途/累计组统计（L/S 数量）
        try:
            self.compressor.reset_group_stats()
        except Exception:
            pass
        
        try:
            # 记录开始日志
            logger.info(f"========== 开始执行备份任务 ==========")
            logger.info(f"任务名称: {task_name}")
            logger.info(f"任务ID: {task_id}")
            logger.info(f"任务类型: {backup_task.task_type}")
            logger.info(f"开始时间: {format_datetime(task_start_time)}")
            
            # 记录操作日志（直接await确保完成，审计日志为毫秒级操作）
            asyncio.create_task(log_system(
                level=LogLevel.INFO,
                category=LogCategory.BACKUP,
                message=f"开始执行备份任务: {task_name} (ID: {task_id})",
                module="backup.backup_engine",
                function="execute_backup_task",
                task_id=task_id,
                details={
                    "task_name": task_name,
                    "task_type": backup_task.task_type.value if hasattr(backup_task.task_type, 'value') else str(backup_task.task_type),
                    "start_time": format_datetime(task_start_time)
                }
            ))
            
            if not self._initialized:
                error_msg = "备份引擎未初始化"
                logger.error(error_msg)
                # 记录操作日志（直接await确保完成，审计日志为毫秒级操作）
                asyncio.create_task(log_system(
                    level=LogLevel.ERROR,
                    category=LogCategory.BACKUP,
                    message=error_msg,
                    module="backup.backup_engine",
                    function="execute_backup_task",
                    task_id=task_id
                ))
                raise RuntimeError(error_msg)

            self._current_task = backup_task
            # 同步到 task_manager，确保 get_task_status 能获取到内存统计
            self.task_manager._current_task = backup_task

            # 清零压缩进度信息（新任务开始时）
            backup_task.current_compression_progress = None

            # 清零累计写带统计（卡片显示"平均写带速度"）
            try:
                self.tape_handler.reset_tape_write_stats()
            except Exception:
                pass

            # 清零"等待压缩"统计（监控线程常驻，start() 的重置不会再次执行，
            # 否则等待秒数跨任务累计、写进本任务的 result_summary）
            try:
                if self.final_dir_monitor:
                    self.final_dir_monitor.reset_starvation_stats()
            except Exception:
                pass

            # 0. 获取备份策略参数（从tapedrive和system配置）
            logger.info("========== 获取备份策略参数 ==========")
            backup_policy = await self._get_backup_policy_parameters()
            logger.info(f"备份策略参数: 压缩级别={backup_policy.get('compression_level', 'N/A')}, "
                       f"最大文件大小={format_bytes(backup_policy.get('max_file_size', 0))}, "
                       f"保留天数={backup_policy.get('retention_days', 'N/A')}")
            
            # 将备份策略参数应用到备份任务（如果任务中没有设置）
            if not hasattr(backup_task, 'compression_level') or backup_task.compression_level is None:
                backup_task.compression_level = backup_policy.get('compression_level', self.settings.COMPRESSION_LEVEL)
            if not hasattr(backup_task, 'max_file_size') or backup_task.max_file_size is None:
                backup_task.max_file_size = backup_policy.get('max_file_size', self.settings.MAX_FILE_SIZE)
            if not hasattr(backup_task, 'retention_days') or backup_task.retention_days is None:
                backup_task.retention_days = backup_policy.get('retention_days', self.settings.DEFAULT_RETENTION_MONTHS * 30)

            # ========== 磁带检查流程（任务开始时执行）==========
            logger.info("========== 磁带检查 ==========")

            # 获取挂载点
            mount_point = self.tape_handler._get_ltfs_mount_point()

            # 1. 检查磁带卷标
            logger.info("[1] 检查磁带卷标")
            from tape.tape_operations import TapeOperations
            from backup.utils import extract_label_year_month

            tape_ops = TapeOperations()
            await tape_ops.initialize()
            label_info = await tape_ops._read_tape_label()

            # 无法读取卷标 → 进入格式化流程
            if not label_info or not label_info.get('tape_id'):
                logger.warning("⚠️ 无法读取磁带卷标，将进入格式化流程")
                label = "Unknown"
            else:
                label = label_info.get('label')
                logger.info(f"✓ 磁带卷标: {label}")

                # 检查月份
                parsed = extract_label_year_month(label)
                if parsed:
                    year, month = parsed['year'], parsed['month']
                    logger.info(f"  年月: {year}年{month:02d}月")

                    current_month = datetime.now().month
                    month_diff = abs(month - current_month)
                    if month_diff > 6:
                        month_diff = 12 - month_diff

                    if month_diff > 1:
                        if manual_run:
                            logger.warning(f"⚠️ 卷标不符（磁带{month:02d}月，当前{current_month:02d}月），手动运行跳过检查")
                        else:
                            error_msg = f"卷标不符（磁带{month:02d}月，当前{current_month:02d}月）"
                            logger.error(f"✗ {error_msg}")
                            backup_task.error_message = error_msg
                            return False
                    logger.info(f"✓ 卷标符合要求")

            # 2. 检查LTFS格式和空盘（仅当卷标可读时）
            is_ltfs = False
            is_empty = False

            if label != "Unknown":
                logger.info("[2] 检查LTFS格式和空盘")

                self.tape_manager = TapeManager()
                await self.tape_manager.initialize()
                self.tape_handler.tape_manager = self.tape_manager

                is_ltfs, msg = await self.tape_handler.check_ltfs_format()

                # 挂载超时等瞬时失败不应直接判定为"非LTFS"（会触发破坏性格式化）：
                # 磁带已有可读卷标时，清理残留后重试一次 LTFS 检查再下结论
                if not is_ltfs and label != "Unknown":
                    logger.warning(
                        f"[LTFS检查] 首次检查未通过（{msg}），5 秒后重试一次"
                        f"（避免瞬时挂载慢导致误判为非LTFS而格式化）"
                    )
                    try:
                        await self.tape_handler.unmount_ltfs()
                    except Exception as cleanup_err:
                        logger.debug(f"[LTFS检查] 重试前清理挂载失败（忽略）: {cleanup_err}")
                    await asyncio.sleep(5)
                    is_ltfs_retry, msg_retry = await self.tape_handler.check_ltfs_format()
                    if is_ltfs_retry:
                        is_ltfs, msg = True, msg_retry
                        logger.info("✓ LTFS格式（重试成功）")
                    else:
                        logger.warning(f"[LTFS检查] 重试仍未通过（{msg_retry}）")

                if is_ltfs:
                    logger.info("✓ LTFS格式")
                    is_empty, count = await self.tape_handler.is_tape_empty()
                    logger.info(f"  用户文件: {count}, 空盘: {'是' if is_empty else '否'}")
                else:
                    logger.info(f"✗ 非LTFS格式: {msg}")

            # 3. 格式化检查
            logger.info("[3] 格式化检查")
            need_format = not (label != "Unknown" and is_ltfs and is_empty)

            # LTFS磁带有数据时：根据自动格式化开关决定是续写还是格式化
            # ENABLE_TAPE_FORMAT_BEFORE_FULL=False → 续写（不格式化）
            # ENABLE_TAPE_FORMAT_BEFORE_FULL=True  → 格式化（覆盖旧数据）
            if need_format and is_ltfs and not is_empty:
                if not self.settings.ENABLE_TAPE_FORMAT_BEFORE_FULL:
                    logger.info("✓ LTFS磁带有数据，自动格式化已关闭，将续写数据")
                    need_format = False
                else:
                    logger.warning("⚠️ LTFS磁带有数据，自动格式化已开启，将格式化覆盖旧数据")

            if need_format:
                if label == "Unknown":
                    reason = "无法读取卷标"
                elif not is_ltfs:
                    reason = "非LTFS"
                else:
                    reason = "磁带不为空"

                # 自动格式化关闭时具备绝对语义：任何需要格式化的场景一律中止任务，
                # 绝不执行破坏性格式化（覆盖"非LTFS/挂载失败"等绕过续写开关的路径）
                if not self.settings.ENABLE_TAPE_FORMAT_BEFORE_FULL:
                    error_msg = (
                        f"磁带需要格式化（{reason}），但自动格式化已关闭，任务中止。"
                        f"请手动格式化磁带（磁带管理页）或开启\"完整备份前自动格式化\"后重试"
                    )
                    logger.error(f"✗ {error_msg}")
                    backup_task.error_message = error_msg
                    await self._send_tape_error_notification(error_msg)
                    return False

                logger.warning(f"⚠️ 需要格式化: {reason}")
                logger.info("开始格式化...")

                # 格式化前准备
                from backup.tape_label_generator import generate_tape_label_and_serial

                new_label = await generate_tape_label_and_serial()
                logger.info(f"新卷标: {new_label.label}")
                logger.info(f"新序列号: {new_label.serial_number}")

                # 格式化
                success, format_msg = await self.tape_handler.format_as_ltfs(
                    volume_name=new_label.label,
                    serial=new_label.serial_number
                )
                if not success:
                    error_msg = f"格式化失败: {format_msg}"
                    logger.error(f"✗ {error_msg}")
                    backup_task.error_message = error_msg
                    await self._send_tape_error_notification(error_msg)
                    return False
                logger.info(f"✓ 格式化成功")

                # 格式化成功后，使用 psycopg 同步连接注册磁带到数据库（与添加磁带相同的方式）
                try:
                    from utils.db_connection_helper import get_psycopg_connection_from_url, set_autocommit
                    from config.settings import get_settings as _get_settings

                    _settings = _get_settings()
                    _db_url = _settings.DATABASE_URL
                    _conn, _is_psycopg3 = get_psycopg_connection_from_url(_db_url, prefer_psycopg3=True)
                    try:
                        set_autocommit(_conn, _is_psycopg3, autocommit=True)
                        with _conn.cursor() as cur:
                            cur.execute("SELECT 1 FROM tape_cartridges WHERE tape_id = %s", (new_label.label,))
                            _tape_exists = cur.fetchone() is not None

                            if not _tape_exists:
                                logger.info(f"数据库中不存在磁带 {new_label.label}，开始录入...")
                                _now = datetime.now()
                                _expiry_month = _now.month + 6
                                _expiry_year = _now.year
                                while _expiry_month > 12:
                                    _expiry_year += 1
                                    _expiry_month -= 12
                                _expiry_date = datetime(_expiry_year, _expiry_month, 1)

                                cur.execute(
                                    """
                                    INSERT INTO tape_cartridges
                                    (tape_id, label, status, media_type, generation, serial_number, location,
                                     capacity_bytes, used_bytes, retention_months, notes, manufactured_date, expiry_date, auto_erase, health_score)
                                    VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s)
                                    """,
                                    (
                                        new_label.label,
                                        new_label.label,
                                        'available',
                                        'LTO',
                                        9,
                                        new_label.serial_number,
                                        '',
                                        18 * 1024 ** 4,
                                        0,
                                        6,
                                        '备份任务格式化后自动注册',
                                        _now,
                                        _expiry_date,
                                        True,
                                        100
                                    )
                                )
                                logger.info(f"✓ 磁带已注册到数据库: {new_label.label}")
                            else:
                                logger.info(f"数据库中已存在磁带 {new_label.label}，跳过录入")

                        # 更新 tape_manager 缓存
                        if self.tape_manager:
                            from tape.tape_cartridge import TapeCartridge, TapeStatus
                            new_tape = TapeCartridge(
                                tape_id=new_label.label,
                                label=new_label.label,
                                status=TapeStatus.AVAILABLE,
                                generation=9,
                                serial_number=new_label.serial_number,
                                capacity_bytes=18 * 1024**4,
                                created_date=datetime.now()
                            )
                            self.tape_manager.tape_cartridges[new_label.label] = new_tape
                            self.tape_manager.current_tape = new_tape
                    finally:
                        _conn.close()
                except Exception as e:
                    logger.warning(f"磁带数据库注册异常: {e}")
            else:
                logger.info("✓ 无需格式化")

            # 4. 等待并挂载（使用重试机制）
            logger.info("[4] 等待20秒...")
            for i in range(20):
                await asyncio.sleep(1)

            logger.info("[5] 检查挂载状态")
            if mount_point.is_mount():
                logger.info("✓ 已挂载")
            else:
                # 使用重试机制挂载
                logger.info("[6] 开始挂载（重试3次，间隔30秒）...")
                mount_success, mount_msg = await self.tape_handler.mount_with_retry(backup_task=backup_task)
                if not mount_success:
                    error_msg = f"挂载失败: {mount_msg}"
                    logger.error(f"✗ {error_msg}")
                    backup_task.error_message = error_msg
                    await self._send_tape_error_notification(error_msg)
                    return False
                logger.info(f"✓ 挂载成功")

            logger.info("========== 磁带检查完成 ==========")

            # 1. 检查任务是否已执行过（在存活期内）- 仅自动执行时检查，手动运行跳过
            if not manual_run and scheduled_task:
                logger.info("========== 执行前检查：任务执行状态 ==========")
                template_id = getattr(backup_task, 'template_id', None)
                if not template_id and hasattr(scheduled_task, 'task_metadata') and scheduled_task.task_metadata:
                    template_id = scheduled_task.task_metadata.get('backup_task_id')
                
                if template_id:
                    logger.info(f"检查模板任务 {template_id} 的执行状态...")
                    from utils.scheduler.db_utils import is_opengauss, get_opengauss_connection
                    
                    if is_opengauss():
                        # 使用连接池
                        async with get_opengauss_connection() as conn:
                            # 检查是否有相同模板的任务在存活期内已成功执行
                            completed_task = await conn.fetchrow(
                                """
                                SELECT id, completed_at, status FROM backup_tasks
                                WHERE template_id = $1 AND status = $2::backuptaskstatus
                                ORDER BY completed_at DESC
                                LIMIT 1
                                """,
                                template_id, 'completed'
                            )
                            
                            if completed_task and completed_task['completed_at']:
                                logger.info(f"找到已完成的模板任务: {completed_task['id']}, 完成时间: {completed_task['completed_at']}")
                                # 这里可以根据存活期判断是否在存活期内，暂时跳过
            elif manual_run:
                logger.info("========== 手动运行模式，跳过任务执行状态检查 ==========")

            # 2. 检查任务是否正在执行（手动运行时跳过）
            if not manual_run:
                logger.info("========== 执行前检查：任务运行状态 ==========")
                from utils.scheduler.db_utils import is_opengauss, get_opengauss_connection
                
                if is_opengauss():
                    # 使用连接池
                    async with get_opengauss_connection() as conn:
                        running_task = await conn.fetchrow(
                            """
                            SELECT id, started_at FROM backup_tasks
                            WHERE id = $1 AND status = $2::backuptaskstatus
                            """,
                            task_id, 'running'
                        )
                        
                        if running_task:
                            logger.warning(f"任务 {task_id} 正在执行中，跳过本次执行")
                            # 记录操作日志（直接await确保完成，审计日志为毫秒级操作）
                            asyncio.create_task(log_system(
                                level=LogLevel.WARNING,
                                category=LogCategory.BACKUP,
                                message=f"任务 {task_id} 正在执行中，跳过本次执行",
                                module="backup.backup_engine",
                                function="execute_backup_task",
                                task_id=task_id
                            ))
                            return False
                        else:
                            logger.info(f"任务 {task_id} 未在运行，可以执行")
            else:
                logger.info("========== 手动运行模式，跳过任务运行状态检查 ==========")

            # 3. 检查磁带卷标是否当月（仅当备份目标为磁带时，手动运行时跳过）
            if not manual_run:
                logger.info("========== 执行前检查：磁带卷标当月验证 ==========")
                if self.tape_manager:
                    try:
                        tape_ops = self.tape_manager.tape_operations
                        if tape_ops and hasattr(tape_ops, '_read_tape_label'):
                            logger.info("尝试读取当前驱动器中的磁带卷标...")
                            metadata = await tape_ops._read_tape_label()
                            
                            if metadata and metadata.get('tape_id'):
                                label_text = metadata.get('label') or metadata.get('tape_id')
                                tape_id = metadata.get('tape_id')
                                logger.info(f"读取到磁带卷标: {label_text}")

                                current_time = now()
                                current_year = current_time.year
                                current_month = current_time.month

                                label_info = extract_label_year_month(label_text)

                                if label_info:
                                    label_year = label_info['year']
                                    label_month = label_info['month']

                                    # 情况4：非法月份（< 1 或 > 12）- 只记录错误，不影响后续执行
                                    if label_month < 1 or label_month > 12:
                                        error_msg = f"当前磁带 {tape_id} 卷标解析到非法月份 {label_month}，跳过当月验证，继续执行"
                                        logger.error(error_msg)
                                        # 不抛出异常，继续执行
                                    # 情况1：月份差异超过1个月 - 发送钉钉消息，阻塞等待，每6分钟检测一次
                                    else:
                                        # 计算月份差异（允许前后1个月）
                                        month_diff = abs(label_month - current_month)
                                        if month_diff > 6:
                                            month_diff = 12 - month_diff

                                        if month_diff > 1:
                                            error_msg = f"当前磁带 {tape_id} 月份不符（卷标显示{label_month:02d}月，当前{current_month:02d}月，差异{month_diff}个月），请更换磁带后重试"
                                            logger.error(error_msg)

                                            # 发送钉钉通知
                                            if self.dingtalk_notifier:
                                                try:
                                                    await self.dingtalk_notifier.send_tape_notification(
                                                        tape_id=tape_id,
                                                        action="error",
                                                        details={
                                                            "error": error_msg,
                                                            "task_name": backup_task.task_name,
                                                            "task_id": backup_task.id,
                                                            "current_month": f"{current_year}年{current_month:02d}月",
                                                            "tape_month": f"{label_year}年{label_month:02d}月",
                                                            "message": f"磁带月份差异{month_diff}个月（允许前后1个月），请更换合适的磁带后重试。系统将每6分钟自动检测一次。"
                                                        }
                                                    )
                                                    logger.info("已发送钉钉通知：磁带月份不符")
                                                except Exception as notify_error:
                                                    logger.warning(f"发送钉钉通知失败: {str(notify_error)}")

                                            # 阻塞等待，每6分钟检测一次卷标
                                            check_interval = 360  # 6分钟 = 360秒
                                            check_count = 0
                                            logger.info(f"开始每{check_interval}秒（6分钟）检测一次磁带卷标，直到检测到可用磁带...")

                                            while True:
                                                # 等待6分钟
                                                await asyncio.sleep(check_interval)
                                                check_count += 1

                                                # 检查任务是否被取消
                                                if backup_task.status == BackupTaskStatus.CANCELLED:
                                                    logger.warning("备份任务已被取消，停止等待磁带")
                                                    raise ValueError("备份任务已被取消")

                                                logger.info(f"第 {check_count} 次检测磁带卷标（每6分钟检测一次）...")

                                                try:
                                                    # 重新读取磁带卷标
                                                    new_metadata = await tape_ops._read_tape_label()

                                                    if new_metadata and new_metadata.get('tape_id'):
                                                        new_label_text = new_metadata.get('label') or new_metadata.get('tape_id')
                                                        new_tape_id = new_metadata.get('tape_id')
                                                        logger.info(f"检测到磁带卷标: {new_label_text}")

                                                        # 重新获取当前时间（可能已经跨月）
                                                        current_time = now()
                                                        current_year = current_time.year
                                                        current_month = current_time.month

                                                        new_label_info = extract_label_year_month(new_label_text)

                                                        if new_label_info:
                                                            new_label_year = new_label_info['year']
                                                            new_label_month = new_label_info['month']

                                                            # 计算月份差异（允许前后1个月）
                                                            new_month_diff = abs(new_label_month - current_month)
                                                            if new_month_diff > 6:
                                                                new_month_diff = 12 - new_month_diff

                                                            # 检查是否在允许范围内（差异<=1）
                                                            if new_month_diff <= 1:
                                                                logger.info(f"✅ 检测到可用磁带 {new_tape_id}（{new_label_year}年{new_label_month:02d}月，差异{new_month_diff}个月），继续执行备份任务")
                                                                # 更新 tape_id，继续执行
                                                                tape_id = new_tape_id
                                                                break
                                                            else:
                                                                logger.warning(
                                                                    f"当前磁带 {new_tape_id} 月份差异过大（卷标{new_label_month:02d}月，当前{current_month:02d}月，差异{new_month_diff}个月），"
                                                                    f"继续等待...（已等待 {check_count * 6} 分钟）"
                                                                )
                                                        else:
                                                            logger.warning(f"无法解析磁带 {new_tape_id} 的年月信息，继续等待...")
                                                    else:
                                                        logger.warning("无法读取磁带卷标，继续等待...")
                                                except Exception as check_error:
                                                    logger.warning(f"检测磁带卷标时出错: {str(check_error)}，继续等待...")

                                            logger.info(f"磁带卷标验证通过，继续执行备份任务")
                                        # 年份不匹配但月份在允许范围内 - 允许通过
                                        elif label_year != current_year:
                                            logger.info(
                                                f"卷标年份 {label_year} 与当前年份 {current_year} 不一致，但月份差异{month_diff}个月在允许范围内，允许通过"
                                            )
                                        else:
                                            logger.info(f"磁带 {tape_id} 卷标月份差异{month_diff}个月，验证通过")
                                else:
                                    # 情况3：无法解析年月信息 - 只记录错误，不影响后续执行
                                    error_msg = f"当前磁带 {tape_id} 卷标无法解析出年月信息，跳过当月验证，继续执行"
                                    logger.error(error_msg)
                                    # 不抛出异常，继续执行
                            else:
                                logger.warning("无法读取磁带卷标，跳过当月验证")

                    except ValueError as tape_check_error:
                        # 如果是 ValueError（任务取消等），重新抛出
                        raise
                    except Exception as tape_check_error:
                        logger.warning(f"检查磁带卷标失败: {str(tape_check_error)}")
                        # 不阻止执行，记录警告即可
            else:
                logger.info("========== 手动运行模式，跳过磁带卷标当月验证 ==========")

            # ========== Linux系统：检查磁带是否可写入 ==========
            import platform
            if platform.system() == 'Linux' and self.tape_manager:
                logger.info("========== Linux磁带状态检查 ==========")

                # ========== 获取磁带操作锁（防止并发操作） ==========
                from utils.linux_tape import acquire_tape_operation_lock, release_tape_operation_lock, is_tape_operation_locked, get_tape_lock_owner
                lock_owner = f"backup_task_{backup_task.id}"
                logger.info(f"尝试获取磁带操作锁... (owner={lock_owner})")

                if is_tape_operation_locked():
                    current_owner = get_tape_lock_owner()
                    error_msg = f"磁带操作锁被占用，当前持有者: {current_owner}，请等待或手动解锁"
                    logger.error(error_msg)
                    raise ValueError(error_msg)

                if not acquire_tape_operation_lock(owner=lock_owner, timeout=10.0):
                    error_msg = "获取磁带操作锁超时，可能有其他任务正在操作磁带"
                    logger.error(error_msg)
                    raise ValueError(error_msg)

                tape_lock_acquired = True
                logger.info(f"磁带操作锁获取成功 (owner={lock_owner})")

                try:
                    # 注意：磁带准备和挂载逻辑已移到 _perform_backup 中
                    # 这里只获取磁带锁，确保在 _perform_backup 执行时不会发生并发冲突
                    logger.info("磁带操作锁已获取，将在 _perform_backup 中进行磁带准备和挂载")

                except ValueError:
                    # 释放磁带锁
                    if tape_lock_acquired:
                        release_tape_operation_lock()
                        tape_lock_acquired = False
                    raise
                except Exception as tape_check_error:
                    logger.error(f"磁带操作锁获取异常: {str(tape_check_error)}", exc_info=True)
                    # 释放磁带锁
                    if tape_lock_acquired:
                        release_tape_operation_lock()
                        tape_lock_acquired = False
                    raise ValueError(f"磁带操作锁获取失败: {str(tape_check_error)}")
                finally:
                    # 注意：这里不释放锁，锁将在 _perform_backup 中释放
                    # 这样可以确保在 _perform_backup 执行期间不会有其他任务操作磁带
                    logger.info(f"磁带锁将传递给 _perform_backup (owner={lock_owner})")

            # 更新任务状态（同时更新 source_paths 和 tape_id，以便任务卡片正确显示）
            # 关键：使用原子操作更新状态，确保只有一个任务能成功从 PENDING 更新为 RUNNING
            logger.info("========== 更新任务状态为运行中（原子操作） ==========")
            # 注意：此时 tape_id 还未设置，将在 _perform_backup 中设置后再次更新
            
            # 使用原子操作：只有当任务状态为 PENDING 时才能更新为 RUNNING
            # 如果任务已经被其他进程更新，这个操作会失败（影响行数为0）
            from utils.scheduler.db_utils import get_opengauss_connection

            async with get_opengauss_connection() as conn:
                    # 原子更新：只有状态为 PENDING 的任务才能更新为 RUNNING
                    result = await conn.execute(
                        """
                        UPDATE backup_tasks
                        SET status = $1::backuptaskstatus, started_at = $2, updated_at = $3
                        WHERE id = $4 AND status = $5::backuptaskstatus
                        """,
                        'running', task_start_time, task_start_time, task_id, 'pending'
                    )
                    
                    # 显式提交事务
                    actual_conn = conn._conn if hasattr(conn, '_conn') else conn
                    try:
                        await actual_conn.commit()
                    except Exception as commit_err:
                        logger.info(f"提交任务状态更新事务失败（可能已自动提交）: {commit_err}")
                        try:
                            await actual_conn.rollback()
                        except:
                            pass
                    
                    if result == 0:
                        # 更新失败，说明任务已经被其他进程更新或状态不是 PENDING
                        logger.warning(f"任务 {task_id} 状态更新失败：任务可能已被其他进程更新或状态不是 PENDING")
                        # 验证当前状态
                        verify_row = await conn.fetchrow(
                            "SELECT id, status FROM backup_tasks WHERE id = $1",
                            task_id
                        )
                        if verify_row:
                            current_status = verify_row['status']
                            if current_status == 'running':
                                logger.warning(f"任务 {task_id} 已被其他进程更新为 RUNNING，跳过本次执行")
                            else:
                                logger.warning(f"任务 {task_id} 当前状态为 {current_status}，不是 PENDING，无法更新为 RUNNING")
                        else:
                            logger.error(f"任务 {task_id} 不存在")
                        
                        # 使用后台任务记录日志
                        asyncio.create_task(log_system(
                            level=LogLevel.WARNING,
                            category=LogCategory.BACKUP,
                            message=f"任务 {task_id} 状态更新失败，可能已被其他进程执行",
                            module="backup.backup_engine",
                            function="execute_backup_task",
                            task_id=task_id
                        ))
                        return False
                    else:
                        logger.info(f"任务 {task_id} 状态已原子更新为 RUNNING（影响行数: {result}）")
                        backup_task.status = BackupTaskStatus.RUNNING
                        backup_task.started_at = task_start_time
            # 任务状态已原子更新为 RUNNING
            logger.info(f"任务 {task_id} 状态已原子更新为 RUNNING")
            
            # 记录操作日志（直接await确保完成，审计日志为毫秒级操作）
            await log_operation(
                operation_type=OperationType.BACKUP_START,
                resource_type="backup",
                resource_id=str(task_id),
                resource_name=task_name,
                operation_name="开始备份任务",
                operation_description=f"开始执行备份任务: {task_name}",
                category="backup",
                success=True
            )

            # 初始化任务状态
            await self.backup_db.update_task_stage_with_description(
                backup_task,
                "scan",
                "[准备开始] 正在初始化备份任务..."
            )

            # 发送开始通知（检查钉钉通知配置中的通知事件）
            if self.dingtalk_notifier:
                try:
                    # 检查钉钉通知配置中的通知事件
                    notification_events = await self._get_notification_events()
                    if notification_events.get("notify_backup_started", True):
                        logger.info("发送备份开始钉钉通知...")
                        await self.dingtalk_notifier.send_backup_notification(
                            backup_task.task_name,
                            "started"
                        )
                        logger.info("备份开始钉钉通知发送成功")
                    else:
                        logger.debug("通知事件配置中备份开始通知已禁用，跳过发送")
                except Exception as notify_error:
                    logger.warning(f"发送备份开始钉钉通知失败: {str(notify_error)}")

            # 执行备份流程
            logger.info("========== 开始执行备份流程 ==========")
            success = await self._perform_backup(backup_task, scheduled_task=scheduled_task)

            # 更新任务完成状态
            task_end_time = now()
            backup_task.completed_at = task_end_time
            duration_seconds = (task_end_time - task_start_time).total_seconds()
            duration_ms = int(duration_seconds * 1000)

            # 写带收尾统计（隔离清单）一并落盘，任务完成后卡片/排查仍可见
            _final_extra = {}
            try:
                _fin = getattr(backup_task, 'final_retry_stats', None) or {}
                if _fin:
                    _final_extra = {
                        'final_retry_attempted': int(_fin.get('attempted') or 0),
                        'final_retry_written': int(_fin.get('written') or 0),
                        'quarantined_files': len(_fin.get('quarantined') or []),
                        'quarantined_bytes': int(_fin.get('quarantined_bytes') or 0),
                    }
                # 任务期间 SSD 剩余的最小值（FinalDirMonitor 每 5 秒采样）：
                # 反映本任务对压缩盘的占用峰值，供完成卡片统计
                if self.final_dir_monitor is not None:
                    _min_free = getattr(self.final_dir_monitor, '_min_free_bytes', None)
                    if _min_free is not None:
                        _final_extra['ssd_min_free_gb'] = round(
                            int(_min_free) / (1024 ** 3), 1)
            except Exception:
                _final_extra = {}

            # 错误日志汇总（压缩/写带/扫描/任务级）：任务完成后“错误日志”弹窗读取。
            # 压缩 worker 与 FinalDirMonitor 的明细只在内存，必须在这里落盘。
            try:
                _err_logs = list(getattr(backup_task, 'error_logs_cache', []) or [])
                if self.final_dir_monitor:
                    _err_logs += list(
                        getattr(self.final_dir_monitor, 'error_logs', []) or [])
                _scan_stats = getattr(backup_task, '_scan_stats', None) or {}
                _ec = int(_scan_stats.get('error_count', 0) or 0)
                _ed = int(_scan_stats.get('error_dirs', 0) or 0)
                if _ec or _ed:
                    _err_logs.append({
                        'category': 'scan',
                        'time': format_datetime(now()),
                        'message': (f"扫描异常：{_ec} 个文件/条目读取失败，"
                                    f"{_ed} 个目录无法访问"),
                        'detail': '',
                    })
                if getattr(backup_task, 'error_message', None):
                    _err_logs.append({
                        'category': 'task',
                        'time': format_datetime(now()),
                        'message': str(backup_task.error_message)[:500],
                        'detail': '',
                    })
                # 压缩比异常等条目带完整文件清单（'files'）：写独立导出文件，
                # result_summary 只保留摘要，避免 jsonb 膨胀。
                try:
                    _export_dir = Path('logs') / 'task_errors'
                    _export_dir.mkdir(parents=True, exist_ok=True)
                    _export_path = _export_dir / f"task_{backup_task.id}.txt"
                    _sections = []
                    for _it in _err_logs:
                        if isinstance(_it, dict) and _it.get('files'):
                            _sections.append((_it.get('message') or '', _it.pop('files')))
                    if _sections:
                        with open(_export_path, 'w', encoding='utf-8') as _ef:
                            _ef.write(f"任务 {backup_task.id} 错误文件清单\n")
                            _ef.write(f"生成时间: {format_datetime(now())}\n")
                            for _msg, _files in _sections:
                                _ef.write(f"\n# {_msg}\n")
                                for _p in _files:
                                    _ef.write(f"{_p}\n")
                        for _it in _err_logs:
                            if (isinstance(_it, dict)
                                    and _it.get('category') == 'compress'):
                                _it['export_available'] = True
                        _final_extra['error_export_file'] = str(_export_path)
                except Exception as _exp_err:
                    logger.debug(f"[备份引擎] 写错误清单导出文件失败: {_exp_err}")
                _final_extra['error_logs'] = _err_logs[:200]
            except Exception as _errlog_err:
                logger.debug(f"[备份引擎] 汇总错误日志失败: {_errlog_err}")

            # 写“等待压缩”统计到 result_summary（任务完成后卡片仍可显示）
            try:
                _starve_seconds = float(
                    getattr(self.final_dir_monitor, '_starvation_seconds', 0.0) or 0.0)
                await self.backup_db.save_task_starvation(
                    backup_task.id, _starve_seconds, _final_extra or None)
            except Exception as _starve_err:
                logger.debug(f"[备份引擎] 保存等待压缩统计失败: {_starve_err}")

            if success:
                # 安全获取处理统计信息
                processed_files = getattr(backup_task, 'processed_files', 0)
                processed_bytes = getattr(backup_task, 'processed_bytes', 0)
                
                logger.info("========== 备份任务执行成功 ==========")
                logger.info(f"处理文件数: {processed_files}")
                logger.info(f"处理字节数: {format_bytes(processed_bytes)}")
                logger.info(f"执行耗时: {duration_seconds:.2f} 秒")
                logger.info(f"完成时间: {format_datetime(task_end_time)}")
                # 写带忙闲统计（记录到日志，便于事后核对写带效率）
                # 完成后卡片需要的**平均值**落盘：写带平均速度 + 压缩开始时间
                try:
                    _th_stats = self.tape_handler
                    _busy = float(getattr(_th_stats, '_tape_cum_seconds', 0) or 0)
                    _cum_bytes = int(getattr(_th_stats, '_tape_cum_bytes', 0) or 0)
                    _first = getattr(_th_stats, '_tape_first_write_start', None)
                    _avg_mbps = 0.0
                    if _first and _cum_bytes > 0:
                        _wall = max(0.001, time.time() - float(_first))
                        _idle = max(0.0, _wall - _busy)
                        # 平均速度按实际写带耗时（不含 final 空等），与卡片口径一致
                        _avg_mbps = (_cum_bytes / _busy / (1024 * 1024)
                                     if _busy > 0 else 0.0)
                        logger.info(
                            f"写带统计: 实际写入 {_busy:.0f} 秒，空闲等待 {_idle:.0f} 秒，"
                            f"写入效率 {_busy / _wall * 100:.1f}%，"
                            f"平均速度 {_avg_mbps:.1f} MB/s"
                        )
                    _cw = getattr(self, '_current_compression_worker', None)
                    # 优先用 _perform_backup finally 里留的快照（worker 引用那时已置空）
                    _comp_started = float(
                        getattr(backup_task, 'compression_started_at_snapshot', 0) or 0)
                    if not _comp_started and _cw:
                        _comp_started = float(getattr(_cw, 'compression_started_at', 0) or 0)
                    _starve_seconds = float(
                        getattr(self.final_dir_monitor, '_starvation_seconds', 0.0) or 0.0)
                    await self.backup_db.save_task_starvation(
                        backup_task.id, _starve_seconds,
                        {"tape_write_avg_speed_mbps": round(_avg_mbps, 1),
                         "compression_started_at": _comp_started,
                         **_final_extra})
                except Exception:
                    pass
                
                # 注意：不再在这里标记任务状态为 COMPLETED
                # 只有文件压缩任务可以标记任务集状态为完成并发钉钉
                # 压缩任务会在收不到队列且任务集完成标记为 completed 时自动调用 _finalize_backup_set_and_notify
                # 这里只更新操作阶段描述
                await self.backup_db.update_task_stage_with_description(
                    backup_task,
                    "finalize",
                    f"[备份完成] 处理了 {processed_files} 个文件，总大小 {format_bytes(processed_bytes)}"
                )

                # 记录操作日志（直接await确保完成，审计日志为毫秒级操作）
                await log_operation(
                    operation_type=OperationType.BACKUP_COMPLETE,
                    resource_type="backup",
                    resource_id=str(task_id),
                    resource_name=task_name,
                    operation_name="备份任务完成",
                    operation_description=f"备份任务执行成功: {task_name}",
                    category="backup",
                    success=True,
                    result_message=f"处理 {processed_files} 个文件，总大小 {format_bytes(processed_bytes)}",
                    duration_ms=duration_ms
                )
                asyncio.create_task(log_system(
                    level=LogLevel.INFO,
                    category=LogCategory.BACKUP,
                    message=f"备份任务执行成功: {task_name}",
                    module="backup.backup_engine",
                    function="execute_backup_task",
                    task_id=task_id,
                    duration_ms=duration_ms,
                    details={
                        "processed_files": processed_files,
                        "processed_bytes": processed_bytes,
                        "duration_seconds": duration_seconds,
                        "backup_set_id": getattr(backup_task, 'backup_set_id', None),
                        "tape_id": getattr(backup_task, 'tape_id', None)
                    }
                ))
                
                # 注意：不再在这里发送钉钉通知
                # 只有文件压缩任务可以标记任务集状态为完成并发钉钉
                # 压缩任务会在收不到队列且任务集完成标记为 completed 时自动调用 _finalize_backup_set_and_notify
            else:
                error_msg = getattr(backup_task, 'error_message', '未知错误')
                logger.error("========== 备份任务执行失败 ==========")
                logger.error(f"错误信息: {error_msg}")
                logger.error(f"执行耗时: {duration_seconds:.2f} 秒")
                logger.error(f"完成时间: {format_datetime(task_end_time)}")
                
                # 更新失败状态
                await self.backup_db.update_task_stage_with_description(
                    backup_task,
                    "failed",
                    f"[任务失败] {error_msg[:100]}{'...' if len(error_msg) > 100 else ''}"
                )
                await self.backup_db.update_task_status(backup_task, BackupTaskStatus.FAILED)

                # 记录操作日志（直接await确保完成，审计日志为毫秒级操作）
                await log_operation(
                    operation_type=OperationType.BACKUP_COMPLETE,
                    resource_type="backup",
                    resource_id=str(task_id),
                    resource_name=task_name,
                    operation_name="备份任务失败",
                    operation_description=f"备份任务执行失败: {task_name}",
                    category="backup",
                    success=False,
                    error_message=error_msg,
                    duration_ms=duration_ms
                )
                asyncio.create_task(log_system(
                    level=LogLevel.ERROR,
                    category=LogCategory.BACKUP,
                    message=f"备份任务执行失败: {task_name}",
                    module="backup.backup_engine",
                    function="execute_backup_task",
                    task_id=task_id,
                    duration_ms=duration_ms,
                    details={
                        "error_message": error_msg,
                        "duration_seconds": duration_seconds,
                        "processed_files": getattr(backup_task, 'processed_files', 0),
                        "processed_bytes": getattr(backup_task, 'processed_bytes', 0)
                    }
                ))
                
                # 发送失败通知（检查钉钉通知配置中的通知事件）
                if self.dingtalk_notifier:
                    try:
                        # 检查钉钉通知配置中的通知事件
                        notification_events = await self._get_notification_events()
                        if notification_events.get("notify_backup_failed", True):
                            logger.info("发送备份失败钉钉通知...")
                            await self.dingtalk_notifier.send_backup_notification(
                                backup_task.task_name,
                                "failed",
                                {'error': error_msg}
                            )
                            logger.info("备份失败钉钉通知发送成功")
                        else:
                            logger.debug("通知事件配置中备份失败通知已禁用，跳过发送")
                    except Exception as notify_error:
                        logger.warning(f"发送备份失败钉钉通知失败: {str(notify_error)}")

            # 保存任务结果 - 使用原生 openGauss SQL
            # 注意：只有文件压缩任务可以标记任务状态为 COMPLETED
            # 这里只处理失败情况，成功情况由压缩任务处理
            if not success:
                from utils.scheduler.db_utils import get_opengauss_connection

                async with get_opengauss_connection() as conn:
                    await conn.execute(
                        """
                        UPDATE backup_tasks
                        SET status = $1::backuptaskstatus,
                            completed_at = $2,
                            error_message = $3,
                            updated_at = $4
                        WHERE id = $5
                        """,
                        BackupTaskStatus.FAILED.value,
                        backup_task.completed_at,
                        getattr(backup_task, 'error_message', None),  # 安全获取 error_message
                        datetime.now(),
                        backup_task.id
                    )

            logger.info(f"========== 备份任务执行完成 ==========")
            logger.info(f"任务名称: {task_name}")
            logger.info(f"任务ID: {task_id}")
            logger.info(f"执行结果: {'成功' if success else '失败'}")
            logger.info(f"总耗时: {duration_seconds:.2f} 秒")

            # 任务结束（无论成功/失败）：卸载 LTFS 并 offline。
            # 带子带着"加载状态+索引分区末尾"停着时，一旦掉电/异常重启，
            # 可能出现卷标签读取失败（2026-10-22 已发生）。offline 后由下次
            # 挂载时的 mt load / LTFS 自身 LOAD 重新加载。
            await self._offline_tape_after_task(
                "备份任务完成" if success else "备份任务失败")

            return success

        except KeyboardInterrupt:
            # 用户按 Ctrl+C 中止任务
            logger.warning("========== 用户中止备份任务执行（Ctrl+C） ==========")
            logger.warning(f"任务名称: {task_name}")
            logger.warning(f"任务ID: {task_id}")
            
            # 更新任务状态为取消
            if backup_task:
                try:
                    backup_task.error_message = "用户中止任务（Ctrl+C）"
                    await self.backup_db.update_task_status(backup_task, BackupTaskStatus.CANCELLED)
                    await self.backup_db.update_scan_progress(backup_task, 
                                                            backup_task.processed_files if hasattr(backup_task, 'processed_files') else 0,
                                                            backup_task.total_files if hasattr(backup_task, 'total_files') else 0, 
                                                            "[已取消]")
                    logger.info("任务状态已更新为取消")
                except Exception as update_error:
                    logger.error(f"更新任务状态失败: {str(update_error)}")
            
            # 重新抛出 KeyboardInterrupt，让上层处理
            raise
            
        except asyncio.CancelledError:
            # 任务被取消
            logger.warning("========== 备份任务执行被取消 ==========")
            logger.warning(f"任务名称: {task_name}")
            logger.warning(f"任务ID: {task_id}")
            
            # 更新任务状态为取消
            if backup_task:
                try:
                    backup_task.error_message = "任务被取消"
                    await self.backup_db.update_task_status(backup_task, BackupTaskStatus.CANCELLED)
                    await self.backup_db.update_scan_progress(backup_task, 
                                                            backup_task.processed_files if hasattr(backup_task, 'processed_files') else 0,
                                                            backup_task.total_files if hasattr(backup_task, 'total_files') else 0, 
                                                            "[已取消]")
                    logger.info("任务状态已更新为取消")
                except Exception as update_error:
                    logger.error(f"更新任务状态失败: {str(update_error)}")
            
            # 重新抛出 CancelledError，让上层处理
            raise

        except Exception as e:
            error_msg = str(e)
            error_trace = traceback.format_exc()
            
            logger.error("========== 备份任务执行异常 ==========")
            logger.error(f"任务名称: {task_name}")
            logger.error(f"任务ID: {task_id}")
            logger.error(f"异常信息: {error_msg}")
            logger.error(f"异常堆栈:\n{error_trace}")
            
            # 系统日志：异常路径必须同步落库（原为 create_task，进程退出会丢）
            try:
                await log_system(
                    level=LogLevel.ERROR,
                    category=LogCategory.BACKUP,
                    message=f"备份任务执行异常: {task_name}",
                    module="backup.backup_engine",
                    function="execute_backup_task",
                    task_id=task_id,
                    exception_type=type(e).__name__,
                    stack_trace=error_trace,
                    details={
                        "error_message": error_msg,
                        "task_name": task_name
                    }
                )
            except Exception as _syslog_err:
                logger.warning(f"记录异常系统日志失败: {_syslog_err}")

            # 操作日志：与成功/失败分支保持一致（原异常路径缺失）
            try:
                await log_operation(
                    operation_type=OperationType.BACKUP_COMPLETE,
                    resource_type="backup",
                    resource_id=str(task_id),
                    resource_name=task_name,
                    operation_name="备份任务异常",
                    operation_description=f"备份任务执行异常: {task_name}",
                    category="backup",
                    success=False,
                    error_message=error_msg[:500],
                    duration_ms=int((now() - task_start_time).total_seconds() * 1000),
                )
            except Exception as _oplog_err:
                logger.warning(f"记录异常操作日志失败: {_oplog_err}")

            # 任务统计 + 错误清单落库：
            # 原异常路径直接 return，result_summary 只有扫描统计、卡片错误日志为空
            try:
                _err_logs = list(getattr(backup_task, 'error_logs_cache', []) or [])
                if self.final_dir_monitor:
                    _err_logs += list(
                        getattr(self.final_dir_monitor, 'error_logs', []) or [])
                _err_logs.append({
                    'category': 'task',
                    'time': format_datetime(now()),
                    'message': error_msg[:500],
                    'detail': error_trace[:2000],
                })
                _starve_seconds = float(
                    getattr(self.final_dir_monitor, '_starvation_seconds', 0.0) or 0.0)
                await self.backup_db.save_task_starvation(
                    backup_task.id, _starve_seconds, {'error_logs': _err_logs[:200]})
            except Exception as _stat_err:
                logger.warning(f"异常路径保存任务统计失败: {_stat_err}")

            if backup_task:
                # 使用 setattr 设置 error_message，因为 BackupTask 可能是数据类或字典
                try:
                    if hasattr(backup_task, 'error_message'):
                        backup_task.error_message = str(e)
                    else:
                        # 如果 BackupTask 是字典或没有 error_message 属性，使用数据库更新
                        from utils.scheduler.db_utils import is_opengauss, get_opengauss_connection
                        if is_opengauss():
                            # 使用连接池
                            async with get_opengauss_connection() as conn:
                                await conn.execute(
                                    """
                                    UPDATE backup_tasks
                                    SET error_message = $1, updated_at = $2
                                    WHERE id = $3
                                    """,
                                    str(e),
                                    datetime.now(),
                                    backup_task.id
                                )
                except Exception as update_error:
                    logger.warning(f"更新错误信息失败: {str(update_error)}")
                await self.backup_db.update_task_status(backup_task, BackupTaskStatus.FAILED)
            return False
        finally:
            # 异常/取消路径同样要 offline（正常路径在 return 前已做；
            # 此前 except 分支没有 offline，带子会保持挂载到停机）
            try:
                await self._offline_tape_after_task("备份任务异常/取消收尾")
            except Exception as _off_err:
                logger.warning(f"[备份引擎] 异常路径 offline 失败（非致命）: {_off_err}")
            self._current_task = None
            # 同步清理 task_manager 的当前任务
            self.task_manager._current_task = None

    async def _perform_backup(self, backup_task: BackupTask, scheduled_task=None) -> bool:
        """执行备份流程（流式处理：扫描和压缩循环执行）
        
        Args:
            backup_task: 备份任务对象
            scheduled_task: 计划任务对象（可选，用于获取排除规则等配置）
        """
        scan_progress_task = None  # 后台扫描任务

        # 文件组预取器（预分组）：可能在压缩启动前 +30 秒提前启动
        file_group_prefetcher = None

        def _create_and_start_prefetcher():
            """创建并启动预取器（幂等：已创建则直接返回）"""
            nonlocal file_group_prefetcher
            if file_group_prefetcher is not None:
                return file_group_prefetcher
            from utils.scheduler.db_utils import is_opengauss
            if not is_opengauss():
                return None
            from backup.file_group_prefetcher import FileGroupPrefetcher
            parallel_batches = getattr(self.settings, 'COMPRESSION_PARALLEL_BATCHES', 3)
            logger.info(
                f"[备份引擎] openGauss模式：创建文件组预取器，实现压缩和搜索并行执行"
                f"（并行批次数: {parallel_batches}，队列大小: {parallel_batches + 1}）"
            )
            fp = FileGroupPrefetcher(
                backup_db=self.backup_db,
                backup_set=backup_set,
                backup_task=backup_task,
                parallel_batches=parallel_batches,
                in_memory_store=in_memory_store,
            )
            # 注入 compressor 引用：大小文件组调度需要读取守卫状态 _guard_active
            try:
                fp.compressor = self.compressor
            except Exception:
                pass
            # 注入压缩启动时间 provider：紧急判定（final≤1 / 在途无 L）在压缩
            # 启动 60 秒后才生效。注意该时间戳在 CompressionWorker 上，
            # 不在 Compressor 上——prefetcher 必须通过 provider 读取，
            # 直接 getattr(compressor, ...) 会恒为 None 导致紧急永不触发。
            try:
                fp.compression_started_provider = (
                    lambda: getattr(compression_worker, 'compression_started_at', None))
            except Exception:
                pass
            # 注入写带喂不饱信号：final 待写文件数 <=1 → 立即放行保护区组（无防抖）
            try:
                _final_set_id = backup_set.set_id if backup_set else None
                fp.final_pending_provider = (
                    lambda: self.final_dir_monitor.count_final_files(_final_set_id))
            except Exception:
                pass
            # 兼容旧布尔信号（保留，回退用）
            try:
                fp.starved_provider = (
                    lambda: bool(getattr(self.final_dir_monitor, '_starve_start', None)))
            except Exception:
                pass
            fp.start()
            self._current_prefetcher = fp
            file_group_prefetcher = fp
            logger.info("[备份引擎] 文件组预取器已启动，开始预取文件组...")
            return fp

        try:
            # 初始化扫描进度
            if backup_task:
                backup_task.progress_percent = 0.0
                await self.backup_db.update_scan_progress(backup_task, 0, 0)
            
            # 获取排除规则：优先从计划任务获取，否则从备份任务获取
            exclude_patterns = []
            if scheduled_task and hasattr(scheduled_task, 'action_config') and scheduled_task.action_config:
                exclude_patterns = scheduled_task.action_config.get('exclude_patterns', [])
                logger.info(f"从计划任务获取排除规则: {len(exclude_patterns)} 个模式")
            elif backup_task and hasattr(backup_task, 'exclude_patterns') and backup_task.exclude_patterns:
                exclude_patterns = backup_task.exclude_patterns if isinstance(backup_task.exclude_patterns, list) else []
                logger.info(f"从备份任务获取排除规则: {len(exclude_patterns)} 个模式")
            
            if exclude_patterns:
                logger.info(f"排除规则: {exclude_patterns}")
            
            # 初始化备份任务的统计信息
            backup_task.processed_files = 0
            backup_task.processed_bytes = 0  # 原始文件的总大小（未压缩）
            backup_task.total_files = 0  # total_files: 总文件数（由后台扫描任务更新）
            backup_task.total_bytes = 0  # total_bytes: 总字节数（由后台扫描任务更新）
            
            # 清零压缩进度信息（新任务开始时）
            backup_task.current_compression_progress = None
            
            # 1. 检查磁带设备是否可用
            logger.info("检查磁带设备...")
            tape_drive = self.settings.TAPE_DEVICE_PATH or '/dev/nst0'
            if not os.path.exists(tape_drive):
                raise RuntimeError(f"磁带设备不存在: {tape_drive}，请检查配置")
            
            logger.info(f"磁带设备可用: {tape_drive}")
            
            # 2. 获取或创建磁带信息（简化处理）
            tape_id = "TAPE001"  # 默认磁带ID，可以从数据库获取或自动生成
            if self.tape_manager:
                try:
                    # 尝试从数据库获取当前磁带或创建新记录
                    current_tape = await self.tape_handler.get_current_drive_tape()
                    if current_tape:
                        tape_id = current_tape.tape_id
                    else:
                        # 从数据库获取可用磁带
                        available_tape = await self.tape_manager.get_available_tape()
                        if available_tape:
                            tape_id = available_tape.tape_id
                except RuntimeError as e:
                    # 如果是因为磁带不在数据库中而抛出的异常，发送钉钉通知并停止任务
                    error_msg = str(e)
                    logger.error(f"磁带检查失败: {error_msg}")
                    
                    # 发送钉钉通知
                    if self.dingtalk_notifier:
                        try:
                            # 尝试从错误信息中提取磁带ID
                            tape_id_from_error = "未知"
                            if "磁带" in error_msg and "未在数据库中注册" in error_msg:
                                # 提取磁带ID（格式：驱动器中的磁带 xxx 未在数据库中注册）
                                match = re.search(r'磁带\s+(\S+)\s+未在数据库中注册', error_msg)
                                if match:
                                    tape_id_from_error = match.group(1)
                            
                            await self.dingtalk_notifier.send_tape_notification(
                                tape_id=tape_id_from_error,
                                action="error",
                                details={
                                    "error": error_msg,
                                    "task_name": backup_task.task_name,
                                    "task_id": backup_task.id,
                                    "message": "驱动器中的磁带未在数据库中注册，请先在磁带管理页面添加该磁带"
                                }
                            )
                            logger.info("已发送钉钉通知：磁带不在数据库中")
                        except Exception as notify_error:
                            logger.error(f"发送钉钉通知失败: {str(notify_error)}")
                    
                    # 重新抛出异常，停止任务执行
                    raise
                except Exception as e:
                    logger.warning(f"获取磁带信息失败，使用默认ID: {str(e)}")
            
            backup_task.tape_id = tape_id
            
            # 更新数据库中的 tape_id，以便任务卡片正确显示
            logger.info(f"更新数据库中的 tape_id: {tape_id}")
            await self.backup_db.update_task_fields(backup_task, tape_id=tape_id)

            # 3. 创建备份集（简化磁带对象）
            from tape.tape_cartridge import TapeCartridge, TapeStatus
            tape_obj = TapeCartridge(
                tape_id=tape_id,
                label=f"备份磁带-{tape_id}",
                status=TapeStatus.IN_USE,
                capacity_bytes=self.settings.MAX_VOLUME_SIZE,
                used_bytes=0
            )
            backup_set = None
            stored_backup_set_id = getattr(backup_task, 'backup_set_id', None)
            if stored_backup_set_id:
                # backup_task.backup_set_id 可能是字符串 set_id（内存赋值）或整数 id（数据库加载）
                # 优先按字符串 set_id 查询，如果失败则按整数 id 查询
                backup_set = await self.backup_db.get_backup_set_by_set_id(stored_backup_set_id)
                if not backup_set and isinstance(stored_backup_set_id, int):
                    # 整数：按 backup_sets.id 查询
                    backup_set = await self.backup_db.get_backup_set_by_pk_id(stored_backup_set_id)
                if backup_set:
                    logger.info(f"检测到已有备份集 {backup_set.set_id}，继续使用")
                    # 确保 backup_tasks.backup_set_id 已写入数据库（续写场景）
                    try:
                        from utils.scheduler.db_utils import get_opengauss_connection
                        async with get_opengauss_connection() as _conn:
                            await _conn.execute(
                                # 占位符按出现顺序绑定（psycopg3_compat），
                                # 重复使用同一编号会多出一个 %s 导致参数不足
                                "UPDATE backup_tasks SET backup_set_id = $1 "
                                "WHERE id = $2 AND (backup_set_id IS NULL OR backup_set_id != $3)",
                                backup_set.id,
                                backup_task.id,
                                backup_set.id
                            )
                    except Exception as _e:
                        logger.debug(f"续写场景：确保 backup_tasks.backup_set_id 已写入数据库: {_e}")
            if not backup_set:
                backup_set = await self.backup_db.create_backup_set(backup_task, tape_obj)

            # 4. 挂载已在 execute_backup_task() 中完成，跳过重复挂载
            logger.info("========== 跳过挂载（已在磁带检查阶段完成）==========")

            # 5. 流式处理：扫描和压缩循环执行
            # 纯内存扫描模式强制启用：扫描结果只放内存，压缩阶段跳过文件记录写库
            # （不再提供 SCAN_MEMORY_ONLY 开关；文件级恢复能力由此停用）
            from backup.in_memory_file_store import InMemoryFileStore
            in_memory_store = InMemoryFileStore(backup_set.id)
            backup_task.in_memory_file_store = in_memory_store
            logger.info("[备份引擎] 纯内存扫描模式已启用（强制），扫描阶段跳过数据库写入")

            logger.info("开始流式处理：扫描和压缩循环执行...")
            logger.info(f"压缩配置：最大文件大小={format_bytes(self.settings.MAX_FILE_SIZE)}")

            scan_status = getattr(backup_task, 'scan_status', 'pending') or 'pending'
            force_rescan = getattr(backup_task, 'force_rescan', False)
            should_run_scanner = force_rescan or scan_status != 'completed'
            restart_scan = force_rescan or scan_status in (None, '', 'pending', 'failed', 'cancelled')
            
            if should_run_scanner:
                logger.info("========== 启动后台扫描任务，写入 backup_files ==========")

                # 记录关键阶段：扫描文件开始
                self.backup_db._log_operation_stage_event(
                    backup_task,
                    "[扫描文件中...]"
                )
                # 更新operation_stage和description
                await self.backup_db.update_task_stage_with_description(
                    backup_task,
                    "scan",
                    "[扫描文件中] 正在扫描源路径..."
                )

                # 创建内存文件存储（纯内存扫描模式）
                # in_memory_store 已在外部初始化，这里不再重复声明

                scan_progress_task = asyncio.create_task(
                    self.backup_scanner.scan_for_progress_update(
                        backup_task,
                        backup_task.source_paths,
                        exclude_patterns,
                        backup_set,
                        restart=restart_scan,
                        in_memory_store=in_memory_store
                    )
                )
                logger.info("后台扫描任务已启动")
                scan_wait_timeout = getattr(self.settings, "SCAN_WAIT_TIMEOUT", 300) or 300
                # 预分组提前量（可配置）：让预取器比压缩早 N 秒启动，压缩启动时队列已有组可吃。
                # 若提前量 ≥ 扫描等待超时，不设提前量，保持原逻辑（同时启动）。
                try:
                    _prefetch_lead = int(
                        getattr(self.settings, 'GROUP_PREFETCH_LEAD_SECONDS', 30) or 0)
                except Exception:
                    _prefetch_lead = 30
                if _prefetch_lead < 0:
                    _prefetch_lead = 0
                _prefetch_early = (_prefetch_lead > 0
                                   and scan_wait_timeout > _prefetch_lead)
                _wait_before_prefetch = (scan_wait_timeout - _prefetch_lead
                                         if _prefetch_early else scan_wait_timeout)
                logger.info(
                    f"等待后台扫描写入文件记录（最多{_wait_before_prefetch}秒，扫描提前完成则立即继续）..."
                    + (f"；预分组将在压缩启动前 {_prefetch_lead} 秒提前启动"
                       if _prefetch_early else "")
                )
                # 等待扫描任务完成或到达"预分组启动点"
                _done, _pending = await asyncio.wait({scan_progress_task}, timeout=_wait_before_prefetch)
                if scan_progress_task in _done:
                    _scan_exc = scan_progress_task.exception()
                    if _scan_exc is not None:
                        # 扫描在等待期内异常退出：立即抛出，避免后续压缩空跑
                        logger.error("后台扫描任务在等待期内异常退出")
                        raise _scan_exc
                    logger.info("[备份引擎] 扫描已提前完成，立即开始压缩（无需等满超时）")
                else:
                    if _prefetch_early:
                        # 提前启动预分组，随后再等 30 秒启动压缩
                        logger.info(
                            f"等待达 {_wait_before_prefetch} 秒，扫描仍在进行，"
                            f"提前启动预分组（压缩将在 {_prefetch_lead} 秒后启动）"
                        )
                        _create_and_start_prefetcher()
                        _done2, _ = await asyncio.wait({scan_progress_task}, timeout=_prefetch_lead)
                        if scan_progress_task in _done2:
                            _scan_exc = scan_progress_task.exception()
                            if _scan_exc is not None:
                                logger.error("后台扫描任务在预分组提前量内异常退出")
                                raise _scan_exc
                            logger.info("[备份引擎] 扫描在提前量内完成，立即开始压缩")
                        else:
                            logger.info(
                                f"预分组已运行 {_prefetch_lead} 秒，开始压缩（扫描与压缩并发执行）"
                            )
                    else:
                        logger.info(
                            f"等待达 {scan_wait_timeout} 秒上限（≤{_prefetch_lead} 秒，不设提前量），"
                            f"扫描仍在进行，开始压缩（扫描与压缩并发执行）"
                        )
            else:
                logger.info("扫描状态为 completed，跳过扫描阶段")
            
            logger.info("========== 开始从数据库读取待压缩文件 ==========")

            # 记录关键阶段：开始压缩
            self.backup_db._log_operation_stage_event(
                backup_task,
                "[压缩文件中...]"
            )
            # 更新operation_stage和description - 使用更准确的状态描述
            await self.backup_db.update_task_stage_with_description(
                backup_task,
                "compress",
                "[压缩文件中...] 初始化压缩引擎..."
            )

            # ========== 设置压缩和移动线程的专属日志处理器 ==========
            try:
                from utils.worker_log_handler import WorkerLogHandler, ScheduleType
                worker_log_handler = WorkerLogHandler()
                
                # 从 scheduled_task 中获取调度类型
                schedule_type = ScheduleType.UNKNOWN
                if scheduled_task and hasattr(scheduled_task, 'schedule_type'):
                    schedule_type_str = str(scheduled_task.schedule_type).lower()
                    if 'daily' in schedule_type_str:
                        schedule_type = ScheduleType.DAILY
                    elif 'monthly' in schedule_type_str:
                        schedule_type = ScheduleType.MONTHLY
                else:
                    # 尝试从 backup_task 推断
                    schedule_type = WorkerLogHandler.get_schedule_type_from_task(backup_task)
                
                # 设置专属日志处理器
                worker_log_handler.setup_worker_loggers(schedule_type)
                logger.info(f"已设置压缩和移动线程专属日志处理器（备份周期: {schedule_type.value}）")
            except Exception as log_setup_error:
                logger.warning(f"设置专属日志处理器失败: {str(log_setup_error)}，将使用默认日志")
            
            # 注意：文件移动到磁带由FinalDirMonitor独立线程监控final目录处理
            # 不再需要FileMoveWorker
            
            # ========== openGauss模式：创建文件组预取器 ==========
            # 若扫描等待阶段已提前启动，这里幂等跳过
            from utils.scheduler.db_utils import is_opengauss
            logger.info(f"[备份引擎] 检查数据库类型: is_opengauss={is_opengauss()}")
            if is_opengauss() and file_group_prefetcher is None:
                _create_and_start_prefetcher()
            
            # ========== 创建压缩循环后台任务（独立线程，顺序执行） ==========
            logger.info(f"[备份引擎] 创建压缩工作线程，预取器: {file_group_prefetcher is not None}")
            compression_worker = CompressionWorker(
                backup_db=self.backup_db,
                compressor=self.compressor,
                backup_set=backup_set,
                backup_task=backup_task,
                settings=self.settings,
                file_move_worker=None,  # 不再向 file_move_worker 发送消息，它独立扫描 final 目录
                backup_notifier=self.backup_notifier,
                tape_file_mover=None,  # 不再使用队列模式，FinalDirMonitor独立监控final目录
                file_group_prefetcher=file_group_prefetcher,  # openGauss模式下的预取器
                in_memory_store=in_memory_store  # 纯内存扫描模式的文件存储
            )
            # 注入写带饿死信号：final 长时间无文件可写时不继续降并发（Vegas 护栏）
            try:
                compression_worker.write_starved_provider = (
                    lambda: bool(getattr(self.final_dir_monitor, '_starve_start', None)))
            except Exception:
                pass
            compression_worker.start()
            logger.info(f"[备份引擎] 压缩工作线程已启动，使用预取模式: {compression_worker.use_prefetcher}")
            # 存储压缩工作线程引用，以便get_task_status可以访问实时进度
            self._current_compression_worker = compression_worker

            # ========== 等待压缩循环完成 ==========
            processed_files = 0
            total_size = 0
            try:
                # 无进展看门狗：不给 compression_task 设固定超时（正常任务十几个小时），
                # 而是检测"组数/文件数/字节数"是否还有变化 + 是否有组在压。
                # 30 分钟无任何进展且无在压组 → 判定收尾挂起（任务 149 曾因僵尸
                # future 卡死），跳出等待，由 finally 的 stop() 取消残留任务。
                _wd_snapshot = None
                _wd_stuck_since = None
                while True:
                    done, _ = await asyncio.wait(
                        {compression_worker.compression_task}, timeout=60)
                    if compression_worker.compression_task.done():
                        break
                    _active = int(getattr(self.compressor, '_active_compressing', 0) or 0)
                    _snap = (compression_worker.group_idx,
                             compression_worker.processed_files,
                             compression_worker.total_size)
                    if _snap != _wd_snapshot or _active > 0:
                        _wd_snapshot = _snap
                        _wd_stuck_since = None
                        continue
                    if _wd_stuck_since is None:
                        _wd_stuck_since = time.time()
                    elif time.time() - _wd_stuck_since > 1800:
                        logger.error(
                            "[备份引擎] 压缩 worker 30 分钟无进展且无在压组，"
                            "判定收尾挂起，强制停止（已完成数据不受影响）")
                        break
                # 压缩完成，获取统计信息
                processed_files = compression_worker.processed_files
                total_size = compression_worker.total_size
            except (KeyboardInterrupt, asyncio.CancelledError):
                logger.warning("========== 压缩循环被中止 ==========")
                logger.warning(f"任务ID: {backup_task.id if backup_task else 'N/A'}")
                # 即使被中断，也尝试获取已处理的统计信息
                try:
                    processed_files = compression_worker.processed_files
                    total_size = compression_worker.total_size
                except:
                    pass
                raise
            finally:
                # 停止压缩循环、文件移动任务和预取器
                await compression_worker.stop()
                # 错误明细快照（worker 引用随后置空；任务结束写 result_summary）
                try:
                    backup_task.error_logs_cache = list(
                        getattr(compression_worker, 'error_logs', []) or [])
                except Exception:
                    pass
                # 压缩开始时间快照：worker 引用随后置空，execute 层统计
                # compression_started_at 时拿到的永远是 None（压缩速度恒显示"—"，
                # 任务 148/149/151 均如此）。快照挂到 backup_task 上传递。
                try:
                    backup_task.compression_started_at_snapshot = float(
                        getattr(compression_worker, 'compression_started_at', 0) or 0)
                except Exception:
                    pass
                # 清除压缩工作线程引用
                self._current_compression_worker = None
                # 注意：不再需要file_move_worker，FinalDirMonitor独立运行
                if file_group_prefetcher:
                    await file_group_prefetcher.stop()
            
            # 压缩完成日志：换行输出，与其他日志有明显差异
            logger.info("=" * 80)
            logger.info("[备份引擎] ========== 数据库压缩完成 ==========")
            logger.info(f"  处理文件组数: {compression_worker.group_idx}")
            logger.info(f"  处理文件数: {compression_worker.processed_files:,} 个文件")
            logger.info(f"  原始总大小: {format_bytes(compression_worker.total_original_size)}")
            logger.info(f"  压缩后总大小: {format_bytes(total_size)}")
            if compression_worker.total_original_size > 0:
                compression_ratio = (1 - total_size / compression_worker.total_original_size) * 100
                logger.info(f"  压缩率: {compression_ratio:.2f}%")
            logger.info("=" * 80)

            # 等待所有文件移动到final目录（检查final目录是否为空）
            # 注意：移动到final就算完成，不需要等待移动到磁带
            final_move_completed = True  # 默认完成（如果没有final_dir_monitor）
            # 写带收尾（必须在"等待 final 为空"之前）：
            #   final 中可能残留此前写带失败、已被监控器拉黑的文件；不强制处理的话，
            #   任务会一直等到 1 小时超时判失败，且停机清理会把它们删掉。
            #   这里逐个再试一次写带；仍失败的移入隔离目录（QUARANTINE_DIR），
            #   保留数据供补写/排查，同时让 final 清空。
            if self.final_dir_monitor:
                try:
                    # 收尾可能包含大文件跨盘隔离，放 executor 执行，避免阻塞事件循环
                    _final_stats = await asyncio.get_event_loop().run_in_executor(
                        None,
                        self.final_dir_monitor.finalize_pending_files,
                        (backup_set.set_id if backup_set else None),
                    )
                    backup_task.final_retry_stats = _final_stats
                    _q = _final_stats.get('quarantined') or []
                    _f = _final_stats.get('failed') or []
                    if _q:
                        logger.warning(
                            f"[备份引擎] ⚠️ {len(_q)} 个文件写带失败，已移入隔离目录: "
                            f"{self.final_dir_monitor._get_quarantine_dir()}")
                    if _f:
                        logger.error(f"[备份引擎] ❌ {len(_f)} 个文件既未写带也无法隔离")
                except Exception as _finalize_err:
                    logger.error(f"[备份引擎] 写带收尾失败: {_finalize_err}", exc_info=True)
            if self.final_dir_monitor:
                logger.info("[备份引擎] 压缩完成，等待所有文件移动到final目录...")
                max_wait_time = 3600  # 最多等待1小时
                check_interval = 10  # 每10秒检查一次
                wait_count = 0
                max_checks = max_wait_time // check_interval
                
                while wait_count < max_checks:
                    current_set_id = backup_set.set_id if backup_set else None
                    if self.final_dir_monitor.is_final_dir_empty(current_set_id):
                        logger.info("[备份引擎] ✅ final目录已为空，所有文件已写入磁带")
                        final_move_completed = True
                        # 结算“压缩喂不饱”统计：丢弃最后一段收尾空转
                        try:
                            self.final_dir_monitor.finalize_starvation()
                        except Exception:
                            pass

                        # 清理当前任务的 set_id 空目录
                        if backup_set and backup_set.set_id:
                            self.final_dir_monitor.cleanup_set_id_dir(backup_set.set_id)

                        break
                    
                    remaining_files = self.final_dir_monitor.get_processed_count()
                    logger.info(f"[备份引擎] final目录仍有文件，等待移动完成... (已处理: {remaining_files} 个文件)")
                    await asyncio.sleep(check_interval)
                    wait_count += 1
                
                if wait_count >= max_checks:
                    logger.warning(f"[备份引擎] ⚠️ 等待文件移动到final目录超时（{max_wait_time}秒），但继续完成备份任务")
                    final_move_completed = False  # 超时，视为未完成
                else:
                    logger.info(f"[备份引擎] ✅ 所有文件已移动到final目录，等待时间: {wait_count * check_interval}秒")
                    final_move_completed = True

            # 注意：不再在这里调用 finalize_backup_set
            # 只有文件压缩任务（compression_worker）可以标记任务集状态为完成并发钉钉
            # 压缩任务会在收不到队列且任务集完成标记为 completed 时自动调用 _finalize_backup_set_and_notify
            
            # 检查所有完成条件：扫描完成 + 文件组预取完成 + 压缩完成 + 文件移动到final目录完成
            scan_status = await self.backup_db.get_scan_status(backup_task.id)
            scan_completed = scan_status == 'completed'
            
            # 检查预取器是否完成（执行次数 > 0 且已停止且无异常退出）
            prefetch_completed = False
            if file_group_prefetcher:
                prefetch_loop_count = getattr(file_group_prefetcher, 'prefetch_loop_count', 0)
                prefetch_running = getattr(file_group_prefetcher, '_running', False)
                prefetch_error = getattr(file_group_prefetcher, 'prefetch_error', None)
                prefetch_completed = (prefetch_loop_count > 0 and not prefetch_running
                                      and not prefetch_error)
                if prefetch_error:
                    logger.error(f"[备份引擎] 预取器异常退出（未分组文件被放弃）: {prefetch_error}")
            else:
                prefetch_completed = True  # 没有预取器，视为已完成

            # 检查压缩是否完成（内存状态；组失败/循环异常时 compression_failed=True，
            # 不得按完成处理）
            compression_completed = (
                getattr(backup_task, 'compression_completed', False)
                and not getattr(backup_task, 'compression_failed', False)
            )
            
            # 所有条件都满足，标记任务完成
            if scan_completed and prefetch_completed and compression_completed and final_move_completed:
                # 备份任务完成日志：换行输出，与其他日志有明显差异
                logger.info("=" * 80)
                logger.info("[备份引擎] ========== 备份任务完成 ==========")
                logger.info(f"  任务ID: {backup_task.id}")
                logger.info(f"  任务名称: {backup_task.task_name}")
                logger.info(f"  扫描完成: ✅")
                logger.info(f"  预取完成: ✅ (执行次数: {getattr(file_group_prefetcher, 'prefetch_loop_count', 0) if file_group_prefetcher else 0})")
                logger.info(f"  压缩完成: ✅")
                logger.info(f"  文件移动到final目录完成: ✅" if final_move_completed else "  文件移动到final目录完成: ⚠️ (超时)")
                logger.info(f"  处理文件数: {processed_files:,} 个文件")
                logger.info(f"  总文件大小: {format_bytes(total_size)}")
                logger.info("=" * 80)
                
                # 更新数据库和内存中的任务状态为完成
                # 注意：不可在此处局部导入 BackupTaskStatus——它会把该名字变成函数局部变量，
                # 使同函数内更早的取消分支（except KeyboardInterrupt / CancelledError）引用时
                # 抛 UnboundLocalError（模块级已导入，直接用全局即可）
                backup_task.status = BackupTaskStatus.COMPLETED
                await self.backup_db.update_task_status(backup_task, BackupTaskStatus.COMPLETED)

                # 更新磁带的已用容量（LTFS路径不走tape_manager.write_data，需要在此手动更新）
                tape_id = getattr(backup_task, 'tape_id', None) or (backup_set.tape_id if backup_set else None)
                if tape_id:
                    from utils.db_connection_helper import get_psycopg_connection_from_url
                    from config.settings import get_settings
                    try:
                        bytes_written = getattr(backup_task, 'compressed_bytes', 0) or total_size
                        if bytes_written > 0:
                            settings = get_settings()
                            conn, _ = get_psycopg_connection_from_url(settings.DATABASE_URL, prefer_psycopg3=True)
                            if conn:
                                try:
                                    with conn.cursor() as cur:
                                        cur.execute(
                                            """
                                            UPDATE tape_cartridges
                                            SET used_bytes = used_bytes + %s,
                                                write_count = write_count + 1,
                                                updated_at = NOW()
                                            WHERE tape_id = %s
                                            """,
                                            (bytes_written, tape_id)
                                        )
                                        conn.commit()
                                    logger.info(f"[备份引擎] 更新磁带已用容量: tape_id={tape_id}, bytes_written={format_bytes(bytes_written)}")
                                finally:
                                    conn.close()
                    except Exception as tape_update_err:
                        logger.warning(f"[备份引擎] 更新磁带已用容量失败: {tape_update_err}")

                # 更新操作状态
                await self.backup_db.update_scan_progress(
                    backup_task, 
                    processed_files, 
                    backup_task.total_files, 
                    f"[备份完成] 处理了 {processed_files} 个文件，总大小 {format_bytes(total_size)}"
                )
                
                # 发送钉钉通知（任务集完成）
                try:
                    if self.dingtalk_notifier:
                        # 计算任务耗时
                        task_start_time = backup_task.started_at if hasattr(backup_task, 'started_at') and backup_task.started_at else None
                        task_end_time = now()
                        duration_seconds = 0
                        if task_start_time:
                            duration_seconds = (task_end_time - task_start_time).total_seconds()
                        
                        # 格式化耗时
                        if duration_seconds < 60:
                            duration_str = f"{duration_seconds:.1f}秒"
                        elif duration_seconds < 3600:
                            duration_str = f"{duration_seconds / 60:.1f}分钟"
                        else:
                            hours = int(duration_seconds / 3600)
                            minutes = int((duration_seconds % 3600) / 60)
                            duration_str = f"{hours}小时{minutes}分钟"
                        
                        # 计算每小时G数（备份速度）
                        speed_gb_per_hour = 0
                        speed_str = "N/A"
                        if duration_seconds > 0:
                            total_size_gb = total_size / (1024 ** 3)  # 转换为GB
                            duration_hours = duration_seconds / 3600  # 转换为小时
                            speed_gb_per_hour = total_size_gb / duration_hours if duration_hours > 0 else 0
                            speed_str = f"{speed_gb_per_hour:.2f} GB/小时"
                        
                        await self.dingtalk_notifier.send_backup_notification(
                            backup_name=backup_task.task_name,
                            status="success",
                            details={
                                'file_count': processed_files,
                                'size': format_bytes(total_size),
                                'duration': duration_str,
                                'speed': speed_str,
                                'speed_gb_per_hour': speed_gb_per_hour,
                                'task_id': backup_task.id,
                                'backup_set_id': backup_set.set_id if backup_set else None
                            }
                        )
                        logger.info(f"[备份引擎] ✅ 任务集完成钉钉通知已发送（速度: {speed_str}）")
                    else:
                        logger.debug(f"[备份引擎] 钉钉通知器未初始化，跳过通知")
                except Exception as notify_error:
                    logger.error(f"[备份引擎] 发送任务集完成钉钉通知异常: {str(notify_error)}", exc_info=True)
            else:
                # 任务未完全完成：必须落到失败终态，不允许任务永久停留在"运行中"
                # （此前的行为是只写 warning + 返回 True，任务状态悬空、无告警）
                error_detail = (
                    f"扫描完成={scan_completed}，预取完成={prefetch_completed}，"
                    f"压缩完成={compression_completed}，"
                    f"文件移动到final目录完成={final_move_completed}"
                )
                logger.warning(f"⚠️ 任务未完全完成：{error_detail}")
                backup_task.error_message = f"任务未完全完成: {error_detail}"[:500]
                try:
                    await self.backup_db.update_task_stage_with_description(
                        backup_task,
                        "failed",
                        f"[任务失败] {backup_task.error_message}"
                    )
                    await self.backup_db.update_task_status(backup_task, BackupTaskStatus.FAILED)
                except Exception as status_err:
                    logger.error(f"[备份引擎] 标记任务失败状态时出错: {status_err}")
                return False

            return True

        except KeyboardInterrupt:
            # 用户按 Ctrl+C 中止任务
            logger.warning("========== 用户中止备份任务（Ctrl+C） ==========")
            logger.warning(f"任务ID: {backup_task.id if backup_task else 'N/A'}")
            
            # 更新任务状态为取消
            if backup_task:
                try:
                    backup_task.error_message = "用户中止任务（Ctrl+C）"
                    await self.backup_db.update_task_stage_with_description(
                        backup_task,
                        "cancelled",
                        "[任务取消] 用户中止任务（Ctrl+C）"
                    )
                    await self.backup_db.update_task_status(backup_task, BackupTaskStatus.CANCELLED)
                    await self.backup_db.update_scan_progress(backup_task, processed_files if 'processed_files' in locals() else 0,
                                                            backup_task.total_files if hasattr(backup_task, 'total_files') else 0,
                                                            "[已取消]")
                    logger.info("任务状态已更新为取消")
                except Exception as update_error:
                    logger.error(f"更新任务状态失败: {str(update_error)}")
            
            # 重新抛出 KeyboardInterrupt，让上层处理
            raise
            
        except asyncio.CancelledError:
            # 任务被取消
            logger.warning("========== 备份任务被取消 ==========")
            logger.warning(f"任务ID: {backup_task.id if backup_task else 'N/A'}")
            
            # 更新任务状态为取消
            if backup_task:
                try:
                    backup_task.error_message = "任务被取消"
                    await self.backup_db.update_task_stage_with_description(
                        backup_task,
                        "cancelled",
                        "[任务取消] 任务被系统取消"
                    )
                    await self.backup_db.update_task_status(backup_task, BackupTaskStatus.CANCELLED)
                    await self.backup_db.update_scan_progress(backup_task, processed_files if 'processed_files' in locals() else 0,
                                                            backup_task.total_files if hasattr(backup_task, 'total_files') else 0,
                                                            "[已取消]")
                    logger.info("任务状态已更新为取消")
                except Exception as update_error:
                    logger.error(f"更新任务状态失败: {str(update_error)}")
            
            # 重新抛出 CancelledError，让上层处理
            raise
            
        except Exception as e:
            logger.error(f"备份流程执行失败: {str(e)}")
            import traceback
            logger.error(f"错误堆栈:\n{traceback.format_exc()}")
            if backup_task:
                backup_task.error_message = str(e)
            return False
            
        finally:
            # 清理资源：取消后台扫描任务
            if scan_progress_task and not scan_progress_task.done():
                logger.info("========== 取消后台扫描任务 ==========")
                try:
                    scan_progress_task.cancel()
                    # 等待任务取消完成（带超时）
                    try:
                        await asyncio.wait_for(scan_progress_task, timeout=5.0)
                    except asyncio.TimeoutError:
                        logger.warning("后台扫描任务取消超时，强制终止")
                    except asyncio.CancelledError:
                        logger.info("后台扫描任务已成功取消")
                    except Exception as cancel_error:
                        logger.warning(f"取消后台扫描任务时发生错误: {str(cancel_error)}")
                except Exception as cleanup_error:
                    logger.error(f"清理后台扫描任务失败: {str(cleanup_error)}")
                finally:
                    logger.info("后台扫描任务清理完成")
            elif scan_progress_task and scan_progress_task.done():
                # 扫描协程已结束（等待期外异常退出过）：消费异常，避免静默丢失
                # （此前只在"未完成"时处理，已失败协程的异常从未被 retrieve）
                _scan_late_exc = None
                try:
                    _scan_late_exc = scan_progress_task.exception()
                except asyncio.CancelledError:
                    pass
                if _scan_late_exc is not None:
                    logger.error(
                        f"[备份引擎] 后台扫描任务异常退出（此前未传播）: {_scan_late_exc}",
                        exc_info=_scan_late_exc)

            # 释放磁带操作锁
            try:
                from utils.linux_tape import release_tape_operation_lock, is_tape_operation_locked
                if is_tape_operation_locked():
                    lock_owner = f"backup_task_{backup_task.id}" if backup_task else "unknown"
                    logger.info(f"[备份完成] 释放磁带操作锁 (owner={lock_owner})")
                    release_tape_operation_lock()
                else:
                    logger.info("[备份完成] 磁带操作锁已释放，无需重复释放")
            except Exception as lock_error:
                logger.warning(f"释放磁带操作锁失败: {str(lock_error)}")

    async def get_task_status(self, task_id: int) -> Optional[Dict]:
        """获取任务状态 - 委托给 BackupTaskManager"""
        return await self.task_manager.get_task_status(task_id)

    async def cancel_task(self, task_id: int) -> bool:
        """取消任务 - 委托给 BackupTaskManager"""
        try:
            # 取消当前任务
            if self._current_task and self._current_task.id == task_id:
                result = await self.task_manager.cancel_task(task_id)
                if result:
                    self._current_task = None
                return result
            else:
                # 取消其他任务
                return await self.task_manager.cancel_task(task_id)
        except Exception as e:
            logger.error(f"取消任务失败: {str(e)}")
            return False
