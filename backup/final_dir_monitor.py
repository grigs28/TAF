#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
Final目录监控器
独立线程监控final目录，发现文件后顺序移动到磁带
"""

import asyncio
import logging
import os
import shutil
import threading
import time
from pathlib import Path
from typing import Optional, Dict, Set
from datetime import datetime

from config.settings import get_settings
from backup.tape_handler import TapeHandler
from models.backup import BackupSet
from backup.utils import format_bytes

logger = logging.getLogger(__name__)


class FinalDirMonitor:
    """Final目录监控器
    
    功能：
    1. 独立线程监控final目录（不阻塞其他程序，也不被其他程序阻塞）
    2. 每10秒轮询扫描final目录
    3. 发现文件后顺序移动到磁带（移动完一个再移动下一个）
    4. 支持任务完成判断
    """
    
    def __init__(self, tape_handler: TapeHandler, settings=None, dingtalk_notifier=None):
        """
        初始化Final目录监控器

        Args:
            tape_handler: TapeHandler实例，用于实际移动文件
            settings: 系统设置
            dingtalk_notifier: 钉钉通知器（可选）
        """
        self.tape_handler = tape_handler
        self.settings = settings or get_settings()
        self.dingtalk_notifier = dingtalk_notifier
        self._worker_thread: Optional[threading.Thread] = None
        self._running = False
        self._lock = threading.Lock()
        self._scan_interval = 10  # 扫描间隔（秒）
        self._processed_files: Set[str] = set()  # 已处理文件的集合（完整路径）
        self._current_tape_file: Optional[str] = None  # 当前正在写入磁带的文件路径
        # "压缩喂不饱写带"的累计等待时间：final 目录无文件可写、等压缩产出的时长。
        # 统计口径（用户明确）：第一次写带开始之后，final 为空的累计时间。
        # 从未写过带之前的空目录不计；任务收尾的最后一段由 finalize_starvation() 丢弃。
        self._starvation_seconds: float = 0.0
        self._starve_start: Optional[float] = None
        self._has_written_once: bool = False   # 是否已完成过至少一次写带
        # 任务收尾期间的互斥标志：finalize_pending_files() 执行时暂停轮询，
        # 避免监控线程与收尾逻辑同时处理同一文件（重复写带/竞态删除）。
        self._finalizing: bool = False
        # 错误明细（供“错误日志”弹窗，任务结束时持久化到 result_summary）
        # 元素：{'category': 'tape', 'time': str, 'message': str, 'detail': str}
        self.error_logs: list = []
        # 本轮是否因“文件仍在写入”而跳过（调用方据此不标记已处理，留待下次轮询）
        self._defer_current_file: bool = False
        
    def start(self):
        """启动监控线程"""
        with self._lock:
            if self._running:
                logger.warning("[Final监控] 监控线程已经在运行")
                return

            # 新任务开始：重置"压缩喂不饱"统计
            self._starvation_seconds = 0.0
            self._starve_start = None
            self._has_written_once = False
            self._running = True
            self._worker_thread = threading.Thread(
                target=self._monitor_loop,
                name="FinalDirMonitor",
                daemon=True
            )
            self._worker_thread.start()
            logger.info("[Final监控] Final目录监控线程已启动（10秒轮询扫描）")
    
    def stop(self):
        """停止监控线程（等待当前文件写入磁带完成）"""
        with self._lock:
            if not self._running:
                return

            self._running = False
            if self._worker_thread and self._worker_thread.is_alive():
                logger.info("[Final监控] 等待当前磁带写入完成...")
                self._worker_thread.join(timeout=300)  # 5分钟，等待大文件写入完成
                if self._worker_thread.is_alive():
                    logger.warning("[Final监控] 监控线程未能在5分钟内停止")
                else:
                    logger.info("[Final监控] Final目录监控线程已停止")
    
    def reset_starvation_stats(self) -> None:
        """新任务开始时清零"压缩喂不饱"统计。

        监控线程常驻（start() 只在引擎初始化时真正执行一次，其后 early-return
        跳过重置），必须由引擎在每个任务开始时显式调用，否则等待秒数会跨任务累计。
        """
        with self._lock:
            self._starvation_seconds = 0.0
            self._starve_start = None
            self._has_written_once = False
            self.error_logs = []
            # 新任务开始：清空上一任务的处理记录。监控线程常驻，集合若不清空
            # 会跨任务累积——同一天重跑同一 set_id 时同名文件会被永久跳过，
            # 且内存随文件数增长。
            self._processed_files.clear()

    def finalize_starvation(self) -> float:
        """任务收尾调用：丢弃未结算的最后一段等待。

        压缩已完成、final 已空后的空转属于任务收尾，不是“压缩喂不饱”，
        因此把当前未结算的等待段直接丢弃，返回累计饿死时间。
        """
        if self._starve_start is not None:
            self._starve_start = None
        return self._starvation_seconds

    def _get_final_dir(self) -> Path:
        """获取final目录路径"""
        compress_dir = Path(self.settings.BACKUP_COMPRESS_DIR)
        final_dir = compress_dir / "final"
        return final_dir

    def _send_failure_notification(self, error_msg: str):
        """发送失败通知到钉钉"""
        logger.error(f"[Final监控] 准备发送失败通知: {error_msg}")
        if self.dingtalk_notifier:
            try:
                # 在新的事件循环中运行异步通知
                loop = asyncio.new_event_loop()
                asyncio.set_event_loop(loop)
                try:
                    loop.run_until_complete(
                        self.dingtalk_notifier.send_backup_notification(
                            "磁带写入",
                            "failed",
                            {'error': error_msg}
                        )
                    )
                    logger.info("[Final监控] 失败通知已发送到钉钉")
                finally:
                    loop.close()
                    asyncio.set_event_loop(None)
            except Exception as notify_error:
                logger.warning(f"[Final监控] 发送钉钉通知失败: {str(notify_error)}")
        else:
            logger.warning("[Final监控] 未配置钉钉通知器，无法发送失败通知")

    def _extract_backup_set_id_from_path(self, file_path: Path) -> Optional[str]:
        """从文件路径提取backup_set.set_id
        
        路径格式: temp/compress/final/{set_id}/backup_xxx.tar.zst
        """
        try:
            # 获取相对于final目录的路径
            final_dir = self._get_final_dir()
            try:
                relative_path = file_path.relative_to(final_dir)
                # 第一级目录就是set_id
                parts = relative_path.parts
                if len(parts) >= 1:
                    return parts[0]  # set_id
            except ValueError:
                # 如果无法计算相对路径，尝试从文件名提取
                filename = file_path.name
                if filename.startswith("backup_"):
                    parts = filename.split("_")
                    if len(parts) >= 2:
                        return parts[1]  # set_id
            return None
        except Exception as e:
            logger.debug(f"[Final监控] 提取backup_set_id失败: {file_path}, 错误: {str(e)}")
            return None

    def _move_file_to_tape(self, file_path: Path) -> bool:
        """
        移动单个文件到磁带

        Args:
            file_path: 源文件路径

        Returns:
            bool: 是否成功
        """
        try:
            source_file = file_path
            if not source_file.exists():
                logger.warning(f"[Final监控] 文件不存在: {source_file}")
                return False

            # 获取源文件大小（用于验证）
            source_size = source_file.stat().st_size

            # 复制前稳定性复核（P1-5）：间隔 2 秒两次 stat，大小变化说明
            # 压缩侧仍在写入（或同名任务并发写），本轮跳过、下轮再试，
            # 避免复制到一半源文件继续增长导致大小校验失败。
            try:
                _s1 = source_file.stat().st_size
                time.sleep(2.0)
                _s2 = source_file.stat().st_size
            except OSError:
                logger.warning(f"[Final监控] 文件在复核期间消失: {source_file}")
                return False
            if _s1 != _s2:
                logger.warning(
                    f"[Final监控] 文件仍在写入（{_s1} -> {_s2}），本轮跳过: {source_file.name}")
                # 通知调用方：这是“暂时跳过”，不要标记为已处理（下一轮重试）
                self._defer_current_file = True
                return False
            source_size = _s2

            logger.info(f"[Final监控] 开始移动文件到磁带: {source_file.name} (大小: {format_bytes(source_size)})")

            # 从路径提取backup_set_id
            backup_set_id = self._extract_backup_set_id_from_path(source_file)
            if not backup_set_id:
                logger.warning(f"[Final监控] 无法从路径提取backup_set_id: {source_file}")
                # 创建一个临时的BackupSet对象
                backup_set = BackupSet()
                backup_set.set_id = "unknown"
            else:
                backup_set = BackupSet()
                backup_set.set_id = backup_set_id

            # 目标路径由 tape_handler 处理（Linux 用 tar 写磁带设备，Windows 用 LTFS 盘符）
            # 这里不需要预先创建目录

            # 在工作线程中运行异步操作
            loop = asyncio.new_event_loop()
            asyncio.set_event_loop(loop)

            try:
                # 调用tape_handler的write_to_tape_drive方法
                tape_file_path = loop.run_until_complete(
                    self.tape_handler.write_to_tape_drive(
                        str(source_file),
                        backup_set,
                        0  # group_idx，这里不需要，传0
                    )
                )

                if tape_file_path:
                    logger.info(f"[Final监控] ✅ 文件已成功移动到磁带: {source_file.name} -> {tape_file_path}")

                    # 记录文件写入磁带到数据库
                    try:
                        from utils.log_utils import log_operation
                        from models.system_log import OperationType
                        loop.run_until_complete(
                            log_operation(
                                operation_type=OperationType.TAPE_LOAD,
                                resource_type="tape",
                                resource_name=str(file_path.name),
                                operation_name="文件写入磁带",
                                operation_description=f"文件 {file_path.name} 已写入磁带",
                                category="tape",
                                success=True,
                            )
                        )
                    except Exception:
                        pass

                    return True
                else:
                    error_msg = f"文件移动到磁带失败: {source_file.name}"
                    logger.error(f"[Final监控] ❌ {error_msg}")
                    try:
                        self.error_logs.append({
                            'category': 'tape',
                            'time': time.strftime('%Y-%m-%d %H:%M:%S'),
                            'message': f"写磁带失败：{source_file.name}",
                            'detail': f"源大小 {format_bytes(source_size)}",
                        })
                    except Exception:
                        pass
                    # 发送钉钉通知
                    self._send_failure_notification(error_msg)
                    return False

            finally:
                # 确保关闭事件循环，释放资源
                try:
                    pending = asyncio.all_tasks(loop)
                    for task_obj in pending:
                        task_obj.cancel()
                    if pending:
                        loop.run_until_complete(asyncio.gather(*pending, return_exceptions=True))
                except Exception:
                    pass
                finally:
                    loop.close()
                    asyncio.set_event_loop(None)

        except Exception as e:
            logger.error(f"[Final监控] 移动文件到磁带失败: {file_path}, 错误: {str(e)}", exc_info=True)
            return False

    # ===== 写带收尾：残余文件强制重试 + 失败隔离 =====

    @staticmethod
    def _is_archive_file(file_path: Path) -> bool:
        """与监控/空目录判定相同的压缩文件口径"""
        return (file_path.suffix in ['.7z', '.gz', '.tar', '.zst']
                or file_path.name.endswith('.tar.gz'))

    def _get_quarantine_dir(self) -> Path:
        """隔离目录（默认 /TAF/quarantine，不占 SSD 压缩盘）"""
        qdir = getattr(self.settings, 'QUARANTINE_DIR', None) or '/TAF/quarantine'
        return Path(qdir)

    def _quarantine_file(self, file_path: Path, set_id: Optional[str] = None) -> Optional[Path]:
        """把写带失败的文件移动到隔离目录（不删除，保留补写/排查）。

        跨文件系统时 shutil.move 会退化为"复制+删除"，大文件会占用一段
        收尾时间，但只在失败路径发生。
        """
        try:
            qdir = self._get_quarantine_dir()
            if set_id:
                qdir = qdir / str(set_id)
            qdir.mkdir(parents=True, exist_ok=True)

            target = qdir / file_path.name
            if target.exists():
                # 同名文件已在隔离目录：加时间戳避免覆盖
                target = qdir / f"{file_path.stem}.{int(time.time())}{file_path.suffix}"

            shutil.move(str(file_path), str(target))
            logger.warning(f"[Final监控] ⚠️ 文件写带失败，已移入隔离目录: {file_path} -> {target}")
            try:
                self.error_logs.append({
                    'category': 'tape',
                    'time': time.strftime('%Y-%m-%d %H:%M:%S'),
                    'message': f"写磁带失败，已隔离：{file_path.name}",
                    'detail': f"隔离路径: {target}",
                })
            except Exception:
                pass
            return target
        except Exception as e:
            logger.error(f"[Final监控] 移动到隔离目录失败: {file_path}, 错误: {e}")
            return None

    def finalize_pending_files(self, set_id: Optional[str] = None) -> Dict:
        """任务收尾：把 final 目录中所有剩余文件强制再写一次带。

        覆盖三类此前会被永久跳过的文件：
          · 写带失败后被 `_processed_files` 拉黑的；
          · 监控线程尚未轮询到的；
          · 压缩侧同名竞态导致路径已不存在的。
        仍失败的文件移动到隔离目录（QUARANTINE_DIR），使 final 清空以便任务
        正常判定完成，同时保留数据供人工补写/排查。

        Returns:
            dict: {attempted, written, quarantined: [...], failed: [...], bytes}
        """
        stats = {'attempted': 0, 'written': 0,
                 'quarantined': [], 'failed': [], 'bytes': 0, 'quarantined_bytes': 0}
        self._finalizing = True
        try:
            final_dir = self._get_final_dir()
            if set_id:
                final_dir = final_dir / set_id
            if not final_dir.exists():
                return stats

            files = []
            for root, dirs, names in os.walk(final_dir):
                for name in names:
                    fp = Path(root) / name
                    if fp.is_file() and self._is_archive_file(fp):
                        files.append(fp)

            if not files:
                return stats

            logger.info(f"[Final监控] 写带收尾：final 中仍有 {len(files)} 个文件，逐个重试写带...")
            for fp in files:
                if not fp.exists():
                    continue
                self._processed_files.discard(str(fp))   # 允许重试
                stats['attempted'] += 1
                try:
                    stats['bytes'] += fp.stat().st_size
                except OSError:
                    pass

                self._current_tape_file = str(fp)
                self._defer_current_file = False
                try:
                    ok = self._move_file_to_tape(fp)
                finally:
                    self._current_tape_file = None

                if ok:
                    stats['written'] += 1
                    continue

                try:
                    _qbytes = fp.stat().st_size
                except OSError:
                    _qbytes = 0
                qpath = self._quarantine_file(fp, set_id)
                if qpath:
                    stats['quarantined'].append(str(qpath))
                    stats['quarantined_bytes'] += _qbytes
                else:
                    stats['failed'].append(str(fp))

            logger.info(
                f"[Final监控] 写带收尾完成：尝试 {stats['attempted']} 个，"
                f"成功 {stats['written']} 个，隔离 {len(stats['quarantined'])} 个，"
                f"无法隔离 {len(stats['failed'])} 个")
            return stats
        except Exception as e:
            logger.error(f"[Final监控] 写带收尾异常: {e}", exc_info=True)
            return stats
        finally:
            self._finalizing = False

    def _monitor_loop(self):
        """监控循环：扫描final目录，发现文件后顺序移动到磁带"""
        logger.info("[Final监控] ========== Final目录监控线程已启动 ==========")
        logger.info(f"[Final监控] 扫描间隔: {self._scan_interval}秒")
        
        try:
            while self._running:
                try:
                    # 收尾期间暂停轮询：由 finalize_pending_files 独占处理 final，
                    # 避免与本次写带/隔离产生重复写入或删除竞态。
                    if self._finalizing:
                        time.sleep(1)
                        continue
                    # 扫描final目录
                    final_dir = self._get_final_dir()
                    
                    if not final_dir.exists():
                        logger.debug(f"[Final监控] final目录不存在: {final_dir}，等待 {self._scan_interval} 秒后重试")
                        time.sleep(self._scan_interval)
                        continue
                    
                    # 递归扫描所有子目录，查找压缩文件
                    found_files = []
                    for root, dirs, files in os.walk(final_dir):
                        root_path = Path(root)
                        for file_name in files:
                            file_path = root_path / file_name
                            
                            if not file_path.is_file():
                                continue
                            
                            # 检查是否是压缩文件
                            if file_path.suffix in ['.7z', '.gz', '.tar', '.zst'] or file_path.name.endswith('.tar.gz'):
                                # 检查是否已处理过
                                file_key = str(file_path)
                                if file_key not in self._processed_files:
                                    found_files.append(file_path)
                    
                    # 每批内按文件大小降序（最大文件优先）：
                    #   · 大文件连续写保持磁带流式，减少文件间 sync/backhitch；
                    #   · 小文件最后填充剩余空间，降低"带尾大文件写不下"的跨带风险；
                    #   · 按批排序（而非全局队列），新到的大文件不会无限推迟小文件。
                    def _size_key(p: Path) -> int:
                        try:
                            return p.stat().st_size
                        except OSError:
                            return 0

                    found_files.sort(key=_size_key, reverse=True)

                    # 顺序处理找到的文件（移动完一个再移动下一个）
                    if found_files:
                        # 有文件可写了：结算"压缩喂不饱"的等待时间
                        if self._starve_start is not None:
                            self._starvation_seconds += max(0.0, time.time() - self._starve_start)
                            self._starve_start = None
                        logger.info(f"[Final监控] 扫描到 {len(found_files)} 个新文件待移动到磁带"
                                    f"（最大优先，首个 {format_bytes(_size_key(found_files[0]))}）")
                        
                        for file_path in found_files:
                            if not self._running:
                                break
                            
                            file_key = str(file_path)
                            
                            # 检查文件是否仍然存在
                            if not file_path.exists():
                                logger.debug(f"[Final监控] 文件已不存在，跳过: {file_path.name}")
                                self._processed_files.add(file_key)
                                continue
                            
                            # 移动文件到磁带
                            self._current_tape_file = str(file_path)
                            self._defer_current_file = False
                            success = self._move_file_to_tape(file_path)
                            self._current_tape_file = None

                            if self._defer_current_file:
                                # 文件仍在写入：不标记已处理，下一轮重新检查
                                logger.info(f"[Final监控] 文件仍在写入，留待下一轮: {file_path.name}")
                                continue

                            # 标记为已处理（无论成功与否，避免重复处理）
                            self._processed_files.add(file_key)
                            
                            if success:
                                self._has_written_once = True
                                logger.debug(f"[Final监控] 文件处理完成: {file_path.name}")

                            else:
                                logger.error(f"[Final监控] 文件处理失败: {file_path.name}")
                    else:
                        # 没有找到新文件：只有第一次写带之后才统计 final 空等时间
                        if self._has_written_once and self._starve_start is None:
                            self._starve_start = time.time()
                        # 等待后继续扫描
                        time.sleep(self._scan_interval)
                        
                except Exception as scan_error:
                    logger.error(f"[Final监控] 扫描final目录时发生错误: {str(scan_error)}", exc_info=True)
                    time.sleep(self._scan_interval)
                    
        except Exception as e:
            logger.error(f"[Final监控] 监控循环异常: {str(e)}", exc_info=True)
        finally:
            logger.info("[Final监控] Final目录监控线程已退出")
    
    def is_final_dir_empty(self, set_id: str = None) -> bool:
        """
        检查final目录是否为空（用于任务完成判断）

        Args:
            set_id: 备份集ID（如 "2026-03_000001"）。如果传入，只检查 final/{set_id}/ 子目录；
                    如果为 None，检查整个 final/ 目录。

        Returns:
            bool: final目录（指定set_id目录）是否为空
        """
        try:
            final_dir = self._get_final_dir()
            if set_id:
                final_dir = final_dir / set_id

            if not final_dir.exists():
                return True

            # 检查是否有压缩文件
            for root, dirs, files in os.walk(final_dir):
                for file_name in files:
                    file_path = Path(root) / file_name
                    if file_path.is_file():
                        # 检查是否是压缩文件
                        if file_path.suffix in ['.7z', '.gz', '.tar', '.zst'] or file_path.name.endswith('.tar.gz'):
                            return False

            return True
        except Exception as e:
            logger.error(f"[Final监控] 检查final目录是否为空时发生错误: {str(e)}")
            # 异常按"未空"处理（宁失败不冒险）：引擎会继续等待并最终超时判失败，
            # 而不是误判"全部写带完成"
            return False
    
    def get_processed_count(self) -> int:
        """获取已处理文件数量"""
        return len(self._processed_files)

    def count_final_files(self, set_id: str = None) -> int:
        """统计 final 目录中待写磁带的压缩文件数（含正在写入的那个）。

        与 is_final_dir_empty 相同的文件后缀口径；用于卡片显示"final 文件数"。
        """
        try:
            final_dir = self._get_final_dir()
            if set_id:
                final_dir = final_dir / set_id
            if not final_dir.exists():
                return 0
            count = 0
            for root, dirs, files in os.walk(final_dir):
                for file_name in files:
                    file_path = Path(root) / file_name
                    if file_path.is_file() and (
                        file_path.suffix in ['.7z', '.gz', '.tar', '.zst']
                        or file_path.name.endswith('.tar.gz')
                    ):
                        count += 1
            return count
        except Exception as e:
            logger.error(f"[Final监控] 统计final目录文件数时发生错误: {str(e)}")
            return 0

    def cleanup_final_dir(self) -> int:
        """停机时把 final 目录中未写带的文件移入隔离目录（不删除）

        只保留当前正在写入磁带的文件（由 FinalDirMonitor.stop() 等待其完成）。
        隔离而非删除：这些文件已经压缩完成，删除会直接丢失；停机后可由人工
        补齐磁带或由下次任务重试。

        Returns:
            int: 已隔离的文件数量
        """
        quarantined_count = 0
        try:
            final_dir = self._get_final_dir()
            if not final_dir.exists():
                return 0

            current_file = self._current_tape_file
            if current_file:
                logger.info(f"[Final监控] 清理final目录，保留当前写入文件: {current_file}")
            else:
                logger.info("[Final监控] 清理final目录（无正在写入的文件）")

            for root, dirs, files in os.walk(final_dir):
                for file_name in files:
                    file_path = Path(root) / file_name
                    if not file_path.is_file():
                        continue

                    # 保留正在写入磁带的文件
                    if current_file and str(file_path) == current_file:
                        logger.info(f"[Final监控] 保留正在写入的文件: {file_path}")
                        continue

                    try:
                        set_id = self._extract_backup_set_id_from_path(file_path)
                        qpath = self._quarantine_file(file_path, set_id)
                        if qpath:
                            quarantined_count += 1
                            logger.info(f"[Final监控] 已隔离: {file_path} -> {qpath}")
                    except Exception as e:
                        logger.warning(f"[Final监控] 隔离文件失败 {file_path}: {e}")

            # 清理空的 set_id 子目录
            if quarantined_count > 0:
                for item in final_dir.iterdir():
                    if item.is_dir():
                        try:
                            # 检查目录是否为空
                            has_files = any(
                                Path(root) / f
                                for root, _, files in os.walk(item)
                                for f in files
                                if (Path(root) / f).is_file()
                            )
                            if not has_files:
                                shutil.rmtree(item, ignore_errors=True)
                                logger.info(f"[Final监控] 已清理空目录: {item}")
                        except Exception:
                            pass

            logger.info(f"[Final监控] final目录清理完成，共隔离 {quarantined_count} 个文件")
        except Exception as e:
            logger.error(f"[Final监控] 清理final目录失败: {e}")

        return quarantined_count

    def cleanup_set_id_dir(self, set_id: str) -> bool:
        """清理指定set_id的空目录（任务完成后调用）

        在确认final目录中所有压缩文件已移动到磁带后，安全删除空的set_id子目录。
        会再次检查目录内是否有文件，防止与监控线程的竞态条件。

        Args:
            set_id: 备份集ID (e.g., "2026-03_000001")

        Returns:
            bool: 是否成功清理
        """
        try:
            final_dir = self._get_final_dir()
            set_id_dir = final_dir / set_id

            if not set_id_dir.exists():
                return True  # 已不存在

            if not set_id_dir.is_dir():
                logger.warning(f"[Final监控] set_id路径不是目录: {set_id_dir}")
                return False

            # 再次确认目录内没有文件（防止与监控线程的竞态）
            has_files = False
            for root, dirs, files in os.walk(set_id_dir):
                for f in files:
                    fp = Path(root) / f
                    if fp.is_file():
                        logger.warning(f"[Final监控] set_id目录仍有文件，跳过清理: {fp}")
                        has_files = True
                        break
                if has_files:
                    break

            if has_files:
                return False

            # 目录为空，安全删除
            shutil.rmtree(set_id_dir, ignore_errors=True)
            logger.info(f"[Final监控] 已清理空的set_id目录: {set_id_dir}")
            return True
        except Exception as e:
            logger.error(f"[Final监控] 清理set_id目录失败: {e}")
            return False



