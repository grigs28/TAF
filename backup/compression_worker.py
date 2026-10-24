#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
压缩工作线程
"""

import asyncio
import logging
import os
import shutil
import time
from typing import List, Dict, Optional, Any
import tempfile

from utils.scheduler.db_utils import is_opengauss
from config.settings import get_settings
from backup.utils import format_bytes

logger = logging.getLogger(__name__)


class CompressionWorker:
    """压缩工作线程"""

    def __init__(
        self,
        backup_db,
        compressor,
        backup_set,
        backup_task,
        settings,
        file_move_worker=None,
        backup_notifier=None,
        tape_file_mover=None,
        file_group_prefetcher=None,
        in_memory_store=None
    ):
        self.backup_task = backup_task
        self.settings = settings
        self.backup_set = backup_set
        self.backup_db = backup_db
        self.compressor = compressor
        self.file_move_worker = file_move_worker
        self.backup_notifier = backup_notifier
        self.tape_file_mover = tape_file_mover
        self.file_group_prefetcher = file_group_prefetcher
        self.in_memory_store = in_memory_store

        # 检查是否使用预取模式（openGauss模式且有预取器）
        self.use_prefetcher = is_opengauss() and file_group_prefetcher is not None
        logger.debug(f"[压缩循环] 初始化: is_opengauss={is_opengauss()}, file_group_prefetcher={file_group_prefetcher is not None}, use_prefetcher={self.use_prefetcher}")

        self.compression_task: Optional[asyncio.Task] = None
        self._running = False

        # 压缩循环统计
        self.processed_files = 0
        self.total_size = 0  # 压缩后的总大小
        self.total_original_size = 0  # 原始文件的总大小（未压缩）
        self.group_idx = 0  # 文件组索引
        # 失败记账：主循环异常退出 / 组压缩失败时置位，
        # 引擎四合一判定据此走 FAILED 而不是假成功
        self.loop_failed_reason: Optional[str] = None
        self.compression_group_failed = False
        self.failed_groups = 0
        self.failed_files_total = 0
        # 错误明细（供“错误日志”弹窗，任务结束时持久化到 result_summary）
        # 元素：{'category': 'compress', 'time': str, 'message': str, 'detail': str}
        self.error_logs: list = []
        # 压缩实际开始时间（epoch 秒）：卡片平均压缩速度以此为起点，
        # 不含前置扫描/等待时间
        self.compression_started_at: Optional[float] = None
        # ===== 自适应并发（计划05，Vegas 变体，信号=组级聚合读取速率） =====
        self._vegas_enabled = bool(getattr(settings, 'GROUP_VEGAS_ENABLED', True))
        # 信号源：internal=组级读取速率（默认，保守）；nas_api=79 disk_api 的
        # md2_read_mbps（bench 两轮 1588GB 验证：误判 5→0、产出 +8%，1.9.65）
        self._vegas_signal = str(getattr(settings, 'GROUP_VEGAS_SIGNAL_SOURCE',
                                         'internal') or 'internal').strip().lower()
        self._vegas_nas = None          # VegasNasController（nas_api 模式惰性创建）
        self._vegas_nas_sampler = None  # NasSignalSampler（任务级共享，见 _ensure_vegas_nas）
        self._vegas_alpha = float(getattr(settings, 'GROUP_VEGAS_ALPHA', 1.0) or 1.0)
        self._vegas_beta = float(getattr(settings, 'GROUP_VEGAS_BETA', 3.0) or 3.0)
        self._vegas_decay = float(getattr(settings, 'GROUP_VEGAS_DECAY', 0.7) or 0.7)
        self._vegas_min = int(getattr(settings, 'GROUP_VEGAS_MIN_BATCHES', 2) or 2)
        self._vegas_base_reset = int(getattr(settings, 'GROUP_VEGAS_BASE_RESET_SEC', 600) or 600)
        self._vegas_limit = 0            # 0 = 未干预（从未降过则不 clamp）
        # 分类型独立基准：L/S 每组速率天然差一个量级，混合平均会把 S 组慢误判为 NAS 拥塞
        self._vegas_states = {
            'large': {'ewma': 0.0, 'base': 0.0, 'base_at': 0.0},
            'small': {'ewma': 0.0, 'base': 0.0, 'base_at': 0.0},
            'buffer': {'ewma': 0.0, 'base': 0.0, 'base_at': 0.0},
        }
        self._vegas_rate_ewma = 0.0      # 展示值：按 active 数加权的综合 ewma（MB/s/组）
        self._vegas_base = 0.0           # 展示值：综合基准
        self._vegas_low_windows = 0      # 低并发仍低速率的连续窗口（基准重校准）
        self._vegas_hold_until = 0.0     # 快速闸：暂停发新组期限（epoch 秒）
        self._vegas_last_sample_at = 0.0  # 上次采样时间戳（真实窗口 = now - 此值）
        self._last_progress_bytes: Dict[int, int] = {}  # group_idx -> 上次采样 processed_bytes
        # 写带饿死护栏：final 长时间无文件可写时不得继续降并发（否则喂不饱写带）
        self.write_starved_provider = None
        # 首次填充完成标志：初始阶段每 3 秒启动一个组，填满后进入稳态（完成即补）
        self._initial_fill_done = False
        self._disk_throttled = False  # 磁盘限流状态（迟滞状态机）
        self._last_adjust_time = 0.0  # 上次并行批次调整时间（限流检查节流）
        self._disk_critical = False  # 磁盘空间危急降级状态

        # 并行批次设置（基础值，会在异步方法中根据扫描状态动态调整）
        if self.use_prefetcher:
            settings = get_settings()
            self.base_parallel_batches = getattr(settings, 'COMPRESSION_PARALLEL_BATCHES', 3)
            self.batches_reduction = getattr(settings, 'COMPRESSION_BATCHES_REDUCTION', 1)  # 扫描时减少的批次数
            self.parallel_batches = self.base_parallel_batches  # 初始值，会在异步方法中调整
        else:
            self.base_parallel_batches = 1
            self.batches_reduction = 0  # 非预取模式不减少
            self.parallel_batches = 1  # 非openGauss模式，顺序执行
        
        # 存储每个压缩任务的进度（用于实时查询）
        # 格式: {group_idx: {'current': int, 'total': int, 'percent': float, 'group_size_bytes': int, 'compress_progress': Dict}}
        # compress_progress 是压缩函数中使用的进度字典，可以直接读取
        self.compression_progress_map: Dict[int, Dict[str, Any]] = {}
        
        # 并发控制：跟踪正在运行的压缩任务
        self.running_compression_futures: List[asyncio.Task] = []

        # 向 Compressor 注入"在途组数"提供者 —— 空间守卫线据此**动态**计算：
        #   所需空间 = 在途组数 × MAX_FILE_SIZE × 安全系数
        # future 在 create_task 后立即 append（中间无 await），所以组内执行
        # 空间检查时"自己"已被计入，语义正好是"含本组"。
        if self.compressor is not None:
            try:
                self.compressor.active_groups_provider = (
                    lambda: len(self.running_compression_futures)
                )
            except Exception as inject_err:
                logger.warning(
                    f"注入在途组数提供者失败（守卫线将回退到配置并发上限）: {inject_err}"
                )
        
        # 压缩进度更新任务（定期从数据库查询聚合进度）
        self.progress_update_task: Optional[asyncio.Task] = None
        
        # openGauss 数据库统一调度器（异步批量更新压缩信息和内存数据库同步）
        if is_opengauss():
            from backup.compression_db_updater import OpenGaussDBScheduler
            self.db_updater = OpenGaussDBScheduler(
                backup_set_db_id=backup_set.id,
                batch_size=3000  # 每3000个文件批量更新一次
            )
        else:
            self.db_updater = None

    async def _get_scan_status(self) -> str:
        """获取扫描状态（优先从内存对象读取，避免数据库查询）

        扫描器在两种模式下都会设置 backup_task.scan_status 属性：
        - DB模式：simple_scanner.scan_and_write() 设置 backup_task.scan_status = "completed"
        - 内存模式：同上
        因此优先读取内存属性，减少数据库查询
        """
        # 优先从内存中的 backup_task 对象读取
        scan_status = getattr(self.backup_task, 'scan_status', None)
        if scan_status:
            return scan_status
        return await self.backup_db.get_scan_status(self.backup_task.id)

    def start(self):
        """启动压缩处理任务"""
        if self._running:
            return

        try:
            # 检查是否有运行中的事件循环
            try:
                loop = asyncio.get_running_loop()
            except RuntimeError:
                logger.error("❌ 没有运行中的事件循环，无法创建任务")
                raise

            # 启动数据库更新器（如果使用）
            if self.db_updater:
                self.db_updater.start()
            
            self.compression_task = asyncio.create_task(self._compression_loop())
            self._running = True
            logger.info("压缩处理任务已启动")
        except Exception as e:
            logger.error(f"❌ 启动压缩处理任务失败: {str(e)}", exc_info=True)
            raise

    async def stop(self):
        """停止压缩处理任务"""
        if not self._running:
            return

        logger.info("收到停止信号，正在停止压缩处理...")
        self._running = False

        # 取消所有正在运行的压缩任务
        if self.running_compression_futures:
            logger.debug(f"取消 {len(self.running_compression_futures)} 个正在运行的压缩任务...")
            for task in self.running_compression_futures:
                if not task.done():
                    task.cancel()
            # 等待所有任务完成（包括被取消的）
            if self.running_compression_futures:
                done, _ = await asyncio.wait(self.running_compression_futures, timeout=10.0)
                for task in done:
                    try:
                        await task
                    except asyncio.CancelledError:
                        logger.debug("压缩任务已被取消")
                    except Exception as e:
                        logger.error(f"压缩任务异常: {str(e)}", exc_info=True)
            self.running_compression_futures.clear()

        if self.compression_task:
            try:
                # 取消主压缩循环任务
                if not self.compression_task.done():
                    self.compression_task.cancel()
                await self.compression_task
                logger.info("压缩处理任务已自然结束")
            except asyncio.CancelledError:
                logger.info("压缩处理任务被取消")
            except KeyboardInterrupt:
                logger.warning("压缩处理收到KeyboardInterrupt")
                raise
            except Exception as e:
                logger.error(f"压缩处理任务异常: {str(e)}", exc_info=True)

        # 停止数据库更新器（处理剩余文件）
        if self.db_updater:
            logger.debug("[压缩循环] 停止数据库更新器，处理剩余文件...")
            await self.db_updater.stop()

        # 强制清理残留的 tar/zstd 子进程。
        # 上面的 cancel() 只作用于 asyncio 任务；真正干活的 tar/zstd 是进程池里
        # spawn 出来的子进程，取消 future 不会杀掉它们（它们跑在同步函数里，
        # 既收不到 CancelledError，也走不到自身的 except），于是成为孤儿——
        # 继续从 NAS 读数据、继续写磁盘。实测任务取消后仍残留 2~7 个。
        try:
            from backup.utils import kill_compression_processes
            kill_compression_processes(reason="压缩停止")
        except Exception as kill_err:
            logger.warning(f"清理残留压缩子进程失败（可能有孤儿进程）: {kill_err}")

        logger.info("压缩处理任务已停止")

    async def _compression_loop(self):
        """简化的压缩处理"""
        logger.info("========== 压缩处理开始 ==========")

        if not self.backup_set.id:
            logger.error(f"⚠️⚠️ 错误：backup_set.id 为空！")
            return

        try:
            # 启动压缩进度更新任务（定期从数据库查询聚合进度）
            self.progress_update_task = asyncio.create_task(
                self._update_compression_progress_periodically()
            )
            
            if self.use_prefetcher:
                await self._process_prefetched_file_groups()
            else:
                await self._process_database_file_groups()
        except Exception as e:
            logger.error(f"❌ 压缩处理异常: {str(e)}", exc_info=True)
            raise
        finally:
            # 停止进度更新任务
            if self.progress_update_task:
                self.progress_update_task.cancel()
                try:
                    await self.progress_update_task
                except asyncio.CancelledError:
                    pass
                logger.debug("压缩进度更新任务已停止")
    
    async def _adjust_parallel_batches(self):
        """根据扫描状态和磁盘使用率调整并行批次数量

        策略：
        1. 扫描阶段减少同时运行的压缩任务数量，扫描结束后恢复正常
        2. 磁盘使用率超过 DISK_THROTTLE_HIGH_PERCENT 时批次减半，
           回落到 DISK_THROTTLE_LOW_PERCENT 以下时直接恢复（迟滞区间防抖）
        每次调用时重新读取 settings，以支持运行时动态修改配置
        """
        try:
            # 重新读取 settings，支持运行时动态修改（立即生效）
            fresh_settings = get_settings()
            if self.use_prefetcher:
                self.base_parallel_batches = getattr(fresh_settings, 'COMPRESSION_PARALLEL_BATCHES', 3)
                self.batches_reduction = getattr(fresh_settings, 'COMPRESSION_BATCHES_REDUCTION', 1)
                # Vegas 开关与参数热生效（UI 保存后当前任务也立即遵循）
                try:
                    _was_vegas_enabled = self._vegas_enabled
                    self._vegas_enabled = bool(
                        getattr(fresh_settings, 'GROUP_VEGAS_ENABLED', self._vegas_enabled))
                    if _was_vegas_enabled and not self._vegas_enabled:
                        # 关闭自适应：清除已下调的并发上限与快速闸，立即回到配置值；
                        # 否则重新开启时会立刻被旧 limit 夹住，看起来像“开关无效”
                        self._vegas_limit = 0
                        self._vegas_hold_until = 0.0
                        self._vegas_low_windows = 0
                        # 展示值重置：重开前卡片显示“无基准”，避免旧健康度误导
                        self._vegas_rate_ewma = 0.0
                        self._vegas_base = 0.0
                    self._vegas_beta = float(
                        getattr(fresh_settings, 'GROUP_VEGAS_BETA', self._vegas_beta) or self._vegas_beta)
                    self._vegas_alpha = float(
                        getattr(fresh_settings, 'GROUP_VEGAS_ALPHA', self._vegas_alpha) or self._vegas_alpha)
                    self._vegas_decay = float(
                        getattr(fresh_settings, 'GROUP_VEGAS_DECAY', self._vegas_decay) or self._vegas_decay)
                    _vm = int(getattr(fresh_settings, 'GROUP_VEGAS_MIN_BATCHES', self._vegas_min) or self._vegas_min)
                    self._vegas_min = max(1, _vm)
                    _was_sig = self._vegas_signal
                    self._vegas_signal = str(
                        getattr(fresh_settings, 'GROUP_VEGAS_SIGNAL_SOURCE',
                                self._vegas_signal) or self._vegas_signal).strip().lower()
                    if _was_sig != self._vegas_signal:
                        # 信号源热切换：停旧采样线程、清控制器，下轮 _ensure 重建
                        logger.info(f"[压缩配置] Vegas 信号源切换: {_was_sig} -> {self._vegas_signal}")
                        if self._vegas_nas_sampler is not None:
                            try:
                                self._vegas_nas_sampler.stop()
                            except Exception:
                                pass
                        self._vegas_nas_sampler = None
                        self._vegas_nas = None
                        self._vegas_limit = 0
                except Exception:
                    pass

            # 优先从内存对象获取，如果没有则从数据库查询
            scan_status = getattr(self.backup_task, "scan_status", None)
            if not scan_status and self.backup_task.id:
                try:
                    scan_status = await self._get_scan_status()
                except Exception:
                    pass

            # 1. 计算扫描态下的正常批次数量
            if scan_status not in (None, "completed"):
                normal_batches = max(0, self.base_parallel_batches - self.batches_reduction)
            else:
                normal_batches = self.base_parallel_batches

            # 2. 磁盘限流状态机（high=0 禁用；恢复为直接恢复）
            high = float(getattr(fresh_settings, 'DISK_THROTTLE_HIGH_PERCENT', 75) or 0)
            low = float(getattr(fresh_settings, 'DISK_THROTTLE_LOW_PERCENT', 30) or 0)
            critical_batches = int(getattr(fresh_settings, 'DISK_CRITICAL_BATCHES', 1) or 0)
            # 总开关关闭：百分比限流与危急层降级都不生效（配置的数值保留，重开即恢复）
            if not bool(getattr(fresh_settings, 'DISK_THROTTLE_ENABLED', True)):
                high = 0
                critical_batches = 0
            desired_batches = normal_batches
            if normal_batches >= 1 and (high > 0 or critical_batches >= 1):
                compress_dir = getattr(fresh_settings, 'BACKUP_COMPRESS_DIR', 'temp/compress')
                try:
                    usage = shutil.disk_usage(compress_dir)
                    used_percent = usage.used / usage.total * 100 if usage.total > 0 else 0
                    free_bytes = usage.free
                except Exception as disk_err:
                    used_percent = None
                    free_bytes = None
                    logger.debug(f"[压缩配置] 获取磁盘使用率失败（跳过本次限流判断）: {disk_err}")

                if used_percent is not None:
                    if not self._disk_throttled and used_percent >= high and high > 0:
                        self._disk_throttled = True
                        logger.warning(
                            f"[压缩配置] 磁盘使用率 {used_percent:.1f}% 达到限流上限 {high}%，"
                            f"并行批次将减半"
                        )
                    elif self._disk_throttled and used_percent <= low:
                        self._disk_throttled = False
                        logger.info(
                            f"[压缩配置] 磁盘使用率 {used_percent:.1f}% 回落至恢复阈值 {low}%，"
                            f"并行批次将直接恢复"
                        )

                    if self._disk_throttled and high > 0:
                        desired_batches = max(1, normal_batches // 2)

                    # 2b. 空间危急层：与空间守卫同口径（剩余待产出，见
                    # compressor.compute_required_free），两条判定线保持一致
                    if critical_batches >= 1:
                        required_free = 0
                        if self.compressor is not None and hasattr(self.compressor, 'compute_required_free'):
                            try:
                                required_free, _crf_detail = self.compressor.compute_required_free()
                            except Exception:
                                required_free = 0
                        if required_free <= 0:
                            # 无压缩机实例时回退旧保守口径
                            from backup.compressor import _compute_required_free_bytes
                            required_free = _compute_required_free_bytes(
                                fresh_settings, len(self.running_compression_futures) or None
                            )
                        if required_free > 0 and free_bytes < required_free:
                            if not self._disk_critical:
                                logger.warning(
                                    f"[压缩配置] 磁盘剩余空间危急（{format_bytes(free_bytes)} < "
                                    f"{format_bytes(required_free)}[守卫线·剩余待产出]），"
                                    f"并行批次强制降为 {critical_batches}"
                                )
                            self._disk_critical = True
                            desired_batches = min(desired_batches, critical_batches)
                        else:
                            if self._disk_critical:
                                logger.info(f"[压缩配置] 磁盘剩余空间恢复（{format_bytes(free_bytes)}），危急降级解除")
                            self._disk_critical = False
                    else:
                        self._disk_critical = False
            elif high <= 0:
                self._disk_throttled = False
                self._disk_critical = False

            # 3b. 自适应并发（计划05 Vegas 变体）：NAS 拥塞感知，
            #     信号源二选一：nas_api（79 read，v2 状态机）/ internal（组级速率 deficit）
            #     SSD 水位优先级更高（两者都只 clamp 到 desired 之下）
            if self._vegas_enabled and self.use_prefetcher:
                if self._vegas_signal == 'nas_api':
                    ctrl = self._ensure_vegas_nas()
                    if ctrl is not None:
                        nas_limit = ctrl.tick(len(self.running_compression_futures))
                        if nas_limit > 0 and desired_batches > nas_limit:
                            desired_batches = nas_limit
                    else:
                        # NAS 信号不可用 → 回退内部信号（不因 API 挂掉停摆）
                        self._update_vegas()
                        if self._vegas_limit > 0 and desired_batches > self._vegas_limit:
                            desired_batches = self._vegas_limit
                else:
                    self._update_vegas()
                    if self._vegas_limit > 0 and desired_batches > self._vegas_limit:
                        desired_batches = self._vegas_limit

            # 3. 应用并记录变化（避免每轮重复刷日志）
            if desired_batches != self.parallel_batches:
                if self._disk_throttled:
                    logger.warning(
                        f"[压缩配置] 磁盘限流中：并行批次 {self.parallel_batches} -> {desired_batches} "
                        f"(正常值 {normal_batches}，扫描状态 scan_status={scan_status})"
                    )
                else:
                    logger.info(
                        f"[压缩配置] 并行批次 {self.parallel_batches} -> {desired_batches} "
                        f"(扫描状态 scan_status={scan_status}，磁盘限流已解除)"
                    )
                self.parallel_batches = desired_batches
        except Exception as e:
            # 即使无法获取 scan_status，也不影响压缩主流程
            logger.debug(f"[压缩配置] 获取 scan_status 时出错，使用默认配置: {e}")

    def _group_type_of(self, file_count: int) -> str:
        """按组内文件数三分：B（保护区）/ L（大文件组）/ S（小文件组），热读配置"""
        try:
            from config.settings import get_settings as _gs
            _s = _gs()
            defer_below = int(getattr(_s, 'GROUP_SCAN_DEFER_BELOW', 1000) or 0)
            thr = int(getattr(_s, 'GROUP_LARGE_FILE_COUNT_THRESHOLD', 20000) or 20000)
        except Exception:
            defer_below, thr = 1000, 20000
        if defer_below > 0 and file_count < defer_below:
            return 'buffer'
        return 'small' if file_count >= thr else 'large'

    def _ensure_vegas_nas(self):
        """nas_api 信号源惰性初始化（URL 为空/导入失败 → 返回 None 回退 internal）"""
        if self._vegas_nas is not None:
            return self._vegas_nas
        try:
            from config.settings import get_settings
            from backup.vegas_nas import NasSignalSampler, VegasNasController
            s = get_settings()
            url = str(getattr(s, 'NAS_API_URL', '') or '').strip()
            key = str(getattr(s, 'NAS_API_KEY', '') or '').strip()
            if not url:
                logger.warning("[VegasNAS] NAS_API_URL 未配置，回退内部信号")
                return None
            base = max(1, self.base_parallel_batches)
            sampler = NasSignalSampler(url, key)
            sampler.start()
            self._vegas_nas_sampler = sampler
            self._vegas_nas = VegasNasController(
                sampler,
                start_n=base,                 # 从配置并发起步自主调节
                min_n=max(2, self._vegas_min),
                max_n=max(base, 8),           # bench 标定：两形态区最优 5~6，8 封顶
                log=lambda m: logger.info(m),
            )
            logger.info(f"[VegasNAS] 启用 nas_api 信号源: {url[:48]}… "
                        f"(start={base}, min={self._vegas_nas.min_n}, max={self._vegas_nas.max_n})")
            return self._vegas_nas
        except Exception as e:
            logger.warning(f"[VegasNAS] 初始化失败，回退内部信号: {e}")
            return None

    def _update_vegas(self) -> None:
        """自适应并发（Vegas 变体，分类型基准）：组级聚合读取速率 → 积压 → 调并发。

        信号 = 窗口内"有进展在途组"的 processed_bytes 平均增量（MB/s/组）。
        按 L/S **分别维护 ewma 与基准**：小文件组每组速率天然只有大文件组的
        1/5~1/3，混合平均会把"切到 S 组"误判成 NAS 拥塞（实测因此一度降到
        4 并发并卡住）。决策取两类 deficit 的最大值；某类样本 < 2 组时不更新
        （单组噪声大）。

          ewma  = 0.3×当前 + 0.7×历史（首样本直接取值）
          base  = 滑动最高 ewma；每 BASE_RESET_SEC 允许 base×0.9 缓降
          deficit = 并发 × (1 − ewma/base)   # 等价 Vegas 队列积压
        deficit > β  → 并发 ×DECAY；> 2β 再加快速闸（暂停发新组一个窗口）
        deficit < α  → 并发 +1 缓慢上探
        护栏：
          · 写带饿死（final 长时间无文件可写）时不降并发——速率低可能只是
            小文件组本身慢，继续降会让写带更饿
          · 已在低并发（≤min+2）仍低速率且连续 3 窗口 → 基准虚高（一次异常
            高点/小文件区），把 base 重校准到 ewma×1.1，避免永久卡在低并发
        窗口时长用真实时间差（快速闸/守卫等待会把节拍拉长到分钟级）。
        """
        now = time.time()
        dt = max(1.0, (now - self._vegas_last_sample_at)) if self._vegas_last_sample_at > 0 else 10.0
        self._vegas_last_sample_at = now

        totals = {'large': [0, 0], 'small': [0, 0], 'buffer': [0, 0]}  # typ -> [delta, active]
        for gidx, info in list(self.compression_progress_map.items()):
            cp = info.get('compress_progress') if isinstance(info, dict) else None
            if not cp:
                continue
            pb = int(cp.get('processed_bytes', 0) or 0)
            prev = self._last_progress_bytes.get(gidx)
            self._last_progress_bytes[gidx] = pb
            if prev is not None and pb > prev:
                typ = info.get('group_type') or 'large'
                if typ not in totals:
                    typ = 'large'
                totals[typ][0] += pb - prev
                totals[typ][1] += 1
        # 已完成组清理
        for g in [g for g in self._last_progress_bytes
                  if g not in self.compression_progress_map]:
            self._last_progress_bytes.pop(g, None)

        cur_limit = self._vegas_limit or max(1, self.base_parallel_batches)

        def _tick(typ: str):
            delta, act = totals[typ]
            st = self._vegas_states[typ]
            if act < 2:
                # 低并发时放宽样本门槛：并发已降到最低附近时，每类往往只有
                # 1 个在途组；若仍要求 act>=2，三类全部无信号 → 永远无法上探
                # 或触发“低并发重校准”，会卡死在最低并发。
                if not (cur_limit <= self._vegas_min + 2 and act >= 1):
                    return None
            rate = delta / dt / act / (1024 * 1024)  # 真实窗口，MB/s/组
            st['ewma'] = (rate if st['ewma'] <= 0
                          else 0.3 * rate + 0.7 * st['ewma'])
            if st['ewma'] > st['base']:
                st['base'] = st['ewma']
                st['base_at'] = now
            elif st['base'] > 0 and now - st['base_at'] >= self._vegas_base_reset:
                st['base'] = max(st['base'] * 0.9, st['ewma'])
                st['base_at'] = now
            if st['base'] <= 0 or st['ewma'] <= 0:
                return None
            return cur_limit * (1.0 - st['ewma'] / st['base'])

        dl = _tick('large')
        ds = _tick('small')
        db = _tick('buffer')

        # 展示值（API 健康度灯）：按 active 数加权；无样本类型不参与
        den = 0
        num_r = 0.0
        num_b = 0.0
        for typ in ('large', 'small', 'buffer'):
            act = totals[typ][1]
            st = self._vegas_states[typ]
            if act > 0 and st['base'] > 0 and st['ewma'] > 0:
                den += act
                num_r += st['ewma'] * act
                num_b += st['base'] * act
        if den > 0:
            self._vegas_rate_ewma = num_r / den
            self._vegas_base = num_b / den

        ds_all = [d for d in (dl, ds, db) if d is not None]
        if not ds_all:
            return
        deficit = max(ds_all)

        starved = False
        try:
            if callable(self.write_starved_provider):
                starved = bool(self.write_starved_provider())
        except Exception:
            starved = False

        note = ''
        if deficit > self._vegas_beta:
            new_limit = max(self._vegas_min, int(cur_limit * self._vegas_decay))
            if starved and cur_limit <= self.base_parallel_batches:
                # 写带已饿死：任何并发水平下都先不降（速率低可能只是小文件组慢）
                self._vegas_low_windows = 0
                note = '写带饿死，暂不降并发'
            elif (new_limit <= self._vegas_min
                  and cur_limit <= self._vegas_min + 2):
                # 已接近下限仍低速率：基准可能虚高，连续 3 窗口后重校准
                self._vegas_low_windows += 1
                if self._vegas_low_windows >= 3:
                    for st in self._vegas_states.values():
                        if st['base'] > 0 and st['ewma'] > 0:
                            st['base'] = max(st['ewma'] * 1.1, 1.0)
                    self._vegas_low_windows = 0
                    note = '低并发仍低速率，重校准基准'
                else:
                    note = (f'低并发仍低速率（{self._vegas_low_windows}/3），'
                            f'暂不再降')
            elif new_limit < cur_limit:
                self._vegas_low_windows = 0
                self._vegas_limit = new_limit
                note = (f"速率跌至基准 {self._vegas_rate_ewma / self._vegas_base * 100:.0f}%，"
                        f"并发×{self._vegas_decay}→{new_limit}")
            else:
                self._vegas_low_windows = 0
            if deficit > 2 * self._vegas_beta and not starved:
                self._vegas_hold_until = now + 10.0
                note += "，触发快速闸（暂停发新组一个窗口）"
        elif deficit < self._vegas_alpha and 0 < self._vegas_limit < self.base_parallel_batches:
            self._vegas_low_windows = 0
            up = min(self.base_parallel_batches, self._vegas_limit + 1)
            if up > self._vegas_limit:
                self._vegas_limit = up
                note = f"速率充分，并发上探→{up}"
        else:
            # 正常窗口（deficit 处于 α~β 之间，或已在上限）：连续计数中断
            self._vegas_low_windows = 0

        if note:
            _sl = self._vegas_states['large']
            _ss = self._vegas_states['small']
            _sb = self._vegas_states['buffer']
            logger.info(
                f"[自适应并发] {note}"
                f"（L {_sl['ewma']:.1f}/{_sl['base']:.1f}，"
                f"S {_ss['ewma']:.1f}/{_ss['base']:.1f}，"
                f"B {_sb['ewma']:.1f}/{_sb['base']:.1f} MB/s/组，"
                f"deficit {deficit:.1f}"
                + ("，写带饿死" if starved else "") + "）")

    async def _process_prefetched_file_groups(self):
        """处理预取的文件组（openGauss模式）- 并发控制"""
        # 压缩循环真正启动的时刻（卡片"平均压缩速度"的计时起点；
        # 也是紧急判定（final≤1/在途无L）的生效起点——
        # 启动初期 final 必空、在途类型未登记，两个条件恒真会误触发，
        # 前 60 秒跳过紧急判定，走正常配额）
        self.compression_started_at = time.time()
        # 根据扫描状态动态调整并行批次数量
        if self.use_prefetcher:
            await self._adjust_parallel_batches()
        logger.info(f"开始处理预取的文件组（并发控制模式，parallel_batches={self.parallel_batches}）")

        while self._running:
            try:
                # 清理已完成但残留在列表中的 task：列表只在"并发满"分支清理，
                # 收尾阶段并发不满时 done-task 残留会让 all_compression_done
                # （len==0）判据失真，退出判定永远不满足（任务 149 卡死根因之一）
                if self.running_compression_futures:
                    _stale = [t for t in self.running_compression_futures if t.done()]
                    if _stale:
                        for task in _stale:
                            self.running_compression_futures.remove(task)
                            try:
                                await task  # 消费异常，避免 "never retrieved"
                            except Exception as e:
                                logger.error(f"压缩任务异常: {str(e)}", exc_info=True)

                # 定期（每10秒）检查扫描状态与磁盘使用率，动态调整并行批次数
                # 注意：不能用 group_idx % N 作条件——重积压时 group_idx 长时间不变，
                # 会导致限流检查被跳过（磁盘涨到 85% 也未触发限流）
                if self.use_prefetcher and (time.time() - self._last_adjust_time) >= 10.0:
                    self._last_adjust_time = time.time()
                    await self._adjust_parallel_batches()

                # Vegas 快速闸：deficit 极端超标时暂停发新组一个窗口——
                # 在途组自然完成收缩，等效提前一个组周期降压（不中断任何组）
                if self._vegas_enabled and self._vegas_hold_until > time.time():
                    if self.running_compression_futures:
                        done, _ = await asyncio.wait(
                            self.running_compression_futures,
                            return_when=asyncio.FIRST_COMPLETED
                        )
                        for task in done:
                            if task in self.running_compression_futures:
                                self.running_compression_futures.remove(task)
                            try:
                                await task
                            except Exception as e:
                                logger.error(f"压缩任务异常: {str(e)}", exc_info=True)
                    else:
                        await asyncio.sleep(2.0)
                    continue

                # 并发控制：如果达到并行限制，等待部分任务完成
                if len(self.running_compression_futures) >= self.parallel_batches:
                    logger.debug(f"达到并行限制 ({self.parallel_batches})，等待部分任务完成...")
                    # 等待至少一个任务完成
                    done, pending = await asyncio.wait(
                        self.running_compression_futures,
                        return_when=asyncio.FIRST_COMPLETED
                    )
                    # 移除已完成的任务并处理异常
                    for task in done:
                        if task in self.running_compression_futures:
                            self.running_compression_futures.remove(task)
                        try:
                            await task  # 获取任务结果（如果有异常会抛出）
                        except Exception as e:
                            logger.error(f"压缩任务异常: {str(e)}", exc_info=True)
                    logger.debug(f"当前运行中的压缩任务数: {len(self.running_compression_futures)}/{self.parallel_batches}")
                    # 关键：必须回顶部重新判断是否仍达到上限。
                    # 否则"运行中配置调小并发"时，完成一个组后会立刻补一个新组，
                    # 运行数永远回到原值，并发只能升不能降（增加立即生效、减少不生效）。
                    continue

                # 获取文件组（消费队列）
                result = await self.file_group_prefetcher.get_file_group(timeout=2.0)

                if result is None:
                    # 在等待文件组时，也检查扫描状态并调整并行批次数量
                    if self.use_prefetcher:
                        await self._adjust_parallel_batches()
                    
                    # 关键修复：先检查队列是否还有文件组，如果有则继续处理
                    queue_size = self.file_group_prefetcher.get_queue_size() if self.file_group_prefetcher else 0
                    if queue_size > 0:
                        logger.debug(
                            f"队列中还有 {queue_size} 个文件组未处理，继续处理..."
                        )
                        # 继续循环，处理队列中的文件组
                        continue
                    
                    # 检查预取器是否还在运行（如果还在运行，即使 get_file_group 返回 None，也应该继续等待）
                    prefetch_running = getattr(self.file_group_prefetcher, '_running', False) if self.file_group_prefetcher else False
                    if prefetch_running:
                        logger.debug(
                            f"预取器仍在运行，继续等待文件组...（当前运行中的任务数: {len(self.running_compression_futures)}/{self.parallel_batches}）"
                        )
                        await asyncio.sleep(2)
                        continue
                    
                    # 检查停止条件：扫描完成 + 文件组预取无内容 + 队列为空 + 预取器已停止 + 所有压缩文件压缩完成 + 预取器执行次数 > 0
                    scan_status = await self._get_scan_status()
                    prefetch_loop_count = getattr(self.file_group_prefetcher, 'prefetch_loop_count', 0) if self.file_group_prefetcher else 0
                    is_rescanning = getattr(self.file_group_prefetcher, 'is_rescanning_missing_files', False) if self.file_group_prefetcher else False
                    
                    # 检查是否满足停止条件
                    scan_completed = scan_status == 'completed'
                    prefetch_no_content = result is None  # 预取器无内容
                    queue_empty = queue_size == 0  # 队列为空
                    all_compression_done = len(self.running_compression_futures) == 0  # 所有压缩任务完成
                    prefetch_executed = prefetch_loop_count > 0  # 预取器执行次数 > 0
                    
                    # 如果预取器正在重新检索遗漏文件，继续等待
                    if is_rescanning:
                        logger.debug(
                            f"预取器正在重新检索遗漏文件，继续等待...（当前运行中的任务数: {len(self.running_compression_futures)}/{self.parallel_batches}）"
                        )
                        await asyncio.sleep(2)
                        continue
                    
                    if not scan_completed:
                        logger.debug(
                            f"无更多文件组，但扫描未完成（状态={scan_status}），"
                            f"继续等待...（当前运行中的任务数: {len(self.running_compression_futures)}/{self.parallel_batches}，"
                            f"预取器执行次数: {prefetch_loop_count}）"
                        )
                        # 扫描未完成时，不清空正在运行的任务，让它们继续执行
                        # 只等待一段时间，让预取器有时间预取更多文件组
                        await asyncio.sleep(2)
                        continue
                    elif not prefetch_executed:
                        logger.debug(
                            f"扫描已完成，但预取器执行次数为 {prefetch_loop_count}（需要 > 0），"
                            f"继续等待...（当前运行中的任务数: {len(self.running_compression_futures)}/{self.parallel_batches}）"
                        )
                        await asyncio.sleep(2)
                        continue
                    elif not all_compression_done:
                        # 扫描已完成，预取器已执行，但还有正在运行的压缩任务，等待它们完成
                        # （带超时：列表混入僵尸 future 时不再永久挂起——任务 149 卡死点）
                        logger.info(
                            f"扫描已完成，预取器执行次数: {prefetch_loop_count}，"
                            f"等待 {len(self.running_compression_futures)} 个正在运行的压缩任务完成..."
                        )
                        done, pending = await asyncio.wait(
                            self.running_compression_futures, timeout=600)
                        for task in done:
                            try:
                                await task
                            except Exception as e:
                                logger.error(f"压缩任务异常: {str(e)}", exc_info=True)
                        if pending:
                            logger.warning(
                                f"[压缩循环] {len(pending)} 个压缩任务 600 秒未完成，"
                                f"强制取消（数据已写带的组不受影响）")
                            for task in pending:
                                task.cancel()
                            await asyncio.wait(pending, timeout=10)
                        self.running_compression_futures.clear()
                        # 继续检查，确保所有条件都满足
                        continue
                    elif not queue_empty:
                        # 队列不为空，继续处理
                        logger.debug(
                            f"队列中还有 {queue_size} 个文件组未处理，继续处理..."
                        )
                        continue
                    else:
                        # 所有条件都满足：扫描完成 + 预取器执行次数 > 0 + 队列为空 + 预取器已停止 + 所有压缩任务完成
                        logger.info(
                            f"✅ 满足停止条件：扫描完成={scan_completed}，"
                            f"预取器执行次数={prefetch_loop_count}，队列为空={queue_empty}，预取器已停止={not prefetch_running}，所有压缩任务完成={all_compression_done}"
                        )
                        # 在内存中设置压缩完成状态
                        # 设置压缩完成标志（动态属性，无需 hasattr 检查）
                        self.backup_task.compression_completed = True
                        logger.info("所有文件组处理完成，退出压缩循环")
                        break

                if isinstance(result, tuple) and len(result) == 2:
                    # 预取器返回格式：(file_groups, last_processed_id)
                    # file_groups 是一个列表，通常是 [[file1, file2, ..., fileN]]
                    file_groups, last_processed_id = result
                    # 检查是否是结束信号（空文件组且 last_processed_id == -1）
                    if not file_groups and last_processed_id == -1:
                        # 检查预取器执行次数
                        prefetch_loop_count = getattr(self.file_group_prefetcher, 'prefetch_loop_count', 0) if self.file_group_prefetcher else 0
                        if prefetch_loop_count <= 0:
                            logger.debug(
                                f"收到结束信号，但预取器执行次数为 {prefetch_loop_count}（需要 > 0），"
                                f"继续等待..."
                            )
                            await asyncio.sleep(2)
                            continue
                        
                        # 等待所有正在运行的任务完成（带超时：僵尸 future 不再永久挂起，
                        # 任务 149 曾因此卡死；数据已写带的组取消收尾不丢数据）
                        if self.running_compression_futures:
                            logger.info(f"收到结束信号，等待 {len(self.running_compression_futures)} 个正在运行的压缩任务完成...")
                            done, pending = await asyncio.wait(
                                self.running_compression_futures, timeout=600)
                            for task in done:
                                try:
                                    await task
                                except Exception as e:
                                    logger.error(f"压缩任务异常: {str(e)}", exc_info=True)
                            if pending:
                                logger.warning(
                                    f"[压缩循环] 结束信号分支：{len(pending)} 个压缩任务 "
                                    f"600 秒未完成，强制取消（数据已写带的组不受影响）")
                                for task in pending:
                                    task.cancel()
                                await asyncio.wait(pending, timeout=10)
                            self.running_compression_futures.clear()
                        
                        # 在内存中设置压缩完成状态
                        # 设置压缩完成标志（动态属性，无需 hasattr 检查）
                        self.backup_task.compression_completed = True
                        logger.info("收到预取器结束信号，退出压缩循环")
                        break
                    
                    # 取第一个文件组（通常只有一个文件组）
                    if file_groups and len(file_groups) > 0:
                        file_group = file_groups[0]
                        logger.debug(f"[压缩循环] 从预取器获取文件组：{len(file_groups)} 个文件组，第一个文件组包含 {len(file_group)} 个文件")
                    else:
                        logger.warning("预取器返回的文件组列表为空，跳过")
                        continue
                    current_group_idx = self.group_idx
                else:
                    # 兼容旧格式（非元组）
                    logger.warning(f"收到非标准格式的文件组：{type(result)}，尝试直接使用")
                    file_group = result if isinstance(result, list) else []
                    current_group_idx = self.group_idx

                # 计算文件组总大小
                total_group_size = sum(f.get('size', 0) or f.get('file_size', 0) or 0 for f in file_group)
                
                # 创建压缩进度跟踪字典（与压缩函数中的compress_progress共享）
                # 这个字典会被压缩函数实时更新，我们可以从外部读取实际进度
                compress_progress = {
                    'bytes_written': 0,
                    'running': True,
                    'completed': False,
                    'current_file_index': 0,  # 初始化为0，压缩过程中会更新
                    'total_files_in_group': len(file_group),  # 文件组总文件数（固定值）
                    'processed_bytes': 0  # 已处理文件的实际大小总和（用于按文件大小计算百分比）
                }
                
                # 初始化进度映射，存储文件组的基本信息和共享的进度字典
                self.compression_progress_map[current_group_idx] = {
                    'compress_progress': compress_progress,  # 共享的进度字典，压缩函数会实时更新
                    'total_files': len(file_group),  # 文件组总文件数
                    'group_type': self._group_type_of(len(file_group)),  # L/S（供分类型速率）
                    'group_size_bytes': total_group_size,  # 文件组总大小
                    # 组内文件路径（卡片"当前目录"用：按 current_file_index 取近似当前文件）
                    # 注意：此处只有 file_group（预取器入队记录，可能已精简为 path/size），
                    # processed_file_group 是 _compress_file_group 的局部变量，不能用。
                    'file_paths': [(f.get('path') or f.get('file_path') or '')
                                   for f in file_group],
                }
                
                # 检查是否应该停止（在启动新任务前）
                if not self._running:
                    logger.debug("收到停止信号，不再启动新的压缩任务")
                    break
                
                # 启动压缩任务（并发执行，但每个任务内部顺序执行：压缩 → 标注 → 移动）
                compression_task = asyncio.create_task(
                    self._compress_file_group(file_group, current_group_idx, compress_progress)
                )
                self.running_compression_futures.append(compression_task)
                logger.debug(
                    f"[压缩循环] ✅ 启动压缩任务 #{current_group_idx + 1}，"
                    f"当前运行中的任务数: {len(self.running_compression_futures)}/{self.parallel_batches}, "
                    f"队列大小: {self.file_group_prefetcher.get_queue_size()}/{self.file_group_prefetcher.queue_maxsize}"
                )

                self.group_idx += 1
                
                # 在启动任务后，也检查扫描状态并调整并行批次数量（确保及时恢复）
                if self.use_prefetcher:
                    await self._adjust_parallel_batches()
                
                # 启动节奏：只有**首次填充**阶段每 3 秒启动一个（避免瞬间拉起大量
                # zstd/tar、抢队列）；填满并发后进入稳态，组完成即补、不再等待。
                if not self._initial_fill_done:
                    if len(self.running_compression_futures) >= self.parallel_batches:
                        self._initial_fill_done = True
                    else:
                        await asyncio.sleep(3.0)
                # 稳态：不 sleep，直接回循环顶部（达到上限会等组完成）

            except asyncio.TimeoutError:
                # 超时时检查扫描状态，如果扫描未完成，继续等待
                # 同时检查并调整并行批次数量
                if self.use_prefetcher:
                    await self._adjust_parallel_batches()
                scan_status = await self._get_scan_status()
                if scan_status != 'completed':
                    logger.debug(f"获取文件组超时，但扫描未完成（状态={scan_status}），继续等待...")
                    # 如果有正在运行的任务，等待它们完成
                    if self.running_compression_futures:
                        logger.debug(f"等待 {len(self.running_compression_futures)} 个正在运行的压缩任务完成...")
                        await asyncio.wait(self.running_compression_futures)
                        self.running_compression_futures.clear()
                    await asyncio.sleep(2)
                    continue
                else:
                    # 扫描已完成，等待所有正在运行的任务完成
                    if self.running_compression_futures:
                        logger.debug(f"扫描已完成，等待 {len(self.running_compression_futures)} 个正在运行的压缩任务完成...")
                        await asyncio.wait(self.running_compression_futures)
                        self.running_compression_futures.clear()
                    logger.debug("获取文件组超时，扫描已完成，退出压缩循环")
                    break
            except Exception as e:
                logger.error(f"处理文件组失败: {str(e)}", exc_info=True)
                # 发生异常时也检查扫描状态
                scan_status = await self._get_scan_status()
                if scan_status != 'completed':
                    logger.debug(f"处理文件组异常，但扫描未完成（状态={scan_status}），继续等待...")
                    # 如果有正在运行的任务，等待它们完成
                    if self.running_compression_futures:
                        logger.debug(f"等待 {len(self.running_compression_futures)} 个正在运行的压缩任务完成...")
                        await asyncio.wait(self.running_compression_futures)
                        self.running_compression_futures.clear()
                    await asyncio.sleep(2)
                    continue
                else:
                    # 扫描已完成，等待所有正在运行的任务完成
                    if self.running_compression_futures:
                        logger.debug(f"扫描已完成，等待 {len(self.running_compression_futures)} 个正在运行的压缩任务完成...")
                        await asyncio.wait(self.running_compression_futures)
                        self.running_compression_futures.clear()
                    # 异常退出不算完成：剩余组被放弃时任务必须失败，不得伪装成功
                    self.loop_failed_reason = f"处理文件组异常: {str(e)[:200]}"
                    logger.error("处理文件组异常，扫描已完成，退出压缩循环（标记压缩失败）")
                    break
        
        # 确保所有任务都已完成（带超时：僵尸 future 不再永久挂起——任务 149 卡死点）
        if self.running_compression_futures:
            logger.info(f"等待剩余的 {len(self.running_compression_futures)} 个压缩任务完成...")
            done, pending = await asyncio.wait(self.running_compression_futures, timeout=600)
            # 处理所有已完成的任务（包括异常）
            for task in done:
                try:
                    await task
                except Exception as e:
                    logger.error(f"压缩任务异常: {str(e)}", exc_info=True)
            if pending:
                logger.warning(
                    f"[压缩循环] 收尾：{len(pending)} 个压缩任务 600 秒未完成，"
                    f"强制取消（数据已写带的组不受影响）")
                for task in pending:
                    task.cancel()
                await asyncio.wait(pending, timeout=10)
            self.running_compression_futures.clear()
        
        # 在内存中设置压缩完成状态：
        # 只有"无主循环异常且无失败组"才算完成；否则置失败标志，
        # 引擎四合一判定（compression_completed and not compression_failed）走 FAILED
        if not self.loop_failed_reason and not self.compression_group_failed:
            self.backup_task.compression_completed = True
        else:
            self.backup_task.compression_failed = True

        # 压缩完成日志：换行输出，与其他日志有明显差异
        logger.info("=" * 80)
        if self.loop_failed_reason or self.compression_group_failed:
            logger.warning("[压缩循环] ========== 压缩循环结束（存在失败） ==========")
            if self.loop_failed_reason:
                logger.warning(f"  主循环异常: {self.loop_failed_reason}")
            if self.compression_group_failed:
                logger.warning(
                    f"  失败组数: {self.failed_groups}，放弃文件数: {self.failed_files_total:,}"
                )
        else:
            logger.info("[压缩循环] ========== 压缩完成 ==========")
        logger.info(f"  处理文件组数: {self.group_idx}")
        logger.info(f"  处理文件数: {self.processed_files:,} 个文件")
        logger.info(f"  原始总大小: {format_bytes(self.total_original_size)}")
        logger.info(f"  压缩后总大小: {format_bytes(self.total_size)}")
        if self.total_original_size > 0:
            compression_ratio = (1 - self.total_size / self.total_original_size) * 100
            logger.info(f"  压缩率: {compression_ratio:.2f}%")
        logger.info("=" * 80)
    
    def get_aggregated_compression_progress(self) -> Optional[Dict[str, Any]]:
        """获取所有并行压缩任务的聚合进度和各个任务的进度列表
        
        从内存中的压缩程序获取实时进度，直接从每个运行中任务的compress_progress字典读取实际进度。
        
        Returns:
            Dict包含：
            - current: 所有并行任务已处理文件数的总和（实际值，用于兼容前端）
            - total: 所有并行任务文件组文件数的总和（实际值，用于兼容前端）
            - current_file_index: 所有并行任务已处理文件数的总和（实际值，不是估算）
            - total_files_in_group: 所有并行任务文件组文件数的总和（实际值，不是估算）
            - percent: 总进度百分比（按文件数）
            - group_size_bytes: 所有并行任务文件组大小的总和
            - running_count: 正在运行的压缩任务数
            - task_progress_list: 各个压缩任务的进度列表，每个任务包含：
                - percent: 该任务的进度百分比（按文件大小）
                - current: 已处理文件数
                - total: 文件组总文件数
                - group_size_bytes: 文件组大小（字节）
        """
        if not self.use_prefetcher or not self._running:
            # 非并行模式或未运行，返回None
            return None
        
        # **在途口径**：只算真正在压缩（会落地产出）的组，与守卫日志一致。
        # 不能再用 len(running_compression_futures) —— 它包含"排队等守卫"和
        # "已通过守卫但尚未开写"的组，会让卡片显示 16/16 而实际只 3~7 个在压。
        # 注意：仍以 futures 非空作为"有任务"的判据，否则全部组在排队时
        # 会 return None，卡片反而不显示"0/16"。
        _futures_count = len(self.running_compression_futures)
        if _futures_count == 0:
            # 没有正在运行的压缩任务
            return None
        try:
            running_count = int(getattr(self.compressor, '_active_compressing', 0) or 0)
        except Exception:
            running_count = _futures_count
        
        # 从compression_progress_map中读取每个运行中任务的实际进度
        total_current = 0
        total_total = 0
        total_group_size_bytes = 0
        active_count = 0
        task_progress_list = []  # 各个任务的进度列表

        # 组类型判定与调度器同口径：组内文件数 < 阈值 = 大文件组(L)，≥ 阈值 = 小文件组(S)
        try:
            from config.settings import get_settings as _gs
            _large_file_threshold = int(
                getattr(_gs(), 'GROUP_LARGE_FILE_COUNT_THRESHOLD', 20000) or 20000)
        except Exception:
            _large_file_threshold = 20000
        
        # 遍历所有运行中的压缩任务，从它们的compress_progress字典读取实际进度
        logger.debug(
            f"[压缩进度聚合] 开始聚合进度: compression_progress_map 大小={len(self.compression_progress_map)}, "
            f"running_compression_futures 大小={running_count}"
        )
        
        # 同时检查 running_compression_futures 中的任务，确保所有正在运行的任务都被计入
        running_group_indices = set()
        for task in self.running_compression_futures:
            # 尝试从任务中获取 group_idx（如果任务有存储）
            # 注意：asyncio.Task 对象可能没有直接存储 group_idx，需要通过其他方式关联
            pass  # 暂时无法直接从 task 获取 group_idx
        
        for group_idx, progress_info in self.compression_progress_map.items():
            compress_progress = progress_info.get('compress_progress')
            if compress_progress:
                running = compress_progress.get('running', False)
                completed = compress_progress.get('completed', False)
                total_files_in_group = compress_progress.get('total_files_in_group', 0)
                logger.debug(
                    f"[压缩进度聚合] 任务#{group_idx}: running={running}, completed={completed}, "
                    f"total_files_in_group={total_files_in_group}, compress_progress存在={compress_progress is not None}"
                )
            
            if compress_progress and compress_progress.get('running', False) and not compress_progress.get('completed', False):
                # 从compress_progress字典读取实际进度
                current_file_index = compress_progress.get('current_file_index', 0)
                total_files_in_group = compress_progress.get('total_files_in_group', 0)
                group_size_bytes = progress_info.get('group_size_bytes', 0)

                # 仅展示已实际开始压缩的组：等待磁盘守卫放行的组尚无任何进度事件
                # （current=0 且无 processed_bytes），以 0% 占位会误导 UI
                _processed = compress_progress.get('processed_bytes')
                if current_file_index <= 0 and (_processed is None or _processed <= 0):
                    continue

                logger.debug(
                    f"[压缩进度聚合] 任务#{group_idx}: current_file_index={current_file_index}, "
                    f"total_files_in_group={total_files_in_group}, group_size_bytes={group_size_bytes}"
                )
                
                # 如果 total_files_in_group 为 0，尝试从 progress_info 中获取（作为后备）
                if total_files_in_group == 0:
                    total_files_in_group = progress_info.get('total_files', 0)
                    if total_files_in_group > 0:
                        logger.warning(
                            f"[压缩进度聚合] 任务#{group_idx}: compress_progress.total_files_in_group=0，"
                            f"使用 progress_info.total_files={total_files_in_group} 作为后备"
                        )
                        # 更新 compress_progress 中的值，避免下次还是 0
                        compress_progress['total_files_in_group'] = total_files_in_group
                
                if total_files_in_group > 0:
                    # 累加每个任务的实际进度（这是正在处理任务的文件数和）
                    total_current += current_file_index  # 3102 = 所有正在处理任务的文件数总和
                    total_total += total_files_in_group  # 11051 = 所有正在处理任务的文件总数和
                    total_group_size_bytes += group_size_bytes
                    active_count += 1
                    
                    # 计算该任务的进度百分比（真正按文件大小计算）
                    # 获取已处理文件的实际大小（如果压缩函数已累计）
                    processed_bytes = compress_progress.get('processed_bytes', 0)
                    
                    if group_size_bytes > 0 and processed_bytes > 0:
                        # 真正按文件大小计算百分比：已处理文件大小 / 文件组总大小 * 100
                        size_percent = (processed_bytes / group_size_bytes * 100) if group_size_bytes > 0 else 0.0
                    elif group_size_bytes > 0 and total_files_in_group > 0:
                        # 如果没有累计已处理大小，回退到按文件数占比（假设文件大小分布均匀）
                        file_count_percent = (current_file_index / total_files_in_group * 100) if total_files_in_group > 0 else 0.0
                        size_percent = file_count_percent
                    elif total_files_in_group > 0:
                        # 如果没有文件组大小信息，仍然按文件数占比计算
                        size_percent = (current_file_index / total_files_in_group * 100) if total_files_in_group > 0 else 0.0
                    else:
                        size_percent = 0.0
                    
                    # 当前（近似）文件所在目录：current_file_index 是 tar 已完成数，
                    # 取该位置（读完则取最后一个）的路径 dirname，供卡片"当前目录"行
                    _current_dir = None
                    try:
                        _paths = progress_info.get('file_paths') or []
                        if _paths:
                            _i = int(current_file_index or 0)
                            if _i >= len(_paths):
                                _i = len(_paths) - 1
                            if _i >= 0:
                                _current_dir = os.path.dirname(_paths[_i]) or None
                    except Exception:
                        _current_dir = None
                    task_progress_list.append({
                        'percent': size_percent,
                        'current': current_file_index,
                        'total': total_files_in_group,
                        'group_type': self._group_type_of(total_files_in_group),
                        'group_size_bytes': group_size_bytes,
                        'processed_bytes': processed_bytes,  # 已处理文件的实际大小
                        'current_dir': _current_dir,
                        # 当前打包的归档文件名（如 backup_2026-10_000113_20261017_083125.tar.zst）
                        'archive_name': progress_info.get('archive_name')
                                        or compress_progress.get('archive_name')
                    })
                    
                    logger.debug(
                        f"[压缩进度] 任务#{group_idx}: {current_file_index}/{total_files_in_group} 个文件 "
                        f"({size_percent:.1f}%), 大小={group_size_bytes / (1024**3):.2f}GB"
                    )
        
        # 即使 total_total == 0，如果有 running_count > 0，也应该返回数据结构（包含空的 task_progress_list）
        # 这样可以避免前端交替显示，让前端知道有任务在运行，只是暂时没有进度数据
        if total_total == 0:
            if running_count > 0:
                # 有任务在运行，但暂时没有进度数据（可能是刚启动或初始化阶段）
                logger.debug(
                    f"[压缩进度聚合] total_total=0 但 running_count={running_count} > 0，"
                    f"返回空进度数据结构。active_count={active_count}, "
                    f"compression_progress_map大小={len(self.compression_progress_map)}, "
                    f"task_progress_list大小={len(task_progress_list)}"
                )
                # 返回包含空 task_progress_list 的数据结构，而不是 None
                return {
                    'current': 0,
                    'total': 0,
                    'current_file_index': 0,
                    'total_files_in_group': 0,
                    'percent': 0.0,
                    'group_size_bytes': 0,
                    'running_count': running_count,
                    'task_progress_list': []  # 空的进度列表，但至少表明有任务在运行
                }
            else:
                # 没有任务在运行，返回 None
                logger.debug(
                    f"[压缩进度聚合] total_total=0 且 running_count=0，返回 None。"
                    f"active_count={active_count}, compression_progress_map大小={len(self.compression_progress_map)}"
                )
                return None
        
        percent = (total_current / total_total * 100) if total_total > 0 else 0.0

        # 在途组类型（供前端"当前阶段"百分比区分 L/S）：
        # 全部大文件组 → 'large'；全部小文件组 → 'small'；混合 → 'mixed'
        _types = {t.get('group_type') for t in task_progress_list if t.get('group_type')}
        if len(_types) == 1:
            _dominant = _types.pop()
        elif len(_types) > 1:
            _dominant = 'mixed'
        else:
            _dominant = None

        logger.debug(
            f"[压缩进度聚合] 并行任务数={running_count}, 活跃任务数={active_count}, "
            f"聚合进度={total_current}/{total_total} ({percent:.1f}%), "
            f"任务进度列表: {[(t.get('percent', 0), t.get('current', 0), t.get('total', 0)) for t in task_progress_list]}"
        )

        return {
            # 兼容前端的字段名（用于"当前阶段"显示）
            'current': total_current,  # 所有正在处理任务的文件数总和（3102）
            'total': total_total,  # 所有正在处理任务的文件总数和（11051）
            # 原始字段名
            'current_file_index': total_current,
            'total_files_in_group': total_total,
            'percent': percent,
            'group_type': _dominant,   # 在途组类型：large/small/mixed/None
            'group_size_bytes': total_group_size_bytes,
            'running_count': running_count,
            'task_progress_list': task_progress_list  # 各个任务的进度列表，用于"各压缩任务进度"显示
        }

    async def _update_compression_progress_periodically(self):
        """定期更新压缩进度（从数据库查询聚合进度，支持多进程）
        
        每5秒查询一次数据库，获取所有已压缩文件数（is_copy_success = TRUE），
        更新 description 字段，显示聚合进度。
        
        进度显示格式：已处理的文件组数/压缩队列中的总文件数
        """
        try:
            # 等待压缩开始（避免在压缩开始前频繁查询）
            await asyncio.sleep(5.0)
            
            update_interval = 5.0  # 每5秒更新一次
            last_update_time = 0.0
            
            while self._running:
                try:
                    current_time = asyncio.get_event_loop().time()
                    
                    # 检查是否需要更新（每5秒更新一次）
                    if current_time - last_update_time >= update_interval:
                        # 从内存获取已压缩文件数（不再依赖数据库状态）
                        # 在 openGauss / 预取模式下，所有压缩进度都由当前进程维护在内存中
                        from utils.scheduler.db_utils import is_opengauss
                        if is_opengauss():
                            # 使用 CompressionWorker 自身统计的 processed_files 作为已压缩文件数
                            compressed_count = getattr(self, "processed_files", 0) or 0
                        else:
                            # 非 openGauss 模式保留原有行为（从数据库聚合查询进度）
                            compressed_count = await self.backup_db.get_compressed_files_count(
                                self.backup_set.id
                            )
                        
                        # 调试日志：记录查询结果
                        logger.debug(
                            f"[压缩进度更新] 查询已压缩文件数: compressed_count={compressed_count}, "
                            f"backup_set_id={self.backup_set.id}"
                        )
                        
                        # 获取需要处理的总文件数（优先使用backup_task.total_files，这是正在处理的文件总数）
                        total_files_to_process = getattr(self.backup_task, 'total_files', 0) or 0
                        
                        # 如果backup_task.total_files为0，尝试从预取器获取累计放入队列的文件数作为后备
                        if total_files_to_process == 0 and self.file_group_prefetcher:
                            total_files_to_process = self.file_group_prefetcher.get_queued_files_count()
                            logger.debug(
                                f"[压缩进度更新] backup_task.total_files为0，使用预取器累计文件数: {total_files_to_process}"
                            )
                        
                        # 调试日志：记录文件数来源
                        if self.file_group_prefetcher:
                            total_queued = getattr(self.file_group_prefetcher, 'total_queued_files_count', 0)
                            current_queued = getattr(self.file_group_prefetcher, 'queued_files_count', 0)
                            logger.debug(
                                f"[压缩进度更新] 需要处理的总文件数: {total_files_to_process} "
                                f"(backup_task.total_files={getattr(self.backup_task, 'total_files', 0)}, "
                                f"预取器累计={total_queued}, 预取器当前={current_queued})"
                            )
                        
                        # 计算进度百分比
                        if total_files_to_process > 0:
                            progress_percent = (compressed_count / total_files_to_process) * 100.0
                            progress_percent = min(100.0, max(0.0, progress_percent))
                            
                            # 更新 description 字段
                            # 格式：已处理的文件数/需要处理的总文件数
                            description = f"[压缩文件中...] {compressed_count}/{total_files_to_process} 个文件 ({progress_percent:.1f}%)"
                            await self.backup_db.update_task_stage_with_description(
                                self.backup_task,
                                "compress",
                                description
                            )
                            
                            # 只在进度变化超过1%时记录日志，减少日志输出
                            if not hasattr(self, '_last_logged_progress') or abs(progress_percent - self._last_logged_progress) >= 1.0:
                                logger.debug(
                                    f"[压缩进度更新] 已处理: {compressed_count}/{total_files_to_process} 个文件 "
                                    f"({progress_percent:.1f}%) "
                                    f"[compressed_count={compressed_count}, total_files_to_process={total_files_to_process}]"
                                )
                                self._last_logged_progress = progress_percent
                        else:
                            logger.debug("[压缩进度更新] 需要处理的总文件数为0，跳过进度更新")
                        
                        # 更新 backup_task.current_compression_progress（包含 task_progress_list，供前端显示各任务进度）
                        # 使用 get_aggregated_compression_progress() 获取包含所有并行任务进度的聚合数据
                        aggregated_progress = self.get_aggregated_compression_progress()
                        if aggregated_progress:
                            # 将聚合进度（包含 task_progress_list）设置到 backup_task，供 UI 使用
                            if hasattr(self.backup_task, 'current_compression_progress'):
                                self.backup_task.current_compression_progress = aggregated_progress
                                logger.debug(
                                    f"[压缩进度更新] 已更新 backup_task.current_compression_progress，"
                                    f"包含 {len(aggregated_progress.get('task_progress_list', []))} 个任务进度"
                                )
                        # 注意：如果 get_aggregated_compression_progress() 返回 None（可能暂时没有活跃任务），
                        # 不更新 backup_task.current_compression_progress，保留上一次的值，避免前端交替显示
                        
                        last_update_time = current_time
                    
                    # 等待1秒后再次检查
                    await asyncio.sleep(1.0)
                    
                except asyncio.CancelledError:
                    logger.debug("[压缩进度更新] 收到取消信号，停止进度更新")
                    break
                except Exception as e:
                    logger.warning(f"[压缩进度更新] 更新进度失败: {str(e)}", exc_info=True)
                    # 出错后等待更长时间再重试
                    await asyncio.sleep(5.0)
                    
        except asyncio.CancelledError:
            logger.debug("[压缩进度更新] 进度更新任务被取消")
        except Exception as e:
            logger.error(f"[压缩进度更新] 进度更新任务异常: {str(e)}", exc_info=True)

    async def _process_database_file_groups(self):
        """处理数据库中的文件组（非openGauss模式）"""
        logger.info("开始处理数据库中的文件组")

        last_processed_id = 0

        while self._running:
            try:
                # 从数据库获取文件组
                file_groups = await self.backup_db.get_next_compression_group(
                    backup_set_id=self.backup_set.id,
                    batch_size=1,
                    last_processed_id=last_processed_id
                )

                if not file_groups:
                    scan_status = await self._get_scan_status()
                    if scan_status == 'completed':
                        logger.info("所有文件组处理完成")
                        break
                    logger.debug("无文件组，等待...")
                    await asyncio.sleep(2)
                    continue

                file_group = file_groups[0]

                # 获取最后一个文件的ID作为下次查询的起点
                if file_group:
                    last_processed_id = 0
                    for f in file_group:
                        if isinstance(f, dict):
                            last_processed_id = max(last_processed_id, f.get('id', 0))
                        elif isinstance(f, list) and len(f) > 0:
                            # 处理不同的列表格式
                            if isinstance(f[0], (int, float)):
                                # 标准格式：[id, file_path, relative_path, file_size, ...]
                                last_processed_id = max(last_processed_id, int(f[0]))
                            elif len(f) >= 8 and isinstance(f[7], dict) and 'id' in f[7]:
                                # 可能的嵌套格式：[..., ..., ..., ..., ..., ..., ..., {'id': id, ...}]
                                last_processed_id = max(last_processed_id, int(f[7]['id']))
                            else:
                                logger.warning(f"[数据库模式] 无法从列表格式获取文件ID: {f}")
                        else:
                            logger.warning(f"[数据库模式] 未知文件格式: {f}")

                # 顺序处理文件组
                await self._compress_file_group(file_group, self.group_idx)

                self.group_idx += 1

            except Exception as e:
                logger.error(f"处理文件组失败: {str(e)}", exc_info=True)
                # 发生异常时检查扫描状态，如果扫描未完成，继续等待
                scan_status = await self._get_scan_status()
                if scan_status != 'completed':
                    logger.debug(f"处理文件组异常，但扫描未完成（状态={scan_status}），继续等待...")
                    await asyncio.sleep(2)
                    continue
                else:
                    logger.error("处理文件组异常，扫描已完成，退出压缩循环")
                    break

        logger.info("数据库文件组处理完成")

    async def _compress_file_group(self, file_group: List[Dict], group_idx: int, compress_progress: Optional[Dict] = None):
        """压缩单个文件组（内部顺序执行：压缩完成 → 标注完成 → 移动到final → 返回，队列已消费）"""
        # 检查是否应该停止
        if not self._running:
            logger.debug(f"[#{group_idx + 1}] 收到停止信号，跳过压缩")
            return
        
        if not file_group:
            logger.warning(f"[#{group_idx + 1}] 文件组为空，跳过")
            return

        try:
            total_files = len(file_group)

            # 处理文件数据格式并转换为压缩器期望的格式
            total_size = 0
            processed_file_group = []

            for f in file_group:
                file_dict = None

                if isinstance(f, dict):
                    # 字典格式 - 直接使用
                    file_dict = f
                    total_size += (file_dict.get('size', 0)
                                   or file_dict.get('file_size', 0))
                elif isinstance(f, list):
                    # 检查是否是嵌套字典格式（最后一个元素是字典）
                    if f and isinstance(f[-1], dict):
                        # 嵌套字典格式：[..., {...dict...}]
                        file_dict = f[-1]
                        total_size += file_dict.get('size', 0)
                    elif len(f) >= 4 and isinstance(f[3], (int, float)):
                        # 标准格式：[id, file_path, relative_path, file_size, ...]
                        # 转换为字典格式
                        file_dict = {
                            'id': f[0] if len(f) > 0 else 0,
                            'file_path': f[1] if len(f) > 1 else '',
                            'relative_path': f[2] if len(f) > 2 else '',
                            'file_size': f[3] if len(f) > 3 else 0,
                            'modified_time': f[4] if len(f) > 4 else None,
                            'md5': f[5] if len(f) > 5 else '',
                            'is_copy_success': f[6] if len(f) > 6 else False,
                            'stage': f[7] if len(f) > 7 else 'pending'
                        }
                        total_size += int(f[3])
                    else:
                        logger.warning(f"[#{group_idx + 1}] 无法从列表格式获取文件大小: {f}")
                        continue
                else:
                    logger.warning(f"[#{group_idx + 1}] 未知文件格式: {f}")
                    continue

                processed_file_group.append(file_dict)

            logger.info(f"[#{group_idx + 1}] 开始压缩 {total_files} 个文件，总大小 {format_bytes(total_size)}")

            # 初始化共享的compress_progress字典（如果传入的字典还没有这些字段）
            if compress_progress is None:
                compress_progress = {
                    'bytes_written': 0,
                    'running': True,
                    'completed': False,
                    'current_file_index': 0,
                    'total_files_in_group': total_files
                }
            else:
                # 确保共享字典有必要的字段
                compress_progress['running'] = True
                compress_progress['completed'] = False
                compress_progress['current_file_index'] = compress_progress.get('current_file_index', 0)
                # 如果 total_files_in_group 不存在或为 0，设置为 total_files
                if compress_progress.get('total_files_in_group', 0) == 0:
                    compress_progress['total_files_in_group'] = total_files
                    logger.debug(f"[压缩任务#{group_idx + 1}] 初始化 total_files_in_group={total_files}")
                else:
                    # 确保值正确（如果传入的字典已经有值，但可能不正确，使用 total_files 覆盖）
                    compress_progress['total_files_in_group'] = total_files
                    logger.debug(f"[压缩任务#{group_idx + 1}] 更新 total_files_in_group={total_files}")
            
            # 更新进度映射中的信息
            if group_idx in self.compression_progress_map:
                self.compression_progress_map[group_idx]['compress_progress'] = compress_progress
                self.compression_progress_map[group_idx]['total_files'] = total_files
                self.compression_progress_map[group_idx]['group_size_bytes'] = total_size
                # 归档文件名（compressor 在生成文件名时写入共享进度字典），
                # 供卡片 hover 显示"当前打包的是哪个归档"
                self.compression_progress_map[group_idx]['archive_name'] = compress_progress.get('archive_name')

            # 更新进度
            await self.backup_db.update_task_stage_with_description(
                self.backup_task,
                "compress",
                f"[压缩文件中...] 0/{total_files} 个文件 (0.0%)"
            )

            # 再次检查是否应该停止（在开始压缩前）
            if not self._running:
                logger.debug(f"[#{group_idx + 1}] 收到停止信号，取消压缩")
                return
            
            # 准备压缩目录
            with tempfile.TemporaryDirectory() as temp_dir:
                # 传递共享的compress_progress字典给压缩函数
                # 压缩函数内部会实时更新这个字典，我们可以在外部读取实际进度
                compressed_info = await self.compressor.compress_file_group(
                    processed_file_group, 
                    self.backup_set, 
                    self.backup_task, 
                    self.processed_files, 
                    total_files,
                    shared_compress_progress=compress_progress  # 传递共享的进度字典
                )
                
                # 压缩完成后检查是否应该停止
                if not self._running:
                    logger.debug(f"[#{group_idx + 1}] 压缩完成，但收到停止信号，跳过后续处理")
                    return
                
                # 压缩完成后，标记进度字典为完成状态
                if compress_progress:
                    compress_progress['completed'] = True
                    compress_progress['running'] = False
                    # 更新进度映射中的完成状态
                    if group_idx in self.compression_progress_map:
                        self.compression_progress_map[group_idx]['compress_progress']['completed'] = True
                        self.compression_progress_map[group_idx]['compress_progress']['running'] = False
                    
                    # 压缩完成后，更新统计和标记文件为已复制
                if compressed_info:
                    # 更新内存中的统计
                    self.processed_files += total_files
                    compressed_size = compressed_info.get('compressed_size', 0) or 0
                    original_size = compressed_info.get('original_size', 0) or 0
                    self.total_size += compressed_size
                    self.total_original_size += original_size

                    # 压缩比异常（源 >100MB 且压缩后 <0.1%）：记录本组文件清单，
                    # 供已完成任务卡片的「错误日志」弹窗定位（这类文件可能未真正备份）
                    if (original_size > 100 * 1024 * 1024 and compressed_size > 0
                            and compressed_size / original_size < 0.001):
                        try:
                            _paths = [
                                (f.get('path') or f.get('file_path') or '?')
                                for f in (file_group or [])
                            ]
                            _sample = '；'.join(_paths[:20])
                            if len(_paths) > 20:
                                _sample += f"；等 {len(_paths)} 个文件"
                            _ratio = compressed_size / original_size * 100
                            self.error_logs.append({
                                'category': 'compress',
                                'time': time.strftime('%Y-%m-%d %H:%M:%S'),
                                'message': (
                                    f"组 #{group_idx + 1} 压缩比异常："
                                    f"{format_bytes(original_size)} → "
                                    f"{format_bytes(compressed_size)}（{_ratio:.3f}%）"
                                ),
                                'detail': (f"文件清单（{len(_paths)} 个）：{_sample}"
                                           + ("（完整清单可在弹窗中导出）" if len(_paths) > 20 else "")),
                                # 完整清单（任务结束时写入导出文件，不塞进 result_summary）
                                'files': _paths,
                            })
                        except Exception:
                            pass

                    # 记录压缩失败的文件（带文件名）到系统日志
                    failed_files = compressed_info.get('failed_files', [])
                    if failed_files:
                        task_id = getattr(self.backup_task, 'id', None)
                        # 汇总写入系统日志表（一条记录包含所有失败文件）
                        try:
                            from utils.log_utils import log_system
                            from models.system_log import LogLevel, LogCategory
                            summary = '; '.join(
                                f"{ff.get('path', '?')}({ff.get('reason', '?')})"
                                for ff in failed_files[:20]
                            )
                            await log_system(
                                level=LogLevel.WARNING,
                                category=LogCategory.BACKUP,
                                message=f"压缩失败 {len(failed_files)} 个文件: {summary}",
                                module='compression_worker',
                                function='_compress_file_group',
                                task_id=task_id,
                                details={'failed_files': failed_files[:50]}
                            )
                        except Exception:
                            pass
                    
                    # 纯内存模式：跳过所有数据库操作（不写chunk_number、不更新DB进度）
                    if self.in_memory_store is not None:
                        # 只更新内存中的backup_task统计
                        total_files_to_process = getattr(self.backup_task, 'total_files', 0) or 0
                        new_progress_percent = min(100.0, (self.processed_files / total_files_to_process) * 100.0) if total_files_to_process > 0 else 0.0
                        self.backup_task.processed_files = self.processed_files
                        self.backup_task.processed_bytes = self.total_original_size
                        self.backup_task.compressed_bytes = self.total_size
                        self.backup_task.progress_percent = new_progress_percent

                        logger.info(
                            f"[#{group_idx + 1}] ✅ 压缩完成（内存模式）: {total_files} 个文件, "
                            f"原始大小: {format_bytes(original_size)}, 压缩后: {format_bytes(compressed_size)}, "
                            f"累计: {self.processed_files}/{total_files_to_process} 个文件 ({new_progress_percent:.1f}%)"
                        )
                    else:
                        # 数据库模式：更新chunk_number和DB进度
                        try:
                            chunk_number = group_idx + 1
                            file_paths = [f.get('file_path') or f.get('path') for f in processed_file_group if f.get('file_path') or f.get('path')]

                            if file_paths:
                                if self.db_updater:
                                    await self.db_updater.submit_compressed_files(
                                        group_idx=group_idx,
                                        file_paths=file_paths,
                                        chunk_number=chunk_number,
                                        compressed_size=compressed_size,
                                        original_size=original_size
                                    )
                                    logger.info(
                                        f"[压缩工作器] ✅ 已提交压缩文件组 #{group_idx + 1} 给调度器: "
                                        f"{len(file_paths)} 个文件, chunk_number={chunk_number}, "
                                        f"压缩大小={format_bytes(compressed_size)}"
                                    )
                                else:
                                    compressed_file_info = {
                                        'compressed_size': compressed_size,
                                        'compression_enabled': compressed_info.get('compression_enabled', True),
                                        'checksum': compressed_info.get('checksum')
                                    }
                                    archive_path = compressed_info.get('path') or ''
                                    await self.backup_db.mark_files_as_copied(
                                        backup_set=self.backup_set,
                                        file_group=processed_file_group,
                                        compressed_file=compressed_file_info,
                                        tape_file_path=archive_path,
                                        chunk_number=chunk_number
                                    )
                                    logger.debug(f"[#{group_idx + 1}] ✅ 已更新 chunk_number={chunk_number}，文件数={len(processed_file_group)}")
                        except Exception as update_error:
                            logger.error(f"[#{group_idx + 1}] ⚠️ 提交压缩文件信息失败: {str(update_error)}", exc_info=True)

                        # 从数据库读取当前值，然后累加（避免并发问题）
                        from utils.scheduler.db_utils import is_opengauss, get_opengauss_connection
                        if is_opengauss():
                            async with get_opengauss_connection() as conn:
                                row = await conn.fetchrow(
                                    "SELECT processed_files, processed_bytes, compressed_bytes, total_files FROM backup_tasks WHERE id = $1",
                                    self.backup_task.id
                                )
                                if row:
                                    current_processed_files = row['processed_files'] or 0
                                    current_processed_bytes = row['processed_bytes'] or 0
                                    current_compressed_bytes = row['compressed_bytes'] or 0
                                    total_files_to_process = row['total_files'] or 0
                                else:
                                    current_processed_files = 0
                                    current_processed_bytes = 0
                                    current_compressed_bytes = 0
                                    total_files_to_process = getattr(self.backup_task, 'total_files', 0) or 0
                        else:
                            current_processed_files = getattr(self.backup_task, 'processed_files', 0) or 0
                            current_processed_bytes = getattr(self.backup_task, 'processed_bytes', 0) or 0
                            current_compressed_bytes = getattr(self.backup_task, 'compressed_bytes', 0) or 0
                            total_files_to_process = getattr(self.backup_task, 'total_files', 0) or 0

                        new_processed_files = current_processed_files + total_files
                        new_processed_bytes = current_processed_bytes + original_size
                        new_compressed_bytes = current_compressed_bytes + compressed_size

                        if total_files_to_process > 0:
                            new_progress_percent = min(100.0, (new_processed_files / total_files_to_process) * 100.0)
                        else:
                            new_progress_percent = getattr(self.backup_task, 'progress_percent', 0.0) or 0.0

                        self.backup_task.processed_files = new_processed_files
                        self.backup_task.processed_bytes = new_processed_bytes
                        self.backup_task.compressed_bytes = new_compressed_bytes
                        self.backup_task.progress_percent = new_progress_percent

                        await self.backup_db.update_scan_progress(
                            self.backup_task,
                            new_processed_files,
                            new_processed_files,
                            None
                        )

                        logger.info(
                            f"[#{group_idx + 1}] ✅ 压缩完成并已移动到final: {total_files} 个文件, "
                            f"原始大小: {format_bytes(original_size)}, 压缩后: {format_bytes(compressed_size)}, "
                            f"累计: {new_processed_files}/{total_files_to_process} 个文件 ({new_progress_percent:.1f}%)"
                        )
                
                if not compressed_info:
                    # 组压缩失败（归档不存在/全部文件失败）：文件已从内存库删除，
                    # 不计数、不留清单就等于静默丢数据 —— 必须记账并让任务失败
                    self.compression_group_failed = True
                    self.backup_task.compression_failed = True
                    self.failed_groups += 1
                    self.failed_files_total += total_files
                    _fail_paths = [
                        (f.get('path') or f.get('file_path') or '?')
                        for f in processed_file_group[:20]
                    ]
                    logger.error(
                        f"[#{group_idx + 1}] ❌ 压缩返回空（组失败）: {total_files} 个文件未备份，"
                        f"样例: {_fail_paths[:5]}"
                    )
                    self.error_logs.append({
                        'category': 'compress',
                        'time': time.strftime('%Y-%m-%d %H:%M:%S'),
                        'message': f"组 #{group_idx + 1} 压缩失败：{total_files} 个文件未备份",
                        'detail': '；'.join(_fail_paths[:5]),
                    })
                    try:
                        from utils.log_utils import log_system
                        from models.system_log import LogLevel, LogCategory
                        await log_system(
                            level=LogLevel.ERROR,
                            category=LogCategory.BACKUP,
                            message=f"压缩组失败: {total_files} 个文件未备份（任务将标记失败）",
                            module='compression_worker',
                            function='_compress_file_group',
                            task_id=getattr(self.backup_task, 'id', None),
                            details={'sample_paths': _fail_paths}
                        )
                    except Exception:
                        pass

                archive_path = compressed_info.get('path') if compressed_info else None

                # 注意：文件移动到磁带由FinalDirMonitor独立线程监控final目录处理

        except Exception as e:
            logger.error(f"[#{group_idx + 1}] ❌ 压缩失败: {str(e)}", exc_info=True)
            # 组内异常同样计入失败记账（不静默：任务必须失败，不得伪装成功）
            self.compression_group_failed = True
            self.backup_task.compression_failed = True
            self.failed_groups += 1
            self.failed_files_total += len(file_group or [])
            self.error_logs.append({
                'category': 'compress',
                'time': time.strftime('%Y-%m-%d %H:%M:%S'),
                'message': f"组 #{group_idx + 1} 压缩异常：{str(e)[:300]}",
                'detail': f"涉及 {len(file_group or [])} 个文件",
            })
            raise

