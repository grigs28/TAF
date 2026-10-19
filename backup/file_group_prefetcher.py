#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
文件组预取器（openGauss模式）
在压缩任务开始前预取文件组，实现压缩和搜索并行执行
"""

import asyncio
import logging
import time
from typing import Optional, Tuple, List, Dict, Any

from models.backup import BackupSet, BackupTask
from backup.backup_db import BackupDB
from backup.utils import format_bytes
from config.settings import get_settings

logger = logging.getLogger(__name__)


class FileGroupPrefetcher:
    """文件组预取器（openGauss模式专用）
    
    功能：
    1. 在任务集开始时运行，预取文件组放入队列
    2. 压缩任务从队列获取文件组，不再直接查询数据库
    3. 压缩开始后，预取线程继续工作获取下一组文件
    4. 内存中同时存在N+1组文件：N个正在压缩的，1个待压缩的（N=COMPRESSION_PARALLEL_BATCHES）
    """
    
    def __init__(
        self,
        backup_db: BackupDB,
        backup_set: BackupSet,
        backup_task: BackupTask,
        parallel_batches: Optional[int] = None,
        in_memory_store=None,
    ):
        self.backup_db = backup_db
        self.backup_set = backup_set
        self.backup_task = backup_task
        self.in_memory_store = in_memory_store
        
        # 获取并行批次数量（默认从配置读取）
        if parallel_batches is None:
            settings = get_settings()
            parallel_batches = getattr(settings, 'COMPRESSION_PARALLEL_BATCHES', 3)
        
        self.parallel_batches = parallel_batches
        # 文件组队列：容量为 parallel_batches + 1（N个正在压缩的 + 1个待压缩的）
        self.queue_maxsize = parallel_batches + 2
        self.file_group_queue: asyncio.Queue = asyncio.Queue(maxsize=0)  # 无界：分组不必等压缩消费
        
        # 预取任务
        self.prefetch_task: Optional[asyncio.Task] = None
        self._running = False
        # 预取循环异常原因（非 None 表示异常退出，引擎据此不把预取判定为"完成"）
        self.prefetch_error: Optional[str] = None
        
        # 最后处理的文件ID（用于连续查询）
        self.last_processed_file_id = 0
        
        # 是否正在重新检索遗漏文件（用于压缩循环判断是否应该继续等待）
        self.is_rescanning_missing_files = False
        
        # 统计信息
        self.prefetched_groups = 0
        self.total_retrieval_time = 0.0
        self.prefetch_loop_count = 0  # 预取循环执行次数（内存设置）
        
        # 队列中的文件总数（用于进度显示）- 使用计数器维护
        self.queued_files_count = 0  # 当前队列中所有文件组的总文件数
        self.total_queued_files_count = 0  # 累计放入队列的总文件数（用于进度显示）

        # 队列中每个组的**字节大小**（与 file_group_queue 严格同序增删，FIFO 一致）。
        # 任务查询 API 读取它，供卡片"预分组"徽章 hover 显示：
        #   "预取队列: 3 组（21.4G / 19.8G / 22.1G）"
        from collections import deque as _deque
        self._queued_group_sizes = _deque()
        self.total_queued_size = 0  # 累计放入队列的总文件大小（字节）

        # ===== 大小文件组动态调度（计划04 / 计划05） =====
        # 按组内文件数分类：< 阈值 = 大文件组（带宽主导），>= 阈值 = 小文件组（IOPS 主导）。
        # 调度规则（计划05：全程统一配额，不再区分扫描期/完成后）：
        #   · 全程 2大:1小；守卫触发时只取小文件组（文件数最多、产出最慢）
        #   · 扫描期暂存"文件数 < GROUP_SCAN_DEFER_BELOW"的超大文件组（产出最快，
        #     留给扫描完成后喂饱写带）；两类队列都空时降级取暂存组（"没合适的可以用"）
        #   · 某类耗尽时取另一类；都空则等待
        # 开关 GROUP_SCHED_ENABLED=false 时完全回退到原来的单队列 FIFO 行为。
        _s = get_settings()
        self.group_sched_enabled = bool(getattr(_s, 'GROUP_SCHED_ENABLED', True))
        self.group_large_threshold = int(
            getattr(_s, 'GROUP_LARGE_FILE_COUNT_THRESHOLD', 20000) or 20000)
        self._scan_ratio = self._parse_ratio(getattr(_s, 'GROUP_SCHED_SCAN_RATIO', '2:1'))
        self._normal_ratio = self._parse_ratio(getattr(_s, 'GROUP_SCHED_NORMAL_RATIO', '2:1'))
        # 扫描期大组保护阈值（0=禁用）
        self._defer_below = int(getattr(_s, 'GROUP_SCAN_DEFER_BELOW', 5000) or 0)
        self._deferred_items: list = []   # 扫描期暂存的超大文件组（原样 tuple，扫描完成放回）
        # 守卫状态切换防抖：跟随“空间不足检查间隔”（守卫每轮检查的节奏），
        # 默认 30 秒；不再使用独立固定值。
        self._sched_debounce = float(
            getattr(_s, 'DISK_CHECK_INTERVAL', 30) or 30)
        # 按组内文件数排序的优先队列（无界）：
        #   large 队列：priority = 文件数（升序）→ 优先“最大文件组”（组内文件最少、产出最快）
        #   small 队列：priority = -文件数（降序）→ 优先“最小文件组”（组内文件最多、产出最慢）
        # 结束信号 priority = +1e18，保证队列中真实文件组全部取完后才被取出。
        # （曾用 -1e18 导致信号入队即被取走，两个队列被误判为已耗尽，
        #  压缩循环提前退出，只剩少量在途组继续跑。）
        self.large_queue: asyncio.PriorityQueue = asyncio.PriorityQueue(maxsize=0)
        self.small_queue: asyncio.PriorityQueue = asyncio.PriorityQueue(maxsize=0)
        self._queue_seq = 0
        self._queued_sizes_by_seq: dict = {}
        # 已分组总数（按类型，含在途/队列中/已完成，仅调度模式统计）
        self._prefetched_large_groups = 0
        self._prefetched_small_groups = 0
        self._large_done = False
        self._small_done = False
        self._large_take = 0
        self._small_take = 0
        self._guard_confirmed = False     # 防抖后的守卫状态
        self._guard_pending_since = 0.0
        self._guard_seen = False
        self.compressor = None            # 由 BackupEngine 注入（读取 _guard_active）

    @staticmethod
    def _parse_ratio(value) -> tuple:
        """解析 '2:1' 为 (2,1)；非法值回退 (2,1)"""
        try:
            left, right = str(value).split(':')
            a, b = int(left), int(right)
            if a <= 0 and b <= 0:
                return (2, 1)
            return (max(0, a), max(0, b))
        except Exception:
            return (2, 1)

    def get_queue_size(self) -> int:
        """当前待压缩组数（兼容调度开关两种模式；含扫描期保护区暂存组）"""
        if self.group_sched_enabled:
            return (self.large_queue.qsize() + self.small_queue.qsize()
                    + len(self._deferred_items))
        return self.file_group_queue.qsize()

    def get_queue_sizes(self) -> list:
        """队列中各组字节大小（用于卡片 hover）"""
        if self.group_sched_enabled:
            return list(self._queued_sizes_by_seq.values())
        return list(self._queued_group_sizes)
        
    def start(self):
        """启动预取任务"""
        if self._running:
            logger.warning("[文件组预取器] 预取任务已在运行")
            return
        
        self._running = True
        self.prefetch_task = asyncio.create_task(self._prefetch_loop())
        logger.info("[文件组预取器] 预取任务已启动（openGauss模式）")
    
    async def stop(self):
        """停止预取任务"""
        if not self._running:
            return
        
        logger.info("[文件组预取器] 收到停止信号，正在停止预取任务...")
        self._running = False
        
        if self.prefetch_task:
            self.prefetch_task.cancel()
            try:
                await self.prefetch_task
            except asyncio.CancelledError:
                logger.info("[文件组预取器] 预取任务已取消")
            except Exception as e:
                logger.error(f"[文件组预取器] 停止预取任务时出错: {e}", exc_info=True)
        
        logger.info("[文件组预取器] 预取任务已停止")
    
    async def get_file_group(self, timeout: Optional[float] = None) -> Optional[Tuple[List[List[Dict]], int]]:
        """从队列获取文件组

        Args:
            timeout: 超时时间（秒），None表示无限等待

        Returns:
            (file_groups, last_processed_id) 或 None（如果超时或停止）
        """
        if self.group_sched_enabled:
            return await self._get_scheduled(timeout)
        try:
            if timeout is None:
                result = await self.file_group_queue.get()
            else:
                result = await asyncio.wait_for(self.file_group_queue.get(), timeout=timeout)
            
            # 更新最后处理的文件ID和队列文件数
            if result and isinstance(result, tuple) and len(result) == 2:
                file_groups, last_processed_id = result
                if last_processed_id > self.last_processed_file_id:
                    self.last_processed_file_id = last_processed_id
                
                # 从队列中取出文件组后，减少队列中的文件数（任务完成时移除）
                if file_groups:
                    files_in_removed_groups = sum(len(group) for group in file_groups)
                    self.queued_files_count = max(0, self.queued_files_count - files_in_removed_groups)
                    # 组大小记录同步出队（FIFO 一致；数量不足时静默忽略）
                    try:
                        self._queued_group_sizes.popleft()
                    except IndexError:
                        pass
                    logger.debug(
                        f"[文件组预取器] 从队列取出文件组，减少 {files_in_removed_groups} 个文件，"
                        f"队列中剩余文件数: {self.queued_files_count}"
                    )
                
                return result
            return result
        except asyncio.TimeoutError:
            logger.debug(f"[文件组预取器] 获取文件组超时（{timeout}秒）")
            return None
        except asyncio.CancelledError:
            logger.debug("[文件组预取器] 获取文件组被取消")
            return None
    
    def _update_guard_state(self) -> bool:
        """防抖后的守卫状态：守卫连续 active 超过 debounce 秒才确认切换。"""
        guard = bool(getattr(self.compressor, '_guard_active', False)) if self.compressor else False
        now = time.time()
        if guard == self._guard_seen:
            self._guard_pending_since = 0.0
            return self._guard_confirmed
        if self._guard_pending_since == 0.0:
            self._guard_pending_since = now
        if now - self._guard_pending_since >= self._sched_debounce:
            self._guard_seen = guard
            self._guard_confirmed = guard
            self._guard_pending_since = 0.0
            logger.info(f"[文件组调度] 守卫状态切换 -> {'拦停' if guard else '正常'}")
        return self._guard_confirmed

    async def _put_done_signal(self):
        """发送预取结束信号（调度模式双队列各发一份）

        用极大的 priority 保证结束标记排在该队列所有真实组之后：
        取到它就意味着该队列确实已取空。
        """
        if self.group_sched_enabled:
            for q in (self.large_queue, self.small_queue):
                self._queue_seq += 1
                await q.put((1e18, self._queue_seq, 0, ([], -1)))
        else:
            await self.file_group_queue.put(([], -1))

    def _scan_completed(self) -> bool:
        # 注意：InMemoryFileStore.is_scan_done() 是 async，此处是同步上下文，
        # 必须用同步的 get_scan_status()。此前误用 is_scan_done() 未 await，
        # 返回协程对象恒为 True——扫描期/完成后配额切换从未生效过（一直按完成后配额）。
        try:
            st = self.in_memory_store.get_scan_status()
            return st == 'completed'
        except Exception:
            return False

    def _choose_queue(self) -> str:
        """选择下一个组队列：'large'（快产出） / 'small'（慢产出）

        注意：这里只决定"配额优先类型"，不推进配额计数；取组成功后再由
        _commit_take() 计数。这样某类队列为空时降级取另一类不会消耗配额，
        下次仍优先配额要求的类型。
        """
        # 某类队列已确认耗尽（结束信号已取出）：强制选另一类
        if self._large_done and not self._small_done:
            return 'small'
        if self._small_done and not self._large_done:
            return 'large'
        guard = self._update_guard_state()
        if guard:
            # 守卫拦停：只取小文件组（文件数最多、产出最慢）；
            # 小文件组耗尽才取大文件组（没有更慢的可选）
            return 'small' if not self.small_queue.empty() else 'large'
        # 计划05：全程统一配额（不再区分扫描期/完成后）
        large_n, small_n = self._normal_ratio
        if large_n <= 0:
            return 'small'
        if small_n <= 0:
            return 'large'
        if self._large_take >= large_n and self._small_take >= small_n:
            self._large_take = self._small_take = 0
        if self._large_take < large_n:
            return 'large'
        return 'small'

    def _commit_take(self, actual: str, choice: str):
        """成功取到组后推进配额计数。

        只有实际取到的类型与配额选择一致时才计数；降级取另一类不计数，
        配额状态保持不变，下次仍优先配额要求的类型。
        """
        if actual != choice:
            return
        large_n, small_n = self._normal_ratio   # 计划05：全程统一配额
        if large_n <= 0 or small_n <= 0:
            return
        if actual == 'large' and self._large_take < large_n:
            self._large_take += 1
        elif actual == 'small' and self._small_take < small_n:
            self._small_take += 1

    @staticmethod
    def _unwrap(raw):
        """解包优先队列元素：(priority, seq, size, item) → (item, seq)"""
        try:
            _prio, seq, _size, item = raw
            return item, seq
        except (TypeError, ValueError):
            return raw, None

    @staticmethod
    def _item_file_count(item) -> Optional[int]:
        """从 (file_groups, last_id) 取组内文件数；结束信号等返回 None"""
        try:
            if item and isinstance(item, tuple) and len(item) == 2 and item[0]:
                return len(item[0][0])
        except Exception:
            pass
        return None

    async def _get_scheduled(self, timeout: Optional[float]):
        """调度模式取组：类型/配额选择 + 守卫联动 + 耗尽降级

        队列按文件数排序：
          large → 取文件数最小（最大文件组，产出最快）
          small → 取文件数最大（最小文件组，产出最慢）
        扫描期大组保护（计划05）：从 L 队列取出的"文件数 < GROUP_SCAN_DEFER_BELOW"
        的组暂存不消费（留给扫描完成后），两类队列都空时降级取暂存组。
        """
        loop = asyncio.get_running_loop()
        deadline = None if timeout is None else loop.time() + timeout
        while True:
            if self._large_done and self._small_done:
                return ([], -1)
            # 扫描已完成：保护区组原样放回 L 队列（tuple 整体 put，priority 保持）
            if self._deferred_items and self._scan_completed():
                for raw in self._deferred_items:
                    await self.large_queue.put(raw)
                logger.info(
                    f"[文件组调度] 扫描完成，{len(self._deferred_items)} 个保护区组"
                    f"放回大文件组队列")
                self._deferred_items.clear()
            choice = self._choose_queue()
            q = self.large_queue if choice == 'large' else self.small_queue
            other_q = self.small_queue if choice == 'large' else self.large_queue
            seq = None
            _raw = None
            _from_deferred = False
            try:
                _raw = q.get_nowait()
            except asyncio.QueueEmpty:
                try:
                    _raw = other_q.get_nowait()
                    q = other_q
                except asyncio.QueueEmpty:
                    # 两类队列都空 → 降级取保护区组（"没合适的可以用"）
                    if self._deferred_items:
                        _raw = self._deferred_items.pop(0)
                        q = self.large_queue
                        _from_deferred = True
                    else:
                        if deadline is not None and deadline - loop.time() <= 0:
                            return None
                        try:
                            _raw = await asyncio.wait_for(q.get(), timeout=2.0)
                        except asyncio.TimeoutError:
                            continue
            item, seq = self._unwrap(_raw)

            # 扫描期大组保护：L 队列取出的保护区组 → 暂存并继续取（不消耗配额）
            if (not _from_deferred and q is self.large_queue
                    and self._defer_below > 0 and not self._scan_completed()):
                _fc = self._item_file_count(item)
                if _fc is not None and _fc < self._defer_below:
                    self._deferred_items.append(_raw)
                    continue

            if seq is not None:
                self._queued_sizes_by_seq.pop(seq, None)
            # 结束标记（预取完成）
            if item and isinstance(item, tuple) and len(item) == 2 and not item[0]:
                if q is self.large_queue:
                    self._large_done = True
                else:
                    self._small_done = True
                continue
            if item and isinstance(item, tuple) and len(item) == 2 and item[0]:
                file_groups, last_processed_id = item
                if last_processed_id > self.last_processed_file_id:
                    self.last_processed_file_id = last_processed_id
                files_in_removed = sum(len(g) for g in file_groups)
                self.queued_files_count = max(0, self.queued_files_count - files_in_removed)
                # 成功取到组后推进配额（降级取组不计数）
                actual = 'large' if q is self.large_queue else 'small'
                self._commit_take(actual, choice)
            return item

    def get_queued_files_count(self) -> int:
        """获取队列中的总文件数（用于进度显示）
        
        返回累计放入队列的总文件数，而不是当前队列中的文件数。
        这样可以正确显示进度：已处理文件数/累计放入队列的总文件数。
        
        Returns:
            累计放入队列的总文件数（用于进度显示）
        """
        # 优先使用累计放入队列的总文件数
        if self.total_queued_files_count > 0:
            return self.total_queued_files_count
        # 如果累计数为0，可能是还没有文件放入队列，使用当前队列中的文件数
        # 但这种情况应该很少见，因为进度更新通常在文件放入队列后才开始
        return self.queued_files_count

    async def _prefetch_from_memory(self):
        """纯内存模式下的预取循环：从 InMemoryFileStore 读取文件并应用三级分组规则

        成组后直接从内存删除，下次始终从头部读取，无需游标定位。

        三级分组规则（与 fetch_pending_files_grouped_by_size 完全一致）：
        1. 超大文件独立成组：单文件 > MAX_FILE_SIZE * 95%
        2. 贪心累积：持续累加文件直到组大小 >= MAX_FILE_SIZE * 95%
        3. 扫描完成兜底：扫描完成后，剩余文件不论大小强制成组
        """
        logger.info(
            f"[文件组预取器-内存模式] ========== 内存预取循环已启动 ==========\n"
            f"backup_set_id={getattr(self.backup_set, 'id', None)}, "
            f"backup_task_id={getattr(self.backup_task, 'id', None)}, "
            f"队列容量={self.queue_maxsize}"
        )

        settings = get_settings()
        max_file_size = getattr(settings, 'MAX_FILE_SIZE', 12 * 1024 * 1024 * 1024) or (12 * 1024 * 1024 * 1024)
        tolerance = max_file_size * 0.05
        min_group_size = max_file_size - tolerance  # 95% 阈值

        batch_size = max(3000, min(int(max_file_size / (1024 * 1024) * 0.5 * 1000), 50000))

        # 跨轮累积：上一轮未凑够半组、未能成组的头部文件。
        # 它们仍留在内存库头部（未删除），下一轮按偏移跳过、只读新文件，
        # 与新文件一起继续累积 —— 否则每轮重复读同一批 5 万文件、永远不够
        # 半组，预分组会在小文件区停摆直到扫描完成。
        carry_group: List[Dict] = []
        carry_size = 0

        try:
            while self._running:
                self.prefetch_loop_count += 1

                # 从头部读取：带上 carry 的偏移量，保证读到"新文件"
                files = await self.in_memory_store.get_pending_files(
                    batch_size + len(carry_group))
                new_files = files[len(carry_group):] if carry_group else files

                if not files:
                    scan_done = await self.in_memory_store.is_scan_done()
                    if scan_done:
                        # 扫描完成且无文件，发送终止信号
                        # （_put_done_signal 兼容两种模式：调度模式发双队列，否则发旧队列）
                        await self._put_done_signal()
                        logger.info("[文件组预取器-内存模式] 扫描已完成且无更多文件，发送终止信号")
                        break
                    else:
                        # 扫描未完成，等待新文件
                        got_new = await self.in_memory_store.wait_for_new_files(timeout=5.0)
                        if not got_new:
                            await asyncio.sleep(2.0)
                        continue

                # 应用三级分组规则（从上一轮未成组的 carry 继续累积）
                current_group = list(carry_group)
                current_size = carry_size
                groups_to_queue = []
                # 收集所有成组文件的路径，用于最后一次性删除
                grouped_paths = []

                for raw_info in new_files:
                    # 分组只保留压缩需要的字段（path + size）：
                    # 原 dict 含 name/modified_time/permissions 等，约 400~500 B/文件；
                    # 精简后约 200~250 B，且分组后原 dict 即可从内存库释放。
                    file_path = raw_info.get('path') or raw_info.get('file_path')
                    file_size = int(raw_info.get('size', 0) or raw_info.get('file_size', 0) or 0)
                    file_info = {'path': file_path, 'size': file_size}

                    # 第一层：超大文件独立成组
                    if file_size > min_group_size:
                        if current_group:
                            groups_to_queue.append(current_group)
                            grouped_paths.extend(f.get('path', '') for f in current_group)
                            current_group = []
                            current_size = 0
                        groups_to_queue.append([file_info])
                        grouped_paths.append(file_path or '')
                        continue

                    # 第二层：贪心累积
                    current_group.append(file_info)
                    current_size += file_size

                    if current_size >= min_group_size:
                        groups_to_queue.append(current_group)
                        grouped_paths.extend(f.get('path', '') for f in current_group)
                        current_group = []
                        current_size = 0

                # 处理剩余的累积文件
                scan_done = await self.in_memory_store.is_scan_done()
                if scan_done and current_group:
                    # 扫描完成，强制成组
                    groups_to_queue.append(current_group)
                    grouped_paths.extend(f.get('path', '') for f in current_group)
                    current_group = []
                    current_size = 0
                elif current_group and current_size >= min_group_size * 0.5:
                    # 未完成扫描但足够大，成组
                    groups_to_queue.append(current_group)
                    grouped_paths.extend(f.get('path', '') for f in current_group)
                    current_group = []
                    current_size = 0

                # 未成组：保留到下一轮继续累积（文件仍在内存库头部，按偏移跳过）
                carry_group = current_group
                carry_size = current_size

                # 一次性从内存删除所有成组文件
                if grouped_paths:
                    await self.in_memory_store.remove_files(grouped_paths)

                # 将文件组放入队列（大小文件组按类型分队列；队列内按文件数排序）
                for group in groups_to_queue:
                    total_size = sum(f.get('size', 0) for f in group)
                    file_count = len(group)

                    if self.group_sched_enabled:
                        is_large = file_count < self.group_large_threshold
                        q = self.large_queue if is_large else self.small_queue
                        if is_large:
                            self._prefetched_large_groups += 1
                        else:
                            self._prefetched_small_groups += 1
                        # large：文件数升序（优先最大文件组/最快产出）
                        # small：文件数降序（优先最小文件组/最慢产出）
                        priority = file_count if is_large else -file_count
                        self._queue_seq += 1
                        seq = self._queue_seq
                        self._queued_sizes_by_seq[seq] = total_size
                        await q.put((priority, seq, total_size, ([group], 0)))
                        _qtype = 'large' if is_large else 'small'
                    else:
                        if self.file_group_queue.full():
                            logger.debug("[文件组预取器-内存模式] 队列已满，等待...")
                            await asyncio.sleep(2.0)
                        await self.file_group_queue.put(([group], 0))
                        self._queued_group_sizes.append(total_size)
                        _qtype = '-'

                    self.prefetched_groups += 1
                    self.total_queued_files_count += len(group)
                    self.total_queued_size += total_size
                    self.queued_files_count += len(group)

                    logger.debug(
                        f"[文件组预取器-内存模式] 预取文件组：{len(group)} 个文件"
                        f"（{_qtype}），大小={format_bytes(total_size)}，"
                        f"累计预取={self.prefetched_groups} 组，"
                        f"队列={self.get_queue_size()}/{self.queue_maxsize}"
                    )

                # 检查扫描是否完成且无剩余文件，发送终止信号
                if scan_done:
                    remaining = await self.in_memory_store.get_all_pending_files()
                    if not remaining:
                        await self._put_done_signal()
                        logger.info("[文件组预取器-内存模式] 所有文件已预取完成，发送终止信号")
                        break

                await asyncio.sleep(1.0)

        except asyncio.CancelledError:
            logger.info("[文件组预取器-内存模式] 预取循环被取消")
        except Exception as e:
            logger.error(f"[文件组预取器-内存模式] 预取循环异常: {e}", exc_info=True)
            # 记录异常：未分组的内存文件被放弃时，任务不得伪装成功
            self.prefetch_error = str(e)
            try:
                await self._put_done_signal()
            except Exception:
                pass
        finally:
            self._running = False
            logger.info(
                f"[文件组预取器-内存模式] 预取循环已结束，"
                f"累计预取 {self.prefetched_groups} 个文件组，"
                f"累计文件数 {self.total_queued_files_count}，"
                f"累计大小 {format_bytes(self.total_queued_size)}"
            )
    
    async def _prefetch_loop(self):
        """预取循环：持续从数据库获取文件组并放入队列

        逻辑：
        1. 保证队列始终是 3/3（满状态）
        2. 6秒轮询检测队列大小
        3. 如果文件大小不足无法组成新文件组，队列保持当前状态，直到有新文件组能入队
        4. 扫描任务完成后，如果不够一组，全库扫描避免遗漏，还不够1组，停止线程
        5. 预取器不能阻塞其他线程（所有操作都是异步的）
        """
        # 纯内存模式：使用专门的内存预取逻辑
        if self.in_memory_store is not None:
            await self._prefetch_from_memory()
            return

        logger.info(
            f"[文件组预取器] ========== 预取循环已启动 ==========\n"
            f"backup_set_id={self.backup_set.id}, backup_task_id={self.backup_task.id}, "
            f"队列容量={self.queue_maxsize}"
        )
        
        settings = get_settings()
        wait_retry_count = 0
        max_wait_retries = 6
        self.prefetch_loop_count = 0  # 预取循环计数（内存设置）
        queue_check_interval = 6.0  # 6秒轮询检测队列大小
        
        try:
            while self._running:
                self.prefetch_loop_count += 1
                try:
                    # 6秒轮询检测队列大小
                    current_queue_size = self.get_queue_size()
                    
                    # 如果队列未满，尝试填充
                    if current_queue_size < self.queue_maxsize:
                        # 队列未满，继续预取
                        logger.debug(
                            f"[文件组预取器] 队列未满（{current_queue_size}/{self.queue_maxsize}），"
                            f"继续预取文件组..."
                        )
                    else:
                        # 队列已满（3/3），6秒后再次检查
                        logger.debug(
                            f"[文件组预取器] 队列已满（{current_queue_size}/{self.queue_maxsize}），"
                            f"{queue_check_interval}秒后再次检查..."
                        )
                        await asyncio.sleep(queue_check_interval)
                        continue
                    
                    # 从数据库获取文件组
                    import time
                    retrieval_start_time = time.time()
                    
                    logger.debug(
                        f"[文件组预取器] [循环 #{self.prefetch_loop_count}] 开始检索文件组："
                        f"队列大小={self.get_queue_size()}/{self.queue_maxsize}, "
                        f"last_processed_id={self.last_processed_file_id}, "
                        f"max_file_size={settings.MAX_FILE_SIZE}"
                    )
                    
                    result = await self.backup_db.fetch_pending_files_grouped_by_size(
                        self.backup_set.id,
                        settings.MAX_FILE_SIZE,
                        self.backup_task.id,
                        should_wait_if_small=(wait_retry_count < max_wait_retries),
                        start_from_id=self.last_processed_file_id
                    )
                    
                    retrieval_elapsed = time.time() - retrieval_start_time
                    self.total_retrieval_time += retrieval_elapsed
                    
                    # 处理返回格式
                    if isinstance(result, tuple) and len(result) == 2:
                        file_groups, last_processed_id = result
                    else:
                        # 兼容旧格式
                        file_groups = result
                        last_processed_id = 0
                    
                    # 更新最后处理的文件ID
                    old_last_processed_id = self.last_processed_file_id
                    # 如果返回的 last_processed_id 为 0，说明检测到异常，需要重置查询起点
                    if last_processed_id == 0 and old_last_processed_id > 0:
                        logger.warning(
                            f"[文件组预取器] 检测到异常返回，重置 last_processed_id: "
                            f"{old_last_processed_id} -> 0（下次将从第一个未压缩文件开始查询）"
                        )
                        self.last_processed_file_id = 0
                    elif last_processed_id > self.last_processed_file_id:
                        self.last_processed_file_id = last_processed_id
                        logger.debug(
                            f"[文件组预取器] 更新 last_processed_id: "
                            f"{old_last_processed_id} -> {self.last_processed_file_id}"
                        )
                    
                    # 计算文件组中的文件总数和总大小（去重后）
                    # 对文件组进行去重：相同 file_path 只保留一个（虽然 fetch_pending_files_grouped_by_size 已经去重，但这里再次确保）
                    seen_paths_in_groups = set()
                    deduplicated_groups = []
                    duplicate_count_in_groups = 0
                    
                    if file_groups:
                        for group in file_groups:
                            deduplicated_group = []
                            for file_info in group:
                                file_path = file_info.get('path') or file_info.get('file_path')
                                if file_path:
                                    if file_path not in seen_paths_in_groups:
                                        seen_paths_in_groups.add(file_path)
                                        deduplicated_group.append(file_info)
                                    else:
                                        duplicate_count_in_groups += 1
                            if deduplicated_group:
                                deduplicated_groups.append(deduplicated_group)
                    
                    # 使用去重后的文件组计算文件数和大小
                    total_files_in_groups = sum(len(group) for group in deduplicated_groups) if deduplicated_groups else 0
                    total_size_in_groups = sum(
                        sum(file_info.get('size', 0) for file_info in group)
                        for group in deduplicated_groups
                    ) if deduplicated_groups else 0
                    
                    # 获取扫描状态用于日志输出（总是获取，无论是否有文件组）
                    scan_status = await self.backup_db.get_scan_status(self.backup_task.id)
                    
                    # 构建去重信息日志（始终显示去重数量，即使为0）
                    dedup_info = f"，去重（{duplicate_count_in_groups}）"
                    
                    logger.debug(
                        f"[文件组预取器] [循环 #{self.prefetch_loop_count}] 检索完成："
                        f"耗时 {retrieval_elapsed:.2f}秒 | "
                        f"文件组 {len(deduplicated_groups) if deduplicated_groups else 0} 个 | "
                        f"文件总数 {total_files_in_groups:,} 个{dedup_info} | "
                        f"当前组大小 {format_bytes(total_size_in_groups)} | "
                        f"累计总大小 {format_bytes(self.total_queued_size)} | "
                        f"last_processed_id={last_processed_id} | "
                        f"扫描状态={scan_status if scan_status else 'N/A'} | "
                        f"累计预取组数={self.prefetched_groups} | "
                        f"累计检索时间={self.total_retrieval_time:.2f}秒"
                    )
                    
                    # 将文件组放入队列（使用去重后的文件组）
                    if deduplicated_groups:
                        # 在放入队列前，直接设置 is_copy_success = TRUE
                        try:
                            dedup_log = f"，去重（{duplicate_count_in_groups}）"
                            logger.debug(
                                f"[文件组预取器] [循环 #{self.prefetch_loop_count}] 开始标记文件为已入队："
                                f"{len(deduplicated_groups)} 个文件组，共 {total_files_in_groups} 个文件{dedup_log}，"
                                f"总大小={format_bytes(total_size_in_groups)}"
                            )
                            await self.backup_db.mark_files_as_queued(
                                backup_set=self.backup_set,
                                file_groups=deduplicated_groups
                            )
                            logger.debug(
                                f"[文件组预取器] [循环 #{self.prefetch_loop_count}] 文件标记完成，"
                                f"is_copy_success 已设置为 TRUE"
                            )
                        except Exception as mark_error:
                            logger.error(
                                f"[文件组预取器] [循环 #{self.prefetch_loop_count}] ⚠️ 标记文件失败: {str(mark_error)}",
                                exc_info=True
                            )
                            # 即使标记失败，也继续放入队列，避免阻塞流程
                        
                        # 有文件组，放入队列（非阻塞，如果队列满则等待）
                        try:
                            # 使用 put_nowait 尝试非阻塞放入，如果队列满则等待
                            if self.file_group_queue.full():
                                logger.debug(
                                    f"[文件组预取器] 队列已满，等待放入文件组..."
                                )
                            await self.file_group_queue.put((deduplicated_groups, last_processed_id))
                            self.prefetched_groups += len(deduplicated_groups)
                            # 更新队列中的文件总数（放入队列时增加，使用去重后的文件数）
                            self.queued_files_count += total_files_in_groups
                            # 更新累计放入队列的总文件数（用于进度显示，不减少，使用去重后的文件数）
                            self.total_queued_files_count += total_files_in_groups
                            # 更新累计放入队列的总文件大小（用于进度显示，不减少，使用去重后的大小）
                            self.total_queued_size += total_size_in_groups
                            wait_retry_count = 0  # 重置等待计数
                            dedup_log = f"，去重（{duplicate_count_in_groups}）"
                            logger.info(
                                f"[文件组预取器] [循环 #{self.prefetch_loop_count}] ✅ 已预取 {len(deduplicated_groups)} 个文件组："
                                f"✅队列大小 {self.get_queue_size()}/{self.queue_maxsize} | "
                                f"文件数 {total_files_in_groups:,} 个{dedup_log} | "
                                f"当前组大小 {format_bytes(total_size_in_groups)} | "
                                f"累计总大小 {format_bytes(self.total_queued_size)} | "
                                f"当前队列文件数 {self.queued_files_count:,} | "
                                f"累计队列文件数 {self.total_queued_files_count:,}"
                            )
                        except Exception as put_error:
                            logger.error(
                                f"[文件组预取器] [循环 #{self.prefetch_loop_count}] 放入队列失败: {put_error}",
                                exc_info=True
                            )
                    else:
                        # 没有收到文件，检查扫描状态（如果之前没有获取，这里获取）
                        if scan_status is None:
                            scan_status = await self.backup_db.get_scan_status(self.backup_task.id)
                        
                        if scan_status == 'completed':
                            # 扫描任务已完成，如果不够一组，先进行全库扫描避免遗漏
                            logger.info(
                                f"[文件组预取器] [循环 #{self.prefetch_loop_count}] 扫描任务已完成，"
                                f"但未检索到文件组，进行全库扫描检查是否有遗漏..."
                            )
                            
                            # 全库扫描：查询所有未压缩文件
                            from utils.scheduler.db_utils import get_opengauss_connection
                            try:
                                async with get_opengauss_connection() as conn:
                                    full_search_timeout = 7200.0  # 7200秒超时（两小时）
                                    await conn.execute(f"SET LOCAL statement_timeout = '{int(full_search_timeout)}s'")
                                    
                                    logger.info(
                                        f"[文件组预取器] [循环 #{self.prefetch_loop_count}] 开始全库检索所有未压缩文件"
                                        f"（超时时间：{full_search_timeout}秒）..."
                                    )
                                    # 多表方案：根据 backup_set_db_id 决定物理表名，避免直接访问基础表 backup_files
                                    from utils.scheduler.db_utils import get_backup_files_table_by_set_id
                                    table_name = await get_backup_files_table_by_set_id(conn, self.backup_set.id)

                                    all_pending_rows = await asyncio.wait_for(
                                        conn.fetch(
                                            f"""
                                            SELECT id, file_path, file_name, directory_path, display_name, file_type,
                                                   file_size, file_permissions, modified_time, accessed_time
                                            FROM {table_name}
                                            WHERE backup_set_id = $1
                                              AND (is_copy_success = FALSE OR is_copy_success IS NULL)
                                              AND file_type = 'file'::backupfiletype
                                            ORDER BY id
                                            """,
                                            self.backup_set.id
                                        ),
                                        timeout=full_search_timeout
                                    )
                                    
                                    logger.info(
                                        f"[文件组预取器] [循环 #{self.prefetch_loop_count}] 全库检索完成，"
                                        f"找到 {len(all_pending_rows)} 个未压缩文件"
                                    )
                                    
                                    if all_pending_rows:
                                        # 全库扫描找到文件，重置 last_processed_file_id 为 0，立即重新检索
                                        logger.warning(
                                            f"[文件组预取器] [循环 #{self.prefetch_loop_count}] ⚠️ 全库扫描发现遗漏的未压缩文件："
                                            f"{len(all_pending_rows)} 个，重置 last_processed_file_id=0，立即重新检索..."
                                        )
                                        self.last_processed_file_id = 0
                                        self.is_rescanning_missing_files = True  # 标记正在重新检索
                                        
                                        # 立即重新检索，不等待，确保遗漏的文件能被处理
                                        result = await self.backup_db.fetch_pending_files_grouped_by_size(
                                            self.backup_set.id,
                                            settings.MAX_FILE_SIZE,
                                            self.backup_task.id,
                                            should_wait_if_small=False,  # 不再等待，立即返回
                                            start_from_id=0  # 从0开始重新检索
                                        )
                                        
                                        if isinstance(result, tuple) and len(result) == 2:
                                            file_groups, last_processed_id = result
                                        else:
                                            file_groups = result
                                            last_processed_id = 0
                                        
                                        # 更新最后处理的文件ID
                                        if last_processed_id > 0:
                                            self.last_processed_file_id = last_processed_id
                                        
                                        if file_groups:
                                            # 对文件组进行去重
                                            seen_paths_in_groups = set()
                                            deduplicated_groups = []
                                            duplicate_count_in_groups = 0
                                            
                                            for group in file_groups:
                                                deduplicated_group = []
                                                for file_info in group:
                                                    file_path = file_info.get('path') or file_info.get('file_path')
                                                    if file_path:
                                                        if file_path not in seen_paths_in_groups:
                                                            seen_paths_in_groups.add(file_path)
                                                            deduplicated_group.append(file_info)
                                                        else:
                                                            duplicate_count_in_groups += 1
                                                if deduplicated_group:
                                                    deduplicated_groups.append(deduplicated_group)
                                            
                                            # 使用去重后的文件组计算文件数和大小
                                            total_files_in_groups = sum(len(group) for group in deduplicated_groups)
                                            total_size_in_groups = sum(
                                                sum(f.get('size', 0) or 0 for f in group) for group in deduplicated_groups
                                            )
                                            
                                            # 标记文件为已入队
                                            try:
                                                dedup_log = f"，去重（{duplicate_count_in_groups}）"
                                                logger.info(
                                                    f"[文件组预取器] [循环 #{self.prefetch_loop_count}] 开始标记文件为已入队："
                                                    f"{len(deduplicated_groups)} 个文件组，共 {total_files_in_groups} 个文件{dedup_log}，"
                                                    f"总大小={format_bytes(total_size_in_groups)}"
                                                )
                                                await self.backup_db.mark_files_as_queued(
                                                    backup_set=self.backup_set,
                                                    file_groups=deduplicated_groups
                                                )
                                                logger.info(
                                                    f"[文件组预取器] [循环 #{self.prefetch_loop_count}] ✅ 文件标记完成，"
                                                    f"is_copy_success 已设置为 TRUE"
                                                )
                                            except Exception as mark_error:
                                                logger.error(
                                                    f"[文件组预取器] [循环 #{self.prefetch_loop_count}] ⚠️ 标记文件失败: {str(mark_error)}",
                                                    exc_info=True
                                                )
                                            
                                            # 有文件组，立即放入队列（使用去重后的文件组）
                                            for group in deduplicated_groups:
                                                try:
                                                    await self.file_group_queue.put(([group], self.last_processed_file_id))
                                                    self.prefetched_groups += 1
                                                    self.queued_files_count += len(group)
                                                    self.total_queued_files_count += len(group)
                                                    self.total_queued_size += sum(f.get('size', 0) or 0 for f in group)
                                                except Exception as e:
                                                    logger.error(f"[文件组预取器] 放入队列失败: {e}", exc_info=True)
                                            
                                            dedup_log = f"，去重（{duplicate_count_in_groups}）"
                                            logger.info(
                                                f"[文件组预取器] [循环 #{self.prefetch_loop_count}] ✅ 重新检索成功，"
                                                f"找到 {len(deduplicated_groups)} 个文件组，"
                                                f"总文件数={total_files_in_groups}{dedup_log}，"
                                                f"总大小={format_bytes(total_size_in_groups)}，"
                                                f"已放入队列"
                                            )
                                            self.is_rescanning_missing_files = False  # 重新检索完成
                                        else:
                                            logger.warning(
                                                f"[文件组预取器] [循环 #{self.prefetch_loop_count}] ⚠️ 重新检索未找到文件组，"
                                                f"但全库扫描发现 {len(all_pending_rows)} 个未压缩文件，"
                                                f"可能文件组大小不足，继续等待..."
                                            )
                                            # 保持 is_rescanning_missing_files = True，继续等待
                                        # 继续循环，等待更多文件或下次检索
                                        continue
                                    else:
                                        # 全库扫描也没有文件，再次调用 fetch_pending_files_grouped_by_size
                                        # 这次应该会返回文件组（即使大小不足，因为扫描已完成）
                                        logger.info(
                                            f"[文件组预取器] [循环 #{self.prefetch_loop_count}] 全库扫描确认没有遗漏文件，"
                                            f"再次调用 fetch_pending_files_grouped_by_size 获取文件组（即使大小不足）..."
                                        )
                                        # 再次查询，这次应该会返回文件组（因为扫描已完成）
                                        result = await self.backup_db.fetch_pending_files_grouped_by_size(
                                            self.backup_set.id,
                                            settings.MAX_FILE_SIZE,
                                            self.backup_task.id,
                                            should_wait_if_small=False,  # 不再等待
                                            start_from_id=self.last_processed_file_id
                                        )
                                        
                                        if isinstance(result, tuple) and len(result) == 2:
                                            file_groups, last_processed_id = result
                                        else:
                                            file_groups = result
                                            last_processed_id = self.last_processed_file_id
                                        
                                        if file_groups:
                                            # 对文件组进行去重
                                            seen_paths_in_groups = set()
                                            deduplicated_groups = []
                                            duplicate_count_in_groups = 0
                                            
                                            for group in file_groups:
                                                deduplicated_group = []
                                                for file_info in group:
                                                    file_path = file_info.get('path') or file_info.get('file_path')
                                                    if file_path:
                                                        if file_path not in seen_paths_in_groups:
                                                            seen_paths_in_groups.add(file_path)
                                                            deduplicated_group.append(file_info)
                                                        else:
                                                            duplicate_count_in_groups += 1
                                                if deduplicated_group:
                                                    deduplicated_groups.append(deduplicated_group)
                                            
                                            # 使用去重后的文件组计算文件数和大小
                                            total_files_in_groups = sum(len(group) for group in deduplicated_groups)
                                            total_size_in_groups = sum(
                                                sum(file_info.get('size', 0) for file_info in group)
                                                for group in deduplicated_groups
                                            )
                                            try:
                                                dedup_log = f"，去重（{duplicate_count_in_groups}）"
                                                logger.info(
                                                    f"[文件组预取器] [循环 #{self.prefetch_loop_count}] 开始标记文件为已入队："
                                                    f"{len(deduplicated_groups)} 个文件组，共 {total_files_in_groups} 个文件{dedup_log}，"
                                                    f"总大小={format_bytes(total_size_in_groups)}"
                                                )
                                                await self.backup_db.mark_files_as_queued(
                                                    backup_set=self.backup_set,
                                                    file_groups=deduplicated_groups
                                                )
                                                logger.info(
                                                    f"[文件组预取器] [循环 #{self.prefetch_loop_count}] ✅ 文件标记完成，"
                                                    f"is_copy_success 已设置为 TRUE"
                                                )
                                            except Exception as mark_error:
                                                logger.error(
                                                    f"[文件组预取器] [循环 #{self.prefetch_loop_count}] ⚠️ 标记文件失败: {str(mark_error)}",
                                                    exc_info=True
                                                )
                                            
                                            await self.file_group_queue.put((deduplicated_groups, last_processed_id))
                                            self.prefetched_groups += len(deduplicated_groups)
                                            self.last_processed_file_id = last_processed_id
                                            # 更新队列中的文件总数（放入队列时增加，使用去重后的文件数）
                                            self.queued_files_count += total_files_in_groups
                                            # 更新累计放入队列的总文件数（用于进度显示，不减少，使用去重后的文件数）
                                            self.total_queued_files_count += total_files_in_groups
                                            # 更新累计放入队列的总文件大小（用于进度显示，不减少，使用去重后的大小）
                                            self.total_queued_size += total_size_in_groups
                                            dedup_log = f"，去重（{duplicate_count_in_groups}）"
                                            logger.info(
                                                f"[文件组预取器] [循环 #{self.prefetch_loop_count}] ✅ 已预取 {len(deduplicated_groups)} 个文件组："
                                                f"✅队列大小 {self.get_queue_size()}/{self.queue_maxsize} | "
                                                f"文件数 {total_files_in_groups:,} 个{dedup_log} | "
                                                f"当前组大小 {format_bytes(total_size_in_groups)} | "
                                                f"累计总大小 {format_bytes(self.total_queued_size)} | "
                                                f"当前队列文件数 {self.queued_files_count:,} | "
                                                f"累计队列文件数 {self.total_queued_files_count:,}"
                                            )
                                            continue
                                        else:
                                            # 全库扫描后仍然没有文件组，停止线程
                                            logger.info(
                                                f"[文件组预取器] [循环 #{self.prefetch_loop_count}] 全库扫描后仍然没有文件组，"
                                                f"不够1组，停止预取线程"
                                            )
                                            # 更新任务状态，标记分组完成
                                            try:
                                                await self.backup_db.update_task_stage_with_description(
                                                    self.backup_task,
                                                    "prefetch",
                                                    "[分组完成] 所有文件已分组完成"
                                                )
                                                logger.info(f"[文件组预取器] ✅ 已更新任务状态为分组完成")
                                            except Exception as update_error:
                                                logger.warning(f"[文件组预取器] ⚠️ 更新任务状态失败: {str(update_error)}")
                                            # 放入结束信号（-1 表示结束），压缩循环会识别并停止
                                            await self.file_group_queue.put(([], -1))
                                            queue_size_after = self.get_queue_size()
                                            actual_groups = queue_size_after - 1  # 减去结束信号
                                            logger.info(
                                                f"[文件组预取器] [循环 #{self.prefetch_loop_count}] 已放入结束信号到队列 | "
                                                f"队列大小: {queue_size_after}/{self.queue_maxsize} "
                                                f"(实际文件组: {actual_groups}, 结束信号: 1)"
                                            )
                                            break
                            except asyncio.TimeoutError:
                                logger.error(
                                    f"[文件组预取器] [循环 #{self.prefetch_loop_count}] 全库扫描超时，"
                                    f"停止预取线程"
                                )
                                await self.file_group_queue.put(([], -1))  # -1 表示结束
                                break
                            except Exception as full_search_error:
                                logger.error(
                                    f"[文件组预取器] [循环 #{self.prefetch_loop_count}] 全库扫描失败: {full_search_error}，"
                                    f"停止预取线程",
                                    exc_info=True
                                )
                                await self.file_group_queue.put(([], -1))  # -1 表示结束
                                break
                        else:
                            # 扫描任务未完成，文件大小不足无法组成新文件组，队列保持当前状态
                            # 6秒后再次检查队列大小和扫描状态
                            logger.info(
                                f"[文件组预取器] [循环 #{self.prefetch_loop_count}] 扫描任务未完成"
                                f"（扫描状态={scan_status}），文件大小不足无法组成新文件组，"
                                f"队列保持当前状态（{current_queue_size}/{self.queue_maxsize}），"
                                f"{queue_check_interval}秒后再次检查..."
                            )
                            await asyncio.sleep(queue_check_interval)
                            continue
                
                except asyncio.CancelledError:
                    logger.info(f"[文件组预取器] [循环 #{self.prefetch_loop_count}] 预取循环被取消")
                    break
                except Exception as e:
                    error_type = type(e).__name__
                    error_msg = str(e)
                    
                    # 检查是否是 BufferError 相关错误
                    is_buffer_error = (
                        error_type == 'BufferError' or 
                        'unexpected trailing' in error_msg.lower() or
                        'buffer' in error_msg.lower() or
                        'AssertionError' in error_type
                    )
                    
                    if is_buffer_error:
                        logger.warning(
                            f"[文件组预取器] [循环 #{self.prefetch_loop_count}] 数据库缓冲区错误："
                            f"{error_type}: {error_msg}\n"
                            f"这通常是由于查询结果太大或网络传输问题导致的。"
                            f"数据库查询函数会自动重试并减小批次大小。"
                        )
                    else:
                        logger.error(
                            f"[文件组预取器] [循环 #{self.prefetch_loop_count}] 预取文件组时出错："
                            f"{error_type}: {error_msg}",
                            exc_info=True
                        )
                    
                    # 出错后等待一段时间再重试
                    logger.info(
                        f"[文件组预取器] [循环 #{self.prefetch_loop_count}] 5秒后重试..."
                    )
                    await asyncio.sleep(5.0)
        
        except Exception as e:
            logger.error(
                f"[文件组预取器] 预取循环异常（循环 #{self.prefetch_loop_count}）: {e}",
                exc_info=True
            )
            # 与内存模式一致：异常退出不算完成（引擎四合一据此判失败）
            self.prefetch_error = str(e)
        finally:
            avg_retrieval_time = (
                self.total_retrieval_time / self.prefetch_loop_count 
                if self.prefetch_loop_count > 0 else 0
            )
            # 计算队列中的实际文件组数（排除结束信号）
            queue_size = self.get_queue_size()
            # 结束信号是 ([], -1)，如果有结束信号，实际文件组数 = queue_size - 1
            actual_file_groups_in_queue = queue_size - (1 if queue_size > 0 else 0)
            
            logger.info("=" * 80)
            logger.info("[文件组预取器] ========== 预取循环已结束 ==========")
            logger.info(f"  总循环次数: {self.prefetch_loop_count}")
            logger.info(f"  预取文件组数: {self.prefetched_groups}")
            logger.info(f"  总检索时间: {self.total_retrieval_time:.2f} 秒")
            logger.info(f"  平均检索时间: {avg_retrieval_time:.2f} 秒/次")
            logger.info(f"  最后处理的文件ID: {self.last_processed_file_id}")
            logger.info(f"  队列大小: {queue_size}/{self.queue_maxsize} (实际文件组: {actual_file_groups_in_queue}, 结束信号: {1 if queue_size > actual_file_groups_in_queue else 0})")
            logger.info("=" * 80)
    
    def get_stats(self) -> Dict[str, Any]:
        """获取统计信息"""
        return {
            'prefetched_groups': self.prefetched_groups,
            'total_retrieval_time': self.total_retrieval_time,
            'queue_size': self.get_queue_size(),
            'last_processed_file_id': self.last_processed_file_id
        }

