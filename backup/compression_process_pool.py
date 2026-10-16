"""multiprocessing 压缩工作进程池（spawn）

线程池中执行压缩时，tarfile 纯 Python 打包与扫描/压缩互抢 GIL，
输出被压到 ~15MB/s。改为独立工作进程（各自独立 GIL），从根上解决：

- 必须用 spawn（不 fork 主进程，避免继承 DB 连接/事件循环）
- worker 只做打包压缩，结果/日志/进度全部经 Queue 回主进程
- 主进程池生命周期由 CompressionWorker 管理（启动/停止/取消）
"""
import asyncio
import logging
import multiprocessing as mp
import traceback
from typing import Dict, Any

logger = logging.getLogger(__name__)


class _ProgressDict(dict):
    """progress dict 的代理：把写入事件转成进度消息"""
    def __init__(self, progress_q, base=None):
        super().__init__(base or {})
        self._q = progress_q

    def __setitem__(self, key, value):
        super().__setitem__(key, value)
        if self._q is not None:
            self._q.put({'k': key, 'v': value})


def _compress_worker_entry(payload: Dict[str, Any]):
    """子进程入口：执行单个文件组压缩（复用现有模块级函数）

    payload: {archive_path, file_group, level, threads, total_files, base_processed_files}
    """
    from backup.compressor import _compress_with_zstd_cli
    from models.backup import BackupTask

    class _ShimTask:
        """backup_task 的轻量替身：进度字段与 BackupTask 兼容"""
        def __init__(self, progress_q=None, source_paths=None):
            self._progress_q = progress_q
            self.source_paths = source_paths or []
            self.progress_percent = 0.0
            self.current_compression_progress = {}

        def __setattr__(self, name, value):
            object.__setattr__(self, name, value)
            if name in ('progress_percent', 'current_compression_progress') and self._progress_q is not None:
                self._progress_q.put({'a': name, 'v': value})

    task = _ShimTask(payload.get('progress_queue'), payload.get('source_paths'))
    # 必须使用代理字典：or 短路时非空原字典会导致泵完全失效（进度始终为 0）
    compress_progress = _ProgressDict(payload.get('progress_queue'), payload.get('compress_progress'))
    try:
        result = _compress_with_zstd_cli(
            payload['archive_path'],
            payload['file_group'],
            task,
            payload['level'],
            payload['threads'],
            compress_progress,
            payload['total_files'],
            payload['base_processed_files'],
        )
        return result
    except BaseException as e:
        return {
            'ok': False,
            'error': f'{type(e).__name__}: {e}',
            'traceback': traceback.format_exc(),
        }


async def _submit_compression_payload(payload: dict, loop=None) -> asyncio.Future:
    """提交一组压缩任务（payload 为可 pickle 的数据）到 spawn 工作进程。

    仅接受数据型 payload（见 _compress_worker_entry 字段约定），
    避免传入嵌套绑定方法（无法 pickle）。
    """
    if loop is None:
        loop = asyncio.get_event_loop()

    pool = CompressionProcessPool.get_instance()
    return await pool.submit(payload)


class CompressionProcessPool:
    """spawn 压缩进程池：lazy 创建，生命周期由调用方管理

    单例：CompressionWorker 生命周期内复用同一个池（备份结束后可显式关闭）。
    """
    _instance = None

    def __init__(self, max_workers: int = 8):
        self.max_workers = max(1, int(max_workers))
        self._ctx = mp.get_context('spawn')
        self._executor = None
        self._pump_task = None
        self._shutdown = False
        self._progress_q = None
        self._progress_task = None
        self._current_backup_task = None
        self._current_compress_progress = None

    @classmethod
    def get_instance(cls, max_workers: int = 16) -> "CompressionProcessPool":
        if cls._instance is None or cls._instance._shutdown:
            cls._instance = cls(max_workers=max(16, int(max_workers)))
        return cls._instance

    async def start(self):
        """启动进程池（lazy）"""
        if self._executor is not None:
            return
        from concurrent.futures import ProcessPoolExecutor
        self._executor = ProcessPoolExecutor(
            max_workers=self.max_workers,
            mp_context=self._ctx,
        )
        self._progress_q = self._ctx.Manager().Queue()
        self._pump_task = None
        logger.info(f"[压缩进程池] spawn 工作进程池已启动（workers={self.max_workers}）")

    def set_progress_context(self, backup_task, compress_progress):
        """登记当前备份任务的进度接收对象（由父进程泵实时更新）"""
        self._current_backup_task = backup_task
        self._current_compress_progress = compress_progress
        if self._progress_task is None:
            self._progress_task = asyncio.create_task(self._progress_pump())

    def set_progress_context(self, backup_task, compress_progress):
        """登记当前备份任务的进度接收对象（由父进程泵实时更新）"""
        self._current_backup_task = backup_task
        self._current_compress_progress = compress_progress
        if self._progress_task is None:
            self._progress_task = asyncio.create_task(self._progress_pump())

    async def _progress_pump(self):
        """把子进程的进度事件实时回写到 backup_task / compress_progress"""
        try:
            while True:
                try:
                    item = await asyncio.to_thread(self._progress_q.get, True, 0.5)
                except Exception:
                    if self._shutdown:
                        break
                    continue
                try:
                    if 'a' in item:
                        if self._current_backup_task is not None:
                            setattr(self._current_backup_task, item['a'], item['v'])
                    elif 'k' in item and self._current_compress_progress is not None:
                        self._current_compress_progress[item['k']] = item['v']
                except Exception:
                    pass
        except asyncio.CancelledError:
            pass

    async def submit(self, work_or_payload) -> asyncio.Future:
        """提交可 pickle 的同步可调用对象或 payload，返回 asyncio.Future（与原线程池 future 语义一致）"""
        if self._executor is None:
            await self.start()
        loop = asyncio.get_running_loop()
        if isinstance(work_or_payload, dict):
            if self._progress_q is not None:
                work_or_payload['progress_queue'] = self._progress_q
            return loop.run_in_executor(self._executor, _compress_worker_entry, work_or_payload)
        return loop.run_in_executor(self._executor, work_or_payload)

    async def shutdown(self):
        """关闭进程池（先等正在执行的任务结束，避免硬杀）"""
        self._shutdown = True
        if self._executor is not None:
            self._executor.shutdown(wait=True)
            self._executor = None
        if self._pump_task is not None:
            self._pump_task.cancel()
            self._pump_task = None
        logger.info("[压缩进程池] 已关闭")

    async def terminate(self):
        """取消时强制终止（用于停止任务/服务关闭）"""
        self._shutdown = True
        if self._executor is not None:
            # shutdown 会等正在执行的任务，进程内部 BrokenPipe 逻辑会尽快退出
            self._executor.shutdown(wait=False, cancel_futures=True)
            self._executor = None
        if self._pump_task is not None:
            self._pump_task.cancel()
            self._pump_task = None
        logger.warning("[压缩进程池] 已强制终止")
