"""tape_device_lock — 磁带设备的进程内互斥（跨线程 / 跨事件循环）

背景（2026-10-25 生产故障）：
  读标签 API 的 `mt rewind` 在磁带未加载时阻塞 60 秒，此时用户点击"格式化"，
  mkltfs 启动瞬间撞上 `Device or resource busy` → 格式化失败 → 钉钉误报。
  两个操作分别跑在 hypercorn 事件循环和格式化线程的独立事件循环里，
  asyncio.Lock 无法跨循环生效，因此用 fcntl.flock 的**进程内**文件锁。

用法：
    from utils.tape_device_lock import tape_device_lock
    with tape_device_lock("read_label", timeout=300):
        ...  mt / mkltfs / ltfs 操作 ...

注意：锁只在最外层入口使用。被嵌套调用的内部方法不要再拿锁
（flock 对不同 fd 不可重入，会自己等自己）。
"""
import contextlib
import fcntl
import logging
import os
import threading
import time

logger = logging.getLogger(__name__)

_LOCK_FILE = '/tmp/.taf_tape_device.lock'
# 同一进程内再加一层线程锁：fcntl 的 flock 在部分文件系统上是进程级语义，
# 同进程不同线程可能都拿到锁；这里保证线程间也严格互斥。
_thread_lock = threading.Lock()


@contextlib.contextmanager
def tape_device_lock(purpose: str = '', timeout: float = 300.0):
    """获取磁带设备独占权（超时抛 TimeoutError）

    purpose: 仅用于日志，标注持有者（如 read_label / format / mount）
    timeout: 最长等待秒数（格式化可能持续很久，调用方自行设置）
    """
    acquired = _thread_lock.acquire(timeout=timeout)
    if not acquired:
        raise TimeoutError(f"等待磁带设备线程锁超时（{purpose}）")
    f = None
    try:
        f = open(_LOCK_FILE, 'a+')
        deadline = time.time() + max(1.0, timeout)
        while True:
            try:
                fcntl.flock(f.fileno(), fcntl.LOCK_EX | fcntl.LOCK_NB)
                break
            except (BlockingIOError, OSError):
                if time.time() >= deadline:
                    raise TimeoutError(f"等待磁带设备锁超时（{purpose}）")
                time.sleep(0.5)
        if purpose:
            logger.debug(f"[磁带锁] 已获取（{purpose}）")
        yield
    finally:
        if f is not None:
            try:
                fcntl.flock(f.fileno(), fcntl.LOCK_UN)
            except Exception:
                pass
            try:
                f.close()
            except Exception:
                pass
        try:
            _thread_lock.release()
        except Exception:
            pass
        if purpose:
            logger.debug(f"[磁带锁] 已释放（{purpose}）")


def is_tape_busy_error(msg: str) -> bool:
    """判断命令输出是否为设备忙（另一个操作正在占用设备）"""
    m = (msg or '').lower()
    return 'busy' in m or 'resource temporarily unavailable' in m


@contextlib.asynccontextmanager
async def async_tape_device_lock(purpose: str = '', timeout: float = 300.0):
    """异步版本：获取锁的等待放到线程里，避免阻塞事件循环"""
    import asyncio
    cm = tape_device_lock(purpose, timeout)
    await asyncio.to_thread(cm.__enter__)
    try:
        yield
    finally:
        try:
            cm.__exit__(None, None, None)
        except Exception:
            pass
