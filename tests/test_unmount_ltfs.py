#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
测试 LTFS 卸载流程 - 不设超时限制，观察每步实际耗时
不依赖项目模块，直接调用系统命令
"""

import asyncio
import subprocess
import time
import sys


MOUNT_POINT = "/mnt/ltfs"


def is_mounted(path):
    """用 mountpoint 命令检查"""
    r = subprocess.run(["mountpoint", "-q", path], capture_output=True)
    return r.returncode == 0


def has_ltfs_process():
    """检查是否有 LTFS 进程（-x 精确匹配进程名，避免匹配到脚本自身）"""
    r = subprocess.run(["pgrep", "-x", "ltfs"], capture_output=True, text=True)
    if r.returncode != 0:
        return False, []
    pids = [p.strip() for p in r.stdout.strip().split("\n") if p.strip()]
    return True, pids


async def wait_ltfs_exit(max_wait=3600, poll_interval=5):
    """等待 LTFS 进程退出，不限制时间"""
    waited = 0
    while waited < max_wait:
        alive, pids = has_ltfs_process()
        if not alive:
            print(f"  LTFS 进程已退出（等待 {waited}s）")
            return True, waited
        print(f"  LTFS 进程仍在运行 (pids={pids}) ... ({waited}s)")
        await asyncio.sleep(poll_interval)
        waited += poll_interval
    return False, waited


async def main():
    print("=== LTFS 卸载测试（不设超时限制）===")
    print(f"挂载点: {MOUNT_POINT}")
    mounted = is_mounted(MOUNT_POINT)
    print(f"是否已挂载: {mounted}")
    alive, pids = has_ltfs_process()
    print(f"LTFS 进程: alive={alive}, pids={pids}")
    print()

    if not mounted:
        print("LTFS 未挂载，无需卸载")
        return

    # 步骤1: sync
    t0 = time.time()
    print("[1/4] sync ...")
    proc = await asyncio.create_subprocess_exec(
        "sync",
        stdout=asyncio.subprocess.PIPE,
        stderr=asyncio.subprocess.PIPE,
    )
    await asyncio.wait_for(proc.communicate(), timeout=300)
    print(f"[1/4] sync 完成 — {time.time()-t0:.1f}s")

    # 步骤2: fusermount -u
    t0 = time.time()
    print(f"[2/4] fusermount -u {MOUNT_POINT} ...")
    proc = await asyncio.create_subprocess_exec(
        "fusermount", "-u", MOUNT_POINT,
        stdout=asyncio.subprocess.PIPE,
        stderr=asyncio.subprocess.PIPE,
    )
    stdout, stderr = await asyncio.wait_for(proc.communicate(), timeout=600)
    elapsed = time.time() - t0
    err = stderr.decode("utf-8", errors="ignore").strip() if stderr else ""
    print(f"[2/4] fusermount 返回码: {proc.returncode} — {elapsed:.1f}s")
    if err:
        print(f"  stderr: {err}")

    if proc.returncode != 0:
        print("[2/4] fusermount 失败，尝试 umount -l ...")
        proc2 = await asyncio.create_subprocess_exec(
            "umount", "-l", MOUNT_POINT,
            stdout=asyncio.subprocess.PIPE,
            stderr=asyncio.subprocess.PIPE,
        )
        await proc2.communicate()
        await asyncio.sleep(2)

    # 步骤3: 检查 LTFS 进程（fusermount 成功后通常已退出）
    alive_after, pids_after = has_ltfs_process()
    if not alive_after:
        print(f"[3/4] LTFS 进程已退出（fusermount 返回时已清理完毕）")
    else:
        print(f"[3/4] LTFS 进程仍在运行 (pids={pids_after})，等待退出...")
        t0 = time.time()
        exited, waited = await wait_ltfs_exit(max_wait=3600, poll_interval=5)
        elapsed = time.time() - t0
        print(f"[3/4] LTFS 进程: exited={exited}, 等待={waited}s — 实际 {elapsed:.1f}s")

    # 步骤4: 验证
    still = is_mounted(MOUNT_POINT)
    print(f"[4/4] 挂载点仍然存在: {still}")

    if still:
        print("[4/4] 尝试 umount -l ...")
        proc3 = await asyncio.create_subprocess_exec(
            "umount", "-l", MOUNT_POINT,
            stdout=asyncio.subprocess.PIPE,
            stderr=asyncio.subprocess.PIPE,
        )
        await proc3.communicate()
        await asyncio.sleep(2)
        print(f"[4/4] umount -l 后: mounted={is_mounted(MOUNT_POINT)}")

    print()
    print(f"=== 最终: mounted={is_mounted(MOUNT_POINT)} ===")


if __name__ == "__main__":
    asyncio.run(main())
