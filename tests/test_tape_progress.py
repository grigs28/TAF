#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
测试 tar 写磁带进度显示
"""

import os
import sys
import asyncio
import subprocess
from pathlib import Path
from datetime import datetime

sys.path.insert(0, str(Path(__file__).parent.parent))

from config.settings import get_settings


async def test_tar_to_tape(tape_device: str, source_file: Path, label: str = "test_progress"):
    """
    tar 写入磁带测试（定期输出进度）
    """
    print("\n" + "=" * 60)
    print("测试: tar 写入磁带（定期输出进度）")
    print("=" * 60)

    parent_dir = source_file.parent
    file_name = source_file.name
    file_size = source_file.stat().st_size
    file_size_mb = file_size / (1024 * 1024)

    print(f"源文件: {source_file}")
    print(f"文件大小: {file_size_mb:.2f} MB")
    print(f"磁带设备: {tape_device}")
    print(f"卷标: {label}")

    # 回绕磁带
    print("\n回绕磁带...")
    subprocess.run(['mt', '-f', tape_device, 'rewind'], capture_output=True)
    print("回绕完成")

    cmd = [
        'tar', '-cvf', tape_device,
        f'--label={label}',
        '-C', str(parent_dir),
        file_name
    ]

    print(f"\n[Linux磁带] 文件大小: {file_size_mb:.2f} MB，开始写入...")
    print("-" * 60)

    start_time = asyncio.get_event_loop().time()
    stop_monitor = False

    # 启动进程
    process = await asyncio.create_subprocess_exec(
        *cmd,
        stdout=asyncio.subprocess.PIPE,
        stderr=asyncio.subprocess.PIPE
    )

    # 进度监控任务（每30秒输出一次）
    async def monitor_progress():
        while not stop_monitor:
            await asyncio.sleep(30)
            if not stop_monitor:
                elapsed = asyncio.get_event_loop().time() - start_time
                print(f"[Linux磁带] 写入进行中... 已耗时: {elapsed:.0f} 秒")

    monitor_task = asyncio.create_task(monitor_progress())

    try:
        stdout, stderr = await process.communicate()
    finally:
        stop_monitor = True
        monitor_task.cancel()
        try:
            await monitor_task
        except asyncio.CancelledError:
            pass

    elapsed = asyncio.get_event_loop().time() - start_time
    speed_mb = file_size_mb / elapsed if elapsed > 0 else 0

    stderr_text = stderr.decode() if stderr else ''

    print("-" * 60)

    if process.returncode == 0:
        print(f"[Linux磁带] ✅ 文件已写入磁带: {file_name} ({file_size_mb:.2f} MB, 耗时: {elapsed:.0f}秒, 速度: {speed_mb:.2f} MB/s)")

        # 验证磁带内容
        print("\n验证磁带内容...")
        subprocess.run(['mt', '-f', tape_device, 'rewind'], capture_output=True)
        result = subprocess.run(['tar', '-tvf', tape_device], capture_output=True, text=True)
        if result.returncode == 0:
            print(f"磁带内容:\n{result.stdout[:500]}")
        return True
    else:
        print(f"[Linux磁带] ❌ tar 写入失败 (返回码={process.returncode}): {stderr_text}")
        return False


async def main():
    print("=" * 60)
    print("tar 写磁带进度显示测试")
    print(f"时间: {datetime.now().strftime('%Y-%m-%d %H:%M:%S')}")
    print("=" * 60)

    settings = get_settings()
    tape_device = settings.TAPE_DRIVE_LETTER

    print(f"\n配置:")
    print(f"  TAPE_DRIVE_LETTER: {tape_device}")

    # 检查磁带设备
    if not tape_device.startswith('/dev/'):
        print("[错误] 不是 Linux 磁带设备")
        return False

    if not os.path.exists(tape_device):
        print(f"[错误] 磁带设备不存在: {tape_device}")
        return False

    # 检查磁带状态
    result = subprocess.run(['mt', '-f', tape_device, 'status'],
                          capture_output=True, text=True)
    print(f"\n磁带状态:\n{result.stdout or result.stderr}")

    # 查找测试文件
    final_dir = Path(settings.BACKUP_COMPRESS_DIR) / "final"
    test_file = None

    if final_dir.exists():
        for subdir in final_dir.iterdir():
            if subdir.is_dir():
                for f in sorted(subdir.glob("*.tar.zst")):
                    test_file = f
                    break
            if test_file:
                break

    if not test_file:
        print("[错误] 没有找到测试文件")
        return False

    print(f"\n使用测试文件: {test_file}")

    # 测试
    success = await test_tar_to_tape(tape_device, test_file, "test_progress")

    print("\n" + "=" * 60)
    print(f"测试完成: {'成功' if success else '失败'}")
    print("=" * 60)

    return success


if __name__ == "__main__":
    try:
        success = asyncio.run(main())
        sys.exit(0 if success else 1)
    except KeyboardInterrupt:
        print("\n用户中断")
        sys.exit(1)
    except Exception as e:
        print(f"\n[异常] {e}")
        import traceback
        traceback.print_exc()
        sys.exit(1)
