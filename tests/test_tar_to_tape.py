#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
测试 tar 写入磁带设备
"""

import os
import sys
import subprocess
import asyncio
from pathlib import Path
from datetime import datetime

# 添加项目根目录到路径
sys.path.insert(0, str(Path(__file__).parent.parent))

from config.settings import get_settings


def test_tar_with_absolute_path(tape_device: str, source_file: Path) -> tuple:
    """
    测试使用绝对路径的 tar 命令（会产生警告）
    """
    print("\n" + "=" * 60)
    print("测试1: 使用绝对路径（会产生警告）")
    print("=" * 60)

    cmd = [
        'tar', '-cvf', tape_device,
        '--label=test_absolute',
        str(source_file)
    ]

    print(f"命令: {' '.join(cmd)}")

    # 使用 subprocess.run 安全执行（不使用 shell）
    result = subprocess.run(cmd, capture_output=True, text=True)

    print(f"返回码: {result.returncode}")
    if result.stdout:
        print(f"stdout: {result.stdout[:500]}")
    if result.stderr:
        print(f"stderr: {result.stderr[:500]}")

    return result.returncode == 0, result.stderr


def test_tar_with_relative_path(tape_device: str, source_file: Path) -> tuple:
    """
    测试使用 -C 参数的 tar 命令（避免绝对路径警告）
    """
    print("\n" + "=" * 60)
    print("测试2: 使用 -C 参数（相对路径）")
    print("=" * 60)

    parent_dir = source_file.parent
    file_name = source_file.name

    cmd = [
        'tar', '-cvf', tape_device,
        '--label=test_relative',
        '-C', str(parent_dir),
        file_name
    ]

    print(f"命令: {' '.join(cmd)}")

    # 使用 subprocess.run 安全执行（不使用 shell）
    result = subprocess.run(cmd, capture_output=True, text=True)

    print(f"返回码: {result.returncode}")
    if result.stdout:
        print(f"stdout: {result.stdout[:500]}")
    if result.stderr:
        print(f"stderr: {result.stderr[:500]}")

    return result.returncode == 0, result.stderr


async def test_tar_async(tape_device: str, source_file: Path) -> tuple:
    """
    测试异步执行 tar 命令（模拟实际代码）
    """
    print("\n" + "=" * 60)
    print("测试3: 异步执行 tar 命令")
    print("=" * 60)

    parent_dir = source_file.parent
    file_name = source_file.name

    cmd = [
        'tar', '-cvf', tape_device,
        '--label=test_async',
        '-C', str(parent_dir),
        file_name
    ]

    print(f"命令: {' '.join(cmd)}")

    # 使用 asyncio.create_subprocess_exec 安全执行（不使用 shell）
    process = await asyncio.create_subprocess_exec(
        *cmd,
        stdout=asyncio.subprocess.PIPE,
        stderr=asyncio.subprocess.PIPE
    )
    stdout, stderr = await process.communicate()

    stdout_text = stdout.decode() if stdout else ''
    stderr_text = stderr.decode() if stderr else ''

    print(f"返回码: {process.returncode}")
    if stdout_text:
        print(f"stdout: {stdout_text[:500]}")
    if stderr_text:
        print(f"stderr: {stderr_text[:500]}")

    # 过滤警告信息
    if stderr_text and '删除开头' not in stderr_text:
        print(f"[警告] stderr 有非预期内容: {stderr_text}")

    return process.returncode == 0, stderr_text


def check_tape_device(tape_device: str) -> bool:
    """检查磁带设备是否存在"""
    print(f"\n检查磁带设备: {tape_device}")

    if not tape_device.startswith('/dev/'):
        print(f"  不是 Linux 磁带设备路径")
        return False

    if not os.path.exists(tape_device):
        print(f"  设备不存在")
        return False

    # 检查设备状态
    result = subprocess.run(['mt', '-f', tape_device, 'status'],
                          capture_output=True, text=True)
    print(f"  mt status: {result.stdout.strip() or result.stderr.strip()}")

    return True


def rewind_tape(tape_device: str) -> bool:
    """回绕磁带"""
    print(f"\n回绕磁带: {tape_device}")
    result = subprocess.run(['mt', '-f', tape_device, 'rewind'],
                          capture_output=True, text=True)
    if result.returncode == 0:
        print("  回绕成功")
        return True
    else:
        print(f"  回绕失败: {result.stderr}")
        return False


def list_tape_contents(tape_device: str) -> list:
    """列出磁带内容"""
    print(f"\n列出磁带内容: {tape_device}")
    result = subprocess.run(['tar', '-tvf', tape_device],
                          capture_output=True, text=True)
    if result.returncode == 0:
        print(f"  内容:\n{result.stdout[:1000]}")
        return result.stdout.split('\n')
    else:
        print(f"  失败: {result.stderr}")
        return []


def main():
    """主测试函数"""
    print("=" * 60)
    print("tar 写磁带设备测试")
    print(f"时间: {datetime.now().strftime('%Y-%m-%d %H:%M:%S')}")
    print("=" * 60)

    settings = get_settings()
    tape_device = settings.TAPE_DRIVE_LETTER

    print(f"\n配置:")
    print(f"  TAPE_DRIVE_LETTER: {tape_device}")

    # 检查磁带设备
    if not check_tape_device(tape_device):
        print("\n[错误] 磁带设备检查失败")
        return False

    # 查找测试文件
    final_dir = Path(settings.BACKUP_COMPRESS_DIR) / "final"
    test_file = None

    # 从 final 目录找一个测试文件
    if final_dir.exists():
        for subdir in final_dir.iterdir():
            if subdir.is_dir():
                for f in subdir.glob("*.tar.zst"):
                    if f.stat().st_size < 1024 * 1024 * 1024:  # 小于 1GB
                        test_file = f
                        break
                    elif test_file is None:  # 如果没有小文件，用第一个
                        test_file = f
            if test_file:
                break

    if not test_file:
        # 创建一个临时测试文件
        print("\n没有找到合适的测试文件，创建临时文件...")
        test_file = Path("/tmp/test_tape_write.txt")
        test_file.write_text(f"测试文件 - {datetime.now()}")
    else:
        print(f"\n使用测试文件: {test_file}")
        print(f"文件大小: {test_file.stat().st_size / (1024*1024):.2f} MB")

    # 回绕磁带
    rewind_tape(tape_device)

    # 测试2: 相对路径（推荐方式）
    success2, stderr2 = test_tar_with_relative_path(tape_device, test_file)

    # 回绕磁带并验证内容
    rewind_tape(tape_device)
    contents = list_tape_contents(tape_device)

    # 打印结果
    print("\n" + "=" * 60)
    print("测试结果:")
    print("=" * 60)
    print(f"  测试 (相对路径): {'成功' if success2 else '失败'}")
    print(f"    stderr 内容: {stderr2[:200] if stderr2 else '(空)'}")
    print(f"  磁带内容验证: {'成功' if contents else '失败'}")

    return success2


if __name__ == "__main__":
    try:
        success = main()
        print("\n" + "=" * 60)
        print(f"测试完成: {'成功' if success else '失败'}")
        print("=" * 60)
        sys.exit(0 if success else 1)
    except Exception as e:
        print(f"\n[异常] {e}")
        import traceback
        traceback.print_exc()
        sys.exit(1)
