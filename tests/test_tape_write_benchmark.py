#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
磁带写入性能基准测试
对比两种模式：
1. tar + 可变块模式 (setblk 0) - 兼容性好，但速度慢
2. LTFS 挂载 - 速度快，需要格式化磁带

注意：固定块模式不测试，因为文件大小不是块大小整数倍时会报错

使用方法:
    python tests/test_tape_write_benchmark.py --size 500    # 使用 500MB 测试文件
    python tests/test_tape_write_benchmark.py --file /path/to/file.tar.zst  # 使用指定文件
    python tests/test_tape_write_benchmark.py --skip-ltfs   # 只测试 tar 模式
"""

import argparse
import asyncio
import os
import shutil
import subprocess
import sys
import tempfile
import time
from datetime import datetime
from pathlib import Path
from typing import Optional, Tuple

# 添加项目根目录到路径
sys.path.insert(0, str(Path(__file__).parent.parent))

from config.settings import get_settings


def format_size(size_bytes: int) -> str:
    """格式化文件大小"""
    if size_bytes >= 1024 * 1024 * 1024:
        return f"{size_bytes / (1024*1024*1024):.2f} GB"
    elif size_bytes >= 1024 * 1024:
        return f"{size_bytes / (1024*1024):.2f} MB"
    elif size_bytes >= 1024:
        return f"{size_bytes / 1024:.2f} KB"
    return f"{size_bytes} bytes"


def format_speed(bytes_per_sec: float) -> str:
    """格式化速度"""
    if bytes_per_sec >= 1024 * 1024:
        return f"{bytes_per_sec / (1024*1024):.2f} MB/s"
    elif bytes_per_sec >= 1024:
        return f"{bytes_per_sec / 1024:.2f} KB/s"
    return f"{bytes_per_sec:.2f} bytes/s"


def create_test_file(size_mb: int, output_dir: Path) -> Path:
    """创建指定大小的测试文件"""
    test_file = output_dir / f"benchmark_test_{size_mb}mb.bin"
    print(f"创建测试文件: {test_file} ({size_mb} MB)")

    # 使用 dd 创建随机数据文件
    cmd = ['dd', 'if=/dev/urandom', f'of={test_file}', f'bs=1M', f'count={size_mb}']
    result = subprocess.run(cmd, capture_output=True, text=True)

    if result.returncode != 0:
        # 如果 urandom 不可用，使用零填充
        cmd = ['dd', 'if=/dev/zero', f'of={test_file}', f'bs=1M', f'count={size_mb}']
        result = subprocess.run(cmd, capture_output=True, text=True)
        if result.returncode != 0:
            raise RuntimeError(f"创建测试文件失败: {result.stderr}")

    print(f"测试文件创建完成: {format_size(test_file.stat().st_size)}")
    return test_file


def check_tape_status(tape_device: str) -> bool:
    """检查磁带设备状态"""
    print(f"\n检查磁带设备: {tape_device}")

    result = subprocess.run(['mt', '-f', tape_device, 'status'],
                          capture_output=True, text=True)
    print(f"  状态: {result.stdout.strip()}")

    if 'ONLINE' not in result.stdout:
        print("  [警告] 磁带不在线")
        return False

    return True


def rewind_tape(tape_device: str) -> bool:
    """回绕磁带"""
    print(f"回绕磁带: {tape_device}")
    result = subprocess.run(['mt', '-f', tape_device, 'rewind'],
                          capture_output=True, text=True, timeout=60)
    if result.returncode == 0:
        print("  回绕成功")
        return True
    else:
        print(f"  回绕失败: {result.stderr}")
        return False


def set_variable_block_mode(tape_device: str) -> bool:
    """设置可变块模式"""
    print(f"设置可变块模式: mt -f {tape_device} setblk 0")
    result = subprocess.run(['mt', '-f', tape_device, 'setblk', '0'],
                          capture_output=True, text=True, timeout=30)
    if result.returncode == 0:
        print("  设置成功")
        return True
    else:
        print(f"  设置失败: {result.stderr}")
        return False


def test_tar_variable_block(tape_device: str, source_file: Path) -> Tuple[bool, float, float, str]:
    """
    测试 tar + 可变块模式写入磁带

    Returns:
        (success, elapsed_time, speed_mb_s, error_message)
    """
    print("\n" + "=" * 60)
    print("测试1: tar + 可变块模式 (setblk 0)")
    print("=" * 60)

    file_size = source_file.stat().st_size
    parent_dir = source_file.parent
    file_name = source_file.name

    # 回绕并设置可变块模式
    if not rewind_tape(tape_device):
        return False, 0, 0, "回绕失败"
    if not set_variable_block_mode(tape_device):
        return False, 0, 0, "设置可变块模式失败"

    # 使用 tar 写入
    cmd = [
        'tar', '-cvb', '512', '-f', tape_device,
        f'--label=benchmark_tar_variable',
        '-C', str(parent_dir),
        file_name
    ]

    print(f"命令: {' '.join(cmd)}")
    print(f"文件大小: {format_size(file_size)}")

    start_time = time.time()

    try:
        result = subprocess.run(cmd, capture_output=True, text=True, timeout=3600)
        elapsed_time = time.time() - start_time
        speed = file_size / elapsed_time if elapsed_time > 0 else 0

        stderr_text = result.stderr or ""

        if result.returncode != 0:
            # 检查是否是空间不足
            if 'No space left' in stderr_text or 'ENOSPC' in stderr_text:
                return False, elapsed_time, speed / (1024*1024), f"磁带空间不足: {stderr_text[:200]}"
            return False, elapsed_time, speed / (1024*1024), f"tar 返回错误 ({result.returncode}): {stderr_text[:200]}"

        print(f"\n结果:")
        print(f"  耗时: {elapsed_time:.2f} 秒")
        print(f"  速度: {format_speed(speed)}")

        return True, elapsed_time, speed / (1024*1024), ""

    except subprocess.TimeoutExpired:
        elapsed_time = time.time() - start_time
        return False, elapsed_time, 0, "操作超时"
    except Exception as e:
        elapsed_time = time.time() - start_time
        return False, elapsed_time, 0, str(e)


def format_ltfs(tape_device: str, volume_name: str = "BENCHMARK") -> Tuple[bool, str]:
    """
    格式化磁带为 LTFS 格式

    Returns:
        (success, error_message)
    """
    print(f"\n格式化磁带为 LTFS: {tape_device}")

    # -s 必须是6个字符的序列号
    cmd = ['/usr/local/bin/mkltfs', '-d', tape_device, '-n', volume_name, '-s', 'BENCH1', '-f']
    print(f"命令: {' '.join(cmd)}")

    result = subprocess.run(cmd, capture_output=True, text=True, timeout=120)

    if result.returncode == 0:
        print("  格式化成功")
        return True, ""
    else:
        return False, f"格式化失败: {result.stderr}"


def mount_ltfs(tape_device: str, mount_point: Path, auto_format: bool = True) -> Tuple[bool, str]:
    """
    挂载 LTFS

    Args:
        tape_device: 磁带设备路径
        mount_point: 挂载点
        auto_format: 如果挂载失败是否自动格式化

    Returns:
        (success, error_message)
    """
    print(f"\n挂载 LTFS: {tape_device} -> {mount_point}")

    # 创建挂载点
    mount_point.mkdir(parents=True, exist_ok=True)

    # 先尝试卸载（如果已挂载）
    subprocess.run(['fusermount', '-u', str(mount_point)],
                  capture_output=True, timeout=30)

    # 挂载 LTFS
    cmd = ['/usr/local/bin/ltfs', '-o', f'devname={tape_device}', str(mount_point)]
    print(f"命令: {' '.join(cmd)}")

    # LTFS 挂载会阻塞，需要在后台运行
    process = subprocess.Popen(
        cmd,
        stdout=subprocess.PIPE,
        stderr=subprocess.PIPE,
        text=True
    )

    # 等待挂载完成
    print("  等待挂载...")
    max_wait = 30
    waited = 0
    while waited < max_wait:
        time.sleep(1)
        waited += 1
        # 检查挂载点是否有内容
        if mount_point.exists():
            try:
                # 检查是否是 LTFS 文件系统
                result = subprocess.run(['mount'], capture_output=True, text=True)
                if str(mount_point) in result.stdout:
                    print(f"  挂载成功 (等待 {waited} 秒)")
                    return True, ""
            except:
                pass

    # 超时，检查进程状态
    if process.poll() is None:
        # 进程还在运行，可能挂载成功了
        print(f"  挂载进程运行中，假设成功")
        return True, ""

    # 进程已退出，读取错误
    _, stderr = process.communicate(timeout=5)

    # 如果挂载失败且启用了自动格式化
    if auto_format and ('LTFS11005' in stderr or 'not formatted' in stderr.lower() or 'format' in stderr.lower()):
        print(f"  挂载失败，尝试格式化...")
        success, format_error = format_ltfs(tape_device)
        if not success:
            return False, f"自动格式化失败: {format_error}"

        # 重新尝试挂载
        print("  格式化完成，重新挂载...")
        process = subprocess.Popen(
            cmd,
            stdout=subprocess.PIPE,
            stderr=subprocess.PIPE,
            text=True
        )

        waited = 0
        while waited < max_wait:
            time.sleep(1)
            waited += 1
            try:
                result = subprocess.run(['mount'], capture_output=True, text=True)
                if str(mount_point) in result.stdout:
                    print(f"  挂载成功 (等待 {waited} 秒)")
                    return True, ""
            except:
                pass

        if process.poll() is None:
            print(f"  挂载进程运行中，假设成功")
            return True, ""

        _, stderr = process.communicate(timeout=5)

    return False, f"LTFS 挂载失败: {stderr}"


def unmount_ltfs(mount_point: Path) -> bool:
    """卸载 LTFS"""
    print(f"\n卸载 LTFS: {mount_point}")

    # 使用 fusermount 卸载
    result = subprocess.run(['fusermount', '-u', str(mount_point)],
                          capture_output=True, text=True, timeout=60)

    if result.returncode == 0:
        print("  卸载成功")
        return True
    else:
        # 尝试 lazy unmount
        print(f"  普通卸载失败，尝试 lazy unmount: {result.stderr}")
        result = subprocess.run(['fusermount', '-u', '-z', str(mount_point)],
                              capture_output=True, text=True, timeout=30)
        if result.returncode == 0:
            print("  lazy 卸载成功")
            return True
        print(f"  卸载失败: {result.stderr}")
        return False


def test_ltfs_copy(tape_device: str, source_file: Path, mount_point: Path) -> Tuple[bool, float, float, str]:
    """
    测试 LTFS 挂载后复制文件

    Returns:
        (success, elapsed_time, speed_mb_s, error_message)
    """
    print("\n" + "=" * 60)
    print("测试2: LTFS 挂载 + 文件复制")
    print("=" * 60)

    file_size = source_file.stat().st_size
    file_name = source_file.name

    # 格式化磁带为 LTFS
    success, error = format_ltfs(tape_device)
    if not success:
        return False, 0, 0, f"格式化失败: {error}"

    # 挂载 LTFS
    success, error = mount_ltfs(tape_device, mount_point, auto_format=False)
    if not success:
        return False, 0, 0, error

    try:
        # 目标路径
        target_dir = mount_point / "benchmark_test"
        target_dir.mkdir(parents=True, exist_ok=True)
        target_file = target_dir / file_name

        print(f"源文件: {source_file}")
        print(f"目标文件: {target_file}")
        print(f"文件大小: {format_size(file_size)}")

        # 开始复制
        print("\n开始复制...")
        start_time = time.time()

        shutil.copy2(str(source_file), str(target_file))

        elapsed_time = time.time() - start_time
        speed = file_size / elapsed_time if elapsed_time > 0 else 0

        # 验证文件大小
        target_size = target_file.stat().st_size
        if target_size != file_size:
            return False, elapsed_time, speed / (1024*1024), f"文件大小不匹配: 源={file_size}, 目标={target_size}"

        print(f"\n结果:")
        print(f"  耗时: {elapsed_time:.2f} 秒")
        print(f"  速度: {format_speed(speed)}")

        return True, elapsed_time, speed / (1024*1024), ""

    except Exception as e:
        return False, 0, 0, str(e)
    finally:
        # 卸载 LTFS
        unmount_ltfs(mount_point)


def main():
    """主测试函数"""
    parser = argparse.ArgumentParser(description='磁带写入性能基准测试')
    parser.add_argument('--size', type=int, default=500, help='测试文件大小 (MB)，默认 500')
    parser.add_argument('--file', type=str, help='使用指定的测试文件')
    parser.add_argument('--skip-ltfs', action='store_true', help='跳过 LTFS 测试')
    parser.add_argument('--skip-tar', action='store_true', help='跳过 tar 测试')
    parser.add_argument('--tape-device', type=str, help='指定磁带设备，默认从配置读取')
    args = parser.parse_args()

    print("=" * 70)
    print("磁带写入性能基准测试")
    print(f"时间: {datetime.now().strftime('%Y-%m-%d %H:%M:%S')}")
    print("=" * 70)

    settings = get_settings()
    tape_device = args.tape_device or settings.TAPE_DRIVE_LETTER

    print(f"\n配置:")
    print(f"  磁带设备: {tape_device}")
    print(f"  测试文件大小: {args.size} MB")

    # 检查磁带设备
    if not tape_device.startswith('/dev/'):
        print(f"\n[错误] {tape_device} 不是 Linux 磁带设备路径")
        return False

    if not os.path.exists(tape_device):
        print(f"\n[错误] 磁带设备不存在: {tape_device}")
        return False

    if not check_tape_status(tape_device):
        print("\n[错误] 磁带设备状态异常")
        return False

    # 准备测试文件
    test_file = None
    if args.file:
        test_file = Path(args.file)
        if not test_file.exists():
            print(f"\n[错误] 指定的测试文件不存在: {test_file}")
            return False
    else:
        # 创建临时测试文件
        temp_dir = Path(tempfile.mkdtemp(prefix='tape_benchmark_'))
        try:
            test_file = create_test_file(args.size, temp_dir)
        except Exception as e:
            print(f"\n[错误] 创建测试文件失败: {e}")
            return False

    print(f"\n测试文件: {test_file}")
    print(f"文件大小: {format_size(test_file.stat().st_size)}")

    # 结果存储
    results = {
        'tar_variable': None,
        'ltfs': None
    }

    # 测试1: tar + 可变块模式
    if not args.skip_tar:
        success, elapsed, speed, error = test_tar_variable_block(tape_device, test_file)
        results['tar_variable'] = {
            'success': success,
            'elapsed': elapsed,
            'speed': speed,
            'error': error
        }

    # 测试2: LTFS（会自动格式化）
    if not args.skip_ltfs:
        mount_point = Path('/mnt/ltfs_benchmark')
        success, elapsed, speed, error = test_ltfs_copy(tape_device, test_file, mount_point)
        results['ltfs'] = {
            'success': success,
            'elapsed': elapsed,
            'speed': speed,
            'error': error
        }

    # 清理临时文件
    if not args.file and test_file:
        try:
            shutil.rmtree(test_file.parent)
            print(f"\n清理临时目录: {test_file.parent}")
        except:
            pass

    # 输出对比结果
    print("\n" + "=" * 70)
    print("性能对比结果")
    print("=" * 70)

    print(f"\n{'测试方式':<25} {'结果':<8} {'耗时(秒)':<12} {'速度(MB/s)':<15} {'备注'}")
    print("-" * 70)

    if results['tar_variable']:
        r = results['tar_variable']
        status = "成功" if r['success'] else "失败"
        elapsed = f"{r['elapsed']:.2f}" if r['elapsed'] > 0 else "N/A"
        speed = f"{r['speed']:.2f}" if r['speed'] > 0 else "N/A"
        note = r['error'][:30] if r['error'] else "-"
        print(f"{'tar + 可变块 (setblk 0)':<25} {status:<8} {elapsed:<12} {speed:<15} {note}")

    if results['ltfs']:
        r = results['ltfs']
        status = "成功" if r['success'] else "失败"
        elapsed = f"{r['elapsed']:.2f}" if r['elapsed'] > 0 else "N/A"
        speed = f"{r['speed']:.2f}" if r['speed'] > 0 else "N/A"
        note = r['error'][:30] if r['error'] else "-"
        print(f"{'LTFS 挂载复制':<25} {status:<8} {elapsed:<12} {speed:<15} {note}")

    # 计算性能差异
    successful_results = {k: v for k, v in results.items() if v and v['success']}
    if len(successful_results) >= 2:
        # 找出最快的
        fastest = max(successful_results.items(), key=lambda x: x[1]['speed'])
        print(f"\n结论: {fastest[0]} 最快，速度 {fastest[1]['speed']:.2f} MB/s")

        # 对比可变块 vs LTFS
        if 'tar_variable' in successful_results and 'ltfs' in successful_results:
            tar_speed = successful_results['tar_variable']['speed']
            ltfs_speed = successful_results['ltfs']['speed']
            if tar_speed > 0:
                ratio = ltfs_speed / tar_speed
                print(f"  LTFS 比可变块快 {ratio:.2f} 倍")

    print("\n" + "=" * 70)

    return all(r['success'] for r in results.values() if r is not None)


if __name__ == "__main__":
    try:
        success = main()
        sys.exit(0 if success else 1)
    except KeyboardInterrupt:
        print("\n\n[中断] 用户取消测试")
        sys.exit(130)
    except Exception as e:
        print(f"\n[异常] {e}")
        import traceback
        traceback.print_exc()
        sys.exit(1)
