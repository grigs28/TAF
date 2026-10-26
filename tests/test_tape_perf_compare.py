#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
磁带写入性能对比测试
对比 tar 可变块 vs LTFS

使用方法:
    python tests/test_tape_perf_compare.py --size 2000
"""

import argparse
import os
import shutil
import subprocess
import sys
import tempfile
import time
from datetime import datetime
from pathlib import Path

sys.path.insert(0, str(Path(__file__).parent.parent))
from config.settings import get_settings


def run_cmd(cmd, timeout=300, check=True):
    """运行命令并返回结果"""
    print(f"  $ {' '.join(cmd)}")
    result = subprocess.run(cmd, capture_output=True, text=True, timeout=timeout)
    if result.stdout:
        print(f"    {result.stdout.strip()[:500]}")
    if result.stderr and result.returncode != 0:
        print(f"    [stderr] {result.stderr.strip()[:300]}")
    if check and result.returncode != 0:
        raise RuntimeError(f"命令失败: {' '.join(cmd)}")
    return result


def format_size(size_bytes):
    if size_bytes >= 1024**3:
        return f"{size_bytes / 1024**3:.2f} GB"
    elif size_bytes >= 1024**2:
        return f"{size_bytes / 1024**2:.2f} MB"
    return f"{size_bytes} bytes"


def format_speed(bytes_per_sec):
    if bytes_per_sec >= 1024**2:
        return f"{bytes_per_sec / 1024**2:.2f} MB/s"
    return f"{bytes_per_sec / 1024:.2f} KB/s"


def reset_tape_device():
    """重置磁带设备"""
    print("\n[重置磁带设备]")
    try:
        run_cmd(['sg_reset', '-d', '-N', '/dev/sg2'], check=False)
        time.sleep(2)
    except:
        pass


def create_test_file(size_mb):
    """创建测试文件"""
    print(f"\n[创建测试文件 {size_mb}MB]")
    temp_dir = Path(tempfile.mkdtemp(prefix='tape_test_'))
    test_file = temp_dir / f"test_{size_mb}mb.bin"

    cmd = ['dd', 'if=/dev/urandom', f'of={test_file}', 'bs=1M', f'count={size_mb}']
    run_cmd(cmd, timeout=600)

    actual_size = test_file.stat().st_size
    print(f"  文件创建完成: {test_file} ({format_size(actual_size)})")
    return test_file


def test_ltfs(test_file, mount_point):
    """测试 LTFS 挂载复制"""
    print("\n" + "=" * 60)
    print("测试1: LTFS 挂载复制")
    print("=" * 60)

    file_size = test_file.stat().st_size
    file_name = test_file.name

    # 重置设备
    reset_tape_device()

    # 格式化磁带
    print("\n[格式化磁带为 LTFS]")
    run_cmd(['/usr/local/bin/mkltfs', '-d', '/dev/nst0', '-n', 'TEST', '-s', 'TST001', '-f'], timeout=120)

    # 创建挂载点
    mount_point.mkdir(parents=True, exist_ok=True)

    # 挂载 LTFS（后台运行）
    print("\n[挂载 LTFS]")
    mount_proc = subprocess.Popen(
        ['/usr/local/bin/ltfs', '-o', 'devname=/dev/nst0', str(mount_point)],
        stdout=subprocess.PIPE, stderr=subprocess.PIPE, text=True
    )

    # 等待挂载
    print("  等待挂载...")
    time.sleep(5)

    # 检查挂载
    for i in range(10):
        result = subprocess.run(['mount'], capture_output=True, text=True)
        if str(mount_point) in result.stdout:
            print(f"  挂载成功")
            break
        time.sleep(1)
    else:
        # 进程还在运行就算成功
        if mount_proc.poll() is None:
            print("  挂载进程运行中")
        else:
            raise RuntimeError("LTFS 挂载失败")

    # 列出磁带内容（挂载前）
    print("\n[磁带内容 - 写入前]")
    run_cmd(['ls', '-la', str(mount_point)], check=False)

    # 复制文件
    print("\n[复制文件到 LTFS]")
    target_file = mount_point / file_name

    start_time = time.time()
    shutil.copy2(str(test_file), str(target_file))
    elapsed = time.time() - start_time
    speed = file_size / elapsed if elapsed > 0 else 0

    print(f"\n  耗时: {elapsed:.2f} 秒")
    print(f"  速度: {format_speed(speed)}")

    # 列出磁带内容（写入后）
    print("\n[磁带内容 - 写入后]")
    run_cmd(['ls', '-la', str(mount_point)], check=False)

    # 验证文件
    if target_file.exists():
        target_size = target_file.stat().st_size
        print(f"  验证: 目标文件大小 {format_size(target_size)}")
        if target_size == file_size:
            print("  验证: 文件大小匹配 ✓")
        else:
            print("  验证: 文件大小不匹配 ✗")

    # 卸载
    print("\n[卸载 LTFS]")
    run_cmd(['fusermount', '-u', str(mount_point)], check=False)
    time.sleep(2)

    return {
        'success': True,
        'elapsed': elapsed,
        'speed': speed / (1024**2)  # MB/s
    }


def test_tar_variable(test_file):
    """测试 tar 可变块模式"""
    print("\n" + "=" * 60)
    print("测试2: tar + 可变块模式")
    print("=" * 60)

    file_size = test_file.stat().st_size
    file_name = test_file.name
    parent_dir = test_file.parent

    # 重置设备
    reset_tape_device()

    # 设置可变块模式
    print("\n[设置可变块模式]")
    run_cmd(['mt', '-f', '/dev/nst0', 'setblk', '0'])

    # 回绕
    print("\n[回绕磁带]")
    run_cmd(['mt', '-f', '/dev/nst0', 'rewind'])

    # 列出磁带内容（写入前 - 应该是空的或无法读取）
    print("\n[磁带内容 - 写入前]")
    result = run_cmd(['tar', '-tvf', '/dev/nst0'], check=False, timeout=60)
    if result.returncode != 0:
        print("  (磁带为空或无法读取)")

    # 写入
    print("\n[tar 写入磁带]")
    start_time = time.time()

    cmd = ['tar', '-cvb', '512', '-f', '/dev/nst0', '-C', str(parent_dir), file_name]
    run_cmd(cmd, timeout=3600)

    elapsed = time.time() - start_time
    speed = file_size / elapsed if elapsed > 0 else 0

    print(f"\n  耗时: {elapsed:.2f} 秒")
    print(f"  速度: {format_speed(speed)}")

    # 回绕并列出磁带内容
    print("\n[回绕磁带]")
    run_cmd(['mt', '-f', '/dev/nst0', 'rewind'])

    print("\n[磁带内容 - 写入后]")
    run_cmd(['tar', '-tvf', '/dev/nst0'], check=False, timeout=120)

    return {
        'success': True,
        'elapsed': elapsed,
        'speed': speed / (1024**2)  # MB/s
    }


def main():
    parser = argparse.ArgumentParser(description='磁带写入性能对比测试')
    parser.add_argument('--size', type=int, default=2000, help='测试文件大小 (MB)')
    parser.add_argument('--skip-ltfs', action='store_true', help='跳过 LTFS 测试')
    parser.add_argument('--skip-tar', action='store_true', help='跳过 tar 测试')
    args = parser.parse_args()

    print("=" * 60)
    print("磁带写入性能对比测试")
    print(f"时间: {datetime.now().strftime('%Y-%m-%d %H:%M:%S')}")
    print(f"测试文件大小: {args.size} MB")
    print("=" * 60)

    # 创建测试文件
    test_file = create_test_file(args.size)
    mount_point = Path('/mnt/ltfs_test')

    results = {}

    # 测试1: LTFS
    if not args.skip_ltfs:
        try:
            results['ltfs'] = test_ltfs(test_file, mount_point)
        except Exception as e:
            print(f"\n[LTFS 测试失败] {e}")
            results['ltfs'] = {'success': False, 'error': str(e)}

    # 测试2: tar 可变块
    if not args.skip_tar:
        try:
            results['tar_variable'] = test_tar_variable(test_file)
        except Exception as e:
            print(f"\n[tar 测试失败] {e}")
            results['tar_variable'] = {'success': False, 'error': str(e)}

    # 清理
    print("\n[清理临时文件]")
    try:
        shutil.rmtree(test_file.parent)
    except:
        pass

    # 输出结果
    print("\n" + "=" * 60)
    print("性能对比结果")
    print("=" * 60)

    print(f"\n{'测试方式':<25} {'结果':<8} {'耗时(秒)':<12} {'速度(MB/s)':<15}")
    print("-" * 60)

    if 'ltfs' in results:
        r = results['ltfs']
        if r['success']:
            print(f"{'LTFS 挂载复制':<25} {'成功':<8} {r['elapsed']:<12.2f} {r['speed']:<15.2f}")
        else:
            print(f"{'LTFS 挂载复制':<25} {'失败':<8} {'N/A':<12} {'N/A':<15}")

    if 'tar_variable' in results:
        r = results['tar_variable']
        if r['success']:
            print(f"{'tar + 可变块':<25} {'成功':<8} {r['elapsed']:<12.2f} {r['speed']:<15.2f}")
        else:
            print(f"{'tar + 可变块':<25} {'失败':<8} {'N/A':<12} {'N/A':<15}")

    # 对比
    if 'ltfs' in results and 'tar_variable' in results:
        if results['ltfs']['success'] and results['tar_variable']['success']:
            ltfs_speed = results['ltfs']['speed']
            tar_speed = results['tar_variable']['speed']
            if tar_speed > 0:
                ratio = ltfs_speed / tar_speed
                print(f"\n结论: LTFS 比 tar 可变块快 {ratio:.2f} 倍")

    print("\n" + "=" * 60)


if __name__ == "__main__":
    try:
        main()
    except KeyboardInterrupt:
        print("\n\n[中断]")
        sys.exit(130)
    except Exception as e:
        print(f"\n[错误] {e}")
        import traceback
        traceback.print_exc()
        sys.exit(1)
