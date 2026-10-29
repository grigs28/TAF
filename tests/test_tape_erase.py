#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
磁带擦除测试程序
Tape Erase Test with Progress Bar

使用方法:
    python tests/test_tape_erase.py [device_path]

示例:
    python tests/test_tape_erase.py /dev/nst0
    python tests/test_tape_erase.py  # 默认使用 /dev/nst0
"""

import sys
import time
import subprocess
from pathlib import Path

# 添加项目根目录到路径
project_root = Path(__file__).parent.parent
sys.path.insert(0, str(project_root))


def print_progress_bar(progress: float, width: int = 50, prefix: str = "", suffix: str = ""):
    """打印进度条

    Args:
        progress: 进度百分比 (0-100)
        width: 进度条宽度
        prefix: 前缀文字
        suffix: 后缀文字
    """
    filled = int(width * progress / 100)
    bar = "█" * filled + "░" * (width - filled)
    percent = f"{progress:.1f}%"

    # 清除当前行并打印新内容
    line = f"\r{prefix}|{bar}| {percent:>6} {suffix}"
    sys.stdout.write(line)
    sys.stdout.flush()


def get_tape_status(device: str) -> dict:
    """获取磁带状态"""
    try:
        result = subprocess.run(
            ['mt', '-f', device, 'status'],
            stdout=subprocess.PIPE,
            stderr=subprocess.PIPE,
            timeout=30,
            text=False
        )

        output = (result.stdout.decode('utf-8', errors='ignore') +
                  result.stderr.decode('utf-8', errors='ignore'))
        output_lower = output.lower()

        import re
        file_match = re.search(r'file\s*number\s*[:=]?\s*(\d+)', output_lower)
        block_match = re.search(r'block\s*number\s*[:=]?\s*(\d+)', output_lower)

        return {
            'success': result.returncode == 0,
            'online': 'online' in output_lower,
            'file_number': int(file_match.group(1)) if file_match else -1,
            'block_number': int(block_match.group(1)) if block_match else -1,
            'raw_output': output
        }
    except Exception as e:
        return {'success': False, 'error': str(e)}


def check_tape_empty(device: str) -> bool:
    """检查磁带是否为空（使用 mt eod 方法）"""
    print("\n📋 检查磁带是否为空...")

    # 获取初始状态
    status = get_tape_status(device)
    if not status.get('success'):
        print(f"❌ 无法获取磁带状态")
        return False

    print(f"   当前位置: file={status.get('file_number')}, block={status.get('block_number')}")

    if status.get('file_number', 0) > 0 or status.get('block_number', 0) > 0:
        print(f"   结论: 磁带不为空")
        return False

    # file=0, block=0 可能在开头，需要用 eod 验证
    print("   使用 mt eod 验证...")

    try:
        # 移动到数据末尾
        subprocess.run(
            ['mt', '-f', device, 'eod'],
            stdout=subprocess.PIPE,
            stderr=subprocess.PIPE,
            timeout=60
        )

        # 再次检查位置
        status2 = get_tape_status(device)
        file_after = status2.get('file_number', 0)
        block_after = status2.get('block_number', 0)

        print(f"   eod 后位置: file={file_after}, block={block_after}")

        # 倒回开头
        subprocess.run(
            ['mt', '-f', device, 'rewind'],
            stdout=subprocess.PIPE,
            stderr=subprocess.PIPE,
            timeout=60
        )

        if file_after == 0 and block_after == 0:
            print(f"   结论: 磁带为空 ✅")
            return True
        else:
            print(f"   结论: 磁带不为空（有 {file_after} 个文件）")
            return False

    except Exception as e:
        print(f"   ⚠️ 检查失败: {e}")
        return False


def erase_tape_with_progress(device: str) -> bool:
    """擦除磁带并显示进度"""

    print("\n" + "=" * 60)
    print("🔧 磁带擦除工具")
    print("=" * 60)
    print(f"📍 设备: {device}")

    # 步骤1: 检查磁带状态
    print_progress_bar(0, prefix="检查状态: ", suffix="获取磁带信息...")
    status = get_tape_status(device)

    if not status.get('success'):
        print(f"\n❌ 无法获取磁带状态")
        print(f"   输出: {status.get('raw_output', '')[:200]}")
        return False

    if not status.get('online'):
        print(f"\n❌ 磁带不在线")
        return False

    print_progress_bar(10, prefix="检查状态: ", suffix="磁带在线 ✓")
    time.sleep(0.3)

    # 步骤2: 检查是否为空
    print_progress_bar(20, prefix="检查数据: ", suffix="检查磁带内容...")
    is_empty = check_tape_empty(device)

    if is_empty:
        print_progress_bar(50, prefix="检查数据: ", suffix="磁带为空，无需擦除")
        print("\n\n✅ 磁带为空，不需要擦除")
        return True

    # 步骤3: 倒带
    print_progress_bar(30, prefix="倒带:     ", suffix="正在倒带...")
    try:
        result = subprocess.run(
            ['mt', '-f', device, 'rewind'],
            stdout=subprocess.PIPE,
            stderr=subprocess.PIPE,
            timeout=60
        )
        if result.returncode != 0:
            print(f"\n❌ 倒带失败")
            return False
    except Exception as e:
        print(f"\n❌ 倒带异常: {e}")
        return False

    print_progress_bar(40, prefix="倒带:     ", suffix="倒带完成 ✓")
    time.sleep(0.3)

    # 步骤4: 擦除（这是最耗时的部分）
    print_progress_bar(45, prefix="擦除:     ", suffix="开始擦除（这可能需要几分钟）...")
    print()  # 换行，因为擦除会阻塞

    print("\n⚠️  正在擦除磁带，请勿操作设备...")
    print("    (mt erase 是阻塞操作，进度无法精确显示)\n")

    start_time = time.time()

    try:
        # 使用 Popen 来尝试监控进度
        process = subprocess.Popen(
            ['mt', '-f', device, 'erase'],
            stdout=subprocess.PIPE,
            stderr=subprocess.PIPE,
            stdin=subprocess.DEVNULL
        )

        # 模拟进度（因为 mt erase 没有进度输出）
        # 短擦除通常需要 10-60 秒
        simulated_duration = 30  # 假设 30 秒
        elapsed = 0

        while process.poll() is None:
            elapsed = time.time() - start_time
            # 进度从 45% 到 90%
            progress = min(90, 45 + (elapsed / simulated_duration) * 45)
            elapsed_str = f"{elapsed:.0f}s"
            print_progress_bar(progress, prefix="擦除:     ", suffix=f"已耗时 {elapsed_str}...")
            time.sleep(0.5)

        # 进程结束
        stdout, stderr = process.communicate()
        elapsed = time.time() - start_time

        if process.returncode != 0:
            print(f"\n❌ 擦除失败 (返回码: {process.returncode})")
            print(f"   stderr: {stderr.decode('utf-8', errors='ignore')[:200]}")
            return False

        print_progress_bar(90, prefix="擦除:     ", suffix=f"完成 ✓ (耗时 {elapsed:.1f}s)")
        print()

    except subprocess.TimeoutExpired:
        print(f"\n❌ 擦除超时")
        return False
    except Exception as e:
        print(f"\n❌ 擦除异常: {e}")
        return False

    # 步骤5: 倒带并设置变长块
    print_progress_bar(92, prefix="完成:     ", suffix="最终倒带...")
    try:
        subprocess.run(
            ['mt', '-f', device, 'rewind'],
            stdout=subprocess.PIPE,
            stderr=subprocess.PIPE,
            timeout=60
        )
    except:
        pass

    print_progress_bar(95, prefix="完成:     ", suffix="设置变长块...")
    try:
        subprocess.run(
            ['mt', '-f', device, 'setblk', '0'],
            stdout=subprocess.PIPE,
            stderr=subprocess.PIPE,
            timeout=30
        )
    except:
        pass

    print_progress_bar(100, prefix="完成:     ", suffix="磁带准备就绪 ✓")
    print()

    # 最终验证
    print("\n📊 最终状态:")
    final_status = get_tape_status(device)
    print(f"   在线: {'是' if final_status.get('online') else '否'}")
    print(f"   位置: file={final_status.get('file_number')}, block={final_status.get('block_number')}")

    print("\n" + "=" * 60)
    print("✅ 磁带擦除完成！")
    print("=" * 60)

    return True


def main():
    # 获取设备路径
    if len(sys.argv) > 1:
        device = sys.argv[1]
    else:
        device = "/dev/nst0"

    print(f"\n🎯 目标设备: {device}")
    print("⚠️  警告: 擦除将删除磁带上的所有数据！")

    # 确认操作
    try:
        confirm = input("\n确认擦除? (输入 'yes' 继续): ")
        if confirm.lower() != 'yes':
            print("❌ 操作已取消")
            return 1
    except KeyboardInterrupt:
        print("\n❌ 操作已取消")
        return 1

    # 执行擦除
    success = erase_tape_with_progress(device)

    return 0 if success else 1


if __name__ == "__main__":
    sys.exit(main())
