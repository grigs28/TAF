#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
磁带挂载测试 - 函数化版本
"""
import asyncio
import sys
import shutil
from pathlib import Path
from datetime import datetime

sys.path.insert(0, str(Path(__file__).parent.parent))

from backup.tape_label_generator import generate_tape_label_and_serial
from backup.utils import extract_label_year_month
from config.settings import get_settings
from utils.dingtalk_notifier import DingTalkNotifier
from tape.tape_operations import TapeOperations
from backup.tape_handler import TapeHandler
from tape.tape_manager import TapeManager

LTFS_BIN = "/usr/local/bin/ltfs"
DEVICE = "/dev/sg2"
MOUNT_POINT = "/mnt/ltfs"


class TapeMountTest:
    """磁带挂载测试类"""

    def __init__(self):
        self.settings = get_settings()
        self.tape_ops = None
        self.tape_manager = None
        self.tape_handler = None
        self.label_info = None
        self.is_ltfs = False
        self.is_empty = False

    async def notify_error(self, error_msg: str):
        """发送钉钉错误通知"""
        try:
            notifier = DingTalkNotifier()
            await notifier.send_backup_notification(
                "磁带挂载测试",
                "failed",
                {'error': error_msg}
            )
            print("✓ 钉钉通知已发送")
        except Exception as e:
            print(f"✗ 钉钉通知发送失败: {e}")

    async def run_cmd(self, cmd, timeout=30):
        """执行命令"""
        print(f"$ {' '.join(cmd)}")
        proc = await asyncio.create_subprocess_exec(
            *cmd, stdout=asyncio.subprocess.PIPE, stderr=asyncio.subprocess.PIPE
        )
        try:
            stdout, stderr = await asyncio.wait_for(proc.communicate(), timeout=timeout)
            return proc.returncode, stdout.decode(), stderr.decode()
        except asyncio.TimeoutError:
            proc.kill()
            return -1, "", "timeout"

    async def check_tape_label(self):
        """步骤1: 检查磁带卷标"""
        print("\n[1] 检查磁带卷标")

        self.tape_ops = TapeOperations()
        await self.tape_ops.initialize()
        self.label_info = await self.tape_ops._read_tape_label()

        if not self.label_info or not self.label_info.get('tape_id'):
            print("✗ 无法读取磁带卷标")
            return False

        label = self.label_info.get('label')
        print(f"✓ 磁带卷标: {label}")

        # 检查月份
        parsed = extract_label_year_month(label)
        if parsed:
            year, month = parsed['year'], parsed['month']
            print(f"  年月: {year}年{month:02d}月")

            current_month = datetime.now().month
            month_diff = abs(month - current_month)
            if month_diff > 6:
                month_diff = 12 - month_diff

            if month_diff > 1:
                error_msg = f"卷标不符（磁带{month:02d}月，当前{current_month:02d}月）"
                print(f"✗ {error_msg}")
                await self.notify_error(error_msg)
                return False
            print(f"✓ 卷标符合要求")

        return True

    async def check_ltfs_format(self):
        """步骤2: 检查LTFS格式和空盘"""
        print("\n[2] 检查LTFS格式")

        self.tape_manager = TapeManager()
        await self.tape_manager.initialize()
        self.tape_handler = TapeHandler(
            tape_manager=self.tape_manager,
            settings=self.settings
        )

        await self.tape_handler.unmount_ltfs()
        await asyncio.sleep(1)

        self.is_ltfs, msg = await self.tape_handler.check_ltfs_format()

        if self.is_ltfs:
            print(f"✓ LTFS格式")
            self.is_empty, count = await self.tape_handler.is_tape_empty()
            print(f"  用户文件: {count}, 空盘: {'是' if self.is_empty else '否'}")
        else:
            print(f"✗ 非LTFS格式: {msg}")

        return True

    async def format_tape(self):
        """步骤3: 格式化磁带（如需要）"""
        print("\n[3] 格式化检查")

        if self.is_ltfs and self.is_empty:
            print("✓ 无需格式化")
            return True

        reason = "非LTFS" if not self.is_ltfs else "磁带不为空"
        print(f"⚠️ 需要格式化: {reason}")

        new_label = await generate_tape_label_and_serial()
        print(f"新卷标: {new_label.label}")
        print(f"新序列号: {new_label.serial_number}")

        # 格式化前准备
        await self.run_cmd(["fusermount", "-u", MOUNT_POINT], timeout=10)
        await asyncio.sleep(1)
        await self.run_cmd(["pkill", "-9", "-f", "ltfs"], timeout=5)
        await asyncio.sleep(1)
        await self.run_cmd(["mt", "-f", "/dev/nst0", "rewind"], timeout=60)

        # === 测试模式：跳过实际格式化 ===
        print("\n[测试模式] 跳过实际格式化操作")
        print(f"  模拟格式化成功: 卷名={new_label.label}, 序列号={new_label.serial_number}")
        print(f"✓ 格式化成功（模拟）")
        await asyncio.sleep(2)
        return True

    async def wait_and_mount(self):
        """步骤4: 等待并挂载"""
        print("\n[4] 等待20秒...")
        for i in range(20):
            await asyncio.sleep(1)
            print(f"  {i+1}/20", end="\r")

        print("\n[5] 检查挂载状态")
        if Path(MOUNT_POINT).is_mount():
            print("✓ 已挂载")
            return True

        # 清空目录
        print("\n[6] 清空挂载点")
        Path(MOUNT_POINT).mkdir(parents=True, exist_ok=True)
        for item in Path(MOUNT_POINT).iterdir():
            (item.rmdir if item.is_dir() else item.unlink)()
        print("✓ 已清空")

        # 挂载
        print("\n[7] 挂载LTFS")
        proc = await asyncio.create_subprocess_exec(
            LTFS_BIN, "-o", f"devname={DEVICE}", MOUNT_POINT,
            stdout=asyncio.subprocess.PIPE, stderr=asyncio.subprocess.PIPE
        )

        print("等待挂载...")
        for i in range(30):
            await asyncio.sleep(1)
            if Path(MOUNT_POINT).is_mount():
                print(f"✓ 挂载成功 ({i+1}秒)")
                break
        else:
            await self.notify_error("30秒未挂载")
            proc.kill()
            return False

        # 验证
        print("\n[8] 验证挂载")
        if Path(MOUNT_POINT).is_mount():
            items = list(Path(MOUNT_POINT).iterdir())
            print(f"✓ 挂载成功，{len(items)} 项")
            for i in items[:5]:
                print(f"  {i.name}")
            return True

        await self.notify_error("挂载失败")
        return False

    async def run(self):
        """运行完整测试"""
        print("=" * 50)
        print("磁带挂载测试")
        print("=" * 50)

        if not await self.check_tape_label():
            return 1

        await self.check_ltfs_format()
        if not await self.format_tape():
            return 1

        result = await self.wait_and_mount()

        print("\n" + "=" * 50)
        print(f"测试完成: {'成功' if result else '失败'}")
        print("=" * 50)
        return 0 if result else 1


async def main():
    test = TapeMountTest()
    sys.exit(await test.run())


if __name__ == "__main__":
    sys.exit(asyncio.run(main()))
