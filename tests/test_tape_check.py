#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
磁带检查测试 - 使用主程序逻辑
Tape Check Test - Using Main Program Logic
"""

import asyncio
import sys
from pathlib import Path

# 添加项目根目录到Python路径
project_root = Path(__file__).parent.parent
sys.path.insert(0, str(project_root))

from config.settings import get_settings
from backup.tape_handler import TapeHandler
from tape.tape_manager import TapeManager
from tape.tape_operations import TapeOperations
from backup.utils import extract_label_year_month
from backup.tape_label_generator import generate_tape_label_and_serial
from utils.dingtalk_notifier import DingTalkNotifier


class TapeCheckTest:
    """磁带检查测试类 - 使用主程序逻辑"""

    def __init__(self):
        self.settings = get_settings()
        self.tape_handler = None
        self.tape_manager = None
        self.dingtalk_notifier = DingTalkNotifier()

    async def send_error_notification(self, error_msg: str):
        """发送错误通知"""
        try:
            await self.dingtalk_notifier.send_backup_notification(
                "磁带检查",
                "failed",
                {'error': error_msg}
            )
            print("✓ 钉钉通知已发送")
        except Exception as e:
            print(f"✗ 钉钉通知发送失败: {e}")

    async def run(self):
        """运行磁带检查流程（完全使用主程序逻辑）"""
        print("=" * 50)
        print("磁带检查测试 - 使用主程序逻辑")
        print("=" * 50)

        # 初始化 tape_handler
        self.tape_handler = TapeHandler(
            tape_manager=None,
            settings=self.settings,
            dingtalk_notifier=self.dingtalk_notifier
        )

        # ========== 磁带检查流程（完全采用主程序逻辑）==========
        print("\n========== 开始磁带检查 ==========")

        # 1. 检查磁带卷标
        print("\n[1] 检查磁带卷标")

        tape_ops = TapeOperations()
        await tape_ops.initialize()
        label_info = await tape_ops._read_tape_label()

        # 无法读取卷标 → 进入格式化流程
        if not label_info or not label_info.get('tape_id'):
            print("⚠️ 无法读取磁带卷标，将进入格式化流程")
            label = "Unknown"
        else:
            label = label_info.get('label')
            print(f"✓ 磁带卷标: {label}")

            # 检查月份
            parsed = extract_label_year_month(label)
            if parsed:
                from datetime import datetime
                year, month = parsed['year'], parsed['month']
                print(f"  年月: {year}年{month:02d}月")

                current_month = datetime.now().month
                month_diff = abs(month - current_month)
                if month_diff > 6:
                    month_diff = 12 - month_diff

                if month_diff > 1:
                    error_msg = f"卷标不符（磁带{month:02d}月，当前{current_month:02d}月）"
                    print(f"✗ {error_msg}")
                    await self.send_error_notification(error_msg)
                    return 1
                print(f"✓ 卷标符合要求")

        # 2. 检查LTFS格式和空盘（仅当卷标可读时）
        is_ltfs = False
        is_empty = False

        if label != "Unknown":
            print("\n[2] 检查LTFS格式和空盘")

            # 先卸载
            await self.tape_handler.unmount_ltfs()
            await asyncio.sleep(1)

            # 获取挂载点
            mount_point = self.tape_handler._get_ltfs_mount_point()

            self.tape_manager = TapeManager()
            await self.tape_manager.initialize()
            self.tape_handler.tape_manager = self.tape_manager

            is_ltfs, msg = await self.tape_handler.check_ltfs_format()

            if is_ltfs:
                print("✓ LTFS格式")
                is_empty, count = await self.tape_handler.is_tape_empty()
                print(f"  用户文件: {count}, 空盘: {'是' if is_empty else '否'}")
            else:
                print(f"✗ 非LTFS格式: {msg}")

        # 3. 格式化检查
        print("\n[3] 格式化检查")
        need_format = not (label != "Unknown" and is_ltfs and is_empty)
        if need_format:
            if label == "Unknown":
                reason = "无法读取卷标"
            elif not is_ltfs:
                reason = "非LTFS"
            else:
                reason = "磁带不为空"
            print(f"⚠️ 需要格式化: {reason}")
            print("开始格式化...")

            # 格式化前准备
            new_label = await generate_tape_label_and_serial()
            print(f"新卷标: {new_label.label}")
            print(f"新序列号: {new_label.serial_number}")

            # 格式化
            success, format_msg = await self.tape_handler.format_as_ltfs(
                volume_name=new_label.label,
                serial=new_label.serial_number
            )
            if not success:
                error_msg = f"格式化失败: {format_msg}"
                print(f"✗ {error_msg}")
                await self.send_error_notification(error_msg)
                return 1
            print("✓ 格式化成功")
        else:
            print("✓ 无需格式化")

        # 4. 等待并挂载（使用重试机制）
        print("\n[4] 等待20秒...")
        for i in range(20):
            await asyncio.sleep(1)

        mount_point = self.tape_handler._get_ltfs_mount_point()
        print("\n[5] 检查挂载状态")
        if mount_point.is_mount():
            print("✓ 已挂载")
        else:
            # 使用重试机制挂载
            print("\n[6] 开始挂载（重试3次，间隔30秒）...")
            mount_success, mount_msg = await self.tape_handler.mount_with_retry(backup_task=None)
            if not mount_success:
                error_msg = f"挂载失败: {mount_msg}"
                print(f"✗ {error_msg}")
                await self.send_error_notification(error_msg)
                return 1
            print(f"✓ 挂载成功")

        print("\n========== 磁带检查完成 ==========")
        print("\n" + "=" * 50)
        print("测试完成: 成功")
        print("=" * 50)
        return 0


async def main():
    """主函数"""
    test = TapeCheckTest()
    return await test.run()


if __name__ == "__main__":
    exit_code = asyncio.run(main())
    sys.exit(exit_code)
