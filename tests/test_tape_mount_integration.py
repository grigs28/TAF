#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
磁带挂载集成测试 - 实际硬件测试
Tape Mount Integration Test - Real Hardware Test

这是一个针对实际磁带设备的集成测试程序。
用于测试磁带挂载、格式化、卸载等完整流程。

使用方法:
    # 测试挂载流程
    python tests/test_tape_mount_integration.py --test mount

    # 测试格式化流程
    python tests/test_tape_mount_integration.py --test format

    # 测试完整流程
    python tests/test_tape_mount_integration.py --test full

    # 运行所有测试
    python tests/test_tape_mount_integration.py --test all

注意: 此测试需要实际的磁带设备连接!
"""

import asyncio
import argparse
import sys
import logging
from pathlib import Path
from datetime import datetime

# 添加项目根目录到Python路径
project_root = Path(__file__).parent.parent
sys.path.insert(0, str(project_root))

from config.settings import get_settings
from backup.tape_handler import TapeHandler
from backup.tape_mounter import TapeMounter
from tape.tape_manager import TapeManager
from tape.tape_operations import TapeOperations
from utils.dingtalk_notifier import DingTalkNotifier

# 配置日志
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(name)s - %(levelname)s - %(message)s'
)
logger = logging.getLogger(__name__)


class TapeMountIntegrationTest:
    """磁带挂载集成测试类"""

    def __init__(self):
        self.settings = get_settings()
        self.tape_manager = None
        self.dingtalk_notifier = None
        self.tape_handler = None
        self.mounter = None

    async def initialize(self):
        """初始化测试环境"""
        logger.info("="*60)
        logger.info("初始化磁带挂载集成测试环境")
        logger.info("="*60)

        # 打印配置信息
        logger.info(f"磁带设备: {self.settings.TAPE_DRIVE_LETTER}")
        logger.info(f"LTFS设备: {getattr(self.settings, 'LTFS_DEVICE_PATH', '自动检测')}")
        logger.info(f"LTFS挂载点: {getattr(self.settings, 'LTFS_MOUNT_POINT', '/mnt/ltfs')}")

        # 初始化磁带管理器
        try:
            self.tape_manager = TapeManager()
            await self.tape_manager.initialize()
            logger.info("✓ 磁带管理器初始化成功")
        except Exception as e:
            logger.error(f"✗ 磁带管理器初始化失败: {e}")
            return False

        # 初始化磁带处理器
        try:
            self.tape_handler = TapeHandler(
                tape_manager=self.tape_manager,
                settings=self.settings
            )
            logger.info("✓ 磁带处理器初始化成功")
        except Exception as e:
            logger.error(f"✗ 磁带处理器初始化失败: {e}")
            return False

        # 初始化挂载器
        try:
            self.mounter = TapeMounter()
            await self.mounter.initialize(
                tape_manager=self.tape_manager,
                dingtalk_notifier=self.dingtalk_notifier
            )
            logger.info("✓ 磁带挂载器初始化成功")
        except Exception as e:
            logger.error(f"✗ 磁带挂载器初始化失败: {e}")
            return False

        logger.info("="*60)
        return True

    async def test_tape_status(self):
        """测试1: 检查磁带状态"""
        logger.info("\n" + "="*60)
        logger.info("测试1: 检查磁带状态")
        logger.info("="*60)

        # 使用 TapeOperations 读取磁带信息
        tape_ops = TapeOperations()
        try:
            await tape_ops.initialize()
        except Exception as e:
            logger.error(f"✗ TapeOperations 初始化失败: {e}")
            return False

        # 检查磁带是否在驱动器中
        in_drive, status = await self.tape_handler.check_tape_in_drive()
        logger.info(f"磁带在驱动器中: {in_drive}")
        logger.info(f"状态: {status}")

        if not in_drive:
            logger.error("✗ 磁带未就绪，请插入磁带")
            return False

        # 读取磁带卷标
        label_info = await tape_ops._read_tape_label()
        if label_info:
            logger.info(f"✓ 磁带卷标: {label_info.get('tape_id', 'Unknown')}")
            logger.info(f"  卷标: {label_info.get('label', 'N/A')}")
            logger.info(f"  序列号: {label_info.get('serial_number', 'N/A')}")
            logger.info(f"  容量: {label_info.get('capacity_bytes', 'N/A')}")
        else:
            logger.warning("✗ 无法读取磁带卷标")

        logger.info("="*60)
        return True

    async def test_mount_check(self):
        """测试2: 检查LTFS格式和挂载状态"""
        logger.info("\n" + "="*60)
        logger.info("测试2: 检查LTFS格式和挂载状态")
        logger.info("="*60)

        # 先卸载（如果已挂载）
        await self.tape_handler.unmount_ltfs()
        await asyncio.sleep(2)

        # 检查LTFS格式
        is_ltfs, msg = await self.tape_handler.check_ltfs_format()

        if is_ltfs:
            logger.info(f"✓ 磁带是LTFS格式: {msg}")
            logger.info(f"✓ 挂载点: {self.tape_handler._ltfs_mount_point}")

            # 列出磁带内容
            contents = await self.tape_handler.list_tape_contents()
            if contents:
                logger.info(f"磁带内容 ({len(contents)} 项):")
                for item in contents[:10]:
                    item_type = "目录" if item['is_dir'] else "文件"
                    logger.info(f"  - {item['name']} ({item_type})")
            else:
                logger.info("磁带为空")

            # 检查是否为空（忽略系统文件）
            is_empty, user_file_count = await self.tape_handler.is_tape_empty()
            logger.info(f"用户文件数: {user_file_count}")
            logger.info(f"磁带为空: {is_empty}")
        else:
            logger.warning(f"✗ 磁带不是LTFS格式或挂载失败: {msg}")

        logger.info("="*60)
        return is_ltfs

    async def test_format_tape(self, volume_name: str = "TEST_TAPE", serial: str = None):
        """测试3: 格式化磁带"""
        logger.info("\n" + "="*60)
        logger.info("测试3: 格式化磁带为LTFS格式")
        logger.info("="*60)
        logger.info(f"卷名: {volume_name}")
        logger.info(f"序列号: {serial or '自动生成'}")
        logger.warning("⚠️  警告: 此操作将擦除磁带上所有数据!")

        # 确认操作
        # response = input("确认格式化? (yes/no): ")
        # if response.lower() != 'yes':
        #     logger.info("已取消格式化")
        #     return False

        # 先卸载
        await self.tape_handler.unmount_ltfs()
        await asyncio.sleep(2)

        # 重置设备
        logger.info("重置磁带设备...")
        await self.tape_handler.reset_tape_device()

        # 格式化
        logger.info("开始格式化...")
        success, msg = await self.tape_handler.format_as_ltfs(
            volume_name=volume_name,
            serial=serial
        )

        if success:
            logger.info(f"✓ 格式化成功: {msg}")

            # 等待格式化完成
            await asyncio.sleep(3)

            # 尝试挂载验证
            logger.info("验证格式化结果...")
            is_ltfs, verify_msg = await self.tape_handler.check_ltfs_format()
            if is_ltfs:
                logger.info(f"✓ 格式化验证成功: {verify_msg}")
                logger.info("="*60)
                return True
            else:
                logger.error(f"✗ 格式化验证失败: {verify_msg}")
        else:
            logger.error(f"✗ 格式化失败: {msg}")

        logger.info("="*60)
        return False

    async def test_mount_flow(self):
        """测试4: 完整挂载流程"""
        logger.info("\n" + "="*60)
        logger.info("测试4: 完整挂载流程 (mount_ltfs)")
        logger.info("="*60)

        success, msg = await self.tape_handler.mount_ltfs()

        if success:
            logger.info(f"✓ 挂载成功: {msg}")
            logger.info(f"挂载点: {self.tape_handler._ltfs_mount_point}")
            logger.info(f"已挂载: {self.tape_handler._ltfs_mounted}")
        else:
            logger.error(f"✗ 挂载失败: {msg}")

        logger.info("="*60)
        return success

    async def test_mounter_quick_mount(self):
        """测试5: 使用TapeMounter快速挂载"""
        logger.info("\n" + "="*60)
        logger.info("测试5: 使用TapeMounter快速挂载")
        logger.info("="*60)

        success, msg = await TapeMounter.quick_mount(
            tape_manager=self.tape_manager,
            dingtalk_notifier=self.dingtalk_notifier
        )

        if success:
            logger.info(f"✓ 快速挂载成功: {msg}")
        else:
            logger.error(f"✗ 快速挂载失败: {msg}")

        logger.info("="*60)
        return success

    async def test_unmount(self):
        """测试6: 卸载磁带"""
        logger.info("\n" + "="*60)
        logger.info("测试6: 卸载磁带")
        logger.info("="*60)

        # 方式1: 使用TapeHandler卸载
        success = await self.tape_handler.unmount_ltfs()
        if success:
            logger.info("✓ 使用TapeHandler卸载成功")
        else:
            logger.warning("✗ 使用TapeHandler卸载失败")

        await asyncio.sleep(2)

        # 方式2: 使用TapeMounter卸载
        success = await self.mounter.unmount_tape()
        if success:
            logger.info("✓ 使用TapeMounter卸载成功")
        else:
            logger.warning("✗ 使用TapeMounter卸载失败")

        logger.info("="*60)
        return True

    async def test_write_read_file(self):
        """测试7: 写入和读取测试文件"""
        logger.info("\n" + "="*60)
        logger.info("测试7: 写入和读取测试文件")
        logger.info("="*60)

        # 确保已挂载
        if not self.tape_handler._ltfs_mounted:
            logger.info("先挂载磁带...")
            success, _ = await self.tape_handler.mount_ltfs()
            if not success:
                logger.error("✗ 挂载失败，无法进行写入测试")
                return False

        mount_point = self.tape_handler._ltfs_mount_point
        if not mount_point:
            logger.error("✗ 挂载点无效")
            return False

        # 创建测试文件
        test_file = mount_point / f"test_{datetime.now().strftime('%Y%m%d_%H%M%S')}.txt"
        test_content = f"Tape Mount Integration Test\nTime: {datetime.now()}\n"

        try:
            logger.info(f"写入测试文件: {test_file}")
            test_file.write_text(test_content)
            logger.info("✓ 文件写入成功")

            # 读取验证
            read_content = test_file.read_text()
            if read_content == test_content:
                logger.info("✓ 文件读取验证成功")
                logger.info(f"内容: {read_content.strip()}")

                # 删除测试文件
                test_file.unlink()
                logger.info("✓ 测试文件已删除")
            else:
                logger.error("✗ 文件内容不匹配")
                return False

        except Exception as e:
            logger.error(f"✗ 文件操作失败: {e}")
            return False

        logger.info("="*60)
        return True

    async def run_all_tests(self):
        """运行所有测试"""
        logger.info("\n" + "="*60)
        logger.info("开始运行所有测试")
        logger.info("="*60)

        results = {}

        # 测试1: 检查磁带状态
        results['tape_status'] = await self.test_tape_status()
        if not results['tape_status']:
            logger.error("磁带未就绪，停止后续测试")
            return results

        # 测试2: 检查LTFS格式
        results['mount_check'] = await self.test_mount_check()

        # 测试4: 完整挂载流程
        results['mount_flow'] = await self.test_mount_flow()

        # 测试5: 快速挂载（先卸载再挂载）
        if results['mount_flow']:
            await self.test_unmount()
            await asyncio.sleep(2)
            results['quick_mount'] = await self.test_mounter_quick_mount()
        else:
            results['quick_mount'] = False

        # 测试7: 写入读取文件
        if results.get('quick_mount') or results.get('mount_flow'):
            results['write_read'] = await self.test_write_read_file()
        else:
            results['write_read'] = False
            logger.warning("跳过写入测试（磁带未挂载）")

        # 测试6: 卸载
        results['unmount'] = await self.test_unmount()

        # 打印结果摘要
        logger.info("\n" + "="*60)
        logger.info("测试结果摘要")
        logger.info("="*60)
        for test_name, result in results.items():
            status = "✓ 通过" if result else "✗ 失败"
            logger.info(f"{test_name}: {status}")
        logger.info("="*60)

        return results


async def main():
    """主函数"""
    parser = argparse.ArgumentParser(description='磁带挂载集成测试')
    parser.add_argument('--test', '-t',
                       choices=['mount', 'format', 'full', 'status', 'all', 'unmount', 'write'],
                       default='all',
                       help='测试类型')
    parser.add_argument('--volume-name', '-n',
                       default='TEST_TAPE',
                       help='格式化时的卷名')
    parser.add_argument('--serial', '-s',
                       default=None,
                       help='格式化时的序列号')

    args = parser.parse_args()

    # 创建测试实例
    test = TapeMountIntegrationTest()

    # 初始化
    if not await test.initialize():
        logger.error("初始化失败，退出测试")
        return 1

    # 根据参数运行测试
    if args.test == 'status':
        result = await test.test_tape_status()
    elif args.test == 'mount':
        result = await test.test_mount_flow()
    elif args.test == 'format':
        result = await test.test_format_tape(args.volume_name, args.serial)
    elif args.test == 'full':
        result = await test.test_format_tape(args.volume_name, args.serial)
        if result:
            await asyncio.sleep(3)
            result = await test.test_mount_flow()
    elif args.test == 'unmount':
        result = await test.test_unmount()
    elif args.test == 'write':
        result = await test.test_write_read_file()
    else:  # all
        results = await test.run_all_tests()
        result = all(results.values())

    return 0 if result else 1


if __name__ == "__main__":
    exit_code = asyncio.run(main())
    sys.exit(exit_code)
