#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
简单磁带挂载测试
Simple Tape Mount Test

测试内容:
1. 磁带设备检测
2. 磁带卷标读取
3. LTFS挂载
4. 挂载成功检测
5. 内容检查
"""

import asyncio
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

# 配置日志
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(levelname)s - %(message)s'
)
logger = logging.getLogger(__name__)


class SimpleTapeMountTest:
    """简单磁带挂载测试"""

    def __init__(self):
        self.settings = get_settings()
        self.tape_manager = None
        self.tape_handler = None

    async def initialize(self):
        """初始化"""
        logger.info("="*50)
        logger.info("简单磁带挂载测试")
        logger.info("="*50)

        # 打印配置
        logger.info(f"磁带设备: {self.settings.TAPE_DRIVE_LETTER}")
        logger.info(f"LTFS挂载点: {getattr(self.settings, 'LTFS_MOUNT_POINT', '/mnt/ltfs')}")
        logger.info("")

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

        logger.info("")
        return True

    async def check_tape_in_drive(self) -> bool:
        """1. 检查磁带是否在驱动器中"""
        logger.info("1. 检查磁带是否在驱动器中")
        logger.info("-" * 50)

        in_drive, status = await self.tape_handler.check_tape_in_drive()

        if in_drive:
            logger.info(f"✓ 磁带已加载")
            logger.info(f"  状态: {status}")
        else:
            logger.error(f"✗ 磁带未就绪: {status}")
            logger.error("  请插入磁带后重试")
            return False

        logger.info("")
        return True

    async def read_tape_label(self) -> str:
        """2. 读取磁带卷标"""
        logger.info("2. 读取磁带卷标")
        logger.info("-" * 50)

        tape_ops = TapeOperations()
        try:
            await tape_ops.initialize()
        except Exception as e:
            logger.error(f"✗ TapeOperations初始化失败: {e}")
            return None

        label_info = await tape_ops._read_tape_label()

        if label_info and label_info.get('tape_id'):
            tape_id = label_info.get('tape_id')
            label = label_info.get('label', 'N/A')
            serial = label_info.get('serial_number', 'N/A')
            logger.info(f"✓ 磁带卷标: {tape_id}")
            logger.info(f"  标签: {label}")
            logger.info(f"  序列号: {serial}")
            logger.info("")
            return tape_id
        else:
            logger.warning("✗ 无法读取磁带卷标 (可能为空白磁带)")
            logger.info("")
            return None

    async def check_ltfs_format(self) -> bool:
        """3. 检查LTFS格式并挂载"""
        logger.info("3. 检查LTFS格式并挂载")
        logger.info("-" * 50)

        is_ltfs, msg = await self.tape_handler.check_ltfs_format()

        if is_ltfs:
            logger.info(f"✓ LTFS格式检测成功")
            logger.info(f"  消息: {msg}")
            logger.info("")
            return True
        else:
            logger.warning(f"✗ 非LTFS格式或挂载失败")
            logger.warning(f"  消息: {msg}")
            logger.info("")
            return False

    async def verify_mount_success(self) -> bool:
        """4. 验证挂载是否成功"""
        logger.info("4. 验证挂载是否成功")
        logger.info("-" * 50)

        # 检查内部状态
        is_mounted = self.tape_handler._ltfs_mounted
        mount_point = self.tape_handler._ltfs_mount_point

        logger.info(f"  内部挂载状态: {is_mounted}")
        logger.info(f"  挂载点路径: {mount_point}")

        # 检查挂载点目录
        if mount_point and mount_point.exists():
            logger.info(f"  挂载点目录存在: ✓")

            # 检查是否为有效挂载点（使用mount命令或is_mount）
            is_mount = mount_point.is_mount()
            logger.info(f"  系统挂载状态: {'✓' if is_mount else '✗'}")

            if is_mount:
                # 检查挂载点是否可访问
                try:
                    items = list(mount_point.iterdir())
                    logger.info(f"  挂载点可访问: ✓")
                    logger.info(f"  根目录项数: {len(items)}")

                    # 列出前几个项
                    for item in items[:5]:
                        item_type = "目录" if item.is_dir() else "文件"
                        logger.info(f"    - {item.name} ({item_type})")

                except Exception as e:
                    logger.error(f"  挂载点访问失败: {e}")
                    return False

                logger.info("")
                logger.info("✓ 挂载成功验证通过")
                logger.info("")
                return True
            else:
                logger.error("✗ 系统未检测到挂载")
                logger.info("")
                return False
        else:
            logger.error("✗ 挂载点目录不存在")
            logger.info("")
            return False

    async def check_tape_contents(self) -> dict:
        """5. 检查磁带内容"""
        logger.info("5. 检查磁带内容")
        logger.info("-" * 50)

        contents = await self.tape_handler.list_tape_contents()

        if contents:
            logger.info(f"  总项数: {len(contents)}")

            # 分类统计
            dirs = [c for c in contents if c['is_dir']]
            files = [c for c in contents if not c['is_dir']]

            logger.info(f"  目录数: {len(dirs)}")
            logger.info(f"  文件数: {len(files)}")

            # 列出非系统文件
            system_patterns = {'.', '..', '.LTFS', 'lost+found'}
            user_files = [c for c in contents if c['name'] not in system_patterns and not c['name'].startswith('.')]

            if user_files:
                logger.info(f"  用户文件数: {len(user_files)}")
                logger.info("  用户文件列表:")
                for f in user_files[:10]:
                    size_info = f"{f['size']} bytes" if not f['is_dir'] else "目录"
                    logger.info(f"    - {f['name']} ({size_info})")
                if len(user_files) > 10:
                    logger.info(f"    ... 还有 {len(user_files) - 10} 个文件")
            else:
                logger.info("  ✓ 磁带为空（无用户文件）")

            is_empty = len(user_files) == 0
            logger.info("")
            return {'empty': is_empty, 'total': len(contents), 'user_files': len(user_files)}
        else:
            logger.info("  ✓ 磁带为空（无法读取内容）")
            logger.info("")
            return {'empty': True, 'total': 0, 'user_files': 0}

    async def check_tape_empty_status(self) -> bool:
        """5b. 使用is_tape_empty检查"""
        logger.info("5b. 使用is_tape_empty检查（过滤系统文件）")
        logger.info("-" * 50)

        is_empty, user_file_count = await self.tape_handler.is_tape_empty()

        logger.info(f"  用户文件数: {user_file_count}")
        logger.info(f"  磁带为空: {'是' if is_empty else '否'}")
        logger.info("")

        return is_empty

    async def mount_ltfs_flow(self) -> bool:
        """完整挂载流程"""
        logger.info("="*50)
        logger.info("开始LTFS挂载流程")
        logger.info("="*50)
        logger.info("")

        # 步骤1: 检查磁带
        if not await self.check_tape_in_drive():
            return False

        # 步骤2: 读取卷标
        await self.read_tape_label()

        # 步骤3: 先卸载（如果已挂载）
        logger.info("卸载已存在的挂载...")
        await self.tape_handler.unmount_ltfs()
        await asyncio.sleep(2)
        logger.info("")

        # 步骤4: 检查LTFS格式并挂载
        is_ltfs = await self.check_ltfs_format()

        if not is_ltfs:
            logger.warning("磁带不是LTFS格式，需要格式化")
            return False

        # 步骤5: 验证挂载成功
        if not await self.verify_mount_success():
            logger.error("挂载验证失败")
            return False

        # 步骤6: 检查内容
        await self.check_tape_empty_status()

        logger.info("="*50)
        logger.info("✓ 挂载测试完成")
        logger.info("="*50)

        return True

    async def unmount_tape(self):
        """卸载磁带"""
        logger.info("")
        logger.info("="*50)
        logger.info("卸载磁带")
        logger.info("="*50)

        success = await self.tape_handler.unmount_ltfs()

        if success:
            logger.info("✓ 卸载成功")
        else:
            logger.warning("✗ 卸载失败")

        logger.info("="*50)

        return success

    async def run_test(self):
        """运行测试"""
        # 初始化
        if not await self.initialize():
            return False

        # 执行挂载流程
        result = await self.mount_ltfs_flow()

        # 卸载
        # await self.unmount_tape()

        return result


async def main():
    """主函数"""
    test = SimpleTapeMountTest()
    result = await test.run_test()
    return 0 if result else 1


if __name__ == "__main__":
    exit_code = asyncio.run(main())
    sys.exit(exit_code)
