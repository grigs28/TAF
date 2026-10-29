#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
测试磁带空盘判断逻辑
Tape Empty Detection Tests

研究LTFS格式化后磁带的内容，确保is_tape_empty方法能正确识别空盘
"""

import pytest
import asyncio
from unittest.mock import Mock, AsyncMock, patch
from pathlib import Path
import sys
import tempfile

# 添加项目根目录到Python路径
project_root = Path(__file__).parent.parent
sys.path.insert(0, str(project_root))

from backup.tape_handler import TapeHandler
from config.settings import get_settings


class TestTapeEmptyDetection:
    """测试磁带空盘判断"""

    @pytest.fixture
    def tape_handler(self):
        """创建TapeHandler实例"""
        handler = TapeHandler(tape_manager=None, settings=get_settings())
        # 模拟已挂载状态
        handler._ltfs_mounted = True
        handler._ltfs_mount_point = Path("/tmp/test_ltfs")
        return handler

    def test_truly_empty_tape(self, tape_handler):
        """测试真正的空磁带"""
        with tempfile.TemporaryDirectory() as tmpdir:
            tape_handler._ltfs_mount_point = Path(tmpdir)

            # 空目录
            is_empty, count = asyncio.run(tape_handler.is_tape_empty())

            assert is_empty is True
            assert count == 0
            print("✓ 真空磁带: 正确识别为空")

    def test_empty_with_dot_files(self, tape_handler):
        """测试只有隐藏文件的磁带"""
        with tempfile.TemporaryDirectory() as tmpdir:
            mount_point = Path(tmpdir)
            tape_handler._ltfs_mount_point = mount_point

            # 创建LTFS系统文件
            (mount_point / ".LTFS").mkdir()
            (mount_point / ".LTFS").touch("metadata")

            is_empty, count = asyncio.run(tape_handler.is_tape_empty())

            assert is_empty is True
            assert count == 0
            print("✓ 只有.LTFS隐藏文件: 正确识别为空")

    def test_ltfs_placeholder_file(self, tape_handler):
        """测试有@placeholder文件的磁带"""
        with tempfile.TemporaryDirectory() as tmpdir:
            mount_point = Path(tmpdir)
            tape_handler._ltfs_mount_point = mount_point

            # LTFS可能创建的占位符文件
            (mount_point / "@placeholder").touch()

            is_empty, count = asyncio.run(tape_handler.is_tape_empty())

            # 修复后：@placeholder应该被排除
            assert is_empty is True
            assert count == 0
            print("✓ @placeholder文件: 正确识别为空（已修复）")

    def test_tape_with_user_data(self, tape_handler):
        """测试有用户数据的磁带"""
        with tempfile.TemporaryDirectory() as tmpdir:
            mount_point = Path(tmpdir)
            tape_handler._ltfs_mount_point = mount_point

            # 创建用户文件
            (mount_point / "backup_001.tar.zst").touch()

            is_empty, count = asyncio.run(tape_handler.is_tape_empty())

            assert is_empty is False
            assert count == 1
            print("✓ 有用户数据: 正确识别为非空")

    def test_tape_with_user_directory(self, tape_handler):
        """测试有用户目录的磁带"""
        with tempfile.TemporaryDirectory() as tmpdir:
            mount_point = Path(tmpdir)
            tape_handler._ltfs_mount_point = mount_point

            # 创建用户目录
            (mount_point / "2025-03-28").mkdir()

            is_empty, count = asyncio.run(tape_handler.is_tape_empty())

            assert is_empty is False
            assert count == 1
            print("✓ 有用户目录: 正确识别为非空")

    def test_ltfs_system_patterns(self, tape_handler):
        """测试各种LTFS系统文件模式"""
        ltfs_system_patterns = [
            ".LTFS",
            "@placeholder",
            "lost+found",
            "$RECYCLE.BIN",
            "System Volume Information",
            ".Trash",
            ".Spotlight-V100",
            ".Trashes",
        ]

        with tempfile.TemporaryDirectory() as tmpdir:
            mount_point = Path(tmpdir)
            tape_handler._ltfs_mount_point = mount_point

            for pattern in ltfs_system_patterns:
                # 清理
                for item in mount_point.iterdir():
                    if item.is_dir():
                        import shutil
                        shutil.rmtree(item)
                    else:
                        item.unlink()

                # 创建文件/目录
                test_path = mount_point / pattern
                if pattern.startswith('.'):
                    test_path.touch()
                else:
                    test_path.mkdir(exist_ok=True)

                is_empty, count = asyncio.run(tape_handler.is_tape_empty())

                print(f"  {pattern:30s}: is_empty={is_empty}, count={count}")

                # 这些都应该被认为是空的（修复后）
                assert is_empty is True, f"{pattern}应该被认为是系统文件"
                assert count == 0


class TestLTFSFormattedTape:
    """测试LTFS格式化后的磁带内容"""

    def test_what_ltfs_creates_after_format(self):
        """
        研究LTFS格式化后创建的文件

        根据LTFS文档，格式化后可能创建：
        1. .LTFS 目录（元数据）
        2. 空的根目录（无用户文件）

        但不应该有@placeholder或其他系统文件
        """
        print("\n=== LTFS格式化后的预期内容 ===")
        print("1. 根目录: /")
        print("2. 元数据: /.LTFS/")
        print("3. 无其他文件或目录")
        print("")
        print("如果检测到其他文件，说明：")
        print("- 可能是之前的测试/备份留下的")
        print("- 或者LTFS版本不同创建了额外的文件")
        print("- 或者挂载了错误的分区")


def print_test_summary():
    """打印测试摘要"""
    print("\n" + "="*60)
    print("磁带空盘判断测试")
    print("="*60)
    print("")
    print("发现的潜在问题:")
    print("")
    print("1. is_tape_empty() 只排除以'.'开头的文件")
    print("   .LTFS 会被排除 ✓")
    print("   但其他系统文件可能不会被排除 ✗")
    print("")
    print("2. LTFS可能创建的非隐藏文件:")
    print("   - @placeholder (某些版本)")
    print("   - lost+found")
    print("   - 其他系统目录")
    print("")
    print("3. 建议修复:")
    print("   扩展排除列表，包含已知LTFS系统文件")
    print("="*60)


if __name__ == "__main__":
    print_test_summary()
    pytest.main([__file__, "-v", "-s"])
