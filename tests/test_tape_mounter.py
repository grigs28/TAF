#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
磁带挂载器测试 - 覆盖挂载失败场景
Tape Mounter Tests - Cover Mount Failure Scenarios

测试场景:
1. 磁带卷标读取为 "Unknown" (无条形码)
2. 磁带卷标读取失败
3. 磁带不在数据库中
4. LTFS 挂载超时
5. 格式化并重新挂载
"""

import pytest
import asyncio
from unittest.mock import Mock, AsyncMock, patch, MagicMock
from datetime import datetime, timedelta
from pathlib import Path
import sys
import subprocess

# 添加项目根目录到Python路径
project_root = Path(__file__).parent.parent
sys.path.insert(0, str(project_root))

from backup.tape_handler import TapeHandler
from backup.tape_mounter import TapeMounter
from tape.tape_manager import TapeManager
from tape.tape_cartridge import TapeCartridge, TapeStatus
from tape.tape_operations import TapeOperations
from config.settings import get_settings


class TestTapeLabelReading:
    """测试磁带卷标读取"""

    @pytest.mark.asyncio
    async def test_read_tape_label_no_barcode(self):
        """测试读取无条形码磁带 (返回 Unknown)"""
        tape_ops = TapeOperations()
        await tape_ops.initialize()

        # Mock LinuxTapeOperator 返回 Unknown
        mock_operator = Mock()
        mock_operator.status = AsyncMock(return_value={
            'online': True,
            'file_number': 0,
            'tape_label': 'Unknown'
        })

        tape_ops.linux_tape_operator = mock_operator

        # 调用 _read_tape_label
        label_info = await tape_ops._read_tape_label()

        # 验证结果
        assert label_info is not None
        assert label_info.get('tape_id') == 'Unknown'
        print(f"✓ 无条形码磁带卷标读取: {label_info}")

    @pytest.mark.asyncio
    async def test_read_tape_label_with_barcode(self):
        """测试读取有正常条形码磁带"""
        tape_ops = TapeOperations()
        await tape_ops.initialize()

        # Mock tar 输出模拟有文件的磁带
        mock_tar_output = "-rw-r--r-- user/group 12345 2024-01-01 12:00 TP20250101"

        with patch('asyncio.to_thread') as mock_thread:
            # 第一次调用 mt status
            # 第二次调用 mt status (再次检查)
            # 第三次调用 tar 读取
            mock_thread.side_effect = [
                ("ONLINE\n", "", 0),  # mt status
                (True,),  # mt rewind
                (mock_tar_output, "", 0),  # tar tvf
                (True,),  # mt rewind (final)
            ]

            label_info = await tape_ops._read_tape_label()

            assert label_info is not None
            assert 'TP20250101' in label_info.get('tape_id', '')
            print(f"✓ 有条形码磁带卷标读取: {label_info}")

    @pytest.mark.asyncio
    async def test_read_tape_label_device_not_found(self):
        """测试磁带设备不存在的情况"""
        tape_ops = TapeOperations()

        with patch('os.path.exists', return_value=False):
            label_info = await tape_ops._read_tape_label()

            assert label_info is None
            print("✓ 设备不存在时返回 None")


class TestTapeDatabaseValidation:
    """测试磁带数据库验证"""

    @pytest.mark.asyncio
    async def test_unknown_tape_not_in_database(self):
        """测试 Unknown 磁带不在数据库中的情况"""
        tape_handler = TapeHandler(tape_manager=None, settings=get_settings())

        # Mock tape_operations 返回 Unknown
        mock_tape_ops = Mock()
        mock_tape_ops._read_tape_label = AsyncMock(return_value={
            'tape_id': 'Unknown',
            'label': 'Unknown',
            'serial_number': ''
        })

        # Mock tape_manager
        mock_manager = Mock()
        mock_manager.tape_operations = mock_tape_ops
        mock_manager.current_tape = None  # 确保没有缓存的当前磁带
        mock_manager.tape_cartridges = {}

        tape_handler.tape_manager = mock_manager

        # Mock 数据库连接 - 磁带不存在
        with patch('utils.scheduler.db_utils.is_opengauss', return_value=True):
            mock_conn = AsyncMock()
            mock_conn.fetchrow = AsyncMock(return_value=None)  # 磁带不存在

            with patch('utils.scheduler.db_utils.get_opengauss_connection') as mock_get_conn:
                mock_get_conn.return_value.__aenter__ = AsyncMock(return_value=mock_conn)
                mock_get_conn.return_value.__aexit__ = AsyncMock()

                # RuntimeError 被捕获，返回 None
                result = await tape_handler.get_current_drive_tape()

                assert result is None
                print(f"✓ Unknown 磁带不在数据库中，返回 None")

    @pytest.mark.asyncio
    async def test_tape_in_database(self):
        """测试磁带在数据库中的情况"""
        tape_handler = TapeHandler(tape_manager=None, settings=get_settings())

        # Mock tape_operations
        mock_tape_ops = Mock()
        mock_tape_ops._read_tape_label = AsyncMock(return_value={
            'tape_id': 'TP20250101',
            'label': 'TP20250101',
            'serial_number': 'SN12345'
        })

        mock_manager = Mock()
        mock_manager.tape_operations = mock_tape_ops
        mock_manager.tape_cartridges = {}
        tape_handler.tape_manager = mock_manager

        # Mock 数据库连接 - 磁带存在
        from datetime import datetime
        mock_row = {
            'tape_id': 'TP20250101',
            'label': 'TP20250101',
            'status': 'available',
            'created_date': datetime.now(),
            'expiry_date': None,
            'capacity_bytes': 18 * 1024**4,
            'used_bytes': 0,
            'serial_number': 'SN12345'
        }

        with patch('utils.scheduler.db_utils.is_opengauss', return_value=True):
            mock_conn = AsyncMock()
            mock_conn.fetchrow = AsyncMock(return_value=mock_row)

            with patch('utils.scheduler.db_utils.get_opengauss_connection') as mock_get_conn:
                mock_get_conn.return_value.__aenter__ = AsyncMock(return_value=mock_conn)
                mock_get_conn.return_value.__aexit__ = AsyncMock()

                # 设置 tape_manager.current_tape 为 None，让方法执行数据库查询
                mock_manager.current_tape = None

                tape = await tape_handler.get_current_drive_tape()

                assert tape is not None
                assert isinstance(tape, TapeCartridge)
                assert tape.tape_id == 'TP20250101'
                print(f"✓ 数据库中的磁带正确加载: {tape.tape_id}")


class TestLTFSMountTimeout:
    """测试 LTFS 挂载超时"""

    @pytest.mark.asyncio
    async def test_mount_timeout_5_seconds(self):
        """测试 5 秒挂载超时"""
        tape_handler = TapeHandler(tape_manager=None, settings=get_settings())

        # Mock subprocess 创建一个永不挂载的进程
        mock_process = Mock()
        mock_process.returncode = None  # 进程一直在运行

        with patch('asyncio.create_subprocess_exec', return_value=mock_process):
            with patch.object(Path, 'is_mount', return_value=False):  # 永远不挂载成功
                # Mock communicate 返回空
                mock_process.communicate = AsyncMock(return_value=(b'', b'Mount timeout'))

                is_ltfs, msg = await tape_handler.check_ltfs_format()

                # 应该返回 False
                assert is_ltfs is False
                assert '超时' in msg or '失败' in msg
                print(f"✓ 挂载超时检测: {msg}")

    @pytest.mark.asyncio
    async def test_mount_success_immediate(self):
        """测试立即挂载成功"""
        tape_handler = TapeHandler(tape_manager=None, settings=get_settings())

        mock_process = Mock()
        mock_process.returncode = None  # 进程运行中

        call_count = [0]

        async def mock_is_mount(self):
            call_count[0] += 1
            # 第一次检查返回 False，第二次返回 True（模拟延迟挂载）
            await asyncio.sleep(0.1)
            return call_count[0] > 1

        with patch('asyncio.create_subprocess_exec', return_value=mock_process):
            with patch('pathlib.Path.is_mount', mock_is_mount):
                is_ltfs, msg = await tape_handler.check_ltfs_format()

                assert is_ltfs is True
                assert '挂载' in msg
                print(f"✓ 延迟挂载成功: {msg}")


class TestTapeMounter:
    """测试 TapeMounter 挂载器"""

    @pytest.fixture
    def mock_tape_manager(self):
        """Mock 磁带管理器"""
        manager = Mock(spec=TapeManager)
        manager.tape_operations = Mock()

        # Mock 磁带在数据库中存在
        tape = TapeCartridge(
            tape_id='TEST_001',
            label='测试磁带',
            status=TapeStatus.AVAILABLE,
            capacity_bytes=18 * 1024**4
        )
        manager.tape_cartridges = {'TEST_001': tape}
        manager.current_tape = tape

        return manager

    @pytest.fixture
    def mock_dingtalk_notifier(self):
        """Mock 钉钉通知器"""
        notifier = Mock()
        notifier.send_backup_notification = AsyncMock()
        return notifier

    @pytest.mark.asyncio
    async def test_quick_mount_success(self, mock_tape_manager, mock_dingtalk_notifier):
        """测试快速挂载成功场景"""
        with patch.object(TapeHandler, 'check_tape_in_drive', return_value=(True, "OK")):
            with patch.object(TapeHandler, 'check_ltfs_format', return_value=(True, "已挂载")):
                with patch.object(TapeHandler, 'is_tape_empty', return_value=(True, 0)):
                    success, msg = await TapeMounter.quick_mount(
                        tape_manager=mock_tape_manager,
                        dingtalk_notifier=mock_dingtalk_notifier
                    )

                    assert success is True
                    print(f"✓ 快速挂载成功: {msg}")

    @pytest.mark.asyncio
    async def test_quick_mount_tape_not_ready(self, mock_tape_manager, mock_dingtalk_notifier):
        """测试磁带未就绪场景"""
        with patch.object(TapeHandler, 'check_tape_in_drive', return_value=(False, "磁带未就绪")):
            success, msg = await TapeMounter.quick_mount(
                tape_manager=mock_tape_manager,
                dingtalk_notifier=mock_dingtalk_notifier
            )

            assert success is False
            assert "未就绪" in msg
            print(f"✓ 磁带未就绪检测: {msg}")

    @pytest.mark.asyncio
    async def test_force_format_mode(self, mock_tape_manager, mock_dingtalk_notifier):
        """测试强制格式化模式"""
        mounter = TapeMounter()
        await mounter.initialize(mock_tape_manager, mock_dingtalk_notifier)

        with patch.object(TapeHandler, 'unmount_ltfs', return_value=True):
            with patch.object(TapeHandler, 'reset_tape_device', return_value=True):
                with patch.object(TapeHandler, 'format_as_ltfs', return_value=(True, "格式化成功")):
                    with patch.object(TapeHandler, 'check_ltfs_format', return_value=(True, "已挂载")):
                        success, msg = await mounter.mount_and_format_tape(force_format=True)

                        assert success is True
                        print(f"✓ 强制格式化模式: {msg}")


class TestMountScenarios:
    """综合挂载场景测试"""

    @pytest.mark.asyncio
    async def test_scenario_no_barcode_not_in_db(self):
        """场景1: 无条形码 + 不在数据库中"""
        tape_handler = TapeHandler(tape_manager=None, settings=get_settings())

        # Mock 磁带读取返回 Unknown
        mock_tape_ops = Mock()
        mock_tape_ops._read_tape_label = AsyncMock(return_value={
            'tape_id': 'Unknown',
            'label': 'Unknown'
        })

        mock_manager = Mock()
        mock_manager.tape_operations = mock_tape_ops
        mock_manager.current_tape = None  # 确保没有缓存的当前磁带
        tape_handler.tape_manager = mock_manager

        # Mock 数据库返回 None（不在数据库中）
        with patch('utils.scheduler.db_utils.is_opengauss', return_value=True):
            mock_conn = AsyncMock()
            mock_conn.fetchrow = AsyncMock(return_value=None)

            with patch('utils.scheduler.db_utils.get_opengauss_connection') as mock_get_conn:
                mock_get_conn.return_value.__aenter__ = AsyncMock(return_value=mock_conn)
                mock_get_conn.return_value.__aexit__ = AsyncMock()

                # RuntimeError 被内部捕获，返回 None
                result = await tape_handler.get_current_drive_tape()

                # 验证返回 None（因为数据库中没有该磁带）
                assert result is None
                print("✓ 场景1: 无条形码 + 不在数据库中 = 返回 None")

    @pytest.mark.asyncio
    async def test_scenario_mount_timeout(self):
        """场景2: LTFS 挂载超时"""
        tape_handler = TapeHandler(tape_manager=None, settings=get_settings())

        # Mock check_tape_in_drive 成功
        with patch.object(tape_handler, 'check_tape_in_drive', return_value=(True, "OK")):
            # Mock check_ltfs_format 超时
            with patch.object(tape_handler, 'check_ltfs_format', return_value=(False, "挂载超时")):
                with patch.object(tape_handler, '_format_and_remount', return_value=(False, "格式化失败")):
                    success, msg = await tape_handler.mount_ltfs()

                    assert success is False
                    print(f"✓ 场景2: 挂载超时处理正确: {msg}")

    @pytest.mark.asyncio
    async def test_scenario_mount_success_format_required(self):
        """场景3: 挂载成功但需要格式化"""
        tape_handler = TapeHandler(tape_manager=None, settings=get_settings())

        # Mock check_tape_in_drive 成功
        with patch.object(tape_handler, 'check_tape_in_drive', return_value=(True, "OK")):
            # Mock check_ltfs_format 成功
            with patch.object(tape_handler, 'check_ltfs_format', return_value=(True, "已挂载")):
                # Mock is_tape_empty 返回有内容
                with patch.object(tape_handler, 'is_tape_empty', return_value=(False, 5)):
                    # Mock format_and_remount 成功
                    with patch.object(tape_handler, '_format_and_remount', return_value=(True, "格式化并重新挂载成功")):
                        success, msg = await tape_handler.mount_ltfs()

                        assert success is True
                        print(f"✓ 场景3: 有内容磁带格式化成功: {msg}")


# ========== 测试运行工具 ==========

def print_test_summary():
    """打印测试摘要"""
    print("\n" + "="*60)
    print("磁带挂载测试场景覆盖:")
    print("="*60)
    print("1. 无条形码磁带 (Unknown)")
    print("2. 有条形码磁带")
    print("3. 磁带设备不存在")
    print("4. 磁带不在数据库中")
    print("5. 磁带在数据库中")
    print("6. LTFS 挂载超时 (5秒)")
    print("7. LTFS 延迟挂载成功")
    print("8. 快速挂载成功")
    print("9. 磁带未就绪")
    print("10. 强制格式化模式")
    print("11. 综合场景: 无条形码 + 不在数据库")
    print("12. 综合场景: 挂载超时")
    print("13. 综合场景: 挂载成功但需要格式化")
    print("="*60)


if __name__ == "__main__":
    print_test_summary()
    pytest.main([__file__, "-v", "-s"])
