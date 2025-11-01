#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
磁带模块测试
Tape Module Tests
"""

import pytest
import asyncio
from unittest.mock import Mock, AsyncMock
from datetime import datetime, timedelta
from pathlib import Path
import sys

# 添加项目根目录到Python路径
project_root = Path(__file__).parent.parent
sys.path.insert(0, str(project_root))

from tape.tape_manager import TapeManager
from tape.tape_cartridge import TapeCartridge, TapeStatus
from tape.scsi_interface import SCSIInterface


class TestTapeCartridge:
    """磁带盒测试类"""

    def test_tape_cartridge_creation(self):
        """测试磁带盒创建"""
        tape = TapeCartridge(
            tape_id="TEST_001",
            label="测试磁带",
            capacity_bytes=1000000000  # 1GB
        )

        assert tape.tape_id == "TEST_001"
        assert tape.label == "测试磁带"
        assert tape.capacity_bytes == 1000000000
        assert tape.used_bytes == 0
        assert tape.status == TapeStatus.NEW
        assert tape.free_bytes == 1000000000

    def test_usage_percent(self):
        """测试使用率计算"""
        tape = TapeCartridge(
            tape_id="TEST_001",
            label="测试磁带",
            capacity_bytes=1000000000,
            used_bytes=500000000  # 500MB
        )

        assert tape.usage_percent == 50.0

    def test_is_full(self):
        """测试是否已满"""
        tape = TapeCartridge(
            tape_id="TEST_001",
            label="测试磁带",
            capacity_bytes=1000000000,
            used_bytes=950000000  # 95%
        )

        assert tape.is_full is True

    def test_is_expired(self):
        """测试是否过期"""
        # 创建过期磁带
        expired_date = datetime.now() - timedelta(days=1)
        tape = TapeCartridge(
            tape_id="TEST_001",
            label="测试磁带",
            expiry_date=expired_date
        )

        assert tape.is_expired is True

    def test_is_available_for_backup(self):
        """测试是否可用于备份"""
        tape = TapeCartridge(
            tape_id="TEST_001",
            label="测试磁带",
            status=TapeStatus.AVAILABLE,
            capacity_bytes=1000000000,
            used_bytes=100000000
        )

        assert tape.is_available_for_backup(500000000) is True

    def test_update_usage(self):
        """测试更新使用量"""
        tape = TapeCartridge(
            tape_id="TEST_001",
            label="测试磁带",
            capacity_bytes=1000000000
        )

        tape.update_usage(100000000)  # 100MB

        assert tape.used_bytes == 100000000
        assert tape.free_bytes == 900000000
        assert tape.write_count == 1

    def test_reset_usage(self):
        """测试重置使用量"""
        tape = TapeCartridge(
            tape_id="TEST_001",
            label="测试磁带",
            capacity_bytes=1000000000,
            used_bytes=500000000
        )

        tape.reset_usage()

        assert tape.used_bytes == 0
        assert tape.free_bytes == 1000000000
        assert tape.status == TapeStatus.AVAILABLE


class TestTapeManager:
    """磁带管理器测试类"""

    @pytest.fixture
    def tape_manager(self):
        """磁带管理器测试夹具"""
        return TapeManager()

    @pytest.fixture
    def mock_scsi_interface(self):
        """SCSI接口模拟"""
        interface = Mock(spec=SCSIInterface)
        interface.initialize = AsyncMock()
        interface.scan_tape_devices = AsyncMock(return_value=[
            {
                'path': '/dev/nst0',
                'type': 'SCSI',
                'vendor': 'QUANTUM',
                'model': 'LTO-8',
                'status': 'online'
            }
        ])
        interface.test_unit_ready = AsyncMock(return_value=True)
        interface.rewind_tape = AsyncMock(return_value=True)
        interface.health_check = AsyncMock(return_value=True)
        return interface

    @pytest.mark.asyncio
    async def test_initialize(self, tape_manager, mock_scsi_interface):
        """测试初始化"""
        tape_manager.scsi_interface = mock_scsi_interface

        await tape_manager.initialize()

        assert tape_manager._initialized is True
        mock_scsi_interface.initialize.assert_called_once()

    @pytest.mark.asyncio
    async def test_get_available_tape(self, tape_manager):
        """测试获取可用磁带"""
        await tape_manager.initialize()

        # 添加测试磁带
        available_tape = TapeCartridge(
            tape_id="AVAILABLE_001",
            label="可用磁带",
            status=TapeStatus.AVAILABLE,
            capacity_bytes=1000000000
        )
        tape_manager.tape_cartridges["AVAILABLE_001"] = available_tape

        result = await tape_manager.get_available_tape()

        assert result is not None
        assert result.tape_id == "AVAILABLE_001"

    @pytest.mark.asyncio
    async def test_load_tape(self, tape_manager, mock_scsi_interface):
        """测试加载磁带"""
        await tape_manager.initialize()
        tape_manager.scsi_interface = mock_scsi_interface

        tape = TapeCartridge(
            tape_id="TEST_001",
            label="测试磁带",
            status=TapeStatus.AVAILABLE
        )
        tape_manager.tape_cartridges["TEST_001"] = tape

        result = await tape_manager.load_tape("TEST_001")

        assert result is True
        assert tape_manager.current_tape is tape
        assert tape.status == TapeStatus.IN_USE

    @pytest.mark.asyncio
    async def test_unload_tape(self, tape_manager):
        """测试卸载磁带"""
        await tape_manager.initialize()

        tape = TapeCartridge(
            tape_id="TEST_001",
            label="测试磁带",
            status=TapeStatus.IN_USE
        )
        tape_manager.current_tape = tape

        result = await tape_manager.unload_tape()

        assert result is True
        assert tape_manager.current_tape is None
        assert tape.status == TapeStatus.AVAILABLE

    @pytest.mark.asyncio
    async def test_get_inventory_status(self, tape_manager):
        """测试获取库存状态"""
        await tape_manager.initialize()

        # 添加测试磁带
        tapes = [
            TapeCartridge(
                tape_id="TAPE_001",
                label="磁带1",
                status=TapeStatus.AVAILABLE,
                capacity_bytes=1000000000
            ),
            TapeCartridge(
                tape_id="TAPE_002",
                label="磁带2",
                status=TapeStatus.IN_USE,
                capacity_bytes=1000000000,
                used_bytes=500000000
            )
        ]

        for tape in tapes:
            tape_manager.tape_cartridges[tape.tape_id] = tape

        status = await tape_manager.get_inventory_status()

        assert status['total_tapes'] == 2
        assert status['available_tapes'] == 1
        assert status['in_use_tapes'] == 1
        assert status['total_capacity_bytes'] == 2000000000
        assert status['used_capacity_bytes'] == 500000000