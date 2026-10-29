#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
备份模块测试
Backup Module Tests
"""

import pytest
import asyncio
from unittest.mock import Mock, AsyncMock
from pathlib import Path
import sys

# 添加项目根目录到Python路径
project_root = Path(__file__).parent.parent
sys.path.insert(0, str(project_root))

from backup.backup_engine import BackupEngine
from models.backup import BackupTask, BackupTaskType
from tape.tape_manager import TapeManager
from utils.dingtalk_notifier import DingTalkNotifier


class TestBackupEngine:
    """备份引擎测试类"""

    @pytest.fixture
    def backup_engine(self):
        """备份引擎测试夹具"""
        return BackupEngine()

    @pytest.fixture
    def mock_tape_manager(self):
        """磁带管理器模拟"""
        manager = Mock(spec=TapeManager)
        manager.get_available_tape = AsyncMock()
        manager.load_tape = AsyncMock(return_value=True)
        manager.unload_tape = AsyncMock(return_value=True)
        manager.write_data = AsyncMock(return_value=True)
        return manager

    @pytest.fixture
    def mock_dingtalk_notifier(self):
        """钉钉通知器模拟"""
        notifier = Mock(spec=DingTalkNotifier)
        notifier.send_backup_notification = AsyncMock()
        return notifier

    @pytest.mark.asyncio
    async def test_initialize(self, backup_engine):
        """测试初始化"""
        await backup_engine.initialize()
        assert backup_engine._initialized is True

    def test_set_dependencies(self, backup_engine, mock_tape_manager, mock_dingtalk_notifier):
        """测试设置依赖"""
        backup_engine.set_dependencies(mock_tape_manager, mock_dingtalk_notifier)
        assert backup_engine.tape_manager is mock_tape_manager
        assert backup_engine.dingtalk_notifier is mock_dingtalk_notifier

    @pytest.mark.asyncio
    async def test_create_backup_task(self, backup_engine):
        """测试创建备份任务"""
        await backup_engine.initialize()

        task = await backup_engine.create_backup_task(
            task_name="测试备份任务",
            source_paths=["/test/path"],
            task_type=BackupTaskType.FULL
        )

        assert task is not None
        assert task.task_name == "测试备份任务"
        assert task.source_paths == ["/test/path"]
        assert task.task_type == BackupTaskType.FULL

    @pytest.mark.asyncio
    async def test_create_backup_task_invalid_paths(self, backup_engine):
        """测试创建备份任务 - 无效路径"""
        await backup_engine.initialize()

        task = await backup_engine.create_backup_task(
            task_name="测试备份任务",
            source_paths=["/invalid/path"]
        )

        assert task is None

    @pytest.mark.asyncio
    async def test_execute_backup_task(self, backup_engine, mock_tape_manager, mock_dingtalk_notifier):
        """测试执行备份任务"""
        await backup_engine.initialize()
        backup_engine.set_dependencies(mock_tape_manager, mock_dingtalk_notifier)

        # 创建备份任务
        task = await backup_engine.create_backup_task(
            task_name="测试备份任务",
            source_paths=["/test/path"],
            task_type=BackupTaskType.FULL
        )

        # 模拟可用磁带
        from tape.tape_cartridge import TapeCartridge
        mock_tape = Mock(spec=TapeCartridge)
        mock_tape.tape_id = "TEST_TAPE_001"
        mock_tape_manager.get_available_tape.return_value = mock_tape

        # 执行备份任务
        result = await backup_engine.execute_backup_task(task)

        # 验证结果
        assert result is True
        assert task.status == BackupTaskStatus.COMPLETED

        # 验证磁带操作被调用
        mock_tape_manager.get_available_tape.assert_called_once()
        mock_tape_manager.load_tape.assert_called_once_with("TEST_TAPE_001")
        mock_tape_manager.unload_tape.assert_called_once()

        # 验证通知被发送
        mock_dingtalk_notifier.send_backup_notification.assert_called()

    def test_format_bytes(self, backup_engine):
        """测试字节格式化"""
        assert backup_engine._format_bytes(1024) == "1.00 KB"
        assert backup_engine._format_bytes(1048576) == "1.00 MB"
        assert backup_engine._format_bytes(1073741824) == "1.00 GB"
        assert backup_engine._format_bytes(1099511627776) == "1.00 TB"

    @pytest.mark.asyncio
    async def test_cancel_task(self, backup_engine):
        """测试取消任务"""
        await backup_engine.initialize()

        # 创建并设置当前任务
        task = await backup_engine.create_backup_task(
            task_name="测试备份任务",
            source_paths=["/test/path"]
        )
        backup_engine._current_task = task

        # 取消任务
        result = await backup_engine.cancel_task(task.id)

        assert result is True
        assert backup_engine._current_task is None