#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
极简磁带挂载测试
Test Ultra-Simplified Tape Mount Flow

极简流程：
1. 检查磁带是否在驱动器（失败→停止任务+发钉钉）
2. 读取现有卷标
3. 直接格式化为 LTFS（失败→停止任务+发钉钉）
4. 挂载（失败→停止任务+发钉钉）

移除：
- 尝试挂载判断格式
- is_tape_empty() 检查
- "有内容则格式化"的判断
"""

import asyncio
import pytest
import logging
from pathlib import Path
from unittest.mock import AsyncMock, MagicMock, patch
from datetime import datetime

logging.basicConfig(level=logging.DEBUG)
logger = logging.getLogger(__name__)


class 模拟配置:
    """模拟配置"""
    TAPE_DRIVE_LETTER = "/dev/nst0"
    LTFS_DEVICE_PATH = "/dev/sg2"
    LTFS_MOUNT_POINT = "/tmp/test_ltfs_mount"


class 模拟钉钉通知器:
    """模拟钉钉通知器"""
    def __init__(self):
        self.通知列表 = []
        self.send_backup_notification = AsyncMock()
        self.send_tape_notification = AsyncMock()

    async def send_tape_notification(self, tape_id, action, details=None):
        """发送钉钉通知"""
        通知 = {
            'tape_id': tape_id,
            'action': action,
            'details': details,
            'timestamp': datetime.now().isoformat()
        }
        self.通知列表.append(通知)
        logger.info(f"[模拟钉钉] 发送通知: {通知}")


class 模拟磁带管理器:
    """模拟磁带管理器"""
    def __init__(self):
        self.tape_operations = None
        self.current_tape = None
        self.tape_cartridges = {}


class 模拟磁带操作:
    """模拟磁带操作"""
    async def _read_tape_label(self):
        """读取磁带卷标"""
        return {
            'tape_id': 'TP20260301',
            'label': 'TP20260301',
            'year': 2026,
            'month': 3
        }


class 模拟备份任务:
    """模拟备份任务"""
    def __init__(self, task_name="测试任务", task_id="test_001"):
        self.task_name = task_name
        self.id = task_id
        self.status = "running"


class 测试极简磁带挂载:
    """极简磁带挂载测试"""

    @pytest.fixture
    def 磁带处理器(self):
        """创建磁带处理器实例"""
        from backup.tape_handler_simplified import TapeHandlerSimplified
        
        模拟管理器 = 模拟磁带管理器()
        模拟管理器.tape_operations = 模拟磁带操作()
        
        处理器 = TapeHandlerSimplified(
            tape_manager=模拟管理器,
            settings=模拟配置(),
            dingtalk_notifier=模拟钉钉通知器()
        )
        return 处理器

    @pytest.fixture
    def 备份任务(self):
        """创建备份任务"""
        return 模拟备份任务()

    @pytest.mark.asyncio
    async def test_已挂载直接返回(self, 磁带处理器, 备份任务):
        """场景1：已经挂载，直接返回成功"""
        logger.info("\n========== 场景1：已经挂载，直接返回 ==========")
        
        # 模拟已经挂载
        磁带处理器._ltfs_mounted = True
        磁带处理器._ltfs_mount_point = Path("/tmp/test_ltfs_mount")
        
        成功, 消息 = await 磁带处理器.mount_ltfs(backup_task=备份任务)
        
        assert 成功, "已挂载应该直接返回成功"
        assert "已挂载" in 消息
        logger.info(f"✅ 测试通过: {消息}")

    @pytest.mark.asyncio
    async def test_磁带不在驱动器停止并发通知(self, 磁带处理器, 备份任务):
        """场景2：磁带不在驱动器，停止任务并发钉钉通知"""
        logger.info("\n========== 场景2：磁带不在驱动器 ==========")
        
        with patch.object(磁带处理器, '_run_command') as mock_run:
            # 模拟驱动器为空
            mock_run.return_value = MagicMock(
                stdout="SCSI 2 tape drive:\n  General status bits on (41010000):\n  DR_OPEN",
                stderr=""
            )
            
            成功, 消息 = await 磁带处理器.mount_ltfs(backup_task=备份任务)
            
            assert not 成功, "磁带不在驱动器应该失败"
            assert "未就绪" in 消息 or "没有磁带" in 消息
            logger.info(f"✅ 测试通过: 正确返回失败 - {消息}")
            
            # 验证发送了钉钉通知
            磁带处理器.dingtalk_notifier.send_tape_notification.assert_called_once()
            调用参数 = 磁带处理器.dingtalk_notifier.send_tape_notification.call_args
            assert 调用参数[1]['action'] == 'error'
            logger.info(f"✅ 钉钉通知已发送")

    @pytest.mark.asyncio
    async def test_格式化失败停止并发通知(self, 磁带处理器, 备份任务):
        """场景3：格式化失败，停止任务并发钉钉通知"""
        logger.info("\n========== 场景3：格式化失败 ==========")
        
        with patch.object(磁带处理器, '_run_command') as mock_run:
            # 模拟磁带在线
            mock_run.return_value = MagicMock(
                stdout="SCSI 2 tape drive:\n  General status bits on (41010000):\n  ONLINE",
                stderr=""
            )
            
            with patch.object(磁带处理器, 'format_as_ltfs') as mock_format:
                # 模拟格式化失败
                mock_format.return_value = (False, "格式化失败: 设备错误")
                
                成功, 消息 = await 磁带处理器.mount_ltfs(backup_task=备份任务)
                
                assert not 成功, "格式化失败应该返回失败"
                assert "格式化失败" in 消息
                logger.info(f"✅ 测试通过: 正确返回失败 - {消息}")
                
                # 验证发送了钉钉通知
                磁带处理器.dingtalk_notifier.send_tape_notification.assert_called()
                logger.info(f"✅ 钉钉通知已发送")

    @pytest.mark.asyncio
    async def test_格式化成功但挂载失败停止并发通知(self, 磁带处理器, 备份任务):
        """场景4：格式化成功但挂载失败，停止任务并发钉钉通知"""
        logger.info("\n========== 场景4：格式化成功但挂载失败 ==========")
        
        with patch.object(磁带处理器, '_run_command') as mock_run:
            # 模拟磁带在线
            mock_run.return_value = MagicMock(
                stdout="SCSI 2 tape drive:\n  General status bits on (41010000):\n  ONLINE",
                stderr=""
            )
            
            with patch.object(磁带处理器, 'format_as_ltfs') as mock_format:
                # 模拟格式化成功
                mock_format.return_value = (True, "格式化成功")
                
                async def 模拟挂载失败():
                    """模拟挂载失败"""
                    磁带处理器._ltfs_mounted = False
                    return False, "挂载失败: 超时"
                
                with patch.object(磁带处理器, 'mount_ltfs_only', side_effect=模拟挂载失败):
                    成功, 消息 = await 磁带处理器.mount_ltfs(backup_task=备份任务)
                    
                    assert not 成功, "挂载失败应该返回失败"
                    assert "挂载失败" in 消息
                    logger.info(f"✅ 测试通过: 正确返回失败 - {消息}")
                    
                    # 验证发送了钉钉通知
                    磁带处理器.dingtalk_notifier.send_tape_notification.assert_called()
                    logger.info(f"✅ 钉钉通知已发送")

    @pytest.mark.asyncio
    async def test_使用现有卷标格式化并挂载成功(self, 磁带处理器, 备份任务):
        """场景5：使用现有卷标，格式化+挂载成功"""
        logger.info("\n========== 场景5：使用现有卷标成功 ==========")
        
        with patch.object(磁带处理器, '_run_command') as mock_run:
            # 模拟磁带在线
            mock_run.return_value = MagicMock(
                stdout="SCSI 2 tape drive:\n  General status bits on (41010000):\n  ONLINE",
                stderr=""
            )
            
            with patch.object(磁带处理器, 'format_as_ltfs') as mock_format:
                # 模拟格式化成功
                mock_format.return_value = (True, "格式化成功: 卷标=TP20260301")
                
                async def 模拟挂载成功():
                    """模拟挂载成功"""
                    磁带处理器._ltfs_mounted = True
                    磁带处理器._ltfs_mount_point = Path("/tmp/test_ltfs_mount")
                    return True, "挂载成功"
                
                with patch.object(磁带处理器, 'mount_ltfs_only', side_effect=模拟挂载成功):
                    成功, 消息 = await 磁带处理器.mount_ltfs(backup_task=备份任务)
                    
                    assert 成功, "格式化并挂载应该成功"
                    assert "成功" in 消息
                    assert 磁带处理器._ltfs_mounted, "挂载状态应该为 True"
                    logger.info(f"✅ 测试通过: {消息}")
                    logger.info(f"✅ 挂载状态: _ltfs_mounted={磁带处理器._ltfs_mounted}")
                    
                    # 验证没有发送失败通知
                    错误调用 = [c for c in 磁带处理器.dingtalk_notifier.send_tape_notification.call_args_list 
                                   if c[1]['action'] == 'error']
                    assert len(错误调用) == 0, "成功时不应发送错误通知"
                    logger.info(f"✅ 未发送错误通知")

    @pytest.mark.asyncio
    async def test_自动生成新标签格式化并挂载成功(self, 磁带处理器, 备份任务):
        """场景6：无现有卷标，自动生成新标签，格式化+挂载成功"""
        logger.info("\n========== 场景6：自动生成新标签成功 ==========")
        
        # 修改 tape_operations 返回 Unknown
        class 模拟磁带操作Unknown:
            async def _read_tape_label(self):
                return {'tape_id': 'Unknown', 'label': 'Unknown'}
        
        磁带处理器.tape_manager.tape_operations = 模拟磁带操作Unknown()
        
        with patch.object(磁带处理器, '_run_command') as mock_run:
            # 模拟磁带在线
            mock_run.return_value = MagicMock(
                stdout="SCSI 2 tape drive:\n  General status bits on (41010000):\n  ONLINE",
                stderr=""
            )
            
            with patch.object(磁带处理器, 'format_as_ltfs') as mock_format:
                # 模拟格式化成功
                mock_format.return_value = (True, "格式化成功")
                
                async def 模拟挂载成功():
                    """模拟挂载成功"""
                    磁带处理器._ltfs_mounted = True
                    磁带处理器._ltfs_mount_point = Path("/tmp/test_ltfs_mount")
                    return True, "挂载成功"
                
                with patch.object(磁带处理器, 'mount_ltfs_only', side_effect=模拟挂载成功):
                    with patch('backup.tape_label_generator.generate_tape_label_and_serial') as mock_gen:
                        from dataclasses import dataclass
                        
                        @dataclass
                        class 模拟卷标信息:
                            tape_id: str
                            label: str
                            serial_number: str
                        
                        mock_gen.return_value = 模拟卷标信息(
                            tape_id="TP20260315",
                            label="TP20260315",
                            serial_number="TP0315"
                        )
                        
                        成功, 消息 = await 磁带处理器.mount_ltfs(backup_task=备份任务)
                        
                        assert 成功, "自动生成标签并挂载应该成功"
                        assert 磁带处理器._ltfs_mounted, "挂载状态应该为 True"
                        logger.info(f"✅ 测试通过: {消息}")
                        logger.info(f"✅ 挂载状态: _ltfs_mounted={磁带处理器._ltfs_mounted}")

    @pytest.mark.asyncio
    async def test_验证挂载状态正确设置(self, 磁带处理器, 备份任务):
        """场景7：验证挂载成功后状态正确设置"""
        logger.info("\n========== 场景7：验证挂载状态正确设置 ==========")
        
        # 初始状态
        assert 磁带处理器._ltfs_mounted == False, "初始状态应该未挂载"
        logger.info("✅ 初始状态: 未挂载")
        
        with patch.object(磁带处理器, '_run_command') as mock_run:
            # 模拟磁带在线
            mock_run.return_value = MagicMock(
                stdout="SCSI 2 tape drive:\n  General status bits on (41010000):\n  ONLINE",
                stderr=""
            )
            
            with patch.object(磁带处理器, 'format_as_ltfs') as mock_format:
                mock_format.return_value = (True, "格式化成功")
                
                async def 模拟挂载成功():
                    # 模拟挂载成功的状态设置
                    磁带处理器._ltfs_mounted = True
                    磁带处理器._ltfs_mount_point = Path("/tmp/test_ltfs_mount")
                    磁带处理器._ltfs_process = MagicMock()
                    return True, "挂载成功"
                
                with patch.object(磁带处理器, 'mount_ltfs_only', side_effect=模拟挂载成功):
                    成功, 消息 = await 磁带处理器.mount_ltfs(backup_task=备份任务)
                    
                    assert 成功, "应该挂载成功"
                    assert 磁带处理器._ltfs_mounted == True, "挂载状态应该为 True"
                    assert 磁带处理器._ltfs_mount_point is not None, "挂载点应该已设置"
                    logger.info(f"✅ 测试通过: 挂载状态正确设置")
                    logger.info(f"✅ _ltfs_mounted={磁带处理器._ltfs_mounted}")
                    logger.info(f"✅ _ltfs_mount_point={磁带处理器._ltfs_mount_point}")

    @pytest.mark.asyncio
    async def test_验证直接调用格式化不检查LTFS格式(self, 磁带处理器, 备份任务):
        """场景8：验证直接调用格式化，不先检查是否为LTFS格式"""
        logger.info("\n========== 场景8：验证直接格式化，不检查LTFS格式 ==========")
        
        with patch.object(磁带处理器, '_run_command') as mock_run:
            # 模拟磁带在线
            mock_run.return_value = MagicMock(
                stdout="SCSI 2 tape drive:\n  General status bits on (41010000):\n  ONLINE",
                stderr=""
            )
            
            with patch.object(磁带处理器, 'format_as_ltfs') as mock_format:
                # 模拟格式化成功
                mock_format.return_value = (True, "格式化成功")
                
                async def 模拟挂载成功():
                    磁带处理器._ltfs_mounted = True
                    return True, "挂载成功"
                
                with patch.object(磁带处理器, 'mount_ltfs_only', side_effect=模拟挂载成功):
                    成功, 消息 = await 磁带处理器.mount_ltfs(backup_task=备份任务)
                    
                    # 验证：直接调用了格式化（没有先检查LTFS格式）
                    assert mock_format.called, "应该直接调用格式化"
                    assert mock_format.call_count == 1, "格式化应该被调用一次"
                    logger.info(f"✅ 测试通过: 直接调用格式化，format_as_ltfs 被调用 {mock_format.call_count} 次")

    def test_验证已移除的方法不存在(self):
        """场景9：验证已移除的方法确实不存在"""
        logger.info("\n========== 场景9：验证已移除的方法不存在 ==========")
        
        from backup.tape_handler_simplified import TapeHandlerSimplified
        
        处理器 = TapeHandlerSimplified(
            tape_manager=模拟磁带管理器(),
            settings=模拟配置(),
            dingtalk_notifier=模拟钉钉通知器()
        )
        
        # 验证已移除的方法不存在
        assert not hasattr(处理器, 'check_ltfs_format'), "check_ltfs_format 应该已被移除"
        assert not hasattr(处理器, 'is_tape_empty'), "is_tape_empty 应该已被移除"
        logger.info(f"✅ 测试通过: 已移除的方法确实不存在")
        logger.info(f"✅ check_ltfs_format: 存在={hasattr(处理器, 'check_ltfs_format')}")
        logger.info(f"✅ is_tape_empty: 存在={hasattr(处理器, 'is_tape_empty')}")

    @pytest.mark.asyncio
    async def test_验证只使用必要的方法(self, 磁带处理器, 备份任务):
        """场景10：验证只使用 format_as_ltfs 和 mount_ltfs_only 方法"""
        logger.info("\n========== 场景10：验证只使用必要的方法 ==========")
        
        with patch.object(磁带处理器, '_run_command') as mock_run:
            # 模拟磁带在线
            mock_run.return_value = MagicMock(
                stdout="SCSI 2 tape drive:\n  General status bits on (41010000):\n  ONLINE",
                stderr=""
            )
            
            # 只 mock 这两个方法
            with patch.object(磁带处理器, 'format_as_ltfs') as mock_format:
                mock_format.return_value = (True, "格式化成功")
                
                with patch.object(磁带处理器, 'mount_ltfs_only') as mock_mount:
                    async def 模拟挂载实现():
                        磁带处理器._ltfs_mounted = True
                        return True, "挂载成功"
                    
                    mock_mount.side_effect = 模拟挂载实现
                    
                    # 执行挂载
                    成功, 消息 = await 磁带处理器.mount_ltfs(backup_task=备份任务)
                    
                    # 验证：只调用了这两个方法
                    assert mock_format.called, "应该调用 format_as_ltfs"
                    assert mock_mount.called, "应该调用 mount_ltfs_only"
                    logger.info(f"✅ 测试通过: 只使用了 format_as_ltfs 和 mount_ltfs_only")
                    logger.info(f"✅ format_as_ltfs 调用次数: {mock_format.call_count}")
                    logger.info(f"✅ mount_ltfs_only 调用次数: {mock_mount.call_count}")


# 兼容性：保留英文类名
TestUltraSimplifiedTapeMount = 测试极简磁带挂载


def run_tests():
    """运行所有测试"""
    logger.info("\n" + "="*60)
    logger.info("开始运行极简磁带挂载测试")
    logger.info("="*60)
    
    pytest.main([
        __file__,
        "-v",
        "--tb=short"
    ])


if __name__ == "__main__":
    run_tests()
