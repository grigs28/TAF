#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
磁带标签生成器测试
Tape Label Generator Tests
"""

import pytest
import asyncio
from unittest.mock import Mock, AsyncMock, patch
from datetime import datetime
from pathlib import Path
import sys

# 添加项目根目录到Python路径
project_root = Path(__file__).parent.parent
sys.path.insert(0, str(project_root))

from backup.tape_label_generator import (
    normalize_tape_label,
    get_next_tape_sequence,
    generate_tape_label_and_serial,
    register_tape_in_database,
    format_and_register_tape,
    TapeLabelInfo
)


class TestNormalizeTapeLabel:
    """测试卷标规范化"""

    def test_normalize_full_label(self):
        """测试完整卷标格式化"""
        result = normalize_tape_label("TP20250101", 2025, 1)
        assert result == "TP20250101"

    def test_normalize_with_suffix(self):
        """测试带后缀的卷标"""
        result = normalize_tape_label("TP20250101A", 2025, 1)
        assert result == "TP20250101A"

    def test_normalize_tape_prefix(self):
        """测试TAPE前缀"""
        result = normalize_tape_label("TAPE20250101", 2025, 1)
        assert result == "TP20250101"

    def test_normalize_partial_label(self):
        """测试部分卷标"""
        result = normalize_tape_label("202501", 2025, 1)
        assert result == "TP20250101"

    def test_normalize_empty_label(self):
        """测试空卷标"""
        result = normalize_tape_label("", 2025, 3)
        assert result == "TP20250301"

    def test_normalize_none_label(self):
        """测试None卷标"""
        result = normalize_tape_label(None, 2025, 6)
        assert result == "TP20250601"

    def test_normalize_sequence_truncate(self):
        """测试序号截断"""
        result = normalize_tape_label("TP202501999", 2025, 1)
        assert result == "TP20250199"

    def test_normalize_different_year_month(self):
        """测试不同年月"""
        result = normalize_tape_label("20241201", 2024, 12)
        assert result == "TP20241201"


class TestGetNextTapeSequence:
    """测试获取下一个磁带序号"""

    @pytest.mark.asyncio
    async def test_sequence_from_empty_db(self):
        """测试空数据库返回1"""
        with patch('backup.tape_label_generator.is_opengauss', return_value=True):
            mock_conn = AsyncMock()
            mock_conn.fetchrow = AsyncMock(return_value={'count': 0})

            with patch('backup.tape_label_generator.get_opengauss_connection') as mock_get_conn:
                mock_get_conn.return_value.__aenter__ = AsyncMock(return_value=mock_conn)
                mock_get_conn.return_value.__aexit__ = AsyncMock()

                result = await get_next_tape_sequence(2025, 3)
                assert result == 1

    @pytest.mark.asyncio
    async def test_sequence_increment(self):
        """测试序号递增"""
        with patch('backup.tape_label_generator.is_opengauss', return_value=True):
            mock_conn = AsyncMock()
            mock_conn.fetchrow = AsyncMock(return_value={'count': 5})

            with patch('backup.tape_label_generator.get_opengauss_connection') as mock_get_conn:
                mock_get_conn.return_value.__aenter__ = AsyncMock(return_value=mock_conn)
                mock_get_conn.return_value.__aexit__ = AsyncMock()

                result = await get_next_tape_sequence(2025, 3)
                assert result == 6

    @pytest.mark.asyncio
    async def test_sequence_max_99(self):
        """测试序号最大99"""
        with patch('backup.tape_label_generator.is_opengauss', return_value=True):
            mock_conn = AsyncMock()
            mock_conn.fetchrow = AsyncMock(return_value={'count': 150})

            with patch('backup.tape_label_generator.get_opengauss_connection') as mock_get_conn:
                mock_get_conn.return_value.__aenter__ = AsyncMock(return_value=mock_conn)
                mock_get_conn.return_value.__aexit__ = AsyncMock()

                result = await get_next_tape_sequence(2025, 3)
                assert result == 99

    @pytest.mark.asyncio
    async def test_sequence_error_fallback(self):
        """测试错误时返回1"""
        with patch('backup.tape_label_generator.is_opengauss', return_value=False):
            with patch('backup.tape_label_generator.is_sqlite', return_value=False):
                with patch('backup.tape_label_generator.is_redis', return_value=False):
                    # 所有数据库模式都返回False，应该抛出异常
                    result = await get_next_tape_sequence(2025, 3)
                    assert result == 1


class TestGenerateTapeLabelAndSerial:
    """测试生成磁带标签和序列号"""

    @pytest.mark.asyncio
    async def test_generate_with_defaults(self):
        """测试使用默认值生成"""
        with patch('backup.tape_label_generator.get_next_tape_sequence', return_value=1):
            result = await generate_tape_label_and_serial()

            assert result.tape_id.startswith("TP")
            assert len(result.tape_id) == 10  # TP + YYYY + MM + SS
            assert result.serial_number.startswith("TP")
            assert len(result.serial_number) == 6  # TP + MM + SS
            print(f"✓ 默认生成: {result.tape_id}, 序列号: {result.serial_number}")

    @pytest.mark.asyncio
    async def test_generate_with_specific_date(self):
        """测试指定日期生成"""
        with patch('backup.tape_label_generator.get_next_tape_sequence', return_value=5):
            result = await generate_tape_label_and_serial(year=2025, month=3)

            assert result.tape_id == "TP20250305"
            assert result.serial_number == "TP0305"
            assert result.year == 2025
            assert result.month == 3
            assert result.sequence == 5
            print(f"✓ 指定日期生成: {result.tape_id}")

    @pytest.mark.asyncio
    async def test_generate_with_existing_label(self):
        """测试使用现有标签"""
        with patch('backup.tape_label_generator.get_next_tape_sequence', return_value=1):
            result = await generate_tape_label_and_serial(
                existing_label="TP20250199",
                year=2025,
                month=1
            )

            # 序号会被截断到2位
            assert result.tape_id == "TP20250199"
            assert result.serial_number == "TP0199"
            print(f"✓ 使用现有标签: {result.tape_id}")

    @pytest.mark.asyncio
    async def test_generate_unknown_label(self):
        """测试Unknown标签生成新标签"""
        with patch('backup.tape_label_generator.get_next_tape_sequence', return_value=3):
            result = await generate_tape_label_and_serial(
                existing_label="Unknown",
                year=2025,
                month=6
            )

            assert result.tape_id == "TP20250603"
            assert result.serial_number == "TP0603"
            print(f"✓ Unknown标签生成: {result.tape_id}")


class TestRegisterTapeInDatabase:
    """测试数据库注册"""

    @pytest.mark.asyncio
    async def test_register_new_tape_opengauss(self):
        """测试openGauss注册新磁带"""
        with patch('backup.tape_label_generator.is_opengauss', return_value=True):
            mock_conn = AsyncMock()
            mock_conn.fetchrow = AsyncMock(return_value=None)  # 磁带不存在

            with patch('backup.tape_label_generator.get_opengauss_connection') as mock_get_conn:
                mock_get_conn.return_value.__aenter__ = AsyncMock(return_value=mock_conn)
                mock_get_conn.return_value.__aexit__ = AsyncMock()

                success, msg = await register_tape_in_database(
                    tape_id="TP20250101",
                    label="TP20250101",
                    serial_number="TP0101"
                )

                assert success is True
                assert "注册成功" in msg
                print(f"✓ openGauss注册成功: {msg}")

    @pytest.mark.asyncio
    async def test_register_existing_tape_update(self):
        """测试更新已存在磁带"""
        with patch('backup.tape_label_generator.is_opengauss', return_value=True):
            mock_conn = AsyncMock()
            mock_conn.fetchrow = AsyncMock(return_value={'tape_id': 'TP20250101'})  # 磁带已存在

            with patch('backup.tape_label_generator.get_opengauss_connection') as mock_get_conn:
                mock_get_conn.return_value.__aenter__ = AsyncMock(return_value=mock_conn)
                mock_get_conn.return_value.__aexit__ = AsyncMock()

                success, msg = await register_tape_in_database(
                    tape_id="TP20250101",
                    label="TP20250101",
                    serial_number="TP0101"
                )

                assert success is True
                assert "注册成功" in msg or "更新成功" in msg
                print(f"✓ 更新已存在磁带: {msg}")

    @pytest.mark.asyncio
    async def test_register_sqlite_mode(self):
        """测试SQLite模式注册"""
        with patch('backup.tape_label_generator.is_sqlite', return_value=True):
            mock_conn = AsyncMock()
            mock_cursor = AsyncMock()

            async def mock_execute(sql, params=None):
                return mock_cursor

            async def mock_fetchone():
                return None  # 磁带不存在

            mock_cursor.execute = mock_execute
            mock_cursor.fetchone = mock_fetchone
            mock_conn.execute = mock_execute
            mock_conn.commit = AsyncMock()

            with patch('backup.tape_label_generator.get_sqlite_connection', return_value=mock_conn):
                success, msg = await register_tape_in_database(
                    tape_id="TP20250101",
                    label="TP20250101",
                    serial_number="TP0101"
                )

                assert success is True
                print(f"✓ SQLite模式注册: {msg}")


class TestFormatAndRegisterTape:
    """测试格式化并注册磁带"""

    @pytest.mark.asyncio
    async def test_format_unknown_tape(self):
        """测试格式化Unknown磁带"""
        # Mock tape_handler
        mock_tape_handler = Mock()
        mock_tape_handler.format_as_ltfs = AsyncMock(return_value=(True, "格式化成功"))
        mock_tape_manager = Mock()
        mock_tape_manager.tape_cartridges = {}
        mock_tape_handler.tape_manager = mock_tape_manager

        with patch('backup.tape_label_generator.generate_tape_label_and_serial') as mock_gen:
            mock_gen.return_value = TapeLabelInfo(
                tape_id="TP20250101",
                label="TP20250101",
                serial_number="TP0101",
                year=2025,
                month=1,
                sequence=1
            )

            with patch('backup.tape_label_generator.register_tape_in_database') as mock_reg:
                mock_reg.return_value = (True, "注册成功")

                success, msg, label_info = await format_and_register_tape(
                    tape_handler=mock_tape_handler,
                    existing_label="Unknown"
                )

                assert success is True
                assert label_info is not None
                assert label_info.tape_id == "TP20250101"
                print(f"✓ 格式化Unknown磁带: {msg}")

    @pytest.mark.asyncio
    async def test_format_with_existing_label(self):
        """测试使用现有标签格式化"""
        mock_tape_handler = Mock()
        mock_tape_handler.format_as_ltfs = AsyncMock(return_value=(True, "格式化成功"))
        mock_tape_manager = Mock()
        mock_tape_manager.tape_cartridges = {}
        mock_tape_handler.tape_manager = mock_tape_manager

        with patch('backup.tape_label_generator.generate_tape_label_and_serial') as mock_gen:
            mock_gen.return_value = TapeLabelInfo(
                tape_id="TP20250603",
                label="TP20250603",
                serial_number="TP0603",
                year=2025,
                month=6,
                sequence=3
            )

            with patch('backup.tape_label_generator.register_tape_in_database') as mock_reg:
                mock_reg.return_value = (True, "注册成功")

                success, msg, label_info = await format_and_register_tape(
                    tape_handler=mock_tape_handler,
                    existing_label="TP20250603"
                )

                assert success is True
                assert label_info.tape_id == "TP20250603"
                print(f"✓ 使用现有标签格式化: {msg}")

    @pytest.mark.asyncio
    async def test_format_failure(self):
        """测试格式化失败"""
        mock_tape_handler = Mock()
        mock_tape_handler.format_as_ltfs = AsyncMock(return_value=(False, "格式化失败"))
        mock_tape_handler.tape_manager = Mock()

        with patch('backup.tape_label_generator.generate_tape_label_and_serial') as mock_gen:
            mock_gen.return_value = TapeLabelInfo(
                tape_id="TP20250101",
                label="TP20250101",
                serial_number="TP0101",
                year=2025,
                month=1,
                sequence=1
            )

            success, msg, label_info = await format_and_register_tape(
                tape_handler=mock_tape_handler,
                existing_label="Unknown"
            )

            assert success is False
            assert "格式化失败" in msg
            print(f"✓ 格式化失败处理: {msg}")

    @pytest.mark.asyncio
    async def test_format_success_register_fail(self):
        """测试格式化成功但注册失败"""
        mock_tape_handler = Mock()
        mock_tape_handler.format_as_ltfs = AsyncMock(return_value=(True, "格式化成功"))
        mock_tape_manager = Mock()
        mock_tape_manager.tape_cartridges = {}
        mock_tape_handler.tape_manager = mock_tape_manager

        with patch('backup.tape_label_generator.generate_tape_label_and_serial') as mock_gen:
            mock_gen.return_value = TapeLabelInfo(
                tape_id="TP20250101",
                label="TP20250101",
                serial_number="TP0101",
                year=2025,
                month=1,
                sequence=1
            )

            with patch('backup.tape_label_generator.register_tape_in_database') as mock_reg:
                mock_reg.return_value = (False, "注册失败: 连接错误")

                success, msg, label_info = await format_and_register_tape(
                    tape_handler=mock_tape_handler,
                    existing_label="Unknown"
                )

                assert success is False
                assert "注册失败" in msg
                print(f"✓ 注册失败处理: {msg}")


class TestTapeLabelInfo:
    """测试TapeLabelInfo数据类"""

    def test_label_info_creation(self):
        """测试创建标签信息"""
        info = TapeLabelInfo(
            tape_id="TP20250101",
            label="TP20250101",
            serial_number="TP0101",
            year=2025,
            month=1,
            sequence=1
        )

        assert info.tape_id == "TP20250101"
        assert info.label == "TP20250101"
        assert info.serial_number == "TP0101"
        assert info.year == 2025
        assert info.month == 1
        assert info.sequence == 1


# ========== 综合场景测试 ==========

class TestIntegrationScenarios:
    """综合集成场景测试"""

    @pytest.mark.asyncio
    async def test_full_workflow_unknown_tape(self):
        """完整工作流: Unknown磁带 -> 生成标签 -> 格式化 -> 注册"""
        # 模拟完整流程
        with patch('backup.tape_label_generator.is_opengauss', return_value=True):
            with patch('backup.tape_label_generator.get_next_tape_sequence', return_value=7):
                # 1. 生成标签 (使用当前日期)
                label_info = await generate_tape_label_and_serial(
                    existing_label="Unknown"
                )

                # 验证格式正确
                assert label_info.tape_id.startswith("TP")
                assert len(label_info.tape_id) == 10
                assert label_info.serial_number.startswith("TP")
                assert len(label_info.serial_number) == 6

                # 2. 模拟格式化成功
                mock_tape_handler = Mock()
                mock_tape_handler.format_as_ltfs = AsyncMock(return_value=(True, "格式化成功"))
                mock_tape_manager = Mock()
                mock_tape_manager.tape_cartridges = {}
                mock_tape_handler.tape_manager = mock_tape_manager

                # 3. 模拟注册
                mock_conn = AsyncMock()
                mock_conn.fetchrow = AsyncMock(return_value=None)
                mock_conn.execute = AsyncMock()

                with patch('backup.tape_label_generator.get_opengauss_connection') as mock_get_conn:
                    mock_get_conn.return_value.__aenter__ = AsyncMock(return_value=mock_conn)
                    mock_get_conn.return_value.__aexit__ = AsyncMock()

                    success, msg, returned_info = await format_and_register_tape(
                        tape_handler=mock_tape_handler,
                        existing_label="Unknown"
                    )

                    assert success is True
                    assert returned_info.tape_id == label_info.tape_id
                    print(f"✓ 完整工作流: {msg}")

    @pytest.mark.asyncio
    async def test_year_transition(self):
        """测试年份过渡"""
        with patch('backup.tape_label_generator.get_next_tape_sequence', return_value=1):
            # 12月 -> 1月
            dec_result = await generate_tape_label_and_serial(year=2024, month=12)
            assert dec_result.tape_id == "TP20241201"

            jan_result = await generate_tape_label_and_serial(year=2025, month=1)
            assert jan_result.tape_id == "TP20250101"

            print(f"✓ 年份过渡: {dec_result.tape_id} -> {jan_result.tape_id}")

    @pytest.mark.asyncio
    async def test_sequence_overflow(self):
        """测试序号溢出"""
        with patch('backup.tape_label_generator.get_next_tape_sequence', return_value=99):
            result = await generate_tape_label_and_serial(year=2025, month=6)
            assert result.tape_id == "TP20250699"
            assert result.sequence == 99
            print(f"✓ 序号最大值: {result.tape_id}")


def print_test_summary():
    """打印测试摘要"""
    print("\n" + "="*60)
    print("磁带标签生成器测试覆盖:")
    print("="*60)
    print("1. 卷标规范化 (normalize_tape_label)")
    print("2. 获取下一个序号 (get_next_tape_sequence)")
    print("3. 生成标签和序列号 (generate_tape_label_and_serial)")
    print("4. 数据库注册 (register_tape_in_database)")
    print("5. 格式化并注册 (format_and_register_tape)")
    print("6. 综合集成场景")
    print("="*60)


if __name__ == "__main__":
    print_test_summary()
    pytest.main([__file__, "-v", "-s"])
