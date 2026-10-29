#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
配置模块测试
Configuration Module Tests
"""

import pytest
import os
from pathlib import Path

# 添加项目根目录到Python路径
project_root = Path(__file__).parent.parent
sys.path.insert(0, str(project_root))

from config.settings import Settings, get_settings


class TestSettings:
    """配置测试类"""

    def test_default_settings(self):
        """测试默认配置"""
        settings = Settings()

        assert settings.APP_NAME == "企业级磁带备份系统"
        assert settings.APP_VERSION == "1.0.0"
        assert settings.WEB_PORT == 8080
        assert settings.DEFAULT_RETENTION_MONTHS == 6
        assert settings.AUTO_ERASE_EXPIRED is True

    def test_database_config(self):
        """测试数据库配置"""
        settings = Settings()

        assert settings.DB_HOST == "192.168.0.20"
        assert settings.DB_PORT == 5560
        assert settings.DB_USER == "grigs"
        assert settings.DB_PASSWORD == "Slnwg123$"
        assert settings.DB_DATABASE == "taf_codex_1"

    def test_tape_config(self):
        """测试磁带配置"""
        settings = Settings()

        assert settings.TAPE_DRIVE_LETTER == "o"
        assert settings.DEFAULT_BLOCK_SIZE == 262144
        assert settings.MAX_VOLUME_SIZE == 322122547200

    def test_compression_config(self):
        """测试压缩配置"""
        settings = Settings()

        assert settings.COMPRESSION_LEVEL == 9
        assert settings.MAX_FILE_SIZE == 3221225472

    def test_dingtalk_config(self):
        """测试钉钉配置"""
        settings = Settings()

        assert settings.DINGTALK_API_URL == "http://192.168.0.20:5555"
        assert settings.DINGTALK_API_KEY == "sk-dingtalk-api-2024-001"
        assert settings.DINGTALK_DEFAULT_PHONE == "13293513336"

    def test_get_settings_singleton(self):
        """测试配置单例"""
        settings1 = get_settings()
        settings2 = get_settings()

        assert settings1 is settings2

    def test_env_file_loading(self, tmp_path):
        """测试环境文件加载"""
        # 创建临时环境文件
        env_file = tmp_path / ".env"
        env_file.write_text("""
TEST_VALUE=test_value
WEB_PORT=9999
""")

        # 测试加载环境文件
        settings = Settings(_env_file=str(env_file))
        assert settings.WEB_PORT == 9999