#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
pytest配置文件
pytest Configuration
"""

import pytest
import asyncio
import sys
from pathlib import Path

# 添加项目根目录到Python路径
project_root = Path(__file__).parent.parent
sys.path.insert(0, str(project_root))


@pytest.fixture(scope="session")
def event_loop():
    """创建事件循环"""
    loop = asyncio.new_event_loop()
    yield loop
    loop.close()


@pytest.fixture(autouse=True)
def setup_test_environment():
    """设置测试环境"""
    # 设置测试环境变量
    import os
    os.environ['LOG_LEVEL'] = 'DEBUG'
    os.environ['DEBUG'] = 'true'

    yield

    # 清理测试环境
    pass