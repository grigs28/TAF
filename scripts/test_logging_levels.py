#!/usr/bin/env python3
"""
测试日志级别设置
验证请求日志是否只在debug模式下输出
"""

import logging
import sys
import os
from pathlib import Path

# 添加项目根目录到路径
sys.path.insert(0, str(Path(__file__).parent.parent))

def test_logging_levels():
    """测试不同日志级别的输出"""
    print("测试日志级别设置")
    print("=" * 50)

    # 导入日志设置
    from utils.logger import setup_logging

    # 设置日志系统
    setup_logging()

    # 获取测试logger
    test_logger = logging.getLogger("test_logging")

    print("当前日志级别配置:")
    print(f"  Root Logger Level: {logging.getLogger().level}")
    print(f"  Test Logger Level: {test_logger.level}")
    print(f"  Console Handler Level: {logging.getLogger().handlers[0].level}")

    print("\n测试不同级别的日志输出:")
    print("-" * 30)

    # 测试不同级别的日志
    test_logger.debug("这是DEBUG级别的日志 - 应该在INFO模式下不显示")
    test_logger.info("这是INFO级别的日志 - 应该总是显示")
    test_logger.warning("这是WARNING级别的日志 - 应该总是显示")
    test_logger.error("这是ERROR级别的日志 - 应该总是显示")

    print(f"\n环境变量 LOG_LEVEL = {os.getenv('LOG_LEVEL', '未设置')}")

def test_web_middleware_logging():
    """测试Web中间件日志"""
    print("\n测试Web中间件日志:")
    print("-" * 30)

    # 导入中间件
    from web.middleware.logging_middleware import LoggingMiddleware
    from unittest.mock import Mock

    # 创建模拟请求和响应
    mock_request = Mock()
    mock_request.method = "GET"
    mock_request.url = Mock()
    mock_request.url.path = "/test"

    mock_response = Mock()
    mock_response.status_code = 200
    mock_response.headers = {}

    # 创建中间件实例
    middleware = LoggingMiddleware(Mock())

    print("模拟Web请求日志:")
    print("注意：下面的日志应该在DEBUG模式下才显示")

    # 测试中间件日志
    middleware.logger.debug(f"请求开始: {mock_request.method} {mock_request.url.path}")
    middleware.logger.debug(
        f"请求完成: {mock_request.method} {mock_request.url.path} "
        f"状态码: {mock_response.status_code} "
        f"处理时间: 0.1234s"
    )

if __name__ == "__main__":
    print("当前工作目录:", os.getcwd())
    print("Python路径:", sys.executable)
    print()

    test_logging_levels()
    test_web_middleware_logging()

    print("\n" + "=" * 50)
    print("测试说明:")
    print("1. 如果只看到INFO、WARNING、ERROR日志，说明配置正确")
    print("2. 如果看到DEBUG日志，说明当前日志级别设置为DEBUG")
    print("3. 要在生产环境中隐藏DEBUG日志，设置 LOG_LEVEL=INFO")
    print("4. 要在开发中显示所有日志，设置 LOG_LEVEL=DEBUG")