#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
无人值守系统准备检查脚本
Unattended System Readiness Check Script
"""

import os
import sys
import asyncio
import shutil
import psutil
from pathlib import Path

# 添加项目根目录到路径
project_root = Path(__file__).parent.parent
sys.path.insert(0, str(project_root))

from utils.logger import setup_logging

class UnattendedReadinessChecker:
    """无人值守准备检查器"""

    def __init__(self):
        self.logger = setup_logging()
        self.issues = []
        self.warnings = []

    async def check_environment_variables(self):
        """检查环境变量"""
        self.logger.info("检查环境变量配置...")

        required_vars = {
            'PRODUCTION_ENV': '生产环境标识',
            'UNATTENDED_MODE': '无人值守模式',
            'DATABASE_URL': '数据库连接',
        }

        optional_vars = {
            'DISK_CHECK_MAX_RETRIES': '磁盘检查最大重试次数',
            'MEMORY_DB_MAX_FILES': '内存数据库最大文件数',
            'DB_POOL_SIZE': '数据库连接池大小',
        }

        missing_required = []
        for var, desc in required_vars.items():
            if not os.getenv(var):
                missing_required.append(f"{var} ({desc})")

        if missing_required:
            self.issues.append(f"缺少必需的环境变量: {', '.join(missing_required)}")
        else:
            self.logger.info("✅ 必需环境变量检查通过")

        # 检查可选变量
        missing_optional = []
        for var, desc in optional_vars.items():
            if not os.getenv(var):
                missing_optional.append(f"{var} ({desc})")

        if missing_optional:
            self.warnings.append(f"建议设置可选变量: {', '.join(missing_optional)}")

    async def check_disk_space(self):
        """检查磁盘空间"""
        self.logger.info("检查磁盘空间...")

        try:
            # 检查主要目录的磁盘空间
            paths_to_check = [
                Path.cwd(),  # 当前工作目录
                Path("/tmp" if os.name != "nt" else Path.cwd() / "temp"),  # 临时目录
            ]

            for path in paths_to_check:
                if path.exists():
                    usage = shutil.disk_usage(path)
                    total_gb = usage.total / (1024**3)
                    free_gb = usage.free / (1024**3)
                    used_percent = (usage.used / usage.total) * 100

                    self.logger.info(f"磁盘 {path}: 总容量 {total_gb:.1f}GB, 可用 {free_gb:.1f}GB, 使用率 {used_percent:.1f}%")

                    if free_gb < 10:  # 少于10GB
                        self.issues.append(f"磁盘 {path} 可用空间不足: {free_gb:.1f}GB")
                    elif free_gb < 50:  # 少于50GB
                        self.warnings.append(f"磁盘 {path} 可用空间较少: {free_gb:.1f}GB")

        except Exception as e:
            self.warnings.append(f"无法检查磁盘空间: {e}")

    async def check_memory_usage(self):
        """检查内存使用情况"""
        self.logger.info("检查内存使用情况...")

        try:
            memory = psutil.virtual_memory()
            total_gb = memory.total / (1024**3)
            available_gb = memory.available / (1024**3)
            used_percent = memory.percent

            self.logger.info(f"内存: 总容量 {total_gb:.1f}GB, 可用 {available_gb:.1f}GB, 使用率 {used_percent:.1f}%")

            if available_gb < 1:  # 少于1GB
                self.issues.append(f"可用内存不足: {available_gb:.1f}GB")
            elif available_gb < 2:  # 少于2GB
                self.warnings.append(f"可用内存较少: {available_gb:.1f}GB")

        except Exception as e:
            self.warnings.append(f"无法检查内存使用: {e}")

    async def check_file_permissions(self):
        """检查文件权限"""
        self.logger.info("检查文件权限...")

        critical_paths = [
            "logs/",
            "temp/",
            "data/",
        ]

        for path_str in critical_paths:
            path = Path(path_str)
            try:
                # 尝试创建目录
                path.mkdir(exist_ok=True)

                # 尝试创建测试文件
                test_file = path / "readiness_test.tmp"
                test_file.write_text("test")
                test_file.unlink()

                self.logger.info(f"✅ 目录 {path} 权限正常")
            except Exception as e:
                self.issues.append(f"目录 {path} 权限问题: {e}")

    async def check_dependencies(self):
        """检查依赖项"""
        self.logger.info("检查依赖项...")

        required_modules = [
            'asyncio', 'pathlib', 'sqlite3', 'aiosqlite',
            'sqlalchemy', 'fastapi', 'psutil'
        ]

        for module in required_modules:
            try:
                __import__(module)
                self.logger.debug(f"✅ 模块 {module} 可用")
            except ImportError:
                self.issues.append(f"缺少必需模块: {module}")

    async def check_interactive_inputs(self):
        """检查是否存在交互式输入"""
        self.logger.info("检查交互式输入...")

        interactive_files = [
            "tests/scantap.py",
            "tests/erasetap.py",
            "tests/tape02.py",
        ]

        for file_path in interactive_files:
            if Path(file_path).exists():
                try:
                    content = file_path.read_text(encoding='utf-8')
                    if 'input(' in content:
                        self.warnings.append(f"发现交互式输入文件: {file_path}")
                except Exception as e:
                    self.warnings.append(f"无法检查文件 {file_path}: {e}")

    async def run_all_checks(self):
        """运行所有检查"""
        self.logger.info("🔍 开始无人值守系统准备检查...")
        self.logger.info("=" * 60)

        checks = [
            self.check_environment_variables,
            self.check_disk_space,
            self.check_memory_usage,
            self.check_file_permissions,
            self.check_dependencies,
            self.check_interactive_inputs,
        ]

        for check in checks:
            try:
                await check()
            except Exception as e:
                self.issues.append(f"检查 {check.__name__} 失败: {e}")

        self.logger.info("=" * 60)
        self.logger.info("📊 检查结果汇总:")

        if self.issues:
            self.logger.error(f"❌ 发现 {len(self.issues)} 个问题:")
            for i, issue in enumerate(self.issues, 1):
                self.logger.error(f"  {i}. {issue}")

        if self.warnings:
            self.logger.warning(f"⚠️  发现 {len(self.warnings)} 个警告:")
            for i, warning in enumerate(self.warnings, 1):
                self.logger.warning(f"  {i}. {warning}")

        if not self.issues and not self.warnings:
            self.logger.info("🎉 系统准备就绪，可以进入无人值守模式！")
            return True
        elif not self.issues:
            self.logger.info("✅ 系统基本准备就绪，建议处理警告后运行")
            return True
        else:
            self.logger.error("❌ 系统存在严重问题，请修复后再运行无人值守模式")
            return False


async def main():
    """主函数"""
    checker = UnattendedReadinessChecker()
    ready = await checker.run_all_checks()

    # 设置退出码
    sys.exit(0 if ready else 1)


if __name__ == "__main__":
    asyncio.run(main())