#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
企业级磁带备份系统 - 主程序入口
Enterprise Tape Backup System - Main Entry Point

启动命令: conda activate taf ; python main.py
"""

import sys
import os
import logging
import asyncio
from pathlib import Path
from datetime import datetime

# 添加项目根目录到Python路径
project_root = Path(__file__).parent
sys.path.insert(0, str(project_root))

from config.settings import Settings
from config.database import db_manager
from web.app import create_app
from utils.logger import setup_logging
from utils.scheduler import BackupScheduler
from tape.tape_manager import TapeManager
from backup.backup_engine import BackupEngine
from recovery.recovery_engine import RecoveryEngine
from utils.dingtalk_notifier import DingTalkNotifier


class TapeBackupSystem:
    """磁带备份系统主类"""

    def __init__(self):
        self.settings = Settings()
        self.db_manager = db_manager  # 使用全局 db_manager
        self.scheduler = BackupScheduler()
        self.tape_manager = TapeManager()
        self.backup_engine = BackupEngine()
        self.recovery_engine = RecoveryEngine()
        self.dingtalk_notifier = DingTalkNotifier()
        self.web_app = None

    async def initialize(self):
        """初始化系统组件"""
        try:
            # 设置日志
            setup_logging()
            logger = logging.getLogger(__name__)
            logger.info("=" * 60)
            logger.info("企业级磁带备份系统启动中...")
            logger.info(f"启动时间: {datetime.now().strftime('%Y-%m-%d %H:%M:%S')}")
            logger.info("=" * 60)

            # 初始化数据库
            try:
                # 先检查并创建数据库
                from config.database_init import DatabaseInitializer
                db_init = DatabaseInitializer()
                await db_init.ensure_database_exists()
                
                await self.db_manager.initialize()
                logger.info("数据库连接初始化完成")
            except Exception as db_error:
                logger.warning(f"数据库初始化失败，将在Web界面中提示用户: {str(db_error)}")
                logger.info("系统将继续启动，以便用户在Web界面中配置数据库")

            # 初始化磁带管理器
            try:
                await self.tape_manager.initialize()
                logger.info("磁带管理器初始化完成")
            except Exception as tape_error:
                logger.warning(f"磁带管理器初始化失败: {str(tape_error)}")

            # 初始化备份引擎
            try:
                await self.backup_engine.initialize()
                logger.info("备份引擎初始化完成")
            except Exception as backup_error:
                logger.warning(f"备份引擎初始化失败: {str(backup_error)}")

            # 初始化恢复引擎
            try:
                await self.recovery_engine.initialize()
                logger.info("恢复引擎初始化完成")
            except Exception as recovery_error:
                logger.warning(f"恢复引擎初始化失败: {str(recovery_error)}")

            # 初始化通知系统
            try:
                await self.dingtalk_notifier.initialize()
                logger.info("通知系统初始化完成")
            except Exception as dingtalk_error:
                logger.warning(f"通知系统初始化失败: {str(dingtalk_error)}")

            # 初始化Web应用
            self.web_app = create_app(self)
            logger.info("Web应用初始化完成")

            # 初始化计划任务
            try:
                await self.scheduler.initialize(self)
                logger.info("计划任务调度器初始化完成")
            except Exception as scheduler_error:
                logger.warning(f"计划任务调度器初始化失败: {str(scheduler_error)}")

            logger.info("系统初始化完成！（部分组件可能未正确初始化，请在Web界面中检查配置）")

            # 发送启动通知（如果通知系统可用）
            try:
                await self.dingtalk_notifier.send_system_notification(
                    "系统启动",
                    "企业级磁带备份系统已启动（可能存在配置问题，请检查）"
                )
            except:
                pass

        except Exception as e:
            logger = logging.getLogger(__name__)
            logger.error(f"系统初始化过程中发生未预期的错误: {str(e)}")
            logger.info("系统将继续启动，以便用户在Web界面中检查和配置")

    async def start(self):
        """启动系统服务"""
        try:
            logger = logging.getLogger(__name__)
            logger.info("启动系统服务...")

            # 启动计划任务调度器
            try:
                await self.scheduler.start()
            except Exception as scheduler_error:
                logger.warning(f"计划任务调度器启动失败: {str(scheduler_error)}")

            # 启动Web服务
            from hypercorn.config import Config
            from hypercorn.asyncio import serve

            config = Config()
            config.bind = [f"0.0.0.0:{self.settings.WEB_PORT}"]
            config.worker_class = "asyncio"

            logger.info(f"Web服务启动在端口 {self.settings.WEB_PORT}")
            logger.info(f"访问地址: http://localhost:{self.settings.WEB_PORT}")

            await serve(self.web_app, config)

        except Exception as e:
            logger = logging.getLogger(__name__)
            logger.error(f"系统服务启动失败: {str(e)}")
            raise

    async def shutdown(self):
        """关闭系统服务"""
        try:
            logger = logging.getLogger(__name__)
            logger.info("正在关闭系统服务...")

            # 停止计划任务
            if self.scheduler:
                try:
                    await self.scheduler.stop()
                except Exception:
                    pass

            # 关闭数据库连接
            if self.db_manager:
                try:
                    await self.db_manager.close()
                except Exception:
                    pass

            # 发送关闭通知
            if self.dingtalk_notifier:
                try:
                    await self.dingtalk_notifier.send_system_notification(
                        "系统关闭",
                        "企业级磁带备份系统已正常关闭"
                    )
                except Exception:
                    pass

            logger.info("系统服务已关闭")

        except Exception as e:
            logger = logging.getLogger(__name__)
            logger.error(f"系统关闭时发生错误: {str(e)}")


async def main():
    """主函数"""
    system = TapeBackupSystem()

    try:
        # 初始化系统
        await system.initialize()

        # 启动系统服务
        await system.start()

    except KeyboardInterrupt:
        logging.getLogger(__name__).info("收到中断信号，正在关闭系统...")
        await system.shutdown()

    except Exception as e:
        logger = logging.getLogger(__name__)
        logger.error(f"系统运行时发生错误: {str(e)}")
        await system.shutdown()
        sys.exit(1)


if __name__ == "__main__":
    # 检查Python版本
    if sys.version_info < (3, 8):
        print("错误: 需要Python 3.8或更高版本")
        sys.exit(1)

    # 运行主程序
    asyncio.run(main())