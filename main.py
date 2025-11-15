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
import signal
from pathlib import Path
from datetime import datetime

# 添加项目根目录到Python路径
project_root = Path(__file__).parent
sys.path.insert(0, str(project_root))

from config.settings import Settings
from config.database import db_manager
from web.app import create_app
from utils.logger import setup_logging
from utils.scheduler import TaskScheduler
from tape.tape_manager import TapeManager
from backup.backup_engine import BackupEngine
from recovery.recovery_engine import RecoveryEngine
from utils.dingtalk_notifier import DingTalkNotifier
from utils.opengauss.guard import get_opengauss_monitor


def safe_print(message: str):
    try:
        print(message)
    except UnicodeEncodeError:
        print(message.encode('ascii', 'ignore').decode('ascii'))


class TapeBackupSystem:
    """磁带备份系统主类"""

    def __init__(self):
        self.settings = Settings()
        self.db_manager = db_manager  # 使用全局 db_manager
        self.scheduler = TaskScheduler()
        self.tape_manager = TapeManager()
        self.backup_engine = BackupEngine()
        self.recovery_engine = RecoveryEngine()
        self.dingtalk_notifier = DingTalkNotifier()
        self.opengauss_monitor = get_opengauss_monitor()
        self.web_app = None

    async def initialize(self):
        """初始化系统组件"""
        import time
        start_time = time.perf_counter()
        
        try:
            # 设置日志
            setup_logging()
            logger = logging.getLogger(__name__)
            
            print("\n" + "=" * 80)
            safe_print("= 系统启动 = 企业级磁带备份系统启动中...")
            safe_print(f"启动时间: {datetime.now().strftime('%Y-%m-%d %H:%M:%S')}")
            print("=" * 80 + "\n")
            
            logger.info("=" * 60)
            logger.info("企业级磁带备份系统启动中...")
            logger.info(f"启动时间: {datetime.now().strftime('%Y-%m-%d %H:%M:%S')}")
            logger.info("=" * 60)

            # 初始化数据库
            safe_print("[1/7] 初始化数据库...")
            step_start = time.time()
            try:
                # 先检查并创建数据库
                from config.database_init import DatabaseInitializer
                db_init = DatabaseInitializer()
                print("   ├─ 检查数据库是否存在...")
                await db_init.ensure_database_exists()
                
                print("   ├─ 初始化数据库连接池...")
                await self.db_manager.initialize()
                step_time = time.time() - step_start
                safe_print(f"   └─ 数据库初始化完成 (耗时: {step_time:.2f}秒)\n")
                logger.info("数据库连接初始化完成")
            except Exception as db_error:
                step_time = time.time() - step_start
                print(f"   └─ 警告: 数据库初始化失败 (耗时: {step_time:.2f}秒)")
                print(f"      错误: {str(db_error)}\n")
                logger.warning(f"数据库初始化失败，将在Web界面中提示用户: {str(db_error)}")
                logger.info("系统将继续启动，以便用户在Web界面中配置数据库")

            # 初始化磁带管理器
            safe_print("[2/7] 初始化磁带管理器...")
            step_start = time.time()
            try:
                print("   ├─ 初始化SCSI接口...")
                print("   ├─ 扫描磁带设备...")
                await self.tape_manager.initialize()
                step_time = time.time() - step_start
                safe_print(f"   └─ 磁带管理器初始化完成 (耗时: {step_time:.2f}秒)\n")
                logger.info("磁带管理器初始化完成")
            except Exception as tape_error:
                step_time = time.time() - step_start
                print(f"   └─ 警告: 磁带管理器初始化失败 (耗时: {step_time:.2f}秒)")
                print(f"      错误: {str(tape_error)}\n")
                logger.warning(f"磁带管理器初始化失败: {str(tape_error)}")

            # 初始化备份引擎
            safe_print("[3/7] 初始化备份引擎...")
            step_start = time.time()
            try:
                await self.backup_engine.initialize()
                step_time = time.time() - step_start
                safe_print(f"   └─ 备份引擎初始化完成 (耗时: {step_time:.2f}秒)\n")
                logger.info("备份引擎初始化完成")
            except Exception as backup_error:
                step_time = time.time() - step_start
                print(f"   └─ 警告: 备份引擎初始化失败 (耗时: {step_time:.2f}秒)")
                print(f"      错误: {str(backup_error)}\n")
                logger.warning(f"备份引擎初始化失败: {str(backup_error)}")

            # 初始化恢复引擎
            safe_print("[4/7] 初始化恢复引擎...")
            step_start = time.time()
            try:
                await self.recovery_engine.initialize()
                step_time = time.time() - step_start
                safe_print(f"   └─ 恢复引擎初始化完成 (耗时: {step_time:.2f}秒)\n")
                logger.info("恢复引擎初始化完成")
            except Exception as recovery_error:
                step_time = time.time() - step_start
                print(f"   └─ 警告: 恢复引擎初始化失败 (耗时: {step_time:.2f}秒)")
                print(f"      错误: {str(recovery_error)}\n")
                logger.warning(f"恢复引擎初始化失败: {str(recovery_error)}")

            # 初始化通知系统
            safe_print("[5/7] 初始化通知系统...")
            step_start = time.time()
            try:
                await self.dingtalk_notifier.initialize()
                step_time = time.time() - step_start
                safe_print(f"   └─ 通知系统初始化完成 (耗时: {step_time:.2f}秒)\n")
                logger.info("通知系统初始化完成")
                if self.opengauss_monitor:
                    self.opengauss_monitor.attach_notifier(self.dingtalk_notifier)
            except Exception as dingtalk_error:
                step_time = time.time() - step_start
                print(f"   └─ 警告: 通知系统初始化失败 (耗时: {step_time:.2f}秒)")
                print(f"      错误: {str(dingtalk_error)}\n")
                logger.warning(f"通知系统初始化失败: {str(dingtalk_error)}")

            # 启动 openGauss 守护
            try:
                await self.opengauss_monitor.start()
            except Exception as guard_error:
                logger.warning(f"openGauss 守护启动失败: {guard_error}")

            # 绑定依赖（备份引擎需要磁带管理器与通知器）
            try:
                if hasattr(self.backup_engine, "set_dependencies"):
                    self.backup_engine.set_dependencies(self.tape_manager, self.dingtalk_notifier)
                    logger.info("备份引擎依赖已绑定：TapeManager, DingTalkNotifier")
            except Exception as dep_error:
                logger.warning(f"绑定备份引擎依赖失败: {str(dep_error)}")

            # 初始化Web应用
            safe_print("[6/7] 初始化Web应用...")
            step_start = time.time()
            try:
                self.web_app = create_app(self)
                if self.web_app is None:
                    raise ValueError("create_app() 返回了 None")
                step_time = time.time() - step_start
                safe_print(f"   └─ Web应用初始化完成 (耗时: {step_time:.2f}秒)\n")
                logger.info("Web应用初始化完成")
            except Exception as web_error:
                step_time = time.time() - step_start
                safe_print(f"   └─ 警告: Web应用初始化失败 (耗时: {step_time:.2f}秒)")
                safe_print(f"      错误: {str(web_error)}\n")
                logger.error(f"Web应用初始化失败: {str(web_error)}", exc_info=True)
                # 创建一个基本的FastAPI应用作为后备
                from fastapi import FastAPI
                self.web_app = FastAPI(title="企业级磁带备份系统（初始化失败）")
                logger.warning("使用后备FastAPI应用，部分功能可能不可用")

            # 初始化计划任务
            safe_print("[7/7] 初始化计划任务调度器...")
            step_start = time.time()
            try:
                print("   ├─ 从数据库加载计划任务...")
                await self.scheduler.initialize(self)
                step_time = time.time() - step_start
                safe_print(f"   └─ 计划任务调度器初始化完成 (耗时: {step_time:.2f}秒)\n")
                logger.info("计划任务调度器初始化完成")
            except Exception as scheduler_error:
                step_time = time.time() - step_start
                print(f"   └─ 警告: 计划任务调度器初始化失败 (耗时: {step_time:.2f}秒)")
                print(f"      错误: {str(scheduler_error)}\n")
                logger.warning(f"计划任务调度器初始化失败: {str(scheduler_error)}")

            total_time = time.perf_counter() - start_time
            print("=" * 80)
            safe_print(f"系统初始化完成，总耗时: {total_time:.2f}秒")
            print("=" * 80 + "\n")
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
            safe_print(f"\n系统初始化失败: {str(e)}\n")
            logger.info("系统将继续启动，以便用户在Web界面中检查和配置")

    async def start(self, shutdown_event=None):
        """启动系统服务"""
        import time
        try:
            logger = logging.getLogger(__name__)
            logger.info("启动系统服务...")
            
            safe_print("启动系统服务...")
            start_time = time.time()

            # 启动计划任务调度器
            safe_print("   ├─ 启动计划任务调度器...")
            step_start = time.time()
            try:
                await self.scheduler.start()
                step_time = time.time() - step_start
                safe_print(f"   ├─ 计划任务调度器已启动 (耗时: {step_time:.2f}秒)")
            except Exception as scheduler_error:
                step_time = time.time() - step_start
                safe_print(f"   ├─ 警告: 计划任务调度器启动失败 (耗时: {step_time:.2f}秒)")
                safe_print(f"      错误: {str(scheduler_error)}")

            # 启动Web服务
            safe_print("   └─ 启动Web服务器...\n")
            from hypercorn.config import Config
            from hypercorn.asyncio import serve

            config = Config()
            config.bind = [f"0.0.0.0:{self.settings.WEB_PORT}"]
            config.worker_class = "asyncio"

            service_time = time.time() - start_time
            print("=" * 80, flush=True)
            safe_print(f"Web服务已启动 (服务启动耗时: {service_time:.2f}秒)")
            safe_print(f"访问地址: http://localhost:{self.settings.WEB_PORT}")
            safe_print(f"局域网访问: http://192.168.0.28:{self.settings.WEB_PORT}")
            print("=" * 80, flush=True)
            safe_print("提示: 按 Ctrl+C 停止服务\n")
            # 确保输出缓冲区刷新，避免Windows终端等待
            import sys
            sys.stdout.flush()
            sys.stderr.flush()
            
            logger.info(f"Web服务启动在端口 {self.settings.WEB_PORT}")
            logger.info(f"访问地址: http://localhost:{self.settings.WEB_PORT}")
            logger.info(f"Web应用对象类型: {type(self.web_app)}")
            logger.info(f"Web应用对象是否为None: {self.web_app is None}")

            # 确保web_app不为None
            if self.web_app is None:
                raise ValueError("Web应用未初始化，无法启动服务器")

            # 如果提供了关闭事件，创建一个任务来监控它
            if shutdown_event:
                async def shutdown_monitor():
                    await shutdown_event.wait()
                    logger.info("收到关闭信号，准备关闭服务...")
                    await self.shutdown()
                
                asyncio.create_task(shutdown_monitor())

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

            # 释放所有活跃的任务锁
            try:
                from utils.scheduler.task_storage import release_all_active_locks
                await release_all_active_locks()
            except Exception as e:
                logger.warning(f"释放任务锁失败: {str(e)}")

            # 停止计划任务
            if self.scheduler:
                try:
                    await self.scheduler.stop()
                except Exception:
                    pass

            # 关闭openGauss连接池（如果使用openGauss，先关闭连接池）
            try:
                from utils.scheduler.db_utils import is_opengauss, close_opengauss_pool
                if is_opengauss():
                    if self.opengauss_monitor:
                        await self.opengauss_monitor.stop()
                    await close_opengauss_pool()
            except Exception as e:
                logger.warning(f"关闭openGauss连接池失败: {str(e)}")

            # 关闭备份引擎（停止文件移动队列管理器）
            if self.backup_engine:
                try:
                    await self.backup_engine.shutdown()
                except Exception as e:
                    logger.warning(f"关闭备份引擎失败: {str(e)}")

            # 关闭数据库连接（后关闭数据库管理器）
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


def setup_signal_handlers(system):
    """设置信号处理器"""
    shutdown_event = asyncio.Event()
    
    def signal_handler(signum, frame):
        """处理信号"""
        logger = logging.getLogger(__name__)
        logger.info(f"收到信号 {signum}，准备关闭系统...")
        # 设置关闭事件
        shutdown_event.set()
    
    # 注册信号处理器
    signal.signal(signal.SIGINT, signal_handler)
    signal.signal(signal.SIGTERM, signal_handler)
    if hasattr(signal, 'SIGBREAK'):  # Windows
        signal.signal(signal.SIGBREAK, signal_handler)
    
    return shutdown_event


async def main():
    """主函数"""
    system = TapeBackupSystem()
    
    # 设置信号处理器
    shutdown_event = setup_signal_handlers(system)

    try:
        # 初始化系统
        await system.initialize()

        # 启动系统服务（传入关闭事件）
        await system.start(shutdown_event)

    except KeyboardInterrupt:
        logger = logging.getLogger(__name__)
        logger.info("收到中断信号（KeyboardInterrupt），正在关闭系统...")
        await system.shutdown()

    except Exception as e:
        logger = logging.getLogger(__name__)
        logger.error(f"系统运行时发生错误: {str(e)}")
        await system.shutdown()
        sys.exit(1)
    
    finally:
        # 确保在退出前释放所有锁
        try:
            await system.shutdown()
        except Exception:
            pass


if __name__ == "__main__":
    # 检查Python版本
    if sys.version_info < (3, 8):
        safe_print("\n错误: 需要Python 3.8或更高版本")
        safe_print(f"   当前版本: Python {sys.version_info.major}.{sys.version_info.minor}.{sys.version_info.micro}\n")
        sys.exit(1)

    safe_print("\nPython 版本: " + sys.version.split()[0])
    safe_print("工作目录: " + os.getcwd())
    
    # 运行主程序
    asyncio.run(main())