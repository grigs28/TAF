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
from utils.production_guard import ProductionGuard, install_production_guard


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
                
                # 如果是 SQLite 模式，启动 SQLite 操作队列管理器（Redis模式不需要）
                from utils.scheduler.db_utils import is_opengauss, is_redis
                from utils.scheduler.sqlite_utils import is_sqlite
                
                if not is_opengauss() and not is_redis() and is_sqlite():
                    print("   ├─ 启动 SQLite 操作队列管理器...")
                    from backup.sqlite_queue_manager import get_sqlite_queue_manager
                    sqlite_queue_manager = get_sqlite_queue_manager()
                    await sqlite_queue_manager.start()
                    logger.info("SQLite 操作队列管理器已启动（写操作优先于同步）")
                elif is_redis():
                    logger.info("[Redis模式] Redis本身是内存数据库，不需要SQLite操作队列管理器")
                
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
                    logger.warning("收到关闭信号（Ctrl+C），准备强制关闭服务...")
                    
                    # 再次确保解锁（防止信号处理器中的解锁失败）
                    try:
                        from utils.scheduler.task_storage import release_all_active_locks
                        await release_all_active_locks()
                    except Exception as unlock_error:
                        logger.warning(f"关闭时解锁失败: {str(unlock_error)}")
                    
                    # 取消所有正在运行的任务
                    try:
                        loop = asyncio.get_running_loop()
                        for task in asyncio.all_tasks(loop):
                            if task != asyncio.current_task():
                                task.cancel()
                                logger.info(f"已取消任务: {task.get_name()}")
                    except Exception as cancel_error:
                        logger.warning(f"取消任务时出错: {str(cancel_error)}")
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

            # 设置正在关闭标志，防止系统日志记录
            try:
                from utils.log_utils import set_shutting_down
                set_shutting_down()
            except Exception as e:
                logger.warning(f"设置关闭标志失败: {str(e)}")

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
                else:
                    # 如果是 SQLite 模式，停止 SQLite 操作队列管理器（Redis模式不需要）
                    from utils.scheduler.db_utils import is_redis
                    from utils.scheduler.sqlite_utils import is_sqlite
                    
                    if not is_redis() and is_sqlite():
                        from backup.sqlite_queue_manager import get_sqlite_queue_manager
                        sqlite_queue_manager = get_sqlite_queue_manager()
                        await sqlite_queue_manager.stop()
                        logger.info("SQLite 操作队列管理器已停止")
                    elif is_redis():
                        logger.debug("[Redis模式] Redis模式无需停止SQLite操作队列管理器")
            except Exception as e:
                logger.warning(f"关闭数据库连接池失败: {str(e)}")

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
        logger.warning(f"收到信号 {signum}（Ctrl+C），准备强制关闭系统...")
        
        # 立即解锁所有任务锁（在关闭前）
        try:
            loop = asyncio.get_running_loop()
            # 创建一个任务来立即解锁
            async def unlock_immediately():
                try:
                    from utils.scheduler.task_storage import release_all_active_locks
                    logger.info("正在立即释放所有任务锁...")
                    await release_all_active_locks()
                    logger.info("所有任务锁已释放")
                except Exception as unlock_error:
                    logger.warning(f"立即解锁失败: {str(unlock_error)}")
            
            # 在事件循环中调度解锁任务（使用 call_soon_threadsafe 或直接创建任务）
            try:
                # 尝试创建任务（如果事件循环正在运行）
                asyncio.create_task(unlock_immediately())
            except RuntimeError:
                # 如果无法创建任务，使用 call_soon_threadsafe
                loop.call_soon_threadsafe(lambda: asyncio.create_task(unlock_immediately()))
        except RuntimeError:
            # 如果没有运行中的事件循环，尝试直接调用（同步方式）
            try:
                # 创建一个新的事件循环来执行解锁
                new_loop = asyncio.new_event_loop()
                asyncio.set_event_loop(new_loop)
                try:
                    from utils.scheduler.task_storage import release_all_active_locks
                    logger.info("正在立即释放所有任务锁...")
                    new_loop.run_until_complete(release_all_active_locks())
                    logger.info("所有任务锁已释放")
                finally:
                    new_loop.close()
            except Exception as unlock_error:
                logger.warning(f"立即解锁失败: {str(unlock_error)}")
        except Exception as e:
            logger.warning(f"解锁时出错: {str(e)}")
        
        # 设置关闭事件
        shutdown_event.set()
        
        # 在 Windows 上，尝试取消所有正在运行的任务
        try:
            loop = asyncio.get_running_loop()
            # 取消所有正在运行的任务（除了当前任务）
            for task in asyncio.all_tasks(loop):
                if task != asyncio.current_task():
                    task.cancel()
                    logger.info(f"已取消任务: {task.get_name()}")
        except RuntimeError:
            # 如果没有运行中的事件循环，忽略
            pass
        except Exception as e:
            logger.warning(f"取消任务时出错: {str(e)}")
    
    # 注册信号处理器
    signal.signal(signal.SIGINT, signal_handler)
    signal.signal(signal.SIGTERM, signal_handler)
    if hasattr(signal, 'SIGBREAK'):  # Windows
        signal.signal(signal.SIGBREAK, signal_handler)
    
    return shutdown_event


async def main():
    """主函数"""
    # 设置asyncio异常处理器（在事件循环运行后）
    setup_asyncio_exception_handler()

    # 安装生产环境保护器
    install_production_guard()

    # 生产环境检查
    if ProductionGuard.is_production():
        safe_print("🛡️  生产环境保护器已激活 - 交互式输入已被阻止")
    if ProductionGuard.is_unattended_mode():
        safe_print("🤖 无人值守模式已激活")

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


def setup_asyncio_exception_handler():
    """设置asyncio异常处理器，确保Future异常不会阻塞"""
    def exception_handler(loop, context):
        """自定义异常处理器，记录异常但不阻塞"""
        exception = context.get('exception')
        message = context.get('message', '')
        
        # 记录异常
        logger = logging.getLogger(__name__)
        
        # 检查是否是 openGauss UNLISTEN 错误（可以安全忽略）
        if exception:
            error_msg = str(exception)
            error_type = type(exception).__name__
            
            # 检查是否是 FeatureNotSupportedError 或 UNLISTEN 相关错误
            try:
                import asyncpg
                if isinstance(exception, asyncpg.exceptions.FeatureNotSupportedError):
                    if "UNLISTEN" in error_msg or "not yet supported" in error_msg:
                        # openGauss 不支持 UNLISTEN，这是 asyncpg 在释放连接时的正常行为
                        # 可以安全忽略，使用 DEBUG 级别记录
                        logger.debug(f"[asyncio异常] openGauss限制（可忽略）: {message} - {error_msg}")
                        return
            except ImportError:
                pass
            
            # 检查是否是 UNLISTEN 相关错误（即使没有导入 asyncpg）
            if "UNLISTEN" in error_msg and "not yet supported" in error_msg:
                logger.debug(f"[asyncio异常] openGauss限制（可忽略）: {message} - {error_msg}")
                return
        
        # 如果是Future异常，记录但不阻塞（避免需要回车）
        if exception and isinstance(exception, (ConnectionError, OSError)):
            if 'connection_lost' in str(exception).lower() or 'unexpected connection' in str(exception).lower():
                logger.warning(f"[asyncio异常] {message}: {exception} (已自动处理，无需手动干预)")
                return  # 不阻塞，直接返回
        
        # 其他异常正常记录
        logger.error(f"[asyncio异常] {message}", exc_info=exception)
    
    # 获取当前事件循环并设置异常处理器
    try:
        loop = asyncio.get_running_loop()
        loop.set_exception_handler(exception_handler)
    except RuntimeError:
        # 如果没有运行中的事件循环，在事件循环创建后设置
        # 这会在 asyncio.run() 创建事件循环后调用
        pass


if __name__ == "__main__":
    # 检查Python版本
    if sys.version_info < (3, 8):
        safe_print("\n错误: 需要Python 3.8或更高版本")
        safe_print(f"   当前版本: Python {sys.version_info.major}.{sys.version_info.minor}.{sys.version_info.micro}\n")
        sys.exit(1)

    safe_print("\nPython 版本: " + sys.version.split()[0])
    safe_print("工作目录: " + os.getcwd())
    
    # 运行主程序（异常处理器在 main() 函数中设置）
    asyncio.run(main())