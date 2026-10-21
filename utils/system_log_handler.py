#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
系统日志处理器：把重要 INFO / 全部 WARNING 及以上写入 system_logs

入库策略（2026-10-21）：
  · ERROR / CRITICAL：所有模块，一律入库
  · WARNING：所有模块，排除高频噪音
  · INFO：仅“重要事件”（关键词命中），避免海量业务日志刷库

后台线程（FinalDirMonitor、executor）没有运行中的事件循环，必须依赖
主循环引用（set_main_loop）提交写入，否则这些线程的警告/错误会被丢弃。
"""

import logging
import asyncio
import traceback
import threading
import time
from typing import Optional

from models.system_log import LogLevel, LogCategory
from utils.log_utils import log_system


class SystemLogHandler(logging.Handler):
    """系统日志处理器 - 将重要日志自动写入系统日志表"""

    MIN_LEVEL = logging.INFO
    FULL_DB_LEVEL = logging.WARNING   # 该级别及以上：所有模块入库

    # INFO 级别只有命中这些关键词才入库（重要事件）
    IMPORTANT_INFO_KEYWORDS = (
        '任务开始', '任务完成', '任务失败', '任务取消',
        '备份任务', '恢复任务', '执行完成', '执行失败',
        '扫描完成', '压缩完成', '写入磁带', '复制成功',
        '挂载', '卸载', '格式化', '磁带',
        '配置已更新', '配置已重新加载', '服务启动', '系统初始化完成',
        '数据库连接', '连接池',
    )

    # 高频噪音：WARNING/INFO 均不入库
    NOISE_PATTERNS = (
        '异步启动压缩任务',
        '压缩任务已完成',
        '等待文件句柄完全释放',
        '预取文件组',
        '压缩文件存在，原始大小',
    )

    # 主事件循环（由 main() 注入）：后台线程没有 running loop，
    # 必须把协程提交到这个循环，否则日志直接丢失
    _main_loop = None
    # 节流：同一级别+消息前缀在窗口内只写一次，防日志风暴
    _throttle = {}
    _throttle_lock = threading.Lock()
    THROTTLE_WINDOW = 2.0

    @classmethod
    def set_main_loop(cls, loop):
        """注入主事件循环（应用启动时调用）"""
        cls._main_loop = loop

    def __init__(self):
        super().__init__()
        self.setLevel(self.MIN_LEVEL)

    def _should_log(self, record: logging.LogRecord, message: str) -> bool:
        if record.levelno < self.MIN_LEVEL:
            return False
        if any(p in message for p in self.NOISE_PATTERNS):
            return False
        if record.levelno >= self.FULL_DB_LEVEL:
            return True
        # INFO：仅重要事件
        return any(k in message for k in self.IMPORTANT_INFO_KEYWORDS)

    def _throttled(self, record: logging.LogRecord, message: str) -> bool:
        key = (record.levelno, message[:80])
        now = time.time()
        with self._throttle_lock:
            last = self._throttle.get(key)
            if last is not None and now - last < self.THROTTLE_WINDOW:
                return True
            if len(self._throttle) > 2000:
                self._throttle.clear()
            self._throttle[key] = now
            return False

    @staticmethod
    def _category_of(module_name: str, message: str) -> LogCategory:
        low = (module_name + ' ' + message).lower()
        if 'tape' in low or 'ltfs' in low or '磁带' in message:
            return LogCategory.TAPE
        if 'opengauss' in low or 'database' in low or '数据库' in message:
            return LogCategory.DATABASE
        if 'recovery' in low or '恢复' in message:
            return LogCategory.RECOVERY
        if module_name.startswith('web.api'):
            return LogCategory.API
        if 'scheduler' in low or '调度' in message:
            return LogCategory.SYSTEM
        if module_name.startswith('backup'):
            return LogCategory.BACKUP
        return LogCategory.SYSTEM

    def emit(self, record: logging.LogRecord):
        """处理日志记录"""
        try:
            message = record.getMessage()
            if not self._should_log(record, message):
                return
            if self._throttled(record, message):
                return

            level_map = {
                logging.INFO: LogLevel.INFO,
                logging.WARNING: LogLevel.WARNING,
                logging.ERROR: LogLevel.ERROR,
                logging.CRITICAL: LogLevel.CRITICAL,
            }
            log_level = level_map.get(record.levelno, LogLevel.INFO)

            module_name = record.name
            category = self._category_of(module_name, message)

            module = record.module if hasattr(record, 'module') else module_name
            function = record.funcName if hasattr(record, 'funcName') else None
            file_path = record.pathname if hasattr(record, 'pathname') else None
            line_number = record.lineno if hasattr(record, 'lineno') else None

            exception_type = None
            stack_trace = None
            if record.exc_info:
                exception_type = (record.exc_info[0].__name__
                                  if record.exc_info[0] else None)
                stack_trace = ''.join(traceback.format_exception(*record.exc_info))

            task_id = getattr(record, 'task_id', None)

            details = {
                'logger_name': module_name,
                'level_name': record.levelname,
                'module': module,
                'function': function,
                'file_path': file_path,
                'line_number': line_number,
            }
            if exception_type:
                details['exception_type'] = exception_type

            # 主循环优先（后台线程没有 running loop）
            loop = SystemLogHandler._main_loop
            if loop is None or not loop.is_running():
                try:
                    loop = asyncio.get_running_loop()
                except RuntimeError:
                    try:
                        loop = asyncio.get_event_loop()
                        if not loop.is_running():
                            loop = None
                    except RuntimeError:
                        loop = None

            if loop is not None and loop.is_running():
                try:
                    asyncio.run_coroutine_threadsafe(
                        log_system(
                            level=log_level,
                            category=category,
                            message=message,
                            module=module,
                            function=function,
                            file_path=file_path,
                            line_number=line_number,
                            task_id=task_id,
                            details=details,
                            exception_type=exception_type,
                            stack_trace=stack_trace,
                        ),
                        loop,
                    )
                except RuntimeError as loop_error:
                    # 事件循环已关闭（系统关闭中）：静默
                    if ('closed' not in str(loop_error).lower()
                            and 'different loop' not in str(loop_error).lower()):
                        import sys
                        print(f"SystemLogHandler: 提交系统日志失败: {loop_error}", file=sys.stderr)
            # 无可用循环：丢弃（日志不应影响主流程）

        except Exception as e:
            # 处理日志记录时的异常（避免影响主程序）
            import sys
            print(f"SystemLogHandler: 处理日志记录时发生异常: {str(e)}", file=sys.stderr)
            self.handleError(record)
