#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
openGauss 无人值守守护模块
负责：
- 连接心跳与可用性检测
- 数据库调用超时保护与失败统计
- 钉钉告警节流
"""

from __future__ import annotations

import asyncio
import logging
import time
from dataclasses import dataclass
from typing import Any, Awaitable, Dict, Optional

from config.settings import get_settings

try:
    from utils.dingtalk_notifier import DingTalkNotifier
except Exception:  # noqa: BLE001
    DingTalkNotifier = None  # type: ignore

logger = logging.getLogger(__name__)


@dataclass
class OperationEvent:
    """记录一次数据库操作事件"""

    name: str
    duration: float
    metadata: Optional[Dict[str, Any]] = None
    detail: Optional[str] = None


class OpenGaussMonitor:
    """openGauss运行监控"""

    def __init__(self):
        self.settings = get_settings()
        database_url = getattr(self.settings, "DATABASE_URL", "")
        self.enabled = "opengauss" in database_url.lower()
        self._dsn = database_url.replace("opengauss://", "postgresql://")

        # 配置参数（提供默认值，若不存在则回退）
        self.heartbeat_interval = getattr(self.settings, "OG_HEARTBEAT_INTERVAL", 30)
        self.heartbeat_timeout = getattr(self.settings, "OG_HEARTBEAT_TIMEOUT", 5.0)
        self.operation_timeout = getattr(self.settings, "OG_OPERATION_TIMEOUT", 30.0)
        self.operation_warn_threshold = getattr(self.settings, "OG_OPERATION_WARN_THRESHOLD", 5.0)
        self.operation_failure_threshold = getattr(self.settings, "OG_OPERATION_FAILURE_THRESHOLD", 3)
        self.max_heartbeat_failures = getattr(self.settings, "OG_MAX_HEARTBEAT_FAILURES", 3)
        self.alert_cooldown = getattr(self.settings, "OG_ALERT_COOLDOWN", 600)

        self._heartbeat_task: Optional[asyncio.Task] = None
        self._stop_event: Optional[asyncio.Event] = None
        self._last_alert_ts: float = 0.0
        self._consecutive_heartbeat_failures = 0
        self._consecutive_operation_failures = 0
        self._last_operation: Optional[OperationEvent] = None
        self._notifier: Optional[DingTalkNotifier] = None

    def attach_notifier(self, notifier: Optional[DingTalkNotifier]) -> None:
        """绑定钉钉通知器"""
        if notifier is None:
            return
        self._notifier = notifier

    async def start(self) -> None:
        """启动心跳守护"""
        if not self.enabled:
            return
        if self._heartbeat_task and not self._heartbeat_task.done():
            return
        self._stop_event = asyncio.Event()
        loop = asyncio.get_running_loop()
        self._heartbeat_task = loop.create_task(self._heartbeat_loop(), name="OpenGaussHeartbeat")
        logger.info("openGauss 守护心跳已启动，间隔 %ss", self.heartbeat_interval)

    async def stop(self) -> None:
        """停止心跳守护"""
        if not self.enabled:
            return
        if self._stop_event:
            self._stop_event.set()
        if self._heartbeat_task:
            self._heartbeat_task.cancel()
            try:
                await self._heartbeat_task
            except asyncio.CancelledError:
                pass
            finally:
                self._heartbeat_task = None
        logger.info("openGauss 守护心跳已停止")

    def ensure_running(self) -> None:
        """确保心跳已启动（供数据库调用时自动触发）"""
        if not self.enabled:
            return
        if self._heartbeat_task and not self._heartbeat_task.done():
            return
        # 在异步上下文中启动
        loop = asyncio.get_running_loop()
        if self._stop_event is None:
            self._stop_event = asyncio.Event()
        self._heartbeat_task = loop.create_task(self._heartbeat_loop(), name="OpenGaussHeartbeat")

    async def watch(
        self,
        coro: Awaitable[Any],
        *,
        operation: str,
        timeout: Optional[float] = None,
        metadata: Optional[Dict[str, Any]] = None,
        critical: bool = False,
    ) -> Any:
        """
        对数据库操作增加超时保护与失败统计
        """
        if not self.enabled:
            return await coro

        self.ensure_running()
        timeout = timeout or self.operation_timeout
        started = time.perf_counter()

        try:
            result = await asyncio.wait_for(coro, timeout=timeout)
            duration = time.perf_counter() - started
            await self._record_success(operation, duration, metadata)
            return result
        except asyncio.TimeoutError as exc:
            duration = time.perf_counter() - started
            await self._record_failure(operation, duration, "timeout", metadata, critical=True)
            raise exc
        except Exception as exc:  # noqa: BLE001
            duration = time.perf_counter() - started
            await self._record_failure(operation, duration, str(exc), metadata, critical=critical)
            raise

    async def _record_success(self, operation: str, duration: float, metadata: Optional[Dict[str, Any]]) -> None:
        self._consecutive_operation_failures = 0
        self._last_operation = OperationEvent(name=operation, duration=duration, metadata=metadata)
        if duration > self.operation_warn_threshold:
            logger.warning("openGauss 操作耗时 %.2fs: %s metadata=%s", duration, operation, metadata)

    async def _record_failure(
        self,
        operation: str,
        duration: float,
        detail: str,
        metadata: Optional[Dict[str, Any]],
        *,
        critical: bool = False,
    ) -> None:
        self._consecutive_operation_failures += 1
        self._last_operation = OperationEvent(
            name=operation,
            duration=duration,
            metadata=metadata,
            detail=detail,
        )
        logger.error(
            "openGauss 操作失败: %s (耗时 %.2fs) detail=%s metadata=%s",
            operation,
            duration,
            detail,
            metadata,
            exc_info=False,
        )

        if critical or self._consecutive_operation_failures >= self.operation_failure_threshold:
            await self._maybe_alert(
                title="openGauss 操作持续失败",
                content=(
                    f"操作: {operation}\n"
                    f"失败次数: {self._consecutive_operation_failures}\n"
                    f"耗时: {duration:.2f}s\n"
                    f"详情: {detail}\n"
                    f"元数据: {metadata or {}}\n"
                ),
            )

    async def _heartbeat_loop(self) -> None:
        if not self.enabled:
            return
        logger.debug("openGauss 心跳循环启动")
        while self._stop_event and not self._stop_event.is_set():
            try:
                await self._run_heartbeat_once()
                self._consecutive_heartbeat_failures = 0
            except Exception as exc:  # noqa: BLE001
                self._consecutive_heartbeat_failures += 1
                logger.warning("openGauss 心跳失败 #%s: %s", self._consecutive_heartbeat_failures, exc)
                if self._consecutive_heartbeat_failures >= self.max_heartbeat_failures:
                    await self._maybe_alert(
                        title="openGauss 心跳失败",
                        content=(
                            f"连续失败次数: {self._consecutive_heartbeat_failures}\n"
                            f"错误: {exc}\n"
                        ),
                    )

            try:
                await asyncio.wait_for(self._stop_event.wait(), timeout=self.heartbeat_interval)
            except asyncio.TimeoutError:
                continue

    async def _run_heartbeat_once(self) -> None:
        if not self.enabled:
            return
        import asyncpg

        conn = await asyncpg.connect(self._dsn, timeout=self.operation_timeout)
        try:
            await asyncio.wait_for(conn.execute("SELECT 1"), timeout=self.heartbeat_timeout)
        finally:
            await conn.close()

    async def _maybe_alert(self, title: str, content: str) -> None:
        now_ts = time.monotonic()
        if now_ts - self._last_alert_ts < self.alert_cooldown:
            logger.debug("openGauss 告警已在冷却期，跳过本次: %s", title)
            return
        self._last_alert_ts = now_ts

        logger.error("[openGauss告警] %s\n%s", title, content)
        if not self._notifier:
            return
        try:
            await self._notifier.send_system_notification(title, content)
        except Exception as exc:  # noqa: BLE001
            logger.error("发送openGauss告警失败: %s", exc)


_monitor: Optional[OpenGaussMonitor] = None


def get_opengauss_monitor() -> OpenGaussMonitor:
    """获取单例监控器"""
    global _monitor
    if _monitor is None:
        _monitor = OpenGaussMonitor()
    return _monitor


__all__ = ["get_opengauss_monitor", "OpenGaussMonitor"]

