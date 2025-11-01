#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
磁带管理器
Tape Manager Module
"""

import os
import platform
import asyncio
import logging
from typing import List, Optional, Dict, Any
from datetime import datetime, timedelta
from pathlib import Path

from config.settings import get_settings
from tape.scsi_interface import SCSIInterface
from tape.tape_cartridge import TapeCartridge, TapeStatus
from tape.tape_operations import TapeOperations

logger = logging.getLogger(__name__)


class TapeManager:
    """磁带管理器"""

    def __init__(self):
        self.settings = get_settings()
        self.scsi_interface = SCSIInterface()
        self.tape_operations = TapeOperations()
        self.tape_cartridges: Dict[str, TapeCartridge] = {}
        self.current_tape: Optional[TapeCartridge] = None
        self._initialized = False
        self._monitoring_task = None

    async def initialize(self):
        """初始化磁带管理器"""
        try:
            # 初始化SCSI接口
            await self.scsi_interface.initialize()
            logger.info("SCSI接口初始化完成")

            # 初始化磁带操作
            await self.tape_operations.initialize(self.scsi_interface)
            logger.info("磁带操作模块初始化完成")

            # 检测磁带设备
            await self._detect_tape_devices()

            # 加载磁带信息
            await self._load_tape_inventory()

            # 启动磁带监控任务
            if self.settings.TAPE_CHECK_INTERVAL > 0:
                self._monitoring_task = asyncio.create_task(self._monitoring_loop())

            self._initialized = True
            logger.info("磁带管理器初始化完成")

        except Exception as e:
            logger.error(f"磁带管理器初始化失败: {str(e)}")
            raise

    async def _detect_tape_devices(self):
        """检测磁带设备"""
        try:
            devices = await self.scsi_interface.scan_tape_devices()
            logger.info(f"检测到 {len(devices)} 个磁带设备")

            for device in devices:
                logger.info(f"磁带设备: {device['path']} - {device['vendor']} {device['model']}")

        except Exception as e:
            logger.error(f"检测磁带设备失败: {str(e)}")

    async def _load_tape_inventory(self):
        """加载磁带库存信息"""
        try:
            # 这里应该从数据库加载磁带信息
            # 暂时创建示例数据
            sample_tapes = [
                TapeCartridge(
                    tape_id="TAPE001",
                    label="备份磁带001",
                    status=TapeStatus.AVAILABLE,
                    capacity_bytes=self.settings.MAX_VOLUME_SIZE,
                    used_bytes=0,
                    created_date=datetime.now() - timedelta(days=30),
                    expiry_date=datetime.now() + timedelta(days=150),
                    location="磁带柜-1-A"
                ),
                TapeCartridge(
                    tape_id="TAPE002",
                    label="备份磁带002",
                    status=TapeStatus.IN_USE,
                    capacity_bytes=self.settings.MAX_VOLUME_SIZE,
                    used_bytes=1073741824,  # 1GB
                    created_date=datetime.now() - timedelta(days=60),
                    expiry_date=datetime.now() + timedelta(days=120),
                    location="磁带柜-1-B"
                )
            ]

            for tape in sample_tapes:
                self.tape_cartridges[tape.tape_id] = tape

            logger.info(f"加载了 {len(self.tape_cartridges)} 个磁带信息")

        except Exception as e:
            logger.error(f"加载磁带库存信息失败: {str(e)}")

    async def get_available_tape(self) -> Optional[TapeCartridge]:
        """获取可用磁带"""
        try:
            # 查找可用磁带
            for tape in self.tape_cartridges.values():
                if tape.status == TapeStatus.AVAILABLE and not tape.is_expired():
                    return tape

            # 如果没有可用磁带，尝试清理过期磁带
            if self.settings.AUTO_ERASE_EXPIRED:
                await self._cleanup_expired_tapes()

                # 再次查找可用磁带
                for tape in self.tape_cartridges.values():
                    if tape.status == TapeStatus.AVAILABLE and not tape.is_expired():
                        return tape

            logger.warning("没有可用的磁带")
            return None

        except Exception as e:
            logger.error(f"获取可用磁带失败: {str(e)}")
            return None

    async def load_tape(self, tape_id: str) -> bool:
        """加载磁带"""
        try:
            if tape_id not in self.tape_cartridges:
                logger.error(f"磁带 {tape_id} 不存在")
                return False

            tape = self.tape_cartridges[tape_id]

            # 检查磁带状态
            if tape.is_expired():
                logger.warning(f"磁带 {tape_id} 已过期，将进行擦除")
                await self.erase_tape(tape_id)

            # 加载磁带
            success = await self.tape_operations.load_tape(tape)
            if success:
                self.current_tape = tape
                tape.status = TapeStatus.IN_USE
                tape.last_used_date = datetime.now()
                logger.info(f"磁带 {tape_id} 加载成功")
                return True
            else:
                logger.error(f"磁带 {tape_id} 加载失败")
                return False

        except Exception as e:
            logger.error(f"加载磁带 {tape_id} 失败: {str(e)}")
            return False

    async def unload_tape(self) -> bool:
        """卸载当前磁带"""
        try:
            if not self.current_tape:
                logger.warning("没有加载的磁带")
                return True

            tape_id = self.current_tape.tape_id
            success = await self.tape_operations.unload_tape()

            if success:
                self.current_tape.status = TapeStatus.AVAILABLE
                self.current_tape = None
                logger.info(f"磁带 {tape_id} 卸载成功")
                return True
            else:
                logger.error(f"磁带 {tape_id} 卸载失败")
                return False

        except Exception as e:
            logger.error(f"卸载磁带失败: {str(e)}")
            return False

    async def erase_tape(self, tape_id: str) -> bool:
        """擦除磁带"""
        try:
            if tape_id not in self.tape_cartridges:
                logger.error(f"磁带 {tape_id} 不存在")
                return False

            tape = self.tape_cartridges[tape_id]

            # 加载磁带（如果未加载）
            was_loaded = self.current_tape and self.current_tape.tape_id == tape_id
            if not was_loaded:
                if not await self.load_tape(tape_id):
                    return False

            # 执行擦除操作
            success = await self.tape_operations.erase_tape()
            if success:
                # 重置磁带信息
                tape.used_bytes = 0
                tape.created_date = datetime.now()
                tape.expiry_date = datetime.now() + timedelta(days=self.settings.DEFAULT_RETENTION_MONTHS * 30)
                tape.status = TapeStatus.AVAILABLE
                tape.last_erase_date = datetime.now()

                logger.info(f"磁带 {tape_id} 擦除成功")

            # 如果原本未加载，则卸载
            if not was_loaded:
                await self.unload_tape()

            return success

        except Exception as e:
            logger.error(f"擦除磁带 {tape_id} 失败: {str(e)}")
            return False

    async def write_data(self, data: bytes, block_number: int = 0) -> bool:
        """写入数据到磁带"""
        try:
            if not self.current_tape:
                logger.error("没有加载的磁带")
                return False

            success = await self.tape_operations.write_data(data, block_number)
            if success:
                # 更新磁带使用信息
                self.current_tape.used_bytes += len(data)
                self.current_tape.last_write_date = datetime.now()

                # 检查容量
                if self.current_tape.used_bytes >= self.current_tape.capacity_bytes:
                    logger.warning(f"磁带 {self.current_tape.tape_id} 已满")
                    await self.unload_tape()

            return success

        except Exception as e:
            logger.error(f"写入数据失败: {str(e)}")
            return False

    async def read_data(self, block_number: int = 0, block_size: int = None) -> Optional[bytes]:
        """从磁带读取数据"""
        try:
            if not self.current_tape:
                logger.error("没有加载的磁带")
                return None

            if block_size is None:
                block_size = self.settings.DEFAULT_BLOCK_SIZE

            return await self.tape_operations.read_data(block_number, block_size)

        except Exception as e:
            logger.error(f"读取数据失败: {str(e)}")
            return None

    async def get_tape_info(self) -> Optional[Dict[str, Any]]:
        """获取当前磁带信息"""
        if not self.current_tape:
            return None

        try:
            scsi_info = await self.scsi_interface.get_tape_info()
            tape_info = {
                'tape_id': self.current_tape.tape_id,
                'label': self.current_tape.label,
                'status': self.current_tape.status.value,
                'capacity_bytes': self.current_tape.capacity_bytes,
                'used_bytes': self.current_tape.used_bytes,
                'free_bytes': self.current_tape.capacity_bytes - self.current_tape.used_bytes,
                'usage_percent': (self.current_tape.used_bytes / self.current_tape.capacity_bytes) * 100,
                'created_date': self.current_tape.created_date.isoformat(),
                'expiry_date': self.current_tape.expiry_date.isoformat(),
                'location': self.current_tape.location,
                'scsi_info': scsi_info
            }
            return tape_info

        except Exception as e:
            logger.error(f"获取磁带信息失败: {str(e)}")
            return None

    async def check_retention_periods(self):
        """检查磁带保留期"""
        try:
            expired_tapes = []
            for tape in self.tape_cartridges.values():
                if tape.is_expired() and tape.status != TapeStatus.EXPIRED:
                    expired_tapes.append(tape)

            if expired_tapes:
                logger.info(f"发现 {len(expired_tapes)} 个过期磁带")

                if self.settings.AUTO_ERASE_EXPIRED:
                    for tape in expired_tapes:
                        logger.info(f"自动擦除过期磁带: {tape.tape_id}")
                        await self.erase_tape(tape.tape_id)

                        # 发送通知
                        from ..utils.dingtalk_notifier import DingTalkNotifier
                        notifier = DingTalkNotifier()
                        await notifier.send_tape_notification(
                            tape.tape_id,
                            "expired",
                            {'expiry_date': tape.expiry_date.isoformat()}
                        )

        except Exception as e:
            logger.error(f"检查磁带保留期失败: {str(e)}")

    async def _cleanup_expired_tapes(self):
        """清理过期磁带"""
        await self.check_retention_periods()

    async def _monitoring_loop(self):
        """磁带监控循环"""
        while self._initialized:
            try:
                # 检查磁带状态
                if self.current_tape:
                    tape_info = await self.get_tape_info()
                    if tape_info:
                        # 检查容量预警
                        usage_percent = tape_info['usage_percent']
                        if usage_percent > 90:
                            from ..utils.dingtalk_notifier import DingTalkNotifier
                            notifier = DingTalkNotifier()
                            await notifier.send_capacity_warning(usage_percent, tape_info)

                # 等待下次检查
                await asyncio.sleep(self.settings.TAPE_CHECK_INTERVAL)

            except asyncio.CancelledError:
                break
            except Exception as e:
                logger.error(f"磁带监控循环异常: {str(e)}")
                await asyncio.sleep(60)  # 出错后等待1分钟

    async def health_check(self) -> bool:
        """健康检查"""
        try:
            # 检查SCSI接口
            if not await self.scsi_interface.health_check():
                return False

            # 检查当前磁带状态
            if self.current_tape:
                tape_info = await self.get_tape_info()
                if not tape_info:
                    return False

            return True

        except Exception as e:
            logger.error(f"磁带管理器健康检查失败: {str(e)}")
            return False

    async def get_inventory_status(self) -> Dict[str, Any]:
        """获取库存状态"""
        try:
            total_tapes = len(self.tape_cartridges)
            available_tapes = len([t for t in self.tape_cartridges.values() if t.status == TapeStatus.AVAILABLE])
            in_use_tapes = len([t for t in self.tape_cartridges.values() if t.status == TapeStatus.IN_USE])
            expired_tapes = len([t for t in self.tape_cartridges.values() if t.is_expired()])

            total_capacity = sum(t.capacity_bytes for t in self.tape_cartridges.values())
            used_capacity = sum(t.used_bytes for t in self.tape_cartridges.values())

            return {
                'total_tapes': total_tapes,
                'available_tapes': available_tapes,
                'in_use_tapes': in_use_tapes,
                'expired_tapes': expired_tapes,
                'total_capacity_bytes': total_capacity,
                'used_capacity_bytes': used_capacity,
                'free_capacity_bytes': total_capacity - used_capacity,
                'usage_percent': (used_capacity / total_capacity * 100) if total_capacity > 0 else 0,
                'current_tape': self.current_tape.tape_id if self.current_tape else None
            }

        except Exception as e:
            logger.error(f"获取库存状态失败: {str(e)}")
            return {}

    async def shutdown(self):
        """关闭磁带管理器"""
        try:
            self._initialized = False

            # 停止监控任务
            if self._monitoring_task:
                self._monitoring_task.cancel()
                try:
                    await self._monitoring_task
                except asyncio.CancelledError:
                    pass

            # 卸载当前磁带
            if self.current_tape:
                await self.unload_tape()

            # 关闭SCSI接口
            await self.scsi_interface.close()

            logger.info("磁带管理器已关闭")

        except Exception as e:
            logger.error(f"关闭磁带管理器时发生错误: {str(e)}")