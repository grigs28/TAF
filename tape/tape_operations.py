#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
磁带操作模块
Tape Operations Module
"""

import os
import asyncio
import logging
from typing import Optional, Dict, Any, Tuple
from datetime import datetime

from tape.tape_cartridge import TapeCartridge
from tape.itdt_interface import ITDTInterface
from config.settings import get_settings
from utils.tape_tools import tape_tools_manager

logger = logging.getLogger(__name__)


class TapeOperations:
    """磁带操作类"""

    def __init__(self):
        self.settings = get_settings()
        self.itdt_interface: ITDTInterface | None = None
        self._initialized = False

    async def initialize(self, scsi_interface=None, itdt_interface=None):
        """初始化磁带操作（ITDT）
        
        Args:
            scsi_interface: 已废弃，保留以兼容旧代码
            itdt_interface: 共享的ITDT接口实例（如果提供，则不再创建新实例）
        """
        try:
            # 如果提供了 ITDT 接口，直接使用（避免重复初始化）
            if itdt_interface:
                self.itdt_interface = itdt_interface
                logger.info("磁带操作模块(ITDT)使用共享接口，跳过重复初始化")
            else:
                # 向后兼容：如果没有提供，则创建新实例
                self.itdt_interface = ITDTInterface()
                await self.itdt_interface.initialize()
                logger.info("磁带操作模块(ITDT)初始化完成")
            
            self._initialized = True
        except Exception as e:
            logger.error(f"磁带操作模块(ITDT)初始化失败: {str(e)}")
            raise

    async def _ensure_initialized(self) -> bool:
        """懒加载初始化，确保 ITDT 可用。"""
        if self._initialized and self.itdt_interface:
            return True
        try:
            # 如果还没有实例，创建新的（向后兼容）
            if not self.itdt_interface:
                self.itdt_interface = ITDTInterface()
                await self.itdt_interface.initialize()
            self._initialized = True
            return True
        except Exception as e:
            logger.error(f"初始化ITDT失败: {str(e)}")
            return False

    async def load_tape(self, tape_cartridge: TapeCartridge) -> bool:
        """加载磁带"""
        try:
            if not self._initialized:
                logger.error("磁带操作模块未初始化")
                return False

            logger.info(f"正在加载磁带: {tape_cartridge.tape_id}")

            # 在实际实现中，这里会：
            # 1. 检查磁带驱动器状态
            # 2. 执行加载操作
            # 3. 验证磁带是否成功加载

            # 检查设备就绪状态（增加重试和更详细的错误信息）
            logger.info("检查磁带设备就绪状态...")
            if not await self._wait_for_tape_ready():
                logger.error("磁带设备未就绪，可能原因：设备未连接、磁带未加载、设备忙或故障")
                # 尝试获取更详细的设备状态信息
                try:
                    devices = await self.itdt_interface.scan_devices() if self.itdt_interface else []
                    if not devices:
                        logger.error("未检测到任何磁带设备")
                    else:
                        logger.info(f"检测到 {len(devices)} 个磁带设备")
                        for device in devices:
                            logger.info(f"设备: {device.get('path', 'N/A')}")
                except Exception as dev_error:
                    logger.warning(f"获取设备信息失败: {str(dev_error)}")
                return False

            # 执行倒带操作
            if not await self._rewind():
                logger.error("磁带倒带失败")
                return False

            # 读取磁带卷标（如果有）
            tape_label = await self._read_tape_label()
            if tape_label:
                logger.info(f"读取到磁带卷标: {tape_label}")

            logger.info(f"磁带 {tape_cartridge.tape_id} 加载成功")
            return True

        except Exception as e:
            logger.error(f"加载磁带失败: {str(e)}")
            return False

    async def unload_tape(self) -> bool:
        """卸载磁带"""
        try:
            if not self._initialized:
                logger.error("磁带操作模块未初始化")
                return False

            logger.info("正在卸载磁带")

            # 写入文件标记
            await self._write_filemark()

            # 倒带
            await self._rewind()

            # 在实际实现中，这里会执行卸载操作
            # 模拟卸载延迟
            await asyncio.sleep(2)

            logger.info("磁带卸载成功")
            return True

        except Exception as e:
            logger.error(f"卸载磁带失败: {str(e)}")
            return False

    async def erase_tape(self, backup_task=None, progress_callback=None) -> bool:
        """擦除磁带
        
        Args:
            backup_task: 备份任务对象，用于更新进度
            progress_callback: 进度回调函数，用于更新进度到数据库
        
        Returns:
            True=擦除成功，False=失败
        """
        try:
            if not self._initialized:
                logger.error("磁带操作模块未初始化")
                return False

            logger.info("正在擦除磁带")

            # 倒带到开始
            if not await self._rewind():
                logger.error("倒带失败，无法擦除磁带")
                return False

            # 执行擦除命令（传递backup_task和progress_callback以更新进度）
            success = await self._execute_erase_command(
                long_erase=True,
                backup_task=backup_task,
                progress_callback=progress_callback
            )
            if not success:
                logger.error("擦除命令执行失败")
                return False

            # 再次倒带
            await self._rewind()

            logger.info("磁带擦除成功")
            return True

        except Exception as e:
            logger.error(f"擦除磁带失败: {str(e)}")
            return False

    async def erase_preserve_label(self, backup_task=None, progress_callback=None, use_current_year_month: bool = False) -> bool:
        """格式化磁带并保留卷标信息（使用LtfsCmdFormat.exe格式化，格式化本身会清空磁带）
        
        Args:
            backup_task: 备份任务对象（可选）
            progress_callback: 进度回调函数（可选）
            use_current_year_month: 是否使用当前年月生成卷标（计划任务使用），默认为False（保留原卷标）
        """
        try:
            if not await self._ensure_initialized():
                logger.error("磁带操作模块未初始化")
                return False

            # 读取当前卷标元数据（格式化前记录原卷标）
            metadata = await self._read_tape_label()
            original_tape_id = None
            original_label = None
            
            if metadata:
                original_tape_id = metadata.get("tape_id")
                original_label = metadata.get("label") or original_tape_id
                logger.info(f"格式化前记录原卷标: tape_id={original_tape_id}, label={original_label}")
            else:
                logger.info("格式化前未读取到卷标，将使用新卷标格式化")
            
            # 获取盘符（不带冒号）
            drive_letter = (self.settings.TAPE_DRIVE_LETTER or "O").strip().upper()
            if drive_letter.endswith(":"):
                drive_letter = drive_letter[:-1]
            
            # 准备卷标和序列号
            from datetime import datetime
            now = datetime.now()
            current_year = now.year
            current_month = now.month
            
            if use_current_year_month:
                # 计划任务：使用当前年月生成卷标
                # 格式：TP{YYYY}{MM}01（例如：TP20251101）
                label = f"TP{current_year:04d}{current_month:02d}01"
                serial_number = None
                logger.info(f"计划任务格式化：使用当前年月生成卷标 {label}")
            elif metadata:
                # 保留原卷标（备份引擎调用）
                tape_id = metadata.get("tape_id")
                label = metadata.get("label") or tape_id
                serial_number = metadata.get("serial_number")
                logger.info(f"完整备份前格式化：将保留磁带卷标 {label}")
            else:
                # 如果没有元数据，使用当前年月日格式（兼容旧逻辑）
                label = f"TP{now.strftime('%Y%m%d')}"
                serial_number = None
                logger.info("完整备份前格式化：未读取到磁带卷标，将使用默认卷标格式化")
            
            logger.info(f"使用LtfsCmdFormat.exe格式化并设置卷标: {label}")
            
            # 初始化进度为0%
            if backup_task:
                backup_task.progress_percent = 0.0
                if progress_callback:
                    await progress_callback(backup_task, 0, 0)
            
            # 使用tape_tools_manager.format_tape_ltfs格式化并设置卷标
            # LtfsCmdFormat格式化本身会清空磁带，不需要先执行擦除
            format_result = await tape_tools_manager.format_tape_ltfs(
                drive_letter=drive_letter,
                volume_label=label,
                serial=serial_number if serial_number and len(serial_number) == 6 and serial_number.isalnum() and serial_number.isupper() else None,
                eject_after=False
            )
            
            # 格式化完成后，更新进度为100%
            if backup_task:
                backup_task.progress_percent = 100.0
                if progress_callback:
                    await progress_callback(backup_task, 1, 1)
            
            if format_result.get("success"):
                logger.info(f"LtfsCmdFormat格式化成功，卷标已设置为: {label}")
                
                # 格式化成功后，尝试更新数据库中的磁带记录
                # 读取格式化后的新卷标（从磁带读取）
                try:
                    new_metadata = await self._read_tape_label()
                    if new_metadata:
                        new_label = new_metadata.get("label") or new_metadata.get("tape_id") or label
                        new_tape_id = new_metadata.get("tape_id") or label
                        
                        # 使用原卷标查找数据库记录，更新为新卷标
                        await self._update_tape_label_in_database(
                            original_tape_id=original_tape_id,
                            original_label=original_label,
                            new_tape_id=new_tape_id,
                            new_label=new_label,
                            use_current_year_month=use_current_year_month
                        )
                        logger.info(f"数据库中的磁带记录已更新: 原卷标={original_label} -> 新卷标={new_label}")
                except Exception as db_error:
                    logger.warning(f"更新数据库磁带记录失败（格式化成功，但数据库未更新）: {str(db_error)}")
                    # 不因为数据库更新失败而返回False，格式化本身是成功的
                
                return True
            else:
                error_detail = format_result.get("stderr") or format_result.get("stdout") or "LtfsCmdFormat执行失败"
                logger.error(f"LtfsCmdFormat格式化失败: {error_detail}")
                return False

        except Exception as e:
            logger.error(f"格式化并保留卷标失败: {str(e)}")
            return False
    
    async def _update_tape_label_in_database(self, original_tape_id: Optional[str], original_label: Optional[str],
                                            new_tape_id: str, new_label: str, use_current_year_month: bool = False):
        """更新数据库中的磁带卷标（使用原卷标查找记录，更新为新卷标）
        
        Args:
            original_tape_id: 格式化前的磁带ID（用于查找数据库记录）
            original_label: 格式化前的卷标（用于查找数据库记录）
            new_tape_id: 格式化后的新磁带ID
            new_label: 格式化后的新卷标
            use_current_year_month: 是否使用当前年月（计划任务格式化）
        """
        try:
            from utils.scheduler.db_utils import is_opengauss, get_opengauss_connection
            
            if not is_opengauss():
                logger.debug("非openGauss数据库，跳过数据库更新")
                return
            
            # 如果没有原卷标信息，无法更新数据库
            if not original_tape_id and not original_label:
                logger.warning("格式化前未记录原卷标，无法更新数据库记录")
                return
            
            conn = await get_opengauss_connection()
            try:
                # 使用原卷标查找数据库记录
                # 优先使用tape_id查找，如果没有则使用label查找
                old_tape = None
                
                if original_tape_id:
                    old_tape = await conn.fetchrow(
                        "SELECT tape_id, label FROM tape_cartridges WHERE tape_id = $1",
                        original_tape_id
                    )
                
                if not old_tape and original_label:
                    old_tape = await conn.fetchrow(
                        "SELECT tape_id, label FROM tape_cartridges WHERE label = $1 LIMIT 1",
                        original_label
                    )
                
                if old_tape:
                    old_tape_id = old_tape['tape_id']
                    
                    # 检查新tape_id是否已存在（避免主键冲突）
                    existing = await conn.fetchrow(
                        "SELECT tape_id FROM tape_cartridges WHERE tape_id = $1",
                        new_tape_id
                    )
                    
                    if existing and existing['tape_id'] != old_tape_id:
                        # 新tape_id已存在且不是当前记录，只更新label
                        logger.warning(f"新tape_id {new_tape_id} 已存在，只更新label字段")
                        await conn.execute(
                            "UPDATE tape_cartridges SET label = $1 WHERE tape_id = $2",
                            new_label, old_tape_id
                        )
                        logger.info(f"更新数据库：tape_id={old_tape_id}（保持不变）, label={new_label}")
                    else:
                        # 更新tape_id和label
                        await conn.execute(
                            "UPDATE tape_cartridges SET tape_id = $1, label = $2 WHERE tape_id = $3",
                            new_tape_id, new_label, old_tape_id
                        )
                        logger.info(f"更新数据库：tape_id {old_tape_id} -> {new_tape_id}, label={new_label}")
                else:
                    # 找不到原记录，尝试创建新记录（如果使用当前年月）
                    if use_current_year_month:
                        logger.info(f"未找到原卷标记录，尝试创建新记录: tape_id={new_tape_id}, label={new_label}")
                        try:
                            await conn.execute(
                                """
                                INSERT INTO tape_cartridges (tape_id, label, status, capacity_bytes, used_bytes, created_at, updated_at)
                                VALUES ($1, $2, $3, $4, $5, NOW(), NOW())
                                ON CONFLICT (tape_id) DO UPDATE SET label = $2, updated_at = NOW()
                                """,
                                new_tape_id, new_label, 'available', 0, 0
                            )
                            logger.info(f"创建/更新数据库记录：tape_id={new_tape_id}, label={new_label}")
                        except Exception as insert_error:
                            logger.warning(f"创建新记录失败: {str(insert_error)}")
                    else:
                        logger.warning(f"未找到原卷标记录（tape_id={original_tape_id}, label={original_label}），无法更新数据库")
                
                await conn.commit()
            finally:
                await conn.close()
                
        except Exception as e:
            logger.error(f"更新数据库磁带卷标失败: {str(e)}")
            # 不抛出异常，避免影响格式化流程

    async def write_data(self, data: bytes, block_number: int = 0) -> bool:
        """写入数据到磁带"""
        try:
            if not self._initialized:
                logger.error("磁带操作模块未初始化")
                return False

            if not data:
                logger.warning("写入数据为空")
                return True

            # 分块写入数据
            block_size = self.settings.DEFAULT_BLOCK_SIZE
            bytes_written = 0

            logger.debug(f"开始写入数据: {len(data)} 字节")

            for i in range(0, len(data), block_size):
                chunk = data[i:i + block_size]
                success = await self._write_block(chunk, block_number + (i // block_size))
                if not success:
                    logger.error(f"写入数据块失败: {i // block_size}")
                    return False
                bytes_written += len(chunk)

            logger.debug(f"数据写入完成: {bytes_written} 字节")
            return True

        except Exception as e:
            logger.error(f"写入数据失败: {str(e)}")
            return False

    async def read_data(self, block_number: int = 0, block_size: int = None) -> Optional[bytes]:
        """从磁带读取数据"""
        try:
            if not self._initialized:
                logger.error("磁带操作模块未初始化")
                return None

            if block_size is None:
                block_size = self.settings.DEFAULT_BLOCK_SIZE

            logger.debug(f"开始读取数据块: {block_number}, 大小: {block_size}")

            data = await self._read_block(block_number, block_size)
            if data:
                logger.debug(f"数据读取完成: {len(data)} 字节")
            else:
                logger.debug(f"读取数据块失败或无数据: {block_number}")

            return data

        except Exception as e:
            logger.error(f"读取数据失败: {str(e)}")
            return None

    async def write_filemark(self) -> bool:
        """写入文件标记"""
        try:
            return await self._write_filemark()
        except Exception as e:
            logger.error(f"写入文件标记失败: {str(e)}")
            return False

    async def position_to_block(self, block_number: int) -> bool:
        """定位到指定数据块"""
        try:
            return await self._position_to_block(block_number)
        except Exception as e:
            logger.error(f"定位数据块失败: {str(e)}")
            return False

    async def get_tape_position(self) -> Optional[int]:
        """获取当前磁带位置"""
        try:
            return await self._get_tape_position()
        except Exception as e:
            logger.error(f"获取磁带位置失败: {str(e)}")
            return None

    async def get_tape_capacity(self) -> Optional[Tuple[int, int]]:
        """获取磁带容量信息"""
        try:
            return await self._get_tape_capacity()
        except Exception as e:
            logger.error(f"获取磁带容量失败: {str(e)}")
            return None

    async def _wait_for_tape_ready(self, timeout: int = 30) -> bool:
        """等待磁带就绪"""
        try:
            logger.debug(f"开始等待磁带就绪（超时: {timeout}秒）...")
            for attempt in range(timeout):
                try:
                    if self.itdt_interface and await self.itdt_interface.test_unit_ready(None):
                        logger.debug(f"磁带设备已就绪（第 {attempt + 1} 次尝试）")
                        return True
                except Exception as test_error:
                    logger.debug(f"第 {attempt + 1} 次就绪检查失败: {str(test_error)}")
                    # 继续重试，不立即返回
                
                if attempt < timeout - 1:  # 最后一次不需要等待
                    await asyncio.sleep(1)
            
            logger.warning(f"等待 {timeout} 秒后磁带设备仍未就绪")
            return False
        except Exception as e:
            logger.error(f"等待磁带就绪过程异常: {str(e)}")
            return False

    async def _rewind(self) -> bool:
        """倒带操作"""
        try:
            if not self.itdt_interface:
                return False
            return await self.itdt_interface.rewind(None)
        except Exception as e:
            logger.error(f"倒带操作失败: {str(e)}")
            return False

    async def _write_block(self, data: bytes, block_number: int) -> bool:
        """写入单个数据块"""
        try:
            # 使用SCSI接口的write_tape_data方法
            result = await self.scsi_interface.write_tape_data(
                device_path=None,
                data=data,
                block_number=block_number,
                block_size=self.settings.DEFAULT_BLOCK_SIZE
            )

            if result['success']:
                return True
            else:
                logger.error(f"写入数据块失败: {result.get('error', '未知错误')}")
                return False

        except Exception as e:
            logger.error(f"写入数据块异常: {str(e)}")
            return False

    async def _read_block(self, block_number: int, block_size: int) -> Optional[bytes]:
        """读取单个数据块"""
        try:
            # 使用SCSI接口的read_tape_data方法
            result = await self.scsi_interface.read_tape_data(
                device_path=None,
                block_number=block_number,
                block_count=1,
                block_size=block_size
            )

            if result['success']:
                return result.get('data', b'')
            else:
                logger.debug(f"读取数据块失败: {result.get('error', '未知错误')}")
                return None

        except Exception as e:
            logger.error(f"读取数据块异常: {str(e)}")
            return None

    async def _write_filemark(self) -> bool:
        """写入文件标记"""
        try:
            # 构造WRITE_FILEMARKS(6) SCSI命令
            cdb = bytes([0x10, 0x00, 0x00, 0x01, 0x00, 0x00])

            result = await self.scsi_interface.execute_scsi_command(
                device_path=None,
                cdb=cdb,
                timeout=30
            )

            return result['success']

        except Exception as e:
            logger.error(f"写入文件标记异常: {str(e)}")
            return False

    async def _execute_erase_command(self, long_erase: bool = True, backup_task=None, progress_callback=None) -> bool:
        """执行擦除命令（LONG ERASE - 整盘物理清0）
        
        Args:
            long_erase: True=长擦除（整盘物理清0，耗时约3小时），False=短擦除
            backup_task: 备份任务对象，用于更新进度（0-100%）
            progress_callback: 进度回调函数，用于更新进度到数据库
        
        Returns:
            True=擦除完成，False=失败或被取消
        """
        try:
            # ERASE(6) SCSI命令
            # Byte 1: Erase Type (bit 0: 0=short, 1=long)
            # 参考代码：long=bit0=1，即 0x01 表示LONG ERASE
            erase_type = 0x01 if long_erase else 0x00
            cdb = bytes([0x19, erase_type, 0x00, 0x00, 0x00, 0x00])
            
            # LONG ERASE可能需要3小时，超时时间设置为3小时（10800秒）
            timeout_seconds = 10800 if long_erase else 600
            
            if long_erase:
                logger.info("========== 开始LONG ERASE（整盘物理清0）==========")
                logger.warning("⚠️ LONG ERASE期间请勿断电或重启驱动器！")
                logger.info(f"预计耗时约3小时，超时时间: {timeout_seconds}秒")
                
                # 初始化进度为0%
                if backup_task:
                    backup_task.progress_percent = 0.0
                    if progress_callback:
                        await progress_callback(backup_task, 0, 0)
            
            # 1) 发送ERASE命令
            result = await self.scsi_interface.execute_scsi_command(
                device_path=None,
                cdb=cdb,
                timeout=timeout_seconds
            )
            
            if not result.get('success', False):
                logger.error("驱动器拒绝ERASE命令或命令执行失败")
                return False
            
            # 2) 对于LONG ERASE，需要轮询TEST UNIT READY直到设备不再忙碌
            if long_erase:
                logger.info("ERASE命令已发送，开始轮询设备状态...")
                poll_count = 0
                poll_interval = 15  # 每15秒轮询一次
                estimated_total_polls = 720  # 预计3小时 = 10800秒 / 15秒 = 720次轮询
                
                while True:
                    await asyncio.sleep(poll_interval)
                    poll_count += 1
                    
                    # 计算进度：基于轮询次数估算（最多到99%，完成时设为100%）
                    if backup_task:
                        # 进度计算：0% -> 99% (基于轮询次数)
                        progress = min(99.0, (poll_count / estimated_total_polls) * 99.0)
                        backup_task.progress_percent = progress
                        
                        # 更新进度到数据库
                        if progress_callback:
                            await progress_callback(backup_task, poll_count, estimated_total_polls)
                    
                    # 发送TEST UNIT READY命令检查设备状态
                    tur_result = await self.scsi_interface.execute_scsi_command(
                        device_path=None,
                        cdb=bytes([0x00, 0x00, 0x00, 0x00, 0x00, 0x00]),  # TEST UNIT READY
                        timeout=30
                    )
                    
                    if tur_result.get('success', False):
                        # 擦除完成，设置进度为100%
                        if backup_task:
                            backup_task.progress_percent = 100.0
                            if progress_callback:
                                await progress_callback(backup_task, estimated_total_polls, estimated_total_polls)
                        
                        elapsed_minutes = poll_count * poll_interval // 60
                        logger.info(f"✅ LONG ERASE完成成功！总耗时约 {elapsed_minutes} 分钟，进度: 100%")
                        return True
                    
                    # 每6分钟（24次轮询）打印一次进度
                    if poll_count % 24 == 0:
                        elapsed_minutes = poll_count * poll_interval // 60
                        current_progress = backup_task.progress_percent if backup_task else 0.0
                        logger.info(f"LONG ERASE进行中... 已耗时约 {elapsed_minutes} 分钟，进度: {current_progress:.1f}%，请继续等待...")
            
            # 短擦除直接返回成功
            if backup_task:
                backup_task.progress_percent = 100.0
                if progress_callback:
                    await progress_callback(backup_task, 1, 1)
            logger.info("擦除完成")
            return True

        except asyncio.CancelledError:
            logger.warning("擦除操作被用户取消")
            if backup_task:
                backup_task.progress_percent = 0.0
                if progress_callback:
                    await progress_callback(backup_task, 0, 0)
            raise
        except Exception as e:
            logger.error(f"执行擦除命令异常: {str(e)}")
            if backup_task:
                backup_task.progress_percent = 0.0
                if progress_callback:
                    await progress_callback(backup_task, 0, 0)
            return False

    async def _position_to_block(self, block_number: int) -> bool:
        """定位到指定数据块"""
        try:
            # 构造SPACE(6) SCSI命令
            # SPACE: 11 00 XX XX XX XX
            # XX: 代码 (00=块, 01=文件标记, 02=顺序文件标记, 03=结束数据)
            cdb = bytes([
                0x11,  # SPACE操作码
                0x00,  # 代码=0 (块)
                (block_number >> 16) & 0xFF,
                (block_number >> 8) & 0xFF,
                block_number & 0xFF,
                0x00   # 控制字节
            ])

            result = await self.scsi_interface.execute_scsi_command(
                device_path=None,
                cdb=cdb,
                timeout=60
            )

            return result['success']

        except Exception as e:
            logger.error(f"定位数据块异常: {str(e)}")
            return False

    async def _get_tape_position(self) -> Optional[int]:
        """获取当前磁带位置"""
        try:
            # 构造READ_POSITION SCSI命令
            cdb = bytes([0x34, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x01, 0x00, 0x00])

            result = await self.scsi_interface.execute_scsi_command(
                device_path=None,
                cdb=cdb,
                data_direction=1,
                data_length=20,
                timeout=30
            )

            if result['success']:
                data = result['data']
                if len(data) >= 20:
                    # 解析位置信息
                    block_number = int.from_bytes(data[4:8], byteorder='big')
                    return block_number

            return None

        except Exception as e:
            logger.error(f"获取磁带位置异常: {str(e)}")
            return None

    async def _get_tape_capacity(self) -> Optional[Tuple[int, int]]:
        """获取磁带容量信息"""
        try:
            # 构造READ_CAPACITY SCSI命令
            cdb = bytes([0x25, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00])

            result = await self.scsi_interface.execute_scsi_command(
                device_path=None,
                cdb=cdb,
                data_direction=1,
                data_length=8,
                timeout=30
            )

            if result['success']:
                data = result['data']
                if len(data) >= 8:
                    # 解析容量信息
                    last_block = int.from_bytes(data[0:4], byteorder='big')
                    block_size = int.from_bytes(data[4:8], byteorder='big')

                    total_capacity = (last_block + 1) * block_size
                    return (total_capacity, block_size)

            return None

        except Exception as e:
            logger.error(f"获取磁带容量异常: {str(e)}")
            return None

    async def _is_tape_formatted(self) -> bool:
        """检查磁带是否已格式化（使用ITDT qrypart命令）
        
        逻辑：
        - 命令执行成功 + 有分区信息 = 已格式化
        - 其他所有情况 = 未格式化
        """
        try:
            if not self.itdt_interface or not self.itdt_interface._initialized:
                await self._ensure_initialized()
            
            # 使用ITDT查询分区信息
            partition_info = await self.itdt_interface.query_partition()
            
            # 命令执行成功 + 有分区 = 已格式化
            is_formatted = partition_info.get("has_partitions", False)
            
            logger.info(f"ITDT格式化检测结果（qrypart）: {is_formatted}")
            
            return is_formatted
        except Exception as e:
            logger.warning(f"使用ITDT检测格式化状态失败: {str(e)}", exc_info=True)
            # 任何异常都认为未格式化
            return False

    async def _read_tape_label(self) -> Optional[Dict[str, Any]]:
        """读取磁带卷标（使用fsutil获取Windows卷标）"""
        logger.info("========== 开始读取磁带卷标 ==========")
        try:
            import platform
            
            logger.info(f"操作系统: {platform.system()}, LTFS盘符配置: {getattr(self.settings, 'TAPE_DRIVE_LETTER', None)}")
            
            # Windows系统且配置了LTFS盘符，使用fsutil读取卷标
            if platform.system() == "Windows" and self.settings.TAPE_DRIVE_LETTER:
                drive_letter = self.settings.TAPE_DRIVE_LETTER.upper()
                drive_with_colon = f"{drive_letter}:" if not drive_letter.endswith(':') else drive_letter
                logger.info(f"使用fsutil读取磁带卷标: {drive_with_colon}")
                
                try:
                    # 检查驱动器是否存在
                    if not os.path.exists(drive_with_colon):
                        logger.warning(f"驱动器 {drive_with_colon} 不存在或未挂载")
                        return None
                    
                    # 使用fsutil获取卷信息
                    proc = await asyncio.create_subprocess_shell(
                        f"fsutil fsinfo volumeinfo {drive_with_colon}",
                        stdout=asyncio.subprocess.PIPE,
                        stderr=asyncio.subprocess.PIPE
                    )
                    
                    # 添加超时处理，避免阻塞
                    try:
                        stdout, stderr = await asyncio.wait_for(proc.communicate(), timeout=10)
                    except asyncio.TimeoutError:
                        logger.warning(f"fsutil命令执行超时，尝试终止进程...")
                        if proc.returncode is None:
                            proc.kill()
                            await proc.wait()
                        stdout = b""
                        stderr = b""
                        logger.error("fsutil命令执行超时")
                        return None
                    
                    stdout_str = stdout.decode('gbk', errors='ignore') if stdout else ""
                    
                    if proc.returncode == 0:
                        # 解析fsutil输出
                        volume_info = {}
                        for line in stdout_str.split('\n'):
                            line = line.strip()
                            if ':' in line:
                                key, value = line.split(':', 1)
                                volume_info[key.strip()] = value.strip()
                        
                        # 提取卷标
                        volume_name = volume_info.get('卷名', volume_info.get('Volume Name', ''))
                        serial_number = volume_info.get('卷序列号', volume_info.get('Volume Serial Number', ''))
                        
                        if volume_name:
                            metadata = {
                                'tape_id': volume_name,
                                'label': volume_name,
                                'serial_number': serial_number,
                                'file_system': volume_info.get('文件系统名', volume_info.get('File System Name', ''))
                            }
                            logger.info(f"从fsutil读取磁带卷标成功: {volume_name}, 序列号: {serial_number}")
                            return metadata
                        else:
                            logger.warning("fsutil未返回卷标信息")
                            return None
                    else:
                        logger.warning(f"fsutil执行失败，返回码: {proc.returncode}")
                        return None
                        
                except Exception as e:
                    logger.warning(f"使用fsutil读取卷标失败: {str(e)}", exc_info=True)
                    return None
            else:
                logger.info("未配置LTFS盘符或非Windows系统")
                return None
            
        except Exception as e:
            logger.error(f"读取磁带卷标异常: {str(e)}", exc_info=True)
            return None

    async def _write_tape_label(self, tape_info: Dict[str, Any]) -> bool:
        """写入磁带卷标（使用Windows label命令设置卷标）"""
        try:
            import platform
            
            # Windows系统且配置了LTFS盘符，使用label命令设置卷标
            if platform.system() == "Windows" and self.settings.TAPE_DRIVE_LETTER:
                drive_letter = self.settings.TAPE_DRIVE_LETTER.upper()
                drive_with_colon = f"{drive_letter}:" if not drive_letter.endswith(':') else drive_letter
                
                try:
                    # 检查驱动器是否存在
                    if not os.path.exists(drive_with_colon):
                        logger.warning(f"驱动器 {drive_with_colon} 不存在或未挂载")
                        return False
                    
                    # 获取卷标
                    tape_id = tape_info.get('tape_id', '')
                    label = tape_info.get('label', tape_id)
                    
                    logger.info(f"使用label命令设置卷标: {label} 到驱动器 {drive_with_colon}")
                    
                    # 使用label命令设置卷标
                    # 格式: echo label_name | label drive:
                    proc = await asyncio.create_subprocess_shell(
                        f'echo {label}| label {drive_with_colon}',
                        stdout=asyncio.subprocess.PIPE,
                        stderr=asyncio.subprocess.PIPE
                    )
                    
                    # 添加超时处理，避免阻塞
                    try:
                        stdout, stderr = await asyncio.wait_for(proc.communicate(), timeout=10)
                    except asyncio.TimeoutError:
                        logger.warning(f"label命令执行超时，尝试终止进程...")
                        if proc.returncode is None:
                            proc.kill()
                            await proc.wait()
                        logger.error("label命令执行超时")
                        return False
                    stdout_str = stdout.decode('gbk', errors='ignore') if stdout else ""
                    stderr_str = stderr.decode('gbk', errors='ignore') if stderr else ""
                    
                    if proc.returncode == 0:
                        logger.info(f"卷标设置成功: {label}")
                        return True
                    else:
                        logger.warning(f"label命令执行失败，返回码: {proc.returncode}")
                        if stderr_str:
                            logger.warning(f"错误信息: {stderr_str}")
                        return False
                        
                except Exception as e:
                    logger.warning(f"使用label命令设置卷标失败: {str(e)}")
                    return False
            else:
                logger.error("无法写入磁带卷标：未配置LTFS盘符或非Windows系统")
                return False
                
        except Exception as e:
            logger.error(f"写入磁带卷标异常: {str(e)}")
            return False

    # IBM LTO特定功能
    async def get_ibm_tape_alerts(self) -> Dict[str, Any]:
        """获取IBM磁带警报信息"""
        try:
            if not self._initialized or not self.scsi_interface:
                return {'success': False, 'error': '磁带操作模块未初始化'}

            # 使用LOG SENSE命令获取TapeAlert信息
            result = await self.scsi_interface.send_ibm_specific_command(
                device_path=None,
                command_type="log_sense",
                parameters={'page_code': 0x2E}  # TapeAlert页面
            )

            if result['success']:
                return self._parse_tape_alert_data(result['log_data'])
            else:
                return result

        except Exception as e:
            logger.error(f"获取IBM磁带警报失败: {str(e)}")
            return {'success': False, 'error': str(e)}

    async def get_ibm_performance_stats(self) -> Dict[str, Any]:
        """获取IBM磁带性能统计"""
        try:
            if not self._initialized or not self.scsi_interface:
                return {'success': False, 'error': '磁带操作模块未初始化'}

            # 获取性能统计日志
            result = await self.scsi_interface.send_ibm_specific_command(
                device_path=None,
                command_type="log_sense",
                parameters={'page_code': 0x17}  # 性能统计页面
            )

            if result['success']:
                return self._parse_performance_data(result['log_data'])
            else:
                return result

        except Exception as e:
            logger.error(f"获取IBM性能统计失败: {str(e)}")
            return {'success': False, 'error': str(e)}

    async def get_ibm_tape_usage(self) -> Dict[str, Any]:
        """获取IBM磁带使用统计"""
        try:
            if not self._initialized or not self.scsi_interface:
                return {'success': False, 'error': '磁带操作模块未初始化'}

            # 获取磁带使用统计
            result = await self.scsi_interface.send_ibm_specific_command(
                device_path=None,
                command_type="log_sense",
                parameters={'page_code': 0x31}  # 磁带使用统计页面
            )

            if result['success']:
                return self._parse_usage_data(result['log_data'])
            else:
                return result

        except Exception as e:
            logger.error(f"获取IBM磁带使用统计失败: {str(e)}")
            return {'success': False, 'error': str(e)}

    async def enable_ibm_encryption(self, encryption_key: str = None) -> Dict[str, Any]:
        """启用IBM磁带加密"""
        try:
            if not self._initialized or not self.scsi_interface:
                return {'success': False, 'error': '磁带操作模块未初始化'}

            # 使用MODE SENSE/SELECT配置加密
            result = await self.scsi_interface.send_ibm_specific_command(
                device_path=None,
                command_type="mode_sense",
                parameters={'page_code': 0x1F}  # 加密控制页面
            )

            if not result['success']:
                return result

            # 解析当前加密设置
            current_mode = self._parse_encryption_mode(result['mode_data'])

            # 构造新的加密设置
            new_mode = self._build_encryption_mode(enable=True, key=encryption_key)

            # 发送MODE SELECT命令
            select_result = await self._send_mode_select(0x1F, new_mode)

            if select_result['success']:
                return {'success': True, 'message': '加密已启用'}
            else:
                return select_result

        except Exception as e:
            logger.error(f"启用IBM加密失败: {str(e)}")
            return {'success': False, 'error': str(e)}

    async def disable_ibm_encryption(self) -> Dict[str, Any]:
        """禁用IBM磁带加密"""
        try:
            if not self._initialized or not self.scsi_interface:
                return {'success': False, 'error': '磁带操作模块未初始化'}

            # 构造禁用加密的设置
            new_mode = self._build_encryption_mode(enable=False)

            # 发送MODE SELECT命令
            result = await self._send_mode_select(0x1F, new_mode)

            if result['success']:
                return {'success': True, 'message': '加密已禁用'}
            else:
                return result

        except Exception as e:
            logger.error(f"禁用IBM加密失败: {str(e)}")
            return {'success': False, 'error': str(e)}

    async def set_ibm_worm_mode(self, enable: bool = True) -> Dict[str, Any]:
        """设置IBM WORM模式"""
        try:
            if not self._initialized or not self.scsi_interface:
                return {'success': False, 'error': '磁带操作模块未初始化'}

            # 使用MODE SENSE/SELECT配置WORM模式
            page_code = 0x1D  # WORM控制页面

            if enable:
                # 启用WORM模式
                new_mode = self._build_worm_mode(enable=True)
            else:
                # 禁用WORM模式
                new_mode = self._build_worm_mode(enable=False)

            # 发送MODE SELECT命令
            result = await self._send_mode_select(page_code, new_mode)

            if result['success']:
                mode_str = "启用" if enable else "禁用"
                return {'success': True, 'message': f'WORM模式已{mode_str}'}
            else:
                return result

        except Exception as e:
            logger.error(f"设置IBM WORM模式失败: {str(e)}")
            return {'success': False, 'error': str(e)}

    async def get_ibm_temperature_status(self) -> Dict[str, Any]:
        """获取IBM磁带机温度状态"""
        try:
            if not self._initialized or not self.scsi_interface:
                return {'success': False, 'error': '磁带操作模块未初始化'}

            # 获取温度监控日志
            result = await self.scsi_interface.send_ibm_specific_command(
                device_path=None,
                command_type="log_sense",
                parameters={'page_code': 0x0D}  # 温度页面
            )

            if result['success']:
                return self._parse_temperature_data(result['log_data'])
            else:
                return result

        except Exception as e:
            logger.error(f"获取IBM温度状态失败: {str(e)}")
            return {'success': False, 'error': str(e)}

    async def get_ibm_drive_serial_number(self) -> Dict[str, Any]:
        """获取IBM磁带机序列号"""
        try:
            if not self._initialized or not self.scsi_interface:
                return {'success': False, 'error': '磁带操作模块未初始化'}

            # 使用INQUIRY VPD页面0x80获取单元序列号
            result = await self.scsi_interface.send_ibm_specific_command(
                device_path=None,
                command_type="inquiry_vpd",
                parameters={'page_code': 0x80}
            )

            if result['success']:
                vpd_data = bytes.fromhex(result['vpd_data'])
                if len(vpd_data) >= 4:
                    serial_length = vpd_data[3]
                    if len(vpd_data) >= 4 + serial_length:
                        serial_number = vpd_data[4:4+serial_length].decode('ascii', errors='ignore').strip()
                        return {
                            'success': True,
                            'serial_number': serial_number
                        }

            return {'success': False, 'error': '无法获取序列号'}

        except Exception as e:
            logger.error(f"获取IBM磁带机序列号失败: {str(e)}")
            return {'success': False, 'error': str(e)}

    async def get_ibm_firmware_version(self) -> Dict[str, Any]:
        """获取IBM磁带机固件版本"""
        try:
            if not self._initialized or not self.scsi_interface:
                return {'success': False, 'error': '磁带操作模块未初始化'}

            # 使用INQUIRY VPD页面获取固件版本
            result = await self.scsi_interface.send_ibm_specific_command(
                device_path=None,
                command_type="inquiry_vpd",
                parameters={'page_code': 0x00}  # 标准INQUIRY数据
            )

            if result['success']:
                vpd_data = bytes.fromhex(result['vpd_data'])
                if len(vpd_data) >= 36:
                    # 产品修订级别在字节32-35
                    revision = vpd_data[32:36].decode('ascii', errors='ignore').strip()
                    return {
                        'success': True,
                        'firmware_version': revision
                    }

            return {'success': False, 'error': '无法获取固件版本'}

        except Exception as e:
            logger.error(f"获取IBM固件版本失败: {str(e)}")
            return {'success': False, 'error': str(e)}

    async def run_ibm_self_test(self) -> Dict[str, Any]:
        """运行IBM磁带机自检"""
        try:
            if not self._initialized or not self.scsi_interface:
                return {'success': False, 'error': '磁带操作模块未初始化'}

            # 发送SEND DIAGNOSTIC命令
            cdb = bytes([0x1D, 0x00, 0x00, 0x00, 0x00, 0x00])

            result = await self.scsi_interface.execute_scsi_command(
                device_path=None,
                cdb=cdb,
                timeout=120  # 自检可能需要较长时间
            )

            if result['success']:
                return {'success': True, 'message': '自检完成'}
            else:
                return result

        except Exception as e:
            logger.error(f"运行IBM自检失败: {str(e)}")
            return {'success': False, 'error': str(e)}

    # 辅助方法
    def _parse_tape_alert_data(self, log_data_hex: str) -> Dict[str, Any]:
        """解析TapeAlert数据"""
        try:
            log_data = bytes.fromhex(log_data_hex)
            alerts = []

            # 解析TapeAlert标志
            if len(log_data) >= 4:
                flags = int.from_bytes(log_data[2:4], byteorder='big')

                # 常见TapeAlert标志
                tape_alert_flags = {
                    0: "磁带需要清理",
                    1: "磁带寿命即将结束",
                    2: "磁带介质错误",
                    3: "读/写错误率过高",
                    4: "驱动器需要维护",
                    5: "温度超出范围",
                    6: "电源问题",
                    7: "冷却风扇故障"
                }

                for bit, description in tape_alert_flags.items():
                    if flags & (1 << bit):
                        alerts.append(description)

            return {
                'success': True,
                'alerts': alerts,
                'alert_count': len(alerts),
                'raw_data': log_data_hex
            }

        except Exception as e:
            return {'success': False, 'error': f'解析TapeAlert数据失败: {str(e)}'}

    def _parse_performance_data(self, log_data_hex: str) -> Dict[str, Any]:
        """解析性能数据"""
        try:
            log_data = bytes.fromhex(log_data_hex)

            # 简化的性能数据解析
            if len(log_data) >= 20:
                performance = {
                    'total_mounts': int.from_bytes(log_data[4:8], byteorder='big'),
                    'total_rewinds': int.from_bytes(log_data[8:12], byteorder='big'),
                    'total_write_megabytes': int.from_bytes(log_data[12:16], byteorder='big'),
                    'total_read_megabytes': int.from_bytes(log_data[16:20], byteorder='big')
                }
            else:
                performance = {}

            return {
                'success': True,
                'performance': performance,
                'raw_data': log_data_hex
            }

        except Exception as e:
            return {'success': False, 'error': f'解析性能数据失败: {str(e)}'}

    def _parse_usage_data(self, log_data_hex: str) -> Dict[str, Any]:
        """解析使用数据"""
        try:
            log_data = bytes.fromhex(log_data_hex)

            # 简化的使用数据解析
            usage = {
                'percent_used': 0,
                'total_capacity_gb': 0,
                'used_capacity_gb': 0
            }

            if len(log_data) >= 8:
                # 假设使用数据格式（实际格式可能需要根据IBM文档调整）
                used_percent = log_data[4]
                usage['percent_used'] = used_percent

            return {
                'success': True,
                'usage': usage,
                'raw_data': log_data_hex
            }

        except Exception as e:
            return {'success': False, 'error': f'解析使用数据失败: {str(e)}'}

    def _parse_temperature_data(self, log_data_hex: str) -> Dict[str, Any]:
        """解析温度数据"""
        try:
            log_data = bytes.fromhex(log_data_hex)

            temperature = {
                'current_celsius': 0,
                'max_celsius': 0,
                'min_celsius': 0,
                'status': 'normal'
            }

            if len(log_data) >= 6:
                # 假设温度数据格式（实际格式需要根据IBM文档调整）
                current_temp = log_data[4]
                max_temp = log_data[5]

                temperature['current_celsius'] = current_temp
                temperature['max_celsius'] = max_temp

                if current_temp > 50:
                    temperature['status'] = 'warning'
                elif current_temp > 60:
                    temperature['status'] = 'critical'

            return {
                'success': True,
                'temperature': temperature,
                'raw_data': log_data_hex
            }

        except Exception as e:
            return {'success': False, 'error': f'解析温度数据失败: {str(e)}'}

    def _parse_encryption_mode(self, mode_data_hex: str) -> Dict[str, Any]:
        """解析加密模式数据"""
        try:
            mode_data = bytes.fromhex(mode_data_hex)

            # 简化的加密模式解析
            encryption = {
                'enabled': False,
                'algorithm': 'AES256',
                'key_index': 0
            }

            if len(mode_data) >= 10:
                # 假设加密状态在字节9
                flags = mode_data[9]
                encryption['enabled'] = bool(flags & 0x80)

            return encryption

        except Exception as e:
            logger.error(f"解析加密模式失败: {str(e)}")
            return {'enabled': False}

    def _build_encryption_mode(self, enable: bool = False, key: str = None) -> bytes:
        """构造加密模式数据"""
        try:
            # 构造简化的加密模式页面
            mode_data = bytearray([0x1F, 0x00, 0x08, 0x00, 0x00, 0x00, 0x00, 0x00])

            if enable:
                mode_data.append(0x80)  # 启用加密
            else:
                mode_data.append(0x00)  # 禁用加密

            mode_data.append(0x00)  # 保留字节

            return bytes(mode_data)

        except Exception as e:
            logger.error(f"构造加密模式失败: {str(e)}")
            return bytes([0x1F, 0x00, 0x08, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00])

    def _build_worm_mode(self, enable: bool = False) -> bytes:
        """构造WORM模式数据"""
        try:
            # 构造简化的WORM模式页面
            mode_data = bytearray([0x1D, 0x00, 0x08, 0x00, 0x00, 0x00, 0x00, 0x00])

            if enable:
                mode_data.append(0x01)  # 启用WORM
            else:
                mode_data.append(0x00)  # 禁用WORM

            mode_data.append(0x00)  # 保留字节

            return bytes(mode_data)

        except Exception as e:
            logger.error(f"构造WORM模式失败: {str(e)}")
            return bytes([0x1D, 0x00, 0x08, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00])

    async def _send_mode_select(self, page_code: int, mode_data: bytes) -> Dict[str, Any]:
        """发送MODE SELECT命令"""
        try:
            # 构造MODE SELECT(10) CDB
            cdb = bytes([
                0x55,        # MODE SELECT(10)
                0x00,        # 保留
                0x00,        # 页面代码
                0x00,        # 子页面代码
                0x00,        # 保留
                0x00,        # 保留
                0x00,        # 保留
                (len(mode_data) >> 8) & 0xFF,  # 参数列表长度高位
                len(mode_data) & 0xFF,         # 参数列表长度低位
                0x00         # 控制
            ])

            result = await self.scsi_interface.execute_scsi_command(
                device_path=None,
                cdb=cdb,
                data_direction=0,  # 出方向
                data_length=len(mode_data),
                timeout=30
            )

            return result

        except Exception as e:
            logger.error(f"发送MODE SELECT失败: {str(e)}")
            return {'success': False, 'error': str(e)}