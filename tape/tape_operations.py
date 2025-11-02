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
from config.settings import get_settings

logger = logging.getLogger(__name__)


class TapeOperations:
    """磁带操作类"""

    def __init__(self):
        self.settings = get_settings()
        self.scsi_interface = None
        self._initialized = False

    async def initialize(self, scsi_interface):
        """初始化磁带操作"""
        try:
            self.scsi_interface = scsi_interface
            self._initialized = True
            logger.info("磁带操作模块初始化完成")
        except Exception as e:
            logger.error(f"磁带操作模块初始化失败: {str(e)}")
            raise

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

            # 检查设备就绪状态
            if not await self._wait_for_tape_ready():
                logger.error("磁带设备未就绪")
                return False

            # 执行倒带操作
            if not await self._rewind():
                logger.error("磁带倒带失败")
                return False

            # 读取磁带标签（如果有）
            tape_label = await self._read_tape_label()
            if tape_label:
                logger.info(f"读取到磁带标签: {tape_label}")

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

    async def erase_tape(self) -> bool:
        """擦除磁带"""
        try:
            if not self._initialized:
                logger.error("磁带操作模块未初始化")
                return False

            logger.info("正在擦除磁带")

            # 倒带到开始
            if not await self._rewind():
                logger.error("倒带失败，无法擦除磁带")
                return False

            # 执行擦除命令
            success = await self._execute_erase_command()
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
            for _ in range(timeout):
                if await self.scsi_interface.test_unit_ready():
                    return True
                await asyncio.sleep(1)
            return False
        except Exception as e:
            logger.error(f"等待磁带就绪失败: {str(e)}")
            return False

    async def _rewind(self) -> bool:
        """倒带操作"""
        try:
            return await self.scsi_interface.rewind_tape()
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

    async def _execute_erase_command(self) -> bool:
        """执行擦除命令"""
        try:
            # 构造ERASE(6) SCSI命令
            cdb = bytes([0x19, 0x00, 0x00, 0x00, 0x00, 0x00])

            result = await self.scsi_interface.execute_scsi_command(
                device_path=None,
                cdb=cdb,
                timeout=300  # 擦除可能需要较长时间
            )

            return result['success']

        except Exception as e:
            logger.error(f"执行擦除命令异常: {str(e)}")
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

    async def _read_tape_label(self) -> Optional[Dict[str, Any]]:
        """读取磁带标签（从磁带头读取元数据）"""
        try:
            if not self.scsi_interface:
                logger.error("SCSI接口未初始化")
                return None
            
            import json
            
            # 倒带到开头
            await self.scsi_interface.rewind_tape()
            
            # 读取第一个数据块（256字节）
            block_size = 256
            result = await self.scsi_interface.read_tape_data(block_number=0, block_count=1, block_size=block_size)
            
            if not result or 'data' not in result:
                logger.warning("无法读取磁带标签")
                return None
            
            data = result['data']
            if len(data) < 16:
                logger.warning("磁带标签数据太短")
                return None
            
            # 解析头部信息
            header_length = int.from_bytes(data[0:4], 'big')
            version = data[4:8].decode('ascii', errors='ignore')
            
            if version != 'TAF1':
                logger.warning(f"不支持的磁带标签格式: {version}")
                return None
            
            # 提取元数据
            if header_length > 0 and header_length < len(data) - 16:
                metadata_bytes = data[8:8+header_length]
                metadata_json = metadata_bytes.decode('utf-8', errors='ignore')
                metadata = json.loads(metadata_json)
                logger.info(f"读取磁带标签成功: {metadata.get('tape_id')}")
                return metadata
            
            logger.warning("磁带标签元数据为空")
            return None
            
        except Exception as e:
            logger.error(f"读取磁带标签异常: {str(e)}")
            return None

    async def _write_tape_label(self, tape_info: Dict[str, Any]) -> bool:
        """写入磁带标签（磁带元数据到磁带头）"""
        try:
            import json
            if not self.scsi_interface:
                logger.error("SCSI接口未初始化")
                return False
            
            # 先尝试格式化磁带（新磁带需要格式化才能写入）
            logger.info("准备格式化磁带...")
            format_result = await self.scsi_interface.format_tape()
            if not format_result:
                logger.warning("磁带格式化失败，尝试继续写入标签")
            
            # 准备磁带元数据
            metadata = {
                "tape_id": tape_info.get("tape_id"),
                "label": tape_info.get("label"),
                "serial_number": tape_info.get("serial_number"),
                "created_date": tape_info.get("created_date"),
                "expiry_date": tape_info.get("expiry_date"),
                "system_version": "TAF_0.0.6"
            }
            
            # 将元数据序列化为JSON
            metadata_json = json.dumps(metadata, default=str)
            metadata_bytes = metadata_json.encode('utf-8')
            
            # 磁带标签应该写入磁带头部（第一个数据块）
            # 这里使用最小的块大小确保写入成功
            block_size = 256  # 256字节
            
            # 确保元数据不超过块大小
            if len(metadata_bytes) > block_size - 16:  # 保留16字节用于头部
                metadata_bytes = metadata_bytes[:block_size-16]
            
            # 添加头部信息（4字节长度 + 4字节版本 + 8字节预留）
            header = len(metadata_bytes).to_bytes(4, 'big')
            version = b'TAF1'  # TAF格式版本1
            
            # 填充到块大小
            label_data = header + version + metadata_bytes
            padding = block_size - len(label_data)
            if padding > 0:
                label_data += b'\x00' * padding
            
            # 倒带到开头
            await self.scsi_interface.rewind_tape()
            
            # 写入标签数据
            result = await self.scsi_interface.write_tape_data(data=label_data, block_number=0, block_size=block_size)
            
            if result:
                logger.info(f"磁带标签写入成功: {tape_info.get('tape_id')}")
                return True
            else:
                logger.error("磁带标签写入失败")
                return False
                
        except Exception as e:
            logger.error(f"写入磁带标签异常: {str(e)}")
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