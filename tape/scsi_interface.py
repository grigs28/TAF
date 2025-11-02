#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
SCSI接口模块
SCSI Interface Module
"""

import os
import sys
import platform
import logging
import re
import asyncio
from typing import List, Dict, Any, Optional
from ctypes import *
from pathlib import Path

# Platform-specific imports
if platform.system() == "Linux":
    import fcntl
    import struct

from config.settings import get_settings

logger = logging.getLogger(__name__)


class SCSIInterface:
    """SCSI接口类"""

    def __init__(self):
        self.system = platform.system()
        self.settings = get_settings()
        self.tape_devices = []
        self._initialized = False
        self._monitoring_task = None
        self._device_change_callback = None

    async def initialize(self):
        """初始化SCSI接口"""
        try:
            if self.system == "Windows":
                await self._init_windows_scsi()
            elif self.system == "Linux":
                await self._init_linux_scsi()
            else:
                raise OSError(f"不支持的操作系统: {self.system}")

            self._initialized = True
            logger.info(f"SCSI接口初始化完成 ({self.system})")

        except Exception as e:
            logger.error(f"SCSI接口初始化失败: {str(e)}")
            raise

    async def _init_windows_scsi(self):
        """初始化Windows SCSI接口"""
        try:
            # Windows SPTI (SCSI Pass Through Interface)
            import ctypes
            from ctypes import wintypes

            # 定义缺失的类型
            if not hasattr(wintypes, 'UCHAR'):
                wintypes.UCHAR = ctypes.c_ubyte
            if not hasattr(wintypes, 'ULONG_PTR'):
                wintypes.ULONG_PTR = ctypes.c_ulong
            if not hasattr(wintypes, 'ULONG'):
                wintypes.ULONG = ctypes.c_ulong

            # 定义Windows API结构
            class SCSI_PASS_THROUGH(Structure):
                _fields_ = [
                    ("Length", wintypes.USHORT),
                    ("ScsiStatus", wintypes.UCHAR),
                    ("PathId", wintypes.UCHAR),
                    ("TargetId", wintypes.UCHAR),
                    ("Lun", wintypes.UCHAR),
                    ("CdbLength", wintypes.UCHAR),
                    ("SenseInfoLength", wintypes.UCHAR),
                    ("DataIn", wintypes.UCHAR),
                    ("DataTransferLength", wintypes.ULONG),
                    ("TimeOutValue", wintypes.ULONG),
                    ("DataBufferOffset", wintypes.ULONG_PTR),
                    ("SenseInfoOffset", wintypes.ULONG),
                    ("Cdb", wintypes.UCHAR * 16)
                ]

            class SCSI_PASS_THROUGH_WITH_BUFFERS(Structure):
                _fields_ = [
                    ("Spt", SCSI_PASS_THROUGH),
                    ("Sense", wintypes.UCHAR * 32),
                    ("Data", wintypes.UCHAR * 4096)
                ]

            # 保存结构体类型供后续使用
            self.SCSI_PASS_THROUGH_WITH_BUFFERS = SCSI_PASS_THROUGH_WITH_BUFFERS

            # 加载kernel32.dll
            self.kernel32 = windll.kernel32
            self.create_file = self.kernel32.CreateFileW
            self.device_io_control = self.kernel32.DeviceIoControl
            
            # IOCTL控制码
            self.IOCTL_SCSI_PASS_THROUGH_DIRECT = 0x4D014  # DIRECT版本
            self.IOCTL_SCSI_PASS_THROUGH = 0x4D002  # 使用缓冲区的版本

        except Exception as e:
            logger.error(f"Windows SCSI接口初始化失败: {str(e)}")
            raise

    async def _init_linux_scsi(self):
        """初始化Linux SCSI接口"""
        try:
            # Linux sg_io接口已在文件顶部导入

            # SG_IO 命令定义
            self.SG_IO = 0x2285

            # 定义SG_IO结构
            class sg_io_hdr(Structure):
                _fields_ = [
                    ("interface_id", c_int),
                    ("dxfer_direction", c_int),
                    ("cmd_len", c_ubyte),
                    ("mx_sb_len", c_ubyte),
                    ("iovec_count", c_ushort),
                    ("dxfer_len", c_uint),
                    ("dxferp", c_void_p),
                    ("cmdp", c_void_p),
                    ("sbp", c_void_p),
                    ("timeout", c_uint),
                    ("flags", c_uint),
                    ("pack_id", c_uint),
                    ("usr_ptr", c_void_p),
                    ("status", c_ubyte),
                    ("masked_status", c_ubyte),
                    ("msg_status", c_ubyte),
                    ("sb_len_wr", c_ubyte),
                    ("host_status", c_ushort),
                    ("driver_status", c_ushort),
                    ("resid", c_uint),
                    ("duration", c_uint),
                    ("info", c_uint)
                ]

            self.sg_io_hdr = sg_io_hdr

        except Exception as e:
            logger.error(f"Linux SCSI接口初始化失败: {str(e)}")
            raise

    async def scan_tape_devices(self) -> List[Dict[str, Any]]:
        """扫描磁带设备"""
        devices = []

        try:
            if self.system == "Windows":
                devices = await self._scan_windows_tape_devices()
            elif self.system == "Linux":
                devices = await self._scan_linux_tape_devices()

            self.tape_devices = devices
            logger.info(f"扫描到 {len(devices)} 个磁带设备")

        except Exception as e:
            logger.error(f"扫描磁带设备失败: {str(e)}")

        return devices

    async def _scan_windows_tape_devices(self) -> List[Dict[str, Any]]:
        """扫描Windows磁带设备"""
        devices = []

        try:
            # 首先通过WMI查询磁带设备
            try:
                import wmi
                c = wmi.WMI()
                for tape in c.Win32_TapeDrive():
                    # 获取详细的设备信息
                    device_info = {
                        'path': tape.DeviceID,
                        'type': 'SCSI',
                        'vendor': getattr(tape, 'Manufacturer', 'Unknown'),
                        'model': getattr(tape, 'Name', 'Unknown'),
                        'serial': getattr(tape, 'SerialNumber', 'Unknown'),
                        'status': 'online',
                        'scsi_bus': getattr(tape, 'SCSIBus', 'Unknown'),
                        'scsi_target_id': getattr(tape, 'SCSITargetId', 'Unknown'),
                        'scsi_lun': getattr(tape, 'SCSILogicalUnit', 'Unknown')
                    }

                    # 检查是否为IBM LTO磁带机
                    if 'IBM' in device_info['vendor'].upper() and 'ULT3580' in device_info['model'].upper():
                        device_info.update({
                            'is_ibm_lto': True,
                            'lto_generation': self._extract_lto_generation(device_info['model']),
                            'supports_worm': True,
                            'supports_encryption': True,
                            'native_capacity': self._get_lto_capacity(device_info['model'])
                        })

                    devices.append(device_info)
                    logger.info(f"发现磁带设备: {device_info['vendor']} {device_info['model']}")

            except ImportError:
                logger.warning("WMI模块不可用，使用基本扫描方法")

            # 如果WMI不可用，检查可用的磁带驱动器盘符
            if not devices:
                # 检查常见磁带设备路径
                tape_paths = [
                    "\\TAPE0",
                    "\\TAPE1",
                    "\\TAPE2",
                    "\\\\.\\TAPE0",
                    "\\\\.\\TAPE1",
                    "\\\\.\\TAPE2"
                ]

                for tape_path in tape_paths:
                    if await self._test_tape_device_access(tape_path):
                        # 尝试获取设备信息
                        tape_info = await self.get_tape_info(tape_path)
                        device_info = {
                            'path': tape_path,
                            'type': 'SCSI',
                            'vendor': tape_info.get('vendor', 'Unknown') if tape_info else 'Unknown',
                            'model': tape_info.get('model', 'Unknown') if tape_info else 'Tape Drive',
                            'serial': tape_info.get('serial', 'Unknown') if tape_info else 'Unknown',
                            'status': 'online'
                        }

                        if tape_info:
                            device_info.update(tape_info)

                        devices.append(device_info)

        except Exception as e:
            logger.error(f"扫描Windows磁带设备失败: {str(e)}")

        return devices

    async def _scan_linux_tape_devices(self) -> List[Dict[str, Any]]:
        """扫描Linux磁带设备"""
        devices = []

        try:
            # 扫描 /dev/nst* 和 /dev/st* 设备
            base_path = Path("/dev")
            tape_pattern = ["nst*", "st*"]

            for pattern in tape_pattern:
                for device_path in base_path.glob(pattern):
                    if device_path.is_char_device():
                        # 获取设备信息
                        vendor, model, serial = await self._get_linux_tape_info(str(device_path))

                        device_info = {
                            'path': str(device_path),
                            'type': 'SCSI',
                            'vendor': vendor,
                            'model': model,
                            'serial': serial,
                            'status': 'online'
                        }

                        # 检查是否为IBM LTO磁带机
                        if 'IBM' in vendor.upper() and 'ULT3580' in model.upper():
                            device_info.update({
                                'is_ibm_lto': True,
                                'lto_generation': self._extract_lto_generation(model),
                                'supports_worm': True,
                                'supports_encryption': True,
                                'native_capacity': self._get_lto_capacity(model)
                            })

                        devices.append(device_info)
                        logger.info(f"发现磁带设备: {device_path} - {vendor} {model}")

        except Exception as e:
            logger.error(f"扫描Linux磁带设备失败: {str(e)}")

        return devices

    async def _get_linux_tape_info(self, device_path: str) -> tuple:
        """获取Linux磁带设备信息"""
        try:
            # 通过 /sys/class/scsi_tape 获取信息
            device_name = Path(device_path).name
            sys_path = Path(f"/sys/class/scsi_tape/{device_name}/device")

            vendor = "Unknown"
            model = "Unknown"
            serial = "Unknown"

            if sys_path.exists():
                try:
                    with open(sys_path / "vendor", "r") as f:
                        vendor = f.read().strip()
                except:
                    pass

                try:
                    with open(sys_path / "model", "r") as f:
                        model = f.read().strip()
                except:
                    pass

                try:
                    with open(sys_path / "serial", "r") as f:
                        serial = f.read().strip()
                except:
                    pass

            return vendor, model, serial

        except Exception as e:
            logger.error(f"获取Linux磁带设备信息失败: {str(e)}")
            return "Unknown", "Unknown", "Unknown"

    async def execute_scsi_command(self, device_path: str, cdb: bytes,
                                 data_direction: int = 0, data_length: int = 0,
                                 data: bytes = b'',
                                 timeout: int = 30) -> Dict[str, Any]:
        """执行SCSI命令"""
        try:
            if self.system == "Windows":
                return await self._execute_windows_scsi(device_path, cdb, data_direction, data_length, data, timeout)
            elif self.system == "Linux":
                return await self._execute_linux_scsi(device_path, cdb, data_direction, data_length, data, timeout)

        except Exception as e:
            logger.error(f"执行SCSI命令失败: {str(e)}")
            return {'success': False, 'error': str(e)}

    async def _execute_windows_scsi(self, device_path: str, cdb: bytes,
                                  data_direction: int, data_length: int,
                                  data: bytes, timeout: int) -> Dict[str, Any]:
        """执行Windows SCSI命令"""
        try:
            # Windows SPTI实现
            handle = self.create_file(
                device_path,
                0x80000000 | 0x40000000,  # GENERIC_READ | GENERIC_WRITE
                0,
                None,
                3,           # OPEN_EXISTING
                0x80,        # FILE_ATTRIBUTE_NORMAL
                None
            )

            if handle == -1:  # INVALID_HANDLE_VALUE
                return {'success': False, 'error': '无法打开设备'}

            try:
                # 构造完整的SCSI_PASS_THROUGH_WITH_BUFFERS结构
                sptwb = self.SCSI_PASS_THROUGH_WITH_BUFFERS()
                
                # 计算偏移量
                sense_offset = sizeof(sptwb.Spt)
                data_offset = sense_offset + sizeof(sptwb.Sense)
                
                # 填充SCSI_PASS_THROUGH字段
                sptwb.Spt.Length = sizeof(sptwb.Spt)
                sptwb.Spt.ScsiStatus = 0
                sptwb.Spt.PathId = 0
                sptwb.Spt.TargetId = 0
                sptwb.Spt.Lun = 0
                sptwb.Spt.CdbLength = len(cdb)
                sptwb.Spt.SenseInfoLength = 32
                sptwb.Spt.DataIn = data_direction  # 1=IN, 0=OUT
                sptwb.Spt.DataTransferLength = data_length
                sptwb.Spt.TimeOutValue = timeout
                sptwb.Spt.DataBufferOffset = data_offset
                sptwb.Spt.SenseInfoOffset = sense_offset
                
                # 复制CDB命令
                for i, byte in enumerate(cdb):
                    if i < 16:
                        sptwb.Spt.Cdb[i] = byte
                
                # 如果数据方向是OUT，复制数据到缓冲区
                if data_direction == 0 and data and len(data) > 0:
                    if len(data) > len(sptwb.Data):
                        return {'success': False, 'error': f'数据长度 {len(data)} 超过缓冲区大小 {len(sptwb.Data)}'}
                    for i, byte in enumerate(data):
                        sptwb.Data[i] = byte
                
                # 调用DeviceIoControl - 使用IOCTL_SCSI_PASS_THROUGH因为有内置缓冲区
                result = self.device_io_control(
                    handle,
                    self.IOCTL_SCSI_PASS_THROUGH,
                    byref(sptwb),
                    sizeof(sptwb),
                    byref(sptwb),
                    sizeof(sptwb),
                    None,
                    None
                )
                
                if result:
                    if sptwb.Spt.ScsiStatus == 0:
                        # 成功，返回数据
                        if data_direction == 1 and data_length > 0:
                            read_data = bytes(sptwb.Data[:data_length])
                            return {'success': True, 'data': read_data}
                        else:
                            return {'success': True, 'data': b''}
                    else:
                        # SCSI错误
                        sense = bytes(sptwb.Sense[:sptwb.Spt.SenseInfoLength])
                        return {
                            'success': False,
                            'error': f'SCSI错误: 状态={sptwb.Spt.ScsiStatus}',
                            'sense_data': sense.hex()
                        }
                else:
                    error_code = self.kernel32.GetLastError()
                    return {'success': False, 'error': f'DeviceIoControl失败: 错误代码={error_code}'}
                    
            finally:
                self.kernel32.CloseHandle(handle)

        except Exception as e:
            logger.error(f"Windows SCSI命令执行异常: {str(e)}")
            return {'success': False, 'error': str(e)}

    async def _execute_linux_scsi(self, device_path: str, cdb: bytes,
                                data_direction: int, data_length: int,
                                data: bytes, timeout: int) -> Dict[str, Any]:
        """执行Linux SCSI命令"""
        try:
            with open(device_path, 'rb+') as fd:
                # 构造SG_IO请求
                hdr = self.sg_io_hdr()
                hdr.interface_id = ord('S')
                hdr.dxfer_direction = data_direction
                hdr.cmd_len = len(cdb)
                hdr.mx_sb_len = 32
                hdr.dxfer_len = data_length
                hdr.timeout = timeout * 1000  # 毫秒

                # 分配缓冲区
                cdb_buffer = create_string_buffer(cdb)
                sense_buffer = create_string_buffer(32)
                data_buffer = create_string_buffer(data_length) if data_length > 0 else None

                # 如果数据方向是OUT，复制数据到缓冲区
                if data_direction == 0 and data and len(data) > 0 and data_buffer:
                    for i, byte in enumerate(data):
                        if i < data_length:
                            data_buffer[i] = byte

                hdr.cmdp = cast(cdb_buffer, c_void_p)
                hdr.sbp = cast(sense_buffer, c_void_p)
                if data_buffer:
                    hdr.dxferp = cast(data_buffer, c_void_p)

                # 执行SG_IO命令
                fcntl.ioctl(fd, self.SG_IO, byref(hdr))

                # 检查结果
                if hdr.status == 0:
                    read_data = data_buffer.raw[:data_length] if data_buffer else b''
                    return {'success': True, 'data': read_data}
                else:
                    return {
                        'success': False,
                        'error': f'SCSI错误: 状态={hdr.status}, 主机状态={hdr.host_status}',
                        'sense_data': sense_buffer.raw[:hdr.sb_len_wr]
                    }

        except Exception as e:
            return {'success': False, 'error': str(e)}

    async def get_tape_info(self, device_path: str = None) -> Optional[Dict[str, Any]]:
        """获取磁带信息"""
        try:
            if not device_path and self.tape_devices:
                device_path = self.tape_devices[0]['path']

            if not device_path:
                return None

            # 发送INQUIRY命令获取设备信息
            cdb = bytes([0x12, 0x00, 0x00, 0x00, 36, 0x00])  # INQUIRY命令
            result = await self.execute_scsi_command(device_path, cdb, data_direction=1, data_length=36)

            if result['success']:
                data = result['data']
                if len(data) >= 36:
                    vendor = data[8:16].decode('ascii', errors='ignore').strip()
                    model = data[16:32].decode('ascii', errors='ignore').strip()
                    revision = data[32:36].decode('ascii', errors='ignore').strip()

                    return {
                        'vendor': vendor,
                        'model': model,
                        'revision': revision,
                        'device_type': data[0] & 0x1F,
                        'device_modifier': (data[0] >> 6) & 0x07
                    }

            return None

        except Exception as e:
            logger.error(f"获取磁带信息失败: {str(e)}")
            return None

    async def test_unit_ready(self, device_path: str = None) -> bool:
        """测试设备就绪状态"""
        try:
            if not device_path and self.tape_devices:
                device_path = self.tape_devices[0]['path']

            if not device_path:
                return False

            # 发送TEST UNIT READY命令
            cdb = bytes([0x00, 0x00, 0x00, 0x00, 0x00, 0x00])
            result = await self.execute_scsi_command(device_path, cdb)

            return result['success']

        except Exception as e:
            logger.error(f"测试设备就绪状态失败: {str(e)}")
            return False

    async def rewind_tape(self, device_path: str = None) -> bool:
        """倒带"""
        try:
            if not device_path and self.tape_devices:
                device_path = self.tape_devices[0]['path']

            if not device_path:
                return False

            # 发送REWIND命令
            cdb = bytes([0x01, 0x00, 0x00, 0x00, 0x00, 0x00])
            result = await self.execute_scsi_command(device_path, cdb, timeout=300)

            return result['success']

        except Exception as e:
            logger.error(f"磁带倒带失败: {str(e)}")
            return False

    async def format_tape(self, device_path: str = None, format_type: int = 0) -> bool:
        """格式化磁带 - FORMAT MEDIUM命令"""
        try:
            if not device_path and self.tape_devices:
                device_path = self.tape_devices[0]['path']

            if not device_path:
                return False

            # FORMAT MEDIUM命令 (0x04)
            # Byte 1: Format Code (0=default, 1=LTO format)
            cdb = bytes([0x04, format_type & 0xFF, 0x00, 0x00, 0x00, 0x00])
            result = await self.execute_scsi_command(device_path, cdb, timeout=600)  # 格式化可能需要更长时间

            return result['success']

        except Exception as e:
            logger.error(f"磁带格式化失败: {str(e)}")
            return False

    async def erase_tape(self, device_path: str = None, erase_type: int = 0) -> bool:
        """擦除磁带 - ERASE命令"""
        try:
            if not device_path and self.tape_devices:
                device_path = self.tape_devices[0]['path']

            if not device_path:
                return False

            # ERASE命令 (0x19)
            # Byte 1: Erase Type (0=long, 1=short)
            cdb = bytes([0x19, erase_type & 0xFF, 0x00, 0x00, 0x00, 0x00])
            result = await self.execute_scsi_command(device_path, cdb, timeout=600)

            return result['success']

        except Exception as e:
            logger.error(f"磁带擦除失败: {str(e)}")
            return False

    async def load_unload(self, device_path: str = None, load: bool = True) -> bool:
        """加载/卸载磁带 - LOAD UNLOAD命令"""
        try:
            if not device_path and self.tape_devices:
                device_path = self.tape_devices[0]['path']

            if not device_path:
                return False

            # LOAD UNLOAD命令 (0x1B)
            # Byte 4: Bit 0 = LOAD (1), UNLOAD (0)
            cdb = bytes([0x1B, 0x00, 0x00, 0x00, 0x01 if load else 0x00, 0x00])
            result = await self.execute_scsi_command(device_path, cdb, timeout=300)

            return result['success']

        except Exception as e:
            logger.error(f"磁带加载/卸载失败: {str(e)}")
            return False

    async def space_blocks(self, device_path: str = None, blocks: int = 1, direction: str = "forward") -> bool:
        """按块定位 - SPACE命令"""
        try:
            if not device_path and self.tape_devices:
                device_path = self.tape_devices[0]['path']

            if not device_path:
                return False

            # SPACE命令 (0x11)
            # Byte 1: Code (1=blocks, 2=filemarks, 3=end of data)
            # Byte 4-6: Count
            direction_code = 0x01  # forward
            if direction == "reverse":
                blocks = -blocks
                direction_code = 0x02  # reverse

            cdb = bytes([
                0x11,  # SPACE
                direction_code,
                0x00,
                ((blocks >> 16) & 0xFF),
                ((blocks >> 8) & 0xFF),
                (blocks & 0xFF)
            ])
            result = await self.execute_scsi_command(device_path, cdb, timeout=300)

            return result['success']

        except Exception as e:
            logger.error(f"磁带定位失败: {str(e)}")
            return False

    async def write_filemarks(self, device_path: str = None, count: int = 1) -> bool:
        """写入文件标记 - WRITE FILEMARKS命令"""
        try:
            if not device_path and self.tape_devices:
                device_path = self.tape_devices[0]['path']

            if not device_path:
                return False

            # WRITE FILEMARKS命令 (0x10)
            # Byte 1: Immediate bit
            # Byte 4-6: Filemark count
            cdb = bytes([
                0x10,  # WRITE FILEMARKS
                0x00,
                0x00,
                ((count >> 16) & 0xFF),
                ((count >> 8) & 0xFF),
                (count & 0xFF)
            ])
            result = await self.execute_scsi_command(device_path, cdb, timeout=300)

            return result['success']

        except Exception as e:
            logger.error(f"写入文件标记失败: {str(e)}")
            return False

    async def set_mark(self, device_path: str = None, mark_type: int = 0) -> bool:
        """设置磁带标记 - SET MARK命令"""
        try:
            if not device_path and self.tape_devices:
                device_path = self.tape_devices[0]['path']

            if not device_path:
                return False

            # SET MARK命令 (0x3B)
            # Byte 1: Mark Type (0=BOM, 1=EOM)
            cdb = bytes([0x3B, mark_type & 0xFF, 0x00, 0x00, 0x00, 0x00])
            result = await self.execute_scsi_command(device_path, cdb, timeout=30)

            return result['success']

        except Exception as e:
            logger.error(f"设置磁带标记失败: {str(e)}")
            return False

    async def health_check(self) -> bool:
        """SCSI接口健康检查"""
        try:
            if not self.tape_devices:
                return False

            for device in self.tape_devices:
                if not await self.test_unit_ready(device['path']):
                    logger.warning(f"磁带设备 {device['path']} 未就绪")
                    return False

            return True

        except Exception as e:
            logger.error(f"SCSI接口健康检查失败: {str(e)}")
            return False

    async def start_device_monitoring(self, interval: int = 60, callback=None):
        """启动设备状态监控"""
        try:
            self._device_change_callback = callback
            self._monitoring_task = asyncio.create_task(
                self._monitoring_loop(interval)
            )
            logger.info(f"启动设备监控任务，间隔: {interval}秒")
        except Exception as e:
            logger.error(f"启动设备监控失败: {str(e)}")

    async def _monitoring_loop(self, interval: int):
        """设备监控循环"""
        while self._initialized:
            try:
                devices = await self.scan_tape_devices()
                
                # 检测设备状态变化
                current_paths = {d['path'] for d in devices}
                previous_paths = {d['path'] for d in self.tape_devices}
                
                # 新设备连接
                new_devices = current_paths - previous_paths
                if new_devices:
                    for path in new_devices:
                        logger.info(f"检测到新设备: {path}")
                        if self._device_change_callback:
                            await self._device_change_callback('connected', path)
                
                # 设备断开
                removed_devices = previous_paths - current_paths
                if removed_devices:
                    for path in removed_devices:
                        logger.warning(f"设备断开连接: {path}")
                        if self._device_change_callback:
                            await self._device_change_callback('disconnected', path)
                
                self.tape_devices = devices
                await asyncio.sleep(interval)
                
            except asyncio.CancelledError:
                break
            except Exception as e:
                logger.error(f"设备监控异常: {str(e)}")
                await asyncio.sleep(interval)

    async def close(self):
        """关闭SCSI接口"""
        try:
            self._initialized = False

            # 停止监控任务
            if self._monitoring_task:
                self._monitoring_task.cancel()
                try:
                    await self._monitoring_task
                except asyncio.CancelledError:
                    pass

            logger.info("SCSI接口已关闭")

        except Exception as e:
            logger.error(f"关闭SCSI接口时发生错误: {str(e)}")

    def _extract_lto_generation(self, model: str) -> int:
        """从型号中提取LTO代数"""
        try:
            model_upper = model.upper()
            if 'HH9' in model_upper or 'LTO-9' in model_upper or 'ULTRIUM-HH9' in model_upper:
                return 9
            elif 'HH8' in model_upper or 'LTO-8' in model_upper or 'ULTRIUM-HH8' in model_upper:
                return 8
            elif 'HH7' in model_upper or 'LTO-7' in model_upper or 'ULTRIUM-HH7' in model_upper:
                return 7
            elif 'HH6' in model_upper or 'LTO-6' in model_upper or 'ULTRIUM-HH6' in model_upper:
                return 6
            elif 'HH5' in model_upper or 'LTO-5' in model_upper or 'ULTRIUM-HH5' in model_upper:
                return 5
            else:
                # 尝试从型号字符串中提取数字
                match = re.search(r'HH(\d+)', model_upper)
                if match:
                    return int(match.group(1))
                return 0
        except:
            return 0

    def _get_lto_capacity(self, model: str) -> int:
        """获取LTO磁带机容量（字节）"""
        lto_gen = self._extract_lto_generation(model)

        # LTO标准容量（未压缩）
        lto_capacities = {
            9: 18 * 1024 * 1024 * 1024 * 1024,  # 18TB
            8: 12 * 1024 * 1024 * 1024 * 1024,  # 12TB
            7: 6 * 1024 * 1024 * 1024 * 1024,   # 6TB
            6: 2.5 * 1024 * 1024 * 1024 * 1024, # 2.5TB
            5: 1.5 * 1024 * 1024 * 1024 * 1024, # 1.5TB
        }

        return lto_capacities.get(lto_gen, 0)

    async def _test_tape_device_access(self, device_path: str) -> bool:
        """测试磁带设备访问"""
        try:
            if self.system == "Windows":
                # Windows设备访问测试
                handle = self.create_file(
                    device_path,
                    0x80000000 | 0x40000000,  # GENERIC_READ | GENERIC_WRITE
                    0,
                    None,
                    3,           # OPEN_EXISTING
                    0x80,        # FILE_ATTRIBUTE_NORMAL
                    None
                )

                if handle != -1:  # INVALID_HANDLE_VALUE
                    self.kernel32.CloseHandle(handle)
                    return True

            elif self.system == "Linux":
                # Linux设备访问测试
                if os.path.exists(device_path):
                    with open(device_path, 'rb') as f:
                        # 尝试读取MTIO状态
                        try:
                            # MTGETSTATUS ioctl
                            fcntl.ioctl(f, 0x801c6d01, b'\x00' * 20)
                            return True
                        except:
                            # 即使ioctl失败，设备存在就认为可访问
                            return True

        except Exception as e:
            logger.debug(f"测试设备访问失败 {device_path}: {str(e)}")

        return False

    async def send_ibm_specific_command(self, device_path: str, command_type: str,
                                      parameters: Dict[str, Any] = None) -> Dict[str, Any]:
        """发送IBM特定的SCSI命令"""
        try:
            if command_type == "log_sense":
                return await self._ibm_log_sense(device_path, parameters or {})
            elif command_type == "mode_sense":
                return await self._ibm_mode_sense(device_path, parameters or {})
            elif command_type == "inquiry_vpd":
                return await self._ibm_inquiry_vpd(device_path, parameters or {})
            elif command_type == "receive_diagnostic":
                return await self._ibm_receive_diagnostic(device_path, parameters or {})
            else:
                return {'success': False, 'error': f'不支持的IBM命令类型: {command_type}'}

        except Exception as e:
            logger.error(f"发送IBM特定命令失败: {str(e)}")
            return {'success': False, 'error': str(e)}

    async def _ibm_log_sense(self, device_path: str, params: Dict[str, Any]) -> Dict[str, Any]:
        """IBM LOG SENSE命令 - 获取详细日志信息"""
        try:
            page_code = params.get('page_code', 0x00)
            subpage_code = params.get('subpage_code', 0x00)

            # 构造LOG SENSE CDB
            cdb = bytes([
                0x4D,        # LOG SENSE
                0x00,        # 保留
                page_code,   # 页面代码
                subpage_code, # 子页面代码
                0x00,        # PC位
                0x00,        # 保留
                0x00,        # 参数指针长度
                0x00,        # 参数指针
                0x00,        # 分配长度高位
                252          # 分配长度低位
            ])

            result = await self.execute_scsi_command(device_path, cdb, data_direction=1, data_length=252)

            if result['success']:
                log_data = result['data']
                return {
                    'success': True,
                    'page_code': page_code,
                    'log_data': log_data.hex(),
                    'data_length': len(log_data)
                }
            else:
                return result

        except Exception as e:
            return {'success': False, 'error': str(e)}

    async def _ibm_mode_sense(self, device_path: str, params: Dict[str, Any]) -> Dict[str, Any]:
        """IBM MODE SENSE命令 - 获取模式参数"""
        try:
            page_code = params.get('page_code', 0x3F)  # 所有页面
            subpage_code = params.get('subpage_code', 0x00)

            # 构造MODE SENSE(10) CDB
            cdb = bytes([
                0x5A,        # MODE SENSE(10)
                0x00,        # 保留
                page_code,   # 页面代码
                subpage_code, # 子页面代码
                0x00,        # 保留
                0x00,        # 保留
                0x00,        # 保留
                0x00,        # 参数列表长度高位
                252,         # 参数列表长度低位
                0x00         # 控制
            ])

            result = await self.execute_scsi_command(device_path, cdb, data_direction=1, data_length=252)

            if result['success']:
                mode_data = result['data']
                return {
                    'success': True,
                    'page_code': page_code,
                    'mode_data': mode_data.hex(),
                    'data_length': len(mode_data)
                }
            else:
                return result

        except Exception as e:
            return {'success': False, 'error': str(e)}

    async def _ibm_inquiry_vpd(self, device_path: str, params: Dict[str, Any]) -> Dict[str, Any]:
        """IBM INQUIRY VPD命令 - 获取产品特定数据"""
        try:
            page_code = params.get('page_code', 0x00)

            # 构造INQUIRY CDB with VPD
            cdb = bytes([
                0x12,        # INQUIRY
                0x01,        # EVPD=1 (启用VPD)
                page_code,   # 页面代码
                0x00,        # 保留
                0x00,        # 分配长度高位
                252,         # 分配长度低位
                0x00         # 控制
            ])

            result = await self.execute_scsi_command(device_path, cdb, data_direction=1, data_length=252)

            if result['success']:
                vpd_data = result['data']
                return {
                    'success': True,
                    'page_code': page_code,
                    'vpd_data': vpd_data.hex(),
                    'data_length': len(vpd_data)
                }
            else:
                return result

        except Exception as e:
            return {'success': False, 'error': str(e)}

    async def _ibm_receive_diagnostic(self, device_path: str, params: Dict[str, Any]) -> Dict[str, Any]:
        """IBM RECEIVE DIAGNOSTIC RESULTS命令"""
        try:
            page_code = params.get('page_code', 0x00)

            # 构造RECEIVE DIAGNOSTIC RESULTS CDB
            cdb = bytes([
                0x1C,        # RECEIVE DIAGNOSTIC RESULTS
                0x01,        # PCV=1 (页面代码有效)
                page_code,   # 页面代码
                0x00,        # 保留
                0x00,        # 分配长度高位
                252,         # 分配长度低位
                0x00         # 控制
            ])

            result = await self.execute_scsi_command(device_path, cdb, data_direction=1, data_length=252)

            if result['success']:
                diagnostic_data = result['data']
                return {
                    'success': True,
                    'page_code': page_code,
                    'diagnostic_data': diagnostic_data.hex(),
                    'data_length': len(diagnostic_data)
                }
            else:
                return result

        except Exception as e:
            return {'success': False, 'error': str(e)}

    async def get_tape_position(self, device_path: str = None) -> Dict[str, Any]:
        """获取磁带位置信息"""
        try:
            if not device_path and self.tape_devices:
                device_path = self.tape_devices[0]['path']

            if not device_path:
                return {'success': False, 'error': '没有指定设备路径'}

            # 使用READ POSITION命令
            cdb = bytes([0x34, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x20])
            result = await self.execute_scsi_command(device_path, cdb, data_direction=1, data_length=32)

            if result['success']:
                data = result['data']
                if len(data) >= 20:
                    flags = data[0]
                    partition = (data[1] << 8) | data[2]
                    file_number = (data[4] << 24) | (data[5] << 16) | (data[6] << 8) | data[7]
                    set_number = (data[8] << 24) | (data[9] << 16) | (data[10] << 8) | data[11]
                    end_of_data = (data[12] << 24) | (data[13] << 16) | (data[14] << 8) | data[15]
                    block_in_buffer = (data[16] << 24) | (data[17] << 16) | (data[18] << 8) | data[19]

                    return {
                        'success': True,
                        'flags': flags,
                        'partition': partition,
                        'file_number': file_number,
                        'set_number': set_number,
                        'end_of_data': end_of_data,
                        'block_in_buffer': block_in_buffer,
                        'is_bop': bool(flags & 0x04),  # Beginning of Partition
                        'is_eop': bool(flags & 0x02),  # End of Partition
                        'is_bom': bool(flags & 0x01),  # Beginning of Medium
                    }
                else:
                    return {'success': False, 'error': '返回数据长度不足'}
            else:
                return result

        except Exception as e:
            logger.error(f"获取磁带位置失败: {str(e)}")
            return {'success': False, 'error': str(e)}

    async def request_sense(self, device_path: str = None) -> Dict[str, Any]:
        """请求Sense数据 - 获取详细错误信息"""
        try:
            if not device_path and self.tape_devices:
                device_path = self.tape_devices[0]['path']

            if not device_path:
                return {'success': False, 'error': '没有指定设备路径'}

            # 构造REQUEST SENSE命令
            cdb = bytes([0x03, 0x00, 0x00, 0x00, 252, 0x00])
            result = await self.execute_scsi_command(device_path, cdb, data_direction=1, data_length=252)

            if result['success']:
                sense_data = result['data']
                if len(sense_data) >= 18:
                    response_code = sense_data[0] & 0x7F
                    sense_key = sense_data[2] & 0x0F
                    asc = sense_data[12]
                    ascq = sense_data[13]

                    return {
                        'success': True,
                        'response_code': response_code,
                        'sense_key': sense_key,
                        'asc': asc,
                        'ascq': ascq,
                        'sense_data': sense_data.hex(),
                        'data_length': len(sense_data)
                    }
                else:
                    return {'success': False, 'error': 'Sense数据长度不足'}
            else:
                return result

        except Exception as e:
            logger.error(f"请求Sense数据失败: {str(e)}")
            return {'success': False, 'error': str(e)}

    async def read_tape_data(self, device_path: str = None, block_number: int = 0,
                            block_count: int = 1, block_size: int = 512) -> Dict[str, Any]:
        """读取磁带数据 - READ(16)命令"""
        try:
            if not device_path and self.tape_devices:
                device_path = self.tape_devices[0]['path']

            if not device_path:
                return {'success': False, 'error': '没有指定设备路径'}

            # READ(16)命令 - 支持64位LBA
            cdb = bytes([
                0x88,  # READ(16)
                0x00,  # RDPROTECT, DPO, FUA
                ((block_number >> 56) & 0xFF),
                ((block_number >> 48) & 0xFF),
                ((block_number >> 40) & 0xFF),
                ((block_number >> 32) & 0xFF),
                ((block_number >> 24) & 0xFF),
                ((block_number >> 16) & 0xFF),
                ((block_number >> 8) & 0xFF),
                (block_number & 0xFF),
                ((block_count >> 32) & 0xFF),
                ((block_count >> 24) & 0xFF),
                ((block_count >> 16) & 0xFF),
                ((block_count >> 8) & 0xFF),
                (block_count & 0xFF),
                0x00   # 控制
            ])

            data_length = block_count * block_size
            return await self.execute_scsi_command(
                device_path, cdb,
                data_direction=1,  # IN
                data_length=data_length,
                timeout=300
            )

        except Exception as e:
            logger.error(f"读取磁带数据失败: {str(e)}")
            return {'success': False, 'error': str(e)}

    async def write_tape_data(self, device_path: str = None, data: bytes = b'',
                            block_number: int = 0, block_size: int = 512) -> Dict[str, Any]:
        """写入磁带数据 - WRITE(16)命令"""
        try:
            if not device_path and self.tape_devices:
                device_path = self.tape_devices[0]['path']

            if not device_path:
                return {'success': False, 'error': '没有指定设备路径'}

            # 计算块数
            block_count = (len(data) + block_size - 1) // block_size

            # WRITE(16)命令 - 支持64位LBA
            cdb = bytes([
                0x8A,  # WRITE(16)
                0x00,  # RDPROTECT, DPO, FUA
                ((block_number >> 56) & 0xFF),
                ((block_number >> 48) & 0xFF),
                ((block_number >> 40) & 0xFF),
                ((block_number >> 32) & 0xFF),
                ((block_number >> 24) & 0xFF),
                ((block_number >> 16) & 0xFF),
                ((block_number >> 8) & 0xFF),
                (block_number & 0xFF),
                ((block_count >> 32) & 0xFF),
                ((block_count >> 24) & 0xFF),
                ((block_count >> 16) & 0xFF),
                ((block_count >> 8) & 0xFF),
                (block_count & 0xFF),
                0x00   # 控制
            ])

            return await self.execute_scsi_command(
                device_path, cdb,
                data_direction=0,  # OUT
                data_length=len(data),
                data=data,
                timeout=300
            )

        except Exception as e:
            logger.error(f"写入磁带数据失败: {str(e)}")
            return {'success': False, 'error': str(e)}

    async def execute_scsi_command_with_retry(self, device_path: str, cdb: bytes,
                                            data_direction: int = 0, data_length: int = 0,
                                            data: bytes = b'',
                                            timeout: int = 30, max_retries: int = 3) -> Dict[str, Any]:
        """执行SCSI命令（带重试机制）"""
        last_error = None

        for attempt in range(max_retries):
            result = await self.execute_scsi_command(
                device_path, cdb, data_direction, data_length, data, timeout
            )

            if result['success']:
                return result

            # 检查错误类型
            error = result.get('error', '')
            if self._is_retryable_error(error):
                last_error = result
                logger.warning(f"SCSI命令失败 (尝试 {attempt+1}/{max_retries}): {error}")
                await asyncio.sleep(2 ** attempt)  # 指数退避
            else:
                # 不可重试的错误
                return result

        return last_error or {'success': False, 'error': '所有重试均失败'}

    def _is_retryable_error(self, error: str) -> bool:
        """判断错误是否可重试"""
        retryable_keywords = [
            'busy',
            'timeout',
            'temporary',
            'not ready',
            'unit attention',
            '设备忙碌'
        ]
        return any(keyword in error.lower() for keyword in retryable_keywords)