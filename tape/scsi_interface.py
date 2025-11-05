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
            # 优先使用配置中的驱动盘符（Windows）
            from config.settings import get_settings
            settings = get_settings()
            
            # 优先使用TAPE_DRIVE_LETTER配置（当成普通盘符使用）
            configured_path = None
            if hasattr(settings, 'TAPE_DRIVE_LETTER') and settings.TAPE_DRIVE_LETTER:
                drive_letter = settings.TAPE_DRIVE_LETTER.strip().upper()
                # 直接使用盘符作为路径（如 O: 或 O:\）
                if len(drive_letter) == 1 and drive_letter.isalpha():
                    # 构造盘符路径（如 O:\）
                    configured_path = f"{drive_letter}:\\"
                    logger.info(f"使用配置的驱动盘符: {drive_letter}:\\")
                elif ':' in drive_letter:
                    # 如果已经是盘符格式（如 O: 或 O:\），直接使用
                    if not drive_letter.endswith('\\'):
                        configured_path = f"{drive_letter}\\"
                    else:
                        configured_path = drive_letter
                    logger.info(f"使用配置的驱动盘符路径: {configured_path}")
            
            # 如果TAPE_DRIVE_LETTER未配置，尝试使用TAPE_DEVICE_PATH
            if not configured_path and hasattr(settings, 'TAPE_DEVICE_PATH') and settings.TAPE_DEVICE_PATH:
                configured_path = settings.TAPE_DEVICE_PATH.strip()
                # 如果是DOS设备路径格式（\\.\TAPE0），直接使用
                if configured_path.startswith('\\\\.\\'):
                    logger.info(f"使用配置的DOS设备路径: {configured_path}")
                # 如果是Linux路径，在Windows下跳过
                elif configured_path.startswith('/dev/'):
                    logger.warning(f"配置的路径是Linux格式: {configured_path}，在Windows下跳过")
                    configured_path = None
                # 如果是盘符格式，直接使用
                elif ':' in configured_path:
                    if not configured_path.endswith('\\'):
                        configured_path = f"{configured_path}\\"
                    logger.info(f"使用配置的盘符路径: {configured_path}")
            
            # 如果配置了路径，优先使用配置的路径
            if configured_path:
                tape_path = configured_path
                
                # 如果是盘符路径（如 O:\），直接使用，当成普通盘符操作
                if ':' in tape_path and tape_path.endswith('\\'):
                    # 验证盘符路径是否存在
                    import os
                    if os.path.exists(tape_path):
                        logger.info(f"✅ 使用配置的盘符路径: {tape_path}（当成普通盘符操作）")
                        # 尝试从盘符路径找到对应的DOS设备路径，然后获取设备信息
                        device_info = None
                        dos_path = None
                        try:
                            # 方法1：先尝试通过WMI查找所有磁带设备，找到对应的DOS路径
                            logger.info(f"通过WMI查找盘符 {tape_path} 对应的DOS设备路径...")
                            dos_path = await self._find_dos_path_via_wmi(tape_path)
                            
                            # 方法2：如果WMI方法失败，尝试直接查找常见路径
                            if not dos_path:
                                logger.info(f"WMI方法未找到DOS路径，尝试直接查找常见路径...")
                                dos_path = await self._get_dos_path_from_drive_letter(tape_path)
                            
                            if dos_path:
                                logger.info(f"✅ 找到盘符 {tape_path} 对应的DOS设备路径: {dos_path}")
                                # 使用DOS路径获取设备信息
                                device_info = await self.get_tape_info(dos_path)
                                if device_info:
                                    logger.info(f"通过DOS路径 {dos_path} 获取设备信息: {device_info.get('vendor', 'Unknown')} {device_info.get('model', 'Unknown')}")
                        except Exception as e:
                            logger.debug(f"获取设备信息失败: {str(e)}")
                        
                        if device_info:
                            # 检查是否为IBM LTO磁带机
                            model_upper = device_info.get('model', '').upper()
                            vendor_upper = device_info.get('vendor', '').upper()
                            
                            device_result = {
                                'path': tape_path,  # 盘符路径（用于文件操作）
                                'dos_path': dos_path,  # DOS设备路径（用于SCSI操作）
                                'type': 'SCSI',
                                'vendor': device_info.get('vendor', 'Unknown'),
                                'model': device_info.get('model', 'Unknown'),
                                'serial': device_info.get('serial', 'Unknown'),
                                'status': 'online',
                                'from_config': True,
                                'is_drive_letter': True  # 标记为盘符路径
                            }
                            
                            # 检查是否为IBM LTO磁带机
                            if 'ULT3580' in model_upper or 'IBM' in vendor_upper:
                                # 如果vendor不是IBM，尝试从model中提取
                                if 'IBM' not in vendor_upper and 'IBM' in model_upper:
                                    device_result['vendor'] = 'IBM'
                                
                                lto_gen = self._extract_lto_generation(model_upper)
                                if lto_gen > 0:
                                    device_result.update({
                                        'is_ibm_lto': True,
                                        'lto_generation': lto_gen,
                                        'supports_worm': True,
                                        'supports_encryption': True,
                                        'native_capacity': self._get_lto_capacity(model_upper)
                                    })
                                    logger.info(f"识别为IBM LTO-{lto_gen}磁带机")
                            
                            devices.append(device_result)
                            logger.info(f"使用盘符路径找到设备: {device_result.get('vendor', 'Unknown')} {device_result.get('model', 'Unknown')}")
                        else:
                            # 即使无法获取详细信息，也尝试找到DOS路径
                            # 如果还没找到DOS路径，尝试通过WMI查找所有磁带设备
                            dos_path_fallback = None
                            if not dos_path:
                                try:
                                    dos_path_fallback = await self._get_dos_path_from_drive_letter(tape_path)
                                except Exception as e:
                                    logger.debug(f"获取DOS路径失败: {str(e)}")
                            
                            # 如果还是没找到，尝试通过WMI扫描所有磁带设备来找到对应的DOS路径
                            if not dos_path and not dos_path_fallback:
                                logger.info(f"未找到DOS路径，尝试通过WMI扫描所有磁带设备...")
                                try:
                                    # 通过WMI查找所有磁带设备，找到与盘符对应的DOS路径
                                    dos_path_fallback = await self._find_dos_path_via_wmi(tape_path)
                                except Exception as e:
                                    logger.debug(f"通过WMI查找DOS路径失败: {str(e)}")
                            
                            # 即使无法获取详细信息，也使用配置的盘符路径
                            devices.append({
                                'path': tape_path,  # 盘符路径（用于文件操作）
                                'dos_path': dos_path or dos_path_fallback,  # DOS设备路径（用于SCSI操作，可能为None）
                                'type': 'SCSI',
                                'vendor': 'Unknown',
                                'model': 'Tape Drive',
                                'serial': 'Unknown',
                                'status': 'online',
                                'from_config': True,
                                'is_drive_letter': True
                            })
                            final_dos_path = dos_path or dos_path_fallback
                            if final_dos_path:
                                logger.info(f"使用盘符路径（无法获取详细信息，但找到DOS路径）: {tape_path} -> {final_dos_path}")
                            else:
                                logger.warning(f"使用盘符路径（无法获取详细信息，未找到DOS路径）: {tape_path}，SCSI操作可能失败")
                        return devices
                    else:
                        logger.warning(f"⚠️ 配置的盘符路径不存在: {tape_path}，尝试自动扫描...")
                # 如果是DOS设备路径（\\.\TAPE0），验证是否可用
                elif tape_path.startswith('\\\\.\\'):
                    if await self._test_tape_device_access_and_verify(tape_path):
                        logger.info(f"✅ 使用配置的DOS设备路径: {tape_path}")
                        # 获取设备信息
                        device_info = await self.get_tape_info(tape_path)
                        if device_info:
                            devices.append({
                                'path': tape_path,
                                'type': 'SCSI',
                                'vendor': device_info.get('vendor', 'Unknown'),
                                'model': device_info.get('model', 'Unknown'),
                                'serial': device_info.get('serial', 'Unknown'),
                                'status': 'online',
                                'from_config': True
                            })
                            logger.info(f"使用配置路径找到设备: {device_info.get('vendor', 'Unknown')} {device_info.get('model', 'Unknown')}")
                            return devices
                        else:
                            devices.append({
                                'path': tape_path,
                                'type': 'SCSI',
                                'vendor': 'Unknown',
                                'model': 'Tape Drive',
                                'serial': 'Unknown',
                                'status': 'online',
                                'from_config': True
                            })
                            logger.info(f"使用配置路径（无法获取详细信息）: {tape_path}")
                            return devices
                    else:
                        logger.warning(f"⚠️ 配置的DOS设备路径不可用: {tape_path}，尝试自动扫描...")
                else:
                    logger.warning(f"⚠️ 配置的路径格式不正确: {tape_path}，尝试自动扫描...")
            
            # 如果配置的路径不可用，或未配置，则通过WMI查询磁带设备
            try:
                import wmi
                c = wmi.WMI()
                for tape in c.Win32_TapeDrive():
                    # 获取DOS设备路径，优先使用PHYSICALDRIVE
                    # 尝试从DeviceID或其他属性获取DOS路径
                    tape_path = None
                    device_id = tape.DeviceID
                    
                    # 尝试找到DOS设备路径
                    # 优先使用Tape0（小写t，更常见）
                    test_path = "\\\\.\\Tape0"
                    if await self._test_tape_device_access_and_verify(test_path):
                        tape_path = test_path
                        logger.info(f"✅ 找到DOS设备路径: {test_path} (DeviceID: {device_id})")
                    
                    # 如果Tape0失败，尝试TAPE路径（\\.\TAPEn），尝试更多路径
                    if not tape_path:
                        for tape_num in range(10):  # 尝试TAPE0-9
                            test_path = f"\\\\.\\TAPE{tape_num}"
                            if await self._test_tape_device_access_and_verify(test_path):
                                tape_path = test_path
                                logger.info(f"找到DOS设备路径: {test_path} (DeviceID: {device_id})")
                                break
                    
                    # 如果TAPE路径没找到，尝试Tape1-9（小写t）
                    if not tape_path:
                        for tape_num in range(1, 10):  # 尝试Tape1-9
                            test_path = f"\\\\.\\Tape{tape_num}"
                            if await self._test_tape_device_access_and_verify(test_path):
                                tape_path = test_path
                                logger.info(f"找到DOS设备路径: {test_path} (DeviceID: {device_id})")
                                break
                    
                    # 如果没有找到TAPE路径，尝试PHYSICALDRIVE（但不推荐，可能误匹配系统盘）
                    if not tape_path:
                        logger.warning("未找到TAPE设备路径，尝试PHYSICALDRIVE（可能不准确）")
                        for drive_num in range(10):  # 尝试0-9
                            test_path = f"\\\\.\\PHYSICALDRIVE{drive_num}"
                            if await self._test_tape_device_access_and_verify(test_path):
                                tape_path = test_path
                                logger.warning(f"找到DOS设备路径: {test_path} (DeviceID: {device_id})")
                                break
                    
                    # 如果还是没找到，尝试从WMI Win32_TapeDrive获取更多信息
                    if not tape_path:
                        try:
                            # 尝试从Win32_TapeDrive的属性中获取DOS路径
                            # 某些系统可能在DeviceID中直接包含路径信息
                            if hasattr(tape, 'DevicePath') and tape.DevicePath:
                                potential_path = tape.DevicePath
                                if potential_path.startswith('\\\\.\\'):
                                    if await self._test_tape_device_access_and_verify(potential_path):
                                        tape_path = potential_path
                                        logger.info(f"从WMI DevicePath找到DOS设备路径: {tape_path} (DeviceID: {device_id})")
                        except Exception as e:
                            logger.debug(f"从WMI DevicePath获取DOS路径失败: {str(e)}")
                    
                    # 如果还是没找到，尝试从注册表查询DOS路径
                    if not tape_path:
                        try:
                            tape_path = await self._get_dos_path_from_registry(device_id)
                            if tape_path:
                                logger.info(f"从注册表找到DOS设备路径: {tape_path} (DeviceID: {device_id})")
                        except Exception as e:
                            logger.debug(f"从注册表查询DOS路径失败: {str(e)}")
                    
                    # 如果还是没找到，尝试通过SCSI信息查找
                    if not tape_path:
                        try:
                            # 尝试使用SCSI总线、目标ID、LUN信息来查找设备
                            scsi_bus = getattr(tape, 'SCSIBus', None)
                            scsi_target = getattr(tape, 'SCSITargetId', None)
                            scsi_lun = getattr(tape, 'SCSILogicalUnit', None)
                            
                            if scsi_bus is not None and scsi_target is not None:
                                # 尝试通过SCSI信息查找对应的设备路径
                                # 这需要更复杂的逻辑，暂时先跳过
                                logger.debug(f"SCSI信息: Bus={scsi_bus}, Target={scsi_target}, LUN={scsi_lun}")
                        except Exception as e:
                            logger.debug(f"通过SCSI信息查找DOS路径失败: {str(e)}")
                    
                    # 如果还是没找到，尝试使用存储API验证常见TAPE路径
                    if not tape_path:
                        logger.info(f"WMI方法未找到DOS路径，尝试使用存储API验证常见TAPE路径...")
                        test_paths = [
                            "\\\\.\\Tape0",  # 小写t，优先
                            "\\\\.\\TAPE0",  # 大写TAPE
                        ]
                        for tape_num in range(1, 10):
                            test_paths.append(f"\\\\.\\TAPE{tape_num}")
                        for tape_num in range(1, 10):
                            test_paths.append(f"\\\\.\\Tape{tape_num}")
                        
                        for test_path in test_paths:
                            if await self._test_tape_device_access(test_path):
                                if await self._verify_tape_device_by_storage_api(test_path):
                                    tape_path = test_path
                                    logger.info(f"通过存储API找到DOS设备路径: {test_path} (DeviceID: {device_id})")
                                    break
                    
                    # 如果还是没找到，最后再使用DeviceID作为fallback（不可直接用于IO）
                    if not tape_path:
                        tape_path = device_id
                        logger.warning(f"无法找到有效的DOS设备路径，使用DeviceID: {device_id}（注意：此路径可能无法直接用于IO操作）")
                    
                    # 获取详细的设备信息
                    device_info = {
                        'path': tape_path if tape_path.startswith('\\\\.\\') else device_id,  # 如果是DOS路径，使用DOS路径；否则使用DeviceID
                        'dos_path': tape_path if tape_path.startswith('\\\\.\\') else None,  # 保存DOS路径（如果找到）
                        'type': 'SCSI',
                        'vendor': getattr(tape, 'Manufacturer', 'Unknown'),
                        'model': getattr(tape, 'Name', 'Unknown'),
                        'serial': getattr(tape, 'SerialNumber', 'Unknown'),
                        'status': 'online',
                        'scsi_bus': getattr(tape, 'SCSIBus', 'Unknown'),
                        'scsi_target_id': getattr(tape, 'SCSITargetId', 'Unknown'),
                        'scsi_lun': getattr(tape, 'SCSILogicalUnit', 'Unknown'),
                        'wmi_device_id': device_id  # 保存原始WMI DeviceID
                    }

                    # 检查是否为IBM LTO磁带机
                    # 从模型名称或DeviceID中检查
                    model_upper = device_info.get('model', '').upper()
                    path_upper = device_info.get('path', '').upper()
                    if 'ULT3580' in model_upper or 'VEN_IBM' in path_upper:
                        # 如果vendor不是IBM，尝试从model或path中提取
                        if 'IBM' not in device_info.get('vendor', '').upper() and 'IBM' in model_upper:
                            device_info['vendor'] = 'IBM'
                        
                        device_info.update({
                            'is_ibm_lto': True,
                            'lto_generation': self._extract_lto_generation(model_upper),
                            'supports_worm': True,
                            'supports_encryption': True,
                            'native_capacity': self._get_lto_capacity(model_upper)
                        })
                        
                        logger.info(f"识别为IBM LTO-{device_info.get('lto_generation', 0)}磁带机")

                    devices.append(device_info)
                    logger.info(f"发现磁带设备: {device_info['vendor']} {device_info['model']}")

            except ImportError:
                logger.warning("WMI模块不可用，使用基本扫描方法")

            # 如果WMI不可用，检查可用的磁带驱动器盘符
            if not devices:
                # 检查常见磁带设备路径
                tape_paths = [
                    "\\\\.\\TAPE0",
                    "\\\\.\\TAPE1",
                    "\\\\.\\TAPE2",
                    "\\\\.\\Tape0",
                    "\\\\.\\Tape1",
                    "\\\\.\\Tape2",
                    "\\\\.\\TAPE0",
                    "\\\\.\\TAPE1",
                    "\\\\.\\TAPE2"
                ]

                for tape_path in tape_paths:
                    if await self._test_tape_device_access_and_verify(tape_path):
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
                        
                        # 检查是否为IBM LTO磁带机
                        vendor = device_info.get('vendor', '').upper()
                        model = device_info.get('model', '').upper()
                        path_upper = device_info.get('path', '').upper()
                        if 'ULT3580' in model or 'VEN_IBM' in path_upper:
                            # 如果vendor不是IBM，尝试从model或path中提取
                            if 'IBM' not in vendor and 'IBM' in model:
                                device_info['vendor'] = 'IBM'
                            
                            lto_gen = self._extract_lto_generation(model)
                            device_info.update({
                                'is_ibm_lto': True,
                                'lto_generation': lto_gen,
                                'supports_worm': True,
                                'supports_encryption': True,
                                'native_capacity': self._get_lto_capacity(model)
                            })
                            
                            logger.info(f"识别为IBM LTO-{lto_gen}磁带机")

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
                        vendor_upper = vendor.upper()
                        model_upper = model.upper()
                        if 'ULT3580' in model_upper:
                            # 如果vendor不是IBM，尝试从model中提取
                            if 'IBM' not in vendor_upper and 'IBM' in model_upper:
                                vendor = 'IBM'
                            
                            lto_gen = self._extract_lto_generation(model_upper)
                            device_info.update({
                                'is_ibm_lto': True,
                                'lto_generation': lto_gen,
                                'supports_worm': True,
                                'supports_encryption': True,
                                'native_capacity': self._get_lto_capacity(model_upper)
                            })
                            
                            logger.info(f"识别为IBM LTO-{lto_gen}磁带机")

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
            # 如果是盘符路径（如 O:\），需要转换为DOS设备路径
            # 因为SCSI命令需要DOS设备路径，不能直接使用盘符
            if ':' in device_path and device_path.endswith('\\'):
                # 尝试从盘符找到对应的DOS设备路径
                dos_path = await self._get_dos_path_from_drive_letter(device_path)
                if dos_path:
                    logger.debug(f"从盘符 {device_path} 转换为DOS路径: {dos_path}")
                    device_path = dos_path
                else:
                    # 如果无法找到DOS路径，返回错误
                    return {'success': False, 'error': f'盘符路径 {device_path} 无法用于SCSI命令，需要DOS设备路径（如 \\\\.\\TAPE0）'}
            
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

    async def _get_dos_device_path(self, device_path: str = None) -> Optional[str]:
        """获取DOS设备路径（如 \\.\Tape0），用于SCSI命令"""
        try:
            # 如果已提供DOS设备路径，直接使用
            if device_path and device_path.startswith('\\\\.\\'):
                if await self._test_tape_device_access(device_path):
                    logger.info(f"使用提供的DOS设备路径: {device_path}")
                    return device_path
                else:
                    logger.warning(f"提供的DOS设备路径不可访问: {device_path}")
            
            # 优先使用配置的TAPE_DEVICE_PATH（DOS设备路径）
            if hasattr(self.settings, 'TAPE_DEVICE_PATH') and self.settings.TAPE_DEVICE_PATH:
                configured_path = self.settings.TAPE_DEVICE_PATH.strip()
                if configured_path.startswith('\\\\.\\'):
                    if await self._test_tape_device_access(configured_path):
                        logger.info(f"使用配置的DOS设备路径: {configured_path}")
                        return configured_path
                    else:
                        logger.warning(f"配置的DOS设备路径不可访问: {configured_path}")
            
            # 如果已扫描的设备列表中有DOS路径，优先使用
            if self.tape_devices:
                logger.info(f"从已扫描的设备列表中选择DOS路径（共 {len(self.tape_devices)} 个设备）...")
                for device in self.tape_devices:
                    # 优先检查设备信息中是否已保存了DOS路径（用于盘符路径对应的DOS路径）
                    dos_path_from_device = device.get('dos_path')
                    if dos_path_from_device and dos_path_from_device.startswith('\\\\.\\'):
                        logger.info(f"尝试使用设备信息中保存的DOS路径: {dos_path_from_device}")
                        if await self._test_tape_device_access(dos_path_from_device):
                            logger.info(f"✅ 使用设备信息中保存的DOS设备路径: {dos_path_from_device} (设备: {device.get('vendor', 'Unknown')} {device.get('model', 'Unknown')})")
                            return dos_path_from_device
                        else:
                            logger.debug(f"设备信息中保存的DOS设备路径不可访问: {dos_path_from_device}")
                    
                    path = device.get('path', '')
                    # 优先选择DOS设备路径（不是盘符路径）
                    if path.startswith('\\\\.\\'):
                        logger.info(f"尝试使用已扫描的DOS设备路径: {path}")
                        if await self._test_tape_device_access(path):
                            logger.info(f"✅ 使用已扫描的DOS设备路径: {path} (设备: {device.get('vendor', 'Unknown')} {device.get('model', 'Unknown')})")
                            return path
                        else:
                            logger.debug(f"已扫描的DOS设备路径不可访问: {path}")
            
            # 如果设备列表中有盘符路径，尝试找到对应的DOS路径
            if self.tape_devices:
                for device in self.tape_devices:
                    path = device.get('path', '')
                    # 如果是盘符路径，尝试找到对应的DOS路径
                    if ':' in path and path.endswith('\\'):
                        logger.info(f"从盘符路径 {path} 查找对应的DOS设备路径...")
                        dos_path = await self._get_dos_path_from_drive_letter(path)
                        if dos_path:
                            # 更新设备信息中的DOS路径，以便下次使用
                            device['dos_path'] = dos_path
                            logger.info(f"✅ 从盘符路径找到对应的DOS设备路径: {dos_path}")
                            return dos_path
            
            # 如果还是没找到，尝试常见的TAPE路径
            logger.info("尝试常见的TAPE设备路径...")
            
            # 优先尝试 \\.\Tape0（小写t）
            test_paths = [
                "\\\\.\\Tape0",  # 小写t，优先
                "\\\\.\\TAPE0",  # 大写TAPE
            ]
            
            # 然后尝试TAPE1-9
            for tape_num in range(1, 10):
                test_paths.append(f"\\\\.\\TAPE{tape_num}")
            
            # 然后尝试Tape1-9
            for tape_num in range(1, 10):
                test_paths.append(f"\\\\.\\Tape{tape_num}")
            
            for test_path in test_paths:
                logger.info(f"尝试DOS设备路径: {test_path}")
                # 先测试基本访问
                access_result = await self._test_tape_device_access(test_path)
                logger.info(f"基本访问测试 {test_path}: {access_result}")
                if not access_result:
                    logger.debug(f"基本访问失败，跳过: {test_path}")
                    continue
                
                # 使用存储API验证设备类型（备用方法）
                logger.info(f"使用存储API验证设备类型: {test_path}")
                storage_api_result = await self._verify_tape_device_by_storage_api(test_path)
                logger.info(f"存储API验证结果 {test_path}: {storage_api_result}")
                if storage_api_result:
                    logger.info(f"✅ 找到可用的DOS设备路径（使用存储API验证）: {test_path}")
                    return test_path
                
                # 如果存储API方法失败，尝试SCSI INQUIRY命令验证
                logger.info(f"存储API验证失败，尝试SCSI INQUIRY验证: {test_path}")
                try:
                    cdb = bytes([0x12, 0x00, 0x00, 0x00, 36, 0x00])
                    result = await self.execute_scsi_command(test_path, cdb, data_direction=1, data_length=36)
                    logger.info(f"SCSI INQUIRY命令结果 {test_path}: success={result.get('success')}, error={result.get('error')}")
                    if result.get('success') and result.get('data'):
                        device_type = result['data'][0] & 0x1F
                        logger.info(f"设备类型: {device_type} (期望1=磁带设备)")
                        if device_type == 1:  # 设备类型1 = 磁带设备
                            logger.info(f"✅ 找到可用的DOS设备路径（使用SCSI INQUIRY验证）: {test_path}")
                            return test_path
                except Exception as e:
                    logger.warning(f"SCSI INQUIRY验证失败 {test_path}: {str(e)}")
                    import traceback
                    logger.debug(f"SCSI INQUIRY验证异常堆栈:\n{traceback.format_exc()}")
                    continue
            
            # 如果所有方法都失败，返回None
            logger.error("无法找到可用的DOS设备路径")
            logger.error(f"已扫描的设备列表: {[d.get('path') for d in self.tape_devices] if self.tape_devices else '无'}")
            logger.error("建议：")
            logger.error("1. 检查磁带设备是否正确连接")
            logger.error("2. 检查设备驱动是否正确安装")
            logger.error("3. 在配置文件中设置 TAPE_DEVICE_PATH 为正确的DOS设备路径（如 \\\\.\\Tape0）")
            logger.error("4. 如果是盘符路径（如 O:\\），确保系统能够访问该路径")
            return None
            
        except Exception as e:
            logger.error(f"获取DOS设备路径失败: {str(e)}")
            import traceback
            logger.error(f"异常堆栈:\n{traceback.format_exc()}")
            return None

    async def test_unit_ready(self, device_path: str = None) -> bool:
        """测试设备就绪状态"""
        try:
            # SCSI操作必须使用DOS设备路径（如 \\.\Tape0）
            device_path = await self._get_dos_device_path(device_path)
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
            # SCSI操作必须使用DOS设备路径（如 \\.\Tape0）
            device_path = await self._get_dos_device_path(device_path)
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
            # 格式化操作必须使用DOS设备路径（如 \\.\Tape0）
            device_path = await self._get_dos_device_path(device_path)
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
            # 擦除操作必须使用DOS设备路径（如 \\.\Tape0）
            device_path = await self._get_dos_device_path(device_path)
            if not device_path:
                return False

            # ERASE命令 (0x19)
            # Byte 1: Erase Type (bit 0: 0=short, 1=long)
            # 注意：根据参考代码，0x01 (bit 0=1) 表示LONG ERASE，0x00 表示SHORT ERASE
            cdb = bytes([0x19, erase_type & 0xFF, 0x00, 0x00, 0x00, 0x00])
            result = await self.execute_scsi_command(device_path, cdb, timeout=600)

            return result['success']

        except Exception as e:
            logger.error(f"磁带擦除失败: {str(e)}")
            return False

    async def load_unload(self, device_path: str = None, load: bool = True) -> bool:
        """加载/卸载磁带 - LOAD UNLOAD命令"""
        try:
            # 加载/卸载操作必须使用DOS设备路径（如 \\.\Tape0）
            device_path = await self._get_dos_device_path(device_path)
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
                # 如果是盘符路径（如 O:\），当成普通盘符访问
                if ':' in device_path and device_path.endswith('\\'):
                    import os
                    # 直接检查盘符路径是否存在
                    if os.path.exists(device_path):
                        # 尝试访问盘符根目录
                        try:
                            os.listdir(device_path)
                            return True
                        except:
                            # 即使listdir失败，路径存在就认为可访问
                            return True
                    return False
                
                # DOS设备路径（\\.\TAPE0），使用SCSI方式访问
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

    async def _verify_tape_device_by_storage_api(self, device_path: str) -> bool:
        """使用IOCTL_STORAGE_QUERY_PROPERTY验证设备是否为磁带设备（备用方法）
        
        参考 tape01.py 的实现，使用只读方式打开设备
        """
        try:
            if self.system != "Windows":
                return False
            
            # 导入必要的模块（文件顶部已导入 ctypes，这里只需要 wintypes）
            from ctypes import wintypes
            
            # 打开设备（参考 tape01.py，使用只读方式）
            # tape01.py 使用 GENERIC_READ，FILE_SHARE_READ | FILE_SHARE_WRITE
            FILE_SHARE_READ = 0x00000001
            FILE_SHARE_WRITE = 0x00000002
            
            handle = self.create_file(
                device_path,
                0x80000000,  # GENERIC_READ（只读，参考 tape01.py）
                FILE_SHARE_READ | FILE_SHARE_WRITE,
                None,
                3,           # OPEN_EXISTING
                0x80,        # FILE_ATTRIBUTE_NORMAL
                None
            )
            
            if handle == -1:  # INVALID_HANDLE_VALUE
                error_code = self.kernel32.GetLastError()
                logger.debug(f"无法打开设备 {device_path}: 错误代码 = {error_code}")
                return False
            
            try:
                # 使用IOCTL_STORAGE_QUERY_PROPERTY获取设备信息
                IOCTL_STORAGE_QUERY_PROPERTY = 0x002D1400
                
                class STORAGE_PROPERTY_QUERY(Structure):
                    _fields_ = [
                        ("PropertyId", wintypes.DWORD),
                        ("QueryType", wintypes.DWORD),
                        ("AdditionalParameters", wintypes.BYTE * 1)
                    ]
                
                class STORAGE_DEVICE_DESCRIPTOR(Structure):
                    _fields_ = [
                        ("Version", wintypes.DWORD),
                        ("Size", wintypes.DWORD),
                        ("DeviceType", wintypes.BYTE),
                        ("DeviceTypeModifier", wintypes.BYTE),
                        ("RemovableMedia", wintypes.BOOLEAN),
                        ("CommandQueueing", wintypes.BOOLEAN),
                        ("VendorIdOffset", wintypes.DWORD),
                        ("ProductIdOffset", wintypes.DWORD),
                        ("ProductRevisionOffset", wintypes.DWORD),
                        ("SerialNumberOffset", wintypes.DWORD),
                        ("BusType", wintypes.DWORD),
                        ("RawPropertiesLength", wintypes.DWORD),
                        ("RawDeviceProperties", wintypes.BYTE * 1)
                    ]
                
                query = STORAGE_PROPERTY_QUERY()
                query.PropertyId = 0  # StorageDeviceProperty
                query.QueryType = 0   # PropertyStandardQuery
                
                buffer_size = 4096
                buffer = ctypes.create_string_buffer(buffer_size)
                bytes_returned = wintypes.DWORD()
                
                # 使用 DeviceIoControl（参考 tape01.py 的实现）
                success = ctypes.windll.kernel32.DeviceIoControl(
                    handle,
                    IOCTL_STORAGE_QUERY_PROPERTY,
                    byref(query), ctypes.sizeof(query),
                    buffer, buffer_size,
                    byref(bytes_returned),
                    None
                )
                
                if success:
                    # 检查返回的字节数
                    if bytes_returned.value == 0:
                        error_code = ctypes.windll.kernel32.GetLastError()
                        logger.debug(f"存储API查询返回0字节: {device_path}, 错误代码 = {error_code}")
                        return False
                    descriptor = STORAGE_DEVICE_DESCRIPTOR.from_buffer(buffer)
                    # 设备类型1 = 磁带设备
                    if descriptor.DeviceType == 1:
                        # 获取厂商和产品信息（用于日志）
                        vendor = "Unknown"
                        product = "Unknown"
                        try:
                            if descriptor.VendorIdOffset > 0:
                                vendor = ctypes.string_at(ctypes.addressof(buffer) + descriptor.VendorIdOffset).decode('utf-8', errors='ignore').strip()
                            if descriptor.ProductIdOffset > 0:
                                product = ctypes.string_at(ctypes.addressof(buffer) + descriptor.ProductIdOffset).decode('utf-8', errors='ignore').strip()
                        except:
                            pass
                        
                        logger.info(f"通过存储API验证为磁带设备: {device_path} (厂商: {vendor}, 产品: {product}, 设备类型: {descriptor.DeviceType})")
                        return True
                    else:
                        logger.debug(f"存储API验证：设备类型不匹配: {device_path}, type={descriptor.DeviceType} (期望1=磁带)")
                        return False
                else:
                    logger.debug(f"存储API查询失败: {device_path}")
                    return False
            finally:
                self.kernel32.CloseHandle(handle)
                
        except Exception as e:
            logger.debug(f"使用存储API验证设备失败 {device_path}: {str(e)}")
            return False

    async def _get_dos_path_from_drive_letter(self, drive_letter_path: str) -> Optional[str]:
        """从盘符路径获取对应的DOS设备路径
        
        使用备用方法（IOCTL_STORAGE_QUERY_PROPERTY）验证设备信息
        """
        try:
            # 提取盘符（如 O:\ -> O）
            drive_letter = drive_letter_path[0].upper()
            
            # 尝试查找对应的TAPE设备路径
            # 方法1：优先尝试 Tape0（小写t，更常见）
            test_paths = [
                "\\\\.\\Tape0",  # 小写t，优先
                "\\\\.\\TAPE0",  # 大写TAPE
            ]
            
            # 然后尝试TAPE1-9
            for tape_num in range(1, 10):
                test_paths.append(f"\\\\.\\TAPE{tape_num}")
            
            # 然后尝试Tape1-9
            for tape_num in range(1, 10):
                test_paths.append(f"\\\\.\\Tape{tape_num}")
            
            for test_path in test_paths:
                # 先测试基本访问
                if not await self._test_tape_device_access(test_path):
                    continue
                
                # 使用备用方法（IOCTL_STORAGE_QUERY_PROPERTY）验证设备类型
                if await self._verify_tape_device_by_storage_api(test_path):
                    logger.info(f"✅ 从盘符 {drive_letter}:\\ 找到对应的DOS路径（使用存储API验证）: {test_path}")
                    return test_path
                
                # 如果存储API方法失败，尝试SCSI INQUIRY命令验证
                try:
                    cdb = bytes([0x12, 0x00, 0x00, 0x00, 36, 0x00])
                    result = await self.execute_scsi_command(test_path, cdb, data_direction=1, data_length=36)
                    if result.get('success') and result.get('data'):
                        device_type = result['data'][0] & 0x1F
                        if device_type == 1:  # 设备类型1 = 磁带设备
                            logger.info(f"✅ 从盘符 {drive_letter}:\\ 找到对应的DOS路径（使用SCSI INQUIRY验证）: {test_path}")
                            return test_path
                except Exception as e:
                    logger.debug(f"SCSI INQUIRY验证失败 {test_path}: {str(e)}")
                    continue
            
            # 如果无法找到，返回None（让调用者处理）
            # 注意：如果使用盘符路径进行文件系统操作，不需要DOS设备路径
            logger.debug(f"无法从盘符 {drive_letter}:\\ 找到对应的DOS设备路径（这是正常的，如果使用文件系统操作）")
            return None
        except Exception as e:
            logger.debug(f"从盘符获取DOS路径失败: {str(e)}")
            import traceback
            logger.debug(f"异常堆栈:\n{traceback.format_exc()}")
            return None

    async def _find_dos_path_via_wmi(self, drive_letter_path: str) -> Optional[str]:
        """通过WMI查找盘符对应的DOS设备路径
        
        逻辑：
        1. 通过WMI查找所有磁带设备
        2. 对每个磁带设备，尝试找到其DOS设备路径（\\.\Tape0, \\.\TAPE0等）
        3. 验证找到的DOS路径是否对应盘符（通过测试SCSI命令是否成功）
        4. 返回第一个验证成功的DOS路径
        
        注意：如果有多个磁带设备，返回第一个可用的DOS路径
        """
        try:
            import wmi
            c = wmi.WMI()
            
            # 提取盘符（如 O:\ -> O）
            drive_letter = drive_letter_path[0].upper() if drive_letter_path else None
            
            # 查找所有磁带设备
            tape_devices = list(c.Win32_TapeDrive())
            logger.info(f"通过WMI找到 {len(tape_devices)} 个磁带设备")
            
            if not tape_devices:
                logger.warning("WMI未找到任何磁带设备")
                return None
            
            # 对每个磁带设备，尝试找到对应的DOS路径
            for tape in tape_devices:
                device_id = tape.DeviceID
                logger.debug(f"处理磁带设备: {device_id}")
                
                # 尝试找到DOS设备路径（使用与扫描时相同的逻辑）
                tape_path = None
                
                # 优先使用Tape0（小写t，更常见）
                test_path = "\\\\.\\Tape0"
                if await self._test_tape_device_access(test_path):
                    # 验证是否是磁带设备
                    try:
                        cdb = bytes([0x12, 0x00, 0x00, 0x00, 36, 0x00])
                        result = await self.execute_scsi_command(test_path, cdb, data_direction=1, data_length=36)
                        if result.get('success') and result.get('data'):
                            device_type = result['data'][0] & 0x1F
                            if device_type == 1:  # 设备类型1 = 磁带设备
                                tape_path = test_path
                                logger.info(f"通过WMI找到DOS设备路径: {test_path} (DeviceID: {device_id})")
                    except Exception as e:
                        logger.debug(f"验证Tape0失败: {str(e)}")
                
                # 如果Tape0失败，尝试TAPE路径
                if not tape_path:
                    for tape_num in range(10):  # 尝试TAPE0-9
                        test_path = f"\\\\.\\TAPE{tape_num}"
                        if await self._test_tape_device_access(test_path):
                            try:
                                cdb = bytes([0x12, 0x00, 0x00, 0x00, 36, 0x00])
                                result = await self.execute_scsi_command(test_path, cdb, data_direction=1, data_length=36)
                                if result.get('success') and result.get('data'):
                                    device_type = result['data'][0] & 0x1F
                                    if device_type == 1:
                                        tape_path = test_path
                                        logger.info(f"通过WMI找到DOS设备路径: {test_path} (DeviceID: {device_id})")
                                        break
                            except Exception as e:
                                logger.debug(f"验证TAPE{tape_num}失败: {str(e)}")
                                continue
                
                # 如果TAPE路径没找到，尝试Tape1-9
                if not tape_path:
                    for tape_num in range(1, 10):  # 尝试Tape1-9
                        test_path = f"\\\\.\\Tape{tape_num}"
                        if await self._test_tape_device_access(test_path):
                            try:
                                cdb = bytes([0x12, 0x00, 0x00, 0x00, 36, 0x00])
                                result = await self.execute_scsi_command(test_path, cdb, data_direction=1, data_length=36)
                                if result.get('success') and result.get('data'):
                                    device_type = result['data'][0] & 0x1F
                                    if device_type == 1:
                                        tape_path = test_path
                                        logger.info(f"通过WMI找到DOS设备路径: {test_path} (DeviceID: {device_id})")
                                        break
                            except Exception as e:
                                logger.debug(f"验证Tape{tape_num}失败: {str(e)}")
                                continue
                
                # 如果找到DOS路径，验证是否与盘符对应
                # 注意：Windows系统中，盘符和DOS路径的对应关系不是直接的
                # 我们假设如果有多个磁带设备，第一个可用的DOS路径就对应盘符
                if tape_path:
                    # 验证DOS路径是否可用（通过SCSI命令）
                    try:
                        # 发送INQUIRY命令验证设备
                        cdb = bytes([0x12, 0x00, 0x00, 0x00, 36, 0x00])
                        result = await self.execute_scsi_command(tape_path, cdb, data_direction=1, data_length=36)
                        if result.get('success'):
                            logger.info(f"✅ 通过WMI验证DOS设备路径: {tape_path} 对应盘符 {drive_letter_path} (DeviceID: {device_id})")
                            return tape_path
                    except Exception as e:
                        logger.debug(f"验证DOS路径失败: {str(e)}")
                        continue
            
            logger.warning(f"通过WMI未找到盘符 {drive_letter_path} 对应的DOS设备路径")
            return None
            
        except ImportError:
            logger.debug("WMI模块不可用，无法通过WMI查找DOS路径")
            return None
        except Exception as e:
            logger.debug(f"通过WMI查找DOS路径失败: {str(e)}")
            import traceback
            logger.debug(f"异常堆栈:\n{traceback.format_exc()}")
            return None

    async def _get_dos_path_from_registry(self, device_id: str) -> Optional[str]:
        """从注册表查询DOS设备路径"""
        try:
            import winreg
            
            # DeviceID格式：SCSI\SEQUENTIAL&VEN_IBM&PROD_ULT3580-HH9\5&23894D7F&0&001800
            # 需要查询注册表：HKEY_LOCAL_MACHINE\SYSTEM\CurrentControlSet\Enum\SCSI\...
            
            # 解析DeviceID
            parts = device_id.split('\\')
            if len(parts) < 2:
                return None
            
            # 构建注册表路径
            # HKEY_LOCAL_MACHINE\SYSTEM\CurrentControlSet\Enum\SCSI\SEQUENTIAL&VEN_IBM&PROD_ULT3580-HH9\5&23894D7F&0&001800\Device Parameters
            reg_path = f"SYSTEM\\CurrentControlSet\\Enum\\{parts[0]}\\{parts[1]}"
            if len(parts) > 2:
                reg_path += f"\\{parts[2]}"
            
            try:
                # 打开注册表键
                key = winreg.OpenKey(winreg.HKEY_LOCAL_MACHINE, reg_path)
                try:
                    # 查找可能的DOS路径值
                    # 某些设备在Device Parameters下有路径信息
                    try:
                        device_params_path = f"{reg_path}\\Device Parameters"
                        params_key = winreg.OpenKey(winreg.HKEY_LOCAL_MACHINE, device_params_path)
                        try:
                            # 尝试读取可能的路径值
                            for value_name in ['DosPath', 'DevicePath', 'Path']:
                                try:
                                    value, _ = winreg.QueryValueEx(params_key, value_name)
                                    if value and value.startswith('\\\\.\\'):
                                        return value
                                except FileNotFoundError:
                                    continue
                        finally:
                            winreg.CloseKey(params_key)
                    except FileNotFoundError:
                        pass
                    
                    # 查找子键，可能包含DOS路径信息
                    # 某些设备在子键中有路径映射
                    try:
                        i = 0
                        while True:
                            subkey_name = winreg.EnumKey(key, i)
                            i += 1
                            
                            # 检查子键中的Device Parameters
                            try:
                                subkey_path = f"{reg_path}\\{subkey_name}\\Device Parameters"
                                subkey = winreg.OpenKey(winreg.HKEY_LOCAL_MACHINE, subkey_path)
                                try:
                                    for value_name in ['DosPath', 'DevicePath', 'Path']:
                                        try:
                                            value, _ = winreg.QueryValueEx(subkey, value_name)
                                            if value and value.startswith('\\\\.\\'):
                                                return value
                                        except FileNotFoundError:
                                            continue
                                finally:
                                    winreg.CloseKey(subkey)
                            except (FileNotFoundError, OSError):
                                continue
                    except OSError:
                        pass
                finally:
                    winreg.CloseKey(key)
            except FileNotFoundError:
                return None
            except Exception as e:
                logger.debug(f"查询注册表失败: {str(e)}")
                return None
            
            return None
        except ImportError:
            # Windows系统应该有winreg模块
            return None
        except Exception as e:
            logger.debug(f"从注册表获取DOS路径失败: {str(e)}")
            return None

    async def _test_tape_device_access_and_verify(self, device_path: str, skip_verify: bool = False) -> bool:
        """测试磁带设备访问并验证是否是真正的磁带设备"""
        try:
            # 如果是盘符路径（如 O:\），当成普通盘符，不需要SCSI验证
            if ':' in device_path and device_path.endswith('\\'):
                # 直接检查盘符路径是否存在
                import os
                if os.path.exists(device_path):
                    logger.info(f"盘符路径可访问: {device_path}（当成普通盘符操作）")
                    return True
                return False
            
            # DOS设备路径（\\.\TAPE0），需要SCSI验证
            # 先测试基本访问
            if not await self._test_tape_device_access(device_path):
                return False
            
            # 如果跳过验证（用于快速测试），直接返回True
            if skip_verify:
                return True
            
            # 尝试发送INQUIRY命令验证是否是磁带设备
            try:
                # 发送INQUIRY命令获取设备信息
                cdb = bytes([0x12, 0x00, 0x00, 0x00, 36, 0x00])  # INQUIRY命令
                result = await self.execute_scsi_command(device_path, cdb, data_direction=1, data_length=36)
                
                if result.get('success') and result.get('data') and len(result.get('data', [])) >= 36:
                    device_type = result['data'][0] & 0x1F
                    # 设备类型1 = 磁带设备
                    if device_type == 1:
                        logger.info(f"验证为磁带设备: {device_path}")
                        return True
                    else:
                        logger.info(f"设备类型不匹配: {device_path}, type={device_type} (期望1=磁带)")
                        return False
            except Exception as verify_error:
                logger.debug(f"验证设备类型失败: {str(verify_error)}")
                # 如果验证失败，但基本访问成功，仍然返回True
                return True
            
            return False

        except Exception as e:
            logger.debug(f"测试设备访问并验证失败 {device_path}: {str(e)}")
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