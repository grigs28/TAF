#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
libltfs.dll 调用模块
用于直接调用IBM LTFS库函数实现卷标、序列号、条码的读写
"""

import os
import ctypes
from ctypes import c_int, c_char_p, c_void_p, POINTER, Structure, byref
from utils.tape_tools import tape_tools_manager
from typing import Optional, Dict, Any
import logging

logger = logging.getLogger(__name__)


class LibLTFSWrapper:
    """libltfs.dll 包装类"""
    
    def __init__(self, dll_path: Optional[str] = None):
        """
        初始化libltfs.dll包装器
        
        Args:
            dll_path: DLL文件路径，如果为None则使用默认路径
        """
        if dll_path is None:
            # 默认路径：ITDT目录下的libltfs.dll
            base_dir = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
            dll_path = os.path.join(base_dir, "ITDT", "libltfs.dll")
        
        if not os.path.exists(dll_path):
            raise FileNotFoundError(f"libltfs.dll not found at: {dll_path}")
        
        self.dll_path = dll_path
        self.dll = None
        self._load_dll()
    
    def _load_dll(self):
        """加载DLL"""
        try:
            # 加载DLL（需要设置工作目录以确保依赖DLL能被找到）
            dll_dir = os.path.dirname(self.dll_path)
            original_dir = os.getcwd()
            try:
                os.chdir(dll_dir)
                self.dll = ctypes.CDLL(self.dll_path)
                logger.info(f"成功加载 libltfs.dll: {self.dll_path}")
            finally:
                os.chdir(original_dir)
        except Exception as e:
            logger.error(f"加载 libltfs.dll 失败: {e}")
            raise
    
    def _get_function(self, name: str, argtypes=None, restype=None):
        """
        获取DLL函数
        
        Args:
            name: 函数名
            argtypes: 参数类型列表
            restype: 返回类型
        
        Returns:
            函数对象，如果不存在则返回None
        """
        try:
            if not self.dll:
                logger.warning(f"DLL未加载，无法获取函数: {name}")
                return None
            func = getattr(self.dll, name)
            if argtypes:
                func.argtypes = argtypes
            if restype:
                func.restype = restype
            return func
        except AttributeError:
            logger.debug(f"函数 {name} 不存在于 libltfs.dll")
            return None
        except Exception as e:
            logger.warning(f"获取函数 {name} 时发生错误: {e}")
            return None
    
    def list_available_functions(self, keywords: list = None) -> list:
        """
        列出可用的函数（需要先探索DLL导出）
        
        Args:
            keywords: 关键词列表，用于过滤函数名
        
        Returns:
            函数名列表
        """
        try:
            from utils.dll_explorer import DLLExplorer
            
            explorer = DLLExplorer(self.dll_path)
            if keywords:
                return explorer.search_functions(keywords)
            else:
                results = explorer.explore_all()
                # 合并所有结果并去重
                all_exports = []
                for method_results in results.values():
                    all_exports.extend(method_results)
                return sorted(list(set(all_exports)))
        except ImportError:
            logger.warning("dll_explorer模块不可用，无法列出导出函数")
            return []
        except Exception as e:
            logger.error(f"列出导出函数失败: {e}", exc_info=True)
            return []
    
    # ===== 卷标相关函数 =====
    
    def get_volume_label(self, drive_identifier: str) -> Optional[str]:
        """
        获取卷标
        
        Args:
            drive_identifier: 驱动器标识（盘符如 'O' 或 SCSI地址如 '0.0.24.0'）
        
        Returns:
            卷标字符串，失败返回None
        """
        if not self.dll:
            logger.warning("libltfs.dll未加载")
            return None
            
        # 尝试常见的LTFS函数名
        # 注意：这些函数名需要根据实际DLL导出函数调整
        
        # 方法1: 尝试 ltfs_get_volume_label 或类似函数
        func_names = [
            'ltfs_get_volume_label',
            'ltfs_get_volume_name',
            'ltfs_volume_label_get',
            'ltfs_get_label',
        ]
        
        for func_name in func_names:
            try:
                func = self._get_function(
                    func_name,
                    argtypes=[c_char_p],
                    restype=c_char_p
                )
                if func:
                    try:
                        drive = drive_identifier.encode('utf-8')
                        result = func(drive)
                        if result:
                            return result.decode('utf-8', errors='ignore')
                    except Exception as e:
                        logger.debug(f"调用 {func_name} 失败: {e}")
            except Exception as e:
                logger.debug(f"获取函数 {func_name} 时发生错误: {e}")
                continue
        
        # 如果直接调用失败，尝试通过文件系统API（仅适用于盘符格式）
        if len(drive_identifier) == 1 and drive_identifier.isalpha():
            try:
                result = self._get_volume_label_via_filesystem(drive_identifier)
                if result:
                    return result
            except Exception as e:
                logger.debug(f"使用文件系统API获取卷标失败: {e}")
        
        # 如果DLL方法都失败，记录日志
        logger.debug(f"libltfs.dll中未找到卷标读取函数，请使用tape_tools_manager.read_tape_label_windows()作为替代")
        
        return None
    
    def set_volume_label(self, drive_identifier: str, label: str) -> bool:
        """
        设置卷标
        
        Args:
            drive_identifier: 驱动器标识（盘符如 'O' 或 SCSI地址如 '0.0.24.0'）
            label: 卷标名称
        
        Returns:
            成功返回True，失败返回False
        """
        func_names = [
            'ltfs_set_volume_label',
            'ltfs_set_volume_name',
            'ltfs_volume_label_set',
            'ltfs_set_label',
        ]
        
        for func_name in func_names:
            func = self._get_function(
                func_name,
                argtypes=[c_char_p, c_char_p],
                restype=c_int
            )
            if func:
                try:
                    drive = drive_identifier.encode('utf-8')
                    label_bytes = label.encode('utf-8')
                    result = func(drive, label_bytes)
                    if result == 0:  # 假设0表示成功
                        logger.info(f"成功设置卷标: {label}")
                        return True
                except Exception as e:
                    logger.debug(f"调用 {func_name} 失败: {e}")
        
        return False
    
    def _get_volume_label_via_filesystem(self, drive_letter: str) -> Optional[str]:
        """通过文件系统API获取卷标（备用方法）"""
        try:
            import win32api
            volume_name = win32api.GetVolumeInformation(f"{drive_letter}:\\")[0]
            return volume_name if volume_name else None
        except ImportError:
            # 如果没有win32api，使用fsutil（已在tape_tools.py中实现）
            logger.debug("使用文件系统方法获取卷标需要win32api或fsutil")
            return None
    
    # ===== MAM属性相关函数 =====
    
    def read_mam_attribute(self, drive_identifier: str, partition: int, attribute_id: str) -> Optional[bytes]:
        """
        读取MAM属性
        
        Args:
            drive_identifier: 驱动器标识（盘符如 'O' 或 SCSI地址如 '0.0.24.0'）
            partition: 分区号（0-3）
            attribute_id: 属性ID（如 "0x0002"）
        
        Returns:
            属性数据（字节），失败返回None
        """
        if not self.dll:
            logger.warning("libltfs.dll未加载，无法读取MAM属性")
            return None
        
        logger.debug(f"开始读取MAM属性，驱动器标识: {drive_identifier}, 分区: {partition}, 属性ID: {attribute_id}")
        
        func_names = [
            'ltfs_read_mam_attribute',
            'ltfs_mam_read',
            'ltfs_read_attribute',
            'mam_read',
        ]
        
        for func_name in func_names:
            try:
                logger.debug(f"尝试函数: {func_name}")
                func = self._get_function(
                    func_name,
                    argtypes=[c_char_p, c_int, c_char_p, POINTER(c_char_p), POINTER(c_int)],
                    restype=c_int
                )
                if func:
                    try:
                        drive = drive_identifier.encode('utf-8')
                        attr_id = attribute_id.encode('utf-8')
                        data_ptr = c_char_p()
                        data_len = c_int()
                        
                        logger.debug(f"调用 {func_name}(drive={drive_identifier}, partition={partition}, attribute_id={attribute_id})")
                        result = func(drive, partition, attr_id, byref(data_ptr), byref(data_len))
                        logger.debug(f"{func_name} 返回码: {result}, 数据长度: {data_len.value}")
                        
                        if result == 0 and data_ptr.value:
                            data = ctypes.string_at(data_ptr.value, data_len.value)
                            logger.info(f"成功通过 {func_name} 读取MAM属性，数据长度: {len(data)} 字节")
                            # 释放内存（如果DLL提供了释放函数）
                            return data
                        else:
                            logger.debug(f"{func_name} 调用失败，返回码: {result}")
                    except Exception as e:
                        logger.debug(f"调用 {func_name} 失败: {e}", exc_info=True)
            except Exception as e:
                logger.debug(f"获取函数 {func_name} 时发生错误: {e}")
                continue
        
        logger.warning(f"libltfs.dll中未找到MAM属性读取函数，驱动器标识: {drive_identifier}, 属性ID: {attribute_id}")
        return None
    
    def write_mam_attribute(self, drive_identifier: str, partition: int, attribute_id: str, data: bytes) -> bool:
        """
        写入MAM属性
        
        Args:
            drive_identifier: 驱动器标识（盘符如 'O' 或 SCSI地址如 '0.0.24.0'）
            partition: 分区号（0-3）
            attribute_id: 属性ID（如 "0x0002"）
            data: 属性数据（字节）
        
        Returns:
            成功返回True，失败返回False
        """
        func_names = [
            'ltfs_write_mam_attribute',
            'ltfs_mam_write',
            'ltfs_write_attribute',
            'mam_write',
        ]
        
        for func_name in func_names:
            func = self._get_function(
                func_name,
                argtypes=[c_char_p, c_int, c_char_p, c_char_p, c_int],
                restype=c_int
            )
            if func:
                try:
                    drive = drive_identifier.encode('utf-8')
                    attr_id = attribute_id.encode('utf-8')
                    
                    result = func(drive, partition, attr_id, data, len(data))
                    if result == 0:
                        logger.info(f"成功写入MAM属性: {attribute_id}")
                        return True
                except Exception as e:
                    logger.debug(f"调用 {func_name} 失败: {e}")
        
        return False
    
    # ===== 便捷方法 =====
    
    def get_serial_number(self, drive_identifier: str) -> Optional[str]:
        """
        获取序列号（从MAM属性0x0002）
        
        Args:
            drive_identifier: 驱动器标识（盘符如 'O' 或 SCSI地址如 '0.0.24.0'）
        
        Returns:
            序列号字符串，失败返回None
        """
        if not self.dll:
            logger.warning("libltfs.dll未加载，无法读取序列号")
            return None
        
        logger.debug(f"开始通过libltfs.dll读取序列号，驱动器标识: {drive_identifier}")
        
        # 方法1: 尝试直接读取MAM属性0x0002
        logger.debug("方法1: 尝试读取MAM属性 0x0002...")
        data = self.read_mam_attribute(drive_identifier, 0, "0x0002")
        if data:
            try:
                # 尝试解码为字符串
                serial = data.decode('utf-8', errors='ignore').strip('\x00 \t\n\r')
                if serial:
                    logger.info(f"成功从MAM属性读取序列号: {serial}")
                    return serial
                else:
                    logger.debug(f"MAM属性数据为空或只包含空白字符")
            except Exception as e:
                logger.debug(f"解码MAM属性数据失败: {e}")
        
        # 方法2: 尝试常见的序列号读取函数名
        logger.debug("方法2: 尝试直接调用序列号读取函数...")
        func_names = [
            'ltfs_get_serial_number',
            'ltfs_serial_number_get',
            'ltfs_read_serial',
            'ltfs_get_mam_serial',
        ]
        
        for func_name in func_names:
            try:
                logger.debug(f"尝试函数: {func_name}")
                # 尝试不同的函数签名
                # 签名1: (drive) -> char*
                func = self._get_function(
                    func_name,
                    argtypes=[c_char_p],
                    restype=c_char_p
                )
                if func:
                    try:
                        drive = drive_identifier.encode('utf-8')
                        logger.debug(f"调用 {func_name}(drive={drive_identifier})")
                        result = func(drive)
                        if result:
                            serial = result.decode('utf-8', errors='ignore').strip('\x00')
                            if serial:
                                logger.info(f"成功通过 {func_name} 读取序列号: {serial}")
                                return serial
                    except Exception as e:
                        logger.debug(f"调用 {func_name} 失败: {e}")
                
                # 签名2: (drive, buffer, buffer_size) -> int
                func = self._get_function(
                    func_name,
                    argtypes=[c_char_p, c_char_p, c_int],
                    restype=c_int
                )
                if func:
                    try:
                        drive = drive_identifier.encode('utf-8')
                        buffer_size = 256
                        buffer = ctypes.create_string_buffer(buffer_size)
                        logger.debug(f"调用 {func_name}(drive={drive_identifier}, buffer_size={buffer_size})")
                        result = func(drive, buffer, buffer_size)
                        if result == 0:  # 0通常表示成功
                            serial = buffer.value.decode('utf-8', errors='ignore').strip('\x00')
                            if serial:
                                logger.info(f"成功通过 {func_name} 读取序列号: {serial}")
                                return serial
                    except Exception as e:
                        logger.debug(f"调用 {func_name} 失败: {e}")
                        
            except Exception as e:
                logger.debug(f"获取函数 {func_name} 时发生错误: {e}")
                continue
        
        logger.warning(f"libltfs.dll中未找到序列号读取函数，驱动器标识: {drive_identifier}")
        return None
    
    def set_serial_number(self, drive_identifier: str, serial_number: str) -> bool:
        """
        设置序列号（写入MAM属性0x0002）
        
        Args:
            drive_identifier: 驱动器标识（盘符如 'O' 或 SCSI地址如 '0.0.24.0'）
            serial_number: 序列号字符串
        
        Returns:
            成功返回True，失败返回False
        """
        data = serial_number.encode('utf-8')
        return self.write_mam_attribute(drive_identifier, 0, "0x0002", data)
    
    def get_barcode(self, drive_identifier: str) -> Optional[str]:
        """
        获取条码（从MAM属性0x0009）
        
        Args:
            drive_identifier: 驱动器标识（盘符如 'O' 或 SCSI地址如 '0.0.24.0'）
        
        Returns:
            条码字符串，失败返回None
        """
        data = self.read_mam_attribute(drive_identifier, 0, "0x0009")
        if data:
            try:
                text = data.decode('ascii', errors='ignore').strip('\x00 \t\n\r')
                printable_text = ''.join(c for c in text if c.isprintable()).strip()
                return printable_text if printable_text else None
            except Exception:
                return None
        return None
    
    def set_barcode(self, drive_identifier: str, barcode: str) -> bool:
        """
        设置条码（写入MAM属性0x0009）
        
        Args:
            drive_identifier: 驱动器标识（盘符如 'O' 或 SCSI地址如 '0.0.24.0'）
            barcode: 条码字符串
        
        Returns:
            成功返回True，失败返回False
        """
        data = barcode.encode('utf-8')
        return self.write_mam_attribute(drive_identifier, 0, "0x0009", data)


# 全局实例
_libltfs_wrapper: Optional[LibLTFSWrapper] = None


def get_libltfs_wrapper() -> Optional[LibLTFSWrapper]:
    """获取libltfs包装器实例（单例模式）"""
    global _libltfs_wrapper
    if _libltfs_wrapper is None:
        try:
            _libltfs_wrapper = LibLTFSWrapper()
        except Exception as e:
            logger.error(f"初始化libltfs包装器失败: {e}")
            return None
    return _libltfs_wrapper


# 便捷函数
def get_volume_label(drive_letter: str) -> Optional[str]:
    """获取卷标"""
    wrapper = get_libltfs_wrapper()
    if wrapper:
        return wrapper.get_volume_label(drive_letter)
    return None


def set_volume_label(drive_letter: str, label: str) -> bool:
    """设置卷标"""
    wrapper = get_libltfs_wrapper()
    if wrapper:
        return wrapper.set_volume_label(drive_letter, label)
    return False


def get_serial_number(drive_letter: str) -> Optional[str]:
    """获取序列号"""
    wrapper = get_libltfs_wrapper()
    if wrapper:
        return wrapper.get_serial_number(drive_letter)
    return None


def set_serial_number(drive_letter: str, serial_number: str) -> bool:
    """设置序列号"""
    wrapper = get_libltfs_wrapper()
    if wrapper:
        return wrapper.set_serial_number(drive_letter, serial_number)
    return False


def get_barcode(drive_letter: str) -> Optional[str]:
    """获取条码"""
    wrapper = get_libltfs_wrapper()
    if wrapper:
        return wrapper.get_barcode(drive_letter)
    return None


def set_barcode(drive_letter: str, barcode: str) -> bool:
    """设置条码"""
    wrapper = get_libltfs_wrapper()
    if wrapper:
        return wrapper.set_barcode(drive_letter, barcode)
    return False

