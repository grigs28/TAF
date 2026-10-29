#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
使用替代方法获取磁带设备信息
"""

import ctypes
from ctypes import wintypes, Structure, byref
import sys

# 定义常量
GENERIC_READ = 0x80000000
GENERIC_WRITE = 0x40000000
OPEN_EXISTING = 3
FILE_SHARE_READ = 0x00000001
FILE_SHARE_WRITE = 0x00000002
INVALID_HANDLE_VALUE = -1

# 定义更多的 IOCTL 代码
IOCTL_STORAGE_QUERY_PROPERTY = 0x002D1400
IOCTL_SCSI_PASS_THROUGH = 0x0004D004
IOCTL_SCSI_GET_ADDRESS = 0x00041018

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

class SCSI_PASS_THROUGH(Structure):
    _fields_ = [
        ("Length", wintypes.WORD),
        ("ScsiStatus", wintypes.BYTE),
        ("PathId", wintypes.BYTE),
        ("TargetId", wintypes.BYTE),
        ("Lun", wintypes.BYTE),
        ("CdbLength", wintypes.BYTE),
        ("SenseInfoLength", wintypes.BYTE),
        ("DataIn", wintypes.BYTE),
        ("DataTransferLength", wintypes.DWORD),
        ("TimeOutValue", wintypes.DWORD),
        ("DataBufferOffset", wintypes.DWORD),
        ("SenseInfoOffset", wintypes.DWORD),
        ("Cdb", wintypes.BYTE * 16)
    ]

class SCSI_PASS_THROUGH_WITH_BUFFERS(Structure):
    _fields_ = [
        ("spt", SCSI_PASS_THROUGH),
        ("SenseBuf", wintypes.BYTE * 32),
        ("DataBuf", wintypes.BYTE * 512)
    ]

def get_storage_property(handle):
    """使用 STORAGE_QUERY_PROPERTY 获取设备信息"""
    print("\n方法1: 使用 STORAGE_QUERY_PROPERTY...")
    
    # 设置查询参数
    query = STORAGE_PROPERTY_QUERY()
    query.PropertyId = 0  # StorageDeviceProperty
    query.QueryType = 0   # PropertyStandardQuery
    
    # 分配缓冲区
    buffer_size = 4096
    buffer = ctypes.create_string_buffer(buffer_size)
    bytes_returned = wintypes.DWORD()
    
    success = ctypes.windll.kernel32.DeviceIoControl(
        handle,
        IOCTL_STORAGE_QUERY_PROPERTY,
        byref(query), ctypes.sizeof(query),
        buffer, buffer_size,
        byref(bytes_returned),
        None
    )
    
    if success:
        descriptor = STORAGE_DEVICE_DESCRIPTOR.from_buffer(buffer)
        print(f"[OK] 存储设备描述符:")
        print(f"    设备类型: {descriptor.DeviceType}")
        print(f"    可移动介质: {descriptor.RemovableMedia}")
        print(f"    总线类型: {descriptor.BusType}")
        
        # 获取厂商信息
        if descriptor.VendorIdOffset > 0:
            vendor_id = ctypes.string_at(ctypes.addressof(buffer) + descriptor.VendorIdOffset)
            print(f"    厂商: {vendor_id.decode('utf-8', errors='ignore').strip()}")
        
        # 获取产品信息
        if descriptor.ProductIdOffset > 0:
            product_id = ctypes.string_at(ctypes.addressof(buffer) + descriptor.ProductIdOffset)
            print(f"    产品: {product_id.decode('utf-8', errors='ignore').strip()}")
        
        # 获取版本信息
        if descriptor.ProductRevisionOffset > 0:
            revision = ctypes.string_at(ctypes.addressof(buffer) + descriptor.ProductRevisionOffset)
            print(f"    版本: {revision.decode('utf-8', errors='ignore').strip()}")
    else:
        error = ctypes.windll.kernel32.GetLastError()
        print(f"[FAIL] 错误代码 = {error}")

def scsi_inquiry(handle):
    """使用 SCSI INQUIRY 命令获取设备信息"""
    print("\n方法2: 使用 SCSI INQUIRY...")
    
    sptwb = SCSI_PASS_THROUGH_WITH_BUFFERS()
    sptwb.spt.Length = ctypes.sizeof(SCSI_PASS_THROUGH)
    sptwb.spt.CdbLength = 6
    sptwb.spt.SenseInfoLength = 32
    sptwb.spt.DataIn = 1  # SCSI_IOCTL_DATA_IN
    sptwb.spt.DataTransferLength = 512
    sptwb.spt.TimeOutValue = 30
    sptwb.spt.DataBufferOffset = ctypes.sizeof(SCSI_PASS_THROUGH) + 32
    sptwb.spt.SenseInfoOffset = ctypes.sizeof(SCSI_PASS_THROUGH)
    
    # SCSI INQUIRY 命令
    sptwb.spt.Cdb[0] = 0x12  # INQUIRY
    sptwb.spt.Cdb[1] = 0x00
    sptwb.spt.Cdb[2] = 0x00
    sptwb.spt.Cdb[3] = 0x00
    sptwb.spt.Cdb[4] = 0xFF  # 分配长度
    sptwb.spt.Cdb[5] = 0x00
    
    bytes_returned = wintypes.DWORD()
    
    success = ctypes.windll.kernel32.DeviceIoControl(
        handle,
        IOCTL_SCSI_PASS_THROUGH,
        byref(sptwb), ctypes.sizeof(sptwb),
        byref(sptwb), ctypes.sizeof(sptwb),
        byref(bytes_returned),
        None
    )
    
    if success:
        print("[OK] SCSI INQUIRY 成功")
        # 解析 INQUIRY 数据
        data = bytes(sptwb.DataBuf)
        vendor = data[8:16].decode('ascii', errors='ignore').strip()
        product = data[16:32].decode('ascii', errors='ignore').strip()
        revision = data[32:36].decode('ascii', errors='ignore').strip()
        print(f"    厂商: {vendor}")
        print(f"    产品: {product}")
        print(f"    版本: {revision}")
    else:
        error = ctypes.windll.kernel32.GetLastError()
        print(f"[FAIL] SCSI INQUIRY 失败: 错误代码 = {error}")

def get_device_basic_info(handle):
    """获取设备基本信息"""
    print("\n方法3: 获取设备基本信息...")
    
    # 获取文件大小（对于设备文件可能无效）
    file_size = ctypes.windll.kernel32.GetFileSize(handle, None)
    print(f"    文件大小: {file_size}")
    
    # 获取文件类型
    file_type = ctypes.windll.kernel32.GetFileType(handle)
    file_types = {
        1: "磁盘文件",
        2: "字符设备（控制台等）",
        3: "管道"
    }
    print(f"    文件类型: {file_types.get(file_type, f'未知 ({file_type})')}")

def try_windows_tape_api():
    """尝试使用 Windows Tape API"""
    print("\n方法4: 使用 Windows Tape API...")
    
    try:
        # 加载磁带API相关的函数
        kernel32 = ctypes.windll.kernel32
        
        # 定义函数原型
        kernel32.PrepareTape.restype = wintypes.DWORD
        kernel32.PrepareTape.argtypes = [wintypes.HANDLE, wintypes.DWORD, wintypes.BOOL]
        
        # 尝试准备磁带
        tape_path = r"\\.\Tape0"
        handle = kernel32.CreateFileW(
            tape_path,
            GENERIC_READ | GENERIC_WRITE,
            FILE_SHARE_READ | FILE_SHARE_WRITE,
            None,
            OPEN_EXISTING,
            0,
            None
        )
        
        if handle != INVALID_HANDLE_VALUE:
            print("[OK] 磁带设备已打开")
            
            # 尝试准备磁带（加载）
            result = kernel32.PrepareTape(handle, 1, True)  # TAPE_LOAD
            if result == 0:
                print("[OK] PrepareTape 成功")
            else:
                error = kernel32.GetLastError()
                print(f"[FAIL] PrepareTape 失败: 错误代码 = {error}")
            
            kernel32.CloseHandle(handle)
        else:
            error = kernel32.GetLastError()
            print(f"[FAIL] 无法打开磁带设备: 错误代码 = {error}")
            
    except Exception as e:
        print(f"[FAIL] Windows Tape API 异常: {str(e)}")

def check_device_via_registry():
    """通过注册表检查设备信息"""
    print("\n方法5: 检查注册表信息...")
    
    try:
        import winreg
        
        # 搜索磁带设备
        tape_keys = []
        try:
            key = winreg.OpenKey(winreg.HKEY_LOCAL_MACHINE, r"SYSTEM\CurrentControlSet\Services\Tape")
            i = 0
            while True:
                try:
                    subkey_name = winreg.EnumKey(key, i)
                    tape_keys.append(subkey_name)
                    i += 1
                except WindowsError:
                    break
            winreg.CloseKey(key)
        except:
            pass
        
        if tape_keys:
            print(f"[OK] 找到磁带设备驱动: {tape_keys}")
        else:
            print("[INFO] 未在注册表中找到磁带设备驱动")
            
    except ImportError:
        print("[INFO] 无法导入 winreg 模块")

def main():
    """主测试函数"""
    tape_path = r"\\.\Tape0"
    
    print(f"详细测试磁带设备: {tape_path}")
    print("=" * 60)
    
    # 打开磁带设备
    kernel32 = ctypes.windll.kernel32
    
    handle = kernel32.CreateFileW(
        tape_path,
        GENERIC_READ,
        FILE_SHARE_READ | FILE_SHARE_WRITE,
        None,
        OPEN_EXISTING,
        0,
        None
    )
    
    if handle == INVALID_HANDLE_VALUE:
        error_code = kernel32.GetLastError()
        print(f"[FAIL] 无法打开磁带设备: 错误代码 = {error_code}")
        print("\n尝试其他诊断方法...")
        try_windows_tape_api()
        check_device_via_registry()
        return
    
    print(f"[OK] 磁带设备已打开，句柄 = {handle}")
    
    try:
        # 尝试各种方法获取设备信息
        get_storage_property(handle)
        scsi_inquiry(handle)
        get_device_basic_info(handle)
        
    finally:
        # 关闭句柄
        kernel32.CloseHandle(handle)
        print(f"\n[OK] 磁带设备句柄已关闭")
    
    # 尝试其他方法
    try_windows_tape_api()
    check_device_via_registry()

if __name__ == "__main__":
    main()
    print("\n测试完成")