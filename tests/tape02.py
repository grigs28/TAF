#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
IBM ULT3580-HH9 磁带驱动器操作控制
支持弹出、擦除、格式化等操作，包含磁带参数获取
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

# Windows Tape API 常量
TAPE_LOAD = 0
TAPE_UNLOAD = 1
TAPE_TENSION = 2
TAPE_LOCK = 3
TAPE_UNLOCK = 4
TAPE_FORMAT = 5

TAPE_ERASE_SHORT = 0
TAPE_ERASE_LONG = 1

# Tape API 函数定义
kernel32 = ctypes.windll.kernel32

# 定义函数原型
kernel32.PrepareTape.restype = wintypes.DWORD
kernel32.PrepareTape.argtypes = [wintypes.HANDLE, wintypes.DWORD, wintypes.BOOL]

kernel32.EraseTape.restype = wintypes.DWORD
kernel32.EraseTape.argtypes = [wintypes.HANDLE, wintypes.DWORD, wintypes.BOOL]

kernel32.WriteTapemark.restype = wintypes.DWORD
kernel32.WriteTapemark.argtypes = [wintypes.HANDLE, wintypes.DWORD, wintypes.DWORD, wintypes.BOOL]

kernel32.SetTapePosition.restype = wintypes.DWORD
kernel32.SetTapePosition.argtypes = [wintypes.HANDLE, wintypes.DWORD, wintypes.DWORD, wintypes.DWORD, wintypes.DWORD, wintypes.BOOL]

# 磁带参数结构体
class TAPE_GET_DRIVE_PARAMETERS(Structure):
    _fields_ = [
        ("ECC", wintypes.BOOLEAN),
        ("Compression", wintypes.BOOLEAN),
        ("DataPadding", wintypes.BOOLEAN),
        ("ReportSetmarks", wintypes.BOOLEAN),
        ("DefaultBlockSize", wintypes.DWORD),
        ("MaximumBlockSize", wintypes.DWORD),
        ("MinimumBlockSize", wintypes.DWORD),
        ("MaximumPartitionCount", wintypes.DWORD),
        ("FeaturesLow", wintypes.DWORD),
        ("FeaturesHigh", wintypes.DWORD),
        ("EOTWarningZoneSize", wintypes.DWORD)
    ]

class TAPE_GET_MEDIA_PARAMETERS(Structure):
    _fields_ = [
        ("Capacity", ctypes.c_ulonglong),
        ("Remaining", ctypes.c_ulonglong),
        ("BlockSize", wintypes.DWORD),
        ("PartitionCount", wintypes.DWORD),
        ("WriteProtected", wintypes.BOOLEAN)
    ]

class TapeOperations:
    def __init__(self, tape_path=r"\\.\Tape0"):
        self.tape_path = tape_path
        self.handle = INVALID_HANDLE_VALUE
        
    def open_tape(self):
        """打开磁带设备"""
        if self.handle != INVALID_HANDLE_VALUE:
            self.close_tape()
            
        self.handle = kernel32.CreateFileW(
            self.tape_path,
            GENERIC_READ | GENERIC_WRITE,
            FILE_SHARE_READ | FILE_SHARE_WRITE,
            None,
            OPEN_EXISTING,
            0,
            None
        )
        
        if self.handle == INVALID_HANDLE_VALUE:
            error = kernel32.GetLastError()
            raise Exception(f"无法打开磁带设备: 错误代码 = {error}")
            
        print(f"[OK] 磁带设备已打开: {self.tape_path}")
        return True
    
    def close_tape(self):
        """关闭磁带设备"""
        if self.handle != INVALID_HANDLE_VALUE:
            kernel32.CloseHandle(self.handle)
            self.handle = INVALID_HANDLE_VALUE
            print("[OK] 磁带设备已关闭")
    
    def get_drive_parameters(self):
        """获取驱动器参数"""
        if self.handle == INVALID_HANDLE_VALUE:
            self.open_tape()
            
        print("\n获取驱动器参数...")
        
        drive_params = TAPE_GET_DRIVE_PARAMETERS()
        bytes_returned = wintypes.DWORD()
        
        # 使用 IOCTL 获取驱动器参数
        IOCTL_TAPE_GET_DRIVE_PARAMS = 0x00070010
        
        success = kernel32.DeviceIoControl(
            self.handle,
            IOCTL_TAPE_GET_DRIVE_PARAMS,
            None, 0,
            byref(drive_params), ctypes.sizeof(drive_params),
            byref(bytes_returned),
            None
        )
        
        if success:
            print("[OK] 驱动器参数:")
            print(f"    ECC 支持: {'是' if drive_params.ECC else '否'}")
            print(f"   压缩支持: {'是' if drive_params.Compression else '否'}")
            print(f"   数据填充: {'是' if drive_params.DataPadding else '否'}")
            print(f"   报告集合标记: {'是' if drive_params.ReportSetmarks else '否'}")
            print(f"   默认块大小: {drive_params.DefaultBlockSize} 字节")
            print(f"   最小块大小: {drive_params.MinimumBlockSize} 字节")
            print(f"   最大块大小: {drive_params.MaximumBlockSize} 字节")
            print(f"   最大分区数: {drive_params.MaximumPartitionCount}")
            print(f"   EOT警告区大小: {drive_params.EOTWarningZoneSize}")
            
            # 解析特性位
            features_low = drive_params.FeaturesLow
            features = []
            if features_low & 0x1: features.append("TAPE_DRIVE_FIXED")
            if features_low & 0x2: features.append("TAPE_DRIVE_SELECT")
            if features_low & 0x4: features.append("TAPE_DRIVE_INITIATOR")
            if features_low & 0x8: features.append("TAPE_DRIVE_ECC")
            if features_low & 0x10: features.append("TAPE_DRIVE_COMPRESSION")
            if features_low & 0x20: features.append("TAPE_DRIVE_PADDING")
            if features_low & 0x40: features.append("TAPE_DRIVE_REPORT_SMKS")
            if features_low & 0x80: features.append("TAPE_DRIVE_GET_ABSOLUTE_BLK")
            if features_low & 0x100: features.append("TAPE_DRIVE_GET_LOGICAL_BLK")
            
            if features:
                print(f"   驱动器特性: {', '.join(features)}")
                
            return drive_params
        else:
            error = kernel32.GetLastError()
            print(f"[INFO] 无法获取驱动器参数: 错误代码 = {error}")
            return None
    
    def get_media_parameters(self):
        """获取介质参数"""
        if self.handle == INVALID_HANDLE_VALUE:
            self.open_tape()
            
        print("\n获取介质参数...")
        
        media_params = TAPE_GET_MEDIA_PARAMETERS()
        bytes_returned = wintypes.DWORD()
        
        # 使用 IOCTL 获取介质参数
        IOCTL_TAPE_GET_MEDIA_PARAMS = 0x00070014
        
        success = kernel32.DeviceIoControl(
            self.handle,
            IOCTL_TAPE_GET_MEDIA_PARAMS,
            None, 0,
            byref(media_params), ctypes.sizeof(media_params),
            byref(bytes_returned),
            None
        )
        
        if success:
            print("[OK] 介质参数:")
            
            # 转换为更易读的格式
            if media_params.Capacity > 0:
                capacity_gb = media_params.Capacity / (1024**3)
                remaining_gb = media_params.Remaining / (1024**3)
                used_gb = capacity_gb - remaining_gb
                usage_percent = (used_gb / capacity_gb) * 100 if capacity_gb > 0 else 0
                
                print(f"   总容量: {capacity_gb:.2f} GB ({media_params.Capacity} 字节)")
                print(f"   剩余容量: {remaining_gb:.2f} GB ({media_params.Remaining} 字节)")
                print(f"   已使用: {used_gb:.2f} GB ({usage_percent:.1f}%)")
            else:
                print(f"   总容量: {media_params.Capacity} 字节")
                print(f"   剩余容量: {media_params.Remaining} 字节")
                
            print(f"   块大小: {media_params.BlockSize} 字节")
            print(f"   分区数: {media_params.PartitionCount}")
            print(f"   写保护: {'是' if media_params.WriteProtected else '否'}")
            
            return media_params
        else:
            error = kernel32.GetLastError()
            print(f"[INFO] 无法获取介质参数: 错误代码 = {error}")
            return None
    
    def get_tape_info(self):
        """获取磁带和驱动器信息"""
        print("\n" + "="*50)
        print("磁带驱动器信息")
        print("="*50)
        
        # 获取驱动器参数
        drive_params = self.get_drive_parameters()
        
        # 获取介质参数
        media_params = self.get_media_parameters()
        
        # 如果标准方法失败，尝试备用方法
        if not drive_params or not media_params:
            print("\n使用备用方法获取信息...")
            self._get_tape_info_alternative()
    
    def _get_tape_info_alternative(self):
        """备用方法获取磁带信息"""
        try:
            # 尝试使用存储设备查询
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
            
            success = kernel32.DeviceIoControl(
                self.handle,
                IOCTL_STORAGE_QUERY_PROPERTY,
                byref(query), ctypes.sizeof(query),
                buffer, buffer_size,
                byref(bytes_returned),
                None
            )
            
            if success:
                descriptor = STORAGE_DEVICE_DESCRIPTOR.from_buffer(buffer)
                print("[OK] 存储设备信息:")
                
                # 获取厂商信息
                if descriptor.VendorIdOffset > 0:
                    vendor_id = ctypes.string_at(ctypes.addressof(buffer) + descriptor.VendorIdOffset)
                    print(f"   厂商: {vendor_id.decode('utf-8', errors='ignore').strip()}")
                
                # 获取产品信息
                if descriptor.ProductIdOffset > 0:
                    product_id = ctypes.string_at(ctypes.addressof(buffer) + descriptor.ProductIdOffset)
                    print(f"   产品: {product_id.decode('utf-8', errors='ignore').strip()}")
                
                # 获取版本信息
                if descriptor.ProductRevisionOffset > 0:
                    revision = ctypes.string_at(ctypes.addressof(buffer) + descriptor.ProductRevisionOffset)
                    print(f"   版本: {revision.decode('utf-8', errors='ignore').strip()}")
                
                print(f"   设备类型: {descriptor.DeviceType}")
                print(f"   可移动介质: {'是' if descriptor.RemovableMedia else '否'}")
                print(f"   总线类型: {descriptor.BusType}")
        except Exception as e:
            print(f"[INFO] 备用方法也失败: {str(e)}")
    
    def load_tape(self):
        """加载磁带"""
        if self.handle == INVALID_HANDLE_VALUE:
            self.open_tape()
            
        result = kernel32.PrepareTape(self.handle, TAPE_LOAD, True)
        if result == 0:
            print("[OK] 磁带加载成功")
            return True
        else:
            error = kernel32.GetLastError()
            print(f"[FAIL] 磁带加载失败: 错误代码 = {error}")
            return False
    
    def unload_tape(self):
        """弹出/卸载磁带"""
        if self.handle == INVALID_HANDLE_VALUE:
            self.open_tape()
            
        result = kernel32.PrepareTape(self.handle, TAPE_UNLOAD, True)
        if result == 0:
            print("[OK] 磁带弹出成功")
            return True
        else:
            error = kernel32.GetLastError()
            print(f"[FAIL] 磁带弹出失败: 错误代码 = {error}")
            return False
    
    def erase_tape_short(self):
        """快速擦除磁带"""
        if self.handle == INVALID_HANDLE_VALUE:
            self.open_tape()
            
        result = kernel32.EraseTape(self.handle, TAPE_ERASE_SHORT, True)
        if result == 0:
            print("[OK] 快速擦除成功")
            return True
        else:
            error = kernel32.GetLastError()
            print(f"[FAIL] 快速擦除失败: 错误代码 = {error}")
            return False
    
    def erase_tape_long(self):
        """完全擦除磁带（安全擦除）"""
        if self.handle == INVALID_HANDLE_VALUE:
            self.open_tape()
            
        result = kernel32.EraseTape(self.handle, TAPE_ERASE_LONG, True)
        if result == 0:
            print("[OK] 完全擦除成功")
            return True
        else:
            error = kernel32.GetLastError()
            print(f"[FAIL] 完全擦除失败: 错误代码 = {error}")
            return False
    
    def lock_tape(self):
        """锁定磁带门"""
        if self.handle == INVALID_HANDLE_VALUE:
            self.open_tape()
            
        result = kernel32.PrepareTape(self.handle, TAPE_LOCK, True)
        if result == 0:
            print("[OK] 磁带门锁定成功")
            return True
        else:
            error = kernel32.GetLastError()
            print(f"[FAIL] 磁带门锁定失败: 错误代码 = {error}")
            return False
    
    def unlock_tape(self):
        """解锁磁带门"""
        if self.handle == INVALID_HANDLE_VALUE:
            self.open_tape()
            
        result = kernel32.PrepareTape(self.handle, TAPE_UNLOCK, True)
        if result == 0:
            print("[OK] 磁带门解锁成功")
            return True
        else:
            error = kernel32.GetLastError()
            print(f"[FAIL] 磁带门解锁失败: 错误代码 = {error}")
            return False
    
    def tension_tape(self):
        """张力磁带（改善磁带与磁头接触）"""
        if self.handle == INVALID_HANDLE_VALUE:
            self.open_tape()
            
        result = kernel32.PrepareTape(self.handle, TAPE_TENSION, True)
        if result == 0:
            print("[OK] 磁带张力操作成功")
            return True
        else:
            error = kernel32.GetLastError()
            print(f"[FAIL] 磁带张力操作失败: 错误代码 = {error}")
            return False
    
    def write_filemark(self, count=1):
        """写入文件标记"""
        if self.handle == INVALID_HANDLE_VALUE:
            self.open_tape()
            
        result = kernel32.WriteTapemark(self.handle, 0, count, True)  # 0 = 文件标记
        if result == 0:
            print(f"[OK] 写入 {count} 个文件标记成功")
            return True
        else:
            error = kernel32.GetLastError()
            print(f"[FAIL] 写入文件标记失败: 错误代码 = {error}")
            return False
    
    def write_setmark(self, count=1):
        """写入集合标记"""
        if self.handle == INVALID_HANDLE_VALUE:
            self.open_tape()
            
        result = kernel32.WriteTapemark(self.handle, 1, count, True)  # 1 = 集合标记
        if result == 0:
            print(f"[OK] 写入 {count} 个集合标记成功")
            return True
        else:
            error = kernel32.GetLastError()
            print(f"[FAIL] 写入集合标记失败: 错误代码 = {error}")
            return False
    
    def rewind_tape(self):
        """倒带（回到磁带开始位置）"""
        if self.handle == INVALID_HANDLE_VALUE:
            self.open_tape()
            
        result = kernel32.SetTapePosition(self.handle, 0, 0, 0, 0, True)  # 0 = 倒带
        if result == 0:
            print("[OK] 倒带成功")
            return True
        else:
            error = kernel32.GetLastError()
            print(f"[FAIL] 倒带失败: 错误代码 = {error}")
            return False
    
    def get_tape_status(self):
        """获取磁带状态"""
        print("\n获取磁带状态...")
        
        if self.handle == INVALID_HANDLE_VALUE:
            try:
                self.open_tape()
                print("[OK] 磁带设备状态: 就绪")
                return True
            except:
                print("[FAIL] 磁带设备状态: 无法访问")
                return False
        else:
            print("[OK] 磁带设备状态: 已打开")
            return True

def main():
    """主函数 - 提供用户交互界面"""
    tape = TapeOperations()
    
    print("IBM ULT3580-HH9 磁带驱动器操作控制")
    print("=" * 50)
    
    try:
        # 首先尝试打开设备检查状态
        if not tape.get_tape_status():
            print("请检查磁带驱动器是否连接并开启")
            return
        
        while True:
            print("\n请选择操作:")
            print("1. 加载磁带")
            print("2. 弹出磁带")
            print("3. 快速擦除")
            print("4. 完全擦除")
            print("5. 锁定磁带门")
            print("6. 解锁磁带门")
            print("7. 张力磁带")
            print("8. 写入文件标记")
            print("9. 写入集合标记")
            print("10. 倒带")
            print("11. 获取磁带信息")
            print("12. 检查状态")
            print("0. 退出")
            
            try:
                choice = input("\n请输入选择 (0-12): ").strip()
            except (EOFError, KeyboardInterrupt):
                print("\n\n检测到退出信号")
                break
            
            if choice == '0':
                break
            elif choice == '1':
                tape.load_tape()
            elif choice == '2':
                tape.unload_tape()
            elif choice == '3':
                confirm = input("确定要快速擦除磁带吗？(y/N): ")
                if confirm.lower() == 'y':
                    tape.erase_tape_short()
            elif choice == '4':
                confirm = input("确定要完全擦除磁带吗？此操作不可逆！(y/N): ")
                if confirm.lower() == 'y':
                    tape.erase_tape_long()
            elif choice == '5':
                tape.lock_tape()
            elif choice == '6':
                tape.unlock_tape()
            elif choice == '7':
                tape.tension_tape()
            elif choice == '8':
                count = input("输入文件标记数量 (默认 1): ")
                try:
                    count = int(count) if count else 1
                    tape.write_filemark(count)
                except ValueError:
                    print("无效的数字")
            elif choice == '9':
                count = input("输入集合标记数量 (默认 1): ")
                try:
                    count = int(count) if count else 1
                    tape.write_setmark(count)
                except ValueError:
                    print("无效的数字")
            elif choice == '10':
                tape.rewind_tape()
            elif choice == '11':
                tape.get_tape_info()
            elif choice == '12':
                tape.get_tape_status()
            else:
                print("无效的选择")
                
            # 每次操作后等待用户按回车继续
            try:
                input("\n按回车键继续...")
            except (EOFError, KeyboardInterrupt):
                print("\n\n检测到退出信号")
                break
                
    except KeyboardInterrupt:
        print("\n\n用户中断操作")
    except Exception as e:
        print(f"\n发生错误: {str(e)}")
    finally:
        tape.close_tape()
        print("\n程序结束")

def test_all_operations():
    """测试所有操作"""
    print("测试所有磁带操作...")
    tape = TapeOperations()
    
    try:
        tape.open_tape()
        
        # 测试各种操作
        operations = [
            ("倒带", tape.rewind_tape),
            ("张力磁带", tape.tension_tape),
            ("写入文件标记", lambda: tape.write_filemark(1)),
            ("锁定磁带门", tape.lock_tape),
            ("解锁磁带门", tape.unlock_tape),
        ]
        
        for op_name, op_func in operations:
            print(f"\n测试: {op_name}")
            try:
                op_func()
            except Exception as e:
                print(f"操作失败: {str(e)}")
        
    finally:
        tape.close_tape()

if __name__ == "__main__":
    if len(sys.argv) > 1 and sys.argv[1] == "test":
        test_all_operations()
    else:
        main()