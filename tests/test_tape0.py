#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
测试 \\\\.\\Tape0 路径
"""

import os
import sys
from pathlib import Path

# 添加项目根目录到路径
sys.path.insert(0, str(Path(__file__).parent.parent))

def test_tape0_path():
    """测试 \\\\.\\Tape0 路径"""
    tape_path = r"\\.\Tape0"
    
    print(f"测试路径: {tape_path}")
    print("-" * 50)
    
    # 方法1: 检查文件是否存在
    print("方法1: 检查文件是否存在...")
    if os.path.exists(tape_path):
        print(f"[OK] 路径存在: {tape_path}")
    else:
        print(f"[FAIL] 路径不存在: {tape_path}")
    
    # 方法2: 尝试打开文件
    print("\n方法2: 尝试打开文件...")
    try:
        with open(tape_path, 'rb') as f:
            print(f"[OK] 可以打开文件: {tape_path}")
            print(f"   文件描述符: {f.fileno()}")
    except Exception as e:
        print(f"[FAIL] 无法打开文件: {str(e)}")
    
    # 方法3: 尝试 CreateFile (Windows API)
    print("\n方法3: 尝试 Windows API CreateFile...")
    try:
        import ctypes
        from ctypes import wintypes
        
        kernel32 = ctypes.windll.kernel32
        
        # 定义常量
        GENERIC_READ = 0x80000000
        GENERIC_WRITE = 0x40000000
        OPEN_EXISTING = 3
        FILE_SHARE_READ = 0x00000001
        FILE_SHARE_WRITE = 0x00000002
        
        # 调用 CreateFile
        handle = kernel32.CreateFileW(
            tape_path,
            GENERIC_READ | GENERIC_WRITE,
            FILE_SHARE_READ | FILE_SHARE_WRITE,
            None,
            OPEN_EXISTING,
            0,
            None
        )
        
        INVALID_HANDLE_VALUE = -1
        if handle != INVALID_HANDLE_VALUE:
            print(f"[OK] CreateFile 成功: 句柄 = {handle}")
            kernel32.CloseHandle(handle)
        else:
            error_code = kernel32.GetLastError()
            print(f"[FAIL] CreateFile 失败: 错误代码 = {error_code}")
            
    except Exception as e:
        print(f"[FAIL] CreateFile 异常: {str(e)}")
    
    print("\n" + "=" * 50)
    print("测试完成")

if __name__ == "__main__":
    test_tape0_path()

