#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
7-Zip命令行工具测试程序
Test program for 7-Zip command line tool
"""

import os
import sys
import subprocess
import tempfile
import shutil
from pathlib import Path
from typing import Optional, Dict, Any

# 添加项目根目录到路径
project_root = Path(__file__).parent.parent
sys.path.insert(0, str(project_root))

try:
    import psutil
    PSUTIL_AVAILABLE = True
except ImportError:
    PSUTIL_AVAILABLE = False
    print("警告: psutil未安装，无法检查系统内存")

from config.settings import get_settings


def check_7zip_exists(sevenzip_path: str) -> bool:
    """检查7-Zip程序是否存在"""
    path = Path(sevenzip_path)
    if path.exists():
        print(f"[OK] 7-Zip程序存在: {sevenzip_path}")
        return True
    else:
        print(f"[ERROR] 7-Zip程序不存在: {sevenzip_path}")
        return False


def test_7zip_version(sevenzip_path: str) -> Optional[str]:
    """测试7-Zip版本信息"""
    try:
        result = subprocess.run(
            [sevenzip_path],
            capture_output=True,
            text=True,
            timeout=10
        )
        if result.returncode == 0 or result.returncode == 7:  # 7-Zip返回7表示显示帮助信息
            # 从输出中提取版本信息
            for line in result.stdout.split('\n'):
                if '7-Zip' in line and 'Copyright' in line:
                    version = line.split('7-Zip')[1].split(':')[0].strip()
                    print(f"[OK] 7-Zip版本: {version}")
                    return version
        print(f"[ERROR] 无法获取7-Zip版本信息")
        print(f"  返回码: {result.returncode}")
        print(f"  输出: {result.stdout[:200]}")
        return None
    except Exception as e:
        print(f"[ERROR] 测试7-Zip版本失败: {str(e)}")
        return None


def get_system_memory() -> Dict[str, float]:
    """获取系统内存信息"""
    if not PSUTIL_AVAILABLE:
        return {}
    
    try:
        mem = psutil.virtual_memory()
        return {
            'total_gb': mem.total / (1024 ** 3),
            'available_gb': mem.available / (1024 ** 3),
            'used_gb': mem.used / (1024 ** 3),
            'percent': mem.percent
        }
    except Exception as e:
        print(f"警告: 无法获取系统内存信息: {str(e)}")
        return {}


def calculate_memory_allocation(dict_size_str: str, threads: int) -> Dict[str, Any]:
    """计算内存分配（与compressor.py中的逻辑一致）"""
    # 解析字典大小
    dict_size_gb = 1.0
    try:
        dict_size_str_lower = dict_size_str.lower().strip()
        if dict_size_str_lower.endswith('g'):
            dict_size_gb = float(dict_size_str_lower[:-1])
        elif dict_size_str_lower.endswith('m'):
            dict_size_gb = float(dict_size_str_lower[:-1]) / 1024.0
        elif dict_size_str_lower.endswith('k'):
            dict_size_gb = float(dict_size_str_lower[:-1]) / (1024.0 * 1024.0)
        else:
            dict_size_gb = float(dict_size_str_lower) / 1024.0
    except (ValueError, TypeError):
        dict_size_gb = 1.0
    
    dict_size_gb = float(dict_size_gb)
    threads = int(threads)
    
    # 计算理论内存需求
    calculated_memory_gb = dict_size_gb * threads * 1.5
    
    # 如果psutil可用，检查系统内存并调整
    if PSUTIL_AVAILABLE:
        try:
            mem = psutil.virtual_memory()
            total_memory_gb = mem.total / (1024 ** 3)
            available_memory_gb = mem.available / (1024 ** 3)
            reserved_memory_gb = total_memory_gb * 0.2
            usable_memory_gb = available_memory_gb - reserved_memory_gb
            
            # 如果理论需求超过可用内存，调整字典大小
            if calculated_memory_gb > usable_memory_gb:
                max_dict_size_gb = usable_memory_gb / (threads * 1.5)
                dict_sizes_gb = [
                    16/1024, 32/1024, 64/1024, 128/1024, 256/1024, 512/1024,
                    1, 2, 4, 8, 16, 32, 64
                ]
                adjusted_dict_size_gb = 16/1024
                for ds in dict_sizes_gb:
                    if ds <= max_dict_size_gb:
                        adjusted_dict_size_gb = ds
                    else:
                        break
                
                if adjusted_dict_size_gb < dict_size_gb:
                    dict_size_gb = adjusted_dict_size_gb
                    if dict_size_gb >= 1:
                        dict_size_str = f"{int(dict_size_gb)}g"
                    else:
                        dict_size_str = f"{int(dict_size_gb * 1024)}m"
            
            # 重新计算内存需求
            memory_gb = int(dict_size_gb * threads * 1.5)
            max_memory_gb = int(usable_memory_gb * 0.9)
            if memory_gb > max_memory_gb:
                memory_gb = max_memory_gb
            
            min_memory_gb = max(4, int(total_memory_gb * 0.05))
            memory_gb = max(min_memory_gb, memory_gb)
            
            return {
                'dict_size_str': dict_size_str,
                'dict_size_gb': dict_size_gb,
                'threads': threads,
                'memory_gb': memory_gb,
                'calculated_memory_gb': calculated_memory_gb,
                'total_memory_gb': total_memory_gb,
                'usable_memory_gb': usable_memory_gb
            }
        except Exception as e:
            print(f"警告: 内存计算失败: {str(e)}")
    
    # 默认策略
    memory_gb = max(16, min(64, int(calculated_memory_gb)))
    return {
        'dict_size_str': dict_size_str,
        'dict_size_gb': dict_size_gb,
        'threads': threads,
        'memory_gb': memory_gb,
        'calculated_memory_gb': calculated_memory_gb
    }


def test_7zip_compression(
    sevenzip_path: str,
    dict_size_str: str,
    threads: int,
    memory_gb: int,
    compression_level: int = 9
) -> bool:
    """测试7-Zip压缩功能"""
    print(f"\n测试7-Zip压缩功能:")
    print(f"  字典大小: {dict_size_str}")
    print(f"  线程数: {threads}")
    print(f"  预计内存使用: {memory_gb}GB (7-Zip自动分配)")
    print(f"  压缩级别: {compression_level}")
    print(f"  注意: 7-Zip不支持-mmem参数，会根据字典大小和线程数自动分配内存")
    
    # 创建临时目录和测试文件
    with tempfile.TemporaryDirectory() as temp_dir:
        temp_path = Path(temp_dir)
        test_file = temp_path / "test_file.txt"
        archive_path = temp_path / "test.7z"
        
        # 创建测试文件（1MB）
        test_content = "This is a test file for 7-Zip compression.\n" * 10000
        test_file.write_text(test_content, encoding='utf-8')
        original_size = test_file.stat().st_size
        
        # 先测试简单命令（不带内存和字典参数）
        print(f"\n  先测试简单命令（不带内存和字典参数）...")
        simple_cmd = [
            str(Path(sevenzip_path).absolute()),
            "a",
            f"-mmt{threads}",
            f"-mx{compression_level}",
            "-y",
            str((archive_path.parent / "test_simple.7z").absolute()),
            str(test_file.absolute())
        ]
        
        try:
            simple_result = subprocess.run(
                simple_cmd,
                capture_output=True,
                text=True,
                timeout=30
            )
            if simple_result.returncode == 0:
                print(f"  [OK] 简单命令测试成功")
            else:
                print(f"  [WARNING] 简单命令测试失败，返回码: {simple_result.returncode}")
                print(f"  错误输出: {simple_result.stderr[:300]}")
        except Exception as e:
            print(f"  [WARNING] 简单命令测试异常: {str(e)}")
        
        # 测试带字典参数的压缩（7-Zip不支持-mmem参数，会根据字典大小和线程数自动分配内存）
        test_cases = [
            {
                "name": "带字典参数（-md）",
                "cmd": [
                    str(Path(sevenzip_path).absolute()),
                    "a",
                    f"-mmt{threads}",
                    f"-mx{compression_level}",
                    f"-md{dict_size_str}",
                    "-y",
                    str(archive_path.absolute()),
                    str(test_file.absolute())
                ]
            }
        ]
        
        success_count = 0
        for test_case in test_cases:
            print(f"\n  测试: {test_case['name']}")
            print(f"    命令: {' '.join(test_case['cmd'])}")
            try:
                result = subprocess.run(
                    test_case['cmd'],
                    capture_output=True,
                    text=True,
                    timeout=30
                )
                if result.returncode == 0:
                    test_archive = Path(test_case['cmd'][-2])
                    if test_archive.exists():
                        print(f"    [OK] 测试成功")
                        success_count += 1
                    else:
                        print(f"    [ERROR] 压缩包文件不存在")
                else:
                    print(f"    [ERROR] 返回码: {result.returncode}")
                    if result.stderr:
                        print(f"    错误输出: {result.stderr[:200]}")
            except Exception as e:
                print(f"    [ERROR] 异常: {str(e)}")
        
        # 使用最后一个测试（完整命令）的结果
        cmd = test_cases[-1]['cmd']
        print(f"\n  最终测试结果: {success_count}/{len(test_cases)} 个测试通过")
        
        # 检查最终压缩包是否存在
        if archive_path.exists():
            archive_size = archive_path.stat().st_size
            compression_ratio = (1 - archive_size / original_size) * 100
            print(f"[OK] 压缩成功!")
            print(f"  原始大小: {original_size:,} 字节")
            print(f"  压缩后大小: {archive_size:,} 字节")
            print(f"  压缩率: {compression_ratio:.2f}%")
            return success_count > 0
        else:
            print(f"[ERROR] 最终压缩包文件不存在")
            return False


def main():
    """主测试函数"""
    print("=" * 60)
    print("7-Zip命令行工具测试程序")
    print("=" * 60)
    
    # 获取配置
    settings = get_settings()
    sevenzip_path = getattr(settings, 'SEVENZIP_PATH', r"C:\Program Files\7-Zip\7z.exe")
    dictionary_size = getattr(settings, 'COMPRESSION_DICTIONARY_SIZE', '1g')
    
    # 获取线程数
    compression_command_threads = getattr(settings, 'COMPRESSION_COMMAND_THREADS', None)
    if compression_command_threads is None:
        compression_command_threads = getattr(settings, 'WEB_WORKERS', 4)
    try:
        compression_command_threads = int(compression_command_threads)
    except (ValueError, TypeError):
        compression_command_threads = 4
    
    print(f"\n配置信息:")
    print(f"  7-Zip路径: {sevenzip_path}")
    print(f"  字典大小: {dictionary_size}")
    print(f"  线程数: {compression_command_threads}")
    
    # 检查7-Zip程序
    print(f"\n1. 检查7-Zip程序...")
    if not check_7zip_exists(sevenzip_path):
        print("\n请检查7-Zip安装路径是否正确")
        return 1
    
    # 测试版本
    print(f"\n2. 测试7-Zip版本...")
    version = test_7zip_version(sevenzip_path)
    if not version:
        print("\n无法获取7-Zip版本，可能程序无法正常运行")
        return 1
    
    # 检查系统内存
    print(f"\n3. 检查系统内存...")
    if PSUTIL_AVAILABLE:
        mem_info = get_system_memory()
        if mem_info:
            print(f"  总内存: {mem_info['total_gb']:.1f} GB")
            print(f"  可用内存: {mem_info['available_gb']:.1f} GB")
            print(f"  已用内存: {mem_info['used_gb']:.1f} GB ({mem_info['percent']:.1f}%)")
    else:
        print("  psutil未安装，跳过内存检查")
    
    # 计算内存分配
    print(f"\n4. 计算内存分配...")
    mem_allocation = calculate_memory_allocation(dictionary_size, compression_command_threads)
    print(f"  字典大小: {mem_allocation['dict_size_str']} ({mem_allocation['dict_size_gb']:.3f} GB)")
    print(f"  线程数: {mem_allocation['threads']}")
    print(f"  理论内存需求: {mem_allocation['calculated_memory_gb']:.1f} GB")
    print(f"  最终分配内存: {mem_allocation['memory_gb']} GB")
    if 'usable_memory_gb' in mem_allocation:
        print(f"  系统可用内存: {mem_allocation['usable_memory_gb']:.1f} GB")
    
    # 测试压缩功能
    print(f"\n5. 测试压缩功能...")
    success = test_7zip_compression(
        sevenzip_path,
        mem_allocation['dict_size_str'],
        mem_allocation['threads'],
        mem_allocation['memory_gb'],
        compression_level=9
    )
    
    # 总结
    print(f"\n" + "=" * 60)
    if success:
        print("[OK] 所有测试通过！7-Zip可以正常工作")
        return 0
    else:
        print("[ERROR] 测试失败，请检查7-Zip配置和系统环境")
        return 1


if __name__ == "__main__":
    sys.exit(main())

