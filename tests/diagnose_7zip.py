#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
7-Zip诊断工具
诊断7z.exe无法正常运行的原因
"""

import os
import sys
import subprocess
import tempfile
from pathlib import Path

# 添加项目根目录到路径
project_root = Path(__file__).parent.parent
sys.path.insert(0, str(project_root))

from config.settings import get_settings


def check_7zip_path():
    """检查7-Zip路径"""
    print("=" * 60)
    print("1. 检查7-Zip路径")
    print("=" * 60)
    
    settings = get_settings()
    sevenzip_path = getattr(settings, 'SEVENZIP_PATH', r"C:\Program Files\7-Zip\7z.exe")
    
    print(f"配置路径: {sevenzip_path}")
    
    path = Path(sevenzip_path)
    if path.exists():
        print(f"[OK] 文件存在")
        print(f"  绝对路径: {path.absolute()}")
        print(f"  文件大小: {path.stat().st_size:,} 字节")
        return str(path.absolute())
    else:
        print(f"[ERROR] 文件不存在")
        # 尝试常见路径
        common_paths = [
            r"C:\Program Files\7-Zip\7z.exe",
            r"C:\Program Files (x86)\7-Zip\7z.exe",
        ]
        for common_path in common_paths:
            if Path(common_path).exists():
                print(f"[INFO] 找到7-Zip: {common_path}")
                return common_path
        return None


def test_simple_command(sevenzip_path):
    """测试简单命令"""
    print("\n" + "=" * 60)
    print("2. 测试简单命令（显示版本）")
    print("=" * 60)
    
    try:
        result = subprocess.run(
            [sevenzip_path],
            capture_output=True,
            text=True,
            timeout=10
        )
        print(f"返回码: {result.returncode}")
        if result.stdout:
            print(f"标准输出:\n{result.stdout[:500]}")
        if result.stderr:
            print(f"错误输出:\n{result.stderr[:500]}")
        return result.returncode == 0 or result.returncode == 7
    except Exception as e:
        print(f"[ERROR] 执行失败: {str(e)}")
        import traceback
        traceback.print_exc()
        return False


def test_compression_command(sevenzip_path):
    """测试压缩命令"""
    print("\n" + "=" * 60)
    print("3. 测试压缩命令（带参数）")
    print("=" * 60)
    
    with tempfile.TemporaryDirectory() as temp_dir:
        temp_path = Path(temp_dir)
        test_file = temp_path / "test.txt"
        archive_path = temp_path / "test.7z"
        
        # 创建测试文件
        test_file.write_text("This is a test file for 7-Zip compression.\n" * 100)
        print(f"测试文件: {test_file}")
        print(f"文件大小: {test_file.stat().st_size:,} 字节")
        
        # 构建命令（模拟实际使用的命令）
        cmd = [
            sevenzip_path,
            "a",
            "-mmt4",
            "-mx9",
            "-md1g",
            "-y",
            str(archive_path.absolute()),
            str(test_file.absolute())
        ]
        
        print(f"\n执行命令:")
        print(f"  {' '.join(cmd)}")
        print(f"  工作目录: {temp_path}")
        
        try:
            result = subprocess.run(
                cmd,
                cwd=str(temp_path),
                capture_output=True,
                text=True,
                timeout=60
            )
            
            print(f"\n返回码: {result.returncode}")
            
            if result.stdout:
                print(f"\n标准输出:")
                print(result.stdout)
            
            if result.stderr:
                print(f"\n错误输出:")
                print(result.stderr)
            
            if result.returncode == 0:
                if archive_path.exists():
                    print(f"\n[OK] 压缩成功!")
                    print(f"  压缩包大小: {archive_path.stat().st_size:,} 字节")
                    return True
                else:
                    print(f"\n[ERROR] 压缩包文件不存在")
                    return False
            else:
                print(f"\n[ERROR] 命令执行失败，返回码: {result.returncode}")
                return False
                
        except subprocess.TimeoutExpired:
            print(f"\n[ERROR] 命令执行超时")
            return False
        except Exception as e:
            print(f"\n[ERROR] 执行异常: {str(e)}")
            import traceback
            traceback.print_exc()
            return False


def test_work_directory_method(sevenzip_path):
    """测试工作目录方法（模拟实际代码）"""
    print("\n" + "=" * 60)
    print("4. 测试工作目录方法（模拟实际代码）")
    print("=" * 60)
    
    with tempfile.TemporaryDirectory() as temp_dir:
        temp_path = Path(temp_dir)
        work_dir = temp_path / ".7z_work_test"
        work_dir.mkdir(parents=True, exist_ok=True)
        
        test_file = temp_path / "source.txt"
        test_file.write_text("This is a test file.\n" * 100)
        
        # 复制文件到工作目录
        target_file = work_dir / "test.txt"
        import shutil
        shutil.copy2(test_file, target_file)
        
        archive_path = temp_path / "test.7z"
        
        # 构建命令（使用工作目录方法）
        cmd = [
            sevenzip_path,
            "a",
            "-mmt4",
            "-mx9",
            "-md1g",
            "-y",
            str(archive_path.absolute()),
            "*"  # 压缩当前目录下的所有文件
        ]
        
        print(f"工作目录: {work_dir}")
        print(f"执行命令: {' '.join(cmd)}")
        
        try:
            result = subprocess.run(
                cmd,
                cwd=str(work_dir),
                capture_output=True,
                text=True,
                timeout=60
            )
            
            print(f"\n返回码: {result.returncode}")
            
            if result.stdout:
                print(f"\n标准输出:")
                print(result.stdout)
            
            if result.stderr:
                print(f"\n错误输出:")
                print(result.stderr)
            
            if result.returncode == 0:
                if archive_path.exists():
                    print(f"\n[OK] 压缩成功!")
                    print(f"  压缩包大小: {archive_path.stat().st_size:,} 字节")
                    return True
                else:
                    print(f"\n[ERROR] 压缩包文件不存在")
                    return False
            else:
                print(f"\n[ERROR] 命令执行失败，返回码: {result.returncode}")
                return False
                
        except Exception as e:
            print(f"\n[ERROR] 执行异常: {str(e)}")
            import traceback
            traceback.print_exc()
            return False


def check_environment():
    """检查环境变量"""
    print("\n" + "=" * 60)
    print("5. 检查环境变量")
    print("=" * 60)
    
    print(f"PATH: {os.environ.get('PATH', '')[:200]}")
    print(f"TEMP: {os.environ.get('TEMP', '')}")
    print(f"TMP: {os.environ.get('TMP', '')}")


def main():
    """主函数"""
    print("7-Zip诊断工具")
    print("=" * 60)
    
    # 检查路径
    sevenzip_path = check_7zip_path()
    if not sevenzip_path:
        print("\n[ERROR] 无法找到7-Zip程序，请检查安装")
        return 1
    
    # 测试简单命令
    if not test_simple_command(sevenzip_path):
        print("\n[WARNING] 简单命令测试失败")
    
    # 测试压缩命令
    if not test_compression_command(sevenzip_path):
        print("\n[ERROR] 压缩命令测试失败")
        return 1
    
    # 测试工作目录方法
    if not test_work_directory_method(sevenzip_path):
        print("\n[ERROR] 工作目录方法测试失败")
        return 1
    
    # 检查环境
    check_environment()
    
    print("\n" + "=" * 60)
    print("[OK] 所有测试通过！7-Zip可以正常工作")
    print("=" * 60)
    return 0


if __name__ == "__main__":
    sys.exit(main())

