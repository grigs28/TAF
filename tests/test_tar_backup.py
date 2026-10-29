#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
简单的tar备份测试
"""

import os
import sys
import tarfile
import subprocess
from pathlib import Path
from datetime import datetime

# 添加项目根目录到路径
sys.path.insert(0, str(Path(__file__).parent.parent))

from config.settings import get_settings

def test_tar_backup():
    """测试简单的tar备份"""
    settings = get_settings()
    
    # 1. 源路径（测试用）
    source_path = Path("tests")  # 使用tests目录作为测试源
    if not source_path.exists():
        source_path = Path(".")  # 如果tests不存在，使用当前目录
    
    # 2. 磁带盘符路径
    tape_drive = settings.TAPE_DRIVE_LETTER.upper() + ":\\"
    
    print("=" * 60)
    print("简单的tar备份测试")
    print("=" * 60)
    print(f"源路径: {source_path.absolute()}")
    print(f"磁带盘符: {tape_drive}")
    print("-" * 60)
    
    # 检查磁带盘符是否存在
    if not os.path.exists(tape_drive):
        print(f"[FAIL] 磁带盘符不存在: {tape_drive}")
        print("请检查TAPE_DRIVE_LETTER配置")
        return False
    
    print(f"[OK] 磁带盘符存在: {tape_drive}")
    
    # 3. 创建备份目录
    backup_dir = Path(tape_drive) / "backup_test"
    backup_dir.mkdir(parents=True, exist_ok=True)
    print(f"[OK] 创建备份目录: {backup_dir}")
    
    # 4. 创建tar备份文件
    timestamp = datetime.now().strftime('%Y%m%d_%H%M%S')
    tar_file = backup_dir / f"backup_{timestamp}.tar.gz"
    
    print(f"\n开始备份...")
    print(f"目标文件: {tar_file}")
    print("-" * 60)
    
    # 方法1: 使用Python tarfile模块
    print("\n方法1: 使用Python tarfile模块...")
    try:
        with tarfile.open(tar_file, 'w:gz', compresslevel=9) as tar:
            # 添加源目录中的所有文件
            file_count = 0
            for root, dirs, files in os.walk(source_path):
                # 排除一些不需要的文件
                dirs[:] = [d for d in dirs if not d.startswith('.') and d != '__pycache__']
                
                for file in files:
                    if file.startswith('.') or file.endswith('.pyc'):
                        continue
                    
                    file_path = Path(root) / file
                    try:
                        # 计算相对路径
                        arcname = file_path.relative_to(source_path.parent)
                        tar.add(file_path, arcname=str(arcname))
                        file_count += 1
                        
                        if file_count % 10 == 0:
                            print(f"  已添加 {file_count} 个文件...")
                    except Exception as e:
                        print(f"  [WARN] 跳过文件 {file_path}: {str(e)}")
        
        # 检查文件大小
        file_size = tar_file.stat().st_size
        size_mb = file_size / (1024 * 1024)
        
        print(f"[OK] tar备份完成")
        print(f"  文件: {tar_file}")
        print(f"  大小: {size_mb:.2f} MB ({file_size:,} 字节)")
        print(f"  文件数: {file_count}")
        
        return True
        
    except Exception as e:
        print(f"[FAIL] tar备份失败: {str(e)}")
        import traceback
        traceback.print_exc()
        return False
    
    # 方法2: 使用系统tar命令（如果需要）
    # print("\n方法2: 使用系统tar命令...")
    # try:
    #     tar_cmd = ['tar', '-czf', str(tar_file), str(source_path)]
    #     result = subprocess.run(tar_cmd, capture_output=True, text=True)
    #     if result.returncode == 0:
    #         print(f"[OK] 系统tar命令成功")
    #     else:
    #         print(f"[FAIL] 系统tar命令失败: {result.stderr}")
    # except Exception as e:
    #     print(f"[FAIL] 系统tar命令异常: {str(e)}")

if __name__ == "__main__":
    success = test_tar_backup()
    print("\n" + "=" * 60)
    if success:
        print("测试完成: 成功")
    else:
        print("测试完成: 失败")
    print("=" * 60)

