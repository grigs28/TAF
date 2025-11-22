#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
诊断 .env 文件问题
Diagnose .env file issues
"""

import sys
from pathlib import Path
from dotenv import dotenv_values

def diagnose_env_file(env_file_path: str = ".env"):
    """诊断 .env 文件问题"""
    env_file = Path(env_file_path)
    
    if not env_file.exists():
        print(f"错误: .env 文件不存在: {env_file_path}")
        return False
    
    print(f"正在诊断 .env 文件: {env_file_path}\n")
    
    # 读取文件内容，显示第 130 行附近
    try:
        with open(env_file, 'r', encoding='utf-8') as f:
            lines = f.readlines()
    except Exception as e:
        print(f"读取文件失败: {e}")
        return False
    
    print(f"文件总行数: {len(lines)}\n")
    
    # 显示第 125-135 行
    print("=" * 80)
    print("第 125-135 行内容:")
    print("=" * 80)
    for i in range(124, min(135, len(lines))):
        line = lines[i]
        line_num = i + 1
        print(f"{line_num:4d}: {repr(line)}")
    print("=" * 80)
    
    # 尝试使用 python-dotenv 解析
    print("\n尝试使用 python-dotenv 解析...")
    try:
        values = dotenv_values(env_file_path)
        print(f"✅ 解析成功，共 {len(values)} 个变量")
        return True
    except Exception as e:
        print(f"❌ 解析失败: {e}")
        print(f"错误类型: {type(e).__name__}")
        
        # 尝试逐行解析，找出问题行
        print("\n逐行检查...")
        for i, line in enumerate(lines, 1):
            stripped = line.strip()
            if not stripped or stripped.startswith('#'):
                continue
            if '=' not in stripped:
                print(f"⚠️  第 {i} 行可能有问题（缺少等号）: {stripped[:60]}")
            else:
                parts = stripped.split('=', 1)
                if len(parts) == 2:
                    key = parts[0].strip()
                    value = parts[1].strip()
                    # 检查值是否包含未转义的特殊字符
                    if value and not (value.startswith('"') and value.endswith('"')) and \
                       not (value.startswith("'") and value.endswith("'")) and \
                       (' ' in value or '\t' in value or '\n' in value):
                        print(f"⚠️  第 {i} 行可能有问题（值包含空格但无引号）: {key}={value[:40]}")
        
        return False


if __name__ == "__main__":
    env_file = sys.argv[1] if len(sys.argv) > 1 else ".env"
    success = diagnose_env_file(env_file)
    sys.exit(0 if success else 1)

