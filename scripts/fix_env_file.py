#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
修复 .env 文件格式问题
Fix .env file format issues
"""

import sys
from pathlib import Path
import json
import re

def fix_env_file(env_file_path: str = ".env"):
    """修复 .env 文件格式问题"""
    env_file = Path(env_file_path)
    
    if not env_file.exists():
        print(f"错误: .env 文件不存在: {env_file_path}")
        return False
    
    print(f"正在检查 .env 文件: {env_file_path}")
    
    # 读取文件内容
    try:
        with open(env_file, 'r', encoding='utf-8') as f:
            lines = f.readlines()
    except Exception as e:
        print(f"读取文件失败: {e}")
        return False
    
    # 检查每一行
    fixed_lines = []
    issues_found = []
    
    for line_num, line in enumerate(lines, 1):
        original_line = line
        stripped = line.strip()
        
        # 跳过空行和注释
        if not stripped or stripped.startswith('#'):
            fixed_lines.append(line)
            continue
        
        # 检查是否有等号
        if '=' not in stripped:
            issues_found.append(f"第 {line_num} 行: 缺少等号 - {stripped[:50]}")
            # 如果看起来像注释，添加 # 前缀
            if not stripped.startswith('#'):
                fixed_lines.append(f"# {line}\n")
            else:
                fixed_lines.append(line)
            continue
        
        # 分割键值对
        parts = stripped.split('=', 1)
        if len(parts) != 2:
            issues_found.append(f"第 {line_num} 行: 格式不正确 - {stripped[:50]}")
            fixed_lines.append(line)
            continue
        
        key = parts[0].strip()
        value = parts[1].strip()
        
        # 检查 NOTIFICATION_EVENTS 行
        if key == "NOTIFICATION_EVENTS":
            # 尝试解析 JSON
            try:
                # 移除引号（如果存在）
                json_str = value
                if (json_str.startswith('"') and json_str.endswith('"')) or \
                   (json_str.startswith("'") and json_str.endswith("'")):
                    json_str = json_str[1:-1]
                    # 处理转义的引号
                    if json_str.startswith('"'):
                        json_str = json_str.replace('\\"', '"')
                    elif json_str.startswith("'"):
                        json_str = json_str.replace("\\'", "'")
                
                # 验证 JSON
                json.loads(json_str)
                
                # 如果原始值没有引号，添加双引号
                if not (value.startswith('"') and value.endswith('"')) and \
                   not (value.startswith("'") and value.endswith("'")):
                    # 转义内部的双引号
                    escaped_value = json_str.replace('"', '\\"')
                    fixed_value = f'"{escaped_value}"'
                    fixed_line = f"{key}={fixed_value}\n"
                    issues_found.append(f"第 {line_num} 行: NOTIFICATION_EVENTS 值缺少引号，已修复")
                    fixed_lines.append(fixed_line)
                else:
                    fixed_lines.append(line)
                    
            except json.JSONDecodeError as e:
                issues_found.append(f"第 {line_num} 行: NOTIFICATION_EVENTS JSON 格式错误 - {str(e)}")
                # 尝试修复：如果值看起来像 JSON 但没有引号，添加引号
                if not (value.startswith('"') and value.endswith('"')) and \
                   not (value.startswith("'") and value.endswith("'")):
                    try:
                        # 尝试直接解析（可能已经是有效的 JSON）
                        json.loads(value)
                        # 如果解析成功，添加引号
                        escaped_value = value.replace('"', '\\"')
                        fixed_value = f'"{escaped_value}"'
                        fixed_line = f"{key}={fixed_value}\n"
                        issues_found.append(f"第 {line_num} 行: NOTIFICATION_EVENTS 已修复（添加引号）")
                        fixed_lines.append(fixed_line)
                    except:
                        # 如果还是失败，注释掉这一行
                        issues_found.append(f"第 {line_num} 行: NOTIFICATION_EVENTS 无法修复，已注释")
                        fixed_lines.append(f"# {line}")
                else:
                    # 有引号但 JSON 无效，注释掉
                    issues_found.append(f"第 {line_num} 行: NOTIFICATION_EVENTS JSON 无效，已注释")
                    fixed_lines.append(f"# {line}")
            except Exception as e:
                issues_found.append(f"第 {line_num} 行: NOTIFICATION_EVENTS 处理错误 - {str(e)}")
                fixed_lines.append(line)
        else:
            # 其他行，检查基本格式
            # 如果值包含空格但没有引号，可能需要引号
            if ' ' in value and not (value.startswith('"') and value.endswith('"')):
                # 但不要修改，因为有些值可能不需要引号
                pass
            fixed_lines.append(line)
    
    # 输出问题
    if issues_found:
        print(f"\n发现 {len(issues_found)} 个问题:")
        for issue in issues_found:
            print(f"  - {issue}")
        
        # 创建备份
        backup_file = env_file.with_suffix('.env.backup')
        try:
            with open(backup_file, 'w', encoding='utf-8') as f:
                f.writelines(lines)
            print(f"\n已创建备份文件: {backup_file}")
        except Exception as e:
            print(f"\n创建备份文件失败: {e}")
            return False
        
        # 写入修复后的内容
        try:
            with open(env_file, 'w', encoding='utf-8') as f:
                f.writelines(fixed_lines)
            print(f"已修复 .env 文件: {env_file_path}")
            return True
        except Exception as e:
            print(f"写入修复后的文件失败: {e}")
            return False
    else:
        print("未发现问题")
        return True


if __name__ == "__main__":
    env_file = sys.argv[1] if len(sys.argv) > 1 else ".env"
    success = fix_env_file(env_file)
    sys.exit(0 if success else 1)

