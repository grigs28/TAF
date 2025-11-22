#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
修复 .env 文件中 NOTIFICATION_EVENTS 的格式问题
Fix NOTIFICATION_EVENTS format issue in .env file
"""

import sys
import json
from pathlib import Path

def fix_notification_events(env_file_path: str = ".env"):
    """修复 NOTIFICATION_EVENTS 格式问题"""
    env_file = Path(env_file_path)
    
    if not env_file.exists():
        print(f"Error: .env file not found: {env_file_path}")
        return False
    
    print(f"Fixing .env file: {env_file_path}")
    
    # 读取文件内容
    try:
        with open(env_file, 'r', encoding='utf-8') as f:
            lines = f.readlines()
    except Exception as e:
        print(f"Failed to read file: {e}")
        return False
    
    # 查找并修复 NOTIFICATION_EVENTS 行
    fixed = False
    fixed_lines = []
    
    for line_num, line in enumerate(lines, 1):
        stripped = line.strip()
        
        if stripped.startswith("NOTIFICATION_EVENTS="):
            # 提取 JSON 部分
            value = stripped.split("=", 1)[1].strip()
            
            # 移除外层引号（如果存在）
            json_str = value
            if (json_str.startswith('"') and json_str.endswith('"')) or \
               (json_str.startswith("'") and json_str.endswith("'")):
                json_str = json_str[1:-1]
            
            # 尝试解析 JSON
            try:
                json_obj = json.loads(json_str)
                # 验证是字典
                if isinstance(json_obj, dict):
                    # 重新序列化为 JSON 字符串，并转义双引号
                    json_str_fixed = json.dumps(json_obj, ensure_ascii=False)
                    # 转义内部的双引号
                    escaped_json = json_str_fixed.replace('"', '\\"')
                    # 用双引号包裹
                    fixed_value = f'"{escaped_json}"'
                    fixed_line = f"NOTIFICATION_EVENTS={fixed_value}\n"
                    fixed_lines.append(fixed_line)
                    fixed = True
                    print(f"Fixed line {line_num}: NOTIFICATION_EVENTS")
                else:
                    fixed_lines.append(line)
            except json.JSONDecodeError as e:
                print(f"Warning: Line {line_num} JSON decode error: {e}")
                # 尝试修复：转义所有内部双引号
                if '"' in json_str and not json_str.startswith('"'):
                    # 转义所有双引号
                    escaped_json = json_str.replace('"', '\\"')
                    fixed_value = f'"{escaped_json}"'
                    fixed_line = f"NOTIFICATION_EVENTS={fixed_value}\n"
                    fixed_lines.append(fixed_line)
                    fixed = True
                    print(f"Fixed line {line_num}: NOTIFICATION_EVENTS (escaped quotes)")
                else:
                    fixed_lines.append(line)
        else:
            fixed_lines.append(line)
    
    if fixed:
        # 创建备份
        backup_file = env_file.with_suffix('.env.backup')
        try:
            with open(backup_file, 'w', encoding='utf-8') as f:
                f.writelines(lines)
            print(f"Backup created: {backup_file}")
        except Exception as e:
            print(f"Failed to create backup: {e}")
            return False
        
        # 写入修复后的内容
        try:
            with open(env_file, 'w', encoding='utf-8') as f:
                f.writelines(fixed_lines)
            print(f"Fixed .env file: {env_file_path}")
            return True
        except Exception as e:
            print(f"Failed to write fixed file: {e}")
            return False
    else:
        print("No changes needed")
        return True


if __name__ == "__main__":
    env_file = sys.argv[1] if len(sys.argv) > 1 else ".env"
    success = fix_notification_events(env_file)
    sys.exit(0 if success else 1)

