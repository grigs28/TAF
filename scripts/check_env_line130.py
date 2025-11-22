#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
检查 .env 文件第130行附近的问题
Check .env file issues around line 130
"""

from pathlib import Path

def check_env_file(env_file_path: str = ".env"):
    """检查 .env 文件第130行附近的问题"""
    env_file = Path(env_file_path)
    
    if not env_file.exists():
        print(f"错误: .env 文件不存在: {env_file_path}")
        return
    
    print(f"正在检查 .env 文件: {env_file_path}")
    print("=" * 80)
    
    # 读取文件内容
    try:
        with open(env_file, 'r', encoding='utf-8') as f:
            lines = f.readlines()
    except Exception as e:
        print(f"读取文件失败: {e}")
        return
    
    # 检查第130行附近（显示125-135行）
    start_line = max(0, 124)  # 第125行（索引124）
    end_line = min(len(lines), 135)  # 第135行（索引134）
    
    print(f"\n检查第 {start_line + 1} 到 {end_line} 行:\n")
    
    for i in range(start_line, end_line):
        line_num = i + 1
        line = lines[i]
        stripped = line.rstrip('\n\r')
        
        # 标记第130行
        marker = " <-- 第130行" if line_num == 130 else ""
        
        # 检查问题
        issues = []
        if not stripped.strip():
            status = "空行"
        elif stripped.strip().startswith('#'):
            status = "注释"
        elif '=' not in stripped.strip():
            status = "⚠️ 缺少等号"
            issues.append("这行没有等号，可能不是有效的环境变量")
        else:
            # 尝试解析
            parts = stripped.split('=', 1)
            if len(parts) == 2:
                key = parts[0].strip()
                value = parts[1].strip()
                
                # 检查值是否包含未转义的特殊字符
                if value and not (value.startswith('"') and value.endswith('"')) and \
                   not (value.startswith("'") and value.endswith("'")) and \
                   (' ' in value or '\t' in value):
                    if not value.startswith('r"') and not value.startswith("r'"):
                        issues.append("值包含空格但无引号，可能需要引号")
                
                status = f"变量: {key}"
            else:
                status = "⚠️ 格式错误"
                issues.append("无法正确分割键值对")
        
        # 显示行
        print(f"第 {line_num:3d} 行: {status}{marker}")
        if stripped:
            # 显示内容（截断过长的行）
            display = stripped[:100] + "..." if len(stripped) > 100 else stripped
            print(f"        内容: {repr(display)}")
        if issues:
            for issue in issues:
                print(f"        WARNING: {issue}")
        print()
    
    # 尝试使用 python-dotenv 解析
    print("\n" + "=" * 80)
    print("尝试使用 python-dotenv 解析...")
    try:
        from dotenv import dotenv_values
        values = dotenv_values(env_file_path)
        print(f"✅ 解析成功，共 {len(values)} 个变量")
    except Exception as e:
        print(f"❌ 解析失败: {e}")
        print(f"错误类型: {type(e).__name__}")
        
        # 尝试找出具体是哪一行
        import traceback
        error_msg = str(e)
        if "line" in error_msg.lower():
            print(f"\n错误信息中提到的行号: {error_msg}")

if __name__ == "__main__":
    check_env_file()

