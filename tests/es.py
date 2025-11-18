#!/usr/bin/env python3
"""
Everything搜索工具 - 只显示文件版
每1000个文件输出一次文件的大小和路径，不显示目录
"""

import subprocess
import sys
import os
import argparse

# 配置
ES_EXE = "E:\\app\\TAF\\ITDT\\ES\\es.exe"
DEFAULT_SEARCH_DIR = "D:\\AI"

def get_short_path_name(long_path):
    """
    获取文件或文件夹的短路径名（8.3格式）
    """
    try:
        import ctypes
        from ctypes import wintypes
        
        # 定义Windows API函数
        GetShortPathName = ctypes.windll.kernel32.GetShortPathNameW
        GetShortPathName.argtypes = [wintypes.LPCWSTR, wintypes.LPWSTR, wintypes.DWORD]
        GetShortPathName.restype = wintypes.DWORD
        
        # 准备缓冲区
        buffer = ctypes.create_unicode_buffer(260)  # MAX_PATH
        
        # 调用API获取短路径
        result = GetShortPathName(long_path, buffer, 260)
        
        if result == 0:
            # API调用失败，返回原路径
            return long_path
        else:
            return buffer.value
    except Exception:
        # 如果无法获取短路径，返回原路径
        return long_path

def build_search_command(search_dir, offset=0, limit=1000):
    """
    构建搜索命令，支持分页，只显示文件
    """
    # 基础命令 - 添加 -a-d 参数排除目录
    cmd = [ES_EXE, "-full-path-and-name", "-size", "-a-d"]
    
    # 添加分页参数
    cmd.extend(["-o", str(offset), "-n", str(limit)])
    
    # 添加搜索目录
    cmd.append(search_dir)
    
    # 获取System Volume Information的短名称
    system_volume_path = os.path.join(search_dir, "System Volume Information")
    short_path = get_short_path_name(system_volume_path)
    
    if short_path != system_volume_path:
        short_name = os.path.basename(short_path)
        print(f"使用短名称排除: {short_name}")
    else:
        short_name = "SYSTEM~1"  # 默认短名称
    
    # 排除规则
    exclude_patterns = [
        "*.bak", "*.tmp", "*.log", "*.swp", "*.cache",
        "Thumbs.db", ".DS_Store", "pagefile.sys", "$*",
        short_name  # 使用短名称排除System Volume Information
    ]
    
    for pattern in exclude_patterns:
        cmd.append(f"!{pattern}")
    
    return cmd

def search_files_only(search_dir=DEFAULT_SEARCH_DIR):
    """
    只搜索文件，不显示目录，每1000个文件输出一次
    """
    print(f"搜索目录: {search_dir}")
    print("只显示文件，不显示目录")
    print("排除规则: *.bak, *.tmp, *.log, *.swp, *.cache, Thumbs.db, .DS_Store, System Volume Information, pagefile.sys, $*")
    print("每1000个文件输出一次")
    print("-" * 80)
    
    total_files = 0
    page = 1
    
    while True:
        print(f"\n第 {page} 页 (文件 {total_files + 1} - {total_files + 1000})")
        print("-" * 60)
        
        # 构建当前页的命令
        cmd = build_search_command(search_dir, offset=total_files, limit=1000)
        
        try:
            result = subprocess.run(
                cmd,
                capture_output=True,
                text=True,
                encoding='utf-8',
                errors='ignore',
                timeout=60
            )
            
            if result.returncode == 0:
                output = result.stdout.strip()
                if output:
                    lines = output.split('\n')
                    # 过滤空行
                    valid_lines = [line for line in lines if line.strip()]
                    
                    if not valid_lines:
                        # 没有更多结果
                        break
                    
                    # 输出当前页的结果
                    for line in valid_lines:
                        print(line)
                    
                    current_page_count = len(valid_lines)
                    total_files += current_page_count
                    
                    print(f"本页找到 {current_page_count} 个文件")
                    print(f"累计找到 {total_files} 个文件")
                    
                    # 如果当前页少于1000个文件，说明已经到达末尾
                    if current_page_count < 1000:
                        break
                    
                    # 询问是否继续
                    if page % 10 == 0:  # 每10页询问一次
                        response = input("\n继续显示下一页？(y/n): ")
                        if response.lower() != 'y':
                            break
                    
                    page += 1
                else:
                    # 没有更多结果
                    break
            else:
                print(f"命令执行失败，返回码: {result.returncode}")
                if result.stderr:
                    print(f"错误信息: {result.stderr}")
                break
                
        except subprocess.TimeoutExpired:
            print("搜索超时")
            break
        except Exception as e:
            print(f"执行搜索时发生错误: {e}")
            break
    
    print("-" * 80)
    print(f"搜索完成，总共找到 {total_files} 个文件")

def get_total_file_count(search_dir):
    """
    获取总文件数量（只计算文件，不包括目录）
    """
    try:
        # 构建基础搜索命令（不带分页）
        base_cmd = build_search_command(search_dir, offset=0, limit=1)
        # 移除分页参数
        base_cmd = [arg for arg in base_cmd if arg not in ["-o", "0", "-n", "1"]]
        
        # 添加获取结果数量的参数
        base_cmd.append("-get-result-count")
        
        result = subprocess.run(
            base_cmd,
            capture_output=True,
            text=True,
            encoding='utf-8',
            errors='ignore',
            timeout=30
        )
        
        if result.returncode == 0:
            count = result.stdout.strip()
            if count.isdigit():
                return int(count)
        
        return None
    except Exception:
        return None

def search_with_estimated_total(search_dir=DEFAULT_SEARCH_DIR):
    """
    带预估总数的分页搜索，只显示文件
    """
    print(f"搜索目录: {search_dir}")
    print("只显示文件，不显示目录")
    print("正在计算预估文件数量...")
    
    # 获取预估总数
    total_count = get_total_file_count(search_dir)
    
    if total_count is not None:
        print(f"预估文件数量: {total_count}")
    
    print("开始分页搜索...")
    print("-" * 80)
    
    total_files = 0
    page = 1
    
    while True:
        if total_count is not None:
            progress = (total_files / total_count) * 100 if total_count > 0 else 0
            print(f"\n第 {page} 页 (进度: {progress:.1f}%, 文件 {total_files + 1} - {min(total_files + 1000, total_count)})")
        else:
            print(f"\n第 {page} 页 (文件 {total_files + 1} - {total_files + 1000})")
        
        print("-" * 60)
        
        # 构建当前页的命令
        cmd = build_search_command(search_dir, offset=total_files, limit=1000)
        
        try:
            result = subprocess.run(
                cmd,
                capture_output=True,
                text=True,
                encoding='utf-8',
                errors='ignore',
                timeout=60
            )
            
            if result.returncode == 0:
                output = result.stdout.strip()
                if output:
                    lines = output.split('\n')
                    # 过滤空行
                    valid_lines = [line for line in lines if line.strip()]
                    
                    if not valid_lines:
                        # 没有更多结果
                        break
                    
                    # 输出当前页的结果
                    for line in valid_lines:
                        print(line)
                    
                    current_page_count = len(valid_lines)
                    total_files += current_page_count
                    
                    print(f"本页找到 {current_page_count} 个文件")
                    print(f"累计找到 {total_files} 个文件")
                    
                    # 如果当前页少于1000个文件，说明已经到达末尾
                    if current_page_count < 1000:
                        break
                    
                    # 每5页询问是否继续
                    if page % 5 == 0:
                        response = input("\n继续显示下一页？(y/n): ")
                        if response.lower() != 'y':
                            break
                    
                    page += 1
                else:
                    # 没有更多结果
                    break
            else:
                print(f"命令执行失败，返回码: {result.returncode}")
                break
                
        except Exception as e:
            print(f"执行搜索时发生错误: {e}")
            break
    
    print("-" * 80)
    print(f"搜索完成，总共找到 {total_files} 个文件")

def main():
    """
    主函数
    """
    parser = argparse.ArgumentParser(description='Everything文件搜索工具 - 只显示文件版')
    parser.add_argument('directory', nargs='?', default=DEFAULT_SEARCH_DIR, 
                       help=f'搜索目录 (默认: {DEFAULT_SEARCH_DIR})')
    parser.add_argument('--estimate', '-e', action='store_true',
                       help='显示预估文件总数')
    parser.add_argument('--auto', '-a', action='store_true',
                       help='自动显示所有结果，不询问')
    
    args = parser.parse_args()
    
    search_dir = args.directory
    
    # 检查ES工具
    if not os.path.exists(ES_EXE):
        print(f"错误: 找不到ES工具 [{ES_EXE}]")
        return
    
    # 检查目录是否存在
    if not os.path.exists(search_dir):
        print(f"错误: 搜索目录不存在 [{search_dir}]")
        return
    
    print("=" * 60)
    print("Everything 文件搜索工具 - 只显示文件版")
    print("=" * 60)
    
    if args.estimate:
        search_with_estimated_total(search_dir)
    else:
        search_files_only(search_dir)

if __name__ == "__main__":
    main()