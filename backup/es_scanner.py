#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
Everything搜索工具扫描模块
ES Scanner Module

使用 Everything 搜索工具 (es.exe) 进行文件扫描
"""

import asyncio
import logging
import os
import subprocess
import ctypes
from pathlib import Path
from typing import List, Dict, Optional, AsyncGenerator
from ctypes import wintypes

logger = logging.getLogger(__name__)


def get_short_path_name(long_path: str) -> str:
    """
    获取文件或文件夹的短路径名（8.3格式）
    """
    try:
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


class ESScanner:
    """Everything搜索工具扫描器"""
    
    def __init__(self, es_exe_path: str = r"E:\app\TAF\ITDT\ES\es.exe"):
        """
        初始化ES扫描器
        
        Args:
            es_exe_path: ES工具可执行文件路径
        """
        self.es_exe_path = es_exe_path
        self._check_es_tool()
    
    def _check_es_tool(self) -> bool:
        """检查ES工具是否存在"""
        if not os.path.exists(self.es_exe_path):
            logger.warning(f"ES工具不存在: {self.es_exe_path}")
            return False
        return True
    
    def build_search_command(
        self, 
        search_dir: str, 
        exclude_patterns: List[str] = None,
        offset: int = 0, 
        limit: int = 1000
    ) -> List[str]:
        """
        构建搜索命令，支持分页，只显示文件
        
        Args:
            search_dir: 搜索目录
            exclude_patterns: 排除模式列表
            offset: 偏移量（从第几个文件开始）
            limit: 每页返回的文件数
            
        Returns:
            命令参数列表
        """
        if exclude_patterns is None:
            exclude_patterns = []
        
        # 基础命令 - 添加 -a-d 参数排除目录
        # 使用 -dm (date-modified) 获取修改时间，-date-format 1 使用 ISO-8601 格式
        # 使用 -tsv 格式输出 Tab 分隔值，便于解析
        cmd = [self.es_exe_path, "-full-path-and-name", "-size", "-dm", "-a-d", "-tsv", "-no-header", "-date-format", "1"]
        
        # 添加分页参数
        cmd.extend(["-o", str(offset), "-n", str(limit)])
        
        # 获取System Volume Information的短名称（系统必要排除）
        system_volume_path = os.path.join(search_dir, "System Volume Information")
        short_path = get_short_path_name(system_volume_path)
        
        if short_path != system_volume_path:
            short_name = os.path.basename(short_path)
            logger.info(f"使用短名称排除: {short_name}")
        else:
            short_name = "SYSTEM~1"  # 默认短名称
        
        # 系统必要的排除规则（System Volume Information必须排除）
        system_excludes = [short_name]
        
        # 合并用户提供的排除规则（从计划任务配置中获取）
        # exclude_patterns 已经包含了计划任务配置中的所有排除规则，直接使用
        all_excludes = list(set(system_excludes + (exclude_patterns or [])))
        
        # 添加搜索目录（必须在排除规则之前）
        cmd.append(search_dir)
        
        # 添加排除规则（在搜索目录之后，符合ES工具命令格式）
        for pattern in all_excludes:
            cmd.append(f"!{pattern}")
        
        return cmd
    
    async def get_total_file_count(
        self, 
        search_dir: str, 
        exclude_patterns: List[str] = None
    ) -> Optional[int]:
        """
        获取总文件数量（只计算文件，不包括目录）
        
        Args:
            search_dir: 搜索目录
            exclude_patterns: 排除模式列表
            
        Returns:
            文件总数，如果获取失败返回None
        """
        try:
            # 构建基础搜索命令（不带分页）
            base_cmd = self.build_search_command(search_dir, exclude_patterns, offset=0, limit=1)
            # 移除分页参数
            base_cmd = [arg for arg in base_cmd if arg not in ["-o", "0", "-n", "1"]]
            
            # 添加获取结果数量的参数
            base_cmd.append("-get-result-count")
            
            result = await asyncio.create_subprocess_exec(
                *base_cmd,
                stdout=asyncio.subprocess.PIPE,
                stderr=asyncio.subprocess.PIPE,
                stdin=asyncio.subprocess.DEVNULL
            )
            
            stdout, stderr = await asyncio.wait_for(result.communicate(), timeout=30)
            
            if result.returncode == 0:
                # Windows上ES可能输出GBK编码，尝试多种编码
                count_str = None
                for encoding in ['utf-8', 'gbk', 'gb2312', 'cp936']:
                    try:
                        count_str = stdout.decode(encoding, errors='strict').strip()
                        break
                    except (UnicodeDecodeError, LookupError):
                        continue
                
                # 如果所有编码都失败，使用errors='ignore'的UTF-8作为后备
                if count_str is None:
                    count_str = stdout.decode('utf-8', errors='ignore').strip()
                
                count = count_str
                if count.isdigit():
                    return int(count)
            
            return None
        except Exception as e:
            logger.warning(f"获取文件总数失败: {str(e)}")
            return None
    
    async def scan_files_streaming(
        self,
        source_paths: List[str],
        exclude_patterns: List[str] = None,
        backup_task: Optional[object] = None,
        log_context: str = "[ES扫描]"
    ) -> AsyncGenerator[List[Dict], None]:
        """
        流式扫描文件（异步生成器）
        
        Args:
            source_paths: 源路径列表
            exclude_patterns: 排除模式列表
            backup_task: 备份任务对象（可选，用于进度更新）
            log_context: 日志上下文前缀
            
        Yields:
            文件信息批次（每批文件数由 SCAN_UPDATE_INTERVAL 配置决定）
        """
        if exclude_patterns is None:
            exclude_patterns = []
        
        # 检查ES工具
        if not self._check_es_tool():
            logger.error(f"{log_context} ES工具不存在: {self.es_exe_path}，无法使用ES扫描")
            raise FileNotFoundError(f"ES工具不存在: {self.es_exe_path}")
        
        # ES扫描器的分页limit，使用SCAN_UPDATE_INTERVAL配置的值
        # 由备份策略中的"后台扫描进度更新间隔（文件数）"控制
        from config.settings import get_settings
        settings = get_settings()
        # 使用SCAN_UPDATE_INTERVAL作为ES查询的批次大小
        limit = getattr(settings, 'SCAN_UPDATE_INTERVAL', 500)  # ES查询批次大小，默认500
        
        total_files_scanned = 0
        
        for source_path_str in source_paths:
            logger.info(f"{log_context} 开始扫描源路径: {source_path_str}")
            
            # 检查目录是否存在
            if not os.path.exists(source_path_str):
                logger.warning(f"{log_context} 搜索目录不存在: {source_path_str}，跳过")
                continue
            
            page = 1
            offset = 0
            
            while True:
                try:
                    # 构建当前页的命令
                    cmd = self.build_search_command(
                        source_path_str, 
                        exclude_patterns, 
                        offset=offset, 
                        limit=limit
                    )
                    
                    logger.debug(f"{log_context} 执行ES命令: {' '.join(cmd)}")
                    
                    # 执行ES命令
                    result = await asyncio.create_subprocess_exec(
                        *cmd,
                        stdout=asyncio.subprocess.PIPE,
                        stderr=asyncio.subprocess.PIPE,
                        stdin=asyncio.subprocess.DEVNULL
                    )
                    
                    stdout, stderr = await asyncio.wait_for(
                        result.communicate(), 
                        timeout=60
                    )
                    
                    if result.returncode == 0:
                        # Windows上ES可能输出GBK编码，尝试多种编码
                        output = None
                        for encoding in ['utf-8', 'gbk', 'gb2312', 'cp936']:
                            try:
                                output = stdout.decode(encoding, errors='strict').strip()
                                break
                            except (UnicodeDecodeError, LookupError):
                                continue
                        
                        # 如果所有编码都失败，使用errors='ignore'的UTF-8作为后备
                        if output is None:
                            output = stdout.decode('utf-8', errors='ignore').strip()
                            logger.warning(f"{log_context} 无法确定ES输出编码，使用UTF-8（忽略错误）")
                        
                        if output:
                            lines = output.split('\n')
                            # 过滤空行
                            valid_lines = [line for line in lines if line.strip()]
                            
                            if not valid_lines:
                                # 没有更多结果
                                break
                            
                            # 解析文件信息（TSV格式：文件路径\t文件大小\t修改日期）
                            batch = []
                            for line in valid_lines:
                                try:
                                    # ES输出格式（TSV）: "文件路径\t文件大小\t修改日期"
                                    parts = line.strip().split('\t')
                                    if len(parts) >= 2:
                                        file_path = parts[0].strip()
                                        file_size_str = parts[1].strip()
                                        
                                        # 验证文件路径
                                        if not file_path:
                                            logger.warning(f"{log_context} 文件路径为空，跳过该行: {repr(line[:100])}")
                                            continue
                                        
                                        # 验证文件大小字符串
                                        if not file_size_str:
                                            logger.warning(f"{log_context} 文件大小为空，路径: {file_path[:100]}")
                                            file_size_str = "0"
                                        
                                        # 解析文件大小（字节）
                                        try:
                                            file_size = int(file_size_str)
                                        except ValueError:
                                            logger.warning(f"{log_context} 文件大小解析失败: {file_size_str}，路径: {file_path[:100]}")
                                            file_size = 0
                                        
                                        # 解析修改日期（ISO-8601格式，如果有）
                                        modified_time_str = parts[2].strip() if len(parts) >= 3 else None
                                        from datetime import datetime
                                        
                                        if modified_time_str:
                                            try:
                                                # ISO-8601格式: "2025-11-18T12:34:56" 或 "2025-11-18T12:34:56.789"
                                                # 可能带时区: "2025-11-18T12:34:56+08:00" 或 "2025-11-18T12:34:56Z"
                                                if modified_time_str.endswith('Z'):
                                                    modified_time_str = modified_time_str[:-1] + '+00:00'
                                                
                                                # 尝试解析ISO-8601格式
                                                if '+' in modified_time_str or modified_time_str.count('-') > 2:
                                                    # 带时区的ISO-8601格式
                                                    modified_time = datetime.fromisoformat(modified_time_str.replace('Z', '+00:00'))
                                                    # 转换为naive datetime（移除时区信息），与file_scanner一致
                                                    modified_time = modified_time.replace(tzinfo=None)
                                                else:
                                                    # 简单的ISO-8601格式（无时区）
                                                    modified_time = datetime.fromisoformat(modified_time_str)
                                            except (ValueError, AttributeError):
                                                # 解析失败，使用当前时间
                                                modified_time = datetime.now()
                                        else:
                                            # 没有修改日期，使用当前时间
                                            modified_time = datetime.now()  # naive datetime，不带时区，与file_scanner一致
                                        
                                        # 构建文件信息字典（必须与file_scanner格式完全一致）
                                        # file_scanner返回格式：{'path': str, 'name': str, 'size': int, 
                                        #                      'modified_time': datetime（naive，不带时区）, 'permissions': str,
                                        #                      'is_file': bool, 'is_dir': bool, 'is_symlink': bool}
                                        file_name = os.path.basename(file_path)
                                        
                                        # 验证文件名不为空
                                        if not file_name:
                                            logger.warning(f"{log_context} 文件名解析失败，路径: {file_path[:100]}")
                                            file_name = os.path.basename(file_path) or "unknown"
                                        
                                        # ES扫描器默认都是文件（因为已经用-a-d参数过滤了目录）
                                        file_info = {
                                            'path': file_path,
                                            'name': file_name,  # 注意：必须是'name'，不是'file_name'，与file_scanner完全一致
                                            'size': file_size,  # int类型，字节数
                                            'modified_time': modified_time,  # naive datetime对象，不带时区
                                            'permissions': None,  # ES扫描不提供权限信息，None与file_scanner的字符串不同，但memory_db_writer可以处理
                                            'is_file': True,  # ES扫描只返回文件
                                            'is_dir': False,
                                            'is_symlink': False
                                            # 注意：不添加任何额外字段，只保留file_scanner格式中的字段
                                        }
                                        
                                        # 验证file_info关键字段
                                        if not file_info.get('path'):
                                            logger.error(f"{log_context} file_info缺少path字段: {file_info}")
                                            continue
                                        
                                        batch.append(file_info)
                                except Exception as e:
                                    logger.warning(f"{log_context} 解析文件信息失败: {repr(line[:200])}, 错误: {str(e)}", exc_info=True)
                                    continue
                            
                            if batch:
                                total_files_scanned += len(batch)
                                logger.info(
                                    f"{log_context} 第 {page} 页: 找到 {len(batch)} 个文件 "
                                    f"(累计: {total_files_scanned} 个文件)"
                                )
                                yield batch
                            
                            # 如果当前页少于limit个文件，说明已经到达末尾
                            if len(valid_lines) < limit:
                                break
                            
                            # 更新偏移量
                            offset += limit
                            page += 1
                        else:
                            # 没有更多结果
                            break
                    else:
                        # Windows上ES可能输出GBK编码，尝试多种编码
                        error_msg = None
                        if stderr:
                            for encoding in ['utf-8', 'gbk', 'gb2312', 'cp936']:
                                try:
                                    error_msg = stderr.decode(encoding, errors='strict')
                                    break
                                except (UnicodeDecodeError, LookupError):
                                    continue
                            
                            # 如果所有编码都失败，使用errors='ignore'的UTF-8作为后备
                            if error_msg is None:
                                error_msg = stderr.decode('utf-8', errors='ignore')
                        else:
                            error_msg = "未知错误"
                        logger.warning(
                            f"{log_context} ES命令执行失败，返回码: {result.returncode}, "
                            f"错误: {error_msg}"
                        )
                        break
                        
                except asyncio.TimeoutError:
                    logger.warning(f"{log_context} ES搜索超时（60秒），跳过当前页")
                    break
                except Exception as e:
                    logger.error(f"{log_context} 执行ES搜索时发生错误: {str(e)}", exc_info=True)
                    break
            
            logger.debug(f"{log_context} 源路径扫描完成: {source_path_str}，共扫描 {total_files_scanned} 个文件")
        
        logger.info(f"{log_context} ES扫描完成，总共扫描 {total_files_scanned} 个文件")

