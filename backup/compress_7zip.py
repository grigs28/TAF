#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""7-Zip 命令行压缩实现（自 compressor.py 拆分，逻辑未变）"""

import logging
import time
import os
import subprocess
import shutil
import tarfile
from pathlib import Path
from typing import List, Dict, Optional, Callable, Awaitable, Any

from models.backup import BackupSet, BackupTask
from utils.datetime_utils import now, format_datetime
from backup.utils import format_bytes
from backup.compress_helpers import (
    _parse_size_to_bytes,
    _finalize_compression_progress,
    _apply_tar_copybufsize,
)

logger = logging.getLogger(__name__)

import py7zr  # noqa: F401

def _compress_with_7zip_command(
    archive_path: Path,
    file_group: List[Dict],
    backup_task: BackupTask,
    compression_level: int,
    compression_threads: int,
    sevenzip_path: str,
    compress_progress: Dict,
    total_files: int,
    base_processed_files: int,
    dictionary_size: str = "1g",
    memory_gb: int = 24,
    temp_work_base_dir: Optional[Path] = None
) -> Dict:
    """使用7-Zip命令行工具压缩文件组
    
    Args:
        archive_path: 压缩包路径
        file_group: 文件组列表
        backup_task: 备份任务对象
        compression_level: 压缩级别 (0-9)
        compression_threads: 线程数
        sevenzip_path: 7-Zip程序路径
        compress_progress: 进度跟踪字典
        total_files: 总文件数
        base_processed_files: 已处理文件数基数
        
    Returns:
        压缩结果字典 {'successful_files': [], 'failed_files': [], 'successful_original_size': 0}
    """
    successful_files = []
    failed_files = []
    
    # 验证7z.exe路径
    sevenzip_exe = Path(sevenzip_path)
    if not sevenzip_exe.exists():
        logger.error(f"7-Zip程序不存在: {sevenzip_path}")
        failed_files = [{'path': str(f['path']), 'reason': f'7-Zip程序不存在: {sevenzip_path}'} for f in file_group]
        return {'successful_files': [], 'failed_files': failed_files, 'successful_original_size': 0}
    
  
    try:
        # 构建7z命令
        # 注意：7-Zip不支持-mmem参数，内存分配由7-Zip根据字典大小和线程数自动计算
        # 7z a -mmt<N> -mx<N> -md<D> archive.7z files...
        # 字典大小由调用者计算并传入，内存大小仅用于日志记录
        dict_size_str = str(dictionary_size).lower().strip()
        cmd = [
            str(sevenzip_exe.absolute()),
            "a",  # Add files to archive
            f"-mmt{compression_threads}",  # 设置线程数
            f"-mx{compression_level}",  # 设置压缩级别
            f"-md{dict_size_str}",  # 字典大小（7-Zip会根据字典大小和线程数自动分配内存）
            str(archive_path.absolute()),
        ]
        
        logger.debug(f"7-Zip压缩参数: 字典={dict_size_str}, 线程={compression_threads}, 预计内存使用={memory_gb}GB (7-Zip自动分配)")
        
        # 添加所有文件路径
        source_paths = getattr(backup_task, 'source_paths', None) or []
        files_to_compress = []
        
        for file_info in file_group:
            file_path = Path(file_info['path'])
            if not file_path.exists():
                logger.warning(f"文件不存在，跳过: {file_path}")
                failed_files.append({'path': str(file_path), 'reason': '文件不存在'})
                continue
            
            # 计算相对路径（保留目录结构）
            try:
                arcname = None
                if source_paths:
                    for src_path in source_paths:
                        src = Path(src_path)
                        try:
                            if file_path.is_relative_to(src):
                                arcname = str(file_path.relative_to(src))
                                break
                        except (ValueError, AttributeError):
                            continue
                
                if arcname:
                    # 7z需要使用源路径和目标路径的方式
                    # 格式: 7z a archive.7z source\path\to\file -t7z
                    # 使用 -spf 选项保留完整路径
                    files_to_compress.append((file_path, arcname))
                else:
                    files_to_compress.append((file_path, file_path.name))
                    
            except Exception as e:
                logger.warning(f"计算相对路径失败: {file_path}, 错误: {e}")
                files_to_compress.append((file_path, file_path.name))
        
        if not files_to_compress:
            logger.warning("没有可压缩的文件")
            _finalize_compression_progress(compress_progress)
            return {'successful_files': [], 'failed_files': failed_files, 'successful_original_size': 0}
        
        # 使用工作目录方式：创建一个临时目录结构
        # 或者直接使用 -spf 选项保留路径
        # 简单方式：将所有文件添加到压缩包，使用文件名作为归档名称
        logger.debug(f"使用7-Zip命令行压缩 {len(files_to_compress)} 个文件 (线程数: {compression_threads}, 级别: {compression_level})")
        
        # 方法1: 直接添加所有文件（简单但可能丢失路径结构）
        # 先尝试使用工作目录方式
        # 将临时工作目录创建在指定的temp目录中，而不是压缩包所在目录（避免在O盘创建临时文件）
        if temp_work_base_dir is None:
            # 如果没有指定，使用默认的temp目录
            from config.settings import get_settings
            settings = get_settings()
            temp_work_base_dir = Path(settings.BACKUP_TEMP_DIR)
        temp_work_base_dir.mkdir(parents=True, exist_ok=True)
        temp_work_dir = temp_work_base_dir / f".7z_work_{archive_path.stem}"
        try:
            temp_work_dir.mkdir(parents=True, exist_ok=True)
            
            # 创建符号链接或复制文件到临时目录（使用相对路径结构）
            prepared_files = []
            for file_path, arcname in files_to_compress:
                target_file = temp_work_dir / arcname
                try:
                    target_file.parent.mkdir(parents=True, exist_ok=True)
                    try:
                        if hasattr(os, 'symlink'):
                            if target_file.exists():
                                target_file.unlink()
                            os.symlink(str(file_path.absolute()), str(target_file))
                        else:
                            shutil.copy2(file_path, target_file)
                    except Exception:
                        # 再尝试一次复制，若仍失败则跳过
                        try:
                            shutil.copy2(file_path, target_file)
                        except Exception as copy_error:
                            logger.warning(
                                f"复制文件到压缩临时目录失败，跳过: {file_path} (错误: {copy_error})"
                            )
                            failed_files.append({'path': str(file_path), 'reason': f'复制失败: {copy_error}'})
                            continue
                    prepared_files.append((file_path, arcname))
                except Exception as prepare_error:
                    logger.warning(f"准备压缩文件失败，跳过: {file_path} (错误: {prepare_error})")
                    failed_files.append({'path': str(file_path), 'reason': f'准备失败: {prepare_error}'})
                    continue
            
            if not prepared_files:
                logger.warning("所有文件在准备阶段失败，跳过该压缩批次")
                _finalize_compression_progress(compress_progress)
                return {'successful_files': [], 'failed_files': failed_files, 'successful_original_size': 0}
            
            files_to_compress = prepared_files
            
            # 在临时工作目录中执行7z命令
            # 注意：7-Zip不支持-mmem参数，内存分配由7-Zip根据字典大小和线程数自动计算
            dict_size_str = str(dictionary_size).lower().strip()
            cmd_work = [
                str(sevenzip_exe.absolute()),
                "a",
                f"-mmt{compression_threads}",
                f"-mx{compression_level}",
                f"-md{dict_size_str}",  # 字典大小（7-Zip会根据字典大小和线程数自动分配内存）
                "-spf2",  # 支持长路径
                "-sccUTF-8",  # 强制使用UTF-8，避免编码问题
                "-y",  # 假设所有查询都回答"是"
                str(archive_path.absolute()),
                "*",  # 压缩当前目录下的所有文件
            ]
            
            # 确保使用绝对路径
            temp_work_dir_abs = temp_work_dir.absolute()
            archive_path_abs = archive_path.absolute()
            
            # 记录完整命令（使用INFO级别，确保能看到）
            logger.debug(f"执行7z命令: {' '.join(cmd_work)}")
            logger.debug(f"工作目录（绝对路径）: {temp_work_dir_abs}")
            logger.debug(f"压缩包路径（绝对路径）: {archive_path_abs}")
            
            # 更新命令中的压缩包路径为绝对路径
            cmd_work_abs = cmd_work.copy()
            cmd_work_abs[-2] = str(archive_path_abs)  # 压缩包路径是倒数第二个参数
            
            def _decode_bytes(data: Optional[bytes]) -> str:
                if not data:
                    return ""
                for encoding in ("utf-8", "gbk", "cp936", "latin-1"):
                    try:
                        return data.decode(encoding)
                    except UnicodeDecodeError:
                        continue
                return data.decode("utf-8", errors="ignore")

            # 执行命令（不设置超时，让压缩自然完成）
            process = subprocess.run(
                cmd_work_abs,
                cwd=str(temp_work_dir_abs),
                capture_output=True,
                text=False,
                stdin=subprocess.DEVNULL
            )
            stdout_text = _decode_bytes(process.stdout)
            stderr_text = _decode_bytes(process.stderr)
            
            if process.returncode != 0:
                # 7-Zip返回码说明：
                # 0 = 成功
                # 1 = 警告（某些文件无法处理）
                # 2 = 致命错误
                # 7 = 命令行错误
                # 8 = 内存不足或用户中断
                # 255 = 用户中断
                return_code_meanings = {
                    0: "成功",
                    1: "警告（某些文件无法处理）",
                    2: "致命错误",
                    7: "命令行错误",
                    8: "内存不足或用户中断",
                    255: "用户中断"
                }
                meaning = return_code_meanings.get(process.returncode, "未知错误")
                
                logger.error(f"7z命令返回码: {process.returncode} ({meaning})")
                logger.error(f"执行的命令: {' '.join(cmd_work_abs)}")
                logger.error(f"工作目录（绝对路径）: {temp_work_dir_abs}")
                logger.error(f"压缩包路径（绝对路径）: {archive_path_abs}")
                
                # 检查工作目录是否存在
                if not temp_work_dir_abs.exists():
                    logger.error(f"工作目录不存在: {temp_work_dir_abs}")
                else:
                    # 列出工作目录中的文件
                    try:
                        files_in_work_dir = list(temp_work_dir_abs.iterdir())
                        logger.error(f"工作目录中的文件数: {len(files_in_work_dir)}")
                        if len(files_in_work_dir) > 0:
                            logger.error(f"工作目录中的前10个文件: {[str(f.name) for f in files_in_work_dir[:10]]}")
                    except Exception as list_err:
                        logger.error(f"无法列出工作目录内容: {str(list_err)}")
                
                # 输出完整的错误信息
                if stderr_text:
                    logger.error(f"错误输出:\n{stderr_text}")
                else:
                    logger.error("无错误输出（stderr为空）")
                
                # 输出标准输出（可能包含有用信息）
                if stdout_text:
                    logger.error(f"标准输出:\n{stdout_text}")
                    # 分析标准输出，查找可能的错误原因
                    stdout_lines = stdout_text.split('\n')
                    for line in stdout_lines:
                        if 'ERROR' in line.upper() or 'FAILED' in line.upper() or 'CANNOT' in line.upper():
                            logger.error(f"标准输出中的错误信息: {line}")
                
                archive_exists = archive_path_abs.exists()
                if archive_exists:
                    logger.warning(f"虽然返回码非0，但压缩包文件存在: {archive_path_abs}")
                    logger.warning(f"压缩包大小: {archive_path_abs.stat().st_size:,} 字节")
                else:
                    logger.error(f"压缩包文件不存在: {archive_path_abs}")
                    if not archive_path_abs.parent.exists():
                        logger.error(f"压缩包父目录不存在: {archive_path_abs.parent}")
                    else:
                        try:
                            test_file = archive_path_abs.parent / "test_write.tmp"
                            test_file.write_text("test")
                            test_file.unlink()
                            logger.debug(f"压缩包父目录可写: {archive_path_abs.parent}")
                        except Exception as write_err:
                            logger.error(f"压缩包父目录不可写: {archive_path_abs.parent}, 错误: {str(write_err)}")
                
                if process.returncode == 8:
                    logger.error("返回码8通常表示内存不足或用户中断")
                    logger.error("建议：")
                    logger.error(f"  1. 检查系统可用内存是否足够")
                    logger.error(f"  2. 减小字典大小（当前: {dict_size_str}）")
                    logger.error(f"  3. 减少线程数（当前: {compression_threads}）")
                    logger.error(f"  4. 检查是否有足够的磁盘空间")
                
                if process.returncode not in (0, 1) and not archive_exists:
                    for file_path, _ in files_to_compress:
                        failed_files.append({'path': str(file_path), 'reason': f'7z命令失败: 返回码{process.returncode}'})
                    _finalize_compression_progress(compress_progress, archive_path_abs)
                    return {'successful_files': [], 'failed_files': failed_files, 'successful_original_size': 0}
                
                if process.returncode == 1:
                    logger.warning("7z返回警告（部分文件可能未压缩），继续使用已生成的压缩包")
                else:
                    logger.warning("7z返回非0但压缩包存在，将继续后续流程；相关文件标记为失败")
                    for file_path, _ in files_to_compress:
                        failed_files.append({'path': str(file_path), 'reason': f'7z返回码{process.returncode}'})
            
            # 命令成功，所有文件都成功
            for file_path, _ in files_to_compress:
                successful_files.append(str(file_path))
                
                # 更新进度
                if total_files > 0:
                    file_idx = len(successful_files) - 1
                    current_processed = base_processed_files + file_idx + 1
                    compress_progress_value = 10.0 + (current_processed / total_files) * 90.0
                    compress_progress['bytes_written'] = archive_path.stat().st_size if archive_path.exists() else 0
                    
                    if backup_task and backup_task.id:
                        backup_task.progress_percent = min(100.0, compress_progress_value)
            
            # 验证压缩包是否存在（使用绝对路径）
            if archive_path_abs.exists():
                archive_size = archive_path_abs.stat().st_size
                logger.info(f"7-Zip命令行压缩完成: {len(files_to_compress)} 个文件成功候选，压缩包大小: {format_bytes(archive_size)}")
            else:
                logger.error(f"7-Zip命令行压缩完成，但压缩包文件不存在: {archive_path_abs}")
                for file_path, _ in files_to_compress:
                    if str(file_path) not in [f['path'] for f in failed_files]:
                        failed_files.append({'path': str(file_path), 'reason': '压缩包文件不存在'})
                _finalize_compression_progress(compress_progress, archive_path_abs)
                return {'successful_files': [], 'failed_files': failed_files, 'successful_original_size': 0}
            
        except Exception as work_dir_error:
            logger.error(f"工作目录方式压缩失败: {str(work_dir_error)}")
            import traceback
            logger.error(traceback.format_exc())
            # 继续尝试其他方式或返回失败
            for file_path, _ in files_to_compress:
                failed_files.append({'path': str(file_path), 'reason': f'工作目录方式失败: {str(work_dir_error)}'})
        finally:
            # 清理临时工作目录
            try:
                import shutil
                if temp_work_dir.exists():
                    shutil.rmtree(temp_work_dir, ignore_errors=True)
            except Exception as cleanup_error:
                logger.warning(f"清理临时工作目录失败: {cleanup_error}")
        
        # 标记压缩完成
        _finalize_compression_progress(compress_progress, archive_path_abs)
        
        failed_paths = {f['path'] for f in failed_files}
        successful_files = [
            str(file_path)
            for file_path, _ in files_to_compress
            if str(file_path) not in failed_paths
        ]
        successful_original_size = sum(
            f['size'] for f in file_group 
            if str(f['path']) in successful_files
        )
        
        return {
            'successful_files': successful_files,
            'failed_files': failed_files,
            'successful_original_size': successful_original_size,
            'archive_path': str(archive_path_abs)
        }
        
    except subprocess.TimeoutExpired:
        logger.error("7z命令执行超时")
        for file_path, _ in files_to_compress:
            failed_files.append({'path': str(file_path), 'reason': '7z命令执行超时'})
        _finalize_compression_progress(compress_progress)
        return {
            'successful_files': [],
            'failed_files': failed_files,
            'successful_original_size': 0,
            'archive_path': str(archive_path_abs)
        }
    except Exception as e:
        logger.error(f"7-Zip命令行压缩失败: {str(e)}")
        import traceback
        logger.error(traceback.format_exc())
        for file_path, _ in files_to_compress:
            failed_files.append({'path': str(file_path), 'reason': str(e)})
        _finalize_compression_progress(compress_progress)
        return {
            'successful_files': [],
            'failed_files': failed_files,
            'successful_original_size': 0,
            'archive_path': str(archive_path_abs)
        }
