#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
压缩处理模块
Compression Module
"""

import asyncio
import logging
import time
import os
import subprocess
import shutil
import tarfile
import py7zr
import pgzip
try:
    import zstandard as zstd
except ImportError:
    zstd = None
from pathlib import Path
from typing import List, Dict, Optional

from models.backup import BackupSet, BackupTask
from utils.datetime_utils import now, format_datetime
from backup.utils import format_bytes

logger = logging.getLogger(__name__)


def _parse_size_to_bytes(size_str: Optional[str], default_bytes: int = 1024 * 1024 * 1024) -> int:
    """解析带单位的块大小字符串为字节"""
    if not size_str:
        return default_bytes
    value = str(size_str).strip().lower()
    try:
        if value.endswith('g'):
            return int(float(value[:-1]) * (1024 ** 3))
        if value.endswith('m'):
            return int(float(value[:-1]) * (1024 ** 2))
        if value.endswith('k'):
            return int(float(value[:-1]) * 1024)
        return int(float(value))
    except ValueError:
        logger.warning(f"无法解析块大小 {size_str}，使用默认 {default_bytes} 字节")
        return default_bytes


def _finalize_compression_progress(compress_progress: Dict, archive: Optional[Path] = None):
    """统一更新压缩进度，避免调用方一直等待"""
    archive_abs = None
    try:
        if archive is not None:
            archive_abs = archive.absolute()
    except Exception:
        archive_abs = None
    compress_progress['running'] = False
    compress_progress['completed'] = True
    if archive_abs and archive_abs.exists():
        try:
            compress_progress['bytes_written'] = archive_abs.stat().st_size
        except Exception:
            compress_progress['bytes_written'] = 0
    else:
        compress_progress['bytes_written'] = 0


# 尝试导入psutil用于内存检查
try:
    import psutil
    PSUTIL_AVAILABLE = True
except ImportError:
    PSUTIL_AVAILABLE = False
    logger.warning("psutil未安装，无法检查系统内存，将使用默认内存分配策略")


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
        
        logger.info(f"7-Zip压缩参数: 字典={dict_size_str}, 线程={compression_threads}, 预计内存使用={memory_gb}GB (7-Zip自动分配)")
        
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
        logger.info(f"使用7-Zip命令行压缩 {len(files_to_compress)} 个文件 (线程数: {compression_threads}, 级别: {compression_level})")
        
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
            logger.info(f"执行7z命令: {' '.join(cmd_work)}")
            logger.info(f"工作目录（绝对路径）: {temp_work_dir_abs}")
            logger.info(f"压缩包路径（绝对路径）: {archive_path_abs}")
            
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

            # 执行命令
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
                            logger.info(f"压缩包父目录可写: {archive_path_abs.parent}")
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
                    _finalize_progress(archive_path_abs)
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
                _finalize_progress(archive_path_abs)
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
        
        _finalize_progress(archive_path_abs)
        
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


def _compress_with_pgzip(
    archive_path: Path,
    file_group: List[Dict],
    backup_task: BackupTask,
    compression_level: int,
    pgzip_threads: int,
    block_size: str,
    compress_progress: Dict,
    total_files: int,
    base_processed_files: int,
) -> Dict:
    """使用PGZip压缩文件"""
    successful_files: List[str] = []
    failed_files: List[Dict[str, str]] = []
    archive_path_abs = archive_path.absolute()
    archive_path_abs.parent.mkdir(parents=True, exist_ok=True)

    block_size_bytes = _parse_size_to_bytes(block_size, 1024 * 1024 * 1024)
    threads = max(1, min(pgzip_threads or 1, 64))
    compresslevel = max(0, min(int(compression_level), 9))

    source_paths = getattr(backup_task, 'source_paths', None) or []

    close_start_time = None
    try:
        logger.info(f"[PGZip] 开始打开压缩文件: {archive_path_abs}")
        with pgzip.open(
            archive_path_abs,
            'wb',
            thread=threads,
            blocksize=block_size_bytes,
            compresslevel=compresslevel or 5
        ) as gz_source:
            logger.info(f"[PGZip] 压缩文件已打开，开始创建tar文件")
            with tarfile.open(fileobj=gz_source, mode='w') as tar:
                total_files_in_group = len(file_group)
                last_log_time = time.time()
                log_interval = 5.0  # 每5秒输出一次进度
                
                for file_idx, file_info in enumerate(file_group):
                    file_path = Path(file_info['path'])
                    
                    # 每100个文件或每5秒输出一次进度
                    current_time = time.time()
                    if file_idx % 100 == 0 or (current_time - last_log_time) >= log_interval:
                        current_progress = file_idx + 1
                        progress_percent = (current_progress / total_files_in_group * 100) if total_files_in_group > 0 else 0
                        logger.info(f"[PGZip] 压缩进度: {current_progress}/{total_files_in_group} 个文件 ({progress_percent:.1f}%)")
                        # 将压缩进度信息存储到 compress_progress 和 backup_task 中
                        compress_progress['current_file_index'] = current_progress
                        compress_progress['total_files_in_group'] = total_files_in_group
                        if hasattr(backup_task, 'current_compression_progress'):
                            backup_task.current_compression_progress = {
                                'current': current_progress,
                                'total': total_files_in_group,
                                'percent': progress_percent
                            }
                        last_log_time = current_time
                    
                    if not file_path.exists():
                        logger.warning(f"文件不存在，跳过: {file_path}")
                        failed_files.append({'path': str(file_path), 'reason': '文件不存在'})
                        continue

                    arcname = file_path.name
                    for src_path in source_paths:
                        src = Path(src_path)
                        try:
                            if file_path.is_relative_to(src):
                                arcname = str(file_path.relative_to(src))
                                break
                        except (ValueError, AttributeError):
                            continue

                    try:
                        # 记录开始添加文件（仅对前10个和每1000个文件，或大文件）
                        file_size = file_info.get('size', 0) or 0
                        is_large_file = file_size > 100 * 1024 * 1024  # 大于100MB的文件
                        
                        if file_idx < 10 or file_idx % 1000 == 0 or is_large_file:
                            file_size_mb = file_size / (1024 * 1024) if file_size > 0 else 0
                            logger.info(f"[PGZip] 开始添加文件 {file_idx + 1}/{total_files_in_group}: {file_path.name} (大小: {file_size_mb:.1f} MB)")
                        
                        # 对于大文件，记录开始时间
                        if is_large_file:
                            add_start_time = time.time()
                        
                        # 检查文件路径是否包含特殊字符，可能导致问题
                        # Windows上，某些特殊字符可能导致文件操作阻塞或需要输入
                        file_path_str = str(file_path)
                        if any(char in file_path_str for char in [' ', '-', '(', ')', '[', ']', '{', '}', '&', '|', '<', '>', '^', '%', '!', '@', '#', '$', '^', '*', '+', '=', ';', ':', '"', "'", '?', '*']):
                            logger.debug(f"[PGZip] 文件路径包含特殊字符: {file_path_str}")
                        
                        # 使用 filter 参数处理特殊文件名，避免阻塞
                        # filter 参数可以自定义文件元数据，避免某些文件系统操作
                        try:
                            tar.add(file_path, arcname=arcname, filter=None)
                        except Exception as tar_add_error:
                            # 如果 tar.add 失败，尝试使用 filter 参数
                            logger.warning(f"[PGZip] tar.add 失败，尝试使用 filter 参数: {file_path}, 错误: {tar_add_error}")
                            # 定义一个简单的 filter 函数，只保留基本信息
                            def safe_filter(tarinfo):
                                # 只保留基本信息，避免触发文件系统的某些操作
                                return tarinfo
                            tar.add(file_path, arcname=arcname, filter=safe_filter)
                        
                        # 对于大文件，记录耗时
                        if is_large_file:
                            add_elapsed = time.time() - add_start_time
                            logger.info(f"[PGZip] 大文件添加完成: {file_path.name} (耗时: {add_elapsed:.1f}秒)")
                        
                        successful_files.append(str(file_path))
                        if total_files > 0:
                            current_processed = base_processed_files + file_idx + 1
                            compress_progress_value = 10.0 + (current_processed / total_files) * 90.0
                            compress_progress['bytes_written'] = archive_path_abs.stat().st_size if archive_path_abs.exists() else 0
                            backup_task.progress_percent = min(100.0, compress_progress_value)
                    except Exception as add_error:
                        logger.warning(f"添加文件到PGZip压缩包失败: {file_path}, 错误: {add_error}")
                        failed_files.append({'path': str(file_path), 'reason': f'写入失败: {add_error}'})
                        continue
                
                logger.info(f"[PGZip] 所有文件已添加到tar，共 {len(successful_files)} 个成功，{len(failed_files)} 个失败")
            
            logger.info(f"[PGZip] tar文件已关闭，准备关闭PGZip文件")
            # tar 文件已关闭，现在需要关闭 PGZip 文件
            
            # 在 with 语句退出前，显式刷新缓冲区（如果支持）
            # 这可以确保所有数据都写入文件，减少 close() 时的等待时间
            try:
                if hasattr(gz_source, 'flush'):
                    logger.info(f"[PGZip] 显式刷新PGZip缓冲区（在关闭前）...")
                    flush_start_time = time.time()
                    gz_source.flush()
                    flush_elapsed = time.time() - flush_start_time
                    logger.info(f"[PGZip] PGZip缓冲区已刷新（耗时: {flush_elapsed:.2f}秒）")
            except Exception as flush_error:
                logger.warning(f"[PGZip] 刷新PGZip缓冲区失败（可能不支持）: {flush_error}")
            
            logger.info(f"[PGZip] 准备退出with语句，PGZip将自动调用close()...")
            # with 语句退出时会自动调用 gz_source.close()
            # 如果 PGZip 内部使用了多线程，close() 可能需要等待所有线程完成
            # 记录关闭开始时间，以便定位阻塞点
            close_start_time = time.time()
        
        # with 语句已退出，gz_source.close() 应该已经完成
        if close_start_time is not None:
            close_elapsed = time.time() - close_start_time
            logger.info(f"[PGZip] PGZip文件已关闭（耗时: {close_elapsed:.2f}秒），检查文件是否存在")
        else:
            logger.info(f"[PGZip] PGZip文件已关闭，检查文件是否存在")
        if archive_path_abs.exists():
            logger.info(
                f"PGZip压缩完成: {len(successful_files)} 个文件成功, "
                f"压缩包大小: {format_bytes(archive_path_abs.stat().st_size)}"
            )
            compress_progress['bytes_written'] = archive_path_abs.stat().st_size

        # 标记压缩完成（关键修复）
        logger.info(f"[PGZip] 标记压缩进度为完成")
        _finalize_compression_progress(compress_progress, archive_path_abs)

        successful_original_size = sum(
            f['size'] for f in file_group if str(f['path']) in successful_files
        )

        return {
            'successful_files': successful_files,
            'failed_files': failed_files,
            'successful_original_size': successful_original_size,
            'archive_path': str(archive_path_abs)
        }
    except Exception as e:
        logger.error(f"PGZip压缩操作失败: {str(e)}")
        import traceback
        logger.error(traceback.format_exc())
        _finalize_compression_progress(compress_progress)
        return {
            'successful_files': [],
            'failed_files': failed_files,
            'successful_original_size': 0,
            'archive_path': str(archive_path_abs)
        }


def _compress_with_tar(
    archive_path: Path,
    file_group: List[Dict],
    backup_task: BackupTask,
    compression_level: int,
    compress_progress: Dict,
    total_files: int,
    base_processed_files: int,
) -> Dict:
    """使用 tar 打包文件（不压缩）"""
    archive_path_abs = archive_path.absolute()
    archive_path_abs.parent.mkdir(parents=True, exist_ok=True)

    successful_files: List[str] = []
    failed_files: List[Dict[str, str]] = []
    source_paths = getattr(backup_task, 'source_paths', None) or []
    total_files_in_group = len(file_group)
    last_log_time = time.time()
    log_interval = 5.0

    logger.info(f"[tar] 开始创建tar归档文件: {archive_path_abs}")
    try:
        with tarfile.open(archive_path_abs, 'w') as tar:
            for file_idx, file_info in enumerate(file_group):
                file_path = Path(file_info['path'])

                current_time = time.time()
                if file_idx % 100 == 0 or (current_time - last_log_time) >= log_interval:
                    logger.info(f"[tar] 打包进度: {file_idx + 1}/{total_files_in_group} 个文件 ({((file_idx + 1) / max(total_files_in_group, 1) * 100):.1f}%)")
                    last_log_time = current_time

                if not file_path.exists():
                    logger.warning(f"[tar] 文件不存在，跳过: {file_path}")
                    failed_files.append({'path': str(file_path), 'reason': '文件不存在'})
                    continue

                arcname = file_path.name
                for src_path in source_paths:
                    src = Path(src_path)
                    try:
                        if file_path.is_relative_to(src):
                            arcname = str(file_path.relative_to(src))
                            break
                    except (ValueError, AttributeError):
                        continue

                try:
                    tar.add(file_path, arcname=arcname)
                    successful_files.append(str(file_path))

                    if total_files > 0:
                        current_processed = base_processed_files + file_idx + 1
                        compress_progress_value = 10.0 + (current_processed / total_files) * 90.0
                        compress_progress['bytes_written'] = archive_path_abs.stat().st_size if archive_path_abs.exists() else 0
                        backup_task.progress_percent = min(100.0, compress_progress_value)
                except Exception as add_error:
                    logger.warning(f"[tar] 添加文件失败: {file_path}, 错误: {add_error}")
                    failed_files.append({'path': str(file_path), 'reason': f'写入失败: {add_error}'})
                    continue

        logger.info(f"[tar] 打包完成：{len(successful_files)} 个文件成功，{len(failed_files)} 个失败")
        compress_progress['completed'] = True
        compress_progress['running'] = False
        compress_progress['bytes_written'] = archive_path_abs.stat().st_size if archive_path_abs.exists() else 0

        successful_original_size = sum(f.get('size', 0) or 0 for f in file_group if str(f.get('path')) in successful_files)

        return {
            'successful_files': successful_files,
            'failed_files': failed_files,
            'successful_original_size': successful_original_size,
            'archive_path': str(archive_path_abs)
        }
    except Exception as tar_error:
        logger.error(f"tar打包失败: {tar_error}")
        import traceback
        logger.error(traceback.format_exc())
        for file_path in successful_files:
            failed_files.append({'path': file_path, 'reason': 'tar打包失败'})

        # 标记压缩完成（即使失败也要标记，避免无限等待）
        _finalize_compression_progress(compress_progress, archive_path_abs)

        return {
            'successful_files': [],
            'failed_files': failed_files,
            'successful_original_size': 0,
            'archive_path': str(archive_path_abs)
        }


def _compress_with_zstd(
    archive_path: Path,
    file_group: List[Dict],
    backup_task: BackupTask,
    compression_level: int,
    zstd_threads: int,
    compress_progress: Dict,
    total_files: int,
    base_processed_files: int,
) -> Dict:
    """使用 Zstandard 压缩（先打包成tar，再用zstd压缩）"""
    if zstd is None:
        raise RuntimeError("未安装 zstandard 库，无法使用 zstd 压缩（请运行 pip install zstandard）")

    archive_path_abs = archive_path.absolute()
    archive_path_abs.parent.mkdir(parents=True, exist_ok=True)

    try:
        threads = max(1, min(int(zstd_threads or 1), 64))
    except (ValueError, TypeError):
        threads = 1

    try:
        level = int(compression_level if compression_level is not None else 5)
    except (ValueError, TypeError):
        level = 5
    level = max(1, min(level, 19))

    successful_files: List[str] = []
    failed_files: List[Dict[str, str]] = []
    source_paths = getattr(backup_task, 'source_paths', None) or []
    total_files_in_group = len(file_group)
    last_log_time = time.time()
    log_interval = 5.0

    logger.info(f"[zstd] 开始创建压缩文件: {archive_path_abs} (level={level}, threads={threads})")
    try:
        with archive_path_abs.open('wb') as raw_out:
            compressor = zstd.ZstdCompressor(level=level, threads=threads)
            with compressor.stream_writer(raw_out, closefd=False) as zstd_stream:
                with tarfile.open(fileobj=zstd_stream, mode='w|') as tar:
                    for file_idx, file_info in enumerate(file_group):
                        file_path = Path(file_info['path'])

                        current_time = time.time()
                        if file_idx % 100 == 0 or (current_time - last_log_time) >= log_interval:
                            current_progress = file_idx + 1
                            progress_percent = (current_progress / max(total_files_in_group, 1) * 100) if total_files_in_group > 0 else 0
                            logger.info(f"[zstd] 压缩进度: {current_progress}/{total_files_in_group} 个文件 ({progress_percent:.1f}%)")
                            # 将压缩进度信息存储到 compress_progress 和 backup_task 中
                            compress_progress['current_file_index'] = current_progress
                            compress_progress['total_files_in_group'] = total_files_in_group
                            if hasattr(backup_task, 'current_compression_progress'):
                                backup_task.current_compression_progress = {
                                    'current': current_progress,
                                    'total': total_files_in_group,
                                    'percent': progress_percent
                                }
                            last_log_time = current_time

                        if not file_path.exists():
                            logger.warning(f"[zstd] 文件不存在，跳过: {file_path}")
                            failed_files.append({'path': str(file_path), 'reason': '文件不存在'})
                            continue

                        arcname = file_path.name
                        for src_path in source_paths:
                            src = Path(src_path)
                            try:
                                if file_path.is_relative_to(src):
                                    arcname = str(file_path.relative_to(src))
                                    break
                            except (ValueError, AttributeError):
                                continue

                        try:
                            tar.add(file_path, arcname=arcname)
                            successful_files.append(str(file_path))

                            if total_files > 0:
                                current_processed = base_processed_files + file_idx + 1
                                compress_progress_value = 10.0 + (current_processed / total_files) * 90.0
                                compress_progress['bytes_written'] = archive_path_abs.stat().st_size if archive_path_abs.exists() else 0
                                backup_task.progress_percent = min(100.0, compress_progress_value)
                        except Exception as add_error:
                            logger.warning(f"[zstd] 添加文件失败: {file_path}, 错误: {add_error}")
                            failed_files.append({'path': str(file_path), 'reason': f'写入失败: {add_error}'})
                            continue

        logger.info(f"[zstd] 压缩完成：{len(successful_files)} 个文件成功，{len(failed_files)} 个失败")
        compress_progress['completed'] = True
        compress_progress['running'] = False
        compress_progress['bytes_written'] = archive_path_abs.stat().st_size if archive_path_abs.exists() else 0

        successful_original_size = sum(f.get('size', 0) or 0 for f in file_group if str(f.get('path')) in successful_files)

        return {
            'successful_files': successful_files,
            'failed_files': failed_files,
            'successful_original_size': successful_original_size,
            'archive_path': str(archive_path_abs)
        }
    except Exception as zstd_error:
        logger.error(f"zstd压缩失败: {zstd_error}")
        import traceback
        logger.error(traceback.format_exc())
        for file_path in successful_files:
            failed_files.append({'path': file_path, 'reason': 'zstd压缩失败'})

        # 标记压缩完成（即使失败也要标记，避免无限等待）
        _finalize_compression_progress(compress_progress, archive_path_abs)

        return {
            'successful_files': [],
            'failed_files': failed_files,
            'successful_original_size': 0,
            'archive_path': str(archive_path_abs)
        }


class Compressor:
    """压缩处理器"""
    
    def __init__(self, settings=None):
        """初始化压缩处理器
        
        Args:
            settings: 系统设置对象
        """
        self.settings = settings
    
    async def group_files_for_compression(self, file_list: List[Dict]) -> List[List[Dict]]:
        """将文件分组以进行压缩
        
        单个压缩包的最大大小从 config 获取（MAX_FILE_SIZE）
        当批次超过阈值时，尽可能均分文件，使得每个压缩包的大小尽可能接近但不超过 MAX_FILE_SIZE
        
        Args:
            file_list: 文件列表
            
        Returns:
            List[List[Dict]]: 分组后的文件列表
        """
        # 从系统配置获取单个压缩包的最大大小
        max_size = self.settings.MAX_FILE_SIZE
        logger.debug(f"使用系统配置的单个压缩包最大大小: {format_bytes(max_size)}")
        
        if not file_list:
            return []
        
        # 计算批次总大小
        total_size = sum(f['size'] for f in file_list)
        logger.debug(f"批次总大小: {format_bytes(total_size)}, 文件数: {len(file_list)}")
        
        # 如果总大小不超过最大大小，直接返回一个组
        if total_size <= max_size:
            logger.debug(f"批次总大小未超过限制，返回单个组")
            return [file_list]
        
        # 如果总大小超过最大大小，需要分成多个组
        # 计算需要分成多少组（向上取整）
        num_groups = (total_size + max_size - 1) // max_size  # 向上取整
        logger.debug(f"批次总大小超过限制，需要分成 {num_groups} 个组")
        
        # 计算目标每组大小（尽可能均分）
        target_group_size = total_size / num_groups
        logger.debug(f"目标每组大小: {format_bytes(target_group_size)}")
        
        # 使用贪心算法分组：尽可能让每组大小接近目标大小，但不超过 max_size
        groups = []
        current_group = []
        current_size = 0
        
        # 按文件大小降序排序，优先处理大文件（有助于更好的分组）
        sorted_files = sorted(file_list, key=lambda x: x['size'], reverse=True)
        
        # 预计算剩余文件大小（用于优化决策）
        # remaining_sizes[i] 表示从索引 i+1 开始的所有文件的累计大小（不包括索引 i 的文件）
        remaining_sizes = [0] * (len(sorted_files) + 1)
        cumulative_size = 0
        for idx in range(len(sorted_files) - 1, -1, -1):
            remaining_sizes[idx] = cumulative_size
            cumulative_size += sorted_files[idx]['size']
        
        for idx, file_info in enumerate(sorted_files):
            file_size = file_info['size']
            # 剩余文件总大小（不包括当前文件）：从下一个文件开始的所有文件
            remaining_size = remaining_sizes[idx]
            
            # 如果单个文件超过最大大小，单独成组
            if file_size > max_size:
                if current_group:
                    groups.append(current_group)
                    current_group = []
                    current_size = 0
                groups.append([file_info])
                continue
            
            # 检查添加到当前组是否会超过最大大小
            if current_size + file_size > max_size and current_group:
                # 当前组已满，开始新组
                groups.append(current_group)
                current_group = []
                current_size = 0
            
            # 检查是否应该开始新组（基于目标大小，尽可能均分）
            # 如果当前组大小接近目标大小，且添加这个文件会明显超过目标大小，考虑开始新组
            if current_group and current_size >= target_group_size * 0.8:
                # 如果添加这个文件会超过目标大小很多，且还有剩余文件，考虑开始新组
                if remaining_size > 0 and current_size + file_size > target_group_size * 1.2:
                    # 检查剩余文件是否足够填满新组（至少达到目标大小的50%）
                    if remaining_size >= target_group_size * 0.5:
                        groups.append(current_group)
                        current_group = []
                        current_size = 0
            
            current_group.append(file_info)
            current_size += file_size
        
        # 添加最后一组
        if current_group:
            groups.append(current_group)
        
        # 验证分组结果
        total_grouped_size = sum(sum(f['size'] for f in group) for group in groups)
        if abs(total_grouped_size - total_size) > 1024:  # 允许1KB的误差
            logger.warning(f"分组大小不匹配: 原始={format_bytes(total_size)}, 分组后={format_bytes(total_grouped_size)}")
        
        # 记录分组信息
        group_sizes = [sum(f['size'] for f in group) for group in groups]
        logger.info(f"文件分组完成: {len(groups)} 个组, "
                   f"组大小范围: {format_bytes(min(group_sizes))} - {format_bytes(max(group_sizes))}, "
                   f"平均组大小: {format_bytes(sum(group_sizes) / len(group_sizes))}")
        
        return groups
    
    async def compress_file_group(
        self, 
        file_group: List[Dict], 
        backup_set: BackupSet, 
        backup_task: BackupTask,
        base_processed_files: int = 0, 
        total_files: int = 0
    ) -> Optional[Dict]:
        """压缩文件组（使用7z压缩，支持多线程，带进度跟踪）
        
        Args:
            file_group: 文件组列表
            backup_set: 备份集对象
            backup_task: 备份任务对象
            base_processed_files: 已处理文件数基数
            total_files: 总文件数
            
        Returns:
            压缩文件信息字典，如果失败则返回None
        """
        try:
            # 从备份任务获取压缩设置
            compression_enabled = getattr(backup_task, 'compression_enabled', True)
            
            # 从系统配置获取压缩级别（从 config 获取）
            compression_level = self.settings.COMPRESSION_LEVEL
            logger.debug(f"使用系统配置的压缩级别: {compression_level}")
            
            # 从系统配置获取压缩方法
            compression_method = getattr(self.settings, 'COMPRESSION_METHOD', 'pgzip')
            if compression_method not in ['pgzip', 'py7zr', '7zip_command', 'tar', 'zstd']:
                logger.warning(f"无效的压缩方法: {compression_method}，使用默认值: pgzip")
                compression_method = 'pgzip'
            if compression_method == 'zstd' and zstd is None:
                raise RuntimeError("未安装 zstandard 库，无法使用 zstd 压缩，请运行 pip install zstandard")
            
            # 从系统配置获取线程数
            compression_threads = self.settings.COMPRESSION_THREADS
            
            # 从系统配置获取7-Zip命令行线程数
            # 优先使用 COMPRESSION_COMMAND_THREADS，如果没有设置则使用 WEB_WORKERS，最后回退到 COMPRESSION_THREADS
            # 注意：COMPRESSION_COMMAND_THREADS 和 WEB_WORKERS 统一协调，默认使用 WEB_WORKERS 的值
            compression_command_threads = getattr(self.settings, 'COMPRESSION_COMMAND_THREADS', None)
            config_source = 'COMPRESSION_COMMAND_THREADS'
            if compression_command_threads is None:
                # 如果 COMPRESSION_COMMAND_THREADS 未设置，使用 WEB_WORKERS
                compression_command_threads = getattr(self.settings, 'WEB_WORKERS', compression_threads)
                config_source = 'WEB_WORKERS'
            
            # 确保 compression_command_threads 是整数类型
            try:
                compression_command_threads = int(compression_command_threads)
            except (ValueError, TypeError):
                logger.warning(f"compression_command_threads 值无效: {compression_command_threads}，使用默认值 {compression_threads}")
                compression_command_threads = int(compression_threads)
            
            logger.debug(f"使用7-Zip命令行线程数: {compression_command_threads} (来源: {config_source})")
            
            # 从系统配置获取7-Zip程序路径
            sevenzip_path = getattr(self.settings, 'SEVENZIP_PATH', r"C:\Program Files\7-Zip\7z.exe")
            
            # 获取字典大小配置
            dictionary_size = getattr(self.settings, 'COMPRESSION_DICTIONARY_SIZE', '1g')
            dict_size_str = str(dictionary_size).lower().strip()

            # PGZip配置
            pgzip_block_size = getattr(self.settings, 'PGZIP_BLOCK_SIZE', '1G')
            pgzip_threads = getattr(self.settings, 'PGZIP_THREADS', compression_threads)
            try:
                pgzip_threads = int(pgzip_threads)
            except (ValueError, TypeError):
                logger.warning(f"pgzip_threads 值无效: {pgzip_threads}，使用默认值 {compression_threads}")
                pgzip_threads = int(compression_threads)
            
            zstd_threads = getattr(self.settings, 'ZSTD_THREADS', compression_threads)
            try:
                zstd_threads = int(zstd_threads)
            except (ValueError, TypeError):
                logger.warning(f"zstd_threads 值无效: {zstd_threads}，使用默认值 {compression_threads}")
                zstd_threads = int(compression_threads)
            
            # 计算内存需求：内存需求 ≈ 字典大小 × 线程数 × 1.5（安全系数）
            # 解析字典大小（支持格式：64m, 128m, 512m, 1g, 2g等）
            dict_size_gb = 1.0  # 默认1GB
            try:
                if dict_size_str.endswith('g'):
                    dict_size_gb = float(dict_size_str[:-1])
                elif dict_size_str.endswith('m'):
                    dict_size_gb = float(dict_size_str[:-1]) / 1024.0
                elif dict_size_str.endswith('k'):
                    dict_size_gb = float(dict_size_str[:-1]) / (1024.0 * 1024.0)
                else:
                    # 纯数字，假设是MB
                    dict_size_gb = float(dict_size_str) / 1024.0
            except (ValueError, TypeError) as e:
                logger.warning(f"解析字典大小失败: {dict_size_str}，使用默认值 1GB，错误: {str(e)}")
                dict_size_gb = 1.0  # 解析失败，使用默认值
            
            # 确保 dict_size_gb 是浮点数类型
            dict_size_gb = float(dict_size_gb)
            
            # 固定字典大小为384M，不再动态调整
            # 计算内存需求：字典大小 × 线程数 × 1.5（安全系数）
            calculated_memory_gb = dict_size_gb * compression_command_threads * 1.5
            memory_gb = max(16, min(64, int(calculated_memory_gb)))
            
            # 记录内存信息（如果可用）
            if PSUTIL_AVAILABLE:
                try:
                    mem = psutil.virtual_memory()
                    total_memory_gb = mem.total / (1024 ** 3)
                    available_memory_gb = mem.available / (1024 ** 3)
                    logger.info(f"系统内存: 总计={total_memory_gb:.1f}GB, 可用={available_memory_gb:.1f}GB | "
                               f"固定字典={dict_size_str} ({dict_size_gb:.3f}GB), "
                               f"线程={compression_command_threads}, 预计内存={memory_gb}GB")
                except Exception as e:
                    logger.info(f"固定字典={dict_size_str}, 线程={compression_command_threads}, 预计内存={memory_gb}GB")
            else:
                logger.info(f"固定字典={dict_size_str}, 线程={compression_command_threads}, 预计内存={memory_gb}GB")
            
            # 统一使用相同的压缩流程：先压缩到temp目录
            # 根据配置决定是否移动文件（直接压缩到磁带时，不移动文件）
            compress_directly_to_tape = getattr(self.settings, 'COMPRESS_DIRECTLY_TO_TAPE', True)
            
            # 生成时间戳
            timestamp = format_datetime(now(), '%Y%m%d_%H%M%S')
            
            # 统一压缩到temp目录（无论是否直接压缩到磁带）
            compress_dir = Path(self.settings.BACKUP_COMPRESS_DIR)
            compress_dir.mkdir(parents=True, exist_ok=True)
            temp_dir = compress_dir / "temp" / backup_set.set_id
            temp_dir.mkdir(parents=True, exist_ok=True)
            backup_dir = temp_dir  # 统一使用temp目录作为压缩目标

            await self._ensure_disk_space(temp_dir)
            
            # 如果不直接压缩到磁带，需要final目录用于移动队列
            if compress_directly_to_tape:
                # 直接压缩到磁带，不需要final_dir（不移动文件）
                final_dir = None
                logger.info(f"压缩到临时目录: {backup_dir}，直接压缩到磁带模式（不移动文件）")
            else:
                # 先压缩到temp目录，再移动到final目录（原有流程）
                # final目录：压缩完成后移动到这里，等待移动到磁带机
                final_dir = compress_dir / "final" / backup_set.set_id
                final_dir.mkdir(parents=True, exist_ok=True)
                logger.info(f"压缩到临时目录: {backup_dir}，完成后将移动到: {final_dir}")
            
            # 进度跟踪变量
            compress_progress = {'bytes_written': 0, 'running': True, 'completed': False}
            total_original_size = sum(f['size'] for f in file_group)
            # 用于存储成功和失败的文件信息（在线程间共享）
            compress_result = {'successful_files': [], 'failed_files': [], 'successful_original_size': 0}
            
            # 将压缩操作放到线程池中执行，避免阻塞事件循环
            def _do_7z_compress():
                """在线程中执行7z压缩操作，带进度跟踪"""
                try:
                    # 在线程中创建目录（避免阻塞事件循环）
                    backup_dir.mkdir(parents=True, exist_ok=True)
                    
                    if compression_enabled:
                        # 根据压缩方法选择使用不同的后缀
                        if compression_method == 'pgzip':
                            archive_suffix = ".tar.gz"
                        elif compression_method == 'tar':
                            archive_suffix = ".tar"
                        elif compression_method == 'zstd':
                            archive_suffix = ".tar.zst"
                        else:
                            archive_suffix = ".7z"
                        archive_path = backup_dir / f"backup_{backup_set.set_id}_{timestamp}{archive_suffix}"
                        
                        if compression_method == '7zip_command':
                            # 使用7-Zip命令行工具进行压缩
                            logger.info(f"使用7-Zip命令行工具压缩 (路径: {sevenzip_path}, 线程数: {compression_command_threads})")
                            compress_result_inner = _compress_with_7zip_command(
                                archive_path, file_group, backup_task,
                                compression_level, compression_command_threads, sevenzip_path,
                                compress_progress, total_files, base_processed_files,
                                dictionary_size=dict_size_str, memory_gb=memory_gb,
                                temp_work_base_dir=temp_dir  # 所有临时工作目录都在temp/compress/temp/{backup_set.set_id}/中
                            )
                            # 将结果合并到外层compress_result
                            compress_result['successful_files'] = compress_result_inner['successful_files']
                            compress_result['failed_files'] = compress_result_inner['failed_files']
                            compress_result['successful_original_size'] = compress_result_inner['successful_original_size']
                            # 优先使用压缩函数返回的实际路径（绝对路径）
                            compress_result['archive_path'] = compress_result_inner.get('archive_path') or str(archive_path.absolute())
                        elif compression_method == 'pgzip':
                            logger.info(f"使用PGZip压缩 (线程数: {pgzip_threads}, 块大小: {pgzip_block_size}, 等级: {compression_level})")
                            compress_result_inner = _compress_with_pgzip(
                                archive_path, file_group, backup_task,
                                compression_level, pgzip_threads, pgzip_block_size,
                                compress_progress, total_files, base_processed_files
                            )
                            compress_result['successful_files'] = compress_result_inner['successful_files']
                            compress_result['failed_files'] = compress_result_inner['failed_files']
                            compress_result['successful_original_size'] = compress_result_inner['successful_original_size']
                            # 优先使用压缩函数返回的实际路径（绝对路径）
                            compress_result['archive_path'] = compress_result_inner.get('archive_path') or str(archive_path.absolute())
                        elif compression_method == 'tar':
                            logger.info(f"使用tar打包 (不压缩)")
                            compress_result_inner = _compress_with_tar(
                                archive_path, file_group, backup_task,
                                compression_level,
                                compress_progress, total_files, base_processed_files
                            )
                            compress_result['successful_files'] = compress_result_inner['successful_files']
                            compress_result['failed_files'] = compress_result_inner['failed_files']
                            compress_result['successful_original_size'] = compress_result_inner['successful_original_size']
                            compress_result['archive_path'] = compress_result_inner.get('archive_path') or str(archive_path.absolute())
                        elif compression_method == 'zstd':
                            logger.info(f"使用Zstandard压缩 (线程数: {zstd_threads}, 等级: {compression_level})")
                            compress_result_inner = _compress_with_zstd(
                                archive_path, file_group, backup_task,
                                compression_level, zstd_threads,
                                compress_progress, total_files, base_processed_files
                            )
                            compress_result['successful_files'] = compress_result_inner['successful_files']
                            compress_result['failed_files'] = compress_result_inner['failed_files']
                            compress_result['successful_original_size'] = compress_result_inner['successful_original_size']
                            compress_result['archive_path'] = compress_result_inner.get('archive_path') or str(archive_path.absolute())
                        else:
                            # 使用py7zr进行7z压缩，启用多进程（mp=True启用多进程压缩）
                            # 注意：py7zr 使用 mp 参数启用多进程，而不是 threads
                            logger.info(f"使用py7zr压缩 (线程数: {compression_threads}, mp={compression_threads > 1})")
                            with py7zr.SevenZipFile(
                                archive_path,
                                mode='w',
                                filters=[{'id': py7zr.FILTER_LZMA2, 'preset': compression_level}],
                                mp=True if compression_threads > 1 else False  # 启用多进程压缩（如果线程数>1）
                            ) as archive:
                                # 添加文件到压缩包
                                successful_files = []
                                failed_files = []
                                
                                for file_idx, file_info in enumerate(file_group):
                                    file_path = Path(file_info['path'])
                                    try:
                                        if not file_path.exists():
                                            logger.warning(f"文件不存在，跳过: {file_path}")
                                            failed_files.append({'path': str(file_path), 'reason': '文件不存在'})
                                            continue
                                        
                                        # 计算相对路径（保留目录结构）
                                        try:
                                            source_paths = backup_task.source_paths or []
                                            if source_paths:
                                                arcname = None
                                                for src_path in source_paths:
                                                    src = Path(src_path)
                                                    try:
                                                        if file_path.is_relative_to(src):
                                                            arcname = str(file_path.relative_to(src))
                                                            break
                                                    except (ValueError, AttributeError):
                                                        continue
                                                if arcname is None:
                                                    arcname = file_path.name
                                            else:
                                                arcname = file_path.name
                                        except Exception:
                                            arcname = file_path.name
                                        
                                        # 添加文件到压缩包
                                        try:
                                            archive.write(file_path, arcname=arcname)
                                            successful_files.append(str(file_path))
                                            
                                            # 更新进度：基于已压缩的文件数量
                                            if total_files > 0:
                                                current_processed = base_processed_files + file_idx + 1
                                                # 扫描阶段占10%，压缩阶段占90%
                                                compress_progress_value = 10.0 + (current_processed / total_files) * 90.0
                                                compress_progress['bytes_written'] = archive_path.stat().st_size if archive_path.exists() else 0
                                                
                                                # 更新任务进度（在后台线程中，需要异步更新）
                                                if backup_task and backup_task.id:
                                                    backup_task.progress_percent = min(100.0, compress_progress_value)
                                        except (PermissionError, OSError, FileNotFoundError, IOError) as write_error:
                                            # 文件写入错误（权限、访问等），跳过该文件，继续处理其他文件
                                            logger.warning(f"⚠️ 压缩时跳过无法访问的文件: {file_path} (错误: {str(write_error)})")
                                            failed_files.append({'path': str(file_path), 'reason': str(write_error)})
                                            continue
                                        except Exception as write_error:
                                            # 其他错误，也跳过该文件
                                            logger.warning(f"⚠️ 压缩时跳过出错的文件: {file_path} (错误: {str(write_error)})")
                                            failed_files.append({'path': str(file_path), 'reason': str(write_error)})
                                            continue
                                    except Exception as file_error:
                                        # 文件处理错误，跳过该文件
                                        logger.warning(f"⚠️ 压缩时跳过出错的文件: {file_path} (错误: {str(file_error)})")
                                        failed_files.append({'path': str(file_path), 'reason': str(file_error)})
                                        continue
                                
                                # 压缩完成，等待文件大小稳定
                                compress_progress['running'] = False
                                
                                # 等待文件大小稳定（最多等待10秒）
                                max_wait_time = 10
                                wait_interval = 0.1
                                wait_count = 0
                                last_size = archive_path.stat().st_size if archive_path.exists() else 0
                                
                                while wait_count < max_wait_time / wait_interval:
                                    time.sleep(wait_interval)  # 使用同步sleep，因为在线程中
                                    wait_count += 1
                                    if archive_path.exists():
                                        current_size = archive_path.stat().st_size
                                        if current_size == last_size:
                                            # 文件大小稳定，压缩完成
                                            break
                                        last_size = current_size
                                
                                compress_progress['completed'] = True
                                compress_progress['bytes_written'] = archive_path.stat().st_size if archive_path.exists() else 0
                                
                                # 存储成功和失败的文件信息
                                compress_result['successful_files'] = successful_files
                                compress_result['failed_files'] = failed_files
                                compress_result['successful_original_size'] = sum(
                                    f['size'] for f in file_group 
                                    if str(f['path']) in successful_files
                                )
                                # py7zr直接使用预设路径，确保使用绝对路径
                                compress_result['archive_path'] = str(archive_path.absolute())
                                
                                logger.info(f"py7zr压缩完成: {len(successful_files)} 个文件成功, {len(failed_files)} 个文件失败")
                            
                    else:
                        # 未启用压缩时，仍旧使用tar存储（TODO：实现真正的无压缩 tar 打包）
                        archive_path = backup_dir / f"backup_{backup_set.set_id}_{timestamp}.tar"
                        logger.warning("当前未启用压缩，暂未实现tar打包，跳过压缩操作")
                        compress_progress['completed'] = True
                        compress_progress['bytes_written'] = 0
                        compress_result['successful_files'] = []
                        compress_result['failed_files'] = []
                        compress_result['successful_original_size'] = 0
                        compress_result['archive_path'] = str(archive_path)
                        
                except Exception as e:
                    logger.error(f"压缩操作失败: {str(e)}")
                    import traceback
                    logger.error(traceback.format_exc())
                    compress_progress['completed'] = True
                    compress_progress['running'] = False
            
            # 在线程池中执行压缩操作
            loop = asyncio.get_event_loop()
            logger.info(f"[压缩] 准备在线程池中执行压缩操作")
            compression_future = loop.run_in_executor(None, _do_7z_compress)
            logger.info(f"[压缩] 压缩任务已提交到线程池，等待完成...")

            # 等待压缩完成，设置超时以避免无限等待
            timeout_seconds = 3600  # 1小时超时
            try:
                # 等待压缩线程完成（支持取消）
                logger.info(f"[压缩] 开始等待压缩线程完成（超时: {timeout_seconds}秒）")
                try:
                    await asyncio.wait_for(compression_future, timeout=timeout_seconds)
                except asyncio.CancelledError:
                    logger.warning(f"[压缩] 压缩任务被取消（Ctrl+C）")
                    # 尝试取消压缩线程（虽然无法直接取消，但至少记录日志）
                    compress_progress['running'] = False
                    compress_progress['completed'] = False
                    raise
                logger.info(f"[压缩] 压缩线程已完成，检查进度标记...")

                # 等待压缩进度标记完成（最多等待30秒）
                max_progress_wait = 30
                progress_wait_count = 0
                while not compress_progress['completed'] and progress_wait_count < max_progress_wait:
                    logger.debug(f"[压缩] 等待进度标记完成... ({progress_wait_count}/{max_progress_wait})")
                    await asyncio.sleep(1)
                    progress_wait_count += 1

                if not compress_progress['completed']:
                    logger.warning("压缩操作完成但进度标记未更新，强制继续")
                    compress_progress['completed'] = True
                    compress_progress['running'] = False
                else:
                    logger.info(f"[压缩] 压缩进度标记已更新为完成")

            except asyncio.TimeoutError:
                logger.error(f"压缩操作超时（{timeout_seconds}秒），强制继续")
                compress_progress['completed'] = True
                compress_progress['running'] = False
            except Exception as compress_error:
                logger.error(f"压缩操作发生异常: {str(compress_error)}")
                compress_progress['completed'] = True
                compress_progress['running'] = False
                raise
            
            # 确定压缩文件的实际路径（使用压缩函数返回的完整路径）
            archive_path_str = compress_result.get('archive_path')
            if archive_path_str:
                temp_archive_path = Path(archive_path_str)
                logger.info(f"压缩函数返回的文件路径: {temp_archive_path}")
            else:
                # 如果压缩函数没有返回路径，回退到预设路径
                if compression_enabled:
                    if compression_method == 'pgzip':
                        suffix = ".tar.gz"
                    elif compression_method == 'tar':
                        suffix = ".tar"
                    elif compression_method == 'zstd':
                        suffix = ".tar.zst"
                    else:
                        suffix = ".7z"
                    temp_archive_path = backup_dir / f"backup_{backup_set.set_id}_{timestamp}{suffix}"
                else:
                    temp_archive_path = backup_dir / f"backup_{backup_set.set_id}_{timestamp}.tar"
                logger.warning(f"压缩函数未返回路径，使用预设路径: {temp_archive_path}")
            
            if not temp_archive_path.exists():
                logger.error(f"压缩文件不存在: {temp_archive_path} (绝对路径: {temp_archive_path.absolute()})")
                return None
            
            logger.info(f"压缩文件存在，大小: {format_bytes(temp_archive_path.stat().st_size)}, 路径: {temp_archive_path}")
            
            # 根据配置决定是否需要移动文件
            # 统一流程：都压缩到temp目录，根据配置决定是否移动到final目录
            if compress_directly_to_tape:
                # 直接压缩到磁带模式：不移动文件，直接使用temp目录中的文件
                final_archive_path = temp_archive_path
                logger.info(f"直接压缩到磁带模式：文件保留在临时目录，不移动: {final_archive_path}")
            else:
                # 非直接压缩模式：压缩完成，将文件从temp目录移动到final目录（后台任务，不阻塞）
                if final_dir is None:
                    # 如果final_dir未定义，说明配置有问题，使用temp目录
                    logger.warning("final_dir未定义，使用temp目录中的文件")
                    final_archive_path = temp_archive_path
                else:
                    final_archive_path = final_dir / temp_archive_path.name
                    
                    # 创建后台任务来移动文件，不阻塞压缩循环
                    async def move_file_to_final_background():
                        """后台任务：将文件从temp移动到final目录"""
                        try:
                            logger.info(f"[文件移动] 后台任务：开始移动文件到正式目录: {temp_archive_path.name}")
                            logger.info(f"[文件移动] 源路径: {temp_archive_path}, 目标路径: {final_archive_path}")
                            
                            # 使用异步文件移动
                            await loop.run_in_executor(None, lambda: shutil.move(str(temp_archive_path), str(final_archive_path)))
                            logger.info(f"[文件移动] 后台任务：文件已移动到正式目录: {final_archive_path}")
                            
                            # 记录关键阶段：文件移动到final目录
                            if backup_task:
                                from backup.backup_db import BackupDB
                                backup_db = BackupDB()
                                backup_db._log_operation_stage_event(
                                    backup_task,
                                    f"[文件已移动到正式目录] {final_archive_path.name}，大小: {format_bytes(final_archive_path.stat().st_size)}"
                                )
                                # 同时更新operation_stage和description
                                await backup_db.update_task_stage_with_description(
                                    backup_task,
                                    "compress",
                                    f"[移动完成] 压缩文件已移动：{final_archive_path.name}"
                                )
                        except asyncio.CancelledError:
                            logger.warning(f"[文件移动] 后台任务：文件移动任务被取消（Ctrl+C）")
                        except Exception as move_error:
                            logger.error(f"[文件移动] 后台任务：移动文件到正式目录失败: {str(move_error)}")
                            # 如果移动失败，文件仍在temp目录中，但返回的路径指向final目录
                            # 这可能会导致后续操作失败，但至少不会阻塞压缩循环
                    
                    # 启动后台任务，不等待完成
                    asyncio.create_task(move_file_to_final_background())
                    logger.info(f"[文件移动] 已启动后台任务移动文件，压缩循环继续执行，不等待移动完成")
                    
                    # 注意：这里返回final路径，即使文件可能还在移动中
                    # 后续操作（如加入移动队列）会检查文件是否存在
                    # 文件大小在移动前后相同，所以使用temp路径获取大小
                    if temp_archive_path.exists():
                        compressed_size = temp_archive_path.stat().st_size
                    else:
                        # 如果temp文件不存在，尝试final路径（可能已经移动完成）
                        try:
                            compressed_size = final_archive_path.stat().st_size
                        except FileNotFoundError:
                            logger.error(f"无法获取压缩文件大小：temp路径不存在，final路径也不存在")
                            return None
            
            # 获取压缩文件大小
            if 'compressed_size' not in locals():
                # 如果上面没有获取到大小（例如 final_dir is None 的情况），现在获取
                if compress_directly_to_tape or final_dir is None:
                    # 直接压缩到磁带模式或final_dir未定义，使用temp路径获取大小
                    compressed_size = temp_archive_path.stat().st_size
                else:
                    # 非直接压缩模式且final_dir已定义，尝试final路径（可能已经移动完成）
                    try:
                        compressed_size = final_archive_path.stat().st_size
                    except FileNotFoundError:
                        # 如果final路径不存在，尝试temp路径（文件可能还在移动中）
                        if temp_archive_path.exists():
                            compressed_size = temp_archive_path.stat().st_size
                        else:
                            logger.error(f"无法获取压缩文件大小：temp和final路径都不存在")
                            return None
            
            # 计算校验和（可选，性能考虑可以跳过）
            checksum = None
            # checksum = calculate_file_checksum(archive_path)  # 注释掉以提高性能
            
            successful_file_count = len(compress_result['successful_files'])
            
            compressed_info = {
                'path': str(final_archive_path.absolute()),  # 使用final目录中的绝对路径（兼容旧代码）
                'temp_path': temp_archive_path,  # temp目录中的路径（Path对象）
                'final_path': final_archive_path,  # final目录中的路径（Path对象）
                'compressed_size': compressed_size,
                'original_size': compress_result['successful_original_size'] or total_original_size,
                'successful_files': successful_file_count,  # 成功压缩的文件数
                'failed_files': 0,  # 失败的文件数（无法精确统计，设为0）
                'checksum': checksum,
                'compression_enabled': compression_enabled,
                'compression_method': compression_method,  # 压缩方法：'pgzip', 'py7zr', '7zip_command', 'tar', 'zstd'
                'compression_level': compression_level if compression_enabled else None,
                'compression_threads': compression_threads if compression_enabled else None
            }
            
            logger.info(f"压缩文件组完成，返回路径: {compressed_info['path']} (文件存在: {final_archive_path.exists()})")

            if compression_enabled:
                compression_ratio = compressed_size / compressed_info['original_size'] if compressed_info['original_size'] > 0 else 0
                logger.info(f"7z压缩完成: {successful_file_count} 个文件, "
                            f"原始大小: {format_bytes(compressed_info['original_size'])}, "
                            f"压缩后: {format_bytes(compressed_size)}, "
                            f"压缩比: {compression_ratio:.2%}, "
                            f"线程数: {compression_threads}")
            else:
                logger.info(f"打包完成: {successful_file_count} 个文件, "
                            f"大小: {format_bytes(compressed_size)}")

            return compressed_info

        except Exception as e:
            logger.error(f"压缩文件组失败: {str(e)}")
            import traceback
            traceback.print_exc()
            return None

    async def _ensure_disk_space(self, target_dir: Path):
        """确保磁盘剩余空间满足 3 * MAX_FILE_SIZE 的要求"""
        try:
            max_file_size = int(getattr(self.settings, 'MAX_FILE_SIZE', 0))
        except Exception:
            max_file_size = 0

        if max_file_size <= 0:
            return

        required_free = max_file_size * 3
        check_interval = getattr(self.settings, 'DISK_CHECK_INTERVAL', 30)
        max_retries = getattr(self.settings, 'DISK_CHECK_MAX_RETRIES', 20)  # 最多重试20次
        retry_count = 0

        while retry_count < max_retries:
            try:
                usage = shutil.disk_usage(str(target_dir))
                free_bytes = usage.free
            except Exception as disk_error:
                logger.warning(f"无法获取磁盘剩余空间（{target_dir}），跳过空间检查: {disk_error}")
                return

            if free_bytes >= required_free:
                logger.info(
                    f"磁盘剩余空间充足：{format_bytes(free_bytes)} >= {format_bytes(required_free)}"
                )
                return

            retry_count += 1
            total_wait_time = retry_count * check_interval
            max_total_wait = max_retries * check_interval

            logger.warning(
                f"磁盘剩余空间不足：{format_bytes(free_bytes)} < {format_bytes(required_free)}，"
                f"暂停压缩，{check_interval} 秒后重试 ({retry_count}/{max_retries}, "
                f"已等待 {total_wait_time}/{max_total_wait} 秒)"
            )

            if retry_count >= max_retries:
                # 达到最大重试次数，抛出异常
                error_msg = (
                    f"磁盘空间持续不足超过 {max_total_wait} 秒。"
                    f"需要空间: {format_bytes(required_free)}, "
                    f"当前可用: {format_bytes(free_bytes)}"
                )
                logger.error(error_msg)
                raise RuntimeError(error_msg)

            await asyncio.sleep(check_interval)

