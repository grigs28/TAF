#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""Zstandard 压缩实现（CLI 管道 + python 库）（自 compressor.py 拆分，逻辑未变）"""

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

try:
    import zstandard as zstd
except ImportError:
    zstd = None

def _compress_with_zstd_cli(
    archive_path: Path,
    file_group: List[Dict],
    backup_task: BackupTask,
    compression_level: int,
    zstd_threads: int,
    compress_progress: Dict,
    total_files: int,
    base_processed_files: int,
) -> Dict:
    """使用 tar | zstd 管道方式压缩，带进度报告"""
    import shutil
    import threading

    archive_path_abs = archive_path.absolute()
    archive_path_abs.parent.mkdir(parents=True, exist_ok=True)

    try:
        threads = max(1, min(int(zstd_threads or 1), 64))
    except (ValueError, TypeError):
        threads = 1

    try:
        level = int(compression_level if compression_level is not None else 3)
    except (ValueError, TypeError):
        level = 3
    level = max(1, min(level, 19))

    successful_files: List[str] = []
    failed_files: List[Dict[str, str]] = []
    source_paths = getattr(backup_task, 'source_paths', None) or []
    total_files_in_group = len(file_group)
    last_log_time = time.time()
    log_interval = 10.0
    last_file_process_time = time.time()

    logger.warning(f"[zstd-cli] 开始创建压缩文件: {archive_path_abs} (level={level}, threads={threads})")
    logger.debug(f"[zstd-cli] 待压缩文件数: {total_files_in_group} 个")

    # 计算总大小
    total_size = sum(f.get('size', 0) or f.get('file_size', 0) or 0 for f in file_group)
    logger.debug(f"[zstd-cli] 文件组总大小: {format_bytes(total_size)}")

    # 启动 zstd 子进程，从 stdin 读取
    zstd_cmd = [
        'zstd',
        f'-{level}',           # 压缩等级
        f'-T{threads}',        # 线程数
        '-f',                  # 强制覆盖
        '-o', str(archive_path_abs),  # 输出文件
        '-'                    # 从 stdin 读取
    ]

    logger.debug(f"[zstd-cli] 执行命令: {' '.join(zstd_cmd)}")

    zstd_process = None
    try:
        if shutil.which('tar') and file_group:
            # ===== 系统 tar（C 实现）打包：tar -cf - | zstd，不占 GIL =====
            import tempfile
            _t_start = time.time()
            name_to_paths = {}
            groups = []  # [(base_dir, [arcname, ...]), ...]，连续同根分组
            for file_info in file_group:
                fp = Path(file_info['path'])
                fsize = file_info.get('size', 0) or file_info.get('file_size', 0) or 0
                if fsize == 0:
                    try:
                        fsize = fp.stat().st_size
                    except (OSError, FileNotFoundError) as path_error:
                        logger.warning(f"[zstd-cli] 文件不存在或无法访问，跳过: {fp}")
                        failed_files.append({'path': str(fp), 'reason': f'文件不存在或无法访问: {str(path_error)}'})
                        continue
                arcname = fp.name
                base_dir = str(fp.parent)
                for src_path in source_paths:
                    src = Path(src_path)
                    try:
                        if fp.is_relative_to(src):
                            arcname = str(fp.relative_to(src))
                            base_dir = str(src)
                            break
                    except (ValueError, AttributeError):
                        continue
                if groups and groups[-1][0] == base_dir:
                    groups[-1][1].append(arcname)
                else:
                    groups.append((base_dir, [arcname]))
                name_to_paths.setdefault(arcname, []).append((str(fp), fsize))

            if not groups:
                # 全部文件缺失：生成空 tar（与 Python 路径行为一致）
                zstd_process = subprocess.Popen(
                    zstd_cmd, stdin=subprocess.PIPE,
                    stdout=subprocess.PIPE, stderr=subprocess.PIPE,
                )
                with tarfile.open(fileobj=zstd_process.stdin, mode='w|') as _empty_tar:
                    pass
                if zstd_process.stdin and not zstd_process.stdin.closed:
                    zstd_process.stdin.close()
                zstd_process.wait()
                if zstd_process.returncode != 0:
                    raise RuntimeError(f"zstd 压缩失败 (返回码: {zstd_process.returncode})")
            else:
                tmpdir = tempfile.mkdtemp(prefix='taf_zstdcli_')
                argv = ['tar', '-cf', '-', '-v']
                list_files = []
                for gi, (base, names) in enumerate(groups):
                    lf = os.path.join(tmpdir, f'list{gi}.txt')
                    with open(lf, 'w') as fh:
                        fh.write(chr(10).join(names) + chr(10))
                    list_files.append(lf)
                    argv += ['-C', base, '-T', lf]

                tar_process = subprocess.Popen(argv, stdout=subprocess.PIPE, stderr=subprocess.PIPE)
                zstd_process = subprocess.Popen(
                    zstd_cmd, stdin=tar_process.stdout,
                    stdout=subprocess.PIPE, stderr=subprocess.PIPE,
                )
                tar_process.stdout.close()  # 父进程释放读端

                done_names = 0
                done_bytes = 0
                _last_log = time.time()
                tar_errors = []
                try:
                    for raw in iter(tar_process.stderr.readline, b''):
                        line = raw.decode('utf-8', errors='replace').rstrip(chr(10))
                        if not line:
                            continue
                        if line.startswith('tar: '):
                            tar_errors.append(line)
                            continue
                        target = name_to_paths.get(line)
                        fsize = 0
                        if target:
                            path_str, fsize = target.pop(0)
                            successful_files.append(path_str)
                        done_names += 1
                        done_bytes += fsize
                        compress_progress['current_file_index'] = done_names
                        compress_progress['total_files_in_group'] = total_files_in_group
                        compress_progress['processed_bytes'] = compress_progress.get('processed_bytes', 0) + fsize
                        if total_files > 0:
                            backup_task.progress_percent = min(
                                100.0, 10.0 + ((base_processed_files + done_names) / total_files) * 90.0
                            )
                        now = time.time()
                        if (done_names == 1 or done_names % 100 == 0
                                or done_names == total_files_in_group
                                or now - _last_log >= log_interval):
                            if hasattr(backup_task, 'current_compression_progress'):
                                backup_task.current_compression_progress = {
                                    'current': done_names,
                                    'total': total_files_in_group,
                                    'percent': (done_bytes / total_size * 100.0) if total_size > 0 else 0.0,
                                    'group_size_bytes': total_size,
                                    'processed_bytes': done_bytes,
                                }
                            _last_log = now
                finally:
                    tar_process.wait()
                    zstd_process.wait()
                    try:
                        for lf in list_files:
                            os.unlink(lf)
                        os.rmdir(tmpdir)
                    except OSError:
                        pass

                for line in tar_errors:
                    body = line[5:]
                    sep = body.find(': Cannot')
                    if sep == -1:
                        logger.warning(f"[zstd-cli] tar: {body}")
                        continue
                    name_part = body[:sep]
                    reason = body[sep + 2:]
                    err_target = name_to_paths.get(name_part)
                    full = err_target.pop(0)[0] if err_target else name_part
                    failed_files.append({'path': full, 'reason': reason})

                if zstd_process.returncode != 0:
                    error_msg = ''
                    try:
                        error_msg = (zstd_process.stderr.read() or b'').decode('utf-8', errors='replace')
                    except Exception:
                        error_msg = ''
                    if not error_msg:
                        error_msg = f"未知错误 (返回码: {zstd_process.returncode})"
                    logger.error(f"[zstd-cli] 压缩失败: {error_msg}")
                    raise RuntimeError(f"zstd 压缩失败: {error_msg}")
                logger.info(
                    f"[zstd-cli] 系统 tar 打包完成: {len(successful_files)} 个文件 "
                    f"(耗时 {time.time() - _t_start:.1f}s, tar rc={tar_process.returncode})"
                )
        else:
            zstd_process = subprocess.Popen(
                zstd_cmd,
                stdin=subprocess.PIPE,
                stdout=subprocess.PIPE,
                stderr=subprocess.PIPE
            )
            # 使用 tarfile 写入 zstd 的 stdin（管道方式）
            with tarfile.open(fileobj=zstd_process.stdin, mode='w|') as tar:
                _apply_tar_copybufsize(tar)
                for file_idx, file_info in enumerate(file_group):
                    file_path = Path(file_info['path'])

                    # 进度日志（每100个文件或每10秒）
                    current_time = time.time()
                    if file_idx % 100 == 0 or (current_time - last_log_time) >= log_interval:
                        current_progress = file_idx + 1
                        progress_percent = (current_progress / max(total_files_in_group, 1) * 100)
                        elapsed_time = current_time - last_log_time if file_idx > 0 else 0
                        files_per_sec = 100 / elapsed_time if elapsed_time > 0 and file_idx % 100 == 0 else 0
                        logger.debug(
                            f"[zstd-cli] 压缩进度: {current_progress}/{total_files_in_group} ({progress_percent:.1f}%)"
                            + (f" - 处理速度: {files_per_sec:.1f} 文件/秒" if files_per_sec > 0 else "")
                        )

                        # 更新 compress_progress
                        compress_progress['current_file_index'] = current_progress
                        compress_progress['total_files_in_group'] = total_files_in_group

                        # 计算当前已处理字节数
                        processed_bytes = compress_progress.get('processed_bytes', 0)
                        total_group_size = total_size

                        # 按文件大小计算进度百分比
                        if total_group_size > 0 and processed_bytes >= 0:
                            size_based_percent = (processed_bytes / total_group_size * 100)
                            actual_progress_percent = size_based_percent
                        else:
                            actual_progress_percent = progress_percent

                        # 更新 backup_task.current_compression_progress
                        if hasattr(backup_task, 'current_compression_progress'):
                            backup_task.current_compression_progress = {
                                'current': current_progress,
                                'total': total_files_in_group,
                                'percent': actual_progress_percent,
                                'group_size_bytes': total_group_size,
                                'processed_bytes': processed_bytes
                            }

                        last_log_time = current_time

                    # 获取文件大小
                    file_size = file_info.get('size', 0) or file_info.get('file_size', 0) or 0
                    file_size_display = format_bytes(file_size) if file_size > 0 else "未知"

                    if file_size == 0:
                        try:
                            file_stat = file_path.stat()
                            file_size = file_stat.st_size
                        except (OSError, FileNotFoundError) as path_error:
                            logger.warning(f"[zstd-cli] 文件不存在或无法访问，跳过: {file_path}")
                            failed_files.append({'path': str(file_path), 'reason': f'文件不存在或无法访问: {str(path_error)}'})
                            continue

                    # 设置 tar 中的文件名
                    arcname = file_path.name
                    for src_path in source_paths:
                        src = Path(src_path)
                        try:
                            if file_path.is_relative_to(src):
                                arcname = str(file_path.relative_to(src))
                                break
                        except (ValueError, AttributeError):
                            continue

                    # 记录文件处理开始时间
                    file_start_time = time.time()

                    # 大文件警告
                    if file_size > 100 * 1024 * 1024:
                        logger.debug(f"[zstd-cli] 开始处理大文件 ({file_size_display}): {file_path.name}")

                    try:
                        tar.add(file_path, arcname=arcname)
                        successful_files.append(str(file_path))

                        # 累计已处理字节数
                        if 'processed_bytes' not in compress_progress:
                            compress_progress['processed_bytes'] = 0
                        compress_progress['processed_bytes'] += file_size

                        # 慢文件处理警告
                        file_process_time = time.time() - file_start_time
                        if file_process_time > 30.0:
                            logger.debug(
                                f"[zstd-cli] 文件处理时间较长 ({file_process_time:.1f}秒, 大小: {file_size_display}): {file_path}"
                            )

                        # 每1000个文件记录速度统计
                        if (file_idx + 1) % 1000 == 0:
                            elapsed = time.time() - last_log_time
                            files_per_sec = 1000.0 / elapsed if elapsed > 0 else 0
                            logger.debug(f"[zstd-cli] 最近1000个文件耗时: {elapsed:.1f}秒，{files_per_sec:.1f}文件/秒")
                            last_log_time = time.time()

                        # 更新任务进度百分比
                        if total_files > 0:
                            current_processed = base_processed_files + file_idx + 1
                            compress_progress_value = 10.0 + (current_processed / total_files) * 90.0
                            backup_task.progress_percent = min(100.0, compress_progress_value)

                    except Exception as add_error:
                        # Broken pipe 说明 zstd 进程已死（通常是关闭时被 kill），立即停止
                        if isinstance(add_error, BrokenPipeError) or 'Broken pipe' in str(add_error):
                            logger.info(f"[zstd-cli] 管道已断开（zstd 进程已终止），停止压缩")
                            break
                        logger.warning(f"[zstd-cli] 添加文件失败: {file_path}, 错误: {add_error}")
                        failed_files.append({'path': str(file_path), 'reason': f'写入失败: {add_error}'})
                        continue

            # 关闭 stdin，等待 zstd 完成
            # 注意：stdin 可能已在 tarfile 退出时关闭，检查后再关闭
            if zstd_process.stdin and not zstd_process.stdin.closed:
                zstd_process.stdin.close()
            # 使用 wait() 而不是 communicate()，因为我们已经通过管道写入数据
            zstd_process.wait()
            stdout, stderr = zstd_process.stdout, zstd_process.stderr

            if zstd_process.returncode != 0:
                error_msg = ""
                if stderr:
                    try:
                        error_msg = stderr.read().decode() if hasattr(stderr, 'read') else str(stderr)
                    except:
                        error_msg = str(stderr)
                if not error_msg and stdout:
                    try:
                        error_msg = stdout.read().decode() if hasattr(stdout, 'read') else str(stdout)
                    except:
                        pass
                if not error_msg:
                    error_msg = f"未知错误 (返回码: {zstd_process.returncode})"
                logger.error(f"[zstd-cli] 压缩失败: {error_msg}")
                raise RuntimeError(f"zstd 压缩失败: {error_msg}")

        # 获取压缩后大小
        if archive_path_abs.exists():
            compressed_size = archive_path_abs.stat().st_size
        else:
            compressed_size = 0

        # 计算压缩比
        if total_size > 0 and compressed_size > 0:
            compression_ratio = (1 - compressed_size / total_size) * 100
            logger.info(
                f"[zstd-cli] 压缩完成: {format_bytes(total_size)} -> {format_bytes(compressed_size)} "
                f"(压缩率: {compression_ratio:.1f}%)"
            )

        logger.info(f"[zstd-cli] 成功: {len(successful_files)} 个文件, 失败: {len(failed_files)} 个文件")

        _finalize_compression_progress(compress_progress, archive_path_abs)

        return {
            'success': len(failed_files) == 0,
            'successful_files': successful_files,
            'failed_files': failed_files,
            'successful_original_size': sum(f.get('size', 0) or 0 for f in file_group if str(f.get('path')) in successful_files),
            'archive_path': str(archive_path_abs)
        }

    except Exception as e:
        logger.error(f"[zstd-cli] 压缩过程出错: {str(e)}")
        # 终止子进程
        try:
            zstd_process.terminate()
            zstd_process.wait(timeout=5)
        except:
            pass
        raise


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
    """使用 Zstandard 压缩（先打包成tar，再用zstd压缩）- Python库版本"""
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
    log_interval = 10.0  # 改为10秒
    last_file_process_time = time.time()

    logger.warning(f"[zstd] 开始创建压缩文件: {archive_path_abs} (level={level}, threads={threads})")
    logger.debug(f"[zstd] 待压缩文件数: {total_files_in_group} 个，预计耗时较长")
    
    # 计算平均文件大小
    total_size = sum(f.get('size', 0) or f.get('file_size', 0) or 0 for f in file_group)
    avg_file_size = total_size / max(total_files_in_group, 1) if total_files_in_group > 0 else 0
    
    # 根据平均文件大小设置 zstd_write_size（细分策略）
    # 策略：
    # - 平均文件大小 < 128KB → 使用 128KB
    # - 平均文件大小 < 256KB → 使用 256KB
    # - 平均文件大小 < 512KB → 使用 512KB
    # - 平均文件大小 < 1MB → 使用 1MB
    # - 平均文件大小 < 5MB → 使用 2MB
    # - 平均文件大小 < 10MB → 使用 4MB
    # - 平均文件大小 < 100MB → 使用平均文件大小的 1/10（最小4MB，最大10MB）
    # - 平均文件大小 >= 100MB → 使用 10MB
    # 范围：最小 128KB，最大 10MB
    if avg_file_size < 128 * 1024:  # < 64KB
        zstd_write_size = 128 * 1024  # 128KB
    elif avg_file_size < 256 * 1024:  # < 256KB
        zstd_write_size = 256 * 1024  # 256KB
    elif avg_file_size < 512 * 1024:  # < 512KB
        zstd_write_size = 512 * 1024  # 512KB
    elif avg_file_size < 1024 * 1024:  # < 1MB
        zstd_write_size = 1024 * 1024  # 1MB
    elif avg_file_size < 5 * 1024 * 1024:  # < 5MB
        zstd_write_size = 2 * 1024 * 1024  # 2MB
    elif avg_file_size < 10 * 1024 * 1024:  # < 10MB
        zstd_write_size = 4 * 1024 * 1024  # 4MB
    elif avg_file_size < 100 * 1024 * 1024:  # < 100MB
        # 使用平均文件大小的 1/10，最小4MB，最大10MB
        calculated_size = int(avg_file_size / 10)
        zstd_write_size = max(4 * 1024 * 1024, min(calculated_size, 10 * 1024 * 1024))  # 4MB ~ 10MB
    else:  # >= 100MB
        zstd_write_size = 10 * 1024 * 1024  # 10MB
    
    # 如果配置中有 ZSTD_WRITE_SIZE，优先使用配置值（但需要在合理范围内）
    from config.settings import get_settings
    settings = get_settings()
    config_write_size = getattr(settings, 'ZSTD_WRITE_SIZE', None)
    if config_write_size is not None:
        # 如果配置值在合理范围内，使用配置值
        config_write_size = int(config_write_size)
        if 128 * 1024 <= config_write_size <= 10 * 1024 * 1024:
            zstd_write_size = config_write_size
        else:
            logger.warning(f"[zstd] 配置的 ZSTD_WRITE_SIZE ({config_write_size}) 不在合理范围内 (128KB ~ 10MB)，使用根据平均文件大小计算的值: {zstd_write_size}")
    
    logger.debug(f"[zstd] 文件组总大小: {format_bytes(total_size)}, 平均文件大小: {format_bytes(avg_file_size)}, 写入缓冲区大小: {format_bytes(zstd_write_size)}")
    
    try:
        with archive_path_abs.open('wb') as raw_out:
            compressor = zstd.ZstdCompressor(level=level, threads=threads)
            logger.debug(f"[zstd] 使用写入缓冲区大小: {format_bytes(zstd_write_size)}")
            with compressor.stream_writer(raw_out, closefd=False, write_size=zstd_write_size) as zstd_stream:
                with tarfile.open(fileobj=zstd_stream, mode='w|') as tar:
                    _apply_tar_copybufsize(tar)
                    for file_idx, file_info in enumerate(file_group):
                        file_path = Path(file_info['path'])

                        current_time = time.time()
                        # 每100个文件或每10秒记录一次进度，避免长时间无日志
                        # 注意：这里只在 tar.add() 之前检查，以便及时发现长时间处理的文件
                        if file_idx % 100 == 0 or (current_time - last_log_time) >= log_interval:
                            current_progress = file_idx + 1
                            progress_percent = (current_progress / max(total_files_in_group, 1) * 100) if total_files_in_group > 0 else 0
                            elapsed_time = current_time - last_log_time if file_idx > 0 else 0
                            files_per_sec = 100 / elapsed_time if elapsed_time > 0 and file_idx % 100 == 0 else 0
                            logger.debug(
                                f"[zstd] 压缩进度: {current_progress}/{total_files_in_group} 个文件 ({progress_percent:.1f}%)"
                                + (f" - 处理速度: {files_per_sec:.1f} 文件/秒" if files_per_sec > 0 else "")
                            )
                            # 将压缩进度信息存储到 compress_progress 和 backup_task 中
                            compress_progress['current_file_index'] = current_progress
                            compress_progress['total_files_in_group'] = total_files_in_group
                            
                            # 计算当前文件组的原始总大小（压缩前）
                            total_group_size = sum(f.get('size', 0) or 0 for f in file_group)
                            # 获取已处理文件的实际大小（如果已累计）
                            processed_bytes = compress_progress.get('processed_bytes', 0)
                            
                            # 按文件大小计算进度百分比（更准确）
                            if total_group_size > 0 and processed_bytes >= 0:
                                size_based_percent = (processed_bytes / total_group_size * 100) if total_group_size > 0 else 0.0
                                # 使用文件大小计算的百分比（更准确）
                                actual_progress_percent = size_based_percent
                            else:
                                # 如果没有大小信息，回退到文件数百分比
                                actual_progress_percent = progress_percent
                            
                            if hasattr(backup_task, 'current_compression_progress'):
                                backup_task.current_compression_progress = {
                                    'current': current_progress,
                                    'total': total_files_in_group,
                                    'percent': actual_progress_percent,
                                    'group_size_bytes': total_group_size,  # 文件组的原始总大小（压缩前）
                                    'processed_bytes': processed_bytes  # 已处理文件的实际大小
                                }
                            last_log_time = current_time

                        # 优化：优先使用扫描时已获取的文件大小，避免重复调用 stat()
                        # 从 file_info 中获取大小（扫描阶段已通过 get_file_info() 获取）
                        file_size = file_info.get('size', 0) or file_info.get('file_size', 0) or 0
                        
                        # 如果 file_info 中没有大小信息，才调用 stat() 获取（后备方案，但通常不会执行）
                        if file_size == 0:
                            try:
                                file_stat = file_path.stat()
                                file_size = file_stat.st_size
                            except (OSError, FileNotFoundError) as path_error:
                                logger.warning(f"[zstd] 文件不存在或无法访问，跳过: {file_path} (错误: {str(path_error)})")
                                failed_files.append({'path': str(file_path), 'reason': f'文件不存在或无法访问: {str(path_error)}'})
                                continue
                        
                        file_size_display = format_bytes(file_size) if file_size > 0 else "未知"
                        
                        # 注意：不检查文件是否存在，因为：
                        # 1. file_info 中有 size 说明扫描时文件存在
                        # 2. 如果文件不存在，tar.add() 会抛出异常，在异常处理中处理

                        arcname = file_path.name
                        for src_path in source_paths:
                            src = Path(src_path)
                            try:
                                if file_path.is_relative_to(src):
                                    arcname = str(file_path.relative_to(src))
                                    break
                            except (ValueError, AttributeError):
                                continue

                        # 记录文件处理开始时间和文件信息
                        file_start_time = time.time()
                        
                        # 如果文件很大（超过 100MB），在处理前记录日志
                        if file_size > 100 * 1024 * 1024:
                            logger.debug(f"[zstd] 开始处理大文件 ({file_size_display}): {file_path.name}")
                        
                        try:
                            tar.add(file_path, arcname=arcname)
                            successful_files.append(str(file_path))
                            # 累计已处理文件的实际大小（用于按文件大小计算百分比）
                            if 'processed_bytes' not in compress_progress:
                                compress_progress['processed_bytes'] = 0
                            compress_progress['processed_bytes'] += file_size

                            # 如果单个文件处理时间超过30秒，记录警告（包含文件大小）
                            file_process_time = time.time() - file_start_time
                            if file_process_time > 30.0:
                                logger.debug(
                                    f"[zstd] 文件处理时间较长 ({file_process_time:.1f}秒, 大小: {file_size_display}): {file_path}"
                                )
                            
                            # 每1000个文件记录一次详细的处理时间统计（重置统计时间点）
                            if (file_idx + 1) % 1000 == 0:
                                # 注意：这里使用 last_log_time 而不是 last_file_process_time
                                # 因为 last_file_process_time 在每100个文件时已更新
                                elapsed = time.time() - last_log_time
                                files_per_sec = 1000.0 / elapsed if elapsed > 0 else 0
                                logger.debug(f"[zstd] 最近1000个文件耗时: {elapsed:.1f}秒，{files_per_sec:.1f}文件/秒")
                                last_log_time = time.time()
                            
                            # 更新任务进度百分比（不更新 bytes_written，避免频繁调用 stat()）
                            # bytes_written 会在压缩完成后由 _finalize_compression_progress 一次性获取
                            if total_files > 0:
                                current_processed = base_processed_files + file_idx + 1
                                compress_progress_value = 10.0 + (current_processed / total_files) * 90.0
                                backup_task.progress_percent = min(100.0, compress_progress_value)
                        except Exception as add_error:
                            if isinstance(add_error, BrokenPipeError) or 'Broken pipe' in str(add_error):
                                logger.info(f"[zstd] 管道已断开，停止压缩")
                                break
                            logger.warning(f"[zstd] 添加文件失败: {file_path}, 错误: {add_error}")
                            failed_files.append({'path': str(file_path), 'reason': f'写入失败: {add_error}'})
                            continue

        # 计算原始文件总大小
        successful_original_size = sum(f.get('size', 0) or 0 for f in file_group if str(f.get('path')) in successful_files)
        
        # 压缩完成后，获取一次最终文件大小（避免压缩过程中频繁调用 stat()）
        compressed_size = 0
        try:
            if archive_path_abs.exists():
                compressed_size = archive_path_abs.stat().st_size
                compress_progress['bytes_written'] = compressed_size
        except Exception:
            compressed_size = 0
            compress_progress['bytes_written'] = 0
        
        compress_progress['completed'] = True
        compress_progress['running'] = False
        
        # 计算压缩比（如果 compressed_size 为 0，压缩比也为 0）
        compression_ratio = (compressed_size / successful_original_size) if (successful_original_size > 0 and compressed_size > 0) else 0.0
        
        # 格式化压缩比和压缩后大小
        compression_ratio_str = f"{compression_ratio:.2%}" if compression_ratio > 0 else "未知"
        compressed_size_str = format_bytes(compressed_size) if compressed_size > 0 else "未知"
        logger.warning(f"[zstd] 压缩完成：{len(successful_files)} 个文件成功，{len(failed_files)} 个失败，"
                   f"原始大小: {format_bytes(successful_original_size)}, "
                   f"压缩后大小: {compressed_size_str}, "
                   f"压缩比: {compression_ratio_str}")

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
        
        # 只保留已经成功添加的文件在 successful_files 中，不把它们移到 failed_files
        # 只把真正没写成功的文件（已经在 failed_files 中的，或者在循环中还没处理的）放进 failed_files
        # 找出所有文件路径，把未成功的添加到 failed_files
        all_file_paths = set()
        file_info_map = {}  # {file_path_str: file_info}
        for f in file_group:
            file_path_str = str(Path(f.get('path', '')).absolute())
            all_file_paths.add(file_path_str)
            file_info_map[file_path_str] = f
        
        successful_file_paths = {str(Path(p).absolute()) for p in successful_files}
        failed_file_paths = {str(Path(f.get('path', '')).absolute()) for f in failed_files}
        
        # 找出那些既不在 successful_files 也不在 failed_files 的文件（循环中还没处理的）
        unprocessed_files = all_file_paths - successful_file_paths - failed_file_paths
        for unprocessed_path in unprocessed_files:
            # 使用原始路径（从 file_info 中获取）
            original_path = file_info_map.get(unprocessed_path, {}).get('path', unprocessed_path)
            failed_files.append({'path': original_path, 'reason': 'zstd压缩过程异常，文件未处理'})
        
        # 计算成功文件的原始大小（只计算已经成功添加的文件）
        successful_original_size = sum(
            f.get('size', 0) or 0 
            for f in file_group 
            if str(Path(f.get('path', '')).absolute()) in successful_file_paths
        )

        # 标记压缩完成（即使失败也要标记，避免无限等待）
        _finalize_compression_progress(compress_progress, archive_path_abs)

        return {
            'successful_files': successful_files,  # 保留已经成功添加的文件
            'failed_files': failed_files,  # 只包含真正失败的文件
            'successful_original_size': successful_original_size,
            'archive_path': str(archive_path_abs)
        }
