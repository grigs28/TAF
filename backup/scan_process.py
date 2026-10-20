#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""独立扫描子进程（spawn）——彻底绕开主进程 GIL 争用。

背景：扫描是元数据密集型（每条目都要短暂持有 GIL 构造 dict/计数），与压缩
主循环/预取器/进度泵同进程时扫描速度掉 85%+（实测 17k → 1.1~2.5k 文件/秒）。
子进程方案与 CompressionProcessPool 同范式（spawn、payload 可 pickle、
不继承 DB 连接/事件循环）。

队列协议（与线程版 _nfs_collect_files 一致，消费侧无需改动）：
    ('batch', [(path, size), ...], stats_snapshot)
    ('done', final_stats)

文件记录精简为 (path, size)：预取器分组/压缩/内存库只依赖这两个字段
（name/modified_time/permissions/is_* 仅供已废弃的 DB 写入路径使用）。
"""

import os
import time
from pathlib import Path

import logging

logger = logging.getLogger(__name__)

try:
    import fnmatch as _fnmatch_mod
except ImportError:  # pragma: no cover
    _fnmatch_mod = None


def _compile_excludes(exclude_patterns):
    """与 simple_scanner._compile_excludes 相同逻辑（子进程侧独立副本）"""
    import re
    name_patterns = []
    path_patterns = []
    for p in (exclude_patterns or []):
        p = str(p).strip()
        if not p:
            continue
        if ('/' in p) or ('\\' in p) or p.startswith('*') is False and '/' not in p and '\\' not in p:
            pass
        # 含路径分隔符 → 全路径模式；否则 → 纯名字模式（预编译为单个正则）
        if '/' in p or '\\' in p:
            path_patterns.append(p)
        else:
            # fnmatch 语法转正则（仅名字，锚定全名）
            rx = '^' + re.escape(p).replace(r'\*', '.*').replace(r'\?', '.') + '$'
            name_patterns.append(rx)
    name_re = re.compile('|'.join(name_patterns)) if name_patterns else None
    return name_re, path_patterns


def scan_worker(expanded_paths, exclude_patterns, batch_size, msg_queue, stop_event):
    """子进程入口：扫描目录树，批次写入 msg_queue（mp.Queue，管道直传）。

    Args:
        expanded_paths: 源路径列表（已展开 UNC）
        exclude_patterns: 排除模式
        batch_size: 每批文件数
        msg_queue: multiprocessing.Queue
        stop_event: multiprocessing.Event（主进程置位即停止）
    """
    stats = {
        'total_scanned': 0,
        'total_scanned_bytes': 0,
        'excluded_count': 0,
        'excluded_dirs': 0,
        'excluded_bytes': 0,
        'error_count': 0,
        'error_dirs': 0,
        'dirs_scanned': 0,
        'dirs_skipped': 0,
        'symlinks_skipped': 0,
    }
    name_re, path_patterns = _compile_excludes(exclude_patterns)
    current_batch = []
    start_time = time.time()
    last_log_time = start_time
    dirs_to_scan = list(expanded_paths)
    scanned_dirs = set()

    def _excluded_name(name):
        return name_re is not None and name_re.match(name) is not None

    def _excluded_path(pathname):
        if path_patterns and _fnmatch_mod is not None:
            for p in path_patterns:
                if _fnmatch_mod.fnmatch(pathname, p):
                    return True
        return False

    def _put_batch():
        if current_batch:
            msg_queue.put(('batch', list(current_batch), dict(stats)))
            current_batch.clear()

    try:
        while dirs_to_scan:
            if stop_event.is_set():
                break
            current_dir = dirs_to_scan.pop(0)
            try:
                current_dir_str = str(Path(current_dir).resolve())
            except Exception:
                current_dir_str = str(current_dir)
            if current_dir_str in scanned_dirs:
                stats['dirs_skipped'] += 1
                continue
            scanned_dirs.add(current_dir_str)
            stats['dirs_scanned'] += 1

            # 目录级剪枝
            dir_name = current_dir_str.rsplit('/', 1)[-1]
            if _excluded_name(dir_name) or _excluded_path(current_dir_str):
                stats['excluded_dirs'] += 1
                continue

            try:
                with os.scandir(current_dir_str) as entries:
                    for entry in entries:
                        if stop_event.is_set():
                            break
                        ename = entry.name
                        try:
                            if _excluded_name(ename) or _excluded_path(entry.path):
                                if entry.is_dir(follow_symlinks=False):
                                    stats['excluded_dirs'] += 1
                                else:
                                    stats['excluded_count'] += 1
                                continue
                            if entry.is_symlink():
                                stats['symlinks_skipped'] += 1
                                continue
                            if entry.is_dir(follow_symlinks=False):
                                dirs_to_scan.append(entry.path)
                                continue
                            if entry.is_file(follow_symlinks=False):
                                try:
                                    # NOFOLLOW：利用 READDIRPLUS 缓存属性
                                    st = entry.stat(follow_symlinks=False)
                                except (PermissionError, OSError, FileNotFoundError):
                                    stats['error_count'] += 1
                                    continue
                                stats['total_scanned'] += 1
                                stats['total_scanned_bytes'] += st.st_size
                                # 精简记录：下游（预取/压缩/内存库）只用 path+size
                                current_batch.append((entry.path, st.st_size))
                                if len(current_batch) >= batch_size:
                                    _put_batch()
                                now = time.time()
                                if now - last_log_time >= 60.0:
                                    fps = stats['total_scanned'] / (now - start_time) if now > start_time else 0
                                    _mb = stats['total_scanned_bytes'] / (1024 * 1024)
                                    logger.info(
                                        f"[扫描子进程] 进度: 已扫描 {stats['total_scanned']:,} 个文件 "
                                        f"({_mb / 1024:.2f} GB), 速度 {fps:.0f} 文件/秒, "
                                        f"待扫描目录: {len(dirs_to_scan)}"
                                    )
                                    last_log_time = now
                            else:
                                stats['error_count'] += 1
                        except (PermissionError, OSError, FileNotFoundError):
                            stats['error_count'] += 1
                            continue
                        except Exception:
                            stats['error_count'] += 1
                            continue
            except (PermissionError, OSError):
                stats['error_dirs'] += 1
                continue

        _put_batch()
    except Exception as e:
        logger.error(f"[扫描子进程] 异常: {e}", exc_info=True)
    finally:
        elapsed = time.time() - start_time
        _mb = stats['total_scanned_bytes'] / (1024 * 1024)
        logger.info(
            f"[扫描子进程] 完成: 扫描 {stats['total_scanned']:,} 个文件 "
            f"({_mb / 1024:.2f} GB), 排除目录 {stats['excluded_dirs']:,}, "
            f"排除文件 {stats['excluded_count']:,}, 耗时 {elapsed:.1f}秒"
        )
        msg_queue.put(('done', dict(stats)))
