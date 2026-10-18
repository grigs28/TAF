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
from typing import List, Dict, Optional, Callable, Awaitable, Any

from models.backup import BackupSet, BackupTask
from utils.datetime_utils import now, format_datetime
from backup.utils import format_bytes
from backup.compression_process_pool import _submit_compression_payload, CompressionProcessPool

logger = logging.getLogger(__name__)


# ========== 拆分后的实现模块（re-export，保持 from backup.compressor import ... 兼容） ==========
from backup.compress_helpers import (  # noqa: F401
    _parse_size_to_bytes,
    _finalize_compression_progress,
    _apply_tar_copybufsize,
)
from backup.compress_7zip import _compress_with_7zip_command  # noqa: F401
from backup.compress_pgzip import _compress_with_pgzip  # noqa: F401
from backup.compress_tar import _compress_with_tar  # noqa: F401
from backup.compress_zstd import _compress_with_zstd_cli, _compress_with_zstd  # noqa: F401

# 尝试导入psutil用于内存检查
try:
    import psutil
    PSUTIL_AVAILABLE = True
except ImportError:
    PSUTIL_AVAILABLE = False
    logger.warning("psutil未安装，无法检查系统内存，将使用默认内存分配策略")


def _compute_required_free_bytes(settings, active_groups: Optional[int] = None) -> int:
    """磁盘守卫线 = 组数 × 最大文件大小 × 安全系数（默认 1.2）

    覆盖"当前在途并行组"的最大产出（每组压缩后 ≤ MAX_FILE_SIZE），再乘安全系数留缓冲。

    组数取值：
    - active_groups 给定时用**实际在途组数**（含正在做空间检查的这一组）。
      这样 6 组在途、想开第 7 组时判定线是 7×24G 而非 8×24G，
      避免组数未达上限时被无谓拦停（拦停过频还会造成守卫线附近反复震荡）。
    - 未给定（或 ≤0）时回退到配置上限 COMPRESSION_PARALLEL_BATCHES（保守）。

    安全系数由 DISK_GUARD_SAFETY_FACTOR 配置（默认 1.2）：
    - 1.2 → 全部在途产出落地后仍剩"在途量 × 20%"空闲
    - 调大更保守（缓冲更厚），调小更激进（SSD 用得更满）
    """
    try:
        max_file_size = int(getattr(settings, 'MAX_FILE_SIZE', 0) or 0)
    except Exception:
        max_file_size = 0
    if max_file_size <= 0:
        return 0
    try:
        if active_groups is not None and int(active_groups) > 0:
            groups = int(active_groups)
        else:
            groups = max(1, int(getattr(settings, 'COMPRESSION_PARALLEL_BATCHES', 3) or 3))
    except Exception:
        groups = 3
    try:
        factor = float(getattr(settings, 'DISK_GUARD_SAFETY_FACTOR', 1.2) or 1.2)
    except Exception:
        factor = 1.2
    if factor < 1.0:  # 小于 1 会让在途产出越界，不接受
        factor = 1.0
    return int(max_file_size * groups * factor)


class Compressor:
    """压缩处理器"""
    
    def __init__(self, settings=None):
        """初始化压缩处理器
        
        Args:
            settings: 系统设置对象
        """
        self.settings = settings
        # 由 CompressionWorker 注入：返回"当前在途压缩组数"（含调用者自己）。
        # 用于空间守卫**动态**判定线；未注入时回退到配置的并发上限（保守）。
        self.active_groups_provider: Optional[Callable[[], int]] = None
        # **真正会产出数据**的在途组记账：每个元素是 (该组源文件字节数, 产出文件绝对路径)。
        #   len(...) = 在途组数（不含"压缩已完成、正在标注/移动到 final"的组——
        #              那些组同盘移动、不再新增占用，计入会抬高守卫线，实测虚高 6 组）
        #   sum(源大小) = 这些组的产出上界（压缩后只会更小，故源大小即保守上界）
        #   产出路径 = 用于扣除"已经落地的字节数"：守卫需求按**剩余待产出**计算。
        #              组刚启动（文件为空/未创建）时扣除量为 0，与纯源大小口径
        #              逐字节一致，因此最坏情况（全部组同时刚启动）的保护不减弱。
        # 守卫线据此按**真实剩余产出**计算，而不是按"组数 × MAX_FILE_SIZE"的理论最大值——
        # 后者在小文件区会严重高估（实测组只有 0.2~4GB 却按 24GB 计），白闲置数百 GB。
        self._active_groups: List[tuple] = []   # List[Tuple[int, str]]
        # **全局活跃度计时**：最后一次有组**真正通过守卫**（空间检查放行）的时刻。
        # 所有等待中的组共享这一个时间戳——用于区分两种"等很久"：
        #   · 系统健康、只是轮不到我  → 只要有别的组被放行，本组继续等（不误杀）
        #   · 系统卡死（全场无人能过守卫）→ 超过阈值即报错，快速暴露
        # 注意：重置信号必须是"通过守卫"而不是"组进入流程"——刚进流程的组也可能
        # 卡在同一个守卫里，不能作为"系统还活着"的证据。
        self._last_release_time: Optional[float] = None
        # 守卫日志节流：等待中的组每 20 秒各重试一次，若每条都打日志，
        # 6 组等待时每分钟就是 18 条重复日志。改为：
        #   · 首次进入拦停 → 立即 INFO
        #   · 其后每 5 分钟 → 一条汇总
        #   · 放行时 → 一条 INFO（含本次拦停持续时长）
        self._guard_active: bool = False
        self._last_guard_log_time: float = 0.0
        # 记录每个备份集的上次压缩文件名和序号，确保文件名不重复
        # 格式: {backup_set_id: {'last_filename': str, 'sequence': int}}
        self._last_archive_info: Dict[str, Dict[str, Any]] = {}
        # 在途组的类型（'large'/'small'），与 _active_groups 严格同序增删，
        # 供卡片"压缩组: X/Y（L a / S b）"显示。
        self._active_group_types: List[str] = []
        try:
            self._group_large_threshold = int(
                getattr(settings, 'GROUP_LARGE_FILE_COUNT_THRESHOLD', 20000) or 20000)
        except Exception:
            self._group_large_threshold = 20000

    @property
    def _active_group_bytes(self) -> List[int]:
        """在途组源大小列表（兼容旧读取方：任务查询/压缩 worker 的 len/sum）"""
        return [int(src) for src, _path in self._active_groups]

    @property
    def _active_compressing(self) -> int:
        """在途（会产出数据的）组数"""
        return len(self._active_groups)

    def _release_active_group(self, group_bytes: int = 0, group_path: str = "") -> None:
        """释放一个在途组的记账（优先按 (源字节, 路径) 精确匹配，容忍大小重复）"""
        try:
            idx = self._active_groups.index((int(group_bytes), str(group_path)))
            del self._active_groups[idx]
            if idx < len(self._active_group_types):
                del self._active_group_types[idx]
            return
        except ValueError:
            pass
        # 路径不一致（异常路径/旧记录）：按值删除首个匹配项
        for idx, (src, _p) in enumerate(self._active_groups):
            if int(src) == int(group_bytes):
                del self._active_groups[idx]
                if idx < len(self._active_group_types):
                    del self._active_group_types[idx]
                break
    
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
        logger.debug(f"文件分组完成: {len(groups)} 个组, "
                   f"组大小范围: {format_bytes(min(group_sizes))} - {format_bytes(max(group_sizes))}, "
                   f"平均组大小: {format_bytes(sum(group_sizes) / len(group_sizes))}")
        
        return groups
    
    async def compress_file_group(
        self, 
        file_group: List[Dict], 
        backup_set: BackupSet, 
        backup_task: BackupTask,
        base_processed_files: int = 0, 
        total_files: int = 0,
        shared_compress_progress: Optional[Dict] = None
    ) -> Optional[Dict]:
        """压缩文件组（异步启动，立即返回，不等待压缩完成）
        
        Args:
            file_group: 文件组列表
            backup_set: 备份集对象
            backup_task: 备份任务对象
            base_processed_files: 已处理文件数基数
            total_files: 总文件数
            
        Returns:
            压缩文件信息字典（包含预设路径和压缩任务future），立即返回，不等待压缩完成
        """
        # 本组的产出量上界（源文件总字节数；压缩后只会更小，属保守估计）
        try:
            _group_bytes = sum(
                int(f.get('size', 0) or f.get('file_size', 0) or 0) for f in file_group
            )
        except Exception:
            _group_bytes = 0
        _active_released = False
        _active_counted = False   # 是否已计入"在途产出"（通过守卫后才计入）
        # 注意：**不在这里 append**。此刻本组可能立刻阻塞在 _ensure_disk_space 里
        # 长时间等待而不产出任何字节；若提前计入，会把判定线抬高（实测 16 组里
        # 有 13 组在排队，线被撑到 351G，SSD 168G 永远不够 → 自我强化地卡住）。
        # 正确做法：通过守卫（真正开始压缩）后才计入。
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
            
            # 从系统配置获取线程数（基础配置）
            compression_threads = int(getattr(self.settings, "COMPRESSION_THREADS", 4))
            
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
            
            # 获取 zstd 线程数配置
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
            
            # 记录内存信息（如果可用，zstd 压缩方法不执行）
            if compression_method != 'zstd':
                if PSUTIL_AVAILABLE:
                    try:
                        mem = psutil.virtual_memory()
                        total_memory_gb = mem.total / (1024 ** 3)
                        available_memory_gb = mem.available / (1024 ** 3)
                        logger.debug(f"系统内存: 总计={total_memory_gb:.1f}GB, 可用={available_memory_gb:.1f}GB | "
                                   f"固定字典={dict_size_str} ({dict_size_gb:.3f}GB), "
                                   f"线程={compression_command_threads}, 预计内存={memory_gb}GB")
                    except Exception as e:
                        logger.debug(f"固定字典={dict_size_str}, 线程={compression_command_threads}, 预计内存={memory_gb}GB")
                else:
                    logger.debug(f"固定字典={dict_size_str}, 线程={compression_command_threads}, 预计内存={memory_gb}GB")
            
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

            # 空间检查时把本组也计入需求（等价于"(在途+1) × 组大小"的语义），
            # 但本组此刻还不在 _active_groups 里，所以通过 pending 参数传入。
            await self._ensure_disk_space(temp_dir, pending_group_bytes=_group_bytes)
            # 注意：记账（append）推迟到压缩文件名确定之后 —— 守卫需要记录产出文件
            # 路径以扣除"已落地产出"。两者之间只有同步代码，无 await，语义不变。
            
            # 如果不直接压缩到磁带，需要final目录用于移动队列
            if compress_directly_to_tape:
                # 直接压缩到磁带，不需要final_dir（不移动文件）
                final_dir = None
                logger.debug(f"压缩到临时目录: {backup_dir}，直接压缩到磁带模式（不移动文件）")
            else:
                # 先压缩到temp目录，再移动到final目录（原有流程）
                # final目录：压缩完成后移动到这里，等待移动到磁带机
                final_dir = compress_dir / "final" / backup_set.set_id
                final_dir.mkdir(parents=True, exist_ok=True)
                logger.debug(f"压缩到临时目录: {backup_dir}，完成后将移动到: {final_dir}")
            
            # 预设压缩文件路径（立即确定，不等待压缩完成）
            if compression_enabled:
                if compression_method == 'pgzip':
                    archive_suffix = ".tar.gz"
                elif compression_method == 'tar':
                    archive_suffix = ".tar"
                elif compression_method == 'zstd':
                    archive_suffix = ".tar.zst"
                else:
                    archive_suffix = ".7z"
            else:
                archive_suffix = ".tar"
            
            # 生成唯一的压缩文件名，确保不重复
            backup_set_id = backup_set.set_id
            base_filename = f"backup_{backup_set_id}_{timestamp}"
            
            # 获取或初始化该备份集的序号信息
            if backup_set_id not in self._last_archive_info:
                self._last_archive_info[backup_set_id] = {'last_filename': None, 'sequence': 0}
            
            archive_info = self._last_archive_info[backup_set_id]
            sequence = archive_info['sequence']
            
            # 生成文件名，如果序号 > 0，添加序号后缀
            if sequence > 0:
                filename = f"{base_filename}_{sequence:04d}{archive_suffix}"
            else:
                filename = f"{base_filename}{archive_suffix}"
            
            archive_path = backup_dir / filename
            temp_archive_path = archive_path.absolute()
            
            # 检查文件是否已存在，如果存在则增加序号
            max_attempts = 1000  # 最多尝试1000次
            attempt = 0
            while temp_archive_path.exists() and attempt < max_attempts:
                sequence += 1
                archive_info['sequence'] = sequence
                if sequence > 0:
                    filename = f"{base_filename}_{sequence:04d}{archive_suffix}"
                else:
                    filename = f"{base_filename}{archive_suffix}"
                archive_path = backup_dir / filename
                temp_archive_path = archive_path.absolute()
                attempt += 1
            
            if attempt >= max_attempts:
                logger.error(f"[压缩] 无法生成唯一的压缩文件名，已尝试 {max_attempts} 次，使用带微秒时间戳的文件名")
                # 使用更精确的时间戳（包含微秒）
                from datetime import datetime
                precise_timestamp = datetime.now().strftime('%Y%m%d_%H%M%S_%f')
                filename = f"backup_{backup_set_id}_{precise_timestamp}{archive_suffix}"
                archive_path = backup_dir / filename
                temp_archive_path = archive_path.absolute()
            
            # 记录本次使用的文件名和序号
            archive_info['last_filename'] = filename
            archive_info['sequence'] = sequence

            # ★ 通过守卫 = 真正开始压缩 = 从现在起会落地产出，此刻才计入
            # （此时产出文件路径已确定，记录路径供守卫按"剩余待产出"计算需求；
            #  同时记录组类型，供卡片显示 L/S 数量）
            self._active_groups.append((int(_group_bytes), str(temp_archive_path)))
            try:
                _grp_type = ('small' if len(file_group) >= self._group_large_threshold
                             else 'large')
            except Exception:
                _grp_type = 'large'
            self._active_group_types.append(_grp_type)
            _active_counted = True
            
            logger.debug(f"[压缩] 生成压缩文件名: {filename} (序号={sequence}, 上次文件名={archive_info.get('last_filename', '无')})")
            
            # 进度跟踪变量：如果有共享的进度字典，使用它；否则创建新的
            if shared_compress_progress is not None:
                compress_progress = shared_compress_progress
                # 确保必要的字段存在（但不重置current_file_index和total_files_in_group，它们应该已经被初始化）
                compress_progress['bytes_written'] = compress_progress.get('bytes_written', 0)
                compress_progress['running'] = True
                compress_progress['completed'] = False
                # 如果共享字典还没有total_files_in_group，初始化它
                if 'total_files_in_group' not in compress_progress:
                    compress_progress['total_files_in_group'] = len(file_group)
                if 'current_file_index' not in compress_progress:
                    compress_progress['current_file_index'] = 0
            else:
                compress_progress = {
                    'bytes_written': 0,
                    'running': True,
                    'completed': False,
                    'current_file_index': 0,
                    'total_files_in_group': len(file_group)
                }
            total_original_size = sum(f['size'] for f in file_group)
            # 把本次生成的归档文件名写进共享进度字典——卡片 hover 需要显示
            # "当前打包的是哪个归档"（如 backup_2026-10_000113_20261017_083125.tar.zst）。
            # 归档名在这里就已确定（组级），无需解析 tar 输出。
            try:
                compress_progress['archive_name'] = temp_archive_path.name
            except Exception:
                pass
            # 用于存储成功和失败的文件信息（在线程间共享）
            compress_result = {'successful_files': [], 'failed_files': [], 'successful_original_size': 0, 'archive_path': str(temp_archive_path)}
            
            # 将压缩操作放到线程池中执行，避免阻塞事件循环
            def _do_7z_compress():
                """在线程中执行7z压缩操作，带进度跟踪（同步顺序执行）"""
                try:
                    # 在线程中创建目录（避免阻塞事件循环）
                    backup_dir.mkdir(parents=True, exist_ok=True)
                    
                    if compression_enabled:
                        
                        if compression_method == '7zip_command':
                            # 使用7-Zip命令行工具进行压缩
                            logger.debug(f"使用7-Zip命令行工具压缩 (路径: {sevenzip_path}, 线程数: {compression_command_threads})")
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
                            # 使用预设路径
                            compress_result['archive_path'] = str(temp_archive_path)
                        elif compression_method == 'pgzip':
                            logger.debug(f"使用PGZip压缩 (线程数: {pgzip_threads}, 块大小: {pgzip_block_size}, 等级: {compression_level})")
                            compress_result_inner = _compress_with_pgzip(
                                archive_path, file_group, backup_task,
                                compression_level, pgzip_threads, pgzip_block_size,
                                compress_progress, total_files, base_processed_files
                            )
                            compress_result['successful_files'] = compress_result_inner['successful_files']
                            compress_result['failed_files'] = compress_result_inner['failed_files']
                            compress_result['successful_original_size'] = compress_result_inner['successful_original_size']
                            # 使用预设路径
                            compress_result['archive_path'] = str(temp_archive_path)
                        elif compression_method == 'tar':
                            logger.debug(f"使用tar打包 (不压缩)")
                            compress_result_inner = _compress_with_tar(
                                archive_path, file_group, backup_task,
                                compression_level,
                                compress_progress, total_files, base_processed_files
                            )
                            compress_result['successful_files'] = compress_result_inner['successful_files']
                            compress_result['failed_files'] = compress_result_inner['failed_files']
                            compress_result['successful_original_size'] = compress_result_inner['successful_original_size']
                            # 使用预设路径
                            compress_result['archive_path'] = str(temp_archive_path)
                        elif compression_method == 'zstd':
                            logger.info(f"使用Zstandard CLI压缩 (线程数: {zstd_threads}, 等级: {compression_level})")
                            compress_result_inner = _compress_with_zstd_cli(
                                archive_path, file_group, backup_task,
                                compression_level, zstd_threads,
                                compress_progress, total_files, base_processed_files
                            )
                            compress_result['successful_files'] = compress_result_inner['successful_files']
                            compress_result['failed_files'] = compress_result_inner['failed_files']
                            compress_result['successful_original_size'] = compress_result_inner['successful_original_size']
                            # 使用预设路径
                            compress_result['archive_path'] = str(temp_archive_path)
                        else:
                            # 使用py7zr进行7z压缩，启用多进程（mp=True启用多进程压缩）
                            # 注意：py7zr 使用 mp 参数启用多进程，而不是 threads
                            logger.debug(f"使用py7zr压缩 (线程数: {compression_threads}, mp={compression_threads > 1})")
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
                                # 使用预设路径
                                compress_result['archive_path'] = str(temp_archive_path)
                                
                                logger.info(f"py7zr压缩完成: {len(successful_files)} 个文件成功, {len(failed_files)} 个文件失败")
                            
                    else:
                        # 未启用压缩时，仍旧使用tar存储（TODO：实现真正的无压缩 tar 打包）
                        logger.warning("当前未启用压缩，暂未实现tar打包，跳过压缩操作")
                        compress_progress['completed'] = True
                        compress_progress['running'] = False
                        compress_progress['bytes_written'] = 0
                        compress_result['successful_files'] = []
                        compress_result['failed_files'] = []
                        compress_result['successful_original_size'] = 0
                        compress_result['archive_path'] = str(temp_archive_path)
                        
                except Exception as e:
                    logger.error(f"压缩操作失败: {str(e)}")
                    import traceback
                    logger.error(traceback.format_exc())
                    compress_progress['completed'] = True
                    compress_progress['running'] = False
            
            # 异步启动压缩操作：zstd 走独立工作进程（spawn，彻底绕开 GIL），
            # 其他压缩方法保留线程池（老路径不可 pickle，行为不变）
            loop = asyncio.get_event_loop()
            logger.warning(f"[压缩] 异步启动压缩任务，立即返回，不等待完成")
            if compression_method == 'zstd':
                payload = {
                    'archive_path': temp_archive_path,
                    'file_group': file_group,
                    'level': compression_level,
                    'threads': zstd_threads,
                    'total_files': total_files,
                    'base_processed_files': base_processed_files,
                    'compress_progress': compress_progress,
                    # 源根目录列表：worker 内用于计算归档内相对路径（丢失会导致成员名退化为纯文件名）
                    'source_paths': getattr(backup_task, 'source_paths', None) or [],
                }
                payload['progress_queue'] = None
                # 组标识：进度事件按此路由到本组字典（并发组间不串写）
                payload['progress_key'] = str(temp_archive_path)
                pool = CompressionProcessPool.get_instance()
                pool.set_task_context(backup_task)
                pool.register_group_progress(payload['progress_key'], compress_progress)
                compression_future = await pool.submit(payload)
            else:
                compression_future = loop.run_in_executor(None, _do_7z_compress)
            
            # 顺序执行：等待压缩完成、标注完成、移动到final
            # 等待压缩完成（不设置超时，让压缩自然完成）
            try:
                await compression_future
            finally:
                # 组结束后注销进度路由（worker 的收尾事件已在返回前送达）
                try:
                    CompressionProcessPool.get_instance().unregister_group_progress(payload.get('progress_key'))
                except Exception:
                    pass
            logger.warning(f"[压缩] 压缩任务已完成")

            # 压缩已结束 → 本组不再产出数据，立即释放"在途产出"记账。
            # 后续的标注/移动到 final 是同盘移动，不新增占用，不该继续占用守卫额度。
            if _active_counted and not _active_released:
                self._release_active_group(_group_bytes, str(temp_archive_path))
                _active_released = True

            # 等待文件完全关闭（Windows上文件句柄释放可能需要时间）
            logger.debug("[压缩] 等待文件句柄完全释放...")
            await asyncio.sleep(1.0)
            
            # 验证压缩文件是否存在
            if not temp_archive_path.exists():
                logger.error(f"[压缩] 压缩文件不存在: {temp_archive_path}")
                return None
            
            # 验证压缩文件大小是否合理
            compressed_size = temp_archive_path.stat().st_size
            total_original_size = sum(f['size'] for f in file_group)
            
            # 计算压缩比
            if total_original_size > 0:
                compression_ratio = compressed_size / total_original_size
                logger.debug(f"[压缩] 压缩文件存在，原始大小: {format_bytes(total_original_size)}, "
                          f"压缩后大小: {format_bytes(compressed_size)}, "
                          f"压缩比: {compression_ratio:.2%}, 路径: {temp_archive_path}")
                
                # 检查压缩比是否异常（压缩后文件过小可能是压缩失败）
                # 如果原始文件 > 100MB，但压缩后 < 原始大小的 0.1%，可能是异常
                if total_original_size > 100 * 1024 * 1024 and compression_ratio < 0.001:
                    logger.warning(f"[压缩] ⚠️ 压缩比异常：原始大小 {format_bytes(total_original_size)} "
                                 f"压缩后只有 {format_bytes(compressed_size)} (压缩比 {compression_ratio:.2%})，"
                                 f"可能压缩失败或文件为空")
                    # 检查成功文件数
                    successful_count = len(compress_result.get('successful_files', []))
                    failed_count = len(compress_result.get('failed_files', []))
                    logger.warning(f"[压缩] 成功文件数: {successful_count}, 失败文件数: {failed_count}")
                    
                    # 如果所有文件都失败，返回None
                    if successful_count == 0:
                        logger.error(f"[压缩] ❌ 所有文件压缩失败，返回None")
                        return None
            else:
                logger.debug(f"[压缩] 压缩文件存在，大小: {format_bytes(compressed_size)}, 路径: {temp_archive_path}")
            
            # 压缩完成后，标注完成
            from backup.backup_db import BackupDB
            backup_db = BackupDB()
            await backup_db.update_task_stage_with_description(
                backup_task,
                "compress",
                f"[压缩完成] {total_files}/{total_files} 个文件 (100.0%)"
            )
            logger.info(f"[压缩] ✅ 已标注压缩完成: {total_files} 个文件")
            
            # 根据配置决定是否需要移动文件
            final_archive_path_for_db = temp_archive_path  # 默认使用temp路径
            if compress_directly_to_tape:
                # 直接压缩到磁带模式：不移动文件
                logger.debug(f"[压缩] 直接压缩到磁带模式：文件保留在临时目录: {temp_archive_path}")
                # 直接压缩到磁带模式，使用temp路径
                final_archive_path_for_db = temp_archive_path
            else:
                # 非直接压缩模式：将文件从temp目录移动到final目录
                if final_dir is None:
                    logger.warning("[压缩] final_dir未定义，使用temp目录中的文件")
                    return None
                
                # 确保目标目录存在
                final_dir.mkdir(parents=True, exist_ok=True)
                final_archive_path = final_dir / temp_archive_path.name
                
                # 移动文件
                if not temp_archive_path.exists():
                    logger.error(f"[压缩] 源文件不存在: {temp_archive_path}")
                    return None
                
                logger.debug(f"[压缩] 开始移动文件到final目录: {final_archive_path}")
                
                # 等待文件稳定
                max_wait_seconds = 30
                check_interval = 0.5
                wait_count = 0
                max_checks = int(max_wait_seconds / check_interval)
                last_size = None
                stable_count = 0
                stable_required = 3
                
                while wait_count < max_checks and stable_count < stable_required:
                    await asyncio.sleep(check_interval)
                    wait_count += 1
                    if temp_archive_path.exists():
                        current_size = temp_archive_path.stat().st_size
                        if last_size is not None and current_size == last_size:
                            stable_count += 1
                        else:
                            stable_count = 0
                        last_size = current_size
                    else:
                        break
                
                if wait_count >= max_checks:
                    logger.warning(f"[压缩] 等待文件稳定超时，但继续尝试移动")
                
                # 移动文件
                loop_move = asyncio.get_event_loop()
                await loop_move.run_in_executor(None, shutil.move, str(temp_archive_path), str(final_archive_path))
                
                # 验证移动成功
                if not final_archive_path.exists():
                    logger.error(f"[压缩] 移动后目标文件不存在: {final_archive_path}")
                    return None
                
                logger.info(f"[压缩] ✅ 文件已成功移动到final目录: {final_archive_path}")
                
                # 更新compress_result中的路径
                compress_result['archive_path'] = str(final_archive_path)
                # 更新最终路径（用于数据库更新）
                final_archive_path_for_db = final_archive_path
            
            # 注意：is_copy_success 已在预读程序入队时设置为 TRUE，此处不再更新
            
            # 计算原始大小
            total_original_size = sum(f['size'] for f in file_group)
            
            # 使用最终路径（已移动到final或保留在temp）
            final_path = final_archive_path_for_db
            
            # 返回压缩信息（压缩已完成，文件已移动到final）
            compressed_info = {
                'path': str(final_path),  # 最终路径（已移动到final或保留在temp）
                'temp_path': temp_archive_path,  # temp目录中的路径（Path对象）
                'final_path': final_dir / temp_archive_path.name if final_dir else temp_archive_path,  # final目录中的路径（Path对象）
                'compressed_size': final_path.stat().st_size if final_path.exists() else 0,  # 压缩文件大小
                'original_size': total_original_size,
                'successful_files': len(compress_result.get('successful_files', [])),  # 实际成功文件数
                'failed_files': len(compress_result.get('failed_files', [])),  # 实际失败文件数
                'checksum': None,
                'compression_enabled': compression_enabled,
                'compression_method': compression_method,
                'compression_level': compression_level if compression_enabled else None,
                'compression_threads': compression_threads if compression_enabled else None,
                'compress_progress': compress_progress,  # 进度跟踪字典
                'compress_result': compress_result  # 压缩结果字典
            }
            
            return compressed_info

        except Exception as e:
            # 兜底释放"在途产出"记账（若上面正常路径尚未释放）
            if _active_counted and not _active_released:
                try:
                    _release_path = str(temp_archive_path)
                except NameError:
                    _release_path = ""
                self._release_active_group(_group_bytes, _release_path)
                _active_released = True
            logger.error(f"压缩文件组失败: {str(e)}")
            import traceback
            traceback.print_exc()
            return None

    async def _ensure_disk_space(self, target_dir: Path, pending_group_bytes: int = 0):
        """确保磁盘剩余空间满足"**在途真实产出** + **本组待产出**"

        `_active_group_bytes` 只记录**已通过守卫、真正在压缩**的组（它们在落地产出）。
        本组此刻还在检查、尚未计入，因此通过 `pending_group_bytes` 显式传入，
        等价于"(在途 + 1) × 组大小"的语义 —— 保证本组落地有空间，同时不让
        "排队等待中的组"虚抬判定线。
        """
        try:
            max_file_size = int(getattr(self.settings, 'MAX_FILE_SIZE', 0))
        except Exception:
            max_file_size = 0

        if max_file_size <= 0:
            return

        # 从配置获取参数
        # 注意：这里现取 settings 而非用构造时捕获的 self.settings——
        # UI 保存压缩配置会调用 reload_settings() 生成新对象，
        # 用捕获的旧对象会导致"改了不生效、必须重启"（与 _adjust_parallel_batches 保持一致）
        try:
            from config.settings import get_settings as _get_settings
            _live_settings = _get_settings()
        except Exception:
            _live_settings = self.settings

        # 空间守卫总开关（DISK_GUARD_ENABLED）：关闭后不再按守卫线暂停发放新组。
        # 注意"与磁盘限流至少保留一个"由 API 层校验，此处只忠实执行开关。
        if not bool(getattr(_live_settings, 'DISK_GUARD_ENABLED', True)):
            logger.debug("空间守卫已关闭（DISK_GUARD_ENABLED=false），跳过磁盘空间检查")
            return

        check_interval = int(getattr(_live_settings, 'DISK_CHECK_INTERVAL', 30) or 30)
        if check_interval < 1:
            check_interval = 30
        max_wait_minutes = getattr(_live_settings, 'DISK_CHECK_MAX_WAIT_MINUTES', 60)

        # 根据时间和间隔计算最大重试次数
        max_retries = (max_wait_minutes * 60) // check_interval

        # 守卫需求 = max( 在途组**剩余待产出上界** , MAX_FILE_SIZE ) × 安全系数
        #
        #   在途剩余 = Σ max(0, 源大小 − 已落地产出)
        #     · 源大小 = 产出上界（压缩后只会更小），替代"组数 × MAX_FILE_SIZE"
        #       可避免小文件区被严重高估（实测组只有 0.2~4GB 却按 24GB/组计，
        #       16 组时守卫线虚高到 384G，白闲置数百 GB）。
        #     · 减去已落地产出：这些字节已经占用 SSD，free 里已扣过一次；需求侧
        #       再按全量源大小算会让守卫线虚高（在途压得越深，虚高越多）。
        #     · 组刚启动（文件未创建/为空）时扣除量为 0 → 与纯源大小口径逐字节
        #       一致，最坏情况（全部组同时刚启动）的保护不减弱。
        #     · 已产出取 min(stat大小, 源大小)：压缩比>1（已压缩数据）时截断，
        #       保证扣除量不超过该组的上界。
        #   保底一项 = MAX_FILE_SIZE：避免"在途全是小文件组 → 守卫线过低 →
        #            下一个大组直接撑爆"。无在途组（如首个组）时该项即全部需求。
        #
        # 安全系数现取 settings：系数改动后守卫线立即随之伸缩。
        try:
            _factor = float(getattr(_live_settings, 'DISK_GUARD_SAFETY_FACTOR', 1.2) or 1.2)
        except Exception:
            _factor = 1.2
        if _factor < 1.0:   # 小于 1 会让在途产出越界
            _factor = 1.0

        try:
            active_src_total = sum(int(src) for src, _p in self._active_groups)
        except Exception:
            active_src_total = 0

        # 扣除已落地产出（DISK_GUARD_COUNT_PRODUCED=false 可一键回退纯源大小口径）
        try:
            _count_produced = bool(getattr(_live_settings, 'DISK_GUARD_COUNT_PRODUCED', True))
        except Exception:
            _count_produced = True
        produced_total = 0
        if _count_produced:
            for _src, _produced_path in list(self._active_groups):
                try:
                    _size = int(os.stat(_produced_path).st_size)
                except (OSError, TypeError, ValueError):
                    _size = 0      # 文件尚未创建/已被移走 → 按 0 计（保守）
                produced_total += max(0, min(_size, int(_src)))
        remaining_bytes = max(0, active_src_total - produced_total)
        if not _count_produced:
            remaining_bytes = active_src_total

        active_groups = len(self._active_groups)
        # 把本组（即将通过守卫、随后落地产出）按**全量源大小**计入需求：本组尚无产出
        if pending_group_bytes and pending_group_bytes > 0:
            remaining_bytes += int(pending_group_bytes)
            active_groups += 1

        if remaining_bytes > 0:
            # 有在途组：按剩余待产出算（含正在检查的这一组，它已计入"本组源大小"）
            required_free = int(max(remaining_bytes, max_file_size) * _factor)
            logger.debug(
                f"守卫线（按剩余待产出）: 在途 {active_groups} 组，源 {format_bytes(active_src_total)}，"
                f"已产出 {format_bytes(produced_total)}，剩余上界 {format_bytes(remaining_bytes)}（含本组），"
                f"保底 {format_bytes(max_file_size)}，×{_factor} → 需 {format_bytes(required_free)}"
            )
        else:
            # 无在途组（首个组/记账为空）：回退保守口径
            _ag = None
            provider = getattr(self, 'active_groups_provider', None)
            if callable(provider):
                try:
                    _ag = int(provider()) or None
                except Exception as provider_err:
                    logger.debug(f"获取在途组数失败，回退配置上限: {provider_err}")
                    _ag = None
            required_free = _compute_required_free_bytes(_live_settings, _ag) or (max_file_size * 3)

        # 循环由"全局无放行超时"控制退出（不再用本组的重试次数——
        # 否则本组重试次数用尽会静默跳出循环、等于无条件放行，与守卫语义冲突）
        while True:
            try:
                usage = shutil.disk_usage(str(target_dir))
                free_bytes = usage.free
            except Exception as disk_error:
                logger.warning(f"无法获取磁盘剩余空间（{target_dir}），跳过空间检查: {disk_error}")
                return

            if free_bytes >= required_free:
                # ★ 放行：更新**全局**活跃时间戳（所有等待组共享）。
                # 它证明"磁盘确实在放行"，是系统健康的最强信号。
                self._last_release_time = time.time()
                if self._guard_active:
                    # 结束一轮拦停，打一条 INFO 便于事后对齐
                    logger.info(
                        f"[空间守卫] 放行：{format_bytes(free_bytes)} >= {format_bytes(required_free)}，"
                        f"本轮拦停结束（在途 {active_groups} 组）"
                    )
                    self._guard_active = False
                    self._last_guard_log_time = 0.0
                logger.debug(
                    f"磁盘剩余空间充足：{format_bytes(free_bytes)} >= {format_bytes(required_free)}"
                )
                return

            max_total_wait = max_retries * check_interval

            # ===== 全局卡死检测 =====
            # 判据：距离"上一次有组通过守卫"已经过去多久。
            #  · 只要不断有组被放行（即使本组一直排队），计时就会被刷新 → 不误杀
            #  · 全场连续无人能过守卫超过阈值 → 视为卡死，本组报错退出
            now_ts = time.time()
            if self._last_release_time is None:
                # 首次进入守卫（还没有任何组放行过）：以本次等待为起点，避免误判
                self._last_release_time = now_ts
            idle_seconds = max(0.0, now_ts - self._last_release_time)

            # 日志节流：首次拦停立即报；其后每 5 分钟一条汇总；避免多组重试刷屏
            _GUARD_LOG_INTERVAL = 300.0
            _should_log = False
            if not self._guard_active:
                self._guard_active = True
                _should_log = True
            elif (now_ts - self._last_guard_log_time) >= _GUARD_LOG_INTERVAL:
                _should_log = True

            if _should_log:
                self._last_guard_log_time = now_ts
                logger.info(
                    f"磁盘剩余空间不足：{format_bytes(free_bytes)} < {format_bytes(required_free)}"
                    f"（在途 {active_groups if active_groups else '并发上限'} 组"
                    + (f"，源 {format_bytes(active_src_total)}"
                       + (f"，已产出 {format_bytes(produced_total)}" if _count_produced else "，已产出未统计")
                       + f"，剩余上界 {format_bytes(remaining_bytes)}（含本组）" if active_src_total > 0 else "")
                    + f"），暂停压缩，{check_interval} 秒后重试"
                    f"（全场已 {idle_seconds:.0f}/{max_total_wait} 秒无任何组放行）"
                )

            if idle_seconds >= max_total_wait:
                # 全场长时间没有任何组能通过守卫 → 判定为卡死
                error_msg = (
                    f"全场已 {idle_seconds:.0f} 秒没有任何压缩组能通过空间守卫（阈值 {max_total_wait} 秒），"
                    f"疑似磁盘/磁带卡死。需要空间: {format_bytes(required_free)}, "
                    f"当前可用: {format_bytes(free_bytes)}"
                )
                logger.error(error_msg)
                raise RuntimeError(error_msg)

            await asyncio.sleep(check_interval)
