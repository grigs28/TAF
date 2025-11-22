#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
Redis批量写入优化模块
Redis Batch Write Optimizer Module

专门优化10000+条记录的批量写入性能
"""

import logging
import json
import time
from typing import List, Dict, Tuple, Optional
from datetime import datetime, timezone
from pathlib import Path

from config.redis_db import get_redis_client
from backup.redis_backup_db import (
    KEY_PREFIX_BACKUP_FILE, KEY_INDEX_BACKUP_FILES,
    KEY_INDEX_BACKUP_FILE_BY_SET_ID, KEY_COUNTER_BACKUP_FILE, _get_redis_key
)

logger = logging.getLogger(__name__)


async def optimized_batch_write_10000(
    backup_set_db_id: int,
    file_batch: List[Dict],
    file_path_cache: Dict[str, int],
    batch_id: int = 0
) -> Tuple[int, int, float]:
    """
    优化的10000条记录批量写入（Redis版本）
    
    优化策略：
    1. 预先准备所有数据，减少循环中的处理
    2. 使用单个大型 Pipeline（最多20000个命令）
    3. 批量获取所有ID（一次性）
    4. 批量构建所有命令（一次性）
    5. 一次性执行所有操作（最少网络往返）
    
    Args:
        backup_set_db_id: 备份集数据库ID
        file_batch: 文件信息列表（最多10000条）
        file_path_cache: 文件路径缓存 {file_path: file_id}
        batch_id: 批次ID（用于日志）
    
    Returns:
        (inserted_count, updated_count, elapsed_time)
    """
    if not file_batch:
        return 0, 0, 0.0
    
    start_time = time.time()
    redis = await get_redis_client()
    
    try:
        # ========== 阶段1：快速分类文件 ==========
        phase1_start = time.time()
        
        # 预先提取所有文件路径
        batch_file_paths = []
        file_info_map = {}
        for file_info in file_batch:
            file_path = file_info.get('path', '')
            if file_path:
                batch_file_paths.append(file_path)
                file_info_map[file_path] = file_info
        
        # 快速查找已存在的文件（只查缓存）
        existing_file_paths = {}
        new_file_paths = []
        
        for file_path in batch_file_paths:
            if file_path in file_path_cache:
                existing_file_paths[file_path] = file_path_cache[file_path]
            else:
                new_file_paths.append(file_path)
        
        phase1_time = time.time() - phase1_start
        
        # ========== 阶段2：批量检查已复制文件（只检查已存在的文件）==========
        phase2_start = time.time()
        skipped_paths = set()
        
        if existing_file_paths:
            # 批量检查 is_copy_success（只检查已存在的文件）
            check_pipe = redis.pipeline()
            check_items = []
            
            for file_path, file_id in existing_file_paths.items():
                file_key = _get_redis_key(KEY_PREFIX_BACKUP_FILE, file_id)
                check_pipe.hget(file_key, 'is_copy_success')
                check_items.append(file_path)
            
            copy_status_results = await check_pipe.execute()
            
            # 过滤已成功复制的文件
            for file_path, is_copy_success in zip(check_items, copy_status_results):
                if is_copy_success in ('1', 'True', 'true'):
                    skipped_paths.add(file_path)
                    # 从待处理列表中移除
                    existing_file_paths.pop(file_path, None)
        
        phase2_time = time.time() - phase2_start
        
        # ========== 阶段3：批量获取新文件ID ==========
        phase3_start = time.time()
        
        new_file_count = len(new_file_paths)
        new_file_ids = []
        
        if new_file_count > 0:
            # 一次性批量获取所有新文件ID
            id_pipe = redis.pipeline()
            for _ in range(new_file_count):
                id_pipe.incr(KEY_COUNTER_BACKUP_FILE)
            id_results = await id_pipe.execute()
            new_file_ids = [int(id_val) for id_val in id_results]
        
        phase3_time = time.time() - phase3_start
        
        # ========== 阶段4：预先准备所有数据 ==========
        phase4_start = time.time()
        
        current_time = datetime.now()
        current_time_tz = current_time.replace(tzinfo=timezone.utc)
        
        # 预先准备插入数据
        insert_items = []
        for file_path, file_id in zip(new_file_paths, new_file_ids):
            if file_path in skipped_paths:
                continue
            
            file_info = file_info_map[file_path]
            file_stat = file_info.get('file_stat')
            
            # 文件大小提取（优化：减少条件判断）
            file_size = (
                int(file_info['size']) 
                if 'size' in file_info and file_info.get('size') is not None 
                else (file_stat.st_size if file_stat and hasattr(file_stat, 'st_size') else 0)
            )
            
            file_name = file_info.get('name') or Path(file_path).name
            metadata = file_info.get('file_metadata') or {}
            metadata.update({'scanned_at': current_time.isoformat()})
            
            # 预先计算所有字段（避免在循环中重复计算）
            file_key = _get_redis_key(KEY_PREFIX_BACKUP_FILE, file_id)
            
            # 时间字段预处理
            created_time_str = (
                datetime.fromtimestamp(file_stat.st_ctime, tz=timezone.utc).isoformat()
                if file_stat and hasattr(file_stat, 'st_ctime')
                else ''
            )
            modified_time_str = (
                datetime.fromtimestamp(file_stat.st_mtime, tz=timezone.utc).isoformat()
                if file_stat and hasattr(file_stat, 'st_mtime')
                else ''
            )
            accessed_time_str = (
                datetime.fromtimestamp(file_stat.st_atime, tz=timezone.utc).isoformat()
                if file_stat and hasattr(file_stat, 'st_atime')
                else ''
            )
            permissions_str = (
                oct(file_stat.st_mode)[-3:]
                if file_stat and hasattr(file_stat, 'st_mode')
                else ''
            )
            
            insert_mapping = {
                'backup_set_id': str(backup_set_db_id),
                'file_path': file_path,
                'file_name': file_name,
                'file_type': 'file',
                'file_size': str(file_size),
                'compressed_size': '',
                'file_permissions': permissions_str,
                'created_time': created_time_str,
                'modified_time': modified_time_str,
                'accessed_time': accessed_time_str,
                'compressed': '0',
                'checksum': '',
                'backup_time': current_time_tz.isoformat(),
                'chunk_number': '',
                'tape_block_start': '',
                'file_metadata': json.dumps(metadata),
                'is_copy_success': '0',
                'copy_status_at': ''
            }
            
            insert_items.append((file_key, file_id, file_path, insert_mapping))
        
        # 预先准备更新数据
        update_items = []
        for file_path, file_id in existing_file_paths.items():
            if file_path in skipped_paths:
                continue
            
            file_info = file_info_map[file_path]
            file_stat = file_info.get('file_stat')
            
            # 文件大小提取（优化）
            file_size = (
                int(file_info['size'])
                if 'size' in file_info and file_info.get('size') is not None
                else (file_stat.st_size if file_stat and hasattr(file_stat, 'st_size') else 0)
            )
            
            file_name = file_info.get('name') or Path(file_path).name
            metadata = file_info.get('file_metadata') or {}
            metadata.update({'scanned_at': current_time.isoformat()})
            
            file_key = _get_redis_key(KEY_PREFIX_BACKUP_FILE, file_id)
            
            # 时间字段预处理
            modified_time_str = (
                datetime.fromtimestamp(file_stat.st_mtime, tz=timezone.utc).isoformat()
                if file_stat and hasattr(file_stat, 'st_mtime')
                else ''
            )
            permissions_str = (
                oct(file_stat.st_mode)[-3:]
                if file_stat and hasattr(file_stat, 'st_mode')
                else ''
            )
            
            update_mapping = {
                'file_name': file_name,
                'file_size': str(file_size),
                'file_permissions': permissions_str,
                'modified_time': modified_time_str,
                'file_metadata': json.dumps(metadata),
                'updated_at': current_time.isoformat()
            }
            
            update_items.append((file_key, update_mapping))
        
        phase4_time = time.time() - phase4_start
        
        # ========== 阶段5：一次性执行所有操作（最大Pipeline）==========
        phase5_start = time.time()
        
        # 构建单个大型 Pipeline（所有操作一次性执行）
        pipe = redis.pipeline()
        
        # 添加所有插入操作（Hash插入 + 2个索引）
        set_index_key = f"{KEY_INDEX_BACKUP_FILE_BY_SET_ID}:{backup_set_db_id}"
        
        for file_key, file_id, file_path, insert_mapping in insert_items:
            # 插入Hash
            pipe.hset(file_key, mapping=insert_mapping)
            # 添加到全局索引
            pipe.sadd(KEY_INDEX_BACKUP_FILES, str(file_id))
            # 添加到备份集索引
            pipe.sadd(set_index_key, str(file_id))
            # 更新缓存
            file_path_cache[file_path] = file_id
        
        # 添加所有更新操作
        for file_key, update_mapping in update_items:
            pipe.hset(file_key, mapping=update_mapping)
        
        # 一次性执行所有操作（最关键：只一次网络往返）
        await pipe.execute()
        
        phase5_time = time.time() - phase5_start
        total_time = time.time() - start_time
        
        inserted_count = len(insert_items)
        updated_count = len(update_items)
        total_ops = inserted_count * 3 + updated_count  # 每个插入：1个HSET + 2个SADD
        
        # 性能日志（只记录关键信息）
        if batch_id % 10 == 0 or total_time > 1.0:  # 每10批或耗时超过1秒时记录
            logger.info(
                f"[Redis优化写入] 批次#{batch_id}: "
                f"插入={inserted_count}, 更新={updated_count}, "
                f"总操作={total_ops}, 耗时={total_time*1000:.1f}ms, "
                f"速度={inserted_count/total_time:.0f} 个/秒"
            )
        else:
            logger.debug(
                f"[Redis优化写入] 批次#{batch_id}: "
                f"插入={inserted_count}, 更新={updated_count}, "
                f"阶段耗时: 准备={phase1_time*1000:.1f}ms, "
                f"检查={phase2_time*1000:.1f}ms, ID={phase3_time*1000:.1f}ms, "
                f"数据准备={phase4_time*1000:.1f}ms, 执行={phase5_time*1000:.1f}ms"
            )
        
        return inserted_count, updated_count, total_time
        
    except Exception as e:
        logger.error(f"[Redis优化写入] 批次#{batch_id} 失败: {str(e)}", exc_info=True)
        raise

