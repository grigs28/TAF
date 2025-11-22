#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
磁带数据库操作 - Redis 实现（原生实现，不使用SQLAlchemy）
Tape Database Operations - Redis Implementation (Native, No SQLAlchemy)
"""

import logging
import json
from datetime import datetime
from typing import Optional, Dict, List, Any
from config.redis_db import get_redis_client

logger = logging.getLogger(__name__)


# Redis键前缀
KEY_PREFIX_TAPE = "tape_cartridge"
KEY_INDEX_TAPES = "tapes:index"  # Set: 所有磁带tape_id
KEY_INDEX_TAPE_BY_LABEL = "tapes:by_label"  # Hash: label -> tape_id
KEY_INDEX_TAPE_BY_SERIAL = "tapes:by_serial"  # Hash: serial_number -> tape_id


def _get_redis_key(entity: str, identifier: Any) -> str:
    """生成Redis键"""
    return f"{entity}:{identifier}"


def _parse_datetime_value(value):
    """将多种时间格式转换为 datetime 对象"""
    if not value:
        return None
    if isinstance(value, datetime):
        return value
    if isinstance(value, (int, float)):
        try:
            return datetime.fromtimestamp(value)
        except Exception:
            return None
    if isinstance(value, str):
        text = value.strip()
        if not text:
            return None
        try:
            if text.endswith("Z"):
                text = text.replace("Z", "+00:00")
            return datetime.fromisoformat(text)
        except ValueError:
            try:
                return datetime.fromtimestamp(float(text))
            except (ValueError, TypeError):
                return None
    return None


def _datetime_to_str(dt: Optional[datetime]) -> Optional[str]:
    """将datetime转换为ISO格式字符串"""
    if dt is None:
        return None
    if isinstance(dt, str):
        return dt
    return dt.isoformat()


async def create_tape_redis(
    tape_id: str,
    label: str,
    status: str = "AVAILABLE",
    media_type: Optional[str] = None,
    generation: Optional[int] = None,
    serial_number: Optional[str] = None,
    location: Optional[str] = None,
    capacity_bytes: int = 0,
    used_bytes: int = 0,
    retention_months: int = 6,
    notes: Optional[str] = None,
    manufactured_date: Optional[datetime] = None,
    expiry_date: Optional[datetime] = None,
    auto_erase: bool = True,
    health_score: int = 100
) -> Dict[str, Any]:
    """创建磁带记录（Redis版本）"""
    try:
        redis = await get_redis_client()
        
        tape_key = _get_redis_key(KEY_PREFIX_TAPE, tape_id)
        
        # 检查磁带是否已存在
        exists = await redis.exists(tape_key)
        tape_exists = exists > 0
        
        # 准备磁带数据
        tape_data = {
            "tape_id": tape_id,
            "label": label,
            "status": status.upper() if status else "AVAILABLE",
            "media_type": media_type or "LTO",
            "generation": str(generation) if generation else "8",
            "serial_number": serial_number or "",
            "location": location or "",
            "capacity_bytes": str(capacity_bytes),
            "used_bytes": str(used_bytes),
            "retention_months": str(retention_months),
            "notes": notes or "",
            "manufactured_date": _datetime_to_str(manufactured_date),
            "expiry_date": _datetime_to_str(expiry_date),
            "auto_erase": "1" if auto_erase else "0",
            "health_score": str(health_score),
            "write_count": "0",
            "read_count": "0",
            "load_count": "0",
            "backup_set_count": "0",
            "created_at": datetime.now().isoformat(),
            "updated_at": datetime.now().isoformat()
        }
        
        # 使用事务保存
        pipe = redis.pipeline()
        pipe.hset(tape_key, mapping=tape_data)
        
        # 添加到索引
        pipe.sadd(KEY_INDEX_TAPES, tape_id)
        if label:
            pipe.hset(KEY_INDEX_TAPE_BY_LABEL, label, tape_id)
        if serial_number:
            pipe.hset(KEY_INDEX_TAPE_BY_SERIAL, serial_number, tape_id)
        
        await pipe.execute()
        
        logger.info(f"[Redis模式] {'更新' if tape_exists else '创建'}磁带记录: {tape_id}")
        
        return {
            "success": True,
            "tape_exists": tape_exists,
            "tape_id": tape_id
        }
        
    except Exception as e:
        logger.error(f"[Redis模式] 创建磁带记录失败: {str(e)}", exc_info=True)
        raise


async def get_tape_redis(tape_id: str) -> Optional[Dict[str, Any]]:
    """获取磁带详情（Redis版本）"""
    try:
        redis = await get_redis_client()
        
        tape_key = _get_redis_key(KEY_PREFIX_TAPE, tape_id)
        tape_data = await redis.hgetall(tape_key)
        
        if not tape_data:
            return None
        
        # 转换为字典（由于 Redis 连接设置了 decode_responses=True，键值已经是字符串）
        tape_dict = {k if isinstance(k, str) else k.decode('utf-8'): 
                    v if isinstance(v, str) else (v.decode('utf-8') if isinstance(v, bytes) else str(v))
                    for k, v in tape_data.items()}
        
        # 解析字段
        return {
            "tape_id": tape_dict.get('tape_id', tape_id),
            "label": tape_dict.get('label', ''),
            "status": tape_dict.get('status', 'AVAILABLE').lower(),
            "media_type": tape_dict.get('media_type', 'LTO'),
            "generation": int(tape_dict.get('generation', 8)) if tape_dict.get('generation') else 8,
            "serial_number": tape_dict.get('serial_number', ''),
            "location": tape_dict.get('location', ''),
            "capacity_bytes": int(tape_dict.get('capacity_bytes', 0) or 0),
            "used_bytes": int(tape_dict.get('used_bytes', 0) or 0),
            "retention_months": int(tape_dict.get('retention_months', 6) or 6),
            "notes": tape_dict.get('notes', ''),
            "manufactured_date": _parse_datetime_value(tape_dict.get('manufactured_date')),
            "expiry_date": _parse_datetime_value(tape_dict.get('expiry_date')),
            "auto_erase": tape_dict.get('auto_erase', '1') == '1',
            "health_score": int(tape_dict.get('health_score', 100) or 100),
            "write_count": int(tape_dict.get('write_count', 0) or 0),
            "read_count": int(tape_dict.get('read_count', 0) or 0),
            "load_count": int(tape_dict.get('load_count', 0) or 0),
            "backup_set_count": int(tape_dict.get('backup_set_count', 0) or 0),
            "created_at": _parse_datetime_value(tape_dict.get('created_at')),
            "updated_at": _parse_datetime_value(tape_dict.get('updated_at'))
        }
        
    except Exception as e:
        logger.error(f"[Redis模式] 获取磁带详情失败: {str(e)}", exc_info=True)
        return None


async def check_tape_exists_redis(tape_id: str) -> bool:
    """检查磁带是否存在（Redis版本）"""
    try:
        redis = await get_redis_client()
        tape_key = _get_redis_key(KEY_PREFIX_TAPE, tape_id)
        exists = await redis.exists(tape_key)
        return exists > 0
    except Exception as e:
        logger.error(f"[Redis模式] 检查磁带存在性失败: {str(e)}", exc_info=True)
        return False


async def check_tape_label_exists_redis(label: str) -> bool:
    """检查磁带标签是否存在（Redis版本）"""
    try:
        redis = await get_redis_client()
        tape_id = await redis.hget(KEY_INDEX_TAPE_BY_LABEL, label)
        return tape_id is not None
    except Exception as e:
        logger.error(f"[Redis模式] 检查磁带标签存在性失败: {str(e)}", exc_info=True)
        return False


async def get_tape_by_label_redis(label: str) -> Optional[Dict[str, Any]]:
    """根据标签获取磁带（Redis版本）"""
    try:
        redis = await get_redis_client()
        tape_id_bytes = await redis.hget(KEY_INDEX_TAPE_BY_LABEL, label)
        if not tape_id_bytes:
            return None
        
        tape_id = tape_id_bytes if isinstance(tape_id_bytes, str) else (tape_id_bytes.decode('utf-8') if isinstance(tape_id_bytes, bytes) else str(tape_id_bytes))
        return await get_tape_redis(tape_id)
    except Exception as e:
        logger.error(f"[Redis模式] 根据标签获取磁带失败: {str(e)}", exc_info=True)
        return None


async def list_tapes_redis() -> List[Dict[str, Any]]:
    """获取所有磁带列表（Redis版本）"""
    try:
        redis = await get_redis_client()
        
        # 获取所有磁带ID
        tape_ids_bytes = await redis.smembers(KEY_INDEX_TAPES)
        tape_ids = [tid if isinstance(tid, str) else (tid.decode('utf-8') if isinstance(tid, bytes) else str(tid)) 
                   for tid in tape_ids_bytes]
        
        tapes = []
        for tape_id in tape_ids:
            tape = await get_tape_redis(tape_id)
            if tape:
                tapes.append(tape)
        
        return tapes
        
    except Exception as e:
        logger.error(f"[Redis模式] 获取磁带列表失败: {str(e)}", exc_info=True)
        return []


async def update_tape_redis(
    tape_id: str,
    label: Optional[str] = None,
    status: Optional[str] = None,
    media_type: Optional[str] = None,
    generation: Optional[int] = None,
    serial_number: Optional[str] = None,
    location: Optional[str] = None,
    capacity_bytes: Optional[int] = None,
    used_bytes: Optional[int] = None,
    retention_months: Optional[int] = None,
    notes: Optional[str] = None,
    manufactured_date: Optional[datetime] = None,
    expiry_date: Optional[datetime] = None,
    auto_erase: Optional[bool] = None,
    health_score: Optional[int] = None,
    **kwargs
) -> bool:
    """更新磁带记录（Redis版本）"""
    try:
        redis = await get_redis_client()
        
        tape_key = _get_redis_key(KEY_PREFIX_TAPE, tape_id)
        
        # 检查磁带是否存在
        exists = await redis.exists(tape_key)
        if not exists:
            return False
        
        # 获取现有数据（由于 Redis 连接设置了 decode_responses=True，键值已经是字符串）
        existing_data = await redis.hgetall(tape_key)
        existing_dict = {k if isinstance(k, str) else k.decode('utf-8'): 
                        v if isinstance(v, str) else (v.decode('utf-8') if isinstance(v, bytes) else str(v))
                        for k, v in existing_data.items()}
        
        # 准备更新数据
        update_data = {}
        
        if label is not None:
            update_data["label"] = label
            # 更新标签索引
            old_label = existing_dict.get('label')
            if old_label and old_label != label:
                pipe = redis.pipeline()
                pipe.hdel(KEY_INDEX_TAPE_BY_LABEL, old_label)
                pipe.hset(KEY_INDEX_TAPE_BY_LABEL, label, tape_id)
                await pipe.execute()
        
        if status is not None:
            update_data["status"] = status.upper()
        if media_type is not None:
            update_data["media_type"] = media_type
        if generation is not None:
            update_data["generation"] = str(generation)
        if serial_number is not None:
            update_data["serial_number"] = serial_number
            # 更新序列号索引
            old_serial = existing_dict.get('serial_number')
            if old_serial and old_serial != serial_number:
                pipe = redis.pipeline()
                if old_serial:
                    pipe.hdel(KEY_INDEX_TAPE_BY_SERIAL, old_serial)
                if serial_number:
                    pipe.hset(KEY_INDEX_TAPE_BY_SERIAL, serial_number, tape_id)
                await pipe.execute()
        if location is not None:
            update_data["location"] = location
        if capacity_bytes is not None:
            update_data["capacity_bytes"] = str(capacity_bytes)
        if used_bytes is not None:
            update_data["used_bytes"] = str(used_bytes)
        if retention_months is not None:
            update_data["retention_months"] = str(retention_months)
        if notes is not None:
            update_data["notes"] = notes
        if manufactured_date is not None:
            update_data["manufactured_date"] = _datetime_to_str(manufactured_date)
        if expiry_date is not None:
            update_data["expiry_date"] = _datetime_to_str(expiry_date)
        if auto_erase is not None:
            update_data["auto_erase"] = "1" if auto_erase else "0"
        if health_score is not None:
            update_data["health_score"] = str(health_score)
        
        # 更新时间戳
        update_data["updated_at"] = datetime.now().isoformat()
        
        # 更新数据
        if update_data:
            await redis.hset(tape_key, mapping=update_data)
        
        logger.info(f"[Redis模式] 更新磁带记录: {tape_id}")
        return True
        
    except Exception as e:
        logger.error(f"[Redis模式] 更新磁带记录失败: {str(e)}", exc_info=True)
        return False


async def delete_tape_redis(tape_id: str) -> bool:
    """删除磁带记录（Redis版本）"""
    try:
        redis = await get_redis_client()
        
        tape_key = _get_redis_key(KEY_PREFIX_TAPE, tape_id)
        
        # 获取磁带信息（用于删除索引）
        tape_data = await redis.hgetall(tape_key)
        if not tape_data:
            return False
        
        tape_dict = {k if isinstance(k, str) else k.decode('utf-8'): 
                    v if isinstance(v, str) else (v.decode('utf-8') if isinstance(v, bytes) else str(v))
                    for k, v in tape_data.items()}
        
        label = tape_dict.get('label')
        serial_number = tape_dict.get('serial_number')
        
        # 使用事务删除
        pipe = redis.pipeline()
        pipe.delete(tape_key)
        pipe.srem(KEY_INDEX_TAPES, tape_id)
        if label:
            pipe.hdel(KEY_INDEX_TAPE_BY_LABEL, label)
        if serial_number:
            pipe.hdel(KEY_INDEX_TAPE_BY_SERIAL, serial_number)
        
        await pipe.execute()
        
        logger.info(f"[Redis模式] 删除磁带记录: {tape_id}")
        return True
        
    except Exception as e:
        logger.error(f"[Redis模式] 删除磁带记录失败: {str(e)}", exc_info=True)
        return False


async def count_serial_numbers_redis(pattern: str) -> int:
    """统计序列号数量（Redis版本）
    
    Args:
        pattern: 序列号模式，如 "TP11%" 表示以 TP11 开头的序列号
    """
    try:
        redis = await get_redis_client()
        
        # 获取所有磁带
        tape_ids_bytes = await redis.smembers(KEY_INDEX_TAPES)
        count = 0
        
        for tape_id_bytes in tape_ids_bytes:
            tape_id = tape_id_bytes if isinstance(tape_id_bytes, str) else (tape_id_bytes.decode('utf-8') if isinstance(tape_id_bytes, bytes) else str(tape_id_bytes))
            tape_key = _get_redis_key(KEY_PREFIX_TAPE, tape_id)
            serial_number = await redis.hget(tape_key, "serial_number")
            
            if serial_number:
                serial_str = serial_number if isinstance(serial_number, str) else (serial_number.decode('utf-8') if isinstance(serial_number, bytes) else str(serial_number))
                # 简单的模式匹配（支持 % 通配符）
                if '%' in pattern:
                    prefix = pattern.replace('%', '')
                    if serial_str.startswith(prefix):
                        count += 1
                elif serial_str == pattern:
                    count += 1
        
        return count
        
    except Exception as e:
        logger.error(f"[Redis模式] 统计序列号数量失败: {str(e)}", exc_info=True)
        return 0

