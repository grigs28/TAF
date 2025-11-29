#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
备份数据库操作 - SQLite 实现
Backup Database Operations - SQLite Implementation
"""

import logging
import json
import re
from datetime import datetime, timedelta
from pathlib import Path
from typing import Optional, Dict, List
from models.backup import (
    BackupSet,
    BackupSetStatus,
    BackupTaskType,
    BackupTask,
    BackupTaskStatus,
    BackupFile,
    BackupFileType,
)
from utils.datetime_utils import format_datetime, now
from utils.scheduler.sqlite_utils import get_sqlite_connection

logger = logging.getLogger(__name__)


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


def _datetime_from_stat(stat_obj, attr_name: str):
    """从 os.stat_result 中提取时间"""
    if not stat_obj:
        return None
    timestamp = getattr(stat_obj, attr_name, None)
    if timestamp is None:
        return None
    try:
        return datetime.fromtimestamp(timestamp)
    except Exception:
        return None


def _normalize_file_type(processed_file: Dict) -> BackupFileType:
    """根据 processed_file 中的信息确定 BackupFileType"""
    file_type_value = processed_file.get("file_type")
    if isinstance(file_type_value, BackupFileType):
        return file_type_value
    if isinstance(file_type_value, str):
        try:
            return BackupFileType(file_type_value.lower())
        except ValueError:
            pass
    if processed_file.get("is_dir"):
        return BackupFileType.DIRECTORY
    if processed_file.get("is_symlink"):
        return BackupFileType.SYMLINK
    return BackupFileType.FILE


def _ensure_metadata_dict(raw_metadata) -> Dict:
    """确保 file_metadata 为 dict"""
    if isinstance(raw_metadata, dict):
        return dict(raw_metadata)
    if isinstance(raw_metadata, str):
        try:
            parsed = json.loads(raw_metadata)
            return parsed if isinstance(parsed, dict) else {}
        except json.JSONDecodeError:
            return {}
    return {}


async def create_backup_set_sqlite(backup_task, tape) -> BackupSet:
    """创建备份集（SQLite 版本）"""
    try:
        # 生成备份集ID
        backup_group = format_datetime(now(), '%Y-%m')
        set_id = f"{backup_group}_{backup_task.id:06d}"
        backup_time = datetime.now()
        retention_until = backup_time + timedelta(days=backup_task.retention_days)
        
        # 使用原生 SQL 插入
        async with get_sqlite_connection() as conn:
            # 准备 source_info
            source_info = {'paths': backup_task.source_paths} if backup_task.source_paths else None
            source_info_json = json.dumps(source_info) if source_info else None
            
            # 插入备份集
            cursor = await conn.execute("""
                INSERT INTO backup_sets (
                    set_id, set_name, backup_group, status, backup_task_id, tape_id,
                    backup_type, backup_time, source_info, retention_until, auto_delete,
                    created_at, updated_at
                ) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
            """, (
                set_id,
                f"{backup_task.task_name}_{set_id}",
                backup_group,
                BackupSetStatus.ACTIVE.value,
                backup_task.id,
                tape.tape_id if hasattr(tape, 'tape_id') else None,
                backup_task.task_type.value if hasattr(backup_task.task_type, 'value') else str(backup_task.task_type),
                backup_time,
                source_info_json,
                retention_until,
                True,
                backup_time,
                backup_time
            ))
            await conn.commit()
            
            # 查询插入的记录获取 id
            cursor = await conn.execute("""
                SELECT id, set_id, set_name, backup_group, status, backup_task_id, tape_id,
                       backup_type, backup_time, source_info, retention_until, created_at, updated_at
                FROM backup_sets
                WHERE set_id = ?
            """, (set_id,))
            row = await cursor.fetchone()
            
            if not row:
                raise RuntimeError(f"创建备份集后查询失败: set_id={set_id}")
            
            # 创建 BackupSet 对象返回
            backup_set = BackupSet(
                id=row[0],
                set_id=row[1],
                set_name=row[2],
                backup_group=row[3],
                status=BackupSetStatus(row[4]) if isinstance(row[4], str) else row[4],
                backup_task_id=row[5],
                tape_id=row[6],
                backup_type=BackupTaskType(row[7]) if isinstance(row[7], str) else row[7],
                backup_time=row[8],
                source_info=json.loads(row[9]) if row[9] else None,
                retention_until=row[10]
            )
            
            logger.info(
                f"[创建备份集] 创建完成: set_id={set_id}, "
                f"backup_set.id={backup_set.id}, "
                f"backup_set.set_id={backup_set.set_id}, "
                f"backup_task_id={backup_set.backup_task_id}"
            )
            
            return backup_set
            
    except Exception as e:
        logger.error(f"创建备份集失败: {str(e)}", exc_info=True)
        raise


async def finalize_backup_set_sqlite(backup_set: BackupSet, file_count: int, total_size: int):
    """完成备份集（SQLite 版本）"""
    try:
        backup_set.total_files = file_count
        backup_set.total_bytes = total_size
        backup_set.compressed_bytes = total_size
        backup_set.compression_ratio = total_size / backup_set.total_bytes if backup_set.total_bytes > 0 else 1.0
        backup_set.chunk_count = 1
        
        async with get_sqlite_connection() as conn:
            # 查询备份集是否存在
            cursor = await conn.execute("""
                SELECT id FROM backup_sets WHERE set_id = ?
            """, (backup_set.set_id,))
            row = await cursor.fetchone()
            
            if row:
                # 更新备份集
                await conn.execute("""
                    UPDATE backup_sets
                    SET total_files = ?, total_bytes = ?, compressed_bytes = ?,
                        compression_ratio = ?, chunk_count = ?, updated_at = ?
                    WHERE set_id = ?
                """, (
                    file_count,
                    total_size,
                    total_size,
                    backup_set.compression_ratio,
                    backup_set.chunk_count,
                    datetime.now(),
                    backup_set.set_id
                ))
                await conn.commit()
            else:
                logger.warning(f"找不到备份集: {backup_set.set_id}，无法更新")
                
    except Exception as e:
        logger.error(f"完成备份集失败: {str(e)}", exc_info=True)
        raise


async def update_scan_progress_sqlite(
    backup_task: BackupTask,
    scanned_count: int,
    valid_count: int,
    operation_status: str = None
):
    """更新扫描进度（SQLite 版本）"""
    try:
        if not backup_task or not backup_task.id:
            return
        
        async with get_sqlite_connection() as conn:
            # 查询任务
            cursor = await conn.execute("""
                SELECT total_files, total_bytes, result_summary, description
                FROM backup_tasks
                WHERE id = ?
            """, (backup_task.id,))
            row = await cursor.fetchone()
            
            if not row:
                logger.warning(f"找不到备份任务: {backup_task.id}")
                return
            
            # 读取当前值
            total_files_value = row[0] or 0
            total_bytes_from_db = row[1] or 0
            result_summary = row[2] or {}
            if isinstance(result_summary, str):
                try:
                    result_summary = json.loads(result_summary)
                except json.JSONDecodeError:
                    result_summary = {}
            elif not isinstance(result_summary, dict):
                result_summary = {}
            
            # 获取压缩字节数
            compressed_bytes = getattr(backup_task, 'compressed_bytes', None) or 0
            processed_bytes = getattr(backup_task, 'processed_bytes', None) or 0
            
            # 更新 result_summary 中的 estimated_archive_count
            if hasattr(backup_task, 'result_summary') and backup_task.result_summary:
                if isinstance(backup_task.result_summary, dict):
                    if 'estimated_archive_count' in backup_task.result_summary:
                        result_summary['estimated_archive_count'] = backup_task.result_summary['estimated_archive_count']
            
            # 处理描述
            current_desc = row[3] or ''
            if operation_status:
                if '[格式化中]' in current_desc:
                    cleaned_desc = re.sub(r'\[(?!格式化中)[^\]]+\.\.\.\]', '', current_desc)
                    cleaned_desc = cleaned_desc.replace('  ', ' ').strip()
                    new_desc = cleaned_desc + ' ' + operation_status if cleaned_desc else operation_status
                else:
                    cleaned_desc = re.sub(r'\[[^\]]+\.\.\.\]', '', current_desc)
                    cleaned_desc = cleaned_desc.replace('  ', ' ').strip()
                    new_desc = cleaned_desc + ' ' + operation_status if cleaned_desc else operation_status
            else:
                new_desc = current_desc
            
            # 更新字段
            await conn.execute("""
                UPDATE backup_tasks
                SET progress_percent = ?, processed_files = ?, total_files = ?,
                    processed_bytes = ?, compressed_bytes = ?, result_summary = ?,
                    description = ?, updated_at = ?
                WHERE id = ?
            """, (
                backup_task.progress_percent,
                scanned_count,
                total_files_value,  # 保持不变
                processed_bytes,
                compressed_bytes,
                json.dumps(result_summary) if result_summary else None,
                new_desc,
                datetime.now(),
                backup_task.id
            ))
            await conn.commit()
            
    except Exception as e:
        logger.warning(f"更新扫描进度失败（忽略继续）: {str(e)}")


async def update_task_status_sqlite(backup_task: BackupTask, status: BackupTaskStatus):
    """更新任务状态（SQLite 版本）"""
    try:
        backup_task.status = status
        current_time = datetime.now()
        
        async with get_sqlite_connection() as conn:
            # 查询任务是否存在
            cursor = await conn.execute("SELECT id FROM backup_tasks WHERE id = ?", (backup_task.id,))
            row = await cursor.fetchone()
            
            if not row:
                logger.warning(f"找不到备份任务: {backup_task.id}")
                return
            
            # 构建更新 SQL
            status_value = status.value if hasattr(status, 'value') else str(status)
            update_fields = ["status = ?", "updated_at = ?"]
            update_values = [status_value, current_time]
            
            if status == BackupTaskStatus.RUNNING:
                update_fields.append("started_at = ?")
                update_values.append(current_time)
                if hasattr(backup_task, 'source_paths') and backup_task.source_paths:
                    update_fields.append("source_paths = ?")
                    update_values.append(json.dumps(backup_task.source_paths))
                if hasattr(backup_task, 'tape_id') and backup_task.tape_id:
                    update_fields.append("tape_id = ?")
                    update_values.append(backup_task.tape_id)
            elif status in (BackupTaskStatus.COMPLETED, BackupTaskStatus.FAILED, BackupTaskStatus.CANCELLED):
                update_fields.append("completed_at = ?")
                update_values.append(current_time)
            
            update_values.append(backup_task.id)
            
            await conn.execute(f"""
                UPDATE backup_tasks
                SET {', '.join(update_fields)}
                WHERE id = ?
            """, tuple(update_values))
            await conn.commit()
            
    except Exception as e:
        logger.error(f"更新任务状态失败: {str(e)}", exc_info=True)


async def update_task_stage_async_sqlite(backup_task: BackupTask, stage_code: str, description: str = None):
    """更新任务阶段（SQLite 版本）"""
    try:
        if not backup_task or not getattr(backup_task, 'id', None):
            logger.warning("无效的任务对象，无法更新阶段")
            return
        
        task_id = backup_task.id
        current_time = datetime.now()
        
        async with get_sqlite_connection() as conn:
            # 查询任务是否存在
            cursor = await conn.execute("SELECT id FROM backup_tasks WHERE id = ?", (task_id,))
            row = await cursor.fetchone()
            
            if not row:
                logger.warning(f"找不到备份任务: {task_id}")
                return
            
            # 构建更新 SQL
            update_fields = ["updated_at = ?"]
            update_values = [current_time]
            
            if stage_code:
                update_fields.append("operation_stage = ?")
                update_values.append(stage_code)
            
            if description:
                update_fields.append("description = ?")
                update_values.append(description)
            
            update_values.append(task_id)
            
            await conn.execute(f"""
                UPDATE backup_tasks
                SET {', '.join(update_fields)}
                WHERE id = ?
            """, tuple(update_values))
            await conn.commit()
            
        logger.debug(f"任务 {task_id} 阶段更新为: {stage_code}" + (f", 描述: {description}" if description else ""))
        
    except Exception as e:
        logger.error(f"更新任务阶段失败: {str(e)}", exc_info=True)


async def update_task_fields_sqlite(backup_task: BackupTask, **fields):
    """更新任务的特定字段（SQLite 版本）"""
    try:
        if not fields:
            return
        
        async with get_sqlite_connection() as conn:
            # 查询任务是否存在
            cursor = await conn.execute("SELECT id FROM backup_tasks WHERE id = ?", (backup_task.id,))
            row = await cursor.fetchone()
            
            if not row:
                logger.warning(f"找不到备份任务: {backup_task.id}")
                return
            
            # 构建更新 SQL
            update_fields = []
            update_values = []
            
            for field, value in fields.items():
                # 处理 JSON 字段
                if field in ('source_paths', 'exclude_patterns', 'result_summary'):
                    value = json.dumps(value) if value else None
                # 处理枚举类型
                elif hasattr(value, 'value'):
                    value = value.value
                update_fields.append(f"{field} = ?")
                update_values.append(value)
            
            update_fields.append("updated_at = ?")
            update_values.append(datetime.now())
            update_values.append(backup_task.id)
            
            await conn.execute(f"""
                UPDATE backup_tasks
                SET {', '.join(update_fields)}
                WHERE id = ?
            """, tuple(update_values))
            await conn.commit()
            
    except Exception as e:
        logger.error(f"更新任务字段失败: {str(e)}", exc_info=True)


async def get_task_status_sqlite(task_id: int) -> Optional[Dict]:
    """获取任务状态（SQLite 版本）"""
    try:
        async with get_sqlite_connection() as conn:
            cursor = await conn.execute("""
                SELECT id, task_name, task_type, status, total_files, processed_files,
                       total_bytes, processed_bytes, compressed_bytes, progress_percent,
                       error_message, result_summary, scan_status, operation_stage,
                       started_at, completed_at, created_at, updated_at
                FROM backup_tasks
                WHERE id = ?
            """, (task_id,))
            row = await cursor.fetchone()
            
            if not row:
                return None
            
            # 解析行数据
            task_id = row[0]
            task_name = row[1]
            task_type = row[2]
            status = row[3]
            total_files = row[4] or 0
            processed_files = row[5] or 0
            total_bytes = row[6] or 0
            processed_bytes = row[7] or 0
            compressed_bytes = row[8] or 0
            progress_percent = row[9] or 0.0
            error_message = row[10]
            result_summary = row[11]
            scan_status = row[12]
            operation_stage = row[13]
            started_at = row[14]
            completed_at = row[15]
            created_at = row[16]
            updated_at = row[17]
            
            # 计算压缩率
            compression_ratio = 0.0
            if processed_bytes and processed_bytes > 0 and compressed_bytes:
                compression_ratio = float(compressed_bytes) / float(processed_bytes)
            
            # 解析 result_summary
            estimated_archive_count = None
            total_scanned_bytes = None
            if result_summary:
                try:
                    result_summary_dict = None
                    if isinstance(result_summary, str):
                        result_summary_dict = json.loads(result_summary)
                    elif isinstance(result_summary, dict):
                        result_summary_dict = result_summary
                    
                    if isinstance(result_summary_dict, dict):
                        estimated_archive_count = result_summary_dict.get('estimated_archive_count')
                        total_scanned_bytes = result_summary_dict.get('total_scanned_bytes')
                except Exception:
                    pass
            
            # 解析 source_paths
            source_paths = []
            cursor2 = await conn.execute("SELECT source_paths FROM backup_tasks WHERE id = ?", (task_id,))
            row2 = await cursor2.fetchone()
            if row2 and row2[0]:
                try:
                    source_paths = json.loads(row2[0]) if isinstance(row2[0], str) else row2[0]
                except:
                    source_paths = []
            
            # 解析 tape_device
            cursor3 = await conn.execute("SELECT tape_device, tape_id FROM backup_tasks WHERE id = ?", (task_id,))
            row3 = await cursor3.fetchone()
            tape_device = row3[0] if row3 else None
            tape_id = row3[1] if row3 else None
            
            # 解析 description
            cursor4 = await conn.execute("SELECT description FROM backup_tasks WHERE id = ?", (task_id,))
            row4 = await cursor4.fetchone()
            description = row4[0] if row4 else ''
            
            return {
                'id': task_id,
                'status': status,
                'progress_percent': progress_percent,
                'processed_files': processed_files,
                'total_files': total_files,
                'processed_bytes': processed_bytes,
                'total_bytes': total_bytes,
                'compressed_bytes': compressed_bytes,
                'compression_ratio': compression_ratio,
                'description': description,
                'source_paths': source_paths,
                'tape_device': tape_device,
                'tape_id': tape_id,
                'estimated_archive_count': estimated_archive_count,
                'total_scanned_bytes': total_scanned_bytes,
                'started_at': started_at,
                'completed_at': completed_at
            }
            
    except Exception as e:
        logger.error(f"获取任务状态失败: {str(e)}", exc_info=True)
        return None


async def update_scan_progress_only_sqlite(backup_task: BackupTask, total_files: int, total_bytes: int):
    """仅更新扫描进度（总文件数和总字节数）（SQLite 版本）"""
    try:
        if not backup_task or not backup_task.id:
            return
        
        backup_task.total_files = total_files
        backup_task.total_bytes = total_bytes
        
        async with get_sqlite_connection() as conn:
            # 查询任务
            cursor = await conn.execute("""
                SELECT result_summary FROM backup_tasks WHERE id = ?
            """, (backup_task.id,))
            row = await cursor.fetchone()
            
            if not row:
                logger.warning(f"找不到备份任务: {backup_task.id}")
                return
            
            # 获取当前的 result_summary
            result_summary = row[0] or {}
            if isinstance(result_summary, str):
                try:
                    result_summary = json.loads(result_summary)
                except json.JSONDecodeError:
                    result_summary = {}
            elif not isinstance(result_summary, dict):
                result_summary = {}
            
            # 更新 result_summary
            result_summary['total_scanned_files'] = total_files
            result_summary['total_scanned_bytes'] = total_bytes
            
            # 更新数据库
            await conn.execute("""
                UPDATE backup_tasks
                SET total_files = ?, total_bytes = ?, result_summary = ?, updated_at = ?
                WHERE id = ?
            """, (
                total_files,
                total_bytes,
                json.dumps(result_summary) if result_summary else None,
                datetime.now(),
                backup_task.id
            ))
            await conn.commit()
            
    except Exception as e:
        logger.warning(f"更新扫描进度失败（忽略继续）: {str(e)}")


async def get_scan_status_sqlite(backup_task_id: int) -> Optional[str]:
    """获取扫描状态（SQLite 版本）"""
    try:
        async with get_sqlite_connection() as conn:
            cursor = await conn.execute("""
                SELECT scan_status FROM backup_tasks WHERE id = ?
            """, (backup_task_id,))
            row = await cursor.fetchone()
            return row[0] if row else None
    except Exception as e:
        logger.warning(f"获取扫描状态失败: {str(e)}")
        return None


async def update_scan_status_sqlite(backup_task_id: int, status: str):
    """更新扫描状态（SQLite 版本）"""
    try:
        current_time = datetime.now()
        
        async with get_sqlite_connection() as conn:
            # 查询任务是否存在
            cursor = await conn.execute("SELECT id FROM backup_tasks WHERE id = ?", (backup_task_id,))
            row = await cursor.fetchone()
            
            if not row:
                logger.warning(f"找不到备份任务: {backup_task_id}")
                return
            
            # 构建更新 SQL
            update_fields = ["scan_status = ?", "updated_at = ?"]
            update_values = [status, current_time]
            
            if status == 'completed':
                update_fields.append("scan_completed_at = ?")
                update_values.append(current_time)
            
            update_values.append(backup_task_id)
            
            await conn.execute(f"""
                UPDATE backup_tasks
                SET {', '.join(update_fields)}
                WHERE id = ?
            """, tuple(update_values))
            await conn.commit()
            
    except Exception as e:
        logger.warning(f"更新扫描状态失败: {str(e)}")


async def insert_backup_files_sqlite(file_dicts: List[Dict]) -> List[int]:
    """将扫描结果插入 SQLite 的 backup_files 表（使用原生 SQL 批量插入，避免 RETURNING 子句问题）"""
    if not file_dicts:
        return []
    
    try:
        from utils.scheduler.sqlite_utils import get_sqlite_connection
        import json as json_module
        
        inserted_ids: List[int] = []
        
        # 调试：检查第一个文件的 backup_set_id
        first_file_backup_set_id = file_dicts[0].get("backup_set_id") if file_dicts else None
        logger.debug(
            f"[插入文件到SQLite] 准备插入 {len(file_dicts)} 个文件, "
            f"第一个文件的 backup_set_id={first_file_backup_set_id}"
        )
        
        # 使用原生 SQL 批量插入，避免 SQLAlchemy 的 RETURNING 子句导致的游标问题
        async with get_sqlite_connection() as conn:
            # 准备批量插入数据
            insert_data = []
            for file_info in file_dicts:
                file_metadata = file_info.get("file_metadata")
                if isinstance(file_metadata, str):
                    try:
                        file_metadata = json_module.loads(file_metadata)
                    except json.JSONDecodeError:
                        file_metadata = None
                file_metadata_str = json_module.dumps(file_metadata) if file_metadata else None

                tags = file_info.get("tags")
                if isinstance(tags, str):
                    try:
                        tags = json_module.loads(tags)
                    except json.JSONDecodeError:
                        tags = None
                tags_str = json_module.dumps(tags) if tags else None

                file_type_value = file_info.get("file_type", "file")
                if isinstance(file_type_value, BackupFileType):
                    file_type = file_type_value.value
                else:
                    try:
                        file_type = BackupFileType(file_type_value.lower()).value
                    except Exception:
                        file_type = BackupFileType.FILE.value

                insert_data.append((
                    file_info["backup_set_id"],
                    file_info["file_path"],
                    file_info["file_name"],
                    file_info.get("directory_path"),
                    file_info.get("display_name"),
                    file_type,
                    file_info.get("file_size", 0),
                    file_info.get("compressed_size"),
                    file_info.get("file_permissions"),
                    file_info.get("file_owner"),
                    file_info.get("file_group"),
                    file_info.get("created_time"),
                    file_info.get("modified_time"),
                    file_info.get("accessed_time"),
                    file_info.get("tape_block_start"),
                    file_info.get("tape_block_count"),
                    file_info.get("compressed", False),
                    file_info.get("encrypted", False),
                    file_info.get("checksum"),
                    file_info.get("is_copy_success", False),
                    file_info.get("copy_status_at"),
                    file_info.get("backup_time"),
                    file_info.get("chunk_number"),
                    file_info.get("version", 1),
                    file_metadata_str,
                    tags_str,
                    None,  # created_by
                    None,  # updated_by
                ))
            
            # 在插入前记录当前最大 ID（用于后续查询新插入的记录）
            backup_set_id = insert_data[0][0] if insert_data else None
            max_id_before = None
            if backup_set_id:
                cursor = await conn.execute(
                    "SELECT MAX(id) FROM backup_files WHERE backup_set_id = ?",
                    (backup_set_id,)
                )
                max_id_row = await cursor.fetchone()
                max_id_before = max_id_row[0] if max_id_row and max_id_row[0] else 0
            
            # 批量插入（使用 executemany）
            # 如果数据量很大，分批插入以避免长时间锁定
            batch_size = 10000  # 每批最多 10000 条
            if len(insert_data) > batch_size:
                logger.info(f"[插入文件到SQLite] 数据量较大（{len(insert_data)} 条），将分批插入（每批 {batch_size} 条）")
                for i in range(0, len(insert_data), batch_size):
                    batch_data = insert_data[i:i + batch_size]
                    await conn.executemany("""
                        INSERT INTO backup_files (
                            backup_set_id, file_path, file_name, directory_path, display_name,
                            file_type, file_size, compressed_size, file_permissions, file_owner,
                            file_group, created_time, modified_time, accessed_time, tape_block_start,
                            tape_block_count, compressed, encrypted, checksum, is_copy_success,
                            copy_status_at, backup_time, chunk_number, version, file_metadata, tags,
                            created_by, updated_by
                        ) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
                    """, batch_data)
                    logger.debug(f"[插入文件到SQLite] 已插入批次 {i // batch_size + 1}/{(len(insert_data) + batch_size - 1) // batch_size}，{len(batch_data)} 条")
            else:
                await conn.executemany("""
                    INSERT INTO backup_files (
                        backup_set_id, file_path, file_name, directory_path, display_name,
                        file_type, file_size, compressed_size, file_permissions, file_owner,
                        file_group, created_time, modified_time, accessed_time, tape_block_start,
                        tape_block_count, compressed, encrypted, checksum, is_copy_success,
                        copy_status_at, backup_time, chunk_number, version, file_metadata, tags,
                        created_by, updated_by
                    ) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
                """, insert_data)
            
            await conn.commit()
            
            # 验证插入后的文件数
            check_stmt = "SELECT COUNT(*) FROM backup_files WHERE backup_set_id = ?"
            check_cursor = await conn.execute(check_stmt, (backup_set_id,))
            check_result = await check_cursor.fetchone()
            files_after_insert = check_result[0] if check_result else 0
            logger.debug(
                f"[插入文件到SQLite] 插入完成，backup_set_id={backup_set_id} 下现在有 {files_after_insert} 个文件 "
                f"(插入前: {max_id_before or 0}, 插入: {len(insert_data)} 个)"
            )
            
            # 查询新插入的记录 ID（通过比较插入前后的最大 ID）
            if backup_set_id and max_id_before is not None:
                cursor = await conn.execute(
                    "SELECT id FROM backup_files WHERE backup_set_id = ? AND id > ? ORDER BY id",
                    (backup_set_id, max_id_before)
                )
                rows = await cursor.fetchall()
                inserted_ids = [row[0] for row in rows]
            else:
                # 如果无法确定插入前的最大 ID，使用 last_insert_rowid() 作为后备方案
                cursor = await conn.execute("SELECT last_insert_rowid()")
                last_id_row = await cursor.fetchone()
                last_id = last_id_row[0] if last_id_row else None
                
                if last_id:
                    # 计算插入的 ID 范围（假设 ID 是连续的）
                    count = len(insert_data)
                    # 生成 ID 列表（从 last_id - count + 1 到 last_id）
                    inserted_ids = list(range(last_id - count + 1, last_id + 1))
                else:
                    inserted_ids = []
        
        return inserted_ids
    except Exception as e:
        logger.error(f"插入 SQLite backup_files 失败: {str(e)}", exc_info=True)
        raise


async def fetch_pending_files_grouped_by_size_sqlite(
    backup_set_db_id: int,
    max_file_size: int,
    backup_task_id: int = None,
    should_wait_if_small: bool = True
) -> List[List[Dict]]:
    """SQLite 版本：获取待压缩文件组"""
    try:
        from backup.utils import format_bytes

        max_retries = 6
        retry_count = 0 if should_wait_if_small else max_retries

        logger.info(
            f"[SQLite压缩检索] 开始检索待压缩文件: backup_set_id={backup_set_db_id}, "
            f"max_file_size={format_bytes(max_file_size)}, backup_task_id={backup_task_id}, "
            f"should_wait_if_small={should_wait_if_small}, retry_count={retry_count}/{max_retries}"
        )

        async with get_sqlite_connection() as conn:
            # 先查询总数和状态统计
            # 调试：先检查数据库中是否有任何文件
            cursor = await conn.execute("SELECT COUNT(*) FROM backup_files")
            row = await cursor.fetchone()
            all_files_count = row[0] if row else 0
            logger.info(f"[SQLite压缩检索] 数据库中总文件数（所有备份集）: {all_files_count}")
            
            # 检查是否有其他 backup_set_id 的文件
            cursor = await conn.execute("""
                SELECT backup_set_id, COUNT(*) as cnt
                FROM backup_files
                GROUP BY backup_set_id
                LIMIT 5
            """)
            other_sets = await cursor.fetchall()
            if other_sets:
                logger.info(
                    f"[SQLite压缩检索] 数据库中各备份集文件数: "
                    f"{[(row[0], row[1]) for row in other_sets[:5]]}"
                )
            else:
                logger.warning(f"[SQLite压缩检索] ⚠️ 数据库中没有任何文件记录！")
            
            # 查询总文件数
            cursor = await conn.execute("""
                SELECT COUNT(*) FROM backup_files
                WHERE backup_set_id = ? AND file_type = ?
            """, (backup_set_db_id, BackupFileType.FILE.value))
            row = await cursor.fetchone()
            total_files = row[0] if row else 0
            
            # 查询已压缩的文件数
            cursor = await conn.execute("""
                SELECT COUNT(*) FROM backup_files
                WHERE backup_set_id = ? AND file_type = ? AND is_copy_success = 1
            """, (backup_set_db_id, BackupFileType.FILE.value))
            row = await cursor.fetchone()
            copied_files = row[0] if row else 0
            
            # 查询未压缩的文件
            # 注意：SQLite 中 Boolean 类型存储为整数（0/1），NULL 需要特殊处理
            cursor = await conn.execute("""
                SELECT id, backup_set_id, file_path, file_name, directory_path, display_name,
                       file_type, file_size, compressed_size, file_permissions, file_owner,
                       file_group, created_time, modified_time, accessed_time, tape_block_start,
                       tape_block_count, compressed, encrypted, checksum, is_copy_success,
                       copy_status_at, backup_time, chunk_number, version, file_metadata, tags
                FROM backup_files
                WHERE backup_set_id = ? 
                  AND (is_copy_success = 0 OR is_copy_success IS NULL)
                  AND file_type = ?
                ORDER BY id
            """, (backup_set_db_id, BackupFileType.FILE.value))
            rows = await cursor.fetchall()
            
            # 调试：检查前几个文件的状态
            if rows and len(rows) > 0:
                sample_row = rows[0]
                logger.debug(
                    f"[SQLite压缩检索] 示例文件状态: id={sample_row[0]}, "
                    f"file_path={sample_row[2][:100] if sample_row[2] else None}, "
                    f"is_copy_success={sample_row[20]} (type: {type(sample_row[20])})"
                )

        logger.info(
            f"[SQLite压缩检索] 数据库查询完成: "
            f"总文件数={total_files}, 已压缩={copied_files}, 未压缩={len(rows)}, "
            f"backup_set_id={backup_set_db_id}"
        )
        
        # 如果总文件数为0，检查是否有其他 backup_set_id 的文件
        if total_files == 0:
            # 查询所有 backup_sets 表，看看是否有其他备份集
            async with get_sqlite_connection() as conn:
                cursor = await conn.execute("""
                    SELECT id, set_id, backup_task_id
                    FROM backup_sets
                    LIMIT 5
                """)
                all_sets_rows = await cursor.fetchall()
                
                # 格式化备份集信息用于日志
                backup_sets_info = [(row[0], row[1]) for row in all_sets_rows]
                
                logger.warning(
                    f"[SQLite压缩检索] ⚠️ 警告：backup_set_id={backup_set_db_id} 下没有文件！"
                    f"数据库中存在的备份集: {backup_sets_info}"
                )
                
                # 检查是否有其他 backup_set_id 的文件
                if all_sets_rows:
                    for backup_set_info in all_sets_rows[:3]:  # 只检查前3个
                        other_set_id = backup_set_info[0]  # id 是第一个字段
                        if other_set_id != backup_set_db_id:
                            cursor = await conn.execute("""
                                SELECT COUNT(*) FROM backup_files
                                WHERE backup_set_id = ? AND file_type = ?
                            """, (other_set_id, BackupFileType.FILE.value))
                            row = await cursor.fetchone()
                            other_count = row[0] if row else 0
                            if other_count > 0:
                                logger.warning(
                                    f"[SQLite压缩检索] 发现其他备份集 backup_set_id={other_set_id} 有 {other_count} 个文件！"
                                    f"当前查询的 backup_set_id={backup_set_db_id} 可能不正确。"
                                )
        
        # 如果总文件数大于0但未压缩文件数为0，记录警告
        if total_files > 0 and len(rows) == 0:
            logger.warning(
                f"[SQLite压缩检索] ⚠️ 警告：总文件数={total_files}，但未找到未压缩文件！"
                f"可能原因：1) 所有文件已标记为 is_copy_success=True 2) 查询条件不匹配"
            )

        if not rows:
            logger.warning(f"[SQLite压缩检索] 未找到任何待压缩文件（backup_set_id={backup_set_db_id}），可能原因：1) 文件尚未同步到SQLite主库 2) 所有文件已压缩")
            return []

        # 转换为统一的文件字典
        # 注意：字段名必须与 openGauss 版本保持一致，同时兼容压缩器使用的字段名
        # rows 字段顺序：id, backup_set_id, file_path, file_name, directory_path, display_name,
        # file_type, file_size, compressed_size, file_permissions, file_owner,
        # file_group, created_time, modified_time, accessed_time, tape_block_start,
        # tape_block_count, compressed, encrypted, checksum, is_copy_success,
        # copy_status_at, backup_time, chunk_number, version, file_metadata, tags
        all_files: List[Dict] = []
        for row in rows:
            file_type_value = row[6] if row[6] else "file"  # file_type 是第7个字段（索引6）
            all_files.append(
                {
                    "id": row[0],  # id
                    "path": row[2],  # file_path，压缩器使用 'path'
                    "file_path": row[2],  # file_path，mark_files_as_copied 使用 'file_path'
                    "name": row[3],  # file_name，压缩器可能使用 'name'
                    "file_name": row[3],  # file_name，mark_files_as_copied 使用 'file_name'
                    "directory_path": row[4],  # directory_path
                    "display_name": row[5],  # display_name
                    "size": row[7] or 0,  # file_size，压缩器使用 'size'
                    "file_size": row[7] or 0,  # file_size，mark_files_as_copied 使用 'file_size'
                    "permissions": row[9],  # file_permissions
                    "file_permissions": row[9],  # file_permissions，mark_files_as_copied 使用 'file_permissions'
                    "modified_time": _parse_datetime_value(row[13]),  # modified_time
                    "accessed_time": _parse_datetime_value(row[14]),  # accessed_time
                    "created_time": _parse_datetime_value(row[12]),  # created_time
                    "is_dir": str(file_type_value).lower() == "directory",
                    "is_file": str(file_type_value).lower() == "file",
                    "is_symlink": str(file_type_value).lower() == "symlink",
                }
            )

        tolerance = max_file_size * 0.05
        min_group_size = max_file_size - tolerance
        max_group_size = max_file_size + tolerance

        current_group: List[Dict] = []
        current_group_size = 0
        skipped_files: List[Dict] = []

        logger.info(
            f"[SQLite策略] 检索到 {len(all_files)} 个未压缩文件，"
            f"目标范围：{format_bytes(min_group_size)} - {format_bytes(max_file_size)} "
            f"(含容差上限：{format_bytes(max_group_size)})，重试次数：{retry_count}/{max_retries}"
        )

        for file_info in all_files:
            file_size = file_info["size"]

            if file_size > max_group_size:
                if current_group:
                    logger.info(
                        f"[SQLite策略] 返回当前组：{len(current_group)} 个文件，总大小 {format_bytes(current_group_size)}，"
                        f"发现超大文件将单独处理"
                    )
                    return [current_group]

                logger.warning(
                    f"[SQLite策略] 发现超大文件单独成组：{format_bytes(file_size)} (超过最大大小 {format_bytes(max_file_size)} 含容差)"
                )
                return [[file_info]]

            new_group_size = current_group_size + file_size
            if new_group_size > max_file_size:
                skipped_files.append(file_info)
                logger.debug(
                    f"[SQLite策略] 跳过文件（超过最大大小）：{file_info['name']} "
                    f"({format_bytes(file_size)})，当前组：{format_bytes(current_group_size)}"
                )
                continue

            current_group.append(file_info)
            current_group_size = new_group_size

        if not current_group:
            if skipped_files:
                logger.warning(
                    f"[SQLite策略] 所有文件都超过最大大小，跳过了 {len(skipped_files)} 个文件。"
                    f"当前组大小: {format_bytes(current_group_size)}, 最大大小: {format_bytes(max_file_size)}"
                )
            else:
                logger.info(
                    f"[SQLite策略] 没有待压缩文件。检索到 {len(all_files)} 个文件，"
                    f"但无法构建文件组（可能所有文件都超过阈值）"
                )
            return []

        size_ratio = current_group_size / max_file_size if max_file_size > 0 else 0
        scan_status = await get_scan_status_sqlite(backup_task_id) if backup_task_id else None

        logger.info(
            f"[SQLite策略] 文件组构建完成: {len(current_group)} 个文件，"
            f"总大小={format_bytes(current_group_size)}, 大小比例={size_ratio*100:.1f}%, "
            f"扫描状态={scan_status}, retry_count={retry_count}/{max_retries}"
        )

        if current_group_size < min_group_size and scan_status != "completed" and retry_count < max_retries:
            logger.warning(
                f"[SQLite策略] 文件组大小低于容差下限：{format_bytes(current_group_size)} "
                f"(需要 ≥ {format_bytes(min_group_size)} = {size_ratio*100:.1f}% of 目标)，"
                f"扫描状态：{scan_status}，等待更多文件...（重试 {retry_count}/{max_retries}）"
            )
            return []

        if current_group_size >= min_group_size:
            logger.info(
                f"[SQLite策略] ✅ 达到容差范围内：{format_bytes(current_group_size)} "
                f"({size_ratio*100:.1f}% of 目标，≥ {format_bytes(min_group_size)})，"
                f"跳过了 {len(skipped_files)} 个文件，返回文件组（{len(current_group)} 个文件）"
            )
        else:
            reason = "扫描已完成" if scan_status == "completed" else "达到重试上限"
            logger.warning(
                f"[SQLite策略] ⚠️ 强制压缩：文件组大小 {format_bytes(current_group_size)} "
                f"({size_ratio*100:.1f}% of 目标，< {format_bytes(min_group_size)})，原因：{reason}，"
                f"返回文件组（{len(current_group)} 个文件）"
            )

        logger.info(
            f"[SQLite策略] 最终返回: 1个文件组，包含 {len(current_group)} 个文件，"
            f"总大小={format_bytes(current_group_size)}"
        )
        return [current_group]
    except Exception as e:
        logger.error(f"获取SQLite压缩文件组失败: {str(e)}", exc_info=True)
        return []


async def get_compressed_files_count_sqlite(backup_set_db_id: int) -> int:
    """SQLite 版本：查询已压缩文件数（聚合所有进程的进度）
    
    注意：只统计真正压缩完成的文件（chunk_number IS NOT NULL），
    不包括预取时标记为已入队的文件（is_copy_success = 1 但 chunk_number IS NULL）。
    
    Args:
        backup_set_db_id: 备份集数据库ID
        
    Returns:
        已压缩文件数（is_copy_success = 1 且 chunk_number IS NOT NULL 的文件数）
    """
    try:
        from utils.scheduler.sqlite_utils import get_sqlite_connection
        from models.backup import BackupFileType
        
        async with get_sqlite_connection() as conn:
            cursor = await conn.execute(
                """
                SELECT COUNT(*) as count
                FROM backup_files
                WHERE backup_set_id = ?
                  AND is_copy_success = 1
                  AND chunk_number IS NOT NULL
                  AND file_type = ?
                """,
                (backup_set_db_id, BackupFileType.FILE.value)
            )
            row = await cursor.fetchone()
            return row[0] if row else 0
    except Exception as e:
        logger.error(f"[SQLite模式] 查询已压缩文件数失败: {str(e)}", exc_info=True)
        return 0

async def mark_files_as_queued_sqlite(
    backup_set_db_id: int,
    file_paths: List[str]
):
    """SQLite 版本：标记文件为已入队（仅设置 is_copy_success = TRUE）"""
    logger.info(f"[SQLite模式] 开始标记 {len(file_paths)} 个文件为已入队（is_copy_success = TRUE）")
    
    if not file_paths:
        logger.warning("[SQLite模式] ❌ 没有可更新的文件，跳过 mark_files_as_queued")
        return
    
    try:
        from utils.scheduler.sqlite_utils import get_sqlite_connection
        async with get_sqlite_connection() as conn:
            # 分批更新，避免单次更新过多文件
            batch_size = 1000
            total_updated = 0
            
            for i in range(0, len(file_paths), batch_size):
                batch_paths = file_paths[i:i + batch_size]
                placeholders = ','.join(['?' for _ in batch_paths])
                
                cursor = await conn.execute(
                    f"""
                    UPDATE backup_files
                    SET is_copy_success = 1,
                        copy_status_at = ?,
                        updated_at = ?
                    WHERE backup_set_id = ? 
                      AND file_path IN ({placeholders})
                      AND (is_copy_success = 0 OR is_copy_success IS NULL)
                    """,
                    (datetime.now(), datetime.now(), backup_set_db_id) + tuple(batch_paths)
                )
                updated_count = cursor.rowcount
                total_updated += updated_count
            
            logger.info(f"[SQLite模式] ✅ 已更新 {total_updated} 个文件的 is_copy_success 状态")
    except Exception as e:
        logger.error(f"[SQLite模式] ❌ 标记文件为已入队失败: {str(e)}", exc_info=True)
        raise

async def mark_files_as_copied_sqlite(
    backup_set_db_id: int,
    processed_files: List[Dict],
    compressed_file: Dict,
    tape_file_path: Optional[str],
    chunk_number: int,
    backup_time: Optional[datetime] = None
):
    """SQLite 版本：标记压缩完成的文件，避免重复压缩/误操作"""
    logger.info(f"[mark_files_as_copied_sqlite] ========== 开始标记文件为复制成功 ==========")
    logger.info(f"[mark_files_as_copied_sqlite] 参数: backup_set_db_id={backup_set_db_id}, 文件数={len(processed_files)}, chunk_number={chunk_number}")
    
    if not processed_files:
        logger.warning("[mark_files_as_copied_sqlite] ❌ 没有可更新的文件，跳过 mark_files_as_copied")
        return

    try:
        backup_time = backup_time or datetime.now()
        per_file_compressed_size = int(
            (compressed_file.get("compressed_size") or 0) / max(len(processed_files), 1)
        )
        is_compressed = bool(compressed_file.get("compression_enabled", True))
        checksum = compressed_file.get("checksum")
        copy_time = datetime.now()

        file_paths: List[str] = []
        for item in processed_files:
            path_value = item.get("file_path") or item.get("path")
            if path_value:
                file_paths.append(path_value)

        logger.info(f"[mark_files_as_copied_sqlite] 提取到 {len(file_paths)} 个文件路径（总文件数={len(processed_files)}）")
        if len(file_paths) < len(processed_files):
            logger.warning(f"[mark_files_as_copied_sqlite] ⚠️ 部分文件缺少路径: 有路径={len(file_paths)}, 总文件数={len(processed_files)}")
            # 显示前几个缺少路径的文件
            missing_paths = [item for item in processed_files[:5] if not (item.get("file_path") or item.get("path"))]
            if missing_paths:
                logger.warning(f"[mark_files_as_copied_sqlite] 缺少路径的文件示例: {missing_paths}")

        if not file_paths:
            logger.error(f"[mark_files_as_copied_sqlite] ❌ 处理中缺少 file_path，无法更新 is_copy_success！processed_files示例: {processed_files[:3] if processed_files else '空'}")
            return

        async with get_sqlite_connection() as conn:
            logger.info(f"[mark_files_as_copied_sqlite] 开始查询数据库中已存在的文件（backup_set_id={backup_set_db_id}）")
            existing_map: Dict[str, tuple] = {}  # {file_path: (id, ...)}
            chunk_size = 400  # SQLite 默认参数上限999，控制在安全范围内
            total_chunks = (len(file_paths) + chunk_size - 1) // chunk_size
            logger.info(f"[mark_files_as_copied_sqlite] 将分 {total_chunks} 批查询（每批最多 {chunk_size} 个文件）")
            
            for i in range(0, len(file_paths), chunk_size):
                chunk = file_paths[i:i + chunk_size]
                chunk_num = i // chunk_size + 1
                # 构建 IN 查询
                placeholders = ','.join(['?' for _ in chunk])
                logger.debug(f"[mark_files_as_copied_sqlite] 查询第 {chunk_num}/{total_chunks} 批: {len(chunk)} 个文件")
                cursor = await conn.execute(f"""
                    SELECT id, backup_set_id, file_path, file_name, directory_path, display_name,
                           file_type, file_size, compressed_size, file_permissions, file_owner,
                           file_group, created_time, modified_time, accessed_time, tape_block_start,
                           tape_block_count, compressed, encrypted, checksum, is_copy_success,
                           copy_status_at, backup_time, chunk_number, version, file_metadata, tags
                    FROM backup_files
                    WHERE backup_set_id = ? AND file_path IN ({placeholders})
                """, (backup_set_db_id,) + tuple(chunk))
                rows = await cursor.fetchall()
                logger.debug(f"[mark_files_as_copied_sqlite] 第 {chunk_num} 批查询结果: 找到 {len(rows)} 个已存在的文件")
                for row in rows:
                    existing_map[row[2]] = row  # file_path 是第3个字段（索引2）
            
            logger.info(f"[mark_files_as_copied_sqlite] 查询完成: 共找到 {len(existing_map)} 个已存在的文件（需要更新），{len(file_paths) - len(existing_map)} 个新文件（需要插入）")

            # 准备批量更新的数据
            update_params = []
            insert_params = []
            skipped = 0
            
            logger.info(f"[mark_files_as_copied_sqlite] 开始准备批量更新/插入数据: {len(processed_files)} 个文件")

            for idx, processed_file in enumerate(processed_files):
                file_path = processed_file.get("file_path") or processed_file.get("path")
                if not file_path:
                    skipped += 1
                    continue

                file_name = processed_file.get("file_name") or Path(file_path).name
                display_name = processed_file.get("display_name") or file_name
                directory_path = processed_file.get("directory_path") or str(Path(file_path).parent)
                file_permissions = processed_file.get("file_permissions") or processed_file.get("permissions")
                file_owner = processed_file.get("file_owner")
                file_group = processed_file.get("file_group")
                file_size = processed_file.get("file_size")
                if file_size is None:
                    file_size = processed_file.get("size") or 0

                created_time = processed_file.get("created_time")
                modified_time = processed_file.get("modified_time")
                accessed_time = processed_file.get("accessed_time")
                file_stat = processed_file.get("file_stat")

                created_time = _parse_datetime_value(created_time) or _datetime_from_stat(file_stat, "st_ctime")
                modified_time = _parse_datetime_value(modified_time) or _datetime_from_stat(file_stat, "st_mtime")
                accessed_time = _parse_datetime_value(accessed_time) or _datetime_from_stat(file_stat, "st_atime")

                tape_block_start = processed_file.get("tape_block_start")
                tape_block_count = processed_file.get("tape_block_count")

                metadata = _ensure_metadata_dict(processed_file.get("file_metadata"))
                metadata["tape_file_path"] = tape_file_path
                metadata["chunk_number"] = chunk_number
                metadata.setdefault("original_path", file_path)
                metadata_json = json.dumps(metadata) if metadata else None

                file_type_enum = _normalize_file_type(processed_file)
                file_type_value = file_type_enum.value if hasattr(file_type_enum, 'value') else str(file_type_enum)

                if file_path in existing_map:
                    # 准备更新参数
                    file_id = existing_map[file_path][0]  # id 是第一个字段
                    existing_row = existing_map[file_path]
                    existing_is_copy_success = existing_row[20]  # is_copy_success 是第21个字段（索引20）
                    
                    # 保留原有值（如果新值为 None）
                    final_created_time = created_time or _parse_datetime_value(existing_row[12])
                    final_modified_time = modified_time or _parse_datetime_value(existing_row[13])
                    final_accessed_time = accessed_time or _parse_datetime_value(existing_row[14])
                    final_tape_block_start = tape_block_start if tape_block_start is not None else existing_row[15]
                    final_tape_block_count = tape_block_count if tape_block_count is not None else existing_row[16]
                    
                    update_params.append((
                        file_name, display_name, directory_path, file_type_value,
                        file_size, per_file_compressed_size, file_permissions, file_owner,
                        file_group, final_created_time, final_modified_time, final_accessed_time,
                        final_tape_block_start, final_tape_block_count, 1 if is_compressed else 0, checksum,
                        backup_time, chunk_number, metadata_json, 1,  # is_copy_success = 1 (True)
                        copy_time, datetime.now(), file_id
                    ))
                else:
                    # 准备插入参数
                    insert_params.append((
                        backup_set_db_id, file_path, file_name, directory_path, display_name,
                        file_type_value, file_size, per_file_compressed_size, file_permissions, file_owner,
                        file_group, created_time, modified_time, accessed_time, tape_block_start,
                        tape_block_count, 1 if is_compressed else 0, 0, checksum, 1,  # is_copy_success = 1 (True)
                        copy_time, backup_time, chunk_number, 1, metadata_json, json.dumps({'status': 'compressed'}),
                        datetime.now(), datetime.now()
                    ))

            # 批量更新
            success_updates = 0
            if update_params:
                logger.info(f"[mark_files_as_copied_sqlite] 开始批量更新 {len(update_params)} 个文件")
                await conn.executemany("""
                    UPDATE backup_files
                    SET file_name = ?, display_name = ?, directory_path = ?, file_type = ?,
                        file_size = ?, compressed_size = ?, file_permissions = ?, file_owner = ?,
                        file_group = ?, created_time = ?, modified_time = ?, accessed_time = ?,
                        tape_block_start = ?, tape_block_count = ?, compressed = ?, checksum = ?,
                        backup_time = ?, chunk_number = ?, file_metadata = ?, is_copy_success = ?,
                        copy_status_at = ?, updated_at = ?
                    WHERE id = ?
                """, update_params)
                success_updates = len(update_params)
                logger.info(f"[mark_files_as_copied_sqlite] ✅ 批量更新完成: {success_updates} 个文件")

            # 批量插入
            success_inserts = 0
            if insert_params:
                logger.info(f"[mark_files_as_copied_sqlite] 开始批量插入 {len(insert_params)} 个文件")
                await conn.executemany("""
                    INSERT INTO backup_files (
                        backup_set_id, file_path, file_name, directory_path, display_name,
                        file_type, file_size, compressed_size, file_permissions, file_owner,
                        file_group, created_time, modified_time, accessed_time, tape_block_start,
                        tape_block_count, compressed, encrypted, checksum, is_copy_success,
                        copy_status_at, backup_time, chunk_number, version, file_metadata, tags,
                        created_at, updated_at
                    ) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
                """, insert_params)
                success_inserts = len(insert_params)
                logger.info(f"[mark_files_as_copied_sqlite] ✅ 批量插入完成: {success_inserts} 个文件")

            logger.info(f"[mark_files_as_copied_sqlite] 准备提交事务: 更新={success_updates}, 插入={success_inserts}, 跳过={skipped}")
            await conn.commit()
            logger.info(f"[mark_files_as_copied_sqlite] ✅ 事务已提交")

            if success_updates or success_inserts:
                logger.info(
                    f"[mark_files_as_copied_sqlite] ✅ 已更新 {success_updates} 个文件、插入 {success_inserts} 个文件的压缩状态 "
                    f"(is_copy_success=1, backup_set_id={backup_set_db_id}, chunk_number={chunk_number})"
                )
                # 验证更新是否成功
                if file_paths:
                    sample_size = min(10, len(file_paths))
                    sample_paths = file_paths[:sample_size]
                    placeholders = ','.join(['?' for _ in sample_paths])
                    verify_cursor = await conn.execute(f"""
                        SELECT COUNT(*) FROM backup_files 
                        WHERE backup_set_id = ? AND file_path IN ({placeholders}) AND is_copy_success = 1
                    """, (backup_set_db_id,) + tuple(sample_paths))
                    verify_row = await verify_cursor.fetchone()
                    verified_count = verify_row[0] if verify_row else 0
                    logger.info(f"[mark_files_as_copied_sqlite] ✅ 验证更新结果: 前{sample_size}个文件中，{verified_count} 个文件的 is_copy_success=1")
                    
                    # 如果验证失败，显示详细信息
                    if verified_count < sample_size:
                        logger.warning(f"[mark_files_as_copied_sqlite] ⚠️ 验证失败: 期望 {sample_size} 个文件 is_copy_success=1，实际只有 {verified_count} 个")
                        # 查询每个文件的状态
                        detail_cursor = await conn.execute(f"""
                            SELECT file_path, is_copy_success FROM backup_files 
                            WHERE backup_set_id = ? AND file_path IN ({placeholders})
                        """, (backup_set_db_id,) + tuple(sample_paths))
                        detail_rows = await detail_cursor.fetchall()
                        for detail_row in detail_rows:
                            logger.warning(f"[mark_files_as_copied_sqlite] 文件状态: {detail_row[0][:100]} -> is_copy_success={detail_row[1]}")
            else:
                logger.error(f"[mark_files_as_copied_sqlite] ❌ 没有任何文件的 is_copy_success 状态被更新 (backup_set_id={backup_set_db_id}, 文件数={len(processed_files)}, 跳过={skipped})")

            if skipped:
                logger.warning(f"[SQLite] 有 {skipped} 个文件缺少 file_path，已跳过")

    except Exception as e:
        logger.error(f"SQLite 更新文件压缩状态失败: {str(e)}", exc_info=True)
        raise


async def get_backup_set_by_set_id_sqlite(set_id: str) -> Optional[BackupSet]:
    """根据 set_id 获取备份集（SQLite 版本）"""
    try:
        if not set_id:
            return None
        
        async with get_sqlite_connection() as conn:
            cursor = await conn.execute("""
                SELECT id, set_id, set_name, backup_group, status, backup_task_id, tape_id,
                       backup_type, backup_time, source_info, retention_until, created_at, updated_at
                FROM backup_sets
                WHERE set_id = ?
            """, (set_id,))
            row = await cursor.fetchone()
            
            if not row:
                return None
            
            # 创建 BackupSet 对象返回
            backup_set = BackupSet(
                id=row[0],
                set_id=row[1],
                set_name=row[2],
                backup_group=row[3],
                status=BackupSetStatus(row[4]) if isinstance(row[4], str) else row[4],
                backup_task_id=row[5],
                tape_id=row[6],
                backup_type=BackupTaskType(row[7]) if isinstance(row[7], str) else row[7],
                backup_time=row[8],
                source_info=json.loads(row[9]) if row[9] else None,
                retention_until=row[10]
            )
            return backup_set
            
    except Exception as e:
        logger.error(f"获取备份集失败: {str(e)}", exc_info=True)
        return None

