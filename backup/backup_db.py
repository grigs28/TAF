#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
数据库操作模块
Database Operations Module
"""

import asyncio
import logging
import json
import re
from datetime import datetime, timedelta
from pathlib import Path
from typing import List, Dict, Optional

from models.backup import BackupTask, BackupSet, BackupFile, BackupTaskStatus, BackupFileType
from utils.datetime_utils import now, format_datetime

logger = logging.getLogger(__name__)


class BackupDB:
    """备份数据库操作类"""
    
    def __init__(self):
        """初始化数据库操作类"""
        pass
    
    async def create_backup_set(self, backup_task: BackupTask, tape) -> BackupSet:
        """创建备份集
        
        Args:
            backup_task: 备份任务对象
            tape: 磁带对象
            
        Returns:
            BackupSet: 备份集对象
        """
        try:
            # 生成备份集ID
            backup_group = format_datetime(now(), '%Y-%m')
            set_id = f"{backup_group}_{backup_task.id:06d}"
            backup_time = datetime.now()
            retention_until = backup_time + timedelta(days=backup_task.retention_days)
            
            # 使用原生 openGauss SQL，避免 SQLAlchemy 版本解析
            from utils.scheduler.db_utils import is_opengauss, get_opengauss_connection
            
            if is_opengauss():
                conn = await get_opengauss_connection()
                try:
                    # 准备 source_info JSON
                    source_info_json = json.dumps({'paths': backup_task.source_paths}) if backup_task.source_paths else None
                    
                    # 插入备份集
                    await conn.execute(
                        """
                        INSERT INTO backup_sets 
                        (set_id, set_name, backup_group, status, backup_task_id, tape_id,
                         backup_type, backup_time, source_info, retention_until, auto_delete,
                         created_at, updated_at)
                        VALUES ($1, $2, $3, $4::backupsetstatus, $5, $6, $7::backuptasktype, $8, $9::json, $10, $11, $12, $13)
                        RETURNING id
                        """,
                        set_id,
                        f"{backup_task.task_name}_{set_id}",
                        backup_group,
                        'ACTIVE',  # BackupSetStatus.ACTIVE
                        backup_task.id,
                        tape.tape_id,
                        backup_task.task_type.value,  # BackupTaskType enum value
                        backup_time,
                        source_info_json,
                        retention_until,
                        True,  # auto_delete
                        backup_time,  # created_at
                        backup_time   # updated_at
                    )
                    
                    # 查询插入的记录
                    row = await conn.fetchrow(
                        """
                        SELECT id, set_id, set_name, backup_group, status, backup_task_id, tape_id,
                               backup_type, backup_time, source_info, retention_until, created_at, updated_at
                        FROM backup_sets
                        WHERE set_id = $1
                        """,
                        set_id
                    )
                    
                    if row:
                        # 创建 BackupSet 对象（用于返回）
                        backup_set = BackupSet(
                            id=row['id'],
                            set_id=row['set_id'],
                            set_name=row['set_name'],
                            backup_group=row['backup_group'],
                            status=row['status'],
                            backup_task_id=row['backup_task_id'],
                            tape_id=row['tape_id'],
                            backup_type=backup_task.task_type,
                            backup_time=row['backup_time'],
                            source_info={'paths': backup_task.source_paths},
                            retention_until=row['retention_until']
                        )
                    else:
                        raise RuntimeError(f"备份集插入后查询失败: {set_id}")
                finally:
                    await conn.close()
            else:
                # 非 openGauss 使用 SQLAlchemy（其他数据库）- 但当前项目只支持 openGauss
                raise RuntimeError("当前项目仅支持 openGauss 数据库")

            backup_task.backup_set_id = set_id
            logger.info(f"创建备份集: {set_id}")

            return backup_set

        except Exception as e:
            logger.error(f"创建备份集失败: {str(e)}")
            raise
    
    async def finalize_backup_set(self, backup_set: BackupSet, file_count: int, total_size: int):
        """完成备份集
        
        Args:
            backup_set: 备份集对象
            file_count: 文件数量
            total_size: 总大小
        """
        try:
            backup_set.total_files = file_count
            backup_set.total_bytes = total_size
            backup_set.compressed_bytes = total_size
            backup_set.compression_ratio = total_size / backup_set.total_bytes if backup_set.total_bytes > 0 else 1.0
            backup_set.chunk_count = 1  # 简化处理

            # 保存更新 - 使用原生 openGauss SQL
            from utils.scheduler.db_utils import is_opengauss, get_opengauss_connection
            
            if is_opengauss():
                conn = await get_opengauss_connection()
                try:
                    await conn.execute(
                        """
                        UPDATE backup_sets
                        SET total_files = $1,
                            total_bytes = $2,
                            compressed_bytes = $3,
                            compression_ratio = $4,
                            chunk_count = $5,
                            updated_at = $6
                        WHERE set_id = $7
                        """,
                        file_count,
                        total_size,
                        total_size,
                        backup_set.compression_ratio,
                        backup_set.chunk_count,
                        datetime.now(),
                        backup_set.set_id
                    )
                finally:
                    await conn.close()
            else:
                # 非 openGauss 使用 SQLAlchemy
                from config.database import get_db
                async for db in get_db():
                    await db.commit()

            logger.info(f"备份集完成: {backup_set.set_id}")

        except Exception as e:
            logger.error(f"完成备份集失败: {str(e)}")
    
    async def save_backup_files_to_db(
        self, 
        file_group: List[Dict], 
        backup_set: BackupSet, 
        compressed_file: Dict, 
        tape_file_path: str, 
        chunk_number: int
    ):
        """保存备份文件信息到数据库（便于恢复）
        
        注意：如果数据库保存失败，不会中断备份流程，只会记录警告日志。
        
        Args:
            file_group: 文件组列表
            backup_set: 备份集对象
            compressed_file: 压缩文件信息字典
            tape_file_path: 磁带文件路径
            chunk_number: 块编号
        """
        try:
            from utils.scheduler.db_utils import is_opengauss, get_opengauss_connection
            
            if not is_opengauss():
                logger.warning("非openGauss数据库，跳过保存备份文件信息")
                return
            
            conn = await get_opengauss_connection()
            try:
                # 获取备份集的数据库ID
                backup_set_row = await conn.fetchrow(
                    """
                    SELECT id FROM backup_sets WHERE set_id = $1
                    """,
                    backup_set.set_id
                )
                
                if not backup_set_row:
                    logger.warning(f"找不到备份集: {backup_set.set_id}，跳过保存文件信息")
                    return
                
                backup_set_db_id = backup_set_row['id']
                backup_time = datetime.now()
                
                # 在线程池中批量处理文件信息，避免阻塞事件循环
                loop = asyncio.get_event_loop()
                
                def _process_file_info(file_info):
                    """在线程中处理单个文件信息"""
                    try:
                        file_path = Path(file_info['path'])
                        
                        # 确定文件类型（使用枚举值）
                        if file_info.get('is_dir', False):
                            file_type = BackupFileType.DIRECTORY.value
                        elif file_info.get('is_symlink', False):
                            if hasattr(BackupFileType, 'SYMLINK'):
                                file_type = BackupFileType.SYMLINK.value
                            else:
                                file_type = BackupFileType.FILE.value
                        else:
                            file_type = BackupFileType.FILE.value
                        
                        # 获取文件元数据（同步操作，在线程中执行）
                        try:
                            file_stat = file_path.stat() if file_path.exists() else None
                        except (PermissionError, OSError, FileNotFoundError, IOError) as stat_error:
                            logger.debug(f"无法获取文件统计信息: {file_path} (错误: {str(stat_error)})")
                            file_stat = None
                        except Exception as stat_error:
                            logger.warning(f"获取文件统计信息失败: {file_path} (错误: {str(stat_error)})")
                            file_stat = None
                        
                        # 跳过单个文件的校验和计算（文件已压缩，压缩包本身有校验和，避免阻塞）
                        file_checksum = None
                        
                        # 获取文件权限（Windows上可能不可用）
                        file_permissions = None
                        if file_stat:
                            try:
                                file_permissions = oct(file_stat.st_mode)[-3:]
                            except:
                                pass
                        
                        return {
                            'file_path': str(file_path),
                            'file_name': file_path.name,
                            'file_type': file_type,
                            'file_size': file_info.get('size', 0),
                            'file_stat': file_stat,
                            'file_permissions': file_permissions,
                            'file_checksum': file_checksum
                        }
                    except Exception as process_error:
                        logger.warning(f"处理文件信息失败: {file_info.get('path', 'unknown')} (错误: {str(process_error)})")
                        return None
                
                # 批量处理文件信息（在线程池中执行）
                processed_files = await asyncio.gather(*[
                    loop.run_in_executor(None, _process_file_info, file_info)
                    for file_info in file_group
                ], return_exceptions=True)
                
                # 过滤掉处理失败的文件（返回None或异常）
                valid_processed_files = [f for f in processed_files if f is not None and not isinstance(f, Exception)]
                
                if len(valid_processed_files) < len(file_group):
                    failed_count = len(file_group) - len(valid_processed_files)
                    logger.warning(f"⚠️ 处理文件信息时，{failed_count} 个文件失败，继续保存其他文件")
                
                # 批量插入文件记录（使用事务）
                success_count = 0
                failed_count = 0
                for processed_file in valid_processed_files:
                    try:
                        file_stat = processed_file['file_stat']
                        await conn.execute(
                            """
                            INSERT INTO backup_files (
                                backup_set_id, file_path, file_name, file_type, file_size,
                                compressed_size, file_permissions, created_time, modified_time,
                                accessed_time, compressed, checksum, backup_time, chunk_number,
                                tape_block_start, file_metadata
                            ) VALUES (
                                $1, $2, $3, $4::backupfiletype, $5, $6, $7, $8, $9, $10, $11, $12, $13, $14, $15, $16::json
                            )
                            """,
                            backup_set_db_id,
                            processed_file['file_path'],
                            processed_file['file_name'],
                            processed_file['file_type'],
                            processed_file['file_size'],
                            compressed_file.get('compressed_size', 0) // len(file_group) if file_group else 0,  # 平均分配压缩后大小
                            processed_file['file_permissions'],
                            datetime.fromtimestamp(file_stat.st_ctime) if file_stat else None,
                            datetime.fromtimestamp(file_stat.st_mtime) if file_stat else None,
                            datetime.fromtimestamp(file_stat.st_atime) if file_stat else None,
                            compressed_file.get('compression_enabled', False),
                            processed_file['file_checksum'],
                            backup_time,
                            chunk_number,
                            0,  # tape_block_start（文件系统操作，暂时设为0）
                            json.dumps({
                                'tape_file_path': tape_file_path,
                                'chunk_number': chunk_number,
                                'original_path': processed_file['file_path'],
                                'relative_path': str(Path(processed_file['file_path']).relative_to(Path(processed_file['file_path']).anchor)) if Path(processed_file['file_path']).is_absolute() else processed_file['file_path']
                            })  # file_metadata 需要序列化为 JSON 字符串
                        )
                        success_count += 1
                    except Exception as insert_error:
                        failed_count += 1
                        logger.warning(f"⚠️ 插入文件记录失败: {processed_file.get('file_path', 'unknown')} (错误: {str(insert_error)})")
                        continue
                
                if success_count > 0:
                    logger.debug(f"已保存 {success_count} 个文件信息到数据库（chunk {chunk_number}）")
                if failed_count > 0:
                    logger.warning(f"⚠️ 保存文件信息到数据库时，{failed_count} 个文件失败，但备份流程继续")
                    
            except Exception as db_conn_error:
                logger.warning(f"⚠️ 数据库连接或查询失败，跳过保存文件信息: {str(db_conn_error)}")
                # 数据库错误不影响备份流程，继续执行
            finally:
                try:
                    await conn.close()
                except:
                    pass
                    
        except Exception as e:
            logger.warning(f"⚠️ 保存备份文件信息到数据库失败: {str(e)}，但备份流程继续")
            import traceback
            logger.debug(f"错误堆栈:\n{traceback.format_exc()}")
            # 不抛出异常，因为文件已经写入磁带，数据库记录失败不应该影响备份流程
    
    async def update_scan_progress(
        self, 
        backup_task: BackupTask, 
        scanned_count: int, 
        valid_count: int, 
        operation_status: str = None
    ):
        """更新扫描进度到数据库
        
        Args:
            backup_task: 备份任务对象
            scanned_count: 已处理文件数（processed_files）
            valid_count: 压缩包数量（total_files），仅在压缩/写入阶段使用；扫描阶段传入的值会被忽略，使用 backup_task.total_files
            operation_status: 操作状态（如"[扫描文件中...]"、"[压缩文件中...]"等）
        """
        try:
            if not backup_task or not backup_task.id:
                return
            
            from utils.scheduler.db_utils import is_opengauss, get_opengauss_connection
            if is_opengauss():
                conn = await get_opengauss_connection()
                try:
                    # 重要：不再更新 total_files 字段（压缩包数量）
                    # total_files 字段由独立的后台扫描任务 _scan_for_progress_update 负责更新（总文件数）
                    # 压缩包数量存储在 result_summary.estimated_archive_count 中
                    
                    if operation_status:
                        # 先获取当前description、total_files、total_bytes和result_summary
                        row = await conn.fetchrow(
                            "SELECT description, total_files, total_bytes, result_summary FROM backup_tasks WHERE id = $1",
                            backup_task.id
                        )
                        current_desc = row['description'] if row and row['description'] else ''
                        # 从数据库读取当前的 total_files 值（总文件数），保持不变
                        total_files_value = row['total_files'] if row and row['total_files'] else 0
                        # 从数据库读取当前的 total_bytes 值（总字节数），保持不变
                        total_bytes_from_db = row['total_bytes'] if row and row['total_bytes'] else 0
                        # 保持数据库中的 result_summary 不变（由后台扫描任务更新）
                        result_summary = row['result_summary'] if row and row['result_summary'] else {}
                        if isinstance(result_summary, str):
                            try:
                                result_summary = json.loads(result_summary)
                            except json.JSONDecodeError:
                                result_summary = {}
                        elif not isinstance(result_summary, dict):
                            result_summary = {}
                        
                        # 移除所有操作状态标记（保留格式化状态）
                        # 移除所有 [操作状态...] 格式的标记，但保留 [格式化中]
                        if '[格式化中]' in current_desc:
                            # 保留格式化状态，移除其他操作状态
                            cleaned_desc = re.sub(r'\[(?!格式化中)[^\]]+\.\.\.\]', '', current_desc)
                            cleaned_desc = cleaned_desc.replace('  ', ' ').strip()
                            new_desc = cleaned_desc + ' ' + operation_status if cleaned_desc else operation_status
                        else:
                            # 移除所有操作状态标记
                            cleaned_desc = re.sub(r'\[[^\]]+\.\.\.\]', '', current_desc)
                            cleaned_desc = cleaned_desc.replace('  ', ' ').strip()
                            new_desc = cleaned_desc + ' ' + operation_status if cleaned_desc else operation_status
                        
                        # 获取compressed_bytes和processed_bytes用于更新
                        compressed_bytes = getattr(backup_task, 'compressed_bytes', 0) or 0
                        processed_bytes = getattr(backup_task, 'processed_bytes', 0) or 0
                        
                        # 重要：不从 backup_task 对象读取 total_bytes 和 total_bytes_actual
                        # 这些字段由独立的后台扫描任务 _scan_for_progress_update 负责更新
                        # 压缩流程只更新 processed_files 和 processed_bytes，不更新总文件数和总字节数
                        
                        # 只更新 result_summary 中的 estimated_archive_count（如果备份任务对象中有）
                        if hasattr(backup_task, 'result_summary') and backup_task.result_summary:
                            if isinstance(backup_task.result_summary, dict):
                                if 'estimated_archive_count' in backup_task.result_summary:
                                    result_summary['estimated_archive_count'] = backup_task.result_summary['estimated_archive_count']
                        
                        # 将result_summary转换为JSON字符串
                        result_summary_json = json.dumps(result_summary) if result_summary else None
                        
                        await conn.execute(
                            """
                            UPDATE backup_tasks
                            SET progress_percent = $1,
                                processed_files = $2,
                                total_files = $3,
                                processed_bytes = $4,
                                compressed_bytes = $5,
                                result_summary = $6::json,
                                description = $7,
                                updated_at = $8
                            WHERE id = $9
                            """,
                            backup_task.progress_percent,
                            scanned_count,  # processed_files: 已处理文件数
                            total_files_value,  # total_files: 压缩包数量
                            processed_bytes,  # processed_bytes: 已处理字节数
                            compressed_bytes,  # compressed_bytes: 压缩后字节数
                            result_summary_json,
                            new_desc,
                            datetime.now(),
                            backup_task.id
                        )
                        # 注意：total_bytes 字段不更新，由后台扫描任务负责更新
                    else:
                        # 没有操作状态，只更新进度和字节数
                        # 从数据库读取当前的 total_files、total_bytes 和 result_summary，保持不变
                        current_row = await conn.fetchrow(
                            "SELECT total_files, total_bytes, result_summary FROM backup_tasks WHERE id = $1",
                            backup_task.id
                        )
                        # 从数据库读取当前的 total_files 值（总文件数），保持不变
                        total_files_value = current_row['total_files'] if current_row and current_row['total_files'] else 0
                        # 从数据库读取当前的 total_bytes 值（总字节数），保持不变（不更新）
                        total_bytes_from_db = current_row['total_bytes'] if current_row and current_row['total_bytes'] else 0
                        # 保持数据库中的 result_summary 不变（由后台扫描任务更新）
                        result_summary = current_row['result_summary'] if current_row and current_row['result_summary'] else {}
                        if isinstance(result_summary, str):
                            try:
                                result_summary = json.loads(result_summary)
                            except json.JSONDecodeError:
                                result_summary = {}
                        elif not isinstance(result_summary, dict):
                            result_summary = {}
                        
                        # 获取compressed_bytes和processed_bytes用于更新
                        compressed_bytes = getattr(backup_task, 'compressed_bytes', 0) or 0
                        processed_bytes = getattr(backup_task, 'processed_bytes', 0) or 0
                        
                        # 重要：不从 backup_task 对象读取 total_bytes 和 total_bytes_actual
                        # 这些字段由独立的后台扫描任务 _scan_for_progress_update 负责更新
                        # 压缩流程只更新 processed_files 和 processed_bytes，不更新总文件数和总字节数
                        
                        # 只更新 result_summary 中的 estimated_archive_count（如果备份任务对象中有）
                        if hasattr(backup_task, 'result_summary') and backup_task.result_summary:
                            if isinstance(backup_task.result_summary, dict):
                                if 'estimated_archive_count' in backup_task.result_summary:
                                    result_summary['estimated_archive_count'] = backup_task.result_summary['estimated_archive_count']
                        
                        # 将result_summary转换为JSON字符串
                        result_summary_json = json.dumps(result_summary) if result_summary else None
                        
                        await conn.execute(
                            """
                            UPDATE backup_tasks
                            SET progress_percent = $1,
                                processed_files = $2,
                                total_files = $3,
                                processed_bytes = $4,
                                compressed_bytes = $5,
                                result_summary = $6::json,
                                updated_at = $7
                            WHERE id = $8
                            """,
                            backup_task.progress_percent,
                            scanned_count,  # processed_files: 已处理文件数
                            total_files_value,  # total_files: 总文件数（从数据库读取，保持不变）
                            processed_bytes,  # processed_bytes: 已处理字节数
                            compressed_bytes,  # compressed_bytes: 压缩后字节数
                            result_summary_json,
                            datetime.now(),
                            backup_task.id
                        )
                        # 注意：total_bytes 字段不更新，由后台扫描任务负责更新
                finally:
                    await conn.close()
            else:
                # 非 openGauss 使用 SQLAlchemy（但当前项目仅支持 openGauss）
                logger.warning("非 openGauss 数据库，跳过进度更新")
        except Exception as e:
            logger.debug(f"更新扫描进度失败（忽略继续）: {str(e)}")
    
    async def update_task_status(self, backup_task: BackupTask, status: BackupTaskStatus):
        """更新任务状态
        
        Args:
            backup_task: 备份任务对象
            status: 任务状态
        """
        try:
            backup_task.status = status
            
            # 根据状态更新 started_at 和 completed_at
            current_time = datetime.now()
            update_fields = ['status', 'updated_at']
            update_values = [status.value, current_time]
            
            if status == BackupTaskStatus.RUNNING:
                # 运行状态：更新 started_at
                update_fields.append('started_at')
                update_values.append(current_time)
                
                # 同时更新 source_paths 和 tape_id（如果有的话），以便任务卡片正确显示
                if hasattr(backup_task, 'source_paths') and backup_task.source_paths:
                    update_fields.append('source_paths')
                    update_values.append(json.dumps(backup_task.source_paths) if isinstance(backup_task.source_paths, list) else backup_task.source_paths)
                
                if hasattr(backup_task, 'tape_id') and backup_task.tape_id:
                    update_fields.append('tape_id')
                    update_values.append(backup_task.tape_id)
            elif status in (BackupTaskStatus.COMPLETED, BackupTaskStatus.FAILED, BackupTaskStatus.CANCELLED):
                # 完成/失败/取消状态：更新 completed_at
                update_fields.append('completed_at')
                update_values.append(current_time)
            
            # 使用原生 openGauss SQL
            from utils.scheduler.db_utils import is_opengauss, get_opengauss_connection
            
            if is_opengauss():
                conn = await get_opengauss_connection()
                try:
                    # 动态构建UPDATE语句
                    set_clauses = []
                    params = []
                    param_index = 1
                    
                    for field in update_fields:
                        if field == 'status':
                            set_clauses.append(f"status = ${param_index}::backuptaskstatus")
                        elif field == 'source_paths':
                            set_clauses.append(f"source_paths = ${param_index}::json")
                        else:
                            set_clauses.append(f"{field} = ${param_index}")
                        params.append(update_values[update_fields.index(field)])
                        param_index += 1
                    
                    params.append(backup_task.id)
                    
                    await conn.execute(
                        f"""
                        UPDATE backup_tasks
                        SET {', '.join(set_clauses)}
                        WHERE id = ${param_index}
                        """,
                        *params
                    )
                finally:
                    await conn.close()
            else:
                # 非 openGauss 使用 SQLAlchemy
                from config.database import get_db
                async for db in get_db():
                    await db.commit()
        except Exception as e:
            logger.error(f"更新任务状态失败: {str(e)}")
    
    async def update_task_fields(self, backup_task: BackupTask, **fields):
        """更新任务的特定字段
        
        Args:
            backup_task: 备份任务对象
            **fields: 要更新的字段（键值对）
        """
        try:
            if not fields:
                return
            
            # 更新对象属性
            for field, value in fields.items():
                if hasattr(backup_task, field):
                    setattr(backup_task, field, value)
            
            # 使用原生 openGauss SQL
            from utils.scheduler.db_utils import is_opengauss, get_opengauss_connection
            
            if is_opengauss():
                conn = await get_opengauss_connection()
                try:
                    # 动态构建UPDATE语句
                    set_clauses = []
                    params = []
                    param_index = 1
                    
                    for field, value in fields.items():
                        if field == 'status':
                            set_clauses.append(f"status = ${param_index}::backuptaskstatus")
                        elif field == 'source_paths':
                            set_clauses.append(f"source_paths = ${param_index}::json")
                        else:
                            set_clauses.append(f"{field} = ${param_index}")
                        
                        # 处理 JSON 字段
                        if field == 'source_paths' and isinstance(value, list):
                            params.append(json.dumps(value))
                        else:
                            params.append(value)
                        param_index += 1
                    
                    params.append(backup_task.id)
                    
                    await conn.execute(
                        f"""
                        UPDATE backup_tasks
                        SET {', '.join(set_clauses)}, updated_at = CURRENT_TIMESTAMP
                        WHERE id = ${param_index}
                        """,
                        *params
                    )
                finally:
                    await conn.close()
            else:
                # 非 openGauss 使用 SQLAlchemy
                from config.database import get_db
                async for db in get_db():
                    await db.commit()
        except Exception as e:
            logger.error(f"更新任务字段失败: {str(e)}")
    
    async def get_task_status(self, task_id: int) -> Optional[Dict]:
        """获取任务状态
        
        Args:
            task_id: 任务ID
            
        Returns:
            任务状态字典，如果不存在则返回None
        """
        try:
            # 使用原生 openGauss SQL
            from utils.scheduler.db_utils import is_opengauss, get_opengauss_connection
            
            if is_opengauss():
                conn = await get_opengauss_connection()
                try:
                    row = await conn.fetchrow(
                        """
                        SELECT id, status, progress_percent, processed_files, total_files,
                               processed_bytes, total_bytes, compressed_bytes, description,
                               source_paths, tape_device, tape_id, result_summary, started_at, completed_at
                        FROM backup_tasks
                        WHERE id = $1
                        """,
                        task_id
                    )
                    
                    if row:
                        source_paths = None
                        if row['source_paths']:
                            try:
                                if isinstance(row['source_paths'], str):
                                    source_paths = json.loads(row['source_paths'])
                                else:
                                    source_paths = row['source_paths']
                            except:
                                source_paths = None
                        
                        # 计算压缩率
                        compression_ratio = 0.0
                        if row['processed_bytes'] and row['processed_bytes'] > 0 and row['compressed_bytes']:
                            compression_ratio = float(row['compressed_bytes']) / float(row['processed_bytes'])
                        
                        # 解析result_summary获取预计的压缩包总数
                        estimated_archive_count = None
                        total_scanned_bytes = None
                        if row['result_summary']:
                            try:
                                result_summary_dict = None
                                if isinstance(row['result_summary'], str):
                                    result_summary_dict = json.loads(row['result_summary'])
                                elif isinstance(row['result_summary'], dict):
                                    result_summary_dict = row['result_summary']
                                
                                if isinstance(result_summary_dict, dict):
                                    estimated_archive_count = result_summary_dict.get('estimated_archive_count')
                                    total_scanned_bytes = result_summary_dict.get('total_scanned_bytes')
                            except Exception:
                                pass
                        
                        return {
                            'task_id': task_id,
                            'status': row['status'],
                            'progress_percent': row['progress_percent'] or 0.0,
                            'processed_files': row['processed_files'] or 0,
                            'total_files': row['total_files'] or 0,  # 总文件数（由后台扫描任务更新）
                            'total_bytes': row['total_bytes'] or 0,  # 总字节数（由后台扫描任务更新）
                            'processed_bytes': row['processed_bytes'] or 0,
                            'compressed_bytes': row['compressed_bytes'] or 0,
                            'compression_ratio': compression_ratio,
                            'estimated_archive_count': estimated_archive_count,  # 压缩包数量（从 result_summary.estimated_archive_count 读取）
                            'description': row['description'] or '',
                            'source_paths': source_paths,
                            'tape_device': row['tape_device'],
                            'tape_id': row['tape_id'],
                            'started_at': row['started_at'],
                            'completed_at': row['completed_at']
                        }
                finally:
                    await conn.close()
            
            return None
        except Exception as e:
            logger.error(f"获取任务状态失败: {str(e)}")
            return None
    
    async def get_total_files_from_db(self, task_id: int) -> int:
        """从数据库读取总文件数（由后台扫描任务更新）
        
        Args:
            task_id: 任务ID
            
        Returns:
            总文件数，如果不存在则返回0
        """
        try:
            from utils.scheduler.db_utils import is_opengauss, get_opengauss_connection
            
            if is_opengauss():
                conn = await get_opengauss_connection()
                try:
                    row = await conn.fetchrow(
                        "SELECT total_files FROM backup_tasks WHERE id = $1",
                        task_id
                    )
                    if row:
                        return row['total_files'] or 0
                finally:
                    await conn.close()
            return 0
        except Exception as e:
            logger.debug(f"读取总文件数失败（忽略继续）: {str(e)}")
            return 0
    
    async def update_scan_progress_only(self, backup_task: BackupTask, total_files: int, total_bytes: int):
        """仅更新扫描进度（总文件数和总字节数），不更新已处理文件数
        
        Args:
            backup_task: 备份任务对象
            total_files: 总文件数
            total_bytes: 总字节数
        """
        try:
            if not backup_task or not backup_task.id:
                return
            
            # 更新备份任务对象的统计信息
            backup_task.total_files = total_files  # total_files: 总文件数
            backup_task.total_bytes = total_bytes  # total_bytes: 总字节数
            
            # 更新数据库
            from utils.scheduler.db_utils import is_opengauss, get_opengauss_connection
            
            if is_opengauss():
                conn = await get_opengauss_connection()
                try:
                    # 获取当前的 result_summary
                    row = await conn.fetchrow(
                        "SELECT result_summary FROM backup_tasks WHERE id = $1",
                        backup_task.id
                    )
                    result_summary = {}
                    if row and row['result_summary']:
                        try:
                            if isinstance(row['result_summary'], str):
                                result_summary = json.loads(row['result_summary'])
                            elif isinstance(row['result_summary'], dict):
                                result_summary = row['result_summary']
                        except Exception:
                            result_summary = {}
                    
                    # 更新 result_summary 中的总文件数和总字节数（作为备份存储）
                    result_summary['total_scanned_files'] = total_files
                    result_summary['total_scanned_bytes'] = total_bytes
                    
                    # 更新数据库：使用正确的字段存储总文件数和总字节数
                    await conn.execute(
                        """
                        UPDATE backup_tasks
                        SET total_files = $1,
                            total_bytes = $2,
                            result_summary = $3::json,
                            updated_at = $4
                        WHERE id = $5
                        """,
                        total_files,  # total_files: 总文件数
                        total_bytes,   # total_bytes: 总字节数
                        json.dumps(result_summary),
                        datetime.now(),
                        backup_task.id
                    )
                finally:
                    await conn.close()
        except Exception as e:
            logger.debug(f"更新扫描进度失败（忽略继续）: {str(e)}")

