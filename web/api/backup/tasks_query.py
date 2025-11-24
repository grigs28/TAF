#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
备份管理API - 任务查询
Backup Management API - Task Query
"""

import logging
import json
from typing import List, Dict, Any, Optional
from fastapi import APIRouter, HTTPException, Request
from models.backup import BackupTaskType, BackupTaskStatus
from utils.scheduler.db_utils import is_opengauss, is_redis, get_opengauss_connection
from utils.scheduler.sqlite_utils import is_sqlite, get_sqlite_connection
from .models import BackupTaskResponse
from .utils import _normalize_status_value, _build_stage_info

logger = logging.getLogger(__name__)
router = APIRouter()


@router.get("/tasks", response_model=List[BackupTaskResponse])
async def get_backup_tasks(
    status: Optional[str] = None,
    task_type: Optional[str] = None,
    q: Optional[str] = None,
    limit: int = 50,
    offset: int = 0,
    http_request: Request = None
):
    """获取备份任务列表（执行记录）
    
    此接口返回所有备份任务的执行记录，包括：
    - 通过计划任务模块创建的备份任务
    - 通过备份管理模块立即执行的备份任务
    """
    try:
        # 在函数开头导入所有需要的函数，避免在条件分支中导入导致作用域问题
        from utils.scheduler.db_utils import is_redis as _is_redis
        from utils.scheduler.sqlite_utils import is_sqlite as _is_sqlite
        
        if is_opengauss():
            def _decode_json_field(value, default=None):
                """openGauss driver可能返回str/memoryview/bytes，统一解码为Python对象"""
                if value is None:
                    return default
                if isinstance(value, memoryview):
                    try:
                        value = value.tobytes()
                    except Exception:
                        return default
                if isinstance(value, (bytes, bytearray)):
                    try:
                        value = value.decode('utf-8')
                    except Exception:
                        return default
                if isinstance(value, str):
                    try:
                        return json.loads(value)
                    except json.JSONDecodeError:
                        return default
                if isinstance(value, (list, dict)):
                    return value
                return default
            # 使用原生SQL查询（使用连接池）
            async with get_opengauss_connection() as conn:
                # 构建WHERE子句
                where_clauses = []
                params = []
                param_index = 1
                
                # 默认返回所有记录（模板+执行记录）；当 status/task_type 为 'all' 或空时不加过滤
                normalized_status = (status or '').lower()
                include_not_run = normalized_status in ('not_run', '未运行')
                if status and normalized_status not in ('all', 'not_run', '未运行'):
                    # 以文本方式匹配，避免依赖枚举类型存在
                    where_clauses.append(f"LOWER(status::text) = LOWER(${param_index})")
                    params.append(status)
                    param_index += 1
                # 未运行：仅限从 backup_tasks 侧筛选"未启动"的pending记录
                if include_not_run:
                    where_clauses.append("(started_at IS NULL) AND LOWER(status::text)=LOWER('PENDING')")
                
                normalized_type = (task_type or '').lower()
                if task_type and normalized_type != 'all':
                    # 以文本方式匹配，避免依赖枚举类型存在
                    where_clauses.append(f"LOWER(task_type::text) = LOWER(${param_index})")
                    params.append(task_type)
                    param_index += 1

                if q and q.strip():
                    where_clauses.append(f"task_name ILIKE ${param_index}")
                    params.append(f"%{q.strip()}%")
                    param_index += 1
                
                where_sql = " AND ".join(where_clauses) if where_clauses else "1=1"
                
                # 构建查询（包含模板与执行记录）- 不在SQL层做分页，合并后在内存分页
                sql = f"""
                    SELECT id, task_name, task_type, status, progress_percent, total_files, 
                           processed_files, total_bytes, processed_bytes, compressed_bytes, 
                           created_at, started_at, completed_at, error_message, is_template, 
                           tape_device, source_paths, description, result_summary, scan_status
                    FROM backup_tasks
                    WHERE {where_sql}
                    ORDER BY created_at DESC
                """
                rows = await conn.fetch(sql, *params)
                
                # 转换为响应格式
                tasks = []
                for row in rows:
                    # 解析JSON字段
                    source_paths = _decode_json_field(row.get("source_paths"), default=[])
                    
                    # 计算压缩率
                    compression_ratio = 0.0
                    total_bytes_actual = 0
                    if row["processed_bytes"] and row["processed_bytes"] > 0 and row["compressed_bytes"]:
                        compression_ratio = float(row["compressed_bytes"]) / float(row["processed_bytes"])
                    
                    # 解析result_summary获取预计的压缩包总数
                    estimated_archive_count = None
                    result_summary_dict = _decode_json_field(row.get("result_summary"), default={})
                    if isinstance(result_summary_dict, dict):
                        estimated_archive_count = result_summary_dict.get('estimated_archive_count')
                        total_bytes_actual = (
                            result_summary_dict.get('total_scanned_bytes')
                            or result_summary_dict.get('total_bytes_actual')
                            or 0
                        )
                    status_value = _normalize_status_value(row["status"])
                    stage_info = _build_stage_info(
                        row.get("description"),
                        row.get("scan_status"),
                        status_value
                    )
                    
                    # 对于运行中的任务，尝试获取 current_compression_progress
                    current_compression_progress = None
                    if status_value and status_value.lower() == 'running':
                        try:
                            from web.api.system import get_system_instance
                            system = get_system_instance(http_request)
                            if system and system.backup_engine:
                                task_status = await system.backup_engine.get_task_status(row["id"])
                                if task_status and 'current_compression_progress' in task_status:
                                    current_compression_progress = task_status['current_compression_progress']
                        except Exception as e:
                            logger.debug(f"获取任务压缩进度失败: {str(e)}")
                    
                    tasks.append({
                        "task_id": row["id"],
                        "task_name": row["task_name"],
                        "task_type": row["task_type"].value if hasattr(row["task_type"], "value") else str(row["task_type"]),
                        "status": status_value,
                        "progress_percent": float(row["progress_percent"]) if row["progress_percent"] else 0.0,
                        "total_files": row["total_files"] or 0,  # 总文件数（由后台扫描任务更新）
                        "processed_files": row["processed_files"] or 0,  # 已处理文件数
                        "total_bytes": row["total_bytes"] or 0,  # 总字节数（由后台扫描任务更新）
                        "total_bytes_actual": total_bytes_actual,
                        "processed_bytes": row["processed_bytes"] or 0,
                        "compressed_bytes": row["compressed_bytes"] or 0,
                        "compression_ratio": compression_ratio,
                        "estimated_archive_count": estimated_archive_count,  # 压缩包数量（从 result_summary.estimated_archive_count 读取）
                        "created_at": row["created_at"],
                        "started_at": row["started_at"],
                        "completed_at": row["completed_at"],
                        "error_message": row["error_message"],
                        "is_template": row["is_template"] or False,
                        "tape_device": row["tape_device"],
                        "source_paths": source_paths or [],
                        "description": row["description"] or "",
                        "from_scheduler": False,
                        "operation_status": stage_info["operation_status"],
                        "operation_stage": stage_info["operation_stage"],
                        "operation_stage_label": stage_info["operation_stage_label"],
                        "stage_steps": stage_info["stage_steps"],
                        "current_compression_progress": current_compression_progress
                    })
                # 追加计划任务（未运行模板）
                # 仅当无状态过滤或过滤为pending/all时返回
                include_sched = (not status) or (normalized_status in ("all", "pending", 'not_run', '未运行'))
                if include_sched:
                    sched_where = ["LOWER(action_type::text)=LOWER('BACKUP')"]
                    sched_params = []
                    if q and q.strip():
                        sched_where.append("task_name ILIKE $1")
                        sched_params.append(f"%{q.strip()}%")
                    # 任务类型筛选
                    if task_type and normalized_type != 'all':
                        # 从 action_config->task_type 里匹配（字符串包含）
                        # openGauss json 提取可后续增强，这里简化为 ILIKE 检测
                        if sched_params:
                            sched_where.append("(action_config::text) ILIKE $2")
                            sched_params.append(f"%\"task_type\": \"{task_type}\"%")
                        else:
                            sched_where.append("(action_config::text) ILIKE $1")
                            sched_params.append(f"%\"task_type\": \"{task_type}\"%")
                    # 未运行：计划任务自然视作未运行
                    sched_sql = f"""
                        SELECT id, task_name, status, enabled, created_at, action_config, task_metadata
                        FROM scheduled_tasks
                        WHERE {' AND '.join(sched_where)}
                        ORDER BY created_at DESC
                    """
                    sched_rows = await conn.fetch(sched_sql, *sched_params)
                    template_ids = set()
                    parsed_sched_rows = []
                    for srow in sched_rows:
                        action_cfg = _decode_json_field(srow.get("action_config"), default={})
                        task_metadata = _decode_json_field(srow.get("task_metadata"), default={})
                        backup_template_id = None
                        if isinstance(task_metadata, dict):
                            backup_template_id = task_metadata.get("backup_task_id")
                            if backup_template_id:
                                template_ids.add(int(backup_template_id))
                        parsed_sched_rows.append((srow, action_cfg, task_metadata, backup_template_id))

                    template_info_map = {}
                    if template_ids:
                        template_rows = await conn.fetch(
                            """
                            SELECT id, source_paths, tape_device
                            FROM backup_tasks
                            WHERE id = ANY($1::int[])
                            """,
                            list(template_ids)
                        )
                        for trow in template_rows:
                            t_source_paths = _decode_json_field(trow.get('source_paths'), default=[])
                            template_info_map[trow['id']] = {
                                "source_paths": t_source_paths or [],
                                "tape_device": trow.get('tape_device')
                            }
                    for srow, acfg, metadata, template_id in parsed_sched_rows:
                        # 从action_config中提取task_type/tape_device/source_paths
                        atype = 'full'
                        tdev = None
                        spaths: Optional[List[str]] = None
                        try:
                            if isinstance(acfg, dict):
                                atype = acfg.get('task_type') or atype
                                tdev = acfg.get('tape_device')
                                cfg_paths = acfg.get('source_paths')
                                if isinstance(cfg_paths, list):
                                    spaths = [str(p) for p in cfg_paths if p]
                                elif isinstance(cfg_paths, str) and cfg_paths.strip():
                                    spaths = [cfg_paths.strip()]
                        except Exception as parse_error:
                            logger.debug(f"解析计划任务 action_config 失败: {parse_error}")
                        template_fallback = template_info_map.get(int(template_id)) if template_id else None
                        if (not spaths) and template_fallback:
                            spaths = template_fallback.get("source_paths") or []
                        if (not tdev) and template_fallback:
                            tdev = template_fallback.get("tape_device")
                        stage_info = _build_stage_info("", None, "pending")
                        tasks.append({
                            "task_id": srow["id"],
                            "task_name": srow["task_name"],
                            "task_type": atype,
                            "status": "pending",  # 计划任务视为未运行
                            "progress_percent": 0.0,
                            "total_files": 0,
                            "processed_files": 0,
                            "total_bytes": 0,
                            "total_bytes_actual": 0,
                            "processed_bytes": 0,
                            "compressed_bytes": 0,
                            "compression_ratio": 0.0,
                            "created_at": srow["created_at"],
                            "started_at": None,
                            "completed_at": None,
                            "error_message": None,
                            "is_template": True,
                            "tape_device": tdev,
                            "source_paths": spaths or [],
                            "from_scheduler": True,
                            "enabled": srow.get("enabled", True),
                            "description": "",
                            "estimated_archive_count": None,
                            "operation_status": stage_info["operation_status"],
                            "operation_stage": stage_info["operation_stage"],
                            "operation_stage_label": stage_info["operation_stage_label"],
                            "stage_steps": stage_info["stage_steps"]
                        })
                # 合并后排序与分页（统一为时间戳，避免aware/naive比较异常）
                def _ts(val):
                    try:
                        if not val:
                            return 0.0
                        if isinstance(val, (int, float)):
                            return float(val)
                        # datetime
                        return val.timestamp()
                    except Exception:
                        return 0.0
                tasks.sort(key=lambda x: _ts(x.get('created_at')), reverse=True)
                return tasks[offset:offset+limit]
        else:
            # 检查是否为Redis数据库
            if _is_redis():
                # Redis版本
                from backup.redis_backup_db import get_backup_tasks_redis
                return await get_backup_tasks_redis(status, task_type, q, limit, offset)
            
            # 检查是否为SQLite（确保只对SQLite使用SQLite连接）
            if not _is_sqlite():
                db_type = "openGauss" if is_opengauss() else "未知类型"
                logger.warning(f"[{db_type}模式] 当前数据库类型不支持使用SQLite连接查询备份任务列表，返回空列表")
                return []
            
            # 使用原生SQL查询（SQLite）
            async with get_sqlite_connection() as conn:
                # 构建WHERE子句
                where_clauses = ["is_template = 0"]
                params = []
                
                # 状态过滤
                normalized_status = (status or '').lower()
                include_not_run = normalized_status in ('not_run', '未运行')
                if status and normalized_status not in ('all', 'not_run', '未运行'):
                    # 使用LOWER进行大小写不敏感匹配
                    where_clauses.append("LOWER(status) = LOWER(?)")
                    params.append(status)
                # 未运行：仅限未启动的pending记录
                if include_not_run:
                    where_clauses.append("(started_at IS NULL) AND LOWER(status) = LOWER('pending')")
                
                if task_type and task_type.lower() != 'all':
                    where_clauses.append("LOWER(task_type) = LOWER(?)")
                    params.append(task_type)
                
                if q and q.strip():
                    where_clauses.append("task_name LIKE ?")
                    params.append(f"%{q.strip()}%")
                
                where_sql = " AND ".join(where_clauses)
                
                # 构建查询
                sql = f"""
                    SELECT id, task_name, task_type, status, progress_percent, total_files, 
                           processed_files, total_bytes, processed_bytes, compressed_bytes, 
                           created_at, started_at, completed_at, error_message, is_template, 
                           tape_device, source_paths, description, result_summary, scan_status
                    FROM backup_tasks
                    WHERE {where_sql}
                    ORDER BY created_at DESC
                """
                cursor = await conn.execute(sql, params)
                rows = await cursor.fetchall()
                
                # 转换为响应格式
                tasks = []
                for row in rows:
                    # 解析JSON字段
                    source_paths = []
                    try:
                        if row[16]:  # source_paths
                            if isinstance(row[16], str):
                                source_paths = json.loads(row[16])
                            elif isinstance(row[16], (list, dict)):
                                source_paths = row[16]
                    except Exception:
                        source_paths = []
                    
                    # 计算压缩率
                    compression_ratio = 0.0
                    total_bytes_actual = 0
                    processed_bytes = row[8] or 0  # processed_bytes
                    compressed_bytes = row[9] or 0  # compressed_bytes
                    if processed_bytes > 0 and compressed_bytes:
                        compression_ratio = float(compressed_bytes) / float(processed_bytes)
                    
                    # 解析result_summary获取预计的压缩包总数
                    estimated_archive_count = None
                    result_summary_dict = {}
                    try:
                        if row[18]:  # result_summary
                            if isinstance(row[18], str):
                                result_summary_dict = json.loads(row[18])
                            elif isinstance(row[18], dict):
                                result_summary_dict = row[18]
                        if isinstance(result_summary_dict, dict):
                            estimated_archive_count = result_summary_dict.get('estimated_archive_count')
                            total_bytes_actual = (
                                result_summary_dict.get('total_scanned_bytes')
                                or result_summary_dict.get('total_bytes_actual')
                                or 0
                            )
                    except Exception:
                        pass
                    
                    # 处理状态值（将小写转换为枚举值）
                    status_raw = row[3]  # status
                    status_value = ""
                    if status_raw:
                        # 如果是字符串，尝试转换为枚举值
                        if isinstance(status_raw, str):
                            # 尝试直接匹配枚举值（不区分大小写）
                            status_lower = status_raw.lower()
                            try:
                                # 枚举值本身就是小写，所以可以直接匹配
                                status_enum = BackupTaskStatus(status_lower)
                                status_value = status_enum.value
                            except (ValueError, AttributeError):
                                # 如果转换失败，尝试匹配枚举名称（大写）
                                try:
                                    # 尝试将小写转换为大写枚举名称
                                    status_upper = status_raw.upper()
                                    status_enum = BackupTaskStatus[status_upper]
                                    status_value = status_enum.value
                                except (KeyError, ValueError, AttributeError):
                                    # 如果都失败，保持原值
                                    status_value = status_raw
                        else:
                            # 如果不是字符串，使用 _normalize_status_value
                            status_value = _normalize_status_value(status_raw)
                    
                    stage_info = _build_stage_info(
                        row[17] or "",  # description
                        row[19],  # scan_status
                        status_value
                    )
                    
                    # 处理task_type
                    task_type_value = row[2]  # task_type
                    if isinstance(task_type_value, str) and task_type_value.islower():
                        try:
                            task_type_enum = BackupTaskType(task_type_value)
                            task_type_value = task_type_enum.value
                        except (ValueError, AttributeError):
                            pass
                    
                    tasks.append({
                        "task_id": row[0],  # id
                        "task_name": row[1],  # task_name
                        "task_type": task_type_value,
                        "status": status_value,
                        "progress_percent": float(row[4]) if row[4] else 0.0,  # progress_percent
                        "total_files": row[5] or 0,  # total_files
                        "processed_files": row[6] or 0,  # processed_files
                        "total_bytes": row[7] or 0,  # total_bytes
                        "total_bytes_actual": total_bytes_actual,
                        "processed_bytes": processed_bytes,
                        "compressed_bytes": compressed_bytes,
                        "compression_ratio": compression_ratio,
                        "estimated_archive_count": estimated_archive_count,
                        "created_at": row[10],  # created_at
                        "started_at": row[11],  # started_at
                        "completed_at": row[12],  # completed_at
                        "error_message": row[13],  # error_message
                        "is_template": bool(row[14]) if row[14] is not None else False,  # is_template
                        "tape_device": row[15],  # tape_device
                        "source_paths": source_paths or [],
                        "description": row[17] or "",  # description
                        "from_scheduler": False,
                        "enabled": True,  # SQLite中没有enabled字段，默认为True
                        "operation_status": stage_info["operation_status"],
                        "operation_stage": stage_info["operation_stage"],
                        "operation_stage_label": stage_info["operation_stage_label"],
                        "stage_steps": stage_info["stage_steps"]
                    })
                
                # 追加计划任务（未运行模板）
                # 仅当无状态过滤或过滤为pending/all时返回
                normalized_status = (status or '').lower()
                include_sched = (not status) or (normalized_status in ("all", "pending", 'not_run', '未运行'))
                if include_sched:
                    sched_where = ["action_type = 'BACKUP'"]
                    sched_params = []
                    if q and q.strip():
                        sched_where.append("task_name LIKE ?")
                        sched_params.append(f"%{q.strip()}%")
                    
                    sched_sql = f"""
                        SELECT id, task_name, status, enabled, created_at, action_config, task_metadata
                        FROM scheduled_tasks
                        WHERE {' AND '.join(sched_where)}
                        ORDER BY created_at DESC
                    """
                    sched_cursor = await conn.execute(sched_sql, sched_params)
                    sched_rows = await sched_cursor.fetchall()
                    
                    template_ids = set()
                    parsed_sched_rows = []
                    for srow in sched_rows:
                        task_metadata = {}
                        try:
                            if srow[6]:  # task_metadata
                                if isinstance(srow[6], str):
                                    task_metadata = json.loads(srow[6])
                                elif isinstance(srow[6], dict):
                                    task_metadata = srow[6]
                        except Exception:
                            pass
                        backup_template_id = task_metadata.get("backup_task_id") if isinstance(task_metadata, dict) else None
                        if backup_template_id:
                            template_ids.add(int(backup_template_id))
                        parsed_sched_rows.append((srow, task_metadata, backup_template_id))
                    
                    template_info_map = {}
                    if template_ids:
                        placeholders = ','.join('?' * len(template_ids))
                        template_sql = f"""
                            SELECT id, source_paths, tape_device
                            FROM backup_tasks
                            WHERE id IN ({placeholders})
                        """
                        template_cursor = await conn.execute(template_sql, list(template_ids))
                        template_rows = await template_cursor.fetchall()
                        for trow in template_rows:
                            t_source_paths = []
                            try:
                                if trow[1]:  # source_paths
                                    if isinstance(trow[1], str):
                                        t_source_paths = json.loads(trow[1])
                                    elif isinstance(trow[1], (list, dict)):
                                        t_source_paths = trow[1]
                            except Exception:
                                pass
                            template_info_map[trow[0]] = {
                                "source_paths": t_source_paths or [],
                                "tape_device": trow[2]
                            }
                    
                    for srow, metadata, template_id in parsed_sched_rows:
                        action_cfg = {}
                        try:
                            if srow[5]:  # action_config
                                if isinstance(srow[5], str):
                                    action_cfg = json.loads(srow[5])
                                elif isinstance(srow[5], dict):
                                    action_cfg = srow[5]
                        except Exception:
                            pass
                        
                        # 从action_config中提取task_type/tape_device/source_paths
                        atype = 'full'
                        tdev = None
                        spaths: Optional[List[str]] = None
                        try:
                            if isinstance(action_cfg, dict):
                                atype = action_cfg.get('task_type') or atype
                                tdev = action_cfg.get('tape_device')
                                cfg_paths = action_cfg.get('source_paths')
                                if isinstance(cfg_paths, list):
                                    spaths = [str(p) for p in cfg_paths if p]
                                elif isinstance(cfg_paths, str) and cfg_paths.strip():
                                    spaths = [cfg_paths.strip()]
                        except Exception as parse_error:
                            logger.debug(f"解析计划任务 action_config 失败: {parse_error}")
                        
                        template_fallback = template_info_map.get(int(template_id)) if template_id else None
                        if (not spaths) and template_fallback:
                            spaths = template_fallback.get("source_paths") or []
                        if (not tdev) and template_fallback:
                            tdev = template_fallback.get("tape_device")
                        
                        stage_info = _build_stage_info("", None, "pending")
                        tasks.append({
                            "task_id": srow[0],  # id
                            "task_name": srow[1],  # task_name
                            "task_type": atype,
                            "status": "pending",  # 计划任务视为未运行
                            "progress_percent": 0.0,
                            "total_files": 0,
                            "processed_files": 0,
                            "total_bytes": 0,
                            "total_bytes_actual": 0,
                            "processed_bytes": 0,
                            "compressed_bytes": 0,
                            "compression_ratio": 0.0,
                            "created_at": srow[4],  # created_at
                            "started_at": None,
                            "completed_at": None,
                            "error_message": None,
                            "is_template": True,
                            "tape_device": tdev,
                            "source_paths": spaths or [],
                            "from_scheduler": True,
                            "enabled": bool(srow[3]) if srow[3] is not None else True,  # enabled
                            "description": "",
                            "estimated_archive_count": None,
                            "operation_status": stage_info["operation_status"],
                            "operation_stage": stage_info["operation_stage"],
                            "operation_stage_label": stage_info["operation_stage_label"],
                            "stage_steps": stage_info["stage_steps"]
                        })
                
                # 合并后排序与分页（统一为时间戳，避免aware/naive比较异常）
                def _ts(val):
                    try:
                        if not val:
                            return 0.0
                        if isinstance(val, (int, float)):
                            return float(val)
                        # datetime
                        return val.timestamp()
                    except Exception:
                        return 0.0
                tasks.sort(key=lambda x: _ts(x.get('created_at')), reverse=True)
                return tasks[offset:offset+limit]

    except Exception as e:
        logger.error(f"获取备份任务列表失败: {str(e)}", exc_info=True)
        raise HTTPException(status_code=500, detail=str(e))


@router.get("/tasks/{task_id}", response_model=BackupTaskResponse)
async def get_backup_task(task_id: int, http_request: Request):
    """获取备份任务详情"""
    try:
        if is_opengauss():
            # 使用原生SQL查询（使用连接池）
            async with get_opengauss_connection() as conn:
                row = await conn.fetchrow(
                    """
                    SELECT id, task_name, task_type, status, progress_percent, total_files, 
                           processed_files, total_bytes, processed_bytes, compressed_bytes,
                           created_at, started_at, completed_at, error_message, is_template,
                           tape_device, source_paths, description, result_summary, enabled
                    FROM backup_tasks
                    WHERE id = $1
                    """,
                    task_id
                )
                
                if not row:
                    raise HTTPException(status_code=404, detail="备份任务不存在")
                
                # 解析JSON字段
                import json
                source_paths = None
                if row["source_paths"]:
                    try:
                        if isinstance(row["source_paths"], str):
                            source_paths = json.loads(row["source_paths"])
                        else:
                            source_paths = row["source_paths"]
                    except:
                        source_paths = None
                
                total_bytes_actual = 0
                estimated_archive_count = None
                try:
                    result_summary = row.get("result_summary")
                    result_summary_dict = None
                    if result_summary:
                        if isinstance(result_summary, str):
                            result_summary_dict = json.loads(result_summary)
                        elif isinstance(result_summary, dict):
                            result_summary_dict = result_summary
                    if isinstance(result_summary_dict, dict):
                        total_bytes_actual = result_summary_dict.get('total_scanned_bytes') or 0
                        estimated_archive_count = result_summary_dict.get('estimated_archive_count')
                except Exception:
                    total_bytes_actual = 0
                compression_ratio = 0.0
                compressed_bytes = row.get("compressed_bytes") or 0
                try:
                    if row["processed_bytes"] and row["processed_bytes"] > 0 and compressed_bytes:
                        compression_ratio = float(compressed_bytes) / float(row["processed_bytes"])
                except Exception:
                    compression_ratio = 0.0
                
                status_value = _normalize_status_value(row["status"])
                stage_info = _build_stage_info(
                    row.get("description"),
                    row.get("scan_status"),
                    status_value
                )
                # 对于运行中的任务，尝试获取 current_compression_progress
                current_compression_progress = None
                if status_value and status_value.lower() == 'running':
                    try:
                        from web.api.system import get_system_instance
                        system = get_system_instance(http_request)
                        if system and system.backup_engine:
                            task_status = await system.backup_engine.get_task_status(row["id"])
                            if task_status and 'current_compression_progress' in task_status:
                                current_compression_progress = task_status['current_compression_progress']
                    except Exception as e:
                        logger.debug(f"获取任务压缩进度失败: {str(e)}")
                
                return {
                    "task_id": row["id"],
                    "task_name": row["task_name"],
                    "task_type": row["task_type"].value if hasattr(row["task_type"], "value") else str(row["task_type"]),
                    "status": status_value,
                    "progress_percent": float(row["progress_percent"]) if row["progress_percent"] else 0.0,
                    "total_files": row["total_files"] or 0,
                    "processed_files": row["processed_files"] or 0,
                    "total_bytes": row["total_bytes"] or 0,
                    "total_bytes_actual": total_bytes_actual,
                    "processed_bytes": row["processed_bytes"] or 0,
                    "compressed_bytes": compressed_bytes,
                    "compression_ratio": compression_ratio,
                    "estimated_archive_count": estimated_archive_count,
                    "created_at": row["created_at"],
                    "started_at": row["started_at"],
                    "completed_at": row["completed_at"],
                    "error_message": row["error_message"],
                    "description": row["description"] or "",
                    "is_template": row["is_template"] or False,
                    "tape_device": row["tape_device"],
                    "source_paths": source_paths or [],
                    "enabled": row.get("enabled", True),
                    "from_scheduler": False,
                    "operation_status": stage_info["operation_status"],
                    "operation_stage": stage_info["operation_stage"],
                    "operation_stage_label": stage_info["operation_stage_label"],
                    "stage_steps": stage_info["stage_steps"],
                    "current_compression_progress": current_compression_progress
                }
        else:
            # 检查是否为Redis数据库
            from utils.scheduler.db_utils import is_redis
            from utils.scheduler.sqlite_utils import is_sqlite
            
            if is_redis():
                logger.warning("[Redis模式] 查询备份任务详情暂未实现，抛出HTTPException")
                raise HTTPException(status_code=404, detail="Redis模式下查询备份任务详情暂未实现，请使用Redis相关API")
            
            if not is_sqlite():
                from utils.scheduler.db_utils import is_opengauss
                db_type = "openGauss" if is_opengauss() else "未知类型"
                logger.warning(f"[{db_type}模式] 当前数据库类型不支持使用SQLite连接查询备份任务详情，抛出HTTPException")
                raise HTTPException(status_code=400, detail=f"{db_type}模式下不支持使用SQLite连接查询备份任务详情")
            
            # 使用原生SQL查询（SQLite）
            async with get_sqlite_connection() as conn:
                cursor = await conn.execute("""
                    SELECT id, task_name, task_type, status, progress_percent, total_files,
                           processed_files, total_bytes, processed_bytes, compressed_bytes,
                           created_at, started_at, completed_at, error_message, is_template,
                           tape_device, source_paths, description, result_summary, scan_status
                    FROM backup_tasks
                    WHERE id = ?
                """, (task_id,))
                row = await cursor.fetchone()
                
                if not row:
                    raise HTTPException(status_code=404, detail="备份任务不存在")
                
                # 解析JSON字段
                source_paths = []
                try:
                    if row[16]:  # source_paths
                        if isinstance(row[16], str):
                            source_paths = json.loads(row[16])
                        elif isinstance(row[16], (list, dict)):
                            source_paths = row[16]
                except Exception:
                    source_paths = []
                
                # 解析result_summary
                total_bytes_actual = 0
                estimated_archive_count = None
                try:
                    if row[18]:  # result_summary
                        result_summary_dict = {}
                        if isinstance(row[18], str):
                            result_summary_dict = json.loads(row[18])
                        elif isinstance(row[18], dict):
                            result_summary_dict = row[18]
                        if isinstance(result_summary_dict, dict):
                            total_bytes_actual = result_summary_dict.get('total_scanned_bytes') or 0
                            estimated_archive_count = result_summary_dict.get('estimated_archive_count')
                except Exception:
                    pass
                
                # 计算压缩率
                compression_ratio = 0.0
                processed_bytes = row[8] or 0  # processed_bytes (索引8)
                compressed_bytes = row[9] or 0  # compressed_bytes (索引9)
                if processed_bytes > 0 and compressed_bytes:
                    compression_ratio = float(compressed_bytes) / float(processed_bytes)
                
                # 处理状态值
                status_raw = row[3]  # status
                status_value = ""
                if status_raw:
                    if isinstance(status_raw, str):
                        status_lower = status_raw.lower()
                        try:
                            status_enum = BackupTaskStatus(status_lower)
                            status_value = status_enum.value
                        except (ValueError, AttributeError):
                            try:
                                status_enum = BackupTaskStatus[status_raw.upper()]
                                status_value = status_enum.value
                            except (KeyError, ValueError, AttributeError):
                                status_value = status_raw
                    else:
                        status_value = _normalize_status_value(status_raw)
                
                # 处理task_type
                task_type_value = row[2]  # task_type
                if isinstance(task_type_value, str) and task_type_value.islower():
                    try:
                        task_type_enum = BackupTaskType(task_type_value)
                        task_type_value = task_type_enum.value
                    except (ValueError, AttributeError):
                        pass
                
                stage_info = _build_stage_info(
                    row[17] or "",  # description
                    row[19],  # scan_status
                    status_value
                )
                
                return {
                    "task_id": row[0],  # id
                    "task_name": row[1],  # task_name
                    "task_type": task_type_value,
                    "status": status_value,
                    "progress_percent": float(row[4]) if row[4] else 0.0,  # progress_percent
                    "total_files": row[5] or 0,  # total_files
                    "processed_files": row[6] or 0,  # processed_files
                    "total_bytes": row[7] or 0,  # total_bytes
                    "total_bytes_actual": total_bytes_actual,
                    "processed_bytes": processed_bytes,
                    "compressed_bytes": compressed_bytes,
                    "compression_ratio": compression_ratio,
                    "estimated_archive_count": estimated_archive_count,
                    "created_at": row[10],  # created_at
                    "started_at": row[11],  # started_at
                    "completed_at": row[12],  # completed_at
                    "error_message": row[13],  # error_message
                    "description": row[17] or "",  # description
                    "is_template": bool(row[14]) if row[14] is not None else False,  # is_template
                    "tape_device": row[15],  # tape_device
                    "source_paths": source_paths or [],
                    "operation_status": stage_info["operation_status"],
                    "operation_stage": stage_info["operation_stage"],
                    "operation_stage_label": stage_info["operation_stage_label"],
                    "stage_steps": stage_info["stage_steps"],
                    "enabled": True,  # SQLite中没有enabled字段，默认为True
                    "from_scheduler": False
                }

    except HTTPException:
        raise
    except Exception as e:
        logger.error(f"获取备份任务详情失败: {str(e)}", exc_info=True)
        raise HTTPException(status_code=500, detail=str(e))


@router.get("/templates", response_model=List[Dict[str, Any]])
async def get_backup_templates(
    limit: int = 50,
    offset: int = 0,
    http_request: Request = None
):
    """获取备份任务模板列表（配置）
    
    返回所有备份任务配置模板，供计划任务模块选择。
    """
    try:
        if is_opengauss():
            # openGauss分支保持不变
            from config.database import db_manager
            from sqlalchemy import select, desc
            from models.backup import BackupTask
            
            async with db_manager.AsyncSessionLocal() as session:
                stmt = select(BackupTask).where(BackupTask.is_template == True)
                stmt = stmt.order_by(desc(BackupTask.created_at))
                stmt = stmt.limit(limit).offset(offset)
                
                result = await session.execute(stmt)
                templates = result.scalars().all()
                
                template_list = []
                for template in templates:
                    template_list.append({
                        "task_id": template.id,
                        "task_name": template.task_name,
                        "task_type": template.task_type.value,
                        "description": template.description,
                        "source_paths": template.source_paths or [],
                        "tape_device": template.tape_device,
                        "compression_enabled": template.compression_enabled,
                        "encryption_enabled": template.encryption_enabled,
                        "retention_days": template.retention_days,
                        "exclude_patterns": template.exclude_patterns or [],
                        "created_at": template.created_at
                    })
                
                return template_list
        else:
            # 检查是否为Redis数据库
            from utils.scheduler.db_utils import is_redis
            from utils.scheduler.sqlite_utils import is_sqlite
            
            if is_redis():
                # Redis模式下返回空列表（暂未实现Redis查询备份任务模板）
                logger.debug("[Redis模式] 查询备份任务模板暂未实现，返回空列表")
                return []
            
            if not is_sqlite():
                from utils.scheduler.db_utils import is_opengauss
                db_type = "openGauss" if is_opengauss() else "未知类型"
                logger.debug(f"[{db_type}模式] 当前数据库类型不支持使用SQLite连接查询备份任务模板，返回空列表")
                return []
            
            # 使用原生SQL查询（SQLite）
            async with get_sqlite_connection() as conn:
                cursor = await conn.execute("""
                    SELECT id, task_name, task_type, description, source_paths, tape_device,
                           compression_enabled, encryption_enabled, retention_days, exclude_patterns, created_at
                    FROM backup_tasks
                    WHERE is_template = 1
                    ORDER BY created_at DESC
                    LIMIT ? OFFSET ?
                """, (limit, offset))
                rows = await cursor.fetchall()
                
                template_list = []
                for row in rows:
                    # 解析JSON字段
                    source_paths = []
                    exclude_patterns = []
                    try:
                        if row[4]:  # source_paths
                            if isinstance(row[4], str):
                                source_paths = json.loads(row[4])
                            elif isinstance(row[4], (list, dict)):
                                source_paths = row[4]
                    except Exception:
                        pass
                    try:
                        if row[9]:  # exclude_patterns
                            if isinstance(row[9], str):
                                exclude_patterns = json.loads(row[9])
                            elif isinstance(row[9], (list, dict)):
                                exclude_patterns = row[9]
                    except Exception:
                        pass
                    
                    # 处理task_type
                    task_type_value = row[2]  # task_type
                    if isinstance(task_type_value, str) and task_type_value.islower():
                        try:
                            task_type_enum = BackupTaskType(task_type_value)
                            task_type_value = task_type_enum.value
                        except (ValueError, AttributeError):
                            pass
                    
                    template_list.append({
                        "task_id": row[0],  # id
                        "task_name": row[1],  # task_name
                        "task_type": task_type_value,
                        "description": row[3] or "",  # description
                        "source_paths": source_paths or [],
                        "tape_device": row[5],  # tape_device
                        "compression_enabled": bool(row[6]) if row[6] is not None else False,  # compression_enabled
                        "encryption_enabled": bool(row[7]) if row[7] is not None else False,  # encryption_enabled
                        "retention_days": row[8],  # retention_days
                        "exclude_patterns": exclude_patterns or [],
                        "created_at": row[10]  # created_at
                    })
                
                return template_list

    except Exception as e:
        logger.error(f"获取备份任务模板列表失败: {str(e)}")
        raise HTTPException(status_code=500, detail=str(e))

