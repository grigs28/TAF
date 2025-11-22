#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
备份管理API - 备份集查询
Backup Management API - Backup Sets Query
"""

import logging
from typing import Optional
from fastapi import APIRouter, HTTPException
from utils.scheduler.db_utils import is_redis, is_opengauss
from utils.scheduler.sqlite_utils import is_sqlite

logger = logging.getLogger(__name__)
router = APIRouter()


@router.get("/backup-sets")
async def get_backup_sets(
    backup_group: Optional[str] = None,
    limit: int = 50,
    offset: int = 0
):
    """获取备份集列表"""
    try:
        if is_redis():
            # Redis 模式：使用 Redis 查询
            from backup.redis_backup_db import list_backup_sets_redis
            return await list_backup_sets_redis(
                backup_group=backup_group,
                limit=limit,
                offset=offset
            )
        elif is_opengauss():
            # openGauss 模式：使用原生 SQL 查询
            from utils.scheduler.db_utils import get_opengauss_connection
            async with get_opengauss_connection() as conn:
                # 构建 WHERE 子句
                where_clauses = []
                params = []
                param_idx = 1
                
                if backup_group:
                    where_clauses.append(f"backup_group = ${param_idx}")
                    params.append(backup_group)
                    param_idx += 1
                
                where_sql = " AND ".join(where_clauses) if where_clauses else "1=1"
                
                # 获取总数
                count_sql = f"SELECT COUNT(*) as total FROM backup_sets WHERE {where_sql}"
                count_row = await conn.fetchrow(count_sql, *params)
                total = count_row['total'] if count_row else 0
                
                # 查询备份集
                sql = f"""
                    SELECT set_id, set_name, backup_group, backup_type, backup_time,
                           total_files, total_bytes, tape_id, status
                    FROM backup_sets
                    WHERE {where_sql}
                    ORDER BY backup_time DESC
                    LIMIT ${param_idx} OFFSET ${param_idx + 1}
                """
                params.extend([limit, offset])
                rows = await conn.fetch(sql, *params)
                
                # 转换为响应格式
                sets_list = []
                for row in rows:
                    sets_list.append({
                        "set_id": row['set_id'],
                        "set_name": row['set_name'],
                        "backup_group": row['backup_group'],
                        "backup_type": row['backup_type'].value if hasattr(row['backup_type'], 'value') else str(row['backup_type']),
                        "backup_time": row['backup_time'].isoformat() if row['backup_time'] else None,
                        "total_files": row['total_files'] or 0,
                        "total_bytes": row['total_bytes'] or 0,
                        "tape_id": row['tape_id'],
                        "status": row['status'].value if hasattr(row['status'], 'value') else str(row['status']).lower()
                    })
                
                return {
                    "backup_sets": sets_list,
                    "total": total,
                    "limit": limit,
                    "offset": offset
                }
        elif is_sqlite():
            # SQLite 模式：使用原生 SQL 查询
            from utils.scheduler.sqlite_utils import get_sqlite_connection
            async with get_sqlite_connection() as conn:
                # 构建 WHERE 子句
                where_clauses = []
                params = []
                
                if backup_group:
                    where_clauses.append("backup_group = ?")
                    params.append(backup_group)
                
                where_sql = " AND ".join(where_clauses) if where_clauses else "1=1"
                
                # 获取总数
                count_sql = f"SELECT COUNT(*) as total FROM backup_sets WHERE {where_sql}"
                cursor = await conn.execute(count_sql, tuple(params))
                count_row = await cursor.fetchone()
                total = count_row[0] if count_row else 0
                
                # 查询备份集
                sql = f"""
                    SELECT set_id, set_name, backup_group, backup_type, backup_time,
                           total_files, total_bytes, tape_id, status
                    FROM backup_sets
                    WHERE {where_sql}
                    ORDER BY backup_time DESC
                    LIMIT ? OFFSET ?
                """
                params.extend([limit, offset])
                cursor = await conn.execute(sql, tuple(params))
                rows = await cursor.fetchall()
                
                # 获取列名
                columns = [desc[0] for desc in cursor.description] if cursor.description else []
                
                # 转换为响应格式
                sets_list = []
                for row in rows:
                    row_dict = dict(zip(columns, row))
                    sets_list.append({
                        "set_id": row_dict.get('set_id'),
                        "set_name": row_dict.get('set_name'),
                        "backup_group": row_dict.get('backup_group'),
                        "backup_type": str(row_dict.get('backup_type', 'full')).lower(),
                        "backup_time": row_dict.get('backup_time').isoformat() if row_dict.get('backup_time') and hasattr(row_dict.get('backup_time'), 'isoformat') else (str(row_dict.get('backup_time')) if row_dict.get('backup_time') else None),
                        "total_files": row_dict.get('total_files') or 0,
                        "total_bytes": row_dict.get('total_bytes') or 0,
                        "tape_id": row_dict.get('tape_id'),
                        "status": str(row_dict.get('status', 'active')).lower()
                    })
                
                return {
                    "backup_sets": sets_list,
                    "total": total,
                    "limit": limit,
                    "offset": offset
                }
        else:
            # 未知数据库类型，返回空列表
            logger.warning("未知的数据库类型，返回空备份集列表")
            return {
                "backup_sets": [],
                "total": 0,
                "limit": limit,
                "offset": offset
            }

    except Exception as e:
        logger.error(f"获取备份集列表失败: {str(e)}", exc_info=True)
        raise HTTPException(status_code=500, detail=str(e))

