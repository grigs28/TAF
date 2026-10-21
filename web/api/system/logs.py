#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
系统管理API - logs
System Management API - logs
"""

import logging
from typing import Dict, Any, Optional
from datetime import datetime
from fastapi import APIRouter, HTTPException, Request
from pydantic import BaseModel, Field

# 系统日志相关路由，无需导入模型

logger = logging.getLogger(__name__)
router = APIRouter()

@router.get("/log-filters")
async def get_log_filter_options():
    """获取日志筛选器选项（从数据库实际存在的值）"""
    try:
        from utils.scheduler.db_utils import get_opengauss_connection

        categories = []
        levels = []
        operation_types = []

        async with get_opengauss_connection() as conn:
            # operation_logs 中的 category
            rows = await conn.fetch("SELECT DISTINCT category FROM operation_logs WHERE category IS NOT NULL ORDER BY category")
            categories.extend(r['category'] for r in rows)

            # system_logs 中的 category
            rows = await conn.fetch("SELECT DISTINCT category FROM system_logs WHERE category IS NOT NULL ORDER BY category")
            for r in rows:
                val = r['category'] if isinstance(r['category'], str) else r['category'].value
                if val not in categories:
                    categories.append(val)

            # system_logs 中的 log_level
            rows = await conn.fetch("SELECT DISTINCT log_level FROM system_logs WHERE log_level IS NOT NULL ORDER BY log_level")
            levels.extend(
                r['log_level'] if isinstance(r['log_level'], str) else r['log_level'].value
                for r in rows
            )

            # operation_logs 中的 operation_type
            rows = await conn.fetch("SELECT DISTINCT operation_type FROM operation_logs WHERE operation_type IS NOT NULL ORDER BY operation_type")
            operation_types.extend(
                r['operation_type'] if isinstance(r['operation_type'], str) else r['operation_type'].value
                for r in rows
            )

        return {
            "success": True,
            "categories": categories,
            "levels": levels,
            "operation_types": operation_types,
        }
    except Exception as e:
        logger.error(f"获取日志筛选选项失败: {str(e)}")
        raise HTTPException(status_code=500, detail=str(e))


@router.get("/logs")
async def get_system_logs(
    category: Optional[str] = None,
    level: Optional[str] = None,
    operation_type: Optional[str] = None,
    resource_type: Optional[str] = None,
    user_id: Optional[int] = None,
    start_time: Optional[datetime] = None,
    end_time: Optional[datetime] = None,
    limit: int = 100,
    offset: int = 0,
    request: Request = None
):
    """获取系统日志
    
    Args:
        category: 日志分类（system/backup/recovery/tape/user/security等）
        level: 日志级别（debug/info/warning/error/critical）
        operation_type: 操作类型（login/backup/recovery/tape_load等）
        resource_type: 资源类型（tape/backup/recovery/scheduler/user/system）
        user_id: 用户ID
        start_time: 开始时间
        end_time: 结束时间
        limit: 返回数量限制
        offset: 偏移量
    """
    try:
        from config.database import db_manager
        from datetime import datetime, timedelta
        import json
        
        # 如果没有指定时间范围，默认查询最近24小时的日志
        if not start_time:
            start_time = datetime.now() - timedelta(days=1)
        if not end_time:
            end_time = datetime.now()
        
        logs = []
        
        # 默认最近 30 天（备份为月任务；前端也会传 start_time）
        if not start_time:
            start_time = datetime.now() - timedelta(days=30)
        if not end_time:
            end_time = datetime.now()

        # 使用原生SQL查询（openGauss/PostgreSQL）
        from utils.scheduler.db_utils import get_opengauss_connection

        # ---- 条件构造（UNION ALL 统一排序分页）----
        # 此前两表各自 LIMIT/OFFSET 再合并，翻页会重复/遗漏。
        op_where = ["operation_time >= $1", "operation_time <= $2"]
        op_params = [start_time, end_time]
        idx = 3
        if category:
            op_where.append(f"category::text = ${idx}")
            op_params.append(category); idx += 1
        if operation_type:
            op_where.append(f"operation_type = ${idx}::operationtype")
            op_params.append(operation_type); idx += 1
        if resource_type:
            op_where.append(f"resource_type = ${idx}")
            op_params.append(resource_type); idx += 1
        if user_id:
            op_where.append(f"user_id = ${idx}")
            op_params.append(user_id); idx += 1
        if level:
            _lv = level.lower()
            if _lv in ('error', 'critical'):
                op_where.append("success = false")
            elif _lv == 'warning':
                op_where.append("false")      # 操作日志没有 warning 级别
            elif _lv == 'info':
                op_where.append("success = true")

        sys_where = [f"log_time >= ${idx}", f"log_time <= ${idx + 1}"]
        sys_params = [start_time, end_time]
        idx += 2
        if category:
            sys_where.append(f"category::text = ${idx}")
            sys_params.append(category); idx += 1
        if level:
            sys_where.append(f"log_level = ${idx}::loglevel")
            sys_params.append(level.lower()); idx += 1
        # 选择“操作类型”时只看操作日志：系统日志不参与，避免混入无关记录
        if operation_type:
            sys_where.append("false")

        all_params = op_params + sys_params
        count_params = list(all_params)
        limit_idx = idx
        offset_idx = idx + 1
        all_params.extend([limit, offset])

        union_sql = f"""
            SELECT * FROM (
                SELECT 'operation' AS src, id, operation_time AS ts,
                       CASE WHEN success THEN 'info' ELSE 'error' END AS level,
                       category::text AS category,
                       operation_type::text AS operation_type,
                       operation_name, success,
                       operation_description, error_message, result_message,
                       NULL::text AS message, NULL::text AS module,
                       NULL::text AS function, NULL::text AS stack_trace
                FROM operation_logs
                WHERE {' AND '.join(op_where)}
                UNION ALL
                SELECT 'system' AS src, id, log_time AS ts,
                       log_level::text AS level,
                       category::text AS category,
                       NULL::text AS operation_type,
                       NULL::text AS operation_name,
                       NULL::boolean AS success,
                       NULL::text AS operation_description,
                       NULL::text AS error_message,
                       NULL::text AS result_message,
                       message, module, function, stack_trace
                FROM system_logs
                WHERE {' AND '.join(sys_where)}
            ) t
            ORDER BY ts DESC
            LIMIT ${limit_idx} OFFSET ${offset_idx}
        """

        count_sql = f"""
            SELECT COUNT(*) AS c FROM (
                SELECT id FROM operation_logs WHERE {' AND '.join(op_where)}
                UNION ALL
                SELECT id FROM system_logs WHERE {' AND '.join(sys_where)}
            ) t
        """

        async with get_opengauss_connection() as conn:
            rows = await conn.fetch(union_sql, *all_params)
            count_row = await conn.fetchrow(count_sql, *count_params)
            total_count = int(count_row['c']) if count_row else 0

        logs = []
        for row in rows:
            ts = row['ts'].isoformat() if row['ts'] else None
            if row['src'] == 'operation':
                logs.append({
                    "id": row['id'],
                    "type": "operation",
                    "timestamp": ts,
                    "level": row['level'],
                    "category": row['category'] or "operation",
                    "operation_type": row['operation_type'],
                    "operation_name": row['operation_name'],
                    "success": bool(row['success']) if row['success'] is not None else True,
                    "operation_description": row['operation_description'],
                    "error_message": row['error_message'],
                    "result_message": row['result_message'],
                })
            else:
                logs.append({
                    "id": row['id'],
                    "type": "system",
                    "timestamp": ts,
                    "level": row['level'] or 'info',
                    "category": row['category'] or 'system',
                    "message": row['message'],
                    "module": row['module'],
                    "function": row['function'],
                    "stack_trace": row['stack_trace'],
                })

        return {
            "success": True,
            "total": total_count,
            "logs": logs,
            "pagination": {
                "limit": limit,
                "offset": offset,
                "has_more": total_count > offset + len(logs)
            }
        }

    except Exception as e:
        logger.error(f"获取系统日志失败: {str(e)}", exc_info=True)
        raise HTTPException(status_code=500, detail=str(e))


@router.get("/logs/export")
async def export_logs_to_excel(
    category: Optional[str] = None,
    level: Optional[str] = None,
    operation_type: Optional[str] = None,
    resource_type: Optional[str] = None,
    user_id: Optional[int] = None,
    start_time: Optional[datetime] = None,
    end_time: Optional[datetime] = None,
):
    """导出系统日志到 Excel 文件"""
    try:
        from io import BytesIO
        from openpyxl import Workbook
        from fastapi.responses import StreamingResponse
        from utils.scheduler.db_utils import get_opengauss_connection
        import json

        if not start_time:
            start_time = datetime.now() - timedelta(days=30)
        if not end_time:
            end_time = datetime.now()

        wb = Workbook()

        # --- Sheet 1: 操作日志 ---
        ws_op = wb.active
        ws_op.title = "操作日志"
        op_headers = ["时间", "用户", "操作类型", "资源类型", "资源名称", "操作描述", "成功", "错误消息", "耗时(ms)", "IP地址"]
        ws_op.append(op_headers)

        async with get_opengauss_connection() as conn:
            op_where = ["operation_time >= $1", "operation_time <= $2"]
            op_params = [start_time, end_time]
            idx = 3
            if category:
                op_where.append(f"category = ${idx}")
                op_params.append(category)
                idx += 1
            if operation_type:
                op_where.append(f"operation_type = ${idx}::operationtype")
                op_params.append(operation_type)
                idx += 1
            if resource_type:
                op_where.append(f"resource_type = ${idx}")
                op_params.append(resource_type)
                idx += 1
            if user_id:
                op_where.append(f"user_id = ${idx}")
                op_params.append(user_id)
                idx += 1

            op_sql = f"SELECT * FROM operation_logs WHERE {' AND '.join(op_where)} ORDER BY operation_time DESC LIMIT 50000"
            op_rows = await conn.fetch(op_sql, *op_params)

            for row in op_rows:
                op_type = row.get('operation_type')
                op_type_str = op_type.value if hasattr(op_type, 'value') else str(op_type or '')
                ws_op.append([
                    row['operation_time'].isoformat() if row.get('operation_time') else '',
                    row.get('username') or '',
                    op_type_str,
                    row.get('resource_type') or '',
                    row.get('resource_name') or '',
                    row.get('operation_description') or '',
                    "是" if row.get('success', True) else "否",
                    row.get('error_message') or '',
                    row.get('duration_ms') or '',
                    row.get('ip_address') or '',
                ])

            # --- Sheet 2: 系统日志 ---
            ws_sys = wb.create_sheet("系统日志")
            sys_headers = ["时间", "级别", "分类", "消息", "模块", "函数", "任务ID", "异常类型"]
            ws_sys.append(sys_headers)

            sys_where = ["log_time >= $1", "log_time <= $2"]
            sys_params = [start_time, end_time]
            sidx = 3
            if category:
                sys_where.append(f"category::text LIKE ${sidx}")
                sys_params.append(f"{category}%")
                sidx += 1
            if level:
                sys_where.append(f"log_level = ${sidx}::loglevel")
                sys_params.append(level.lower())
                sidx += 1

            sys_sql = f"SELECT * FROM system_logs WHERE {' AND '.join(sys_where)} ORDER BY log_time DESC LIMIT 50000"
            sys_rows = await conn.fetch(sys_sql, *sys_params)

            for row in sys_rows:
                log_level = row.get('log_level')
                log_level_str = log_level.value if hasattr(log_level, 'value') else str(log_level or '')
                cat = row.get('category')
                cat_str = cat.value if hasattr(cat, 'value') else str(cat or '')
                ws_sys.append([
                    row['log_time'].isoformat() if row.get('log_time') else '',
                    log_level_str,
                    cat_str,
                    row.get('message') or '',
                    row.get('module') or '',
                    row.get('function') or '',
                    row.get('task_id') or '',
                    row.get('exception_type') or '',
                ])

        # 自动调整列宽
        for ws in [ws_op, ws_sys]:
            for col in ws.columns:
                max_length = 0
                col_letter = col[0].column_letter
                for cell in col:
                    try:
                        if cell.value:
                            max_length = max(max_length, len(str(cell.value)))
                    except Exception:
                        pass
                ws.column_dimensions[col_letter].width = min(max_length + 2, 50)

        buf = BytesIO()
        wb.save(buf)
        buf.seek(0)

        from urllib.parse import quote

        filename = f"系统日志_{datetime.now().strftime('%Y%m%d_%H%M%S')}.xlsx"
        encoded_filename = quote(filename)

        return StreamingResponse(
            buf,
            media_type="application/vnd.openxmlformats-officedocument.spreadsheetml.sheet",
            headers={
                "Content-Disposition": f"attachment; filename=\"logs.xlsx\"; filename*=UTF-8''{encoded_filename}"
            }
        )
    except Exception as e:
        logger.error(f"导出日志失败: {str(e)}", exc_info=True)
        raise HTTPException(status_code=500, detail=str(e))


@router.get("/scheduler/history")
async def get_scheduler_history(limit: int = 20, offset: int = 0):
    """获取计划任务操作历史（从 operation_logs 查询）"""
    try:
        from utils.scheduler.db_utils import get_opengauss_connection

        async with get_opengauss_connection() as conn:
            rows = await conn.fetch("""
                SELECT
                    id, operation_time, operation_name, operation_type,
                    operation_description, username, success, result_message,
                    error_message, resource_id, resource_name, category
                FROM operation_logs
                WHERE category = 'scheduler'
                   OR category LIKE 'scheduler%%'
                   OR operation_type LIKE 'scheduler%%'
                ORDER BY operation_time DESC
                LIMIT $1 OFFSET $2
            """, limit, offset)

            count_row = await conn.fetchrow("""
                SELECT COUNT(*) as total FROM operation_logs
                WHERE category = 'scheduler'
                   OR category LIKE 'scheduler%%'
                   OR operation_type LIKE 'scheduler%%'
            """)
            total = count_row["total"] if count_row else 0

            history = []
            for row in rows:
                ts = row['operation_time']
                history.append({
                    "id": row['id'],
                    "time": ts.isoformat() if ts else None,
                    "operation_name": row['operation_name'] or "",
                    "operation_type": row['operation_type'] or "",
                    "description": row['operation_description'] or "",
                    "username": row['username'] or "system",
                    "success": row['success'],
                    "result_message": row['result_message'] or "",
                    "error_message": row['error_message'] or "",
                    "resource_id": row['resource_id'] or "",
                    "resource_name": row['resource_name'] or "",
                })

            return {"success": True, "data": history, "total": total}

    except Exception as e:
        logger.error(f"获取计划任务操作历史失败: {str(e)}", exc_info=True)
        raise HTTPException(status_code=500, detail=str(e))

