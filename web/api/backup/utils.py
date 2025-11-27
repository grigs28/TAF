#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
备份管理API - 工具函数
Backup Management API - Utility Functions
"""

import logging

import re
from typing import Dict, Any, Optional
from fastapi import Request

logger = logging.getLogger(__name__)

# 阶段流程定义
STAGE_FLOW_DEFINITION = [
    ("scan", "扫描文件"),
    ("compress", "压缩/打包"),
    ("copy", "写入磁带"),
    ("finalize", "完成"),
]
STAGE_INDEX = {code: idx for idx, (code, _) in enumerate(STAGE_FLOW_DEFINITION)}
STAGE_LABELS = {
    "scan": "扫描文件",
    "compress": "压缩/打包",
    "copy": "写入磁带",
    "finalize": "完成备份",
    "waiting": "等待批次",
    "cancelled": "任务已取消",
    "failed": "任务失败",
    "format": "格式化磁带",
}
OP_STATUS_PATTERN = re.compile(r'\[([^\]]+)\]')
OP_STAGE_KEYWORDS = [
    ("扫描", "scan"),
    ("准备压缩", "compress"),
    ("压缩", "compress"),
    ("等待下一批", "compress"),
    ("复制", "copy"),
    ("写入", "copy"),
    ("完成", "finalize"),
    ("格式化", "format"),
    ("取消", "cancelled"),
    ("失败", "failed"),
]


def _normalize_status_value(value: Any) -> str:
    """标准化状态值"""
    if value is None:
        return ""
    
    # 如果是枚举类型，获取其value属性
    if hasattr(value, "value"):
        result = str(value.value).lower()
        return result
    
    # 如果是字符串，直接转为小写
    if isinstance(value, str):
        return value.lower()
    
    # 其他类型，转换为字符串并转为小写
    return str(value).lower()


def _build_stage_info(description: Optional[str], scan_status: Optional[str], status_value: str, operation_stage: Optional[str] = None) -> Dict[str, Any]:
    """构建阶段信息
    
    Args:
        description: 任务描述（包含操作状态信息）
        scan_status: 扫描状态
        status_value: 任务状态值
        operation_stage: 操作阶段代码（优先使用，如果提供则直接使用，不再从description解析）
    """
    # 优先使用数据库中的 operation_stage 字段
    stage_code = operation_stage
    
    desc = description or ""
    matches = OP_STATUS_PATTERN.findall(desc)
    operation_status = matches[-1] if matches else None
    
    # 提取完整的操作状态，包括方括号后的进度信息
    # 例如: "[压缩文件中...] 814/1637 个文件 (49.7%)" -> "压缩文件中 814/1637 个文件 (49.7%)"
    if operation_status:
        operation_status = operation_status.replace("...", "")
        # 检查方括号后是否有进度信息
        last_bracket_pos = desc.rfind(']')
        if last_bracket_pos >= 0 and last_bracket_pos + 1 < len(desc):
            remaining_text = desc[last_bracket_pos + 1:].strip()
            if remaining_text:
                # 如果方括号后有文本，将其追加到 operation_status
                operation_status = operation_status + " " + remaining_text

    # 特殊处理：如果操作状态包含进度信息（如"123/500 个文件 (24.6%)"），优先使用这个状态
    # 这可以覆盖"初始化压缩引擎"等初始状态
    import re
    progress_match = re.search(r'\[压缩文件中[^\]]*\]\s*(\d+/\d+\s*个文件\s*\([\d.]+%\))', desc)
    if progress_match and stage_code == 'compress':
        operation_status = f"压缩文件中 {progress_match[1]}"
        logger.info(f"[状态解析] 从description解析到压缩进度: {progress_match[1]}, 操作状态: {operation_status}")
    elif stage_code == 'compress':
        logger.debug(f"[状态解析] 压缩阶段但未找到进度信息，原始description: {desc}")

    # 如果没有从数据库获取到 stage_code，则从 operation_status 中解析
    if not stage_code and operation_status:
        lowered = operation_status.lower()
        for keyword, code in OP_STAGE_KEYWORDS:
            if keyword.lower() in lowered:
                stage_code = code
                break

    normalized_status = (status_value or "").lower()
    normalized_scan = (scan_status or "").lower()

    if not stage_code:
        if normalized_status in ("failed",):
            stage_code = "failed"
            operation_status = operation_status or "任务失败"
        elif normalized_status in ("cancelled",):
            stage_code = "cancelled"
            operation_status = operation_status or "任务已取消"
        elif normalized_status in ("completed",):
            stage_code = "finalize"
            operation_status = operation_status or "备份完成"
        elif normalized_status in ("running",):
            stage_code = normalized_scan if normalized_scan in STAGE_INDEX else "scan"
            operation_status = operation_status or "正在处理"
        else:
            stage_code = "waiting"
            operation_status = operation_status or "等待开始"

    stage_label = STAGE_LABELS.get(stage_code, "未知阶段")
    stage_steps = []

    if stage_code and stage_code in STAGE_INDEX:
        current_index = STAGE_INDEX[stage_code]
        for idx, (code, label) in enumerate(STAGE_FLOW_DEFINITION):
            step = {
                "code": code,
                "label": label,
                "status": "completed" if idx < current_index else ("active" if idx == current_index else "pending")
            }
            stage_steps.append(step)

    return {
        "operation_status": operation_status,
        "operation_stage": stage_code,
        "operation_stage_label": stage_label,
        "stage_steps": stage_steps
    }


def get_system_instance(request: Request):
    """获取系统实例"""
    return request.app.state.system

