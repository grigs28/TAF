#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
备份引擎拆分计划
Backup Engine Refactoring Plan

将 backup_engine.py (3141行) 按功能拆分为以下模块：

1. backup/utils.py - 工具函数
   - normalize_volume_label()
   - extract_label_year_month()
   - format_bytes()
   - calculate_file_checksum()

2. backup/file_scanner.py - 文件扫描
   - FileScanner 类
   - scan_source_files_streaming() - 流式扫描
   - scan_source_files() - 批量扫描
   - get_file_info() - 获取文件信息
   - should_exclude_file() - 排除规则检查

3. backup/compressor.py - 压缩处理
   - Compressor 类
   - group_files_for_compression() - 文件分组
   - compress_file_group() - 压缩文件组

4. backup/backup_db.py - 数据库操作
   - BackupDB 类
   - create_backup_set() - 创建备份集
   - finalize_backup_set() - 完成备份集
   - save_backup_files_to_db() - 保存文件信息
   - update_scan_progress() - 更新扫描进度
   - update_task_status() - 更新任务状态
   - get_task_status() - 获取任务状态

5. backup/tape_handler.py - 磁带处理
   - TapeHandler 类
   - get_current_drive_tape() - 获取当前驱动器磁带
   - write_to_tape_drive() - 写入磁带

6. backup/backup_engine.py - 主引擎（保留核心逻辑）
   - BackupEngine 类
   - create_backup_task() - 创建任务
   - execute_backup_task() - 执行任务
   - _perform_backup() - 执行备份流程
   - cancel_task() - 取消任务
   - initialize() - 初始化
   - set_dependencies() - 设置依赖
   - add_progress_callback() - 添加进度回调
   - _get_notification_events() - 获取通知事件
   - _get_backup_policy_parameters() - 获取备份策略
   - _notify_progress() - 通知进度

注意：拆分时需要保持向后兼容，确保现有代码可以正常工作。
