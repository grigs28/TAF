#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
系统环境配置API
System Environment Configuration API
"""

import logging
from typing import Dict, Any, Optional, Union
from fastapi import APIRouter, HTTPException, Request
from pydantic import BaseModel, Field

from config.env_file_manager import get_env_manager
from config.settings import get_settings, reload_settings

logger = logging.getLogger(__name__)
router = APIRouter()


class SystemEnvConfig(BaseModel):
    """系统环境配置模型"""
    # 应用配置
    app_name: Optional[str] = Field(None, description="应用名称")
    debug: Optional[bool] = Field(None, description="调试模式")

    # Web服务配置
    web_port: Optional[int] = Field(None, description="监听端口")
    
    # 数据库配置
    db_host: Optional[str] = Field(None, description="数据库主机")
    db_port: Optional[int] = Field(None, description="数据库端口")
    db_user: Optional[str] = Field(None, description="数据库用户名")
    db_password: Optional[str] = Field(None, description="数据库密码")
    db_database: Optional[str] = Field(None, description="数据库名称")
    db_pool_size: Optional[int] = Field(None, description="连接池大小")
    db_max_overflow: Optional[int] = Field(None, description="最大溢出")
    db_query_dop: Optional[int] = Field(None, description="openGauss查询并行度(1-64，默认16)")
    
    # ITDT工具配置
    itdt_path: Optional[str] = Field(None, description="ITDT可执行文件路径")
    
    # LTFS工具配置
    ltfs_tools_dir: Optional[str] = Field(None, description="LTFS工具目录")
    ltfs_device_path: Optional[str] = Field(None, description="LTFS SCSI设备路径（如 /dev/sg2）")
    
    # 钉钉通知配置
    dingtalk_api_url: Optional[str] = Field(None, description="钉钉API地址")
    dingtalk_api_key: Optional[str] = Field(None, description="钉钉API密钥")
    dingtalk_default_phone: Optional[str] = Field(None, description="默认手机号")
    
    # 备份策略配置
    default_retention_months: Optional[int] = Field(None, description="默认保留月数")
    auto_erase_expired: Optional[bool] = Field(None, description="自动擦除过期磁带")
    max_file_size: Optional[int] = Field(None, description="最大文件大小（字节）")
    backup_compress_dir: Optional[str] = Field(None, description="压缩文件临时目录")
    quarantine_dir: Optional[str] = Field(None, description="写带失败文件隔离目录（默认 /TAF/quarantine，不占 SSD）")
    log_retention_months: Optional[int] = Field(None, description="数据库日志保留月数（1=1个月，12=1年，24=2年）")
    scan_update_interval: Optional[int] = Field(None, description="后台扫描进度更新间隔（文件数）")
    scan_engine: Optional[str] = Field(None, description="扫描引擎：nfs=优化扫描，classic=经典扫描")
    scan_log_interval_seconds: Optional[int] = Field(None, description="扫描批次进度日志的最小间隔（秒），默认60")
    scan_wait_timeout: Optional[int] = Field(None, description="等待后台扫描写入文件记录的超时时间（秒），默认300秒（5分钟）")
    # 纯内存扫描模式已强制启用（不再提供开关）
    compression_parallel_batches: Optional[int] = Field(None, description="压缩并行批次数量（默认2），预读取程序队列数为该值+1")
    compression_batches_reduction: Optional[int] = Field(None, description="扫描时减少的并行批次数（默认1），0表示不减少")
    disk_throttle_enabled: Optional[bool] = Field(None, description="磁盘限流总开关：关闭后百分比限流与危急层降级均不生效（数值保留）；空间守卫不受影响")
    disk_throttle_high_percent: Optional[float] = Field(None, description="磁盘限流上限（%），超过后压缩并行批次减半，0=禁用")
    disk_throttle_low_percent: Optional[float] = Field(None, description="磁盘限流恢复阈值（%），低于后并行批次直接恢复")
    disk_critical_batches: Optional[int] = Field(None, description="剩余空间 < N倍最大文件大小时，压缩并行批次强制降至此值，0=禁用")
    disk_check_max_wait_minutes: Optional[int] = Field(None, description="磁盘空间不足时暂停压缩的最大等待分钟数")
    disk_check_interval: Optional[int] = Field(None, description="磁盘空间不足时暂停压缩的重试间隔（秒），5-600，默认 30")
    disk_guard_enabled: Optional[bool] = Field(None, description="空间守卫总开关：关闭后不再按守卫线暂停发放新压缩组（与磁盘限流至少保留一个）")
    disk_guard_safety_factor: Optional[float] = Field(None, description="空间守卫安全系数（默认 1.2）：守卫线 = 在途组数 × 最大文件大小 × 本系数")
    group_sched_enabled: Optional[bool] = Field(None, description="大小文件组调度总开关：按组内文件数分大/小文件组、全程按配额取组、守卫拦停时只取小文件组；关闭=原 FIFO 顺序")
    group_sched_ratio: Optional[str] = Field(None, description="全程配额（大:小），如 2:1、3:1、1:1")
    group_large_threshold: Optional[int] = Field(None, description="大/小文件组分界：组内文件数低于此值为大文件组（默认 20000）")
    group_scan_defer_below: Optional[int] = Field(None, description="大组保护阈值：全程不消费文件数低于此值的大文件组（默认 5000，0=禁用）；写带喂不饱时放出")
    group_prefetch_lead_seconds: Optional[int] = Field(None, description="预分组比压缩提前启动的秒数（默认 30；0=同时启动；需小于扫描等待超时）")
    group_vegas_enabled: Optional[bool] = Field(None, description="自适应并发（Vegas）开关：关闭后并发严格按配置/磁盘水位执行，不再按 NAS 读取速率增减")
    group_vegas_beta: Optional[float] = Field(None, description="降并发阈值 β（默认 3.0，越小越敏感）：deficit 超过它时并发乘性下降")
    group_vegas_decay: Optional[float] = Field(None, description="每次降并发比例（默认 0.7，范围 0.3-0.95）")
    group_vegas_min_batches: Optional[int] = Field(None, description="自适应并发下限（默认 2）")

    # 日志配置
    log_level: Optional[str] = Field(None, description="日志级别")

    # 磁带自动格式化配置
    enable_tape_format_before_full: Optional[bool] = Field(None, description="完整备份前是否自动格式化磁带（保留卷标）")

    # SSO 单点登录配置
    yz_login_url: Optional[str] = Field(None, description="yz-login 服务地址")
    taf_callback_url: Optional[str] = Field(None, description="TAF SSO 回调地址")


@router.get("/env-config")
async def get_env_config():
    """获取系统环境配置（从.env文件读取，结合settings默认值）"""
    try:
        env_manager = get_env_manager()
        
        # 从.env文件读取配置（会自动合并settings默认值）
        env_vars = env_manager.read_env_file(include_defaults=True)
        
        # 辅助函数：解析布尔值
        def parse_bool(value: Any, default: bool = False) -> bool:
            if isinstance(value, bool):
                return value
            if isinstance(value, str):
                return value.lower() in ['true', '1', 'yes', 'on']
            return default
        
        # 辅助函数：解析整数
        def parse_int(value: Any, default: int = 0) -> int:
            if isinstance(value, int):
                return value
            if isinstance(value, str):
                try:
                    return int(value) if value else default
                except (ValueError, TypeError):
                    return default
            return default
        
        # 构建配置字典（env_vars已经包含了.env文件和settings的合并值）
        # 直接从 env_vars 读取，因为它已经合并了 .env 文件和 settings 的默认值
        # .env 文件中的值会覆盖 settings 中的默认值
        config = {
            # 应用配置
            "app_name": env_vars.get("APP_NAME", ""),
            "debug": parse_bool(env_vars.get("DEBUG"), False),

            # Web服务配置
            "web_port": parse_int(env_vars.get("WEB_PORT"), 8080),

            # 数据库配置
            "db_host": env_vars.get("DB_HOST", ""),
            "db_port": parse_int(env_vars.get("DB_PORT"), None) if env_vars.get("DB_PORT") else None,
            "db_user": env_vars.get("DB_USER", ""),
            "db_password": env_vars.get("DB_PASSWORD", ""),
            "db_database": env_vars.get("DB_DATABASE", ""),
            "db_pool_size": parse_int(env_vars.get("DB_POOL_SIZE"), 10),
            "db_max_overflow": parse_int(env_vars.get("DB_MAX_OVERFLOW"), 20),
            "db_query_dop": parse_int(env_vars.get("DB_QUERY_DOP"), 16),  # openGauss 查询并行度
            
            # ITDT工具配置
            "itdt_path": env_vars.get("ITDT_PATH", ""),

            # LTFS工具配置
            "ltfs_tools_dir": env_vars.get("LTFS_TOOLS_DIR", ""),
            "ltfs_device_path": env_vars.get("LTFS_DEVICE_PATH", "/dev/sg2"),
            
            # 钉钉通知配置
            "dingtalk_api_url": env_vars.get("DINGTALK_API_URL", ""),
            "dingtalk_api_key": env_vars.get("DINGTALK_API_KEY", ""),
            "dingtalk_default_phone": env_vars.get("DINGTALK_DEFAULT_PHONE", ""),
            
            # 备份策略配置
            "default_retention_months": parse_int(env_vars.get("DEFAULT_RETENTION_MONTHS"), 6),
            "auto_erase_expired": parse_bool(env_vars.get("AUTO_ERASE_EXPIRED"), True),
            # compression_level已移除，由压缩配置中的compression_level替代
            "max_file_size": parse_int(env_vars.get("MAX_FILE_SIZE"), 12 * 1024 * 1024 * 1024),
            "backup_compress_dir": env_vars.get("BACKUP_COMPRESS_DIR", "temp/compress"),
            "quarantine_dir": env_vars.get("QUARANTINE_DIR", "/TAF/quarantine"),
            "log_retention_months": parse_int(env_vars.get("LOG_RETENTION_MONTHS"), 6),
            "scan_update_interval": parse_int(env_vars.get("SCAN_UPDATE_INTERVAL"), 500),
            "scan_engine": env_vars.get("SCAN_ENGINE", "nfs"),
            "scan_log_interval_seconds": parse_int(env_vars.get("SCAN_LOG_INTERVAL_SECONDS"), 60),
            "scan_wait_timeout": parse_int(env_vars.get("SCAN_WAIT_TIMEOUT"), 300),
            "compression_parallel_batches": parse_int(env_vars.get("COMPRESSION_PARALLEL_BATCHES"), 3),
            "compression_batches_reduction": parse_int(env_vars.get("COMPRESSION_BATCHES_REDUCTION"), 1),
            "disk_throttle_enabled": parse_bool(env_vars.get("DISK_THROTTLE_ENABLED"), True),
            "disk_throttle_high_percent": float(env_vars.get("DISK_THROTTLE_HIGH_PERCENT", 75) or 0),
            "disk_throttle_low_percent": float(env_vars.get("DISK_THROTTLE_LOW_PERCENT", 30) or 0),
            "disk_critical_batches": parse_int(env_vars.get("DISK_CRITICAL_BATCHES"), 1),
            "disk_check_max_wait_minutes": parse_int(env_vars.get("DISK_CHECK_MAX_WAIT_MINUTES"), 120),
            "disk_check_interval": parse_int(env_vars.get("DISK_CHECK_INTERVAL"), 30),
            "disk_guard_enabled": parse_bool(env_vars.get("DISK_GUARD_ENABLED"), True),
            "group_sched_enabled": parse_bool(env_vars.get("GROUP_SCHED_ENABLED"), True),
            "group_sched_ratio": env_vars.get("GROUP_SCHED_NORMAL_RATIO", "2:1") or "2:1",
            "group_large_threshold": parse_int(env_vars.get("GROUP_LARGE_FILE_COUNT_THRESHOLD"), 20000),
            "group_scan_defer_below": parse_int(env_vars.get("GROUP_SCAN_DEFER_BELOW"), 5000),
            "group_prefetch_lead_seconds": parse_int(env_vars.get("GROUP_PREFETCH_LEAD_SECONDS"), 30),
            "group_vegas_enabled": parse_bool(env_vars.get("GROUP_VEGAS_ENABLED"), True),
            "group_vegas_beta": float(env_vars.get("GROUP_VEGAS_BETA", 3.0) or 3.0),
            "group_vegas_decay": float(env_vars.get("GROUP_VEGAS_DECAY", 0.7) or 0.7),
            "group_vegas_min_batches": parse_int(env_vars.get("GROUP_VEGAS_MIN_BATCHES"), 2),
            "disk_guard_safety_factor": float(env_vars.get("DISK_GUARD_SAFETY_FACTOR", 1.2) or 1.2),

            # 日志配置
            "log_level": env_vars.get("LOG_LEVEL", "INFO"),

            # 磁带自动格式化配置
            "enable_tape_format_before_full": parse_bool(env_vars.get("ENABLE_TAPE_FORMAT_BEFORE_FULL"), True),

            # SSO 单点登录配置
            "yz_login_url": env_vars.get("YZ_LOGIN_URL", ""),
            "taf_callback_url": env_vars.get("TAF_CALLBACK_URL", ""),
        }
        
        return {
            "success": True,
            "config": config
        }
    
    except Exception as e:
        logger.error(f"获取环境配置失败: {str(e)}", exc_info=True)
        raise HTTPException(status_code=500, detail=f"获取环境配置失败: {str(e)}")


@router.put("/env-config")
async def update_env_config(config: SystemEnvConfig, request: Request):
    """更新系统环境配置（写入.env文件）"""
    try:
        env_manager = get_env_manager()
        
        # 构建更新字典（只包含非None的值）
        updates: Dict[str, str] = {}
        
        # 应用配置
        if config.app_name is not None:
            updates["APP_NAME"] = config.app_name
        if config.debug is not None:
            updates["DEBUG"] = str(config.debug).lower()

        # Web服务配置
        if config.web_port is not None:
            updates["WEB_PORT"] = str(config.web_port)
        
        # 数据库配置
        if config.db_host is not None:
            updates["DB_HOST"] = config.db_host
        if config.db_port is not None:
            updates["DB_PORT"] = str(config.db_port)
        if config.db_user is not None:
            updates["DB_USER"] = config.db_user
        if config.db_password is not None:
            updates["DB_PASSWORD"] = config.db_password
        if config.db_database is not None:
            updates["DB_DATABASE"] = config.db_database
        if config.db_pool_size is not None:
            updates["DB_POOL_SIZE"] = str(config.db_pool_size)
        if config.db_max_overflow is not None:
            updates["DB_MAX_OVERFLOW"] = str(config.db_max_overflow)
        if config.db_query_dop is not None:
            # 验证 query_dop 范围 (1-64)
            if config.db_query_dop < 1 or config.db_query_dop > 64:
                raise ValueError("DB_QUERY_DOP 必须在 1-64 之间")
            updates["DB_QUERY_DOP"] = str(config.db_query_dop)
        
        # ITDT工具配置
        if config.itdt_path is not None:
            updates["ITDT_PATH"] = config.itdt_path

        # LTFS工具配置
        if config.ltfs_tools_dir is not None:
            updates["LTFS_TOOLS_DIR"] = config.ltfs_tools_dir
        if config.ltfs_device_path is not None:
            updates["LTFS_DEVICE_PATH"] = config.ltfs_device_path
        
        # 钉钉通知配置
        if config.dingtalk_api_url is not None:
            updates["DINGTALK_API_URL"] = config.dingtalk_api_url
        if config.dingtalk_api_key is not None:
            updates["DINGTALK_API_KEY"] = config.dingtalk_api_key
        if config.dingtalk_default_phone is not None:
            updates["DINGTALK_DEFAULT_PHONE"] = config.dingtalk_default_phone
        
        # 备份策略配置
        if config.default_retention_months is not None:
            updates["DEFAULT_RETENTION_MONTHS"] = str(config.default_retention_months)
        if config.auto_erase_expired is not None:
            updates["AUTO_ERASE_EXPIRED"] = str(config.auto_erase_expired).lower()
        # compression_level已移除，由压缩配置中的compression_level替代
        if config.max_file_size is not None:
            updates["MAX_FILE_SIZE"] = str(config.max_file_size)
        if config.backup_compress_dir is not None:
            updates["BACKUP_COMPRESS_DIR"] = config.backup_compress_dir
        if config.quarantine_dir is not None:
            _qdir = str(config.quarantine_dir).strip()
            if not _qdir:
                raise ValueError("隔离目录不能为空")
            if not _qdir.startswith('/'):
                raise ValueError("隔离目录必须是绝对路径（以 / 开头）")
            updates["QUARANTINE_DIR"] = _qdir
        if config.log_retention_months is not None:
            if config.log_retention_months < 1 or config.log_retention_months > 120:
                raise ValueError("日志保留时间必须在 1~120 个月之间")
            updates["LOG_RETENTION_MONTHS"] = str(config.log_retention_months)
        if config.scan_update_interval is not None:
            updates["SCAN_UPDATE_INTERVAL"] = str(config.scan_update_interval)
        if config.scan_engine is not None:
            if config.scan_engine.lower() not in ("nfs", "classic"):
                raise ValueError("SCAN_ENGINE 仅支持 nfs 或 classic")
            updates["SCAN_ENGINE"] = config.scan_engine.lower()
        if config.scan_log_interval_seconds is not None:
            if config.scan_log_interval_seconds < 1:
                raise ValueError("SCAN_LOG_INTERVAL_SECONDS 必须大于等于 1 秒")
            updates["SCAN_LOG_INTERVAL_SECONDS"] = str(config.scan_log_interval_seconds)
        if config.scan_wait_timeout is not None:
            updates["SCAN_WAIT_TIMEOUT"] = str(config.scan_wait_timeout)
        if config.compression_parallel_batches is not None:
            # 验证并行批次数范围（至少为1）
            if config.compression_parallel_batches < 1:
                raise ValueError("COMPRESSION_PARALLEL_BATCHES 必须大于等于 1")
            updates["COMPRESSION_PARALLEL_BATCHES"] = str(config.compression_parallel_batches)
        if config.compression_batches_reduction is not None:
            # 验证减少批次数范围（至少为0）
            if config.compression_batches_reduction < 0:
                raise ValueError("COMPRESSION_BATCHES_REDUCTION 必须大于等于 0")
            updates["COMPRESSION_BATCHES_REDUCTION"] = str(config.compression_batches_reduction)

        if config.disk_throttle_high_percent is not None or config.disk_throttle_low_percent is not None:
            high = config.disk_throttle_high_percent
            low = config.disk_throttle_low_percent
            if high is not None and not (0 <= high <= 100):
                raise ValueError("DISK_THROTTLE_HIGH_PERCENT 必须在 0-100 之间")
            if low is not None and not (0 <= low <= 100):
                raise ValueError("DISK_THROTTLE_LOW_PERCENT 必须在 0-100 之间")
            # 校验迟滞区间：high 必须大于 low（high=0 为禁用除外）
            if high is not None and high > 0:
                current_low = low if low is not None else float(env_manager.read_env_file().get("DISK_THROTTLE_LOW_PERCENT", 30) or 0)
                if current_low >= high:
                    raise ValueError(f"恢复阈值（{current_low}%）必须小于限流上限（{high}%），否则会在边界反复切换")
            if high is not None:
                updates["DISK_THROTTLE_HIGH_PERCENT"] = str(high)
            if low is not None:
                updates["DISK_THROTTLE_LOW_PERCENT"] = str(low)

        if config.disk_critical_batches is not None:
            if config.disk_critical_batches < 0 or config.disk_critical_batches > 64:
                raise ValueError("DISK_CRITICAL_BATCHES 必须在 0-64 之间（0=禁用）")
            updates["DISK_CRITICAL_BATCHES"] = str(config.disk_critical_batches)
        if config.disk_throttle_enabled is not None:
            updates["DISK_THROTTLE_ENABLED"] = "true" if config.disk_throttle_enabled else "false"
        if config.disk_guard_enabled is not None:
            updates["DISK_GUARD_ENABLED"] = "true" if config.disk_guard_enabled else "false"
        if config.group_sched_enabled is not None:
            updates["GROUP_SCHED_ENABLED"] = "true" if config.group_sched_enabled else "false"
        if config.group_sched_ratio is not None:
            # 格式校验：N:M（正整数），与 _parse_ratio 口径一致
            _r = str(config.group_sched_ratio).strip()
            _parts = _r.split(':')
            if (len(_parts) != 2
                    or not _parts[0].strip().isdigit()
                    or not _parts[1].strip().isdigit()
                    or int(_parts[0]) < 0 or int(_parts[1]) < 0
                    or int(_parts[0]) + int(_parts[1]) <= 0):
                raise ValueError(f"全程配额格式必须为 N:M（正整数），当前: {config.group_sched_ratio}")
            updates["GROUP_SCHED_NORMAL_RATIO"] = _r.replace(' ', '')
        if config.group_large_threshold is not None:
            if config.group_large_threshold < 100:
                raise ValueError("大/小文件组分界必须 ≥ 100")
            updates["GROUP_LARGE_FILE_COUNT_THRESHOLD"] = str(config.group_large_threshold)
        if config.group_scan_defer_below is not None:
            if config.group_scan_defer_below < 0:
                raise ValueError("大组保护阈值必须 ≥ 0（0=禁用）")
            updates["GROUP_SCAN_DEFER_BELOW"] = str(config.group_scan_defer_below)
        if config.group_prefetch_lead_seconds is not None:
            if config.group_prefetch_lead_seconds < 0 or config.group_prefetch_lead_seconds > 3600:
                raise ValueError("预分组提前秒数必须在 0-3600 之间（0=同时启动）")
            updates["GROUP_PREFETCH_LEAD_SECONDS"] = str(config.group_prefetch_lead_seconds)
        if config.group_vegas_enabled is not None:
            updates["GROUP_VEGAS_ENABLED"] = "true" if config.group_vegas_enabled else "false"
        if config.group_vegas_beta is not None:
            if config.group_vegas_beta < 1.0 or config.group_vegas_beta > 20.0:
                raise ValueError("降并发阈值 β 必须在 1.0-20.0 之间")
            updates["GROUP_VEGAS_BETA"] = str(config.group_vegas_beta)
        if config.group_vegas_decay is not None:
            if config.group_vegas_decay < 0.3 or config.group_vegas_decay > 0.95:
                raise ValueError("降并发比例必须在 0.3-0.95 之间")
            updates["GROUP_VEGAS_DECAY"] = str(config.group_vegas_decay)
        if config.group_vegas_min_batches is not None:
            if config.group_vegas_min_batches < 1 or config.group_vegas_min_batches > 64:
                raise ValueError("自适应并发下限必须在 1-64 之间")
            updates["GROUP_VEGAS_MIN_BATCHES"] = str(config.group_vegas_min_batches)

        # 互斥校验：磁盘限流与空间守卫**至少保留一个**——两者同时关闭等于放弃全部磁盘保护，
        # SSD 可能被在途压缩产出写满。部分更新时需与 .env 中的现有值合并后再判断。
        if config.disk_throttle_enabled is not None or config.disk_guard_enabled is not None:
            def _as_bool(v: Any, default: bool) -> bool:
                if isinstance(v, bool):
                    return v
                if isinstance(v, str):
                    return v.strip().lower() in ('true', '1', 'yes', 'on')
                return default

            _cur = env_manager.read_env_file()
            cur_throttle = _as_bool(_cur.get("DISK_THROTTLE_ENABLED"), True)
            cur_guard = _as_bool(_cur.get("DISK_GUARD_ENABLED"), True)
            new_throttle = (config.disk_throttle_enabled
                            if config.disk_throttle_enabled is not None else cur_throttle)
            new_guard = (config.disk_guard_enabled
                         if config.disk_guard_enabled is not None else cur_guard)
            if not new_throttle and not new_guard:
                raise ValueError("磁盘限流与空间守卫不能同时关闭，至少保留一个作为磁盘保护")

        if config.disk_check_max_wait_minutes is not None:
            if config.disk_check_max_wait_minutes < 1:
                raise ValueError("DISK_CHECK_MAX_WAIT_MINUTES 必须大于等于 1 分钟")
            updates["DISK_CHECK_MAX_WAIT_MINUTES"] = str(config.disk_check_max_wait_minutes)

        if config.disk_check_interval is not None:
            if config.disk_check_interval < 5 or config.disk_check_interval > 600:
                raise ValueError("DISK_CHECK_INTERVAL 必须在 5-600 秒之间")
            updates["DISK_CHECK_INTERVAL"] = str(config.disk_check_interval)

        if config.disk_guard_safety_factor is not None:
            if config.disk_guard_safety_factor < 1.0 or config.disk_guard_safety_factor > 3.0:
                raise ValueError("DISK_GUARD_SAFETY_FACTOR 必须在 1.0-3.0 之间（小于 1 会让在途产出越界）")
            updates["DISK_GUARD_SAFETY_FACTOR"] = str(config.disk_guard_safety_factor)


        # 日志配置
        if config.log_level is not None:
            updates["LOG_LEVEL"] = config.log_level

        # 磁带自动格式化配置
        if config.enable_tape_format_before_full is not None:
            updates["ENABLE_TAPE_FORMAT_BEFORE_FULL"] = str(config.enable_tape_format_before_full).lower()

        # SSO 单点登录配置
        if config.yz_login_url is not None:
            updates["YZ_LOGIN_URL"] = config.yz_login_url
        if config.taf_callback_url is not None:
            updates["TAF_CALLBACK_URL"] = config.taf_callback_url
        
        # 更新数据库URL（如果数据库配置有变化）
        if any(key in updates for key in ["DB_HOST", "DB_PORT", "DB_USER", "DB_PASSWORD", "DB_DATABASE"]):
            # 从当前配置读取数据库类型
            current_settings = get_settings()
            db_url = current_settings.DATABASE_URL
            
            # 确定数据库类型
            if db_url.startswith("sqlite"):
                # SQLite不需要更新URL
                pass
            elif db_url.startswith("opengauss://") or db_url.startswith("postgresql://"):
                # 构建新的数据库URL
                db_host = updates.get("DB_HOST", current_settings.DB_HOST or "localhost")
                db_port = updates.get("DB_PORT", str(current_settings.DB_PORT or 5432))
                db_user = updates.get("DB_USER", current_settings.DB_USER or "username")
                db_password = updates.get("DB_PASSWORD", current_settings.DB_PASSWORD or "password")
                db_database = updates.get("DB_DATABASE", current_settings.DB_DATABASE or "backup_db")
                
                # 确定协议（opengauss或postgresql）
                if db_url.startswith("opengauss://"):
                    db_protocol = "opengauss://"
                else:
                    db_protocol = "postgresql://"
                
                new_db_url = f"{db_protocol}{db_user}:{db_password}@{db_host}:{db_port}/{db_database}"
                updates["DATABASE_URL"] = new_db_url
        
        # 写入.env文件
        success = env_manager.write_env_file(updates, backup=True)
        
        if not success:
            raise HTTPException(status_code=500, detail="写入环境配置失败")
        
        logger.info(f"环境配置已更新: {len(updates)} 个配置项")
        
        # 重新加载配置，使后续调用get_settings()能获取最新配置
        from config.settings import reload_settings
        reload_settings()
        logger.info("配置已重新加载，新配置将立即生效")

        # 日志保留时间变更后立即清理一次（后台执行，不阻塞保存响应）
        if config.log_retention_months is not None:
            try:
                import asyncio as _aio
                from utils.log_utils import cleanup_expired_logs as _clean_logs
                _aio.create_task(_clean_logs(int(config.log_retention_months)))
            except Exception as _clean_err:
                logger.warning(f"触发日志保留清理失败: {_clean_err}")

        # 如果修改了日志级别，立即动态更新 root logger
        if 'LOG_LEVEL' in updates:
            try:
                new_level = getattr(logging, updates['LOG_LEVEL'].upper())
                root_logger = logging.getLogger()
                root_logger.setLevel(new_level)
                for handler in root_logger.handlers:
                    # 更新所有 handler 的级别（保留 WARNING+ 的专用 handler）
                    if handler.level not in (logging.WARNING, logging.ERROR, logging.CRITICAL):
                        handler.setLevel(new_level)
                logger.info(f"日志级别已动态更新为: {updates['LOG_LEVEL']}")
            except Exception as level_err:
                logger.warning(f"动态更新日志级别失败: {level_err}")

        # 记录配置修改到数据库
        try:
            from utils.log_utils import log_operation
            from models.system_log import OperationType
            # 排除敏感字段
            safe_updates = {k: v for k, v in updates.items()
                          if k not in ('DB_PASSWORD', 'DINGTALK_API_KEY', 'SECRET_KEY', 'SMB_PASSWORD')}
            await log_operation(
                operation_type=OperationType.CONFIG,
                resource_type="system",
                resource_name="env_config",
                operation_name="修改系统配置",
                operation_description=f"更新了 {len(updates)} 个配置项",
                category="system",
                success=True,
                new_values=safe_updates,
                changed_fields=list(updates.keys()),
                ip_address=request.client.host if request.client else None,
            )
        except Exception as log_err:
            logger.warning(f"记录配置变更日志失败: {log_err}")

        return {
            "success": True,
            "message": "环境配置已更新并重新加载，新配置将立即生效",
            "updated_keys": list(updates.keys())
        }
    
    except Exception as e:
        logger.error(f"更新环境配置失败: {str(e)}", exc_info=True)
        raise HTTPException(status_code=500, detail=f"更新环境配置失败: {str(e)}")

