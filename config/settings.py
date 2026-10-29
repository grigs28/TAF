#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
系统配置管理模块
System Configuration Management Module
"""

import os
import re
from pathlib import Path
from typing import Optional
from pydantic import Field, field_validator

try:
    from pydantic_settings import BaseSettings
except ImportError:
    # 如果 pydantic-settings 没有安装，提供错误信息
    raise ImportError(
        "需要安装 pydantic-settings 包。请运行: pip install pydantic-settings\n"
        "Pydantic v2 将 BaseSettings 移动到了单独的包中。"
    )


def _read_version_from_changelog() -> str:
    """从CHANGELOG.md读取最新版本号"""
    try:
        changelog_path = Path("CHANGELOG.md")
        if changelog_path.exists():
            with open(changelog_path, "r", encoding="utf-8") as f:
                content = f.read()
                # 匹配 ## [版本号] 格式
                match = re.search(r'^##\s+\[([\d.]+)\]', content, re.MULTILINE)
                if match:
                    return match.group(1)
    except Exception:
        pass
    # 默认版本
    return "0.0.2"


class Settings(BaseSettings):
    """系统配置类"""

    # 应用配置
    APP_NAME: str = "企业级磁带备份系统"
    APP_VERSION: str = Field(default_factory=_read_version_from_changelog, description="版本号从CHANGELOG.md自动读取")
    DEBUG: bool = False
    WEB_PORT: int = 8080

    # 数据库配置
    DATABASE_URL: str = "opengauss://username:password@localhost:5432/backup_db"
    # 连接池大小：进一步调大，适应扫描 + 压缩 + 其他任务的并发
    DB_POOL_SIZE: int = 40          # 基础连接数（可通过 .env 覆盖）
    DB_MAX_OVERFLOW: int = 80       # 允许的额外临时连接数
    DB_POOL_TIMEOUT: float = 30.0  # 连接池连接超时时间（秒）
    DB_COMMAND_TIMEOUT: float = 60.0  # 命令超时时间（秒）
    DB_ACQUIRE_TIMEOUT: float = 10.0  # 从连接池获取连接的超时时间（秒）
    DB_MAX_INACTIVE_CONNECTION_LIFETIME: float = 600.0  # 非活跃连接的最大生命周期（秒，默认10分钟）
    DB_QUERY_DOP: int = 16  # openGauss 查询并行度（1-64，默认16，用于优化查询性能）
    OG_HEARTBEAT_INTERVAL: int = 30  # openGauss 心跳间隔（秒）
    OG_HEARTBEAT_TIMEOUT: float = 5.0  # 单次心跳超时时间
    OG_OPERATION_TIMEOUT: float = 45.0  # 默认数据库操作超时
    OG_OPERATION_WARN_THRESHOLD: float = 5.0  # 操作耗时告警阈值
    OG_OPERATION_FAILURE_THRESHOLD: int = 3  # 连续失败次数触发告警
    OG_MAX_HEARTBEAT_FAILURES: int = 3  # 心跳失败重试次数
    OG_ALERT_COOLDOWN: int = 600  # 告警冷却时间（秒）

    # 数据库配置（兼容格式）
    DB_HOST: Optional[str] = "localhost"
    DB_PORT: Optional[int] = 5432
    DB_USER: Optional[str] = "username"
    DB_PASSWORD: Optional[str] = "password"
    DB_DATABASE: Optional[str] = "backup_db"

    @field_validator('DB_PORT', mode='before')
    @classmethod
    def validate_db_port(cls, v):
        """验证DB_PORT，将空字符串转换为None"""
        if v == '':
            return None
        return v

    # 安全配置
    SECRET_KEY: str = "your-jwt-secret-key-change-in-production"
    ALGORITHM: str = "HS256"
    ACCESS_TOKEN_EXPIRE_MINUTES: int = 30

    # 磁带配置 (Linux)
    DEFAULT_BLOCK_SIZE: int = 262144  # 256KB
    MAX_VOLUME_SIZE: int = 322122547200  # 300GB
    # 磁带设备路径（用于 mt 命令，Linux 下为 /dev/nst0）
    TAPE_DRIVE_LETTER: str = "/dev/nst0"
    # 是否在完整备份前自动格式化磁带
    ENABLE_TAPE_FORMAT_BEFORE_FULL: bool = True
    # ITDT 接口配置
    ITDT_PATH: str = "/usr/local/itdt/itdt"

    # LTFS 工具目录配置
    LTFS_TOOLS_DIR: str = "/usr/local/ltfs"
    MKLTFS_PATH: str = "/usr/local/bin/mkltfs"  # mkltfs 命令路径

    # 压缩配置
    COMPRESSION_LEVEL: int = 9
    SOLID_BLOCK_SIZE: int = 67108864  # 64MB
    MAX_FILE_SIZE: int = 12 * 1024 * 1024 * 1024  # 12GB (默认值，可通过.env中的MAX_FILE_SIZE覆盖)
    COMPRESSION_DICTIONARY_SIZE: str = "256m"  # 7-Zip字典大小（固定256M）
    COMPRESS_DIRECTLY_TO_TAPE: bool = True  # 是否直接压缩到磁带机（默认True，跳过temp/final目录）

    # 磁盘空间检查配置
    DISK_GUARD_ENABLED: bool = True  # 空间守卫总开关：关闭则不再按守卫线暂停发放新压缩组（与磁盘限流至少保留一个）
    DISK_GUARD_SAFETY_FACTOR: float = 1.2  # 空间守卫安全系数：守卫线 = 在途组剩余待产出 × 本系数（1.0~3.0）
    DISK_GUARD_COUNT_PRODUCED: bool = True  # 守卫需求按"剩余待产出"计算：在途组已落地字节从源大小上界中扣除（关闭=纯源大小口径）

    # 大小文件组动态调度（计划04 / 计划05）
    GROUP_SCHED_ENABLED: bool = True  # 总开关：false 回退到原来的单队列 FIFO 顺序    GROUP_LARGE_FILE_COUNT_THRESHOLD: int = 20000  # 组内文件数 < 阈值 = 大文件组（带宽主导）
    GROUP_SCHED_SCAN_RATIO: str = "2:1"   # （已弃用，保留兼容）扫描期配额
    GROUP_SCHED_NORMAL_RATIO: str = "2:1"  # 全程大:小 配额（计划05：不再区分扫描期，默认 2:1）

    # 归档清单落库（压缩完成时把"归档 → 文件列表"写入 tape_archive_contents）
    # 开启后：恢复检索全量秒开、磁带验证跳过解压建缓存；
    # 代价：每次备份多写约 1 帧 GB 级数据（882 万行/12TB 规模约 1.7GB），
    # 但写在每组压缩完成后，耗时被压缩时间掩盖，失败不阻断备份。
    ARCHIVE_MANIFEST_PERSIST: bool = False
    GROUP_SCAN_DEFER_BELOW: int = 5000  # 大组保护阈值：全程不消费"文件数<该值"的组（写带喂不饱时除外），0=禁用
    GROUP_MAX_FILES_PER_GROUP: int = 50000  # 组内文件数上限（0=禁用）：与大小上限同时生效，谁先到就封组
    GROUP_STARVE_RELEASE_SEC: int = 30  # 写带喂不饱（final 为空）持续多少秒后，才放保护区组出来
    GROUP_GUARD_S_PER_L: int = 4  # 磁盘守卫触发时：每取 N 个 S 组，至少保留/取 1 个 L 组（0=纯 S）
    GROUP_PREFETCH_LEAD_SECONDS: int = 30  # 预分组比压缩提前启动的秒数（0=同时启动；需小于扫描等待超时）
    # 自适应并发（计划05，Vegas 变体，信号=组级聚合读取速率）
    GROUP_VEGAS_ENABLED: bool = True    # 总开关：关闭则并发完全由配置/磁盘水位决定
    GROUP_VEGAS_SIGNAL_SOURCE: str = "internal"  # 信号源：internal=组级读取速率(deficit)；nas_api=79 disk_api read(v2状态机)
    GROUP_VEGAS_ALPHA: float = 1.0      # deficit < α → 并发 +1（缓慢上探）
    GROUP_VEGAS_BETA: float = 3.0       # deficit > β → 并发 ×DECAY（乘性下降）；> 2β 触发快速闸（暂停发新组一个窗口）
    GROUP_VEGAS_DECAY: float = 0.7      # 乘性下降因子
    GROUP_VEGAS_MIN_BATCHES: int = 2    # 自适应并发下限
    GROUP_VEGAS_BASE_RESET_SEC: int = 600  # 速率基准重置周期（秒）：base = max(base×0.9, ewma)
    # NAS 磁盘压力 API（如群晖 disk_api.py :8090）——Vegas 的外部压力信号源
    NAS_API_URL: str = ""               # 完整地址（如 http://192.168.0.79:8090/api/disk-stats?simple=1），空=不采集
    NAS_API_KEY: str = ""               # 鉴权 key（请求带 X-API-Key 头；服务端未启用鉴权时留空）
    # 守卫状态切换防抖跟随 DISK_CHECK_INTERVAL（空间不足检查间隔），不再单独配置
    DISK_CHECK_INTERVAL: int = 30  # 磁盘空间检查间隔（秒）
    DISK_CHECK_MAX_WAIT_MINUTES: int = 120  # 磁盘空间不足时暂停压缩的最大等待分钟数（超时抛错）
    DISK_CHECK_MIN_FREE_MULTIPLIER: int = 3  # 最小剩余空间倍数（3 * MAX_FILE_SIZE）

    # 计划任务配置
    SCHEDULER_ENABLED: bool = True
    MONTHLY_BACKUP_CRON: str = "0 2 1 * *"  # 每月1号02:00
    RETENTION_CHECK_CRON: str = "0 3 * * *"  # 每天03:00

    # 系统配置
    DEFAULT_RETENTION_MONTHS: int = 6
    AUTO_ERASE_EXPIRED: bool = True

    # 日志配置
    LOG_LEVEL: str = "INFO"
    LOG_FILE: str = "logs/application.log"
    LOG_MAX_BYTES: int = 10 * 1024 * 1024  # 日志文件最大大小（默认10MB）
    LOG_BACKUP_COUNT: int = 30  # 日志文件备份数量

    # 钉钉通知配置
    DINGTALK_API_URL: str = "http://localhost:5555"
    DINGTALK_API_KEY: str = "your-dingtalk-api-key"
    DINGTALK_DEFAULT_PHONE: str = "13800000000"

    # 微信通知配置（企业微信机器人）
    WECHAT_WEBHOOK_URL: str = ""  # 企业微信机器人 Webhook URL
    WECHAT_ENABLED: bool = False  # 是否启用微信通知
    WECHAT_REPORT_INTERVAL: int = 30  # 备份进度汇报间隔（分钟）

    # Syslog 转发配置
    SYSLOG_ENABLED: bool = False  # 是否启用 syslog 转发
    SYSLOG_HOST: str = "192.168.0.14"  # Syslog 服务器地址
    SYSLOG_PORT: int = 515  # Syslog 服务器端口（Vector JSON 端口）
    SYSLOG_LEVEL: str = "WARNING"  # 转发级别：DEBUG/INFO/WARNING/ERROR/CRITICAL

    # YZ SSO 登录配置
    YZ_LOGIN_URL: str = "http://192.168.0.19:5555"
    TAF_CALLBACK_URL: str = "http://192.168.0.19:8081/api/yz/callback"

    # SMB/CIFS 网络路径配置
    SMB_USERNAME: str = ""  # SMB 用户名（如：administrator）
    SMB_PASSWORD: str = ""  # SMB 密码
    SMB_DOMAIN: str = ""  # SMB 域（如：DOMAIN 或 nt08）
    SMB_MOUNT_BASE: str = "/mnt/smb"  # SMB 挂载基础目录

    # 备份配置
    BACKUP_TEMP_DIR: str = "temp/backup"
    RECOVERY_TEMP_DIR: str = "temp/recovery"
    BACKUP_COMPRESS_DIR: str = "temp/compress"  # 压缩文件临时目录（先压缩到这里，再移动到磁带机）
    # 写带失败文件的隔离目录：任务收尾时 final 中仍写不进磁带的文件移动到这里，
    # 不删除（保留补写/排查）；放系统盘（/TAF）避免占用 SSD 压缩盘空间。
    QUARANTINE_DIR: str = "/TAF/quarantine"
    # 数据库日志保留时间（月）：operation_logs / system_logs 超过该期限的记录
    # 由后台任务每日清理（1=1个月，12=1年，24=2年）
    LOG_RETENTION_MONTHS: int = 6
    VERIFY_TEMP_DIR: str = "temp/verify"  # 验证任务临时目录
    COMPRESSION_THREADS: int = 3  # Python压缩线程数（py7zr/PGZip）
    # 压缩方法配置
    COMPRESSION_METHOD: str = "zstd"  # 压缩方法: "pgzip"、"zstd" 或 "tar"
    # 注意：COMPRESSION_COMMAND_THREADS 默认使用 WEB_WORKERS 的值，在代码中动态获取
    PGZIP_BLOCK_SIZE: str = "1M"  # PGZip块大小（默认1M，可通过.env中的PGZIP_BLOCK_SIZE覆盖）
    PGZIP_THREADS: int = 3  # PGZip线程数
    ZSTD_THREADS: int = 3  # Zstandard压缩线程数
    ZSTD_WRITE_SIZE: int = 1048576  # Zstandard压缩写入缓冲区大小（字节），默认1MB（1048576字节）

    # 扫描进度更新配置
    SCAN_UPDATE_INTERVAL: int = 2000  # 后台扫描每处理多少个文件更新一次数据库（total_files/total_bytes）
    SCAN_ENGINE: str = "nfs"  # 扫描引擎：nfs=预编译排除+READDIRPLUS优化，CIFS挂载自动回退经典引擎
    SCAN_USE_PROCESS: bool = True  # NFS 引擎扫描走独立 spawn 子进程（绕开 GIL；false 回退线程模式）
    SCAN_LOG_INTERVAL_SECONDS: int = 60  # 扫描批次进度日志的最小间隔（秒），0或负值视为60
    SCAN_WAIT_TIMEOUT: int = 300  # 等待后台扫描写入文件记录的超时时间（秒），默认300秒（5分钟）
    # 压缩并行批次配置
    COMPRESSION_PARALLEL_BATCHES: int = 3  # 压缩并行批次数量（默认3），预读取程序队列数为该值+1
    COMPRESSION_TAR_COPYBUF_MB: int = 4  # tarfile 复制缓冲区（MB），0=Python 默认16KB；推荐 4MB
    DISK_THROTTLE_ENABLED: bool = True  # 磁盘限流总开关：关闭则百分比限流与危急层降级均不生效（数值保留）
    DISK_THROTTLE_HIGH_PERCENT: float = 75  # 磁盘使用率超过此百分比时压缩并行批次减半（0=禁用）
    DISK_THROTTLE_LOW_PERCENT: float = 30  # 磁盘使用率回落到此百分比以下时直接恢复批次（迟滞防抖）
    DISK_CRITICAL_BATCHES: int = 1  # 剩余空间 < N倍最大文件大小时，压缩并行批次强制降至此值（0=禁用）
    COMPRESSION_BATCHES_REDUCTION: int = 1  # 扫描时减少的并行批次数（默认1），0表示不减少

    # 扫描方法配置
    SCAN_METHOD: str = "default"  # 扫描方法: "default" (默认)
    
    # 简洁扫描配置（已移除开关，始终使用 SimpleScanner）

    # 纯内存扫描模式配置
    SCAN_MEMORY_ONLY: bool = True  # 纯内存扫描模式：已强制启用（UI 开关已移除，扫描阶段不写数据库）
    # 启用后：扫描器将文件记录写入内存，预取器从内存读取，压缩完成后一次性写入数据库
    # 优点：减少扫描阶段数据库I/O，提升性能；崩溃时无孤儿记录
    # 注意：需要足够内存存放文件记录（100万文件约500MB）

    # 磁带管理配置
    TAPE_POOL_SIZE: int = 12  # 磁带池大小
    TAPE_CHECK_INTERVAL: int = 3600  # 磁带状态检查间隔（秒）
    AUTO_TAPE_CLEANUP: bool = True

    # 高级配置
    WEB_WORKERS: int = 4  # Web服务器工作进程数，同时作为7-Zip命令行线程数（-mmt参数）的默认值
    TAPE_DEVICE_PATH: str = "/dev/nst0"  # mt 命令使用的磁带设备
    LTFS_DEVICE_PATH: str = "/dev/sg2"   # LTFS 挂载使用的 SCSI generic 设备

    class Config:
        env_file = ".env"
        env_file_encoding = "utf-8"
        case_sensitive = True
        extra = "allow"  # 允许额外的字段


# 全局配置实例
settings = Settings()


def get_settings() -> Settings:
    """获取配置实例"""
    return settings


def reload_settings() -> Settings:
    """重新加载配置"""
    global settings
    settings = Settings()
    return settings