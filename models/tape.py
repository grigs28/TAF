#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
磁带数据模型
Tape Data Models
"""

from datetime import datetime
from sqlalchemy import Column, Integer, String, DateTime, Text, BigInteger, Enum, Float, JSON, Boolean, ForeignKey
from sqlalchemy.orm import relationship
from sqlalchemy.dialects.postgresql import UUID
import enum
import uuid

from .base import BaseModel


class TapeStatus(enum.Enum):
    """磁带状态"""
    NEW = "new"                   # 新磁带
    AVAILABLE = "available"       # 可用
    IN_USE = "in_use"            # 使用中
    FULL = "full"                # 已满
    EXPIRED = "expired"          # 已过期
    ERROR = "error"              # 错误
    MAINTENANCE = "maintenance"   # 维护中
    RETIRED = "retired"          # 已退役


class TapeCartridge(BaseModel):
    """磁带盒表"""

    __tablename__ = "tape_cartridges"

    # 基本信息
    tape_uuid = Column(UUID(as_uuid=True), default=uuid.uuid4, unique=True, nullable=False, comment="磁带UUID")
    tape_id = Column(String(50), unique=True, nullable=False, comment="磁带ID")
    label = Column(String(200), nullable=False, comment="磁带标签")
    status = Column(Enum(TapeStatus), default=TapeStatus.NEW, comment="状态")
    location = Column(String(100), comment="存储位置")

    # 物理信息
    media_type = Column(String(50), default="LTO", comment="介质类型")
    generation = Column(Integer, default=8, comment="代数(LTO-8等)")
    serial_number = Column(String(100), comment="序列号")
    manufacturer = Column(String(100), comment="制造商")
    purchase_date = Column(DateTime(timezone=True), comment="购买日期")

    # 容量信息
    capacity_bytes = Column(BigInteger, nullable=False, comment="容量(字节)")
    used_bytes = Column(BigInteger, default=0, comment="已使用(字节)")
    compressed_bytes = Column(BigInteger, default=0, comment="压缩后使用(字节)")

    # 时间信息
    manufactured_date = Column(DateTime(timezone=True), comment="生产日期")
    first_use_date = Column(DateTime(timezone=True), comment="首次使用日期")
    last_erase_date = Column(DateTime(timezone=True), comment="最后擦除日期")
    expiry_date = Column(DateTime(timezone=True), comment="过期日期")

    # 使用统计
    write_count = Column(Integer, default=0, comment="写入次数")
    read_count = Column(Integer, default=0, comment="读取次数")
    pass_count = Column(Integer, default=0, comment="通过次数")
    load_count = Column(Integer, default=0, comment="加载次数")
    mount_hours = Column(Float, default=0.0, comment="挂载小时数")

    # 健康状态
    health_score = Column(Integer, default=100, comment="健康分数(0-100)")
    error_count = Column(Integer, default=0, comment="错误次数")
    warning_count = Column(Integer, default=0, comment="警告次数")
    last_error_date = Column(DateTime(timezone=True), comment="最后错误时间")

    # 备份组信息
    backup_group = Column(String(20), comment="当前备份组(YYYY-MM)")
    backup_set_count = Column(Integer, default=0, comment="备份集数量")

    # 配置信息
    retention_months = Column(Integer, default=6, comment="保留月数")
    auto_erase = Column(Boolean, default=True, comment="自动擦除")
    write_protect = Column(Boolean, default=False, comment="写保护")

    # 元数据
    tape_metadata = Column(JSON, comment="元数据")
    notes = Column(Text, comment="备注")

    # 关联关系
    tape_usage = relationship("TapeUsage", back_populates="tape", cascade="all, delete-orphan")
    tape_logs = relationship("TapeLog", back_populates="tape", cascade="all, delete-orphan")

    def __repr__(self):
        return f"<TapeCartridge(id={self.tape_id}, status={self.status.value})>"


class TapeOperationType(enum.Enum):
    """磁带操作类型"""
    LOAD = "load"           # 加载
    UNLOAD = "unload"       # 卸载
    WRITE = "write"         # 写入
    READ = "read"          # 读取
    ERASE = "erase"        # 擦除
    REWIND = "rewind"      # 倒带
    VERIFY = "verify"      # 验证
    CLEAN = "clean"        # 清洁
    ERROR = "error"        # 错误
    MAINTENANCE = "maintenance"  # 维护


class TapeUsage(BaseModel):
    """磁带使用记录表"""

    __tablename__ = "tape_usage"

    # 关联信息
    tape_id = Column(String(50), ForeignKey("tape_cartridges.tape_id"), nullable=False, comment="磁带ID")

    # 操作信息
    operation_type = Column(Enum(TapeOperationType), nullable=False, comment="操作类型")
    operation_result = Column(String(20), nullable=False, comment="操作结果(success/failed)")
    start_time = Column(DateTime(timezone=True), nullable=False, comment="开始时间")
    end_time = Column(DateTime(timezone=True), comment="结束时间")
    duration_seconds = Column(Integer, comment="持续时间(秒)")

    # 数据信息
    bytes_processed = Column(BigInteger, comment="处理字节数")
    block_count = Column(Integer, comment="数据块数量")
    file_count = Column(Integer, comment="文件数量")

    # 关联信息
    backup_set_id = Column(String(50), comment="备份集ID")
    task_id = Column(String(100), comment="任务ID")
    worker_id = Column(String(100), comment="Worker ID")

    # 性能信息
    throughput_mbps = Column(Float, comment="吞吐量(MB/s)")
    compression_ratio = Column(Float, comment="压缩比")

    # 错误信息
    error_code = Column(String(50), comment="错误代码")
    error_message = Column(Text, comment="错误信息")
    scsi_sense_data = Column(String(200), comment="SCSI Sense数据")

    # 环境信息
    drive_serial = Column(String(100), comment="驱动器序列号")
    host_name = Column(String(100), comment="主机名")
    process_id = Column(Integer, comment="进程ID")

    # 元数据
    tape_metadata = Column(JSON, comment="元数据")

    # 关联关系
    tape = relationship("TapeCartridge", back_populates="tape_usage")

    def __repr__(self):
        return f"<TapeUsage(id={self.id}, tape={self.tape_id}, operation={self.operation_type.value})>"


class TapeLogLevel(enum.Enum):
    """磁带日志级别"""
    INFO = "info"
    WARNING = "warning"
    ERROR = "error"
    DEBUG = "debug"


class TapeLog(BaseModel):
    """磁带日志表"""

    __tablename__ = "tape_logs"

    # 关联信息
    tape_id = Column(String(50), ForeignKey("tape_cartridges.tape_id"), comment="磁带ID")

    # 日志信息
    log_level = Column(Enum(TapeLogLevel), nullable=False, comment="日志级别")
    category = Column(String(50), comment="日志分类")
    message = Column(Text, nullable=False, comment="日志消息")
    details = Column(JSON, comment="详细信息")

    # 时间信息
    log_time = Column(DateTime(timezone=True), nullable=False, comment="日志时间")

    # 来源信息
    source_module = Column(String(100), comment="来源模块")
    source_function = Column(String(100), comment="来源函数")
    source_line = Column(Integer, comment="源代码行号")

    # 关联信息
    task_id = Column(String(100), comment="任务ID")
    operation_id = Column(Integer, comment="操作ID")

    # 关联关系
    tape = relationship("TapeCartridge", back_populates="tape_logs")

    def __repr__(self):
        return f"<TapeLog(id={self.id}, tape={self.tape_id}, level={self.log_level.value})>"