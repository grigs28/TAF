#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
系统日志数据模型
System Log Data Models
"""

from datetime import datetime
from sqlalchemy import Column, Integer, String, DateTime, Text, JSON, ForeignKey, Enum, Float, Boolean
from sqlalchemy.orm import relationship
import enum

from .base import BaseModel


class LogLevel(enum.Enum):
    """日志级别"""
    DEBUG = "debug"
    INFO = "info"
    WARNING = "warning"
    ERROR = "error"
    CRITICAL = "critical"


class LogCategory(enum.Enum):
    """日志分类"""
    SYSTEM = "system"         # 系统日志
    BACKUP = "backup"         # 备份日志
    RECOVERY = "recovery"     # 恢复日志
    TAPE = "tape"            # 磁带日志
    USER = "user"            # 用户操作日志
    SECURITY = "security"     # 安全日志
    PERFORMANCE = "performance"  # 性能日志
    API = "api"              # API日志
    WEB = "web"              # Web日志
    DATABASE = "database"     # 数据库日志


class SystemLog(BaseModel):
    """系统日志表"""

    __tablename__ = "system_logs"

    # 日志信息
    log_level = Column(Enum(LogLevel), nullable=False, comment="日志级别")
    category = Column(Enum(LogCategory), nullable=False, comment="日志分类")
    message = Column(Text, nullable=False, comment="日志消息")
    details = Column(JSON, comment="详细信息")

    # 时间信息
    log_time = Column(DateTime(timezone=True), nullable=False, comment="日志时间")
    timestamp = Column(Integer, comment="时间戳(毫秒)")

    # 来源信息
    module = Column(String(100), comment="模块名")
    function = Column(String(100), comment="函数名")
    line_number = Column(Integer, comment="行号")
    file_path = Column(String(500), comment="文件路径")

    # 上下文信息
    thread_id = Column(String(50), comment="线程ID")
    process_id = Column(Integer, comment="进程ID")
    request_id = Column(String(100), comment="请求ID")
    session_id = Column(String(100), comment="会话ID")

    # 环境信息
    hostname = Column(String(100), comment="主机名")
    environment = Column(String(50), comment="环境(dev/test/prod)")
    version = Column(String(50), comment="版本号")

    # 关联信息
    user_id = Column(Integer, ForeignKey("users.id"), comment="用户ID")
    task_id = Column(String(100), comment="任务ID")
    correlation_id = Column(String(100), comment="关联ID")

    # 性能信息
    duration_ms = Column(Integer, comment="持续时间(毫秒)")
    memory_usage_mb = Column(Float, comment="内存使用(MB)")
    cpu_usage_percent = Column(Float, comment="CPU使用率")

    # 索引字段
    exception_type = Column(String(200), comment="异常类型")
    stack_trace = Column(Text, comment="堆栈跟踪")

    def __repr__(self):
        return f"<SystemLog(id={self.id}, level={self.log_level.value}, category={self.category.value})>"


class OperationType(enum.Enum):
    """操作类型"""
    CREATE = "create"         # 创建
    UPDATE = "update"         # 更新
    DELETE = "delete"         # 删除
    READ = "read"            # 读取
    EXECUTE = "execute"       # 执行
    LOGIN = "login"          # 登录
    LOGOUT = "logout"        # 登出
    BACKUP = "backup"        # 备份
    RECOVERY = "recovery"    # 恢复
    CONFIG = "config"        # 配置
    EXPORT = "export"        # 导出
    IMPORT = "import"        # 导入


class OperationLog(BaseModel):
    """操作日志表"""

    __tablename__ = "operation_logs"

    # 关联信息
    user_id = Column(Integer, ForeignKey("users.id"), comment="用户ID")

    # 操作信息
    operation_type = Column(Enum(OperationType), nullable=False, comment="操作类型")
    resource_type = Column(String(100), comment="资源类型")
    resource_id = Column(String(100), comment="资源ID")
    operation_name = Column(String(200), comment="操作名称")
    operation_description = Column(Text, comment="操作描述")

    # 时间信息
    operation_time = Column(DateTime(timezone=True), nullable=False, comment="操作时间")
    duration_ms = Column(Integer, comment="持续时间(毫秒)")

    # 请求信息
    request_method = Column(String(10), comment="请求方法")
    request_url = Column(String(1000), comment="请求URL")
    request_params = Column(JSON, comment="请求参数")
    request_body = Column(JSON, comment="请求体")

    # 响应信息
    response_status = Column(Integer, comment="响应状态码")
    response_body = Column(JSON, comment="响应体")
    response_size = Column(Integer, comment="响应大小(字节)")

    # 结果信息
    success = Column(Boolean, nullable=False, comment="是否成功")
    result_message = Column(Text, comment="结果消息")
    error_code = Column(String(50), comment="错误代码")
    error_message = Column(Text, comment="错误消息")

    # 客户端信息
    ip_address = Column(String(45), comment="IP地址")
    user_agent = Column(Text, comment="用户代理")
    referer = Column(String(1000), comment="来源页面")

    # 审计信息
    old_values = Column(JSON, comment="修改前值")
    new_values = Column(JSON, comment="修改后值")
    changed_fields = Column(JSON, comment="变更字段")

    # 关联关系
    user = relationship("User", back_populates="operation_logs")

    def __repr__(self):
        return f"<OperationLog(id={self.id}, user_id={self.user_id}, operation={self.operation_type.value})>"


class ErrorLevel(enum.Enum):
    """错误级别"""
    LOW = "low"              # 低级
    MEDIUM = "medium"        # 中级
    HIGH = "high"            # 高级
    CRITICAL = "critical"    # 严重


class ErrorLog(BaseModel):
    """错误日志表"""

    __tablename__ = "error_logs"

    # 错误信息
    error_level = Column(Enum(ErrorLevel), nullable=False, comment="错误级别")
    error_code = Column(String(50), comment="错误代码")
    error_name = Column(String(200), comment="错误名称")
    error_message = Column(Text, nullable=False, comment="错误消息")

    # 异常信息
    exception_type = Column(String(200), comment="异常类型")
    exception_module = Column(String(200), comment="异常模块")
    stack_trace = Column(Text, comment="堆栈跟踪")
    inner_exception = Column(Text, comment="内部异常")

    # 时间信息
    error_time = Column(DateTime(timezone=True), nullable=False, comment="错误时间")

    # 来源信息
    module = Column(String(100), comment="模块名")
    function = Column(String(100), comment="函数名")
    line_number = Column(Integer, comment="行号")
    file_path = Column(String(500), comment="文件路径")

    # 上下文信息
    user_id = Column(Integer, ForeignKey("users.id"), comment="用户ID")
    session_id = Column(String(100), comment="会话ID")
    request_id = Column(String(100), comment="请求ID")
    task_id = Column(String(100), comment="任务ID")

    # 环境信息
    hostname = Column(String(100), comment="主机名")
    environment = Column(String(50), comment="环境")
    version = Column(String(50), comment="版本号")

    # 状态信息
    resolved = Column(Boolean, default=False, comment="是否已解决")
    resolved_at = Column(DateTime(timezone=True), comment="解决时间")
    resolved_by = Column(Integer, ForeignKey("users.id"), comment="解决者ID")
    resolution_notes = Column(Text, comment="解决说明")

    # 影响信息
    affected_users = Column(Integer, default=0, comment="影响用户数")
    affected_operations = Column(Integer, default=0, comment="影响操作数")
    estimated_loss = Column(Float, comment="预估损失")

    # 处理信息
    assigned_to = Column(Integer, ForeignKey("users.id"), comment="分配给")
    priority = Column(Integer, default=3, comment="优先级(1-5)")
    category = Column(String(100), comment="错误分类")
    tags = Column(JSON, comment="标签")

    # 关联关系
    user = relationship("User", foreign_keys=[user_id])
    resolver = relationship("User", foreign_keys=[resolved_by])
    assignee = relationship("User", foreign_keys=[assigned_to])

    def __repr__(self):
        return f"<ErrorLog(id={self.id}, level={self.error_level.value}, code={self.error_code})>"


class AuditLog(BaseModel):
    """审计日志表"""

    __tablename__ = "audit_logs"

    # 审计信息
    audit_type = Column(String(100), nullable=False, comment="审计类型")
    event_name = Column(String(200), nullable=False, comment="事件名称")
    event_description = Column(Text, comment="事件描述")

    # 时间信息
    event_time = Column(DateTime(timezone=True), nullable=False, comment="事件时间")

    # 主体信息
    actor_id = Column(Integer, ForeignKey("users.id"), comment="操作者ID")
    actor_type = Column(String(50), comment="操作者类型")
    actor_name = Column(String(200), comment="操作者名称")

    # 客体信息
    object_type = Column(String(100), comment="对象类型")
    object_id = Column(String(100), comment="对象ID")
    object_name = Column(String(200), comment="对象名称")
    object_data = Column(JSON, comment="对象数据")

    # 操作信息
    action = Column(String(100), comment="动作")
    result = Column(String(50), comment="结果")
    reason = Column(Text, comment="原因")

    # 位置信息
    ip_address = Column(String(45), comment="IP地址")
    user_agent = Column(Text, comment="用户代理")
    location = Column(JSON, comment="地理位置")

    # 合规信息
    compliance_category = Column(String(100), comment="合规分类")
    retention_period_days = Column(Integer, comment="保留天数")
    sensitive_data = Column(Boolean, default=False, comment="是否敏感数据")

    # 关联信息
    correlation_id = Column(String(100), comment="关联ID")
    parent_audit_id = Column(Integer, ForeignKey("audit_logs.id"), comment="父审计ID")

    def __repr__(self):
        return f"<AuditLog(id={self.id}, type={self.audit_type}, actor={self.actor_name})>"