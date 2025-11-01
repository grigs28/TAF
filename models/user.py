#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
用户数据模型
User Data Models
"""

from datetime import datetime
from sqlalchemy import Column, Integer, String, DateTime, Text, Boolean, Enum, JSON, ForeignKey, Table
from sqlalchemy.orm import relationship
import enum

from .base import BaseModel


# 用户角色关联表
user_roles_table = Table(
    'user_roles',
    BaseModel.metadata,
    Column('user_id', Integer, ForeignKey('users.id'), primary_key=True),
    Column('role_id', Integer, ForeignKey('roles.id'), primary_key=True),
    Column('assigned_at', DateTime(timezone=True), default=datetime.utcnow),
    Column('assigned_by', String(100))
)

# 角色权限关联表
role_permissions_table = Table(
    'role_permissions',
    BaseModel.metadata,
    Column('role_id', Integer, ForeignKey('roles.id'), primary_key=True),
    Column('permission_id', Integer, ForeignKey('permissions.id'), primary_key=True),
    Column('granted_at', DateTime(timezone=True), default=datetime.utcnow),
    Column('granted_by', String(100))
)


class UserStatus(enum.Enum):
    """用户状态"""
    ACTIVE = "active"       # 活跃
    INACTIVE = "inactive"   # 非活跃
    LOCKED = "locked"      # 锁定
    SUSPENDED = "suspended"  # 暂停


class User(BaseModel):
    """用户表"""

    __tablename__ = "users"

    # 基本信息
    username = Column(String(100), unique=True, nullable=False, comment="用户名")
    email = Column(String(255), unique=True, nullable=False, comment="邮箱")
    full_name = Column(String(200), comment="全名")
    phone = Column(String(20), comment="电话号码")

    # 认证信息
    password_hash = Column(String(255), nullable=False, comment="密码哈希")
    salt = Column(String(100), comment="密码盐")
    api_key = Column(String(255), comment="API密钥")

    # 状态信息
    status = Column(Enum(UserStatus), default=UserStatus.ACTIVE, comment="状态")
    is_admin = Column(Boolean, default=False, comment="是否管理员")
    must_change_password = Column(Boolean, default=False, comment="是否必须修改密码")

    # 登录信息
    last_login_at = Column(DateTime(timezone=True), comment="最后登录时间")
    last_login_ip = Column(String(45), comment="最后登录IP")
    failed_login_count = Column(Integer, default=0, comment="失败登录次数")
    locked_until = Column(DateTime(timezone=True), comment="锁定至")

    # 安全信息
    two_factor_enabled = Column(Boolean, default=False, comment="是否启用双因子认证")
    two_factor_secret = Column(String(100), comment="双因子认证密钥")
    backup_codes = Column(JSON, comment="备用验证码")

    # 个人设置
    timezone = Column(String(50), default="Asia/Shanghai", comment="时区")
    language = Column(String(10), default="zh-CN", comment="语言")
    theme = Column(String(20), default="light", comment="主题")
    preferences = Column(JSON, comment="个人偏好设置")

    # 元数据
    user_metadata = Column(JSON, comment="元数据")
    notes = Column(Text, comment="备注")

    # 关联关系
    roles = relationship("Role", secondary=user_roles_table, back_populates="users")
    operation_logs = relationship("OperationLog", back_populates="user")

    def __repr__(self):
        return f"<User(id={self.id}, username={self.username}, status={self.status.value})>"


class Role(BaseModel):
    """角色表"""

    __tablename__ = "roles"

    # 基本信息
    name = Column(String(100), unique=True, nullable=False, comment="角色名称")
    display_name = Column(String(200), comment="显示名称")
    description = Column(Text, comment="角色描述")

    # 状态信息
    is_active = Column(Boolean, default=True, comment="是否激活")
    is_system = Column(Boolean, default=False, comment="是否系统角色")

    # 权限信息
    permissions = relationship("Permission", secondary=role_permissions_table, back_populates="roles")

    # 关联关系
    users = relationship("User", secondary=user_roles_table, back_populates="roles")

    def __repr__(self):
        return f"<Role(id={self.id}, name={self.name})>"


class PermissionCategory(enum.Enum):
    """权限分类"""
    BACKUP = "backup"           # 备份操作
    RECOVERY = "recovery"       # 恢复操作
    TAPE = "tape"              # 磁带操作
    SYSTEM = "system"           # 系统管理
    USER = "user"              # 用户管理
    LOG = "log"                # 日志查看
    CONFIG = "config"          # 配置管理
    MONITOR = "monitor"        # 监控查看


class Permission(BaseModel):
    """权限表"""

    __tablename__ = "permissions"

    # 基本信息
    name = Column(String(100), unique=True, nullable=False, comment="权限名称")
    display_name = Column(String(200), comment="显示名称")
    description = Column(Text, comment="权限描述")
    category = Column(Enum(PermissionCategory), nullable=False, comment="权限分类")

    # 权限信息
    resource = Column(String(100), comment="资源")
    action = Column(String(100), comment="操作")
    conditions = Column(JSON, comment="权限条件")

    # 状态信息
    is_active = Column(Boolean, default=True, comment="是否激活")
    is_system = Column(Boolean, default=False, comment="是否系统权限")

    # 关联关系
    roles = relationship("Role", secondary=role_permissions_table, back_populates="permissions")

    def __repr__(self):
        return f"<Permission(id={self.id}, name={self.name}, category={self.category.value})>"


class UserSession(BaseModel):
    """用户会话表"""

    __tablename__ = "user_sessions"

    # 关联信息
    user_id = Column(Integer, ForeignKey("users.id"), nullable=False, comment="用户ID")

    # 会话信息
    session_token = Column(String(255), unique=True, nullable=False, comment="会话令牌")
    refresh_token = Column(String(255), unique=True, comment="刷新令牌")

    # 时间信息
    created_at = Column(DateTime(timezone=True), nullable=False, comment="创建时间")
    expires_at = Column(DateTime(timezone=True), nullable=False, comment="过期时间")
    last_accessed_at = Column(DateTime(timezone=True), comment="最后访问时间")

    # 客户端信息
    user_agent = Column(Text, comment="用户代理")
    ip_address = Column(String(45), comment="IP地址")
    device_info = Column(JSON, comment="设备信息")

    # 状态信息
    is_active = Column(Boolean, default=True, comment="是否活跃")
    logout_at = Column(DateTime(timezone=True), comment="登出时间")
    logout_reason = Column(String(100), comment="登出原因")

    def __repr__(self):
        return f"<UserSession(id={self.id}, user_id={self.user_id}, active={self.is_active})>"


class UserLoginHistory(BaseModel):
    """用户登录历史表"""

    __tablename__ = "user_login_history"

    # 关联信息
    user_id = Column(Integer, ForeignKey("users.id"), nullable=False, comment="用户ID")

    # 登录信息
    login_time = Column(DateTime(timezone=True), nullable=False, comment="登录时间")
    login_result = Column(String(20), nullable=False, comment="登录结果(success/failed)")
    failure_reason = Column(String(200), comment="失败原因")

    # 客户端信息
    ip_address = Column(String(45), comment="IP地址")
    user_agent = Column(Text, comment="用户代理")
    device_fingerprint = Column(String(255), comment="设备指纹")

    # 地理信息
    country = Column(String(100), comment="国家")
    region = Column(String(100), comment="地区")
    city = Column(String(100), comment="城市")

    # 安全信息
    is_suspicious = Column(Boolean, default=False, comment="是否可疑")
    risk_score = Column(Integer, default=0, comment="风险评分")

    def __repr__(self):
        return f"<UserLoginHistory(id={self.id}, user_id={self.user_id}, result={self.login_result})>"