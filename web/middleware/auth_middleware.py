#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
认证中间件
Authentication Middleware
"""

import logging
from typing import Callable
from fastapi import Request, Response, HTTPException, status
from starlette.middleware.base import BaseHTTPMiddleware

logger = logging.getLogger(__name__)


class AuthMiddleware(BaseHTTPMiddleware):
    """认证中间件"""

    # 不需要认证的路径
    EXCLUDED_PATHS = {
        "/",
        "/health",
        "/login",
        "/static",
        "/api/user/login",
        "/api/user/register",
        # 页面路由（HTML页面不需要认证，由前端处理）
        "/backup",
        "/recovery",
        "/tape",
        "/tapedrive",
        "/scheduler",
        "/tools",
        "/system"
    }

    async def dispatch(self, request: Request, call_next: Callable) -> Response:
        path = request.url.path

        # 检查是否需要跳过认证
        if self._should_skip_auth(path):
            logger.debug(f"跳过认证检查: {path}")
            return await call_next(request)
        
        logger.debug(f"需要认证检查: {path}")

        # 检查认证令牌
        auth_header = request.headers.get("Authorization")
        if not auth_header or not auth_header.startswith("Bearer "):
            raise HTTPException(
                status_code=status.HTTP_401_UNAUTHORIZED,
                detail="未提供认证令牌",
                headers={"WWW-Authenticate": "Bearer"},
            )

        token = auth_header.split(" ")[1]

        # 验证令牌（这里应该实现真实的令牌验证逻辑）
        if not self._validate_token(token):
            raise HTTPException(
                status_code=status.HTTP_401_UNAUTHORIZED,
                detail="无效的认证令牌",
                headers={"WWW-Authenticate": "Bearer"},
            )

        # 令牌有效，继续处理请求
        return await call_next(request)

    def _should_skip_auth(self, path: str) -> bool:
        """检查是否应该跳过认证"""
        for excluded_path in self.EXCLUDED_PATHS:
            if path.startswith(excluded_path):
                return True
        return False

    def _validate_token(self, token: str) -> bool:
        """验证令牌（这里应该实现真实的JWT验证逻辑）"""
        # 暂时简单检查令牌格式
        if len(token) < 10:
            return False

        # 这里应该：
        # 1. 验证JWT签名
        # 2. 检查令牌是否过期
        # 3. 验证用户权限
        # 4. 检查令牌是否被撤销

        return True