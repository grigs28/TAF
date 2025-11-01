#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
Web应用主模块
Web Application Main Module
"""

import logging
from fastapi import FastAPI, Request, Depends, HTTPException, status
from fastapi.staticfiles import StaticFiles
from fastapi.templating import Jinja2Templates
from fastapi.middleware.cors import CORSMiddleware
from fastapi.responses import HTMLResponse
from contextlib import asynccontextmanager

from config.settings import get_settings
from utils.logger import get_logger
from web.api import backup, recovery, tape, system, user
from web.middleware.auth_middleware import AuthMiddleware
from web.middleware.logging_middleware import LoggingMiddleware

logger = get_logger(__name__)
settings = get_settings()


@asynccontextmanager
async def lifespan(app: FastAPI):
    """应用生命周期管理"""
    # 启动时执行
    logger.info("Web应用启动中...")
    yield
    # 关闭时执行
    logger.info("Web应用关闭中...")


def create_app(system_instance=None) -> FastAPI:
    """创建FastAPI应用"""
    app = FastAPI(
        title=settings.APP_NAME,
        version=settings.APP_VERSION,
        description="企业级磁带备份系统管理界面",
        lifespan=lifespan
    )

    # 配置静态文件
    app.mount("/static", StaticFiles(directory="web/static"), name="static")

    # 配置模板
    templates = Jinja2Templates(directory="web/templates")

    # 添加CORS中间件
    app.add_middleware(
        CORSMiddleware,
        allow_origins=["*"],
        allow_credentials=True,
        allow_methods=["*"],
        allow_headers=["*"],
    )

    # 添加自定义中间件
    app.add_middleware(LoggingMiddleware)
    if not settings.DEBUG:
        app.add_middleware(AuthMiddleware)

    # 注册API路由
    app.include_router(backup.router, prefix="/api/backup", tags=["备份管理"])
    app.include_router(recovery.router, prefix="/api/recovery", tags=["恢复管理"])
    app.include_router(tape.router, prefix="/api/tape", tags=["磁带管理"])
    app.include_router(system.router, prefix="/api/system", tags=["系统管理"])
    app.include_router(user.router, prefix="/api/user", tags=["用户管理"])

    # 存储系统实例引用
    app.state.system = system_instance

    @app.get("/", response_class=HTMLResponse)
    async def index(request: Request):
        """首页"""
        return templates.TemplateResponse("index.html", {
            "request": request,
            "app_name": settings.APP_NAME,
            "version": settings.APP_VERSION
        })

    @app.get("/backup", response_class=HTMLResponse)
    async def backup_page(request: Request):
        """备份管理页面"""
        return templates.TemplateResponse("backup.html", {
            "request": request,
            "app_name": settings.APP_NAME,
            "version": settings.APP_VERSION
        })

    @app.get("/recovery", response_class=HTMLResponse)
    async def recovery_page(request: Request):
        """恢复管理页面"""
        return templates.TemplateResponse("recovery.html", {
            "request": request,
            "app_name": settings.APP_NAME,
            "version": settings.APP_VERSION
        })

    @app.get("/tape", response_class=HTMLResponse)
    async def tape_page(request: Request):
        """磁带管理页面"""
        return templates.TemplateResponse("tape.html", {
            "request": request,
            "app_name": settings.APP_NAME,
            "version": settings.APP_VERSION
        })

    @app.get("/system", response_class=HTMLResponse)
    async def system_page(request: Request):
        """系统设置页面"""
        return templates.TemplateResponse("system.html", {
            "request": request,
            "app_name": settings.APP_NAME,
            "version": settings.APP_VERSION
        })

    @app.get("/health")
    async def health_check():
        """健康检查"""
        return {
            "status": "healthy",
            "timestamp": "2024-10-30T04:20:00Z",
            "version": settings.APP_VERSION
        }

    return app