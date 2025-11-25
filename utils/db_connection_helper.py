#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
数据库连接辅助函数
统一处理 psycopg2 和 psycopg3 的连接
"""

import logging
import re
from typing import Optional, Tuple, Any

logger = logging.getLogger(__name__)


def get_psycopg_connection(
    host: str,
    port: str,
    user: str,
    password: str,
    database: str,
    prefer_psycopg3: bool = True
) -> Tuple[Any, bool]:
    """
    获取 PostgreSQL/openGauss 数据库连接（支持 psycopg2 和 psycopg3）
    在 openGauss 模式下优先使用 psycopg3 binary protocol
    
    Args:
        host: 数据库主机
        port: 数据库端口
        user: 用户名
        password: 密码
        database: 数据库名
        prefer_psycopg3: 是否优先使用 psycopg3
    
    Returns:
        (connection, is_psycopg3): 连接对象和是否为 psycopg3 的布尔值
    
    Raises:
        ImportError: 如果 psycopg2 和 psycopg3 都未安装
    """
    use_psycopg3 = False
    
    if prefer_psycopg3:
        try:
            import psycopg
            use_psycopg3 = True
            logger.debug("使用 psycopg3（同步，binary protocol）创建数据库连接...")
        except ImportError:
            logger.debug("psycopg3 未安装，尝试使用 psycopg2...")
    
    if not use_psycopg3:
        try:
            import psycopg2
            logger.debug("使用 psycopg2 创建数据库连接...")
        except ImportError:
            if prefer_psycopg3:
                # 如果优先使用 psycopg3 但未安装，再尝试一次
                try:
                    import psycopg
                    use_psycopg3 = True
                    logger.debug("回退到 psycopg3（同步，binary protocol）创建数据库连接...")
                except ImportError:
                    logger.error("psycopg2 和 psycopg3 都未安装，无法创建数据库连接")
                    raise ImportError("需要安装 psycopg2 或 psycopg3 来创建数据库连接")
            else:
                logger.error("psycopg2 未安装，无法创建数据库连接")
                raise ImportError("需要安装 psycopg2 来创建数据库连接")
    
    if use_psycopg3:
        import psycopg
        # psycopg3 使用 binary protocol（通过安装 psycopg[binary] 启用）
        # 使用连接字符串格式，确保正确连接到 openGauss
        conninfo = f"host={host} port={port} user={user} password={password} dbname={database}"
        conn = psycopg.connect(conninfo)
        logger.debug("psycopg3 连接已创建（binary protocol）")
        return conn, True
    else:
        import psycopg2
        conn = psycopg2.connect(
            host=host,
            port=port,
            user=user,
            password=password,
            database=database  # psycopg2 使用 database
        )
        return conn, False


def parse_database_url(database_url: str) -> Tuple[str, str, str, str, str]:
    """
    解析数据库 URL，返回连接参数
    
    Args:
        database_url: 数据库连接 URL（支持 postgresql:// 和 opengauss://）
    
    Returns:
        (username, password, host, port, database)
    
    Raises:
        ValueError: 如果 URL 格式不正确
    """
    # 处理 opengauss URL
    if database_url.startswith("opengauss://"):
        database_url = database_url.replace("opengauss://", "postgresql://", 1)
    
    pattern = r'postgresql://([^:]+):([^@]+)@([^:]+):(\d+)/(.+)'
    match = re.match(pattern, database_url)
    if not match:
        raise ValueError("无法解析数据库连接URL")
    
    username, password, host, port, database = match.groups()
    return username, password, host, port, database


def get_psycopg_connection_from_url(
    database_url: str,
    prefer_psycopg3: bool = True
) -> Tuple[Any, bool]:
    """
    从数据库 URL 创建连接
    
    Args:
        database_url: 数据库连接 URL
        prefer_psycopg3: 是否优先使用 psycopg3
    
    Returns:
        (connection, is_psycopg3): 连接对象和是否为 psycopg3 的布尔值
    """
    username, password, host, port, database = parse_database_url(database_url)
    return get_psycopg_connection(host, port, user=username, password=password, database=database, prefer_psycopg3=prefer_psycopg3)


def set_autocommit(conn: Any, is_psycopg3: bool, autocommit: bool = True):
    """
    设置连接的自动提交模式
    
    Args:
        conn: 数据库连接对象
        is_psycopg3: 是否为 psycopg3
        autocommit: 是否自动提交
    """
    if is_psycopg3:
        # psycopg3 使用 autocommit 属性
        conn.autocommit = autocommit
    else:
        # psycopg2 使用 set_isolation_level
        from psycopg2.extensions import ISOLATION_LEVEL_AUTOCOMMIT
        if autocommit:
            conn.set_isolation_level(ISOLATION_LEVEL_AUTOCOMMIT)
        else:
            conn.set_isolation_level(0)  # READ COMMITTED

