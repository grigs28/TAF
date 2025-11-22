#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
Redis数据库操作模块 - 原生实现（不使用SQLAlchemy）
Redis Database Operations Module - Native Implementation (No SQLAlchemy)
"""

import logging
import json
import os
from typing import Optional, Dict, Any, List
from pathlib import Path
from urllib.parse import urlparse, parse_qs
import redis.asyncio as aioredis
from redis.asyncio import Redis
from redis.exceptions import ConnectionError, TimeoutError, RedisError

logger = logging.getLogger(__name__)

# 全局Redis连接池
_redis_pool: Optional[Redis] = None

# 全局Redis管理器实例（使用字符串类型注解避免前向引用问题）
_redis_manager_instance: Optional["RedisDBManager"] = None


class RedisDBManager:
    """Redis数据库管理器 - 原生实现"""
    
    def __init__(self, host: str = "localhost", port: int = 6379, 
                 password: Optional[str] = None, db: int = 0,
                 decode_responses: bool = True):
        """
        初始化Redis数据库管理器
        
        Args:
            host: Redis主机地址
            port: Redis端口
            password: Redis密码
            db: Redis数据库编号（0-15）
            decode_responses: 是否自动解码响应为字符串
        """
        self.host = host
        self.port = port
        self.password = password
        self.db = db
        self.decode_responses = decode_responses
        self.redis: Optional[Redis] = None
        self._initialized = False
    
    async def initialize(self) -> bool:
        """初始化Redis连接"""
        try:
            self.redis = await aioredis.from_url(
                f"redis://{':' + self.password + '@' if self.password else ''}{self.host}:{self.port}/{self.db}",
                decode_responses=self.decode_responses,
                socket_connect_timeout=5,
                socket_timeout=5,
                retry_on_timeout=True,
                health_check_interval=30
            )
            
            # 测试连接
            await self.redis.ping()
            self._initialized = True
            logger.info(f"Redis连接初始化成功: {self.host}:{self.port}/{self.db}")
            return True
        except Exception as e:
            logger.error(f"Redis连接初始化失败: {str(e)}", exc_info=True)
            self._initialized = False
            return False
    
    async def close(self):
        """关闭Redis连接"""
        if self.redis:
            try:
                await self.redis.close()
                logger.info("Redis连接已关闭")
            except Exception as e:
                logger.error(f"关闭Redis连接失败: {str(e)}")
            finally:
                self.redis = None
                self._initialized = False
    
    async def health_check(self) -> bool:
        """健康检查"""
        if not self._initialized or not self.redis:
            return False
        try:
            await self.redis.ping()
            return True
        except Exception as e:
            logger.warning(f"Redis健康检查失败: {str(e)}")
            return False
    
    async def get(self, key: str) -> Optional[str]:
        """获取键值"""
        if not self._initialized or not self.redis:
            raise RuntimeError("Redis未初始化")
        try:
            return await self.redis.get(key)
        except Exception as e:
            logger.error(f"Redis GET操作失败: key={key}, error={str(e)}")
            raise
    
    async def set(self, key: str, value: Any, ex: Optional[int] = None) -> bool:
        """设置键值"""
        if not self._initialized or not self.redis:
            raise RuntimeError("Redis未初始化")
        try:
            # 如果是复杂对象，序列化为JSON
            if isinstance(value, (dict, list)):
                value = json.dumps(value, ensure_ascii=False)
            result = await self.redis.set(key, value, ex=ex)
            return bool(result)
        except Exception as e:
            logger.error(f"Redis SET操作失败: key={key}, error={str(e)}")
            raise
    
    async def delete(self, *keys: str) -> int:
        """删除键"""
        if not self._initialized or not self.redis:
            raise RuntimeError("Redis未初始化")
        try:
            return await self.redis.delete(*keys)
        except Exception as e:
            logger.error(f"Redis DELETE操作失败: keys={keys}, error={str(e)}")
            raise
    
    async def exists(self, *keys: str) -> int:
        """检查键是否存在"""
        if not self._initialized or not self.redis:
            raise RuntimeError("Redis未初始化")
        try:
            return await self.redis.exists(*keys)
        except Exception as e:
            logger.error(f"Redis EXISTS操作失败: keys={keys}, error={str(e)}")
            raise
    
    async def keys(self, pattern: str = "*") -> List[str]:
        """获取匹配模式的键列表"""
        if not self._initialized or not self.redis:
            raise RuntimeError("Redis未初始化")
        try:
            return await self.redis.keys(pattern)
        except Exception as e:
            logger.error(f"Redis KEYS操作失败: pattern={pattern}, error={str(e)}")
            raise
    
    async def hget(self, name: str, key: str) -> Optional[str]:
        """获取哈希表字段值"""
        if not self._initialized or not self.redis:
            raise RuntimeError("Redis未初始化")
        try:
            return await self.redis.hget(name, key)
        except Exception as e:
            logger.error(f"Redis HGET操作失败: name={name}, key={key}, error={str(e)}")
            raise
    
    async def hset(self, name: str, key: str, value: Any) -> int:
        """设置哈希表字段值"""
        if not self._initialized or not self.redis:
            raise RuntimeError("Redis未初始化")
        try:
            # 如果是复杂对象，序列化为JSON
            if isinstance(value, (dict, list)):
                value = json.dumps(value, ensure_ascii=False)
            return await self.redis.hset(name, key, value)
        except Exception as e:
            logger.error(f"Redis HSET操作失败: name={name}, key={key}, error={str(e)}")
            raise
    
    async def hgetall(self, name: str) -> Dict[str, str]:
        """获取哈希表所有字段和值"""
        if not self._initialized or not self.redis:
            raise RuntimeError("Redis未初始化")
        try:
            return await self.redis.hgetall(name)
        except Exception as e:
            logger.error(f"Redis HGETALL操作失败: name={name}, error={str(e)}")
            raise
    
    async def hdel(self, name: str, *keys: str) -> int:
        """删除哈希表字段"""
        if not self._initialized or not self.redis:
            raise RuntimeError("Redis未初始化")
        try:
            return await self.redis.hdel(name, *keys)
        except Exception as e:
            logger.error(f"Redis HDEL操作失败: name={name}, keys={keys}, error={str(e)}")
            raise
    
    async def lpush(self, name: str, *values: Any) -> int:
        """从左侧推入列表"""
        if not self._initialized or not self.redis:
            raise RuntimeError("Redis未初始化")
        try:
            # 序列化复杂对象
            serialized_values = []
            for v in values:
                if isinstance(v, (dict, list)):
                    serialized_values.append(json.dumps(v, ensure_ascii=False))
                else:
                    serialized_values.append(v)
            return await self.redis.lpush(name, *serialized_values)
        except Exception as e:
            logger.error(f"Redis LPUSH操作失败: name={name}, error={str(e)}")
            raise
    
    async def rpush(self, name: str, *values: Any) -> int:
        """从右侧推入列表"""
        if not self._initialized or not self.redis:
            raise RuntimeError("Redis未初始化")
        try:
            # 序列化复杂对象
            serialized_values = []
            for v in values:
                if isinstance(v, (dict, list)):
                    serialized_values.append(json.dumps(v, ensure_ascii=False))
                else:
                    serialized_values.append(v)
            return await self.redis.rpush(name, *serialized_values)
        except Exception as e:
            logger.error(f"Redis RPUSH操作失败: name={name}, error={str(e)}")
            raise
    
    async def lrange(self, name: str, start: int = 0, end: int = -1) -> List[str]:
        """获取列表范围"""
        if not self._initialized or not self.redis:
            raise RuntimeError("Redis未初始化")
        try:
            return await self.redis.lrange(name, start, end)
        except Exception as e:
            logger.error(f"Redis LRANGE操作失败: name={name}, error={str(e)}")
            raise
    
    async def info(self, section: Optional[str] = None) -> Dict[str, Any]:
        """获取Redis服务器信息"""
        if not self._initialized or not self.redis:
            raise RuntimeError("Redis未初始化")
        try:
            info_str = await self.redis.info(section)
            # 解析INFO字符串为字典
            info_dict = {}
            for line in info_str.split('\n'):
                line = line.strip()
                if ':' in line and not line.startswith('#'):
                    key, value = line.split(':', 1)
                    key = key.strip()
                    value = value.strip()
                    # 尝试转换数值
                    try:
                        if '.' in value:
                            value = float(value)
                        else:
                            value = int(value)
                    except ValueError:
                        pass
                    info_dict[key] = value
            return info_dict
        except Exception as e:
            logger.error(f"Redis INFO操作失败: section={section}, error={str(e)}")
            raise
    
    async def get_next_id(self, counter_key: str) -> int:
        """获取下一个ID（使用Redis INCR命令）"""
        if not self._initialized or not self.redis:
            raise RuntimeError("Redis未初始化")
        try:
            # 使用INCR命令原子性地递增并返回新值
            # 如果键不存在，INCR会先将其设置为0，然后递增为1
            next_id = await self.redis.incr(counter_key)
            return next_id
        except Exception as e:
            logger.error(f"Redis INCR操作失败: counter_key={counter_key}, error={str(e)}")
            raise


def get_redis_manager() -> Optional["RedisDBManager"]:
    """获取全局Redis管理器实例"""
    global _redis_manager_instance
    if _redis_manager_instance is None:
        from config.settings import get_settings
        settings = get_settings()
        
        # 从DATABASE_URL解析Redis连接信息
        database_url = settings.DATABASE_URL
        logger.debug(f"[Redis管理器] 尝试创建Redis管理器实例，DATABASE_URL: {database_url}")
        
        if not database_url:
            logger.warning("[Redis管理器] DATABASE_URL为空，无法创建Redis管理器")
            return None
        
        if not (database_url.startswith("redis://") or database_url.startswith("rediss://")):
            logger.warning(f"[Redis管理器] DATABASE_URL不以redis://或rediss://开头: {database_url}")
            return None
        
        try:
            # 使用urllib.parse解析URL，支持特殊字符
            parsed = urlparse(database_url)
            logger.debug(f"[Redis管理器] URL解析结果: hostname={parsed.hostname}, port={parsed.port}, username={parsed.username}, password={'*' * len(parsed.password) if parsed.password else None}, path={parsed.path}")
            
            # 获取主机和端口
            host = parsed.hostname or "localhost"
            port = parsed.port or 6379
            
            # 获取密码（从netloc中的userinfo部分）
            password = None
            if parsed.password:
                # 格式: redis://:password@host:port 或 redis://username:password@host:port
                password = parsed.password
            elif parsed.username and "@" in parsed.netloc:
                # 格式可能是 redis://password@host:port（密码作为用户名）
                password = parsed.username
            
            # 获取数据库编号（从路径中，如 /0）
            db = 0
            if parsed.path:
                db_str = parsed.path.lstrip("/")
                if db_str:
                    try:
                        db = int(db_str)
                    except ValueError:
                        db = 0
            
            # 类已经定义，可以直接使用
            _redis_manager_instance = RedisDBManager(
                host=host,
                port=port,
                password=password,
                db=db
            )
            logger.info(f"[Redis管理器] 创建Redis管理器实例: {host}:{port}/{db}")
        except Exception as e:
            logger.error(f"[Redis管理器] 解析DATABASE_URL失败: {database_url}, 错误: {str(e)}", exc_info=True)
            _redis_manager_instance = None
    
    if _redis_manager_instance is None:
        logger.warning("[Redis管理器] get_redis_manager() 返回 None，Redis管理器未初始化")
    
    return _redis_manager_instance


async def get_redis_client() -> Redis:
    """获取Redis客户端（已初始化）"""
    redis_manager = get_redis_manager()
    if not redis_manager:
        raise RuntimeError("Redis管理器未初始化，请先检查数据库配置")
    
    if not redis_manager._initialized:
        await redis_manager.initialize()
    
    if not redis_manager.redis:
        raise RuntimeError("Redis连接未初始化")
    
    return redis_manager.redis


def get_redis_config_file_path() -> Optional[str]:
    """
    获取Redis配置文件路径
    
    常见路径:
    - Windows: C:\\Program Files\\Redis\\redis.conf 或安装目录下的redis.conf
    - Linux: /etc/redis/redis.conf 或 /usr/local/redis/redis.conf
    """
    # 常见的Redis配置文件路径
    possible_paths = [
        # Windows常见路径
        "C:\\Program Files\\Redis\\redis.conf",
        "C:\\Program Files (x86)\\Redis\\redis.conf",
        os.path.expanduser("~\\redis.conf"),
        # Linux常见路径
        "/etc/redis/redis.conf",
        "/etc/redis/6379.conf",
        "/usr/local/redis/redis.conf",
        "/usr/local/etc/redis.conf",
        # 当前目录
        os.path.join(os.getcwd(), "redis.conf"),
    ]
    
    # 从环境变量获取
    redis_config_path = os.environ.get("REDIS_CONFIG_PATH")
    if redis_config_path and os.path.exists(redis_config_path):
        return redis_config_path
    
    # 检查常见路径
    for path in possible_paths:
        if os.path.exists(path):
            return path
    
    return None


async def test_redis_connection(host: str = "localhost", port: int = 6379,
                                password: Optional[str] = None, db: int = 0) -> Dict[str, Any]:
    """
    测试Redis连接
    
    Returns:
        Dict包含success(bool)和message(str)
    """
    redis_manager = RedisDBManager(host=host, port=port, password=password, db=db)
    try:
        success = await redis_manager.initialize()
        if success:
            # 获取服务器信息
            info = await redis_manager.info()
            await redis_manager.close()
            return {
                "success": True,
                "message": "Redis连接测试成功",
                "info": {
                    "redis_version": info.get("redis_version", "unknown"),
                    "used_memory_human": info.get("used_memory_human", "unknown"),
                    "connected_clients": info.get("connected_clients", 0)
                }
            }
        else:
            return {
                "success": False,
                "message": "Redis连接初始化失败"
            }
    except ConnectionError as e:
        return {
            "success": False,
            "message": f"无法连接到Redis服务器: {str(e)}"
        }
    except TimeoutError as e:
        return {
            "success": False,
            "message": f"Redis连接超时: {str(e)}"
        }
    except Exception as e:
        return {
            "success": False,
            "message": f"Redis连接测试失败: {str(e)}"
        }

