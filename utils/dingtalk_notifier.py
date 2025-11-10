#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
钉钉通知模块
DingTalk Notification Module
"""

import json
import asyncio
import logging
from typing import Optional, Dict, Any
from datetime import datetime
from utils.datetime_utils import now, format_datetime
from pathlib import Path
import aiohttp

from config.settings import get_settings

logger = logging.getLogger(__name__)


class DingTalkNotifier:
    """钉钉通知器"""

    def __init__(self):
        self.settings = get_settings()
        self.api_url = self.settings.DINGTALK_API_URL
        self.api_key = self.settings.DINGTALK_API_KEY
        self.default_phone = self.settings.DINGTALK_DEFAULT_PHONE
        self._session = None
        self._notification_events = None

    async def initialize(self):
        """初始化通知器"""
        # 创建 aiohttp 会话
        connector = aiohttp.TCPConnector(limit=10, limit_per_host=5)
        timeout = aiohttp.ClientTimeout(total=30)
        self._session = aiohttp.ClientSession(
            connector=connector,
            timeout=timeout
        )
        logger.info("钉钉通知器初始化完成")

    async def close(self):
        """关闭通知器"""
        if self._session:
            await self._session.close()
            logger.info("钉钉通知器已关闭")

    def _load_notification_events(self) -> Dict[str, bool]:
        """加载通知事件配置"""
        if self._notification_events is not None:
            return self._notification_events
        
        # 从.env文件读取
        env_file = Path(".env")
        if env_file.exists():
            with open(env_file, "r", encoding="utf-8") as f:
                for line in f:
                    line = line.strip()
                    if line.startswith("NOTIFICATION_EVENTS="):
                        events_json = line.split("=", 1)[1]
                        self._notification_events = json.loads(events_json)
                        return self._notification_events
        
        # 如果.env中没有，返回默认配置（全部启用）
        self._notification_events = {
            "notify_backup_success": True,
            "notify_backup_started": True,
            "notify_backup_failed": True,
            "notify_recovery_success": True,
            "notify_recovery_failed": True,
            "notify_tape_change": True,
            "notify_tape_expired": True,
            "notify_tape_error": True,
            "notify_capacity_warning": True,
            "notify_system_error": True,
            "notify_system_started": True
        }
        return self._notification_events

    def _should_send_notification(self, event_name: str) -> bool:
        """检查是否应该发送某个通知事件"""
        events = self._load_notification_events()
        return events.get(event_name, True)

    async def send_message(self, phone: str, title: str, content: str,
                          message_type: str = "markdown") -> Dict[str, Any]:
        """发送单条消息"""
        try:
            url = f"{self.api_url}/api/v1/messages/send"
            headers = {
                "Content-Type": "application/json",
                "Authorization": f"Bearer {self.api_key}"
            }

            payload = {
                "phone": phone,
                "title": title,
                "content": content,
                "message_type": message_type
            }

            async with self._session.post(url, headers=headers, json=payload) as response:
                result = await response.json()

                if result.get('success'):
                    logger.info(f"钉钉消息发送成功: {title} -> {phone}")
                else:
                    logger.error(f"钉钉消息发送失败: {result.get('message', '未知错误')}")

                return result

        except Exception as e:
            logger.error(f"发送钉钉消息异常: {str(e)}")
            return {
                'success': False,
                'message': f'发送异常: {str(e)}'
            }

    async def send_batch_message(self, phones: list, title: str, content: str,
                                message_type: str = "markdown") -> Dict[str, Any]:
        """发送批量消息"""
        try:
            url = f"{self.api_url}/api/v1/messages/batch"
            headers = {
                "Content-Type": "application/json",
                "Authorization": f"Bearer {self.api_key}"
            }

            payload = {
                "phones": phones,
                "title": title,
                "content": content,
                "message_type": message_type
            }

            async with self._session.post(url, headers=headers, json=payload) as response:
                result = await response.json()

                if result.get('success'):
                    logger.info(f"钉钉批量消息发送成功: {title} -> {len(phones)}个接收者")
                else:
                    logger.error(f"钉钉批量消息发送失败: {result.get('message', '未知错误')}")

                return result

        except Exception as e:
            logger.error(f"发送钉钉批量消息异常: {str(e)}")
            return {
                'success': False,
                'message': f'发送异常: {str(e)}'
            }

    async def send_backup_notification(self, backup_name: str, status: str,
                                     details: Optional[Dict] = None):
        """发送备份通知"""
        # 根据状态检查是否应该发送
        if status == "success" and not self._should_send_notification("notify_backup_success"):
            logger.debug("备份成功通知已禁用")
            return
        elif status == "failed" and not self._should_send_notification("notify_backup_failed"):
            logger.debug("备份失败通知已禁用")
            return
        elif status == "started" and not self._should_send_notification("notify_backup_started"):
            logger.debug("备份开始通知已禁用")
            return
        
        if status == "success":
            title = "✅ 备份任务完成"
            content = f"""## 备份任务完成通知

**备份名称**: {backup_name}
**状态**: 成功完成
**完成时间**: {format_datetime(now())}

"""
            if details:
                content += f"**备份大小**: {details.get('size', 'N/A')}\n"
                content += f"**文件数量**: {details.get('file_count', 'N/A')}\n"
                content += f"**压缩率**: {details.get('compression_ratio', 'N/A')}\n"
                content += f"**耗时**: {details.get('duration', 'N/A')}\n"

        elif status == "failed":
            title = "❌ 备份任务失败"
            content = f"""## 备份任务失败通知

**备份名称**: {backup_name}
**状态**: 执行失败
**失败时间**: {format_datetime(now())}

"""
            if details:
                content += f"**错误信息**: {details.get('error', '未知错误')}\n"

        elif status == "started":
            title = "🚀 备份任务开始"
            content = f"""## 备份任务开始通知

**备份名称**: {backup_name}
**状态**: 正在执行
**开始时间**: {format_datetime(now())}

"""

        await self.send_message(self.default_phone, title, content)

    async def send_recovery_notification(self, recovery_name: str, status: str,
                                       details: Optional[Dict] = None):
        """发送恢复通知"""
        # 根据状态检查是否应该发送
        if status == "success" and not self._should_send_notification("notify_recovery_success"):
            logger.debug("恢复成功通知已禁用")
            return
        elif status == "failed" and not self._should_send_notification("notify_recovery_failed"):
            logger.debug("恢复失败通知已禁用")
            return
        
        if status == "success":
            title = "✅ 恢复任务完成"
            content = f"""## 恢复任务完成通知

**恢复名称**: {recovery_name}
**状态**: 成功完成
**完成时间**: {format_datetime(now())}

"""
            if details:
                content += f"**恢复文件数**: {details.get('file_count', 'N/A')}\n"
                content += f"**恢复大小**: {details.get('size', 'N/A')}\n"
                content += f"**耗时**: {details.get('duration', 'N/A')}\n"

        elif status == "failed":
            title = "❌ 恢复任务失败"
            content = f"""## 恢复任务失败通知

**恢复名称**: {recovery_name}
**状态**: 执行失败
**失败时间**: {format_datetime(now())}

"""
            if details:
                content += f"**错误信息**: {details.get('error', '未知错误')}\n"

        await self.send_message(self.default_phone, title, content)

    async def send_tape_notification(self, tape_id: str, action: str,
                                   details: Optional[Dict] = None):
        """发送磁带操作通知"""
        # 根据动作检查是否应该发送
        if action == "expired" and not self._should_send_notification("notify_tape_expired"):
            logger.debug("磁带过期通知已禁用")
            return
        elif action == "error" and not self._should_send_notification("notify_tape_error"):
            logger.debug("磁带错误通知已禁用")
            return
        elif action == "change_required" and not self._should_send_notification("notify_tape_change"):
            logger.debug("磁带更换通知已禁用")
            return
        
        if action == "change_required":
            title = "📼 需要更换磁带"
            content = f"""## 磁带更换提醒

**磁带ID**: {tape_id}
**操作**: 需要更换磁带
**时间**: {format_datetime(now())}

请及时更换磁带以继续备份任务。

"""

        elif action == "expired":
            title = "⏰ 磁带已过期"
            content = f"""## 磁带过期通知

**磁带ID**: {tape_id}
**状态**: 数据保留期已满
**时间**: {format_datetime(now())}

磁带将被自动擦除并重新投入使用。

"""

        elif action == "error":
            title = "⚠️ 磁带操作异常"
            content = f"""## 磁带操作异常通知

**磁带ID**: {tape_id}
**状态**: 操作异常
**时间**: {format_datetime(now())}

"""
            if details:
                content += f"**错误信息**: {details.get('error', '未知错误')}\n"

        await self.send_message(self.default_phone, title, content)

    async def send_system_notification(self, title: str, content: str):
        """发送系统通知"""
        # 检查是否应该发送系统通知
        if not self._should_send_notification("notify_system_started"):
            logger.debug("系统启动通知已禁用")
            return
        
        formatted_content = f"""## 系统通知

{content}

**时间**: {format_datetime(now())}
"""
        await self.send_message(self.default_phone, title, formatted_content)

    async def send_tape_format_notification(self, tape_id: str, status: str, 
                                           error_detail: Optional[str] = None,
                                           volume_label: Optional[str] = None,
                                           serial_number: Optional[str] = None):
        """发送磁带格式化通知"""
        # 检查是否应该发送通知
        if status == "failed" and not self._should_send_notification("notify_tape_error"):
            logger.debug("磁带格式化失败通知已禁用")
            return
        
        try:
            if status == "success":
                title = "✅ 磁带格式化完成"
                content = f"""## 磁带格式化完成通知

**磁带ID**: {tape_id}
**状态**: 格式化成功
**完成时间**: {format_datetime(now())}
"""
                if volume_label:
                    content += f"**卷标**: {volume_label}\n"
                if serial_number:
                    content += f"**序列号**: {serial_number}\n"
                content += "\n磁带已成功格式化，可以正常使用。"
            elif status == "failed":
                title = "❌ 磁带格式化失败"
                content = f"""## 磁带格式化失败通知

**磁带ID**: {tape_id}
**状态**: 格式化失败
**失败时间**: {format_datetime(now())}
"""
                if volume_label:
                    content += f"**卷标**: {volume_label}\n"
                if serial_number:
                    content += f"**序列号**: {serial_number}\n"
                if error_detail:
                    content += f"\n**错误详情**:\n```\n{error_detail}\n```\n"
                content += "\n请检查设备状态和磁带是否正确加载。"
            else:
                return
            
            await self.send_message(self.default_phone, title, content)
        except Exception as e:
            logger.error(f"发送磁带格式化通知失败: {str(e)}")

    async def send_capacity_warning(self, used_percent: float, details: Optional[Dict] = None):
        """发送容量预警通知"""
        # 检查是否应该发送容量预警
        if not self._should_send_notification("notify_capacity_warning"):
            logger.debug("容量预警通知已禁用")
            return
        
        title = "⚠️ 存储容量预警"
        content = f"""## 存储容量预警

**当前使用率**: {used_percent:.1f}%
**时间**: {format_datetime(now())}

"""
        if details:
            content += f"**总容量**: {details.get('total', 'N/A')}\n"
            content += f"**已使用**: {details.get('used', 'N/A')}\n"
            content += f"**剩余空间**: {details.get('free', 'N/A')}\n"

        await self.send_message(self.default_phone, title, content)

    async def test_connection(self) -> bool:
        """测试连接"""
        try:
            test_url = f"{self.api_url}/api/v1/health"
            headers = {
                "Authorization": f"Bearer {self.api_key}"
            }

            async with self._session.get(test_url, headers=headers) as response:
                if response.status == 200:
                    logger.info("钉钉API连接测试成功")
                    return True
                else:
                    logger.error(f"钉钉API连接测试失败: {response.status}")
                    return False

        except Exception as e:
            logger.error(f"钉钉API连接测试异常: {str(e)}")
            return False

    async def get_message_status(self, task_id: str) -> Dict[str, Any]:
        """获取消息状态"""
        try:
            url = f"{self.api_url}/api/v1/tasks/{task_id}"
            headers = {
                "Authorization": f"Bearer {self.api_key}"
            }

            async with self._session.get(url, headers=headers) as response:
                result = await response.json()
                return result

        except Exception as e:
            logger.error(f"获取消息状态异常: {str(e)}")
            return {
                'success': False,
                'message': f'查询异常: {str(e)}'
            }