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
        if status == "success":
            title = "✅ 备份任务完成"
            content = f"""## 备份任务完成通知

**备份名称**: {backup_name}
**状态**: 成功完成
**完成时间**: {datetime.now().strftime('%Y-%m-%d %H:%M:%S')}

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
**失败时间**: {datetime.now().strftime('%Y-%m-%d %H:%M:%S')}

"""
            if details:
                content += f"**错误信息**: {details.get('error', '未知错误')}\n"

        elif status == "started":
            title = "🚀 备份任务开始"
            content = f"""## 备份任务开始通知

**备份名称**: {backup_name}
**状态**: 正在执行
**开始时间**: {datetime.now().strftime('%Y-%m-%d %H:%M:%S')}

"""

        await self.send_message(self.default_phone, title, content)

    async def send_recovery_notification(self, recovery_name: str, status: str,
                                       details: Optional[Dict] = None):
        """发送恢复通知"""
        if status == "success":
            title = "✅ 恢复任务完成"
            content = f"""## 恢复任务完成通知

**恢复名称**: {recovery_name}
**状态**: 成功完成
**完成时间**: {datetime.now().strftime('%Y-%m-%d %H:%M:%S')}

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
**失败时间**: {datetime.now().strftime('%Y-%m-%d %H:%M:%S')}

"""
            if details:
                content += f"**错误信息**: {details.get('error', '未知错误')}\n"

        await self.send_message(self.default_phone, title, content)

    async def send_tape_notification(self, tape_id: str, action: str,
                                   details: Optional[Dict] = None):
        """发送磁带操作通知"""
        if action == "change_required":
            title = "📼 需要更换磁带"
            content = f"""## 磁带更换提醒

**磁带ID**: {tape_id}
**操作**: 需要更换磁带
**时间**: {datetime.now().strftime('%Y-%m-%d %H:%M:%S')}

请及时更换磁带以继续备份任务。

"""

        elif action == "expired":
            title = "⏰ 磁带已过期"
            content = f"""## 磁带过期通知

**磁带ID**: {tape_id}
**状态**: 数据保留期已满
**时间**: {datetime.now().strftime('%Y-%m-%d %H:%M:%S')}

磁带将被自动擦除并重新投入使用。

"""

        elif action == "error":
            title = "⚠️ 磁带操作异常"
            content = f"""## 磁带操作异常通知

**磁带ID**: {tape_id}
**状态**: 操作异常
**时间**: {datetime.now().strftime('%Y-%m-%d %H:%M:%S')}

"""
            if details:
                content += f"**错误信息**: {details.get('error', '未知错误')}\n"

        await self.send_message(self.default_phone, title, content)

    async def send_system_notification(self, title: str, content: str):
        """发送系统通知"""
        formatted_content = f"""## 系统通知

{content}

**时间**: {datetime.now().strftime('%Y-%m-%d %H:%M:%S')}
"""
        await self.send_message(self.default_phone, title, formatted_content)

    async def send_capacity_warning(self, used_percent: float, details: Optional[Dict] = None):
        """发送容量预警通知"""
        title = "⚠️ 存储容量预警"
        content = f"""## 存储容量预警

**当前使用率**: {used_percent:.1f}%
**时间**: {datetime.now().strftime('%Y-%m-%d %H:%M:%S')}

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