#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
调度时间计算器
Schedule Time Calculator
"""

import logging
from datetime import datetime, timedelta
from typing import Optional
from croniter import croniter

from models.scheduled_task import ScheduledTask, ScheduleType
from utils.datetime_utils import parse_datetime, now

logger = logging.getLogger(__name__)


def calculate_next_run_time(scheduled_task: ScheduledTask) -> Optional[datetime]:
    """计算下次执行时间"""
    try:
        config = scheduled_task.schedule_config or {}
        schedule_type = scheduled_task.schedule_type
        current_time = now()  # 使用统一的日期时间工具
        
        if schedule_type == ScheduleType.ONCE:
            # 一次性任务：某月某日某时
            datetime_str = config.get('datetime')
            if datetime_str:
                # 使用统一的日期时间解析工具
                next_time = parse_datetime(datetime_str)
                if next_time is None:
                    logger.error(f"无法解析时间格式: {datetime_str}")
                    return None
                # 如果已经过了执行时间，返回None（不再执行）
                if next_time <= current_time:
                    return None
                return next_time
                
        elif schedule_type == ScheduleType.INTERVAL:
            # 间隔任务：每N分钟/小时/天
            interval = config.get('interval', 60)
            unit = config.get('unit', 'minutes')  # minutes/hours/days
            
            if unit == 'minutes':
                delta = timedelta(minutes=interval)
            elif unit == 'hours':
                delta = timedelta(hours=interval)
            elif unit == 'days':
                delta = timedelta(days=interval)
            else:
                logger.error(f"不支持的间隔单位: {unit}")
                return None
            
            # 如果从未执行过，从当前时间开始
            if not scheduled_task.last_run_time:
                return current_time + delta
            
            # 从上次执行时间开始计算
            last_run = scheduled_task.last_run_time
            next_time = last_run + delta
            
            # 如果下次执行时间已经过了，从当前时间开始
            if next_time <= current_time:
                next_time = current_time + delta
                
            return next_time
            
        elif schedule_type == ScheduleType.DAILY:
            # 每日任务：每天固定时间
            time_str = config.get('time', '02:00:00')
            # 支持 HH:MM 和 HH:MM:SS 格式
            time_parts = time_str.split(':')
            hour = int(time_parts[0])
            minute = int(time_parts[1]) if len(time_parts) > 1 else 0
            second = int(time_parts[2]) if len(time_parts) > 2 else 0
            
            next_time = current_time.replace(hour=hour, minute=minute, second=second, microsecond=0)
            if next_time <= current_time:
                # 如果今天的时间已过，执行明天的
                next_time += timedelta(days=1)
                
            return next_time
            
        elif schedule_type == ScheduleType.WEEKLY:
            # 每周任务：每周固定星期几的固定时间
            day_of_week = config.get('day_of_week', 0)  # 0=Monday, 6=Sunday
            time_str = config.get('time', '02:00:00')
            # 支持 HH:MM 和 HH:MM:SS 格式
            time_parts = time_str.split(':')
            hour = int(time_parts[0])
            minute = int(time_parts[1]) if len(time_parts) > 1 else 0
            second = int(time_parts[2]) if len(time_parts) > 2 else 0
            
            current_weekday = current_time.weekday()  # 0=Monday, 6=Sunday
            days_ahead = day_of_week - current_weekday
            
            # 构建时间对象用于比较
            time_obj = datetime.strptime(f"{hour:02d}:{minute:02d}:{second:02d}", '%H:%M:%S').time()
            if days_ahead < 0 or (days_ahead == 0 and current_time.time() >= time_obj):
                days_ahead += 7
                
            next_time = current_time + timedelta(days=days_ahead)
            next_time = next_time.replace(hour=hour, minute=minute, second=second, microsecond=0)
            
            return next_time
            
        elif schedule_type == ScheduleType.MONTHLY:
            # 每月任务：每月固定日期的固定时间
            day_of_month = config.get('day_of_month', 1)
            time_str = config.get('time', '02:00:00')
            # 支持 HH:MM 和 HH:MM:SS 格式
            time_parts = time_str.split(':')
            hour = int(time_parts[0])
            minute = int(time_parts[1]) if len(time_parts) > 1 else 0
            second = int(time_parts[2]) if len(time_parts) > 2 else 0
            
            next_time = current_time.replace(day=day_of_month, hour=hour, minute=minute, second=second, microsecond=0)
            if next_time <= current_time:
                # 如果本月的日期已过，执行下个月的
                if next_time.month == 12:
                    next_time = next_time.replace(year=next_time.year + 1, month=1)
                else:
                    next_time = next_time.replace(month=next_time.month + 1)
            
            return next_time
            
        elif schedule_type == ScheduleType.YEARLY:
            # 每年任务：每年固定月日的固定时间
            month = config.get('month', 1)
            day = config.get('day', 1)
            time_str = config.get('time', '02:00:00')
            # 支持 HH:MM 和 HH:MM:SS 格式
            time_parts = time_str.split(':')
            hour = int(time_parts[0])
            minute = int(time_parts[1]) if len(time_parts) > 1 else 0
            second = int(time_parts[2]) if len(time_parts) > 2 else 0
            
            next_time = current_time.replace(month=month, day=day, hour=hour, minute=minute, second=second, microsecond=0)
            if next_time <= current_time:
                # 如果今年的日期已过，执行明年的
                next_time = next_time.replace(year=next_time.year + 1)
            
            return next_time
            
        elif schedule_type == ScheduleType.CRON:
            # Cron表达式
            cron_expr = config.get('cron')
            if cron_expr:
                cron = croniter(cron_expr, current_time)
                return cron.get_next(datetime)
                
        return None
        
    except Exception as e:
        logger.error(f"计算下次执行时间失败: {str(e)}")
        return None

