#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
磁带管理API - crud
Tape Management API - crud
"""

import logging
import traceback
import json
import re
import os
import asyncio
from typing import List, Dict, Any, Optional
from datetime import datetime
from fastapi import APIRouter, HTTPException, Request, Depends, BackgroundTasks
from pydantic import BaseModel

from .models import CreateTapeRequest, UpdateTapeRequest
from models.system_log import OperationType, LogCategory, LogLevel
from utils.log_utils import log_operation, log_system
from utils.scheduler.db_utils import is_opengauss, get_opengauss_connection
from utils.tape_tools import tape_tools_manager
from config.database import db_manager

logger = logging.getLogger(__name__)
router = APIRouter()


# 辅助函数已移动到各个模块文件中
# 路由函数已拆分到以下文件：
# - tape_create.py: create_tape
# - tape_query.py: get_tape, check_tape_exists, list_tapes, get_current_tape, update_tape, get_tape_inventory
# - tape_update.py: update_tape
# - tape_statistics.py: get_tape_inventory
# - tape_history.py: get_tape_history
# - tape_delete.py: delete_tape

# 所有函数已拆分到对应的模块文件中，此文件保留为空文件以保持向后兼容

