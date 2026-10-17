#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""SSO 账号本地授权管理 API

对应「系统设置 → 管理员设置」页面：
  - GET  /api/system/sso-users        列出所有登录过的账号（含被拒绝的）
  - PUT  /api/system/sso-users/{id}   修改本地角色（admin / user / denied）

grigs 为永久管理员，任何修改请求都会被拒绝（403）。
"""
import logging
from typing import Any, Dict, List, Optional

from fastapi import APIRouter, HTTPException
from pydantic import BaseModel, Field

from web.yz_auth.user_registry import (
    VALID_ROLES,
    delete_user as delete_sso_user,
    is_permanent_admin,
    list_users,
    set_role,
)

logger = logging.getLogger(__name__)
router = APIRouter()


class RoleUpdateRequest(BaseModel):
    local_role: str = Field(..., description="本地角色：admin / user / denied")


@router.get("/sso-users", response_model=List[Dict[str, Any]])
async def get_sso_users():
    """列出所有通过 yz-login 登录过的账号（含被拒绝的）"""
    try:
        users = await list_users()
        # datetime → ISO 字符串，避免前端解析差异
        for u in users:
            for k in ("last_login_at", "first_seen_at"):
                v = u.get(k)
                if v is not None and hasattr(v, "isoformat"):
                    u[k] = v.isoformat()
        return users
    except Exception as e:
        logger.error(f"获取 SSO 账号列表失败: {e}", exc_info=True)
        raise HTTPException(status_code=500, detail=f"获取账号列表失败: {e}")


@router.put("/sso-users/{yz_user_id}")
async def update_sso_user_role(yz_user_id: int, payload: RoleUpdateRequest):
    """修改某账号的本地角色（grigs 永久管理员不可修改）"""
    role = (payload.local_role or "").strip().lower()
    if role not in VALID_ROLES:
        raise HTTPException(
            status_code=400,
            detail=f"无效角色: {payload.local_role}（仅支持 {', '.join(VALID_ROLES)}）",
        )
    try:
        ok = await set_role(yz_user_id, role)
        return {"success": bool(ok), "yz_user_id": yz_user_id, "local_role": role}
    except PermissionError as pe:
        # grigs 永久管理员
        raise HTTPException(status_code=403, detail=str(pe))
    except ValueError as ve:
        raise HTTPException(status_code=404, detail=str(ve))
    except Exception as e:
        logger.error(f"修改 SSO 账号角色失败: {e}", exc_info=True)
        raise HTTPException(status_code=500, detail=f"修改角色失败: {e}")


@router.delete("/sso-users/{yz_user_id}")
async def delete_sso_user_record(yz_user_id: int):
    """删除某账号的记录（grigs 永久管理员不可删除）。

    删除后该账号下次登录会被视作首次出现，重新继承 yz 侧角色。
    """
    try:
        ok = await delete_sso_user(yz_user_id)
        return {"success": bool(ok), "yz_user_id": yz_user_id}
    except PermissionError as pe:
        raise HTTPException(status_code=403, detail=str(pe))
    except ValueError as ve:
        raise HTTPException(status_code=404, detail=str(ve))
    except Exception as e:
        logger.error(f"删除 SSO 账号失败: {e}", exc_info=True)
        raise HTTPException(status_code=500, detail=f"删除失败: {e}")
