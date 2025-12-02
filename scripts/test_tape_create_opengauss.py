#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
测试 openGauss 模式下创建/更新磁带记录
Test create/update tape record in openGauss mode with psycopg3 binary protocol
"""

import asyncio
import sys
from pathlib import Path
from datetime import datetime

# Windows 下需要设置事件循环策略（psycopg3 要求）
if sys.platform == 'win32':
    asyncio.set_event_loop_policy(asyncio.WindowsSelectorEventLoopPolicy())

# 添加项目根目录到路径
project_root = Path(__file__).parent.parent
sys.path.insert(0, str(project_root))

from utils.scheduler.db_utils import is_opengauss, get_opengauss_connection


async def test_create_tape_opengauss():
    """测试 openGauss 模式下创建磁带记录"""
    print("\n" + "=" * 80)
    print("测试: openGauss 模式下创建/更新磁带记录")
    print("=" * 80)
    
    if not is_opengauss():
        print("\n[SKIP] 当前不是 openGauss 模式，跳过测试")
        return False
    
    try:
        # 测试数据
        tape_id_value = "TP20251101"
        final_label = "TP20251101"
        final_serial = "TP1101"
        capacity_bytes = 18 * 1024 * (1024 ** 3)  # 18TB
        retention_months = 12
        created_date = datetime(2025, 11, 1)
        expiry_year = created_date.year
        expiry_month = created_date.month + retention_months
        while expiry_month > 12:
            expiry_year += 1
            expiry_month -= 12
        expiry_date = datetime(expiry_year, expiry_month, 1)
        
        print(f"\n测试数据:")
        print(f"  tape_id: {tape_id_value}")
        print(f"  label: {final_label}")
        print(f"  serial_number: {final_serial}")
        print(f"  capacity_bytes: {capacity_bytes / (1024**3):.2f} GB")
        print(f"  retention_months: {retention_months}")
        print(f"  created_date: {created_date}")
        print(f"  expiry_date: {expiry_date}")
        
        # 使用 openGauss 连接池
        async with get_opengauss_connection() as conn:
            print(f"\n[1] 检查磁带是否已存在...")
            tape_id_row = await conn.fetchrow(
                "SELECT 1 FROM tape_cartridges WHERE tape_id = $1",
                tape_id_value
            )
            tape_exists = tape_id_row is not None
            
            if not tape_exists:
                label_row = await conn.fetchrow(
                    "SELECT 1 FROM tape_cartridges WHERE label = $1",
                    final_label
                )
                label_exists = label_row is not None
            else:
                label_exists = False
            
            print(f"  磁带存在: {tape_exists}, 卷标存在: {label_exists}")
            
            if tape_exists or label_exists:
                print(f"\n[2] 更新现有磁带记录...")
                # openGauss 模式下，参数需要展开传递（*params），而不是作为元组传递
                # 注意：直接使用字符串值，不使用类型转换（::tape_status），让数据库自动转换
                await conn.execute("""
                    UPDATE tape_cartridges
                    SET label = $1, status = $2, serial_number = $3, capacity_bytes = $4,
                        retention_months = $5, manufactured_date = $6, expiry_date = $7, updated_at = NOW()
                    WHERE tape_id = $8 OR label = $9
                """,
                    final_label,
                    'available',  # 直接使用字符串值，不使用类型转换
                    final_serial,
                    capacity_bytes,
                    retention_months,
                    created_date,
                    expiry_date,
                    tape_id_value,
                    final_label
                )
                print(f"  ✅ UPDATE 执行成功")
            else:
                print(f"\n[2] 创建新磁带记录...")
                # openGauss 模式下，参数需要展开传递（*params），而不是作为元组传递
                # 注意：直接使用字符串值，不使用类型转换（::tape_status），让数据库自动转换
                await conn.execute("""
                    INSERT INTO tape_cartridges 
                    (tape_id, label, status, media_type, generation, serial_number, location,
                     capacity_bytes, used_bytes, retention_months, notes, manufactured_date, expiry_date, auto_erase, health_score)
                    VALUES ($1, $2, $3, $4, $5, $6, $7, $8, $9, $10, $11, $12, $13, $14, $15)
                """,
                    tape_id_value,
                    final_label,
                    'available',  # 直接使用字符串值，不使用类型转换
                    'LTO',
                    9,  # generation 是整数类型，9 表示 LTO-9
                    final_serial,
                    '',
                    capacity_bytes,
                    0,
                    retention_months,
                    '完整备份前格式化',
                    created_date,
                    expiry_date,
                    True,
                    100
                )
                print(f"  ✅ INSERT 执行成功")
            
            # psycopg3 binary protocol 需要显式提交事务
            print(f"\n[3] 显式提交事务...")
            try:
                # 获取实际连接对象（兼容 psycopg3 包装器）
                actual_conn = conn._conn if hasattr(conn, '_conn') else conn
                
                # 事务状态映射
                status_map = {0: 'IDLE', 1: 'INTRANS', 3: 'INERROR'}
                
                # 检查提交前的事务状态
                if hasattr(actual_conn, 'info') and hasattr(actual_conn.info, 'transaction_status'):
                    status_before = actual_conn.info.transaction_status
                    print(f"  提交前事务状态: {status_map.get(status_before, 'UNKNOWN')} ({status_before})")
                
                await actual_conn.commit()
                
                # 等待一小段时间让状态更新
                await asyncio.sleep(0.01)
                
                # 检查提交后的事务状态
                if hasattr(actual_conn, 'info') and hasattr(actual_conn.info, 'transaction_status'):
                    status_after = actual_conn.info.transaction_status
                    print(f"  提交后事务状态: {status_map.get(status_after, 'UNKNOWN')} ({status_after})")
                    if status_after == 0:
                        print(f"  ✅ 事务提交成功，连接状态=IDLE")
                    else:
                        print(f"  ⚠️  事务状态异常，状态={status_after}")
                else:
                    print(f"  ✅ 事务提交成功（无法检查状态）")
            except Exception as commit_err:
                print(f"  ⚠️  提交事务失败（可能已自动提交）: {commit_err}")
            
            # 验证数据
            print(f"\n[4] 验证数据...")
            verify_row = await conn.fetchrow(
                "SELECT tape_id, label, status, serial_number, capacity_bytes FROM tape_cartridges WHERE tape_id = $1",
                tape_id_value
            )
            if verify_row:
                print(f"  ✅ 验证成功:")
                print(f"    tape_id: {verify_row['tape_id']}")
                print(f"    label: {verify_row['label']}")
                print(f"    status: {verify_row['status']}")
                print(f"    serial_number: {verify_row['serial_number']}")
                print(f"    capacity_bytes: {verify_row['capacity_bytes'] / (1024**3):.2f} GB")
            else:
                print(f"  ❌ 验证失败: 找不到磁带记录")
                return False
        
        print(f"\n[OK] 测试通过！")
        return True
        
    except Exception as e:
        print(f"\n[FAIL] 测试失败: {str(e)}")
        import traceback
        traceback.print_exc()
        return False


async def main():
    """主函数"""
    success = await test_create_tape_opengauss()
    sys.exit(0 if success else 1)


if __name__ == "__main__":
    asyncio.run(main())

