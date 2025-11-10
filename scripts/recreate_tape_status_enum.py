#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
重新创建磁带状态枚举类型（使用小写）
Recreate TapeStatus Enum with Lowercase Values
"""

import sys
from pathlib import Path

# 添加项目根目录到路径
project_root = Path(__file__).parent.parent
sys.path.insert(0, str(project_root))


def recreate_tape_status_enum():
    """重新创建磁带状态枚举类型"""
    import psycopg2
    from config.settings import get_settings
    import re
    
    settings = get_settings()
    print("重新创建磁带状态枚举类型...")
    
    try:
        # 解析DATABASE_URL
        match = re.match(r'(?:opengauss|postgresql)(?:\+psycopg2)?://([^:]+):([^@]+)@([^:]+):(\d+)/(.+)', settings.DATABASE_URL)
        if not match:
            print(f"[ERROR] 无法解析DATABASE_URL")
            return
        
        user, password, host, port, database = match.groups()
        conn = psycopg2.connect(
            host=host,
            port=int(port),
            database=database,
            user=user,
            password=password
        )
        conn.autocommit = False
        
        with conn.cursor() as cur:
            print("\n步骤1: 备份现有数据")
            cur.execute("""
                CREATE TEMP TABLE tape_cartridges_backup AS 
                SELECT * FROM tape_cartridges
            """)
            backup_count = cur.rowcount
            print(f"  - 备份了 {backup_count} 条记录")
            
            print("\n步骤2: 将状态列改为文本类型")
            cur.execute("""
                ALTER TABLE tape_cartridges 
                ALTER COLUMN status TYPE VARCHAR(50)
            """)
            print("  - 列类型已更改为VARCHAR")
            
            print("\n步骤3: 删除旧的枚举类型")
            cur.execute("DROP TYPE IF EXISTS tapestatus CASCADE")
            print("  - 旧枚举类型已删除")
            
            print("\n步骤4: 创建新的枚举类型（小写）")
            cur.execute("""
                CREATE TYPE tapestatus AS ENUM (
                    'new', 'available', 'in_use', 'full', 
                    'expired', 'error', 'maintenance', 'retired'
                )
            """)
            print("  - 新枚举类型已创建（使用小写值）")
            
            print("\n步骤5: 更新数据为小写")
            status_map = {
                'NEW': 'new',
                'AVAILABLE': 'available',
                'IN_USE': 'in_use',
                'FULL': 'full',
                'EXPIRED': 'expired',
                'ERROR': 'error',
                'MAINTENANCE': 'maintenance',
                'RETIRED': 'retired'
            }
            
            for old_val, new_val in status_map.items():
                cur.execute(
                    "UPDATE tape_cartridges SET status = %s WHERE status = %s",
                    (new_val, old_val)
                )
                if cur.rowcount > 0:
                    print(f"  - 更新 {old_val} -> {new_val}: {cur.rowcount} 条")
            
            print("\n步骤6: 将状态列改回枚举类型")
            cur.execute("""
                ALTER TABLE tape_cartridges 
                ALTER COLUMN status TYPE tapestatus 
                USING status::tapestatus
            """)
            print("  - 列类型已更改为枚举类型")
            
            print("\n步骤7: 设置默认值")
            cur.execute("""
                ALTER TABLE tape_cartridges 
                ALTER COLUMN status SET DEFAULT 'new'::tapestatus
            """)
            print("  - 默认值已设置")
            
            # 提交事务
            conn.commit()
            print("\n[OK] 磁带状态枚举类型重建完成！")
            print("      所有磁带状态值已更新为小写格式")
            
    except Exception as e:
        print(f"\n[ERROR] 操作失败: {str(e)}")
        if 'conn' in locals():
            conn.rollback()
            print("  - 已回滚所有更改")
        import traceback
        traceback.print_exc()
    finally:
        if 'conn' in locals():
            conn.close()


if __name__ == "__main__":
    recreate_tape_status_enum()

