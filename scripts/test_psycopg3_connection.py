#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
测试 psycopg3 连接和查询功能
Test psycopg3 connection and query functionality
"""

import sys
import asyncio
from pathlib import Path

# 添加项目根目录到Python路径
project_root = Path(__file__).parent.parent
sys.path.insert(0, str(project_root))

# Windows 平台：设置事件循环策略（psycopg3 需要）
if sys.platform == "win32":
    try:
        import psycopg_pool
        asyncio.set_event_loop_policy(asyncio.WindowsSelectorEventLoopPolicy())
        print("[OK] Windows 事件循环策略已设置为 SelectorEventLoop（psycopg3 兼容）")
    except ImportError:
        print("[INFO] psycopg3 未安装，将使用 asyncpg")
    except Exception as e:
        print(f"[WARN] 设置事件循环策略时出错: {e}")

from config.settings import Settings
from config.database import db_manager
from utils.scheduler.db_utils import (
    get_opengauss_connection,
    is_opengauss,
    close_opengauss_pool
)


async def test_connection():
    """测试数据库连接"""
    print("\n" + "=" * 80)
    print("测试 1: 数据库连接")
    print("=" * 80)
    
    try:
        # 初始化数据库管理器设置（跳过表创建，测试不需要）
        settings = Settings()
        # 直接设置 settings，不调用 initialize（避免需要 psycopg2）
        db_manager.settings = settings
        print("✓ 数据库管理器设置初始化成功")
        
        # 检查数据库类型
        if is_opengauss():
            print("✓ 检测到 openGauss/PostgreSQL 数据库")
        else:
            print("⚠ 未检测到 openGauss/PostgreSQL 数据库，跳过测试")
            return False
        
        # 测试获取连接
        async with get_opengauss_connection() as conn:
            print("[OK] 成功获取数据库连接")
            print(f"  连接类型: {type(conn)}")
            
            # 检查是否是兼容层包装的连接
            if hasattr(conn, '_conn'):
                print(f"  [OK] 使用兼容层包装 (AsyncPGCompatConnection)")
                print(f"  实际连接类型: {type(conn._conn)}")
            else:
                print(f"  [WARN] 未使用兼容层，直接使用原始连接")
        
        print("[OK] 连接测试通过")
        return True
        
    except Exception as e:
        print(f"[FAIL] 连接测试失败: {e}")
        import traceback
        traceback.print_exc()
        return False


async def test_basic_query():
    """测试基本查询功能"""
    print("\n" + "=" * 80)
    print("测试 2: 基本查询功能")
    print("=" * 80)
    
    try:
        async with get_opengauss_connection() as conn:
            # 测试 fetchval - 获取单个值
            print("\n2.1 测试 fetchval (获取单个值)")
            try:
                result = await conn.fetchval("SELECT 1 as test_value")
                print(f"   [OK] fetchval 成功: {result}")
                assert result == 1, f"期望值 1，实际值 {result}"
            except Exception as e:
                print(f"   [FAIL] fetchval 失败: {e}")
                raise
            
            # 测试 fetchrow - 获取单行
            print("\n2.2 测试 fetchrow (获取单行)")
            try:
                result = await conn.fetchrow("SELECT 1 as col1, 'test' as col2, 3.14 as col3")
                print(f"   [OK] fetchrow 成功: {result}")
                assert result is not None, "fetchrow 返回 None"
                assert result['col1'] == 1, f"期望 col1=1，实际 {result.get('col1')}"
                assert result['col2'] == 'test', f"期望 col2='test'，实际 {result.get('col2')}"
                print(f"   结果: col1={result['col1']}, col2={result['col2']}, col3={result['col3']}")
            except Exception as e:
                print(f"   [FAIL] fetchrow 失败: {e}")
                raise
            
            # 测试 fetch - 获取多行
            print("\n2.3 测试 fetch (获取多行)")
            try:
                result = await conn.fetch("SELECT generate_series(1, 5) as num")
                print(f"   [OK] fetch 成功: 返回 {len(result)} 行")
                assert len(result) == 5, f"期望 5 行，实际 {len(result)} 行"
                for i, row in enumerate(result, 1):
                    assert row['num'] == i, f"第 {i} 行期望值 {i}，实际值 {row['num']}"
                print(f"   结果: {[row['num'] for row in result]}")
            except Exception as e:
                print(f"   [FAIL] fetch 失败: {e}")
                raise
            
            # 测试 execute - 执行 SQL
            print("\n2.4 测试 execute (执行 SQL)")
            try:
                # 创建一个临时表用于测试（openGauss 不支持临时表的 SERIAL，使用 INTEGER）
                await conn.execute("""
                    CREATE TEMP TABLE IF NOT EXISTS test_table (
                        id INTEGER PRIMARY KEY,
                        name VARCHAR(100),
                        value INTEGER
                    )
                """)
                print("   [OK] 创建临时表成功")
                
                # 插入数据（需要手动指定 ID）
                rowcount = await conn.execute(
                    "INSERT INTO test_table (id, name, value) VALUES ($1, $2, $3)",
                    1, "test_name", 42
                )
                print(f"   [OK] 插入数据成功: 影响 {rowcount} 行")
                
                # 查询数据
                result = await conn.fetchrow(
                    "SELECT * FROM test_table WHERE name = $1",
                    "test_name"
                )
                assert result is not None, "查询结果为空"
                assert result['name'] == 'test_name', f"期望 name='test_name'，实际 {result.get('name')}"
                assert result['value'] == 42, f"期望 value=42，实际 {result.get('value')}"
                print(f"   [OK] 查询数据成功: {result}")
            except Exception as e:
                print(f"   [FAIL] execute 失败: {e}")
                raise
        
        print("\n[OK] 基本查询测试通过")
        return True
        
    except Exception as e:
        print(f"\n[FAIL] 基本查询测试失败: {e}")
        import traceback
        traceback.print_exc()
        return False


async def test_batch_operations():
    """测试批量操作"""
    print("\n" + "=" * 80)
    print("测试 3: 批量操作")
    print("=" * 80)
    
    try:
        async with get_opengauss_connection() as conn:
            # 创建临时表（openGauss 不支持临时表的 SERIAL，使用 INTEGER）
            await conn.execute("""
                CREATE TEMP TABLE IF NOT EXISTS test_batch (
                    id INTEGER PRIMARY KEY,
                    name VARCHAR(100),
                    value INTEGER
                )
            """)
            
            # 测试 executemany - 批量插入（需要手动指定 ID）
            print("\n3.1 测试 executemany (批量插入)")
            try:
                data = [
                    (1, "name1", 1),
                    (2, "name2", 2),
                    (3, "name3", 3),
                    (4, "name4", 4),
                    (5, "name5", 5),
                ]
                rowcount = await conn.executemany(
                    "INSERT INTO test_batch (id, name, value) VALUES ($1, $2, $3)",
                    data
                )
                print(f"   [OK] 批量插入成功: 影响 {rowcount} 行")
                
                # 验证数据
                result = await conn.fetch("SELECT COUNT(*) as count FROM test_batch")
                count = result[0]['count']
                assert count == 5, f"期望 5 行，实际 {count} 行"
                print(f"   [OK] 验证成功: 表中共有 {count} 行数据")
            except Exception as e:
                print(f"   [FAIL] executemany 失败: {e}")
                raise
        
        print("\n[OK] 批量操作测试通过")
        return True
        
    except Exception as e:
        print(f"\n[FAIL] 批量操作测试失败: {e}")
        import traceback
        traceback.print_exc()
        return False


async def test_large_query():
    """测试大查询（模拟实际使用场景）"""
    print("\n" + "=" * 80)
    print("测试 4: 大查询（模拟实际使用场景）")
    print("=" * 80)
    
    try:
        async with get_opengauss_connection() as conn:
            # 创建临时表并插入大量数据（openGauss 不支持临时表的 SERIAL，使用 INTEGER）
            print("\n4.1 准备测试数据")
            await conn.execute("""
                CREATE TEMP TABLE IF NOT EXISTS test_large (
                    id INTEGER PRIMARY KEY,
                    file_path VARCHAR(500),
                    file_name VARCHAR(255),
                    file_size BIGINT,
                    created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
                )
            """)
            
            # 插入 1000 行测试数据（需要手动指定 ID，因为不能使用 SERIAL）
            print("   正在插入 1000 行测试数据...")
            data = [
                (i, f"/path/to/file_{i}.txt", f"file_{i}.txt", 1024 * i, None)
                for i in range(1, 1001)
            ]
            await conn.executemany(
                "INSERT INTO test_large (id, file_path, file_name, file_size, created_at) VALUES ($1, $2, $3, $4, $5)",
                data
            )
            print("   [OK] 数据插入完成")
            
            # 测试大查询（模拟 fetch_pending_files_grouped_by_size）
            print("\n4.2 测试大查询（1000 行）")
            try:
                rows = await conn.fetch("""
                    SELECT 
                        id,
                        file_path,
                        file_name,
                        file_size
                    FROM test_large
                    WHERE id > $1
                    ORDER BY id
                    LIMIT $2
                """, 0, 1000)
                
                print(f"   [OK] 大查询成功: 返回 {len(rows)} 行")
                assert len(rows) == 1000, f"期望 1000 行，实际 {len(rows)} 行"
                
                # 验证数据完整性
                for i, row in enumerate(rows, 1):
                    assert row['id'] == i, f"第 {i} 行 ID 不匹配"
                    assert row['file_name'] == f"file_{i}.txt", f"第 {i} 行文件名不匹配"
                
                print("   [OK] 数据完整性验证通过")
                print(f"   第一行: id={rows[0]['id']}, file_name={rows[0]['file_name']}")
                print(f"   最后一行: id={rows[-1]['id']}, file_name={rows[-1]['file_name']}")
            except Exception as e:
                print(f"   [FAIL] 大查询失败: {e}")
                import traceback
                traceback.print_exc()
                raise
        
        print("\n[OK] 大查询测试通过")
        return True
        
    except Exception as e:
        print(f"\n[FAIL] 大查询测试失败: {e}")
        import traceback
        traceback.print_exc()
        return False


async def test_error_handling():
    """测试错误处理"""
    print("\n" + "=" * 80)
    print("测试 5: 错误处理")
    print("=" * 80)
    
    try:
        async with get_opengauss_connection() as conn:
            # 测试 SQL 语法错误
            print("\n5.1 测试 SQL 语法错误处理")
            try:
                await conn.fetch("SELECT * FROM non_existent_table")
                print("   [FAIL] 应该抛出异常，但没有")
                return False
            except Exception as e:
                print(f"   [OK] 正确捕获异常: {type(e).__name__}: {e}")
            
            # 测试参数错误
            print("\n5.2 测试参数错误处理")
            try:
                await conn.fetch("SELECT $1", "arg1", "arg2")  # 参数数量不匹配
                print("   [FAIL] 应该抛出异常，但没有")
                return False
            except Exception as e:
                print(f"   [OK] 正确捕获异常: {type(e).__name__}: {e}")
        
        print("\n[OK] 错误处理测试通过")
        return True
        
    except Exception as e:
        print(f"\n[FAIL] 错误处理测试失败: {e}")
        import traceback
        traceback.print_exc()
        return False


async def test_connection_pool():
    """测试连接池功能"""
    print("\n" + "=" * 80)
    print("测试 6: 连接池功能")
    print("=" * 80)
    
    try:
        # 测试多次获取连接
        print("\n6.1 测试多次获取连接")
        connections = []
        for i in range(5):
            conn_context = get_opengauss_connection()
            conn = await conn_context.__aenter__()
            connections.append((conn_context, conn))
            print(f"   [OK] 获取连接 {i+1}/5")
        
        # 释放所有连接
        print("\n6.2 释放所有连接")
        for i, (conn_context, conn) in enumerate(connections):
            await conn_context.__aexit__(None, None, None)
            print(f"   [OK] 释放连接 {i+1}/5")
        
        # 测试并发查询
        print("\n6.3 测试并发查询")
        async def query_task(task_id):
            async with get_opengauss_connection() as conn:
                result = await conn.fetchval("SELECT $1", task_id)
                return result
        
        tasks = [query_task(i) for i in range(10)]
        results = await asyncio.gather(*tasks)
        print(f"   [OK] 并发查询成功: {len(results)} 个任务完成")
        assert all(r == i for i, r in enumerate(results)), "并发查询结果不匹配"
        print(f"   结果: {results}")
        
        print("\n[OK] 连接池测试通过")
        return True
        
    except Exception as e:
        print(f"\n[FAIL] 连接池测试失败: {e}")
        import traceback
        traceback.print_exc()
        return False


async def main():
    """主测试函数"""
    print("=" * 80)
    print("psycopg3 连接和查询功能测试")
    print("=" * 80)
    
    # 检查是否使用 openGauss
    if not is_opengauss():
        print("\n[WARN] 未检测到 openGauss/PostgreSQL 数据库")
        print("   请确保 DATABASE_URL 配置正确")
        return
    
    results = []
    
    # 运行所有测试
    results.append(("连接测试", await test_connection()))
    results.append(("基本查询", await test_basic_query()))
    results.append(("批量操作", await test_batch_operations()))
    results.append(("大查询", await test_large_query()))
    results.append(("错误处理", await test_error_handling()))
    results.append(("连接池", await test_connection_pool()))
    
    # 关闭连接池
    try:
        await close_opengauss_pool()
        print("\n[OK] 连接池已关闭")
    except Exception as e:
        print(f"\n[WARN] 关闭连接池时出错: {e}")
    
    # 输出测试结果摘要
    print("\n" + "=" * 80)
    print("测试结果摘要")
    print("=" * 80)
    passed = sum(1 for _, result in results if result)
    total = len(results)
    
    for test_name, result in results:
        status = "[PASS]" if result else "[FAIL]"
        print(f"{test_name}: {status}")
    
    print(f"\n总计: {passed}/{total} 测试通过")
    
    if passed == total:
        print("\n[SUCCESS] 所有测试通过！psycopg3 连接和查询功能正常")
        return 0
    else:
        print(f"\n[WARN] {total - passed} 个测试失败，请检查错误信息")
        return 1


if __name__ == "__main__":
    try:
        exit_code = asyncio.run(main())
        sys.exit(exit_code)
    except KeyboardInterrupt:
        print("\n\n测试被用户中断")
        sys.exit(1)
    except Exception as e:
        print(f"\n\n测试过程中发生未预期的错误: {e}")
        import traceback
        traceback.print_exc()
        sys.exit(1)

