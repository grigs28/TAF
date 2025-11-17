#!/usr/bin/env python3
"""
内存数据库性能测试脚本
对比内存数据库 vs 批量写入 vs 单条写入的性能差异
"""

import asyncio
import time
import logging
import sys
import tempfile
import json
from pathlib import Path

# 添加项目根目录到路径
sys.path.insert(0, str(Path(__file__).parent.parent))

from backup.backup_db import BackupDB, BatchDBWriter
from backup.memory_db_writer import MemoryDBWriter
from utils.scheduler.db_utils import get_opengauss_connection, is_opengauss

# 配置日志
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(name)s - %(levelname)s - %(message)s'
)
logger = logging.getLogger(__name__)

def generate_test_files(count: int = 1000):
    """生成测试文件数据"""
    files = []
    for i in range(count):
        file_path = f"/test/directory/subdir/test_file_{i:04d}.txt"
        files.append({
            'path': file_path,
            'name': f"test_file_{i:04d}.txt",
            'size': 1024 + (i % 1000),  # 1KB-2KB 变化大小
            'file_stat': type('FileStat', (), {
                'st_size': 1024 + (i % 1000),
                'st_mode': 0o644,
                'st_ctime': 1640995200.0 + i,
                'st_mtime': 1640995200.0 + i,
                'st_atime': 1640995200.0 + i,
            })(),
            'file_metadata': {
                'scanned_at': '2024-01-01T12:00:00',
                'test_id': i,
                'test_data': f"test_file_{i}",
                'large_data': 'x' * 100  # 模拟更大的JSON数据
            }
        })
    return files

async def test_original_method(files, backup_set_id):
    """测试原始的单条插入方法"""
    print(f"\n=== 测试原始单条插入方法 ({len(files)} 个文件) ===")

    backup_db = BackupDB()
    start_time = time.time()

    try:
        for i, file_info in enumerate(files):
            await backup_db.upsert_scanned_file_record(backup_set_id, file_info)

            if (i + 1) % 100 == 0:
                elapsed = time.time() - start_time
                rate = (i + 1) / elapsed
                print(f"已处理 {i + 1} 个文件，速度: {rate:.1f} 文件/秒")

    except Exception as e:
        print(f"原始方法测试失败: {e}")
        return None, None

    end_time = time.time()
    elapsed = end_time - start_time
    rate = len(files) / elapsed

    print(f"原始方法完成:")
    print(f"  耗时: {elapsed:.2f} 秒")
    print(f"  速度: {rate:.1f} 文件/秒")

    return elapsed, rate

async def test_batch_method(files, backup_set_id, batch_size=1000):
    """测试批量插入方法"""
    print(f"\n=== 测试批量插入方法 ({len(files)} 个文件, batch_size={batch_size}) ===")

    start_time = time.time()

    try:
        batch_writer = BatchDBWriter(
            backup_set_db_id=backup_set_id,
            batch_size=batch_size
        )

        await batch_writer.start()

        for i, file_info in enumerate(files):
            await batch_writer.add_file(file_info)

            if (i + 1) % 100 == 0:
                elapsed = time.time() - start_time
                rate = (i + 1) / elapsed
                print(f"已添加 {i + 1} 个文件，速度: {rate:.1f} 文件/秒")

        await batch_writer.stop()
        stats = batch_writer.get_stats()

    except Exception as e:
        print(f"批量方法测试失败: {e}")
        return None, None

    end_time = time.time()
    elapsed = end_time - start_time
    rate = len(files) / elapsed

    print(f"批量方法完成:")
    print(f"  耗时: {elapsed:.2f} 秒")
    print(f"  速度: {rate:.1f} 文件/秒")
    print(f"  批次统计: {stats}")

    return elapsed, rate

async def test_memory_db_method(files, backup_set_id, sync_batch_size=5000, sync_interval=10):
    """测试内存数据库方法"""
    print(f"\n=== 测试内存数据库方法 ({len(files)} 个文件, sync_batch={sync_batch_size}, interval={sync_interval}s) ===")

    start_time = time.time()

    try:
        memory_writer = MemoryDBWriter(
            backup_set_db_id=backup_set_id,
            sync_batch_size=sync_batch_size,
            sync_interval=sync_interval,
            max_memory_files=200000  # 增大内存限制
        )

        await memory_writer.initialize()

        # 添加文件到内存数据库（极速）
        for i, file_info in enumerate(files):
            await memory_writer.add_file(file_info)

            if (i + 1) % 500 == 0:
                elapsed = time.time() - start_time
                rate = (i + 1) / elapsed
                print(f"已添加 {i + 1} 个文件到内存数据库，速度: {rate:.1f} 文件/秒")

        print("所有文件已添加到内存数据库，等待同步完成...")

        # 等待同步完成
        max_wait_time = 120  # 最多等待2分钟
        wait_start = time.time()

        while time.time() - wait_start < max_wait_time:
            sync_status = await memory_writer.get_sync_status()
            progress = sync_status['sync_progress']

            if progress >= 99.9:
                break

            print(f"同步进度: {progress:.1f}% (总计: {sync_status['total_files']}, "
                  f"已同步: {sync_status['synced_files']}, 待同步: {sync_status['pending_files']})")
            await asyncio.sleep(2)

        # 强制完成剩余同步
        await memory_writer.force_sync()

        stats = memory_writer.get_stats()
        final_sync_status = await memory_writer.get_sync_status()

        await memory_writer.stop()

    except Exception as e:
        print(f"内存数据库方法测试失败: {e}")
        import traceback
        traceback.print_exc()
        return None, None

    end_time = time.time()
    elapsed = end_time - start_time
    rate = len(files) / elapsed

    print(f"内存数据库方法完成:")
    print(f"  总耗时: {elapsed:.2f} 秒")
    print(f"  平均速度: {rate:.1f} 文件/秒")
    print(f"  统计信息: {stats}")
    print(f"  最终同步状态: {final_sync_status}")

    return elapsed, rate

async def create_test_backup_set(name_suffix):
    """创建测试备份集"""
    async with get_opengauss_connection() as conn:
        result = await conn.fetchrow(
            """
            INSERT INTO backup_sets (set_id, set_name, backup_group, status, created_at, updated_at)
            VALUES ($1, $2, $3, $4, NOW(), NOW())
            RETURNING id
            """,
            f"test_{name_suffix}_{int(time.time())}", "test_group", "scanning"
        )
        return result['id']

async def cleanup_test_data(backup_set_id):
    """清理测试数据"""
    try:
        async with get_opengauss_connection() as conn:
            await conn.execute("DELETE FROM backup_files WHERE backup_set_id = $1", backup_set_id)
            await conn.execute("DELETE FROM backup_sets WHERE id = $1", backup_set_id)
        print(f"已清理测试数据 (backup_set_id={backup_set_id})")
    except Exception as e:
        print(f"清理测试数据失败: {e}")

async def run_performance_comparison():
    """运行完整的性能对比测试"""
    print("内存数据库性能对比测试")
    print("=" * 80)

    # 检查数据库连接
    try:
        async with get_opengauss_connection() as conn:
            await conn.fetchval("SELECT 1")
        print("✓ 数据库连接正常")
    except Exception as e:
        print(f"✗ 数据库连接失败: {e}")
        return

    # 测试不同规模的文件数量
    test_scenarios = [
        {"name": "小规模", "file_count": 1000, "batch_size": 500, "sync_batch": 1000},
        {"name": "中规模", "file_count": 5000, "batch_size": 1000, "sync_batch": 2500},
        {"name": "大规模", "file_count": 10000, "batch_size": 2000, "sync_batch": 5000},
    ]

    for scenario in test_scenarios:
        file_count = scenario["file_count"]
        batch_size = scenario["batch_size"]
        sync_batch = scenario["sync_batch"]

        print(f"\n{'='*80}")
        print(f"测试场景: {scenario['name']} - {file_count} 个文件")
        print(f"{'='*80}")

        # 生成测试数据
        test_files = generate_test_files(file_count)
        print(f"✓ 已生成 {len(test_files)} 个测试文件数据")

        # 测试三种方法
        results = {}

        # 1. 测试原始方法（仅小规模）
        if file_count <= 2000:
            backup_set_id = await create_test_backup_set("original")
            try:
                original_time, original_rate = await test_original_method(test_files, backup_set_id)
                results["original"] = {"time": original_time, "rate": original_rate}
            finally:
                await cleanup_test_data(backup_set_id)

        # 2. 测试批量方法
        backup_set_id = await create_test_backup_set("batch")
        try:
            batch_time, batch_rate = await test_batch_method(test_files, backup_set_id, batch_size)
            results["batch"] = {"time": batch_time, "rate": batch_rate}
        finally:
            await cleanup_test_data(backup_set_id)

        # 3. 测试内存数据库方法
        backup_set_id = await create_test_backup_set("memory")
        try:
            memory_time, memory_rate = await test_memory_db_method(
                test_files, backup_set_id, sync_batch, sync_interval=15
            )
            results["memory"] = {"time": memory_time, "rate": memory_rate}
        finally:
            await cleanup_test_data(backup_set_id)

        # 输出对比结果
        print(f"\n--- {scenario['name']} 性能对比结果 ---")
        print(f"{'方法':<15} {'耗时(秒)':<10} {'速度(文件/秒)':<15} {'相对性能':<10}")
        print("-" * 60)

        baseline_time = None
        for method, data in results.items():
            time_val = data["time"]
            rate_val = data["rate"]

            if baseline_time is None:
                baseline_time = time_val
                relative_perf = "1.00x"
            else:
                relative_perf = f"{baseline_time/time_val:.2f}x"

            method_name = {
                "original": "原始单条",
                "batch": "批量写入",
                "memory": "内存数据库"
            }.get(method, method)

            print(f"{method_name:<15} {time_val:<10.2f} {rate_val:<15.1f} {relative_perf:<10}")

        # 计算性能提升
        if "batch" in results and "memory" in results:
            batch_vs_memory = results["batch"]["time"] / results["memory"]["time"]
            print(f"\n内存数据库相对批量写入性能提升: {batch_vs_memory:.2f}x")

        if "original" in results and "memory" in results:
            original_vs_memory = results["original"]["time"] / results["memory"]["time"]
            print(f"内存数据库相对原始方法性能提升: {original_vs_memory:.2f}x")

async def main():
    """主函数"""
    try:
        await run_performance_comparison()
    except KeyboardInterrupt:
        print("\n测试被用户中断")
    except Exception as e:
        print(f"\n测试过程中发生错误: {e}")
        import traceback
        traceback.print_exc()

if __name__ == "__main__":
    asyncio.run(main())