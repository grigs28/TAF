#!/usr/bin/env python3
"""
批量数据库操作性能测试脚本
用于对比优化前后的性能差异
"""

import asyncio
import time
import logging
import sys
from pathlib import Path

# 添加项目根目录到路径
sys.path.insert(0, str(Path(__file__).parent.parent))

from backup.backup_db import BatchDBWriter, BackupDB
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
                'test_data': f"test_file_{i}"
            }
        })
    return files

async def test_original_method(files):
    """测试原始的单条插入方法"""
    print(f"\n=== 测试原始单条插入方法 ({len(files)} 个文件) ===")

    # 创建临时备份集用于测试
    async with get_opengauss_connection() as conn:
        backup_set_result = await conn.fetchrow(
            """
            INSERT INTO backup_sets (set_id, set_name, backup_group, status, created_at, updated_at)
            VALUES ($1, $2, $3, $4, NOW(), NOW())
            RETURNING id
            """,
            f"test_original_{int(time.time())}", "test_group", "scanning"
        )
        backup_set_id = backup_set_result['id']

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

    # 清理测试数据
    async with get_opengauss_connection() as conn:
        await conn.execute("DELETE FROM backup_files WHERE backup_set_id = $1", backup_set_id)
        await conn.execute("DELETE FROM backup_sets WHERE id = $1", backup_set_id)

    return elapsed, rate

async def test_batch_method(files, batch_size=1000, max_queue_size=5000):
    """测试批量插入方法"""
    print(f"\n=== 测试批量插入方法 ({len(files)} 个文件, batch_size={batch_size}) ===")

    # 创建临时备份集用于测试
    async with get_opengauss_connection() as conn:
        backup_set_result = await conn.fetchrow(
            """
            INSERT INTO backup_sets (set_id, set_name, backup_group, status, created_at, updated_at)
            VALUES ($1, $2, $3, $4, NOW(), NOW())
            RETURNING id
            """,
            f"test_batch_{int(time.time())}", "test_group", "scanning"
        )
        backup_set_id = backup_set_result['id']

    start_time = time.time()

    try:
        # 创建批量写入器
        batch_writer = BatchDBWriter(
            backup_set_db_id=backup_set_id,
            batch_size=batch_size,
            max_queue_size=max_queue_size
        )

        await batch_writer.start()

        # 添加文件到批量写入器
        for i, file_info in enumerate(files):
            await batch_writer.add_file(file_info)

            if (i + 1) % 100 == 0:
                elapsed = time.time() - start_time
                rate = (i + 1) / elapsed
                print(f"已添加 {i + 1} 个文件，速度: {rate:.1f} 文件/秒")

        # 等待所有文件写入完成
        await batch_writer.stop()

        # 获取统计信息
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

    # 清理测试数据
    async with get_opengauss_connection() as conn:
        await conn.execute("DELETE FROM backup_files WHERE backup_set_id = $1", backup_set_id)
        await conn.execute("DELETE FROM backup_sets WHERE id = $1", backup_set_id)

    return elapsed, rate

async def main():
    """主函数"""
    print("批量数据库操作性能测试")
    print("=" * 60)

    # 检查数据库连接
    try:
        async with get_opengauss_connection() as conn:
            await conn.fetchval("SELECT 1")
        print("数据库连接正常")
    except Exception as e:
        print(f"数据库连接失败: {e}")
        return

    # 测试不同规模的文件数量
    test_sizes = [500, 1000, 2000]
    batch_sizes = [500, 1000, 2000]

    for file_count in test_sizes:
        print(f"\n{'='*60}")
        print(f"测试规模: {file_count} 个文件")
        print(f"{'='*60}")

        # 生成测试数据
        test_files = generate_test_files(file_count)
        print(f"已生成 {len(test_files)} 个测试文件数据")

        # 测试原始方法
        original_time, original_rate = await test_original_method(test_files)

        # 测试不同批次大小的批量方法
        best_batch_time = None
        best_batch_rate = None
        best_batch_size = None

        for batch_size in batch_sizes:
            batch_time, batch_rate = await test_batch_method(test_files, batch_size)

            if batch_time and (best_batch_time is None or batch_time < best_batch_time):
                best_batch_time = batch_time
                best_batch_rate = batch_rate
                best_batch_size = batch_size

        # 输出对比结果
        if original_time and best_batch_time:
            print(f"\n--- 性能对比结果 ({file_count} 个文件) ---")
            print(f"原始方法:   {original_time:.2f}s ({original_rate:.1f} 文件/秒)")
            print(f"最佳批量方法: {best_batch_time:.2f}s ({best_batch_rate:.1f} 文件/秒, batch_size={best_batch_size})")

            improvement = (original_time - best_batch_time) / original_time * 100
            speedup = best_batch_rate / original_rate

            print(f"性能提升: {improvement:.1f}% (加速 {speedup:.1f}x)")

if __name__ == "__main__":
    asyncio.run(main())