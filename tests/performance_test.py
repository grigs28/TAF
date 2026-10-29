#!/usr/bin/env python3
"""
性能对比测试：纯扫描 vs 扫描+异步写入
"""

import asyncio
import time
import json
import os
import psutil
from pathlib import Path
from typing import List, Dict
import random
import string

# 模拟数据库操作
async def simulate_db_write(file_info: Dict):
    """模拟数据库写入操作（包含JSON序列化）"""
    # 模拟JSON序列化开销
    metadata = {
        'scanned_at': time.time(),
        'permissions': '755',
        'created_time': file_info.get('created_time'),
        'modified_time': file_info.get('modified_time'),
        'accessed_time': file_info.get('accessed_time'),
        'file_hash': ''.join(random.choices(string.hexdigits.lower(), k=32)),
        'original_path': file_info.get('path'),
        'size_on_disk': file_info.get('size'),
        'file_attributes': {
            'readonly': False,
            'hidden': file_info.get('name', '').startswith('.'),
            'system': False,
            'archive': True
        }
    }

    # JSON序列化 - 这是CPU密集型操作
    json_str = json.dumps(metadata)

    # 模拟网络延迟和数据库处理时间
    await asyncio.sleep(0.001)  # 1ms的数据库操作延迟

    return len(json_str)  # 返回JSON大小

class PerformanceTest:
    def __init__(self, test_dir: str = "test_files"):
        self.test_dir = Path(test_dir)
        self.test_dir.mkdir(exist_ok=True)

        # 创建测试文件
        self.create_test_files(1000)

    def create_test_files(self, count: int):
        """创建测试文件"""
        print(f"创建 {count} 个测试文件...")
        for i in range(count):
            file_path = self.test_dir / f"test_file_{i:04d}.txt"
            with open(file_path, 'w') as f:
                f.write(f"Test file {i}\n" * 100)  # 每个文件约2KB
        print("测试文件创建完成")

    async def test_pure_scan(self, file_count: int = 1000):
        """纯扫描测试"""
        print(f"\n=== 纯扫描测试 ({file_count} 个文件) ===")

        start_time = time.time()
        start_memory = psutil.Process().memory_info().rss

        scanned_files = []

        for i in range(file_count):
            file_path = self.test_dir / f"test_file_{i:04d:04d}.txt"

            # 模拟文件扫描操作
            stat = file_path.stat()
            file_info = {
                'path': str(file_path),
                'name': file_path.name,
                'size': stat.st_size,
                'modified_time': stat.st_mtime,
                'created_time': stat.st_ctime,
                'accessed_time': stat.st_atime,
            }

            scanned_files.append(file_info)

            # 每100个文件输出进度
            if (i + 1) % 100 == 0:
                elapsed = time.time() - start_time
                rate = (i + 1) / elapsed
                print(f"已扫描 {i + 1} 个文件，速度: {rate:.1f} 文件/秒")

        end_time = time.time()
        end_memory = psutil.Process().memory_info().rss

        elapsed = end_time - start_time
        rate = file_count / elapsed
        memory_used = (end_memory - start_memory) / 1024 / 1024  # MB

        print(f"纯扫描完成:")
        print(f"  耗时: {elapsed:.2f} 秒")
        print(f"  速度: {rate:.1f} 文件/秒")
        print(f"  内存使用: {memory_used:.2f} MB")

        return elapsed, rate, memory_used

    async def test_scan_with_write(self, file_count: int = 1000):
        """扫描+异步写入测试"""
        print(f"\n=== 扫描+异步写入测试 ({file_count} 个文件) ===")

        start_time = time.time()
        start_memory = psutil.Process().memory_info().rss

        # 创建无限制队列
        file_queue = asyncio.Queue(maxsize=0)
        scanned_count = 0
        written_count = 0

        # 异步写入任务
        async def db_writer():
            nonlocal written_count
            while True:
                file_info = await file_queue.get()
                if file_info is None:  # 结束信号
                    break

                # 模拟数据库写入（包含JSON序列化）
                json_size = await simulate_db_write(file_info)
                written_count += 1

                file_queue.task_done()

                # 每100个写入输出进度
                if written_count % 100 == 0:
                    elapsed = time.time() - start_time
                    write_rate = written_count / elapsed
                    print(f"已写入 {written_count} 个文件，写入速度: {write_rate:.1f} 文件/秒")

        # 启动写入任务
        write_task = asyncio.create_task(db_writer())

        # 扫描任务
        for i in range(file_count):
            file_path = self.test_dir / f"test_file_{i:04d:04d}.txt"

            # 模拟文件扫描
            stat = file_path.stat()
            file_info = {
                'path': str(file_path),
                'name': file_path.name,
                'size': stat.st_size,
                'modified_time': stat.st_mtime,
                'created_time': stat.st_ctime,
                'accessed_time': stat.st_atime,
            }

            # 放入队列（非阻塞）
            await file_queue.put(file_info)
            scanned_count += 1

            # 每100个扫描输出进度
            if (scanned_count % 100) == 0:
                elapsed = time.time() - start_time
                scan_rate = scanned_count / elapsed
                queue_size = file_queue.qsize()
                print(f"已扫描 {scanned_count} 个文件，扫描速度: {scan_rate:.1f} 文件/秒，队列积压: {queue_size}")

        # 等待所有文件写入完成
        await file_queue.join()
        await file_queue.put(None)  # 发送结束信号
        await write_task

        end_time = time.time()
        end_memory = psutil.Process().memory_info().rss

        elapsed = end_time - start_time
        scan_rate = scanned_count / elapsed
        write_rate = written_count / elapsed
        memory_used = (end_memory - start_memory) / 1024 / 1024  # MB

        print(f"扫描+写入完成:")
        print(f"  总耗时: {elapsed:.2f} 秒")
        print(f"  扫描速度: {scan_rate:.1f} 文件/秒")
        print(f"  写入速度: {write_rate:.1f} 文件/秒")
        print(f"  内存使用: {memory_used:.2f} MB")

        return elapsed, scan_rate, write_rate, memory_used

    async def run_comparison(self, file_count: int = 1000):
        """运行对比测试"""
        print("=" * 60)
        print(f"性能对比测试 - {file_count} 个文件")
        print("=" * 60)

        # 纯扫描测试
        pure_scan_time, pure_scan_rate, pure_scan_memory = await self.test_pure_scan(file_count)

        # 扫描+写入测试
        scan_write_time, scan_scan_rate, write_rate, scan_write_memory = await self.test_scan_with_write(file_count)

        # 对比结果
        print("\n" + "=" * 60)
        print("性能对比结果")
        print("=" * 60)

        time_diff = scan_write_time - pure_scan_time
        rate_diff = pure_scan_rate - scan_scan_rate
        memory_diff = scan_write_memory - pure_scan_memory

        print(f"纯扫描:")
        print(f"  耗时: {pure_scan_time:.2f} 秒")
        print(f"  速度: {pure_scan_rate:.1f} 文件/秒")
        print(f"  内存: {pure_scan_memory:.2f} MB")

        print(f"\n扫描+写入:")
        print(f"  耗时: {scan_write_time:.2f} 秒")
        print(f"  扫描速度: {scan_scan_rate:.1f} 文件/秒")
        print(f"  写入速度: {write_rate:.1f} 文件/秒")
        print(f"  内存: {scan_write_memory:.2f} MB")

        print(f"\n性能影响:")
        print(f"  时间增加: {time_diff:.2f} 秒 ({time_diff/pure_scan_time*100:.1f}%)")
        print(f"  扫描速度下降: {rate_diff:.1f} 文件/秒 ({rate_diff/pure_scan_rate*100:.1f}%)")
        print(f"  内存增加: {memory_diff:.2f} MB")

async def main():
    test = PerformanceTest()

    # 运行不同规模的测试
    for file_count in [1000, 5000, 10000]:
        await test.run_comparison(file_count)
        print("\n" + "=" * 80 + "\n")

    # 清理测试文件
    import shutil
    shutil.rmtree(test.test_dir)
    print("测试文件已清理")

if __name__ == "__main__":
    asyncio.run(main())