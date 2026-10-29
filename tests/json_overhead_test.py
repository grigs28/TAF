#!/usr/bin/env python3
"""
JSON序列化开销分析测试
"""

import json
import time
import datetime
import statistics
from pathlib import Path

def simulate_file_metadata():
    """模拟一个文件的元数据"""
    # 模拟真实的文件信息（从file_scanner.py复制）
    file_path = Path("/path/to/important/document.pdf")

    # 基本文件信息
    basic_info = {
        'path': str(file_path),
        'name': file_path.name,
        'size': 1024 * 1024,  # 1MB
        'modified_time': 1640995200.0,  # 时间戳
        'created_time': 1640995200.0,
        'accessed_time': 1640995200.0,
    }

    # 复杂的metadata（当前设计）
    complex_metadata = {
        'scanned_at': datetime.datetime.now().isoformat(),
        'original_path': str(file_path),
        'permissions': '644',
        'created_time': datetime.datetime.fromtimestamp(basic_info['created_time']).isoformat(),
        'modified_time': datetime.datetime.fromtimestamp(basic_info['modified_time']).isoformat(),
        'accessed_time': datetime.datetime.fromtimestamp(basic_info['accessed_time']).isoformat(),
        'uid': 1000,
        'gid': 1000,
        'device': 2049,
        'inode': 123456,
        'nlink': 1,
        'file_hash': 'd41d8cd98f00b204e9800998ecf8427e',
        'file_attributes': {
            'readonly': False,
            'hidden': False,
            'system': False,
            'archive': True,
            'compressed': False,
            'encrypted': False,
            'temporary': False,
            'offline': False
        },
        'scan_info': {
            'scan_duration_ms': 1.5,
            'scan_method': 'stat',
            'scanner_version': '1.0.0',
            'scan_errors': []
        },
        'security_info': {
            'integrity_verified': True,
            'checksum_algorithm': 'sha256',
            'signature_valid': False
        }
    }

    return basic_info, complex_metadata

def test_json_overhead(file_count: int = 10000):
    """测试JSON序列化开销"""
    print(f"=== JSON序列化开销测试 ({file_count:,} 个文件) ===")

    # 测试简单数据结构（无JSON）
    print("\n1. 简单数据结构（无JSON）:")
    basic_times = []
    for _ in range(file_count):
        basic_info, _ = simulate_file_metadata()

        start_time = time.perf_counter()

        # 模拟简单处理（无JSON）
        processed_info = {
            'path': basic_info['path'],
            'name': basic_info['name'],
            'size': basic_info['size'],
            'modified': basic_info['modified_time']
        }

        end_time = time.perf_counter()
        basic_times.append(end_time - start_time)

    basic_avg = statistics.mean(basic_times) * 1000000  # 转换为微秒
    basic_total = sum(basic_times)

    print(f"   单文件处理时间: {basic_avg:.2f} μs")
    print(f"   总处理时间: {basic_total:.3f} 秒")
    print(f"   处理速度: {file_count/basic_total:,.0f} 文件/秒")

    # 测试JSON序列化
    print("\n2. 复杂数据结构（含JSON序列化）:")
    json_times = []
    json_sizes = []

    for _ in range(file_count):
        _, complex_metadata = simulate_file_metadata()

        start_time = time.perf_counter()

        # JSON序列化
        json_str = json.dumps(complex_metadata, ensure_ascii=False)

        end_time = time.perf_counter()
        json_times.append(end_time - start_time)
        json_sizes.append(len(json_str))

    json_avg = statistics.mean(json_times) * 1000000  # 转换为微秒
    json_total = sum(json_times)
    json_avg_size = statistics.mean(json_sizes)

    print(f"   单文件序列化时间: {json_avg:.2f} μs")
    print(f"   总序列化时间: {json_total:.3f} 秒")
    print(f"   序列化速度: {file_count/json_total:,.0f} 文件/秒")
    print(f"   平均JSON大小: {json_avg_size:,.0f} 字节")

    # 测试JSON反序列化
    print("\n3. JSON反序列化:")
    deserialize_times = []

    for _ in range(file_count):
        _, complex_metadata = simulate_file_metadata()
        json_str = json.dumps(complex_metadata, ensure_ascii=False)

        start_time = time.perf_counter()

        # JSON反序列化
        parsed_data = json.loads(json_str)

        end_time = time.perf_counter()
        deserialize_times.append(end_time - start_time)

    deserialize_avg = statistics.mean(deserialize_times) * 1000000  # 转换为微秒
    deserialize_total = sum(deserialize_times)

    print(f"   单文件反序列化时间: {deserialize_avg:.2f} μs")
    print(f"   总反序列化时间: {deserialize_total:.3f} 秒")
    print(f"   反序列化速度: {file_count/deserialize_total:,.0f} 文件/秒")

    # 对比分析
    print("\n=== 性能对比分析 ===")

    # 时间开销
    json_overhead = json_total - basic_total
    time_increase = (json_overhead / basic_total) * 100

    print(f"时间开销:")
    print(f"   简单处理: {basic_total:.3f} 秒")
    print(f"   JSON处理: {json_total:.3f} 秒")
    print(f"   开销增加: {json_overhead:.3f} 秒 ({time_increase:.1f}%)")

    # 存储开销
    basic_data_size = 100  # 假设基本数据100字节
    storage_increase = ((json_avg_size - basic_data_size) / basic_data_size) * 100

    print(f"\n存储开销:")
    print(f"   基本数据: ~{basic_data_size} 字节/文件")
    print(f"   JSON数据: ~{json_avg_size:,.0f} 字节/文件")
    print(f"   存储增加: {storage_increase:.1f}%")

    # 大规模场景分析
    print(f"\n大规模场景分析 ({file_count:,} 个文件):")

    total_json_size = json_avg_size * file_count
    total_json_storage_mb = total_json_size / 1024 / 1024
    total_json_storage_gb = total_json_storage_mb / 1024

    print(f"   JSON总存储: {total_json_storage_mb:,.0f} MB ({total_json_storage_gb:.2f} GB)")
    print(f"   JSON序列化总时间: {json_total:.1f} 秒")
    print(f"   如果无JSON，可以节省: {json_overhead:.1f} 秒")

def test_alternative_design():
    """测试替代设计方案"""
    print(f"\n=== 替代设计方案测试 ===")

    file_count = 10000

    # 方案1：优化JSON（只保留必要字段）
    print("\n方案1: 优化JSON（只保留必要字段）:")

    optimized_times = []
    optimized_sizes = []

    for _ in range(file_count):
        _, complex_metadata = simulate_file_metadata()

        start_time = time.perf_counter()

        # 只保留必要字段
        optimized_metadata = {
            'scan_time': complex_metadata['scanned_at'],
            'permissions': complex_metadata['permissions'],
            'hash': complex_metadata['file_hash']
        }
        json_str = json.dumps(optimized_metadata, separators=(',', ':'))

        end_time = time.perf_counter()
        optimized_times.append(end_time - start_time)
        optimized_sizes.append(len(json_str))

    optimized_total = sum(optimized_times)
    optimized_avg_size = statistics.mean(optimized_sizes)

    print(f"   优化后序列化时间: {optimized_total:.3f} 秒")
    print(f"   优化后JSON大小: {optimized_avg_size:,.0f} 字节")

    # 方案2：拆分为独立字段
    print("\n方案2: 拆分为独立数据库字段:")

    field_times = []

    for _ in range(file_count):
        _, complex_metadata = simulate_file_metadata()

        start_time = time.perf_counter()

        # 模拟直接字段赋值（无JSON）
        scan_time = complex_metadata['scanned_at']
        permissions = complex_metadata['permissions']
        file_hash = complex_metadata['file_hash']

        # 模拟数据库绑定参数
        db_params = (scan_time, permissions, file_hash)

        end_time = time.perf_counter()
        field_times.append(end_time - start_time)

    field_total = sum(field_times)

    print(f"   字段处理时间: {field_total:.3f} 秒")
    print(f"   存储开销: 最小（直接字段存储）")

    # 方案3：批量JSON
    print("\n方案3: 批量JSON处理:")

    batch_size = 100
    batch_count = file_count // batch_size

    batch_times = []

    for _ in range(batch_count):
        batch_metadata = []

        # 收集批次数据
        for _ in range(batch_size):
            _, complex_metadata = simulate_file_metadata()
            batch_metadata.append({
                'scan_time': complex_metadata['scanned_at'],
                'permissions': complex_metadata['permissions'],
                'hash': complex_metadata['file_hash']
            })

        start_time = time.perf_counter()

        # 批量JSON序列化
        json_str = json.dumps(batch_metadata, separators=(',', ':'))

        end_time = time.perf_counter()
        batch_times.append(end_time - start_time)

    batch_total = sum(batch_times)

    print(f"   批量处理时间: {batch_total:.3f} 秒")
    print(f"   平均每文件: {batch_total/file_count*1000000:.2f} μs")

def main():
    """主函数"""
    print("JSON序列化性能开销分析")
    print("=" * 60)

    # 测试不同规模的性能开销
    for file_count in [1000, 10000, 100000]:
        test_json_overhead(file_count)
        print("\n" + "=" * 80)

    # 测试替代方案
    test_alternative_design()

if __name__ == "__main__":
    main()