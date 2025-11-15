#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
py7zr 压缩库测试
测试 py7zr.SevenZipFile 的各种参数和多线程/多进程实现方法
"""

import os
import sys
import time
import tempfile
import shutil
import subprocess
from pathlib import Path
from typing import List, Dict
import threading

# 添加项目根目录到路径
project_root = Path(__file__).parent.parent
sys.path.insert(0, str(project_root))

try:
    import py7zr
    PY7ZR_AVAILABLE = True
except ImportError:
    PY7ZR_AVAILABLE = False
    print("警告: py7zr 未安装，无法运行测试")
    print("请运行: pip install py7zr")

import psutil


class Py7zrCompressionTester:
    """py7zr 压缩测试类"""
    
    def __init__(self):
        # 修改：使用程序所在目录下的 temp 文件夹
        current_dir = Path(__file__).parent
        self.test_dir = current_dir / "temp"
        
        # 确保 temp 目录存在
        self.test_dir.mkdir(exist_ok=True)
        
        # 清理之前的测试文件（可选，可以根据需要注释掉）
        self._clean_previous_tests()
        
        self.results = []
        print(f"测试目录: {self.test_dir}")
    
    def _clean_previous_tests(self):
        """清理之前的测试文件"""
        try:
            # 只删除 .7z 文件和测试数据目录，保留目录结构
            for item in self.test_dir.glob("*.7z"):
                item.unlink()
                print(f"已删除旧测试文件: {item}")
            
            test_data_dir = self.test_dir / "test_data"
            if test_data_dir.exists():
                shutil.rmtree(test_data_dir)
                print(f"已删除旧测试数据目录: {test_data_dir}")
                
            # 清理多线程和多进程的临时存档目录
            for dir_name in ["multithread_archives", "multiprocess_archives"]:
                archive_dir = self.test_dir / dir_name
                if archive_dir.exists():
                    shutil.rmtree(archive_dir)
                    print(f"已删除旧存档目录: {archive_dir}")
                    
        except Exception as e:
            print(f"清理旧测试文件时出错: {e}")
    
    def create_test_files(self, count: int = 10, size_mb: float = 1.0) -> List[Path]:
        """创建测试文件"""
        files = []
        test_data_dir = self.test_dir / "test_data"
        test_data_dir.mkdir(parents=True, exist_ok=True)
        
        print(f"创建 {count} 个测试文件，每个约 {size_mb} MB...")
        
        # 生成测试数据（随机字节）
        chunk_size = 1024 * 1024  # 1MB
        chunk_data = os.urandom(chunk_size)
        
        for i in range(count):
            file_path = test_data_dir / f"test_file_{i:03d}.bin"
            size_bytes = int(size_mb * 1024 * 1024)
            
            with open(file_path, 'wb') as f:
                written = 0
                while written < size_bytes:
                    write_size = min(chunk_size, size_bytes - written)
                    f.write(chunk_data[:write_size])
                    written += write_size
            
            files.append(file_path)
            if (i + 1) % 5 == 0:
                print(f"  已创建 {i + 1}/{count} 个文件")
        
        print(f"[OK] 测试文件创建完成，总大小: {sum(f.stat().st_size for f in files) / 1024 / 1024:.2f} MB")
        return files
    
    def test_basic_compression(self, files: List[Path], compression_level: int = 5):
        """测试基本压缩功能"""
        print(f"\n{'='*60}")
        print(f"测试 1: 基本压缩 (压缩级别: {compression_level})")
        print(f"{'='*60}")
        
        archive_path = self.test_dir / f"test_basic_{compression_level}.7z"
        
        start_time = time.time()
        
        try:
            with py7zr.SevenZipFile(
                archive_path,
                mode='w',
                filters=[{'id': py7zr.FILTER_LZMA2, 'preset': compression_level}]
            ) as archive:
                for file_path in files:
                    archive.write(file_path, file_path.name)
            
            elapsed = time.time() - start_time
            archive_size = archive_path.stat().st_size
            original_size = sum(f.stat().st_size for f in files)
            compression_ratio = (1 - archive_size / original_size) * 100
            
            result = {
                'test': 'basic_compression',
                'test_number': '测试1',
                'compression_level': compression_level,
                'elapsed_time': elapsed,
                'original_size': original_size,
                'archive_size': archive_size,
                'compression_ratio': compression_ratio,
                'files_count': len(files),
                'mp': False
            }
            
            print(f"[OK] 压缩完成")
            print(f"  耗时: {elapsed:.2f} 秒")
            print(f"  原始大小: {original_size / 1024 / 1024:.2f} MB")
            print(f"  压缩后大小: {archive_size / 1024 / 1024:.2f} MB")
            print(f"  压缩率: {compression_ratio:.2f}%")
            print(f"  文件数: {len(files)}")
            
            self.results.append(result)
            return result
            
        except Exception as e:
            print(f"[ERROR] 测试失败: {e}")
            import traceback
            traceback.print_exc()
            return None
    
    def test_mp_parameter(self, files: List[Path], mp: bool = True, compression_level: int = 5):
        """测试 mp 参数（多进程）"""
        print(f"\n{'='*60}")
        print(f"测试 2: 多进程压缩 (mp={mp}, 压缩级别: {compression_level})")
        print(f"{'='*60}")
        
        archive_path = self.test_dir / f"test_mp_{mp}_{compression_level}.7z"
        
        # 检查 mp 参数是否支持
        try:
            # 尝试检查 SevenZipFile 的签名
            import inspect
            sig = inspect.signature(py7zr.SevenZipFile.__init__)
            has_mp = 'mp' in sig.parameters
            print(f"  SevenZipFile 是否支持 mp 参数: {has_mp}")
            if has_mp:
                print(f"  mp 参数类型: {sig.parameters['mp'].annotation}")
                print(f"  mp 参数默认值: {sig.parameters['mp'].default}")
        except Exception as e:
            print(f"  无法检查参数签名: {e}")
        
        start_time = time.time()
        
        try:
            kwargs = {
                'mode': 'w',
                'filters': [{'id': py7zr.FILTER_LZMA2, 'preset': compression_level}]
            }
            
            # 尝试添加 mp 参数
            try:
                kwargs['mp'] = mp
                print(f"  使用参数: {kwargs}")
            except TypeError as e:
                print(f"  警告: mp 参数可能不被支持: {e}")
                if 'mp' in kwargs:
                    del kwargs['mp']
            
            with py7zr.SevenZipFile(archive_path, **kwargs) as archive:
                for file_path in files:
                    archive.write(file_path, file_path.name)
            
            elapsed = time.time() - start_time
            archive_size = archive_path.stat().st_size
            original_size = sum(f.stat().st_size for f in files)
            compression_ratio = (1 - archive_size / original_size) * 100
            
            # 监控 CPU 使用情况
            cpu_usage = self._monitor_cpu_during_compression(start_time, elapsed)
            
            result = {
                'test': 'mp_compression',
                'test_number': f'测试2 (mp={mp})',
                'mp': mp,
                'compression_level': compression_level,
                'elapsed_time': elapsed,
                'original_size': original_size,
                'archive_size': archive_size,
                'compression_ratio': compression_ratio,
                'files_count': len(files),
                'avg_cpu_percent': cpu_usage.get('avg', 0),
                'max_cpu_percent': cpu_usage.get('max', 0)
            }
            
            print(f"[OK] 压缩完成")
            print(f"  耗时: {elapsed:.2f} 秒")
            print(f"  原始大小: {original_size / 1024 / 1024:.2f} MB")
            print(f"  压缩后大小: {archive_size / 1024 / 1024:.2f} MB")
            print(f"  压缩率: {compression_ratio:.2f}%")
            print(f"  平均 CPU 使用率: {cpu_usage.get('avg', 0):.1f}%")
            print(f"  最大 CPU 使用率: {cpu_usage.get('max', 0):.1f}%")
            
            self.results.append(result)
            return result
            
        except Exception as e:
            print(f"[ERROR] 测试失败: {e}")
            import traceback
            traceback.print_exc()
            return None
    
    def test_multithreading_approach(self, files: List[Path], thread_count: int = 4, compression_level: int = 5):
        """测试多线程方法（多个文件并行压缩）"""
        print(f"\n{'='*60}")
        print(f"测试 3: 多线程压缩 (线程数: {thread_count}, 压缩级别: {compression_level})")
        print(f"{'='*60}")
        
        from concurrent.futures import ThreadPoolExecutor, as_completed
        
        archives_dir = self.test_dir / "multithread_archives"
        archives_dir.mkdir(exist_ok=True)
        
        def compress_file(file_path: Path, index: int):
            """压缩单个文件"""
            archive_path = archives_dir / f"archive_{index:03d}.7z"
            start_time = time.time()
            
            try:
                with py7zr.SevenZipFile(
                    archive_path,
                    mode='w',
                    filters=[{'id': py7zr.FILTER_LZMA2, 'preset': compression_level}]
                ) as archive:
                    archive.write(file_path, file_path.name)
                
                elapsed = time.time() - start_time
                archive_size = archive_path.stat().st_size
                original_size = file_path.stat().st_size
                
                return {
                    'file': file_path.name,
                    'elapsed': elapsed,
                    'original_size': original_size,
                    'archive_size': archive_size,
                    'success': True
                }
            except Exception as e:
                return {
                    'file': file_path.name,
                    'elapsed': time.time() - start_time,
                    'error': str(e),
                    'success': False
                }
        
        start_time = time.time()
        results = []
        
        with ThreadPoolExecutor(max_workers=thread_count) as executor:
            futures = {
                executor.submit(compress_file, file_path, i): file_path
                for i, file_path in enumerate(files)
            }
            
            for future in as_completed(futures):
                result = future.result()
                results.append(result)
                if result['success']:
                    print(f"  [OK] {result['file']}: {result['elapsed']:.2f}s")
                else:
                    print(f"  [ERROR] {result['file']}: {result.get('error', 'Unknown error')}")
        
        elapsed = time.time() - start_time
        
        successful = [r for r in results if r.get('success')]
        total_original = sum(r['original_size'] for r in successful)
        total_archive = sum(r['archive_size'] for r in successful)
        compression_ratio = (1 - total_archive / total_original) * 100 if total_original > 0 else 0
        
        result = {
            'test': 'multithreading',
            'test_number': '测试3',
            'thread_count': thread_count,
            'compression_level': compression_level,
            'elapsed_time': elapsed,
            'original_size': total_original,
            'archive_size': total_archive,
            'compression_ratio': compression_ratio,
            'files_count': len(successful),
            'failed_count': len(results) - len(successful)
        }
        
        print(f"\n[OK] 多线程压缩完成")
        print(f"  总耗时: {elapsed:.2f} 秒")
        print(f"  原始大小: {total_original / 1024 / 1024:.2f} MB")
        print(f"  压缩后总大小: {total_archive / 1024 / 1024:.2f} MB")
        print(f"  压缩率: {compression_ratio:.2f}%")
        print(f"  成功: {len(successful)}, 失败: {len(results) - len(successful)}")
        
        self.results.append(result)
        return result
    
    def test_multiprocessing_approach(self, files: List[Path], process_count: int = 4, compression_level: int = 5):
        """测试多进程方法"""
        print(f"\n{'='*60}")
        print(f"测试 4: 多进程压缩 (进程数: {process_count}, 压缩级别: {compression_level})")
        print(f"{'='*60}")
        
        from multiprocessing import Pool
        
        archives_dir = self.test_dir / "multiprocess_archives"
        archives_dir.mkdir(exist_ok=True)
        
        def compress_file(args):
            """压缩单个文件（用于多进程）"""
            file_path, index, level = args
            archive_path = archives_dir / f"archive_{index:03d}.7z"
            start_time = time.time()
            
            try:
                with py7zr.SevenZipFile(
                    archive_path,
                    mode='w',
                    filters=[{'id': py7zr.FILTER_LZMA2, 'preset': level}]
                ) as archive:
                    archive.write(file_path, file_path.name)
                
                elapsed = time.time() - start_time
                archive_size = archive_path.stat().st_size
                original_size = file_path.stat().st_size
                
                return {
                    'file': file_path.name,
                    'elapsed': elapsed,
                    'original_size': original_size,
                    'archive_size': archive_size,
                    'success': True
                }
            except Exception as e:
                return {
                    'file': file_path.name,
                    'elapsed': time.time() - start_time,
                    'error': str(e),
                    'success': False
                }
        
        start_time = time.time()
        
        with Pool(processes=process_count) as pool:
            args = [(file_path, i, compression_level) for i, file_path in enumerate(files)]
            results = pool.map(compress_file, args)
        
        elapsed = time.time() - start_time
        
        successful = [r for r in results if r.get('success')]
        total_original = sum(r['original_size'] for r in successful)
        total_archive = sum(r['archive_size'] for r in successful)
        compression_ratio = (1 - total_archive / total_original) * 100 if total_original > 0 else 0
        
        result = {
            'test': 'multiprocessing',
            'process_count': process_count,
            'compression_level': compression_level,
            'elapsed_time': elapsed,
            'original_size': total_original,
            'archive_size': total_archive,
            'compression_ratio': compression_ratio,
            'files_count': len(successful),
            'failed_count': len(results) - len(successful)
        }
        
        print(f"\n[OK] 多进程压缩完成")
        print(f"  总耗时: {elapsed:.2f} 秒")
        print(f"  原始大小: {total_original / 1024 / 1024:.2f} MB")
        print(f"  压缩后总大小: {total_archive / 1024 / 1024:.2f} MB")
        print(f"  压缩率: {compression_ratio:.2f}%")
        print(f"  成功: {len(successful)}, 失败: {len(results) - len(successful)}")
        
        self.results.append(result)
        return result
    
    def test_threads_parameter(self, files: List[Path], threads: int = None, compression_level: int = 5):
        """测试 threads 参数（直接指定线程数）"""
        if threads is None:
            threads = os.cpu_count()
        
        print(f"\n{'='*60}")
        print(f"测试 5: threads 参数测试 (threads={threads}, 压缩级别: {compression_level})")
        print(f"{'='*60}")
        
        archive_path = self.test_dir / f"test_threads_{threads}.7z"
        
        # 检查 threads 参数是否支持
        try:
            import inspect
            sig = inspect.signature(py7zr.SevenZipFile.__init__)
            has_threads = 'threads' in sig.parameters
            print(f"  SevenZipFile 是否支持 threads 参数: {has_threads}")
            if has_threads:
                print(f"  threads 参数类型: {sig.parameters['threads'].annotation}")
                print(f"  threads 参数默认值: {sig.parameters['threads'].default}")
        except Exception as e:
            print(f"  无法检查参数签名: {e}")
            has_threads = False
        
        start_time = time.time()
        
        try:
            kwargs = {
                'mode': 'w',
                'filters': [{'id': py7zr.FILTER_LZMA2, 'preset': compression_level}]
            }
            
            # 尝试添加 threads 参数
            try:
                kwargs['threads'] = threads
                print(f"  使用参数: {kwargs}")
            except TypeError as e:
                print(f"  警告: threads 参数可能不被支持: {e}")
                if 'threads' in kwargs:
                    del kwargs['threads']
            
            # 同时尝试 mp 参数
            kwargs['mp'] = True
            print(f"  同时启用 mp=True")
            
            with py7zr.SevenZipFile(archive_path, **kwargs) as archive:
                for file_path in files:
                    archive.write(file_path, file_path.name)
            
            elapsed = time.time() - start_time
            archive_size = archive_path.stat().st_size
            original_size = sum(f.stat().st_size for f in files)
            compression_ratio = (1 - archive_size / original_size) * 100
            
            # 监控 CPU 使用情况
            cpu_usage = self._monitor_cpu_during_compression(start_time, elapsed)
            
            result = {
                'test': 'threads_parameter',
                'test_number': '测试5',
                'threads': threads,
                'mp': True,
                'compression_level': compression_level,
                'elapsed_time': elapsed,
                'original_size': original_size,
                'archive_size': archive_size,
                'compression_ratio': compression_ratio,
                'files_count': len(files),
                'avg_cpu_percent': cpu_usage.get('avg', 0),
                'max_cpu_percent': cpu_usage.get('max', 0),
                'threads_supported': has_threads if 'has_threads' in locals() else False
            }
            
            print(f"[OK] 压缩完成")
            print(f"  耗时: {elapsed:.2f} 秒")
            print(f"  原始大小: {original_size / 1024 / 1024:.2f} MB")
            print(f"  压缩后大小: {archive_size / 1024 / 1024:.2f} MB")
            print(f"  压缩率: {compression_ratio:.2f}%")
            print(f"  平均 CPU 使用率: {cpu_usage.get('avg', 0):.1f}%")
            print(f"  最大 CPU 使用率: {cpu_usage.get('max', 0):.1f}%")
            if 'has_threads' in locals():
                print(f"  threads 参数支持: {has_threads}")
            
            self.results.append(result)
            return result
            
        except TypeError as e:
            if 'threads' in str(e):
                print(f"  [ERROR] threads 参数不被支持: {e}")
                print(f"  尝试不使用 threads 参数...")
                
                # 重试，不使用 threads 参数
                try:
                    kwargs = {
                        'mode': 'w',
                        'filters': [{'id': py7zr.FILTER_LZMA2, 'preset': compression_level}],
                        'mp': True
                    }
                    
                    with py7zr.SevenZipFile(archive_path, **kwargs) as archive:
                        for file_path in files:
                            archive.write(file_path, file_path.name)
                    
                    elapsed = time.time() - start_time
                    archive_size = archive_path.stat().st_size
                    original_size = sum(f.stat().st_size for f in files)
                    compression_ratio = (1 - archive_size / original_size) * 100
                    
                    result = {
                        'test': 'threads_parameter',
                        'test_number': '测试5',
                        'threads': threads,
                        'mp': True,
                        'compression_level': compression_level,
                        'elapsed_time': elapsed,
                        'original_size': original_size,
                        'archive_size': archive_size,
                        'compression_ratio': compression_ratio,
                        'files_count': len(files),
                        'threads_supported': False,
                        'note': 'threads参数不支持，仅使用mp=True'
                    }
                    
                    print(f"[OK] 压缩完成（未使用threads参数）")
                    print(f"  耗时: {elapsed:.2f} 秒")
                    print(f"  压缩率: {compression_ratio:.2f}%")
                    
                    self.results.append(result)
                    return result
                except Exception as e2:
                    print(f"[ERROR] 重试也失败: {e2}")
                    return None
            else:
                print(f"[ERROR] 测试失败: {e}")
                import traceback
                traceback.print_exc()
                return None
        except Exception as e:
            print(f"[ERROR] 测试失败: {e}")
            import traceback
            traceback.print_exc()
            return None
    
    def test_environment_variable(self, files: List[Path], threads: int = 4, compression_level: int = 5):
        """测试环境变量设置线程数"""
        print(f"\n{'='*60}")
        print(f"测试 6: 环境变量设置线程数 (7Z_THREADS={threads}, 压缩级别: {compression_level})")
        print(f"{'='*60}")
        
        archive_path = self.test_dir / f"test_env_threads_{threads}.7z"
        
        original_env = os.environ.get('7Z_THREADS')
        
        try:
            # 设置环境变量
            os.environ['7Z_THREADS'] = str(threads)
            print(f"  设置环境变量: 7Z_THREADS={threads}")
            
            start_time = time.time()
            
            with py7zr.SevenZipFile(
                archive_path,
                mode='w',
                filters=[{'id': py7zr.FILTER_LZMA2, 'preset': compression_level}],
                mp=True  # 同时启用 mp
            ) as archive:
                for file_path in files:
                    archive.write(file_path, file_path.name)
            
            elapsed = time.time() - start_time
            archive_size = archive_path.stat().st_size
            original_size = sum(f.stat().st_size for f in files)
            compression_ratio = (1 - archive_size / original_size) * 100
            
            result = {
                'test': 'environment_variable',
                'test_number': '测试6',
                '7Z_THREADS': threads,
                'mp': True,
                'compression_level': compression_level,
                'elapsed_time': elapsed,
                'original_size': original_size,
                'archive_size': archive_size,
                'compression_ratio': compression_ratio,
                'files_count': len(files)
            }
            
            print(f"[OK] 压缩完成")
            print(f"  耗时: {elapsed:.2f} 秒")
            print(f"  原始大小: {original_size / 1024 / 1024:.2f} MB")
            print(f"  压缩后大小: {archive_size / 1024 / 1024:.2f} MB")
            print(f"  压缩率: {compression_ratio:.2f}%")
            
            self.results.append(result)
            return result
            
        except Exception as e:
            print(f"[ERROR] 测试失败: {e}")
            import traceback
            traceback.print_exc()
            return None
        finally:
            # 恢复环境变量
            if original_env is not None:
                os.environ['7Z_THREADS'] = original_env
            elif '7Z_THREADS' in os.environ:
                del os.environ['7Z_THREADS']
    
    def test_7zip_command_line(self, files: List[Path], threads: int = None, compression_level: int = 5, md_l:int=128):
        """测试使用7-Zip命令行工具压缩（使用-mmt参数指定线程数）""" "-md1g"
        if threads is None:
            threads = os.cpu_count()
        
        print(f"\n{'='*60}")
        print(f"测试 7: 7-Zip命令行工具 (threads={threads}, -mmt={threads}, 压缩级别: {compression_level}),字典：{md_l}")
        print(f"{'='*60}")
        
        archive_path = self.test_dir / f"test_7zip_cli_{threads}_{compression_level}_{md_l}.7z"
        
        # 查找7z.exe路径
        possible_paths = [
            r"C:\Program Files\7-Zip\7z.exe",
            r"C:\Program Files (x86)\7-Zip\7z.exe",
            "7z.exe",  # 如果在PATH中
        ]
        
        sevenzip_path = None
        for path in possible_paths:
            if Path(path).exists() if not path == "7z.exe" else shutil.which("7z.exe"):
                sevenzip_path = path if path != "7z.exe" else shutil.which("7z.exe")
                break
        
        if not sevenzip_path:
            print(f"  [ERROR] 未找到7z.exe，请确保7-Zip已安装")
            print(f"  尝试的路径: {possible_paths}")
            return None
        
        print(f"  使用7z.exe路径: {sevenzip_path}")
        print(f"  线程数: {threads}")
        print(f"  压缩级别: {compression_level}")
        
        start_time = time.time()
        
        try:
            # 构建7z命令
            # 7z a -mmt<N> -mx<N> archive.7z files...
            cmd = [
                sevenzip_path,
                "a",  # Add files to archive
                f"-mmt{threads}",  # 设置线程数
                f"-mx{compression_level}",  # 设置压缩级别
                f"-md{md_l}m",  # 设置字典大小"-md1g"
                str(archive_path),
            ]
            
            # 添加所有文件路径
            for file_path in files:
                cmd.append(str(file_path))
            
            print(f"  执行命令: {' '.join(cmd[:5])} ... ({len(files)} 个文件)")
            
            # 执行命令
            process = subprocess.run(
                cmd,
                capture_output=True,
                text=True,
                timeout=300  # 5分钟超时
            )
            
            if process.returncode != 0:
                print(f"  [ERROR] 7z命令执行失败，返回码: {process.returncode}")
                print(f"  错误输出: {process.stderr[:500]}")
                return None
            
            elapsed = time.time() - start_time
            
            # 检查压缩包是否存在
            if not archive_path.exists():
                print(f"  [ERROR] 压缩包未生成: {archive_path}")
                return None
            
            archive_size = archive_path.stat().st_size
            original_size = sum(f.stat().st_size for f in files)
            compression_ratio = (1 - archive_size / original_size) * 100
            
            # 监控 CPU 使用情况
            cpu_usage = self._monitor_cpu_during_compression(start_time, elapsed)
            
            result = {
                'test': '7zip_command_line',
                'test_number': '测试7',
                'threads': threads,
                'compression_level': compression_level,
                'elapsed_time': elapsed,
                'original_size': original_size,
                'archive_size': archive_size,
                'compression_ratio': compression_ratio,
                'files_count': len(files),
                'avg_cpu_percent': cpu_usage.get('avg', 0),
                'max_cpu_percent': cpu_usage.get('max', 0),
                'sevenzip_path': sevenzip_path,
                'command': ' '.join(cmd[:5]) + ' ...'
            }
            
            print(f"[OK] 压缩完成")
            print(f"  耗时: {elapsed:.2f} 秒")
            print(f"  原始大小: {original_size / 1024 / 1024:.2f} MB")
            print(f"  压缩后大小: {archive_size / 1024 / 1024:.2f} MB")
            print(f"  压缩率: {compression_ratio:.2f}%")
            print(f"  平均 CPU 使用率: {cpu_usage.get('avg', 0):.1f}%")
            print(f"  最大 CPU 使用率: {cpu_usage.get('max', 0):.1f}%")
            
            # 解析7z输出中的信息（如果有）
            if process.stdout:
                # 尝试提取压缩统计信息
                output_lines = process.stdout.split('\n')
                for line in output_lines:
                    if 'Everything is Ok' in line:
                        print(f"  7z输出: {line.strip()}")
                    elif 'Compressed' in line and 'ratio' in line.lower():
                        print(f"  7z统计: {line.strip()}")
            
            self.results.append(result)
            return result
            
        except subprocess.TimeoutExpired:
            print(f"[ERROR] 7z命令执行超时（>5分钟）")
            return None
        except FileNotFoundError:
            print(f"[ERROR] 7z.exe未找到: {sevenzip_path}")
            return None
        except Exception as e:
            print(f"[ERROR] 测试失败: {e}")
            import traceback
            traceback.print_exc()
            return None
    
    def _monitor_cpu_during_compression(self, start_time: float, duration: float, interval: float = 0.5):
        """监控压缩期间的 CPU 使用情况"""
        cpu_samples = []
        end_time = start_time + duration
        
        while time.time() < end_time:
            cpu_percent = psutil.cpu_percent(interval=0.1)
            cpu_samples.append(cpu_percent)
            time.sleep(interval)
        
        if cpu_samples:
            return {
                'avg': sum(cpu_samples) / len(cpu_samples),
                'max': max(cpu_samples),
                'min': min(cpu_samples),
                'samples': len(cpu_samples)
            }
        return {'avg': 0, 'max': 0, 'min': 0, 'samples': 0}
    
    def print_summary(self):
        """打印测试结果总结"""
        print(f"\n{'='*60}")
        print("测试结果总结")
        print(f"{'='*60}")
        
        if not self.results:
            print("没有测试结果")
            return
        
        # 测试编号映射
        test_number_map = {
            'basic_compression': '测试1',
            'mp_compression': '测试2',
            'multithreading': '测试3',
            'multiprocessing': '测试4',
            'threads_parameter': '测试5',
            'environment_variable': '测试6',
            '7zip_command_line': '测试7'
        }
        
        # 对于mp_compression，需要区分mp=True和mp=False
        mp_true_count = 0
        mp_false_count = 0
        
        print(f"\n{'测试编号':<10} {'测试方法':<25} {'耗时(秒)':<12} {'压缩率(%)':<12} {'CPU使用率':<15}")
        print("-" * 75)
        
        test_index = 1
        for result in self.results:
            test_name = result['test']
            
            # 生成测试编号
            if test_name == 'basic_compression':
                test_number = '测试1'
            elif test_name == 'mp_compression':
                if result.get('mp', False):
                    mp_true_count += 1
                    if mp_true_count == 1:
                        test_number = '测试2'
                        test_name_display = 'mp_compression (mp=False)'
                    else:
                        test_number = f'测试{test_index}'
                        test_name_display = 'mp_compression (mp=True)'
                else:
                    mp_false_count += 1
                    test_number = '测试2'
                    test_name_display = 'mp_compression (mp=False)'
            elif test_name == 'multithreading':
                test_number = '测试3'
                test_name_display = 'multithreading'
            elif test_name == 'multiprocessing':
                test_number = '测试4'
                test_name_display = 'multiprocessing'
            elif test_name == 'threads_parameter':
                test_number = '测试5'
                test_name_display = 'threads_parameter'
            elif test_name == 'environment_variable':
                test_number = '测试6'
                test_name_display = 'environment_variable'
            elif test_name == '7zip_command_line':
                test_number = '测试7'
                test_name_display = '7zip_command_line'
            else:
                test_number = f'测试{test_index}'
                test_name_display = test_name
            
            # 如果没有特殊处理，使用默认显示名称
            if 'test_name_display' not in locals():
                if test_name == 'mp_compression':
                    pass  # 已经设置了
                else:
                    test_name_display = test_name
            
            elapsed = result.get('elapsed_time', 0)
            ratio = result.get('compression_ratio', 0)
            cpu_info = ""
            
            if 'avg_cpu_percent' in result and result.get('avg_cpu_percent', 0) > 0:
                cpu_info = f"{result['avg_cpu_percent']:.1f}%"
            elif 'mp' in result and result['mp']:
                cpu_info = "多进程启用"
            
            # 对于multithreading，显示线程数信息
            if test_name == 'multithreading' and 'thread_count' in result:
                test_name_display = f"{test_name_display} (线程数:{result['thread_count']})"
            
            # 对于threads_parameter，显示线程数和是否支持
            if test_name == 'threads_parameter':
                threads = result.get('threads', 'N/A')
                supported = result.get('threads_supported', False)
                test_name_display = f"{test_name_display} (threads={threads}, 支持={supported})"
            
            # 对于7zip_command_line，显示线程数和使用的方法
            if test_name == '7zip_command_line':
                threads = result.get('threads', 'N/A')
                test_name_display = f"{test_name_display} (-mmt={threads})"
            
            print(f"{test_number:<10} {test_name_display:<25} {elapsed:<12.2f} {ratio:<12.2f} {cpu_info:<15}")
            test_index += 1
        
        # 找出最快的测试
        if len(self.results) > 1:
            fastest = min(self.results, key=lambda x: x.get('elapsed_time', float('inf')))
            fastest_number = test_number_map.get(fastest['test'], f"测试{test_index}")
            if fastest['test'] == 'mp_compression' and fastest.get('mp'):
                fastest_number = '测试2 (mp=True)'
            print(f"\n最快的测试方法: {fastest_number} - {fastest['test']} ({fastest.get('elapsed_time', 0):.2f} 秒)")


def main():
    """主测试函数"""
    if not PY7ZR_AVAILABLE:
        print("错误: py7zr 未安装")
        print("请运行: pip install py7zr")
        return 1
    
    print("="*60)
    print("py7zr 压缩库测试")
    print("="*60)
    
    # 显示系统信息
    cpu_count = psutil.cpu_count(logical=True)
    cpu_count_physical = psutil.cpu_count(logical=False)
    memory = psutil.virtual_memory()
    
    print(f"\n系统信息:")
    print(f"  CPU 核心数 (逻辑): {cpu_count}")
    print(f"  CPU 核心数 (物理): {cpu_count_physical}")
    print(f"  内存: {memory.total / 1024 / 1024 / 1024:.2f} GB")
    print(f"  可用内存: {memory.available / 1024 / 1024 / 1024:.2f} GB")
    
    # 显示 py7zr 版本信息
    try:
        print(f"\npy7zr 版本: {py7zr.__version__}")
    except:
        print(f"\npy7zr 版本: 未知")
    
    tester = Py7zrCompressionTester()
    
    try:
        # 创建测试文件
        # 使用较小的文件进行快速测试
        test_files = tester.create_test_files(count=10, size_mb=1000)
        
        # 测试 1: 基本压缩
       # tester.test_basic_compression(test_files, compression_level=5)
        
        # 测试 2: mp 参数
      #  tester.test_mp_parameter(test_files, mp=False, compression_level=5)
      #  tester.test_mp_parameter(test_files, mp=True, compression_level=5)
        
        # 测试 3: 多线程方法
       # tester.test_multithreading_approach(test_files, thread_count=4, compression_level=5)
        
        # 测试 4: 多进程方法
        # tester.test_multiprocessing_approach(test_files, process_count=4, compression_level=5)
        
        # 测试 5: threads 参数（使用 os.cpu_count()）
     #   tester.test_threads_parameter(test_files, threads=os.cpu_count(), compression_level=5)
        
        # 测试 6: 环境变量
       # tester.test_environment_variable(test_files, threads=4, compression_level=5)
        
        # 测试 7: 7-Zip命令行工具（使用-mmt参数指定线程数）
        tester.test_7zip_command_line(test_files, threads=os.cpu_count(), compression_level=0,md_l=128)
        tester.test_7zip_command_line(test_files, threads=os.cpu_count(), compression_level=0,md_l=256)
        
        tester.test_7zip_command_line(test_files, threads=os.cpu_count(), compression_level=1,md_l=128)
        tester.test_7zip_command_line(test_files, threads=os.cpu_count(), compression_level=1,md_l=256)
        
        tester.test_7zip_command_line(test_files, threads=os.cpu_count(), compression_level=3,md_l=128)
        tester.test_7zip_command_line(test_files, threads=os.cpu_count(), compression_level=3,md_l=256)
        
        tester.test_7zip_command_line(test_files, threads=os.cpu_count(), compression_level=5,md_l=128)
        tester.test_7zip_command_line(test_files, threads=os.cpu_count(), compression_level=5,md_l=256)
        
        tester.test_7zip_command_line(test_files, threads=os.cpu_count(), compression_level=7,md_l=128)
        tester.test_7zip_command_line(test_files, threads=os.cpu_count(), compression_level=7,md_l=256)
        
        tester.test_7zip_command_line(test_files, threads=os.cpu_count(), compression_level=9,md_l=128) 
        tester.test_7zip_command_line(test_files, threads=os.cpu_count(), compression_level=9,md_l=256)
     
        # 打印总结
        tester.print_summary()
        
        print(f"\n测试完成！测试目录: {tester.test_dir}")
        print("您可以手动检查生成的压缩文件")
        
    except KeyboardInterrupt:
        print("\n\n测试被用户中断")
        return 1
    except Exception as e:
        print(f"\n\n测试过程中发生错误: {e}")
        import traceback
        traceback.print_exc()
        return 1
    
    return 0


if __name__ == "__main__":
    sys.exit(main())