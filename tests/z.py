import lz4.frame
import os
import time
import threading
import random
import string
from concurrent.futures import ThreadPoolExecutor, as_completed
import json
from pathlib import Path
import pandas as pd

class LZ4CompressionTester:
    def __init__(self, test_dir="temp"):
        self.test_dir = Path(test_dir)
        self.test_dir.mkdir(exist_ok=True)
        self.results = []
        
    def generate_test_data(self, num_files=5, sizes_kb=[100000, 1000000, 5000000, 10000000]):
        """生成多种类型的测试数据"""
        print("生成测试文件中...")
        test_files = []
        
        # 不同类型的数据模板
        data_types = {
            'text': string.ascii_letters + string.digits + ' ' * 10 + '\n' * 2,
            'json': string.ascii_letters + string.digits + '{}[],: ',
            'log': string.ascii_letters + string.digits + ' :.-_[]()' + '\n' * 3,
            'binary': None  # 纯随机字节
        }
        
        for i in range(num_files):
            for size_kb in sizes_kb:
                for data_type, charset in data_types.items():
                    filename = self.test_dir / f"test_{data_type}_{i}_{size_kb}KB.dat"
                    size_bytes = size_kb * 1024
                    
                    if data_type == 'binary':
                        # 生成随机二进制数据
                        content = bytes(random.getrandbits(8) for _ in range(size_bytes))
                    else:
                        # 生成文本数据
                        text_content = ''.join(random.choices(
                            charset, 
                            k=size_bytes
                        ))
                        content = text_content.encode('utf-8')
                    
                    # 写入文件
                    with open(filename, 'wb') as f:
                        f.write(content)
                    
                    test_files.append(filename)
                    print(f"生成文件: {filename} ({size_kb}KB, {data_type})")
        
        return test_files
    
    def compress_file(self, input_file, compression_level=1, block_size=0, 
                     content_checksum=False, block_checksum=False):
        """压缩单个文件"""
        output_file = input_file.with_suffix(f'.lz4_cl{compression_level}_bs{block_size}')
        
        try:
            start_time = time.time()
            
            with open(input_file, 'rb') as f_in:
                original_data = f_in.read()
            
            # 使用LZ4压缩
            compressed_data = lz4.frame.compress(
                original_data,
                compression_level=compression_level,
                block_size=block_size,
                content_checksum=content_checksum,
                block_checksum=block_checksum,
                store_size=True
            )
            
            compression_time = time.time() - start_time
            
            # 写入压缩文件
            with open(output_file, 'wb') as f_out:
                f_out.write(compressed_data)
            
            # 计算压缩率和其他指标
            original_size = len(original_data)
            compressed_size = len(compressed_data)
            compression_ratio = (compressed_size / original_size) * 100
            space_saving = 100 - compression_ratio
            compression_speed = original_size / compression_time / 1024 / 1024  # MB/s
            
            result = {
                'input_file': str(input_file),
                'output_file': str(output_file),
                'file_size_kb': original_size / 1024,
                'data_type': input_file.stem.split('_')[1],
                'compression_level': compression_level,
                'block_size': block_size,
                'content_checksum': content_checksum,
                'block_checksum': block_checksum,
                'original_size': original_size,
                'compressed_size': compressed_size,
                'compression_ratio': compression_ratio,
                'space_saving': space_saving,
                'compression_time': compression_time,
                'compression_speed_mbps': compression_speed,
                'thread_id': threading.current_thread().name
            }
            
            return result
            
        except Exception as e:
            print(f"压缩文件 {input_file} 时出错: {e}")
            return None
    
    def run_compression_tests(self, test_files, compression_params):
        """运行多线程压缩测试"""
        print("\n开始多线程压缩测试...")
        print(f"测试文件数量: {len(test_files)}")
        print(f"参数组合数量: {len(compression_params)}")
        print(f"总任务数: {len(test_files) * len(compression_params)}")
        
        all_tasks = []
        for file_path in test_files:
            for params in compression_params:
                all_tasks.append((file_path, params))
        
        total_tasks = len(all_tasks)
        completed_tasks = 0
        start_time = time.time()
        
        with ThreadPoolExecutor(max_workers=min(12, len(all_tasks))) as executor:
            future_to_task = {
                executor.submit(self.compress_file, file_path, **params): (file_path, params)
                for file_path, params in all_tasks
            }
            
            for future in as_completed(future_to_task):
                file_path, params = future_to_task[future]
                try:
                    result = future.result()
                    if result:
                        self.results.append(result)
                        completed_tasks += 1
                        
                        # 进度显示
                        if completed_tasks % 10 == 0 or completed_tasks == total_tasks:
                            elapsed = time.time() - start_time
                            speed = completed_tasks / elapsed if elapsed > 0 else 0
                            eta = (total_tasks - completed_tasks) / speed if speed > 0 else 0
                            
                            print(f"进度: {completed_tasks}/{total_tasks} "
                                  f"({completed_tasks/total_tasks*100:.1f}%) - "
                                  f"速度: {speed:.1f} 任务/秒 - ETA: {eta:.0f}秒")
                        
                except Exception as e:
                    print(f"任务失败: {file_path} {params}, 错误: {e}")
        
        total_time = time.time() - start_time
        print(f"\n所有压缩任务完成! 总用时: {total_time:.2f}秒")
        print(f"平均速度: {total_tasks/total_time:.2f} 任务/秒")
    
    def analyze_compression_results(self):
        """详细分析压缩结果"""
        if not self.results:
            print("没有测试结果可分析")
            return
        
        print("\n" + "="*100)
        print("LZ4 压缩测试结果详细分析")
        print("="*100)
        
        # 使用pandas进行数据分析
        df = pd.DataFrame(self.results)
        
        # 按压缩级别分析
        print("\n1. 按压缩级别分析:")
        level_stats = df.groupby('compression_level').agg({
            'compression_ratio': ['mean', 'min', 'max', 'std'],
            'compression_time': ['mean', 'min', 'max'],
            'compression_speed_mbps': ['mean', 'min', 'max'],
            'input_file': 'count'
        }).round(3)
        
        print(level_stats)
        
        # 按数据类型分析
        print("\n2. 按数据类型分析:")
        type_stats = df.groupby('data_type').agg({
            'compression_ratio': 'mean',
            'compression_speed_mbps': 'mean',
            'input_file': 'count'
        }).round(3)
        print(type_stats)
        
        # 按文件大小分析
        print("\n3. 按文件大小分析:")
        df['size_category'] = pd.cut(df['file_size_kb'], 
                                   bins=[0, 100, 500, 1000, float('inf')],
                                   labels=['0-100KB', '100-500KB', '500-1000KB', '1MB+'])
        size_stats = df.groupby('size_category').agg({
            'compression_ratio': 'mean',
            'compression_speed_mbps': 'mean',
            'input_file': 'count'
        }).round(3)
        print(size_stats)
        
        # 找出最佳配置
        print("\n4. 最佳配置推荐:")
        
        # 最佳压缩率
        best_ratio = df.loc[df['compression_ratio'].idxmin()]
        print(f"最佳压缩率: {best_ratio['compression_ratio']:.2f}%")
        print(f"  配置: 级别{best_ratio['compression_level']}, "
              f"块大小{best_ratio['block_size']}, "
              f"文件: {Path(best_ratio['input_file']).name}")
        
        # 最快压缩速度
        best_speed = df.loc[df['compression_speed_mbps'].idxmax()]
        print(f"最快压缩速度: {best_speed['compression_speed_mbps']:.2f} MB/s")
        print(f"  配置: 级别{best_speed['compression_level']}, "
              f"块大小{best_speed['block_size']}, "
              f"文件: {Path(best_speed['input_file']).name}")
        
        # 最佳平衡（压缩率前10%中速度最快的）
        top_10_percent = df.nsmallest(len(df) // 10, 'compression_ratio')
        best_balanced = top_10_percent.loc[top_10_percent['compression_speed_mbps'].idxmax()]
        print(f"最佳平衡: 压缩率{best_balanced['compression_ratio']:.2f}%, "
              f"速度{best_balanced['compression_speed_mbps']:.2f} MB/s")
        print(f"  配置: 级别{best_balanced['compression_level']}, "
              f"块大小{best_balanced['block_size']}")
    
    def save_detailed_results(self):
        """保存详细结果到文件"""
        if not self.results:
            print("没有结果可保存")
            return
            
        # 保存JSON格式的详细结果
        json_file = self.test_dir / "compression_detailed_results.json"
        with open(json_file, 'w', encoding='utf-8') as f:
            # 转换numpy类型为Python原生类型
            serializable_results = []
            for result in self.results:
                serializable_result = {}
                for key, value in result.items():
                    if hasattr(value, 'item'):  # 处理numpy类型
                        serializable_result[key] = value.item()
                    else:
                        serializable_result[key] = value
                serializable_results.append(serializable_result)
            
            json.dump(serializable_results, f, indent=2, ensure_ascii=False)
        
        # 保存CSV格式便于分析
        csv_file = self.test_dir / "compression_results.csv"
        df = pd.DataFrame(self.results)
        df.to_csv(csv_file, index=False, encoding='utf-8')
        
        print(f"\n详细结果已保存:")
        print(f"  JSON格式: {json_file}")
        print(f"  CSV格式: {csv_file}")
    
    def print_summary(self):
        """打印测试摘要"""
        if not self.results:
            return
            
        print("\n" + "="*60)
        print("测试摘要")
        print("="*60)
        
        total_original_size = sum(r['original_size'] for r in self.results) / 1024 / 1024
        total_compressed_size = sum(r['compressed_size'] for r in self.results) / 1024 / 1024
        total_time = sum(r['compression_time'] for r in self.results)
        
        print(f"总测试文件数: {len(set(r['input_file'] for r in self.results))}")
        print(f"总压缩任务数: {len(self.results)}")
        print(f"总原始数据量: {total_original_size:.2f} MB")
        print(f"总压缩后数据量: {total_compressed_size:.2f} MB")
        print(f"总体压缩率: {(total_compressed_size/total_original_size)*100:.2f}%")
        print(f"总压缩时间: {total_time:.2f} 秒")
        print(f"平均压缩速度: {total_original_size/total_time:.2f} MB/s")

def main():
    # 创建测试器实例
    tester = LZ4CompressionTester("temp")
    
    try:
        # 生成测试文件
        test_files = tester.generate_test_data(
            num_files=3,  # 每种类型生成3个文件
            sizes_kb=[500, 2000, 5000, 10000]  # 4种大小
        )
        
        # 定义压缩参数组合
        compression_params = [
            # 快速压缩模式
            {'compression_level': 1, 'block_size': 0, 'content_checksum': False, 'block_checksum': False},
            {'compression_level': 3, 'block_size': 0, 'content_checksum': False, 'block_checksum': False},
            {'compression_level': 5, 'block_size': 0, 'content_checksum': False, 'block_checksum': False},
            
            # 平衡模式
            {'compression_level': 6, 'block_size': 0, 'content_checksum': True, 'block_checksum': False},
            {'compression_level': 7, 'block_size': 0, 'content_checksum': True, 'block_checksum': False},
            {'compression_level': 8, 'block_size': 0, 'content_checksum': True, 'block_checksum': False},
            
            # 高压缩模式
            {'compression_level': 9, 'block_size': 0, 'content_checksum': True, 'block_checksum': True},
            {'compression_level': 12, 'block_size': 0, 'content_checksum': True, 'block_checksum': True},
            {'compression_level': 16, 'block_size': 0, 'content_checksum': True, 'block_checksum': True},
            
            # 不同块大小测试
            {'compression_level': 6, 'block_size': 65536, 'content_checksum': False, 'block_checksum': False},
            {'compression_level': 6, 'block_size': 262144, 'content_checksum': False, 'block_checksum': False},
            {'compression_level': 6, 'block_size': 1048576, 'content_checksum': False, 'block_checksum': False},
        ]
        
        # 运行压缩测试
        tester.run_compression_tests(test_files, compression_params)
        
        # 分析结果
        tester.analyze_compression_results()
        
        # 保存结果
        tester.save_detailed_results()
        
        # 打印摘要
        tester.print_summary()
        
    except Exception as e:
        print(f"测试过程中出错: {e}")
        import traceback
        traceback.print_exc()
    
    print(f"\n测试完成! 所有压缩文件保存在: {tester.test_dir}")

if __name__ == "__main__":
    # 检查是否安装了必要库
    try:
        import lz4.frame
        import pandas as pd
    except ImportError as e:
        print(f"请先安装必要的库: {e}")
        print("安装命令: pip install lz4 pandas")
        exit(1)
    
    main()