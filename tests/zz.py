import lz4.frame
import os
import threading
from concurrent.futures import ThreadPoolExecutor, as_completed
from pathlib import Path
import time
import argparse

class DirectoryCompressor:
    def __init__(self, source_dir, output_file, compression_level=1, num_threads=4):
        self.source_dir = Path(source_dir)
        self.output_file = Path(output_file)
        self.compression_level = compression_level
        self.num_threads = num_threads
        self.file_list = []
        self.compressed_files = {}
        self.lock = threading.Lock()
        
        # 确保输出目录存在
        self.output_file.parent.mkdir(parents=True, exist_ok=True)
        
    def scan_directory(self):
        """扫描目录，获取所有文件列表"""
        print(f"扫描目录: {self.source_dir}")
        
        if not self.source_dir.exists():
            raise FileNotFoundError(f"源目录不存在: {self.source_dir}")
        
        for root, dirs, files in os.walk(self.source_dir):
            for file in files:
                file_path = Path(root) / file
                # 计算相对路径
                relative_path = file_path.relative_to(self.source_dir)
                self.file_list.append((str(file_path), str(relative_path)))
        
        print(f"找到 {len(self.file_list)} 个文件")
        return self.file_list
    
    def compress_single_file(self, file_info):
        """压缩单个文件"""
        file_path, relative_path = file_info
        
        try:
            with open(file_path, 'rb') as f:
                file_data = f.read()
            
            # 使用LZ4压缩文件数据
            compressed_data = lz4.frame.compress(
                file_data,
                compression_level=self.compression_level,
                content_checksum=True,
                block_checksum=False
            )
            
            file_info = {
                'relative_path': relative_path,
                'original_size': len(file_data),
                'compressed_size': len(compressed_data),
                'compressed_data': compressed_data
            }
            
            return file_info
            
        except Exception as e:
            print(f"压缩文件失败 {file_path}: {e}")
            return None
    
    def compress_directory(self):
        """多线程压缩整个目录"""
        if not self.file_list:
            self.scan_directory()
        
        print(f"\n开始压缩目录...")
        print(f"压缩级别: {self.compression_level}")
        print(f"线程数: {self.num_threads}")
        print(f"输出文件: {self.output_file}")
        
        total_files = len(self.file_list)
        completed_files = 0
        start_time = time.time()
        
        with ThreadPoolExecutor(max_workers=self.num_threads) as executor:
            # 提交所有压缩任务
            future_to_file = {
                executor.submit(self.compress_single_file, file_info): file_info 
                for file_info in self.file_list
            }
            
            # 处理完成的任务
            for future in as_completed(future_to_file):
                file_info = future_to_file[future]
                try:
                    result = future.result()
                    if result:
                        with self.lock:
                            self.compressed_files[result['relative_path']] = result
                            completed_files += 1
                            
                        # 显示进度
                        if completed_files % 10 == 0 or completed_files == total_files:
                            elapsed = time.time() - start_time
                            progress = (completed_files / total_files) * 100
                            speed = completed_files / elapsed if elapsed > 0 else 0
                            
                            print(f"进度: {completed_files}/{total_files} ({progress:.1f}%) - "
                                  f"速度: {speed:.1f} 文件/秒")
                            
                except Exception as e:
                    print(f"处理文件失败 {file_info[0]}: {e}")
        
        return self.compressed_files
    
    def create_archive(self):
        """创建最终的压缩档案文件"""
        print("\n创建压缩档案文件...")
        
        # 创建文件索引
        file_index = {}
        total_original_size = 0
        total_compressed_size = 0
        
        for relative_path, file_info in self.compressed_files.items():
            file_index[relative_path] = {
                'original_size': file_info['original_size'],
                'compressed_size': file_info['compressed_size'],
                'offset': 0  # 将在写入时计算
            }
            total_original_size += file_info['original_size']
            total_compressed_size += file_info['compressed_size']
        
        # 写入压缩文件
        with open(self.output_file, 'wb') as f_out:
            # 写入文件头（包含文件索引信息）
            header_data = {
                'source_directory': str(self.source_dir),
                'compression_level': self.compression_level,
                'total_files': len(self.compressed_files),
                'total_original_size': total_original_size,
                'total_compressed_size': total_compressed_size,
                'file_index': file_index,
                'created_time': time.time()
            }
            
            import pickle
            header_bytes = pickle.dumps(header_data)
            header_size = len(header_bytes)
            
            # 写入头部信息（头部大小 + 头部数据）
            f_out.write(header_size.to_bytes(8, 'little'))
            f_out.write(header_bytes)
            
            # 写入所有压缩文件数据
            current_offset = f_out.tell()
            for relative_path, file_info in self.compressed_files.items():
                # 更新文件偏移量
                file_index[relative_path]['offset'] = current_offset
                
                # 写入文件数据
                f_out.write(file_info['compressed_data'])
                current_offset += len(file_info['compressed_data'])
            
            # 重新写入更新后的文件索引
            f_out.seek(8)  # 跳过头部长度的位置
            updated_header_bytes = pickle.dumps(header_data)
            f_out.write(updated_header_bytes)
        
        # 计算压缩率
        compression_ratio = (total_compressed_size / total_original_size) * 100
        
        print(f"\n压缩完成!")
        print(f"原始大小: {total_original_size / 1024 / 1024:.2f} MB")
        print(f"压缩后大小: {total_compressed_size / 1024 / 1024:.2f} MB")
        print(f"压缩率: {compression_ratio:.2f}%")
        print(f"节省空间: {100 - compression_ratio:.2f}%")
        print(f"输出文件: {self.output_file}")
    
    def run(self):
        """运行完整的压缩流程"""
        try:
            self.scan_directory()
            self.compress_directory()
            self.create_archive()
            return True
        except Exception as e:
            print(f"压缩过程出错: {e}")
            return False

def main():
    parser = argparse.ArgumentParser(description='多线程目录压缩工具')
    parser.add_argument('source', help='源目录路径')
    parser.add_argument('-o', '--output', default='temp/test.lz4', 
                       help='输出文件路径 (默认: temp/test.lz4)')
    parser.add_argument('-l', '--level', type=int, default=1, choices=range(1, 13),
                       help='压缩级别 (1-12, 默认: 1)')
    parser.add_argument('-t', '--threads', type=int, default=4,
                       help='线程数量 (默认: 4)')
    
    args = parser.parse_args()
    
    # 创建压缩器并运行
    compressor = DirectoryCompressor(
        source_dir=args.source,
        output_file=args.output,
        compression_level=args.level,
        num_threads=args.threads
    )
    
    success = compressor.run()
    
    if success:
        print("\n目录压缩成功完成!")
    else:
        print("\n目录压缩失败!")
        return 1
    
    return 0

if __name__ == "__main__":
    # 如果没有安装lz4，提示安装
    try:
        import lz4.frame
    except ImportError:
        print("请先安装lz4: pip install lz4")
        exit(1)
    
    # 直接运行示例（不使用命令行参数）
    if len(os.sys.argv) == 1:
        print("使用示例配置压缩 D:\\AI 目录...")
        
        compressor = DirectoryCompressor(
            source_dir=r"D:\AI",
            output_file="temp/test.lz4",
            compression_level=6,  # 中等压缩级别
            num_threads=8         # 8个线程
        )
        
        success = compressor.run()
        
        if success:
            print("\n目录压缩成功完成!")
        else:
            print("\n目录压缩失败!")
    else:
        # 使用命令行参数
        exit(main())