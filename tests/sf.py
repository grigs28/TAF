import os
import time
import sys
from datetime import datetime

class FileScanner:
    def __init__(self, search_directory, summary_interval=1000):
        self.search_directory = os.path.normpath(search_directory)
        self.scan_results = []
        self.start_time = None
        self.file_count = 0
        self.dir_count = 0
        self.total_size = 0
        self.last_update_time = 0
        self.update_interval = 1.0  # 每秒更新一次进度
        self.summary_interval = summary_interval  # 每多少个文件小结一次
        self.last_summary_count = 0  # 上次小结时的文件数量
        self.last_summary_size = 0  # 上次小结时的大小
        self.summary_lines = 8  # 小结信息占用的行数
        
    def clear_line(self):
        """清除当前行"""
        sys.stdout.write('\r' + ' ' * 100 + '\r')
        sys.stdout.flush()
        
    def move_cursor_up(self, lines=1):
        """将光标向上移动指定行数"""
        sys.stdout.write(f"\033[{lines}A")
        sys.stdout.flush()
        
    def move_cursor_down(self, lines=1):
        """将光标向下移动指定行数"""
        sys.stdout.write(f"\033[{lines}B")
        sys.stdout.flush()
        
    def save_cursor_position(self):
        """保存光标位置"""
        sys.stdout.write("\033[s")
        sys.stdout.flush()
        
    def restore_cursor_position(self):
        """恢复光标位置"""
        sys.stdout.write("\033[u")
        sys.stdout.flush()
        
    def clear_from_cursor_to_end(self):
        """从光标位置清除到屏幕末尾"""
        sys.stdout.write("\033[J")
        sys.stdout.flush()
        
    def print_progress(self, force=False):
        """打印进度信息"""
        current_time = time.time()
        
        # 控制更新频率，避免过于频繁的屏幕刷新
        if not force and current_time - self.last_update_time < self.update_interval:
            return
            
        self.last_update_time = current_time
        elapsed_time = current_time - self.start_time
        
        # 计算处理速度
        if elapsed_time > 0:
            files_per_sec = self.file_count / elapsed_time
        else:
            files_per_sec = 0
            
        # 计算预计剩余时间（如果速度稳定）
        if files_per_sec > 0:
            estimated_total_files = self.file_count + files_per_sec * 10  # 简单估算
            remaining_time = (estimated_total_files - self.file_count) / files_per_sec
            remaining_str = f"预计剩余: {remaining_time:.0f}秒"
        else:
            remaining_str = "预计剩余: 计算中..."
        
        # 格式化输出
        size_gb = self.total_size / (1024**3)
        
        self.clear_line()
        sys.stdout.write(
            f"\r扫描进度: {self.file_count} 文件, {self.dir_count} 目录 | "
            f"大小: {size_gb:.2f} GB | "
            f"速度: {files_per_sec:.1f} 文件/秒 | "
            f"{remaining_str}"
        )
        sys.stdout.flush()
        
    def print_file_info(self, file_info):
        """打印单个文件信息"""
        self.clear_line()
        print(f"\r发现文件: {file_info['name']} | "
              f"大小: {file_info['size_mb']:.2f} MB | "
              f"路径: {file_info['path'][:80]}...")
        
    def print_summary(self):
        """打印阶段性小结 - 固定在底部"""
        elapsed_time = time.time() - self.start_time
        files_since_last = self.file_count - self.last_summary_count
        
        if elapsed_time > 0:
            speed_since_last = files_since_last / elapsed_time
        else:
            speed_since_last = 0
            
        size_gb = self.total_size / (1024**3)
        size_since_last = (self.total_size - self.last_summary_size) / (1024**3)
        
        # 打印小结信息
        print(f"\n{'='*60}")
        print(f"阶段性小结 (每 {self.summary_interval} 个文件)")
        print(f"{'='*60}")
        print(f"已扫描文件: {self.file_count} 个")
        print(f"已扫描目录: {self.dir_count} 个")
        print(f"总大小: {size_gb:.2f} GB (本阶段增加: {size_since_last:.2f} GB)")
        print(f"扫描用时: {elapsed_time:.1f} 秒")
        print(f"平均速度: {speed_since_last:.1f} 文件/秒")
        print(f"{'='*60}")
        
        # 更新上次小结的计数和大小
        self.last_summary_count = self.file_count
        self.last_summary_size = self.total_size
        
    def scan_directory(self):
        """
        主扫描函数 - 使用栈实现非递归DFS遍历
        """
        print("=" * 100)
        print(f"开始扫描目录: {self.search_directory}")
        print(f"开始时间: {datetime.now().strftime('%Y-%m-%d %H:%M:%S')}")
        print(f"小结间隔: 每 {self.summary_interval} 个文件")
        print("=" * 100)
        
        self.start_time = time.time()
        self.last_update_time = self.start_time
        self.last_summary_count = 0
        self.last_summary_size = 0
        
        # 使用栈代替递归，避免栈溢出
        dir_stack = [self.search_directory]
        
        try:
            while dir_stack:
                current_dir = dir_stack.pop()
                self.dir_count += 1
                
                # 显示当前扫描的目录（每100个目录显示一次）
                if self.dir_count % 100 == 0:
                    self.clear_line()
                    print(f"\r正在扫描目录 #{self.dir_count}: {current_dir[:80]}...")
                
                try:
                    # 使用os.scandir()提高性能
                    with os.scandir(current_dir) as entries:
                        for entry in entries:
                            try:
                                if entry.is_file():
                                    # 获取文件信息
                                    stat = entry.stat()
                                    file_info = {
                                        'name': entry.name,
                                        'path': entry.path,
                                        'size': stat.st_size,
                                        'size_mb': stat.st_size / (1024 * 1024),
                                        'modified_time': datetime.fromtimestamp(stat.st_mtime).strftime('%Y-%m-%d %H:%M:%S'),
                                        'created_time': datetime.fromtimestamp(stat.st_ctime).strftime('%Y-%m-%d %H:%M:%S'),
                                        'extension': os.path.splitext(entry.name)[1].lower()
                                    }
                                    
                                    # 添加到结果集
                                    self.scan_results.append(file_info)
                                    self.total_size += stat.st_size
                                    self.file_count += 1
                                    
                                    # 显示重要文件（大于10MB或特定类型）
                                    if (file_info['size_mb'] > 10 or 
                                        file_info['extension'] in ['.exe', '.dll', '.zip', '.rar', '.7z']):
                                        self.print_file_info(file_info)
                                    
                                    # 每扫描指定数量的文件，进行一次小结
                                    if self.file_count % self.summary_interval == 0:
                                        self.print_summary()
                                    
                                    # 更新进度显示
                                    self.print_progress()
                                
                                elif entry.is_dir():
                                    # 将子目录加入栈
                                    dir_stack.append(entry.path)
                                    
                            except (OSError, PermissionError) as e:
                                # 跳过无法访问的文件/目录
                                continue
                                
                except (OSError, PermissionError) as e:
                    # 跳过无法访问的目录
                    self.clear_line()
                    print(f"\r无法访问目录: {current_dir} | 错误: {e}")
                    continue
                    
        except KeyboardInterrupt:
            self.clear_line()
            print(f"\n\n扫描被用户中断!")
            print(f"已扫描: {self.file_count} 个文件, {self.dir_count} 个目录")
        
        # 最终进度显示
        self.print_progress(force=True)
        print("\n")  # 换行
        
        return self.scan_results
    
    def generate_summary(self):
        """生成扫描摘要"""
        if not self.scan_results:
            return "没有扫描到任何文件"
            
        elapsed_time = time.time() - self.start_time
        
        # 文件类型统计
        file_types = {}
        for file_info in self.scan_results:
            ext = file_info['extension'] or '无扩展名'
            file_types[ext] = file_types.get(ext, 0) + 1
        
        # 找出前10种最常见的文件类型
        sorted_types = sorted(file_types.items(), key=lambda x: x[1], reverse=True)[:10]
        
        # 找出最大的10个文件
        largest_files = sorted(self.scan_results, key=lambda x: x['size'], reverse=True)[:10]
        
        summary = f"""
{'='*80}
扫描摘要
{'='*80}
扫描目录: {self.search_directory}
扫描时间: {datetime.now().strftime('%Y-%m-%d %H:%M:%S')}
扫描耗时: {elapsed_time:.2f} 秒 ({elapsed_time/60:.2f} 分钟)

文件统计:
  总文件数: {self.file_count}
  总目录数: {self.dir_count}
  总大小: {self.total_size / (1024**3):.2f} GB
  平均文件大小: {self.total_size / max(1, self.file_count) / 1024:.2f} KB

前10种文件类型:"""
        
        for ext, count in sorted_types:
            percentage = (count / self.file_count) * 100
            summary += f"\n  {ext:15} : {count:8} 个文件 ({percentage:.1f}%)"
        
        summary += f"\n\n前10个最大文件:"
        for i, file_info in enumerate(largest_files, 1):
            summary += f"\n  {i:2}. {file_info['name'][:30]:30} | {file_info['size_mb']:8.2f} MB | {file_info['path'][:50]}"
        
        summary += f"\n{'='*80}"
        
        return summary
    
    def save_results(self, filename=None):
        """保存扫描结果到文件"""
        if not self.scan_results:
            print("没有扫描结果可保存")
            return None
            
        if filename is None:
            timestamp = datetime.now().strftime("%Y%m%d_%H%M%S")
            filename = f"file_scan_results_{timestamp}.txt"
        
        try:
            with open(filename, 'w', encoding='utf-8') as f:
                # 写入摘要
                f.write(self.generate_summary())
                f.write("\n\n详细文件列表:\n")
                f.write("="*100 + "\n")
                
                # 写入文件详情
                for file_info in self.scan_results:
                    f.write(f"{file_info['path']} | {file_info['size_mb']:.2f} MB | {file_info['modified_time']}\n")
            
            print(f"扫描结果已保存到: {filename}")
            return filename
            
        except Exception as e:
            print(f"保存结果失败: {e}")
            return None

def main():
    """主函数"""
    # 解析命令行参数
    search_directory = r"D:"
    summary_interval = 1000  # 默认每1000个文件小结一次
    
    if len(sys.argv) > 1:
        search_directory = sys.argv[1]
        print(f"使用命令行参数指定的目录: {search_directory}")
        
        # 检查是否有第二个参数（小结间隔）
        if len(sys.argv) > 2:
            try:
                summary_interval = int(sys.argv[2])
                print(f"使用指定的小结间隔: 每 {summary_interval} 个文件小结一次")
            except ValueError:
                print(f"错误: 第二个参数 '{sys.argv[2]}' 不是有效的数字，使用默认间隔: 1000")
    else:
        print(f"未指定目录，使用默认目录: {search_directory}")
    
    # 检查目录是否存在
    if not os.path.exists(search_directory):
        print(f"错误: 目录不存在 - {search_directory}")
        
        # 尝试使用当前目录
        current_dir = os.getcwd()
        use_current = input(f"是否使用当前目录 '{current_dir}' 进行测试? (y/n): ")
        if use_current.lower() == 'y':
            search_directory = current_dir
        else:
            print("程序退出")
            return
    
    # 创建扫描器
    scanner = FileScanner(search_directory, summary_interval)
    
    # 开始扫描
    try:
        results = scanner.scan_directory()
        
        # 显示摘要
        print(scanner.generate_summary())
        
        # 询问是否保存结果
        save = input("\n是否保存详细结果到文件? (y/n): ")
        if save.lower() == 'y':
            scanner.save_results()
            
    except Exception as e:
        print(f"扫描过程中发生错误: {e}")
        
    finally:
        print("\n程序执行完成")

if __name__ == "__main__":
    main()