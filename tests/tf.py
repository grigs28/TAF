import subprocess
import os
import time
import ctypes
from ctypes import wintypes
import pandas as pd
from tabulate import tabulate
import win32serviceutil
import win32service
import tempfile

class EverythingPerformanceTester:
    def __init__(self, search_directory):
        self.search_directory = os.path.normpath(search_directory)
        self.results = []
        self.everything_path = r"E:\APP\Everything\Everything.exe"
        
    def check_everything_service(self):
        """检查Everything服务状态"""
        try:
            service_name = "Everything"
            status = win32serviceutil.QueryServiceStatus(service_name)
            state = status[1]
            
            states = {
                win32service.SERVICE_STOPPED: "已停止",
                win32service.SERVICE_START_PENDING: "启动中",
                win32service.SERVICE_STOP_PENDING: "停止中",
                win32service.SERVICE_RUNNING: "运行中",
                win32service.SERVICE_CONTINUE_PENDING: "继续中",
                win32service.SERVICE_PAUSE_PENDING: "暂停中",
                win32service.SERVICE_PAUSED: "已暂停"
            }
            
            return states.get(state, "未知状态")
        except Exception as e:
            return f"检查服务状态失败: {str(e)}"
    
    def method1_cli_search(self):
        """方法一：Everything命令行工具"""
        if not os.path.exists(self.everything_path):
            return None, "Everything命令行工具未找到"
            
        try:
            start_time = time.time()
            # 使用-s参数进行搜索，-nocase不区分大小写
            cmd = f'"{self.everything_path}" -s "{self.search_directory}\\*"'
            result = subprocess.run(cmd, capture_output=True, text=True, shell=True)
            end_time = time.time()
            
            if result.returncode == 0:
                files = [f.strip() for f in result.stdout.strip().split('\n') if f.strip()]
                return end_time - start_time, len(files)
            else:
                return None, f"命令行搜索失败: {result.stderr}"
                
        except Exception as e:
            return None, f"命令行搜索异常: {str(e)}"
    
    def method2_service_search(self):
        """方法二：Everything服务直接搜索"""
        try:
            # 检查Everything是否在运行
            everything_hwnd = ctypes.windll.user32.FindWindowW(None, "Everything")
            if not everything_hwnd:
                return None, "Everything窗口未找到，请确保Everything正在运行"
            
            # 使用导出功能通过临时文件获取结果
            temp_dir = tempfile.gettempdir()
            result_file = os.path.join(temp_dir, f"everything_results_{os.getpid()}.txt")
            
            if not os.path.exists(self.everything_path):
                return None, "Everything命令行工具未找到"
            
            search_path = self.search_directory + "\\*"
            
            start_time = time.time()
            # 使用-export参数导出结果到文件
            cmd = f'"{self.everything_path}" -export "{result_file}" "{search_path}"'
            result = subprocess.run(cmd, capture_output=True, text=True, shell=True)
            
            if result.returncode == 0 and os.path.exists(result_file):
                with open(result_file, 'r', encoding='utf-8') as f:
                    files = [line.strip() for line in f.readlines() if line.strip()]
                os.remove(result_file)  # 清理临时文件
                end_time = time.time()
                return end_time - start_time, len(files)
            else:
                if os.path.exists(result_file):
                    os.remove(result_file)
                return None, f"服务搜索失败: {result.stderr}"
                
        except Exception as e:
            # 清理临时文件
            try:
                if 'result_file' in locals() and os.path.exists(result_file):
                    os.remove(result_file)
            except:
                pass
            return None, f"服务搜索异常: {str(e)}"
    
    def method3_enhanced_cli_search(self):
        """方法三：增强型命令行搜索"""
        if not os.path.exists(self.everything_path):
            return None, "Everything命令行工具未找到"
            
        try:
            search_path = self.search_directory + "\\*"
            # 使用更多优化参数
            cmd = f'"{self.everything_path}" -s -sort-path -name "{search_path}"'
            
            start_time = time.time()
            result = subprocess.run(cmd, capture_output=True, text=True, shell=True)
            end_time = time.time()
            
            if result.returncode == 0:
                files = [line.strip() for line in result.stdout.split('\n') if line.strip()]
                return end_time - start_time, len(files)
            else:
                return None, f"增强命令行搜索失败: {result.stderr}"
                
        except Exception as e:
            return None, f"增强命令行搜索异常: {str(e)}"
    
    def method4_windows_search(self):
        """方法四：Windows原生搜索作为对比"""
        try:
            start_time = time.time()
            # 使用Windows的dir命令进行搜索作为基准对比
            cmd = f'dir "{self.search_directory}" /s /b'
            result = subprocess.run(cmd, capture_output=True, text=True, shell=True)
            end_time = time.time()
            
            if result.returncode == 0:
                files = [f.strip() for f in result.stdout.strip().split('\n') if f.strip()]
                return end_time - start_time, len(files)
            else:
                return None, f"Windows搜索失败: {result.stderr}"
                
        except Exception as e:
            return None, f"Windows搜索异常: {str(e)}"
    
    def run_performance_test(self, iterations=3):
        """运行性能测试"""
        print(f"开始性能测试，目录: {self.search_directory}")
        print(f"每种方法运行 {iterations} 次取平均值")
        print("=" * 80)
        
        # 检查Everything服务状态
        service_status = self.check_everything_service()
        print(f"Everything服务状态: {service_status}")
        
        methods = [
            ("Everything命令行工具", self.method1_cli_search),
            ("Everything服务搜索", self.method2_service_search),
            ("增强型命令行搜索", self.method3_enhanced_cli_search),
            ("Windows原生搜索(对比)", self.method4_windows_search)
        ]
        
        test_results = []
        
        for method_name, method_func in methods:
            print(f"\n测试方法: {method_name}")
            print("-" * 40)
            
            times = []
            file_counts = []
            errors = []
            
            for i in range(iterations):
                print(f"  第 {i+1} 次执行...", end=" ")
                try:
                    duration, result = method_func()
                    
                    if duration is not None:
                        times.append(duration)
                        file_counts.append(result)
                        print(f"完成 - 耗时: {duration:.4f}秒, 找到文件: {result}个")
                    else:
                        errors.append(result)
                        print(f"失败 - 错误: {result}")
                        # 如果第一次就失败，不再继续尝试
                        if i == 0:
                            break
                except Exception as e:
                    errors.append(str(e))
                    print(f"异常 - 错误: {str(e)}")
                    if i == 0:
                        break
            
            if times:
                avg_time = sum(times) / len(times)
                avg_files = sum(file_counts) / len(file_counts)
                min_time = min(times)
                max_time = max(times)
                status = "成功"
            else:
                avg_time = avg_files = min_time = max_time = 0
                status = f"失败: {errors[0] if errors else '未知错误'}"
            
            test_results.append({
                '方法名称': method_name,
                '状态': status,
                '平均耗时(秒)': f"{avg_time:.4f}" if avg_time > 0 else "N/A",
                '最短耗时(秒)': f"{min_time:.4f}" if min_time > 0 else "N/A",
                '最长耗时(秒)': f"{max_time:.4f}" if max_time > 0 else "N/A",
                '平均文件数': int(avg_files) if avg_files > 0 else "N/A"
            })
        
        return test_results
    
    def display_results(self, test_results):
        """显示测试结果"""
        print("\n" + "=" * 80)
        print("性能测试结果汇总")
        print("=" * 80)
        
        # 创建DataFrame以便更好的显示
        df = pd.DataFrame(test_results)
        
        # 使用tabulate创建漂亮的表格
        table = tabulate(df, headers='keys', tablefmt='grid', showindex=False)
        print(table)
        
        # 找出最快的成功方法
        successful_methods = [r for r in test_results if r['状态'] == '成功' and r['平均耗时(秒)'] != 'N/A']
        if successful_methods:
            # 排除Windows搜索进行排名
            everything_methods = [m for m in successful_methods if 'Windows' not in m['方法名称']]
            if everything_methods:
                fastest = min(everything_methods, key=lambda x: float(x['平均耗时(秒)']))
                print(f"\n🏆 最快的Everything方法: {fastest['方法名称']} (平均 {fastest['平均耗时(秒)']} 秒)")
            
            # 显示Windows搜索的性能对比
            windows_method = next((m for m in successful_methods if 'Windows' in m['方法名称']), None)
            if windows_method:
                print(f"📊 Windows搜索对比: {windows_method['平均耗时(秒)']} 秒")
        
        # 计算性能提升倍数
        everything_success = [m for m in successful_methods if 'Everything' in m['方法名称'] and 'Windows' not in m['方法名称']]
        windows_success = next((m for m in successful_methods if 'Windows' in m['方法名称']), None)
        
        if everything_success and windows_success:
            fastest_everything = min(everything_success, key=lambda x: float(x['平均耗时(秒)']))
            everything_time = float(fastest_everything['平均耗时(秒)'])
            windows_time = float(windows_success['平均耗时(秒)'])
            
            if everything_time > 0 and windows_time > 0:
                speedup = windows_time / everything_time
                print(f"🚀 Everything比Windows搜索快 {speedup:.1f} 倍")
        
        # 建议
        print("\n📋 使用建议:")
        for result in test_results:
            if result['状态'] == '成功':
                if '命令行' in result['方法名称'] and '增强' not in result['方法名称']:
                    print(f"  • {result['方法名称']}: 适合简单搜索，响应快速")
                elif '增强' in result['方法名称']:
                    print(f"  • {result['方法名称']}: 适合需要排序和高级选项的场景")
                elif '服务' in result['方法名称']:
                    print(f"  • {result['方法名称']}: 适合批量操作和导出结果")
                elif 'Windows' in result['方法名称']:
                    print(f"  • {result['方法名称']}: 作为性能基准参考")

def main():
    # 要搜索的目录
    search_directory = r"D:\备份\天正协同备份\tbmdata\data\ftpdata"
    
    # 检查目录是否存在
    if not os.path.exists(search_directory):
        print(f"错误: 目录不存在 - {search_directory}")
        
        # 提供备选目录
        alternatives = [
            r"C:\Windows\System32",
            r"C:\Program Files",
            os.path.expanduser("~")  # 用户主目录
        ]
        
        for alt in alternatives:
            if os.path.exists(alt):
                use_alt = input(f"是否使用备选目录 '{alt}' 进行测试? (y/n): ")
                if use_alt.lower() == 'y':
                    search_directory = alt
                    break
        else:
            print("未找到合适的测试目录，程序退出")
            return
    
    # 创建测试器并运行测试
    tester = EverythingPerformanceTester(search_directory)
    
    # 运行3次测试取平均值
    test_results = tester.run_performance_test(iterations=3)
    
    # 显示结果
    tester.display_results(test_results)
    
    # 保存详细结果到文件
    timestamp = time.strftime("%Y%m%d_%H%M%S")
    output_file = f"everything_performance_test_{timestamp}.txt"
    
    with open(output_file, 'w', encoding='utf-8') as f:
        f.write("Everything搜索性能测试报告\n")
        f.write("=" * 50 + "\n")
        f.write(f"测试目录: {search_directory}\n")
        f.write(f"测试时间: {time.strftime('%Y-%m-%d %H:%M:%S')}\n\n")
        
        for result in test_results:
            f.write(f"方法: {result['方法名称']}\n")
            f.write(f"状态: {result['状态']}\n")
            f.write(f"平均耗时: {result['平均耗时(秒)']} 秒\n")
            f.write(f"文件数量: {result['平均文件数']}\n")
            f.write("-" * 30 + "\n")
    
    print(f"\n详细报告已保存至: {output_file}")

# 简化版本，不需要安装额外依赖
class SimpleEverythingTester:
    """简化版的Everything测试器，不需要pandas和tabulate"""
    
    def __init__(self, search_directory):
        self.search_directory = os.path.normpath(search_directory)
        self.everything_path = r"E:\APP\Everything\Everything.exe"
    
    def test_all_methods(self):
        """测试所有方法"""
        print(f"测试目录: {self.search_directory}")
        print("=" * 60)
        
        # 创建测试器实例
        tester = EverythingPerformanceTester(self.search_directory)
        
        methods = [
            ("命令行工具", tester.method1_cli_search),
            ("服务搜索", tester.method2_service_search),
            ("增强命令行", tester.method3_enhanced_cli_search),
            ("Windows搜索", tester.method4_windows_search)
        ]
        
        results = []
        
        for name, method in methods:
            print(f"\n测试 {name}...")
            duration, file_count = method()
            
            if duration is not None:
                status = "成功"
                time_str = f"{duration:.4f}秒"
                files_str = f"{file_count}个文件"
            else:
                status = "失败"
                time_str = "N/A"
                files_str = file_count  # 错误信息
            
            results.append((name, status, time_str, files_str))
            print(f"  {name}: {status} - {time_str} - {files_str}")
        
        # 显示汇总结果
        print("\n" + "=" * 60)
        print("汇总结果:")
        print("-" * 60)
        for name, status, time_str, files_str in results:
            print(f"{name:12} | {status:5} | {time_str:10} | {files_str}")

if __name__ == "__main__":
    # 检查是否安装了pandas和tabulate
    try:
        import pandas
        import tabulate
        main()
    except ImportError:
        print("检测到缺少pandas或tabulate库，使用简化版测试器")
        print("要安装完整依赖: pip install pandas tabulate pywin32")
        print()
        
        # 使用简化版测试器
        search_directory = r"D:\备份\天正协同备份\tbmdata\data\ftpdata"
        if not os.path.exists(search_directory):
            search_directory = r"C:\Windows\System32"  # 备选目录
            
        tester = SimpleEverythingTester(search_directory)
        tester.test_all_methods()