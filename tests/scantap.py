import subprocess
import os
import json
import re
import struct
import time
from pathlib import Path
from datetime import datetime

class TapeMediaInfo:
    def __init__(self, itdt_path=r"D:\APP\TAF\ITDT\itdt.exe"):
        self.tape_drive = r'\\.\tape0'  # 默认使用 tape0
        self.media_info = {}
        self.itdt_path = itdt_path
        
        # 检查ITDT工具是否存在
        if not os.path.exists(self.itdt_path):
            raise FileNotFoundError(f"ITDT工具未找到: {self.itdt_path}")
        print(f"使用ITDT工具: {self.itdt_path}")
    
    def run_itdt_command(self, args, timeout=30):
        """运行ITDT命令并输出完整命令行"""
        cmd = [self.itdt_path] + args
        cmd_str = ' '.join(cmd)
        print(f"执行命令: {cmd_str}")
        
        try:
            # 使用二进制模式读取，避免编码问题
            result = subprocess.run(cmd, capture_output=True, timeout=timeout)
            
            # 尝试多种编码方式解码输出
            stdout = ""
            stderr = ""
            
            # 尝试UTF-8解码
            try:
                stdout = result.stdout.decode('utf-8', errors='replace')
                stderr = result.stderr.decode('utf-8', errors='replace')
            except:
                # 如果UTF-8失败，尝试GBK
                try:
                    stdout = result.stdout.decode('gbk', errors='replace')
                    stderr = result.stderr.decode('gbk', errors='replace')
                except:
                    # 如果都失败，使用原始字节
                    stdout = str(result.stdout)
                    stderr = str(result.stderr)
            
            if result.returncode != 0:
                print(f"命令执行失败，返回码: {result.returncode}")
                if stderr:
                    print(f"错误信息: {stderr}")
            
            # 创建一个简单的结果对象
            class Result:
                def __init__(self, returncode, stdout, stderr):
                    self.returncode = returncode
                    self.stdout = stdout
                    self.stderr = stderr
            
            return Result(result.returncode, stdout, stderr)
            
        except subprocess.TimeoutExpired:
            print(f"命令执行超时: {cmd_str}")
            return None
        except Exception as e:
            print(f"命令执行异常: {e}")
            return None
    
    def scan_devices(self):
        """扫描可用的磁带设备，但保持默认使用 tape0"""
        print("\n" + "="*50)
        print("扫描磁带设备...")
        try:
            result = self.run_itdt_command(['scan'], timeout=30)
            if result and result.returncode == 0:
                lines = result.stdout.split('\n')
                found_devices = []
                for line in lines:
                    # 查找所有可能的设备
                    if any(keyword in line for keyword in ['tape', 'scsi', 'ULT3580']):
                        print(f"找到设备: {line.strip()}")
                        found_devices.append(line.strip())
                
                if found_devices:
                    print(f"共找到 {len(found_devices)} 个设备")
                    print(f"使用默认设备: {self.tape_drive}")
                else:
                    print("未找到其他设备，使用默认设备")
                    
            else:
                print("扫描设备失败，使用默认设备")
                
        except Exception as e:
            print(f"扫描设备错误: {e}")
            print("使用默认设备")
    
    def test_device(self):
        """测试默认设备是否可用"""
        print(f"\n测试设备 {self.tape_drive} 是否可用...")
        try:
            result = self.run_itdt_command(['-f', self.tape_drive, 'tur'], timeout=15)
            if result and result.returncode == 0:
                print("设备测试通过")
                return True
            else:
                print("设备测试失败")
                return False
        except Exception as e:
            print(f"设备测试异常: {e}")
            return False
    
    def load_tape(self):
        """加载磁带"""
        try:
            result = self.run_itdt_command(['-f', self.tape_drive, 'load'], timeout=60)
            return result and result.returncode == 0
        except Exception as e:
            print(f"加载磁带失败: {e}")
            return False
    
    def unload_tape(self):
        """卸载磁带"""
        try:
            result = self.run_itdt_command(['-f', self.tape_drive, 'unload'], timeout=60)
            return result and result.returncode == 0
        except Exception as e:
            print(f"卸载磁带失败: {e}")
            return False
    
    def set_volume_label(self, new_label):
        """设置磁带卷标"""
        try:
            # 使用setparm命令设置卷标
            result = self.run_itdt_command(['-f', self.tape_drive, 'setparm', 'volid', new_label], timeout=30)
            if result and result.returncode == 0:
                print(f"卷标已成功修改为: {new_label}")
                return True
            else:
                print("卷标修改失败")
                return False
        except Exception as e:
            print(f"设置卷标时发生错误: {e}")
            return False
    
    def read_mam_attribute_safe(self, attribute_id, attribute_name):
        """安全读取MAM属性，避免长时间等待"""
        temp_file = f"temp_{attribute_id}.bin"
        try:
            cmd_args = ['-f', self.tape_drive, 'readattr', '-p0', 
                       f'-a{attribute_id}', '-d', temp_file]
            result = self.run_itdt_command(cmd_args, timeout=15)  # 缩短超时时间
            
            if result and result.returncode == 0 and os.path.exists(temp_file):
                # 以二进制方式读取文件
                with open(temp_file, 'rb') as f:
                    binary_data = f.read()
                
                os.remove(temp_file)
                
                # 转换为十六进制表示
                hex_data = binary_data.hex()
                if hex_data:
                    # 简化显示，只显示关键部分
                    if len(hex_data) > 24:
                        return f"HEX: {hex_data[:24]}..."
                    else:
                        return f"HEX: {hex_data}"
                
                return None
            else:
                if os.path.exists(temp_file):
                    os.remove(temp_file)
                return None
        except Exception as e:
            print(f"读取属性 {attribute_name} 失败: {e}")
            if os.path.exists(temp_file):
                os.remove(temp_file)
            return None
    
    def get_tape_position(self):
        """获取磁带位置信息"""
        try:
            result = self.run_itdt_command(['-f', self.tape_drive, 'qrypos'], timeout=15)
            return result.stdout if result and result.returncode == 0 else None
        except Exception as e:
            print(f"获取磁带位置失败: {e}")
            return None
    
    def get_device_info(self):
        """获取设备信息"""
        try:
            result = self.run_itdt_command(['-f', self.tape_drive, 'devinfo'], timeout=15)
            if result and result.returncode == 0:
                # 提取关键设备信息
                lines = result.stdout.split('\n')
                info_lines = []
                for line in lines[:10]:  # 只取前10行关键信息
                    clean_line = line.strip()
                    if clean_line and ('=' in clean_line or 'SCSI' in clean_line or 'ULT3580' in clean_line):
                        info_lines.append(clean_line)
                return ' | '.join(info_lines) if info_lines else result.stdout[:200]
            return None
        except Exception as e:
            print(f"获取设备信息失败: {e}")
            return None
    
    def get_drive_serial(self):
        """获取磁带机序列号"""
        try:
            result = self.run_itdt_command(['-f', self.tape_drive, 'inquiry'], timeout=15)
            if result and result.returncode == 0:
                # 从inquiry输出中提取序列号
                lines = result.stdout.split('\n')
                for line in lines:
                    if 'serial' in line.lower() or '序列' in line.lower() or 'S/N' in line:
                        return line.strip()
                # 返回第一行有意义的信息
                for line in lines:
                    clean_line = line.strip()
                    if clean_line and len(clean_line) > 10:
                        return clean_line[:100]
            return None
        except Exception as e:
            print(f"获取驱动器序列号失败: {e}")
            return None
    
    def parse_hex_to_readable(self, hex_data):
        """将十六进制数据转换为可读信息"""
        if not hex_data or not hex_data.startswith("HEX: "):
            return hex_data
        
        hex_str = hex_data[5:]  # 去掉"HEX: "前缀
        
        # 尝试解析磁带序列号
        if len(hex_str) >= 24:
            # 假设序列号在数据的特定位置
            # 这里可以根据实际情况调整解析逻辑
            serial_part = hex_str[-12:]  # 取最后12个字符作为可能的序列号
            try:
                # 尝试将十六进制转换为ASCII
                serial_ascii = bytes.fromhex(serial_part).decode('ascii', errors='ignore')
                if serial_ascii and any(c.isalnum() for c in serial_ascii):
                    return f"序列号: {serial_ascii.strip()} (原始: {hex_data})"
            except:
                pass
        
        return hex_data
    
    def collect_media_info(self):
        """收集所有磁带介质信息"""
        print("\n" + "="*50)
        print("收集磁带介质信息...")
        
        # 先测试设备是否就绪
        if not self.test_device():
            print("警告: 设备未就绪，可能无法读取MAM属性")
        
        # 基本信息分类
        self.media_info = {
            '驱动器信息': {},
            '磁带基本信息': {},
            '状态信息': {}
        }
        
        # 获取驱动器信息
        print("\n获取驱动器信息...")
        drive_serial = self.get_drive_serial()
        if drive_serial:
            self.media_info['驱动器信息']['驱动器序列号'] = drive_serial
        
        device_info = self.get_device_info()
        if device_info:
            self.media_info['驱动器信息']['设备信息'] = device_info
        
        self.media_info['驱动器信息']['设备路径'] = self.tape_drive
        
        # 只读取关键的MAM属性，避免长时间等待
        print("\n读取关键MAM属性...")
        key_mam_attributes = {
            '0x0001': '磁带序列号',
            '0x0002': '制造商',
            '0x0003': '介质类型', 
        }
        
        for attr_id, attr_name in key_mam_attributes.items():
            value = self.read_mam_attribute_safe(attr_id, attr_name)
            if value:
                print(f"  {attr_name}: {value}")
                # 尝试将十六进制数据转换为可读格式
                readable_value = self.parse_hex_to_readable(value)
                self.media_info['磁带基本信息'][attr_name] = readable_value
            else:
                print(f"  {attr_name}: 读取失败或为空")
        
        # 获取磁带位置信息
        print("\n获取磁带位置信息...")
        position = self.get_tape_position()
        if position:
            # 提取关键位置信息
            position_lines = position.split('\n')
            key_info = []
            for line in position_lines:
                if 'Partition Number' in line or 'Current Block ID' in line:
                    clean_line = line.strip()
                    if clean_line:
                        key_info.append(clean_line)
            if key_info:
                self.media_info['状态信息']['位置信息'] = ' | '.join(key_info[:3])
    
    def display_info(self):
        """分类显示信息"""
        if not self.media_info:
            print("没有可用的磁带介质信息")
            return
        
        print("\n" + "="*60)
        print("磁带介质信息分类显示")
        print("="*60)
        
        # 首先显示驱动器信息
        if '驱动器信息' in self.media_info and self.media_info['驱动器信息']:
            print(f"\n【驱动器信息】")
            print("-" * 40)
            for key, value in self.media_info['驱动器信息'].items():
                if value and str(value).strip():
                    clean_value = str(value).replace('\n', ' ').replace('\r', '').strip()
                    if len(clean_value) > 100:
                        clean_value = clean_value[:100] + "..."
                    print(f"  {key:<15}: {clean_value}")
        
        # 显示磁带基本信息
        if '磁带基本信息' in self.media_info and self.media_info['磁带基本信息']:
            print(f"\n【磁带基本信息】")
            print("-" * 40)
            for key, value in self.media_info['磁带基本信息'].items():
                if value and str(value).strip():
                    clean_value = str(value).replace('\n', ' ').replace('\r', '').strip()
                    print(f"  {key:<15}: {clean_value}")
        
        # 显示状态信息
        if '状态信息' in self.media_info and self.media_info['状态信息']:
            print(f"\n【状态信息】")
            print("-" * 40)
            for key, value in self.media_info['状态信息'].items():
                if value and str(value).strip():
                    clean_value = str(value).replace('\n', ' ').replace('\r', '').strip()
                    if len(clean_value) > 100:
                        clean_value = clean_value[:100] + "..."
                    print(f"  {key:<15}: {clean_value}")
        
        print("\n" + "="*60)
        
        # 显示唯一性标识总结 - 第一套（技术性）
        print("\n【唯一性标识总结 - 技术信息】")
        print("-" * 50)
        
        unique_identifiers = []
        
        # 驱动器唯一标识
        drive_info = self.media_info.get('驱动器信息', {})
        if '驱动器序列号' in drive_info:
            unique_identifiers.append(f"驱动器序列号: {drive_info['驱动器序列号']}")
        if '设备路径' in drive_info:
            unique_identifiers.append(f"设备路径: {drive_info['设备路径']}")
        
        # 磁带基本信息
        tape_info = self.media_info.get('磁带基本信息', {})
        for key in ['磁带序列号', '制造商', '介质类型']:
            if key in tape_info and tape_info[key]:
                value = tape_info[key]
                unique_identifiers.append(f"{key}: {value}")
        
        if unique_identifiers:
            for uid in unique_identifiers:
                print(f"  ✓ {uid}")
        else:
            print("  未找到有效的唯一性标识信息")
        
        # 显示唯一性标识总结 - 第二套（用户友好）
        print("\n【唯一性标识总结 - 用户友好描述】")
        print("-" * 50)
        
        # 构建友好的描述
        friendly_descriptions = []
        
        # 驱动器描述
        drive_desc = "IBM ULT3580-HH9 磁带驱动器"
        if '驱动器序列号' in drive_info:
            # 尝试提取序列号
            serial_match = re.search(r'S/N:([A-Z0-9]+)', drive_info['驱动器序列号'])
            if serial_match:
                drive_desc += f" (序列号: {serial_match.group(1)})"
        friendly_descriptions.append(f"驱动器: {drive_desc}")
        
        # 磁带描述
        tape_serial = tape_info.get('磁带序列号', '')
        if '序列号:' in tape_serial:
            # 提取解析后的序列号
            serial_match = re.search(r'序列号:\s*([^\s(]+)', tape_serial)
            if serial_match:
                friendly_descriptions.append(f"磁带序列号: {serial_match.group(1)}")
        elif 'HEX:' in tape_serial:
            # 使用十六进制标识
            hex_match = re.search(r'HEX:\s*([a-f0-9]+)', tape_serial.lower())
            if hex_match:
                hex_id = hex_match.group(1)[-8:]  # 使用最后8个字符作为标识
                friendly_descriptions.append(f"磁带标识: {hex_id.upper()}")
        
        # 介质类型描述
        media_type = tape_info.get('介质类型', '')
        if 'HEX:' in media_type:
            # 根据十六进制代码判断介质类型
            hex_match = re.search(r'HEX:\s*([a-f0-9]+)', media_type.lower())
            if hex_match:
                hex_code = hex_match.group(1)
                # 简单的介质类型映射（可根据实际情况扩展）
                media_map = {
                    '0007': 'LTO-7 磁带',
                    '0008': 'LTO-8 磁带', 
                    '0009': 'LTO-9 磁带',
                    # 添加更多映射...
                }
                media_desc = media_map.get(hex_code[-4:], '未知类型磁带')
                friendly_descriptions.append(f"介质类型: {media_desc}")
        
        # 位置信息
        position_info = self.media_info.get('状态信息', {}).get('位置信息', '')
        if 'Partition Number' in position_info:
            friendly_descriptions.append("磁带位置: 分区0，块0 (起始位置)")
        
        if friendly_descriptions:
            for desc in friendly_descriptions:
                print(f"  • {desc}")
        else:
            print("  无法生成用户友好描述")
        
        print("\n【使用建议】")
        print("-" * 50)
        print("  • 磁带序列号是识别特定磁带的最佳方式")
        print("  • 驱动器序列号用于识别特定的磁带驱动器")
        print("  • 结合两者可以唯一标识磁带在特定驱动器中的使用")
        print("  • 建议在磁带管理系统中记录这些标识信息")
    
    def cleanup(self):
        """清理临时文件"""
        for temp_file in Path('.').glob('temp_*.bin'):
            try:
                temp_file.unlink()
            except:
                pass

def main():
    """主函数"""
    print("磁带介质信息收集工具")
    print("="*50)
    
    # 使用固定的ITDT路径
    itdt_path = r"D:\APP\TAF\ITDT\itdt.exe"
    
    try:
        tape_info = TapeMediaInfo(itdt_path)
        
        # 扫描设备（仅用于信息显示，不影响默认设备选择）
        tape_info.scan_devices()
        
        print(f"\n使用设备: {tape_info.tape_drive}")
        
        # 加载磁带
        print("\n正在加载磁带...")
        if tape_info.load_tape():
            print("磁带加载成功")
        else:
            print("磁带加载失败，可能已加载或无磁带")
        
        # 收集信息
        tape_info.collect_media_info()
        
        # 显示信息
        tape_info.display_info()
        
        # 询问用户是否要修改卷标
        print("\n" + "="*60)
        print("卷标管理选项")
        print("="*60)
        
        change_vol = input("\n是否要修改磁带卷标？(y/n): ").strip().lower()
        if change_vol == 'y':
            new_label = input("请输入新的卷标（最多6个字符）: ").strip()
            if new_label and len(new_label) <= 6:
                confirm = input(f"确认要将卷标修改为 '{new_label}' 吗？(y/n): ").strip().lower()
                if confirm == 'y':
                    if tape_info.set_volume_label(new_label):
                        print("卷标修改成功！")
                        # 重新加载磁带以应用更改
                        print("重新加载磁带以应用更改...")
                        tape_info.unload_tape()
                        time.sleep(2)
                        tape_info.load_tape()
                    else:
                        print("卷标修改失败")
                else:
                    print("已取消卷标修改")
            else:
                print("卷标无效，应为1-6个字符")
        
    except FileNotFoundError as e:
        print(f"错误: {e}")
        print("请检查ITDT工具路径是否正确")
    except KeyboardInterrupt:
        print("\n用户中断操作")
    except Exception as e:
        print(f"程序执行错误: {e}")
    finally:
        if 'tape_info' in locals():
            tape_info.cleanup()
        print("\n程序执行完毕")

if __name__ == "__main__":
    main()