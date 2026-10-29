import subprocess
import os
import time
from datetime import datetime

class TapeManager:
    def __init__(self, itdt_path=r"D:\APP\TAF\ITDT\itdt.exe", 
                 tools_dir=r"D:\APP\TAF\ITDT"):
        self.tape_drive = r'\\.\tape0'
        self.itdt_path = itdt_path
        self.tools_dir = tools_dir
        self.drive_letter = "O:"  # 挂载后的盘符
        self.default_device = "0.0.24.0"  # 默认设备地址
        
        # LTFS 工具命令
        self.ltfs_tools = {
            'assign': 'LtfsCmdAssign.exe',
            'unassign': 'LtfsCmdUnassign.exe', 
            'load': 'LtfsCmdLoad.exe',
            'eject': 'LtfsCmdEject.exe',
            'format': 'LtfsCmdFormat.exe',
            'unformat': 'LtfsCmdUnformat.exe',
            'check': 'LtfsCmdCheck.exe',
            'rollback': 'LtfsCmdRollback.exe',
            'drives': 'LtfsCmdDrives.exe',
            'mkltfs': 'mkltfs.exe'
        }
        
        # 验证工具存在
        for tool_name, tool_file in self.ltfs_tools.items():
            tool_path = os.path.join(self.tools_dir, tool_file)
            if not os.path.exists(tool_path):
                print(f"警告: {tool_file} 未找到")
        
        if not os.path.exists(self.itdt_path):
            print(f"警告: ITDT工具未找到: {self.itdt_path}")
            
        print(f"工具目录: {self.tools_dir}")
        print(f"LTFS盘符: {self.drive_letter}")
        print(f"默认设备: {self.default_device}")
    
    def run_command(self, cmd, timeout=300, shell=False, tool_type="ITDT"):
        """运行命令并显示完整信息 - 修复编码问题"""
        if isinstance(cmd, list):
            cmd_str = ' '.join(cmd)
        else:
            cmd_str = cmd
            
        print(f"\n[{tool_type}] 执行命令: {cmd_str}")
        print(f"[{tool_type}] 工作目录: {self.tools_dir}")
        
        try:
            # 使用通用的编码方式处理输出，避免GBK解码错误
            result = subprocess.run(cmd, capture_output=True, text=False, 
                                  timeout=timeout, shell=shell, cwd=self.tools_dir)
            
            # 手动解码输出，使用错误忽略策略
            stdout = ""
            stderr = ""
            
            if result.stdout:
                try:
                    stdout = result.stdout.decode('utf-8', errors='ignore')
                except:
                    stdout = result.stdout.decode('gbk', errors='ignore')
            
            if result.stderr:
                try:
                    stderr = result.stderr.decode('utf-8', errors='ignore')
                except:
                    stderr = result.stderr.decode('gbk', errors='ignore')
            
            if result.returncode != 0:
                print(f"[{tool_type}] 命令执行失败，返回码: {result.returncode}")
                if stderr:
                    print(f"[{tool_type}] 错误信息: {stderr}")
            else:
                print(f"[{tool_type}] 命令执行成功")
                if stdout:
                    print(f"[{tool_type}] 输出: {stdout}")
            
            # 返回一个包含解码后文本的类似对象
            class Result:
                def __init__(self, returncode, stdout, stderr):
                    self.returncode = returncode
                    self.stdout = stdout
                    self.stderr = stderr
            
            return Result(result.returncode, stdout, stderr)
            
        except subprocess.TimeoutExpired:
            print(f"[{tool_type}] 命令执行超时")
            return None
        except Exception as e:
            print(f"[{tool_type}] 命令执行异常: {e}")
            return None
    
    # ===== 卷信息获取方法 =====
    def get_volume_info_fsutil(self):
        """使用fsutil获取卷信息 - 这是唯一有效的方法"""
        print(f"使用fsutil获取 {self.drive_letter} 卷信息...")
        
        # 检查驱动器是否存在
        if not os.path.exists(self.drive_letter):
            print(f"✗ 驱动器 {self.drive_letter} 不存在或未分配")
            return None
            
        cmd = f"fsutil fsinfo volumeinfo {self.drive_letter}"
        result = self.run_command(cmd, shell=True, tool_type="SYSTEM")
        
        if result and result.returncode == 0:
            # 解析输出
            lines = result.stdout.split('\n')
            volume_info = {}
            
            for line in lines:
                line = line.strip()
                if ':' in line:
                    key, value = line.split(':', 1)
                    volume_info[key.strip()] = value.strip()
            
            # 显示解析后的信息
            print(f"\n✓ 磁带卷信息解析:")
            if '卷名' in volume_info:
                print(f"  卷标: {volume_info['卷名']}")
            if '卷序列号' in volume_info:
                print(f"  序列号: {volume_info['卷序列号']}")
            if '文件系统名' in volume_info:
                print(f"  文件系统: {volume_info['文件系统名']}")
            if '组件长度最大值' in volume_info:
                print(f"  最大文件名长度: {volume_info['组件长度最大值']}")
            
            return volume_info
        else:
            print("✗ 无法获取卷信息")
            return None
    
    def get_tape_volume_info(self):
        """获取磁带卷信息 - 简化版本，只使用有效的fsutil命令"""
        print(f"\n获取磁带卷信息 ({self.drive_letter})...")
        
        volume_info = self.get_volume_info_fsutil()
        if volume_info:
            print(f"\n✓ 磁带卷信息获取成功")
            return True
        else:
            print(f"✗ 磁带卷信息获取失败")
            return False

    # ===== ITDT 命令 =====
    def load_tape_itdt(self):
        """使用ITDT加载磁带"""
        print("使用ITDT加载磁带...")
        cmd = [self.itdt_path, '-f', self.tape_drive, 'load']
        result = self.run_command(cmd, timeout=60, tool_type="ITDT")
        
        if result:
            if result.returncode == 0:
                print("✓ 磁带加载成功")
                return True
            else:
                # 检查错误信息，判断是否磁带已经加载
                if "already loaded" in result.stderr.lower() or "已经加载" in result.stderr:
                    print("⚠ 磁带可能已经处于加载状态")
                    return True
                else:
                    print("✗ 磁带加载失败")
                    return False
        return False
    
    def unload_tape_itdt(self):
        """使用ITDT卸载磁带 - 处理卸载错误"""
        print("使用ITDT卸载磁带...")
        cmd = [self.itdt_path, '-f', self.tape_drive, 'unload']
        result = self.run_command(cmd, timeout=60, tool_type="ITDT")
        
        if result:
            if result.returncode == 0:
                print("✓ 磁带卸载成功")
                return True
            else:
                # 检查错误信息，判断是否磁带已经卸载
                if "系统找不到指定的文件" in result.stderr or "not found" in result.stderr.lower():
                    print("⚠ 磁带可能已经处于卸载状态，或者设备路径不正确")
                    # 尝试检查设备状态
                    self.check_tape_status()
                    return True
                else:
                    print("✗ 磁带卸载失败")
                    return False
        return False
    
    def check_tape_status(self):
        """检查磁带设备状态"""
        print("检查磁带设备状态...")
        cmd = [self.itdt_path, '-f', self.tape_drive, 'inq']
        result = self.run_command(cmd, timeout=30, tool_type="ITDT")
        
        if result and result.returncode == 0:
            print("✓ 磁带设备响应正常")
            return True
        else:
            print("✗ 无法访问磁带设备，请检查设备连接和路径")
            return False
    
    def get_tape_info_itdt(self):
        """使用ITDT获取磁带信息"""
        print("使用ITDT获取磁带信息...")
        cmd = [self.itdt_path, '-f', self.tape_drive, 'inq']
        return self.run_command(cmd, timeout=30, tool_type="ITDT")
    
    def set_volume_label_itdt(self, label):
        """使用ITDT设置磁带卷标"""
        print(f"使用ITDT设置卷标: {label}")
        cmd = [self.itdt_path, '-f', self.tape_drive, 'setparm', 'volid', label]
        return self.run_command(cmd, timeout=30, tool_type="ITDT")
    
    def get_partition_info_itdt(self):
        """使用ITDT获取分区信息"""
        print("使用ITDT检查分区信息...")
        cmd = [self.itdt_path, '-f', self.tape_drive, 'qrypart']
        return self.run_command(cmd, timeout=30, tool_type="ITDT")
    
    def change_partition_itdt(self, partition_number):
        """使用ITDT切换分区"""
        print(f"使用ITDT切换到分区 {partition_number}")
        cmd = [self.itdt_path, '-f', self.tape_drive, 'chgpart', str(partition_number)]
        return self.run_command(cmd, timeout=60, tool_type="ITDT")
    
    def erase_tape_itdt(self, quick=True):
        """使用ITDT擦除磁带"""
        print("使用ITDT擦除磁带...")
        if quick:
            cmd = [self.itdt_path, '-f', self.tape_drive, 'erase', '-short']
        else:
            cmd = [self.itdt_path, '-f', self.tape_drive, 'erase', '-long']
        return self.run_command(cmd, timeout=1800, tool_type="ITDT")
    
    # ===== LTFS 命令 =====
    def list_drives_ltfs(self):
        """列出LTFS驱动器"""
        print("列出LTFS驱动器...")
        cmd = [self.ltfs_tools['drives']]
        return self.run_command(cmd, timeout=30, tool_type="LTFS")
    
    def assign_tape_ltfs(self, drive_id=None):
        """分配磁带给驱动器并挂载到O:盘"""
        print(f"分配磁带给驱动器并挂载到 {self.drive_letter} 盘...")
        if not drive_id:
            # 获取可用的驱动器
            result = self.list_drives_ltfs()
            if result and result.returncode == 0:
                drive_id = input("请输入驱动器ID: ").strip()
        
        if drive_id:
            # 分配并挂载到O:盘
            cmd = [self.ltfs_tools['assign'], drive_id, self.drive_letter]
            result = self.run_command(cmd, timeout=60, tool_type="LTFS")
            if result and result.returncode == 0:
                print(f"✓ 磁带已成功分配到 {self.drive_letter} 盘")
            return result
        return None
    
    def unassign_tape_ltfs(self, drive_id=None):
        """从驱动器卸载磁带"""
        print("从驱动器卸载磁带...")
        if not drive_id:
            drive_id = input("请输入驱动器ID: ").strip()
        
        if drive_id:
            cmd = [self.ltfs_tools['unassign'], drive_id]
            result = self.run_command(cmd, timeout=60, tool_type="LTFS")
            if result and result.returncode == 0:
                print(f"✓ 磁带已从 {self.drive_letter} 盘卸载")
            return result
        return None
    
    def load_tape_ltfs(self, drive_id=None):
        """加载磁带到驱动器（物理装载）"""
        print("LTFS物理加载磁带...")
        if not drive_id:
            drive_id = input("请输入驱动器ID: ").strip()
        
        if drive_id:
            cmd = [self.ltfs_tools['load'], drive_id]
            result = self.run_command(cmd, timeout=60, tool_type="LTFS")
            if result and result.returncode == 0:
                print("✓ 磁带物理加载成功")
            return result
        return None
    
    def eject_tape_ltfs(self, drive_id=None):
        """弹出磁带（物理卸载）"""
        print("LTFS物理弹出磁带...")
        if not drive_id:
            drive_id = input("请输入驱动器ID: ").strip()
        
        if drive_id:
            cmd = [self.ltfs_tools['eject'], drive_id]
            result = self.run_command(cmd, timeout=60, tool_type="LTFS")
            if result and result.returncode == 0:
                print("✓ 磁带物理弹出成功")
            return result
        return None
    
    def format_tape_ltfs(self, drive_id=None, volume_label=None, serial=None, eject_after=False):
        """格式化磁带为LTFS格式 - 使用正确的参数格式"""
        print("LTFS格式化磁带...")
        
        if not drive_id:
            # 显示可用驱动器并让用户选择
            self.list_drives_ltfs()
            drive_id = input("请输入要格式化的驱动器ID: ").strip()
        
        if not drive_id:
            print("未指定驱动器ID")
            return None
            
        # 使用驱动器ID而不是盘符
        cmd = [self.ltfs_tools['format'], drive_id]
        
        # 添加序列号参数（如果需要）
        if serial:
            cmd.append(f"/S:{serial}")
        
        # 添加卷标参数（如果需要）
        if volume_label:
            cmd.append(f"/N:{volume_label}")
        
        # 添加格式化后弹出参数（如果需要）
        if eject_after:
            cmd.append("/E")
        
        result = self.run_command(cmd, timeout=3600, tool_type="LTFS")
        if result and result.returncode == 0:
            print("✓ LTFS格式化成功")
        return result
    
    def format_tape_mkltfs(self, device_id=None, volume_label=None):
        """使用mkltfs格式化磁带"""
        print("使用mkltfs格式化磁带...")
        if not device_id:
            device_id = self.default_device
            
        cmd = [self.ltfs_tools['mkltfs'], '-d', device_id, '--force']
        if volume_label:
            cmd.extend(['--volume-name', volume_label])
        result = self.run_command(cmd, timeout=3600, tool_type="LTFS")
        if result and result.returncode == 0:
            print("✓ mkltfs格式化成功")
        return result
    
    def unformat_tape_ltfs(self, drive_id=None):
        """取消格式化（清除LTFS卷标）"""
        print("取消LTFS格式化...")
        if not drive_id:
            drive_id = input("请输入驱动器ID: ").strip()
        
        if drive_id:
            cmd = [self.ltfs_tools['unformat'], drive_id]
            result = self.run_command(cmd, timeout=300, tool_type="LTFS")
            if result and result.returncode == 0:
                print("✓ 取消格式化成功")
            return result
        return None
    
    def check_tape_ltfs(self, drive_id=None):
        """检查磁带完整性"""
        print("检查磁带完整性...")
        if not drive_id:
            drive_id = input("请输入驱动器ID: ").strip()
        
        if drive_id:
            cmd = [self.ltfs_tools['check'], drive_id]
            result = self.run_command(cmd, timeout=7200, tool_type="LTFS")  # 2小时超时
            if result and result.returncode == 0:
                print("✓ 磁带检查完成")
            return result
        return None
    
    def rollback_tape_ltfs(self, drive_id=None):
        """回滚到上一个一致性点"""
        print("回滚磁带...")
        if not drive_id:
            drive_id = input("请输入驱动器ID: ").strip()
        
        if drive_id:
            cmd = [self.ltfs_tools['rollback'], drive_id]
            result = self.run_command(cmd, timeout=300, tool_type="LTFS")
            if result and result.returncode == 0:
                print("✓ 回滚操作完成")
            return result
        return None
    
    def mount_tape_complete(self, drive_id=None, volume_label=None):
        """完整的磁带挂载流程：加载->格式化->分配"""
        print(f"\n开始完整磁带挂载流程到 {self.drive_letter} 盘...")
        
        if not drive_id:
            # 显示可用驱动器
            self.list_drives_ltfs()
            drive_id = input("请输入驱动器ID: ").strip()
        
        if not drive_id:
            print("未指定驱动器ID")
            return False
        
        # 1. 物理加载磁带
        print(f"\n步骤1: 物理加载磁带到驱动器 {drive_id}")
        if not self.load_tape_ltfs(drive_id):
            print("物理加载失败")
            return False
        
        # 2. 格式化磁带（如果需要）
        format_choice = input("是否需要格式化磁带? (y/N): ").strip().lower()
        if format_choice == 'y':
            if not volume_label:
                default_label = generate_default_label()
                custom_label = input(f"请输入卷标 (回车使用默认 '{default_label}'): ").strip()
                volume_label = custom_label if custom_label else default_label
            
            print(f"\n步骤2: 格式化磁带")
            if not self.format_tape_ltfs(drive_id, volume_label):
                print("格式化失败")
                return False
        else:
            print("跳过格式化步骤")
        
        # 3. 分配并挂载到O:盘
        print(f"\n步骤3: 分配并挂载到 {self.drive_letter} 盘")
        if self.assign_tape_ltfs(drive_id):
            print(f"✓ 完整挂载流程完成，磁带现在可以在 {self.drive_letter} 盘访问")
            return True
        else:
            print("✗ 分配失败")
            return False

def clear_screen():
    """清屏"""
    os.system('cls' if os.name == 'nt' else 'clear')

def generate_default_label():
    """生成默认卷标（基于日期）"""
    now = datetime.now()
    return f"BK{now.strftime('%y%m%d_%H%M')}"

def show_main_menu():
    """显示主菜单"""
    clear_screen()
    print("=" * 70)
    print("                   磁带管理工具 (ITDT + LTFS)")
    print("=" * 70)
    print(f"设备: \\\\.\\tape0")
    print(f"工具目录: D:\\APP\\TAF\\ITDT")
    print(f"LTFS盘符: O:")
    print("\n请选择操作类别:")
    print("1. ITDT 工具 (低级操作)")
    print("2. LTFS 工具 (高级文件系统操作)")
    print("3. 组合操作流程")
    print("4. 获取磁带卷信息 (使用fsutil)")
    print("0. 退出程序")
    print("=" * 70)

def show_itdt_menu():
    """显示ITDT工具菜单"""
    clear_screen()
    print("=" * 60)
    print("                 ITDT 工具菜单")
    print("=" * 60)
    print("1. 加载磁带")
    print("2. 卸载磁带")
    print("3. 检查磁带设备状态")
    print("4. 获取磁带信息")
    print("5. 设置卷标")
    print("6. 检查分区信息")
    print("7. 切换分区")
    print("8. 快速擦除磁带")
    print("9. 完全擦除磁带")
    print("10. 返回主菜单")
    print("=" * 60)

def show_ltfs_menu():
    """显示LTFS工具菜单"""
    clear_screen()
    print("=" * 60)
    print("                 LTFS 工具菜单")
    print("=" * 60)
    print(f"挂载盘符: O:")
    print("1. 列出驱动器")
    print("2. 分配磁带 (挂载到O:盘)")
    print("3. 卸载磁带 (从O:盘卸载)")
    print("4. 加载磁带 (物理)")
    print("5. 弹出磁带 (物理)")
    print("6. 格式化磁带 (LtfsCmdFormat) - 使用驱动器ID")
    print("7. 格式化磁带 (mkltfs) - 使用设备地址")
    print("8. 取消格式化")
    print("9. 检查磁带完整性")
    print("10. 回滚磁带")
    print("11. 完整挂载流程 (加载->格式化->挂载O:盘)")
    print("12. 返回主菜单")
    print("=" * 60)

def handle_itdt_operations(manager):
    """处理ITDT操作"""
    while True:
        show_itdt_menu()
        choice = input("\n请输入选择 (1-10): ").strip()
        
        if choice == '1':
            if manager.load_tape_itdt():
                print("✓ 磁带加载操作完成")
        elif choice == '2':
            if manager.unload_tape_itdt():
                print("✓ 磁带卸载操作完成")
        elif choice == '3':
            manager.check_tape_status()
        elif choice == '4':
            manager.get_tape_info_itdt()
        elif choice == '5':
            label = input("请输入卷标: ").strip()
            if label:
                result = manager.set_volume_label_itdt(label)
                if result and result.returncode == 0:
                    print("✓ 卷标设置成功")
        elif choice == '6':
            manager.get_partition_info_itdt()
        elif choice == '7':
            partition = input("请输入分区号 (0或1): ").strip()
            if partition in ['0', '1']:
                result = manager.change_partition_itdt(int(partition))
                if result and result.returncode == 0:
                    print("✓ 分区切换成功")
        elif choice == '8':
            result = manager.erase_tape_itdt(quick=True)
            if result and result.returncode == 0:
                print("✓ 快速擦除完成")
        elif choice == '9':
            result = manager.erase_tape_itdt(quick=False)
            if result and result.returncode == 0:
                print("✓ 完全擦除完成")
        elif choice == '10':
            break
        else:
            print("无效选择")
        
        input("\n按回车键继续...")

def handle_ltfs_operations(manager):
    """处理LTFS操作"""
    while True:
        show_ltfs_menu()
        choice = input("\n请输入选择 (1-12): ").strip()
        
        if choice == '1':
            manager.list_drives_ltfs()
        elif choice == '2':
            result = manager.assign_tape_ltfs()
            if result and result.returncode == 0:
                print(f"✓ 磁带已分配到 O: 盘")
        elif choice == '3':
            result = manager.unassign_tape_ltfs()
            if result and result.returncode == 0:
                print(f"✓ 磁带已从 O: 盘卸载")
        elif choice == '4':
            result = manager.load_tape_ltfs()
            if result and result.returncode == 0:
                print("✓ 磁带物理加载成功")
        elif choice == '5':
            result = manager.eject_tape_ltfs()
            if result and result.returncode == 0:
                print("✓ 磁带物理弹出成功")
        elif choice == '6':
            # 使用LtfsCmdFormat格式化 - 需要驱动器ID
            print("使用LtfsCmdFormat格式化磁带")
            manager.list_drives_ltfs()
            drive_id = input("请输入驱动器ID: ").strip()
            
            if drive_id:
                label = input("请输入卷标 (可选): ").strip()
                serial = input("请输入序列号 (6位大写字母数字，可选): ").strip()
                eject_choice = input("格式化后是否弹出磁带? (y/N): ").strip().lower()
                eject_after = (eject_choice == 'y')
                
                result = manager.format_tape_ltfs(drive_id=drive_id, volume_label=label, serial=serial, eject_after=eject_after)
                if result and result.returncode == 0:
                    print("✓ LTFS格式化成功")
        elif choice == '7':
            # 使用mkltfs格式化 - 需要设备地址
            print("使用mkltfs格式化磁带")
            label = input("请输入卷标 (可选): ").strip()
            device_id = input(f"请输入设备ID (回车使用默认 '{manager.default_device}'): ").strip()
            if not device_id:
                device_id = manager.default_device
                
            result = manager.format_tape_mkltfs(device_id=device_id, volume_label=label)
            if result and result.returncode == 0:
                print("✓ mkltfs格式化成功")
        elif choice == '8':
            result = manager.unformat_tape_ltfs()
            if result and result.returncode == 0:
                print("✓ 取消格式化成功")
        elif choice == '9':
            result = manager.check_tape_ltfs()
            if result and result.returncode == 0:
                print("✓ 磁带检查完成")
        elif choice == '10':
            result = manager.rollback_tape_ltfs()
            if result and result.returncode == 0:
                print("✓ 回滚操作完成")
        elif choice == '11':
            # 完整挂载流程
            manager.list_drives_ltfs()
            drive_id = input("\n请输入驱动器ID: ").strip()
            if drive_id:
                label = input("请输入卷标 (可选，格式化时使用): ").strip()
                if manager.mount_tape_complete(drive_id, label):
                    print("✓ 完整挂载流程成功")
                else:
                    print("✗ 完整挂载流程失败")
        elif choice == '12':
            break
        else:
            print("无效选择")
        
        input("\n按回车键继续...")

def handle_workflow_operations(manager):
    """处理组合操作流程"""
    clear_screen()
    print("=" * 60)
    print("                 组合操作流程")
    print("=" * 60)
    print(f"挂载盘符: O:")
    print("1. 完整LTFS初始化流程 (格式化并挂载到O:盘)")
    print("2. 磁带检查流程")
    print("3. 快速挂载已格式化磁带")
    print("4. 返回主菜单")
    print("=" * 60)
    
    choice = input("\n请输入选择 (1-4): ").strip()
    
    if choice == '1':
        print("\n开始完整LTFS初始化流程...")
        # 1. 列出驱动器
        print("\n步骤1: 检查驱动器状态")
        manager.list_drives_ltfs()
        
        drive_id = input("\n请输入要操作的驱动器ID: ").strip()
        if not drive_id:
            print("未输入驱动器ID，中止流程")
            return
        
        # 2. 加载磁带
        print(f"\n步骤2: 加载磁带到驱动器 {drive_id}")
        result = manager.load_tape_ltfs(drive_id)
        if not result or result.returncode != 0:
            print("磁带加载失败，中止流程")
            return
        
        # 3. 格式化磁带
        print(f"\n步骤3: 格式化磁带")
        label = generate_default_label()
        custom_label = input(f"请输入卷标 (回车使用默认 '{label}'): ").strip()
        volume_label = custom_label if custom_label else label
        
        # 使用LtfsCmdFormat格式化
        result = manager.format_tape_ltfs(drive_id, volume_label)
        if not result or result.returncode != 0:
            print("格式化失败，中止流程")
            return
        
        # 4. 分配到O:盘
        print(f"\n步骤4: 分配到 O: 盘")
        result = manager.assign_tape_ltfs(drive_id)
        if result and result.returncode == 0:
            print("✓ LTFS初始化完成，磁带现在可以在 O: 盘访问")
        else:
            print("✗ 分配失败")
    
    elif choice == '2':
        print("\n开始磁带检查流程...")
        manager.list_drives_ltfs()
        
        drive_id = input("\n请输入要检查的驱动器ID: ").strip()
        if drive_id:
            print(f"\n检查驱动器 {drive_id} 的磁带...")
            result = manager.check_tape_ltfs(drive_id)
            if result and result.returncode == 0:
                print("✓ 磁带检查完成")
            else:
                print("✗ 磁带检查失败")
    
    elif choice == '3':
        print("\n快速挂载已格式化的磁带...")
        manager.list_drives_ltfs()
        
        drive_id = input("\n请输入驱动器ID: ").strip()
        if drive_id:
            # 直接分配已格式化的磁带
            result = manager.assign_tape_ltfs(drive_id)
            if result and result.returncode == 0:
                print("✓ 磁带已挂载到 O: 盘")
            else:
                print("✗ 挂载失败")
    
    input("\n按回车键继续...")

def handle_volume_info_operations(manager):
    """处理卷信息获取操作 - 简化版本，只使用fsutil"""
    clear_screen()
    print("=" * 60)
    print("                 获取磁带卷信息")
    print("=" * 60)
    print(f"目标盘符: O:")
    print("说明: 使用 fsutil fsinfo volumeinfo O: 获取卷信息")
    print("1. 获取磁带卷信息")
    print("2. 返回主菜单")
    print("=" * 60)
    
    choice = input("\n请输入选择 (1-2): ").strip()
    
    if choice == '1':
        manager.get_tape_volume_info()
    elif choice == '2':
        return
    else:
        print("无效选择")
    
    input("\n按回车键继续...")

def main():
    """主函数"""
    try:
        manager = TapeManager()
        
        while True:
            show_main_menu()
            choice = input("\n请输入选择 (0-4): ").strip()
            
            if choice == '0':
                print("退出程序...")
                break
            elif choice == '1':
                handle_itdt_operations(manager)
            elif choice == '2':
                handle_ltfs_operations(manager)
            elif choice == '3':
                handle_workflow_operations(manager)
            elif choice == '4':
                handle_volume_info_operations(manager)
            else:
                print("无效选择")
                input("\n按回车键继续...")
                
    except KeyboardInterrupt:
        print("\n\n程序被用户中断")
    except Exception as e:
        print(f"程序执行错误: {e}")
        input("\n按回车键退出...")
    finally:
        print("程序结束")

if __name__ == "__main__":
    main()