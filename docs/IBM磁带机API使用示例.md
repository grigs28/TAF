# IBM磁带机API使用示例

## 概述

本文档提供了IBM ULT3580-HH9磁带机API的详细使用示例，包括Python、JavaScript、cURL等多种调用方式的完整代码示例。

## 🔧 环境准备

### 1. 系统要求
- 企业级磁带备份系统已安装并运行
- IBM ULT3580-HH9磁带机已正确连接
- 网络连接正常，可访问 http://localhost:8080

### 2. 基础URL
所有API请求的基础URL为：
```
http://localhost:8080/api/tape
```

## 📋 基础API使用示例

### 1. 设备发现与管理

#### 获取磁带设备列表
```python
import requests

def get_tape_devices():
    """获取所有磁带设备信息"""
    url = "http://localhost:8080/api/tape/devices"

    try:
        response = requests.get(url)
        response.raise_for_status()

        devices = response.json()
        print("发现的磁带设备:")
        for i, device in enumerate(devices['devices'], 1):
            print(f"{i}. 路径: {device['path']}")
            print(f"   厂商: {device['vendor']}")
            print(f"   型号: {device['model']}")
            print(f"   状态: {device['status']}")
            if device.get('is_ibm_lto'):
                print(f"   LTO代数: {device['lto_generation']}")
                print(f"   原生容量: {device['native_capacity'] / (1024**4):.1f} TB")
            print("-" * 50)

        return devices
    except requests.exceptions.RequestException as e:
        print(f"请求失败: {e}")
        return None

# 使用示例
devices = get_tape_devices()
```

```bash
# cURL命令示例
curl -X GET "http://localhost:8080/api/tape/devices" \
     -H "Content-Type: application/json"
```

```javascript
// JavaScript (fetch API) 示例
async function getTapeDevices() {
    try {
        const response = await fetch('http://localhost:8080/api/tape/devices');
        const data = await response.json();

        console.log('发现的磁带设备:', data.devices);
        return data;
    } catch (error) {
        console.error('请求失败:', error);
        return null;
    }
}

getTapeDevices();
```

#### 检查磁带健康状态
```python
def check_tape_health():
    """检查磁带系统健康状态"""
    url = "http://localhost:8080/api/tape/health"

    try:
        response = requests.get(url)
        response.raise_for_status()

        health = response.json()
        if health['healthy']:
            print("✅ 磁带系统运行正常")
        else:
            print("❌ 磁带系统存在问题")

        return health['healthy']
    except requests.exceptions.RequestException as e:
        print(f"健康检查失败: {e}")
        return False

# 使用示例
is_healthy = check_tape_health()
```

### 2. 磁带操作示例

#### 获取磁带库存状态
```python
def get_tape_inventory():
    """获取磁带库存概览"""
    url = "http://localhost:8080/api/tape/inventory"

    try:
        response = requests.get(url)
        response.raise_for_status()

        inventory = response.json()
        print("=== 磁带库存状态 ===")
        print(f"总磁带数: {inventory['total_tapes']}")
        print(f"可用磁带: {inventory['available_tapes']}")
        print(f"使用中: {inventory['in_use_tapes']}")
        print(f"已过期: {inventory['expired_tapes']}")
        print(f"总容量: {inventory['total_capacity_bytes'] / (1024**4):.1f} TB")
        print(f"已用容量: {inventory['used_capacity_bytes'] / (1024**4):.1f} TB")
        print(f"使用率: {inventory['usage_percent']:.1f}%")

        return inventory
    except requests.exceptions.RequestException as e:
        print(f"获取库存失败: {e}")
        return None

# 使用示例
inventory = get_tape_inventory()
```

#### 加载磁带
```python
def load_tape(tape_id):
    """加载指定磁带"""
    url = f"http://localhost:8080/api/tape/load"
    params = {"tape_id": tape_id}

    try:
        response = requests.post(url, params=params)
        response.raise_for_status()

        result = response.json()
        if result['success']:
            print(f"✅ 磁带 {tape_id} 加载成功")
        else:
            print(f"❌ 磁带 {tape_id} 加载失败")

        return result['success']
    except requests.exceptions.RequestException as e:
        print(f"加载磁带失败: {e}")
        return False

# 使用示例
success = load_tape("TAPE001")
```

#### 获取当前磁带信息
```python
def get_current_tape_info():
    """获取当前加载的磁带信息"""
    url = "http://localhost:8080/api/tape/current"

    try:
        response = requests.get(url)
        response.raise_for_status()

        tape_info = response.json()
        if 'tape_id' in tape_info:
            print("=== 当前磁带信息 ===")
            print(f"磁带ID: {tape_info['tape_id']}")
            print(f"标签: {tape_info['label']}")
            print(f"状态: {tape_info['status']}")
            print(f"容量: {tape_info['capacity_bytes'] / (1024**4):.1f} TB")
            print(f"已用: {tape_info['used_bytes'] / (1024**4):.1f} TB")
            print(f"剩余: {tape_info['free_bytes'] / (1024**4):.1f} TB")
            print(f"使用率: {tape_info['usage_percent']:.1f}%")
            print(f"位置: {tape_info['location']}")
            print(f"创建时间: {tape_info['created_date']}")
            print(f"过期时间: {tape_info['expiry_date']}")

            return tape_info
        else:
            print("当前没有加载的磁带")
            return None

    except requests.exceptions.RequestException as e:
        print(f"获取磁带信息失败: {e}")
        return None

# 使用示例
current_tape = get_current_tape_info()
```

## 🔬 IBM特定功能示例

### 1. 监控功能

#### 获取TapeAlert警报信息
```python
def get_tape_alerts():
    """获取磁带警报信息"""
    url = "http://localhost:8080/api/tape/ibm/alerts"

    try:
        response = requests.get(url)
        response.raise_for_status()

        alerts = response.json()
        if alerts['success']:
            print("=== TapeAlert 警报 ===")
            if alerts['alert_count'] > 0:
                print(f"发现 {alerts['alert_count']} 个警报:")
                for i, alert in enumerate(alerts['alerts'], 1):
                    print(f"{i}. {alert}")
            else:
                print("✅ 没有警报信息")

            return alerts
        else:
            print(f"获取警报失败: {alerts['error']}")
            return None

    except requests.exceptions.RequestException as e:
        print(f"请求警报信息失败: {e}")
        return None

# 使用示例
alerts = get_tape_alerts()
```

#### 获取性能统计
```python
def get_performance_stats():
    """获取磁带机性能统计"""
    url = "http://localhost:8080/api/tape/ibm/performance"

    try:
        response = requests.get(url)
        response.raise_for_status()

        perf = response.json()
        if perf['success']:
            stats = perf['performance']
            print("=== 性能统计 ===")
            print(f"总挂载次数: {stats.get('total_mounts', 0)}")
            print(f"总倒带次数: {stats.get('total_rewinds', 0)}")
            print(f"总写入数据: {stats.get('total_write_megabytes', 0) / 1024:.1f} GB")
            print(f"总读取数据: {stats.get('total_read_megabytes', 0) / 1024:.1f} GB")

            return perf
        else:
            print(f"获取性能统计失败: {perf['error']}")
            return None

    except requests.exceptions.RequestException as e:
        print(f"获取性能统计失败: {e}")
        return None

# 使用示例
performance = get_performance_stats()
```

#### 获取温度状态
```python
def get_temperature_status():
    """获取磁带机温度状态"""
    url = "http://localhost:8080/api/tape/ibm/temperature"

    try:
        response = requests.get(url)
        response.raise_for_status()

        temp = response.json()
        if temp['success']:
            temp_info = temp['temperature']
            print("=== 温度状态 ===")
            print(f"当前温度: {temp_info['current_celsius']}°C")
            print(f"最高温度: {temp_info['max_celsius']}°C")
            print(f"最低温度: {temp_info['min_celsius']}°C")
            print(f"状态: {temp_info['status']}")

            # 温度警告
            if temp_info['status'] == 'warning':
                print("⚠️  温度偏高，请注意散热")
            elif temp_info['status'] == 'critical':
                print("🔥 温度过高，请立即检查！")
            else:
                print("✅ 温度正常")

            return temp
        else:
            print(f"获取温度状态失败: {temp['error']}")
            return None

    except requests.exceptions.RequestException as e:
        print(f"获取温度状态失败: {e}")
        return None

# 使用示例
temperature = get_temperature_status()
```

#### 获取设备信息
```python
def get_device_info():
    """获取IBM磁带机详细信息"""
    base_url = "http://localhost:8080/api/tape/ibm"

    try:
        # 获取序列号
        serial_response = requests.get(f"{base_url}/serial")
        serial_data = serial_response.json()

        # 获取固件版本
        firmware_response = requests.get(f"{base_url}/firmware")
        firmware_data = firmware_response.json()

        print("=== 设备信息 ===")

        if serial_data['success']:
            print(f"序列号: {serial_data['serial_number']}")
        else:
            print(f"序列号: 获取失败 - {serial_data['error']}")

        if firmware_data['success']:
            print(f"固件版本: {firmware_data['firmware_version']}")
        else:
            print(f"固件版本: 获取失败 - {firmware_data['error']}")

        return {
            'serial': serial_data,
            'firmware': firmware_data
        }

    except requests.exceptions.RequestException as e:
        print(f"获取设备信息失败: {e}")
        return None

# 使用示例
device_info = get_device_info()
```

### 2. 高级配置功能

#### 硬件加密管理
```python
class TapeEncryptionManager:
    """磁带加密管理器"""

    def __init__(self, base_url="http://localhost:8080/api/tape/ibm"):
        self.base_url = base_url

    def enable_encryption(self, encryption_key=None):
        """启用硬件加密"""
        url = f"{self.base_url}/encryption/enable"
        params = {}
        if encryption_key:
            params['encryption_key'] = encryption_key

        try:
            response = requests.post(url, params=params)
            response.raise_for_status()

            result = response.json()
            if result['success']:
                print("✅ 硬件加密已启用")
                return True
            else:
                print(f"❌ 启用加密失败: {result['error']}")
                return False

        except requests.exceptions.RequestException as e:
            print(f"启用加密请求失败: {e}")
            return False

    def disable_encryption(self):
        """禁用硬件加密"""
        url = f"{self.base_url}/encryption/disable"

        try:
            response = requests.post(url)
            response.raise_for_status()

            result = response.json()
            if result['success']:
                print("✅ 硬件加密已禁用")
                return True
            else:
                print(f"❌ 禁用加密失败: {result['error']}")
                return False

        except requests.exceptions.RequestException as e:
            print(f"禁用加密请求失败: {e}")
            return False

# 使用示例
encryption_manager = TapeEncryptionManager()

# 启用加密
encryption_manager.enable_encryption("my_secure_key_123")

# 禁用加密
encryption_manager.disable_encryption()
```

#### WORM模式管理
```python
class TapeWormManager:
    """WORM模式管理器"""

    def __init__(self, base_url="http://localhost:8080/api/tape/ibm"):
        self.base_url = base_url

    def enable_worm_mode(self):
        """启用WORM模式"""
        url = f"{self.base_url}/worm/enable"

        try:
            response = requests.post(url)
            response.raise_for_status()

            result = response.json()
            if result['success']:
                print("✅ WORM模式已启用")
                print("⚠️  注意：WORM模式下数据只能写入一次")
                return True
            else:
                print(f"❌ 启用WORM模式失败: {result['error']}")
                return False

        except requests.exceptions.RequestException as e:
            print(f"启用WORM模式请求失败: {e}")
            return False

    def disable_worm_mode(self):
        """禁用WORM模式"""
        url = f"{self.base_url}/worm/disable"

        try:
            response = requests.post(url)
            response.raise_for_status()

            result = response.json()
            if result['success']:
                print("✅ WORM模式已禁用")
                return True
            else:
                print(f"❌ 禁用WORM模式失败: {result['error']}")
                return False

        except requests.exceptions.RequestException as e:
            print(f"禁用WORM模式请求失败: {e}")
            return False

# 使用示例
worm_manager = TapeWormManager()

# 启用WORM模式
worm_manager.enable_worm_mode()

# 禁用WORM模式
worm_manager.disable_worm_mode()
```

### 3. 诊断功能

#### 运行自检程序
```python
def run_self_test():
    """运行磁带机自检程序"""
    url = "http://localhost:8080/api/tape/ibm/self-test"

    print("🔍 开始运行磁带机自检程序...")

    try:
        response = requests.post(url, timeout=180)  # 自检可能需要较长时间
        response.raise_for_status()

        result = response.json()
        if result['success']:
            print("✅ 自检程序完成，磁带机状态正常")
        else:
            print(f"❌ 自检程序失败: {result['error']}")

        return result['success']

    except requests.exceptions.Timeout:
        print("⏰ 自检程序超时，请稍后检查结果")
        return False
    except requests.exceptions.RequestException as e:
        print(f"运行自检失败: {e}")
        return False

# 使用示例
self_test_result = run_self_test()
```

#### 获取磁带位置信息
```python
def get_tape_position():
    """获取磁带当前位置信息"""
    url = "http://localhost:8080/api/tape/ibm/position"

    try:
        response = requests.get(url)
        response.raise_for_status()

        position = response.json()
        if position['success']:
            print("=== 磁带位置信息 ===")
            print(f"分区: {position['partition']}")
            print(f"文件号: {position['file_number']}")
            print(f"设置号: {position['set_number']}")
            print(f"数据结束位置: {position['end_of_data']}")
            print(f"缓冲区块号: {position['block_in_buffer']}")

            # 位置状态标志
            flags = []
            if position['is_bom']:
                flags.append("磁带开始")
            if position['is_eop']:
                flags.append("分区结束")
            if position['is_bop']:
                flags.append("分区开始")

            if flags:
                print(f"位置状态: {', '.join(flags)}")

            return position
        else:
            print(f"获取位置信息失败: {position['error']}")
            return None

    except requests.exceptions.RequestException as e:
        print(f"获取位置信息失败: {e}")
        return None

# 使用示例
position = get_tape_position()
```

## 🔧 高级SCSI命令示例

### 1. 自定义LOG SENSE命令

```python
class SCSICommandManager:
    """SCSI命令管理器"""

    def __init__(self, base_url="http://localhost:8080/api/tape/ibm"):
        self.base_url = base_url

    def send_log_sense(self, page_code, subpage_code=0):
        """发送LOG SENSE命令"""
        url = f"{self.base_url}/log-sense"
        params = {
            'page_code': page_code,
            'subpage_code': subpage_code
        }

        try:
            response = requests.post(url, params=params)
            response.raise_for_status()

            result = response.json()
            if result['success']:
                print(f"✅ LOG SENSE命令执行成功")
                print(f"页面代码: 0x{page_code:02X}")
                print(f"子页面代码: 0x{subpage_code:02X}")
                print(f"数据长度: {result['data_length']} 字节")
                print(f"原始数据: {result['log_data'][:64]}...")
                return result
            else:
                print(f"❌ LOG SENSE命令失败: {result['error']}")
                return None

        except requests.exceptions.RequestException as e:
            print(f"发送LOG SENSE命令失败: {e}")
            return None

    def send_mode_sense(self, page_code=0x3F, subpage_code=0):
        """发送MODE SENSE命令"""
        url = f"{self.base_url}/mode-sense"
        params = {
            'page_code': page_code,
            'subpage_code': subpage_code
        }

        try:
            response = requests.post(url, params=params)
            response.raise_for_status()

            result = response.json()
            if result['success']:
                print(f"✅ MODE SENSE命令执行成功")
                print(f"页面代码: 0x{page_code:02X}")
                print(f"子页面代码: 0x{subpage_code:02X}")
                print(f"数据长度: {result['data_length']} 字节")
                print(f"原始数据: {result['mode_data'][:64]}...")
                return result
            else:
                print(f"❌ MODE SENSE命令失败: {result['error']}")
                return None

        except requests.exceptions.RequestException as e:
            print(f"发送MODE SENSE命令失败: {e}")
            return None

    def send_inquiry_vpd(self, page_code):
        """发送INQUIRY VPD命令"""
        url = f"{self.base_url}/inquiry-vpd"
        params = {'page_code': page_code}

        try:
            response = requests.post(url, params=params)
            response.raise_for_status()

            result = response.json()
            if result['success']:
                print(f"✅ INQUIRY VPD命令执行成功")
                print(f"页面代码: 0x{page_code:02X}")
                print(f"数据长度: {result['data_length']} 字节")
                print(f"原始数据: {result['vpd_data'][:64]}...")
                return result
            else:
                print(f"❌ INQUIRY VPD命令失败: {result['error']}")
                return None

        except requests.exceptions.RequestException as e:
            print(f"发送INQUIRY VPD命令失败: {e}")
            return None

# 使用示例
scsi_manager = SCSICommandManager()

# 常用LOG SENSE页面
print("=== LOG SENSE命令示例 ===")

# TapeAlert页面 (0x2E)
scsi_manager.send_log_sense(0x2E)

# 性能统计页面 (0x17)
scsi_manager.send_log_sense(0x17)

# 温度监控页面 (0x0D)
scsi_manager.send_log_sense(0x0D)

# 使用统计页面 (0x31)
scsi_manager.send_log_sense(0x31)
```

## 📊 完整监控脚本示例

```python
#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
IBM磁带机监控脚本
"""

import requests
import time
import json
from datetime import datetime

class TapeMonitor:
    """磁带监控器"""

    def __init__(self, base_url="http://localhost:8080/api/tape"):
        self.base_url = base_url

    def get_system_status(self):
        """获取系统整体状态"""
        try:
            # 获取设备列表
            devices_response = requests.get(f"{self.base_url}/devices")
            devices = devices_response.json()

            # 获取健康状态
            health_response = requests.get(f"{self.base_url}/health")
            health = health_response.json()

            # 获取库存状态
            inventory_response = requests.get(f"{self.base_url}/inventory")
            inventory = inventory_response.json()

            return {
                'timestamp': datetime.now().isoformat(),
                'devices': devices,
                'health': health,
                'inventory': inventory
            }
        except Exception as e:
            print(f"获取系统状态失败: {e}")
            return None

    def get_ibm_status(self):
        """获取IBM特定状态"""
        try:
            # 获取警报信息
            alerts_response = requests.get(f"{self.base_url}/ibm/alerts")
            alerts = alerts_response.json()

            # 获取性能统计
            perf_response = requests.get(f"{self.base_url}/ibm/performance")
            performance = perf_response.json()

            # 获取温度状态
            temp_response = requests.get(f"{self.base_url}/ibm/temperature")
            temperature = temp_response.json()

            # 获取设备信息
            serial_response = requests.get(f"{self.base_url}/ibm/serial")
            firmware_response = requests.get(f"{self.base_url}/ibm/firmware")

            return {
                'alerts': alerts,
                'performance': performance,
                'temperature': temperature,
                'serial': serial_response,
                'firmware': firmware_response
            }
        except Exception as e:
            print(f"获取IBM状态失败: {e}")
            return None

    def print_status_report(self):
        """打印状态报告"""
        print("=" * 60)
        print(f"磁带系统监控报告 - {datetime.now().strftime('%Y-%m-%d %H:%M:%S')}")
        print("=" * 60)

        # 系统状态
        system_status = self.get_system_status()
        if system_status:
            print("\n📊 系统状态")
            print("-" * 30)
            print(f"设备数量: {len(system_status['devices']['devices'])}")
            print(f"健康状态: {'✅ 正常' if system_status['health']['healthy'] else '❌ 异常'}")
            print(f"总磁带: {system_status['inventory']['total_tapes']}")
            print(f"可用: {system_status['inventory']['available_tapes']}")
            print(f"使用中: {system_status['inventory']['in_use_tapes']}")
            print(f"使用率: {system_status['inventory']['usage_percent']:.1f}%")

        # IBM状态
        ibm_status = self.get_ibm_status()
        if ibm_status:
            print("\n🔧 IBM设备状态")
            print("-" * 30)

            # 设备信息
            if ibm_status['serial']['success']:
                print(f"序列号: {ibm_status['serial']['serial_number']}")
            if ibm_status['firmware']['success']:
                print(f"固件版本: {ibm_status['firmware']['firmware_version']}")

            # 警报信息
            if ibm_status['alerts']['success']:
                alert_count = ibm_status['alerts']['alert_count']
                if alert_count > 0:
                    print(f"⚠️  警报数量: {alert_count}")
                    for alert in ibm_status['alerts']['alerts']:
                        print(f"   - {alert}")
                else:
                    print("✅ 无警报信息")

            # 温度状态
            if ibm_status['temperature']['success']:
                temp = ibm_status['temperature']['temperature']
                status_icon = "✅"
                if temp['status'] == 'warning':
                    status_icon = "⚠️"
                elif temp['status'] == 'critical':
                    status_icon = "🔥"

                print(f"{status_icon} 当前温度: {temp['current_celsius']}°C")
                print(f"   范围: {temp['min_celsius']}°C - {temp['max_celsius']}°C")

            # 性能统计
            if ibm_status['performance']['success']:
                perf = ibm_status['performance']['performance']
                print(f"📈 性能统计")
                print(f"   挂载次数: {perf.get('total_mounts', 0)}")
                print(f"   倒带次数: {perf.get('total_rewinds', 0)}")
                print(f"   写入数据: {perf.get('total_write_megabytes', 0) / 1024:.1f} GB")
                print(f"   读取数据: {perf.get('total_read_megabytes', 0) / 1024:.1f} GB")

        print("\n" + "=" * 60)

    def run_monitoring(self, interval=60):
        """运行监控循环"""
        print("🚀 启动磁带系统监控...")

        try:
            while True:
                self.print_status_report()
                time.sleep(interval)
        except KeyboardInterrupt:
            print("\n👋 监控已停止")
        except Exception as e:
            print(f"\n❌ 监控异常: {e}")

# 使用示例
if __name__ == "__main__":
    monitor = TapeMonitor()

    # 单次状态报告
    monitor.print_status_report()

    # 持续监控（每60秒一次）
    # monitor.run_monitoring(interval=60)
```

## 🐳 Docker容器中的使用

### Docker Compose配置示例

```yaml
version: '3.8'

services:
  tape-system:
    build: .
    ports:
      - "8080:8080"
    volumes:
      - ./logs:/app/logs
      - ./data:/app/data
      - /dev:/dev  # 挂载设备文件（Linux）
    privileged: true  # 需要特权访问SCSI设备
    environment:
      - TAPE_DRIVE_LETTER=A
      - TAPE_CHECK_INTERVAL=60
    restart: unless-stopped
```

### 容器内Python脚本

```python
import requests
import os

class TapeAPIClient:
    def __init__(self):
        self.base_url = os.getenv('TAPE_API_URL', 'http://localhost:8080/api/tape')

    def check_container_environment(self):
        """检查容器环境"""
        print("=== 容器环境检查 ===")
        print(f"API URL: {self.base_url}")

        # 检查设备文件访问权限
        device_files = ['/dev/nst0', '/dev/st0', '/dev/sg0']
        for device in device_files:
            if os.path.exists(device):
                print(f"✅ 设备文件存在: {device}")
            else:
                print(f"❌ 设备文件不存在: {device}")

        # 测试API连接
        try:
            response = requests.get(f"{self.base_url}/health", timeout=5)
            if response.status_code == 200:
                print("✅ API连接正常")
            else:
                print(f"❌ API连接异常: {response.status_code}")
        except Exception as e:
            print(f"❌ API连接失败: {e}")

# 容器启动时检查
client = TapeAPIClient()
client.check_container_environment()
```

## 📱 Web界面集成示例

### React组件示例

```jsx
import React, { useState, useEffect } from 'react';
import axios from 'axios';

const TapeMonitorDashboard = () => {
    const [devices, setDevices] = useState([]);
    const [alerts, setAlerts] = useState([]);
    const [temperature, setTemperature] = useState(null);
    const [performance, setPerformance] = useState(null);
    const [loading, setLoading] = useState(true);

    const API_BASE = 'http://localhost:8080/api/tape';

    useEffect(() => {
        fetchData();
        const interval = setInterval(fetchData, 30000); // 每30秒更新
        return () => clearInterval(interval);
    }, []);

    const fetchData = async () => {
        try {
            setLoading(true);

            // 并行获取所有数据
            const [devicesRes, alertsRes, tempRes, perfRes] = await Promise.all([
                axios.get(`${API_BASE}/devices`),
                axios.get(`${API_BASE}/ibm/alerts`),
                axios.get(`${API_BASE}/ibm/temperature`),
                axios.get(`${API_BASE}/ibm/performance`)
            ]);

            setDevices(devicesRes.data.devices || []);
            setAlerts(alertsRes.data.alerts || []);
            setTemperature(tempRes.data);
            setPerformance(perfRes.data);

        } catch (error) {
            console.error('获取数据失败:', error);
        } finally {
            setLoading(false);
        }
    };

    const handleEnableEncryption = async () => {
        try {
            const response = await axios.post(`${API_BASE}/ibm/encryption/enable`);
            alert(response.data.message);
        } catch (error) {
            console.error('启用加密失败:', error);
            alert('启用加密失败');
        }
    };

    if (loading) {
        return <div className="loading">加载中...</div>;
    }

    return (
        <div className="tape-dashboard">
            <h1>IBM磁带机监控面板</h1>

            {/* 设备列表 */}
            <div className="devices-section">
                <h2>设备列表</h2>
                {devices.map((device, index) => (
                    <div key={index} className="device-card">
                        <h3>{device.vendor} {device.model}</h3>
                        <p>路径: {device.path}</p>
                        <p>状态: {device.status}</p>
                        {device.is_ibm_lto && (
                            <div>
                                <p>LTO代数: {device.lto_generation}</p>
                                <p>支持WORM: {device.supports_worm ? '是' : '否'}</p>
                                <p>支持加密: {device.supports_encryption ? '是' : '否'}</p>
                            </div>
                        )}
                    </div>
                ))}
            </div>

            {/* 警报信息 */}
            <div className="alerts-section">
                <h2>系统警报</h2>
                {alerts.length > 0 ? (
                    <div className="alert-list">
                        {alerts.map((alert, index) => (
                            <div key={index} className="alert-item">
                                ⚠️ {alert}
                            </div>
                        ))}
                    </div>
                ) : (
                    <div className="no-alerts">✅ 无警报信息</div>
                )}
            </div>

            {/* 温度监控 */}
            {temperature && temperature.success && (
                <div className="temperature-section">
                    <h2>温度监控</h2>
                    <div className={`temp-status ${temperature.temperature.status}`}>
                        <p>当前温度: {temperature.temperature.current_celsius}°C</p>
                        <p>状态: {temperature.temperature.status}</p>
                    </div>
                </div>
            )}

            {/* 性能统计 */}
            {performance && performance.success && (
                <div className="performance-section">
                    <h2>性能统计</h2>
                    <div className="perf-stats">
                        <p>挂载次数: {performance.performance.total_mounts}</p>
                        <p>倒带次数: {performance.performance.total_rewinds}</p>
                        <p>写入数据: {(performance.performance.total_write_megabytes / 1024).toFixed(1)} GB</p>
                        <p>读取数据: {(performance.performance.total_read_megabytes / 1024).toFixed(1)} GB</p>
                    </div>
                </div>
            )}

            {/* 操作按钮 */}
            <div className="actions-section">
                <button onClick={handleEnableEncryption}>
                    启用硬件加密
                </button>
                <button onClick={() => axios.post(`${API_BASE}/ibm/self-test`)}>
                    运行自检
                </button>
            </div>
        </div>
    );
};

export default TapeMonitorDashboard;
```

## 🔗 错误处理和重试机制

```python
import requests
import time
from functools import wraps

def retry_on_failure(max_retries=3, delay=1):
    """重试装饰器"""
    def decorator(func):
        @wraps(func)
        def wrapper(*args, **kwargs):
            for attempt in range(max_retries):
                try:
                    return func(*args, **kwargs)
                except requests.exceptions.RequestException as e:
                    if attempt == max_retries - 1:
                        raise
                    print(f"请求失败，第{attempt + 1}次重试... ({e})")
                    time.sleep(delay * (2 ** attempt))  # 指数退避
            return None
        return wrapper
    return decorator

class RobustTapeAPI:
    """健壮的磁带API客户端"""

    def __init__(self, base_url="http://localhost:8080/api/tape"):
        self.base_url = base_url
        self.session = requests.Session()
        self.session.timeout = 30

    @retry_on_failure(max_retries=3, delay=1)
    def get_devices(self):
        """获取设备列表（带重试）"""
        response = self.session.get(f"{self.base_url}/devices")
        response.raise_for_status()
        return response.json()

    @retry_on_failure(max_retries=2, delay=2)
    def get_alerts(self):
        """获取警报信息（带重试）"""
        response = self.session.get(f"{self.base_url}/ibm/alerts")
        response.raise_for_status()
        return response.json()

    def safe_api_call(self, endpoint, method='GET', **kwargs):
        """安全的API调用"""
        url = f"{self.base_url}{endpoint}"

        try:
            if method.upper() == 'GET':
                response = self.session.get(url, **kwargs)
            elif method.upper() == 'POST':
                response = self.session.post(url, **kwargs)
            else:
                raise ValueError(f"不支持的HTTP方法: {method}")

            response.raise_for_status()
            return response.json()

        except requests.exceptions.Timeout:
            print(f"请求超时: {endpoint}")
            return {'success': False, 'error': '请求超时'}
        except requests.exceptions.ConnectionError:
            print(f"连接错误: {endpoint}")
            return {'success': False, 'error': '连接失败'}
        except requests.exceptions.HTTPError as e:
            print(f"HTTP错误: {endpoint} - {e.response.status_code}")
            return {'success': False, 'error': f'HTTP {e.response.status_code}'}
        except Exception as e:
            print(f"未知错误: {endpoint} - {str(e)}")
            return {'success': False, 'error': str(e)}

# 使用示例
robust_api = RobustTapeAPI()
devices = robust_api.get_devices()
alerts = robust_api.safe_api_call('/ibm/alerts')
```

这个完整的API使用示例文档涵盖了：

1. **基础API使用** - 设备发现、状态查询、磁带操作
2. **IBM特定功能** - 监控、加密、WORM模式、诊断
3. **高级SCSI命令** - 自定义LOG SENSE、MODE SENSE、INQUIRY VPD
4. **完整监控脚本** - 实时监控和报告生成
5. **容器化部署** - Docker Compose配置
6. **Web界面集成** - React组件示例
7. **错误处理** - 重试机制和异常处理

所有示例都经过精心设计，可以直接复制使用或根据需要进行修改。