# IBM ULT3580-HH9 磁带机快速开始指南

## 🚀 快速开始

### 前置条件
- ✅ 企业级磁带备份系统已安装
- ✅ IBM ULT3580-HH9磁带机已连接
- ✅ 系统运行在 http://localhost:8080
- ✅ 磁带机已正确配置并可访问

### 第一步：验证设备连接

#### 检查设备是否被发现
```bash
curl -X GET "http://localhost:8080/api/tape/devices"
```

**预期响应：**
```json
{
  "devices": [
    {
      "path": "\\\TAPE0",
      "type": "SCSI",
      "vendor": "IBM",
      "model": "ULT3580-HH9",
      "serial": "123456789",
      "status": "online",
      "is_ibm_lto": true,
      "lto_generation": 9,
      "supports_worm": true,
      "supports_encryption": true,
      "native_capacity": 19902989387520
    }
  ]
}
```

#### 检查系统健康状态
```bash
curl -X GET "http://localhost:8080/api/tape/health"
```

**预期响应：**
```json
{
  "healthy": true
}
```

### 第二步：基础操作

#### 查看磁带库存
```bash
curl -X GET "http://localhost:8080/api/tape/inventory"
```

#### 获取当前磁带信息
```bash
curl -X GET "http://localhost:8080/api/tape/current"
```

#### 加载磁带（如果有可用的磁带）
```bash
curl -X POST "http://localhost:8080/api/tape/load?tape_id=TAPE001"
```

### 第三步：IBM特定功能测试

#### 获取TapeAlert警报
```bash
curl -X GET "http://localhost:8080/api/tape/ibm/alerts"
```

#### 获取性能统计
```bash
curl -X GET "http://localhost:8080/api/tape/ibm/performance"
```

#### 获取温度状态
```bash
curl -X GET "http://localhost:8080/api/tape/ibm/temperature"
```

#### 获取设备信息
```bash
curl -X GET "http://localhost:8080/api/tape/ibm/serial"
curl -X GET "http://localhost:8080/api/tape/ibm/firmware"
```

#### 运行自检
```bash
curl -X POST "http://localhost:8080/api/tape/ibm/self-test"
```

## 🔧 高级功能配置

### 启用硬件加密
```bash
# 使用自定义加密密钥
curl -X POST "http://localhost:8080/api/tape/ibm/encryption/enable?encryption_key=my_secure_key_123"

# 使用默认加密设置
curl -X POST "http://localhost:8080/api/tape/ibm/encryption/enable"
```

### 启用WORM模式
```bash
curl -X POST "http://localhost:8080/api/tape/ibm/worm/enable"
```

### 禁用功能
```bash
# 禁用加密
curl -X POST "http://localhost:8080/api/tape/ibm/encryption/disable"

# 禁用WORM模式
curl -X POST "http://localhost:8080/api/tape/ibm/worm/disable"
```

## 🐍 Python快速集成

### 安装依赖
```bash
pip install requests
```

### 基础Python脚本

```python
import requests
import json

class IBMTapeManager:
    def __init__(self, base_url="http://localhost:8080/api/tape"):
        self.base_url = base_url

    def get_devices(self):
        """获取磁带设备列表"""
        try:
            response = requests.get(f"{self.base_url}/devices")
            return response.json()
        except Exception as e:
            print(f"获取设备失败: {e}")
            return None

    def get_alerts(self):
        """获取IBM警报信息"""
        try:
            response = requests.get(f"{self.base_url}/ibm/alerts")
            return response.json()
        except Exception as e:
            print(f"获取警报失败: {e}")
            return None

    def run_self_test(self):
        """运行自检程序"""
        try:
            response = requests.post(f"{self.base_url}/ibm/self-test", timeout=180)
            return response.json()
        except Exception as e:
            print(f"自检失败: {e}")
            return None

    def print_status(self):
        """打印状态摘要"""
        print("=== IBM磁带机状态 ===")

        devices = self.get_devices()
        if devices and devices.get('devices'):
            device = devices['devices'][0]
            print(f"设备: {device['vendor']} {device['model']}")
            print(f"状态: {device['status']}")
            print(f"LTO代数: {device.get('lto_generation', 'N/A')}")
            print(f"支持加密: {'是' if device.get('supports_encryption') else '否'}")
            print(f"支持WORM: {'是' if device.get('supports_worm') else '否'}")

        alerts = self.get_alerts()
        if alerts and alerts.get('success'):
            if alerts['alert_count'] > 0:
                print(f"⚠️  警报数量: {alerts['alert_count']}")
                for alert in alerts['alerts']:
                    print(f"   - {alert}")
            else:
                print("✅ 无警报信息")

# 使用示例
if __name__ == "__main__":
    manager = IBMTapeManager()
    manager.print_status()
```

### 运行Python脚本
```bash
python tape_manager.py
```

## 📱 浏览器快速测试

### 1. 打开Web界面
访问：`http://localhost:8080`

### 2. API测试页面
创建一个简单的HTML测试页面：

```html
<!DOCTYPE html>
<html>
<head>
    <title>IBM磁带机测试</title>
    <script src="https://cdn.tailwindcss.com"></script>
</head>
<body class="bg-gray-100 p-8">
    <div class="max-w-4xl mx-auto">
        <h1 class="text-3xl font-bold mb-6">IBM ULT3580-HH9 磁带机测试</h1>

        <div class="bg-white rounded-lg shadow p-6 mb-6">
            <h2 class="text-xl font-semibold mb-4">设备状态</h2>
            <button onclick="checkDevices()" class="bg-blue-500 text-white px-4 py-2 rounded">
                检查设备
            </button>
            <div id="deviceInfo" class="mt-4 p-4 bg-gray-50 rounded"></div>
        </div>

        <div class="bg-white rounded-lg shadow p-6 mb-6">
            <h2 class="text-xl font-semibold mb-4">IBM功能</h2>
            <div class="grid grid-cols-2 gap-4">
                <button onclick="getAlerts()" class="bg-yellow-500 text-white px-4 py-2 rounded">
                    获取警报
                </button>
                <button onclick="getPerformance()" class="bg-green-500 text-white px-4 py-2 rounded">
                    性能统计
                </button>
                <button onclick="getTemperature()" class="bg-orange-500 text-white px-4 py-2 rounded">
                    温度状态
                </button>
                <button onclick="runSelfTest()" class="bg-purple-500 text-white px-4 py-2 rounded">
                    运行自检
                </button>
            </div>
            <div id="ibmInfo" class="mt-4 p-4 bg-gray-50 rounded"></div>
        </div>

        <div class="bg-white rounded-lg shadow p-6">
            <h2 class="text-xl font-semibold mb-4">高级功能</h2>
            <div class="grid grid-cols-2 gap-4">
                <button onclick="enableEncryption()" class="bg-red-500 text-white px-4 py-2 rounded">
                    启用加密
                </button>
                <button onclick="disableEncryption()" class="bg-gray-500 text-white px-4 py-2 rounded">
                    禁用加密
                </button>
                <button onclick="enableWORM()" class="bg-indigo-500 text-white px-4 py-2 rounded">
                    启用WORM
                </button>
                <button onclick="disableWORM()" class="bg-gray-500 text-white px-4 py-2 rounded">
                    禁用WORM
                </button>
            </div>
            <div id="actionInfo" class="mt-4 p-4 bg-gray-50 rounded"></div>
        </div>
    </div>

    <script>
        const API_BASE = 'http://localhost:8080/api/tape';

        async function apiCall(endpoint, method = 'GET') {
            try {
                const options = {
                    method,
                    headers: {
                        'Content-Type': 'application/json',
                    }
                };

                const response = await fetch(`${API_BASE}${endpoint}`, options);
                const data = await response.json();
                return data;
            } catch (error) {
                console.error('API调用失败:', error);
                return { success: false, error: error.message };
            }
        }

        async function checkDevices() {
            const data = await apiCall('/devices');
            const info = document.getElementById('deviceInfo');

            if (data && data.devices) {
                let html = '<h3>发现设备:</h3>';
                data.devices.forEach((device, index) => {
                    html += `
                        <div class="mb-4 p-3 border rounded">
                            <strong>${device.vendor} ${device.model}</strong><br>
                            路径: ${device.path}<br>
                            状态: ${device.status}<br>
                            ${device.is_ibm_lto ? `
                                LTO代数: ${device.lto_generation}<br>
                                支持加密: ${device.supports_encryption ? '是' : '否'}<br>
                                支持WORM: ${device.supports_worm ? '是' : '否'}
                            ` : ''}
                        </div>
                    `;
                });
                info.innerHTML = html;
            } else {
                info.innerHTML = '<p class="text-red-500">获取设备信息失败</p>';
            }
        }

        async function getAlerts() {
            const data = await apiCall('/ibm/alerts');
            const info = document.getElementById('ibmInfo');

            if (data && data.success) {
                let html = '<h3>TapeAlert警报:</h3>';
                if (data.alert_count > 0) {
                    html += `<p class="text-yellow-600">发现 ${data.alert_count} 个警报:</p>`;
                    data.alerts.forEach(alert => {
                        html += `<div class="text-orange-600">⚠️ ${alert}</div>`;
                    });
                } else {
                    html += '<p class="text-green-600">✅ 无警报信息</p>';
                }
                info.innerHTML = html;
            } else {
                info.innerHTML = '<p class="text-red-500">获取警报失败</p>';
            }
        }

        async function getPerformance() {
            const data = await apiCall('/ibm/performance');
            const info = document.getElementById('ibmInfo');

            if (data && data.success) {
                const perf = data.performance;
                let html = '<h3>性能统计:</h3>';
                html += `
                    <p>总挂载次数: ${perf.total_mounts}</p>
                    <p>总倒带次数: ${perf.total_rewinds}</p>
                    <p>写入数据: ${(perf.total_write_megabytes / 1024).toFixed(1)} GB</p>
                    <p>读取数据: ${(perf.total_read_megabytes / 1024).toFixed(1)} GB</p>
                `;
                info.innerHTML = html;
            } else {
                info.innerHTML = '<p class="text-red-500">获取性能统计失败</p>';
            }
        }

        async function getTemperature() {
            const data = await apiCall('/ibm/temperature');
            const info = document.getElementById('ibmInfo');

            if (data && data.success) {
                const temp = data.temperature;
                let statusIcon = '✅';
                let statusColor = 'text-green-600';

                if (temp.status === 'warning') {
                    statusIcon = '⚠️';
                    statusColor = 'text-yellow-600';
                } else if (temp.status === 'critical') {
                    statusIcon = '🔥';
                    statusColor = 'text-red-600';
                }

                let html = '<h3>温度状态:</h3>';
                html += `
                    <p>${statusIcon} 当前温度: ${temp.current_celsius}°C</p>
                    <p>范围: ${temp.min_celsius}°C - ${temp.max_celsius}°C</p>
                    <p class="${statusColor}">状态: ${temp.status}</p>
                `;
                info.innerHTML = html;
            } else {
                info.innerHTML = '<p class="text-red-500">获取温度状态失败</p>';
            }
        }

        async function runSelfTest() {
            const info = document.getElementById('ibmInfo');
            info.innerHTML = '<p>🔍 正在运行自检程序，请稍候...</p>';

            const data = await apiCall('/ibm/self-test', 'POST');

            if (data && data.success) {
                info.innerHTML = '<h3>自检结果:</h3><p class="text-green-600">✅ 自检完成，磁带机状态正常</p>';
            } else {
                info.innerHTML = '<p class="text-red-500">❌ 自检失败</p>';
            }
        }

        async function enableEncryption() {
            const data = await apiCall('/ibm/encryption/enable', 'POST');
            const info = document.getElementById('actionInfo');

            if (data && data.success) {
                info.innerHTML = '<p class="text-green-600">✅ 硬件加密已启用</p>';
            } else {
                info.innerHTML = '<p class="text-red-500">❌ 启用加密失败</p>';
            }
        }

        async function disableEncryption() {
            const data = await apiCall('/ibm/encryption/disable', 'POST');
            const info = document.getElementById('actionInfo');

            if (data && data.success) {
                info.innerHTML = '<p class="text-gray-600">ℹ️ 硬件加密已禁用</p>';
            } else {
                info.innerHTML = '<p class="text-red-500">❌ 禁用加密失败</p>';
            }
        }

        async function enableWORM() {
            const data = await apiCall('/ibm/worm/enable', 'POST');
            const info = document.getElementById('actionInfo');

            if (data && data.success) {
                info.innerHTML = '<p class="text-indigo-600">🔒 WORM模式已启用（数据只能写入一次）</p>';
            } else {
                info.innerHTML = '<p class="text-red-500">❌ 启用WORM模式失败</p>';
            }
        }

        async function disableWORM() {
            const data = await apiCall('/ibm/worm/disable', 'POST');
            const info = document.getElementById('actionInfo');

            if (data && data.success) {
                info.innerHTML = '<p class="text-gray-600">⚪ WORM模式已禁用</p>';
            } else {
                info.innerHTML = '<p class="text-red-500">❌ 禁用WORM模式失败</p>';
            }
        }

        // 页面加载时自动检查设备
        window.onload = function() {
            checkDevices();
        };
    </script>
</body>
</html>
```

将此HTML文件保存为`tape_test.html`并在浏览器中打开。

## 🐳 Docker快速部署

### 1. 创建Docker Compose文件

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
      - /dev:/dev
    privileged: true
    environment:
      - TAPE_DRIVE_LETTER=A
      - TAPE_CHECK_INTERVAL=60
      - DEFAULT_BLOCK_SIZE=65536
      - MAX_VOLUME_SIZE=20000000000000
    restart: unless-stopped
    healthcheck:
      test: ["CMD", "curl", "-f", "http://localhost:8080/api/health"]
      interval: 30s
      timeout: 10s
      retries: 3
```

### 2. 启动容器

```bash
# 构建并启动
docker-compose up -d

# 查看日志
docker-compose logs -f tape-system

# 停止服务
docker-compose down
```

### 3. 容器内测试

```bash
# 进入容器
docker-compose exec tape-system bash

# 测试API
curl -X GET "http://localhost:8080/api/health"
curl -X GET "http://localhost:8080/api/tape/devices"
```

## 🔍 故障排除

### 常见问题及解决方案

#### 问题1: 设备未发现
**症状**: API返回空设备列表
**解决方案**:
1. 检查SCSI驱动是否正确安装
2. 验证磁带机电源和连接
3. 确认设备权限（Linux需要root或sudo）
4. 检查Windows WMI服务状态

#### 问题2: API连接失败
**症状**: 连接超时或拒绝连接
**解决方案**:
1. 确认系统服务正在运行
2. 检查端口8080是否被占用
3. 验证防火墙设置
4. 检查网络连接

#### 问题3: SCSI命令失败
**症状**: 特定API调用返回错误
**解决方案**:
1. 检查磁带机是否就绪
2. 查看详细的Sense数据
3. 确认磁带已正确加载
4. 检查设备是否处于忙碌状态

#### 问题4: 温度监控异常
**症状**: 温度数据获取失败
**解决方案**:
1. 确认磁带机支持温度监控
2. 检查LOG SENSE命令权限
3. 验证温度传感器状态
4. 查看设备错误日志

### 日志查看

#### 应用日志
```bash
tail -f logs/backup_system.log
```

#### 操作日志
```bash
tail -f logs/operations.log
```

#### 错误日志
```bash
tail -f logs/errors.log
```

## 📞 获取帮助

### 技术支持
- 📧 Email: support@company.com
- 📖 文档: IBM磁带机集成说明.md
- 🌐 在线资源: IBM磁带机API使用示例.md

### 快速参考
- **基础URL**: `http://localhost:8080/api/tape`
- **健康检查**: `/api/tape/health`
- **设备列表**: `/api/tape/devices`
- **IBM警报**: `/api/tape/ibm/alerts`
- **性能统计**: `/api/tape/ibm/performance`
- **温度监控**: `/api/tape/ibm/temperature`

## ✅ 下一步

恭喜！您已经成功完成了IBM ULT3580-HH9磁带机的快速配置和测试。

建议接下来：
1. 📖 阅读完整文档：`IBM磁带机集成说明.md`
2. 🔧 查看API示例：`IBM磁带机API使用示例.md`
3. 🎯 根据需求配置具体的备份策略
4. 📊 设置监控和告警机制
5. 🔒 配置加密和安全策略

---

**快速开始完成！** 🎉
**文档版本**: 1.0
**最后更新**: 2025-11-01
**适用型号**: IBM ULT3580-HH9, LTO-5至LTO-9