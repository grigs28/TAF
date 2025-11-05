# ITDT集成方案
IBM Tape Diagnostic Tool (ITDT) 集成方案

## 一、概述

### 1.1 ITDT简介

ITDT (IBM Tape Diagnostic Tool) 是IBM官方提供的磁带驱动器诊断和管理工具，支持命令行脚本模式，可以替代或补充现有的SCSI接口操作。

### 1.2 集成目标

- 使用ITDT作为磁带操作的底层实现
- 保持现有API接口不变，后端切换为ITDT调用
- 支持ITDT和SCSI接口的切换（配置选择）
- 提供更稳定、更标准的磁带操作

### 1.3 ITDT路径

- Windows: `c:\itdt\itdt.exe`
- Linux: `/usr/local/itdt/itdt` (假设路径，需要确认)

## 二、ITDT命令分析

### 2.1 基本格式

```bash
itdt -f <device_file> [-w <mode>] <subcommand> [options]
```

**设备文件格式：**
- Windows: `\\.\tape0`, `\\.\TAPE0`
- Linux: `/dev/IBMtape0`, `/dev/nst0`

**打开模式 (-w)：**
- 1 = Read/Write (默认)
- 2 = Read Only
- 3 = Write Only
- 4 = Append

### 2.2 核心操作命令

#### 磁带操作
```bash
# 加载磁带
itdt -f \\.\tape0 load [-amu]

# 卸载磁带
itdt -f \\.\tape0 unload

# 倒带
itdt -f \\.\tape0 rewind

# 擦除磁带
itdt -f \\.\tape0 erase           # 完整擦除
itdt -f \\.\tape0 erase -short     # 快速擦除

# 格式化磁带
itdt -f \\.\tape0 formattape [-immed] [-verify] [-mode value]

# 写入文件
itdt -f \\.\tape0 write -s source_file [-raw]

# 读取文件
itdt -f \\.\tape0 read -d destination_file [-c count]

# 写入文件标记
itdt -f \\.\tape0 weof [count]

# 前向/后向定位
itdt -f \\.\tape0 fsf [count]      # 前向定位文件标记
itdt -f \\.\tape0 fsr [count]      # 前向定位记录
itdt -f \\.\tape0 bsf [count]      # 后向定位文件标记
itdt -f \\.\tape0 bsr [count]      # 后向定位记录
```

#### 状态查询
```bash
# 测试单元就绪
itdt -f \\.\tape0 tur

# 查询磁带位置
itdt -f \\.\tape0 qrypos

# 获取设备信息
itdt -f \\.\tape0 devinfo

# 获取所有Log Sense页面
itdt -f \\.\tape0 logsense

# 执行Inquiry命令
itdt -f \\.\tape0 inquiry [Page]

# 获取VPD信息
itdt -f \\.\tape0 vpd
```

#### 设备扫描
```bash
# 扫描所有设备
itdt scan [-o Formatstring]

# 查询设备路径
itdt qrypath
```

#### 诊断测试
```bash
# 标准测试
itdt -f \\.\tape0 standardtest [-forcedataoverwrite]

# 系统测试
itdt -f \\.\tape0 systemtest [-forcedataoverwrite]

# 读写测试
itdt -f \\.\tape0 rwtest [-b Blocksize] [-c Count] [-r Repetition]
```

## 三、架构设计

### 3.1 类结构

```
tape/
├── itdt_interface.py      # ITDT接口类（新建）
├── scsi_interface.py      # SCSI接口类（现有，保持不变）
├── tape_operations.py     # 磁带操作类（修改，支持ITDT/SCSI切换）
└── tape_manager.py        # 磁带管理器（修改，支持配置选择）
```

### 3.2 ITDT接口类设计

```python
class ITDTInterface:
    """ITDT接口类"""
    
    def __init__(self):
        self.itdt_path = None  # ITDT可执行文件路径
        self.default_mode = 1  # 默认打开模式：Read/Write
        
    async def initialize(self):
        """初始化ITDT接口"""
        # 检测ITDT路径
        # Windows: c:\itdt\itdt.exe
        # Linux: /usr/local/itdt/itdt
        
    async def execute_command(self, device_path: str, command: str, 
                            options: List[str] = None) -> Dict[str, Any]:
        """执行ITDT命令"""
        # 构建命令: itdt -f device_path command [options]
        # 使用subprocess执行
        # 解析输出并返回结果
        
    # 具体的操作封装方法
    async def load_tape(self, device_path: str) -> bool
    async def unload_tape(self, device_path: str) -> bool
    async def rewind_tape(self, device_path: str) -> bool
    async def erase_tape(self, device_path: str, short: bool = False) -> bool
    async def format_tape(self, device_path: str, immediate: bool = False, 
                         verify: bool = False, mode: int = None) -> bool
    async def write_file(self, device_path: str, source_file: str, 
                        raw: bool = False) -> bool
    async def read_file(self, device_path: str, destination_file: str, 
                       count: int = None) -> bool
    async def write_filemark(self, device_path: str, count: int = 1) -> bool
    async def test_unit_ready(self, device_path: str) -> bool
    async def get_tape_position(self, device_path: str) -> Optional[int]
    async def get_device_info(self, device_path: str) -> Dict[str, Any]
    async def scan_devices(self) -> List[Dict[str, Any]]
```

### 3.3 配置支持

在 `config/settings.py` 中添加配置项：

```python
# 磁带操作接口选择
TAPE_INTERFACE_TYPE = "itdt"  # "itdt" 或 "scsi"

# ITDT配置
ITDT_PATH = "c:\\itdt\\itdt.exe"  # Windows路径
# ITDT_PATH = "/usr/local/itdt/itdt"  # Linux路径
ITDT_LOG_LEVEL = "Information"  # Errors|Warnings|Information|Debug
ITDT_LOG_PATH = "output"  # 日志路径
ITDT_RESULT_PATH = "output"  # 结果文件路径
```

### 3.4 磁带操作类修改

修改 `TapeOperations` 类，支持ITDT和SCSI接口切换：

```python
class TapeOperations:
    """磁带操作类"""
    
    def __init__(self):
        self.settings = get_settings()
        self.interface_type = self.settings.TAPE_INTERFACE_TYPE
        self.scsi_interface = None
        self.itdt_interface = None
        self._initialized = False
        
    async def initialize(self, scsi_interface=None):
        """初始化磁带操作"""
        if self.interface_type == "itdt":
            from tape.itdt_interface import ITDTInterface
            self.itdt_interface = ITDTInterface()
            await self.itdt_interface.initialize()
        else:
            self.scsi_interface = scsi_interface
            if not self.scsi_interface:
                from tape.scsi_interface import SCSIInterface
                self.scsi_interface = SCSIInterface()
                await self.scsi_interface.initialize()
        
        self._initialized = True
        
    async def load_tape(self, tape_cartridge: TapeCartridge) -> bool:
        """加载磁带"""
        if self.interface_type == "itdt":
            device_path = self._get_device_path()
            return await self.itdt_interface.load_tape(device_path)
        else:
            # 使用现有SCSI接口逻辑
            ...
```

## 四、实现步骤

### 4.1 第一阶段：创建ITDT接口类

1. **创建 `tape/itdt_interface.py`**
   - 实现 `ITDTInterface` 类
   - 实现基本的命令执行方法
   - 实现设备扫描功能
   - 实现错误处理和日志记录

2. **测试ITDT接口**
   - 测试设备扫描
   - 测试基本操作（load, unload, rewind）
   - 测试状态查询（tur, qrypos）

### 4.2 第二阶段：集成到磁带操作类

1. **修改 `config/settings.py`**
   - 添加ITDT相关配置项
   - 添加接口类型选择配置

2. **修改 `tape/tape_operations.py`**
   - 添加接口类型判断逻辑
   - 为每个操作添加ITDT分支
   - 保持API接口不变

3. **修改 `tape/tape_manager.py`**
   - 支持ITDT接口初始化
   - 更新设备扫描逻辑

### 4.3 第三阶段：高级功能

1. **实现读写操作**
   - 使用ITDT的write/read命令
   - 处理大文件分块传输
   - 实现进度回调

2. **实现诊断功能**
   - 集成ITDT的诊断测试命令
   - 提供诊断结果报告

3. **实现日志收集**
   - 收集ITDT日志
   - 集成到系统日志系统

## 五、错误处理

### 5.1 ITDT错误输出格式

ITDT命令失败时，会输出错误信息到stderr或返回非零退出码。需要：

1. 检查命令退出码
2. 解析stderr输出
3. 转换为系统内部错误格式

### 5.2 错误码映射

```python
ITDT_ERROR_MAP = {
    "No device file specified": "设备文件未指定",
    "Device not ready": "设备未就绪",
    "Medium not present": "磁带未加载",
    "Write protected": "写保护",
    "Invalid command": "无效命令",
    ...
}
```

## 六、性能考虑

### 6.1 命令执行开销

- ITDT命令通过subprocess执行，有进程启动开销
- 对于频繁操作，考虑保持连接或批量操作
- 对比SCSI接口的直接调用性能

### 6.2 异步处理

- 使用 `asyncio.create_subprocess_exec` 异步执行ITDT命令
- 避免阻塞主线程
- 设置合理的超时时间

## 七、测试计划

### 7.1 单元测试

- ITDT接口类方法测试
- 命令构建和解析测试
- 错误处理测试

### 7.2 集成测试

- 与现有系统集成测试
- 接口切换测试
- 端到端操作测试

### 7.3 性能测试

- ITDT vs SCSI性能对比
- 并发操作测试
- 长时间运行稳定性测试

## 八、文档更新

### 8.1 README.md

- 添加ITDT安装说明
- 添加配置说明
- 添加接口选择说明

### 8.2 CHANGELOG.md

- 记录版本0.1.0的ITDT集成
- 记录新增功能和改进

### 8.3 用户文档

- ITDT使用指南
- 故障排除指南
- 性能调优建议

## 九、风险评估

### 9.1 技术风险

- **ITDT版本兼容性**：不同版本的ITDT命令参数可能不同
- **跨平台支持**：Windows和Linux的ITDT路径和行为可能不同
- **性能影响**：subprocess调用可能比直接SCSI调用慢

### 9.2 缓解措施

- 版本检测和兼容性处理
- 提供配置选项，允许回退到SCSI接口
- 性能监控和优化

## 十、后续优化

### 10.1 功能增强

- 支持ITDT的高级功能（分区、加密等）
- 集成ITDT的诊断报告
- 实现ITDT命令的批处理

### 10.2 性能优化

- 命令缓存机制
- 连接池管理
- 批量操作支持

## 十一、时间表

1. **第1周**：创建ITDT接口类，实现基本操作
2. **第2周**：集成到磁带操作类，实现接口切换
3. **第3周**：实现高级功能，完善错误处理
4. **第4周**：测试、文档更新、版本发布

