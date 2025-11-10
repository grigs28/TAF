# libltfs.dll MAM 和卷标功能说明

## 概述

`libltfs.dll` 是 IBM LTFS (Linear Tape File System) 的核心库，包含 586 个导出函数。虽然导出函数名称在分析时显示为乱码，但根据 LTFS 标准和 IBM 磁带机的特性，该库应该支持以下功能。

## 功能分类

### 1. 磁带卷标（Volume Label）

#### 设置卷标
- **方式1**: 通过 `LtfsCmdFormat.exe` 格式化时设置
  - 命令格式: `LtfsCmdFormat.exe O /N:卷标名称`
  - 参数: `/N:` 用于指定卷标名称
  - **libltfs.dll 支持**: 格式化功能由 libltfs.dll 提供底层支持

#### 读取卷标
- **方式1**: 通过 Windows `fsutil` 命令读取
  - 命令格式: `fsutil fsinfo volumeinfo O:`
  - 返回: 卷名（Volume Name）、卷序列号等信息
- **方式2**: 通过 libltfs.dll API（需要调用相应的函数）
  - libltfs.dll 应该提供读取 LTFS 元数据的 API
  - 卷标存储在 LTFS 文件系统的元数据中

#### 当前实现
- ✅ 已实现: `format_tape_ltfs()` - 格式化时设置卷标
- ✅ 已实现: `read_tape_label_windows()` - 通过 fsutil 读取卷标
- ⚠️ 未实现: 直接通过 libltfs.dll API 读取卷标

### 2. 序列号（Serial Number）

#### 存储位置
- **MAM (Media Auxiliary Memory)**: 序列号通常存储在磁带的 MAM 中
- **LTFS 格式化参数**: 格式化时可以通过 `/S:` 参数设置序列号（6位大写字母数字）

#### 读取方式
- **ITDT 命令**: `itdt.exe -f \\.\tape0 readattr` - 读取 MAM 属性
- **libltfs.dll**: 应该提供读取 MAM 的 API 函数

#### 当前实现
- ✅ 已实现: `format_tape_ltfs()` - 格式化时设置序列号（通过 `/S:` 参数）
- ⚠️ 未实现: 通过 ITDT `readattr` 命令读取 MAM 属性
- ⚠️ 未实现: 直接通过 libltfs.dll API 读取序列号

### 3. 二维码（Barcode）

#### 存储位置
- **MAM (Media Auxiliary Memory)**: 二维码信息通常也存储在 MAM 中
- **物理标签**: 磁带盒上的物理二维码标签

#### 读取方式
- **ITDT 命令**: `itdt.exe -f \\.\tape0 readattr` - 读取 MAM 属性（可能包含二维码信息）
- **libltfs.dll**: 应该提供读取 MAM 的 API 函数

#### 当前实现
- ❌ 未实现: 读取二维码功能

## ITDT 命令支持

根据代码中的 ITDT 命令列表，以下命令与 MAM 和卷标相关：

### 已实现的命令
- ✅ `inq` - 查询设备信息（可能包含序列号）
- ✅ `inqj` - 查询设备信息（JSON格式）
- ✅ `vpd` - 显示重要产品数据（VPD 可能包含序列号）
- ✅ `devinfo` - 获取设备详细信息

### 未实现的命令（高级命令）
- ⚠️ `readattr` - 读取 MAM 属性（**关键命令**）
  - 用途: 读取磁带的 MAM 属性，包括序列号、二维码等信息
  - 状态: 在命令列表中列出，但未实现具体功能

## libltfs.dll 功能推测

基于 LTFS 标准和 IBM 磁带机的特性，`libltfs.dll` 应该包含以下类型的函数：

### 1. 卷标相关函数
- `ltfs_*_volume_label*` - 设置/读取卷标
- `ltfs_*_metadata*` - 元数据操作（卷标存储在元数据中）

### 2. MAM 相关函数
- `ltfs_*_mam*` - MAM 读写操作
- `ltfs_*_attribute*` - 属性读写（MAM 属性）
- `ltfs_read_attribute*` - 读取属性
- `ltfs_write_attribute*` - 写入属性

### 3. 磁带信息函数
- `ltfs_*_tape_info*` - 磁带信息查询
- `ltfs_*_serial*` - 序列号操作

## 建议实现的功能

### 1. ITDT readattr 命令封装
```python
async def read_mam_attributes_itdt(self) -> Dict[str, Any]:
    """使用ITDT读取MAM属性"""
    cmd = [self.itdt_path, '-f', self.tape_drive, 'readattr']
    return await self.run_command(cmd, timeout=30, tool_type="ITDT")
```

### 2. ITDT writeattr 命令封装（如果支持）
```python
async def write_mam_attribute_itdt(self, attribute_name: str, value: str) -> Dict[str, Any]:
    """使用ITDT写入MAM属性"""
    cmd = [self.itdt_path, '-f', self.tape_drive, 'writeattr', attribute_name, value]
    return await self.run_command(cmd, timeout=30, tool_type="ITDT")
```

### 3. 解析 MAM 属性
- 序列号（Serial Number）
- 二维码（Barcode）
- 制造商信息（Manufacturer）
- 生产日期（Manufactured Date）
- 其他 MAM 属性

## 当前系统状态

### 已支持的功能
1. ✅ **设置卷标**: 通过 `LtfsCmdFormat.exe` 格式化时设置
2. ✅ **读取卷标**: 通过 Windows `fsutil` 命令读取
3. ✅ **设置序列号**: 格式化时通过 `/S:` 参数设置（6位）

### 未支持的功能
1. ❌ **读取序列号**: 从 MAM 中读取
2. ❌ **读取二维码**: 从 MAM 中读取
3. ❌ **写入 MAM 属性**: 设置序列号、二维码等
4. ❌ **直接调用 libltfs.dll API**: 需要通过命令行工具间接使用

## 实现建议

### 短期方案（推荐）
使用 ITDT 命令行工具：
1. 实现 `readattr` 命令封装，读取 MAM 属性
2. 解析 MAM 属性输出，提取序列号、二维码等信息
3. 如果 ITDT 支持 `writeattr`，实现写入功能

### 长期方案
直接调用 libltfs.dll API：
1. 使用 `ctypes` 或 `cffi` 加载 libltfs.dll
2. 解析导出函数名称（可能需要反编译或查看官方文档）
3. 调用相应的 API 函数读取/写入 MAM 属性

## 相关资源

- **ITDT 文档**: IBM Tape Diagnostic Tool 官方文档
- **LTFS 规范**: Linear Tape File System 规范文档
- **MAM 规范**: Media Auxiliary Memory 规范（SCSI 标准）
- **libltfs.dll**: IBM LTFS 核心库（586 个导出函数）

## 注意事项

1. **libltfs.dll 导出函数名称**: 由于函数名称在分析时显示为乱码，可能需要：
   - 查看 IBM 官方文档
   - 使用反编译工具分析
   - 通过函数序号调用（如果知道序号）

2. **MAM 属性格式**: MAM 属性遵循 SCSI 标准格式，需要正确解析

3. **权限要求**: 读取/写入 MAM 属性可能需要管理员权限

4. **兼容性**: 不同代数的 LTO 磁带可能支持不同的 MAM 属性

