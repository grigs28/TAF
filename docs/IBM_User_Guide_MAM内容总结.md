# IBM Tape Device Drivers User's Guide 中关于 MAM 的内容总结

## 概述

根据 `IBM_Tape_Device_Drivers_and_Diagnostic_Tool_User_s_Guide_20251029_251106_055726.md` 文档，MAM (Media Auxiliary Memory，媒体辅助内存) 是磁带盒上的非易失性存储区域，用于存储磁带的元数据信息。

## MAM 相关命令

### 1. readattr - 读取 MAM 属性

**位置**: 第 4809-4821 行

**功能**: 从磁带盒的 MAM 中读取属性到指定的目标文件。

**命令格式**:
```
readattr -p[0|1|2|3] -a Identifier -d DestinationPathFile
```

**参数说明**:
- `-p`: 分区参数，范围 0-3（必需）
- `-a`: 属性标识符，十六进制值（必需）
- `-d`: 目标文件路径（必需）

**平台支持**: 所有平台

**注意事项**:
- 仅支持磁带设备
- 需要已加载的磁带盒

**示例**:
```bash
itdt -f \\.\tape0 readattr -p0 -a0x0002 -dserial_number.bin
```

### 2. writeattr - 写入 MAM 属性

**位置**: 第 5062-5072 行

**功能**: 从源文件写入 MAM 属性到磁带盒。

**命令格式**:
```
writeattr -p[0|1|2|3] -a Identifier -s SourcePathFile
```

**参数说明**:
- `-p`: 分区参数，范围 0-3（必需）
- `-a`: 属性标识符，十六进制值（必需）
- `-s`: 源文件路径（必需）

**平台支持**: 所有平台

**注意事项**:
- 仅支持磁带设备
- 需要已加载的磁带盒

**示例**:
```bash
itdt -f \\.\tape0 writeattr -p0 -a0x0002 -sserial_number.bin
```

### 3. qrymon / qrymediaoptimizationneeded - 查询媒体优化需求

**位置**: 第 4745-4755 行

**功能**: 查询磁带盒的 MAM，检查是否需要媒体优化。

**命令格式**:
```
qrymon
```

**参数**: 无

**平台支持**: 仅 LTO-9

**注意事项**:
- 仅支持磁带设备
- 需要已加载的磁带盒

### 4. qrymov / qrymediaoptimizationversion - 查询媒体优化版本

**位置**: 第 4757-4767 行

**功能**: 查询磁带盒的 MAM，显示媒体优化版本。

**命令格式**:
```
qrymov
```

**参数**: 无

**平台支持**: 仅 LTO-9

**注意事项**:
- 仅支持磁带设备
- 需要已加载的磁带盒

## MAM 属性标识符

根据文档和代码实现，常见的 MAM 属性标识符包括：

| 标识符 | 名称 | 说明 |
|--------|------|------|
| 0x0001 | Media Manufacturer | 制造商信息 |
| 0x0002 | Media Serial Number | 序列号 |
| 0x0009 | Media Barcode | 二维码/条码 |

## 分区说明

MAM 支持 4 个分区（0-3）：
- **分区 0**: 通常用于主要属性（序列号、制造商等）
- **分区 1-3**: 用于其他特定用途的属性

## 使用场景

### 1. 读取序列号
```bash
# 读取序列号（属性 0x0002）
itdt -f \\.\tape0 readattr -p0 -a0x0002 -dserial.bin
```

### 2. 读取制造商信息
```bash
# 读取制造商信息（属性 0x0001）
itdt -f \\.\tape0 readattr -p0 -a0x0001 -dmanufacturer.bin
```

### 3. 读取二维码
```bash
# 读取二维码（属性 0x0009）
itdt -f \\.\tape0 readattr -p0 -a0x0009 -dbarcode.bin
```

### 4. 写入序列号
```bash
# 写入序列号（需要先创建包含序列号的文件）
echo -n "TP1101" > serial.bin
itdt -f \\.\tape0 writeattr -p0 -a0x0002 -sserial.bin
```

## 与当前实现的对比

### 已实现的功能 ✅

1. **read_mam_attributes_itdt()** (`utils/tape_tools.py:257-424`)
   - ✅ 支持读取 MAM 属性
   - ✅ 支持指定分区（0-3）
   - ✅ 支持指定属性标识符
   - ✅ 自动解析常见属性（序列号、制造商、二维码）
   - ✅ 支持多种编码格式解析

### 未实现的功能 ❌

1. **write_mam_attribute_itdt()**
   - ❌ 写入 MAM 属性的功能尚未实现
   - 需要实现 `writeattr` 命令封装

2. **qrymon / qrymov**
   - ❌ LTO-9 媒体优化查询功能未实现

## 实现建议

### 1. 实现 writeattr 命令封装

```python
async def write_mam_attribute_itdt(
    self, 
    partition: int = 0,
    attribute_id: str,
    source_file: str
) -> Dict[str, Any]:
    """使用ITDT写入MAM属性
    
    Args:
        partition: 分区号（0-3）
        attribute_id: 属性标识符（如 "0x0002"）
        source_file: 源文件路径（包含要写入的数据）
    
    Returns:
        包含操作结果的字典
    """
    logger.info(f"使用ITDT写入MAM属性 (分区: {partition}, 属性ID: {attribute_id})...")
    
    # 验证分区范围
    if partition not in [0, 1, 2, 3]:
        raise ValueError(f"分区号必须是 0-3 之间的整数，当前值: {partition}")
    
    # 验证源文件存在
    if not os.path.exists(source_file):
        raise FileNotFoundError(f"源文件不存在: {source_file}")
    
    # 构建命令
    cmd = [
        self.itdt_path,
        '-f', self.tape_drive,
        'writeattr',
        f'-p{partition}',
        f'-a{attribute_id}',
        f'-s{source_file}'
    ]
    
    result = await self.run_command(cmd, timeout=60, tool_type="ITDT")
    
    if result.get("success"):
        logger.info(f"MAM属性写入成功: {attribute_id}")
    else:
        logger.error(f"MAM属性写入失败: {result.get('stderr', '未知错误')}")
    
    return result
```

### 2. 实现媒体优化查询（LTO-9）

```python
async def query_media_optimization_needed(self) -> Dict[str, Any]:
    """查询媒体优化需求（LTO-9）"""
    logger.info("查询媒体优化需求...")
    
    cmd = [self.itdt_path, '-f', self.tape_drive, 'qrymon']
    result = await self.run_command(cmd, timeout=30, tool_type="ITDT")
    
    return result

async def query_media_optimization_version(self) -> Dict[str, Any]:
    """查询媒体优化版本（LTO-9）"""
    logger.info("查询媒体优化版本...")
    
    cmd = [self.itdt_path, '-f', self.tape_drive, 'qrymov']
    result = await self.run_command(cmd, timeout=30, tool_type="ITDT")
    
    return result
```

## 注意事项

1. **权限要求**: MAM 读写操作可能需要管理员权限
2. **设备状态**: 操作前需要确保磁带设备已就绪且已加载磁带盒
3. **数据格式**: MAM 属性数据可能是二进制格式，需要正确解析
4. **平台限制**: 某些功能（如媒体优化查询）仅支持特定代数（LTO-9）
5. **分区选择**: 大多数情况下使用分区 0，但某些属性可能存储在其他分区

## 相关文档

- **ITDT 命令参考**: IBM Tape Diagnostic Tool User's Guide
- **MAM 规范**: SCSI Media Auxiliary Memory 标准
- **当前实现**: `utils/tape_tools.py` 中的 `read_mam_attributes_itdt()` 方法

