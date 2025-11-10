# MAM (Media Auxiliary Memory) 属性格式说明

## 概述

MAM (Media Auxiliary Memory) 是磁带盒上的辅助存储区域，用于存储磁带的元数据信息。IBM ITDT工具提供了`readattr`和`writeattr`命令来读写MAM属性。

## 问题

IBM官方文档（`IBM_Tape_Device_Drivers_and_Diagnostic_Tool_User_s_Guide`）中**没有详细说明**：
- MAM属性数据的格式结构
- 字符编码方式（ASCII、EBCDIC、UTF-8等）
- 数据头部结构（长度字段、类型字段等）
- 字符串存储格式（是否以null结尾、填充方式等）

## 标准MAM属性

根据LTO标准，常见的MAM属性包括：

| 属性ID | 名称 | 说明 | 数据类型 |
|--------|------|------|----------|
| 0x0001 | Media Manufacturer | 制造商名称 | ASCII字符串 |
| 0x0002 | Media Serial Number | 序列号 | ASCII字符串 |
| 0x0009 | Media Barcode | 条形码/二维码 | ASCII字符串 |

## 实际测试发现

根据实际测试和SCSI标准，MAM属性数据格式可能包含以下特征：

### 1. 数据格式可能性

**可能性A：纯ASCII字符串**
```
数据 = "TP1101" + null终止符 + 填充字节
```

**可能性B：带长度头的格式**
```
字节0-1: 长度字段（大端序）
字节2+: 实际数据（ASCII字符串）
```

**可能性C：SCSI标准格式**
```
字节0: 属性长度（不包括长度字段本身）
字节1: 属性类型/标志
字节2+: 实际数据
```

### 2. 字符编码

- **ASCII**: 最常用的编码方式，适用于序列号、制造商名称等
- **EBCDIC**: IBM传统编码，但现代LTO磁带通常使用ASCII
- **UTF-8**: 可能用于国际化场景

### 3. 字符串处理

- 可能包含null终止符（`\x00`）
- 可能包含前导/尾随空格
- 可能包含填充字节（通常为`0x00`或`0x20`）

## 当前实现策略

由于IBM文档未提供详细格式说明，我们的实现采用了**多策略解析**方法：

### 1. 多偏移量尝试
```python
# 尝试跳过可能的头部字节（0, 2, 4, 6, 8字节）
for skip_bytes in [0, 2, 4, 6, 8]:
    test_data = mam_data[skip_bytes:]
    # 尝试解析...
```

### 2. 多编码尝试
```python
# 尝试不同的字符编码
for encoding in ['ascii', 'utf-8', 'latin-1']:
    decoded = mam_data.decode(encoding, errors='ignore')
    # 提取可打印字符...
```

### 3. 正则表达式匹配
```python
# 查找连续的可打印字符序列
matches = re.findall(r'[a-zA-Z0-9\-_]{3,}', decoded)
```

### 4. 原始数据显示
```python
# 如果无法解析，显示原始十六进制数据
result["mam_data_raw_hex"] = mam_data.hex()
```

## 建议的改进方向

### 1. 参考SCSI标准文档
- SCSI MAM标准（ANSI INCITS 464）
- LTO Consortium规范

### 2. 实际测试验证
- 使用已知的MAM属性值进行测试
- 对比写入前后的数据格式
- 分析不同LTO代数的格式差异

### 3. 逆向工程
- 分析ITDT工具的输出文件格式
- 对比不同属性的数据格式
- 识别数据结构的模式

## 当前已知信息

### ITDT命令格式
```bash
# 读取MAM属性
itdt.exe -f <drive> readattr -p<partition> -a<attribute_id> -d<output_file>

# 写入MAM属性
itdt.exe -f <drive> writeattr -p<partition> -a<attribute_id> -s<source_file>
```

### 参数说明
- `-p`: 分区号（0-3）
- `-a`: 属性标识符（十六进制，如0x0002）
- `-d`: 输出文件路径（读取时）
- `-s`: 源文件路径（写入时）

### 注意事项
- 命令需要已加载的磁带盒
- 仅支持磁带设备
- 所有参数都是必需的

## 参考资料

1. **IBM ITDT文档**: `IBM_Tape_Device_Drivers_and_Diagnostic_Tool_User_s_Guide`
2. **SCSI标准**: ANSI INCITS 464 (SCSI MAM)
3. **LTO规范**: LTO Consortium Technical Specifications

## 结论

由于IBM文档中**没有提供MAM属性的详细格式说明**，当前实现采用了**自适应解析策略**，通过多种方法尝试解析MAM属性数据。如果解析失败，会显示原始十六进制数据供用户分析。

建议在实际使用中：
1. 记录成功解析的MAM属性数据格式
2. 分析不同LTO代数的格式差异
3. 根据实际数据调整解析逻辑
4. 参考SCSI标准文档获取更详细的格式信息

