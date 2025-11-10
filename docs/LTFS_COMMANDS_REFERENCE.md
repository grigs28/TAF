# LTFS 命令参数参考

## 命令分类

### 1. 驱动器管理命令

#### LtfsCmdDrives.exe
**功能**: 列出所有LTFS驱动器
**参数**: 无
```bash
LtfsCmdDrives.exe
```
**输出示例**:
```
Assigned   Address      Serial                   Status
---------- ------------ ------------------------ --------------------
O          0.0.24.0     10WT036260               LTFS_MEDIA
```

---

### 2. 物理操作命令（使用驱动器地址）

#### LtfsCmdLoad.exe
**功能**: 物理加载磁带到驱动器
**参数**: `drive_id` (驱动器地址)
```bash
LtfsCmdLoad.exe 0.0.24.0
```

#### LtfsCmdEject.exe
**功能**: 物理弹出磁带
**参数**: `drive_id` (驱动器地址)
```bash
LtfsCmdEject.exe 0.0.24.0
```

---

### 3. 格式化命令（使用驱动器地址）

#### LtfsCmdFormat.exe
**功能**: 格式化磁带为LTFS格式
**参数**: `drive_id` + 可选参数
```bash
# 基本格式化
LtfsCmdFormat.exe 0.0.24.0

# 设置卷标
LtfsCmdFormat.exe 0.0.24.0 /N:BK251107_1200

# 设置序列号（6位大写字母数字）
LtfsCmdFormat.exe 0.0.24.0 /N:BK251107_1200 /S:ABC123

# 格式化后弹出
LtfsCmdFormat.exe 0.0.24.0 /N:BK251107_1200 /E
```
**参数说明**:
- `/N:卷标名称` - 设置卷标
- `/S:序列号` - 序列号（必须6位大写字母数字）
- `/E` - 格式化后弹出磁带

#### mkltfs.exe
**功能**: 使用mkltfs工具格式化磁带
**参数**: `-d device_id --force` + 可选参数
```bash
# 基本格式化
mkltfs.exe -d 0.0.24.0 --force

# 设置卷标
mkltfs.exe -d 0.0.24.0 --force --volume-name BK251107_1200
```

---

### 4. 挂载/卸载命令

#### LtfsCmdAssign.exe ⚠️ 重要
**功能**: 分配磁带到Windows盘符
**参数**: `drive_id` + `盘符（带冒号）`
```bash
LtfsCmdAssign.exe 0.0.24.0 O:
```
**注意**: 
- 第一个参数：驱动器地址（如 `0.0.24.0`）
- 第二个参数：盘符**带冒号**（如 `O:`）

#### LtfsCmdUnassign.exe
**功能**: 从盘符卸载磁带
**参数**: `drive_id` (驱动器地址)
```bash
LtfsCmdUnassign.exe 0.0.24.0
```

---

### 5. 维护命令（使用驱动器地址）

#### LtfsCmdCheck.exe
**功能**: 检查磁带完整性
**参数**: `drive_id` (驱动器地址)
```bash
LtfsCmdCheck.exe 0.0.24.0
```
**注意**: 可能需要2小时以上

#### LtfsCmdRollback.exe
**功能**: 回滚到上一个一致性点
**参数**: `drive_id` (驱动器地址)
```bash
LtfsCmdRollback.exe 0.0.24.0
```

#### LtfsCmdUnformat.exe
**功能**: 取消LTFS格式化
**参数**: `drive_id` (驱动器地址)
```bash
LtfsCmdUnformat.exe 0.0.24.0
```

---

## 参数格式总结

### 驱动器地址 (drive_id)
- **格式**: `0.0.24.0` (SCSI地址: 总线.目标.LUN.子LUN)
- **用途**: 所有LTFS命令的主要参数
- **获取方式**: 运行 `LtfsCmdDrives.exe` 查看 "Address" 列

### 盘符 (drive_letter)
- **格式**: `O:` (大写字母 + 冒号)
- **用途**: 仅用于 `LtfsCmdAssign.exe` 命令
- **示例**: `O:`, `P:`, `T:`

### 格式化方式对比

| 命令 | 参数格式 | 卷标设置 | 序列号 | 弹出 | 速度 |
|------|---------|---------|--------|------|------|
| LtfsCmdFormat.exe | `drive_id /N:label /S:serial /E` | ✅ | ✅ | ✅ | 快 |
| mkltfs.exe | `-d device_id --force --volume-name label` | ✅ | ❌ | ❌ | 慢 |

**推荐**: 使用 `LtfsCmdFormat.exe`，功能更完整，速度更快。

---

## 完整挂载流程示例

```bash
# 1. 查看驱动器
LtfsCmdDrives.exe

# 2. 物理加载磁带
LtfsCmdLoad.exe 0.0.24.0

# 3. 格式化磁带（可选）
LtfsCmdFormat.exe 0.0.24.0 /N:BK251107_1200

# 4. 分配到盘符
LtfsCmdAssign.exe 0.0.24.0 O:

# 现在可以在 O: 盘访问磁带了
```

## 完整卸载流程示例

```bash
# 1. 从盘符卸载
LtfsCmdUnassign.exe 0.0.24.0

# 2. 物理弹出
LtfsCmdEject.exe 0.0.24.0
```

