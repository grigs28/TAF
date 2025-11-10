# 磁带工具配置说明

## 配置文件位置
`config/settings.py`

## 关键配置项

### 1. ITDT工具配置
```python
ITDT_PATH: str = "D:\\APP\\TAF\\ITDT\\itdt.exe"
```
- ITDT可执行文件的完整路径
- 示例：`D:\APP\TAF\ITDT\itdt.exe`

### 2. LTFS工具目录配置
```python
LTFS_TOOLS_DIR: str = "D:\\APP\\TAF\\ITDT"
```
- **重要**：LTFS命令必须在它们的程序目录下执行
- 该目录应包含以下工具：
  - `LtfsCmdAssign.exe` - 分配盘符
  - `LtfsCmdUnassign.exe` - 卸载盘符
  - `LtfsCmdLoad.exe` - 加载磁带
  - `LtfsCmdEject.exe` - 弹出磁带
  - `LtfsCmdFormat.exe` - 格式化磁带
  - `LtfsCmdUnformat.exe` - 取消格式化
  - `LtfsCmdCheck.exe` - 检查完整性
  - `LtfsCmdRollback.exe` - 回滚
  - `LtfsCmdDrives.exe` - 列出驱动器
  - `mkltfs.exe` - 格式化工具

### 3. 盘符配置
```python
TAPE_DRIVE_LETTER: str = "O"
```
- **注意**：使用大写字母，不带冒号
- LTFS命令使用：`O` （不带冒号）
- Windows系统命令使用：`O:` （带冒号，程序会自动添加）

## 工作目录说明

### ITDT命令
- 工作目录：从 `ITDT_PATH` 自动派生
- 示例：如果 `ITDT_PATH = "D:\APP\TAF\ITDT\itdt.exe"`
  - 工作目录为：`D:\APP\TAF\ITDT`

### LTFS命令
- 工作目录：使用 `LTFS_TOOLS_DIR` 配置
- **必须在LTFS程序目录下执行**，否则可能找不到依赖的DLL文件

## 命令示例

### 列出驱动器
```bash
# 在 D:\APP\TAF\ITDT 目录下执行
LtfsCmdDrives.exe
```
输出示例：
```
Assigned   Address      Serial                   Status
---------- ------------ ------------------------ --------------------
O          0.0.24.0     10WT036260               LTFS_MEDIA
```

### 分配盘符
```bash
# 在 D:\APP\TAF\ITDT 目录下执行
LtfsCmdAssign.exe 0.0.24.0 O
```
- 参数1：驱动器地址（如 `0.0.24.0`）
- 参数2：盘符（大写，不带冒号，如 `O`）

### 格式化磁带
```bash
# 在 D:\APP\TAF\ITDT 目录下执行
LtfsCmdFormat.exe 0.0.24.0 /N:BK251107_1200 /S:ABC123 /E
```
- `/N:卷标名称`
- `/S:序列号`（6位大写字母数字）
- `/E` - 格式化后弹出

## 常见问题

### Q: LTFS命令执行失败，提示找不到DLL？
**A**: 确保 `LTFS_TOOLS_DIR` 配置正确，指向LTFS工具所在目录。LTFS命令必须在它们自己的程序目录下运行。

### Q: 盘符应该用 O 还是 O: ？
**A**: 
- LTFS命令参数使用：`O` （不带冒号）
- Windows系统命令（如fsutil）使用：`O:` （带冒号）
- 配置文件中只需设置 `O`，程序会自动处理

### Q: ITDT和LTFS工具可以在不同目录吗？
**A**: 可以！分别配置 `ITDT_PATH` 和 `LTFS_TOOLS_DIR` 即可。

## 验证配置

访问 http://localhost:8081/tools 页面，点击"检查工具"按钮，系统会显示：
- ITDT工具是否可用
- 每个LTFS工具是否可用
- 工具的完整路径

全部显示为绿色 ✓ 表示配置正确。

