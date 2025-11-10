# ITDT DLL 功能说明

## 概述

IBM Tape Diagnostic Tool (ITDT) 目录下的 DLL 文件功能说明。这些 DLL 是 IBM LTFS (Linear Tape File System) 和 ITDT 工具的核心组件。

## 主要 DLL 文件功能

### 1. **LtfsApi10.dll**
- **功能**: LTFS API 库（版本 10）
- **用途**: 提供 LTFS 文件系统操作的 API 接口
- **说明**: 可能包含 LTFS 文件系统的核心 API 函数，用于程序化访问 LTFS 功能
- **导出函数**: 无导出函数（可能是静态库或通过其他方式调用）

### 2. **LtfsCmdLib.dll**
- **功能**: LTFS 命令行工具库
- **用途**: 为 LTFS 命令行工具（如 LtfsCmdFormat.exe, LtfsCmdLoad.exe 等）提供底层功能支持
- **说明**: 封装了 LTFS 操作的命令行接口
- **导出函数**: 无导出函数（可能是静态库）

### 3. **LtfsMgmtLib.dll**
- **功能**: LTFS 管理库
- **用途**: 提供 LTFS 管理功能，如配置、监控、状态查询等
- **说明**: 用于 LTFS 管理工具（如 LtfsManager.exe, LtfsMgmtSvc.exe）的核心库
- **导出函数**: 无导出函数（可能是静态库）

### 4. **libltfs.dll**
- **功能**: LTFS 核心库
- **用途**: LTFS 文件系统的核心实现库
- **说明**: 包含 LTFS 文件系统的所有核心功能，如文件读写、目录管理、元数据处理等
- **导出函数**: 586 个导出函数（核心功能库）
- **主要功能**:
  - LTFS 文件系统操作
  - 磁带读写操作
  - 元数据管理
  - 文件系统挂载/卸载
  - 错误处理和日志记录

### 5. **LTFSShellEx.dll**
- **功能**: Windows Shell 扩展
- **用途**: 为 Windows 资源管理器提供 LTFS 磁带驱动器的 Shell 集成
- **说明**: 允许在 Windows 资源管理器中像普通磁盘一样访问 LTFS 磁带
- **导出函数**: 14 个导出函数（Windows Shell 扩展接口）

### 6. **PanelCommon.dll**
- **功能**: 通用面板库
- **用途**: 为 GUI 工具提供通用的 UI 组件
- **说明**: 用于属性面板、版本面板等 GUI 组件的共享库
- **导出函数**: 无导出函数（资源 DLL 或静态库）

### 7. **ltfsusr.dll**
- **功能**: LTFS 用户库
- **用途**: 提供用户级别的 LTFS 功能接口
- **说明**: 可能是 LTFS 用户空间库的 Windows 版本
- **导出函数**: 2 个导出函数

## 支持库 DLL

### 8. **libwinlog.dll**
- **功能**: Windows 日志库
- **用途**: 提供 Windows 平台特定的日志记录功能

### 9. **libwinmsg.dll**
- **功能**: Windows 消息库
- **用途**: 提供 Windows 平台特定的消息处理功能

### 10. **libiconv-2.dll**
- **功能**: 字符编码转换库
- **用途**: 提供字符编码转换功能（如 UTF-8, GBK 等）

### 11. **libxml2.dll**
- **功能**: XML 解析库
- **用途**: 解析和处理 XML 格式的配置文件和数据

### 12. **icuuc48.dll** / **icudt48.dll**
- **功能**: ICU (International Components for Unicode) 库
- **用途**: 提供国际化支持，包括字符编码、日期时间格式化、字符串处理等
- **说明**: 
  - `icuuc48.dll`: ICU 通用组件库
  - `icudt48.dll`: ICU 数据文件库

## LTFS 子目录中的 DLL

### 13. **libiosched-fcfs.dll** / **libiosched-unified.dll**
- **功能**: I/O 调度器库
- **用途**: 
  - `libiosched-fcfs.dll`: 先来先服务 (FCFS) I/O 调度器
  - `libiosched-unified.dll`: 统一 I/O 调度器

### 14. **libkmi-simple.dll**
- **功能**: 密钥管理接口 (KMI) 库
- **用途**: 提供简单的密钥管理功能，用于加密磁带操作

### 15. **libtape-file.dll** / **libtape-scsilib.dll**
- **功能**: 磁带驱动库
- **用途**: 
  - `libtape-file.dll`: 文件接口磁带驱动
  - `libtape-scsilib.dll`: SCSI 接口磁带驱动

## 功能总结

### 核心功能模块

1. **LTFS 文件系统支持**
   - `libltfs.dll`: 核心文件系统实现
   - `LtfsApi10.dll`: API 接口
   - `ltfsusr.dll`: 用户空间库

2. **命令行工具支持**
   - `LtfsCmdLib.dll`: 命令行工具库
   - 支持的工具: Format, Load, Eject, Check, Assign, Unassign 等

3. **管理功能**
   - `LtfsMgmtLib.dll`: 管理功能库
   - `LTFSShellEx.dll`: Windows Shell 集成

4. **平台支持**
   - `libwinlog.dll`, `libwinmsg.dll`: Windows 平台支持
   - `libiconv-2.dll`: 字符编码支持
   - `libxml2.dll`: XML 配置支持
   - ICU 库: 国际化支持

5. **I/O 和驱动**
   - I/O 调度器: 优化磁带读写性能
   - 磁带驱动: 文件接口和 SCSI 接口支持
   - 密钥管理: 加密磁带支持

## 使用场景

这些 DLL 主要用于：

1. **LTFS 磁带格式化**: 使用 `LtfsCmdFormat.exe` 和相关库
2. **磁带文件系统访问**: 通过 `libltfs.dll` 访问磁带上的文件
3. **Windows 集成**: 通过 `LTFSShellEx.dll` 在资源管理器中访问磁带
4. **管理操作**: 通过管理库进行配置和监控
5. **命令行操作**: 通过命令行工具库执行各种磁带操作

## 注意事项

- 这些 DLL 是 IBM ITDT 和 LTFS 工具包的组成部分
- 大多数 DLL 是内部库，不直接对外提供 API
- 主要的使用方式是通过命令行工具（.exe 文件）或 GUI 工具
- `libltfs.dll` 是核心库，包含最多的导出函数（586 个）

## 相关工具

与这些 DLL 配合使用的命令行工具：

- `LtfsCmdFormat.exe`: 格式化磁带为 LTFS 格式
- `LtfsCmdLoad.exe`: 加载磁带
- `LtfsCmdEject.exe`: 弹出磁带
- `LtfsCmdCheck.exe`: 检查磁带
- `LtfsCmdAssign.exe`: 分配驱动器
- `LtfsCmdUnassign.exe`: 取消分配驱动器
- `LtfsCmdRollback.exe`: 回滚操作
- `LtfsCmdUnformat.exe`: 取消格式化
- `LtfsManager.exe`: LTFS 管理器 GUI
- `LtfsMain.exe`: LTFS 主程序

