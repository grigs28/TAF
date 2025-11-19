# 企业级磁带备份系统 (TAF)
Enterprise Tape Backup System

[![Python Version](https://img.shields.io/badge/python-3.8+-blue.svg)](https://www.python.org/downloads/)
[![License](https://img.shields.io/badge/license-MIT-green.svg)](LICENSE)
[![Platform](https://img.shields.io/badge/platform-Windows%20%7C%20Linux-lightgrey.svg)]()

## 📋 项目简介

企业级磁带备份系统（TAF - Tape Archive File）是一个基于 Python 开发的现代化磁带备份解决方案，专为企业级数据备份和归档场景设计。系统采用分层架构，支持 Windows 和 Linux（openEuler）双平台部署，提供完整的 Web 管理界面和 RESTful API。

### 核心特性

- 🔄 **智能备份策略** - 支持完整备份、增量备份、差异备份、镜像备份、归档备份
- 🗜️ **多压缩算法** - 支持 PGZip、7-Zip、Tar、Zstandard 等多种压缩方法
- 📼 **磁带生命周期管理** - 自动管理磁带库存、格式化、擦除、过期检测
- 📅 **计划任务调度** - 支持每日、每周、每月、每年定时备份任务
- 🌐 **现代化 Web 界面** - 深色科技主题，响应式设计，实时进度监控
- 🗄️ **多数据库支持** - SQLite（开发/测试）、PostgreSQL、openGauss（推荐）、MySQL
- ⚡ **高性能架构** - 原生 SQL 查询、连接池、异步处理、批量操作
- 🔔 **钉钉通知集成** - 实时推送备份状态、错误告警、任务完成通知
- 🔧 **ITDT/SCSI 双接口** - 支持 IBM Tape Diagnostic Tool 和原生 SCSI 接口
- 📊 **详细日志系统** - 系统日志、操作日志、性能监控、错误追踪

## 🚀 快速开始

### 系统要求

- **Python**: 3.8 或更高版本
- **操作系统**: Windows 10/11 或 Linux (openEuler/Ubuntu/CentOS)
- **数据库**: SQLite（开发）或 openGauss/PostgreSQL（生产）
- **磁带设备**: 支持 SCSI 或 ITDT 接口的磁带驱动器
- **内存**: 建议 4GB 以上
- **磁盘空间**: 建议 10GB 以上（用于临时文件和日志）

### 安装步骤

#### 1. 克隆项目

```bash
git clone https://github.com/grigs28/TAF.git
cd TAF
```

#### 2. 创建 Python 环境

**使用 Conda（推荐）:**

```bash
conda create -n taf python=3.9
conda activate taf
```

**或使用 venv:**

```bash
python -m venv venv
# Windows
venv\Scripts\activate
# Linux/Mac
source venv/bin/activate
```

#### 3. 安装依赖

```bash
pip install -r requirements.txt
```

#### 4. 配置环境变量

复制环境配置文件模板：

```bash
cp .env.sample .env
```

编辑 `.env` 文件，配置数据库连接等参数：

```ini
# 数据库配置（openGauss 示例）
DATABASE_URL=opengauss://username:password@localhost:5432/backup_db
DB_HOST=localhost
DB_PORT=5432
DB_USER=username
DB_PASSWORD=password
DB_DATABASE=backup_db

# 或使用 SQLite（开发/测试）
# DATABASE_URL=sqlite:///./data/taf_backup.db

# Web 服务配置
WEB_PORT=8080
WEB_HOST=0.0.0.0

# 压缩配置
COMPRESSION_METHOD=pgzip  # pgzip, py7zr, 7zip_command, tar, zstd
COMPRESSION_THREADS=4

# 磁带配置
TAPE_INTERFACE_TYPE=itdt  # itdt 或 scsi
ITDT_PATH=c:\itdt\itdt.exe  # Windows
# ITDT_PATH=/usr/local/itdt/itdt  # Linux

# 钉钉通知（可选）
DINGTALK_API_URL=http://localhost:5555
DINGTALK_API_KEY=your-api-key
```

#### 5. 初始化数据库

**使用 openGauss（推荐）:**

```sql
CREATE DATABASE backup_db;
CREATE USER username WITH PASSWORD 'password';
GRANT ALL PRIVILEGES ON DATABASE backup_db TO username;
```

**使用 SQLite:**

系统会自动创建数据库文件，无需手动初始化。

#### 6. 启动系统

```bash
# 激活环境（如果使用 conda）
conda activate taf

# 启动主程序
python main.py
```

#### 7. 访问 Web 界面

打开浏览器访问：`http://localhost:8080`

默认无需登录（开发模式），生产环境请配置认证。

## 📁 项目结构

```
TAF/
├── main.py                      # 主程序入口
├── requirements.txt             # Python 依赖包
├── .env.sample                  # 环境配置模板
├── CHANGELOG.md                 # 版本更新日志
│
├── config/                      # 配置管理
│   ├── settings.py              # 系统配置类
│   ├── database.py              # 数据库管理器
│   ├── database_init.py         # 数据库初始化
│   ├── sqlite_init.py           # SQLite 初始化
│   └── config_manager.py        # 配置管理器
│
├── models/                      # 数据模型
│   ├── backup.py                # 备份任务、备份集模型
│   ├── tape.py                  # 磁带模型
│   ├── user.py                  # 用户模型
│   ├── system_log.py            # 系统日志模型
│   └── scheduled_task.py        # 计划任务模型
│
├── backup/                      # 备份处理模块
│   ├── backup_engine.py          # 备份引擎（主控制器）
│   ├── backup_db.py              # 备份数据库操作
│   ├── backup_scanner.py         # 文件扫描器
│   ├── backup_task_manager.py    # 备份任务管理器
│   ├── compression_worker.py    # 压缩工作线程
│   ├── compressor.py             # 压缩处理器
│   ├── file_move_worker.py       # 文件移动工作线程
│   ├── memory_db_writer.py       # 内存数据库写入器
│   ├── sqlite_backup_db.py       # SQLite 备份数据库操作
│   ├── tape_file_mover.py        # 磁带文件移动器
│   └── tape_handler.py           # 磁带处理器
│
├── recovery/                     # 恢复处理模块
│   └── recovery_engine.py        # 恢复引擎
│
├── tape/                         # 磁带管理模块
│   ├── tape_manager.py           # 磁带管理器
│   ├── tape_operations.py        # 磁带操作
│   ├── scsi_interface.py         # SCSI 接口
│   ├── itdt_interface.py          # ITDT 接口
│   └── tape_cartridge.py         # 磁带盒类
│
├── web/                          # Web 应用
│   ├── app.py                    # FastAPI 应用入口
│   ├── api/                      # RESTful API
│   │   ├── backup.py             # 备份管理 API
│   │   ├── recovery.py           # 恢复管理 API
│   │   ├── scheduler.py          # 计划任务 API
│   │   ├── backup_statistics.py  # 备份统计 API
│   │   ├── tape/                 # 磁带管理 API
│   │   │   ├── crud.py           # 磁带 CRUD
│   │   │   ├── operations.py     # 磁带操作
│   │   │   └── device.py         # 设备管理
│   │   └── system/               # 系统管理 API
│   │       ├── database.py       # 数据库配置
│   │       ├── logs.py           # 日志查询
│   │       └── statistics.py     # 系统统计
│   ├── templates/                # HTML 模板
│   ├── static/                   # 静态资源（CSS/JS/图片）
│   └── middleware/               # 中间件
│       ├── auth_middleware.py    # 认证中间件
│       └── logging_middleware.py # 日志中间件
│
├── utils/                        # 工具模块
│   ├── logger.py                 # 日志管理器
│   ├── log_utils.py              # 日志工具函数
│   ├── scheduler/                # 计划任务调度器
│   │   ├── scheduler.py          # 任务调度器
│   │   ├── task_storage.py       # 任务存储（openGauss）
│   │   ├── sqlite_task_storage.py # 任务存储（SQLite）
│   │   └── task_executor.py      # 任务执行器
│   ├── dingtalk_notifier.py      # 钉钉通知器
│   └── db_utils.py               # 数据库工具函数
│
├── mcp/                          # 核心备份模块
│   └── core.py                   # 核心备份处理器
│
├── tests/                        # 测试用例
├── docs/                         # 文档目录
├── data/                         # 数据目录（SQLite 数据库文件）
├── logs/                         # 日志目录
└── temp/                         # 临时文件目录
```

## 🔧 配置说明

### 数据库配置

系统支持多种数据库类型，推荐使用 **openGauss**（生产环境）或 **SQLite**（开发/测试）。

#### Web 界面配置（推荐）

1. 启动系统后访问：`http://localhost:8080`
2. 进入"系统设置" → "数据库"选项卡
3. 选择数据库类型并填写连接信息
4. 点击"测试连接"验证配置
5. 点击"保存配置"保存设置

#### 环境变量配置

在 `.env` 文件中配置：

```ini
# openGauss 配置
DATABASE_URL=opengauss://username:password@host:port/database
DB_HOST=192.168.0.36
DB_PORT=5560
DB_USER=username
DB_PASSWORD=password
DB_DATABASE=backup_db
DB_POOL_SIZE=10
DB_MAX_OVERFLOW=20

# SQLite 配置（开发/测试）
# DATABASE_URL=sqlite:///./data/taf_backup.db
```

**注意**: 系统已完全移除 SQLAlchemy ORM，使用原生 SQL：
- **openGauss**: 使用 `asyncpg` 连接池，参数化查询
- **SQLite**: 使用 `aiosqlite` 连接，参数化查询

### 压缩配置

系统支持多种压缩方法，可在 Web 界面或 `.env` 文件中配置：

```ini
# 压缩方法选择
COMPRESSION_METHOD=pgzip  # 可选: pgzip, py7zr, 7zip_command, tar, zstd

# PGZip 配置
PGZIP_BLOCK_SIZE=1M  # 块大小: 1M, 128K, 1G 等

# 7-Zip 配置
SEVENZIP_PATH=C:\Program Files\7-Zip\7z.exe  # Windows
# SEVENZIP_PATH=/usr/bin/7z  # Linux

# Zstandard 配置
ZSTD_THREADS=4  # 压缩线程数

# 压缩线程数
COMPRESSION_THREADS=4
```

**压缩方法说明**:
- **pgzip**: 并行 GZip 压缩，生成 `.tar.gz` 文件（默认，推荐）
- **py7zr**: Python 7-Zip 库，生成 `.7z` 文件
- **7zip_command**: 调用 7-Zip 命令行工具，生成 `.7z` 文件
- **tar**: 仅打包不压缩，生成 `.tar` 文件
- **zstd**: Zstandard 压缩，生成 `.tar.zst` 文件

### 磁带配置

#### ITDT 接口（推荐）

IBM Tape Diagnostic Tool (ITDT) 提供更稳定和标准的磁带操作。

**安装 ITDT**:

- **Windows**: 下载安装包，安装到 `c:\itdt\` 目录
- **Linux**: 下载安装包，安装到 `/usr/local/itdt/` 目录

**配置**:

```ini
TAPE_INTERFACE_TYPE=itdt
ITDT_PATH=c:\itdt\itdt.exe  # Windows
# ITDT_PATH=/usr/local/itdt/itdt  # Linux
ITDT_LOG_LEVEL=Information  # Errors|Warnings|Information|Debug
```

#### SCSI 接口

直接使用 SCSI 命令操作磁带设备：

```ini
TAPE_INTERFACE_TYPE=scsi
TAPE_DRIVE_LETTER=O:  # Windows 磁带驱动器盘符
DEFAULT_BLOCK_SIZE=256KB
MAX_VOLUME_SIZE=300GB
```

### 钉钉通知配置

```ini
DINGTALK_API_URL=http://localhost:5555
DINGTALK_API_KEY=your-api-key
DINGTALK_DEFAULT_PHONE=13800000000
```

## 🎯 主要功能

### 备份管理

- ✅ **创建备份任务** - 支持完整、增量、差异、镜像、归档备份
- ✅ **实时进度监控** - Web 界面实时显示扫描、压缩、写入进度
- ✅ **文件分组压缩** - 智能文件分组，单包不超过配置大小
- ✅ **多源路径支持** - 支持同时备份多个源路径
- ✅ **排除模式** - 支持通配符排除不需要的文件
- ✅ **后台扫描** - 扫描与压缩并行执行，提升效率
- ✅ **内存数据库** - 使用内存数据库加速文件扫描和同步

### 恢复管理

- ✅ **备份集搜索** - 按时间、标签、任务名称搜索备份集
- ✅ **文件树浏览** - 可视化浏览备份文件结构
- ✅ **选择性恢复** - 支持选择单个或多个文件/目录恢复
- ✅ **恢复进度监控** - 实时显示恢复进度和速度
- ✅ **自动解压** - 根据文件扩展名自动选择解压方法

### 磁带管理

- ✅ **磁带库存管理** - 完整的磁带信息记录和查询
- ✅ **自动格式化** - 创建磁带时自动格式化（可选）
- ✅ **标签管理** - 自动生成和管理磁带标签（TPYYYYMMNN 格式）
- ✅ **生命周期管理** - 自动检测过期磁带，支持自动擦除
- ✅ **健康监控** - 记录磁带健康分数和使用统计
- ✅ **设备扫描** - 自动扫描和识别磁带设备

### 计划任务

- ✅ **定时备份** - 支持每日、每周、每月、每年定时执行
- ✅ **任务模板** - 创建备份任务模板，快速创建任务
- ✅ **任务历史** - 记录任务执行历史和统计信息
- ✅ **任务管理** - 支持启用/禁用、立即执行、删除任务

### 系统监控

- ✅ **系统统计** - 备份任务统计、磁带库存统计、存储使用统计
- ✅ **操作日志** - 详细记录所有操作和状态变更
- ✅ **系统日志** - 系统运行日志、错误日志、性能日志
- ✅ **实时监控** - Web 界面实时显示系统状态

## 🔌 API 接口

系统提供完整的 RESTful API 接口：

### 备份管理

- `GET /api/backup/tasks` - 获取备份任务列表
- `POST /api/backup/tasks` - 创建备份任务
- `GET /api/backup/tasks/{task_id}` - 获取任务详情
- `PUT /api/backup/tasks/{task_id}` - 更新任务
- `DELETE /api/backup/tasks/{task_id}` - 删除任务
- `POST /api/backup/tasks/{task_id}/run` - 立即执行任务
- `POST /api/backup/tasks/{task_id}/cancel` - 取消任务

### 恢复管理

- `GET /api/recovery/sets` - 搜索备份集
- `GET /api/recovery/sets/{set_id}/files` - 获取备份集文件列表
- `GET /api/recovery/sets/{set_id}/groups` - 获取备份组列表
- `POST /api/recovery/restore` - 创建恢复任务

### 磁带管理

- `GET /api/tape/list` - 获取磁带列表
- `POST /api/tape/create` - 创建磁带记录
- `PUT /api/tape/update/{tape_id}` - 更新磁带信息
- `GET /api/tape/show/{tape_id}` - 获取磁带详情
- `POST /api/tape/format` - 格式化磁带
- `POST /api/tape/erase` - 擦除磁带
- `GET /api/tape/inventory` - 获取磁带库存统计

### 计划任务

- `GET /api/scheduler/tasks` - 获取计划任务列表
- `POST /api/scheduler/tasks` - 创建计划任务
- `PUT /api/scheduler/tasks/{task_id}` - 更新计划任务
- `DELETE /api/scheduler/tasks/{task_id}` - 删除计划任务
- `POST /api/scheduler/tasks/{task_id}/run` - 立即执行任务
- `POST /api/scheduler/tasks/{task_id}/enable` - 启用任务
- `POST /api/scheduler/tasks/{task_id}/disable` - 禁用任务

### 系统管理

- `GET /api/system/statistics` - 获取系统统计信息
- `GET /api/system/logs` - 查询系统日志
- `GET /api/system/database/config` - 获取数据库配置
- `PUT /api/system/database/config` - 更新数据库配置
- `POST /api/system/database/test` - 测试数据库连接

详细的 API 文档请参考代码中的接口定义或使用 FastAPI 自动生成的文档：`http://localhost:8080/docs`

## 🧪 测试

运行测试套件：

```bash
# 运行所有测试
pytest

# 运行特定测试文件
pytest tests/test_backup.py

# 运行测试并生成覆盖率报告
pytest --cov=. --cov-report=html

# 运行测试并显示详细输出
pytest -v
```

## 🐛 故障排除

### 常见问题

#### 1. 数据库连接失败

**问题**: 启动时提示数据库连接失败

**解决方案**:
- 检查数据库服务是否运行
- 验证 `.env` 文件中的连接参数
- 使用 Web 界面的"测试连接"功能验证配置
- 检查防火墙和网络连接
- 确认数据库用户权限

#### 2. 磁带设备无法识别

**问题**: 系统无法检测到磁带设备

**解决方案**:
- 检查磁带驱动器物理连接
- 确认设备驱动已正确安装
- 检查设备权限（Linux 需要用户组权限）
- 尝试使用 ITDT 接口（更稳定）
- 查看系统日志获取详细错误信息

#### 3. 压缩失败

**问题**: 压缩过程中出现错误

**解决方案**:
- 检查临时目录权限和磁盘空间
- 验证压缩工具安装（7-Zip、Zstandard 等）
- 检查 `COMPRESSION_METHOD` 配置是否正确
- 查看压缩日志获取详细错误信息
- 尝试切换压缩方法（如从 7zip 切换到 pgzip）

#### 4. 同步持续时间显示为 0.0 秒

**问题**: 内存数据库同步时显示持续时间为 0.0 秒

**解决方案**:
- 此问题已在 v0.1.20 版本修复
- 确保使用最新版本代码
- 检查 `backup/memory_db_writer.py` 中的 `_sync_start_time` 设置

#### 5. SQLAlchemy 相关错误

**问题**: 提示 SQLAlchemy 相关错误

**解决方案**:
- 系统已完全移除 SQLAlchemy ORM，使用原生 SQL
- 如果遇到相关错误，请检查代码是否使用了旧的 ORM 方法
- 确保使用 `get_opengauss_connection()` 或 `get_sqlite_connection()` 获取连接

## 📚 文档

- [系统架构文档](docs/系统架构.md) - 详细的系统架构说明
- [使用说明](docs/使用说明.md) - 用户使用指南
- [开发说明](docs/开发说明.md) - 开发者指南
- [数据库配置说明](docs/数据库配置说明.md) - 数据库配置详细说明
- [ITDT集成方案](docs/ITDT集成方案.md) - ITDT 集成详细说明
- [版本更新日志](CHANGELOG.md) - 完整的版本更新历史

## 🛠️ 开发指南

### 添加新的压缩方法

1. 在 `backup/compressor.py` 中添加新的压缩函数
2. 在 `config/settings.py` 中添加配置项
3. 在 Web 界面的压缩配置中添加选项
4. 更新恢复引擎以支持解压

### 添加新的 API 接口

1. 在 `web/api/` 目录下创建或修改 API 文件
2. 使用 FastAPI 定义路由和请求/响应模型
3. 在 `web/app.py` 中注册路由
4. 添加相应的前端页面（如需要）

### 数据库操作规范

**重要**: 系统已完全移除 SQLAlchemy ORM，所有数据库操作必须使用原生 SQL。

**openGauss 示例**:

```python
from utils.scheduler.db_utils import get_opengauss_connection

async with get_opengauss_connection() as conn:
    row = await conn.fetchrow(
        "SELECT * FROM backup_tasks WHERE id = $1",
        task_id
    )
```

**SQLite 示例**:

```python
from utils.scheduler.sqlite_utils import get_sqlite_connection

async with get_sqlite_connection() as conn:
    cursor = await conn.execute(
        "SELECT * FROM backup_tasks WHERE id = ?",
        (task_id,)
    )
    row = await cursor.fetchone()
```

## 🤝 贡献指南

欢迎贡献代码！请遵循以下步骤：

1. Fork 本项目
2. 创建特性分支 (`git checkout -b feature/AmazingFeature`)
3. 提交更改 (`git commit -m '添加新功能: AmazingFeature'`)
4. 推送到分支 (`git push origin feature/AmazingFeature`)
5. 创建 Pull Request

### 代码规范

- 使用 Python 3.8+ 语法
- 遵循 PEP 8 代码风格
- 添加适当的注释和文档字符串
- 编写单元测试
- 确保所有测试通过

## 📄 许可证

本项目采用 MIT 许可证。详见 [LICENSE](LICENSE) 文件。

## 📞 联系方式

- **项目地址**: https://github.com/grigs28/TAF
- **问题反馈**: 请在 GitHub Issues 中提交
- **功能建议**: 欢迎提交 Pull Request

## 🎉 致谢

感谢所有为本项目做出贡献的开发者和用户！

---

**企业级磁带备份系统** - 让数据备份更简单、更可靠、更高效

版本: v0.1.20 | 最后更新: 2025-11-19
