# 企业级磁带备份系统
Enterprise Tape Backup System

## 项目概述

企业级磁带备份系统是一个基于Python开发的现代化磁带备份解决方案，支持Windows和openEuler双平台部署。系统采用Web界面管理，支持6个月循环备份体系，集成7-Zip SDK级别压缩和钉钉通知功能。

## 主要特性

- 🔄 **6个月循环备份体系** - 自动管理备份生命周期
- 💾 **多策略备份** - 支持完整备份、增量备份、差异备份
- 🗜️ **7-Zip SDK压缩** - 高效压缩，单包不超过3GB
- 📼 **磁带生命周期管理** - 自动擦除过期磁带
- 🌐 **Web管理界面** - 现代化中文管理界面
- 📱 **钉钉通知集成** - 实时推送备份状态
- 🗄️ **多数据库支持** - SQLite、PostgreSQL、openGauss、MySQL
- 🛠️ **可视化数据库配置** - Web界面配置和测试数据库连接
- 🖥️ **跨平台支持** - Windows + openEuler

## 系统要求

- Python 3.8+
- openGauss数据库
- SCSI磁带驱动器
- 7-Zip SDK

## 快速开始

### 克隆项目

```bash
git clone https://github.com/grigs28/TAF.git
cd TAF
```

### 1. 环境准备

```bash
# 创建conda环境（推荐）
conda create -n taf python=3.9
conda activate taf

# 或使用venv
python -m venv venv
source venv/bin/activate  # Linux/Mac
# 或
venv\Scripts\activate  # Windows

# 安装依赖
pip install -r requirements.txt
```

### 2. 配置数据库

#### 使用openGauss（推荐）
确保openGauss数据库运行并创建所需数据库：

```sql
CREATE DATABASE backup_db;
CREATE USER username WITH PASSWORD 'password';
GRANT ALL PRIVILEGES ON DATABASE backup_db TO username;
```

#### 使用SQLite（开发/测试）
对于开发或测试环境，可以使用SQLite，系统会自动创建数据库文件。

### 3. 配置环境

复制环境配置文件：

```bash
cp .env.sample .env
# 编辑.env文件，修改数据库连接等配置
```

### 4. 启动系统

```bash
# 启动主程序
python main.py
```

### 5. 访问Web界面

打开浏览器访问：http://localhost:8080

## 项目结构

```
d:\app\TAF claude1\
├── main.py                 # 主程序入口
├── requirements.txt        # 项目依赖
├── .env                    # 环境配置
├── config/                 # 配置管理
│   ├── settings.py         # 系统配置
│   └── database.py         # 数据库管理
├── models/                 # 数据模型
│   ├── backup.py           # 备份模型
│   ├── tape.py             # 磁带模型
│   ├── user.py             # 用户模型
│   └── system_log.py       # 日志模型
├── backup/                 # 备份处理
│   └── backup_engine.py    # 备份引擎
├── recovery/               # 恢复处理
│   └── recovery_engine.py  # 恢复引擎
├── tape/                   # 磁带驱动
│   ├── tape_manager.py     # 磁带管理器
│   ├── scsi_interface.py   # SCSI接口
│   ├── tape_cartridge.py   # 磁带盒类
│   └── tape_operations.py  # 磁带操作
├── web/                    # Web界面
│   ├── app.py              # Web应用
│   ├── api/                # API接口
│   ├── templates/          # 页面模板
│   ├── static/             # 静态资源
│   └── middleware/         # 中间件
├── utils/                  # 工具类
│   ├── logger.py           # 日志管理
│   ├── scheduler.py        # 计划任务
│   └── dingtalk_notifier.py # 钉钉通知
├── mcp/                    # 核心备份模块
│   └── core.py             # 核心处理器
├── tests/                  # 测试程序
├── docs/                   # 文档目录
└── logs/                   # 日志目录
```

## 配置说明

### 数据库配置

系统支持多种数据库类型，可通过Web界面可视化配置：

#### 1. Web界面配置（推荐）

1. 启动系统后访问：http://localhost:8080
2. 进入"系统设置" → "数据库"选项卡
3. 选择数据库类型并填写连接信息
4. 点击"测试连接"验证配置
5. 点击"保存配置"保存设置

支持的数据库类型：
- **SQLite**: 轻量级本地数据库
- **PostgreSQL**: 开源关系型数据库
- **openGauss**: 华为开源企业级数据库
- **MySQL**: 流行的开源数据库

详细配置说明请参考：[数据库配置说明](docs/数据库配置说明.md)

#### 2. 环境变量配置

也可以通过 `.env` 文件配置：

```ini
# 数据库配置示例（openGauss）
DATABASE_URL=opengauss://username:password@localhost:5432/backup_db
DB_HOST=localhost
DB_PORT=5432
DB_USER=username
DB_PASSWORD=password
DB_DATABASE=backup_db
DB_POOL_SIZE=10
DB_MAX_OVERFLOW=20

# 或使用SQLite（开发/测试）
# DATABASE_URL=sqlite:///./data/taf_backup.db
```

### 磁带配置

- `TAPE_DRIVE_LETTER`: 磁带驱动器盘符（Windows）
- `DEFAULT_BLOCK_SIZE`: 默认块大小（256KB）
- `MAX_VOLUME_SIZE`: 最大卷大小（300GB）

### 压缩配置

- `COMPRESSION_LEVEL`: 压缩级别（1-9）
- `MAX_FILE_SIZE`: 最大文件大小（3GB）
- `SOLID_BLOCK_SIZE`: 固实块大小（64MB）

### 钉钉通知配置

```ini
DINGTALK_API_URL=http://localhost:5555
DINGTALK_API_KEY=your-dingtalk-api-key
DINGTALK_DEFAULT_PHONE=13800000000
```

## API接口

系统提供RESTful API接口：

- `/api/backup` - 备份管理
- `/api/recovery` - 恢复管理
- `/api/tape` - 磁带管理
- `/api/system` - 系统管理
- `/api/user` - 用户管理

## 测试

运行测试：

```bash
# 运行所有测试
pytest

# 运行特定测试
pytest tests/test_backup.py

# 生成覆盖率报告
pytest --cov=.
```

## 故障排除

### 常见问题

1. **数据库连接失败**
   - 检查openGauss服务状态
   - 验证连接参数配置

2. **磁带设备无法识别**
   - 检查SCSI驱动器状态
   - 确认磁带驱动器权限

3. **压缩失败**
   - 检查7-Zip SDK安装
   - 验证临时目录权限

## 开发指南

### 添加新的备份策略

1. 在`mcp/core.py`中创建新的策略类
2. 继承`BackupStrategy`基类
3. 实现`execute`方法

### 添加新的API接口

1. 在`web/api/`目录下创建新文件
2. 使用FastAPI定义路由
3. 在`web/app.py`中注册路由

## 贡献指南

1. Fork项目
2. 创建特性分支
3. 提交更改
4. 推送到分支
5. 创建Pull Request

## 许可证

本项目采用MIT许可证。详见LICENSE文件。

## 联系方式

- 项目维护者：企业级磁带备份系统团队
- 邮箱：support@example.com
- 项目地址：https://github.com/grigs28/TAF

## 版本历史

- v0.0.1 - 初始版本
  - 基础备份恢复功能
  - Web管理界面
  - 钉钉通知集成
  - 跨平台支持