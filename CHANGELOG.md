# 更新日志

所有重要的变更都会记录在此文件中。

本文档遵循 [Keep a Changelog](https://keepachangelog.com/zh-CN/1.0.0/) 格式。

## [0.0.3] - 2024-11-01

### 新增

#### 用户界面全面升级
- ✅ 应用现代暗色科技主题界面
  - 从.sample目录引入完整的UI组件和样式
  - 深色背景色(#0a0e17)配合紫色系主题
  - 响应式布局，支持各种屏幕尺寸
- ✅ 丰富的视觉背景效果
  - 粒子动画效果(particles.js)
  - 电路板图案覆盖层
  - 数据流动画背景
  - 毛玻璃模糊效果(backdrop-filter)
- ✅ 优化的导航栏设计
  - 固定顶部导航栏
  - 半透明背景与模糊效果
  - 悬停和激活状态的平滑过渡动画
  - 用户头像和版本信息显示
- ✅ 控制台面板(console-panel)样式
  - 半透明卡片的毛玻璃效果
  - 圆角边框和阴影
  - 悬停时的上浮动画效果
- ✅ 服务卡片(service-card)组件
  - 顶部彩色渐变条
  - 图标容器样式
  - 状态指示器可视化
- ✅ 首页完全重构
  - 系统状态卡片展示
  - 快速操作按钮
  - 系统信息表格
  - 响应式栅格布局

### 改进

- ✅ 配色方案更新
  - 主色调从蓝色改为紫色(#8b7cf6)
  - 更适合磁带备份系统的科技感
  - 统一的色彩变量系统
- ✅ 字体系统优化
  - 使用AlimamaDaoLiTi字体
  - 统一的基础字体大小(0.9rem)
  - 导航、标题、正文分层字体大小
- ✅ 静态资源整理
  - 引入Bootstrap Icons图标库
  - 引入Particles.js动画库
  - 引入Marked.js Markdown渲染
  - 整理CSS/JS/图片资源结构

### 技术细节

#### 前端资源
- `web/static/css/ai.css` - 主样式文件(紫色主题)
- `web/static/js/components/backgroundEffects.js` - 背景效果
- `web/static/js/vendor/` - 第三方库文件
- `web/static/img/` - Logo和装饰图片

#### 模板更新
- `web/templates/base.html` - 基础模板全面重构
- `web/templates/index.html` - 首页应用新UI风格

#### UI特性
- CSS变量系统支持主题定制
- 响应式设计，移动端友好
- 动画过渡效果，提升用户体验
- 毛玻璃和模糊效果，现代感强

## [0.0.2] - 2024-11-01

### 新增

#### 磁带机配置增强
- ✅ 最大卷大小单位改为GB显示（更用户友好）
- ✅ 已检测设备显示磁盘容量和LTO代数信息
- ✅ 设备列表增强显示（厂商、型号、路径、容量、状态）

#### 通知系统增强
- ✅ 完整的通知事件配置界面
  - 备份相关：成功、开始、失败
  - 恢复相关：成功、失败
  - 磁带相关：更换、过期、错误
  - 系统相关：容量预警、系统错误
- ✅ 通知人员管理界面
  - 添加通知人员模态框
  - 支持多人员通知配置

#### 备份任务创建增强
- ✅ 新增备份类型支持
  - 完整备份 (Full Backup)
  - 增量备份 (Incremental)
  - 差异备份 (Differential)
  - 镜像备份 (Mirror) - 新增
  - 归档备份 (Archive) - 新增
  - 快照备份 (Snapshot) - 新增
- ✅ 源路径选择改进
  - 输入框支持多路径
  - 浏览按钮（待实现文件选择对话框）
- ✅ 备份目标选择
  - 磁盘存储
  - 磁带机（可选择具体磁带）

#### SCSI接口增强
- ✅ 新增磁带SCSI操作命令
  - format_tape - 格式化磁带
  - erase_tape - 擦除磁带
  - load_unload - 加载/卸载
  - space_blocks - 按块定位
  - write_filemarks - 写入文件标记
  - set_mark - 设置磁带标记
- ✅ 新增磁带操作API端点
  - POST /api/tape/format - 格式化
  - POST /api/tape/rewind - 倒带
  - POST /api/tape/space - 定位

## [0.0.1] - 2024-11-01

### 新增

#### 配置管理
- ✅ 新增 `.env.sample` 配置模板文件
- ✅ 新增 `SystemConfig` 数据模型（`models/system_config.py`）
- ✅ 新增 `SystemConfigManager` 配置管理器（`config/config_manager.py`）
- ✅ 新增 Web界面数据库配置功能
- ✅ 支持多数据库类型（SQLite、PostgreSQL、openGauss、MySQL）
- ✅ 配置参数自动从.env文件加载
- ✅ 数据库配置支持Web界面修改

#### 文档
- ✅ `README.md` - 项目说明文档
- ✅ `docs/系统架构.md` - 系统架构说明
- ✅ `docs/使用说明.md` - 用户使用指南
- ✅ `docs/开发说明.md` - 开发指南
- ✅ `docs/配置管理说明.md` - 配置管理说明
- ✅ `docs/数据库配置说明.md` - 数据库配置说明
- ✅ `docs/数据库配置测试说明.md` - 数据库配置测试指南
- ✅ `docs/配置参数存储规划.md` - 配置存储策略
- ✅ `docs/配置系统优化总结.md` - 配置系统优化总结
- ✅ `docs/仅使用OpenGauss的可行性说明.md` - OpenGauss使用说明
- ✅ `docs/Redis和Celery使用说明.md` - Redis/Celery使用说明
- ✅ `docs/IBM磁带机API使用示例.md` - IBM磁带机API示例
- ✅ `docs/IBM磁带机快速开始指南.md` - IBM磁带机快速开始
- ✅ `docs/IBM磁带机集成说明.md` - IBM磁带机集成说明

#### 核心功能
- ✅ 备份引擎（`backup/backup_engine.py`）
- ✅ 恢复引擎（`recovery/recovery_engine.py`）
- ✅ 磁带管理器（`tape/tape_manager.py`）
- ✅ 磁带操作（`tape/tape_operations.py`）
- ✅ SCSI接口（`tape/scsi_interface.py`）
- ✅ 核心备份处理器（`mcp/core.py`）
- ✅ 计划任务调度器（`utils/scheduler.py`）
- ✅ 钉钉通知器（`utils/dingtalk_notifier.py`）
- ✅ 日志管理器（`utils/logger.py`）

#### Web界面
- ✅ 主入口（`web/app.py`）
- ✅ 备份管理API（`web/api/backup.py`）
- ✅ 恢复管理API（`web/api/recovery.py`）
- ✅ 磁带管理API（`web/api/tape.py`）
- ✅ 系统管理API（`web/api/system.py`）
- ✅ 用户管理API（`web/api/user.py`）
- ✅ 认证中间件（`web/middleware/auth_middleware.py`）
- ✅ 日志中间件（`web/middleware/logging_middleware.py`）
- ✅ HTML模板（`web/templates/`）
  - index.html - 首页
  - backup.html - 备份管理
  - recovery.html - 恢复管理
  - tape.html - 磁带管理
  - system.html - 系统设置
  - base.html - 基础模板

#### 数据模型
- ✅ 基础模型（`models/base.py`）
- ✅ 备份模型（`models/backup.py`）
- ✅ 磁带模型（`models/tape.py`）
- ✅ 用户模型（`models/user.py`）
- ✅ 系统日志模型（`models/system_log.py`）
- ✅ 系统配置模型（`models/system_config.py`）

#### 静态资源
- ✅ CSS样式（`web/static/css/main.css`）
- ✅ JavaScript（`web/static/js/main.js`）
- ✅ Markdown渲染支持

#### 测试
- ✅ 测试框架配置（`tests/conftest.py`）
- ✅ 备份功能测试（`tests/test_backup.py`）
- ✅ 磁带功能测试（`tests/test_tape.py`）
- ✅ 配置功能测试（`tests/test_config.py`）

#### 文档
- ✅ `SCSI接口实现分析报告.md` - SCSI接口分析

### 修改

#### 配置优化
- ✅ 脱敏敏感配置信息（密码、密钥）
- ✅ 补充配置参数（ENVIRONMENT, WEB_HOST, ENABLE_CORS等）
- ✅ 统一Base模型引用
- ✅ 优化配置加载逻辑
- ✅ 数据库文件路径标准化（移动到data目录）
- ✅ 版本号从1.0.0调整为0.0.1
- ✅ 完善.env和.env.sample文件

#### 数据库
- ✅ 优化openGauss版本检测处理
- ✅ 支持异步和同步数据库操作
- ✅ 添加数据库连接池配置
- ✅ 改进数据库健康检查
- ✅ 支持SQLite数据库文件路径配置

### 安全

- ✅ 敏感信息脱敏处理
- ✅ 密码字段加密存储
- ✅ JWT令牌认证
- ✅ 输入验证和SQL注入防护

### 文档

- ✅ 完善README.md项目说明
- ✅ 添加详细的配置说明文档
- ✅ 补充开发和使用指南
- ✅ 创建系统架构文档

### 已知问题

- ⚠️ 磁带设备检测功能需要实际硬件测试
- ⚠️ 部分核心功能需要实际业务验证
- ⚠️ 计划任务持久化待完善

### 待办事项

- 🔲 标准LOG SENSE解析优化
- 🔲 MODE SENSE/SELECT完善
- 🔲 UI SCSI状态显示
- 🔲 实现Celery分布式任务（可选）
- 🔲 添加配置加密功能
- 🔲 完善Web界面交互
- 🔲 增加更多测试用例
- 🔲 实现配置版本管理

### 版本管理

- ✅ 新增CHANGELOG.md版本管理文件
- ✅ 实现版本API接口（GET /api/system/version）
- ✅ 添加UI版本显示和弹窗功能
- ✅ 优化数据库文件路径管理（data/taf_backup.db）
- ✅ 配置参数脱敏和标准化

#### SCSI接口重构

- ✅ 完善Windows SCSI Pass Through完整实现
  - 完整实现SCSI_PASS_THROUGH结构填充
  - 正确执行DeviceIoControl调用
  - 支持数据双向传输
- ✅ 修复Linux SG_IO导入问题
  - 优化平台特定导入逻辑
  - 确保fcntl正确可用
- ✅ 实现READ/WRITE SCSI命令
  - READ(16) 和 WRITE(16)完整实现
  - 支持64位LBA寻址
  - 替换旧式READ/WRITE(6)
- ✅ 添加SCSI命令重试机制
  - 指数退避重试策略
  - 智能错误类型判断
  - 自动处理临时性错误
- ✅ 实现设备热插拔监控
  - 设备连接/断开自动检测
  - 状态变化事件通知
  - 监控任务管理
- ✅ 优化SCSI接口架构
  - 代码结构优化
  - 错误处理增强
  - 日志记录完善

#### 磁带机配置UI

- ✅ 新增磁带机配置标签页
  - 设备路径配置
  - 块大小和卷大小配置
  - 磁带池配置
- ✅ 实现配置API
  - GET /api/system/tape/config - 获取配置
  - POST /api/system/tape/test - 测试连接
  - PUT /api/system/tape/config - 保存配置
  - GET /api/system/tape/scan - 扫描设备
- ✅ 设备扫描和测试
  - 实时扫描磁带设备
  - 连接状态测试
  - 设备列表显示

#### Bug修复

- ✅ 数据库健康检查修复（text导入）
- ✅ Recovery API Request参数修复
- ✅ 数据库配置密码自动填充
- ✅ 磁带连接测试逻辑优化
- ✅ 错误处理改进

#### 文档

- ✅ `SCSI接口重构总结.md` - SCSI重构文档
- ✅ `Build_Summary_20241101.md` - 构建总结

## [未发布]

### 计划

- 标准LOG SENSE解析（需要IBM文档参考）
- MODE SENSE/SELECT完整实现
- UI SCSI状态显示增强
- 配置加密功能
- 配置导入/导出
- 配置变更历史
- 配置版本回滚
- 更多监控指标
- 性能优化

---

**企业级磁带备份系统**
项目地址: https://github.com/grigs28/TAF
版本：v0.0.2

