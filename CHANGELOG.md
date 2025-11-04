# 更新日志
## [0.0.12] - 2025-11-04

### 修复

#### 计划任务系统修复
- ✅ 修复时间格式解析问题
  - 支持多种时间格式：`2025-11-04 17:05:00`、`2025/11/04 17:05:00`、`2025-11-04 17:05`、`2025/11/04 17:05`
  - 使用链式try-except处理不同格式，提高兼容性
  
- ✅ 修复openGauss数据库操作问题
  - `get_task`、`update_task`、`delete_task` 方法对openGauss使用原生SQL查询（asyncpg）
  - 避免SQLAlchemy版本解析错误：`Could not determine version from string '(openGauss-lite 7.0.0-RC1...)'`
  - 正确处理枚举类型（使用CAST）和JSON字段
  - 确保整数字段（total_runs、success_runs、failure_runs）不为None
  
- ✅ 修复API路由匹配问题
  - 调整路由定义顺序：更具体的路径（如`/tasks/{task_id}/disable`）必须在通用路径（如`/tasks/{task_id}`）之前定义
  - 修复`POST /api/scheduler/tasks/{task_id}/disable`和`DELETE /api/scheduler/tasks/{task_id}`返回404的问题
  - 确保FastAPI能够正确匹配路由

- ✅ 增强错误日志
  - 所有数据库操作错误都包含详细的堆栈跟踪信息
  - 便于调试和排查问题

## [0.0.11] - 2025-11-04

### 改进

#### 计划任务界面全面优化
- ✅ 优化计划任务弹出窗口布局
  - 任务名称、备份目标和备份类型一行三个字段（各占 col-md-4）
  - 调度类型、任务动作类型和调度配置一行三个字段（各占 col-md-4）
  - 备份源路径和目标路径一行显示（各占 col-md-6）
  - 任务描述和排除模式一行显示（各占 col-md-6）
  
- ✅ 备份目标配置优化
  - 备份目标选择改为下拉框（磁带机/存储），默认磁带机
  - 备份目标选择"磁带机"时：显示磁带机选择区域，隐藏目标路径
  - 备份目标选择"存储"时：显示目标路径，隐藏磁带机选择
  - 磁带机和存储共用同一位置，根据选择动态切换显示
  
- ✅ 目标磁带机选择优化
  - 改为下拉框 + 添加按钮方式（类似目录添加逻辑）
  - 下拉框包含"自动选择磁带机"、"全部磁带机"和所有可用磁带机
  - 选择后点击"添加"按钮添加到列表（深色背景卡片显示）
  - 支持添加多个磁带机
  - 选择"全部磁带机"时自动清除其他选择
  - 已选择"全部磁带机"时不允许添加单个磁带机
  
- ✅ 目标路径和源路径优化
  - 目标路径支持多选（类似源路径）
  - 目标路径列表使用深色背景卡片显示
  - 浏览按钮文字改为"浏览"（不再显示"浏览并添加"）
  - 合并浏览和添加功能：浏览选择后自动添加，无需再点击添加按钮
  
- ✅ 排除模式优化
  - 预填常用排除项：*.bak, *.tmp, *.log, *.swp, *.cache, Thumbs.db, .DS_Store, $RECYCLE.BIN, System Volume Information, pagefile.sys, $*
  - 删除提示文字"支持通配符模式，每行一个"

## [0.0.10] - 2025-11-03

### 改进

#### API路由优化
- ✅ 优化磁带管理API路由命名
  - `PUT /api/tape/{tape_id}` 改为 `PUT /api/tape/update/{tape_id}`
  - `GET /api/tape/{tape_id}` 改为 `GET /api/tape/show/{tape_id}`
  - 更新所有相关前端调用

#### 磁带管理界面优化
- ✅ 优化磁带扫描和标签读取流程
  - 合并"读取磁带标签"功能到"从磁带机扫描信息"按钮
  - 扫描时并行执行设备扫描和读取磁带标签
  - 优化扫描结果模态框显示格式：
    - 设备信息：标题在上，关键信息（厂商、型号、产品）显示在下方
    - 磁带信息：标题在上，关键信息（磁带标签、序列号、创建日期）对齐显示
  - 序列号使用后端Python `uuid.uuid4()`生成（HEX格式：全大写无连字符，32字符）
  - 添加后端UUID生成API：`GET /api/tape/generate-uuid`
  - 删除序列号刷新按钮，序列号由扫描结果自动生成
  - "应用信息"按钮仅在获得磁带标签后启用
  - 优化设备信息显示：隐藏"Unknown"序列号，代码块使用深底白字样式

#### 计划任务界面优化
- ✅ 优化计划任务弹出窗口布局
  - 任务名称和备份类型并排显示（第一行标签，第二行输入框）

## [0.0.9] - 2025-11-02

### 新增

- 计划任务目录浏览能力
  - 新增后端文件系统浏览接口：GET `/api/system/file-system/drives`、GET `/api/system/file-system/list?path=...`
  - Windows 使用 WinAPI 获取盘符，Linux 获取挂载点，统一返回结构
  - 前端新增左右分栏目录浏览器（驱动器/网络路径 + 目录/文件列表）
  - 支持输入网络路径（`\\server\share`、`//server/share`、`smb://...`）

- 备份管理与计划任务联动
  - 计划任务可选择备份模板执行，避免重复配置
  - 执行时自动生成备份记录并关联模板，防止同模板同日重复执行

### 改进

- 计划任务 UI 优化
  - 备份源路径支持多选与样式优化（卡片式、网络标签、路径截断）
  - 目录浏览器深色主题适配，文本高对比度，交互高亮
  - 表单布局优化：
    - “备份类型 + 目标磁带机”同一行两列
    - “启用压缩 + 启用加密 + 启用任务”同一行三列，并移动到“排除模式”上方

- 依赖与加载顺序
  - 全局引入 `axios.min.js`，修复 `axios is not defined` 问题（base 模板统一加载）
  - `scheduler.js` 作为 ES Module 导入并兼容全局 axios

- 系统与数据库
  - 数据库初始化引入所有模型，确保新表与枚举自动创建
  - 移除应用启动时的路由清单调试日志

### 修复

- SQLAlchemy Declarative 冲突：将 `metadata` 字段重命名为 `task_metadata`
- openGauss 版本解析导致任务加载报错的容错处理（记录日志，不阻塞表创建）


所有重要的变更都会记录在此文件中。

本文档遵循 [Keep a Changelog](https://keepachangelog.com/zh-CN/1.0.0/) 格式。

## [0.0.8] - 2025-11-01

### 新增

#### 磁带创建和管理逻辑优化
- ✅ 完善磁带创建逻辑：月份选择默认当前月，创建和过期日期仅计算年月
  - 创建日期和过期日期计算只考虑年月，忽略具体日期
  - 过期判断仅比较年月：当前年月 >= 过期年月
  - 月份选择器默认选中当前月，添加提示文本
  - 实现磁带标签读取后自动填充：如果数据库已存在，禁用标签输入框并更新其他信息
  
#### 磁带标签保持不变机制
- ✅ 实现擦除和格式化后保留磁带标签
  - 擦除操作后自动重新写入原始标签，保留创建日期和过期日期
  - 格式化操作前读取现有标签，格式化成功后重新写入
  - 确保磁带标签在整个生命周期内保持恒定

### 修复

#### LTFS文件系统标签支持
- ✅ 修复Windows下LTFS环境中无法读取磁带标签的问题
  - 实现双模式标签读取/写入：优先使用LTFS文件系统（`O:\TAPE_LABEL.txt`），失败回退到SCSI磁带头
  - Windows系统且配置了LTFS盘符时，自动从文件系统读取标签
  - 兼容IBM LTFS环境中磁带设备被独占的情况
  - 解析LTFS格式的标签文件（TAPE_xxx, Created, Capacity字段）
  - 写入标签时同样优先写入LTFS文件系统
  
#### 时区感知datetime处理
- ✅ 修复过期判断时区问题：使用timezone-aware datetime进行比较
  - 数据库返回的是timezone-aware datetime，需要使用相同的类型进行比较
  - 统一使用datetime.now(timezone.utc)进行过期判断

## [0.0.7] - 2025-10-31

### 移除

#### UUID功能完全移除
- ✅ 移除所有UUID相关功能和代码
  - 从数据库模型中删除`tape_uuid`和`set_uuid`字段
  - 删除物理UUID读取相关的所有SCSI接口方法
  - 移除UUID读取失败的错误对话框
  - 删除磁带列表中的UUID显示
  - 简化磁带创建流程，取消UUID验证要求
  - 清理所有UUID相关的Windows Storage API和VPD读取代码

## [0.0.6] - 2025-10-30

### 新增

#### 磁带管理扫描结果模态框
- ✅ 添加磁带设备扫描结果详情显示
  - 扫描后显示完整设备信息（厂商、型号、序列号、设备类型、建议容量等）
  - 提供"应用信息"按钮自动填充表单
  - 改善用户体验

#### 磁带编辑功能
- ✅ 实现磁带记录编辑功能
  - 支持编辑磁带序列号、类型、容量、位置、备注
  - 主键(tape_id)和标签不允许修改
  - 添加PUT `/api/tape/{tape_id}` 更新API

#### 物理磁带标签读写
- ✅ 实现磁带物理标签自动读写功能
  - 创建磁带时自动写入标签到物理磁带
  - 读取磁带标签以检测是否已格式化
  - **独立的写入标签API**：POST `/api/tape/write-label`允许用户修改现有磁带的卷标
  - 智能格式化：仅未格式化磁带才格式化
  - 使用SCSI READ(16)/WRITE(16)命令直接操作磁带头部
  - 修复读写操作错误处理以检查success字段
  - **WMI到DOS路径映射**：自动将WMI DeviceID转换为Windows DOS设备路径
  - **磁带设备路径验证**：优先使用\\.\TAPEn路径，通过INQUIRY命令验证设备类型确保准确性

### 修复

#### IBM LTO检测修复
- ✅ 修复WMI识别vendor字段错误导致LTO检测失败
  - vendor为"LTO"时从model或path中提取IBM信息
  - 从model中提取LTO代数并设置建议容量
  - Windows/Linux平台全面修复

#### 磁带容量单位转换
- ✅ 修复TB转GB容量计算错误
  - 18TB应转换为18432GB（18 * 1024）
  - 更新前后端容量转换逻辑

#### 数据库枚举值大小写
- ✅ 修复PostgreSQL/OpenGauss枚举值区分大小写问题
  - 使用全大写枚举值（AVAILABLE, IN_USE, FULL等）
  - 统一前后端状态值格式

#### 磁带创建API兼容性
- ✅ 修复磁带创建API在OpenGauss上的兼容性问题
  - 使用psycopg2直接连接避免SQLAlchemy版本解析
  - 修复tape status枚举值大小写问题
  - `/api/tape/list`、`/api/tape/create`、`/api/tape/check`全面优化
  - 明确指定health_score默认值为100，确保新磁带显示健康而非严重

#### 磁带显示问题
- ✅ 修复磁带卡片显示异常
  - health_score为空时默认100分
  - 状态映射支持大小写（AVAILABLE/available等）
  - 添加过期时间显示
  - 类型和容量显示为"LTO-9 / 18.0 TB"格式
  - 使用容量显示为"实际使用/实际最大容量"

### 改进

#### 磁带默认值优化
- ✅ 添加磁带默认位置为"机房"
  - 新磁带创建时自动填充位置为"机房"

#### 磁带下拉选择器
- ✅ 修复磁带容量下拉选择器值匹配问题
  - 统一选项值格式（18TB而非18 TB）
  - 确保自动填充功能正常工作

#### 调试信息增强
- ✅ 添加数据库枚举类型创建和检查调试日志
  - 显示枚举类型值信息
  - 便于排查数据库兼容性问题

## [0.0.5] - 2025-10-29

### 新增

#### 模态框拖拽功能
- ✅ 实现所有模态框的可拖拽功能
  - 模态框内容区域可移动
  - 移除模态框透明度，改为不透明深色背景(#0d1626)
  - 改善模态框的用户交互体验

#### 数据库自动初始化
- ✅ OpenGauss数据库自动创建和权限配置
  - 自动检测数据库是否存在，不存在则创建
  - 自动设置数据库所有者权限
  - 自动配置public schema权限和默认权限
  - 使用psycopg2直接执行DDL语句

#### 磁带管理数据库集成
- ✅ 磁带列表API支持数据库查询
  - `/api/tape/list` - 从数据库获取所有磁带记录
  - `/api/tape/inventory` - 从数据库聚合统计信息
  - 支持磁带状态、位置、健康状态筛选
  - 支持磁带搜索功能

#### 磁带操作数据库同步
- ✅ 磁带操作自动同步到数据库
  - `load_tape` - 加载磁带时更新状态为in_use
  - `unload_tape` - 卸载磁带时更新状态为available
  - `erase_tape` - 擦除磁带时重置磁带信息
  - `write_data` - 写入数据后更新used_bytes和write_count

#### 磁带管理UI动态加载
- ✅ 磁带管理页面完全动态化
  - 统计卡片：磁带总数、可用磁带、已满磁带、错误磁带
  - 磁带列表：支持状态、位置、健康状态筛选
  - 磁带搜索：按标签、序列号、ID搜索
  - 磁带详情：动态显示完整磁带信息

### 修复

#### OpenGauss兼容性
- ✅ 修复OpenGauss版本字符串解析问题
  - 使用psycopg2直接连接，绕过SQLAlchemy版本解析
  - 修复DATETIME类型在OpenGauss中不支持的问题，改用TIMESTAMP
  - 修复ENUM类型创建问题，使用SQLAlchemy引擎生成SQL
  - 移除无效的server_version_check参数

#### 数据库初始化
- ✅ 修复数据库未初始化导致的RuntimeError
  - 统一使用全局db_manager实例
  - 修复main.py中创建新DatabaseManager实例的问题
  - 使用依赖注入get_db()获取数据库会话

#### 模态框显示
- ✅ 彻底禁用模态框遮罩层
  - 设置.modal-backdrop { display: none !important; }
  - 移除所有模态框的data-bs-backdrop属性或设为false
  - 设置模态框z-index为9999确保显示在最上层

#### 系统启动容错
- ✅ 系统启动时数据库连接失败不退出
  - 组件初始化失败时记录警告日志继续启动
  - 允许用户通过Web界面修复配置
  - 提供清晰的错误提示信息

### 改进

#### 磁带机配置页面
- ✅ 自动扫描磁带设备
  - 页面加载时自动触发设备扫描
  - 连接状态和已检测设备并排显示
  - 提高用户体验

#### 数据库健康检查
- ✅ OpenGauss数据库健康检查优化
  - 使用psycopg2直接连接，避免SQLAlchemy版本解析
  - 异步健康检查使用原生SQL语句
  - 添加5秒连接超时设置

#### API性能优化
- ✅ 减少重复API调用
  - `loadTapeStatistics`使用已加载的磁带数据
  - 优化DOMContentLoaded事件处理顺序
  - 提升页面加载速度

## [0.0.4] - 2025-10-28

### 修复

#### 模态框显示问题
- ✅ 修复所有模态框(modal)被遮罩层遮挡的问题
  - 设置模态框 z-index 为 9999
  - 设置遮罩层 z-index 为 9998
  - 确保所有弹窗(添加磁带、扫描磁带、添加通知人员等)正常显示

#### 配置保存优化
- ✅ 修复通知设置保存后被重载覆盖的问题
- ✅ 修复数据库配置保存后被重载覆盖的问题
- ✅ 修复磁带机配置保存后被重载覆盖的问题
- ✅ 保存配置后不再自动重载，保持用户输入内容

#### 数据库支持
- ✅ 添加 OpenGauss 数据库方言支持
  - 安装 `opengauss-sqlalchemy>=2.4.0` 依赖包
  - 支持在系统设置中选择 OpenGauss 数据库类型

#### 配置测试优化
- ✅ 数据库测试从输入框读取配置信息
- ✅ 通知测试从输入框读取配置信息
- ✅ 测试前不需要保存配置，实时验证

### 改进

#### UI统一性增强
- ✅ 统一所有页面使用 console-panel 和 service-card 样式
- ✅ 首页、备份管理、恢复管理、磁带管理等页面视觉一致
- ✅ 首页磁带设备状态动态加载
- ✅ 提高文字亮度，改善深色背景下的可读性

#### 导航优化
- ✅ 磁带机配置提升为独立的顶级导航项
- ✅ 系统设置移除磁带机配置选项卡
- ✅ 系统设置默认显示"常规设置"选项卡
- ✅ 优化选中标签的文字颜色和背景

## [0.0.3] - 2025-10-28

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

## [0.0.2] - 2025-10-28

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

## [0.0.1] - 2025-10-28

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
版本：v0.0.5

