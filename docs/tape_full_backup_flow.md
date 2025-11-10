# 磁带建档与完整备份流程说明

本文档梳理从“创建新磁带”到“执行完整备份”全过程的关键步骤、涉及的模块及核心数据流。适用于 Windows 环境、使用 LTFS 与 ITDT 工具的 TAF 系统。

## 1. 工具与约定

- **LTFS 命令套件**：位于 `LTFS_TOOLS_DIR`（默认 `D:\APP\TAF\ITDT`），其中 `LtfsCmdFormat.exe` 只能接受 **大写盘符且不带冒号** 的参数（如 `O`）。
- **ITDT 工具**：`ITDT_PATH`（默认 `D:\APP\TAF\ITDT\itdt.exe`），用于设备识别与格式化检测。
- **驱动器盘符**：配置项 `TAPE_DRIVE_LETTER` 存储为单个字母（例如 `O`），相关逻辑会自动附加或去除冒号。
- **卷标规范**：系统统一采用 `TPYYYYMMNN` 形式，其中 `YYYY` 为4位年份、`MM` 为月份、`NN` 表示当月第几盘（两位序号，如 `TP20250201` 表示2025年2月第1盘）。

## 2. 新磁带建档流程

### 2.1 前端表单（`web/templates/tape.html`）

1. 用户点击“添加磁带”按钮，弹出模态框。
2. 表单字段：
   - **创建年份**（新增）：下拉框默认当前年份，可上下5年范围选择；若从磁带读取卷标，则自动填充但仍可修改。
   - **创建月份**：默认当前月份；与年份一样，可通过卷标解析自动回填，并可手动调整。
   - **磁带卷标**：可手动输入或点击“生成”按钮生成 `TPYYYYMMNN` 卷标（序号自动递增，可继续修改）。从磁带读取卷标后仍允许修改/刷新。
   - **序列号、类型、容量、位置、备注**等其他字段。
3. 若点击“从磁带机扫描信息”：
   - 调用 `/api/system/tape/scan`，在 `scanResultModal` 中显示驱动器信息。
   - 如果能从驱动器读取卷标（经 `fsutil`），将卷标、年份、月份写回表单，用户可继续调整；同时检查数据库是否已存在同名磁带，决定进入“更新”或“创建”流程。
4. 点击“确认”时：
   - 若前置检查发现磁带未格式化，会提示是否先执行格式化（`/api/tape/operations/format`）。
   - 根据表单填充 `CreateTapeRequest`，包含 `create_year` 与 `create_month`。

### 2.2 后端 API（`web/api/tape/crud.py#create_tape`）

1. 解析请求后，根据 `create_year/create_month` 与用户输入的卷标，通过 `_normalize_tape_label` 生成最终卷标：
   - 保留原有后缀，仅更新年份与月份；默认生成 `TPYYYYMM01`，当月已有卷标会自动递增序号。
   - `tape_id` 也保持大写无空格。
2. 调用 `tape_tools_manager.format_tape_ltfs`：
   - 传入规范化盘符（大写无冒号）和生成的卷标。
   - 若失败，记录 `OperationLog/SystemLog` 并返回 500。
3. 根据 `tape_id` 判定数据库中是否已有记录：
   - 存在：执行 `UPDATE`，刷新卷标、容量等字段。
   - 不存在：执行 `INSERT`，建档时 `tape_id` 使用卷标值。
4. 记录 `OperationLog`，并把最终卷标与 `formatted=true` 返回给前端。

### 2.3 数据库存储

- 表 `tape_cartridges` 中的 `label` 字段始终保持 `TPYYYYMMNN` 格式（可能带后缀）。
- `manufactured_date` 与 `expiry_date` 由 `create_year/month + retention_months` 推算。

## 3. 磁带卷标读取与写入

### 3.1 读取（`tape/tape_operations.py#_read_tape_label`）

- 使用 `fsutil fsinfo volumeinfo O:` 获取卷名、序列号等信息；
- 不再读取 `.TAPE_LABEL.txt` 文件，如卷标获取失败将记录日志并返回 `None`。

### 3.2 写入

- 卷标写入由 `LtfsCmdFormat.exe` 完成；创建与完整备份流程都会调用该命令以确保卷标一致。
- 其他方式（如 Windows `label` 命令）不再用于设置卷标。

## 4. 备份引擎中的卷标与月份验证

### 4.1 当月校验（`backup/backup_engine.py#execute_backup_task`）

1. 在执行备份前调用 `_read_tape_label` 获取卷标；
2. 解析卷标中的年月：
   - 支持 `TPYYYYMMNN`（兼容旧格式会自动转成新格式）；
   - 仅检查月份与当前月份是否一致（年份不同仅产生提示日志，不阻止备份）。
3. 如果卷标无法解析或月份不匹配，抛出异常并通过 `OperationLog/SystemLog` 通知用户。

### 4.2 完整备份前格式化

1. 仅当任务类型为 `FULL` 时触发。
2. 使用当前磁带的 `label` 或 `tape_id` 为基础，调用 `normalize_volume_label` 更新为当年当月：
   - 如原卷标 `TP20240101`，在 2026 年完整备份前会更新为 `TP20260101`。
3. 调用 `tape_tools_manager.format_tape_ltfs()`，参数依然是大写盘符；
4. 成功后：
   - 更新 `backup_task` 进度为 100%；
   - 在 `tape_cartridges` 中刷新 `label` 与 `updated_at`；
   - 记录 `OperationLog/SystemLog`。
5. 失败或异常时，标记任务失败并发送钉钉通知。

## 5. 完整备份执行流程（简要）

| 步骤 | 组件 | 描述 |
|------|------|------|
| 1 | `BackupEngine.execute_backup_task` | 检查任务状态、当月卷标等前置条件 |
| 2 | `LtfsCmdFormat` | 针对 `FULL` 任务执行格式化并刷新卷标 |
| 3 | `_perform_backup` | 扫描源数据、生成 `tar`/`tar.gz` 写入 `O:` 盘 |
| 4 | `OperationLog`/`SystemLog` | 记录开始、完成或失败情况，必要时发送钉钉通知 |
| 5 | `tape_cartridges` | 更新任务涉及磁带的卷标、完成时间等信息 |

## 6. 注意事项

1. **盘符要求**：所有 LTFS 命令都使用大写无冒号的盘符（`O`），`TapeToolsManager` 会自动校正配置值。
2. **卷标来源优先级**：无论卷标来源（磁带或手工生成），用户均可在提交前修改并重新生成。
3. **格式化策略**：
   - `api/tape/create` 中始终执行 `LtfsCmdFormat`；
   - 备份任务中的格式化仅在 `FULL` 类型触发，以保证卷标包含当年信息。
4. **文档更新**：历史文档中提及 `.TAPE_LABEL.txt` 的地方，需要同步说明新的卷标读写策略。

## 7. 相关文件索引

- 前端 UI：`web/templates/tape.html`
- 磁带 API：`web/api/tape/crud.py`
- 磁带工具封装：`utils/tape_tools.py`
- 磁带操作核心：`tape/tape_operations.py`
- 备份引擎：`backup/backup_engine.py`
- 系统设置（盘符等）：`config/settings.py`、`web/api/system/*`

以上流程覆盖了从磁带开卡、卷标生成、格式化到完整备份执行的关键链路，相关模块之间的接口也在表中列出，方便后续排查与扩展。
