# 计划任务执行细节文档

## 概述

本文档详细说明计划任务（Scheduled Task）的执行流程，特别是备份任务中的格式化操作如何执行和完成。

## 执行流程总览

```
计划任务调度器
    ↓
BackupActionHandler.execute()
    ↓
执行前检查（周期检查、运行中检查、磁带标签检查）
    ↓
创建备份任务执行记录
    ↓
完整备份前格式化（如果任务类型为FULL）
    ├─ 格式化前：记录原卷标
    ├─ 执行格式化：LtfsCmdFormat.exe
    ├─ 格式化后：读取新卷标
    └─ 更新数据库：原卷标记录 → 新卷标
    ↓
执行备份任务（BackupEngine.execute_backup_task）
    ↓
更新任务状态和结果
```

## 详细执行流程

### 1. 计划任务触发

**位置**: `utils/scheduler/task_scheduler.py` 或类似调度器

**触发条件**:
- 根据 Cron 表达式或调度配置
- 检查任务是否启用
- 检查是否到达执行时间

### 2. 执行前检查

**位置**: `utils/scheduler/action_handlers.py` - `BackupActionHandler.execute()`

#### 2.1 周期检查（仅自动运行）

```python
# 检查当前周期内是否已成功执行
if scheduled_task.last_success_time:
    # 根据 schedule_type 判断周期
    # daily: 检查日期
    # weekly: 检查周
    # monthly: 检查月份
    # yearly: 检查年份
```

**结果**:
- 如果周期内已执行 → 跳过本次执行，返回 `{"status": "skipped"}`
- 如果未执行 → 继续执行

#### 2.2 运行中检查

```python
# 检查任务状态是否为 RUNNING
if scheduled_task.status == 'RUNNING':
    # 跳过执行
    return {"status": "skipped", "message": "任务正在执行中"}
```

#### 2.3 磁带标签当月验证（仅备份目标为磁带时）

```python
# 读取当前驱动器中的磁带卷标
metadata = await tape_ops._read_tape_label()

# 检查卷标年月是否匹配当前年月
if metadata:
    created_dt = metadata.get('created_date')
    if created_dt.year != current_time.year or created_dt.month != current_time.month:
        # 抛出异常，要求更换磁带
        raise ValueError("当前磁带非当月，请更换磁带后重试")
```

**结果**:
- 如果磁带非当月 → 抛出异常，发送通知，任务失败
- 如果磁带是当月 → 继续执行

### 3. 创建备份任务执行记录

**位置**: `utils/scheduler/action_handlers.py` - 第393-467行

**操作**:
- 从模板或配置加载备份参数
- 创建 `backup_tasks` 表记录（`is_template=False`）
- 设置任务状态为 `PENDING`

### 4. 完整备份前格式化

**位置**: `utils/scheduler/action_handlers.py` - 第469-501行

**触发条件**: 任务类型为 `BackupTaskType.FULL`

#### 4.1 格式化执行

```python
# 调用格式化方法（计划任务使用当前年月）
ok = await tape_ops.erase_preserve_label(use_current_year_month=True)
```

**详细流程** (`tape/tape_operations.py` - `erase_preserve_label`):

1. **格式化前记录原卷标** (第197-207行)
   ```python
   metadata = await self._read_tape_label()
   original_tape_id = metadata.get("tape_id")
   original_label = metadata.get("label") or original_tape_id
   ```

2. **生成新卷标** (第220-225行)
   ```python
   # 计划任务：使用当前年月生成卷标
   # 格式：TP{YYYY}{MM}01（例如：TP20251101）
   label = f"TP{current_year:04d}{current_month:02d}01"
   ```

3. **执行格式化命令** (第248-253行)
   ```python
   format_result = await tape_tools_manager.format_tape_ltfs(
       drive_letter=drive_letter,
       volume_label=label,
       serial=serial_number,
       eject_after=False
   )
   ```

4. **格式化命令执行监控** (`utils/tape_tools.py` - `run_command`)

   **关键机制**:
   ```python
   # 使用 wait() 等待进程结束
   returncode = await asyncio.wait_for(proc.wait(), timeout=timeout)
   ```
   
   **判断执行完成的依据**:
   - `proc.wait()` 返回进程的返回码（`returncode`）
   - 如果进程已结束，`wait()` 会立即返回，不会继续等待
   - 返回码为 0 表示成功，非 0 表示失败
   
   **超时处理**:
   - 如果 `wait()` 超时，检查 `proc.returncode`
   - 如果 `returncode` 为 `None`：进程仍在运行，调用 `proc.kill()` 终止
   - 如果 `returncode` 不为 `None`：进程已结束，使用返回码

5. **格式化成功后处理** (第261-285行)
   ```python
   if format_result.get("success"):
       # 读取格式化后的新卷标
       new_metadata = await self._read_tape_label()
       new_label = new_metadata.get("label") or new_metadata.get("tape_id")
       new_tape_id = new_metadata.get("tape_id") or label
       
       # 更新数据库：使用原卷标查找记录，更新为新卷标
       await self._update_tape_label_in_database(
           original_tape_id=original_tape_id,
           original_label=original_label,
           new_tape_id=new_tape_id,
           new_label=new_label,
           use_current_year_month=True
       )
   ```

#### 4.2 数据库更新逻辑

**位置**: `tape/tape_operations.py` - `_update_tape_label_in_database`

**更新流程**:

1. **查找原记录**
   ```python
   # 优先使用 original_tape_id 查找
   old_tape = await conn.fetchrow(
       "SELECT tape_id, label FROM tape_cartridges WHERE tape_id = $1",
       original_tape_id
   )
   
   # 如果没有，使用 original_label 查找
   if not old_tape:
       old_tape = await conn.fetchrow(
           "SELECT tape_id, label FROM tape_cartridges WHERE label = $1",
           original_label
       )
   ```

2. **更新记录**
   ```python
   if old_tape:
       # 检查新tape_id是否已存在（避免主键冲突）
       if new_tape_id 已存在且不是当前记录:
           # 只更新 label
       else:
           # 更新 tape_id 和 label
   else:
       # 如果使用当前年月，尝试创建新记录
   ```

**更新内容**:
- `tape_id`: 更新为新卷标（如 `TP20251101`）
- `label`: 更新为新卷标
- `updated_at`: 自动更新为当前时间

#### 4.3 格式化失败处理

**位置**: `utils/scheduler/action_handlers.py` - 第484-501行

```python
if not ok:
    logger.warning("完整备份前格式化失败，将尝试继续执行备份")
    # 记录警告日志，但继续执行备份
```

**注意**: 格式化失败不会阻止备份任务执行，只会记录警告日志。

### 5. 执行备份任务

**位置**: `backup/backup_engine.py` - `execute_backup_task()`

**调用**:
```python
success = await self.system_instance.backup_engine.execute_backup_task(
    backup_task, 
    scheduled_task=scheduled_task, 
    manual_run=manual_run
)
```

**备份引擎中的格式化** (如果任务类型为FULL):
- 备份引擎也会执行格式化（第562行）
- 但计划任务已经在第483行执行过格式化
- 备份引擎的格式化会保留原卷标（`use_current_year_month=False`）

### 6. 任务完成处理

**位置**: `utils/scheduler/action_handlers.py` - 第514-553行

**成功时**:
```python
if success:
    # 发送成功通知
    await dingtalk_notifier.send_backup_notification(
        backup_name=task_name,
        status='success',
        details={'size': total_bytes, 'file_count': total_files}
    )
    
    return {
        "status": "success",
        "message": "备份任务执行成功",
        "backup_task_id": backup_task.id,
        "backup_set_id": backup_task.backup_set_id,
        "tape_id": backup_task.tape_id,
        ...
    }
```

**失败时**:
```python
else:
    # 发送失败通知
    await dingtalk_notifier.send_backup_notification(
        backup_name=task_name,
        status='failed',
        details={'error': error_message}
    )
    
    raise RuntimeError(f"备份任务执行失败: {backup_task.error_message}")
```

## 格式化任务完成判断

### 判断机制

格式化任务通过以下机制判断是否完成：

1. **进程监控** (`utils/tape_tools.py` - `run_command`)
   ```python
   # 使用 proc.wait() 等待进程结束
   returncode = await asyncio.wait_for(proc.wait(), timeout=timeout)
   ```

2. **完成标志**
   - `proc.wait()` 返回进程返回码
   - 返回码不为 `None` 表示进程已结束
   - 返回码为 `0` 表示成功，非 `0` 表示失败

3. **返回值**
   ```python
   return {
       "success": returncode == 0,
       "returncode": returncode,
       "stdout": stdout_str,
       "stderr": stderr_str
   }
   ```

### 完成后的操作

1. **读取新卷标** (第267行)
   ```python
   new_metadata = await self._read_tape_label()
   ```

2. **更新数据库** (第273-279行)
   ```python
   await self._update_tape_label_in_database(
       original_tape_id=original_tape_id,
       original_label=original_label,
       new_tape_id=new_tape_id,
       new_label=new_label,
       use_current_year_month=use_current_year_month
   )
   ```

3. **返回成功** (第285行)
   ```python
   return True  # 格式化完成
   ```

## 关键时间点

### 格式化任务时间线

```
T0: 格式化前记录原卷标
    ├─ original_tape_id = "TP20241015"
    └─ original_label = "TP20241015"

T1: 生成新卷标（计划任务）
    └─ label = "TP20251101"  (当前年月)

T2: 执行 LtfsCmdFormat.exe
    ├─ 创建子进程
    ├─ proc.wait() 等待进程结束
    └─ 进程返回码：0 (成功)

T3: 格式化完成
    ├─ 读取新卷标：new_label = "TP20251101"
    ├─ 更新数据库：原记录(TP20241015) → 新记录(TP20251101)
    └─ 返回 True

T4: 继续执行备份任务
```

## 错误处理

### 格式化失败场景

1. **格式化命令执行失败**
   - `format_result.get("success") == False`
   - 返回 `False`
   - 计划任务会记录警告但继续执行备份

2. **数据库更新失败**
   - 捕获异常，记录警告日志
   - **不影响格式化流程**（格式化本身成功）
   - 返回 `True`（格式化成功）

3. **进程超时**
   - `proc.wait()` 超时
   - 检查 `proc.returncode`
   - 如果进程仍在运行，调用 `proc.kill()` 终止
   - 返回超时错误

## 数据库更新详情

### 更新策略

1. **使用原卷标查找记录**
   - 优先使用 `original_tape_id` 查找
   - 如果没有，使用 `original_label` 查找

2. **更新记录**
   - 如果找到记录：更新 `tape_id` 和 `label`
   - 如果新 `tape_id` 已存在：只更新 `label`
   - 如果未找到记录且是计划任务：尝试创建新记录

3. **更新字段**
   ```sql
   UPDATE tape_cartridges
   SET tape_id = $1, label = $2, updated_at = NOW()
   WHERE tape_id = $3
   ```

## 日志记录

### 关键日志点

1. **格式化前**
   ```
   INFO: 格式化前记录原卷标: tape_id=TP20241015, label=TP20241015
   INFO: 计划任务格式化：使用当前年月生成卷标 TP20251101
   ```

2. **格式化执行**
   ```
   INFO: [LTFS] 执行命令: D:\APP\TAF\ITDT\LtfsCmdFormat.exe O /N:TP20251101
   INFO: [LTFS] 进程已结束，返回码: 0
   INFO: [LTFS] 命令执行成功
   ```

3. **格式化后**
   ```
   INFO: LtfsCmdFormat格式化成功，卷标已设置为: TP20251101
   INFO: 数据库中的磁带记录已更新: 原卷标=TP20241015 -> 新卷标=TP20251101
   ```

## 总结

### 格式化任务完成判断

✅ **格式化任务已完成的标志**:
1. `proc.wait()` 返回进程返回码（不为 `None`）
2. `format_result.get("success") == True`
3. `erase_preserve_label()` 返回 `True`
4. 数据库更新完成（或更新失败但已记录日志）

### 执行流程特点

1. **异步非阻塞**: 使用 `asyncio.wait_for()` 和 `proc.wait()` 监控进程
2. **进程状态监控**: 通过 `proc.returncode` 判断进程是否结束
3. **数据库同步**: 格式化成功后自动更新数据库记录
4. **错误容错**: 数据库更新失败不影响格式化流程

### 计划任务格式化特点

1. **使用当前年月**: 卷标格式为 `TP{YYYY}{MM}01`
2. **记录原卷标**: 格式化前记录，用于数据库更新
3. **更新数据库**: 使用原卷标查找记录，更新为新卷标
4. **容错处理**: 格式化失败不会阻止备份任务执行

## 重要说明：格式化不是线程

### 执行方式

**格式化不是线程，而是异步函数（async/await）**：

```python
# 格式化是异步函数，不是线程
async def erase_preserve_label(...) -> bool:
    # 使用 await 等待格式化完成
    format_result = await tape_tools_manager.format_tape_ltfs(...)
```

### 完成判断机制

格式化任务通过以下方式判断是否完成：

1. **进程监控** (`utils/tape_tools.py` - `run_command`)
   ```python
   # 使用 proc.wait() 等待进程结束
   returncode = await asyncio.wait_for(proc.wait(), timeout=timeout)
   ```
   - `proc.wait()` 返回进程返回码
   - 返回码不为 `None` 表示进程已结束
   - 返回码为 `0` 表示成功

2. **返回值确认**
   ```python
   format_result.get("success") == True  # 格式化成功
   erase_preserve_label() 返回 True      # 方法返回成功
   ```

### 潜在的阻塞问题（已修复）

**问题**: 格式化完成后可能卡住的原因：

1. **`_read_tape_label()` 中的 `proc.communicate()` 没有超时**
   - 位置: `tape/tape_operations.py` - 第814行（已修复）
   - 问题: 如果 `fsutil` 命令卡住，会一直等待
   - 修复: 添加10秒超时，超时后终止进程

2. **`_write_tape_label()` 中的 `proc.communicate()` 没有超时**
   - 位置: `tape/tape_operations.py` - 第899行（已修复）
   - 问题: 如果 `label` 命令卡住，会一直等待
   - 修复: 添加10秒超时，超时后终止进程

**修复后的代码**:
```python
# 添加超时处理，避免阻塞
try:
    stdout, stderr = await asyncio.wait_for(proc.communicate(), timeout=10)
except asyncio.TimeoutError:
    logger.warning(f"命令执行超时，尝试终止进程...")
    if proc.returncode is None:
        proc.kill()
        await proc.wait()
    return None  # 或 False
```

### 执行流程中的阻塞点

格式化执行流程中的潜在阻塞点：

1. ✅ **`proc.wait()`** - 已有超时处理（`timeout` 参数）
2. ✅ **`proc.communicate()`** - 已有超时处理（10秒）
3. ✅ **`_read_tape_label()`** - 已添加超时处理（10秒）
4. ✅ **`_update_tape_label_in_database()`** - 数据库操作有异常处理，不会阻塞

### 建议

如果格式化仍然卡住，检查：

1. **日志输出**: 查看日志中是否有超时警告
2. **进程状态**: 检查 `LtfsCmdFormat.exe` 进程是否真的结束
3. **数据库连接**: 检查数据库连接是否正常
4. **磁盘I/O**: 检查磁带驱动器是否有I/O问题

## 相关文件

- `utils/scheduler/action_handlers.py` - 计划任务动作处理器
- `tape/tape_operations.py` - 磁带操作（格式化）
- `utils/tape_tools.py` - LTFS工具封装（命令执行）
- `backup/backup_engine.py` - 备份引擎

