# openGauss 无人值守守护模块

> 位置：`utils/opengauss/guard.py`

## 设计目标

- **实时心跳**：异步循环 `SELECT 1`，及时发现数据库断链/宕机。
- **操作看门狗**：所有通过 `utils.scheduler.db_utils` 建立的连接都会经过 `watch()`，统一施加超时控制。
- **自动告警**：连续失败达到阈值或标记为 `critical` 的操作，借助 `DingTalkNotifier` 推送钉钉消息。
- **无人值守**：异常无需人工“回车”，系统会尝试重试、记录上下文并推送通知。

## 关键流程

1. `TapeBackupSystem` 初始化阶段绑定 `DingTalkNotifier`，随后 `await monitor.start()` 启动心跳。
2. `db_utils` 在 `pool.acquire / release` 时调用 `monitor.watch()`，该函数使用 `asyncio.wait_for()` 强制超时并记录耗时。
3. `watch()` 失败或心跳连续失败时，累加计数并通过 `_maybe_alert()` 触发钉钉消息（带节流）。
4. `shutdown()` 时先停止守护，再关闭连接池，保证心跳协程不会在关闭后继续访问数据库。

## 可调参数（`.env`）

| 变量 | 默认值 | 说明 |
| --- | --- | --- |
| `OG_HEARTBEAT_INTERVAL` | `30` | 心跳间隔（秒） |
| `OG_HEARTBEAT_TIMEOUT` | `5.0` | 单次心跳超时 |
| `OG_OPERATION_TIMEOUT` | `45.0` | 受监控操作默认超时 |
| `OG_OPERATION_WARN_THRESHOLD` | `5.0` | 超过该耗时打印 Warning |
| `OG_OPERATION_FAILURE_THRESHOLD` | `3` | 连续失败次数触发告警 |
| `OG_MAX_HEARTBEAT_FAILURES` | `3` | 心跳连续失败阈值 |
| `OG_ALERT_COOLDOWN` | `600` | 告警冷却时间（秒） |

## 故障示例

- 连接池 `acquire` 卡住：`watch()` 超时 → 记录元数据（重试次数、host 等） → 钉钉通知。
- openGauss 短暂不可达：心跳失败累计，达到阈值后推送“心跳失败”消息。
- 长耗时（> `OG_OPERATION_WARN_THRESHOLD`）但未失败：日志中会记录 Warning，方便离线排查慢查询。

## 后续扩展建议

- 将 `monitor.watch()` 注入到执行 SQL 的 Helper 中，实现对查询级别的监控。
- 根据告警内容自动切换备用库或进入降级流程。

