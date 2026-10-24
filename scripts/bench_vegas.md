# bench_vegas.py — Vegas 自适应并发全程调试工具

## 用途
在真实混合数据上调试 Vegas（自适应并发）：用 TAF 同款流程
（扫描 → 预分组 12G×95% → tar|zstd 压缩，不写带），按 79 NAS 的
磁盘压力 API 与内部产出自动增减压缩并发，记录全程决策日志，
用于标定 Vegas 阈值（起步/上限/上探/回退/快速保护）。

## 用法
```bash
python3 scripts/bench_vegas.py        # 前台；nohup 后台运行推荐
```
运行前置：79 上需运行 disk_api.py（:8090，见下方"依赖"）。

## 流程
1. 扫描 /mnt/79-fs + /mnt/79-yzbak（每源限 800GB，结果存 scan.json，可复用）
2. 预分组：TAF 三级规则（11.4GB/组贪心 + 超大文件独立成组）
3. 压缩：tar -cf - -T 清单 | zstd -3；组完成即删输出（SSD 峰值≈并发×9GB）
4. Vegas 状态机（PROBING/STABLE/COOLDOWN，周期 120s）：
   - 上探：产出稳/升 → 并发 +1（起步 4，上限 12）
   - 回退：产出降 >5% 或 util≥95% 到顶 → 并发 -1，冷却 2 周期
   - 重探：稳定期每 8 周期或产出掉 >20%（文件形态变化）→ 重新探测
   - 快速保护：util≥98% 且 await≥25ms → 立即 -1
   - 下限 3

## 信号
- 79 disk_api（每 10s）：md2_util / md2_await_ms / md2_iops / md2_read_mbps
- 内部产出（每周期）：完成组源字节 ÷ 周期秒（MB/s）

## 输出（/mnt/SSD/bench/）
- scan.json          扫描结果（文件清单+统计，二次运行复用）
- groups.json        分组结果
- vegas_decisions.csv 每个 Vegas 周期的指标与决策（调参依据）
- groups_result.json  每组压缩明细（n=当时并发）
- out/               临时归档（组完成即删）
- run.log            运行日志（stdout 重定向）

## 依赖
79 上的 disk_api.py（/var/services/homes/grigs/disk_api.py，:8090）：
读取 /proc/diskstats 提供 RAID/单盘 IOPS、吞吐、util、await、队列。
标准库实现，无需 root；目前手动启动，需配置 DSM 开机自启。
