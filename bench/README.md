# bench — Vegas 自适应并发调试工作目录

本目录存放 **Vegas 全程调试测试**（`scripts/bench_vegas.py`）的结果文件。
测试目的：用 TAF 同款流程（扫描→预分组→分组→tar|zstd 压缩，不写带）跑真实混合数据，
驱动 Vegas 状态机每 120 秒决策，**标定 79（群晖 NAS）磁盘压力 API 与压缩产出之间的
拐点规律**，为 Vegas 接入 TAF 生产（`GROUP_VEGAS_SIGNAL_SOURCE=nas_api`）提供阈值依据。

测试在 16（生产机）上运行：`/mnt/SSD/bench/`，产出文件如下。

## 文件说明

| 文件 | 作用 | 入仓 |
|------|------|------|
| `bench_vegas.py` | 测试脚本本体（与 `scripts/bench_vegas.py` 同步） | 仓库版在 `scripts/`，此处不入 |
| `scan.json` | 扫描结果：全部待压文件 `[路径,大小]` 列表 + 总字节 | **否**（~100MB，超 GitHub 单文件上限） |
| `groups.json` | 预分组结果：136 组（12G×95% 贪心 + 超大文件独立成组） | **否**（~100MB，同上） |
| `vegas_decisions.csv` | Vegas 每周期决策日志：n/P/util/await/iops/act/why —— **调参核心依据** | 是（测试完成后复制） |
| `groups_result.json` | 每组压缩结果：文件数/源GB/压缩后GB/耗时/速率/n | 是（测试完成后复制） |
| `run.log` | 运行日志（组完成行 + [Vegas] 决策行） | 摘要入仓（首尾截断） |
| `nas_api.key` | 79 disk_api 鉴权 key（脚本运行时读取；环境变量 `NAS_API_KEY` 优先） | **否**（密钥不入仓） |
| `out/` | 压缩输出临时目录（组完成即删，正常应为空） | 否 |

## 大文件的再生成

`scan.json` / `groups.json` 不入仓，重跑即自动生成（脚本优先复用已存在的 scan.json，
想强制重扫删除它即可）：

```bash
# 16 上（需 root：/mnt/79-fs 挂载点权限 d---------，普通用户读不了）
cd /mnt/SSD/bench
nohup /opt/conda3/envs/taf/bin/python bench_vegas.py >> run.log 2>&1 < /dev/null &
```

数据源：`/mnt/79-fs`（大文件区，均值 ~9.5MB）+ `/mnt/79-yzbak`（小文件区，均值 ~1.6MB），
每源限扫 800GB，混合形态用于验证"固定并发不可行、必须自适应"。

## Vegas 状态机（测试版本）

- 信号：内部产出 P（本周期完成源字节 MB/s）为主判据；79 API 的 util/await 为辅助
  （到顶判定、快速保护）；每 120s 一个决策周期
- PROBING：产出稳/升 → +1 上探；产出降 >5% → -1 进冷却；产出持平且 util≥95% → 到顶 -1
- STABLE：产出掉 >20%（数据形态变化）→ 重新探测；每 8 周期主动 +1 重探
- 快速保护：util≥98% 且 await≥25ms → 立即 -1
- 范围：n ∈ [3,12]，起点 4

## 已知结论（阶梯测试，~1MB 均值文件）

| 并发 | 读取 MB/s | util% | 产出 MB/s |
|------|-----------|-------|-----------|
| 4 | 292 | 79 | 183 |
| 5 | 341 | 93 | **172（拐点）** |
| 6 | 273 | — | 127（-30% 坍塌） |

混合数据下 4 并发即可达 687MB/s / 11563 IOPS / util 95%（大文件区），
证明最优并发随数据形态漂移，需 Vegas 持续自适应。

## 相关

- 测试脚本：`scripts/bench_vegas.py`（说明文档 `scripts/bench_vegas.md`）
- 79 压力 API：`http://192.168.0.79:8090/api/disk-stats?simple=1`（disk_api.py，
  开机自启 `/usr/local/etc/rc.d/diskapi.sh`，鉴权 `X-API-Key`/`?key=`）
- TAF 配置：`NAS_API_URL` / `NAS_API_KEY`（系统配置 → 备份策略 → 自适应并发区块）
