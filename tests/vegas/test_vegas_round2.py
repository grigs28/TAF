#!/usr/bin/env python3
"""1.9.83 修复回归测试（vegas_nas 审查 10 项）"""
import os
import time
import sys
# 从 __file__ 推仓库根（可移植，克隆到任意目录都能跑）
ROOT = os.path.dirname(os.path.dirname(os.path.dirname(os.path.abspath(__file__))))
sys.path.insert(0, ROOT)
from backup.vegas_nas import VegasNasController, PERIOD


class S:
    """采样桩：可分别控制 read/util/await/pressure"""
    def __init__(self, read, util=99.0, await_ms=8.0, pressure='moderate'):
        self.read, self.util, self.await_ms, self.pressure = read, util, await_ms, pressure
    def window(self, span=0):
        return [{'md2_read_mbps': self.read, 'md2_util': self.util,
                 'md2_await_ms': self.await_ms, 'md2_iops': 5000,
                 'pressure': self.pressure, 'ts': time.time()}] * 12
    ok = True
    fail_count = 0


def ctrl(read=300, start=8, mn=4, mx=10):
    return VegasNasController(S(read), start, mn, mx, log=lambda m: None)

PASS = []
def check(name, cond, info=''):
    PASS.append((name, bool(cond)))
    print(('✅' if cond else '❌') + f' {name} {info}')

# ---- P1: md2 快速保护跳过期 ----
_logs = []
c = ctrl(); c._probe_base = 300
c.log = _logs.append
c.sampler.util = 100.0; c.sampler.await_ms = 17.0
for i in range(5):
    c._t_next = 0
    c.tick(8, 8)
_fp = sum(1 for m in _logs if '快速保护' in m)
check('P1 快速保护只降一次（跳过期生效）', _fp == 1, f'快速保护触发{_fp}次, n={c.n}')

# ---- P2: 外部限并发时不计无增量 ----
c = ctrl(mn=2)                       # min_n=2 才能让 desired=3 生效
c._probe_base = 300
# 模拟扫描期：desired=3（n 会被对齐）
c._t_next = 0
c.tick(3, 3)          # 对齐到 3
for i in range(4):
    c._t_next = 0
    c.tick(3, 3)
check('P2 外部限并发下不降档（无 plateau 回退）', c.n == 3, f'n={c.n}')

# ---- P3: 外部对齐作废在飞 VERIFY ----
c = ctrl(start=10)                   # n=10=max_n 才能触发 VERIFY
c.state = 'STABLE'; c._stable_since = time.time() - 2000; c._probe_base = 300
c._t_next = 0
c.tick(9, 10)   # fed(9>=9) → 触发 VERIFY (n=10→9)
assert c.state == 'VERIFY', c.state
c.sampler.read = 290
c._t_next = 0
c.tick(9, 10)   # 样本1
# 中途磁盘限流把 desired 压到 5
c._t_next = 0
c.tick(5, 5)
check('P3 在飞验证被作废（状态转 STABLE、无锁）',
      c.state == 'STABLE' and c._verify_blocked_n is None,
      f'state={c.state} blocked={c._verify_blocked_n}')

# ---- P4/P5: POST_ROLLBACK 3 样本中位 + 原始参考值 ----
c = ctrl(195, start=5, mn=4)
c.state = 'PROBING'; c._probe_base = 195
c._t_next = 0; c.tick(5, 10)   # plateau 1
c._t_next = 0; c.tick(5, 10)   # plateau 2 → POST_ROLLBACK (n→4)
assert c.state == 'POST_ROLLBACK' and c.n == 4, (c.state, c.n)
check('P5 回退参考值用原始值', c._rollback_read == 195, f'ref={c._rollback_read}')
for r in (120, 120, 122, 118):
    c.sampler.read = r
    c._t_next = 0
    c.tick(4, 10)
check('P4 回退验证用 3 样本判有害并恢复', c.n == 5 and c._min_good_n == 5,
      f'n={c.n} min_good={c._min_good_n}')

# 降档无损保持
c2 = ctrl(195, start=5, mn=4)
c2.state = 'PROBING'; c2._probe_base = 195
c2._t_next = 0; c2.tick(5, 10)
c2._t_next = 0; c2.tick(5, 10)
for r in (192, 192, 190, 191):
    c2.sampler.read = r
    c2._t_next = 0; c2.tick(4, 10)
check('P4 降档无损保持 4', c2.n == 4 and c2.state == 'STABLE',
      f'n={c2.n} state={c2.state}')

# ---- P6: 漂移清除两个验证锁 ----
c3 = ctrl(); c3._min_good_n = 6; c3._verify_blocked_n = 8
c3.state = 'PROBING'; c3._probe_base = 270   # 首拍增量重置 plateau，避免 POST_ROLLBACK 抢占漂移
for r in (285, 280, 275):
    c3.sampler.read = r
    c3._t_next = 0
    c3.tick(8, 10)
check('P6 漂移后清除 _min_good_n/_verify_blocked_n',
      c3._min_good_n is None and c3._verify_blocked_n is None,
      f'min_good={c3._min_good_n} blocked={c3._verify_blocked_n}')

# ---- P10: 下限地板允许 1 ----
from backup.vegas_nas import VegasNasController as V
c4 = ctrl(mn=1)
check('P10 控制器允许 min_n=1', c4.min_n == 1, f'min_n={c4.min_n}')

print()
fails = [n for n, ok in PASS if not ok]
print(f'共 {len(PASS)} 项，失败 {len(fails)} 项' + (f'：{fails}' if fails else ''))
sys.exit(1 if fails else 0)
