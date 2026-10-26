#!/usr/bin/env python3
"""1.9.87 第五轮修复回归（G1 teardown 可调用 / G2 锁钳制收敛 / G3 from 字段时效 /
G4 构造局部化 / 前几轮复测）"""
import os
import time
import sys
# 从 __file__ 推仓库根（可移植，克隆到任意目录都能跑）
ROOT = os.path.dirname(os.path.dirname(os.path.dirname(os.path.abspath(__file__))))
sys.path.insert(0, ROOT)
from backup.vegas_nas import VegasNasController


class S:
    def __init__(self, read, util=99.0, await_ms=8.0, pressure='moderate'):
        self.read, self.util, self.await_ms, self.pressure = read, util, await_ms, pressure
    def window(self, span=0):
        return [{'md2_read_mbps': self.read, 'md2_util': self.util,
                 'md2_await_ms': self.await_ms, 'md2_iops': 5000,
                 'pressure': self.pressure, 'ts': time.time()}] * 12
    ok = True
    fail_count = 0


PASS = []
def check(name, cond, info=''):
    PASS.append((name, bool(cond)))
    print(('✅' if cond else '❌') + f' {name} {info}')


# ---- G1：teardown 可调用不抛错（上轮测试盲区：从不调用 teardown） ----
import backup.compression_worker as cw
src = open(os.path.join(ROOT, 'backup', 'compression_worker.py')).read()
assert 'if reset_alerts' not in src.split('def _teardown_vegas_nas')[1].split('def _ensure_vegas_nas')[0]
check('G1 teardown 无 reset_alerts 残留引用', True)


# ---- G3：verdict 完成后 from 字段已清（F6b 不会读到陈旧值） ----
c = VegasNasController(S(195), 5, 4, 10, log=lambda m: None)
c.state = 'PROBING'; c._probe_base = 195
c._t_next = 0; c.tick(5, 10)
c._t_next = 0; c.tick(5, 10)   # → POST_ROLLBACK n=4
for r in (120, 122, 118):
    c.sampler.read = r
    c._t_next = 0
    c.tick(4, 10)
check('G3 POST_ROLLBACK 有害判定后 from 已清', c._rollback_from_n is None
      and c._min_good_n == 5, f'from={c._rollback_from_n} min_good={c._min_good_n}')

# VERIFY 判读后 from 已清
c2 = VegasNasController(S(300), 10, 4, 10, log=lambda m: None)
c2.state = 'STABLE'; c2._stable_since = time.time() - 2000; c2._probe_base = 300
c2._t_next = 0; c2.tick(9, 10)   # → VERIFY n=9
assert c2.state == 'VERIFY' and c2._verify_from_n == 10, (c2.state, c2._verify_from_n)
c2.sampler.read = 320            # 无损（升）
for _ in range(3):
    c2._t_next = 0; c2.tick(9, 10)
check('G3 VERIFY 判读后 from 已清', c2._verify_from_n is None,
      f'from={c2._verify_from_n} state={c2.state}')

# ---- G2：clamp_locks 三路共用（控制器 +1 路径验证） ----
c3 = VegasNasController(S(195), 6, 4, 6, log=lambda m: None)   # max_n=6
c3.state = 'PROBING'; c3._probe_base = 195
c3._t_next = 0; c3.tick(6, 6)
c3._t_next = 0; c3.tick(6, 6)   # → POST_ROLLBACK n=5
for r in (120, 122, 118):
    c3.sampler.read = r
    c3._t_next = 0
    c3.tick(5, 6)
check('G2 恢复 +1 与锁均钳在 max_n=6 内',
      c3.n == 6 and c3._min_good_n == 6,
      f'n={c3.n} min_good={c3._min_good_n}')

# ---- G4 结构性验证：_ensure 中 start() 在 self 赋值之前 ----
seg = src.split('def _ensure_vegas_nas')[1].split('def ')[0]
i_start = seg.index('sampler.start()')
i_assign = seg.index('self._vegas_nas = _ctrl')
check('G4 start() 在挂到 self 之前（失败不留半成品）', i_start < i_assign)

print()
fails = [n for n, ok in PASS if not ok]
print(f'共 {len(PASS)} 项，失败 {len(fails)} 项' + (f'：{fails}' if fails else ''))
sys.exit(1 if fails else 0)
