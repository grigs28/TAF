#!/usr/bin/env python3
"""1.9.89 第七轮修复回归（I1 热关闭拆除 / I2 下限守卫 / I3 update_range /
I4 修复点覆盖 / I5 生命周期覆盖）"""
import os
import time
import sys

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


# ---- I2：下限处 plateau 满两拍不进 POST_ROLLBACK ----
c = VegasNasController(S(200), 4, 4, 10, log=lambda m: None)
c.state = 'PROBING'; c._probe_base = 200
for _ in range(4):
    c._t_next = 0
    c.tick(4, 10)
check('I2 下限处无增量不进 POST_ROLLBACK（转 STABLE 保持）',
      c.n == 4 and c.state == 'STABLE', f'n={c.n} state={c.state}')

# ---- I3/I4：update_range 行为级覆盖（修复点本身） ----
# 无在飞验证、max_n 降低、n 未变 → 锁必须被钳
c2 = VegasNasController(S(250), 8, 4, 10, log=lambda m: None)
c2._min_good_n = 10
c2.update_range(4, 8)
check('I4 update_range 无在飞验证时也钳锁（H1 修复点覆盖）',
      c2._min_good_n == 8, f'min_good={c2._min_good_n}')

# n 被钳制时基线失效 + 在飞验证作废
c3 = VegasNasController(S(300), 10, 4, 10, log=lambda m: None)
c3.state = 'STABLE'; c3._stable_since = time.time() - 2000; c3._probe_base = 300
c3._t_next = 0; c3.tick(9, 10)   # → VERIFY n=9 from=10
assert c3.state == 'VERIFY'
c3.update_range(4, 6)            # n 9→6
check('I3 update_range 钳制 n 时作废在飞验证+清基线',
      c3.state == 'STABLE' and c3._verify_from_n is None and c3._probe_base is None)

# n 未变但 from 越界 → 作废
c4 = VegasNasController(S(300), 8, 4, 8, log=lambda m: None)
c4.state = 'STABLE'; c4._stable_since = time.time() - 2000; c4._probe_base = 300
c4._t_next = 0; c4.tick(7, 8)    # → VERIFY n=7 from=8
assert c4.state == 'VERIFY' and c4._verify_from_n == 8
c4.update_range(4, 7)            # n=7 未变，max 8→7 < from=8
check('I3 update_range n 未变但 from 越界也作废',
      c4.state == 'STABLE' and c4._verify_from_n is None)

# ---- I1/I5：热关闭拆除 + start 复位标志（结构性 + 行为级） ----
src_w = open(os.path.join(ROOT, 'backup', 'compression_worker.py')).read()
disable_seg = src_w.split('_was_vegas_enabled and not self._vegas_enabled')[1]
check('I1 热关闭分支含 teardown（nas_api 模式）',
      "_teardown_vegas_nas()" in disable_seg.split('self._vegas_beta')[0])

from backup.compression_worker import CompressionWorker
w = CompressionWorker.__new__(CompressionWorker)
w._vegas_nas = None
w._vegas_nas_sampler = None
w._vegas_nas_stopped = True
assert w._ensure_vegas_nas() is None
check('I5 stop 后拒绝重建（已有覆盖保持）', True)

# start() 复位标志（结构性：start 函数体内含复位行）
start_seg = src_w.split('def start(self):')[1].split('def ')[0]
check('I5 start() 复位 _vegas_nas_stopped',
      '_vegas_nas_stopped = False' in start_seg)

print()
fails = [n for n, ok in PASS if not ok]
print(f'共 {len(PASS)} 项，失败 {len(fails)} 项' + (f'：{fails}' if fails else ''))
sys.exit(1 if fails else 0)
