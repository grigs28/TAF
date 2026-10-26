#!/usr/bin/env python3
"""1.9.88 第六轮修复回归（H1 无条件钳锁 / H2-H3 日志恢复档 / H4 teardown 行为级调用 /
H5 stop→start 生命周期 / H6 可移植路径）"""
import os
import time
import sys

# H6：从 __file__ 推仓库根（不再硬编码 /opt/TAF，克隆到任意目录都能跑）
ROOT = os.path.dirname(os.path.dirname(os.path.dirname(os.path.abspath(__file__))))
sys.path.insert(0, ROOT)
from backup.vegas_nas import VegasNasController
from backup.compression_worker import CompressionWorker


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


# ---- H1：热更新范围变更无条件钳锁（无需在飞验证） ----
# 模拟 worker 热更新块的语义：范围从 (4,10) 降到 (4,8)，n=8 未变、无在飞验证
c = VegasNasController(S(250), 8, 4, 10, log=lambda m: None)
c._min_good_n = 10                     # 旧上限下验证出的锁
c.max_n = 8                            # 热更新把上限降到 8
c.clamp_locks()                        # H1：无条件钳锁
check('H1 无在飞验证时锁也被钳到新上限内', c._min_good_n == 8,
      f'min_good={c._min_good_n}（max_n=8）')

# ---- H2：POST_ROLLBACK 有害恢复日志含真实档位 ----
logs = []
c2 = VegasNasController(S(195), 5, 4, 10, log=logs.append)
c2.state = 'PROBING'; c2._probe_base = 195
c2._t_next = 0; c2.tick(5, 10)
c2._t_next = 0; c2.tick(5, 10)   # → POST_ROLLBACK n=4
for r in (120, 122, 118):
    c2.sampler.read = r
    c2._t_next = 0; c2.tick(4, 10)
rec = [m for m in logs if '恢复' in m]
check('H2 回退恢复日志含真实档位（非"恢复None"）',
      any('恢复5' in m for m in rec), f'日志={[m[-30:] for m in rec]}')

# ---- H3：VERIFY 有害恢复日志含真实档位 ----
logs2 = []
c3 = VegasNasController(S(300), 10, 4, 10, log=logs2.append)
c3.state = 'STABLE'; c3._stable_since = time.time() - 2000; c3._probe_base = 300
c3._t_next = 0; c3.tick(9, 10)   # → VERIFY n=9
c3.sampler.read = 250            # 降档有害（-17%）
for _ in range(3):
    c3._t_next = 0; c3.tick(9, 10)
rec2 = [m for m in logs2 if '恢复' in m]
check('H3 下探恢复日志含真实档位（非"恢复None"）',
      any('恢复10' in m for m in rec2), f'日志={[m[-30:] for m in rec2]}')

# ---- H4：teardown 行为级调用（不再是源码字符串断言） ----
w = CompressionWorker.__new__(CompressionWorker)
w._vegas_nas_sampler = None
w._vegas_nas = object()          # 放一个哨兵，验证会被清掉
w._vegas_nas_no_url_logged = 'empty'
w._vegas_nas_fail_logged = True
w._vegas_nas_lost_logged = True
try:
    w._teardown_vegas_nas()      # G1 教训：必须真实调用
    ok = (w._vegas_nas is None and w._vegas_nas_no_url_logged is None
          and w._vegas_nas_fail_logged is False and w._vegas_nas_lost_logged is False)
except NameError as e:
    ok = False
check('H4 teardown 真实调用不抛错且清理完整', ok)

# ---- H5：stop 后 _ensure 拒绝重建；复位后可重建流程入口恢复 ----
w2 = CompressionWorker.__new__(CompressionWorker)
w2._vegas_nas = None
w2._vegas_nas_sampler = None
w2._vegas_nas_stopped = True     # 模拟 stop() 已置位
assert w2._ensure_vegas_nas() is None, '停止后应拒绝重建'
check('H5 stop 后 _ensure 拒绝重建 nas_api', True)

print()
fails = [n for n, ok in PASS if not ok]
print(f'共 {len(PASS)} 项，失败 {len(fails)} 项' + (f'：{fails}' if fails else ''))
sys.exit(1 if fails else 0)
