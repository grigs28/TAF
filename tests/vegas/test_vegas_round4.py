#!/usr/bin/env python3
"""1.9.86 第四轮修复回归（F1 稳态 tick / F4 过载跳过期 / F5 冷却不清漂移 / F6 恢复钳上限）"""
import time
import sys
sys.path.insert(0, '/opt/TAF')
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


# ---- F1 结构性验证：稳态下 tick 在范围不变时也被调用（worker 层缩进回归） ----
src = open('/opt/TAF/backup/compression_worker.py').read()
tick_line = next(l for l in src.split('\n') if 'nas_limit = ctrl.tick(' in l)
check('F1 tick 调用不在范围热更新 if 内（缩进 24）',
      tick_line.startswith(' ' * 24) and not tick_line.startswith(' ' * 28),
      f'缩进={len(tick_line) - len(tick_line.lstrip())}')

# ---- F4：过载降档纳入共享跳过期 ----
logs = []
c = VegasNasController(S(200, util=100.0, await_ms=17.0, pressure='saturated'),
                       8, 4, 10, log=logs.append)
c._probe_base = 300
c._t_next = 0
c.tick(8, 10)                    # read 200 < 270 → 过载 -1
over = [m for m in logs if '过载' in m]
check('F4 过载触发时设置共享跳过期', len(over) == 1
      and c._fastp_skip_until > 0 and c._pressure_skip_until > 0,
      f'过载={len(over)} fastp_skip={c._fastp_skip_until > 0} press_skip={c._pressure_skip_until > 0}')
# 下一拍冷却中 act=hold，保护不应追加
c._t_next = 0
c.tick(7, 10)
fastp = sum(1 for m in logs if '快速保护' in m)
press = sum(1 for m in logs if '物理盘饱和' in m)
check('F4 过载后冷却拍不被保护追加', fastp == 0 and press == 0,
      f'fastp={fastp} press={press}')

# ---- F5：冷却期间不累积漂移样本，保护善后不误清锁 ----
c2 = VegasNasController(S(300), 8, 4, 10, log=lambda m: None)
c2._min_good_n = 6; c2._verify_blocked_n = 8
c2.state = 'PROBING'; c2._probe_base = 300
# 冷却两拍期间 read 连降
c2._cooldown = 2; c2.state = 'COOLDOWN'
c2._t_next = 0
c2.sampler.read = 280; c2.tick(8, 10)
c2._t_next = 0
c2.sampler.read = 260; c2.tick(8, 10)
# 冷却结束一拍：仍降（240）→ 若冷却期样本被累积会凑成三连降触发漂移清锁
c2._t_next = 0
c2.sampler.read = 240; c2.tick(8, 10)
check('F5 冷却期样本不累积，保护善后不误清验证锁',
      c2._min_good_n == 6 and c2._verify_blocked_n == 8,
      f'min_good={c2._min_good_n} blocked={c2._verify_blocked_n}')

# ---- F6：恢复性 +1 钳制在 max_n 内、锁同步钳制 ----
c3 = VegasNasController(S(195), 5, 4, 5, log=lambda m: None)  # max_n=5，先 195 建基线
c3.state = 'PROBING'; c3._probe_base = 195
c3._t_next = 0; c3.tick(5, 5)
c3._t_next = 0; c3.tick(5, 5)   # → POST_ROLLBACK n=4
for r in (120, 122, 118):
    c3.sampler.read = r
    c3._t_next = 0
    c3.tick(4, 5)
check('F6 恢复判定不越过 max_n', c3.n == 5, f'n={c3.n}（max_n=5）')
# 构造 max_n=4 的场景：锁被钳到 4
c4 = VegasNasController(S(195), 4, 3, 4, log=lambda m: None)  # max_n=4，先 195 建基线
c4.state = 'PROBING'; c4._probe_base = 195
c4._t_next = 0; c4.tick(4, 4)
c4._t_next = 0; c4.tick(4, 4)   # → POST_ROLLBACK n=3
for r in (120, 122, 118):
    c4.sampler.read = r
    c4._t_next = 0
    c4.tick(3, 4)
check('F6 锁被钳制在 max_n 内', c4.n == 4 and c4._min_good_n == 4,
      f'n={c4.n} min_good={c4._min_good_n}')

print()
fails = [n for n, ok in PASS if not ok]
print(f'共 {len(PASS)} 项，失败 {len(fails)} 项' + (f'：{fails}' if fails else ''))
sys.exit(1 if fails else 0)
