#!/usr/bin/env python3
"""1.9.84 复审修复回归测试（R1~R7 中可由控制器层验证的部分）"""
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


# ---- R1: 到内部上限时 plateau 照常累积（无增量回退不死） ----
c = VegasNasController(S(300), 10, 4, 10, log=lambda m: None)
c.state = 'PROBING'; c._probe_base = 300
# desired 等于 max_n → ext_cap=max_n → 不算外部限制生效
for i in range(3):
    c._t_next = 0
    c.tick(9, 10)
check('R1 内部上限处 plateau 仍累积并回退（无增量回退未死）',
      c.state == 'POST_ROLLBACK' and c.n == 9, f'state={c.state} n={c.n}')

# R1 对照：外部限制真实生效时仍不计 plateau
c2 = VegasNasController(S(300), 10, 2, 10, log=lambda m: None)
c2.state = 'PROBING'; c2._probe_base = 300
c2._t_next = 0; c2.tick(3, 3)     # 对齐到 3
for i in range(4):
    c2._t_next = 0; c2.tick(3, 3)
check('R1 对照：外部限并发（desired<max_n）仍豁免 plateau', c2.n == 3, f'n={c2.n}')

# ---- R4: 快速保护在 POST_ROLLBACK 期间作废验证 ----
c3 = VegasNasController(S(195), 5, 3, 10, log=lambda m: None)  # min=3，降到 4 后保护仍可在 4 触发
c3.state = 'PROBING'; c3._probe_base = 195
c3._t_next = 0; c3.tick(5, 10)
c3._t_next = 0; c3.tick(5, 10)   # → POST_ROLLBACK n=4
assert c3.state == 'POST_ROLLBACK', c3.state
c3.sampler.util = 100.0; c3.sampler.await_ms = 17.0
c3._t_next = 0
c3.tick(4, 10)                    # 快速保护打断
check('R4 保护打断 POST_ROLLBACK 时作废验证（无残留簿记）',
      c3.state == 'COOLDOWN' and c3._rollback_from_n is None
      and c3._rollback_samples == [],
      f'state={c3.state} from_n={c3._rollback_from_n}')

# ---- R5: POST_ROLLBACK 首拍即采样（不再被冷却吃掉） ----
c4 = VegasNasController(S(195), 5, 4, 10, log=lambda m: None)
c4.state = 'PROBING'; c4._probe_base = 195
c4._t_next = 0; c4.tick(5, 10)
c4._t_next = 0; c4.tick(5, 10)   # → POST_ROLLBACK
c4.sampler.read = 120
c4._t_next = 0
c4.tick(4, 10)
check('R5 POST_ROLLBACK 首观察拍即采样', len(c4._rollback_samples) == 1,
      f'样本={len(c4._rollback_samples)}')
# 再两拍出结论
for r in (122, 118):
    c4.sampler.read = r
    c4._t_next = 0; c4.tick(4, 10)
check('R5 三拍出结论恢复 5', c4.n == 5 and c4._min_good_n == 5,
      f'n={c4.n} min_good={c4._min_good_n}')

# ---- R7/T5: 构造器只校验不静默抬升（规范化在 worker 做） ----
try:
    VegasNasController(S(300), 8, 8, 8, log=lambda m: None)
    check('R7 构造器 max<=min 拒绝', False, '未抛错')
except ValueError:
    check('R7 构造器 max<=min 拒绝', True)

# ---- R7 worker 侧公式（压下限留调节空间） ----
base, red, vmin = 8, 0, 8
_new_max = max(base, 8)
_new_min = max(1, min(vmin, max(1, base - red)))
if _new_min >= _new_max:
    _new_min = max(1, _new_max - 1)
check('R7 下限=上限时压下限保留调节空间', _new_min == 7 and _new_max == 8,
      f'min={_new_min} max={_new_max}')

print()
fails = [n for n, ok in PASS if not ok]
print(f'共 {len(PASS)} 项，失败 {len(fails)} 项' + (f'：{fails}' if fails else ''))
sys.exit(1 if fails else 0)
