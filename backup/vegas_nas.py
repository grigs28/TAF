"""vegas_nas — Vegas 自适应并发的 NAS API 信号源（方案C v2 状态机）

背景（bench 两轮 1588GB 全程验证，2026-10-24/25）：
  - 组完成结算产出信号在 3:1 大小文件组混合下不可换算，且小文件区
    单组耗时(170~315s) > 决策周期，信号量化跳变 ±80%
  - 79 disk_api 的 md2_read_mbps 是连续真实物理信号，NAS 硬件自动积分掉
    组构成的全部复杂性；两轮验证拐点定位均正确（大文件区 6 / 小文件区 5~6）
  - v1（滚动点对点比较）误判 5 次；v2（配对判定+漂移检测）误判 0 次，
    产出 +8%

v2 状态机（bench_vegas.py 同源，适配 TAF 的 clamp 语义）：
  决策值   read = 360s 滑窗中位数（覆盖 ≥3 个组周转，压平组间速率差 ±30%）
  PROBING  probe_base 只在上探/重置时更新（与滚动值无关，杜绝冷却后基准错位）
           增量确认: read ≥ base×1.03 → +1，base 更新
           过载:     read < base×0.90 且 util≥95 → -1（双证据）
           无增量:   连续 2 周期（plateau）→ -1
  漂移     近 3 周期 read 单调下滑 → 组序列形态漂移（非过载），转 STABLE 重置基线
  STABLE   满 8 周期定时重探（+1 重置基线）
  快速保护 util≥98 且 await≥15ms → 立即 -1（bench 实战验证：await 17.9 触发）

信号源失败保护：连续 3 个采样周期 API 不可用 → 输出 available=False，
调用方回退内部信号（compression_worker._update_vegas）。
"""
import json
import threading
import time
import urllib.request
from collections import deque

# ===== 决策参数（bench 1.9.65 两轮标定） =====
PERIOD = 120              # 决策周期（秒）
WIN_SPAN = 370            # read 滑窗（360s + 采样余量）
POLL = 10                 # 采样间隔（秒）
GAIN = 1.03               # 增量确认阈值（+3%）
OVERLOAD = 0.90           # 过载阈值（-10%，需 util 佐证）
PLATEAU_MAX = 2           # 无增量双周期确认
DRIFT_N = 3               # 连续单调下滑周期数 → 判形态漂移
REPROBE_PERIODS = 8       # STABLE 定时重探周期数
FASTP_UTIL = 98           # 快速保护：util%
FASTP_AWAIT = 15          # 快速保护：await ms
SAMPLES_MIN = 6           # 周期内最少样本数（不足则 hold）


class NasSignalSampler:
    """79 disk_api 采样线程（10s 一次，环形缓冲 48 样本≈8 分钟）"""

    def __init__(self, url: str, api_key: str):
        self._url = url
        self._req_headers = {'X-API-Key': api_key} if api_key else {}
        self._samples = deque(maxlen=48)   # [{md2_*:..., ts}]
        self._lock = threading.Lock()
        self._fail = 0                     # 连续失败次数
        self._stop = threading.Event()
        self._thread = None

    def start(self):
        if self._thread and self._thread.is_alive():
            return
        self._stop.clear()
        self._thread = threading.Thread(target=self._run, daemon=True,
                                        name='vegas-nas-sampler')
        self._thread.start()

    def stop(self):
        self._stop.set()

    def _run(self):
        while not self._stop.is_set():
            try:
                req = urllib.request.Request(self._url, headers=self._req_headers)
                with urllib.request.urlopen(req, timeout=5) as r:
                    summary = json.loads(r.read()).get('summary', {})
                with self._lock:
                    self._samples.append({**summary, 'ts': time.time()})
                    self._fail = 0
            except Exception:
                with self._lock:
                    self._fail += 1
            self._stop.wait(POLL)

    def window(self, span: float = WIN_SPAN):
        """返回最近 span 秒的样本列表"""
        now = time.time()
        with self._lock:
            return [s for s in self._samples if now - s.get('ts', 0) <= span]

    @property
    def fail_count(self) -> int:
        with self._lock:
            return self._fail

    @property
    def ok(self) -> bool:
        """采样健康：连续失败 < 3 且缓冲有数据"""
        with self._lock:
            return self._fail < 3 and len(self._samples) >= SAMPLES_MIN


class VegasNasController:
    """v2 状态机：read 信号 → 并发上限（clamp 到 desired_batches 之下）"""

    def __init__(self, sampler: NasSignalSampler, start_n: int, min_n: int,
                 max_n: int, log=None):
        self.sampler = sampler
        self.log = log or (lambda msg: None)
        self.min_n = max(1, min_n)
        self.max_n = max(self.min_n + 1, max_n)
        # 并发值：从 start_n 开始自主调节
        self.n = min(max(start_n, self.min_n), self.max_n)
        self.state = 'PROBING'
        self._cooldown = 0
        self._stable_since = None
        self._probe_base = None
        self._plateau = 0
        self._drift_reads = []
        self._t_next = 0.0          # 下次决策时刻（tick 节流）
        self.last_decision = None   # 供日志/诊断

    # ---- 采样统计 ----
    @staticmethod
    def _med(win, key):
        v = sorted(s.get(key, 0) for s in win
                   if isinstance(s.get(key), (int, float)))
        return round(v[len(v) // 2], 1) if v else 0.0

    # ---- 状态机（每 PERIOD 秒一拍） ----
    def tick(self, active_batches: int) -> int:
        """由压缩主循环调用（自带节流）。返回当前并发上限。

        active_batches: 调用时的在途压缩组数（低于 n 时不做 +1 决策，
        避免没喂满就把"没喂满"当"无增量"——bench 里 worker 池恒满无此问题）
        """
        now = time.time()
        if now < self._t_next:
            return self.n
        self._t_next = now + PERIOD

        win = self.sampler.window(WIN_SPAN)
        if len(win) < SAMPLES_MIN:
            self.last_decision = {'act': 'hold', 'why': f'样本不足({len(win)})'}
            return self.n
        if not self.sampler.ok:
            self.last_decision = {'act': 'hold', 'why': 'API失联'}
            return self.n

        R = self._med(win, 'md2_read_mbps')
        U = self._med(win, 'md2_util')
        A = self._med(win, 'md2_await_ms')
        I = self._med(win, 'md2_iops')
        act, why = 'hold', ''
        fed = active_batches >= self.n     # 并发是否已喂满
        prev_n = self.n                    # 日志展示"决策前→后"

        # 漂移检测（优先于一切回退）
        self._drift_reads.append(R)
        if len(self._drift_reads) > DRIFT_N:
            self._drift_reads.pop(0)
        drifting = (len(self._drift_reads) == DRIFT_N and
                    self._drift_reads[0] > self._drift_reads[1] > self._drift_reads[2])

        if self._cooldown > 0:
            self._cooldown -= 1
            act, why = 'hold', f'冷却{self._cooldown}'
            if self._cooldown == 0:
                self.state = 'PROBING'
                self._plateau = 0
        elif drifting and self.state != 'STABLE':
            self.state = 'STABLE'
            self._probe_base = R
            self._stable_since = now
            why = f'漂移中(read {self._drift_reads[0]:.0f}→{R:.0f},重置基准)'
        elif self.state == 'PROBING':
            if self._probe_base is None:
                self._probe_base = R
                if fed and self.n < self.max_n:
                    act, why = '+1', '首周期上探'
            elif not fed:
                act, why = 'hold', '未喂满'
            elif R >= self._probe_base * GAIN:
                self._plateau = 0
                if self.n < self.max_n:
                    act, why = '+1', f'增量确认({R:.0f}≥{self._probe_base:.0f}+3%)'
                    self._probe_base = R
                else:
                    self.state = 'STABLE'
                    self._stable_since = now
                    why = '到上限'
            elif R < self._probe_base * OVERLOAD and U >= 95:
                self.state = 'COOLDOWN'
                self._cooldown = 2
                act, why = '-1', f'过载({R:.0f}<-10%于{self._probe_base:.0f},util={U})'
            else:
                self._plateau += 1
                if self._plateau >= PLATEAU_MAX:
                    self.state = 'COOLDOWN'
                    self._cooldown = 2
                    act, why = '-1', f'无增量x{self._plateau}({R:.0f}vs{self._probe_base:.0f})'
                else:
                    act, why = 'hold', f'观察中({R:.0f}vs{self._probe_base:.0f})'
        elif self.state == 'STABLE':
            if (now - (self._stable_since or now)) > REPROBE_PERIODS * PERIOD \
                    and self.n < self.max_n and fed:
                self.state = 'PROBING'
                self._probe_base = R
                self._plateau = 0
                act, why = '+1', '周期重探(重置基线)'
            else:
                act, why = 'hold', f'稳定(read={R:.0f})'
        # 快速保护（任何状态）
        if U >= FASTP_UTIL and A >= FASTP_AWAIT and self.n > self.min_n \
                and act not in ('-1',):
            self.state = 'COOLDOWN'
            self._cooldown = 1
            act, why = '-1', f'快速保护(util={U},await={A})'

        if act == '+1':
            self.n += 1
        elif act == '-1' and self.n > self.min_n:
            self.n -= 1     # 收敛式：调用方按完成即不补实现平滑缩容

        self.last_decision = {
            'time': time.strftime('%H:%M:%S'), 'state': self.state, 'n': self.n,
            'read': R, 'iops': I, 'util': U, 'await': A,
            'probe_base': self._probe_base, 'drift': drifting,
            'act': act, 'why': why,
        }
        # 日志：动作/探测期/形态漂移/到上限 均需可见（运维判断依据）
        if act != 'hold' or self.state == 'PROBING' or '漂移' in why or '到上限' in why \
                or '快速保护' in why:
            self.log(f"[VegasNAS] n={prev_n}→{self.n} read={R:.0f}MB/s util={U}% "
                     f"await={A}ms 基线{self._probe_base or 0:.0f} "
                     f"→ {act} ({why})")
        return self.n

    @property
    def available(self) -> bool:
        """信号源健康（采样 ok 且决策已在跑）"""
        return self.sampler.ok and self._probe_base is not None

    @property
    def signal_lost(self) -> bool:
        """API 已失联（连续 3 次采样失败 = 30s）——调用方应回退内部信号。

        注意：刚启动时 fail_count=0（尚未采到样本），不算失联，
        此时控制器只会 hold（样本不足），不会误调并发。
        """
        return self.sampler.fail_count >= 3
