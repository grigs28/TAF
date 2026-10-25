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
READ_EWMA_ALPHA = 0.35    # read 平滑权重（新值占比）

# 生产实测（2026-10-25）：360s 中位数 read 仍受组形态混合影响，噪声达 ±10%
# （320~356），与判决阈值 +3%/-10% 重叠 → 每次上探 2~6 分钟即回退，形成 9↔10
# 振荡。给 read 加 EWMA（0.35 新值）后单周期噪声压到 ±3% 内，阈值保持
# bench 标定值不变。


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
        self._read_ewma = None      # read 平滑值（降噪，见 READ_EWMA_ALPHA）
        self._ewma_lock = threading.Lock()
        # 下探验证（STABLE 期降 1 档确认当前并发是否过高）
        self._verify_since = None
        self._verify_read = 0.0
        self._verify_from_n = None
        self._verify_blocked_n = None   # 已验证"降并发有害"的并发值，不再重复验证
        self._verify_samples = []       # 验证期原始 read 样本
        self._t_next = 0.0          # 下次决策时刻（tick 节流）
        self.last_decision = None   # 供日志/诊断

    # ---- 采样统计 ----
    @staticmethod
    def _med(win, key):
        v = sorted(s.get(key, 0) for s in win
                   if isinstance(s.get(key), (int, float)))
        return round(v[len(v) // 2], 1) if v else 0.0

    # ---- 状态机（每 PERIOD 秒一拍） ----
    def tick(self, active_batches: int, desired_batches: int = None) -> int:
        """由压缩主循环调用（自带节流）。返回当前并发上限。

        active_batches: 调用时的在途压缩组数（低于 n 时不做 +1 决策，
        避免没喂满就把"没喂满"当"无增量"——bench 里 worker 池恒满无此问题）
        desired_batches: 外部已算出的目标并发（含扫描减批/磁盘限流/守卫）。
        外部限制比 Vegas 更严时，本控制器**对齐**到该值并重置基准：
        否则 fed 永远为假 → 扫描与压缩并发期间 Vegas 全程不决策（生产必现）。
        """
        # 外部限制对齐（优先级高于 Vegas 自身判断）
        ext_cap = None
        if desired_batches is not None:
            ext_cap = max(self.min_n, min(int(desired_batches), self.max_n))
            if ext_cap < self.n:
                self.n = ext_cap
                self._probe_base = None      # 并发变了，旧基准失效
                self._plateau = 0
                self._drift_reads = []
                self.last_decision = {'act': 'hold', 'why': f'外部限并发→{ext_cap}'}
                self.log(f"[VegasNAS] 外部限并发 {self.n}（desired={desired_batches}），待下次决策")
                return self.n

        now = time.time()
        if now < self._t_next:
            return self.n
        self._t_next = now + PERIOD

        win = self.sampler.window(WIN_SPAN)
        if len(win) < SAMPLES_MIN:
            self.last_decision = {'act': 'hold', 'why': f'样本不足({len(win)})'}
            self.log(f"[VegasNAS] 采样不足 {len(win)}/{SAMPLES_MIN}（fail={self.sampler.fail_count}），本周期不决策")
            return self.n
        if not self.sampler.ok:
            self.last_decision = {'act': 'hold', 'why': 'API失联'}
            self.log(f"[VegasNAS] 采样异常 fail={self.sampler.fail_count} 样本={len(win)}，本周期不决策")
            return self.n

        R_raw = self._med(win, 'md2_read_mbps')
        # EWMA 降噪：生产 read 单周期噪声 ±10%（组形态混合），判决阈值仅 3%~10%，
        # 不平滑会每次上探即回退（9↔10 振荡）。平滑后单周期噪声压到 ±3% 内。
        with self._ewma_lock:
            self._read_ewma = (R_raw if self._read_ewma is None
                               else self._read_ewma * (1 - READ_EWMA_ALPHA)
                               + R_raw * READ_EWMA_ALPHA)
            R = round(self._read_ewma, 1)
        U = self._med(win, 'md2_util')
        A = self._med(win, 'md2_await_ms')
        I = self._med(win, 'md2_iops')
        act, why = 'hold', ''
        # 喂满判据：允许差 1（生产主循环在组完成→补位之间有间隙，实测常驻 7/8；
        # 若要求 active>=n，则永远不会决策——2026-10-25 生产实测）
        fed = active_batches >= max(1, self.n - 1)
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
                # 仅过载（state=COOLDOWN）冷却后回探测；无增量回退设为 STABLE，
                # 需保持稳定观察，不能被冷却结束逻辑覆盖
                if self.state == 'COOLDOWN':
                    self.state = 'PROBING'
                self._plateau = 0
        elif drifting and self.state not in ('STABLE', 'VERIFY'):
            self.state = 'STABLE'
            self._probe_base = R
            self._stable_since = now
            why = f'漂移中(read {self._drift_reads[0]:.0f}→{R:.0f},重置基准)'
        elif self.state == 'PROBING':
            # 上探同时受外部上限约束（扫描减批/磁盘限流压得比 Vegas 低时不上探）
            can_up = self.n < self.max_n and (ext_cap is None or self.n < ext_cap)
            if self._probe_base is None:
                self._probe_base = R
                if fed and can_up:
                    act, why = '+1', '首周期上探'
            elif not fed:
                act, why = 'hold', '未喂满'
            elif R >= self._probe_base * GAIN:
                self._plateau = 0
                if can_up:
                    act, why = '+1', f'增量确认({R:.0f}≥{self._probe_base:.0f}+3%)'
                    self._probe_base = R
                else:
                    self.state = 'STABLE'
                    self._stable_since = now
                    why = '到上限' if self.n >= self.max_n else '受外部限并发'
            elif R < self._probe_base * OVERLOAD and U >= 95:
                self.state = 'COOLDOWN'
                self._cooldown = 2
                act, why = '-1', f'过载({R:.0f}<-10%于{self._probe_base:.0f},util={U})'
            else:
                self._plateau += 1
                if self._plateau >= PLATEAU_MAX:
                    # 无增量回退后转 STABLE（不连续下探）：若 NAS 已饱和、
                    # read 与并发无关，连续"无增量"会把并发一路压到下限；
                    # 转 STABLE 后由 8 周期定时重探验证是否需要再降。
                    self.state = 'STABLE'
                    self._cooldown = 1
                    self._stable_since = None
                    act, why = '-1', f'无增量x{self._plateau}({R:.0f}vs{self._probe_base:.0f})'
                else:
                    act, why = 'hold', f'观察中({R:.0f}vs{self._probe_base:.0f})'
        elif self.state == 'STABLE':
            if self._stable_since is None:
                self._stable_since = now      # 冷却结束首次进入，开始计时
            _due = (now - self._stable_since) > REPROBE_PERIODS * PERIOD and fed
            if _due and self.n < self.max_n:
                # 上探验证：还有空间，+1 看重探后 read 是否回升
                self.state = 'PROBING'
                self._probe_base = R
                self._plateau = 0
                act, why = '+1', '周期重探(重置基线)'
            elif _due and self.n > self.min_n and self._verify_blocked_n != self.n:
                # 下探验证：已在 max_n（无上探空间），降 1 档看 read 是否受损。
                # 目的：发现"当前并发其实过高"（bench：大文件区最优 6，配置 10 时
                # 原逻辑永远停在 10 无法验证）。降并发后若 read 明显下降 → 恢复。
                self._verify_from_n = self.n
                self._verify_read = R
                self._verify_since = now
                self._verify_samples = []      # 收集原始 read（EWMA 会掩盖降幅）
                self.state = 'VERIFY'
                act, why = '-1', f'下探验证({self.n}→{self.n-1},基线{R:.0f})'
            else:
                if self.n >= self.max_n:
                    act, why = 'hold', f'稳定(read={R:.0f},已在并发上限)'
                else:
                    act, why = 'hold', f'稳定(read={R:.0f})'
        elif self.state == 'VERIFY':
            # 下探验证观察期：收集 3 个周期的**原始** read（EWMA 平滑会把
            # 真实降幅从 -10% 掩盖成 -3%，单用平滑值会误判"无损"），取中位数判读
            self._verify_samples.append(R_raw)
            if len(self._verify_samples) < 3:
                # 每拍 120 秒，3 拍≈6 分钟足够新并发稳定
                act, why = 'hold', (f'下探观察({self.n},原读{self._verify_read:.0f},'
                                    f'样本{len(self._verify_samples)}/3)')
            else:
                _ref = self._verify_read or 0
                _med = sorted(self._verify_samples)[len(self._verify_samples) // 2]
                _drop = (_ref - _med) / _ref if _ref > 0 else 0.0
                if _drop > 0.05:
                    # 降并发导致 read 明显下降 → 原并发更优，恢复并锁定不再验证该值
                    self._verify_blocked_n = self._verify_from_n
                    self.state = 'PROBING'
                    self._probe_base = R
                    self._plateau = 0
                    act, why = '+1', (f'验证:降并发有害(原始中位{_med:.0f}比{_ref:.0f}低'
                                      f'{_drop*100:.0f}%),恢复{self._verify_from_n}')
                else:
                    # 降并发 read 无明显损失 → 保持更低并发，延长稳定期后再验证
                    self._verify_blocked_n = None
                    self.state = 'STABLE'
                    self._stable_since = now
                    self._probe_base = R
                    act, why = 'hold', (f'验证:降并发无损(原始中位{_med:.0f}≈{_ref:.0f}),'
                                        f'保持 {self.n}')
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
                or '快速保护' in why or '验证' in why:
            self.log(f"[VegasNAS] n={prev_n}→{self.n} read={R:.0f}(原始{R_raw:.0f})MB/s "
                     f"util={U}% await={A}ms 基线{self._probe_base or 0:.0f} "
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
