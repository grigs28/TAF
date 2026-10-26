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
        # R7/T5：规范化只在 worker（创建/热更新两处）做；构造器只校验，
        # 不再静默抬上限（原 max(min_n+1, max_n) 会越过 bench 封顶 8 且无感知）
        if max_n <= self.min_n:
            raise ValueError(f"max_n({max_n}) 必须大于 min_n({self.min_n})")
        self.max_n = max_n
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
        self._verify_read = 0.0
        self._verify_from_n = None
        self._verify_blocked_n = None   # 已验证"降并发有害"的并发值，不再重复验证
        self._verify_samples = []       # 验证期原始 read 样本
        self._rebuild_base = False      # 降档后重建基线标志（不上探）
        self._pressure_skip_until = 0.0 # 物理盘饱和判据的跳过期（防滞后指标连降）
        self._fastp_skip_until = 0.0    # md2 快速保护的跳过期（同 pressure，见 P1）
        # 无增量回退验证（防小文件区噪声导致误降）
        self._rollback_from_n = None
        self._rollback_read = 0.0
        self._rollback_samples = []
        self._min_good_n = None         # 已验证的最小可用并发（不得再降到其下；形态漂移时失效）
        self._t_next = 0.0          # 下次决策时刻（tick 节流）
        self.last_decision = None   # 供日志/诊断

    # ---- 采样统计 ----
    @staticmethod
    def _med(win, key):
        v = sorted(s.get(key, 0) for s in win
                   if isinstance(s.get(key), (int, float)))
        return round(v[len(v) // 2], 1) if v else 0.0

    def _abort_verification(self):
        """作废在飞的 VERIFY/POST_ROLLBACK（外部对齐/保护触发/热更新钳制时调用）。

        清样本与簿记，**不设任何锁**（_min_good_n/_verify_blocked_n 保持原值，
        由调用方决定是否保留历史结论）。所有改 n 的路径共用，保持清理一致。"""
        self._verify_samples = []
        self._rollback_samples = []
        self._verify_from_n = None
        self._rollback_from_n = None

    def update_range(self, min_n: int, max_n: int) -> bool:
        """热更新并发范围（I3：worker 侧对 8+ 私有字段的开放编码改写收敛于此，
        新增内部字段不会再出现"worker 热更新块漏清"的 H1 式残留）。

        返回 n 是否被钳制变化（供调用方决定日志文案）。
        规则：钳 n → 无条件钳锁 → n 变了则基线/漂移/EWMA 失效并作废在飞验证；
        n 未变但在飞验证的恢复目标越界（from_n > 新上限）→ 同样作废。"""
        min_n = max(1, min_n)
        if max_n <= min_n:
            max_n = min_n + 1        # 构造器不变量（调用方应已规范化，这里兜底）
        if self.min_n == min_n and self.max_n == max_n:
            return False
        prev_n = self.n
        self.min_n = min_n
        self.max_n = max_n
        self.n = min(max(self.n, min_n), max_n)
        self.clamp_locks()           # H1：范围变了无条件钳锁
        if self.n != prev_n:
            self._probe_base = None
            self._rebuild_base = True
            self._plateau = 0
            self._drift_reads = []
            with self._ewma_lock:
                self._read_ewma = None
        if self.state in ('VERIFY', 'POST_ROLLBACK'):
            _from = self._verify_from_n if self.state == 'VERIFY' else self._rollback_from_n
            if self.n != prev_n or (_from is not None and max_n < _from):
                self._abort_verification()
                self.state = 'STABLE'
                self._stable_since = time.time()
        return self.n != prev_n

    def clamp_locks(self):
        """把验证锁钳制到当前上限内（G2：范围变化/恢复判定三路共用）"""
        if self._min_good_n is not None and self._min_good_n > self.max_n:
            self._min_good_n = self.max_n
        if self._verify_blocked_n is not None and self._verify_blocked_n > self.max_n:
            self._verify_blocked_n = self.max_n

    def _skip_protections(self, now: float):
        """任一保护触发时同时跳过期生效（T1）：
        fastp/pressure 两个保护的跳过期若各自独立，一次真实饱和事件会被两个
        保护器交替放行连降 3 档（10-26 复审）。统一设置。"""
        self._fastp_skip_until = now + 2 * PERIOD
        self._pressure_skip_until = now + 2 * PERIOD

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
                # T7：EWMA 同样失效（旧值掺着旧并发的读速；R2/R3 热更新已清，
                # 此路径漏了同项清理，基线会偏高推迟上探 2~4 拍）
                with self._ewma_lock:
                    self._read_ewma = None
                # P3：外部对齐打断在飞验证——样本跨并发污染、verdict 的 +1 只恢复
                # 一档到不了 from_n、还会设永久锁。直接作废本次验证，不设任何锁。
                if self.state in ('VERIFY', 'POST_ROLLBACK'):
                    self._abort_verification()
                    self.state = 'STABLE'
                    self._stable_since = time.time()
                    self.log(f"[VegasNAS] 外部限并发 {self.n}，在飞验证作废（不设锁）")
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
        # F5：冷却期间不累积漂移样本（保护/过载善后的 read 连降是 370s 滑窗
        # 滞后造成的，非形态变化；累积会让冷却结束的漂移判定误清验证锁）
        if self._cooldown > 0:
            self._drift_reads = []
        else:
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
                    # 关键：并发已降低，旧基线是降档前的 read（必然更高），
                    # 不重置会导致"read 天然低于旧基线 → 再过载 → 再降"的连降
                    # 直到下限（2026-10-25 月度任务实测 7→6→5 连续误判）
                    self._probe_base = None
                    self._rebuild_base = True   # 重建基线时不上探（避免降档后立即回弹）
                self._plateau = 0
        elif drifting and self.state not in ('STABLE', 'VERIFY', 'POST_ROLLBACK'):
            # 注意：POST_ROLLBACK 必须豁免——降档后 read 下降正是要验证的信号，
            # 被漂移检测抢走会导致回退验证失效（实测场景1）
            self.state = 'STABLE'
            self._probe_base = R
            self._stable_since = now
            # P6：形态变了，此前验证的"最小可用并发"与"已验证有害并发"都失效
            self._min_good_n = None
            self._verify_blocked_n = None
            why = f'漂移中(read {self._drift_reads[0]:.0f}→{R:.0f},重置基准,清验证锁)'
        elif self.state == 'PROBING':
            # 上探同时受外部上限约束（扫描减批/磁盘限流压得比 Vegas 低时不上探）
            can_up = self.n < self.max_n and (ext_cap is None or self.n < ext_cap)
            if self._probe_base is None:
                self._probe_base = R
                if self._rebuild_base:
                    # 降档后重建基线：本拍只记录基线，不上探
                    self._rebuild_base = False
                    act, why = 'hold', f'降档后建基线({R:.0f})'
                elif fed and can_up:
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
                # F4：过载同样纳入共享跳过期——否则冷却拍 act=='hold' 会被
                # fastp/pressure 追加一刀（同一饱和事件 4 分钟内连降 2 档）
                self._skip_protections(now)
                act, why = '-1', f'过载({R:.0f}<-10%于{self._probe_base:.0f},util={U})'
            else:
                # P2/R1：仅"外部限制真实生效"（ext_cap 严格小于内部上限 max_n，
                # 即扫描减批/磁盘限流把并发压在内部上限之下）时不计 plateau——
                # 此时无增量是结构性的。
                # 到内部上限 max_n（ext_cap >= max_n 或无限流）时 plateau 照常累积，
                # 否则"配置并发高于最优、read 持平"的场景永远无法降档验证（R1）。
                if not can_up and ext_cap is not None and ext_cap < self.max_n:
                    self._plateau = 0
                    act, why = 'hold', f'受外部限并发({self.n}≥{ext_cap}，不计无增量)'
                else:
                    self._plateau += 1
                    if self._plateau >= PLATEAU_MAX:
                        # 无增量回退后转 POST_ROLLBACK 观察 3 周期：小文件区 read 噪声大，
                        # "无增量"可能误判（实测 5→4 后 read 从 195 掉到 120，-38%）。
                        # 3 周期后若 read 明显下降 → 降错了，恢复原并发并锁定。
                        if self._min_good_n is not None and (self.n - 1) < self._min_good_n:
                            # 该并发已验证不可降（降了会掉吞吐），保持
                            self.state = 'STABLE'
                            self._stable_since = now
                            self._probe_base = R
                            act, why = 'hold', (f'无增量但已锁定下限({self._min_good_n})，'
                                                f'保持 {self.n}')
                            self._plateau = 0
                        elif self.n <= self.min_n:
                            # I2：已到下限——回退不会实际降档（末尾 min_n 守卫挡住），
                            # 但验证会在同一并发上采样，噪声判"有害"后 +1 会把并发
                            # 抬到 from_n+1（超过回退前），日志还声称"恢复from_n"。
                            # 下限处直接保持，不进验证（与 VERIFY 的 n>min_n 守卫一致）
                            self.state = 'STABLE'
                            self._stable_since = now
                            self._probe_base = R
                            act, why = 'hold', f'无增量但已到并发下限({self.min_n})，保持'
                            self._plateau = 0
                        else:
                            self.state = 'POST_ROLLBACK'
                            # R5：不再设 cooldown——首观察拍被冷却吃掉不采样，
                            # 3 样本实际要 4 拍（约 8 分钟），与注释不符且拖长恢复
                            self._stable_since = None
                            self._rollback_from_n = self.n
                            self._rollback_read = R_raw   # P5：参考值用原始值，与样本同口径
                            self._rollback_samples = []
                            act, why = '-1', f'无增量x{self._plateau}({R:.0f}vs{self._probe_base:.0f})'
                    else:
                        act, why = 'hold', f'观察中({R:.0f}vs{self._probe_base:.0f})'
        elif self.state == 'POST_ROLLBACK':
            # 回退验证：收集 3 个周期的原始 read 取真中位（P4：2 样本取索引1=较大值，
            # 噪声下系统性高估 _med、误判"无损"；与 VERIFY 路径一致用 3 样本）
            self._rollback_samples.append(R_raw)
            if len(self._rollback_samples) < 3:
                act, why = 'hold', (f'回退观察({self.n},原读{self._rollback_read:.0f},'
                                    f'样本{len(self._rollback_samples)}/3)')
            else:
                _ref = self._rollback_read or 0
                _med = sorted(self._rollback_samples)[len(self._rollback_samples) // 2]
                if _ref > 0 and _med < _ref * 0.90:
                    # 降档后 read 下降 >10% → 回退错误，恢复原并发并锁定该值
                    _from_n = self._rollback_from_n        # H2：先存局部再清，
                    self._min_good_n = _from_n              #    否则日志输出"恢复None"
                    self._rollback_from_n = None            # G3：verdict 完成后清 from 字段
                    self.state = 'STABLE'
                    self._stable_since = now
                    self._probe_base = R
                    act, why = '+1', (f'回退验证:降档有害(原始中位{_med:.0f}比{_ref:.0f}低'
                                      f'{(1-_med/_ref)*100:.0f}%),恢复{_from_n}')
                else:
                    # 降档无损 → 保持更低并发
                    self._rollback_from_n = None   # G3
                    self.state = 'STABLE'
                    self._stable_since = now
                    self._probe_base = R
                    act, why = 'hold', (f'回退验证:降档无损(原始中位{_med:.0f}≈{_ref:.0f}),'
                                        f'保持 {self.n}')
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
            elif _due and self.n > self.min_n and self._verify_blocked_n != self.n \
                    and not (self._min_good_n is not None
                             and (self.n - 1) < self._min_good_n):
                # 下探验证：已在 max_n（无上探空间），降 1 档看 read 是否受损。
                # 目的：发现"当前并发其实过高"（bench：大文件区最优 6，配置 10 时
                # 原逻辑永远停在 10 无法验证）。降并发后若 read 明显下降 → 恢复。
                # T4：目标档已被回退验证判有害（_min_good_n）时不重复验证——
                # R1 打通 max_n plateau 后，否则每次漂移清锁都会白验证一轮
                self._verify_from_n = self.n
                self._verify_read = R_raw      # P5：参考值用原始值，与样本同口径
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
                    _from_n = self._verify_from_n          # H3：先存局部再清
                    self._verify_blocked_n = _from_n
                    self._verify_from_n = None             # G3
                    self.state = 'PROBING'
                    self._probe_base = R
                    self._plateau = 0
                    act, why = '+1', (f'验证:降并发有害(原始中位{_med:.0f}比{_ref:.0f}低'
                                      f'{_drop*100:.0f}%),恢复{_from_n}')
                else:
                    # 降并发 read 无明显损失 → 保持更低并发，延长稳定期后再验证
                    self._verify_blocked_n = None
                    self._verify_from_n = None     # G3
                    self.state = 'STABLE'
                    self._stable_since = now
                    self._probe_base = R
                    act, why = 'hold', (f'验证:降并发无损(原始中位{_med:.0f}≈{_ref:.0f}),'
                                        f'保持 {self.n}')
        # 快速保护（任何状态）
        # ① 阵列层：util 打满且排队恶化（bench 实战验证有效）。
        #    T1：两个保护共享跳过期（任一触发两个都跳），否则一次真实饱和事件
        #    被 fastp/pressure 交替放行连降 3 档（三轮复审）
        #    T3：只在 act=='hold' 时触发——不能覆盖 '+1' 恢复判定（否则
        #    验证判有害设了锁、n 却被保护反降一档，锁不变量当场破坏）
        if U >= FASTP_UTIL and A >= FASTP_AWAIT and self.n > self.min_n \
                and act == 'hold' and now >= self._fastp_skip_until:
            if self.state in ('VERIFY', 'POST_ROLLBACK'):
                # R4：在飞验证被保护打断必须显式作废（否则降档既无 verdict
                # 也无锁，同一档位的误判会原样再来一次）
                self._abort_verification()
                why_note = '（在飞验证作废）'
            else:
                why_note = ''
            self.state = 'COOLDOWN'
            self._cooldown = 1
            self._skip_protections(now)
            act, why = '-1', f'快速保护(util={U},await={A}){why_note}'
        # ② 物理盘层：disk_api 的 pressure 判定为 saturated（sata util≥95 且
        #    await≥20 或队列≥20）。阵列缓存会掩盖物理盘延迟，只看 md2 会漏
        #    （2026-10-25 实测：md2 await 仅 9~25ms，而 sata await 26~128ms、
        #    util 88~100%）。窗口内过半采样饱和才降，避免单点尖峰误触发。
        #    降档后 pressure 是 360s 窗口的**滞后指标**，须跳过 2 个周期再判，
        #    否则窗口内残留的饱和样本会让 Vegas 一路连降（2026-10-26 实测 7→6→5）。
        if act == 'hold' and self.n > self.min_n and now >= self._pressure_skip_until:
            _press = [s.get('pressure') for s in win if s.get('pressure')]
            if _press:
                _sat = _press.count('saturated')
                if _sat * 2 >= len(_press):      # ≥50% 采样为 saturated
                    if self.state in ('VERIFY', 'POST_ROLLBACK'):
                        self._abort_verification()
                        why_note = '（在飞验证作废）'
                    else:
                        why_note = ''
                    self.state = 'COOLDOWN'
                    self._cooldown = 1
                    self._skip_protections(now)
                    act, why = '-1', (f'物理盘饱和(pressure saturated '
                                      f'{_sat}/{len(_press)}){why_note}')

        if act == '+1':
            # F6/G2：恢复到 from_n 时不得越过当前 max_n（热更新可能在验证期间
            # 把上限降到 from_n 之下）；锁统一走 clamp_locks()
            self.n = min(self.n + 1, self.max_n)
            self.clamp_locks()
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
                or '快速保护' in why or '验证' in why or '回退' in why:
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

    # ---- 展示字段（P9：nas_api 模式下任务卡片的 Vegas 健康度/速率） ----
    @property
    def display_rate_mbps(self) -> float:
        """当前 read 平滑值（MB/s），供卡片显示"""
        with self._ewma_lock:
            return round(self._read_ewma or 0.0, 1)

    @property
    def display_base_mbps(self) -> float:
        """当前基线（MB/s），供卡片健康度 = rate/base 计算"""
        return round(self._probe_base or 0.0, 1)
