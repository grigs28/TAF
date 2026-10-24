#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""bench_vegas.py — TAF 同款流程的 Vegas 全程调试测试

流程：扫描(/mnt/79-fs + /mnt/79-yzbak) → 预分组(12G×95%) → 压缩(tar|zstd)
不写带；压缩输出组完成即删；Vegas 按 79 API 压力+内部产出自动调并发。
结果存 /mnt/SSD/bench/。
"""
import json
import os
import shutil
import subprocess
import threading
import time
import urllib.request
from collections import deque

# ===== 配置 =====
SOURCES = ['/mnt/79-fs', '/mnt/79-yzbak']
SCAN_LIMIT_PER_SRC = 800 * 1024 ** 3     # 每源最多扫 800GB（控制总时长 ~2h）
BENCH = '/mnt/SSD/bench'
OUT_DIR = os.path.join(BENCH, 'out')
NAS_BASE = 'http://192.168.0.79:8090/api/disk-stats?simple=1'
# key 不入仓：优先环境变量 NAS_API_KEY，其次 BENCH 目录 nas_api.key 文件
_NK = os.environ.get('NAS_API_KEY', '')
if not _NK:
    try:
        with open(os.path.join(BENCH, 'nas_api.key')) as _f:
            _NK = _f.read().strip()
    except OSError:
        pass
NAS = NAS_BASE + ('&key=' + _NK if _NK else '')

MAX_FILE = 12 * 1024 ** 3
MIN_GROUP = MAX_FILE * 0.95              # 11.4GB

START_N, MIN_N, MAX_N = 4, 3, 12
PERIOD = 120                             # Vegas 决策周期（秒）
POLL = 10                                # 79 API 采样间隔
COOLDOWN_PERIODS = 2                     # 回退后冷却周期数
REPROBE_PERIODS = 8                      # 稳定期每 N 周期主动重探

# ===== 状态 =====
lock = threading.Lock()
desired_n = START_N
groups_left = []                         # 待压组
worker_seqs = []                         # 活跃 worker 序号
next_seq = [1]
done_src_bytes = [0]                     # 累计完成源字节（Vegas 产出口径）
period_src_bytes = [0]                   # 本周期完成源字节
group_log = []                           # 每组结果
decisions = []                           # Vegas 决策日志
nas_samples = []                         # 原始 79 采样
all_done = threading.Event()


def log(msg):
    print(time.strftime('%H:%M:%S'), msg, flush=True)


def nas():
    with urllib.request.urlopen(NAS, timeout=5) as r:
        return json.loads(r.read()).get('summary', {})


# ================= Phase 1: 扫描 =================
def scan():
    log(f'Phase1 扫描: {[s for s in SOURCES]}')
    files = []
    t0 = time.time()
    for src in SOURCES:
        acc = 0
        for root, dirs, fs in os.walk(src):
            for fn in fs:
                p = os.path.join(root, fn)
                try:
                    sz = os.path.getsize(p)
                except OSError:
                    continue
                if sz <= 0:
                    continue
                files.append([p, sz])
                acc += sz
                if acc >= SCAN_LIMIT_PER_SRC:
                    break
            if acc >= SCAN_LIMIT_PER_SRC:
                break
        log(f'  {src}: {acc / 1024**3:.1f} GB, 累计文件 {len(files):,}')
    total = sum(f[1] for f in files)
    with open(os.path.join(BENCH, 'scan.json'), 'w') as f:
        json.dump({'files': files, 'total_bytes': total,
                   'elapsed': round(time.time() - t0)}, f)
    log(f'扫描完成: {len(files):,} 文件 / {total/1024**3:.1f} GB / {time.time()-t0:.0f}s')
    return files


# ================= Phase 2: 预分组（TAF 三级规则） =================
def make_groups(files):
    groups, cur, cur_sz = [], [], 0
    for p, sz in files:
        if sz > MIN_GROUP:                    # 超大文件独立成组
            if cur:
                groups.append(cur)
                cur, cur_sz = [], 0
            groups.append([[p, sz]])
            continue
        cur.append([p, sz])
        cur_sz += sz
        if cur_sz >= MIN_GROUP:               # 贪心到 95% 成组
            groups.append(cur)
            cur, cur_sz = [], 0
    if cur:
        groups.append(cur)
    with open(os.path.join(BENCH, 'groups.json'), 'w') as f:
        json.dump(groups, f)
    log(f'分组完成: {len(groups)} 组')
    return groups


# ================= Phase 3: 压缩 worker =================
def compile_group(g, seq):
    out = os.path.join(OUT_DIR, f'g{seq}.tar.zst')
    lst = os.path.join(OUT_DIR, f'g{seq}.list')
    src_bytes = sum(s for _p, s in g)
    with open(lst, 'w') as f:
        for p, _s in g:
            f.write(p + '\n')
    t0 = time.time()
    with open(out, 'wb') as fo:
        p_tar = subprocess.Popen(['tar', '-cf', '-', '-P', '-T', lst],
                                 stdout=subprocess.PIPE,
                                 stderr=subprocess.DEVNULL)
        p_z = subprocess.Popen(['zstd', '-3', '-q', '-f'],
                               stdin=p_tar.stdout, stdout=fo,
                               stderr=subprocess.DEVNULL)
        p_tar.stdout.close()
        rc = p_z.wait() or p_tar.wait()
    dt = time.time() - t0
    out_sz = os.path.getsize(out) if os.path.exists(out) else 0
    for x in (lst, out):                      # 压缩完即删
        try:
            os.remove(x)
        except OSError:
            pass
    return src_bytes, out_sz, dt, rc


def worker(my_seq):
    global next_seq
    while not all_done.is_set():
        with lock:
            if len(worker_seqs) > desired_n or not groups_left:
                if my_seq in worker_seqs:
                    worker_seqs.remove(my_seq)
                return
            g = groups_left.pop(0)
        try:
            src_b, out_b, dt, rc = compile_group(g, my_seq)
            with lock:
                done_src_bytes[0] += src_b
                period_src_bytes[0] += src_b
                group_log.append({'seq': my_seq, 'files': len(g),
                                  'src_gb': round(src_b/1024**3, 2),
                                  'out_gb': round(out_b/1024**3, 2),
                                  'sec': round(dt, 1),
                                  'mbps': round(src_b/dt/1048576, 1),
                                  'n': desired_n})
            log(f'  [w{my_seq}] 组完成: {len(g)}文件 {src_b/1024**3:.2f}GB '
                f'{dt:.0f}s ({src_b/dt/1048576:.0f}MB/s) n={desired_n}')
        except Exception as e:
            log(f'  [w{my_seq}] 组失败: {e}')


def spawn_worker():
    with lock:
        s = next_seq[0]
        next_seq[0] += 1
        worker_seqs.append(s)
    threading.Thread(target=worker, args=(s,), daemon=True).start()


def sampler():
    """79 API 采样线程"""
    while not all_done.is_set():
        try:
            nas_samples.append({**nas(), 'ts': time.time()})
        except Exception:
            pass
        time.sleep(POLL)


# ================= Vegas 状态机 =================
def vegas_loop():
    global desired_n
    state = 'PROBING'          # PROBING / STABLE（COOLDOWN 只是倒计时，扣完回 PROBING）
    cooldown = 0
    stable_since = None
    last_P = None
    ewma_P = None              # 产出 EWMA（平滑小文件区"周期<组耗时"的量化抖动）
    stable_P = deque(maxlen=3)   # 稳定期产出参考
    t_next = time.time() + PERIOD
    while not all_done.is_set():
        time.sleep(2)
        if time.time() < t_next:
            continue
        t_next = time.time() + PERIOD
        with lock:
            raw_P = period_src_bytes[0] / PERIOD / 1048576   # MB/s（本周期原始产出）
            period_src_bytes[0] = 0
            n = len(worker_seqs) or desired_n
            left = len(groups_left)
        # EWMA 平滑：小文件区单组耗时(~186s) > 周期(120s)，raw_P 是完成组数的
        # 整数函数（±80% 阶梯跳变），直接用会误触发回退；0.6 历史 / 0.4 本周期
        P = raw_P if ewma_P is None else ewma_P * 0.6 + raw_P * 0.4
        ewma_P = P
        # 本周期 79 均值
        now = time.time()
        win = [s for s in nas_samples if now - s['ts'] <= PERIOD + POLL]
        def avg(k):
            v = [s.get(k, 0) for s in win if isinstance(s.get(k), (int, float))]
            return round(sum(v)/len(v), 1) if v else 0
        U, A, I, R = avg('md2_util'), avg('md2_await_ms'), avg('md2_iops'), avg('md2_read_mbps')

        act, why = 'hold', ''
        if left == 0 and not win:
            break
        if cooldown > 0:
            cooldown -= 1
            act, why = 'hold', f'冷却{cooldown}'
            if cooldown == 0:
                state = 'PROBING'   # 修复：冷却结束回 PROBING（否则死悬在 COOLDOWN）
        elif state == 'PROBING':
            if n >= MAX_N:
                state, act, why = 'STABLE', 'hold', '到上限'
                stable_since = time.time()
            elif last_P is None:
                act, why = '+1', '首周期上探'
            elif last_P > 0 and P < last_P * 0.88:
                state = 'COOLDOWN'; cooldown = COOLDOWN_PERIODS
                act, why = '-1', f'产出降({P:.0f}<-12%于{last_P:.0f})'
            elif abs(P - (last_P or 0)) <= (last_P or 1) * 0.05 and U >= 95:
                state = 'COOLDOWN'; cooldown = COOLDOWN_PERIODS
                act, why = '-1', f'到顶(util={U})'
            else:
                act, why = '+1', f'产出稳/升({P:.0f}vs{last_P:.0f},util={U})'
        elif state == 'STABLE':
            stable_P.append(P)
            ref = sum(stable_P) / max(1, len(stable_P))
            if len(stable_P) >= 2 and ref > 0 and P < ref * 0.85:
                state = 'PROBING'
                act, why = '-1', f'产出掉{P:.0f}vs均{ref:.0f}(形态变化)'
            elif (time.time() - (stable_since or time.time())) > REPROBE_PERIODS * PERIOD \
                    and n < MAX_N:
                state = 'PROBING'
                act, why = '+1', f'周期重探'
            else:
                act, why = 'hold', f'稳定(P={P:.0f})'
        # 快速保护（任何状态）——await 阈值按实测下调：79 打满时 await<11ms，
        # 25ms 永远够不着；15ms 才能在 IOPS 排队恶化时触发
        if U >= 98 and A >= 15 and n > MIN_N and act not in ('-1',):
            state = 'COOLDOWN'; cooldown = 1
            act, why = '-1', f'快速保护(util={U},await={A})'

        with lock:
            if act == '+1' and n < MAX_N:
                desired_n = n + 1
            elif act == '-1' and n > MIN_N:
                desired_n = n - 1        # worker 完成组后自行退出
        # spawn_worker 内部也要拿 lock，必须在锁外调用，
        # 否则同一线程二次 acquire 不可重入的 Lock → 死锁（首次+1上探即卡死）
        if act == '+1' and n < MAX_N:
            spawn_worker()
        if state == 'STABLE':
            stable_since = stable_since or time.time()

        decisions.append({'time': time.strftime('%H:%M:%S'), 'state': state,
                          'n': n, 'P_mbps': round(P, 1), 'raw_P': round(raw_P, 1),
                          'last_P': last_P,
                          'util': U, 'await': A, 'iops': I, 'read': R,
                          'act': act, 'why': why, 'groups_left': left})
        log(f'[Vegas] n={n} P={P:.0f}MB/s util={U}% await={A}ms iops={I} '
            f'→ {act} ({why}) 剩{left}组')
        last_P = P
        if left == 0:
            break


# ================= main =================
def main():
    os.makedirs(OUT_DIR, exist_ok=True)
    sf = os.path.join(BENCH, 'scan.json')
    if os.path.exists(sf):
        with open(sf) as f:
            d = json.load(f)
        files = d['files']
        log(f'复用已有扫描: {len(files):,} 文件 / {d["total_bytes"]/1024**3:.1f} GB')
    else:
        files = scan()
    groups_left.extend(make_groups(files))

    threading.Thread(target=sampler, daemon=True).start()
    for _ in range(START_N):
        spawn_worker()

    vegas = threading.Thread(target=vegas_loop)
    vegas.start()
    vegas.join()
    all_done.set()
    # 等 worker 收尾（最多 10 分钟）
    t0 = time.time()
    while worker_seqs and time.time() - t0 < 600:
        time.sleep(5)
    # 落盘
    with open(os.path.join(BENCH, 'vegas_decisions.csv'), 'w') as f:
        f.write('time,state,n,P_mbps,raw_P,last_P,util,await,iops,read,act,why,groups_left\n')
        for d in decisions:
            f.write(f"{d['time']},{d['state']},{d['n']},{d['P_mbps']},{d['raw_P']},{d['last_P']},"
                    f"{d['util']},{d['await']},{d['iops']},{d['read']},"
                    f"{d['act']},\"{d['why']}\",{d['groups_left']}\n")
    with open(os.path.join(BENCH, 'groups_result.json'), 'w') as f:
        json.dump(group_log, f, ensure_ascii=False)
    log(f'完成: {len(group_log)} 组 / {done_src_bytes[0]/1024**3:.1f} GB / '
        f'{len(decisions)} 个决策周期')


if __name__ == '__main__':
    main()
