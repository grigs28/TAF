#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""bench_ladder_small.py — 小文件组（4000~5500 文件/组）阶梯并发实测 4→8

背景：bench_vegas 全程测试在 5 并发定格（状态机 bug），小文件区（P 从 600 掉到
195MB/s）从未探测更优并发。本脚本用相同源、相同压缩方式（tar|zstd）、相同 79
信号，实测各并发档位下小文件组的真实产出。

规则（每档至少 2 轮）：每档完成组数 = 2 × 并发数（4并发8组、5并发10组、…
8并发16组），分两轮、每轮恰好 n 组（每 worker 压 1 组/轮），两轮独立统计；
两轮方向一致才作数，矛盾时以均值为准并在日志标注。

源组循环使用（压缩只读源文件、产物即删，源不变可重压）。

用法（16 上，root）：
    cd /mnt/SSD/bench && nohup python bench_ladder_small.py >> ladder_small.log 2>&1 &

输出：/mnt/SSD/bench/ladder_small_result.csv
"""
import concurrent.futures as cf
import json
import os
import subprocess
import threading
import time
import urllib.request

BENCH = '/mnt/SSD/bench'
GROUPS_JSON = os.path.join(BENCH, 'groups.json')
OUT_DIR = os.path.join(BENCH, 'out')
CSV = os.path.join(BENCH, 'ladder_small_result.csv')
NAS_BASE = 'http://192.168.0.79:8090/api/disk-stats?simple=1'
_NK = os.environ.get('NAS_API_KEY', '')
if not _NK:
    try:
        with open(os.path.join(BENCH, 'nas_api.key')) as _f:
            _NK = _f.read().strip()
    except OSError:
        pass
NAS = NAS_BASE + ('&key=' + _NK if _NK else '')

LADDER = [4, 5, 6, 7, 8]
MIN_FILES, MAX_FILES = 4000, 5500     # 小文件组过滤（文件数/组）
ROUNDS = 2                             # 每档轮数；每轮完成组数 = n
MIN_FREE_GB = 60
POLL = 10

results = []
samples = []
_seq = 0
_seq_lock = threading.Lock()


def log(msg):
    print(time.strftime('%H:%M:%S'), msg, flush=True)


def nas():
    with urllib.request.urlopen(NAS, timeout=5) as r:
        return json.loads(r.read()).get('summary', {})


def free_gb():
    st = os.statvfs(OUT_DIR)
    return st.f_bavail * st.f_frsize / 1024 ** 3


def ensure_space():
    if free_gb() >= MIN_FREE_GB:
        return
    log(f'  SSD 剩余 {free_gb():.0f}GB 不足，先清 out/')
    for fn in os.listdir(OUT_DIR):
        try:
            os.remove(os.path.join(OUT_DIR, fn))
        except OSError:
            pass


def compile_group(g):
    """压缩一组（tar|zstd），返回 (源字节, 压缩字节, 秒)"""
    global _seq
    with _seq_lock:
        _seq += 1
        idx = _seq
    out = os.path.join(OUT_DIR, f'ls{idx}.tar.zst')
    lst = os.path.join(OUT_DIR, f'ls{idx}.list')
    with open(lst, 'w') as f:
        for p, _s in g:
            f.write(p + '\n')
    src_bytes = sum(s for _p, s in g)
    t0 = time.time()
    with open(out, 'wb') as fo:
        p_tar = subprocess.Popen(['tar', '-cf', '-', '-P', '-T', lst],
                                 stdout=subprocess.PIPE, stderr=subprocess.DEVNULL)
        p_z = subprocess.Popen(['zstd', '-3', '-q', '-f'],
                               stdin=p_tar.stdout, stdout=fo, stderr=subprocess.DEVNULL)
        p_tar.stdout.close()
        rc = p_z.wait() or p_tar.wait()
    dt = time.time() - t0
    out_sz = os.path.getsize(out) if os.path.exists(out) else 0
    for x in (lst, out):                     # 压完即删
        try:
            os.remove(x)
        except OSError:
            pass
    return src_bytes, out_sz, dt, rc


def sampler(stop):
    while not stop.is_set():
        try:
            samples.append({**nas(), 'ts': time.time()})
        except Exception:
            pass
        time.sleep(POLL)


def nas_avg(t0, t1, key):
    v = [s.get(key, 0) for s in samples
         if t0 <= s.get('ts', 0) <= t1 and isinstance(s.get(key), (int, float))]
    return round(sum(v) / len(v), 1) if v else 0


def run_round(n, pool, cursor, ex, round_no):
    """一轮：恰好提交并完成 n 组（并发上限 n）。返回 dict 统计。"""
    t0 = time.time()
    inflight, submitted = [], 0
    detail = []
    while True:
        while len(inflight) < n and submitted < n:
            g = pool[cursor[0] % len(pool)]
            cursor[0] += 1
            ensure_space()
            inflight.append(ex.submit(compile_group, g))
            submitted += 1
        done, _ = cf.wait(inflight, timeout=5, return_when=cf.FIRST_COMPLETED)
        for fut in done:
            inflight.remove(fut)
            try:
                src, comp, dt, rc = fut.result()
                detail.append((src, comp, dt))
                log(f'  [n={n} R{round_no}] 组完成: {src/1024**3:.2f}GB → '
                    f'{comp/1024**3:.2f}GB {dt:.0f}s ({src/dt/1048576:.0f}MB/s)')
            except Exception as e:
                log(f'  [n={n} R{round_no}] 组失败: {e}')
        if submitted >= n and not inflight:
            break
    t1 = time.time()
    total_src = sum(d[0] for d in detail)
    dur = t1 - t0
    return {'n': n, 'round': round_no, 'groups': len(detail),
            'src_gb': round(total_src / 1024 ** 3, 2),
            'out_gb': round(sum(d[1] for d in detail) / 1024 ** 3, 2),
            'sec': round(dur, 1),
            'yield_mbps': round(total_src / dur / 1048576, 1),
            'util': nas_avg(t0, t1, 'md2_util'),
            'await': nas_avg(t0, t1, 'md2_await_ms'),
            'iops': nas_avg(t0, t1, 'md2_iops'),
            'read': nas_avg(t0, t1, 'md2_read_mbps')}


def run_tier(n, pool, cursor):
    log(f'=== 档位 {n}（{ROUNDS}轮 × {n}组 = {ROUNDS * n}组）===')
    with cf.ThreadPoolExecutor(max_workers=n) as ex:
        for r in range(1, ROUNDS + 1):
            results.append(run_round(n, pool, cursor, ex, r))
    ys = [x['yield_mbps'] for x in results if x['n'] == n]
    if len(ys) >= 2:
        spread = abs(ys[0] - ys[1]) / max(ys) * 100
        tag = '一致' if spread <= 15 else f'两轮偏差{spread:.0f}%（注意）'
        log(f'  档位{n} 两轮产出: {ys[0]} / {ys[1]} MB/s —— {tag}')
    for fn in os.listdir(OUT_DIR):           # 清场
        try:
            os.remove(os.path.join(OUT_DIR, fn))
        except OSError:
            pass


def main():
    os.makedirs(OUT_DIR, exist_ok=True)
    with open(GROUPS_JSON) as f:
        groups = json.load(f)
    pool = [g for g in groups if MIN_FILES <= len(g) <= MAX_FILES]
    if not pool:
        log(f'groups.json 无 {MIN_FILES}~{MAX_FILES} 文件/组的组，退出')
        return
    log(f'小文件组池: {len(pool)} 组（循环重压），'
        f'总 {sum(sum(s for _, s in g) for g in pool) / 1024 ** 3:.1f} GB')

    stop = threading.Event()
    threading.Thread(target=sampler, args=(stop,), daemon=True).start()
    cursor = [0]
    try:
        for n in LADDER:
            run_tier(n, pool, cursor)
    finally:
        stop.set()

    with open(CSV, 'w') as f:
        f.write('n,round,groups,src_gb,out_gb,sec,yield_mbps,util,await,iops,read\n')
        for r in results:
            f.write(','.join(str(r[k]) for k in (
                'n', 'round', 'groups', 'src_gb', 'out_gb', 'sec',
                'yield_mbps', 'util', 'await', 'iops', 'read')) + '\n')
        # 档位均值行
        f.write('\n# n,avg_yield_mbps,groups_total\n')
        for n in LADDER:
            ys = [x['yield_mbps'] for x in results if x['n'] == n]
            gs = sum(x['groups'] for x in results if x['n'] == n)
            if ys:
                f.write(f'{n},{round(sum(ys) / len(ys), 1)},{gs}\n')
    log(f'CSV → {CSV}')
    avg = {n: round(sum(x['yield_mbps'] for x in results if x['n'] == n) /
                    max(1, len([1 for x in results if x['n'] == n])), 1) for n in LADDER}
    best = max(avg, key=avg.get)
    log(f'各档均值: {avg} → 最优 n={best}（{avg[best]}MB/s）')


if __name__ == '__main__':
    main()
