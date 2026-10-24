#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""bench_ladder_small.py — 小文件组阶梯并发实测（全量备组版，测 6/7/8）

规则：
  ① 组池准备：全量扫描 /mnt/79-yzbak（不限量），TAF 同款分组（12G×95% 贪心），
     过滤"4000~5500 文件/组"的小文件组，随机洗牌（seed 固定，每档/每轮组形态
     均匀）→ small_groups.json；已存在则直接复用。
  ② 每档 2 轮 × n 组，每组只用一次（绝不重压——上轮 7/8 档缓存污染的教训）；
     组池不足直接报错退出，不降级。
  ③ 两轮产出偏差 >15% 时自动加跑第 3 轮（仲裁轮），CSV 记 round=3。
  ④ 压缩同款：tar|zstd -3；组完成即删产物；79 API 采样（带 key）。

用法（16 上，root）：
    cd /mnt/SSD/bench && nohup python bench_ladder_small.py >> ladder_small.log 2>&1 &

输出：/mnt/SSD/bench/ladder_small_result.csv
"""
import concurrent.futures as cf
import json
import os
import random
import subprocess
import threading
import time
import urllib.request

BENCH = '/mnt/SSD/bench'
SRC = '/mnt/79-yzbak'                       # 小文件区（大文件区出不了 4000+ 文件/组）
SCAN_JSON = os.path.join(BENCH, 'scan_full.json')   # 全量扫描文件列表（保留复用，勿删）
POOL_JSON = os.path.join(BENCH, 'small_groups.json')
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

LADDER = [6, 7, 8]
MAX_N = 12                            # 8 未到顶峰自动 +1 续测的硬上限
MIN_FILES, MAX_FILES = 4000, 5500     # 小文件组过滤（文件数/组）
ROUNDS = 2                             # 每档基础轮数；偏差>ARBITRATE_SPREAD 加仲裁轮
ARBITRATE_SPREAD = 15                  # 两轮产出偏差百分比上限
MAX_FILE = 12 * 1024 ** 3
MIN_GROUP = MAX_FILE * 0.95
MIN_FREE_GB = 60
POLL = 10

results = []
samples = []
_seq = 0
_seq_lock = threading.Lock()
pool = []                              # 全局组池（只 pop 不回绕）


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


# ---------------- 组池准备 ----------------
def scan_full():
    """全量扫描源；结果存 scan_full.json（保留复用，存在则不重扫）"""
    if os.path.exists(SCAN_JSON):
        with open(SCAN_JSON) as f:
            d = json.load(f)
        log(f'复用全量扫描 {SCAN_JSON}: {len(d["files"]):,} 文件 / '
            f'{d["total_bytes"] / 1024 ** 3:.1f} GB（当初耗时 {d.get("elapsed", "?")}s）')
        return d['files']
    log(f'全量扫描 {SRC}（不限量）…')
    files = []
    t0 = time.time()
    acc = 0
    for root, dirs, fs in os.walk(SRC):
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
            if acc // (200 * 1024 ** 3) != (acc - sz) // (200 * 1024 ** 3):
                log(f'  已扫 {acc / 1024 ** 3:.0f}GB / {len(files):,} 文件 / {time.time() - t0:.0f}s')
    log(f'扫描完成: {len(files):,} 文件 / {acc / 1024 ** 3:.1f} GB / {time.time() - t0:.0f}s')
    with open(SCAN_JSON, 'w') as f:
        json.dump({'files': files, 'total_bytes': acc,
                   'elapsed': round(time.time() - t0)}, f)
    return files


def build_pool():
    """全量扫描（可复用）→ TAF 分组 → 过滤小文件组 → 洗牌 → 存盘"""
    files = scan_full()

    groups, cur, cur_sz = [], [], 0
    for p, sz in files:
        if sz > MIN_GROUP:
            if cur:
                groups.append(cur)
                cur, cur_sz = [], 0
            groups.append([[p, sz]])
            continue
        cur.append([p, sz])
        cur_sz += sz
        if cur_sz >= MIN_GROUP:
            groups.append(cur)
            cur, cur_sz = [], 0
    if cur:
        groups.append(cur)

    small = [g for g in groups if MIN_FILES <= len(g) <= MAX_FILES]
    log(f'分组 {len(groups)} 组，其中小文件组({MIN_FILES}~{MAX_FILES}文件) {len(small)} 组 '
        f'/ {sum(sum(s for _, s in g) for g in small) / 1024 ** 3:.1f} GB')
    random.Random(42).shuffle(small)    # 固定 seed：每档/每轮形态均匀、可复现
    with open(POOL_JSON, 'w') as f:
        json.dump(small, f)
    return small


def load_pool():
    if os.path.exists(POOL_JSON):
        with open(POOL_JSON) as f:
            p = json.load(f)
        log(f'复用组池 {POOL_JSON}: {len(p)} 组')
        return p
    return build_pool()


def take_group():
    """取一组（只出不回绕）"""
    return pool.pop(0) if pool else None


# ---------------- 压缩 ----------------
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
    for x in (lst, out):
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


def run_round(n, ex, round_no):
    """一轮：恰好提交并完成 n 组（并发上限 n），返回统计 dict"""
    t0 = time.time()
    inflight, submitted = [], 0
    detail = []
    while True:
        while len(inflight) < n and submitted < n:
            g = take_group()
            if g is None:
                log(f'  ⚠️ 组池耗尽（已提交 {submitted}/{n}）——数据不足，中止！')
                raise SystemExit(2)
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


def run_tier(n):
    """一个档位：ROUNDS 轮，偏差大加仲裁轮"""
    need = n * ROUNDS + n               # 预留仲裁轮余量
    if len(pool) < n * ROUNDS:
        log(f'⚠️ 档位{n}: 组池剩 {len(pool)} < {n * ROUNDS}，数据不足！')
        raise SystemExit(2)
    log(f'=== 档位 {n}（{ROUNDS}轮 × {n}组，池剩 {len(pool)}，含仲裁余量需 {need}）===')
    with cf.ThreadPoolExecutor(max_workers=n) as ex:
        for r in range(1, ROUNDS + 1):
            results.append(run_round(n, ex, r))
    ys = [x['yield_mbps'] for x in results if x['n'] == n]
    spread = abs(ys[0] - ys[1]) / max(ys) * 100
    if spread > ARBITRATE_SPREAD and len(pool) >= n:
        log(f'  两轮偏差 {spread:.0f}% > {ARBITRATE_SPREAD}% → 加跑仲裁轮')
        with cf.ThreadPoolExecutor(max_workers=n) as ex:
            results.append(run_round(n, ex, 3))
        ys = [x['yield_mbps'] for x in results if x['n'] == n]
        log(f'  三轮产出: {ys} MB/s')
    else:
        log(f'  档位{n} 两轮产出: {ys[0]} / {ys[1]} MB/s —— 偏差 {spread:.0f}% '
            f'({"一致" if spread <= ARBITRATE_SPREAD else "注意"})')
    for fn in os.listdir(OUT_DIR):
        try:
            os.remove(os.path.join(OUT_DIR, fn))
        except OSError:
            pass


def tier_avg(n):
    ys = [x['yield_mbps'] for x in results if x['n'] == n]
    return round(sum(ys) / len(ys), 1) if ys else None


def main():
    os.makedirs(OUT_DIR, exist_ok=True)
    pool.extend(load_pool())
    total_need = sum(n * ROUNDS for n in LADDER)
    log(f'组池 {len(pool)} 组；基础需求 {total_need} 组（{"够" if len(pool) >= total_need else "不够！"}），'
        f'仲裁余量按需另计；8 档未到顶自动 +1 续测（上限 {MAX_N}）')

    stop = threading.Event()
    threading.Thread(target=sampler, args=(stop,), daemon=True).start()
    ladder = list(LADDER)
    try:
        i = 0
        while i < len(ladder):
            n = ladder[i]
            try:
                run_tier(n)
            except SystemExit as e:
                if e.code == 2:
                    log(f'⛔ 组池耗尽，未测完档位 {n}；已有档位结果照常输出')
                    break
                raise
            # 顶峰判定：最后一档产出不低于前档 = 未到顶，+1 续测
            if i == len(ladder) - 1 and i >= 1:
                avg, prev = tier_avg(n), tier_avg(ladder[i - 1])
                if avg is not None and prev is not None and avg >= prev:
                    nxt = n + 1
                    if nxt > MAX_N:
                        log(f'{n} 档产出 {avg} ≥ 前档 {prev}（未到顶），已达硬上限 {MAX_N}，停止')
                    elif len(pool) < nxt * ROUNDS:
                        log(f'{n} 档产出 {avg} ≥ 前档 {prev}（未到顶），但组池剩 '
                            f'{len(pool)} < {nxt * ROUNDS}，无法续测，停止')
                    else:
                        log(f'{n} 档产出 {avg} ≥ 前档 {prev} —— 未到顶峰，自动加测 {nxt} 档')
                        ladder.append(nxt)
            i += 1
    finally:
        stop.set()

    with open(CSV, 'w') as f:
        f.write('n,round,groups,src_gb,out_gb,sec,yield_mbps,util,await,iops,read\n')
        for r in results:
            f.write(','.join(str(r[k]) for k in (
                'n', 'round', 'groups', 'src_gb', 'out_gb', 'sec',
                'yield_mbps', 'util', 'await', 'iops', 'read')) + '\n')
        f.write('\n# n,avg_yield_mbps,rounds,groups_total\n')
        for n in ladder:
            rows = [x for x in results if x['n'] == n]
            if rows:
                f.write(f'{n},{tier_avg(n)},{len(rows)},{sum(x["groups"] for x in rows)}\n')
    log(f'CSV → {CSV}')
    done = {n: tier_avg(n) for n in ladder if tier_avg(n) is not None}
    if done:
        best = max(done, key=done.get)
        log(f'各档均值: {done} → 最优 n={best}（{done[best]}MB/s）')


if __name__ == '__main__':
    main()
