#!/usr/bin/env python3
import os, time, shutil, subprocess, concurrent.futures as fut, math, lz4.frame, zstandard as zstd, pgzip
from tqdm import tqdm

SRC       = r"D:\AI\opt\models\DeepSeek-R1-Distill-Qwen-7B-GGUF\DeepSeek-R1-Distill-Qwen-7B-F16.gguf"
TEMP_DIR  = "temp"
THREADS   = 8
CHUNK     = 64 * 1024 * 1024          # 64 MB
os.makedirs(TEMP_DIR, exist_ok=True)
base      = os.path.basename(SRC)
TOTAL     = os.path.getsize(SRC)
results   = []

def log(name, t, size):
    results.append((name, t, size))
    print(f"{name} 耗时 {t:.2f}s  -> {size>>20} MB\n")

# ---------- 1. TAR 多线程（Python 流式） ----------
def tar_mt():
    out = os.path.join(TEMP_DIR, base + ".tar")
    print("TAR 多线程打包开始 …")
    t0 = time.time()
    block_size = CHUNK
    blocks = math.ceil(TOTAL / block_size)
    def read_block(idx):
        with open(SRC, "rb") as f:
            f.seek(idx * block_size)
            return f.read(block_size)
    with tqdm(total=blocks, desc="TAR-MT") as bar, open(out, "wb") as fout:
        with fut.ThreadPoolExecutor(max_workers=THREADS) as pool:
            for blk in pool.map(read_block, range(blocks)):
                fout.write(blk)
                bar.update(1)
    log("TAR-MT", time.time() - t0, os.path.getsize(out))

# ---------- 2. LZ4 单线程 ----------
def lz4_st():
    out = os.path.join(TEMP_DIR, base + "_st.lz4")
    print("LZ4 单线程压缩开始 …")
    t0 = time.time()
    with open(SRC, "rb") as fin, open(out, "wb") as fout:
        with lz4.frame.open(fout, "wb", compression_level=0) as c:
            shutil.copyfileobj(fin, c, length=CHUNK)
    log("LZ4-ST", time.time() - t0, os.path.getsize(out))

# ---------- 3. LZ4 多线程 ----------
def lz4_mt():
    out = os.path.join(TEMP_DIR, base + "_mt.lz4")
    print("LZ4 多线程压缩开始 …")
    t0 = time.time()
    block_size = CHUNK
    blocks = math.ceil(TOTAL / block_size)
    def job(idx):
        with open(SRC, "rb") as f:
            f.seek(idx * block_size)
            data = f.read(block_size)
        return lz4.frame.compress(data, compression_level=0)
    with tqdm(total=blocks, desc="LZ4-MT") as bar:
        with fut.ThreadPoolExecutor(max_workers=THREADS) as pool:
            chunks = []
            for c in pool.map(job, range(blocks)):
                chunks.append(c)
                bar.update(1)
    with open(out, "wb") as fout:
        for c in chunks:
            fout.write(c)
    log("LZ4-MT", time.time() - t0, os.path.getsize(out))

# ---------- 4. Zstd 单线程 ----------
def zstd_st():
    out = os.path.join(TEMP_DIR, base + "_st.zst")
    print("Zstd 单线程压缩开始 …")
    t0 = time.time()
    with open(SRC, "rb") as fin, open(out, "wb") as fout:
        zstd.ZstdCompressor(level=3, threads=1).copy_stream(fin, fout)
    log("Zstd-ST", time.time() - t0, os.path.getsize(out))

# ---------- 5. Zstd 多线程（Python API） ----------
def zstd_mt():
    out = os.path.join(TEMP_DIR, base + "_mt.zst")
    print("Zstd 多线程压缩开始 …")
    t0 = time.time()
    with open(SRC, "rb") as fin, open(out, "wb") as fout:
        zstd.ZstdCompressor(level=3, threads=THREADS).copy_stream(fin, fout)
    log("Zstd-MT", time.time() - t0, os.path.getsize(out))

# ---------- 6. 外部 zstd 多线程 ----------
def zstd_exe_mt():
    out = os.path.join(TEMP_DIR, base + "_exe_mt.zst")
    print("外部 zstd 多线程压缩开始 …")
    t0 = time.time()
    # -T0 自动核心数，-3 默认级别
    ZSTD_EXE = r"E:\app\TAF\ITDT\zstd\zstd.exe"
    cmd = [ZSTD_EXE, "-T0", "-3", "-o", out, SRC]
    #cmd = ["zstd", "-T0", "-3", "-o", out, SRC]
    subprocess.run(cmd, check=True, stdout=subprocess.DEVNULL, stderr=subprocess.DEVNULL)
    log("zstd-exe-MT", time.time() - t0, os.path.getsize(out))

# ---------- 7. pgzip 多线程 ----------
def pgzip_mt():
    out = os.path.join(TEMP_DIR, base + ".gz")
    print("pgzip 多线程压缩开始 …")
    t0 = time.time()
    with open(SRC, "rb") as fin, pgzip.open(out, "wb", thread=THREADS) as fout:
        shutil.copyfileobj(fin, fout, length=CHUNK)
    log("pgzip-MT", time.time() - t0, os.path.getsize(out))

# ---------- 汇总 ----------
def summary():
    print("="*70)
    print("汇总（源文件 %.2f GB）" % (TOTAL/1024**3))
    print(f"{'方法':<15} {'耗时(s)':<10} {'输出(MB)':<12} {'压缩率':<10}")
    print("-"*55)
    for name, t, s in results:
        print(f"{name:<15} {t:<10.2f} {s>>20:<12} {s/TOTAL:<10.2%}")
    if "LZ4-ST" in [r[0] for r in results] and "LZ4-MT" in [r[0] for r in results]:
        st = next(r[1] for r in results if r[0]=="LZ4-ST")
        mt = next(r[1] for r in results if r[0]=="LZ4-MT")
        print(f"LZ4 多线程加速比: {st/mt:.2f}x")
    print("="*70)

# ---------- 主流程 ----------
if __name__ == "__main__":
    print(f"测试文件: {base}  ({TOTAL>>20} MB)\n")
    tar_mt(); lz4_st(); lz4_mt(); zstd_st(); zstd_mt(); zstd_exe_mt(); pgzip_mt()
    summary()