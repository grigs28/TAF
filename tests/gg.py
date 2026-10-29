#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
实时目录压缩 + 动态内存自适应块大小 (固定 8 线程)
pip install pgzip psutil
"""
import os, time, psutil, argparse
from pathlib import Path
import pgzip

# ---------- 工具 ----------
def fmt_size(n: int) -> str:
    for u in ["B", "KB", "MB", "GB"]:
        if n < 1024:
            return f"{n:.1f}{u}"
        n /= 1024
    return f"{n:.1f}TB"

def calc_block_size() -> int:
    """根据当前剩余内存返回块大小 [128 KB, 64 MB]"""
    free = psutil.virtual_memory().available
    # 线性映射：0 GB -> 128 KB　　8 GB -> 64 MB
    min_mem, max_mem = 0, 8 * 1024**3
    min_blk, max_blk = 128 * 1024, 64 * 1024 * 1024
    ratio = max(0, min(1, (free - min_mem) / (max_mem - min_mem)))
    return int(1*1024*1024*1024) 

# ---------- 核心 ----------
class MemAdaptiveCompressor:
    def __init__(self, src: Path, dst: Path, threads: int = 18):
        self.src = Path(src)
        self.dst = Path(dst)
        self.threads = threads
        self.proc = 0
        self.skip = 0
        self.total_in = 0
        self.start = time.time()

    def run(self):
        self.dst.parent.mkdir(parents=True, exist_ok=True)
        block_size = calc_block_size()
        print(f"[START] 块大小={fmt_size(block_size)}  线程=24")
        with pgzip.open(self.dst, 'wb', thread=self.threads,
                        blocksize=block_size) as gz:
            for root, _, files in os.walk(self.src):
                for name in files:
                    file_path = Path(root) / name
                    try:
                        self._compress_file(file_path, gz, block_size)
                    except Exception as e:
                        print(f"[SKIP] {file_path}  {e}")
                        self.skip += 1
                    if (self.proc + self.skip) % 100 == 0:
                        self._progress()
        self._final()

    def _compress_file(self, file_path: Path, gz, block_size: int):
        size = file_path.stat().st_size
        self.total_in += size
        # 内存足够则一次性读，否则流式
        if size < psutil.virtual_memory().available * 0.8:
            gz.write(file_path.read_bytes())
        else:
            with open(file_path, 'rb') as f:
                while chunk := f.read(block_size):
                    gz.write(chunk)
        self.proc += 1

    def _progress(self):
        elapsed = time.time() - self.start
        print(f"[PROG] 已处理 {self.proc:>5}  跳过 {self.skip:>3}  "
              f"用时 {elapsed:>6.1f}s  速率 {fmt_size(self.total_in/elapsed):>8}/s")

    def _final(self):
        elapsed = time.time() - self.start
        out_sz = self.dst.stat().st_size
        ratio = (1 - out_sz / self.total_in) * 100
        print("=" * 60)
        print(f"[DONE] 输入 {fmt_size(self.total_in)}  输出 {fmt_size(out_sz)}  "
              f"压缩率 {ratio:>4.1f}%")
        print(f"[DONE] 总用时 {elapsed:>6.1f}s  文件 {self.proc}  跳过 {self.skip}")
        print("=" * 60)

# ------------------- CLI -------------------
def main():
    parser = argparse.ArgumentParser(description="目录实时压缩（内存自适应块，8线程）")
    parser.add_argument("src", help="源目录")
    parser.add_argument("dst", help="输出.gz文件")
    args = parser.parse_args()

    comp = MemAdaptiveCompressor(Path(args.src), Path(args.dst), threads=8)
    comp.run()

if __name__ == "__main__":
    main()