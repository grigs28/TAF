#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
pgzip_bench.py  ——  pgzip 多线程压缩性能基准
用法:
    python pgzip_bench.py  [--num_files N] [--file_size_gb S] [--threads T]
"""
import os
import time
import subprocess
import shutil
import sys
import tempfile
import argparse
from datetime import datetime
from pathlib import Path

SCRIPT_DIR = Path(__file__).resolve().parent
DEFAULT_THREADS = os.cpu_count() or 8

# -------------------- 工具函数 --------------------
def human_size(size_bytes: int) -> str:
    """Bytes -> human readable"""
    for unit in ["B", "KB", "MB", "GB"]:
        if size_bytes < 1024.0:
            return f"{size_bytes:.2f}{unit}"
        size_bytes /= 1024.0
    return f"{size_bytes:.2f}TB"

def parse_block_size(blk: str) -> int | None:
    """1M -> 1048576；None/ default -> None"""
    if blk is None or blk.lower() == "default":
        return None
    blk = blk.upper()
    if blk.endswith("K"):
        return int(blk[:-1]) * 1024
    if blk.endswith("M"):
        return int(blk[:-1]) * 1024 ** 2
    if blk.endswith("G"):
        return int(blk[:-1]) * 1024 ** 3
    return int(blk)  # 纯数字视为字节

# -------------------- 核心测试器 --------------------
class PGZipTester:
    def __init__(self, num_files: int = 1, file_size_gb: int = 6, threads: int = DEFAULT_THREADS):
        self.num_files = num_files
        self.file_size_gb = file_size_gb
        self.threads = threads
        self.work_dir = SCRIPT_DIR / "bench_temp"
        self.results: list[dict] = []

    # ---------- 生成可压缩大文件 ----------
    def create_files(self):
        self.work_dir.mkdir(exist_ok=True)
        tgt_size = self.file_size_gb * 1024 ** 3
        chunk = "A" * 1000 + "\n"  # 单条 1001 B
        for idx in range(1, self.num_files + 1):
            fpath = self.work_dir / f"big_{idx}.log"
            print(f"[+] 生成 {fpath}  ({self.file_size_gb} GB)  ...")
            written = 0
            with open(fpath, "w", buffering=1024 * 1024) as fh:
                while written < tgt_size:
                    fh.write(chunk)
                    written += len(chunk)
            print(f"    实际大小 {human_size(fpath.stat().st_size)}")

    # ---------- 单次压缩 ----------
    def run_once(self, level: int, blk_str: str) -> bool:
        blk_bytes = parse_block_size(blk_str)
        blk_desc = blk_str if blk_str else "default"
        print(f"\n>>> 测试 level={level}  block={blk_desc}  threads={self.threads}")

        total_raw = total_cmp = total_sec = 0
        ok_cnt = 0

        for idx in range(1, self.num_files + 1):
            src = self.work_dir / f"big_{idx}.log"
            dst = self.work_dir / f"big_{idx}.l{level}.b{blk_desc}.gz"

            cmd = [sys.executable, "-m", "pgzip", "-l", str(level), "-t", str(self.threads), "-o", str(dst), str(src)]
            if blk_bytes:
                cmd += ["-b", str(blk_bytes)]

            print(f"    compressing  {src.name}  ...", end="")
            t0 = time.time()
            try:
                subprocess.run(cmd, check=True, capture_output=True)
            except subprocess.CalledProcessError as e:
                print(f" fail: {e.stderr.decode()[:80]}")
                continue
            t1 = time.time()

            raw_gb = src.stat().st_size / 1024 ** 3
            cmp_gb = dst.stat().st_size / 1024 ** 3
            span = t1 - t0
            total_raw += raw_gb
            total_cmp += cmp_gb
            total_sec += span
            ok_cnt += 1
            print(f"  {span:.1f}s  {cmp_gb/raw_gb*100:.1f}%")

        if ok_cnt == 0:
            return False

        # 记录结果
        self.results.append(
            dict(
                level=level,
                block=blk_desc,
                threads=self.threads,
                raw_gb=total_raw,
                cmp_gb=total_cmp,
                ratio=total_cmp / total_raw * 100,
                seconds=total_sec,
                speed_gb_s=total_raw / total_sec,
            )
        )
        return True

    # ---------- 汇总 ----------
    def report(self):
        if not self.results:
            print("无有效结果")
            return
        baseline = min(self.results, key=lambda x: x["seconds"])
        print("\n" + "=" * 95)
        print("pgzip 基准小结  (" + datetime.now().strftime("%Y-%m-%d %H:%M:%S") + ")")
        print("=" * 95)
        print(f"文件数×大小: {self.num_files} × {self.file_size_gb} GB   线程: {self.threads}")
        print("-" * 95)
        print(f"{'配置':<12} {'压缩比':<10} {'时间(s)':<10} {'速度(GB/s)':<12} {'vs最快':<10}")
        print("-" * 95)
        for r in self.results:
            vs = r["seconds"] / baseline["seconds"]
            print(f"L{r['level']}_B{r['block']:<7} {r['ratio']:>6.1f}%   {r['seconds']:>7.1f}  "
                  f"{r['speed_gb_s']:>9.2f}     {vs:>5.2f}x")
        print("=" * 95)
        # 保存到文件
        txt = SCRIPT_DIR / f"pgzip_benchmark_{datetime.now():%Y%m%d_%H%M%S}.txt"
        txt.write_text("\n".join(str(r) for r in self.results) + "\n" + "=" * 95 + "\n")
        print(f"详细结果已写至  {txt}")

    # ---------- 清理 ----------
    def clean(self):
        if self.work_dir.exists():
            shutil.rmtree(self.work_dir)
            print("[*] 临时目录已清理")


# -------------------- CLI --------------------
def main():
    ap = argparse.ArgumentParser(description="pgzip 多线程压缩基准")
    ap.add_argument("--num_files", type=int, default=1, help="生成几个大文件 (默认1)")
    ap.add_argument("--file_size_gb", type=int, default=6, help="单个文件大小 GB (默认6)")
    ap.add_argument("--threads", type=int, default=DEFAULT_THREADS, help=f"线程数 (默认{DEFAULT_THREADS})")
    args = ap.parse_args()

    tester = PGZipTester(args.num_files, args.file_size_gb, args.threads)
    try:
        # 检查 pgzip 可用
        subprocess.run([sys.executable, "-m", "pgzip", "--help"], check=True, capture_output=True)
        # 生成文件
        tester.create_files()
        # 测试矩阵
        for lvl in (1, 6):
            for blk in ("default", "1M", "4M"):
                tester.run_once(lvl, blk)
        # 汇总
        tester.report()
    except subprocess.CalledProcessError:
        print("pgzip 模块未安装，请先  pip install pgzip")
    except KeyboardInterrupt:
        print("用户中断")
    finally:
        tester.clean()


if __name__ == "__main__":
    main()