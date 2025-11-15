import py7zr, os, time, tempfile
from pathlib import Path

archive = Path(tempfile.gettempdir()) / 'test.7z'
data    = Path(r'temp')

t0 = time.perf_counter()
with py7zr.SevenZipFile(
    archive, 'w',
    filters=[{'id': py7zr.FILTER_LZMA2, 'preset': 5}],
    blocksize=16*1024*1024,      # 16 MiB 块
    block_count=os.cpu_count()   # 块数=核心数
) as z:
    z.writeall(data)
print('elapsed:', time.perf_counter() - t0)