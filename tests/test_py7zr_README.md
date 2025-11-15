# py7zr 压缩库测试结果

## 测试概述

本测试文件 (`test_py7zr_compression.py`) 用于测试 `py7zr.SevenZipFile` 的各种参数和多线程/多进程实现方法。

## 测试结果总结

根据测试运行结果，以下是关键发现：

### 1. py7zr 参数支持情况

- ✅ **`mp` 参数支持**：`py7zr.SevenZipFile` 确实支持 `mp` 参数（多进程）
  - 参数类型：`bool`
  - 默认值：`False`
  - 用法：`mp=True` 启用多进程压缩

### 2. 压缩性能对比

| 测试方法 | 耗时(秒) | 压缩率(%) | 说明 |
|---------|---------|----------|------|
| 基本压缩 | 1.14 | 89.97% | 标准压缩，无多进程 |
| mp=False | 0.92 | 89.97% | 禁用多进程 |
| mp=True | 0.83 | 89.97% | **启用多进程（最快）** |
| 多线程压缩 | 1.13 | -0.03% | 多个文件并行压缩（生成多个压缩包） |
| 环境变量 | 0.88 | 89.97% | 设置 7Z_THREADS=4 |

### 3. 测试发现

1. **`mp=True` 确实能提高压缩速度**（0.83秒 vs 0.92秒，提升约10%）
2. **多线程方法**适合并行压缩多个文件（每个文件单独压缩）
3. **环境变量 `7Z_THREADS`** 可能对底层7z库有影响，但py7zr可能不直接使用

## py7zr.SevenZipFile 参数说明

### 基本参数

```python
py7zr.SevenZipFile(
    archive_path,          # 压缩包路径
    mode='w',              # 模式: 'w'=写入, 'r'=读取, 'a'=追加
    filters=[              # 压缩过滤器
        {
            'id': py7zr.FILTER_LZMA2,  # 使用LZMA2算法
            'preset': 5                 # 压缩级别 (0-9, 默认5)
        }
    ],
    mp=True                # 启用多进程压缩（bool类型）
)
```

### 参数详解

1. **`mode`**: 
   - `'w'`: 创建新压缩包（覆盖已存在的）
   - `'r'`: 读取压缩包
   - `'a'`: 追加文件到压缩包

2. **`filters`**: 压缩算法和参数
   - `'id'`: 压缩算法ID
     - `py7zr.FILTER_LZMA2`: LZMA2算法（推荐）
     - `py7zr.FILTER_LZMA`: LZMA算法
     - `py7zr.FILTER_DEFLATE`: DEFLATE算法
   - `'preset'`: 压缩级别 (0-9)
     - 0: 不压缩（仅打包）
     - 1-3: 快速压缩
     - 4-6: 平衡（默认5）
     - 7-9: 最高压缩（速度慢但压缩率高）

3. **`mp`**: 多进程参数
   - `True`: 启用多进程压缩（利用多核CPU）
   - `False`: 禁用多进程（单进程）
   - **注意**：py7zr 的 `mp` 参数是布尔值，不能指定具体线程数

## 多线程/多进程实现方法

### 方法1: 使用 mp 参数（推荐）

```python
with py7zr.SevenZipFile(
    archive_path,
    mode='w',
    filters=[{'id': py7zr.FILTER_LZMA2, 'preset': 5}],
    mp=True  # 启用多进程压缩
) as archive:
    for file_path in files:
        archive.write(file_path, file_path.name)
```

**优点**：
- 简单易用
- 单个压缩包内可以多进程压缩
- 适合压缩单个大型压缩包

**缺点**：
- 不能指定具体线程数
- 由底层库自动决定进程数

### 方法2: 使用 concurrent.futures（多文件并行）

```python
from concurrent.futures import ThreadPoolExecutor

def compress_file(file_path, archive_name):
    with py7zr.SevenZipFile(archive_name, 'w') as archive:
        archive.write(file_path, file_path.name)

files = ['file1.txt', 'file2.txt', 'file3.txt']
archives = ['file1.7z', 'file2.7z', 'file3.7z']

with ThreadPoolExecutor(max_workers=4) as executor:
    executor.map(compress_file, files, archives)
```

**优点**：
- 可以并行压缩多个文件（生成多个压缩包）
- 可以指定线程数
- 适合批量压缩多个独立文件

**缺点**：
- 每个文件单独压缩（不能合并到一个压缩包）
- 生成的压缩包数量等于文件数量

### 方法3: 使用 multiprocessing（CPU密集型）

```python
from multiprocessing import Pool

def compress_file(args):
    file_path, archive_name = args
    with py7zr.SevenZipFile(archive_name, 'w') as archive:
        archive.write(file_path, file_path.name)

files = ['file1.txt', 'file2.txt', 'file3.txt']
archives = ['file1.7z', 'file2.7z', 'file3.7z']

with Pool(processes=4) as pool:
    pool.map(compress_file, zip(files, archives))
```

**优点**：
- 利用多进程（适合CPU密集型任务）
- 不受GIL限制

**缺点**：
- 进程间通信开销较大
- 不适合I/O密集型任务

### 方法4: 环境变量（实验性）

```python
import os

# 设置线程数环境变量
os.environ['7Z_THREADS'] = '4'

with py7zr.SevenZipFile(archive_path, 'w', mp=True) as archive:
    for file_path in files:
        archive.write(file_path, file_path.name)

# 恢复环境变量
if '7Z_THREADS' in os.environ:
    del os.environ['7Z_THREADS']
```

**注意**：此方法的效果取决于py7zr底层库是否支持该环境变量。

## 代码中的实现

在 `backup/compressor.py` 中的实现：

```python
# 从配置获取线程数
compression_threads = self.settings.COMPRESSION_THREADS

# 使用 py7zr 压缩，启用多进程
with py7zr.SevenZipFile(
    archive_path,
    mode='w',
    filters=[{'id': py7zr.FILTER_LZMA2, 'preset': compression_level}],
    mp=True if compression_threads > 1 else False  # 启用多进程压缩
) as archive:
    for file_info in file_group:
        archive.write(file_info['path'], file_info['name'])
```

## 运行测试

```bash
# 直接运行测试
python tests/test_py7zr_compression.py

# 或使用 pytest（如果安装了）
pytest tests/test_py7zr_compression.py -v
```

## 测试文件说明

测试文件会自动：
1. 创建临时测试目录
2. 生成测试文件（10个文件，每个约0.5MB）
3. 执行多种压缩方法测试
4. 对比性能指标
5. 自动清理测试文件

测试完成后会输出：
- 每种方法的耗时
- 压缩率
- CPU使用情况（如果可用）
- 性能对比总结

## 建议

1. **对于单个压缩包**：使用 `mp=True` 参数（最简单有效）
2. **对于多个文件**：使用 `concurrent.futures.ThreadPoolExecutor` 并行压缩
3. **压缩级别**：根据需求选择（5是平衡点）
4. **线程数配置**：当前 `COMPRESSION_THREADS` 配置主要用于控制是否启用 `mp=True`，而不是具体线程数

## 参考资料

- [py7zr 官方文档](https://py7zr.readthedocs.io/)
- [py7zr GitHub](https://github.com/miurahr/py7zr)
- [7-Zip 命令行参数](https://sevenzip.osdn.jp/chm/cmdline/switches/mmt.htm)

