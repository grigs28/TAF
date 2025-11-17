#!/usr/bin/env python3
"""
批量数据库操作配置更新脚本
用于在 .env 文件中添加批量操作配置项
"""

import os
from pathlib import Path

def update_env_file():
    """更新 .env 文件，添加批量操作配置"""

    env_file = Path('.env')
    env_sample = Path('.env.sample')

    # 如果没有 .env 文件，从 .env.sample 复制
    if not env_file.exists() and env_sample.exists():
        print("从 .env.sample 复制配置到 .env")
        with open(env_sample, 'r', encoding='utf-8') as src:
            content = src.read()
        with open(env_file, 'w', encoding='utf-8') as dst:
            dst.write(content)

    # 读取现有配置
    if env_file.exists():
        with open(env_file, 'r', encoding='utf-8') as f:
            content = f.read()
    else:
        content = ""

    # 检查是否已有批量配置
    batch_configs = [
        'DB_BATCH_SIZE=',
        'DB_QUEUE_MAX_SIZE=',
        'DB_BATCH_TIMEOUT='
    ]

    has_batch_config = any(config in content for config in batch_configs)

    if has_batch_config:
        print("批量数据库操作配置已存在，跳过更新")
        return

    # 添加批量操作配置到文件末尾
    batch_config_section = """

# 批量数据库操作配置
# 批量写入的文件数量，默认1000
DB_BATCH_SIZE=1000

# 数据库队列最大大小，默认5000（防止内存溢出）
DB_QUEUE_MAX_SIZE=5000

# 批量写入超时时间（秒），默认5秒
DB_BATCH_TIMEOUT=5
"""

    with open(env_file, 'a', encoding='utf-8') as f:
        f.write(batch_config_section)

    print("已添加批量数据库操作配置到 .env 文件")

def main():
    """主函数"""
    print("更新批量数据库操作配置...")
    update_env_file()
    print("配置更新完成！")

if __name__ == "__main__":
    main()