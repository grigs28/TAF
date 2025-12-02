#!/usr/bin/env python3
"""
内存数据库配置更新脚本
用于在 .env 文件中添加内存数据库配置项
"""

import os
from pathlib import Path

def update_env_file():
    """更新 .env 文件，添加内存数据库配置"""

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

    # 检查是否已有内存数据库配置
    memory_db_configs = [
        'USE_MEMORY_DB=',
        'MEMORY_DB_SYNC_BATCH_SIZE=',
        'MEMORY_DB_SYNC_INTERVAL=',
        'MEMORY_DB_MAX_FILES='
    ]

    has_memory_db_config = any(config in content for config in memory_db_configs)

    if has_memory_db_config:
        print("内存数据库配置已存在，跳过更新")
        return

    # 添加内存数据库配置到文件末尾
    memory_db_config_section = """

# 内存数据库配置 (极速写入 + 异步同步到openGauss)
# 是否使用内存数据库，默认启用（性能最优）
USE_MEMORY_DB=True

# 同步到openGauss的批次大小，默认3000个文件
MEMORY_DB_SYNC_BATCH_SIZE=3000

# 同步间隔（秒），默认30秒同步一次
MEMORY_DB_SYNC_INTERVAL=30

# 内存中最大文件数，超过此值会强制同步，默认10万个文件
MEMORY_DB_MAX_FILES=100000

# 检查点间隔（秒），创建持久化检查点，默认5分钟
MEMORY_DB_CHECKPOINT_INTERVAL=300
"""

    with open(env_file, 'a', encoding='utf-8') as f:
        f.write(memory_db_config_section)

    print("已添加内存数据库配置到 .env 文件")

def main():
    """主函数"""
    print("更新内存数据库配置...")
    update_env_file()
    print("配置更新完成！")
    print("\n使用说明:")
    print("1. 设置 USE_MEMORY_DB=True 启用内存数据库（推荐，性能最优）")
    print("2. 设置 USE_MEMORY_DB=False 使用批量写入器（兼容模式）")
    print("3. 调整同步参数根据系统性能优化")

if __name__ == "__main__":
    main()