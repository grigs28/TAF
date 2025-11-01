#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
企业级磁带备份系统安装脚本
Enterprise Tape Backup System Setup Script
"""

from setuptools import setup, find_packages
import os

# 读取README文件
def read_readme():
    with open("README.md", "r", encoding="utf-8") as fh:
        return fh.read()

# 读取requirements文件
def read_requirements():
    with open("requirements.txt", "r", encoding="utf-8") as fh:
        return [line.strip() for line in fh if line.strip() and not line.startswith("#")]

setup(
    name="enterprise-tape-backup",
    version="0.0.1",
    author="Enterprise Tape Backup Team",
    author_email="support@example.com",
    description="企业级磁带备份系统",
    long_description=read_readme(),
    long_description_content_type="text/markdown",
    url="https://github.com/example/enterprise-tape-backup",
    packages=find_packages(),
    classifiers=[
        "Development Status :: 4 - Beta",
        "Intended Audience :: System Administrators",
        "Topic :: System :: Archiving :: Backup",
        "License :: OSI Approved :: MIT License",
        "Programming Language :: Python :: 3",
        "Programming Language :: Python :: 3.8",
        "Programming Language :: Python :: 3.9",
        "Programming Language :: Python :: 3.10",
        "Programming Language :: Python :: 3.11",
        "Operating System :: OS Independent",
    ],
    python_requires=">=3.8",
    install_requires=read_requirements(),
    extras_require={
        "dev": [
            "pytest>=7.4.3",
            "pytest-asyncio>=0.21.1",
            "pytest-cov>=4.1.0",
            "black>=23.11.0",
            "flake8>=6.1.0",
            "mypy>=1.7.1",
        ],
    },
    entry_points={
        "console_scripts": [
            "tape-backup=main:main",
        ],
    },
    include_package_data=True,
    package_data={
        "web": ["templates/*", "static/*"],
    },
)