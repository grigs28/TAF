#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
SMB 连接测试程序
测试 SMB/CIFS 网络共享连接

测试路径: \\192.168.0.79\bak
用户名: winroot
密码: jfadmin3285411
域: nt08 (可选)
"""

import os
import subprocess
import tempfile
import shutil
from pathlib import Path


# 测试配置
SMB_CONFIG = {
    "server": "192.168.0.79",
    "share": "bak",
    "username": "winroot",
    "password": "jfadmin3285411",
    "domain": "nt08",  # 可选，如果需要域认证
}


def test_smb_connection():
    """测试 SMB 连接"""
    print("=" * 60)
    print("SMB 连接测试")
    print("=" * 60)
    print(f"服务器: {SMB_CONFIG['server']}")
    print(f"共享名: {SMB_CONFIG['share']}")
    print(f"用户名: {SMB_CONFIG['username']}")
    print(f"域: {SMB_CONFIG['domain'] or '(无)'}")
    print("=" * 60)

    # 创建临时挂载点（使用 sudo）
    mount_point = Path("/mnt/smb_test")
    subprocess.run(["sudo", "mkdir", "-p", str(mount_point)], check=True)
    subprocess.run(["sudo", "chown", f"{os.environ.get('USER', 'root')}:{os.environ.get('USER', 'root')}", str(mount_point)], check=True)

    smb_path = f"//{SMB_CONFIG['server']}/{SMB_CONFIG['share']}"

    # 构建挂载选项
    mount_options = [
        f"username={SMB_CONFIG['username']}",
        f"password={SMB_CONFIG['password']}",
        "vers=3.0",  # SMB 协议版本
    ]

    # 如果有域，添加域选项
    if SMB_CONFIG.get("domain"):
        mount_options.append(f"domain={SMB_CONFIG['domain']}")

    mount_options_str = ",".join(mount_options)

    print(f"\nSMB 路径: {smb_path}")
    print(f"挂载点: {mount_point}")
    print(f"挂载选项: username=***,password=***,vers=3.0{',domain=' + SMB_CONFIG['domain'] if SMB_CONFIG.get('domain') else ''}")
    print()

    try:
        # 1. 先卸载（如果已挂载）
        print("1. 检查是否已挂载...")
        result = subprocess.run(
            ["mountpoint", "-q", str(mount_point)],
            capture_output=True
        )
        if result.returncode == 0:
            print("   已挂载，先卸载...")
            subprocess.run(["sudo", "umount", str(mount_point)], capture_output=True)

        # 2. 挂载 SMB 共享
        print("2. 挂载 SMB 共享...")
        mount_cmd = [
            "sudo", "mount", "-t", "cifs",
            smb_path,
            str(mount_point),
            "-o", mount_options_str
        ]

        print(f"   执行命令: sudo mount -t cifs {smb_path} {mount_point} -o ***")
        result = subprocess.run(mount_cmd, capture_output=True, text=True, timeout=30)

        if result.returncode != 0:
            print(f"   挂载失败!")
            print(f"   错误输出: {result.stderr}")

            # 尝试不同的协议版本
            print("\n   尝试 SMB 2.0 协议...")
            mount_options_v2 = mount_options_str.replace("vers=3.0", "vers=2.0")
            result = subprocess.run(
                ["sudo", "mount", "-t", "cifs", smb_path, str(mount_point), "-o", mount_options_v2],
                capture_output=True, text=True, timeout=30
            )

            if result.returncode != 0:
                print(f"   SMB 2.0 也失败: {result.stderr}")

                # 尝试 SMB 1.0
                print("\n   尝试 SMB 1.0 协议...")
                mount_options_v1 = mount_options_str.replace("vers=3.0", "vers=1.0")
                result = subprocess.run(
                    ["sudo", "mount", "-t", "cifs", smb_path, str(mount_point), "-o", mount_options_v1],
                    capture_output=True, text=True, timeout=30
                )

                if result.returncode != 0:
                    print(f"   SMB 1.0 也失败: {result.stderr}")
                    print("\n   连接测试失败!")
                    return False

        print("   挂载成功!")

        # 3. 列出目录内容
        print("\n3. 列出共享目录内容...")
        print("-" * 40)
        result = subprocess.run(
            ["ls", "-la", str(mount_point)],
            capture_output=True, text=True
        )
        if result.returncode == 0:
            print(result.stdout)
        else:
            print(f"   列出目录失败: {result.stderr}")

        # 4. 测试读取权限
        print("4. 测试读取权限...")
        items = list(mount_point.iterdir())
        print(f"   共有 {len(items)} 个项目")

        if items:
            # 测试读取第一个文件/目录
            first_item = items[0]
            print(f"   测试项目: {first_item.name}")
            if first_item.is_file():
                size = first_item.stat().st_size
                print(f"   文件大小: {size} 字节")
            elif first_item.is_dir():
                sub_items = list(first_item.iterdir())
                print(f"   目录包含 {len(sub_items)} 个项目")

        print("\n" + "=" * 60)
        print("连接测试成功!")
        print("=" * 60)
        return True

    except subprocess.TimeoutExpired:
        print("   挂载超时!")
        return False
    except Exception as e:
        print(f"   测试异常: {e}")
        return False
    finally:
        # 清理：卸载
        print("\n清理: 卸载 SMB 共享...")
        subprocess.run(["sudo", "umount", str(mount_point)], capture_output=True)
        # 尝试删除挂载点目录
        try:
            subprocess.run(["sudo", "rmdir", str(mount_point)], capture_output=True)
            print("   清理完成")
        except:
            pass


def test_smb_with_smbclient():
    """使用 smbclient 测试 SMB 连接（不需要挂载）"""
    print("\n" + "=" * 60)
    print("使用 smbclient 测试连接")
    print("=" * 60)

    # 检查 smbclient 是否安装
    result = subprocess.run(["which", "smbclient"], capture_output=True)
    if result.returncode != 0:
        print("smbclient 未安装，跳过此测试")
        print("安装命令: sudo dnf install samba-client")
        return False

    server = SMB_CONFIG["server"]
    share = SMB_CONFIG["share"]
    username = SMB_CONFIG["username"]
    password = SMB_CONFIG["password"]
    domain = SMB_CONFIG.get("domain", "")

    # 构建 smbclient 命令
    smb_path = f"//{server}/{share}"

    cmd = [
        "smbclient",
        smb_path,
        "-U", f"{domain}\\{username}" if domain else username,
        "--password", password,
        "-c", "ls"
    ]

    print(f"执行: smbclient {smb_path} -U {domain}\\{username if domain else username} -c ls")
    print()

    result = subprocess.run(cmd, capture_output=True, text=True, timeout=30)

    if result.returncode == 0:
        print("连接成功!")
        print("目录内容:")
        print(result.stdout)
        return True
    else:
        print(f"连接失败: {result.stderr}")
        return False


def test_ping():
    """测试网络连通性"""
    print("\n" + "=" * 60)
    print("网络连通性测试")
    print("=" * 60)

    server = SMB_CONFIG["server"]
    print(f"Ping {server}...")

    result = subprocess.run(
        ["ping", "-c", "3", server],
        capture_output=True, text=True
    )

    if result.returncode == 0:
        print("网络连通正常")
        print(result.stdout)
        return True
    else:
        print(f"网络不通: {result.stderr}")
        return False


def test_smb_port():
    """测试 SMB 端口是否开放"""
    print("\n" + "=" * 60)
    print("SMB 端口测试")
    print("=" * 60)

    server = SMB_CONFIG["server"]

    # 检查 nc 或 nmap 是否可用
    nc_result = subprocess.run(["which", "nc"], capture_output=True)
    if nc_result.returncode == 0:
        # 使用 nc 测试端口
        ports = [445, 139]  # SMB 端口
        for port in ports:
            print(f"测试端口 {server}:{port}...")
            result = subprocess.run(
                ["nc", "-zv", "-w", "3", server, str(port)],
                capture_output=True, text=True, timeout=5
            )
            if result.returncode == 0:
                print(f"   端口 {port} 开放")
            else:
                print(f"   端口 {port} 关闭或超时")
    else:
        print("nc 未安装，跳过端口测试")
        print("安装命令: sudo dnf install nmap-ncat")


def main():
    """主函数"""
    print("\n" + "#" * 60)
    print("# SMB 连接测试程序")
    print("#" * 60)

    # 1. 测试网络连通性
    test_ping()

    # 2. 测试 SMB 端口
    test_smb_port()

    # 3. 使用 smbclient 测试
    test_smb_with_smbclient()

    # 4. 使用 mount.cifs 测试
    test_smb_connection()

    print("\n" + "#" * 60)
    print("# 测试完成")
    print("#" * 60)


if __name__ == "__main__":
    main()
