#!/usr/bin/env python3
"""测试 JSON 格式 Syslog 转发"""

import socket
import json
import datetime

TARGET_HOST = "192.168.93.14"
TARGET_PORT = 515


def send_log(message, hostname="taf", app="taf", level="info"):
    """发送 JSON 格式日志到 Vector syslog 端口"""
    data = json.dumps({
        "timestamp": datetime.datetime.now().isoformat() + "Z",
        "host": hostname,
        "level": level,
        "message": message,
    })
    sock = socket.socket(socket.AF_INET, socket.SOCK_DGRAM)
    sock.sendto(data.encode(), (TARGET_HOST, TARGET_PORT))
    sock.close()
    print(f"[已发送] -> {TARGET_HOST}:{TARGET_PORT}")
    print(f"  内容: {data}")


if __name__ == "__main__":
    print(f"=== 测试 JSON Syslog 转发 ({TARGET_HOST}:{TARGET_PORT}) ===\n")

    send_log("TAF Syslog JSON 格式连通性测试", level="info")
    send_log("测试 WARNING 级别消息", level="warning")
    send_log("测试 ERROR 级别消息", level="error")

    print(f"\n3条测试消息已发送到 {TARGET_HOST}:{TARGET_PORT}，请检查 VictoriaLogs 是否收到")
