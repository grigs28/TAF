#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
测试 .env 文件解析
Test .env file parsing
"""

import sys
from dotenv import dotenv_values

def test_env_parse(env_file_path: str = ".env"):
    """测试 .env 文件解析"""
    try:
        values = dotenv_values(env_file_path)
        print(f"SUCCESS: Parsed {len(values)} variables")
        
        # 检查 NOTIFICATION_EVENTS
        if "NOTIFICATION_EVENTS" in values:
            print("NOTIFICATION_EVENTS found in parsed values")
            import json
            try:
                events = json.loads(values["NOTIFICATION_EVENTS"])
                print(f"NOTIFICATION_EVENTS is valid JSON with {len(events)} keys")
            except Exception as e:
                print(f"NOTIFICATION_EVENTS JSON parse error: {e}")
        
        return True
    except Exception as e:
        print(f"ERROR: {e}")
        return False

if __name__ == "__main__":
    env_file = sys.argv[1] if len(sys.argv) > 1 else ".env"
    success = test_env_parse(env_file)
    sys.exit(0 if success else 1)

