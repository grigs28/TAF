#!/bin/bash
# Vegas nas_api 状态机回归（逐轮修复的回归套件）
# 用法：bash tests/vegas/run_all.sh
cd "$(dirname "$0")/../.."
rc=0
for f in tests/vegas/test_vegas_round*.py; do
    echo "=== $f ==="
    python3 "$f" || rc=1
done
exit $rc
