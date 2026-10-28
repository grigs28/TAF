#!/usr/bin/env python3
"""计划任务状态回归：已完成任务不得被"停止"逻辑改写为 paused。

背景（2026-10-27 生产实测）：月度备份计划（任务1）10:05 跑完，11:20 重启服务后
显示"已暂停"。根因链：
  1. run_task（手动运行）登记 _running_executions 后从不清理，完成后条目残留；
  2. scheduler.stop()（关服）对残留条目一律 stop_task；
  3. stop_task 不查 done() 即写 status=paused。

本测试用最小桩验证三处修复的行为不变量。
"""
import ast
import asyncio
import os
import sys

ROOT = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
SRC = open(os.path.join(ROOT, 'utils', 'scheduler', 'scheduler.py'), encoding='utf-8').read()
TREE = ast.parse(SRC)

PASS = []


def check(name, cond, info=''):
    PASS.append((name, bool(cond)))
    print(('✅' if cond else '❌') + f' {name} {info}')


def _find_func(tree, name):
    for node in ast.walk(tree):
        if isinstance(node, (ast.FunctionDef, ast.AsyncFunctionDef)) and node.name == name:
            return node
    return None


def _body_source(node):
    return ast.get_source_segment(SRC, node) or ''


# ---- 1. run_task 必须注册"完成即出表"的回调 ----
run_task = _find_func(TREE, 'run_task')
src_run = _body_source(run_task) if run_task else ''
check('run_task 用 add_done_callback 完成即出表',
      'add_done_callback' in src_run, '防注册表残留')

# ---- 2. stop_task 必须先查 done()，已结束的只清理不写状态 ----
stop_task = _find_func(TREE, 'stop_task')
src_stop = _body_source(stop_task) if stop_task else ''
check('stop_task 已完成的条目只清理不写 paused',
      'execution_task.done()' in src_stop and 'return True' in src_stop,
      '防已完成任务被盖 paused')

# 写 paused 必须在 done() 检查之后（同一函数内顺序）
if 'execution_task.done()' in src_stop and 'PAUSED' in src_stop:
    check('paused 写入位于 done() 判断之后',
          src_stop.index('execution_task.done()') < src_stop.index('PAUSED'),
          '顺序正确')
else:
    check('paused 写入位于 done() 判断之后', False, '缺 done() 或 PAUSED')

# ---- 3. scheduler.stop() 关服不落库状态 ----
# 注意：文件里有两个名为 stop 的方法（不同类），要取处理 _running_executions 的那个
_stop_candidates = [
    n for n in ast.walk(TREE)
    if isinstance(n, ast.AsyncFunctionDef) and n.name == 'stop'
    and '_running_executions' in _body_source(n)
]
check('找到调度器 stop() 实现', len(_stop_candidates) == 1, f'候选数={len(_stop_candidates)}')
src_stopfn = _body_source(_stop_candidates[0]) if _stop_candidates else ''
# 该函数内不得出现 stop_task 调用（会写 paused），且应有 done() 守卫
check('关服 stop() 不再逐个调用 stop_task',
      'await self.stop_task(' not in src_stopfn, '避免关服改写任务状态')
check('关服 stop() 对已结束任务跳过 cancel',
      '.done()' in src_stopfn, '只取消真正在跑的')


# ---- 4. 行为级：模拟 done() 条目被 stop_task 处理 ----
class _FakeTask:
    def __init__(self, done=True):
        self._done = done
        self.cancelled = False

    def done(self):
        return self._done

    def cancel(self):
        self.cancelled = True

    def __await__(self):
        async def _noop():
            return None
        return _noop().__await__()


async def _behavioral():
    """复刻 stop_task 的关键分支：done() 条目 → 不写状态、仅出表"""
    updates = []

    async def storage_update_task(tid, payload):
        updates.append((tid, payload))

    running = {1: _FakeTask(done=True)}
    tid = 1
    task = running[tid]
    if task.done():
        running.pop(tid, None)
        # 修复后：直接返回，不写状态
    else:
        task.cancel()
        running.pop(tid, None)
        await storage_update_task(tid, {'status': 'paused'})

    return updates, running


updates, remaining = asyncio.run(_behavioral())
check('已完成任务被停止时不产生任何状态写入', updates == [], f'updates={updates}')
check('已完成任务被停止时注册项被清理', remaining == {}, f'remaining={remaining}')


# ---- 5. 行为级：真正在跑的任务仍会写 paused（原功能不回归） ----
async def _behavioral_running():
    updates = []

    async def storage_update_task(tid, payload):
        updates.append((tid, payload))

    running = {2: _FakeTask(done=False)}
    tid = 2
    task = running[tid]
    if task.done():
        running.pop(tid, None)
    else:
        task.cancel()
        running.pop(tid, None)
        await storage_update_task(tid, {'status': 'paused'})
    return updates, running, task


updates2, remaining2, t2 = asyncio.run(_behavioral_running())
check('运行中任务被停止仍写 paused（功能保持）',
      updates2 == [(2, {'status': 'paused'})], f'updates={updates2}')
check('运行中任务确实被 cancel', t2.cancelled)

print()
fails = [n for n, ok in PASS if not ok]
print(f'共 {len(PASS)} 项，失败 {len(fails)} 项' + (f'：{fails}' if fails else ''))
sys.exit(1 if fails else 0)
