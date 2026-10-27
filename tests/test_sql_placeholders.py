#!/usr/bin/env python3
"""SQL 占位符一致性回归测试。

背景：`utils/scheduler/psycopg3_compat` 把 asyncpg 风格的 `$N` 一律替换成
`%s`（按**出现顺序**绑参），而 asyncpg 是**按编号**绑定。由此产生两类事故：

1. 编号乱序：`SET x = ... || $2::jsonb WHERE id = $1` → id 被绑到 jsonb 位置，
   报 `cannot cast type smallint to jsonb`（2026-10-26 月度验证任务 167）。
2. 同一编号重复引用：`COALESCE(result_summary,'{}') || COALESCE($6::jsonb,'{}')`
   → 多出一个 %s，报 `the query has 10 placeholders but 9 parameters were passed`，
   异常被吞，任务进度永远是 0（任务 166）。
   注意注释里的 `$N` 同样会被替换，所以 SQL 注释里也不能写占位符编号。

已知例外（有意为之，需手动重排参数来适配）：见 KNOWN_REORDERED。
新增代码请让编号从 $1 起按出现顺序连续编号。
"""
import ast
import os
import re
import sys

ROOT = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
PATTERN = re.compile(r'\$(\d+)')
SKIP_DIRS = {'.git', '__pycache__', 'node_modules', 'bench', 'tests',
             'logs', 'temp', '.sbot'}

# 这些位置的 SQL 编号乱序是有意的：调用前已手动把参数重排为出现顺序。
KNOWN_REORDERED = {
    ('backup/backup_db.py', 'UPDATE backup_files'),
}


def iter_sql_strings():
    """遍历仓库里所有传给 execute/fetch/fetchrow/fetchval/executemany 的 SQL 串"""
    for root, dirs, files in os.walk(ROOT):
        dirs[:] = [d for d in dirs if d not in SKIP_DIRS]
        for name in files:
            if not name.endswith('.py'):
                continue
            path = os.path.join(root, name)
            rel = os.path.relpath(path, ROOT)
            try:
                tree = ast.parse(open(path, encoding='utf-8').read())
            except SyntaxError:
                continue
            for node in ast.walk(tree):
                if not isinstance(node, ast.Call):
                    continue
                fn = getattr(node.func, 'attr', None) or getattr(node.func, 'id', None)
                if fn not in ('execute', 'fetch', 'fetchrow', 'fetchval', 'executemany'):
                    continue
                if not node.args:
                    continue
                first = node.args[0]
                if not (isinstance(first, ast.Constant) and isinstance(first.value, str)):
                    continue
                if '$' not in first.value:
                    continue
                yield rel, node.lineno, first.value


def check_sql(rel, lineno, sql):
    nums = [int(x) for x in PATTERN.findall(sql)]
    if not nums:
        return []
    head = ' '.join(sql.strip().split()[:2])
    if (rel, head) in KNOWN_REORDERED:
        return []
    problems = []
    if len(nums) != len(set(nums)):
        dup = sorted({n for n in nums if nums.count(n) > 1})
        problems.append(
            f'占位符重复引用 {dup}（会被换成多个 %s，参数个数对不上）')
    if nums != sorted(nums):
        problems.append(f'编号乱序 {nums}（按出现顺序绑参会错位）')
    elif nums and set(nums) != set(range(1, max(nums) + 1)):
        problems.append(f'编号不连续 {nums}')
    return [f'{rel}:{lineno}  {p}\n      {sql.strip().splitlines()[0][:80]}'
            for p in problems]


def test_sql_placeholders_consistent():
    problems = []
    for rel, lineno, sql in iter_sql_strings():
        problems.extend(check_sql(rel, lineno, sql))
    assert not problems, 'SQL 占位符问题：\n' + '\n'.join(problems)


def main():
    problems = []
    for rel, lineno, sql in iter_sql_strings():
        problems.extend(check_sql(rel, lineno, sql))
    if problems:
        print('❌ 发现 SQL 占位符问题：')
        for p in problems:
            print('  ' + p)
        return 1
    print('✅ SQL 占位符检查通过（编号连续、无重复引用、无乱序）')
    return 0


if __name__ == '__main__':
    sys.exit(main())
