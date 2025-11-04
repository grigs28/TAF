import uuid, base64, json

u = uuid.uuid4()          # 以随机 UUID 为例
print('原始对象:', u)

# 1. 标准带连字符 36 字符
print('str        :', str(u))

# 2. 去掉连字符 32 字符
print('hex        :', u.hex)

# 3. 全大写无连字符
print('HEX        :', u.hex.upper())

# 4. 全大写有连字符
print('UPPER      :', str(u).upper())

# 5. 花括号格式（常用于 Windows 注册表）
print('braces     :', '{%s}' % u)

# 6. URN 标准格式
print('urn        :', u.urn)

# 7. bytes 16 字节（二进制）
print('bytes      :', u.bytes)

# 8. base64 短串（22 字符，URL 安全）
print('base64     :', base64.urlsafe_b64encode(u.bytes).decode().rstrip('='))

# 9. int / 128 bit 大整数
print('int        :', u.int)

# 10. 比特串 128 位
print('bits       :', format(u.int, '0128b'))

# 11. JSON 序列化
print('json       :', json.dumps({'id': str(u)}))

# 12. 自定义模板
print('template   :', f'ID-{u.hex[:8]}')