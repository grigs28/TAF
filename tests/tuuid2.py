import subprocess, re

def itdt_tape_uuid(device='Tape0'):
    """
    需先安装 IBM ITDT（Windows 版）
    device : 默认 \\.\Tape0
    """
    cmd = f'itdt.exe -f \\\\.\\{device} -c "READ ATTRIBUTE" --uid'
    out = subprocess.run(cmd, capture_output=True, text=True).stdout
    m = re.search(r'Medium UUID:\s*({[\dA-F-]+})', out, re.I)
    return m.group(1) if m else None

if __name__ == '__main__':
    uid = itdt_tape_uuid()
    print('MAM UUID :', uid)