import subprocess

def get_tape_devices_cmd():
    try:
        # 使用ftutil命令
        result = subprocess.run(['ftutil', 'list'], 
                              capture_output=True, 
                              text=True, 
                              check=True)
        print("磁带设备列表:")
        print(result.stdout)
    except subprocess.CalledProcessError:
        print("ftutil命令执行失败或未找到磁带设备")
    except FileNotFoundError:
        print("ftutil工具不可用")

get_tape_devices_cmd()