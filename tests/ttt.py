import wmi

def get_tape_drives():
    c = wmi.WMI()
    tape_drives = c.Win32_TapeDrive()
    
    if tape_drives:
        for tape in tape_drives:
            print(f"设备名: {tape.Name}")
            print(f"描述: {tape.Description}")
            print(f"设备ID: {tape.DeviceID}")
            print("---")
    else:
        print("未找到磁带设备")

get_tape_drives()