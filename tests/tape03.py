import wmi

def get_disk_info():
    """获取物理磁盘信息[citation:2][citation:8]"""
    c = wmi.WMI()
    for disk in c.Win32_DiskDrive():
        print(f"磁盘名称: {disk.Caption}")
        print(f"大小: {int(disk.Size) // (1024**3)} GB")  # 转换为GB
        print(f"制造商: {disk.Manufacturer}")
        print(f"接口类型: {disk.InterfaceType}")
        print("-" * 50)

def get_volume_info():
    """获取逻辑分区信息[citation:2][citation:8]"""
    c = wmi.WMI()
    for partition in c.Win32_LogicalDisk():
        if partition.DriveType == 3:  # 固定磁盘
            print(f"分区: {partition.Caption}")
            print(f"总大小: {int(partition.Size) // (1024**3)} GB")
            print(f"可用空间: {int(partition.FreeSpace) // (1024**3)} GB")
            print(f"使用率: {100 - int(100 * int(partition.FreeSpace) / int(partition.Size))}%")
            print("-" * 30)

if __name__ == "__main__":
    print("物理磁盘信息:")
    get_disk_info()
    print("\n逻辑分区信息:")
    get_volume_info()