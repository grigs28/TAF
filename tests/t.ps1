# 综合磁带信息查询
function Get-TapeDetailedInfo {
    Write-Host "正在查询磁带驱动器信息..." -ForegroundColor Cyan
    
    # 1. 使用WMI获取基本信息
    $tapeDrives = Get-WmiObject -Class Win32_TapeDrive
    if ($tapeDrives) {
        Write-Host "`n找到磁带驱动器:" -ForegroundColor Green
        foreach ($drive in $tapeDrives) {
            Write-Host "  名称: $($drive.Name)" -ForegroundColor White
            Write-Host "  制造商: $($drive.Manufacturer)" -ForegroundColor White
            Write-Host "  状态: $($drive.Status)" -ForegroundColor White
            Write-Host "  ID: $($drive.DeviceID)" -ForegroundColor White
        }
    } else {
        Write-Host "未找到磁带驱动器" -ForegroundColor Red
        return
    }
    
    # 2. 尝试通过SCSI端口查询
    Write-Host "`n尝试获取SCSI设备信息..." -ForegroundColor Cyan
    $scsiDevices = Get-WmiObject -Class Win32_SCSIControllerDevice | Where-Object {$_.Dependent -like "*Tape*"}
    if ($scsiDevices) {
        foreach ($device in $scsiDevices) {
            Write-Host "  SCSI设备: $($device.Dependent)" -ForegroundColor White
        }
    }
    
    # 3. 检查存储设备
    Write-Host "`n存储设备信息:" -ForegroundColor Cyan
    $storageDevices = Get-WmiObject -Class Win32_LogicalDisk | Where-Object {$_.DriveType -eq 5}  # DriveType 5 = 可移动媒体
    if ($storageDevices) {
        foreach ($device in $storageDevices) {
            if ($device.Size -gt 0) {
                $sizeGB = [math]::Round($device.Size / 1GB, 2)
                $freeGB = [math]::Round($device.FreeSpace / 1GB, 2)
                Write-Host "  设备: $($device.DeviceID) 大小: ${sizeGB}GB 可用: ${freeGB}GB" -ForegroundColor White
            }
        }
    } else {
        Write-Host "  未找到可移动存储设备" -ForegroundColor Yellow
    }
}

Get-TapeDetailedInfo