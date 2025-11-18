/**
 * 计划任务路径管理模块
 * Scheduler Path Manager Module
 */

import { escapeHtml, showMessage } from './scheduler-utils.js';

/**
 * 路径管理器类
 */
export class PathManager {
    constructor() {
        // 备份源路径列表
        this.backupSourcePaths = [];
        
        // 备份目标路径列表
        this.backupTargetPaths = [];
        
        // 磁带设备列表
        this.backupTapeDevices = [];
        
        // 磁带机设备列表（从API加载）
        this.tapeDevices = [];
    }
    
    /**
     * 添加源路径
     */
    addSourcePath(path) {
        if (!path) {
            showMessage('请输入路径', 'error');
            return false;
        }
        
        if (this.backupSourcePaths.includes(path)) {
            showMessage('该路径已添加', 'error');
            return false;
        }
        
        this.backupSourcePaths.push(path);
        this.renderSourcePathsList();
        return true;
    }
    
    /**
     * 移除源路径
     */
    removeSourcePath(index) {
        this.backupSourcePaths.splice(index, 1);
        this.renderSourcePathsList();
    }
    
    /**
     * 渲染源路径列表
     */
    renderSourcePathsList() {
        const container = document.getElementById('backupSourcePathsList');
        if (!container) return;
        
        if (this.backupSourcePaths.length === 0) {
            container.innerHTML = '';
            return;
        }
        
        container.innerHTML = this.backupSourcePaths.map((path, index) => {
            // 判断是否为网络路径
            const isNetwork = path.startsWith('\\\\') || path.startsWith('//') || path.startsWith('smb://');
            const icon = isNetwork ? 'bi-share' : 'bi-folder';
            const badge = isNetwork ? '<span class="badge bg-info ms-2">网络</span>' : '';
            
            return `
                <div class="card mb-2 bg-dark text-light">
                    <div class="card-body p-2">
                        <div class="d-flex align-items-center">
                            <i class="bi ${icon} me-2 text-primary fs-5"></i>
                            <div class="flex-grow-1" style="min-width: 0;">
                                <div class="d-flex align-items-center">
                                    <code class="text-truncate mb-0" style="font-size: 0.9rem; max-width: 100%;">${escapeHtml(path)}</code>
                                    ${badge}
                                </div>
                            </div>
                            <button class="btn btn-sm btn-outline-danger ms-2" onclick="window.pathManager.removeSourcePath(${index})" title="删除">
                                <i class="bi bi-trash"></i>
                            </button>
                        </div>
                    </div>
                </div>
            `;
        }).join('');
    }
    
    /**
     * 添加目标路径
     */
    addTargetPath(path) {
        if (!path) {
            showMessage('请输入路径', 'error');
            return false;
        }
        
        if (this.backupTargetPaths.includes(path)) {
            showMessage('该路径已添加', 'error');
            return false;
        }
        
        this.backupTargetPaths.push(path);
        this.renderTargetPathsList();
        return true;
    }
    
    /**
     * 移除目标路径
     */
    removeTargetPath(index) {
        this.backupTargetPaths.splice(index, 1);
        this.renderTargetPathsList();
    }
    
    /**
     * 渲染目标路径列表
     */
    renderTargetPathsList() {
        const container = document.getElementById('backupTargetPathsList');
        if (!container) return;
        
        if (this.backupTargetPaths.length === 0) {
            container.innerHTML = '';
            return;
        }
        
        container.innerHTML = this.backupTargetPaths.map((path, index) => {
            // 判断是否为网络路径
            const isNetwork = path.startsWith('\\\\') || path.startsWith('//') || path.startsWith('smb://');
            const icon = isNetwork ? 'bi-share' : 'bi-folder';
            const badge = isNetwork ? '<span class="badge bg-info ms-2">网络</span>' : '';
            
            return `
                <div class="card mb-2 bg-dark text-light">
                    <div class="card-body p-2">
                        <div class="d-flex align-items-center">
                            <i class="bi ${icon} me-2 text-light fs-5"></i>
                            <div class="flex-grow-1" style="min-width: 0;">
                                <div class="d-flex align-items-center">
                                    <code class="text-truncate mb-0 text-light" style="font-size: 0.9rem; max-width: 100%;">${escapeHtml(path)}</code>
                                    ${badge}
                                </div>
                            </div>
                            <button class="btn btn-sm btn-outline-light ms-2" onclick="window.pathManager.removeTargetPath(${index})" title="删除">
                                <i class="bi bi-trash"></i>
                            </button>
                        </div>
                    </div>
                </div>
            `;
        }).join('');
    }
    
    /**
     * 设置磁带设备列表
     */
    setTapeDevices(devices) {
        this.tapeDevices = devices || [];
    }
    
    /**
     * 渲染磁带设备选择下拉框
     */
    renderTapeDevicesSelect() {
        // 支持两个ID：backupTapeDevice 和 backupTapeDeviceSelect
        const select = document.getElementById('backupTapeDevice') || document.getElementById('backupTapeDeviceSelect');
        if (!select) return;
        
        // 清空现有选项
        select.innerHTML = '';
        
        // 添加"请选择"选项
        const placeholderOption = document.createElement('option');
        placeholderOption.value = '';
        placeholderOption.textContent = '请选择磁带机';
        select.appendChild(placeholderOption);
        
        // 添加磁带设备选项（只列出实际的磁带机，不包含"自动选择"和"全部"）
        this.tapeDevices.forEach(device => {
            const option = document.createElement('option');
            option.value = device.path || device.device_path || device;
            option.textContent = device.name || device.label || device.path || device.device_path || device;
            select.appendChild(option);
        });
        
        // 如果有当前值，恢复选中
        const currentValue = select.getAttribute('data-current-value');
        if (currentValue) {
            select.value = currentValue;
        }
    }
    
    /**
     * 添加磁带设备
     */
    addTapeDevice() {
        const select = document.getElementById('backupTapeDevice') || document.getElementById('backupTapeDeviceSelect');
        const selectedValue = select?.value;
        
        if (!selectedValue) {
            showMessage('请选择磁带机', 'error');
            return false;
        }
        
        // 检查是否已添加
        if (this.backupTapeDevices.includes(selectedValue)) {
            showMessage('该磁带机已添加', 'error');
            return false;
        }
        
        this.backupTapeDevices.push(selectedValue);
        this.renderTapeDevicesList();
        return true;
    }
    
    /**
     * 移除磁带设备
     */
    removeTapeDevice(index) {
        this.backupTapeDevices.splice(index, 1);
        this.renderTapeDevicesList();
    }
    
    /**
     * 渲染磁带设备列表
     */
    renderTapeDevicesList() {
        const container = document.getElementById('backupTapeDevicesList');
        if (!container) return;
        
        if (this.backupTapeDevices.length === 0) {
            container.innerHTML = '';
            return;
        }
        
        container.innerHTML = this.backupTapeDevices.map((devicePath, index) => {
            let displayText = devicePath;
            let icon = 'bi-hdd';
            
            // 查找设备信息
            const deviceInfo = this.tapeDevices.find(d => 
                (d.path || d.device_path) === devicePath
            );
            if (deviceInfo) {
                displayText = deviceInfo.name || deviceInfo.label || devicePath;
            }
            
            return `
                <div class="card mb-2 bg-dark text-light">
                    <div class="card-body p-2">
                        <div class="d-flex align-items-center">
                            <i class="bi ${icon} me-2 text-light fs-5"></i>
                            <div class="flex-grow-1" style="min-width: 0;">
                                <code class="text-truncate mb-0 text-light" style="font-size: 0.9rem; max-width: 100%;">${escapeHtml(displayText)}</code>
                            </div>
                            <button class="btn btn-sm btn-outline-light ms-2" onclick="window.pathManager.removeTapeDevice(${index})" title="删除">
                                <i class="bi bi-trash"></i>
                            </button>
                        </div>
                    </div>
                </div>
            `;
        }).join('');
    }
    
    /**
     * 清空所有路径
     */
    clearAll() {
        this.backupSourcePaths = [];
        this.backupTargetPaths = [];
        this.backupTapeDevices = [];
        this.renderSourcePathsList();
        this.renderTargetPathsList();
        this.renderTapeDevicesList();
    }
}

