// 系统设置JavaScript
// 数据库配置
document.addEventListener('DOMContentLoaded', function() {
    const dbType = document.getElementById('dbType');
    if (dbType) {
        loadDatabaseConfig();
        loadSystemConfig();
        loadServiceStatus();

        // systemd 服务开关
        const enableSystemService = document.getElementById('enableSystemService');
        if (enableSystemService) {
            enableSystemService.addEventListener('change', async function() {
                const action = this.checked ? 'install' : 'uninstall';
                const actionName = this.checked ? '安装' : '卸载';
                if (!confirm('确定要' + actionName + '系统服务吗？')) {
                    this.checked = !this.checked;
                    return;
                }
                try {
                    const r = await fetch('/api/system/service/' + action, { method: 'POST' });
                    const data = await r.json();
                    if (data.success) {
                        document.getElementById('serviceStatusHint').textContent = data.message;
                    } else {
                        alert(data.detail || data.message || actionName + '失败');
                        this.checked = !this.checked;
                    }
                } catch (e) {
                    alert(actionName + '失败: ' + e.message);
                    this.checked = !this.checked;
                }
                setTimeout(loadServiceStatus, 2000);
            });
        }
        
        // 测试数据库连接
        const testDbConnectionBtn = document.getElementById('testDbConnectionBtn');
        if (testDbConnectionBtn) {
            testDbConnectionBtn.addEventListener('click', async function() {
                const btn = this;
                const originalHtml = btn.innerHTML;
                btn.disabled = true;
                btn.innerHTML = '<i class="bi bi-arrow-clockwise spinner me-2"></i>测试中...';
                
                try {
                    const config = getDatabaseConfig();
                    const response = await fetch('/api/system/database/test', {
                        method: 'POST',
                        headers: {'Content-Type': 'application/json'},
                        body: JSON.stringify(config)
                    });
                    
                    const result = await response.json();
                    
                    if (result.success) {
                        alert('✅ 数据库连接测试成功！');
                    } else {
                        alert('❌ 数据库连接测试失败：' + result.message);
                    }
                } catch (error) {
                    alert('❌ 测试失败：' + error.message);
                } finally {
                    btn.disabled = false;
                    btn.innerHTML = originalHtml;
                }
            });
        }
    }
});

// 加载系统常规配置（包含磁带自动格式化选项）
async function loadSystemConfig() {
    try {
        const response = await fetch('/api/system/config');
        const data = await response.json();
        if (!data) return;

        const enableTapeFormatCheckbox = document.getElementById('enableTapeFormatBeforeFull');
        if (enableTapeFormatCheckbox && typeof data.enable_tape_format_before_full === 'boolean') {
            enableTapeFormatCheckbox.checked = data.enable_tape_format_before_full;
        }
    } catch (e) {
        console.error('加载系统配置失败:', e);
    }
}

// 获取数据库配置
function getDatabaseConfig() {
    const dbType = document.getElementById('dbType').value;
    const config = {
        db_type: dbType,
        pool_size: parseInt(document.getElementById('poolSize').value),
        max_overflow: parseInt(document.getElementById('maxOverflow').value)
    };

    // openGauss 服务器数据库
    config.db_host = document.getElementById('dbHost').value;
    config.db_port = parseInt(document.getElementById('dbPort').value);
    config.db_user = document.getElementById('dbUser').value;
    config.db_password = document.getElementById('dbPassword').value;
    config.db_database = document.getElementById('dbDatabase').value;

    return config;
}

// 保存数据库配置（统一由右上角按钮调用）
async function saveDatabaseConfigSection() {
    const dbTypeElement = document.getElementById('dbType');
    if (!dbTypeElement) {
        return true;
    }
    const config = getDatabaseConfig();

    // 验证必填字段 - 如果配置不完整，跳过保存（允许用户只保存其他配置）
    // openGauss：需要完整的主机/端口/用户名/密码/数据库名
    if (!config.db_host || config.db_host.trim() === '' ||
        !config.db_port ||
        !config.db_user || config.db_user.trim() === '' ||
        !config.db_password || config.db_password.trim() === '' ||
        !config.db_database || config.db_database.trim() === '') {
        console.log('openGauss 数据库配置不完整，跳过保存数据库配置');
        return true;
    }
    
    const response = await fetch('/api/system/database/config', {
        method: 'PUT',
        headers: {'Content-Type': 'application/json'},
        body: JSON.stringify(config)
    });
    
    const result = await response.json();
    if (!response.ok || !result.success) {
        throw new Error(result.message || '保存数据库配置失败');
    }
    return true;
}

// 加载数据库配置
async function loadDatabaseConfig() {
    try {
        const response = await fetch('/api/system/database/config');
        const config = await response.json();

        if (config) {
            document.getElementById('dbType').value = config.db_type || 'opengauss';

            // openGauss 配置
            if (config.db_host) document.getElementById('dbHost').value = config.db_host;
            if (config.db_port) document.getElementById('dbPort').value = config.db_port;
            if (config.db_user) document.getElementById('dbUser').value = config.db_user;
            if (config.db_password !== undefined) document.getElementById('dbPassword').value = config.db_password;
            if (config.db_database) document.getElementById('dbDatabase').value = config.db_database;

            if (config.pool_size) document.getElementById('poolSize').value = config.pool_size;
            if (config.max_overflow) document.getElementById('maxOverflow').value = config.max_overflow;

            document.getElementById('currentPoolSize').textContent = config.pool_size || '10';
            document.getElementById('currentMaxOverflow').textContent = config.max_overflow || '20';
        }

        // 加载数据库状态
        loadDatabaseStatus();
    } catch (error) {
        console.error('加载数据库配置失败:', error);
    }
}

// 加载数据库状态
async function loadDatabaseStatus() {
    try {
        const response = await fetch('/api/system/database/status');
        const status = await response.json();
        
        if (status) {
            const statusIndicator = document.querySelector('#dbStatus .status-indicator');
            const statusText = document.getElementById('dbStatusText');
            const dbTypeText = document.getElementById('dbTypeText');
            
            statusIndicator.className = 'status-indicator';
            
            if (status.status === 'online') {
                statusIndicator.classList.add('status-online');
                statusText.textContent = '已连接';
                statusText.className = 'text-success';
            } else if (status.status === 'offline') {
                statusIndicator.classList.add('status-offline');
                statusText.textContent = '未连接';
                statusText.className = 'text-danger';
            } else {
                statusIndicator.classList.add('status-warning');
                statusText.textContent = status.message || '未知';
                statusText.className = 'text-warning';
            }
            
            dbTypeText.textContent = status.db_type || 'Unknown';
        }
    } catch (error) {
        console.error('加载数据库状态失败:', error);
    }
}

// 定期刷新数据库状态
setInterval(loadDatabaseStatus, 30000); // 每30秒刷新一次

// 处理URL hash，自动打开对应的标签页
document.addEventListener('DOMContentLoaded', function() {
    const hash = window.location.hash;
    if (hash) {
        // 移除#号，获取标签页ID
        const tabId = hash.substring(1);
        // 查找对应的tab按钮（ID格式为 {tabId}-tab）
        const tabButton = document.getElementById(tabId + '-tab');
        if (tabButton) {
            // 使用Bootstrap的tab API切换标签页
            const tab = new bootstrap.Tab(tabButton);
            tab.show();
        }
    }

    // 切换标签页时把当前标签写入 URL hash，刷新后保持在当前标签页
    document.querySelectorAll('[data-bs-toggle="tab"]').forEach(function(btn) {
        btn.addEventListener('shown.bs.tab', function() {
            const target = this.getAttribute('data-bs-target') || this.getAttribute('href') || '';
            const tabId = target.replace('#', '');
            if (tabId) {
                history.replaceState(null, '', '#' + tabId);
            }
        });
    });
});

// ===== 通知配置JavaScript =====
document.addEventListener('DOMContentLoaded', function() {
    // 加载通知配置
    loadNotificationConfig();
    
    // 测试通知
    const testNotificationBtn = document.getElementById('testNotificationBtn');
    if (testNotificationBtn) {
        testNotificationBtn.addEventListener('click', async function() {
            const btn = this;
            const originalHtml = btn.innerHTML;
            btn.disabled = true;
            btn.innerHTML = '<i class="bi bi-arrow-clockwise spinner me-2"></i>发送中...';
            
            try {
                const phone = document.getElementById('dingtalkDefaultPhone').value;
                const response = await fetch(`/api/system/notification/test?phone=${phone}`, {
                    method: 'POST'
                });
                const result = await response.json();
                
                if (result.success) {
                    alert('✅ ' + result.message);
                } else {
                    alert('❌ ' + result.message);
                }
            } catch (error) {
                alert('❌ 测试失败：' + error.message);
            } finally {
                btn.disabled = false;
                btn.innerHTML = originalHtml;
            }
        });
    }
    
});

// 加载通知配置
function loadNotificationConfig() {
    fetch('/api/system/notification/config')
        .then(response => response.json())
        .then(data => {
            if (data.success && data.config) {
                const config = data.config;
                if (config.dingtalk_api_url) {
                    document.getElementById('dingtalkApiUrl').value = config.dingtalk_api_url;
                }
                if (config.dingtalk_api_key) {
                    document.getElementById('dingtalkApiKey').value = config.dingtalk_api_key;
                }
                if (config.dingtalk_default_phone) {
                    document.getElementById('dingtalkDefaultPhone').value = config.dingtalk_default_phone;
                    document.getElementById('defaultPhoneDisplay').textContent = config.dingtalk_default_phone;
                }
            }
        })
        .catch(error => console.error('加载通知配置失败:', error));
    
    // 加载通知事件配置
    fetch('/api/system/notification/events')
        .then(response => response.json())
        .then(data => {
            if (data.success && data.events) {
                const events = data.events;
                document.getElementById('notifyBackupSuccess').checked = events.notify_backup_success ?? true;
                document.getElementById('notifyBackupStarted').checked = events.notify_backup_started ?? true;
                document.getElementById('notifyBackupFailed').checked = events.notify_backup_failed ?? true;
                document.getElementById('notifyRecoverySuccess').checked = events.notify_recovery_success ?? true;
                document.getElementById('notifyRecoveryFailed').checked = events.notify_recovery_failed ?? true;
                document.getElementById('notifyTapeChange').checked = events.notify_tape_change ?? true;
                document.getElementById('notifyTapeExpired').checked = events.notify_tape_expired ?? true;
                document.getElementById('notifyTapeError').checked = events.notify_tape_error ?? true;
                document.getElementById('notifyCapacityWarning').checked = events.notify_capacity_warning ?? true;
                document.getElementById('notifySystemError').checked = events.notify_system_error ?? true;
                document.getElementById('notifySystemStarted').checked = events.notify_system_started ?? true;
            }
        })
        .catch(error => console.error('加载通知事件配置失败:', error));
}

// 保存通知配置（统一由右上角按钮调用）
async function saveNotificationSection() {
    const apiUrlInput = document.getElementById('dingtalkApiUrl');
    if (!apiUrlInput) {
        return true;
    }
    const config = {
        dingtalk_api_url: apiUrlInput.value,
        dingtalk_api_key: document.getElementById('dingtalkApiKey').value,
        dingtalk_default_phone: document.getElementById('dingtalkDefaultPhone').value
    };
    
    const response = await fetch('/api/system/notification/config', {
        method: 'PUT',
        headers: {'Content-Type': 'application/json'},
        body: JSON.stringify(config)
    });
    const result = await response.json();
    if (!response.ok || !result.success) {
        throw new Error(result.message || '保存通知配置失败');
    }
    
    const eventConfig = {
        notify_backup_success: document.getElementById('notifyBackupSuccess')?.checked ?? true,
        notify_backup_started: document.getElementById('notifyBackupStarted')?.checked ?? true,
        notify_backup_failed: document.getElementById('notifyBackupFailed')?.checked ?? true,
        notify_recovery_success: document.getElementById('notifyRecoverySuccess')?.checked ?? true,
        notify_recovery_failed: document.getElementById('notifyRecoveryFailed')?.checked ?? true,
        notify_tape_change: document.getElementById('notifyTapeChange')?.checked ?? true,
        notify_tape_expired: document.getElementById('notifyTapeExpired')?.checked ?? true,
        notify_tape_error: document.getElementById('notifyTapeError')?.checked ?? true,
        notify_capacity_warning: document.getElementById('notifyCapacityWarning')?.checked ?? true,
        notify_system_error: document.getElementById('notifySystemError')?.checked ?? true,
        notify_system_started: document.getElementById('notifySystemStarted')?.checked ?? true
    };
    
    const eventResponse = await fetch('/api/system/notification/events', {
        method: 'PUT',
        headers: {'Content-Type': 'application/json'},
        body: JSON.stringify(eventConfig)
    });
    const eventResult = await eventResponse.json();
    if (!eventResponse.ok || !eventResult.success) {
        throw new Error(eventResult.message || '保存通知事件配置失败');
    }
    
    const defaultPhoneDisplay = document.getElementById('defaultPhoneDisplay');
    if (defaultPhoneDisplay && config.dingtalk_default_phone) {
        defaultPhoneDisplay.textContent = config.dingtalk_default_phone;
    }

    // 保存 Syslog 配置
    const syslogEnabled = document.getElementById('syslogEnabled');
    if (syslogEnabled) {
        const syslogConfig = {
            syslog_enabled: syslogEnabled.checked,
            syslog_host: document.getElementById('syslogHost').value,
            syslog_port: parseInt(document.getElementById('syslogPort').value) || 514,
            syslog_level: document.getElementById('syslogLevel').value
        };
        const syslogResp = await fetch('/api/system/notification/syslog', {
            method: 'PUT',
            headers: {'Content-Type': 'application/json'},
            body: JSON.stringify(syslogConfig)
        });
        const syslogResult = await syslogResp.json();
        if (!syslogResp.ok || !syslogResult.success) {
            throw new Error(syslogResult.message || '保存 Syslog 配置失败');
        }
    }

    return true;
}

// 添加通知人员
async function addNotificationUser() {
    const phone = document.getElementById('notificationPhone').value.trim();
    const name = document.getElementById('notificationName').value.trim();
    const remark = document.getElementById('notificationRemark').value.trim();
    
    if (!phone || phone.length !== 11) {
        alert('请输入正确的11位手机号');
        return;
    }
    
    if (!name) {
        alert('请输入姓名');
        return;
    }
    
    try {
        const response = await fetch('/api/system/notification/users', {
            method: 'POST',
            headers: {'Content-Type': 'application/json'},
            body: JSON.stringify({
                phone: phone,
                name: name,
                remark: remark || null,
                enabled: true
            })
        });
        
        const result = await response.json();
        
        if (result.success) {
            alert('✅ 通知人员添加成功');
            // 重新加载通知人员列表
            await loadNotificationUsers();
            
            // 清空表单
            document.getElementById('notificationPhone').value = '';
            document.getElementById('notificationName').value = '';
            document.getElementById('notificationRemark').value = '';
            
            // 关闭模态框
            const modal = bootstrap.Modal.getInstance(document.getElementById('addNotificationUserModal'));
            modal.hide();
        } else {
            alert('❌ 添加失败：' + (result.message || '未知错误'));
        }
    } catch (error) {
        alert('❌ 添加失败：' + error.message);
    }
}

// 加载通知人员列表
async function loadNotificationUsers() {
    try {
        const response = await fetch('/api/system/notification/users');
        const result = await response.json();
        
        const container = document.getElementById('notificationUsers');
        if (!container) return;
        
        container.innerHTML = '';
        
        if (result.success && result.users && result.users.length > 0) {
            result.users.forEach(user => {
                addNotificationUserToList(user.id, user.phone, user.name, user.remark, user.enabled);
            });
        } else {
            container.innerHTML = '<p class="text-muted">暂无通知人员</p>';
        }
    } catch (error) {
        console.error('加载通知人员列表失败:', error);
    }
}

// 添加通知人员到列表显示
function addNotificationUserToList(userId, phone, name, remark, enabled) {
    const container = document.getElementById('notificationUsers');
    if (!container) return;
    
    const userDiv = document.createElement('div');
    userDiv.className = 'alert alert-light mb-2 border';
    userDiv.id = `notification-user-${userId}`;
    userDiv.innerHTML = `
        <div class="d-flex justify-content-between align-items-center">
            <div class="form-check">
                <input class="form-check-input" type="checkbox" ${enabled ? 'checked' : ''} 
                       onchange="toggleNotificationUser(${userId}, this.checked)">
                <label class="form-check-label">
                    <strong>${name || '未命名'}</strong> <small class="text-muted">${phone}</small>
                    ${remark ? `<br><small class="text-muted">${remark}</small>` : ''}
                </label>
            </div>
            <div>
                <button class="btn btn-sm btn-danger" onclick="deleteNotificationUser(${userId})">
                    <i class="bi bi-trash"></i> 删除
                </button>
            </div>
        </div>
    `;
    container.appendChild(userDiv);
}

// 切换通知人员启用状态
async function toggleNotificationUser(userId, enabled) {
    try {
        // 先获取用户信息
        const getResponse = await fetch('/api/system/notification/users');
        const getResult = await getResponse.json();
        
        if (!getResult.success || !getResult.users) {
            alert('❌ 获取通知人员信息失败');
            return;
        }
        
        const user = getResult.users.find(u => u.id === userId);
        if (!user) {
            alert('❌ 通知人员不存在');
            return;
        }
        
        // 更新用户信息
        const response = await fetch(`/api/system/notification/users/${userId}`, {
            method: 'PUT',
            headers: {'Content-Type': 'application/json'},
            body: JSON.stringify({
                phone: user.phone,
                name: user.name,
                remark: user.remark || null,
                enabled: enabled
            })
        });
        
        const result = await response.json();
        
        if (result.success) {
            console.log(`通知人员 ${user.name} ${enabled ? '已启用' : '已禁用'}`);
        } else {
            alert('❌ 更新失败：' + (result.message || '未知错误'));
            // 恢复复选框状态
            const checkbox = document.querySelector(`#notification-user-${userId} input[type="checkbox"]`);
            if (checkbox) checkbox.checked = !enabled;
        }
    } catch (error) {
        alert('❌ 更新失败：' + error.message);
        // 恢复复选框状态
        const checkbox = document.querySelector(`#notification-user-${userId} input[type="checkbox"]`);
        if (checkbox) checkbox.checked = !enabled;
    }
}

// 删除通知人员
async function deleteNotificationUser(userId) {
    if (!confirm('确定要删除该通知人员吗？')) {
        return;
    }
    
    try {
        const response = await fetch(`/api/system/notification/users/${userId}`, {
            method: 'DELETE'
        });
        
        const result = await response.json();
        
        if (result.success) {
            alert('✅ 通知人员删除成功');
            // 重新加载通知人员列表
            await loadNotificationUsers();
        } else {
            alert('❌ 删除失败：' + (result.message || '未知错误'));
        }
    } catch (error) {
        alert('❌ 删除失败：' + error.message);
    }
}

// 页面加载时加载通知人员列表
document.addEventListener('DOMContentLoaded', function() {
    // 当通知设置标签页激活时加载通知人员列表
    const notificationTab = document.getElementById('notification-tab');
    if (notificationTab) {
        notificationTab.addEventListener('shown.bs.tab', function() {
            loadNotificationUsers();
        });
        
        // 如果标签页已经激活，立即加载
        if (notificationTab.classList.contains('active')) {
            loadNotificationUsers();
        }
    }
});

// 系统信息标签页的版本链接
document.addEventListener('DOMContentLoaded', function() {
    const aboutVersionLink = document.getElementById('about-version-link');
    if (aboutVersionLink) {
        aboutVersionLink.addEventListener('click', function(e) {
            e.preventDefault();
            loadChangelog();
        });
    }
});

// ===== 统一系统配置管理 =====
// 加载所有系统配置
// 磁盘限流开关联动：关闭时把同组磁盘参数一并置灰（仅不可编辑，数值不变、照常提交）
// 联动字段：限流上限 / 限流恢复阈值 / 空间危急时压缩批次数
function applyDiskThrottleEditability(on) {
    ['diskThrottleHighPercent', 'diskThrottleLowPercent', 'diskCriticalBatches'].forEach(function (id) {
        const el = document.getElementById(id);
        if (el) el.disabled = !on;
    });
}

function applyDiskGuardEditability(on) {
    ['diskGuardSafetyFactor', 'diskCheckInterval', 'diskCheckMaxWaitMinutes'].forEach(function (id) {
        const el = document.getElementById(id);
        if (el) el.disabled = !on;
    });
}

// 两个磁盘保护至少保留一个：试图关掉最后一个时拦下并提示（后端亦有同样校验）
function guardAgainstClosingBoth(changedSwitch) {
    if (changedSwitch && changedSwitch.checked) return true;   // 本次是"打开"，放行
    const throttle = document.getElementById('diskThrottleEnabled');
    const guard = document.getElementById('diskGuardEnabled');
    if (!throttle || !guard) return true;
    if (!throttle.checked && !guard.checked) {
        alert('磁盘限流与空间守卫至少要保留一个：请先启用另一项，再关闭本项。');
        if (changedSwitch) {
            changedSwitch.checked = true;                       // 回滚本次操作
            if (changedSwitch.id === 'diskGuardEnabled') applyDiskGuardEditability(true);
            else applyDiskThrottleEditability(true);
        }
        return false;
    }
    return true;
}

async function loadAllSystemConfig() {
    try {
        const response = await fetch('/api/system/env-config');
        const result = await response.json();
        
        if (result.success && result.config) {
            const config = result.config;
            
            // 应用配置
            if (config.app_name) {
                const appNameInput = document.getElementById('appName');
                if (appNameInput) appNameInput.value = config.app_name;
            }
            if (config.log_level) {
                const logLevelSelect = document.getElementById('logLevel');
                if (logLevelSelect) logLevelSelect.value = (config.log_level || 'INFO').toUpperCase();
            } else {
                const logLevelSelect = document.getElementById('logLevel');
                if (logLevelSelect) logLevelSelect.value = 'INFO';
            }
            
            // Web服务配置
            if (config.web_host) {
                const webHostInput = document.getElementById('webHost');
                if (webHostInput) webHostInput.value = config.web_host;
            }
            if (config.web_port) {
                const webPortInput = document.getElementById('webPort');
                if (webPortInput) webPortInput.value = config.web_port;
            }
            if (config.enable_cors !== undefined) {
                const enableCorsCheckbox = document.getElementById('enableCors');
                if (enableCorsCheckbox) enableCorsCheckbox.checked = config.enable_cors;
            }
            
            // 磁带设备配置
            if (config.itdt_device_path) {
                const itdtDevicePathInput = document.getElementById('itdtDevicePath');
                if (itdtDevicePathInput) itdtDevicePathInput.value = config.itdt_device_path;
            }

            // LTFS工具配置
            if (config.ltfs_tools_dir) {
                const ltfsToolsDirInput = document.getElementById('ltfsToolsDir');
                if (ltfsToolsDirInput) ltfsToolsDirInput.value = config.ltfs_tools_dir;
            }
            if (config.ltfs_device_path) {
                const ltfsDevicePathInput = document.getElementById('ltfsDevicePath');
                if (ltfsDevicePathInput) ltfsDevicePathInput.value = config.ltfs_device_path;
            }
            
            // 备份策略配置
            if (config.default_retention_months) {
                const defaultRetentionMonthsInput = document.getElementById('defaultRetentionMonths');
                if (defaultRetentionMonthsInput) defaultRetentionMonthsInput.value = config.default_retention_months;
            }
            if (config.auto_erase_expired !== undefined) {
                const autoEraseExpiredCheckbox = document.getElementById('autoEraseExpired');
                if (autoEraseExpiredCheckbox) autoEraseExpiredCheckbox.checked = config.auto_erase_expired;
            }
            if (config.max_file_size) {
                // 将字节转换为GB
                const maxFileSizeGB = config.max_file_size / (1024 * 1024 * 1024);
                const maxFileSizeGBInput = document.getElementById('maxFileSizeGB');
                if (maxFileSizeGBInput) maxFileSizeGBInput.value = maxFileSizeGB.toFixed(1);
            }
            if (config.backup_compress_dir) {
                const backupCompressDirInput = document.getElementById('backupCompressDir');
                if (backupCompressDirInput) backupCompressDirInput.value = config.backup_compress_dir;
            }
            if (config.scan_update_interval) {
                const scanUpdateIntervalInput = document.getElementById('scanUpdateInterval');
                if (scanUpdateIntervalInput) scanUpdateIntervalInput.value = config.scan_update_interval;
            }
            if (config.scan_log_interval_seconds) {
                const scanLogIntervalInput = document.getElementById('scanLogIntervalSeconds');
                if (scanLogIntervalInput) scanLogIntervalInput.value = config.scan_log_interval_seconds;
            }
            if (config.scan_wait_timeout) {
                const scanWaitTimeoutInput = document.getElementById('scanWaitTimeout');
                if (scanWaitTimeoutInput) scanWaitTimeoutInput.value = config.scan_wait_timeout;
            }
            if (config.compression_parallel_batches) {
                const compressionParallelBatchesInput = document.getElementById('compressionParallelBatches');
                if (compressionParallelBatchesInput) compressionParallelBatchesInput.value = config.compression_parallel_batches;
            }
            if (config.compression_batches_reduction !== undefined) {
                const compressionBatchesReductionInput = document.getElementById('compressionBatchesReduction');
                if (compressionBatchesReductionInput) compressionBatchesReductionInput.value = config.compression_batches_reduction;
            }
            if (config.disk_throttle_high_percent !== undefined) {
                const el = document.getElementById('diskThrottleHighPercent');
                const sw = document.getElementById('diskThrottleEnabled');
                if (el) el.value = config.disk_throttle_high_percent;
                // 开关状态取自 DISK_THROTTLE_ENABLED（老数据回退：上限 > 0 视为启用）
                if (sw) {
                    const on = (config.disk_throttle_enabled !== undefined)
                        ? !!config.disk_throttle_enabled
                        : (Number(config.disk_throttle_high_percent) > 0);
                    sw.checked = on;
                    applyDiskThrottleEditability(on);
                    // 事件只注册一次（本函数会被反复调用）
                    if (!sw.dataset.bound) {
                        sw.dataset.bound = '1';
                        sw.addEventListener('change', function () {
                            if (guardAgainstClosingBoth(this)) {
                                applyDiskThrottleEditability(this.checked);
                            }
                        });
                    }
                }
            }
            // 大小文件组调度开关（默认启用）+ 计划05 参数
            const gsch = document.getElementById('groupSchedEnabled');
            if (gsch) {
                gsch.checked = (config.group_sched_enabled !== undefined)
                    ? !!config.group_sched_enabled
                    : true;
            }
            const gsr = document.getElementById('groupSchedRatio');
            if (gsr) gsr.value = config.group_sched_ratio || '2:1';
            const glt = document.getElementById('groupLargeThreshold');
            if (glt) glt.value = (config.group_large_threshold !== undefined && config.group_large_threshold !== null)
                ? config.group_large_threshold : 20000;
            const gsd = document.getElementById('groupScanDeferBelow');
            if (gsd) gsd.value = (config.group_scan_defer_below !== undefined && config.group_scan_defer_below !== null)
                ? config.group_scan_defer_below : 5000;
            const gpl = document.getElementById('groupPrefetchLeadSeconds');
            if (gpl) gpl.value = (config.group_prefetch_lead_seconds !== undefined && config.group_prefetch_lead_seconds !== null)
                ? config.group_prefetch_lead_seconds : 30;
            // 空间守卫开关（默认启用；老数据无该字段时按启用处理）
            const gsw = document.getElementById('diskGuardEnabled');
            if (gsw) {
                const gon = (config.disk_guard_enabled !== undefined)
                    ? !!config.disk_guard_enabled
                    : true;
                gsw.checked = gon;
                applyDiskGuardEditability(gon);
                if (!gsw.dataset.bound) {
                    gsw.dataset.bound = '1';
                    gsw.addEventListener('change', function () {
                        if (guardAgainstClosingBoth(this)) {
                            applyDiskGuardEditability(this.checked);
                        }
                    });
                }
            }
            if (config.disk_throttle_low_percent !== undefined) {
                const el = document.getElementById('diskThrottleLowPercent');
                if (el) el.value = config.disk_throttle_low_percent;
            }
            if (config.scan_engine !== undefined) {
                const el = document.getElementById('scanEngine');
                if (el) el.value = config.scan_engine;
            }
            if (config.disk_critical_batches !== undefined) {
                const el = document.getElementById('diskCriticalBatches');
                if (el) el.value = config.disk_critical_batches;
            }
            if (config.disk_check_max_wait_minutes !== undefined) {
                const el = document.getElementById('diskCheckMaxWaitMinutes');
                if (el) el.value = config.disk_check_max_wait_minutes;
            }
            if (config.disk_check_interval !== undefined) {
                const el = document.getElementById('diskCheckInterval');
                if (el) el.value = config.disk_check_interval;
            }
            if (config.disk_guard_safety_factor !== undefined) {
                const el = document.getElementById('diskGuardSafetyFactor');
                if (el) el.value = config.disk_guard_safety_factor;
            }


            // SSO 单点登录配置
            if (config.yz_login_url) {
                const yzLoginInput = document.getElementById('yzLoginUrl');
                if (yzLoginInput) yzLoginInput.value = config.yz_login_url;
            }
            if (config.taf_callback_url) {
                const tafCallbackInput = document.getElementById('tafCallbackUrl');
                if (tafCallbackInput) tafCallbackInput.value = config.taf_callback_url;
            }

            console.log('系统配置加载完成');
        }
    } catch (error) {
        console.error('加载系统配置失败:', error);
    }
}

// 保存所有系统配置
async function saveEnvConfigSection() {
    try {
        // 收集所有配置
        const config = {
            // 应用配置
            app_name: document.getElementById('appName')?.value || null,
            
            // Web服务配置
            web_host: document.getElementById('webHost')?.value || null,
            web_port: parseInt(document.getElementById('webPort')?.value) || null,
            enable_cors: document.getElementById('enableCors')?.checked || null,
            
            // ITDT工具配置
            itdt_device_path: document.getElementById('itdtDevicePath')?.value || null,
            
            // LTFS工具配置
            ltfs_tools_dir: document.getElementById('ltfsToolsDir')?.value || null,
            ltfs_device_path: document.getElementById('ltfsDevicePath')?.value || null,
            
            // 备份策略配置
            default_retention_months: parseInt(document.getElementById('defaultRetentionMonths')?.value) || null,
            auto_erase_expired: document.getElementById('autoEraseExpired')?.checked || null,
            max_file_size: parseFloat(document.getElementById('maxFileSizeGB')?.value) * 1024 * 1024 * 1024 || null,
            backup_compress_dir: document.getElementById('backupCompressDir')?.value || null,
            scan_update_interval: (() => {
                const input = document.getElementById('scanUpdateInterval');
                if (!input) return null;
                const value = input.value;
                if (value === '' || value === null || value === undefined) return null;
                const parsed = parseInt(value);
                return isNaN(parsed) ? null : parsed;
            })(),
            scan_log_interval_seconds: (() => {
                const input = document.getElementById('scanLogIntervalSeconds');
                if (!input) return null;
                const value = input.value;
                if (value === '' || value === null || value === undefined) return null;
                const parsed = parseInt(value);
                return isNaN(parsed) ? null : parsed;
            })(),
            scan_wait_timeout: (() => {
                const input = document.getElementById('scanWaitTimeout');
                if (!input) return null;
                const value = input.value;
                if (value === '' || value === null || value === undefined) return null;
                const parsed = parseInt(value);
                return isNaN(parsed) ? null : parsed;
            })(),
            compression_parallel_batches: (() => {
                const input = document.getElementById('compressionParallelBatches');
                if (!input) return null;
                const value = input.value;
                if (value === '' || value === null || value === undefined) return null;
                const parsed = parseInt(value);
                return isNaN(parsed) ? null : parsed;
            })(),
            compression_batches_reduction: (() => {
                const input = document.getElementById('compressionBatchesReduction');
                if (!input) return null;
                const value = input.value;
                if (value === '' || value === null || value === undefined) return null;
                const parsed = parseInt(value);
                return isNaN(parsed) ? null : parsed;
            })(),
            disk_throttle_enabled: (() => {
                const sw = document.getElementById('diskThrottleEnabled');
                return sw ? !!sw.checked : null;
            })(),
            disk_guard_enabled: (() => {
                const sw = document.getElementById('diskGuardEnabled');
                return sw ? !!sw.checked : null;
            })(),
            group_sched_enabled: (() => {
                const sw = document.getElementById('groupSchedEnabled');
                return sw ? !!sw.checked : null;
            })(),
            group_sched_ratio: (() => {
                const input = document.getElementById('groupSchedRatio');
                return (input && input.value.trim()) ? input.value.trim() : null;
            })(),
            group_large_threshold: (() => {
                const input = document.getElementById('groupLargeThreshold');
                if (!input || input.value === '') return null;
                const parsed = parseInt(input.value, 10);
                return isNaN(parsed) ? null : parsed;
            })(),
            group_scan_defer_below: (() => {
                const input = document.getElementById('groupScanDeferBelow');
                if (!input || input.value === '') return null;
                const parsed = parseInt(input.value, 10);
                return isNaN(parsed) ? null : parsed;
            })(),
            group_prefetch_lead_seconds: (() => {
                const input = document.getElementById('groupPrefetchLeadSeconds');
                if (!input || input.value === '') return null;
                const parsed = parseInt(input.value, 10);
                return isNaN(parsed) ? null : parsed;
            })(),
            disk_throttle_high_percent: (() => {
                const input = document.getElementById('diskThrottleHighPercent');
                if (!input || input.value === '') return null;
                const parsed = parseFloat(input.value);
                return isNaN(parsed) ? null : parsed;
            })(),
            disk_throttle_low_percent: (() => {
                const input = document.getElementById('diskThrottleLowPercent');
                if (!input || input.value === '') return null;
                const parsed = parseFloat(input.value);
                return isNaN(parsed) ? null : parsed;
            })(),
            disk_critical_batches: (() => {
                const input = document.getElementById('diskCriticalBatches');
                if (!input || input.value === '') return null;
                const parsed = parseInt(input.value);
                return isNaN(parsed) ? null : parsed;
            })(),
            scan_engine: document.getElementById('scanEngine')?.value || null,
            disk_guard_safety_factor: (() => {
                const input = document.getElementById('diskGuardSafetyFactor');
                if (!input || input.value === '') return null;
                const parsed = parseFloat(input.value);
                return isNaN(parsed) ? null : parsed;
            })(),
            disk_check_interval: (() => {
                const input = document.getElementById('diskCheckInterval');
                if (!input || input.value === '') return null;
                const parsed = parseInt(input.value);
                return isNaN(parsed) ? null : parsed;
            })(),
            disk_check_max_wait_minutes: (() => {
                const input = document.getElementById('diskCheckMaxWaitMinutes');
                if (!input || input.value === '') return null;
                const parsed = parseInt(input.value);
                return isNaN(parsed) ? null : parsed;
            })(),

            log_level: document.getElementById('logLevel')?.value || null,

            // 磁带自动格式化配置
            enable_tape_format_before_full: (() => {
                const checkbox = document.getElementById('enableTapeFormatBeforeFull');
                if (!checkbox) return undefined;
                return checkbox.checked === true;
            })(),

            // SSO 单点登录配置
            yz_login_url: document.getElementById('yzLoginUrl')?.value || null,
            taf_callback_url: document.getElementById('tafCallbackUrl')?.value || null,
        };
        
        // 移除 null 值
        Object.keys(config).forEach(key => {
            if (config[key] === null || config[key] === undefined) {
                delete config[key];
            }
        });
        
        const response = await fetch('/api/system/env-config', {
            method: 'PUT',
            headers: {'Content-Type': 'application/json'},
            body: JSON.stringify(config)
        });
        
        const result = await response.json();
        if (!response.ok || !result.success) {
            throw new Error(result.message || '保存失败');
        }
        return true;
    } catch (error) {
        throw error;
    }
}

async function saveAllSystemConfig() {
    try {
        await saveEnvConfigSection();
        await saveDatabaseConfigSection();
        await saveNotificationSection();
        if (typeof saveCompressionConfig === 'function') {
            await saveCompressionConfig({silent: true, reload: false});
        }
        alert('✅ 配置已保存（部分配置需要重启服务后生效）');
        return true;
    } catch (error) {
        console.error('保存系统配置失败:', error);
        alert('❌ 保存失败：' + error.message);
        return false;
    }
}

// 页面加载时加载配置
document.addEventListener('DOMContentLoaded', function() {
    // 加载所有系统配置
    loadAllSystemConfig();
    
    // 保存设置按钮
    const saveSettingsBtn = document.getElementById('saveSettingsBtn');
    if (saveSettingsBtn) {
        saveSettingsBtn.addEventListener('click', async function() {
            const btn = this;
            const originalHtml = btn.innerHTML;
            btn.disabled = true;
            btn.innerHTML = '<i class="bi bi-arrow-clockwise spinner me-2"></i>保存中...';
            
            try {
                // 保存所有配置
                await saveAllSystemConfig();
            } finally {
                btn.disabled = false;
                btn.innerHTML = originalHtml;
            }
        });
    }
    
    // 重启系统按钮
    const restartSystemBtn = document.getElementById('restartSystemBtn');
    if (restartSystemBtn) {
        restartSystemBtn.addEventListener('click', function() {
            if (confirm('确定要重启系统吗？这将中断所有正在运行的任务。')) {
                // 尝试通过 systemd 重启，失败则提示手动操作
                fetch('/api/system/service/restart', { method: 'POST' })
                    .then(function(r) { return r.json(); })
                    .then(function(data) {
                        if (data.success) {
                            alert('重启命令已发送，请等待服务重新启动...');
                            setTimeout(function() { window.location.reload(); }, 10000);
                        } else {
                            alert('systemd 重启失败: ' + (data.detail || data.message || '') + '\n请手动执行: sudo systemctl restart taf');
                        }
                    })
                    .catch(function() {
                        alert('无法通过服务管理器重启。\n请手动执行: sudo systemctl restart taf\n或: Ctrl+C 后重新运行 python main.py');
                    });
            }
        });
    }
});

// 加载 systemd 服务状态
function loadServiceStatus() {
    const checkbox = document.getElementById('enableSystemService');
    const hint = document.getElementById('serviceStatusHint');
    if (!checkbox || !hint) return;

    fetch('/api/system/service/status')
        .then(function(r) { return r.json(); })
        .then(function(data) {
            checkbox.checked = data.is_installed;
            if (data.is_systemd) {
                hint.textContent = 'systemd 服务模式运行中';
            } else if (data.is_installed) {
                hint.textContent = '已注册，当前手动运行';
            } else if (data.is_root) {
                hint.textContent = '未安装（以 root 运行，可开启）';
            } else {
                hint.textContent = '未安装（需要 root 权限，请手动执行: sudo cp deploy/taf.service /etc/systemd/system/）';
            }
        })
        .catch(function() {
            hint.textContent = '检测失败';
        });
}

// ===== Syslog 转发配置 =====
document.addEventListener('DOMContentLoaded', function() {
    // 加载 syslog 配置
    fetch('/api/system/notification/syslog')
        .then(r => r.json())
        .then(data => {
            if (data.success && data.config) {
                const c = data.config;
                document.getElementById('syslogEnabled').checked = c.syslog_enabled;
                document.getElementById('syslogHost').value = c.syslog_host || '192.168.0.14';
                document.getElementById('syslogPort').value = c.syslog_port || 514;
                document.getElementById('syslogLevel').value = (c.syslog_level || 'WARNING').toUpperCase();
                toggleSyslogSettings(c.syslog_enabled);
            }
        })
        .catch(err => console.error('加载 Syslog 配置失败:', err));

    // 启用/禁用开关
    document.getElementById('syslogEnabled').addEventListener('change', function() {
        toggleSyslogSettings(this.checked);
    });

    // 测试连接
    document.getElementById('testSyslogBtn').addEventListener('click', async function() {
        const btn = this;
        const originalHtml = btn.innerHTML;
        btn.disabled = true;
        btn.textContent = '测试中...';

        try {
            const resp = await fetch('/api/system/notification/syslog/test', {method: 'POST'});
            const result = await resp.json();
            showToast(result.message, result.success ? 'success' : 'danger');
        } catch (e) {
            showToast('测试失败: ' + e.message, 'danger');
        } finally {
            btn.disabled = false;
            btn.innerHTML = originalHtml;
        }
    });
});

function toggleSyslogSettings(enabled) {
    const el = document.getElementById('syslogSettings');
    if (el) {
        el.style.opacity = enabled ? '1' : '0.5';
        el.querySelectorAll('input, select, button').forEach(e => e.disabled = !enabled);
    }
}


/* ============================================================
 * 管理员设置（SSO 账号本地授权）
 *   GET  /api/system/sso-users          列出所有登录过的账号
 *   PUT  /api/system/sso-users/{id}     修改本地角色
 *   grigs 为永久管理员，不可修改
 * ============================================================ */
const SSO_ROLE_LABELS = {
    admin: { text: '管理员', cls: 'bg-success' },
    user: { text: '用户（不可登录）', cls: 'bg-secondary' },
    denied: { text: '禁止登录', cls: 'bg-danger' }
};

function ssoRoleOptions(current, permanent) {
    const roles = ['admin', 'user', 'denied'];
    return roles.map(r => {
        const sel = r === current ? ' selected' : '';
        return `<option value="${r}"${sel}>${(SSO_ROLE_LABELS[r] || { text: r }).text}</option>`;
    }).join('');
}

function ssoFmtTime(iso) {
    if (!iso) return '-';
    try {
        const d = new Date(iso);
        if (isNaN(d.getTime())) return iso;
        const p = n => String(n).padStart(2, '0');
        return `${d.getFullYear()}-${p(d.getMonth() + 1)}-${p(d.getDate())} ${p(d.getHours())}:${p(d.getMinutes())}`;
    } catch (e) { return iso; }
}

async function loadSsoUsers() {
    const body = document.getElementById('ssoUsersBody');
    const summary = document.getElementById('ssoUsersSummary');
    if (!body) return;
    try {
        const resp = await fetch('/api/system/sso-users');
        if (!resp.ok) throw new Error('HTTP ' + resp.status);
        const users = await resp.json();

        if (!Array.isArray(users) || users.length === 0) {
            body.innerHTML = '<tr><td colspan="8" class="text-center text-muted">暂无登录记录</td></tr>';
            if (summary) summary.textContent = '暂无记录';
            return;
        }

        // 统计：管理员数 / 被拒绝数
        const admins = users.filter(u => u.local_role === 'admin' || u.is_permanent_admin).length;
        const denied = users.filter(u => u.local_role !== 'admin' && !u.is_permanent_admin).length;
        if (summary) {
            summary.textContent = `共 ${users.length} 个账号 · 管理员 ${admins} · 不可登录 ${denied}`;
        }

        body.innerHTML = users.map(u => {
            const permanent = !!u.is_permanent_admin;
            const yzAdmin = Number(u.yz_is_admin) === 1;
            const role = u.local_role || 'denied';
            const roleBadge = permanent
                ? '<span class="badge bg-primary">永久管理员</span>'
                : `<select class="form-select form-select-sm sso-role-select" data-user-id="${u.yz_user_id}" style="min-width:9rem">
                       ${ssoRoleOptions(role, permanent)}
                   </select>`;
            const resultText = u.last_result === 'ok'
                ? '<span class="text-success">成功</span>'
                : (u.last_result ? '<span class="text-danger">被拒绝</span>' : '-');
            return `<tr${permanent ? ' class="table-primary"' : (role !== 'admin' ? ' class="text-muted"' : '')}>
                <td><span class="fw-semibold">${u.username || '-'}</span></td>
                <td>${u.display_name || '-'}</td>
                <td>${yzAdmin ? '<span class="badge bg-info text-dark">管理员</span>' : '<span class="badge bg-secondary">普通用户</span>'}</td>
                <td>${roleBadge}</td>
                <td>${ssoFmtTime(u.last_login_at)}</td>
                <td>${resultText}</td>
                <td class="text-end">${u.login_count || 0}</td>
                <td class="text-end">
                    ${permanent ? '<span class="text-muted">—</span>'
                        : `<button class="btn btn-sm btn-outline-danger sso-del-btn"
                                  data-user-id="${u.yz_user_id}"
                                  data-username="${(u.username || u.yz_user_id)}"
                                  title="删除该账号记录">
                               <i class="bi bi-trash"></i>
                           </button>`}
                </td>
            </tr>`;
        }).join('');

        // 绑定删除按钮（grigs 不会渲染此按钮）
        body.querySelectorAll('.sso-del-btn').forEach(btn => {
            btn.addEventListener('click', async function () {
                const uid = this.dataset.userId;
                const uname = this.dataset.username;
                if (!window.confirm(`确定删除账号「${uname}」的记录？\n\n删除后该账号下次登录会被视作首次出现，重新继承网管中心角色。`)) return;
                this.disabled = true;
                try {
                    const r = await fetch('/api/system/sso-users/' + encodeURIComponent(uid), { method: 'DELETE' });
                    const data = await r.json().catch(() => ({}));
                    if (!r.ok) throw new Error(data.detail || ('HTTP ' + r.status));
                    if (typeof showToast === 'function') showToast(`已删除账号「${uname}」`, 'success');
                    loadSsoUsers();
                } catch (e) {
                    if (typeof showToast === 'function') showToast('删除失败: ' + e.message, 'danger');
                    this.disabled = false;
                }
            });
        });

        // 绑定下拉变更
        body.querySelectorAll('.sso-role-select').forEach(sel => {
            sel.addEventListener('change', async function () {
                const uid = this.dataset.userId;
                const newRole = this.value;
                this.disabled = true;
                try {
                    const r = await fetch('/api/system/sso-users/' + encodeURIComponent(uid), {
                        method: 'PUT',
                        headers: { 'Content-Type': 'application/json' },
                        body: JSON.stringify({ local_role: newRole })
                    });
                    const data = await r.json().catch(() => ({}));
                    if (!r.ok) throw new Error(data.detail || ('HTTP ' + r.status));
                    if (typeof showToast === 'function') {
                        showToast(`账号 ${uid} 已设为「${(SSO_ROLE_LABELS[newRole] || {}).text || newRole}」`, 'success');
                    }
                    this.disabled = false;
                    loadSsoUsers();
                } catch (e) {
                    if (typeof showToast === 'function') showToast('修改失败: ' + e.message, 'danger');
                    this.disabled = false;
                    loadSsoUsers();   // 回滚到服务端真实状态
                }
            });
        });
    } catch (e) {
        body.innerHTML = `<tr><td colspan="8" class="text-center text-danger">加载失败: ${e.message}</td></tr>`;
        if (summary) summary.textContent = '加载失败';
    }
}

document.addEventListener('DOMContentLoaded', function () {
    const refreshBtn = document.getElementById('ssoUsersRefresh');
    if (refreshBtn) refreshBtn.addEventListener('click', loadSsoUsers);

    // 首次进入「管理员设置」标签页时加载（避免每次打开系统设置都查库）
    const tabBtn = document.getElementById('sso-users-tab');
    if (tabBtn) {
        tabBtn.addEventListener('shown.bs.tab', function () { loadSsoUsers(); });
    }
});
