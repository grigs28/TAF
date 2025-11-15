// 系统日志管理
// System Logs Management

let currentLogPage = 0;
const logPageSize = 50;

// 初始化系统日志
document.addEventListener('DOMContentLoaded', function() {
    // 当系统日志标签页激活时加载日志
    const logsTab = document.getElementById('logs-tab');
    if (logsTab) {
        logsTab.addEventListener('shown.bs.tab', function() {
            loadSystemLogs();
        });
        
        // 如果标签页已经激活，立即加载
        if (logsTab.classList.contains('active')) {
            loadSystemLogs();
        }
    }
    
    // 刷新日志按钮
    const refreshLogsBtn = document.getElementById('refreshLogsBtn');
    if (refreshLogsBtn) {
        refreshLogsBtn.addEventListener('click', function() {
            loadSystemLogs();
        });
    }
    
    // 清空日志按钮
    const clearLogsBtn = document.getElementById('clearLogsBtn');
    if (clearLogsBtn) {
        clearLogsBtn.addEventListener('click', function() {
            if (confirm('确定要清空系统日志吗？此操作不可恢复！')) {
                clearSystemLogs();
            }
        });
    }
    
    // 分页按钮
    const logPrevPage = document.getElementById('logPrevPage');
    const logNextPage = document.getElementById('logNextPage');
    
    if (logPrevPage) {
        logPrevPage.addEventListener('click', function() {
            if (currentLogPage > 0) {
                currentLogPage--;
                loadSystemLogs();
            }
        });
    }
    
    if (logNextPage) {
        logNextPage.addEventListener('click', function() {
            currentLogPage++;
            loadSystemLogs();
        });
    }
});

// 加载系统日志
async function loadSystemLogs() {
    const logContainer = document.getElementById('logContainer');
    if (!logContainer) return;
    
    // 显示加载中
    logContainer.innerHTML = '<div class="text-center text-muted py-5"><i class="bi bi-hourglass-split me-2"></i>加载中...</div>';
    
    try {
        // 获取筛选条件
        const category = document.getElementById('logCategory')?.value || '';
        const level = document.getElementById('logLevel')?.value || '';
        const operationType = document.getElementById('logOperationType')?.value || '';
        
        // 构建查询参数
        const params = new URLSearchParams({
            limit: logPageSize.toString(),
            offset: (currentLogPage * logPageSize).toString()
        });
        
        if (category) params.append('category', category);
        if (level) params.append('level', level);
        if (operationType) params.append('operation_type', operationType);
        
        const response = await fetch(`/api/system/logs?${params.toString()}`);
        
        if (!response.ok) {
            throw new Error(`HTTP ${response.status}: ${response.statusText}`);
        }
        
        const result = await response.json();
        
        if (result.success) {
            const logs = result.logs || [];
            const total = result.total !== undefined ? result.total : logs.length;
            displaySystemLogs(logs, total);
            updateLogPagination(total);
        } else {
            logContainer.innerHTML = `<div class="text-center text-danger py-5">
                <i class="bi bi-exclamation-triangle me-2"></i>加载失败：${result.message || result.detail || '未知错误'}
            </div>`;
        }
    } catch (error) {
        console.error('加载系统日志失败:', error);
        logContainer.innerHTML = `<div class="text-center text-danger py-5">
            <i class="bi bi-exclamation-triangle me-2"></i>加载失败：${error.message}
        </div>`;
    }
}

// 显示系统日志
function displaySystemLogs(logs, total) {
    const logContainer = document.getElementById('logContainer');
    if (!logContainer) return;
    
    if (logs.length === 0) {
        logContainer.innerHTML = '<div class="text-center text-muted py-5"><i class="bi bi-inbox me-2"></i>暂无日志</div>';
        return;
    }
    
    let html = '';
    logs.forEach(log => {
        const timestamp = log.timestamp ? new Date(log.timestamp).toLocaleString('zh-CN') : '未知时间';
        const level = log.level || 'info';
        const levelClass = `bar-${level}`;
        const levelBadge = getLevelBadge(level);
        const category = log.category || 'system';
        const categoryBadge = getCategoryBadge(category);
        
        // 根据日志类型显示不同内容
        if (log.type === 'operation') {
            // 操作日志
            const operationName = log.operation_name || log.operation_type || '未知操作';
            const success = log.success !== false;
            const successBadge = success 
                ? '<span class="badge bg-success">成功</span>' 
                : '<span class="badge bg-danger">失败</span>';
            
            html += `
                <div class="log-entry mb-2 d-flex">
                    <div class="log-bar ${levelClass}"></div>
                    <div class="content flex-grow-1">
                        <div class="content-line d-flex align-items-center flex-wrap">
                            ${levelBadge}
                            ${categoryBadge}
                            ${successBadge}
                            <span class="message">${operationName}</span>
                            <small class="text-muted ms-auto">${timestamp}</small>
                        </div>
                        ${log.operation_description ? `<div class="text-muted small mt-1">${log.operation_description}</div>` : ''}
                        ${log.error_message ? `<div class="text-danger small mt-1">错误：${log.error_message}</div>` : ''}
                        ${log.result_message ? `<div class="text-success small mt-1">${log.result_message}</div>` : ''}
                    </div>
                </div>
            `;
        } else {
            // 系统日志
            const message = log.message || '无消息';
            const module = log.module || '';
            const functionName = log.function || '';
            
            html += `
                <div class="log-entry mb-2 d-flex">
                    <div class="log-bar ${levelClass}"></div>
                    <div class="content flex-grow-1">
                        <div class="content-line d-flex align-items-center flex-wrap">
                            ${levelBadge}
                            ${categoryBadge}
                            <span class="message">${message}</span>
                            <small class="text-muted ms-auto">${timestamp}</small>
                        </div>
                        ${module || functionName ? `<div class="text-muted small mt-1">${module}${functionName ? '.' + functionName : ''}</div>` : ''}
                        ${log.stack_trace ? `<div class="text-danger small mt-1" style="white-space: pre-wrap; font-family: monospace;">${log.stack_trace}</div>` : ''}
                    </div>
                </div>
            `;
        }
    });
    
    logContainer.innerHTML = html;
}

// 获取级别徽章
function getLevelBadge(level) {
    const badges = {
        'debug': '<span class="badge bg-secondary">调试</span>',
        'info': '<span class="badge bg-info">信息</span>',
        'warning': '<span class="badge bg-warning text-dark">警告</span>',
        'error': '<span class="badge bg-danger">错误</span>',
        'critical': '<span class="badge bg-dark">严重</span>'
    };
    return badges[level] || badges['info'];
}

// 获取分类徽章
function getCategoryBadge(category) {
    const badges = {
        'system': '<span class="badge bg-primary">系统</span>',
        'backup': '<span class="badge bg-success">备份</span>',
        'recovery': '<span class="badge bg-info">恢复</span>',
        'tape': '<span class="badge bg-warning text-dark">磁带</span>',
        'user': '<span class="badge bg-secondary">用户</span>',
        'security': '<span class="badge bg-danger">安全</span>',
        'scheduler': '<span class="badge bg-purple">计划任务</span>',
        'api': '<span class="badge bg-cyan">API</span>',
        'database': '<span class="badge bg-orange">数据库</span>'
    };
    return badges[category] || '<span class="badge bg-secondary">' + category + '</span>';
}

// 更新日志分页信息
function updateLogPagination(total) {
    const paginationInfo = document.getElementById('logPaginationInfo');
    const logPrevPage = document.getElementById('logPrevPage');
    const logNextPage = document.getElementById('logNextPage');
    
    if (paginationInfo) {
        const start = currentLogPage * logPageSize + 1;
        const end = Math.min((currentLogPage + 1) * logPageSize, total);
        paginationInfo.textContent = `显示 ${start}-${end} 条，共 ${total} 条`;
    }
    
    if (logPrevPage) {
        logPrevPage.disabled = currentLogPage === 0;
    }
    
    if (logNextPage) {
        logNextPage.disabled = (currentLogPage + 1) * logPageSize >= total;
    }
}

// 清空系统日志
async function clearSystemLogs() {
    try {
        // 注意：这里需要后端提供清空日志的API
        // 目前先提示用户
        alert('清空日志功能需要后端API支持，请联系管理员');
    } catch (error) {
        console.error('清空系统日志失败:', error);
        alert('清空失败：' + error.message);
    }
}
