// 系统日志管理
document.addEventListener('DOMContentLoaded', function() {
    const logContainer = document.getElementById('logContainer');
    const refreshLogsBtn = document.getElementById('refreshLogsBtn');
    const clearLogsBtn = document.getElementById('clearLogsBtn');
    const logCategory = document.getElementById('logCategory');
    const logLevel = document.getElementById('logLevel');
    const logOperationType = document.getElementById('logOperationType');
    const logPaginationInfo = document.getElementById('logPaginationInfo');
    const logPrevPage = document.getElementById('logPrevPage');
    const logNextPage = document.getElementById('logNextPage');
    
    if (!logContainer) return;
    
    let currentPage = 0;
    const pageSize = 50;
    let totalLogs = 0;
    let currentLogs = [];
    
    // 加载日志
    async function loadLogs() {
        try {
            logContainer.innerHTML = '<div class="text-center text-muted py-5"><i class="bi bi-hourglass-split me-2"></i>加载中...</div>';
            
            const params = new URLSearchParams();
            if (logCategory.value) params.append('category', logCategory.value);
            if (logLevel.value) params.append('level', logLevel.value);
            if (logOperationType.value) params.append('operation_type', logOperationType.value);
            params.append('limit', pageSize);
            params.append('offset', currentPage * pageSize);
            
            const response = await fetch(`/api/system/logs?${params.toString()}`);
            const data = await response.json();
            
            if (data.success) {
                currentLogs = data.logs || [];
                totalLogs = data.total || 0;
                renderLogs(currentLogs);
                updatePagination();
            } else {
                logContainer.innerHTML = '<div class="text-center text-danger py-5"><i class="bi bi-exclamation-triangle me-2"></i>加载失败</div>';
            }
        } catch (error) {
            console.error('加载日志失败:', error);
            logContainer.innerHTML = '<div class="text-center text-danger py-5"><i class="bi bi-exclamation-triangle me-2"></i>加载失败: ' + error.message + '</div>';
        }
    }
    
    // 渲染日志
    function renderLogs(logs) {
        if (!logs || logs.length === 0) {
            logContainer.innerHTML = '<div class="text-center text-muted py-5"><i class="bi bi-inbox me-2"></i>暂无日志</div>';
            return;
        }
        
        const logsHtml = logs.map(log => {
            const timestamp = log.timestamp ? new Date(log.timestamp).toLocaleString('zh-CN') : '未知时间';
            const levelClass = getLevelClass(log.level);
            const levelIcon = getLevelIcon(log.level);
            const barClass = getLevelBarClass(log.level);
            const typeBadge = log.type === 'operation' ? '<span class="badge bg-info me-1">操作</span>' : '<span class="badge bg-secondary me-1">系统</span>';
            
            let content = '';
            if (log.type === 'operation') {
                content = `
                    <div class="mb-1">
                        <strong>${log.operation_name || '未知操作'}</strong>
                        ${log.operation_description ? `<span class="text-muted ms-2">${log.operation_description}</span>` : ''}
                    </div>
                    ${log.resource_name ? `<div class="text-muted small">资源: ${log.resource_name}</div>` : ''}
                    ${log.username ? `<div class="text-muted small">用户: ${log.username}</div>` : ''}
                    ${log.success !== undefined ? `<div class="small"><span class="badge ${log.success ? 'bg-success' : 'bg-danger'}">${log.success ? '成功' : '失败'}</span></div>` : ''}
                `;
            } else {
                content = `
                    <div class="mb-1">
                        <strong>${log.message || '未知消息'}</strong>
                    </div>
                    ${log.module ? `<div class="text-muted small">模块: ${log.module}</div>` : ''}
                    ${log.function ? `<div class="text-muted small">函数: ${log.function}</div>` : ''}
                    ${log.file_path ? `<div class="text-muted small">文件: ${log.file_path}</div>` : ''}
                `;
            }
            
            return `
                <div class="log-entry d-flex mb-2">
                    <div class="log-bar ${barClass}"></div>
                    <div class="content flex-grow-1">
                        <div class="d-flex justify-content-between align-items-start mb-1">
                            <div class="flex-grow-1">
                                ${typeBadge}
                                <span class="badge ${getCategoryBadgeClass(log.category)} me-1">${getCategoryName(log.category)}</span>
                                <span class="badge ${levelClass} me-1">
                                    <i class="bi ${levelIcon} me-1"></i>${getLevelName(log.level)}
                                </span>
                                <span class="text-muted small ms-2">${timestamp}</span>
                            </div>
                        </div>
                        ${content}
                    </div>
                </div>
            `;
        }).join('');
        
        logContainer.innerHTML = logsHtml;
    }

    // 左侧颜色条样式类
    function getLevelBarClass(level) {
        const map = {
            'debug': 'bar-debug',
            'info': 'bar-info',
            'warning': 'bar-warning',
            'error': 'bar-error',
            'critical': 'bar-critical'
        };
        return map[level] || 'bar-debug';
    }
    
    // 获取级别样式类
    function getLevelClass(level) {
        const levelMap = {
            'debug': 'bg-secondary',
            'info': 'bg-info',
            'warning': 'bg-warning text-dark',
            'error': 'bg-danger',
            'critical': 'bg-dark text-danger'
        };
        return levelMap[level] || 'bg-secondary';
    }
    
    // 获取级别图标
    function getLevelIcon(level) {
        const iconMap = {
            'debug': 'bi-bug',
            'info': 'bi-info-circle',
            'warning': 'bi-exclamation-triangle',
            'error': 'bi-x-circle',
            'critical': 'bi-exclamation-octagon'
        };
        return iconMap[level] || 'bi-circle';
    }
    
    // 获取级别名称
    function getLevelName(level) {
        const nameMap = {
            'debug': '调试',
            'info': '信息',
            'warning': '警告',
            'error': '错误',
            'critical': '严重'
        };
        return nameMap[level] || level;
    }
    
    // 获取分类徽章样式
    function getCategoryBadgeClass(category) {
        const categoryMap = {
            'system': 'bg-primary',
            'tape': 'bg-success',
            'backup': 'bg-info',
            'recovery': 'bg-warning text-dark',
            'user': 'bg-secondary',
            'security': 'bg-danger',
            'scheduler': 'bg-dark',
            'api': 'bg-purple',
            'database': 'bg-indigo'
        };
        return categoryMap[category] || 'bg-secondary';
    }
    
    // 获取分类名称
    function getCategoryName(category) {
        const nameMap = {
            'system': '系统',
            'tape': '磁带',
            'backup': '备份',
            'recovery': '恢复',
            'user': '用户',
            'security': '安全',
            'scheduler': '计划任务',
            'api': 'API',
            'database': '数据库'
        };
        return nameMap[category] || category;
    }
    
    // 更新分页信息
    function updatePagination() {
        const start = currentPage * pageSize + 1;
        const end = Math.min((currentPage + 1) * pageSize, totalLogs);
        logPaginationInfo.textContent = `显示 ${start}-${end} 条，共 ${totalLogs} 条`;
        
        logPrevPage.disabled = currentPage === 0;
        logNextPage.disabled = end >= totalLogs;
    }
    
    // 事件监听
    if (refreshLogsBtn) {
        refreshLogsBtn.addEventListener('click', () => {
            currentPage = 0;
            loadLogs();
        });
    }
    
    if (clearLogsBtn) {
        clearLogsBtn.addEventListener('click', () => {
            if (confirm('确定要清空日志显示吗？')) {
                logContainer.innerHTML = '<div class="text-center text-muted py-5"><i class="bi bi-inbox me-2"></i>暂无日志</div>';
                currentLogs = [];
                totalLogs = 0;
                currentPage = 0;
                updatePagination();
            }
        });
    }
    
    if (logCategory) {
        logCategory.addEventListener('change', () => {
            currentPage = 0;
            loadLogs();
        });
    }
    
    if (logLevel) {
        logLevel.addEventListener('change', () => {
            currentPage = 0;
            loadLogs();
        });
    }
    
    if (logOperationType) {
        logOperationType.addEventListener('change', () => {
            currentPage = 0;
            loadLogs();
        });
    }
    
    if (logPrevPage) {
        logPrevPage.addEventListener('click', () => {
            if (currentPage > 0) {
                currentPage--;
                loadLogs();
            }
        });
    }
    
    if (logNextPage) {
        logNextPage.addEventListener('click', () => {
            const maxPage = Math.ceil(totalLogs / pageSize) - 1;
            if (currentPage < maxPage) {
                currentPage++;
                loadLogs();
            }
        });
    }
    
    // 当切换到日志标签页时自动加载
    const logsTab = document.querySelector('#logs-tab');
    if (logsTab) {
        logsTab.addEventListener('shown.bs.tab', () => {
            loadLogs();
        });
    }
    
    // 如果当前在日志标签页，自动加载
    const activeTab = document.querySelector('#logs.active');
    if (activeTab) {
        loadLogs();
    }
});

