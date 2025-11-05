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
            const typeText = log.type === 'operation' ? '操作' : '系统';
            
            // 单行文本内容
            const primaryText = log.type === 'operation'
                ? (log.operation_name || log.operation_description || log.message || '未知操作')
                : (log.message || '未知消息');
            const resource = log.resource_name ? ` ${log.resource_name}` : '';
            const userText = log.username ? ` ${log.username}` : '';
            const successText = (log.success === true) ? ' 成功' : (log.success === false ? ' 失败' : '');
            
            return `
                <div class="log-entry d-flex mb-2">
                    <div class="log-bar ${barClass}"></div>
                    <div class="content flex-grow-1">
                        <div class="content-line">
                            <span class="text-muted small">${typeText}</span>
                            <span class="badge ${getCategoryBadgeClass(log.category)}">${getCategoryName(log.category)}</span>
                            <span class="badge ${levelClass}"><i class="bi ${levelIcon} me-1"></i>${getLevelName(log.level)}</span>
                            <span class="text-muted small">${timestamp}</span>
                            <span class="message">${primaryText}${resource}${userText}${successText}</span>
                        </div>
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

