/**
 * 计划任务工具函数模块
 * Scheduler Utilities Module
 */

/**
 * HTML转义
 */
export function escapeHtml(text) {
    const div = document.createElement('div');
    div.textContent = text;
    return div.innerHTML;
}

/**
 * 显示消息
 */
export function showMessage(message, type = 'info') {
    const alertClass = type === 'success' ? 'alert-success' : 
                      type === 'error' ? 'alert-danger' : 
                      type === 'warning' ? 'alert-warning' : 'alert-info';
    
    const alertDiv = document.createElement('div');
    alertDiv.className = `alert ${alertClass} alert-dismissible fade show position-fixed top-0 start-50 translate-middle-x mt-3`;
    alertDiv.style.zIndex = '9999';
    alertDiv.innerHTML = `
        ${message}
        <button type="button" class="btn-close" data-bs-dismiss="alert"></button>
    `;
    
    document.body.appendChild(alertDiv);
    
    setTimeout(() => {
        alertDiv.remove();
    }, 3000);
}

/**
 * 格式化文件大小
 */
export function formatFileSize(bytes) {
    if (!bytes || bytes === 0) return '0 B';
    
    const k = 1024;
    const sizes = ['B', 'KB', 'MB', 'GB', 'TB'];
    const i = Math.floor(Math.log(bytes) / Math.log(k));
    
    return Math.round(bytes / Math.pow(k, i) * 100) / 100 + ' ' + sizes[i];
}

/**
 * 获取单位标签
 */
export function getUnitLabel(unit) {
    const units = {
        'minutes': '分钟',
        'hours': '小时',
        'days': '天'
    };
    return units[unit] || unit;
}

/**
 * 安全获取元素值
 */
export function safeGetValue(elementId, defaultValue = '') {
    const element = document.getElementById(elementId);
    return element ? element.value : defaultValue;
}

/**
 * 安全获取整数元素值
 */
export function safeGetIntValue(elementId, defaultValue = 0) {
    const element = document.getElementById(elementId);
    if (!element) return defaultValue;
    const value = parseInt(element.value, 10);
    return isNaN(value) ? defaultValue : value;
}

/**
 * 检查元素是否可见
 */
export function isElementVisible(elementId) {
    const element = document.getElementById(elementId);
    if (!element) return false;
    const style = window.getComputedStyle(element);
    return style.display !== 'none' && style.visibility !== 'hidden';
}

