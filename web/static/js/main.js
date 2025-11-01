// 主JavaScript文件
// Main JavaScript File

document.addEventListener('DOMContentLoaded', function() {
    // 初始化版本点击事件
    initVersionModal();
    
    // 修复模态框z-index问题
    fixModalZIndex();
});

// 修复模态框z-index，确保所有模态框显示在最上层
function fixModalZIndex() {
    // 监听所有模态框的显示事件
    const modals = document.querySelectorAll('.modal');
    modals.forEach(modal => {
        modal.addEventListener('show.bs.modal', function() {
            // 模态框显示时，确保正确的z-index
            this.style.zIndex = '9999';
            
            // 为所有modal-backdrop设置正确的z-index
            const backdrops = document.querySelectorAll('.modal-backdrop');
            backdrops.forEach(backdrop => {
                backdrop.style.zIndex = '9998';
            });
        });
        
        // 如果模态框已经显示，也要修复
        if (modal.classList.contains('show')) {
            modal.style.zIndex = '9999';
            const backdrops = document.querySelectorAll('.modal-backdrop');
            backdrops.forEach(backdrop => {
                backdrop.style.zIndex = '9998';
            });
        }
    });
}

/**
 * 初始化版本模态框
 */
function initVersionModal() {
    const versionLink = document.getElementById('version-link');
    if (!versionLink) return;

    versionLink.addEventListener('click', function(e) {
        e.preventDefault();
        loadChangelog();
    });
}

/**
 * 加载CHANGELOG并显示模态框
 */
async function loadChangelog() {
    const modal = new bootstrap.Modal(document.getElementById('versionModal'));
    modal.show();

    try {
        const response = await fetch('/api/system/version');
        const data = await response.json();

        const contentDiv = document.getElementById('changelog-content');
        
        if (data.changelog) {
            // 转换Markdown为HTML
            contentDiv.innerHTML = marked.parse(data.changelog);
        } else {
            contentDiv.innerHTML = '<p class="text-muted">暂无更新日志</p>';
        }
    } catch (error) {
        console.error('加载更新日志失败:', error);
        document.getElementById('changelog-content').innerHTML = 
            '<div class="alert alert-danger">加载更新日志失败，请稍后重试</div>';
    }
}

