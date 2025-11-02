// 主JavaScript文件
// Main JavaScript File

document.addEventListener('DOMContentLoaded', function() {
    // 初始化版本点击事件
    initVersionModal();
    
    // 修复模态框z-index问题
    fixModalZIndex();
    
    // 初始化所有模态框拖拽功能
    initModalDraggable();
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

/**
 * 初始化所有模态框的拖拽功能
 */
function initModalDraggable() {
    // 为所有模态框添加拖拽功能
    document.querySelectorAll('.modal').forEach(modal => {
        // 为每个模态框添加显示事件监听
        modal.addEventListener('show.bs.modal', function() {
            const modalDialog = this.querySelector('.modal-dialog');
            const modalHeader = this.querySelector('.modal-header');
            if (!modalDialog || !modalHeader) return;
            
            // 设置模态框为可拖拽
            makeDraggable(modalDialog, modalHeader);
        });
        
        // 如果模态框已经显示，也要初始化拖拽
        if (modal.classList.contains('show')) {
            const modalDialog = modal.querySelector('.modal-dialog');
            const modalHeader = modal.querySelector('.modal-header');
            if (modalDialog && modalHeader) {
                makeDraggable(modalDialog, modalHeader);
            }
        }
    });
}

/**
 * 使元素可拖拽
 * @param {HTMLElement} element - 要拖拽的元素
 * @param {HTMLElement} handle - 拖拽手柄（通常是header）
 */
function makeDraggable(element, handle) {
    let isDragging = false;
    let currentX;
    let currentY;
    let initialX;
    let initialY;
    let xOffset = 0;
    let yOffset = 0;
    
    // 给拖拽手柄添加样式
    handle.style.cursor = 'move';
    
    // 鼠标按下事件
    handle.addEventListener('mousedown', dragStart);
    
    // 触摸开始事件（移动端支持）
    handle.addEventListener('touchstart', touchStart);
    
    function dragStart(e) {
        // 防止拖拽按钮等元素
        if (e.target.tagName === 'BUTTON' || e.target.tagName === 'A') {
            return;
        }
        
        e.preventDefault();
        e.stopPropagation();
        
        // 获取鼠标初始位置和元素当前位置
        initialX = e.clientX;
        initialY = e.clientY;
        
        // 获取元素的当前transform值
        const transform = element.style.transform;
        if (transform) {
            const matches = transform.match(/translate\((-?\d+)px,\s*(-?\d+)px\)/);
            if (matches) {
                xOffset = parseInt(matches[1], 10);
                yOffset = parseInt(matches[2], 10);
            }
        }
        
        isDragging = true;
        
        // 添加移动和释放事件监听
        document.addEventListener('mousemove', drag);
        document.addEventListener('mouseup', dragEnd);
    }
    
    function touchStart(e) {
        // 防止拖拽按钮等元素
        if (e.target.tagName === 'BUTTON' || e.target.tagName === 'A') {
            return;
        }
        
        e.preventDefault();
        e.stopPropagation();
        
        const touch = e.touches[0];
        initialX = touch.clientX;
        initialY = touch.clientY;
        
        isDragging = true;
        document.addEventListener('touchmove', touchDrag);
        document.addEventListener('touchend', touchEnd);
    }
    
    function drag(e) {
        if (!isDragging) return;
        
        e.preventDefault();
        e.stopPropagation();
        
        currentX = xOffset + (e.clientX - initialX);
        currentY = yOffset + (e.clientY - initialY);
        
        setTranslate(currentX, currentY, element);
    }
    
    function touchDrag(e) {
        if (!isDragging) return;
        
        e.preventDefault();
        e.stopPropagation();
        
        const touch = e.touches[0];
        currentX = xOffset + (touch.clientX - initialX);
        currentY = yOffset + (touch.clientY - initialY);
        
        setTranslate(currentX, currentY, element);
    }
    
    function dragEnd(e) {
        if (!isDragging) return;
        
        isDragging = false;
        xOffset = currentX;
        yOffset = currentY;
        
        // 移除事件监听
        document.removeEventListener('mousemove', drag);
        document.removeEventListener('mouseup', dragEnd);
    }
    
    function touchEnd(e) {
        if (!isDragging) return;
        
        isDragging = false;
        xOffset = currentX;
        yOffset = currentY;
        
        document.removeEventListener('touchmove', touchDrag);
        document.removeEventListener('touchend', touchEnd);
    }
    
    function setTranslate(xPos, yPos, el) {
        el.style.transform = `translate(${xPos}px, ${yPos}px)`;
    }
}

