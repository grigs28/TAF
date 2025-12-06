(function () {
    const POLL_INTERVAL = 2000;  // 减少到2秒，进一步提高状态更新频率
    const REFRESH_INTERVAL = 30000;

    const dom = {
        totalTasks: document.getElementById('totalTasks'),
        completedTasks: document.getElementById('completedTasks'),
        runningTasksCounter: document.getElementById('runningTasks'),
        failedTasks: document.getElementById('failedTasks'),
        runningList: document.getElementById('runningTasksList'),
        allTasksTable: document.getElementById('allTasksTable'),
        statusFilter: document.getElementById('statusFilter'),
        typeFilter: document.getElementById('typeFilter'),
        searchInput: document.getElementById('searchInput'),
        searchBtn: document.getElementById('searchBtn'),
    };

    let runningInterval = null;
    // 记录已完成的任务ID，这些任务不再刷新
    const completedTaskIds = new Set();

    function formatBytes(bytes) {
        if (!bytes || bytes <= 0) return '0 B';
        const units = ['B', 'KB', 'MB', 'GB', 'TB'];
        const index = Math.floor(Math.log(bytes) / Math.log(1024));
        return (bytes / Math.pow(1024, index)).toFixed(2) + ' ' + units[index];
    }

    function formatDateTime(value) {
        if (!value) return '-';
        const date = new Date(value);
        if (Number.isNaN(date.getTime())) return '-';
        return date.toLocaleString('zh-CN', {
            year: 'numeric',
            month: '2-digit',
            day: '2-digit',
            hour: '2-digit',
            minute: '2-digit',
            second: '2-digit',
        });
    }

    function formatElapsedTime(startedAt, completedAt) {
        if (!startedAt) return '-';

        // 确保时间戳正确解析，处理带时区的时间格式
        let start, end;
        try {
            start = new Date(startedAt);
            // 如果是运行中的任务，使用当前时间而不是 completedAt
            end = completedAt ? new Date(completedAt) : new Date();

            // 验证日期是否有效
            if (Number.isNaN(start.getTime())) {
                console.warn('formatElapsedTime: 无效的开始时间:', startedAt);
                return '-';
            }
            if (Number.isNaN(end.getTime())) {
                console.warn('formatElapsedTime: 无效的结束时间:', completedAt);
                return '-';
            }
        } catch (error) {
            console.error('formatElapsedTime: 时间解析错误:', error);
            return '-';
        }

        const diffMs = end - start;
        if (diffMs <= 0) return '0秒';

        const seconds = Math.floor(diffMs / 1000);
        const minutes = Math.floor(seconds / 60);
        const hours = Math.floor(minutes / 60);
        const days = Math.floor(hours / 24);

        // 格式化显示，总是显示秒数
        if (days > 0) return `${days}天${hours % 24}小时${minutes % 60}分钟`;
        if (hours > 0) return `${hours}小时${minutes % 60}分钟`;
        if (minutes > 0) return `${minutes}分钟${seconds % 60}秒`;
        return `${seconds}秒`;
    }

    function calculateProcessingSpeed(processedBytes, startedAt, completedAt) {
        if (!processedBytes || processedBytes <= 0 || !startedAt) return null;
        const start = new Date(startedAt);
        const end = completedAt ? new Date(completedAt) : new Date();
        const diffMs = end - start;
        if (diffMs <= 0) return null;
        // 转换为秒
        const seconds = diffMs / 1000;
        if (seconds <= 0) return null;
        // 将字节转换为GB
        const processedGB = processedBytes / (1024 * 1024 * 1024);
        // 计算每秒处理的GB数
        const speedGBPerSec = processedGB / seconds;
        return speedGBPerSec.toFixed(2);
    }
    
    function calculateProcessingSpeedGBPerSec(task) {
        // 优先使用后端传递的实时速度
        if (task.compression_speed_gb_per_sec !== null && task.compression_speed_gb_per_sec !== undefined) {
            const speed = parseFloat(task.compression_speed_gb_per_sec);
            if (!isNaN(speed) && speed > 0) {
                return speed.toFixed(4); // 保留更多小数位以提高精度
            }
        }
        
        // 如果没有实时速度，计算平均速度（G/秒）
        // 使用已处理数据/已用时间来计算
        if (task.processed_bytes && task.started_at) {
            const start = new Date(task.started_at);
            const end = task.completed_at ? new Date(task.completed_at) : new Date();
            const diffMs = end - start;
            if (diffMs > 0) {
                const seconds = diffMs / 1000;
                if (seconds > 0) {
                    const processedGB = task.processed_bytes / (1024 * 1024 * 1024);
                    const speedGBPerSec = processedGB / seconds;
                    if (!isNaN(speedGBPerSec) && speedGBPerSec > 0) {
                        return speedGBPerSec.toFixed(4); // 保留更多小数位以提高精度
                    }
                }
            }
        }
        
        return null;
    }

    async function fetchJSON(url, options) {
        const response = await fetch(url, options);
        if (!response.ok) {
            const text = await response.text();
            throw new Error(text || response.statusText);
        }
        return response.json();
    }

    async function loadBackupStatistics() {
        try {
            const stats = await fetchJSON('/api/backup/statistics');
            if (dom.totalTasks) dom.totalTasks.textContent = stats.total_tasks ?? '-';
            if (dom.completedTasks) dom.completedTasks.textContent = stats.completed_tasks ?? '-';
            if (dom.runningTasksCounter) dom.runningTasksCounter.textContent = stats.running_tasks ?? '-';
            if (dom.failedTasks) dom.failedTasks.textContent = stats.failed_tasks ?? '-';
        } catch (error) {
            console.error('加载备份统计失败:', error);
        }
    }

    function buildStatusBadge(task) {
        const status = (task.status || '').toLowerCase();
        const description = task.description || '';

        // 针对模板任务的特殊处理
        if (task.is_template) {
            if (task.from_scheduler) {
                // 计划任务的模板
                if (task.enabled === false) {
                    return '<span class="badge bg-secondary">已禁用</span>';
                } else {
                    return '<span class="badge bg-info text-dark">计划中</span>';
                }
            } else {
                // 普通模板
                return '<span class="badge bg-secondary">模板</span>';
            }
        }

        // 添加调试日志（对最近的任务或状态异常的任务）
        const taskId = task.task_id || task.id;
        const taskName = task.task_name || '';
        const isRecentTask = (
            taskId >= 26 ||
            taskName.includes('计划备份-20251123_234825') ||
            taskName.includes('计划备份-20251123_222248') ||
            taskName.includes('计划备份-20251124_022039')
        );
        const hasStarted = task.started_at && task.started_at !== null;
        const statusMismatch = hasStarted && status === 'pending';

        if (isRecentTask || statusMismatch) {
            console.log('buildStatusBadge: 任务状态判断:', {
                task_id: taskId,
                task_name: taskName,
                status: task.status,
                status_lower: status,
                status_type: typeof task.status,
                started_at: task.started_at,
                is_template: task.is_template,
                from_scheduler: task.from_scheduler,
                status_mismatch: statusMismatch
            });
        }

        if (description.includes('[格式化中]')) {
            return '<span class="badge bg-info">格式化中</span>';
        }

        // 执行记录的状态处理
        switch (status) {
            case 'completed':
                return '<span class="badge bg-success">成功</span>';
            case 'failed':
                return '<span class="badge bg-danger">失败</span>';
            case 'running':
                return '<span class="badge bg-primary">运行中</span>';
            case 'cancelled':
                return '<span class="badge bg-secondary">已取消</span>';
            case 'pending':
                // 对于执行记录的pending状态，需要进一步判断
                if (hasStarted) {
                    return '<span class="badge bg-warning text-dark">已开始</span>';
                } else {
                    return '<span class="badge bg-warning text-dark">等待中</span>';
                }
            default:
                return `<span class="badge bg-secondary">${status || '未知'}</span>`;
        }
    }

    function computeProgress(task) {
        // processedFiles: 已经压缩的文件数（由压缩工作线程更新）
        // totalFiles: 同步过来的总文件数（由后台扫描任务更新）
        // processedBytes: 已经压缩的文件原始大小（由压缩工作线程更新）
        // totalBytes: 同步过来的总文件大小（由后台扫描任务更新）
        let processedFiles = task.processed_files || 0;
        let totalFiles = task.total_files || 0;
        const processedBytes = task.processed_bytes || 0;
        let totalBytes = task.total_bytes || task.total_bytes_actual || 0;
        const compressedBytes = task.compressed_bytes || 0;
        
        // 修复：如果 processedFiles > totalFiles，说明数据异常，使用 processedFiles 作为 totalFiles
        // 这种情况可能是 total_files 没有正确更新，或者 processed_files 被重复累加
        if (processedFiles > totalFiles && totalFiles > 0) {
            // 如果差异很大（超过10%），说明数据异常，使用 processedFiles 作为上限
            if (processedFiles > totalFiles * 1.1) {
                console.warn(`任务 ${task.task_id || task.id} 数据异常: processedFiles (${processedFiles}) > totalFiles (${totalFiles})，使用 processedFiles 作为 totalFiles`);
                totalFiles = processedFiles;
            } else {
                // 差异不大，可能是 totalFiles 更新滞后，使用 processedFiles 作为上限
                totalFiles = processedFiles;
            }
        }
        
        // 如果 totalFiles 为 0 但 processedFiles > 0，使用 processedFiles 作为 totalFiles
        if (totalFiles === 0 && processedFiles > 0) {
            totalFiles = processedFiles;
        }
        
        // 如果 totalBytes 为 0 但 processedBytes > 0，使用 processedBytes 作为 totalBytes
        if (totalBytes === 0 && processedBytes > 0) {
            totalBytes = processedBytes;
        }
        
        // 优先使用后端返回的 progress_percent（后端已经基于内存统计计算好了）
        // 如果后端没有返回，再自己计算
        let percent = 0;
        if (task.progress_percent !== null && task.progress_percent !== undefined) {
            // 使用后端计算的进度百分比（包括 0 值）
            percent = Math.min(100, Math.max(0, task.progress_percent));
        } else if (totalFiles > 0) {
            // 后端没有返回，自己计算
            percent = Math.min(100, (processedFiles / totalFiles) * 100);
        }
        
        let compressionRatio = task.compression_ratio || 0;
        if ((!compressionRatio || compressionRatio <= 0) && processedBytes > 0 && compressedBytes > 0) {
            compressionRatio = compressedBytes / processedBytes;
        }
        
        return {
            percent: Number(percent.toFixed(1)),
            processedFiles,  // 已经压缩的文件数
            totalFiles,      // 同步过来的总文件数（已修复异常情况）
            processedBytes,   // 已经压缩的文件原始大小
            totalBytes: totalBytes || processedBytes,  // 同步过来的总文件大小（已修复异常情况）
            compressedBytes,
            compressionRatio
        };
    }

    function formatCompressionRatio(value) {
        if (!value || value <= 0) return '-';
        return (value * 100).toFixed(2) + '%';
    }

    function getStageBadgeClass(state, stageCode, progressPercent = null, task = null) {
        switch ((state || '').toLowerCase()) {
            case 'done':
                return 'bg-success';
            case 'current':
                // 对于写入磁带阶段，需要特殊处理
                if (stageCode === 'copy') {
                    // 检查是否正在向磁带移动
                    const operationStatus = (task?.operation_status || '').toLowerCase();
                    const isMoving = operationStatus.includes('写入磁带中') || 
                                     operationStatus.includes('正在写入') ||
                                     operationStatus.includes('向磁带移动');
                    
                    if (isMoving) {
                        // 正在移动时闪烁
                        return 'bg-danger text-white pulse-badge';
                    } else {
                        // 移动完成但任务未完成时，不闪烁
                        return 'bg-danger text-white';
                    }
                }
                
                // 如果有进度信息，根据进度百分比改变颜色
                if (progressPercent !== null && progressPercent >= 0) {
                    // 根据进度百分比设置颜色
                    // >= 80%: 绿色（接近完成）
                    // 50-80%: 黄色（进行中）
                    // < 50%: 蓝色/红色（刚开始）
                    if (progressPercent >= 80) {
                        return 'bg-success pulse-badge'; // 接近完成 - 绿色脉冲
                    } else if (progressPercent >= 50) {
                        return 'bg-warning text-dark pulse-badge'; // 进行中 - 黄色脉冲
                    } else {
                        // 根据阶段类型设置初始颜色
                        switch (stageCode) {
                            case 'scan':
                                return 'bg-info pulse-badge'; // 扫描文件 - 蓝色脉冲
                            case 'compress':
                                return 'bg-warning text-dark pulse-badge'; // 压缩文件 - 黄色脉冲
                            case 'copy':
                                return 'bg-danger text-white pulse-badge'; // 写入磁带 - 红色脉冲
                            default:
                                return 'bg-primary pulse-badge';
                        }
                    }
                }
                // 没有进度信息时，使用原来的逻辑
                switch (stageCode) {
                    case 'scan':
                        return 'bg-info pulse-badge'; // 扫描文件 - 蓝色脉冲
                    case 'compress':
                        return 'bg-warning text-dark pulse-badge'; // 压缩文件 - 黄色脉冲
                    case 'copy':
                        return 'bg-danger text-white pulse-badge'; // 写入磁带 - 红色脉冲
                    default:
                        return 'bg-primary pulse-badge';
                }
            case 'pending':
            default:
                return 'bg-secondary';
        }
    }

    function getStageProgressCircleClass(stageCode) {
        switch ((stageCode || '').toLowerCase()) {
            case 'scan':
                return 'bg-info text-white';
            case 'compress':
                return 'bg-warning text-dark';
            case 'copy':
                return 'bg-danger text-white';
            case 'finalize':
                return 'bg-success text-white';
            default:
                return 'bg-primary text-white';
        }
    }

    function getCompletedStageBadgeClass(state, stageCode, task = null) {
        switch ((state || '').toLowerCase()) {
            case 'done':
                // 完成的阶段根据类型使用不同颜色
                switch (stageCode) {
                    case 'scan':
                        return 'bg-info'; // 扫描完成 - 蓝色
                    case 'compress':
                        return 'bg-warning text-dark'; // 压缩完成 - 黄色
                    case 'copy':
                        // 写入磁带完成：熄灭（不显示高亮），只有进行中才亮起
                        // 如果整个任务完成，显示绿色；否则不显示（已完成但任务未完成）
                        if (task && task.status && task.status.toLowerCase() === 'completed') {
                            return 'bg-success'; // 任务完成时亮起绿色（任务整体完成）
                        }
                        return 'bg-secondary'; // 写入磁带完成但任务未完成 - 熄灭（灰色）
                    case 'finalize':
                        return 'bg-success pulse-badge'; // 最终完成 - 绿色脉冲
                    default:
                        return 'bg-success';
                }
            case 'current':
                // 当前阶段（完成状态下的finalize阶段）
                if (stageCode === 'finalize') {
                    return 'bg-success pulse-badge'; // 最终完成阶段 - 绿色脉冲
                }
                return 'bg-primary pulse-badge';
            case 'pending':
            default:
                return 'bg-secondary';
        }
    }

    function createRunningCard(task) {
        // 验证任务数据
        if (!task) {
            console.error('createRunningCard: task is null or undefined');
            return null;
        }
        
        // 验证必需字段 - task_name 是必需的，但如果没有可以用 task_id 或 id 作为后备
        if (!task.task_name) {
            // 如果没有 task_name，尝试使用 task_id 或 id 作为名称
            if (task.task_id) {
                task.task_name = `任务 #${task.task_id}`;
            } else if (task.id) {
                task.task_name = `任务 #${task.id}`;
            } else {
                console.error('createRunningCard: task missing required fields (task_name, task_id, id):', task);
                return null;
            }
        }
        
        try {
            const cardCol = document.createElement('div');
            cardCol.className = 'col-md-4 col-lg-4 mb-3';

            const card = document.createElement('div');
            card.className = 'service-card';

            const body = document.createElement('div');
            body.className = 'card-body';

            // 修复状态判断：确保正确识别运行中的任务
            const taskStatus = (task.status || '').toLowerCase().trim();
            const isRunning = taskStatus === 'running';
            
            // 添加调试日志（仅对运行中的任务）
            if (taskStatus === 'running' || task.status === 'running') {
                console.log('createRunningCard: 运行中的任务状态判断:', {
                    task_id: task.task_id || task.id,
                    task_name: task.task_name,
                    status: task.status,
                    status_type: typeof task.status,
                    status_lower: taskStatus,
                    isRunning: isRunning
                });
            }
            
            let progressInfo = null;
            if (isRunning) {
                progressInfo = computeProgress(task);
            }

        const header = document.createElement('div');
        header.className = 'd-flex justify-content-between align-items-start mb-2';
        header.style.position = 'relative';

        const title = document.createElement('h6');
        title.className = 'card-title mb-0';
        title.textContent = task.task_name || '未命名任务';
        header.appendChild(title);

        const badgeWrapper = document.createElement('div');
        badgeWrapper.className = 'd-flex align-items-center gap-2';

        if (isRunning && progressInfo) {
            // 计算每小时处理GB数
            // 使用已处理数据/已用时间来计算速度
            const speedGBPerSec = calculateProcessingSpeedGBPerSec(task);
            if (speedGBPerSec !== null && parseFloat(speedGBPerSec) > 0) {
                // 计算每小时处理GB数（G/秒 * 3600秒）
                const speedGBPerHour = parseFloat(speedGBPerSec) * 3600;
                
                // 根据80G基准判断颜色
                // >= 80G: 绿色（bg-success）- 良好
                // < 80G: 黄色（bg-warning）- 较慢
                const badgeClass = speedGBPerHour >= 80 ? 'badge bg-success' : 'badge bg-warning text-dark';
                
                // 在右上角徽章中显示每小时处理GB数（只显示数字，不带单位）
                const speedBadge = document.createElement('span');
                speedBadge.className = badgeClass;
                speedBadge.style.cssText = 'font-size: 0.85rem; font-weight: 600; padding: 0.35em 0.65em;';
                // 格式化数字：如果 >= 1，显示2位小数；如果 < 1，显示更多小数位
                const displayValue = speedGBPerHour >= 1 
                    ? speedGBPerHour.toFixed(2) 
                    : speedGBPerHour.toFixed(4);
                speedBadge.textContent = displayValue;
                speedBadge.title = `每小时处理: ${displayValue} GB\n已处理数据: ${formatBytes(progressInfo.processedBytes)}\n已用时间: ${formatElapsedTime(task.started_at, task.completed_at)}`;

                // 为运行中的任务添加ID属性，便于后续更新
                speedBadge.setAttribute('data-task-speed', task.task_id || task.id);
                badgeWrapper.appendChild(speedBadge);
            } else {
                // 如果无法计算速度，显示状态徽章
                badgeWrapper.innerHTML = buildStatusBadge(task);
            }
        } else {
            // 非运行状态，显示状态徽章
            badgeWrapper.innerHTML = buildStatusBadge(task);
        }

        header.appendChild(badgeWrapper);

        body.appendChild(header);

        const highlightClass = 'fw-semibold text-body-emphasis text-break';
        const sourcePathsLabel = (task.source_paths || []).join(', ') || 'N/A';
        const source = document.createElement('div');
        source.className = 'mb-2';
        source.innerHTML = `<small class="text-muted">源路径:</small><br><span class="${highlightClass}">${sourcePathsLabel}</span>`;
        body.appendChild(source);

        const tape = document.createElement('div');
        tape.className = 'mb-2';
        const target = task.tape_device || task.tape_id || '自动选择';
        tape.innerHTML = `<small class="text-muted">目标:</small><br><span class="${highlightClass}">${target}</span>`;
        body.appendChild(tape);

        // 优先使用后端返回的 operation_stage 和 stage_steps（这些已经是基于内存变量构建的）
        const operationStage = (task.operation_stage || '').toLowerCase();
        const isCompleted = (task.status || '').toLowerCase() === 'completed';
        
        // 定义阶段顺序和映射
        const stageOrder = ['scan', 'prefetch', 'compress', 'copy', 'finalize'];
        const stageLabels = {
            'scan': '扫描文件',
            'prefetch': '预分组',
            'compress': '压缩/打包',
            'copy': '写入磁带',
            'finalize': '完成'
        };
        
        // 构建阶段步骤，优先使用后端返回的 stage_steps（基于内存变量）
        let stageSteps = [];
        
        // 如果后端已经提供了 stage_steps，优先使用后端的（这些是基于内存变量构建的）
        // 后端的 stage_steps 已经根据内存变量动态更新了标签（如"预分组中"、"写入磁带中"等）
        if (Array.isArray(task.stage_steps) && task.stage_steps.length > 0) {
            // 转换后端的 status 字段为前端的 state 字段
            stageSteps = task.stage_steps.map(step => {
                // 后端使用 status: "completed"/"active"/"pending"
                // 前端使用 state: "done"/"current"/"pending"
                let state = step.state || step.status || 'pending';
                if (state === 'completed') state = 'done';
                if (state === 'active') state = 'current';
                
                // 优先使用后端返回的 label（基于内存变量动态构建，如"预分组中"、"分组完成"、"写入磁带中"等）
                let label = step.label || stageLabels[step.code] || step.code;
                
                // 特殊处理：如果 prefetch 阶段的状态是 completed/done，确保显示"分组完成"
                if (step.code === 'prefetch' && (state === 'done' || step.status === 'completed')) {
                    // 如果后端返回的 label 不是"分组完成"，则更新为"分组完成"
                    if (!label.includes('分组完成') && !label.includes('完成')) {
                        label = '分组完成';
                    }
                }
                
                return {
                    code: step.code,
                    label: label,
                    state: state
                };
            });
            // 使用后端返回的 stage_steps，不再需要自己构建
            // 这些步骤已经根据内存变量（scan_status, operation_stage等）动态更新
        }
        
        // 如果后端没有提供 stage_steps 或为空，根据 operation_stage 动态构建（回退方案）
        if (stageSteps.length === 0 && operationStage) {
            if (isCompleted) {
                // 完成状态：所有阶段都是 done，finalize 是 current
                stageOrder.forEach(code => {
                    let label = stageLabels[code] || code;
                    // 完成状态下的特殊标签
                    if (code === 'scan' && isScanCompleted) {
                        label = '扫描完成';
                    } else if (code === 'prefetch') {
                        label = '分组完成';
                    }
                    stageSteps.push({
                        code: code,
                        label: label,
                        state: code === 'finalize' ? 'current' : 'done'
                    });
                });
            } else {
                // 运行中：根据当前阶段设置状态
                const currentIndex = stageOrder.indexOf(operationStage);
                if (currentIndex >= 0) {
                    stageOrder.forEach((code, index) => {
                        let state = 'pending';
                        if (index < currentIndex) {
                            state = 'done';  // 已完成的阶段
                        } else if (index === currentIndex) {
                            state = 'current';  // 当前阶段
                        } else {
                            state = 'pending';  // 未开始的阶段
                        }
                        
                        let label = stageLabels[code] || code;
                        // 根据状态调整标签（优先使用后端返回的 operation_stage_label）
                        if (code === operationStage && task.operation_stage_label) {
                            // 如果当前阶段有后端返回的标签（基于内存变量），优先使用
                            label = task.operation_stage_label;
                        } else if (code === 'scan' && isScanCompleted) {
                            label = '扫描完成';
                        } else if (code === 'prefetch') {
                            // 预分组：根据当前阶段和状态动态显示
                            if (index < currentIndex) {
                                // 预分组已完成（已进入下一阶段），显示"分组完成"
                                label = '分组完成';
                            } else if (code === operationStage) {
                                // 当前正在预分组，检查 operation_status 判断是否完成
                                const operationStatus = (task.operation_status || '').toLowerCase();
                                if (operationStatus.includes('分组完成')) {
                                    label = '分组完成';
                                } else {
                                    label = '预分组中';
                                }
                            } else if (isScanCompleted && currentIndex > index) {
                                // 扫描完成且预分组已完成，显示"分组完成"
                                label = '分组完成';
                            } else {
                                label = '预分组';
                            }
                        } else if (code === 'copy') {
                            // 写入磁带：根据当前阶段动态显示
                            if (code === operationStage) {
                                // 当前正在写入磁带，显示"写入磁带中"
                                label = '写入磁带中';
                            }
                        }
                        
                        stageSteps.push({
                            code: code,
                            label: label,
                            state: state
                        });
                    });
                }
            }
        }
        
        // 如果仍然没有 stage_steps，使用默认的（所有都是 pending）
        if (stageSteps.length === 0) {
            stageOrder.forEach(code => {
                stageSteps.push({
                    code: code,
                    label: stageLabels[code] || code,
                    state: 'pending'
                });
            });
        }
        
        if (stageSteps.length) {
            // 检查任务是否完成，如果是完成状态，确保最终阶段徽章高亮
            if (isCompleted) {
                // 生成完成状态信息
                const completedLabel = '🎉 备份完成';
                const currentStageLabel = completedLabel;

                const stageSection = document.createElement('div');
                stageSection.className = 'mb-2';
                stageSection.innerHTML = `
                    <div class="d-flex justify-content-between align-items-center">
                        <small class="text-muted">状态:</small>
                        <small class="text-success"><strong>${currentStageLabel}</strong></small>
                    </div>
                    <div class="d-flex flex-wrap gap-1 mt-1">
                        ${stageSteps.map(step => {
                            // 特殊处理：finalize 阶段正在写入磁带时，copy 阶段应该亮起
                            let badgeClass = getCompletedStageBadgeClass(step.state, step.code, task);
                            if (step.code === 'copy' && operationStage === 'finalize') {
                                // 检查是否正在写入磁带
                                const operationStatus = (task.operation_status || '').toLowerCase();
                                const isWritingToTape = operationStatus.includes('写入') || 
                                                       operationStatus.includes('复制') ||
                                                       operationStatus.includes('向磁带');
                                
                                if (isWritingToTape) {
                                    // 正在写入磁带，copy 阶段亮起（红色脉冲）
                                    badgeClass = 'bg-danger text-white pulse-badge';
                                } else {
                                    // 写入完成，copy 阶段熄灭（灰色）
                                    badgeClass = 'bg-secondary';
                                }
                            }
                            return `<span class="badge ${badgeClass}">${step.label}</span>`;
                        }).join('')}
                    </div>
                `;
                body.appendChild(stageSection);
            } else {
                const isCompressStage = (task.operation_stage || '').toLowerCase() === 'compress';

                // 优先：如果是压缩阶段且后端提供了 current_compression_progress，直接用它构造文案
                // 避免依赖后端 description 是否及时更新，保证"当前阶段"能实时显示压缩进度
                let currentStageLabel = null;
                if (isCompressStage && task.current_compression_progress) {
                    console.log('[压缩进度] 前端获取到压缩进度数据:', task.current_compression_progress);
                    const compProg = task.current_compression_progress;
                    // 优先使用 current 和 total（兼容字段），如果没有则使用 current_file_index 和 total_files_in_group
                    const current = compProg.current !== undefined ? compProg.current : (compProg.current_file_index || 0);
                    const total = compProg.total !== undefined ? compProg.total : (compProg.total_files_in_group || 0);
                    let percent = typeof compProg.percent === 'number'
                        ? compProg.percent
                        : (total > 0 ? (current / total * 100) : 0);
                    // 保证百分比是有限数值
                    if (!Number.isFinite(percent)) {
                        percent = 0;
                    }
                    // 如果 current 和 total 都是 0，显示百分比即可
                    if (current === 0 && total === 0) {
                        currentStageLabel = `压缩文件中 (${percent.toFixed(1)}%)`;
                    } else {
                        currentStageLabel = `压缩文件中 ${current}/${total} 个文件 (${percent.toFixed(1)}%)`;
                    }
                    console.log('[压缩进度] 构造的标签:', currentStageLabel);
                } else {
                    console.log('[压缩进度] 压缩阶段但没有进度数据 - isCompressStage:', isCompressStage, 'current_compression_progress:', task.current_compression_progress);
                }

                // 退回：如果不是压缩阶段，或者没有进度信息，仍然使用后端提供的 operation_status / 阶段标签
                // 但是过滤掉"初始化压缩引擎"这样的初始状态，显示更有意义的信息
                if (!currentStageLabel) {
                    let operationStatus = task.operation_status
                        || stageSteps.find(step => step.state === 'current')?.label
                        || '-';

                    // 如果操作状态包含"初始化压缩引擎"且是压缩阶段，显示默认压缩状态
                    if (isCompressStage && operationStatus.includes('初始化压缩引擎')) {
                        operationStatus = '压缩文件中...';
                    }

                    currentStageLabel = operationStatus;
                }

                // 从 currentStageLabel 中解析进度百分比
                // 格式示例: "压缩文件中 1201/3395 个文件 (35.4%)"
                let progressPercent = null;
                const progressMatch = currentStageLabel.match(/\(([\d.]+)%\)/);
                if (progressMatch) {
                    progressPercent = parseFloat(progressMatch[1]);
                } else if (progressInfo && progressInfo.percent) {
                    // 如果没有从标签中解析到，使用 progressInfo 中的百分比
                    progressPercent = progressInfo.percent;
                }
                
                // 构建显示文本，添加大小信息
                let displayLabel = currentStageLabel;
                
                // 如果是压缩阶段，必须使用当前文件组的总容量（压缩前）
                // 501/20362 个文件 (2.5%) 中的 103.22G 应该是当前文件组的总文件大小，不是整个任务的总大小
                if (isCompressStage && task.current_compression_progress) {
                    const compProg = task.current_compression_progress;
                    // 优先使用 group_size_bytes（当前文件组的总文件大小）
                    if (compProg.group_size_bytes && compProg.group_size_bytes > 0) {
                        const sizeGB = (compProg.group_size_bytes / (1024 * 1024 * 1024)).toFixed(2);
                        // 在文件数量和百分比后添加大小信息（当前文件组的总容量）
                        displayLabel = currentStageLabel.replace(/(\([\d.]+%\))/, `$1 ${sizeGB}G`);
                    }
                } else if (!isCompressStage && progressInfo && (progressInfo.processedBytes > 0 || progressInfo.compressedBytes > 0)) {
                    // 非压缩阶段，使用累计数据
                    const sizeBytes = progressInfo.processedBytes || progressInfo.compressedBytes;
                    if (sizeBytes > 0) {
                        const sizeGB = (sizeBytes / (1024 * 1024 * 1024)).toFixed(2);
                        // 在文件数量和百分比后添加大小信息
                        displayLabel = currentStageLabel.replace(/(\([\d.]+%\))/, `$1 ${sizeGB}G`);
                    }
                }
                
                const stageSection = document.createElement('div');
                stageSection.className = 'mb-2';
                stageSection.innerHTML = `
                    <div class="d-flex justify-content-between align-items-center">
                        <small class="text-muted">当前阶段:</small>
                        <small class="text-muted">${displayLabel}</small>
                    </div>
                    <div class="d-flex flex-wrap gap-1 mt-1">
                        ${stageSteps.map(step => {
                            // 如果是当前阶段且有进度信息，传递进度百分比
                            const progress = (step.state === 'current' && progressPercent !== null) ? progressPercent : null;
                            
                            // 特殊处理：finalize 阶段正在写入磁带时，copy 阶段应该亮起
                            let badgeClass = getStageBadgeClass(step.state, step.code, progress, task);
                            if (step.code === 'copy' && operationStage === 'finalize') {
                                // 检查是否正在写入磁带
                                const operationStatus = (task.operation_status || '').toLowerCase();
                                const isWritingToTape = operationStatus.includes('写入') || 
                                                       operationStatus.includes('复制') ||
                                                       operationStatus.includes('向磁带');
                                
                                if (isWritingToTape) {
                                    // 正在写入磁带，copy 阶段亮起（红色脉冲）
                                    badgeClass = 'bg-danger text-white pulse-badge';
                                } else {
                                    // 写入完成，copy 阶段熄灭（灰色）
                                    badgeClass = 'bg-secondary';
                                }
                            }
                            
                            return `<span class="badge ${badgeClass}">${step.label}</span>`;
                        }).join('')}
                    </div>
                `;
                body.appendChild(stageSection);
            }
        }

        if (isRunning && progressInfo) {
            const progressSection = document.createElement('div');
            progressSection.className = 'mb-2';
            
            // 压缩阶段：始终显示"各任务"行
            let batchProgressHtml = '';
            if (task.operation_stage === 'compress') {
                let taskProgressText = '';
                let hasValidProgress = false;
                
                if (task.current_compression_progress) {
                    const compProg = task.current_compression_progress;
                    console.log('[各压缩任务进度] compProg:', compProg);
                    
                    // 优先显示各压缩任务的进度百分比
                    if (compProg.task_progress_list && Array.isArray(compProg.task_progress_list) && compProg.task_progress_list.length > 0) {
                        // 过滤出有效的进度数据（total > 0 或 percent > 0）
                        const validProgressList = compProg.task_progress_list.filter(taskProg => {
                            const total = taskProg.total || 0;
                            const percent = taskProg.percent || 0;
                            return total > 0 || percent > 0;
                        });
                        
                        if (validProgressList.length > 0) {
                            // 按文件大小显示各任务的进度：文件总数（百分比%）
                            taskProgressText = validProgressList.map(taskProg => {
                                const total = taskProg.total || 0;
                                const percent = taskProg.percent || 0;
                                return `${total}（${percent.toFixed(0)}%）`;
                            }).join('   ');
                            hasValidProgress = true;
                            console.log('[各压缩任务进度] 显示任务进度:', taskProgressText);
                        }
                    }
                    
                    // 如果没有有效的 task_progress_list，但有 running_count，显示任务数量
                    if (!hasValidProgress && compProg.running_count && compProg.running_count > 0) {
                        taskProgressText = `${compProg.running_count} 个任务运行中`;
                        hasValidProgress = true;
                        console.log('[各压缩任务进度] 没有task_progress_list，显示任务数量:', compProg.running_count);
                    }
                }
                
                // 无论是否有进度信息，都显示"各任务"行
                // 只有当有有效进度数据时才显示进度，否则显示 "-"
                batchProgressHtml = `
                    <div class="d-flex justify-content-between align-items-center mb-1">
                        <small class="text-muted">各任务:</small>
                        <small class="text-muted fw-semibold">${hasValidProgress ? taskProgressText : '-'}</small>
                    </div>
                `;
            }
            
            progressSection.innerHTML = `
                ${batchProgressHtml}
                <div class="d-flex justify-content-between align-items-center">
                    <small class="text-muted">进度:</small>
                    <small class="text-muted">${progressInfo.percent}%</small>
                </div>
                <div class="progress" style="height:6px;">
                    <div class="progress-bar bg-primary" role="progressbar" style="width:${progressInfo.percent}%"></div>
                </div>
                <div class="d-flex justify-content-between">
                    <small class="text-muted">文件进度:</small>
                    <small class="text-muted">${progressInfo.processedFiles}/${progressInfo.totalFiles || progressInfo.processedFiles}</small>
                </div>
                <div class="d-flex justify-content-between">
                    <small class="text-muted">已处理数据:</small>
                    <small class="text-muted">${formatBytes(progressInfo.processedBytes)} / ${formatBytes(progressInfo.totalBytes || progressInfo.processedBytes)}</small>
                </div>
                <div class="d-flex justify-content-between">
                    <small class="text-muted">压缩后大小:</small>
                    <small class="text-muted">${formatBytes(progressInfo.compressedBytes)}</small>
                </div>
                <div class="d-flex justify-content-between">
                    <small class="text-muted">压缩率:</small>
                    <small class="text-muted">${formatCompressionRatio(progressInfo.compressionRatio)}</small>
                </div>
            `;
            body.appendChild(progressSection);
        }

        const meta = document.createElement('div');
        meta.className = 'd-flex justify-content-between';
        meta.innerHTML = `<small class="text-muted">开始时间:</small><small class="text-muted">${formatDateTime(task.started_at)}</small>`;
        body.appendChild(meta);

        const elapsed = document.createElement('div');
        elapsed.className = 'd-flex justify-content-between';
        elapsed.innerHTML = `<small class="text-muted">已用时间:</small><small class="text-muted">${formatElapsedTime(task.started_at, task.completed_at)}</small>`;
        body.appendChild(elapsed);

        if (task.error_message) {
            const error = document.createElement('div');
            error.className = 'mt-2';
            error.innerHTML = `<small class="text-danger"><i class="bi bi-exclamation-triangle me-1"></i>${task.error_message}</small>`;
            body.appendChild(error);
        }

            card.appendChild(body);
            cardCol.appendChild(card);
            return cardCol;
        } catch (error) {
            console.error('createRunningCard: Error creating card:', error);
            console.error('createRunningCard: Task data:', task);
            console.error('createRunningCard: Error stack:', error.stack);
            return null;
        }
    }

    async function loadRunningTasks() {
        if (!dom.runningList) {
            console.warn('loadRunningTasks: runningList element not found');
            return;
        }
        try {
            const [running, failed] = await Promise.all([
                fetchJSON('/api/backup/tasks?status=running&limit=10'),
                fetchJSON('/api/backup/tasks?status=failed&limit=5'),
            ]);
            
            // 调试信息 - 使用 console.log 确保在浏览器控制台可见
            console.log('loadRunningTasks: running tasks:', running);
            console.log('loadRunningTasks: failed tasks:', failed);
            console.log('loadRunningTasks: running type:', typeof running, 'isArray:', Array.isArray(running));
            console.log('loadRunningTasks: failed type:', typeof failed, 'isArray:', Array.isArray(failed));
            
            // 检查运行中的任务
            if (Array.isArray(running) && running.length > 0) {
                console.log('loadRunningTasks: running列表中的任务:', running.map(t => ({
                    id: t.task_id || t.id,
                    name: t.task_name,
                    status: t.status,
                    status_type: typeof t.status
                })));
            }
            
            // 清空容器（但保留已完成任务的卡片）
            const existingCards = Array.from(dom.runningList.children);
            const existingTaskIds = new Set();
            existingCards.forEach(card => {
                // 检查卡片本身或卡片内的元素是否有任务ID
                const taskIdAttr = card.getAttribute('data-task-id') ||
                                   card.querySelector('[data-task-id]')?.getAttribute('data-task-id') ||
                                   card.querySelector('[data-task-speed]')?.getAttribute('data-task-speed');
                if (taskIdAttr) {
                    existingTaskIds.add(parseInt(taskIdAttr, 10));
                }
            });
            
            // 清空容器，但稍后会重新添加需要显示的任务
            dom.runningList.innerHTML = '';
            const tasks = [];
            
            // 确保 running 是数组
            if (Array.isArray(running)) {
                tasks.push(...running);
            } else if (running) {
                console.warn('loadRunningTasks: running is not an array:', running);
                if (typeof running === 'object') {
                    tasks.push(running);
                }
            }
            
            const now = Date.now();
            // 确保 failed 是数组
            if (Array.isArray(failed)) {
                failed.forEach(task => {
                    const completed = task.completed_at ? new Date(task.completed_at).getTime() : 0;
                    if (completed && now - completed <= 10 * 60 * 1000) {
                        tasks.push(task);
                    }
                });
            } else if (failed && typeof failed === 'object') {
                console.warn('loadRunningTasks: failed is not an array:', failed);
                const completed = failed.completed_at ? new Date(failed.completed_at).getTime() : 0;
                if (completed && now - completed <= 10 * 60 * 1000) {
                    tasks.push(failed);
                }
            }
            
            // 检查任务状态，标记已完成的任务
            tasks.forEach(task => {
                const taskId = task.task_id || task.id;
                const taskStatus = (task.status || '').toLowerCase();
                if (taskStatus === 'completed' && taskId) {
                    completedTaskIds.add(taskId);
                }
            });
            
            console.log('loadRunningTasks: total tasks to display:', tasks.length);
            console.log('loadRunningTasks: tasks data:', tasks);
            console.log('loadRunningTasks: completed task IDs:', Array.from(completedTaskIds));
            
            if (tasks.length === 0) {
                // 如果没有新任务，但有待显示已完成的任务卡片，保留它们
                if (existingCards.length > 0) {
                    existingCards.forEach(card => {
                        const taskIdAttr = card.getAttribute('data-task-id') ||
                                           card.querySelector('[data-task-id]')?.getAttribute('data-task-id') ||
                                           card.querySelector('[data-task-speed]')?.getAttribute('data-task-speed');
                        if (taskIdAttr) {
                            const taskId = parseInt(taskIdAttr, 10);
                            // 只保留已完成的任务卡片
                            if (completedTaskIds.has(taskId)) {
                                dom.runningList.appendChild(card);
                            }
                        }
                    });
                    if (dom.runningList.children.length === 0) {
                        dom.runningList.innerHTML = '<div class="col-12"><p class="text-muted">暂无运行中的任务和最近失败的任务</p></div>';
                    }
                } else {
                    dom.runningList.innerHTML = '<div class="col-12"><p class="text-muted">暂无运行中的任务和最近失败的任务</p></div>';
                }
            } else {
                // 验证并创建/更新卡片
                let cardsCreated = 0;
                const processedTaskIds = new Set();
                
                tasks.forEach((task, index) => {
                    try {
                        // 验证任务数据是否完整
                        if (!task) {
                            console.warn(`loadRunningTasks: 任务 ${index} 为空`);
                            return;
                        }
                        
                        // 验证必需字段 - 确保至少有一个标识符
                        if (!task.task_name && !task.task_id && !task.id) {
                            console.warn(`loadRunningTasks: 任务 ${index} 缺少必需字段:`, task);
                            return;
                        }
                        
                        const taskId = task.task_id || task.id;
                        const taskStatus = (task.status || '').toLowerCase();
                        
                        // 如果任务已完成且已经在 completedTaskIds 中，且已有卡片存在，则跳过刷新
                        if (taskStatus === 'completed' && taskId && completedTaskIds.has(taskId)) {
                            // 查找是否已有该任务的卡片
                            const existingCard = existingCards.find(card => {
                                const cardTaskId = card.getAttribute('data-task-id') ||
                                                  card.querySelector('[data-task-id]')?.getAttribute('data-task-id') ||
                                                  card.querySelector('[data-task-speed]')?.getAttribute('data-task-speed');
                                return cardTaskId && parseInt(cardTaskId, 10) === taskId;
                            });
                            
                            if (existingCard) {
                                // 保留现有卡片，不刷新
                                dom.runningList.appendChild(existingCard);
                                processedTaskIds.add(taskId);
                                return;
                            }
                        }
                        
                        // 如果没有 task_name，尝试使用 task_id 或 id 作为名称
                        if (!task.task_name) {
                            if (task.task_id) {
                                task.task_name = `任务 #${task.task_id}`;
                            } else if (task.id) {
                                task.task_name = `任务 #${task.id}`;
                            }
                        }
                        
                        // 使用 createRunningCard 创建卡片元素
                        const card = createRunningCard(task);
                        if (card && card.nodeType === 1) { // 检查是否是有效的DOM元素
                            // 为卡片添加 data-task-id 属性，便于后续识别
                            if (taskId) {
                                card.setAttribute('data-task-id', taskId);
                            }
                            dom.runningList.appendChild(card);
                            cardsCreated++;
                            processedTaskIds.add(taskId);
                        } else {
                            console.error('loadRunningTasks: createRunningCard returned invalid element for task:', task);
                            console.error('loadRunningTasks: card value:', card);
                        }
                    } catch (cardError) {
                        console.error('loadRunningTasks: Error creating card for task:', task);
                        console.error('loadRunningTasks: Error details:', cardError);
                        console.error('loadRunningTasks: Error stack:', cardError.stack);
                    }
                });
                
                // 保留其他已完成任务的卡片（如果它们不在当前任务列表中）
                existingCards.forEach(card => {
                    const taskIdAttr = card.getAttribute('data-task-id') ||
                                       card.querySelector('[data-task-id]')?.getAttribute('data-task-id') ||
                                       card.querySelector('[data-task-speed]')?.getAttribute('data-task-speed');
                    if (taskIdAttr) {
                        const taskId = parseInt(taskIdAttr, 10);
                        // 如果该任务已完成且不在当前处理的任务列表中，保留其卡片
                        if (completedTaskIds.has(taskId) && !processedTaskIds.has(taskId)) {
                            dom.runningList.appendChild(card);
                        }
                    }
                });
                
                // 如果没有创建任何卡片，显示提示
                if (cardsCreated === 0 && tasks.length > 0 && dom.runningList.children.length === 0) {
                    console.error('loadRunningTasks: 有任务但无法创建卡片，任务数据:', tasks);
                    dom.runningList.innerHTML = '<div class="col-12"><p class="text-warning">无法生成任务卡片，请检查控制台错误信息</p></div>';
                }
            }
            if (dom.runningTasksCounter) dom.runningTasksCounter.textContent = tasks.length;
        } catch (error) {
            console.error('加载运行中的任务失败:', error);
            console.error('错误堆栈:', error.stack);
            dom.runningList.innerHTML = '<div class="col-12"><p class="text-danger">加载失败: ' + (error.message || '未知错误') + '</p></div>';
        }
    }

    function buildTaskNameCell(task) {
        let badges = '';

        // 模板任务标识
        if (task.is_template) {
            badges += ' <span class="badge bg-secondary">模板</span>';
        }

        // 计划任务标识
        if (task.from_scheduler) {
            badges += ' <span class="badge bg-info text-dark">计划</span>';
        }

        // 任务状态特殊说明
        if (task.is_template && task.status === 'pending') {
            // 使用更协调的样式：浅灰色背景，深灰色文字
            badges += ' <span class="badge bg-secondary bg-opacity-25 text-dark">等待执行</span>';
        }

        return `<strong>${task.task_name || '未命名任务'}</strong>${badges}`;
    }

    function formatTaskType(type) {
        const map = {
            full: '完整备份',
            incremental: '增量备份',
            differential: '差异备份',
            monthly_full: '月度备份',
        };
        const label = map[type] || type || 'unknown';
        return `<span class="badge bg-info">${label}</span>`;
    }

    function buildTableRow(task) {
        const sourcePaths = Array.isArray(task.source_paths)
            ? task.source_paths.join(', ')
            : (task.source_paths || (task.is_template ? '计划任务' : 'N/A'));

        // 为模板任务提供更友好的数据显示
        let startTimeDisplay = formatDateTime(task.started_at);
        let completedTimeDisplay = formatDateTime(task.completed_at);
        let dataSizeDisplay = '';

        if (task.is_template) {
            // 模板任务的特殊显示
            startTimeDisplay = task.from_scheduler ? '计划执行' : '-';
            completedTimeDisplay = '-';
            dataSizeDisplay = task.from_scheduler ? '待执行' : '模板配置';
        } else {
            // 执行记录的正常显示
            const processedBytes = task.processed_bytes || 0;
            const totalBytes = task.total_bytes || 0;
            dataSizeDisplay = `${formatBytes(processedBytes)} / ${formatBytes(totalBytes)}`;

            // 如果已开始但处理数据为0，显示说明
            if (task.started_at && processedBytes === 0 && totalBytes === 0) {
                dataSizeDisplay = '准备中...';
            }
        }

        // 模板任务且状态为pending时，使用更协调的样式
        let rowClass = '';
        if (task.is_template) {
            // 所有模板任务都使用浅灰色背景，更柔和协调
            rowClass = 'table-light';
        }
        
        return `
            <tr class="${rowClass}">
                <td>${buildTaskNameCell(task)}</td>
                <td>${formatTaskType(task.task_type || 'full')}</td>
                <td><code class="text-truncate d-inline-block" style="max-width:200px;" title="${sourcePaths}">${sourcePaths}</code></td>
                <td>${buildStatusBadge(task)}</td>
                <td>${startTimeDisplay}</td>
                <td>${completedTimeDisplay}</td>
                <td>${dataSizeDisplay}</td>
                <td>
                    <div class="btn-group btn-group-sm" data-task-id="${task.task_id || task.id}" data-scheduler-task-id="${task.scheduler_task_id || task.task_id || task.id}" data-from-scheduler="${task.from_scheduler ? 'true' : 'false'}" data-enabled="${task.enabled !== false ? 'true' : 'false'}">
                        ${task.from_scheduler ? `
                            ${task.enabled === false ? `
                                <button class="btn btn-outline-success btn-action-enable" title="启用"><i class="bi bi-play"></i></button>
                            ` : `
                                <button class="btn btn-outline-warning btn-action-disable" title="禁用"><i class="bi bi-pause"></i></button>
                            `}
                            <button class="btn btn-outline-info btn-action-run" title="立即运行"><i class="bi bi-play-circle"></i></button>
                            <button class="btn btn-outline-secondary btn-action-unlock" title="解锁"><i class="bi bi-unlock"></i></button>
                        ` : `
                            <button class="btn btn-outline-warning" title="禁用" disabled><i class="bi bi-pause"></i></button>
                            <button class="btn btn-outline-info" title="立即运行" disabled><i class="bi bi-play-circle"></i></button>
                        `}
                        <button class="btn btn-outline-primary btn-action-edit" title="编辑"><i class="bi bi-pencil"></i></button>
                        <button class="btn btn-outline-danger btn-action-delete" title="删除"><i class="bi bi-trash"></i></button>
                    </div>
                </td>
            </tr>
        `;
    }

    async function loadAllTasks() {
        if (!dom.allTasksTable) return;
        try {
            let url = '/api/backup/tasks?limit=100&offset=0';
            const statusValue = dom.statusFilter ? dom.statusFilter.value : '';
            const typeValue = dom.typeFilter ? dom.typeFilter.value : '';
            const searchValue = dom.searchInput ? dom.searchInput.value.trim() : '';
            if (statusValue) url += `&status=${encodeURIComponent(statusValue)}`;
            if (typeValue) url += `&task_type=${encodeURIComponent(typeValue)}`;
            if (searchValue) url += `&q=${encodeURIComponent(searchValue)}`;
            const tasks = await fetchJSON(url);
            
            // 添加调试日志
            console.log('loadAllTasks: 获取到的任务数量:', tasks ? tasks.length : 0);
            if (tasks && tasks.length > 0) {
                console.log('loadAllTasks: 第一个任务:', tasks[0]);
                console.log('loadAllTasks: 第一个任务的状态:', tasks[0].status);
                // 检查所有任务的状态
                const runningTasks = tasks.filter(t => (t.status || '').toLowerCase() === 'running');
                const pendingTasks = tasks.filter(t => (t.status || '').toLowerCase() === 'pending');
                console.log('loadAllTasks: 任务状态统计:', {
                    total: tasks.length,
                    running: runningTasks.length,
                    pending: pendingTasks.length,
                    running_tasks: runningTasks.map(t => ({id: t.task_id || t.id, name: t.task_name, status: t.status})),
                    pending_tasks: pendingTasks.slice(0, 3).map(t => ({id: t.task_id || t.id, name: t.task_name, status: t.status}))
                });
            }
            
            if (!tasks || tasks.length === 0) {
                dom.allTasksTable.innerHTML = '<tr><td colspan="8" class="text-center text-muted">暂无任务</td></tr>';
                return;
            }
            dom.allTasksTable.innerHTML = tasks.map(buildTableRow).join('');
        } catch (error) {
            console.error('加载所有任务失败:', error);
            dom.allTasksTable.innerHTML = `<tr><td colspan="8" class="text-center text-danger">加载失败: ${error.message}</td></tr>`;
        }
    }

    async function toggleSchedulerTask(taskId, enable) {
        const action = enable ? 'enable' : 'disable';
        await fetchJSON(`/api/scheduler/tasks/${taskId}/${action}`, { method: 'POST' });
        alert(enable ? '任务已启用' : '任务已禁用');
        loadAllTasks();
    }

    async function runSchedulerTask(taskId) {
        // 显示确认对话框并直接调用 API
        // 注意：这个函数只在不使用 SchedulerManager.runTask 时调用
        // 此函数不应该被调用，因为所有计划任务都应该通过 SchedulerManager.runTask 处理
        console.warn('[backup.js] runSchedulerTask 被调用，这不应该发生。taskId:', taskId);
        if (!confirm('确定要立即运行此计划任务吗？')) return;
        
        try {
            const result = await fetchJSON(`/api/scheduler/tasks/${taskId}/run`, { method: 'POST' });
            // 检查返回的 success 字段
            if (result && result.success === false) {
                const errorMsg = result.message || result.detail || '任务运行失败';
                alert('运行任务失败: ' + errorMsg);
                return;
            }
            alert('任务已提交运行');
            loadRunningTasks();
            loadAllTasks();
        } catch (error) {
            console.error('运行任务失败:', error);
            const errorMsg = error.message || '未知错误';
            alert('运行任务失败: ' + errorMsg);
        }
    }

    async function unlockSchedulerTask(taskId) {
        if (!confirm('确定要解锁此任务吗？这将释放任务锁并重置状态。')) {
            return;
        }
        try {
            await fetchJSON(`/api/scheduler/tasks/${taskId}/unlock`, { method: 'POST' });
            alert('任务已解锁');
            // 重新加载任务列表
            if (typeof loadRunningTasks === 'function') {
                loadRunningTasks();
            }
        } catch (error) {
            console.error('解锁任务失败:', error);
            alert('解锁任务失败: ' + (error.message || '未知错误'));
        }
    }

    async function deleteBackupTask(taskId, fromScheduler) {
        if (!confirm('确定要删除此任务吗？此操作不可恢复。')) return;
        if (fromScheduler) {
            await fetchJSON(`/api/scheduler/tasks/${taskId}`, { method: 'DELETE' });
        } else {
            await fetchJSON(`/api/backup/tasks/${taskId}`, { method: 'DELETE' });
        }
        alert('任务已删除');
        loadAllTasks();
        loadRunningTasks();
    }

    function editBackupTask(taskId, fromScheduler) {
        if (!window.SchedulerManager) {
            alert('计划任务管理模块未加载，请刷新页面后重试');
            return;
        }
        if (fromScheduler) {
            window.SchedulerManager.editTask(taskId);
        } else {
            window.SchedulerManager.loadTemplateAsTask(taskId);
        }
    }

    function bindFilterEvents() {
        if (dom.statusFilter) {
            dom.statusFilter.addEventListener('change', loadAllTasks);
        }
        if (dom.typeFilter) {
            dom.typeFilter.addEventListener('change', loadAllTasks);
        }
        if (dom.searchInput) {
            dom.searchInput.addEventListener('keypress', (e) => {
                if (e.key === 'Enter') loadAllTasks();
            });
        }
        if (dom.searchBtn) {
            dom.searchBtn.addEventListener('click', loadAllTasks);
        }
        setInterval(() => {
            loadBackupStatistics();
            loadAllTasks();
        }, REFRESH_INTERVAL);
    }

    function bindTableActions() {
        document.addEventListener('click', (event) => {
            const button = event.target.closest('button');
            if (!button) return;

            // 排除计划任务表格中的按钮（由 scheduler.js 处理）
            const scheduledTasksTableBody = document.getElementById('scheduledTasksTableBody');
            if (scheduledTasksTableBody && scheduledTasksTableBody.contains(button)) {
                return; // 计划任务表格中的按钮由 scheduler.js 处理，这里不处理
            }

            // 处理btn-action-*类型的按钮（仅处理备份任务表格中的按钮）
            // 确保按钮在 allTasksTable 中，且不在 scheduledTasksTableBody 中
            if (button.classList.contains('btn-action-run')) {
                // 双重检查：确保按钮在 allTasksTable 中
                const allTasksTable = document.getElementById('allTasksTable');
                if (!allTasksTable || !allTasksTable.contains(button)) {
                    return; // 不在 allTasksTable 中，不处理
                }
                
                event.preventDefault();
                event.stopPropagation();
                event.stopImmediatePropagation(); // 阻止其他监听器处理

                // 尝试从不同位置获取taskId
                const group = button.closest('.btn-group');
                const taskId = group ?
                    parseInt(group.dataset.taskId, 10) :
                    parseInt(button.dataset.taskId, 10);

                if (taskId && !isNaN(taskId)) {
                    // 检查是否来自计划任务（from_scheduler）
                    const fromScheduler = group && group.dataset.fromScheduler === 'true';
                    
                    // 所有计划任务都应该通过 SchedulerManager.runTask 处理
                    // 如果 SchedulerManager 不存在，说明模块未加载，显示错误
                    if (fromScheduler) {
                        if (window.SchedulerManager && typeof window.SchedulerManager.runTask === 'function') {
                            // 使用 SchedulerManager.runTask（包含确认对话框）
                            window.SchedulerManager.runTask(taskId).then(() => {
                                loadRunningTasks();
                                loadAllTasks();
                            }).catch(err => {
                                console.error('运行任务失败:', err);
                                alert('运行任务失败: ' + (err.message || '未知错误'));
                            });
                        } else {
                            console.error('[backup.js] SchedulerManager 不存在或 runTask 方法不可用');
                            alert('计划任务管理器未加载，请刷新页面后重试');
                        }
                    } else {
                        // 非计划任务（模板任务），不应该有"立即运行"按钮，但为了兼容性保留
                        console.warn('[backup.js] 非计划任务尝试运行，taskId:', taskId);
                        runSchedulerTask(taskId).catch(err => alert(err.message));
                    }
                }
                return;
            }

            // 其他备份页面特定的按钮
            const group = button.closest('.btn-group');
            if (!group || !dom.allTasksTable.contains(group)) return;

            const taskId = parseInt(group.dataset.taskId, 10);
            const fromScheduler = group.dataset.fromScheduler === 'true';
            // 获取 scheduler_task_id（如果存在），用于启用/禁用操作
            const schedulerTaskId = group.dataset.schedulerTaskId ? parseInt(group.dataset.schedulerTaskId, 10) : taskId;
            
            // 删除按钮的特殊处理：计划任务由 SchedulerManager 处理，非计划任务由 deleteBackupTask 处理
            if (button.classList.contains('btn-action-delete')) {
                event.preventDefault();
                event.stopPropagation();
                event.stopImmediatePropagation(); // 阻止其他监听器处理
                
                if (fromScheduler) {
                    // 计划任务：使用 SchedulerManager.deleteTask（包含确认对话框）
                    if (window.SchedulerManager && typeof window.SchedulerManager.deleteTask === 'function') {
                        window.SchedulerManager.deleteTask(taskId).then(() => {
                            loadAllTasks();
                            loadRunningTasks();
                        }).catch(err => {
                            console.error('删除任务失败:', err);
                            alert('删除任务失败: ' + (err.message || '未知错误'));
                        });
                    } else {
                        // 如果 SchedulerManager 不存在，直接调用 deleteBackupTask（它会处理计划任务）
                        console.warn('[backup.js] SchedulerManager 不存在，使用 deleteBackupTask 删除计划任务');
                        deleteBackupTask(taskId, fromScheduler).catch(err => alert(err.message));
                    }
                } else {
                    // 非计划任务（模板任务）：使用 deleteBackupTask（包含确认对话框）
                    deleteBackupTask(taskId, fromScheduler).catch(err => alert(err.message));
                }
                return; // 重要：处理完删除后直接返回，不继续执行
            }
            
            event.preventDefault();
            event.stopPropagation();
            event.stopImmediatePropagation(); // 阻止其他监听器处理

            if (button.classList.contains('btn-action-enable')) {
                // 使用 scheduler_task_id 进行启用/禁用操作
                toggleSchedulerTask(schedulerTaskId, true).catch(err => alert(err.message));
            } else if (button.classList.contains('btn-action-disable')) {
                // 使用 scheduler_task_id 进行启用/禁用操作
                toggleSchedulerTask(schedulerTaskId, false).catch(err => alert(err.message));
            } else if (button.classList.contains('btn-action-unlock')) {
                // 使用 scheduler_task_id 进行解锁操作
                const schedulerTaskId = button.closest('.btn-group')?.getAttribute('data-scheduler-task-id');
                const unlockTaskId = schedulerTaskId ? parseInt(schedulerTaskId) : taskId;
                if (!isNaN(unlockTaskId)) {
                    unlockSchedulerTask(unlockTaskId).catch(err => alert(err.message));
                } else {
                    alert('无法获取任务ID');
                }
            } else if (button.classList.contains('btn-action-edit')) {
                editBackupTask(taskId, fromScheduler);
            }
        });
    }

    function startRunningTasksPolling() {
        if (runningInterval) clearInterval(runningInterval);
        runningInterval = setInterval(loadRunningTasks, POLL_INTERVAL);
    }

    document.addEventListener('DOMContentLoaded', () => {
        loadBackupStatistics();
        loadRunningTasks();
        loadAllTasks();
        startRunningTasksPolling();
        bindFilterEvents();
        bindTableActions();
        window.addEventListener('beforeunload', () => {
            if (runningInterval) clearInterval(runningInterval);
        });
    });
})();

