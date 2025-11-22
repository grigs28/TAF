(function () {
    const POLL_INTERVAL = 5000;
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
        const start = new Date(startedAt);
        const end = completedAt ? new Date(completedAt) : new Date();
        const diffMs = end - start;
        if (diffMs <= 0) return '0秒';
        const seconds = Math.floor(diffMs / 1000);
        const minutes = Math.floor(seconds / 60);
        const hours = Math.floor(minutes / 60);
        const days = Math.floor(hours / 24);
        if (days > 0) return `${days}天 ${hours % 24}小时 ${minutes % 60}分钟`;
        if (hours > 0) return `${hours}小时 ${minutes % 60}分钟`;
        if (minutes > 0) return `${minutes}分钟 ${seconds % 60}秒`;
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
            return parseFloat(task.compression_speed_gb_per_sec).toFixed(2);
        }
        
        // 如果没有实时速度，计算平均速度（G/秒）
        if (task.processed_bytes && task.started_at) {
            const start = new Date(task.started_at);
            const end = task.completed_at ? new Date(task.completed_at) : new Date();
            const diffMs = end - start;
            if (diffMs > 0) {
                const seconds = diffMs / 1000;
                const processedGB = task.processed_bytes / (1024 * 1024 * 1024);
                const speedGBPerSec = processedGB / seconds;
                return speedGBPerSec.toFixed(2);
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
        if (description.includes('[格式化中]')) {
            return '<span class="badge bg-info">格式化中</span>';
        }
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
                return '<span class="badge bg-warning text-dark">等待中</span>';
            default:
                return `<span class="badge bg-secondary">${status || '未知'}</span>`;
        }
    }

    function computeProgress(task) {
        const processedFiles = task.processed_files || 0;
        const totalFiles = task.total_files || 0;
        const processedBytes = task.processed_bytes || 0;
        const totalBytes = task.total_bytes || task.total_bytes_actual || 0;
        const compressedBytes = task.compressed_bytes || 0;
        let percent = task.progress_percent || 0;
        if ((!percent || percent < 1) && totalFiles > 0) {
            percent = Math.min(100, (processedFiles / totalFiles) * 100);
        }
        let compressionRatio = task.compression_ratio || 0;
        if ((!compressionRatio || compressionRatio <= 0) && processedBytes > 0 && compressedBytes > 0) {
            compressionRatio = compressedBytes / processedBytes;
        }
        
        return {
            percent: Number(percent.toFixed(1)),
            processedFiles,
            totalFiles,
            processedBytes,
            totalBytes: totalBytes || processedBytes,
            compressedBytes,
            compressionRatio
        };
    }

    function formatCompressionRatio(value) {
        if (!value || value <= 0) return '-';
        return (value * 100).toFixed(2) + '%';
    }

    function getStageBadgeClass(state, stageCode, progressPercent = null) {
        switch ((state || '').toLowerCase()) {
            case 'done':
                return 'bg-success';
            case 'current':
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

    function getCompletedStageBadgeClass(state, stageCode) {
        switch ((state || '').toLowerCase()) {
            case 'done':
                // 完成的阶段根据类型使用不同颜色
                switch (stageCode) {
                    case 'scan':
                        return 'bg-info'; // 扫描完成 - 蓝色
                    case 'compress':
                        return 'bg-warning text-dark'; // 压缩完成 - 黄色
                    case 'copy':
                        return 'bg-danger text-white'; // 写入磁带完成 - 红色
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
        const cardCol = document.createElement('div');
        cardCol.className = 'col-md-4 col-lg-4 mb-3';

        const card = document.createElement('div');
        card.className = 'service-card';

        const body = document.createElement('div');
        body.className = 'card-body';

        const isRunning = (task.status || '').toLowerCase() === 'running';
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
            const speedGBPerSec = calculateProcessingSpeedGBPerSec(task);
            if (speedGBPerSec !== null && parseFloat(speedGBPerSec) > 0) {
                // 计算每小时处理GB数（G/秒 * 3600秒）
                const speedGBPerHour = parseFloat((parseFloat(speedGBPerSec) * 3600).toFixed(2));
                
                // 根据80G基准判断颜色
                // >= 80G: 绿色（bg-success）- 良好
                // < 80G: 黄色（bg-warning）- 较慢
                const badgeClass = speedGBPerHour >= 80 ? 'badge bg-success' : 'badge bg-warning text-dark';
                
                // 在右上角徽章中显示每小时处理GB数（只显示数字）
                const speedBadge = document.createElement('span');
                speedBadge.className = badgeClass;
                speedBadge.style.cssText = 'font-size: 0.85rem; font-weight: 600; padding: 0.35em 0.65em;';
                speedBadge.textContent = speedGBPerHour.toFixed(2);
                speedBadge.title = `每小时处理: ${speedGBPerHour.toFixed(2)} GB ${speedGBPerHour >= 80 ? '(良好)' : '(较慢)'}`;
                
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

        // 根据 operation_stage 动态构建阶段步骤
        const operationStage = (task.operation_stage || '').toLowerCase();
        const isCompleted = (task.status || '').toLowerCase() === 'completed';
        
        // 定义阶段顺序和映射
        const stageOrder = ['scan', 'compress', 'copy', 'finalize'];
        const stageLabels = {
            'scan': '扫描文件',
            'compress': '压缩/打包',
            'copy': '写入磁带',
            'finalize': '完成'
        };
        
        // 构建阶段步骤，根据 operation_stage 动态设置状态
        let stageSteps = [];
        
        // 如果后端已经提供了 stage_steps，先使用后端的（但需要转换 status 为 state）
        if (Array.isArray(task.stage_steps) && task.stage_steps.length > 0) {
            // 转换后端的 status 字段为前端的 state 字段
            stageSteps = task.stage_steps.map(step => {
                // 后端使用 status: "completed"/"active"/"pending"
                // 前端使用 state: "done"/"current"/"pending"
                let state = step.state || step.status || 'pending';
                if (state === 'completed') state = 'done';
                if (state === 'active') state = 'current';
                return {
                    code: step.code,
                    label: step.label,
                    state: state
                };
            });
        }
        
        // 如果后端没有提供 stage_steps 或为空，根据 operation_stage 动态构建
        if (stageSteps.length === 0 && operationStage) {
            if (isCompleted) {
                // 完成状态：所有阶段都是 done，finalize 是 current
                stageOrder.forEach(code => {
                    stageSteps.push({
                        code: code,
                        label: stageLabels[code] || code,
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
                        stageSteps.push({
                            code: code,
                            label: stageLabels[code] || code,
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
                        ${stageSteps.map(step => `<span class="badge ${getCompletedStageBadgeClass(step.state, step.code)}">${step.label}</span>`).join('')}
                    </div>
                `;
                body.appendChild(stageSection);
            } else {
                const currentStageLabel = task.operation_status
                    || stageSteps.find(step => step.state === 'current')?.label
                    || '-';
                
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
                
                const stageSection = document.createElement('div');
                stageSection.className = 'mb-2';
                stageSection.innerHTML = `
                    <div class="d-flex justify-content-between align-items-center">
                        <small class="text-muted">当前阶段:</small>
                        <small class="text-muted">${currentStageLabel}</small>
                    </div>
                    <div class="d-flex flex-wrap gap-1 mt-1">
                        ${stageSteps.map(step => {
                            // 如果是当前阶段且有进度信息，传递进度百分比
                            const progress = (step.state === 'current' && progressPercent !== null) ? progressPercent : null;
                            return `<span class="badge ${getStageBadgeClass(step.state, step.code, progress)}">${step.label}</span>`;
                        }).join('')}
                    </div>
                `;
                body.appendChild(stageSection);
            }
        }

        if (isRunning && progressInfo) {
            const progressSection = document.createElement('div');
            progressSection.className = 'mb-2';
            
            // 检查是否有批次压缩进度信息
            let batchProgressHtml = '';
            if (task.operation_stage === 'compress' && task.current_compression_progress) {
                const compProg = task.current_compression_progress;
                batchProgressHtml = `
                    <div class="d-flex justify-content-between align-items-center mb-1">
                        <small class="text-muted">本批次压缩进度:</small>
                        <small class="text-muted fw-semibold">${compProg.current}/${compProg.total} 个文件 (${compProg.percent.toFixed(1)}%)</small>
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
    }

    async function loadRunningTasks() {
        if (!dom.runningList) return;
        try {
            const [running, failed] = await Promise.all([
                fetchJSON('/api/backup/tasks?status=running&limit=10'),
                fetchJSON('/api/backup/tasks?status=failed&limit=5'),
            ]);
            dom.runningList.innerHTML = '';
            const tasks = [];
            tasks.push(...(running || []));
            const now = Date.now();
            (failed || []).forEach(task => {
                const completed = task.completed_at ? new Date(task.completed_at).getTime() : 0;
                if (completed && now - completed <= 10 * 60 * 1000) {
                    tasks.push(task);
                }
            });
            if (tasks.length === 0) {
                dom.runningList.innerHTML = '<div class="col-12"><p class="text-muted">暂无运行中的任务和最近失败的任务</p></div>';
            } else {
                tasks.forEach(task => {
                    dom.runningList.appendChild(createRunningCard(task));
                });
            }
            if (dom.runningTasksCounter) dom.runningTasksCounter.textContent = tasks.length;
        } catch (error) {
            console.error('加载运行中的任务失败:', error);
            dom.runningList.innerHTML = '<div class="col-12"><p class="text-danger">加载失败</p></div>';
        }
    }

    function buildTaskNameCell(task) {
        const badge = task.is_template ? ' <span class="badge bg-secondary">模板</span>' : '';
        return `<strong>${task.task_name || '未命名任务'}</strong>${badge}`;
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
        return `
            <tr class="${task.is_template ? 'table-warning' : ''}">
                <td>${buildTaskNameCell(task)}</td>
                <td>${formatTaskType(task.task_type || 'full')}</td>
                <td><code class="text-truncate d-inline-block" style="max-width:200px;" title="${sourcePaths}">${sourcePaths}</code></td>
                <td>${buildStatusBadge(task)}</td>
                <td>${formatDateTime(task.started_at)}</td>
                <td>${formatDateTime(task.completed_at)}</td>
                <td>${formatBytes(task.processed_bytes || 0)} / ${formatBytes(task.total_bytes || 0)}</td>
                <td>
                    <div class="btn-group btn-group-sm" data-task-id="${task.task_id || task.id}" data-from-scheduler="${task.from_scheduler ? 'true' : 'false'}" data-enabled="${task.enabled !== false ? 'true' : 'false'}">
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
        // 显示确认对话框（与 scheduler.js 保持一致）
        if (!confirm('确定要立即运行此计划任务吗？')) return;
        
        try {
            await fetchJSON(`/api/scheduler/tasks/${taskId}/run`, { method: 'POST' });
            alert('任务已提交运行');
            loadRunningTasks();
            loadAllTasks();
        } catch (error) {
            console.error('运行任务失败:', error);
            alert('运行任务失败: ' + (error.message || '未知错误'));
        }
    }

    async function unlockSchedulerTask(taskId) {
        await fetchJSON(`/api/scheduler/tasks/${taskId}/unlock`, { method: 'POST' });
        alert('任务已解锁');
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
            if (button.classList.contains('btn-action-run')) {
                event.preventDefault();
                event.stopPropagation();

                // 尝试从不同位置获取taskId
                const group = button.closest('.btn-group');
                const taskId = group ?
                    parseInt(group.dataset.taskId, 10) :
                    parseInt(button.dataset.taskId, 10);

                if (taskId && !isNaN(taskId)) {
                    runSchedulerTask(taskId).catch(err => alert(err.message));
                }
                return;
            }

            // 其他备份页面特定的按钮
            const group = button.closest('.btn-group');
            if (!group || !dom.allTasksTable.contains(group)) return;

            const taskId = parseInt(group.dataset.taskId, 10);
            const fromScheduler = group.dataset.fromScheduler === 'true';
            event.preventDefault();
            event.stopPropagation();

            if (button.classList.contains('btn-action-enable')) {
                toggleSchedulerTask(taskId, true).catch(err => alert(err.message));
            } else if (button.classList.contains('btn-action-disable')) {
                toggleSchedulerTask(taskId, false).catch(err => alert(err.message));
            } else if (button.classList.contains('btn-action-unlock')) {
                unlockSchedulerTask(taskId).catch(err => alert(err.message));
            } else if (button.classList.contains('btn-action-edit')) {
                editBackupTask(taskId, fromScheduler);
            } else if (button.classList.contains('btn-action-delete')) {
                deleteBackupTask(taskId, fromScheduler).catch(err => alert(err.message));
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

