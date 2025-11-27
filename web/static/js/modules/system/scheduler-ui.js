/**
 * 计划任务UI渲染模块
 * Scheduler UI Module
 */

import { escapeHtml } from './scheduler-utils.js';
import { ScheduleConfigManager } from './scheduler-schedule-config.js';

/**
 * UI渲染管理器类
 */
export class SchedulerUI {
    /**
     * 渲染任务表格
     */
    static renderTasksTable(tasks, scheduleTypes) {
        const tbody = document.getElementById('scheduledTasksTableBody');
        if (!tbody) return;
        
        if (tasks.length === 0) {
            tbody.innerHTML = `
                <tr>
                    <td colspan="8" class="text-center text-muted">
                        <i class="bi bi-inbox me-2"></i>暂无计划任务
                    </td>
                </tr>
            `;
            return;
        }
        
        tbody.innerHTML = tasks.map(task => {
            const statusBadge = this.getStatusBadge(task);
            const scheduleTypeLabel = scheduleTypes.find(t => t.value === task.schedule_type)?.label || task.schedule_type;
            const nextRun = task.next_run_time ? new Date(task.next_run_time).toLocaleString('zh-CN') : '未设置';
            const lastRun = task.last_run_time ? new Date(task.last_run_time).toLocaleString('zh-CN') : '从未执行';
            
            return `
                <tr data-task-id="${task.id}">
                    <td>${escapeHtml(task.task_name)}</td>
                    <td>${scheduleTypeLabel}</td>
                    <td>${ScheduleConfigManager.formatScheduleConfig(task.schedule_type, task.schedule_config)}</td>
                    <td>${statusBadge}</td>
                    <td>${nextRun}</td>
                    <td>${lastRun}</td>
                    <td>${task.success_runs || 0}/${task.total_runs || 0}</td>
                    <td>
                        <div class="btn-group btn-group-sm">
                            ${task.enabled
                                ? `<button class="btn btn-outline-warning btn-action-disable" data-task-id="${task.id}" title="禁用">
                                    <i class="bi bi-pause"></i> 禁用
                                </button>`
                                : `<button class="btn btn-outline-success btn-action-enable" data-task-id="${task.id}" title="启用">
                                    <i class="bi bi-play"></i> 启用
                                </button>`
                            }
                            <button class="btn btn-outline-primary btn-action-edit" data-task-id="${task.id}" title="编辑">
                                <i class="bi bi-pencil"></i>
                            </button>
                            <button class="btn btn-outline-info btn-action-run" data-task-id="${task.id}" title="立即运行">
                                <i class="bi bi-play-circle"></i>
                            </button>
                            <button class="btn btn-outline-secondary btn-action-unlock" data-task-id="${task.id}" title="解锁">
                                <i class="bi bi-unlock"></i>
                            </button>
                            <button class="btn btn-outline-danger btn-action-delete" data-task-id="${task.id}" title="删除">
                                <i class="bi bi-trash"></i>
                            </button>
                        </div>
                    </td>
                </tr>
            `;
        }).join('');
    }
    
    /**
     * 获取状态徽章
     */
    static getStatusBadge(task) {
        // 如果任务被禁用，显示"已禁用"
        if (!task.enabled) {
            return '<span class="badge bg-secondary">已禁用</span>';
        }
        
        const status = task.status || 'inactive';
        
        // 如果状态是 running，显示"运行中"
        if (status === 'running') {
            return '<span class="badge bg-primary">运行中</span>';
        }
        
        // 如果状态是 error，显示"错误"
        if (status === 'error') {
            return '<span class="badge bg-danger">错误</span>';
        }
        
        // 如果状态是 paused，显示"已暂停"
        if (status === 'paused') {
            return '<span class="badge bg-warning">已暂停</span>';
        }
        
        // 如果从未执行过（last_run_time 为空），显示"未运行"
        if (!task.last_run_time) {
            return '<span class="badge bg-info">未运行</span>';
        }
        
        // 如果状态是 active，显示"已启用"
        if (status === 'active') {
            return '<span class="badge bg-success">已启用</span>';
        }
        
        // 其他状态显示"未激活"
        return '<span class="badge bg-secondary">未激活</span>';
    }
}

