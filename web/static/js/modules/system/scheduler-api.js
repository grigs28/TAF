/**
 * 计划任务API调用模块
 * Scheduler API Module
 */

import { showMessage } from './scheduler-utils.js';

// 确保 axios 可用（从全局作用域获取）
const axios = window.axios || (typeof axios !== 'undefined' ? axios : null);

if (!axios) {
    console.error('axios 未加载，请确保在页面中引入了 axios.min.js');
}

/**
 * fetchJSON工具函数 - 与backup.js保持一致
 */
async function fetchJSON(url, options) {
    const response = await fetch(url, options);
    if (!response.ok) {
        const text = await response.text();
        throw new Error(text || response.statusText);
    }
    return response.json();
}

/**
 * 计划任务API类
 */
export class SchedulerAPI {
    /**
     * 加载任务列表
     */
    static async loadTasks() {
        try {
            const response = await axios.get('/api/scheduler/tasks');
            
            if (response.data && Array.isArray(response.data)) {
                return response.data;
            }
            return [];
        } catch (error) {
            console.error('加载计划任务失败:', error);
            showMessage('加载计划任务失败: ' + (error.response?.data?.detail || error.message), 'error');
            throw error;
        }
    }
    
    /**
     * 加载磁带机设备列表
     */
    static async loadTapeDevices() {
        try {
            const response = await axios.get('/api/system/tape/scan');
            
            if (response.data && response.data.devices) {
                return response.data.devices;
            }
            return [];
        } catch (error) {
            console.error('加载磁带机设备失败:', error);
            showMessage('加载磁带机设备失败: ' + (error.response?.data?.detail || error.message), 'error');
            return [];
        }
    }
    
    /**
     * 获取任务详情
     */
    static async getTask(taskId) {
        try {
            const response = await axios.get(`/api/scheduler/tasks/${taskId}`);
            return response.data;
        } catch (error) {
            console.error('加载任务详情失败:', error);
            showMessage('加载任务详情失败: ' + (error.response?.data?.detail || error.message), 'error');
            throw error;
        }
    }
    
    /**
     * 保存任务（创建或更新）
     */
    static async saveTask(taskData) {
        try {
            let response;
            if (taskData.id) {
                // 更新任务
                response = await axios.put(`/api/scheduler/tasks/${taskData.id}`, taskData);
            } else {
                // 创建任务
                response = await axios.post('/api/scheduler/tasks', taskData);
            }
            
            showMessage(taskData.id ? '任务更新成功' : '任务创建成功', 'success');
            return response.data;
        } catch (error) {
            console.error('保存任务失败:', error);
            showMessage('保存任务失败: ' + (error.response?.data?.detail || error.message), 'error');
            throw error;
        }
    }
    
    /**
     * 删除任务
     */
    static async deleteTask(taskId) {
        try {
            await fetchJSON(`/api/scheduler/tasks/${taskId}`, { method: 'DELETE' });
            alert('任务已删除');
            return true;
        } catch (error) {
            console.error('删除任务失败:', error);
            alert('删除任务失败: ' + (error.message || '未知错误'));
            throw error;
        }
    }
    
    /**
     * 启用任务
     */
    static async enableTask(taskId) {
        try {
            await fetchJSON(`/api/scheduler/tasks/${taskId}/enable`, { method: 'POST' });
            alert('任务已启用');
            return true;
        } catch (error) {
            console.error('启用任务失败:', error);
            alert('启用任务失败: ' + (error.message || '未知错误'));
            throw error;
        }
    }

    /**
     * 禁用任务
     */
    static async disableTask(taskId) {
        try {
            await fetchJSON(`/api/scheduler/tasks/${taskId}/disable`, { method: 'POST' });
            alert('任务已禁用');
            return true;
        } catch (error) {
            console.error('禁用任务失败:', error);
            alert('禁用任务失败: ' + (error.message || '未知错误'));
            throw error;
        }
    }
    
    /**
     * 立即运行任务
     */
    static async runTask(taskId) {
        try {
            await fetchJSON(`/api/scheduler/tasks/${taskId}/run`, { method: 'POST' });
            alert('任务已提交运行');
            return true;
        } catch (error) {
            console.error('运行任务失败:', error);
            alert('运行任务失败: ' + (error.message || '未知错误'));
            throw error;
        }
    }
    
    /**
     * 解锁任务
     */
    static async unlockTask(taskId) {
        try {
            await fetchJSON(`/api/scheduler/tasks/${taskId}/unlock`, { method: 'POST' });
            alert('任务已解锁');
            return true;
        } catch (error) {
            console.error('解锁任务失败:', error);
            alert('解锁任务失败: ' + (error.message || '未知错误'));
            throw error;
        }
    }
    
    /**
     * 浏览目录
     */
    static async browseDirectory(path) {
        try {
            const response = await axios.get('/api/system/file-system/list', {
                params: { path: path }
            });
            return response.data;
        } catch (error) {
            if (error.response?.status === 403) {
                throw new Error('访问被拒绝');
            }
            console.error('浏览目录失败:', error);
            throw error;
        }
    }
    
    /**
     * 加载驱动器列表
     */
    static async loadDrives() {
        try {
            const response = await axios.get('/api/system/file-system/drives');
            return response.data;
        } catch (error) {
            console.error('加载驱动器列表失败:', error);
            throw error;
        }
    }
}

