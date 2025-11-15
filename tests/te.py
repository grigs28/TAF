# everything_service_manager.py
import win32serviceutil
import win32service
import win32event
import servicemanager

class EverythingServiceManager:
    """Everything服务管理器"""
    
    @staticmethod
    def get_service_status():
        """获取Everything服务状态"""
        try:
            status = win32serviceutil.QueryServiceStatus("Everything")
            state = status[1]
            
            states = {
                win32service.SERVICE_STOPPED: "已停止",
                win32service.SERVICE_START_PENDING: "启动中",
                win32service.SERVICE_STOP_PENDING: "停止中",
                win32service.SERVICE_RUNNING: "运行中",
                win32service.SERVICE_CONTINUE_PENDING: "继续中",
                win32service.SERVICE_PAUSE_PENDING: "暂停中",
                win32service.SERVICE_PAUSED: "已暂停"
            }
            
            return states.get(state, f"未知状态 ({state})")
        except Exception as e:
            return f"错误: {str(e)}"
    
    @staticmethod
    def start_service():
        """启动Everything服务"""
        try:
            win32serviceutil.StartService("Everything")
            print("Everything服务启动命令已发送")
            return True
        except Exception as e:
            print(f"启动服务失败: {str(e)}")
            return False
    
    @staticmethod
    def stop_service():
        """停止Everything服务"""
        try:
            win32serviceutil.StopService("Everything")
            print("Everything服务停止命令已发送")
            return True
        except Exception as e:
            print(f"停止服务失败: {str(e)}")
            return False
    
    @staticmethod
    def restart_service():
        """重启Everything服务"""
        try:
            win32serviceutil.RestartService("Everything")
            print("Everything服务重启命令已发送")
            return True
        except Exception as e:
            print(f"重启服务失败: {str(e)}")
            return False

def manage_everything_service():
    """Everything服务管理界面"""
    manager = EverythingServiceManager()
    
    while True:
        print("\nEverything服务管理器")
        print("=" * 30)
        status = manager.get_service_status()
        print(f"当前状态: {status}")
        print("\n可选操作:")
        print("1. 启动服务")
        print("2. 停止服务")
        print("3. 重启服务")
        print("4. 刷新状态")
        print("5. 退出")
        
        choice = input("\n请选择操作 (1-5): ").strip()
        
        if choice == '1':
            manager.start_service()
        elif choice == '2':
            manager.stop_service()
        elif choice == '3':
            manager.restart_service()
        elif choice == '4':
            pass  # 状态会自动刷新
        elif choice == '5':
            print("退出服务管理器")
            break
        else:
            print("无效选择，请重新输入")
        
        # 等待一下让状态更新
        import time
        time.sleep(1)

if __name__ == "__main__":
    # 如果要使用服务管理器，取消下面的注释
    # manage_everything_service()
    pass