#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
磁带处理模块
Tape Handler Module
"""

import logging
from pathlib import Path
from typing import Optional

from models.backup import BackupSet
from tape.tape_manager import TapeManager
from tape.tape_cartridge import TapeCartridge, TapeStatus

logger = logging.getLogger(__name__)


class TapeHandler:
    """磁带处理器"""
    
    def __init__(self, tape_manager: TapeManager = None, settings=None):
        """初始化磁带处理器
        
        Args:
            tape_manager: 磁带管理器对象
            settings: 系统设置对象
        """
        self.tape_manager = tape_manager
        self.settings = settings
    
    async def get_current_drive_tape(self) -> Optional[TapeCartridge]:
        """获取当前驱动器中的磁带
        
        Returns:
            TapeCartridge: 磁带对象，如果不存在则返回None
        """
        try:
            if not self.tape_manager:
                logger.warning("磁带管理器未初始化")
                return None
            
            # 检查当前磁带管理器是否已有当前磁带
            if self.tape_manager.current_tape:
                logger.info(f"当前驱动器已有磁带: {self.tape_manager.current_tape.tape_id}")
                return self.tape_manager.current_tape
            
            # 尝试扫描当前驱动器中的磁带卷标
            try:
                tape_ops = self.tape_manager.tape_operations
                if tape_ops and hasattr(tape_ops, '_read_tape_label'):
                    label_info = await tape_ops._read_tape_label()
                    if label_info and label_info.get('tape_id'):
                        tape_id = label_info.get('tape_id')
                        logger.info(f"从驱动器扫描到磁带卷标: {tape_id}")
                        
                        # 检查数据库中是否有该磁带
                        from utils.scheduler.db_utils import is_opengauss, get_opengauss_connection
                        if is_opengauss():
                            # 使用连接池
                            async with get_opengauss_connection() as conn:
                                row = await conn.fetchrow(
                                    """
                                    SELECT tape_id, label, status, 
                                           COALESCE(first_use_date, manufactured_date, created_at) as created_date,
                                           expiry_date,
                                           capacity_bytes, used_bytes, serial_number
                                    FROM tape_cartridges
                                    WHERE tape_id = $1
                                    """,
                                    tape_id
                                )
                                
                                if row:
                                    # 磁带在数据库中，创建 TapeCartridge 对象
                                    from datetime import datetime
                                    created_date = row['created_date']
                                    if created_date and isinstance(created_date, str):
                                        try:
                                            created_date = datetime.fromisoformat(created_date.replace('Z', '+00:00'))
                                        except:
                                            created_date = datetime.fromisoformat(created_date.split('T')[0])
                                    
                                    # 处理状态值（数据库可能返回大写，枚举是小写）
                                    status_str = row.get('status')
                                    if status_str:
                                        # 转换为小写并尝试创建枚举
                                        status_lower = status_str.lower().strip() if isinstance(status_str, str) else str(status_str).lower().strip()
                                        try:
                                            tape_status = TapeStatus(status_lower)
                                        except ValueError:
                                            # 如果直接匹配失败，尝试匹配枚举值
                                            tape_status = TapeStatus.AVAILABLE
                                            for status in TapeStatus:
                                                if status.value.lower() == status_lower:
                                                    tape_status = status
                                                    break
                                            else:
                                                logger.warning(f"无法解析磁带状态值 '{status_str}'，使用默认值 AVAILABLE")
                                    else:
                                        tape_status = TapeStatus.AVAILABLE
                                    
                                    tape = TapeCartridge(
                                        tape_id=row['tape_id'],
                                        label=row['label'],
                                        status=tape_status,
                                        created_date=created_date,
                                        expiry_date=row['expiry_date'],
                                        capacity_bytes=row['capacity_bytes'] or 0,
                                        used_bytes=row['used_bytes'] or 0,
                                        serial_number=row['serial_number'] or ''
                                    )
                                    # 更新磁带管理器的当前磁带
                                    self.tape_manager.current_tape = tape
                                    self.tape_manager.tape_cartridges[tape_id] = tape
                                    logger.info(f"从数据库加载磁带信息: {tape_id}")
                                    return tape
                                else:
                                    # 磁带不在数据库中，但驱动器中有磁带
                                    logger.error(f"驱动器中的磁带不在数据库中: {tape_id}")
                                    logger.error("检测到驱动器中的磁带未在数据库中注册，任务将停止")
                                    # 抛出异常，停止任务执行
                                    raise RuntimeError(f"驱动器中的磁带 {tape_id} 未在数据库中注册，请先在磁带管理页面添加该磁带")
            except Exception as e:
                logger.warning(f"扫描当前驱动器磁带失败: {str(e)}")
                return None
                
            return None
        except Exception as e:
            logger.error(f"获取当前驱动器磁带失败: {str(e)}")
            return None
    
    async def write_to_tape_drive(self, source_path: str, backup_set: BackupSet, group_idx: int) -> Optional[str]:
        """将压缩文件从本地目录复制到磁带机（LTFS挂载的盘符）
        
        流程：
        1. 先复制文件到磁带盘符（通过LTFS挂载）
        2. 验证复制成功（检查文件大小）
        3. 确认成功后再删除源文件
        
        Args:
            source_path: 源文件路径（本地压缩文件路径）
            backup_set: 备份集对象
            group_idx: 组索引
            
        Returns:
            str: 磁带上的相对路径，如果失败则返回None
        """
        try:
            import shutil
            
            source_file = Path(source_path)
            if not source_file.exists():
                logger.error(f"压缩文件不存在: {source_path}")
                return None
            
            # 获取源文件大小（用于验证）
            source_size = source_file.stat().st_size
            logger.info(f"准备复制文件到磁带机: {source_file} (大小: {source_size} 字节)")
            
            # 目标路径：磁带盘符（通过LTFS挂载）
            tape_drive = self.settings.TAPE_DRIVE_LETTER.upper() + ":\\"
            tape_backup_dir = Path(tape_drive) / backup_set.set_id
            tape_backup_dir.mkdir(parents=True, exist_ok=True)
            
            # 目标文件路径
            target_file = tape_backup_dir / source_file.name
            
            # 步骤1: 复制文件到磁带机（LTFS挂载的盘符）
            logger.info(f"正在复制文件到磁带机: {source_file} -> {target_file}")
            shutil.copy2(str(source_file), str(target_file))
            
            # 步骤2: 验证复制成功（检查文件是否存在且大小匹配）
            if not target_file.exists():
                logger.error(f"复制后目标文件不存在: {target_file}")
                return None
            
            target_size = target_file.stat().st_size
            if target_size != source_size:
                logger.error(f"文件大小不匹配: 源文件={source_size} 字节, 目标文件={target_size} 字节")
                # 删除不完整的文件
                try:
                    target_file.unlink()
                except Exception:
                    pass
                return None
            
            # 步骤3: 验证成功，删除源文件
            logger.info(f"文件复制成功，验证通过（大小: {target_size} 字节），删除源文件")
            try:
                source_file.unlink()
                logger.info(f"源文件已删除: {source_file}")
            except Exception as del_error:
                logger.warning(f"删除源文件失败（可稍后手动删除）: {del_error}")
                # 删除失败不影响备份流程，继续执行
            
            # 返回磁带上的相对路径
            relative_path = str(target_file.relative_to(Path(tape_drive)))
            logger.info(f"压缩文件已成功复制到磁带: {relative_path}")
            
            return relative_path
            
        except Exception as e:
            logger.error(f"复制文件到磁带机失败: {str(e)}")
            import traceback
            logger.error(traceback.format_exc())
            return None

