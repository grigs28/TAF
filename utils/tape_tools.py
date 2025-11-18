#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
磁带工具模块
Tape Tools Module - 封装 ITDT 和 LTFS 命令行工具
"""

import os
import asyncio
import logging
from typing import Optional, Dict, Any, List
from pathlib import Path
from datetime import datetime

from config.settings import get_settings

logger = logging.getLogger(__name__)


class TapeToolsManager:
    """磁带工具管理器 - 封装 ITDT 和 LTFS 命令"""
    
    def __init__(self):
        self.settings = get_settings()
        self.tape_drive = r'\\.\tape0'
        self.itdt_path = getattr(self.settings, 'ITDT_PATH', r"D:\APP\TAF\ITDT\itdt.exe")
        self.itdt_dir = os.path.dirname(self.itdt_path)
        # LTFS工具必须在它们的程序目录下运行
        self.ltfs_tools_dir = getattr(self.settings, 'LTFS_TOOLS_DIR', self.itdt_dir)
        drive_letter = getattr(self.settings, 'TAPE_DRIVE_LETTER', 'O') or 'O'
        drive_letter = str(drive_letter).strip().upper()
        if drive_letter.endswith(':'):
            drive_letter = drive_letter[:-1]
        self.drive_letter = drive_letter or 'O'
        self.default_device = "0.0.24.0"  # 默认设备地址
        
        # LTFS 工具命令
        self.ltfs_tools = {
            'assign': 'LtfsCmdAssign.exe',
            'unassign': 'LtfsCmdUnassign.exe',
            'load': 'LtfsCmdLoad.exe',
            'eject': 'LtfsCmdEject.exe',
            'format': 'LtfsCmdFormat.exe',
            'unformat': 'LtfsCmdUnformat.exe',
            'check': 'LtfsCmdCheck.exe',
            'rollback': 'LtfsCmdRollback.exe',
            'drives': 'LtfsCmdDrives.exe',
            'mkltfs': 'mkltfs.exe'
        }
    
    async def run_command(self, cmd: List[str], timeout: int = 300, tool_type: str = "ITDT", 
                          working_dir: str = None) -> Dict[str, Any]:
        """运行命令并返回结果
        
        Args:
            cmd: 命令列表
            timeout: 超时时间（秒）
            tool_type: 工具类型（ITDT或LTFS）
            working_dir: 工作目录，如果为None则使用默认目录
        """
        # 确定工作目录
        if working_dir is None:
            working_dir = self.ltfs_tools_dir if tool_type == "LTFS" else self.itdt_dir
        
        cmd_str = ' '.join(cmd)
        logger.info(f"[{tool_type}] 执行命令: {cmd_str}")
        logger.info(f"[{tool_type}] 工作目录: {working_dir}")
        
        proc = None
        try:
            proc = await asyncio.create_subprocess_exec(
                *cmd,
                stdout=asyncio.subprocess.PIPE,
                stderr=asyncio.subprocess.PIPE,
                stdin=asyncio.subprocess.DEVNULL,  # 防止子进程等待输入导致阻塞
                cwd=working_dir  # 使用正确的工作目录
            )
            
            # 使用 wait() 和 read() 分别处理，避免 communicate() 的问题
            stdout = b""
            stderr = b""
            returncode = -1
            
            # 创建读取任务 - 在进程结束后读取所有输出
            async def read_stdout():
                if proc.stdout:
                    try:
                        # 进程结束后，读取所有剩余数据
                        data = b""
                        import time
                        read_start_time = time.time()
                        while True:
                            # 添加单次读取超时（1秒），防止无限等待
                            try:
                                chunk = await asyncio.wait_for(
                                    proc.stdout.read(4096),  # 每次读取4KB
                                    timeout=1.0
                                )
                                if not chunk:
                                    break
                                data += chunk
                                # 如果已经读取了很长时间（超过30秒），强制退出
                                if time.time() - read_start_time > 30:
                                    logger.warning(f"[{tool_type}] 读取stdout超时（30秒），强制退出")
                                    break
                            except asyncio.TimeoutError:
                                # 单次读取超时，可能管道已关闭，尝试检查
                                logger.debug(f"[{tool_type}] 读取stdout单次读取超时，可能管道已关闭")
                                break
                        return data
                    except Exception as e:
                        logger.warning(f"[{tool_type}] 读取stdout失败: {e}")
                        return b""
                return b""
            
            async def read_stderr():
                if proc.stderr:
                    try:
                        # 进程结束后，读取所有剩余数据
                        data = b""
                        import time
                        read_start_time = time.time()
                        while True:
                            # 添加单次读取超时（1秒），防止无限等待
                            try:
                                chunk = await asyncio.wait_for(
                                    proc.stderr.read(4096),  # 每次读取4KB
                                    timeout=1.0
                                )
                                if not chunk:
                                    break
                                data += chunk
                                # 如果已经读取了很长时间（超过30秒），强制退出
                                if time.time() - read_start_time > 30:
                                    logger.warning(f"[{tool_type}] 读取stderr超时（30秒），强制退出")
                                    break
                            except asyncio.TimeoutError:
                                # 单次读取超时，可能管道已关闭，尝试检查
                                logger.debug(f"[{tool_type}] 读取stderr单次读取超时，可能管道已关闭")
                                break
                        return data
                    except Exception as e:
                        logger.warning(f"[{tool_type}] 读取stderr失败: {e}")
                        return b""
                return b""
            
            try:
                # 等待进程结束
                returncode = await asyncio.wait_for(proc.wait(), timeout=timeout)
                logger.info(f"[{tool_type}] 进程已结束，返回码: {returncode}")
                
                # 读取输出（进程已结束，应该很快）
                # 使用较短的超时时间，因为进程已经结束
                try:
                    stdout_task = asyncio.create_task(read_stdout())
                    stderr_task = asyncio.create_task(read_stderr())
                    stdout, stderr = await asyncio.wait_for(
                        asyncio.gather(stdout_task, stderr_task),
                        timeout=30  # 增加到30秒，确保能读取完所有输出
                    )
                    logger.debug(f"[{tool_type}] 成功读取输出，stdout长度: {len(stdout)}, stderr长度: {len(stderr)}")
                except asyncio.TimeoutError:
                    logger.warning(f"[{tool_type}] 读取输出超时（进程已结束但读取超时）")
                    # 尝试强制关闭管道
                    try:
                        if proc.stdout:
                            proc.stdout.close()
                        if proc.stderr:
                            proc.stderr.close()
                    except Exception:
                        pass
                    stdout = b""
                    stderr = b""
                except Exception as e:
                    logger.warning(f"[{tool_type}] 读取输出时出错: {e}")
                    stdout = b""
                    stderr = b""
                    
            except asyncio.TimeoutError:
                # 超时了，尝试终止进程
                logger.warning(f"[{tool_type}] 命令执行超时，尝试终止进程...")
                try:
                    if proc and proc.returncode is None:
                        proc.kill()
                        # 等待进程终止
                        try:
                            await asyncio.wait_for(proc.wait(), timeout=5)
                            returncode = proc.returncode if proc.returncode is not None else -1
                        except asyncio.TimeoutError:
                            returncode = -1
                    else:
                        returncode = proc.returncode if proc else -1
                except Exception as e:
                    logger.error(f"[{tool_type}] 终止进程时出错: {e}")
                    returncode = -1
                
                logger.error(f"[{tool_type}] 命令执行超时")
                return {
                    "success": False,
                    "returncode": returncode,
                    "stdout": "",
                    "stderr": "命令执行超时",
                    "error": "Timeout",
                    "command": cmd_str  # 添加完整命令行
                }
            
            # 解码输出
            stdout_str = stdout.decode('utf-8', errors='ignore') if stdout else ""
            stderr_str = stderr.decode('utf-8', errors='ignore') if stderr else ""
            
            # 记录详细的执行结果
            logger.info(f"[{tool_type}] 命令执行完成 - 返回码: {returncode}, stdout长度: {len(stdout_str)}, stderr长度: {len(stderr_str)}")
            if stdout_str:
                logger.debug(f"[{tool_type}] stdout内容: {stdout_str[:500]}")  # 只记录前500字符
            if stderr_str:
                logger.debug(f"[{tool_type}] stderr内容: {stderr_str[:500]}")  # 只记录前500字符
            
            # 判断成功：返回码为0表示成功
            success = returncode == 0
            
            if not success:
                logger.warning(f"[{tool_type}] 命令执行失败，返回码: {returncode}")
                if stderr_str:
                    logger.warning(f"[{tool_type}] 错误信息: {stderr_str}")
                if stdout_str:
                    logger.warning(f"[{tool_type}] 输出信息: {stdout_str}")
            else:
                logger.info(f"[{tool_type}] 命令执行成功")
            
            return {
                "success": success,
                "returncode": returncode,
                "stdout": stdout_str,
                "stderr": stderr_str,
                "command": cmd_str  # 添加完整命令行
            }
            
        except Exception as e:
            logger.error(f"[{tool_type}] 命令执行异常: {str(e)}", exc_info=True)
            # 确保进程被清理
            if proc and proc.returncode is None:
                try:
                    proc.kill()
                    # 不等待，直接返回错误
                except Exception:
                    pass
            
            return {
                "success": False,
                "returncode": -1,
                "stdout": "",
                "stderr": str(e),
                "error": str(e),
                "command": cmd_str  # 添加完整命令行
            }
    
    # ===== ITDT 命令 =====
    async def load_tape_itdt(self) -> Dict[str, Any]:
        """使用ITDT加载磁带"""
        logger.info("使用ITDT加载磁带...")
        cmd = [self.itdt_path, '-f', self.tape_drive, 'load']
        return await self.run_command(cmd, timeout=60, tool_type="ITDT")
    
    async def check_tape_status_itdt(self) -> Dict[str, Any]:
        """检查磁带设备状态"""
        logger.info("检查磁带设备状态...")
        
        # 检查工具是否存在
        if not os.path.exists(self.itdt_path):
            logger.error(f"ITDT工具不存在: {self.itdt_path}")
            return {
                "success": False,
                "returncode": -1,
                "stdout": "",
                "stderr": f"工具文件不存在: {self.itdt_path}",
                "error": f"工具文件不存在: {self.itdt_path}"
            }
        
        # Windows设备路径（如 \\.\tape0）无法用os.path.exists()检查，直接尝试执行命令
        cmd = [self.itdt_path, '-f', self.tape_drive, 'inq']
        result = await self.run_command(cmd, timeout=30, tool_type="ITDT")
        
        # 如果命令执行成功但没有输出，记录日志
        if result.get("success") and not result.get("stdout") and not result.get("stderr"):
            logger.info("ITDT命令执行成功，但无输出（这可能是正常的）")
        
        return result
    
    async def get_partition_info_itdt(self) -> Dict[str, Any]:
        """使用ITDT获取分区信息"""
        logger.info("使用ITDT检查分区信息...")
        cmd = [self.itdt_path, '-f', self.tape_drive, 'qrypart']
        return await self.run_command(cmd, timeout=30, tool_type="ITDT")
    
    async def change_partition_itdt(self, partition_number: int) -> Dict[str, Any]:
        """使用ITDT切换分区"""
        logger.info(f"使用ITDT切换到分区 {partition_number}")
        cmd = [self.itdt_path, '-f', self.tape_drive, 'chgpart', str(partition_number)]
        return await self.run_command(cmd, timeout=60, tool_type="ITDT")
    
    async def erase_tape_itdt(self, quick: bool = True) -> Dict[str, Any]:
        """使用ITDT擦除磁带"""
        logger.info(f"使用ITDT擦除磁带 (快速: {quick})...")
        if quick:
            cmd = [self.itdt_path, '-f', self.tape_drive, 'erase', '-short']
        else:
            cmd = [self.itdt_path, '-f', self.tape_drive, 'erase']
        return await self.run_command(cmd, timeout=10800, tool_type="ITDT")  # 3小时超时
    
    async def get_tape_usage_itdt(self) -> Dict[str, Any]:
        """使用ITDT获取磁带使用统计"""
        logger.info("使用ITDT获取磁带使用统计...")
        cmd = [self.itdt_path, '-f', self.tape_drive, 'tapeusage']
        return await self.run_command(cmd, timeout=60, tool_type="ITDT")
    
    async def list_tape_itdt(self) -> Dict[str, Any]:
        """使用ITDT列出磁带内容"""
        logger.info("使用ITDT列出磁带内容...")
        cmd = [self.itdt_path, '-f', self.tape_drive, 'list']
        return await self.run_command(cmd, timeout=300, tool_type="ITDT")  # 5分钟超时
    
    async def read_mam_attributes_itdt(self, partition: Optional[int] = None, 
                                       attribute_id: Optional[str] = None,
                                       output_file: Optional[str] = None) -> Dict[str, Any]:
        """使用ITDT读取MAM属性（包括序列号、二维码等）
        
        Args:
            partition: 分区号（0-3），如果为None则不指定分区
            attribute_id: 属性标识符（如 "0x0001"），如果为None则尝试读取常见属性
            output_file: 输出文件路径，如果为None则使用临时文件
        
        Returns:
            包含MAM属性信息的字典
        """
        logger.info(f"使用ITDT读取MAM属性 (分区: {partition}, 属性ID: {attribute_id})...")
        
        # 如果没有指定输出文件，使用临时文件（使用.bin扩展名，符合ITDT惯例）
        import tempfile
        if not output_file:
            temp_file = tempfile.NamedTemporaryFile(mode='w', suffix='.bin', delete=False)
            output_file = temp_file.name
            temp_file.close()
        
        # 确保输出文件路径使用正确的格式（Windows路径）
        output_file = os.path.normpath(output_file)
        
        # 构建命令
        cmd = [self.itdt_path, '-f', self.tape_drive, 'readattr']
        
        # 添加分区参数（如果指定），格式为 -p0, -p1, -p2, -p3
        # 根据样例，通常需要指定分区，如果未指定则默认使用 -p0
        if partition is not None:
            if partition not in [0, 1, 2, 3]:
                raise ValueError(f"分区号必须是 0-3 之间的整数，当前值: {partition}")
            cmd.append(f'-p{partition}')
        else:
            # 默认使用分区0（根据样例）
            cmd.append('-p0')
        
        # 添加属性标识符（必需参数）
        # 如果没有指定属性ID，默认读取序列号（0x0002）
        # 常见的MAM属性标识符：
        # 0x0001: Media Manufacturer (制造商)
        # 0x0002: Media Serial Number (序列号)
        # 0x0009: Media Barcode (二维码)
        if not attribute_id:
            attribute_id = "0x0002"  # 默认读取序列号
        
        # 根据帮助信息，格式为 -aIdentifier（无空格）
        cmd.append(f'-a{attribute_id}')
        # 输出文件路径格式为 -dDestinationPathFile（无空格）
        cmd.append(f'-d{output_file}')
        
        result = await self.run_command(cmd, timeout=60, tool_type="ITDT")
        
        # 如果命令失败，记录详细的错误信息
        if not result.get("success"):
            logger.error(f"ITDT readattr 命令执行失败")
            logger.error(f"返回码: {result.get('returncode')}")
            logger.error(f"stdout: {result.get('stdout', '')}")
            logger.error(f"stderr: {result.get('stderr', '')}")
            # 即使命令失败，也尝试读取输出文件（某些情况下可能仍然生成了文件）
        
        # 读取输出文件内容
        mam_data = None
        file_exists = os.path.exists(output_file)
        logger.info(f"MAM属性输出文件路径: {output_file}")
        logger.info(f"文件是否存在: {file_exists}")
        
        if file_exists:
            try:
                file_size = os.path.getsize(output_file)
                logger.info(f"MAM属性文件大小: {file_size} 字节")
                
                with open(output_file, 'rb') as f:
                    mam_data = f.read()
                
                logger.info(f"实际读取的数据大小: {len(mam_data)} 字节")
                logger.info(f"数据前32字节(hex): {mam_data[:32].hex() if len(mam_data) >= 32 else mam_data.hex()}")
                
                # 尝试删除临时文件
                try:
                    os.unlink(output_file)
                except Exception:
                    pass
            except Exception as e:
                logger.warning(f"读取MAM属性文件失败: {e}")
                result["error_detail"] = f"读取输出文件失败: {str(e)}"
        else:
            logger.warning(f"MAM属性输出文件不存在: {output_file}")
            if not result.get("success"):
                # 如果命令失败且文件不存在，添加更详细的错误信息
                result["error_detail"] = f"命令执行失败，输出文件未生成。stderr: {result.get('stderr', '无')}"
            else:
                # 命令成功但文件不存在，可能是路径问题
                result["error_detail"] = f"命令执行成功，但输出文件未找到。可能原因：路径问题或权限问题。"
        
        # 解析MAM属性数据
        if result.get("success") and mam_data:
            try:
                # MAM属性数据格式说明：
                # - MAM属性数据可能包含头部信息（长度、类型等）
                # - 实际文本数据可能在数据的中后部
                # - 数据可能包含前导/尾随的空字节或控制字符
                # - 需要尝试多种解析方式
                
                logger.info(f"MAM原始数据长度: {len(mam_data)} 字节")
                logger.info(f"MAM原始数据(hex): {mam_data.hex()}")
                
                mam_text = None
                
                # 方法0: 尝试SCSI标准格式（如果数据长度>=2）
                # SCSI MAM格式可能：字节0=长度（不包括长度字段），字节1=类型，字节2+=数据
                if len(mam_data) >= 2:
                    try:
                        # 检查第一个字节是否是长度字段（应该 <= 剩余数据长度）
                        length_field = mam_data[0]
                        if length_field > 0 and length_field < len(mam_data):
                            # 尝试从字节2开始解析（跳过长度和类型字段）
                            if len(mam_data) >= 2 + length_field:
                                scsi_data = mam_data[2:2+length_field]
                                scsi_text = scsi_data.decode('ascii', errors='ignore').strip('\x00 \t\n\r')
                                printable_text = ''.join(c for c in scsi_text if c.isprintable()).strip()
                                if printable_text and len(printable_text) >= 2:
                                    mam_text = printable_text
                                    logger.info(f"使用SCSI标准格式解析到文本: {mam_text}")
                    except Exception:
                        pass
                
                # 方法1: 尝试跳过可能的头部（前4-8字节可能是长度/类型信息）
                if not mam_text:
                    for skip_bytes in [0, 2, 4, 6, 8]:
                        if skip_bytes >= len(mam_data):
                            break
                        try:
                            # 从skip_bytes位置开始解码
                            test_data = mam_data[skip_bytes:]
                            # 先尝试ASCII
                            test_text = test_data.decode('ascii', errors='ignore').strip('\x00 \t\n\r')
                            # 如果解码后包含可打印字符，使用它
                            if test_text and any(c.isprintable() for c in test_text):
                                # 提取连续的可打印字符
                                printable_text = ''.join(c for c in test_text if c.isprintable()).strip()
                                if printable_text and len(printable_text) >= 2:  # 至少2个字符才认为有效
                                    mam_text = printable_text
                                    logger.info(f"从偏移 {skip_bytes} 字节处解析到文本: {mam_text}")
                                    break
                        except Exception:
                            continue
                
                # 方法2: 如果方法1失败，尝试UTF-8解码
                if not mam_text:
                    for skip_bytes in [0, 2, 4, 6, 8]:
                        if skip_bytes >= len(mam_data):
                            break
                        try:
                            test_data = mam_data[skip_bytes:]
                            test_text = test_data.decode('utf-8', errors='ignore').strip('\x00 \t\n\r')
                            printable_text = ''.join(c for c in test_text if c.isprintable()).strip()
                            if printable_text and len(printable_text) >= 2:
                                mam_text = printable_text
                                logger.info(f"从偏移 {skip_bytes} 字节处(UTF-8)解析到文本: {mam_text}")
                                break
                        except Exception:
                            continue
                
                # 方法3: 如果还是失败，尝试查找ASCII字符串模式
                if not mam_text:
                    # 查找连续的可打印ASCII字符（至少3个）
                    import re
                    # 尝试不同的编码
                    for encoding in ['ascii', 'utf-8', 'latin-1']:
                        try:
                            decoded = mam_data.decode(encoding, errors='ignore')
                            # 查找连续的可打印字符序列（至少3个字符）
                            matches = re.findall(r'[a-zA-Z0-9\-_]{3,}', decoded)
                            if matches:
                                # 选择最长的匹配
                                mam_text = max(matches, key=len)
                                logger.info(f"通过正则表达式({encoding})解析到文本: {mam_text}")
                                break
                        except Exception:
                            continue
                
                # 方法4: 如果数据很短（<=16字节），可能是纯二进制标识符，尝试显示为hex
                if not mam_text and len(mam_data) <= 16:
                    mam_text = mam_data.hex().upper()
                    logger.info(f"数据太短，显示为十六进制: {mam_text}")
                
                # 将结果添加到返回字典
                import base64
                result["mam_data_base64"] = base64.b64encode(mam_data).decode('ascii')  # Base64编码的原始二进制数据
                result["mam_data_hex"] = mam_data.hex()  # 十六进制表示
                result["mam_data_text"] = mam_text if mam_text else ""  # 文本表示
                result["mam_data_length"] = len(mam_data)  # 数据长度
                result["mam_data_raw_hex"] = mam_data.hex()  # 原始十六进制（用于调试）
                
                # 根据属性ID解析特定字段
                if attribute_id == "0x0002":  # Serial Number
                    serial = None
                    
                    # 优先使用解析后的文本
                    if mam_text and len(mam_text) >= 2:
                        # 检查是否是有效的序列号格式（不是纯十六进制）
                        # 如果mam_text是十六进制字符串（全部是0-9A-F），可能不是有效的序列号
                        hex_chars_only = all(c in '0123456789ABCDEFabcdef' for c in mam_text.replace(' ', ''))
                        
                        # 如果包含字母（非十六进制）或长度合理，认为是有效序列号
                        if not hex_chars_only or (len(mam_text) <= 16 and any(c.isalpha() for c in mam_text)):
                            serial = mam_text.strip()
                            logger.info(f"解析到序列号: {serial}")
                        else:
                            # 如果是纯十六进制且长度较长，可能是二进制数据，尝试进一步解析
                            logger.debug(f"序列号数据是十六进制格式，尝试进一步解析...")
                            # 尝试从二进制数据中提取文本（跳过可能的头部）
                            for offset in [4, 6, 8]:
                                if offset < len(mam_data):
                                    try:
                                        test_data = mam_data[offset:]
                                        # 查找连续的可打印ASCII字符
                                        text_parts = []
                                        for byte in test_data:
                                            if 32 <= byte <= 126:  # 可打印ASCII范围
                                                text_parts.append(chr(byte))
                                            elif byte == 0:
                                                break  # 遇到空字节停止
                                        if text_parts:
                                            test_serial = ''.join(text_parts).strip()
                                            if len(test_serial) >= 2:
                                                serial = test_serial
                                                logger.info(f"从偏移{offset}字节处解析到序列号: {serial}")
                                                break
                                    except Exception:
                                        continue
                            
                            # 如果还是无法解析，记录警告
                            if not serial:
                                logger.warning(f"序列号属性数据无法解析为有效文本，原始数据(hex): {mam_data.hex()}")
                                # 如果数据看起来是空的或默认值，不返回序列号
                                known_empty_patterns = [
                                    '00028000080040000000000000',
                                    '00000000000000000000000000',
                                    '0002800008004000',
                                    '00000000'
                                ]
                                
                                if mam_data.hex().upper() in [p.upper() for p in known_empty_patterns]:
                                    logger.info("序列号数据为空或未设置（检测到已知的空值模式）")
                                    serial = ""
                                elif len(mam_data) <= 4:
                                    # 非常短的数据，可能是空值
                                    logger.info("序列号数据太短，可能是空值")
                                    serial = ""
                                else:
                                    # 返回十六进制表示作为最后手段（但仅在数据长度合理时）
                                    if len(mam_data) <= 16:
                                        serial = mam_data.hex().upper()
                                        logger.info(f"使用十六进制表示作为序列号: {serial}")
                                    else:
                                        logger.warning("序列号数据太长，可能是二进制数据，不返回")
                                        serial = ""
                    
                    result["serial_number"] = serial if serial else ""
                elif attribute_id == "0x0001":  # Manufacturer
                    manufacturer = mam_text.strip() if mam_text else ""
                    if manufacturer and len(manufacturer) >= 2:
                        result["manufacturer"] = manufacturer
                        logger.info(f"解析到制造商: {manufacturer}")
                    else:
                        logger.warning(f"制造商属性数据为空或无法解析，原始数据(hex): {mam_data.hex()}")
                elif attribute_id == "0x0009":  # Barcode
                    barcode = mam_text.strip() if mam_text else ""
                    if barcode and len(barcode) >= 2:
                        result["barcode"] = barcode
                        logger.info(f"解析到二维码: {barcode}")
                    else:
                        logger.warning(f"二维码属性数据为空或无法解析，原始数据(hex): {mam_data.hex()}")
                
                # 如果解析到文本数据，添加到mam_attributes字典
                # 注意：对于序列号，如果检测到空值模式，不应该添加到mam_attributes
                if mam_text:
                    # 对于序列号属性，检查是否是空值模式
                    if attribute_id == "0x0002" and result.get("serial_number") == "":
                        # 序列号是空值，不添加到mam_attributes
                        logger.debug("序列号为空值，不添加到mam_attributes字典")
                        result["mam_attributes"] = {}
                    else:
                        result["mam_attributes"] = {
                            attribute_id: mam_text
                        }
                        logger.info(f"MAM属性解析成功: {attribute_id} = {mam_text}")
                else:
                    logger.warning(f"MAM属性数据无法解析为文本，原始数据(hex): {mam_data.hex()}")
                    result["mam_attributes"] = {}
                    # 即使无法解析，也提供原始数据供用户查看
                    result["mam_data_parse_error"] = "无法解析为文本，请查看原始十六进制数据"
                    
            except Exception as e:
                logger.error(f"解析MAM属性数据失败: {e}", exc_info=True)
                result["mam_data_hex"] = mam_data.hex() if mam_data else ""
                result["error_detail"] = f"解析失败: {str(e)}"
        elif result.get("success") and not mam_data:
            # 命令成功但没有数据
            logger.warning(f"命令执行成功，但未读取到MAM属性数据")
            result["error_detail"] = "命令执行成功，但输出文件为空或无法读取"
        
        # 添加命令信息
        result["attribute_id"] = attribute_id
        result["partition"] = partition
        result["output_file"] = output_file
        
        return result
    
    async def write_mam_attribute_itdt(
        self,
        attribute_id: str,
        attribute_value: str,
        partition: int = 0,
        source_file: Optional[str] = None
    ) -> Dict[str, Any]:
        """使用ITDT写入MAM属性
        
        Args:
            partition: 分区号（0-3），默认0
            attribute_id: 属性标识符（如 "0x0002"）
            attribute_value: 要写入的属性值（字符串）
            source_file: 源文件路径，如果为None则创建临时文件
        
        Returns:
            包含操作结果的字典
        """
        logger.info(f"使用ITDT写入MAM属性 (分区: {partition}, 属性ID: {attribute_id}, 值: {attribute_value})...")
        
        # 验证分区范围
        if partition not in [0, 1, 2, 3]:
            raise ValueError(f"分区号必须是 0-3 之间的整数，当前值: {partition}")
        
        # 如果没有指定源文件，创建临时文件
        import tempfile
        if not source_file:
            temp_file = tempfile.NamedTemporaryFile(mode='wb', suffix='.bin', delete=False)
            source_file = temp_file.name
            # 将属性值写入文件（作为二进制数据）
            attribute_bytes = attribute_value.encode('utf-8')
            temp_file.write(attribute_bytes)
            temp_file.close()
            logger.info(f"创建临时文件: {source_file}, 大小: {len(attribute_bytes)} 字节")
        else:
            # 验证源文件存在
            if not os.path.exists(source_file):
                raise FileNotFoundError(f"源文件不存在: {source_file}")
        
        # 确保源文件路径使用正确的格式（Windows路径）
        source_file = os.path.normpath(source_file)
        
        # 构建命令
        cmd = [
            self.itdt_path,
            '-f', self.tape_drive,
            'writeattr',
            f'-p{partition}',
            f'-a{attribute_id}',
            f'-s{source_file}'
        ]
        
        result = await self.run_command(cmd, timeout=60, tool_type="ITDT")
        
        # 清理临时文件
        if not source_file or (source_file.startswith(tempfile.gettempdir()) and os.path.exists(source_file)):
            try:
                os.unlink(source_file)
                logger.info(f"已删除临时文件: {source_file}")
            except Exception as e:
                logger.warning(f"删除临时文件失败: {e}")
        
        if result.get("success"):
            logger.info(f"MAM属性写入成功: {attribute_id} = {attribute_value}")
            result["attribute_id"] = attribute_id
            result["partition"] = partition
            result["attribute_value"] = attribute_value
        else:
            logger.error(f"MAM属性写入失败: {result.get('stderr', '未知错误')}")
            result["error_detail"] = result.get('stderr') or result.get('stdout') or "写入失败"
        
        return result
    
    # ===== LTFS 命令 =====
    async def list_drives_ltfs(self) -> Dict[str, Any]:
        """列出LTFS驱动器"""
        logger.info("列出LTFS驱动器...")
        tool_path = os.path.join(self.ltfs_tools_dir, self.ltfs_tools['drives'])
        
        # 检查工具是否存在
        if not os.path.exists(tool_path):
            logger.error(f"LTFS工具不存在: {tool_path}")
            return {
                "success": False,
                "returncode": -1,
                "stdout": "",
                "stderr": f"工具文件不存在: {tool_path}",
                "error": f"工具文件不存在: {tool_path}",
                "drives": [],
                "drive_count": 0
            }
        
        cmd = [tool_path]
        result = await self.run_command(cmd, timeout=30, tool_type="LTFS", working_dir=self.ltfs_tools_dir)
        
        # 初始化驱动器列表
        result.setdefault("drives", [])
        result.setdefault("drive_count", 0)
        
        # 解析输出，提取驱动器信息
        if result.get("success") and result.get("stdout"):
            drives = []
            lines = result["stdout"].strip().split('\n')
            
            # 跳过表头和分隔线
            for line in lines[2:]:  # 第1行是表头，第2行是分隔线
                line = line.strip()
                if not line:
                    continue
                
                # 解析每一行：Assigned   Address      Serial                   Status
                parts = line.split()
                if len(parts) >= 4:
                    drives.append({
                        "assigned": parts[0],
                        "address": parts[1],
                        "serial": parts[2],
                        "status": parts[3]
                    })
            
            result["drives"] = drives
            result["drive_count"] = len(drives)
        elif result.get("success") and not result.get("stdout"):
            # 命令执行成功但没有输出
            logger.warning("LTFS命令执行成功，但输出为空")
            result["drives"] = []
            result["drive_count"] = 0
        
        return result
    
    async def load_tape_ltfs(self, drive_id: str) -> Dict[str, Any]:
        """加载磁带到驱动器（物理装载）"""
        logger.info(f"LTFS物理加载磁带到驱动器 {drive_id}...")
        tool_path = os.path.join(self.ltfs_tools_dir, self.ltfs_tools['load'])
        cmd = [tool_path, drive_id]
        return await self.run_command(cmd, timeout=60, tool_type="LTFS", working_dir=self.ltfs_tools_dir)
    
    async def eject_tape_ltfs(self, drive_id: str) -> Dict[str, Any]:
        """弹出磁带（物理卸载）"""
        logger.info(f"LTFS物理弹出磁带从驱动器 {drive_id}...")
        tool_path = os.path.join(self.ltfs_tools_dir, self.ltfs_tools['eject'])
        cmd = [tool_path, drive_id]
        return await self.run_command(cmd, timeout=60, tool_type="LTFS", working_dir=self.ltfs_tools_dir)
    
    async def format_tape_ltfs(self, drive_letter: Optional[str] = None, volume_label: Optional[str] = None, 
                               serial: Optional[str] = None, eject_after: bool = False) -> Dict[str, Any]:
        """格式化磁带为LTFS格式 (使用 LtfsCmdFormat.exe)
        
        Args:
            drive_letter: 盘符（大写，不带冒号，如 "O"），如果为None则使用配置的盘符
            volume_label: 卷标名称
            serial: 序列号（6位大写字母数字）
            eject_after: 格式化后是否弹出
        
        Returns:
            操作结果字典
        """
        # 使用配置的盘符或提供的盘符
        if not drive_letter:
            drive_letter = self.drive_letter
        
        # 确保盘符不带冒号
        if drive_letter.endswith(':'):
            drive_letter = drive_letter[:-1]
        
        logger.info(f"LTFS格式化磁带 (盘符: {drive_letter}, 卷标: {volume_label})...")
        tool_path = os.path.join(self.ltfs_tools_dir, self.ltfs_tools['format'])
        
        # LtfsCmdFormat.exe的参数是盘符（如 "O"），不是驱动器地址
        cmd = [tool_path, drive_letter]
        
        # 添加序列号参数
        if serial and len(serial) == 6 and serial.isalnum() and serial.isupper():
            cmd.append(f"/S:{serial}")
        
        # 添加卷标参数
        if volume_label:
            cmd.append(f"/N:{volume_label}")
        
        # 添加格式化后弹出参数
        if eject_after:
            cmd.append("/E")
        
        return await self.run_command(cmd, timeout=3600, tool_type="LTFS", working_dir=self.ltfs_tools_dir)
    
    async def format_tape_mkltfs(self, device_id: str, volume_label: Optional[str] = None) -> Dict[str, Any]:
        """使用 mkltfs.exe 格式化磁带（备用方式）"""
        logger.info(f"使用mkltfs格式化磁带 (设备: {device_id}, 卷标: {volume_label})...")
        tool_path = os.path.join(self.ltfs_tools_dir, self.ltfs_tools['mkltfs'])
        cmd = [tool_path, '-d', device_id, '--force']
        
        # 添加卷标参数
        if volume_label:
            cmd.extend(['--volume-name', volume_label])
        
        return await self.run_command(cmd, timeout=3600, tool_type="LTFS", working_dir=self.ltfs_tools_dir)
    
    async def assign_tape_ltfs(self, drive_id: str) -> Dict[str, Any]:
        """分配磁带给驱动器并挂载到指定盘符"""
        logger.info(f"分配磁带给驱动器 {drive_id} 并挂载到 {self.drive_letter}: 盘...")
        tool_path = os.path.join(self.ltfs_tools_dir, self.ltfs_tools['assign'])
        # LtfsCmdAssign 使用：驱动器ID + 盘符(带冒号)
        # 格式：LtfsCmdAssign.exe 0.0.24.0 O:
        drive_letter_with_colon = f"{self.drive_letter}:" if not self.drive_letter.endswith(':') else self.drive_letter
        cmd = [tool_path, drive_id, drive_letter_with_colon]
        return await self.run_command(cmd, timeout=60, tool_type="LTFS", working_dir=self.ltfs_tools_dir)
    
    async def unassign_tape_ltfs(self, drive_id: str) -> Dict[str, Any]:
        """从驱动器卸载磁带"""
        logger.info(f"从驱动器 {drive_id} 卸载磁带...")
        tool_path = os.path.join(self.ltfs_tools_dir, self.ltfs_tools['unassign'])
        cmd = [tool_path, drive_id]
        return await self.run_command(cmd, timeout=60, tool_type="LTFS", working_dir=self.ltfs_tools_dir)
    
    async def check_tape_ltfs(self, drive_id: str) -> Dict[str, Any]:
        """检查磁带完整性"""
        logger.info(f"检查磁带完整性 (驱动器: {drive_id})...")
        tool_path = os.path.join(self.ltfs_tools_dir, self.ltfs_tools['check'])
        cmd = [tool_path, drive_id]
        return await self.run_command(cmd, timeout=7200, tool_type="LTFS", working_dir=self.ltfs_tools_dir)  # 2小时超时
    
    async def rollback_tape_ltfs(self, drive_id: str) -> Dict[str, Any]:
        """回滚到上一个一致性点"""
        logger.info(f"回滚磁带 (驱动器: {drive_id})...")
        tool_path = os.path.join(self.ltfs_tools_dir, self.ltfs_tools['rollback'])
        cmd = [tool_path, drive_id]
        return await self.run_command(cmd, timeout=300, tool_type="LTFS", working_dir=self.ltfs_tools_dir)
    
    async def unformat_tape_ltfs(self, drive_id: str) -> Dict[str, Any]:
        """取消格式化（清除LTFS卷标）"""
        logger.info(f"取消LTFS格式化 (驱动器: {drive_id})...")
        tool_path = os.path.join(self.ltfs_tools_dir, self.ltfs_tools['unformat'])
        cmd = [tool_path, drive_id]
        return await self.run_command(cmd, timeout=300, tool_type="LTFS", working_dir=self.ltfs_tools_dir)
    
    # ===== 卷信息和卷标读取 =====
    async def get_volume_info(self) -> Dict[str, Any]:
        """使用fsutil获取卷信息（包括卷标）"""
        # fsutil需要带冒号的盘符
        drive_with_colon = f"{self.drive_letter}:" if not self.drive_letter.endswith(':') else self.drive_letter
        logger.info(f"使用fsutil获取 {drive_with_colon} 卷信息...")
        
        # 检查驱动器是否存在
        if not os.path.exists(drive_with_colon):
            return {
                "success": False,
                "error": f"驱动器 {drive_with_colon} 不存在或未分配"
            }
        
        cmd = f"fsutil fsinfo volumeinfo {drive_with_colon}"
        proc = await asyncio.create_subprocess_shell(
            cmd,
            stdout=asyncio.subprocess.PIPE,
            stderr=asyncio.subprocess.PIPE,
            stdin=asyncio.subprocess.DEVNULL  # 防止子进程等待输入导致阻塞
        )
        
        try:
            # 添加超时设置（10秒），防止fsutil命令卡住
            stdout, stderr = await asyncio.wait_for(
                proc.communicate(),
                timeout=10
            )
        except asyncio.TimeoutError:
            logger.warning(f"读取卷信息超时（10秒），尝试终止进程...")
            try:
                if proc.returncode is None:
                    proc.kill()
                    await asyncio.wait_for(proc.wait(), timeout=2)
            except Exception as kill_error:
                logger.warning(f"终止fsutil进程时出错: {kill_error}")
            return {
                "success": False,
                "error": "读取卷信息超时（fsutil命令执行超过10秒）"
            }
        
        stdout_str = stdout.decode('gbk', errors='ignore') if stdout else ""
        
        if proc.returncode == 0:
            # 解析输出
            lines = stdout_str.split('\n')
            volume_info = {}
            
            for line in lines:
                line = line.strip()
                if ':' in line:
                    key, value = line.split(':', 1)
                    volume_info[key.strip()] = value.strip()
            
            # 提取关键信息
            result = {
                "success": True,
                "volume_info": volume_info,
                "volume_name": volume_info.get('卷名', volume_info.get('Volume Name', '')),
                "serial_number": volume_info.get('卷序列号', volume_info.get('Volume Serial Number', '')),
                "file_system": volume_info.get('文件系统名', volume_info.get('File System Name', '')),
                "raw_output": stdout_str
            }
            
            logger.info(f"成功读取卷标: {result['volume_name']}, 序列号: {result['serial_number']}")
            return result
        else:
            return {
                "success": False,
                "error": "无法获取卷信息"
            }
    
    async def read_tape_label_windows(self) -> Dict[str, Any]:
        """读取Windows磁带卷标（使用fsutil）"""
        logger.info("读取磁带卷标...")
        
        # 使用fsutil读取卷信息
        volume_info_result = await self.get_volume_info()
        
        if not volume_info_result.get("success"):
            return volume_info_result
        
        return {
            "success": True,
            "volume_name": volume_info_result.get("volume_name", ""),
            "serial_number": volume_info_result.get("serial_number", ""),
            "file_system": volume_info_result.get("file_system", ""),
            "volume_info": volume_info_result.get("volume_info", {}),
            "raw_output": volume_info_result.get("raw_output", "")
        }
    
    # ===== 组合流程 =====
    async def mount_tape_complete(self, drive_id: str, volume_label: Optional[str] = None, 
                                   format_tape: bool = True) -> Dict[str, Any]:
        """完整的磁带挂载流程：加载->格式化(可选)->分配
        
        Args:
            drive_id: 驱动器地址（如 0.0.24.0）- 用于Load/Eject/Assign操作
            volume_label: 卷标名称
            format_tape: 是否格式化磁带
        
        Returns:
            操作结果字典
        """
        logger.info(f"开始完整磁带挂载流程 (驱动器: {drive_id}, 格式化: {format_tape})...")
        
        steps = []
        
        # 1. 物理加载磁带
        logger.info("步骤1: 物理加载磁带...")
        load_result = await self.load_tape_ltfs(drive_id)
        steps.append({"step": "load", "success": load_result.get("success", False), "message": "物理加载磁带"})
        
        if not load_result.get("success"):
            return {
                "success": False,
                "error": "物理加载失败",
                "steps": steps
            }
        
        # 2. 分配到盘符（必须在格式化之前）
        logger.info(f"步骤2: 分配并挂载到 {self.drive_letter} 盘...")
        assign_result = await self.assign_tape_ltfs(drive_id)
        steps.append({"step": "assign", "success": assign_result.get("success", False), "message": f"分配到 {self.drive_letter} 盘"})
        
        if not assign_result.get("success"):
            return {
                "success": False,
                "error": "分配失败",
                "steps": steps
            }
        
        # 3. 格式化磁带（如果需要）
        if format_tape:
            logger.info("步骤3: 格式化磁带...")
            if not volume_label:
                volume_label = self._generate_default_label()
            
            # LtfsCmdFormat使用盘符，不是驱动器地址
            format_result = await self.format_tape_ltfs(
                drive_letter=self.drive_letter,
                volume_label=volume_label
            )
            steps.append({"step": "format", "success": format_result.get("success", False), "message": f"格式化磁带 (卷标: {volume_label})"})
            
            if not format_result.get("success"):
                return {
                    "success": False,
                    "error": "格式化失败",
                    "steps": steps
                }
        
        logger.info("完整挂载流程成功！")
        return {
            "success": True,
            "message": f"磁带已成功挂载到 {self.drive_letter} 盘",
            "steps": steps
        }
    
    async def unmount_tape_complete(self, drive_id: str) -> Dict[str, Any]:
        """完整的磁带卸载流程：卸载->弹出"""
        logger.info(f"开始完整磁带卸载流程 (驱动器: {drive_id})...")
        
        steps = []
        
        # 1. 从盘符卸载
        logger.info(f"步骤1: 从 {self.drive_letter} 盘卸载...")
        unassign_result = await self.unassign_tape_ltfs(drive_id)
        steps.append({"step": "unassign", "success": unassign_result.get("success", False), "message": "从盘符卸载"})
        
        # 2. 物理弹出
        logger.info("步骤2: 物理弹出磁带...")
        eject_result = await self.eject_tape_ltfs(drive_id)
        steps.append({"step": "eject", "success": eject_result.get("success", False), "message": "物理弹出磁带"})
        
        overall_success = all(step["success"] for step in steps)
        
        return {
            "success": overall_success,
            "message": "磁带已完全卸载" if overall_success else "卸载过程部分失败",
            "steps": steps
        }
    
    def _generate_default_label(self) -> str:
        """生成默认卷标（基于日期，包含年份）"""
        now = datetime.now()
        return f"BK{now.strftime('%Y%m%d_%H%M')}"  # %Y为4位年份
    
    # ===== 工具可用性检查 =====
    def check_tools_availability(self) -> Dict[str, Any]:
        """检查工具可用性"""
        result = {
            "itdt_available": os.path.exists(self.itdt_path),
            "itdt_path": self.itdt_path,
            "itdt_commands": self._get_itdt_commands(),
            "ltfs_tools_dir": self.ltfs_tools_dir,
            "ltfs_tools": {}
        }
        
        for tool_name, tool_file in self.ltfs_tools.items():
            tool_path = os.path.join(self.ltfs_tools_dir, tool_file)
            result["ltfs_tools"][tool_name] = {
                "available": os.path.exists(tool_path),
                "path": tool_path
            }
        
        return result
    
    def _get_itdt_commands(self) -> Dict[str, list]:
        """获取ITDT可用命令列表（基于程序实际实现）"""
        return {
            "implemented": [
                {"cmd": "tur", "desc": "测试设备就绪", "method": "test_unit_ready()"},
                {"cmd": "scan", "desc": "扫描连接的设备", "method": "scan_devices()"},
                {"cmd": "load", "desc": "加载磁带", "method": "load()"},
                {"cmd": "unload", "desc": "卸载磁带", "method": "unload()"},
                {"cmd": "rewind", "desc": "回绕磁带", "method": "rewind()"},
                {"cmd": "erase", "desc": "完全擦除磁带", "method": "erase(short=False)"},
                {"cmd": "erase -short", "desc": "快速擦除磁带", "method": "erase(short=True)"},
                {"cmd": "qrypos", "desc": "查询磁带位置", "method": "query_position()"},
                {"cmd": "qrypart", "desc": "查询分区信息", "method": "query_partition()"},
                {"cmd": "weof", "desc": "写入文件标记", "method": "write_filemark()"},
                {"cmd": "tapeusage", "desc": "获取磁带使用统计", "method": "tape_usage()"}
            ],
            "general": [
                {"cmd": "inq", "desc": "查询设备信息（标准）"},
                {"cmd": "inqj", "desc": "查询设备信息(JSON)"},
                {"cmd": "devinfo", "desc": "获取设备详细信息"},
                {"cmd": "vpd", "desc": "显示重要产品数据"},
                {"cmd": "qrypath", "desc": "显示设备和路径信息"},
                {"cmd": "reqsense", "desc": "请求传感器数据"},
                {"cmd": "logpage", "desc": "检索日志页"},
                {"cmd": "logsense", "desc": "获取所有日志页"}
            ],
            "advanced": [
                {"cmd": "chgpart", "desc": "切换分区"},
                {"cmd": "formattape", "desc": "格式化磁带"},
                {"cmd": "getparms", "desc": "获取驱动器参数"},
                {"cmd": "setparm", "desc": "设置驱动器参数"},
                {"cmd": "qrytemp", "desc": "查询温度"},
                {"cmd": "fsf/bsf", "desc": "跳过文件标记"},
                {"cmd": "seod", "desc": "定位到数据结束"},
                {"cmd": "list", "desc": "列出磁带内容"},
                {"cmd": "read", "desc": "读取文件"},
                {"cmd": "readattr", "desc": "读取MAM属性"}
            ]
        }


# 全局工具管理器实例
tape_tools_manager = TapeToolsManager()

