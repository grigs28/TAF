#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
磁带处理模块
Tape Handler Module

LTFS 写入流程：
1. 检查磁带是否在驱动器中
2. 尝试挂载 LTFS
   ├─ 成功 → 检查磁带内容
   └─ 失败 → 尝试格式化为 LTFS → 重新挂载
3. 复制文件到 LTFS 挂载点
4. 验证复制成功
5. 删除源文件
"""

import asyncio
import logging
import shutil
import subprocess
import time
from datetime import datetime
from pathlib import Path
from typing import Optional, Tuple

from models.backup import BackupSet
from tape.tape_manager import TapeManager
from tape.tape_cartridge import TapeCartridge, TapeStatus
from utils.log_utils import log_operation
from models.system_log import OperationType

logger = logging.getLogger(__name__)

# LTFS 工具路径
LTFS_BIN = "/usr/local/bin/ltfs"
MKLTFS_BIN = "/usr/local/bin/mkltfs"

# 默认 LTFS 挂载点
DEFAULT_LTFS_MOUNT = "/mnt/ltfs"


async def _copy_with_buffer(source_file: Path, target_file: Path,
                            buffer_size: int = 4 * 1024 * 1024,
                            on_progress=None):
    """以指定缓冲区复制文件（等价 shutil.copy2，但缓冲更大）

    copy2 默认约 64KB 缓冲，向 LTFS(FUSE) 写入时产生大量用户态/内核往返。
    改为 4MB 缓冲后往返次数降至 1/64。copystat 保留 copy2 的元数据语义。

    on_progress: 可选回调，每写完一块调用一次 `on_progress(已写字节数)`，
                 用于对外暴露实时写入进度（卡片显示磁带速度）。
                 注意：它在复制线程里被调用，回调实现须轻量且线程安全。
    """
    def _do_copy():
        with open(source_file, 'rb') as fsrc, open(target_file, 'wb') as fdst:
            if on_progress is None:
                # 快路径：无需进度时不额外开销
                shutil.copyfileobj(fsrc, fdst, buffer_size)
            else:
                written = 0
                while True:
                    chunk = fsrc.read(buffer_size)
                    if not chunk:
                        break
                    fdst.write(chunk)
                    written += len(chunk)
                    try:
                        on_progress(written)
                    except Exception:
                        pass       # 进度回调失败不得影响复制
        shutil.copystat(source_file, target_file)

    await asyncio.to_thread(_do_copy)


class TapeHandler:
    """磁带处理器 - 使用 LTFS 挂载方式写入"""

    def __init__(self, tape_manager: TapeManager = None, settings=None, dingtalk_notifier=None):
        """初始化磁带处理器

        Args:
            tape_manager: 磁带管理器对象
            settings: 系统设置对象
            dingtalk_notifier: 钉钉通知器
        """
        self.tape_manager = tape_manager
        self.settings = settings
        self.dingtalk_notifier = dingtalk_notifier
        self._ltfs_mounted = False
        self._ltfs_mount_point = None
        self._ltfs_process = None
        # 磁带写入实时状态（供任务卡片显示"正在写哪个文件、目前多快"）：
        # 由 write_to_tape_drive 维护，_copy_with_buffer 的分块回调持续更新已写字节。
        self._tape_write_file: Optional[str] = None     # 当前正在写的文件名
        self._tape_write_bytes: int = 0                 # 当前文件已写字节
        self._tape_write_total: int = 0                 # 当前文件总字节
        self._tape_write_start: Optional[float] = None  # 当前文件开始时间（time.time()）
        # 本次任务的累计写带统计（用于卡片显示"平均写带速度"）
        self._tape_cum_bytes: int = 0                   # 累计写入字节
        self._tape_cum_seconds: float = 0.0             # 累计实际写带耗时（不含文件间空档）
        self._tape_first_write_start: Optional[float] = None  # 本任务第一次写带的起始时间

    def reset_tape_write_stats(self) -> None:
        """新任务开始时清零累计写带统计（平均写带速度的分母/分子）。"""
        self._tape_cum_bytes = 0
        self._tape_cum_seconds = 0.0
        self._tape_first_write_start = None

    def _get_tape_device(self) -> str:
        """获取磁带设备路径（用于 mt 命令）"""
        return getattr(self.settings, 'TAPE_DRIVE_LETTER', '/dev/nst0')

    def _get_ltfs_device(self) -> str:
        """获取 LTFS 挂载使用的设备路径（SCSI generic 设备）

        LTFS 需要 SCSI generic 设备（/dev/sg*），而不是 tape 设备（/dev/nst*）。
        优先通过 sysfs 从磁带机动态解析 sg 节点——/dev/sgN 编号会随重启漂移，
        配置中的 LTFS_DEVICE_PATH 可能是过期值（曾因此导致格式化失败）。
        解析失败时才回退到配置值。
        """
        from utils.ltfs_ops import resolve_ltfs_sg_device

        resolved = resolve_ltfs_sg_device(self.settings, self._get_tape_device())
        ltfs_device = getattr(self.settings, 'LTFS_DEVICE_PATH', None)
        if resolved:
            if ltfs_device and ltfs_device != resolved:
                logger.warning(
                    f"[LTFS] 配置 LTFS_DEVICE_PATH={ltfs_device} 与磁带机实际 sg 设备 "
                    f"{resolved} 不一致，使用动态解析结果（建议更新配置）"
                )
            return resolved

        # 动态解析失败，回退到配置值
        if ltfs_device:
            return ltfs_device

        return self._get_tape_device()

    def _get_ltfs_mount_point(self) -> Path:
        """获取 LTFS 挂载点"""
        mount_point = getattr(self.settings, 'LTFS_MOUNT_POINT', DEFAULT_LTFS_MOUNT)
        return Path(mount_point)

    async def _run_command(self, cmd: list, timeout: int = 300, check: bool = False) -> subprocess.CompletedProcess:
        """异步运行命令"""
        logger.debug(f"执行命令: {' '.join(cmd)}")
        result = await asyncio.to_thread(
            subprocess.run,
            cmd,
            capture_output=True,
            text=True,
            timeout=timeout
        )
        if check and result.returncode != 0:
            raise RuntimeError(f"命令失败: {' '.join(cmd)}\n{result.stderr}")
        return result

    async def check_tape_in_drive(self) -> Tuple[bool, str]:
        """检查磁带是否在驱动器中

        Returns:
            (是否在驱动器中, 状态信息)
        """
        tape_device = self._get_tape_device()
        try:
            result = await self._run_command(['mt', '-f', tape_device, 'status'], timeout=30)
            output = result.stdout + result.stderr

            if 'ONLINE' in output:
                logger.info("[LTFS] 磁带已加载")
                return True, "磁带已加载"
            elif 'DR_OPEN' in output or 'No tape' in output:
                logger.warning("[LTFS] 驱动器中没有磁带")
                return False, "驱动器中没有磁带"
            else:
                # 介质在驱动器里但没有加载到读写通路（如停服时 mt offline 后
                # 整机重启，驱动器上电不会自动 load）。尝试 mt load 兜底。
                logger.warning(f"[LTFS] 磁带状态未知，尝试 mt load: {output[:100]}")
                loaded, msg = await self._load_tape_if_needed(tape_device)
                if loaded:
                    return True, "磁带已加载（mt load）"
                return False, msg
        except Exception as e:
            logger.error(f"[LTFS] 检查磁带状态失败: {e}")
            return False, str(e)

    async def _load_tape_if_needed(self, tape_device: str, timeout: float = 120.0) -> Tuple[bool, str]:
        """介质在驱动器内但未加载时，发送 mt load 并等待 ONLINE。

        LTO 驱动器上电时只会恢复"断电前正在加载"的介质；已经 unload 过的
        磁带必须收到显式 LOAD 命令。生产换带时驱动器会自动加载，所以平时
        碰不到；但"停服时 eject → 重启"后必须兜底，否则挂载直接失败。

        Args:
            tape_device: 磁带设备（如 /dev/nst0）
            timeout: 等待 ONLINE 的最长秒数

        Returns:
            (是否加载成功, 信息)
        """
        logger.info("[LTFS] 磁带在驱动器内但未加载，执行 mt load...")
        # 2026-10-25 生产故障：mt load 偶发 Device busy（另一操作占用设备）
        # 或超时，但**驱动器仍会在后台继续加载**（实测：mt load 被超时杀掉后
        # dd 能直接读到卷标、rewind 瞬间完成）。因此：
        #   ① busy 时重试 2 次
        #   ② 无论 mt load 返回值如何，都继续轮询 status 等 ONLINE
        from utils.tape_device_lock import is_tape_busy_error
        load_err = ''
        for attempt in range(1, 4):
            try:
                result = await self._run_command(['mt', '-f', tape_device, 'load'], timeout=180)
                msg = (result.stderr or result.stdout or '').strip()
                if result.returncode == 0:
                    load_err = ''
                    break
                load_err = msg
                if is_tape_busy_error(msg) and attempt < 3:
                    logger.warning(f"[LTFS] mt load 设备忙（第 {attempt} 次），{5 * attempt} 秒后重试")
                    await asyncio.sleep(5 * attempt)
                    continue
                logger.warning(f"[LTFS] mt load 返回失败（继续等待 ONLINE）: {msg[:120]}")
                break
            except Exception as e:
                load_err = str(e)
                logger.warning(f"[LTFS] mt load 异常（第 {attempt} 次，继续等待 ONLINE）: {e}")
                await asyncio.sleep(3)

        deadline = time.time() + timeout
        while time.time() < deadline:
            await asyncio.sleep(3)
            try:
                st = await self._run_command(['mt', '-f', tape_device, 'status'], timeout=30)
                out = st.stdout + st.stderr
            except Exception as e:
                logger.warning(f"[LTFS] 查询加载状态失败: {e}")
                continue
            if 'ONLINE' in out:
                logger.info("[LTFS] 磁带加载完成（ONLINE）")
                return True, "磁带已加载"
            if 'DR_OPEN' in out or 'No tape' in out:
                return False, "mt load 后驱动器中没有磁带"

        logger.warning(f"[LTFS] mt load 后 {int(timeout)} 秒仍未 ONLINE")
        return False, f"mt load 后 {int(timeout)} 秒仍未 ONLINE"

    async def reset_tape_device(self) -> bool:
        """重置磁带设备（清除预留）"""
        tape_device = self._get_tape_device()
        try:
            # 使用 sg_reset 重置设备
            sg_device = tape_device.replace('/dev/nst', '/dev/sg').replace('/dev/st', '/dev/sg')
            if sg_device == tape_device:
                sg_device = '/dev/sg2'  # 默认

            result = await self._run_command(['sg_reset', '-d', '-N', sg_device], timeout=30, check=False)
            await asyncio.sleep(2)

            # 回绕磁带
            await self._run_command(['mt', '-f', tape_device, 'rewind'], timeout=60, check=False)

            logger.info("[LTFS] 磁带设备已重置")
            return True
        except Exception as e:
            logger.warning(f"[LTFS] 重置磁带设备失败: {e}")
            return False

    async def _format_and_remount(self, existing_label: str = "Unknown") -> Tuple[bool, str]:
        """格式化磁带并重新挂载（内部辅助方法）

        统一处理"格式化→重新挂载"的流程，避免代码重复

        如果磁带没有有效卷标（Unknown），将自动生成符合规则的卷标和序列号，
        并在格式化成功后自动在数据库中注册。

        Args:
            existing_label: 现有磁带卷标 (Unknown 表示无有效卷标)

        Returns:
            (是否成功, 信息)
        """
        logger.info("[LTFS] 开始格式化并重新挂载...")

        # 卸载
        await self.unmount_ltfs()

        # 重置设备
        await self.reset_tape_device()

        # 始终生成标准的 TP 格式卷标（无论现有标签是什么）
        volume_name = "BACKUP"
        serial = None

        logger.info("[LTFS] 生成标准磁带标签...")
        try:
            from backup.tape_label_generator import generate_tape_label_and_serial
            label_info = await generate_tape_label_and_serial()
            volume_name = label_info.label
            serial = label_info.serial_number
            logger.info(f"[LTFS] 自动生成标签: {volume_name}, 序列号: {serial}")
        except Exception as e:
            logger.warning(f"[LTFS] 自动生成标签失败: {e}，使用默认值")
            volume_name = "BACKUP"
            serial = None

        # 格式化
        success, format_msg = await self.format_as_ltfs(volume_name=volume_name, serial=serial)
        if not success:
            return False, format_msg

        # 等待格式化完成（LTO磁带需要更长时间）
        # 参考测试程序：格式化后等待至少20秒让磁带准备好
        await asyncio.sleep(20)

        # 重新挂载
        is_ltfs, msg = await self.check_ltfs_format()

        if is_ltfs:
            logger.info("[LTFS] ✅ 格式化并重新挂载成功")
            # 注：数据库注册已在 execute_backup_task() 格式化成功后完成，此处不再重复注册

            return True, "格式化并重新挂载成功"

        return False, f"格式化后挂载失败: {msg}"

    async def check_ltfs_format(self) -> Tuple[bool, str]:
        """检查磁带是否为 LTFS 格式

        尝试挂载来判断是否为 LTFS 格式

        Returns:
            (是否为 LTFS 格式, 信息)
        """
        tape_device = self._get_ltfs_device()  # 使用 SCSI generic 设备
        mount_point = self._get_ltfs_mount_point()

        logger.info(f"[LTFS] 检查磁带是否为 LTFS 格式... (设备: {tape_device})")

        # 创建挂载点
        mount_point.mkdir(parents=True, exist_ok=True)

        # ===== 挂载前清理：卸载可能损坏的 FUSE 挂载 =====
        if mount_point.is_mount():
            # 检查挂载点是否可访问
            try:
                list(mount_point.iterdir())
                logger.info(f"[LTFS] 目录已挂载且可访问: {mount_point}")
                # 挂载点正常，检查是否可以读取
                self._ltfs_mounted = True
                self._ltfs_mount_point = mount_point
                return True, "LTFS 格式，已挂载"
            except OSError as e:
                # 挂载点损坏（Transport endpoint is not connected）
                logger.warning(f"[LTFS] 挂载点损坏，尝试卸载: {e}")
                try:
                    from utils.ltfs_ops import cleanup_mount
                    await cleanup_mount(mount_point)
                    logger.info(f"[LTFS] 已卸载损坏的挂载点")
                except Exception as unmount_err:
                    logger.warning(f"[LTFS] 卸载失败: {unmount_err}")

        # 清空挂载点目录（FUSE 要求挂载点为空）
        try:
            import shutil
            for item in mount_point.iterdir():
                if item.is_dir():
                    shutil.rmtree(item)
                else:
                    item.unlink()
            logger.info(f"[LTFS] 已清空挂载点目录: {mount_point}")
        except Exception as e:
            logger.warning(f"[LTFS] 清空挂载点目录失败: {e}")

        # 尝试挂载
        try:
            process = await asyncio.create_subprocess_exec(
                # max_write+big_writes：让大缓冲写以完整 FUSE 消息下发，
                # 避免被默认 128KB 上限拆碎（与 _copy_with_buffer 的 4MB 缓冲配套）
                LTFS_BIN, '-o', f'devname={tape_device},max_write=1048576,big_writes', str(mount_point),
                stdout=asyncio.subprocess.PIPE,
                stderr=asyncio.subprocess.PIPE
            )

            # 等待挂载
            await asyncio.sleep(5)

            # 检查是否挂载成功 - 使用 os.path.ismount 更可靠
            if mount_point.is_mount():
                self._ltfs_mounted = True
                self._ltfs_mount_point = mount_point
                self._ltfs_process = process
                logger.info("[LTFS] 磁带是 LTFS 格式，挂载成功")
                return True, "LTFS 格式，已挂载"

            # 检查进程状态
            if process.returncode is None:
                # 进程还在运行，LTO磁带需要更长时间才能挂载
                # 等待更长时间后再检查（格式化后的LTO磁带可能需要30-60秒）
                await asyncio.sleep(30)  # 增加等待时间到30秒
                # 再次检查挂载状态
                if mount_point.is_mount():
                    self._ltfs_mounted = True
                    self._ltfs_mount_point = mount_point
                    self._ltfs_process = process
                    logger.info("[LTFS] 磁带是 LTFS 格式，挂载成功（延迟检测）")
                    return True, "LTFS 格式，已挂载"

                # 进程还在运行但未挂载，安全等待进程退出（不 kill）
                error_msg = "挂载超时或失败"
                try:
                    # 尝试读取进程输出
                    stdout, stderr = await asyncio.wait_for(process.communicate(), timeout=5)
                    error_msg = stderr.decode('utf-8', errors='ignore')[:500] if stderr else "未知错误"
                except asyncio.TimeoutError:
                    error_msg = "挂载超时"
                    # 不终止进程，等 LTFS 自行完成写入
                    logger.warning("[LTFS] 挂载超时，等待 LTFS 进程自行退出（不强制终止）...")
                    try:
                        await asyncio.wait_for(process.wait(), timeout=600)
                        logger.info("[LTFS] 进程已自行退出")
                        # 采集进程输出便于诊断挂载失败原因
                        try:
                            _out, _err = await asyncio.wait_for(process.communicate(), timeout=5)
                            if _err:
                                logger.warning(f"[LTFS] 挂载进程错误输出: {_err.decode('utf-8', errors='ignore')[:400]}")
                        except Exception:
                            pass
                    except asyncio.TimeoutError:
                        logger.warning("[LTFS] 进程 600s 后仍未退出，不再等待（进程仍在后台运行）")

                # 检查进程是否已退出
                if process.returncode is None:
                    logger.warning("[LTFS] 进程仍在后台运行，不强制终止")

                # 检查常见错误
                if 'mountpoint is not empty' in error_msg.lower():
                    logger.error(f"[LTFS] 挂载失败: 挂载点目录不为空")
                    return False, "挂载失败: 挂载点目录不为空"

                logger.warning(f"[LTFS] 挂载失败: {error_msg}")
                return False, f"挂载失败: {error_msg}"

            # 挂载失败，读取错误信息
            # 注意：communicate() 返回的是 **bytes**，必须先 decode 再做字符串匹配，
            # 否则下面 `'LTFS11005' in error_msg` 会抛
            # TypeError: a bytes-like object is required, not 'str'
            # ——该异常会被外层 except 吞成"检查失败"，使"磁带不是 LTFS"这一
            # 正常判定退化为异常路径（配合重试才没误触发格式化）。
            _, stderr = await process.communicate()
            error_msg = stderr.decode('utf-8', errors='ignore')[:200] if stderr else "未知错误"

            if 'LTFS11005' in error_msg or 'not formatted' in error_msg.lower():
                logger.info("[LTFS] 磁带不是 LTFS 格式")
                return False, "不是 LTFS 格式"

            logger.warning(f"[LTFS] 挂载失败: {error_msg}")
            return False, f"挂载失败: {error_msg}"

        except Exception as e:
            logger.error(f"[LTFS] 检查 LTFS 格式失败: {e}")
            return False, str(e)

    async def format_as_ltfs(self, volume_name: str = "BACKUP", serial: str = None) -> Tuple[bool, str]:
        """格式化磁带为 LTFS 格式

        Args:
            volume_name: 卷名
            serial: 序列号（必须是6个字符，如果不提供则自动生成）

        Returns:
            (是否成功, 信息)
        """
        tape_device = self._get_ltfs_device()  # 使用 SCSI generic 设备

        logger.info(f"[LTFS] 开始格式化磁带为 LTFS 格式... (设备: {tape_device})")

        # ===== 格式化前清理：终止可能占用设备的 LTFS 进程 =====
        try:
            from utils.ltfs_ops import cleanup_mount
            mount_point = self._get_ltfs_mount_point()
            await cleanup_mount(mount_point)

            # 终止所有 LTFS 进程
            result = await self._run_command(['pkill', '-9', '-f', 'ltfs'], timeout=10, check=False)
            await asyncio.sleep(2)
            logger.info("[LTFS] 已清理旧的 LTFS 进程")
        except Exception as cleanup_err:
            logger.warning(f"[LTFS] 清理旧进程时出错: {cleanup_err}")

        # 生成6字符序列号（如果未提供）
        if not serial or len(serial) != 6:
            serial = f"T{int(time.time()) % 100000:05d}"

        try:
            cmd = [MKLTFS_BIN, '-d', tape_device, '-n', volume_name, '-s', serial, '-f']
            logger.info(f"[LTFS] 执行命令: {' '.join(cmd)}")

            # 使用 subprocess 实时输出进度
            process = await asyncio.create_subprocess_exec(
                *cmd,
                stdout=asyncio.subprocess.PIPE,
                stderr=asyncio.subprocess.PIPE
            )

            # 实时读取 stderr（mkltfs 输出到 stderr）
            stderr_output = []
            stdout_output = []

            async def read_stream(stream, output_list, prefix):
                """实时读取流并输出"""
                while True:
                    line = await stream.readline()
                    if not line:
                        break
                    line_text = line.decode('utf-8', errors='replace').strip()
                    if line_text:
                        output_list.append(line_text)
                        logger.info(f"[LTFS] {prefix}: {line_text}")

            # 同时读取 stdout 和 stderr
            await asyncio.gather(
                read_stream(process.stdout, stdout_output, "stdout"),
                read_stream(process.stderr, stderr_output, "mkltfs")
            )

            await process.wait()

            stderr_text = '\n'.join(stderr_output)
            stdout_text = '\n'.join(stdout_output)

            if process.returncode == 0 or 'Medium formatted successfully' in stderr_text:
                logger.info(f"[LTFS] ✅ 格式化成功!")

                # 提取容量信息
                capacity = None
                for line in stderr_output:
                    if 'Volume capacity is' in line:
                        import re
                        match = re.search(r'Volume capacity is\s+(\d+(?:\.\d+)?)\s+GB', line)
                        if match:
                            capacity = match.group(1)
                            break

                log_msg = f"[LTFS] 卷名: {volume_name}, 序列号: {serial}"
                if capacity:
                    log_msg += f", 容量: {capacity} GB"
                logger.info(log_msg)

                # 记录格式化操作到数据库
                try:
                    from utils.log_utils import log_operation
                    from models.system_log import OperationType
                    await log_operation(
                        operation_type=OperationType.TAPE_FORMAT,
                        resource_type="tape",
                        resource_name=volume_name or "",
                        operation_name="磁带格式化",
                        operation_description=f"磁带格式化成功: 卷名={volume_name}, 序列号={serial}",
                        category="tape",
                        success=True,
                    )
                except Exception:
                    pass

                return True, f"格式化成功: 卷名={volume_name}, 序列号={serial}"
            else:
                error = stderr_text[:500] if stderr_text else "未知错误"
                logger.error(f"[LTFS] ❌ 格式化失败 (返回码={process.returncode})")
                logger.error(f"[LTFS] 错误信息: {error}")
                return False, f"格式化失败: {error}"

        except Exception as e:
            logger.error(f"[LTFS] 格式化异常: {e}")
            return False, str(e)

    async def format_tape(self, volume_name: str = "BACKUP", serial: str = None) -> Tuple[bool, str]:
        """直接格式化磁带为 LTFS（公开方法）

        会自动重置设备并格式化

        Args:
            volume_name: 卷名（如 "BACKUP001"）
            serial: 序列号（必须是6个字符，如 "TP0001"）

        Returns:
            (是否成功, 信息)
        """
        # 检查磁带是否在驱动器
        # 与读标签/挂载互斥：否则读标签的 rewind 阻塞期间点格式化会 Device busy
        from utils.tape_device_lock import async_tape_device_lock
        async with async_tape_device_lock('format', timeout=600):
            in_drive, status = await self.check_tape_in_drive()
            if not in_drive:
                return False, f"磁带未就绪: {status}"

            # 重置设备
            await self.reset_tape_device()

            # 格式化
            return await self.format_as_ltfs(volume_name, serial)

    async def check_if_mounted(self) -> bool:
        """检查 LTFS 是否已挂载

        Returns:
            True=已挂载，False=未挂载
        """
        mount_point = self._get_ltfs_mount_point()
        try:
            if mount_point.exists():
                is_mount = mount_point.is_mount()
                logger.debug(f"[LTFS] 挂载点 {mount_point} 挂载状态: {is_mount}")
                return is_mount
            return False
        except Exception as e:
            logger.warning(f"[LTFS] 检查挂载状态失败: {e}")
            return False

    async def mount_ltfs(self, backup_task=None) -> Tuple[bool, str]:
        """挂载 LTFS

        逻辑流程：
        1. 检查磁带是否在驱动器
        2. 尝试挂载 LTFS
        3. 如果失败，格式化后重新挂载
        4. 如果成功但有内容，格式化后重新挂载
        5. 只有空的 LTFS 磁带才直接使用

        Args:
            backup_task: 备份任务对象（用于发送钉钉通知）

        Returns:
            (是否成功, 信息)
        """
        tape_device = self._get_ltfs_device()  # 使用 SCSI generic 设备
        mount_point = self._get_ltfs_mount_point()

        async def _send_failure_notification(error_msg: str):
            """发送挂载失败通知"""
            logger.error(f"[LTFS] 挂载失败: {error_msg}")
            if self.dingtalk_notifier:
                try:
                    task_name = backup_task.task_name if backup_task else "未知任务"
                    await self.dingtalk_notifier.send_backup_notification(
                        task_name,
                        "failed",
                        {'error': f"LTFS 挂载失败: {error_msg}"}
                    )
                    logger.info("[LTFS] 挂载失败钉钉通知已发送")
                except Exception as notify_error:
                    logger.warning(f"[LTFS] 发送钉钉通知失败: {str(notify_error)}")

        # 1. 检查磁带是否在驱动器
        in_drive, status = await self.check_tape_in_drive()
        if not in_drive:
            error_msg = f"磁带未就绪: {status}"
            await _send_failure_notification(error_msg)
            return False, error_msg

        # 1.5 尝试读取现有磁带卷标
        existing_label = "Unknown"
        try:
            if self.tape_manager and self.tape_manager.tape_operations:
                tape_ops = self.tape_manager.tape_operations
                if hasattr(tape_ops, '_read_tape_label'):
                    label_info = await tape_ops._read_tape_label()
                    if label_info and label_info.get('tape_id'):
                        existing_label = label_info.get('tape_id')
                        logger.info(f"[LTFS] 读取到现有磁带卷标: {existing_label}")
        except Exception as e:
            logger.warning(f"[LTFS] 读取磁带卷标失败: {e}，将作为新磁带处理")

        # 2. 检查磁带状态并处理
        is_ltfs = False
        msg = ""

        if self._ltfs_mounted:
            # 已挂载：跳过挂载检查，直接进入内容检查
            logger.info("[LTFS] 已经挂载，检查磁带内容...")
            is_ltfs = True
            msg = "已挂载"
        else:
            # 未挂载：尝试挂载并检查格式
            is_ltfs, msg = await self.check_ltfs_format()

        if is_ltfs:
            # LTFS 格式，直接使用（空盘或续写由 backup_engine 决定）
            is_empty, file_count = await self.is_tape_empty()
            if is_empty:
                logger.info("[LTFS] 磁带为空，可以使用")
            else:
                logger.info(f"[LTFS] 磁带有 {file_count} 个文件，将续写数据")
            return True, msg

        # 非 LTFS 格式，返回失败（格式化由 backup_engine 负责）
        error_msg = f"磁带不是 LTFS 格式，请先格式化"
        logger.warning(f"[LTFS] {error_msg}")
        await _send_failure_notification(error_msg)
        return False, error_msg

    async def offline_tape(self) -> bool:
        """把介质卸载出读写通路（停机收尾用）。

        LTFS 卸载后带子仍处于加载状态（停在索引分区末尾），若此时系统
        关机/重启，驱动器会在下电前带着"加载状态"复位，重启后 LTFS 按
        MAM 分区信息定位标签可能落错位置（曾导致读 ANSI 标签失败）。

        磁带有 ONLINE 位才执行 mt offline；未加载时直接返回，避免无谓报错。
        """
        from utils.ltfs_ops import eject_tape

        tape_device = self._get_tape_device()
        try:
            result = await self._run_command(['mt', '-f', tape_device, 'status'], timeout=30)
            output = result.stdout + result.stderr
            if 'ONLINE' not in output:
                logger.info("[LTFS] 磁带未加载，无需 offline")
                return True
        except Exception as e:
            logger.warning(f"[LTFS] 检查磁带状态失败，跳过 offline: {e}")
            return False

        try:
            ok, msg = await eject_tape(tape_device, timeout=600)
        except Exception as e:
            logger.warning(f"[LTFS] 磁带 offline 异常: {e}")
            return False

        if ok:
            logger.info(f"[LTFS] 磁带已 offline（{msg}）")
        else:
            logger.warning(f"[LTFS] 磁带 offline 失败（非致命）: {msg}")
        return ok

    async def unmount_ltfs(self, eject: bool = False) -> bool:
        """安全卸载 LTFS（委托给标准工具函数）

        Args:
            eject: 是否在卸载后弹出磁带（默认 False，仅卸载不弹出）
        """
        if not self._ltfs_mounted:
            # 标志可能没来得及置位（挂载过程中被中断、跨实例调用等），
            # 但实际挂载仍存在时必须清理，否则留下僵尸挂载 + 占着设备的
            # LTFS 进程（2026-10-26 19:17 实测，后续 ltfsck 报 EBUSY）
            fallback = self._ltfs_mount_point or Path(self._get_ltfs_mount_point())
            try:
                if not Path(fallback).is_mount():
                    return True
            except Exception:
                return True
            logger.warning(f"[LTFS] 挂载标志缺失但 {fallback} 仍挂载，执行清理")
            self._ltfs_mount_point = Path(fallback)

        mount_point = self._ltfs_mount_point
        if not mount_point:
            return True

        try:
            from utils.ltfs_ops import safe_unmount_ltfs
            tape_device = self._get_tape_device() if eject else None
            success, msg = await safe_unmount_ltfs(
                mount_point=mount_point,
                tape_device=tape_device,
                wait_ltfs=True,
            )
            self._ltfs_mounted = False
            self._ltfs_process = None

            # 边界：挂载点实际已卸载时 safe_unmount_ltfs 会直接返回，
            # 不会执行 mt offline，而带子可能仍处于加载状态。
            # eject 场景下补一次 offline（已离线时只做一次 mt status，无副作用）。
            if eject and success:
                try:
                    await self.offline_tape()
                except Exception as e:
                    logger.warning(f"[LTFS] 卸载后补 offline 失败（非致命）: {e}")

            # 记录卸载到数据库
            try:
                from utils.log_utils import log_operation
                from models.system_log import OperationType
                await log_operation(
                    operation_type=OperationType.TAPE_UNMOUNT,
                    resource_type="tape",
                    operation_name="磁带卸载",
                    operation_description=msg,
                    category="tape",
                    success=success,
                )
            except Exception:
                pass
            return success
        except Exception as e:
            logger.warning(f"[LTFS] 卸载失败: {e}")
            return False

    async def _wait_index_ready(self, mount_point: Path, timeout: float = 300.0) -> bool:
        """等待 LTFS 索引加载完成（.LTFS 元数据目录出现）。

        `mount_point.is_mount()` 只代表 FUSE 挂载完成，索引是后台读的：
        9TB 带子（1169 个归档）读索引要 1~3 分钟，这期间目录是空的。
        若此时就返回"挂载成功"，上层立刻统计归档会得到 0 个，
        进而误报"磁带上未发现归档文件（请确认装载的磁带是否正确）"
        （2026-10-26 19:03 实测：第 2 次重试 4 秒就 is_mount()，
        挂载点连 .LTFS 都没有）。
        """
        deadline = time.time() + timeout
        while time.time() < deadline:
            try:
                if (Path(mount_point) / '.LTFS').exists():
                    return True
            except OSError:
                pass
            await asyncio.sleep(2)
        return False

    async def mount_with_retry(self, backup_task=None, max_retries: int = 3, retry_interval: int = 30) -> Tuple[bool, str]:
        """尝试挂载磁带，重试多次

        Args:
            backup_task: 备份任务对象（用于发送通知）
            max_retries: 最大重试次数（默认3次）
            retry_interval: 重试间隔秒数（默认30秒）

        Returns:
            (是否成功, 信息)
        """
        tape_device = self._get_ltfs_device()
        mount_point = self._get_ltfs_mount_point()

        async def _send_failure_notification(error_msg: str):
            """发送失败通知"""
            logger.error(f"[挂载重试] {error_msg}")
            if self.dingtalk_notifier and backup_task:
                try:
                    task_name = backup_task.task_name if backup_task else "磁带挂载"
                    await self.dingtalk_notifier.send_backup_notification(
                        task_name,
                        "failed",
                        {'error': error_msg}
                    )
                    logger.info("[挂载重试] 失败通知已发送")
                except Exception as notify_error:
                    logger.warning(f"[挂载重试] 发送通知失败: {notify_error}")

        for attempt in range(1, max_retries + 1):
            logger.info(f"[挂载重试] 第 {attempt}/{max_retries} 次尝试挂载磁带...")

            # 先确认介质在驱动器并已加载到读写通路。
            # 漏了这步会一直卡死：带子被 eject 后 LTFS 打开设备拿不到介质，
            # 进程原地等设备、挂载点始终为空（2026-10-26 19:07 实测卡 5 分钟）。
            # check_tape_in_drive 内部含 mt load 兜底。
            in_drive, drive_msg = await self.check_tape_in_drive()
            if not in_drive:
                logger.warning(f"[挂载重试] 第 {attempt} 次：磁带未就绪 - {drive_msg}")
                if attempt == max_retries:
                    await _send_failure_notification(f"磁带未就绪（已尝试 {max_retries} 次）: {drive_msg}")
                    return False, f"磁带未就绪: {drive_msg}"
                await asyncio.sleep(retry_interval)
                continue

            # 清理旧挂载
            try:
                await self.unmount_ltfs()
                await asyncio.sleep(2)
            except Exception:
                pass

            # 清空挂载点
            try:
                mount_point.mkdir(parents=True, exist_ok=True)
                for item in mount_point.iterdir():
                    if item.is_dir():
                        shutil.rmtree(item)
                    else:
                        item.unlink()
            except Exception as e:
                logger.warning(f"[挂载重试] 清空挂载点失败: {e}")

            # 尝试挂载
            try:
                process = await asyncio.create_subprocess_exec(
                    LTFS_BIN, "-o", f"devname={tape_device},max_write=1048576,big_writes", str(mount_point),
                    stdout=asyncio.subprocess.PIPE,
                    stderr=asyncio.subprocess.PIPE
                )

                # LTO磁带需要时间挂载，等待检查。
                # 90 秒：之前 30 秒判超时，而实测大盘带（9TB/1169 归档）
                # 装带+定位+挂载 FUSE 经常超过 60 秒（19:51 实测挂载成功
                # 却已被判超时，留下无人管理的挂载）。
                for wait_time in range(90):
                    await asyncio.sleep(1)
                    if mount_point.is_mount():
                        # 先登记挂载状态再做索引等待：等待期间任务可能被停止，
                        # unmount_ltfs() 靠 _ltfs_mounted 判断是否需要清理，
                        # 不先置位会被当成"没挂载"直接跳过 → 留下僵尸挂载
                        # 和占着 /dev/sg1 的 LTFS 进程（2026-10-26 19:17 实测）
                        self._ltfs_mounted = True
                        self._ltfs_mount_point = mount_point
                        self._ltfs_process = process
                        # FUSE 挂上了不代表索引读完，必须等 .LTFS 出现
                        if not await self._wait_index_ready(mount_point):
                            logger.warning(
                                f"[挂载重试] 第 {attempt} 次：挂载点已挂载但索引未就绪，视为失败")
                            break
                        logger.info(f"[挂载重试] 第 {attempt} 次挂载成功")
                        # 记录挂载成功到数据库
                        try:
                            from utils.log_utils import log_operation
                            from models.system_log import OperationType
                            await log_operation(
                                operation_type=OperationType.TAPE_MOUNT,
                                resource_type="tape",
                                operation_name="磁带挂载",
                                operation_description=f"磁带挂载成功（第{attempt}次尝试）",
                                category="tape",
                                success=True,
                            )
                        except Exception:
                            pass
                        return True, f"挂载成功（第{attempt}次尝试）"
                    # LTFS进程已退出，检查是否daemon化挂载成功（returncode==0 说明成功)
                    if process.returncode is not None:
                        if process.returncode == 0 and mount_point.is_mount():
                            # 先登记挂载状态（同即时检测分支，保证中断时能清理）
                            self._ltfs_mounted = True
                            self._ltfs_mount_point = mount_point
                            self._ltfs_process = process
                            if not await self._wait_index_ready(mount_point):
                                logger.warning(
                                    f"[挂载重试] 第 {attempt} 次：daemon 化退出但索引未就绪，视为失败")
                                break
                            logger.info(f"[挂载重试] 第 {attempt} 次挂载成功（LTFS daemon化退出）")
                            try:
                                from utils.log_utils import log_operation
                                from models.system_log import OperationType
                                await log_operation(
                                    operation_type=OperationType.TAPE_MOUNT,
                                    resource_type="tape",
                                    operation_name="磁带挂载",
                                    operation_description=f"磁带挂载成功（第{attempt}次尝试，daemon化退出）",
                                    category="tape",
                                    success=True,
                                )
                            except Exception:
                                pass
                            return True, f"挂载成功（第{attempt}次尝试）"
                        break

                # 进程已退出则跳过二次等待，直接读取错误
                if process.returncode is not None:
                    pass
                else:
                    # 再给 60 秒（大盘带装带后定位/读索引可能更久）
                    await asyncio.sleep(60)

                if mount_point.is_mount():
                    # 先登记挂载状态（中断时能清理），再等索引就绪
                    self._ltfs_mounted = True
                    self._ltfs_mount_point = mount_point
                    self._ltfs_process = process
                if mount_point.is_mount() and await self._wait_index_ready(mount_point):
                    logger.info(f"[挂载重试] 第 {attempt} 次挂载成功（延迟检测）")
                    # 记录延迟检测挂载成功到数据库
                    try:
                        from utils.log_utils import log_operation
                        from models.system_log import OperationType
                        await log_operation(
                            operation_type=OperationType.TAPE_MOUNT,
                            resource_type="tape",
                            operation_name="磁带挂载",
                            operation_description=f"磁带挂载成功（第{attempt}次尝试，延迟检测）",
                            category="tape",
                            success=True,
                        )
                    except Exception:
                        pass
                    return True, f"挂载成功（第{attempt}次尝试，延迟检测）"

                # 挂载失败，读取错误信息
                try:
                    stdout, stderr = await asyncio.wait_for(process.communicate(), timeout=5)
                    error_msg = stderr.decode('utf-8', errors='ignore')[:200] if stderr else "未知错误"
                except asyncio.TimeoutError:
                    # 不 kill LTFS 进程，等它自行退出
                    logger.warning("[挂载重试] 读取超时，等待 LTFS 进程自行退出...")
                    try:
                        await asyncio.wait_for(process.wait(), timeout=600)
                    except asyncio.TimeoutError:
                        logger.warning("[挂载重试] LTFS 进程 600s 后仍未退出，不再等待")
                    error_msg = "挂载超时"

                logger.warning(f"[挂载重试] 第 {attempt} 次挂载失败: {error_msg}")

                # 最后一次尝试失败后，不等待重试间隔直接停止
                if attempt == max_retries:
                    await _send_failure_notification(f"挂载失败（已尝试{max_retries}次，间隔{retry_interval}秒）: {error_msg}")
                    return False, f"挂载失败（{max_retries}次尝试均失败）"

                # 不是最后一次尝试，等待重试间隔
                if attempt < max_retries:
                    logger.info(f"[挂载重试] 等待 {retry_interval} 秒后重试...")
                    await asyncio.sleep(retry_interval)

            except Exception as e:
                logger.error(f"[挂载重试] 第 {attempt} 次尝试异常: {e}")
                if attempt == max_retries:
                    await _send_failure_notification(f"挂载异常（已尝试{max_retries}次）: {str(e)}")
                    return False, f"挂载异常: {str(e)}"

                # 不是最后一次尝试，等待重试间隔
                if attempt < max_retries:
                    await asyncio.sleep(retry_interval)

        # 所有尝试都失败
        await _send_failure_notification(f"挂载完全失败：已尝试{max_retries}次，间隔{retry_interval}秒")
        return False, f"挂载失败（{max_retries}次尝试均失败）"

    async def list_tape_contents(self) -> list:
        """列出磁带内容"""
        if not self._ltfs_mounted or not self._ltfs_mount_point:
            return []

        contents = []
        try:
            for item in self._ltfs_mount_point.iterdir():
                contents.append({
                    'name': item.name,
                    'is_dir': item.is_dir(),
                    'size': item.stat().st_size if item.is_file() else 0
                })
            # 调试：打印找到的文件
            if contents:
                file_info = []
                for f in contents:
                    if f['is_dir']:
                        file_info.append((f['name'], 'DIR'))
                    else:
                        file_info.append((f['name'], f'{f["size"]} bytes'))
                logger.debug(f"[LTFS] 磁带内容: {file_info}")
        except Exception as e:
            logger.warning(f"[LTFS] 列出磁带内容失败: {e}")

        return contents

    async def is_tape_empty(self) -> Tuple[bool, int]:
        """检查磁带是否为空

        Returns:
            (是否为空, 用户文件数量)
        """
        contents = await self.list_tape_contents()

        # LTFS 和文件系统的已知系统文件/目录模式
        system_patterns = {
            # 隐藏文件/目录
            '.',
            '..',
            # LTFS 元数据
            '.LTFS',
            # Linux/Unix 系统文件
            'lost+found',
            '.Trash',
            '.Trashes',
            '.Spotlight-V100',
            '.Spotlight',
            '.fseventsd',
            '.VolumeIcon.icns',
            '.DS_Store',
            '.AppleDouble',
            '.AppleDB',
            '.TemporaryItems',
            # Windows 系统文件（如果在Windows上创建）
            '$RECYCLE.BIN',
            'System Volume Information',
            'RECYCLER',
            'desktop.ini',
            'Thumbs.db',
            # 其他可能的自动生成的文件
            '@placeholder',
            '@eadir',
        }

        # 过滤出用户文件
        user_files = []
        for f in contents:
            name = f['name']
            is_system = False

            # 检查是否是系统文件
            if name in system_patterns:
                is_system = True
            elif name.startswith('.'):
                is_system = True
            # 检查是否是特定模式的文件
            elif name.startswith('@') and name in ['@placeholder', '@eadir']:
                is_system = True
            elif name.startswith('$'):
                is_system = True

            if not is_system:
                user_files.append(f)

        # 调试日志
        if contents:
            all_names = [f['name'] for f in contents]
            user_names = [f['name'] for f in user_files]
            logger.debug(f"[LTFS] 磁带内容检查: 全部文件={all_names}, 用户文件={user_names}, 用户文件数={len(user_files)}")

        return len(user_files) == 0, len(user_files)

    async def write_to_tape_drive(self, source_path: str, backup_set: BackupSet, group_idx: int) -> Optional[str]:
        """将压缩文件复制到磁带（通过 LTFS 挂载）

        流程：
        1. 检查磁带内容（仅日志）
        2. 复制文件
        3. 验证复制成功
        4. 删除源文件

        注意：挂载操作应在任务开始时通过 mount_with_retry 执行一次

        Args:
            source_path: 源文件路径（本地压缩文件路径）
            backup_set: 备份集对象
            group_idx: 组索引

        Returns:
            str: 磁带上的相对路径，如果失败则返回 None
        """
        try:
            source_file = Path(source_path)
            if not source_file.exists():
                logger.error(f"[LTFS] 压缩文件不存在: {source_path}")
                return None

            source_size = source_file.stat().st_size
            source_size_mb = source_size / (1024 * 1024)
            logger.info(f"[LTFS] 准备复制文件到磁带: {source_file.name} (大小: {source_size_mb:.2f} MB)")

            # 任务启动时已确保磁带挂载
            mount_point = self._ltfs_mount_point
            if not mount_point or not mount_point.is_mount():
                logger.error(f"[LTFS] 磁带未挂载，请先调用 mount_with_retry")
                return None

            # ===== 步骤1: 检查磁带内容（仅日志）=====
            is_empty, file_count = await self.is_tape_empty()
            logger.debug(f"[LTFS] 磁带状态: {'空' if is_empty else f'已有 {file_count} 个文件'}")

            if not is_empty:
                contents = await self.list_tape_contents()
                for item in contents[:10]:  # 只显示前10个
                    logger.debug(f"[LTFS]   - {item['name']} ({item['size'] / (1024*1024):.2f} MB)")

            # ===== 步骤3: 复制文件 =====
            target_dir = mount_point / backup_set.set_id
            target_dir.mkdir(parents=True, exist_ok=True)
            target_file = target_dir / source_file.name

            logger.info(f"[LTFS] 开始复制: {source_file} -> {target_file}")
            start_time = time.time()

            # 对外暴露实时写入状态（供任务卡片读取）。回调在复制线程中执行，
            # 只做一次整数赋值，足够轻量。
            self._tape_write_file = source_file.name
            self._tape_write_total = source_size
            self._tape_write_bytes = 0
            self._tape_write_start = start_time
            # 本任务第一次写带的起始时间（平均速度 = 累计字节 / 从此刻到现在的墙钟时间）
            if self._tape_first_write_start is None:
                self._tape_first_write_start = start_time

            def _on_copy_progress(written: int) -> None:
                self._tape_write_bytes = written

            try:
                await _copy_with_buffer(source_file, target_file,
                                        on_progress=_on_copy_progress)
            except asyncio.CancelledError:
                logger.warning("[LTFS] 复制任务被取消")
                raise
            except Exception as e:
                logger.error(f"[LTFS] 复制失败: {e}")
                return None
            finally:
                # 不论成功/失败/取消，都清空实时状态，避免卡片显示过期进度
                self._tape_write_file = None
                self._tape_write_bytes = 0
                self._tape_write_total = 0
                self._tape_write_start = None

            elapsed = time.time() - start_time
            speed_mb = source_size_mb / elapsed if elapsed > 0 else 0

            # ===== 步骤4: 验证复制 =====
            if not target_file.exists():
                logger.error(f"[LTFS] 复制后目标文件不存在: {target_file}")
                return None

            target_size = target_file.stat().st_size
            if target_size != source_size:
                logger.error(f"[LTFS] 文件大小不匹配: 源={source_size}, 目标={target_size}")
                try:
                    target_file.unlink()
                except:
                    pass
                return None

            logger.info(f"[LTFS] 复制成功: {source_size_mb:.2f} MB, 耗时 {elapsed:.1f}s, 速度 {speed_mb:.2f} MB/s")

            # 累计写带统计（仅供"平均写带速度"显示；只统计成功完成并通过大小校验的文件）
            self._tape_cum_bytes += int(source_size)
            self._tape_cum_seconds += float(elapsed)

            # ===== 步骤5: 列出磁带内容（写入后）=====
            contents_after = await self.list_tape_contents()
            logger.debug(f"[LTFS] 写入后磁带内容 ({len(contents_after)} 个文件):")
            for item in contents_after[:5]:
                logger.debug(f"[LTFS]   - {item['name']}")

            # ===== 步骤6: 删除源文件 =====
            try:
                await asyncio.to_thread(source_file.unlink)
                logger.debug(f"[LTFS] 源文件已删除: {source_file}")
            except Exception as e:
                logger.warning(f"[LTFS] 删除源文件失败: {e}")

            # 返回磁带上的相对路径
            relative_path = str(target_file.relative_to(mount_point))
            return relative_path

        except asyncio.CancelledError:
            logger.warning("[LTFS] 写入任务被取消")
            raise
        except Exception as e:
            logger.error(f"[LTFS] 写入磁带失败: {e}")
            import traceback
            logger.error(traceback.format_exc())
            return None

    # ===== 保留原有方法（兼容性）=====

    async def get_current_drive_tape(self) -> Optional[TapeCartridge]:
        """获取当前驱动器中的磁带

        Returns:
            TapeCartridge: 磁带对象，如果不存在则返回 None
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
                        # 记录磁带扫描到数据库
                        try:
                            from utils.log_utils import log_operation
                            from models.system_log import OperationType
                            await log_operation(
                                operation_type=OperationType.TAPE_SCAN,
                                resource_type="tape",
                                resource_name=tape_id,
                                operation_name="磁带设备扫描",
                                operation_description=f"从驱动器扫描到磁带卷标: {tape_id}",
                                category="tape",
                                success=True,
                            )
                        except Exception:
                            pass

                        # 检查数据库中是否有该磁带
                        from utils.scheduler.db_utils import is_opengauss, get_opengauss_connection
                        if is_opengauss():
                            async with get_opengauss_connection() as conn:
                                row = await conn.fetchrow(
                                    """
                                    SELECT tape_id, label, status,
                                           COALESCE(first_use_date, manufactured_date, created_at) as created_date,
                                           expiry_date, capacity_bytes, used_bytes, serial_number
                                    FROM tape_cartridges
                                    WHERE tape_id = $1
                                    """,
                                    tape_id
                                )

                                if row:
                                    from datetime import datetime
                                    created_date = row['created_date']
                                    if created_date and isinstance(created_date, str):
                                        try:
                                            created_date = datetime.fromisoformat(created_date.replace('Z', '+00:00'))
                                        except:
                                            created_date = datetime.fromisoformat(created_date.split('T')[0])

                                    status_str = row.get('status')
                                    if status_str:
                                        status_lower = status_str.lower().strip() if isinstance(status_str, str) else str(status_str).lower().strip()
                                        try:
                                            tape_status = TapeStatus(status_lower)
                                        except ValueError:
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
                                    self.tape_manager.current_tape = tape
                                    self.tape_manager.tape_cartridges[tape_id] = tape
                                    logger.info(f"从数据库加载磁带信息: {tape_id}")
                                    return tape
                                else:
                                    logger.error(f"驱动器中的磁带不在数据库中: {tape_id}")
                                    raise RuntimeError(f"驱动器中的磁带 {tape_id} 未在数据库中注册，请先在磁带管理页面添加该磁带")
            except Exception as e:
                logger.warning(f"扫描当前驱动器磁带失败: {str(e)}")
                return None

            return None
        except Exception as e:
            logger.error(f"获取当前驱动器磁带失败: {str(e)}")
            return None
