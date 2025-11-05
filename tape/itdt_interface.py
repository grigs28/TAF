#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
ITDT 接口
IBM Tape Diagnostic Tool (ITDT) 命令封装
"""

import asyncio
import logging
import os
import platform
from pathlib import Path
from typing import Any, Dict, List, Optional

from config.settings import get_settings

logger = logging.getLogger(__name__)


class ITDTInterface:
	"""ITDT 命令行接口封装。"""

	def __init__(self) -> None:
		self.settings = get_settings()
		self.system = platform.system()
		self.itdt_path: Optional[str] = None
		self._initialized = False

	async def initialize(self) -> None:
		"""初始化 ITDT 路径并检查可用性。"""
		itdt_path = getattr(self.settings, "ITDT_PATH", None)
		candidates: List[str] = []
		if itdt_path:
			candidates.append(itdt_path)
		if self.system == "Windows":
			candidates += [
				"C:\\itdt\\itdt.exe",
				"C:\\Program Files\\IBM\\ITDT\\itdt.exe",
				"C:\\Program Files (x86)\\IBM\\ITDT\\itdt.exe",
			]
			# 项目内路径（当前工作目录 / 脚本目录上级）
			cwd_candidate = str(Path(os.getcwd()) / "ITDT" / "itdt.exe")
			pkg_candidate = str((Path(__file__).resolve().parents[1] / "ITDT" / "itdt.exe"))
			candidates = [cwd_candidate, pkg_candidate] + candidates
		else:
			candidates.append("/usr/local/itdt/itdt")

		# 选择第一个存在的路径
		for p in candidates:
			if p and os.path.exists(p):
				self.itdt_path = p
				break
		# 如果都不存在，仍然使用首个候选以便日志提示
		if not self.itdt_path:
			self.itdt_path = candidates[0] if candidates else None

		if not await self._check_itdt_available():
			raise FileNotFoundError(f"未找到 ITDT 可执行文件: {self.itdt_path}")

		self._initialized = True
		logger.info("ITDT 接口初始化完成: %s", self.itdt_path)

	async def _check_itdt_available(self) -> bool:
		try:
			proc = await asyncio.create_subprocess_exec(
				self.itdt_path, "-version",
				stdout=asyncio.subprocess.PIPE,
				stderr=asyncio.subprocess.PIPE,
			)
			stdout, stderr = await proc.communicate()
			if stdout:
				logger.debug("[ITDT] %s", stdout.decode(errors="ignore").strip())
			if stderr:
				logger.debug("[ITDT-ERR] %s", stderr.decode(errors="ignore").strip())
			return proc.returncode == 0
		except Exception as e:
			logger.error("检查 ITDT 可用性失败: %s", str(e))
			return False

	def _resolve_device(self, device_path: Optional[str]) -> str:
		"""解析设备路径，Windows 使用 \\.\tape0。"""
		if device_path:
			return device_path
		default_path = getattr(self.settings, "ITDT_DEVICE_PATH", None)
		if default_path:
			return default_path
		return "\\\\.\\tape0" if self.system == "Windows" else "/dev/IBMtape0"

	async def _run_itdt(self, args: List[str]) -> Dict[str, Any]:
		"""运行 ITDT 命令，整合日志并返回结果。"""
		if not self._initialized:
			raise RuntimeError("ITDT 接口未初始化")

		# 全局标志
		global_flags: List[str] = []
		if getattr(self.settings, "ITDT_FORCE_GENERIC_DD", False):
			global_flags.append("-force-generic-dd")
		cmd = [self.itdt_path] + global_flags + args
		cmd_str = " ".join([str(a) for a in cmd])
		# 输出完整命令行到终端（便于验证）
		print(f"\n[ITDT 完整命令] {cmd_str}\n")
		logger.info("[ITDT] 执行: %s", cmd_str)

		proc = await asyncio.create_subprocess_exec(
			*cmd,
			stdout=asyncio.subprocess.PIPE,
			stderr=asyncio.subprocess.PIPE,
		)

		stdout, stderr = await proc.communicate()

		out_text = stdout.decode(errors="ignore") if stdout else ""
		err_text = stderr.decode(errors="ignore") if stderr else ""

		# 输出结果到终端（便于验证）
		if out_text.strip():
			print(f"[ITDT 标准输出]\n{out_text}")
		if err_text.strip():
			print(f"[ITDT 标准错误]\n{err_text}")
		print(f"[ITDT 退出码] {proc.returncode}\n")

		for line in out_text.splitlines():
			if line.strip():
				logger.info("[ITDT] %s", line.strip())
		for line in err_text.splitlines():
			if line.strip():
				logger.warning("[ITDT] %s", line.strip())

		return {
			"success": proc.returncode == 0,
			"returncode": proc.returncode,
			"stdout": out_text,
			"stderr": err_text,
		}

	# 基础操作封装
	async def test_unit_ready(self, device_path: Optional[str] = None) -> bool:
		dev = self._resolve_device(device_path)
		# 清理设备路径（移除可能的冒号）
		if dev.endswith(':'):
			dev = dev[:-1]
		# Windows下优先使用\\.\Tape0格式
		if self.system == "Windows" and dev.startswith("\\\\.\\scsi"):
			# 尝试使用\\.\Tape0格式
			dev = "\\\\.\\Tape0"
		res = await self._run_itdt(["-f", dev, "tur"])
		return res["success"]

	async def rewind(self, device_path: Optional[str] = None) -> bool:
		dev = self._resolve_device(device_path)
		res = await self._run_itdt(["-f", dev, "rewind"])
		return res["success"]

	async def load(self, device_path: Optional[str] = None, amu: bool = False) -> bool:
		dev = self._resolve_device(device_path)
		args = ["-f", dev, "load"]
		if amu:
			args.append("-amu")
		res = await self._run_itdt(args)
		return res["success"]

	async def unload(self, device_path: Optional[str] = None) -> bool:
		dev = self._resolve_device(device_path)
		res = await self._run_itdt(["-f", dev, "unload"])
		return res["success"]

	async def erase(self, device_path: Optional[str] = None, short: bool = False) -> bool:
		dev = self._resolve_device(device_path)
		args = ["-f", dev, "erase"]
		if short:
			args.append("-short")
		res = await self._run_itdt(args)
		return res["success"]

	async def query_position(self, device_path: Optional[str] = None) -> Optional[int]:
		dev = self._resolve_device(device_path)
		res = await self._run_itdt(["-f", dev, "qrypos"])
		if not res["success"]:
			return None
		# 解析位置（如果输出包含 Block id）
		for line in res["stdout"].splitlines():
			if "Block" in line or "block" in line:
				return None  # 位置格式依赖具体输出，后续细化解析
		return None

	async def write_filemark(self, device_path: Optional[str] = None, count: int = 1) -> bool:
		dev = self._resolve_device(device_path)
		args = ["-f", dev, "weof"]
		if count and count != 1:
			args.append(str(count))
		res = await self._run_itdt(args)
		return res["success"]

	async def query_partition(self, device_path: Optional[str] = None) -> Dict[str, Any]:
		"""查询磁带分区信息（使用qrypart命令）
		
		Args:
			device_path: 设备路径，如果为None则使用默认设备
		
		Returns:
			包含分区信息的字典，例如：
			{
				"active_partition": 0,
				"max_additional_partitions": 3,
				"additional_partitions_defined": 1,
				"partitioning_type": "wrap-wise partitioning",
				"partitions": [
					{"index": 0, "size_meg": 128000},
					{"index": 1, "size_meg": 17614000}
				],
				"has_partitions": True  # 是否有分区信息
			}
		"""
		dev = self._resolve_device(device_path)
		# 清理设备路径（移除可能的冒号）
		if dev.endswith(':'):
			dev = dev[:-1]
		# Windows下优先使用\\.\Tape0格式
		if self.system == "Windows" and dev.startswith("\\\\.\\scsi"):
			dev = "\\\\.\\Tape0"
		
		logger.info(f"[ITDT分区查询] 使用设备路径: {dev}")
		res = await self._run_itdt(["-f", dev, "qrypart"])
		
		partition_data = {
			"active_partition": None,
			"max_additional_partitions": None,
			"additional_partitions_defined": None,
			"partitioning_type": None,
			"partitions": [],
			"has_partitions": False
		}
		
		if not res["success"]:
			logger.warning(f"[ITDT分区查询] 命令执行失败，退出码: {res['returncode']}")
			return partition_data
		
		# 解析输出
		stdout = res["stdout"]
		logger.info(f"[ITDT分区查询] 输出: {stdout[:500]}")
		
		import re
		lines = stdout.split('\n')
		for line in lines:
			line = line.strip()
			if not line:
				continue
			
			# 匹配分区信息
			patterns = {
				r"Active Partition\s+(\d+)": ("active_partition", int),
				r"Max\. Additional Partitions\s+(\d+)": ("max_additional_partitions", int),
				r"Additional Partitions defined\s+(\d+)": ("additional_partitions_defined", int),
				r"Partitioning Type is\s+(.+)": ("partitioning_type", str),
			}
			
			for pattern, (key, type_func) in patterns.items():
				match = re.search(pattern, line, re.IGNORECASE)
				if match:
					value = match.group(1).strip()
					if type_func == int:
						partition_data[key] = int(value)
					else:
						partition_data[key] = value
					break
			
			# 匹配分区大小信息 (Partition 0 Size (Meg) ...... 128000)
			partition_match = re.search(r"Partition\s+(\d+)\s+Size\s+\(Meg\)\s+(\d+)", line, re.IGNORECASE)
			if partition_match:
				partition_index = int(partition_match.group(1))
				partition_size = int(partition_match.group(2))
				partition_data["partitions"].append({
					"index": partition_index,
					"size_meg": partition_size
				})
		
		# 判断是否有分区信息（已格式化的磁带必然有分区）
		partition_data["has_partitions"] = (
			partition_data["active_partition"] is not None or
			len(partition_data["partitions"]) > 0 or
			partition_data["max_additional_partitions"] is not None
		)
		
		logger.info(f"[ITDT分区查询] 解析结果: 有分区={partition_data['has_partitions']}, 活动分区={partition_data['active_partition']}")
		
		return partition_data

	async def tape_usage(self, device_path: Optional[str] = None) -> Dict[str, Any]:
		"""获取磁带使用统计信息（使用tapeusage命令）
		
		Args:
			device_path: 设备路径，如果为None则使用默认设备（\\.\tape0）
		
		Returns:
			包含磁带使用统计信息的字典，例如：
			{
				"thread_count": 7,
				"data_sets_read": 294,
				"data_sets_written": 218,
				"read_retries": 0,
				"write_retries": 4,
				"unrecovered_read_errors": 0,
				"unrecovered_write_errors": 0,
				"suspended_reads": 0,
				"suspended_writes": 4,
				"fatal_suspend_reads": 0,
				"fatal_suspended_writes": 0,
				"health_score": 100,  # 根据错误计算
				"result": "PASSED",
				"code": "OK"
			}
		"""
		dev = self._resolve_device(device_path)
		# 清理设备路径（移除可能的冒号）
		if dev.endswith(':'):
			dev = dev[:-1]
		# Windows下优先使用\\.\Tape0格式
		if self.system == "Windows" and dev.startswith("\\\\.\\scsi"):
			dev = "\\\\.\\Tape0"
		
		logger.info(f"[ITDT磁带使用统计] 使用设备路径: {dev}")
		res = await self._run_itdt(["-f", dev, "tapeusage"])
		
		usage_data = {
			"thread_count": 0,
			"data_sets_read": 0,
			"data_sets_written": 0,
			"read_retries": 0,
			"write_retries": 0,
			"unrecovered_read_errors": 0,
			"unrecovered_write_errors": 0,
			"suspended_reads": 0,
			"suspended_writes": 0,
			"fatal_suspend_reads": 0,
			"fatal_suspended_writes": 0,
			"health_score": 100,
			"result": "UNKNOWN",
			"code": "UNKNOWN",
			"is_formatted": None  # 格式化状态：True=已格式化, False=未格式化, None=无法确定
		}
		
		if not res["success"]:
			logger.warning(f"[ITDT磁带使用统计] 命令执行失败，退出码: {res['returncode']}")
			usage_data["is_formatted"] = False
			return usage_data
		
		# 使用qrypart命令判断格式化状态
		try:
			partition_info = await self.query_partition(device_path)
			usage_data["is_formatted"] = partition_info.get("has_partitions", False)
		except Exception as e:
			logger.warning(f"[ITDT磁带使用统计] 查询分区信息失败: {str(e)}")
			usage_data["is_formatted"] = False
		
		# 解析输出
		stdout = res["stdout"]
		logger.info(f"[ITDT磁带使用统计] 输出: {stdout[:500]}")
		
		# 解析各个字段
		import re
		lines = stdout.split('\n')
		for line in lines:
			line = line.strip()
			if not line:
				continue
			
			# 匹配数值字段
			patterns = {
				r"Thread Count\s+(\d+)": "thread_count",
				r"Data Sets Read\s+(\d+)": "data_sets_read",
				r"Data Sets Written\s+(\d+)": "data_sets_written",
				r"Read Retries\s+(\d+)": "read_retries",
				r"Write Retries\s+(\d+)": "write_retries",
				r"Unrecovered Read Err\.\s+(\d+)": "unrecovered_read_errors",
				r"Unrecovered Write Err\.\s+(\d+)": "unrecovered_write_errors",
				r"Suspended Reads\s+(\d+)": "suspended_reads",
				r"Suspended Writes\s+(\d+)": "suspended_writes",
				r"Fatal Suspend Reads\s+(\d+)": "fatal_suspend_reads",
				r"Fatal Suspended Writes\s+(\d+)": "fatal_suspended_writes",
			}
			
			for pattern, key in patterns.items():
				match = re.search(pattern, line, re.IGNORECASE)
				if match:
					usage_data[key] = int(match.group(1))
					break
			
			# 匹配结果
			if "Result:" in line:
				match = re.search(r"Result:\s*(\w+)", line, re.IGNORECASE)
				if match:
					usage_data["result"] = match.group(1).upper()
			
			if "Code:" in line:
				match = re.search(r"Code:\s*(\w+)", line, re.IGNORECASE)
				if match:
					usage_data["code"] = match.group(1).upper()
		
		# 计算健康分数（基于错误统计）
		# 基础分数100，根据错误情况扣分
		health_score = 100
		
		# 致命错误扣分最多
		health_score -= usage_data["fatal_suspend_reads"] * 10
		health_score -= usage_data["fatal_suspended_writes"] * 10
		
		# 未恢复错误扣分
		health_score -= usage_data["unrecovered_read_errors"] * 5
		health_score -= usage_data["unrecovered_write_errors"] * 5
		
		# 暂停操作扣分
		health_score -= usage_data["suspended_reads"] * 2
		health_score -= usage_data["suspended_writes"] * 2
		
		# 重试次数扣分（较少）
		health_score -= min(usage_data["read_retries"] + usage_data["write_retries"], 10)
		
		# 确保分数在0-100范围内
		usage_data["health_score"] = max(0, min(100, health_score))
		
		logger.info(f"[ITDT磁带使用统计] 解析结果: 健康分数={usage_data['health_score']}, 结果={usage_data['result']}, 格式化={usage_data['is_formatted']}")
		
		return usage_data

	async def scan_devices(self) -> List[Dict[str, Any]]:
		scan_args: List[str] = ["scan"]
		if getattr(self.settings, "ITDT_SCAN_SHOW_ALL_PATHS", False):
			scan_args.append("-showallpaths")
		res = await self._run_itdt(scan_args)  # 无需 -f
		devices: List[Dict[str, Any]] = []
		if not res["success"]:
			return devices
		import re
		for raw in res["stdout"].splitlines():
			line = raw.strip()
			if not line:
				continue
			# 典型输出: "#0 \\.\\scsi0: - [ULT3580-HH9]-[R3G1] S/N:10WT036260 H0-B0-T24-L0  (Generic-Device)"
			m = re.search(r"#\d+\s+([^\s]+):\s+-\s+\[([^\]]+)\](?:-\[([^\]]+)\])?\s+S/N:([^\s]+)", line, re.IGNORECASE)
			if m:
				dev_node = m.group(1)  # \\.\scsi0:
				model = m.group(2)     # ULT3580-HH9
				gen = (m.group(3) or "").strip()  # R3G1 等
				serial = m.group(4)
				devices.append({
					"path": dev_node.rstrip(':'),
					"vendor": "IBM" if "ULT3580" in model.upper() else "Unknown",
					"model": model,
					"generation": gen,
					"serial": serial,
					"status": "online",
					"is_ibm_lto": "ULT3580" in model.upper(),
				})
				continue
			# 回退：匹配 \\.\\tapeX 或 \\.\\scsiY
			if "\\\\.\\" in line.lower():
				# 取第一个形如 \\.\xxxx 片段
				mm = re.search(r"(\\\\\\.\\\\[A-Za-z0-9_-]+)[:]?", line)
				if mm:
					devices.append({"path": mm.group(1), "status": "online"})
		return devices
