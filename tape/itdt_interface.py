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

	async def check_ltfs_readiness(self, device_path: Optional[str] = None, force_data_overwrite: bool = False) -> bool:
		"""检查磁带是否支持LTFS并已格式化（使用checkltfsreadiness命令）
		
		Args:
			device_path: 设备路径，如果为None则使用默认设备
			force_data_overwrite: 是否添加-forcedataoverwrite参数
		
		Returns:
			True表示磁带已格式化且支持LTFS，False表示未格式化或不支持LTFS
		"""
		dev = self._resolve_device(device_path)
		args = ["-f", dev, "checkltfsreadiness"]
		if force_data_overwrite:
			args.append("-forcedataoverwrite")
		res = await self._run_itdt(args)
		
		# 检查退出码和输出
		if res["success"]:
			stdout_lower = res["stdout"].lower()
			# 如果输出包含"ready"、"formatted"、"ltfs ready"、"ltfs is ready"等关键词，认为已格式化
			# 排除"not ready"、"not formatted"等否定词
			if ("not ready" not in stdout_lower and "not formatted" not in stdout_lower and 
				("ready" in stdout_lower or "formatted" in stdout_lower or "ltfs ready" in stdout_lower or 
				 "ltfs is ready" in stdout_lower or "ltfs support" in stdout_lower)):
				return True
			# 如果明确包含"not ready"或"not formatted"，返回False
			if "not ready" in stdout_lower or "not formatted" in stdout_lower or "not supported" in stdout_lower:
				return False
		# 如果命令失败，检查stderr中是否有错误信息
		if not res["success"]:
			err_lower = res["stderr"].lower() if res.get("stderr") else ""
			# 如果错误信息明确表示未格式化，返回False
			if "not formatted" in err_lower or "not ready" in err_lower or "not supported" in err_lower:
				return False
		return False

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
