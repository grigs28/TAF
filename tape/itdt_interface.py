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

		cmd = [self.itdt_path] + args
		logger.info("[ITDT] 执行: %s", " ".join([str(a) for a in cmd]))

		proc = await asyncio.create_subprocess_exec(
			*cmd,
			stdout=asyncio.subprocess.PIPE,
			stderr=asyncio.subprocess.PIPE,
		)

		stdout, stderr = await proc.communicate()

		out_text = stdout.decode(errors="ignore") if stdout else ""
		err_text = stderr.decode(errors="ignore") if stderr else ""

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

	async def scan_devices(self) -> List[Dict[str, Any]]:
		res = await self._run_itdt(["scan"])  # 无需 -f
		devices: List[Dict[str, Any]] = []
		if not res["success"]:
			return devices
		for line in res["stdout"].splitlines():
			# 简单解析：提取 Windows 设备名，例如 \\.\tape0
			if "\\\\.\\tape" in line.lower():
				devices.append({"path": line.strip()})
		return devices
