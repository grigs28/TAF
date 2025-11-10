#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
DLL导出函数探索工具
用于探索DLL文件中的导出函数，帮助找到正确的函数名和签名
"""

import os
import ctypes
import struct
import logging
from typing import List, Dict, Optional

logger = logging.getLogger(__name__)


class DLLExplorer:
    """DLL导出函数探索器"""
    
    def __init__(self, dll_path: str):
        """
        初始化DLL探索器
        
        Args:
            dll_path: DLL文件路径
        """
        self.dll_path = dll_path
        if not os.path.exists(dll_path):
            raise FileNotFoundError(f"DLL文件不存在: {dll_path}")
    
    def explore_exports_pefile(self) -> List[str]:
        """
        使用pefile库探索导出函数（如果可用）
        
        Returns:
            导出函数名列表
        """
        try:
            import pefile
            pe = pefile.PE(self.dll_path)
            
            exports = []
            if hasattr(pe, 'DIRECTORY_ENTRY_EXPORT'):
                for exp in pe.DIRECTORY_ENTRY_EXPORT.symbols:
                    if exp.name:
                        exports.append(exp.name.decode('utf-8', errors='ignore'))
            
            pe.close()
            return exports
        except ImportError:
            logger.warning("pefile库未安装，无法使用pefile方法")
            return []
        except Exception as e:
            logger.error(f"使用pefile探索失败: {e}")
            return []
    
    def explore_exports_ctypes(self) -> List[str]:
        """
        使用ctypes探索导出函数（有限支持）
        
        Returns:
            导出函数名列表（可能不完整）
        """
        exports = []
        try:
            # 尝试加载DLL
            dll = ctypes.CDLL(self.dll_path)
            
            # ctypes无法直接列出导出函数
            # 但我们可以尝试常见的函数名模式
            common_patterns = [
                'ltfs_get_volume_label',
                'ltfs_set_volume_label',
                'ltfs_get_volume_name',
                'ltfs_set_volume_name',
                'ltfs_read_mam',
                'ltfs_write_mam',
                'ltfs_mam_read',
                'ltfs_mam_write',
                'ltfs_get_serial',
                'ltfs_set_serial',
                'ltfs_get_barcode',
                'ltfs_set_barcode',
            ]
            
            for pattern in common_patterns:
                try:
                    func = getattr(dll, pattern)
                    if func:
                        exports.append(pattern)
                except AttributeError:
                    pass
            
        except Exception as e:
            logger.error(f"使用ctypes探索失败: {e}")
        
        return exports
    
    def explore_exports_manual(self) -> List[str]:
        """
        手动解析PE文件导出表（基础实现）
        
        Returns:
            导出函数名列表
        """
        exports = []
        try:
            with open(self.dll_path, 'rb') as f:
                # 读取DOS头
                dos_header = f.read(64)
                if dos_header[:2] != b'MZ':
                    logger.error("不是有效的PE文件")
                    return []
                
                # 获取PE头偏移
                pe_offset = struct.unpack('<I', dos_header[60:64])[0]
                f.seek(pe_offset)
                
                # 读取PE签名
                pe_sig = f.read(4)
                if pe_sig != b'PE\x00\x00':
                    logger.error("PE签名无效")
                    return []
                
                # 读取COFF头
                coff_header = f.read(20)
                machine = struct.unpack('<H', coff_header[0:2])[0]
                num_sections = struct.unpack('<H', coff_header[2:4])[0]
                opt_header_size = struct.unpack('<H', coff_header[16:18])[0]
                
                # 读取可选头
                opt_header = f.read(opt_header_size)
                
                # 查找导出表RVA
                # 在PE32中，导出表在可选头的偏移0x60处（数据目录表的第一项）
                if opt_header_size >= 96:
                    export_rva = struct.unpack('<I', opt_header[96:100])[0]
                    export_size = struct.unpack('<I', opt_header[100:104])[0]
                    
                    if export_rva > 0:
                        # 计算导出表的文件偏移（简化版，假设在第一个节中）
                        # 实际应该遍历节表来正确计算
                        f.seek(pe_offset + 24 + opt_header_size)
                        sections = []
                        for i in range(num_sections):
                            section = f.read(40)
                            if len(section) == 40:
                                section_name = section[0:8].rstrip(b'\x00').decode('ascii', errors='ignore')
                                virtual_addr = struct.unpack('<I', section[8:12])[0]
                                raw_size = struct.unpack('<I', section[16:20])[0]
                                raw_addr = struct.unpack('<I', section[20:24])[0]
                                sections.append({
                                    'name': section_name,
                                    'virtual_addr': virtual_addr,
                                    'raw_size': raw_size,
                                    'raw_addr': raw_addr
                                })
                        
                        # 找到包含导出表的节
                        for section in sections:
                            if section['virtual_addr'] <= export_rva < section['virtual_addr'] + section['raw_size']:
                                export_offset = section['raw_addr'] + (export_rva - section['virtual_addr'])
                                f.seek(export_offset)
                                
                                # 读取导出目录
                                export_dir = f.read(40)
                                if len(export_dir) == 40:
                                    name_rva = struct.unpack('<I', export_dir[12:16])[0]
                                    ordinal_base = struct.unpack('<I', export_dir[16:20])[0]
                                    num_functions = struct.unpack('<I', export_dir[20:24])[0]
                                    num_names = struct.unpack('<I', export_dir[24:28])[0]
                                    addr_table_rva = struct.unpack('<I', export_dir[28:32])[0]
                                    name_table_rva = struct.unpack('<I', export_dir[32:36])[0]
                                    ordinal_table_rva = struct.unpack('<I', export_dir[36:40])[0]
                                    
                                    # 读取名称表
                                    if num_names > 0:
                                        name_table_offset = section['raw_addr'] + (name_table_rva - section['virtual_addr'])
                                        ordinal_table_offset = section['raw_addr'] + (ordinal_table_rva - section['virtual_addr'])
                                        
                                        f.seek(name_table_offset)
                                        name_rvas = []
                                        for i in range(num_names):
                                            name_rva = struct.unpack('<I', f.read(4))[0]
                                            if name_rva > 0:
                                                name_table_offset2 = section['raw_addr'] + (name_rva - section['virtual_addr'])
                                                f.seek(name_table_offset2)
                                                name_bytes = b''
                                                while True:
                                                    byte = f.read(1)
                                                    if byte == b'\x00' or not byte:
                                                        break
                                                    name_bytes += byte
                                                if name_bytes:
                                                    try:
                                                        name = name_bytes.decode('ascii', errors='ignore')
                                                        exports.append(name)
                                                    except:
                                                        pass
                                        
                                        break
        except Exception as e:
            logger.error(f"手动解析PE文件失败: {e}", exc_info=True)
        
        return exports
    
    def explore_all(self) -> Dict[str, List[str]]:
        """
        使用所有可用方法探索导出函数
        
        Returns:
            包含各种方法结果的字典
        """
        results = {
            'pefile': [],
            'ctypes': [],
            'manual': []
        }
        
        # 方法1: pefile（最准确）
        results['pefile'] = self.explore_exports_pefile()
        
        # 方法2: ctypes（有限支持）
        results['ctypes'] = self.explore_exports_ctypes()
        
        # 方法3: 手动解析（基础实现）
        if not results['pefile']:
            results['manual'] = self.explore_exports_manual()
        
        return results
    
    def search_functions(self, keywords: List[str]) -> List[str]:
        """
        搜索包含关键词的导出函数
        
        Args:
            keywords: 关键词列表（如 ['mam', 'serial', 'volume']）
        
        Returns:
            匹配的函数名列表
        """
        all_exports = []
        results = self.explore_all()
        
        # 合并所有结果
        for method_results in results.values():
            all_exports.extend(method_results)
        
        # 去重
        all_exports = list(set(all_exports))
        
        # 搜索匹配的函数
        matches = []
        for export in all_exports:
            export_lower = export.lower()
            for keyword in keywords:
                if keyword.lower() in export_lower:
                    matches.append(export)
                    break
        
        return sorted(matches)


def explore_libltfs_dll(dll_path: Optional[str] = None) -> Dict[str, any]:
    """
    探索libltfs.dll的导出函数
    
    Args:
        dll_path: DLL路径，如果为None则使用默认路径
    
    Returns:
        探索结果字典
    """
    if dll_path is None:
        base_dir = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
        dll_path = os.path.join(base_dir, "ITDT", "libltfs.dll")
    
    if not os.path.exists(dll_path):
        return {
            'error': f'DLL文件不存在: {dll_path}',
            'exports': []
        }
    
    explorer = DLLExplorer(dll_path)
    results = explorer.explore_all()
    
    # 搜索相关的函数
    mam_functions = explorer.search_functions(['mam', 'attribute'])
    volume_functions = explorer.search_functions(['volume', 'label', 'name'])
    serial_functions = explorer.search_functions(['serial', 'sn'])
    barcode_functions = explorer.search_functions(['barcode', 'barcode'])
    
    return {
        'dll_path': dll_path,
        'all_exports': {
            'pefile': results['pefile'],
            'ctypes': results['ctypes'],
            'manual': results['manual']
        },
        'total_count': len(set(results['pefile'] + results['ctypes'] + results['manual'])),
        'mam_functions': mam_functions,
        'volume_functions': volume_functions,
        'serial_functions': serial_functions,
        'barcode_functions': barcode_functions
    }


if __name__ == '__main__':
    import json
    logging.basicConfig(level=logging.INFO)
    
    results = explore_libltfs_dll()
    print(json.dumps(results, indent=2, ensure_ascii=False))

