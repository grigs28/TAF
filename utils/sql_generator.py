#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
原生 SQL 生成器
用于 openGauss 模式，不依赖 SQLAlchemy
"""

import logging
from typing import List, Tuple, Optional
from datetime import datetime

logger = logging.getLogger(__name__)


def generate_create_table_sql(table_name: str, columns: List[Tuple[str, str, bool, Optional[str], Optional[str]]]) -> str:
    """
    生成 CREATE TABLE SQL 语句（原生 SQL，不依赖 SQLAlchemy）
    
    Args:
        table_name: 表名
        columns: 列定义列表，每个元素为 (column_name, column_type, nullable, default_value, constraints)
                 column_type 为 PostgreSQL/openGauss 类型（如 TEXT, INTEGER, BIGINT, TIMESTAMP, BOOLEAN）
                 nullable: True 表示可为空，False 表示不可为空
                 default_value: 默认值（字符串形式，如 'FALSE', '0', 'CURRENT_TIMESTAMP'）
                 constraints: 约束（如 'PRIMARY KEY', 'REFERENCES table(column)'）
    
    Returns:
        CREATE TABLE SQL 语句
    """
    column_defs = []
    primary_key_cols = []
    
    for col_info in columns:
        if len(col_info) == 5:
            col_name, col_type, nullable, default_val, constraints = col_info
        else:
            # 兼容旧格式
            col_name, col_type, nullable, default_val = col_info[:4]
            constraints = col_info[4] if len(col_info) > 4 else None
        
        col_def = f'    {col_name} {col_type}'
        if not nullable:
            col_def += ' NOT NULL'
        if default_val is not None:
            col_def += f' DEFAULT {default_val}'
        if constraints:
            if constraints == 'PRIMARY KEY':
                # 主键单独处理
                primary_key_cols.append(col_name)
            else:
                col_def += f' {constraints}'
        column_defs.append(col_def)
    
    sql = f'CREATE TABLE {table_name} (\n'
    sql += ',\n'.join(column_defs)
    
    # 如果有主键列，添加主键约束
    if primary_key_cols:
        sql += f',\n    PRIMARY KEY ({", ".join(primary_key_cols)})'
    
    sql += '\n)'
    return sql


def get_table_definition_from_model(table_name: str) -> Optional[List[Tuple[str, str, bool, Optional[str], Optional[str]]]]:
    """
    从模型定义中提取表结构信息（不依赖 SQLAlchemy 编译）
    
    注意：这个方法只是从模型列定义中提取信息，不调用 SQLAlchemy 的编译功能
    
    Returns:
        List of (column_name, column_type, nullable, default_value, constraints)
        constraints 包括 PRIMARY KEY, FOREIGN KEY 等
    """
    from models.base import Base
    
    # 获取表对象
    table = Base.metadata.tables.get(table_name)
    if table is None:
        return None
    
    columns = []
    primary_keys = [pk.name for pk in table.primary_key.columns]
    
    for column in table.columns:
        # 获取列名
        col_name = column.name
        
        # 获取列类型（转换为 PostgreSQL 类型）
        col_type = _convert_sqlalchemy_type_to_postgresql(column.type)
        
        # 检查是否是自增主键（SERIAL/BIGSERIAL）
        if col_name == 'id' and column.primary_key and column.autoincrement:
            # 根据类型选择 SERIAL 或 BIGSERIAL
            if col_type == 'BIGINT':
                col_type = 'BIGSERIAL'
            else:
                col_type = 'SERIAL'
        
        # 获取是否可为空
        nullable = column.nullable
        
        # 获取默认值
        default_val = None
        if column.default is not None:
            if hasattr(column.default, 'arg'):
                default_arg = column.default.arg
                
                # 检查是否是 datetime 函数（如 datetime.utcnow）
                if callable(default_arg):
                    # 检查是否是 datetime.utcnow 或类似的函数
                    func_name = getattr(default_arg, '__name__', '')
                    func_module = getattr(default_arg, '__module__', None)
                    # 检查是否是 datetime.utcnow 或 datetime.now
                    if func_name in ['utcnow', 'now'] and func_module and ('datetime' in func_module or func_module == 'builtins'):
                        # 对于 TIMESTAMP 类型，使用 CURRENT_TIMESTAMP
                        if col_type in ['TIMESTAMP', 'TIMESTAMP WITH TIME ZONE']:
                            default_val = 'CURRENT_TIMESTAMP'
                        else:
                            default_val = 'CURRENT_TIMESTAMP'
                    else:
                        # 其他可调用对象，跳过默认值（避免生成无效的 SQL）
                        logger.warning(f"列 {col_name} 的默认值是可调用对象 {func_name}，跳过默认值设置")
                        default_val = None
                # 检查是否是枚举类型（col_type 应该是字符串类型）
                elif isinstance(default_arg, type) or (hasattr(default_arg, '__class__') and 'Enum' in str(type(default_arg))):
                    col_type_lower = str(col_type).lower() if col_type else ''
                    is_enum_type = col_type_lower in ['tapestatus', 'tapeoperationtype', 'tapeloglevel', 
                                                       'backuptasktype', 'backuptaskstatus', 'backupsetstatus', 'backupfiletype',
                                                       'scheduletype', 'scheduledtaskstatus', 'taskactiontype',
                                                       'loglevel', 'logcategory', 'operationtype', 'errorlevel',
                                                       'configtype', 'configcategory', 'userstatus', 'permissioncategory']
                    
                    if is_enum_type:
                        # 枚举类型的默认值需要类型转换
                        enum_value_str = None
                        if hasattr(default_arg, 'value'):
                            # 如果是枚举对象，获取其值
                            enum_value_str = str(default_arg.value)
                        elif isinstance(default_arg, str):
                            enum_value_str = default_arg
                        else:
                            # 尝试转换为字符串
                            enum_value_str = str(default_arg)
                        
                        if enum_value_str:
                            # 确保值被引号包围，并添加类型转换
                            default_val = f"'{enum_value_str}'::{col_type_lower}"
                        else:
                            logger.warning(f"列 {col_name} 的枚举类型默认值无法解析，跳过默认值设置")
                            default_val = None
                    else:
                        default_val = str(default_arg)
                else:
                    # 检查是否是枚举类型（col_type 应该是字符串类型）
                    col_type_lower = str(col_type).lower() if col_type else ''
                    is_enum_type = col_type_lower in ['tapestatus', 'tapeoperationtype', 'tapeloglevel', 
                                                       'backuptasktype', 'backuptaskstatus', 'backupsetstatus', 'backupfiletype',
                                                       'scheduletype', 'scheduledtaskstatus', 'taskactiontype',
                                                       'loglevel', 'logcategory', 'operationtype', 'errorlevel',
                                                       'configtype', 'configcategory', 'userstatus', 'permissioncategory']
                    
                    if is_enum_type:
                        # 枚举类型的默认值需要类型转换
                        enum_value_str = None
                        if hasattr(default_arg, 'value'):
                            # 如果是枚举对象，获取其值
                            enum_value_str = str(default_arg.value)
                        elif isinstance(default_arg, str):
                            enum_value_str = default_arg
                        else:
                            # 尝试转换为字符串
                            enum_value_str = str(default_arg)
                        
                        if enum_value_str:
                            # 确保值被引号包围，并添加类型转换
                            default_val = f"'{enum_value_str}'::{col_type_lower}"
                        else:
                            logger.warning(f"列 {col_name} 的枚举类型默认值无法解析，跳过默认值设置")
                            default_val = None
                    elif isinstance(default_arg, str):
                        default_val = f"'{default_arg}'"
                    elif default_arg is True:
                        default_val = 'TRUE'
                    elif default_arg is False:
                        default_val = 'FALSE'
                    elif default_arg == 0:
                        default_val = '0'
                    else:
                        default_val = str(default_arg)
            else:
                # 如果 default 没有 arg 属性，尝试直接转换
                try:
                    default_str = str(column.default)
                    # 检查是否是枚举类型
                    col_type_lower = str(col_type).lower() if col_type else ''
                    is_enum_type = col_type_lower in ['tapestatus', 'tapeoperationtype', 'tapeloglevel', 
                                                       'backuptasktype', 'backuptaskstatus', 'backupsetstatus', 'backupfiletype',
                                                       'scheduletype', 'scheduledtaskstatus', 'taskactiontype',
                                                       'loglevel', 'logcategory', 'operationtype', 'errorlevel',
                                                       'configtype', 'configcategory', 'userstatus', 'permissioncategory']
                    if is_enum_type and hasattr(column.default, 'value'):
                        default_val = f"'{column.default.value}'::{col_type_lower}"
                    else:
                        default_val = default_str
                except Exception:
                    logger.warning(f"列 {col_name} 的默认值无法处理，跳过默认值设置")
                    default_val = None
        
        # 检查是否是主键
        constraints = None
        if col_name in primary_keys:
            constraints = 'PRIMARY KEY'
        
        # 检查唯一约束（unique=True）
        is_unique = False
        # 方法1: 检查 Column 的 unique 参数（SQLAlchemy 1.4+）
        if hasattr(column, 'unique') and column.unique:
            is_unique = True
            logger.debug(f"列 {table.name}.{col_name} 通过 column.unique 检测到唯一约束")
        # 方法2: 检查 Column 的 _user_defined_unique 属性（SQLAlchemy 内部）
        elif hasattr(column, '_user_defined_unique') and column._user_defined_unique:
            is_unique = True
            logger.debug(f"列 {table.name}.{col_name} 通过 _user_defined_unique 检测到唯一约束")
        # 方法3: 检查索引中的唯一约束
        if not is_unique:
            for index in table.indexes:
                index_col_names = [c.name for c in index.columns]
                if col_name in index_col_names and index.unique:
                    is_unique = True
                    logger.debug(f"列 {table.name}.{col_name} 通过索引 {index.name} 检测到唯一约束")
                    break
        # 方法4: 检查表级别的唯一约束（通过 UniqueConstraint）
        if not is_unique:
            for constraint in table.constraints:
                if hasattr(constraint, 'columns'):
                    constraint_col_names = [c.name for c in constraint.columns]
                    if col_name in constraint_col_names:
                        # 检查是否是 UniqueConstraint
                        from sqlalchemy import UniqueConstraint
                        if isinstance(constraint, UniqueConstraint):
                            is_unique = True
                            logger.debug(f"列 {table.name}.{col_name} 通过 UniqueConstraint 检测到唯一约束")
                            break
                        # 也检查约束的 unique 属性
                        elif hasattr(constraint, 'unique') and constraint.unique:
                            is_unique = True
                            logger.debug(f"列 {table.name}.{col_name} 通过约束 {constraint} 检测到唯一约束")
                            break
        
        # 检查外键
        fk_constraint = None
        for fk in column.foreign_keys:
            ref_table = fk.column.table.name
            ref_column = fk.column.name
            fk_constraint = f'REFERENCES {ref_table}({ref_column})'
            break
        
        # 组合约束：唯一约束和外键约束
        if constraints == 'PRIMARY KEY':
            # 主键优先，不添加其他约束
            pass
        elif is_unique and fk_constraint:
            # 既有唯一约束又有外键约束
            constraints = f'{fk_constraint} UNIQUE'
        elif is_unique:
            constraints = 'UNIQUE'
        elif fk_constraint:
            constraints = fk_constraint
        
        columns.append((col_name, col_type, nullable, default_val, constraints))
    
    return columns


def _convert_sqlalchemy_type_to_postgresql(sqlalchemy_type) -> str:
    """
    将 SQLAlchemy 类型转换为 PostgreSQL/openGauss 类型（不依赖 SQLAlchemy 编译）
    """
    # 处理类型包装（如 TypeDecorator）
    actual_type = sqlalchemy_type
    while hasattr(actual_type, 'impl'):
        actual_type = actual_type.impl
    
    type_name = type(actual_type).__name__
    
    # 基本类型映射
    type_mapping = {
        'Integer': 'INTEGER',
        'BigInteger': 'BIGINT',
        'String': 'TEXT',  # openGauss 使用 TEXT 而不是 VARCHAR
        'Text': 'TEXT',
        'Boolean': 'BOOLEAN',
        'DateTime': 'TIMESTAMP',
        'Float': 'REAL',
        'Double': 'DOUBLE PRECISION',
        'JSON': 'JSONB',  # openGauss 使用 JSONB
        'Date': 'DATE',
        'Time': 'TIME',
    }
    
    if type_name in type_mapping:
        return type_mapping[type_name]
    
    # 处理 Enum 类型
    if type_name == 'Enum':
        # 获取枚举类型名（需要在创建表前先创建枚举类型）
        enum_type_name = None
        
        # 方法1: 从 SQLAlchemy Enum 类型的 name 属性获取
        if hasattr(sqlalchemy_type, 'name') and sqlalchemy_type.name:
            enum_type_name = sqlalchemy_type.name
        elif hasattr(actual_type, 'name') and actual_type.name:
            enum_type_name = actual_type.name
        
        # 方法2: 从枚举类获取类型名
        if not enum_type_name:
            if hasattr(sqlalchemy_type, 'enums') and sqlalchemy_type.enums:
                # 从枚举类获取类型名（枚举类名转小写）
                enum_class = sqlalchemy_type.enums[0] if isinstance(sqlalchemy_type.enums, (list, tuple)) else sqlalchemy_type.enums
                if hasattr(enum_class, '__name__'):
                    enum_type_name = enum_class.__name__.lower()
            elif hasattr(actual_type, 'enums') and actual_type.enums:
                enum_class = actual_type.enums[0] if isinstance(actual_type.enums, (list, tuple)) else actual_type.enums
                if hasattr(enum_class, '__name__'):
                    enum_type_name = enum_class.__name__.lower()
        
        # 如果仍然无法获取，使用默认值
        if not enum_type_name:
            logger.warning(f"无法获取枚举类型名，使用 'text' 作为默认类型")
            return 'TEXT'
        
        return enum_type_name.lower()  # 枚举类型名通常是小写
    
    # 处理带长度的 String
    if type_name == 'String' and hasattr(actual_type, 'length'):
        # 即使有长度，openGauss 也使用 TEXT
        return 'TEXT'
    
    # 默认返回 TEXT
    logger.warning(f"未知的 SQLAlchemy 类型: {type_name}，使用 TEXT 作为默认类型")
    return 'TEXT'

