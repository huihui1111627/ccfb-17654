"""
mini_fkdb —— 带外键约束的自写关系存储引擎（纯 Python 标准库）。

模块组成：
  schema     表/列/外键/目录，JSON 持久化
  codec      手写二进制帧与行编解码（堆文件、WAL）
  locking    行级 S/X 锁、FIFO 公平授予、等待图死锁检测
  storage    堆文件 + WAL + 内存主键/外键索引 + 崩溃恢复
  planner    级联规划器（CASCADE/SET NULL/SET DEFAULT/RESTRICT/NO ACTION）
  transaction 严格 2PL 事务、DML、计划执行
  db         引擎门面、拆批删除、预估核对
"""
from .db import Database
from .schema import (Catalog, Table, Column, ForeignKey,
                     CASCADE, SET_NULL, SET_DEFAULT, RESTRICT, NO_ACTION,
                     INTEGER, BIGINT, TEXT, REAL, BOOLEAN)
from .errors import (FKDBError, SchemaError, NotFound, UniqueViolation,
                     ForeignKeyViolation, LockTimeout, TransactionAborted)

__all__ = [
    "Database", "Catalog", "Table", "Column", "ForeignKey",
    "CASCADE", "SET_NULL", "SET_DEFAULT", "RESTRICT", "NO_ACTION",
    "INTEGER", "BIGINT", "TEXT", "REAL", "BOOLEAN",
    "FKDBError", "SchemaError", "NotFound", "UniqueViolation",
    "ForeignKeyViolation", "LockTimeout", "TransactionAborted",
]
