"""
表结构 / 外键 / 数据目录（catalog）。

目录是纯数据结构，序列化为 JSON 落盘，存储层不依赖任何现成数据库。
本引擎支持单列外键、单列整数主键（rowid 即主键值），覆盖题目要求的
全部场景：自引用、同表多外键、多表成环、多层多分支级联。
"""
from __future__ import annotations

import json
import os
from dataclasses import dataclass, field, asdict
from typing import Dict, List, Optional, Tuple

from .errors import SchemaError

# 支持的删除/更新动作
CASCADE = "CASCADE"
SET_NULL = "SET NULL"
SET_DEFAULT = "SET DEFAULT"
RESTRICT = "RESTRICT"
NO_ACTION = "NO ACTION"

ALL_ACTIONS = (CASCADE, SET_NULL, SET_DEFAULT, RESTRICT, NO_ACTION)

# 列类型
INTEGER = "INTEGER"
BIGINT = "BIGINT"
TEXT = "TEXT"
REAL = "REAL"
BOOLEAN = "BOOLEAN"
SUPPORTED_TYPES = (INTEGER, BIGINT, TEXT, REAL, BOOLEAN)


@dataclass
class Column:
    name: str
    ctype: str
    nullable: bool = True
    default: Optional[object] = None  # None 既可能是默认值 NULL，也可能表示没配默认值
    has_default: bool = False
    pk: bool = False


@dataclass
class ForeignKey:
    """
    本表的列 columns[0] 引用 ref_table 的列 ref_columns[0]。
    on_delete / on_update 取 ALL_ACTIONS 中的动作。
    not_null_fk=True 时不允许 SET NULL（建表期检查）。
    """
    name: str
    columns: List[str]
    ref_table: str
    ref_columns: List[str]
    on_delete: str = RESTRICT
    on_update: str = NO_ACTION
    deferrable: bool = False          # NO ACTION 一律按可延迟到语句末处理
    not_null_fk: bool = False

    def validate(self):
        if self.on_delete not in ALL_ACTIONS:
            raise SchemaError("未知 on_delete 动作: %s" % self.on_delete)
        if self.on_update not in ALL_ACTIONS:
            raise SchemaError("未知 on_update 动作: %s" % self.on_update)
        if len(self.columns) != 1 or len(self.ref_columns) != 1:
            raise SchemaError("本引擎只支持单列外键: %s" % self.name)
        if self.on_delete == SET_NULL and self.not_null_fk:
            raise SchemaError(
                "外键 %s 配了 ON DELETE SET NULL，但外键列 NOT NULL" % self.name)


@dataclass
class Table:
    name: str
    columns: List[Column]
    fks: List[ForeignKey] = field(default_factory=list)
    pk_column: Optional[str] = None

    def col(self, name: str) -> Column:
        for c in self.columns:
            if c.name == name:
                return c
        raise SchemaError("表 %s 没有列 %s" % (self.name, name))

    def has_column(self, name: str) -> bool:
        return any(c.name == name for c in self.columns)

    def incoming_fks(self, catalog: "Catalog") -> List[ForeignKey]:
        """所有指向本表的外键（可能来自本表自身）。"""
        out = []
        for t in catalog.tables.values():
            for fk in t.fks:
                if fk.ref_table == self.name:
                    out.append(fk)
        return out


class Catalog:
    """整个数据库的结构目录。"""

    def __init__(self, tables: Optional[Dict[str, Table]] = None):
        self.tables: Dict[str, Table] = dict(tables or {})

    # ---- DDL ----------------------------------------------------------
    def create_table(self, name: str, columns: List[Column],
                     fks: Optional[List[ForeignKey]] = None):
        if name in self.tables:
            raise SchemaError("表 %s 已存在" % name)
        pks = [c for c in columns if c.pk]
        if len(pks) != 1:
            raise SchemaError("表 %s 必须恰好有一个主键列" % name)
        pk = pks[0]
        if pk.ctype not in (INTEGER, BIGINT):
            raise SchemaError("表 %s 主键必须是整数" % name)
        for c in columns:
            if c.ctype not in SUPPORTED_TYPES:
                raise SchemaError("表 %s 列 %s 类型不支持: %s"
                                  % (name, c.name, c.ctype))
        t = Table(name=name, columns=list(columns), fks=list(fks or []),
                  pk_column=pk.name)
        self.tables[name] = t
        # 跨表引用的目标表允许稍后才建（create_all 时统一 validate）
        return t

    def validate(self):
        """全部表建完后做一致性检查（环、自引用都允许）。"""
        for t in self.tables.values():
            col_names = {c.name for c in t.columns}
            for fk in t.fks:
                fk.validate()
                if fk.columns[0] not in col_names:
                    raise SchemaError("表 %s 外键 %s 的列不存在"
                                      % (t.name, fk.name))
                ref = self.tables.get(fk.ref_table)
                if ref is None:
                    raise SchemaError("外键 %s 引用了不存在的表 %s"
                                      % (fk.name, fk.ref_table))
                if fk.ref_columns[0] != ref.pk_column:
                    raise SchemaError(
                        "外键 %s 只支持引用主键（表 %s 的 %s）"
                        % (fk.name, ref.name, ref.pk_column))
                fk_col = t.col(fk.columns[0])
                fk.not_null_fk = not fk_col.nullable
                if fk.on_delete == SET_DEFAULT:
                    if not fk_col.has_default:
                        raise SchemaError(
                            "外键 %s 配 SET DEFAULT 但列 %s 没有默认值"
                            % (fk.name, fk_col.name))
                    if fk_col.default is None and not fk_col.nullable:
                        raise SchemaError(
                            "外键 %s 的 SET DEFAULT 是 NULL，但列 NOT NULL"
                            % fk.name)
                if fk.on_update == SET_DEFAULT and not fk_col.has_default:
                    raise SchemaError(
                        "外键 %s ON UPDATE SET DEFAULT 但列没有默认值" % fk.name)

    # ---- 查询辅助 ------------------------------------------------------
    def table(self, name: str) -> Table:
        if name not in self.tables:
            raise SchemaError("未知表: %s" % name)
        return self.tables[name]

    def fk_by_name(self, name: str) -> Tuple[Table, ForeignKey]:
        for t in self.tables.values():
            for fk in t.fks:
                if fk.name == name:
                    return t, fk
        raise SchemaError("未知外键: %s" % name)

    # ---- 持久化 --------------------------------------------------------
    def to_json(self) -> str:
        return json.dumps(
            {"tables": {name: asdict(t) for name, t in self.tables.items()}},
            ensure_ascii=False, indent=2)

    @classmethod
    def from_json(cls, text: str) -> "Catalog":
        raw = json.loads(text)
        tables = {}
        for name, td in raw["tables"].items():
            cols = [Column(**c) for c in td["columns"]]
            fks = [ForeignKey(**f) for f in td.get("fks", [])]
            tables[name] = Table(name=name, columns=cols, fks=fks,
                                 pk_column=td["pk_column"])
        cat = cls(tables)
        return cat

    def save(self, path: str):
        tmp = path + ".tmp"
        with open(tmp, "w", encoding="utf-8") as f:
            f.write(self.to_json())
        os.replace(tmp, path)

    @classmethod
    def load(cls, path: str) -> "Catalog":
        with open(path, "r", encoding="utf-8") as f:
            return cls.from_json(f.read())
