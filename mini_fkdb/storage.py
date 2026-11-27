"""
自写存储层。

文件布局（数据库目录 dbpath）:
  catalog.json          表结构
  seq.json              各表自增主键计数
  heap/<table>.heap     堆文件：append-only 帧，更新=追加新版本，删除=追加墓碑
  wal.log               预写日志：数据帧 + 提交帧 + 检查点帧

提交协议（no-steal + WAL）:
  1. 事务的全部改动作为数据帧追加 WAL 并 fsync
  2. 追加提交帧并 fsync —— 这一步是原子提交点
  3. 在 storage 全局锁内把帧应用到堆文件和内存索引（对读者表现为原子切换）
崩溃恢复:
  - 堆文件尾部残缺帧直接截掉
  - 扫描堆文件重建主键/外键索引，记录每张表已见到的最大 lsn
  - 重放 WAL 中“已提交”且 lsn 更新的帧；未提交事务的帧天然忽略

索引全部在内存里，启动时从堆文件重建，不需要额外的索引文件。
"""
from __future__ import annotations

import json
import os
import struct
import threading
from typing import Dict, List, Optional, Sequence, Set, Tuple

from . import codec
from .errors import FKDBError
from .schema import Catalog, Table

HEAP_DIR = "heap"
WAL_NAME = "wal.log"
SEQ_NAME = "seq.json"
CATALOG_NAME = "catalog.json"


class Storage:
    def __init__(self, dbpath: str, catalog: Catalog):
        self.dbpath = dbpath
        self.catalog = catalog
        self._mu = threading.RLock()        # 索引/堆一致性的全局短临界区
        self._commit_serial = threading.Lock()

        # 内存状态
        self.pk: Dict[str, Dict[int, dict]] = {}
        self.fk_index: Dict[Tuple[str, str], Dict[object, Set[int]]] = {}
        self._heap_fh: Dict[str, object] = {}
        self._heap_offset: Dict[str, Dict[int, int]] = {}  # 当前版本帧偏移
        self._heap_frame_end: Dict[str, Dict[int, int]] = {}  # 帧结束偏移
        self._max_lsn: Dict[str, int] = {}
        self._lsn = 0
        self._seq: Dict[str, int] = {}

        os.makedirs(os.path.join(dbpath, HEAP_DIR), exist_ok=True)
        self._wal_path = os.path.join(dbpath, WAL_NAME)
        self._recover()

    # ============================================================ 打开/恢复
    def _recover(self):
        # 1. 序列计数
        seq_path = os.path.join(self.dbpath, SEQ_NAME)
        if os.path.exists(seq_path):
            with open(seq_path, "r", encoding="utf-8") as f:
                self._seq = json.load(f)

        # 2. 初始化索引骨架
        for tname, table in self.catalog.tables.items():
            self.pk[tname] = {}
            self._max_lsn[tname] = 0
            self._heap_offset[tname] = {}
            self._heap_frame_end[tname] = {}
            for fk in table.fks:
                self.fk_index[(tname, fk.name)] = {}

        # 3. 扫堆文件重建索引，截掉尾部残缺帧
        for tname, table in self.catalog.tables.items():
            self._rebuild_table(table)

        # 4. 重放已提交 WAL
        self._replay_wal()

        # 5. 打开堆句柄 & WAL 句柄（追加模式）
        for tname in self.catalog.tables:
            self._heap_fh[tname] = open(
                self._heap_path(tname), "ab", buffering=0)
        self._wal_fh = open(self._wal_path, "ab", buffering=0)

    def _heap_path(self, tname: str) -> str:
        return os.path.join(self.dbpath, HEAP_DIR, tname + ".heap")

    def _rebuild_table(self, table: Table):
        path = self._heap_path(table.name)
        pk = self.pk[table.name]
        if not os.path.exists(path):
            return
        good_size = 0
        with open(path, "rb") as f:
            while True:
                frame_start = f.tell()
                frame = codec.read_heap_frame(f)
                if frame is None:
                    good_size = f.tell()
                    break
                rowid, lsn, payload = frame
                frame_end = f.tell()
                if lsn > self._max_lsn[table.name]:
                    self._max_lsn[table.name] = lsn
                self._lsn = max(self._lsn, lsn)
                if payload is None:
                    pk.pop(rowid, None)
                else:
                    pk[rowid] = codec.decode_row(table, payload)
                self._heap_offset[table.name][rowid] = frame_start
                self._heap_frame_end[table.name][rowid] = frame_end
                good_size = frame_end
            # 尾部残缺（写一半崩溃）→ 截断
            f.seek(0, os.SEEK_END)
            if f.tell() != good_size:
                os.truncate(path, good_size)
        # 重建外键索引
        for fk in table.fks:
            idx: Dict[object, Set[int]] = {}
            for rowid, row in pk.items():
                v = row[fk.columns[0]]
                if v is not None:
                    idx.setdefault(v, set()).add(rowid)
            self.fk_index[(table.name, fk.name)] = idx

    def _replay_wal(self):
        """重放 WAL：只应用带提交帧的事务，且只应用比堆里更新的帧。"""
        if not os.path.exists(self._wal_path):
            return
        committed: Set[int] = set()
        txn_frames: Dict[int, List[dict]] = {}
        checkpoint_lsn = 0
        with open(self._wal_path, "rb") as f:
            while True:
                try:
                    fr = codec.read_wal_frame(f)
                except FKDBError:
                    break  # 尾部写坏，停止解析
                if fr is None:
                    break
                if fr["kind"] == "checkpoint":
                    checkpoint_lsn = fr["lsn"]
                    committed.clear()
                    txn_frames.clear()
                    continue
                if fr["kind"] == "commit":
                    committed.add(fr["txn"])
                    continue
                if fr["lsn"] <= checkpoint_lsn:
                    continue
                txn_frames.setdefault(fr["txn"], []).append(fr)

        for txn_id in committed:
            for fr in txn_frames.get(txn_id, ()):
                tname = fr["table"]
                if fr["lsn"] <= self._max_lsn.get(tname, 0):
                    continue  # 堆里已有同 rowid 更新或同等版本
                self._apply_frame(tname, fr["rowid"], fr["op"],
                                  fr["payload"], fr["lsn"], write_heap=True)
        # 重放完成后写一个检查点并精简 WAL
        if committed or checkpoint_lsn:
            self._lsn = max(self._lsn, checkpoint_lsn)
            with open(self._wal_path, "wb", buffering=0) as f:
                f.write(codec.pack_wal_checkpoint(self._lsn))

    # ============================================================ 帧应用
    def _write_heap_frame(self, tname: str, rowid: int, lsn: int,
                          payload: Optional[bytes]):
        """
        写一帧。堆文件是 append-only：更新/删除都追加新版本帧，
        偏移表只指向每 rowid 的最新版本。旧帧留在文件里，由
        重建扫描按 rowid 的最新出现位置自然覆盖，vacuum 时再物理回收。
        不做原地覆写，避免大小不一致时槽位残留造成扫描错位。
        """
        fh = self._heap_fh[tname]
        frame = codec.pack_heap_frame(
            rowid, lsn, payload if payload is not None else None)
        fh.seek(0, os.SEEK_END)
        start = fh.tell()
        fh.write(frame)
        self._heap_offset[tname][rowid] = start
        self._heap_frame_end[tname][rowid] = start + len(frame)
    def _apply_frame(self, tname: str, rowid: int, op: int,
                     payload: Optional[bytes], lsn: int,
                     write_heap: bool):
        table = self.catalog.table(tname)
        pk = self.pk[tname]
        old_row = pk.get(rowid)
        if op == codec.OP_UPSERT:
            row = codec.decode_row(table, payload)
            pk[rowid] = row
            for fk in table.fks:
                col = fk.columns[0]
                idx = self.fk_index[(tname, fk.name)]
                if old_row is not None and old_row[col] is not None:
                    s = idx.get(old_row[col])
                    if s is not None:
                        s.discard(rowid)
                if row[col] is not None:
                    idx.setdefault(row[col], set()).add(rowid)
        elif op == codec.OP_DELETE:
            pk.pop(rowid, None)
            if old_row is not None:
                for fk in table.fks:
                    v = old_row[fk.columns[0]]
                    if v is not None:
                        s = self.fk_index[(tname, fk.name)].get(v)
                        if s is not None:
                            s.discard(rowid)
        else:  # pragma: no cover
            raise FKDBError("未知 WAL op %d" % op)

        self._max_lsn[tname] = max(self._max_lsn[tname], lsn)
        if write_heap:
            frame_payload = payload if op == codec.OP_UPSERT else None
            self._write_heap_frame(tname, rowid, lsn, frame_payload)

    # ============================================================ 公共 API
    def next_lsn(self) -> int:
        with self._mu:
            self._lsn += 1
            return self._lsn

    def reserve_lsns(self, n: int) -> int:
        """批量预分配 n 个连续 lsn，返回起始 lsn（区间 [start, start+n)）。"""
        with self._mu:
            start = self._lsn + 1
            self._lsn += n
            return start

    def allocate_rowid(self, tname: str) -> int:
        with self._mu:
            nxt = self._seq.get(tname, 0) + 1
            self._seq[tname] = nxt
            return nxt

    def reserve_rowids(self, tname: str, n: int) -> int:
        """批量装载用：保留 n 个 id，返回起始 id（区间 [start, start+n)）。"""
        with self._mu:
            start = self._seq.get(tname, 0) + 1
            self._seq[tname] = start + n - 1
            return start

    def commit_txn(self, txn_id: int,
                   mutations: Sequence[Tuple[int, str, int, int, Optional[bytes]]]
                   ) -> List[int]:
        """
        mutations: (lsn, table, rowid, op, payload) —— lsn 由事务提前申请。
        返回写入的 lsn 列表。WAL 先行，提交帧之后才在锁内原子应用。
        """
        if not mutations:
            return []
        with self._commit_serial:
            # 1. WAL 数据帧：内存拼一整块，一次 write + 一次 fsync。
            #    同时把堆帧字节缓存下来，第 3 步直接复用，不重复打包。
            wal_buf = bytearray()
            heap_frames: Dict[str, List[Tuple[int, bytes]]] = {}
            for lsn, tname, rowid, op, payload in mutations:
                wal_buf += codec.pack_wal_data(
                    txn_id, lsn, op, tname, rowid, payload)
                frame_payload = payload if op == codec.OP_UPSERT else None
                heap_frames.setdefault(tname, []).append(
                    (rowid, codec.pack_heap_frame(rowid, lsn, frame_payload)))
            self._wal_fh.write(wal_buf)
            self._wal_fh.flush()
            os.fsync(self._wal_fh.fileno())
            # 2. 提交帧（原子提交点）
            commit_lsn = self.next_lsn()
            self._wal_fh.write(codec.pack_wal_commit(txn_id, commit_lsn))
            self._wal_fh.flush()
            os.fsync(self._wal_fh.fileno())
            # 3. 锁内应用，对读者是一次性原子切换
            with self._mu:
                for lsn, tname, rowid, op, payload in mutations:
                    self._apply_frame(tname, rowid, op, payload, lsn,
                                      write_heap=False)
                # 每张表一整块顺序写，避免逐行系统调用
                for tname, frames in heap_frames.items():
                    fh = self._heap_fh[tname]
                    fh.seek(0, os.SEEK_END)
                    cur = fh.tell()
                    offsets = self._heap_offset[tname]
                    ends = self._heap_frame_end[tname]
                    blob = bytearray()
                    for rowid, frame in frames:
                        offsets[rowid] = cur
                        ends[rowid] = cur + len(frame)
                        blob += frame
                        cur += len(frame)
                    fh.write(blob)
                self._persist_seq()
            return [m[0] for m in mutations] + [commit_lsn]

    def _persist_seq(self):
        tmp = os.path.join(self.dbpath, SEQ_NAME + ".tmp")
        with open(tmp, "w", encoding="utf-8") as f:
            json.dump(self._seq, f)
        os.replace(tmp, os.path.join(self.dbpath, SEQ_NAME))

    # ------------------------------------------------------------ 读 API
    def get(self, tname: str, rowid: int) -> Optional[dict]:
        with self._mu:
            row = self.pk[tname].get(rowid)
            return dict(row) if row is not None else None

    def count(self, tname: str) -> int:
        with self._mu:
            return len(self.pk[tname])

    def all_rows(self, tname: str) -> List[dict]:
        with self._mu:
            return [dict(r) for r in self.pk[tname].values()]

    def all_rowids(self, tname: str) -> List[int]:
        with self._mu:
            return list(self.pk[tname].keys())

    def find_by_fk(self, tname: str, fk_name: str,
                   value: object) -> List[int]:
        with self._mu:
            s = self.fk_index[(tname, fk_name)].get(value)
            return list(s) if s is not None else []

    def fk_value_exists(self, tname: str, fk_name: str, value: object) -> bool:
        with self._mu:
            return bool(self.fk_index[(tname, fk_name)].get(value))

    # ------------------------------------------------------------ 批量装载
    def bulk_load(self, tname: str, rowid: int, values: dict):
        """
        初始装载专用：绕过 WAL/事务直接写堆并更新索引。
        只允许在数据库还没有业务事务时使用（见 Database.bulk_loader）。
        同一 rowid 的回填会原地覆写或追加新版本（lsn 固定为 0，
        之后所有真实事务 lsn 都更大，恢复时不会被旧 WAL 回退）。
        """
        table = self.catalog.table(tname)
        payload = codec.encode_row(table, values)
        lsn = 0
        with self._mu:
            self._write_heap_frame(tname, rowid, lsn, payload)
            self.pk[tname][rowid] = values
            for fk in table.fks:
                idx = self.fk_index[(tname, fk.name)]
                v = values[fk.columns[0]]
                if v is not None:
                    idx.setdefault(v, set()).add(rowid)

    def flush_bulk(self):
        with self._mu:
            for fh in self._heap_fh.values():
                fh.flush()
                os.fsync(fh.fileno())
            self._persist_seq()
            # 干净的 WAL 基线
            with open(self._wal_path, "wb", buffering=0) as f:
                f.write(codec.pack_wal_checkpoint(0))

    # ------------------------------------------------------------ 检查点/关闭
    def checkpoint(self):
        with self._commit_serial:
            with self._mu:
                for fh in self._heap_fh.values():
                    fh.flush()
                    os.fsync(fh.fileno())
                lsn = self._lsn
            with open(self._wal_path, "wb", buffering=0) as f:
                f.write(codec.pack_wal_checkpoint(lsn))
                f.flush()
                os.fsync(f.fileno())

    def close(self):
        with self._mu:
            for fh in self._heap_fh.values():
                fh.flush()
                fh.close()
            self._wal_fh.close()
            self._persist_seq()

    # ------------------------------------------------------------ 完整性检查
    def dangling_refs(self) -> List[dict]:
        """全库扫描，返回所有悬空外键引用（正常情况下必须为空）。"""
        out = []
        with self._mu:
            for tname, table in self.catalog.tables.items():
                pk = self.pk[tname]
                for fk in table.fks:
                    ref_pk = self.pk[fk.ref_table]
                    for rowid, row in pk.items():
                        v = row[fk.columns[0]]
                        if v is not None and v not in ref_pk:
                            out.append({"table": tname, "rowid": rowid,
                                        "fk": fk.name, "value": v})
        return out
