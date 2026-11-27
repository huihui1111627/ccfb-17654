"""
手写二进制编解码。

堆文件帧（heap frame）:
  magic(1)=0xA1 | flags(1) | rowid(uint64, 8) | lsn(uint64, 8)
  | payload_len(uint32, 4) | payload
flags: bit0 = 墓碑（删除标记）

WAL 帧:
  数据帧 magic=0xA2 | txn_id(8) | lsn(8) | op(1)
        | tname_len(uint16) | tname(utf8) | rowid(8)
        | payload_len(uint32) | payload      # op=1 upsert 才有 payload
  提交帧 magic=0xA3 | txn_id(8) | lsn(8)
  检查点帧 magic=0xA4 | lsn(8)

行 payload: ncols(uint16) 后跟若干 [tag(1)][value]，按表列顺序。
整数用 zigzag varint，省空间同时支持负数。
"""
from __future__ import annotations

import os
import struct
from typing import BinaryIO, Dict, List, Optional, Tuple

from .errors import FKDBError
from .schema import Table, INTEGER, BIGINT, TEXT, REAL, BOOLEAN

MAGIC_HEAP = 0xA1
MAGIC_WAL_DATA = 0xA2
MAGIC_WAL_COMMIT = 0xA3
MAGIC_WAL_CHECKPOINT = 0xA4

FLAG_TOMBSTONE = 0x01

OP_UPSERT = 1
OP_DELETE = 2

TAG_NULL = 0
TAG_INT = 1
TAG_TEXT = 2
TAG_REAL = 3
TAG_BOOL = 4


# ---------------------------------------------------------------- varint
def encode_varint(value: int) -> bytes:
    # zigzag 编码，让小绝对值（含负数）占用更少字节
    u = (value << 1) ^ (value >> 63)
    out = bytearray()
    while True:
        b = u & 0x7F
        u >>= 7
        if u:
            out.append(b | 0x80)
        else:
            out.append(b)
            return bytes(out)


def decode_varint(buf: bytes, pos: int) -> Tuple[int, int]:
    shift = 0
    u = 0
    while True:
        if pos >= len(buf):
            raise FKDBError("varint 截断")
        b = buf[pos]
        pos += 1
        u |= (b & 0x7F) << shift
        if not (b & 0x80):
            value = (u >> 1) ^ -(u & 1)
            return value, pos
        shift += 7
        if shift > 63:
            raise FKDBError("varint 过长")


# ------------------------------------------------------------- 行编解码
def encode_row(table: Table, values: Dict[str, object]) -> bytes:
    out = bytearray()
    out.extend(struct.pack("<H", len(table.columns)))
    for col in table.columns:
        v = values.get(col.name)
        if v is None:
            out.append(TAG_NULL)
        elif col.ctype in (INTEGER, BIGINT):
            out.append(TAG_INT)
            out.extend(encode_varint(int(v)))
        elif col.ctype == TEXT:
            raw = str(v).encode("utf-8")
            out.append(TAG_TEXT)
            out.extend(struct.pack("<I", len(raw)))
            out.extend(raw)
        elif col.ctype == REAL:
            out.append(TAG_REAL)
            out.extend(struct.pack("<d", float(v)))
        elif col.ctype == BOOLEAN:
            out.append(TAG_BOOL)
            out.append(1 if v else 0)
        else:  # pragma: no cover - schema 层已拦截
            raise FKDBError("不支持的列类型: %s" % col.ctype)
    return bytes(out)


def decode_row(table: Table, payload: bytes) -> Dict[str, object]:
    if len(payload) < 2:
        raise FKDBError("行 payload 截断")
    (ncols,) = struct.unpack_from("<H", payload, 0)
    pos = 2
    if ncols != len(table.columns):
        raise FKDBError(
            "表 %s 列数不匹配（文件 %d vs 目录 %d），目录可能被改过"
            % (table.name, ncols, len(table.columns)))
    row = {}
    for col in table.columns:
        tag = payload[pos]
        pos += 1
        if tag == TAG_NULL:
            row[col.name] = None
        elif tag == TAG_INT:
            v, pos = decode_varint(payload, pos)
            row[col.name] = v
        elif tag == TAG_TEXT:
            (length,) = struct.unpack_from("<I", payload, pos)
            pos += 4
            row[col.name] = payload[pos:pos + length].decode("utf-8")
            pos += length
        elif tag == TAG_REAL:
            (v,) = struct.unpack_from("<d", payload, pos)
            pos += 8
            row[col.name] = v
        elif tag == TAG_BOOL:
            row[col.name] = bool(payload[pos])
            pos += 1
        else:
            raise FKDBError("未知值标签 %d（表 %s）" % (tag, table.name))
    return row


# ------------------------------------------------------------- 堆帧读写
HEAP_HEADER_SIZE = 22  # B(1)+B(1)+Q(8)+Q(8)+I(4)


def pack_heap_frame(rowid: int, lsn: int, payload: Optional[bytes]) -> bytes:
    flags = FLAG_TOMBSTONE if payload is None else 0
    body = payload or b""
    return struct.pack("<BBQQI", MAGIC_HEAP, flags, rowid, lsn, len(body)) + body


def read_heap_frame(f: BinaryIO):
    """返回 (rowid, lsn, payload_or_None)；文件结束返回 None。"""
    first = f.read(1)
    if not first:
        return None
    head = first + f.read(HEAP_HEADER_SIZE - 1)
    if len(head) < HEAP_HEADER_SIZE:
        raise FKDBError("堆帧头部截断")
    magic, flags, rowid, lsn, plen = struct.unpack("<BBQQI", head)
    if magic != MAGIC_HEAP:
        raise FKDBError("堆帧 magic 错误: %02x" % magic)
    payload = f.read(plen)
    if len(payload) < plen:
        raise FKDBError("堆帧 payload 截断")
    return rowid, lsn, None if (flags & FLAG_TOMBSTONE) else payload


# ------------------------------------------------------------- WAL 帧
def pack_wal_data(txn_id: int, lsn: int, op: int, tname: str,
                  rowid: int, payload: Optional[bytes]) -> bytes:
    name_bytes = tname.encode("utf-8")
    out = struct.pack("<BQQQH", MAGIC_WAL_DATA, txn_id, lsn, op,
                      len(name_bytes)) + name_bytes
    out += struct.pack("<Q", rowid)
    body = payload or b""
    out += struct.pack("<I", len(body)) + body
    return out


def pack_wal_commit(txn_id: int, lsn: int) -> bytes:
    return struct.pack("<BQQ", MAGIC_WAL_COMMIT, txn_id, lsn)


def pack_wal_checkpoint(lsn: int) -> bytes:
    return struct.pack("<BQ", MAGIC_WAL_CHECKPOINT, lsn)


def read_wal_frame(f: BinaryIO):
    """
    返回字典：
      {kind:'data', txn, lsn, op, table, rowid, payload}
      {kind:'commit', txn, lsn}
      {kind:'checkpoint', lsn}
    """
    first = f.read(1)
    if not first:
        return None
    magic = first[0]
    if magic == MAGIC_WAL_COMMIT:
        txn, lsn = struct.unpack("<QQ", f.read(16))
        return {"kind": "commit", "txn": txn, "lsn": lsn}
    if magic == MAGIC_WAL_CHECKPOINT:
        (lsn,) = struct.unpack("<Q", f.read(8))
        return {"kind": "checkpoint", "lsn": lsn}
    if magic != MAGIC_WAL_DATA:
        raise FKDBError("WAL magic 错误: %02x" % magic)
    txn, lsn, op, nlen = struct.unpack("<QQQH", f.read(26))
    tname = f.read(nlen).decode("utf-8")
    (rowid,) = struct.unpack("<Q", f.read(8))
    (plen,) = struct.unpack("<I", f.read(4))
    payload = f.read(plen) if plen else None
    if payload is not None and len(payload) < plen:
        raise FKDBError("WAL payload 截断")
    return {"kind": "data", "txn": txn, "lsn": lsn, "op": op,
            "table": tname, "rowid": rowid, "payload": payload}
