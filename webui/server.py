"""
Web 控制台（纯标准库 http.server，无第三方依赖）。

API:
  GET  /api/schema                 表结构 + 外键 + 行数
  GET  /api/tables/<t>?limit=      表数据（分页）
  GET  /api/row/<t>/<id>           单行详情
  POST /api/estimate               {"roots":[[table,id],...]} 级联预估
  GET  /api/impact/<t>/<id>        单行级联影响树（页面主交互）
  POST /api/delete                 实删（mode=one_shot|batched）
  GET  /api/report                 最近一次实验报告（若存在）
  GET  /api/integrity              悬空外键自检
静态资源由 webui/static 提供。
"""
from __future__ import annotations

import argparse
import json
import os
import sys
from http.server import BaseHTTPRequestHandler, ThreadingHTTPServer
from urllib.parse import urlparse, parse_qs

sys.path.insert(0, os.path.dirname(os.path.dirname(os.path.abspath(__file__))))

from mini_fkdb import Database, ForeignKeyViolation, FKDBError

STATIC_DIR = os.path.join(os.path.dirname(os.path.abspath(__file__)),
                          "static")
_REPORT_PATH = os.path.join(
    os.path.dirname(os.path.dirname(os.path.abspath(__file__))),
    "experiments", "out", "report.json")


class AppState:
    def __init__(self, db_path: str):
        self.db_path = db_path
        self.db = Database(db_path)
        self.last_manifest = None


def schema_payload(state: AppState) -> dict:
    db = state.db
    tables = []
    for tname in db.table_names():
        t = db.catalog.table(tname)
        tables.append({
            "name": tname,
            "pk": t.pk_column,
            "row_count": db.count(tname),
            "columns": [{"name": c.name, "type": c.ctype,
                         "nullable": c.nullable,
                         "default": c.default if c.has_default else None,
                         "pk": c.pk} for c in t.columns],
            "fks": [{"name": f.name, "column": f.columns[0],
                     "ref_table": f.ref_table,
                     "ref_column": f.ref_columns[0],
                     "on_delete": f.on_delete,
                     "on_update": f.on_update} for f in t.fks]})
    return {"tables": tables}


def estimate_payload(state: AppState, roots) -> dict:
    plan = state.db.estimate_delete(roots)
    return {
        "blocked": False,
        "roots": roots,
        "counts": plan.counts_by_table(),
        "totals": _total_counts(plan),
        "impact_tree": plan.impact_tree(),
        "max_depth": max((n.depth for n in plan.nodes.values()), default=0),
        "node_count": len(plan.nodes)}


def _total_counts(plan) -> dict:
    totals = {"delete": 0, "set_null": 0, "set_default": 0, "blocked": 0}
    for c in plan.counts_by_table().values():
        for k in totals:
            totals[k] += c.get(k, 0)
    return totals


class Handler(BaseHTTPRequestHandler):
    state: AppState = None  # 类属性，启动时注入

    def log_message(self, fmt, *args):
        sys.stderr.write("[web] " + fmt % args + "\n")

    def _send_json(self, obj, status=200):
        body = json.dumps(obj, ensure_ascii=False).encode("utf-8")
        self.send_response(status)
        self.send_header("Content-Type", "application/json; charset=utf-8")
        self.send_header("Content-Length", str(len(body)))
        self.end_headers()
        self.wfile.write(body)

    def _send_static(self, rel: str):
        if rel == "/" or rel == "":
            rel = "index.html"
        path = os.path.normpath(os.path.join(STATIC_DIR, rel))
        if not path.startswith(STATIC_DIR) or not os.path.isfile(path):
            self.send_error(404)
            return
        ctype = "text/html; charset=utf-8" if path.endswith(".html") else \
            "application/javascript; charset=utf-8" if path.endswith(".js") \
            else "text/css; charset=utf-8" if path.endswith(".css") else \
            "application/octet-stream"
        with open(path, "rb") as f:
            body = f.read()
        self.send_response(200)
        self.send_header("Content-Type", ctype)
        self.send_header("Content-Length", str(len(body)))
        self.end_headers()
        self.wfile.write(body)

    def do_GET(self):
        parsed = urlparse(self.path)
        path = parsed.path
        q = parse_qs(parsed.query)
        try:
            if path == "/api/schema":
                return self._send_json(schema_payload(self.state))
            if path == "/api/integrity":
                dangling = self.state.db.dangling_refs()
                return self._send_json({"ok": not dangling,
                                        "dangling": dangling[:50],
                                        "count": len(dangling)})
            if path == "/api/report":
                if os.path.exists(_REPORT_PATH):
                    with open(_REPORT_PATH, "r", encoding="utf-8") as f:
                        return self._send_json(json.load(f))
                return self._send_json({"available": False})
            if path.startswith("/api/tables/"):
                tname = path.split("/")[-1]
                limit = int(q.get("limit", ["100"])[0])
                rows = self.state.db.scan(tname)[:limit]
                return self._send_json({"table": tname, "rows": rows,
                                        "total": self.state.db.count(tname)})
            if path.startswith("/api/row/"):
                parts = path.split("/")
                tname, rid = parts[-2], int(parts[-1])
                row = self.state.db.get(tname, rid)
                return self._send_json({"table": tname, "rowid": rid,
                                        "row": row})
            if path.startswith("/api/impact/"):
                parts = path.split("/")
                tname, rid = parts[-2], int(parts[-1])
                try:
                    return self._send_json(
                        estimate_payload(self.state, [(tname, rid)]))
                except ForeignKeyViolation as e:
                    return self._send_json(
                        {"blocked": True,
                         "blocks": e.details.get("blocks", []),
                         "message": str(e)}, status=409)
            if path.startswith("/api/"):
                return self.send_error(404)
            return self._send_static(path.lstrip("/"))
        except FKDBError as e:
            return self._send_json({"error": str(e),
                                    "type": type(e).__name__}, status=400)
        except Exception as e:  # pragma: no cover
            return self._send_json({"error": repr(e)}, status=500)

    def do_POST(self):
        parsed = urlparse(self.path)
        try:
            length = int(self.headers.get("Content-Length", 0))
            payload = json.loads(self.rfile.read(length).decode("utf-8"))
            if parsed.path == "/api/estimate":
                roots = [tuple(r) for r in payload["roots"]]
                try:
                    return self._send_json(estimate_payload(self.state, roots))
                except ForeignKeyViolation as e:
                    return self._send_json(
                        {"blocked": True,
                         "blocks": e.details.get("blocks", []),
                         "message": str(e)}, status=409)
            if parsed.path == "/api/delete":
                roots = [tuple(r) for r in payload["roots"]]
                mode = payload.get("mode", "one_shot")
                batch_size = int(payload.get("batch_size", 50))
                t0 = __import__("time").monotonic()
                if mode == "batched":
                    manifest = self.state.db.cascade_delete_batched(
                        roots, batch_size=batch_size)
                else:
                    manifest = self.state.db.cascade_delete(roots)
                manifest["wall_ms"] = round(
                    (__import__("time").monotonic() - t0) * 1000, 2)
                self.state.last_manifest = manifest
                return self._send_json({"ok": True, "manifest": manifest,
                                        "integrity": self.state.db
                                        .dangling_refs() == []})
            self.send_error(404)
        except ForeignKeyViolation as e:
            return self._send_json(
                {"blocked": True, "blocks": e.details.get("blocks", []),
                 "message": str(e)}, status=409)
        except FKDBError as e:
            return self._send_json({"error": str(e),
                                    "type": type(e).__name__}, status=400)
        except Exception as e:  # pragma: no cover
            return self._send_json({"error": repr(e)}, status=500)


def main():
    ap = argparse.ArgumentParser(description="mini_fkdb Web 控制台")
    ap.add_argument("--db", default="data/shop")
    ap.add_argument("--host", default="127.0.0.1")
    ap.add_argument("--port", type=int, default=8000)
    args = ap.parse_args()
    Handler.state = AppState(args.db)
    httpd = ThreadingHTTPServer((args.host, args.port), Handler)
    print("mini_fkdb 控制台: http://%s:%d （库 %s）"
          % (args.host, args.port, os.path.abspath(args.db)))
    try:
        httpd.serve_forever()
    except KeyboardInterrupt:
        print("\n关闭中 ...")
        httpd.shutdown()
        Handler.state.db.close()


if __name__ == "__main__":
    main()
