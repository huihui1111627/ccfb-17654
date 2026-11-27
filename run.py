#!/usr/bin/env python3 -I
"""项目统一入口：把工作目录注入 sys.path 后转发到子模块。

用法：
  python3 -I run.py gen --db data/shop --customers 1000 --scale 1.0
  python3 -I run.py exp --db data/shop --out experiments/out
  python3 -I run.py web --db data/shop --port 8000
  python3 -I run.py test
"""
import os
import sys

sys.path.insert(0, os.path.dirname(os.path.abspath(__file__)))


def main():
    if len(sys.argv) < 2:
        print(__doc__)
        return
    cmd = sys.argv[1]
    rest = sys.argv[2:]
    if cmd == "gen":
        import argparse
        ap = argparse.ArgumentParser()
        ap.add_argument("--db", default="data/shop")
        ap.add_argument("--customers", type=int, default=1000)
        ap.add_argument("--scale", type=float, default=1.0)
        ap.add_argument("--seed", type=int, default=20261128)
        ap.add_argument("--ka-orders", type=int, default=0)
        a = ap.parse_args(rest)
        from ecommerce.generate import generate
        generate(a.db, n_customers=a.customers, scale=a.scale,
                 seed=a.seed, ka_orders=a.ka_orders)
    elif cmd == "exp":
        from experiments.run_experiments import main as exp_main
        sys.argv = ["experiments"] + rest
        exp_main()
    elif cmd == "web":
        from webui.server import main as web_main
        sys.argv = ["webui"] + rest
        web_main()
    elif cmd == "test":
        import subprocess
        root = os.path.dirname(os.path.abspath(__file__))
        for t in ("tests/smoke.py", "tests/test_semantics.py"):
            r = subprocess.run([sys.executable, "-I", os.path.join(root, t)])
            if r.returncode:
                sys.exit(r.returncode)
    else:
        print("未知命令:", cmd)
        print(__doc__)
        sys.exit(1)


if __name__ == "__main__":
    main()
