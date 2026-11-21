from __future__ import annotations

import argparse
from pathlib import Path

from .contracts import ReplayRequest, canonical
from .ingest import import_archive
from .models import train
from .replay import advance, create_run
from .storage import Lab


def main():
    p = argparse.ArgumentParser(description="Market State Lab")
    p.add_argument("--workspace", type=Path)
    sub = p.add_subparsers(dest="command", required=True)
    i = sub.add_parser("import")
    i.add_argument("archive", type=Path)
    i.add_argument("--prices-only", action="store_true")
    t = sub.add_parser("train")
    t.add_argument("--train-end", default="2022-12-31")
    r = sub.add_parser("replay")
    r.add_argument("model_id")
    r.add_argument("--start", default="2023-01-01")
    r.add_argument("--end", default="2026-05-02")
    sub.add_parser("status")
    args = p.parse_args()
    lab = Lab(args.workspace)
    if args.command == "import":
        result = import_archive(lab, args.archive, not args.prices_only)
    elif args.command == "train":
        result = train(lab, lab.dataset()["id"], args.train_end)
    elif args.command == "replay":
        rid = create_run(
            lab, ReplayRequest(model_id=args.model_id, start=args.start, end=args.end)
        )
        result = advance(lab, rid, 100000)
    else:
        result = {
            "datasets": lab.rows("SELECT * FROM datasets"),
            "sources": lab.rows("SELECT * FROM sources"),
            "runs": lab.rows("SELECT * FROM runs"),
        }
    print(canonical(result))


if __name__ == "__main__":
    main()
