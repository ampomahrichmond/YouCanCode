"""Run one sweep (or one career-site pass) in its own process.

The web server launches this with `python -m app.sweep [--ats-only]` so the
sweep's CPU work (parsing thousands of board responses, scoring, tokenising
40k descriptions) never competes with page requests for the interpreter lock.
Both processes share the SQLite database in WAL mode. Log lines go to stderr,
which the server's launcher already redirects to jobradar.log."""
from __future__ import annotations

import argparse
import logging
import sys

from . import llm, matching, scraper


def main(argv: list[str] | None = None) -> int:
    ap = argparse.ArgumentParser(prog="python -m app.sweep")
    ap.add_argument("--ats-only", action="store_true", help="check the career sites only (no job-board searches)")
    ap.add_argument("--rescore", type=int, metavar="USER_ID", help="score every stored role against this user's resume")
    args = ap.parse_args(argv)
    logging.basicConfig(level=logging.INFO, format="%(asctime)s %(name)s %(message)s")
    llm.load_env_file()                     # proxies / ReliefWeb app name from .env
    try:
        if args.rescore:
            matching.rescore_all(args.rescore)
        elif args.ats_only:
            scraper.run_ats_pass()
        else:
            scraper.run_cycle()
    except Exception:
        logging.getLogger("jobradar.sweep").exception("sweep crashed")
        return 1
    return 0


if __name__ == "__main__":
    sys.exit(main())
