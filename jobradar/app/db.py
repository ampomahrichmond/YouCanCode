"""SQLite store.

Shared:   jobs (deduplicated postings), runs, ats_boards, ats_seen.
Per user: users (name + preferences), user_jobs (tracking status, notes, match
score, tailoring, drafts, apply state), user_companies (watch/block), and
settings keyed "u<id>:<key>" (resume text, applicant profile, apply rules,
digest state)."""
import hashlib
import json
import re
import sqlite3
from contextlib import contextmanager
from datetime import datetime, timezone
from pathlib import Path

DB_PATH = Path(__file__).resolve().parent.parent / "jobradar.db"
DEFAULT_USER = 1

SCHEMA = """
CREATE TABLE IF NOT EXISTS jobs (
    id TEXT PRIMARY KEY,
    title TEXT NOT NULL,
    company TEXT,
    location TEXT,
    site TEXT,
    url TEXT,
    date_posted TEXT,
    salary_min REAL,
    salary_max REAL,
    salary_interval TEXT,
    is_remote INTEGER DEFAULT 0,
    snippet TEXT,
    search_term TEXT,
    first_seen TEXT NOT NULL,
    last_seen TEXT NOT NULL
);
CREATE INDEX IF NOT EXISTS idx_jobs_first_seen ON jobs(first_seen DESC);
CREATE INDEX IF NOT EXISTS idx_jobs_site ON jobs(site);

CREATE TABLE IF NOT EXISTS runs (
    id INTEGER PRIMARY KEY AUTOINCREMENT,
    started TEXT NOT NULL,
    finished TEXT,
    scraped INTEGER DEFAULT 0,
    new_jobs INTEGER DEFAULT 0,
    errors TEXT
);

CREATE TABLE IF NOT EXISTS settings (
    key TEXT PRIMARY KEY,
    value TEXT
);

CREATE TABLE IF NOT EXISTS companies (
    name_norm TEXT PRIMARY KEY,
    name TEXT NOT NULL,
    flag TEXT NOT NULL,
    created TEXT NOT NULL
);

CREATE TABLE IF NOT EXISTS ats_boards (
    id INTEGER PRIMARY KEY AUTOINCREMENT,
    provider TEXT NOT NULL,
    slug TEXT NOT NULL,
    company TEXT,
    source TEXT DEFAULT 'auto',
    enabled INTEGER DEFAULT 1,
    added TEXT NOT NULL,
    last_checked TEXT,
    last_count INTEGER,
    last_new INTEGER,
    last_error TEXT,
    UNIQUE(provider, slug)
);
CREATE TABLE IF NOT EXISTS ats_seen (url TEXT PRIMARY KEY, tried TEXT);

CREATE TABLE IF NOT EXISTS users (
    id INTEGER PRIMARY KEY AUTOINCREMENT,
    name TEXT NOT NULL,
    prefs TEXT DEFAULT '{}',        -- JSON: categories, levels, remote_only, countries, emoji
    created TEXT NOT NULL
);

CREATE TABLE IF NOT EXISTS user_jobs (
    user_id INTEGER NOT NULL,
    job_id TEXT NOT NULL,
    status TEXT DEFAULT '',
    status_at TEXT,
    notes TEXT DEFAULT '',
    match_score INTEGER,
    match_why TEXT,
    fit_summary TEXT,
    cover_letter TEXT,
    tailoring TEXT,
    resume_tailored TEXT,
    apply_state TEXT DEFAULT '',
    apply_log TEXT,
    apply_at TEXT,
    PRIMARY KEY (user_id, job_id)
);
CREATE INDEX IF NOT EXISTS idx_uj_status ON user_jobs(user_id, status);
CREATE INDEX IF NOT EXISTS idx_uj_match ON user_jobs(user_id, match_score DESC);

CREATE TABLE IF NOT EXISTS user_companies (
    user_id INTEGER NOT NULL,
    name_norm TEXT NOT NULL,
    name TEXT NOT NULL,
    flag TEXT NOT NULL,             -- watch | block
    created TEXT NOT NULL,
    PRIMARY KEY (user_id, name_norm)
);
"""

# Shared job columns added after the first release (legacy per-user columns are
# left in place but no longer read; user_jobs holds that data now).
NEW_COLUMNS = [
    ("url_direct", "TEXT"), ("easy_apply", "INTEGER"), ("salary_annual", "REAL"), ("currency", "TEXT"),
    ("salary_source", "TEXT"), ("job_type", "TEXT"), ("job_level", "TEXT"), ("level", "TEXT"),
    ("job_function", "TEXT"), ("company_industry", "TEXT"), ("country", "TEXT"), ("description", "TEXT"),
    ("categories", "TEXT"),          # comma-joined category keys from config (e.g. ",data,hr,")
    ("sector", "TEXT"),              # one sector key from config `sectors` (employer type)
    ("company_norm", "TEXT"),        # normalised company name (see _sql_norm_py) for the watch/block join
    ("travel", "INTEGER"),           # NULL unknown, 0 none, 1 required, 2..100 = stated percentage
]
ATS_NEW_COLUMNS = [("sector", "TEXT")]
JOB_COLS = ["id", "title", "company", "location", "site", "url", "date_posted", "salary_min", "salary_max",
            "salary_interval", "is_remote", "snippet", "search_term", "first_seen", "last_seen"] + [c for c, _ in NEW_COLUMNS]
USER_JOB_COLS = ["status", "status_at", "notes", "match_score", "match_why", "fit_summary", "cover_letter",
                 "tailoring", "resume_tailored", "apply_state", "apply_log", "apply_at"]
EXTRA_INDEXES = """
CREATE INDEX IF NOT EXISTS idx_jobs_level ON jobs(level);
CREATE INDEX IF NOT EXISTS idx_jobs_salary ON jobs(salary_annual DESC);
CREATE INDEX IF NOT EXISTS idx_jobs_last_seen ON jobs(last_seen);
CREATE INDEX IF NOT EXISTS idx_jobs_sector ON jobs(sector);
CREATE INDEX IF NOT EXISTS idx_jobs_company_norm ON jobs(company_norm);
CREATE INDEX IF NOT EXISTS idx_jobs_site_company ON jobs(site, company);
"""

PIPELINE = ["saved", "applied", "interviewing", "offer", "rejected"]
STATUSES = ("", *PIPELINE, "hidden", "archived")
APPLY_STATES = ("", "queued", "preparing", "prepared", "needs_review", "submitted", "failed", "skipped")
LEVEL_ORDER = ["C-level", "VP", "Head", "Director", "Senior Manager", "Manager", "Lead",
               "Senior Analyst", "Analyst", "Other"]
_ACTIVE_SQL = "COALESCE(uj.status,'') NOT IN ('hidden','archived') AND COALESCE(c.flag,'') != 'block'"
_USER_SETTING_KEYS = ("resume_text", "resume_name", "resume_at", "applicant_profile", "apply_rules",
                      "last_digest_at", "last_digest_path")


def _now() -> str:
    return datetime.now(timezone.utc).isoformat()


@contextmanager
def connect():
    """One connection per call. The database runs in WAL mode (set once in
    init), so readers never wait for the sweep thread's writes and writers
    queue on SQLite's own busy timeout instead of a process-wide lock. A big
    page cache + mmap keeps the repeated scans inside one request cheap."""
    con = sqlite3.connect(DB_PATH, timeout=30)
    con.row_factory = sqlite3.Row
    con.execute("PRAGMA synchronous=NORMAL")
    con.execute("PRAGMA cache_size=-65536")        # 64 MB
    con.execute("PRAGMA mmap_size=268435456")      # 256 MB
    con.execute("PRAGMA temp_store=MEMORY")
    try:
        yield con
        con.commit()
    finally:
        con.close()


def init():
    with connect() as con:
        con.execute("PRAGMA journal_mode=WAL")     # persistent; lets reads and writes overlap
        con.executescript(SCHEMA)
        existing = {r["name"] for r in con.execute("PRAGMA table_info(jobs)")}
        for col, decl in NEW_COLUMNS:
            if col not in existing:
                con.execute(f"ALTER TABLE jobs ADD COLUMN {col} {decl}")
        ats_cols = {r["name"] for r in con.execute("PRAGMA table_info(ats_boards)")}
        for col, decl in ATS_NEW_COLUMNS:
            if col not in ats_cols:
                con.execute(f"ALTER TABLE ats_boards ADD COLUMN {col} {decl}")
        con.executescript(EXTRA_INDEXES)
        # one-time fill of the normalised company column (same expression as _NORM_SQL)
        con.execute("UPDATE jobs SET company_norm = LOWER(TRIM(REPLACE(REPLACE(REPLACE(company,',',''),'.',''),'  ',' '))) "
                    "WHERE company_norm IS NULL AND company IS NOT NULL")
        # first user
        if con.execute("SELECT COUNT(*) c FROM users").fetchone()["c"] == 0:
            con.execute("INSERT INTO users (id, name, prefs, created) VALUES (1, 'Me', '{}', ?)", (_now(),))
        # one-time migration of the single-user data into the per-user tables
        if con.execute("SELECT COUNT(*) c FROM user_jobs").fetchone()["c"] == 0 and "status" in existing:
            legacy = [c for c in USER_JOB_COLS if c in existing]
            rows = con.execute(
                f"SELECT id, {', '.join(legacy)} FROM jobs WHERE COALESCE(status,'')!='' OR COALESCE(notes,'')!='' "
                "OR match_score IS NOT NULL OR fit_summary IS NOT NULL OR cover_letter IS NOT NULL OR tailoring IS NOT NULL "
                "OR resume_tailored IS NOT NULL OR COALESCE(apply_state,'')!=''").fetchall()
            for r in rows:
                con.execute(f"INSERT OR IGNORE INTO user_jobs (user_id, job_id, {', '.join(legacy)}) "
                            f"VALUES (1, ?, {', '.join('?' * len(legacy))})", [r["id"]] + [r[c] for c in legacy])
        if con.execute("SELECT COUNT(*) c FROM user_companies").fetchone()["c"] == 0:
            con.execute("INSERT OR IGNORE INTO user_companies (user_id, name_norm, name, flag, created) "
                        "SELECT 1, name_norm, name, flag, created FROM companies")
        for k in _USER_SETTING_KEYS:
            r = con.execute("SELECT value FROM settings WHERE key=?", (k,)).fetchone()
            if r and not con.execute("SELECT 1 FROM settings WHERE key=?", (f"u1:{k}",)).fetchone():
                con.execute("INSERT INTO settings (key, value) VALUES (?, ?)", (f"u1:{k}", r["value"]))


# ── users ─────────────────────────────────────────────────────────────────
PREFS_DEFAULT = {"categories": [], "sectors": [], "levels": [], "remote_only": False, "countries": [], "no_travel": True,
                 "emoji": "🟢"}


def _user_row(r) -> dict:
    prefs = dict(PREFS_DEFAULT)
    try:
        prefs.update(json.loads(r["prefs"] or "{}"))
    except json.JSONDecodeError:
        pass
    return {"id": r["id"], "name": r["name"], "prefs": prefs, "created": r["created"]}


def list_users() -> list[dict]:
    with connect() as con:
        return [_user_row(r) for r in con.execute("SELECT * FROM users ORDER BY id")]


def get_user(uid: int) -> dict | None:
    with connect() as con:
        r = con.execute("SELECT * FROM users WHERE id=?", (uid,)).fetchone()
        return _user_row(r) if r else None


def add_user(name: str, prefs: dict | None = None) -> dict:
    with connect() as con:
        cur = con.execute("INSERT INTO users (name, prefs, created) VALUES (?,?,?)",
                          (name.strip(), json.dumps({**PREFS_DEFAULT, **(prefs or {})}), _now()))
        uid = cur.lastrowid
    return get_user(uid)


def update_user(uid: int, name: str | None = None, prefs: dict | None = None) -> dict | None:
    u = get_user(uid)
    if not u:
        return None
    with connect() as con:
        if name is not None and name.strip():
            con.execute("UPDATE users SET name=? WHERE id=?", (name.strip(), uid))
        if prefs is not None:
            con.execute("UPDATE users SET prefs=? WHERE id=?", (json.dumps({**u["prefs"], **prefs}), uid))
    return get_user(uid)


def delete_user(uid: int) -> bool:
    if uid == DEFAULT_USER:
        return False
    with connect() as con:
        con.execute("DELETE FROM user_jobs WHERE user_id=?", (uid,))
        con.execute("DELETE FROM user_companies WHERE user_id=?", (uid,))
        con.execute("DELETE FROM settings WHERE key LIKE ?", (f"u{uid}:%",))
        cur = con.execute("DELETE FROM users WHERE id=?", (uid,))
        return cur.rowcount == 1


def users_with_resume() -> list[int]:
    with connect() as con:
        rows = con.execute("SELECT key FROM settings WHERE key LIKE 'u%:resume_text'").fetchall()
    return [int(r["key"][1:].split(":")[0]) for r in rows]


# ── settings ──────────────────────────────────────────────────────────────
def _skey(key: str, uid: int | None) -> str:
    return f"u{int(uid)}:{key}" if uid is not None else key


def get_setting(key: str, uid: int | None = None) -> str | None:
    with connect() as con:
        r = con.execute("SELECT value FROM settings WHERE key=?", (_skey(key, uid),)).fetchone()
        return r["value"] if r else None


def set_setting(key: str, value: str | None, uid: int | None = None):
    k = _skey(key, uid)
    with connect() as con:
        if value is None:
            con.execute("DELETE FROM settings WHERE key=?", (k,))
        else:
            con.execute("INSERT INTO settings (key,value) VALUES (?,?) "
                        "ON CONFLICT(key) DO UPDATE SET value=excluded.value", (k, value))


# ── companies (watch / block, per user) ───────────────────────────────────
_NORM_SQL = "LOWER(TRIM(REPLACE(REPLACE(REPLACE(j.company,',',''),'.',''),'  ',' ')))"


def _sql_norm_py(name: str) -> str:
    """Python twin of _NORM_SQL (must stay in sync)."""
    return (name or "").replace(",", "").replace(".", "").replace("  ", " ").strip().lower()


def _joins(uid: int) -> str:
    uid = int(uid)
    return (f"LEFT JOIN user_jobs uj ON uj.user_id={uid} AND uj.job_id=j.id "
            f"LEFT JOIN user_companies c ON c.user_id={uid} AND c.name_norm=j.company_norm")


def list_companies(uid: int) -> list[dict]:
    with connect() as con:
        rows = con.execute(
            "SELECT c.*, (SELECT COUNT(*) FROM jobs j WHERE j.company_norm=c.name_norm) AS jobs "
            "FROM user_companies c WHERE c.user_id=? ORDER BY flag, name", (uid,)).fetchall()
        return [dict(r) for r in rows]


def set_company(uid: int, name: str, flag: str) -> dict:
    if flag not in ("watch", "block"):
        raise ValueError("flag must be watch or block")
    norm = _sql_norm_py(name)
    with connect() as con:
        con.execute("INSERT INTO user_companies (user_id,name_norm,name,flag,created) VALUES (?,?,?,?,?) "
                    "ON CONFLICT(user_id,name_norm) DO UPDATE SET flag=excluded.flag, name=excluded.name",
                    (uid, norm, name.strip(), flag, _now()))
    return {"name_norm": norm, "name": name.strip(), "flag": flag}


def delete_company(uid: int, name_norm: str):
    with connect() as con:
        con.execute("DELETE FROM user_companies WHERE user_id=? AND name_norm=?", (uid, name_norm))


# ── ATS boards (shared) ───────────────────────────────────────────────────
def ats_list(with_counts: bool = True) -> list[dict]:
    """Boards with their stored-role counts. One grouped pass over jobs instead
    of a correlated subquery per board (2,000 boards made that take ~50 s)."""
    with connect() as con:
        rows = [dict(r) for r in con.execute(
            "SELECT b.* FROM ats_boards b ORDER BY enabled DESC, LOWER(COALESCE(company, slug))")]
        counts = {}
        if with_counts and rows:
            sites = sorted({r["provider"] for r in rows})
            q = f"SELECT site, LOWER(company) k, COUNT(*) c FROM jobs WHERE site IN ({','.join('?' * len(sites))}) GROUP BY 1, 2"
            counts = {(r["site"], r["k"]): r["c"] for r in con.execute(q, sites)}
    for r in rows:
        r["jobs"] = counts.get((r["provider"], (r["company"] or "").lower()), 0)
    return rows


def ats_enabled_count() -> int:
    with connect() as con:
        return con.execute("SELECT COUNT(*) c FROM ats_boards WHERE enabled=1").fetchone()["c"]


def ats_add(provider: str, slug: str, company: str | None, source: str, sector: str | None = None) -> bool:
    """Register a board; on a known board fill a missing company name and, when a
    sector is given (config `organizations` / manual add), set it. Returns True if new."""
    with connect() as con:
        cur = con.execute(
            "INSERT OR IGNORE INTO ats_boards (provider, slug, company, source, added, sector) VALUES (?,?,?,?,?,?)",
            (provider, slug, (company or "").strip() or None, source, _now(), sector or None))
        if cur.rowcount == 0:
            if company:
                con.execute("UPDATE ats_boards SET company=COALESCE(company, ?) WHERE provider=? AND slug=?",
                            (company.strip(), provider, slug))
            if sector:
                con.execute("UPDATE ats_boards SET sector=? WHERE provider=? AND slug=? AND COALESCE(sector,'')!=?",
                            (sector, provider, slug, sector))
        return cur.rowcount == 1


def ats_set_enabled(bid: int, enabled: bool):
    with connect() as con:
        con.execute("UPDATE ats_boards SET enabled=? WHERE id=?", (int(enabled), bid))


def ats_set_sector(bid: int, sector: str | None):
    with connect() as con:
        con.execute("UPDATE ats_boards SET sector=? WHERE id=?", (sector or None, bid))
        # re-tag that board's stored roles so the filter agrees with the table
        if sector:
            con.execute("UPDATE jobs SET sector=? WHERE id IN (SELECT j.id FROM jobs j, ats_boards b WHERE b.id=? "
                        "AND j.site=b.provider AND LOWER(j.company)=LOWER(COALESCE(b.company,'')))", (sector, bid))


def ats_delete(bid: int):
    with connect() as con:
        con.execute("DELETE FROM ats_boards WHERE id=?", (bid,))


def ats_update_result(bid: int, count: int, new: int, error: str | None, company: str | None = None):
    with connect() as con:
        con.execute("UPDATE ats_boards SET last_checked=?, last_count=?, last_new=?, last_error=?, "
                    "company=COALESCE(company, ?) WHERE id=?", (_now(), count, new, error, company, bid))


def ats_seen(url: str) -> bool:
    with connect() as con:
        return con.execute("SELECT 1 FROM ats_seen WHERE url=?", (url,)).fetchone() is not None


def ats_mark_seen(url: str):
    with connect() as con:
        con.execute("INSERT OR IGNORE INTO ats_seen (url, tried) VALUES (?,?)", (url, _now()))


def jobs_urls_for_discovery(since: str | None = None) -> list[tuple[str, str]]:
    """Employer links to scan for new boards; `since` limits it to rows first
    seen after that timestamp (every sweep rescanning 40k rows is wasted CPU)."""
    with connect() as con:
        sql = ("SELECT url_direct, company FROM jobs WHERE url_direct IS NOT NULL "
               "AND site IN ('indeed','linkedin','glassdoor','zip_recruiter','google')")
        args: list = []
        if since:
            sql += " AND first_seen >= ?"; args.append(since)
        rows = con.execute(sql + " ORDER BY first_seen DESC", args).fetchall()
        return [(r["url_direct"], r["company"]) for r in rows]


def job_exists(jid: str) -> bool:
    with connect() as con:
        return con.execute("SELECT 1 FROM jobs WHERE id=?", (jid,)).fetchone() is not None


def jobs_exist(ids: list[str]) -> set[str]:
    """Which of these ids are already stored — one query instead of one per row."""
    out: set[str] = set()
    if not ids:
        return out
    with connect() as con:
        for i in range(0, len(ids), 500):
            chunk = ids[i:i + 500]
            out.update(r["id"] for r in con.execute(
                f"SELECT id FROM jobs WHERE id IN ({','.join('?' * len(chunk))})", chunk))
    return out


def jobs_count() -> int:
    with connect() as con:
        return con.execute("SELECT COUNT(*) c FROM jobs").fetchone()["c"]


def touch_jobs(ids: list[str]):
    if not ids:
        return
    now = _now()
    with connect() as con:
        con.executemany("UPDATE jobs SET last_seen=? WHERE id=?", [(now, i) for i in ids])


# ── jobs (shared) ─────────────────────────────────────────────────────────
def job_id(title: str, company: str, location: str) -> str:
    key = f"{(title or '').strip().lower()}|{(company or '').strip().lower()}|{(location or '').strip().lower()}"
    return hashlib.sha1(key.encode()).hexdigest()[:16]


_FILL_ON_RESEEN = ("url_direct", "easy_apply", "salary_min", "salary_max", "salary_interval",
                   "salary_annual", "currency", "salary_source", "job_type", "job_level",
                   "job_function", "company_industry", "country", "description", "snippet", "categories", "sector", "travel")


def upsert_jobs(rows: list[dict]) -> int:
    """Insert new jobs, bump last_seen on known ones (filling blanks). Returns count of NEW jobs.
    Sets r['id'] on every row so callers can attach per-user data afterwards."""
    new = 0
    now = _now()
    fill_sql = ", ".join(f"{c}=COALESCE({c}, ?)" for c in _FILL_ON_RESEEN)
    with connect() as con:
        for r in rows:
            jid = job_id(r.get("title"), r.get("company"), r.get("location"))
            r["id"] = jid
            exists = con.execute("SELECT 1 FROM jobs WHERE id=?", (jid,)).fetchone()
            if exists:
                con.execute(f"UPDATE jobs SET last_seen=?, {fill_sql} WHERE id=?",
                            [now] + [r.get(c) for c in _FILL_ON_RESEEN] + [jid])
                continue
            values = {**r, "first_seen": now, "last_seen": now, "is_remote": int(bool(r.get("is_remote"))),
                      "company_norm": _sql_norm_py(r.get("company")) or None}
            con.execute(f"INSERT INTO jobs ({','.join(JOB_COLS)}) VALUES ({','.join('?' * len(JOB_COLS))})",
                        [values.get(c) for c in JOB_COLS])
            new += 1
    return new


def get_job(jid: str, uid: int = DEFAULT_USER) -> dict | None:
    with connect() as con:
        r = con.execute(f"{_select(uid, None)} WHERE j.id=?", (jid,)).fetchone()
        return dict(r) if r else None


def _upsert_user_job(con, uid: int, jid: str, **fields):
    cols = list(fields)
    con.execute(
        f"INSERT INTO user_jobs (user_id, job_id, {', '.join(cols)}) VALUES (?, ?, {', '.join('?' * len(cols))}) "
        f"ON CONFLICT(user_id, job_id) DO UPDATE SET {', '.join(f'{c}=excluded.{c}' for c in cols)}",
        [uid, jid] + [fields[c] for c in cols])


def set_status(jid: str, status: str, notes: str | None = None, uid: int = DEFAULT_USER) -> dict | None:
    if status not in STATUSES:
        raise ValueError(f"bad status {status!r}")
    with connect() as con:
        f = {"status": status, "status_at": _now()}
        if notes is not None:
            f["notes"] = notes
        _upsert_user_job(con, uid, jid, **f)
    return get_job(jid, uid)


def set_field(jid: str, field: str, value: str | None, uid: int = DEFAULT_USER):
    assert field in ("fit_summary", "cover_letter", "notes", "tailoring", "resume_tailored")
    with connect() as con:
        _upsert_user_job(con, uid, jid, **{field: value})


def set_apply_state(jid: str, state: str, log: dict | None = None, uid: int = DEFAULT_USER):
    assert state in APPLY_STATES
    with connect() as con:
        f = {"apply_state": state, "apply_at": _now()}
        if log is not None:
            f["apply_log"] = json.dumps(log)
        _upsert_user_job(con, uid, jid, **f)


def apply_queue(uid: int, stale_after: int = 3) -> list[dict]:
    cut = stale_cutoff(stale_after)
    with connect() as con:
        rows = con.execute(f"{_select(uid, cut)} WHERE COALESCE(uj.apply_state,'') != '' "
                           "ORDER BY CASE uj.apply_state WHEN 'preparing' THEN 0 WHEN 'prepared' THEN 1 "
                           "WHEN 'needs_review' THEN 2 WHEN 'queued' THEN 3 ELSE 4 END, uj.match_score DESC").fetchall()
    return [dict(r) for r in rows]


def submitted_today(uid: int) -> int:
    with connect() as con:
        return con.execute("SELECT COUNT(*) c FROM user_jobs WHERE user_id=? AND apply_state='submitted' "
                           "AND apply_at >= datetime('now','-24 hours')", (uid,)).fetchone()["c"]


# ── runs ──────────────────────────────────────────────────────────────────
def start_run() -> int:
    with connect() as con:
        return con.execute("INSERT INTO runs (started) VALUES (?)", (_now(),)).lastrowid


def progress_run(run_id: int, scraped: int, new_jobs: int):
    with connect() as con:
        con.execute("UPDATE runs SET scraped=?, new_jobs=? WHERE id=?", (scraped, new_jobs, run_id))


def finish_run(run_id: int, scraped: int, new_jobs: int, errors: str = ""):
    with connect() as con:
        con.execute("UPDATE runs SET finished=?, scraped=?, new_jobs=?, errors=? WHERE id=?",
                    (_now(), scraped, new_jobs, errors, run_id))


def last_completed_run_started() -> str | None:
    with connect() as con:
        r = con.execute("SELECT started FROM runs WHERE finished IS NOT NULL ORDER BY id DESC LIMIT 1").fetchone()
        return r["started"] if r else None


def stale_cutoff(after_sweeps: int = 3) -> str | None:
    if after_sweeps < 1:
        return None
    with connect() as con:
        rows = con.execute("SELECT started FROM runs WHERE finished IS NOT NULL ORDER BY id DESC LIMIT ?",
                           (after_sweeps,)).fetchall()
    return rows[-1]["started"] if len(rows) >= after_sweeps else None


# ── queries ───────────────────────────────────────────────────────────────
SORTS = {
    "newest": "j.first_seen DESC",
    "match": "uj.match_score IS NULL, uj.match_score DESC, j.first_seen DESC",
    "posted": "COALESCE(NULLIF(j.date_posted,''), j.first_seen) DESC, j.first_seen DESC",
    "salary": "j.salary_annual IS NULL, j.salary_annual DESC, j.first_seen DESC",
    "company": "LOWER(j.company) ASC, j.first_seen DESC",
    "title": "LOWER(j.title) ASC, j.first_seen DESC",
}


def _select(uid: int, stale_cut: str | None) -> str:
    stale = f"CASE WHEN j.last_seen < '{stale_cut}' THEN 1 ELSE 0 END" if stale_cut else "0"
    jcols = ", ".join(f"j.{c}" for c in JOB_COLS)
    ucols = ("COALESCE(uj.status,'') AS status, uj.status_at, COALESCE(uj.notes,'') AS notes, uj.match_score, "
             "uj.match_why, uj.fit_summary, uj.cover_letter, uj.tailoring, uj.resume_tailored, "
             "COALESCE(uj.apply_state,'') AS apply_state, uj.apply_log, uj.apply_at")
    return (f"SELECT {jcols}, {ucols}, {stale} AS stale, "
            "CASE WHEN c.flag='watch' THEN 1 ELSE 0 END AS watched, "
            "CASE WHEN c.flag='block' THEN 1 ELSE 0 END AS blocked "
            f"FROM jobs j {_joins(uid)}")


def _cat_sql(categories: list[str]) -> tuple[str, list]:
    keys = [k for k in categories if re.fullmatch(r"[a-z0-9_-]+", k)]
    if not keys:
        return "1=1", []
    return "(" + " OR ".join("j.categories LIKE ?" for _ in keys) + ")", [f"%,{k},%" for k in keys]


def _sector_sql(sectors: list[str]) -> tuple[str, list]:
    keys = [k for k in sectors if re.fullmatch(r"[a-z0-9_-]+", k)]
    if not keys:
        return "1=1", []
    return f"COALESCE(j.sector,'other') IN ({','.join('?' * len(keys))})", keys


def query_jobs(uid: int = DEFAULT_USER, q: str = "", site: str = "", since_hours: int = 0,
               remote: bool = False, new_only: bool = False, easy_apply: bool = False,
               has_salary: bool = False, min_salary: int = 0, levels: list[str] | None = None,
               country: str = "", status: str = "active", term: str = "", sort: str = "newest",
               min_match: int = 0, stale: str = "", stale_after: int = 3,
               categories: list[str] | None = None, sectors: list[str] | None = None,
               no_travel: bool = False, limit: int = 500, offset: int = 0) -> dict:
    where, args = ["1=1"], []
    if sectors:
        s, a = _sector_sql(sectors); where.append(s); args += a
    if no_travel:
        where.append("COALESCE(j.travel,0)=0")
    if q:
        where.append("(j.title LIKE ? OR j.company LIKE ? OR j.location LIKE ? OR j.description LIKE ?)")
        args += [f"%{q}%"] * 4
    if site:
        where.append("j.site=?"); args.append(site)
    if remote:
        where.append("j.is_remote=1")
    if easy_apply:
        where.append("j.easy_apply=1")
    if has_salary:
        where.append("j.salary_annual IS NOT NULL")
    if min_salary:
        where.append("j.salary_annual >= ?"); args.append(min_salary)
    if min_match:
        where.append("uj.match_score >= ?"); args.append(min_match)
    if levels:
        where.append(f"j.level IN ({','.join('?' * len(levels))})"); args += levels
    if categories:
        s, a = _cat_sql(categories); where.append(s); args += a
    if country:                                   # one code or a comma list ("US,CA")
        codes = [c.strip().upper() for c in country.split(",") if c.strip()]
        if codes:
            where.append(f"j.country IN ({','.join('?' * len(codes))})"); args += codes
    if term:
        where.append("j.search_term=?"); args.append(term)
    if since_hours:
        where.append("j.first_seen >= datetime('now', ?)"); args.append(f"-{since_hours} hours")
    if new_only:
        last = last_completed_run_started()
        if last:
            where.append("j.first_seen >= ?"); args.append(last)
    cut = stale_cutoff(stale_after)
    if stale == "hide" and cut:
        where.append("j.last_seen >= ?"); args.append(cut)
    elif stale == "only":
        where.append("j.last_seen < ?" if cut else "0")
        if cut:
            args.append(cut)
    if status == "active":
        where.append(_ACTIVE_SQL)
    elif status in STATUSES and status:
        where.append("uj.status=?"); args.append(status)
    elif status == "pipeline":
        where.append(f"uj.status IN ({','.join('?' * len(PIPELINE))})"); args += PIPELINE
    wsql = " AND ".join(where)
    order = SORTS.get(sort, SORTS["newest"])
    if status == "active" and sort in ("newest", "match"):
        order = "watched DESC, " + order
    with connect() as con:
        total = con.execute(f"SELECT COUNT(*) c FROM jobs j {_joins(uid)} WHERE {wsql}", args).fetchone()["c"]
        rows = con.execute(f"{_select(uid, cut)} WHERE {wsql} ORDER BY {order} LIMIT ? OFFSET ?",
                           args + [limit, offset]).fetchall()
    return {"jobs": [dict(r) for r in rows], "total": total}


def pipeline(uid: int, stale_after: int = 3) -> dict:
    cut = stale_cutoff(stale_after)
    out = {s: [] for s in PIPELINE}
    with connect() as con:
        rows = con.execute(f"{_select(uid, cut)} WHERE uj.status IN ({','.join('?' * len(PIPELINE))}) "
                           "ORDER BY uj.status_at DESC", PIPELINE).fetchall()
    for r in rows:
        out[r["status"]].append(dict(r))
    return out


def archive_stale(uid: int, stale_after: int = 3) -> int:
    cut = stale_cutoff(stale_after)
    if not cut:
        return 0
    with connect() as con:
        ids = [r["id"] for r in con.execute(
            f"SELECT j.id FROM jobs j {_joins(uid)} WHERE j.last_seen < ? AND COALESCE(uj.status,'')=''", (cut,))]
        for jid in ids:
            _upsert_user_job(con, uid, jid, status="archived", status_at=_now())
    return len(ids)


def jobs_for_scoring() -> list[dict]:
    with connect() as con:
        return [dict(r) for r in con.execute(
            "SELECT id, title, company, level, is_remote, country, snippet, description FROM jobs")]


def update_scores(uid: int, rows: list[tuple]):
    """rows: (score, why_json, job_id)"""
    with connect() as con:
        con.executemany(
            "INSERT INTO user_jobs (user_id, job_id, match_score, match_why) VALUES (?, ?, ?, ?) "
            "ON CONFLICT(user_id, job_id) DO UPDATE SET match_score=excluded.match_score, match_why=excluded.match_why",
            [(uid, jid, s, w) for s, w, jid in rows])


def clear_scores(uid: int):
    with connect() as con:
        con.execute("UPDATE user_jobs SET match_score=NULL, match_why=NULL WHERE user_id=?", (uid,))


def digest_jobs(uid: int, hours: int = 24, min_match: int = 0, categories: list[str] | None = None,
                limit: int = 25, sectors: list[str] | None = None, no_travel: bool = False) -> list[dict]:
    with connect() as con:
        sql = f"{_select(uid, None)} WHERE j.first_seen >= datetime('now', ?) AND {_ACTIVE_SQL}"
        args: list = [f"-{hours} hours"]
        if no_travel:
            sql += " AND COALESCE(j.travel,0)=0"
        if min_match:
            sql += " AND uj.match_score >= ?"; args.append(min_match)
        if categories:
            s, a = _cat_sql(categories); sql += f" AND {s}"; args += a
        if sectors:
            s, a = _sector_sql(sectors); sql += f" AND {s}"; args += a
        sql += " ORDER BY uj.match_score IS NULL, uj.match_score DESC, j.first_seen DESC LIMIT ?"
        args.append(limit)
        return [dict(r) for r in con.execute(sql, args)]


# ── aggregates ────────────────────────────────────────────────────────────
def status(uid: int, stale_after: int = 3, categories: list[str] | None = None) -> dict:
    """Header numbers. Two passes over the active rows (one aggregate, one
    grouped by site × level × sector) instead of nine; the dashboard polls this
    every 15 s, so it has to stay cheap."""
    cut = stale_cutoff(stale_after)
    cs, ca = _cat_sql(categories or [])
    with connect() as con:
        base = f"FROM jobs j {_joins(uid)} WHERE {_ACTIVE_SQL} AND {cs}"
        agg = con.execute(
            f"""SELECT COUNT(*) total, SUM(j.first_seen >= datetime('now','-24 hours')) new24,
                       SUM(j.easy_apply=1) easy, SUM(j.salary_annual IS NOT NULL) with_salary,
                       SUM(uj.match_score >= 70) strong, SUM(j.last_seen < ?) stale {base}""",
            [cut or ""] + ca).fetchone()                       # '' < nothing → stale 0 when no cutoff
        groups = con.execute(
            f"SELECT j.site, j.level, COALESCE(j.sector,'other') sector, COUNT(*) c {base} GROUP BY 1, 2, 3", ca).fetchall()
        pipe = {s: 0 for s in PIPELINE}
        for r in con.execute("SELECT status, COUNT(*) c FROM user_jobs WHERE user_id=? GROUP BY status", (uid,)):
            if r["status"] in pipe:
                pipe[r["status"]] = r["c"]
        last = con.execute("SELECT * FROM runs WHERE finished IS NOT NULL ORDER BY id DESC LIMIT 1").fetchone()
    sites: dict[str, int] = {}
    levels: dict[str, int] = {}
    secs: dict[str, int] = {}
    for r in groups:
        sites[r["site"]] = sites.get(r["site"], 0) + r["c"]
        if r["level"] is not None:
            levels[r["level"]] = levels.get(r["level"], 0) + r["c"]
        secs[r["sector"]] = secs.get(r["sector"], 0) + r["c"]
    return {
        "total_jobs": agg["total"] or 0, "new_last_24h": agg["new24"] or 0, "easy_apply": agg["easy"] or 0,
        "with_salary": agg["with_salary"] or 0, "strong_matches": agg["strong"] or 0, "stale": agg["stale"] or 0,
        "saved": pipe["saved"], "applied": pipe["applied"], "pipeline": pipe,
        "last_run": dict(last) if last else None,
        "by_site": [{"site": k, "c": v} for k, v in sorted(sites.items(), key=lambda x: -x[1])],
        "by_sector": [{"sector": k, "c": v} for k, v in sorted(secs.items(), key=lambda x: -x[1])],
        "by_level": sorted(({"level": k, "c": v} for k, v in levels.items()),
                           key=lambda x: LEVEL_ORDER.index(x["level"]) if x["level"] in LEVEL_ORDER else 99),
    }


def _quantiles(vals: list[float]) -> dict:
    if not vals:
        return {}
    v = sorted(vals)
    q = lambda p: v[min(len(v) - 1, int(round(p * (len(v) - 1))))]  # noqa: E731
    return {"n": len(v), "min": v[0], "p25": q(.25), "median": q(.5), "p75": q(.75), "max": v[-1]}


def insights(uid: int, stale_after: int = 3, categories: list[str] | None = None) -> dict:
    cut = stale_cutoff(stale_after)
    cs, ca = _cat_sql(categories or [])
    with connect() as con:
        base = f"FROM jobs j {_joins(uid)} WHERE {_ACTIVE_SQL} AND {cs}"
        sal_rows = con.execute(f"SELECT j.level, j.search_term, j.salary_annual {base} "
                               "AND j.salary_annual BETWEEN 30000 AND 900000", ca).fetchall()
        term_rows = con.execute(
            f"""SELECT j.search_term AS term, COUNT(*) AS jobs,
                   SUM(j.first_seen >= datetime('now','-7 days')) AS new7,
                   SUM(uj.status='saved') AS saved,
                   SUM(uj.status IN ('applied','interviewing','offer')) AS applied,
                   ROUND(AVG(uj.match_score),1) AS avg_match,
                   SUM(uj.match_score >= 70) AS strong
               FROM jobs j {_joins(uid)} WHERE j.search_term IS NOT NULL AND {cs}
               GROUP BY j.search_term ORDER BY jobs DESC""", ca).fetchall()
        stale_n = con.execute(f"SELECT COUNT(*) c {base} AND j.last_seen < ?", ca + [cut]).fetchone()["c"] if cut else 0
        match_hist = con.execute(
            f"SELECT (uj.match_score/10)*10 AS bucket, COUNT(*) c {base} AND uj.match_score IS NOT NULL "
            "GROUP BY bucket ORDER BY bucket", ca).fetchall()
        sector_rows = con.execute(
            f"""SELECT COALESCE(j.sector,'other') AS sector, COUNT(*) AS jobs,
                   SUM(j.first_seen >= datetime('now','-7 days')) AS new7,
                   SUM(uj.status='saved') AS saved,
                   SUM(uj.status IN ('applied','interviewing','offer')) AS applied,
                   ROUND(AVG(uj.match_score),1) AS avg_match,
                   SUM(uj.match_score >= 70) AS strong,
                   COUNT(DISTINCT LOWER(j.company)) AS employers
               {base} GROUP BY 1 ORDER BY jobs DESC""", ca).fetchall()
    by_level: dict[str, list] = {}
    by_term: dict[str, list] = {}
    for r in sal_rows:
        by_level.setdefault(r["level"] or "Other", []).append(r["salary_annual"])
        by_term.setdefault(r["search_term"] or "", []).append(r["salary_annual"])
    lvl = [{"level": l, **_quantiles(v)} for l, v in by_level.items() if len(v) >= 3]
    lvl.sort(key=lambda x: LEVEL_ORDER.index(x["level"]) if x["level"] in LEVEL_ORDER else 99)
    trm = [{"term": t, **_quantiles(v)} for t, v in by_term.items() if len(v) >= 3]
    trm.sort(key=lambda x: -x["median"])
    return {"salary_by_level": lvl, "salary_by_term": trm, "terms": [dict(r) for r in term_rows],
            "sectors": [dict(r) for r in sector_rows],
            "stale": {"count": stale_n, "after_sweeps": stale_after, "cutoff": cut},
            "match_histogram": [dict(r) for r in match_hist]}


def meta(uid: int, configured_terms: list[str] | None = None, categories: dict | None = None,
         sectors: dict | None = None) -> dict:
    cat_keys = [k for k in (categories or {}) if re.fullmatch(r"[a-z0-9_-]+", k)]
    with connect() as con:
        counts = {r["search_term"]: r["c"] for r in con.execute(
            f"SELECT j.search_term, COUNT(*) c FROM jobs j {_joins(uid)} WHERE j.search_term IS NOT NULL "
            f"AND {_ACTIVE_SQL} GROUP BY 1")}
        # one pass gives every category's count (summed LIKE flags) and every sector's count
        cat_sql = "".join(f", SUM(j.categories LIKE '%,{k},%') AS cat_{i}" for i, k in enumerate(cat_keys))
        rows = con.execute(f"SELECT COALESCE(j.sector,'other') AS s, COUNT(*) c{cat_sql} "
                           f"FROM jobs j {_joins(uid)} WHERE {_ACTIVE_SQL} GROUP BY 1").fetchall()
        sec_counts = {r["s"]: r["c"] for r in rows}
        cat_counts = {k: sum((r[f"cat_{i}"] or 0) for r in rows) for i, k in enumerate(cat_keys)}
        sites = [r["site"] for r in con.execute("SELECT DISTINCT site FROM jobs WHERE site IS NOT NULL ORDER BY 1")]
        countries = [r["country"] for r in con.execute(
            "SELECT DISTINCT country FROM jobs WHERE country IS NOT NULL ORDER BY 1")]
    cfg_terms = list(dict.fromkeys(configured_terms or []))
    terms = [{"term": t, "count": counts.get(t, 0), "configured": True} for t in cfg_terms]
    terms += [{"term": t, "count": c, "configured": False} for t, c in sorted(counts.items()) if t not in cfg_terms]
    cats = [{"key": k, "label": v.get("label", k), "count": cat_counts.get(k, 0)} for k, v in (categories or {}).items()]
    secs = [{"key": k, "label": v.get("label", k), "count": sec_counts.get(k, 0)} for k, v in (sectors or {}).items()]
    return {"terms": terms, "sites": sites, "countries": countries, "levels": LEVEL_ORDER,
            "statuses": list(STATUSES), "pipeline": PIPELINE, "categories": cats, "sectors": secs}
