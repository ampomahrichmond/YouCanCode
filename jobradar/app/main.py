"""JobRadar server: FastAPI + scheduled sweeps + per-user digests + dashboard.

The current user comes from the `jr_user` cookie (set by the header switcher)
or an `X-User` header; anything else falls back to user 1."""
import csv
import io
import json
import logging
import subprocess
import sys
import threading
from contextlib import asynccontextmanager
from datetime import datetime, timezone
from pathlib import Path

from apscheduler.schedulers.background import BackgroundScheduler
from fastapi import Depends, FastAPI, File, HTTPException, Query, Request, UploadFile
from fastapi.responses import FileResponse, PlainTextResponse, Response, StreamingResponse
from fastapi.staticfiles import StaticFiles
from pydantic import BaseModel

from . import apply, ats, atscheck, db, digest, docx_export, llm, matching, scraper

logging.basicConfig(level=logging.INFO, format="%(asctime)s %(name)s %(message)s")
log = logging.getLogger("jobradar")

STATIC = Path(__file__).resolve().parent / "static"
ROOT = Path(__file__).resolve().parent.parent
llm.load_env_file()   # ./.env → environment (API key, proxies, SMTP password)
scheduler = BackgroundScheduler(timezone="UTC")
_sweep: dict = {"proc": None, "kind": None}
_sweep_lock = threading.Lock()


def _cfg() -> dict:
    return scraper.load_config()


def _stale_after() -> int:
    return int(_cfg().get("stale_after_sweeps", 3))


# ── sweeps run in a child process ─────────────────────────────────────────
# A sweep parses thousands of responses and tokenises every description; in
# the server process that work starves page requests of the interpreter lock
# (a static file took 3 s to serve mid-sweep). The child shares the SQLite
# database (WAL) and runs below normal CPU priority.
def _sweep_running() -> bool:
    p = _sweep["proc"]
    return p is not None and p.poll() is None


def _start_sweep(kind: str = "full") -> bool:
    """Launch `python -m app.sweep`; False if one is already running."""
    with _sweep_lock:
        if _sweep_running():
            return False
        args = [sys.executable, "-m", "app.sweep"] + (["--ats-only"] if kind == "ats" else [])
        flags = getattr(subprocess, "BELOW_NORMAL_PRIORITY_CLASS", 0)
        _sweep["proc"] = subprocess.Popen(args, cwd=ROOT, creationflags=flags)
        _sweep["kind"] = kind
        log.info("%s sweep started in process %d", kind, _sweep["proc"].pid)
        return True


_rescores: dict[int, subprocess.Popen] = {}


def _start_rescore(u: int) -> bool:
    """Score every role against a freshly uploaded resume in a child process
    (3–4 min of CPU on the current corpus). One at a time per person."""
    p = _rescores.get(u)
    if p is not None and p.poll() is None:
        return False
    flags = getattr(subprocess, "BELOW_NORMAL_PRIORITY_CLASS", 0)
    _rescores[u] = subprocess.Popen([sys.executable, "-m", "app.sweep", "--rescore", str(u)], cwd=ROOT, creationflags=flags)
    log.info("rescore for user %d started in process %d", u, _rescores[u].pid)
    return True


def _stop_sweep():
    for p in [_sweep["proc"], *_rescores.values()]:
        if p is not None and p.poll() is None:
            p.terminate()


def uid(request: Request) -> int:
    raw = request.headers.get("x-user") or request.cookies.get("jr_user") or ""
    try:
        u = int(raw)
    except ValueError:
        return db.DEFAULT_USER
    return u if db.get_user(u) else db.DEFAULT_USER


def _user_cats(u: int) -> list[str]:
    user = db.get_user(u)
    return (user or {}).get("prefs", {}).get("categories") or []


def _cycle():
    _start_sweep("full")


def _digest_job():
    try:
        digest.run_all(_cfg())
    except Exception:
        log.exception("digest failed")


@asynccontextmanager
async def lifespan(app: FastAPI):
    db.init()
    scraper.backfill()
    cfg = _cfg()
    scheduler.add_job(_cycle, "interval", minutes=cfg.get("refresh_minutes", 60), id="scrape",
                      next_run_time=datetime.now(timezone.utc))
    d = cfg.get("digest") or {}
    if d.get("enabled", True):
        hour, minute = (str(d.get("time", "08:00")).split(":") + ["0"])[:2]
        scheduler.add_job(_digest_job, "cron", hour=int(hour), minute=int(minute),
                          id="digest", timezone=d.get("timezone", "America/New_York"))
    scheduler.start()
    yield
    scheduler.shutdown(wait=False)
    _stop_sweep()


app = FastAPI(title="JobRadar", lifespan=lifespan)


# ── users ─────────────────────────────────────────────────────────────────
class UserBody(BaseModel):
    name: str | None = None
    prefs: dict | None = None


@app.get("/api/users")
def api_users(u: int = Depends(uid)):
    return {"users": db.list_users(), "current": u}


@app.post("/api/users")
def api_user_add(body: UserBody):
    if not (body.name or "").strip():
        raise HTTPException(400, "name required")
    return db.add_user(body.name, body.prefs)


@app.put("/api/users/{user_id}")
def api_user_update(user_id: int, body: UserBody):
    u = db.update_user(user_id, body.name, body.prefs)
    if not u:
        raise HTTPException(404, "user not found")
    return u


@app.delete("/api/users/{user_id}")
def api_user_delete(user_id: int):
    if not db.delete_user(user_id):
        raise HTTPException(400, "cannot delete this user")
    return {"deleted": user_id}


# ── jobs ──────────────────────────────────────────────────────────────────
def _filters(q, site, since_hours, remote, new_only, easy_apply, has_salary, min_salary, level, country,
             status, term, sort, min_match, stale, categories, sector="", no_travel=False) -> dict:
    levels = [l.strip() for l in level.split(",") if l.strip()] if level else None
    cats = [c.strip() for c in categories.split(",") if c.strip()] if categories else None
    secs = [s.strip() for s in sector.split(",") if s.strip()] if sector else None
    return dict(q=q, site=site, since_hours=since_hours, remote=remote, new_only=new_only,
                easy_apply=easy_apply, has_salary=has_salary, min_salary=min_salary, levels=levels,
                country=country, status=status, term=term, sort=sort, min_match=min_match, stale=stale,
                stale_after=_stale_after(), categories=cats, sectors=secs, no_travel=no_travel)


@app.get("/api/jobs")
def api_jobs(q: str = "", site: str = "", since_hours: int = Query(0, ge=0), remote: bool = False,
             new_only: bool = False, easy_apply: bool = False, has_salary: bool = False,
             min_salary: int = Query(0, ge=0), level: str = "", country: str = "", status: str = "active",
             term: str = "", sort: str = "newest", min_match: int = Query(0, ge=0, le=100), stale: str = "",
             categories: str = "", sector: str = "", no_travel: bool = False, limit: int = Query(300, ge=1, le=2000),
             offset: int = Query(0, ge=0), u: int = Depends(uid)):
    f = _filters(q, site, since_hours, remote, new_only, easy_apply, has_salary, min_salary, level, country,
                 status, term, sort, min_match, stale, categories, sector, no_travel)
    return db.query_jobs(u, **f, limit=limit, offset=offset)


@app.get("/api/jobs.csv")
def api_jobs_csv(q: str = "", site: str = "", since_hours: int = Query(0, ge=0), remote: bool = False,
                 new_only: bool = False, easy_apply: bool = False, has_salary: bool = False,
                 min_salary: int = Query(0, ge=0), level: str = "", country: str = "", status: str = "active",
                 term: str = "", sort: str = "newest", min_match: int = Query(0, ge=0, le=100), stale: str = "",
                 categories: str = "", sector: str = "", no_travel: bool = False, u: int = Depends(uid)):
    f = _filters(q, site, since_hours, remote, new_only, easy_apply, has_salary, min_salary, level, country,
                 status, term, sort, min_match, stale, categories, sector, no_travel)
    rows = db.query_jobs(u, **f, limit=5000)["jobs"]
    cols = ["title", "company", "location", "country", "level", "categories", "sector", "travel", "site", "is_remote", "easy_apply",
            "match_score", "salary_min", "salary_max", "salary_interval", "salary_annual", "currency", "job_type",
            "date_posted", "first_seen", "last_seen", "stale", "status", "notes", "url", "url_direct", "search_term"]
    buf = io.StringIO()
    w = csv.DictWriter(buf, fieldnames=cols, extrasaction="ignore")
    w.writeheader()
    w.writerows(rows)
    stamp = datetime.now().strftime("%Y%m%d-%H%M")
    return StreamingResponse(iter([buf.getvalue()]), media_type="text/csv",
                             headers={"Content-Disposition": f'attachment; filename="jobradar-{stamp}.csv"'})


class StatusUpdate(BaseModel):
    status: str
    notes: str | None = None


@app.patch("/api/jobs/{jid}")
def api_set_status(jid: str, body: StatusUpdate, u: int = Depends(uid)):
    if body.status not in db.STATUSES:
        raise HTTPException(400, f"status must be one of {db.STATUSES}")
    row = db.set_status(jid, body.status, body.notes, uid=u)
    if not row:
        raise HTTPException(404, "job not found")
    return row


@app.get("/api/pipeline")
def api_pipeline(u: int = Depends(uid)):
    return db.pipeline(u, _stale_after())


@app.post("/api/archive_stale")
def api_archive_stale(u: int = Depends(uid)):
    return {"archived": db.archive_stale(u, _stale_after())}


# ── resume & matching ─────────────────────────────────────────────────────
@app.get("/api/resume")
def api_resume(u: int = Depends(uid)):
    return matching.resume_summary(u)


@app.post("/api/resume")
async def api_resume_upload(file: UploadFile = File(...), u: int = Depends(uid)):
    data = await file.read()
    if len(data) > 10 * 1024 * 1024:
        raise HTTPException(413, "resume larger than 10 MB")
    try:
        text = matching.extract_text(file.filename or "", data)
    except Exception as e:
        raise HTTPException(400, f"could not read {file.filename}: {e}")
    text = text.strip()
    if len(text.split()) < 50:
        raise HTTPException(400, "could not extract enough text from that file (scanned PDF? try DOCX or TXT)")
    db.set_setting("resume_text", text, u)
    db.set_setting("resume_name", file.filename or "resume", u)
    db.set_setting("resume_at", datetime.now(timezone.utc).isoformat(), u)
    db.set_setting("resume_summary", None, u)
    _start_rescore(u)                       # scores arrive over the next few minutes
    return {**matching.resume_summary(u), "scored": None, "queued": db.jobs_count()}


@app.delete("/api/resume")
def api_resume_delete(u: int = Depends(uid)):
    for k in ("resume_text", "resume_name", "resume_at", "resume_summary", "rescore_n"):
        db.set_setting(k, None, u)
    db.clear_scores(u)
    return {"present": False}


# ── companies ─────────────────────────────────────────────────────────────
class CompanyBody(BaseModel):
    name: str
    flag: str


@app.get("/api/companies")
def api_companies(u: int = Depends(uid)):
    return db.list_companies(u)


@app.post("/api/companies")
def api_company_set(body: CompanyBody, u: int = Depends(uid)):
    if body.flag not in ("watch", "block") or not body.name.strip():
        raise HTTPException(400, "need name and flag=watch|block")
    return db.set_company(u, body.name, body.flag)


@app.delete("/api/companies/{name_norm}")
def api_company_delete(name_norm: str, u: int = Depends(uid)):
    db.delete_company(u, name_norm)
    return {"deleted": name_norm}


# ── employer ATS boards (shared) ──────────────────────────────────────────
class AtsBody(BaseModel):
    url: str | None = None
    provider: str | None = None
    slug: str | None = None
    company: str | None = None
    sector: str | None = None


def _sector_labels() -> dict:
    return {k: v.get("label", k) for k, v in (_cfg().get("sectors") or {}).items()}


@app.get("/api/ats")
def api_ats():
    last = db.get_setting("ats_last")
    return {"boards": db.ats_list(), "last": json.loads(last) if last else None,
            "running": _sweep_running(), "providers": ats.LABELS, "list_only": sorted(ats.LIST_ONLY),
            "sectors": _sector_labels()}


@app.post("/api/ats")
def api_ats_add(body: AtsBody):
    prov, slug = body.provider, body.slug
    if body.url:
        url = body.url.strip()
        if ats.is_short(url):
            url = ats.resolve_short(url) or url
        hit = ats.detect(url) or ats.probe(url)
        if not hit:
            raise HTTPException(400, "That URL isn't on a supported career-site system (Greenhouse, Lever, Ashby, "
                                     "SmartRecruiters, Workable, BambooHR, Workday, Oracle Cloud, SuccessFactors, "
                                     "Cornerstone, Taleo, UN Careers). Paste a job or careers-page link.")
        prov, slug = hit
    if prov not in ats.PROVIDERS or not slug:
        raise HTTPException(400, "need url, or provider + slug")
    sector = body.sector if body.sector in _sector_labels() else None
    return {"added": db.ats_add(prov, slug, body.company, "manual", sector), "provider": prov, "slug": slug,
            "sector": sector}


class AtsUpdate(BaseModel):
    enabled: bool | None = None
    sector: str | None = None


@app.patch("/api/ats/{bid}")
def api_ats_update(bid: int, body: AtsUpdate):
    if body.enabled is not None:
        db.ats_set_enabled(bid, body.enabled)
    if body.sector is not None:
        if body.sector and body.sector not in _sector_labels():
            raise HTTPException(400, "unknown sector")
        db.ats_set_sector(bid, body.sector or None)
    return {"id": bid, "enabled": body.enabled, "sector": body.sector}


@app.delete("/api/ats/{bid}")
def api_ats_delete(bid: int):
    db.ats_delete(bid)
    return {"deleted": bid}


@app.post("/api/ats/discover")
def api_ats_discover():
    return ats.discover(_cfg(), full=True)


@app.post("/api/ats/run")
def api_ats_run():
    started = _start_sweep("ats")
    return {"started": started, "note": None if started else "a sweep is already running"}


# ── LLM (optional) ────────────────────────────────────────────────────────
class LetterBody(BaseModel):
    notes: str | None = None


def _job_and_resume(jid: str, u: int):
    job = db.get_job(jid, u)
    if not job:
        raise HTTPException(404, "job not found")
    resume = matching.resume_text(u)
    if not resume:
        raise HTTPException(400, "upload a resume first")
    return job, resume


def _llm_call(jid: str, u: int, kind: str, notes: str = "") -> dict:
    job, resume = _job_and_resume(jid, u)
    model = _cfg().get("llm_model") or llm.DEFAULT_MODEL
    try:
        text = (llm.fit_summary(resume, job, model) if kind == "fit_summary"
                else llm.cover_letter(resume, job, notes or job.get("notes") or "", model))
    except llm.LLMError as e:
        raise HTTPException(503, str(e))
    db.set_field(jid, kind, text, u)
    return {kind: text}


@app.post("/api/jobs/{jid}/fit")
def api_fit(jid: str, u: int = Depends(uid)):
    return _llm_call(jid, u, "fit_summary")


@app.post("/api/jobs/{jid}/cover_letter")
def api_cover_letter(jid: str, body: LetterBody | None = None, u: int = Depends(uid)):
    return _llm_call(jid, u, "cover_letter", (body.notes if body else "") or "")


@app.post("/api/jobs/{jid}/tailor")
def api_tailor(jid: str, u: int = Depends(uid)):
    job, resume = _job_and_resume(jid, u)
    model = _cfg().get("llm_model") or llm.DEFAULT_MODEL
    try:
        data = llm.tailor(resume, job, model)
    except llm.LLMError as e:
        raise HTTPException(503, str(e))
    db.set_field(jid, "tailoring", json.dumps(data, ensure_ascii=False), u)
    return {"tailoring": data}


class DraftBody(BaseModel):
    notes: str | None = None
    refine: bool = False


@app.post("/api/jobs/{jid}/tailored_resume")
def api_tailored_resume(jid: str, body: DraftBody | None = None, u: int = Depends(uid)):
    job, resume = _job_and_resume(jid, u)
    model = _cfg().get("llm_model") or llm.DEFAULT_MODEL
    tailoring = json.loads(job["tailoring"]) if job.get("tailoring") else None
    keywords = atscheck.target_keywords(job, tailoring)
    missing = None
    if body and body.refine and job.get("resume_tailored"):
        missing = atscheck.coverage(job["resume_tailored"], keywords)["missing"]
    try:
        text = llm.tailored_resume(resume, job, tailoring, keywords, model, missing=missing)
    except llm.LLMError as e:
        raise HTTPException(503, str(e))
    db.set_field(jid, "resume_tailored", text, u)
    job["resume_tailored"] = text
    return {"resume_tailored": text, "ats": atscheck.check_job(job, u)}


@app.get("/api/jobs/{jid}/ats_check")
def api_ats_check(jid: str, u: int = Depends(uid)):
    job = db.get_job(jid, u)
    if not job:
        raise HTTPException(404, "job not found")
    return atscheck.check_job(job, u)


def _safe_name(job: dict) -> str:
    return "".join(c if c.isalnum() or c in " -_&" else "" for c in
                   f"{job.get('company') or ''} - {job.get('title') or ''}").strip()[:90]


def _doc_download(jid: str, u: int, field: str, label: str, fmt: str):
    job = db.get_job(jid, u)
    if not job or not job.get(field):
        raise HTTPException(404, f"no {label} for this job yet")
    name = f"{label} - {_safe_name(job)}"
    if fmt == "md":
        return PlainTextResponse(job[field], media_type="text/markdown",
                                 headers={"Content-Disposition": f'attachment; filename="{name}.md"'})
    data = docx_export.md_to_docx(job[field], title_centered=(field == "resume_tailored"))
    return Response(data, media_type="application/vnd.openxmlformats-officedocument.wordprocessingml.document",
                    headers={"Content-Disposition": f'attachment; filename="{name}.docx"'})


@app.get("/api/jobs/{jid}/resume.docx")
def api_resume_docx(jid: str, u: int = Depends(uid)):
    return _doc_download(jid, u, "resume_tailored", "Resume", "docx")


@app.get("/api/jobs/{jid}/resume.md")
def api_resume_md(jid: str, u: int = Depends(uid)):
    return _doc_download(jid, u, "resume_tailored", "Resume", "md")


@app.get("/api/jobs/{jid}/cover_letter.docx")
def api_cover_docx(jid: str, u: int = Depends(uid)):
    return _doc_download(jid, u, "cover_letter", "Cover letter", "docx")


# ── auto-apply ────────────────────────────────────────────────────────────
@app.get("/api/apply/profile")
def api_apply_profile(u: int = Depends(uid)):
    p = apply.get_profile(u)
    return {"profile": p, "missing": apply.profile_ready(p)}


@app.put("/api/apply/profile")
def api_apply_profile_save(body: dict, u: int = Depends(uid)):
    p = apply.save_profile(u, body)
    return {"profile": p, "missing": apply.profile_ready(p)}


@app.get("/api/apply/rules")
def api_apply_rules(u: int = Depends(uid)):
    return apply.get_rules(u)


@app.put("/api/apply/rules")
def api_apply_rules_save(body: dict, u: int = Depends(uid)):
    return apply.save_rules(u, body)


@app.get("/api/apply/queue")
def api_apply_queue(u: int = Depends(uid)):
    rules = apply.get_rules(u)
    return {"queue": db.apply_queue(u, _stale_after()), "eligible": len(apply.eligible(u, rules, _stale_after())),
            "submitted_today": db.submitted_today(u), "session": apply.session_status(), "rules": rules,
            "supported": list(apply.SUPPORTED)}


@app.post("/api/apply/scan")
def api_apply_scan(u: int = Depends(uid)):
    return apply.scan(u, apply.get_rules(u), _stale_after())


class ApplyBody(BaseModel):
    submit: bool = False


@app.post("/api/apply/{jid}/prepare")
def api_apply_prepare(jid: str, body: ApplyBody | None = None, u: int = Depends(uid)):
    submit = bool(body and body.submit)
    if submit and not apply.get_rules(u).get("auto_submit"):
        raise HTTPException(400, "turn on auto_submit in the apply rules to let the engine press Submit")
    if apply.session_status()["running"]:
        raise HTTPException(409, "another application is being prepared")
    result = {}

    def work():
        result.update(apply.prepare(jid, u, submit=submit, headless=False))
    t = threading.Thread(target=work, daemon=True)
    t.start()
    t.join(timeout=170)
    if t.is_alive():
        return {"pending": True, "note": "still filling; watch the browser window and refresh the queue"}
    if result.get("error") and "not found" in result["error"]:
        raise HTTPException(404, result["error"])
    return result


@app.post("/api/apply/{jid}/state")
def api_apply_state(jid: str, body: dict, u: int = Depends(uid)):
    state = body.get("state", "")
    if state not in db.APPLY_STATES:
        raise HTTPException(400, f"state must be one of {db.APPLY_STATES}")
    db.set_apply_state(jid, state, {"note": body.get("note") or f"set to {state or 'cleared'} by user",
                                    "at": datetime.now(timezone.utc).isoformat(timespec="seconds")}, u)
    if state == "submitted":
        db.set_status(jid, "applied", uid=u)
    return {"id": jid, "apply_state": state}


@app.post("/api/apply/run")
def api_apply_run(u: int = Depends(uid)):
    rules = apply.get_rules(u)
    if not rules.get("enabled") or not rules.get("auto_submit"):
        raise HTTPException(400, "enable the rules and auto_submit first")
    threading.Thread(target=apply.run_queue, args=(u, rules, _stale_after()), daemon=True).start()
    return {"started": True}


@app.post("/api/apply/close")
def api_apply_close():
    apply.close_session()
    return {"closed": True}


# ── insights, digest, status ──────────────────────────────────────────────
@app.get("/api/insights")
def api_insights(categories: str = "", u: int = Depends(uid)):
    cats = [c for c in categories.split(",") if c] if categories else _user_cats(u)
    return db.insights(u, _stale_after(), cats)


@app.get("/api/digest", response_class=PlainTextResponse)
def api_digest(u: int = Depends(uid)):
    md = digest.latest(u)
    if md is None:
        md, _ = digest.build(_cfg(), u)
    return md


@app.post("/api/digest/run")
def api_digest_run(u: int = Depends(uid)):
    return digest.run(_cfg(), u)


@app.get("/api/status")
def api_status(categories: str = "", u: int = Depends(uid)):
    cfg = _cfg()
    cats = [c for c in categories.split(",") if c] if categories else None
    s = db.status(u, int(cfg.get("stale_after_sweeps", 3)), cats)
    job = scheduler.get_job("scrape") if scheduler.running else None
    s["next_run"] = job.next_run_time.isoformat() if job and job.next_run_time else None
    s["refreshing"] = _sweep_running()
    s["resume"] = bool(db.get_setting("resume_text", u))
    s["llm"] = llm.available()
    s["last_digest_at"] = db.get_setting("last_digest_at", u)
    s["user"] = db.get_user(u)
    last = db.get_setting("ats_last")
    s["ats"] = {"boards": db.ats_enabled_count(), "last": json.loads(last) if last else None}
    s["config"] = {
        "searches": len(scraper.build_searches(cfg)), "sites": cfg.get("sites") or [],
        "refresh_minutes": cfg.get("refresh_minutes", 60), "hours_old": cfg.get("hours_old", 72),
        "stale_after_sweeps": int(cfg.get("stale_after_sweeps", 3)),
        "digest_time": (cfg.get("digest") or {}).get("time", "08:00"),
        "llm_model": cfg.get("llm_model") or llm.DEFAULT_MODEL,
    }
    return s


@app.get("/api/meta")
def api_meta(u: int = Depends(uid)):
    cfg = _cfg()
    terms = [s["term"] for s in scraper.build_searches(cfg)]
    m = db.meta(u, terms, cfg.get("categories") or {}, cfg.get("sectors") or {})
    labels = cfg.get("country_labels") or scraper.COUNTRY_LABELS
    allowed = [c.upper() for c in cfg.get("countries") or []] or list(labels)
    m["country_options"] = [{"code": c, "label": labels.get(c, c)} for c in allowed if c in labels]
    return m


@app.post("/api/refresh")
def api_refresh():
    started = _start_sweep("full")
    return {"started": started, "note": None if started else "a sweep is already running"}


@app.get("/")
def index():
    return FileResponse(STATIC / "index.html")


app.mount("/static", StaticFiles(directory=STATIC), name="static")
