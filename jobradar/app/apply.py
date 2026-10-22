"""Auto-apply engine: fills employer application forms with a preconfigured
applicant profile, the tailored resume and cover letter, then either stops
for the candidate to review and press Submit (assisted, the default) or
submits on its own when `auto_submit` is on and nothing required is missing.

Works on the public, login-free forms of Greenhouse, Lever, Ashby,
SmartRecruiters and Workable by reading each field's label and mapping it to
the profile. Workday (account per tenant), LinkedIn and Indeed are not
supported. Runs a real, visible Chromium window through Playwright so the
candidate can watch, correct and finish anything the engine could not."""
from __future__ import annotations

import json
import logging
import re
import threading
import time
from datetime import datetime, timezone
from pathlib import Path

from . import db, docx_export

log = logging.getLogger("jobradar.apply")
ROOT = Path(__file__).resolve().parent.parent
APP_DIR = ROOT / "applications"
PROFILE_DIR = ROOT / "browser-profile"
SUPPORTED = ("greenhouse", "lever", "ashby", "smartrecruiters", "workable")

PROFILE_DEFAULT = {
    "first_name": "", "last_name": "", "email": "", "phone": "", "city": "", "state": "", "country": "United States",
    "linkedin": "", "website": "", "github": "", "current_company": "", "current_title": "",
    "address": "", "postal_code": "", "degree": "", "school": "", "field_of_study": "",
    "salary_expectation": "", "start_date": "", "how_heard": "Job board", "pronouns": "", "sms_consent": "no",
    "work_authorized": "yes", "needs_sponsorship": "no", "relocate": "no", "remote_ok": "yes", "hybrid_ok": "yes",
    "over_18": "yes", "previously_employed": "no", "gender": "decline", "hispanic": "decline", "race": "decline",
    "veteran": "decline", "disability": "decline",
    "custom": [],   # [{"match": "regex on the question text", "answer": "text or option"}]
}
RULES_DEFAULT = {
    "enabled": False, "auto_submit": False, "min_match": 75, "levels": [], "providers": list(SUPPORTED),
    "remote_only": False, "countries": ["US", "CA"], "daily_cap": 5, "require_tailored": True, "skip_stale": True,
}

# ── settings ──────────────────────────────────────────────────────────────
def get_profile(uid: int) -> dict:
    raw = db.get_setting("applicant_profile", uid)
    p = dict(PROFILE_DEFAULT)
    if raw:
        try:
            p.update(json.loads(raw))
        except json.JSONDecodeError:
            pass
    return p


def save_profile(uid: int, p: dict) -> dict:
    clean = {k: p.get(k, v) for k, v in PROFILE_DEFAULT.items()}
    clean["custom"] = [c for c in (p.get("custom") or []) if isinstance(c, dict) and c.get("match")]
    db.set_setting("applicant_profile", json.dumps(clean), uid)
    return clean


def get_rules(uid: int) -> dict:
    raw = db.get_setting("apply_rules", uid)
    r = dict(RULES_DEFAULT)
    if raw:
        try:
            r.update(json.loads(raw))
        except json.JSONDecodeError:
            pass
    return r


def save_rules(uid: int, r: dict) -> dict:
    clean = {k: r.get(k, v) for k, v in RULES_DEFAULT.items()}
    clean["providers"] = [p for p in clean["providers"] if p in SUPPORTED]
    db.set_setting("apply_rules", json.dumps(clean), uid)
    return clean


def profile_ready(p: dict) -> list[str]:
    return [k for k in ("first_name", "last_name", "email", "phone") if not (p.get(k) or "").strip()]


# ── queue ─────────────────────────────────────────────────────────────────
def eligible(uid: int, rules: dict, stale_after: int = 3, limit: int = 100) -> list[dict]:
    user = db.get_user(uid) or {"prefs": {}}
    res = db.query_jobs(uid, status="active", min_match=int(rules.get("min_match") or 0),
                        levels=rules.get("levels") or None, remote=bool(rules.get("remote_only")),
                        stale="hide" if rules.get("skip_stale", True) else "", stale_after=stale_after,
                        categories=user["prefs"].get("categories") or None, sort="match", limit=500)
    out = []
    for j in res["jobs"]:
        if j.get("apply_state"):
            continue
        if j.get("site") not in (rules.get("providers") or SUPPORTED):
            continue
        if rules.get("countries") and j.get("country") not in rules["countries"] and j.get("country") is not None:
            continue
        if rules.get("require_tailored") and not j.get("resume_tailored"):
            j["_needs_tailoring"] = True
        if not (j.get("url_direct") or j.get("url")):
            continue
        out.append(j)
        if len(out) >= limit:
            break
    return out


def scan(uid: int, rules: dict, stale_after: int = 3) -> dict:
    jobs = eligible(uid, rules, stale_after)
    for j in jobs:
        db.set_apply_state(j["id"], "queued", {"queued_at": _now(), "note": "matched apply rules"}, uid)
    return {"queued": len(jobs)}


def _now() -> str:
    return datetime.now(timezone.utc).isoformat(timespec="seconds")


# ── documents to attach ───────────────────────────────────────────────────
def _text_to_md(text: str) -> str:
    return "\n\n".join(p.strip() for p in re.split(r"\n\s*\n", text or "") if p.strip())


def build_documents(job: dict, uid: int) -> dict:
    d = APP_DIR / f"u{uid}" / job["id"]
    d.mkdir(parents=True, exist_ok=True)
    out = {}
    resume_md = job.get("resume_tailored") or _text_to_md(db.get_setting("resume_text", uid) or "")
    if resume_md:
        p = d / "resume.docx"
        p.write_bytes(docx_export.md_to_docx(resume_md, title_centered=bool(job.get("resume_tailored"))))
        out["resume"] = str(p)
        out["resume_is_tailored"] = bool(job.get("resume_tailored"))
    if job.get("cover_letter"):
        p = d / "cover_letter.docx"
        p.write_bytes(docx_export.md_to_docx(job["cover_letter"], title_centered=False))
        out["cover_letter"] = str(p)
        out["cover_letter_text"] = job["cover_letter"]
    return out


# ── field mapping ─────────────────────────────────────────────────────────
_YES = {"yes": ["yes", "i am", "i do", "true", "authorized", "eligible"], "no": ["no", "i am not", "i do not", "false", "not "]}
_EEO = {
    "gender": {"male": ["male"], "female": ["female"], "nonbinary": ["non-binary", "nonbinary", "non binary"],
               "decline": ["decline", "prefer not", "do not wish", "don't wish", "not to"]},
    "hispanic": {"yes": ["yes"], "no": ["no"], "decline": ["decline", "prefer not", "do not wish", "don't wish"]},
    "veteran": {"not": ["not a protected veteran", "not a veteran", "no"], "yes": ["identify as one or more", "i am a", "yes"],
                "decline": ["don't wish", "do not wish", "decline", "prefer not"]},
    "disability": {"no": ["no, i do not", "no, i don't", "no"], "yes": ["yes, i have", "yes"],
                   "decline": ["do not want", "don't want", "decline", "prefer not", "do not wish"]},
}
# (regex on the label, profile key, kind) — first match wins; kind: text | file | yesno | eeo | option
_MAP = [
    (r"resume|cv\b|curriculum", "resume", "file"),
    (r"cover[ _]letter", "cover_letter", "cover"),
    (r"^first ?name|given name|first$", "first_name", "text"),
    (r"^last ?name|surname|family name|last$", "last_name", "text"),
    (r"^(full )?name$|^your name|preferred name", "full_name", "text"),
    (r"e-?mail", "email", "text"),
    (r"sms|text message|mobile messages|consent to receive", "sms_consent", "yesno"),
    (r"phone|mobile|telephone", "phone", "text"),
    (r"linkedin", "linkedin", "text"),
    (r"github", "github", "text"),
    (r"website|portfolio|personal site|url", "website", "text"),
    (r"pronoun", "pronouns", "text"),
    (r"current (company|employer)|most recent (company|employer)|^company$|^employer$|organization", "current_company", "text"),
    (r"current (title|role|position)|job title|^title$", "current_title", "text"),
    (r"salary|compensation|pay expectation|rate expectation", "salary_expectation", "text"),
    (r"start date|available to start|availability|notice period|earliest", "start_date", "text"),
    (r"how did you (hear|find|learn)|referral source|^source$|hear about", "how_heard", "option"),
    (r"postal|zip", "postal_code", "text"),
    (r"street|^address|current address|mailing address|home address", "address", "text"),
    (r"^degree|highest (level of )?(education|degree)|education level", "degree", "option"),
    (r"^school|university|college|institution", "school", "text"),
    (r"field of study|major|discipline", "field_of_study", "text"),
    (r"^(current )?(location|city)( \(city\))?$|^city, state|city and state|^location \(|where (are|do) you (live|reside|based)", "city", "text"),
    (r"^state$|^state ?/ ?province|^province", "state", "option"),
    (r"^country|country of residence", "country", "option"),
    (r"sponsor|visa", "needs_sponsorship", "yesno"),
    (r"authori[sz]ed to work|legally (authori[sz]ed|eligible)|eligible to work|work authori|right to work", "work_authorized", "yesno"),
    (r"hybrid|on-?site|in[- ]office|in person", "hybrid_ok", "yesno"),
    (r"relocat", "relocate", "yesno"),
    (r"remote", "remote_ok", "yesno"),
    (r"18 years|at least 18|over 18|age of 18", "over_18", "yesno"),
    (r"previously (worked|employed)|former employee|worked (for|at) .* before|currently or previously", "previously_employed", "yesno"),
    (r"hispanic|latin", "hispanic", "eeo"),
    (r"gender|^sex$|sex\b", "gender", "eeo"),
    (r"race|ethnic", "race", "option"),
    (r"veteran", "veteran", "eeo"),
    (r"disabilit", "disability", "eeo"),
]


def _answer_for(label: str, profile: dict) -> tuple[str | None, str, str]:
    """(answer, kind, key) for a field label, honouring the profile's custom rules first."""
    lab = re.sub(r"\s+", " ", (label or "")).strip().lower()
    lab = re.sub(r"[*✱†]+|\(required\)|\(optional\)|required$", "", lab).strip(" :.-")   # "Country*", "Full name ✱"
    for c in profile.get("custom") or []:
        try:
            if re.search(c["match"], lab, re.I):
                return str(c.get("answer") or ""), "custom", "custom"
        except re.error:
            continue
    for rx, key, kind in _MAP:
        if re.search(rx, lab):
            if key == "full_name":
                return f"{profile.get('first_name','')} {profile.get('last_name','')}".strip(), "text", key
            if kind in ("file", "cover"):
                return key, kind, key
            if key == "city" and profile.get("city") and profile.get("state"):
                return f"{profile['city']}, {profile['state']}", kind, key      # disambiguates place pickers
            return str(profile.get(key) or ""), kind, key
    return None, "", ""


_HEARD_FALLBACK = ["linkedin", "indeed", "job board", "job boards", "online", "internet", "other"]


def _pick_option(options: list[str], answer: str, kind: str, key: str) -> str | None:
    """Best matching option text for an answer."""
    if not options:
        return None
    a = (answer or "").strip().lower()
    low = [o.strip().lower() for o in options]
    if not a:
        return None
    if a in low:
        return options[low.index(a)]
    cands = _EEO.get(key, {}).get(a) if kind == "eeo" else (_YES.get(a) if kind == "yesno" else None)
    if cands:
        for c in cands:
            for i, o in enumerate(low):
                if c in o and not (c == "male" and "female" in o) and not (c == "no" and o.startswith("not ") is False and "no" != o and not o.startswith("no,") and not o.startswith("no ")):
                    return options[i]
    for i, o in enumerate(low):
        if a in o or o in a:
            return options[i]
    return None


# ── browser session ───────────────────────────────────────────────────────
_lock = threading.Lock()
_session: dict = {"pw": None, "ctx": None, "page": None, "jid": None, "since": None, "watch": None}
_state = {"running": False, "current": None}


def session_status() -> dict:
    return {"open": _session["page"] is not None, "jid": _session["jid"], "since": _session["since"],
            "running": _state["running"], "current": _state["current"]}


def close_session():
    try:
        if _session["ctx"]:
            _session["ctx"].close()
        if _session["pw"]:
            _session["pw"].stop()
    except Exception:
        pass
    _session.update(pw=None, ctx=None, page=None, jid=None, since=None)


def _open_page(headless: bool):
    from playwright.sync_api import sync_playwright
    close_session()
    pw = sync_playwright().start()
    PROFILE_DIR.mkdir(exist_ok=True)
    ctx = pw.chromium.launch_persistent_context(
        str(PROFILE_DIR), headless=headless, viewport={"width": 1280, "height": 960},
        args=["--disable-blink-features=AutomationControlled"])
    page = ctx.pages[0] if ctx.pages else ctx.new_page()
    _session.update(pw=pw, ctx=ctx, page=page, since=_now())
    return page


_FIELDS_JS = """
() => {
  const seen = new Set(); const out = [];
  const els = Array.from(document.querySelectorAll('input, select, textarea, [role=combobox], [role=radiogroup], [role=group]'));
  let idx = 0;
  const textOf = el => (el ? (el.innerText || el.textContent || '') : '').replace(/\\s+/g, ' ').trim();
  const labelFor = el => {
    let t = '';
    if (el.id) { const l = document.querySelector(`label[for="${CSS.escape(el.id)}"]`); if (l) t = textOf(l); }
    if (!t && el.getAttribute('aria-label')) t = el.getAttribute('aria-label');
    if (!t && el.getAttribute('aria-labelledby')) t = el.getAttribute('aria-labelledby').split(' ').map(i => textOf(document.getElementById(i))).join(' ');
    if (!t) { const l = el.closest('label'); if (l) t = textOf(l); }
    if (!t) { const fs = el.closest('fieldset'); const lg = fs && fs.querySelector('legend'); if (lg) t = textOf(lg); }
    if (!t) { let p = el.parentElement, hops = 0; while (p && hops < 4 && !t) { const l = p.querySelector('label, legend, .label, [class*=label], [class*=question]'); if (l && !l.contains(el)) t = textOf(l); p = p.parentElement; hops++; } }
    if (!t && el.placeholder) t = el.placeholder;
    if (!t && el.name) t = el.name;
    if (el.type === 'file') t = (t + ' ' + (el.id || '') + ' ' + (el.name || '')).trim();   // "Attach" + id=resume
    return t;
  };
  for (const el of els) {
    const st = window.getComputedStyle(el);
    const type = (el.getAttribute('type') || el.tagName.toLowerCase());
    if (type === 'hidden' || type === 'submit' || type === 'button') continue;
    if (type !== 'file' && (st.display === 'none' || st.visibility === 'hidden') && !el.matches('select')) continue;
    if (type !== 'file' && el.getAttribute('aria-hidden') === 'true') continue;
    if (type !== 'file' && !el.matches('select') && el.offsetWidth === 0 && el.offsetHeight === 0) continue;  // react-select's shadow inputs
    if (type === 'radio' || type === 'checkbox') {
      const key = (el.name || '') + '|' + (el.closest('fieldset, [role=group], [role=radiogroup]') ? 'g' : el.id);
      if (seen.has(key)) continue; seen.add(key);
      const grp = el.closest('fieldset, [role=group], [role=radiogroup]');
      const members = grp ? Array.from(grp.querySelectorAll('input[type=radio], input[type=checkbox]')) : Array.from(document.querySelectorAll(`input[name="${CSS.escape(el.name)}"]`));
      const opts = members.map(m => { m.setAttribute('data-jr-opt', String(idx) + ':' + members.indexOf(m)); return labelFor(m); });
      el.setAttribute('data-jr-idx', String(idx));
      out.push({ idx, type: 'choice', label: grp ? (textOf(grp.querySelector('legend')) || labelFor(el)) : labelFor(el), options: opts,
                 required: members.some(m => m.required || m.getAttribute('aria-required') === 'true'), checked: members.some(m => m.checked) });
      idx++; continue;
    }
    el.setAttribute('data-jr-idx', String(idx));
    const rec = { idx, type, label: labelFor(el), required: !!(el.required || el.getAttribute('aria-required') === 'true'),
                  value: el.value || '', name: el.name || '', role: el.getAttribute('role') || '' };
    if (el.tagName === 'SELECT') rec.options = Array.from(el.options).map(o => o.text.trim()).filter(Boolean);
    out.push(rec); idx++;
  }
  return out;
}
"""


def _combobox_options(page, loc) -> list:
    """Visible options belonging to THIS combobox (react-select points at its
    listbox via aria-controls; other dropdowns on the page may sit in the DOM)."""
    ctrl = loc.get_attribute("aria-controls") or loc.get_attribute("aria-owns")
    if ctrl:
        scoped = page.locator(f'#{ctrl.split()[0]} [role="option"]')
        if scoped.count():
            return [scoped.nth(i) for i in range(min(scoped.count(), 40))]
    vis = page.locator('[role="option"]:visible')
    return [vis.nth(i) for i in range(min(vis.count(), 40))]


def _choose_combobox(page, loc, answer: str, kind: str, key: str) -> str | None:
    """Open a custom dropdown, type to filter, pick the best option. Returns its text."""
    try:
        loc.click()
        page.wait_for_timeout(250)
        typed = answer if kind == "option" else ""      # EEO/yes-no lists are short: don't filter, just scan
        if typed:
            loc.fill(typed)
            page.wait_for_timeout(600)
        options = _combobox_options(page, loc)
        texts = [o.inner_text().strip() for o in options]
        pick = _pick_option(texts, answer, kind, key)
        if pick is None and typed:                       # e.g. a place search: take the first suggestion containing the answer
            pick = next((t for t in texts if answer.lower() in t.lower()), None)
            if pick is None and key == "city" and "," in answer:   # "Wilmington, Delaware" → retry on the city alone
                loc.fill(answer.split(",")[0].strip()); page.wait_for_timeout(600)
                options = _combobox_options(page, loc); texts = [o.inner_text().strip() for o in options]
                st = answer.split(",")[1].strip().lower()
                pick = next((t for t in texts if st and st in t.lower()), None) or (texts[0] if texts else None)
        if pick is None and key == "how_heard":
            loc.fill(""); page.wait_for_timeout(400)
            options = _combobox_options(page, loc); texts = [o.inner_text().strip() for o in options]
            pick = next((t for fb in _HEARD_FALLBACK for t in texts if fb in t.lower()), None)
        if pick is None:
            page.keyboard.press("Escape")
            return None
        options[texts.index(pick)].click()
        page.wait_for_timeout(250)
        return pick
    except Exception as e:
        log.debug("combobox failed: %s", e)
        try:
            page.keyboard.press("Escape")
        except Exception:
            pass
        return None


def _fill_form(page, profile: dict, docs: dict) -> dict:
    """Read every field on the page, fill what the profile answers. Returns a report."""
    fields = page.evaluate(_FIELDS_JS)
    filled, skipped, unanswered = [], [], []
    filled_idx: set[str] = set()
    for f in fields:
        if f["type"] in ("div", "group", "radiogroup"):
            continue                                  # section wrappers; their inputs are handled individually
        loc = page.locator(f'[data-jr-idx="{f["idx"]}"]').first
        label = f.get("label") or ""
        answer, kind, key = _answer_for(label, profile)
        try:
            if kind == "file":
                if docs.get("resume"):
                    loc.set_input_files(docs["resume"]); filled.append((label, "resume.docx"))
                else:
                    unanswered.append(label)
                continue
            if kind == "cover":
                if f["type"] == "file" and docs.get("cover_letter"):
                    loc.set_input_files(docs["cover_letter"]); filled.append((label, "cover_letter.docx"))
                elif f["type"] == "textarea" and docs.get("cover_letter_text"):
                    loc.fill(docs["cover_letter_text"]); filled.append((label, "cover letter text"))
                elif f["type"] == "file":
                    skipped.append(label)          # optional; nothing to attach
                continue
            if answer is None:
                if f.get("required") and not f.get("value") and not f.get("checked"):
                    unanswered.append(label)
                else:
                    skipped.append(label)
                continue
            if not answer:
                if f.get("required"):
                    unanswered.append(label)
                continue
            t = f["type"]
            if t == "choice":
                pick = _pick_option(f.get("options") or [], answer, "yesno" if kind == "yesno" else ("eeo" if kind == "eeo" else "option"), key)
                if pick is None and kind == "yesno":
                    pick = _pick_option(f.get("options") or [], answer, "yesno", key)
                if pick is not None:
                    i = (f.get("options") or []).index(pick)
                    page.locator(f'[data-jr-opt="{f["idx"]}:{i}"]').first.check(force=True)
                    filled.append((label, pick))
                else:
                    unanswered.append(label)
            elif t == "select":
                pick = _pick_option(f.get("options") or [], answer, "yesno" if kind == "yesno" else ("eeo" if kind == "eeo" else "option"), key)
                if pick is None and key == "how_heard":
                    pick = next((o for fb in _HEARD_FALLBACK for o in (f.get("options") or []) if fb in o.lower()), None)
                if pick is not None:
                    loc.select_option(label=pick); filled.append((label, pick))
                else:
                    unanswered.append(label)
            elif f.get("role") == "combobox" or t == "combobox":
                chosen = _choose_combobox(page, loc, answer, "yesno" if kind == "yesno" else ("eeo" if kind == "eeo" else "option"), key)
                if chosen:
                    filled.append((label, chosen))
                else:
                    unanswered.append(label)
            elif t in ("text", "email", "tel", "url", "number", "search", "textarea", "input"):
                loc.fill(answer); filled.append((label, answer))
            else:
                skipped.append(label)
        except Exception as e:  # keep going; the candidate finishes by hand
            log.debug("field '%s' failed: %s", label, e)
            unanswered.append(label)
        if filled and filled[-1][0] == label:
            filled_idx.add(str(f["idx"]))
    # required fields still empty after filling
    missing = page.evaluate("""() => Array.from(document.querySelectorAll('input, select, textarea')).filter(el => {
        const req = el.required || el.getAttribute('aria-required') === 'true'; if (!req) return false;
        if (el.type !== 'file' && el.getAttribute('aria-hidden') === 'true') return false;
        if (el.type !== 'file' && el.tagName !== 'SELECT' && el.offsetWidth === 0 && el.offsetHeight === 0) return false;
        if (el.getAttribute('role') === 'combobox') { const c = el.closest('[class*=control], [class*=select]'); if (c && /single-value|value-container/.test(c.innerHTML) && c.innerText.trim()) return false; }
        if (el.type === 'radio' || el.type === 'checkbox') { const grp = document.querySelectorAll(`input[name="${CSS.escape(el.name)}"]`); return !Array.from(grp).some(g => g.checked); }
        if (el.type === 'file') return el.files.length === 0;
        return !el.value; }).map(el => el.getAttribute('data-jr-idx')).filter(Boolean)""")
    idx2label = {str(f["idx"]): f.get("label") for f in fields}
    # custom dropdowns keep their shadow <input> empty even when a value is chosen, so
    # trust our own record of what was filled over the DOM's idea of "empty"
    filled_labels = {lab for lab, _ in filled}
    still_missing = sorted({idx2label.get(i, i) for i in missing
                            if i not in filled_idx and idx2label.get(i) not in filled_labels})
    return {"filled": filled, "skipped": skipped, "unanswered": sorted(set(unanswered)), "missing_required": still_missing,
            "fields": len(fields)}


_SUBMIT_RX = re.compile(r"submit( application)?|apply( now)?|send application", re.I)
_DONE_RX = re.compile(r"thank you|application (has been |was )?(submitted|received)|we('ve| have) received|successfully submitted", re.I)


def _apply_url(job: dict) -> str:
    url = job.get("url_direct") or job.get("url") or ""
    site = job.get("site")
    if site == "lever" and "/apply" not in url:
        url = url.split("?")[0].rstrip("/") + "/apply"
    if site == "ashby" and "/application" not in url:
        url = url.split("?")[0].rstrip("/") + "/application"
    return url


def _watch_for_confirmation(jid: str, uid: int, page, minutes: int = 25):
    """Assisted mode: after the candidate presses Submit in the open window, mark the job applied."""
    deadline = time.time() + minutes * 60
    while time.time() < deadline and _session.get("page") is page:
        try:
            if _DONE_RX.search(page.inner_text("body")[:20000]) or re.search(r"confirmation|thank", page.url, re.I):
                db.set_apply_state(jid, "submitted", {"submitted_at": _now(), "how": "confirmed in browser"}, uid)
                db.set_status(jid, "applied", uid=uid)
                log.info("application confirmed for %s", jid)
                return
        except Exception:
            pass
        time.sleep(4)


def prepare(jid: str, uid: int, submit: bool = False, headless: bool = False) -> dict:
    """Fill the application for one job. Returns the report (also stored on the job)."""
    if not _lock.acquire(blocking=False):
        return {"error": "another application is being prepared"}
    _state.update(running=True, current=jid)
    job = db.get_job(jid, uid)
    try:
        if not job:
            return {"error": "job not found"}
        if job.get("site") not in SUPPORTED:
            rep = {"error": f"{job.get('site')} applications need an account or are not supported; apply manually",
                   "url": job.get("url_direct") or job.get("url")}
            db.set_apply_state(jid, "skipped", rep, uid)
            return rep
        profile = get_profile(uid)
        gaps = profile_ready(profile)
        if gaps:
            return {"error": "applicant profile incomplete: " + ", ".join(gaps)}
        db.set_apply_state(jid, "preparing", uid=uid)
        docs = build_documents(job, uid)
        url = _apply_url(job)
        page = _open_page(headless)
        _session["jid"] = jid
        page.goto(url, wait_until="domcontentloaded", timeout=45000)
        page.wait_for_timeout(1500)
        # greenhouse legacy boards keep the form behind an "Apply" button
        for sel in ("a:has-text('Apply for this job')", "button:has-text('Apply for this job')", "a:has-text('Apply Now')", "button:has-text('Apply')"):
            try:
                if page.locator(sel).first.is_visible() and not page.locator("input[type=file]").first.is_visible():
                    page.locator(sel).first.click(); page.wait_for_timeout(1500)
                    break
            except Exception:
                pass
        rep = _fill_form(page, profile, docs)
        rep.update(url=url, prepared_at=_now(), resume=docs.get("resume"), resume_is_tailored=docs.get("resume_is_tailored"),
                   cover_letter=bool(docs.get("cover_letter")))
        shot = APP_DIR / f"u{uid}" / jid / "prepared.png"
        try:
            page.screenshot(path=str(shot), full_page=True); rep["screenshot"] = str(shot)
        except Exception:
            pass
        if submit and not rep["missing_required"]:
            btn = page.get_by_role("button", name=_SUBMIT_RX).first
            if btn.count() == 0:
                btn = page.locator("button[type=submit], input[type=submit]").first
            btn.click()
            try:
                page.wait_for_function("() => /thank you|submitted|received/i.test(document.body.innerText)", timeout=25000)
                rep.update(submitted_at=_now(), how="auto")
                db.set_apply_state(jid, "submitted", rep, uid)
                db.set_status(jid, "applied", uid=uid)
                if headless:
                    close_session()
                return rep
            except Exception:
                rep["note"] = "submit clicked but no confirmation seen; check the browser window"
                db.set_apply_state(jid, "needs_review", rep, uid)
                return rep
        state = "prepared" if not rep["missing_required"] else "needs_review"
        db.set_apply_state(jid, state, rep, uid)
        if not headless:
            t = threading.Thread(target=_watch_for_confirmation, args=(jid, uid, page), daemon=True)
            _session["watch"] = t
            t.start()
        return rep
    except Exception as e:
        log.exception("prepare failed for %s", jid)
        rep = {"error": f"{type(e).__name__}: {str(e)[:300]}", "url": job.get("url_direct") if job else None}
        db.set_apply_state(jid, "failed", rep, uid)
        return rep
    finally:
        _state.update(running=False, current=None)
        _lock.release()


def run_queue(uid: int, rules: dict | None = None, stale_after: int = 3) -> dict:
    """Unattended mode: prepare + submit queued jobs up to the daily cap."""
    rules = rules or get_rules(uid)
    if not rules.get("enabled") or not rules.get("auto_submit"):
        return {"ran": 0, "note": "auto_submit is off"}
    budget = int(rules.get("daily_cap") or 0) - db.submitted_today(uid)
    done, results = 0, []
    for j in db.apply_queue(uid, stale_after):
        if budget <= 0:
            break
        if j.get("apply_state") != "queued" or j.get("site") not in SUPPORTED:
            continue
        if rules.get("require_tailored") and not j.get("resume_tailored"):
            continue
        rep = prepare(j["id"], uid, submit=True, headless=False)
        results.append({"id": j["id"], "title": j["title"], "state": db.get_job(j["id"], uid).get("apply_state"), "error": rep.get("error")})
        done += 1
        if rep.get("submitted_at"):
            budget -= 1
        time.sleep(3)
    return {"ran": done, "results": results, "remaining_budget": budget}
