"""Scrape cycle: run every configured search across every enabled site,
enrich (level, country, easy-apply, annualised salary), filter, and upsert
into SQLite. Built on python-jobspy, which handles Indeed / LinkedIn /
Glassdoor / ZipRecruiter / Google Jobs (Google Jobs also surfaces Dice,
Lever, Greenhouse and Workday postings)."""
from __future__ import annotations

import json
import logging
import math
import os
import re
from datetime import datetime, timezone
from pathlib import Path

import pandas as pd
import yaml
from jobspy import scrape_jobs

from . import db, matching, sectors

log = logging.getLogger("jobradar.scraper")
CONFIG_PATH = Path(__file__).resolve().parent.parent / "config.yaml"


def load_config() -> dict:
    with open(CONFIG_PATH, encoding="utf-8") as f:
        cfg = yaml.safe_load(f) or {}
    # categories: normalise, and fold their terms / title keywords into the flat lists
    cats = {}
    for key, c in (cfg.get("categories") or {}).items():
        if isinstance(c, dict):
            cats[str(key)] = {"label": c.get("label") or str(key).title(),
                              "terms": [t for t in (c.get("terms") or []) if t],
                              "title_keywords": [k.lower() for k in (c.get("title_keywords") or []) if k]}
    cfg["categories"] = cats
    flat = [t for t in (cfg.get("terms") or []) if t]
    cfg["terms"] = list(dict.fromkeys([t for c in cats.values() for t in c["terms"]] + flat))
    tmi = [k.lower() for k in (cfg.get("title_must_include") or [])]
    cfg["title_must_include"] = list(dict.fromkeys(tmi + [k for c in cats.values() for k in c["title_keywords"]]))
    sectors.normalise_config(cfg)          # sectors, organizations, _org_index
    cfg["country_labels"] = {**COUNTRY_LABELS, **{str(k).upper(): str(v) for k, v in (cfg.get("country_labels") or {}).items()}}
    acfg = cfg.get("ats")
    if isinstance(acfg, dict):             # `search_terms` is the generic name; `workday_terms` the old one
        acfg["search_terms"] = [t for t in (acfg.get("search_terms") or acfg.get("workday_terms") or []) if t]
    return cfg


def classify(title: str, cfg: dict) -> str:
    """Category keys whose title keywords appear in the title, as ',a,b,' (or ',general,')."""
    t = (title or "").lower()
    keys = [k for k, c in (cfg.get("categories") or {}).items() if any(_has_word(t, kw) for kw in c["title_keywords"])]
    return "," + ",".join(keys or ["general"]) + ","


# ── Glassdoor easyApply capture ───────────────────────────────────────────
# jobspy requests the `easyApply` flag from Glassdoor's API but never maps it
# onto the JobPost. Wrap its per-job parser to remember the flag keyed by URL.
# Guarded so a jobspy internals change degrades to "unknown", never a crash.
_gd_easy: dict[str, bool] = {}
try:
    from jobspy.glassdoor import Glassdoor as _GD

    _gd_orig = _GD._process_job

    def _gd_patched(self, job_data):
        try:
            jid = job_data["jobview"]["job"]["listingId"]
            flag = job_data["jobview"]["header"].get("easyApply")
            if flag is not None:
                _gd_easy[f"{self.base_url}job-listing/j?jl={jid}"] = bool(flag)
        except Exception:
            pass
        return _gd_orig(self, job_data)

    _GD._process_job = _gd_patched
except Exception:  # pragma: no cover
    log.warning("could not hook Glassdoor easyApply capture")


# ── enrichment helpers ────────────────────────────────────────────────────
def _clean(v):
    if v is None:
        return None
    if isinstance(v, float) and math.isnan(v):
        return None
    return v


_RX = {
    "C-level": re.compile(r"\b(chief|cdo|cio|cto|cdao|ciso|cfo|coo)\b", re.I),
    "VP": re.compile(r"\b(vp|svp|evp|avp|vice[\s-]?president)\b", re.I),
    "Head": re.compile(r"\bhead\b", re.I),
    "Director": re.compile(r"\bdirector\b", re.I),
    "Manager": re.compile(r"\bmanager\b", re.I),
    "Senior": re.compile(r"\b(senior|sr\.?|principal|group)\b|\b(manager|analyst)\s+(ii|iii|iv)\b", re.I),
    "Lead": re.compile(r"\b(lead|principal|owner)\b", re.I),
    "Analyst": re.compile(r"\banalyst\b", re.I),
}


def infer_level(title: str, board_level: str | None = None) -> str:
    """Seniority bucket from the title, falling back to the board's own label."""
    t = title or ""
    for name in ("C-level", "VP", "Head", "Director"):
        if _RX[name].search(t):
            return name
    if _RX["Manager"].search(t):
        return "Senior Manager" if _RX["Senior"].search(t) else "Manager"
    if _RX["Analyst"].search(t):
        return "Senior Analyst" if _RX["Senior"].search(t) or _RX["Lead"].search(t) else "Analyst"
    if _RX["Lead"].search(t):
        return "Lead"
    bl = (board_level or "").lower()
    if bl == "director":
        return "Director"
    if bl == "executive":
        return "VP"
    # UN common-system grades (P-1…P-5, D-1/D-2, ASG/USG) carry the seniority the title omits
    g = re.fullmatch(r"\s*(usg|asg|d|p|no|g|fs)\s*-?\s*([a-z0-9])?\s*", bl)
    if g:
        band, step = g.group(1), g.group(2) or ""
        if band in ("usg", "asg"):
            return "C-level"
        if band == "d":
            return "Director"
        if band == "p":
            return {"5": "Senior Manager", "4": "Manager", "3": "Lead", "2": "Analyst", "1": "Analyst"}.get(step, "Other")
        if band == "no":
            return {"d": "Manager", "c": "Lead", "b": "Analyst", "a": "Analyst"}.get(step, "Other")
    return "Other"


_US_STATES = set("""AL AK AZ AR CA CO CT DE FL GA HI ID IL IN IA KS KY LA ME MD MA MI MN MS MO MT NE NV
NH NJ NM NY NC ND OH OK OR PA RI SC SD TN TX UT VT VA WA WV WI WY DC PR""".split())
_CA_PROVINCES = set("AB BC MB NB NL NS NT NU ON PE QC SK YT".split())
_CA_NAMES = {"alberta", "british columbia", "manitoba", "new brunswick", "newfoundland and labrador",
             "nova scotia", "northwest territories", "nunavut", "ontario", "prince edward island",
             "quebec", "québec", "saskatchewan", "yukon"}
_US_NAMES = {"alabama", "alaska", "arizona", "arkansas", "california", "colorado", "connecticut",
             "delaware", "florida", "georgia", "hawaii", "idaho", "illinois", "indiana", "iowa",
             "kansas", "kentucky", "louisiana", "maine", "maryland", "massachusetts", "michigan",
             "minnesota", "mississippi", "missouri", "montana", "nebraska", "nevada", "new hampshire",
             "new jersey", "new mexico", "new york", "north carolina", "north dakota", "ohio",
             "oklahoma", "oregon", "pennsylvania", "rhode island", "south carolina", "south dakota",
             "tennessee", "texas", "utah", "vermont", "virginia", "washington", "west virginia",
             "wisconsin", "wyoming", "district of columbia", "puerto rico", "washington dc", "washington d.c.",
             "washington d.c", "new york city", "nyc"}


# Country names and regions that mark a role as outside US/Canada. Cities are
# left out on purpose: Rome NY, Geneva IL, Vienna VA, Kingston ON all exist.
_FOREIGN_RX = re.compile(
    r"\b(uk|u\.k\.|united kingdom|england|scotland|wales|ireland|germany|france|spain|portugal|italy|"
    r"netherlands|belgium|switzerland|austria|sweden|norway|denmark|finland|poland|czech|czechia|hungary|romania|"
    r"greece|turkey|türkiye|israel|uae|dubai|saudi|saudi arabia|qatar|india|pakistan|bangladesh|sri lanka|philippines|"
    r"singapore|malaysia|indonesia|thailand|vietnam|viet nam|china|hong kong|taiwan|japan|korea|australia|"
    r"new zealand|brazil|argentina|chile|colombia|peru|mexico|costa rica|south africa|nigeria|kenya|egypt|"
    r"afghanistan|albania|algeria|angola|armenia|azerbaijan|bahrain|barbados|belarus|benin|bhutan|bolivia|bosnia|"
    r"botswana|bulgaria|burkina faso|burundi|cambodia|cameroon|cape verde|cabo verde|chad|congo|cote d'?ivoire|"
    r"côte d'?ivoire|ivory coast|croatia|cuba|cyprus|djibouti|dominican republic|ecuador|el salvador|eritrea|estonia|"
    r"eswatini|ethiopia|fiji|gabon|gambia|ghana|guatemala|guinea|guyana|haiti|honduras|iceland|iran|iraq|jamaica|"
    r"jordan|kazakhstan|kosovo|kuwait|kyrgyzstan|laos|latvia|lebanon|lesotho|liberia|libya|lithuania|luxembourg|"
    r"madagascar|malawi|maldives|mali|malta|mauritania|mauritius|moldova|mongolia|montenegro|morocco|mozambique|"
    r"myanmar|namibia|nepal|nicaragua|niger|north macedonia|oman|panama|papua new guinea|paraguay|rwanda|senegal|"
    r"serbia|sierra leone|slovakia|slovenia|somalia|south sudan|sudan|syria|tajikistan|tanzania|timor-leste|togo|"
    r"tunisia|turkmenistan|uganda|ukraine|united arab emirates|uruguay|uzbekistan|venezuela|yemen|zambia|zimbabwe|"
    r"europe|emea|apac|latam|asia|africa|middle east|caribbean|oceania|worldwide|global)\b", re.I)


# Countries roles can be tagged with besides US/CA: the UAE and the hubs whose
# employers routinely hire remote talent in North America. Code → name/city
# patterns (checked in this order; AU before GB so "New South Wales" isn't Wales).
COUNTRY_RX = {
    "AE": re.compile(r"united arab emirates|\bu\.?a\.?e\b|\bdubai\b|abu dhabi|\bsharjah\b", re.I),
    "AU": re.compile(r"\baustralia\b|new south wales|\bqueensland\b|\bsydney\b|\bmelbourne\b|\bbrisbane\b|\bcanberra\b", re.I),
    "GB": re.compile(r"united kingdom|\bu\.?k\b|\bengland\b|\bscotland\b|\bwales\b|great britain|\blondon\b|\bmanchester\b|\bedinburgh\b|\bglasgow\b", re.I),
    "IE": re.compile(r"\bireland\b|\bdublin\b", re.I),
    "DE": re.compile(r"\bgermany\b|\bdeutschland\b|\bberlin\b|\bmunich\b|\bmünchen\b|\bfrankfurt\b|\bhamburg\b", re.I),
    "NL": re.compile(r"\bnetherlands\b|\bamsterdam\b|\brotterdam\b|the hague", re.I),
    "CH": re.compile(r"\bswitzerland\b|\bzurich\b|\bzürich\b|\bgeneva\b|\bbasel\b|\blausanne\b", re.I),
    "FR": re.compile(r"\bfrance\b|\bparis\b", re.I),
    "SG": re.compile(r"\bsingapore\b", re.I),
    "IL": re.compile(r"\bisrael\b|tel aviv|\bjerusalem\b|\bhaifa\b", re.I),
    "SA": re.compile(r"saudi arabia|\bsaudi\b|\briyadh\b|\bjeddah\b|\bksa\b", re.I),
    "QA": re.compile(r"\bqatar\b|\bdoha\b", re.I),
}
COUNTRY_LABELS = {"US": "United States", "CA": "Canada", "AE": "United Arab Emirates", "GB": "United Kingdom",
                  "IE": "Ireland", "DE": "Germany", "NL": "Netherlands", "CH": "Switzerland", "FR": "France",
                  "SG": "Singapore", "AU": "Australia", "IL": "Israel", "SA": "Saudi Arabia", "QA": "Qatar"}
COUNTRIES = tuple(COUNTRY_LABELS)
_ISO_ALIASES = {"USA": "US", "CAN": "CA", "ARE": "AE", "UAE": "AE", "GBR": "GB", "UK": "GB", "IRL": "IE", "DEU": "DE",
                "NLD": "NL", "CHE": "CH", "FRA": "FR", "SGP": "SG", "AUS": "AU", "ISR": "IL", "SAU": "SA", "KSA": "SA", "QAT": "QA"}


def country_code(value: str | None) -> str | None:
    """ISO-2/ISO-3 code or a country name → one of COUNTRIES, 'Other', or None."""
    if not value:
        return None
    v = str(value).strip()
    key = v.upper()
    if key in COUNTRY_LABELS:
        return key
    if key in _ISO_ALIASES:
        return _ISO_ALIASES[key]
    return infer_country(v, "Other")


def country_ok(code: str | None, cfg: dict) -> bool:
    """Config `countries` keeps roles in those countries; unknown (None) passes."""
    allowed = [c.upper() for c in cfg.get("countries") or []]
    return not allowed or not code or code in allowed


def infer_country(location: str | None, default: str | None = None) -> str | None:
    """A code from COUNTRIES, 'Other', or None when the string carries no signal
    (e.g. 'Remote'). An explicit US state / Canadian province suffix wins over
    a foreign-looking word ("Rome, NY" is in the US; "Rome, Italy" is not;
    "London, ON" is in Canada, "London" alone is in the UK)."""
    if not location:
        return default
    # "Wilmington, DE 19801 (Job Posting)", "Columbus, OH 43068", "Toronto, ON - CCIT" → drop the noise
    loc = re.sub(r"\([^)]*\)", " ", location)
    loc = re.sub(r"\s+-\s+[A-Z]{2,6}\b", " ", loc)
    loc = re.sub(r"\b\d{5}(?:-\d{4})?\b|\b[A-Z]\d[A-Z]\s?\d[A-Z]\d\b", " ", loc)
    parts = [re.sub(r"\s+", " ", p).strip() for p in loc.split(",")]
    parts = [p for p in parts if p]
    if not parts or re.fullmatch(r"\d+ locations?", parts[-1].lower()):
        return default
    last = parts[-1]
    ll = last.lower()
    if ll in ("us", "usa", "united states", "u.s.", "united states of america") or ll in _US_NAMES:
        return "US"
    if ll == "can" or (ll in ("canada", "ca") and len(parts) != 2):   # ', CA' alone is California
        return "CA"
    if ll in _CA_NAMES:
        return "CA"
    if len(parts) >= 2:
        st = last.upper()
        if st in _US_STATES:
            return "US"
        if st in _CA_PROVINCES:
            return "CA"
        if ll == "canada":
            return "CA"
    if not re.search(r"\b(US|USA|United States|Canada)\b", location, re.I):
        for code, rx in COUNTRY_RX.items():
            if rx.search(location):
                return code      # "Dubai, United Arab Emirates", "Remote - UK", "Berlin, Germany"
    if _FOREIGN_RX.search(location) and not re.search(r"\b(US|USA|United States|Canada)\b", location, re.I):
        return "Other"           # "Remote Spain", "Remote - Europe", "Tanzania"
    if len(parts) >= 2:
        return "Other"  # "London Area, United Kingdom", "Taguig, NCR, Philippines"
    if ll in ("remote", "anywhere", "united states remote", "remote, us"):
        return default
    return default


def _annualise(mn, mx, interval) -> float | None:
    amt = mx or mn
    if not amt:
        return None
    mult = {"hourly": 2080, "daily": 260, "weekly": 52, "monthly": 12}.get((interval or "").lower(), 1)
    val = float(amt) * mult
    # a "yearly" figure under $1k is almost certainly an hourly rate mis-tagged by the board
    if mult == 1 and val < 1000:
        val *= 2080
    return round(val)


_INDEED_APPLY = re.compile(r"indeed\.com/(job|applystart|viewjob)|indeed-apply|indeedapply", re.I)

# ── travel requirement, read from the description ─────────────────────────
_TRAVEL_NOISE = re.compile(r"travel (industry|insurance|agency|agent|benefits?|discounts?|perks?|reimbursements?|expenses?|and expense|"
                           r"& expense|nurs(e|ing)|cards?|booking|management|technology|tech|sector|company|companies)|"
                           r"\btravelers\b|travel\s*\+\s*leisure|business travel (policy|program)", re.I)
_TRAVEL_NO = re.compile(r"\b(no|without|zero|little to no|minimal to no)\s+(?:\w+\s+){0,2}travel\b|"
                        r"\btravel\b[^.\n;]{0,30}?\b(?:is\s+|will\s+)?not\s+(?:be\s+)?(required|expected|necessary|needed|anticipated)|"
                        r"\b(does|do) not require (?:any )?travel|\b0\s?%\s*travel", re.I)
_TRAVEL_PCT = re.compile(r"(\d{1,3})\s?%\s*(?:\w+\s+){0,2}travel\b|\btravel\b[^.\n;]{0,45}?(\d{1,3})\s?%", re.I)
_TRAVEL_YES = re.compile(r"\b(willing(?:ness)?|ability|able|must be able|required|expected|need|needs|needed|requirement|prepared|availability|available)"
                         r"\s+(?:\w+\s+){0,2}to\s+travel\b|"
                         r"\b(frequent|regular|occasional|extensive|moderate|domestic|international|overnight|some|significant|periodic|"
                         r"business|heavy|light|limited|minimal|infrequent|rare|minor|monthly|weekly|quarterly|global|regional|local)\s+travel\b|"
                         r"\btravel\s+(?:is|will be|may be|might be)\s+(?:\w+\s+){0,2}(required|expected|necessary|needed|involved|anticipated)|"
                         r"\btravel\s*(requirements?|required|expectations?|expected)\b|\btravel\s*:\s*(yes|\d|up to|required|some|occasional)|"
                         r"\btravel\s+(up to|as needed|as required|to client|to customer|to other|between)\b|\bmust travel\b", re.I)


def infer_travel(description: str | None) -> int | None:
    """None = no description to judge from; 0 = no travel requirement found;
    1 = travel required; 2..100 = the percentage the posting states."""
    if not description:
        return None
    text = _TRAVEL_NOISE.sub(" ", description)
    if _TRAVEL_NO.search(text):
        return 0
    m = _TRAVEL_PCT.search(text)
    if m:
        pct = int(m.group(1) or m.group(2))
        if 0 < pct <= 100:
            return pct if pct > 1 else 1
        return 0 if pct == 0 else None
    return 1 if _TRAVEL_YES.search(text) else 0


def infer_easy_apply(site: str, url: str | None, url_direct: str | None,
                     description: str | None) -> bool | None:
    """1 = the application happens on the board itself (Indeed Apply, ZipRecruiter
    1-Click, Glassdoor Easy Apply, LinkedIn Easy Apply); 0 = redirects to an ATS;
    None = board doesn't tell us. LinkedIn is resolved separately (see run_cycle)."""
    site = (site or "").lower()
    if site == "indeed":
        if not url_direct:
            return None
        return bool(_INDEED_APPLY.search(url_direct))
    if site == "zip_recruiter":
        if not description:
            return None          # detail page fetch failed → unknown
        return url_direct is None
    if site == "glassdoor":
        return _gd_easy.get(url)
    return None


def _row_to_job(row: pd.Series, term: str, country_default: str | None, cfg: dict | None = None) -> dict:
    cfg = cfg or {}
    desc = _clean(row.get("description")) or ""
    desc = re.sub(r"\n{3,}", "\n\n", desc).strip()
    site = _clean(row.get("site"))
    url = _clean(row.get("job_url"))
    url_direct = _clean(row.get("job_url_direct"))
    title = _clean(row.get("title"))
    loc = _clean(row.get("location"))
    mn, mx = _clean(row.get("min_amount")), _clean(row.get("max_amount"))
    interval = _clean(row.get("interval"))
    board_level = _clean(row.get("job_level"))
    dp = _clean(row.get("date_posted"))
    return {
        "title": title,
        "company": _clean(row.get("company")),
        "location": loc,
        "site": site,
        "url": url,
        "url_direct": url_direct if url_direct and url_direct != url else None,
        "date_posted": str(dp)[:10] if dp else "",
        "salary_min": mn,
        "salary_max": mx,
        "salary_interval": interval,
        "salary_annual": _annualise(mn, mx, interval),
        "currency": _clean(row.get("currency")),
        "salary_source": _clean(row.get("salary_source")),
        "is_remote": bool(_clean(row.get("is_remote"))),
        "easy_apply": infer_easy_apply(site, url, url_direct, desc),
        "job_type": _clean(row.get("job_type")),
        "job_level": board_level,
        "level": infer_level(title, board_level),
        "job_function": _clean(row.get("job_function")),
        "company_industry": _clean(row.get("company_industry")),
        "country": infer_country(loc, country_default),
        "travel": infer_travel(desc),
        "snippet": desc[:400],
        "description": desc[:4000],
        "search_term": term,
        "categories": classify(title, cfg),
        "sector": sectors.classify(_clean(row.get("company")), _clean(row.get("company_industry")), cfg),
    }


# ── filtering ─────────────────────────────────────────────────────────────
_word_cache: dict[str, re.Pattern] = {}


def _has_word(text: str, keyword: str) -> bool:
    """Whole-word match so 'hr' doesn't hit 'three' and 'bi' doesn't hit 'bid'."""
    rx = _word_cache.get(keyword)
    if rx is None:
        rx = _word_cache[keyword] = re.compile(r"(?<![a-z0-9])" + re.escape(keyword) + r"(?![a-z0-9])", re.I)
    return bool(rx.search(text))


def _passes_filters(job: dict, cfg: dict) -> tuple[bool, str]:
    title = (job.get("title") or "").lower()
    text = f"{title} {job.get('description') or ''}".lower()
    tk = [k.lower() for k in cfg.get("title_keywords") or []]
    tmi = [k.lower() for k in cfg.get("title_must_include") or []]
    inc = [k.lower() for k in cfg.get("include_keywords") or []]
    exc = [k.lower() for k in cfg.get("exclude_keywords") or []]
    exc_t = [k.lower() for k in cfg.get("exclude_title_keywords") or []]
    if tk and not any(_has_word(title, k) for k in tk):
        return False, "title_level"
    if tmi and not any(_has_word(title, k) for k in tmi):
        return False, "title_topic"
    if inc and not any(k in text for k in inc):
        return False, "include"
    if any(k in text for k in exc):
        return False, "exclude"
    if any(_has_word(title, k) for k in exc_t):
        return False, "exclude_title"
    if cfg.get("remote_only") and not job.get("is_remote"):
        return False, "remote"
    if not country_ok(job.get("country"), cfg):
        return False, "country"
    min_sal = cfg.get("min_salary") or 0
    if min_sal and job.get("salary_annual") and job["salary_annual"] < min_sal:
        return False, "salary"
    levels = cfg.get("levels") or []
    if levels and job.get("level") not in levels:
        return False, "level"
    return True, ""


# ── search plan ───────────────────────────────────────────────────────────
def build_searches(cfg: dict) -> list[dict]:
    """terms × locations, plus any explicit `searches` entries."""
    out = []
    locs = []
    for loc in cfg.get("locations") or []:
        if isinstance(loc, str):
            loc = {"name": loc}
        locs.append({"name": loc.get("name", ""), "country": loc.get("country", "USA"),
                     "remote": bool(loc.get("remote"))})
    for term in cfg.get("terms") or []:
        for loc in locs:
            out.append({"term": term, **loc})
    for s in cfg.get("searches") or []:
        out.append({"term": s.get("term", ""), "name": s.get("location", ""),
                    "country": s.get("country", cfg.get("country_indeed", "USA")),
                    "remote": bool(s.get("remote"))})
    return [s for s in out if s["term"]]


def load_proxies(cfg: dict) -> list[str]:
    """Proxy list from config `proxies`, the env var named by `proxies_env`
    (comma-separated), and the file named by `proxies_file` (one per line,
    '#' comments) so credentials never have to live in config.yaml. Re-read at
    every sweep, so a new proxies file takes effect without a restart.
    Entries look like user:pass@host:port or host:port."""
    out = [str(p).strip() for p in cfg.get("proxies") or [] if str(p).strip()]
    env_name = cfg.get("proxies_env") or "JOBRADAR_PROXIES"
    out += [p.strip() for p in os.environ.get(env_name, "").split(",") if p.strip()]
    pf = cfg.get("proxies_file")
    if pf:
        path = Path(pf) if Path(pf).is_absolute() else CONFIG_PATH.parent / pf
        if path.exists():
            for line in path.read_text(encoding="utf-8-sig").splitlines():  # -sig: tolerate Notepad's BOM
                line = line.split("#", 1)[0].strip()
                if line:
                    out.append(line)
    return list(dict.fromkeys(out))


def _proxy_groups(cfg: dict, sites: list[str]) -> list[tuple[list[str], list[str] | None]]:
    """Split sites into (sites, proxies) groups. `proxy_sites` limits paid proxy
    traffic to the boards that need it; empty means every board goes via proxy."""
    proxies = load_proxies(cfg)
    if not proxies:
        return [(sites, None)]
    only = [s.lower() for s in cfg.get("proxy_sites") or []]
    if not only:
        return [(sites, proxies)]
    via = [s for s in sites if s in only]
    direct = [s for s in sites if s not in only]
    return [g for g in ((via, proxies), (direct, None)) if g[0]]


def _scrape(cfg: dict, sites: list[str], term: str, location: str, country: str,
            is_remote: bool = False, easy_apply: bool | None = None,
            fetch_desc: bool | None = None, proxies: list[str] | None = None) -> pd.DataFrame:
    if fetch_desc is None:
        fetch_desc = bool(cfg.get("linkedin_fetch_description", True))
    return scrape_jobs(
        site_name=sites,
        search_term=term,
        location=location,
        results_wanted=cfg.get("results_per_site", 20),
        hours_old=cfg.get("hours_old", 72),
        country_indeed=country or "USA",
        proxies=proxies,
        is_remote=is_remote,
        easy_apply=easy_apply or None,
        linkedin_fetch_description=fetch_desc,
        verbose=0,
    )


def backfill() -> int:
    """Fill level / country / salary_annual on rows stored before those columns
    existed, re-bucket 'Other' rows in case the level rules grew, and (re)tag
    categories for every row so config changes apply to old data. Idempotent."""
    cfg = load_config()
    with db.connect() as con:
        rows = con.execute(
            "SELECT id, title, location, salary_min, salary_max, salary_interval, job_level "
            "FROM jobs WHERE level IS NULL OR level='Other' "
            "OR (salary_annual IS NULL AND salary_max IS NOT NULL)"
        ).fetchall()
        for r in rows:
            con.execute(
                "UPDATE jobs SET level=?, country=COALESCE(country, ?), salary_annual=? WHERE id=?",
                (infer_level(r["title"], r["job_level"]), infer_country(r["location"]),
                 _annualise(r["salary_min"], r["salary_max"], r["salary_interval"]), r["id"]),
            )
        sig = json.dumps({k: c["title_keywords"] for k, c in cfg.get("categories", {}).items()}, sort_keys=True)
        if con.execute("SELECT value FROM settings WHERE key='categories_sig'").fetchone() != (sig,) or True:
            stored = con.execute("SELECT value FROM settings WHERE key='categories_sig'").fetchone()
            if not stored or stored["value"] != sig:
                allrows = con.execute("SELECT id, title FROM jobs").fetchall()
                con.executemany("UPDATE jobs SET categories=? WHERE id=?",
                                [(classify(r["title"], cfg), r["id"]) for r in allrows])
                con.execute("INSERT INTO settings (key,value) VALUES ('categories_sig',?) "
                            "ON CONFLICT(key) DO UPDATE SET value=excluded.value", (sig,))
                log.info("re-tagged categories on %d rows", len(allrows))
        # sectors: re-tag everything when the rules (sectors / organizations) change
        ssig = sectors.signature(cfg)
        stored = con.execute("SELECT value FROM settings WHERE key='sectors_sig'").fetchone()
        if not stored or stored["value"] != ssig:
            board_sector = {(b["provider"], (b["company"] or "").lower()): b["sector"]
                            for b in con.execute("SELECT provider, company, sector FROM ats_boards WHERE sector IS NOT NULL")}
            allrows = con.execute("SELECT id, company, company_industry, site FROM jobs").fetchall()
            con.executemany("UPDATE jobs SET sector=? WHERE id=?", [
                (sectors.classify(r["company"], r["company_industry"], cfg,
                                  board_sector.get((r["site"], (r["company"] or "").lower()))), r["id"])
                for r in allrows])
            con.execute("INSERT INTO settings (key,value) VALUES ('sectors_sig',?) "
                        "ON CONFLICT(key) DO UPDATE SET value=excluded.value", (ssig,))
            log.info("re-tagged sectors on %d rows", len(allrows))
        # one-time: travel flag for rows stored before the column existed (streamed, ids only kept)
        if not con.execute("SELECT 1 FROM settings WHERE key='travel_backfilled'").fetchone():
            updates = []
            cur = con.execute("SELECT id, description FROM jobs WHERE travel IS NULL AND description IS NOT NULL AND description != ''")
            while True:
                chunk = cur.fetchmany(1000)
                if not chunk:
                    break
                updates += [(infer_travel(r["description"]), r["id"]) for r in chunk]
            con.executemany("UPDATE jobs SET travel=? WHERE id=?", updates)
            con.execute("INSERT INTO settings (key,value) VALUES ('travel_backfilled','1')")
            log.info("travel flag computed for %d rows", len(updates))
        # one-time: countries beyond US/CA (UAE, UK, …) for rows that resolved to Other/unknown before
        if not con.execute("SELECT 1 FROM settings WHERE key='country_v2'").fetchone():
            rows2 = con.execute("SELECT id, location, country FROM jobs WHERE country IS NULL OR country='Other'").fetchall()
            upd = [(c, r["id"]) for r in rows2 if (c := infer_country(r["location"], r["country"])) != r["country"]]
            con.executemany("UPDATE jobs SET country=? WHERE id=?", upd)
            con.execute("INSERT INTO settings (key,value) VALUES ('country_v2','1')")
            log.info("country re-resolved on %d rows", len(upd))
    if rows:
        log.info("backfilled %d rows", len(rows))
    return len(rows)


def _store(batch: list[dict], cfg: dict | None = None) -> int:
    """Upsert a batch and attach match scores for every user who has a resume.
    Rows without a sector (employer-board rows) are tagged here."""
    if not batch:
        return 0
    if any(not j.get("sector") for j in batch):
        cfg = cfg or load_config()
        for j in batch:
            if not j.get("sector"):
                j["sector"] = sectors.classify(j.get("company"), j.get("company_industry"), cfg)
    scores = {}
    try:
        scores = matching.score_batch(batch)
    except Exception:
        log.exception("resume scoring failed for batch")
    added = db.upsert_jobs(batch)          # assigns job['id']
    for uid, rows in scores.items():
        try:
            db.update_scores(uid, [(s, w, j["id"]) for s, w, j in rows if j.get("id")])
        except Exception:
            log.exception("storing scores failed for user %s", uid)
    return added


def run_ats_pass() -> dict:
    """Career sites only (the Companies tab's "Check sites now"). Same storage
    path as the sweep's first stage, recorded as its own run."""
    from . import ats
    cfg = load_config()
    run_id = db.start_run()
    res = ats.run(cfg)
    batch = res["jobs"]
    for j in batch:
        j.setdefault("categories", classify(j.get("title") or "", cfg))
    added = _store(batch, cfg)
    db.finish_run(run_id, res["matched"], added, "")
    db.set_setting("ats_last", json.dumps({"at": datetime.now(timezone.utc).isoformat(timespec="seconds"),
                                           "boards": res["boards"], "matched": res["matched"],
                                           "stored": len(batch), "new": added, "errors": res["errors"]}))
    log.info("career-site pass: %d sites, %d matched, %d stored, %d new", res["boards"], res["matched"], len(batch), added)
    return {"boards": res["boards"], "matched": res["matched"], "stored": len(batch), "new": added}


def run_cycle() -> dict:
    """One full scrape pass. Returns {'scraped': n, 'new': n, 'errors': [...]}."""
    cfg = load_config()
    run_id = db.start_run()
    sites = [s.lower() for s in cfg.get("sites") or ["indeed"]]
    searches = build_searches(cfg)
    scraped, kept, new, errors = 0, 0, 0, []
    dropped: dict[str, int] = {}
    li_pass = bool(cfg.get("linkedin_easy_apply_pass", True)) and "linkedin" in sites
    li_proxies = next((p for s, p in _proxy_groups(cfg, ["linkedin"]) if "linkedin" in s), None)
    n_proxy = len(load_proxies(cfg))
    log.info("cycle start: %d searches × %s, %d prox%s (%s)", len(searches), ",".join(sites),
             n_proxy, "y" if n_proxy == 1 else "ies",
             ",".join(cfg.get("proxy_sites") or ["all sites"]) if n_proxy else "direct")

    # Tier 1 first: employers' own ATS boards (fast, no blocking, best data)
    if (cfg.get("ats") or {}).get("enabled", True):
        from . import ats  # local import: ats depends on this module's helpers
        try:
            res = ats.run(cfg)
            batch = res["jobs"]
            for j in batch:
                j.setdefault("categories", classify(j.get("title") or "", cfg))
            added = _store(batch, cfg)
            scraped += res["matched"]
            kept += len(batch)
            new += added
            db.progress_run(run_id, scraped, new)
            db.set_setting("ats_last", json.dumps({"at": datetime.now(timezone.utc).isoformat(timespec="seconds"),
                                                   "boards": res["boards"], "matched": res["matched"],
                                                   "stored": len(batch), "new": added, "errors": res["errors"]}))
            log.info("  %-34s %3d boards  %3d matched, %2d stored, %2d new", "employer boards (ATS)",
                     res["boards"], res["matched"], len(batch), added)
        except Exception:
            log.exception("ats pass crashed")

    for s in searches:
        term, loc, country, remote = s["term"], s["name"], s["country"], s["remote"]
        country_default = country_code(country) or "US"
        calls = []
        if remote:
            # Indeed treats "Remote" as a location and ignores is_remote when hours_old is set;
            # LinkedIn/Glassdoor/Zip want the real location plus the remote flag.
            if "indeed" in sites:
                calls.append((["indeed"], "Remote", False))
            others = [x for x in sites if x != "indeed"]
            if others:
                calls.append((others, loc, True))
        else:
            calls.append((sites, loc, False))

        frames = []
        for call_sites, call_loc, flag in calls:
            for group_sites, group_proxies in _proxy_groups(cfg, call_sites):
                try:
                    df = _scrape(cfg, group_sites, term, call_loc, country, is_remote=flag,
                                 proxies=group_proxies)
                    if df is not None and not df.empty:
                        frames.append(df)
                except Exception as e:  # a blocked site shouldn't kill the whole cycle
                    log.warning("search '%s' @ '%s' (%s) failed: %s", term, call_loc, group_sites, e)
                    errors.append(f"{term} @ {call_loc}: {e}")
        # LinkedIn no longer exposes the apply URL on guest job pages, so ask it
        # directly: a cheap card-only pass with its Easy Apply filter on. Those
        # cards are real matches too, so they join the results (the main pass's
        # richer row wins when both saw the same job).
        li_easy: set[str] = set()
        li_exhaustive = False
        if li_pass and any((f["site"] == "linkedin").any() for f in frames):
            try:
                edf = _scrape(cfg, ["linkedin"], term, loc, country, is_remote=remote,
                              easy_apply=True, fetch_desc=False, proxies=li_proxies)
                if edf is not None and not edf.empty:
                    li_easy = set(edf["job_url"].dropna())
                    frames.append(edf)
                # fewer than requested → we saw every Easy Apply role in the window,
                # so anything not in the set is genuinely an external application
                li_exhaustive = len(li_easy) < cfg.get("results_per_site", 20)
            except Exception as e:
                log.warning("linkedin easy-apply pass '%s' @ '%s' failed: %s", term, loc, e)
        if not frames:
            continue
        df = pd.concat([f.dropna(axis=1, how="all") for f in frames], ignore_index=True)
        df = df.drop_duplicates(subset="job_url", keep="first")
        scraped += len(df)

        batch = []
        for _, row in df.iterrows():
            job = _row_to_job(row, term, country_default, cfg)
            if job["site"] == "linkedin" and li_pass:
                if job["url"] in li_easy:
                    job["easy_apply"] = True
                elif li_exhaustive:
                    job["easy_apply"] = False
            if not job["title"]:
                continue
            ok, why = _passes_filters(job, cfg)
            if ok:
                batch.append(job)
            else:
                dropped[why] = dropped.get(why, 0) + 1
        added = _store(batch, cfg)
        kept += len(batch)
        new += added
        db.progress_run(run_id, scraped, new)
        log.info("  %-34s @ %-14s %3d scraped, %2d kept, %2d new", term, loc, len(df), len(batch), added)

    db.finish_run(run_id, scraped, new, "; ".join(errors)[:2000])
    for uid in db.users_with_resume():
        try:
            matching.refresh(uid)       # full rescore only when the corpus has drifted enough to move IDF
        except Exception:
            log.exception("rescore after sweep failed for user %s", uid)
    try:                                # queue anything that now meets each user's apply rules
        from . import apply
        for u in db.list_users():
            rules = apply.get_rules(u["id"])
            if rules.get("enabled"):
                q = apply.scan(u["id"], rules, int(cfg.get("stale_after_sweeps", 3)))
                log.info("apply rules (%s): %d new roles queued", u["name"], q["queued"])
    except Exception:
        log.exception("apply scan failed")
    log.info("cycle done: %d scraped, %d kept, %d new, %d errors, dropped=%s",
             scraped, kept, new, len(errors), dropped)
    return {"scraped": scraped, "kept": kept, "new": new, "errors": errors, "dropped": dropped}
