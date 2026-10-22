"""Tier-1 sources: employers' and organizations' own career sites.

Every provider here exposes a public feed per organization that never blocks:

  Greenhouse, Lever, Ashby, SmartRecruiters, Workable, BambooHR, Workday
      JSON feeds (listed, or keyword-searched for Workday);
  Oracle Cloud HCM   keyword search over the public REST API (UNDP and many
      corporates); descriptions when the tenant allows it;
  SuccessFactors     the career site's RSS feed per keyword (IDB, EY, ILO, EBRD,
      SAP…) — the feed carries the full posting even where the HTML pages sit
      behind a bot wall;
  Cornerstone        the career-site search API, using the anonymous token the
      site's own page embeds (World Bank Group);
  UN Careers         the UN Secretariat's job-opening API (full postings);
  Taleo              the career-section REST search (WHO, many agencies);
      list only — Taleo renders descriptions client-side;
  ReliefWeb          the humanitarian/development jobs aggregator (UN agencies,
      NGOs, contractors); needs a registered `appname` in config.

Boards are discovered automatically from the "apply at employer" links the big
boards already gave us, seeded from the `organizations` catalogue in
config.yaml (which also fixes each one's sector), or added by hand from any
careers URL. Each sweep: list every board's postings, keep the titles that
pass the config filters, fetch descriptions only for postings we haven't
stored yet, and hand the rows to the normal upsert."""
from __future__ import annotations

import html
import json
import logging
import os
import re
import time
from concurrent.futures import ThreadPoolExecutor, as_completed
from datetime import datetime, timedelta, timezone
from email.utils import parsedate_to_datetime
from urllib.parse import quote, urlparse

import requests

from . import db
from .scraper import COUNTRIES, _annualise, _passes_filters, country_code, country_ok, infer_country, infer_level, infer_travel

log = logging.getLogger("jobradar.ats")

UA = "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/128.0 Safari/537.36"
PROVIDERS = ("greenhouse", "lever", "ashby", "smartrecruiters", "workable", "bamboohr", "workday",
             "oracle", "successfactors", "cornerstone", "uncareers", "taleo", "reliefweb")
LABELS = {"greenhouse": "Greenhouse", "lever": "Lever", "ashby": "Ashby", "smartrecruiters": "SmartRecruiters",
          "workable": "Workable", "bamboohr": "BambooHR", "workday": "Workday", "oracle": "Oracle Cloud",
          "successfactors": "SuccessFactors", "cornerstone": "Cornerstone", "uncareers": "UN Careers",
          "taleo": "Taleo", "reliefweb": "ReliefWeb"}
# providers whose feed is searched by keyword rather than listed in full
SEARCHED = {"workday", "oracle", "successfactors", "cornerstone", "taleo", "reliefweb"}
# providers that cannot hand over a description (the role still gets title, location, date, link)
LIST_ONLY = {"cornerstone", "taleo"}
DEFAULT_TERMS = ["data governance", "data management", "data quality", "data analytics", "human resources"]
DISCOVERY_VERSION = 2       # bump when detect() learns a new provider, so stored links are rescanned once
TIMEOUT = 20
HTML_ACCEPT = "text/html,application/xhtml+xml,application/xml;q=0.9,*/*;q=0.8"


def _session() -> requests.Session:
    s = requests.Session()
    s.headers.update({"User-Agent": UA, "Accept": "application/json, text/plain, */*", "Accept-Language": "en-US,en;q=0.9"})
    return s


# ── URL → (provider, slug) ────────────────────────────────────────────────
_RX = {
    "greenhouse": re.compile(r"(?:job-boards|boards)\.greenhouse\.io/(?:embed/job_board\?for=)?([A-Za-z0-9_-]+)"),
    "lever": re.compile(r"jobs\.lever\.co/([A-Za-z0-9_-]+)"),
    "ashby": re.compile(r"jobs\.ashbyhq\.com/([A-Za-z0-9_.-]+)"),
    "smartrecruiters": re.compile(r"(?:jobs|careers)\.smartrecruiters\.com/([A-Za-z0-9_-]+)"),
    "workable": re.compile(r"apply\.workable\.com/(?!j/|api/)([A-Za-z0-9_-]+)/"),
    "bamboohr": re.compile(r"https?://([A-Za-z0-9-]+)\.bamboohr\.com/careers"),
    "uncareers": re.compile(r"https?://(careers\.un\.org)"),
}
_RX_WD_JOBS = re.compile(r"https?://([a-z0-9-]+\.wd\d+\.myworkdayjobs\.com)/(?:[a-z]{2}-[A-Z]{2}/)?([A-Za-z0-9_-]+)")
_RX_WD_SITE = re.compile(r"https?://((?:[a-z0-9-]+\.)?wd\d+\.myworkdaysite\.com)/recruiting/([A-Za-z0-9_-]+)/([A-Za-z0-9_-]+)")
_RX_ORACLE = re.compile(r"https?://([a-z0-9.-]+\.oraclecloud\.com)/hcmUI/CandidateExperience/[a-z-]+/sites/([A-Za-z0-9_-]+)")
_RX_CSOD = re.compile(r"https?://([a-z0-9-]+)\.csod\.com/ux/ats/careersite/(\d+)")
_RX_TALEO = re.compile(r"https?://([a-z0-9.-]+)/careersection/([A-Za-z0-9_+-]+)/(?:jobsearch|jobdetail|moresearch|joblist)\.ftl")
_RX_SF = re.compile(r"https?://([a-z0-9.-]+)/job/[^/?#]+/\d{6,}/?(?:[?#]|$)")       # SuccessFactors job pages
_RX_SF_RSS = re.compile(r"https?://([a-z0-9.-]+)/services/rss/job/")
_RX_SHORT = re.compile(r"https?://(?:grnh\.se/[A-Za-z0-9]+|apply\.workable\.com/j/[A-Za-z0-9]+)")
_WD_RESERVED = {"wday", "job", "jobs", "login", "en-US", "fr-CA"}


def detect(url: str | None) -> tuple[str, str] | None:
    """Return (provider, slug) for a URL on a supported ATS, else None.
    Workday slugs look like host/tenant/site, Oracle host/site, Cornerstone
    corp/siteId, Taleo host/careersection, SuccessFactors just the host."""
    if not url:
        return None
    for prov, rx in _RX.items():
        m = rx.search(url)
        if m:
            return prov, m.group(1)
    m = _RX_WD_JOBS.search(url)
    if m and m.group(2) not in _WD_RESERVED:
        host, site = m.group(1), m.group(2)
        return "workday", f"{host}/{host.split('.')[0]}/{site}"
    m = _RX_WD_SITE.search(url)
    if m:
        return "workday", f"{m.group(1)}/{m.group(2)}/{m.group(3)}"
    m = _RX_ORACLE.search(url)
    if m:
        return "oracle", f"{m.group(1)}/{m.group(2)}"
    m = _RX_CSOD.search(url)
    if m:
        return "cornerstone", f"{m.group(1)}/{m.group(2)}"
    m = _RX_TALEO.search(url)
    if m:
        return "taleo", f"{m.group(1)}/{m.group(2)}"
    m = _RX_SF.search(url) or _RX_SF_RSS.search(url)
    if m:
        return "successfactors", m.group(1)
    return None


def probe(url: str, s: requests.Session | None = None) -> tuple[str, str] | None:
    """For a careers URL `detect` doesn't recognise: ask the host whether it is a
    SuccessFactors career site (they all serve an RSS feed). One request."""
    host = urlparse(url if "://" in url else "https://" + url).netloc.lower()
    if not host:
        return None
    try:
        r = (s or _session()).get(f"https://{host}/services/rss/job/?locale=en_US", timeout=TIMEOUT,
                                  headers={"Accept": "application/rss+xml, application/xml, text/xml, */*"})
        if r.ok and "<rss" in r.text[:500]:
            return "successfactors", host
    except requests.RequestException:
        pass
    return None


def is_short(url: str | None) -> bool:
    return bool(url and _RX_SHORT.match(url))


def resolve_short(url: str, s: requests.Session | None = None) -> str | None:
    try:
        r = (s or _session()).get(url, allow_redirects=True, timeout=TIMEOUT)
        return r.url if r.ok else None
    except requests.RequestException:
        return None


def discover(cfg: dict, full: bool = False) -> dict:
    """Register boards found in stored employer links. Short links (grnh.se,
    workable /j/) need one request each to resolve; capped per call."""
    acfg = cfg.get("ats") or {}
    cap = int(acfg.get("resolve_per_sweep", 25))
    added, resolved = 0, 0
    s = _session()
    # only links stored since the last scan; a full rescan happens when the
    # provider list changes (bump DISCOVERY_VERSION) or on demand from the UI
    marker = db.get_setting("ats_discover_at")
    since = None if full else (marker.split("|", 1)[1] if marker and marker.startswith(f"{DISCOVERY_VERSION}|") else None)
    started = datetime.now(timezone.utc).isoformat()
    for url, company in db.jobs_urls_for_discovery(since):
        hit = detect(url)
        if not hit and is_short(url) and resolved < cap and not db.ats_seen(url):
            target = resolve_short(url, s)
            db.ats_mark_seen(url)
            resolved += 1
            hit = detect(target)
        if hit:
            prov, slug = hit
            if db.ats_add(prov, slug, company, "auto"):
                added += 1
    db.set_setting("ats_discover_at", f"{DISCOVERY_VERSION}|{started}")
    if added or resolved:
        log.info("ats discovery: %d boards added, %d short links resolved", added, resolved)
    return {"added": added, "resolved": resolved}


def register_organizations(cfg: dict) -> int:
    """Seed the boards named in config `organizations` (and `ats.boards`), each
    carrying its sector. Entries without a career-site URL only feed the
    sector classifier. Returns the number of new boards."""
    added = 0
    for org in cfg.get("organizations") or []:
        prov, slug = org.get("provider"), org.get("slug")
        if org.get("url") and not (prov and slug):
            hit = detect(org["url"])
            if hit:
                prov, slug = hit
        if prov in PROVIDERS and slug:
            if db.ats_add(prov, str(slug), org["name"], "config", org.get("sector")):
                added += 1
    acfg = cfg.get("ats") or {}
    for b in acfg.get("boards") or []:              # hand-written boards
        if isinstance(b, dict) and b.get("provider") in PROVIDERS and b.get("slug"):
            added += db.ats_add(b["provider"], b["slug"], b.get("company"), "config", b.get("sector"))
        elif isinstance(b, str):
            hit = detect(b)
            if hit:
                added += db.ats_add(hit[0], hit[1], None, "config")
    rw = cfg.get("reliefweb") or {}
    if reliefweb_appname(cfg):
        added += db.ats_add("reliefweb", "jobs", "ReliefWeb", "config", rw.get("sector") or "international")
    return added


def reliefweb_appname(cfg: dict) -> str:
    return ((cfg.get("reliefweb") or {}).get("appname") or os.environ.get("JOBRADAR_RELIEFWEB_APPNAME", "")).strip()


# ── helpers ───────────────────────────────────────────────────────────────
_TAG_RX = re.compile(r"<[^>]+>")


def clean_html(s: str | None) -> str:
    if not s:
        return ""
    t = html.unescape(s)
    if "<" in t and ">" in t:
        t = re.sub(r"(?i)</?(p|div|br|li|h\d|tr|ul|ol|table)[^>]*>", "\n", t)
        t = _TAG_RX.sub(" ", t)
        t = html.unescape(t)
    t = re.sub(r"[ \t\xa0 ]+", " ", t)
    t = re.sub(r"\n\s*\n+", "\n\n", t)
    return t.strip()


def _iso_date(v) -> str:
    if not v:
        return ""
    if isinstance(v, (int, float)):
        ts = v / 1000 if v > 1e11 else v
        return datetime.fromtimestamp(ts, tz=timezone.utc).strftime("%Y-%m-%d")
    return str(v)[:10]


def _us_date(v) -> str:
    """'10/1/2026' → '2026-10-01'."""
    m = re.match(r"\s*(\d{1,2})/(\d{1,2})/(\d{4})", str(v or ""))
    return f"{m.group(3)}-{int(m.group(1)):02d}-{int(m.group(2)):02d}" if m else _iso_date(v)


def _rfc_date(v) -> str:
    try:
        return parsedate_to_datetime(v).strftime("%Y-%m-%d")
    except Exception:
        return ""


def _posted_on_days(text: str | None) -> int | None:
    t = (text or "").lower()
    if "today" in t:
        return 0
    if "yesterday" in t:
        return 1
    m = re.search(r"(\d+)\+?\s*day", t)
    return int(m.group(1)) + (1 if "+" in t else 0) if m else None


def _too_old(date_posted: str, max_age: int) -> bool:
    if not date_posted:
        return False
    try:
        d = datetime.strptime(date_posted[:10], "%Y-%m-%d").replace(tzinfo=timezone.utc)
    except ValueError:
        return False
    return (datetime.now(timezone.utc) - d).days > max_age


# UN duty stations: the API gives a city only. Everything not listed here is abroad.
_UN_STATIONS = {"NEW YORK": "US", "WASHINGTON": "US", "WASHINGTON DC": "US", "WASHINGTON D.C.": "US",
                "MONTREAL": "CA", "OTTAWA": "CA", "TORONTO": "CA", "VANCOUVER": "CA", "DUBAI": "AE", "ABU DHABI": "AE",
                "LONDON": "GB", "GENEVA": "CH", "PARIS": "FR", "BONN": "DE", "THE HAGUE": "NL", "DOHA": "QA"}


def _cc(code: str | None) -> str | None:
    """ISO-2/3 country code → a supported code / Other / None."""
    return country_code(code)


def _country_name(name: str | None) -> str | None:
    return country_code(name)


_SMALL = {"of", "and", "for", "the", "in", "to", "on", "at", "a", "an", "&"}
_ACRONYMS = {"ict", "it", "hr", "un", "gis", "m&e", "sdg", "erp", "ai", "ml", "bi", "hq", "crm", "ocha", "undp",
             "unicef", "who", "wfp", "dpo", "dpko", "dos", "osaa", "unep", "unodc", "escap", "eca", "eclac", "escwa",
             "unctad", "oios", "ola", "ohchr", "unhcr", "unfpa", "unrwa", "icty", "irmct", "nob", "noc", "nod", "noa"}


def _titlecase(t: str) -> str:
    """UN posts titles in capitals; 'ASSOCIATE DATA SPECIALIST, ICT' → 'Associate Data Specialist, ICT'."""
    if not t or t != t.upper():
        return t
    out = []
    for i, w in enumerate(re.split(r"(\s+)", t.strip())):
        if not w.strip():
            out.append(w)
            continue
        core = w.strip(",;:()")
        lw = core.lower()
        if lw in _ACRONYMS:
            out.append(w)
        elif i and lw in _SMALL:
            out.append(w.lower())
        else:
            out.append(w[:1] + w[1:].lower())
    return "".join(out)


def _salary_from_text(desc: str) -> tuple[str | None, float | None, float | None, str | None]:
    """Reuse jobspy's description salary parser (interval, min, max, currency)."""
    try:
        from jobspy.util import extract_salary
        iv, mn, mx, cur = extract_salary(desc, enforce_annual_salary=False)
        return iv, mn, mx, cur
    except Exception:
        return None, None, None, None


def _finish(job: dict, cfg: dict) -> dict:
    """Fill the derived fields the scraper normally computes."""
    desc = clean_html(job.pop("description_html", None)) or job.get("description") or ""
    job["description"] = desc[:4000]
    job["snippet"] = desc[:400]
    if not job.get("salary_max") and not job.get("salary_min") and desc:
        iv, mn, mx, cur = _salary_from_text(desc)
        if mx or mn:
            job.update(salary_interval=iv, salary_min=mn, salary_max=mx, currency=cur or "USD",
                       salary_source="description")
    job["salary_annual"] = _annualise(job.get("salary_min"), job.get("salary_max"), job.get("salary_interval"))
    job["level"] = infer_level(job.get("title") or "", job.get("job_level"))
    job["travel"] = infer_travel(desc)
    if job.get("country") not in COUNTRIES:            # the provider's own country field wins
        job["country"] = infer_country(job.get("location"), job.get("country"))
    if not job.get("is_remote") and re.search(r"\b(remote|home[- ]based)\b", (job.get("location") or "").lower()):
        job["is_remote"] = True
    job["easy_apply"] = 0                      # you apply on the employer's own site
    job.setdefault("url_direct", job.get("url"))
    return job


# ── per-provider fetchers: list() → [raw], detail(raw) → description html ──
class _Board:
    def __init__(self, provider: str, slug: str, company: str | None, s: requests.Session, cfg: dict):
        self.provider, self.slug, self.company, self.s, self.cfg = provider, slug, company, s, cfg
        self.acfg = cfg.get("ats") or {}
        self.max_age = int(self.acfg.get("max_age_days", 21))
        self.terms = self.acfg.get("search_terms") or self.acfg.get("workday_terms") or DEFAULT_TERMS

    def _req(self, method, url, **kw):
        """One retry on a dropped connection (some tenants reset between keyword searches)."""
        for attempt in (1, 2):
            try:
                r = self.s.request(method, url, timeout=TIMEOUT, **kw)
                r.raise_for_status()
                return r
            except (requests.ConnectionError, requests.Timeout):
                if attempt == 2:
                    raise
                time.sleep(1.5)

    def _get(self, url, **kw):
        return self._req("GET", url, **kw).json()

    def _post(self, url, body, headers=None, **kw):
        return self._req("POST", url, json=body, headers={"Content-Type": "application/json", **(headers or {})}, **kw).json()

    def _text(self, url, accept=HTML_ACCEPT, **kw):
        return self._req("GET", url, headers={"Accept": accept}, **kw).text

    # each list() yields dicts with: title, location, url, url_direct, date_posted, is_remote,
    # optional salary_*, country, job_level, company (per-row override), and either
    # description_html / description or a `_detail` callable
    def list(self) -> list[dict]:
        return getattr(self, f"_list_{self.provider}")()

    def _list_greenhouse(self):
        data = self._get(f"https://boards-api.greenhouse.io/v1/boards/{self.slug}/jobs?content=true")
        out = []
        for j in data.get("jobs", []):
            self.company = self.company or (j.get("company_name") or "").strip() or self.slug
            loc = (j.get("location") or {}).get("name") or ""
            out.append({"title": (j.get("title") or "").strip(), "location": loc, "url": j.get("absolute_url"),
                        "date_posted": _iso_date(j.get("first_published") or j.get("updated_at")),
                        "is_remote": "remote" in loc.lower(), "description_html": j.get("content")})
        return out

    def _list_lever(self):
        data = self._get(f"https://api.lever.co/v0/postings/{self.slug}?mode=json")
        out = []
        for j in data:
            cat = j.get("categories") or {}
            sr = j.get("salaryRange") or {}
            desc = "\n\n".join(filter(None, [j.get("openingPlain"), j.get("descriptionPlain")]
                                      + [f"{l.get('text','')}\n{clean_html(l.get('content'))}" for l in j.get("lists") or []]
                                      + [j.get("additionalPlain")]))
            iv = {"per-year-salary": "yearly", "per-month-salary": "monthly", "per-hour-wage": "hourly"}.get(sr.get("interval"))
            out.append({"title": (j.get("text") or "").strip(), "location": cat.get("location") or "",
                        "url": j.get("hostedUrl"), "url_direct": j.get("applyUrl") or j.get("hostedUrl"),
                        "date_posted": _iso_date(j.get("createdAt")), "job_type": (cat.get("commitment") or "").lower() or None,
                        "is_remote": (j.get("workplaceType") or "").lower() == "remote",
                        "salary_min": sr.get("min"), "salary_max": sr.get("max"), "salary_interval": iv,
                        "currency": sr.get("currency"), "salary_source": "direct_data" if sr.get("max") else None,
                        "country": {"US": "US", "CA": "CA"}.get(j.get("country")), "description": desc})
        return out

    def _list_ashby(self):
        data = self._get(f"https://api.ashbyhq.com/posting-api/job-board/{self.slug}?includeCompensation=true")
        out = []
        for j in data.get("jobs", []):
            if j.get("isListed") is False:
                continue
            comp = j.get("compensation") or {}
            mn = mx = cur = None
            for c in comp.get("summaryComponents") or []:
                if (c.get("compensationType") or "").lower() == "salary":
                    mn, mx, cur = c.get("minValue"), c.get("maxValue"), c.get("currencyCode")
            addr = ((j.get("address") or {}).get("postalAddress") or {})
            loc = j.get("location") or ""
            out.append({"title": (j.get("title") or "").strip(), "location": loc, "url": j.get("jobUrl"),
                        "url_direct": j.get("applyUrl") or j.get("jobUrl"), "date_posted": _iso_date(j.get("publishedAt")),
                        "is_remote": bool(j.get("isRemote")) or (j.get("workplaceType") or "").lower() == "remote",
                        "job_type": (j.get("employmentType") or "").lower() or None,
                        "salary_min": mn, "salary_max": mx, "currency": cur, "salary_interval": "yearly" if mx else None,
                        "salary_source": "direct_data" if mx else None,
                        "country": {"USA": "US", "US": "US", "United States": "US", "Canada": "CA", "CA": "CA"}.get(addr.get("addressCountry")),
                        "description": j.get("descriptionPlain") or clean_html(j.get("descriptionHtml"))})
        return out

    def _list_smartrecruiters(self):
        out, offset = [], 0
        while offset < 500:
            data = self._get(f"https://api.smartrecruiters.com/v1/companies/{self.slug}/postings?limit=100&offset={offset}")
            items = data.get("content") or []
            for j in items:
                self.company = self.company or (j.get("company") or {}).get("name") or self.slug
                loc = j.get("location") or {}
                city = ", ".join(filter(None, [loc.get("city"), loc.get("region")]))
                cc = (loc.get("country") or "").upper()
                ref = j.get("ref")
                out.append({"title": (j.get("name") or "").strip(), "location": city or cc,
                            "url": f"https://jobs.smartrecruiters.com/{self.slug}/{j.get('id')}",
                            "date_posted": _iso_date(j.get("releasedDate")), "is_remote": bool(loc.get("remote")),
                            "country": {"US": "US", "CA": "CA"}.get(cc) or ("Other" if cc else None),
                            "job_level": ((j.get("experienceLevel") or {}).get("label") or "").lower() or None,
                            "_detail": (lambda ref=ref: self._detail_smartrecruiters(ref))})
            offset += 100
            if len(items) < 100 or offset >= int(data.get("totalFound") or 0):
                break
        return out

    def _detail_smartrecruiters(self, ref):
        d = self._get(ref)
        secs = (d.get("jobAd") or {}).get("sections") or {}
        parts = [secs.get(k, {}).get("text") for k in ("companyDescription", "jobDescription", "qualifications", "additionalInformation")]
        return {"description_html": "\n\n".join(p for p in parts if p), "url_direct": d.get("applyUrl") or d.get("postingUrl")}

    def _list_workable(self):
        out, token, pages = [], None, 0
        while pages < 10:
            body = {"query": "", "location": [], "department": [], "worktype": [], "remote": []}
            if token:
                body["token"] = token
            data = self._post(f"https://apply.workable.com/api/v3/accounts/{self.slug}/jobs", body)
            for j in data.get("results") or []:
                loc = j.get("location") or {}
                city = ", ".join(filter(None, [loc.get("city"), loc.get("region")]))
                code = j.get("shortcode")
                out.append({"title": (j.get("title") or "").strip(), "location": city or loc.get("country") or "",
                            "url": f"https://apply.workable.com/{self.slug}/j/{code}/", "date_posted": _iso_date(j.get("published")),
                            "is_remote": bool(j.get("remote")) or (j.get("workplace") or "") == "remote",
                            "job_type": {"full": "fulltime", "part": "parttime", "contract": "contract"}.get(j.get("type")),
                            "country": {"US": "US", "CA": "CA"}.get((loc.get("countryCode") or "").upper()) or ("Other" if loc.get("countryCode") else None),
                            "_detail": (lambda code=code: self._detail_workable(code))})
            token = data.get("nextPage")
            pages += 1
            if not token:
                break
        return out

    def _detail_workable(self, code):
        d = self._get(f"https://apply.workable.com/api/v2/accounts/{self.slug}/jobs/{code}")
        return {"description_html": "\n\n".join(filter(None, [d.get("description"), d.get("requirements"), d.get("benefits")]))}

    def _list_bamboohr(self):
        data = self._get(f"https://{self.slug}.bamboohr.com/careers/list")
        out = []
        for j in data.get("result") or []:
            loc = j.get("location") or {}
            city = ", ".join(filter(None, [loc.get("city"), loc.get("state")]))
            jid = j.get("id")
            out.append({"title": (j.get("jobOpeningName") or "").strip(), "location": city,
                        "url": f"https://{self.slug}.bamboohr.com/careers/{jid}", "date_posted": "",
                        "is_remote": bool(j.get("isRemote")) or str(j.get("locationType")) == "1",
                        "job_type": (j.get("employmentStatusLabel") or "").lower() or None,
                        "_detail": (lambda jid=jid: self._detail_bamboohr(jid))})
        return out

    def _detail_bamboohr(self, jid):
        d = (self._get(f"https://{self.slug}.bamboohr.com/careers/{jid}/detail").get("result") or {}).get("jobOpening") or {}
        return {"description_html": d.get("description"), "date_posted": _iso_date(d.get("datePosted")),
                "url_direct": d.get("jobOpeningShareUrl")}

    def _list_workday(self):
        host, tenant, site = self.slug.split("/", 2)
        api = f"https://{host}/wday/cxs/{tenant}/{site}"
        base = f"https://{host}/{site}" if "myworkdayjobs" in host else f"https://{host}/recruiting/{tenant}/{site}"
        out, seen = [], set()
        for term in self.terms:
            data = self._post(f"{api}/jobs", {"appliedFacets": {}, "limit": 20, "offset": 0, "searchText": term})
            for j in data.get("jobPostings") or []:
                path = j.get("externalPath")
                if not path or path in seen:
                    continue
                seen.add(path)
                age = _posted_on_days(j.get("postedOn"))
                if age is not None and age > self.max_age:
                    continue
                dp = (datetime.now(timezone.utc) - timedelta(days=age)).strftime("%Y-%m-%d") if age is not None else ""
                out.append({"title": (j.get("title") or "").strip(), "location": j.get("locationsText") or "",
                            "url": base + path, "date_posted": dp,
                            "is_remote": "remote" in (j.get("remoteType") or "").lower(),
                            "_detail": (lambda path=path: self._detail_workday(api, path))})
        return out

    def _detail_workday(self, api, path):
        d = self._get(f"{api}{path}").get("jobPostingInfo") or {}
        return {"description_html": d.get("jobDescription"), "url_direct": d.get("externalUrl"),
                "date_posted": _iso_date(d.get("startDate")), "location": d.get("location") or None,
                "country": country_code((d.get("country") or {}).get("descriptor") or "")}

    # Oracle Cloud HCM (Recruiting Cloud "CandidateExperience"): public REST, searched by keyword.
    def _list_oracle(self):
        host, site = self.slug.split("/", 1)
        out, seen = [], set()
        for term in self.terms:
            url = (f"https://{host}/hcmRestApi/resources/latest/recruitingCEJobRequisitions?onlyData=true"
                   f"&expand=requisitionList.secondaryLocations&finder=findReqs;siteNumber={site},limit=25,offset=0,"
                   f"sortBy=POSTING_DATES_DESC,keyword={quote(term)}")
            data = self._get(url, headers={"Accept": "application/json"})
            for j in ((data.get("items") or [{}])[0].get("requisitionList") or []):
                rid = j.get("Id")
                if not rid or rid in seen:
                    continue
                seen.add(rid)
                dp = _iso_date(j.get("PostedDate"))
                if _too_old(dp, self.max_age):
                    continue
                locs = [j.get("PrimaryLocation")] + [x.get("Name") for x in (j.get("secondaryLocations") or []) if x.get("Name")]
                wt = (j.get("WorkplaceType") or "").lower()
                out.append({"title": (j.get("Title") or "").strip(), "location": "; ".join(x for x in locs if x),
                            "url": f"https://{host}/hcmUI/CandidateExperience/en/sites/{site}/job/{rid}",
                            "date_posted": dp, "country": _cc(j.get("PrimaryLocationCountry")),
                            "is_remote": "remote" in wt, "job_type": (j.get("JobType") or "").lower() or None,
                            "job_function": j.get("JobFamily") or j.get("JobFunction"),
                            "description": clean_html(j.get("ShortDescriptionStr")),
                            "_detail": (lambda rid=rid: self._detail_oracle(host, site, rid))})
        return out

    def _detail_oracle(self, host, site, rid):
        # Not every tenant exposes the details resource; the row keeps its short description otherwise.
        url = (f"https://{host}/hcmRestApi/resources/latest/recruitingCEJobRequisitionDetails?expand=all&onlyData=true"
               f"&finder=ByRequisitionId;Id=%22{rid}%22,siteNumber={site}")
        d = (self._get(url, headers={"Accept": "application/json"}).get("items") or [{}])[0]
        parts = [d.get("ExternalDescriptionStr"), d.get("ExternalResponsibilitiesStr"), d.get("ExternalQualificationsStr")]
        return {"description_html": "\n\n".join(p for p in parts if p), "date_posted": _iso_date(d.get("ExternalPostedStartDate")),
                "country": _cc(d.get("PrimaryLocationCountry"))}

    # SuccessFactors Career Site Builder: every site serves an RSS feed per keyword search.
    _SF_LOC = re.compile(r"\s*\(([^()]*?),\s*([A-Z]{2})(?:,\s*[^()]*)?\)\s*$")

    def _list_successfactors(self):
        host = self.slug
        out, seen = [], set()
        for term in self.terms:
            xml = self._text(f"https://{host}/services/rss/job/?locale=en_US&keywords=({quote(term)})",
                             accept="application/rss+xml, application/xml, text/xml, */*")
            if "<rss" not in xml[:500]:
                raise ValueError("not an RSS feed")
            for item in re.findall(r"<item>(.*?)</item>", xml, re.S):
                link = html.unescape((re.search(r"<link>(.*?)</link>", item, re.S) or [None, ""])[1]).strip()
                link = link.split("?")[0]
                if not link or link in seen:
                    continue
                seen.add(link)
                title = _cdata(re.search(r"<title>(.*?)</title>", item, re.S))
                desc = _cdata(re.search(r"<description>(.*?)</description>", item, re.S))
                dp = _rfc_date((re.search(r"<pubDate>(.*?)</pubDate>", item, re.S) or [None, ""])[1])
                if _too_old(dp, self.max_age):
                    continue
                loc, cc = "", None
                m = self._SF_LOC.search(title)
                if m:
                    loc, cc = m.group(1).strip(), _cc(m.group(2))
                    title = title[:m.start()].strip()
                out.append({"title": title, "location": loc, "url": link, "date_posted": dp, "country": cc,
                            "is_remote": "remote" in loc.lower(), "description_html": desc})
        return out

    # Cornerstone OnDemand career sites: the page embeds an anonymous JWT the search API accepts.
    def _list_cornerstone(self):
        corp, site_id = self.slug.split("/", 1)
        base = f"https://{corp}.csod.com"
        home = f"{base}/ux/ats/careersite/{site_id}/home?c={corp}"
        page = self._text(home)
        m = re.search(r'"token":"([^"]+)"', page)
        if not m:
            raise ValueError("no career-site token")
        hdr = {"Authorization": f"Bearer {m.group(1)}", "Referer": home, "Origin": base, "Accept": "application/json"}
        out, seen = [], set()
        for term in self.terms:
            body = {"careerSiteId": int(site_id), "careerSitePageId": int(site_id), "pageNumber": 1, "pageSize": 50,
                    "cultureId": 1, "cultureName": "en-US", "searchText": term, "cities": [], "countryCodes": [],
                    "cultures": [], "customFieldCheckboxKeys": [], "customFieldDropdowns": [], "customFieldRadios": [],
                    "placeID": "", "radius": None, "postingsWithinDays": None}
            data = self._post(f"{base}/services/x/career-site/v1/search", body, headers=hdr)
            for j in (data.get("data") or {}).get("requisitions") or []:
                rid = j.get("requisitionId")
                if not rid or rid in seen:
                    continue
                seen.add(rid)
                dp = _us_date(j.get("postingEffectiveDate"))
                if _too_old(dp, self.max_age):
                    continue
                locs = j.get("locations") or [{}]
                loc = "; ".join(", ".join(filter(None, [l.get("city"), l.get("state")])) or (l.get("country") or "") for l in locs)
                out.append({"title": (j.get("displayJobTitle") or "").strip(), "location": loc, "date_posted": dp,
                            "url": f"{base}/ux/ats/careersite/{site_id}/home/requisition/{rid}?c={corp}",
                            "country": _cc((locs[0] or {}).get("country")), "is_remote": "remote" in loc.lower()})
        return out

    # UN Secretariat (careers.un.org): one JSON API, full postings, paged 100 at a time.
    def _list_uncareers(self):
        out, page, total = [], 0, None
        while page < 10:
            data = self._post("https://careers.un.org/api/public/opening/jo/list/filteredV2/en",
                              {"filterConfig": {"keyword": ""},
                               "pagination": {"page": page, "itemPerPage": 100, "sortBy": "startDate", "sortDirection": -1}})
            d = data.get("data") or {}
            items = d.get("list") or []
            total = int(d.get("count") or 0)
            for j in items:
                jid = j.get("jobId")
                if not jid:
                    continue
                stations = [s.get("description") or "" for s in (j.get("dutyStation") or [])]
                cc = None
                for st in stations:
                    cc = _UN_STATIONS.get(st.strip().upper(), "Other")
                    if cc in ("US", "CA"):
                        break
                dp = _iso_date(j.get("startDate"))
                if _too_old(dp, self.max_age):
                    continue
                jf = (j.get("jf") or {}).get("Name") or (j.get("jf") or {}).get("name")
                out.append({"title": _titlecase((j.get("jobTitle") or j.get("postingTitle") or "").strip()),
                            "location": "; ".join(s.title() for s in stations if s), "date_posted": dp, "country": cc,
                            "url": f"https://careers.un.org/jobSearchDescription/{jid}?language=en",
                            "job_level": j.get("jobLevel"), "job_function": jf,
                            "description_html": j.get("jobDescription")})
            page += 1
            if len(items) < 100 or page * 100 >= total:
                break
        return out

    # Taleo career sections: REST search works anonymously; descriptions render client-side (list only).
    _TALEO_FILTERS = [{"id": k, "selectedValues": []} for k in ("POSTING_DATE", "LOCATION", "JOB_FIELD", "JOB_TYPE", "JOB_SCHEDULE", "JOB_LEVEL")]
    _TALEO_ADV = [{"id": k, "selectedValues": []} for k in ("ORGANIZATION", "LOCATION", "JOB_FIELD", "URGENT_JOB", "EMPLOYEE_STATUS", "STUDY_LEVEL", "WILL_TRAVEL", "JOB_SHIFT", "JOB_NUMBER")]

    def _list_taleo(self):
        parts = self.slug.split("/")
        host, cs = parts[0], parts[1]
        portal = parts[2] if len(parts) > 2 else None
        search_page = f"https://{host}/careersection/{cs}/jobsearch.ftl?lang=en"
        if not portal:
            m = re.search(r"portal=(\d+)", self._text(search_page))
            if not m:
                raise ValueError("no Taleo portal id")
            portal = m.group(1)
        out, seen = [], set()
        for term in self.terms:
            body = {"multilineEnabled": False, "sortingSelection": {"sortBySelectionParam": "3", "ascendingSortingOrder": "false"},
                    "fieldData": {"fields": {"KEYWORD": term, "LOCATION": "", "JOB_TITLE": "", "JOB_NUMBER": "", "URGENT_JOB": "",
                                             "ORGANIZATION": "", "JOB_FIELD": "", "JOB_SCHEDULE": "", "POSTING_DATE": "", "JOB_TYPE": ""}, "valid": True},
                    "filterSelectionParam": {"searchFilterSelections": self._TALEO_FILTERS},
                    "advancedSearchFiltersSelectionParam": {"searchFilterSelections": self._TALEO_ADV}, "pageNo": 1}
            data = self._post(f"https://{host}/careersection/rest/jobboard/searchjobs?lang=en&portal={portal}", body,
                              headers={"tz": "GMT-05:00", "tzname": "America/New_York", "Referer": search_page})
            for j in data.get("requisitionList") or []:
                no = j.get("contestNo")
                cols = j.get("column") or []
                if not no or no in seen or not cols:
                    continue
                seen.add(no)
                title = (cols[0] or "").strip()
                loc, cc = "", None
                if len(cols) > 1:
                    try:
                        raw = json.loads(cols[1]) if str(cols[1]).startswith("[") else [cols[1]]
                    except ValueError:
                        raw = [cols[1]]
                    pieces = []
                    for r in raw:
                        country, _, city = str(r).partition("-")
                        pieces.append(", ".join(filter(None, [city.strip(), country.strip()])))
                        cc = cc if cc in ("US", "CA") else _country_name(country)
                    loc = "; ".join(pieces)
                dp = ""
                if len(cols) > 2:
                    raw_dt = re.sub(r"[ \xa0]", " ", str(cols[2]))
                    for fmt in ("%b %d, %Y, %I:%M:%S %p", "%b %d, %Y %I:%M:%S %p", "%b %d, %Y"):
                        try:
                            dp = datetime.strptime(raw_dt.strip(), fmt).strftime("%Y-%m-%d")
                            break
                        except ValueError:
                            continue
                if _too_old(dp, self.max_age):
                    continue
                out.append({"title": title, "location": loc, "date_posted": dp, "country": cc,
                            "url": f"https://{host}/careersection/{cs}/jobdetail.ftl?job={no}&lang=en",
                            "is_remote": "remote" in loc.lower()})
        return out

    # ReliefWeb jobs API v2 (UN agencies, NGOs, development contractors). Needs a registered appname.
    def _list_reliefweb(self):
        appname = reliefweb_appname(self.cfg)
        if not appname:
            raise ValueError("reliefweb.appname not set")
        iso3 = {"US": "usa", "CA": "can", "AE": "are", "GB": "gbr", "IE": "irl", "DE": "deu", "NL": "nld", "CH": "che",
                "FR": "fra", "SG": "sgp", "AU": "aus", "IL": "isr", "SA": "sau", "QA": "qat"}
        countries = [iso3[c.upper()] for c in (self.cfg.get("countries") or []) if c.upper() in iso3]
        out, seen = [], set()
        for term in self.terms:
            body = {"query": {"value": term, "operator": "AND"}, "limit": 50, "sort": ["date.created:desc"],
                    "fields": {"include": ["title", "url", "url_alias", "date.created", "date.closing", "source.name",
                                           "source.shortname", "country.name", "city.name", "career_categories.name",
                                           "experience.name", "type.name", "body-html"]}}
            if countries:
                body["filter"] = {"field": "country.iso3", "value": countries, "operator": "OR"}
            data = self._post(f"https://api.reliefweb.int/v2/jobs?appname={quote(appname)}", body)
            for item in data.get("data") or []:
                f = item.get("fields") or {}
                url = f.get("url_alias") or f.get("url")
                if not url or url in seen:
                    continue
                seen.add(url)
                dp = _iso_date((f.get("date") or {}).get("created"))
                if _too_old(dp, self.max_age):
                    continue
                src = (f.get("source") or [{}])[0]
                cname = ((f.get("country") or [{}])[0]).get("name") or ""
                city = ((f.get("city") or [{}])[0]).get("name") or ""
                types = [t.get("name") or "" for t in f.get("type") or []]
                out.append({"title": (f.get("title") or "").strip(), "company": src.get("name") or src.get("shortname") or "ReliefWeb",
                            "location": ", ".join(filter(None, [city, cname])), "url": url, "date_posted": dp,
                            "country": _country_name(cname), "is_remote": any("remote" in t.lower() for t in types),
                            "job_function": ", ".join(c.get("name") or "" for c in f.get("career_categories") or []) or None,
                            "job_level": ((f.get("experience") or [{}])[0]).get("name"),
                            "description_html": f.get("body-html")})
        return out


def _cdata(m) -> str:
    if not m:
        return ""
    t = m.group(1).strip()
    if t.startswith("<![CDATA["):
        t = t[9:]
        if t.endswith("]]>"):
            t = t[:-3]
        return t
    return html.unescape(t)


# ── one board ─────────────────────────────────────────────────────────────
def process_board(board: dict, cfg: dict) -> dict:
    """Returns {'jobs': [...new job dicts...], 'touched': [ids], 'matched': n, 'error': str|None}."""
    acfg = cfg.get("ats") or {}
    max_new = int(acfg.get("max_new_per_board", 12))
    s = _session()
    b = _Board(board["provider"], board["slug"], board.get("company"), s, cfg)
    try:
        raw = b.list()
    except requests.HTTPError as e:
        code = e.response.status_code if e.response is not None else "?"
        return {"jobs": [], "touched": [], "matched": 0, "error": f"HTTP {code}", "company": b.company}
    except (requests.RequestException, ValueError, KeyError, IndexError) as e:
        return {"jobs": [], "touched": [], "matched": 0, "error": type(e).__name__ if not isinstance(e, ValueError) else str(e)[:40] or "ValueError",
                "company": b.company}

    board_company = (b.company or board.get("company") or board["slug"]).strip()
    candidates = []                      # (job, raw, jid) that pass the title/country filters
    for r in raw:
        if not r.get("title"):
            continue
        job = {k: v for k, v in r.items() if k != "_detail"}
        company = (job.pop("company", None) or board_company).strip()   # aggregators carry the employer per row
        job.update(company=company, site=board["provider"], search_term=f"ats:{LABELS[board['provider']]}")
        if board.get("sector"):
            job["sector"] = board["sector"]
        ok, _ = _passes_filters({"title": job["title"], "description": ""}, cfg)
        if not ok:
            continue
        if job.get("country") not in COUNTRIES:
            job["country"] = infer_country(job.get("location"), job.get("country"))
        if not country_ok(job.get("country"), cfg):
            continue
        candidates.append((job, r, db.job_id(job["title"], company, job.get("location"))))
    existing = db.jobs_exist([jid for _, _, jid in candidates])   # one query, not one per row
    jobs, touched, matched = [], [], len(candidates)
    for job, r, jid in candidates:
        if jid in existing:
            touched.append(jid)
            continue
        if len(jobs) >= max_new:
            continue
        detail = r.get("_detail")
        if detail:
            try:
                extra = detail() or {}
                job.update({k: v for k, v in extra.items() if v})
            except Exception as e:  # keep the row; description just stays empty
                log.debug("detail fetch failed for %s: %s", job.get("url"), e)
        job = _finish(job, cfg)
        ok, _ = _passes_filters(job, cfg)          # now with the description (exclude_keywords etc.)
        if ok:
            jobs.append(job)
    return {"jobs": jobs, "touched": touched, "matched": matched, "error": None, "company": board_company}


def run(cfg: dict) -> dict:
    """Discover, seed the organization catalogue, then check every enabled board
    concurrently. Returns rows for upsert."""
    acfg = cfg.get("ats") or {}
    if acfg.get("auto_discover", True):
        discover(cfg)
    seeded = register_organizations(cfg)
    if seeded:
        log.info("organizations catalogue: %d new boards registered", seeded)
    boards = [b for b in db.ats_list(with_counts=False) if b["enabled"]]
    if not boards:
        return {"boards": 0, "jobs": [], "matched": 0, "errors": 0}
    workers = int(acfg.get("workers", 6))
    all_jobs, touched, matched, errors = [], [], 0, 0
    with ThreadPoolExecutor(max_workers=workers) as ex:
        futs = {ex.submit(process_board, b, cfg): b for b in boards}
        for fut in as_completed(futs):
            b = futs[fut]
            try:
                res = fut.result()
            except Exception as e:
                res = {"jobs": [], "touched": [], "matched": 0, "error": type(e).__name__, "company": b.get("company")}
            db.ats_update_result(b["id"], res["matched"], len(res["jobs"]), res["error"], res.get("company"))
            all_jobs += res["jobs"]
            touched += res["touched"]
            matched += res["matched"]
            errors += 1 if res["error"] else 0
    db.touch_jobs(touched)
    log.info("ats pass: %d boards, %d titles matched, %d new rows to store, %d board errors",
             len(boards), matched, len(all_jobs), errors)
    return {"boards": len(boards), "jobs": all_jobs, "matched": matched, "errors": errors}
