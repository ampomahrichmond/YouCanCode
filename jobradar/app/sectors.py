"""Sector tagging: which part of the economy an employer belongs to.

Categories (config `categories`) describe the *job function* (HR, analytics,
governance…); sectors describe the *employer* (international organizations,
consulting, government contractors, finance…). Every stored role carries one
sector key so the dashboard can filter by it.

Resolution order, first hit wins:
  1. the board's own sector (an `organizations` entry in config.yaml that owns
     the career site the role came from);
  2. the employer name matches an organization or alias in `organizations`;
  3. the board's industry label (LinkedIn / Indeed) matches a sector's
     `industries` keywords;
  4. the employer name contains one of a sector's `companies` keywords
     (whole-word, sectors checked in config order);
  5. "other".
"""
from __future__ import annotations

import json
import re

FALLBACK = {"other": {"label": "Other", "companies": [], "industries": []}}


def _exact(keyword: str) -> bool:
    """A keyword written in CAPITALS in config ('BIS', 'IRC', 'TD', 'PATH', 'CHAI')
    is an acronym and must be the employer's whole name: 'Synergy BIS' is not
    the Bank for International Settlements, 'Path 2 Potential' is not PATH.
    Lower-case words ('oil', 'bank', 'consulting') match anywhere in the name."""
    k = keyword.strip()
    return len(k) <= 6 and k == k.upper() and any(ch.isalpha() for ch in k)


def _alt(keyword: str) -> str:
    k = re.escape(norm(keyword))
    return rf"^{k}$" if _exact(keyword) else rf"(?<![a-z0-9]){k}(?![a-z0-9])"


def _word_rx(keywords: list[str]) -> re.Pattern | None:
    """One alternation for a keyword list (original spellings), longest first."""
    kws = sorted({k.strip() for k in keywords if k and norm(k)}, key=lambda k: -len(norm(k)))
    return re.compile("|".join(_alt(k) for k in kws), re.I) if kws else None


def _org_rx(index: list[tuple[str, str]]) -> tuple[re.Pattern | None, dict[str, str]]:
    """All organization aliases in one regex, longest first across sectors, so
    'Bain Capital' (finance) wins over 'Bain' (consulting). Matched text → sector."""
    seen: dict[str, str] = {}
    order = []
    for alias, sec in index:                      # index is already longest-first
        n = norm(alias)
        if n and n not in seen:
            seen[n] = sec
            order.append(alias)
    if not order:
        return None, {}
    return re.compile("|".join(_alt(a) for a in order), re.I), seen


def _industry_hit(label: str, keywords: list[str]) -> bool:
    """`=foo` must equal the whole label; anything else is a substring."""
    for k in keywords:
        if k.startswith("="):
            if label == k[1:]:
                return True
        elif k in label:
            return True
    return False


def norm(name: str | None) -> str:
    """Lower-case, punctuation squashed, so 'J.P. Morgan, Inc.' ~ 'jp morgan inc'."""
    s = (name or "").lower()
    s = s.replace("&amp;", "&").replace("’", "'").replace("'", "")
    s = re.sub(r"[.,()\[\]/\\|:;\"]+", " ", s)
    return re.sub(r"\s+", " ", s).strip()


def normalise_config(cfg: dict) -> dict:
    """Fill cfg['sectors'] (ordered {key: {label, companies, industries}}),
    cfg['organizations'] (list of dicts), cfg['_org_index'] ([(alias_norm,
    sector)]) and the compiled matchers. Returns cfg."""
    raw = cfg.get("sectors") or {}
    sectors: dict[str, dict] = {}
    for key, s in raw.items():
        k = re.sub(r"[^a-z0-9_-]", "", str(key).lower())
        if not k:
            continue
        s = s if isinstance(s, dict) else {}
        sectors[k] = {"label": s.get("label") or k.title(),
                      "companies": [str(x).strip() for x in (s.get("companies") or []) if x],   # original spelling
                      "industries": [str(x).lower().strip() for x in (s.get("industries") or []) if x]}
    if not sectors:
        sectors = dict(FALLBACK)
    sectors.setdefault("other", {"label": "Other", "companies": [], "industries": []})
    cfg["sectors"] = sectors

    orgs = []
    for o in cfg.get("organizations") or []:
        if isinstance(o, str):
            o = {"name": o}
        if not isinstance(o, dict) or not o.get("name"):
            continue
        sec = str(o.get("sector") or "other").lower()
        orgs.append({**o, "name": str(o["name"]).strip(), "sector": sec if sec in sectors else "other",
                     "aliases": [str(a) for a in (o.get("aliases") or []) if a]})
    cfg["organizations"] = orgs
    index = []
    for o in orgs:
        for alias in [o["name"], *o["aliases"]]:
            if norm(alias):
                index.append((alias.strip(), o["sector"]))
    index.sort(key=lambda t: -len(norm(t[0])))
    cfg["_org_index"] = index
    cfg["_matchers"] = {
        "orgs": _org_rx(index),
        "companies": [(sec, _word_rx(s["companies"])) for sec, s in sectors.items() if s["companies"]],
        "industries": [(sec, s["industries"]) for sec, s in sectors.items() if s["industries"]],
    }
    return cfg


def classify(company: str | None, industry: str | None, cfg: dict, hint: str | None = None) -> str:
    sectors = cfg.get("sectors") or FALLBACK
    if hint and hint in sectors:
        return hint
    m = cfg.get("_matchers")
    if m is None:
        normalise_config(cfg)
        m = cfg["_matchers"]
    cn = norm(company)
    if cn:
        rx, lookup = m["orgs"]
        hit = rx.search(cn) if rx else None
        if hit:
            sec = lookup.get(norm(hit.group(0)))
            if sec:
                return sec
    ind = (industry or "").lower().strip()
    if ind:
        for sec, kws in m["industries"]:
            if _industry_hit(ind, kws):
                return sec
    if cn:
        for sec, rx in m["companies"]:
            if rx and rx.search(cn):
                return sec
    return "other"


def signature(cfg: dict) -> str:
    """Changes whenever the tagging rules change, so stored rows get re-tagged."""
    return json.dumps({"v": 2, "s": {k: [v["companies"], v["industries"]] for k, v in (cfg.get("sectors") or {}).items()},
                       "o": cfg.get("_org_index") or []}, sort_keys=True)
