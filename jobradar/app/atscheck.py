"""ATS keyword targeting and coverage check, fully local.

Which phrases a screening system will look for is derived from the posting
itself: distinctive terms by TF-IDF against the whole job corpus, the exact
phrases the tailoring analysis flagged, and known hard-skill vocabulary.
Coverage then checks the draft resume for each phrase, whole-word and
case-insensitive, with a simple singular/plural tolerance."""
from __future__ import annotations

import json
import re
from collections import Counter

from . import db, matching

# Vocabulary an ATS for data / analytics / HR roles typically keys on.
_SKILL_VOCAB = """
data governance|data management|data quality|data lineage|data catalog|data cataloging|data stewardship|data steward|
metadata management|metadata|master data management|mdm|reference data|data architecture|data strategy|data modeling|
data warehouse|data lake|data platform|data privacy|privacy|gdpr|ccpa|hipaa|sox|bcbs 239|regulatory compliance|
compliance|risk management|records management|records retention|retention schedule|data retention|disposition|
information governance|information management|data classification|data security|access control|dama|dmbok|cdmp|
collibra|alation|informatica|talend|ataccama|purview|microsoft purview|unity catalog|snowflake|databricks|
azure|aws|gcp|google cloud|sql|python|r|tableau|power bi|looker|qlik|excel|sap|oracle|salesforce|workday|
data analytics|analytics|business intelligence|bi|reporting|dashboards|kpis|data science|machine learning|ai|
artificial intelligence|statistics|forecasting|etl|elt|data pipelines|data integration|api|agile|scrum|jira|
change management|stakeholder management|program management|project management|pmp|roadmap|policy|standards|
data literacy|training|team leadership|people management|budget|vendor management|
human resources|hr|hris|hr analytics|people analytics|workforce planning|talent acquisition|talent management|
employee relations|compensation|benefits|performance management|succession planning|onboarding|employee engagement|
hr business partner|hrbp|labor relations|dei|diversity|payroll|adp|ultipro|ukg|successfactors|ceridian
""".replace("\n", "").split("|")
_SKILL_VOCAB = [s.strip() for s in _SKILL_VOCAB if s.strip()]


def _wordrx(term: str) -> re.Pattern:
    t = re.escape(term.strip().lower()).replace(r"\ ", r"\s+")
    return re.compile(r"(?<![a-z0-9])" + t + r"(?:s|es|ing|ed)?(?![a-z0-9])", re.I)


def _present(text: str, term: str) -> bool:
    return bool(_wordrx(term).search(text or ""))


def target_keywords(job: dict, tailoring: dict | None = None, limit: int = 28) -> list[str]:
    """Phrases the posting is likely screened on, most important first."""
    posting = f"{job.get('title') or ''}\n{job.get('description') or job.get('snippet') or ''}"
    low = posting.lower()
    scores: Counter = Counter()
    # 1. hard-skill vocabulary present in the posting, weighted by mentions and title presence
    for term in _SKILL_VOCAB:
        n = len(_wordrx(term).findall(posting))
        if n:
            scores[term] += 2 + min(n, 4) + (4 if _present(job.get("title") or "", term) else 0)
    # 2. tailoring's own picks are strongest evidence
    if tailoring:
        for k in tailoring.get("keywords_to_add") or []:
            scores[k.strip().lower()] += 8
        for m in tailoring.get("missing") or []:
            if isinstance(m, dict) and m.get("item"):
                scores[m["item"].strip().lower()] += 5 if m.get("importance") == "must" else 2
    # 3. distinctive phrases by TF-IDF against the corpus (bigrams preferred)
    try:
        jobs = db.jobs_for_scoring()
        model = matching.Model({j["id"]: f"{j['title']}\n{j.get('description') or ''}" for j in jobs[:3000]})
        vec = model.vec(posting)
        for t, w in sorted(vec.items(), key=lambda x: -x[1])[:60]:
            if len(t) < 4 or t.isdigit() or model.idf(t) < 1.6:
                continue
            if " " in t or len(t) >= 6:
                scores[t] += 1 + (2 if " " in t else 0)
    except Exception:
        pass
    # merge near-duplicates: drop a unigram if a chosen bigram contains it
    ordered = [t for t, _ in scores.most_common(limit * 3)]
    out: list[str] = []
    for t in ordered:
        if any(t != o and t in o.split() for o in out):
            continue
        if any(t != o and o in t.split() for o in out):
            out = [o for o in out if not (o in t.split())]
        out.append(t)
        if len(out) >= limit:
            break
    return out


def coverage(draft: str, keywords: list[str]) -> dict:
    present = [k for k in keywords if _present(draft, k)]
    missing = [k for k in keywords if k not in present]
    counts = {k: len(_wordrx(k).findall(draft or "")) for k in present}
    stuffed = [k for k, n in counts.items() if n > 6]      # a governance resume naturally says "data governance" a lot
    checks = {
        "has_summary": bool(re.search(r"(?im)^#+\s*professional summary|^#+\s*summary", draft or "")),
        "has_competencies": bool(re.search(r"(?im)^#+\s*(core competencies|skills)", draft or "")),
        "has_experience": bool(re.search(r"(?im)^#+\s*(professional )?experience", draft or "")),
        "has_education": bool(re.search(r"(?im)^#+\s*education", draft or "")),
        "no_tables": "|" not in (draft or "").replace("| ", "").split("\n", 1)[-1] if draft else True,
        "no_confirm_markers": "[confirm" not in (draft or "").lower(),
    }
    score = round(100 * len(present) / len(keywords)) if keywords else 0
    return {"score": score, "present": present, "missing": missing, "stuffed": stuffed,
            "counts": counts, "checks": checks, "total": len(keywords)}


def check_job(job: dict, uid: int = db.DEFAULT_USER) -> dict:
    tailoring = None
    if job.get("tailoring"):
        try:
            tailoring = json.loads(job["tailoring"])
        except json.JSONDecodeError:
            pass
    kws = target_keywords(job, tailoring)
    original = db.get_setting("resume_text", uid) or ""
    return {"keywords": kws,
            "original": coverage(original, kws) if original else None,
            "draft": coverage(job["resume_tailored"], kws) if job.get("resume_tailored") else None}
