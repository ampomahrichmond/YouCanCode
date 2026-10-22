"""Optional Claude layer: per-job fit summary and cover-letter draft.

Runs only when Anthropic credentials are available (ANTHROPIC_API_KEY, or an
`ant auth login` profile). The resume goes in the cached system prompt so the
per-job calls only pay for the job description."""
from __future__ import annotations

import logging
import os
from pathlib import Path

log = logging.getLogger("jobradar.llm")

DEFAULT_MODEL = "claude-opus-5"
ENV_FILE = Path(__file__).resolve().parent.parent / ".env"


def load_env_file():
    """KEY=VALUE lines from ./.env (git-ignored) into the environment, without
    overriding anything already set. Re-read on every check so a key added
    while the server runs takes effect without a restart."""
    if not ENV_FILE.exists():
        return
    try:
        lines = ENV_FILE.read_text(encoding="utf-8-sig").splitlines()
    except OSError:
        return
    for line in lines:
        line = line.strip()
        if not line or line.startswith("#") or "=" not in line:
            continue
        k, v = line.split("=", 1)
        k, v = k.strip(), v.strip().strip('"').strip("'")
        if k and v and not os.environ.get(k):
            os.environ[k] = v


def available() -> bool:
    load_env_file()
    if os.environ.get("ANTHROPIC_API_KEY") or os.environ.get("ANTHROPIC_AUTH_TOKEN"):
        return True
    cfg_dir = Path(os.environ.get("XDG_CONFIG_HOME") or Path.home() / ".config") / "anthropic"
    return cfg_dir.exists() and any(cfg_dir.iterdir())


class LLMError(Exception):
    pass


def _client():
    try:
        import anthropic
    except ImportError as e:  # pragma: no cover
        raise LLMError("The anthropic package is not installed (pip install anthropic).") from e
    if not available():
        raise LLMError("No Anthropic credentials found. Set ANTHROPIC_API_KEY (or run `ant auth login`) "
                       "and restart the server.")
    return anthropic.Anthropic()


def _job_block(job: dict) -> str:
    salary = ""
    if job.get("salary_annual"):
        salary = f"\nSalary (annualised): ${int(job['salary_annual']):,}"
    return (f"Title: {job.get('title')}\nCompany: {job.get('company')}\nLocation: {job.get('location')}"
            f"{' (remote)' if job.get('is_remote') else ''}\nSeniority bucket: {job.get('level')}{salary}\n\n"
            f"Job description:\n{job.get('description') or job.get('snippet') or '(none captured)'}")


def _run(system_text: str, user_text: str, resume: str, model: str, max_tokens: int) -> str:
    import anthropic
    client = _client()
    system = [
        {"type": "text", "text": system_text},
        {"type": "text", "text": f"Candidate resume:\n\n{resume}", "cache_control": {"type": "ephemeral"}},
    ]
    try:
        with client.beta.messages.stream(
            model=model,
            max_tokens=max_tokens,
            system=system,
            messages=[{"role": "user", "content": user_text}],
            thinking={"type": "adaptive"},
            output_config={"effort": "medium"},
            betas=["server-side-fallback-2026-07-01"],
            fallbacks="default",
        ) as stream:
            msg = stream.get_final_message()
    except anthropic.AuthenticationError as e:
        raise LLMError("Anthropic rejected the credentials (401).") from e
    except anthropic.RateLimitError as e:
        raise LLMError("Anthropic rate limit hit; try again in a minute.") from e
    except anthropic.APIStatusError as e:
        raise LLMError(f"Anthropic API error {e.status_code}: {e.message}") from e
    except anthropic.APIConnectionError as e:
        raise LLMError("Could not reach the Anthropic API (network).") from e
    if msg.stop_reason == "refusal":
        raise LLMError("The model declined this request.")
    text = "".join(b.text for b in msg.content if b.type == "text").strip()
    if not text:
        raise LLMError("Empty response from the model.")
    return text


FIT_SYSTEM = """You are a candid career coach helping a data-leadership candidate triage job postings.
Given the candidate's resume and one job posting, write a short fit assessment in Markdown:

**Fit: <Strong | Good | Stretch | Weak>** — one sentence on why.
**What to emphasise** — 3 bullets naming the resume experiences that map to this role's priorities.
**Gaps** — up to 3 bullets on what the posting wants that the resume doesn't show (or "None material").
**Positioning line** — one sentence the candidate could open a cover letter with.

Be specific to this posting; quote its own language where useful. Under 180 words. No preamble."""

COVER_SYSTEM = """You write concise, specific cover letters for a data-leadership candidate.
Using the candidate's resume and the job posting, draft a cover letter:

- Four short paragraphs: hook tied to the company/role, two paragraphs of matched evidence with concrete results from the resume, a close with a clear ask.
- Mirror the posting's key priorities and terminology; never invent experience that isn't in the resume.
- Plain, confident, human tone. No clichés ("I am writing to apply", "passionate", "synergy").
- 230–300 words. Address to "Hiring Manager" unless a name is given. Sign off with the candidate's name if it appears in the resume.
Output only the letter in Markdown."""


def fit_summary(resume: str, job: dict, model: str = DEFAULT_MODEL) -> str:
    return _run(FIT_SYSTEM, _job_block(job), resume, model, max_tokens=2000)


# ── resume tailoring (structured) ─────────────────────────────────────────
TAILOR_SYSTEM = """You are an expert resume strategist and former recruiter for data, analytics and HR leadership roles.
You are given a candidate's resume and one job posting. Your job is to make this candidate a top applicant for THIS role
by showing exactly what the posting rewards that the resume under-sells or lacks, and how to fix it honestly.

Rules:
- Never invent experience, employers, titles, tools, certifications or numbers. You may only reframe, reorder,
  re-word and quantify what the resume already contains. If something is genuinely absent, say so and put it under
  `missing` with a suggestion that is either "add it if you have it", "reframe X as evidence of it", or "cannot claim".
- Quote the posting's own language for evidence so the candidate can mirror it (ATS screening matches phrases).
- Be concrete: rewritten bullets must be ready to paste, lead with a strong verb, and keep the candidate's facts.
- Importance: "must" = the posting lists it as required or repeats it; "nice" = preferred or mentioned once.
- Keep every string plain text (no Markdown), short and specific."""

TAILOR_SCHEMA = {
    "type": "object",
    "properties": {
        "verdict": {"type": "string", "enum": ["Strong", "Good", "Stretch", "Weak"]},
        "headline": {"type": "string", "description": "One sentence: overall fit and the single biggest lever."},
        "match_reasons": {"type": "array", "items": {"type": "string"}, "description": "2-4 strengths that map directly to the posting."},
        "missing": {"type": "array", "items": {"type": "object", "properties": {
            "item": {"type": "string"},
            "importance": {"type": "string", "enum": ["must", "nice"]},
            "evidence": {"type": "string", "description": "Short quote from the posting."},
            "suggestion": {"type": "string", "description": "How to address it honestly."}},
            "required": ["item", "importance", "evidence", "suggestion"], "additionalProperties": False}},
        "keywords_to_add": {"type": "array", "items": {"type": "string"},
                            "description": "Exact phrases from the posting the resume should contain (only if truthful)."},
        "emphasize": {"type": "array", "items": {"type": "string"},
                      "description": "Existing resume content to move up, expand or quantify, and why."},
        "bullet_rewrites": {"type": "array", "items": {"type": "object", "properties": {
            "original": {"type": "string"}, "rewritten": {"type": "string"}, "why": {"type": "string"}},
            "required": ["original", "rewritten", "why"], "additionalProperties": False},
            "description": "3-6 of the candidate's bullets rewritten for this role."},
        "summary": {"type": "string", "description": "A tailored 3-4 sentence professional summary for the top of the resume."},
        "confirm": {"type": "array", "items": {"type": "string"},
                    "description": "Things to check with the candidate that, if true, should be added (e.g. a tool used but not listed)."},
        "interview_probes": {"type": "array", "items": {"type": "string"},
                             "description": "2-3 questions an interviewer will likely ask about the gaps, with a one-line way to answer."},
    },
    "required": ["verdict", "headline", "match_reasons", "missing", "keywords_to_add", "emphasize",
                 "bullet_rewrites", "summary", "confirm", "interview_probes"],
    "additionalProperties": False,
}


def tailor(resume: str, job: dict, model: str = DEFAULT_MODEL) -> dict:
    """Structured gap analysis + concrete edits for one posting."""
    import anthropic
    client = _client()
    system = [
        {"type": "text", "text": TAILOR_SYSTEM},
        {"type": "text", "text": f"Candidate resume:\n\n{resume}", "cache_control": {"type": "ephemeral"}},
    ]
    try:
        msg = client.messages.create(
            model=model,
            max_tokens=8000,
            system=system,
            messages=[{"role": "user", "content": _job_block(job)}],
            thinking={"type": "adaptive"},
            output_config={"effort": "high", "format": {"type": "json_schema", "schema": TAILOR_SCHEMA}},
        )
    except anthropic.AuthenticationError as e:
        raise LLMError("Anthropic rejected the credentials (401).") from e
    except anthropic.RateLimitError as e:
        raise LLMError("Anthropic rate limit hit; try again in a minute.") from e
    except anthropic.APIStatusError as e:
        raise LLMError(f"Anthropic API error {e.status_code}: {e.message}") from e
    except anthropic.APIConnectionError as e:
        raise LLMError("Could not reach the Anthropic API (network).") from e
    if msg.stop_reason == "refusal":
        raise LLMError("The model declined this request.")
    import json
    text = next((b.text for b in msg.content if b.type == "text"), "")
    try:
        return json.loads(text)
    except json.JSONDecodeError as e:
        raise LLMError("The model returned malformed JSON; try again.") from e


RESUME_SYSTEM = """You rewrite a candidate's resume so it is tuned to one specific job posting AND parses and scores well in
applicant-tracking systems (Workday, Greenhouse, Lever, iCIMS, Taleo). You receive the candidate's current resume, the
posting, a gap analysis already agreed with the candidate, and a list of target ATS keywords extracted from the posting.

Truth rules (absolute):
- Use ONLY facts in the resume. Never add employers, titles, dates, tools, certifications, metrics or responsibilities
  that are not there. Where the gap analysis says something should be confirmed, leave it out or mark it "[confirm: …]".
- A target keyword may be used only where the resume genuinely supports it. Skip keywords the candidate cannot claim.

ATS rules:
- Standard section headings, exactly: "Professional Summary", "Core Competencies", "Professional Experience",
  "Education", "Certifications" (omit a section only if the resume has nothing for it). Reverse-chronological.
- Each role as a heading line "Title — Company, City, ST (Mon YYYY – Mon YYYY)"; use "Present" for current. If the
  candidate's real title differs from the posting's, keep the real title and add the aligned framing in the summary,
  e.g. "Data Governance Manager-level leader…" — never change actual titles.
- Mirror the posting's exact phrasing for every target keyword the candidate can truthfully claim: in Core
  Competencies AND at least once in context inside an experience bullet. Spell out acronyms with the acronym on first
  use, e.g. "Master Data Management (MDM)". Include both singular/plural or verb/noun forms only if natural.
- No keyword stuffing: a term at most three times overall, always in a real sentence.
- Plain text structure only: no tables, columns, text boxes, icons, images, headers/footers, or special bullet
  glyphs. Use "-" bullets. No first-person pronouns. Numbers as digits.
- Bullets: strong verb, what you did, scale, measurable result using the resume's own numbers. 3-6 bullets per recent
  role, fewer for older ones. Cut anything irrelevant to this role.
- Professional Summary: 3-4 lines that name the target role family, years of experience, and the 4-6 most
  important keywords. Core Competencies: 12-18 short phrases, the target keywords first.
- Length: one page equivalent, at most two.
- Output the complete resume in clean Markdown. Output only the resume, no commentary."""


def tailored_resume(resume: str, job: dict, tailoring: dict | None, keywords: list[str] | None = None,
                    model: str = DEFAULT_MODEL, missing: list[str] | None = None) -> str:
    import json
    user = _job_block(job)
    if keywords:
        user += "\n\nTarget ATS keywords (exact phrasing; use only where truthful):\n- " + "\n- ".join(keywords)
    if tailoring:
        user += "\n\nAgreed gap analysis (JSON):\n" + json.dumps(tailoring, ensure_ascii=False)
    if missing:
        user += ("\n\nA previous draft was missing these target keywords; work each one in naturally where the resume "
                 "supports it, or leave it out if it cannot be claimed:\n- " + "\n- ".join(missing))
    return _run(RESUME_SYSTEM, user, resume, model, max_tokens=8000)


def cover_letter(resume: str, job: dict, notes: str = "", model: str = DEFAULT_MODEL) -> str:
    user = _job_block(job)
    if notes.strip():
        user += f"\n\nCandidate's notes for this application:\n{notes.strip()}"
    return _run(COVER_SYSTEM, user, resume, model, max_tokens=3000)
