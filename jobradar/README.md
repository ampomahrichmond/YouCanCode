# JobRadar

A self-hosted job-board consolidator for senior data-leadership roles. Every
sweep it queries Indeed, LinkedIn, Glassdoor, ZipRecruiter, and Google Jobs
across the US and Canada, checks the career sites of international
organizations (IMF, World Bank, UN Secretariat, UNDP, UNHCR, WFP, WHO, ILO,
IDB, EBRD…), consulting firms and government contractors directly, tags every
role with the employer's **sector**, deduplicates everything into a local
SQLite database, scores each role against your resume, and shows a live
dashboard with sector and category filters, a tracking pipeline, insights,
and a daily digest.

## Run it

```bash
python -m venv .venv && source .venv/bin/activate   # Windows: .venv\Scripts\activate
pip install -r requirements.txt
uvicorn app.main:app --port 8000
```

On Windows, `.\start.ps1` does the same and first stops any JobRadar server
already running (add `-Background` to detach it and log to `jobradar.log`).

Open http://localhost:8000 — a first sweep starts automatically at boot,
then repeats every `refresh_minutes`. An existing `jobradar.db` is migrated in
place at boot (new columns and tables are added, old rows backfilled).

## People and categories

JobRadar is shared by several people, each with their own search. The
header has a **person switcher** (＋ adds someone, ✎ renames or removes). The
job pool and employer boards are shared; everything personal is separate per
person: category and seniority preferences, resume and match scores, saved /
applied / pipeline state, notes, tailoring and drafts, watch and block lists,
applicant profile and apply rules, and the daily digest.

Roles are tagged with **categories** by title at ingest, using the
`categories` block in `config.yaml` (each has a label, the search terms it
contributes to the sweep, and the title keywords that tag a role; a role can
be in several). The sidebar shows category chips; **Save as my defaults**
stores the current categories, sectors, seniority, remote and country as that
person's preferences, applied whenever they open the dashboard, used to scope
their digest and their apply queue.

## Sectors

Categories say what the job is; **sectors** say who the employer is. Every
role gets exactly one sector key from the `sectors` block in `config.yaml`:
international organizations & development, staffing agencies, government &
development contractors, consulting, public sector, healthcare, education,
finance, energy, media, tech, retail, industrial, nonprofits, other. A role's
sector comes from, in order, the career site it was found on (the
`organizations` catalogue fixes each site's sector), an organization name or
alias match, the board's industry label (LinkedIn/Indeed), then whole-word
employer-name keywords checked sector by sector in config order. Change the
rules and every stored row is re-tagged at the next boot. The sidebar shows
sector chips (counts included), each row carries a clickable sector tag,
Insights has a "Roles by sector" table, the CSV export has a `sector` column,
and a person's saved sectors scope their digest.

## International organizations, consulting and contracting

The `organizations` catalogue in `config.yaml` lists ~95 organizations with
their sector. Those with a career-site URL are checked directly every sweep:
IMF, UNHCR, WFP, Gates Foundation, BIS, World Vision, PATH, IRC, PwC, Booz
Allen, Guidehouse, Marsh McLennan/Oliver Wyman, Gartner, Huron, Protiviti,
Cognizant, BDO, Baker Tilly, DXC, Genpact, Fractal, Ankura, BRG, PCG, Leidos,
CACI, ICF, GDIT, MITRE, ManTech (Workday); UNDP (Oracle Cloud); IDB, EY, ILO,
EBRD (SuccessFactors RSS); World Bank Group (Cornerstone); the UN Secretariat
(UN Careers API); WHO (Taleo). Name-only entries (UNICEF, UNOPS, OAS, PAHO,
ADB, OECD, McKinsey, BCG, Bain, Deloitte, Accenture, KPMG, SAIC, Chemonics,
DAI, Tetra Tech, Abt, RTI, Mathematica…) teach the classifier, so their roles
found on Indeed/LinkedIn are tagged too. Add any organization by pasting its
careers URL on the Companies tab and picking a sector. **ReliefWeb** (UN
agencies, NGOs and development contractors in one feed) switches on once you
set `reliefweb.appname` to a name registered at
https://apidoc.reliefweb.int/parameters#appname.

International organizations title data roles differently, so the sweep has a
"M&E, Statistics & Development Data" category (monitoring & evaluation,
information management officer, statistician) and the seniority keywords
include officer, specialist, consultant, adviser, economist and statistician;
UN grades (P-3…D-2) are mapped onto the seniority buckets. With `countries:
["US", "CA"]` only their Washington, New York, Montreal and remote roles get
through; empty the list to keep Geneva, Nairobi, Vienna and the rest.

## The dashboard

**Openings** — every active role, 8 per page (adjustable). Every filter in
the sidebar is a dropdown: categories, sectors and seniority are multi-select
dropdowns with counts; tracking bucket, minimum match, country, source,
search term, time on radar, minimum salary and "likely filled" are single
selects; "Only show" is a dropdown of toggles for new-since-last-sweep /
remote / easy apply / salary listed / **no travel required**. Sort by newest,
best match, date posted, salary, company or title. `/` focuses search,
`←`/`→` flip pages. Filters persist in the browser. Export CSV of the current view.

**Countries.** Locations resolve to one of the countries in
`country_labels`: the US and Canada plus the UAE and the hubs whose employers
routinely hire remote talent in North America (UK, Ireland, Germany,
Netherlands, Switzerland, France, Singapore, Australia, Israel, Saudi Arabia,
Qatar); anywhere else is "Other" and dropped. The Country filter offers
"All", "US + Canada" and each country; the sweep also searches the UAE
directly (a `locations` entry). A US state or Canadian province suffix always
wins over a foreign city name ("London, ON" is Canada).

**Travel.** Each posting's description is checked for a travel requirement
("willingness to travel", "up to 25% travel", …, ignoring travel insurance /
Travelers-style noise) and stored as none / required / stated percentage. Rows
carry a ✈ Travel badge; the "No travel required" toggle (on by default, saved
in each person's defaults, applied to their digest) removes them from the list.

Each row shows salary (annualised where needed), an ⚡ Easy apply badge, the
inferred seniority, country, board, a match ring once a resume is loaded, and
★ Watched / Likely filled / pipeline-stage badges. Actions: ★ Save, ✓ Applied,
a stage picker (Saved → Applied → Interviewing → Offer / Rejected), ✕ Hide,
Watch / Block the company, open on the board, apply at the employer when the
board exposes the ATS link. Expanding a row reveals the description, "why it
matched" (shared skills and gaps), facts, notes, and — with Claude enabled —
four buttons: **Tailor resume** (a structured gap analysis: what the posting
rewards that the resume lacks or under-sells, exact phrases to mirror,
ready-to-paste bullet rewrites, a tailored summary, things to confirm, and the
interview probes to expect), **Draft tailored resume** (a complete resume
rewritten for that posting using only what's in yours, with `[confirm]`
markers where you must verify, copy or download as Markdown), **Fit summary**
and **Cover letter**. Nothing is invented: the prompts forbid adding
employers, tools, dates or numbers that aren't in your resume. The draft is
written for ATS parsers (standard headings, "Title — Company (dates)" lines,
exact posting phrases in a Core Competencies block and in context, acronyms
spelled out, no tables or graphics), and an **ATS keyword check** scores the
draft and your original against the posting's target phrases, listing what's
missing; **Refine** re-drafts to work those in where your resume supports them.

**Best matches** — the same list sorted by resume fit with a 50%+ floor, plus
the resume card: drop a PDF, DOCX or TXT to (re)score every role. Scoring is
local TF-IDF + title coverage + seniority fit; the file never leaves the
machine unless you press a Claude button.

**Pipeline** — a Kanban board of Saved / Applied / Interviewing / Offer /
Rejected. Drag cards between stages or use ◀ ▶; each card shows match score,
days in the current stage, and a dashed border if the role looks filled.

**Insights** — salary by seniority (range chart: middle half, median, full
range), salary by search term, resume match spread, search-term performance
(roles, new in 7 days, saved, applied, average match), and a "likely filled"
card with one-click archiving.

**Companies** — watchlist (roles pin to the top with a ★ badge), blocklist
(roles vanish from every list, count and digest; good for staffing agencies),
and **Career sites checked directly**: the organizations' own career sites.
Greenhouse, Lever, Ashby, SmartRecruiters, Workable, BambooHR, Workday,
Oracle Cloud HCM, SuccessFactors (RSS), Cornerstone, Taleo and the UN Careers
API all expose a public feed per organization, so these never block and carry
the original posting, pay range and apply link (Cornerstone and Taleo give
titles, locations and links only). Sites come from the `organizations`
catalogue, are discovered automatically from the "apply at employer" links
Indeed and LinkedIn return (short links are resolved a few at a time), or are
added by pasting any careers URL with a sector. Each sweep checks every
enabled site first; titles go through the same filters as the big boards and
only new postings have descriptions fetched. Large systems (Workday, Oracle,
SuccessFactors, Cornerstone, Taleo, ReliefWeb) are searched by the keywords in
`ats.search_terms` because listing a whole tenant isn't possible. The table
shows each site's sector (editable; stored roles are re-tagged) and can be
filtered. These sources appear under "Career sites (direct)" in the Source
filter.

**Apply** — an application engine for employer career sites. Fill in the
applicant profile once (contact details, education, screening answers such as
work authorisation and sponsorship, voluntary EEO self-identification, plus
free-form "question pattern → answer" rules for anything recurring). Set the
rules (minimum match, seniority, career sites, daily cap) and roles that meet
them are queued after every sweep; any role can also be queued from Openings.
**Prepare** opens a real Chrome window (Playwright, persistent profile in
`browser-profile/`), fills the form from the profile, attaches the tailored
resume as Word and the cover letter, answers what it has rules for, saves a
screenshot under `applications/<job>/`, and then stops so you can review and
press Submit; when the confirmation page appears the role is marked Applied.
The report lists every field it filled, the required ones it couldn't, and
questions it had no rule for, so you can add profile rules over time.
**Auto-submit** (off by default, behind a confirmation) submits queued roles
unattended up to the daily cap when nothing required is missing. Supported:
Greenhouse, Lever, Ashby, SmartRecruiters and Workable public forms. Workday,
LinkedIn and Indeed require accounts and are not automated.

**Digest** — the header button opens today's Markdown digest of new roles
above the match threshold. It is also written to `./digests/YYYY-MM-DD.md`
every morning at `digest.time`, and emailed when SMTP is configured.

The header stays fixed while scrolling; "Hide panel" collapses it to one line.

## Configure

Everything lives in `config.yaml`:

- `categories` — `{key: {label, terms, title_keywords}}`; their terms form
  the sweep and their title keywords tag roles and extend `title_must_include`.
  `terms` holds extra searches outside any category (tagged "general").
- `locations` — every term runs against every location, in order, and
  results are written after each search. `remote: true` asks each board for
  remote roles; `country` picks Indeed/Glassdoor's regional site.
- `searches` — extra explicit term + location pairs.
- `sites`, `results_per_site`, `hours_old`, `refresh_minutes`.
- `title_keywords` (must contain one: manager, director, analyst…),
  `title_must_include` (must also contain a domain word: data, HR, BI…),
  `exclude_title_keywords`, `exclude_keywords`, `include_keywords`, `levels`,
  `countries`, `min_salary`, `remote_only`. Title matching is whole-word.
- `linkedin_fetch_description` / `linkedin_easy_apply_pass` — LinkedIn only
  reveals salary/seniority/description per job page, and Easy Apply is
  detected via a second card-only pass with LinkedIn's own filter.
- `sectors` — ordered `{key: {label, companies, industries}}`; `companies`
  are whole-word employer-name keywords, `industries` substrings of the
  board's industry label. `organizations` — `[{name, aliases, sector, url |
  provider+slug}]`, the catalogue of sites checked directly and names the
  classifier knows. `reliefweb.appname` — registered ReliefWeb app name
  (or `JOBRADAR_RELIEFWEB_APPNAME` in `.env`).
- `ats` — career sites: `enabled`, `auto_discover`, `resolve_per_sweep`,
  `max_new_per_board`, `max_age_days`, `workers`, `search_terms` (keywords
  for Workday/Oracle/SuccessFactors/Cornerstone/Taleo/ReliefWeb searches;
  `workday_terms` still works), and `boards` (URLs or `{provider, slug,
  sector}` to add by hand).
- `stale_after_sweeps` — roles absent from this many completed sweeps are
  flagged "likely filled".
- `digest` — `time`, `timezone`, `hours`, `min_match`, `max_jobs`, optional
  `email` (SMTP; password from the env var named by `password_env`).
- `llm_model` — model for tailoring, fit summaries and cover letters (default
  `claude-opus-5`). Needs `ANTHROPIC_API_KEY`: put it in a `.env` file next to
  `config.yaml` (see `.env.example`; the server reads it at startup) or in the
  environment; without it the buttons explain what to set.
- `proxies` / `proxies_file` / `proxies_env` / `proxy_sites` — rotating
  proxies for the boards that block direct requests.

## API

- `GET /api/jobs` — `{jobs, total}`; params mirror the sidebar plus
  `sort=match`, `min_match`, `stale=hide|only`, `status=active|pipeline|saved|…|all`,
  `categories=a,b`, `sector=a,b`.
- `GET /api/jobs.csv` — same filters, CSV.
- `PATCH /api/jobs/{id}` — `{"status": "...", "notes": "..."}`.
- `POST /api/jobs/{id}/fit`, `POST /api/jobs/{id}/cover_letter` — Claude.
- `GET|POST|DELETE /api/resume` — upload (multipart `file`), summary, remove.
- `GET /api/pipeline`, `POST /api/archive_stale`, `GET /api/insights`.
- `GET|POST /api/companies`, `DELETE /api/companies/{name_norm}`.
- `GET /api/digest` (Markdown), `POST /api/digest/run`.
- `GET|POST /api/ats` (POST body `{url | provider+slug, company, sector}`),
  `PATCH /api/ats/{id}` (`{enabled}` and/or `{sector}`), `DELETE /api/ats/{id}`,
  `POST /api/ats/discover`, `POST /api/ats/run` — career sites checked directly.
- `GET /api/status` (includes `by_sector`), `GET /api/meta` (includes
  `sectors` with counts), `POST /api/refresh`.
- `GET|POST /api/users`, `PUT|DELETE /api/users/{id}` — people and their
  preferences. Every other endpoint acts for the person in the `jr_user`
  cookie (or `X-User` header); default is user 1.

## Honest caveats

- These boards prohibit scraping in their terms of service and actively fight
  it. As of September 2026 (jobspy 1.1.82, the latest release) only Indeed
  and LinkedIn answer direct requests: Glassdoor and ZipRecruiter return
  Cloudflare 403s, and Google serves a bot-check interstitial instead of
  results. Rotating residential proxies are the only route past those. A
  blocked site never kills a sweep — errors are logged per-search and the
  rest continue.
- A full sweep (25 terms × 3 locations + extras = 77 searches × 5 boards) with
  LinkedIn page fetching takes 45+ minutes, hence the 90-minute interval. If
  sweeps overlap, the later one is skipped. The career-site pass before it
  (1,000+ sites × 9 search terms for the keyword-searched systems) takes
  10–20 minutes; trim `ats.search_terms` to shorten it.
- Career-site coverage is only as good as the feed: SuccessFactors RSS returns
  the 20 newest matches per keyword worldwide (so EY's US roles mostly arrive
  via LinkedIn/Indeed instead), Cornerstone and Taleo give no description,
  and some Oracle tenants refuse the detail call (the short description is
  kept). Sector tagging by employer-name keyword is a heuristic: fix a wrong
  one by adding the organization (with `aliases`) to the catalogue, or by
  changing the site's sector on the Companies tab.
- Match scores are a heuristic ranking, not a verdict: they reward vocabulary
  overlap with your resume, title coverage and seniority fit. Treat 70+ as
  "read this", 50–70 as "maybe", and use the Fit summary for the real call.
- Easy-apply detection is best effort per board; Google Jobs never says.
- "Likely filled" means the boards stopped returning the role, which also
  happens when you remove its search term from the config.
- Dedup is by (title, company, location); later sightings fill in salary /
  easy-apply / description if the first lacked them.
- The database is `jobradar.db` in the project root; delete it to start fresh.
  It runs in WAL mode (you will see `jobradar.db-wal` / `-shm` beside it).

## Performance notes

- Sweeps run in a child process (`python -m app.sweep`, below-normal CPU
  priority) so parsing thousands of career-site responses and scoring never
  competes with page requests. `start.ps1` stops the child along with the
  server. `--ats-only` checks the career sites only; `--rescore <user id>`
  scores every role against a resume (what a resume upload launches; it takes
  a few minutes on a 40k-row corpus and the card shows "scoring" meanwhile).
- A full resume rescore happens only when the corpus has grown by more than
  10 % since the last one; new roles are scored as they are stored.
- The header numbers (`/api/status`) come from two passes over the active
  rows and the career-site list from one grouped query, so both stay well
  under a second at 40k roles. Free-text search still scans descriptions
  (about a second).
- If pages are slow while the machine shows little free memory, it is the
  machine: JobRadar itself uses ~200 MB for the server and ~50–300 MB for a
  sweep.
