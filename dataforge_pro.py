#!/usr/bin/env python3
"""
DataForge Pro  v3.0
━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━
NEW IN v3
  • Full-Name column parser  — splits  ZONE>DB>SCHEMA>TABLE  strings in the
    reference file into individual columns so SCHEMA and TABLE can be joined
  • Explicit Join selector   — choose exactly which column pairs are joined
    (e.g. SCHEMA_NAME ↔ ref_schema) before fuzzy search begins
  • Preview Search bar       — live text-filter on both the cleaned source and
    reference data grids
  • Richer Results page      — per-anchor breakdown, confidence histogram,
    match-pair table, and a clean multi-sheet Excel export
  • Faster + sharper engine  — direct rapidfuzz scoring within exact-join
    groups; TF-IDF only as a pre-filter for large groups; stricter anti-false-
    positive guard (minimum char overlap + length-ratio check)
━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━
Dependencies: customtkinter, pandas, scikit-learn, rapidfuzz, openpyxl
"""

import os, re, threading, warnings
from pathlib import Path
from typing import Dict, List, Optional, Tuple

import numpy as np
import pandas as pd
import customtkinter as ctk
import tkinter as tk
from tkinter import filedialog, messagebox, ttk
from sklearn.feature_extraction.text import TfidfVectorizer
from sklearn.metrics.pairwise import cosine_similarity
from rapidfuzz import fuzz as rfuzz

warnings.filterwarnings("ignore")
ctk.set_appearance_mode("light")
ctk.set_default_color_theme("blue")

# ══════════════════════════════════════════════════════════════════════════════
#  DESIGN TOKENS
# ══════════════════════════════════════════════════════════════════════════════
C: Dict[str,str] = {
    "bg":"#F4F6FA", "card":"#FFFFFF", "stripe":"#F9FAFB",
    "navy":"#0C1824", "navy2":"#172535",
    "blue":"#2563EB", "blue_lt":"#EFF6FF", "blue_dk":"#1D4ED8", "blue_mid":"#3B82F6",
    "teal":"#0D9488", "teal_lt":"#F0FDFA",
    "green":"#16A34A", "green_lt":"#F0FDF4",
    "amber":"#B45309", "amber_lt":"#FFFBEB",
    "red":"#DC2626",   "red_lt":"#FEF2F2",
    "purple":"#7C3AED","purple_lt":"#F5F3FF",
    "slate":"#334155", "slate_lt":"#F1F5F9",
    "text":"#0C1824",  "text2":"#475569", "text3":"#94A3B8", "text_inv":"#FFFFFF",
    "border":"#E2E8F0","border2":"#CBD5E1",
}
FD=("Georgia",18,"bold"); FH=("Georgia",14,"bold"); FS=("Georgia",12,"bold")
FB=("Helvetica",10);  FBB=("Helvetica",10,"bold")
Fs=("Helvetica",9);   FsB=("Helvetica",9,"bold")
FM=("Courier New",9); FBG=("Helvetica",8)

# ══════════════════════════════════════════════════════════════════════════════
#  FILE LOADING  (unchanged from v2)
# ══════════════════════════════════════════════════════════════════════════════

def smart_load(fp:str)->Tuple[pd.DataFrame,str]:
    ext=Path(fp).suffix.lower()
    if ext in(".xlsx",".xls",".xlsm",".xlsb"):
        df=pd.read_excel(fp,dtype=str,keep_default_na=False)
        return df.fillna(""),f"Excel ({ext})"
    try:
        with open(fp,"r",encoding="utf-8",errors="replace") as f:
            head="".join(f.readline() for _ in range(5))
    except: head=""
    p,c,t=head.count("|"),head.count(","),head.count("\t")
    if p>=c and p>=t: sep,lbl="|","pipe"
    elif t>c: sep,lbl="\t","tab"
    else: sep,lbl=",","csv"
    try:
        df=pd.read_csv(fp,sep=sep,dtype=str,encoding="utf-8",errors="replace",
                       keep_default_na=False,on_bad_lines="skip")
        return df.fillna(""),f"{ext.upper().lstrip('.')} ({lbl})"
    except: pass
    with open(fp,"r",encoding="utf-8",errors="replace") as f:
        lines=[l.strip() for l in f if l.strip()]
    return pd.DataFrame({"raw_content":lines}),"raw"

def parse_pipe_source(fp:str)->pd.DataFrame:
    ext=Path(fp).suffix.lower()
    if ext in(".xlsx",".xls",".xlsm",".xlsb"): return _pipe_excel(fp)
    with open(fp,"r",encoding="utf-8",errors="replace") as f:
        raw=[l.rstrip("\r\n") for l in f if l.strip()]
    if not raw: return pd.DataFrame()
    if "|" not in raw[0]:
        try:
            tmp=pd.read_csv(fp,dtype=str,keep_default_na=False,encoding="utf-8",errors="replace")
            fc=tmp.columns[0]
            if "|" in fc:
                hdrs=[h.strip() for h in fc.split("|") if h.strip()]
                rows=[]
                for cell in tmp.iloc[:,0]:
                    p=str(cell).split("|"); p=(p+[""]*len(hdrs))[:len(hdrs)]
                    rows.append([x.strip() for x in p])
                return pd.DataFrame(rows,columns=hdrs)
        except: pass
        return pd.DataFrame({"content":raw})
    return _pipe_lines(raw)

def _pipe_excel(fp:str)->pd.DataFrame:
    rdf=pd.read_excel(fp,header=None,dtype=str,keep_default_na=False)
    lines=[]
    for _,row in rdf.iterrows():
        cells=[str(v) for v in row if str(v).strip() not in("","nan","None")]
        if cells: lines.append("".join(cells).strip())
    return _pipe_lines(lines) if lines else pd.DataFrame()

def _pipe_lines(lines:List[str])->pd.DataFrame:
    if not lines: return pd.DataFrame()
    hdrs=[h.strip() for h in lines[0].split("|")]
    while hdrs and not hdrs[-1]: hdrs.pop()
    seen:Dict[str,int]={}; ch:List[str]=[]
    for h in hdrs:
        lbl=h or f"col_{len(ch)}"
        if lbl in seen: seen[lbl]+=1; ch.append(f"{lbl}_{seen[lbl]}")
        else: seen[lbl]=0; ch.append(lbl)
    n=len(ch); rows=[]
    for line in lines[1:]:
        p=line.split("|"); p=(p+[""]*n)[:n]; rows.append([x.strip() for x in p])
    df=pd.DataFrame(rows,columns=ch)
    return df.loc[:,(df!="").any(axis=0)]

def detect_key_columns(df:pd.DataFrame)->List[str]:
    pat=re.compile(r"(mal.?code|malcode|mal_code|table.?name|zone.?name|column.?name|"
                   r"steward|model|edm|edc|schema|database|owner|object|asset|domain|"
                   r"class|community|name)",re.I)
    return [c for c in df.columns if pat.search(str(c))]

# ══════════════════════════════════════════════════════════════════════════════
#  FULL-NAME HIERARCHICAL COLUMN PARSER  (NEW v3)
# ══════════════════════════════════════════════════════════════════════════════

HIER_SEP = re.compile(r"[>\\/]")          # accept  >  \  /  as separators

HIER_LABELS = {
    2: ["parsed_schema","parsed_table"],
    3: ["parsed_database","parsed_schema","parsed_table"],
    4: ["parsed_zone","parsed_database","parsed_schema","parsed_table"],
    5: ["parsed_zone","parsed_database","parsed_schema","parsed_table","parsed_column"],
}

def detect_hierarchical_columns(df:pd.DataFrame,sep_re=HIER_SEP)->List[str]:
    """Return columns whose values look like hierarchical paths (zone>db>schema>table)."""
    results=[]
    for col in df.columns:
        sample=df[col].dropna().head(20)
        hits=sum(1 for v in sample if isinstance(v,str) and len(sep_re.findall(v))>=2)
        if hits>=min(3,len(sample)//2+1):
            results.append(col)
    return results

def parse_hierarchical_column(df:pd.DataFrame, col:str,
                               prefix:str="parsed") -> pd.DataFrame:
    """
    Split a column like  CDIS_AAEDL_ADB_SRZ>hive_metastore>euc101209>tree_ops_def
    into:  parsed_zone | parsed_database | parsed_schema | parsed_table

    KEY FIX: parts are aligned from the RIGHT so that schema and table are
    ALWAYS the last two parts regardless of how many prefix segments exist.

      4 parts: zone=CDIS_AAEDL_ADB_SRZ  db=hive_metastore  schema=euc101209  table=tree_ops_def
      3 parts: zone=(empty)              db=hive_metastore  schema=euc101209  table=tree_ops_def
      2 parts: zone=(empty)              db=(empty)         schema=euc101209  table=tree_ops_def

    This prevents hive_metastore being misidentified as the schema.
    """
    if col not in df.columns:
        return df

    # Determine depth: use the MOST COMMON depth in the sample, not the max.
    # This prevents one outlier 5-part row from shifting schema/table for
    # all the regular 4-part rows.
    from collections import Counter
    sample = df[col].dropna().head(200)
    depth_counts: Counter = Counter()
    for v in sample:
        parts = [p.strip() for p in HIER_SEP.split(str(v)) if p.strip()]
        if len(parts) >= 2:
            depth_counts[len(parts)] += 1

    if depth_counts:
        # Pick modal depth; if tied prefer the higher one (more info); clamp 2–5
        modal_depth = max(depth_counts, key=lambda d: (depth_counts[d], d))
        modal_depth = max(2, min(modal_depth, 5))
    else:
        modal_depth = 4  # sensible default for zone>db>schema>table

    labels = HIER_LABELS.get(modal_depth, [f"{prefix}_{i}" for i in range(modal_depth)])
    if prefix != "parsed":
        labels = [l.replace("parsed_", f"{prefix}_") for l in labels]

    split_data = {l: [] for l in labels}
    for v in df[col]:
        parts = [p.strip() for p in HIER_SEP.split(str(v)) if p.strip()]

        # RIGHT-ALIGN to modal_depth: pad on the LEFT with empty strings so that
        # schema is always second-to-last and table is always last.
        if len(parts) < modal_depth:
            parts = [""] * (modal_depth - len(parts)) + parts
        else:
            # For rows deeper than modal, keep the rightmost modal_depth parts
            # (preserves schema and table which are always at the end)
            parts = parts[-modal_depth:]

        for lbl, p in zip(labels, parts):
            split_data[lbl].append(p)

    new_cols = pd.DataFrame(split_data, index=df.index)
    pos = df.columns.get_loc(col) + 1
    result = df.copy()
    for i, lbl in enumerate(labels):
        result.insert(pos + i, lbl, new_cols[lbl])
    return result

# ══════════════════════════════════════════════════════════════════════════════
#  TEXT UTILITIES
# ══════════════════════════════════════════════════════════════════════════════

def normalise(v)->str:
    if v is None: return ""
    if isinstance(v,float) and np.isnan(v): return ""
    s=str(v).lower().strip()
    s=re.sub(r"[\-_\s]+"," ",s)
    s=re.sub(r"[^\w\s/]"," ",s)
    return re.sub(r"\s+"," ",s).strip()

def tokenise_path(v:str)->List[str]:
    """Break a path/identifier string into all meaningful sub-tokens."""
    if not v: return []
    result:set=set()
    full=normalise(v)
    if full: result.add(full)
    for sep in (r"[>\\/]",r"[/\\_\-\s{}()\[\],;>]"):
        for part in re.split(sep,v):
            n=normalise(part)
            if n and len(n)>2: result.add(n)
    return list(result)

def _classify(a:str,b:str)->str:
    na,nb=normalise(a),normalise(b)
    if na==nb: return "Exact"
    if na in nb or nb in na: return "Substring"
    if set(na.split())&set(nb.split()): return "Token Overlap"
    return "Fuzzy"

def _make_detail(join_src_col,join_src_val,join_ref_col,join_ref_val,
                 scol,sval,rcol,rval,mtype,score)->str:
    parts=[]
    if join_src_col and join_src_val:
        parts.append(f"JOIN: {join_src_col}='{join_src_val}' ↔ {join_ref_col or '?'}='{join_ref_val or '?'}'")
    parts.append(f"MATCH: {scol} '{sval}' → {rcol} '{rval}'  [{mtype}  {score:.0f}%]")
    return "  |  ".join(parts)

# ══════════════════════════════════════════════════════════════════════════════
#  MATCH ENGINE  v3  (join-first, direct fuzzy, anti-false-positive)
# ══════════════════════════════════════════════════════════════════════════════

class MatchEngine:
    """
    Strategy
    ────────
    1. Hard join  — group reference rows by normalised join-column value.
       Source rows are mapped to their exact join-group before any fuzzy work.
    2. Direct scoring  — within each group use rapidfuzz directly on every
       (src_value, ref_value) pair.  Fast for small groups (<1000 candidates).
    3. TF-IDF pre-filter  — for large groups, use char-ngram cosine similarity
       to find the top-N candidates before running direct scoring.
    4. Anti-false-positive guard:
         • min token length  : skip values < 3 chars
         • length-ratio gate : reject if len(a)/len(b) < 0.4 (or > 2.5)
         • char-overlap gate : normalised common-bigrams must be ≥ 0.25
         • combined score    : 40% token_set_ratio + 40% partial_ratio
                               + 20% exact-substring bonus
    """

    COLS = ["Src Row","Join Src Val","Join Ref Val",
            "Src Column","Src Value",
            "Ref Row","Ref Column","Ref Value",
            "Match Detail","Score %","Match Type","Confidence"]

    TFIDF_THRESHOLD = 500   # use TF-IDF pre-filter if group has > this many rows

    def __init__(self, threshold:float=0.70):
        self.threshold = threshold
        # join-based index: norm_join_val → list of ref row indices
        self._join_groups: Dict[str,List[int]] = {}
        # ref data snapshot for scoring
        self._ref_df:  Optional[pd.DataFrame] = None
        self._ref_search_cols: List[str] = []
        self._ref_join_col:    Optional[str] = None
        # TF-IDF per-group (built lazily for large groups)
        self._tfidf_cache: Dict[str, Tuple] = {}   # norm_join → (vectorizer, matrix, rows)

    # ── Fit ───────────────────────────────────────────────────────────────────
    def fit(self, ref_df:pd.DataFrame,
            ref_join_col:Optional[str],
            ref_search_cols:List[str],
            status_cb=None):
        self._ref_df = ref_df.reset_index(drop=True)
        self._join_groups.clear()
        self._tfidf_cache.clear()
        self._ref_search_cols = [c for c in ref_search_cols if c in ref_df.columns]
        if not self._ref_search_cols:
            self._ref_search_cols = list(ref_df.columns)
        self._ref_join_col = ref_join_col if ref_join_col in ref_df.columns else None

        if status_cb: status_cb("Building join index…")
        for ridx in range(len(self._ref_df)):
            jval = normalise(str(self._ref_df.iloc[ridx][self._ref_join_col])) \
                   if self._ref_join_col else "__all__"
            self._join_groups.setdefault(jval,[]).append(ridx)

        if status_cb:
            n_groups=len(self._join_groups)
            status_cb(f"Join index ready — {n_groups:,} groups, {len(self._ref_df):,} ref rows")

    # ── Score a single pair ───────────────────────────────────────────────────
    def _score_pair(self, a:str, b:str) -> float:
        """Return [0,1] composite similarity.  Returns 0 if anti-FP guard fires."""
        if not a or not b: return 0.0
        na, nb = normalise(a), normalise(b)
        if not na or not nb: return 0.0

        # Guard: minimum length
        if len(na)<3 or len(nb)<3: return 0.0

        # Guard: length ratio (prevents "ABC" matching "ABCDEFGHIJKLMNOPQRST")
        lr = min(len(na),len(nb))/max(len(na),len(nb))
        if lr < 0.25: return 0.0

        # Guard: bigram overlap
        def bigrams(s): return {s[i:i+2] for i in range(len(s)-1)}
        ba,bb=bigrams(na),bigrams(nb)
        if ba and bb:
            bg_overlap=len(ba&bb)/max(len(ba),len(bb))
            if bg_overlap < 0.15: return 0.0

        # Exact / substring bonus
        if na == nb: return 1.0
        sub_bonus = (min(len(na),len(nb))/max(len(na),len(nb))) if (na in nb or nb in na) else 0.0

        # Rapidfuzz scores
        tsr = rfuzz.token_set_ratio(na,nb)/100.0
        pr  = rfuzz.partial_ratio(na,nb)/100.0
        tr  = rfuzz.token_sort_ratio(na,nb)/100.0

        combined = tsr*0.40 + pr*0.35 + tr*0.25
        score = max(combined, sub_bonus*0.90)

        # Penalty for very different lengths (partially mitigates false positives
        # where a short token matches a long unrelated string)
        score *= (0.6 + 0.4*lr)
        return round(score, 4)

    # ── Get ref rows for a join value ─────────────────────────────────────────
    def _get_group(self, norm_jval:str) -> List[int]:
        rows = self._join_groups.get(norm_jval,[])
        if not rows:
            # Partial fallback: allow contained match (e.g. "acca" in "acca_v2")
            rows = []
            for k,v in self._join_groups.items():
                if k!="__all__" and (norm_jval in k or k in norm_jval) and k:
                    rows.extend(v)
        return rows

    # ── TF-IDF pre-filter for large groups ───────────────────────────────────
    def _tfidf_candidates(self, query:str, norm_jval:str,
                          ref_rows:List[int], top_k:int=30) -> List[int]:
        if norm_jval not in self._tfidf_cache:
            # Build per-group index lazily
            docs=[]
            for ridx in ref_rows:
                row=self._ref_df.iloc[ridx]
                vals=[normalise(str(row[c])) for c in self._ref_search_cols if c in row.index]
                docs.append(" ".join(v for v in vals if v))
            try:
                vec=TfidfVectorizer(analyzer="char_wb",ngram_range=(2,4),
                                    max_features=20_000,sublinear_tf=True,min_df=1)
                mat=vec.fit_transform(docs)
                self._tfidf_cache[norm_jval]=(vec,mat,ref_rows)
            except Exception:
                return ref_rows[:top_k]
        vec,mat,_=self._tfidf_cache[norm_jval]
        try:
            qv=vec.transform([normalise(query)])
            sims=cosine_similarity(qv,mat).flatten()
            top_ix=sims.argsort()[-top_k:][::-1]
            return [ref_rows[i] for i in top_ix if sims[i]>0.05]
        except Exception:
            return ref_rows[:top_k]

    # ── Score one source value against a group ───────────────────────────────
    def _match_value(self, src_val:str, ref_rows:List[int],
                     norm_jval:str, top_k:int=3) -> List[Tuple]:
        """Returns [(score, ridx, ref_col, ref_raw), …] sorted desc."""
        if not ref_rows or not src_val: return []

        # Pre-filter with TF-IDF for large groups
        candidates = (self._tfidf_candidates(src_val, norm_jval, ref_rows, top_k*10)
                      if len(ref_rows) > self.TFIDF_THRESHOLD
                      else ref_rows)

        best: Dict[Tuple[int,str], Tuple[float,str]] = {}
        for ridx in candidates:
            row = self._ref_df.iloc[ridx]
            for col in self._ref_search_cols:
                ref_val = str(row.get(col,"")).strip()
                if not ref_val or ref_val in ("nan","None"): continue
                sc = self._score_pair(src_val, ref_val)
                if sc >= self.threshold:
                    key=(ridx,col)
                    if sc > best.get(key,(0,""))[0]:
                        best[key]=(sc,ref_val)

        hits=[(sc,ridx,col,rv) for (ridx,col),(sc,rv) in best.items()]
        hits.sort(key=lambda x:-x[0])
        return hits[:top_k]

    # ── Full sweep ────────────────────────────────────────────────────────────
    def run(self, src_df:pd.DataFrame,
            src_join_col:Optional[str],
            src_search_cols:List[str],
            stop_event:Optional[threading.Event]=None,
            progress_cb=None, status_cb=None) -> pd.DataFrame:

        src_join_col = src_join_col if src_join_col in src_df.columns else None
        src_search_cols = [c for c in src_search_cols if c in src_df.columns]
        if not src_search_cols:
            src_search_cols=[c for c in src_df.columns if c!=src_join_col]

        total = len(src_df)*len(src_search_cols)
        done  = 0
        output:List[Dict]=[]

        for ridx in range(len(src_df)):
            if stop_event and stop_event.is_set(): break
            row = src_df.iloc[ridx]

            # Determine join group
            jval_raw = str(row[src_join_col]).strip() if src_join_col else ""
            norm_j   = normalise(jval_raw) if jval_raw else "__all__"
            ref_rows = self._get_group(norm_j) if norm_j!="__all__" \
                       else list(range(len(self._ref_df)))

            if not ref_rows:
                done+=len(src_search_cols)
                if progress_cb and total>0: progress_cb(done/total)
                continue

            # Get actual join value from ref (first matching row)
            ref_jval=""
            if self._ref_join_col and ref_rows:
                ref_jval=str(self._ref_df.iloc[ref_rows[0]].get(
                             self._ref_join_col,"")).strip()

            for scol in src_search_cols:
                if stop_event and stop_event.is_set(): break
                sv = str(row[scol]).strip()
                if not sv or len(sv)<3 or sv in("nan","None"):
                    done+=1; continue

                for score,ref_ridx,ref_col,ref_raw in self._match_value(sv,ref_rows,norm_j):
                    mtype    = _classify(sv,ref_raw)
                    conf     = _confidence_label(score)
                    detail   = _make_detail(src_join_col,jval_raw,
                                            self._ref_join_col,ref_jval,
                                            scol,sv,ref_col,ref_raw,mtype,score*100)
                    output.append({
                        "Src Row":      ridx+1,
                        "Join Src Val": jval_raw,
                        "Join Ref Val": ref_jval,
                        "Src Column":   scol,
                        "Src Value":    sv,
                        "Ref Row":      ref_ridx+1,
                        "Ref Column":   ref_col,
                        "Ref Value":    ref_raw,
                        "Match Detail": detail,
                        "Score %":      round(score*100,1),
                        "Match Type":   mtype,
                        "Confidence":   conf,
                    })
                done+=1
                if progress_cb and total>0: progress_cb(done/total)
                if status_cb and done%200==0:
                    status_cb(f"Matching… {done:,}/{total:,}  ·  {len(output):,} matches")

        if not output: return pd.DataFrame(columns=self.COLS)
        df=pd.DataFrame(output)
        df.sort_values("Score %",ascending=False,inplace=True)
        # Keep best match per (Src Row, Src Column) — avoids duplicating source rows
        df.drop_duplicates(subset=["Src Row","Src Column","Ref Row","Ref Column"],
                           keep="first",inplace=True)
        df.reset_index(drop=True,inplace=True)
        return df

def _confidence_label(score:float)->str:
    if score>=0.95: return "★★★  Definite"
    if score>=0.85: return "★★☆  High"
    if score>=0.75: return "★☆☆  Medium"
    return "⚡  Low"

# ══════════════════════════════════════════════════════════════════════════════
#  EXACT JOIN ENGINE  v4  (SQL-style, fully vectorised, handles 1M+ rows)
# ══════════════════════════════════════════════════════════════════════════════

class ExactJoinEngine:
    """
    High-performance exact-match engine using pandas merge.

    Architecture
    ────────────
    1. Normalise both files' join columns to lowercase / stripped strings.
    2. Pre-filter source to rows whose join key actually exists in the
       reference — eliminates the bulk of rows in O(n) with a set lookup.
    3. Inner-merge the filtered source chunk with the (small) reference on
       the normalised join key.  pandas merge is implemented in C and handles
       millions of rows in seconds.
    4. For each (src_search_col, ref_search_col) pair, apply vectorised
       comparisons over the merged frame:
           • Exact  — normalised string equality
           • Substring — one value contained in the other (numpy array loop,
             fast because the merged frame is typically small after join)
    5. Collect result rows, deduplicate, return.

    Progress
    ────────
    Source is processed in CHUNK_SIZE slices. After every chunk, progress_cb
    is called with a float in [0,1] and status_cb with a human-readable string.
    The stop_event is checked at the start of every chunk.
    """

    COLS = MatchEngine.COLS          # reuse same schema → results page unchanged
    CHUNK_SIZE = 500_000             # rows per chunk (tune for memory vs. granularity)

    def run(self,
            src_df:         pd.DataFrame,
            ref_df:         pd.DataFrame,
            src_join_col:   Optional[str],
            ref_join_col:   Optional[str],
            src_search_cols:List[str],
            ref_search_cols:List[str],
            include_substring: bool = True,
            stop_event:     Optional[threading.Event] = None,
            progress_cb     = None,
            status_cb       = None) -> pd.DataFrame:

        # ── Validate columns ──────────────────────────────────────────────────
        src_join_col = src_join_col if src_join_col and src_join_col in src_df.columns else None
        ref_join_col = ref_join_col if ref_join_col and ref_join_col in ref_df.columns else None
        src_s = [c for c in src_search_cols if c in src_df.columns]
        ref_s = [c for c in ref_search_cols if c in ref_df.columns]
        if not src_s or not ref_s:
            return pd.DataFrame(columns=self.COLS)

        # ── Prepare reference (normalise join key once, add _r_ prefix) ───────
        ref_w = ref_df.reset_index(drop=True).copy()
        ref_w.insert(0, "__ref_row__", range(1, len(ref_w)+1))
        if ref_join_col:
            ref_w["__jk__"] = (ref_w[ref_join_col]
                               .astype(str).str.lower().str.strip())
        else:
            ref_w["__jk__"] = "__all__"

        # Rename all ref columns with _r_ prefix to avoid collision after merge
        ref_rename = {c: f"_r_{c}" for c in ref_w.columns if not c.startswith("__")}
        ref_w = ref_w.rename(columns=ref_rename)
        ref_s_r = [f"_r_{c}" for c in ref_s]          # prefixed ref search col names
        ref_jcol_r = f"_r_{ref_join_col}" if ref_join_col else None

        # Set of valid join keys in ref (for fast pre-filter)
        valid_jk = set(ref_w["__jk__"].unique()) - {"", "nan", "none"}

        n_src    = len(src_df)
        all_rows:List[Dict] = []

        for i0 in range(0, n_src, self.CHUNK_SIZE):
            if stop_event and stop_event.is_set():
                break

            i1  = min(i0 + self.CHUNK_SIZE, n_src)
            pct = i1 / n_src

            # ── Slice and prefix source chunk ─────────────────────────────────
            chunk = src_df.iloc[i0:i1].reset_index(drop=True).copy()
            chunk.insert(0, "__src_row__", range(i0+1, i0+len(chunk)+1))
            if src_join_col:
                chunk["__jk__"] = (chunk[src_join_col]
                                   .astype(str).str.lower().str.strip())
            else:
                chunk["__jk__"] = "__all__"

            # Pre-filter: keep only rows whose join key appears in ref
            if src_join_col and valid_jk:
                chunk = chunk[chunk["__jk__"].isin(valid_jk)]

            if chunk.empty:
                if progress_cb: progress_cb(pct)
                if status_cb: status_cb(
                    f"Scanned {i1:,}/{n_src:,} rows  ·  "
                    f"{len(all_rows):,} matches  (no join keys matched in this block)")
                continue

            # Rename source columns with _s_ prefix
            src_rename = {c: f"_s_{c}" for c in chunk.columns if not c.startswith("__")}
            chunk = chunk.rename(columns=src_rename)
            src_s_s  = [f"_s_{c}" for c in src_s]          # prefixed src search cols
            src_jcol_s = f"_s_{src_join_col}" if src_join_col else None

            # ── Inner merge on join key ───────────────────────────────────────
            merged = chunk.merge(ref_w, on="__jk__", how="inner")
            if merged.empty:
                if progress_cb: progress_cb(pct)
                continue

            merged_len = len(merged)

            # ── Compare each search column pair ───────────────────────────────
            for sc_s in src_s_s:
                if sc_s not in merged.columns: continue
                sv = merged[sc_s].astype(str).str.lower().str.strip()
                sc_orig = sc_s[3:]          # strip _s_ prefix for labelling

                for rc_r in ref_s_r:
                    if rc_r not in merged.columns: continue
                    rv = merged[rc_r].astype(str).str.lower().str.strip()
                    rc_orig = rc_r[3:]      # strip _r_ prefix

                    # Exact match (vectorised)
                    exact_mask = (sv == rv) & (sv.str.len() >= 2)

                    # Substring match (numpy loop — fast for typical merged sizes)
                    if include_substring:
                        sa = sv.values; ra = rv.values
                        sub_arr = np.fromiter(
                            (len(a)>=2 and len(b)>=2 and a!=b and (a in b or b in a)
                             for a,b in zip(sa,ra)),
                            dtype=bool, count=merged_len)
                        match_mask = exact_mask | pd.Series(sub_arr, index=merged.index)
                    else:
                        match_mask = exact_mask

                    hits = merged[match_mask]
                    if hits.empty: continue

                    # ── Build result rows fully vectorised ────────────────────
                    h = hits.copy()
                    is_ex = (h[sc_s].str.lower().str.strip() ==
                             h[rc_r].str.lower().str.strip())

                    jv_s_col = (h[src_jcol_s].astype(str)
                                if src_jcol_s and src_jcol_s in h.columns
                                else pd.Series("", index=h.index))
                    jv_r_col = (h[ref_jcol_r].astype(str)
                                if ref_jcol_r and ref_jcol_r in h.columns
                                else h["__jk__"])

                    h["__mtype__"]  = np.where(is_ex, "Exact", "Substring")
                    h["__score__"]  = np.where(is_ex, 100.0,   92.0)
                    h["__conf__"]   = np.where(is_ex, "★★★  Definite", "★★☆  High")
                    h["__jvs__"]    = jv_s_col
                    h["__jvr__"]    = jv_r_col
                    h["__sv__"]     = h[sc_s].astype(str)
                    h["__rv__"]     = h[rc_r].astype(str)
                    h["__detail__"] = (
                        "JOIN: " + (src_join_col or "?") + "='" + h["__jvs__"] +
                        "' ↔ " + (ref_join_col or "?") + "='" + h["__jvr__"] +
                        "'  |  MATCH: " + sc_orig + " '" + h["__sv__"] +
                        "' = " + rc_orig + " '" + h["__rv__"] +
                        "'  [" + h["__mtype__"] + "]"
                    )

                    batch = pd.DataFrame({
                        "Src Row":      h["__src_row__"].values,
                        "Join Src Val": h["__jvs__"].values,
                        "Join Ref Val": h["__jvr__"].values,
                        "Src Column":   sc_orig,
                        "Src Value":    h["__sv__"].values,
                        "Ref Row":      h["__ref_row__"].values,
                        "Ref Column":   rc_orig,
                        "Ref Value":    h["__rv__"].values,
                        "Match Detail": h["__detail__"].values,
                        "Score %":      h["__score__"].values,
                        "Match Type":   h["__mtype__"].values,
                        "Confidence":   h["__conf__"].values,
                    })
                    all_rows.append(batch)

            if progress_cb: progress_cb(pct)
            if status_cb:
                status_cb(f"Processed {i1:,}/{n_src:,} rows  ·  {len(all_rows):,} matches found")

        if not all_rows:
            return pd.DataFrame(columns=self.COLS)

        df = pd.concat(all_rows, ignore_index=True)
        df.drop_duplicates(
            subset=["Src Row","Src Column","Ref Row","Ref Column"],
            keep="first", inplace=True)
        df.sort_values(["Join Src Val","Score %"], ascending=[True,False], inplace=True)
        df.reset_index(drop=True, inplace=True)
        return df

# ══════════════════════════════════════════════════════════════════════════════
#  GAP ANALYSER  v5  — finds what is MISSING between the two files
# ══════════════════════════════════════════════════════════════════════════════

class GapResult:
    """Container for gap-analysis output."""
    __slots__=(
        "src_label","ref_label",
        # Schema-level
        "schema_src_only","schema_ref_only","schema_matched",
        # Table-level (schema+table pair)
        "table_src_only","table_ref_only","table_matched",
        # Coverage numbers
        "src_schema_total","ref_schema_total",
        "src_table_total", "ref_table_total",
    )
    def __init__(self):
        for s in self.__slots__: setattr(self,s,None)


def compute_gaps(src_df:pd.DataFrame, ref_df:pd.DataFrame,
                 src_join_col:Optional[str], ref_join_col:Optional[str],
                 src_table_col:Optional[str], ref_table_col:Optional[str],
                 src_label:str="EDC (Source)",
                 ref_label:str="Collibra (Reference)") -> GapResult:
    """
    Perform a full-outer-join gap analysis at two granularities:

      Schema level  — which schema values exist only on one side?
      Table  level  — which (schema, table) pairs exist only on one side?

    Everything is normalised (lower-case, stripped) before comparison so
    casing differences do not create false gaps.

    Returns a GapResult with DataFrames and summary counts.
    """
    gr=GapResult()
    gr.src_label=src_label
    gr.ref_label=ref_label

    # ── Safe normalise: always returns a 1-D Series ───────────────────────────
    def norm_series(s) -> pd.Series:
        """Normalise a column value to lowercase stripped string Series.
        Robust against DataFrames with duplicate column names (squeeze first)."""
        if isinstance(s, pd.DataFrame):
            s = s.iloc[:, 0]        # take first column if accidentally a DataFrame
        s = pd.Series(s).astype(str)
        s = s.str.lower().str.strip()
        s = s.str.replace(r"\s+", " ", regex=True)
        return s

    # ── Schema-level gap ──────────────────────────────────────────────────────
    if src_join_col and src_join_col in src_df.columns and \
       ref_join_col and ref_join_col in ref_df.columns:

        src_schemas = norm_series(src_df[src_join_col]).unique()
        ref_schemas = norm_series(ref_df[ref_join_col]).unique()

        src_set = {s for s in src_schemas if s not in("","nan","none")}
        ref_set = {s for s in ref_schemas if s not in("","nan","none")}

        gr.src_schema_total = len(src_set)
        gr.ref_schema_total = len(ref_set)

        gr.schema_src_only = pd.DataFrame(
            {"Schema — In EDC, Missing from Collibra": sorted(src_set - ref_set)})
        gr.schema_ref_only = pd.DataFrame(
            {"Schema — In Collibra, Not in EDC": sorted(ref_set - src_set)})
        gr.schema_matched  = pd.DataFrame(
            {"Schema — Present in BOTH": sorted(src_set & ref_set)})
    else:
        for attr in ("schema_src_only","schema_ref_only","schema_matched"):
            setattr(gr, attr, pd.DataFrame())
        gr.src_schema_total = gr.ref_schema_total = 0

    # ── Table-level gap ───────────────────────────────────────────────────────
    # Guard: if table col same as join col, or if table col missing, skip
    src_tbl_valid = (src_table_col and src_table_col in src_df.columns
                     and src_table_col != src_join_col)
    ref_tbl_valid = (ref_table_col and ref_table_col in ref_df.columns
                     and ref_table_col != ref_join_col)

    if src_tbl_valid and ref_tbl_valid:

        # Build de-duplicated column lists (never include same col twice)
        src_sel = []
        if src_join_col and src_join_col in src_df.columns:
            src_sel.append(src_join_col)
        src_sel.append(src_table_col)

        ref_sel = []
        if ref_join_col and ref_join_col in ref_df.columns:
            ref_sel.append(ref_join_col)
        ref_sel.append(ref_table_col)

        # Build working frames with only those two columns, normalised
        src_t = src_df[src_sel].copy().reset_index(drop=True)
        ref_t = ref_df[ref_sel].copy().reset_index(drop=True)

        for c in src_sel:
            src_t[c] = norm_series(src_df[c])   # use original df to avoid dup issues
        for c in ref_sel:
            ref_t[c] = norm_series(ref_df[c])

        # Drop blanks and deduplicate
        src_t = src_t[norm_series(src_t[src_table_col]).isin(
            {s for s in norm_series(src_t[src_table_col]) if s not in("","nan","none")}
        )].drop_duplicates()
        ref_t = ref_t[norm_series(ref_t[ref_table_col]).isin(
            {s for s in norm_series(ref_t[ref_table_col]) if s not in("","nan","none")}
        )].drop_duplicates()

        gr.src_table_total = len(src_t)
        gr.ref_table_total = len(ref_t)

        # Rename to universal names for merging
        has_schema = len(src_sel) == 2     # both files have schema + table cols
        if has_schema:
            src_t = src_t.rename(columns={src_join_col:"__schema__", src_table_col:"__table__"})
            ref_t = ref_t.rename(columns={ref_join_col:"__schema__", ref_table_col:"__table__"})
            join_on = ["__schema__","__table__"]
        else:
            src_t = src_t.rename(columns={src_table_col:"__table__"})
            ref_t = ref_t.rename(columns={ref_table_col:"__table__"})
            join_on = ["__table__"]

        src_t["__in_src__"] = True
        ref_t["__in_ref__"] = True

        merged = src_t.merge(ref_t, on=join_on, how="outer")
        in_src = merged["__in_src__"].fillna(False).astype(bool)
        in_ref = merged["__in_ref__"].fillna(False).astype(bool)

        def _fmt_gap(sub, gap_label) -> pd.DataFrame:
            cols_out = ["Schema","Table"] if has_schema else ["Table"]
            map_from = join_on
            out = sub[map_from].copy()
            out.columns = cols_out
            out.insert(0, "Gap Type", gap_label)
            out = out.fillna("").sort_values(cols_out).reset_index(drop=True)
            return out

        gr.table_src_only = _fmt_gap(
            merged[in_src & ~in_ref],
            f"In {src_label}  — NOT in {ref_label}")
        gr.table_ref_only = _fmt_gap(
            merged[~in_src & in_ref],
            f"In {ref_label}  — NOT in {src_label}")
        gr.table_matched  = _fmt_gap(
            merged[in_src & in_ref],
            "Present in BOTH")
    else:
        for attr in ("table_src_only","table_ref_only","table_matched"):
            setattr(gr, attr, pd.DataFrame())
        gr.src_table_total = gr.ref_table_total = 0

    return gr

# ══════════════════════════════════════════════════════════════════════════════
#  SHARED WIDGET HELPERS
# ══════════════════════════════════════════════════════════════════════════════

def _ttk_style():
    s=ttk.Style()
    try: s.theme_use("clam")
    except: pass
    s.configure("DF.Treeview",background=C["card"],foreground=C["text"],
                fieldbackground=C["card"],rowheight=24,font=Fs,
                bordercolor=C["border"],relief="flat")
    s.configure("DF.Treeview.Heading",background=C["navy"],foreground=C["text_inv"],
                font=FsB,relief="flat",padding=(5,4))
    s.map("DF.Treeview",background=[("selected",C["blue_lt"])],
          foreground=[("selected",C["blue_dk"])])

def make_tree(parent,columns:List[str],widths:Optional[List[int]]=None)->ttk.Treeview:
    _ttk_style()
    vsb=ttk.Scrollbar(parent,orient="vertical")
    hsb=ttk.Scrollbar(parent,orient="horizontal")
    tree=ttk.Treeview(parent,columns=columns,show="headings",style="DF.Treeview",
                      yscrollcommand=vsb.set,xscrollcommand=hsb.set,selectmode="browse")
    vsb.configure(command=tree.yview); hsb.configure(command=tree.xview)
    dw=max(80,1100//max(len(columns),1))
    for i,col in enumerate(columns):
        w=widths[i] if widths and i<len(widths) else dw
        tree.heading(col,text=col,anchor="w")
        tree.column(col,width=w,minwidth=40,anchor="w",stretch=True)
    vsb.pack(side="right",fill="y"); hsb.pack(side="bottom",fill="x")
    tree.pack(fill="both",expand=True)
    for tag,bg,fg in [("odd",C["stripe"],C["text"]),("even",C["card"],C["text"]),
                      ("Exact","#F0FDF4","#166534"),("Substring",C["blue_lt"],C["blue_dk"]),
                      ("Token Overlap",C["amber_lt"],C["amber"]),
                      ("Fuzzy",C["red_lt"],C["red"]),
                      ("key",C["teal_lt"],C["teal"]),("hier","#FFF7ED","#9A3412")]:
        tree.tag_configure(tag,background=bg,foreground=fg)
    return tree

def fill_tree(tree:ttk.Treeview,df:pd.DataFrame,max_rows:int=500,tag_col:Optional[str]=None):
    tree.delete(*tree.get_children())
    cols=list(df.columns)
    tree["columns"]=cols
    cw=max(70,min(200,1050//max(len(cols),1)))
    for c in cols:
        tree.heading(c,text=c,anchor="w")
        tree.column(c,width=cw,minwidth=40,anchor="w")
    for i,(_,row) in enumerate(df.head(max_rows).iterrows()):
        vals=[str(row[c])[:200] for c in cols]
        tag=str(row[tag_col]) if tag_col and tag_col in cols else ("odd" if i%2 else "even")
        tree.insert("","end",values=vals,tags=(tag,))
    if len(df)>max_rows:
        tree.insert("","end",values=[f"  ⋯  {len(df)-max_rows:,} more rows not shown"]+[""]*(len(cols)-1))

def _checklist(parent,items:List[str],checked:bool=True,height:int=160,
               bg:str=C["card"],accent:str=C["blue_lt"])->Tuple[tk.Frame,Dict[str,tk.BooleanVar]]:
    outer=tk.Frame(parent,bg=bg,highlightthickness=1,highlightbackground=C["border"])
    canvas=tk.Canvas(outer,bg=bg,bd=0,highlightthickness=0,height=height)
    vsb=ttk.Scrollbar(outer,orient="vertical",command=canvas.yview)
    canvas.configure(yscrollcommand=vsb.set)
    vsb.pack(side="right",fill="y"); canvas.pack(side="left",fill="both",expand=True)
    inner=tk.Frame(canvas,bg=bg)
    win=canvas.create_window((0,0),window=inner,anchor="nw")
    def _resize(e):
        canvas.configure(scrollregion=canvas.bbox("all"))
        canvas.itemconfig(win,width=e.width)
    inner.bind("<Configure>",_resize)
    canvas.bind("<Configure>",lambda e:canvas.itemconfig(win,width=e.width))
    vars_:Dict[str,tk.BooleanVar]={}
    for item in items:
        v=tk.BooleanVar(value=checked); vars_[item]=v
        tk.Checkbutton(inner,text=item[:54],variable=v,bg=bg,fg=C["text"],
                       font=Fs,activebackground=bg,selectcolor=accent,
                       anchor="w").pack(fill="x",padx=6,pady=1)
    return outer,vars_

def _lbl(parent,text,bg,fg,font,**kw):
    return tk.Label(parent,text=text,bg=bg,fg=fg,font=font,**kw)

def _sep(parent,bg=C["border"],height=1):
    tk.Frame(parent,bg=bg,height=height).pack(fill="x",pady=6)

def _card(parent,**kw)->tk.Frame:
    return tk.Frame(parent,bg=C["card"],
                    highlightthickness=1,highlightbackground=C["border"],**kw)

# ══════════════════════════════════════════════════════════════════════════════
#  APPLICATION
# ══════════════════════════════════════════════════════════════════════════════

class App(ctk.CTk):
    def __init__(self):
        super().__init__()
        self.title("DataForge Pro  v3"); self.geometry("1480x920")
        self.minsize(1200,740); self.configure(fg_color=C["bg"])

        # Data state
        self.src_path=self.ref_path=None
        self.src_df=self.ref_df=self.results_df=None
        self.gap_result:Optional[GapResult]=None
        self._stop_event=threading.Event(); self._step=0

        # New v5 validation state
        self._val_srz_only:Optional[pd.DataFrame]=None
        self._val_edc_only:Optional[pd.DataFrame]=None
        self._val_sch_matched:List[str]=[]
        self._val_sch_srz_only:List[str]=[]
        self._val_sch_edc_only:List[str]=[]
        self._val_ref_nm:str="SRZ"
        self._val_src_nm:str="EDC"
        self._val_ref_total:int=0
        self._val_src_total:int=0

        # Config state (column mapping)
        self._hier_col_var  = tk.StringVar()
        self._ref_schema_var= tk.StringVar()
        self._ref_table_var = tk.StringVar()
        self._ref_asset_var = tk.StringVar(value="(none)")
        self._src_schema_var= tk.StringVar()
        self._src_table_var = tk.StringVar()
        self._use_pipe      = tk.BooleanVar(value=True)
        self._use_pipe_ref  = tk.BooleanVar(value=False)
        # Keep these for any remaining references in non-replaced code
        self._src_join_var  = tk.StringVar()
        self._ref_join_var  = tk.StringVar()
        self._match_mode    = tk.StringVar(value="exact")
        self._thresh_var    = tk.DoubleVar(value=0.70)
        self._substr_var    = tk.BooleanVar(value=True)
        self._src_search_vars:Dict[str,tk.BooleanVar]={}
        self._ref_search_vars:Dict[str,tk.BooleanVar]={}
        self._prog_var      = tk.StringVar(value="")
        self._prog_detail   = tk.StringVar(value="")

        # Preview search state
        self._prev_search_var = tk.StringVar()
        self._prev_search_var.trace_add("write",self._on_prev_search)
        self._prev_df_cache:Optional[pd.DataFrame]=None

        self._build_header(); self._build_step_bar()
        self._content=tk.Frame(self,bg=C["bg"])
        self._content.pack(fill="both",expand=True)
        self._build_status_bar()
        self._pages:Dict[int,tk.Frame]={}
        self._build_upload(); self._build_preview()
        self._build_configure(); self._build_results()
        self._goto(0)

    # ── Chrome ────────────────────────────────────────────────────────────────
    def _build_header(self):
        bar=tk.Frame(self,bg=C["navy"],height=56); bar.pack(fill="x"); bar.pack_propagate(False)
        tk.Frame(bar,bg=C["blue"],width=4).pack(side="left",fill="y")
        inner=tk.Frame(bar,bg=C["navy"]); inner.pack(side="left",fill="both",expand=True,padx=18)
        _lbl(inner,"DataForge Pro",C["navy"],"#FFF",FD).pack(side="left",pady=10)
        _lbl(inner,"  ·  Data Cleaning & Intelligent Cross-File Matching",
             C["navy"],"#7CA8CC",FB).pack(side="left",pady=10)
        right=tk.Frame(bar,bg=C["navy"]); right.pack(side="right",padx=18)
        self._busy_lbl=_lbl(right,"",C["navy"],C["blue_mid"],FsB)
        self._busy_lbl.pack(side="right",padx=(8,0))
        _lbl(right,"v3.0",C["navy2"],"#7CA8CC",FBG,padx=8,pady=3).pack(side="right")

    def _build_step_bar(self):
        bar=_card(self); bar.pack(fill="x")
        self._step_c:List[tk.Label]=[]; self._step_l:List[tk.Label]=[]
        STEPS=[("1","Upload"),("2","Preview"),("3","Configure"),("4","Results")]
        inner=tk.Frame(bar,bg=C["card"]); inner.pack(pady=9)
        for i,(num,lbl) in enumerate(STEPS):
            if i: tk.Frame(inner,bg=C["border2"],width=40,height=2).pack(side="left",pady=6)
            cell=tk.Frame(inner,bg=C["card"]); cell.pack(side="left",padx=6)
            c=tk.Label(cell,text=num,bg=C["text3"],fg="#FFF",font=FsB,width=3,pady=1); c.pack(side="left")
            l=tk.Label(cell,text=f"  {lbl}",bg=C["card"],fg=C["text3"],font=Fs); l.pack(side="left")
            self._step_c.append(c); self._step_l.append(l)

    def _update_steps(self,active:int):
        for i,(c,l) in enumerate(zip(self._step_c,self._step_l)):
            if i<active:   c.configure(bg=C["teal"]); l.configure(fg=C["teal"],font=Fs)
            elif i==active:c.configure(bg=C["blue"]); l.configure(fg=C["blue"],font=FsB)
            else:          c.configure(bg=C["text3"]);l.configure(fg=C["text3"],font=Fs)

    def _build_status_bar(self):
        bar=_card(self); bar.configure(height=27); bar.pack(fill="x",side="bottom"); bar.pack_propagate(False)
        self._status_var=tk.StringVar(value="Ready — upload your files to begin.")
        tk.Label(bar,textvariable=self._status_var,bg=C["card"],fg=C["text2"],
                 font=Fs,anchor="w",padx=12).pack(fill="y",side="left")

    def _goto(self,step:int):
        for p in self._pages.values(): p.pack_forget()
        self._pages[step].pack(fill="both",expand=True)
        self._step=step; self._update_steps(step)

    def _status(self,msg:str): self._status_var.set(msg)
    def _busy(self,msg:str=""): self._busy_lbl.configure(text=f"⟳ {msg}" if msg else "")

    # ══════════════════════════════════════════════════════════════════════════
    #  PAGE 0 — UPLOAD
    # ══════════════════════════════════════════════════════════════════════════
    def _build_upload(self):
        pg=tk.Frame(self._content,bg=C["bg"]); self._pages[0]=pg
        _lbl(pg,"Upload Your Files",C["bg"],C["text"],FH).pack(pady=(22,3))
        _lbl(pg,"Select the pipe-delimited source and the reference file.",
             C["bg"],C["text2"],FB).pack()
        row=tk.Frame(pg,bg=C["bg"]); row.pack(pady=20,padx=40,fill="x")
        row.columnconfigure(0,weight=1); row.columnconfigure(1,weight=1)

        def _upload_card(parent,col,emoji,title,subtitle,color,browse_cmd,info_attr,lbl_attr):
            c=_card(parent,padx=26,pady=20)
            c.grid(row=0,column=col,padx=(0,12) if col==0 else (12,0),sticky="nsew")
            _lbl(c,emoji,C["card"],C["text"],("Helvetica",34)).pack()
            _lbl(c,title,C["card"],C["text"],FS).pack(pady=(6,2))
            _lbl(c,subtitle,C["card"],C["text2"],Fs,justify="center").pack()
            if col==0:
                tk.Checkbutton(c,text="First row has pipe-separated column headers",
                               variable=self._use_pipe,bg=C["card"],fg=C["text2"],
                               font=Fs,activebackground=C["card"],
                               selectcolor=C["blue_lt"]).pack(pady=(8,0))
            else:
                tk.Checkbutton(c,text="Reference file also has pipe-separated headers",
                               variable=self._use_pipe_ref,bg=C["card"],fg=C["text2"],
                               font=Fs,activebackground=C["card"],
                               selectcolor=C["teal_lt"]).pack(pady=(8,0))
            btn=tk.Button(c,text=f"Browse {title}…",bg=color,fg=C["text_inv"],
                          font=FBB,relief="flat",padx=16,pady=8,cursor="hand2",
                          command=browse_cmd)
            btn.pack(pady=(12,4))
            v=tk.StringVar(value="No file selected")
            setattr(self,info_attr,v)
            lbl=_lbl(c,None,C["card"],C["text2"],Fs,textvariable=v,wraplength=290)
            lbl.pack(); setattr(self,lbl_attr,lbl)

        _upload_card(row,0,"📄","Source File","Pipe-delimited · headers in row 1",
                     C["blue"],self._browse_src,"_src_info","_src_info_lbl")
        _upload_card(row,1,"📊","Reference File","Excel, CSV, TXT, TSV — auto-detected",
                     C["teal"],self._browse_ref,"_ref_info","_ref_info_lbl")

        self._parse_btn=tk.Button(pg,text="Parse & Clean  →",
                                  bg=C["text3"],fg=C["text_inv"],font=FBB,
                                  relief="flat",padx=26,pady=10,cursor="hand2",
                                  state="disabled",command=self._do_parse)
        self._parse_btn.pack(pady=8)

    def _browse_src(self):
        p=filedialog.askopenfilename(title="Select Source File",
            filetypes=[("All supported","*.csv *.txt *.tsv *.xlsx *.xls *.xlsm"),
                       ("All files","*.*")])
        if p:
            self.src_path=p
            self._src_info.set(f"✓  {Path(p).name}  ({os.path.getsize(p)/1024:.1f} KB)")
            self._src_info_lbl.configure(fg=C["green"]); self._check_ready()

    def _browse_ref(self):
        p=filedialog.askopenfilename(title="Select Reference File",
            filetypes=[("All supported","*.csv *.txt *.tsv *.xlsx *.xls *.xlsm"),
                       ("All files","*.*")])
        if p:
            self.ref_path=p
            self._ref_info.set(f"✓  {Path(p).name}  ({os.path.getsize(p)/1024:.1f} KB)")
            self._check_ready()

    def _check_ready(self):
        if self.src_path and self.ref_path:
            self._parse_btn.configure(state="normal",bg=C["blue"])

    def _do_parse(self):
        self._parse_btn.configure(state="disabled",text="Parsing…")
        self._busy("Parsing…")
        threading.Thread(target=self._parse_worker,daemon=True).start()

    def _parse_worker(self):
        try:
            src=(parse_pipe_source(self.src_path) if self._use_pipe.get()
                 else smart_load(self.src_path)[0])
            src=src.apply(lambda c:c.map(lambda x:str(x).strip()))
            src=src.loc[:,(src!="").any(axis=0)]; src=src.loc[:,~src.columns.duplicated()]

            # Reference — apply same pipe-parse if the user checked it
            if self._use_pipe_ref.get():
                ref=parse_pipe_source(self.ref_path)
                ref_m="pipe-delimited (parsed)"
            else:
                ref,ref_m=smart_load(self.ref_path)
            ref=ref.apply(lambda c:c.map(lambda x:str(x).strip()))
            ref=ref.loc[:,(ref!="").any(axis=0)]; ref=ref.loc[:,~ref.columns.duplicated()]

            self.src_df=src; self.ref_df=ref
            self.after(0,lambda:self._parse_done(ref_m))
        except Exception as e:
            import traceback; traceback.print_exc()
            self.after(0,lambda ex=e:self._on_error("Parse Error",ex))

    def _parse_done(self,ref_m:str):
        self._parse_btn.configure(state="normal",text="Parse & Clean  →",bg=C["blue"])
        self._busy()
        self._status(f"Source: {len(self.src_df):,}×{len(self.src_df.columns)}  ·  "
                     f"Reference ({ref_m}): {len(self.ref_df):,}×{len(self.ref_df.columns)}")
        self._populate_preview(); self._goto(1)

    # ══════════════════════════════════════════════════════════════════════════
    #  PAGE 1 — PREVIEW  (with search bar — NEW v3)
    # ══════════════════════════════════════════════════════════════════════════
    def _build_preview(self):
        pg=tk.Frame(self._content,bg=C["bg"]); self._pages[1]=pg

        # Top nav
        top=tk.Frame(pg,bg=C["bg"]); top.pack(fill="x",padx=20,pady=(14,4))
        _lbl(top,"Clean & Preview",C["bg"],C["text"],FH).pack(side="left")
        rbar=tk.Frame(top,bg=C["bg"]); rbar.pack(side="right")
        _back(rbar,"← Upload",lambda:self._goto(0))
        _navbtn(rbar,"Configure  →",self._go_configure,C["blue"])

        # Stats line
        self._prev_stats=tk.StringVar(value="")
        _lbl(pg,None,C["bg"],C["text2"],Fs,textvariable=self._prev_stats).pack(padx=20,anchor="w")

        # Tab bar + search bar in same row
        ctrl=_card(pg); ctrl.pack(fill="x",padx=20,pady=(4,0))
        self._prev_tab=tk.StringVar(value="source")
        for txt,val in [("✓ Source (Cleaned)","source"),("Reference File","ref")]:
            tk.Radiobutton(ctrl,text=txt,variable=self._prev_tab,value=val,
                           bg=C["card"],fg=C["text"],selectcolor=C["blue_lt"],
                           font=FB,activebackground=C["card"],
                           command=self._switch_prev_tab).pack(side="left",padx=14,pady=7)

        # Search entry
        tk.Frame(ctrl,bg=C["border"],width=1).pack(side="left",fill="y",pady=4)
        _lbl(ctrl,"  🔍 Search:",C["card"],C["text2"],Fs).pack(side="left")
        self._prev_search_entry=tk.Entry(ctrl,textvariable=self._prev_search_var,
                                          bg=C["stripe"],fg=C["text"],
                                          font=Fs,relief="flat",width=28)
        self._prev_search_entry.pack(side="left",padx=(4,8),pady=7)
        tk.Button(ctrl,text="✕",bg=C["card"],fg=C["text3"],font=FBG,relief="flat",
                  padx=4,command=lambda:self._prev_search_var.set("")
                  ).pack(side="left")
        self._prev_match_lbl=_lbl(ctrl,"",C["card"],C["text3"],Fs)
        self._prev_match_lbl.pack(side="left",padx=8)

        # Tree
        th=tk.Frame(pg,bg=C["bg"]); th.pack(fill="both",expand=True,padx=20,pady=(4,6))
        self._prev_tree=make_tree(th,["Loading…"])

    def _populate_preview(self): self._switch_prev_tab()

    def _switch_prev_tab(self,*_):
        tab=self._prev_tab.get()
        df=self.src_df if tab=="source" else self.ref_df
        if df is None: return
        self._prev_df_cache=df
        self._prev_stats.set(
            f"{len(df):,} rows  ·  {len(df.columns)} cols  ·  "
            f"{int(df.apply(lambda c:(c!='').sum()).sum()):,} non-empty cells")
        self._apply_prev_search(df)

    def _on_prev_search(self,*_):
        if self._prev_df_cache is not None:
            self._apply_prev_search(self._prev_df_cache)

    def _apply_prev_search(self,df:pd.DataFrame):
        q=self._prev_search_var.get().strip().lower()
        if q:
            mask=df.apply(lambda r:r.astype(str).str.lower().str.contains(
                re.escape(q),regex=False).any(),axis=1)
            filtered=df[mask]
            self._prev_match_lbl.configure(
                text=f"{len(filtered):,} of {len(df):,} rows match")
        else:
            filtered=df; self._prev_match_lbl.configure(text="")
        fill_tree(self._prev_tree,filtered,max_rows=400)

    def _go_configure(self):
        self._populate_configure(); self._goto(2)

    # ══════════════════════════════════════════════════════════════════════════
    #  PAGE 2 — CONFIGURE  (Full-Name parser + Join selector — NEW v3)
    # ══════════════════════════════════════════════════════════════════════════
    # ══════════════════════════════════════════════════════════════════════════
    #  PAGE 2 — CONFIGURE  (v5: simple column mapping, no fuzzy settings)
    # ══════════════════════════════════════════════════════════════════════════
    def _build_configure(self):
        pg=tk.Frame(self._content,bg=C["bg"]); self._pages[2]=pg

        # Top nav
        top=tk.Frame(pg,bg=C["bg"]); top.pack(fill="x",padx=20,pady=(12,6))
        _lbl(top,"Configure Validation",C["bg"],C["text"],FH).pack(side="left")
        rbar=tk.Frame(top,bg=C["bg"]); rbar.pack(side="right")
        _back(rbar,"← Preview",lambda:self._goto(1))
        self._stop_btn=tk.Button(rbar,text="⏹  Stop",bg=C["red"],fg=C["text_inv"],
                                 font=FBB,relief="flat",padx=14,pady=6,cursor="hand2",
                                 command=self._do_stop)
        self._run_btn=tk.Button(rbar,text="▶  Run Validation",
                                bg=C["teal"],fg=C["text_inv"],font=FBB,
                                relief="flat",padx=18,pady=6,cursor="hand2",
                                command=self._do_match)
        self._run_btn.pack(side="left",padx=6)

        outer=tk.Frame(pg,bg=C["bg"]); outer.pack(fill="both",expand=True,padx=20,pady=(0,8))

        # ── Banner: what this page does ───────────────────────────────────────
        banner=_card(outer,padx=18,pady=14); banner.pack(fill="x",pady=(0,10))
        brow=tk.Frame(banner,bg=C["card"]); brow.pack(fill="x")
        tk.Frame(brow,bg=C["blue"],width=4).pack(side="left",fill="y",padx=(0,12))
        btxt=tk.Frame(brow,bg=C["card"]); btxt.pack(side="left",fill="both",expand=True)
        _lbl(btxt,"How Validation Works",C["card"],C["text"],FS).pack(anchor="w")
        _lbl(btxt,
             "Map the Schema column and Table column from each file. "
             "The engine builds exact sets from each side and computes three groups:\n"
             "  ✅  Matched — schema+table pairs present in BOTH files\n"
             "  ⚠   In SRZ only — present in SRZ reference but MISSING from EDC source  ← gaps to fix\n"
             "  ℹ   In EDC only — in EDC source but not in SRZ reference",
             C["card"],C["text2"],Fs,justify="left",wraplength=900).pack(anchor="w",pady=(4,0))

        # ── Optional: Full-Name parser (ref side) ─────────────────────────────
        hier_card=_card(outer,padx=16,pady=10); hier_card.pack(fill="x",pady=(0,10))
        hrow=tk.Frame(hier_card,bg=C["card"]); hrow.pack(fill="x")
        _lbl(hrow,"📐  Parse Hierarchical Column (optional)",C["card"],C["slate"],FsB).pack(side="left")
        _lbl(hrow,"  Splits  ZONE>DB>SCHEMA>TABLE  strings into separate columns",
             C["card"],C["text3"],Fs).pack(side="left",padx=10)
        hrow2=tk.Frame(hier_card,bg=C["card"]); hrow2.pack(fill="x",pady=(6,0))
        _lbl(hrow2,"Column to parse:",C["card"],C["text"],Fs).pack(side="left")
        self._hier_col_cb=ttk.Combobox(hrow2,textvariable=self._hier_col_var,
                                        state="readonly",font=Fs,width=30)
        self._hier_col_cb.pack(side="left",padx=8)
        tk.Button(hrow2,text="Parse Now",bg=C["amber_lt"],fg=C["amber"],font=FsB,
                  relief="flat",padx=10,pady=3,cursor="hand2",
                  command=self._do_parse_hier).pack(side="left")
        self._hier_status=_lbl(hrow2,"",C["card"],C["teal"],Fs); self._hier_status.pack(side="left",padx=10)

        # ── Column Mapping ────────────────────────────────────────────────────
        map_card=_card(outer,padx=18,pady=16); map_card.pack(fill="x",pady=(0,10))
        _lbl(map_card,"Column Mapping",C["card"],C["text"],FS).pack(anchor="w")
        _sep(map_card)

        # Two-column header labels
        hdr_row=tk.Frame(map_card,bg=C["card"]); hdr_row.pack(fill="x",pady=(0,8))
        tk.Frame(hdr_row,bg=C["card"],width=160).pack(side="left")  # label spacer
        lh=tk.Frame(hdr_row,bg=C["teal"],padx=10,pady=4); lh.pack(side="left")
        _lbl(lh,"SRZ Reference File  (the authoritative list)",C["teal"],C["text_inv"],FsB).pack()
        _lbl(hdr_row,"  ↔  ",C["card"],C["text2"],FBB).pack(side="left")
        rh=tk.Frame(hdr_row,bg=C["blue"],padx=10,pady=4); rh.pack(side="left")
        _lbl(rh,"EDC Source File  (what was loaded)",C["blue"],C["text_inv"],FsB).pack()

        # Schema row
        sr=tk.Frame(map_card,bg=C["card"]); sr.pack(fill="x",pady=5)
        _lbl(sr,"Schema column:",C["card"],C["slate"],FBB,width=20,anchor="w").pack(side="left")
        self._ref_schema_var=tk.StringVar()
        self._ref_schema_cb=ttk.Combobox(sr,textvariable=self._ref_schema_var,
                                          state="readonly",font=Fs,width=30)
        self._ref_schema_cb.pack(side="left",padx=(0,6))
        _lbl(sr,"  ↔  ",C["card"],C["text2"],FBB).pack(side="left")
        self._src_schema_var=tk.StringVar()
        self._src_schema_cb=ttk.Combobox(sr,textvariable=self._src_schema_var,
                                          state="readonly",font=Fs,width=30)
        self._src_schema_cb.pack(side="left",padx=(6,0))

        # Table row
        tr2=tk.Frame(map_card,bg=C["card"]); tr2.pack(fill="x",pady=5)
        _lbl(tr2,"Table column:",C["card"],C["slate"],FBB,width=20,anchor="w").pack(side="left")
        self._ref_table_var=tk.StringVar()
        self._ref_table_cb=ttk.Combobox(tr2,textvariable=self._ref_table_var,
                                         state="readonly",font=Fs,width=30)
        self._ref_table_cb.pack(side="left",padx=(0,6))
        _lbl(tr2,"  ↔  ",C["card"],C["text2"],FBB).pack(side="left")
        self._src_table_var=tk.StringVar()
        self._src_table_cb=ttk.Combobox(tr2,textvariable=self._src_table_var,
                                         state="readonly",font=Fs,width=30)
        self._src_table_cb.pack(side="left",padx=(6,0))

        # Asset / carry-through column row (SRZ side only)
        ar=tk.Frame(map_card,bg=C["card"]); ar.pack(fill="x",pady=5)
        _lbl(ar,"Asset column\n(optional carry-through):",C["card"],C["slate"],FBB,
             width=20,anchor="w",justify="left").pack(side="left")
        self._ref_asset_var=tk.StringVar(value="(none)")
        self._ref_asset_cb=ttk.Combobox(ar,textvariable=self._ref_asset_var,
                                          state="readonly",font=Fs,width=30)
        self._ref_asset_cb.pack(side="left",padx=(0,6))
        _lbl(ar,"  ←  included in results for side-by-side comparison",
             C["card"],C["text3"],Fs).pack(side="left",padx=(6,0))

        _lbl(map_card,
             "Tip: for SRZ 'Name' column (col A) → select it as the Table column. "
             "For SRZ 'Asset' column (col D) → use as Schema. "
             "Case differences are ignored automatically.",
             C["card"],C["text3"],Fs,wraplength=880).pack(anchor="w",pady=(8,0))

        # ── Progress ──────────────────────────────────────────────────────────
        prog_card=_card(outer,padx=18,pady=12); prog_card.pack(fill="x")
        _lbl(prog_card,"Progress",C["card"],C["text"],FBB).pack(anchor="w")
        self._prog_var   =tk.StringVar(value="Ready — click Run Validation to start.")
        self._prog_detail=tk.StringVar(value="")
        tk.Label(prog_card,textvariable=self._prog_var,
                 bg=C["card"],fg=C["text2"],font=FsB).pack(anchor="w",pady=(4,0))
        self._prog_bar=ctk.CTkProgressBar(prog_card,width=500,height=12,
                                           corner_radius=4,
                                           progress_color=C["teal"],
                                           fg_color=C["border"])
        self._prog_bar.set(0); self._prog_bar.pack(pady=(4,2),anchor="w")
        tk.Label(prog_card,textvariable=self._prog_detail,
                 bg=C["card"],fg=C["text3"],font=Fs,wraplength=600,
                 justify="left").pack(anchor="w")

    def _do_parse_hier(self):
        col=self._hier_col_var.get()
        if not col or self.ref_df is None: return
        try:
            self.ref_df=parse_hierarchical_column(self.ref_df,col)
            new_cols=[c for c in self.ref_df.columns if c.startswith("parsed_")]
            self._hier_status.configure(text=f"✓  Added: {', '.join(new_cols)}")
            self._populate_configure()
            self._status(f"Parsed '{col}' → {len(new_cols)} new columns")
        except Exception as e:
            messagebox.showerror("Parse Error",str(e))

    def _populate_configure(self):
        if self.src_df is None or self.ref_df is None: return
        src_cols=list(self.src_df.columns)
        ref_cols=list(self.ref_df.columns)

        # Hier detector
        hier_cands=detect_hierarchical_columns(self.ref_df)
        self._hier_col_cb["values"]=ref_cols
        if not self._hier_col_var.get() or self._hier_col_var.get() not in ref_cols:
            self._hier_col_var.set(hier_cands[0] if hier_cands else ref_cols[0])

        # SRZ (ref) schema column default → parsed_schema, or col containing "asset","schema","malcode"
        self._ref_schema_cb["values"]=ref_cols
        def _best(cols,patterns,fallback):
            for p in patterns:
                c=next((x for x in cols if re.search(p,x,re.I)),None)
                if c: return c
            return fallback
        ref_schema_dflt=_best(ref_cols,
            [r"^asset$",r"parsed_schema",r"schema",r"mal.?code",r"community"],
            ref_cols[0] if ref_cols else "")
        if not self._ref_schema_var.get() or self._ref_schema_var.get() not in ref_cols:
            self._ref_schema_var.set(ref_schema_dflt)

        # SRZ (ref) table column default → parsed_table, Name, or col with "table","name"
        self._ref_table_cb["values"]=ref_cols
        ref_table_dflt=_best(ref_cols,
            [r"^name$",r"parsed_table",r"table.?name",r"asset.?name",r"object"],
            ref_cols[min(1,len(ref_cols)-1)] if ref_cols else "")
        if not self._ref_table_var.get() or self._ref_table_var.get() not in ref_cols:
            self._ref_table_var.set(ref_table_dflt)

        # EDC (src) schema column
        self._src_schema_cb["values"]=src_cols
        src_schema_dflt=_best(src_cols,
            [r"schema.?name",r"^schema$",r"mal.?code"],
            src_cols[0] if src_cols else "")
        if not self._src_schema_var.get() or self._src_schema_var.get() not in src_cols:
            self._src_schema_var.set(src_schema_dflt)

        # EDC (src) table column
        self._src_table_cb["values"]=src_cols
        src_table_dflt=_best(src_cols,
            [r"table.?name",r"^table$",r"object.?name"],
            src_cols[min(1,len(src_cols)-1)] if src_cols else "")
        if not self._src_table_var.get() or self._src_table_var.get() not in src_cols:
            self._src_table_var.set(src_table_dflt)

        # SRZ Asset / carry-through column (optional)
        self._ref_asset_cb["values"]=["(none)"]+ref_cols
        asset_dflt=_best(ref_cols,
            [r"^asset$",r"asset.?type",r"type",r"domain",r"community"],
            "(none)")
        if not self._ref_asset_var.get() or self._ref_asset_var.get() not in (["(none)"]+ref_cols):
            self._ref_asset_var.set(asset_dflt if asset_dflt in ref_cols else "(none)")

    # ── Run / Stop ────────────────────────────────────────────────────────────
    def _do_match(self):
        ref_schema=self._ref_schema_var.get()
        ref_table =self._ref_table_var.get()
        src_schema=self._src_schema_var.get()
        src_table =self._src_table_var.get()

        if not all([ref_schema,ref_table,src_schema,src_table]):
            messagebox.showwarning("Configuration",
                "Please select Schema and Table columns for both files."); return
        if ref_schema not in self.ref_df.columns:
            messagebox.showwarning("Configuration",
                f"Column '{ref_schema}' not found in Reference file."); return
        if src_table not in self.src_df.columns:
            messagebox.showwarning("Configuration",
                f"Column '{src_table}' not found in Source file."); return

        self._stop_event.clear()
        self._run_btn.pack_forget(); self._stop_btn.pack(side="left",padx=6)
        self._prog_bar.set(0)
        self._prog_bar.configure(progress_color=C["teal"])
        self._prog_var.set("Starting validation…"); self._prog_detail.set("")
        self._busy("Validating…")

        threading.Thread(
            target=self._match_worker,
            args=(ref_schema,ref_table,src_schema,src_table),
            daemon=True).start()

    def _do_stop(self):
        self._stop_event.set()
        self._stop_btn.configure(state="disabled",text="Stopping…")
        self._status("Stop requested…")


    # ════════════════════════════════════════════════════════════════════════
    #  CORE VALIDATION ENGINE
    # ════════════════════════════════════════════════════════════════════════

    def _match_worker(self, ref_schema, ref_table, src_schema, src_table):
        try:
            def st(m):   self.after(0, lambda msg=m: self._status(msg))
            def pr(p):   self.after(0, lambda v=p:   self._prog_bar.set(min(float(v), 1.0)))
            def pv(m):   self.after(0, lambda msg=m: self._prog_var.set(msg))
            def pd2(m):  self.after(0, lambda msg=m: self._prog_detail.set(msg))

            src_nm = Path(self.src_path).stem if self.src_path else "EDC"
            ref_nm = Path(self.ref_path).stem if self.ref_path else "SRZ"

            # Read Asset column selection (optional)
            ref_asset_col = self._ref_asset_var.get()
            if ref_asset_col == "(none)" or ref_asset_col not in self.ref_df.columns:
                ref_asset_col = None

            # ── Normalisation ─────────────────────────────────────────────────
            # Collapse ALL unicode whitespace variants (incl non-breaking spaces)
            # then strip and lowercase — prevents ' AAP' ≠ 'AAP' false gaps.
            def norm(series):
                return (pd.Series(series)
                        .astype(str)
                        .str.replace(r"[\s\u00a0\u200b\ufeff]+", " ", regex=True)
                        .str.strip()
                        .str.lower())

            pv("Normalising SRZ reference…"); pr(0.10)
            st("Normalising SRZ reference data…")

            # ── SRZ side: build normalised schema+table pairs ─────────────────
            srz_schema_norm = norm(self.ref_df[ref_schema])
            srz_table_norm  = norm(self.ref_df[ref_table])

            # Keep the original raw values so we can join them back later
            ref_work = pd.DataFrame({
                "schema": srz_schema_norm,
                "table":  srz_table_norm,
                "srz_schema_raw": self.ref_df[ref_schema].astype(str).str.strip(),
                "srz_table_raw":  self.ref_df[ref_table].astype(str).str.strip(),
                "srz_asset":      (self.ref_df[ref_asset_col].astype(str).str.strip()
                                   if ref_asset_col else ""),
            })
            # Drop blanks / nulls
            ref_work = ref_work[
                ref_work["schema"].isin(["","nan","none"]) == False
            ].copy()
            ref_work = ref_work[
                ref_work["table"].isin(["","nan","none"]) == False
            ].copy()
            # Deduplicate on normalised pair, keep first raw+asset values
            ref_pairs = (ref_work
                         .sort_values(["schema","table","srz_asset"])
                         .drop_duplicates(subset=["schema","table"])
                         .reset_index(drop=True))

            pv("Normalising EDC source…"); pr(0.25)
            pd2(f"SRZ: {len(ref_pairs):,} unique (schema, table) pairs")
            st("Normalising EDC source data…")

            # ── EDC side: normalised schema+table pairs ───────────────────────
            edc_schema_norm = norm(self.src_df[src_schema])
            edc_table_norm  = norm(self.src_df[src_table])

            src_work = pd.DataFrame({
                "schema": edc_schema_norm,
                "table":  edc_table_norm,
                "edc_schema_raw": self.src_df[src_schema].astype(str).str.strip(),
                "edc_table_raw":  self.src_df[src_table].astype(str).str.strip(),
            })
            src_work = src_work[
                src_work["schema"].isin(["","nan","none"]) == False
            ].copy()
            src_work = src_work[
                src_work["table"].isin(["","nan","none"]) == False
            ].copy()
            src_pairs = (src_work
                         .sort_values(["schema","table"])
                         .drop_duplicates(subset=["schema","table"])
                         .reset_index(drop=True))

            pv("Running set comparison…"); pr(0.50)
            pd2(f"SRZ: {len(ref_pairs):,}  ·  EDC: {len(src_pairs):,} unique pairs")
            st("Computing matches and gaps…")

            if self._stop_event.is_set():
                self.after(0, lambda: self._match_done(True)); return

            # ── Full outer join on normalised keys ────────────────────────────
            ref_key = ref_pairs[["schema","table"]].copy()
            src_key = src_pairs[["schema","table"]].copy()
            ref_key["__in_ref__"] = True
            src_key["__in_src__"] = True

            merged = ref_key.merge(src_key, on=["schema","table"], how="outer")
            in_ref = merged["__in_ref__"].fillna(False).astype(bool)
            in_src = merged["__in_src__"].fillna(False).astype(bool)

            pv("Building result tables…"); pr(0.80)

            # Helper: extract a group and attach raw + asset columns from SRZ
            # and raw columns from EDC where available.
            def _build_result(mask):
                sub = merged[mask][["schema","table"]].copy().reset_index(drop=True)

                # Join SRZ raw + asset values
                sub = sub.merge(
                    ref_pairs[["schema","table","srz_schema_raw",
                               "srz_table_raw","srz_asset"]],
                    on=["schema","table"], how="left"
                )
                # Join EDC raw values
                sub = sub.merge(
                    src_pairs[["schema","table","edc_schema_raw","edc_table_raw"]],
                    on=["schema","table"], how="left"
                )
                sub = sub.fillna("")
                # Rename for display
                sub = sub.rename(columns={
                    "schema":         "Normalised Schema",
                    "table":          "Normalised Table",
                    "srz_schema_raw": "SRZ Schema",
                    "srz_table_raw":  "SRZ Table",
                    "srz_asset":      "SRZ Asset",
                    "edc_schema_raw": "EDC Schema",
                    "edc_table_raw":  "EDC Table",
                })
                return sub.sort_values(
                    ["Normalised Schema","Normalised Table"]
                ).reset_index(drop=True)

            matched_df  = _build_result(in_ref &  in_src)   # in both
            srz_only_df = _build_result(in_ref & ~in_src)   # in SRZ, missing from EDC ← GAPS
            edc_only_df = _build_result(~in_ref &  in_src)  # in EDC, not in SRZ

            # ── Near-miss: flag SRZ-only rows that have a SIMILAR EDC schema ──
            # Never flag a value that IS already an exact match.
            src_schema_set = set(src_pairs["schema"].unique())

            def _near_miss(srz_sch: str) -> str:
                if not srz_sch or srz_sch in src_schema_set:
                    return ""
                for e in src_schema_set:
                    if srz_sch in e or e in srz_sch:
                        return e
                for e in src_schema_set:
                    if e.endswith(srz_sch) or srz_sch.endswith(e):
                        return e
                return ""

            if not srz_only_df.empty:
                srz_only_df["Closest EDC Schema"] = srz_only_df["Normalised Schema"].map(
                    _near_miss)
                srz_only_df["Near-Miss Note"] = srz_only_df.apply(
                    lambda r: (
                        f"⚡ SRZ '{r['Normalised Schema']}' ≈ EDC '{r['Closest EDC Schema']}'"
                        f" — substring/suffix, verify if same schema")
                    if r["Closest EDC Schema"]
                    else "✗ Not found in EDC — confirm should be loaded",
                    axis=1)
            else:
                srz_only_df["Closest EDC Schema"] = pd.Series(dtype=str)
                srz_only_df["Near-Miss Note"] = pd.Series(dtype=str)

            # ── Schema-level summary ──────────────────────────────────────────
            ref_schemas = set(ref_pairs["schema"].unique())
            src_schemas = set(src_pairs["schema"].unique())
            sch_matched  = sorted(ref_schemas & src_schemas)
            sch_srz_only = sorted(ref_schemas - src_schemas)
            sch_edc_only = sorted(src_schemas - ref_schemas)

            pv("Running gap analysis…"); pr(0.95)
            try:
                self.gap_result = compute_gaps(
                    self.src_df, self.ref_df,
                    src_join_col=src_schema, ref_join_col=ref_schema,
                    src_table_col=src_table, ref_table_col=ref_table,
                    src_label=src_nm, ref_label=ref_nm)
            except Exception:
                self.gap_result = None

            # ── Store results ─────────────────────────────────────────────────
            self.results_df        = matched_df
            self._val_srz_only     = srz_only_df
            self._val_edc_only     = edc_only_df
            self._val_sch_matched  = sch_matched
            self._val_sch_srz_only = sch_srz_only
            self._val_sch_edc_only = sch_edc_only
            self._val_ref_nm       = ref_nm
            self._val_src_nm       = src_nm
            self._val_ref_total    = len(ref_pairs)
            self._val_src_total    = len(src_pairs)

            pr(1.0)
            self.after(0, lambda stopped=self._stop_event.is_set():
                       self._match_done(stopped))

        except Exception as e:
            import traceback; traceback.print_exc()
            self.after(0, lambda ex=e: self._on_error("Validation Error", ex))

    def _match_done(self, stopped: bool = False):
        n_match = len(self.results_df)    if self.results_df is not None       else 0
        n_srz   = len(self._val_srz_only) if hasattr(self,"_val_srz_only") else 0
        n_edc   = len(self._val_edc_only) if hasattr(self,"_val_edc_only") else 0
        self._prog_bar.set(1.0)
        self._prog_var.set(
            f"{'Stopped — ' if stopped else 'Complete — '}"
            f"{n_match:,} matched  ·  {n_srz:,} SRZ gaps  ·  {n_edc:,} EDC-only")
        self._stop_btn.pack_forget()
        self._stop_btn.configure(state="normal", text="⏹  Stop")
        self._run_btn.pack(side="left", padx=6)
        self._busy()
        self._status(
            f"Validation {'stopped' if stopped else 'complete'}  —  "
            f"Matched: {n_match:,}  ·  SRZ only (gaps): {n_srz:,}  ·  EDC only: {n_edc:,}")
        self._populate_results()
        self._goto(3)

    # ════════════════════════════════════════════════════════════════════════
    #  PAGE 3 — VALIDATION REPORT
    # ════════════════════════════════════════════════════════════════════════

    def _build_results(self):
        pg = tk.Frame(self._content, bg=C["bg"]); self._pages[3] = pg

        top = tk.Frame(pg, bg=C["bg"]); top.pack(fill="x", padx=20, pady=(12,4))
        _lbl(top,"Validation Report", C["bg"], C["text"], FH).pack(side="left")
        rb = tk.Frame(top, bg=C["bg"]); rb.pack(side="right")
        _back(rb,"← Reconfigure", lambda: self._goto(2))
        _navbtn(rb,"⬇  Export Full Report", self._export_results, C["green"])
        _navbtn(rb,"⬇  Export Cleaned Source", self._export_cleaned, C["teal"])

        # Stat cards
        self._stat_host = tk.Frame(pg, bg=C["bg"])
        self._stat_host.pack(fill="x", padx=20, pady=(4,8))

        # Tab bar
        tab_bar = _card(pg); tab_bar.pack(fill="x", padx=20)
        self._res_tab = tk.StringVar(value="summary")
        TAB_DEFS = [
            ("summary", "📊  Summary",                    C["blue"]),
            ("matched", "✅  Matched",                    C["green"]),
            ("srz_only","⚠   In SRZ — Missing from EDC",  C["amber"]),
            ("edc_only","ℹ   In EDC — Not in SRZ",         C["purple"]),
        ]
        self._tab_btns: Dict[str,tk.Label] = {}
        for val, txt, color in TAB_DEFS:
            btn = tk.Label(tab_bar, text=txt, bg=C["card"], fg=C["text2"],
                           font=FsB, padx=16, pady=8, cursor="hand2")
            btn.pack(side="left")
            btn.bind("<Button-1>", lambda e, v=val: self._switch_tab(v))
            self._tab_btns[val] = btn

        # Tab content frames
        self._tab_host = tk.Frame(pg, bg=C["bg"])
        self._tab_host.pack(fill="both", expand=True, padx=20, pady=(0,6))
        self._tab_frames: Dict[str,tk.Frame] = {}
        for val,*_ in TAB_DEFS:
            f = tk.Frame(self._tab_host, bg=C["bg"])
            self._tab_frames[val] = f

        # ── SUMMARY tab ───────────────────────────────────────────────────────
        sf = self._tab_frames["summary"]
        cov_row = tk.Frame(sf, bg=C["bg"]); cov_row.pack(fill="x", pady=(10,6))
        cov_row.columnconfigure(0, weight=1); cov_row.columnconfigure(1, weight=1)

        self._tbl_cov_card = _card(cov_row, padx=20, pady=16)
        self._tbl_cov_card.grid(row=0, column=0, sticky="nsew", padx=(0,8))
        _lbl(self._tbl_cov_card,"Table Coverage", C["card"], C["text"], FS).pack(anchor="w")
        _sep(self._tbl_cov_card)
        self._tbl_cov_body = tk.Frame(self._tbl_cov_card, bg=C["card"])
        self._tbl_cov_body.pack(fill="both", expand=True)

        self._sch_cov_card = _card(cov_row, padx=20, pady=16)
        self._sch_cov_card.grid(row=0, column=1, sticky="nsew", padx=(8,0))
        _lbl(self._sch_cov_card,"Schema Coverage", C["card"], C["text"], FS).pack(anchor="w")
        _sep(self._sch_cov_card)
        self._sch_cov_body = tk.Frame(self._sch_cov_card, bg=C["card"])
        self._sch_cov_body.pack(fill="both", expand=True)

        sg_row = tk.Frame(sf, bg=C["bg"]); sg_row.pack(fill="both", expand=True)
        sg_row.columnconfigure(0, weight=1); sg_row.columnconfigure(1, weight=1)

        sl = _card(sg_row, padx=12, pady=12)
        sl.grid(row=0, column=0, sticky="nsew", padx=(0,6))
        tk.Frame(sl, bg=C["amber"], height=4).pack(fill="x")
        self._sch_srz_lbl = _lbl(sl,"",C["card"],C["amber"],FBB)
        self._sch_srz_lbl.pack(anchor="w", pady=(6,2))
        _lbl(sl,"Schemas in SRZ — NOT found in EDC",
             C["card"],C["text3"],Fs,wraplength=420).pack(anchor="w",pady=(0,4))
        self._sch_srz_tree = make_tree(tk.Frame(sl,bg=C["card"]).apply(
            lambda w: w.pack(fill="both",expand=True) or w),
            ["Schema — In SRZ, Not in EDC"],[420])

        sr2 = _card(sg_row, padx=12, pady=12)
        sr2.grid(row=0, column=1, sticky="nsew", padx=(6,0))
        tk.Frame(sr2, bg=C["purple"], height=4).pack(fill="x")
        self._sch_edc_lbl = _lbl(sr2,"",C["card"],C["purple"],FBB)
        self._sch_edc_lbl.pack(anchor="w", pady=(6,2))
        _lbl(sr2,"Schemas in EDC — NOT found in SRZ",
             C["card"],C["text3"],Fs,wraplength=420).pack(anchor="w",pady=(0,4))
        self._sch_edc_tree = make_tree(tk.Frame(sr2,bg=C["card"]).apply(
            lambda w: w.pack(fill="both",expand=True) or w),
            ["Schema — In EDC, Not in SRZ"],[420])

        # ── Data tabs ─────────────────────────────────────────────────────────
        self._build_data_tab("matched",  C["green"],
            "✅  Matched Pairs",
            "Schema+Table pairs present in BOTH SRZ and EDC.")
        self._build_data_tab("srz_only", C["amber"],
            "⚠   In SRZ — MISSING from EDC  ← Action Required",
            "These are in SRZ (reference) but NOT in EDC. "
            "Amber rows have a similar-but-different EDC schema (near-miss). "
            "Red rows have no similar schema at all.")
        self._build_data_tab("edc_only", C["purple"],
            "ℹ   In EDC — Not in SRZ",
            "These are in EDC source but not in SRZ reference.")

    def _build_data_tab(self, key, color, title, subtitle):
        f = self._tab_frames[key]
        # Header banner
        hdr = tk.Frame(f, bg=color, padx=16, pady=10); hdr.pack(fill="x", pady=(8,0))
        _lbl(hdr, title, color, C["text_inv"], FBB).pack(anchor="w")
        _lbl(hdr, subtitle, color, C["text_inv"], ("Helvetica",8),
             justify="left", wraplength=900).pack(anchor="w", pady=(2,0))
        # Stat row
        sr = tk.Frame(f, bg=C["bg"]); sr.pack(fill="x", pady=(6,4))
        setattr(self, f"_dt_{key}_stats", sr)
        # Search bar
        sc = _card(f); sc.pack(fill="x", pady=(0,4))
        _lbl(sc,"Search:", C["card"], C["text2"], Fs).pack(side="left", padx=8, pady=6)
        sv = tk.StringVar(); setattr(self, f"_dt_{key}_flt", sv)
        tk.Entry(sc, textvariable=sv, bg=C["stripe"], fg=C["text"],
                 font=Fs, relief="flat", width=34).pack(side="left", padx=4)
        sv.trace_add("write", lambda *_, k=key: self._filter_dt(k))
        cnt = tk.StringVar(value=""); setattr(self, f"_dt_{key}_cnt", cnt)
        _lbl(sc, None, C["card"], C["text3"], Fs, textvariable=cnt).pack(side="right", padx=10)
        # Tree
        th = tk.Frame(f, bg=C["bg"]); th.pack(fill="both", expand=True)
        if key == "srz_only":
            cols   = ["SRZ Schema","SRZ Table","SRZ Asset",
                      "Closest EDC Schema","Near-Miss Note"]
            widths = [180, 250, 120, 180, 290]
        elif key == "matched":
            cols   = ["SRZ Schema","SRZ Table","SRZ Asset","EDC Schema","EDC Table"]
            widths = [170, 230, 100, 170, 230]
        else:  # edc_only
            cols   = ["EDC Schema","EDC Table"]
            widths = [280, 420]
        tree = make_tree(th, cols, widths)
        setattr(self, f"_dt_{key}_tree", tree)
        for col in cols[:2]:
            tree.heading(col, text=col, anchor="w",
                         command=lambda c=col, k=key: self._sort_dt(k, c))

    def _switch_tab(self, tab: str):
        self._res_tab.set(tab)
        for v, f in self._tab_frames.items(): f.pack_forget()
        self._tab_frames[tab].pack(fill="both", expand=True)
        for v, btn in self._tab_btns.items():
            btn.configure(
                bg=C["blue_lt"] if v==tab else C["card"],
                fg=C["blue_dk"] if v==tab else C["text2"],
                font=FBB if v==tab else FsB)

    def _populate_results(self):
        matched  = self.results_df              if self.results_df is not None else pd.DataFrame()
        srz_only = getattr(self,"_val_srz_only", pd.DataFrame())
        edc_only = getattr(self,"_val_edc_only", pd.DataFrame())
        ref_nm   = getattr(self,"_val_ref_nm","SRZ")
        src_nm   = getattr(self,"_val_src_nm","EDC")
        ref_total= getattr(self,"_val_ref_total",0)
        src_total= getattr(self,"_val_src_total",0)
        n_m   = len(matched)
        n_srz = len(srz_only)
        n_edc = len(edc_only)
        cov   = round(n_m / max(ref_total,1) * 100, 1)
        sch_matched  = getattr(self,"_val_sch_matched",[])
        sch_srz_only = getattr(self,"_val_sch_srz_only",[])
        sch_edc_only = getattr(self,"_val_sch_edc_only",[])
        n_near = int((srz_only["Closest EDC Schema"] != "").sum()) \
                 if (not srz_only.empty and "Closest EDC Schema" in srz_only.columns) else 0

        # ── Stat cards ────────────────────────────────────────────────────────
        for w in self._stat_host.winfo_children(): w.destroy()
        cards = [
            (f"{ref_total:,}", f"Total in {ref_nm}",             C["blue"],  "Unique (schema,table) pairs"),
            (f"{src_total:,}", f"Total in {src_nm}",             C["slate"], "Unique (schema,table) pairs"),
            (f"{n_m:,}",       "✅  Matched",                    C["green"], "In both files"),
            (f"{cov}%",        "Coverage",                       C["teal"],  f"% of {ref_nm} found in {src_nm}"),
            (f"{n_srz:,}",     f"⚠  In {ref_nm}, Not {src_nm}", C["amber"], "Gaps — action needed"),
            (f"{n_near:,}",    "⚡  Near-Misses",                C["amber"], "Similar but not exact"),
            (f"{n_edc:,}",     f"ℹ  In {src_nm}, Not {ref_nm}", C["purple"],"Extra in EDC"),
            (f"{len(sch_srz_only)}","Schema Gaps",               C["red"],   "Schemas only on one side"),
        ]
        for val,lbl,color,tip in cards:
            c = _card(self._stat_host, padx=10, pady=6)
            c.pack(side="left", padx=(0,4))
            _lbl(c, val, C["card"], color, ("Georgia",14,"bold")).pack()
            _lbl(c, lbl, C["card"], C["text"], Fs).pack()
            _lbl(c, tip, C["card"], C["text3"], ("Helvetica",7), wraplength=110).pack()

        # ── Coverage gauges ───────────────────────────────────────────────────
        def _gauge(host, n_match, n_left, n_right, unit, left_lbl, right_lbl):
            for w in host.winfo_children(): w.destroy()
            total_g = n_match + n_left + n_right
            pct = round(n_match / max(total_g,1) * 100, 1)
            col = C["green"] if pct>=95 else C["amber"] if pct>=75 else C["red"]
            _lbl(host, f"{pct}%", C["card"], col, ("Georgia",30,"bold")).pack(pady=(4,0))
            _lbl(host, f"of {total_g:,} {unit}", C["card"], C["text2"], Fs).pack()
            BAR = 280
            bar = tk.Frame(host, bg=C["border"], width=BAR, height=16)
            bar.pack(pady=(6,0)); bar.pack_propagate(False)
            for n,c_bar in [(n_match,C["green"]),(n_left,C["amber"]),(n_right,C["purple"])]:
                w2 = int(n/max(total_g,1)*BAR)
                if w2>0: tk.Frame(bar,bg=c_bar,width=w2,height=16).pack(side="left",fill="y")
            leg = tk.Frame(host,bg=C["card"]); leg.pack(pady=(4,0))
            for n,c_bar,lbl2 in [(n_match,C["green"],"Matched"),
                                  (n_left, C["amber"],left_lbl),
                                  (n_right,C["purple"],right_lbl)]:
                r = tk.Frame(leg,bg=C["card"]); r.pack(side="left",padx=6)
                tk.Frame(r,bg=c_bar,width=10,height=10).pack(side="left",pady=2)
                _lbl(r, f"  {n:,} {lbl2}", C["card"], C["text"], Fs).pack(side="left")

        _gauge(self._tbl_cov_body, n_m,  n_srz, n_edc,
               "tables", "SRZ only", "EDC only")
        _gauge(self._sch_cov_body, len(sch_matched), len(sch_srz_only), len(sch_edc_only),
               "schemas", "SRZ only", "EDC only")

        # ── Schema gap trees ──────────────────────────────────────────────────
        self._sch_srz_lbl.configure(
            text=f"{len(sch_srz_only):,} schema(s) in {ref_nm} — not in {src_nm}")
        self._sch_edc_lbl.configure(
            text=f"{len(sch_edc_only):,} schema(s) in {src_nm} — not in {ref_nm}")

        def _fill_sch_tree(tree, items, heading):
            tree.delete(*tree.get_children())
            tree["columns"] = [heading]
            tree.heading(heading, text=heading, anchor="w")
            tree.column(heading, width=420, anchor="w")
            for i,v in enumerate(items):
                tree.insert("","end", values=(v,), tags=("odd" if i%2 else "even",))

        _fill_sch_tree(self._sch_srz_tree, sch_srz_only,
                       "Schema — In SRZ, Not in EDC")
        _fill_sch_tree(self._sch_edc_tree, sch_edc_only,
                       "Schema — In EDC, Not in SRZ")

        # ── Data tabs ─────────────────────────────────────────────────────────
        self._load_dt("matched",  matched,  n_m,   C["green"])
        self._load_dt("srz_only", srz_only, n_srz, C["amber"])
        self._load_dt("edc_only", edc_only, n_edc, C["purple"])

        self._switch_tab("summary")

    def _load_dt(self, key: str, df: pd.DataFrame, total: int, color: str):
        sr      = getattr(self, f"_dt_{key}_stats")
        tree    = getattr(self, f"_dt_{key}_tree")
        cnt_var = getattr(self, f"_dt_{key}_cnt")
        setattr(self, f"_dt_{key}_full", df)

        for w in sr.winfo_children(): w.destroy()
        if total == 0:
            _lbl(sr,"  ✅  None — all records present on both sides!",
                 C["bg"], C["green"], FBB).pack(side="left", pady=4, padx=4)
        else:
            c2 = _card(sr, padx=12, pady=6); c2.pack(side="left")
            _lbl(c2, f"{total:,}", C["card"], color, ("Georgia",16,"bold")).pack()
            _lbl(c2, "records",   C["card"], C["text2"], Fs).pack()
            if not df.empty:
                # Use whichever schema column is present to count unique schemas
                sch_col = next((c for c in ("SRZ Schema","EDC Schema","Normalised Schema")
                                if c in df.columns), None)
                if sch_col:
                    ns = df[sch_col].nunique()
                    c3 = _card(sr, padx=12, pady=6); c3.pack(side="left", padx=(5,0))
                    _lbl(c3, f"{ns:,}", C["card"], color, ("Georgia",16,"bold")).pack()
                    _lbl(c3, "schemas", C["card"], C["text2"], Fs).pack()

        self._fill_dt_tree(tree, df, cnt_var)

    def _fill_dt_tree(self, tree, df: Optional[pd.DataFrame], cnt_var: tk.StringVar):
        tree.delete(*tree.get_children())
        if df is None or df.empty:
            cnt_var.set("0 records"); return

        # Determine display columns based on tab type
        if "Closest EDC Schema" in df.columns:
            # srz_only tab
            display_cols = ["SRZ Schema","SRZ Table","SRZ Asset",
                            "Closest EDC Schema","Near-Miss Note"]
            col_widths   = {"SRZ Schema":180,"SRZ Table":250,"SRZ Asset":120,
                            "Closest EDC Schema":180,"Near-Miss Note":290}
        elif "EDC Schema" in df.columns and "SRZ Schema" in df.columns:
            # matched tab — show both sides
            display_cols = ["SRZ Schema","SRZ Table","SRZ Asset","EDC Schema","EDC Table"]
            col_widths   = {"SRZ Schema":170,"SRZ Table":230,"SRZ Asset":100,
                            "EDC Schema":170,"EDC Table":230}
        else:
            # edc_only tab
            display_cols = ["EDC Schema","EDC Table"]
            col_widths   = {"EDC Schema":280,"EDC Table":420}

        # Only show columns that actually exist
        display_cols = [c for c in display_cols if c in df.columns]

        tree["columns"] = display_cols
        for c in display_cols:
            tree.heading(c, text=c, anchor="w")
            tree.column(c, width=col_widths.get(c,150), minwidth=60, anchor="w")

        cap = 10_000
        for i, (_, row) in enumerate(df.head(cap).iterrows()):
            vals = [str(row.get(c,"")) for c in display_cols]
            if "Near-Miss Note" in display_cols:
                has_nm = bool(row.get("Closest EDC Schema",""))
                tag    = "Token Overlap" if has_nm else "Fuzzy"
            else:
                tag = "odd" if i%2 else "even"
            tree.insert("","end", values=vals, tags=(tag,))

        if len(df) > cap:
            tree.insert("","end",
                        values=[f"  ⋯  {len(df)-cap:,} more — export for full list"]
                        + [""]*( len(display_cols)-1))
        cnt_var.set(f"{min(len(df),cap):,} of {len(df):,}")

    def _filter_dt(self, key: str):
        df      = getattr(self, f"_dt_{key}_full", None)
        tree    = getattr(self, f"_dt_{key}_tree")
        cnt_var = getattr(self, f"_dt_{key}_cnt")
        flt_var = getattr(self, f"_dt_{key}_flt")
        if df is None or df.empty: return
        q = flt_var.get().strip().lower()
        if q:
            df = df[df.apply(lambda r: any(q in str(v).lower() for v in r), axis=1)]
        self._fill_dt_tree(tree, df, cnt_var)

    _dt_sort_asc: Dict[str,bool] = {}
    def _sort_dt(self, key: str, col: str):
        df = getattr(self, f"_dt_{key}_full", None)
        if df is None or df.empty: return
        k = f"{key}_{col}"
        asc = not self._dt_sort_asc.get(k, False)
        self._dt_sort_asc[k] = asc
        df = df.sort_values(col, ascending=asc).reset_index(drop=True)
        setattr(self, f"_dt_{key}_full", df)
        self._fill_dt_tree(
            getattr(self, f"_dt_{key}_tree"), df,
            getattr(self, f"_dt_{key}_cnt"))

    # ── Export ─────────────────────────────────────────────────────────────────
    def _export_results(self):
        matched  = self.results_df if self.results_df is not None else pd.DataFrame()
        srz_only = getattr(self,"_val_srz_only", pd.DataFrame())
        edc_only = getattr(self,"_val_edc_only", pd.DataFrame())
        if matched.empty and srz_only.empty and edc_only.empty:
            messagebox.showinfo("Export","No results yet."); return

        path = filedialog.asksaveasfilename(
            defaultextension=".xlsx",
            filetypes=[("Excel Workbook","*.xlsx"),("CSV","*.csv")],
            title="Export Validation Report")
        if not path: return

        try:
            ref_nm   = getattr(self,"_val_ref_nm","SRZ")
            src_nm   = getattr(self,"_val_src_nm","EDC")
            ref_total= getattr(self,"_val_ref_total",0)
            src_total= getattr(self,"_val_src_total",0)
            n_m      = len(matched)
            n_srz    = len(srz_only)
            n_edc    = len(edc_only)
            cov      = round(n_m/max(ref_total,1)*100,1)
            sch_srz  = getattr(self,"_val_sch_srz_only",[])
            sch_edc  = getattr(self,"_val_sch_edc_only",[])
            sch_mat  = getattr(self,"_val_sch_matched",[])

            if path.endswith(".csv"):
                # CSV: export the gaps only
                parts = []
                if not srz_only.empty:
                    parts.append(srz_only.assign(**{"Gap Side":f"In {ref_nm} — Missing from {src_nm}"}))
                if not edc_only.empty:
                    parts.append(edc_only.assign(**{"Gap Side":f"In {src_nm} — Not in {ref_nm}"}))
                if parts:
                    pd.concat(parts,ignore_index=True).to_csv(path,index=False,encoding="utf-8-sig")
            else:
                with pd.ExcelWriter(path, engine="openpyxl") as w:

                    # Sheet 1: Executive Summary
                    pd.DataFrame([
                        {"Metric":"Source File (EDC)","Value":str(self.src_path)},
                        {"Metric":"Reference File (SRZ)","Value":str(self.ref_path)},
                        {"Metric":"","Value":""},
                        {"Metric":f"Total in {ref_nm} (authoritative)","Value":ref_total},
                        {"Metric":f"Total in {src_nm} (loaded)","Value":src_total},
                        {"Metric":"Matched (in both)","Value":n_m},
                        {"Metric":"Coverage %","Value":f"{cov}%"},
                        {"Metric":"","Value":""},
                        {"Metric":f"⚠  In {ref_nm} — MISSING from {src_nm}  ← GAPS","Value":n_srz},
                        {"Metric":f"ℹ  In {src_nm} — Not in {ref_nm}","Value":n_edc},
                        {"Metric":"","Value":""},
                        {"Metric":"Schemas matched","Value":len(sch_mat)},
                        {"Metric":f"Schemas in {ref_nm} only (gaps)","Value":len(sch_srz)},
                        {"Metric":f"Schemas in {src_nm} only","Value":len(sch_edc)},
                    ]).to_excel(w, sheet_name="Executive Summary", index=False)

                    # Sheet 2: SRZ gaps (the critical list)
                    if not srz_only.empty:
                        srz_out = srz_only.copy()
                        srz_out.insert(0,"Gap Type",f"In {ref_nm} — MISSING from {src_nm}")
                        srz_out.to_excel(w, sheet_name=f"GAPS — Missing from EDC", index=False)

                    # Sheet 3: EDC extras
                    if not edc_only.empty:
                        edc_out = edc_only.copy()
                        edc_out.insert(0,"Gap Type",f"In {src_nm} — Not in {ref_nm}")
                        edc_out.to_excel(w, sheet_name=f"In EDC — Not in SRZ", index=False)

                    # Sheet 4: Near-miss summary
                    if not srz_only.empty and "Closest EDC Schema" in srz_only.columns:
                        nm_cols = [c for c in ["SRZ Schema","SRZ Table","SRZ Asset",
                                               "Closest EDC Schema","Near-Miss Note"]
                                   if c in srz_only.columns]
                        nm_df = (srz_only[srz_only["Closest EDC Schema"]!=""][nm_cols]
                                 .drop_duplicates().sort_values(nm_cols[:2]))
                        if not nm_df.empty:
                            nm_df.to_excel(w, sheet_name="Near-Miss (Check These)", index=False)

                    # Sheet 5: Matched pairs (with SRZ and EDC values side by side)
                    if not matched.empty:
                        matched.to_excel(w, sheet_name="Matched Pairs", index=False)

                    # Sheet 6: Schema analysis
                    sch_df = pd.DataFrame([
                        *[{"Schema":s,"Status":"Matched (both)"} for s in sch_mat],
                        *[{"Schema":s,"Status":f"In {ref_nm} only — GAPS"} for s in sch_srz],
                        *[{"Schema":s,"Status":f"In {src_nm} only"} for s in sch_edc],
                    ])
                    if not sch_df.empty:
                        sch_df.sort_values("Schema").to_excel(
                            w, sheet_name="Schema Analysis", index=False)

                    # Sheet 7+: Raw cleaned files
                    if self.src_df is not None:
                        self.src_df.to_excel(w, sheet_name="Cleaned EDC Source", index=False)
                    if self.ref_df is not None:
                        self.ref_df.to_excel(w, sheet_name="SRZ Reference", index=False)

            self._status(f"Exported → {Path(path).name}")
            messagebox.showinfo("Export Complete",
                f"Validation report saved:\n{path}\n\n"
                f"  ✅  Matched:      {n_m:,}\n"
                f"  ⚠   SRZ gaps:    {n_srz:,}  ← tables to investigate\n"
                f"  ℹ   EDC extras:  {n_edc:,}\n"
                f"  📊  Coverage:    {cov}%")
        except Exception as e:
            messagebox.showerror("Export Error", str(e))

    def _export_cleaned(self):
        if self.src_df is None:
            messagebox.showinfo("Export","No cleaned source."); return
        path = filedialog.asksaveasfilename(
            defaultextension=".xlsx",
            filetypes=[("Excel Workbook","*.xlsx"),("CSV","*.csv")],
            title="Export Cleaned Source")
        if not path: return
        try:
            if path.endswith(".csv"):
                self.src_df.to_csv(path, index=False, encoding="utf-8-sig")
            else:
                self.src_df.to_excel(path, index=False)
            messagebox.showinfo("Exported", f"Saved:\n{path}")
        except Exception as e:
            messagebox.showerror("Export Error", str(e))

    def _on_error(self, title: str, exc: Exception):
        self._busy(); self._status(f"Error: {exc}")
        self._stop_btn.pack_forget()
        self._stop_btn.configure(state="normal", text="⏹  Stop")
        self._run_btn.pack(side="left", padx=6)
        try: self._parse_btn.configure(state="normal",text="Parse & Clean  →",bg=C["blue"])
        except: pass
        messagebox.showerror(title, str(exc))


def _back(parent,text:str,cmd):
    tk.Button(parent,text=text,bg=C["card"],fg=C["text2"],font=FB,relief="flat",
              padx=12,pady=5,cursor="hand2",
              highlightthickness=1,highlightbackground=C["border2"],
              command=cmd).pack(side="left",padx=(0,6))

def _navbtn(parent,text:str,cmd,bg:str=C["blue"]):
    tk.Button(parent,text=text,bg=bg,fg=C["text_inv"],font=FBB,relief="flat",
              padx=14,pady=5,cursor="hand2",command=cmd).pack(side="left",padx=(0,6))

# ══════════════════════════════════════════════════════════════════════════════
#  ENTRY POINT
# ══════════════════════════════════════════════════════════════════════════════
def _check_deps():
    missing=[]
    for pkg,pip in[("customtkinter","customtkinter"),("sklearn","scikit-learn"),
                   ("rapidfuzz","rapidfuzz"),("openpyxl","openpyxl")]:
        try: __import__(pkg)
        except ImportError: missing.append(pip)
    if missing:
        import subprocess,sys
        subprocess.check_call([sys.executable,"-m","pip","install","--quiet"]+missing)

if __name__=="__main__":
    _check_deps()
    App().mainloop()
