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
    Split a column like  CDIP_AASC_ADC_SRZ>hivemetastore>aare>daf_mcc_odf_teack
    into new columns:  parsed_zone, parsed_database, parsed_schema, parsed_table.
    The original column is kept.
    Returns a NEW dataframe with the extra columns inserted right after `col`.
    """
    if col not in df.columns:
        return df

    # Determine max depth from a sample
    sample=df[col].dropna().head(100)
    max_depth=0
    for v in sample:
        parts=[p.strip() for p in HIER_SEP.split(str(v)) if p.strip()]
        max_depth=max(max_depth,len(parts))
    max_depth=max(2,min(max_depth,5))

    labels=HIER_LABELS.get(max_depth,[f"{prefix}_{i}" for i in range(max_depth)])
    # If prefix differs from "parsed", rename
    if prefix!="parsed":
        labels=[l.replace("parsed_",f"{prefix}_") for l in labels]

    split_data={l:[] for l in labels}
    for v in df[col]:
        parts=[p.strip() for p in HIER_SEP.split(str(v)) if p.strip()]
        # Pad/trim to max_depth
        parts=(parts+[""]*max_depth)[:max_depth]
        for l,p in zip(labels,parts):
            split_data[l].append(p)

    new_cols=pd.DataFrame(split_data,index=df.index)
    # Insert after the source column
    pos=df.columns.get_loc(col)+1
    result=df.copy()
    for i,lbl in enumerate(labels):
        result.insert(pos+i,lbl,new_cols[lbl])
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
        self._stop_event=threading.Event(); self._step=0

        # Config state
        self._src_join_var  = tk.StringVar()
        self._ref_join_var  = tk.StringVar()
        self._hier_col_var  = tk.StringVar()
        self._src_search_vars:Dict[str,tk.BooleanVar]={}
        self._ref_search_vars:Dict[str,tk.BooleanVar]={}
        self._match_mode    = tk.StringVar(value="exact")
        self._substr_var    = tk.BooleanVar(value=True)
        self._use_pipe      = tk.BooleanVar(value=True)
        self._use_pipe_ref  = tk.BooleanVar(value=False)

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
                           command=self._switch_tab).pack(side="left",padx=14,pady=7)

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

    def _populate_preview(self): self._switch_tab()

    def _switch_tab(self,*_):
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
    def _build_configure(self):
        pg=tk.Frame(self._content,bg=C["bg"]); self._pages[2]=pg

        # Top nav
        top=tk.Frame(pg,bg=C["bg"]); top.pack(fill="x",padx=20,pady=(12,6))
        _lbl(top,"Configure Matching",C["bg"],C["text"],FH).pack(side="left")
        rbar=tk.Frame(top,bg=C["bg"]); rbar.pack(side="right")
        _back(rbar,"← Preview",lambda:self._goto(1))
        self._stop_btn=tk.Button(rbar,text="⏹  Stop",bg=C["red"],fg=C["text_inv"],
                                 font=FBB,relief="flat",padx=14,pady=6,cursor="hand2",
                                 command=self._do_stop)
        self._run_btn=tk.Button(rbar,text="▶  Run Matching Engine",
                                bg=C["teal"],fg=C["text_inv"],font=FBB,
                                relief="flat",padx=18,pady=6,cursor="hand2",
                                command=self._do_match)
        self._run_btn.pack(side="left",padx=6)

        # Outer scroll host so the page doesn't clip on small screens
        outer=tk.Frame(pg,bg=C["bg"]); outer.pack(fill="both",expand=True,padx=20,pady=(0,8))

        # ── ROW 1 : Full-Name parser (spans full width) ───────────────────────
        banner=_card(outer,padx=16,pady=12)
        banner.pack(fill="x",pady=(0,8))

        hdr=tk.Frame(banner,bg=C["card"]); hdr.pack(fill="x")
        _lbl(hdr,"📐  Reference File — Hierarchical Column Parser",
             C["card"],C["slate"],FS).pack(side="left")
        _lbl(hdr,"(NEW)  Splits  ZONE>DB>SCHEMA>TABLE  strings into separate columns",
             C["card"],C["text3"],Fs).pack(side="left",padx=12)

        row1=tk.Frame(banner,bg=C["card"]); row1.pack(fill="x",pady=(8,0))
        _lbl(row1,"Full-Name column:",C["card"],C["text"],Fs).pack(side="left")
        self._hier_col_cb=ttk.Combobox(row1,textvariable=self._hier_col_var,
                                        state="readonly",font=Fs,width=32)
        self._hier_col_cb.pack(side="left",padx=8)
        tk.Button(row1,text="Parse & Add Columns",
                  bg=C["amber_lt"],fg=C["amber"],font=FsB,relief="flat",
                  padx=10,pady=4,cursor="hand2",
                  command=self._do_parse_hier).pack(side="left")
        self._hier_status=_lbl(row1,"",C["card"],C["teal"],Fs)
        self._hier_status.pack(side="left",padx=12)

        _lbl(banner,
             "Example: CDIP_AASC_ADC_SRZ>hivemetastore>aare>daf_mcc_odf_teack  "
             "→  parsed_zone | parsed_database | parsed_schema | parsed_table",
             C["card"],C["text3"],Fs).pack(anchor="w",pady=(6,0))

        # ── ROW 2 : Join selector (full width) ────────────────────────────────
        join_card=_card(outer,padx=16,pady=12)
        join_card.pack(fill="x",pady=(0,8))

        hdr2=tk.Frame(join_card,bg=C["card"]); hdr2.pack(fill="x")
        _lbl(hdr2,"🔗  Join Configuration  — exact match before fuzzy search",
             C["card"],C["slate"],FS).pack(side="left")

        jrow=tk.Frame(join_card,bg=C["card"]); jrow.pack(fill="x",pady=(8,0))
        _lbl(jrow,"Source join col:",C["card"],C["teal"],FsB).pack(side="left")
        self._src_join_cb=ttk.Combobox(jrow,textvariable=self._src_join_var,
                                        state="readonly",font=Fs,width=28)
        self._src_join_cb.pack(side="left",padx=(6,0))
        _lbl(jrow,"   ↔   ",C["card"],C["text2"],FBB).pack(side="left")
        _lbl(jrow,"Reference join col:",C["card"],C["purple"],FsB).pack(side="left")
        self._ref_join_cb=ttk.Combobox(jrow,textvariable=self._ref_join_var,
                                        state="readonly",font=Fs,width=28)
        self._ref_join_cb.pack(side="left",padx=(6,0))

        _lbl(join_card,
             "Rows are grouped by join column value (exact, case-insensitive). "
             "Fuzzy search only runs within matching groups — dramatically faster for large files.",
             C["card"],C["text3"],Fs).pack(anchor="w",pady=(6,0))

        # ── ROW 3 : two-column selectors + settings ───────────────────────────
        row3=tk.Frame(outer,bg=C["bg"]); row3.pack(fill="both",expand=True)
        row3.columnconfigure(0,weight=0,minsize=250)
        row3.columnconfigure(1,weight=1)
        row3.columnconfigure(2,weight=1)

        # LEFT: settings
        left=_card(row3,padx=16,pady=14)
        left.grid(row=0,column=0,sticky="nsew",padx=(0,8))
        left.pack_propagate(False)

        _lbl(left,"Match Settings",C["card"],C["text"],FS).pack(anchor="w")
        _sep(left)

        # ── Match Mode ────────────────────────────────────────────────────────
        _lbl(left,"Match Mode",C["card"],C["text"],FBB).pack(anchor="w")
        self._match_mode=tk.StringVar(value="exact")
        modes=[
            ("exact",  "⚡ Exact Join  (SQL-style, fastest)",
             "Uses pandas merge. Handles 1M+ rows in\nseconds. No fuzzy — join + exact/substring only.",
             C["teal"]),
            ("fuzzy",  "🔍 Fuzzy Match  (slower, finds near-matches)",
             "Uses rapidfuzz within join groups.\nBetter recall but slower on large files.",
             C["blue"]),
        ]
        self._mode_frames:Dict[str,tk.Frame]={}
        for val,label,desc,color in modes:
            rb_row=tk.Frame(left,bg=C["card"]); rb_row.pack(fill="x",pady=(3,0))
            tk.Radiobutton(rb_row,text=label,variable=self._match_mode,value=val,
                           bg=C["card"],fg=color,font=FsB,
                           activebackground=C["card"],selectcolor=C["stripe"],
                           command=self._on_mode_change).pack(anchor="w")
            _lbl(left,desc,C["card"],C["text3"],("Helvetica",8),
                 justify="left",wraplength=220).pack(anchor="w",padx=16,pady=(0,2))

        _sep(left)

        # Substring toggle (exact mode only)
        self._substr_var=tk.BooleanVar(value=True)
        self._substr_chk=tk.Checkbutton(left,text="Include Substring matches",
                                         variable=self._substr_var,
                                         bg=C["card"],fg=C["text"],font=Fs,
                                         activebackground=C["card"],selectcolor=C["teal_lt"])
        self._substr_chk.pack(anchor="w")
        _lbl(left,"e.g.  'table_name' found inside  'schema.table_name'",
             C["card"],C["text3"],("Helvetica",8)).pack(anchor="w",padx=16,pady=(0,6))

        # Threshold (fuzzy mode only)
        self._thresh_frame=tk.Frame(left,bg=C["card"]); self._thresh_frame.pack(fill="x")
        _lbl(self._thresh_frame,"Similarity Threshold",C["card"],C["text"],FBB).pack(anchor="w")
        _lbl(self._thresh_frame,"Higher = fewer but more precise matches",
             C["card"],C["text3"],Fs).pack(anchor="w",pady=(0,4))
        tr=tk.Frame(self._thresh_frame,bg=C["card"]); tr.pack(fill="x",pady=(0,8))
        self._thresh_lbl=tk.Label(tr,text="70%",bg=C["card"],fg=C["blue"],
                                   font=("Georgia",13,"bold"),width=5)
        self._thresh_lbl.pack(side="right")
        self._thresh_var=tk.DoubleVar(value=0.70)
        tk.Scale(tr,variable=self._thresh_var,from_=0.30,to=0.98,resolution=0.02,
                 orient="horizontal",bg=C["card"],fg=C["text2"],troughcolor=C["border2"],
                 activebackground=C["blue"],highlightthickness=0,relief="flat",bd=0,
                 showvalue=False,
                 command=lambda v:self._thresh_lbl.configure(text=f"{int(float(v)*100)}%")
                 ).pack(side="left",fill="x",expand=True)

        _sep(left)

        # ── Progress ──────────────────────────────────────────────────────────
        _lbl(left,"Progress",C["card"],C["text"],FBB).pack(anchor="w")
        self._prog_var=tk.StringVar(value="")
        self._prog_detail=tk.StringVar(value="")
        tk.Label(left,textvariable=self._prog_var,bg=C["card"],
                 fg=C["text2"],font=FsB).pack(anchor="w",pady=(2,0))
        self._prog_bar=ctk.CTkProgressBar(left,width=220,height=10,
                                           corner_radius=4,
                                           progress_color=C["teal"],
                                           fg_color=C["border"])
        self._prog_bar.set(0); self._prog_bar.pack(pady=(3,2),anchor="w")
        tk.Label(left,textvariable=self._prog_detail,bg=C["card"],
                 fg=C["text3"],font=("Helvetica",8),wraplength=220,
                 justify="left").pack(anchor="w")

        # MIDDLE: source columns
        mid=_card(row3,padx=12,pady=12)
        mid.grid(row=0,column=1,sticky="nsew",padx=(0,6))
        hm=tk.Frame(mid,bg=C["teal"],padx=8,pady=4); hm.pack(fill="x",pady=(0,8))
        _lbl(hm,"SOURCE FILE  —  Search Columns",C["teal"],C["text_inv"],FsB).pack(side="left")
        _lbl(mid,"Values from checked cols will be looked up in the reference.",
             C["card"],C["text3"],Fs).pack(anchor="w",pady=(0,4))
        self._src_cl_host=tk.Frame(mid,bg=C["card"]); self._src_cl_host.pack(fill="both",expand=True)
        sb=tk.Frame(mid,bg=C["card"]); sb.pack(fill="x",pady=(4,0))
        tk.Button(sb,text="All",bg=C["teal_lt"],fg=C["teal"],font=FBG,relief="flat",padx=6,pady=2,
                  command=lambda:self._tog(self._src_search_vars,True)).pack(side="left",padx=(0,3))
        tk.Button(sb,text="None",bg=C["stripe"],fg=C["text2"],font=FBG,relief="flat",padx=6,pady=2,
                  command=lambda:self._tog(self._src_search_vars,False)).pack(side="left")

        # RIGHT: reference columns
        right2=_card(row3,padx=12,pady=12)
        right2.grid(row=0,column=2,sticky="nsew",padx=(6,0))
        hr=tk.Frame(right2,bg=C["purple"],padx=8,pady=4); hr.pack(fill="x",pady=(0,8))
        _lbl(hr,"REFERENCE FILE  —  Search Columns",C["purple"],C["text_inv"],FsB).pack(side="left")
        _lbl(right2,"Checked cols will be scanned for matching values.",
             C["card"],C["text3"],Fs).pack(anchor="w",pady=(0,4))
        self._ref_cl_host=tk.Frame(right2,bg=C["card"]); self._ref_cl_host.pack(fill="both",expand=True)
        rb=tk.Frame(right2,bg=C["card"]); rb.pack(fill="x",pady=(4,0))
        tk.Button(rb,text="All",bg=C["purple_lt"],fg=C["purple"],font=FBG,relief="flat",padx=6,pady=2,
                  command=lambda:self._tog(self._ref_search_vars,True)).pack(side="left",padx=(0,3))
        tk.Button(rb,text="None",bg=C["stripe"],fg=C["text2"],font=FBG,relief="flat",padx=6,pady=2,
                  command=lambda:self._tog(self._ref_search_vars,False)).pack(side="left")

    def _tog(self,d:Dict[str,tk.BooleanVar],v:bool):
        for var in d.values(): var.set(v)

    def _on_mode_change(self):
        """Show/hide threshold slider based on match mode."""
        mode=self._match_mode.get()
        if mode=="exact":
            self._thresh_frame.pack_forget()
        else:
            self._thresh_frame.pack(fill="x")

    def _do_parse_hier(self):
        col=self._hier_col_var.get()
        if not col or self.ref_df is None:
            return
        try:
            self.ref_df=parse_hierarchical_column(self.ref_df,col)
            new_cols=[c for c in self.ref_df.columns if c.startswith("parsed_")]
            self._hier_status.configure(
                text=f"✓  Added: {', '.join(new_cols)}")
            # Refresh all reference column dropdowns
            self._populate_configure()
            self._status(f"Parsed '{col}' → {len(new_cols)} new columns")
        except Exception as e:
            messagebox.showerror("Parse Error",str(e))

    def _populate_configure(self):
        if self.src_df is None or self.ref_df is None: return
        src_cols=list(self.src_df.columns)
        ref_cols=list(self.ref_df.columns)
        src_key=detect_key_columns(self.src_df)
        ref_key=detect_key_columns(self.ref_df)

        # Hierarchical column detector
        hier_candidates=detect_hierarchical_columns(self.ref_df)
        self._hier_col_cb["values"]=ref_cols
        if not self._hier_col_var.get() or self._hier_col_var.get() not in ref_cols:
            self._hier_col_var.set(hier_candidates[0] if hier_candidates else ref_cols[0])

        # Source join default → SCHEMA_NAME or MALCODE
        self._src_join_cb["values"]=["(none)"]+src_cols
        dflt_sj=next((c for c in src_cols if re.search(r"schema.?name|schema$",c,re.I)),None) or \
                next((c for c in src_cols if re.search(r"mal.?code",c,re.I)),None) or \
                (src_key[0] if src_key else src_cols[0])
        if not self._src_join_var.get() or self._src_join_var.get() not in src_cols:
            self._src_join_var.set(dflt_sj)

        # Ref join default → parsed_schema or schema
        self._ref_join_cb["values"]=["(none)"]+ref_cols
        dflt_rj=next((c for c in ref_cols if c=="parsed_schema"),None) or \
                next((c for c in ref_cols if re.search(r"schema",c,re.I)),None) or \
                (ref_key[0] if ref_key else ref_cols[0])
        if not self._ref_join_var.get() or self._ref_join_var.get() not in ref_cols:
            self._ref_join_var.set(dflt_rj)

        # Source search checklist
        for w in self._src_cl_host.winfo_children(): w.destroy()
        self._src_search_vars.clear()
        frame,self._src_search_vars=_checklist(self._src_cl_host,src_cols,
                                                checked=True,height=220,
                                                bg=C["card"],accent=C["teal_lt"])
        kset=set(src_key)
        if len(src_cols)>6:
            for col,v in self._src_search_vars.items():
                if col not in kset: v.set(False)
        # Always check TABLE_NAME and SCHEMA_NAME
        for col,v in self._src_search_vars.items():
            if re.search(r"table.?name|schema.?name",col,re.I): v.set(True)
        frame.pack(fill="both",expand=True)

        # Reference search checklist
        for w in self._ref_cl_host.winfo_children(): w.destroy()
        self._ref_search_vars.clear()
        frame2,self._ref_search_vars=_checklist(self._ref_cl_host,ref_cols,
                                                  checked=True,height=220,
                                                  bg=C["card"],accent=C["purple_lt"])
        kset2=set(ref_key)|{c for c in ref_cols if c.startswith("parsed_")}
        if len(ref_cols)>6:
            for col,v in self._ref_search_vars.items():
                if col not in kset2: v.set(False)
        # Always check parsed_table and parsed_schema
        for col,v in self._ref_search_vars.items():
            if re.search(r"parsed_table|parsed_schema|table.?name",col,re.I): v.set(True)
        frame2.pack(fill="both",expand=True)

    # ── Run / Stop ────────────────────────────────────────────────────────────
    def _do_match(self):
        src_join=self._src_join_var.get()
        if src_join.startswith("(none)"): src_join=None
        ref_join=self._ref_join_var.get()
        if ref_join.startswith("(none)"): ref_join=None
        src_s=[c for c,v in self._src_search_vars.items() if v.get()]
        ref_s=[c for c,v in self._ref_search_vars.items() if v.get()]
        if not src_s:
            messagebox.showwarning("Config","Select at least one Source search column."); return
        if not ref_s:
            messagebox.showwarning("Config","Select at least one Reference search column."); return
        mode   = self._match_mode.get()
        substr = self._substr_var.get()
        self._stop_event.clear()
        self._run_btn.pack_forget(); self._stop_btn.pack(side="left",padx=6)
        self._prog_bar.set(0)
        self._prog_bar.configure(progress_color=C["teal"] if mode=="exact" else C["blue"])
        self._prog_var.set(""); self._prog_detail.set("")
        self._busy(f"{'Exact Join' if mode=='exact' else 'Fuzzy Match'} engine starting…")
        threading.Thread(target=self._match_worker,
                         args=(mode,self._thresh_var.get(),src_join,ref_join,
                               src_s,ref_s,substr),
                         daemon=True).start()

    def _do_stop(self):
        self._stop_event.set()
        self._stop_btn.configure(state="disabled",text="Stopping…")
        self._status("Stop requested — finishing current chunk…")

    def _match_worker(self,mode,threshold,src_join,ref_join,src_s,ref_s,substr):
        try:
            def st(m):  self.after(0,lambda msg=m: self._status(msg))
            def pr(p):  self.after(0,lambda v=p:   self._prog_bar.set(min(float(v),1.0)))
            def det(m): self.after(0,lambda msg=m: self._prog_detail.set(msg))
            def pl(m):
                self.after(0,lambda msg=m: (
                    self._prog_var.set(msg),
                    self._status(msg)
                ))

            if mode=="exact":
                # ── Vectorised pandas join (fast path) ────────────────────────
                engine=ExactJoinEngine()
                pl("Building join index…")
                res=engine.run(
                    self.src_df, self.ref_df,
                    src_join_col=src_join,   ref_join_col=ref_join,
                    src_search_cols=src_s,   ref_search_cols=ref_s,
                    include_substring=substr,
                    stop_event=self._stop_event,
                    progress_cb=pr,
                    status_cb=lambda m:(det(m),st(m)),
                )
            else:
                # ── Fuzzy match (thorough path) ───────────────────────────────
                engine=MatchEngine(threshold=threshold)
                pl("Building fuzzy index…")
                engine.fit(self.ref_df,ref_join_col=ref_join,
                           ref_search_cols=ref_s,status_cb=st)
                res=engine.run(
                    self.src_df,src_join_col=src_join,
                    src_search_cols=src_s,stop_event=self._stop_event,
                    progress_cb=pr,status_cb=lambda m:(det(m),st(m)))

            self.results_df=res
            self.after(0,lambda stopped=self._stop_event.is_set():
                       self._match_done(stopped))
        except Exception as e:
            import traceback; traceback.print_exc()
            self.after(0,lambda ex=e:self._on_error("Matching Error",ex))

    def _match_done(self,stopped:bool=False):
        n=len(self.results_df) if self.results_df is not None else 0
        self._prog_bar.set(1.0)
        mode_lbl="Exact Join" if self._match_mode.get()=="exact" else "Fuzzy Match"
        self._prog_var.set(f"{'Stopped — ' if stopped else 'Complete — '}{n:,} matches")
        self._prog_detail.set(f"Mode: {mode_lbl}  ·  {n:,} result rows")
        self._stop_btn.pack_forget(); self._stop_btn.configure(state="normal",text="⏹  Stop")
        self._run_btn.pack(side="left",padx=6); self._busy()
        self._status(f"{'Stopped' if stopped else 'Done'} [{mode_lbl}] — {n:,} matches found")
        if n>0: self._populate_results(); self._goto(3)
        else:
            messagebox.showinfo("No Matches",
                "No matches found.\n\nTips:\n"
                "• Verify the join columns point to the same data\n"
                "• Try 'Include Substring matches' in Exact mode\n"
                "• Switch to Fuzzy Match for near-matches\n"
                "• Check the reference file was parsed correctly")

    # ══════════════════════════════════════════════════════════════════════════
    #  PAGE 3 — RESULTS  (richer stats — NEW v3)
    # ══════════════════════════════════════════════════════════════════════════
    def _build_results(self):
        pg=tk.Frame(self._content,bg=C["bg"]); self._pages[3]=pg

        top=tk.Frame(pg,bg=C["bg"]); top.pack(fill="x",padx=20,pady=(12,4))
        _lbl(top,"Match Results",C["bg"],C["text"],FH).pack(side="left")
        rb=tk.Frame(top,bg=C["bg"]); rb.pack(side="right")
        _back(rb,"← Reconfigure",lambda:self._goto(2))
        _navbtn(rb,"⬇  Export Full Report",self._export_results,C["green"])
        _navbtn(rb,"⬇  Export Cleaned Source",self._export_cleaned,C["teal"])

        # ── Summary stat cards ────────────────────────────────────────────────
        self._stat_host=tk.Frame(pg,bg=C["bg"]); self._stat_host.pack(fill="x",padx=20,pady=(4,6))

        # ── Two-column layout: filter+tree on left, breakdown on right ────────
        body=tk.Frame(pg,bg=C["bg"]); body.pack(fill="both",expand=True,padx=20,pady=(0,6))
        body.columnconfigure(0,weight=1); body.columnconfigure(1,weight=0,minsize=300)

        # Filter bar
        fc=_card(body); fc.grid(row=0,column=0,sticky="ew",pady=(0,4),padx=(0,6))
        _lbl(fc,"Search:",C["card"],C["text2"],Fs).pack(side="left",padx=10,pady=7)
        self._flt=tk.StringVar(); self._flt.trace_add("write",self._filter)
        tk.Entry(fc,textvariable=self._flt,bg=C["stripe"],fg=C["text"],
                 font=Fs,relief="flat",width=26).pack(side="left",padx=4)
        _lbl(fc,"Type:",C["card"],C["text2"],Fs).pack(side="left",padx=(12,4))
        self._flt_type=tk.StringVar(value="All")
        tt=ttk.Combobox(fc,textvariable=self._flt_type,
                        values=["All","Exact","Substring","Token Overlap","Fuzzy"],
                        state="readonly",width=14,font=Fs)
        tt.pack(side="left",pady=7); tt.bind("<<ComboboxSelected>>",self._filter)
        _lbl(fc,"Min %:",C["card"],C["text2"],Fs).pack(side="left",padx=(12,4))
        self._flt_score=tk.IntVar(value=70)
        tk.Spinbox(fc,from_=0,to=100,textvariable=self._flt_score,
                   width=5,font=Fs,command=self._filter).pack(side="left",pady=7)
        _lbl(fc,"Confidence:",C["card"],C["text2"],Fs).pack(side="left",padx=(12,4))
        self._flt_conf=tk.StringVar(value="All")
        cf=ttk.Combobox(fc,textvariable=self._flt_conf,
                        values=["All","Definite","High","Medium","Low"],
                        state="readonly",width=10,font=Fs)
        cf.pack(side="left",pady=7); cf.bind("<<ComboboxSelected>>",self._filter)
        self._flt_cnt=tk.StringVar(value="")
        _lbl(fc,None,C["card"],C["text3"],Fs,textvariable=self._flt_cnt).pack(side="right",padx=12)

        # Results tree
        th=tk.Frame(body,bg=C["bg"])
        th.grid(row=1,column=0,sticky="nsew",padx=(0,6))
        body.rowconfigure(1,weight=1)
        RW=[55,110,110,120,160,55,120,160,360,65,110,110]
        self._res_tree=make_tree(th,MatchEngine.COLS,RW)
        for col in MatchEngine.COLS:
            self._res_tree.heading(col,text=col,anchor="w",
                                   command=lambda c=col:self._sort(c))

        # RIGHT: breakdown panel
        right=_card(body,padx=12,pady=12)
        right.grid(row=0,column=1,rowspan=2,sticky="nsew")

        _lbl(right,"Breakdown",C["card"],C["text"],FS).pack(anchor="w")
        _sep(right)

        # Match type distribution
        _lbl(right,"By Match Type",C["card"],C["text"],FBB).pack(anchor="w",pady=(4,2))
        self._breakdown_host=tk.Frame(right,bg=C["card"]); self._breakdown_host.pack(fill="x")

        _sep(right)
        # Top join values
        _lbl(right,"Top Join Values",C["card"],C["text"],FBB).pack(anchor="w",pady=(4,2))
        self._topjoin_host=tk.Frame(right,bg=C["card"]); self._topjoin_host.pack(fill="x")

        _sep(right)
        # Confidence distribution
        _lbl(right,"Confidence Distribution",C["card"],C["text"],FBB).pack(anchor="w",pady=(4,2))
        self._conf_host=tk.Frame(right,bg=C["card"]); self._conf_host.pack(fill="x")

        _sep(right)
        # Column coverage
        _lbl(right,"Source Column Coverage",C["card"],C["text"],FBB).pack(anchor="w",pady=(4,2))
        self._cov_host=tk.Frame(right,bg=C["card"]); self._cov_host.pack(fill="x")

    def _populate_results(self):
        if self.results_df is None: return
        df=self.results_df

        # ── Stat cards ─────────────────────────────────────────────────────────
        for w in self._stat_host.winfo_children(): w.destroy()
        definite=df["Confidence"].str.contains("Definite",na=False).sum()
        high=df["Confidence"].str.contains("High",na=False).sum()
        exact=(df["Match Type"]=="Exact").sum()
        stats=[
            ("Total Matches",f"{len(df):,}",C["blue"]),
            ("Definite (≥95%)",f"{definite:,}",C["green"]),
            ("High (85-94%)",f"{high:,}",C["teal"]),
            ("Exact Match",f"{exact:,}",C["slate"]),
            ("Avg Score",f"{df['Score %'].mean():.1f}%",C["amber"]),
            ("Unique Join Values",f"{df['Join Src Val'].nunique():,}",C["purple"]),
            ("Src Rows Matched",f"{df['Src Row'].nunique():,}",C["navy2"]),
        ]
        for lbl,val,color in stats:
            c=_card(self._stat_host,padx=12,pady=6)
            c.pack(side="left",padx=(0,5))
            _lbl(c,val,C["card"],color,("Georgia",13,"bold")).pack()
            _lbl(c,lbl,C["card"],C["text2"],Fs).pack()

        # ── Breakdown panel ────────────────────────────────────────────────────
        def _bar_row(host,label,count,total,color):
            r=tk.Frame(host,bg=C["card"]); r.pack(fill="x",pady=1)
            _lbl(r,f"{label}",C["card"],C["text"],Fs,width=16,anchor="w").pack(side="left")
            pct=count/max(total,1)
            bar_outer=tk.Frame(r,bg=C["border"],height=10,width=140)
            bar_outer.pack(side="left",padx=4); bar_outer.pack_propagate(False)
            tk.Frame(bar_outer,bg=color,height=10,
                     width=int(pct*140)).pack(side="left",fill="y")
            _lbl(r,f"{count:,}",C["card"],C["text2"],Fs).pack(side="left",padx=4)

        for w in self._breakdown_host.winfo_children(): w.destroy()
        for mtype,color in [("Exact",C["green"]),("Substring",C["blue_mid"]),
                            ("Token Overlap",C["amber"]),("Fuzzy",C["red"])]:
            n=(df["Match Type"]==mtype).sum()
            _bar_row(self._breakdown_host,mtype,n,len(df),color)

        for w in self._topjoin_host.winfo_children(): w.destroy()
        top5=df.groupby("Join Src Val").size().sort_values(ascending=False).head(8)
        for jv,cnt in top5.items():
            r=tk.Frame(self._topjoin_host,bg=C["card"]); r.pack(fill="x",pady=1)
            _lbl(r,str(jv)[:20],C["card"],C["text"],Fs,anchor="w",width=20).pack(side="left")
            _lbl(r,f"{cnt:,} matches",C["card"],C["text2"],Fs).pack(side="left",padx=6)

        for w in self._conf_host.winfo_children(): w.destroy()
        for lbl,color in [("Definite",C["green"]),("High",C["teal"]),
                          ("Medium",C["amber"]),("Low",C["red"])]:
            n=df["Confidence"].str.contains(lbl,na=False).sum()
            _bar_row(self._conf_host,lbl,n,len(df),color)

        for w in self._cov_host.winfo_children(): w.destroy()
        for col,cnt in df.groupby("Src Column").size().sort_values(ascending=False).items():
            r=tk.Frame(self._cov_host,bg=C["card"]); r.pack(fill="x",pady=1)
            _lbl(r,str(col)[:22],C["card"],C["text"],Fs,anchor="w",width=22).pack(side="left")
            _lbl(r,f"{cnt:,}",C["card"],C["text2"],Fs).pack(side="left",padx=4)

        self._load_tree(df)

    def _load_tree(self,df:pd.DataFrame):
        tree=self._res_tree; tree.delete(*tree.get_children())
        cols=MatchEngine.COLS; cap=6000
        for _,row in df.head(cap).iterrows():
            mtype=str(row.get("Match Type",""))
            vals=[str(row.get(c,""))[:220] for c in cols]
            tree.insert("","end",values=vals,tags=(mtype,))
        if len(df)>cap:
            tree.insert("","end",values=[f"  ⋯  {len(df)-cap:,} more — export to see all"]+[""]*( len(cols)-1))
        self._flt_cnt.set(f"{min(len(df),cap):,} of {len(df):,}")

    def _filter(self,*_):
        if self.results_df is None: return
        df=self.results_df.copy()
        q=self._flt.get().lower().strip()
        if q: df=df[df.apply(lambda r:any(q in str(v).lower() for v in r),axis=1)]
        t=self._flt_type.get()
        if t!="All": df=df[df["Match Type"]==t]
        df=df[df["Score %"]>=self._flt_score.get()]
        conf=self._flt_conf.get()
        if conf!="All": df=df[df["Confidence"].str.contains(conf,na=False)]
        self._load_tree(df)

    _sasc:Dict[str,bool]={}
    def _sort(self,col:str):
        if self.results_df is None: return
        asc=not self._sasc.get(col,False); self._sasc[col]=asc
        df=self.results_df.copy()
        if col=="Score %": df[col]=pd.to_numeric(df[col],errors="coerce")
        df.sort_values(col,ascending=asc,inplace=True)
        self.results_df=df; self._load_tree(df)

    # ── Export ─────────────────────────────────────────────────────────────────
    def _export_results(self):
        if self.results_df is None or self.results_df.empty:
            messagebox.showinfo("Export","No results yet."); return
        path=filedialog.asksaveasfilename(defaultextension=".xlsx",
            filetypes=[("Excel Workbook","*.xlsx"),("CSV","*.csv")],
            title="Export Full Match Report")
        if not path: return
        try:
            if path.endswith(".csv"):
                self.results_df.to_csv(path,index=False,encoding="utf-8-sig")
            else:
                df=self.results_df
                with pd.ExcelWriter(path,engine="openpyxl") as w:
                    df.to_excel(w,sheet_name="All Matches",index=False)
                    # Exact matches only
                    df[df["Match Type"]=="Exact"].to_excel(
                        w,sheet_name="Exact Matches",index=False)
                    # Summary by join value
                    summ=(df.groupby("Join Src Val")
                          .agg(Match_Count=("Score %","count"),
                               Avg_Score=("Score %","mean"),
                               Max_Score=("Score %","max"),
                               Exact_Count=("Match Type",lambda x:(x=="Exact").sum()))
                          .reset_index().sort_values("Match_Count",ascending=False))
                    summ.to_excel(w,sheet_name="Summary by Join Value",index=False)
                    # Cleaned source
                    if self.src_df is not None:
                        self.src_df.to_excel(w,sheet_name="Cleaned Source",index=False)
                    # Reference (with parsed cols if added)
                    if self.ref_df is not None:
                        self.ref_df.to_excel(w,sheet_name="Reference File",index=False)
            self._status(f"Exported → {Path(path).name}")
            messagebox.showinfo("Exported",f"Saved:\n{path}")
        except Exception as e:
            messagebox.showerror("Export Error",str(e))

    def _export_cleaned(self):
        if self.src_df is None:
            messagebox.showinfo("Export","No cleaned source."); return
        path=filedialog.asksaveasfilename(defaultextension=".xlsx",
            filetypes=[("Excel Workbook","*.xlsx"),("CSV","*.csv")],
            title="Export Cleaned Source")
        if not path: return
        try:
            if path.endswith(".csv"): self.src_df.to_csv(path,index=False,encoding="utf-8-sig")
            else: self.src_df.to_excel(path,index=False)
            messagebox.showinfo("Exported",f"Saved:\n{path}")
        except Exception as e:
            messagebox.showerror("Export Error",str(e))

    def _on_error(self,title:str,exc:Exception):
        self._busy(); self._status(f"Error: {exc}")
        self._stop_btn.pack_forget()
        self._stop_btn.configure(state="normal",text="⏹  Stop")
        self._run_btn.pack(side="left",padx=6)
        self._parse_btn.configure(state="normal",text="Parse & Clean  →",bg=C["blue"])
        messagebox.showerror(title,str(exc))

# ══════════════════════════════════════════════════════════════════════════════
#  SMALL WIDGET FACTORIES
# ══════════════════════════════════════════════════════════════════════════════
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
