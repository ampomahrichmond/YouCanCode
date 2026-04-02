#!/usr/bin/env python3
"""
╔══════════════════════════════════════════════════════════════════════════════╗
║  DataForge Pro  v1.0                                                        ║
║  Intelligent Data Cleaning & Cross-File Matching Platform                   ║
║                                                                              ║
║  Workflow:                                                                   ║
║    1. Upload messy pipe-delimited source file + any reference file           ║
║    2. Auto-parse pipe headers + pipe-separated row values into a clean DF   ║
║    3. Build a TF-IDF + fuzzy matching engine over the reference file         ║
║    4. Find every overlap between the two files — exact, substring, or fuzzy ║
║                                                                              ║
║  Dependencies: customtkinter, pandas, scikit-learn, rapidfuzz, openpyxl     ║
╚══════════════════════════════════════════════════════════════════════════════╝
"""

# ── stdlib ─────────────────────────────────────────────────────────────────────
import os
import re
import threading
import warnings
from pathlib import Path
from typing import Dict, List, Optional, Tuple

# ── third-party ────────────────────────────────────────────────────────────────
import numpy as np
import pandas as pd

import customtkinter as ctk
import tkinter as tk
from tkinter import filedialog, messagebox, ttk

from sklearn.feature_extraction.text import TfidfVectorizer
from sklearn.metrics.pairwise import cosine_similarity
from rapidfuzz import fuzz

warnings.filterwarnings("ignore")
ctk.set_appearance_mode("light")
ctk.set_default_color_theme("blue")


# ═══════════════════════════════════════════════════════════════════════════════
#  DESIGN TOKENS
# ═══════════════════════════════════════════════════════════════════════════════

C: Dict[str, str] = {
    # Backgrounds
    "bg":          "#F4F6FA",
    "card":        "#FFFFFF",
    "stripe":      "#F9FAFB",
    # Header
    "navy":        "#0C1824",
    "navy2":       "#172535",
    "navy_border": "#1E3347",
    # Primary accent – steel blue
    "blue":        "#2563EB",
    "blue_lt":     "#EFF6FF",
    "blue_dk":     "#1D4ED8",
    "blue_mid":    "#3B82F6",
    # Semantic
    "teal":        "#0D9488",
    "teal_lt":     "#F0FDFA",
    "green":       "#16A34A",
    "green_lt":    "#F0FDF4",
    "amber":       "#B45309",
    "amber_lt":    "#FFFBEB",
    "red":         "#DC2626",
    "red_lt":      "#FEF2F2",
    # Text
    "text":        "#0C1824",
    "text2":       "#475569",
    "text3":       "#94A3B8",
    "text_inv":    "#FFFFFF",
    # Borders
    "border":      "#E2E8F0",
    "border2":     "#CBD5E1",
}

# Font tuples
F_DISPLAY  = ("Georgia",           18, "bold")
F_HEADING  = ("Georgia",           14, "bold")
F_SUBHEAD  = ("Georgia",           12, "bold")
F_BODY     = ("Helvetica",         10)
F_BODY_B   = ("Helvetica",         10, "bold")
F_SMALL    = ("Helvetica",          9)
F_SMALL_B  = ("Helvetica",          9, "bold")
F_MONO     = ("Courier New",        9)
F_BADGE    = ("Helvetica",          8)


# ═══════════════════════════════════════════════════════════════════════════════
#  DATA ENGINE
# ═══════════════════════════════════════════════════════════════════════════════

def smart_load(filepath: str) -> Tuple[pd.DataFrame, str]:
    """
    Auto-detect file type / delimiter and load into a string DataFrame.
    Returns (df, human_readable_method).
    """
    path = Path(filepath)
    ext  = path.suffix.lower()

    # ── Excel ──────────────────────────────────────────────────────────────────
    if ext in (".xlsx", ".xls", ".xlsm", ".xlsb"):
        df = pd.read_excel(filepath, dtype=str, keep_default_na=False)
        return df.fillna(""), f"Excel ({ext})"

    # ── Text-based: sniff delimiter from first few lines ──────────────────────
    try:
        with open(filepath, "r", encoding="utf-8", errors="replace") as fh:
            head = "".join(fh.readline() for _ in range(5))
    except Exception:
        head = ""

    pipes  = head.count("|")
    commas = head.count(",")
    tabs   = head.count("\t")

    if pipes >= commas and pipes >= tabs:
        sep, label = "|",  "pipe-delimited"
    elif tabs > commas:
        sep, label = "\t", "tab-delimited"
    else:
        sep, label = ",",  "comma-delimited"

    try:
        df = pd.read_csv(
            filepath, sep=sep, dtype=str,
            encoding="utf-8", errors="replace",
            keep_default_na=False, on_bad_lines="skip",
        )
        return df.fillna(""), f"{ext.upper().lstrip('.')} ({label})"
    except Exception:
        pass

    # ── Raw fallback ───────────────────────────────────────────────────────────
    with open(filepath, "r", encoding="utf-8", errors="replace") as fh:
        lines = [ln.strip() for ln in fh if ln.strip()]
    return pd.DataFrame({"raw_content": lines}), "raw text"


def parse_pipe_source(filepath: str) -> pd.DataFrame:
    """
    Parse a file whose structure is:
      Row 0 :  ZONE_NAME|EDC_MODEL_NAME|STEWARD|MALCODE|…   ← column headers
      Row 1+:  /TX/AVORA/AASXC/SRX|ERCD{SZX FS}|Corporate Service|EDCV|…

    Also handles the degenerate case where the whole file was saved as a
    single-column CSV and the pipe string ends up as the column *name*.
    """
    with open(filepath, "r", encoding="utf-8", errors="replace") as fh:
        raw = [ln.rstrip("\r\n") for ln in fh if ln.strip()]

    if not raw:
        return pd.DataFrame()

    first = raw[0]

    # ── Degenerate: no pipes in first line – maybe saved as single CSV column ─
    if "|" not in first:
        try:
            tmp = pd.read_csv(
                filepath, dtype=str, keep_default_na=False,
                encoding="utf-8", errors="replace",
            )
            # Column name might itself be the pipe-header string
            first_col_name = tmp.columns[0]
            if "|" in first_col_name:
                headers = [h.strip() for h in first_col_name.split("|") if h.strip()]
                rows = []
                for cell in tmp.iloc[:, 0]:
                    parts = str(cell).split("|")
                    parts = (parts + [""] * len(headers))[: len(headers)]
                    rows.append([p.strip() for p in parts])
                return pd.DataFrame(rows, columns=headers)
        except Exception:
            pass
        return pd.DataFrame({"content": raw})

    # ── Standard: first line = pipe-separated headers ─────────────────────────
    headers = [h.strip() for h in first.split("|")]
    # Drop trailing empty headers
    while headers and not headers[-1]:
        headers.pop()
    headers = [h or f"col_{i}" for i, h in enumerate(headers)]

    rows = []
    for line in raw[1:]:
        parts = line.split("|")
        parts = (parts + [""] * len(headers))[: len(headers)]
        rows.append([p.strip() for p in parts])

    df = pd.DataFrame(rows, columns=headers)
    return df


def detect_key_columns(df: pd.DataFrame) -> List[str]:
    """
    Return column names that are likely to hold key identifiers
    (malcode, table name, zone, etc.) — used to focus matching.
    """
    pattern = re.compile(
        r"(mal.?code|malcode|mal_code|table.?name|zone.?name|"
        r"column.?name|steward|model|edm|edc|schema|database|"
        r"owner|object|asset|domain|class)",
        re.IGNORECASE,
    )
    return [c for c in df.columns if pattern.search(str(c))]


# ── Text normalisation ─────────────────────────────────────────────────────────

def normalise(v) -> str:
    """Lowercase, collapse separators, strip punctuation."""
    if v is None:
        return ""
    if isinstance(v, float) and np.isnan(v):
        return ""
    s = str(v).lower().strip()
    s = re.sub(r"[\-_\s]+", " ", s)
    s = re.sub(r"[^\w\s/]", " ", s)
    return re.sub(r"\s+", " ", s).strip()


def tokenise(v: str) -> List[str]:
    """
    Produce every meaningful search token for a value:
    • The normalised full string
    • Each component split on  / (for path-like values)
    • Each word-token split on punctuation/separators
    """
    if not v:
        return []
    result: set = set()

    full = normalise(v)
    if full:
        result.add(full)

    for part in v.split("/"):
        n = normalise(part)
        if n and len(n) > 1:
            result.add(n)

    for part in re.split(r"[/\\_\-\s{}()\[\],;]", v):
        n = normalise(part)
        if n and len(n) > 1:
            result.add(n)

    return list(result)


# ── Matching engine ─────────────────────────────────────────────────────────────

class MatchEngine:
    """
    Multi-strategy similarity engine:
      1. TF-IDF char-ngram cosine similarity  (vocabulary-level)
      2. Rapidfuzz token-sort ratio           (sequence-level)
      3. Jaccard token overlap                (set-level)
      4. Substring containment               (exact sub-string)

    All four are blended into a [0,1] composite score.
    Values below `threshold` are dropped.
    """

    def __init__(self, threshold: float = 0.60):
        self.threshold   = threshold
        self._tfidf: Optional[TfidfVectorizer] = None
        self._matrix     = None
        self._ref_norms: List[str]                    = []
        self._ref_meta:  List[Tuple[int, str, str]]   = []   # (row, col, original)

    # ── Build reference index ─────────────────────────────────────────────────
    def fit(self, ref_df: pd.DataFrame, status_cb=None):
        self._ref_norms = []
        self._ref_meta  = []

        for col in ref_df.columns:
            for ridx, val in enumerate(ref_df[col]):
                raw = str(val).strip()
                if not raw:
                    continue
                for token in tokenise(raw):
                    self._ref_norms.append(token)
                    self._ref_meta.append((ridx, col, raw))

        if not self._ref_norms:
            return

        if status_cb:
            status_cb(f"Vectorising {len(self._ref_norms):,} reference tokens…")

        self._tfidf = TfidfVectorizer(
            analyzer="char_wb",
            ngram_range=(2, 4),
            max_features=80_000,
            sublinear_tf=True,
            min_df=1,
        )
        self._matrix = self._tfidf.fit_transform(self._ref_norms)

    # ── Query a single value ──────────────────────────────────────────────────
    def query(self, value: str, top_k: int = 5) -> List[Tuple]:
        """Returns [(score, ref_row_idx, ref_col, ref_original), …] sorted desc."""
        if not self._tfidf or not self._ref_norms:
            return []

        q_tokens = tokenise(value)
        if not q_tokens:
            return []

        best: Dict[Tuple[int, str], Tuple[float, str]] = {}   # (row,col) → (score, orig)

        for token in q_tokens:
            if len(token) < 2:
                continue
            try:
                q_vec   = self._tfidf.transform([token])
                sims    = cosine_similarity(q_vec, self._matrix).flatten()
                top_ix  = sims.argsort()[-(top_k * 4):][::-1]

                for ix in top_ix:
                    tsim = float(sims[ix])
                    if tsim < 0.05:
                        break
                    ref_norm  = self._ref_norms[ix]
                    meta      = self._ref_meta[ix]
                    composite = self._composite(token, ref_norm)
                    final     = tsim * 0.30 + composite * 0.70

                    key = (meta[0], meta[1])
                    if final > best.get(key, (-1, ""))[0]:
                        best[key] = (final, meta[2])
            except Exception:
                pass

        results = [
            (score, row, col, orig)
            for (row, col), (score, orig) in best.items()
            if score >= self.threshold
        ]
        results.sort(key=lambda x: -x[0])
        return results[:top_k]

    # ── Composite similarity ──────────────────────────────────────────────────
    def _composite(self, a: str, b: str) -> float:
        if not a or not b:
            return 0.0
        if a == b:
            return 1.0
        # Substring
        sub = 0.0
        if a in b or b in a:
            sub = min(len(a), len(b)) / max(len(a), len(b), 1)
        # Fuzzy token sort
        fz  = fuzz.token_sort_ratio(a, b) / 100.0
        # Jaccard on word tokens
        ta, tb = set(a.split()), set(b.split())
        jac = len(ta & tb) / len(ta | tb) if (ta | tb) else 0.0
        return max(sub, fz * 0.60 + jac * 0.40)

    # ── Full file sweep ───────────────────────────────────────────────────────
    def run(
        self,
        src_df:      pd.DataFrame,
        key_cols:    Optional[List[str]] = None,
        progress_cb  = None,
        status_cb    = None,
    ) -> pd.DataFrame:

        work_cols = key_cols if key_cols else list(src_df.columns)
        work_cols = [c for c in work_cols if c in src_df.columns]

        total  = sum(
            src_df[c].apply(lambda x: bool(str(x).strip())).sum()
            for c in work_cols
        )
        done   = 0
        output = []

        for col in work_cols:
            for ridx, val in enumerate(src_df[col]):
                raw = str(val).strip()
                if not raw or len(raw) < 2:
                    done += 1
                    continue

                for score, ref_row, ref_col, ref_val in self.query(raw, top_k=3):
                    output.append({
                        "Src Row":    ridx + 1,
                        "Src Column": col,
                        "Src Value":  raw,
                        "Ref Row":    ref_row + 1,
                        "Ref Column": ref_col,
                        "Ref Value":  ref_val,
                        "Score %":    round(score * 100, 1),
                        "Match Type": _classify(raw, ref_val),
                    })

                done += 1
                if progress_cb and total > 0:
                    progress_cb(done / total)
                if status_cb and done % 50 == 0:
                    status_cb(f"Matching… {done:,} / {total:,} values")

        if not output:
            return pd.DataFrame(columns=[
                "Src Row", "Src Column", "Src Value",
                "Ref Row", "Ref Column", "Ref Value",
                "Score %", "Match Type",
            ])

        df = pd.DataFrame(output)
        df.sort_values("Score %", ascending=False, inplace=True)
        df.drop_duplicates(subset=["Src Value", "Ref Value"], keep="first", inplace=True)
        df.reset_index(drop=True, inplace=True)
        return df


def _classify(a: str, b: str) -> str:
    na, nb = normalise(a), normalise(b)
    if na == nb:               return "Exact"
    if na in nb or nb in na:   return "Substring"
    if set(na.split()) & set(nb.split()):
        return "Token Overlap"
    return "Fuzzy"


# ═══════════════════════════════════════════════════════════════════════════════
#  SHARED WIDGET HELPERS
# ═══════════════════════════════════════════════════════════════════════════════

def _ttk_style():
    """Configure shared ttk styles (idempotent)."""
    s = ttk.Style()
    try:
        s.theme_use("clam")
    except Exception:
        pass
    s.configure(
        "DF.Treeview",
        background=C["card"],
        foreground=C["text"],
        fieldbackground=C["card"],
        rowheight=26,
        font=F_SMALL,
        bordercolor=C["border"],
        relief="flat",
    )
    s.configure(
        "DF.Treeview.Heading",
        background=C["navy"],
        foreground=C["text_inv"],
        font=F_SMALL_B,
        relief="flat",
        padding=(6, 5),
    )
    s.map(
        "DF.Treeview",
        background=[("selected", C["blue_lt"])],
        foreground=[("selected", C["blue_dk"])],
    )
    s.configure(
        "DF.Horizontal.TScrollbar",
        troughcolor=C["bg"],
        background=C["border2"],
    )


def make_scrolled_tree(parent, columns: List[str], col_widths: Optional[List[int]] = None) -> ttk.Treeview:
    """Create a styled Treeview with vertical + horizontal scrollbars."""
    _ttk_style()

    vsb = ttk.Scrollbar(parent, orient="vertical")
    hsb = ttk.Scrollbar(parent, orient="horizontal")

    tree = ttk.Treeview(
        parent,
        columns=columns,
        show="headings",
        style="DF.Treeview",
        yscrollcommand=vsb.set,
        xscrollcommand=hsb.set,
        selectmode="browse",
    )
    vsb.configure(command=tree.yview)
    hsb.configure(command=tree.xview)

    default_w = max(80, 1100 // max(len(columns), 1))
    for i, col in enumerate(columns):
        w = (col_widths[i] if col_widths and i < len(col_widths) else default_w)
        tree.heading(col, text=col, anchor="w")
        tree.column(col, width=w, minwidth=50, anchor="w", stretch=True)

    vsb.pack(side="right", fill="y")
    hsb.pack(side="bottom", fill="x")
    tree.pack(fill="both", expand=True)

    tree.tag_configure("odd",          background=C["stripe"])
    tree.tag_configure("even",         background=C["card"])
    tree.tag_configure("Exact",        background="#F0FDF4", foreground="#166534")
    tree.tag_configure("Substring",    background=C["blue_lt"], foreground=C["blue_dk"])
    tree.tag_configure("Token Overlap",background=C["amber_lt"], foreground=C["amber"])
    tree.tag_configure("Fuzzy",        background=C["red_lt"], foreground=C["red"])
    tree.tag_configure("key_col",      background="#F0FDFA", foreground=C["teal"])

    return tree


def load_df_into_tree(tree: ttk.Treeview, df: pd.DataFrame, max_rows: int = 500,
                      key_cols: Optional[List[str]] = None):
    """Populate a Treeview from a DataFrame."""
    tree.delete(*tree.get_children())
    cols = list(df.columns)
    tree["columns"] = cols
    col_w = max(70, min(200, 1050 // max(len(cols), 1)))
    for c in cols:
        tree.heading(c, text=c, anchor="w")
        tree.column(c, width=col_w, minwidth=50, anchor="w")

    kset = set(key_cols or [])
    for i, (_, row) in enumerate(df.head(max_rows).iterrows()):
        vals = [str(row[c])[:150] for c in cols]
        tag  = "odd" if i % 2 else "even"
        tree.insert("", "end", values=vals, tags=(tag,))

    if len(df) > max_rows:
        msg = [f"  ⋯  {len(df) - max_rows:,} more rows not shown"] + [""] * (len(cols) - 1)
        tree.insert("", "end", values=msg)


# ═══════════════════════════════════════════════════════════════════════════════
#  APPLICATION
# ═══════════════════════════════════════════════════════════════════════════════

class App(ctk.CTk):

    def __init__(self):
        super().__init__()

        # ── Window ─────────────────────────────────────────────────────────────
        self.title("DataForge Pro")
        self.geometry("1360x840")
        self.minsize(1100, 720)
        self.configure(fg_color=C["bg"])

        # ── State ──────────────────────────────────────────────────────────────
        self.src_path:   Optional[str]           = None
        self.ref_path:   Optional[str]           = None
        self.src_df:     Optional[pd.DataFrame]  = None
        self.ref_df:     Optional[pd.DataFrame]  = None
        self.results_df: Optional[pd.DataFrame]  = None
        self._key_col_vars: Dict[str, tk.BooleanVar] = {}
        self._step: int = 0

        # ── Build ──────────────────────────────────────────────────────────────
        self._build_header()
        self._build_step_bar()
        self._content_host = tk.Frame(self, bg=C["bg"])
        self._content_host.pack(fill="both", expand=True)
        self._build_status_bar()

        # ── Frames ─────────────────────────────────────────────────────────────
        self._pages: Dict[int, tk.Frame] = {}
        self._build_page_upload()
        self._build_page_preview()
        self._build_page_match_cfg()
        self._build_page_results()

        self._goto(0)

    # ══════════════════════════════════════════════════════════════════════════
    #  CHROME
    # ══════════════════════════════════════════════════════════════════════════

    def _build_header(self):
        bar = tk.Frame(self, bg=C["navy"], height=58)
        bar.pack(fill="x")
        bar.pack_propagate(False)

        # Accent line on left
        tk.Frame(bar, bg=C["blue"], width=4).pack(side="left", fill="y")

        inner = tk.Frame(bar, bg=C["navy"])
        inner.pack(side="left", fill="both", expand=True, padx=18)

        # Title
        tk.Label(inner, text="DataForge Pro",
                 bg=C["navy"], fg="#FFFFFF",
                 font=F_DISPLAY).pack(side="left", pady=10)

        # Subtitle
        tk.Label(inner, text="  ·  Intelligent Data Cleaning & Cross-File Matching",
                 bg=C["navy"], fg="#7CA8CC",
                 font=F_BODY).pack(side="left", pady=10)

        # Right-side version + status
        right = tk.Frame(bar, bg=C["navy"])
        right.pack(side="right", padx=18)

        # Processing spinner label
        self._spinner_lbl = tk.Label(
            right, text="", bg=C["navy"], fg=C["blue_mid"],
            font=F_SMALL_B,
        )
        self._spinner_lbl.pack(side="right", padx=(8, 0))

        badge = tk.Label(right, text="v1.0",
                         bg=C["navy2"], fg="#7CA8CC",
                         font=F_BADGE, padx=8, pady=3)
        badge.pack(side="right")

    def _build_step_bar(self):
        bar = tk.Frame(self, bg=C["card"],
                       highlightthickness=1, highlightbackground=C["border"])
        bar.pack(fill="x")
        self._step_circles: List[tk.Label] = []
        self._step_texts:   List[tk.Label] = []

        STEPS = [
            ("1", "Upload Files"),
            ("2", "Clean & Preview"),
            ("3", "Configure Match"),
            ("4", "Results"),
        ]

        inner = tk.Frame(bar, bg=C["card"])
        inner.pack(pady=10)

        for i, (num, label) in enumerate(STEPS):
            if i:
                tk.Frame(inner, bg=C["border2"], width=44, height=2).pack(
                    side="left", pady=6)

            cell = tk.Frame(inner, bg=C["card"])
            cell.pack(side="left", padx=6)

            c = tk.Label(cell, text=num, bg=C["text3"], fg="#FFF",
                         font=F_SMALL_B, width=3, pady=2)
            c.pack(side="left")

            lbl = tk.Label(cell, text=f"  {label}",
                           bg=C["card"], fg=C["text3"], font=F_SMALL)
            lbl.pack(side="left")

            self._step_circles.append(c)
            self._step_texts.append(lbl)

    def _update_step_bar(self, active: int):
        for i, (circ, lbl) in enumerate(zip(self._step_circles, self._step_texts)):
            if i < active:
                circ.configure(bg=C["teal"])
                lbl.configure(fg=C["teal"], font=F_SMALL)
            elif i == active:
                circ.configure(bg=C["blue"])
                lbl.configure(fg=C["blue"], font=F_SMALL_B)
            else:
                circ.configure(bg=C["text3"])
                lbl.configure(fg=C["text3"], font=F_SMALL)

    def _build_status_bar(self):
        bar = tk.Frame(self, bg=C["card"], height=28,
                       highlightthickness=1, highlightbackground=C["border"])
        bar.pack(fill="x", side="bottom")
        bar.pack_propagate(False)

        self._status_var = tk.StringVar(value="Ready — upload your files to begin.")
        tk.Label(bar, textvariable=self._status_var,
                 bg=C["card"], fg=C["text2"],
                 font=F_SMALL, anchor="w",
                 padx=12).pack(fill="y", side="left")

    def _goto(self, step: int):
        for p in self._pages.values():
            p.pack_forget()
        self._pages[step].pack(fill="both", expand=True)
        self._step = step
        self._update_step_bar(step)

    def _status(self, msg: str):
        self._status_var.set(msg)

    def _set_busy(self, msg: str = ""):
        if msg:
            self._spinner_lbl.configure(text=f"⟳  {msg}")
        else:
            self._spinner_lbl.configure(text="")

    # ══════════════════════════════════════════════════════════════════════════
    #  PAGE 0 — UPLOAD
    # ══════════════════════════════════════════════════════════════════════════

    def _build_page_upload(self):
        pg = tk.Frame(self._content_host, bg=C["bg"])
        self._pages[0] = pg

        # ── Title ──────────────────────────────────────────────────────────────
        tk.Label(pg, text="Upload Your Files",
                 bg=C["bg"], fg=C["text"], font=F_HEADING).pack(pady=(22, 3))
        tk.Label(pg,
                 text="Select the messy pipe-delimited source and the reference file you want to match against.",
                 bg=C["bg"], fg=C["text2"], font=F_BODY).pack()

        # ── Two upload cards ───────────────────────────────────────────────────
        row = tk.Frame(pg, bg=C["bg"])
        row.pack(pady=20, padx=40, fill="x")
        row.columnconfigure(0, weight=1)
        row.columnconfigure(1, weight=1)

        # Source card
        src_card = tk.Frame(row, bg=C["card"],
                            highlightthickness=1, highlightbackground=C["border"],
                            padx=28, pady=22)
        src_card.grid(row=0, column=0, padx=(0, 12), sticky="nsew")

        tk.Label(src_card, text="📄", bg=C["card"], font=("Helvetica", 36)).pack()
        tk.Label(src_card, text="Source File",
                 bg=C["card"], fg=C["text"], font=F_SUBHEAD).pack(pady=(6, 2))
        tk.Label(src_card,
                 text="Pipe-delimited file with headers in row 1\nand values separated by  |",
                 bg=C["card"], fg=C["text2"], font=F_SMALL, justify="center").pack()

        # Option toggle
        self._use_pipe_var = tk.BooleanVar(value=True)
        tk.Checkbutton(src_card,
                       text="First row contains pipe-separated column headers",
                       variable=self._use_pipe_var,
                       bg=C["card"], fg=C["text2"], font=F_SMALL,
                       activebackground=C["card"],
                       selectcolor=C["blue_lt"]).pack(pady=(10, 0))

        self._src_browse_btn = tk.Button(
            src_card, text="Browse Source File…",
            bg=C["blue"], fg=C["text_inv"], font=F_BODY_B,
            relief="flat", padx=18, pady=9, cursor="hand2",
            command=self._browse_src,
        )
        self._src_browse_btn.pack(pady=(12, 4))

        self._src_info_var = tk.StringVar(value="No file selected")
        self._src_info_lbl = tk.Label(src_card, textvariable=self._src_info_var,
                                       bg=C["card"], fg=C["text2"],
                                       font=F_SMALL, wraplength=300)
        self._src_info_lbl.pack()

        # Reference card
        ref_card = tk.Frame(row, bg=C["card"],
                            highlightthickness=1, highlightbackground=C["border"],
                            padx=28, pady=22)
        ref_card.grid(row=0, column=1, padx=(12, 0), sticky="nsew")

        tk.Label(ref_card, text="📊", bg=C["card"], font=("Helvetica", 36)).pack()
        tk.Label(ref_card, text="Reference File",
                 bg=C["card"], fg=C["text"], font=F_SUBHEAD).pack(pady=(6, 2))
        tk.Label(ref_card,
                 text="Clean comparison file — any format accepted\n(Excel, CSV, TXT, TSV)",
                 bg=C["card"], fg=C["text2"], font=F_SMALL, justify="center").pack()

        tk.Label(ref_card, text="", bg=C["card"], height=2).pack()   # spacer

        self._ref_browse_btn = tk.Button(
            ref_card, text="Browse Reference File…",
            bg=C["teal"], fg=C["text_inv"], font=F_BODY_B,
            relief="flat", padx=18, pady=9, cursor="hand2",
            command=self._browse_ref,
        )
        self._ref_browse_btn.pack(pady=(12, 4))

        self._ref_info_var = tk.StringVar(value="No file selected")
        tk.Label(ref_card, textvariable=self._ref_info_var,
                 bg=C["card"], fg=C["text2"],
                 font=F_SMALL, wraplength=300).pack()

        # ── Proceed ────────────────────────────────────────────────────────────
        self._proceed_btn = tk.Button(
            pg,
            text="Parse & Clean  →",
            bg=C["text3"], fg=C["text_inv"],
            font=F_BODY_B, relief="flat",
            padx=28, pady=11, cursor="hand2",
            state="disabled",
            command=self._do_parse,
        )
        self._proceed_btn.pack(pady=8)

        # Help note
        note = (
            "Supported formats:  CSV · TXT · TSV · Excel (.xlsx / .xls / .xlsm)\n"
            "The app auto-detects delimiters and file types."
        )
        tk.Label(pg, text=note,
                 bg=C["bg"], fg=C["text3"], font=F_SMALL,
                 justify="center").pack(pady=4)

    def _browse_src(self):
        p = filedialog.askopenfilename(
            title="Select Source (Messy) File",
            filetypes=[
                ("All supported", "*.csv *.txt *.tsv *.xlsx *.xls *.xlsm"),
                ("CSV", "*.csv"),
                ("Text / TSV", "*.txt *.tsv"),
                ("Excel", "*.xlsx *.xls *.xlsm"),
                ("All files", "*.*"),
            ],
        )
        if p:
            self.src_path = p
            name = Path(p).name
            size = os.path.getsize(p) / 1024
            self._src_info_var.set(f"✓  {name}  ({size:.1f} KB)")
            self._src_info_lbl.configure(fg=C["green"])
            self._check_ready()

    def _browse_ref(self):
        p = filedialog.askopenfilename(
            title="Select Reference File",
            filetypes=[
                ("All supported", "*.csv *.txt *.tsv *.xlsx *.xls *.xlsm"),
                ("CSV", "*.csv"),
                ("Text / TSV", "*.txt *.tsv"),
                ("Excel", "*.xlsx *.xls *.xlsm"),
                ("All files", "*.*"),
            ],
        )
        if p:
            self.ref_path = p
            name = Path(p).name
            size = os.path.getsize(p) / 1024
            self._ref_info_var.set(f"✓  {name}  ({size:.1f} KB)")
            self._check_ready()

    def _check_ready(self):
        if self.src_path and self.ref_path:
            self._proceed_btn.configure(state="normal", bg=C["blue"])

    # ── Parse worker ──────────────────────────────────────────────────────────

    def _do_parse(self):
        self._proceed_btn.configure(state="disabled", text="Parsing…")
        self._set_busy("Parsing files…")
        threading.Thread(target=self._parse_worker, daemon=True).start()

    def _parse_worker(self):
        try:
            # ── Source ──────────────────────────────────────────────────────────
            if self._use_pipe_var.get():
                src = parse_pipe_source(self.src_path)
            else:
                src, _ = smart_load(self.src_path)

            # Normalise: strip cells, drop fully-empty columns
            src = src.apply(lambda col: col.map(lambda x: str(x).strip()))
            src = src.loc[:, (src != "").any(axis=0)]
            src = src.loc[:, ~src.columns.duplicated()]

            # ── Reference ───────────────────────────────────────────────────────
            ref, ref_method = smart_load(self.ref_path)
            ref = ref.apply(lambda col: col.map(lambda x: str(x).strip()))
            ref = ref.loc[:, (ref != "").any(axis=0)]
            ref = ref.loc[:, ~ref.columns.duplicated()]

            self.src_df = src
            self.ref_df = ref

            self.after(0, lambda: self._parse_done(ref_method))
        except Exception as exc:
            import traceback; traceback.print_exc()
            self.after(0, lambda e=exc: self._on_error("Parse Error", e))

    def _parse_done(self, ref_method: str):
        self._proceed_btn.configure(state="normal",
                                    text="Parse & Clean  →",
                                    bg=C["blue"])
        self._set_busy()
        self._status(
            f"Source: {len(self.src_df):,} rows × {len(self.src_df.columns)} cols  ·  "
            f"Reference ({ref_method}): {len(self.ref_df):,} rows × {len(self.ref_df.columns)} cols"
        )
        self._populate_preview()
        self._goto(1)

    # ══════════════════════════════════════════════════════════════════════════
    #  PAGE 1 — CLEAN & PREVIEW
    # ══════════════════════════════════════════════════════════════════════════

    def _build_page_preview(self):
        pg = tk.Frame(self._content_host, bg=C["bg"])
        self._pages[1] = pg

        # ── Top bar ────────────────────────────────────────────────────────────
        top = tk.Frame(pg, bg=C["bg"])
        top.pack(fill="x", padx=20, pady=(14, 4))

        tk.Label(top, text="Clean & Preview",
                 bg=C["bg"], fg=C["text"], font=F_HEADING).pack(side="left")

        right = tk.Frame(top, bg=C["bg"])
        right.pack(side="right")

        _back_btn(right, "← Upload", lambda: self._goto(0))
        _nav_btn(right,  "Configure Matching  →", self._go_match_cfg,
                 bg=C["blue"])

        # ── Stats row ──────────────────────────────────────────────────────────
        self._prev_stats_var = tk.StringVar(value="")
        tk.Label(pg, textvariable=self._prev_stats_var,
                 bg=C["bg"], fg=C["text2"], font=F_SMALL).pack(padx=20, anchor="w")

        # ── Tab switcher ───────────────────────────────────────────────────────
        tab_bar = tk.Frame(pg, bg=C["card"],
                           highlightthickness=1, highlightbackground=C["border"])
        tab_bar.pack(fill="x", padx=20, pady=(4, 0))

        self._prev_tab = tk.StringVar(value="source")
        for txt, val in [("✓ Source File (Cleaned)", "source"),
                         ("Reference File",          "ref")]:
            tk.Radiobutton(
                tab_bar, text=txt,
                variable=self._prev_tab, value=val,
                bg=C["card"], fg=C["text"],
                selectcolor=C["blue_lt"],
                font=F_BODY,
                activebackground=C["card"],
                command=self._switch_prev_tab,
            ).pack(side="left", padx=14, pady=7)

        # ── Tree ───────────────────────────────────────────────────────────────
        tree_host = tk.Frame(pg, bg=C["bg"])
        tree_host.pack(fill="both", expand=True, padx=20, pady=(4, 6))

        self._prev_tree = make_scrolled_tree(tree_host, ["Loading…"])

    def _populate_preview(self):
        self._switch_prev_tab()

    def _switch_prev_tab(self, *_):
        tab = self._prev_tab.get()
        df  = self.src_df if tab == "source" else self.ref_df
        if df is None:
            return
        self._prev_stats_var.set(
            f"{len(df):,} rows  ·  {len(df.columns)} columns  ·  "
            f"{int(df.apply(lambda c: (c != '').sum()).sum()):,} non-empty cells"
        )
        load_df_into_tree(self._prev_tree, df, max_rows=300)

    def _go_match_cfg(self):
        self._populate_match_cfg()
        self._goto(2)

    # ══════════════════════════════════════════════════════════════════════════
    #  PAGE 2 — CONFIGURE MATCHING
    # ══════════════════════════════════════════════════════════════════════════

    def _build_page_match_cfg(self):
        pg = tk.Frame(self._content_host, bg=C["bg"])
        self._pages[2] = pg

        # ── Top bar ────────────────────────────────────────────────────────────
        top = tk.Frame(pg, bg=C["bg"])
        top.pack(fill="x", padx=20, pady=(14, 8))

        tk.Label(top, text="Configure Matching",
                 bg=C["bg"], fg=C["text"], font=F_HEADING).pack(side="left")

        rbar = tk.Frame(top, bg=C["bg"])
        rbar.pack(side="right")

        _back_btn(rbar, "← Preview", lambda: self._goto(1))

        self._run_btn = tk.Button(
            rbar, text="▶  Run Matching Engine",
            bg=C["teal"], fg=C["text_inv"],
            font=F_BODY_B, relief="flat",
            padx=18, pady=7, cursor="hand2",
            command=self._do_match,
        )
        self._run_btn.pack(side="left", padx=6)

        # ── Two-column body ────────────────────────────────────────────────────
        body = tk.Frame(pg, bg=C["bg"])
        body.pack(fill="both", expand=True, padx=20)
        body.columnconfigure(0, weight=0)
        body.columnconfigure(1, weight=1)

        # ── LEFT: settings panel ───────────────────────────────────────────────
        left = tk.Frame(body, bg=C["card"], width=310,
                        highlightthickness=1, highlightbackground=C["border"],
                        padx=22, pady=18)
        left.grid(row=0, column=0, sticky="nsew", padx=(0, 10))
        left.pack_propagate(False)
        left.grid_propagate(False)

        _section_label(left, "Match Settings")
        tk.Frame(left, bg=C["border"], height=1).pack(fill="x", pady=(0, 12))

        # Threshold
        tk.Label(left, text="Similarity Threshold",
                 bg=C["card"], fg=C["text"], font=F_BODY_B).pack(anchor="w")
        tk.Label(left,
                 text="Higher = stricter · Lower = more (lenient) results",
                 bg=C["card"], fg=C["text3"], font=F_SMALL).pack(anchor="w", pady=(0, 6))

        thresh_row = tk.Frame(left, bg=C["card"])
        thresh_row.pack(fill="x", pady=(0, 16))

        self._thresh_lbl = tk.Label(thresh_row, text="60%",
                                     bg=C["card"], fg=C["blue"],
                                     font=("Georgia", 15, "bold"), width=5)
        self._thresh_lbl.pack(side="right")

        self._thresh_var = tk.DoubleVar(value=0.60)
        tk.Scale(
            thresh_row,
            variable=self._thresh_var,
            from_=0.15, to=0.95,
            resolution=0.05,
            orient="horizontal",
            bg=C["card"], fg=C["text2"],
            troughcolor=C["border2"],
            activebackground=C["blue"],
            highlightthickness=0, relief="flat", bd=0,
            showvalue=False,
            command=lambda v: self._thresh_lbl.configure(
                text=f"{int(float(v)*100)}%"),
        ).pack(side="left", fill="x", expand=True)

        # Column strategy
        tk.Label(left, text="Column Strategy",
                 bg=C["card"], fg=C["text"], font=F_BODY_B).pack(anchor="w", pady=(4, 2))
        self._col_mode = tk.StringVar(value="smart")
        for txt, val in [
            ("Smart — key columns only  (faster)", "smart"),
            ("All columns  (thorough, slower)",    "all"),
        ]:
            tk.Radiobutton(
                left, text=txt, variable=self._col_mode, value=val,
                bg=C["card"], fg=C["text"], font=F_SMALL,
                activebackground=C["card"], selectcolor=C["blue_lt"],
            ).pack(anchor="w")

        tk.Frame(left, bg=C["border"], height=1).pack(fill="x", pady=14)

        # Key columns checkboxes (populated dynamically)
        tk.Label(left, text="Key Columns in Source",
                 bg=C["card"], fg=C["text"], font=F_BODY_B).pack(anchor="w")
        tk.Label(left, text="Auto-detected. Uncheck to exclude.",
                 bg=C["card"], fg=C["text3"], font=F_SMALL).pack(anchor="w", pady=(0, 4))

        self._key_col_host = tk.Frame(left, bg=C["card"])
        self._key_col_host.pack(fill="x")

        tk.Frame(left, bg=C["border"], height=1).pack(fill="x", pady=14)

        # Progress
        self._prog_var  = tk.StringVar(value="")
        self._prog_lbl  = tk.Label(left, textvariable=self._prog_var,
                                    bg=C["card"], fg=C["text2"], font=F_SMALL)
        self._prog_lbl.pack(anchor="w")
        self._prog_bar  = ctk.CTkProgressBar(left, width=260, height=7)
        self._prog_bar.set(0)
        self._prog_bar.pack(pady=4, anchor="w")

        # ── RIGHT: column overview tree ────────────────────────────────────────
        right = tk.Frame(body, bg=C["card"],
                         highlightthickness=1, highlightbackground=C["border"],
                         padx=12, pady=12)
        right.grid(row=0, column=1, sticky="nsew")

        _section_label(right, "Column Overview  —  Source File")
        tk.Label(right,
                 text="★ marks detected key columns  ·  Counts show non-empty values",
                 bg=C["card"], fg=C["text3"], font=F_SMALL).pack(anchor="w", pady=(0, 6))

        self._col_overview_tree = make_scrolled_tree(
            right,
            ["Column", "Non-Empty", "Sample Values"],
            [200, 90, 450],
        )

    def _populate_match_cfg(self):
        if self.src_df is None:
            return

        # Refresh key columns checkboxes
        for w in self._key_col_host.winfo_children():
            w.destroy()
        self._key_col_vars.clear()

        key_cols = detect_key_columns(self.src_df)
        if key_cols:
            for col in key_cols:
                v = tk.BooleanVar(value=True)
                self._key_col_vars[col] = v
                tk.Checkbutton(
                    self._key_col_host, text=col[:46],
                    variable=v,
                    bg=C["card"], fg=C["text"], font=F_SMALL,
                    activebackground=C["card"], selectcolor=C["teal_lt"],
                ).pack(anchor="w")
        else:
            tk.Label(self._key_col_host,
                     text="None detected — will use all columns",
                     bg=C["card"], fg=C["text3"], font=F_SMALL).pack(anchor="w")

        # Column overview
        kset = set(key_cols)
        rows = []
        for col in self.src_df.columns:
            non_empty = int((self.src_df[col] != "").sum())
            samples   = self.src_df.loc[self.src_df[col] != "", col].head(3).tolist()
            sample_str = "  ·  ".join(str(s)[:35] for s in samples)
            star  = "★ " if col in kset else "   "
            rows.append((f"{star}{col}", non_empty, sample_str))

        self._col_overview_tree.delete(*self._col_overview_tree.get_children())
        self._col_overview_tree["columns"] = ["Column", "Non-Empty", "Sample Values"]
        self._col_overview_tree["show"] = "headings"
        self._col_overview_tree.heading("Column",        text="Column",       anchor="w")
        self._col_overview_tree.heading("Non-Empty",     text="Non-Empty",    anchor="center")
        self._col_overview_tree.heading("Sample Values", text="Sample Values",anchor="w")
        self._col_overview_tree.column("Column",        width=200, minwidth=120, anchor="w")
        self._col_overview_tree.column("Non-Empty",     width=90,  minwidth=70,  anchor="center")
        self._col_overview_tree.column("Sample Values", width=500, minwidth=200, anchor="w")

        for i, (col, cnt, sample) in enumerate(rows):
            tag = "key_col" if "★" in col else ("odd" if i % 2 else "even")
            self._col_overview_tree.insert(
                "", "end", values=(col, cnt, sample), tags=(tag,))

    # ── Match worker ──────────────────────────────────────────────────────────

    def _do_match(self):
        threshold = self._thresh_var.get()
        mode      = self._col_mode.get()

        key_cols: Optional[List[str]]
        if mode == "smart":
            key_cols = [c for c, v in self._key_col_vars.items() if v.get()]
            if not key_cols:
                key_cols = detect_key_columns(self.src_df)
            if not key_cols:
                key_cols = None  # fall back to all
        else:
            key_cols = None

        self._run_btn.configure(state="disabled", text="Running…")
        self._prog_bar.set(0)
        self._prog_var.set("")
        self._set_busy("Matching…")

        threading.Thread(
            target=self._match_worker,
            args=(threshold, key_cols),
            daemon=True,
        ).start()

    def _match_worker(self, threshold: float, key_cols: Optional[List[str]]):
        try:
            engine = MatchEngine(threshold=threshold)

            def status(m):
                self.after(0, lambda msg=m: self._status(msg))

            def prog(p):
                self.after(0, lambda v=p: self._prog_bar.set(v))

            def prog_lbl(m):
                self.after(0, lambda msg=m: self._prog_var.set(msg))

            status("Building reference index…")
            engine.fit(self.ref_df, status_cb=status)

            results = engine.run(
                self.src_df,
                key_cols=key_cols,
                progress_cb=prog,
                status_cb=lambda m: (prog_lbl(m), status(m)),
            )
            self.results_df = results
            self.after(0, self._match_done)
        except Exception as exc:
            import traceback; traceback.print_exc()
            self.after(0, lambda e=exc: self._on_error("Matching Error", e))

    def _match_done(self):
        n = len(self.results_df) if self.results_df is not None else 0
        self._prog_bar.set(1.0)
        self._prog_var.set(f"Complete — {n:,} matches found")
        self._run_btn.configure(state="normal",
                                text="▶  Run Matching Engine",
                                bg=C["teal"])
        self._set_busy()
        self._status(f"Matching complete — {n:,} matches found across "
                     f"{len(self.src_df):,} source rows")
        self._populate_results()
        self._goto(3)

    # ══════════════════════════════════════════════════════════════════════════
    #  PAGE 3 — RESULTS
    # ══════════════════════════════════════════════════════════════════════════

    def _build_page_results(self):
        pg = tk.Frame(self._content_host, bg=C["bg"])
        self._pages[3] = pg

        # ── Top bar ────────────────────────────────────────────────────────────
        top = tk.Frame(pg, bg=C["bg"])
        top.pack(fill="x", padx=20, pady=(14, 4))

        tk.Label(top, text="Match Results",
                 bg=C["bg"], fg=C["text"], font=F_HEADING).pack(side="left")

        rbar = tk.Frame(top, bg=C["bg"])
        rbar.pack(side="right")

        _back_btn(rbar, "← Reconfigure", lambda: self._goto(2))
        _nav_btn(rbar, "⬇  Export Matches",  self._export_results,  bg=C["green"])
        _nav_btn(rbar, "⬇  Export Cleaned Source", self._export_cleaned, bg=C["teal"])

        # ── Summary stats ──────────────────────────────────────────────────────
        self._summary_host = tk.Frame(pg, bg=C["bg"])
        self._summary_host.pack(fill="x", padx=20, pady=(4, 8))

        # ── Filter bar ─────────────────────────────────────────────────────────
        fbar = tk.Frame(pg, bg=C["card"],
                        highlightthickness=1, highlightbackground=C["border"])
        fbar.pack(fill="x", padx=20, pady=(0, 4))

        tk.Label(fbar, text="Search:", bg=C["card"], fg=C["text2"],
                 font=F_SMALL).pack(side="left", padx=10, pady=7)

        self._flt_var = tk.StringVar()
        self._flt_var.trace_add("write", self._apply_filter)
        tk.Entry(fbar, textvariable=self._flt_var,
                 bg=C["stripe"], fg=C["text"],
                 font=F_SMALL, relief="flat",
                 width=28).pack(side="left", padx=4)

        tk.Label(fbar, text="Type:", bg=C["card"], fg=C["text2"],
                 font=F_SMALL).pack(side="left", padx=(16, 4))
        self._type_var = tk.StringVar(value="All")
        type_cb = ttk.Combobox(
            fbar, textvariable=self._type_var,
            values=["All", "Exact", "Substring", "Token Overlap", "Fuzzy"],
            state="readonly", width=14, font=F_SMALL,
        )
        type_cb.pack(side="left", pady=7)
        type_cb.bind("<<ComboboxSelected>>", self._apply_filter)

        tk.Label(fbar, text="Min Score %:", bg=C["card"], fg=C["text2"],
                 font=F_SMALL).pack(side="left", padx=(16, 4))
        self._min_score_var = tk.IntVar(value=60)
        tk.Spinbox(
            fbar, from_=0, to=100,
            textvariable=self._min_score_var,
            width=5, font=F_SMALL,
            command=self._apply_filter,
        ).pack(side="left", pady=7)

        self._flt_count_var = tk.StringVar(value="")
        tk.Label(fbar, textvariable=self._flt_count_var,
                 bg=C["card"], fg=C["text3"],
                 font=F_SMALL).pack(side="right", padx=12)

        # ── Results tree ───────────────────────────────────────────────────────
        tree_host = tk.Frame(pg, bg=C["bg"])
        tree_host.pack(fill="both", expand=True, padx=20, pady=(0, 6))

        RES_COLS = ["Src Row", "Src Column", "Src Value",
                    "Ref Row", "Ref Column", "Ref Value",
                    "Score %", "Match Type"]
        RES_WIDTHS = [60, 130, 220, 60, 130, 220, 70, 110]

        self._res_tree = make_scrolled_tree(tree_host, RES_COLS, RES_WIDTHS)
        for col in RES_COLS:
            self._res_tree.heading(
                col, text=col, anchor="w",
                command=lambda c=col: self._sort_results(c),
            )

    def _populate_results(self):
        if self.results_df is None:
            return

        # ── Summary cards ──────────────────────────────────────────────────────
        for w in self._summary_host.winfo_children():
            w.destroy()

        df = self.results_df
        stats = [
            ("Total Matches",    f"{len(df):,}",                              C["blue"]),
            ("Exact",            f"{(df['Match Type']=='Exact').sum():,}",     C["green"]),
            ("Substring",        f"{(df['Match Type']=='Substring').sum():,}", C["blue_mid"]),
            ("Token / Fuzzy",
             f"{df['Match Type'].isin(['Token Overlap','Fuzzy']).sum():,}",    C["amber"]),
            ("Avg Score",        f"{df['Score %'].mean():.1f}%",               C["teal"]),
            ("Unique Src Values",f"{df['Src Value'].nunique():,}",             C["navy2"]),
        ]

        for label, val, color in stats:
            card = tk.Frame(self._summary_host, bg=C["card"],
                            highlightthickness=1, highlightbackground=C["border"],
                            padx=16, pady=8)
            card.pack(side="left", padx=(0, 8))
            tk.Label(card, text=val,   bg=C["card"], fg=color,
                     font=("Georgia", 15, "bold")).pack()
            tk.Label(card, text=label, bg=C["card"], fg=C["text2"],
                     font=F_SMALL).pack()

        self._load_results_tree(df)

    def _load_results_tree(self, df: pd.DataFrame):
        tree = self._res_tree
        tree.delete(*tree.get_children())
        cols = ["Src Row", "Src Column", "Src Value",
                "Ref Row", "Ref Column", "Ref Value",
                "Score %", "Match Type"]
        cap = 3000
        for _, row in df.head(cap).iterrows():
            mtype = str(row.get("Match Type", ""))
            vals  = [str(row.get(c, ""))[:120] for c in cols]
            tree.insert("", "end", values=vals, tags=(mtype,))
        if len(df) > cap:
            tree.insert("", "end",
                        values=[f"  ⋯  {len(df)-cap:,} more rows not shown"]
                        + [""] * 7)
        self._flt_count_var.set(f"{len(df):,} matches shown")

    def _apply_filter(self, *_):
        if self.results_df is None:
            return
        df = self.results_df.copy()
        q  = self._flt_var.get().lower()
        if q:
            df = df[df.apply(lambda r: any(q in str(v).lower() for v in r), axis=1)]
        t = self._type_var.get()
        if t != "All":
            df = df[df["Match Type"] == t]
        ms = self._min_score_var.get()
        df = df[df["Score %"] >= ms]
        self._load_results_tree(df)

    _sort_asc: Dict[str, bool] = {}

    def _sort_results(self, col: str):
        if self.results_df is None:
            return
        asc = not self._sort_asc.get(col, False)
        self._sort_asc[col] = asc
        df = self.results_df.copy()
        # numeric sort for score
        if col == "Score %":
            df[col] = pd.to_numeric(df[col], errors="coerce")
        df.sort_values(col, ascending=asc, inplace=True)
        self.results_df = df
        self._load_results_tree(df)

    # ── Export ─────────────────────────────────────────────────────────────────

    def _export_results(self):
        if self.results_df is None or self.results_df.empty:
            messagebox.showinfo("Export", "No match results to export yet.")
            return
        path = filedialog.asksaveasfilename(
            defaultextension=".xlsx",
            filetypes=[("Excel Workbook", "*.xlsx"), ("CSV", "*.csv")],
            title="Export Match Results",
        )
        if not path:
            return
        try:
            if path.endswith(".csv"):
                self.results_df.to_csv(path, index=False, encoding="utf-8-sig")
            else:
                with pd.ExcelWriter(path, engine="openpyxl") as writer:
                    self.results_df.to_excel(
                        writer, sheet_name="Match Results", index=False)
                    if self.src_df is not None:
                        self.src_df.to_excel(
                            writer, sheet_name="Cleaned Source", index=False)
                    if self.ref_df is not None:
                        self.ref_df.to_excel(
                            writer, sheet_name="Reference File", index=False)
            self._status(f"Exported to {Path(path).name}")
            messagebox.showinfo("Export Complete",
                                f"Saved successfully:\n{path}")
        except Exception as exc:
            messagebox.showerror("Export Error", str(exc))

    def _export_cleaned(self):
        if self.src_df is None:
            messagebox.showinfo("Export", "No cleaned source to export yet.")
            return
        path = filedialog.asksaveasfilename(
            defaultextension=".xlsx",
            filetypes=[("Excel Workbook", "*.xlsx"), ("CSV", "*.csv")],
            title="Export Cleaned Source File",
        )
        if not path:
            return
        try:
            if path.endswith(".csv"):
                self.src_df.to_csv(path, index=False, encoding="utf-8-sig")
            else:
                self.src_df.to_excel(path, index=False)
            self._status(f"Cleaned source exported to {Path(path).name}")
            messagebox.showinfo("Export Complete",
                                f"Saved successfully:\n{path}")
        except Exception as exc:
            messagebox.showerror("Export Error", str(exc))

    # ── Error handler ──────────────────────────────────────────────────────────

    def _on_error(self, title: str, exc: Exception):
        self._set_busy()
        self._status(f"Error: {exc}")
        self._proceed_btn.configure(state="normal",
                                    text="Parse & Clean  →",
                                    bg=C["blue"])
        self._run_btn.configure(state="normal",
                                text="▶  Run Matching Engine",
                                bg=C["teal"])
        messagebox.showerror(title, str(exc))


# ═══════════════════════════════════════════════════════════════════════════════
#  SMALL WIDGET FACTORIES
# ═══════════════════════════════════════════════════════════════════════════════

def _section_label(parent, text: str):
    tk.Label(parent, text=text,
             bg=C["card"], fg=C["text"], font=F_SUBHEAD).pack(anchor="w", pady=(0, 4))


def _back_btn(parent, text: str, cmd):
    tk.Button(
        parent, text=text,
        bg=C["card"], fg=C["text2"],
        font=F_BODY, relief="flat",
        padx=12, pady=6, cursor="hand2",
        highlightthickness=1, highlightbackground=C["border2"],
        command=cmd,
    ).pack(side="left", padx=(0, 6))


def _nav_btn(parent, text: str, cmd, bg: str = C["blue"]):
    tk.Button(
        parent, text=text,
        bg=bg, fg=C["text_inv"],
        font=F_BODY_B, relief="flat",
        padx=14, pady=6, cursor="hand2",
        command=cmd,
    ).pack(side="left", padx=(0, 6))


# ═══════════════════════════════════════════════════════════════════════════════
#  DEPENDENCY BOOTSTRAP
# ═══════════════════════════════════════════════════════════════════════════════

def _check_deps():
    missing = []
    try:    import customtkinter   # noqa
    except ImportError: missing.append("customtkinter")
    try:    import sklearn         # noqa
    except ImportError: missing.append("scikit-learn")
    try:    import rapidfuzz       # noqa
    except ImportError: missing.append("rapidfuzz")
    try:    import openpyxl        # noqa
    except ImportError: missing.append("openpyxl")

    if missing:
        import subprocess, sys
        print(f"Installing missing packages: {', '.join(missing)}")
        subprocess.check_call(
            [sys.executable, "-m", "pip", "install", "--quiet"] + missing
        )


# ═══════════════════════════════════════════════════════════════════════════════
#  ENTRY POINT
# ═══════════════════════════════════════════════════════════════════════════════

if __name__ == "__main__":
    _check_deps()
    app = App()
    app.mainloop()
