#!/usr/bin/env python3
"""
╔══════════════════════════════════════════════════════════════════════════════╗
║  DataForge Pro  v2.0                                                        ║
║  Intelligent Data Cleaning & Cross-File Anchor Matching Platform            ║
╚══════════════════════════════════════════════════════════════════════════════╝
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
from rapidfuzz import fuzz

warnings.filterwarnings("ignore")
ctk.set_appearance_mode("light")
ctk.set_default_color_theme("blue")

# ═══════════════════════════════════════════════════════════════════════════════
#  DESIGN TOKENS
# ═══════════════════════════════════════════════════════════════════════════════
C: Dict[str,str] = {
    "bg":"#F4F6FA","card":"#FFFFFF","stripe":"#F9FAFB",
    "navy":"#0C1824","navy2":"#172535","navy_border":"#1E3347",
    "blue":"#2563EB","blue_lt":"#EFF6FF","blue_dk":"#1D4ED8","blue_mid":"#3B82F6",
    "teal":"#0D9488","teal_lt":"#F0FDFA",
    "green":"#16A34A","green_lt":"#F0FDF4",
    "amber":"#B45309","amber_lt":"#FFFBEB",
    "red":"#DC2626","red_lt":"#FEF2F2",
    "purple":"#7C3AED","purple_lt":"#F5F3FF",
    "text":"#0C1824","text2":"#475569","text3":"#94A3B8","text_inv":"#FFFFFF",
    "border":"#E2E8F0","border2":"#CBD5E1",
}
F_DISPLAY=("Georgia",18,"bold"); F_HEADING=("Georgia",14,"bold")
F_SUBHEAD=("Georgia",12,"bold"); F_BODY=("Helvetica",10)
F_BODY_B=("Helvetica",10,"bold"); F_SMALL=("Helvetica",9)
F_SMALL_B=("Helvetica",9,"bold"); F_MONO=("Courier New",9)
F_BADGE=("Helvetica",8)

# ═══════════════════════════════════════════════════════════════════════════════
#  FILE LOADING
# ═══════════════════════════════════════════════════════════════════════════════

def smart_load(filepath:str)->Tuple[pd.DataFrame,str]:
    ext=Path(filepath).suffix.lower()
    if ext in(".xlsx",".xls",".xlsm",".xlsb"):
        df=pd.read_excel(filepath,dtype=str,keep_default_na=False)
        return df.fillna(""),f"Excel ({ext})"
    try:
        with open(filepath,"r",encoding="utf-8",errors="replace") as fh:
            head="".join(fh.readline() for _ in range(5))
    except: head=""
    pipes=head.count("|"); commas=head.count(","); tabs=head.count("\t")
    if pipes>=commas and pipes>=tabs: sep,label="|","pipe-delimited"
    elif tabs>commas: sep,label="\t","tab-delimited"
    else: sep,label=",","comma-delimited"
    try:
        df=pd.read_csv(filepath,sep=sep,dtype=str,encoding="utf-8",
                       errors="replace",keep_default_na=False,on_bad_lines="skip")
        return df.fillna(""),f"{ext.upper().lstrip('.')} ({label})"
    except: pass
    with open(filepath,"r",encoding="utf-8",errors="replace") as fh:
        lines=[ln.strip() for ln in fh if ln.strip()]
    return pd.DataFrame({"raw_content":lines}),"raw text"

def parse_pipe_source(filepath:str)->pd.DataFrame:
    ext=Path(filepath).suffix.lower()
    if ext in(".xlsx",".xls",".xlsm",".xlsb"):
        return _parse_pipe_excel(filepath)
    with open(filepath,"r",encoding="utf-8",errors="replace") as fh:
        raw=[ln.rstrip("\r\n") for ln in fh if ln.strip()]
    if not raw: return pd.DataFrame()
    first=raw[0]
    if "|" not in first:
        try:
            tmp=pd.read_csv(filepath,dtype=str,keep_default_na=False,
                            encoding="utf-8",errors="replace")
            fc=tmp.columns[0]
            if "|" in fc:
                headers=[h.strip() for h in fc.split("|") if h.strip()]
                rows=[]
                for cell in tmp.iloc[:,0]:
                    parts=str(cell).split("|")
                    parts=(parts+[""]*len(headers))[:len(headers)]
                    rows.append([p.strip() for p in parts])
                return pd.DataFrame(rows,columns=headers)
        except: pass
        return pd.DataFrame({"content":raw})
    return _pipe_lines_to_df(raw)

def _parse_pipe_excel(filepath:str)->pd.DataFrame:
    raw_df=pd.read_excel(filepath,header=None,dtype=str,keep_default_na=False)
    pipe_lines:List[str]=[]
    for _,row in raw_df.iterrows():
        cells=[str(v) for v in row if str(v).strip() not in("","nan","None")]
        if not cells: continue
        pipe_lines.append("".join(cells).strip())
    if not pipe_lines: return pd.DataFrame()
    return _pipe_lines_to_df(pipe_lines)

def _pipe_lines_to_df(lines:List[str])->pd.DataFrame:
    if not lines: return pd.DataFrame()
    headers=[h.strip() for h in lines[0].split("|")]
    while headers and not headers[-1]: headers.pop()
    seen:Dict[str,int]={}; clean_headers:List[str]=[]
    for h in headers:
        label=h or f"col_{len(clean_headers)}"
        if label in seen:
            seen[label]+=1; clean_headers.append(f"{label}_{seen[label]}")
        else:
            seen[label]=0; clean_headers.append(label)
    rows:List[List[str]]=[]; n=len(clean_headers)
    for line in lines[1:]:
        parts=line.split("|")
        parts=(parts+[""]*n)[:n]
        rows.append([p.strip() for p in parts])
    df=pd.DataFrame(rows,columns=clean_headers)
    return df.loc[:,(df!="").any(axis=0)]

def detect_key_columns(df:pd.DataFrame)->List[str]:
    pat=re.compile(
        r"(mal.?code|malcode|mal_code|table.?name|zone.?name|column.?name|"
        r"steward|model|edm|edc|schema|database|owner|object|asset|domain|"
        r"class|community|name)",re.IGNORECASE)
    return [c for c in df.columns if pat.search(str(c))]

# ═══════════════════════════════════════════════════════════════════════════════
#  TEXT UTILITIES
# ═══════════════════════════════════════════════════════════════════════════════

def normalise(v)->str:
    if v is None: return ""
    if isinstance(v,float) and np.isnan(v): return ""
    s=str(v).lower().strip()
    s=re.sub(r"[\-_\s]+"," ",s)
    s=re.sub(r"[^\w\s/]"," ",s)
    return re.sub(r"\s+"," ",s).strip()

def tokenise(v:str)->List[str]:
    if not v: return []
    result:set=set()
    full=normalise(v)
    if full: result.add(full)
    for part in v.split("/"):
        n=normalise(part)
        if n and len(n)>1: result.add(n)
    for part in re.split(r"[/\\_\-\s{}()\[\],;]",v):
        n=normalise(part)
        if n and len(n)>1: result.add(n)
    return list(result)

def _classify(a:str,b:str)->str:
    na,nb=normalise(a),normalise(b)
    if na==nb: return "Exact"
    if na in nb or nb in na: return "Substring"
    if set(na.split())&set(nb.split()): return "Token Overlap"
    return "Fuzzy"

def _make_detail(src_ac,src_av,ref_ac,ref_av,scol,sval,rcol,rval,mtype)->str:
    parts=[]
    if src_ac and src_av:
        ra=ref_av if ref_av and ref_av!="__all__" else "?"
        parts.append(f"{src_ac} '{src_av}' \u2194 {ref_ac or 'Ref Anchor'} '{ra}'")
    parts.append(f"{scol} '{sval}' \u2192 {rcol} '{rval}'  [{mtype}]")
    return "  |  ".join(parts)

# ═══════════════════════════════════════════════════════════════════════════════
#  MATCH ENGINE  v2  (anchor-first, stoppable)
# ═══════════════════════════════════════════════════════════════════════════════

class MatchEngine:
    COLS=["Src Row","Src Anchor","Src Column","Src Value",
          "Ref Row","Ref Anchor","Ref Column","Ref Value",
          "Match Detail","Score %","Match Type"]

    def __init__(self,threshold:float=0.60):
        self.threshold=threshold
        self._tfidf:Optional[TfidfVectorizer]=None
        self._full_matrix=None
        self._anchor_idx:Dict[str,List[int]]={}
        self._tokens:List[str]=[]
        self._token_meta:List[Tuple]=[]  # (ridx,col,raw,norm_anchor)

    # ── Build index ───────────────────────────────────────────────────────────
    def fit(self,ref_df:pd.DataFrame,ref_anchor_col:Optional[str],
            ref_search_cols:List[str],status_cb=None):
        self._tokens=[]; self._token_meta=[]; self._anchor_idx={}
        self._tfidf=None; self._full_matrix=None
        if ref_anchor_col and ref_anchor_col not in ref_df.columns:
            ref_anchor_col=None
        search_cols=[c for c in ref_search_cols if c in ref_df.columns] or list(ref_df.columns)
        for ridx,row in ref_df.iterrows():
            norm_anchor=normalise(str(row[ref_anchor_col])) if ref_anchor_col else "__all__"
            for col in search_cols:
                raw=str(row[col]).strip()
                if not raw or raw in("nan","None"): continue
                for tok in tokenise(raw):
                    pos=len(self._tokens)
                    self._tokens.append(tok)
                    self._token_meta.append((ridx,col,raw,norm_anchor))
                    self._anchor_idx.setdefault(norm_anchor,[]).append(pos)
        if not self._tokens: return
        if status_cb: status_cb(f"Vectorising {len(self._tokens):,} reference tokens\u2026")
        self._tfidf=TfidfVectorizer(analyzer="char_wb",ngram_range=(2,4),
                                    max_features=100_000,sublinear_tf=True,min_df=1)
        self._full_matrix=self._tfidf.fit_transform(self._tokens)

    # ── Query within a position subset ────────────────────────────────────────
    def _query_subset(self,value:str,positions:List[int],top_k:int=3)->List[Tuple]:
        if not self._tfidf or not positions: return []
        q_tokens=tokenise(value)
        if not q_tokens: return []
        sub=self._full_matrix[positions]
        best:Dict[Tuple,Tuple]={}
        for token in q_tokens:
            if len(token)<2: continue
            try:
                qv=self._tfidf.transform([token])
                sims=cosine_similarity(qv,sub).flatten()
                top_n=min(top_k*4,len(positions))
                for ix in sims.argsort()[-top_n:][::-1]:
                    tsim=float(sims[ix])
                    if tsim<0.05: break
                    pos=positions[ix]
                    meta=self._token_meta[pos]
                    comp=self._composite(token,self._tokens[pos])
                    final=tsim*0.30+comp*0.70
                    key=(meta[0],meta[1])
                    if final>best.get(key,(-1,"",""))[0]:
                        best[key]=(final,meta[2],meta[3])
            except: pass
        results=[(s,r,c,raw,na) for (r,c),(s,raw,na) in best.items() if s>=self.threshold]
        results.sort(key=lambda x:-x[0])
        return results[:top_k]

    def _composite(self,a:str,b:str)->float:
        if not a or not b: return 0.0
        if a==b: return 1.0
        sub=min(len(a),len(b))/max(len(a),len(b),1) if a in b or b in a else 0.0
        fz=fuzz.token_sort_ratio(a,b)/100.0
        ta,tb=set(a.split()),set(b.split())
        jac=len(ta&tb)/len(ta|tb) if (ta|tb) else 0.0
        return max(sub,fz*0.60+jac*0.40)

    # ── Full sweep ────────────────────────────────────────────────────────────
    def run(self,src_df:pd.DataFrame,src_anchor_col:Optional[str],
            ref_anchor_col:Optional[str],src_search_cols:List[str],
            stop_event:Optional[threading.Event]=None,
            progress_cb=None,status_cb=None)->pd.DataFrame:

        src_anchor_col=(src_anchor_col if src_anchor_col in src_df.columns else None)
        src_search_cols=[c for c in src_search_cols if c in src_df.columns]
        if not src_search_cols:
            src_search_cols=[c for c in src_df.columns if c!=src_anchor_col]

        total=len(src_df)*len(src_search_cols); done=0; output:List[Dict]=[]

        for ridx,row in src_df.iterrows():
            if stop_event and stop_event.is_set(): break
            src_av=str(row[src_anchor_col]).strip() if src_anchor_col else ""
            norm_anch=normalise(src_av) if src_av else "__all__"

            # Anchor lookup
            if norm_anch and norm_anch!="__all__":
                positions=self._anchor_idx.get(norm_anch,[])
                if not positions:
                    positions=[]
                    for ak,pl in self._anchor_idx.items():
                        if (norm_anch in ak or ak in norm_anch) and ak!="__all__":
                            positions.extend(pl)
            else:
                positions=list(range(len(self._tokens)))

            if not positions:
                done+=len(src_search_cols)
                if progress_cb and total>0: progress_cb(done/total)
                continue

            for scol in src_search_cols:
                if stop_event and stop_event.is_set(): break
                sv=str(row[scol]).strip()
                if not sv or len(sv)<2 or sv in("nan","None"):
                    done+=1; continue
                for score,ref_ridx,ref_col,ref_raw,ref_na in self._query_subset(sv,positions,top_k=3):
                    mtype=_classify(sv,ref_raw)
                    detail=_make_detail(src_anchor_col,src_av,ref_anchor_col,
                                        ref_na,scol,sv,ref_col,ref_raw,mtype)
                    output.append({"Src Row":int(ridx)+1,"Src Anchor":src_av,
                                   "Src Column":scol,"Src Value":sv,
                                   "Ref Row":int(ref_ridx)+1,
                                   "Ref Anchor":ref_na if ref_na!="__all__" else "",
                                   "Ref Column":ref_col,"Ref Value":ref_raw,
                                   "Match Detail":detail,
                                   "Score %":round(score*100,1),"Match Type":mtype})
                done+=1
                if progress_cb and total>0: progress_cb(done/total)
                if status_cb and done%100==0:
                    status_cb(f"Matching\u2026 {done:,}/{total:,}  |  {len(output):,} matches so far")

        if not output: return pd.DataFrame(columns=self.COLS)
        df=pd.DataFrame(output)
        df.sort_values("Score %",ascending=False,inplace=True)
        df.drop_duplicates(subset=["Src Value","Ref Value","Src Column"],keep="first",inplace=True)
        df.reset_index(drop=True,inplace=True)
        return df

# ═══════════════════════════════════════════════════════════════════════════════
#  SHARED WIDGET HELPERS
# ═══════════════════════════════════════════════════════════════════════════════

def _ttk_style():
    s=ttk.Style()
    try: s.theme_use("clam")
    except: pass
    s.configure("DF.Treeview",background=C["card"],foreground=C["text"],
                fieldbackground=C["card"],rowheight=26,font=F_SMALL,
                bordercolor=C["border"],relief="flat")
    s.configure("DF.Treeview.Heading",background=C["navy"],foreground=C["text_inv"],
                font=F_SMALL_B,relief="flat",padding=(6,5))
    s.map("DF.Treeview",background=[("selected",C["blue_lt"])],
          foreground=[("selected",C["blue_dk"])])

def make_scrolled_tree(parent,columns:List[str],
                       col_widths:Optional[List[int]]=None)->ttk.Treeview:
    _ttk_style()
    vsb=ttk.Scrollbar(parent,orient="vertical")
    hsb=ttk.Scrollbar(parent,orient="horizontal")
    tree=ttk.Treeview(parent,columns=columns,show="headings",style="DF.Treeview",
                      yscrollcommand=vsb.set,xscrollcommand=hsb.set,selectmode="browse")
    vsb.configure(command=tree.yview); hsb.configure(command=tree.xview)
    dw=max(80,1100//max(len(columns),1))
    for i,col in enumerate(columns):
        w=col_widths[i] if col_widths and i<len(col_widths) else dw
        tree.heading(col,text=col,anchor="w")
        tree.column(col,width=w,minwidth=50,anchor="w",stretch=True)
    vsb.pack(side="right",fill="y"); hsb.pack(side="bottom",fill="x")
    tree.pack(fill="both",expand=True)
    tree.tag_configure("odd",background=C["stripe"])
    tree.tag_configure("even",background=C["card"])
    tree.tag_configure("Exact",background="#F0FDF4",foreground="#166534")
    tree.tag_configure("Substring",background=C["blue_lt"],foreground=C["blue_dk"])
    tree.tag_configure("Token Overlap",background=C["amber_lt"],foreground=C["amber"])
    tree.tag_configure("Fuzzy",background=C["red_lt"],foreground=C["red"])
    tree.tag_configure("key_col",background=C["teal_lt"],foreground=C["teal"])
    return tree

def load_df_into_tree(tree:ttk.Treeview,df:pd.DataFrame,max_rows:int=500):
    tree.delete(*tree.get_children())
    cols=list(df.columns)
    tree["columns"]=cols
    cw=max(70,min(200,1050//max(len(cols),1)))
    for c in cols:
        tree.heading(c,text=c,anchor="w")
        tree.column(c,width=cw,minwidth=50,anchor="w")
    for i,(_,row) in enumerate(df.head(max_rows).iterrows()):
        vals=[str(row[c])[:150] for c in cols]
        tree.insert("","end",values=vals,tags=("odd" if i%2 else "even",))
    if len(df)>max_rows:
        msg=[f"  \u22ef  {len(df)-max_rows:,} more rows not shown"]+[""]*(len(cols)-1)
        tree.insert("","end",values=msg)

def _scrolled_checklist(parent,items:List[str],checked:bool=True,
                        height:int=160,bg:str=C["card"],
                        accent:str=C["blue_lt"])->Tuple[tk.Frame,Dict[str,tk.BooleanVar]]:
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
        tk.Checkbutton(inner,text=item[:52],variable=v,bg=bg,fg=C["text"],
                       font=F_SMALL,activebackground=bg,selectcolor=accent,
                       anchor="w").pack(fill="x",padx=6,pady=1)
    return outer,vars_

# ═══════════════════════════════════════════════════════════════════════════════
#  APPLICATION
# ═══════════════════════════════════════════════════════════════════════════════

class App(ctk.CTk):

    def __init__(self):
        super().__init__()
        self.title("DataForge Pro"); self.geometry("1420x880")
        self.minsize(1100,720); self.configure(fg_color=C["bg"])

        # State
        self.src_path=self.ref_path=None
        self.src_df=self.ref_df=self.results_df=None
        self._stop_event=threading.Event(); self._step=0
        self._src_anchor_var=tk.StringVar(value="")
        self._ref_anchor_var=tk.StringVar(value="")
        self._src_search_vars:Dict[str,tk.BooleanVar]={}
        self._ref_search_vars:Dict[str,tk.BooleanVar]={}

        self._build_header(); self._build_step_bar()
        self._content_host=tk.Frame(self,bg=C["bg"])
        self._content_host.pack(fill="both",expand=True)
        self._build_status_bar()
        self._pages:Dict[int,tk.Frame]={}
        self._build_page_upload(); self._build_page_preview()
        self._build_page_match_cfg(); self._build_page_results()
        self._goto(0)

    # ── Chrome ────────────────────────────────────────────────────────────────
    def _build_header(self):
        bar=tk.Frame(self,bg=C["navy"],height=58); bar.pack(fill="x"); bar.pack_propagate(False)
        tk.Frame(bar,bg=C["blue"],width=4).pack(side="left",fill="y")
        inner=tk.Frame(bar,bg=C["navy"]); inner.pack(side="left",fill="both",expand=True,padx=18)
        tk.Label(inner,text="DataForge Pro",bg=C["navy"],fg="#FFFFFF",font=F_DISPLAY).pack(side="left",pady=10)
        tk.Label(inner,text="  \u00b7  Intelligent Data Cleaning & Anchor-Based Cross-File Matching",
                 bg=C["navy"],fg="#7CA8CC",font=F_BODY).pack(side="left",pady=10)
        right=tk.Frame(bar,bg=C["navy"]); right.pack(side="right",padx=18)
        self._spinner_lbl=tk.Label(right,text="",bg=C["navy"],fg=C["blue_mid"],font=F_SMALL_B)
        self._spinner_lbl.pack(side="right",padx=(8,0))
        tk.Label(right,text="v2.0",bg=C["navy2"],fg="#7CA8CC",
                 font=F_BADGE,padx=8,pady=3).pack(side="right")

    def _build_step_bar(self):
        bar=tk.Frame(self,bg=C["card"],highlightthickness=1,highlightbackground=C["border"])
        bar.pack(fill="x")
        self._step_circles:List[tk.Label]=[]; self._step_texts:List[tk.Label]=[]
        STEPS=[("1","Upload Files"),("2","Clean & Preview"),
               ("3","Configure Match"),("4","Results")]
        inner=tk.Frame(bar,bg=C["card"]); inner.pack(pady=10)
        for i,(num,label) in enumerate(STEPS):
            if i: tk.Frame(inner,bg=C["border2"],width=44,height=2).pack(side="left",pady=6)
            cell=tk.Frame(inner,bg=C["card"]); cell.pack(side="left",padx=6)
            c=tk.Label(cell,text=num,bg=C["text3"],fg="#FFF",font=F_SMALL_B,width=3,pady=2); c.pack(side="left")
            lbl=tk.Label(cell,text=f"  {label}",bg=C["card"],fg=C["text3"],font=F_SMALL); lbl.pack(side="left")
            self._step_circles.append(c); self._step_texts.append(lbl)

    def _update_step_bar(self,active:int):
        for i,(circ,lbl) in enumerate(zip(self._step_circles,self._step_texts)):
            if i<active: circ.configure(bg=C["teal"]); lbl.configure(fg=C["teal"],font=F_SMALL)
            elif i==active: circ.configure(bg=C["blue"]); lbl.configure(fg=C["blue"],font=F_SMALL_B)
            else: circ.configure(bg=C["text3"]); lbl.configure(fg=C["text3"],font=F_SMALL)

    def _build_status_bar(self):
        bar=tk.Frame(self,bg=C["card"],height=28,
                     highlightthickness=1,highlightbackground=C["border"])
        bar.pack(fill="x",side="bottom"); bar.pack_propagate(False)
        self._status_var=tk.StringVar(value="Ready \u2014 upload your files to begin.")
        tk.Label(bar,textvariable=self._status_var,bg=C["card"],fg=C["text2"],
                 font=F_SMALL,anchor="w",padx=12).pack(fill="y",side="left")

    def _goto(self,step:int):
        for p in self._pages.values(): p.pack_forget()
        self._pages[step].pack(fill="both",expand=True)
        self._step=step; self._update_step_bar(step)

    def _status(self,msg:str): self._status_var.set(msg)
    def _set_busy(self,msg:str=""): self._spinner_lbl.configure(text=f"\u27f3  {msg}" if msg else "")

    # ══════════════════════════════════════════════════════════════════════════
    #  PAGE 0 — UPLOAD
    # ══════════════════════════════════════════════════════════════════════════
    def _build_page_upload(self):
        pg=tk.Frame(self._content_host,bg=C["bg"]); self._pages[0]=pg
        tk.Label(pg,text="Upload Your Files",bg=C["bg"],fg=C["text"],font=F_HEADING).pack(pady=(22,3))
        tk.Label(pg,text="Select the messy pipe-delimited source and the reference file to match against.",
                 bg=C["bg"],fg=C["text2"],font=F_BODY).pack()
        row=tk.Frame(pg,bg=C["bg"]); row.pack(pady=20,padx=40,fill="x")
        row.columnconfigure(0,weight=1); row.columnconfigure(1,weight=1)

        sc=tk.Frame(row,bg=C["card"],highlightthickness=1,highlightbackground=C["border"],padx=28,pady=22)
        sc.grid(row=0,column=0,padx=(0,12),sticky="nsew")
        tk.Label(sc,text="\U0001f4c4",bg=C["card"],font=("Helvetica",36)).pack()
        tk.Label(sc,text="Source File",bg=C["card"],fg=C["text"],font=F_SUBHEAD).pack(pady=(6,2))
        tk.Label(sc,text="Pipe-delimited \u2014 headers in row 1, values separated by  |",
                 bg=C["card"],fg=C["text2"],font=F_SMALL,justify="center").pack()
        self._use_pipe_var=tk.BooleanVar(value=True)
        tk.Checkbutton(sc,text="First row contains pipe-separated column headers",
                       variable=self._use_pipe_var,bg=C["card"],fg=C["text2"],font=F_SMALL,
                       activebackground=C["card"],selectcolor=C["blue_lt"]).pack(pady=(10,0))
        self._src_browse_btn=tk.Button(sc,text="Browse Source File\u2026",
                                       bg=C["blue"],fg=C["text_inv"],font=F_BODY_B,
                                       relief="flat",padx=18,pady=9,cursor="hand2",command=self._browse_src)
        self._src_browse_btn.pack(pady=(12,4))
        self._src_info_var=tk.StringVar(value="No file selected")
        self._src_info_lbl=tk.Label(sc,textvariable=self._src_info_var,
                                    bg=C["card"],fg=C["text2"],font=F_SMALL,wraplength=300)
        self._src_info_lbl.pack()

        rc=tk.Frame(row,bg=C["card"],highlightthickness=1,highlightbackground=C["border"],padx=28,pady=22)
        rc.grid(row=0,column=1,padx=(12,0),sticky="nsew")
        tk.Label(rc,text="\U0001f4ca",bg=C["card"],font=("Helvetica",36)).pack()
        tk.Label(rc,text="Reference File",bg=C["card"],fg=C["text"],font=F_SUBHEAD).pack(pady=(6,2))
        tk.Label(rc,text="Clean comparison file \u2014 Excel, CSV, TXT, TSV all accepted",
                 bg=C["card"],fg=C["text2"],font=F_SMALL,justify="center").pack()
        tk.Label(rc,text="",bg=C["card"],height=2).pack()
        self._ref_browse_btn=tk.Button(rc,text="Browse Reference File\u2026",
                                       bg=C["teal"],fg=C["text_inv"],font=F_BODY_B,
                                       relief="flat",padx=18,pady=9,cursor="hand2",command=self._browse_ref)
        self._ref_browse_btn.pack(pady=(12,4))
        self._ref_info_var=tk.StringVar(value="No file selected")
        tk.Label(rc,textvariable=self._ref_info_var,bg=C["card"],fg=C["text2"],
                 font=F_SMALL,wraplength=300).pack()

        self._proceed_btn=tk.Button(pg,text="Parse & Clean  \u2192",
                                    bg=C["text3"],fg=C["text_inv"],font=F_BODY_B,
                                    relief="flat",padx=28,pady=11,cursor="hand2",
                                    state="disabled",command=self._do_parse)
        self._proceed_btn.pack(pady=8)
        tk.Label(pg,text="Supported: CSV \u00b7 TXT \u00b7 TSV \u00b7 Excel (.xlsx/.xls/.xlsm)\nAuto-detects delimiters and file types.",
                 bg=C["bg"],fg=C["text3"],font=F_SMALL,justify="center").pack(pady=4)

    def _browse_src(self):
        p=filedialog.askopenfilename(
            title="Select Source (Messy) File",
            filetypes=[("All supported","*.csv *.txt *.tsv *.xlsx *.xls *.xlsm"),
                       ("CSV","*.csv"),("Text/TSV","*.txt *.tsv"),
                       ("Excel","*.xlsx *.xls *.xlsm"),("All files","*.*")])
        if p:
            self.src_path=p
            self._src_info_var.set(f"\u2713  {Path(p).name}  ({os.path.getsize(p)/1024:.1f} KB)")
            self._src_info_lbl.configure(fg=C["green"]); self._check_ready()

    def _browse_ref(self):
        p=filedialog.askopenfilename(
            title="Select Reference File",
            filetypes=[("All supported","*.csv *.txt *.tsv *.xlsx *.xls *.xlsm"),
                       ("CSV","*.csv"),("Text/TSV","*.txt *.tsv"),
                       ("Excel","*.xlsx *.xls *.xlsm"),("All files","*.*")])
        if p:
            self.ref_path=p
            self._ref_info_var.set(f"\u2713  {Path(p).name}  ({os.path.getsize(p)/1024:.1f} KB)")
            self._check_ready()

    def _check_ready(self):
        if self.src_path and self.ref_path:
            self._proceed_btn.configure(state="normal",bg=C["blue"])

    def _do_parse(self):
        self._proceed_btn.configure(state="disabled",text="Parsing\u2026")
        self._set_busy("Parsing files\u2026")
        threading.Thread(target=self._parse_worker,daemon=True).start()

    def _parse_worker(self):
        try:
            src=(parse_pipe_source(self.src_path) if self._use_pipe_var.get()
                 else smart_load(self.src_path)[0])
            src=src.apply(lambda col:col.map(lambda x:str(x).strip()))
            src=src.loc[:,(src!="").any(axis=0)]; src=src.loc[:,~src.columns.duplicated()]
            ref,ref_method=smart_load(self.ref_path)
            ref=ref.apply(lambda col:col.map(lambda x:str(x).strip()))
            ref=ref.loc[:,(ref!="").any(axis=0)]; ref=ref.loc[:,~ref.columns.duplicated()]
            self.src_df=src; self.ref_df=ref
            self.after(0,lambda:self._parse_done(ref_method))
        except Exception as exc:
            import traceback; traceback.print_exc()
            self.after(0,lambda e=exc:self._on_error("Parse Error",e))

    def _parse_done(self,ref_method:str):
        self._proceed_btn.configure(state="normal",text="Parse & Clean  \u2192",bg=C["blue"])
        self._set_busy()
        self._status(f"Source: {len(self.src_df):,} rows \u00d7 {len(self.src_df.columns)} cols  \u00b7  "
                     f"Reference ({ref_method}): {len(self.ref_df):,} rows \u00d7 {len(self.ref_df.columns)} cols")
        self._populate_preview(); self._goto(1)

    # ══════════════════════════════════════════════════════════════════════════
    #  PAGE 1 — CLEAN & PREVIEW
    # ══════════════════════════════════════════════════════════════════════════
    def _build_page_preview(self):
        pg=tk.Frame(self._content_host,bg=C["bg"]); self._pages[1]=pg
        top=tk.Frame(pg,bg=C["bg"]); top.pack(fill="x",padx=20,pady=(14,4))
        tk.Label(top,text="Clean & Preview",bg=C["bg"],fg=C["text"],font=F_HEADING).pack(side="left")
        right=tk.Frame(top,bg=C["bg"]); right.pack(side="right")
        _back_btn(right,"← Upload",lambda:self._goto(0))
        _nav_btn(right,"Configure Matching  \u2192",self._go_match_cfg,bg=C["blue"])
        self._prev_stats_var=tk.StringVar(value="")
        tk.Label(pg,textvariable=self._prev_stats_var,bg=C["bg"],fg=C["text2"],font=F_SMALL).pack(padx=20,anchor="w")
        tab_bar=tk.Frame(pg,bg=C["card"],highlightthickness=1,highlightbackground=C["border"])
        tab_bar.pack(fill="x",padx=20,pady=(4,0))
        self._prev_tab=tk.StringVar(value="source")
        for txt,val in [("\u2713 Source File (Cleaned)","source"),("Reference File","ref")]:
            tk.Radiobutton(tab_bar,text=txt,variable=self._prev_tab,value=val,
                           bg=C["card"],fg=C["text"],selectcolor=C["blue_lt"],
                           font=F_BODY,activebackground=C["card"],
                           command=self._switch_prev_tab).pack(side="left",padx=14,pady=7)
        tree_host=tk.Frame(pg,bg=C["bg"]); tree_host.pack(fill="both",expand=True,padx=20,pady=(4,6))
        self._prev_tree=make_scrolled_tree(tree_host,["Loading\u2026"])

    def _populate_preview(self): self._switch_prev_tab()
    def _switch_prev_tab(self,*_):
        tab=self._prev_tab.get(); df=self.src_df if tab=="source" else self.ref_df
        if df is None: return
        self._prev_stats_var.set(
            f"{len(df):,} rows  \u00b7  {len(df.columns)} columns  \u00b7  "
            f"{int(df.apply(lambda c:(c!='').sum()).sum()):,} non-empty cells")
        load_df_into_tree(self._prev_tree,df,max_rows=300)
    def _go_match_cfg(self): self._populate_match_cfg(); self._goto(2)

    # ══════════════════════════════════════════════════════════════════════════
    #  PAGE 2 — CONFIGURE MATCHING  v2
    # ══════════════════════════════════════════════════════════════════════════
    def _build_page_match_cfg(self):
        pg=tk.Frame(self._content_host,bg=C["bg"]); self._pages[2]=pg

        # Top bar
        top=tk.Frame(pg,bg=C["bg"]); top.pack(fill="x",padx=20,pady=(14,8))
        tk.Label(top,text="Configure Matching",bg=C["bg"],fg=C["text"],font=F_HEADING).pack(side="left")
        rbar=tk.Frame(top,bg=C["bg"]); rbar.pack(side="right")
        _back_btn(rbar,"← Preview",lambda:self._goto(1))
        self._stop_btn=tk.Button(rbar,text="\u23f9  Stop",
                                 bg=C["red"],fg=C["text_inv"],font=F_BODY_B,
                                 relief="flat",padx=14,pady=7,cursor="hand2",
                                 command=self._do_stop)
        self._run_btn=tk.Button(rbar,text="\u25b6  Run Matching Engine",
                                bg=C["teal"],fg=C["text_inv"],font=F_BODY_B,
                                relief="flat",padx=18,pady=7,cursor="hand2",
                                command=self._do_match)
        self._run_btn.pack(side="left",padx=6)

        # Body
        body=tk.Frame(pg,bg=C["bg"]); body.pack(fill="both",expand=True,padx=20,pady=(0,8))
        body.columnconfigure(0,weight=0,minsize=270); body.columnconfigure(1,weight=1)

        # LEFT — settings + progress
        left=tk.Frame(body,bg=C["card"],highlightthickness=1,highlightbackground=C["border"],
                      padx=18,pady=16)
        left.grid(row=0,column=0,sticky="nsew",padx=(0,10))
        left.pack_propagate(False)

        _section_label(left,"Match Settings")
        tk.Frame(left,bg=C["border"],height=1).pack(fill="x",pady=(0,10))

        tk.Label(left,text="Similarity Threshold",bg=C["card"],fg=C["text"],font=F_BODY_B).pack(anchor="w")
        tk.Label(left,text="Higher = stricter  \u00b7  Lower = more results",
                 bg=C["card"],fg=C["text3"],font=F_SMALL).pack(anchor="w",pady=(0,4))
        tr=tk.Frame(left,bg=C["card"]); tr.pack(fill="x",pady=(0,10))
        self._thresh_lbl=tk.Label(tr,text="60%",bg=C["card"],fg=C["blue"],
                                   font=("Georgia",14,"bold"),width=5)
        self._thresh_lbl.pack(side="right")
        self._thresh_var=tk.DoubleVar(value=0.60)
        tk.Scale(tr,variable=self._thresh_var,from_=0.15,to=0.95,resolution=0.05,
                 orient="horizontal",bg=C["card"],fg=C["text2"],troughcolor=C["border2"],
                 activebackground=C["blue"],highlightthickness=0,relief="flat",bd=0,showvalue=False,
                 command=lambda v:self._thresh_lbl.configure(text=f"{int(float(v)*100)}%")
                 ).pack(side="left",fill="x",expand=True)

        tk.Frame(left,bg=C["border"],height=1).pack(fill="x",pady=8)
        tk.Label(left,text="How anchors work",bg=C["card"],fg=C["text"],font=F_BODY_B).pack(anchor="w")
        tk.Label(left,
                 text=("Pick one anchor col from EACH file.\n"
                       "The engine groups ref rows by anchor\n"
                       "value so only rows sharing the same\n"
                       "anchor are compared \u2014 makes 15M\n"
                       "record matching practical.\n\n"
                       "Example:\n"
                       "  Source anchor \u2192 MALCODE  ('ACCA')\n"
                       "  Ref anchor    \u2192 Schema   ('ACCA')\n\n"
                       "Then search columns are compared\n"
                       "only within that ACCA subset."),
                 bg=C["card"],fg=C["text2"],font=F_SMALL,justify="left",wraplength=225
                 ).pack(anchor="w",pady=(0,8))

        tk.Frame(left,bg=C["border"],height=1).pack(fill="x",pady=8)
        self._prog_var=tk.StringVar(value="")
        tk.Label(left,textvariable=self._prog_var,bg=C["card"],fg=C["text2"],font=F_SMALL).pack(anchor="w")
        self._prog_bar=ctk.CTkProgressBar(left,width=230,height=7)
        self._prog_bar.set(0); self._prog_bar.pack(pady=4,anchor="w")

        # RIGHT — dual column selectors
        right_panel=tk.Frame(body,bg=C["card"],
                             highlightthickness=1,highlightbackground=C["border"],
                             padx=14,pady=14)
        right_panel.grid(row=0,column=1,sticky="nsew")
        right_panel.columnconfigure(0,weight=1); right_panel.columnconfigure(1,weight=1)

        # ── SOURCE side ────────────────────────────────────────────────────────
        src_panel=tk.Frame(right_panel,bg=C["card"])
        src_panel.grid(row=0,column=0,sticky="nsew",padx=(0,10))

        hdr_s=tk.Frame(src_panel,bg=C["teal"],padx=8,pady=5)
        hdr_s.pack(fill="x",pady=(0,8))
        tk.Label(hdr_s,text="SOURCE FILE",bg=C["teal"],fg=C["text_inv"],font=F_SMALL_B).pack(side="left")

        tk.Label(src_panel,text="\U0001f511  Anchor Column",bg=C["card"],fg=C["teal"],font=F_SMALL_B).pack(anchor="w")
        tk.Label(src_panel,
                 text="Narrows search scope. E.g., MALCODE",
                 bg=C["card"],fg=C["text3"],font=F_SMALL).pack(anchor="w",pady=(0,3))
        self._src_anchor_cb=ttk.Combobox(src_panel,textvariable=self._src_anchor_var,
                                          state="readonly",font=F_SMALL)
        self._src_anchor_cb.pack(fill="x",pady=(0,10))

        tk.Label(src_panel,text="Search Columns",bg=C["card"],fg=C["text"],font=F_SMALL_B).pack(anchor="w")
        tk.Label(src_panel,text="Values from these cols will be looked up in the reference.",
                 bg=C["card"],fg=C["text3"],font=F_SMALL).pack(anchor="w",pady=(0,4))
        self._src_checklist_host=tk.Frame(src_panel,bg=C["card"])
        self._src_checklist_host.pack(fill="both",expand=True)
        sb=tk.Frame(src_panel,bg=C["card"]); sb.pack(fill="x",pady=(4,0))
        tk.Button(sb,text="All",bg=C["teal_lt"],fg=C["teal"],font=F_BADGE,relief="flat",padx=6,pady=2,
                  command=lambda:self._toggle_all(self._src_search_vars,True)).pack(side="left",padx=(0,3))
        tk.Button(sb,text="None",bg=C["stripe"],fg=C["text2"],font=F_BADGE,relief="flat",padx=6,pady=2,
                  command=lambda:self._toggle_all(self._src_search_vars,False)).pack(side="left")

        # Vertical divider
        tk.Frame(right_panel,bg=C["border"],width=1).grid(row=0,column=0,sticky="nse",padx=(0,10))

        # ── REFERENCE side ─────────────────────────────────────────────────────
        ref_panel=tk.Frame(right_panel,bg=C["card"])
        ref_panel.grid(row=0,column=1,sticky="nsew",padx=(10,0))

        hdr_r=tk.Frame(ref_panel,bg=C["purple"],padx=8,pady=5)
        hdr_r.pack(fill="x",pady=(0,8))
        tk.Label(hdr_r,text="REFERENCE FILE",bg=C["purple"],fg=C["text_inv"],font=F_SMALL_B).pack(side="left")

        tk.Label(ref_panel,text="\U0001f511  Anchor Column",bg=C["card"],fg=C["purple"],font=F_SMALL_B).pack(anchor="w")
        tk.Label(ref_panel,
                 text="Matched against source anchor. E.g., Schema",
                 bg=C["card"],fg=C["text3"],font=F_SMALL).pack(anchor="w",pady=(0,3))
        self._ref_anchor_cb=ttk.Combobox(ref_panel,textvariable=self._ref_anchor_var,
                                          state="readonly",font=F_SMALL)
        self._ref_anchor_cb.pack(fill="x",pady=(0,10))

        tk.Label(ref_panel,text="Search Columns",bg=C["card"],fg=C["text"],font=F_SMALL_B).pack(anchor="w")
        tk.Label(ref_panel,text="These reference columns will be searched for matching values.",
                 bg=C["card"],fg=C["text3"],font=F_SMALL).pack(anchor="w",pady=(0,4))
        self._ref_checklist_host=tk.Frame(ref_panel,bg=C["card"])
        self._ref_checklist_host.pack(fill="both",expand=True)
        rb=tk.Frame(ref_panel,bg=C["card"]); rb.pack(fill="x",pady=(4,0))
        tk.Button(rb,text="All",bg=C["purple_lt"],fg=C["purple"],font=F_BADGE,relief="flat",padx=6,pady=2,
                  command=lambda:self._toggle_all(self._ref_search_vars,True)).pack(side="left",padx=(0,3))
        tk.Button(rb,text="None",bg=C["stripe"],fg=C["text2"],font=F_BADGE,relief="flat",padx=6,pady=2,
                  command=lambda:self._toggle_all(self._ref_search_vars,False)).pack(side="left")

    def _toggle_all(self,var_dict:Dict[str,tk.BooleanVar],state:bool):
        for v in var_dict.values(): v.set(state)

    def _populate_match_cfg(self):
        if self.src_df is None or self.ref_df is None: return
        src_cols=list(self.src_df.columns); ref_cols=list(self.ref_df.columns)

        # Source anchor default: first MALCODE-like col
        self._src_anchor_cb["values"]=["(none \u2014 search all)"]+src_cols
        src_key=detect_key_columns(self.src_df)
        default_src=next((c for c in src_key if re.search(r"mal.?code|malcode",c,re.I)),None) or \
                    (src_key[0] if src_key else src_cols[0])
        self._src_anchor_var.set(default_src)

        # Ref anchor default: first Schema/Community/Asset col
        self._ref_anchor_cb["values"]=["(none \u2014 search all)"]+ref_cols
        ref_key=detect_key_columns(self.ref_df)
        default_ref=next((c for c in ref_key if re.search(r"schema|community|asset",c,re.I)),None) or \
                    (ref_key[0] if ref_key else ref_cols[0])
        self._ref_anchor_var.set(default_ref)

        # Source checklist
        for w in self._src_checklist_host.winfo_children(): w.destroy()
        self._src_search_vars.clear()
        frame,self._src_search_vars=_scrolled_checklist(
            self._src_checklist_host,src_cols,checked=True,height=230,
            bg=C["card"],accent=C["teal_lt"])
        if len(src_cols)>6:
            kset=set(src_key)
            for col,v in self._src_search_vars.items():
                if col not in kset: v.set(False)
        frame.pack(fill="both",expand=True)

        # Ref checklist
        for w in self._ref_checklist_host.winfo_children(): w.destroy()
        self._ref_search_vars.clear()
        frame2,self._ref_search_vars=_scrolled_checklist(
            self._ref_checklist_host,ref_cols,checked=True,height=230,
            bg=C["card"],accent=C["purple_lt"])
        if len(ref_cols)>6:
            kset2=set(ref_key)
            for col,v in self._ref_search_vars.items():
                if col not in kset2: v.set(False)
        frame2.pack(fill="both",expand=True)

    # ── Run / Stop ────────────────────────────────────────────────────────────
    def _do_match(self):
        src_anchor=self._src_anchor_var.get()
        if src_anchor.startswith("(none"): src_anchor=None
        ref_anchor=self._ref_anchor_var.get()
        if ref_anchor.startswith("(none"): ref_anchor=None
        src_search=[c for c,v in self._src_search_vars.items() if v.get()]
        ref_search=[c for c,v in self._ref_search_vars.items() if v.get()]
        if not src_search:
            messagebox.showwarning("Configuration","Select at least one Source search column."); return
        if not ref_search:
            messagebox.showwarning("Configuration","Select at least one Reference search column."); return
        self._stop_event.clear()
        self._run_btn.pack_forget()
        self._stop_btn.pack(side="left",padx=6)
        self._prog_bar.set(0); self._prog_var.set(""); self._set_busy("Matching\u2026")
        threading.Thread(
            target=self._match_worker,
            args=(self._thresh_var.get(),src_anchor,ref_anchor,src_search,ref_search),
            daemon=True).start()

    def _do_stop(self):
        self._stop_event.set()
        self._stop_btn.configure(state="disabled",text="Stopping\u2026")
        self._status("Stop requested \u2014 finishing current batch\u2026")

    def _match_worker(self,threshold,src_anchor,ref_anchor,src_search,ref_search):
        try:
            engine=MatchEngine(threshold=threshold)
            def status(m): self.after(0,lambda msg=m:self._status(msg))
            def prog(p):   self.after(0,lambda v=p:  self._prog_bar.set(v))
            def plbl(m):   self.after(0,lambda msg=m:self._prog_var.set(msg))
            status("Building reference index\u2026")
            engine.fit(self.ref_df,ref_anchor_col=ref_anchor,
                       ref_search_cols=ref_search,status_cb=status)
            results=engine.run(
                self.src_df,src_anchor_col=src_anchor,ref_anchor_col=ref_anchor,
                src_search_cols=src_search,stop_event=self._stop_event,
                progress_cb=prog,status_cb=lambda m:(plbl(m),status(m)))
            self.results_df=results
            self.after(0,lambda stopped=self._stop_event.is_set():self._match_done(stopped))
        except Exception as exc:
            import traceback; traceback.print_exc()
            self.after(0,lambda e=exc:self._on_error("Matching Error",e))

    def _match_done(self,stopped:bool=False):
        n=len(self.results_df) if self.results_df is not None else 0
        self._prog_bar.set(1.0)
        self._prog_var.set(f"{'Stopped \u2014 ' if stopped else 'Complete \u2014 '}{n:,} matches found")
        self._stop_btn.pack_forget()
        self._stop_btn.configure(state="normal",text="\u23f9  Stop")
        self._run_btn.pack(side="left",padx=6)
        self._set_busy()
        self._status(f"{'Stopped early' if stopped else 'Matching complete'} \u2014 {n:,} matches found")
        if n>0:
            self._populate_results(); self._goto(3)
        else:
            messagebox.showinfo("No Matches",
                "No matches found above the threshold.\n\nTry:\n"
                "\u2022 Lowering the threshold\n"
                "\u2022 Changing the anchor columns\n"
                "\u2022 Selecting more search columns")

    # ══════════════════════════════════════════════════════════════════════════
    #  PAGE 3 — RESULTS  v2
    # ══════════════════════════════════════════════════════════════════════════
    def _build_page_results(self):
        pg=tk.Frame(self._content_host,bg=C["bg"]); self._pages[3]=pg
        top=tk.Frame(pg,bg=C["bg"]); top.pack(fill="x",padx=20,pady=(14,4))
        tk.Label(top,text="Match Results",bg=C["bg"],fg=C["text"],font=F_HEADING).pack(side="left")
        rbar=tk.Frame(top,bg=C["bg"]); rbar.pack(side="right")
        _back_btn(rbar,"← Reconfigure",lambda:self._goto(2))
        _nav_btn(rbar,"\u2b07  Export Matches",self._export_results,bg=C["green"])
        _nav_btn(rbar,"\u2b07  Export Cleaned Source",self._export_cleaned,bg=C["teal"])

        self._summary_host=tk.Frame(pg,bg=C["bg"])
        self._summary_host.pack(fill="x",padx=20,pady=(4,8))

        # Filter bar
        fbar=tk.Frame(pg,bg=C["card"],highlightthickness=1,highlightbackground=C["border"])
        fbar.pack(fill="x",padx=20,pady=(0,4))
        tk.Label(fbar,text="Search:",bg=C["card"],fg=C["text2"],font=F_SMALL).pack(side="left",padx=10,pady=7)
        self._flt_var=tk.StringVar(); self._flt_var.trace_add("write",self._apply_filter)
        tk.Entry(fbar,textvariable=self._flt_var,bg=C["stripe"],fg=C["text"],
                 font=F_SMALL,relief="flat",width=30).pack(side="left",padx=4)
        tk.Label(fbar,text="Match Type:",bg=C["card"],fg=C["text2"],font=F_SMALL).pack(side="left",padx=(14,4))
        self._type_var=tk.StringVar(value="All")
        type_cb=ttk.Combobox(fbar,textvariable=self._type_var,
                             values=["All","Exact","Substring","Token Overlap","Fuzzy"],
                             state="readonly",width=14,font=F_SMALL)
        type_cb.pack(side="left",pady=7); type_cb.bind("<<ComboboxSelected>>",self._apply_filter)
        tk.Label(fbar,text="Min Score:",bg=C["card"],fg=C["text2"],font=F_SMALL).pack(side="left",padx=(14,4))
        self._min_score_var=tk.IntVar(value=60)
        tk.Spinbox(fbar,from_=0,to=100,textvariable=self._min_score_var,
                   width=5,font=F_SMALL,command=self._apply_filter).pack(side="left",pady=7)
        tk.Label(fbar,text="%",bg=C["card"],fg=C["text2"],font=F_SMALL).pack(side="left")
        self._flt_count_var=tk.StringVar(value="")
        tk.Label(fbar,textvariable=self._flt_count_var,bg=C["card"],fg=C["text3"],
                 font=F_SMALL).pack(side="right",padx=12)

        tree_host=tk.Frame(pg,bg=C["bg"]); tree_host.pack(fill="both",expand=True,padx=20,pady=(0,6))
        RES_COLS=MatchEngine.COLS
        RES_W=[55,110,120,170,55,110,120,170,420,65,110]
        self._res_tree=make_scrolled_tree(tree_host,RES_COLS,RES_W)
        for col in RES_COLS:
            self._res_tree.heading(col,text=col,anchor="w",
                                   command=lambda c=col:self._sort_results(c))

    def _populate_results(self):
        if self.results_df is None: return
        for w in self._summary_host.winfo_children(): w.destroy()
        df=self.results_df
        stats=[
            ("Total Matches",    f"{len(df):,}",                                  C["blue"]),
            ("Exact",            f"{(df['Match Type']=='Exact').sum():,}",         C["green"]),
            ("Substring",        f"{(df['Match Type']=='Substring').sum():,}",     C["blue_mid"]),
            ("Token / Fuzzy",    f"{df['Match Type'].isin(['Token Overlap','Fuzzy']).sum():,}", C["amber"]),
            ("Avg Score",        f"{df['Score %'].mean():.1f}%",                  C["teal"]),
            ("Unique Src Values",f"{df['Src Value'].nunique():,}",                C["navy2"]),
            ("Unique Anchors",   f"{df['Src Anchor'].nunique():,}",               C["purple"]),
        ]
        for label,val,color in stats:
            card=tk.Frame(self._summary_host,bg=C["card"],
                          highlightthickness=1,highlightbackground=C["border"],padx=14,pady=7)
            card.pack(side="left",padx=(0,6))
            tk.Label(card,text=val,bg=C["card"],fg=color,font=("Georgia",14,"bold")).pack()
            tk.Label(card,text=label,bg=C["card"],fg=C["text2"],font=F_SMALL).pack()
        self._load_results_tree(df)

    def _load_results_tree(self,df:pd.DataFrame):
        tree=self._res_tree; tree.delete(*tree.get_children())
        cols=MatchEngine.COLS; cap=5000
        for _,row in df.head(cap).iterrows():
            mtype=str(row.get("Match Type",""))
            vals=[str(row.get(c,""))[:200] for c in cols]
            tree.insert("","end",values=vals,tags=(mtype,))
        if len(df)>cap:
            tree.insert("","end",values=[f"  \u22ef  {len(df)-cap:,} more rows \u2014 export to see all"]+[""]*( len(cols)-1))
        self._flt_count_var.set(f"{min(len(df),cap):,} of {len(df):,} shown")

    def _apply_filter(self,*_):
        if self.results_df is None: return
        df=self.results_df.copy()
        q=self._flt_var.get().lower().strip()
        if q: df=df[df.apply(lambda r:any(q in str(v).lower() for v in r),axis=1)]
        t=self._type_var.get()
        if t!="All": df=df[df["Match Type"]==t]
        ms=self._min_score_var.get(); df=df[df["Score %"]>=ms]
        self._load_results_tree(df)

    _sort_asc:Dict[str,bool]={}
    def _sort_results(self,col:str):
        if self.results_df is None: return
        asc=not self._sort_asc.get(col,False); self._sort_asc[col]=asc
        df=self.results_df.copy()
        if col=="Score %": df[col]=pd.to_numeric(df[col],errors="coerce")
        df.sort_values(col,ascending=asc,inplace=True)
        self.results_df=df; self._load_results_tree(df)

    # ── Export ─────────────────────────────────────────────────────────────────
    def _export_results(self):
        if self.results_df is None or self.results_df.empty:
            messagebox.showinfo("Export","No match results to export yet."); return
        path=filedialog.asksaveasfilename(defaultextension=".xlsx",
            filetypes=[("Excel Workbook","*.xlsx"),("CSV","*.csv")],title="Export Match Results")
        if not path: return
        try:
            if path.endswith(".csv"):
                self.results_df.to_csv(path,index=False,encoding="utf-8-sig")
            else:
                with pd.ExcelWriter(path,engine="openpyxl") as writer:
                    self.results_df.to_excel(writer,sheet_name="Match Results",index=False)
                    if self.src_df is not None:
                        self.src_df.to_excel(writer,sheet_name="Cleaned Source",index=False)
                    if self.ref_df is not None:
                        self.ref_df.to_excel(writer,sheet_name="Reference File",index=False)
            self._status(f"Exported to {Path(path).name}")
            messagebox.showinfo("Export Complete",f"Saved successfully:\n{path}")
        except Exception as exc:
            messagebox.showerror("Export Error",str(exc))

    def _export_cleaned(self):
        if self.src_df is None:
            messagebox.showinfo("Export","No cleaned source to export yet."); return
        path=filedialog.asksaveasfilename(defaultextension=".xlsx",
            filetypes=[("Excel Workbook","*.xlsx"),("CSV","*.csv")],title="Export Cleaned Source")
        if not path: return
        try:
            if path.endswith(".csv"): self.src_df.to_csv(path,index=False,encoding="utf-8-sig")
            else: self.src_df.to_excel(path,index=False)
            self._status(f"Cleaned source exported to {Path(path).name}")
            messagebox.showinfo("Export Complete",f"Saved successfully:\n{path}")
        except Exception as exc:
            messagebox.showerror("Export Error",str(exc))

    # ── Error ──────────────────────────────────────────────────────────────────
    def _on_error(self,title:str,exc:Exception):
        self._set_busy(); self._status(f"Error: {exc}")
        self._stop_btn.pack_forget()
        self._stop_btn.configure(state="normal",text="\u23f9  Stop")
        self._run_btn.pack(side="left",padx=6)
        self._proceed_btn.configure(state="normal",text="Parse & Clean  \u2192",bg=C["blue"])
        messagebox.showerror(title,str(exc))


# ═══════════════════════════════════════════════════════════════════════════════
#  WIDGET FACTORIES
# ═══════════════════════════════════════════════════════════════════════════════
def _section_label(parent,text:str):
    tk.Label(parent,text=text,bg=C["card"],fg=C["text"],font=F_SUBHEAD).pack(anchor="w",pady=(0,4))

def _back_btn(parent,text:str,cmd):
    tk.Button(parent,text=text,bg=C["card"],fg=C["text2"],font=F_BODY,relief="flat",
              padx=12,pady=6,cursor="hand2",
              highlightthickness=1,highlightbackground=C["border2"],command=cmd
              ).pack(side="left",padx=(0,6))

def _nav_btn(parent,text:str,cmd,bg:str=C["blue"]):
    tk.Button(parent,text=text,bg=bg,fg=C["text_inv"],font=F_BODY_B,relief="flat",
              padx=14,pady=6,cursor="hand2",command=cmd).pack(side="left",padx=(0,6))

# ═══════════════════════════════════════════════════════════════════════════════
#  ENTRY POINT
# ═══════════════════════════════════════════════════════════════════════════════
def _check_deps():
    missing=[]
    for pkg,pip in [("customtkinter","customtkinter"),("sklearn","scikit-learn"),
                    ("rapidfuzz","rapidfuzz"),("openpyxl","openpyxl")]:
        try: __import__(pkg)
        except ImportError: missing.append(pip)
    if missing:
        import subprocess,sys
        subprocess.check_call([sys.executable,"-m","pip","install","--quiet"]+missing)

if __name__=="__main__":
    _check_deps()
    App().mainloop()
