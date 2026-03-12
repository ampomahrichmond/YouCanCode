"""
MetaHarvest v3  ·  Main Application Window
Enterprise Metadata Discovery & Collibra Ingestion Platform
"""
from __future__ import annotations
import customtkinter as ctk
from tkinter import messagebox, filedialog
import tkinter as tk
import threading, json, os
from datetime import datetime
from typing import List, Optional, Dict, Any

from app.config import (C, APP_NAME, APP_SUBTITLE, APP_VERSION,
                         SOURCE_TYPES, ENVIRONMENTS, ENV_COLORS, ENV_IDS, ENV_LABELS)
from app.models import (ConnectionConfig, TableMeta, ScanResult,
                         IngestionResult, CollibraCommunity, CollibraDomain,
                         DQRunResult, DQTableResult)
import app.storage as storage

ctk.set_appearance_mode("dark")
ctk.set_default_color_theme("blue")

BG   = C["bg_main"];   PANEL = C["bg_panel"];  CARD  = C["bg_card"]
HOVER= C["bg_hover"];  ACCN  = C["accent"];    PURP  = C["purple"]
TEAL = C["teal"];      AMBN  = C["amber"];     ERR   = C["error"]
SUCC = C["success"];   TXT   = C["text_primary"]; TXS = C["text_sec"]
TXD  = C["text_dim"];  BORD  = C["border"]

# ══════════════════════════════════════════════════════════════
#  Reusable Widgets
# ══════════════════════════════════════════════════════════════
class GlowLabel(ctk.CTkLabel):
    def __init__(self, m, text, size=13, color=TXT, **k):
        super().__init__(m, text=text, font=ctk.CTkFont("Consolas", size),
                         text_color=color, **k)

class SectionHeader(ctk.CTkLabel):
    def __init__(self, m, text, **k):
        super().__init__(m, text=text.upper(),
                         font=ctk.CTkFont("Consolas", 10, weight="bold"),
                         text_color=TXD, **k)

class AccentButton(ctk.CTkButton):
    def __init__(self, m, text, command=None, color=ACCN, width=130, **k):
        super().__init__(m, text=text, command=command,
                         fg_color=color, hover_color=C["accent_dim"],
                         font=ctk.CTkFont("Consolas", 12, weight="bold"),
                         corner_radius=6, height=36, width=width, **k)

class GhostButton(ctk.CTkButton):
    def __init__(self, m, text, command=None, width=120, color=TXS, **k):
        super().__init__(m, text=text, command=command,
                         fg_color=C["bg_panel"], hover_color=HOVER,
                         border_color=BORD, border_width=1, text_color=color,
                         font=ctk.CTkFont("Consolas", 12),
                         corner_radius=6, height=34, width=width, **k)

class DataEntry(ctk.CTkEntry):
    def __init__(self, m, placeholder="", show="", width=300, **k):
        super().__init__(m, placeholder_text=placeholder, show=show, width=width,
                         fg_color=C["bg_input"], border_color=BORD, text_color=TXT,
                         placeholder_text_color=TXD,
                         font=ctk.CTkFont("Consolas", 12),
                         corner_radius=6, height=36, **k)

class StatusBadge(ctk.CTkLabel):
    STATUS_COLORS = {
        "ok": SUCC, "success": SUCC, "complete": SUCC,
        "running": ACCN, "scanning": ACCN,
        "error": ERR, "partial": AMBN, "fail": ERR,
        "warn": AMBN, "warning": AMBN,
        "untested": TXD, "pending": TXD,
    }
    STATUS_BG = {
        "ok": "#0D2E1F", "success": "#0D2E1F", "complete": "#0D2E1F",
        "running": "#0D2233", "scanning": "#0D2233",
        "error": "#2E0D14", "fail": "#2E0D14", "partial": "#2A1800",
        "warn": "#2A1800", "warning": "#2A1800",
        "untested": "#1A2030", "pending": "#1A2030",
    }
    def __init__(self, m, status="untested", **k):
        color = self.STATUS_COLORS.get(status, TXD)
        bg    = self.STATUS_BG.get(status, "#1A2030")
        super().__init__(m, text=f"●  {status.upper()}",
                         font=ctk.CTkFont("Consolas", 10, weight="bold"),
                         text_color=color, fg_color=bg,
                         corner_radius=4, padx=8, pady=2, **k)

class EnvBadge(ctk.CTkFrame):
    """Colored bordered badge for environment labels.
    CTkLabel does not support border_color — we use a CTkFrame border instead."""
    def __init__(self, m, env_id="dev", **k):
        color = ENV_COLORS.get(env_id, TXD)
        label = env_id.upper()
        super().__init__(m, fg_color=CARD, corner_radius=4,
                         border_color=color, border_width=1, **k)
        ctk.CTkLabel(self, text=f" {label} ",
                     font=ctk.CTkFont("Consolas", 10, weight="bold"),
                     text_color=color, fg_color="transparent"
                     ).pack(padx=4, pady=2)

class Card(ctk.CTkFrame):
    def __init__(self, m, **k):
        super().__init__(m, fg_color=CARD, corner_radius=10,
                         border_color=BORD, border_width=1, **k)

class ProgressCard(ctk.CTkFrame):
    def __init__(self, m, label="", **k):
        super().__init__(m, fg_color=CARD, corner_radius=8,
                         border_color=BORD, border_width=1, **k)
        self._lbl = ctk.CTkLabel(self, text=label,
                                  font=ctk.CTkFont("Consolas", 12), text_color=TXS)
        self._lbl.pack(anchor="w", padx=16, pady=(12, 4))
        self._bar = ctk.CTkProgressBar(self, height=6, progress_color=ACCN, fg_color=C["bg_input"])
        self._bar.set(0)
        self._bar.pack(fill="x", padx=16, pady=(0, 4))
        self._sub = ctk.CTkLabel(self, text="", font=ctk.CTkFont("Consolas", 10), text_color=TXD)
        self._sub.pack(anchor="w", padx=16, pady=(0, 12))

    def update(self, msg="", val=0.0, sub=""):
        self._lbl.configure(text=msg)
        self._bar.set(min(max(val, 0), 1))
        self._sub.configure(text=sub)

def log_append(widget, msg):
    widget.configure(state="normal")
    widget.insert("end", f"[{datetime.now().strftime('%H:%M:%S')}]  {msg}\n")
    widget.see("end")
    widget.configure(state="disabled")

def make_log_box(parent, height=300):
    box = ctk.CTkTextbox(parent, height=height, fg_color=C["bg_input"],
                          text_color=SUCC, font=ctk.CTkFont("Consolas", 11),
                          border_color=BORD, border_width=1, corner_radius=8)
    box.configure(state="disabled")
    return box


# ══════════════════════════════════════════════════════════════
#  Sidebar
# ══════════════════════════════════════════════════════════════
NAV_ICONS  = {"dashboard":"⬡","environments":"◈","connections":"◉",
              "scanner":"⟳","preview":"⬢","dq":"⚑","collibra":"◆",
              "logs":"≡","settings":"⚙"}
NAV_LABELS = {"dashboard":"Dashboard","environments":"Environments",
              "connections":"Connections","scanner":"Scanner",
              "preview":"Metadata Preview","dq":"Data Quality",
              "collibra":"Collibra Ingest","logs":"Audit Logs","settings":"Settings"}

class Sidebar(ctk.CTkFrame):
    def __init__(self, master, on_nav, **k):
        super().__init__(master, fg_color=PANEL, width=220, corner_radius=0, **k)
        self.on_nav      = on_nav
        self._active_key = "dashboard"
        self._btns: Dict[str, ctk.CTkButton] = {}
        self._env_lbl    = None
        self._build()

    def _build(self):
        lf = ctk.CTkFrame(self, fg_color="transparent")
        lf.pack(fill="x", pady=(28, 4), padx=20)
        ctk.CTkLabel(lf, text="⬡ MetaHarvest",
                     font=ctk.CTkFont("Consolas", 20, weight="bold"),
                     text_color=ACCN).pack(anchor="w")
        ctk.CTkLabel(lf, text=f"Enterprise Harvester  v{APP_VERSION}",
                     font=ctk.CTkFont("Consolas", 10), text_color=TXD).pack(anchor="w", pady=(2,0))

        # Active env indicator
        self._env_lbl = ctk.CTkLabel(lf, text="ENV: DEV",
                                      font=ctk.CTkFont("Consolas", 10, weight="bold"),
                                      text_color=SUCC, fg_color=CARD,
                                      corner_radius=4, padx=8, pady=2)
        self._env_lbl.pack(anchor="w", pady=(6, 0))

        ctk.CTkFrame(self, height=1, fg_color=BORD).pack(fill="x", padx=16, pady=16)
        SectionHeader(self, "NAVIGATION").pack(anchor="w", padx=20, pady=(0, 6))

        for key, label in NAV_LABELS.items():
            icon = NAV_ICONS.get(key, "●")
            btn  = ctk.CTkButton(
                self, text=f"  {icon}   {label}", anchor="w",
                fg_color=PANEL, hover_color=HOVER, text_color=TXS,
                font=ctk.CTkFont("Consolas", 13), corner_radius=8,
                height=40, command=lambda k=key: self._nav(k))
            btn.pack(fill="x", padx=12, pady=2)
            self._btns[key] = btn

        ctk.CTkFrame(self, height=1, fg_color=BORD).pack(
            fill="x", padx=16, side="bottom", pady=(0, 12))
        ctk.CTkLabel(self, text="Data Discovery & Metadata Team",
                     font=ctk.CTkFont("Consolas", 9), text_color=TXD
                     ).pack(side="bottom", pady=2)
        self._highlight("dashboard")

    def _nav(self, key):
        self._active_key = key
        self._highlight(key)
        self.on_nav(key)

    def _highlight(self, active):
        for key, btn in self._btns.items():
            if key == active:
                btn.configure(fg_color=C["accent_glow"], text_color=ACCN,
                               border_color=ACCN, border_width=1)
            else:
                btn.configure(fg_color=PANEL, text_color=TXS,
                               border_color=PANEL, border_width=0)

    def set_active(self, key): self._nav(key)

    def set_env(self, env_id: str):
        color = ENV_COLORS.get(env_id, TXD)
        self._env_lbl.configure(text=f"ENV: {env_id.upper()}", text_color=color)


# ══════════════════════════════════════════════════════════════
#  Top Bar
# ══════════════════════════════════════════════════════════════
class TopBar(ctk.CTkFrame):
    def __init__(self, master, **k):
        super().__init__(master, fg_color=PANEL, height=56, corner_radius=0, **k)
        self.grid_propagate(False)
        self._title = ctk.CTkLabel(self, text="Dashboard",
                                    font=ctk.CTkFont("Consolas", 17, weight="bold"),
                                    text_color=TXT)
        self._title.pack(side="left", padx=28)
        self._status = ctk.CTkLabel(self, text="",
                                     font=ctk.CTkFont("Consolas", 11),
                                     text_color=SUCC)
        self._status.pack(side="right", padx=20)
        self._ts = ctk.CTkLabel(self, text="",
                                 font=ctk.CTkFont("Consolas", 11), text_color=TXD)
        self._ts.pack(side="right", padx=8)
        self._tick(self._ts)

    def set_title(self, t): self._title.configure(text=t)
    def set_status(self, t, color=SUCC): self._status.configure(text=t, text_color=color)

    def _tick(self, lbl):
        lbl.configure(text=datetime.now().strftime("%a %d %b %Y  %H:%M"))
        lbl.after(30_000, lambda: self._tick(lbl))


# ══════════════════════════════════════════════════════════════
#  PAGE: Dashboard
# ══════════════════════════════════════════════════════════════
class DashboardPage(ctk.CTkScrollableFrame):
    def __init__(self, master, app, **k):
        super().__init__(master, fg_color=BG, **k)
        self.app = app
        self._build()

    def _build(self):
        # Header
        f = ctk.CTkFrame(self, fg_color="transparent")
        f.pack(fill="x", padx=32, pady=(28, 8))
        ctk.CTkLabel(f, text="Enterprise Metadata Harvester",
                     font=ctk.CTkFont("Consolas", 26, weight="bold"),
                     text_color=TXT).pack(anchor="w")
        ctk.CTkLabel(f,
                     text="Discover · Harvest · Govern  ─  16 Source Connectors · DQ Engine · Collibra REST v2",
                     font=ctk.CTkFont("Consolas", 11), text_color=TXD).pack(anchor="w", pady=(4,0))
        ctk.CTkFrame(self, height=1, fg_color=BORD).pack(fill="x", padx=32, pady=(12, 20))

        # Stats row
        conns  = storage.load_connections()
        scans  = storage.load_scan_history()
        dq     = storage.load_dq_history()
        tables = sum(s.get("table_count", 0) for s in scans[:10])
        dq_fails = sum(1 for d in dq[:20] if d.get("status") in ("fail","warn"))

        stats = [
            ("Connections",    str(len(conns)),   ACCN, "Active sources"),
            ("Recent Scans",   str(len(scans)),   PURP, "Last 100"),
            ("Objects Found",  f"{tables:,}",     TEAL, "Across recent scans"),
            ("DQ Runs",        str(len(dq)),       AMBN, f"{dq_fails} with issues"),
        ]
        row = ctk.CTkFrame(self, fg_color="transparent")
        row.pack(fill="x", padx=32, pady=(0, 24))
        for label, val, color, sub in stats:
            c = Card(row)
            c.pack(side="left", padx=(0, 16), fill="both", expand=True)
            ctk.CTkLabel(c, text=label, font=ctk.CTkFont("Consolas", 11),
                         text_color=TXD).pack(anchor="w", padx=20, pady=(16, 4))
            ctk.CTkLabel(c, text=val, font=ctk.CTkFont("Consolas", 28, weight="bold"),
                         text_color=color).pack(anchor="w", padx=20)
            ctk.CTkLabel(c, text=sub, font=ctk.CTkFont("Consolas", 10),
                         text_color=TXD).pack(anchor="w", padx=20, pady=(2, 16))

        # Source tiles
        SectionHeader(self, "SUPPORTED SOURCES").pack(anchor="w", padx=32, pady=(0, 10))
        groups: Dict[str, list] = {}
        for s in SOURCE_TYPES:
            groups.setdefault(s["group"], []).append(s)
        for grp, sources in groups.items():
            gf = ctk.CTkFrame(self, fg_color="transparent")
            gf.pack(fill="x", padx=32, pady=(0, 8))
            ctk.CTkLabel(gf, text=grp,
                         font=ctk.CTkFont("Consolas", 10), text_color=TXD).pack(anchor="w", pady=(0,4))
            rf = ctk.CTkFrame(gf, fg_color="transparent")
            rf.pack(fill="x")
            for src in sources:
                tile = ctk.CTkFrame(rf, fg_color=CARD, corner_radius=8,
                                     border_color=BORD, border_width=1)
                tile.pack(side="left", padx=(0, 8), pady=2)
                ctk.CTkLabel(tile, text=src["icon"],
                             font=ctk.CTkFont("Consolas", 16)).pack(side="left", padx=(10,4), pady=8)
                ctk.CTkLabel(tile, text=src["label"],
                             font=ctk.CTkFont("Consolas", 11),
                             text_color=src["color"]).pack(side="left", padx=(0, 12), pady=8)

        # Quick actions
        ctk.CTkFrame(self, height=1, fg_color=BORD).pack(fill="x", padx=32, pady=16)
        SectionHeader(self, "QUICK ACTIONS").pack(anchor="w", padx=32, pady=(0, 10))
        af = ctk.CTkFrame(self, fg_color="transparent")
        af.pack(fill="x", padx=32, pady=(0, 24))
        for label, page, color in [
            ("+ Add Connection",      "connections", ACCN),
            ("⟳  Run Scanner",        "scanner",     PURP),
            ("⚑  Data Quality",       "dq",          AMBN),
            ("⬆  Ingest to Collibra", "collibra",    C["collibra"]),
        ]:
            AccentButton(af, label, command=lambda p=page: self.app.sidebar.set_active(p),
                         color=color, width=195).pack(side="left", padx=(0, 12))

        # Recent scans table
        SectionHeader(self, "RECENT SCANS").pack(anchor="w", padx=32, pady=(8, 10))
        self._render_scan_table(scans[:6])

        # Recent DQ
        if dq:
            SectionHeader(self, "RECENT DQ RUNS").pack(anchor="w", padx=32, pady=(12, 10))
            self._render_dq_table(dq[:6])

    def _render_scan_table(self, scans):
        if not scans:
            ctk.CTkLabel(self, text="No scans yet.", font=ctk.CTkFont("Consolas", 12),
                         text_color=TXD).pack(padx=32, pady=4)
            return
        card = Card(self)
        card.pack(fill="x", padx=32, pady=(0, 20))
        headers = ["Connection", "Env", "Type", "Started", "Tables", "Status"]
        widths  = [200, 70, 100, 160, 80, 90]
        hrow = ctk.CTkFrame(card, fg_color=HOVER, corner_radius=6)
        hrow.pack(fill="x", padx=12, pady=(12, 4))
        for h, w in zip(headers, widths):
            ctk.CTkLabel(hrow, text=h.upper(), font=ctk.CTkFont("Consolas", 10, weight="bold"),
                         text_color=TXD, width=w, anchor="w").pack(side="left", padx=8, pady=8)
        for s in scans:
            row = ctk.CTkFrame(card, fg_color="transparent")
            row.pack(fill="x", padx=12)
            ctk.CTkFrame(card, height=1, fg_color=BORD).pack(fill="x", padx=12)
            st = s.get("status","")
            sc = SUCC if st == "complete" else ERR if st == "error" else AMBN
            vals = [s.get("connection_name",""), s.get("environment","dev").upper(),
                    s.get("source_type","").upper(),
                    s.get("started_at","")[:16].replace("T"," "),
                    str(s.get("table_count",0)), st.upper()]
            cols = [TXT, ENV_COLORS.get(s.get("environment","dev"), TXS),
                    TXS, TXD, TEAL, sc]
            for v, w, c in zip(vals, widths, cols):
                ctk.CTkLabel(row, text=v, font=ctk.CTkFont("Consolas", 11),
                             text_color=c, width=w, anchor="w").pack(side="left", padx=8, pady=10)

    def _render_dq_table(self, dq_rows):
        card = Card(self)
        card.pack(fill="x", padx=32, pady=(0, 32))
        headers = ["Run ID", "Source Env", "Target Env", "Tables", "Pass", "Warn", "Fail", "Status"]
        widths  = [120, 90, 90, 80, 70, 70, 70, 90]
        hrow = ctk.CTkFrame(card, fg_color=HOVER, corner_radius=6)
        hrow.pack(fill="x", padx=12, pady=(12, 4))
        for h, w in zip(headers, widths):
            ctk.CTkLabel(hrow, text=h.upper(), font=ctk.CTkFont("Consolas", 10, weight="bold"),
                         text_color=TXD, width=w, anchor="w").pack(side="left", padx=8, pady=8)
        for d in dq_rows:
            row = ctk.CTkFrame(card, fg_color="transparent")
            row.pack(fill="x", padx=12)
            ctk.CTkFrame(card, height=1, fg_color=BORD).pack(fill="x", padx=12)
            st = d.get("status","")
            sc = SUCC if st == "ok" else ERR if st == "fail" else AMBN
            for v, w, c in zip(
                [d.get("run_id","")[:10], d.get("source_env",""),
                 d.get("target_env",""), str(d.get("total_tables",0)),
                 str(d.get("summary_pass",0)), str(d.get("summary_warn",0)),
                 str(d.get("summary_fail",0)), st.upper()],
                widths,
                [TXD, ACCN, TXS, TXT, SUCC, AMBN, ERR, sc]
            ):
                ctk.CTkLabel(row, text=v, font=ctk.CTkFont("Consolas", 11),
                             text_color=c, width=w, anchor="w").pack(side="left", padx=8, pady=10)

    def refresh(self):
        for w in self.winfo_children(): w.destroy()
        self._build()


# ══════════════════════════════════════════════════════════════
#  PAGE: Environments
# ══════════════════════════════════════════════════════════════
class EnvironmentsPage(ctk.CTkScrollableFrame):
    def __init__(self, master, app, **k):
        super().__init__(master, fg_color=BG, **k)
        self.app = app
        self._build()

    def _build(self):
        hdr = ctk.CTkFrame(self, fg_color="transparent")
        hdr.pack(fill="x", padx=32, pady=24)
        ctk.CTkLabel(hdr, text="Environment Profiles",
                     font=ctk.CTkFont("Consolas", 20, weight="bold"),
                     text_color=TXT).pack(side="left")
        ctk.CTkFrame(self, height=1, fg_color=BORD).pack(fill="x", padx=32, pady=(0, 20))

        ctk.CTkLabel(self,
                     text="Assign connections to environments. Switch the active environment to scope scans and ingestion.",
                     font=ctk.CTkFont("Consolas", 11), text_color=TXD).pack(anchor="w", padx=32, pady=(0, 16))

        # Active environment selector
        ae_card = Card(self)
        ae_card.pack(fill="x", padx=32, pady=(0, 20))
        ctk.CTkLabel(ae_card, text="Active Environment",
                     font=ctk.CTkFont("Consolas", 14, weight="bold"),
                     text_color=TXT).pack(anchor="w", padx=20, pady=(16, 8))
        rf = ctk.CTkFrame(ae_card, fg_color="transparent")
        rf.pack(fill="x", padx=20, pady=(0, 16))
        for env in ENVIRONMENTS:
            btn = AccentButton(rf, env["label"],
                               command=lambda e=env: self._set_active_env(e),
                               color=env["color"], width=90)
            btn.pack(side="left", padx=(0, 8))
        self._active_lbl = ctk.CTkLabel(ae_card, text="",
                                         font=ctk.CTkFont("Consolas", 11), text_color=SUCC)
        self._active_lbl.pack(anchor="w", padx=20, pady=(0, 8))
        self._refresh_active_lbl()

        # Per-environment connection matrix
        SectionHeader(self, "CONNECTION ASSIGNMENTS").pack(anchor="w", padx=32, pady=(8, 12))
        conns = storage.load_connections()
        profiles = storage.load_env_profiles()

        for env in ENVIRONMENTS:
            ec = Card(self)
            ec.pack(fill="x", padx=32, pady=(0, 12))
            eh = ctk.CTkFrame(ec, fg_color=HOVER, corner_radius=8)
            eh.pack(fill="x", padx=12, pady=(12, 8))
            ctk.CTkLabel(eh, text=f"  {env['label']}  —  {env['desc']}",
                         font=ctk.CTkFont("Consolas", 13, weight="bold"),
                         text_color=env["color"]).pack(side="left", padx=12, pady=10)
            env_conns = [c for c in conns if c.environment == env["id"]]
            ctk.CTkLabel(eh, text=f"{len(env_conns)} connection(s)",
                         font=ctk.CTkFont("Consolas", 11), text_color=TXD
                         ).pack(side="right", padx=12)
            for conn in env_conns:
                src = next((s for s in SOURCE_TYPES if s["id"] == conn.source_type), {})
                rf2 = ctk.CTkFrame(ec, fg_color="transparent")
                rf2.pack(fill="x", padx=20, pady=2)
                ctk.CTkLabel(rf2, text=f"  {src.get('icon','●')}  {conn.name}",
                             font=ctk.CTkFont("Consolas", 12), text_color=TXT,
                             width=280, anchor="w").pack(side="left")
                ctk.CTkLabel(rf2, text=src.get("label", conn.source_type),
                             font=ctk.CTkFont("Consolas", 11), text_color=TXS,
                             width=150, anchor="w").pack(side="left")
                StatusBadge(rf2, conn.status).pack(side="left", padx=8)
            if not env_conns:
                ctk.CTkLabel(ec, text="   No connections assigned to this environment.",
                             font=ctk.CTkFont("Consolas", 11), text_color=TXD
                             ).pack(anchor="w", padx=20, pady=8)
            ctk.CTkFrame(ec, height=4).pack()

        ctk.CTkLabel(self, text="To assign a connection to an environment, edit the connection and set its Environment field.",
                     font=ctk.CTkFont("Consolas", 11), text_color=TXD
                     ).pack(anchor="w", padx=32, pady=(8, 32))

    def _set_active_env(self, env: dict):
        self.app.active_env = env["id"]
        self.app.sidebar.set_env(env["id"])
        self._refresh_active_lbl()
        self.app.topbar.set_status(f"✔  Active environment: {env['label']}", env["color"])

    def _refresh_active_lbl(self):
        env_id = self.app.active_env
        color  = ENV_COLORS.get(env_id, TXD)
        self._active_lbl.configure(
            text=f"Current: {env_id.upper()}",
            text_color=color)

    def refresh(self):
        for w in self.winfo_children(): w.destroy()
        self._build()


# ══════════════════════════════════════════════════════════════
#  PAGE: Connections  (with expanded connector form)
# ══════════════════════════════════════════════════════════════
class ConnectionsPage(ctk.CTkFrame):
    def __init__(self, master, app, **k):
        super().__init__(master, fg_color=BG, **k)
        self.app = app
        self._build()

    def _build(self):
        top = ctk.CTkFrame(self, fg_color="transparent")
        top.pack(fill="x", padx=32, pady=24)
        ctk.CTkLabel(top, text="Data Source Connections",
                     font=ctk.CTkFont("Consolas", 20, weight="bold"),
                     text_color=TXT).pack(side="left")
        AccentButton(top, "+ New Connection", command=self._new_conn).pack(side="right")
        ctk.CTkFrame(self, height=1, fg_color=BORD).pack(fill="x", padx=32, pady=(0, 16))
        self._list_frame = ctk.CTkScrollableFrame(self, fg_color="transparent")
        self._list_frame.pack(fill="both", expand=True, padx=32, pady=(0, 16))
        self._render_list()

    def _render_list(self):
        for w in self._list_frame.winfo_children(): w.destroy()
        conns = storage.load_connections()
        # Group by environment
        groups: Dict[str, list] = {}
        for c in conns:
            groups.setdefault(c.environment, []).append(c)
        if not conns:
            ctk.CTkLabel(self._list_frame,
                         text="No connections yet.\nClick '+ New Connection' to add your first source.",
                         font=ctk.CTkFont("Consolas", 13), text_color=TXD,
                         justify="center").pack(expand=True, pady=60)
            return
        for env_id in ENV_IDS:
            env_conns = groups.get(env_id, [])
            if not env_conns: continue
            env_def = next(e for e in ENVIRONMENTS if e["id"] == env_id)
            ctk.CTkLabel(self._list_frame, text=f"  {env_def['label']}  —  {env_def['desc']}",
                         font=ctk.CTkFont("Consolas", 11, weight="bold"),
                         text_color=env_def["color"]).pack(anchor="w", pady=(12, 4))
            for conn in env_conns:
                self._render_card(conn)

    def _render_card(self, conn: ConnectionConfig):
        src  = next((s for s in SOURCE_TYPES if s["id"] == conn.source_type), {})
        icon = src.get("icon", "●")
        col  = src.get("color", ACCN)

        # Outer wrapper holds card row + collapsible action bar
        wrapper = ctk.CTkFrame(self._list_frame, fg_color="transparent")
        wrapper.pack(fill="x", pady=4)

        card = ctk.CTkFrame(wrapper, fg_color=CARD, corner_radius=10,
                            border_color=BORD, border_width=1)
        card.pack(fill="x")

        # ── Left: icon + name + host ──────────────────────────
        icon_lbl = ctk.CTkLabel(card, text=icon, font=ctk.CTkFont("Consolas", 22),
                                 text_color=col)
        icon_lbl.pack(side="left", padx=(16, 0), pady=14)

        name_lbl = ctk.CTkLabel(card, text=conn.name,
                                 font=ctk.CTkFont("Consolas", 13, weight="bold"),
                                 text_color=TXT, anchor="w")
        name_lbl.pack(side="left", padx=(12, 0), pady=14)

        host = (conn.host or conn.account_name or conn.account or conn.folder_path or conn.dsn or "—")
        host_lbl = ctk.CTkLabel(card,
                                 text=f"  {src.get('label', conn.source_type)}  ·  {host}",
                                 font=ctk.CTkFont("Consolas", 10), text_color=TXD, anchor="w")
        host_lbl.pack(side="left", pady=14)

        # ── Right: badges + action buttons ───────────────────
        right = ctk.CTkFrame(card, fg_color="transparent")
        right.pack(side="right", padx=12)
        EnvBadge(right, conn.environment).pack(side="left", padx=4)
        StatusBadge(right, conn.status).pack(side="left", padx=4)
        GhostButton(right, "✎ Edit",   command=lambda c=conn: self._edit(c), width=76).pack(side="left", padx=2)
        GhostButton(right, "⚡ Test",  command=lambda c=conn: self._test(c), width=76, color=TEAL).pack(side="left", padx=2)
        GhostButton(right, "✕ Delete", command=lambda c=conn: self._delete(c), width=76, color=ERR).pack(side="left", padx=2)

        # ── Collapsible action bar (hidden by default) ────────
        action_bar = ctk.CTkFrame(wrapper, fg_color=C["bg_hover"], corner_radius=8,
                                   border_color=ACCN, border_width=1)
        selected   = [False]   # mutable closure flag — no widget attribute needed

        def _show_bar():
            selected[0] = True
            card.configure(border_color=ACCN)
            action_bar.pack(fill="x", pady=(2, 0))
            ctk.CTkLabel(action_bar, text="  Connection selected:",
                         font=ctk.CTkFont("Consolas", 11), text_color=TXS
                         ).pack(side="left", padx=(12, 8), pady=8)
            AccentButton(action_bar, "⏏  Disconnect & Remove",
                         command=lambda c=conn: self._disconnect(c),
                         color=ERR, width=200).pack(side="left", pady=8)
            GhostButton(action_bar, "✕  Deselect",
                        command=_hide_bar, width=100
                        ).pack(side="left", padx=8, pady=8)

        def _hide_bar():
            selected[0] = False
            card.configure(border_color=BORD)
            action_bar.pack_forget()
            for w in action_bar.winfo_children():
                w.destroy()

        def _on_click(event=None):
            if selected[0]:
                _hide_bar()
            else:
                _show_bar()

        # Bind only to known safe widgets (no winfo_children traversal)
        for w in (card, icon_lbl, name_lbl, host_lbl):
            w.bind("<Button-1>", _on_click)

    def _new_conn(self): ConnDialog(self, is_new=True, on_save=self._render_list)
    def _edit(self, c):  ConnDialog(self, is_new=False, conn=c, on_save=self._render_list)

    def _test(self, conn):
        from app.connectors.connectors import get_connector
        self.app.topbar.set_status("Testing connection…", AMBN)
        def run():
            try:
                ok, msg = get_connector(conn).test_connection()
                conn.status = "ok" if ok else "error"
                conn.last_tested = datetime.now().isoformat()
                storage.upsert_connection(conn)
                self.after(0, lambda: self.app.topbar.set_status(
                    f"{'✔' if ok else '✖'}  {msg}", SUCC if ok else ERR))
                self.after(0, self._render_list)
            except Exception as e:
                self.after(0, lambda: self.app.topbar.set_status(f"✖  {e}", ERR))
        threading.Thread(target=run, daemon=True).start()

    def _delete(self, conn):
        if messagebox.askyesno("Delete", f"Delete '{conn.name}'?"):
            storage.delete_connection(conn.id)
            self._render_list()

    def _disconnect(self, conn):
        if messagebox.askyesno(
            "Disconnect & Remove",
            f"Disconnect and remove '{conn.name}'?\n\n"
            "This removes it from MetaHarvest. The source itself is untouched."
        ):
            storage.delete_connection(conn.id)
            self.app.topbar.set_status(f"✔  '{conn.name}' disconnected.", C["success"])
            self._render_list()


# ══════════════════════════════════════════════════════════════
#  Connection Dialog  (all source types)
# ══════════════════════════════════════════════════════════════
class ConnDialog(ctk.CTkToplevel):
    def __init__(self, master, is_new=True, conn=None, on_save=None):
        super().__init__(master)
        self.title("New Connection" if is_new else "Edit Connection")
        self.geometry("720x820")
        self.configure(fg_color=BG)
        self.resizable(True, True)
        self.on_save = on_save
        self.conn    = conn or ConnectionConfig()
        self.is_new  = is_new
        self._entries: Dict[str, Any] = {}
        self._build()
        self.lift()
        self.focus_force()
        # Defer grab_set — calling it immediately blocks rendering on Windows/CTk 5.x
        self.after(150, self._safe_grab)

    def _safe_grab(self):
        try:
            self.grab_set()
        except Exception:
            pass

    def _build(self):
        # ── Top bar: title on left, buttons on right ──────────
        topbar = ctk.CTkFrame(self, fg_color=PANEL, corner_radius=0)
        topbar.pack(fill="x", side="top")
        ctk.CTkLabel(topbar, text="Configure Connection",
                     font=ctk.CTkFont("Consolas", 17, weight="bold"),
                     text_color=TXT).pack(side="left", padx=24, pady=16)
        GhostButton(topbar, "Cancel",  command=self.destroy, width=100).pack(side="right", padx=8,  pady=14)
        AccentButton(topbar, "💾 Save", command=self._save,  width=120).pack(side="right", padx=4,  pady=14)
        ctk.CTkFrame(self, height=1, fg_color=BORD).pack(fill="x", side="top")
        ctk.CTkLabel(self, text="Credentials stored in  ~/.metaharvest/connections.json",
                     font=ctk.CTkFont("Consolas", 10), text_color=TXD
                     ).pack(anchor="w", padx=24, pady=(6, 0), side="top")
        sf = ctk.CTkScrollableFrame(self, fg_color="transparent")
        sf.pack(fill="both", expand=True, padx=16, side="top")
        self._sf = sf

        # Name
        self._row(sf, "Connection Name *", "name", self.conn.name, "My Oracle PROD")

        # Environment
        lf = ctk.CTkFrame(sf, fg_color="transparent")
        lf.pack(fill="x", padx=8, pady=(10, 0))
        ctk.CTkLabel(lf, text="Environment",
                     font=ctk.CTkFont("Consolas", 11), text_color=TXS).pack(anchor="w")
        self._env_var = ctk.StringVar(value=self.conn.environment or "dev")
        envf = ctk.CTkFrame(lf, fg_color="transparent")
        envf.pack(anchor="w", pady=4)
        for env in ENVIRONMENTS:
            rb = ctk.CTkRadioButton(envf, text=env["label"], variable=self._env_var,
                                     value=env["id"],
                                     text_color=env["color"], fg_color=env["color"],
                                     font=ctk.CTkFont("Consolas", 12))
            rb.pack(side="left", padx=8)

        # Source type
        stf = ctk.CTkFrame(sf, fg_color="transparent")
        stf.pack(fill="x", padx=8, pady=(10, 0))
        ctk.CTkLabel(stf, text="Source Type *",
                     font=ctk.CTkFont("Consolas", 11), text_color=TXS).pack(anchor="w")
        self._src_var = ctk.StringVar(value=self.conn.source_type or SOURCE_TYPES[0]["id"])
        self._src_om  = ctk.CTkOptionMenu(stf,
                           values=[s["id"] for s in SOURCE_TYPES],
                           variable=self._src_var,
                           command=self._on_src_change,
                           fg_color=CARD, button_color=BORD,
                           button_hover_color=HOVER, text_color=TXT,
                           font=ctk.CTkFont("Consolas", 12), width=360, height=36)
        self._src_om.pack(anchor="w", pady=4)

        self._dyn = ctk.CTkFrame(sf, fg_color="transparent")
        self._dyn.pack(fill="x")
        self._render_fields(self._src_var.get())

        # Notes
        self._row(sf, "Notes (optional)", "notes", self.conn.notes, "")

    def _on_src_change(self, val): self._render_fields(val)

    def _render_fields(self, src: str):
        for w in self._dyn.winfo_children(): w.destroy()
        for k in [k for k in list(self._entries) if k not in ("name","notes")]:
            del self._entries[k]
        f = self._dyn
        c = self.conn

        if src == "databricks":
            self._row(f, "Workspace Host *",  "host",      c.host,      "https://adb-xxx.azuredatabricks.net")
            self._row(f, "Access Token *",    "token",     c.token,     "dapi…", show="*")
            self._row(f, "HTTP Path",         "http_path", c.http_path, "/sql/1.0/warehouses/…")
            self._row(f, "Default Catalog",   "catalog",   c.catalog,   "main")

        elif src == "adls":
            self._row(f, "Storage Account *",  "account_name", c.account_name, "mystorageaccount")
            self._row(f, "Account Key",         "account_key",  c.account_key,  "", show="*")
            self._sep(f, "─── or Service Principal ───")
            self._row(f, "Tenant ID",    "tenant_id",    c.tenant_id,    "xxxxxxxx-xxxx-xxxx-xxxx-xxxxxxxxxxxx")
            self._row(f, "Client ID",    "client_id",    c.client_id,    "")
            self._row(f, "Client Secret","client_secret",c.client_secret,"", show="*")
            self._sep(f, "─── Path Scoping ───")
            self._row(f, "Container (blank=all)", "container", c.container, "raw-data")
            self._row(f, "Root Path (strip prefix)", "root_path", c.root_path,
                      "raw/finance/2024  ← scan starts here")
            self._row(f, "Path Depth (schema levels)", "path_depth",
                      str(c.path_depth or 2), "2")

        elif src in ("synapse", "azure_sql", "sqlserver"):
            self._row(f, "Server *",    "host",     c.host,     "server.database.windows.net")
            self._row(f, "Database *",  "database", c.database, "DataWarehouse")
            self._row(f, "Username *",  "username", c.username, "")
            self._row(f, "Password *",  "password", c.password, "", show="*")
            self._row(f, "ODBC Driver", "odbc_driver", c.odbc_driver,
                      "ODBC Driver 18 for SQL Server")

        elif src == "oracle":
            self._row(f, "Host *",          "host",         c.host,         "oracle-server.internal")
            self._row(f, "Port",            "port",         str(c.port or 1521), "1521")
            self._row(f, "Service Name",    "service_name", c.service_name, "ORCL  (or SID below)")
            self._row(f, "SID",             "sid",          c.sid,          "ORCL")
            self._row(f, "Username *",      "username",     c.username,     "")
            self._row(f, "Password *",      "password",     c.password,     "", show="*")
            self._row(f, "Schema Filter",   "schema",       c.schema,       "HR  (blank=all accessible)")

        elif src == "postgresql":
            self._row(f, "Host *",    "host",     c.host,     "pg-server.internal")
            self._row(f, "Port",      "port",     str(c.port or 5432), "5432")
            self._row(f, "Database *","database", c.database, "postgres")
            self._row(f, "Username *","username", c.username, "")
            self._row(f, "Password *","password", c.password, "", show="*")
            self._row(f, "Schema",    "schema",   c.schema,   "public  (blank=all)")

        elif src == "mysql":
            self._row(f, "Host *",    "host",     c.host,     "mysql-server.internal")
            self._row(f, "Port",      "port",     str(c.port or 3306), "3306")
            self._row(f, "Database *","database", c.database, "mydb")
            self._row(f, "Username *","username", c.username, "")
            self._row(f, "Password *","password", c.password, "", show="*")

        elif src == "db2":
            self._row(f, "Host *",     "host",     c.host,     "db2-server.internal")
            self._row(f, "Port",       "port",     str(c.port or 50000), "50000")
            self._row(f, "Database *", "database", c.database, "BLUDB")
            self._row(f, "Username *", "username", c.username, "")
            self._row(f, "Password *", "password", c.password, "", show="*")
            self._row(f, "Schema",     "schema",   c.schema,   "DB2ADMIN")

        elif src == "teradata":
            self._row(f, "Host *",     "host",     c.host,     "td-server.internal")
            self._row(f, "Database",   "database", c.database, "leave blank for all")
            self._row(f, "Username *", "username", c.username, "")
            self._row(f, "Password *", "password", c.password, "", show="*")

        elif src == "snowflake":
            self._row(f, "Account *",   "account",   c.account,   "xy12345.us-east-1")
            self._row(f, "Username *",  "username",  c.username,  "")
            self._row(f, "Password *",  "password",  c.password,  "", show="*")
            self._row(f, "Warehouse",   "warehouse", c.warehouse, "COMPUTE_WH")
            self._row(f, "Database",    "database",  c.database,  "")
            self._row(f, "Schema",      "schema",    c.schema,    "")
            self._row(f, "Role",        "role",      c.role,      "SYSADMIN")

        elif src in ("parquet", "csv"):
            self._browse_row(f, "Folder Path *", "folder_path", c.folder_path)
            if src == "csv":
                self._row(f, "Delimiter", "delimiter", c.delimiter or ",", ",  or  |  or  \\t")

        elif src == "hive":
            self._row(f, "Hive Host *", "host",     c.host,     "hive-server.internal")
            self._row(f, "Port",        "port",     str(c.port or 10000), "10000")
            self._row(f, "Username",    "username", c.username, "hive")

        elif src == "impala":
            self._row(f, "Impala Host *","host",     c.host,     "impala-server.internal")
            self._row(f, "Port",         "port",     str(c.port or 21050), "21050")
            self._row(f, "Username",     "username", c.username, "impala")

        elif src == "odbc":
            self._sep(f, "─── Use DSN (preferred) ───")
            self._row(f, "DSN Name",    "dsn",         c.dsn,         "MyDatasource")
            self._sep(f, "─── or manual connection string ───")
            self._row(f, "ODBC Driver", "odbc_driver", c.odbc_driver, "e.g. {IBM DB2 ODBC Driver}")
            self._row(f, "Host",        "host",        c.host,        "")
            self._row(f, "Port",        "port",        str(c.port or 0), "")
            self._row(f, "Database",    "database",    c.database,    "")
            self._row(f, "Username",    "username",    c.username,    "")
            self._row(f, "Password",    "password",    c.password,    "", show="*")
            self._row(f, "Extra params","extra_params",c.extra_params,
                      "TrustServerCertificate=yes;Encrypt=no")

    def _sep(self, parent, text):
        ctk.CTkLabel(parent, text=text,
                     font=ctk.CTkFont("Consolas", 10), text_color=TXD
                     ).pack(anchor="w", padx=8, pady=(10, 2))

    def _row(self, parent, label, key, value="", placeholder="", show=""):
        f = ctk.CTkFrame(parent, fg_color="transparent")
        f.pack(fill="x", padx=8, pady=(8, 0))
        ctk.CTkLabel(f, text=label, font=ctk.CTkFont("Consolas", 11),
                     text_color=TXS).pack(anchor="w")
        e = DataEntry(f, placeholder, show=show, width=490)
        if value: e.insert(0, str(value))
        e.pack(anchor="w", pady=(4, 0))
        self._entries[key] = e

    def _browse_row(self, parent, label, key, value=""):
        f = ctk.CTkFrame(parent, fg_color="transparent")
        f.pack(fill="x", padx=8, pady=(8, 0))
        ctk.CTkLabel(f, text=label, font=ctk.CTkFont("Consolas", 11),
                     text_color=TXS).pack(anchor="w")
        rf = ctk.CTkFrame(f, fg_color="transparent")
        rf.pack(fill="x")
        e = DataEntry(rf, "C:\\Data\\files", width=400)
        if value: e.insert(0, value)
        e.pack(side="left", pady=4)
        GhostButton(rf, "Browse…", command=lambda: self._browse(e), width=90).pack(side="left", padx=8)
        self._entries[key] = e

    def _browse(self, e):
        p = filedialog.askdirectory()
        if p: e.delete(0, "end"); e.insert(0, p)

    def _save(self):
        name_e = self._entries.get("name")
        if not name_e or not name_e.get().strip():
            messagebox.showerror("Validation", "Connection name is required."); return
        c = self.conn
        c.name        = self._entries["name"].get().strip()
        c.source_type = self._src_var.get()
        c.environment = self._env_var.get()
        for k, widget in self._entries.items():
            if k in ("name",): continue
            if isinstance(widget, ctk.CTkEntry):
                val = widget.get().strip()
                if hasattr(c, k):
                    if k in ("port", "hive_port", "path_depth"):
                        try: setattr(c, k, int(val))
                        except: setattr(c, k, 0)
                    else:
                        setattr(c, k, val)
        notes_e = self._entries.get("notes")
        if notes_e: c.notes = notes_e.get().strip()
        storage.upsert_connection(c)
        if self.on_save: self.on_save()
        self.destroy()


# ══════════════════════════════════════════════════════════════
#  PAGE: Scanner
# ══════════════════════════════════════════════════════════════
class ScannerPage(ctk.CTkFrame):
    def __init__(self, master, app, **k):
        super().__init__(master, fg_color=BG, **k)
        self.app = app
        self._connector = None
        self._build()

    def _build(self):
        ctk.CTkLabel(self, text="Metadata Scanner",
                     font=ctk.CTkFont("Consolas", 20, weight="bold"),
                     text_color=TXT).pack(anchor="w", padx=32, pady=(24, 4))
        ctk.CTkFrame(self, height=1, fg_color=BORD).pack(fill="x", padx=32, pady=(4, 16))

        content = ctk.CTkFrame(self, fg_color="transparent")
        content.pack(fill="both", expand=True, padx=32)
        content.columnconfigure(0, weight=1)
        content.columnconfigure(1, weight=2)

        # Left config
        left = ctk.CTkFrame(content, fg_color="transparent")
        left.grid(row=0, column=0, sticky="nsew", padx=(0, 16))

        # Environment filter
        SectionHeader(left, "ENVIRONMENT FILTER").pack(anchor="w", pady=(0, 6))
        self._env_filter = ctk.StringVar(value="all")
        ef = ctk.CTkFrame(left, fg_color="transparent")
        ef.pack(anchor="w", pady=(0, 12))
        ctk.CTkRadioButton(ef, text="All", variable=self._env_filter, value="all",
                            font=ctk.CTkFont("Consolas", 11), text_color=TXS,
                            command=self._refresh_conn_list).pack(side="left", padx=(0, 8))
        for env in ENVIRONMENTS:
            ctk.CTkRadioButton(ef, text=env["label"], variable=self._env_filter,
                                value=env["id"], text_color=env["color"],
                                font=ctk.CTkFont("Consolas", 11),
                                fg_color=env["color"],
                                command=self._refresh_conn_list).pack(side="left", padx=4)

        SectionHeader(left, "SELECT SOURCE").pack(anchor="w", pady=(0, 6))
        self._conn_var = ctk.StringVar()
        self._conn_map: Dict[str, ConnectionConfig] = {}
        self._conn_om = ctk.CTkOptionMenu(left, values=["— no connections —"],
                                           variable=self._conn_var,
                                           fg_color=CARD, button_color=BORD,
                                           button_hover_color=HOVER, text_color=TXT,
                                           font=ctk.CTkFont("Consolas", 12), width=300, height=36)
        self._conn_om.pack(anchor="w", pady=(0, 14))
        self._refresh_conn_list()

        SectionHeader(left, "FILTER PATTERN").pack(anchor="w", pady=(0, 6))
        self._filter_e = DataEntry(left, "* or sales_* or *_prod", width=300)
        self._filter_e.insert(0, "*")
        self._filter_e.pack(anchor="w", pady=(0, 14))

        SectionHeader(left, "OPTIONS").pack(anchor="w", pady=(0, 6))
        self._cols_var = ctk.BooleanVar(value=True)
        ctk.CTkCheckBox(left, text="Scan column metadata", variable=self._cols_var,
                        font=ctk.CTkFont("Consolas", 12), text_color=TXS,
                        fg_color=ACCN).pack(anchor="w", pady=3)
        self._hive_var = ctk.BooleanVar(value=True)
        ctk.CTkCheckBox(left, text="Auto-fix Hive 18-char names", variable=self._hive_var,
                        font=ctk.CTkFont("Consolas", 12), text_color=TXS,
                        fg_color=AMBN).pack(anchor="w", pady=3)

        bf = ctk.CTkFrame(left, fg_color="transparent")
        bf.pack(anchor="w", pady=18)
        self._scan_btn = AccentButton(bf, "▶  Start Scan", command=self._start, width=150)
        self._scan_btn.pack(side="left", padx=(0, 8))
        self._cancel_btn = GhostButton(bf, "◼ Cancel", command=self._cancel, width=100, color=ERR)
        self._cancel_btn.pack(side="left")
        self._cancel_btn.configure(state="disabled")

        # Right status
        right = ctk.CTkFrame(content, fg_color="transparent")
        right.grid(row=0, column=1, sticky="nsew")
        SectionHeader(right, "PROGRESS").pack(anchor="w", pady=(0, 6))
        self._prog = ProgressCard(right, "Ready.")
        self._prog.pack(fill="x", pady=(0, 14))
        SectionHeader(right, "LIVE OUTPUT").pack(anchor="w", pady=(0, 6))
        self._log = make_log_box(right, height=400)
        self._log.pack(fill="both", expand=True)
        log_append(self._log, "⬡  MetaHarvest Scanner ready.")

    def _refresh_conn_list(self):
        env_f = self._env_filter.get() if hasattr(self, "_env_filter") else "all"
        conns = storage.load_connections()
        if env_f != "all":
            conns = [c for c in conns if c.environment == env_f]
        self._conn_map = {c.name: c for c in conns}
        names = list(self._conn_map.keys()) or ["— no connections —"]
        if hasattr(self, "_conn_om"):
            self._conn_om.configure(values=names)
            self._conn_var.set(names[0])

    def _start(self):
        self._refresh_conn_list()
        cname = self._conn_var.get()
        if cname not in self._conn_map:
            messagebox.showerror("Scanner", "Select a connection first."); return
        conn = self._conn_map[cname]
        from app.connectors.connectors import get_connector
        self._connector = get_connector(conn)
        self._scan_btn.configure(state="disabled")
        self._cancel_btn.configure(state="normal")
        log_append(self._log, f"Starting scan: '{cname}' [{conn.source_type}] [{conn.environment.upper()}]")

        def run():
            try:
                result = ScanResult(connection_id=conn.id, connection_name=conn.name,
                                    source_type=conn.source_type, environment=conn.environment)
                def pcb(msg, cur, tot):
                    frac = cur / max(tot, 1)
                    self.after(0, lambda: self._prog.update(msg, frac, f"{cur}/{tot}"))
                    self.after(0, lambda: log_append(self._log, msg))
                tables = self._connector.scan(progress_cb=pcb,
                                               filter_pattern=self._filter_e.get().strip() or "*")
                result.tables     = tables
                result.status     = "complete"
                result.finished_at= datetime.now().isoformat()
                storage.save_scan_summary(result)
                self.app.current_scan = result
                self.after(0, lambda: self._prog.update(
                    f"✔  {len(tables)} objects found", 1.0,
                    f"{result.column_count} columns  |  {result.duration_sec:.1f}s"))
                self.after(0, lambda: log_append(self._log,
                    f"✔  Scan complete: {len(tables)} tables, {result.column_count} columns"))
                self.after(0, lambda: self.app.topbar.set_status(
                    f"✔  Scan complete — {len(tables)} objects", SUCC))
            except Exception as e:
                self.after(0, lambda: log_append(self._log, f"✖  {e}"))
                self.after(0, lambda: self._prog.update(f"✖  {e}", 0))
                self.after(0, lambda: self.app.topbar.set_status("✖  Scan failed", ERR))
            finally:
                self.after(0, lambda: self._scan_btn.configure(state="normal"))
                self.after(0, lambda: self._cancel_btn.configure(state="disabled"))
        threading.Thread(target=run, daemon=True).start()

    def _cancel(self):
        if self._connector: self._connector.cancel()
        log_append(self._log, "⚠  Cancelled by user.")
        self._cancel_btn.configure(state="disabled")


# ══════════════════════════════════════════════════════════════
#  PAGE: Metadata Preview
# ══════════════════════════════════════════════════════════════
class PreviewPage(ctk.CTkFrame):
    def __init__(self, master, app, **k):
        super().__init__(master, fg_color=BG, **k)
        self.app = app
        self._build()

    def _build(self):
        hdr = ctk.CTkFrame(self, fg_color="transparent")
        hdr.pack(fill="x", padx=32, pady=24)
        ctk.CTkLabel(hdr, text="Metadata Preview & Editing",
                     font=ctk.CTkFont("Consolas", 20, weight="bold"),
                     text_color=TXT).pack(side="left")
        GhostButton(hdr, "⟳ Refresh", command=self.refresh, width=110).pack(side="right")
        ctk.CTkFrame(self, height=1, fg_color=BORD).pack(fill="x", padx=32, pady=(0, 16))

        panes = ctk.CTkFrame(self, fg_color="transparent")
        panes.pack(fill="both", expand=True, padx=32, pady=(0, 16))
        panes.columnconfigure(0, weight=2)
        panes.columnconfigure(1, weight=3)
        panes.rowconfigure(0, weight=1)

        # Left list
        left = ctk.CTkFrame(panes, fg_color="transparent")
        left.grid(row=0, column=0, sticky="nsew", padx=(0, 12))
        SectionHeader(left, "SCANNED OBJECTS").pack(anchor="w", pady=(0, 6))
        self._search_var = ctk.StringVar()
        self._search_var.trace_add("write", lambda *_: self._render_list())
        se = DataEntry(left, "Filter tables…", width=300)
        se.configure(textvariable=self._search_var)
        se.pack(anchor="w", pady=(0, 8))

        # Select all / none
        sf2 = ctk.CTkFrame(left, fg_color="transparent")
        sf2.pack(anchor="w", pady=(0, 4))
        GhostButton(sf2, "✔ All",  command=self._select_all,  width=70, color=TEAL).pack(side="left", padx=(0,4))
        GhostButton(sf2, "✕ None", command=self._select_none, width=70, color=ERR).pack(side="left")

        self._tbl_list = ctk.CTkScrollableFrame(left, fg_color="transparent", corner_radius=0)
        self._tbl_list.pack(fill="both", expand=True)
        self._render_list()

        # Right detail
        right = ctk.CTkFrame(panes, fg_color="transparent")
        right.grid(row=0, column=1, sticky="nsew")
        SectionHeader(right, "TABLE DETAILS").pack(anchor="w", pady=(0, 6))
        self._detail = ctk.CTkScrollableFrame(right, fg_color=CARD, corner_radius=10,
                                               border_color=BORD, border_width=1)
        self._detail.pack(fill="both", expand=True)
        ctk.CTkLabel(self._detail, text="Select a table to view details.",
                     font=ctk.CTkFont("Consolas", 12), text_color=TXD
                     ).pack(expand=True, pady=60)

    def _render_list(self):
        for w in self._tbl_list.winfo_children(): w.destroy()
        scan = self.app.current_scan
        if not scan:
            ctk.CTkLabel(self._tbl_list, text="No scan results yet.",
                         font=ctk.CTkFont("Consolas", 12), text_color=TXD).pack(pady=40)
            return
        q = self._search_var.get().lower()
        tables = [t for t in scan.tables if q in t.display_name.lower()] if q else scan.tables
        for t in tables:
            row  = ctk.CTkFrame(self._tbl_list, fg_color="transparent")
            row.pack(fill="x", pady=2)
            inner = ctk.CTkFrame(row, fg_color=CARD, corner_radius=8,
                                  border_color=BORD, border_width=1)
            inner.pack(fill="x")
            var = ctk.BooleanVar(value=t.selected)
            ctk.CTkCheckBox(inner, text="", variable=var, width=20, fg_color=ACCN,
                             hover_color=C["accent_dim"],
                             command=lambda t=t, v=var: setattr(t, "selected", v.get())
                             ).pack(side="left", padx=8, pady=8)
            ctk.CTkLabel(inner, text=t.table_name,
                         font=ctk.CTkFont("Consolas", 12, weight="bold"),
                         text_color=TXT, anchor="w").pack(side="left", padx=4)
            ctk.CTkLabel(inner, text=f"{t.col_count} cols",
                         font=ctk.CTkFont("Consolas", 10), text_color=TXD
                         ).pack(side="right", padx=10)
            ctk.CTkLabel(inner, text=t.object_type,
                         font=ctk.CTkFont("Consolas", 10),
                         text_color=ACCN if t.object_type == "Table" else PURP
                         ).pack(side="right", padx=4)
            inner.bind("<Button-1>", lambda e, tbl=t: self._show_detail(tbl))
            for child in inner.winfo_children():
                child.bind("<Button-1>", lambda e, tbl=t: self._show_detail(tbl))

    def _select_all(self):
        if self.app.current_scan:
            for t in self.app.current_scan.tables: t.selected = True
            self._render_list()

    def _select_none(self):
        if self.app.current_scan:
            for t in self.app.current_scan.tables: t.selected = False
            self._render_list()

    def _show_detail(self, t: TableMeta):
        for w in self._detail.winfo_children(): w.destroy()
        # Header
        ih = ctk.CTkFrame(self._detail, fg_color=HOVER, corner_radius=8)
        ih.pack(fill="x", padx=14, pady=(14, 8))
        hf = ctk.CTkFrame(ih, fg_color="transparent")
        hf.pack(fill="x", padx=14, pady=10)
        ctk.CTkLabel(hf, text=t.table_name,
                     font=ctk.CTkFont("Consolas", 15, weight="bold"),
                     text_color=ACCN).pack(side="left")
        EnvBadge(hf, t.environment).pack(side="right")
        ctk.CTkLabel(ih, text=t.full_path,
                     font=ctk.CTkFont("Consolas", 10), text_color=TXD
                     ).pack(anchor="w", padx=14, pady=(0, 10))
        # Meta chips
        meta = [("Type", t.object_type), ("DB", t.database), ("Schema", t.schema),
                ("Cols", str(t.col_count)),
                ("Rows", f"{t.row_count:,}" if t.row_count else "—"),
                ("Size", f"{t.size_bytes//1024:,}KB" if t.size_bytes else "—")]
        mr = ctk.CTkFrame(self._detail, fg_color="transparent")
        mr.pack(fill="x", padx=14, pady=(0, 10))
        for k, v in meta:
            mf = ctk.CTkFrame(mr, fg_color=CARD, corner_radius=6)
            mf.pack(side="left", padx=(0, 6))
            ctk.CTkLabel(mf, text=k, font=ctk.CTkFont("Consolas", 9),
                         text_color=TXD).pack(anchor="w", padx=8, pady=(4, 0))
            ctk.CTkLabel(mf, text=v, font=ctk.CTkFont("Consolas", 12, weight="bold"),
                         text_color=TXT).pack(anchor="w", padx=8, pady=(0, 6))
        # Collibra name
        SectionHeader(self._detail, "COLLIBRA TARGET NAME").pack(anchor="w", padx=14, pady=(4, 4))
        ne = DataEntry(self._detail, "Collibra name", width=420)
        ne.insert(0, t.collibra_name or t.table_name)
        ne.pack(anchor="w", padx=14, pady=(0, 8))
        AccentButton(self._detail, "Apply", color=TEAL, width=100,
                     command=lambda: self._apply_name(t, ne.get())).pack(anchor="w", padx=14, pady=(0, 12))
        # Hive warning
        if t.source_type == "hive" and len(t.table_name) > 18:
            wf = ctk.CTkFrame(self._detail, fg_color="#2A1800", corner_radius=8,
                               border_color=AMBN, border_width=1)
            wf.pack(fill="x", padx=14, pady=(0, 10))
            ctk.CTkLabel(wf,
                         text=f"⚠  Hive 18-char limit: '{t.table_name}' → '{t.properties.get('hive_safe_name','')}'",
                         font=ctk.CTkFont("Consolas", 11), text_color=AMBN).pack(padx=10, pady=8)
        # Columns
        SectionHeader(self._detail, f"COLUMNS  ({t.col_count})").pack(anchor="w", padx=14, pady=(4, 4))
        ch = ctk.CTkFrame(self._detail, fg_color=HOVER, corner_radius=6)
        ch.pack(fill="x", padx=14, pady=(0, 4))
        for lbl, w in [("#",30),("Column Name",180),("Type",130),("Nullable",80),("Collibra Name",180)]:
            ctk.CTkLabel(ch, text=lbl, font=ctk.CTkFont("Consolas", 10, weight="bold"),
                         text_color=TXD, width=w, anchor="w").pack(side="left", padx=6, pady=7)
        for col in t.columns:
            cr = ctk.CTkFrame(self._detail, fg_color="transparent")
            cr.pack(fill="x", padx=14)
            ctk.CTkFrame(self._detail, height=1, fg_color=BORD).pack(fill="x", padx=14)
            for v, w, c in [
                (str(col.ordinal+1), 30, TXD), (col.name, 180, TXT),
                (col.data_type, 130, PURP),
                ("NULL" if col.nullable else "NOT NULL", 80, TXD),
                (col.collibra_name, 180, ACCN),
            ]:
                ctk.CTkLabel(cr, text=v, font=ctk.CTkFont("Consolas", 11),
                             text_color=c, width=w, anchor="w").pack(side="left", padx=6, pady=7)

    def _apply_name(self, t, name):
        t.collibra_name = name.strip()
        self.app.topbar.set_status(f"✔  Renamed: {name}", SUCC)

    def refresh(self): self._render_list()


# ══════════════════════════════════════════════════════════════
#  PAGE: Data Quality
# ══════════════════════════════════════════════════════════════
class DQPage(ctk.CTkFrame):
    def __init__(self, master, app, **k):
        super().__init__(master, fg_color=BG, **k)
        self.app = app
        self._dq_result: Optional[DQRunResult] = None
        self._engine    = None
        self._build()

    def _build(self):
        ctk.CTkLabel(self, text="Data Quality Engine",
                     font=ctk.CTkFont("Consolas", 20, weight="bold"),
                     text_color=TXT).pack(anchor="w", padx=32, pady=(24, 4))
        ctk.CTkLabel(self,
                     text="Compare source schema against a target scan or Collibra catalog to detect dropped fields, type mismatches, and row variance.",
                     font=ctk.CTkFont("Consolas", 11), text_color=TXD
                     ).pack(anchor="w", padx=32, pady=(0, 8))
        ctk.CTkFrame(self, height=1, fg_color=BORD).pack(fill="x", padx=32, pady=(0, 16))

        scroll = ctk.CTkScrollableFrame(self, fg_color="transparent")
        scroll.pack(fill="both", expand=True, padx=32, pady=(0, 16))

        # ── Configuration ─────────────────────────────────────
        cfg = Card(scroll)
        cfg.pack(fill="x", pady=(0, 16))
        ctk.CTkLabel(cfg, text="DQ Run Configuration",
                     font=ctk.CTkFont("Consolas", 14, weight="bold"),
                     text_color=TXT).pack(anchor="w", padx=20, pady=(16, 8))

        r1 = ctk.CTkFrame(cfg, fg_color="transparent")
        r1.pack(fill="x", padx=20, pady=(0, 8))
        ctk.CTkLabel(r1, text="Source scan:", font=ctk.CTkFont("Consolas", 11),
                     text_color=TXS, width=160, anchor="w").pack(side="left")
        scan = self.app.current_scan
        src_label = (f"{scan.connection_name} [{scan.environment.upper()}]  "
                     f"{scan.table_count} tables" if scan else "No scan loaded — run Scanner first")
        ctk.CTkLabel(r1, text=src_label, font=ctk.CTkFont("Consolas", 11),
                     text_color=ACCN if scan else ERR).pack(side="left")

        r2 = ctk.CTkFrame(cfg, fg_color="transparent")
        r2.pack(fill="x", padx=20, pady=(0, 8))
        ctk.CTkLabel(r2, text="Compare against:",
                     font=ctk.CTkFont("Consolas", 11), text_color=TXS, width=160, anchor="w"
                     ).pack(side="left")
        self._compare_mode = ctk.StringVar(value="scan")
        for val, label in [("scan","Second scan (cross-env)"), ("collibra","Collibra catalog")]:
            ctk.CTkRadioButton(r2, text=label, variable=self._compare_mode, value=val,
                                font=ctk.CTkFont("Consolas", 11), text_color=TXS,
                                fg_color=ACCN).pack(side="left", padx=8)

        # Second scan connection selector
        self._tgt_conn_frame = ctk.CTkFrame(cfg, fg_color="transparent")
        self._tgt_conn_frame.pack(fill="x", padx=20, pady=(0, 4))
        ctk.CTkLabel(self._tgt_conn_frame, text="Target connection:",
                     font=ctk.CTkFont("Consolas", 11), text_color=TXS, width=160, anchor="w"
                     ).pack(side="left")
        self._tgt_conn_var = ctk.StringVar()
        conns = {c.name: c for c in storage.load_connections()}
        self._tgt_conn_om = ctk.CTkOptionMenu(
            self._tgt_conn_frame,
            values=list(conns.keys()) or ["—"],
            variable=self._tgt_conn_var,
            fg_color=CARD, button_color=BORD, button_hover_color=HOVER,
            text_color=TXT, font=ctk.CTkFont("Consolas", 12), width=300, height=34)
        self._tgt_conn_om.pack(side="left", padx=8)

        # Checks selector
        SectionHeader(cfg, "CHECKS TO RUN").pack(anchor="w", padx=20, pady=(8, 6))
        chk_f = ctk.CTkFrame(cfg, fg_color="transparent")
        chk_f.pack(fill="x", padx=20, pady=(0, 16))
        self._check_vars: Dict[str, ctk.BooleanVar] = {}
        from app.config import DQ_CHECKS
        for i, chk in enumerate(DQ_CHECKS):
            var = ctk.BooleanVar(value=True)
            self._check_vars[chk["id"]] = var
            ctk.CTkCheckBox(chk_f, text=f"{chk['label']}  ({chk['category']})",
                             variable=var, font=ctk.CTkFont("Consolas", 11), text_color=TXS,
                             fg_color=ACCN).pack(anchor="w", pady=2)

        bf = ctk.CTkFrame(cfg, fg_color="transparent")
        bf.pack(anchor="w", padx=20, pady=(0, 16))
        self._run_btn = AccentButton(bf, "⚑  Run DQ Checks", command=self._run, width=180, color=AMBN)
        self._run_btn.pack(side="left")
        self._cancel_btn = GhostButton(bf, "◼ Cancel", command=self._cancel, width=100, color=ERR)
        self._cancel_btn.pack(side="left", padx=8)
        self._cancel_btn.configure(state="disabled")

        # Progress
        self._prog = ProgressCard(scroll, "Ready.")
        self._prog.pack(fill="x", pady=(0, 16))

        # Results
        SectionHeader(scroll, "DQ RESULTS").pack(anchor="w", pady=(0, 8))
        self._results_frame = ctk.CTkScrollableFrame(scroll, fg_color=CARD, corner_radius=10,
                                                      border_color=BORD, border_width=1,
                                                      height=380)
        self._results_frame.pack(fill="x", pady=(0, 16))
        ctk.CTkLabel(self._results_frame, text="Run DQ checks to see results.",
                     font=ctk.CTkFont("Consolas", 12), text_color=TXD).pack(pady=40)

        # Export
        self._export_btn = GhostButton(scroll, "⬇ Export to Excel",
                                        command=self._export, width=180, color=TEAL)
        self._export_btn.pack(anchor="w", pady=(0, 24))
        self._export_btn.configure(state="disabled")

        # DQ Log
        SectionHeader(scroll, "DQ LOG").pack(anchor="w", pady=(0, 6))
        self._log = make_log_box(scroll, height=180)
        self._log.pack(fill="x", pady=(0, 24))

    def _run(self):
        scan = self.app.current_scan
        if not scan:
            messagebox.showerror("DQ", "Run a scan first."); return
        from app.dq_engine import DQEngine
        self._engine = DQEngine()
        checks = [k for k, v in self._check_vars.items() if v.get()]
        self._run_btn.configure(state="disabled")
        self._cancel_btn.configure(state="normal")
        log_append(self._log, f"Starting DQ run on {scan.table_count} tables…")
        log_append(self._log, f"Checks: {', '.join(checks)}")

        tgt_scan = None
        mode = self._compare_mode.get()
        if mode == "scan":
            tgt_name = self._tgt_conn_var.get()
            conns    = {c.name: c for c in storage.load_connections()}
            tgt_conn = conns.get(tgt_name)
            if tgt_conn:
                log_append(self._log, f"Scanning target: {tgt_name}…")
                from app.connectors.connectors import get_connector
                try:
                    tgt_scan = ScanResult(
                        connection_id=tgt_conn.id,
                        connection_name=tgt_conn.name,
                        source_type=tgt_conn.source_type,
                        environment=tgt_conn.environment,
                    )
                    tgt_scan.tables = get_connector(tgt_conn).scan()
                except Exception as e:
                    log_append(self._log, f"⚠  Could not scan target: {e} — running structural only")
                    tgt_scan = None

        def run():
            try:
                def pcb(msg, cur, tot):
                    frac = cur / max(tot, 1)
                    self.after(0, lambda: self._prog.update(msg, frac, f"{cur}/{tot}"))
                    self.after(0, lambda: log_append(self._log, msg))
                result = self._engine.run(
                    source_scan=scan, target_scan=tgt_scan,
                    progress_cb=pcb, checks=checks if checks else None)
                self._dq_result = result
                storage.save_dq_summary(result)
                self.after(0, self._render_results)
                self.after(0, lambda: self._export_btn.configure(state="normal"))
                self.after(0, lambda: self.app.topbar.set_status(
                    f"DQ: {result.summary_pass}✔ {result.summary_warn}⚠ {result.summary_fail}✖",
                    SUCC if result.status == "ok" else AMBN if result.status == "warn" else ERR))
            except Exception as e:
                self.after(0, lambda: log_append(self._log, f"✖  DQ error: {e}"))
            finally:
                self.after(0, lambda: self._run_btn.configure(state="normal"))
                self.after(0, lambda: self._cancel_btn.configure(state="disabled"))

        threading.Thread(target=run, daemon=True).start()

    def _cancel(self):
        if self._engine: self._engine.cancel()
        self._cancel_btn.configure(state="disabled")
        log_append(self._log, "⚠  DQ cancelled.")

    def _render_results(self):
        for w in self._results_frame.winfo_children(): w.destroy()
        r = self._dq_result
        if not r: return

        # Summary bar
        sb = ctk.CTkFrame(self._results_frame, fg_color=HOVER, corner_radius=8)
        sb.pack(fill="x", padx=12, pady=(12, 8))
        for val, lbl, color in [
            (str(r.summary_pass), "PASS", SUCC),
            (str(r.summary_warn), "WARN", AMBN),
            (str(r.summary_fail), "FAIL", ERR),
            (str(r.total_dropped_cols), "Dropped Cols", ERR),
            (str(r.total_type_issues), "Type Issues", AMBN),
        ]:
            f = ctk.CTkFrame(sb, fg_color="transparent")
            f.pack(side="left", padx=20, pady=10)
            ctk.CTkLabel(f, text=val,
                         font=ctk.CTkFont("Consolas", 22, weight="bold"),
                         text_color=color).pack()
            ctk.CTkLabel(f, text=lbl,
                         font=ctk.CTkFont("Consolas", 10), text_color=TXD).pack()

        # Column headers
        headers = ["Table", "Status", "Src Cols", "Tgt Cols", "Row Var%", "Dropped", "Type Issues"]
        widths  = [200, 80, 80, 80, 90, 80, 100]
        hrow = ctk.CTkFrame(self._results_frame, fg_color=CARD, corner_radius=6)
        hrow.pack(fill="x", padx=12, pady=(0, 4))
        for h, w in zip(headers, widths):
            ctk.CTkLabel(hrow, text=h.upper(), font=ctk.CTkFont("Consolas", 10, weight="bold"),
                         text_color=TXD, width=w, anchor="w").pack(side="left", padx=6, pady=7)

        STATUS_COLORS = {"ok": SUCC, "warn": AMBN, "fail": ERR, "pending": TXD}
        for t in r.tables:
            row = ctk.CTkFrame(self._results_frame, fg_color="transparent")
            row.pack(fill="x", padx=12)
            ctk.CTkFrame(self._results_frame, height=1, fg_color=BORD).pack(fill="x", padx=12)
            sc = STATUS_COLORS.get(t.status, TXD)
            dc = len(t.dropped_cols)
            tc = len(t.type_mismatches)
            for v, w, c in zip(
                [t.table_name, t.status.upper(),
                 str(t.source_col_count), str(t.target_col_count),
                 f"{t.row_variance_pct:.1f}%" if t.row_variance_pct is not None else "—",
                 str(dc) if dc else "—",
                 str(tc) if tc else "—"],
                widths,
                [TXT, sc, TXS, TXS, AMBN if t.row_variance_pct and t.row_variance_pct > 5 else TXD,
                 ERR if dc else TXD, AMBN if tc else TXD]
            ):
                ctk.CTkLabel(row, text=v, font=ctk.CTkFont("Consolas", 11),
                             text_color=c, width=w, anchor="w").pack(side="left", padx=6, pady=9)

            # Expandable detail — dropped/new columns
            issues = t.dropped_cols + t.new_cols + [f"type: {m.column_name}" for m in t.type_mismatches]
            if issues:
                df = ctk.CTkFrame(self._results_frame, fg_color=C["bg_input"], corner_radius=6)
                df.pack(fill="x", padx=24, pady=(0, 4))
                ctk.CTkLabel(df, text="  " + "  ·  ".join(issues[:8]),
                             font=ctk.CTkFont("Consolas", 10), text_color=AMBN
                             ).pack(anchor="w", padx=8, pady=6)

    def _export(self):
        if not self._dq_result:
            messagebox.showerror("Export", "No DQ results to export."); return
        path = filedialog.asksaveasfilename(
            defaultextension=".xlsx",
            filetypes=[("Excel", "*.xlsx")],
            initialfile=f"DQ_Report_{datetime.now().strftime('%Y%m%d_%H%M%S')}.xlsx")
        if not path: return
        from app.dq_engine import export_dq_report
        ok, msg = export_dq_report(self._dq_result, path)
        self.app.topbar.set_status(f"{'✔' if ok else '✖'}  {msg}", SUCC if ok else ERR)


# ══════════════════════════════════════════════════════════════
#  PAGE: Collibra Ingestion
# ══════════════════════════════════════════════════════════════
class CollibraPage(ctk.CTkFrame):
    def __init__(self, master, app, **k):
        super().__init__(master, fg_color=BG, **k)
        self.app = app
        self._communities: List[CollibraCommunity] = []
        self._domains:     List[CollibraDomain]    = []
        self._client       = None
        self._build()

    def _build(self):
        ctk.CTkLabel(self, text="Collibra Ingestion",
                     font=ctk.CTkFont("Consolas", 20, weight="bold"),
                     text_color=TXT).pack(anchor="w", padx=32, pady=(24, 4))
        ctk.CTkFrame(self, height=1, fg_color=BORD).pack(fill="x", padx=32, pady=(4, 16))

        scroll = ctk.CTkScrollableFrame(self, fg_color="transparent")
        scroll.pack(fill="both", expand=True, padx=32, pady=(0, 16))

        # Credentials
        cc = Card(scroll); cc.pack(fill="x", pady=(0, 16))
        ctk.CTkLabel(cc, text="Collibra Credentials",
                     font=ctk.CTkFont("Consolas", 14, weight="bold"),
                     text_color=TXT).pack(anchor="w", padx=20, pady=(16, 8))
        for label, key, attr, ph, show in [
            ("Collibra URL",  "coll_url",  "collibra_url",      "https://your-org.collibra.com", ""),
            ("Username",      "coll_user", "collibra_username",  "admin", ""),
            ("Password",      "coll_pass", "collibra_password",  "", "*"),
        ]:
            rf = ctk.CTkFrame(cc, fg_color="transparent")
            rf.pack(fill="x", padx=20, pady=(0, 6))
            ctk.CTkLabel(rf, text=label, font=ctk.CTkFont("Consolas", 11),
                         text_color=TXS, width=140, anchor="w").pack(side="left")
            e = DataEntry(rf, ph, show=show, width=460)
            e.insert(0, self.app.settings.get(attr, ""))
            e.pack(side="left")
            setattr(self, f"_{key}", e)
        bf = ctk.CTkFrame(cc, fg_color="transparent")
        bf.pack(anchor="w", padx=20, pady=(4, 16))
        AccentButton(bf, "⚡ Test", command=self._test, width=120, color=TEAL).pack(side="left")
        AccentButton(bf, "⟳ Load Communities", command=self._load_comms, width=170).pack(side="left", padx=8)
        self._coll_status = ctk.CTkLabel(bf, text="", font=ctk.CTkFont("Consolas", 11))
        self._coll_status.pack(side="left", padx=8)

        # Target
        tc = Card(scroll); tc.pack(fill="x", pady=(0, 16))
        ctk.CTkLabel(tc, text="Target Community & Domain",
                     font=ctk.CTkFont("Consolas", 14, weight="bold"),
                     text_color=TXT).pack(anchor="w", padx=20, pady=(16, 8))
        for label, attr in [("Community", "_comm_om"), ("Domain", "_dom_om")]:
            rf = ctk.CTkFrame(tc, fg_color="transparent")
            rf.pack(fill="x", padx=20, pady=(0, 8))
            ctk.CTkLabel(rf, text=label, font=ctk.CTkFont("Consolas", 11),
                         text_color=TXS, width=120, anchor="w").pack(side="left")
            var = ctk.StringVar()
            om  = ctk.CTkOptionMenu(rf, values=["— load first —"], variable=var,
                                     command=self._on_comm_select if label == "Community" else None,
                                     fg_color=CARD, button_color=BORD, button_hover_color=HOVER,
                                     text_color=TXT, font=ctk.CTkFont("Consolas", 12),
                                     width=360, height=36)
            om.pack(side="left", padx=8)
            setattr(self, attr, om)
            setattr(self, attr.replace("_om","_var"), var)
        GhostButton(tc, "+ New Domain", command=self._new_domain, width=130, color=TEAL
                    ).pack(anchor="w", padx=20, pady=(0, 16))

        # Options
        oc = Card(scroll); oc.pack(fill="x", pady=(0, 16))
        ctk.CTkLabel(oc, text="Ingestion Options",
                     font=ctk.CTkFont("Consolas", 14, weight="bold"),
                     text_color=TXT).pack(anchor="w", padx=20, pady=(16, 8))
        of = ctk.CTkFrame(oc, fg_color="transparent")
        of.pack(fill="x", padx=20, pady=(0, 16))
        self._ingest_cols = ctk.BooleanVar(value=True)
        self._block_on_dq = ctk.BooleanVar(value=True)
        for text, var in [
            ("Ingest column metadata", self._ingest_cols),
            ("Block ingestion if DQ run has failures (recommended)", self._block_on_dq),
        ]:
            ctk.CTkCheckBox(of, text=text, variable=var,
                             font=ctk.CTkFont("Consolas", 12), text_color=TXS,
                             fg_color=ACCN).pack(anchor="w", pady=3)

        # Summary
        self._summary_card = Card(scroll)
        self._summary_card.pack(fill="x", pady=(0, 16))
        self._render_summary()

        # DQ gate status
        self._dq_gate_lbl = ctk.CTkLabel(scroll, text="",
                                          font=ctk.CTkFont("Consolas", 12), text_color=AMBN)
        self._dq_gate_lbl.pack(anchor="w", pady=(0, 8))
        self._refresh_dq_gate()

        # Buttons
        bf2 = ctk.CTkFrame(scroll, fg_color="transparent")
        bf2.pack(fill="x", pady=(0, 12))
        self._ingest_btn = AccentButton(bf2, "⬆  Ingest to Collibra",
                                         command=self._run_ingestion,
                                         width=220, color=C["collibra"])
        self._ingest_btn.pack(side="left")
        self._cancel_ing = GhostButton(bf2, "◼ Cancel", command=self._cancel_ing_fn,
                                        width=100, color=ERR)
        self._cancel_ing.pack(side="left", padx=8)
        self._cancel_ing.configure(state="disabled")

        self._ingest_prog = ProgressCard(scroll, "Waiting…")
        self._ingest_prog.pack(fill="x", pady=(0, 12))
        SectionHeader(scroll, "INGESTION LOG").pack(anchor="w", pady=(0, 6))
        self._ingest_log = make_log_box(scroll, height=200)
        self._ingest_log.pack(fill="x", pady=(0, 24))

    def _render_summary(self):
        for w in self._summary_card.winfo_children(): w.destroy()
        ctk.CTkLabel(self._summary_card, text="Ingestion Summary",
                     font=ctk.CTkFont("Consolas", 14, weight="bold"),
                     text_color=TXT).pack(anchor="w", padx=20, pady=(16, 8))
        scan = self.app.current_scan
        if not scan:
            ctk.CTkLabel(self._summary_card,
                         text="No scan data. Run a scan first.",
                         font=ctk.CTkFont("Consolas", 12), text_color=TXD
                         ).pack(anchor="w", padx=20, pady=(0, 16))
            return
        selected = [t for t in scan.tables if t.selected]
        cols     = sum(t.col_count for t in selected)
        rf = ctk.CTkFrame(self._summary_card, fg_color="transparent")
        rf.pack(anchor="w", padx=20, pady=(0, 16))
        for val, lbl, color in [
            (str(len(selected)), "tables selected", ACCN),
            (str(cols),          "columns",          TEAL),
            (str(len(scan.tables)-len(selected)), "excluded", TXD),
        ]:
            f = ctk.CTkFrame(rf, fg_color=CARD, corner_radius=8)
            f.pack(side="left", padx=(0, 12))
            ctk.CTkLabel(f, text=val,
                         font=ctk.CTkFont("Consolas", 22, weight="bold"),
                         text_color=color).pack(padx=16, pady=(10, 2))
            ctk.CTkLabel(f, text=lbl,
                         font=ctk.CTkFont("Consolas", 10), text_color=TXD
                         ).pack(padx=16, pady=(0, 10))

    def _refresh_dq_gate(self):
        dq = self.app.current_dq
        if not dq:
            self._dq_gate_lbl.configure(
                text="⚠  No DQ run on record. It is recommended to run DQ checks before ingesting to PROD.",
                text_color=AMBN)
        elif dq.status == "fail":
            self._dq_gate_lbl.configure(
                text=f"✖  DQ run FAILED ({dq.summary_fail} failures). Ingestion blocked.",
                text_color=ERR)
        elif dq.status == "warn":
            self._dq_gate_lbl.configure(
                text=f"⚠  DQ run has {dq.summary_warn} warning(s). Review before promoting to PROD.",
                text_color=AMBN)
        else:
            self._dq_gate_lbl.configure(text="✔  DQ checks passed. Safe to ingest.", text_color=SUCC)

    def _get_client(self):
        from app.collibra.client import CollibraClient
        url = self._coll_url.get().strip()
        if not url: raise ValueError("Collibra URL required.")
        return CollibraClient(url, self._coll_user.get().strip(), self._coll_pass.get().strip())

    def _test(self):
        self._coll_status.configure(text="Testing…", text_color=AMBN)
        def run():
            try:
                ok, msg = self._get_client().test_connection()
                self.after(0, lambda: self._coll_status.configure(
                    text=f"{'✔' if ok else '✖'}  {msg}",
                    text_color=SUCC if ok else ERR))
            except Exception as e:
                self.after(0, lambda: self._coll_status.configure(text=f"✖  {e}", text_color=ERR))
        threading.Thread(target=run, daemon=True).start()

    def _load_comms(self):
        def run():
            try:
                c = self._get_client()
                self._client = c
                comms = c.get_communities()
                self._communities = comms
                names = [cm.name for cm in comms]
                self.after(0, lambda: self._comm_om.configure(values=names or ["—"]))
                if names: self.after(0, lambda: self._comm_var.set(names[0]))
                self.after(0, lambda: self._coll_status.configure(
                    text=f"✔  {len(comms)} communities", text_color=SUCC))
            except Exception as e:
                self.after(0, lambda: self._coll_status.configure(text=f"✖  {e}", text_color=ERR))
        threading.Thread(target=run, daemon=True).start()

    def _on_comm_select(self, name):
        comm = next((c for c in self._communities if c.name == name), None)
        if not comm or not self._client: return
        def run():
            try:
                doms = self._client.get_domains(comm.id)
                self._domains = doms
                names = [d.name for d in doms]
                self.after(0, lambda: self._dom_om.configure(values=names or ["—"]))
                if names: self.after(0, lambda: self._dom_var.set(names[0]))
            except Exception as e:
                self.after(0, lambda: self.app.topbar.set_status(f"✖  {e}", ERR))
        threading.Thread(target=run, daemon=True).start()

    def _new_domain(self):
        dlg = ctk.CTkInputDialog(text="New domain name:", title="Create Domain")
        name = dlg.get_input()
        if not name or not name.strip(): return
        comm = next((c for c in self._communities if c.name == self._comm_var.get()), None)
        if not comm or not self._client:
            messagebox.showerror("Error", "Load communities first."); return
        def run():
            try:
                self._client.get_or_create_domain(comm.id, name.strip())
                self.after(0, lambda: self._on_comm_select(self._comm_var.get()))
            except Exception as e:
                self.after(0, lambda: messagebox.showerror("Error", str(e)))
        threading.Thread(target=run, daemon=True).start()

    def _run_ingestion(self):
        scan = self.app.current_scan
        if not scan or not scan.tables:
            messagebox.showerror("Ingestion", "No scan data."); return
        # DQ gate
        dq = self.app.current_dq
        if self._block_on_dq.get() and dq and dq.status == "fail":
            messagebox.showerror("DQ Gate",
                "DQ run has failures. Fix issues or disable the DQ gate to proceed."); return
        comm = next((c for c in self._communities if c.name == self._comm_var.get()), None)
        dom  = next((d for d in self._domains if d.name == self._dom_var.get()), None)
        if not comm or not dom:
            messagebox.showerror("Target", "Select community and domain."); return
        # Save settings
        s = self.app.settings
        s["collibra_url"]      = self._coll_url.get().strip()
        s["collibra_username"] = self._coll_user.get().strip()
        s["collibra_password"] = self._coll_pass.get().strip()
        storage.save_settings(s)

        self._ingest_btn.configure(state="disabled")
        self._cancel_ing.configure(state="normal")
        log_append(self._ingest_log, f"Ingesting → {comm.name} / {dom.name}")
        self._render_summary()
        self._refresh_dq_gate()

        def run():
            from app.models import IngestionResult
            from app.collibra.client import CollibraClient
            result = IngestionResult(scan_id=scan.scan_id,
                                     collibra_url=s["collibra_url"],
                                     community_name=comm.name, domain_name=dom.name)
            client = CollibraClient(s["collibra_url"], s["collibra_username"], s["collibra_password"])
            self._client = client
            selected = [t for t in scan.tables if t.selected]
            def pcb(msg, cur, tot):
                frac = cur / max(tot, 1)
                self.after(0, lambda: self._ingest_prog.update(msg, frac, f"{cur}/{tot}"))
                self.after(0, lambda: log_append(self._ingest_log, msg))
            try:
                client.ingest_tables(selected, comm.id, dom.id, result, pcb,
                                     ingest_cols=self._ingest_cols.get())
                summary = (f"✔  {result.assets_created} created, "
                           f"{result.assets_updated} updated, "
                           f"{result.assets_failed} failed, "
                           f"{result.relations_created} relations")
                self.after(0, lambda: log_append(self._ingest_log, summary))
                self.after(0, lambda: self.app.topbar.set_status(summary[:90], SUCC))
            except Exception as e:
                self.after(0, lambda: log_append(self._ingest_log, f"✖  {e}"))
            finally:
                self.after(0, lambda: self._ingest_btn.configure(state="normal"))
                self.after(0, lambda: self._cancel_ing.configure(state="disabled"))
        threading.Thread(target=run, daemon=True).start()

    def _cancel_ing_fn(self):
        if self._client: self._client.cancel()
        self._cancel_ing.configure(state="disabled")


# ══════════════════════════════════════════════════════════════
#  PAGE: Audit Logs
# ══════════════════════════════════════════════════════════════
class LogsPage(ctk.CTkScrollableFrame):
    def __init__(self, master, app, **k):
        super().__init__(master, fg_color=BG, **k)
        self.app = app
        self._build()

    def _build(self):
        hdr = ctk.CTkFrame(self, fg_color="transparent")
        hdr.pack(fill="x", padx=32, pady=24)
        ctk.CTkLabel(hdr, text="Audit Logs & History",
                     font=ctk.CTkFont("Consolas", 20, weight="bold"),
                     text_color=TXT).pack(side="left")
        GhostButton(hdr, "⟳ Refresh", command=self.refresh, width=100).pack(side="right")
        ctk.CTkFrame(self, height=1, fg_color=BORD).pack(fill="x", padx=32, pady=(0, 16))

        # Scan history
        SectionHeader(self, "SCAN HISTORY").pack(anchor="w", padx=32, pady=(0, 8))
        scans = storage.load_scan_history()
        if not scans:
            ctk.CTkLabel(self, text="No scans yet.", font=ctk.CTkFont("Consolas", 12),
                         text_color=TXD).pack(padx=32)
        else:
            card = Card(self); card.pack(fill="x", padx=32, pady=(0, 20))
            headers = ["Scan ID","Connection","Env","Type","Started","Tables","Cols","Status"]
            widths  = [120,160,70,100,155,75,85,90]
            hrow = ctk.CTkFrame(card, fg_color=HOVER, corner_radius=6)
            hrow.pack(fill="x", padx=12, pady=(12,4))
            for h, w in zip(headers, widths):
                ctk.CTkLabel(hrow, text=h.upper(), font=ctk.CTkFont("Consolas", 10, weight="bold"),
                             text_color=TXD, width=w, anchor="w").pack(side="left", padx=6, pady=7)
            for s in scans:
                row = ctk.CTkFrame(card, fg_color="transparent")
                row.pack(fill="x", padx=12)
                ctk.CTkFrame(card, height=1, fg_color=BORD).pack(fill="x", padx=12)
                st = s.get("status",""); sc = SUCC if st=="complete" else ERR if st=="error" else AMBN
                env= s.get("environment","dev")
                for v, w, c in zip(
                    [s.get("scan_id","")[:12], s.get("connection_name",""),
                     env.upper(), s.get("source_type","").upper(),
                     s.get("started_at","")[:16].replace("T"," "),
                     str(s.get("table_count",0)), str(s.get("column_count",0)), st.upper()],
                    widths,
                    [TXD,TXT,ENV_COLORS.get(env,TXS),TXS,TXD,TEAL,TXS,sc]
                ):
                    ctk.CTkLabel(row, text=v, font=ctk.CTkFont("Consolas", 11),
                                 text_color=c, width=w, anchor="w").pack(side="left", padx=6, pady=9)

        # DQ history
        SectionHeader(self, "DQ HISTORY").pack(anchor="w", padx=32, pady=(8, 8))
        dq_rows = storage.load_dq_history()
        if not dq_rows:
            ctk.CTkLabel(self, text="No DQ runs yet.", font=ctk.CTkFont("Consolas", 12),
                         text_color=TXD).pack(padx=32, pady=(0, 24))
        else:
            card2 = Card(self); card2.pack(fill="x", padx=32, pady=(0, 32))
            headers2 = ["Run ID","Src Env","Tgt","Tables","Pass","Warn","Fail","Status"]
            widths2  = [120,90,90,80,70,70,70,90]
            hrow2 = ctk.CTkFrame(card2, fg_color=HOVER, corner_radius=6)
            hrow2.pack(fill="x", padx=12, pady=(12,4))
            for h, w in zip(headers2, widths2):
                ctk.CTkLabel(hrow2, text=h.upper(), font=ctk.CTkFont("Consolas", 10, weight="bold"),
                             text_color=TXD, width=w, anchor="w").pack(side="left", padx=6, pady=7)
            for d in dq_rows:
                row = ctk.CTkFrame(card2, fg_color="transparent")
                row.pack(fill="x", padx=12)
                ctk.CTkFrame(card2, height=1, fg_color=BORD).pack(fill="x", padx=12)
                st = d.get("status",""); sc = SUCC if st=="ok" else ERR if st=="fail" else AMBN
                for v, w, c in zip(
                    [d.get("run_id","")[:12], d.get("source_env","").upper(),
                     d.get("target_env","").upper(), str(d.get("total_tables",0)),
                     str(d.get("summary_pass",0)), str(d.get("summary_warn",0)),
                     str(d.get("summary_fail",0)), st.upper()],
                    widths2,
                    [TXD,ACCN,TXS,TXT,SUCC,AMBN,ERR,sc]
                ):
                    ctk.CTkLabel(row, text=v, font=ctk.CTkFont("Consolas", 11),
                                 text_color=c, width=w, anchor="w").pack(side="left", padx=6, pady=9)

    def refresh(self):
        for w in self.winfo_children(): w.destroy()
        self._build()


# ══════════════════════════════════════════════════════════════
#  PAGE: Settings
# ══════════════════════════════════════════════════════════════
class SettingsPage(ctk.CTkScrollableFrame):
    def __init__(self, master, app, **k):
        super().__init__(master, fg_color=BG, **k)
        self.app = app
        self._entries: Dict[str, Any] = {}
        self._build()

    def _build(self):
        ctk.CTkLabel(self, text="Settings",
                     font=ctk.CTkFont("Consolas", 20, weight="bold"),
                     text_color=TXT).pack(anchor="w", padx=32, pady=(24, 4))
        ctk.CTkLabel(self, text="Stored in ~/.metaharvest/settings.json",
                     font=ctk.CTkFont("Consolas", 11), text_color=TXD).pack(anchor="w", padx=32)
        ctk.CTkFrame(self, height=1, fg_color=BORD).pack(fill="x", padx=32, pady=(8, 16))
        s = self.app.settings

        self._section("Collibra Defaults")
        for lbl, key, ph in [
            ("Collibra URL",      "collibra_url",      "https://your-org.collibra.com"),
            ("Username",          "collibra_username",  "admin"),
            ("Password",          "collibra_password",  ""),
            ("Default Community", "default_community",  ""),
            ("Default Domain",    "default_domain",     ""),
        ]:
            show = "*" if "password" in key.lower() else ""
            self._row(lbl, key, s.get(key,""), ph, show=show)

        self._section("Scanner")
        self._row("Scan Timeout (sec)", "scan_timeout", str(s.get("scan_timeout",300)), "300")

        self._section("DQ Thresholds")
        from app.config import DQ_THRESHOLDS
        self._row("Null Rate Warn (0-1)", "null_rate_warn", str(DQ_THRESHOLDS["null_rate_warn"]), "0.05")
        self._row("Null Rate Fail (0-1)", "null_rate_fail", str(DQ_THRESHOLDS["null_rate_fail"]), "0.20")
        self._row("Row Count Warn (0-1)", "row_count_warn", str(DQ_THRESHOLDS["row_count_warn"]), "0.10")
        self._row("Row Count Fail (0-1)", "row_count_fail", str(DQ_THRESHOLDS["row_count_fail"]), "0.30")

        self._section("Features")
        self._bool("Auto-transform names for Collibra", "auto_transform_names", s.get("auto_transform_names", True))
        self._bool("Ingest columns by default",         "ingest_columns",       s.get("ingest_columns", True))
        self._bool("Block ingestion on DQ failures",    "block_on_dq_fail",     s.get("block_on_dq_fail", True))

        AccentButton(self, "💾 Save Settings", command=self._save, width=180).pack(anchor="w", padx=32, pady=20)

        info = Card(self); info.pack(fill="x", padx=32, pady=(0, 32))
        ctk.CTkLabel(info, text=f"Data dir: ~/.metaharvest/",
                     font=ctk.CTkFont("Consolas", 11), text_color=TXD).pack(anchor="w", padx=20, pady=12)
        GhostButton(info, "🗑 Clear Scan History",
                    command=self._clear_scans, width=180, color=ERR).pack(anchor="w", padx=20)
        GhostButton(info, "🗑 Clear DQ History",
                    command=self._clear_dq,    width=180, color=ERR).pack(anchor="w", padx=20, pady=(8,12))

    def _section(self, label):
        SectionHeader(self, label).pack(anchor="w", padx=32, pady=(14, 6))

    def _row(self, label, key, value="", placeholder="", show=""):
        f = ctk.CTkFrame(self, fg_color="transparent")
        f.pack(fill="x", padx=32, pady=(0, 6))
        ctk.CTkLabel(f, text=label, font=ctk.CTkFont("Consolas", 11),
                     text_color=TXS, width=220, anchor="w").pack(side="left")
        e = DataEntry(f, placeholder, show=show, width=420)
        if value: e.insert(0, value)
        e.pack(side="left")
        self._entries[key] = e

    def _bool(self, label, key, value=True):
        f = ctk.CTkFrame(self, fg_color="transparent")
        f.pack(fill="x", padx=32, pady=(0, 4))
        var = ctk.BooleanVar(value=value)
        ctk.CTkCheckBox(f, text=label, variable=var,
                        font=ctk.CTkFont("Consolas", 12), text_color=TXS,
                        fg_color=ACCN).pack(side="left")
        self._entries[key] = var

    def _save(self):
        s = self.app.settings
        from app.config import DQ_THRESHOLDS
        for k, widget in self._entries.items():
            if isinstance(widget, ctk.BooleanVar):
                s[k] = widget.get()
            else:
                v = widget.get().strip()
                if k in DQ_THRESHOLDS:
                    try: DQ_THRESHOLDS[k] = float(v)
                    except: pass
                s[k] = v
        storage.save_settings(s)
        self.app.topbar.set_status("✔  Settings saved.", SUCC)

    def _clear_scans(self):
        if messagebox.askyesno("Confirm", "Clear scan history?"):
            storage.SCANS_FILE.write_text("[]")
            self.app.topbar.set_status("✔  Cleared.", SUCC)

    def _clear_dq(self):
        if messagebox.askyesno("Confirm", "Clear DQ history?"):
            storage.DQ_FILE.write_text("[]")
            self.app.topbar.set_status("✔  Cleared.", SUCC)


# ══════════════════════════════════════════════════════════════
#  Main Application
# ══════════════════════════════════════════════════════════════
class MetaHarvestApp(ctk.CTk):
    def __init__(self):
        super().__init__()
        self.title(f"{APP_NAME}  ·  {APP_SUBTITLE}")
        self.geometry("1440x900")
        self.minsize(1100, 700)
        self.configure(fg_color=BG)

        self.settings      = storage.load_settings()
        self.current_scan: Optional[ScanResult]   = None
        self.current_dq:   Optional[DQRunResult]  = None
        self.active_env    = "dev"
        self._pages: Dict[str, ctk.CTkFrame] = {}
        self._build_layout()
        self._show_page("dashboard")

    def _build_layout(self):
        self.sidebar = Sidebar(self, on_nav=self._show_page)
        self.sidebar.pack(side="left", fill="y")
        right = ctk.CTkFrame(self, fg_color=BG, corner_radius=0)
        right.pack(side="left", fill="both", expand=True)
        self.topbar = TopBar(right)
        self.topbar.pack(fill="x")
        ctk.CTkFrame(right, height=1, fg_color=BORD).pack(fill="x")
        self._container = ctk.CTkFrame(right, fg_color=BG, corner_radius=0)
        self._container.pack(fill="both", expand=True)

    def _show_page(self, key: str):
        PAGE_MAP = {
            "dashboard":    DashboardPage,
            "environments": EnvironmentsPage,
            "connections":  ConnectionsPage,
            "scanner":      ScannerPage,
            "preview":      PreviewPage,
            "dq":           DQPage,
            "collibra":     CollibraPage,
            "logs":         LogsPage,
            "settings":     SettingsPage,
        }
        if key not in self._pages:
            cls = PAGE_MAP.get(key)
            if cls:
                pg = cls(self._container, app=self)
                pg.place(relx=0, rely=0, relwidth=1, relheight=1)
                self._pages[key] = pg

        for pg in self._pages.values(): pg.place_forget()
        page = self._pages.get(key)
        if page:
            page.place(relx=0, rely=0, relwidth=1, relheight=1)
            if key in ("dashboard","environments","preview","logs") and hasattr(page, "refresh"):
                page.refresh()

        TITLES = {
            "dashboard": "Dashboard", "environments": "Environment Profiles",
            "connections": "Data Source Connections", "scanner": "Metadata Scanner",
            "preview": "Metadata Preview & Editing", "dq": "Data Quality Engine",
            "collibra": "Collibra Ingestion", "logs": "Audit Logs", "settings": "Settings",
        }
        self.topbar.set_title(TITLES.get(key, key.title()))
