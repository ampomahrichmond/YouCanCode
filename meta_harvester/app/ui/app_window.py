"""
MetaHarvest  ·  Main Application Window
Enterprise Metadata Discovery & Collibra Ingestion Platform
"""
from __future__ import annotations
import customtkinter as ctk
from tkinter import ttk, messagebox, filedialog
import tkinter as tk
import threading, json, os, re
from datetime import datetime
from typing import List, Optional, Dict, Any

from app.config import C, APP_NAME, APP_SUBTITLE, APP_VERSION, SOURCE_TYPES
from app.models import (
    ConnectionConfig, TableMeta, ColumnMeta,
    ScanResult, IngestionResult, CollibraCommunity, CollibraDomain
)
import app.storage as storage

ctk.set_appearance_mode("dark")
ctk.set_default_color_theme("blue")

# ═══════════════════════════════════════════════════════════════
#  Palette helpers
# ═══════════════════════════════════════════════════════════════
BG   = C["bg_main"]
PANEL= C["bg_panel"]
CARD = C["bg_card"]
HOVER= C["bg_hover"]
ACCN = C["accent"]
PURP = C["purple"]
TEAL = C["teal"]
AMBN = C["amber"]
ERR  = C["error"]
SUCC = C["success"]
TXT  = C["text_primary"]
TXS  = C["text_sec"]
TXD  = C["text_dim"]
BORD = C["border"]


# ═══════════════════════════════════════════════════════════════
#  Reusable Widgets
# ═══════════════════════════════════════════════════════════════
class GlowLabel(ctk.CTkLabel):
    def __init__(self, master, text, font_size=13, color=TXT, **kw):
        super().__init__(master, text=text,
                         font=ctk.CTkFont("Consolas", font_size),
                         text_color=color, **kw)

class SectionHeader(ctk.CTkLabel):
    def __init__(self, master, text, **kw):
        super().__init__(master, text=text.upper(),
                         font=ctk.CTkFont("Consolas", 10, weight="bold"),
                         text_color=TXD, **kw)

class AccentButton(ctk.CTkButton):
    def __init__(self, master, text, command=None, color=ACCN, width=130, **kw):
        super().__init__(master, text=text, command=command,
                         fg_color=color, hover_color=C["accent_dim"],
                         font=ctk.CTkFont("Consolas", 12, weight="bold"),
                         corner_radius=6, height=36, width=width, **kw)

class GhostButton(ctk.CTkButton):
    def __init__(self, master, text, command=None, width=120, color=TXS, **kw):
        super().__init__(master, text=text, command=command,
                         fg_color="transparent", hover_color=HOVER,
                         border_color=BORD, border_width=1,
                         text_color=color,
                         font=ctk.CTkFont("Consolas", 12),
                         corner_radius=6, height=34, width=width, **kw)

class DataEntry(ctk.CTkEntry):
    def __init__(self, master, placeholder="", show="", width=300, **kw):
        super().__init__(master, placeholder_text=placeholder,
                         show=show, width=width,
                         fg_color=C["bg_input"], border_color=BORD,
                         text_color=TXT, placeholder_text_color=TXD,
                         font=ctk.CTkFont("Consolas", 12),
                         corner_radius=6, height=36, **kw)

class StatusBadge(ctk.CTkLabel):
    STATUS_COLORS = {
        "ok": SUCC, "success": SUCC, "complete": SUCC,
        "running": ACCN, "scanning": ACCN,
        "error": ERR, "partial": AMBN,
        "untested": TXD, "warning": AMBN,
    }
    def __init__(self, master, status="untested", **kw):
        color = self.STATUS_COLORS.get(status, TXD)
        dot   = "●"
        super().__init__(master,
                         text=f"{dot}  {status.upper()}",
                         font=ctk.CTkFont("Consolas", 10, weight="bold"),
                         text_color=color,
                         fg_color=f"{color}22",
                         corner_radius=4,
                         padx=8, pady=2, **kw)

class Card(ctk.CTkFrame):
    def __init__(self, master, **kw):
        super().__init__(master, fg_color=CARD, corner_radius=10,
                         border_color=BORD, border_width=1, **kw)

class ScrollableCard(ctk.CTkScrollableFrame):
    def __init__(self, master, **kw):
        super().__init__(master, fg_color=CARD, corner_radius=10,
                         border_color=BORD, border_width=1,
                         scrollbar_button_color=BORD,
                         scrollbar_button_hover_color=ACCN, **kw)

class ProgressCard(ctk.CTkFrame):
    def __init__(self, master, label="", **kw):
        super().__init__(master, fg_color=CARD, corner_radius=8,
                         border_color=BORD, border_width=1, **kw)
        self._lbl = ctk.CTkLabel(self, text=label,
                                  font=ctk.CTkFont("Consolas", 12),
                                  text_color=TXS)
        self._lbl.pack(anchor="w", padx=16, pady=(12,4))
        self._bar = ctk.CTkProgressBar(self, height=6,
                                        progress_color=ACCN,
                                        fg_color=C["bg_input"])
        self._bar.set(0)
        self._bar.pack(fill="x", padx=16, pady=(0,4))
        self._sub = ctk.CTkLabel(self, text="",
                                  font=ctk.CTkFont("Consolas", 10),
                                  text_color=TXD)
        self._sub.pack(anchor="w", padx=16, pady=(0,12))

    def update_progress(self, msg="", val=0.0, sub=""):
        self._lbl.configure(text=msg)
        self._bar.set(min(max(val, 0), 1))
        self._sub.configure(text=sub)


# ═══════════════════════════════════════════════════════════════
#  Sidebar Navigation
# ═══════════════════════════════════════════════════════════════
NAV_ITEMS = [
    ("dashboard",   "⬡",  "Dashboard"),
    ("connections", "⬡",  "Connections"),
    ("scanner",     "⬡",  "Scanner"),
    ("preview",     "⬡",  "Metadata Preview"),
    ("collibra",    "⬡",  "Collibra Ingest"),
    ("logs",        "⬡",  "Audit Logs"),
    ("settings",    "⬡",  "Settings"),
]

class Sidebar(ctk.CTkFrame):
    NAV_ICONS = {
        "dashboard":   "  ⬡  ",
        "connections": "  ◈  ",
        "scanner":     "  ◉  ",
        "preview":     "  ⬢  ",
        "collibra":    "  ◆  ",
        "logs":        "  ≡  ",
        "settings":    "  ⚙  ",
    }
    NAV_LABELS = {
        "dashboard":   "Dashboard",
        "connections": "Connections",
        "scanner":     "Scanner",
        "preview":     "Preview",
        "collibra":    "Collibra",
        "logs":        "Audit Logs",
        "settings":    "Settings",
    }

    def __init__(self, master, on_nav, **kw):
        super().__init__(master, fg_color=C["bg_panel"], width=220,
                         corner_radius=0, **kw)
        self.on_nav      = on_nav
        self._active_key = "dashboard"
        self._btns: Dict[str, ctk.CTkButton] = {}
        self._build()

    def _build(self):
        # Logo
        logo_frame = ctk.CTkFrame(self, fg_color="transparent")
        logo_frame.pack(fill="x", pady=(28, 4), padx=20)
        ctk.CTkLabel(logo_frame,
                     text="⬡ MetaHarvest",
                     font=ctk.CTkFont("Consolas", 20, weight="bold"),
                     text_color=ACCN).pack(anchor="w")
        ctk.CTkLabel(logo_frame,
                     text="Enterprise Harvester",
                     font=ctk.CTkFont("Consolas", 10),
                     text_color=TXD).pack(anchor="w", pady=(2, 0))

        sep = ctk.CTkFrame(self, height=1, fg_color=BORD)
        sep.pack(fill="x", padx=16, pady=20)

        SectionHeader(self, "NAVIGATION").pack(anchor="w", padx=20, pady=(0, 8))

        for key in self.NAV_LABELS:
            icon  = self.NAV_ICONS[key]
            label = self.NAV_LABELS[key]
            btn   = ctk.CTkButton(
                self,
                text=f"{icon}  {label}",
                anchor="w",
                fg_color="transparent",
                hover_color=HOVER,
                text_color=TXS,
                font=ctk.CTkFont("Consolas", 13),
                corner_radius=8,
                height=40,
                command=lambda k=key: self._nav(k),
            )
            btn.pack(fill="x", padx=12, pady=2)
            self._btns[key] = btn

        # Bottom version
        ctk.CTkFrame(self, height=1, fg_color=BORD).pack(
            fill="x", padx=16, side="bottom", pady=(0, 12))
        ctk.CTkLabel(self,
                     text=f"v{APP_VERSION}",
                     font=ctk.CTkFont("Consolas", 10),
                     text_color=TXD).pack(side="bottom", pady=4)

        self._highlight("dashboard")

    def _nav(self, key: str):
        self._active_key = key
        self._highlight(key)
        self.on_nav(key)

    def _highlight(self, active_key: str):
        for key, btn in self._btns.items():
            if key == active_key:
                btn.configure(fg_color=C["accent_glow"], text_color=ACCN,
                               border_color=ACCN, border_width=1)
            else:
                btn.configure(fg_color="transparent", text_color=TXS,
                               border_color="transparent", border_width=0)

    def set_active(self, key: str):
        self._nav(key)


# ═══════════════════════════════════════════════════════════════
#  Top Bar
# ═══════════════════════════════════════════════════════════════
class TopBar(ctk.CTkFrame):
    def __init__(self, master, **kw):
        super().__init__(master, fg_color=C["bg_panel"], height=56,
                         corner_radius=0, **kw)
        self.grid_propagate(False)
        self._title_lbl = ctk.CTkLabel(self, text="Dashboard",
                                        font=ctk.CTkFont("Consolas", 17, weight="bold"),
                                        text_color=TXT)
        self._title_lbl.pack(side="left", padx=28)
        self._status_lbl = ctk.CTkLabel(self, text="",
                                         font=ctk.CTkFont("Consolas", 11),
                                         text_color=SUCC)
        self._status_lbl.pack(side="right", padx=20)
        ts_lbl = ctk.CTkLabel(self, text="",
                               font=ctk.CTkFont("Consolas", 11),
                               text_color=TXD)
        ts_lbl.pack(side="right", padx=8)
        self._update_ts(ts_lbl)

    def set_title(self, t: str): self._title_lbl.configure(text=t)
    def set_status(self, t: str, color=SUCC): self._status_lbl.configure(text=t, text_color=color)

    def _update_ts(self, lbl):
        lbl.configure(text=datetime.now().strftime("%a %d %b %Y  %H:%M"))
        lbl.after(30_000, lambda: self._update_ts(lbl))


# ═══════════════════════════════════════════════════════════════
#  PAGE: Dashboard
# ═══════════════════════════════════════════════════════════════
class DashboardPage(ctk.CTkScrollableFrame):
    def __init__(self, master, app, **kw):
        super().__init__(master, fg_color=BG, **kw)
        self.app = app
        self._build()

    def _build(self):
        self._header()
        self._stats_row()
        self._quick_actions()
        self._recent_scans()

    def _header(self):
        f = ctk.CTkFrame(self, fg_color="transparent")
        f.pack(fill="x", padx=32, pady=(28, 12))
        ctk.CTkLabel(f, text="Enterprise Metadata Harvester",
                     font=ctk.CTkFont("Consolas", 26, weight="bold"),
                     text_color=TXT).pack(anchor="w")
        ctk.CTkLabel(f, text="Discover · Harvest · Govern  ·  Databricks  ·  ADLS  ·  Synapse  ·  Collibra",
                     font=ctk.CTkFont("Consolas", 12),
                     text_color=TXD).pack(anchor="w", pady=(4, 0))
        sep = ctk.CTkFrame(self, height=1, fg_color=BORD)
        sep.pack(fill="x", padx=32, pady=(8, 20))

    def _stats_row(self):
        row = ctk.CTkFrame(self, fg_color="transparent")
        row.pack(fill="x", padx=32, pady=(0, 20))
        conns  = storage.load_connections()
        scans  = storage.load_scan_history()
        tables = sum(s.get("table_count", 0) for s in scans[:10])
        stats  = [
            ("Connections", str(len(conns)),      ACCN, "Active sources"),
            ("Recent Scans", str(len(scans)),      PURP, "Last 100"),
            ("Tables Found", f"{tables:,}",        TEAL, "From recent scans"),
            ("Collibra URL",
             "✔  Configured" if self.app.settings.get("collibra_url") else "✖  Not set",
             SUCC if self.app.settings.get("collibra_url") else ERR,
             "Target catalog"),
        ]
        for i, (label, val, color, sub) in enumerate(stats):
            c = Card(row)
            c.pack(side="left", padx=(0, 16), fill="both", expand=True)
            ctk.CTkLabel(c, text=label,
                         font=ctk.CTkFont("Consolas", 11),
                         text_color=TXD).pack(anchor="w", padx=20, pady=(16, 4))
            ctk.CTkLabel(c, text=val,
                         font=ctk.CTkFont("Consolas", 28, weight="bold"),
                         text_color=color).pack(anchor="w", padx=20)
            ctk.CTkLabel(c, text=sub,
                         font=ctk.CTkFont("Consolas", 10),
                         text_color=TXD).pack(anchor="w", padx=20, pady=(2, 16))

    def _quick_actions(self):
        ctk.CTkLabel(self, text="QUICK ACTIONS",
                     font=ctk.CTkFont("Consolas", 10, weight="bold"),
                     text_color=TXD).pack(anchor="w", padx=32, pady=(0, 10))
        row = ctk.CTkFrame(self, fg_color="transparent")
        row.pack(fill="x", padx=32, pady=(0, 24))
        actions = [
            ("+ Add Connection",  "connections", ACCN),
            ("⟳  Run Scanner",   "scanner",     PURP),
            ("⬆  Ingest to Collibra", "collibra", C["collibra"]),
            ("⚙  Settings",      "settings",    TXS),
        ]
        for label, page, color in actions:
            AccentButton(row, label,
                         command=lambda p=page: self.app.sidebar.set_active(p),
                         color=color, width=180).pack(side="left", padx=(0, 12))

    def _recent_scans(self):
        ctk.CTkLabel(self, text="RECENT SCAN HISTORY",
                     font=ctk.CTkFont("Consolas", 10, weight="bold"),
                     text_color=TXD).pack(anchor="w", padx=32, pady=(0, 10))
        scans = storage.load_scan_history()[:8]
        if not scans:
            ctk.CTkLabel(self, text="No scans yet — run your first scan from the Scanner page.",
                         font=ctk.CTkFont("Consolas", 12), text_color=TXD).pack(padx=32)
            return
        tbl_frame = Card(self)
        tbl_frame.pack(fill="x", padx=32, pady=(0, 32))
        headers = ["Connection", "Type", "Started", "Tables", "Columns", "Status"]
        col_ws   = [200, 100, 170, 80, 80, 90]
        hdr_row  = ctk.CTkFrame(tbl_frame, fg_color=C["bg_hover"], corner_radius=8)
        hdr_row.pack(fill="x", padx=12, pady=(12, 4))
        for h, w in zip(headers, col_ws):
            ctk.CTkLabel(hdr_row, text=h.upper(),
                         font=ctk.CTkFont("Consolas", 10, weight="bold"),
                         text_color=TXD, width=w, anchor="w").pack(side="left", padx=8, pady=8)
        for s in scans:
            row = ctk.CTkFrame(tbl_frame, fg_color="transparent")
            row.pack(fill="x", padx=12)
            sep = ctk.CTkFrame(tbl_frame, height=1, fg_color=BORD)
            sep.pack(fill="x", padx=12)
            vals = [
                s.get("connection_name",""),
                s.get("source_type","").upper(),
                s.get("started_at","")[:16].replace("T"," "),
                str(s.get("table_count", 0)),
                str(s.get("column_count", 0)),
                s.get("status","").upper(),
            ]
            colors = [TXT, TXS, TXD, TEAL, TXS,
                      SUCC if s.get("status") == "complete" else ERR]
            for v, w, col in zip(vals, col_ws, colors):
                ctk.CTkLabel(row, text=v,
                             font=ctk.CTkFont("Consolas", 11),
                             text_color=col, width=w, anchor="w"
                             ).pack(side="left", padx=8, pady=10)

    def refresh(self):
        for w in self.winfo_children(): w.destroy()
        self._build()


# ═══════════════════════════════════════════════════════════════
#  PAGE: Connections
# ═══════════════════════════════════════════════════════════════
class ConnectionsPage(ctk.CTkFrame):
    def __init__(self, master, app, **kw):
        super().__init__(master, fg_color=BG, **kw)
        self.app = app
        self._build()

    def _build(self):
        top = ctk.CTkFrame(self, fg_color="transparent")
        top.pack(fill="x", padx=32, pady=24)
        ctk.CTkLabel(top, text="Data Source Connections",
                     font=ctk.CTkFont("Consolas", 20, weight="bold"),
                     text_color=TXT).pack(side="left")
        AccentButton(top, "+ New Connection",
                     command=self._new_conn).pack(side="right")

        sep = ctk.CTkFrame(self, height=1, fg_color=BORD)
        sep.pack(fill="x", padx=32, pady=(0, 16))

        self._list_frame = ctk.CTkScrollableFrame(self, fg_color="transparent")
        self._list_frame.pack(fill="both", expand=True, padx=32, pady=(0, 16))
        self._render_list()

    def _render_list(self):
        for w in self._list_frame.winfo_children(): w.destroy()
        conns = storage.load_connections()
        if not conns:
            ctk.CTkLabel(self._list_frame,
                         text="No connections yet.\nClick '+ New Connection' to add your first data source.",
                         font=ctk.CTkFont("Consolas", 13), text_color=TXD,
                         justify="center").pack(expand=True, pady=60)
            return
        for conn in conns:
            self._render_conn_card(conn)

    def _render_conn_card(self, conn: ConnectionConfig):
        src  = next((s for s in SOURCE_TYPES if s["id"] == conn.source_type), {})
        icon = src.get("icon", "●")
        col  = src.get("color", ACCN)

        card = ctk.CTkFrame(self._list_frame, fg_color=CARD, corner_radius=10,
                             border_color=BORD, border_width=1)
        card.pack(fill="x", pady=6)

        left = ctk.CTkFrame(card, fg_color="transparent")
        left.pack(side="left", padx=20, pady=16)
        ctk.CTkLabel(left, text=icon, font=ctk.CTkFont("Consolas", 26),
                     text_color=col).pack(side="left", padx=(0, 16))

        info = ctk.CTkFrame(left, fg_color="transparent")
        info.pack(side="left")
        ctk.CTkLabel(info, text=conn.name,
                     font=ctk.CTkFont("Consolas", 14, weight="bold"),
                     text_color=TXT).pack(anchor="w")
        host_str = conn.host or conn.account_name or conn.server or conn.folder_path or conn.hive_host or "—"
        ctk.CTkLabel(info, text=f"{src.get('label',conn.source_type)}  ·  {host_str}",
                     font=ctk.CTkFont("Consolas", 11),
                     text_color=TXD).pack(anchor="w")

        right = ctk.CTkFrame(card, fg_color="transparent")
        right.pack(side="right", padx=16)
        StatusBadge(right, conn.status).pack(side="left", padx=8)
        GhostButton(right, "✎ Edit",   command=lambda c=conn: self._edit_conn(c), width=80).pack(side="left", padx=4)
        GhostButton(right, "⚡ Test",  command=lambda c=conn: self._test_conn(c), width=80, color=TEAL).pack(side="left", padx=4)
        GhostButton(right, "✕ Delete", command=lambda c=conn: self._del_conn(c),  width=80, color=ERR).pack(side="left", padx=4)

    def _new_conn(self):
        ConnDialog(self, is_new=True, on_save=self._on_saved)

    def _edit_conn(self, conn: ConnectionConfig):
        ConnDialog(self, is_new=False, conn=conn, on_save=self._on_saved)

    def _test_conn(self, conn: ConnectionConfig):
        from app.connectors.connectors import get_connector
        self.app.topbar.set_status("Testing connection…", AMBN)
        def run():
            try:
                c = get_connector(conn)
                ok, msg = c.test_connection()
                conn.status     = "ok" if ok else "error"
                conn.last_tested = datetime.now().isoformat()
                storage.upsert_connection(conn)
                color = SUCC if ok else ERR
                self.after(0, lambda: self.app.topbar.set_status(f"{'✔' if ok else '✖'}  {msg}", color))
                self.after(0, self._render_list)
            except Exception as e:
                self.after(0, lambda: self.app.topbar.set_status(f"✖  {e}", ERR))
        threading.Thread(target=run, daemon=True).start()

    def _del_conn(self, conn: ConnectionConfig):
        if messagebox.askyesno("Delete", f"Delete '{conn.name}'?"):
            storage.delete_connection(conn.id)
            self._render_list()

    def _on_saved(self):
        self._render_list()


# ═══════════════════════════════════════════════════════════════
#  Connection Dialog
# ═══════════════════════════════════════════════════════════════
class ConnDialog(ctk.CTkToplevel):
    def __init__(self, master, is_new=True, conn: ConnectionConfig = None, on_save=None):
        super().__init__(master)
        self.title("New Connection" if is_new else "Edit Connection")
        self.geometry("680x760")
        self.configure(fg_color=BG)
        self.resizable(False, True)
        self.grab_set()
        self.on_save   = on_save
        self.conn      = conn or ConnectionConfig()
        self.is_new    = is_new
        self._entries: Dict[str, ctk.CTkEntry] = {}
        self._build()

    def _build(self):
        ctk.CTkLabel(self,
                     text="Configure Connection",
                     font=ctk.CTkFont("Consolas", 17, weight="bold"),
                     text_color=TXT).pack(anchor="w", padx=24, pady=(20, 4))
        ctk.CTkLabel(self,
                     text="All credentials are stored locally on your machine.",
                     font=ctk.CTkFont("Consolas", 11),
                     text_color=TXD).pack(anchor="w", padx=24, pady=(0, 12))

        # Scroll area
        sf = ctk.CTkScrollableFrame(self, fg_color="transparent")
        sf.pack(fill="both", expand=True, padx=16, pady=(0, 0))

        # Name
        self._row(sf, "Connection Name", "name", self.conn.name, "My Databricks Prod")

        # Source Type
        lbl_f = ctk.CTkFrame(sf, fg_color="transparent")
        lbl_f.pack(fill="x", padx=8, pady=(8, 0))
        ctk.CTkLabel(lbl_f, text="Source Type",
                     font=ctk.CTkFont("Consolas", 11), text_color=TXS).pack(anchor="w")
        self._src_var = ctk.StringVar(value=self.conn.source_type or SOURCE_TYPES[0]["id"])
        om = ctk.CTkOptionMenu(sf,
                               values=[s["id"] for s in SOURCE_TYPES],
                               variable=self._src_var,
                               command=self._on_src_change,
                               fg_color=CARD, button_color=BORD,
                               button_hover_color=HOVER,
                               text_color=TXT,
                               font=ctk.CTkFont("Consolas", 12),
                               width=320, height=36)
        om.pack(anchor="w", padx=8, pady=(4, 0))

        self._dyn_frame = ctk.CTkFrame(sf, fg_color="transparent")
        self._dyn_frame.pack(fill="x")
        self._render_fields(self._src_var.get())

        # Buttons
        btn_row = ctk.CTkFrame(self, fg_color="transparent")
        btn_row.pack(fill="x", padx=16, pady=16)
        AccentButton(btn_row, "Save", command=self._save, width=120).pack(side="right", padx=8)
        GhostButton(btn_row, "Cancel", command=self.destroy, width=100).pack(side="right")

    def _on_src_change(self, val):
        self._render_fields(val)

    def _render_fields(self, src_type: str):
        for w in self._dyn_frame.winfo_children(): w.destroy()
        # Clear dynamic entries (keep name)
        for k in list(self._entries.keys()):
            if k != "name": del self._entries[k]

        f = self._dyn_frame
        if src_type == "databricks":
            self._row(f, "Workspace Host", "host", self.conn.host, "https://adb-xxx.azuredatabricks.net")
            self._row(f, "Personal Access Token", "token", self.conn.token, "dapi…", show="*")
            self._row(f, "HTTP Path (optional)", "http_path", self.conn.http_path, "/sql/1.0/warehouses/…")
            self._row(f, "Default Catalog (optional)", "catalog", self.conn.catalog, "main")
        elif src_type == "adls":
            self._row(f, "Storage Account Name", "account_name", self.conn.account_name, "mystorageaccount")
            self._row(f, "Account Key", "account_key", self.conn.account_key, "", show="*")
            ctk.CTkLabel(f, text="  — or Service Principal —",
                         font=ctk.CTkFont("Consolas", 10), text_color=TXD).pack(anchor="w", padx=8, pady=4)
            self._row(f, "Tenant ID", "tenant_id", self.conn.tenant_id, "xxxxxxxx-xxxx-xxxx-xxxx-xxxxxxxxxxxx")
            self._row(f, "Client ID",     "client_id",     self.conn.client_id, "")
            self._row(f, "Client Secret", "client_secret", self.conn.client_secret, "", show="*")
            self._row(f, "Container (leave blank for all)", "container", self.conn.container, "raw-data")
        elif src_type in ("synapse", "azure_sql"):
            self._row(f, "Server",   "server",   self.conn.server,   "workspace.sql.azuresynapse.net")
            self._row(f, "Database", "database", self.conn.database, "DataWarehouse")
            self._row(f, "Username", "username", self.conn.username, "")
            self._row(f, "Password", "password", self.conn.password, "", show="*")
        elif src_type == "parquet":
            path_f = ctk.CTkFrame(f, fg_color="transparent")
            path_f.pack(fill="x", padx=8, pady=8)
            ctk.CTkLabel(path_f, text="Folder Path",
                         font=ctk.CTkFont("Consolas", 11), text_color=TXS).pack(anchor="w")
            row2 = ctk.CTkFrame(path_f, fg_color="transparent")
            row2.pack(fill="x")
            e = DataEntry(row2, "C:\\Data\\parquet", width=400)
            e.insert(0, self.conn.folder_path)
            e.pack(side="left", pady=4)
            GhostButton(row2, "Browse…",
                        command=lambda: self._browse(e), width=90).pack(side="left", padx=8)
            self._entries["folder_path"] = e
        elif src_type == "hive":
            self._row(f, "Hive Host",   "hive_host", self.conn.hive_host, "hive-server.internal")
            self._row(f, "Port",        "hive_port", str(self.conn.hive_port), "10000")
            self._row(f, "Username",    "username",  self.conn.username,  "hive")

    def _browse(self, entry):
        path = filedialog.askdirectory()
        if path:
            entry.delete(0, "end")
            entry.insert(0, path)

    def _row(self, parent, label, key, value="", placeholder="", show=""):
        f = ctk.CTkFrame(parent, fg_color="transparent")
        f.pack(fill="x", padx=8, pady=(8, 0))
        ctk.CTkLabel(f, text=label,
                     font=ctk.CTkFont("Consolas", 11), text_color=TXS).pack(anchor="w")
        e = DataEntry(f, placeholder, show=show, width=460)
        if value: e.insert(0, str(value))
        e.pack(anchor="w", pady=(4, 0))
        self._entries[key] = e

    def _save(self):
        name = self._entries.get("name")
        if not name or not name.get().strip():
            messagebox.showerror("Validation", "Connection name is required.")
            return
        c = self.conn
        c.name        = self._entries["name"].get().strip()
        c.source_type = self._src_var.get()
        for k, e in self._entries.items():
            if k == "name": continue
            val = e.get().strip()
            if hasattr(c, k):
                if k == "hive_port":
                    try: setattr(c, k, int(val))
                    except: setattr(c, k, 10000)
                else:
                    setattr(c, k, val)
        storage.upsert_connection(c)
        if self.on_save: self.on_save()
        self.destroy()


# ═══════════════════════════════════════════════════════════════
#  PAGE: Scanner
# ═══════════════════════════════════════════════════════════════
class ScannerPage(ctk.CTkFrame):
    def __init__(self, master, app, **kw):
        super().__init__(master, fg_color=BG, **kw)
        self.app = app
        self._scan_thread: Optional[threading.Thread] = None
        self._connector    = None
        self._build()

    def _build(self):
        # Header
        hdr = ctk.CTkFrame(self, fg_color="transparent")
        hdr.pack(fill="x", padx=32, pady=24)
        ctk.CTkLabel(hdr, text="Metadata Scanner",
                     font=ctk.CTkFont("Consolas", 20, weight="bold"),
                     text_color=TXT).pack(side="left")

        sep = ctk.CTkFrame(self, height=1, fg_color=BORD)
        sep.pack(fill="x", padx=32, pady=(0, 20))

        content = ctk.CTkFrame(self, fg_color="transparent")
        content.pack(fill="both", expand=True, padx=32)
        content.columnconfigure(0, weight=1)
        content.columnconfigure(1, weight=2)

        # ── Left config panel ──────────────────────────────────
        left = ctk.CTkFrame(content, fg_color="transparent")
        left.grid(row=0, column=0, sticky="nsew", padx=(0, 16))

        SectionHeader(left, "SELECT SOURCE").pack(anchor="w", pady=(0, 8))
        self._conn_var = ctk.StringVar()
        self._conn_map: Dict[str, ConnectionConfig] = {}
        self._refresh_connections()
        self._conn_om = ctk.CTkOptionMenu(
            left,
            values=list(self._conn_map.keys()) or ["— no connections —"],
            variable=self._conn_var,
            fg_color=CARD, button_color=BORD, button_hover_color=HOVER,
            text_color=TXT, font=ctk.CTkFont("Consolas", 12),
            width=300, height=36
        )
        self._conn_om.pack(anchor="w", pady=(0, 16))

        SectionHeader(left, "FILTER PATTERN").pack(anchor="w", pady=(0, 8))
        self._filter_entry = DataEntry(left, "* or sales_* or *_prod", width=300)
        self._filter_entry.insert(0, "*")
        self._filter_entry.pack(anchor="w", pady=(0, 16))

        SectionHeader(left, "OPTIONS").pack(anchor="w", pady=(0, 8))
        self._scan_cols_var = ctk.BooleanVar(value=True)
        ctk.CTkCheckBox(left, text="Scan column metadata",
                        variable=self._scan_cols_var,
                        font=ctk.CTkFont("Consolas", 12), text_color=TXS,
                        fg_color=ACCN, hover_color=C["accent_dim"]).pack(anchor="w", pady=4)
        self._hive_fix_var = ctk.BooleanVar(value=True)
        ctk.CTkCheckBox(left, text="Auto-fix Hive 18-char names",
                        variable=self._hive_fix_var,
                        font=ctk.CTkFont("Consolas", 12), text_color=TXS,
                        fg_color=AMBN, hover_color=C["accent_dim"]).pack(anchor="w", pady=4)

        btn_row = ctk.CTkFrame(left, fg_color="transparent")
        btn_row.pack(anchor="w", pady=20)
        self._scan_btn = AccentButton(btn_row, "▶  Start Scan",
                                       command=self._start_scan, width=150)
        self._scan_btn.pack(side="left", padx=(0, 8))
        self._cancel_btn = GhostButton(btn_row, "◼  Cancel",
                                        command=self._cancel_scan, width=100,
                                        color=ERR)
        self._cancel_btn.pack(side="left")
        self._cancel_btn.configure(state="disabled")

        # ── Right status panel ─────────────────────────────────
        right = ctk.CTkFrame(content, fg_color="transparent")
        right.grid(row=0, column=1, sticky="nsew")

        SectionHeader(right, "SCAN STATUS").pack(anchor="w", pady=(0, 8))
        self._prog_card = ProgressCard(right, "Ready to scan…")
        self._prog_card.pack(fill="x", pady=(0, 16))

        SectionHeader(right, "LIVE OUTPUT").pack(anchor="w", pady=(0, 8))
        self._log_box = ctk.CTkTextbox(right, height=360,
                                        fg_color=C["bg_input"],
                                        text_color=SUCC,
                                        font=ctk.CTkFont("Consolas", 11),
                                        border_color=BORD, border_width=1,
                                        corner_radius=8)
        self._log_box.pack(fill="both", expand=True)
        self._log("⬡  MetaHarvest Scanner ready.\n")

    def _log(self, msg: str):
        self._log_box.configure(state="normal")
        self._log_box.insert("end", f"[{datetime.now().strftime('%H:%M:%S')}]  {msg}\n")
        self._log_box.see("end")
        self._log_box.configure(state="disabled")

    def _refresh_connections(self):
        self._conn_map = {c.name: c for c in storage.load_connections()}

    def _start_scan(self):
        self._refresh_connections()
        conn_name = self._conn_var.get()
        if not conn_name or conn_name not in self._conn_map:
            messagebox.showerror("Scanner", "Please select a connection first.")
            return
        conn = self._conn_map[conn_name]

        from app.connectors.connectors import get_connector
        self._connector = get_connector(conn)

        self._scan_btn.configure(state="disabled")
        self._cancel_btn.configure(state="normal")
        self._log(f"Starting scan on '{conn_name}' ({conn.source_type})…")
        self._prog_card.update_progress("Initialising…", 0.0)

        def run():
            try:
                result = ScanResult(
                    connection_id   = conn.id,
                    connection_name = conn.name,
                    source_type     = conn.source_type,
                )

                def progress_cb(msg, cur, tot):
                    frac = (cur / max(tot, 1)) if tot else 0
                    self.after(0, lambda: self._prog_card.update_progress(
                        msg, frac, f"{cur}/{tot}"))
                    self.after(0, lambda: self._log(msg))

                tables = self._connector.scan(
                    progress_cb    = progress_cb,
                    filter_pattern = self._filter_entry.get().strip() or "*",
                )
                result.tables      = tables
                result.status      = "complete"
                result.finished_at = datetime.now().isoformat()

                storage.save_scan_summary(result)
                self.app.current_scan = result

                self.after(0, lambda: self._prog_card.update_progress(
                    f"✔  Scan complete — {len(tables)} objects", 1.0,
                    f"{result.column_count} columns | {result.duration_sec:.1f}s"))
                self.after(0, lambda: self._log(
                    f"✔  Scan complete: {len(tables)} tables, {result.column_count} columns"))
                self.after(0, lambda: self.app.topbar.set_status(
                    f"✔  Scan complete — {len(tables)} tables", SUCC))

            except Exception as e:
                self.after(0, lambda: self._log(f"✖  Error: {e}"))
                self.after(0, lambda: self._prog_card.update_progress(f"✖  {e}", 0, ""))
                self.after(0, lambda: self.app.topbar.set_status(f"✖  Scan error", ERR))
            finally:
                self.after(0, lambda: self._scan_btn.configure(state="normal"))
                self.after(0, lambda: self._cancel_btn.configure(state="disabled"))

        self._scan_thread = threading.Thread(target=run, daemon=True)
        self._scan_thread.start()

    def _cancel_scan(self):
        if self._connector:
            self._connector.cancel()
        self._log("⚠  Scan cancelled by user.")
        self._cancel_btn.configure(state="disabled")


# ═══════════════════════════════════════════════════════════════
#  PAGE: Metadata Preview
# ═══════════════════════════════════════════════════════════════
class PreviewPage(ctk.CTkFrame):
    def __init__(self, master, app, **kw):
        super().__init__(master, fg_color=BG, **kw)
        self.app = app
        self._selected_table: Optional[TableMeta] = None
        self._build()

    def _build(self):
        hdr = ctk.CTkFrame(self, fg_color="transparent")
        hdr.pack(fill="x", padx=32, pady=24)
        ctk.CTkLabel(hdr, text="Metadata Preview & Editing",
                     font=ctk.CTkFont("Consolas", 20, weight="bold"),
                     text_color=TXT).pack(side="left")
        GhostButton(hdr, "⟳ Refresh",
                    command=self.refresh, width=110).pack(side="right")

        sep = ctk.CTkFrame(self, height=1, fg_color=BORD)
        sep.pack(fill="x", padx=32, pady=(0, 16))

        panes = ctk.CTkFrame(self, fg_color="transparent")
        panes.pack(fill="both", expand=True, padx=32, pady=(0, 16))
        panes.columnconfigure(0, weight=2)
        panes.columnconfigure(1, weight=3)
        panes.rowconfigure(0, weight=1)

        # ── Table list ─────────────────────────────────────────
        left = ctk.CTkFrame(panes, fg_color="transparent")
        left.grid(row=0, column=0, sticky="nsew", padx=(0, 12))

        SectionHeader(left, "SCANNED OBJECTS").pack(anchor="w", pady=(0, 8))
        self._search_var = ctk.StringVar()
        self._search_var.trace_add("write", lambda *_: self._render_table_list())
        DataEntry(left, "Filter tables…", width=300).pack(anchor="w", pady=(0, 8))
        self._search_entry = left.winfo_children()[-1]
        self._search_entry.configure(textvariable=self._search_var)

        self._tbl_list = ctk.CTkScrollableFrame(left, fg_color="transparent",
                                                 corner_radius=0)
        self._tbl_list.pack(fill="both", expand=True)
        self._render_table_list()

        # ── Detail panel ───────────────────────────────────────
        right = ctk.CTkFrame(panes, fg_color="transparent")
        right.grid(row=0, column=1, sticky="nsew")

        SectionHeader(right, "TABLE DETAILS").pack(anchor="w", pady=(0, 8))
        self._detail_frame = ctk.CTkScrollableFrame(right, fg_color=CARD,
                                                     corner_radius=10,
                                                     border_color=BORD, border_width=1)
        self._detail_frame.pack(fill="both", expand=True)
        ctk.CTkLabel(self._detail_frame,
                     text="Select a table on the left to view details.",
                     font=ctk.CTkFont("Consolas", 12), text_color=TXD
                     ).pack(expand=True, pady=60)

    def _render_table_list(self):
        for w in self._tbl_list.winfo_children(): w.destroy()
        scan = self.app.current_scan
        if not scan:
            ctk.CTkLabel(self._tbl_list,
                         text="No scan results.\nRun a scan first.",
                         font=ctk.CTkFont("Consolas", 12), text_color=TXD,
                         justify="center").pack(pady=40)
            return

        q = self._search_var.get().lower()
        tables = [t for t in scan.tables if q in t.display_name.lower()] if q else scan.tables

        for t in tables:
            row = ctk.CTkFrame(self._tbl_list, fg_color="transparent",
                               cursor="hand2")
            row.pack(fill="x", pady=2)
            inner = ctk.CTkFrame(row, fg_color=CARD, corner_radius=8,
                                  border_color=BORD, border_width=1)
            inner.pack(fill="x")

            # checkbox
            var = ctk.BooleanVar(value=t.selected)
            cb  = ctk.CTkCheckBox(inner, text="", variable=var, width=20,
                                   fg_color=ACCN, hover_color=C["accent_dim"],
                                   command=lambda t=t, v=var: self._toggle_sel(t, v))
            cb.pack(side="left", padx=8, pady=10)

            ctk.CTkLabel(inner, text=t.table_name,
                         font=ctk.CTkFont("Consolas", 12, weight="bold"),
                         text_color=TXT, anchor="w").pack(side="left", padx=4)
            ctk.CTkLabel(inner, text=f"{t.col_count} cols",
                         font=ctk.CTkFont("Consolas", 10),
                         text_color=TXD).pack(side="right", padx=12)
            ctk.CTkLabel(inner, text=t.object_type,
                         font=ctk.CTkFont("Consolas", 10),
                         text_color=ACCN if t.object_type == "Table" else PURP
                         ).pack(side="right", padx=4)

            inner.bind("<Button-1>", lambda e, tbl=t: self._show_detail(tbl))
            for child in inner.winfo_children():
                child.bind("<Button-1>", lambda e, tbl=t: self._show_detail(tbl))

    def _toggle_sel(self, t: TableMeta, var: ctk.BooleanVar):
        t.selected = var.get()

    def _show_detail(self, t: TableMeta):
        self._selected_table = t
        for w in self._detail_frame.winfo_children(): w.destroy()

        # Info header
        ih = ctk.CTkFrame(self._detail_frame, fg_color=C["bg_hover"], corner_radius=8)
        ih.pack(fill="x", padx=16, pady=(16, 8))
        ctk.CTkLabel(ih, text=t.table_name,
                     font=ctk.CTkFont("Consolas", 16, weight="bold"),
                     text_color=ACCN).pack(anchor="w", padx=16, pady=(12, 4))
        ctk.CTkLabel(ih, text=t.full_path,
                     font=ctk.CTkFont("Consolas", 10), text_color=TXD
                     ).pack(anchor="w", padx=16, pady=(0, 12))

        # Meta row
        meta = [
            ("Type",     t.object_type),
            ("Database", t.database),
            ("Schema",   t.schema),
            ("Columns",  str(t.col_count)),
            ("Rows",     f"{t.row_count:,}" if t.row_count else "—"),
            ("Size",     f"{t.size_bytes // 1024:,} KB" if t.size_bytes else "—"),
        ]
        mr = ctk.CTkFrame(self._detail_frame, fg_color="transparent")
        mr.pack(fill="x", padx=16, pady=(0, 12))
        for k, v in meta:
            mf = ctk.CTkFrame(mr, fg_color=CARD, corner_radius=6)
            mf.pack(side="left", padx=(0, 6), pady=2)
            ctk.CTkLabel(mf, text=k, font=ctk.CTkFont("Consolas", 9), text_color=TXD
                         ).pack(anchor="w", padx=8, pady=(6, 0))
            ctk.CTkLabel(mf, text=v, font=ctk.CTkFont("Consolas", 12, weight="bold"),
                         text_color=TXT).pack(anchor="w", padx=8, pady=(0, 6))

        # Collibra name edit
        SectionHeader(self._detail_frame, "COLLIBRA TARGET NAME").pack(anchor="w", padx=16, pady=(8, 4))
        cname_entry = DataEntry(self._detail_frame, "Collibra asset name", width=400)
        cname_entry.insert(0, t.collibra_name or t.table_name)
        cname_entry.pack(anchor="w", padx=16, pady=(0, 12))
        AccentButton(self._detail_frame, "Apply Name",
                     command=lambda: self._apply_name(t, cname_entry.get()),
                     width=120, color=TEAL).pack(anchor="w", padx=16, pady=(0, 16))

        # Hive warning
        if len(t.table_name) > 18 and t.source_type == "hive":
            warn = ctk.CTkFrame(self._detail_frame, fg_color=f"{AMBN}22",
                                 corner_radius=8, border_color=AMBN, border_width=1)
            warn.pack(fill="x", padx=16, pady=(0, 12))
            ctk.CTkLabel(warn, text=f"⚠  Hive 18-char limit: '{t.table_name}' will be stored as '{t.properties.get('hive_safe_name', t.table_name[:18])}'",
                         font=ctk.CTkFont("Consolas", 11), text_color=AMBN
                         ).pack(padx=12, pady=10)

        # Columns
        SectionHeader(self._detail_frame, f"COLUMNS ({t.col_count})").pack(anchor="w", padx=16, pady=(8, 4))
        # Column header
        ch = ctk.CTkFrame(self._detail_frame, fg_color=HOVER, corner_radius=6)
        ch.pack(fill="x", padx=16, pady=(0, 4))
        for lbl, w in [("#", 30), ("Column Name", 180), ("Type", 120), ("Nullable", 80), ("Collibra Name", 180)]:
            ctk.CTkLabel(ch, text=lbl, font=ctk.CTkFont("Consolas", 10, weight="bold"),
                         text_color=TXD, width=w, anchor="w").pack(side="left", padx=6, pady=8)
        # Rows
        for col in t.columns:
            cr = ctk.CTkFrame(self._detail_frame, fg_color="transparent")
            cr.pack(fill="x", padx=16)
            sep2 = ctk.CTkFrame(self._detail_frame, height=1, fg_color=BORD)
            sep2.pack(fill="x", padx=16)
            row_data = [
                (str(col.ordinal + 1), 30, TXD),
                (col.name,             180, TXT),
                (col.data_type,        120, PURP),
                ("NULL" if col.nullable else "NOT NULL", 80, TXD),
                (col.collibra_name,    180, ACCN),
            ]
            for v, w, c in row_data:
                ctk.CTkLabel(cr, text=v, font=ctk.CTkFont("Consolas", 11),
                             text_color=c, width=w, anchor="w"
                             ).pack(side="left", padx=6, pady=8)

    def _apply_name(self, t: TableMeta, name: str):
        t.collibra_name = name.strip()
        self.app.topbar.set_status(f"✔  Name updated: {name}", SUCC)

    def refresh(self):
        self._render_table_list()


# ═══════════════════════════════════════════════════════════════
#  PAGE: Collibra Ingestion
# ═══════════════════════════════════════════════════════════════
class CollibraPage(ctk.CTkFrame):
    def __init__(self, master, app, **kw):
        super().__init__(master, fg_color=BG, **kw)
        self.app = app
        self._communities: List[CollibraCommunity] = []
        self._domains: List[CollibraDomain] = []
        self._ingest_thread = None
        self._client        = None
        self._build()

    def _build(self):
        hdr = ctk.CTkFrame(self, fg_color="transparent")
        hdr.pack(fill="x", padx=32, pady=24)
        ctk.CTkLabel(hdr, text="Collibra Ingestion",
                     font=ctk.CTkFont("Consolas", 20, weight="bold"),
                     text_color=TXT).pack(side="left")

        sep = ctk.CTkFrame(self, height=1, fg_color=BORD)
        sep.pack(fill="x", padx=32, pady=(0, 20))

        scroll = ctk.CTkScrollableFrame(self, fg_color="transparent")
        scroll.pack(fill="both", expand=True, padx=32, pady=(0, 16))

        # ── Collibra credentials ───────────────────────────────
        cred_card = Card(scroll)
        cred_card.pack(fill="x", pady=(0, 20))
        ctk.CTkLabel(cred_card, text="Collibra Connection",
                     font=ctk.CTkFont("Consolas", 14, weight="bold"),
                     text_color=TXT).pack(anchor="w", padx=20, pady=(16, 8))

        r1 = ctk.CTkFrame(cred_card, fg_color="transparent")
        r1.pack(fill="x", padx=20, pady=(0, 8))
        ctk.CTkLabel(r1, text="Collibra URL",
                     font=ctk.CTkFont("Consolas", 11), text_color=TXS, width=150, anchor="w"
                     ).pack(side="left")
        self._url_entry = DataEntry(r1, "https://your-org.collibra.com", width=400)
        self._url_entry.insert(0, self.app.settings.get("collibra_url", ""))
        self._url_entry.pack(side="left")

        r2 = ctk.CTkFrame(cred_card, fg_color="transparent")
        r2.pack(fill="x", padx=20, pady=(0, 8))
        ctk.CTkLabel(r2, text="Username",
                     font=ctk.CTkFont("Consolas", 11), text_color=TXS, width=150, anchor="w"
                     ).pack(side="left")
        self._user_entry = DataEntry(r2, "admin", width=240)
        self._user_entry.insert(0, self.app.settings.get("collibra_username", ""))
        self._user_entry.pack(side="left")
        ctk.CTkLabel(r2, text="Password",
                     font=ctk.CTkFont("Consolas", 11), text_color=TXS
                     ).pack(side="left", padx=(20, 0))
        self._pass_entry = DataEntry(r2, "", show="*", width=200)
        self._pass_entry.insert(0, self.app.settings.get("collibra_password", ""))
        self._pass_entry.pack(side="left", padx=8)

        rb = ctk.CTkFrame(cred_card, fg_color="transparent")
        rb.pack(anchor="w", padx=20, pady=(8, 16))
        AccentButton(rb, "⚡ Test Connection",
                     command=self._test_collibra, width=160, color=TEAL).pack(side="left")
        AccentButton(rb, "⟳ Load Communities",
                     command=self._load_communities, width=160).pack(side="left", padx=8)
        self._collibra_status = ctk.CTkLabel(rb, text="",
                                              font=ctk.CTkFont("Consolas", 11))
        self._collibra_status.pack(side="left", padx=8)

        # ── Target ────────────────────────────────────────────
        target_card = Card(scroll)
        target_card.pack(fill="x", pady=(0, 20))
        ctk.CTkLabel(target_card, text="Target Community & Domain",
                     font=ctk.CTkFont("Consolas", 14, weight="bold"),
                     text_color=TXT).pack(anchor="w", padx=20, pady=(16, 8))

        t1 = ctk.CTkFrame(target_card, fg_color="transparent")
        t1.pack(fill="x", padx=20, pady=(0, 8))
        ctk.CTkLabel(t1, text="Community",
                     font=ctk.CTkFont("Consolas", 11), text_color=TXS, width=120
                     ).pack(side="left")
        self._comm_var = ctk.StringVar()
        self._comm_om  = ctk.CTkOptionMenu(t1, values=["— load communities first —"],
                                            variable=self._comm_var,
                                            command=self._on_comm_select,
                                            fg_color=CARD, button_color=BORD,
                                            button_hover_color=HOVER, text_color=TXT,
                                            font=ctk.CTkFont("Consolas", 12), width=350)
        self._comm_om.pack(side="left", padx=8)

        t2 = ctk.CTkFrame(target_card, fg_color="transparent")
        t2.pack(fill="x", padx=20, pady=(0, 8))
        ctk.CTkLabel(t2, text="Domain",
                     font=ctk.CTkFont("Consolas", 11), text_color=TXS, width=120
                     ).pack(side="left")
        self._dom_var = ctk.StringVar()
        self._dom_om  = ctk.CTkOptionMenu(t2, values=["— select community first —"],
                                           variable=self._dom_var,
                                           fg_color=CARD, button_color=BORD,
                                           button_hover_color=HOVER, text_color=TXT,
                                           font=ctk.CTkFont("Consolas", 12), width=350)
        self._dom_om.pack(side="left", padx=8)
        GhostButton(t2, "+ New Domain", command=self._new_domain, width=110, color=TEAL
                    ).pack(side="left", padx=8)

        # ── Options ────────────────────────────────────────────
        opt_card = Card(scroll)
        opt_card.pack(fill="x", pady=(0, 20))
        ctk.CTkLabel(opt_card, text="Ingestion Options",
                     font=ctk.CTkFont("Consolas", 14, weight="bold"),
                     text_color=TXT).pack(anchor="w", padx=20, pady=(16, 8))
        of = ctk.CTkFrame(opt_card, fg_color="transparent")
        of.pack(fill="x", padx=20, pady=(0, 16))
        self._ingest_cols_var = ctk.BooleanVar(value=True)
        ctk.CTkCheckBox(of, text="Ingest column metadata",
                        variable=self._ingest_cols_var,
                        font=ctk.CTkFont("Consolas", 12), text_color=TXS,
                        fg_color=ACCN).pack(anchor="w", pady=4)
        self._create_rels_var = ctk.BooleanVar(value=True)
        ctk.CTkCheckBox(of, text="Create hierarchy relations (DB → Schema → Table → Column)",
                        variable=self._create_rels_var,
                        font=ctk.CTkFont("Consolas", 12), text_color=TXS,
                        fg_color=ACCN).pack(anchor="w", pady=4)

        # ── Summary ───────────────────────────────────────────
        self._summary_card = Card(scroll)
        self._summary_card.pack(fill="x", pady=(0, 20))
        self._render_summary()

        # ── Ingest button ─────────────────────────────────────
        btn_row = ctk.CTkFrame(scroll, fg_color="transparent")
        btn_row.pack(fill="x", pady=(0, 12))
        self._ingest_btn = AccentButton(btn_row, "⬆  Ingest to Collibra",
                                         command=self._run_ingestion,
                                         width=200, color=C["collibra"])
        self._ingest_btn.pack(side="left")
        self._cancel_ing_btn = GhostButton(btn_row, "◼ Cancel",
                                            command=self._cancel_ingestion,
                                            width=100, color=ERR)
        self._cancel_ing_btn.pack(side="left", padx=8)
        self._cancel_ing_btn.configure(state="disabled")

        # ── Progress ──────────────────────────────────────────
        self._ingest_prog = ProgressCard(scroll, "Waiting to start…")
        self._ingest_prog.pack(fill="x", pady=(0, 8))

        # ── Log ───────────────────────────────────────────────
        SectionHeader(scroll, "INGESTION LOG").pack(anchor="w", pady=(8, 4))
        self._ingest_log = ctk.CTkTextbox(scroll, height=200,
                                           fg_color=C["bg_input"],
                                           text_color=TEAL,
                                           font=ctk.CTkFont("Consolas", 11),
                                           border_color=BORD, border_width=1,
                                           corner_radius=8)
        self._ingest_log.pack(fill="x", pady=(0, 24))

    def _render_summary(self):
        for w in self._summary_card.winfo_children(): w.destroy()
        scan = self.app.current_scan
        ctk.CTkLabel(self._summary_card, text="Ingestion Summary",
                     font=ctk.CTkFont("Consolas", 14, weight="bold"),
                     text_color=TXT).pack(anchor="w", padx=20, pady=(16, 8))
        if not scan:
            ctk.CTkLabel(self._summary_card,
                         text="No scan data available. Run a scan first.",
                         font=ctk.CTkFont("Consolas", 12), text_color=TXD
                         ).pack(anchor="w", padx=20, pady=(0, 16))
            return
        selected = [t for t in scan.tables if t.selected]
        cols     = sum(t.col_count for t in selected)
        row = ctk.CTkFrame(self._summary_card, fg_color="transparent")
        row.pack(anchor="w", padx=20, pady=(0, 16))
        for val, lbl, col in [
            (str(len(selected)),  "tables selected", ACCN),
            (str(cols),           "columns",         TEAL),
            (str(len(scan.tables) - len(selected)), "excluded", TXD),
        ]:
            f = ctk.CTkFrame(row, fg_color=CARD, corner_radius=8)
            f.pack(side="left", padx=(0, 12))
            ctk.CTkLabel(f, text=val,
                         font=ctk.CTkFont("Consolas", 22, weight="bold"),
                         text_color=col).pack(padx=16, pady=(10, 2))
            ctk.CTkLabel(f, text=lbl,
                         font=ctk.CTkFont("Consolas", 10),
                         text_color=TXD).pack(padx=16, pady=(0, 10))

    def _get_client(self):
        from app.collibra.client import CollibraClient
        url  = self._url_entry.get().strip()
        user = self._user_entry.get().strip()
        pw   = self._pass_entry.get().strip()
        if not url: raise ValueError("Collibra URL is required.")
        return CollibraClient(url, user, pw)

    def _test_collibra(self):
        self._collibra_status.configure(text="Testing…", text_color=AMBN)
        def run():
            try:
                c = self._get_client()
                ok, msg = c.test_connection()
                color = SUCC if ok else ERR
                self.after(0, lambda: self._collibra_status.configure(
                    text=f"{'✔' if ok else '✖'}  {msg}", text_color=color))
            except Exception as e:
                self.after(0, lambda: self._collibra_status.configure(
                    text=f"✖  {e}", text_color=ERR))
        threading.Thread(target=run, daemon=True).start()

    def _load_communities(self):
        def run():
            try:
                c = self._get_client()
                self._client = c
                comms = c.get_communities()
                self._communities = comms
                names = [cm.name for cm in comms]
                self.after(0, lambda: self._comm_om.configure(values=names or ["— none —"]))
                if names: self.after(0, lambda: self._comm_var.set(names[0]))
                self.after(0, lambda: self._collibra_status.configure(
                    text=f"✔  {len(comms)} communities loaded", text_color=SUCC))
            except Exception as e:
                self.after(0, lambda: self._collibra_status.configure(
                    text=f"✖  {e}", text_color=ERR))
        threading.Thread(target=run, daemon=True).start()

    def _on_comm_select(self, comm_name: str):
        comm = next((c for c in self._communities if c.name == comm_name), None)
        if not comm or not self._client: return
        def run():
            try:
                doms = self._client.get_domains(comm.id)
                self._domains = doms
                names = [d.name for d in doms]
                self.after(0, lambda: self._dom_om.configure(values=names or ["— no domains —"]))
                if names: self.after(0, lambda: self._dom_var.set(names[0]))
            except Exception as e:
                self.after(0, lambda: self.app.topbar.set_status(f"✖  {e}", ERR))
        threading.Thread(target=run, daemon=True).start()

    def _new_domain(self):
        dialog = ctk.CTkInputDialog(text="Enter new domain name:", title="New Domain")
        name = dialog.get_input()
        if not name or not name.strip(): return
        comm = next((c for c in self._communities if c.name == self._comm_var.get()), None)
        if not comm or not self._client:
            messagebox.showerror("Error", "Load communities first.")
            return
        def run():
            try:
                did = self._client.get_or_create_domain(comm.id, name.strip())
                self.after(0, self._on_comm_select, self._comm_var.get())
                self.after(0, lambda: self.app.topbar.set_status(f"✔  Domain '{name}' created.", SUCC))
            except Exception as e:
                self.after(0, lambda: messagebox.showerror("Error", str(e)))
        threading.Thread(target=run, daemon=True).start()

    def _ingest_log_msg(self, msg: str):
        self._ingest_log.configure(state="normal")
        self._ingest_log.insert("end", f"[{datetime.now().strftime('%H:%M:%S')}]  {msg}\n")
        self._ingest_log.see("end")
        self._ingest_log.configure(state="disabled")

    def _run_ingestion(self):
        scan = self.app.current_scan
        if not scan or not scan.tables:
            messagebox.showerror("Ingestion", "No scan data. Run a scan first.")
            return
        comm_name = self._comm_var.get()
        dom_name  = self._dom_var.get()
        if "—" in comm_name or "—" in dom_name:
            messagebox.showerror("Ingestion", "Select a community and domain.")
            return

        comm = next((c for c in self._communities if c.name == comm_name), None)
        dom  = next((d for d in self._domains if d.name == dom_name), None)
        if not comm or not dom:
            messagebox.showerror("Ingestion", "Invalid community or domain selection.")
            return

        # Save settings
        s = self.app.settings
        s["collibra_url"]      = self._url_entry.get().strip()
        s["collibra_username"] = self._user_entry.get().strip()
        s["collibra_password"] = self._pass_entry.get().strip()
        storage.save_settings(s)

        self._ingest_btn.configure(state="disabled")
        self._cancel_ing_btn.configure(state="normal")
        self._ingest_log_msg(f"Starting ingestion → {comm_name} / {dom_name}")
        self._render_summary()

        def run():
            from app.models import IngestionResult
            from app.collibra.client import CollibraClient
            result = IngestionResult(
                scan_id         = scan.scan_id,
                collibra_url    = s["collibra_url"],
                community_name  = comm_name,
                domain_name     = dom_name,
            )
            client = CollibraClient(s["collibra_url"], s["collibra_username"], s["collibra_password"])
            self._client = client

            selected = [t for t in scan.tables if t.selected]
            total    = len(selected)

            def prog_cb(msg, cur, tot):
                frac = (cur / max(tot, 1)) if tot else 0
                self.after(0, lambda: self._ingest_prog.update_progress(
                    msg, frac, f"{cur}/{tot}"))
                self.after(0, lambda: self._ingest_log_msg(msg))

            try:
                client.ingest_tables(
                    tables       = selected,
                    community_id = comm.id,
                    domain_id    = dom.id,
                    result       = result,
                    progress_cb  = prog_cb,
                    ingest_cols  = self._ingest_cols_var.get(),
                )
                summary = (f"✔  Ingestion complete — "
                           f"{result.assets_created} created, "
                           f"{result.assets_updated} updated, "
                           f"{result.assets_failed} failed, "
                           f"{result.relations_created} relations")
                self.after(0, lambda: self._ingest_log_msg(summary))
                self.after(0, lambda: self.app.topbar.set_status(summary[:80], SUCC))
            except Exception as e:
                self.after(0, lambda: self._ingest_log_msg(f"✖  {e}"))
                self.after(0, lambda: self.app.topbar.set_status(f"✖  Ingestion error", ERR))
            finally:
                self.after(0, lambda: self._ingest_btn.configure(state="normal"))
                self.after(0, lambda: self._cancel_ing_btn.configure(state="disabled"))

        self._ingest_thread = threading.Thread(target=run, daemon=True)
        self._ingest_thread.start()

    def _cancel_ingestion(self):
        if self._client: self._client.cancel()
        self._ingest_log_msg("⚠  Ingestion cancelled.")
        self._cancel_ing_btn.configure(state="disabled")


# ═══════════════════════════════════════════════════════════════
#  PAGE: Logs
# ═══════════════════════════════════════════════════════════════
class LogsPage(ctk.CTkScrollableFrame):
    def __init__(self, master, app, **kw):
        super().__init__(master, fg_color=BG, **kw)
        self.app = app
        self._build()

    def _build(self):
        hdr = ctk.CTkFrame(self, fg_color="transparent")
        hdr.pack(fill="x", padx=32, pady=24)
        ctk.CTkLabel(hdr, text="Audit Log & Scan History",
                     font=ctk.CTkFont("Consolas", 20, weight="bold"),
                     text_color=TXT).pack(side="left")
        GhostButton(hdr, "⟳ Refresh", command=self.refresh, width=100).pack(side="right")

        sep = ctk.CTkFrame(self, height=1, fg_color=BORD)
        sep.pack(fill="x", padx=32, pady=(0, 20))
        self._render()

    def _render(self):
        for w in self.winfo_children()[2:]: w.destroy()
        scans = storage.load_scan_history()
        if not scans:
            ctk.CTkLabel(self, text="No scan history yet.",
                         font=ctk.CTkFont("Consolas", 13), text_color=TXD
                         ).pack(pady=40)
            return

        headers = ["Scan ID", "Connection", "Type", "Started", "Finished", "Tables", "Columns", "Status"]
        col_ws  = [120, 160, 100, 160, 160, 80, 90, 90]

        card = Card(self)
        card.pack(fill="x", padx=32, pady=(0, 24))
        hrow = ctk.CTkFrame(card, fg_color=HOVER, corner_radius=6)
        hrow.pack(fill="x", padx=12, pady=(12, 4))
        for h, w in zip(headers, col_ws):
            ctk.CTkLabel(hrow, text=h.upper(),
                         font=ctk.CTkFont("Consolas", 10, weight="bold"),
                         text_color=TXD, width=w, anchor="w").pack(side="left", padx=6, pady=8)

        for s in scans:
            row = ctk.CTkFrame(card, fg_color="transparent")
            row.pack(fill="x", padx=12)
            ctk.CTkFrame(card, height=1, fg_color=BORD).pack(fill="x", padx=12)
            st = s.get("status","")
            sc = SUCC if st == "complete" else ERR if st == "error" else AMBN
            vals = [
                s.get("scan_id","")[:12],
                s.get("connection_name",""),
                s.get("source_type","").upper(),
                s.get("started_at","")[:16].replace("T"," "),
                (s.get("finished_at","") or "")[:16].replace("T"," "),
                str(s.get("table_count",0)),
                str(s.get("column_count",0)),
                st.upper(),
            ]
            for v, w, c in zip(vals, col_ws,
                               [TXD,TXT,TXS,TXD,TXD,TEAL,TXS,sc]):
                ctk.CTkLabel(row, text=v, font=ctk.CTkFont("Consolas", 11),
                             text_color=c, width=w, anchor="w"
                             ).pack(side="left", padx=6, pady=10)

    def refresh(self):
        self._render()


# ═══════════════════════════════════════════════════════════════
#  PAGE: Settings
# ═══════════════════════════════════════════════════════════════
class SettingsPage(ctk.CTkScrollableFrame):
    def __init__(self, master, app, **kw):
        super().__init__(master, fg_color=BG, **kw)
        self.app = app
        self._entries: Dict[str, ctk.CTkEntry] = {}
        self._build()

    def _build(self):
        ctk.CTkLabel(self, text="Settings",
                     font=ctk.CTkFont("Consolas", 20, weight="bold"),
                     text_color=TXT).pack(anchor="w", padx=32, pady=(24, 4))
        ctk.CTkLabel(self, text="All settings are persisted to ~/.metaharvest/settings.json",
                     font=ctk.CTkFont("Consolas", 11), text_color=TXD
                     ).pack(anchor="w", padx=32, pady=(0, 12))
        ctk.CTkFrame(self, height=1, fg_color=BORD).pack(fill="x", padx=32, pady=(0, 20))

        s = self.app.settings

        # Collibra
        self._section("Collibra Defaults")
        self._row("Collibra URL",       "collibra_url",      s.get("collibra_url",""),
                  "https://your-org.collibra.com")
        self._row("Username",           "collibra_username", s.get("collibra_username",""), "admin")
        self._row("Password",           "collibra_password", s.get("collibra_password",""), "", show="*")
        self._row("Default Community",  "default_community", s.get("default_community",""), "")
        self._row("Default Domain",     "default_domain",    s.get("default_domain",""),    "")

        # Scanner
        self._section("Scanner Defaults")
        self._row("Scan Timeout (sec)", "scan_timeout",      str(s.get("scan_timeout", 300)), "300")

        # Name transform
        self._section("Name Transformation")
        self._bool_row("Auto-transform names for Collibra", "auto_transform_names",
                        s.get("auto_transform_names", True))
        self._bool_row("Ingest column metadata by default", "ingest_columns",
                        s.get("ingest_columns", True))

        # Save
        AccentButton(self, "💾  Save Settings", command=self._save, width=180).pack(
            anchor="w", padx=32, pady=20)

        # Data dir info
        info = Card(self)
        info.pack(fill="x", padx=32, pady=(0, 32))
        ctk.CTkLabel(info,
                     text=f"Data Directory:  ~/.metaharvest/",
                     font=ctk.CTkFont("Consolas", 11), text_color=TXD
                     ).pack(anchor="w", padx=20, pady=12)
        GhostButton(info, "🗑 Clear Scan History",
                    command=self._clear_history, color=ERR, width=180).pack(anchor="w", padx=20, pady=(0, 12))

    def _section(self, label: str):
        SectionHeader(self, label).pack(anchor="w", padx=32, pady=(16, 8))

    def _row(self, label, key, value="", placeholder="", show=""):
        f = ctk.CTkFrame(self, fg_color="transparent")
        f.pack(fill="x", padx=32, pady=(0, 8))
        ctk.CTkLabel(f, text=label,
                     font=ctk.CTkFont("Consolas", 11), text_color=TXS, width=200, anchor="w"
                     ).pack(side="left")
        e = DataEntry(f, placeholder, show=show, width=400)
        if value: e.insert(0, value)
        e.pack(side="left")
        self._entries[key] = e

    def _bool_row(self, label, key, value=True):
        f = ctk.CTkFrame(self, fg_color="transparent")
        f.pack(fill="x", padx=32, pady=(0, 8))
        var = ctk.BooleanVar(value=value)
        ctk.CTkCheckBox(f, text=label, variable=var,
                        font=ctk.CTkFont("Consolas", 12), text_color=TXS,
                        fg_color=ACCN, hover_color=C["accent_dim"]).pack(side="left")
        self._entries[key] = var  # store BooleanVar

    def _save(self):
        s = self.app.settings
        for k, widget in self._entries.items():
            if isinstance(widget, ctk.BooleanVar):
                s[k] = widget.get()
            else:
                s[k] = widget.get().strip()
        storage.save_settings(s)
        self.app.topbar.set_status("✔  Settings saved.", SUCC)

    def _clear_history(self):
        if messagebox.askyesno("Confirm", "Clear all scan history?"):
            import app.storage as st
            st.SCANS_FILE.write_text("[]")
            self.app.topbar.set_status("✔  Scan history cleared.", SUCC)


# ═══════════════════════════════════════════════════════════════
#  Main Application
# ═══════════════════════════════════════════════════════════════
class MetaHarvestApp(ctk.CTk):
    def __init__(self):
        super().__init__()
        self.title(f"{APP_NAME}  ·  {APP_SUBTITLE}")
        self.geometry("1400x860")
        self.minsize(1100, 700)
        self.configure(fg_color=BG)

        self.settings     = storage.load_settings()
        self.current_scan: Optional[ScanResult] = None

        self._pages: Dict[str, ctk.CTkFrame] = {}
        self._build_layout()
        self._show_page("dashboard")

    def _build_layout(self):
        # Sidebar
        self.sidebar = Sidebar(self, on_nav=self._show_page)
        self.sidebar.pack(side="left", fill="y")

        # Right area
        right = ctk.CTkFrame(self, fg_color=BG, corner_radius=0)
        right.pack(side="left", fill="both", expand=True)

        # Top bar
        self.topbar = TopBar(right)
        self.topbar.pack(fill="x")

        sep = ctk.CTkFrame(right, height=1, fg_color=BORD)
        sep.pack(fill="x")

        # Page container
        self._page_container = ctk.CTkFrame(right, fg_color=BG, corner_radius=0)
        self._page_container.pack(fill="both", expand=True)

    def _show_page(self, key: str):
        # Lazy-init pages
        if key not in self._pages:
            cls_map = {
                "dashboard":   DashboardPage,
                "connections": ConnectionsPage,
                "scanner":     ScannerPage,
                "preview":     PreviewPage,
                "collibra":    CollibraPage,
                "logs":        LogsPage,
                "settings":    SettingsPage,
            }
            cls = cls_map.get(key)
            if cls:
                pg = cls(self._page_container, app=self)
                pg.place(relx=0, rely=0, relwidth=1, relheight=1)
                self._pages[key] = pg

        # Hide all, show selected
        for k, pg in self._pages.items():
            pg.place_forget()

        page = self._pages.get(key)
        if page:
            page.place(relx=0, rely=0, relwidth=1, relheight=1)
            # Refresh dashboard & preview on each visit
            if key == "dashboard" and hasattr(page, "refresh"):
                page.refresh()
            if key == "preview" and hasattr(page, "refresh"):
                page.refresh()

        labels = {
            "dashboard":   "Dashboard",
            "connections": "Data Source Connections",
            "scanner":     "Metadata Scanner",
            "preview":     "Metadata Preview & Editing",
            "collibra":    "Collibra Ingestion",
            "logs":        "Audit Logs",
            "settings":    "Settings",
        }
        self.topbar.set_title(labels.get(key, key.title()))
