import { useEffect, useState } from "react";
import { NavLink, Outlet } from "react-router-dom";

import { api, type BotStatus } from "../api";

const links = [
  { to: "/", label: "Dashboard", end: true },
  { to: "/positions", label: "Positions" },
  { to: "/trades", label: "Trades" },
  { to: "/settings", label: "Settings" },
];

export default function Layout() {
  const [status, setStatus] = useState<BotStatus | null>(null);

  useEffect(() => {
    let cancelled = false;
    const tick = async () => {
      try {
        const s = await api.bot.status();
        if (!cancelled) setStatus(s);
      } catch {
        if (!cancelled) setStatus(null);
      }
    };
    tick();
    const id = setInterval(tick, 5000);
    return () => {
      cancelled = true;
      clearInterval(id);
    };
  }, []);

  const modeClass = status ? `pill pill-${status.trade_mode}` : "pill pill-paper";
  const stateClass = status?.paused ? "pill pill-paused" : "pill pill-running";

  return (
    <div className="layout">
      <aside className="sidebar">
        <div className="sidebar-brand">
          <span className="sidebar-brand-dot" />
          OKX Reversion
        </div>
        {links.map((l) => (
          <NavLink
            key={l.to}
            to={l.to}
            end={l.end}
            className={({ isActive }) => `sidebar-link${isActive ? " active" : ""}`}
          >
            {l.label}
          </NavLink>
        ))}
        <div className="statusbar">
          <div className="statusbar-row">
            <span>Mode</span>
            <span className={modeClass}>{status?.trade_mode ?? "—"}</span>
          </div>
          <div className="statusbar-row">
            <span>State</span>
            <span className={stateClass}>{status?.paused ? "Paused" : "Running"}</span>
          </div>
        </div>
      </aside>
      <main className="main">
        <Outlet />
      </main>
    </div>
  );
}
