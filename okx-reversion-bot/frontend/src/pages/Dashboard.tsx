import { useEffect, useState } from "react";

import { api, type BotStatus, type Health } from "../api";

export default function Dashboard() {
  const [health, setHealth] = useState<Health | null>(null);
  const [status, setStatus] = useState<BotStatus | null>(null);
  const [busy, setBusy] = useState(false);
  const [error, setError] = useState<string | null>(null);

  const refresh = async () => {
    setError(null);
    try {
      const [h, s] = await Promise.all([api.health(), api.bot.status()]);
      setHealth(h);
      setStatus(s);
    } catch (e) {
      setError((e as Error).message);
    }
  };

  useEffect(() => {
    refresh();
  }, []);

  const toggle = async () => {
    if (!status) return;
    setBusy(true);
    setError(null);
    try {
      const next = status.paused ? await api.bot.resume() : await api.bot.pause();
      setStatus(next);
    } catch (e) {
      setError((e as Error).message);
    } finally {
      setBusy(false);
    }
  };

  return (
    <>
      <h1 className="page-title">Dashboard</h1>
      <p className="page-subtitle">
        Phase 1 scaffold. Live market data, signals, and PnL arrive in phase 2+.
      </p>

      {error && <div className="toast toast-error">{error}</div>}

      <div className="tile-grid">
        <div className="tile">
          <div className="tile-label">Trade mode</div>
          <div className="tile-value">{status?.trade_mode ?? "—"}</div>
        </div>
        <div className="tile">
          <div className="tile-label">State</div>
          <div className="tile-value">{status?.paused ? "Paused" : "Running"}</div>
        </div>
        <div className="tile">
          <div className="tile-label">Open positions</div>
          <div className="tile-value">0</div>
        </div>
        <div className="tile">
          <div className="tile-label">Realized PnL (24h)</div>
          <div className="tile-value">$0.00</div>
        </div>
      </div>

      <div className="card">
        <h2 className="card-title">
          Bot control
          <span className="card-meta">
            v{health?.version ?? "—"} · {health?.database ?? "—"}
          </span>
        </h2>
        <div className="form-actions">
          <button className="btn btn-primary" onClick={toggle} disabled={busy || !status}>
            {status?.paused ? "Resume bot" : "Pause bot"}
          </button>
          <button className="btn" onClick={refresh} disabled={busy}>
            Refresh
          </button>
        </div>
      </div>

      <div className="card">
        <h2 className="card-title">Active signals</h2>
        <div className="empty">No signals yet — the strategy engine ships in phase 3.</div>
      </div>
    </>
  );
}
