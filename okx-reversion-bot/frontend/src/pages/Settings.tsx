import { useEffect, useState } from "react";

import {
  api,
  type CredentialStatus,
  type OKXCredentialIn,
  type TestConnectionResult,
  type TradeMode,
} from "../api";

type Toast = { kind: "success" | "error" | "info"; text: string } | null;

export default function Settings() {
  const [creds, setCreds] = useState<CredentialStatus[]>([]);
  const [mode, setMode] = useState<TradeMode>("paper");
  const [okxForm, setOkxForm] = useState<OKXCredentialIn>({
    api_key: "",
    api_secret: "",
    passphrase: "",
    environment: "live",
  });
  const [perplexityKey, setPerplexityKey] = useState("");
  const [toast, setToast] = useState<Toast>(null);
  const [busy, setBusy] = useState(false);

  const refresh = async () => {
    try {
      const [list, modeOut] = await Promise.all([
        api.settings.listCredentials(),
        api.settings.getMode(),
      ]);
      setCreds(list);
      setMode(modeOut.mode);
    } catch (e) {
      setToast({ kind: "error", text: (e as Error).message });
    }
  };

  useEffect(() => {
    refresh();
  }, []);

  const okxStatus = creds.find((c) => c.provider === "okx");
  const perplexityStatus = creds.find((c) => c.provider === "perplexity");

  const saveOkx = async () => {
    setBusy(true);
    setToast(null);
    try {
      await api.settings.saveOKX(okxForm);
      setToast({ kind: "success", text: "OKX credentials saved (encrypted)." });
      setOkxForm({ ...okxForm, api_key: "", api_secret: "", passphrase: "" });
      await refresh();
    } catch (e) {
      setToast({ kind: "error", text: (e as Error).message });
    } finally {
      setBusy(false);
    }
  };

  const savePerplexity = async () => {
    setBusy(true);
    setToast(null);
    try {
      await api.settings.savePerplexity({ api_key: perplexityKey });
      setToast({ kind: "success", text: "Perplexity key saved (encrypted)." });
      setPerplexityKey("");
      await refresh();
    } catch (e) {
      setToast({ kind: "error", text: (e as Error).message });
    } finally {
      setBusy(false);
    }
  };

  const removeCred = async (provider: string) => {
    setBusy(true);
    try {
      await api.settings.deleteCredentials(provider);
      setToast({ kind: "info", text: `${provider} credentials removed.` });
      await refresh();
    } catch (e) {
      setToast({ kind: "error", text: (e as Error).message });
    } finally {
      setBusy(false);
    }
  };

  const testOkx = async () => {
    setBusy(true);
    try {
      const r: TestConnectionResult = await api.settings.testOKX();
      setToast({
        kind: r.status.includes("ok") ? "success" : "info",
        text: `${r.status}: ${r.detail}`,
      });
    } catch (e) {
      setToast({ kind: "error", text: (e as Error).message });
    } finally {
      setBusy(false);
    }
  };

  const updateMode = async (next: TradeMode) => {
    setBusy(true);
    try {
      const r = await api.settings.setMode(next);
      setMode(r.mode);
      setToast({ kind: "success", text: `Trade mode set to ${r.mode}.` });
    } catch (e) {
      setToast({ kind: "error", text: (e as Error).message });
    } finally {
      setBusy(false);
    }
  };

  return (
    <>
      <h1 className="page-title">Settings</h1>
      <p className="page-subtitle">
        Credentials are encrypted at rest with the master key in <code>data/.master.key</code>.
        They are never returned in plaintext after saving.
      </p>

      {toast && <div className={`toast toast-${toast.kind}`}>{toast.text}</div>}

      <div className="card">
        <h2 className="card-title">
          Trade mode
          <span className="card-meta">
            Live trading additionally requires a per-session unlock (phase 4).
          </span>
        </h2>
        <div className="form-actions">
          {(["paper", "demo", "live"] as TradeMode[]).map((m) => (
            <button
              key={m}
              className={`btn ${mode === m ? "btn-primary" : ""}`}
              onClick={() => updateMode(m)}
              disabled={busy}
            >
              {m}
            </button>
          ))}
        </div>
      </div>

      <div className="card">
        <h2 className="card-title">
          OKX API
          <span className="card-meta">
            {okxStatus?.configured
              ? `Configured · ends in …${okxStatus.key_hint} · ${okxStatus.environment}`
              : "Not configured"}
          </span>
        </h2>
        <div className="form-grid">
          <div className="form-field">
            <label>API key</label>
            <input
              type="password"
              autoComplete="off"
              value={okxForm.api_key}
              onChange={(e) => setOkxForm({ ...okxForm, api_key: e.target.value })}
              placeholder="•••••••••••••"
            />
          </div>
          <div className="form-field">
            <label>API secret</label>
            <input
              type="password"
              autoComplete="off"
              value={okxForm.api_secret}
              onChange={(e) => setOkxForm({ ...okxForm, api_secret: e.target.value })}
            />
          </div>
          <div className="form-field">
            <label>Passphrase</label>
            <input
              type="password"
              autoComplete="off"
              value={okxForm.passphrase}
              onChange={(e) => setOkxForm({ ...okxForm, passphrase: e.target.value })}
            />
          </div>
          <div className="form-field">
            <label>Environment</label>
            <select
              value={okxForm.environment}
              onChange={(e) =>
                setOkxForm({
                  ...okxForm,
                  environment: e.target.value as "live" | "demo",
                })
              }
            >
              <option value="live">Live</option>
              <option value="demo">Demo (paper-trading)</option>
            </select>
          </div>
        </div>
        <div className="form-actions">
          <button
            className="btn btn-primary"
            onClick={saveOkx}
            disabled={
              busy ||
              !okxForm.api_key ||
              !okxForm.api_secret ||
              !okxForm.passphrase
            }
          >
            Save credentials
          </button>
          <button className="btn" onClick={testOkx} disabled={busy || !okxStatus?.configured}>
            Test connection
          </button>
          {okxStatus?.configured && (
            <button
              className="btn btn-danger"
              onClick={() => removeCred("okx")}
              disabled={busy}
            >
              Remove
            </button>
          )}
        </div>
      </div>

      <div className="card">
        <h2 className="card-title">
          Perplexity API
          <span className="card-meta">
            {perplexityStatus?.configured
              ? `Configured · ends in …${perplexityStatus.key_hint}`
              : "Optional · used for market context only"}
          </span>
        </h2>
        <div className="form-grid">
          <div className="form-field full">
            <label>API key</label>
            <input
              type="password"
              autoComplete="off"
              value={perplexityKey}
              onChange={(e) => setPerplexityKey(e.target.value)}
            />
          </div>
        </div>
        <div className="form-actions">
          <button
            className="btn btn-primary"
            onClick={savePerplexity}
            disabled={busy || !perplexityKey}
          >
            Save key
          </button>
          {perplexityStatus?.configured && (
            <button
              className="btn btn-danger"
              onClick={() => removeCred("perplexity")}
              disabled={busy}
            >
              Remove
            </button>
          )}
        </div>
      </div>
    </>
  );
}
