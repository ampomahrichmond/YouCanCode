export type TradeMode = "paper" | "demo" | "live";

export interface Health {
  status: string;
  version: string;
  trade_mode: TradeMode;
  paused: boolean;
  database: string;
}

export interface BotStatus {
  paused: boolean;
  trade_mode: TradeMode;
  last_change_at: string;
  last_change_by: string;
}

export interface CredentialStatus {
  provider: string;
  configured: boolean;
  key_hint: string;
  environment: string;
}

export interface OKXCredentialIn {
  api_key: string;
  api_secret: string;
  passphrase: string;
  environment: "live" | "demo";
}

export interface PerplexityCredentialIn {
  api_key: string;
}

export interface TestConnectionResult {
  status: string;
  detail: string;
  environment?: string;
  mode?: string;
}

async function request<T>(path: string, init?: RequestInit): Promise<T> {
  const res = await fetch(`/api${path}`, {
    headers: { "Content-Type": "application/json" },
    ...init,
  });
  if (!res.ok) {
    let detail = res.statusText;
    try {
      const body = await res.json();
      detail = body?.detail ?? detail;
    } catch {
      /* ignore */
    }
    throw new Error(`${res.status}: ${detail}`);
  }
  if (res.status === 204) return undefined as T;
  return (await res.json()) as T;
}

export const api = {
  health: () => request<Health>("/health"),
  bot: {
    status: () => request<BotStatus>("/bot/status"),
    pause: () => request<BotStatus>("/bot/pause", { method: "POST" }),
    resume: () => request<BotStatus>("/bot/resume", { method: "POST" }),
  },
  settings: {
    listCredentials: () => request<CredentialStatus[]>("/settings/credentials"),
    saveOKX: (body: OKXCredentialIn) =>
      request<CredentialStatus>("/settings/credentials/okx", {
        method: "PUT",
        body: JSON.stringify(body),
      }),
    savePerplexity: (body: PerplexityCredentialIn) =>
      request<CredentialStatus>("/settings/credentials/perplexity", {
        method: "PUT",
        body: JSON.stringify(body),
      }),
    deleteCredentials: (provider: string) =>
      request<void>(`/settings/credentials/${provider}`, { method: "DELETE" }),
    testOKX: () =>
      request<TestConnectionResult>("/settings/credentials/okx/test", { method: "POST" }),
    getMode: () => request<{ mode: TradeMode; paused: boolean }>("/settings/mode"),
    setMode: (mode: TradeMode) =>
      request<{ mode: TradeMode; paused: boolean }>("/settings/mode", {
        method: "PUT",
        body: JSON.stringify({ mode }),
      }),
  },
};
