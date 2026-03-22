const API_BASE = process.env.NEXT_PUBLIC_API_URL || "http://localhost:8080";

// ---------- Types ----------

export interface Hypothesis {
  id: string;
  name: string;
  generator: string;
  tolerance: Record<string, unknown>;
  status: "idle" | "running" | "passed" | "failed" | "stopped";
  created: string;
  last_run: string | null;
}

export interface RunStatus {
  hypothesis_id: string;
  status: "idle" | "running" | "passed" | "failed" | "stopped";
  ops: number;
  batches: number;
  checkpoints: number;
  failures: number;
  started_at: string | null;
  elapsed_ms: number;
}

export interface HypothesisEvent {
  id: string;
  hypothesis_id: string;
  kind: string;
  message: string;
  timestamp: string;
}

export interface Generator {
  id: string;
  name: string;
  description: string;
  rate: number;
  workload: Record<string, unknown>;
  builtin: boolean;
  created: string;
}

export interface Adapter {
  id: string;
  name: string;
  image: string;
  env: Record<string, string>;
  created: string;
}

export interface DashboardStats {
  total: number;
  running: number;
  passed: number;
  failed: number;
}

// ---------- Helpers ----------

async function request<T>(path: string, init?: RequestInit): Promise<T> {
  const res = await fetch(`${API_BASE}${path}`, {
    ...init,
    headers: {
      "Content-Type": "application/json",
      ...init?.headers,
    },
  });
  if (!res.ok) {
    const text = await res.text().catch(() => "");
    throw new Error(`API ${res.status}: ${text}`);
  }
  // Handle 204 or empty body
  const text = await res.text();
  if (!text) return undefined as unknown as T;
  return JSON.parse(text) as T;
}

// ---------- Hypotheses ----------

export async function listHypotheses(): Promise<Hypothesis[]> {
  return request<Hypothesis[]>("/api/hypotheses");
}

export async function createHypothesis(body: {
  name: string;
  generator: string;
  tolerance: Record<string, unknown>;
}): Promise<Hypothesis> {
  return request<Hypothesis>("/api/hypotheses", {
    method: "POST",
    body: JSON.stringify(body),
  });
}

export async function getHypothesis(id: string): Promise<Hypothesis> {
  return request<Hypothesis>(`/api/hypotheses/${id}`);
}

export async function deleteHypothesis(id: string): Promise<void> {
  return request<void>(`/api/hypotheses/${id}`, { method: "DELETE" });
}

// ---------- Runs ----------

export async function generateOrigin(id: string): Promise<void> {
  return request<void>(`/api/hypotheses/${id}/origin`, { method: "POST" });
}

export async function startRun(
  id: string,
  body: {
    adapter: string;
    batch_size: number;
    checkpoint_every: number;
    duration: string;
  }
): Promise<void> {
  return request<void>(`/api/hypotheses/${id}/run`, {
    method: "POST",
    body: JSON.stringify(body),
  });
}

export async function stopRun(id: string): Promise<void> {
  return request<void>(`/api/hypotheses/${id}/run`, { method: "DELETE" });
}

export async function getStatus(id: string): Promise<RunStatus> {
  return request<RunStatus>(`/api/hypotheses/${id}/status`);
}

export async function getEvents(id: string): Promise<HypothesisEvent[]> {
  return request<HypothesisEvent[]>(`/api/hypotheses/${id}/events`);
}

export async function downloadBundle(id: string): Promise<Blob> {
  const res = await fetch(`${API_BASE}/api/hypotheses/${id}/bundle`);
  if (!res.ok) throw new Error(`API ${res.status}`);
  return res.blob();
}

// ---------- Generators ----------

export async function listGenerators(): Promise<Generator[]> {
  return request<Generator[]>("/api/generators");
}

export async function createGenerator(body: {
  name: string;
  description: string;
  rate: number;
  workload: Record<string, unknown>;
}): Promise<Generator> {
  return request<Generator>("/api/generators", {
    method: "POST",
    body: JSON.stringify(body),
  });
}

export async function deleteGenerator(id: string): Promise<void> {
  return request<void>(`/api/generators/${id}`, { method: "DELETE" });
}

// ---------- Adapters ----------

export async function listAdapters(): Promise<Adapter[]> {
  return request<Adapter[]>("/api/adapters");
}

export async function createAdapter(body: {
  name: string;
  image: string;
  env: Record<string, string>;
}): Promise<Adapter> {
  return request<Adapter>("/api/adapters", {
    method: "POST",
    body: JSON.stringify(body),
  });
}

export async function deleteAdapter(id: string): Promise<void> {
  return request<void>(`/api/adapters/${id}`, { method: "DELETE" });
}
