// API client — calls through Next.js proxy at /api/* → pilot

// ---------- Types ----------

export interface Hypothesis {
  id: string;
  name: string;
  generator: string;
  state_machine: Record<string, unknown>;
  tolerance?: Record<string, unknown>;
  status: "idle" | "running" | "passed" | "failed" | "stopped";
  created_at: string;
  last_run_at: string | null;
}

export interface ProgressInfo {
  total_ops: number;
  completed_ops: number;
  total_batches: number;
  completed_batches: number;
  total_checkpoints: number;
  passed_checkpoints: number;
  failed_checkpoints: number;
  failed_response_ops: number;
  elapsed_secs: number;
  ops_per_sec: number;
}

export interface StatusResponse {
  hypothesis_id: string;
  status: string;
  run_id?: string;
  progress?: ProgressInfo;
}

export interface RunResult {
  id: string;
  run_id: string;
  total_batches: number;
  total_checkpoints: number;
  passed_checkpoints: number;
  failed_checkpoints: number;
  total_response_ops: number;
  failed_response_ops: number;
  stop_reason: string;
  started_at: string | null;
  finished_at: string | null;
}

export interface Generator {
  name: string;
  description: string;
  workload: Record<string, number>;
  rate: number;
  builtin: boolean;
  created_at: string;
}

export interface Adapter {
  id: string;
  name: string;
  image: string;
  env: Record<string, string>;
  created_at: string;
}

// ---------- Helpers ----------

async function request<T>(path: string, init?: RequestInit): Promise<T> {
  const res = await fetch(path, {
    ...init,
    headers: { "Content-Type": "application/json", ...init?.headers },
  });
  if (!res.ok) {
    const text = await res.text().catch(() => "");
    throw new Error(`API ${res.status}: ${text}`);
  }
  const text = await res.text();
  if (!text) return undefined as unknown as T;
  return JSON.parse(text) as T;
}

// ---------- Hypotheses ----------

export async function listHypotheses(): Promise<Hypothesis[]> {
  const data = await request<{ items: Hypothesis[] }>("/api/hypothesis");
  return data.items;
}

export async function createHypothesis(body: {
  name: string;
  generator: string;
  state_machine?: Record<string, unknown>;
  tolerance?: Record<string, unknown>;
}): Promise<Hypothesis> {
  return request<Hypothesis>("/api/hypothesis", {
    method: "POST",
    body: JSON.stringify({
      name: body.name,
      generator: body.generator,
      state_machine: body.state_machine || { type: body.generator },
      tolerance: body.tolerance,
    }),
  });
}

export async function getHypothesis(id: string): Promise<Hypothesis> {
  return request<Hypothesis>(`/api/hypothesis/${id}`);
}

export async function deleteHypothesis(id: string): Promise<void> {
  return request<void>(`/api/hypothesis/${id}`, { method: "DELETE" });
}

// ---------- Runs ----------

export async function startRun(
  id: string,
  body: {
    adapter?: string;
    adapter_addr?: string;
    execution?: {
      batch_size?: number;
      checkpoint_every?: number;
      fail_fast?: boolean;
      duration?: string;
    };
  }
): Promise<{ run_id: string; hypothesis_id: string; status: string }> {
  return request(`/api/hypothesis/${id}/run`, {
    method: "POST",
    body: JSON.stringify(body),
  });
}

export async function stopRun(id: string): Promise<void> {
  return request<void>(`/api/hypothesis/${id}/run`, { method: "DELETE" });
}

export async function getStatus(id: string): Promise<StatusResponse> {
  return request<StatusResponse>(`/api/hypothesis/${id}/status`);
}

export async function getEvents(id: string): Promise<string> {
  const res = await fetch(`/api/hypothesis/${id}/events`);
  return res.text();
}

export async function getResults(id: string): Promise<RunResult[]> {
  const data = await request<{ items: RunResult[] }>(`/api/hypothesis/${id}/results`);
  return data.items;
}

export async function downloadBundle(id: string): Promise<void> {
  const res = await fetch(`/api/hypothesis/${id}/bundle`);
  if (!res.ok) throw new Error(`API ${res.status}`);
  const blob = await res.blob();
  const url = URL.createObjectURL(blob);
  const a = document.createElement("a");
  a.href = url;
  a.download = `hypothesis-${id}.zip`;
  a.click();
  URL.revokeObjectURL(url);
}

// ---------- Generators ----------

export async function listGenerators(): Promise<Generator[]> {
  const data = await request<{ items: Generator[] }>("/api/generators");
  return data.items;
}

export async function createGenerator(body: {
  name: string;
  description: string;
  rate: number;
  workload: Record<string, number>;
}): Promise<Generator> {
  return request<Generator>("/api/generators", {
    method: "POST",
    body: JSON.stringify(body),
  });
}

export async function deleteGenerator(name: string): Promise<void> {
  return request<void>(`/api/generators/${name}`, { method: "DELETE" });
}

// ---------- Adapters ----------

export async function listAdapters(): Promise<Adapter[]> {
  const data = await request<{ items: Adapter[] }>("/api/adapters");
  return data.items;
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

// ---------- Dashboard ----------

export async function getDashboardStats() {
  const hypotheses = await listHypotheses();
  return {
    total: hypotheses.length,
    running: hypotheses.filter((h) => h.status === "running").length,
    passed: hypotheses.filter((h) => h.status === "passed").length,
    failed: hypotheses.filter((h) => h.status === "failed").length,
    recent: hypotheses.slice(0, 10),
  };
}
