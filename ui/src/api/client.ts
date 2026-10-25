import type {
  Catalog,
  Connection,
  Json,
  Pipeline,
  PipelineSummary,
  PlanResult,
  PreviewResult,
  QualityResult,
  Run,
  RunEvent,
  Version,
  ValidateResult,
  VersionSummary,
} from "./types";

const BASE = "/api/v1";

export class ApiError extends Error {
  constructor(
    public status: number,
    public detail: unknown,
  ) {
    super(ApiError.describe(detail) || `request failed (${status})`);
  }

  static describe(detail: unknown): string {
    if (typeof detail === "string") return detail;
    if (detail && typeof detail === "object") {
      const d = detail as { message?: string; issues?: { message: string }[] };
      if (d.issues?.length) return d.issues.map((i) => i.message).join("; ");
      if (d.message) return d.message;
      if (Array.isArray(detail)) return detail.map((e) => (e as { msg?: string }).msg ?? String(e)).join("; ");
    }
    return "";
  }
}

async function request<T>(method: string, path: string, body?: unknown, signal?: AbortSignal): Promise<T> {
  const init: RequestInit = { method, signal, headers: {} };
  if (body instanceof FormData) {
    init.body = body;
  } else if (body !== undefined) {
    init.body = JSON.stringify(body);
    (init.headers as Record<string, string>)["content-type"] = "application/json";
  }
  const response = await fetch(BASE + path, init);
  if (response.status === 204) return undefined as T;
  const text = await response.text();
  const data = text ? (response.headers.get("content-type")?.includes("json") ? JSON.parse(text) : text) : undefined;
  if (!response.ok) throw new ApiError(response.status, (data as { detail?: unknown })?.detail ?? data);
  return data as T;
}

const get = <T>(path: string, signal?: AbortSignal) => request<T>("GET", path, undefined, signal);
const post = <T>(path: string, body?: unknown, signal?: AbortSignal) => request<T>("POST", path, body, signal);
const put = <T>(path: string, body: unknown) => request<T>("PUT", path, body);
const del = (path: string) => request<void>("DELETE", path);

export interface SpecBody {
  spec: Json;
  env?: string | null;
}

export const api = {
  components: () => get<Catalog>("/components"),

  pipelines: () => get<PipelineSummary[]>("/pipelines"),
  pipeline: (id: string) => get<Pipeline>(`/pipelines/${id}`),
  createPipeline: (name: string, spec: Json = {}) => post<Pipeline>("/pipelines", { name, spec }),
  importPipeline: (name: string, content: string, format: "yaml" | "json") =>
    post<Pipeline>("/pipelines/import", { name, content, format }),
  saveDraft: (id: string, spec: Json, ui_layout: Json) => put<Pipeline>(`/pipelines/${id}`, { spec, ui_layout }),
  renamePipeline: (id: string, name: string) => put<Pipeline>(`/pipelines/${id}`, { name }),
  deletePipeline: (id: string) => del(`/pipelines/${id}`),
  versions: (id: string) => get<VersionSummary[]>(`/pipelines/${id}/versions`),
  version: (id: string, version: number) => get<Version>(`/pipelines/${id}/versions/${version}`),
  createVersion: (id: string, message: string) => post<VersionSummary>(`/pipelines/${id}/versions`, { message }),

  validate: (body: SpecBody & { deep?: boolean }, signal?: AbortSignal) =>
    post<ValidateResult>("/validate", body, signal),
  plan: (body: SpecBody) => post<PlanResult>("/plan", body),
  preview: (body: SpecBody & { node_id: string; limit?: number; sample_data?: Record<string, string>; write?: boolean }) =>
    post<PreviewResult>("/preview", body),
  uploadSample: (file: File) => {
    const form = new FormData();
    form.append("file", file);
    return post<{ sample_id: string; filename: string; size: number }>("/samples", form);
  },

  startRun: (pipelineId: string, env: string, version?: number) =>
    post<Run>(`/pipelines/${pipelineId}/runs`, { env, version }),
  runs: (pipelineId?: string) => get<Run[]>(`/runs${pipelineId ? `?pipeline_id=${pipelineId}` : ""}`),
  run: (id: string) => get<Run>(`/runs/${id}`),
  cancelRun: (id: string) => post<Run>(`/runs/${id}/cancel`),
  runQuality: (id: string) => get<QualityResult[]>(`/runs/${id}/quality`),

  connections: () => get<Connection[]>("/connections"),
  createConnection: (c: Partial<Connection> & { secret_fields?: string[] }) => post<Connection>("/connections", c),
  updateConnection: (name: string, c: Partial<Connection> & { secret_fields?: string[] }) =>
    put<Connection>(`/connections/${name}`, c),
  deleteConnection: (name: string) => del(`/connections/${name}`),
};

/** Live run status + log lines. Returns a function that closes the socket. */
export function subscribeRun(runId: string, onEvent: (event: RunEvent) => void): () => void {
  const scheme = location.protocol === "https:" ? "wss" : "ws";
  const socket = new WebSocket(`${scheme}://${location.host}${BASE}/runs/${runId}/events`);
  socket.onmessage = (message) => onEvent(JSON.parse(message.data));
  return () => socket.close();
}
