export type Json = Record<string, unknown>;

export interface JsonSchema {
  type?: string | string[];
  title?: string;
  description?: string;
  default?: unknown;
  enum?: unknown[];
  const?: unknown;
  format?: string;
  properties?: Record<string, JsonSchema>;
  required?: string[];
  additionalProperties?: boolean | JsonSchema;
  items?: JsonSchema;
  anyOf?: JsonSchema[];
  $ref?: string;
  $defs?: Record<string, JsonSchema>;
  minimum?: number;
  maximum?: number;
  minItems?: number;
}

export interface PluginMeta {
  type: string;
  category: string;
  display_name: string;
  description: string;
  icon: string;
  capabilities: string[];
  options_schema: JsonSchema;
}

export type Category = "source" | "transform" | "sink" | "check" | "metrics_sink" | "schema" | "secret_provider";

export interface Catalog {
  pipeline: JsonSchema;
  components: Record<Category, PluginMeta[]>;
}

export interface PipelineSummary {
  id: string;
  name: string;
  description: string | null;
  latest_version: number;
  created_at: string;
  updated_at: string;
}

export interface Pipeline extends PipelineSummary {
  draft_spec: Json;
  draft_layout: Record<string, { x: number; y: number }>;
}

export interface VersionSummary {
  version: number;
  message: string | null;
  created_at: string;
}

export interface Issue {
  path: string;
  message: string;
  node_id: string | null;
}

export interface FieldSchema {
  name: string;
  type: unknown;
  nullable: boolean;
}

export interface NodeAnalysis {
  node_id: string;
  ok: boolean;
  schema: { fields: FieldSchema[] } | null;
  error: string | null;
}

export interface ValidateResult {
  valid: boolean;
  env?: string;
  issues: Issue[];
  warnings: string[];
  nodes?: Record<string, NodeAnalysis>;
}

export interface QualityResult {
  run_id?: string;
  node_id: string;
  quality_id: string;
  check_id: string;
  check_type: string;
  level: string;
  action: string;
  total_rows: number;
  failed_rows: number | null;
  observed_value: number | null;
  passed: boolean;
  batch_id: number | null;
}

export interface PreviewResult {
  node_id: string;
  kind: string;
  schema: { fields: FieldSchema[] };
  rows: Json[];
  quality: QualityResult[];
  target: string | null;
  written: boolean;
}

export interface PlanResult {
  explain: string;
  resources: { profile: string; master: string | null; reasons: string[]; conf: Record<string, string> };
  warnings: string[];
}

export type RunStatus = "queued" | "running" | "succeeded" | "failed" | "cancelled";

export interface Run {
  id: string;
  pipeline_id: string;
  version: number;
  env: string;
  mode: string;
  status: RunStatus;
  error: string | null;
  summary: Json;
  created_at: string;
  started_at: string | null;
  finished_at: string | null;
}

export interface Connection {
  name: string;
  type: string;
  description: string | null;
  environments: Record<string, Json>;
  secret_fields: string[];
}

export type RunEvent =
  | { type: "status"; run: Run }
  | { type: "log"; lines: string[] }
  | { type: "error"; message: string };
