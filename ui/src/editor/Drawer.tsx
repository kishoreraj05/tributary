import Editor from "@monaco-editor/react";
import { ChevronDown, ChevronUp, CircleAlert, CircleCheck, Eye, FileCode2, FlaskConical, History, Loader2, Network, Square, TriangleAlert, type LucideIcon } from "lucide-react";
import { useEffect, useMemo, useState } from "react";
import YAML from "yaml";

import { api, ApiError, subscribeRun } from "../api/client";
import type { PlanResult, QualityResult, Run } from "../api/types";
import { monacoTheme } from "../components/monaco";
import { toSpec } from "../model/graph";
import { problemCount, useEditor, type DrawerTab } from "../store/editor";
import { DRAWER, ResizeHandle, usePanelSize } from "./layout";

const TABS: { id: DrawerTab; label: string; icon: LucideIcon }[] = [
  { id: "preview", label: "Preview", icon: Eye },
  { id: "problems", label: "Problems", icon: CircleAlert },
  { id: "yaml", label: "YAML", icon: FileCode2 },
  { id: "plan", label: "Plan", icon: Network },
  { id: "runs", label: "Runs", icon: History },
];

function Empty({ icon: Icon, children }: { icon: LucideIcon; children: React.ReactNode }) {
  return (
    <div className="row muted" style={{ justifyContent: "center", height: "100%", minHeight: 120, gap: 10 }}>
      <Icon size={18} className={Icon === Loader2 ? "spin" : ""} />
      <span>{children}</span>
    </div>
  );
}

export function Drawer() {
  const { drawer, setDrawer, issues, analysis } = useEditor();
  const problems = problemCount({ issues, analysis });
  const [height, setHeight] = usePanelSize(DRAWER);
  return (
    <div className={`drawer${drawer ? " open" : ""}`} style={drawer ? { height } : undefined}>
      {drawer && <ResizeHandle panel={DRAWER} size={height} edge="top" label="Resize bottom panel" onResize={setHeight} />}
      <div className="tabs">
        {TABS.map((t) => (
          <button key={t.id} className={drawer === t.id ? "active" : ""} onClick={() => setDrawer(drawer === t.id ? null : t.id)}>
            <t.icon size={14} />
            {t.label}
            {t.id === "problems" && problems > 0 && <span className="count">{problems}</span>}
          </button>
        ))}
        <span className="spacer" />
        <button className="ghost icon sm" onClick={() => setDrawer(drawer ? null : "preview")} title={drawer ? "Collapse" : "Expand"} aria-label={drawer ? "Collapse panel" : "Expand panel"}>
          {drawer ? <ChevronDown size={15} /> : <ChevronUp size={15} />}
        </button>
      </div>
      {drawer && (
        <div className="drawer-body">
          {drawer === "preview" && <PreviewView />}
          {drawer === "problems" && <ProblemsView />}
          {drawer === "yaml" && <YamlView />}
          {drawer === "plan" && <PlanView />}
          {drawer === "runs" && <RunsView />}
        </div>
      )}
    </div>
  );
}

const FAILED = "_dq_failed_checks";

function Cell({ value, field }: { value: unknown; field?: string }) {
  if (field === FAILED) {
    const failed = (value as string[]) ?? [];
    return (
      <td>
        {failed.length ? (
          <span className="row" style={{ gap: 4 }}>{failed.map((c) => <span key={c} className="pill bad">{c}</span>)}</span>
        ) : (
          <span className="pill ok"><CircleCheck size={12} />passes</span>
        )}
      </td>
    );
  }
  if (value === null || value === undefined) return <td className="null">null</td>;
  return <td>{typeof value === "object" ? JSON.stringify(value) : String(value)}</td>;
}

export function QualityTable({ results }: { results: QualityResult[] }) {
  if (!results.length) return null;
  return (
    <div className="table-wrap" style={{ marginTop: 12 }}>
    <table className="grid">
      <thead>
        <tr><th>Check</th><th>On</th><th>Result</th><th>Failed / total</th><th>Observed</th>{results.some((r) => r.batch_id !== null) && <th>Batch</th>}</tr>
      </thead>
      <tbody>
        {results.map((r, i) => (
          <tr key={i}>
            <td>{r.quality_id}.{r.check_id} <span className="muted">({r.check_type})</span></td>
            <td>{r.node_id}</td>
            <td><span className={`pill ${r.passed ? "ok" : "bad"}`}>{r.passed ? <CircleCheck size={12} /> : <CircleAlert size={12} />}{r.passed ? "pass" : "fail"}</span></td>
            <td>{r.level === "row" ? `${r.failed_rows} / ${r.total_rows}` : "—"}</td>
            <td>{r.observed_value ?? "—"}</td>
            {r.batch_id !== null && <td>{r.batch_id}</td>}
          </tr>
        ))}
      </tbody>
    </table>
    </div>
  );
}

function PreviewView() {
  const preview = useEditor((s) => s.preview);
  if (!preview) return <Empty icon={FlaskConical}>Select a node and press <b>Test</b> to see its output on sample data.</Empty>;
  if (preview.loading)
    return <Empty icon={Loader2}>Running <b>{preview.specId}</b> on sample data… the first preview starts Spark and takes a little longer.</Empty>;
  if (preview.error)
    return <div className="callout danger"><CircleAlert size={16} /><div><b>{preview.specId}</b>: {preview.error}</div></div>;
  const result = preview.result!;
  // A quality preview: show which checks each row fails first, then the data.
  const fields = [...result.schema.fields].sort((a, b) => Number(b.name === FAILED) - Number(a.name === FAILED));
  const failingRows = result.kind === "quality" ? result.rows.filter((r) => ((r[FAILED] as string[]) ?? []).length).length : 0;
  return (
    <div>
      <div className="row small" style={{ marginBottom: 8 }}>
        <b style={{ fontSize: 13 }}>{result.node_id}</b>
        <span className="pill">{result.rows.length} rows</span>
        <span className="pill">{fields.length} columns</span>
        {result.target && <span className="pill info">{result.written ? "written to" : "dry run →"} {result.target}</span>}
        {result.kind === "quality" && (
          <span className={`pill ${failingRows ? "bad" : "ok"}`}>
            {failingRows ? <CircleAlert size={12} /> : <CircleCheck size={12} />}
            {failingRows} of {result.rows.length} sample rows fail a check
          </span>
        )}
      </div>
      <div className="table-wrap" style={{ maxHeight: 220 }}>
        <table className="grid">
          <thead>
            <tr>
              {fields.map((f) =>
                f.name === FAILED ? (
                  <th key={f.name}>Failed checks</th>
                ) : (
                  <th key={f.name}>{f.name}<span className="type">{typeof f.type === "string" ? f.type : "struct"}</span></th>
                ),
              )}
            </tr>
          </thead>
          <tbody>
            {result.rows.map((row, i) => (
              <tr key={i} className={((row[FAILED] as string[]) ?? []).length ? "failing" : ""}>
                {fields.map((f) => <Cell key={f.name} field={f.name} value={row[f.name]} />)}
              </tr>
            ))}
          </tbody>
        </table>
      </div>
      <QualityTable results={result.quality} />
    </div>
  );
}

function ProblemsView() {
  const { issues, warnings, analysis, nodes, select, checking } = useEditor();
  const select_ = (specId: string | null) => {
    const node = nodes.find((n) => n.data.specId === specId);
    if (node) select(node.id);
  };
  const columnErrors = Object.values(analysis).filter((a) => !a.ok && !a.error?.startsWith("skipped"));
  if (!issues.length && !columnErrors.length && !warnings.length)
    return checking !== "idle" ? <Empty icon={Loader2}>Checking…</Empty> : <Empty icon={CircleCheck}>No problems found.</Empty>;
  return (
    <div className="table-wrap">
    <table className="grid list">
      <tbody>
        {issues.map((i, k) => (
          <tr key={`i${k}`} onClick={() => select_(i.node_id)}>
            <td><span className="pill bad"><CircleAlert size={12} />error</span></td><td>{i.node_id ?? "pipeline"}</td><td className="mono">{i.path}</td><td style={{ whiteSpace: "normal" }}>{i.message}</td>
          </tr>
        ))}
        {columnErrors.map((a) => (
          <tr key={`a${a.node_id}`} onClick={() => select_(a.node_id)}>
            <td><span className="pill bad"><CircleAlert size={12} />columns</span></td><td>{a.node_id}</td><td /><td style={{ whiteSpace: "normal" }}>{a.error}</td>
          </tr>
        ))}
        {warnings.map((w) => (
          <tr key={w}><td><span className="pill warn"><TriangleAlert size={12} />warning</span></td><td colSpan={3} style={{ whiteSpace: "normal" }}>{w}</td></tr>
        ))}
      </tbody>
    </table>
    </div>
  );
}

function YamlView() {
  const { nodes, edges, meta, applySpec } = useEditor();
  const current = useMemo(() => YAML.stringify(toSpec(nodes, edges, meta)), [nodes, edges, meta]);
  const [text, setText] = useState(current);
  const [error, setError] = useState<string | null>(null);
  const [dirty, setDirty] = useState(false);
  useEffect(() => {
    if (!dirty) setText(current);
  }, [current, dirty]);

  const apply = () => {
    try {
      const parsed = YAML.parse(text);
      if (!parsed || typeof parsed !== "object") throw new Error("the document must be a mapping");
      applySpec(parsed);
      setDirty(false);
      setError(null);
    } catch (e) {
      setError(String((e as Error).message));
    }
  };
  return (
    <div style={{ display: "grid", gridTemplateRows: "auto 1fr", height: "100%" }}>
      <div className="row small" style={{ marginBottom: 6 }}>
        <span className="muted">The spec the engine receives. Edit and apply to update the canvas.</span>
        <span className="spacer" />
        {error && <span className="error-text">{error}</span>}
        <button disabled={!dirty} onClick={() => { setDirty(false); setText(current); setError(null); }}>Discard</button>
        <button className="primary" disabled={!dirty} onClick={apply}>Apply</button>
      </div>
      <Editor
        height="220px"
        language="yaml"
        theme={monacoTheme()}
        value={text}
        onChange={(v) => { setText(v ?? ""); setDirty(true); }}
        options={{ minimap: { enabled: false }, fontSize: 12, scrollBeyondLastLine: false, automaticLayout: true }}
      />
    </div>
  );
}

function PlanView() {
  const { nodes, edges, meta, env } = useEditor();
  const [plan, setPlan] = useState<PlanResult | null>(null);
  const [error, setError] = useState<string | null>(null);
  useEffect(() => {
    api.plan({ spec: toSpec(nodes, edges, meta), env })
      .then((p) => { setPlan(p); setError(null); })
      .catch((e) => setError(e instanceof ApiError ? e.message : String(e)));
  }, [nodes, edges, meta, env]);
  if (error) return <div className="callout warn"><TriangleAlert size={16} /><div>The plan is available once the pipeline is valid: {error}</div></div>;
  if (!plan) return <Empty icon={Loader2}>Planning…</Empty>;
  return (
    <div className="row" style={{ alignItems: "flex-start", gap: 16 }}>
      <pre className="log" style={{ flex: 1, minWidth: 0, overflow: "auto" }}>{plan.explain}</pre>
      <div className="card small" style={{ width: 260, flex: "none" }}>
        <div className="row" style={{ marginBottom: 6 }}><b>Spark resources</b><span className="spacer" /><span className="pill info">{plan.resources.profile}</span></div>
        <ul style={{ margin: 0, paddingLeft: 16, color: "var(--muted)" }}>{plan.resources.reasons.map((r) => <li key={r}>{r}</li>)}</ul>
      </div>
    </div>
  );
}

const STATUS_CLASS: Record<string, string> = { succeeded: "ok", failed: "bad", cancelled: "warn", running: "info", queued: "" };

export function StatusPill({ status }: { status: string }) {
  const live = status === "running" || status === "queued";
  return <span className={`pill ${STATUS_CLASS[status] ?? ""}`}><span className={`dot${live ? " pulse" : ""}`} />{status}</span>;
}

function RunsView() {
  const pipeline = useEditor((s) => s.pipeline);
  const lastRunId = useEditor((s) => s.lastRunId);
  const [runs, setRuns] = useState<Run[]>([]);
  const [selected, setSelected] = useState<string | null>(lastRunId);
  useEffect(() => {
    if (lastRunId) setSelected(lastRunId);
  }, [lastRunId]);
  useEffect(() => {
    if (!pipeline) return;
    const load = () => api.runs(pipeline.id).then((list) => { setRuns(list); setSelected((s) => s ?? list[0]?.id ?? null); });
    load();
    const timer = setInterval(load, 4000);
    return () => clearInterval(timer);
  }, [pipeline, lastRunId]);
  if (!runs.length) return <Empty icon={History}>No runs yet. Press <b>Run</b> in the toolbar to start one.</Empty>;
  return (
    <div className="row" style={{ alignItems: "flex-start", gap: 16, height: "100%" }}>
      <div className="table-wrap" style={{ width: 290, flex: "none", maxHeight: "100%" }}>
      <table className="grid list">
        <tbody>
          {runs.map((r) => (
            <tr key={r.id} onClick={() => setSelected(r.id)} className={r.id === selected ? "active" : ""}>
              <td><StatusPill status={r.status} /></td>
              <td>v{r.version} · {r.env}</td>
              <td className="muted">{new Date(r.created_at).toLocaleTimeString()}</td>
            </tr>
          ))}
        </tbody>
      </table>
      </div>
      {selected && <RunDetail key={selected} runId={selected} />}
    </div>
  );
}

export function RunDetail({ runId }: { runId: string }) {
  const [run, setRun] = useState<Run | null>(null);
  const [lines, setLines] = useState<string[]>([]);
  const [quality, setQuality] = useState<QualityResult[]>([]);
  useEffect(() => {
    setLines([]);
    return subscribeRun(runId, (event) => {
      if (event.type === "status") setRun(event.run);
      if (event.type === "log") setLines((l) => [...l, ...event.lines].slice(-2000));
    });
  }, [runId]);
  useEffect(() => {
    const load = () => api.runQuality(runId).then(setQuality);
    load();
    if (run && !["running", "queued"].includes(run.status)) return;
    const timer = setInterval(load, 5000);
    return () => clearInterval(timer);
  }, [runId, run?.status]);
  if (!run) return <Empty icon={Loader2}>Connecting…</Empty>;
  return (
    <div style={{ flex: 1, minWidth: 0 }}>
      <div className="row small">
        <StatusPill status={run.status} />
        <span className="mono muted">{run.id.slice(0, 12)}</span>
        <span className="muted">v{run.version} · {run.env} · {run.mode}</span>
        <span className="spacer" />
        {["queued", "running"].includes(run.status) && (
          <button className="danger sm" onClick={() => api.cancelRun(run.id).then(setRun)}><Square size={11} fill="currentColor" />Cancel</button>
        )}
      </div>
      {run.error && <div className="callout danger" style={{ marginTop: 10, marginBottom: 0 }}><CircleAlert size={16} /><div>{run.error}</div></div>}
      {Object.keys(run.summary ?? {}).length > 0 && <div className="mono muted" style={{ marginTop: 6, overflowWrap: "anywhere" }}>{JSON.stringify(run.summary)}</div>}
      <QualityTable results={quality} />
      <pre className="log" style={{ marginTop: 10, maxHeight: 160, overflow: "auto" }}>{lines.join("\n")}</pre>
    </div>
  );
}
