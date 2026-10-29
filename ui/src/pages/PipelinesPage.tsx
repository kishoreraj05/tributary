import { Clock, FileUp, GitCommitHorizontal, Plus, Trash2, Workflow, X } from "lucide-react";
import { useEffect, useState } from "react";
import { useNavigate } from "react-router-dom";

import { api, ApiError } from "../api/client";
import type { PipelineSummary } from "../api/types";
import { IconChip, timeAgo } from "../components/icons";

const slug = (name: string) => name.toLowerCase().replace(/[^a-z0-9]+/g, "_").replace(/^_+|_+$/g, "").replace(/^(\d)/, "p_$1") || "pipeline";

export function PipelinesPage() {
  const navigate = useNavigate();
  const [pipelines, setPipelines] = useState<PipelineSummary[] | null>(null);
  const [mode, setMode] = useState<"create" | "import" | null>(null);
  const [name, setName] = useState("");
  const [importText, setImportText] = useState("");
  const [error, setError] = useState<string | null>(null);
  const reload = () => api.pipelines().then(setPipelines).catch((e) => setError(String(e.message)));
  useEffect(() => {
    reload();
  }, []);

  const create = async () => {
    try {
      const p = await api.createPipeline(name, {
        version: 1,
        pipeline: { id: slug(name), mode: "batch" },
        // local first, so a new pipeline opens there: sources and sinks can use dummy data in it.
        environments: { local: { vars: {} }, dev: { vars: {} } },
        sources: [],
        sinks: [],
      });
      navigate(`/pipelines/${p.id}`);
    } catch (e) {
      setError(e instanceof ApiError ? e.message : String(e));
    }
  };
  const doImport = async () => {
    try {
      const format = importText.trim().startsWith("{") ? "json" : "yaml";
      const p = await api.importPipeline(name, importText, format);
      navigate(`/pipelines/${p.id}`);
    } catch (e) {
      setError(e instanceof ApiError ? e.message : String(e));
    }
  };
  const open = (next: "create" | "import") => {
    setMode(mode === next ? null : next);
    setError(null);
  };

  return (
    <div className="page">
      <div className="page-header">
        <div>
          <h1>Pipelines</h1>
          <p>Draw data pipelines, test each step on sample data, and run them on Spark.</p>
        </div>
        <span className="spacer" />
        <button onClick={() => open("import")}><FileUp size={15} />Import</button>
        <button className="primary" onClick={() => open("create")}><Plus size={15} />New pipeline</button>
      </div>

      {mode && (
        <div className="card stack" style={{ marginBottom: 20 }}>
          <div className="row">
            <b>{mode === "create" ? "New pipeline" : "Import a pipeline spec"}</b>
            <span className="spacer" />
            <button className="ghost icon sm" onClick={() => setMode(null)} aria-label="Close"><X size={15} /></button>
          </div>
          <div>
            <label>Name</label>
            <input
              autoFocus
              value={name}
              placeholder="e.g. Orders daily"
              onChange={(e) => setName(e.target.value)}
              onKeyDown={(e) => e.key === "Enter" && name && mode === "create" && create()}
            />
          </div>
          {mode === "import" && (
            <>
              <div>
                <label>YAML or JSON file</label>
                <input type="file" accept=".yaml,.yml,.json" onChange={async (e) => {
                  const file = e.target.files?.[0];
                  if (file) { setImportText(await file.text()); if (!name) setName(file.name.replace(/\.(ya?ml|json)$/, "")); }
                }} />
              </div>
              <textarea rows={8} value={importText} placeholder="…or paste a pipeline spec" onChange={(e) => setImportText(e.target.value)} />
            </>
          )}
          {error && <div className="error-text small">{error}</div>}
          <div className="row">
            <span className="spacer" />
            <button onClick={() => setMode(null)}>Cancel</button>
            {mode === "create" ? (
              <button className="primary" disabled={!name} onClick={create}>Create and open</button>
            ) : (
              <button className="primary" disabled={!name || !importText} onClick={doImport}>Import</button>
            )}
          </div>
        </div>
      )}

      {pipelines === null ? (
        <div className="muted">Loading…</div>
      ) : pipelines.length === 0 ? (
        <div className="empty">
          <IconChip icon={Workflow} color="var(--accent)" size={20} large />
          <h2>No pipelines yet</h2>
          <p>Create one and draw it on the canvas, or import a spec such as <span className="mono">examples/csv_to_parquet.yaml</span>.</p>
          <div className="row" style={{ marginTop: 6 }}>
            <button onClick={() => open("import")}><FileUp size={15} />Import</button>
            <button className="primary" onClick={() => open("create")}><Plus size={15} />New pipeline</button>
          </div>
        </div>
      ) : (
        <div className="card-grid">
          {pipelines.map((p) => (
            <div key={p.id} className="item-card" role="link" tabIndex={0}
              onClick={() => navigate(`/pipelines/${p.id}`)}
              onKeyDown={(e) => e.key === "Enter" && navigate(`/pipelines/${p.id}`)}>
              <div className="head">
                <IconChip icon={Workflow} color="var(--accent)" size={18} large />
                <div style={{ minWidth: 0 }}>
                  <div className="name">{p.name}</div>
                  <div className="sub">{p.description ?? "No description"}</div>
                </div>
              </div>
              <div className="foot">
                {p.latest_version ? (
                  <span className="pill ok"><GitCommitHorizontal size={12} />v{p.latest_version}</span>
                ) : (
                  <span className="pill warn"><span className="dot" />Draft</span>
                )}
                <span className="row" style={{ gap: 4 }}><Clock size={12} />Updated {timeAgo(p.updated_at)}</span>
              </div>
              <div className="actions">
                <button className="ghost icon sm danger" title="Delete pipeline" aria-label={`Delete ${p.name}`} onClick={async (e) => {
                  e.stopPropagation();
                  if (!confirm(`Delete pipeline "${p.name}" and its versions and run history?`)) return;
                  try { await api.deletePipeline(p.id); reload(); } catch (err) { setError((err as Error).message); }
                }}><Trash2 size={14} /></button>
              </div>
            </div>
          ))}
        </div>
      )}
      {error && !mode && <div className="callout danger" style={{ marginTop: 16 }}>{error}</div>}
    </div>
  );
}
