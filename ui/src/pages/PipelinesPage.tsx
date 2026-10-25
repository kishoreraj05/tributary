import { useEffect, useState } from "react";
import { useNavigate } from "react-router-dom";

import { api, ApiError } from "../api/client";
import type { PipelineSummary } from "../api/types";

const slug = (name: string) => name.toLowerCase().replace(/[^a-z0-9]+/g, "_").replace(/^_+|_+$/g, "").replace(/^(\d)/, "p_$1") || "pipeline";

export function PipelinesPage() {
  const navigate = useNavigate();
  const [pipelines, setPipelines] = useState<PipelineSummary[] | null>(null);
  const [name, setName] = useState("");
  const [importing, setImporting] = useState(false);
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
        environments: { dev: { vars: {} } },
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

  return (
    <div className="page">
      <h1>Pipelines</h1>
      <div className="card stack" style={{ marginBottom: 16 }}>
        <div className="row">
          <input value={name} placeholder="New pipeline name" onChange={(e) => setName(e.target.value)} onKeyDown={(e) => e.key === "Enter" && name && !importing && create()} />
          {!importing && <button className="primary" disabled={!name} onClick={create}>Create</button>}
          <button onClick={() => setImporting(!importing)}>{importing ? "Cancel import" : "Import YAML/JSON…"}</button>
        </div>
        {importing && (
          <>
            <input type="file" accept=".yaml,.yml,.json" onChange={async (e) => {
              const file = e.target.files?.[0];
              if (file) { setImportText(await file.text()); if (!name) setName(file.name.replace(/\.(ya?ml|json)$/, "")); }
            }} />
            <textarea rows={8} value={importText} placeholder="…or paste a pipeline spec" onChange={(e) => setImportText(e.target.value)} />
            <div className="row"><span className="spacer" /><button className="primary" disabled={!name || !importText} onClick={doImport}>Import</button></div>
          </>
        )}
        {error && <div className="error-text small">{error}</div>}
      </div>
      {pipelines === null ? (
        <div className="muted">Loading…</div>
      ) : pipelines.length === 0 ? (
        <div className="muted">No pipelines yet. Create one above, or import a YAML file from examples/.</div>
      ) : (
        <div className="card" style={{ padding: 0, overflow: "auto" }}>
          <table className="grid list">
            <thead><tr><th>Name</th><th>Version</th><th>Updated</th><th /></tr></thead>
            <tbody>
              {pipelines.map((p) => (
                <tr key={p.id} onClick={() => navigate(`/pipelines/${p.id}`)}>
                  <td><b>{p.name}</b>{p.description && <div className="muted small">{p.description}</div>}</td>
                  <td>{p.latest_version ? `v${p.latest_version}` : <span className="muted">draft</span>}</td>
                  <td className="muted">{new Date(p.updated_at).toLocaleString()}</td>
                  <td>
                    <button className="link small danger" onClick={async (e) => {
                      e.stopPropagation();
                      if (!confirm(`Delete pipeline "${p.name}" and its versions and run history?`)) return;
                      try { await api.deletePipeline(p.id); reload(); } catch (err) { setError((err as Error).message); }
                    }}>Delete</button>
                  </td>
                </tr>
              ))}
            </tbody>
          </table>
        </div>
      )}
    </div>
  );
}
