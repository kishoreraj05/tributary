import { useEffect, useState } from "react";

import { api, ApiError } from "../api/client";
import { useCatalog } from "../api/hooks";
import type { Connection } from "../api/types";

const MASK = "********";
const SECRET_HINTS = ["password", "passwd", "secret", "token", "credential", "api_key", "access_key", "private_key"];

type Row = { field: string; value: string; secret: boolean };
interface Draft {
  name: string;
  type: string;
  description: string;
  envs: Record<string, Row[]>;
  existing: boolean;
}

const empty = (): Draft => ({ name: "", type: "", description: "", envs: { dev: [{ field: "", value: "", secret: false }] }, existing: false });

function toDraft(c: Connection): Draft {
  const envs: Record<string, Row[]> = {};
  for (const [env, values] of Object.entries(c.environments)) {
    envs[env] = Object.entries(values).map(([field, value]) => ({
      field,
      value: typeof value === "string" ? value : JSON.stringify(value),
      secret: c.secret_fields.includes(field),
    }));
  }
  return { name: c.name, type: c.type, description: c.description ?? "", envs, existing: true };
}

function parseValue(text: string): unknown {
  if (text === "true" || text === "false") return text === "true";
  if (text.trim() !== "" && !Number.isNaN(Number(text)) && !/^0\d/.test(text)) return Number(text);
  return text;
}

export function ConnectionsPage() {
  const catalog = useCatalog();
  const [connections, setConnections] = useState<Connection[]>([]);
  const [draft, setDraft] = useState<Draft | null>(null);
  const [error, setError] = useState<string | null>(null);
  const reload = () => api.connections().then(setConnections);
  useEffect(() => {
    reload();
  }, []);

  const types = catalog
    ? [...new Set(["source", "sink", "metrics_sink"].flatMap((c) => catalog.components[c as "source"].map((p) => p.type)))].sort()
    : [];

  const save = async () => {
    if (!draft) return;
    const environments: Record<string, Record<string, unknown>> = {};
    const secretFields = new Set<string>();
    for (const [env, rows] of Object.entries(draft.envs)) {
      environments[env] = {};
      for (const row of rows.filter((r) => r.field)) {
        environments[env][row.field] = row.secret ? row.value : parseValue(row.value);
        if (row.secret) secretFields.add(row.field);
      }
    }
    const body = { name: draft.name, type: draft.type, description: draft.description || null, environments, secret_fields: [...secretFields] };
    try {
      await (draft.existing ? api.updateConnection(draft.name, body) : api.createConnection(body));
      setDraft(null);
      setError(null);
      reload();
    } catch (e) {
      setError(e instanceof ApiError ? e.message : String(e));
    }
  };

  const setRows = (env: string, rows: Row[]) => draft && setDraft({ ...draft, envs: { ...draft.envs, [env]: rows } });

  return (
    <div className="page">
      <div className="row" style={{ marginBottom: 16 }}>
        <h1 style={{ margin: 0 }}>Connections</h1>
        <span className="spacer" />
        <button className="primary" onClick={() => setDraft(empty())}>New connection</button>
      </div>
      <p className="muted small">
        Settings per environment that nodes reference by name. Secret fields are encrypted at rest and never shown again.
      </p>
      {draft && (
        <div className="card stack" style={{ marginBottom: 16 }}>
          <div className="row wrap">
            <div style={{ flex: 1, minWidth: 160 }}>
              <label>Name</label>
              <input value={draft.name} disabled={draft.existing} onChange={(e) => setDraft({ ...draft, name: e.target.value })} placeholder="kafka_main" />
            </div>
            <div style={{ flex: 1, minWidth: 160 }}>
              <label>Type</label>
              <input list="connection-types" value={draft.type} onChange={(e) => setDraft({ ...draft, type: e.target.value })} placeholder="kafka" />
              <datalist id="connection-types">{types.map((t) => <option key={t} value={t} />)}</datalist>
            </div>
          </div>
          <div>
            <label>Description</label>
            <input value={draft.description} onChange={(e) => setDraft({ ...draft, description: e.target.value })} />
          </div>
          {Object.entries(draft.envs).map(([env, rows]) => (
            <div key={env} className="subform">
              <div className="row"><b>{env}</b><span className="spacer" />
                <button className="link small danger" onClick={() => { const next = { ...draft.envs }; delete next[env]; setDraft({ ...draft, envs: next }); }}>Remove env</button>
              </div>
              {rows.map((row, i) => (
                <div className="row" key={i} style={{ marginTop: 4 }}>
                  <input value={row.field} placeholder="field (e.g. brokers)" onChange={(e) => {
                    const field = e.target.value;
                    setRows(env, rows.map((r, j) => (j === i ? { ...r, field, secret: r.secret || SECRET_HINTS.some((h) => field.toLowerCase().includes(h)) } : r)));
                  }} />
                  <input type={row.secret ? "password" : "text"} value={row.value} placeholder="value" onChange={(e) => setRows(env, rows.map((r, j) => (j === i ? { ...r, value: e.target.value } : r)))} />
                  <label className="row small" style={{ margin: 0, whiteSpace: "nowrap" }}>
                    <input type="checkbox" checked={row.secret} onChange={(e) => setRows(env, rows.map((r, j) => (j === i ? { ...r, secret: e.target.checked } : r)))} />
                    secret
                  </label>
                  <button onClick={() => setRows(env, rows.filter((_, j) => j !== i))}>×</button>
                </div>
              ))}
              <button className="small" style={{ marginTop: 6 }} onClick={() => setRows(env, [...rows, { field: "", value: "", secret: false }])}>+ Field</button>
              {rows.some((r) => r.secret && r.value === MASK) && <div className="desc small">Masked values keep the stored secret.</div>}
            </div>
          ))}
          <div className="row">
            <button onClick={() => {
              const env = prompt("Environment name (e.g. prod)");
              if (env && !(env in draft.envs)) setDraft({ ...draft, envs: { ...draft.envs, [env]: [] } });
            }}>+ Environment</button>
            <span className="spacer" />
            {error && <span className="error-text small">{error}</span>}
            <button onClick={() => { setDraft(null); setError(null); }}>Cancel</button>
            <button className="primary" disabled={!draft.name || !draft.type} onClick={save}>Save</button>
          </div>
        </div>
      )}
      {connections.length === 0 ? (
        <div className="muted">No connections yet.</div>
      ) : (
        <div className="card" style={{ padding: 0, overflow: "auto" }}>
          <table className="grid list">
            <thead><tr><th>Name</th><th>Type</th><th>Environments</th><th /></tr></thead>
            <tbody>
              {connections.map((c) => (
                <tr key={c.name} onClick={() => setDraft(toDraft(c))}>
                  <td><b>{c.name}</b>{c.description && <div className="muted small">{c.description}</div>}</td>
                  <td>{c.type}</td>
                  <td>{Object.keys(c.environments).map((e) => <span key={e} className="pill" style={{ marginRight: 4 }}>{e}</span>)}</td>
                  <td>
                    <button className="link small danger" onClick={async (e) => {
                      e.stopPropagation();
                      if (confirm(`Delete connection "${c.name}"?`)) { await api.deleteConnection(c.name); reload(); }
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
