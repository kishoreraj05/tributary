import { KeyRound, Lock, Plug, Plus, Trash2, X } from "lucide-react";
import { useEffect, useState } from "react";

import { api, ApiError } from "../api/client";
import { useCatalog } from "../api/hooks";
import { connectionIcon, IconChip } from "../components/icons";
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
      <div className="page-header">
        <div>
          <h1>Connections</h1>
          <p>Settings per environment that nodes use by name. Secret fields are encrypted and never shown again.</p>
        </div>
        <span className="spacer" />
        <button className="primary" onClick={() => setDraft(empty())}><Plus size={15} />New connection</button>
      </div>
      {draft && (
        <div className="card stack" style={{ marginBottom: 20 }}>
          <div className="row">
            <IconChip icon={connectionIcon(draft.type)} color="var(--accent)" />
            <b>{draft.existing ? `Edit ${draft.name}` : "New connection"}</b>
            <span className="spacer" />
            <button className="ghost icon sm" onClick={() => { setDraft(null); setError(null); }} aria-label="Close"><X size={15} /></button>
          </div>
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
              <div className="row" style={{ marginBottom: 6 }}><span className="pill info">{env}</span><span className="spacer" />
                <button className="link small danger" onClick={() => { const next = { ...draft.envs }; delete next[env]; setDraft({ ...draft, envs: next }); }}>Remove env</button>
              </div>
              {rows.map((row, i) => (
                <div className="row" key={i} style={{ marginTop: 4 }}>
                  <input value={row.field} placeholder="field (e.g. brokers)" onChange={(e) => {
                    const field = e.target.value;
                    setRows(env, rows.map((r, j) => (j === i ? { ...r, field, secret: r.secret || SECRET_HINTS.some((h) => field.toLowerCase().includes(h)) } : r)));
                  }} />
                  <input type={row.secret ? "password" : "text"} value={row.value} placeholder="value" onChange={(e) => setRows(env, rows.map((r, j) => (j === i ? { ...r, value: e.target.value } : r)))} />
                  <label className="check small" style={{ whiteSpace: "nowrap", fontSize: 12 }} title="Encrypt and hide this value">
                    <input type="checkbox" checked={row.secret} onChange={(e) => setRows(env, rows.map((r, j) => (j === i ? { ...r, secret: e.target.checked } : r)))} />
                    <Lock size={12} />secret
                  </label>
                  <button className="ghost icon" aria-label="Remove field" onClick={() => setRows(env, rows.filter((_, j) => j !== i))}>×</button>
                </div>
              ))}
              <button className="sm" style={{ margin: "6px 0 10px" }} onClick={() => setRows(env, [...rows, { field: "", value: "", secret: false }])}><Plus size={12} />Field</button>
              {rows.some((r) => r.secret && r.value === MASK) && <div className="desc small">Masked values keep the stored secret.</div>}
            </div>
          ))}
          <div className="row">
            <button onClick={() => {
              const env = prompt("Environment name (e.g. prod)");
              if (env && !(env in draft.envs)) setDraft({ ...draft, envs: { ...draft.envs, [env]: [] } });
            }}><Plus size={14} />Environment</button>
            <span className="spacer" />
            {error && <span className="error-text small">{error}</span>}
            <button onClick={() => { setDraft(null); setError(null); }}>Cancel</button>
            <button className="primary" disabled={!draft.name || !draft.type} onClick={save}>Save</button>
          </div>
        </div>
      )}
      {connections.length === 0 ? (
        !draft && (
          <div className="empty">
            <IconChip icon={Plug} color="var(--accent)" size={20} large />
            <h2>No connections yet</h2>
            <p>Store brokers, databases and buckets once per environment, then pick them on any source or sink.</p>
            <button className="primary" style={{ marginTop: 6 }} onClick={() => setDraft(empty())}><Plus size={15} />New connection</button>
          </div>
        )
      ) : (
        <div className="card-grid">
          {connections.map((c) => (
            <div key={c.name} className="item-card" role="button" tabIndex={0}
              onClick={() => setDraft(toDraft(c))}
              onKeyDown={(e) => e.key === "Enter" && setDraft(toDraft(c))}>
              <div className="head">
                <IconChip icon={connectionIcon(c.type)} color="var(--source)" size={18} large />
                <div style={{ minWidth: 0 }}>
                  <div className="name mono" style={{ fontSize: 14 }}>{c.name}</div>
                  <div className="sub">{c.description ?? c.type}</div>
                </div>
              </div>
              <div className="foot">
                <span className="pill">{c.type}</span>
                {Object.keys(c.environments).map((e) => <span key={e} className="pill info">{e}</span>)}
                {c.secret_fields.length > 0 && <span className="row" style={{ gap: 4 }} title={c.secret_fields.join(", ")}><KeyRound size={12} />{c.secret_fields.length} secret</span>}
              </div>
              <div className="actions">
                <button className="ghost icon sm danger" title="Delete connection" aria-label={`Delete ${c.name}`} onClick={async (e) => {
                  e.stopPropagation();
                  if (confirm(`Delete connection "${c.name}"?`)) { await api.deleteConnection(c.name); reload(); }
                }}><Trash2 size={14} /></button>
              </div>
            </div>
          ))}
        </div>
      )}
    </div>
  );
}
