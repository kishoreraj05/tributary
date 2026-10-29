import { CircleAlert, FileJson, FlaskConical, Info, Link2Off, Plus, Trash2, Upload } from "lucide-react";
import { useMemo, useState } from "react";

import { api } from "../../api/client";
import { plugin, useCatalog, useConnections } from "../../api/hooks";
import type { Catalog, Category, Json } from "../../api/types";
import { IconChip, pluginIcon } from "../../components/icons";
import { SchemaForm } from "../../components/SchemaForm";
import { countRecords, dummyOf, isDummy, isTap, LOCAL_ENV, MAX_DUMMY_RECORDS, type DummySettings, type FlowNode } from "../../model/graph";
import { nodeIssues, useEditor } from "../../store/editor";
import { exactErrors, fieldErrors, nodePath } from "../issues";
import { runPreview } from "../preview";
import { SqlEditor } from "../SqlEditor";

const IDENT = /^[A-Za-z_][A-Za-z0-9_]*$/;

export function NodePanel({ node }: { node: FlowNode }) {
  const catalog = useCatalog();
  const { nodes, edges, issues, analysis, updateNode, removeNode, renameNode, env } = useEditor();
  const unattached = node.data.kind === "quality" && !edges.some((e) => e.source === node.id && isTap(e));
  const [draftId, setDraftId] = useState(node.data.specId);
  const [idError, setIdError] = useState<string | null>(null);
  const messages = useMemo(() => nodeIssues({ issues, analysis }, node.data.specId), [issues, analysis, node.data.specId]);
  if (!catalog) return null;
  const path = nodePath(nodes, node);
  const { kind, type } = node.data;
  // A dummy node stands in for the real system: its connection and options are not used.
  const dummy = isDummy(node.data, env);

  const commitId = () => {
    if (draftId === node.data.specId) return setIdError(null);
    if (!IDENT.test(draftId)) return setIdError("letters, digits and _ only; must not start with a digit");
    if (nodes.some((n) => n.data.specId === draftId)) return setIdError("another node already uses this id");
    setIdError(null);
    renameNode(node.id, draftId);
  };

  return (
    <div key={node.id}>
      <div className="panel-head">
        <IconChip
          icon={pluginIcon(kind === "quality" ? undefined : plugin(catalog, kind as Category, type)?.icon, kind, type)}
          color={`var(--${kind})`}
          size={18}
          large
        />
        <div className="text">
          <div className="title">{node.data.specId}</div>
          <div className="sub">
            {kind === "quality" ? "Data quality checks" : `${plugin(catalog, kind as Category, type)?.display_name ?? type} · ${kind}`}
          </div>
        </div>
        <button
          className="sm"
          disabled={unattached}
          onClick={() => runPreview(node.data.specId)}
          title={
            unattached
              ? "Attach this block to a node first"
              : kind === "quality"
                ? "Run these checks on sample data and see which rows fail"
                : "Preview this node's output on sample data"
          }
        >
          <FlaskConical size={13} />Test
        </button>
        <button className="ghost icon sm danger" onClick={() => removeNode(node.id)} title="Delete node" aria-label="Delete node">
          <Trash2 size={14} />
        </button>
      </div>
      {messages.length > 0 && (
        <div className="callout danger">
          <CircleAlert size={16} />
          <div>{messages.map((m) => <div key={m}>{m}</div>)}</div>
        </div>
      )}
      <div className="field">
        <label>Node id</label>
        <input className="mono" value={draftId} onChange={(e) => setDraftId(e.target.value)} onBlur={commitId} onKeyDown={(e) => e.key === "Enter" && commitId()} />
        {idError ? <div className="desc error-text">{idError}</div> : <div className="desc">Used as the table name in downstream SQL, unless the input is renamed.</div>}
      </div>
      {(kind === "source" || kind === "sink") && <DummyFields node={node} path={path} />}
      {kind === "source" && !dummy && <SourceFields node={node} catalog={catalog} path={path} />}
      {kind === "source" && dummy && <SchemaFields node={node} catalog={catalog} path={path} />}
      {kind === "transform" && <TransformFields node={node} catalog={catalog} path={path} />}
      {kind === "sink" && !dummy && <SinkFields node={node} catalog={catalog} path={path} />}
      {kind === "quality" && <QualityFields node={node} catalog={catalog} path={path} />}
      {kind !== "quality" && !dummy && (
        <PluginOptions node={node} catalog={catalog} errors={fieldErrors(issues, `${path}.options`)} onChange={(options) => updateNode(node.id, { options })} />
      )}
    </div>
  );
}

function PluginOptions({ node, catalog, errors, onChange }: { node: FlowNode; catalog: Catalog; errors: Record<string, string[]>; onChange(o: Json): void }) {
  const meta = plugin(catalog, node.data.kind as Category, node.data.type);
  if (!meta) return <div className="error-text">Unknown {node.data.kind} type '{node.data.type}'</div>;
  const exclude = node.data.type === "sql" ? ["sql"] : [];
  return (
    <>
      <h3>Options</h3>
      <SchemaForm schema={meta.options_schema} value={node.data.options} onChange={onChange} errors={errors} exclude={exclude} />
    </>
  );
}

function ConnectionField({ node, path }: { node: FlowNode; path: string }) {
  const connections = useConnections();
  const issues = useEditor((s) => s.issues);
  const updateNode = useEditor((s) => s.updateNode);
  const current = (node.data.extra.connection as string) ?? "";
  const matching = connections.filter((c) => c.type === node.data.type || c.name === current);
  return (
    <div className="field">
      <label>Connection</label>
      <select value={current} onChange={(e) => updateNode(node.id, { extra: { ...node.data.extra, connection: e.target.value || undefined } })}>
        <option value="">none (options below only)</option>
        {matching.map((c) => <option key={c.name} value={c.name}>{c.name}</option>)}
      </select>
      <div className="desc">Settings per environment come from the connection; options below override them.</div>
      {exactErrors(issues, `${path}.connection`).map((e) => <div key={e} className="desc error-text">{e}</div>)}
    </div>
  );
}

function SourceFields({ node, catalog, path }: { node: FlowNode; catalog: Catalog; path: string }) {
  const { updateNode, samples, set, meta } = useEditor();
  const extra = node.data.extra;
  const sample = samples[node.data.specId];
  const setExtra = (patch: Json) => updateNode(node.id, { extra: { ...extra, ...patch } });

  const upload = async (file: File) => {
    const result = await api.uploadSample(file);
    set({ samples: { ...useEditor.getState().samples, [node.data.specId]: result } });
  };

  return (
    <>
      <ConnectionField node={node} path={path} />
      {meta.pipeline.mode === "streaming" && (
        <div className="field">
          <label className="check">
            <input type="checkbox" checked={Boolean(extra.stream)} onChange={(e) => setExtra({ stream: e.target.checked || undefined })} />
            This is the streaming source
          </label>
        </div>
      )}
      <SchemaFields node={node} catalog={catalog} path={path} />
      <h3>Sample data</h3>
      <div className="field">
        <label className="row" style={{ fontWeight: 500, color: "var(--muted)" }}><Upload size={13} />CSV, JSON or Parquet</label>
        <input type="file" accept=".csv,.json,.jsonl,.ndjson,.parquet" onChange={(e) => e.target.files?.[0] && upload(e.target.files[0])} />
        <div className="desc">
          {sample ? (
            <>
              Previews use <b>{sample.filename}</b> instead of reading the source.{" "}
              <button className="link small" onClick={() => { const next = { ...samples }; delete next[node.data.specId]; set({ samples: next }); }}>
                Stop using it
              </button>
            </>
          ) : (
            "Optional: upload a CSV/JSON/Parquet file to preview without the real source."
          )}
        </div>
      </div>
    </>
  );
}

function SchemaFields({ node, catalog, path }: { node: FlowNode; catalog: Catalog; path: string }) {
  const { updateNode, issues } = useEditor();
  const extra = node.data.extra;
  const schema = (extra.schema as Json) ?? undefined;
  const kindMeta = schema ? plugin(catalog, "schema", String(schema.kind)) : undefined;
  const setExtra = (patch: Json) => updateNode(node.id, { extra: { ...extra, ...patch } });
  return (
    <>
      <h3>Schema</h3>
      <div className="field">
        <select value={schema ? String(schema.kind) : ""} onChange={(e) => setExtra({ schema: e.target.value ? { kind: e.target.value } : undefined })}>
          <option value="">none</option>
          {catalog.components.schema.map((s) => <option key={s.type} value={s.type}>{s.display_name} — {s.description}</option>)}
        </select>
      </div>
      {schema && kindMeta && (
        <SchemaForm
          schema={kindMeta.options_schema}
          value={schema}
          exclude={["kind"]}
          errors={fieldErrors(issues, `${path}.schema`)}
          onChange={(v) => setExtra({ schema: { ...v, kind: schema.kind } })}
        />
      )}
    </>
  );
}

/**
 * The local environment's stand-in: a source hands its uploaded records downstream instead
 * of reading, a sink writes nothing and Preview shows what it would get. Offered only while
 * the local environment is selected; elsewhere the setting is kept but has no effect.
 */
function DummyFields({ node, path }: { node: FlowNode; path: string }) {
  const { updateNode, issues, env, setEnv, meta } = useEditor();
  const [fileError, setFileError] = useState<string | null>(null);
  const settings = dummyOf(node.data);
  const source = node.data.kind === "source";
  const setDummy = (patch: DummySettings) => updateNode(node.id, { extra: { ...node.data.extra, dummy: { ...settings, ...patch } } });
  const hasLocal = LOCAL_ENV in ((meta.environments as Json) ?? {});

  if (env !== LOCAL_ENV) {
    if (!settings.enabled) return null;
    return (
      <div className="callout info">
        <Info size={16} />
        <div className="small">
          Dummy data is on, but only used in the <span className="mono">local</span> environment. Here this node uses its real settings below.
          {hasLocal && <> <button className="link small" onClick={() => setEnv(LOCAL_ENV)}>Switch to local</button></>}
        </div>
      </div>
    );
  }

  const data = settings.data ?? "";
  const { count, error } = countRecords(data);
  const upload = async (file: File) => {
    setFileError(null);
    const text = await file.text();
    // A JSON array is accepted too, and kept as JSON Lines so it can be edited line by line.
    let lines = text;
    if (text.trim().startsWith("[")) {
      try {
        lines = (JSON.parse(text) as unknown[]).map((r) => JSON.stringify(r)).join("\n");
      } catch {
        return setFileError(`${file.name} is not valid JSON`);
      }
    }
    setDummy({ enabled: true, data: lines });
  };
  return (
    <div className="dummy-box">
      <label className="switch">
        <input type="checkbox" role="switch" checked={Boolean(settings.enabled)} onChange={(e) => setDummy({ enabled: e.target.checked })} />
        <span className="track" aria-hidden />
        <span>
          <b>Use dummy data</b>
          <span className="desc">
            {source
              ? "Hand uploaded records downstream instead of reading. Connection and options are not needed."
              : "Write nothing: Preview shows the rows this sink would get. Connection and options are not needed."}
          </span>
        </span>
      </label>
      {settings.enabled && source && (
        <>
          <div className="field" style={{ marginTop: 12 }}>
            <label className="row" style={{ fontWeight: 500, color: "var(--muted)" }}>
              <FileJson size={13} />Records: JSON Lines, one JSON object per line, up to {MAX_DUMMY_RECORDS}
            </label>
            <input type="file" accept=".jsonl,.ndjson,.json,.txt" onChange={(e) => e.target.files?.[0] && upload(e.target.files[0])} />
            {fileError && <div className="desc error-text">{fileError}</div>}
          </div>
          <div className="field">
            <textarea
              className="mono"
              rows={6}
              spellCheck={false}
              value={data}
              placeholder={'{"order_id": 1, "amount": 25.5, "country": "IN"}\n{"order_id": 2, "amount": 9.0, "country": "US"}'}
              onChange={(e) => setDummy({ data: e.target.value })}
            />
            <div className={`desc${error ? " error-text" : ""}`}>
              {error ?? `${count} record${count === 1 ? "" : "s"}. Columns are inferred from them, or typed by the schema below.`}
            </div>
            {exactErrors(issues, `${path}.dummy.data`).map((e) => <div key={e} className="desc error-text">{e}</div>)}
          </div>
        </>
      )}
    </div>
  );
}

function TransformFields({ node, path }: { node: FlowNode; catalog: Catalog; path: string }) {
  const { nodes, edges, analysis, issues, setAlias, updateNode } = useEditor();
  const inputs = edges.filter((e) => e.target === node.id && !isTap(e));
  const tables: Record<string, string[]> = {};
  for (const edge of inputs) {
    const upstream = nodes.find((n) => n.id === edge.source);
    if (!upstream) continue;
    const name = edge.data?.alias || upstream.data.specId;
    tables[name] = (analysis[upstream.data.specId]?.schema?.fields ?? []).map((f) => f.name);
  }
  return (
    <>
      <h3>Inputs</h3>
      {inputs.length === 0 && <div className="muted small field">Connect a source or transform to this node's left handle.</div>}
      <div className="chips">
        {inputs.map((edge) => {
          const upstream = nodes.find((n) => n.id === edge.source)!;
          const cols = analysis[upstream.data.specId]?.schema?.fields;
          return (
            <div className="chip" key={edge.id} title={`Table name for ${upstream.data.specId} in SQL`}>
              <div className="row">
                <input value={edge.data?.alias ?? ""} placeholder={upstream.data.specId} onChange={(e) => setAlias(edge.id, e.target.value)} />
                <span className="muted">← {upstream.data.specId}</span>
              </div>
              <div className="cols">{cols ? cols.map((f) => f.name).join(", ") : "columns appear after the check"}</div>
            </div>
          );
        })}
      </div>
      {node.data.type === "sql" && (
        <>
          <h3>SQL</h3>
          <SqlEditor
            value={String(node.data.options.sql ?? "")}
            tables={tables}
            problems={issues.filter((i) => i.path.startsWith(path)).map((i) => i.message).concat(analysis[node.data.specId]?.error ?? [])}
            onChange={(sql) => updateNode(node.id, { options: { ...node.data.options, sql: sql || undefined } })}
          />
          <div className="desc small" style={{ marginTop: 4 }}>
            Refer to inputs by the names above. {Object.keys(tables).length > 0 && `Try: SELECT * FROM ${Object.keys(tables)[0]}`}
          </div>
        </>
      )}
    </>
  );
}

function SinkFields({ node, path }: { node: FlowNode; catalog: Catalog; path: string }) {
  return <ConnectionField node={node} path={path} />;
}

function TargetPicker({ label, category, value, onChange, catalog, errors }: {
  label: string; category: Category; value: Json | undefined; onChange(v: Json | undefined): void; catalog: Catalog; errors: Record<string, string[]>;
}) {
  const connections = useConnections();
  const meta = value ? plugin(catalog, category, String(value.type)) : undefined;
  return (
    <div className="subform">
      <div className="row field">
        <select value={value ? String(value.type) : ""} onChange={(e) => onChange(e.target.value ? { type: e.target.value } : undefined)}>
          <option value="">{label}…</option>
          {catalog.components[category].map((p) => <option key={p.type} value={p.type}>{p.display_name}</option>)}
        </select>
        {value && (
          <select value={String(value.connection ?? "")} onChange={(e) => onChange({ ...value, connection: e.target.value || undefined })}>
            <option value="">no connection</option>
            {connections.map((c) => <option key={c.name} value={c.name}>{c.name}</option>)}
          </select>
        )}
      </div>
      {value && meta && (
        <SchemaForm schema={meta.options_schema} value={(value.options as Json) ?? {}} errors={errors} onChange={(options) => onChange({ ...value, options })} />
      )}
    </div>
  );
}

function QualityFields({ node, catalog, path }: { node: FlowNode; catalog: Catalog; path: string }) {
  const { nodes, edges, issues, updateNode } = useEditor();
  const extra = node.data.extra;
  const checks = (extra.checks as Json[]) ?? [];
  const results = (extra.results_sinks as Json[]) ?? [];
  const setExtra = (patch: Json) => updateNode(node.id, { extra: { ...extra, ...patch } });
  const tap = edges.find((e) => e.source === node.id && isTap(e));
  const target = tap ? nodes.find((n) => n.id === tap.target) : undefined;

  return (
    <>
      <div className="field small">
        {target ? (
          <>
            {target.data.kind === "sink" ? "Checks data going into sink " : "Checks the output of "}
            <b>{target.data.specId}</b>
          </>
        ) : (
          <div className="callout warn"><Link2Off size={16} /><div>Not attached yet. Drag from this block's bottom handle to the amber handle on top of the node it should check.</div></div>
        )}
      </div>
      <div className="field">
        <label>When checks fail</label>
        <select value={String(extra.action ?? "observe")} onChange={(e) => setExtra({ action: e.target.value })}>
          <option value="observe">observe — record results only</option>
          <option value="fail">fail — stop before any sink writes</option>
          <option value="drop">drop — remove failing rows</option>
          <option value="quarantine">quarantine — divert failing rows to a sink</option>
        </select>
      </div>
      <h3>Checks</h3>
      {checks.map((check, i) => {
        const meta = plugin(catalog, "check", String(check.type));
        return (
          <div className="subform" key={i}>
            <div className="row field">
              <select value={String(check.type)} onChange={(e) => setExtra({ checks: checks.map((c, j) => (j === i ? { type: e.target.value } : c)) })}>
                {catalog.components.check.map((p) => <option key={p.type} value={p.type}>{p.type}</option>)}
              </select>
              <button className="link small danger" onClick={() => setExtra({ checks: checks.filter((_, j) => j !== i) })}>Remove</button>
            </div>
            {meta && (
              <SchemaForm
                schema={meta.options_schema}
                value={check}
                exclude={["type"]}
                errors={fieldErrors(issues, `${path}.checks[${i}]`)}
                onChange={(v) => setExtra({ checks: checks.map((c, j) => (j === i ? { ...v, type: check.type } : c)) })}
              />
            )}
          </div>
        );
      })}
      <button className="sm" onClick={() => setExtra({ checks: [...checks, { type: "not_null" }] })}><Plus size={13} />Add check</button>

      <h3>Results go to</h3>
      {results.map((t, i) => (
        <TargetPicker
          key={i}
          label="metrics sink"
          category="metrics_sink"
          value={t}
          catalog={catalog}
          errors={fieldErrors(issues, `${path}.results_sinks[${i}].options`)}
          onChange={(v) => setExtra({ results_sinks: v ? results.map((x, j) => (j === i ? v : x)) : results.filter((_, j) => j !== i) })}
        />
      ))}
      <button className="sm" onClick={() => setExtra({ results_sinks: [...results, { type: "console" }] })}><Plus size={13} />Add results sink</button>
      <div className="desc small" style={{ marginTop: 4 }}>Runs started from here always keep results in the run history too.</div>

      {extra.action === "quarantine" && (
        <>
          <h3>Quarantine sink</h3>
          <TargetPicker
            label="sink"
            category="sink"
            value={extra.quarantine_sink as Json | undefined}
            catalog={catalog}
            errors={fieldErrors(issues, `${path}.quarantine_sink.options`)}
            onChange={(v) => setExtra({ quarantine_sink: v })}
          />
        </>
      )}
    </>
  );
}

