import { CircleAlert, Settings2 } from "lucide-react";
import { useState } from "react";

import type { Json } from "../../api/types";
import { IconChip } from "../../components/icons";
import { KeyValueEditor } from "../../components/KeyValueEditor";
import { useEditor } from "../../store/editor";

/** Shown when no node is selected: pipeline-level settings and environments. */
export function PipelinePanel() {
  const { meta, setMeta, issues } = useEditor();
  const [newEnv, setNewEnv] = useState("");
  const pipeline = meta.pipeline;
  const streaming = (pipeline.streaming as Json) ?? {};
  const envs = (meta.environments as Record<string, Json>) ?? {};
  const setPipeline = (patch: Json) => setMeta({ ...meta, pipeline: { ...pipeline, ...patch } });
  const setEnvs = (next: Record<string, Json>) => setMeta({ ...meta, environments: next });
  const global = issues.filter((i) => !i.node_id);

  return (
    <div>
      <div className="panel-head">
        <IconChip icon={Settings2} color="var(--accent)" size={18} large />
        <div className="text">
          <div className="title">Pipeline settings</div>
          <div className="sub">Select a node on the canvas to edit it</div>
        </div>
      </div>
      {global.length > 0 && (
        <div className="callout danger">
          <CircleAlert size={16} />
          <div>{global.map((i) => <div key={i.path + i.message}>{i.path}: {i.message}</div>)}</div>
        </div>
      )}
      <div className="field">
        <label>Pipeline id *</label>
        <input value={String(pipeline.id ?? "")} onChange={(e) => setPipeline({ id: e.target.value })} />
      </div>
      <div className="field">
        <label>Mode</label>
        <select value={String(pipeline.mode ?? "batch")} onChange={(e) => setPipeline({ mode: e.target.value })}>
          <option value="batch">batch</option>
          <option value="streaming">streaming</option>
        </select>
      </div>
      {pipeline.mode === "streaming" && (
        <div className="subform">
          <div className="field">
            <label>Checkpoint location *</label>
            <input value={String(streaming.checkpoint_location ?? "")} onChange={(e) => setPipeline({ streaming: { ...streaming, checkpoint_location: e.target.value || undefined } })} />
          </div>
          <div className="field">
            <label>Trigger</label>
            <select value={String(streaming.trigger ?? "processing_time")} onChange={(e) => setPipeline({ streaming: { ...streaming, trigger: e.target.value } })}>
              <option value="processing_time">every interval</option>
              <option value="available_now">process available data, then stop</option>
              <option value="once">one micro-batch</option>
            </select>
          </div>
          {(streaming.trigger ?? "processing_time") === "processing_time" && (
            <div className="field">
              <label>Interval</label>
              <input value={String(streaming.interval ?? "")} placeholder="10 seconds" onChange={(e) => setPipeline({ streaming: { ...streaming, interval: e.target.value || undefined } })} />
            </div>
          )}
        </div>
      )}

      <h3>Environments</h3>
      <div className="desc small field">Variables are used as <span className="mono">{"${env.NAME}"}</span>. Connections are managed on the Connections page.</div>
      {Object.entries(envs).map(([name, env]) => (
        <div className="subform" key={name}>
          <div className="row">
            <span className="pill info">{name}</span>
            <span className="spacer" />
            <button className="link small danger" onClick={() => { const next = { ...envs }; delete next[name]; setEnvs(next); }}>Remove</button>
          </div>
          <KeyValueEditor value={(env.vars as Json) ?? {}} onChange={(vars) => setEnvs({ ...envs, [name]: { ...env, vars } })} />
        </div>
      ))}
      <div className="row">
        <input value={newEnv} placeholder="New environment, e.g. prod" onChange={(e) => setNewEnv(e.target.value)} />
        <button disabled={!newEnv || newEnv in envs} onClick={() => { setEnvs({ ...envs, [newEnv]: { vars: {} } }); setNewEnv(""); }}>Add</button>
      </div>
      <p className="muted small">Runtime resources and Spark settings can be edited in the YAML tab.</p>
    </div>
  );
}
