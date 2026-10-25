import { useEffect, useState } from "react";

import type { Json } from "../api/types";

/** "true"/"false" -> boolean, numeric text -> number, anything else stays a string. */
export function parseScalar(text: string): unknown {
  if (text === "true") return true;
  if (text === "false") return false;
  if (text.trim() !== "" && !Number.isNaN(Number(text)) && !/^0\d/.test(text)) return Number(text);
  return text;
}

type Row = { key: string; value: string };

export function KeyValueEditor({ value, onChange, coerce = true }: { value: Json; onChange(v: Json): void; coerce?: boolean }) {
  const [rows, setRows] = useState<Row[]>(() => toRows(value));
  useEffect(() => {
    // Re-sync when the value changes from outside (e.g. YAML apply), not on our own edits.
    if (JSON.stringify(fromRows(rows, coerce)) !== JSON.stringify(value)) setRows(toRows(value));
    // eslint-disable-next-line react-hooks/exhaustive-deps
  }, [JSON.stringify(value)]);

  const update = (next: Row[]) => {
    setRows(next);
    onChange(fromRows(next, coerce));
  };
  return (
    <div>
      {rows.map((row, i) => (
        <div className="kv" key={i}>
          <input value={row.key} placeholder="key" onChange={(e) => update(rows.map((r, j) => (j === i ? { ...r, key: e.target.value } : r)))} />
          <input value={row.value} placeholder="value" onChange={(e) => update(rows.map((r, j) => (j === i ? { ...r, value: e.target.value } : r)))} />
          <button type="button" className="ghost icon" title="Remove" aria-label="Remove" onClick={() => update(rows.filter((_, j) => j !== i))}>
            ×
          </button>
        </div>
      ))}
      <button type="button" className="sm" onClick={() => setRows([...rows, { key: "", value: "" }])}>
        + Add
      </button>
    </div>
  );
}

function toRows(value: Json): Row[] {
  return Object.entries(value ?? {}).map(([key, v]) => ({ key, value: typeof v === "string" ? v : JSON.stringify(v) }));
}

function fromRows(rows: Row[], coerce: boolean): Json {
  return Object.fromEntries(rows.filter((r) => r.key).map((r) => [r.key, coerce ? parseScalar(r.value) : r.value]));
}
