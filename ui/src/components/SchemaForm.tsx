/**
 * Renders a form for the JSON Schema Pydantic produces for a plugin's Options model.
 *
 * Handles exactly the shapes Pydantic emits: optional fields (anyOf [T, null]), unions
 * of scalars, enums, maps (additionalProperties), lists of scalars or of $ref objects.
 * Required fields and fields with a value are shown; the rest fold under "More options".
 */
import { useState, type ReactNode } from "react";

import type { Json, JsonSchema } from "../api/types";
import { KeyValueEditor, parseScalar } from "./KeyValueEditor";

export interface SchemaFormProps {
  schema: JsonSchema;
  value: Json;
  onChange(value: Json): void;
  root?: JsonSchema; // where $defs live (defaults to schema)
  exclude?: string[];
  errors?: Record<string, string[]>;
  custom?: Record<string, (value: unknown, onChange: (v: unknown) => void) => ReactNode>;
}

export function SchemaForm({ schema, value, onChange, root, exclude = [], errors = {}, custom = {} }: SchemaFormProps) {
  const [showAll, setShowAll] = useState(false);
  const base = root ?? schema;
  const props = Object.entries(schema.properties ?? {}).filter(([k]) => !exclude.includes(k));
  const required = new Set(schema.required ?? []);
  const primary = props.filter(([k]) => required.has(k) || value[k] !== undefined || errors[k] || custom[k]);
  const secondary = props.filter((p) => !primary.includes(p));

  const set = (key: string, v: unknown) => {
    const next = { ...value };
    if (v === undefined || v === "" || (Array.isArray(v) && v.length === 0)) delete next[key];
    else next[key] = v;
    onChange(next);
  };

  const render = ([key, prop]: [string, JsonSchema]) => (
    <Field
      key={key}
      name={key}
      schema={prop}
      root={base}
      value={value[key]}
      required={required.has(key)}
      errors={errors[key]}
      onChange={(v) => set(key, v)}
      custom={custom[key]}
    />
  );

  return (
    <div>
      {primary.map(render)}
      {secondary.length > 0 && (
        <>
          <button type="button" className="link small" onClick={() => setShowAll(!showAll)}>
            {showAll ? "Fewer options" : `More options (${secondary.length})`}
          </button>
          {showAll && <div style={{ marginTop: 8 }}>{secondary.map(render)}</div>}
        </>
      )}
    </div>
  );
}

function deref(schema: JsonSchema, root: JsonSchema): JsonSchema {
  if (schema.$ref?.startsWith("#/$defs/")) return root.$defs?.[schema.$ref.slice(8)] ?? schema;
  return schema;
}

/** Collapses Pydantic's Optional/Union encoding to one effective schema. */
export function effective(schema: JsonSchema, root: JsonSchema): JsonSchema & { scalarUnion?: boolean } {
  const s = deref(schema, root);
  if (!s.anyOf) return s;
  const variants = s.anyOf.map((v) => deref(v, root)).filter((v) => v.type !== "null");
  const meta = { title: s.title, description: s.description, default: s.default, format: s.format };
  if (variants.length === 1) return { ...variants[0], ...stripUndefined(meta) };
  const scalars = variants.every((v) => ["string", "integer", "number", "boolean"].includes(String(v.type)));
  return { ...stripUndefined(meta), type: "string", scalarUnion: scalars };
}

function stripUndefined(o: Json): Json {
  return Object.fromEntries(Object.entries(o).filter(([, v]) => v !== undefined));
}

interface FieldProps {
  name: string;
  schema: JsonSchema;
  root: JsonSchema;
  value: unknown;
  required: boolean;
  errors?: string[];
  onChange(v: unknown): void;
  custom?: (value: unknown, onChange: (v: unknown) => void) => ReactNode;
}

function Field({ name, schema: raw, root, value, required, errors, onChange, custom }: FieldProps) {
  const schema = effective(raw, root);
  const label = schema.title ?? name;
  const placeholder = schema.default !== undefined && schema.default !== null ? String(JSON.stringify(schema.default)).replace(/^"|"$/g, "") : "";
  let input: ReactNode;

  if (custom) {
    input = custom(value, onChange);
  } else if (schema.enum) {
    input = (
      <select value={value === undefined ? "" : String(value)} onChange={(e) => onChange(e.target.value || undefined)}>
        <option value="">{placeholder ? `default (${placeholder})` : "—"}</option>
        {schema.enum.map((option) => (
          <option key={String(option)} value={String(option)}>
            {String(option)}
          </option>
        ))}
      </select>
    );
  } else if (schema.type === "boolean") {
    const checked = value === undefined ? Boolean(schema.default) : Boolean(value);
    input = (
      <label className="row" style={{ color: "var(--text)", fontSize: 13 }}>
        <input type="checkbox" checked={checked} onChange={(e) => onChange(e.target.checked)} />
        {label}
      </label>
    );
  } else if (schema.type === "integer" || schema.type === "number") {
    input = (
      <input
        type="number"
        value={value === undefined ? "" : String(value)}
        placeholder={placeholder}
        min={schema.minimum}
        onChange={(e) => onChange(e.target.value === "" ? undefined : Number(e.target.value))}
      />
    );
  } else if (schema.type === "object" && schema.properties) {
    input = (
      <div className="subform">
        <SchemaForm schema={schema} root={root} value={(value as Json) ?? {}} onChange={(v) => onChange(Object.keys(v).length ? v : undefined)} />
      </div>
    );
  } else if (schema.type === "object") {
    input = <KeyValueEditor value={(value as Json) ?? {}} onChange={(v) => onChange(Object.keys(v).length ? v : undefined)} />;
  } else if (schema.type === "array") {
    const items = effective(schema.items ?? {}, root);
    if (items.type === "object" && items.properties) {
      const list = (value as Json[]) ?? [];
      input = (
        <div>
          {list.map((item, i) => (
            <div className="subform" key={i}>
              <SchemaForm
                schema={items}
                root={root}
                value={item}
                onChange={(v) => onChange(list.map((x, j) => (j === i ? v : x)))}
              />
              <button type="button" className="link small danger" onClick={() => onChange(list.filter((_, j) => j !== i))}>
                Remove
              </button>
            </div>
          ))}
          <button type="button" className="small" onClick={() => onChange([...list, {}])}>
            + Add
          </button>
        </div>
      );
    } else {
      input = (
        <ListInput
          value={(value as unknown[]) ?? []}
          numeric={items.type === "integer" || items.type === "number"}
          onChange={onChange}
          placeholder={items.enum ? items.enum.join(", ") : "comma separated"}
        />
      );
    }
  } else {
    const text = value === undefined ? "" : String(value);
    const multiline = schema.format === "sql" || text.includes("\n");
    const change = (t: string) => onChange(t === "" ? undefined : schema.scalarUnion ? parseScalar(t) : t);
    input = multiline ? (
      <textarea rows={5} value={text} placeholder={placeholder} onChange={(e) => change(e.target.value)} />
    ) : (
      <input value={text} placeholder={placeholder} onChange={(e) => change(e.target.value)} />
    );
  }

  return (
    <div className={`field${errors?.length ? " invalid" : ""}`}>
      {schema.type !== "boolean" && (
        <label>
          {label}
          {required && " *"}
        </label>
      )}
      {input}
      {schema.description && <div className="desc">{schema.description}</div>}
      {errors?.map((e) => (
        <div key={e} className="desc error-text">
          {e}
        </div>
      ))}
    </div>
  );
}

function ListInput({ value, numeric, onChange, placeholder }: { value: unknown[]; numeric: boolean; onChange(v: unknown): void; placeholder: string }) {
  const [text, setText] = useState(value.join(", "));
  const commit = (t: string) => {
    const parts = t.split(",").map((p) => p.trim()).filter(Boolean);
    onChange(parts.length ? parts.map((p) => (numeric ? Number(p) : parseScalar(p))) : undefined);
  };
  return <input value={text} placeholder={placeholder} onChange={(e) => setText(e.target.value)} onBlur={(e) => commit(e.target.value)} />;
}
