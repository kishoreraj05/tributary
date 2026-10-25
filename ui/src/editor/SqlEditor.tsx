import Editor, { type OnMount } from "@monaco-editor/react";
import { useEffect, useRef } from "react";

import { monaco, monacoTheme, registerSqlCompletions, sqlContext } from "../components/monaco";

interface Props {
  value: string;
  onChange(value: string): void;
  tables: Record<string, string[]>;
  problems: string[];
}

/** Monaco SQL editor: completes input tables/columns and underlines what validation reports. */
export function SqlEditor({ value, onChange, tables, problems }: Props) {
  const editorRef = useRef<Parameters<OnMount>[0] | null>(null);

  useEffect(() => {
    sqlContext.tables = tables;
  }, [tables]);

  useEffect(() => {
    const model = editorRef.current?.getModel();
    if (!model) return;
    const markers: Parameters<typeof monaco.editor.setModelMarkers>[2] = [];
    for (const message of problems) {
      // sqlglot: "line 2, col 7" (1-based); Spark analysis: "line 1 pos 66" (0-based).
      const sqlglot = /line (\d+), col (\d+)/.exec(message);
      const spark = /line (\d+) pos (\d+)/.exec(message);
      const at = sqlglot ?? (spark ? [spark[0], spark[1], String(Number(spark[2]) + 1)] : null);
      const table = /unknown table '([^']+)'/.exec(message);
      if (at) {
        const line = Number(at[1]);
        const col = Number(at[2]);
        markers.push({ message, severity: monaco.MarkerSeverity.Error, startLineNumber: line, endLineNumber: line, startColumn: Math.max(1, col - 1), endColumn: col + 4 });
      } else if (table) {
        for (const match of model.findMatches(`\\b${table[1].replace(/\./g, "\\.")}\\b`, false, true, false, null, false)) {
          markers.push({ message, severity: monaco.MarkerSeverity.Error, ...match.range });
        }
      }
    }
    monaco.editor.setModelMarkers(model, "tributary", markers);
  }, [problems, value]);

  return (
    <div className="sql-box">
      <Editor
        height="190px"
        language="sql"
        theme={monacoTheme()}
        value={value}
        onChange={(v) => onChange(v ?? "")}
        onMount={(editor) => {
          editorRef.current = editor;
          registerSqlCompletions();
          editor.onDidFocusEditorText(() => (sqlContext.tables = tables));
        }}
        options={{ minimap: { enabled: false }, fontSize: 12.5, lineNumbers: "on", scrollBeyondLastLine: false, wordWrap: "on", automaticLayout: true }}
      />
    </div>
  );
}
