import { loader } from "@monaco-editor/react";
import * as monaco from "monaco-editor";
import EditorWorker from "monaco-editor/esm/vs/editor/editor.worker?worker";

import { onThemeChange, resolvedTheme } from "../theme";

// Bundle Monaco locally instead of loading it from a CDN at runtime.
self.MonacoEnvironment = { getWorker: () => new EditorWorker() };
loader.config({ monaco });

export const monacoTheme = () => (resolvedTheme() === "dark" ? "vs-dark" : "vs");
// Monaco's theme is global: switch every open editor when the app theme changes.
onThemeChange(() => monaco.editor.setTheme(monacoTheme()));

/** Tables (input aliases) and their columns visible to the SQL editor that has focus. */
export const sqlContext: { tables: Record<string, string[]> } = { tables: {} };

let registered = false;
export function registerSqlCompletions(): void {
  if (registered) return;
  registered = true;
  monaco.languages.registerCompletionItemProvider("sql", {
    triggerCharacters: [".", " "],
    provideCompletionItems(model, position) {
      const word = model.getWordUntilPosition(position);
      const range = new monaco.Range(position.lineNumber, word.startColumn, position.lineNumber, word.endColumn);
      const before = model.getLineContent(position.lineNumber).slice(0, word.startColumn - 1);
      const qualifier = /([A-Za-z_][A-Za-z0-9_]*)\.$/.exec(before)?.[1];
      const { tables } = sqlContext;
      const Kind = monaco.languages.CompletionItemKind;
      if (qualifier && tables[qualifier]) {
        return { suggestions: tables[qualifier].map((c) => ({ label: c, kind: Kind.Field, insertText: c, range })) };
      }
      const suggestions = [
        ...Object.keys(tables).map((t) => ({ label: t, kind: Kind.Struct, insertText: t, range, detail: "input" })),
        ...Object.entries(tables).flatMap(([t, cols]) =>
          cols.map((c) => ({ label: c, kind: Kind.Field, insertText: c, range, detail: t })),
        ),
      ];
      return { suggestions };
    },
  });
}

export { monaco };
