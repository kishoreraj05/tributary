# tributary-ui

A React + TypeScript canvas for drawing Tributary pipelines. Everything it shows comes from the backend:

- the palette and every node form come from `GET /api/v1/components`, which is generated from the engine's plugins;
- validation, previews and runs are backend calls.

Install a new plugin and it appears in the UI with no frontend change.

## Run it

```bash
npm install
npm run dev        # http://localhost:5173, proxies /api (HTTP and WebSocket) to the backend on :8000
```

Start the backend first (`backend/.venv/bin/tributary-backend serve`). Set `TRIBUTARY_API` to point the proxy at another backend.

```bash
npm test           # graph <-> spec conversion and drawing rules
npm run build      # typecheck + production bundle
```

## What's in it

- **Canvas** ([React Flow](https://reactflow.dev)):
  - Drag sources, transforms, sinks and quality blocks from the palette, or click a palette item to add it.
  - Connect a node's right handle to the next node's left handle. The edge label is the table name used in SQL, editable on the transform.
  - Attach a quality block by dragging from its bottom handle to the amber handle on top of a node. A source or transform becomes `attach_to`; a sink becomes `before`.
  - The canvas refuses invalid edges: cycles, a second input into a sink, or inputs into a source.
- **Node panel:**
  - Forms are built from each plugin's JSON Schema. Required fields and fields you've set are shown; the rest fold under "More options".
  - Field-level errors from validation appear next to the field.
  - Pick a stored connection, a schema type and a sample-data file per source.
  - The SQL editor (Monaco) shows the input tables with their columns, autocompletes them, and underlines unknown tables and columns.
- **Live validation:** each edit is checked structurally (about 0.5 s after you stop typing). Once that passes, column-level analysis runs on the backend's Spark session. Errors appear as badges on nodes and in the Problems tab.
- **Drawer:**
  - **Preview:** ▶ Test on any node shows sample rows and DQ results; sinks are a dry run.
  - **Problems:** every structural and column-level error.
  - **YAML:** two-way. Edit the spec and apply it back to the canvas.
  - **Plan:** cache points, quality gates and the chosen Spark resources.
  - **Runs:** live status and logs over WebSocket, DQ results, cancel.
- **Saving:** the draft autosaves after every change. *Save version* validates for every environment and freezes it; *Run* starts a saved version in a chosen environment.
  - **Version status:** the toolbar pill is green when the draft matches the latest version. It turns amber ("draft has unsaved changes") and lists the nodes that differ when it doesn't. Layout moves and formatting-only differences don't count.
  - **Run dialog:** when the draft has changes the latest version lacks, the dialog lists them and offers *Save as vN+1 & run* or *Run vN as saved*.
- **Connections page:** settings per environment. Fields that look secret (password, token, …) are marked secret automatically, sent encrypted and never shown again.

## Code map

| Path | What |
|---|---|
| `src/model/graph.ts` | Graph ↔ spec mapping, auto layout, drawing rules (unit-tested) |
| `src/store/editor.ts` | Zustand store for the editor |
| `src/components/SchemaForm.tsx` | Form renderer for Pydantic-generated JSON Schema |
| `src/editor/` | Canvas, nodes, palette, panels, SQL editor, drawer |
| `src/api/` | Typed API client, run-events WebSocket, catalog hooks |

## Known limits (v0.1)

- The bundle is large (Monaco ships every language). Code-split it before production.
- There is no undo/redo on the canvas yet. Versions and the YAML tab are the way back.
- The "More options" fold and form layout are generic. Per-plugin UI hints are a possible next step.
- Tested in Chromium (the desktop app's browser pane) only.
