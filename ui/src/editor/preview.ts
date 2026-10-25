import { ApiError, api } from "../api/client";
import { layoutOf, toSpec } from "../model/graph";
import { useEditor } from "../store/editor";

export function currentSpec() {
  const { nodes, edges, meta } = useEditor.getState();
  return toSpec(nodes, edges, meta);
}

export function currentLayout() {
  return layoutOf(useEditor.getState().nodes);
}

export async function runPreview(specId: string): Promise<void> {
  const { env, samples, set } = useEditor.getState();
  set({ drawer: "preview", preview: { specId, loading: true } });
  const sample_data = Object.fromEntries(Object.entries(samples).map(([k, v]) => [k, v.sample_id]));
  try {
    const result = await api.preview({ spec: currentSpec(), env, node_id: specId, limit: 50, sample_data });
    useEditor.getState().set({ preview: { specId, loading: false, result } });
  } catch (error) {
    const message = error instanceof ApiError ? error.message : String(error);
    useEditor.getState().set({ preview: { specId, loading: false, error: message } });
  }
}
