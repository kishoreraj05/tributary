import { useEffect, useState } from "react";

import { api } from "./client";
import type { Catalog, Category, Connection, PluginMeta } from "./types";

let catalogPromise: Promise<Catalog> | null = null;

export function useCatalog(): Catalog | null {
  const [catalog, setCatalog] = useState<Catalog | null>(null);
  useEffect(() => {
    catalogPromise ??= api.components();
    catalogPromise.then(setCatalog).catch(() => (catalogPromise = null));
  }, []);
  return catalog;
}

export function plugin(catalog: Catalog | null, category: Category, type: string): PluginMeta | undefined {
  return catalog?.components[category]?.find((p) => p.type === type);
}

export function useConnections(): Connection[] {
  const [connections, setConnections] = useState<Connection[]>([]);
  useEffect(() => {
    api.connections().then(setConnections).catch(() => setConnections([]));
  }, []);
  return connections;
}
