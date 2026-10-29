import { describe, expect, it } from "vitest";

import {
  connectionError,
  countRecords,
  dataEdge,
  describeDiff,
  fromSpec,
  isDummy,
  isSameSpec,
  prune,
  specDiff,
  tapEdge,
  toSpec,
  uniqueSpecId,
  type FlowNode,
} from "./graph";

const SPEC = {
  version: 1,
  pipeline: { id: "orders_daily", mode: "batch" },
  environments: { dev: { vars: { out: "/tmp/out" } } },
  sources: [
    { id: "orders_src", type: "file", options: { path: "a.csv", format: "csv" }, schema: { kind: "ddl", ddl: "id INT" } },
    { id: "customers_src", type: "file", options: { path: "c.csv" } },
  ],
  transforms: [
    {
      id: "orders_clean",
      type: "sql",
      inputs: [{ node: "orders_src", as: "orders" }, { node: "customers_src" }],
      options: { sql: "SELECT * FROM orders JOIN customers_src" },
    },
  ],
  quality: [
    { id: "dq", attach_to: "orders_clean", action: "drop", checks: [{ type: "not_null", column: "id" }] },
    { id: "guard", before: "out", checks: [{ type: "row_count", min: 1 }] },
  ],
  sinks: [{ id: "out", type: "file", input: "orders_clean", options: { path: "${env.out}" } }],
  runtime: { dev: { master: "local[*]" } },
};

describe("spec <-> graph", () => {
  it("round-trips a spec unchanged", () => {
    const { nodes, edges, meta } = fromSpec(SPEC);
    expect(toSpec(nodes, edges, meta)).toEqual(SPEC);
  });

  it("maps quality taps to attach_to / before", () => {
    const { nodes, edges } = fromSpec(SPEC);
    const taps = edges.filter((e) => e.targetHandle === "dq");
    const name = (id: string) => nodes.find((n) => n.id === id)!.data.specId;
    expect(taps.map((t) => [name(t.source), name(t.target)])).toEqual([["dq", "orders_clean"], ["guard", "out"]]);
  });

  it("uses saved positions and lays out the rest by depth", () => {
    const { nodes } = fromSpec(SPEC, { out: { x: 999, y: 5 } });
    const pos = (id: string) => nodes.find((n) => n.data.specId === id)!.position;
    expect(pos("out")).toEqual({ x: 999, y: 5 });
    expect(pos("orders_src").x).toBeLessThan(pos("orders_clean").x);
  });

  it("keeps an unconnected sink's empty input so validation can flag it", () => {
    const { nodes, meta } = fromSpec(SPEC);
    const spec = toSpec(nodes, [], meta) as { sinks: { input: string }[]; transforms: { inputs: unknown[] }[] };
    expect(spec.sinks[0].input).toBe("");
    expect(spec.transforms[0].inputs).toEqual([]);
  });

  it("prunes empty values", () => {
    expect(prune({ a: "", b: null, c: {}, d: { e: undefined }, f: 0, g: false, h: ["x"] })).toEqual({ f: 0, g: false, h: ["x"] });
  });
});

describe("drawing rules", () => {
  const { nodes, edges } = fromSpec(SPEC);
  const id = (spec: string) => nodes.find((n) => n.data.specId === spec)!.id;

  it("rejects cycles, inputs into sources and second sink inputs", () => {
    expect(connectionError(nodes, edges, id("orders_clean"), id("orders_src"))).toBe("sources have no inputs");
    expect(connectionError(nodes, edges, id("orders_src"), id("out"))).toBe("a sink has one input");
    const loop = [...edges, dataEdge(id("orders_clean"), id("orders_src"))];
    expect(connectionError(nodes, loop, id("orders_src"), id("orders_clean"))).toBe("already connected");
  });

  it("allows one tap per quality block", () => {
    expect(connectionError(nodes, edges, id("dq"), id("orders_src"), "dq")).toBe("a quality block checks exactly one node");
    const fresh = edges.filter((e) => e.source !== id("dq"));
    expect(connectionError(nodes, fresh, id("dq"), id("orders_src"), "dq")).toBeNull();
    expect(connectionError(nodes, [...fresh, tapEdge(id("dq"), id("orders_src"))], id("dq"), id("out"), "dq")).not.toBeNull();
  });

  it("generates unique identifier ids", () => {
    const fake = [{ data: { specId: "kafka_1" } }, { data: { specId: "kafka_2" } }] as FlowNode[];
    expect(uniqueSpecId("kafka", fake)).toBe("kafka_3");
    expect(uniqueSpecId("1st-node", [])).toBe("_1st_node_1");
  });
});

describe("draft vs version diff", () => {
  it("ignores spelling differences and key order", () => {
    const spelled = { ...SPEC, transforms: [{ ...SPEC.transforms[0], inputs: [{ node: "orders_src", as: "orders" }, "customers_src"] }] };
    const reordered = Object.fromEntries(Object.entries(SPEC).reverse());
    expect(isSameSpec(specDiff(spelled, SPEC))).toBe(true);
    expect(isSameSpec(specDiff(reordered, SPEC))).toBe(true);
  });

  it("reports changed, added and removed nodes and settings", () => {
    const draft = JSON.parse(JSON.stringify(SPEC));
    draft.sinks[0].options.path = "/somewhere/else";
    draft.sinks.push({ id: "extra", type: "console", input: "orders_clean", options: {} });
    draft.quality = draft.quality.filter((q: { id: string }) => q.id !== "guard");
    draft.environments.dev.vars.out = "/tmp/other";
    const diff = specDiff(draft, SPEC);
    expect(diff).toEqual({ added: ["extra"], removed: ["guard"], changed: ["out"], settings: true });
    expect(describeDiff(diff)).toEqual(["changed: out", "added: extra", "removed: guard", "pipeline settings or environments"]);
  });

  it("canvas layout is not part of the spec", () => {
    const { nodes, edges, meta } = fromSpec(SPEC, { out: { x: 1, y: 1 } });
    nodes.forEach((n) => (n.position = { x: n.position.x + 50, y: 0 }));
    expect(isSameSpec(specDiff(toSpec(nodes, edges, meta), SPEC))).toBe(true);
  });
});

describe("dummy data (local environment)", () => {
  const spec = {
    version: 1,
    pipeline: { id: "p" },
    environments: { local: {}, dev: {} },
    sources: [{ id: "orders", type: "mongodb", options: {}, dummy: { enabled: true, data: '{"a": 1}' } }],
    transforms: [],
    sinks: [{ id: "out", type: "elasticsearch", input: "orders", options: {}, dummy: { enabled: false } }],
  };

  it("keeps the dummy settings through the graph, also when switched off", () => {
    const { nodes, edges, meta } = fromSpec(spec);
    expect(toSpec(nodes, edges, meta)).toEqual(spec);
  });

  it("only counts as dummy in the local environment, on sources and sinks", () => {
    const { nodes } = fromSpec(spec);
    const [source, sink] = nodes;
    expect(isDummy(source.data, "local")).toBe(true);
    expect(isDummy(source.data, "dev")).toBe(false);
    expect(isDummy(sink.data, "local")).toBe(false);
  });

  it("counts JSON Lines records and says what is wrong", () => {
    expect(countRecords('{"a": 1}\n\n{"a": 2}\n')).toEqual({ count: 2 });
    expect(countRecords('{"a": 1}\n{"a": ')).toEqual({ count: 1, error: "line 2 is not JSON" });
    expect(countRecords("[1]")).toEqual({ count: 0, error: "line 1 is not a JSON object" });
    expect(countRecords(Array(1001).fill('{"a": 1}').join("\n")).error).toBe("at most 1000 records (1001 given)");
  });
});
