#!/usr/bin/env node
/**
 * Regenerates the README screenshots in docs/images/.
 *
 *   node scripts/capture-screenshots.mjs
 *
 * Needs the backend (:8000) and the UI dev server (:5173) running, and Google Chrome.
 * It creates a demo copy of examples/csv_to_parquet.yaml, saves a version, runs it
 * once, captures each screen with headless Chrome (a throwaway profile, driven over the
 * DevTools protocol), then deletes the demo pipeline. No npm dependencies.
 *
 * Env: TRIBUTARY_UI (default http://localhost:5173), CHROME (path to the browser binary).
 */
import { spawn } from "node:child_process";
import { mkdtemp, mkdir, readFile, rm, writeFile } from "node:fs/promises";
import os from "node:os";
import path from "node:path";
import { fileURLToPath } from "node:url";

const ROOT = path.resolve(path.dirname(fileURLToPath(import.meta.url)), "..");
const UI = process.env.TRIBUTARY_UI ?? "http://localhost:5173";
const CHROME = process.env.CHROME ?? "/Applications/Google Chrome.app/Contents/MacOS/Google Chrome";
const OUT = path.join(ROOT, "docs/images");
const WIDTH = 1600;
const HEIGHT = 1000;
const DEMO_NAME = "Orders daily";

const sleep = (ms) => new Promise((r) => setTimeout(r, ms));

async function api(method, route, body) {
  const res = await fetch(`${UI}/api/v1${route}`, {
    method,
    headers: body ? { "content-type": "application/json" } : {},
    body: body ? JSON.stringify(body) : undefined,
  });
  if (!res.ok && res.status !== 404) throw new Error(`${method} ${route}: ${res.status} ${await res.text()}`);
  return res.status === 204 ? null : res.json();
}

async function createDemo() {
  for (const p of await api("GET", "/pipelines")) if (p.name === DEMO_NAME) await api("DELETE", `/pipelines/${p.id}`);
  const content = await readFile(path.join(ROOT, "examples/csv_to_parquet.yaml"), "utf8");
  const pipeline = await api("POST", "/pipelines/import", { name: DEMO_NAME, content, format: "yaml" });
  await api("POST", `/pipelines/${pipeline.id}/versions`, { message: "Join orders with customers, quarantine bad rows" });
  const run = await api("POST", `/pipelines/${pipeline.id}/runs`, { env: "dev" });
  for (let i = 0; i < 120; i++) {
    const { status } = await api("GET", `/runs/${run.id}`);
    if (status === "succeeded") return pipeline.id;
    if (status === "failed" || status === "cancelled") throw new Error(`demo run ${status}`);
    await sleep(1000);
  }
  throw new Error("demo run did not finish");
}

/** Minimal Chrome DevTools Protocol client over the built-in WebSocket. */
class Cdp {
  static async connect(url) {
    const cdp = new Cdp(new WebSocket(url));
    await new Promise((resolve, reject) => {
      cdp.ws.onopen = resolve;
      cdp.ws.onerror = reject;
    });
    return cdp;
  }
  constructor(ws) {
    this.ws = ws;
    this.id = 0;
    this.pending = new Map();
    this.waiters = [];
    ws.onmessage = ({ data }) => {
      const msg = JSON.parse(data);
      if (msg.id && this.pending.has(msg.id)) {
        const { resolve, reject } = this.pending.get(msg.id);
        this.pending.delete(msg.id);
        msg.error ? reject(new Error(msg.error.message)) : resolve(msg.result);
      } else if (msg.method) {
        this.waiters = this.waiters.filter((w) => !(w.method === msg.method && (w.resolve(msg.params), true)));
      }
    };
  }
  send(method, params = {}) {
    const id = ++this.id;
    this.ws.send(JSON.stringify({ id, method, params }));
    return new Promise((resolve, reject) => this.pending.set(id, { resolve, reject }));
  }
  once(method) {
    return new Promise((resolve) => this.waiters.push({ method, resolve }));
  }
  async eval(expression) {
    const { result, exceptionDetails } = await this.send("Runtime.evaluate", { expression, awaitPromise: true, returnByValue: true });
    if (exceptionDetails) throw new Error(exceptionDetails.exception?.description ?? exceptionDetails.text);
    return result.value;
  }
  async waitFor(expression, what, timeout = 60000) {
    const start = Date.now();
    while (Date.now() - start < timeout) {
      if (await this.eval(`Boolean(${expression})`)) return;
      await sleep(250);
    }
    throw new Error(`timed out waiting for ${what}`);
  }
}

async function launchChrome() {
  const profile = await mkdtemp(path.join(os.tmpdir(), "tributary-shots-"));
  const chrome = spawn(CHROME, [
    "--headless=new",
    "--remote-debugging-port=0",
    `--user-data-dir=${profile}`,
    `--window-size=${WIDTH},${HEIGHT}`,
    "--hide-scrollbars",
    "--no-first-run",
    "--no-default-browser-check",
    "about:blank",
  ]);
  const wsUrl = await new Promise((resolve, reject) => {
    let log = "";
    chrome.stderr.on("data", (chunk) => {
      log += chunk;
      const match = /DevTools listening on (ws:\/\/\S+)/.exec(log);
      if (match) resolve(match[1]);
    });
    chrome.on("exit", (code) => reject(new Error(`Chrome exited (${code}):\n${log}`)));
  });
  const port = new URL(wsUrl).port;
  const targets = await (await fetch(`http://127.0.0.1:${port}/json/list`)).json();
  const page = targets.find((t) => t.type === "page");
  const cdp = await Cdp.connect(page.webSocketDebuggerUrl);
  await cdp.send("Page.enable");
  await cdp.send("Runtime.enable");
  await cdp.send("Emulation.setDeviceMetricsOverride", { width: WIDTH, height: HEIGHT, deviceScaleFactor: 1, mobile: false });
  const close = async () => {
    chrome.kill();
    await rm(profile, { recursive: true, force: true }).catch(() => {});
  };
  return { cdp, close };
}

// --- page actions (run inside the app) ---------------------------------------------------
const clickNode = (specId) => `(() => {
  const node = [...document.querySelectorAll('.react-flow__node')].find((n) => n.querySelector('.title')?.textContent === ${JSON.stringify(specId)});
  node.dispatchEvent(new MouseEvent('click', { bubbles: true }));
  return true;
})()`;
const clickButton = (scope, label) => `(() => {
  const button = [...document.querySelectorAll(${JSON.stringify(scope)})].find((b) => b.textContent.trim() === ${JSON.stringify(label)} || b.getAttribute('aria-label') === ${JSON.stringify(label)});
  button.click();
  return true;
})()`;

async function open(cdp, pipelineId, theme) {
  await cdp.send("Emulation.setEmulatedMedia", { features: [{ name: "prefers-color-scheme", value: theme }] });
  const loaded = cdp.once("Page.loadEventFired");
  await cdp.send("Page.navigate", { url: `${UI}/pipelines/${pipelineId}` });
  await loaded;
  await cdp.eval(`localStorage.setItem('tributary.theme', ${JSON.stringify(theme)}); localStorage.removeItem('tributary.paletteWidth'); true`);
  const reloaded = cdp.once("Page.loadEventFired");
  await cdp.send("Page.reload");
  await reloaded;
  await cdp.waitFor("document.querySelectorAll('.react-flow__node').length >= 8", "the canvas");
  await cdp.waitFor("document.querySelector('.status.ok')", "validation to pass");
}

async function tidy(cdp) {
  await cdp.eval(clickButton(".react-flow__controls-button", "Tidy layout"));
  await sleep(900);
}

async function shoot(cdp, name) {
  await sleep(400);
  const { data } = await cdp.send("Page.captureScreenshot", { format: "png" });
  const file = path.join(OUT, `${name}.png`);
  await writeFile(file, Buffer.from(data, "base64"));
  console.log("wrote", path.relative(ROOT, file));
}

async function previewNode(cdp, specId) {
  await cdp.eval(clickNode(specId));
  await cdp.waitFor(`document.querySelector('.panel-head .title')?.textContent === ${JSON.stringify(specId)}`, `${specId} panel`);
  await cdp.eval(clickButton(".panel-head button", "Test"));
  await cdp.waitFor("document.querySelector('.drawer-body table.grid tbody tr')", `${specId} preview`, 120000);
}

async function main() {
  await mkdir(OUT, { recursive: true });
  console.log("creating the demo pipeline and running it once…");
  const pipelineId = await createDemo();
  const { cdp, close } = await launchChrome();
  try {
    for (const theme of ["light", "dark"]) {
      await open(cdp, pipelineId, theme);
      await previewNode(cdp, "orders_clean");
      await tidy(cdp);
      await shoot(cdp, theme === "light" ? "editor" : "editor-dark");
    }

    await open(cdp, pipelineId, "light");
    await previewNode(cdp, "orders_clean_dq");
    await cdp.waitFor("[...document.querySelectorAll('.drawer-body th')].some((th) => th.textContent === 'Failed checks')", "failing rows");
    await tidy(cdp);
    await shoot(cdp, "quality-test");

    await open(cdp, pipelineId, "light");
    await cdp.eval(clickButton(".tabs button", "Runs"));
    await cdp.waitFor("document.querySelector('.drawer-body pre.log')?.textContent.includes('succeeded')", "the run log");
    await cdp.waitFor("document.querySelectorAll('.drawer-body table.grid').length >= 2", "run quality results");
    await tidy(cdp);
    await shoot(cdp, "runs");
  } finally {
    await close();
    await api("DELETE", `/pipelines/${pipelineId}`);
  }
}

main().catch((error) => {
  console.error(error);
  process.exit(1);
});
