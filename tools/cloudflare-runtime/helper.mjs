/**
 * Long-lived Miniflare control helper.
 * Protocol v1: one JSON object per line on stdin → one JSON line on stdout.
 * Ops: start | kv_put | kv_get | worker_fetch | stop
 * Never evaluates JS from the control channel; Worker script is a fixed file.
 */
import { createInterface } from "node:readline";
import { readFileSync, existsSync } from "node:fs";
import { dirname, join } from "node:path";
import { fileURLToPath } from "node:url";
import { createRequire } from "node:module";

const PROTOCOL = 1;
const ROOT = dirname(fileURLToPath(import.meta.url));
const WORKER_PATH = join(ROOT, "worker.mjs");
const MINIFLARE_ENTRY = join(ROOT, "node_modules", "miniflare", "dist", "src", "index.js");

let mf = null;
let kvBinding = "KV";

function reply(id, ok, payload) {
  const msg = { v: PROTOCOL, id, ok };
  if (ok) {
    if (payload !== undefined) msg.result = payload;
  } else {
    msg.error = payload;
  }
  process.stdout.write(JSON.stringify(msg) + "\n");
}

function errBody(code, message) {
  return { code, message };
}

function requireMiniflare() {
  if (!existsSync(join(ROOT, "node_modules", "miniflare"))) {
    throw Object.assign(new Error("miniflare not installed; run npm ci in tools/cloudflare-runtime"), {
      code: "unavailable",
    });
  }
  // Prefer package export resolution; fall back to known dist path.
  const require = createRequire(import.meta.url);
  try {
    return require("miniflare");
  } catch {
    return require(MINIFLARE_ENTRY);
  }
}


function isString(v) {
  return Object.prototype.toString.call(v) === "[object String]";
}

function requireKey(args) {
  const key = args.key;
  if (!isString(key) || key === "") {
    throw Object.assign(new Error("key required"), { code: "validation" });
  }
  return key;
}

async function opStart(args) {
  if (mf) {
    await mf.dispose();
    mf = null;
  }
  kvBinding = isString(args.kv_binding) && args.kv_binding !== "" ? args.kv_binding : "KV";
  const { Miniflare } = requireMiniflare();
  // Fixed Worker on disk — not from control-channel script text.
  const script = readFileSync(WORKER_PATH, "utf8");
  mf = new Miniflare({
    modules: true,
    script,
    scriptPath: WORKER_PATH,
    kvNamespaces: [kvBinding],
    // Local-only defaults for the helper profile.
    host: "127.0.0.1",
  });
  // Touch the namespace so workerd is warm.
  await mf.getKVNamespace(kvBinding);
  return { kv_binding: kvBinding, worker: "worker.mjs" };
}

async function opKvPut(args) {
  if (!mf) throw Object.assign(new Error("not started"), { code: "unavailable" });
  const key = requireKey(args);
  const ns = await mf.getKVNamespace(kvBinding);
  const buf = Buffer.from(String(args.value_b64 ?? ""), "base64");
  // Miniflare/workerd rejects Node Buffer (assert false == true); use Uint8Array.
  await ns.put(key, new Uint8Array(buf));
  return { ok: true, bytes: buf.length };
}

async function opKvGet(args) {
  if (!mf) throw Object.assign(new Error("not started"), { code: "unavailable" });
  const key = requireKey(args);
  const ns = await mf.getKVNamespace(kvBinding);
  const value = await ns.get(key, "arrayBuffer");
  if (value == null) {
    throw Object.assign(new Error("missing"), { code: "absent" });
  }
  return { value_b64: Buffer.from(value).toString("base64"), bytes: value.byteLength };
}

async function opWorkerFetch(args) {
  if (!mf) throw Object.assign(new Error("not started"), { code: "unavailable" });
  const method = (args.method || "GET").toUpperCase();
  const key = requireKey(args);
  const url = `http://127.0.0.1/?key=${encodeURIComponent(key)}`;
  const init = { method };
  if (method === "PUT" || method === "POST") {
    init.body = Buffer.from(String(args.value_b64 ?? ""), "base64");
  }
  const res = await mf.dispatchFetch(url, init);
  const body = Buffer.from(await res.arrayBuffer());
  return {
    status: res.status,
    value_b64: body.length ? body.toString("base64") : "",
  };
}

async function opStop() {
  if (mf) {
    await mf.dispose();
    mf = null;
  }
  return { stopped: true };
}

async function handle(line) {
  let msg;
  try {
    msg = JSON.parse(line);
  } catch {
    reply(null, false, errBody("validation", "invalid json"));
    return;
  }
  const id = msg.id ?? null;
  if (msg.v !== PROTOCOL) {
    reply(id, false, errBody("validation", `unsupported protocol ${msg.v}`));
    return;
  }
  const op = msg.op;
  try {
    let result;
    switch (op) {
      case "start":
        result = await opStart(msg);
        break;
      case "kv_put":
        result = await opKvPut(msg);
        break;
      case "kv_get":
        result = await opKvGet(msg);
        break;
      case "worker_fetch":
        result = await opWorkerFetch(msg);
        break;
      case "stop":
        result = await opStop();
        reply(id, true, result);
        process.exit(0);
        return;
      default:
        reply(id, false, errBody("unsupported", `unknown op ${op}`));
        return;
    }
    reply(id, true, result);
  } catch (e) {
    reply(id, false, errBody(e.code || "error", e.message || String(e)));
  }
}

// Announce ready without creating Miniflare (Start does that).
process.stdout.write(JSON.stringify({ v: PROTOCOL, op: "ready" }) + "\n");

const rl = createInterface({ input: process.stdin, crlfDelay: Infinity });
rl.on("line", (line) => {
  if (!line.trim()) return;
  handle(line).catch((e) => {
    reply(null, false, errBody("error", e.message || String(e)));
  });
});
rl.on("close", async () => {
  try {
    await opStop();
  } finally {
    process.exit(0);
  }
});
