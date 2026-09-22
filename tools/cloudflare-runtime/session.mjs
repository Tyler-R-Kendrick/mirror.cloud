// session.mjs — mirror.cloud's optional Miniflare execution session.
//
// Spawned only when the miniflare execution profile is selected; the native
// mirror binary never requires Node, this package, or workerd. The Go side
// starts this process with a scrubbed environment and a per-session bearer
// token, waits on /v1/describe readiness, and drives a closed set of versioned
// actions over loopback HTTP.
//
// Control protocol v1 (JSON):
//   GET  /v1/describe   -> {protocol, backend, version, ready, capabilities}
//   POST /v1/apply      -> materialize a binding graph; answers {generation}
//   POST /v1/call       -> {action,...} from a CLOSED action set:
//                            kv.get / kv.put / kv.delete / kv.list
//                            worker.dispatch
//   POST /v1/quiesce    -> no new application work; stable-state report
//   POST /v1/close      -> dispose Miniflare, then answer
//
// There is deliberately NO arbitrary-eval, NO arbitrary module load, NO
// shell, NO filesystem path from the caller, and NO caller-chosen upstream
// destination: mirror resolves every resource reference before dispatch, and
// this process only reaches bindings `apply` registered. Authentication is a
// per-session bearer token from the environment that starts us, independent
// of any dummy public Cloudflare credentials the public API accepts.
//
// Offline posture: no telemetry, no remote bindings (apply rejects them),
// no metadata auto-fetch. Normal operation makes no outbound Internet
// request; the Go suite proves that with socket tripwires.

import http from 'node:http';
import { readFileSync } from 'node:fs';
import { Miniflare, convertV4MiniflareOptions } from 'miniflare';

const PROTOCOL = 1;
const TOKEN = process.env.MIRROR_SESSION_TOKEN;
if (!TOKEN || TOKEN.length < 32) {
  process.stderr.write('session.mjs: MIRROR_SESSION_TOKEN missing or too short\n');
  process.exit(2);
}

/** Miniflare instance once apply has run; null before first apply. */
let mf = null;
/** KV namespaces registered by the last apply: the closed set call may
 *  address. Anything else is a validation failure, not a lookup. */
let kvNames = new Set();
let generation = 0;
let ready = false;
let quiesced = false;

function fail(res, status, code, message) {
  res.writeHead(status, { 'content-type': 'application/json' });
  res.end(JSON.stringify({ error: { code, message } }));
}

function readBody(req, limit) {
  return new Promise((resolve, reject) => {
    const chunks = [];
    let size = 0;
    req.on('data', (c) => {
      size += c.length;
      if (size > limit) {
        reject(Object.assign(new Error('body too large'), { code: 'body_too_large' }));
        req.destroy();
        return;
      }
      chunks.push(c);
    });
    req.on('end', () => resolve(Buffer.concat(chunks)));
    req.on('error', reject);
  });
}

function isPlainObject(v) {
  return v !== null && typeof v === 'object' && !Array.isArray(v);
}

function callError(status, code, message) {
  const e = new Error(message);
  e.status = status;
  e.code = code;
  return e;
}

function requireNamespace(ns) {
  if (!kvNames.has(ns)) {
    throw callError(404, 'namespace_absent',
      `namespace ${JSON.stringify(ns)} is not registered in this session`);
  }
}

// validateApply is the admission gate for a binding graph. The graph arrives
// already normalized by mirror's config layer; this process re-checks only
// what ITS safety depends on: no remote services, no caller-chosen
// executables, well-formed workers. An unsupported field is a diagnostic,
// never a silent drop.
function validateApply(body) {
  if (!isPlainObject(body) || !Array.isArray(body.workers)) {
    return 'apply must be {workers:[...]}';
  }
  if (body.workers.length === 0) return 'apply needs at least one worker';
  for (const [i, w] of body.workers.entries()) {
    if (!isPlainObject(w)) return `workers[${i}] must be an object`;
    if (typeof w.name !== 'string' || w.name.length === 0) return `workers[${i}].name required`;
    if (typeof w.script !== 'string' || w.script.length === 0) return `workers[${i}].script required`;
    if (typeof w.compatibilityDate !== 'string') return `workers[${i}].compatibilityDate required`;
    if (w.bindings !== undefined && !isPlainObject(w.bindings)) return `workers[${i}].bindings must be an object`;
    if (Array.isArray(w.kvNamespaces)) {
      for (const n of w.kvNamespaces) {
        if (typeof n !== 'string' || n.length === 0) return `workers[${i}].kvNamespaces entries must be non-empty strings`;
      }
    }
    // Remote bindings would make local execution dial Cloudflare. The
    // offline contract forbids it, so a graph that asks for one is refused
    // before Miniflare ever sees it.
    if (w.remoteBindings !== undefined && w.remoteBindings !== false && w.remoteBindings !== null) {
      return `workers[${i}]: remote bindings are not allowed in this session`;
    }
  }
  if (body.kvNamespaces !== undefined) {
    if (!Array.isArray(body.kvNamespaces)) return 'kvNamespaces must be an array of strings';
    for (const n of body.kvNamespaces) {
      if (typeof n !== 'string' || n.length === 0) return 'kvNamespaces entries must be non-empty strings';
    }
  }
  if (body.durableObjects !== undefined && !isPlainObject(body.durableObjects)) {
    return 'durableObjects must be an object';
  }
  return null;
}

async function doApply(body) {
  const invalid = validateApply(body);
  if (invalid) throw callError(400, 'invalid_graph', invalid);
  const kv = Array.isArray(body.kvNamespaces) ? body.kvNamespaces : [];
  const workers = body.workers.map((w) => ({
    name: w.name,
    script: w.script,
    modules: true,
    compatibilityDate: w.compatibilityDate,
    // Local-only session: every binding is one apply registered; nothing
    // is discovered from a dev registry or inherited from the environment.
    bindings: { ...(w.bindings || {}) },
    // KV namespaces ride per-worker so a Worker's env binding and mirror's
    // kv.* actions address ONE namespace object inside Miniflare.
    kvNamespaces: Array.isArray(w.kvNamespaces) ? w.kvNamespaces : [],
  }));
  const opts = { workers, kvNamespaces: kv, cf: false };
  if (isPlainObject(body.durableObjects)) opts.durableObjects = body.durableObjects;
  // The v4->v5 converter is the documented bridge for the flat worker shape
  // (script/modules/kvNamespaces). cf:false pins local `cf` data: no
  // metadata auto-fetch, which is one of the offline posture's parts.
  const converted = convertV4MiniflareOptions(opts);
  if (mf) {
    // Re-apply on the LIVE instance: setOptions swaps the binding graph
    // while keeping the storage attached, so namespace data survives a
    // reconfiguration. Constructing a fresh Miniflare here would silently
    // start empty -- a data-loss bug wearing the costume of a reload.
    // (Verified against the installed package: put -> setOptions -> get
    // keeps the value; only the worker script changes.)
    await mf.setOptions(converted);
  } else {
    // First apply: construct, wait ready, then adopt. A failed first apply
    // leaves no session (not_applied), never a half-applied one.
    const first = new Miniflare(converted);
    await first.ready;
    mf = first;
  }
  kvNames = new Set([...kv, ...workers.flatMap((w) => w.kvNamespaces)]);
  generation += 1;
  ready = true;
  quiesced = false;
  return { generation };
}

function needMF() {
  if (!mf) throw callError(409, 'not_applied', 'no binding graph applied yet');
  if (quiesced) throw callError(503, 'quiesced', 'session is quiesced');
}

async function doCall(body) {
  if (!isPlainObject(body) || typeof body.action !== 'string') {
    throw callError(400, 'invalid_call', 'call must be {action:string, ...}');
  }
  needMF();
  switch (body.action) {
    case 'kv.get': {
      if (typeof body.namespace !== 'string' || typeof body.key !== 'string') {
        throw callError(400, 'invalid_call', 'kv.get needs {namespace, key}');
      }
      requireNamespace(body.namespace);
      const ns = await mf.getKVNamespace(body.namespace);
      // Byte-exact: the control path transports base64 so arbitrary bytes
      // (including invalid UTF-8) cohere with what a Worker stored. A
      // Worker that calls env.KV.get() with the default text type gets
      // Cloudflare's own UTF-8 decode, exactly as the real service behaves;
      // storage itself never transcodes.
      const raw = await ns.get(body.key, { type: 'arrayBuffer' });
      if (raw === null) {
        if (body.withMetadata) {
          const withMeta = await ns.getWithMetadata(body.key);
          if (withMeta.value === null || withMeta.value === undefined) return { found: false };
        }
        return { found: false };
      }
      const out = { found: true, value: Buffer.from(raw).toString('base64') };
      if (body.withMetadata) {
        const wm = await ns.getWithMetadata(body.key);
        out.metadata = wm.metadata;
      }
      return out;
    }
    case 'kv.put': {
      if (typeof body.namespace !== 'string' || typeof body.key !== 'string' || typeof body.value !== 'string') {
        throw callError(400, 'invalid_call', 'kv.put needs {namespace, key, value(base64)}');
      }
      requireNamespace(body.namespace);
      const ns = await mf.getKVNamespace(body.namespace);
      const opts = {};
      if (body.expirationTtl !== undefined) opts.expirationTtl = body.expirationTtl;
      if (body.metadata !== undefined) opts.metadata = body.metadata;
      await ns.put(body.key, Buffer.from(body.value, 'base64'), opts);
      return { stored: true };
    }
    case 'kv.delete': {
      if (typeof body.namespace !== 'string' || typeof body.key !== 'string') {
        throw callError(400, 'invalid_call', 'kv.delete needs {namespace, key}');
      }
      requireNamespace(body.namespace);
      const ns = await mf.getKVNamespace(body.namespace);
      await ns.delete(body.key);
      return { deleted: true };
    }
    case 'kv.list': {
      if (typeof body.namespace !== 'string') {
        throw callError(400, 'invalid_call', 'kv.list needs {namespace}');
      }
      requireNamespace(body.namespace);
      const ns = await mf.getKVNamespace(body.namespace);
      const opts = {};
      if (body.prefix !== undefined) opts.prefix = body.prefix;
      if (body.limit !== undefined) opts.limit = body.limit;
      if (body.cursor !== undefined) opts.cursor = body.cursor;
      const page = await ns.list(opts);
      return {
        keys: (page.keys || []).map((k) => ({
          name: k.name,
          ...(k.expiration !== undefined ? { expiration: k.expiration } : {}),
          ...(k.metadata !== undefined ? { metadata: k.metadata } : {}),
        })),
        listComplete: page.list_complete === true,
        ...(page.cursor !== undefined ? { cursor: page.cursor } : {}),
      };
    }
    case 'worker.dispatch': {
      // Bounded HTTP invocation of the application graph. The host is
      // synthetic: dispatchFetch targets the configured workers, so the
      // caller cannot name an upstream destination.
      if (typeof body.path !== 'string' || !body.path.startsWith('/')) {
        throw callError(400, 'invalid_call', 'worker.dispatch needs {path:"/..."}');
      }
      if (body.method !== undefined && typeof body.method !== 'string') {
        throw callError(400, 'invalid_call', 'worker.dispatch method must be a string');
      }
      const headers = {};
      if (isPlainObject(body.headers)) {
        for (const [k, v] of Object.entries(body.headers)) {
          if (typeof v !== 'string') {
            throw callError(400, 'invalid_call', 'worker.dispatch headers must be string-valued');
          }
          headers[k] = v;
        }
      }
      const init = { method: body.method || 'GET', headers };
      if (typeof body.body === 'string') init.body = Buffer.from(body.body, 'base64');
      const resp = await mf.dispatchFetch('http://mirror.local' + body.path, init);
      const buf = Buffer.from(await resp.arrayBuffer());
      const outHeaders = {};
      resp.headers.forEach((v, k) => { outHeaders[k] = v; });
      return { status: resp.status, headers: outHeaders, body: buf.toString('base64') };
    }
    default:
      throw callError(400, 'unknown_action',
        `unknown action ${JSON.stringify(body.action)} (closed set: kv.get, kv.put, kv.delete, kv.list, worker.dispatch)`);
  }
}

async function doQuiesce() {
  quiesced = true;
  // Quiesce is NOT close: no new application work is accepted, persisted
  // state stays, and describe keeps reporting the session identity.
  return { quiesced: true, generation, hadSession: mf !== null };
}

async function doClose() {
  ready = false;
  quiesced = true;
  if (mf) {
    await mf.dispose();
    mf = null;
  }
  return { closed: true, generation };
}

function describe() {
  let version = 'unknown';
  try {
    const pkg = JSON.parse(readFileSync(new URL('./package.json', import.meta.url), 'utf8'));
    version = pkg.dependencies?.miniflare || 'unknown';
  } catch { /* keep unknown */ }
  return {
    protocol: PROTOCOL,
    backend: 'miniflare',
    version,
    ready,
    generation,
    quiesced,
    capabilities: ['kv.get', 'kv.put', 'kv.delete', 'kv.list', 'worker.dispatch'],
  };
}

const server = http.createServer(async (req, res) => {
  // Bearer auth on every route, including describe: the control channel is
  // private regardless of what dummy credentials the public API accepts.
  const auth = req.headers['authorization'] || '';
  if (auth !== `Bearer ${TOKEN}`) {
    fail(res, 401, 'unauthorized', 'bad or missing session token');
    return;
  }
  // Loopback only: the listener binds 127.0.0.1; this is defense in depth
  // against a proxy that rewrites the bind.
  const addr = req.socket.remoteAddress;
  if (addr !== '127.0.0.1' && addr !== '::1' && addr !== '::ffff:127.0.0.1') {
    fail(res, 403, 'forbidden', 'control channel is loopback-only');
    return;
  }
  try {
    if (req.method === 'GET' && req.url === '/v1/describe') {
      res.writeHead(200, { 'content-type': 'application/json' });
      res.end(JSON.stringify(describe()));
      return;
    }
    if (req.method !== 'POST') {
      fail(res, 405, 'method_not_allowed', 'POST required');
      return;
    }
    const raw = await readBody(req, 64 * 1024 * 1024); // 64 MiB control bound
    let body;
    try {
      body = JSON.parse(raw.toString('utf8'));
    } catch {
      fail(res, 400, 'invalid_json', 'body must be JSON');
      return;
    }
    let out;
    if (req.url === '/v1/apply') out = await doApply(body);
    else if (req.url === '/v1/call') out = await doCall(body);
    else if (req.url === '/v1/quiesce') out = await doQuiesce();
    else if (req.url === '/v1/close') out = await doClose();
    else {
      fail(res, 404, 'not_found', 'unknown control route');
      return;
    }
    res.writeHead(200, { 'content-type': 'application/json' });
    res.end(JSON.stringify(out));
  } catch (e) {
    fail(res, e.status || 500, e.code || 'internal', e.message || 'unknown error');
  }
});

// Bind loopback on an ephemeral port; the chosen port is printed on stdout
// as one JSON line for the Go parent to read. Never a fixed port: parallel
// sessions cannot collide, and nothing can pre-attach to a known address.
server.listen(0, '127.0.0.1', () => {
  const { port } = server.address();
  process.stdout.write(JSON.stringify({ ready: true, port, protocol: PROTOCOL }) + '\n');
});

// Clean shutdown on the parent's signal: dispose Miniflare so workerd
// children are reaped, then exit.
for (const sig of ['SIGTERM', 'SIGINT']) {
  process.on(sig, async () => {
    ready = false;
    try { if (mf) await mf.dispose(); } catch { /* best effort */ }
    server.close();
    process.exit(0);
  });
}

