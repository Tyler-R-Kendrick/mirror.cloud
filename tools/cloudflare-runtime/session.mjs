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
//                            worker.dispatch / hang
//                            d1.query / d1.exec / r2.put / r2.get / queue.send
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
import net from 'node:net';
import dns from 'node:dns';
import { readFileSync } from 'node:fs';
import miniflarePkg from 'miniflare';
const { Miniflare } = miniflarePkg;


function installOfflineTripwire() {
  const allow = new Set(['127.0.0.1', '::1', 'localhost']);
  const blockHost = (host) => {
    if (!host || allow.has(host)) return;
    throw Object.assign(new Error(`offline tripwire: blocked connect to ${host}`), { code: 'OFFLINE_TRIPWIRE' });
  };
  const origConnect = net.connect;
  net.connect = function (...args) {
    let host;
    if (typeof args[0] === 'object' && args[0]) host = args[0].host || args[0].hostname;
    else if (typeof args[1] === 'string') host = args[1];
    blockHost(host);
    return origConnect.apply(this, args);
  };
  const origLookup = dns.lookup;
  dns.lookup = function (hostname, ...rest) {
    if (hostname && !allow.has(hostname)) {
      const cb = rest[rest.length - 1];
      if (typeof cb === 'function') {
        process.nextTick(() => cb(Object.assign(new Error(`offline tripwire: blocked dns ${hostname}`), { code: 'OFFLINE_TRIPWIRE' })));
        return;
      }
    }
    return origLookup.call(this, hostname, ...rest);
  };
}

installOfflineTripwire();

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
let d1Names = new Set();
let r2Names = new Set();
let queueNames = new Set();
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
  for (const key of ['d1Databases', 'r2Buckets', 'queueProducers']) {
    if (body[key] !== undefined) {
      if (!Array.isArray(body[key])) return key + ' must be an array of strings';
      for (const n of body[key]) {
        if (typeof n !== 'string' || n.length === 0) return key + ' entries must be non-empty strings';
      }
    }
  }
  if (body.workflows !== undefined && !isPlainObject(body.workflows)) {
    return 'workflows must be an object';
  }
  return null;
}

async function doApply(body) {
  const invalid = validateApply(body);
  if (invalid) throw callError(400, 'invalid_graph', invalid);
  const kv = Array.isArray(body.kvNamespaces) ? body.kvNamespaces : [];
  const d1 = Array.isArray(body.d1Databases) ? body.d1Databases : [];
  const r2 = Array.isArray(body.r2Buckets) ? body.r2Buckets : [];
  const queues = Array.isArray(body.queueProducers) ? body.queueProducers : [];
  const workflows = isPlainObject(body.workflows) ? body.workflows : {};

  // Miniflare 5: each worker is `{config:{name,compatibilityDate,manifest,env}}`.
  // Legacy flat `script`/`modules`/`kvNamespaces` keys are rejected.
  const workers = body.workers.map((w) => {
    const env = { ...(w.bindings || {}) };
    const kvBind = Array.isArray(w.kvNamespaces) ? w.kvNamespaces : kv;
    for (const name of kvBind) env[name] = { type: 'kv', id: name };
    const d1Bind = Array.isArray(w.d1Databases) ? w.d1Databases : d1;
    for (const name of d1Bind) env[name] = { type: 'd1', id: name };
    const r2Bind = Array.isArray(w.r2Buckets) ? w.r2Buckets : r2;
    for (const name of r2Bind) env[name] = { type: 'r2', name };
    const qBind = Array.isArray(w.queueProducers) ? w.queueProducers : queues;
    for (const name of qBind) env[name] = { type: 'queue', name };
    for (const [bind, spec] of Object.entries(workflows)) {
      if (!isPlainObject(spec)) continue;
      env[bind] = {
        type: 'workflow',
        name: spec.name || bind,
        worker: w.name,
        exportName: spec.className || spec.exportName || bind,
      };
    }
    if (isPlainObject(body.durableObjects)) {
      for (const [bind, className] of Object.entries(body.durableObjects)) {
        const cn = typeof className === 'string' ? className : className?.className;
        if (!cn) continue;
        env[bind] = { type: 'durable-object', className: cn, scriptName: w.name };
      }
    }
    return {
      config: {
        name: w.name,
        compatibilityDate: w.compatibilityDate,
        manifest: {
          mainModule: 'index.js',
          modules: {
            'index.js': { type: 'esm', contents: w.script },
          },
        },
        env,
      },
    };
  });

  const opts = { workers, cf: false };
  if (mf) {
    await mf.setOptions(opts);
  } else {
    const first = new Miniflare(opts);
    await first.ready;
    mf = first;
  }
  kvNames = new Set([...kv, ...body.workers.flatMap((w) => Array.isArray(w.kvNamespaces) ? w.kvNamespaces : [])]);
  d1Names = new Set([...d1, ...body.workers.flatMap((w) => Array.isArray(w.d1Databases) ? w.d1Databases : [])]);
  r2Names = new Set([...r2, ...body.workers.flatMap((w) => Array.isArray(w.r2Buckets) ? w.r2Buckets : [])]);
  queueNames = new Set([...queues, ...body.workers.flatMap((w) => Array.isArray(w.queueProducers) ? w.queueProducers : [])]);
  generation += 1;
  ready = true;
  quiesced = false;
  return { generation };
}

function needMF() {
  if (!mf) throw callError(409, 'not_applied', 'no binding graph applied yet');
  if (quiesced) throw callError(503, 'quiesced', 'session is quiesced');
}


async function doSnapshot() {
  needMF();
  const kv = {};
  for (const name of kvNames) {
    const ns = await mf.getKVNamespace(name);
    const page = await ns.list({ limit: 1000 });
    kv[name] = {};
    for (const k of page.keys || []) {
      const raw = await ns.get(k.name, { type: 'arrayBuffer' });
      if (raw == null) continue;
      kv[name][k.name] = Buffer.from(raw).toString('base64');
    }
  }
  return { kv, generation };
}

async function doRestore(body) {
  needMF();
  if (!isPlainObject(body) || !isPlainObject(body.kv)) {
    throw callError(400, 'invalid_call', 'restore needs {kv: {ns: {key: b64}}}');
  }
  for (const [name, entries] of Object.entries(body.kv)) {
    requireNamespace(name);
    const ns = await mf.getKVNamespace(name);
    for (const [key, value] of Object.entries(entries)) {
      await ns.put(key, Uint8Array.from(Buffer.from(String(value), 'base64')));
    }
  }
  return { restored: true, generation };
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
      if (body.expiration !== undefined && body.expiration > 0) opts.expiration = body.expiration;
      else if (body.expirationTtl !== undefined && body.expirationTtl > 0) opts.expirationTtl = body.expirationTtl;
      if (body.metadata !== undefined) opts.metadata = body.metadata;
      await ns.put(body.key, Uint8Array.from(Buffer.from(body.value, 'base64')), opts);
      return { stored: true };
    }
    case 'kv.delete': {
      if (typeof body.namespace !== 'string' || typeof body.key !== 'string') {
        throw callError(400, 'invalid_call', 'kv.delete needs {namespace, key}');
      }
      requireNamespace(body.namespace);
      const ns = await mf.getKVNamespace(body.namespace);
      const prior = await ns.get(body.key, { type: 'arrayBuffer' });
      if (prior === null) return { deleted: false, found: false };
      await ns.delete(body.key);
      return { deleted: true, found: true };
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
    case 'snapshot': {
      return await doSnapshot();
    }
    case 'restore': {
      return await doRestore(body);
    }
    case 'hang': {
      // Deliberately block so the host can SIGKILL mid-invocation (CF-CRASH).
      const ms = typeof body.ms === 'number' && body.ms > 0 ? body.ms : 60000;
      await new Promise((r) => setTimeout(r, ms));
      return { hung: true, ms };
    }
    case 'd1.query': {
      if (typeof body.database !== 'string' || typeof body.sql !== 'string') {
        throw callError(400, 'invalid_call', 'd1.query needs {database, sql}');
      }
      if (!d1Names.has(body.database)) {
        throw callError(404, 'namespace_absent', `d1 database ${JSON.stringify(body.database)} not registered`);
      }
      const db = await mf.getD1Database(body.database);
      const binds = Array.isArray(body.binds) ? body.binds : [];
      const stmt = db.prepare(body.sql);
      const result = binds.length ? await stmt.bind(...binds).all() : await stmt.all();
      return { results: result.results || [], success: result.success !== false, meta: result.meta || {} };
    }
    case 'd1.exec': {
      if (typeof body.database !== 'string' || typeof body.sql !== 'string') {
        throw callError(400, 'invalid_call', 'd1.exec needs {database, sql}');
      }
      if (!d1Names.has(body.database)) {
        throw callError(404, 'namespace_absent', `d1 database ${JSON.stringify(body.database)} not registered`);
      }
      const db = await mf.getD1Database(body.database);
      await db.exec(body.sql);
      return { executed: true };
    }
    case 'r2.put': {
      if (typeof body.bucket !== 'string' || typeof body.key !== 'string' || typeof body.value !== 'string') {
        throw callError(400, 'invalid_call', 'r2.put needs {bucket, key, value(base64)}');
      }
      if (!r2Names.has(body.bucket)) {
        throw callError(404, 'namespace_absent', `r2 bucket ${JSON.stringify(body.bucket)} not registered`);
      }
      const bucket = await mf.getR2Bucket(body.bucket);
      await bucket.put(body.key, Uint8Array.from(Buffer.from(body.value, 'base64')));
      return { stored: true };
    }
    case 'r2.get': {
      if (typeof body.bucket !== 'string' || typeof body.key !== 'string') {
        throw callError(400, 'invalid_call', 'r2.get needs {bucket, key}');
      }
      if (!r2Names.has(body.bucket)) {
        throw callError(404, 'namespace_absent', `r2 bucket ${JSON.stringify(body.bucket)} not registered`);
      }
      const bucket = await mf.getR2Bucket(body.bucket);
      const obj = await bucket.get(body.key);
      if (!obj) return { found: false };
      const buf = Buffer.from(await obj.arrayBuffer());
      return { found: true, value: buf.toString('base64') };
    }
    case 'r2.delete': {
      if (typeof body.bucket !== 'string' || typeof body.key !== 'string') {
        throw callError(400, 'invalid_call', 'r2.delete needs {bucket, key}');
      }
      if (!r2Names.has(body.bucket)) {
        throw callError(404, 'namespace_absent', `r2 bucket ${JSON.stringify(body.bucket)} not registered`);
      }
      const bucket = await mf.getR2Bucket(body.bucket);
      await bucket.delete(body.key);
      return { deleted: true };
    }
    case 'r2.list': {
      if (typeof body.bucket !== 'string') {
        throw callError(400, 'invalid_call', 'r2.list needs {bucket}');
      }
      if (!r2Names.has(body.bucket)) {
        throw callError(404, 'namespace_absent', `r2 bucket ${JSON.stringify(body.bucket)} not registered`);
      }
      const bucket = await mf.getR2Bucket(body.bucket);
      const listed = await bucket.list({ prefix: body.prefix || undefined, limit: body.limit || undefined });
      return {
        objects: (listed.objects || []).map((o) => ({ key: o.key, size: o.size, etag: o.etag })),
        truncated: !!listed.truncated,
      };
    }
    case 'queue.send': {
      if (typeof body.queue !== 'string') {
        throw callError(400, 'invalid_call', 'queue.send needs {queue, body}');
      }
      if (!queueNames.has(body.queue)) {
        throw callError(404, 'namespace_absent', `queue ${JSON.stringify(body.queue)} not registered`);
      }
      const q = await mf.getQueueProducer(body.queue);
      const payload = typeof body.body === 'string' ? body.body : JSON.stringify(body.body ?? null);
      await q.send(payload);
      return { sent: true };
    }
    case 'offline.probe': {
      // Deliberately attempt an outbound TCP connect; tripwire must refuse.
      try {
        await new Promise((resolve, reject) => {
          const s = net.connect({ host: '1.1.1.1', port: 443 }, () => {
            s.destroy();
            reject(new Error('outbound connect succeeded'));
          });
          s.on('error', reject);
        });
        throw callError(500, 'offline_breach', 'outbound connect was allowed');
      } catch (e) {
        if (e && (e.code === 'OFFLINE_TRIPWIRE' || String(e.message || e).includes('offline tripwire'))) {
          return { blocked: true, detail: String(e.message || e) };
        }
        throw e;
      }
    }
    default:
      throw callError(400, 'unsupported',
        `unknown action ${JSON.stringify(body.action)} (closed set: kv.get, kv.put, kv.delete, kv.list, worker.dispatch, snapshot, restore, offline.probe, hang, d1.*, r2.*, queue.send)`);
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
    capabilities: ['kv.get', 'kv.put', 'kv.delete', 'kv.list', 'worker.dispatch', 'snapshot', 'restore', 'offline.probe', 'hang', 'd1.query', 'd1.exec', 'r2.put', 'r2.get', 'r2.delete', 'r2.list', 'queue.send'],
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
