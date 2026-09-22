export { Counter } from './counter.js';
export default {
  async fetch(req, env) {
    const u = new URL(req.url);
    if (u.pathname === '/kv' && req.method === 'PUT') {
      await env.DATA.put(u.searchParams.get('k'), await req.text());
      return new Response('ok');
    }
    if (u.pathname === '/kv') {
      const v = await env.DATA.get(u.searchParams.get('k'));
      return new Response(v === null ? 'miss' : v);
    }
    if (u.pathname === '/d1') {
      await env.DB.exec('CREATE TABLE IF NOT EXISTS t (id INTEGER PRIMARY KEY, v TEXT)');
      await env.DB.prepare('INSERT INTO t (v) VALUES (?)').bind('hello').run();
      const row = await env.DB.prepare('SELECT v FROM t LIMIT 1').first();
      return new Response(row ? row.v : 'empty');
    }
    if (u.pathname === '/r2' && req.method === 'PUT') {
      await env.BUCKET.put(u.searchParams.get('k'), await req.arrayBuffer());
      return new Response('ok');
    }
    if (u.pathname === '/r2') {
      const obj = await env.BUCKET.get(u.searchParams.get('k'));
      if (!obj) return new Response('miss', { status: 404 });
      return new Response(await obj.arrayBuffer());
    }
    if (u.pathname === '/do') {
      const id = env.COUNTER.idFromName('n');
      const stub = env.COUNTER.get(id);
      return stub.fetch(req);
    }
    if (u.pathname === '/queue' && req.method === 'POST') {
      await env.Q.send({ body: await req.text() });
      return new Response('queued');
    }
    return new Response('mirror-celld-fixture');
  },
};
