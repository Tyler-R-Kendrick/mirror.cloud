// Adapter-owned utility Worker: read/write the KV binding over HTTP.
// Loaded from disk by helper.mjs — never from the control channel.
export default {
  async fetch(request, env) {
    const url = new URL(request.url);
    const key = url.searchParams.get("key") || url.pathname.replace(/^\//, "");
    if (!key) {
      return new Response("missing key", { status: 400 });
    }
    if (request.method === "PUT" || request.method === "POST") {
      const body = await request.arrayBuffer();
      await env.KV.put(key, body);
      return new Response("ok", { status: 200 });
    }
    if (request.method === "GET") {
      const value = await env.KV.get(key, "arrayBuffer");
      if (value == null) {
        return new Response(null, { status: 404 });
      }
      return new Response(value, {
        status: 200,
        headers: { "content-type": "application/octet-stream" },
      });
    }
    if (request.method === "DELETE") {
      await env.KV.delete(key);
      return new Response(null, { status: 204 });
    }
    return new Response("method not allowed", { status: 405 });
  },
};
