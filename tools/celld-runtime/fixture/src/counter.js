export class Counter {
  constructor(state) { this.state = state; }
  async fetch(req) {
    let n = (await this.state.storage.get('n')) || 0;
    if (req.method === 'POST') {
      n += 1;
      await this.state.storage.put('n', n);
    }
    return new Response(String(n));
  }
}
