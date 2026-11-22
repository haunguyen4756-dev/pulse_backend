// Metric Pulse collector — runs 24/7 on Render. Polls Metric's public API, logs history to Firebase Firestore.
// Env: FIREBASE_SERVICE_ACCOUNT (service-account JSON) · INTERVAL (poll secs, default 30) · RETAIN_DAYS (default 90)
//      SELF_PING=0 to disable the keep-awake self request · METRIC_API_BASE (testing only)
import http from 'node:http';
import admin from 'firebase-admin';

const API = process.env.METRIC_API_BASE || 'https://api.metric.xyz/public/v1';
const PORT = +process.env.PORT || 10000;
const EVERY = Math.max(5, +process.env.INTERVAL || 30) * 1000;
const BUCKET = 600; // history granularity (s) — must match the frontend
const S = ['healthy', 'feed_down', 'pool_refused', 'pool_paused', 'unknown'];
const T = ['green', 'yellow', 'red', 'unclassified', 'black'];
const MAXU = 2n ** 128n - 1n;
const sleep = ms => new Promise(r => setTimeout(r, ms));

// ── Metric API ──
async function call(path, opt = {}) {
  let err;
  for (let i = 0; i < 3; i++) {
    try {
      const r = await fetch(API + path, { ...opt, ...(opt.body && { headers: { 'content-type': 'application/json' } }) });
      if (r.ok) return await r.json();
      err = new Error(`${path} -> ${r.status}`);
      if (r.status < 500 && r.status !== 429) break;
    } catch (e) { err = e; }
    await sleep(1000 * (i + 1));
  }
  throw err;
}
const spreadBps = q => { try { const b = BigInt(q.bidAdj), a = BigInt(q.askAdj); return b <= 0n || a >= MAXU || a < b ? null : Number(((a - b) * 2000000n) / (a + b)) / 100 } catch { return null } };

let chains;
async function getChains() { // /chains shape isn't fully documented: accept array, {chains|data:[]}, or {name:id}
  const raw = await call('/chains'), arr = Array.isArray(raw) ? raw : raw.chains || raw.data;
  const l = arr ? arr.map(c => ({ id: c.chainId ?? c.chain_id ?? c.id, name: c.name ?? c.chain ?? String(c.chainId ?? c.id) })) : Object.entries(raw).map(([name, id]) => ({ id: +id, name }));
  return l.filter(c => Number.isInteger(c.id));
}

// snapshot.p[addr] = [status, tier, spreadBps, tvl, vol24h, aprBps7d, pauseLevel, px0, px1]
async function collect() {
  chains ??= await getChains();
  const p = {}, meta = {}, c = [0, 0, 0, 0, 0], k = [0, 0, 0, 0, 0];
  let tvl = 0, vol = 0, ok = 0;
  await Promise.all(chains.map(async ch => {
    try {
      const pools = []; for (let off = 0; off != null; ) { const r = await call(`/evm/${ch.id}/metadata?count=500&offset=${off}&include24h=true`); pools.push(...r.data); off = r.nextOffset }
      const live = pools.filter(x => x.priceProviderStatus === 'healthy' && !x.pauseLevel).map(x => x.poolAddress), sp = {};
      for (let i = 0; i < live.length; i += 100) {
        try { const r = await call(`/evm/${ch.id}/bid_ask/batch`, { method: 'POST', body: JSON.stringify({ pools: live.slice(i, i + 100), depth: false }) }); for (const q of r.results) if (q.quote) sp[q.poolAddress.toLowerCase()] = spreadBps(q.quote) } catch (e) { console.warn('bid_ask', e.message) }
      }
      for (const x of pools) {
        const a = x.poolAddress.toLowerCase(), si = S.includes(x.priceProviderStatus) ? S.indexOf(x.priceProviderStatus) : 4, ti = Math.max(0, T.indexOf(x.classificationSummary?.tierQualification));
        const tv = Math.round(x.tvlFiat ?? 0), v = Math.round(x.analytics24h?.volumeFiat ?? x.volumeByWindow?.['24h'] ?? 0);
        meta[a] = { pair: x.pair, chain: ch.name };
        p[a] = [si, ti, sp[a] ?? null, tv, v, x.aprBps7d ?? null, x.pauseLevel ?? 0, x.token0FiatPrice ?? null, x.token1FiatPrice ?? null];
        c[si]++; k[ti]++; tvl += tv; vol += v;
      }
      ok++;
    } catch (e) { console.warn('chain', ch.name, e.message) }
  }));
  if (!ok || !Object.keys(p).length) throw new Error('no pool data returned');
  return { snap: { ts: Math.floor(Date.now() / 1000), n: Object.keys(p).length, c, k, tvl, vol, p }, meta };
}

// ── Firestore ──
if (!process.env.FIREBASE_SERVICE_ACCOUNT) { console.error('FIREBASE_SERVICE_ACCOUNT is not set'); process.exit(1) }
admin.initializeApp({ credential: admin.credential.cert(JSON.parse(process.env.FIREBASE_SERVICE_ACCOUNT)) });
const db = admin.firestore(), stRef = db.doc('meta/state');
let state;

async function tick() {
  const { snap, meta } = await collect(), { p } = snap, b = Math.floor(snap.ts / BUCKET) * BUCKET;
  state ??= (await stRef.get()).data() ?? { p: {}, h: 0 };

  // status / tier / pause changes since the previous poll (deterministic ids => idempotent)
  const events = [], known = Object.keys(state.p).length;
  const add = (a, kind, from, to) => events.push([`${a}_${kind}_${from}_${to}_${b}`, { ts: snap.ts, a, pair: meta[a].pair, chain: meta[a].chain, kind, from, to }]);
  if (known) for (const a in p) {
    const o = state.p[a], [s, t] = p[a], z = p[a][6] ?? 0;
    if (!o) { add(a, 'new', 'none', S[s]); continue }
    if (o[0] !== s) add(a, 'status', S[o[0]], S[s]);
    if (o[1] !== t) add(a, 'tier', T[o[1]], T[t]);
    if (o[2] !== z) add(a, 'pause', String(o[2]), String(z));
  }

  const writeHist = (state.h || 0) < b, ops = [];
  if (writeHist) ops.push([db.doc(`snapshots/${b}`), { ...snap, ts: b }]);
  events.forEach(([id, e]) => ops.push([db.doc(`events/${id}`), e]));
  const next = { ts: snap.ts, h: writeHist ? b : state.h || 0, p: Object.fromEntries(Object.entries(p).map(([a, v]) => [a, [v[0], v[1], v[6] ?? 0]])) };
  if (writeHist || events.length || !known) ops.push([stRef, next]);   // state doc only when something changed
  for (let i = 0; i < ops.length; i += 400) { const batch = db.batch(); ops.slice(i, i + 400).forEach(([r, d]) => batch.set(r, d)); await batch.commit() }
  state = { ...next, p: next.p };

  if (writeHist) { // retention
    const old = await db.collection('snapshots').where('ts', '<', b - 86400 * (+process.env.RETAIN_DAYS || 90)).limit(300).get();
    if (!old.empty) { const d = db.batch(); old.docs.forEach(x => d.delete(x.ref)); await d.commit() }
  }
  return { pools: snap.n, events: events.length, wroteSnapshot: writeHist, bucket: b };
}

// ── loop + health server ──
const stats = { startedAt: new Date().toISOString(), runs: 0, errors: 0, lastOkAt: null, lastError: null, lastResult: null };
let backoff = 0;
async function loop() {
  try {
    stats.lastResult = await tick(); stats.runs++; stats.lastOkAt = new Date().toISOString(); stats.lastError = null; backoff = 0;
    if (stats.lastResult.wroteSnapshot || stats.lastResult.events) console.log(new Date().toISOString(), JSON.stringify(stats.lastResult));
  } catch (e) {
    stats.errors++; stats.lastError = e.message; console.error(new Date().toISOString(), e.message);
    if (/429/.test(e.message)) backoff = Math.min(300e3, (backoff || EVERY) * 2);
  }
  setTimeout(loop, Math.max(EVERY, backoff));
}

const server = http.createServer((req, res) => {
  if (req.url === '/health') { res.writeHead(200, { 'content-type': 'text/plain' }); return res.end('ok') }
  res.writeHead(200, { 'content-type': 'application/json' });
  res.end(JSON.stringify({ service: 'metric-pulse-collector', ...stats }, null, 2));
}).listen(PORT, () => console.log(`collector up on :${PORT}, polling every ${EVERY / 1000}s`));

// Render free web services sleep after 15 min without inbound traffic: hit our own public URL periodically.
const self = process.env.RENDER_EXTERNAL_URL;
if (self && process.env.SELF_PING !== '0') setInterval(() => fetch(self + '/health').catch(() => {}), 10 * 60e3);

for (const sig of ['SIGTERM', 'SIGINT']) process.on(sig, () => server.close(() => process.exit(0)));
loop();
