// End-to-end smoke test: server + mock agent + a viewer client.
// Run: node test.js
const { spawn } = require('child_process');
const WebSocket = require('ws');
const assert = require('assert');

const PORT = 8799;
const BASE = `http://localhost:${PORT}`;
const sleep = ms => new Promise(r => setTimeout(r, ms));

(async () => {
  const srv = spawn('node', ['server.js'], { env: { ...process.env, PORT, ROOM_GRACE_MS: '1500' }, stdio: 'inherit' });
  await sleep(600);
  let failed = false;
  try {
    // 1. create room + viewer page served
    const created = await (await fetch(`${BASE}/api/rooms`, { method: 'POST' })).json();
    assert(created.room && created.token, 'room created');
    assert(/^[BCDFGHJKMNPQRSTVWXZ2-9]{3}-[BCDFGHJKMNPQRSTVWXZ2-9]{3}$/.test(created.code), 'code is 3-3 vowel-free: ' + created.code);
    const page = await fetch(`${BASE}${created.viewerPath}`);
    assert.strictEqual(page.status, 200);
    assert((await page.text()).includes('Live Match'), 'viewer html served');

    // 2. bad token rejected on ingest
    await new Promise(resolve => {
      const bad = new WebSocket(`ws://localhost:${PORT}/ingest?room=${created.room}&token=nope`);
      bad.on('unexpected-response', (_, res) => { assert.strictEqual(res.statusCode, 401); resolve(); });
      bad.on('open', () => assert.fail('bad token should not connect'));
    });

    // 3. viewer on a fresh mock-agent room
    const agent = spawn('node', ['mock-agent.js', '--fast', '--server', BASE], { stdio: ['ignore', 'pipe', 'inherit'] });
    let link = '', code = '';
    await new Promise(r => agent.stdout.on('data', d => {
      const m = d.toString().match(/Code: ([A-Z0-9]{3}-[A-Z0-9]{3}).*\/s\/([a-f0-9]{10})/);
      if (m && !link) { code = m[1]; link = m[2]; r(); }
    }));

    // code lookups: exact, lowercase without hyphen, redirect page, wrong code
    const hit = await (await fetch(`${BASE}/api/codes/${code}`)).json();
    assert.strictEqual(hit.room, link, 'code resolves to the room');
    const loose = await (await fetch(`${BASE}/api/codes/${code.toLowerCase().replace('-', ' ')}`)).json();
    assert.strictEqual(loose.room, link, 'lookup ignores case and separators');
    const redirect = await fetch(`${BASE}/c/${code}`, { redirect: 'manual' });
    assert.strictEqual(redirect.status, 302); assert.strictEqual(redirect.headers.get('location'), `/s/${link}`);
    assert.strictEqual((await fetch(`${BASE}/api/codes/ZZZ-ZZZ`)).status, 404, 'unknown code 404');
    assert.strictEqual((await fetch(`${BASE}/c/nope`)).status, 404, 'malformed code 404');

    const msgs = [];
    const viewer = new WebSocket(`ws://localhost:${PORT}/watch?room=${link}`);
    viewer.on('message', m => msgs.push(JSON.parse(m)));
    const viewerClosed = new Promise(r => viewer.on('close', code => r(code)));
    await new Promise(r => agent.on('exit', r));
    await sleep(200);

    const events = msgs.filter(m => m.type === 'event').map(m => m.event);
    const types = new Set(events.map(e => e.type));
    for (const t of ['match_start', 'round_update', 'player_update', 'match_end']) assert(types.has(t), `saw ${t}`);
    assert(msgs.some(m => m.type === 'snapshot'), 'got snapshot on connect');

    // 4. enemy data never reaches viewers
    const players = events.filter(e => e.type === 'player_update').flatMap(e => e.data.players);
    assert(players.length > 0, 'got player data');
    assert(players.every(p => p.ally === true), 'only ally players forwarded');
    assert(!JSON.stringify(msgs).includes('Reyna'), 'enemy agent/lock never forwarded');
    assert(!JSON.stringify(msgs).match(/"id":"p[1-5]"/), 'raw ids are pseudonymised');

    // 5. late joiner gets final snapshot
    const late = await new Promise(resolve => {
      const w = new WebSocket(`ws://localhost:${PORT}/watch?room=${link}`);
      w.on('message', m => { const j = JSON.parse(m); if (j.type === 'snapshot') { w.close(); resolve(j); } });
    });
    assert.strictEqual(late.snapshot.phase, 'offline', 'agent gone -> offline');
    assert.strictEqual(late.snapshot.map, 'Ascent');
    assert(late.log.length > 0, 'late joiner gets event log');

    // stream over: agent gone past the grace period -> code dies, viewers told
    await sleep(2300);
    assert.strictEqual((await fetch(`${BASE}/api/codes/${code}`)).status, 404, 'code dies after stream ends');
    assert.strictEqual(await viewerClosed, 4001, 'viewers closed with "stream ended"');
    assert.strictEqual((await fetch(`${BASE}${created.viewerPath}`)).status, 200, 'viewer page itself still served');

    // lookup rate limit (per IP)
    let limited = false;
    for (let i = 0; i < 40 && !limited; i++) limited = (await fetch(`${BASE}/api/codes/ZZZ-ZZZ`)).status === 429;
    assert(limited, 'code lookups are rate limited');

    console.log(`PASS: ${events.length} events, ${players.length} player entries, late-join snapshot ok`);
  } catch (e) {
    failed = true; console.error('FAIL:', e.message);
  } finally {
    srv.kill();
    process.exit(failed ? 1 : 0);
  }
})();
