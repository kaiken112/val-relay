// Mock agent: plays a scripted fake match into the relay so the viewer can be
// tested without Valorant running. Mimics what the Electron agent will send.
//
// Usage:
//   node mock-agent.js                       # real-time-ish pacing
//   node mock-agent.js --fast                # compressed timings (for tests)
//   node mock-agent.js --server http://host:8787 --rounds 6

const WebSocket = require('ws');

const args = process.argv.slice(2);
const opt = (name, def) => {
  const i = args.indexOf(name);
  return i === -1 ? def : (args[i + 1] && !args[i + 1].startsWith('--') ? args[i + 1] : true);
};
const SERVER = opt('--server', 'http://localhost:8787');
const FAST = !!opt('--fast', false);
const ROUNDS = parseInt(opt('--rounds', FAST ? 4 : 8), 10);
const wait = ms => new Promise(r => setTimeout(r, FAST ? Math.max(5, ms / 20) : ms));

const TEAM = [
  { id: 'p1', name: 'Alpha', agent: 'Neon' },
  { id: 'p2', name: 'Bravo', agent: 'Omen' },
  { id: 'p3', name: 'Charlie', agent: 'Sova' },
  { id: 'p4', name: 'Delta', agent: 'Killjoy' },
  { id: 'p5', name: 'Echo', agent: 'Jett' },
];

async function main() {
  const res = await fetch(`${SERVER}/api/rooms`, { method: 'POST' });
  if (!res.ok) throw new Error(`create room failed: ${res.status}`);
  const { room, token, viewerPath, code } = await res.json();
  console.log(`Code: ${code}   Viewer link: ${SERVER}${viewerPath}`);

  const wsUrl = SERVER.replace(/^http/, 'ws') + `/ingest?room=${room}&token=${token}`;
  const ws = new WebSocket(wsUrl);
  await new Promise((ok, bad) => { ws.once('open', ok); ws.once('error', bad); });
  ws.on('message', m => { const j = JSON.parse(m); if (j.type === 'error') console.warn('relay error:', j.error); });

  const emit = (type, data) => ws.send(JSON.stringify({ v: 1, type, ts: Date.now(), data }));

  const hb = setInterval(() => emit('heartbeat', {}), 10_000);

  emit('phase_change', { phase: 'pregame' });
  await wait(1000);
  for (const p of TEAM) {
    emit('agent_lock', { id: p.id, name: p.name, agent: p.agent, ally: true });
    await wait(400);
  }
  // an enemy lock, which the relay must drop
  emit('agent_lock', { id: 'e1', name: 'Enemy', agent: 'Reyna', ally: false });

  emit('match_start', { map: 'Ascent', mode: 'competitive' });
  let ally = 0, enemy = 0;

  const state = TEAM.map(p => ({
    ...p, hp: 100, credits: 800, alive: true,
    x: 0.15 + Math.random() * 0.1, y: 0.8 + Math.random() * 0.1,
  }));

  for (let r = 1; r <= ROUNDS; r++) {
    const side = r <= 12 ? 'attack' : 'defense';
    emit('round_update', { round: r, side, score: { ally, enemy }, phase: 'buy' });
    state.forEach(p => { p.hp = 100; p.alive = true; p.credits = 800 + Math.floor(Math.random() * 8200); });
    await wait(2000);
    emit('round_update', { round: r, side, score: { ally, enemy }, phase: 'live' });

    for (let tick = 0; tick < 12; tick++) {
      for (const p of state) {
        if (!p.alive) continue;
        p.x = Math.min(0.95, Math.max(0.05, p.x + (Math.random() - 0.4) * 0.06));
        p.y = Math.min(0.95, Math.max(0.05, p.y - Math.random() * 0.05));
        if (Math.random() < 0.04) { p.hp = Math.max(0, p.hp - 40); if (p.hp === 0) p.alive = false; }
      }
      emit('player_update', {
        players: [
          ...state.map(p => ({ id: p.id, name: p.name, agent: p.agent, hp: p.hp, credits: p.credits, alive: p.alive, x: p.x, y: p.y, ally: true })),
          // an enemy entry, which the relay must also drop
          { id: 'e1', name: 'Enemy', agent: 'Reyna', hp: 100, x: 0.5, y: 0.5, ally: false },
        ],
      });
      await wait(500);
    }

    const won = Math.random() < 0.55;
    won ? ally++ : enemy++;
    emit('round_update', { round: r, side, score: { ally, enemy }, phase: 'ended', winner: won ? 'ally' : 'enemy' });
    await wait(1500);
  }

  emit('match_end', { score: { ally, enemy }, won: ally > enemy, decided: true });
  emit('phase_change', { phase: 'ended' });
  await wait(500);
  clearInterval(hb);
  ws.close();
  console.log('Mock match finished.');
}

main().catch(e => { console.error(e); process.exit(1); });
