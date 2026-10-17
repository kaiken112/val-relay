// val-relay: relays a streamer's own match feed to a shareable viewer link.
// Agent (Electron app) --WS /ingest--> relay --WS /watch--> browser viewers.
//
// Integrity rule baked in: only the streamer's OWN team data is accepted.
// Any player entry not flagged ally:true is dropped, so enemy positions can
// never pass through this relay even if an agent bug sends them.

const http = require('http');
const fs = require('fs');
const path = require('path');
const crypto = require('crypto');
const { WebSocketServer } = require('ws');

const PORT = parseInt(process.env.PORT || '8787', 10);
const ROOM_GRACE_MS = parseInt(process.env.ROOM_GRACE_MS || String(2 * 60 * 1000), 10);   // agent gone this long = stream over
const UNUSED_TTL_MS = parseInt(process.env.UNUSED_TTL_MS || String(10 * 60 * 1000), 10);  // room created but agent never connected
const SWEEP_MS = Math.min(30_000, Math.max(100, Math.floor(ROOM_GRACE_MS / 3)));
// No vowels, no 0/1/O/I/L: codes can't spell words and are hard to misread.
const CODE_ALPHABET = 'BCDFGHJKMNPQRSTVWXZ23456789';
const CODE_LEN = 6;
const LOOKUPS_PER_IP_PER_MIN = 30;
const FAILED_LOOKUPS_PER_MIN = 300;   // global backstop against code guessing
const PLAYER_UPDATE_MIN_MS = 250;        // throttle: max 4 player_update/s per room
const MAX_PAYLOAD = 64 * 1024;
const MAX_ROOMS_PER_IP_PER_HOUR = 10;
const EVENT_LOG_SIZE = 50;

const EVENT_TYPES = new Set([
  'phase_change', 'agent_lock', 'match_start', 'player_update',
  'round_update', 'heartbeat', 'match_end',
]);

const VIEWER_HTML = fs.readFileSync(path.join(__dirname, 'public', 'viewer.html'));

/** @type {Map<string, Room>} */
const rooms = new Map();
const codes = new Map();      // 6-char code -> room id
const createLog = new Map();  // ip -> [timestamps]
const lookupLog = new Map();  // ip -> [timestamps]
let failedLookups = [];       // timestamps of failed code lookups (all clients)

function makeCode() {
  for (;;) {
    let c = '';
    for (let i = 0; i < CODE_LEN; i++) c += CODE_ALPHABET[crypto.randomInt(CODE_ALPHABET.length)];
    if (!codes.has(c)) return c;
  }
}
const formatCode = c => `${c.slice(0, 3)}-${c.slice(3)}`;
const normaliseCode = raw => String(raw || '').toUpperCase().replace(/[^A-Z0-9]/g, '');

function newSnapshot() {
  return {
    phase: 'offline',
    map: null,
    mode: null,
    score: { ally: 0, enemy: 0 },
    side: null,
    round: null,
    roundPhase: null,
    agents: {},   // slot -> agent
    players: {},  // id -> latest ally state
    result: null,
  };
}

function createRoom() {
  const id = crypto.randomBytes(5).toString('hex');
  const token = crypto.randomBytes(16).toString('hex');
  const code = makeCode();
  const room = {
    id, token, code,
    createdAt: Date.now(),
    snapshot: newSnapshot(),
    log: [],
    viewers: new Set(),
    agent: null,
    lastPlayerUpdate: 0,
    lastSeen: Date.now(),
    agentGone: null,
  };
  rooms.set(id, room);
  codes.set(code, id);
  return room;
}

function destroyRoom(room, reason) {
  for (const v of room.viewers) v.close(4001, reason);
  codes.delete(room.code);
  rooms.delete(room.id);
}

// ---- sanitising / reducing ------------------------------------------------

function hashId(id) {
  // never forward raw puuids; stable per-room pseudonym instead
  return crypto.createHash('sha256').update(String(id)).digest('hex').slice(0, 8);
}

function validate(msg) {
  if (!msg || typeof msg !== 'object') return 'not an object';
  if (msg.v !== 1) return 'unsupported version';
  if (!EVENT_TYPES.has(msg.type)) return 'unknown type';
  if (typeof msg.ts !== 'number') return 'bad ts';
  if (msg.data === null || typeof msg.data !== 'object') return 'bad data';
  return null;
}

function sanitise(msg) {
  const out = { v: 1, type: msg.type, ts: msg.ts, data: { ...msg.data } };
  const d = out.data;
  delete d.puuid; delete d.token; delete d.ip; delete d.riot_id;

  if (msg.type === 'player_update') {
    const list = Array.isArray(d.players) ? d.players : [];
    d.players = list
      .filter(p => p && p.ally === true)          // own team only
      .map(p => ({
        id: hashId(p.id ?? p.puuid ?? p.name ?? 'x'),
        name: typeof p.name === 'string' ? p.name.slice(0, 24) : undefined,
        agent: p.agent,
        hp: p.hp,
        credits: p.credits,
        alive: p.alive,
        x: p.x, y: p.y,
        ally: true,
      }));
  }
  if (msg.type === 'agent_lock') {
    if (d.ally !== true) return null;              // ignore enemy locks
    d.id = hashId(d.id ?? d.puuid ?? d.name ?? 'x');
  }
  return out;
}

function reduce(snap, msg) {
  const d = msg.data;
  switch (msg.type) {
    case 'phase_change': snap.phase = d.phase ?? snap.phase; break;
    case 'agent_lock': snap.agents[d.id] = { name: d.name, agent: d.agent }; break;
    case 'match_start':
      snap.map = d.map ?? null; snap.mode = d.mode ?? null;
      snap.score = { ally: 0, enemy: 0 }; snap.round = 0; snap.result = null;
      snap.phase = 'ingame'; snap.players = {};
      break;
    case 'round_update':
      if (d.round != null) snap.round = d.round;
      if (d.score) snap.score = d.score;
      if (d.side) snap.side = d.side;
      if (d.phase) snap.roundPhase = d.phase;
      break;
    case 'player_update':
      for (const p of d.players) snap.players[p.id] = p;
      break;
    case 'match_end': snap.result = d; snap.phase = 'ended'; break;
  }
}

// ---- broadcasting ---------------------------------------------------------

function send(ws, obj) {
  if (ws.readyState === 1) ws.send(JSON.stringify(obj));
}
function broadcast(room, obj) {
  const s = JSON.stringify(obj);
  for (const v of room.viewers) if (v.readyState === 1) v.send(s);
}
function announceViewers(room) {
  const n = room.viewers.size;
  if (room.agent) send(room.agent, { type: 'viewers', n });
  broadcast(room, { type: 'viewers', n });
}

// ---- http -----------------------------------------------------------------

function clientIp(req) {
  return (req.headers['x-forwarded-for'] || req.socket.remoteAddress || '').split(',')[0].trim();
}
function rateLimited(ip) {
  const now = Date.now();
  const hits = (createLog.get(ip) || []).filter(t => now - t < 3600_000);
  if (hits.length >= MAX_ROOMS_PER_IP_PER_HOUR) { createLog.set(ip, hits); return true; }
  hits.push(now); createLog.set(ip, hits);
  return false;
}
function lookupAllowed(ip) {
  const now = Date.now();
  const hits = (lookupLog.get(ip) || []).filter(t => now - t < 60_000);
  failedLookups = failedLookups.filter(t => now - t < 60_000);
  if (hits.length >= LOOKUPS_PER_IP_PER_MIN || failedLookups.length >= FAILED_LOOKUPS_PER_MIN) {
    lookupLog.set(ip, hits); return false;
  }
  hits.push(now); lookupLog.set(ip, hits);
  return true;
}
function roomForCode(raw) {
  const c = normaliseCode(raw);
  const id = c.length === CODE_LEN ? codes.get(c) : undefined;
  if (!id) failedLookups.push(Date.now());
  return id ? rooms.get(id) : undefined;
}
function json(res, code, obj) {
  res.writeHead(code, { 'content-type': 'application/json' });
  res.end(JSON.stringify(obj));
}

const server = http.createServer((req, res) => {
  const url = new URL(req.url, 'http://x');
  const m = url.pathname.match(/^\/s\/([a-f0-9]{10})$/);

  if (req.method === 'GET' && url.pathname === '/healthz') return json(res, 200, { ok: true, rooms: rooms.size });

  if (req.method === 'POST' && url.pathname === '/api/rooms') {
    if (rateLimited(clientIp(req))) return json(res, 429, { error: 'too many rooms, try later' });
    const room = createRoom();
    return json(res, 201, {
      room: room.id, token: room.token,
      code: formatCode(room.code), viewerPath: `/s/${room.id}`,
    });
  }

  const byCode = url.pathname.match(/^\/api\/codes\/([^/]+)$/);
  if (req.method === 'GET' && byCode) {
    if (!lookupAllowed(clientIp(req))) return json(res, 429, { error: 'too many lookups, slow down' });
    const room = roomForCode(decodeURIComponent(byCode[1]));
    if (!room) return json(res, 404, { error: 'code not found or stream has ended' });
    return json(res, 200, { room: room.id, viewerPath: `/s/${room.id}`, live: !!room.agent });
  }

  const codePage = url.pathname.match(/^\/c\/([^/]+)$/);
  if (req.method === 'GET' && codePage) {
    if (!lookupAllowed(clientIp(req))) { res.writeHead(429, { 'content-type': 'text/plain' }); return res.end('Too many lookups, slow down.'); }
    const room = roomForCode(decodeURIComponent(codePage[1]));
    if (!room) {
      res.writeHead(404, { 'content-type': 'text/html; charset=utf-8' });
      return res.end('<!doctype html><meta name="viewport" content="width=device-width"><body style="font:16px system-ui;background:#0f1923;color:#ece8e1;display:grid;place-items:center;height:100vh;margin:0"><p>Code not found, or that stream has ended.</p>');
    }
    res.writeHead(302, { location: `/s/${room.id}` });
    return res.end();
  }

  if (req.method === 'GET' && m) {
    res.writeHead(200, { 'content-type': 'text/html; charset=utf-8', 'cache-control': 'no-store' });
    return res.end(VIEWER_HTML);
  }

  if (req.method === 'GET' && url.pathname === '/') {
    res.writeHead(200, { 'content-type': 'text/plain' });
    return res.end('val-relay is running. Create a room with POST /api/rooms.\n');
  }

  json(res, 404, { error: 'not found' });
});

// ---- websockets -----------------------------------------------------------

const wss = new WebSocketServer({ noServer: true, maxPayload: MAX_PAYLOAD });

server.on('upgrade', (req, socket, head) => {
  const url = new URL(req.url, 'http://x');
  const room = rooms.get(url.searchParams.get('room') || '');
  if (!room) { socket.write('HTTP/1.1 404 Not Found\r\n\r\n'); return socket.destroy(); }

  if (url.pathname === '/ingest') {
    const t = Buffer.from(url.searchParams.get('token') || '');
    const ok = t.length === room.token.length && crypto.timingSafeEqual(t, Buffer.from(room.token));
    if (!ok) { socket.write('HTTP/1.1 401 Unauthorized\r\n\r\n'); return socket.destroy(); }
    wss.handleUpgrade(req, socket, head, ws => onAgent(room, ws));
  } else if (url.pathname === '/watch') {
    wss.handleUpgrade(req, socket, head, ws => onViewer(room, ws));
  } else {
    socket.write('HTTP/1.1 404 Not Found\r\n\r\n'); socket.destroy();
  }
});

function onAgent(room, ws) {
  if (room.agent && room.agent.readyState === 1) room.agent.close(4000, 'replaced by new agent');
  room.agent = ws; room.agentGone = null; room.lastSeen = Date.now();
  ws.isAlive = true;
  ws.on('pong', () => { ws.isAlive = true; });
  send(ws, { type: 'hello', room: room.id, viewers: room.viewers.size });
  broadcast(room, { type: 'agent', online: true });

  ws.on('message', raw => {
    let msg;
    try { msg = JSON.parse(raw.toString()); } catch { return send(ws, { type: 'error', error: 'bad json' }); }
    const err = validate(msg);
    if (err) return send(ws, { type: 'error', error: err });

    room.lastSeen = Date.now();
    if (msg.type === 'heartbeat') return; // keepalive only

    if (msg.type === 'player_update') {
      if (Date.now() - room.lastPlayerUpdate < PLAYER_UPDATE_MIN_MS) return; // throttle
      room.lastPlayerUpdate = Date.now();
    }

    const clean = sanitise(msg);
    if (!clean) return;
    reduce(room.snapshot, clean);
    if (clean.type !== 'player_update') {
      room.log.push(clean);
      if (room.log.length > EVENT_LOG_SIZE) room.log.shift();
    }
    broadcast(room, { type: 'event', event: clean });
  });

  ws.on('close', () => {
    if (room.agent === ws) {
      room.agent = null; room.agentGone = Date.now();
      room.snapshot.phase = 'offline';
      broadcast(room, { type: 'agent', online: false });
    }
  });
}

function onViewer(room, ws) {
  room.viewers.add(ws);
  ws.isAlive = true;
  ws.on('pong', () => { ws.isAlive = true; });
  send(ws, {
    type: 'snapshot',
    snapshot: room.snapshot,
    log: room.log,
    agentOnline: !!room.agent,
    viewers: room.viewers.size,
  });
  announceViewers(room);
  ws.on('message', () => {}); // viewers are read-only; ignore anything they send
  ws.on('close', () => { room.viewers.delete(ws); announceViewers(room); });
}

// ---- housekeeping ---------------------------------------------------------

setInterval(() => {
  for (const ws of wss.clients) {
    if (ws.isAlive === false) { ws.terminate(); continue; }
    ws.isAlive = false; ws.ping();
  }
}, 30_000).unref();

// A stream is over when its agent has been gone longer than the grace period.
setInterval(() => {
  const now = Date.now();
  for (const room of [...rooms.values()]) {
    if (!room.agent && room.agentGone && now - room.agentGone > ROOM_GRACE_MS) destroyRoom(room, 'stream ended');
    else if (!room.agent && !room.agentGone && now - room.createdAt > UNUSED_TTL_MS) destroyRoom(room, 'room expired');
  }
}, SWEEP_MS).unref();

server.listen(PORT, () => console.log(`val-relay listening on :${PORT}`));

module.exports = { server, rooms };
