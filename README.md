# val-relay

Relay for val-overlay's live stream link.

    Electron agent --WS /ingest--> relay --WS /watch--> browser viewers (/s/<room>)

## Run

    npm install
    npm start            # relay on :8787
    npm run mock         # in another terminal: fake match, prints a viewer link
    npm test             # end-to-end smoke test

Open the printed `/s/<room>` link in a browser to watch the mock match.

## Codes

`POST /api/rooms` also returns a short `code` (like `K7Q-X92`, vowel-free so it can't spell
words). `GET /api/codes/<code>` returns the room, and `GET /c/<code>` redirects to the
viewer page. Lookups are rate limited. A code lasts until the streamer has been gone for
2 minutes (`ROOM_GRACE_MS`); viewers then see "stream ended". A room nobody connected to is
dropped after 10 minutes (`UNUSED_TTL_MS`).

Hosting: see `DEPLOY.md` (Render free tier).

## Protocol (agent -> relay)

Create a room: `POST /api/rooms` -> `{ room, token, viewerPath }`.
Connect: `ws://host/ingest?room=<room>&token=<token>`.
Every message: `{ "v": 1, "type": "...", "ts": <ms>, "data": {...} }`

Types: `phase_change`, `agent_lock`, `match_start`, `player_update`,
`round_update`, `heartbeat`, `match_end`.

## Integrity rules (enforced server-side)

- Only the streamer's own team is accepted: `player_update` entries and
  `agent_lock` events without `ally: true` are dropped.
- Raw puuids/Riot IDs are stripped; players get a per-event hashed id.
- `player_update` is throttled to 4/s per room.
- Viewers are read-only.

## Not done yet

- Riot compliance: the policy asks for RSO opt-in + a Production API Key for streaming apps (ticket #138436932 is still open).
- Real agent: see the sibling `val-agent` project (standalone; mock-agent.js stays for testing).
- Hosting: Fly.io config, wss/TLS, and a persistent room store if you want rooms to survive restarts.
