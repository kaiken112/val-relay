# Deploying val-relay to Render (free)

The relay is a plain Node server, so Render runs it as-is.

1. Put the `val-relay` folder in its own GitHub repo (the folder itself is the repo root: `server.js`, `package.json`, `render.yaml` at the top).
2. On render.com: **New +** > **Blueprint**, pick the repo. It reads `render.yaml` (free web service, `npm install` / `npm start`, health check `/healthz`).
   Or by hand: **New +** > **Web Service**, runtime Node, build `npm install`, start `npm start`, instance type Free.
3. When it's live you get a URL like `https://val-relay-xxxx.onrender.com`. Check `https://<your-url>/healthz` returns `{"ok":true,...}`.
4. Point the agent at it:

       npm start -- --relay https://val-relay-xxxx.onrender.com

   or set `VAL_RELAY_URL` once and just run `npm start`.

## Things to know

- **Free tier sleeps after ~15 min with no traffic**; the first request then takes about a minute. The agent wakes the relay as soon as it starts and says so, then connects. While someone is streaming, the agent's heartbeat keeps it awake.
- Rooms and codes live in memory. A restart or sleep ends active streams; the agent just makes a new room and prints a new code.
- A code works until the streamer has been disconnected for 2 minutes (`ROOM_GRACE_MS`). A room nobody connected to is dropped after 10 minutes (`UNUSED_TTL_MS`).
- Render's dashboard labels may differ slightly from the steps above; the settings themselves (Node, `npm install`, `npm start`, Free) are what matter.
