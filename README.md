# Card-War

Card Wars — a Unity WebGL card game, served together with its own WebSocket
multiplayer relay.

This folder is a self-contained copy of the WebGL build plus the Python server that
serves the files **and** the `/ws` multiplayer relay on the same origin.

```
index.html
Build/                 # Unity build files (+ precompressed .gz)
TemplateData/
serve_webgl.py         # static server + WebSocket relay
render.yaml            # Render Blueprint
requirements.txt       # no third-party dependencies
```

## Test locally

```powershell
$env:PORT=8092
python serve_webgl.py
```

Open <http://127.0.0.1:8092/>. The game's online mode automatically connects to
`ws://<same-host>/ws`, so multiplayer works without any URL configuration.

## Deploy to Render

1. Push this repository to GitHub.
2. Sign in at <https://dashboard.render.com>.
3. **New +** → **Blueprint**.
4. Connect GitHub and select this repository.
5. Render reads `render.yaml` and creates a **Web Service** named `card-wars`
   (Python, free plan, start command `python serve_webgl.py`).
6. Click **Apply** and wait for the first deploy.

Your game will be at `https://card-wars-xxxx.onrender.com`.

If you prefer the manual route: **New +** → **Web Service** → pick the repo →
Runtime **Python 3** → Build Command `echo ok` → Start Command
`python serve_webgl.py` → Plan **Free**.

Online 1v1 uses `wss://<render-host>/ws` automatically because the client
connects to its own origin.

## Free-plan behavior

- The service **sleeps after ~15 minutes without traffic**. The next visit takes
  roughly 30–60 seconds to wake up. After that it is normal speed.
- 512 MB RAM, shared CPU — plenty for serving this build.
- WebSockets are supported on the free plan.
- To keep it always warm, either upgrade the service to **Starter ($7/month)** or
  point a free uptime monitor (e.g. UptimeRobot) at the URL every 10 minutes.
- Keep the service at **one instance**. Rooms live in memory, so multiple
  instances would not see each other's rooms.

## Updating the game

After rebuilding in Unity:

```powershell
powershell -ExecutionPolicy Bypass -File .\prepare_render_deploy.ps1
git add .
git commit -m "Update build"
git push
```

`prepare_render_deploy.ps1` copies only the files `index.html` actually loads and
skips stale artifacts from older builds. Render redeploys automatically on push.

## Notes

- `serve_webgl.py` reads the `PORT` environment variable injected by Render and
  binds to `0.0.0.0`.
- Precompressed `.gz` files are committed, so a cold start does not spend CPU
  compressing the 75 MB WASM.
- The high-DPI text fix is already baked into `index.html`.
- Custom domain: Render dashboard → your service → **Settings** → **Custom
  Domains**.
