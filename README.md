# TransLink Bus MCP Server

Real-time TransLink bus arrivals for Poke (or any MCP-compatible client).
Ask things like:
- *"yo what's the bus times for 15 St going to Vancouver"*
- *"when's the next bus at stop 55079"*
- *"buses at Lonsdale & 13th heading downtown"*
- *"any service alerts for the 230?"*

## Tools

| Tool | What it does |
|---|---|
| `translink_get_bus_times` | Main tool — natural language stop lookup + live arrivals |
| `translink_find_stops` | Search stops by intersection/name, returns stop codes |
| `translink_get_alerts` | Live service alerts, optional route filter |

---

## Setup

### 1. Get a TransLink API key
Register free at [developer.translink.ca](https://developer.translink.ca)

### 2. Deploy

#### Option A — Fly.io (recommended, free tier available)
```bash
# Install flyctl: https://fly.io/docs/hands-on/install-flyctl/
fly auth login
fly launch        # first time — follow prompts, use fly.toml config
fly secrets set TRANSLINK_API_KEY=your_key_here
fly deploy
```
Your server URL will be: `https://translink-mcp.fly.dev/mcp`

#### Option B — Railway
1. Push this folder to a GitHub repo
2. New project → Deploy from GitHub → select repo
3. Add env var: `TRANSLINK_API_KEY=your_key_here`
4. Railway auto-detects Dockerfile and deploys

Your server URL will be in Railway dashboard under "Networking".

#### Option C — Run locally (for testing)
```bash
pip install -r requirements.txt
export TRANSLINK_API_KEY=your_key_here
python server.py
# Server runs at http://localhost:8000/mcp
```

---

### 3. Add to Poke
1. Open Poke → Settings → Integrations → Add Custom MCP Server
2. Paste your server URL (e.g. `https://translink-mcp.fly.dev/mcp`)
3. Save — that's it

---

## Usage examples

```
you: yo what's the bus times for 15 st going to Vancouver
poke: 🚌 Stop #54439 — NB 15 ST FS MARINE DR
  Route 253 → Caulfeild  ·  2 min
  Route 257 → Horseshoe Bay  ·  8 min
  Route 253 → Caulfeild  ·  22 min

you: buses at park royal
poke: 🚌 Stop #53230 — Park Royal Exchange
  Route R2 → Phibbs Exchange  ·  4 min
  Route 250 → Dundarave  ·  6 min
  ...

you: stop 55079
poke: 🚌 Stop #55079 — SB 123A ST FS 99 AVE
  ...
```

---

## Notes
- GTFS-RT feed is cached for 30 seconds to avoid hammering the API
- GTFS static data (stop names, routes) cached for 24 hours
- On first request after deploy, server pre-fetches static GTFS (~2-5s startup)
- Attribution required by TransLink ToS: "Route and arrival data used in this product or service is provided by permission of TransLink."
