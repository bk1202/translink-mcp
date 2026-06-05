"""
TransLink Bus MCP Server
Real-time bus arrivals for Metro Vancouver via TransLink GTFS-RT.
Stop lookup uses geocoding (Nominatim) + proximity matching + direction hints.
"""

import os
import io
import csv
import math
import time
import zipfile
import logging
from typing import Optional
from contextlib import asynccontextmanager

import httpx
from rapidfuzz import fuzz, process
from pydantic import BaseModel, Field, ConfigDict
from mcp.server.fastmcp import FastMCP
from mcp.server.transport_security import TransportSecuritySettings

logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)

# ─── Constants ────────────────────────────────────────────────────────────────

GTFS_RT_URL        = "https://gtfs.translink.ca/v2/gtfsrealtime"
GTFS_ALERTS_URL    = "https://gtfs.translink.ca/v2/gtfsrealtimealerts"
GTFS_STATIC_URL    = "https://gtfs.translink.ca/v2/gtfs"
NOMINATIM_URL      = "https://nominatim.openstreetmap.org/search"
GEOCODE_RADIUS_M   = 400    # max metres from geocoded point to consider a stop
GTFS_STATIC_TTL    = 86400  # 24h
GTFS_RT_TTL        = 30     # 30s

API_KEY = os.environ.get("TRANSLINK_API_KEY", "")

# Direction keywords → tokens to look for in stop name/desc
DIRECTION_MAP = {
    "vancouver":  ["sb", "southbound", "eb", "eastbound"],
    "downtown":   ["sb", "southbound", "eb", "eastbound"],
    "north":      ["nb", "northbound"],
    "north van":  ["nb", "northbound"],
    "west van":   ["wb", "westbound"],
    "west":       ["wb", "westbound"],
    "east":       ["eb", "eastbound"],
    "south":      ["sb", "southbound"],
    "burnaby":    ["eb", "eastbound"],
    "park royal": ["wb", "westbound"],
    "horseshoe":  ["wb", "westbound"],
    "caulfeild":  ["wb", "westbound"],
    "phibbs":     ["eb", "eastbound", "nb", "northbound"],
    "lonsdale":   ["nb", "northbound"],
}

# ─── Cache ────────────────────────────────────────────────────────────────────

class Cache:
    def __init__(self):
        self.rt_feed   = None
        self.rt_ts     = 0.0
        self.stops:  dict[str, dict] = {}  # stop_id → {id, code, name, desc, lat, lon}
        self.routes: dict[str, str]  = {}  # route_id → short_name
        self.static_ts = 0.0

cache = Cache()

# ─── GTFS static ──────────────────────────────────────────────────────────────

async def load_static_gtfs():
    if time.time() - cache.static_ts < GTFS_STATIC_TTL and cache.stops:
        return
    logger.info("Fetching GTFS static data...")
    async with httpx.AsyncClient(timeout=30) as client:
        r = await client.get(GTFS_STATIC_URL, params={"apikey": API_KEY})
        r.raise_for_status()
    with zipfile.ZipFile(io.BytesIO(r.content)) as z:
        with z.open("stops.txt") as f:
            for row in csv.DictReader(io.TextIOWrapper(f, "utf-8")):
                sid = row["stop_id"].strip()
                cache.stops[sid] = {
                    "id":   sid,
                    "code": row.get("stop_code", "").strip(),
                    "name": row.get("stop_name", "").strip(),
                    "desc": row.get("stop_desc", "").strip(),
                    "lat":  row.get("stop_lat",  "").strip(),
                    "lon":  row.get("stop_lon",  "").strip(),
                }
        with z.open("routes.txt") as f:
            for row in csv.DictReader(io.TextIOWrapper(f, "utf-8")):
                rid = row["route_id"].strip()
                cache.routes[rid] = row.get("route_short_name", rid).strip()
    cache.static_ts = time.time()
    logger.info(f"Loaded {len(cache.stops)} stops, {len(cache.routes)} routes")

# ─── GTFS-RT ──────────────────────────────────────────────────────────────────

async def load_rt_feed():
    from google.transit import gtfs_realtime_pb2
    if time.time() - cache.rt_ts < GTFS_RT_TTL and cache.rt_feed is not None:
        return cache.rt_feed
    async with httpx.AsyncClient(timeout=15) as client:
        r = await client.get(GTFS_RT_URL, params={"apikey": API_KEY})
        r.raise_for_status()
    feed = gtfs_realtime_pb2.FeedMessage()
    feed.ParseFromString(r.content)
    cache.rt_feed = feed
    cache.rt_ts = time.time()
    return feed

# ─── Geocoding ────────────────────────────────────────────────────────────────

async def geocode(query: str) -> tuple[float, float] | None:
    """Geocode a query in Metro Vancouver using Nominatim. Returns (lat, lon) or None."""
    # Append Vancouver context if not already present
    search = query if any(kw in query.lower() for kw in ["vancouver", "bc", "burnaby", "surrey", "richmond"]) \
             else f"{query}, Metro Vancouver, BC, Canada"
    try:
        async with httpx.AsyncClient(timeout=8, headers={"User-Agent": "translink-mcp/1.0 (personal transit tool)"}) as client:
            r = await client.get(NOMINATIM_URL, params={"q": search, "format": "json", "limit": 1, "countrycodes": "ca"})
            r.raise_for_status()
            results = r.json()
        if results:
            return float(results[0]["lat"]), float(results[0]["lon"])
    except Exception as e:
        logger.warning(f"Geocode failed for '{query}': {e}")
    return None

# ─── Stop search ──────────────────────────────────────────────────────────────

def haversine(lat1: float, lon1: float, lat2: float, lon2: float) -> float:
    """Distance in metres between two points."""
    R = 6371000
    p1, p2 = math.radians(lat1), math.radians(lat2)
    dp = math.radians(lat2 - lat1)
    dl = math.radians(lon2 - lon1)
    a = math.sin(dp/2)**2 + math.cos(p1)*math.cos(p2)*math.sin(dl/2)**2
    return R * 2 * math.atan2(math.sqrt(a), math.sqrt(1 - a))

def extract_dir_hints(query: str) -> list[str]:
    """Extract direction tokens from a natural language query."""
    q = query.lower()
    hints = []
    for kw, tokens in DIRECTION_MAP.items():
        if kw in q:
            hints.extend(tokens)
    return list(set(hints))

def dir_score(stop: dict, hints: list[str]) -> int:
    """How many direction hints appear in the stop's name/desc."""
    text = (stop["name"] + " " + stop["desc"]).lower()
    return sum(1 for h in hints if h in text)

def find_stops_near(lat: float, lon: float, dir_hints: list[str], limit: int = 5) -> list[dict]:
    """Find stops within GEOCODE_RADIUS_M metres, ranked by direction then distance."""
    candidates = []
    for stop in cache.stops.values():
        if not stop["lat"] or not stop["lon"]:
            continue
        dist = haversine(lat, lon, float(stop["lat"]), float(stop["lon"]))
        if dist > GEOCODE_RADIUS_M:
            continue
        candidates.append((dist, stop))
    # Sort: direction matches first, then by distance
    candidates.sort(key=lambda x: (-dir_score(x[1], dir_hints), x[0]))
    return [s for _, s in candidates[:limit]]

def find_stops_by_code(code: str) -> Optional[dict]:
    for stop in cache.stops.values():
        if stop["code"] == code.strip():
            return stop
    return None

def find_stops_fuzzy(query: str, dir_hints: list[str], limit: int = 5) -> list[dict]:
    """Fuzzy text match, re-ranked by direction score."""
    q = query.lower().strip()
    candidates = {sid: (s["name"] + " " + s["desc"] + " " + s["code"]).lower()
                  for sid, s in cache.stops.items()}
    results = process.extract(q, candidates, scorer=fuzz.partial_ratio, limit=limit * 4)
    ranked = []
    for match_str, score, sid in results:
        bonus = 20 if q in match_str else 0
        ranked.append((score + bonus, sid))
    ranked.sort(reverse=True)
    seen, out = set(), []
    for _, sid in ranked:
        if sid not in seen:
            seen.add(sid)
            out.append(cache.stops[sid])
        if len(out) >= limit * 2:
            break
    # Re-rank by direction
    out.sort(key=lambda s: -dir_score(s, dir_hints))
    return out[:limit]

# ─── Arrivals ─────────────────────────────────────────────────────────────────

async def get_arrivals_for_stop(stop_id: str, limit: int = 6) -> list[dict]:
    feed = await load_rt_feed()
    now = int(time.time())
    arrivals = []
    for entity in feed.entity:
        if not entity.HasField("trip_update"):
            continue
        tu = entity.trip_update
        for stu in tu.stop_time_update:
            if stu.stop_id != stop_id:
                continue
            arr_time = stu.arrival.time if stu.HasField("arrival") else stu.departure.time
            if arr_time <= now:
                continue
            route_id = tu.trip.route_id
            arrivals.append({
                "route":    cache.routes.get(route_id, route_id),
                "headsign": tu.trip.trip_headsign or "",
                "mins":     round((arr_time - now) / 60),
                "arr_time": arr_time,
            })
    arrivals.sort(key=lambda x: x["arr_time"])
    return arrivals[:limit]

def format_arrivals(stop: dict, arrivals: list[dict], alternates: list[dict]) -> str:
    label = stop["name"]
    if stop["desc"]:
        label += f" ({stop['desc']})"
    if not arrivals:
        out = f"🚌 Stop #{stop['code']} — {label}\nNo upcoming arrivals right now."
    else:
        lines = [f"🚌 Stop #{stop['code']} — {label}\n"]
        for a in arrivals:
            eta = "Due now" if a["mins"] == 0 else f"{a['mins']} min"
            hs  = f" → {a['headsign']}" if a["headsign"] else ""
            lines.append(f"  {a['route']}{hs}  ·  {eta}")
        out = "\n".join(lines)
    if alternates:
        out += "\n\n💡 Nearby stops also matched:"
        for alt in alternates[:3]:
            out += f"\n  #{alt['code']} — {alt['name']} ({alt['desc']})"
    return out

# ─── Lifespan ─────────────────────────────────────────────────────────────────

@asynccontextmanager
async def lifespan(server: FastMCP):
    if not API_KEY:
        logger.warning("TRANSLINK_API_KEY not set — requests will fail.")
    try:
        await load_static_gtfs()
    except Exception as e:
        logger.error(f"Failed to pre-load GTFS static data: {e}")
    yield

# ─── MCP Server ───────────────────────────────────────────────────────────────

mcp = FastMCP(
    "translink_mcp",
    lifespan=lifespan,
    transport_security=TransportSecuritySettings(enable_dns_rebinding_protection=False),
)

# ─── Tool 1: Bus times ────────────────────────────────────────────────────────

class BusTimesInput(BaseModel):
    model_config = ConfigDict(str_strip_whitespace=True, extra="forbid")
    query: str = Field(
        ...,
        description=(
            "Natural language stop query. Examples: '15th & Marine going to Vancouver', "
            "'Lonsdale & 13th heading downtown', 'Park Royal towards Phibbs', "
            "'stop 55079', 'Capilano & Welch northbound'. "
            "Always include a direction hint (going to X / towards X / northbound etc) "
            "for best results."
        ),
        min_length=2, max_length=200,
    )
    limit: int = Field(default=6, description="Max arrivals to return (1–10).", ge=1, le=10)


@mcp.tool(
    name="translink_get_bus_times",
    annotations={"title": "Get TransLink Bus Arrival Times", "readOnlyHint": True,
                 "destructiveHint": False, "idempotentHint": False, "openWorldHint": True},
)
async def translink_get_bus_times(params: BusTimesInput) -> str:
    """
    Get real-time TransLink bus arrivals for a stop in Metro Vancouver.

    Uses geocoding to find the physically correct stop for an intersection,
    then ranks by direction hint (e.g. 'going to Vancouver', 'northbound').
    Falls back to fuzzy text search if geocoding returns no results.

    Always include a direction in the query for accuracy — e.g.
    '15th & Marine going to Vancouver' not just '15th & Marine'.
    """
    await load_static_gtfs()
    query = params.query.strip()
    dir_hints = extract_dir_hints(query)

    stop = None
    alternates = []

    # 1. Exact stop code
    if query.isdigit() and len(query) == 5:
        stop = find_stops_by_code(query)

    # 2. Geocode → proximity search
    if stop is None:
        coords = await geocode(query)
        if coords:
            matches = find_stops_near(coords[0], coords[1], dir_hints, limit=6)
            if matches:
                stop = matches[0]
                alternates = matches[1:]

    # 3. Fuzzy fallback
    if stop is None:
        matches = find_stops_fuzzy(query, dir_hints, limit=5)
        if not matches:
            return (f"Couldn't find any stops matching '{query}'. "
                    "Try including the full intersection and a direction, "
                    "e.g. 'Marine Dr & 15th St going to Vancouver'.")
        stop = matches[0]
        alternates = matches[1:]

    try:
        arrivals = await get_arrivals_for_stop(stop["id"], limit=params.limit)
    except httpx.HTTPStatusError as e:
        if e.response.status_code == 401:
            return "Error: Invalid TransLink API key."
        return f"Error fetching real-time data: HTTP {e.response.status_code}"
    except Exception as e:
        return f"Error fetching real-time data: {e}"

    return format_arrivals(stop, arrivals, alternates)


# ─── Tool 2: Find stops ───────────────────────────────────────────────────────

class FindStopsInput(BaseModel):
    model_config = ConfigDict(str_strip_whitespace=True, extra="forbid")
    query: str = Field(..., description="Intersection, street, or landmark.", min_length=2, max_length=200)
    limit: int = Field(default=6, description="Max stops to return (1–10).", ge=1, le=10)


@mcp.tool(
    name="translink_find_stops",
    annotations={"title": "Find TransLink Bus Stops", "readOnlyHint": True,
                 "destructiveHint": False, "idempotentHint": True, "openWorldHint": False},
)
async def translink_find_stops(params: FindStopsInput) -> str:
    """
    Find TransLink stops near an intersection or landmark, with coordinates.
    Useful for discovering the exact stop code before querying arrivals.
    Uses geocoding for accuracy, falls back to fuzzy text search.
    """
    await load_static_gtfs()
    dir_hints = extract_dir_hints(params.query)

    coords = await geocode(params.query)
    if coords:
        matches = find_stops_near(coords[0], coords[1], dir_hints, limit=params.limit)
    else:
        matches = find_stops_fuzzy(params.query, dir_hints, limit=params.limit)

    if not matches:
        return f"No stops found matching '{params.query}'."

    lines = [f"Stops near '{params.query}':\n"]
    for s in matches:
        dist_str = ""
        if coords and s["lat"] and s["lon"]:
            d = haversine(coords[0], coords[1], float(s["lat"]), float(s["lon"]))
            dist_str = f"  ({int(d)}m)"
        desc = f" — {s['desc']}" if s["desc"] else ""
        lines.append(f"  #{s['code']}  {s['name']}{desc}{dist_str}")

    lines.append("\nUse the stop number with translink_get_bus_times for live arrivals.")
    return "\n".join(lines)


# ─── Tool 3: Service alerts ───────────────────────────────────────────────────

@mcp.tool(
    name="translink_get_alerts",
    annotations={"title": "Get TransLink Service Alerts", "readOnlyHint": True,
                 "destructiveHint": False, "idempotentHint": False, "openWorldHint": True},
)
async def translink_get_alerts(route: Optional[str] = None) -> str:
    """
    Get active TransLink service alerts. Filter by route number (e.g. '250', 'R2').
    If no route given, returns all active alerts.
    """
    from google.transit import gtfs_realtime_pb2
    async with httpx.AsyncClient(timeout=15) as client:
        r = await client.get(GTFS_ALERTS_URL, params={"apikey": API_KEY})
        r.raise_for_status()
    feed = gtfs_realtime_pb2.FeedMessage()
    feed.ParseFromString(r.content)

    alerts = []
    for entity in feed.entity:
        if not entity.HasField("alert"):
            continue
        alert = entity.alert
        if route:
            route_names = [cache.routes.get(ie.route_id, ie.route_id)
                           for ie in alert.informed_entity if ie.route_id]
            if route.upper() not in [r.upper() for r in route_names]:
                continue
        header = next((t.translation[0].text for t in [alert.header_text] if t.translation), "No header")
        desc   = next((t.translation[0].text for t in [alert.description_text] if t.translation), "")
        affected = list({cache.routes.get(ie.route_id, ie.route_id)
                         for ie in alert.informed_entity if ie.route_id})
        alerts.append({"header": header, "desc": desc, "affected": affected})

    if not alerts:
        return f"No active alerts{f' for route {route}' if route else ''}."

    lines = [f"⚠️ {len(alerts)} active alert(s):\n"]
    for a in alerts:
        routes_str = ", ".join(a["affected"]) if a["affected"] else "General"
        lines.append(f"[{routes_str}] {a['header']}")
        if a["desc"]:
            lines.append(f"  {a['desc'][:200]}")
        lines.append("")
    return "\n".join(lines)


# ─── Entry point ──────────────────────────────────────────────────────────────

if __name__ == "__main__":
    import uvicorn
    uvicorn.run(mcp.sse_app(), host="0.0.0.0", port=int(os.environ.get("PORT", 8000)))