"""
TransLink Bus MCP Server
Provides real-time bus arrival times for Metro Vancouver via TransLink's GTFS-RT feed.
Natural-language stop lookup using GTFS static data + fuzzy matching.
"""

import os
import time
import json
import csv
import io
import zipfile
import asyncio
import logging
from typing import Optional
from contextlib import asynccontextmanager

import httpx
from rapidfuzz import fuzz, process
from pydantic import BaseModel, Field, ConfigDict
from mcp.server.fastmcp import FastMCP

logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)

# ─── Constants ────────────────────────────────────────────────────────────────

GTFS_RT_URL = "https://gtfs.translink.ca/v2/gtfsrealtime"
GTFS_RT_POSITIONS_URL = "https://gtfs.translink.ca/v2/gtfsrealtimeposition"
GTFS_STATIC_URL = "https://gtfs.translink.ca/v2/gtfs"
GTFS_STATIC_CACHE_TTL = 86400      # 24h — static data changes weekly
GTFS_RT_CACHE_TTL = 30             # 30s — real-time feed

API_KEY = os.environ.get("TRANSLINK_API_KEY", "")

# ─── In-memory cache ──────────────────────────────────────────────────────────

class Cache:
    def __init__(self):
        self.rt_feed = None
        self.rt_ts = 0.0
        self.stops: dict[str, dict] = {}   # stop_id → {code, name, desc, lat, lon}
        self.stops_ts = 0.0
        self.routes: dict[str, str] = {}   # route_id → route_short_name
        self.routes_ts = 0.0

cache = Cache()

# ─── GTFS static data loaders ─────────────────────────────────────────────────

async def load_static_gtfs():
    """Download and parse GTFS static zip — stops.txt + routes.txt."""
    if time.time() - cache.stops_ts < GTFS_STATIC_CACHE_TTL and cache.stops:
        return

    logger.info("Fetching GTFS static data...")
    async with httpx.AsyncClient(timeout=30) as client:
        r = await client.get(GTFS_STATIC_URL, params={"apikey": API_KEY})
        r.raise_for_status()

    with zipfile.ZipFile(io.BytesIO(r.content)) as z:
        # Parse stops
        with z.open("stops.txt") as f:
            reader = csv.DictReader(io.TextIOWrapper(f, encoding="utf-8"))
            for row in reader:
                sid = row["stop_id"].strip()
                cache.stops[sid] = {
                    "id": sid,
                    "code": row.get("stop_code", "").strip(),
                    "name": row.get("stop_name", "").strip(),
                    "desc": row.get("stop_desc", "").strip(),
                    "lat": row.get("stop_lat", "").strip(),
                    "lon": row.get("stop_lon", "").strip(),
                }

        # Parse routes
        with z.open("routes.txt") as f:
            reader = csv.DictReader(io.TextIOWrapper(f, encoding="utf-8"))
            for row in reader:
                rid = row["route_id"].strip()
                cache.routes[rid] = row.get("route_short_name", rid).strip()

    cache.stops_ts = time.time()
    cache.routes_ts = time.time()
    logger.info(f"Loaded {len(cache.stops)} stops, {len(cache.routes)} routes")


# ─── GTFS-RT feed loader ──────────────────────────────────────────────────────

async def load_rt_feed():
    """Fetch GTFS-RT trip updates feed, cache for GTFS_RT_CACHE_TTL seconds."""
    from google.transit import gtfs_realtime_pb2

    if time.time() - cache.rt_ts < GTFS_RT_CACHE_TTL and cache.rt_feed is not None:
        return cache.rt_feed

    async with httpx.AsyncClient(timeout=15) as client:
        r = await client.get(GTFS_RT_URL, params={"apikey": API_KEY})
        r.raise_for_status()

    feed = gtfs_realtime_pb2.FeedMessage()
    feed.ParseFromString(r.content)
    cache.rt_feed = feed
    cache.rt_ts = time.time()
    return feed


# ─── Stop search helpers ──────────────────────────────────────────────────────

def _stop_search_string(stop: dict) -> str:
    """Build a combined searchable string for a stop."""
    parts = [stop["name"], stop["desc"], stop["code"]]
    return " ".join(p for p in parts if p).lower()


def find_stops_fuzzy(query: str, limit: int = 5) -> list[dict]:
    """
    Fuzzy-match a natural language query against stop names/descriptions.
    Returns up to `limit` best matching stops.
    """
    q = query.lower().strip()
    candidates = {sid: _stop_search_string(s) for sid, s in cache.stops.items()}

    results = process.extract(
        q,
        candidates,
        scorer=fuzz.partial_ratio,
        limit=limit * 3,  # over-fetch then re-rank
    )

    # Re-rank: prefer exact substring matches
    ranked = []
    for match_str, score, sid in results:
        bonus = 20 if q in match_str else 0
        ranked.append((score + bonus, sid))

    ranked.sort(reverse=True)
    seen = set()
    out = []
    for _, sid in ranked:
        if sid not in seen:
            seen.add(sid)
            out.append(cache.stops[sid])
        if len(out) >= limit:
            break
    return out


def find_stops_by_code(code: str) -> Optional[dict]:
    """Look up a stop by its 5-digit stop code (the number on the sign)."""
    for stop in cache.stops.values():
        if stop["code"] == code.strip():
            return stop
    return None


# ─── Arrival lookup ───────────────────────────────────────────────────────────

async def get_arrivals_for_stop(stop_id: str, limit: int = 6) -> list[dict]:
    """
    Pull upcoming arrivals for a stop from the GTFS-RT feed.
    Returns list of {route, headsign, mins, scheduled} sorted by arrival time.
    """
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
            mins = round((arr_time - now) / 60)
            route_id = tu.trip.route_id
            route_name = cache.routes.get(route_id, route_id)
            headsign = tu.trip.trip_headsign if tu.trip.trip_headsign else ""
            arrivals.append({
                "route": route_name,
                "headsign": headsign,
                "mins": mins,
                "arr_time": arr_time,
            })

    arrivals.sort(key=lambda x: x["arr_time"])
    return arrivals[:limit]


def format_arrivals(stop: dict, arrivals: list[dict]) -> str:
    """Format arrivals into a clean, readable string for Poke."""
    stop_label = stop["name"]
    if stop["desc"]:
        stop_label += f" ({stop['desc']})"

    if not arrivals:
        return f"No upcoming arrivals at stop {stop_label} (#{stop['code']})."

    lines = [f"🚌 Stop #{stop['code']} — {stop_label}\n"]
    for a in arrivals:
        if a["mins"] == 0:
            eta = "Due now"
        elif a["mins"] == 1:
            eta = "1 min"
        else:
            eta = f"{a['mins']} min"

        headsign = f" → {a['headsign']}" if a["headsign"] else ""
        lines.append(f"  Route {a['route']}{headsign}  ·  {eta}")

    return "\n".join(lines)


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

mcp = FastMCP("translink_mcp", lifespan=lifespan)


# ─── Tool 1: Natural-language arrivals ────────────────────────────────────────

class BusTimesInput(BaseModel):
    model_config = ConfigDict(str_strip_whitespace=True, extra="forbid")

    query: str = Field(
        ...,
        description=(
            "Natural language stop query. Can be a stop number, intersection, street name, "
            "or landmark. Examples: '55079', '15 St', 'Lonsdale & 13th', 'Marine Dr / Main St', "
            "'Park Royal Exchange'. Direction hints like 'going to Vancouver' or 'northbound' "
            "are also useful."
        ),
        min_length=2,
        max_length=200,
    )
    limit: int = Field(
        default=6,
        description="Max number of upcoming arrivals to return (1–10).",
        ge=1,
        le=10,
    )


@mcp.tool(
    name="translink_get_bus_times",
    annotations={
        "title": "Get TransLink Bus Arrival Times",
        "readOnlyHint": True,
        "destructiveHint": False,
        "idempotentHint": False,
        "openWorldHint": True,
    },
)
async def translink_get_bus_times(params: BusTimesInput) -> str:
    """
    Get real-time TransLink bus arrival times for a stop in Metro Vancouver.

    Accepts natural language: stop numbers, intersections, street names, or landmarks.
    Direction hints (e.g. 'going to Vancouver', 'eastbound', 'towards downtown') help
    disambiguate when multiple stops match.

    Args:
        params (BusTimesInput):
            - query (str): Stop identifier or natural language description
            - limit (int): Max arrivals to return (default 6)

    Returns:
        str: Formatted list of upcoming arrivals with route number, headsign, and ETA in minutes.
             If multiple stops match, returns arrivals for the best match with alternatives listed.
    """
    await load_static_gtfs()

    query = params.query.strip()

    # 1. Try exact 5-digit stop code first
    stop = None
    if query.isdigit() and len(query) == 5:
        stop = find_stops_by_code(query)

    # 2. Fuzzy search
    if stop is None:
        matches = find_stops_fuzzy(query, limit=5)
        if not matches:
            return f"Couldn't find any stops matching '{query}'. Try a stop number (e.g. 55079) or intersection (e.g. 'Marine Dr / 15 St')."

        # Direction disambiguation
        direction_keywords = {
            "vancouver": ["sb", "southbound", "eb", "eastbound", "downtown", "van"],
            "north": ["nb", "northbound", "north van", "north shore"],
            "west": ["wb", "westbound"],
            "east": ["eb", "eastbound"],
            "south": ["sb", "southbound"],
            "downtown": ["sb", "southbound", "eb", "eastbound"],
        }
        q_lower = query.lower()
        dir_hints = []
        for kw, tags in direction_keywords.items():
            if kw in q_lower:
                dir_hints.extend(tags)

        # Score matches against direction hints
        if dir_hints:
            def dir_score(s: dict) -> int:
                desc_lower = (s["name"] + " " + s["desc"]).lower()
                return sum(1 for hint in dir_hints if hint in desc_lower)

            matches.sort(key=dir_score, reverse=True)

        stop = matches[0]
        alternates = matches[1:]
    else:
        alternates = []

    try:
        arrivals = await get_arrivals_for_stop(stop["id"], limit=params.limit)
    except httpx.HTTPStatusError as e:
        if e.response.status_code == 401:
            return "Error: Invalid TransLink API key. Set TRANSLINK_API_KEY correctly."
        return f"Error fetching real-time data: HTTP {e.response.status_code}"
    except Exception as e:
        return f"Error fetching real-time data: {e}"

    result = format_arrivals(stop, arrivals)

    # Append alternates hint if there were other plausible matches
    if alternates:
        alt_lines = [f"\n💡 Other stops that matched:"]
        for alt in alternates[:3]:
            alt_lines.append(f"  #{alt['code']} — {alt['name']} ({alt['desc']})")
        result += "\n" + "\n".join(alt_lines)

    return result


# ─── Tool 2: Search stops ──────────────────────────────────────────────────────

class FindStopsInput(BaseModel):
    model_config = ConfigDict(str_strip_whitespace=True, extra="forbid")

    query: str = Field(
        ...,
        description="Intersection, street name, or landmark to search for stops near.",
        min_length=2,
        max_length=200,
    )
    limit: int = Field(
        default=5,
        description="Max stops to return (1–10).",
        ge=1,
        le=10,
    )


@mcp.tool(
    name="translink_find_stops",
    annotations={
        "title": "Find TransLink Bus Stops",
        "readOnlyHint": True,
        "destructiveHint": False,
        "idempotentHint": True,
        "openWorldHint": False,
    },
)
async def translink_find_stops(params: FindStopsInput) -> str:
    """
    Search for TransLink bus stops by name, intersection, or landmark.
    Returns stop codes, names, and descriptions — useful for finding the right
    stop number before querying arrivals.

    Args:
        params (FindStopsInput):
            - query (str): Search string
            - limit (int): Max results (default 5)

    Returns:
        str: List of matching stops with stop code, name, description, and coordinates.
    """
    await load_static_gtfs()

    matches = find_stops_fuzzy(params.query, limit=params.limit)
    if not matches:
        return f"No stops found matching '{params.query}'."

    lines = [f"Stops matching '{params.query}':\n"]
    for s in matches:
        desc = f" — {s['desc']}" if s["desc"] else ""
        lines.append(f"  #{s['code']}  {s['name']}{desc}")

    lines.append("\nUse the stop number with translink_get_bus_times to get live arrivals.")
    return "\n".join(lines)


# ─── Tool 3: Service alerts ────────────────────────────────────────────────────

@mcp.tool(
    name="translink_get_alerts",
    annotations={
        "title": "Get TransLink Service Alerts",
        "readOnlyHint": True,
        "destructiveHint": False,
        "idempotentHint": False,
        "openWorldHint": True,
    },
)
async def translink_get_alerts(route: Optional[str] = None) -> str:
    """
    Get current TransLink service alerts. Optionally filter by route number.

    Args:
        route (Optional[str]): Route number to filter by (e.g. '253', 'R2'). If omitted, returns all active alerts.

    Returns:
        str: Active service alerts with affected routes and descriptions.
    """
    from google.transit import gtfs_realtime_pb2

    async with httpx.AsyncClient(timeout=15) as client:
        r = await client.get(
            "https://gtfs.translink.ca/v2/gtfsrealtimealerts",
            params={"apikey": API_KEY},
        )
        r.raise_for_status()

    feed = gtfs_realtime_pb2.FeedMessage()
    feed.ParseFromString(r.content)

    alerts = []
    for entity in feed.entity:
        if not entity.HasField("alert"):
            continue
        alert = entity.alert

        # Filter by route if requested
        if route:
            route_ids = [
                ie.route_id for ie in alert.informed_entity if ie.route_id
            ]
            route_names = [cache.routes.get(rid, rid) for rid in route_ids]
            if route.upper() not in [r.upper() for r in route_names]:
                continue

        header = next(
            (t.translation[0].text for t in [alert.header_text] if t.translation),
            "No header"
        )
        desc = next(
            (t.translation[0].text for t in [alert.description_text] if t.translation),
            ""
        )
        affected = [
            cache.routes.get(ie.route_id, ie.route_id)
            for ie in alert.informed_entity
            if ie.route_id
        ]
        alerts.append({"header": header, "desc": desc, "affected": list(set(affected))})

    if not alerts:
        suffix = f" for route {route}" if route else ""
        return f"No active service alerts{suffix}."

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
    uvicorn.run(mcp.streamable_http_app(), host="0.0.0.0", port=int(os.environ.get("PORT", 8000)))