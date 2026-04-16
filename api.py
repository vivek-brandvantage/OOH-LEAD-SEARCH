import os
import asyncio
import re
from typing import List, Optional
import io
from fastapi.responses import StreamingResponse
from fastapi.middleware.cors import CORSMiddleware
import openpyxl
import dotenv

import httpx
from bs4 import BeautifulSoup
from fastapi import FastAPI, HTTPException
from pydantic import BaseModel

# ── ENV ─────────────────────────────────────────────
dotenv.load_dotenv()
GOOGLE_API_KEY = os.getenv("GOOGLE_MAPS_API_KEY", "")

PLACES_URL = "https://maps.googleapis.com/maps/api/place/nearbysearch/json"
DETAILS_URL = "https://maps.googleapis.com/maps/api/place/details/json"
GEOCODE_URL = "https://maps.googleapis.com/maps/api/geocode/json"

# ── SCRAPER HEADERS ─────────────────────────────────
SCRAPER_HEADERS = {
    "User-Agent": (
        "Mozilla/5.0 (Windows NT 10.0; Win64; x64) "
        "AppleWebKit/537.36 (KHTML, like Gecko) "
        "Chrome/124.0.0.0 Safari/537.36"
    ),
    "Accept": "text/html,application/xhtml+xml,application/xml;q=0.9,*/*;q=0.8",
    "Accept-Language": "en-US,en;q=0.9",
}

# ── APP ─────────────────────────────────────────────
app = FastAPI(title="OOH Business Finder")

# origins=["https://ooh-frontend-lead.vercel.app/"]

# ── CORS ─────────────────────────────────────────────
app.add_middleware(
    CORSMiddleware,
    allow_origins=["*"],
    allow_credentials=True,
    allow_methods=["*"],
    allow_headers=["*"],
)

# ── MODELS ──────────────────────────────────────────
class SearchRequest(BaseModel):
    center_point: str
    business_types: List[str]
    radius_meters: float

class Business(BaseModel):
    name: str
    business_type: str
    lat: float
    lng: float
    phone: Optional[str]
    website: Optional[str]
    maps_url: Optional[str]
    running_google_ads: str = "No"
    running_facebook_ads: str = "No"

# ── WEBSITE VALIDATOR ────────────────────────────────
SOCIAL_DOMAINS = [
    "facebook.com",
    "instagram.com",
    "x.com",
    "tiktok.com",
]

def validate_website(url: Optional[str]) -> Optional[str]:
    if not url:
        return None

    url = url.lower()

    if any(domain in url for domain in SOCIAL_DOMAINS):
        return None  # ignore social links

    if not url.startswith("http"):
        url = f"https://{url}"

    return url

# ── ADS DETECTION ───────────────────────────────────
# Patterns are compiled ONCE at import time into a single alternation regex
# per role — the engine performs exactly ONE pass instead of N separate searches.
# Tag Assistant works the same way: it checks <script src=""> against a known
# URL allowlist, then falls back to inline body inspection.

# ─ Pre-compiled combined regexes ────────────────────────────────────────────

# Google Ads: <script async src="…"> CDN URL signatures
_G_SRC = re.compile(
    r"googletagmanager\.com/(?:gtag/js|gtm\.js)"
    r"|pagead2\.googlesyndication\.com/pagead/js"
    r"|googleadservices\.com/pagead/conversion"
    r"|google-analytics\.com/(?:analytics|gtag)",
    re.IGNORECASE,
)

# Google Ads: inline <script> body (gtag config call or bare AW- / G- / GTM- ID)
_G_INLINE = re.compile(
    r"gtag\s*\(\s*['\"]config['\"]"
    r"|AW-\d+"
    r"|GTM-[A-Z0-9]+"
    r"|G-[A-Z0-9]+",
    re.IGNORECASE,
)

# Facebook Pixel: <script async src="…"> CDN URL
# One alternation covers both fbevents.js and all.js bundles, accommodating varying paths
_FB_SRC = re.compile(
    r"connect\.facebook\.net/.*?/(?:fbevents|all)\.js",
    re.IGNORECASE,
)

# Facebook Pixel: inline fbq('init',…) call or noscript pixel <img>
_FB_INLINE = re.compile(
    r"fbq\s*\(\s*['\"]init['\"]"
    r"|fbq\s*\(\s*['\"]track['\"]"
    r"|facebook\.com/tr\?id=",
    re.IGNORECASE,
)

# Only decode the first 150 KB of each page.
# Google/Facebook tags always live in <head> — the rest of the page is waste.
_MAX_HTML_BYTES = 150_000


def _detect_from_script_tags(soup: BeautifulSoup) -> tuple[bool, bool]:
    """
    Primary layer — Tag Assistant style:
    Walk every <script> tag once using a lazy generator.
    • src attribute  → checked with pre-compiled CDN regex (single pass).
    • inline body    → only decoded when at least one platform is still unresolved.
    Exits as soon as both platforms are confirmed.
    """
    g = fb = False

    for tag in soup.find_all("script"):        # generator, not a list
        src = tag.get("src") or ""
        if src:
            if not g  and _G_SRC.search(src):  g  = True
            if not fb and _FB_SRC.search(src): fb = True

        # Skip body decode when both already confirmed
        if not g or not fb:
            body = tag.string or ""
            if body:
                if not g  and _G_INLINE.search(body):  g  = True
                if not fb and _FB_INLINE.search(body): fb = True

        if g and fb:
            break   # early exit — no need to walk remaining tags

    return g, fb


async def detect_ads(website: Optional[str], client: httpx.AsyncClient) -> tuple[str, str]:
    if not website:
        return "No", "No"

    url = website if website.startswith("http") else f"https://{website}"

    try:
        resp = await client.get(
            url,
            headers=SCRAPER_HEADERS,
            timeout=10,
            follow_redirects=True,
        )
        # Slice raw bytes BEFORE decode — avoids allocating a multi-MB Python str.
        # Ad tags live in <head>; 150 KB captures all of them on any real site.
        raw = resp.content[:_MAX_HTML_BYTES].decode("utf-8", errors="replace")
    except Exception:
        return "No", "No"

    soup = BeautifulSoup(raw, "html.parser")

    # Layer 1: structured tag walk (Tag Assistant style)
    g, fb = _detect_from_script_tags(soup)

    # Layer 2: raw-text fallback (handles GTM-injected / obfuscated / lazy scripts)
    if not g:  g  = bool(_G_SRC.search(raw)  or _G_INLINE.search(raw))
    if not fb: fb = bool(_FB_SRC.search(raw) or _FB_INLINE.search(raw))

    # Explicit cleanup — frees the parsed tree and raw string before the
    # coroutine yields back to the event loop, keeping per-request heap low.
    del soup, raw

    return ("Yes" if g else "No", "Yes" if fb else "No")





# ── HELPERS ─────────────────────────────────────────
async def geocode(address: str):
    if not GOOGLE_API_KEY:
        raise HTTPException(500, "Missing API key")

    async with httpx.AsyncClient() as client:
        r = await client.get(GEOCODE_URL, params={
            "address": address,
            "key": GOOGLE_API_KEY
        })
        data = r.json()

    if data["status"] != "OK":
        raise HTTPException(400, "Geocoding failed")

    loc = data["results"][0]["geometry"]["location"]
    return loc["lat"], loc["lng"]

async def get_details(place_id: str, client):
    try:
        r = await client.get(DETAILS_URL, params={
            "place_id": place_id,
            "fields": "formatted_phone_number,website,url",
            "key": GOOGLE_API_KEY
        })
        data = r.json()
        if data["status"] == "OK":
            return data["result"]
    except:
        pass
    return {}

async def search_places(lat, lng, radius, keyword):
    results = []
    params = {
        "location": f"{lat},{lng}",
        "radius": int(radius),
        "keyword": keyword,
        "key": GOOGLE_API_KEY
    }

    async with httpx.AsyncClient() as client:
        while True:
            r = await client.get(PLACES_URL, params=params)
            data = r.json()

            if data["status"] not in ["OK", "ZERO_RESULTS"]:
                break

            results.extend(data.get("results", []))

            token = data.get("next_page_token")
            if not token:
                break

            await asyncio.sleep(2)
            params = {"pagetoken": token, "key": GOOGLE_API_KEY}

    return results

# ── HEALTH ──────────────────────────────────────────
@app.get("/health")
def health():
    return {"status": "ok"}

# ── SEARCH ENDPOINT ─────────────────────────────────
@app.post("/search_sheets")
async def search_sheets(req: SearchRequest):
    lat, lng = await geocode(req.center_point)

    seen = set()
    all_results = []

    async with httpx.AsyncClient() as client:
        for btype in req.business_types:
            places = await search_places(lat, lng, req.radius_meters, btype)

            for p in places:
                if p["place_id"] in seen:
                    continue
                seen.add(p["place_id"])

                loc = p["geometry"]["location"]
                detail = await get_details(p["place_id"], client)

                raw_website = detail.get("website")
                website = validate_website(raw_website)

                google_ads, facebook_ads = await detect_ads(website, client)

                all_results.append(Business(
                    name=p.get("name"),
                    business_type=btype,
                    lat=loc["lat"],
                    lng=loc["lng"],
                    phone=detail.get("formatted_phone_number"),
                    website=website,
                    maps_url=detail.get("url"),
                    running_google_ads=google_ads,
                    running_facebook_ads=facebook_ads,
                ))

    all_results.sort(key=lambda x: x.name)

    rows = []
    for b in all_results:
        rows.append({
            "name": b.name,
            "type": b.business_type,
            "phone": b.phone,
            "website": b.website,
            "maps_url": b.maps_url,
            "running_google_ads": b.running_google_ads,
            "running_facebook_ads": b.running_facebook_ads,
        })

    return {
        "total": len(rows),
        "rows": rows
    }

# ── EXCEL DOWNLOAD ──────────────────────────────────
@app.post("/excel_download")
async def excel_download(req: SearchRequest):
    data = await search_sheets(req)

    wb = openpyxl.Workbook()
    ws = wb.active
    ws.title = "Businesses"

    headers = ["Name", "Type", "Phone", "Website", "Maps URL", "Running Google Ads", "Running Facebook Ads"]
    ws.append(headers)

    for b in data["rows"]:
        ws.append([
            b["name"],
            b["type"],
            b["phone"],
            b["website"],
            b["maps_url"],
            b["running_google_ads"],
            b["running_facebook_ads"],
        ])

    buffer = io.BytesIO()
    wb.save(buffer)
    buffer.seek(0)

    return StreamingResponse(
        buffer,
        media_type="application/vnd.openxmlformats-officedocument.spreadsheetml.sheet",
        headers={"Content-Disposition": "attachment; filename=businesses.xlsx"}
    )

# ── MAP ENDPOINT ────────────────────────────────────
@app.post("/map")
async def map_view(req: SearchRequest):
    lat, lng = await geocode(req.center_point)

    seen = set()
    results = []

    async with httpx.AsyncClient() as client:
        for btype in req.business_types:
            places = await search_places(lat, lng, req.radius_meters, btype)

            for p in places:
                if p["place_id"] in seen:
                    continue
                seen.add(p["place_id"])

                loc = p["geometry"]["location"]
                results.append({
                    "name": p.get("name"),
                    "lat": loc["lat"],
                    "lng": loc["lng"]
                })

    return {
        "center": {
            "lat": lat,
            "lng": lng
        },
        "radius": req.radius_meters,
        "rows": results
    }