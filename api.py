import os
import asyncio
import re
from typing import List, Optional
from urllib.parse import urlparse
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

# Base URL for the adsapi.py service (adjust port/host as needed)
ADS_API_URL = "https://lead-ads-api.onrender.com"

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
    has_google_ads_tag: str = "No"
    running_google_ads: str = "No"
    has_facebook_tag: str = "No"

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


def extract_domain(url: Optional[str]) -> Optional[str]:
    """
    Parse out the bare domain from a URL, stripping www. prefix.
    e.g. "https://www.example.com/page" → "example.com"
    """
    if not url:
        return None
    try:
        parsed = urlparse(url if url.startswith("http") else f"https://{url}")
        host = parsed.netloc or parsed.path  # fallback if scheme was missing
        host = host.split(":")[0]            # strip port if present
        if host.startswith("www."):
            host = host[4:]
        return host.lower() if host else None
    except Exception:
        return None


# ── ADS DETECTION ───────────────────────────────────
# Patterns are compiled ONCE at import time into a single alternation regex
# per role — the engine performs exactly ONE pass instead of N separate searches.

# Google Ads: <script async src="…"> CDN URL signatures
_G_SRC = re.compile(
    r"googletagmanager\.com/gtag/js\?id=AW-\d+",
    re.IGNORECASE,
)

# Google Ads: inline <script> body
_G_INLINE = re.compile(
    r"AW-\d+",
    re.IGNORECASE,
)

# Facebook Pixel: <script async src="…"> CDN URL
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

    for tag in soup.find_all("script"):
        src = tag.get("src") or ""
        if src:
            if not g  and _G_SRC.search(src):  g  = True
            if not fb and _FB_SRC.search(src): fb = True

        if not g or not fb:
            body = tag.string or ""
            if body:
                if not g  and _G_INLINE.search(body):  g  = True
                if not fb and _FB_INLINE.search(body): fb = True

        if g and fb:
            break

    return g, fb


async def check_active_google_ads(domain: str, client: httpx.AsyncClient) -> bool:
    """
    Hit adsapi.py's /check_ads_status to confirm the domain is actively
    running Google Ads in the Ads Transparency centre.
    Returns True if active ads are found, False otherwise (including on errors).
    """
    try:
        resp = await client.post(
            f"{ADS_API_URL}/check_ads_status",
            json={"domain": domain, "region": "AU"},
            timeout=30,          # Playwright needs extra time
        )
        if resp.status_code == 200:
            return resp.json().get("has_ads", "no").lower() == "yes"
    except Exception:
        pass
    return False


async def detect_ads(website, client):
    # Returns (has_google_ads_tag, running_google_ads, has_facebook_tag)
    # has_google_ads_tag  — tag/AW- ID present on the page
    # running_google_ads  — tag present AND adsapi confirms active ads
    # has_facebook_tag    — Facebook Pixel present on the page
    if not website:
        return "No", "No", "No"

    url = website if website.startswith("http") else f"https://{website}"

    try:
        resp = await client.get(
            url,
            headers=SCRAPER_HEADERS,
            timeout=10,
            follow_redirects=True,
        )
        raw = resp.content[:_MAX_HTML_BYTES].decode("utf-8", errors="replace")
    except Exception:
        return "No", "No", "No"

    soup = BeautifulSoup(raw, "html.parser")

    # Layer 1: structured tag walk (Tag Assistant style)
    g, fb = _detect_from_script_tags(soup)

    # Layer 2: raw-text fallback (handles GTM-injected / obfuscated / lazy scripts)
    if not g:  g  = bool(_G_SRC.search(raw)  or _G_INLINE.search(raw))
    if not fb: fb = bool(_FB_SRC.search(raw) or _FB_INLINE.search(raw))

    del soup, raw

    # Only hit adsapi when the Google tag is actually present — skip otherwise
    running_google = "No"
    if g:
        domain = extract_domain(website)
        if domain:
            is_active = await check_active_google_ads(domain, client)
            running_google = "Yes" if is_active else "No"

    return ("Yes" if g else "No", running_google, "Yes" if fb else "No")


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

                has_g_tag, running_google, has_fb_tag = await detect_ads(website, client)

                all_results.append(Business(
                    name=p.get("name"),
                    business_type=btype,
                    lat=loc["lat"],
                    lng=loc["lng"],
                    phone=detail.get("formatted_phone_number"),
                    website=website,
                    maps_url=detail.get("url"),
                    has_google_ads_tag=has_g_tag,
                    running_google_ads=running_google,
                    has_facebook_tag=has_fb_tag,
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
            "has_google_ads_tag": b.has_google_ads_tag,
            "running_google_ads": b.running_google_ads,
            "has_facebook_tag": b.has_facebook_tag,
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

    headers = ["Name", "Type", "Phone", "Website", "Maps URL", "Has Google Ads Tag", "Running Google Ads", "Has Facebook Tag"]
    ws.append(headers)

    for b in data["rows"]:
        ws.append([
            b["name"],
            b["type"],
            b["phone"],
            b["website"],
            b["maps_url"],
            b["has_google_ads_tag"],
            b["running_google_ads"],
            b["has_facebook_tag"],
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