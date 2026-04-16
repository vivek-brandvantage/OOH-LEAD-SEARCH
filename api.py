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

# ── SCRAPER HEADERS (mimic a real browser so sites don't block us) ───────────
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
app = FastAPI(title="OOH Business Finder (Sheets Ready)")

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

# ── ADS DETECTION ────────────────────────────────────────────────────────────
#
# Strategy: fetch only the raw HTML (no JS execution), parse the <head> with
# BeautifulSoup, then regex-scan every <script> src and inline content.
#
# Google Ads signals (any one = Yes):
#   • googleads.g.doubleclick.net          ← conversion pixel (strongest signal)
#   • gtag/js?id=AW-XXXXXXXXX              ← Google Ads tag via gtag
#   • googleadservices.com                 ← ad serving domain
#   • gtag("config", "AW-...)              ← inline gtag config for Ads
#
# Facebook Ads signals (any one = Yes):
#   • connect.facebook.net/fbevents.js     ← FB pixel script src
#   • fbq("init", "...")                   ← inline pixel init call
#   • facebook.com/tr?id=                  ← noscript img pixel fallback

GOOGLE_ADS_PATTERNS = [
    r"googleads\.g\.doubleclick\.net",          # conversion pixel src
    r"googletagmanager\.com/gtag/js\?id=AW-",   # AW- tag via GTM
    r"googleadservices\.com",                    # ad serving domain
    r"""gtag\s*\(\s*['"]config['"]\s*,\s*['"]AW-""",  # inline gtag config
]

FACEBOOK_ADS_PATTERNS = [
    r"connect\.facebook\.net/[^/]+/fbevents\.js",  # pixel script src
    r"""fbq\s*\(\s*['"]init['"]""",                 # inline fbq init
    r"facebook\.com/tr\?id=",                       # noscript img pixel
]

async def detect_ads(website: Optional[str], client: httpx.AsyncClient) -> tuple[str, str]:
    """
    Fetches the website's raw HTML, parses only the <head> section with
    BeautifulSoup, and checks for Google Ads / Facebook Ads signals.

    Returns: ("Yes"/"No", "Yes"/"No")  →  (google_ads, facebook_ads)
    """
    if not website:
        return "No", "No"

    # Normalize — ensure scheme is present
    url = website if website.startswith("http") else f"https://{website}"

    try:
        resp = await client.get(
            url,
            headers=SCRAPER_HEADERS,
            timeout=10,
            follow_redirects=True,
        )
        html = resp.text
    except Exception:
        # Network error, timeout, SSL issue — skip silently
        return "No", "No"

    # Parse only the <head> tag to keep things fast; fall back to full HTML
    soup = BeautifulSoup(html, "html.parser")
    head = soup.find("head")
    head_html = str(head) if head else html  # raw string for regex scanning

    google_ads = _match_any(head_html, GOOGLE_ADS_PATTERNS)
    facebook_ads = _match_any(head_html, FACEBOOK_ADS_PATTERNS)

    return (
        "Yes" if google_ads else "No",
        "Yes" if facebook_ads else "No",
    )


def _match_any(text: str, patterns: list[str]) -> bool:
    return any(re.search(pat, text, re.IGNORECASE) for pat in patterns)


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

    # One shared client for Places details + ads scraping (connection pooling)
    async with httpx.AsyncClient() as client:
        for btype in req.business_types:
            places = await search_places(lat, lng, req.radius_meters, btype)

            for p in places:
                if p["place_id"] in seen:
                    continue
                seen.add(p["place_id"])

                loc = p["geometry"]["location"]
                detail = await get_details(p["place_id"], client)
                website = detail.get("website")

                # ── Ads detection (async, non-blocking) ──────────────────────
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