import math
import os
import asyncio
from typing import List, Optional

import httpx
from fastapi import FastAPI, HTTPException
from pydantic import BaseModel

# ── ENV ─────────────────────────────────────────────
GOOGLE_API_KEY = os.getenv("GOOGLE_MAPS_API_KEY", "")

PLACES_URL = "https://maps.googleapis.com/maps/api/place/nearbysearch/json"
DETAILS_URL = "https://maps.googleapis.com/maps/api/place/details/json"
GEOCODE_URL = "https://maps.googleapis.com/maps/api/geocode/json"

# ── APP ─────────────────────────────────────────────
app = FastAPI(title="OOH Business Finder (Sheets Ready)")

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
    rating: Optional[float]
    reviews: Optional[int]
    phone: Optional[str]
    website: Optional[str]
    maps_url: Optional[str]

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


# ── MAIN ENDPOINT ───────────────────────────────────
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

                all_results.append(Business(
                    name=p.get("name"),
                    business_type=btype,
                    lat=loc["lat"],
                    lng=loc["lng"],
                    rating=p.get("rating"),
                    reviews=p.get("user_ratings_total"),
                    phone=detail.get("formatted_phone_number"),
                    website=detail.get("website"),
                    maps_url=detail.get("url"),
                ))

    # 🔥 sort + limit
    all_results.sort(key=lambda x: x.rating or 0, reverse=True)
    all_results = all_results[:30]

    # 🔥 convert to sheet-friendly rows
    rows = []
    for b in all_results:
        rows.append({
            "name": b.name,
            "type": b.business_type,
            "lat": b.lat,
            "lng": b.lng,
            "rating": b.rating,
            "reviews": b.reviews,
            "phone": b.phone,
            "website": b.website,
            "maps_url": b.maps_url
        })

    return {
        "total": len(rows),
        "rows": rows
    }


# ── HEALTH ──────────────────────────────────────────
@app.get("/health")
def health():
    return {"status": "ok"}