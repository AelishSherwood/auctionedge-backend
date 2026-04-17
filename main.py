"""
AuctionEdge Backend - FastAPI server for foreclosure/tax auction tracking.
Scrapes Kershaw SC, Buncombe NC, Madison NC, Yancey NC county sites.
"""

import asyncio
import json
import re
import time
from datetime import datetime, timedelta
from typing import Optional
from urllib.parse import urljoin

import httpx
from apscheduler.schedulers.asyncio import AsyncIOScheduler
from fastapi import BackgroundTasks, FastAPI, HTTPException, Query
from fastapi.middleware.cors import CORSMiddleware
from pydantic import BaseModel

# ─────────────────────────────────────────────
# App Setup
# ─────────────────────────────────────────────

app = FastAPI(title="AuctionEdge API", version="1.0.0")

app.add_middleware(
    CORSMiddleware,
    allow_origins=["http://localhost:3000", "http://localhost:5173"],
    allow_credentials=True,
    allow_methods=["*"],
    allow_headers=["*"],
)

# In-memory store (replace with PostgreSQL/SQLite for production)
property_store: list[dict] = []
last_refresh: Optional[datetime] = None
scheduler = AsyncIOScheduler()

POLITE_DELAY = 2.0  # seconds between requests

HEADERS = {
    "User-Agent": (
        "AuctionEdge/1.0 (Real estate research tool; contact: your@email.com)"
    )
}


# ─────────────────────────────────────────────
# Models
# ─────────────────────────────────────────────


class DealKitchenPayload(BaseModel):
    property_id: str
    arv: float
    repair_costs: float
    holding_costs: float
    desired_profit: float
    closing_costs: float = 5000.0


class PropertyUpdate(BaseModel):
    notes: Optional[str] = None
    starred: Optional[bool] = None


# ─────────────────────────────────────────────
# Geocoding (OpenStreetMap Nominatim - free, no key needed)
# ─────────────────────────────────────────────


async def geocode_address(address: str, client: httpx.AsyncClient) -> tuple[float, float] | None:
    """Geocode an address using Nominatim (OpenStreetMap). Politely rate-limited."""
    try:
        await asyncio.sleep(1.1)  # Nominatim requires 1 req/sec max
        resp = await client.get(
            "https://nominatim.openstreetmap.org/search",
            params={"q": address, "format": "json", "limit": 1},
            headers={**HEADERS, "Accept-Language": "en-US"},
            timeout=10,
        )
        data = resp.json()
        if data:
            return float(data[0]["lat"]), float(data[0]["lon"])
    except Exception as e:
        print(f"Geocode failed for '{address}': {e}")
    return None


# ─────────────────────────────────────────────
# Street View URL Builder
# ─────────────────────────────────────────────


def street_view_url(address: str, api_key: str = "") -> str:
    """
    Build a Google Street View Static API URL.
    Substitute your real API key in .env for production.
    Without a key, returns a placeholder Zillow search link.
    """
    if api_key:
        encoded = address.replace(" ", "+")
        return (
            f"https://maps.googleapis.com/maps/api/streetview"
            f"?size=600x400&location={encoded}&key={api_key}"
        )
    # Fallback: Zillow search link (opens in browser)
    encoded = address.replace(" ", "-").replace(",", "")
    return f"https://www.zillow.com/homes/{encoded}_rb/"


# ─────────────────────────────────────────────
# Scraper: Kershaw County SC
# ─────────────────────────────────────────────


async def scrape_kershaw(client: httpx.AsyncClient) -> list[dict]:
    """
    Kershaw County SC - Magistrate Court Public Sales page.
    Finds the most recent monthly PDF and parses listings.
    Requires: pdfplumber  (pip install pdfplumber)
    """
    properties = []
    base_url = "https://www.kershaw.sc.gov/government/departments-h-q/magistrate-court/public-sales"

    try:
        await asyncio.sleep(POLITE_DELAY)
        resp = await client.get(base_url, headers=HEADERS, timeout=15, follow_redirects=True)
        resp.raise_for_status()

        # Find PDF links on the page
        pdf_links = re.findall(r'href=["\']([^"\']*\.pdf[^"\']*)["\']', resp.text, re.IGNORECASE)
        if not pdf_links:
            print("Kershaw: No PDFs found on page")
            return []

        # Take the first (most recent) PDF
        pdf_url = pdf_links[0]
        if not pdf_url.startswith("http"):
            pdf_url = urljoin(base_url, pdf_url)

        await asyncio.sleep(POLITE_DELAY)
        pdf_resp = await client.get(pdf_url, headers=HEADERS, timeout=30)
        pdf_resp.raise_for_status()

        # Parse PDF with pdfplumber
        try:
            import io
            import pdfplumber

            with pdfplumber.open(io.BytesIO(pdf_resp.content)) as pdf:
                full_text = "\n".join(page.extract_text() or "" for page in pdf.pages)

            # Parse listings — adapt regex to actual PDF format
            # Common format: Case No | Address | Sale Date | Opening Bid
            lines = [l.strip() for l in full_text.split("\n") if l.strip()]
            for i, line in enumerate(lines):
                # Look for case numbers (e.g., 2024-CP-28-XXXX or similar)
                case_match = re.search(r'\d{4}-[A-Z]{2}-\d{2}-\d+', line)
                if case_match:
                    case_num = case_match.group()
                    # Try to extract bid amount
                    bid_match = re.search(r'\$[\d,]+', line)
                    opening_bid = bid_match.group() if bid_match else "See listing"

                    # Context lines for address
                    context = " ".join(lines[max(0, i-1):i+2])
                    addr_match = re.search(r'\d+\s+[A-Z][a-zA-Z\s]+(?:Rd|St|Ave|Dr|Ln|Blvd|Hwy|Way)', context)
                    address = addr_match.group() + ", Kershaw County, SC" if addr_match else f"See case {case_num}"

                    properties.append({
                        "id": f"kershaw-{case_num}",
                        "county": "Kershaw County",
                        "state": "SC",
                        "case_number": case_num,
                        "address": address,
                        "sale_date": extract_date_from_text(context),
                        "opening_bid": opening_bid,
                        "property_type": "Foreclosure",
                        "source_url": pdf_url,
                        "source": "Kershaw SC Magistrate Court",
                        "scraped_at": datetime.now().isoformat(),
                        "lat": None,
                        "lng": None,
                        "photo_url": None,
                        "starred": False,
                        "notes": "",
                    })
        except ImportError:
            print("Install pdfplumber: pip install pdfplumber")
        except Exception as e:
            print(f"Kershaw PDF parse error: {e}")

    except Exception as e:
        print(f"Kershaw scrape error: {e}")

    return properties


# ─────────────────────────────────────────────
# Scraper: Buncombe County NC
# ─────────────────────────────────────────────


async def scrape_buncombe(client: httpx.AsyncClient) -> list[dict]:
    """
    Buncombe County NC - Tax Foreclosure listing page (interactive app).
    The link-app at buncombenc.gov loads data via an internal API — we fetch the HTML
    and look for embedded JSON data or table rows.
    """
    properties = []
    url = "https://buncombenc.gov/link-app-tax-foreclosures"

    try:
        await asyncio.sleep(POLITE_DELAY)
        resp = await client.get(url, headers=HEADERS, timeout=15, follow_redirects=True)
        resp.raise_for_status()
        html = resp.text

        # Attempt 1: Look for JSON data embedded in page
        json_match = re.search(r'var\s+listings\s*=\s*(\[.*?\]);', html, re.DOTALL)
        if json_match:
            try:
                listings = json.loads(json_match.group(1))
                for item in listings:
                    properties.append(_normalize_buncombe_item(item))
                return properties
            except Exception:
                pass

        # Attempt 2: Parse HTML table rows (BeautifulSoup)
        try:
            from bs4 import BeautifulSoup
            soup = BeautifulSoup(html, "html.parser")
            rows = soup.select("table tr")[1:]  # skip header
            for row in rows:
                cells = [td.get_text(strip=True) for td in row.find_all("td")]
                if len(cells) >= 3:
                    properties.append({
                        "id": f"buncombe-{cells[0].replace(' ', '-')}",
                        "county": "Buncombe County",
                        "state": "NC",
                        "case_number": cells[0] if cells else "N/A",
                        "address": f"{cells[1]}, Buncombe County, NC" if len(cells) > 1 else "N/A",
                        "sale_date": cells[2] if len(cells) > 2 else "TBD",
                        "opening_bid": cells[3] if len(cells) > 3 else "See listing",
                        "property_type": "Tax Foreclosure",
                        "source_url": url,
                        "source": "Buncombe County NC",
                        "scraped_at": datetime.now().isoformat(),
                        "lat": None,
                        "lng": None,
                        "photo_url": None,
                        "starred": False,
                        "notes": "",
                    })
        except ImportError:
            print("Install beautifulsoup4: pip install beautifulsoup4")
        except Exception as e:
            print(f"Buncombe HTML parse error: {e}")

    except Exception as e:
        print(f"Buncombe scrape error: {e}")

    return properties


def _normalize_buncombe_item(item: dict) -> dict:
    return {
        "id": f"buncombe-{item.get('caseNumber', item.get('id', 'unknown'))}",
        "county": "Buncombe County",
        "state": "NC",
        "case_number": item.get("caseNumber", "N/A"),
        "address": f"{item.get('address', 'N/A')}, Buncombe County, NC",
        "sale_date": item.get("saleDate", "TBD"),
        "opening_bid": item.get("openingBid", item.get("bid", "See listing")),
        "property_type": "Tax Foreclosure",
        "source_url": "https://buncombenc.gov/link-app-tax-foreclosures",
        "source": "Buncombe County NC",
        "scraped_at": datetime.now().isoformat(),
        "lat": None,
        "lng": None,
        "photo_url": None,
        "starred": False,
        "notes": "",
    }


# ─────────────────────────────────────────────
# Scraper: Kania Law Firm (Madison + Yancey NC)
# ─────────────────────────────────────────────


async def scrape_kania(client: httpx.AsyncClient) -> list[dict]:
    """
    Kania Law Firm foreclosure listings - covers Madison and Yancey NC.
    Parses the HTML table of active foreclosure notices.
    """
    properties = []
    url = "https://kanialawfirm.com/tax-foreclosures/foreclosure-listings/"
    target_counties = {"madison", "yancey"}

    try:
        await asyncio.sleep(POLITE_DELAY)
        resp = await client.get(url, headers=HEADERS, timeout=15, follow_redirects=True)
        resp.raise_for_status()

        from bs4 import BeautifulSoup
        soup = BeautifulSoup(resp.text, "html.parser")

        # Kania typically has a table with County | Case No | Address | Sale Date | Bid
        rows = soup.select("table tbody tr")
        for row in rows:
            cells = [td.get_text(strip=True) for td in row.find_all("td")]
            if not cells:
                continue

            county_text = cells[0].lower() if cells else ""
            if not any(tc in county_text for tc in target_counties):
                continue  # Skip counties we don't track

            county_display = cells[0] if cells else "NC County"
            case_num = cells[1] if len(cells) > 1 else "N/A"
            raw_addr = cells[2] if len(cells) > 2 else "N/A"
            sale_date = cells[3] if len(cells) > 3 else "TBD"
            bid = cells[4] if len(cells) > 4 else "See listing"
            full_address = f"{raw_addr}, {county_display} County, NC"

            properties.append({
                "id": f"kania-{case_num.replace(' ', '-').replace('/', '-')}",
                "county": f"{county_display} County",
                "state": "NC",
                "case_number": case_num,
                "address": full_address,
                "sale_date": sale_date,
                "opening_bid": bid,
                "property_type": "Tax Foreclosure",
                "source_url": url,
                "source": "Kania Law Firm",
                "scraped_at": datetime.now().isoformat(),
                "lat": None,
                "lng": None,
                "photo_url": None,
                "starred": False,
                "notes": "",
            })

    except ImportError:
        print("Install beautifulsoup4: pip install beautifulsoup4")
    except Exception as e:
        print(f"Kania scrape error: {e}")

    return properties


# ─────────────────────────────────────────────
# Scraper: Madison County NC (official site)
# ─────────────────────────────────────────────


async def scrape_madison(client: httpx.AsyncClient) -> list[dict]:
    """
    Madison County NC official site - check for foreclosure/tax sale notices.
    URL may need updating as the county site structure changes.
    """
    properties = []
    candidates = [
        "https://www.madisoncountync.gov/tax-foreclosures",
        "https://www.madisoncountync.gov/foreclosures",
        "https://www.madisoncountync.gov/departments/tax",
    ]

    for url in candidates:
        try:
            await asyncio.sleep(POLITE_DELAY)
            resp = await client.get(url, headers=HEADERS, timeout=10, follow_redirects=True)
            if resp.status_code == 200:
                from bs4 import BeautifulSoup
                soup = BeautifulSoup(resp.text, "html.parser")
                # Look for sale/auction related links or tables
                links = soup.find_all("a", string=re.compile(r"foreclos|auction|tax sale", re.I))
                for link in links[:5]:
                    href = link.get("href", "")
                    if href and not href.startswith("http"):
                        href = urljoin(url, href)
                    # Add as a placeholder — requires deeper parsing per actual page
                    print(f"Madison: Found related link: {href}")
                break
        except Exception:
            continue

    return properties  # Returns Kania results for Madison via scrape_kania()


# ─────────────────────────────────────────────
# Helpers
# ─────────────────────────────────────────────


def extract_date_from_text(text: str) -> str:
    """Extract a date string from free text."""
    patterns = [
        r'\b(?:January|February|March|April|May|June|July|August|September|October|November|December)\s+\d{1,2},?\s+\d{4}\b',
        r'\b\d{1,2}/\d{1,2}/\d{4}\b',
        r'\b\d{4}-\d{2}-\d{2}\b',
    ]
    for pattern in patterns:
        match = re.search(pattern, text, re.IGNORECASE)
        if match:
            return match.group()
    return "TBD"


def deduplicate(properties: list[dict]) -> list[dict]:
    seen = set()
    out = []
    for p in properties:
        key = (p.get("case_number"), p.get("county"))
        if key not in seen:
            seen.add(key)
            out.append(p)
    return out


# ─────────────────────────────────────────────
# Core refresh logic
# ─────────────────────────────────────────────


async def refresh_all_listings():
    """Scrape all counties and geocode new addresses."""
    global property_store, last_refresh
    print(f"[{datetime.now()}] Starting full refresh...")

    async with httpx.AsyncClient() as client:
        results = await asyncio.gather(
            scrape_kershaw(client),
            scrape_buncombe(client),
            scrape_kania(client),
            scrape_madison(client),
            return_exceptions=True,
        )

    all_props = []
    for r in results:
        if isinstance(r, list):
            all_props.extend(r)
        elif isinstance(r, Exception):
            print(f"Scraper error: {r}")

    all_props = deduplicate(all_props)

    # Geocode addresses without coordinates
    async with httpx.AsyncClient() as client:
        for prop in all_props:
            if prop["lat"] is None and prop["address"] != "N/A":
                coords = await geocode_address(prop["address"], client)
                if coords:
                    prop["lat"], prop["lng"] = coords
            # Build photo URL (Street View or Zillow fallback)
            if not prop.get("photo_url") and prop["address"] != "N/A":
                prop["photo_url"] = street_view_url(prop["address"])

    property_store = all_props
    last_refresh = datetime.now()
    print(f"[{last_refresh}] Refresh complete. {len(all_props)} listings loaded.")


# ─────────────────────────────────────────────
# API Routes
# ─────────────────────────────────────────────


@app.on_event("startup")
async def startup():
    # Schedule refresh every 6 hours
    scheduler.add_job(refresh_all_listings, "interval", hours=6, next_run_time=datetime.now())
    scheduler.start()


@app.on_event("shutdown")
async def shutdown():
    scheduler.shutdown()


@app.get("/api/properties")
async def get_properties(
    county: Optional[str] = Query(None),
    state: Optional[str] = Query(None),
    property_type: Optional[str] = Query(None),
    min_bid: Optional[float] = Query(None),
    max_bid: Optional[float] = Query(None),
    starred: Optional[bool] = Query(None),
    sort_by: str = Query("sale_date"),
    sort_dir: str = Query("asc"),
):
    """Return filtered property listings."""
    props = list(property_store)

    if county:
        props = [p for p in props if county.lower() in p.get("county", "").lower()]
    if state:
        props = [p for p in props if p.get("state", "").upper() == state.upper()]
    if property_type:
        props = [p for p in props if property_type.lower() in p.get("property_type", "").lower()]
    if starred is not None:
        props = [p for p in props if p.get("starred") == starred]
    if min_bid is not None:
        props = [p for p in props if _parse_bid(p.get("opening_bid", "0")) >= min_bid]
    if max_bid is not None:
        props = [p for p in props if _parse_bid(p.get("opening_bid", "0")) <= max_bid]

    reverse = sort_dir.lower() == "desc"
    props.sort(key=lambda p: str(p.get(sort_by, "")), reverse=reverse)
    return {"count": len(props), "last_refresh": last_refresh, "properties": props}


@app.get("/api/properties/{property_id}")
async def get_property(property_id: str):
    for p in property_store:
        if p["id"] == property_id:
            return p
    raise HTTPException(404, "Property not found")


@app.patch("/api/properties/{property_id}")
async def update_property(property_id: str, update: PropertyUpdate):
    for p in property_store:
        if p["id"] == property_id:
            if update.notes is not None:
                p["notes"] = update.notes
            if update.starred is not None:
                p["starred"] = update.starred
            return p
    raise HTTPException(404, "Property not found")


@app.post("/api/deal-kitchen")
async def send_to_deal_kitchen(payload: DealKitchenPayload):
    """
    Calculate max offer and return Deal Kitchen integration payload.
    Max Offer = ARV - Repairs - Holding - Closing - Desired Profit
    """
    prop = next((p for p in property_store if p["id"] == payload.property_id), None)
    if not prop:
        raise HTTPException(404, "Property not found")

    max_offer = (
        payload.arv
        - payload.repair_costs
        - payload.holding_costs
        - payload.closing_costs
        - payload.desired_profit
    )

    return {
        "property": prop,
        "analysis": {
            "arv": payload.arv,
            "repair_costs": payload.repair_costs,
            "holding_costs": payload.holding_costs,
            "closing_costs": payload.closing_costs,
            "desired_profit": payload.desired_profit,
            "max_offer": max(0, max_offer),
            "opening_bid": _parse_bid(prop.get("opening_bid", "0")),
            "deal_score": _score_deal(max_offer, _parse_bid(prop.get("opening_bid", "0"))),
        },
    }


@app.post("/api/refresh")
async def trigger_refresh(background_tasks: BackgroundTasks):
    """Manually trigger a data refresh."""
    background_tasks.add_task(refresh_all_listings)
    return {"message": "Refresh started", "started_at": datetime.now().isoformat()}


@app.get("/api/stats")
async def get_stats():
    counties = {}
    for p in property_store:
        c = p.get("county", "Unknown")
        counties[c] = counties.get(c, 0) + 1
    return {
        "total": len(property_store),
        "last_refresh": last_refresh,
        "next_refresh": (last_refresh + timedelta(hours=6)) if last_refresh else None,
        "by_county": counties,
        "starred": sum(1 for p in property_store if p.get("starred")),
    }


# ─────────────────────────────────────────────
# Helpers
# ─────────────────────────────────────────────


def _parse_bid(bid_str: str) -> float:
    try:
        cleaned = re.sub(r"[^\d.]", "", str(bid_str))
        return float(cleaned) if cleaned else 0.0
    except Exception:
        return 0.0


def _score_deal(max_offer: float, opening_bid: float) -> str:
    if opening_bid <= 0:
        return "unknown"
    if max_offer > opening_bid * 1.3:
        return "excellent"
    elif max_offer > opening_bid * 1.1:
        return "good"
    elif max_offer > opening_bid:
        return "marginal"
    return "over-bid"


if __name__ == "__main__":
    import uvicorn
    uvicorn.run("main:app", host="0.0.0.0", port=8000, reload=True)
