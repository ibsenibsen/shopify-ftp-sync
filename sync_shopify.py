"""
Shopify FTP Sync – daglig opdatering af priser og lager
Første kørsel: opretter produkter. Efterfølgende: opdaterer kun pris + lager.

Miljøvariabler (sættes som GitHub Secrets):
  FTP_HOST, FTP_USER, FTP_PASS, FTP_PATH
  SHOPIFY_STORE       (f.eks. din-butik.myshopify.com)
  SHOPIFY_API_KEY     (Admin API access token)
  TEST_MODE           (sæt til "true" for kun at behandle 10 produkter)
"""

import os
import csv
import time
import ftplib
import logging
import requests
from io import StringIO

# ── Konfiguration ─────────────────────────────────────────────────────────────

FTP_HOST  = os.environ["FTP_HOST"]
FTP_USER  = os.environ["FTP_USER"]
FTP_PASS  = os.environ["FTP_PASS"]
FTP_PATH  = os.environ.get("FTP_PATH", "/PE_All_Parts_v6.csv")

SHOPIFY_STORE   = os.environ["SHOPIFY_STORE"]          # din-butik.myshopify.com
SHOPIFY_TOKEN   = os.environ["SHOPIFY_API_KEY"]
TEST_MODE       = os.environ.get("TEST_MODE", "false").lower() == "true"
TEST_LIMIT      = 10

API_BASE  = f"https://{SHOPIFY_STORE}/admin/api/2024-01"
HEADERS   = {
    "X-Shopify-Access-Token": SHOPIFY_TOKEN,
    "Content-Type": "application/json",
}

# Tilpas denne mapping efter hvad dine scrape-produkter allerede indeholder.
# Pris-kolonne: brug RetailPrice (vejl. udsalgspris) eller DealerPrice.
PRICE_COLUMN = "RetailPrice"   # skift til "DealerPrice" hvis ønsket

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s  %(levelname)-8s  %(message)s",
    datefmt="%H:%M:%S",
)
log = logging.getLogger(__name__)


# ── FTP-download ──────────────────────────────────────────────────────────────

def download_csv_from_ftp() -> str:
    """Henter CSV fra FTP og returnerer den som en streng."""
    log.info("Forbinder til FTP: %s", FTP_HOST)
    buf = StringIO()
    raw = bytearray()

    with ftplib.FTP(FTP_HOST) as ftp:
        ftp.login(FTP_USER, FTP_PASS)
        log.info("Henter fil: %s", FTP_PATH)
        ftp.retrbinary(f"RETR {FTP_PATH}", raw.extend)

    content = raw.decode("utf-8", errors="replace")
    log.info("Fil hentet – %d bytes", len(raw))
    return content


# ── CSV-parsing ───────────────────────────────────────────────────────────────

def parse_csv(content: str) -> list[dict]:
    """Parser semikolon-separeret CSV og returnerer liste af dicts."""
    reader = csv.DictReader(StringIO(content), delimiter=";")
    rows = list(reader)
    if TEST_MODE:
        rows = rows[:TEST_LIMIT]
        log.info("TEST MODE – behandler kun %d produkter", len(rows))
    else:
        log.info("CSV indlæst – %d rækker", len(rows))
    return rows


# ── Shopify API helpers ───────────────────────────────────────────────────────

def shopify_get(endpoint: str, params: dict = None) -> dict:
    url = f"{API_BASE}/{endpoint}"
    r = requests.get(url, headers=HEADERS, params=params)
    r.raise_for_status()
    return r.json()


def shopify_post(endpoint: str, payload: dict) -> dict:
    url = f"{API_BASE}/{endpoint}"
    r = requests.post(url, headers=HEADERS, json=payload)
    r.raise_for_status()
    return r.json()


def shopify_put(endpoint: str, payload: dict) -> dict:
    url = f"{API_BASE}/{endpoint}"
    r = requests.put(url, headers=HEADERS, json=payload)
    r.raise_for_status()
    return r.json()


def rate_limit_pause():
    """Shopify API tillader ~2 req/sek på Basic / 4 req/sek på Advanced."""
    time.sleep(0.6)


# ── Shopify: hent eksisterende produkter (SKU → variant-id mapping) ──────────

def fetch_existing_products() -> dict:
    """
    Returnerer dict: { sku: {"product_id": ..., "variant_id": ..., "inventory_item_id": ...} }
    Paginerer automatisk igennem alle produkter.
    """
    mapping = {}
    params = {"limit": 250, "fields": "id,variants"}
    page_info = None

    while True:
        if page_info:
            params = {"limit": 250, "fields": "id,variants", "page_info": page_info}

        data = shopify_get("products.json", params)
        products = data.get("products", [])

        for product in products:
            for variant in product.get("variants", []):
                sku = str(variant.get("sku", "")).strip()
                if sku:
                    mapping[sku] = {
                        "product_id":        product["id"],
                        "variant_id":        variant["id"],
                        "inventory_item_id": variant.get("inventory_item_id"),
                    }

        # Link-header paginering
        link = data.get("_link_header")  # sættes ikke automatisk – se nedenfor
        # Bruger requests response headers i stedet – se fetch_with_pagination()
        break  # forenklet – erstat med paginering nedenfor ved >250 produkter

    log.info("Fandt %d eksisterende SKU'er i Shopify", len(mapping))
    return mapping


def fetch_existing_products_paginated() -> dict:
    """
    Komplet pagineret version – bruges ved >250 produkter.
    """
    mapping = {}
    url = f"{API_BASE}/products.json"
    params = {"limit": 250, "fields": "id,variants"}

    while url:
        r = requests.get(url, headers=HEADERS, params=params)
        r.raise_for_status()
        data = r.json()

        for product in data.get("products", []):
            for variant in product.get("variants", []):
                sku = str(variant.get("sku", "")).strip()
                if sku:
                    mapping[sku] = {
                        "product_id":        product["id"],
                        "variant_id":        variant["id"],
                        "inventory_item_id": variant.get("inventory_item_id"),
                    }

        # Næste side via Link-header
        link_header = r.headers.get("Link", "")
        url = None
        params = {}
        if 'rel="next"' in link_header:
            for part in link_header.split(","):
                if 'rel="next"' in part:
                    url = part.split(";")[0].strip().strip("<>")
                    break

        rate_limit_pause()

    log.info("Fandt %d eksisterende SKU'er i Shopify", len(mapping))
    return mapping


# ── Shopify: hent lokations-id til lageroppdatering ──────────────────────────

def fetch_location_id() -> int:
    """Returnerer ID for første aktive lokation."""
    data = shopify_get("locations.json")
    locations = data.get("locations", [])
    if not locations:
        raise RuntimeError("Ingen lokationer fundet i Shopify-butikken")
    loc_id = locations[0]["id"]
    log.info("Bruger lokation: %s (id=%s)", locations[0]["name"], loc_id)
    return loc_id


# ── Shopify: opret produkt ────────────────────────────────────────────────────

def create_product(row: dict, location_id: int) -> dict:
    """
    Opretter et nyt produkt i Shopify baseret på CSV-rækken.
    Billeder og beskrivelse tilføjes IKKE her – det gøres via separat scrape-flow.
    """
    sku   = str(row.get("ItemNumber", "")).strip()
    title = row.get("ItemDescription", sku)
    price = row.get(PRICE_COLUMN, "0")
    qty   = int(row.get("Availability", 0) or 0)
    ean   = str(row.get("EANBarcode", "")).strip()
    brand = row.get("BrandName", "")
    ptype = row.get("ProductType", "")
    pgrp  = row.get("ProductGroup", "")

    payload = {
        "product": {
            "title":        title,
            "vendor":       brand,
            "product_type": ptype,
            "tags":         f"{brand},{pgrp},{ptype}".strip(","),
            "variants": [{
                "sku":                  sku,
                "price":                str(price),
                "barcode":              ean,
                "inventory_management": "shopify",
                "inventory_quantity":   qty,
                "fulfillment_service":  "manual",
            }],
        }
    }

    data = shopify_post("products.json", payload)
    product   = data["product"]
    variant   = product["variants"][0]
    inv_item  = variant["inventory_item_id"]

    # Sæt lagerantal eksplicit via inventory level
    shopify_post("inventory_levels/set.json", {
        "location_id":        location_id,
        "inventory_item_id":  inv_item,
        "available":          qty,
    })

    log.info("OPRETTET  %-20s  %s", sku, title[:50])
    return {
        "product_id":        product["id"],
        "variant_id":        variant["id"],
        "inventory_item_id": inv_item,
    }


# ── Shopify: opdater pris + lager ─────────────────────────────────────────────

def update_product(row: dict, ids: dict, location_id: int):
    """Opdaterer kun pris og lagerantal for et eksisterende produkt."""
    sku   = str(row.get("ItemNumber", "")).strip()
    price = str(row.get(PRICE_COLUMN, "0"))
    qty   = int(row.get("Availability", 0) or 0)

    # Opdater pris
    shopify_put(f"variants/{ids['variant_id']}.json", {
        "variant": {"id": ids["variant_id"], "price": price}
    })
    rate_limit_pause()

    # Opdater lager
    shopify_post("inventory_levels/set.json", {
        "location_id":        location_id,
        "inventory_item_id":  ids["inventory_item_id"],
        "available":          qty,
    })

    log.info("OPDATERET %-20s  pris=%-10s lager=%d", sku, price, qty)


# ── Hovedlogik ────────────────────────────────────────────────────────────────

def main():
    log.info("=== Shopify FTP Sync starter (TEST_MODE=%s) ===", TEST_MODE)

    # 1. Hent CSV fra FTP
    csv_content = download_csv_from_ftp()

    # 2. Parse CSV
    rows = parse_csv(csv_content)

    # 3. Hent eksisterende produkter fra Shopify
    existing = fetch_existing_products_paginated()

    # 4. Hent lokations-id
    location_id = fetch_location_id()

    # 5. Behandl hver række
    created = updated = skipped = errors = 0

    for row in rows:
        sku = str(row.get("ItemNumber", "")).strip()
        if not sku:
            skipped += 1
            continue

        try:
            if sku in existing:
                update_product(row, existing[sku], location_id)
                updated += 1
            else:
                ids = create_product(row, location_id)
                existing[sku] = ids   # tilføj til mapping så næste kørsel finder det
                created += 1

            rate_limit_pause()

        except requests.HTTPError as e:
            log.error("FEJL %-20s  %s", sku, e.response.text[:200])
            errors += 1
        except Exception as e:
            log.error("FEJL %-20s  %s", sku, str(e))
            errors += 1

    log.info("=== FÆRDIG – oprettet=%d  opdateret=%d  sprunget_over=%d  fejl=%d ===",
             created, updated, skipped, errors)


if __name__ == "__main__":
    main()
