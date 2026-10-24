"""
daily_reporter.py
FBA Bros — Amazon Daily Reporting Agent

Fetches daily data from:
  - SP-API: Sales (Orders), FBA Inventory
  - Amazon Ads API v3 (async): Campaigns, Search Terms, Placements

Pushes results to Firebase Realtime Database.

CLI Usage:
  python daily_reporter.py --run-once              # fetch today for all accounts
  python daily_reporter.py --run-once --account budhagirl
  python daily_reporter.py --date 2026-10-23       # backfill a specific date
  python daily_reporter.py --daemon                # run on schedule (6am PKT daily)
"""

import os
import sys
import time
import gzip
import json
import argparse
import logging
import requests
import schedule

from io import BytesIO
from datetime import datetime, timedelta, timezone
from dotenv import load_dotenv

import firebase_admin
from firebase_admin import credentials, db

# ── Load env ──────────────────────────────────────────────────────────────────
load_dotenv()

# ── Logging ───────────────────────────────────────────────────────────────────
logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s  %(levelname)-8s  %(message)s",
    handlers=[
        logging.StreamHandler(sys.stdout),
        logging.FileHandler("daily_reporter.log"),
    ],
)
log = logging.getLogger(__name__)


# ═══════════════════════════════════════════════════════════════════════════════
# ACCOUNT CONFIG
# Add / remove accounts here. Each account needs its own SP-API + Ads credentials.
# ═══════════════════════════════════════════════════════════════════════════════
ACCOUNTS = {
    "budhagirl": {
        "label": "BuDhaGirl",
        "marketplace_id": "ATVPDKIKX0DER",   # US
        "marketplace_country": "US",
        # SP-API credentials (from env or hardcode for local testing only)
        "sp_refresh_token": os.getenv("BUDHAGIRL_SP_REFRESH_TOKEN"),
        "sp_lwa_app_id":    os.getenv("SP_LWA_APP_ID"),
        "sp_lwa_secret":    os.getenv("SP_LWA_SECRET"),
        # Ads API credentials
        "ads_client_id":     os.getenv("ADS_CLIENT_ID"),
        "ads_client_secret": os.getenv("ADS_CLIENT_SECRET"),
        "ads_refresh_token": os.getenv("BUDHAGIRL_ADS_REFRESH_TOKEN"),
        "ads_profile_id":    os.getenv("BUDHAGIRL_ADS_PROFILE_ID"),
    },
    "latika": {
        "label": "Latika Beauty",
        "marketplace_id": "ATVPDKIKX0DER",
        "marketplace_country": "US",
        "sp_refresh_token": os.getenv("LATIKA_SP_REFRESH_TOKEN"),
        "sp_lwa_app_id":    os.getenv("SP_LWA_APP_ID"),
        "sp_lwa_secret":    os.getenv("SP_LWA_SECRET"),
        "ads_client_id":     os.getenv("ADS_CLIENT_ID"),
        "ads_client_secret": os.getenv("ADS_CLIENT_SECRET"),
        "ads_refresh_token": os.getenv("LATIKA_ADS_REFRESH_TOKEN"),
        "ads_profile_id":    os.getenv("LATIKA_ADS_PROFILE_ID"),
    },
    "helixal": {
        "label": "HELIXAL",
        "marketplace_id": "ATVPDKIKX0DER",
        "marketplace_country": "US",
        "sp_refresh_token": os.getenv("HELIXAL_SP_REFRESH_TOKEN"),
        "sp_lwa_app_id":    os.getenv("SP_LWA_APP_ID"),
        "sp_lwa_secret":    os.getenv("SP_LWA_SECRET"),
        "ads_client_id":     os.getenv("ADS_CLIENT_ID"),
        "ads_client_secret": os.getenv("ADS_CLIENT_SECRET"),
        "ads_refresh_token": os.getenv("HELIXAL_ADS_REFRESH_TOKEN"),
        "ads_profile_id":    os.getenv("HELIXAL_ADS_PROFILE_ID"),
    },
    # Add more accounts following the same pattern
}


# ═══════════════════════════════════════════════════════════════════════════════
# FIREBASE INIT
# ═══════════════════════════════════════════════════════════════════════════════
def init_firebase():
    """Initialize Firebase once. Safe to call multiple times."""
    if not firebase_admin._apps:
        cred_path = os.getenv("FIREBASE_CRED_PATH", "firebase_creds.json")
        db_url    = os.getenv("FIREBASE_DB_URL")           # e.g. https://your-project-default-rtdb.firebaseio.com
        cred = credentials.Certificate(cred_path)
        firebase_admin.initialize_app(cred, {"databaseURL": db_url})
        log.info("Firebase initialized.")


# ═══════════════════════════════════════════════════════════════════════════════
# SP-API HELPERS
# ═══════════════════════════════════════════════════════════════════════════════

SP_API_BASE = "https://sellingpartnerapi-na.amazon.com"

def get_sp_access_token(account: dict) -> str:
    """Exchange LWA refresh token for a short-lived SP-API access token."""
    resp = requests.post(
        "https://api.amazon.com/auth/o2/token",
        data={
            "grant_type":    "refresh_token",
            "refresh_token": account["sp_refresh_token"],
            "client_id":     account["sp_lwa_app_id"],
            "client_secret": account["sp_lwa_secret"],
        },
        timeout=10,
    )
    resp.raise_for_status()
    return resp.json()["access_token"]


def fetch_sales(account: dict, date_str: str) -> dict:
    """
    Fetch ordered revenue + units for a single calendar day via SP-API Orders.
    date_str: 'YYYY-MM-DD'
    Returns: { revenue, units, order_count, currency }
    """
    token = get_sp_access_token(account)

    # SP-API accepts ISO 8601 timestamps
    start = f"{date_str}T00:00:00Z"
    end   = f"{date_str}T23:59:59Z"

    headers = {
        "x-amz-access-token": token,
        "x-amz-date": datetime.utcnow().strftime("%Y%m%dT%H%M%SZ"),
    }

    params = {
        "MarketplaceIds":        account["marketplace_id"],
        "CreatedAfter":          start,
        "CreatedBefore":         end,
        "OrderStatuses":         "Unshipped,Shipped,Delivered",
        "FulfillmentChannels":   "AFN",   # FBA only; remove to include MFN
    }

    revenue     = 0.0
    units       = 0
    order_count = 0
    currency    = "USD"
    next_token  = None

    while True:
        if next_token:
            params = {"NextToken": next_token}

        resp = requests.get(
            f"{SP_API_BASE}/orders/v0/orders",
            headers=headers,
            params=params,
            timeout=15,
        )
        resp.raise_for_status()
        body = resp.json().get("payload", {})

        for order in body.get("Orders", []):
            order_count += 1
            amt = order.get("OrderTotal", {})
            if amt:
                revenue  += float(amt.get("Amount", 0))
                currency  = amt.get("CurrencyCode", currency)

        # Count units via order items (extra call per order — batched below)
        order_ids = [o["AmazonOrderId"] for o in body.get("Orders", [])]
        units += fetch_order_units(token, account, order_ids)

        next_token = body.get("NextToken")
        if not next_token:
            break

    return {
        "revenue":     round(revenue, 2),
        "units":       units,
        "order_count": order_count,
        "currency":    currency,
    }


def fetch_order_units(token: str, account: dict, order_ids: list) -> int:
    """Fetch total units from a list of order IDs."""
    total = 0
    headers = {"x-amz-access-token": token}
    for oid in order_ids:
        try:
            resp = requests.get(
                f"{SP_API_BASE}/orders/v0/orders/{oid}/orderItems",
                headers=headers,
                timeout=10,
            )
            resp.raise_for_status()
            items = resp.json().get("payload", {}).get("OrderItems", [])
            total += sum(int(i.get("QuantityOrdered", 0)) for i in items)
        except Exception as e:
            log.warning(f"Could not fetch items for order {oid}: {e}")
    return total


def fetch_inventory(account: dict) -> dict:
    """
    Fetch FBA inventory snapshot via SP-API FBA Inventory.
    Returns: { total_units, asins: [{asin, sku, units, days_of_supply}] }
    """
    token = get_sp_access_token(account)
    headers = {"x-amz-access-token": token}

    params = {
        "details":        "true",
        "marketplaceIds": account["marketplace_id"],
        "granularityType": "Marketplace",
        "granularityId":   account["marketplace_id"],
    }

    asins      = []
    total      = 0
    next_token = None

    while True:
        if next_token:
            params = {"nextToken": next_token, "marketplaceIds": account["marketplace_id"]}

        resp = requests.get(
            f"{SP_API_BASE}/fba/inventory/v1/summaries",
            headers=headers,
            params=params,
            timeout=15,
        )
        resp.raise_for_status()
        payload = resp.json().get("payload", {})

        for item in payload.get("inventorySummaries", []):
            qty = item.get("inventoryDetails", {}).get("fulfillableQuantity", 0)
            dos = item.get("daysOfSupply", None)
            asins.append({
                "asin":           item.get("asin"),
                "sku":            item.get("sellerSku"),
                "units":          qty,
                "days_of_supply": dos,
                "reorder_alert":  dos is not None and dos < 30,
            })
            total += qty

        next_token = payload.get("nextToken")
        if not next_token:
            break

    return {
        "total_units": total,
        "asins":       asins,
        "reorder_count": sum(1 for a in asins if a["reorder_alert"]),
    }


# ═══════════════════════════════════════════════════════════════════════════════
# ADS API v3 HELPERS  (async: request → poll → download → decompress)
# Extends your existing amazon_agent.py pattern
# ═══════════════════════════════════════════════════════════════════════════════

ADS_API_BASE = "https://advertising-api.amazon.com"

def get_ads_access_token(account: dict) -> str:
    resp = requests.post(
        "https://api.amazon.com/auth/o2/token",
        data={
            "grant_type":    "refresh_token",
            "refresh_token": account["ads_refresh_token"],
            "client_id":     account["ads_client_id"],
            "client_secret": account["ads_client_secret"],
        },
        timeout=10,
    )
    resp.raise_for_status()
    return resp.json()["access_token"]


def request_ads_report(token: str, profile_id: str, date_str: str, report_type: str) -> str:
    """
    Request an async Ads API v3 report.
    report_type: spCampaigns | spSearchTerm | spTargeting | spAdvertisedProduct | spPlacement
    Returns reportId.
    """
    date_nodash = date_str.replace("-", "")   # YYYYMMDD

    # Metrics per report type
    METRICS = {
        "spCampaigns": [
            "impressions","clicks","cost","purchases1d","sales1d",
            "costPerClick","clickThroughRate","returnOnAdSpend"
        ],
        "spSearchTerm": [
            "impressions","clicks","cost","purchases1d","sales1d",
            "searchTermImpressionRank"
        ],
        "spTargeting": [
            "impressions","clicks","cost","purchases1d","sales1d"
        ],
        "spAdvertisedProduct": [
            "impressions","clicks","cost","purchases1d","sales1d","advertisedAsin"
        ],
        "spPlacement": [
            "impressions","clicks","cost","purchases1d","sales1d",
            "placementClassification"
        ],
    }

    body = {
        "name":          f"{report_type}_{date_str}",
        "startDate":     date_str,
        "endDate":       date_str,
        "configuration": {
            "adProduct":    "SPONSORED_PRODUCTS",
            "groupBy":      ["campaign"] if "Campaign" in report_type else ["targeting"],
            "columns":      METRICS.get(report_type, METRICS["spCampaigns"]),
            "reportTypeId": report_type,
            "timeUnit":     "SUMMARY",
            "format":       "GZIP_JSON",
        },
    }

    resp = requests.post(
        f"{ADS_API_BASE}/reporting/reports",
        headers={
            "Amazon-Advertising-API-ClientId": account["ads_client_id"],
            "Amazon-Advertising-API-Scope":    profile_id,
            "Authorization":                   f"Bearer {token}",
            "Content-Type":                    "application/json",
        },
        json=body,
        timeout=15,
    )
    resp.raise_for_status()
    return resp.json()["reportId"]


def poll_ads_report(token: str, account: dict, report_id: str,
                    max_wait_sec: int = 120) -> str:
    """Poll until report is COMPLETED. Returns download URL."""
    headers = {
        "Amazon-Advertising-API-ClientId": account["ads_client_id"],
        "Amazon-Advertising-API-Scope":    account["ads_profile_id"],
        "Authorization":                   f"Bearer {token}",
    }
    waited = 0
    while waited < max_wait_sec:
        resp = requests.get(
            f"{ADS_API_BASE}/reporting/reports/{report_id}",
            headers=headers,
            timeout=10,
        )
        resp.raise_for_status()
        data   = resp.json()
        status = data.get("status")
        if status == "COMPLETED":
            return data["url"]
        if status == "FAILED":
            raise RuntimeError(f"Ads report {report_id} failed: {data}")
        log.info(f"  Report {report_id} status={status}, waiting 10s...")
        time.sleep(10)
        waited += 10
    raise TimeoutError(f"Ads report {report_id} did not complete in {max_wait_sec}s")


def download_ads_report(url: str) -> list:
    """Download + decompress gzip JSON report. Returns list of row dicts."""
    resp = requests.get(url, timeout=30)
    resp.raise_for_status()
    with gzip.open(BytesIO(resp.content), "rt", encoding="utf-8") as f:
        return json.load(f)


def fetch_ppc_summary(account: dict, date_str: str) -> dict:
    """
    Fetch all 5 PPC report types for a single day.
    Returns aggregated summary + raw rows per report.
    """
    token      = get_ads_access_token(account)
    profile_id = account["ads_profile_id"]

    report_types = [
        "spCampaigns",
        "spSearchTerm",
        "spTargeting",
        "spAdvertisedProduct",
        "spPlacement",
    ]

    # 1. Request all reports in parallel (fire all, then poll)
    report_ids = {}
    for rt in report_types:
        try:
            rid = request_ads_report(token, profile_id, date_str, rt)
            report_ids[rt] = rid
            log.info(f"  Requested {rt} report: {rid}")
        except Exception as e:
            log.warning(f"  Could not request {rt}: {e}")

    # 2. Poll + download each
    raw_data = {}
    for rt, rid in report_ids.items():
        try:
            url  = poll_ads_report(token, account, rid)
            rows = download_ads_report(url)
            raw_data[rt] = rows
            log.info(f"  Downloaded {rt}: {len(rows)} rows")
        except Exception as e:
            log.warning(f"  Could not download {rt}: {e}")
            raw_data[rt] = []

    # 3. Aggregate campaign-level summary
    campaigns = raw_data.get("spCampaigns", [])
    total_spend       = sum(float(r.get("cost", 0))         for r in campaigns)
    total_sales       = sum(float(r.get("sales1d", 0))      for r in campaigns)
    total_clicks      = sum(int(r.get("clicks", 0))         for r in campaigns)
    total_impressions = sum(int(r.get("impressions", 0))    for r in campaigns)
    total_orders      = sum(int(r.get("purchases1d", 0))    for r in campaigns)

    acos = round(total_spend / total_sales * 100, 2) if total_sales > 0 else 0
    roas = round(total_sales / total_spend, 2)        if total_spend > 0 else 0
    ctr  = round(total_clicks / total_impressions * 100, 2) if total_impressions > 0 else 0
    cpc  = round(total_spend / total_clicks, 2)       if total_clicks > 0 else 0

    # 4. Top 5 search terms by spend
    search_terms = sorted(
        raw_data.get("spSearchTerm", []),
        key=lambda r: float(r.get("cost", 0)),
        reverse=True,
    )[:5]

    # 5. Placement breakdown
    placements = {
        r.get("placementClassification", "unknown"): {
            "spend":  float(r.get("cost", 0)),
            "sales":  float(r.get("sales1d", 0)),
            "clicks": int(r.get("clicks", 0)),
        }
        for r in raw_data.get("spPlacement", [])
    }

    return {
        "summary": {
            "spend":       round(total_spend, 2),
            "sales":       round(total_sales, 2),
            "clicks":      total_clicks,
            "impressions": total_impressions,
            "orders":      total_orders,
            "acos":        acos,
            "roas":        roas,
            "ctr":         ctr,
            "cpc":         cpc,
        },
        "top_search_terms": [
            {
                "term":   r.get("searchTerm", r.get("keywordText", "N/A")),
                "spend":  float(r.get("cost", 0)),
                "sales":  float(r.get("sales1d", 0)),
                "clicks": int(r.get("clicks", 0)),
            }
            for r in search_terms
        ],
        "placements": placements,
        "campaign_count": len(campaigns),
    }


# ═══════════════════════════════════════════════════════════════════════════════
# FIREBASE WRITER
# ═══════════════════════════════════════════════════════════════════════════════

def push_to_firebase(account_id: str, date_str: str, payload: dict):
    """Write daily snapshot to Firebase at /reports/{account_id}/{date}."""
    ref = db.reference(f"/reports/{account_id}/{date_str}")
    ref.set(payload)
    log.info(f"  ✅ Firebase updated: /reports/{account_id}/{date_str}")


# ═══════════════════════════════════════════════════════════════════════════════
# MAIN FETCH ROUTINE
# ═══════════════════════════════════════════════════════════════════════════════

def run_for_account(account_id: str, date_str: str):
    account = ACCOUNTS.get(account_id)
    if not account:
        log.error(f"Unknown account: {account_id}")
        return

    label = account["label"]
    log.info(f"━━━ Starting fetch: {label} | {date_str} ━━━")

    result = {
        "account_id":  account_id,
        "account_name": label,
        "date":        date_str,
        "fetched_at":  datetime.utcnow().isoformat(),
        "sales":       {},
        "inventory":   {},
        "ppc":         {},
        "alerts":      [],
    }

    # — Sales ——————————————————————————————————————————————
    try:
        log.info("  Fetching sales...")
        result["sales"] = fetch_sales(account, date_str)
        log.info(f"  Sales: ${result['sales']['revenue']:,.2f} | "
                 f"{result['sales']['units']} units | "
                 f"{result['sales']['order_count']} orders")
    except Exception as e:
        log.error(f"  Sales fetch failed: {e}")
        result["alerts"].append({"type": "error", "source": "sales", "message": str(e)})

    # — Inventory ——————————————————————————————————————————
    try:
        log.info("  Fetching inventory...")
        result["inventory"] = fetch_inventory(account)
        log.info(f"  Inventory: {result['inventory']['total_units']} units | "
                 f"{result['inventory']['reorder_count']} items need reorder")
        # Add reorder alerts
        for item in result["inventory"]["asins"]:
            if item.get("reorder_alert"):
                result["alerts"].append({
                    "type":    "reorder",
                    "source":  "inventory",
                    "sku":     item["sku"],
                    "asin":    item["asin"],
                    "units":   item["units"],
                    "dos":     item["days_of_supply"],
                    "message": f"Low stock: {item['sku']} — {item['days_of_supply']} days of supply",
                })
    except Exception as e:
        log.error(f"  Inventory fetch failed: {e}")
        result["alerts"].append({"type": "error", "source": "inventory", "message": str(e)})

    # — PPC ————————————————————————————————————————————————
    try:
        log.info("  Fetching PPC data...")
        result["ppc"] = fetch_ppc_summary(account, date_str)
        s = result["ppc"]["summary"]
        log.info(f"  PPC: Spend ${s['spend']:,.2f} | Sales ${s['sales']:,.2f} | "
                 f"ACoS {s['acos']}% | ROAS {s['roas']}")

        # ACoS alert if over 40%
        if s["acos"] > 40:
            result["alerts"].append({
                "type":    "high_acos",
                "source":  "ppc",
                "acos":    s["acos"],
                "message": f"High ACoS: {s['acos']}% — review bids",
            })
    except Exception as e:
        log.error(f"  PPC fetch failed: {e}")
        result["alerts"].append({"type": "error", "source": "ppc", "message": str(e)})

    # — Push to Firebase ——————————————————————————————————
    try:
        push_to_firebase(account_id, date_str, result)
    except Exception as e:
        log.error(f"  Firebase push failed: {e}")

    log.info(f"━━━ Done: {label} | {len(result['alerts'])} alerts ━━━\n")
    return result


def run_all_accounts(date_str: str):
    log.info(f"🚀 Running daily reporter for ALL accounts — {date_str}")
    for account_id in ACCOUNTS:
        run_for_account(account_id, date_str)
    log.info("✅ All accounts complete.")


# ═══════════════════════════════════════════════════════════════════════════════
# SCHEDULER (daemon mode)
# ═══════════════════════════════════════════════════════════════════════════════

def scheduled_job():
    yesterday = (datetime.utcnow() - timedelta(days=1)).strftime("%Y-%m-%d")
    run_all_accounts(yesterday)


def run_daemon():
    log.info("🕐 Daemon mode: will run every day at 01:00 UTC (06:00 PKT)")
    schedule.every().day.at("01:00").do(scheduled_job)
    while True:
        schedule.run_pending()
        time.sleep(30)


# ═══════════════════════════════════════════════════════════════════════════════
# CLI ENTRY POINT
# ═══════════════════════════════════════════════════════════════════════════════

def parse_args():
    p = argparse.ArgumentParser(description="FBA Bros Daily Reporter Agent")
    group = p.add_mutually_exclusive_group(required=True)
    group.add_argument("--run-once",  action="store_true",
                       help="Fetch today (or --date) and exit")
    group.add_argument("--daemon",    action="store_true",
                       help="Run on daily schedule (blocking)")

    p.add_argument("--account", type=str, default=None,
                   help="Single account ID to run (default: all). "
                        f"Options: {list(ACCOUNTS.keys())}")
    p.add_argument("--date", type=str, default=None,
                   help="Date to fetch: YYYY-MM-DD (default: yesterday)")
    return p.parse_args()


if __name__ == "__main__":
    args = parse_args()
    init_firebase()

    if args.daemon:
        run_daemon()
    else:
        # Default date = yesterday (Ads API data for today isn't final until EOD)
        date_str = args.date or (datetime.utcnow() - timedelta(days=1)).strftime("%Y-%m-%d")
        if args.account:
            run_for_account(args.account, date_str)
        else:
            run_all_accounts(date_str)
