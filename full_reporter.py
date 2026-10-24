"""
full_reporter.py
FBA Bros — Full Amazon Account Health Agent

Fetches daily data from Amazon SP-API and Ads API v3 across all sections:

  SALES & ORDERS
    ✅ Sales (revenue, units, orders)

  INVENTORY
    ✅ FBA Inventory (stock levels, days of supply — reorder alert at 90 days)
    ✅ Stranded Inventory (listings stuck with no buyable offer)
    ✅ AWD Stock (Amazon Warehousing & Distribution inbound + on-hand)

  ADVERTISING
    ✅ Amazon Ads PPC — Campaigns, Search Terms, Targeting, Placements
       ⚠️  REPORT ONLY — agent reports findings, NO automatic bid/budget changes

  ACCOUNT HEALTH
    ✅ Account Health Score + metrics (late shipment, cancellation, defect rates)
    ✅ Performance Notifications (warnings, suspensions, policy alerts)
    ✅ Voice of Customer (product quality issues flagged by Amazon)

  CUSTOMER
    ✅ Customer Feedback (seller feedback score + recent negative reviews)
    ✅ Customer Reviews by ASIN (star ratings, recent reviews)
    ✅ Recent Seller Support Cases

  COMMUNICATIONS
    ✅ Creator Connections (new messages from influencers)

  LOGISTICS
    ✅ Inbound Shipments (FBA shipment status)

Output:
  - Firebase Realtime Database  (/reports/{account_id}/{date}/)
  - Excel report file           (report_{account}_{date}.xlsx)
  - JSON report file            (report_{account}_{date}.json)

CLI:
  python full_reporter.py --run-once --account budhagirl
  python full_reporter.py --run-once              # all accounts
  python full_reporter.py --date 2026-10-23       # backfill
  python full_reporter.py --daemon                # daily 6am PKT
"""

import os, sys, time, gzip, json, argparse, logging, requests, schedule
from io import BytesIO
from datetime import datetime, timedelta
from dotenv import load_dotenv
import openpyxl
from openpyxl.styles import Font, PatternFill, Alignment, Border, Side
from openpyxl.utils import get_column_letter
import firebase_admin
from firebase_admin import credentials, db

load_dotenv()

# ── Logging ──────────────────────────────────────────────────────────────────
logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s  %(levelname)-8s  %(message)s",
    handlers=[
        logging.StreamHandler(sys.stdout),
        logging.FileHandler("full_reporter.log"),
    ],
)
log = logging.getLogger(__name__)

# ═══════════════════════════════════════════════════════════════════════════════
# ACCOUNT CONFIG
# ═══════════════════════════════════════════════════════════════════════════════
ACCOUNTS = {
    "budhagirl": {
        "label":               "BuDhaGirl",
        "marketplace_id":      "ATVPDKIKX0DER",
        "marketplace_country": "US",
        "sp_refresh_token":    os.getenv("BUDHAGIRL_SP_REFRESH_TOKEN"),
        "sp_lwa_app_id":       os.getenv("SP_LWA_APP_ID"),
        "sp_lwa_secret":       os.getenv("SP_LWA_SECRET"),
        "ads_client_id":       os.getenv("ADS_CLIENT_ID"),
        "ads_client_secret":   os.getenv("ADS_CLIENT_SECRET"),
        "ads_refresh_token":   os.getenv("BUDHAGIRL_ADS_REFRESH_TOKEN"),
        "ads_profile_id":      os.getenv("BUDHAGIRL_ADS_PROFILE_ID"),
    },
    "latika": {
        "label":               "Latika Beauty",
        "marketplace_id":      "ATVPDKIKX0DER",
        "marketplace_country": "US",
        "sp_refresh_token":    os.getenv("LATIKA_SP_REFRESH_TOKEN"),
        "sp_lwa_app_id":       os.getenv("SP_LWA_APP_ID"),
        "sp_lwa_secret":       os.getenv("SP_LWA_SECRET"),
        "ads_client_id":       os.getenv("ADS_CLIENT_ID"),
        "ads_client_secret":   os.getenv("ADS_CLIENT_SECRET"),
        "ads_refresh_token":   os.getenv("LATIKA_ADS_REFRESH_TOKEN"),
        "ads_profile_id":      os.getenv("LATIKA_ADS_PROFILE_ID"),
    },
    "helixal": {
        "label":               "HELIXAL",
        "marketplace_id":      "ATVPDKIKX0DER",
        "marketplace_country": "US",
        "sp_refresh_token":    os.getenv("HELIXAL_SP_REFRESH_TOKEN"),
        "sp_lwa_app_id":       os.getenv("SP_LWA_APP_ID"),
        "sp_lwa_secret":       os.getenv("SP_LWA_SECRET"),
        "ads_client_id":       os.getenv("ADS_CLIENT_ID"),
        "ads_client_secret":   os.getenv("ADS_CLIENT_SECRET"),
        "ads_refresh_token":   os.getenv("HELIXAL_ADS_REFRESH_TOKEN"),
        "ads_profile_id":      os.getenv("HELIXAL_ADS_PROFILE_ID"),
    },
}

SP_API_BASE  = "https://sellingpartnerapi-na.amazon.com"
ADS_API_BASE = "https://advertising-api.amazon.com"

# ═══════════════════════════════════════════════════════════════════════════════
# FIREBASE
# ═══════════════════════════════════════════════════════════════════════════════
def init_firebase():
    if not firebase_admin._apps:
        cred = credentials.Certificate(os.getenv("FIREBASE_CRED_PATH", "firebase_creds.json"))
        firebase_admin.initialize_app(cred, {"databaseURL": os.getenv("FIREBASE_DB_URL")})
        log.info("Firebase initialized.")

def push_to_firebase(account_id, date_str, payload):
    ref = db.reference(f"/reports/{account_id}/{date_str}")
    ref.set(payload)
    log.info(f"  ✅ Firebase: /reports/{account_id}/{date_str}")

# ═══════════════════════════════════════════════════════════════════════════════
# AUTH HELPERS
# ═══════════════════════════════════════════════════════════════════════════════
def get_sp_token(account):
    r = requests.post("https://api.amazon.com/auth/o2/token", data={
        "grant_type":    "refresh_token",
        "refresh_token": account["sp_refresh_token"],
        "client_id":     account["sp_lwa_app_id"],
        "client_secret": account["sp_lwa_secret"],
    }, timeout=10)
    r.raise_for_status()
    return r.json()["access_token"]

def get_ads_token(account):
    r = requests.post("https://api.amazon.com/auth/o2/token", data={
        "grant_type":    "refresh_token",
        "refresh_token": account["ads_refresh_token"],
        "client_id":     account["ads_client_id"],
        "client_secret": account["ads_client_secret"],
    }, timeout=10)
    r.raise_for_status()
    return r.json()["access_token"]

def sp_headers(token):
    return {
        "x-amz-access-token": token,
        "x-amz-date": datetime.utcnow().strftime("%Y%m%dT%H%M%SZ"),
    }

def ads_headers(token, account):
    return {
        "Amazon-Advertising-API-ClientId": account["ads_client_id"],
        "Amazon-Advertising-API-Scope":    account["ads_profile_id"],
        "Authorization":                   f"Bearer {token}",
        "Content-Type":                    "application/json",
    }

# ═══════════════════════════════════════════════════════════════════════════════
# 1. SALES
# ═══════════════════════════════════════════════════════════════════════════════
def fetch_sales(account, date_str):
    """Daily revenue, units, order count via SP-API Orders."""
    token = get_sp_token(account)
    headers = sp_headers(token)
    params = {
        "MarketplaceIds": account["marketplace_id"],
        "CreatedAfter":   f"{date_str}T00:00:00Z",
        "CreatedBefore":  f"{date_str}T23:59:59Z",
        "OrderStatuses":  "Unshipped,Shipped,Delivered",
        "FulfillmentChannels": "AFN",
    }
    revenue, units, order_count, currency = 0.0, 0, 0, "USD"
    next_token = None
    while True:
        if next_token:
            params = {"NextToken": next_token}
        r = requests.get(f"{SP_API_BASE}/orders/v0/orders",
                         headers=headers, params=params, timeout=15)
        r.raise_for_status()
        body = r.json().get("payload", {})
        for o in body.get("Orders", []):
            order_count += 1
            amt = o.get("OrderTotal", {})
            if amt:
                revenue += float(amt.get("Amount", 0))
                currency = amt.get("CurrencyCode", currency)
            # fetch units per order
            try:
                ir = requests.get(f"{SP_API_BASE}/orders/v0/orders/{o['AmazonOrderId']}/orderItems",
                                  headers={"x-amz-access-token": token}, timeout=10)
                ir.raise_for_status()
                items = ir.json().get("payload", {}).get("OrderItems", [])
                units += sum(int(i.get("QuantityOrdered", 0)) for i in items)
            except Exception as e:
                log.warning(f"    Order items fetch failed for {o['AmazonOrderId']}: {e}")
        next_token = body.get("NextToken")
        if not next_token:
            break
    return {"revenue": round(revenue, 2), "units": units,
            "order_count": order_count, "currency": currency}

# ═══════════════════════════════════════════════════════════════════════════════
# 2. FBA INVENTORY (90-day reorder alert)
# ═══════════════════════════════════════════════════════════════════════════════
def fetch_inventory(account):
    """FBA stock levels. Flags reorder_alert when days_of_supply < 90."""
    token = get_sp_token(account)
    params = {
        "details": "true",
        "marketplaceIds": account["marketplace_id"],
        "granularityType": "Marketplace",
        "granularityId":   account["marketplace_id"],
    }
    asins, total, next_token = [], 0, None
    while True:
        if next_token:
            params = {"nextToken": next_token, "marketplaceIds": account["marketplace_id"]}
        r = requests.get(f"{SP_API_BASE}/fba/inventory/v1/summaries",
                         headers=sp_headers(token), params=params, timeout=15)
        r.raise_for_status()
        payload = r.json().get("payload", {})
        for item in payload.get("inventorySummaries", []):
            qty = item.get("inventoryDetails", {}).get("fulfillableQuantity", 0)
            dos = item.get("daysOfSupply")
            asins.append({
                "asin": item.get("asin"),
                "sku":  item.get("sellerSku"),
                "units": qty,
                "days_of_supply": dos,
                "reorder_alert": dos is not None and dos < 90,
            })
            total += qty
        next_token = payload.get("nextToken")
        if not next_token:
            break
    return {
        "total_units":    total,
        "asins":          asins,
        "reorder_count":  sum(1 for a in asins if a["reorder_alert"]),
    }

# ═══════════════════════════════════════════════════════════════════════════════
# 3. STRANDED INVENTORY
# ═══════════════════════════════════════════════════════════════════════════════
def fetch_stranded_inventory(account):
    """
    Stranded inventory = units in FBA warehouse with no active buyable listing.
    Uses SP-API FBA Inventory with 'STRANDED' condition filter.
    """
    token = get_sp_token(account)
    params = {
        "details": "true",
        "marketplaceIds": account["marketplace_id"],
        "granularityType": "Marketplace",
        "granularityId":   account["marketplace_id"],
    }
    stranded_items, total_stranded = [], 0
    next_token = None
    while True:
        if next_token:
            params = {"nextToken": next_token, "marketplaceIds": account["marketplace_id"]}
        r = requests.get(f"{SP_API_BASE}/fba/inventory/v1/summaries",
                         headers=sp_headers(token), params=params, timeout=15)
        r.raise_for_status()
        payload = r.json().get("payload", {})
        for item in payload.get("inventorySummaries", []):
            details = item.get("inventoryDetails", {})
            stranded_qty = details.get("strandedQuantity", 0)
            if stranded_qty and stranded_qty > 0:
                stranded_items.append({
                    "asin":          item.get("asin"),
                    "sku":           item.get("sellerSku"),
                    "stranded_units": stranded_qty,
                    "reason":        details.get("strandedReasonCode", "Unknown"),
                })
                total_stranded += stranded_qty
        next_token = payload.get("nextToken")
        if not next_token:
            break
    return {
        "total_stranded_units": total_stranded,
        "stranded_count":       len(stranded_items),
        "items":                stranded_items,
    }

# ═══════════════════════════════════════════════════════════════════════════════
# 4. AWD STOCK (Amazon Warehousing & Distribution)
# ═══════════════════════════════════════════════════════════════════════════════
def fetch_awd_stock(account):
    """
    AWD inbound + on-hand inventory via SP-API AWD (awd/2024-05-09).
    Returns list of SKUs with quantities at AWD vs FBA.
    """
    token = get_sp_token(account)
    try:
        r = requests.get(
            f"{SP_API_BASE}/awd/2024-05-09/inventory",
            headers=sp_headers(token),
            params={"marketplaceId": account["marketplace_id"]},
            timeout=15,
        )
        r.raise_for_status()
        inventory = r.json().get("inventory", [])
        items = []
        for item in inventory:
            items.append({
                "sku":            item.get("sku"),
                "awd_quantity":   item.get("totalAWDQuantity", 0),
                "inbound":        item.get("inboundQuantity", 0),
                "available":      item.get("availableQuantity", 0),
                "reserved":       item.get("reservedQuantity", 0),
            })
        return {
            "total_awd_units": sum(i["awd_quantity"] for i in items),
            "items":           items,
        }
    except Exception as e:
        log.warning(f"  AWD fetch failed (may not be enrolled): {e}")
        return {"total_awd_units": 0, "items": [], "note": "AWD not enrolled or API unavailable"}

# ═══════════════════════════════════════════════════════════════════════════════
# 5. ACCOUNT HEALTH
# ═══════════════════════════════════════════════════════════════════════════════
def fetch_account_health(account):
    """
    Account Health Score + key metrics via SP-API Account Health API.
    Metrics: Order Defect Rate, Late Shipment Rate, Valid Tracking Rate, etc.
    """
    token = get_sp_token(account)
    try:
        r = requests.get(
            f"{SP_API_BASE}/seller/v1/account/salesRank",
            headers=sp_headers(token),
            timeout=15,
        )
        # Account health endpoint
        r2 = requests.get(
            f"{SP_API_BASE}/sales/v1/orderMetrics",
            headers=sp_headers(token),
            params={
                "marketplaceIds": account["marketplace_id"],
                "interval": f"{datetime.utcnow().strftime('%Y-%m-%d')}T00:00:00Z/"
                            f"{datetime.utcnow().strftime('%Y-%m-%d')}T23:59:59Z",
                "granularity": "Day",
                "granularityTimeZone": "UTC",
            },
            timeout=15,
        )
        # Try the dedicated account health endpoint
        r3 = requests.get(
            f"{SP_API_BASE}/selling-partner-insights/2024-09-19/customers/customerMetrics",
            headers=sp_headers(token),
            timeout=15,
        )
    except Exception:
        pass

    # Primary Account Health API
    try:
        r_health = requests.get(
            f"{SP_API_BASE}/account/2022-09-06/externalId/{account['marketplace_id']}/sellerAccountHealth",
            headers=sp_headers(token),
            timeout=15,
        )
        r_health.raise_for_status()
        health_data = r_health.json()
        return {
            "health_status":      health_data.get("healthStatus", "Unknown"),
            "health_score":       health_data.get("healthScore"),
            "metrics":            health_data.get("issues", []),
        }
    except Exception:
        pass

    # Fallback: use the standard account health endpoint
    try:
        r_health2 = requests.get(
            f"{SP_API_BASE}/account/v1/amazonOrderMetrics",
            headers=sp_headers(token),
            params={"marketplaceIds": account["marketplace_id"]},
            timeout=15,
        )
        r_health2.raise_for_status()
        return r_health2.json()
    except Exception as e:
        log.warning(f"  Account health fetch failed: {e}")
        return {
            "health_status": "Unable to fetch",
            "note": "Requires Selling Partner Insights permission in SP-API app",
            "error": str(e),
        }

# ═══════════════════════════════════════════════════════════════════════════════
# 6. PERFORMANCE NOTIFICATIONS
# ═══════════════════════════════════════════════════════════════════════════════
def fetch_performance_notifications(account):
    """
    Performance Notifications — warnings, policy alerts, suspension risks.
    Uses SP-API Notifications subscription or Messaging API.
    """
    token = get_sp_token(account)
    try:
        # Get subscribed notification types
        r = requests.get(
            f"{SP_API_BASE}/notifications/v1/subscriptions/ACCOUNT_STATUS_CHANGED",
            headers=sp_headers(token),
            timeout=15,
        )
        r.raise_for_status()
        return {"notifications": r.json().get("payload", {})}
    except Exception:
        pass

    # Fallback: check for any active issues via account health
    try:
        r2 = requests.get(
            f"{SP_API_BASE}/notifications/v1/subscriptions/PERFORMANCE_NOTIFICATION",
            headers=sp_headers(token),
            timeout=15,
        )
        r2.raise_for_status()
        return {"notifications": r2.json().get("payload", {})}
    except Exception as e:
        log.warning(f"  Performance notifications fetch failed: {e}")
        return {
            "notifications": [],
            "note": "Check Seller Central > Performance > Performance Notifications manually",
            "error": str(e),
        }

# ═══════════════════════════════════════════════════════════════════════════════
# 7. VOICE OF CUSTOMER
# ═══════════════════════════════════════════════════════════════════════════════
def fetch_voice_of_customer(account):
    """
    Voice of Customer — product quality issues flagged by Amazon based on returns/feedback.
    Uses SP-API Listings Issues or VOC endpoint.
    """
    token = get_sp_token(account)
    try:
        r = requests.get(
            f"{SP_API_BASE}/listings/2021-08-01/issues",
            headers=sp_headers(token),
            params={
                "marketplaceIds": account["marketplace_id"],
                "issueLocale": "en_US",
            },
            timeout=15,
        )
        r.raise_for_status()
        issues = r.json().get("issues", [])
        voc_issues = [i for i in issues if "voice" in str(i).lower() or "quality" in str(i).lower()]
        return {
            "total_issues":  len(issues),
            "voc_issues":    voc_issues,
            "all_issues":    issues[:20],   # cap at 20 for report
        }
    except Exception as e:
        log.warning(f"  Voice of Customer fetch failed: {e}")
        return {
            "voc_issues": [],
            "note": "Check Seller Central > Voice of the Customer manually",
            "error": str(e),
        }

# ═══════════════════════════════════════════════════════════════════════════════
# 8. SELLER FEEDBACK
# ═══════════════════════════════════════════════════════════════════════════════
def fetch_seller_feedback(account):
    """
    Seller feedback score + recent negative/neutral feedback via SP-API Feedback.
    """
    token = get_sp_token(account)
    try:
        r = requests.get(
            f"{SP_API_BASE}/feedback/v1/summaries",
            headers=sp_headers(token),
            params={
                "marketplaceId":   account["marketplace_id"],
                "startTime":       (datetime.utcnow() - timedelta(days=90)).strftime("%Y-%m-%dT00:00:00Z"),
                "endTime":         datetime.utcnow().strftime("%Y-%m-%dT23:59:59Z"),
            },
            timeout=15,
        )
        r.raise_for_status()
        data = r.json()
        return {
            "rating_30d":       data.get("30DayRating"),
            "rating_90d":       data.get("90DayRating"),
            "rating_365d":      data.get("365DayRating"),
            "lifetime_rating":  data.get("lifetimeRating"),
            "count_30d":        data.get("30DayCount"),
            "negative_30d":     data.get("30DayNegativeCount"),
            "neutral_30d":      data.get("30DayNeutralCount"),
        }
    except Exception as e:
        log.warning(f"  Seller feedback fetch failed: {e}")
        return {"error": str(e), "note": "Requires Feedback permission in SP-API app"}

# ═══════════════════════════════════════════════════════════════════════════════
# 9. CUSTOMER REVIEWS
# ═══════════════════════════════════════════════════════════════════════════════
def fetch_customer_reviews(account):
    """
    Customer reviews by ASIN — recent reviews, star rating breakdown.
    Uses Product Reviews API (requires special approval from Amazon).
    """
    token = get_sp_token(account)
    try:
        # First get list of ASINs from inventory
        inv = fetch_inventory(account)
        asin_list = list(set(a["asin"] for a in inv["asins"] if a.get("asin")))[:10]  # max 10

        all_reviews = []
        for asin in asin_list:
            try:
                r = requests.get(
                    f"{SP_API_BASE}/productReviews/v1/reviews/{asin}",
                    headers=sp_headers(token),
                    params={
                        "marketplaceId": account["marketplace_id"],
                        "sortBy": "RECENT",
                        "pageSize": 5,
                    },
                    timeout=15,
                )
                r.raise_for_status()
                reviews = r.json().get("reviews", [])
                all_reviews.append({
                    "asin":    asin,
                    "reviews": reviews[:5],
                    "count":   len(reviews),
                })
            except Exception:
                pass  # ASIN may have no reviews or no permission

        return {
            "asins_checked":  len(asin_list),
            "asin_reviews":   all_reviews,
            "note": "Reviews API requires Product Reviews permission in SP-API developer console",
        }
    except Exception as e:
        log.warning(f"  Customer reviews fetch failed: {e}")
        return {"asin_reviews": [], "error": str(e)}

# ═══════════════════════════════════════════════════════════════════════════════
# 10. SELLER SUPPORT CASES
# ═══════════════════════════════════════════════════════════════════════════════
def fetch_support_cases(account):
    """
    Recent Seller Support cases — open, pending, recently resolved.
    Uses SP-API Support Cases API.
    """
    token = get_sp_token(account)
    try:
        r = requests.get(
            f"{SP_API_BASE}/support/2021-12-01/cases",
            headers=sp_headers(token),
            params={
                "marketplaceId": account["marketplace_id"],
                "status": "OPEN",
                "pageSize": 20,
            },
            timeout=15,
        )
        r.raise_for_status()
        cases = r.json().get("cases", [])
        return {
            "open_cases": len(cases),
            "cases": [
                {
                    "case_id":     c.get("caseId"),
                    "subject":     c.get("subject"),
                    "status":      c.get("status"),
                    "created":     c.get("createDate"),
                    "last_update": c.get("lastUpdateDate"),
                    "category":    c.get("category"),
                }
                for c in cases
            ],
        }
    except Exception as e:
        log.warning(f"  Support cases fetch failed: {e}")
        return {"open_cases": 0, "cases": [], "error": str(e)}

# ═══════════════════════════════════════════════════════════════════════════════
# 11. CREATOR CONNECTIONS (new messages)
# ═══════════════════════════════════════════════════════════════════════════════
def fetch_creator_connections(account):
    """
    Creator Connections — new influencer messages/applications.
    Uses SP-API Creator Connections endpoint.
    """
    token = get_sp_token(account)
    try:
        r = requests.get(
            f"{SP_API_BASE}/creatorConnections/v1/conversations",
            headers=sp_headers(token),
            params={
                "marketplaceId": account["marketplace_id"],
                "status": "UNREAD",
                "pageSize": 20,
            },
            timeout=15,
        )
        r.raise_for_status()
        convos = r.json().get("conversations", [])
        return {
            "unread_count": len(convos),
            "conversations": [
                {
                    "id":       c.get("conversationId"),
                    "creator":  c.get("creatorName"),
                    "subject":  c.get("subject"),
                    "received": c.get("receivedDate"),
                    "status":   c.get("status"),
                }
                for c in convos
            ],
        }
    except Exception as e:
        log.warning(f"  Creator Connections fetch failed: {e}")
        return {
            "unread_count": 0,
            "conversations": [],
            "note": "Check Seller Central > Advertising > Creator Connections manually",
            "error": str(e),
        }

# ═══════════════════════════════════════════════════════════════════════════════
# 12. INBOUND SHIPMENTS
# ═══════════════════════════════════════════════════════════════════════════════
def fetch_shipments(account):
    """
    FBA inbound shipments — active, receiving, closed, problem shipments.
    Uses SP-API FBA Inbound API.
    """
    token = get_sp_token(account)
    all_shipments = []
    status_list = ["WORKING", "SHIPPED", "IN_TRANSIT", "RECEIVING", "CHECKED_IN", "CLOSED", "CANCELLED"]
    for status in status_list:
        try:
            r = requests.get(
                f"{SP_API_BASE}/fba/inbound/v0/shipments",
                headers=sp_headers(token),
                params={
                    "MarketplaceId": account["marketplace_id"],
                    "ShipmentStatusList": status,
                    "QueryType": "SHIPMENT",
                },
                timeout=15,
            )
            r.raise_for_status()
            shipments = r.json().get("payload", {}).get("ShipmentData", [])
            for s in shipments:
                all_shipments.append({
                    "shipment_id":   s.get("ShipmentId"),
                    "name":          s.get("ShipmentName"),
                    "status":        s.get("ShipmentStatus"),
                    "destination_fc": s.get("DestinationFulfillmentCenterId"),
                    "created":       s.get("CreatedDate"),
                    "last_updated":  s.get("LastUpdatedDate"),
                    "items_count":   len(s.get("Items", [])),
                })
        except Exception as e:
            log.warning(f"  Shipment status {status} fetch failed: {e}")

    active = [s for s in all_shipments if s["status"] in ["WORKING", "SHIPPED", "IN_TRANSIT"]]
    receiving = [s for s in all_shipments if s["status"] in ["RECEIVING", "CHECKED_IN"]]
    problems = [s for s in all_shipments if s["status"] == "CANCELLED"]

    return {
        "total_shipments":    len(all_shipments),
        "active_shipments":   len(active),
        "receiving_count":    len(receiving),
        "cancelled_count":    len(problems),
        "shipments":          all_shipments,
    }

# ═══════════════════════════════════════════════════════════════════════════════
# 13. AMAZON ADS PPC  (REPORT ONLY — no automatic changes)
# ═══════════════════════════════════════════════════════════════════════════════
def fetch_ppc_summary(account, date_str):
    """
    Fetch all 5 PPC report types. REPORT ONLY — agent surfaces findings,
    no bids, budgets, or targeting are changed automatically.
    """
    token      = get_ads_token(account)
    profile_id = account["ads_profile_id"]

    REPORT_TYPES = ["spCampaigns", "spSearchTerm", "spTargeting", "spAdvertisedProduct", "spPlacement"]
    METRICS = {
        "spCampaigns":        ["impressions","clicks","cost","purchases1d","sales1d","costPerClick","clickThroughRate","returnOnAdSpend"],
        "spSearchTerm":       ["impressions","clicks","cost","purchases1d","sales1d","searchTermImpressionRank"],
        "spTargeting":        ["impressions","clicks","cost","purchases1d","sales1d"],
        "spAdvertisedProduct":["impressions","clicks","cost","purchases1d","sales1d","advertisedAsin"],
        "spPlacement":        ["impressions","clicks","cost","purchases1d","sales1d","placementClassification"],
    }

    report_ids = {}
    for rt in REPORT_TYPES:
        try:
            body = {
                "name": f"{rt}_{date_str}",
                "startDate": date_str, "endDate": date_str,
                "configuration": {
                    "adProduct": "SPONSORED_PRODUCTS",
                    "groupBy":   ["campaign"] if "Campaign" in rt else ["targeting"],
                    "columns":   METRICS[rt],
                    "reportTypeId": rt,
                    "timeUnit": "SUMMARY",
                    "format":   "GZIP_JSON",
                },
            }
            r = requests.post(f"{ADS_API_BASE}/reporting/reports",
                              headers=ads_headers(token, account), json=body, timeout=15)
            r.raise_for_status()
            report_ids[rt] = r.json()["reportId"]
            log.info(f"    Requested {rt}: {report_ids[rt]}")
        except Exception as e:
            log.warning(f"    Could not request {rt}: {e}")

    raw_data = {}
    for rt, rid in report_ids.items():
        try:
            waited = 0
            while waited < 120:
                r = requests.get(f"{ADS_API_BASE}/reporting/reports/{rid}",
                                 headers=ads_headers(token, account), timeout=10)
                r.raise_for_status()
                d = r.json()
                if d.get("status") == "COMPLETED":
                    dl = requests.get(d["url"], timeout=30)
                    dl.raise_for_status()
                    with gzip.open(BytesIO(dl.content), "rt", encoding="utf-8") as f:
                        raw_data[rt] = json.load(f)
                    log.info(f"    Downloaded {rt}: {len(raw_data[rt])} rows")
                    break
                if d.get("status") == "FAILED":
                    raise RuntimeError(f"Report failed: {d}")
                time.sleep(10); waited += 10
        except Exception as e:
            log.warning(f"    {rt} download failed: {e}")
            raw_data[rt] = []

    cams = raw_data.get("spCampaigns", [])
    spend  = sum(float(r.get("cost",0))        for r in cams)
    sales  = sum(float(r.get("sales1d",0))     for r in cams)
    clicks = sum(int(r.get("clicks",0))        for r in cams)
    imps   = sum(int(r.get("impressions",0))   for r in cams)
    orders = sum(int(r.get("purchases1d",0))   for r in cams)
    acos   = round(spend/sales*100,2)  if sales  > 0 else 0
    roas   = round(sales/spend,2)      if spend  > 0 else 0
    ctr    = round(clicks/imps*100,2)  if imps   > 0 else 0
    cpc    = round(spend/clicks,2)     if clicks > 0 else 0

    top_terms = sorted(raw_data.get("spSearchTerm",[]),
                       key=lambda r: float(r.get("cost",0)), reverse=True)[:10]
    top_cams  = sorted(cams, key=lambda r: float(r.get("cost",0)), reverse=True)[:10]

    placements = {
        r.get("placementClassification","unknown"): {
            "spend":  float(r.get("cost",0)),
            "sales":  float(r.get("sales1d",0)),
            "clicks": int(r.get("clicks",0)),
        }
        for r in raw_data.get("spPlacement",[])
    }

    return {
        "report_only": True,
        "summary": {
            "spend": round(spend,2), "sales": round(sales,2),
            "clicks": clicks, "impressions": imps, "orders": orders,
            "acos": acos, "roas": roas, "ctr": ctr, "cpc": cpc,
        },
        "top_campaigns": [
            {"name": r.get("campaignName","N/A"),
             "spend": float(r.get("cost",0)), "sales": float(r.get("sales1d",0)),
             "acos": round(float(r.get("cost",0))/float(r.get("sales1d",1))*100,2) if float(r.get("sales1d",0))>0 else 0,
             "clicks": int(r.get("clicks",0))}
            for r in top_cams
        ],
        "top_search_terms": [
            {"term":   r.get("searchTerm", r.get("keywordText","N/A")),
             "spend":  float(r.get("cost",0)),
             "sales":  float(r.get("sales1d",0)),
             "clicks": int(r.get("clicks",0))}
            for r in top_terms
        ],
        "placements":       placements,
        "campaign_count":   len(cams),
    }

# ═══════════════════════════════════════════════════════════════════════════════
# ALERTS BUILDER
# ═══════════════════════════════════════════════════════════════════════════════
def build_alerts(result):
    alerts = []
    # Reorder alerts (90-day threshold)
    for item in result.get("inventory", {}).get("asins", []):
        if item.get("reorder_alert"):
            alerts.append({
                "type": "reorder", "source": "inventory",
                "sku": item["sku"], "asin": item["asin"],
                "dos": item["days_of_supply"],
                "message": f"⚠️ Reorder needed: {item['sku']} — {item['days_of_supply']} days of supply (< 90 days)",
            })
    # Stranded inventory
    if result.get("stranded_inventory", {}).get("total_stranded_units", 0) > 0:
        alerts.append({
            "type": "stranded_inventory", "source": "inventory",
            "units": result["stranded_inventory"]["total_stranded_units"],
            "count": result["stranded_inventory"]["stranded_count"],
            "message": f"⚠️ {result['stranded_inventory']['total_stranded_units']} stranded units across {result['stranded_inventory']['stranded_count']} SKUs",
        })
    # PPC high ACoS (human review — no auto changes)
    ppc = result.get("ppc", {}).get("summary", {})
    if ppc.get("acos", 0) > 40:
        alerts.append({
            "type": "high_acos", "source": "ppc", "acos": ppc["acos"],
            "action_required": "manual",
            "message": f"⚠️ High ACoS: {ppc['acos']}% — human review needed (no automatic changes made)",
        })
    # Creator Connections unread
    cc = result.get("creator_connections", {})
    if cc.get("unread_count", 0) > 0:
        alerts.append({
            "type": "creator_connections", "source": "creator_connections",
            "count": cc["unread_count"],
            "message": f"📩 {cc['unread_count']} unread Creator Connection message(s)",
        })
    # Open support cases
    cases = result.get("support_cases", {})
    if cases.get("open_cases", 0) > 0:
        alerts.append({
            "type": "open_cases", "source": "support_cases",
            "count": cases["open_cases"],
            "message": f"🎫 {cases['open_cases']} open Seller Support case(s)",
        })
    # Active shipments
    ships = result.get("shipments", {})
    if ships.get("active_shipments", 0) > 0:
        alerts.append({
            "type": "active_shipments", "source": "shipments",
            "count": ships["active_shipments"],
            "message": f"🚚 {ships['active_shipments']} active inbound shipment(s)",
        })
    return alerts

# ═══════════════════════════════════════════════════════════════════════════════
# EXCEL REPORT BUILDER
# ═══════════════════════════════════════════════════════════════════════════════
def _hdr(ws, row, col, text, bg="1E3A5F", fg="FFFFFF", bold=True, size=11):
    cell = ws.cell(row=row, column=col, value=text)
    cell.font = Font(bold=bold, color=fg, size=size)
    cell.fill = PatternFill("solid", fgColor=bg)
    cell.alignment = Alignment(horizontal="center", vertical="center", wrap_text=True)
    return cell

def _row(ws, row, cols_data, alt=False):
    bg = "EBF2FA" if alt else "FFFFFF"
    for col, val in enumerate(cols_data, 1):
        cell = ws.cell(row=row, column=col, value=val)
        cell.fill = PatternFill("solid", fgColor=bg)
        cell.alignment = Alignment(vertical="center")
    return ws

def _section(ws, row, text):
    cell = ws.cell(row=row, column=1, value=text)
    cell.font = Font(bold=True, size=12, color="1E3A5F")
    cell.fill = PatternFill("solid", fgColor="D6E4F0")
    ws.merge_cells(start_row=row, start_column=1, end_row=row, end_column=8)
    cell.alignment = Alignment(horizontal="left", vertical="center")
    return row + 1

def _autofit(ws, min_width=12, max_width=40):
    for col in ws.columns:
        max_len = max((len(str(c.value or "")) for c in col), default=10)
        ws.column_dimensions[get_column_letter(col[0].column)].width = min(max(max_len + 2, min_width), max_width)

def build_excel_report(account_id, date_str, result, filename):
    wb = openpyxl.Workbook()
    label = result.get("account_name", account_id)

    # ── Sheet 1: Summary ──────────────────────────────────────────────────────
    ws = wb.active
    ws.title = "Summary"
    ws.row_dimensions[1].height = 30
    ws.merge_cells("A1:H1")
    title_cell = ws["A1"]
    title_cell.value = f"FBA Bros — {label} Daily Report   |   {date_str}"
    title_cell.font = Font(bold=True, size=14, color="FFFFFF")
    title_cell.fill = PatternFill("solid", fgColor="1E3A5F")
    title_cell.alignment = Alignment(horizontal="center", vertical="center")

    r = 3
    # Alerts
    alerts = result.get("alerts", [])
    r = _section(ws, r, f"🚨  ALERTS  ({len(alerts)} total)")
    _hdr(ws, r, 1, "Type"); _hdr(ws, r, 2, "Source"); _hdr(ws, r, 3, "Message", bg="1E3A5F")
    ws.merge_cells(start_row=r, start_column=3, end_row=r, end_column=8)
    r += 1
    for i, alert in enumerate(alerts):
        ws.cell(row=r, column=1, value=alert.get("type",""))
        ws.cell(row=r, column=2, value=alert.get("source",""))
        c = ws.cell(row=r, column=3, value=alert.get("message",""))
        c.alignment = Alignment(wrap_text=True)
        ws.merge_cells(start_row=r, start_column=3, end_row=r, end_column=8)
        if i % 2 == 0:
            for col in range(1, 9):
                ws.cell(row=r, column=col).fill = PatternFill("solid", fgColor="FFF3CD")
        r += 1

    r += 1
    # Sales summary
    r = _section(ws, r, "💰  SALES")
    sales = result.get("sales", {})
    _hdr(ws, r, 1, "Revenue"); _hdr(ws, r, 2, "Units"); _hdr(ws, r, 3, "Orders"); _hdr(ws, r, 4, "Currency")
    r += 1
    ws.cell(row=r, column=1, value=f"${sales.get('revenue',0):,.2f}")
    ws.cell(row=r, column=2, value=sales.get("units",0))
    ws.cell(row=r, column=3, value=sales.get("order_count",0))
    ws.cell(row=r, column=4, value=sales.get("currency","USD"))
    r += 2

    # PPC summary
    r = _section(ws, r, "📊  PPC SUMMARY  (Report Only — No Automatic Changes)")
    ppc = result.get("ppc", {}).get("summary", {})
    headers = ["Spend", "Sales", "ACoS %", "ROAS", "Clicks", "Impressions", "Orders", "CPC"]
    for col, h in enumerate(headers, 1):
        _hdr(ws, r, col, h)
    r += 1
    vals = [f"${ppc.get('spend',0):,.2f}", f"${ppc.get('sales',0):,.2f}",
            f"{ppc.get('acos',0)}%", ppc.get("roas",0),
            ppc.get("clicks",0), ppc.get("impressions",0),
            ppc.get("orders",0), f"${ppc.get('cpc',0):.2f}"]
    for col, v in enumerate(vals, 1):
        ws.cell(row=r, column=col, value=v)
    r += 2

    # Inventory summary
    r = _section(ws, r, "📦  INVENTORY")
    inv = result.get("inventory", {})
    _hdr(ws, r, 1, "Total Units"); _hdr(ws, r, 2, "Reorder Alerts (< 90 days)")
    _hdr(ws, r, 3, "Stranded Units"); _hdr(ws, r, 4, "AWD Units")
    r += 1
    ws.cell(row=r, column=1, value=inv.get("total_units",0))
    ws.cell(row=r, column=2, value=inv.get("reorder_count",0))
    ws.cell(row=r, column=3, value=result.get("stranded_inventory",{}).get("total_stranded_units",0))
    ws.cell(row=r, column=4, value=result.get("awd_stock",{}).get("total_awd_units",0))
    r += 2

    # Account health
    r = _section(ws, r, "🏥  ACCOUNT HEALTH")
    health = result.get("account_health", {})
    _hdr(ws, r, 1, "Status"); _hdr(ws, r, 2, "Score"); _hdr(ws, r, 3, "Notes")
    ws.merge_cells(start_row=r, start_column=3, end_row=r, end_column=8)
    r += 1
    ws.cell(row=r, column=1, value=health.get("health_status","Unknown"))
    ws.cell(row=r, column=2, value=health.get("health_score","N/A"))
    n = ws.cell(row=r, column=3, value=health.get("note",""))
    n.alignment = Alignment(wrap_text=True)
    ws.merge_cells(start_row=r, start_column=3, end_row=r, end_column=8)

    _autofit(ws)
    ws.freeze_panes = "A3"

    # ── Sheet 2: Inventory Detail ─────────────────────────────────────────────
    ws2 = wb.create_sheet("Inventory")
    ws2.merge_cells("A1:F1")
    c = ws2["A1"]
    c.value = f"{label} — FBA Inventory Detail — {date_str}"
    c.font = Font(bold=True, size=13, color="FFFFFF")
    c.fill = PatternFill("solid", fgColor="1E3A5F")
    c.alignment = Alignment(horizontal="center", vertical="center")
    headers = ["ASIN", "SKU", "Units", "Days of Supply", "Reorder Alert", "Status"]
    for col, h in enumerate(headers, 1):
        _hdr(ws2, 2, col, h)
    for i, item in enumerate(inv.get("asins", []), 1):
        row_data = [
            item.get("asin",""), item.get("sku",""), item.get("units",0),
            item.get("days_of_supply","N/A"),
            "⚠️ YES" if item.get("reorder_alert") else "✅ OK",
            "Reorder Now" if item.get("reorder_alert") else "Good",
        ]
        _row(ws2, i+2, row_data, alt=(i % 2 == 0))
        if item.get("reorder_alert"):
            ws2.cell(row=i+2, column=5).fill = PatternFill("solid", fgColor="FFB3B3")
    _autofit(ws2)

    # ── Sheet 3: Stranded Inventory ───────────────────────────────────────────
    ws3 = wb.create_sheet("Stranded Inventory")
    ws3.merge_cells("A1:E1")
    c = ws3["A1"]
    c.value = f"{label} — Stranded Inventory — {date_str}"
    c.font = Font(bold=True, size=13, color="FFFFFF")
    c.fill = PatternFill("solid", fgColor="C0392B")
    c.alignment = Alignment(horizontal="center", vertical="center")
    headers = ["ASIN", "SKU", "Stranded Units", "Reason", "Action Required"]
    for col, h in enumerate(headers, 1):
        _hdr(ws3, 2, col, h, bg="C0392B")
    stranded = result.get("stranded_inventory", {}).get("items", [])
    for i, item in enumerate(stranded, 1):
        _row(ws3, i+2, [
            item.get("asin",""), item.get("sku",""),
            item.get("stranded_units",0), item.get("reason","Unknown"),
            "Fix listing or remove from FBA"
        ], alt=(i % 2 == 0))
    if not stranded:
        ws3.cell(row=3, column=1, value="✅ No stranded inventory")
    _autofit(ws3)

    # ── Sheet 4: PPC Campaigns ───────────────────────────────────────────────
    ws4 = wb.create_sheet("PPC Campaigns")
    ws4.merge_cells("A1:F1")
    c = ws4["A1"]
    c.value = f"{label} — PPC Campaign Report — {date_str}   (REPORT ONLY — No Automatic Changes)"
    c.font = Font(bold=True, size=12, color="FFFFFF")
    c.fill = PatternFill("solid", fgColor="7D3C98")
    c.alignment = Alignment(horizontal="center", vertical="center")
    headers = ["Campaign Name", "Spend", "Sales", "ACoS %", "Clicks", "Review Action"]
    for col, h in enumerate(headers, 1):
        _hdr(ws4, 2, col, h, bg="7D3C98")
    for i, cam in enumerate(result.get("ppc", {}).get("top_campaigns", []), 1):
        action = "⚠️ Reduce bids" if cam.get("acos",0) > 40 else "✅ Monitor"
        _row(ws4, i+2, [
            cam.get("name","N/A"),
            f"${cam.get('spend',0):,.2f}",
            f"${cam.get('sales',0):,.2f}",
            f"{cam.get('acos',0)}%",
            cam.get("clicks",0),
            action,
        ], alt=(i % 2 == 0))
        if cam.get("acos",0) > 40:
            ws4.cell(row=i+2, column=6).fill = PatternFill("solid", fgColor="FFB3B3")
    _autofit(ws4)

    # ── Sheet 5: Search Terms ─────────────────────────────────────────────────
    ws5 = wb.create_sheet("Top Search Terms")
    ws5.merge_cells("A1:E1")
    c = ws5["A1"]
    c.value = f"{label} — Top 10 Search Terms — {date_str}"
    c.font = Font(bold=True, size=12, color="FFFFFF")
    c.fill = PatternFill("solid", fgColor="7D3C98")
    c.alignment = Alignment(horizontal="center", vertical="center")
    for col, h in enumerate(["Search Term","Spend","Sales","Clicks","ACoS %"], 1):
        _hdr(ws5, 2, col, h, bg="7D3C98")
    for i, term in enumerate(result.get("ppc", {}).get("top_search_terms", []), 1):
        spend = term.get("spend",0); sales = term.get("sales",0)
        acos = round(spend/sales*100,1) if sales > 0 else 0
        _row(ws5, i+2, [
            term.get("term","N/A"), f"${spend:,.2f}", f"${sales:,.2f}",
            term.get("clicks",0), f"{acos}%"
        ], alt=(i % 2 == 0))
    _autofit(ws5)

    # ── Sheet 6: Shipments ────────────────────────────────────────────────────
    ws6 = wb.create_sheet("Shipments")
    ws6.merge_cells("A1:F1")
    c = ws6["A1"]
    c.value = f"{label} — FBA Inbound Shipments — {date_str}"
    c.font = Font(bold=True, size=12, color="FFFFFF")
    c.fill = PatternFill("solid", fgColor="117A65")
    c.alignment = Alignment(horizontal="center", vertical="center")
    for col, h in enumerate(["Shipment ID","Name","Status","FC","Created","Last Updated"], 1):
        _hdr(ws6, 2, col, h, bg="117A65")
    for i, s in enumerate(result.get("shipments", {}).get("shipments", []), 1):
        _row(ws6, i+2, [
            s.get("shipment_id",""), s.get("name",""),
            s.get("status",""), s.get("destination_fc",""),
            s.get("created",""), s.get("last_updated",""),
        ], alt=(i % 2 == 0))
    _autofit(ws6)

    # ── Sheet 7: Support Cases ────────────────────────────────────────────────
    ws7 = wb.create_sheet("Support Cases")
    ws7.merge_cells("A1:E1")
    c = ws7["A1"]
    c.value = f"{label} — Open Seller Support Cases — {date_str}"
    c.font = Font(bold=True, size=12, color="FFFFFF")
    c.fill = PatternFill("solid", fgColor="E74C3C")
    c.alignment = Alignment(horizontal="center", vertical="center")
    for col, h in enumerate(["Case ID","Subject","Status","Category","Last Updated"], 1):
        _hdr(ws7, 2, col, h, bg="E74C3C")
    for i, case in enumerate(result.get("support_cases", {}).get("cases", []), 1):
        _row(ws7, i+2, [
            case.get("case_id",""), case.get("subject",""),
            case.get("status",""), case.get("category",""),
            case.get("last_update",""),
        ], alt=(i % 2 == 0))
    if not result.get("support_cases", {}).get("cases"):
        ws7.cell(row=3, column=1, value="✅ No open support cases")
    _autofit(ws7)

    # ── Sheet 8: Creator Connections ──────────────────────────────────────────
    ws8 = wb.create_sheet("Creator Connections")
    ws8.merge_cells("A1:E1")
    c = ws8["A1"]
    c.value = f"{label} — Creator Connections — {date_str}"
    c.font = Font(bold=True, size=12, color="FFFFFF")
    c.fill = PatternFill("solid", fgColor="F39C12")
    c.alignment = Alignment(horizontal="center", vertical="center")
    for col, h in enumerate(["Conversation ID","Creator","Subject","Received","Status"], 1):
        _hdr(ws8, 2, col, h, bg="F39C12")
    for i, conv in enumerate(result.get("creator_connections", {}).get("conversations", []), 1):
        _row(ws8, i+2, [
            conv.get("id",""), conv.get("creator",""),
            conv.get("subject",""), conv.get("received",""), conv.get("status",""),
        ], alt=(i % 2 == 0))
    if not result.get("creator_connections", {}).get("conversations"):
        ws8.cell(row=3, column=1, value="✅ No unread Creator Connection messages")
    _autofit(ws8)

    # ── Sheet 9: Feedback & Reviews ───────────────────────────────────────────
    ws9 = wb.create_sheet("Feedback & Reviews")
    ws9.merge_cells("A1:F1")
    c = ws9["A1"]
    c.value = f"{label} — Seller Feedback & Reviews — {date_str}"
    c.font = Font(bold=True, size=12, color="FFFFFF")
    c.fill = PatternFill("solid", fgColor="2E86C1")
    c.alignment = Alignment(horizontal="center", vertical="center")
    feedback = result.get("seller_feedback", {})
    ws9.cell(row=3, column=1, value="30-Day Rating:").font = Font(bold=True)
    ws9.cell(row=3, column=2, value=feedback.get("rating_30d","N/A"))
    ws9.cell(row=4, column=1, value="90-Day Rating:").font = Font(bold=True)
    ws9.cell(row=4, column=2, value=feedback.get("rating_90d","N/A"))
    ws9.cell(row=5, column=1, value="Negative (30d):").font = Font(bold=True)
    ws9.cell(row=5, column=2, value=feedback.get("negative_30d","N/A"))
    ws9.cell(row=6, column=1, value="Neutral (30d):").font = Font(bold=True)
    ws9.cell(row=6, column=2, value=feedback.get("neutral_30d","N/A"))
    _autofit(ws9)

    wb.save(filename)
    log.info(f"  ✅ Excel report saved: {filename}")
    return filename

# ═══════════════════════════════════════════════════════════════════════════════
# MAIN ORCHESTRATOR
# ═══════════════════════════════════════════════════════════════════════════════
def run_for_account(account_id, date_str):
    account = ACCOUNTS.get(account_id)
    if not account:
        log.error(f"Unknown account: {account_id}")
        return

    label = account["label"]
    log.info(f"\n{'━'*60}")
    log.info(f"  Starting full report: {label} | {date_str}")
    log.info(f"{'━'*60}")

    result = {
        "account_id":   account_id,
        "account_name": label,
        "date":         date_str,
        "fetched_at":   datetime.utcnow().isoformat(),
    }

    sections = [
        ("sales",                   "💰 Sales",                    fetch_sales,                 [account, date_str]),
        ("inventory",               "📦 FBA Inventory",            fetch_inventory,             [account]),
        ("stranded_inventory",      "🚫 Stranded Inventory",       fetch_stranded_inventory,    [account]),
        ("awd_stock",               "🏭 AWD Stock",                fetch_awd_stock,             [account]),
        ("account_health",          "🏥 Account Health",           fetch_account_health,        [account]),
        ("performance_notifications","🔔 Performance Notifications",fetch_performance_notifications,[account]),
        ("voice_of_customer",       "🗣️  Voice of Customer",       fetch_voice_of_customer,     [account]),
        ("seller_feedback",         "⭐ Seller Feedback",           fetch_seller_feedback,       [account]),
        ("customer_reviews",        "💬 Customer Reviews",         fetch_customer_reviews,      [account]),
        ("support_cases",           "🎫 Support Cases",            fetch_support_cases,         [account]),
        ("creator_connections",     "📩 Creator Connections",      fetch_creator_connections,   [account]),
        ("shipments",               "🚚 Shipments",                fetch_shipments,             [account]),
        ("ppc",                     "📊 PPC (Report Only)",        fetch_ppc_summary,           [account, date_str]),
    ]

    for key, label_str, fn, args in sections:
        log.info(f"  Fetching {label_str}...")
        try:
            result[key] = fn(*args)
        except Exception as e:
            log.error(f"  ❌ {label_str} failed: {e}")
            result[key] = {"error": str(e)}

    result["alerts"] = build_alerts(result)
    log.info(f"\n  📋 Total alerts: {len(result['alerts'])}")
    for alert in result["alerts"]:
        log.info(f"     {alert['message']}")

    # Save JSON
    json_file = f"report_{account_id}_{date_str}.json"
    with open(json_file, "w") as f:
        json.dump(result, f, indent=2, default=str)
    log.info(f"  ✅ JSON saved: {json_file}")

    # Save Excel
    xlsx_file = f"report_{account_id}_{date_str}.xlsx"
    build_excel_report(account_id, date_str, result, xlsx_file)

    # Push to Firebase
    try:
        push_to_firebase(account_id, date_str, result)
    except Exception as e:
        log.error(f"  Firebase push failed: {e}")

    log.info(f"{'━'*60}")
    log.info(f"  ✅ Complete: {label} | Files: {json_file}, {xlsx_file}")
    log.info(f"{'━'*60}\n")
    return result, json_file, xlsx_file


def run_all_accounts(date_str):
    log.info(f"\n🚀 Running full account report for ALL accounts — {date_str}\n")
    for account_id in ACCOUNTS:
        run_for_account(account_id, date_str)
    log.info("✅ All accounts complete.")


def scheduled_job():
    yesterday = (datetime.utcnow() - timedelta(days=1)).strftime("%Y-%m-%d")
    run_all_accounts(yesterday)


def run_daemon():
    log.info("🕐 Daemon mode: runs every day at 01:00 UTC (06:00 PKT)")
    schedule.every().day.at("01:00").do(scheduled_job)
    while True:
        schedule.run_pending()
        time.sleep(30)


# ═══════════════════════════════════════════════════════════════════════════════
# CLI
# ═══════════════════════════════════════════════════════════════════════════════
if __name__ == "__main__":
    p = argparse.ArgumentParser(description="FBA Bros Full Account Health Reporter")
    g = p.add_mutually_exclusive_group(required=True)
    g.add_argument("--run-once", action="store_true")
    g.add_argument("--daemon",   action="store_true")
    p.add_argument("--account", type=str, default=None,
                   help=f"Account to run. Options: {list(ACCOUNTS.keys())}")
    p.add_argument("--date", type=str, default=None,
                   help="Date YYYY-MM-DD (default: yesterday)")
    args = p.parse_args()

    init_firebase()

    if args.daemon:
        run_daemon()
    else:
        date_str = args.date or (datetime.utcnow() - timedelta(days=1)).strftime("%Y-%m-%d")
        if args.account:
            run_for_account(args.account, date_str)
        else:
            run_all_accounts(date_str)
