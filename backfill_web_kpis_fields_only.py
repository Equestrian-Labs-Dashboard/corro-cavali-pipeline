"""
Backfill ONLY Website KPI fields in kpis_daily.

This does NOT clear sheets and does NOT rewrite financial data.
It only fills/updates these columns on existing kpis_daily rows:
  - pageviews
  - sessions_reached_checkout
  - sessions_completed_checkout
  - checkout_abandonments
  - checkout_abandonment_rate

Optional:
  - it will create missing columns at the end of kpis_daily header.
  - it can also update conversion_rate if UPDATE_CONVERSION_RATE=true.

Run in GitHub Actions manually:
  python -u backfill_web_kpis_fields_only.py

Env already used by the main pipeline:
  SHOPIFY_TOKEN_CORRO
  SHOPIFY_TOKEN_CAVALI
  GOOGLE_CREDENTIALS

Optional env:
  RUN_BRANDS=corro,cavali
  START_DATE=2024-01-01
  END_DATE=2026-06-30
  UPDATE_CONVERSION_RATE=false
"""

import os
import json
import time
import random
import requests
import gspread
from datetime import datetime, date
from google.oauth2.service_account import Credentials

GQL_VERSION = "2025-10"
SCOPES = ["https://www.googleapis.com/auth/spreadsheets"]

STORES = {
    "corro": {
        "url": os.environ.get("SHOPIFY_URL_CORRO", "equestrian-labs.myshopify.com"),
        "token": os.environ["SHOPIFY_TOKEN_CORRO"],
        "sheet_id": os.environ.get("SHEET_ID_CORRO", "1nq8xkDzowAvhD3wpMBlVK2M3FZSNS2DrAiPxz-Y2tdU"),
    },
    "cavali": {
        "url": os.environ.get("SHOPIFY_URL_CAVALI", "cavali-club.myshopify.com"),
        "token": os.environ["SHOPIFY_TOKEN_CAVALI"],
        "sheet_id": os.environ.get("SHEET_ID_CAVALI", "1QUdJc2EIdElIX5nlLQxWxS98aAz-TgQnSg9glJpNtig"),
    },
}

TARGET_COLUMNS = [
    "pageviews",
    "sessions_reached_checkout",
    "sessions_completed_checkout",
    "checkout_abandonments",
    "checkout_abandonment_rate",
]

OPTIONAL_COLUMNS = ["conversion_rate"]

def parse_date(s: str) -> date:
    return date.fromisoformat(str(s)[:10])

def money(v) -> float:
    try:
        return float(str(v or 0).replace(",", "").replace("%", "").strip())
    except Exception:
        return 0.0

def gm_ratio(v) -> float:
    """Return percent points: 0.51 -> 51, 51 -> 51."""
    val = money(v)
    return round(val * 100, 2) if abs(val) <= 1.0 else round(val, 2)

def gql(store_url, token, query, variables):
    url = f"https://{store_url}/admin/api/{GQL_VERSION}/graphql.json"
    payload = {"query": query, "variables": variables}
    headers = {"X-Shopify-Access-Token": token, "Content-Type": "application/json"}

    for attempt in range(8):
        r = requests.post(url, headers=headers, json=payload, timeout=90)
        if r.status_code in (429, 500, 502, 503, 504):
            sleep_for = min(90, 4 + attempt * 5 + random.random())
            print(f"    Shopify HTTP {r.status_code}; retry {attempt+1}/8 in {sleep_for:.1f}s")
            time.sleep(sleep_for)
            continue

        r.raise_for_status()
        data = r.json()
        errors = data.get("errors") or []
        if errors:
            msg = json.dumps(errors)[:500]
            if "THROTTLED" in msg.upper() and attempt < 7:
                sleep_for = min(90, 8 + attempt * 8 + random.random())
                print(f"    Shopify GraphQL throttled; retry {attempt+1}/8 in {sleep_for:.1f}s")
                time.sleep(sleep_for)
                continue
            raise RuntimeError(f"Shopify GraphQL errors: {msg}")
        return data

    raise RuntimeError("Shopify GraphQL failed after retries")

def parse_shopifyql(data):
    q = data.get("data", {}).get("shopifyqlQuery", {})
    if q.get("parseErrors"):
        raise RuntimeError(f"ShopifyQL parse errors: {q.get('parseErrors')}")
    table = q.get("tableData") or {}
    cols = table.get("columns") or []
    col_names = [(c.get("name") or c.get("displayName") or f"col_{i}") for i,c in enumerate(cols)]
    rows = table.get("rows")
    if rows is None:
        rows = table.get("rowData") or []
    out = []
    for row in rows or []:
        if isinstance(row, dict):
            out.append(row)
        else:
            out.append({col_names[i] if i < len(col_names) else f"col_{i}": v for i, v in enumerate(row)})
    return out

def run_shopifyql(store_url, token, shopifyql):
    query = """
    query ShopifyQL($query: String!) {
      shopifyqlQuery(query: $query) {
        tableData {
          columns { name displayName dataType }
          rows
        }
        parseErrors { code message }
      }
    }
    """
    data = gql(store_url, token, query, {"query": shopifyql})
    return parse_shopifyql(data)

def pick(row, *names):
    normalized = {str(k).lower().replace(" ", "_"): v for k, v in row.items()}
    for name in names:
        if name in row and row.get(name) is not None:
            return row.get(name)
        key = str(name).lower().replace(" ", "_")
        if key in normalized and normalized[key] is not None:
            return normalized[key]
    return None

def fetch_web_fields(store_url, token, start, end):
    # Main Website KPI fields.
    web_rows = run_shopifyql(
        store_url,
        token,
        f"""
        FROM sessions
        SHOW sessions, online_store_visitors, pageviews, conversion_rate
        SINCE {start}
        UNTIL {end}
        """
    )
    web = web_rows[0] if web_rows else {}

    # Checkout funnel fields. Separate query so pageviews still backfill if funnel fields fail.
    funnel = {}
    try:
        funnel_rows = run_shopifyql(
            store_url,
            token,
            f"""
            FROM sessions
            SHOW sessions_that_reached_checkout, sessions_that_reached_and_completed_checkout
            SINCE {start}
            UNTIL {end}
            """
        )
        funnel = funnel_rows[0] if funnel_rows else {}
    except Exception as exc:
        print(f"    ⚠ checkout funnel unavailable {start} → {end}: {exc}")

    pageviews = int(abs(money(pick(web, "pageviews", "Pageviews"))))
    conversion_rate = gm_ratio(pick(web, "conversion_rate", "Conversion rate"))

    reached = int(abs(money(pick(
        funnel,
        "sessions_that_reached_checkout",
        "Sessions that reached checkout",
        "Reached checkout",
    ))))
    completed = int(abs(money(pick(
        funnel,
        "sessions_that_reached_and_completed_checkout",
        "Sessions that reached and completed checkout",
        "Completed checkout",
    ))))
    abandoned = max(reached - completed, 0)
    abandonment_rate = round(abandoned / reached * 100, 2) if reached else 0

    return {
        "pageviews": pageviews,
        "conversion_rate": conversion_rate,
        "sessions_reached_checkout": reached,
        "sessions_completed_checkout": completed,
        "checkout_abandonments": abandoned,
        "checkout_abandonment_rate": abandonment_rate,
    }

def get_gc():
    creds = Credentials.from_service_account_info(
        json.loads(os.environ["GOOGLE_CREDENTIALS"]),
        scopes=SCOPES,
    )
    return gspread.authorize(creds)

def ensure_headers(ws, headers):
    existing = ws.row_values(1)
    changed = False
    for col in headers:
        if col not in existing:
            existing.append(col)
            changed = True
    if changed:
        ws.update("1:1", [existing])
    return existing

def col_letter(n):
    s = ""
    while n:
        n, r = divmod(n - 1, 26)
        s = chr(65 + r) + s
    return s

def backfill_brand(brand, store):
    print(f"\n============================================================")
    print(f"  {brand.upper()} — web KPI fields only")
    print(f"============================================================")

    gc = get_gc()
    sh = gc.open_by_key(store["sheet_id"])
    ws = sh.worksheet("kpis_daily")

    headers = ensure_headers(ws, TARGET_COLUMNS + OPTIONAL_COLUMNS)
    hmap = {h: i+1 for i, h in enumerate(headers)}
    values = ws.get_all_values()
    rows = values[1:]

    start_limit = parse_date(os.environ.get("START_DATE", "2024-01-01"))
    end_limit = parse_date(os.environ.get("END_DATE", date.today().isoformat()))
    update_cr = os.environ.get("UPDATE_CONVERSION_RATE", "false").lower() in ("1", "true", "yes", "y")

    updates = []
    touched = 0
    skipped = 0

    for idx, raw in enumerate(rows, start=2):
        row = {headers[i]: raw[i] if i < len(raw) else "" for i in range(len(headers))}
        ps = row.get("period_start")
        pe = row.get("period_end")
        period = row.get("period")
        if not ps or not pe:
            skipped += 1
            continue
        try:
            s = parse_date(ps)
            e = parse_date(pe)
        except Exception:
            skipped += 1
            continue

        if e < start_limit or s > end_limit:
            skipped += 1
            continue

        # Only existing dashboard periods; no clearing, no adding rows.
        try:
            metrics = fetch_web_fields(store["url"], store["token"], s.isoformat(), e.isoformat())
        except Exception as exc:
            print(f"    ⚠ failed {period} {s} → {e}: {exc}")
            continue

        row_updates = []
        for col in TARGET_COLUMNS:
            c = hmap[col]
            row_updates.append({
                "range": f"{col_letter(c)}{idx}",
                "values": [[metrics[col]]],
            })
        if update_cr and "conversion_rate" in hmap:
            c = hmap["conversion_rate"]
            row_updates.append({
                "range": f"{col_letter(c)}{idx}",
                "values": [[metrics["conversion_rate"]]],
            })

        updates.extend(row_updates)
        touched += 1
        print(
            f"    {period:24} {s} → {e}  "
            f"pageviews={metrics['pageviews']:,}  "
            f"checkout_abandonment={metrics['checkout_abandonment_rate']:.2f}%"
        )

        # Flush in chunks to avoid very large requests.
        if len(updates) >= 250:
            ws.batch_update(updates, value_input_option="USER_ENTERED")
            updates = []
            time.sleep(1)

    if updates:
        ws.batch_update(updates, value_input_option="USER_ENTERED")

    print(f"  ✓ {brand.upper()} updated rows: {touched}; skipped: {skipped}")

def main():
    brands = [b.strip().lower() for b in os.environ.get("RUN_BRANDS", "corro,cavali").split(",") if b.strip()]
    for brand in brands:
        if brand not in STORES:
            print(f"Skipping unknown brand: {brand}")
            continue
        backfill_brand(brand, STORES[brand])

if __name__ == "__main__":
    main()
