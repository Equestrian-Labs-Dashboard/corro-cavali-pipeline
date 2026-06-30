import os
import json
import time
from datetime import datetime, date
import requests
import gspread
from google.oauth2.service_account import Credentials

SCOPES = ["https://www.googleapis.com/auth/spreadsheets"]
API_VERSION = "2024-10"

STORES = {
    "corro": {
        # Use SHOPIFY_URL_CORRO first to match pipeline.py.
        # Default must be corroshop.com; equestrian-labs.myshopify.com returns 404.
        "url": os.environ.get("SHOPIFY_URL_CORRO") or os.environ.get("SHOPIFY_STORE_CORRO") or "corroshop.com",
        "token": os.environ.get("SHOPIFY_TOKEN_CORRO", ""),
        "sheet_id": os.environ.get("SHEET_ID_CORRO", "1nq8xkDzowAvhD3wpMBlVK2M3FZSNS2DrAiPxz-Y2tdU"),
    },
    "cavali": {
        "url": os.environ.get("SHOPIFY_URL_CAVALI") or os.environ.get("SHOPIFY_STORE_CAVALI") or "cavali-club.myshopify.com",
        "token": os.environ.get("SHOPIFY_TOKEN_CAVALI", ""),
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

# Default: only fill blanks/zero missing columns, never overwrite financials.
# Set UPDATE_EXISTING_WEBSITE_KPIS=true if you want to refresh these website columns too.
UPDATE_EXISTING = os.environ.get("UPDATE_EXISTING_WEBSITE_KPIS", "false").lower() == "true"


def get_gc():
    creds = Credentials.from_service_account_info(
        json.loads(os.environ["GOOGLE_CREDENTIALS"]),
        scopes=SCOPES,
    )
    return gspread.authorize(creds)


def money(v):
    try:
        return float(str(v or 0).replace(",", "").replace("%", "").strip())
    except Exception:
        return 0.0


def request_with_retry(method, url, **kwargs):
    last = None
    for attempt in range(8):
        r = requests.request(method, url, timeout=60, **kwargs)
        last = r
        if r.status_code in (429, 500, 502, 503, 504):
            wait = min(90, 4 + attempt * 4)
            print(f"    HTTP {r.status_code}; retry {attempt+1}/8 in {wait}s")
            time.sleep(wait)
            continue
        r.raise_for_status()
        return r
    last.raise_for_status()
    return last


def run_shopifyql(store, token, shopifyql):
    url = f"https://{store}/admin/api/{API_VERSION}/graphql.json"
    query = """
    query ShopifyAnalytics($query: String!) {
      shopifyqlQuery(query: $query) {
        tableData {
          columns { name dataType displayName }
          rows
          rowData
        }
        parseErrors { code message }
      }
    }
    """
    payload = request_with_retry(
        "POST",
        url,
        headers={"X-Shopify-Access-Token": token, "Content-Type": "application/json"},
        json={"query": query, "variables": {"query": shopifyql}},
    ).json()

    q = (payload.get("data") or {}).get("shopifyqlQuery") or {}
    if q.get("parseErrors"):
        raise RuntimeError(q["parseErrors"])

    table = q.get("tableData") or {}
    cols = table.get("columns") or []
    rows = table.get("rows") or table.get("rowData") or []
    names = [(c.get("name") or c.get("displayName") or f"col_{i}") for i, c in enumerate(cols)]

    parsed = []
    for row in rows:
        if isinstance(row, dict):
            parsed.append(row)
        else:
            parsed.append({names[i] if i < len(names) else f"col_{i}": v for i, v in enumerate(row)})
    return parsed


def first_row(rows):
    return rows[0] if rows else {}


def fetch_website_extra(store, token, start, end):
    # Only the missing historical fields. This does NOT touch gross/net/cogs/etc.
    q1 = f"""
    FROM sessions
    SHOW pageviews
    SINCE {start}
    UNTIL {end}
    """
    r1 = first_row(run_shopifyql(store, token, q1))
    pageviews = int(abs(money(r1.get("pageviews", 0))))

    q2 = f"""
    FROM sessions
    SHOW sessions_that_reached_checkout, sessions_that_reached_and_completed_checkout
    SINCE {start}
    UNTIL {end}
    """
    r2 = first_row(run_shopifyql(store, token, q2))
    reached = int(abs(money(r2.get("sessions_that_reached_checkout", 0))))
    completed = int(abs(money(r2.get("sessions_that_reached_and_completed_checkout", 0))))
    abandoned = max(reached - completed, 0)
    rate = round(abandoned / reached * 100, 2) if reached else 0

    return {
        "pageviews": pageviews,
        "sessions_reached_checkout": reached,
        "sessions_completed_checkout": completed,
        "checkout_abandonments": abandoned,
        "checkout_abandonment_rate": rate,
    }


def ensure_columns(ws, values):
    header = list(values[0]) if values else []
    changed = False
    for col in TARGET_COLUMNS:
        if col not in header:
            header.append(col)
            changed = True

    if changed:
        print(f"    adding missing columns: {[c for c in TARGET_COLUMNS if c not in values[0]]}")
        ws.update("1:1", [header], value_input_option="USER_ENTERED")
        # pad current values for indexing in memory
        for r in values:
            while len(r) < len(header):
                r.append("")
        values[0] = header

    return header, values


def is_blankish(v):
    s = str(v or "").strip()
    return s in ("", "0", "0.0", "0.00", "—", "-")


def main():
    gc = get_gc()
    brands_filter = os.environ.get("RUN_BRANDS", "corro,cavali").lower().replace(" ", "").split(",")

    for brand, cfg in STORES.items():
        if brand not in brands_filter:
            continue
        if not cfg["token"]:
            print(f"Skipping {brand}: missing Shopify token")
            continue

        print(f"\n=== {brand.upper()} website KPI historical filler ===")
        sh = gc.open_by_key(cfg["sheet_id"])
        ws = sh.worksheet("kpis_daily")
        values = ws.get_all_values()
        if not values:
            print("    empty kpis_daily")
            continue

        header, values = ensure_columns(ws, values)
        idx = {h: i for i, h in enumerate(header)}

        required = ["period", "period_start", "period_end"]
        missing_required = [c for c in required if c not in idx]
        if missing_required:
            raise RuntimeError(f"Missing required columns: {missing_required}")

        updates = []
        processed = 0
        skipped = 0

        for row_num, row in enumerate(values[1:], start=2):
            period = row[idx["period"]] if idx["period"] < len(row) else ""
            start = row[idx["period_start"]] if idx["period_start"] < len(row) else ""
            end = row[idx["period_end"]] if idx["period_end"] < len(row) else ""
            if not start or not end:
                continue

            # Keep runtime small if desired: FILL_FROM=2025-01-01, FILL_TO=2026-12-31
            fill_from = os.environ.get("FILL_FROM", "")
            fill_to = os.environ.get("FILL_TO", "")
            if fill_from and end < fill_from:
                skipped += 1
                continue
            if fill_to and start > fill_to:
                skipped += 1
                continue

            needs = UPDATE_EXISTING
            for col in TARGET_COLUMNS:
                ci = idx[col]
                val = row[ci] if ci < len(row) else ""
                if is_blankish(val):
                    needs = True
                    break
            if not needs:
                skipped += 1
                continue

            print(f"    row {row_num}: {period} {start} → {end}")
            try:
                data = fetch_website_extra(cfg["url"], cfg["token"], start, end)
            except Exception as exc:
                # Do not fail the whole presentation backfill for one old period/API gap.
                print(f"    ⚠ skipped row {row_num} {period}: {exc}")
                skipped += 1
                continue

            for col in TARGET_COLUMNS:
                ci = idx[col] + 1
                updates.append({
                    "range": gspread.utils.rowcol_to_a1(row_num, ci),
                    "values": [[data[col]]],
                })
            processed += 1

            if len(updates) >= 100:
                ws.batch_update(updates, value_input_option="USER_ENTERED")
                updates = []
                time.sleep(1)

        if updates:
            ws.batch_update(updates, value_input_option="USER_ENTERED")

        print(f"  done {brand}: processed={processed}, skipped={skipped}")


if __name__ == "__main__":
    main()
