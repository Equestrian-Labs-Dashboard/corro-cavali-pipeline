import os
import json
import time
from datetime import datetime, date
import requests
import gspread
from google.oauth2.service_account import Credentials

GQL_VERSION = "2025-10"
SCOPES = ["https://www.googleapis.com/auth/spreadsheets", "https://www.googleapis.com/auth/drive"]

STORES = {
    "corro": {
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
UPDATE_EXISTING = os.environ.get("UPDATE_EXISTING_WEBSITE_KPIS", "true").lower() == "true"


def get_gc():
    creds = Credentials.from_service_account_info(json.loads(os.environ["GOOGLE_CREDENTIALS"]), scopes=SCOPES)
    return gspread.authorize(creds)


def num(v):
    try:
        return float(str(v or 0).replace(",", "").replace("%", "").strip())
    except Exception:
        return 0.0


def gm(v):
    try:
        f = float(str(v or 0).replace(",", "").replace("%", "").strip())
        return round(f * 100, 2) if abs(f) <= 1 else round(f, 2)
    except Exception:
        return 0.0


def gql(store, token, query):
    url = f"https://{store}/admin/api/{GQL_VERSION}/graphql.json"
    last = None
    for attempt in range(8):
        r = requests.post(
            url,
            headers={"X-Shopify-Access-Token": token, "Content-Type": "application/json"},
            json={"query": query},
            timeout=60,
        )
        last = r
        if r.status_code in (429, 500, 502, 503, 504):
            wait = min(90, 4 + attempt * 5)
            print(f"    HTTP {r.status_code}; retry {attempt+1}/8 in {wait}s")
            time.sleep(wait)
            continue
        if r.status_code != 200:
            raise RuntimeError(f"HTTP {r.status_code} for {store}: {r.text[:250]}")
        data = r.json()
        if data.get("errors"):
            raise RuntimeError(f"GQL errors: {data['errors']}")
        return data.get("data") or {}
    last.raise_for_status()
    return {}


def ql_run(store, token, shopifyql):
    escaped = shopifyql.replace("\\", "\\\\").replace('"', '\\"')
    query = (
        f'{{ shopifyqlQuery(query: "{escaped}") {{ '
        f'tableData {{ columns {{ name }} rows }} '
        f'parseErrors }} }}'
    )
    data = gql(store, token, query)
    obj = data.get("shopifyqlQuery") or {}
    errs = obj.get("parseErrors") or []
    if errs:
        print(f"    parseErrors for query: {errs}")
        return []
    td = obj.get("tableData") or {}
    rows = td.get("rows") or []
    if isinstance(rows, list) and rows and isinstance(rows[0], dict):
        return rows
    if isinstance(rows, list):
        cols = [(c.get("name") or f"col_{i}") for i, c in enumerate(td.get("columns") or [])]
        return [{cols[i] if i < len(cols) else f"col_{i}": v for i, v in enumerate(row)} for row in rows]
    if isinstance(rows, str):
        try:
            parsed = json.loads(rows)
            return parsed if isinstance(parsed, list) else []
        except Exception:
            return []
    return []


def ql_row(store, token, shopifyql):
    rows = ql_run(store, token, shopifyql)
    return rows[-1] if rows else {}


def fetch_website_extra(store, token, start, end):
    # Same shape as the working pipeline query; this is more reliable than SHOW pageviews alone.
    row = ql_row(store, token,
        f"FROM sessions SHOW sessions, online_store_visitors, pageviews, conversion_rate "
        f"SINCE {start} UNTIL {end}"
    )
    pageviews = int(abs(num(row.get("pageviews", 0))))

    funnel = ql_row(store, token,
        f"FROM sessions SHOW sessions_that_reached_checkout, sessions_that_reached_and_completed_checkout "
        f"SINCE {start} UNTIL {end}"
    )
    reached = int(abs(num(funnel.get("sessions_that_reached_checkout", 0))))
    completed = int(abs(num(funnel.get("sessions_that_reached_and_completed_checkout", 0))))
    abandoned = max(reached - completed, 0)
    rate = round(abandoned / reached * 100, 2) if reached else 0

    return {
        "pageviews": pageviews,
        "sessions_reached_checkout": reached,
        "sessions_completed_checkout": completed,
        "checkout_abandonments": abandoned,
        "checkout_abandonment_rate": rate,
    }


def ensure_cols(ws, values):
    header = list(values[0])
    changed = False
    for c in TARGET_COLUMNS:
        if c not in header:
            header.append(c)
            changed = True
    if changed:
        ws.update("1:1", [header], value_input_option="USER_ENTERED")
        for r in values:
            while len(r) < len(header):
                r.append("")
        values[0] = header
    return header, values


def blankish(v):
    return str(v or "").strip() in ("", "0", "0.0", "0.00", "-", "—")


def main():
    gc = get_gc()
    brands = [b for b in os.environ.get("RUN_BRANDS", "corro,cavali").lower().replace(" ", "").split(",") if b]
    fill_from = os.environ.get("FILL_FROM", "")
    fill_to = os.environ.get("FILL_TO", "")

    for brand in brands:
        cfg = STORES[brand]
        print(f"\n=== {brand.upper()} — website-only KPI backfill ===")
        print(f"    store={cfg['url']}")
        sh = gc.open_by_key(cfg["sheet_id"])
        ws = sh.worksheet("kpis_daily")
        values = ws.get_all_values()
        if not values:
            continue
        header, values = ensure_cols(ws, values)
        idx = {h: i for i, h in enumerate(header)}

        updates = []
        processed = skipped = failed = 0
        for rn, row in enumerate(values[1:], start=2):
            period = row[idx["period"]] if idx["period"] < len(row) else ""
            start = row[idx["period_start"]] if idx["period_start"] < len(row) else ""
            end = row[idx["period_end"]] if idx["period_end"] < len(row) else ""
            if not start or not end:
                continue
            if fill_from and end < fill_from:
                continue
            if fill_to and start > fill_to:
                continue

            needs = UPDATE_EXISTING or any(blankish(row[idx[c]] if idx[c] < len(row) else "") for c in TARGET_COLUMNS)
            if not needs:
                skipped += 1
                continue

            print(f"    row {rn}: {period} {start} → {end}")
            try:
                data = fetch_website_extra(cfg["url"], cfg["token"], start, end)
            except Exception as exc:
                print(f"    ⚠ failed row {rn}: {exc}")
                failed += 1
                continue

            for c in TARGET_COLUMNS:
                updates.append({"range": gspread.utils.rowcol_to_a1(rn, idx[c] + 1), "values": [[data[c]]]})
            processed += 1

            if len(updates) >= 100:
                ws.batch_update(updates, value_input_option="USER_ENTERED")
                updates = []
                time.sleep(1)

        if updates:
            ws.batch_update(updates, value_input_option="USER_ENTERED")
        print(f"  done {brand}: processed={processed}, skipped={skipped}, failed={failed}")


if __name__ == "__main__":
    main()
