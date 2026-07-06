"""
Targeted Website KPI month repair for both Corro and Cavali.

Use this when Pageviews / Checkout Abandonment are missing for specific historical months.
It updates only website KPI fields in kpis_daily and preserves financial/Smartrr data.

Examples:
  RUN_BRANDS=corro TARGET_MONTHS=2024-11,2024-12,2025-01 python -u backfill_website_kpis_target_months.py
  RUN_BRANDS=cavali TARGET_MONTHS=2024-07,2024-08,2024-09,2024-10,2024-11,2024-12,2025-01,2025-02,2025-03,2025-04,2025-05,2025-06 python -u backfill_website_kpis_target_months.py
"""

import os
import calendar
import time
from datetime import datetime, date
import pipeline as p

WEB_FIELDS = ["sessions", "unique_visitors", "pageviews", "conversion_rate", "checkout_abandonment_rate"]

def month_range(ym):
    y, m = [int(x) for x in ym.split("-")]
    return date(y, m, 1).isoformat(), date(y, m, calendar.monthrange(y, m)[1]).isoformat()

def row_to_map(headers, row):
    return {h: (row[i] if i < len(row) else "") for i, h in enumerate(headers)}

def map_to_row(headers, m):
    return [m.get(h, "") for h in headers]

def safe_date(v):
    try:
        return datetime.fromisoformat(str(v)[:10])
    except Exception:
        return datetime.min

def save_ws(ws, headers, existing):
    rows = sorted(existing.values(), key=lambda m: (safe_date(m.get("period_start", "")), str(m.get("period", ""))))
    ws.clear()
    ws.append_row(headers)
    if rows:
        ws.append_rows([map_to_row(headers, m) for m in rows], value_input_option="USER_ENTERED")
    return len(rows)

def repair_brand(gc, brand, months):
    cfg = p.STORES[brand]
    sh = gc.open_by_key(cfg["sheet_id"])
    try:
        ws = sh.worksheet("kpis_daily")
    except Exception:
        ws = sh.add_worksheet("kpis_daily", rows=800, cols=len(p.HEADERS))

    values = ws.get_all_values()
    headers = values[0] if values else list(p.HEADERS)
    for h in p.HEADERS:
        if h not in headers:
            headers.append(h)

    existing = {}
    for r in values[1:]:
        m = row_to_map(headers, r)
        pk = str(m.get("period", "")).strip()
        if pk:
            existing[pk] = m

    now_str = datetime.now(p.TIMEZONE).strftime("%Y-%m-%d %H:%M")
    url, token = cfg["url"], cfg["token"]

    print(f"\n{'='*60}\n  {brand.upper()} — targeted website KPI repair\n{'='*60}", flush=True)

    for ym in months:
        s, e = month_range(ym)
        print(f"  Repairing {ym}: {s} -> {e}", flush=True)
        web = p.fetch_sessions(url, token, s, e)

        row = existing.get(ym, {})
        row["updated_at"] = now_str
        row["period"] = ym
        row["period_start"] = s
        row["period_end"] = e
        for k in WEB_FIELDS:
            row[k] = web.get(k, "")
        existing[ym] = row

        print(f"    sessions={row.get('sessions')} visitors={row.get('unique_visitors')} pageviews={row.get('pageviews')} cr={row.get('conversion_rate')} checkout_abandonment={row.get('checkout_abandonment_rate')}", flush=True)
        total = save_ws(ws, headers, existing)
        print(f"    saved {ym}; total kpis_daily rows={total}", flush=True)
        time.sleep(1.25)

    print(f"  done {brand}", flush=True)

def main():
    months_raw = os.environ.get("TARGET_MONTHS", "").strip()
    if not months_raw:
        raise SystemExit("Set TARGET_MONTHS, example: TARGET_MONTHS=2024-11,2024-12,2025-01")
    months = [m.strip() for m in months_raw.split(",") if m.strip()]
    brands = [b.strip().lower() for b in os.environ.get("RUN_BRANDS", "corro").split(",") if b.strip()]

    gc = p.get_gc()
    for brand in brands:
        if brand not in p.STORES:
            print(f"Skipping unknown brand: {brand}", flush=True)
            continue
        repair_brand(gc, brand, months)

if __name__ == "__main__":
    main()
