"""
Backfill Website KPIs Only

Compatible with the GitHub Actions form fields:
  RUN_BRANDS=corro
  START_DATE=2024-01-01
  END_DATE=2024-12-31
  OVERWRITE_WEBSITE_VALUES=true
  MAX_ROWS_PER_BRAND=30
  SLEEP_SECONDS=2.5

Also supports the older format:
  TARGET_MONTHS=2024-11,2024-12,2025-01

What it updates:
  sessions
  unique_visitors
  pageviews
  conversion_rate
  checkout_abandonment_rate

What it preserves:
  financial KPIs, Smartrr rows/data, revenue share, new/returning, etc.
"""

import os
import calendar
import time
from datetime import datetime, date
import pipeline as p

WEB_FIELDS = [
    "sessions",
    "unique_visitors",
    "pageviews",
    "conversion_rate",
    "checkout_abandonment_rate",
]


def parse_bool(v, default=True):
    s = str(v if v is not None else "").strip().lower()
    if not s:
        return default
    return s in ("1", "true", "yes", "y", "si", "sí")


def month_range(ym):
    y, m = [int(x) for x in ym.split("-")]
    return date(y, m, 1).isoformat(), date(y, m, calendar.monthrange(y, m)[1]).isoformat()


def months_between(start_date, end_date):
    s = date.fromisoformat(start_date[:10])
    e = date.fromisoformat(end_date[:10])
    cur = date(s.year, s.month, 1)
    last = date(e.year, e.month, 1)
    out = []
    while cur <= last:
        out.append(f"{cur.year}-{cur.month:02d}")
        if cur.month == 12:
            cur = date(cur.year + 1, 1, 1)
        else:
            cur = date(cur.year, cur.month + 1, 1)
    return out


def row_to_map(headers, row):
    return {h: (row[i] if i < len(row) else "") for i, h in enumerate(headers)}


def map_to_row(headers, m):
    return [m.get(h, "") for h in headers]


def safe_date(v):
    try:
        return datetime.fromisoformat(str(v)[:10])
    except Exception:
        return datetime.min


def has_value(v):
    return v is not None and str(v).strip() != ""


def save_ws(ws, headers, existing):
    rows = sorted(
        existing.values(),
        key=lambda m: (safe_date(m.get("period_start", "")), str(m.get("period", "")))
    )
    ws.clear()
    ws.append_row(headers)
    if rows:
        ws.append_rows([map_to_row(headers, m) for m in rows], value_input_option="USER_ENTERED")
    return len(rows)


def repair_brand(gc, brand, months, overwrite=True, max_rows=30, sleep_seconds=2.5):
    cfg = p.STORES[brand]
    sh = gc.open_by_key(cfg["sheet_id"])

    try:
        ws = sh.worksheet("kpis_daily")
    except Exception:
        ws = sh.add_worksheet("kpis_daily", rows=1000, cols=len(p.HEADERS))

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

    print(f"\n{'='*60}\n  {brand.upper()} — WEBSITE KPI ONLY BACKFILL\n{'='*60}", flush=True)
    print(f"  months requested: {months}", flush=True)
    print(f"  overwrite website values: {overwrite}", flush=True)
    print(f"  max rows this run: {max_rows}", flush=True)

    processed = 0

    for ym in months:
        if processed >= max_rows:
            print(f"  Reached MAX_ROWS_PER_BRAND={max_rows}. Stop safely.", flush=True)
            break

        s, e = month_range(ym)
        print(f"\n  Repairing {ym}: {s} -> {e}", flush=True)

        row = existing.get(ym, {})
        if not overwrite:
            already_ok = all(has_value(row.get(k)) and str(row.get(k)).strip() not in ("0", "0.0") for k in ("pageviews", "checkout_abandonment_rate"))
            if already_ok:
                print("    skip: website fields already populated", flush=True)
                continue

        web = p.fetch_sessions(url, token, s, e)

        row["updated_at"] = now_str
        row["period"] = ym
        row["period_start"] = s
        row["period_end"] = e

        for k in WEB_FIELDS:
            new_val = web.get(k, "")
            if overwrite or not has_value(row.get(k)) or str(row.get(k)).strip() in ("0", "0.0"):
                row[k] = new_val

        existing[ym] = row
        processed += 1

        print(
            f"    sessions={row.get('sessions')} "
            f"visitors={row.get('unique_visitors')} "
            f"pageviews={row.get('pageviews')} "
            f"cr={row.get('conversion_rate')} "
            f"checkout_abandonment={row.get('checkout_abandonment_rate')}",
            flush=True
        )

        total = save_ws(ws, headers, existing)
        print(f"    ✓ saved {ym}; total kpis_daily rows={total}", flush=True)
        time.sleep(float(sleep_seconds))

    print(f"\n  ✓ {brand.upper()} website KPI backfill done. Processed={processed}", flush=True)


def main():
    target_months = os.environ.get("TARGET_MONTHS", "").strip()

    if target_months:
        months = [m.strip() for m in target_months.split(",") if m.strip()]
    else:
        start_date = os.environ.get("START_DATE", "").strip()
        end_date = os.environ.get("END_DATE", "").strip()

        if not start_date:
            raise SystemExit("Set START_DATE or TARGET_MONTHS. Example START_DATE=2024-01-01 END_DATE=2024-12-31")

        if not end_date:
            end_date = datetime.now(p.TIMEZONE).date().isoformat()

        months = months_between(start_date, end_date)

    brands = [b.strip().lower() for b in os.environ.get("RUN_BRANDS", "corro").split(",") if b.strip()]
    overwrite = parse_bool(os.environ.get("OVERWRITE_WEBSITE_VALUES", "true"), True)
    max_rows = int(os.environ.get("MAX_ROWS_PER_BRAND", "30"))
    sleep_seconds = float(os.environ.get("SLEEP_SECONDS", "2.5"))

    gc = p.get_gc()

    for brand in brands:
        if brand not in p.STORES:
            print(f"Skipping unknown brand: {brand}", flush=True)
            continue
        repair_brand(gc, brand, months, overwrite=overwrite, max_rows=max_rows, sleep_seconds=sleep_seconds)


if __name__ == "__main__":
    main()
