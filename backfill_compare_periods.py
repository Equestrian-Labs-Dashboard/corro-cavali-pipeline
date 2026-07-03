"""
Quick/targeted compare-period repair for Dashboard Corro/Cavali.

Why this exists:
- The full compare backfill can take too long and may be canceled before writing.
- This version writes in small batches, so completed periods are saved even if the run stops.
- It focuses first on the periods needed for current YOY:
  * Current month comparison full month last year, e.g. July 2025
  * Current/previous full months
  * Recent weeks plus their YOY comparison weeks
  * Current/recent quarters plus their YOY comparison quarters

Safe for Smartrr:
- Does NOT rebuild or overwrite smartrr_product_volume.
- Only writes KPI / revenue_share / new_vs_returning via pipeline.write_all.

Recommended run:
  RUN_BRANDS=corro python -u backfill_compare_periods.py

Optional env:
  RUN_BRANDS=corro,cavali
  QUICK_WEEKS=16
  QUICK_MONTHS_BACK=3
  QUICK_QUARTERS_BACK=2
  QUICK_BATCH_SIZE=4
"""

import os
import calendar
import time
from datetime import datetime, timedelta, date

import pipeline as p


def _month_start(y, m):
    return date(y, m, 1)


def _month_end(y, m, today):
    end = date(y, m, calendar.monthrange(y, m)[1])
    return min(end, today)


def _full_month_end(y, m):
    return date(y, m, calendar.monthrange(y, m)[1])


def _add_months(d, n):
    y = d.year + (d.month - 1 + n) // 12
    m = (d.month - 1 + n) % 12 + 1
    return date(y, m, 1)


def _monday_of(d):
    return d - timedelta(days=d.weekday())


def _quarter_of(d):
    return ((d.month - 1) // 3) + 1


def _quarter_start(y, q):
    return date(y, 3 * (q - 1) + 1, 1)


def _quarter_end(y, q, today=None):
    m = 3 * q
    end = date(y, m, calendar.monthrange(y, m)[1])
    return min(end, today) if today else end


def _period_exists_key(rows_seen, key):
    return key in rows_seen


def _build_period_row(now_str, url, token, period_key, start, end):
    print(f"    repair row {period_key}: {start} → {end}", flush=True)
    sales = p.fetch_sales(url, token, start, end)
    sessions = p.fetch_sessions(url, token, start, end)
    fulfilled = p.fetch_orders_fulfilled(url, token, start, end)
    orders = p.fetch_orders(url, token, start, end)
    nvr = p.fetch_new_vs_returning(url, token, start, end)
    cur = p.build(sales, orders, nvr, sessions, fulfilled)
    print(
        f"      sessions:{cur.get('sessions',0):,} visitors:{cur.get('online_store_visitors',0):,} "
        f"pageviews:{cur.get('pageviews',0):,} checkout_abandonment:{cur.get('checkout_abandonment_rate','')}",
        flush=True
    )
    time.sleep(1.5)
    return p.make_kpi_row(now_str, period_key, start, end, cur), orders, sales, cur, nvr


def _append_rows(now_str, period_key, start, end, row, orders, sales, cur, nvr, kpi_rows, rs_rows, nvr_rows):
    kpi_rows.append(row)
    for ch, v in p.calc_rs(orders, sales.get("pct_gm", 0)).items():
        rs_rows.append([
            now_str, period_key, ch,
            v["amount"], v["pct"],
            v["gross_profit"], v["gross_margin"],
            "", "", str(v["gp_is_estimate"])
        ])
    nvr_rows.append([
        now_str, period_key, str(start), str(end),
        nvr.get("new_customers", 0), nvr.get("returning_customers", 0),
        nvr.get("new_revenue", 0), nvr.get("returning_revenue", 0),
        cur.get("new_gross_profit", 0), cur.get("returning_gross_profit", 0)
    ])


def _flush(gc, sheet_id, brand_name, kpi_rows, rs_rows, nvr_rows):
    if not kpi_rows and not rs_rows and not nvr_rows:
        return
    print(f"\n  Writing batch: {len(kpi_rows)} KPI rows for {brand_name}...", flush=True)
    p.write_all(gc, sheet_id, kpi_rows, rs_rows, nvr_rows, brand_name)
    kpi_rows.clear()
    rs_rows.clear()
    nvr_rows.clear()
    print("  ✓ batch saved\n", flush=True)


def _unique_periods(periods):
    seen = set()
    out = []
    for key, start, end in periods:
        if key not in seen:
            seen.add(key)
            out.append((key, start, end))
    return out


def build_quick_periods(today):
    periods = []

    months_back = int(os.environ.get("QUICK_MONTHS_BACK", "3"))
    weeks_back = int(os.environ.get("QUICK_WEEKS", "16"))
    quarters_back = int(os.environ.get("QUICK_QUARTERS_BACK", "2"))

    # 1) Current month comparison full month last year FIRST.
    # Example: July 2026 MTD needs full July 2025 for YOY per business rule.
    cur_month = today.replace(day=1)
    py = today.year - 1
    pm = today.month
    periods.append((f"{py}-{pm:02d}", _month_start(py, pm), _full_month_end(py, pm)))

    # 2) Recent full/current months and their YOY full months.
    for i in range(0, months_back + 1):
        d = _add_months(cur_month, -i)
        y, m = d.year, d.month
        periods.append((f"{y}-{m:02d}", _month_start(y, m), _month_end(y, m, today)))
        periods.append((f"{y-1}-{m:02d}", _month_start(y-1, m), _full_month_end(y-1, m)))

    # 3) Current and recent weeks plus their YOY comparison weeks.
    this_monday = _monday_of(today)
    for i in range(0, weeks_back + 1):
        ws = this_monday - timedelta(days=7*i)
        we = min(ws + timedelta(days=6), today)
        periods.append((f"week_{ws}", ws, we))

        yws = ws - timedelta(days=364)
        ywe = yws + timedelta(days=6)
        periods.append((f"week_{yws}", yws, ywe))

    # 4) Current/recent quarters and their YOY full quarters.
    tq = _quarter_of(today)
    ty = today.year
    cur_q_idx = ty * 4 + (tq - 1)
    for i in range(0, quarters_back + 1):
        qi = cur_q_idx - i
        y = qi // 4
        q = (qi % 4) + 1
        periods.append((f"q{q}_{y}", _quarter_start(y, q), _quarter_end(y, q, today)))

        yy = y - 1
        periods.append((f"q{q}_{yy}", _quarter_start(yy, q), _quarter_end(yy, q, None)))

    return _unique_periods(periods)


def main():
    gc = p.get_gc()
    today = datetime.now(p.TIMEZONE).date()
    now_str = datetime.now(p.TIMEZONE).strftime("%Y-%m-%d %H:%M")
    batch_size = max(1, int(os.environ.get("QUICK_BATCH_SIZE", "4")))

    run_brands = [x.strip().lower() for x in os.environ.get("RUN_BRANDS", "corro").split(",") if x.strip()]
    periods = build_quick_periods(today)

    print(f"Quick compare repair periods: {len(periods)}", flush=True)
    for key, start, end in periods[:8]:
        print(f"  queued: {key} {start} → {end}", flush=True)

    for brand_name in run_brands:
        if brand_name not in p.STORES:
            print(f"Skipping unknown brand: {brand_name}")
            continue

        cfg = p.STORES[brand_name]
        url, token = cfg["url"], cfg["token"]
        kpi_rows, rs_rows, nvr_rows = [], [], []

        print(f"\n{'='*60}\n  {brand_name.upper()} — QUICK compare repair\n{'='*60}", flush=True)

        done = 0
        for period_key, start, end in periods:
            row, orders, sales, cur, nvr = _build_period_row(now_str, url, token, period_key, start, end)
            _append_rows(now_str, period_key, start, end, row, orders, sales, cur, nvr, kpi_rows, rs_rows, nvr_rows)
            done += 1

            # Save very early: the first period is the current month YOY full month.
            if done == 1 or len(kpi_rows) >= batch_size:
                _flush(gc, cfg["sheet_id"], brand_name, kpi_rows, rs_rows, nvr_rows)

        _flush(gc, cfg["sheet_id"], brand_name, kpi_rows, rs_rows, nvr_rows)
        print(f"  ✓ {brand_name.upper()} quick compare repair done", flush=True)


if __name__ == "__main__":
    main()
