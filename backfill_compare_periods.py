"""
Backfill comparison periods for the dashboard without touching Smartrr.

Purpose:
- Fill/repair month, week and quarter comparison rows.
- Includes Pageviews and Checkout Abandonment Rate from ShopifyQL.
- Safe for Smartrr: this writes KPI/revenue_share/new_vs_returning sheets only through
  pipeline.write_all and does not rebuild Smartrr product-volume subscriber rows.

Run:
  python -u backfill_compare_periods.py

Optional env:
  RUN_BRANDS=corro,cavali
  COMPARE_BACKFILL_MONTHS=24
  COMPARE_BACKFILL_WEEKS=90
  COMPARE_BACKFILL_QUARTERS=12
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


def _add_months(d, n):
    y = d.year + (d.month - 1 + n) // 12
    m = (d.month - 1 + n) % 12 + 1
    return date(y, m, 1)


def _monday_of(d):
    return d - timedelta(days=d.weekday())


def _quarter_start(y, q):
    return date(y, 3 * (q - 1) + 1, 1)


def _quarter_end(y, q, today):
    m = 3 * q
    end = date(y, m, calendar.monthrange(y, m)[1])
    return min(end, today)


def _quarter_of(d):
    return ((d.month - 1) // 3) + 1


def _build_period_row(now_str, url, token, period_key, start, end):
    print(f"    compare row {period_key}: {start} → {end}")
    sales = p.fetch_sales(url, token, start, end)
    sessions = p.fetch_sessions(url, token, start, end)
    fulfilled = p.fetch_orders_fulfilled(url, token, start, end)
    orders = p.fetch_orders(url, token, start, end)
    nvr = p.fetch_new_vs_returning(url, token, start, end)
    cur = p.build(sales, orders, nvr, sessions, fulfilled)
    # Gentle pacing avoids ShopifyQL and REST throttle spikes.
    time.sleep(1.25)
    return p.make_kpi_row(now_str, period_key, start, end, cur), orders, sales, cur, nvr


def _append_rows(now_str, period_key, start, end, row, orders, sales, cur, nvr, kpi_rows, rs_rows, nvr_rows):
    kpi_rows.append(row)
    for ch, v in p.calc_rs(orders, sales.get("pct_gm", 0)).items():
        rs_rows.append([now_str, period_key, ch, v["amount"], v["pct"], v["gross_profit"], v["gross_margin"], "", "", str(v["gp_is_estimate"])])
    nvr_rows.append([now_str, period_key, str(start), str(end), nvr.get("new_customers",0), nvr.get("returning_customers",0), nvr.get("new_revenue",0), nvr.get("returning_revenue",0), cur.get("new_gross_profit",0), cur.get("returning_gross_profit",0)])


def main():
    gc = p.get_gc()
    today = datetime.now(p.TIMEZONE).date()
    now_str = datetime.now(p.TIMEZONE).strftime("%Y-%m-%d %H:%M")

    run_brands = [x.strip().lower() for x in os.environ.get("RUN_BRANDS", "cavali,corro").split(",") if x.strip()]
    months_back = int(os.environ.get("COMPARE_BACKFILL_MONTHS", "24"))
    weeks_back = int(os.environ.get("COMPARE_BACKFILL_WEEKS", "90"))
    quarters_back = int(os.environ.get("COMPARE_BACKFILL_QUARTERS", "12"))

    for brand_name in run_brands:
        if brand_name not in p.STORES:
            print(f"Skipping unknown brand: {brand_name}")
            continue

        cfg = p.STORES[brand_name]
        url, token = cfg["url"], cfg["token"]
        kpi_rows, rs_rows, nvr_rows = [], [], []

        print(f"\n{'='*60}\n  {brand_name.upper()} — compare period backfill / repair\n{'='*60}")

        # Full month rows. For current month, also write mtd_YYYY-MM as current-to-date.
        first_month = _add_months(today.replace(day=1), -months_back)
        cur_month = first_month
        while cur_month <= today.replace(day=1):
            y, m = cur_month.year, cur_month.month
            period_key = f"{y}-{m:02d}"
            start = _month_start(y, m)
            end = _month_end(y, m, today)
            row, orders, sales, cur, nvr = _build_period_row(now_str, url, token, period_key, start, end)
            _append_rows(now_str, period_key, start, end, row, orders, sales, cur, nvr, kpi_rows, rs_rows, nvr_rows)

            mtd_key = f"mtd_{period_key}"
            mtd_row = list(row)
            mtd_row[1] = mtd_key
            _append_rows(now_str, mtd_key, start, end, mtd_row, orders, sales, cur, nvr, kpi_rows, rs_rows, nvr_rows)

            cur_month = _add_months(cur_month, 1)

        # Full week rows.
        wk_start = _monday_of(today) - timedelta(days=7 * weeks_back)
        while wk_start <= today:
            wk_end = min(wk_start + timedelta(days=6), today)
            period_key = f"week_{wk_start}"
            row, orders, sales, cur, nvr = _build_period_row(now_str, url, token, period_key, wk_start, wk_end)
            _append_rows(now_str, period_key, wk_start, wk_end, row, orders, sales, cur, nvr, kpi_rows, rs_rows, nvr_rows)
            wk_start += timedelta(days=7)

        # Full/current quarter rows.
        tq = _quarter_of(today)
        ty = today.year
        start_q_index = (ty * 4 + (tq - 1)) - quarters_back
        end_q_index = ty * 4 + (tq - 1)
        for qi in range(start_q_index, end_q_index + 1):
            y = qi // 4
            q = (qi % 4) + 1
            start = _quarter_start(y, q)
            end = _quarter_end(y, q, today)
            period_key = f"q{q}_{y}"
            row, orders, sales, cur, nvr = _build_period_row(now_str, url, token, period_key, start, end)
            _append_rows(now_str, period_key, start, end, row, orders, sales, cur, nvr, kpi_rows, rs_rows, nvr_rows)

        print(f"\n  Writing {len(kpi_rows)} KPI compare/repair rows for {brand_name}...")
        p.write_all(gc, cfg["sheet_id"], kpi_rows, rs_rows, nvr_rows, brand_name)
        print(f"  ✓ {brand_name.upper()} compare backfill / repair done")


if __name__ == "__main__":
    main()
