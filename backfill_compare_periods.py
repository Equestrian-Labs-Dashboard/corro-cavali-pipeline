"""
Backfill comparison periods for the dashboard without touching Smartrr.

Purpose:
- Fill missing month/week comparison rows, especially YOY rows like:
  2025-06 for June 2026 comparisons
  week_2025-06-23 for Jun 22–28, 2026 comparisons
- Keep this separate from the main pipeline so the existing Smartrr/product logic is not affected.

Run:
  python -u backfill_compare_periods_v89.py

Optional env:
  RUN_BRANDS=corro,cavali
  COMPARE_BACKFILL_MONTHS=24
  COMPARE_BACKFILL_WEEKS=70
"""

import os
import calendar
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


def _build_period_row(now_str, url, token, period_key, start, end):
    print(f"    compare row {period_key}: {start} → {end}")
    sales = p.fetch_sales(url, token, start, end)
    sessions = p.fetch_sessions(url, token, start, end)
    fulfilled = p.fetch_orders_fulfilled(url, token, start, end)
    orders = p.fetch_orders(url, token, start, end)
    nvr = p.fetch_new_vs_returning(url, token, start, end)
    cur = p.build(sales, orders, nvr, sessions, fulfilled)
    return p.make_kpi_row(now_str, period_key, start, end, cur), orders, sales, cur, nvr


def main():
    gc = p.get_gc()
    today = datetime.now(p.TIMEZONE).date()
    now_str = datetime.now(p.TIMEZONE).strftime("%Y-%m-%d %H:%M")

    run_brands = [x.strip().lower() for x in os.environ.get("RUN_BRANDS", "cavali,corro").split(",") if x.strip()]
    months_back = int(os.environ.get("COMPARE_BACKFILL_MONTHS", "24"))
    weeks_back = int(os.environ.get("COMPARE_BACKFILL_WEEKS", "70"))

    for brand_name in run_brands:
        if brand_name not in p.STORES:
            print(f"Skipping unknown brand: {brand_name}")
            continue

        cfg = p.STORES[brand_name]
        url, token = cfg["url"], cfg["token"]
        kpi_rows, rs_rows, nvr_rows = [], [], []

        print(f"\n{'='*60}\n  {brand_name.upper()} — compare period backfill\n{'='*60}")

        # Monthly rows, including closed months and current MTD month label.
        first_month = _add_months(today.replace(day=1), -months_back)
        cur_month = first_month
        while cur_month <= today.replace(day=1):
            y, m = cur_month.year, cur_month.month
            period_key = f"{y}-{m:02d}"
            start = _month_start(y, m)
            end = _month_end(y, m, today)
            row, orders, sales, cur, nvr = _build_period_row(now_str, url, token, period_key, start, end)
            kpi_rows.append(row)

            # Also write mtd_YYYY-MM for current month-to-date comparisons.
            mtd_key = f"mtd_{period_key}"
            mtd_row = list(row)
            mtd_row[1] = mtd_key
            kpi_rows.append(mtd_row)

            for ch, v in p.calc_rs(orders, sales.get("pct_gm", 0)).items():
                rs_rows.append([now_str, period_key, ch, v["amount"], v["pct"], v["gross_profit"], v["gross_margin"], "", "", str(v["gp_is_estimate"])])
                rs_rows.append([now_str, mtd_key, ch, v["amount"], v["pct"], v["gross_profit"], v["gross_margin"], "", "", str(v["gp_is_estimate"])])

            nvr_rows.append([now_str, period_key, str(start), str(end), nvr.get("new_customers",0), nvr.get("returning_customers",0), nvr.get("new_revenue",0), nvr.get("returning_revenue",0), cur.get("new_gross_profit",0), cur.get("returning_gross_profit",0)])
            nvr_rows.append([now_str, mtd_key, str(start), str(end), nvr.get("new_customers",0), nvr.get("returning_customers",0), nvr.get("new_revenue",0), nvr.get("returning_revenue",0), cur.get("new_gross_profit",0), cur.get("returning_gross_profit",0)])

            cur_month = _add_months(cur_month, 1)

        # Weekly rows. Use Monday-start labels matching the dashboard.
        wk_start = _monday_of(today) - timedelta(days=7 * weeks_back)
        while wk_start <= today:
            wk_end = min(wk_start + timedelta(days=6), today)
            period_key = f"week_{wk_start}"
            row, orders, sales, cur, nvr = _build_period_row(now_str, url, token, period_key, wk_start, wk_end)
            kpi_rows.append(row)

            for ch, v in p.calc_rs(orders, sales.get("pct_gm", 0)).items():
                rs_rows.append([now_str, period_key, ch, v["amount"], v["pct"], v["gross_profit"], v["gross_margin"], "", "", str(v["gp_is_estimate"])])

            nvr_rows.append([now_str, period_key, str(wk_start), str(wk_end), nvr.get("new_customers",0), nvr.get("returning_customers",0), nvr.get("new_revenue",0), nvr.get("returning_revenue",0), cur.get("new_gross_profit",0), cur.get("returning_gross_profit",0)])
            wk_start += timedelta(days=7)

        print(f"\n  Writing {len(kpi_rows)} KPI compare rows for {brand_name}...")
        p.write_all(gc, cfg["sheet_id"], kpi_rows, rs_rows, nvr_rows, brand_name)
        print(f"  ✓ {brand_name.upper()} compare backfill done")


if __name__ == "__main__":
    main()
