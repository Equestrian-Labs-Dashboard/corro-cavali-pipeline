# LTV v4.7 — period-aware historical cohort KPI

## What changed
- The existing/manual LTV ($178 fallback) is untouched.
- `Shopify LTV (12M · as of filter)` no longer uses the newest cohort for every dashboard date.
- `ltv_monthly` is backfilled with the Shopify acquisition cohorts needed for dashboard filters from 2025 onward.
- Month/MTD, Week and Quarter resolve the LTV using the selected period end.
- If the exact historical cohort is missing, the dashboard shows `—` instead of repeating a wrong latest value.

## Date rule
LTV is a 12-month cohort metric, so it is treated as an **as-of** KPI rather than summed across a period.

- Historical full Month / Quarter ending at month-end: includes that closed month.
- Current/partial Month / Quarter: uses the previous fully closed month.
- Week: uses the latest fully closed calendar month available by that week-end.

Examples at 2026-08-31:
- January 2026 -> cohort February 2025, measured Feb-2025 through Jan-2026.
- July 2026 -> cohort August 2025, measured Aug-2025 through Jul-2026.
- Current August 2026 MTD -> cohort August 2025, because August is not closed yet.
- A week ending 2026-01-11 -> cohort January 2025, using December 2025 as the last fully closed month.

## Automatic refresh
`pipeline.py` runs on the existing schedule. It:
1. Backfills missing historical cohorts (default support from dashboard year 2025).
2. Keeps historical completed cohorts fixed.
3. Refreshes the newest matured cohort once per calendar month.

Environment overrides if ever needed:
- `LTV_DASHBOARD_HISTORY_START_YEAR` (default `2025`)
- `LTV_BACKFILL_MAX_PER_RUN` (default `36` cohorts per brand)

## First deployment
After deploying this version, run the existing GitHub Action once. The first run can take longer because it fills historical `ltv_monthly` rows. Then hard-refresh the dashboard or use its Refresh button.
