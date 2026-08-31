# LTV v4.8 — Month / MTD + Quarter only

## Methodology decision

LTV 12M is based on Shopify monthly acquisition cohorts and 12 fully closed calendar months. It is therefore displayed only for:

- Month / MTD
- Quarter

It is intentionally **not displayed for Week**. A weekly LTV would be a repeated or interpolated monthly observation rather than a true weekly cohort metric and could be misleading.

## Dashboard behavior

When **Week** is selected:

- Current LTV ($178/manual reference) is not displayed in Section 05.
- Shopify LTV 12M is not displayed.
- LTV / CAC ratio and health are not displayed.
- The manual LTV field and LTV-derived cards are hidden for the weekly view.
- A methodology message states that LTV is available only for Month / MTD and Quarter.

When **Month / MTD** or **Quarter** is selected:

- Existing Current LTV remains unchanged.
- Shopify LTV 12M is selected historically according to the filter end date.
- Historical Shopify LTV never falls back to the newest/current cohort if the matching matured cohort is unavailable.

## Pipeline

No change to the existing scheduler is required. `ltv_monthly` remains the historical monthly cohort store and continues to update/backfill through the existing pipeline.
