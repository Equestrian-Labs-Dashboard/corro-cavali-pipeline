name: Historical Dashboard Backfill SAFE

on:
  workflow_dispatch:

jobs:
  backfill:
    runs-on: ubuntu-latest
    timeout-minutes: 360

    steps:
      - name: Checkout repo
        uses: actions/checkout@v4

      - name: Set up Python
        uses: actions/setup-python@v5
        with:
          python-version: '3.11'

      - name: Install dependencies
        run: |
          pip install requests gspread google-auth pytz

      - name: Verify SAFE backfill file
        run: |
          echo "Checking backfill_safe.py..."
          if [ ! -f backfill_safe.py ]; then
            echo "::error::backfill_safe.py is missing in the repo root. Upload it before running."
            exit 1
          fi

          if grep -q "Limpiando tabs existentes" backfill_safe.py; then
            echo "::error::OLD / UNSAFE code detected inside backfill_safe.py. It would clear Sheets at start."
            exit 1
          fi

          grep -n "FAST / SAFE MODE\|Safe mode\|tabs are NOT cleared" backfill_safe.py || {
            echo "::error::SAFE marker not found. Upload the correct file."
            exit 1
          }

      - name: Run SAFE historical backfill
        env:
          SHOPIFY_TOKEN_CORRO: ${{ secrets.SHOPIFY_TOKEN_CORRO }}
          SHOPIFY_TOKEN_CAVALI: ${{ secrets.SHOPIFY_TOKEN_CAVALI }}
          GOOGLE_CREDENTIALS: ${{ secrets.GOOGLE_CREDENTIALS }}
          SMARTRR_API_KEY_CAVALI: ${{ secrets.SMARTRR_API_KEY_CAVALI }}
        run: python -u backfill_safe.py
