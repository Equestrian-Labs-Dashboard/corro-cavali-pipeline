"""
Pipeline CORRO / CAVALI v4.5 — CORRECTO REAL
=============================================
Cambios clave de esta versión:
- Cavali sección 06 YA NO usa la pestaña vieja de resumen Smartrr.
- YA NO consulta subscription-contract en Shopify GraphQL.
- Escribe la pestaña correcta: smartrr_product_volume.
- Para Cavali/Smartrr usa Order Line Item Created Date para el filtro de fecha.
- Escribe por producto/variant:
  active_subscribers_to_date = total activo acumulado por producto hasta el fin del filtro.
  new_subscribers = nuevos del rango seleccionado.
- Si Smartrr no entrega líneas utilizables, usa fallback de Shopify order line_items para no dejar vacío.
- Mantiene kpis_daily, revenue_share, new_vs_returning y ad_spend.

EJECUCIÓN:
  python -u pipeline.py

Requiere env vars:
  SHOPIFY_TOKEN_CORRO
  SHOPIFY_TOKEN_CAVALI
  GOOGLE_CREDENTIALS
  SMARTRR_API_KEY_CAVALI opcional/recomendado
"""

import os, json, time, random, requests, gspread, calendar
from google.oauth2.service_account import Credentials
from datetime import datetime, timedelta, date
import pytz
import re

# SANITY MARKERS expected in GitHub Actions logs:
#   ✅ smartrr_product_volume: ... refreshed rows
#   ❌ should NOT print the old Smartrr subscriber-summary tab logs
#   ❌ should NOT request Shopify subscription contracts

TIMEZONE    = pytz.timezone("America/Bogota")
GQL_VERSION = "2025-10"

STORES = {
    "cavali": {
        # Uses the same GitHub Secrets you already have. Defaults keep the old behavior intact.
        "url":      os.environ.get("SHOPIFY_URL_CAVALI", "cavali-club.myshopify.com"),
        "token":    os.environ["SHOPIFY_TOKEN_CAVALI"],
        "sheet_id": os.environ.get("SHEET_ID_CAVALI", "1QUdJc2EIdElIX5nlLQxWxS98aAz-TgQnSg9glJpNtig"),
    },
    "corro":  {
        # Uses the same GitHub Secrets you already have. Defaults keep the old behavior intact.
        "url":      os.environ.get("SHOPIFY_URL_CORRO", "equestrian-labs.myshopify.com"),
        "token":    os.environ["SHOPIFY_TOKEN_CORRO"],
        "sheet_id": os.environ.get("SHEET_ID_CORRO", "1nq8xkDzowAvhD3wpMBlVK2M3FZSNS2DrAiPxz-Y2tdU"),
    },
}
SCOPES = [
    "https://www.googleapis.com/auth/spreadsheets",
    "https://www.googleapis.com/auth/drive",
]

HEADERS = [
    "updated_at", "period", "period_start", "period_end",
    "gross_sales", "net_sales", "gross_profit", "total_discounts",
    "total_returns", "cogs",
    "pct_discount", "pct_returns", "pct_gm",
    "nb_orders", "nb_units", "aov", "units_per_order",
    "sessions", "unique_visitors", "conversion_rate",
    "new_customers", "returning_customers",
    "new_revenue", "returning_revenue",
    "new_gross_profit", "returning_gross_profit",
]

AD_SPEND_DATA = {
    "corro": {
        "2024-01": {"spend": 82069,  "roas": 2.12, "cos": 0.472},
        "2024-02": {"spend": 38738,  "roas": 2.94, "cos": 0.341},
        "2024-03": {"spend": 39391,  "roas": 3.24, "cos": 0.309},
        "2024-04": {"spend": 16371,  "roas": 6.22, "cos": 0.161},
        "2024-05": {"spend": 7909,   "roas": 13.78,"cos": 0.073},
        "2024-06": {"spend": 19752,  "roas": 4.98, "cos": 0.201},
        "2024-07": {"spend": 10491,  "roas": 6.21, "cos": 0.161},
        "2024-08": {"spend": 16110,  "roas": 5.34, "cos": 0.187},
        "2024-09": {"spend": 18786,  "roas": 4.54, "cos": 0.220},
        "2024-10": {"spend": 22284,  "roas": 3.95, "cos": 0.253},
        "2024-11": {"spend": 30959,  "roas": 3.77, "cos": 0.265},
        "2024-12": {"spend": 22994,  "roas": 4.84, "cos": 0.207},
        "2025-01": {"spend": 32136,  "roas": 2.77, "cos": 0.362},
        "2025-02": {"spend": 26531,  "roas": 4.16, "cos": 0.240},
        "2025-03": {"spend": 32810,  "roas": 3.64, "cos": 0.275},
        "2025-04": {"spend": 40677,  "roas": 3.19, "cos": 0.313},
        "2025-05": {"spend": 59424,  "roas": 2.88, "cos": 0.348},
        "2025-06": {"spend": 45524,  "roas": 3.23, "cos": 0.310},
        "2025-07": {"spend": 51788,  "roas": 3.10, "cos": 0.322},
        "2025-08": {"spend": 27828,  "roas": 3.72, "cos": 0.269},
        "2025-09": {"spend": 36960,  "roas": 3.34, "cos": 0.300},
        "2025-10": {"spend": 45790,  "roas": 2.95, "cos": 0.339},
        "2025-11": {"spend": 41051,  "roas": 4.08, "cos": 0.245},
        "2025-12": {"spend": 36657,  "roas": 3.55, "cos": 0.282},
        "2026-01": {"spend": 33133,  "roas": 3.77, "cos": 0.265},
        "2026-02": {"spend": 16470,  "roas": 4.56, "cos": 0.219},
        "2026-03": {"spend": 0,      "roas": 0,    "cos": 0},
        "2026-04": {"spend": 7883,   "roas": 3.85, "cos": 0.260},
    },
    "cavali": {},
}


SMARTRR_API_KEYS = {
    # Store these in GitHub repository secrets. Do not commit them in HTML.
    "cavali": os.environ.get("SMARTRR_API_KEY_CAVALI") or os.environ.get("SMARTRR_TOKEN_CAVALI") or "",
    "corro":  os.environ.get("SMARTRR_API_KEY_CORRO")  or os.environ.get("SMARTRR_TOKEN_CORRO")  or "",
}


# ─────────────────────────────────────────────────────────────────
# GOOGLE SHEETS
# ─────────────────────────────────────────────────────────────────
def get_gc():
    creds = Credentials.from_service_account_info(
        json.loads(os.environ["GOOGLE_CREDENTIALS"]), scopes=SCOPES)
    return gspread.authorize(creds)

# ─────────────────────────────────────────────────────────────────
# SHOPIFY GQL — raw request
# ─────────────────────────────────────────────────────────────────
def gql(store_url, token, query):
    r = requests.post(
        f"https://{store_url}/admin/api/{GQL_VERSION}/graphql.json",
        headers={"X-Shopify-Access-Token": token, "Content-Type": "application/json"},
        json={"query": query}, timeout=60,
    )
    if r.status_code != 200:
        print(f"    HTTP {r.status_code} — {r.text[:200]}")
        return None
    d = r.json()
    if d.get("errors"):
        print(f"    GQL errors: {d['errors']}")
        return None
    return d.get("data")

# ─────────────────────────────────────────────────────────────────
# ql_run — DEFINITIVO (verificado contra docs.shopify.dev 2026-01)
#
# Estructura oficial ShopifyqlQueryResponse:
#   parseErrors  [String!]!    → [] si OK, ["msg..."] si error ShopifyQL
#   tableData    ShopifyqlTableData | null
#     columns    [ShopifyqlTableDataColumn!]!
#     rows       JSON!  → lista de dicts {"col_name": "value"}
# ─────────────────────────────────────────────────────────────────
def ql_run(store_url, token, ql_query):
    """
    Ejecuta una ShopifyQL query contra la Admin API 2025-10+.
    Devuelve lista de {columna: valor} o [] si no hay datos / error.
    """
    escaped = ql_query.replace("\\", "\\\\").replace('"', '\\"')

    # parseErrors NO tiene subfields — es [String!]! (lista de strings)
    q = (
        f'{{ shopifyqlQuery(query: "{escaped}") {{ '
        f'tableData {{ columns {{ name }} rows }} '
        f'parseErrors }} }}'
    )
    data = gql(store_url, token, q)
    if not data:
        return []

    ql_obj = data.get("shopifyqlQuery") or {}

    # parseErrors = [String!]! — lista vacía [] cuando OK
    errs = ql_obj.get("parseErrors") or []
    if isinstance(errs, list) and len(errs) > 0:
        print(f"    parseErrors: {errs}")
        return []

    # tableData es null cuando hay parseErrors
    td = ql_obj.get("tableData")
    if not td:
        return []

    # rows = JSON! scalar → lista de dicts {"col_name": "value"}
    rows = td.get("rows") or []
    if not rows:
        return []

    # Tipo esperado: lista de dicts
    if isinstance(rows, list) and isinstance(rows[0], dict):
        return rows

    # Fallback defensivo por si rows llega como string JSON
    if isinstance(rows, str):
        try:
            parsed = json.loads(rows)
            if isinstance(parsed, list):
                return parsed
        except Exception:
            pass

    return []


def ql_row(store_url, token, ql_query):
    rows = ql_run(store_url, token, ql_query)
    return rows[-1] if rows else None


def _m(v):
    if v is None:
        return 0.0
    try:
        return float(str(v).replace(",", "").strip())
    except Exception:
        return 0.0


def _gm(v):
    if v is None:
        return 0.0
    try:
        f = float(str(v).replace("%", "").replace(",", "").strip())
        return round(f * 100, 2) if abs(f) <= 1.0 else round(f, 2)
    except Exception:
        return 0.0


def _until(e):
    """ShopifyQL UNTIL es exclusivo cuando e == hoy. Pasamos e+1 para incluir el día actual."""
    today = datetime.now(TIMEZONE).date()
    if e >= today:
        return e + timedelta(days=1)
    return e

# ─────────────────────────────────────────────────────────────────
# FETCH: SALES
# ─────────────────────────────────────────────────────────────────
def fetch_sales(url, token, s, e):
    e_ql = _until(e)
    row  = ql_row(url, token,
        f"FROM sales SHOW gross_sales, discounts, returns, net_sales, "
        f"cost_of_goods_sold, gross_profit, gross_margin, orders "
        f"SINCE {s} UNTIL {e_ql}")

    if not row:
        print(f"    ⚠ fetch_sales: sin datos para {s} → {e_ql}")
        return {k: 0 for k in
                ["gross_sales","discounts","returns","net_sales",
                 "cogs","gross_profit","pct_gm","orders"]}

    g  = round(_m(row.get("gross_sales")),        2)
    d  = round(abs(_m(row.get("discounts"))),      2)
    r  = round(abs(_m(row.get("returns"))),        2)
    n  = round(_m(row.get("net_sales")),           2)
    c  = round(_m(row.get("cost_of_goods_sold")),  2)
    gp = round(_m(row.get("gross_profit")),        2)
    gm = _gm(row.get("gross_margin"))
    o  = int(abs(_m(row.get("orders"))))

    print(f"    gross:{g:>12,.2f}  net:{n:>12,.2f}  gp:{gp:>10,.2f}  "
          f"cogs:{c:>9,.2f}  gm:{gm:>5.1f}%  orders:{o}  [UNTIL {e_ql}]")

    return {"gross_sales": g, "discounts": d, "returns": r, "net_sales": n,
            "cogs": c, "gross_profit": gp, "pct_gm": gm, "orders": o}

# ─────────────────────────────────────────────────────────────────
# FETCH: SESSIONS
# ─────────────────────────────────────────────────────────────────
def fetch_sessions(url, token, s, e):
    """
    Section 01 source: Shopify only.

    Formulas written to the dashboard:
      - Traffic = sessions
      - Unique Visitors = online_store_visitors
      - CR% = Transactions / Sessions, calculated in build()

    We do NOT query GA/Looker-only fields such as browser, screen_resolution,
    or new_users. ShopifyQL confirmed those are not available here.

    When available, Shopify's own bot classification is applied:
      WHERE human_or_bot_session != 'human_bot'
    If that field is not available for any store/range, the function falls back
    to Shopify sessions + online_store_visitors without that filter so the
    pipeline keeps running.
    """
    e_ql = _until(e)
    source = "ShopifyQL sessions + online_store_visitors; exclude human_bot"
    row = ql_row(url, token,
        f"FROM sessions SHOW online_store_visitors, sessions "
        f"WHERE human_or_bot_session != 'human_bot' "
        f"SINCE {s} UNTIL {e_ql}")

    if not row:
        source = "ShopifyQL sessions + online_store_visitors"
        row = ql_row(url, token,
            f"FROM sessions SHOW online_store_visitors, sessions SINCE {s} UNTIL {e_ql}")

    if not row:
        print("    sessions: 0  online_store_visitors: 0  [ShopifyQL]")
        return {"sessions": 0, "unique_visitors": 0, "traffic_source": source}

    sessions = int(abs(_m(row.get("sessions", 0))))
    unique = int(abs(_m(row.get("online_store_visitors", 0))))
    print(f"    sessions: {sessions:,}  online_store_visitors: {unique:,}  [{source}]")
    return {"sessions": sessions, "unique_visitors": unique, "traffic_source": source}

def fetch_orders_fulfilled(url, token, s, e):
    e_ql = _until(e)
    row  = ql_row(url, token,
        f"FROM fulfillments SHOW orders_fulfilled SINCE {s} UNTIL {e_ql}")
    if not row:
        return None
    v = int(abs(_m(row.get("orders_fulfilled", 0))))
    print(f"    orders_fulfilled: {v:,}")
    return v

# ─────────────────────────────────────────────────────────────────
# FETCH: REST ORDERS (new vs returning)
# ─────────────────────────────────────────────────────────────────
def rest(store_url, token, endpoint, params):
    url     = f"https://{store_url}/admin/api/2024-01/{endpoint}"
    headers = {"X-Shopify-Access-Token": token}
    results = []
    while url:
        r = requests.get(url, headers=headers, params=params, timeout=60)
        r.raise_for_status()
        data = r.json()
        key  = list(data.keys())[0]
        results.extend(data[key])
        link = r.headers.get("Link", "")
        url  = None
        params = {}
        if 'rel="next"' in link:
            for part in link.split(","):
                if 'rel="next"' in part:
                    url = part.split(";")[0].strip().strip("<>")
    return results




CUSTOMER_ORDER_COUNT_CACHE = {}
CUSTOMER_FIRST_ORDER_DATE_CACHE = {}


def _shopify_rest_get_json_with_retry(store_url, token, endpoint, params=None, max_retries=7):
    """Small REST GET helper for customer enrichment, with 429/5xx retry."""
    url = f"https://{store_url}/admin/api/2024-01/{endpoint}"
    headers = {"X-Shopify-Access-Token": token}
    params = params or {}
    last_resp = None
    for attempt in range(max_retries):
        r = requests.get(url, headers=headers, params=params, timeout=60)
        last_resp = r
        if r.status_code == 429 or r.status_code in (500, 502, 503, 504):
            retry_after = r.headers.get("Retry-After")
            if retry_after:
                try:
                    sleep_for = float(retry_after)
                except Exception:
                    sleep_for = 2.0
            else:
                sleep_for = min(45, (2 ** attempt) + random.random())
            print(f"    Shopify REST {r.status_code} on {endpoint}; retrying in {sleep_for:.1f}s")
            time.sleep(sleep_for)
            continue
        r.raise_for_status()
        lim = r.headers.get("X-Shopify-Shop-Api-Call-Limit", "")
        try:
            used, cap = [int(x) for x in lim.split("/", 1)]
            if cap and used / cap >= 0.80:
                time.sleep(0.75)
        except Exception:
            pass
        return r.json()
    if last_resp is not None:
        last_resp.raise_for_status()
    return {}


def _order_customer_id(order):
    customer = order.get("customer") or {}
    cid = customer.get("id")
    return str(cid) if cid not in (None, "") else ""


def enrich_orders_with_customer_order_counts(store_url, token, orders):
    """
    Enrich each order with:
      - customer.orders_count when available
      - customer._first_order_created_at from Shopify Customer Orders

    New vs Returning is then date-aware:
      first-ever order before this order => Returning
      this is the customer's first-ever order => New
    This avoids the dashboard showing every Cavali subscription order as New.
    """
    ids = []
    seen = set()
    for o in orders or []:
        cid = _order_customer_id(o)
        if not cid or cid in seen:
            continue
        seen.add(cid)
        customer = o.get("customer") or {}
        current = customer.get("orders_count")
        if current not in (None, ""):
            try:
                CUSTOMER_ORDER_COUNT_CACHE[cid] = int(current)
            except Exception:
                pass
        if cid not in CUSTOMER_FIRST_ORDER_DATE_CACHE:
            ids.append(cid)

    for i, cid in enumerate(ids, 1):
        try:
            data = _shopify_rest_get_json_with_retry(
                store_url, token, f"customers/{cid}/orders.json",
                {"status": "any", "limit": 1, "order": "created_at asc", "fields": "id,created_at"}
            )
            first_order = (data.get("orders") or [{}])[0]
            CUSTOMER_FIRST_ORDER_DATE_CACHE[cid] = first_order.get("created_at") or ""
        except Exception as e:
            print(f"    ⚠ first-order fallback cid={cid}: {e}")
            CUSTOMER_FIRST_ORDER_DATE_CACHE[cid] = ""

        # orders_count is only used as fallback if first-order lookup fails.
        if cid not in CUSTOMER_ORDER_COUNT_CACHE:
            try:
                data = _shopify_rest_get_json_with_retry(store_url, token, f"customers/{cid}.json", {"fields": "id,orders_count"})
                customer = data.get("customer") or {}
                CUSTOMER_ORDER_COUNT_CACHE[cid] = int(customer.get("orders_count", 1) or 1)
            except Exception as e:
                print(f"    ⚠ customer orders_count fallback cid={cid}: {e}")
                CUSTOMER_ORDER_COUNT_CACHE[cid] = 1

        if i % 35 == 0:
            time.sleep(0.5)

    for o in orders or []:
        cid = _order_customer_id(o)
        if not cid:
            continue
        if not o.get("customer"):
            o["customer"] = {"id": cid}
        o["customer"]["orders_count"] = CUSTOMER_ORDER_COUNT_CACHE.get(cid, o["customer"].get("orders_count", 1))
        o["customer"]["_first_order_created_at"] = CUSTOMER_FIRST_ORDER_DATE_CACHE.get(cid, "")
    return orders


def fetch_new_vs_returning(url, token, s, e):
    orders = rest(url, token, "orders.json", {
        "status":           "any",
        "financial_status": "paid,partially_paid,partially_refunded,refunded",
        "created_at_min":   f"{s}T00:00:00-05:00",
        "created_at_max":   f"{e}T23:59:59-05:00",
        "limit":            250,
        "fields":           "id,subtotal_price,created_at,customer",
    })
    orders = enrich_orders_with_customer_order_counts(url, token, orders)

    result = {
        "new_customers":          0,
        "returning_customers":    0,
        "new_revenue":            0.0,
        "returning_revenue":      0.0,
    }

    for o in orders:
        amt      = float(o.get("subtotal_price", 0) or 0)
        customer = o.get("customer") or {}
        first_dt = _parse_shopify_dt(customer.get("_first_order_created_at"))
        order_dt = _parse_shopify_dt(o.get("created_at"))
        if first_dt and order_dt:
            is_returning = first_dt < order_dt
        else:
            count = int(customer.get("orders_count", 1) or 1)
            is_returning = count > 1

        if is_returning:
            result["returning_customers"] += 1
            result["returning_revenue"]   += amt
        else:
            result["new_customers"] += 1
            result["new_revenue"]   += amt

    result["new_revenue"]       = round(result["new_revenue"],       2)
    result["returning_revenue"] = round(result["returning_revenue"],  2)

    print(f"    new_customers:{result['new_customers']:>5}  "
          f"new_rev:{result['new_revenue']:>10,.2f}  "
          f"ret:{result['returning_customers']:>5}  "
          f"ret_rev:{result['returning_revenue']:>10,.2f}")
    return result


def fetch_orders(url, token, s, e):
    orders = rest(url, token, "orders.json", {
        "status":           "any",
        "financial_status": "paid,partially_paid,partially_refunded,refunded",
        "created_at_min":   f"{s}T00:00:00-05:00",
        "created_at_max":   f"{e}T23:59:59-05:00",
        "limit":            250,
        "fields":           "id,subtotal_price,created_at,line_items,source_name,tags,customer",
    })
    return enrich_orders_with_customer_order_counts(url, token, orders)


def calc_units(orders):
    return sum(
        sum(int(li.get("quantity", 0) or 0) for li in o.get("line_items", []))
        for o in orders
    )


def calc_rs(orders, overall_gm_pct):
    """
    Revenue share by channel.

    Important: Gross Profit by channel is NOT estimated here anymore.
    The dashboard only shows GP when a real channel-level GP source exists.
    """
    ch    = {"Wellington (POS)": 0., "Concierge": 0., "Online": 0., "Others": 0.}
    total = 0.
    for o in orders:
        amt    = float(o.get("subtotal_price", 0) or 0)
        total += amt
        src    = (o.get("source_name") or "").lower().strip()
        tags   = (o.get("tags") or "").lower()
        if src == "pos" or "wellington" in tags or "pos" in tags:
            ch["Wellington (POS)"] += amt
        elif "concierge" in tags or "concierge" in src:
            ch["Concierge"] += amt
        elif src in ("web", "shopify", "", "online_store") or not src:
            ch["Online"] += amt
        else:
            ch["Others"] += amt

    result = {}
    for k, v in ch.items():
        pct = round(v / total * 100, 2) if total else 0
        result[k] = {
            "amount":         round(v, 2),
            "pct":            pct,
            "gross_profit":   "",
            "gross_margin":   "",
            "gp_is_estimate": False,
        }
    return result

# ─────────────────────────────────────────────────────────────────
# BUILD KPI DICT
# ─────────────────────────────────────────────────────────────────
def build(sales, orders, nvr, sessions=0, orders_fulfilled=None):
    g  = sales.get("gross_sales",  0)
    d  = sales.get("discounts",    0)
    r  = sales.get("returns",      0)
    n  = sales.get("net_sales",    0)
    c  = sales.get("cogs",         0)
    gp = sales.get("gross_profit", 0)
    gm = sales.get("pct_gm",       0)
    nb = int(orders_fulfilled) if orders_fulfilled is not None \
         else (sales.get("orders", 0) or len(orders))

    units = calc_units(orders)
    aov   = round(n / nb,     2) if nb   else 0
    upo   = round(units / nb, 2) if nb   else 0
    pdisc = round(d / g * 100, 2) if g   else 0
    pret  = round(r / g * 100, 2) if g   else 0
    if isinstance(sessions, dict):
        sess = int(sessions.get("sessions") or 0)
        uv   = int(sessions.get("unique_visitors") or sessions.get("new_users") or 0)
    else:
        sess = int(sessions or 0)
        uv   = 0
    # CR% = Transactions / Sessions. Use Shopify sales orders as the
    # closest available transaction count; fulfilled orders can be higher/lower
    # because it measures operations, not checkout transactions.
    transactions = int(sales.get("orders", 0) or nb or 0)
    cr    = round(transactions / sess * 100, 4) if sess else 0

    gm_rate = gm / 100 if gm > 0 else (gp / n if n > 0 else 0)
    new_gp  = round(nvr.get("new_revenue",       0) * gm_rate, 2)
    ret_gp  = round(nvr.get("returning_revenue", 0) * gm_rate, 2)

    return {
        "gross_sales":            g,
        "net_sales":              n,
        "gross_profit":           gp,
        "total_discounts":        d,
        "total_returns":          r,
        "cogs":                   c,
        "pct_discount":           pdisc,
        "pct_returns":            pret,
        "pct_gm":                 gm,
        "nb_orders":              nb,
        "nb_units":               units,
        "aov":                    aov,
        "units_per_order":        upo,
        "sessions":               sess,
        "unique_visitors":        uv,
        "conversion_rate":        cr,
        "transactions":           transactions,
        "new_customers":          nvr.get("new_customers",       0),
        "returning_customers":    nvr.get("returning_customers", 0),
        "new_revenue":            nvr.get("new_revenue",         0),
        "returning_revenue":      nvr.get("returning_revenue",   0),
        "new_gross_profit":       new_gp,
        "returning_gross_profit": ret_gp,
    }


def make_kpi_row(now_str, period_key, s, e, cur):
    return [
        now_str, period_key, str(s), str(e),
        cur.get("gross_sales",            0),
        cur.get("net_sales",              0),
        cur.get("gross_profit",           0),
        cur.get("total_discounts",        0),
        cur.get("total_returns",          0),
        cur.get("cogs",                   0),
        cur.get("pct_discount",           0),
        cur.get("pct_returns",            0),
        cur.get("pct_gm",                 0),
        cur.get("nb_orders",              0),
        cur.get("nb_units",               0),
        cur.get("aov",                    0),
        cur.get("units_per_order",        0),
        cur.get("sessions",               0),
        cur.get("unique_visitors",        0),
        cur.get("conversion_rate",        0),
        cur.get("new_customers",          0),
        cur.get("returning_customers",    0),
        cur.get("new_revenue",            0),
        cur.get("returning_revenue",      0),
        cur.get("new_gross_profit",       0),
        cur.get("returning_gross_profit", 0),
    ]

# ─────────────────────────────────────────────────────────────────
# PERIODS
# ─────────────────────────────────────────────────────────────────
def get_periods():
    today = datetime.now(TIMEZONE).date()
    dow   = today.weekday()  # 0=Mon

    mtd_s  = today.replace(day=1)
    mtd_e  = today
    mtd_pk = f"mtd_{today.strftime('%Y-%m')}"

    prev_mo_end    = mtd_s - timedelta(days=1)
    prev_mo_s      = prev_mo_end.replace(day=1)
    prev_mo_mtd_e  = prev_mo_end.replace(day=min(today.day, prev_mo_end.day))
    prev_mo_mtd_pk = f"mtd_{prev_mo_s.strftime('%Y-%m')}"

    yoy_mtd_s = mtd_s.replace(year=mtd_s.year - 1)
    yoy_mtd_e = today.replace(year=today.year - 1)
    yoy_mtd_pk = f"mtd_{yoy_mtd_s.strftime('%Y-%m')}"

    wk_s   = today - timedelta(days=dow)
    wk_e   = today
    wk_pk  = f"week_{wk_s}"

    pwk_e  = wk_s - timedelta(days=1)
    pwk_s  = pwk_e - timedelta(days=6)
    pwk_pk = f"week_{pwk_s}"

    yoy_wk_s = wk_s - timedelta(days=364)
    yoy_wk_e = wk_e - timedelta(days=364)

    mo_e   = mtd_s - timedelta(days=1)
    mo_s   = mo_e.replace(day=1)
    mo_pk  = mo_s.strftime("%Y-%m")

    pmo_e  = mo_s - timedelta(days=1)
    pmo_s  = pmo_e.replace(day=1)
    pmo_pk = pmo_s.strftime("%Y-%m")

    yoy_mo_s = mo_s.replace(year=mo_s.year - 1)
    yoy_mo_e = mo_e.replace(year=mo_e.year - 1)

    q_num = (today.month - 1) // 3 + 1
    q_s   = today.replace(month=(q_num - 1) * 3 + 1, day=1)
    q_e   = today
    q_pk  = f"q{q_num}_{today.year}"

    pq    = q_num - 1 if q_num > 1 else 4
    pq_y  = today.year if q_num > 1 else today.year - 1
    pq_s  = date(pq_y, (pq - 1) * 3 + 1, 1)
    pq_em = pq * 3
    pq_e  = date(pq_y, pq_em, calendar.monthrange(pq_y, pq_em)[1])
    pq_pk = f"q{pq}_{pq_y}"

    yoy_q_s  = q_s.replace(year=q_s.year - 1)
    yoy_q_e  = today.replace(year=today.year - 1)
    yoy_q_pk = f"q{q_num}_{today.year - 1}"

    return {
        "mtd":          (mtd_s,        mtd_e,         mtd_pk),
        # Keep MTD comparison rows as exact date-to-date ranges (e.g. May 1–7 vs Apr 1–7).
        "mtd_prev":     (prev_mo_s,    prev_mo_mtd_e, prev_mo_mtd_pk),
        "mtd_yoy":      (yoy_mtd_s,    yoy_mtd_e,     yoy_mtd_pk),
        "week":         (wk_s,         wk_e,           wk_pk),
        "week_prev":    (pwk_s,        pwk_e,          pwk_pk),
        "week_yoy":     (yoy_wk_s,     yoy_wk_e,       None),
        "month":        (mo_s,         mo_e,            mo_pk),
        "month_prev":   (pmo_s,        pmo_e,           pmo_pk),
        "month_yoy":    (yoy_mo_s,     yoy_mo_e,        None),
        "quarter":      (q_s,          q_e,             q_pk),
        "quarter_prev": (pq_s,         pq_e,            pq_pk),
        "quarter_yoy":  (yoy_q_s,      yoy_q_e,         yoy_q_pk),
    }


# ─────────────────────────────────────────────────────────────────
# SMARTRR — product volume by Product-Variant using CREATED DATE
# ─────────────────────────────────────────────────────────────────

SMARTRR_PRODUCT_VOLUME_HEADERS = [
    "updated_at", "brand", "period", "period_start", "period_end",
    "product_variant", "sku",
    "total_quantity", "new_subscribers",
    # Named *_current so the dashboard JS reads them via activeOf / pausedOf / cancelledOf
    "active_subscribers_current", "paused_subscribers_current", "cancelled_subscribers_current",
    "gross_revenue", "source", "date_basis", "active_filter", "sample_line_ids",
]


def _norm_txt(v):
    return re.sub(r"\s+", " ", str(v or "").strip())


def _norm_key(k):
    return re.sub(r"[^a-z0-9]", "", str(k or "").lower())


def _dig(obj, *paths):
    """Return first non-empty nested value from a dict/list using dot paths."""
    for path in paths:
        cur = obj
        ok = True
        for part in path.split("."):
            if isinstance(cur, dict):
                cur = cur.get(part)
            elif isinstance(cur, list):
                try:
                    cur = cur[int(part)]
                except Exception:
                    ok = False
                    break
            else:
                ok = False
                break
        if ok and cur not in (None, ""):
            return cur
    return ""


def _deep_values_for_keys(obj, key_names, depth=0, limit=80):
    """Find values anywhere in a nested object for loose key-name matches."""
    out = []
    wanted = {_norm_key(k) for k in key_names}
    if obj is None or depth > 8 or len(out) >= limit:
        return out
    if isinstance(obj, dict):
        for k, v in obj.items():
            nk = _norm_key(k)
            if nk in wanted and v not in (None, ""):
                out.append(v)
            if isinstance(v, (dict, list)) and len(out) < limit:
                out.extend(_deep_values_for_keys(v, key_names, depth + 1, limit - len(out)))
    elif isinstance(obj, list):
        for v in obj[:80]:
            if len(out) >= limit:
                break
            out.extend(_deep_values_for_keys(v, key_names, depth + 1, limit - len(out)))
    return out


def _smartrr_items(payload):
    """Normalize Smartrr list responses into a list."""
    if isinstance(payload, list):
        return payload
    if not isinstance(payload, dict):
        return []
    for key in (
        "data", "items", "results", "records", "purchaseStates", "purchase_states",
        "purchaseState", "purchase_state", "subscriptions", "subscription_contracts", "contracts",
    ):
        val = payload.get(key)
        if isinstance(val, list):
            return val
        if isinstance(val, dict):
            nested = _smartrr_items(val)
            if nested:
                return nested
    return []


def _smartrr_total_hint(payload):
    if not isinstance(payload, dict):
        return None
    for path in ("total", "totalCount", "count", "meta.total", "pagination.total", "page.total"):
        val = _dig(payload, path)
        if val not in (None, ""):
            try:
                return int(float(str(val).replace(",", "")))
            except Exception:
                pass
    return None


def _smartrr_headers(api_key, mode="token"):
    if mode == "bearer":
        return {
            "Authorization": f"Bearer {api_key}",
            "Accept": "application/json",
            "Content-Type": "application/json",
        }
    return {
        "x-smartrr-access-token": api_key,
        "Accept": "application/json",
        "Content-Type": "application/json",
    }


def _smartrr_get(url, api_key, params=None):
    r = requests.get(url, headers=_smartrr_headers(api_key, "token"), params=params, timeout=60)
    if r.status_code in (401, 403):
        rb = requests.get(url, headers=_smartrr_headers(api_key, "bearer"), params=params, timeout=60)
        if rb.status_code < 400:
            return rb
    return r


def _smartrr_status(subscription):
    return str(
        _dig(subscription, "purchaseStateStatus") or
        _dig(subscription, "purchase_state_status") or
        _dig(subscription, "status") or
        _dig(subscription, "subscriptionStatus") or
        _dig(subscription, "subscription_status") or
        _dig(subscription, "state") or
        _dig(subscription, "sts.0.purchaseStateStatus") or
        _dig(subscription, "sts.0.status")
    ).strip().lower()


def _smartrr_status_group(subscription):
    """Return active / paused / inactive for Smartrr purchase states.

    Priority order:
    1. Explicit cancelled/deleted markers → inactive (regardless of hint)
    2. Explicit paused markers or status → paused
    3. Hint (set from the API filter used) when status field is empty/missing
    4. Explicit active status → active
    5. Empty status with no hint → active (fallback, rare)
    """
    status = _smartrr_status(subscription)
    hint = str(subscription.get("_smartrr_status_hint", "") if isinstance(subscription, dict) else "").strip().lower()

    # 1 — Hard cancelled/deleted signals always win (even if hint says active/paused)
    cancelled = (
        _dig(subscription, "cancelledAt") or _dig(subscription, "cancelled_at") or
        _dig(subscription, "deletedAt")   or _dig(subscription, "deleted_at")
    )
    if cancelled or status in ("cancelled", "canceled", "inactive", "expired", "deleted"):
        return "inactive"

    # 2 — Explicit paused fields
    paused_marker = (
        _dig(subscription, "pausedAt")        or _dig(subscription, "paused_at") or
        _dig(subscription, "pauseStartedAt")  or _dig(subscription, "pause_started_at") or
        _dig(subscription, "pausedUntil")     or _dig(subscription, "paused_until")
    )
    if paused_marker or status in ("paused", "pause", "pausing", "suspended"):
        return "paused"

    # 3 — When the status field returned nothing, trust the API filter hint.
    #     (Smartrr sometimes omits purchaseStateStatus in the payload body even
    #      though we filtered by it — the hint carries the requested_status value.)
    if not status and hint:
        if hint in ("paused", "pause", "pausing", "suspended"):
            return "paused"
        if hint in ("active", "activated"):
            return "active"

    # 4 — Explicit active
    if status in ("active", "activated"):
        return "active"

    # 5 — Empty status, no hint → default active (was already the behaviour)
    if status == "":
        return "active"

    # Any other unrecognised value → inactive
    return "inactive"


def _smartrr_is_active(subscription):
    """Backward-compatible helper used by older diagnostics."""
    return _smartrr_status_group(subscription) == "active"


def _smartrr_is_active_or_paused(subscription):
    return _smartrr_status_group(subscription) in ("active", "paused")


def _parse_smartrr_date(v):
    """Parse Smartrr/Shopify datetimes. Returns a date in local dashboard timezone."""
    if v in (None, "", "ø", "null", "None"):
        return None
    s = str(v).strip()
    # Smartrr UI often shows `2026-04-17 14:02:55.033`.
    for fmt in ("%Y-%m-%d", "%Y-%m-%d %H:%M:%S", "%Y-%m-%d %H:%M:%S.%f"):
        try:
            return datetime.strptime(s[:26], fmt).date()
        except Exception:
            pass
    try:
        dt = datetime.fromisoformat(s.replace("Z", "+00:00"))
        if dt.tzinfo:
            dt = dt.astimezone(TIMEZONE)
        return dt.date()
    except Exception:
        return None


def _to_number(v, default=0.0):
    if v in (None, "", "ø", "null", "None"):
        return default
    try:
        return float(str(v).replace(",", "").replace("USD", "").strip())
    except Exception:
        return default


def _money_to_usd(v):
    n = _to_number(v, 0.0)
    # Smartrr ORM price commonly comes as cents, e.g. 6900 = $69.
    if abs(n) >= 1000:
        return n / 100.0
    return n


def _first_deep(obj, keys):
    vals = _deep_values_for_keys(obj, keys, limit=8)
    for v in vals:
        if v not in (None, "", "ø"):
            return v
    return ""


def _candidate_line_dicts(obj, depth=0, out=None):
    """Find nested dicts that look like Smartrr order line items."""
    if out is None:
        out = []
    if obj is None or depth > 9:
        return out
    if isinstance(obj, dict):
        keys = {_norm_key(k) for k in obj.keys()}
        has_qty = any(k in keys for k in ("quantity", "qty"))
        has_product = any(k in keys for k in (
            "purchasableandpurchasablevariantname", "productvariant", "productvariantname",
            "producttitle", "productname", "title", "name", "varianttitle", "variantname",
        )) or bool(_first_deep(obj, [
            "purchasable_and_purchasable_variant_name", "purchasableAndPurchasableVariantName",
            "productTitle", "product_title", "productName", "product_name", "variantTitle", "variant_title",
        ]))
        has_created = any("created" in k for k in keys)
        has_line_marker = any("lineitem" in k or "orderlineitem" in k for k in keys)
        if has_qty and has_product and (has_created or has_line_marker):
            out.append(obj)
        for v in obj.values():
            if isinstance(v, (dict, list)):
                _candidate_line_dicts(v, depth + 1, out)
    elif isinstance(obj, list):
        for v in obj[:500]:
            _candidate_line_dicts(v, depth + 1, out)
    return out


def _line_created_date(line):
    # IMPORTANT: this intentionally uses the line item's Created Date, matching the Smartrr drilldown.
    val = (
        _dig(line, "createdDate") or _dig(line, "created_date") or
        _dig(line, "createdAt") or _dig(line, "created_at") or
        _dig(line, "created") or _dig(line, "lineCreatedDate") or
        _dig(line, "orderLineItemCreatedDate") or
        _first_deep(line, [
            "createdDate", "created_date", "createdAt", "created_at", "created",
            "lineCreatedDate", "orderLineItemCreatedDate",
        ])
    )
    return _parse_smartrr_date(val)


def _line_deleted(line):
    val = (
        _dig(line, "deletedAt") or _dig(line, "deleted_at") or
        _dig(line, "deleted") or _first_deep(line, ["deletedAt", "deleted_at", "deleted"])
    )
    return bool(val and str(val).strip() not in ("", "ø", "null", "None"))


def _line_product(line):
    vals = [
        _dig(line, "purchasable_and_purchasable_variant_name"),
        _dig(line, "purchasableAndPurchasableVariantName"),
        _dig(line, "productVariant"), _dig(line, "productVariantName"),
        _dig(line, "product_variant"), _dig(line, "product_variant_name"),
        _dig(line, "variantTitle"), _dig(line, "variant_title"),
        _dig(line, "productTitle"), _dig(line, "product_title"),
        _dig(line, "productName"), _dig(line, "product_name"),
        _dig(line, "title"), _dig(line, "name"),
        _first_deep(line, [
            "purchasable_and_purchasable_variant_name", "purchasableAndPurchasableVariantName",
            "productVariantName", "product_variant_name", "productTitle", "product_title",
            "productName", "product_name", "variantTitle", "variant_title", "title", "name",
        ]),
    ]
    for v in vals:
        txt = _norm_txt(v)
        if txt and txt not in ("ø", "Default Title"):
            return txt
    return "Other"


def _line_sku(line):
    vals = [
        _dig(line, "sku"), _dig(line, "SKU"), _dig(line, "variant.sku"),
        _dig(line, "purchasableVariant.sku"), _dig(line, "purchasable_variant.sku"),
        _first_deep(line, ["sku", "SKU", "purchasableVariantSku", "purchasable_variant_sku"]),
    ]
    for v in vals:
        txt = _norm_txt(v)
        if txt:
            return txt
    return "ø"


def _line_id(line):
    vals = [
        _dig(line, "id"), _dig(line, "lineItemId"), _dig(line, "line_item_id"),
        _dig(line, "shopifyId"), _dig(line, "shopify_id"), _dig(line, "shopifyLineItemId"),
        _first_deep(line, ["id", "lineItemId", "line_item_id", "shopifyId", "shopify_id", "shopifyLineItemId"]),
    ]
    for v in vals:
        txt = _norm_txt(v)
        if txt:
            return txt
    return ""


def _line_quantity(line):
    q = _dig(line, "quantity") or _dig(line, "qty") or _first_deep(line, ["quantity", "qty"])
    n = _to_number(q, 1.0)
    return int(n) if n and n > 0 else 1


def _line_revenue(line, qty):
    gross = (
        _dig(line, "grossRevenue") or _dig(line, "gross_revenue") or
        _dig(line, "shopIncome") or _dig(line, "shop_income") or
        _dig(line, "totalPrice") or _dig(line, "total_price") or
        _dig(line, "linePrice") or _dig(line, "line_price") or
        ""
    )
    if gross not in (None, ""):
        return _money_to_usd(gross)
    price = (
        _dig(line, "price") or _dig(line, "unitPrice") or _dig(line, "unit_price") or
        _first_deep(line, ["price", "unitPrice", "unit_price"])
    )
    return _money_to_usd(price) * qty


def fetch_smartrr_active_purchase_states(brand_name):
    """Fetch ACTIVE and PAUSED purchase states once. No Shopify subscription-contract lookup."""
    key = SMARTRR_API_KEYS.get(brand_name, "")
    if brand_name != "cavali" or not key:
        if brand_name == "cavali":
            print("    ⚠ smartrr: SMARTRR_API_KEY_CAVALI missing")
        return []

    base_url = "https://api.smartrr.com/vendor/purchase-state"
    states = []
    seen = set()
    page_size = 250

    # User requirement: keep new subscribers for the selected period, but also show
    # current ACTIVE, PAUSED, and CANCELLED subscriber totals per product.
    for requested_status in ("ACTIVE", "PAUSED", "CANCELLED"):
        page_number = 0
        total_hint = None
        fetched_for_status = 0

        while page_number < 200:
            params = {
                "pageSize": page_size,
                "pageNumber": page_number,
                "filterEquals[purchaseStateStatus]": requested_status,
                "include": "items,lineItems,orderLineItems,stLineItems,product,variant,purchasableVariant,orders",
            }
            r = _smartrr_get(base_url, key, params=params)
            if r.status_code >= 400:
                print(f"    ⚠ smartrr {requested_status} HTTP {r.status_code}: {(r.text or '')[:350]}")
                break

            payload = r.json()
            items = _smartrr_items(payload)
            total_hint = _smartrr_total_hint(payload)
            if not items:
                break

            for sub in items:
                if isinstance(sub, dict):
                    sub = dict(sub)
                    sub["_smartrr_status_hint"] = requested_status.lower()

                if not _smartrr_is_active_or_paused(sub) and requested_status != "CANCELLED":
                    continue

                sid = str(
                    _dig(sub, "id") or _dig(sub, "purchaseStateId") or _dig(sub, "shopifyId") or
                    _dig(sub, "subscriptionId") or _dig(sub, "subscription_id") or
                    json.dumps(sub, sort_keys=True)[:200]
                )
                if sid in seen:
                    continue
                seen.add(sid)
                states.append(sub)
                fetched_for_status += 1

            if len(items) < page_size:
                break
            if total_hint is not None and (page_number + 1) * page_size >= total_hint:
                break
            page_number += 1

        print(f"    smartrr {requested_status.lower()} purchase states fetched: {fetched_for_status}")

    active_count = sum(1 for s in states if _smartrr_status_group(s) == "active")
    paused_count = sum(1 for s in states if _smartrr_status_group(s) == "paused")
    inactive_count = sum(1 for s in states if _smartrr_status_group(s) == "inactive")
    paused_no_status = sum(
        1 for s in states
        if str(s.get("_smartrr_status_hint","")).strip().lower() == "paused"
        and not _smartrr_status(s)
    )
    print(f"    smartrr active+paused purchase states fetched: active={active_count} paused={paused_count} inactive_filtered={inactive_count} total={len(states)}")
    if paused_no_status:
        print(f"    ⚠ smartrr: {paused_no_status} PAUSED records had empty purchaseStateStatus — classified via API filter hint")
    return states


def _smartrr_plan_text(sub):
    """
    Extract product/plan label from purchase-state root when stLineItems does not
    expose usable product fields.
    """
    for fk in (
        "planTitle", "plan_title",
        "purchasableTitle", "purchasable_title",
        "productTitle", "product_title",
        "title", "name",
        "planName", "plan_name",
        "subscriptionTitle", "subscription_title",
    ):
        v = sub.get(fk) if isinstance(sub, dict) else ""
        if v and str(v).strip() and re.search(r"[A-Za-z]", str(v)):
            return str(v).strip()

    for nested_k in ("plan", "purchasable", "subscription"):
        nested = sub.get(nested_k) if isinstance(sub, dict) else None
        if not isinstance(nested, dict):
            continue
        for fk in ("title", "name", "planTitle", "plan_title"):
            v = nested.get(fk)
            if v and str(v).strip() and re.search(r"[A-Za-z]", str(v)):
                return str(v).strip()
    return ""



def _find_variant_gids_from_smartrr_states(states):
    """Collect Shopify ProductVariant GIDs from Smartrr line-item variant objects."""
    out = set()
    def walk(obj, depth=0):
        if obj is None or depth > 8:
            return
        if isinstance(obj, str):
            m = re.search(r"gid://shopify/ProductVariant/\d+", obj)
            if m:
                out.add(m.group(0))
            return
        if isinstance(obj, dict):
            for k, v in obj.items():
                if k in ("shopifyId", "shopifyProductVariantStorefrontId", "admin_graphql_api_id", "id") and isinstance(v, str):
                    m = re.search(r"gid://shopify/ProductVariant/\d+", v)
                    if m:
                        out.add(m.group(0))
                if isinstance(v, (dict, list, str)):
                    walk(v, depth + 1)
        elif isinstance(obj, list):
            for v in obj[:500]:
                walk(v, depth + 1)
    for s in states or []:
        walk(s)
    return sorted(out)


def fetch_shopify_product_variant_titles(store_url, token, variant_gids):
    """
    Resolve ProductVariant GIDs from Smartrr's stLineItems.vnt to Shopify product titles.
    This is required because Smartrr often returns only the ProductVariant GID in vnt,
    not the human product name.
    """
    ids = sorted({v for v in (variant_gids or []) if str(v).startswith("gid://shopify/ProductVariant/")})
    result = {}
    if not store_url or not token or not ids:
        return result

    for i in range(0, len(ids), 80):
        chunk = ids[i:i + 80]
        ids_arg = ",".join(json.dumps(x) for x in chunk)
        q = f"""
        {{
          nodes(ids: [{ids_arg}]) {{
            id
            ... on ProductVariant {{
              title
              sku
              product {{
                title
              }}
            }}
          }}
        }}
        """
        data = gql(store_url, token, q) or {}
        for node in data.get("nodes") or []:
            if not node or not node.get("id"):
                continue
            product = (node.get("product") or {}).get("title") or ""
            variant = node.get("title") or ""
            sku = node.get("sku") or ""
            label = product
            if variant and variant.lower() not in ("default title", "default"):
                label = f"{product} · {variant}" if product else variant
            result[node["id"]] = {"product": label or product or variant or node["id"], "sku": sku or "ø"}
        if i + 80 < len(ids):
            time.sleep(0.35)

    print(f"    smartrr product variant titles resolved: {len(result)} of {len(ids)} ProductVariant IDs")
    return result


def build_smartrr_product_volume_rows(now_str, brand_name, active_states, period_defs, store_url=None, token=None):
    """
    Build Smartrr product rows for dashboard section 06.

    Phase 1 (global totals): active/paused totals by product from current
    purchase-state status (no date filter).

    Phase 2 (period new): new subscribers in selected period using line/state
    created date.

    Returns (rows, active_total_by_norm, paused_total_by_norm) where the last
    two dicts are keyed by _product_match_key(product) for loose matching.
    """
    if brand_name != "cavali" or not active_states:
        return [], {}, {}

    period_ranges = []
    for pk, s, e in period_defs:
        try:
            period_ranges.append((
                pk, str(s), str(e),
                datetime.strptime(str(s), "%Y-%m-%d").date(),
                datetime.strptime(str(e), "%Y-%m-%d").date(),
            ))
        except Exception:
            pass

    variant_title_map = {}
    if store_url and token:
        variant_gids = _find_variant_gids_from_smartrr_states(active_states)
        variant_title_map = fetch_shopify_product_variant_titles(store_url, token, variant_gids)

    if active_states:
        first = active_states[0]
        print(f"    smartrr debug: top-level keys = {list(first.keys())[:35]}")
        for dbk in ("stLineItems", "lineItems", "orderLineItems", "items"):
            v = first.get(dbk)
            if isinstance(v, list) and v:
                item0 = v[0]
                print(f"    smartrr debug: {dbk}[0] FULL = {json.dumps(item0, default=str)[:800]}")
                if isinstance(item0, dict):
                    vnt0 = item0.get("vnt")
                    print(f"    smartrr debug: {dbk}[0].vnt type={type(vnt0).__name__} value={json.dumps(vnt0, default=str)[:400] if vnt0 else 'NULL'}")
        # Also dump the first state's top-level for name fields
        print(f"    smartrr debug: first state FULL (no stLineItems) = {json.dumps({k:v for k,v in first.items() if k not in ('stLineItems','lineItems','orderLineItems','items','orders','discounts')}, default=str)[:600]}")

    def _extract_product_name_from_st_line(st_line):
        # First resolve Shopify ProductVariant GIDs found in Smartrr's vnt object.
        for possible in (
            _dig(st_line, "vnt.shopifyId"),
            _dig(st_line, "vnt.shopifyProductVariantStorefrontId"),
            _dig(st_line, "vnt.id"),
            _dig(st_line, "variant.shopifyId"),
            _dig(st_line, "purchasableVariant.shopifyId"),
            _dig(st_line, "shopifyId"),
        ):
            if possible:
                m = re.search(r"gid://shopify/ProductVariant/\d+", str(possible))
                if m and m.group(0) in variant_title_map:
                    return variant_title_map[m.group(0)].get("product") or ""
        # Direct top-level name fields
        for fk in (
            "purchasableAndPurchasableVariantName",
            "purchasable_and_purchasable_variant_name",
            "productTitle", "product_title",
            "variantTitle", "variant_title",
            "title", "name",
        ):
            v = st_line.get(fk)
            if v and str(v).strip() not in ("", "?", "Default Title"):
                return str(v).strip()
        # vnt first — it is the variant object confirmed in Smartrr's schema
        for nested_key in ("vnt", "purchasable", "purchasableVariant", "variant", "product"):
            nested = st_line.get(nested_key)
            if not isinstance(nested, dict):
                continue
            for fk in (
                "purchasableAndPurchasableVariantName",
                "displayName", "display_name",
                "productVariantName", "product_variant_name",
                "name", "title",
                "productTitle", "product_title",
                "variantTitle", "variant_title",
            ):
                v = nested.get(fk)
                if v and str(v).strip() not in ("", "?", "Default Title"):
                    return str(v).strip()
            # One level deeper (product inside vnt)
            for sub_key in ("product", "purchasable"):
                sub = nested.get(sub_key)
                if isinstance(sub, dict):
                    for fk in ("name", "title", "productTitle", "displayName"):
                        v = sub.get(fk)
                        if v and str(v).strip() not in ("", "?", "Default Title"):
                            return str(v).strip()
        return ""

    def _extract_sku_from_st_line(st_line):
        for possible in (
            _dig(st_line, "vnt.shopifyId"),
            _dig(st_line, "vnt.shopifyProductVariantStorefrontId"),
            _dig(st_line, "vnt.id"),
            _dig(st_line, "variant.shopifyId"),
            _dig(st_line, "purchasableVariant.shopifyId"),
            _dig(st_line, "shopifyId"),
        ):
            if possible:
                m = re.search(r"gid://shopify/ProductVariant/\d+", str(possible))
                if m and m.group(0) in variant_title_map:
                    sku = variant_title_map[m.group(0)].get("sku")
                    if sku:
                        return sku
        # currentSku is confirmed in Smartrr debug output
        for fk in ("currentSku", "sku", "SKU"):
            v = st_line.get(fk)
            if v and str(v).strip():
                return str(v).strip()
        for nested_key in ("vnt", "purchasable", "purchasableVariant", "variant"):
            nested = st_line.get(nested_key)
            if isinstance(nested, dict):
                for fk in ("sku", "SKU", "currentSku"):
                    v = nested.get(fk)
                    if v and str(v).strip():
                        return str(v).strip()
        return "?"

    def _extract_qty(st_line):
        qty_raw = st_line.get("quantity") or st_line.get("qty") or 1
        try:
            return max(1, int(float(str(qty_raw))))
        except Exception:
            return 1

    def _extract_price(st_line, qty):
        for fk in ("priceAfterDiscounts", "price_after_discounts", "basePrice", "base_price",
                   "price", "linePrice", "line_price", "totalPrice", "total_price",
                   "unitPrice", "unit_price", "priceAfterDiscount", "price_after_discount"):
            v = st_line.get(fk)
            if v not in (None, ""):
                return _money_to_usd(v) * qty
        for nested_key in ("purchasable", "purchasableVariant", "variant", "vnt"):
            nested = st_line.get(nested_key)
            if isinstance(nested, dict):
                for fk in ("price", "unitPrice", "unit_price"):
                    v = nested.get(fk)
                    if v not in (None, ""):
                        return _money_to_usd(v) * qty
        return 0.0

    def _get_state_created(sub):
        # For Smartrr "New Subscriptions", use the purchase-state/subscription creation date,
        # not the order line item created date and not Shopify order processed date.
        # This matches Smartrr Advanced Analytics "New Subscriptions" cards.
        for ck in (
            "initialSubmissionDate", "initial_submission_date",
            "externalSubscriptionCreatedDate", "external_subscription_created_date",
            "createdDate", "created_date", "createdAt", "created_at",
        ):
            v = sub.get(ck)
            if v:
                d = _parse_smartrr_date(v)
                if d:
                    return d
        return None

    def _get_products_from_state(sub):
        results = []
        st_lines = sub.get("stLineItems") or sub.get("lineItems") or sub.get("orderLineItems") or []
        # Fallback name from parent state (plan title, subscription title, etc.)
        parent_name = _smartrr_plan_text(sub)

        for st_line in (st_lines if isinstance(st_lines, list) else []):
            if not isinstance(st_line, dict):
                continue
            if st_line.get("deletedAt") or st_line.get("deleted_at"):
                continue
            prod = _extract_product_name_from_st_line(st_line)
            # If no name from line item fields, use parent state name
            if not prod or not re.search(r"[A-Za-z]", prod):
                prod = parent_name
            if not prod or not re.search(r"[A-Za-z]", prod):
                continue
            sku = _extract_sku_from_st_line(st_line)
            qty = _extract_qty(st_line)
            price = _extract_price(st_line, qty)
            results.append((prod, sku, qty, price))

        if not results:
            for line in _candidate_line_dicts(sub):
                if _line_deleted(line):
                    continue
                prod = _line_product(line)
                if not prod or prod in ("Unknown Product", "Other") or not re.search(r"[A-Za-z]", prod):
                    prod = parent_name
                if not prod or not re.search(r"[A-Za-z]", prod):
                    continue
                sku = _line_sku(line)
                qty = _line_quantity(line)
                price = _line_revenue(line, qty)
                results.append((prod, sku, qty, price))

        # Final fallback: use parent plan name directly with qty=1
        if not results and parent_name and re.search(r"[A-Za-z]", parent_name):
            results.append((parent_name, "?", 1, 0.0))

        return results

    active_total = {}
    paused_total = {}
    cancelled_total = {}
    no_product_count = 0

    for sub in active_states:
        state_group = _smartrr_status_group(sub)
        if state_group not in ("active", "paused", "inactive"):
            continue

        products = _get_products_from_state(sub)
        if not products:
            no_product_count += 1
            prod = "Other"
            products = [(prod, "ø", 1, 0.0)]

        for prod, sku, qty, _ in products:
            key = (prod, sku)
            if state_group == "active":
                active_total[key] = active_total.get(key, 0) + qty
            elif state_group == "paused":
                paused_total[key] = paused_total.get(key, 0) + qty
            else:
                cancelled_total[key] = cancelled_total.get(key, 0) + qty

    total_active_global = sum(active_total.values())
    total_paused_global = sum(paused_total.values())
    total_cancelled_global = sum(cancelled_total.values())
    print(f"    FASE 1 — Totales globales: active={total_active_global} paused={total_paused_global} cancelled={total_cancelled_global} sin_producto={no_product_count}")

    new_items = []
    skipped_no_date = 0
    seen_sub_products = set()

    # IMPORTANT: New Subscribers must match Smartrr's "New Subscriptions" metric.
    # Therefore we count PURCHASE STATES created in the selected range.
    # We include ACTIVE, PAUSED and CANCELLED purchase states, because a subscription
    # can be created and cancelled in the same period and still counts as New in Smartrr.
    # We do NOT use Shopify order sales quantity and do NOT use stLineItems.createdDate
    # as the primary date, because those represent line/order activity, not subscriber creation.
    for sub in active_states:
        state_group = _smartrr_status_group(sub)
        state_created = _get_state_created(sub)
        if not state_created:
            skipped_no_date += 1
            continue

        products = _get_products_from_state(sub)
        if not products:
            products = [("Other", "ø", 1, 0.0)]

        sid = str(
            _dig(sub, "id") or _dig(sub, "purchaseStateId") or _dig(sub, "shopifyId") or
            _dig(sub, "subscriptionId") or _dig(sub, "subscription_id") or
            json.dumps({k: sub.get(k) for k in ("createdDate", "shopifyId", "externalSubscriptionId") if isinstance(sub, dict)}, sort_keys=True)
        )[:120]

        for prod, sku, qty, gross in products:
            # Subscriber count is one subscription, not line quantity. Keep product label for product breakdown.
            if not prod or not re.search(r"[A-Za-z]", str(prod)):
                prod = "Other"
            dedupe = f"{sid}|{_product_match_key(prod)}|{sku}"
            if dedupe in seen_sub_products:
                continue
            seen_sub_products.add(dedupe)
            new_items.append({
                "created": state_created,
                "product": prod,
                "sku": sku or "ø",
                "qty": 1,
                "gross": 0.0,
                "id": sid,
                "status": state_group,
            })

    print(f"    FASE 2 — Purchase states nuevos con fecha: {len(new_items)} items ? sin_fecha={skipped_no_date}")

    rows = []
    all_keys = set(active_total.keys()) | set(paused_total.keys()) | set(cancelled_total.keys())

    for pk, ps, pe, ds, de in period_ranges:
        new_buckets = {}
        for item in new_items:
            if ds <= item["created"] <= de:
                key = (item["product"], item["sku"])
                rec = new_buckets.setdefault(key, {"new": 0, "gross": 0.0, "ids": []})
                rec["new"] += item["qty"]
                rec["gross"] += item["gross"]
                if len(rec["ids"]) < 5:
                    rec["ids"].append(item["id"])

        period_keys = all_keys | set(new_buckets.keys())
        for prod, sku in sorted(
            period_keys,
            key=lambda k: (-(active_total.get(k, 0) + paused_total.get(k, 0) + new_buckets.get(k, {}).get("new", 0)), k[0].lower()),
        ):
            active_to_date = active_total.get((prod, sku), 0)
            paused_to_date = paused_total.get((prod, sku), 0)
            cancelled_to_date = cancelled_total.get((prod, sku), 0)
            new_count = new_buckets.get((prod, sku), {}).get("new", 0)
            gross = new_buckets.get((prod, sku), {}).get("gross", 0.0)
            ids = new_buckets.get((prod, sku), {}).get("ids") or []

            if active_to_date <= 0 and paused_to_date <= 0 and cancelled_to_date <= 0 and new_count <= 0:
                continue

            rows.append([
                now_str, brand_name, pk, ps, pe,
                prod, sku,
                0, new_count,
                active_to_date, paused_to_date, cancelled_to_date,
                0,
                "Smartrr subscriber snapshot + current ACTIVE/PAUSED/CANCELLED totals",
                "Smartrr purchase-state/subscription created date for new subscribers",
                "purchaseStateStatus=ACTIVE/PAUSED/CANCELLED;row_type=smartrr_subscribers",
                "; ".join(ids),
            ])

    print(f"    smartrr_product_volume FINAL: {len(new_items)} items de fecha — {len(rows)} filas — skipped_sin_fecha={skipped_no_date}")

    # Build normalized-key dicts so merge can match even when product names differ slightly
    active_norm = {}
    paused_norm = {}
    cancelled_norm = {}
    for (prod, sku), qty in active_total.items():
        nk = _product_match_key(prod)
        active_norm[nk] = active_norm.get(nk, 0) + qty
    for (prod, sku), qty in paused_total.items():
        nk = _product_match_key(prod)
        paused_norm[nk] = paused_norm.get(nk, 0) + qty
    for (prod, sku), qty in cancelled_total.items():
        nk = _product_match_key(prod)
        cancelled_norm[nk] = cancelled_norm.get(nk, 0) + qty

    return rows, active_norm, paused_norm, cancelled_norm


# SMARTRR fallback — period product rows from Shopify order line_items
# ─────────────────────────────────────────────────────────────────

def _shopify_line_product(li):
    """Keep the product/variant name exactly as Shopify/Smartrr displays it."""
    for k in ("title", "name", "product_title", "variant_title"):
        v = li.get(k) if isinstance(li, dict) else None
        if v not in (None, ""):
            txt = str(v).strip()
            if txt:
                return txt
    return "Other"


def _shopify_line_sku(li):
    for k in ("sku", "variant_id", "product_id"):
        v = li.get(k) if isinstance(li, dict) else None
        if v not in (None, ""):
            txt = str(v).strip()
            if txt:
                return txt
    return "ø"


def _shopify_line_revenue(li):
    qty = int(_to_number((li or {}).get("quantity"), 0) or 0)
    for k in ("price", "pre_tax_price", "discounted_price"):
        v = (li or {}).get(k)
        if v not in (None, ""):
            try:
                return round(float(v) * max(qty, 1), 2)
            except Exception:
                pass
    return 0.0


def _is_subscription_line_item(li, product_label=""):
    """True only for subscription-created line items, not normal sales volume."""
    if not isinstance(li, dict):
        return False
    # Shopify REST usually exposes this when the order line was created from a selling plan.
    if li.get("selling_plan_allocation"):
        return True
    if li.get("selling_plan_id") or li.get("selling_plan_name"):
        return True
    # Some apps store the subscription/contract identifiers in properties.
    props = li.get("properties") or []
    if isinstance(props, list):
        for p in props:
            if not isinstance(p, dict):
                continue
            k = str(p.get("name") or p.get("key") or "").lower()
            v = str(p.get("value") or "").lower()
            if "subscription" in k or "selling_plan" in k or "contract" in k or "subscription" in v:
                return True
    txt = f"{product_label} {li.get('title','')} {li.get('name','')} {li.get('variant_title','')}".lower()
    # Name fallback only when the product explicitly says subscription/membership.
    return ("subscription" in txt) or ("membership" in txt)

def build_smartrr_product_volume_from_orders(now_str, brand_name, period, start, end, orders):
    """
    Cavali order-volume rows from Shopify order line_items.

    HARD SEPARATION:
      - row_type=subscription_sales_volume: Shopify line items that clearly came from a selling plan/subscription.
      - row_type=sales_volume_only: one-time Shopify line items only.
      - new_subscribers is ALWAYS 0 here. Subscriber counts come only from Smartrr purchase-state rows.
      - active/paused/cancelled are NEVER stored here. They are injected/displayed from Smartrr subscriber rows.
    """
    if brand_name != "cavali":
        return []

    buckets = {}
    for o in orders or []:
        for li in (o.get("line_items") or []):
            qty = int(_to_number(li.get("quantity"), 0) or 0)
            if qty <= 0:
                continue
            product = _shopify_line_product(li)
            sku = _shopify_line_sku(li)
            is_sub = bool(_is_subscription_line_item(li, product))
            key = (product, sku, is_sub)
            rec = buckets.setdefault(key, {"qty": 0, "gross": 0.0, "ids": []})
            rec["qty"] += qty
            rec["gross"] += _shopify_line_revenue(li)
            lid = str(li.get("id") or li.get("admin_graphql_api_id") or "")[:80]
            if lid and len(rec["ids"]) < 5:
                rec["ids"].append(lid)

    rows = []
    for (product, sku, is_sub), v in sorted(buckets.items(), key=lambda kv: (-kv[1]["qty"], kv[0][0].lower(), kv[0][2])):
        qty = int(v["qty"])
        rows.append([
            now_str, brand_name, period, str(start), str(end),
            product, sku,
            qty, 0,
            0, 0, 0,
            round(v["gross"], 2),
            "Shopify order line_items — Subscription Sales Volume only" if is_sub else "Shopify order line_items — One-Time Sales Volume only",
            "Shopify order processed date",
            "row_type=subscription_sales_volume" if is_sub else "row_type=sales_volume_only",
            "; ".join(v["ids"]),
        ])

    if rows:
        print(f"    smartrr_product_volume order-volume rows for {period}: {len(rows)}")
        for r in rows[:8]:
            print(f"      volume row: {r[5]} · sku={r[6]} · sales_qty={r[7]} · new_subscribers={r[8]} · active={r[9]} · gross={r[12]} · {r[15]}")
    return rows


def _product_match_key(product):
    """Loose match key so Smartrr and Shopify product labels merge reliably.

    Examples:
      - "The Premier Box Subscription" -> "premierbox"
      - "THE SIGNATURE BOX / Quarterly" -> "signaturebox"
      - "Cavali Club Membership - Annual" -> "cavaliclubmembership"
    """
    raw = str(product or "").lower()
    if "signature" in raw:
        return "signaturebox"
    if "premier" in raw:
        return "premierbox"
    if "junior" in raw and "membership" in raw:
        return "cavaliclubjuniormembership"
    if "cavali club" in raw and "membership" in raw:
        return "cavaliclubmembership"
    if "welcome" in raw and "box" in raw:
        return "welcomebox"
    if "sona" in raw and "bundle" in raw:
        return "sonabundle"
    if "cheese" in raw and "kniv" in raw:
        return "cavaliclubcheeseknives"
    if "kriste" in raw or "training course" in raw:
        return "kristekehoetrainingcourse"
    if "fly spray" in raw:
        return "stopbuggnflyspray16oz"
    if "spicy pony" in raw or "candle" in raw:
        return "stablestylespicyponycandle"
    p = raw
    for word in ("subscription", "default title", "quarterly", "monthly", "annual", "yearly"):
        p = p.replace(word, "")
    p = re.sub(r"[^a-z0-9]+", "", p)
    return p


def _is_blank_number(v):
    return v in (None, "", "None", "null", "ø")


def merge_smartrr_product_volume_rows(order_rows, active_rows, active_norm=None, paused_norm=None, cancelled_norm=None):
    """
    Prefer period rows from Shopify orders for Sales Volume. New Subscribers come only
    from Smartrr purchase-state rows. Enrich volume rows with Smartrr ACTIVE,
    PAUSED and CANCELLED totals-to-date when Smartrr exposes usable product line data.

    active_norm / paused_norm: global totals keyed by _product_match_key(product).
    These are injected into ALL rows (including fallback) so the dashboard always
    shows real current totals per product even when the Shopify fallback is used.

    Key detail: match by normalized product name first, not only exact product+SKU,
    because Smartrr and Shopify can label the same item differently.
    """
    order_rows = order_rows or []
    active_rows = active_rows or []

    # Column indexes for SMARTRR_PRODUCT_VOLUME_HEADERS
    IDX_PERIOD = 2
    IDX_PRODUCT = 5
    IDX_SKU = 6
    IDX_TOTAL_QTY = 7
    IDX_NEW = 8
    IDX_ACTIVE = 9    # active_subscribers_current
    IDX_PAUSED = 10   # paused_subscribers_current
    IDX_CANCELLED = 11  # cancelled_subscribers_current  ← NEW
    IDX_GROSS = 12
    IDX_SOURCE = 13
    IDX_DATE_BASIS = 14
    IDX_FILTER = 15
    IDX_SAMPLE = 16

    active_exact = {}
    active_product = {}
    for r in active_rows:
        try:
            exact_key = (str(r[IDX_PERIOD]), str(r[IDX_PRODUCT]), str(r[IDX_SKU]))
            active_exact[exact_key] = r
            loose_key = (str(r[IDX_PERIOD]), _product_match_key(r[IDX_PRODUCT]))
            prev = active_product.get(loose_key)
            r_total = _to_number(r[IDX_ACTIVE], 0) + _to_number(r[IDX_PAUSED], 0)
            p_total = (_to_number(prev[IDX_ACTIVE], 0) + _to_number(prev[IDX_PAUSED], 0)) if prev else -1
            if not prev or r_total > p_total:
                active_product[loose_key] = r
        except Exception:
            pass

    merged = []
    # IMPORTANT v55:
    # Do NOT mark Smartrr subscriber rows as "used" when they are used only to
    # enrich Shopify volume rows with current active/paused totals. A volume row
    # and a subscriber row are two different concepts and must both be written.
    used_active = set()

    for r in order_rows:
        try:
            key = (str(r[IDX_PERIOD]), str(r[IDX_PRODUCT]), str(r[IDX_SKU]))
            loose_key = (str(r[IDX_PERIOD]), _product_match_key(r[IDX_PRODUCT]))
            a = active_exact.get(key) or active_product.get(loose_key)
            row = list(r)
            while len(row) < len(SMARTRR_PRODUCT_VOLUME_HEADERS):
                row.append("")

            if a:
                # Enrich order-volume row with current totals for context, but
                # keep the separate Smartrr subscriber row in the output so
                # New Subscribers remains visible and does not get swallowed by volume.
                # used_active is intentionally NOT updated here.
                if len(a) > IDX_ACTIVE and not _is_blank_number(a[IDX_ACTIVE]):
                    row[IDX_ACTIVE] = a[IDX_ACTIVE]
                if len(a) > IDX_PAUSED and not _is_blank_number(a[IDX_PAUSED]):
                    row[IDX_PAUSED] = a[IDX_PAUSED]
                if len(a) > IDX_CANCELLED and not _is_blank_number(a[IDX_CANCELLED]):
                    row[IDX_CANCELLED] = a[IDX_CANCELLED]
                # Preserve Shopify row_type/source/date_basis. These rows are sales-volume rows,
                # not subscriber rows. Only enrich their current totals.
                if len(a) > IDX_SAMPLE and a[IDX_SAMPLE] not in (None, "") and not row[IDX_SAMPLE]:
                    row[IDX_SAMPLE] = a[IDX_SAMPLE]

            if len(row) > IDX_ACTIVE and _is_blank_number(row[IDX_ACTIVE]):
                row[IDX_ACTIVE] = 0
            if len(row) > IDX_PAUSED and _is_blank_number(row[IDX_PAUSED]):
                row[IDX_PAUSED] = 0
            if len(row) > IDX_CANCELLED and _is_blank_number(row[IDX_CANCELLED]):
                row[IDX_CANCELLED] = 0
            merged.append(row)
        except Exception:
            merged.append(r)

    for r in active_rows:
        try:
            active_key = (str(r[IDX_PERIOD]), str(r[IDX_PRODUCT]), str(r[IDX_SKU]))
            if active_key not in used_active:
                row = list(r)
                while len(row) < len(SMARTRR_PRODUCT_VOLUME_HEADERS):
                    row.append("")
                if _is_blank_number(row[IDX_NEW]):
                    row[IDX_NEW] = 0
                if _is_blank_number(row[IDX_TOTAL_QTY]):
                    row[IDX_TOTAL_QTY] = row[IDX_NEW]
                if _is_blank_number(row[IDX_ACTIVE]):
                    row[IDX_ACTIVE] = 0
                if _is_blank_number(row[IDX_PAUSED]):
                    row[IDX_PAUSED] = 0
                if _is_blank_number(row[IDX_CANCELLED]):
                    row[IDX_CANCELLED] = 0
                merged.append(row)
        except Exception:
            merged.append(r)

    # Phase 3: inject global Smartrr totals into ALL rows that still have
    # blank or fallback-derived active/paused counts.
    if active_norm or paused_norm:
        for row in merged:
            try:
                nk = _product_match_key(str(row[IDX_PRODUCT]))
                src = str(row[IDX_SOURCE] if len(row) > IDX_SOURCE else "").lower()
                is_fallback = "fallback" in src and "active/paused" not in src
                a_real = active_norm.get(nk) if active_norm else None
                p_real = (paused_norm or {}).get(nk, 0)
                c_real = (cancelled_norm or {}).get(nk, 0)

                # Fuzzy prefix match: handles cases where Smartrr adds "Monthly",
                # "Annual", etc. to a name that Shopify shows shorter (or vice versa).
                if a_real is None and active_norm and len(nk) >= 8:
                    for ak, av in active_norm.items():
                        if len(ak) >= 8 and (ak.startswith(nk) or nk.startswith(ak)):
                            a_real = av
                            p_real = (paused_norm or {}).get(ak, 0)
                            c_real = (cancelled_norm or {}).get(ak, 0)
                            break

                if a_real is not None and (_is_blank_number(row[IDX_ACTIVE]) or is_fallback):
                    row[IDX_ACTIVE] = a_real
                    row[IDX_PAUSED] = p_real
                    if len(row) > IDX_CANCELLED:
                        row[IDX_CANCELLED] = c_real
                    # Keep original row_type/source so sales-volume rows never become subscriber rows.
            except Exception:
                pass
        print(f"    smartrr_product_volume phase3: global totals injected into rows using active_norm({len(active_norm)} products)")

    if active_rows:
        print(f"    smartrr_product_volume merge: {len(order_rows)} order rows + {len(active_rows)} active/paused rows => {len(merged)} rows")
    else:
        print(f"    smartrr_product_volume merge: active/paused rows unavailable; using {len(order_rows)} order fallback rows")
    return merged


def write_smartrr_product_volume(gc, sheet_id, rows, periods_to_replace):
    """Upsert Smartrr product-volume rows without leaving stale rows for refreshed periods."""
    sh = gc.open_by_key(sheet_id)
    try:
        ws = sh.worksheet("smartrr_product_volume")
    except Exception:
        ws = sh.add_worksheet("smartrr_product_volume", rows=1000, cols=len(SMARTRR_PRODUCT_VOLUME_HEADERS))

    vals = ws.get_all_values()
    keep = []
    replace = {str(p).strip() for p in periods_to_replace if p}
    if len(vals) >= 2:
        h = vals[0]
        for r in vals[1:]:
            m = _row_to_map(h, r)
            if str(m.get("period", "")).strip() not in replace:
                keep.append(_map_to_row(SMARTRR_PRODUCT_VOLUME_HEADERS, m))

    cleaned_rows = []
    for r in rows or []:
        row = list(r)
        while len(row) < len(SMARTRR_PRODUCT_VOLUME_HEADERS):
            row.append("")
        # Minimum safeguard: product card totals cannot be empty.
        if _is_blank_number(row[9]):
            row[9] = row[8] if not _is_blank_number(row[8]) else row[7]
        if len(row) > 10 and _is_blank_number(row[10]):
            row[10] = 0
        cleaned_rows.append(row[:len(SMARTRR_PRODUCT_VOLUME_HEADERS)])

    merged = keep + cleaned_rows
    merged = sorted(
        merged,
        key=lambda r: (
            str(r[1]),
            _safe_date(r[3]),
            str(r[2]),
            -(_to_number(r[9], 0) + _to_number(r[10], 0)),
            str(r[5]).lower(),
        )
    )

    ws.clear()
    ws.append_row(SMARTRR_PRODUCT_VOLUME_HEADERS)
    if merged:
        ws.append_rows(merged, value_input_option="USER_ENTERED")
    print(f"    smartrr_product_volume: {len(cleaned_rows)} refreshed rows; {len(merged)} total rows")


# HELPERS SHEETS
# ─────────────────────────────────────────────────────────────────
def _safe_date(v):
    try:    return datetime.strptime(str(v), "%Y-%m-%d").date()
    except: return date(1900, 1, 1)


def _parse_shopify_dt(v):
    """Parse Shopify datetime strings safely for new vs returning logic."""
    if not v:
        return None
    s = str(v).strip()
    if not s:
        return None
    try:
        dt = datetime.fromisoformat(s.replace("Z", "+00:00"))
        if dt.tzinfo is not None:
            dt = dt.astimezone(TIMEZONE)
        return dt
    except Exception:
        pass
    for fmt in ("%Y-%m-%dT%H:%M:%S%z", "%Y-%m-%dT%H:%M:%S", "%Y-%m-%d"):
        try:
            dt = datetime.strptime(s.replace("Z", "+0000"), fmt)
            if dt.tzinfo is not None:
                dt = dt.astimezone(TIMEZONE)
            return dt
        except Exception:
            continue
    return None


def _row_to_map(headers, row):
    return {h: (row[i] if i < len(row) else "") for i, h in enumerate(headers)}


def _map_to_row(headers, m):
    return [m.get(h, "") for h in headers]

# ─────────────────────────────────────────────────────────────────
# WRITE — upsert (no borra datos históricos)
# ─────────────────────────────────────────────────────────────────
def write_all(gc, sheet_id, kpi_rows, rs_rows, nvr_rows, brand_name):
    sh = gc.open_by_key(sheet_id)

    # ── kpis_daily ──────────────────────────────────────────────
    try:    ws = sh.worksheet("kpis_daily")
    except: ws = sh.add_worksheet("kpis_daily", rows=600, cols=40)

    existing_vals = ws.get_all_values()
    existing = {}
    if len(existing_vals) >= 2:
        ex_h = existing_vals[0]
        for r in existing_vals[1:]:
            m  = _row_to_map(ex_h, r)
            pk = str(m.get("period", "")).strip()
            if pk:
                existing[pk] = _map_to_row(HEADERS, m)
    for r in kpi_rows:
        existing[str(r[1]).strip()] = r

    merged = sorted(existing.values(), key=lambda r: (_safe_date(r[2]), str(r[1])))
    ws.clear()
    ws.append_row(HEADERS)
    if merged:
        ws.append_rows(merged, value_input_option="USER_ENTERED")
    print(f"    kpis_daily: {len(merged)} rows")

    # ── revenue_share ────────────────────────────────────────────
    try:    ws_rs = sh.worksheet("revenue_share")
    except: ws_rs = sh.add_worksheet("revenue_share", rows=600, cols=12)

    rs_headers = [
        "updated_at", "period", "channel",
        "amount", "pct",
        "gross_profit", "gross_margin",
        "pct_prev", "pct_chg",
        "gp_is_estimate",
    ]
    rs_vals = ws_rs.get_all_values()
    existing_rs = {}
    if len(rs_vals) >= 2:
        ex_h = rs_vals[0]
        for r in rs_vals[1:]:
            m  = _row_to_map(ex_h, r)
            p  = str(m.get("period",  "")).strip()
            ch = str(m.get("channel", "")).strip()
            if p and ch:
                existing_rs[(p, ch)] = _map_to_row(rs_headers, m)
    for r in rs_rows:
        existing_rs[(str(r[1]).strip(), str(r[2]).strip())] = r

    sorted_rs = sorted(existing_rs.values(), key=lambda r: (str(r[2]), str(r[1])))
    rs_idx    = {(str(r[2]).strip(), str(r[1]).strip()): r for r in sorted_rs}

    for r in sorted_rs:
        ch = str(r[2]).strip()
        pk = str(r[1]).strip()
        prev_pk = None
        if pk.startswith("mtd_"):
            yr, mo  = map(int, pk[4:].split("-"))
            pmo     = mo - 1 if mo > 1 else 12
            py      = yr if mo > 1 else yr - 1
            prev_pk = f"mtd_{py}-{str(pmo).zfill(2)}"
        elif pk.startswith("week_"):
            try:
                d_      = datetime.strptime(pk[5:], "%Y-%m-%d").date()
                prev_pk = f"week_{d_ - timedelta(days=7)}"
            except Exception:
                pass
        elif len(pk) == 7 and "-" in pk:
            yr, mo  = int(pk[:4]), int(pk[5:])
            pmo     = mo - 1 if mo > 1 else 12
            py      = yr if mo > 1 else yr - 1
            prev_pk = f"{py}-{str(pmo).zfill(2)}"
        elif pk.startswith("q") and "_" in pk:
            parts   = pk[1:].split("_")
            q, yr   = int(parts[0]), int(parts[1])
            pq      = q - 1 if q > 1 else 4
            py      = yr if q > 1 else yr - 1
            prev_pk = f"q{pq}_{py}"

        prev_row = rs_idx.get((ch, prev_pk)) if prev_pk else None
        pct_now  = float(r[4]) if r[4] not in ("", "None") else None
        pct_prev = float(prev_row[4]) if prev_row and prev_row[4] not in ("", "None") else None
        pct_chg  = round(pct_now - pct_prev, 2) \
                   if pct_now is not None and pct_prev is not None else None
        while len(r) < len(rs_headers):
            r.append("")
        r[7] = pct_prev if pct_prev is not None else ""
        r[8] = pct_chg  if pct_chg  is not None else ""

    merged_rs = sorted(existing_rs.values(), key=lambda r: (str(r[1]), str(r[2])))
    ws_rs.clear()
    ws_rs.append_row(rs_headers)
    if merged_rs:
        ws_rs.append_rows(merged_rs, value_input_option="USER_ENTERED")
    print(f"    revenue_share: {len(merged_rs)} rows")

    # ── new_vs_returning ─────────────────────────────────────────
    try:    ws_nvr = sh.worksheet("new_vs_returning")
    except: ws_nvr = sh.add_worksheet("new_vs_returning", rows=300, cols=12)

    nvr_headers = [
        "updated_at", "period", "period_start", "period_end",
        "new_customers", "returning_customers",
        "new_revenue", "returning_revenue",
        "new_gross_profit", "returning_gross_profit",
    ]
    nvr_vals = ws_nvr.get_all_values()
    existing_nvr = {}
    if len(nvr_vals) >= 2:
        ex_h = nvr_vals[0]
        for r in nvr_vals[1:]:
            m  = _row_to_map(ex_h, r)
            pk = str(m.get("period", "")).strip()
            if pk:
                existing_nvr[pk] = _map_to_row(nvr_headers, m)
    for r in nvr_rows:
        existing_nvr[str(r[1]).strip()] = r

    merged_nvr = sorted(existing_nvr.values(), key=lambda r: (_safe_date(r[2]), str(r[1])))
    ws_nvr.clear()
    ws_nvr.append_row(nvr_headers)
    if merged_nvr:
        ws_nvr.append_rows(merged_nvr, value_input_option="USER_ENTERED")
    print(f"    new_vs_returning: {len(merged_nvr)} rows")

    # ── ad_spend ─────────────────────────────────────────────────
    try:    ws_ad = sh.worksheet("ad_spend")
    except: ws_ad = sh.add_worksheet("ad_spend", rows=200, cols=10)

    ad_headers = [
        "updated_at", "brand", "period", "period_start", "period_end",
        "ad_spend", "roas", "cos", "cac_auto",
    ]
    now_str    = datetime.now(TIMEZONE).strftime("%Y-%m-%d %H:%M")
    brand_data = AD_SPEND_DATA.get(brand_name, {})

    nc_by_month = {}
    for r in merged:
        pk = str(r[1]).strip()
        if len(pk) == 7 and "-" in pk and not pk.startswith("mtd_"):
            try:
                nc_by_month[pk] = int(float(r[HEADERS.index("new_customers")] or 0))
            except Exception:
                pass

    ad_rows = []
    for mo, vals in sorted(brand_data.items()):
        if not vals.get("spend"):
            continue
        yr, mn = int(mo[:4]), int(mo[5:])
        ps     = f"{mo}-01"
        pe     = f"{mo}-{calendar.monthrange(yr, mn)[1]:02d}"
        nc     = nc_by_month.get(mo, 0)
        spend  = vals.get("spend", 0)
        cac_auto = round(spend / nc, 2) if nc > 0 else ""
        ad_rows.append([
            now_str, brand_name, mo, ps, pe,
            spend, vals.get("roas", 0), vals.get("cos", 0), cac_auto,
        ])

    ws_ad.clear()
    ws_ad.append_row(ad_headers)
    if ad_rows:
        ws_ad.append_rows(ad_rows, value_input_option="USER_ENTERED")
    print(f"    ad_spend: {len(ad_rows)} months")

# ─────────────────────────────────────────────────────────────────
# MAIN
# ─────────────────────────────────────────────────────────────────
def main():
    gc      = get_gc()
    P       = get_periods()
    now_str = datetime.now(TIMEZONE).strftime("%Y-%m-%d %H:%M")

    for brand_name, cfg in STORES.items():
        print(f"\n{'='*60}\n  {brand_name.upper()}\n{'='*60}")
        url, token = cfg["url"], cfg["token"]
        kpi_rows, rs_rows, nvr_rows = [], [], []
        smartrr_order_rows = []

        periods_to_run = [
            {"label": "MTD",          "cur": "mtd",          "is_snapshot": False},
            # These MTD snapshots are required by the dashboard so Previous Period and YOY compare MTD vs MTD,
            # never current partial month vs a full month.
            {"label": "MTD_PREV",     "cur": "mtd_prev",     "is_snapshot": True},
            {"label": "MTD_YOY",      "cur": "mtd_yoy",      "is_snapshot": True},
            {"label": "WEEK",         "cur": "week",         "is_snapshot": False},
            {"label": "MONTH",        "cur": "month",        "is_snapshot": False},
            {"label": "QUARTER",      "cur": "quarter",      "is_snapshot": False},
            {"label": "WEEK_PREV",    "cur": "week_prev",    "is_snapshot": True},
            {"label": "MONTH_PREV",   "cur": "month_prev",   "is_snapshot": True},
            {"label": "QUARTER_PREV", "cur": "quarter_prev", "is_snapshot": True},
        ]

        for it in periods_to_run:
            label    = it["label"]
            cur_k    = it["cur"]
            s, e, pk = P[cur_k]
            if pk is None:
                continue

            print(f"\n  [{label}] {s} → {e}  (period='{pk}')")

            sal  = fetch_sales(url, token, s, e)
            sess = fetch_sessions(url, token, s, e)
            of   = fetch_orders_fulfilled(url, token, s, e)
            ords = fetch_orders(url, token, s, e)
            if brand_name == "cavali":
                smartrr_order_rows.extend(build_smartrr_product_volume_from_orders(now_str, brand_name, pk, s, e, ords))
            nvr  = fetch_new_vs_returning(url, token, s, e)

            cur = build(sal, ords, nvr, sess, of)
            kpi_rows.append(make_kpi_row(now_str, pk, s, e, cur))

            gm_pct = sal.get("pct_gm", 0)
            rs     = calc_rs(ords, gm_pct)
            for ch, v in rs.items():
                rs_rows.append([
                    now_str, pk, ch,
                    v["amount"], v["pct"],
                    v["gross_profit"], v["gross_margin"],
                    "", "",
                    str(v["gp_is_estimate"]),
                ])

            nvr_rows.append([
                now_str, pk, str(s), str(e),
                nvr.get("new_customers",          0),
                nvr.get("returning_customers",    0),
                nvr.get("new_revenue",            0),
                nvr.get("returning_revenue",      0),
                cur.get("new_gross_profit",       0),
                cur.get("returning_gross_profit", 0),
            ])

        write_all(gc, cfg["sheet_id"], kpi_rows, rs_rows, nvr_rows, brand_name)

        if brand_name == "cavali":
            # Smartrr Section 06: period-exact product volume using Order Line Item Created Date.
            # This matches the Smartrr drilldown where April uses line-item Created Date within Apr 1–Apr 30.
            active_states = fetch_smartrr_active_purchase_states(brand_name)
            period_defs = [(r[1], r[2], r[3]) for r in kpi_rows if r and r[1] and r[2] and r[3]]
            active_rows, active_norm, paused_norm, cancelled_norm = build_smartrr_product_volume_rows(now_str, brand_name, active_states, period_defs, url, token)
            smartrr_rows = merge_smartrr_product_volume_rows(smartrr_order_rows, active_rows, active_norm, paused_norm, cancelled_norm)
            write_smartrr_product_volume(gc, cfg["sheet_id"], smartrr_rows, [p[0] for p in period_defs])

        print(f"\n  ✓ {brand_name.upper()} — {len(kpi_rows)} periods written")
        for row in kpi_rows:
            print(f"    {row[1]:<24}  {row[2]} → {row[3]}"
                  f"  gross:{float(row[4] or 0):>12,.2f}"
                  f"  net:{float(row[5] or 0):>12,.2f}"
                  f"  gp:{float(row[6] or 0):>10,.2f}"
                  f"  new_cust:{int(row[20] or 0):>5}")


if __name__ == "__main__":
    main()
