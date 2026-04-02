from fastapi import FastAPI, Query, HTTPException
from fastapi.middleware.cors import CORSMiddleware
from google.cloud import bigquery
from google.oauth2 import service_account
import os, json, re
from typing import Optional

app = FastAPI(title="HouseCheck API")

app.add_middleware(
    CORSMiddleware,
    allow_origins=["*"],
    allow_methods=["*"],
    allow_headers=["*"],
)

PROJECT_ID = os.getenv("GCP_PROJECT_ID", "uk-house-prices-491810")
DATASET   = os.getenv("BQ_DATASET", "uk_house_prices")

def get_client():
    creds_json = os.getenv("GOOGLE_CREDENTIALS")
    if creds_json:
        info = json.loads(creds_json)
        creds = service_account.Credentials.from_service_account_info(info)
        return bigquery.Client(project=PROJECT_ID, credentials=creds)
    # local dev: uses application default credentials (gcloud auth)
    return bigquery.Client(project=PROJECT_ID)


def postcode_to_sector(postcode: str) -> Optional[str]:
    """SW6 2LE → SW6 2"""
    pc = postcode.strip().upper().replace("  ", " ")
    # already a sector e.g. SW6 2
    if re.match(r'^[A-Z]{1,2}\d{1,2}[A-Z]?\s\d$', pc):
        return pc
    # full postcode e.g. SW6 2LE → take everything except last 2 chars of inward
    m = re.match(r'^([A-Z]{1,2}\d{1,2}[A-Z]?)\s?(\d)', pc)
    if m:
        return f"{m.group(1)} {m.group(2)}"
    return None


@app.get("/search")
def search(q: str = Query(..., min_length=2)):
    """
    Search by full postcode (SW6 2LE), postcode sector (SW6 2), or street name.
    Returns street-level YoY price trends + newbuild premium.
    Falls back from postcode → street if no results found.
    """
    client = get_client()
    sector = postcode_to_sector(q)

    rows = []
    search_mode = None

    # --- attempt 1: postcode sector search ---
    if sector:
        search_mode = "postcode"
        sql = f"""
        SELECT
            t.sale_year,
            t.street,
            t.postcode_sector,
            t.town_city,
            t.district,
            t.transaction_count,
            t.median_sale_price_gbp_filled  AS median_price,
            t.avg_sale_price_gbp_filled     AS avg_price,
            t.yoy_avg_price_change_pct      AS yoy_pct,
            t.prev_year_avg_price_gbp       AS prev_year_avg,
            t.rolling_3yr_avg_price_gbp     AS rolling_3yr_avg,
            n.avg_premium_pct               AS newbuild_premium_pct,
            n.avg_premium_gbp               AS newbuild_premium_gbp
        FROM `{PROJECT_ID}.{DATASET}.mart_price_trends` t
        LEFT JOIN `{PROJECT_ID}.{DATASET}.mart_newbuild_premium` n
            ON  t.postcode_sector = n.postcode_sector
            AND t.sale_year       = n.sale_year
            AND t.street          = n.street
        WHERE t.postcode_sector = @sector
          AND t.has_postcode = true
        ORDER BY t.street, t.sale_year
        """
        job_config = bigquery.QueryJobConfig(
            query_parameters=[bigquery.ScalarQueryParameter("sector", "STRING", sector)]
        )
        rows = list(client.query(sql, job_config=job_config).result())

    # --- attempt 2: street name fallback ---
    if not rows:
        search_mode = "street"
        street_q = f"%{q.upper().strip()}%"
        sql = f"""
        SELECT
            t.sale_year,
            t.street,
            t.postcode_sector,
            t.town_city,
            t.district,
            t.transaction_count,
            t.median_sale_price_gbp_filled  AS median_price,
            t.avg_sale_price_gbp_filled     AS avg_price,
            t.yoy_avg_price_change_pct      AS yoy_pct,
            t.prev_year_avg_price_gbp       AS prev_year_avg,
            t.rolling_3yr_avg_price_gbp     AS rolling_3yr_avg,
            n.avg_premium_pct               AS newbuild_premium_pct,
            n.avg_premium_gbp               AS newbuild_premium_gbp
        FROM `{PROJECT_ID}.{DATASET}.mart_price_trends` t
        LEFT JOIN `{PROJECT_ID}.{DATASET}.mart_newbuild_premium` n
            ON  t.postcode_sector = n.postcode_sector
            AND t.sale_year       = n.sale_year
            AND t.street          = n.street
        WHERE UPPER(t.street) LIKE @street
        ORDER BY t.street, t.sale_year
        LIMIT 500
        """
        job_config = bigquery.QueryJobConfig(
            query_parameters=[bigquery.ScalarQueryParameter("street", "STRING", street_q)]
        )
        rows = list(client.query(sql, job_config=job_config).result())

    if not rows:
        return {"search_mode": search_mode, "query": q, "sector": sector, "streets": []}

    # --- group by street ---
    streets: dict = {}
    for r in rows:
        key = f"{r['street']}|{r['postcode_sector']}"
        if key not in streets:
            streets[key] = {
                "street": r["street"],
                "postcode_sector": r["postcode_sector"],
                "town_city": r["town_city"],
                "district": r["district"],
                "years": []
            }
        streets[key]["years"].append({
            "year": r["sale_year"],
            "transaction_count": r["transaction_count"],
            "median_price": r["median_price"],
            "avg_price": r["avg_price"],
            "yoy_pct": round(float(r["yoy_pct"]), 1) if r["yoy_pct"] is not None else None,
            "prev_year_avg": r["prev_year_avg"],
            "rolling_3yr_avg": r["rolling_3yr_avg"],
            "newbuild_premium_pct": round(float(r["newbuild_premium_pct"]), 1) if r["newbuild_premium_pct"] is not None else None,
            "newbuild_premium_gbp": r["newbuild_premium_gbp"],
        })

    return {
        "search_mode": search_mode,
        "query": q,
        "sector": sector,
        "streets": list(streets.values())
    }


@app.get("/health")
def health():
    return {"status": "ok"}
