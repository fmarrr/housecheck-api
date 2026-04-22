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


def is_full_postcode(q: str) -> bool:
    """Returns True for a full postcode like SW6 2LE (not just a sector SW6 2)."""
    pc = q.strip().upper().replace("  ", " ")
    return bool(re.match(r'^[A-Z]{1,2}\d{1,2}[A-Z]?\s\d[A-Z]{2}$', pc))


def _build_postcode_sql(project, dataset, sector, full_postcode, property_type):
    """Return (sql, params) for a postcode-based search."""
    pt_filter = "AND p.property_type = @property_type" if property_type else ""

    if property_type:
        # Query mart_property_type_yearly
        street_filter = (
            "AND p.street IN (SELECT DISTINCT street FROM `{p}.{d}.mart_transactions` WHERE postcode = @postcode)"
            if full_postcode else ""
        ).format(p=project, d=dataset)
        sql = f"""
        SELECT
            p.sale_year,
            p.street,
            p.postcode_sector,
            p.town_city,
            p.district,
            p.transaction_count,
            p.median_sale_price_gbp_filled  AS median_price,
            p.avg_sale_price_gbp_filled     AS avg_price,
            p.yoy_avg_price_change_pct      AS yoy_pct,
            p.rolling_3yr_avg_price_gbp     AS rolling_3yr_avg,
            CAST(NULL AS FLOAT64)           AS newbuild_premium_pct,
            p.new_build_count,
            p.established_count
        FROM `{project}.{dataset}.mart_property_type_yearly` p
        WHERE p.postcode_sector = @sector
          AND p.has_postcode = true
          {pt_filter}
          {street_filter}
        ORDER BY p.street, p.sale_year
        """
        params = [bigquery.ScalarQueryParameter("sector", "STRING", sector)]
        if property_type:
            params.append(bigquery.ScalarQueryParameter("property_type", "STRING", property_type))
        if full_postcode:
            params.append(bigquery.ScalarQueryParameter("postcode", "STRING", full_postcode))
    else:
        # Query mart_price_trends (all property types)
        street_filter = (
            "AND t.street IN (SELECT DISTINCT street FROM `{p}.{d}.mart_transactions` WHERE postcode = @postcode)"
            if full_postcode else ""
        ).format(p=project, d=dataset)
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
            t.rolling_3yr_avg_price_gbp     AS rolling_3yr_avg,
            n.avg_premium_pct               AS newbuild_premium_pct,
            CAST(NULL AS INT64)             AS new_build_count,
            CAST(NULL AS INT64)             AS established_count
        FROM `{project}.{dataset}.mart_price_trends` t
        LEFT JOIN `{project}.{dataset}.mart_newbuild_premium` n
            ON  t.postcode_sector = n.postcode_sector
            AND t.sale_year       = n.sale_year
            AND t.street          = n.street
        WHERE t.postcode_sector = @sector
          AND t.has_postcode = true
          {street_filter}
        ORDER BY t.street, t.sale_year
        """
        params = [bigquery.ScalarQueryParameter("sector", "STRING", sector)]
        if full_postcode:
            params.append(bigquery.ScalarQueryParameter("postcode", "STRING", full_postcode))

    return sql, params


def _build_street_sql(project, dataset, property_type):
    """Return (sql, param_name) for a street name search."""
    pt_filter = "AND p.property_type = @property_type" if property_type else ""

    if property_type:
        sql = f"""
        SELECT
            p.sale_year,
            p.street,
            p.postcode_sector,
            p.town_city,
            p.district,
            p.transaction_count,
            p.median_sale_price_gbp_filled  AS median_price,
            p.avg_sale_price_gbp_filled     AS avg_price,
            p.yoy_avg_price_change_pct      AS yoy_pct,
            p.rolling_3yr_avg_price_gbp     AS rolling_3yr_avg,
            CAST(NULL AS FLOAT64)           AS newbuild_premium_pct,
            p.new_build_count,
            p.established_count
        FROM `{project}.{dataset}.mart_property_type_yearly` p
        WHERE UPPER(p.street) LIKE @street
          {pt_filter}
        ORDER BY p.street, p.sale_year
        LIMIT 500
        """
    else:
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
            t.rolling_3yr_avg_price_gbp     AS rolling_3yr_avg,
            n.avg_premium_pct               AS newbuild_premium_pct,
            CAST(NULL AS INT64)             AS new_build_count,
            CAST(NULL AS INT64)             AS established_count
        FROM `{project}.{dataset}.mart_price_trends` t
        LEFT JOIN `{project}.{dataset}.mart_newbuild_premium` n
            ON  t.postcode_sector = n.postcode_sector
            AND t.sale_year       = n.sale_year
            AND t.street          = n.street
        WHERE UPPER(t.street) LIKE @street
        ORDER BY t.street, t.sale_year
        LIMIT 500
        """
    return sql


VALID_PROPERTY_TYPES = {"Detached", "Semi-Detached", "Terraced", "Flat/Maisonette", "Other"}


@app.get("/search")
def search(
    q: str = Query(..., min_length=2),
    property_type: Optional[str] = Query(None),
):
    """
    Search by full postcode (SW6 2LE), postcode sector (SW6 2), or street name.
    Optional property_type filter: Detached | Semi-Detached | Terraced | Flat/Maisonette | Other
    """
    if property_type and property_type not in VALID_PROPERTY_TYPES:
        raise HTTPException(status_code=400, detail=f"Invalid property_type. Must be one of: {', '.join(sorted(VALID_PROPERTY_TYPES))}")

    client = get_client()
    sector = postcode_to_sector(q)
    postcode_normalised = q.strip().upper().replace("  ", " ") if is_full_postcode(q) else None

    rows = []
    search_mode = None

    # --- attempt 1: postcode search ---
    if sector:
        search_mode = "postcode"
        sql, params = _build_postcode_sql(PROJECT_ID, DATASET, sector, postcode_normalised, property_type)
        job_config = bigquery.QueryJobConfig(query_parameters=params)
        rows = list(client.query(sql, job_config=job_config).result())

    # --- attempt 2: street name fallback ---
    if not rows:
        search_mode = "street"
        sql = _build_street_sql(PROJECT_ID, DATASET, property_type)
        params = [bigquery.ScalarQueryParameter("street", "STRING", f"%{q.upper().strip()}%")]
        if property_type:
            params.append(bigquery.ScalarQueryParameter("property_type", "STRING", property_type))
        job_config = bigquery.QueryJobConfig(query_parameters=params)
        rows = list(client.query(sql, job_config=job_config).result())

    if not rows:
        return {"search_mode": search_mode, "query": q, "sector": sector, "property_type": property_type, "streets": []}

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
            "rolling_3yr_avg": r["rolling_3yr_avg"],
            "newbuild_premium_pct": round(float(r["newbuild_premium_pct"]), 1) if r["newbuild_premium_pct"] is not None else None,
            "new_build_count": r["new_build_count"],
            "established_count": r["established_count"],
        })

    return {
        "search_mode": search_mode,
        "query": q,
        "sector": sector,
        "property_type": property_type,
        "streets": list(streets.values())
    }


@app.get("/health")
def health():
    return {"status": "ok"}
