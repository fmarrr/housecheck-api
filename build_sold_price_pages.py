#!/usr/bin/env python3
"""
Generate static, pre-rendered "Sold house prices in <district>" landing pages.

Why static: Google must see the price data in the raw HTML to index/rank it.
This fetches per-street data from the live API at BUILD time and bakes the
numbers into static HTML. The per-street charts are the SAME Chart.js charts as
the homepage search, drawn from data baked into the page (no live API call) —
so they match the site's main feature while staying SEO-safe.

Usage:    python3 build_sold_price_pages.py
          PREVIEW=1 python3 build_sold_price_pages.py   # strips analytics/ads
Output:   sold-prices/<slug>.html  (served at /sold-prices/<slug> via cleanUrls)

------------------------------------------------------------------------------
FEATURED STREETS — "on the rise despite a recent dip"
------------------------------------------------------------------------------
Per-street medians are thin/noisy, so featured streets are chosen with a
defensive, all-editable filter:
  * CAP_LO/CAP_HI - drop year-medians outside this range (kills source errors
                    like a £315,000,000 record).
  * MIN_SALES     - require this many sales in each 3-year window.
  * MAX_WIN_RATIO - reject a window whose max/min year-median ratio exceeds this
                    (kills single-year outliers).
A street is FEATURED if its smoothed 3-year trend (RECENT vs PRIOR window) is
up AND its latest year pulled back (the "sustained climb, recent dip" pattern).
"""

import json
import os
import urllib.parse
import urllib.request
from collections import defaultdict

API_BASE = "https://housecheck-api-580723587126.europe-west2.run.app"
SITE = "https://soldbystreet.co.uk"
GA_ID = "G-9WWR5FF5VK"
OUT_DIR = os.path.join(os.path.dirname(os.path.abspath(__file__)), "sold-prices")
PREVIEW = os.getenv("PREVIEW") == "1"

CAP_LO, CAP_HI = 150_000, 15_000_000
MIN_SALES = 12
MAX_WIN_RATIO = 1.8
RECENT = (2023, 2025)
PRIOR = (2020, 2022)
N_FEATURED = 3
N_TABLE = 18

DISTRICTS = [
    {
        "district": "SW6",
        "area": "Fulham",
        "blurb": (
            "SW6 covers Fulham in the London Borough of Hammersmith and Fulham, "
            "from Parsons Green and Fulham Broadway down to Sands End and the "
            "river. It is a predominantly Victorian and Edwardian terraced area, "
            "with prime pockets such as the Peterborough Estate and Munster "
            "Village."
        ),
        "exclude": ["Michael Road"],  # known-bad source data
        "nearby": [
            ("Battersea (SW11)", "/"),
            ("Chelsea & West Brompton (SW10)", "/"),
            ("Hammersmith (W6)", "/"),
            ("West Kensington (W14)", "/"),
            ("Wandsworth & Earlsfield (SW18)", "/"),
        ],
    },
]


# --- data fetching ----------------------------------------------------------
def fetch_sector(query):
    url = f"{API_BASE}/search?q={urllib.parse.quote(query)}"
    with urllib.request.urlopen(url, timeout=60) as r:
        return json.loads(r.read().decode())


def fetch_district(district):
    streets = []
    for n in range(0, 10):
        s = fetch_sector(f"{district} {n}").get("streets", [])
        if s:
            print(f"  {district} {n}: {len(s)} streets")
            streets.extend(s)
    return streets


# --- region aggregation -----------------------------------------------------
def aggregate(streets):
    years = sorted({y["year"] for st in streets for y in st["years"]})
    year_stats = {}
    for y in years:
        total = wsum = 0.0
        for st in streets:
            for yr in st["years"]:
                if (yr["year"] == y and yr["transaction_count"] and yr["avg_price"]
                        and CAP_LO <= yr["avg_price"] <= CAP_HI):
                    total += yr["transaction_count"]
                    wsum += yr["avg_price"] * yr["transaction_count"]
        year_stats[y] = {"sales": int(total), "mean": (wsum / total) if total else None}
    for i, y in enumerate(years):
        pv = year_stats[years[i - 1]]["mean"] if i else None
        cur = year_stats[y]["mean"]
        year_stats[y]["yoy"] = round((cur - pv) / pv * 100, 1) if (cur and pv) else None

    latest = years[-1]

    med = sorted(
        max(st["years"], key=lambda y: y["year"])["median_price"]
        for st in streets
        if st["years"] and max(st["years"], key=lambda y: y["year"])["median_price"]
        and CAP_LO <= max(st["years"], key=lambda y: y["year"])["median_price"] <= CAP_HI)
    meta = {
        "latest_year": latest, "headline_year": latest,
        "partial_year": None, "first_year": years[0],
        "total_streets": len(streets),
        "town_city": titlecase(streets[0]["town_city"]) if streets else "",
        "borough": titlecase(streets[0]["district"]) if streets else "",
        "typical_median": med[len(med) // 2] if med else None,
    }
    return year_stats, meta


# --- street trend selection -------------------------------------------------
def capped_year_medians(year_lists):
    d = defaultdict(lambda: [0.0, 0])
    for y in year_lists:
        m, c = y["median_price"], y["transaction_count"]
        if c and m and CAP_LO <= m <= CAP_HI:
            d[y["year"]][0] += m * c
            d[y["year"]][1] += c
    return {yr: (wsum / c, c) for yr, (wsum, c) in d.items()}


def window(cy, lo, hi):
    vals = [m for yr, (m, c) in cy.items() if lo <= yr <= hi]
    tot = sum(c for yr, (m, c) in cy.items() if lo <= yr <= hi)
    if not vals or not tot:
        return None
    return {"mean": sum(m * c for yr, (m, c) in cy.items() if lo <= yr <= hi) / tot,
            "n": tot, "ratio": max(vals) / min(vals)}


def clean_years(st):
    """Year objects for the chart, with outliers nulled (matches homepage shape)."""
    out = []
    for y in sorted(st["years"], key=lambda y: y["year"]):
        m = y["median_price"]
        out.append({
            "year": y["year"],
            "median_price": m if (m and CAP_LO <= m <= CAP_HI) else None,
            "rolling_3yr_avg": (y["rolling_3yr_avg"]
                                if (y["rolling_3yr_avg"]
                                    and CAP_LO <= y["rolling_3yr_avg"] <= CAP_HI) else None),
            "median_price_per_sqm": y.get("median_price_per_sqm"),
            "median_price_per_sqm_est": y.get("median_price_per_sqm_est"),
            "transaction_count": y["transaction_count"],
            "yoy_pct": y.get("yoy_pct"),
        })
    return out


def trending_streets(streets, exclude):
    exclude_u = {s.upper() for s in exclude}
    merged = defaultdict(list)        # street -> raw year lists (across sectors)
    sectors = {}                      # street -> dominant sector
    sector_n = defaultdict(lambda: defaultdict(int))
    for st in streets:
        nm = st["street"].upper()
        if nm in exclude_u:
            continue
        merged[nm].extend(st["years"])
        for y in st["years"]:
            sector_n[nm][st["postcode_sector"]] += y["transaction_count"] or 0
    raw_by_name = defaultdict(list)
    for st in streets:
        raw_by_name[st["street"].upper()].append(st)

    picks = []
    for nm, ys in merged.items():
        cy = capped_year_medians(ys)
        rec, pri = window(cy, *RECENT), window(cy, *PRIOR)
        if not (rec and pri) or rec["n"] < MIN_SALES or pri["n"] < MIN_SALES:
            continue
        if rec["ratio"] > MAX_WIN_RATIO or pri["ratio"] > MAX_WIN_RATIO:
            continue
        trend = (rec["mean"] - pri["mean"]) / pri["mean"] * 100
        if trend <= 0:
            continue
        real = sorted([y for y in ys if y["transaction_count"]], key=lambda y: y["year"])
        latest_yoy = real[-1].get("yoy_pct") if real else None
        sector = max(sector_n[nm], key=sector_n[nm].get)
        # chart payload: merge same-named streets' years into one cleaned series
        combo = {"street": titlecase(nm), "years": clean_years({"years": ys})}
        picks.append({
            "street": titlecase(nm), "sector": sector, "trend": trend,
            "recent_yoy": latest_yoy, "sales": int(rec["n"] + pri["n"]),
            "dip": (latest_yoy is not None and latest_yoy < 0), "chart": combo,
        })
    # featured: recent-dip pattern first, by trend; fill with other up-streets
    picks.sort(key=lambda p: (-p["trend"], -p["sales"]))
    dip = [p for p in picks if p["dip"]]
    featured = (dip + [p for p in picks if not p["dip"]])[:N_FEATURED]
    return featured, picks[:N_TABLE]   # (3 to chart) + (full by-street table)


# --- formatting -------------------------------------------------------------
def titlecase(s):
    return " ".join(w.capitalize() for w in s.split()) if s else s


def gbp(n):
    return f"£{int(round(n)):,}" if n else "—"


def pct(p, signed=True):
    if p is None:
        return "—"
    a = "▲" if p > 0 else ("▼" if p < 0 else "•")
    return f"{a} {abs(p):.0f}%"


def pct_class(p):
    return "" if p is None else ("up" if p > 0 else ("down" if p < 0 else ""))


# --- CSS / chart JS ---------------------------------------------------------
CSS = """
  :root{--ink:#0f0e0d;--paper:#f5f2ee;--cream:#ede9e3;--accent:#c8401a;
    --muted:#7a7570;--border:#d4cfc9;--up:#2d5a3d;--down:#9b2c2c;--psm:#5a7a9e;}
  *,*::before,*::after{box-sizing:border-box;margin:0;padding:0;}
  body{font-family:'DM Sans',sans-serif;background:var(--paper);color:var(--ink);min-height:100vh;}
  a{color:inherit;}
  header{border-bottom:1.5px solid var(--ink);padding:0 2.5rem;display:flex;align-items:center;
    justify-content:space-between;height:60px;background:var(--paper);position:sticky;top:0;z-index:100;}
  .logo{font-family:'DM Serif Display',serif;font-size:1.4rem;letter-spacing:-0.02em;
    text-decoration:none;color:var(--ink);}
  .logo span{color:var(--accent);font-style:italic;}
  .header-nav{display:flex;align-items:center;gap:1.5rem;}
  .nav-link{font-family:'DM Mono',monospace;font-size:0.65rem;text-transform:uppercase;
    letter-spacing:0.08em;color:var(--ink);text-decoration:none;border:1px solid var(--border);
    padding:0.28rem 0.7rem;transition:background .15s,border-color .15s;}
  .nav-link:hover{background:var(--cream);border-color:var(--ink);}
  .wrap{max-width:880px;margin:0 auto;padding:3.5rem 2.5rem 5rem;}
  .breadcrumb{font-family:'DM Mono',monospace;font-size:0.62rem;text-transform:uppercase;
    letter-spacing:0.1em;color:var(--muted);margin-bottom:2rem;}
  .breadcrumb a{text-decoration:none;}.breadcrumb a:hover{color:var(--ink);}
  .breadcrumb span{margin:0 0.5rem;}
  .tag{font-family:'DM Mono',monospace;font-size:0.62rem;text-transform:uppercase;
    letter-spacing:0.12em;color:var(--accent);margin-bottom:0.75rem;}
  h1{font-family:'DM Serif Display',serif;font-size:clamp(1.8rem,4vw,2.8rem);line-height:1.1;
    letter-spacing:-0.02em;margin-bottom:1.25rem;}
  h1 em{font-style:italic;color:var(--accent);}
  .intro{font-size:1.05rem;line-height:1.7;color:var(--muted);font-weight:300;
    margin-bottom:2.5rem;padding-bottom:2.5rem;border-bottom:1.5px solid var(--ink);}
  h2{font-family:'DM Serif Display',serif;font-size:1.4rem;letter-spacing:-0.01em;margin:3rem 0 1rem;}
  p{font-size:0.95rem;line-height:1.75;color:#2a2826;margin-bottom:1.1rem;font-weight:300;}
  .stat-row{display:grid;grid-template-columns:repeat(auto-fit,minmax(160px,1fr));gap:1px;
    background:var(--border);border:1.5px solid var(--ink);margin:0 0 1rem;}
  .stat{background:white;padding:1.25rem 1.25rem 1.1rem;}
  .stat .label{font-family:'DM Mono',monospace;font-size:0.6rem;text-transform:uppercase;
    letter-spacing:0.1em;color:var(--muted);margin-bottom:0.5rem;}
  .stat .value{font-family:'DM Serif Display',serif;font-size:1.7rem;line-height:1;}
  .stat .sub{font-family:'DM Mono',monospace;font-size:0.7rem;margin-top:0.4rem;}
  .up{color:var(--up);}.down{color:var(--down);}
  .note{font-family:'DM Mono',monospace;font-size:0.66rem;color:var(--muted);
    line-height:1.6;margin-bottom:2rem;}
  .street-block{border:1.5px solid var(--ink);background:white;margin:1.5rem 0;}
  .sb-head{display:grid;grid-template-columns:1fr auto;align-items:start;gap:1rem;
    padding:1.1rem 1.3rem;border-bottom:1.5px solid var(--ink);}
  .sb-name{font-family:'DM Serif Display',serif;font-size:1.3rem;}
  .sb-loc{font-family:'DM Mono',monospace;font-size:0.62rem;text-transform:uppercase;
    letter-spacing:0.08em;color:var(--muted);margin-top:0.2rem;}
  .sb-stat{text-align:right;}
  .sb-stat .growth{font-family:'DM Mono',monospace;font-size:1.05rem;font-weight:500;}
  .sb-stat .recent{display:block;font-family:'DM Mono',monospace;font-size:0.6rem;
    text-transform:uppercase;letter-spacing:0.06em;color:var(--muted);margin-top:0.25rem;}
  .chart-wrap{height:280px;padding:1rem 1.1rem 0.3rem;}
  .legend{display:flex;flex-wrap:wrap;gap:1.1rem;padding:0.4rem 1.3rem 0.9rem;
    font-family:'DM Mono',monospace;font-size:0.62rem;color:var(--muted);}
  .legend i{display:inline-block;width:16px;height:0;border-top-width:2px;
    border-top-style:solid;vertical-align:middle;margin-right:5px;}
  .k-med{border-top-color:var(--ink);}
  .k-roll{border-top-color:var(--accent);border-top-style:dashed;}
  .k-psm{border-top-color:var(--psm);border-top-style:dashed;}
  .sb-link{display:block;border-top:1.5px solid var(--border);padding:0.8rem 1.3rem;
    font-family:'DM Mono',monospace;font-size:0.66rem;text-transform:uppercase;
    letter-spacing:0.07em;text-decoration:none;color:var(--accent);}
  .sb-link:hover{background:var(--cream);}
  table{width:100%;border-collapse:collapse;margin:1rem 0 2rem;font-size:0.85rem;}
  th,td{text-align:left;padding:0.6rem 0.7rem;border-bottom:1px solid var(--border);}
  th{font-family:'DM Mono',monospace;font-size:0.6rem;text-transform:uppercase;
    letter-spacing:0.08em;color:var(--muted);border-bottom:1.5px solid var(--ink);}
  td.num,th.num{text-align:right;font-variant-numeric:tabular-nums;}
  tbody tr:hover{background:var(--cream);}
  .chips{display:flex;flex-wrap:wrap;gap:0.5rem;margin:0.5rem 0 2rem;}
  .chip{font-family:'DM Mono',monospace;font-size:0.7rem;text-decoration:none;
    border:1px solid var(--border);padding:0.4rem 0.8rem;transition:background .15s,border-color .15s;}
  .chip:hover{background:var(--cream);border-color:var(--ink);}
  .cta{border:1.5px solid var(--ink);background:white;padding:2rem;margin:3rem 0 0;text-align:center;}
  .cta .label{font-family:'DM Mono',monospace;font-size:0.62rem;text-transform:uppercase;
    letter-spacing:0.12em;color:var(--accent);margin-bottom:0.6rem;}
  .cta h3{font-family:'DM Serif Display',serif;font-size:1.5rem;margin-bottom:0.5rem;}
  .cta a{display:inline-block;margin-top:1rem;font-family:'DM Mono',monospace;font-size:0.72rem;
    text-transform:uppercase;letter-spacing:0.08em;text-decoration:none;background:var(--ink);
    color:var(--paper);padding:0.7rem 1.4rem;}
  footer{border-top:1.5px solid var(--ink);padding:2rem 2.5rem;display:flex;gap:1.5rem;
    font-family:'DM Mono',monospace;font-size:0.65rem;text-transform:uppercase;letter-spacing:0.08em;}
  footer a{text-decoration:none;color:var(--muted);}footer a:hover{color:var(--ink);}
  @media(max-width:600px){header,.wrap{padding-left:1.2rem;padding-right:1.2rem;}
    th,td{padding:0.5rem 0.4rem;}.chart-wrap{height:240px;}}
"""

# Chart drawing replicated from the homepage (index.html). __DATA__ is replaced
# with the baked per-street series at build time.
CHART_JS = """
const STREETS_DATA = __DATA__;
const fmt = n => n == null ? '—' : '£' + Math.round(n).toLocaleString('en-GB');
STREETS_DATA.forEach((street, idx) => {
  const ctx = document.getElementById('chart-' + idx);
  if (!ctx) return;
  const years = street.years;
  const psmData = years.map(y => y.median_price_per_sqm ?? y.median_price_per_sqm_est ?? null);
  const psmIsEst = years.map(y => y.median_price_per_sqm == null && y.median_price_per_sqm_est != null);
  new Chart(ctx, {
    type: 'line',
    data: {
      labels: years.map(y => y.year),
      datasets: [
        { label: 'Median price', data: years.map(y => y.median_price),
          borderColor: '#0f0e0d', borderWidth: 2, pointBackgroundColor: '#0f0e0d',
          pointRadius: 3, fill: true, spanGaps: true,
          backgroundColor: c => { const g = c.chart.ctx.createLinearGradient(0,0,0,200);
            g.addColorStop(0,'rgba(15,14,13,0.07)'); g.addColorStop(1,'rgba(15,14,13,0)'); return g; },
          tension: 0.35 },
        { label: '3yr rolling avg', data: years.map(y => y.rolling_3yr_avg),
          borderColor: '#c8401a', borderWidth: 1.5, borderDash: [4,3], pointRadius: 0,
          fill: false, tension: 0.35, spanGaps: true },
        { label: 'Median £/m²', data: psmData,
          borderColor: '#5a7a9e', borderWidth: 1.5, borderDash: [2,2],
          pointRadius: psmData.map(v => v != null ? 3 : 0),
          pointBackgroundColor: '#5a7a9e', fill: false, tension: 0.35,
          yAxisID: 'y2', spanGaps: true }
      ]
    },
    options: {
      responsive: true, maintainAspectRatio: false,
      interaction: { mode: 'index', intersect: false },
      plugins: {
        legend: { display: false },
        tooltip: { backgroundColor: '#0f0e0d', titleColor: '#f5f2ee', bodyColor: '#ede9e3',
          padding: 10, titleFont: { family: 'DM Mono', size: 10 },
          bodyFont: { family: 'DM Sans', size: 13 },
          callbacks: { label: c => {
            if (c.dataset.yAxisID === 'y2') return '  ' + c.dataset.label + ': £' + Math.round(c.raw).toLocaleString() + '/m²' + (psmIsEst[c.dataIndex] ? ' (est.)' : '');
            return '  ' + c.dataset.label + ': ' + fmt(c.raw); } } }
      },
      scales: {
        x: { grid: { display: false }, border: { color: '#d4cfc9' },
          ticks: { font: { family: 'DM Mono', size: 10 }, color: '#7a7570' } },
        y: { grid: { color: '#ede9e3' }, border: { display: false },
          ticks: { font: { family: 'DM Mono', size: 10 }, color: '#7a7570',
            callback: v => '£' + (v >= 1000000 ? (v/1000000).toFixed(1)+'m' : Math.round(v/1000)+'k') } },
        y2: { position: 'right', display: psmData.some(v => v != null), grid: { display: false },
          border: { display: false },
          ticks: { font: { family: 'DM Mono', size: 10 }, color: '#5a7a9e',
            callback: v => '£' + Math.round(v/1000) + 'k/m²' } }
      }
    }
  });
});
"""


def render(d, year_stats, meta, featured, table_streets):
    district, area = d["district"], d["area"]
    slug = district.lower().replace(" ", "-")
    ly, fy = meta["headline_year"], meta["first_year"]
    ls = year_stats[ly]
    title = f"Sold house prices in {district} ({area}) — {ly} | SoldByStreet"
    desc = (f"What houses sold for in {district} ({area}, {meta['town_city']}) — "
            f"{ls['sales']} sales in {ly}, average {gbp(ls['mean'])}. See the {area} "
            f"streets with the strongest multi-year price growth, with charts from "
            f"HM Land Registry data.")
    canonical = f"{SITE}/sold-prices/{slug}"

    stats = f"""
    <div class="stat-row">
      <div class="stat"><div class="label">Homes sold ({ly})</div>
        <div class="value">{ls['sales']:,}</div></div>
      <div class="stat"><div class="label">Average sold price ({ly})</div>
        <div class="value">{gbp(ls['mean'])}</div>
        <div class="sub {pct_class(ls['yoy'])}">{pct(ls['yoy'])} vs {ly-1}</div></div>
      <div class="stat"><div class="label">Typical street median</div>
        <div class="value">{gbp(meta['typical_median'])}</div></div>
      <div class="stat"><div class="label">Streets with sales</div>
        <div class="value">{meta['total_streets']:,}</div></div>
    </div>
    <p class="note">Average = mean of all recorded sale prices (pulled up by
      high-value sales). "Typical street median" is the middle street's median.
      Source: HM Land Registry Price Paid Data.</p>"""

    # featured streets with real Chart.js charts
    blocks = ""
    for i, p in enumerate(featured):
        blocks += f"""
    <div class="street-block">
      <div class="sb-head">
        <div><div class="sb-name">{p['street']}</div>
          <div class="sb-loc">{p['sector']} · {meta['town_city']}</div></div>
        <div class="sb-stat">
          <span class="growth up">{pct(p['trend'])} <span style="color:var(--muted)">3yr</span></span>
          <span class="recent">recent year {pct(p['recent_yoy'])}</span>
        </div>
      </div>
      <div class="chart-wrap"><canvas id="chart-{i}"></canvas></div>
      <div class="legend"><span><i class="k-med"></i>Median price</span>
        <span><i class="k-roll"></i>3yr rolling avg</span>
        <span><i class="k-psm"></i>£/m²</span></div>
      <a class="sb-link" href="/">Search {p['street']} on SoldByStreet →</a>
    </div>"""
    feat_names = ", ".join(p["street"] for p in featured)
    featured_html = f"""
    <h2>Three {area} streets in focus</h2>
    <p>A closer look at {feat_names} — each chart shows the median price, its
      3-year rolling average and £/m², the same view you get searching on the
      homepage.</p>
    {blocks}
    <p class="note">Charts are drawn from data baked into this page; year-medians
      are capped to remove source errors.</p>"""

    # streets trending up — BY STREET table
    tbl_rows = "".join(
        f"<tr><td>{p['street']}</td><td>{p['sector']}</td>"
        f"<td class='num up'>{pct(p['trend'])}</td>"
        f"<td class='num {pct_class(p['recent_yoy'])}'>{pct(p['recent_yoy'])}</td>"
        f"<td class='num'>{p['sales']}</td></tr>"
        for p in table_streets)
    by_street_html = f"""
    <h2>{area} streets trending up</h2>
    <p>{district} streets with the strongest <strong>multi-year</strong> price
      growth — comparing {RECENT[0]}–{RECENT[1]} with {PRIOR[0]}–{PRIOR[1]} —
      even where the latest year pulled back. Only streets with ≥{MIN_SALES} sales
      in each window and no extreme single-year swing are shown.</p>
    <table><thead><tr><th>Street</th><th>Sector</th>
      <th class="num">3yr trend</th><th class="num">Recent year</th>
      <th class="num">Sales</th></tr></thead><tbody>{tbl_rows}</tbody></table>"""

    # year trend
    pyr = meta["partial_year"]
    trend_rows = "".join(
        f"<tr><td>{y}{' *' if y == pyr else ''}</td>"
        f"<td class='num'>{year_stats[y]['sales']:,}</td>"
        f"<td class='num'>{gbp(year_stats[y]['mean'])}</td>"
        f"<td class='num {pct_class(year_stats[y]['yoy'])}'>{pct(year_stats[y]['yoy'])}</td></tr>"
        for y in sorted(year_stats))
    partial_note = (f'<p class="note">* {pyr} is still incomplete — HM Land '
                    f'Registry registers completions with a lag, so {pyr} figures '
                    f'will rise as more are recorded.</p>' if pyr else "")
    trend = f"""
    <h2>{district} price trend by year</h2>
    <table><thead><tr><th>Year</th><th class="num">Sales</th>
      <th class="num">Average price</th><th class="num">YoY</th></tr></thead>
      <tbody>{trend_rows}</tbody></table>
    {partial_note}"""

    chips = "".join(f'<a class="chip" href="{u}">{l} →</a>' for l, u in d["nearby"])

    jsonld = json.dumps({
        "@context": "https://schema.org", "@type": "Dataset",
        "name": f"Sold house prices in {district} ({area})", "description": desc,
        "url": canonical, "temporalCoverage": f"{fy}/{ly}",
        "spatialCoverage": {"@type": "Place", "name": f"{district}, {area}, {meta['town_city']}"},
        "creator": {"@type": "Organization", "name": "HM Land Registry"},
        "publisher": {"@type": "Organization", "name": "Houses SoldByStreet", "url": SITE + "/"},
    }, indent=2)

    chart_data = json.dumps([f["chart"] for f in featured])
    chart_block = (f'<script src="https://cdn.jsdelivr.net/npm/chart.js@4.4.0/dist/chart.umd.min.js"></script>\n'
                   f'<script>{CHART_JS.replace("__DATA__", chart_data)}</script>')

    third_party = "" if PREVIEW else f"""<script id="cookieyes" type="text/javascript" src="https://cdn-cookieyes.com/client_data/7b30e2f2d9316ee4614729dbf30f3c60/script.js"></script>
<script async src="https://www.googletagmanager.com/gtag/js?id={GA_ID}"></script>
<script>
  window.dataLayer = window.dataLayer || [];
  function gtag(){{dataLayer.push(arguments);}}
  gtag('js', new Date());
  gtag('config', '{GA_ID}');
</script>
<script defer src="/_vercel/insights/script.js"></script>"""
    ads = "" if PREVIEW else '<script async src="https://pagead2.googlesyndication.com/pagead/js/adsbygoogle.js?client=ca-pub-6325652833259788" crossorigin="anonymous"></script>'

    return f"""<!DOCTYPE html>
<html lang="en">
<head>
<meta charset="UTF-8">
{third_party}
<meta name="viewport" content="width=device-width, initial-scale=1.0">
<title>{title}</title>
<meta name="description" content="{desc}">
<meta name="robots" content="index, follow">
<link rel="canonical" href="{canonical}">
<link rel="icon" type="image/png" sizes="32x32" href="/favicon.png">
<link rel="icon" href="/favicon.svg" type="image/svg+xml">
{ads}
<meta property="og:type" content="website">
<meta property="og:url" content="{canonical}">
<meta property="og:site_name" content="Houses SoldByStreet">
<meta property="og:title" content="Sold house prices in {district} ({area}) — {ly}">
<meta property="og:description" content="{desc}">
<script type="application/ld+json">
{jsonld}
</script>
<link rel="preconnect" href="https://fonts.googleapis.com">
<link rel="preconnect" href="https://fonts.gstatic.com" crossorigin>
<link href="https://fonts.googleapis.com/css2?family=DM+Serif+Display:ital@0;1&family=DM+Mono:wght@400;500&family=DM+Sans:wght@300;400;500&display=swap" rel="stylesheet">
<style>{CSS}</style>
</head>
<body>
<header>
  <a href="/" class="logo">SoldBy<span>Street</span></a>
  <nav class="header-nav">
    <a href="/" class="nav-link">Search</a>
    <a href="/guides" class="nav-link">Guides</a>
  </nav>
</header>
<div class="wrap">
  <div class="breadcrumb">
    <a href="/">Home</a><span>·</span>Sold prices<span>·</span>{district}
  </div>
  <div class="tag">{meta['borough']}</div>
  <h1>Sold house prices in <em>{district}</em> ({area})</h1>
  <p class="intro">{d['blurb']} Below is what homes actually sold for in {district},
    using completed-sale records from HM Land Registry ({fy}–{ly}).</p>
  {stats}
  {by_street_html}
  {trend}
  {featured_html}
  <h2>Nearby areas</h2>
  <div class="chips">{chips}</div>
  <div class="cta">
    <div class="label">Check any postcode</div>
    <h3>See sold prices on your street</h3>
    <p>Search by postcode for street-level medians, year-on-year trends and
      transaction history from HM Land Registry.</p>
    <a href="/">Search a postcode</a>
  </div>
</div>
<footer>
  <a href="/guides">Guides</a>
  <a href="/privacy">Privacy</a>
  <a href="/terms">Terms</a>
</footer>
{chart_block}
</body>
</html>"""


def main():
    os.makedirs(OUT_DIR, exist_ok=True)
    for d in DISTRICTS:
        print(f"Building {d['district']} ({d['area']})…")
        streets = fetch_district(d["district"])
        if not streets:
            print(f"  ! no data for {d['district']}, skipping")
            continue
        year_stats, meta = aggregate(streets)
        featured, table_streets = trending_streets(streets, d.get("exclude", []))
        print("  featured: " + ", ".join(
            f"{p['street']} (+{p['trend']:.0f}%, recent {p['recent_yoy']:.0f}%)" for p in featured))
        print(f"  by-street table rows: {len(table_streets)}")
        html = render(d, year_stats, meta, featured, table_streets)
        slug = d["district"].lower().replace(" ", "-")
        fname = f"{slug}.preview.html" if PREVIEW else f"{slug}.html"
        path = os.path.join(OUT_DIR, fname)
        with open(path, "w") as f:
            f.write(html)
        print(f"  ✓ wrote {path} ({len(featured)} featured, "
              f"{len(table_streets)} in by-street table)")


if __name__ == "__main__":
    main()
