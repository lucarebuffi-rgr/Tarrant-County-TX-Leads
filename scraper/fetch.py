#!/usr/bin/env python3
"""
Tarrant County – Motivated Seller Lead Scraper
Clerk  : tarrant.tx.publicsearch.us
CAD    : tad.org  →  PropertyData_R_2025(Certified).ZIP  (pipe-delimited)
"""

import asyncio, csv, io, json, os, re, unicodedata, zipfile
from datetime import datetime, timedelta, timezone
from pathlib import Path

import httpx
from playwright.async_api import async_playwright

LOOKBACK_DAYS = int(os.getenv("LOOKBACK_DAYS", "14"))
BASE_URL      = "https://tarrant.tx.publicsearch.us"
CAD_ZIP_URL   = (
    "https://www.tad.org/content/data-download/"
    "PropertyData_R_2025(Certified).ZIP"
)
OUT_PATH      = Path(__file__).parent.parent / "dashboard" / "records.json"

KEEP_TYPES = {
    "TAX LIEN", "FEDERAL TAX LIEN", "STATE TAX LIEN",
    "JUDGMENT", "ABSTRACT OF JUDGMENT",
    "LIS PENDENS",
    "PROBATE", "LETTERS TESTAMENTARY", "LETTERS OF ADMINISTRATION",
    "NOTICE OF DEFAULT",
    "MECHANIC LIEN", "MATERIALMAN LIEN",
}

def normalize(s: str) -> str:
    s = unicodedata.normalize("NFD", s)
    s = "".join(c for c in s if unicodedata.category(c) != "Mn")
    return re.sub(r"\s+", " ", s).strip().lower()

ENTITY_RE = re.compile(
    r"\b(llc|lp|inc|corp|trust|trustee|bank|mortgage|financial|"
    r"properties|investments|holdings|services|county|city|state|"
    r"federal|government|isd|school|church|association|assoc|"
    r"national|united|american)\b"
)

def is_entity(name: str) -> bool:
    n = normalize(name)
    if ENTITY_RE.search(n):
        return True
    if "," not in name:
        return True
    return False

def name_tokens(name: str):
    parts = name.split(",", 1)
    last  = normalize(parts[0]).split()
    first = normalize(parts[1]).split() if len(parts) > 1 else []
    return (last[0] if last else ""), (first[0] if first else "")

def fuzzy_match(cad_name: str, clerk_name: str) -> bool:
    cl, cf = name_tokens(cad_name)
    kl, kf = name_tokens(clerk_name)
    if not cl or not kl:
        return False
    return cl == kl and cf and kf and cf == kf

def load_cad() -> dict[str, dict]:
    print("Downloading TAD CAD data ...")
    resp = httpx.get(CAD_ZIP_URL, timeout=120, follow_redirects=True,
                     headers={"User-Agent": "Mozilla/5.0"})
    resp.raise_for_status()
    print(f"  Downloaded {len(resp.content)/1_048_576:.1f} MB")
    zf   = zipfile.ZipFile(io.BytesIO(resp.content))
    name = next(n for n in zf.namelist() if n.upper().endswith(".TXT"))
    raw  = zf.read(name).decode("latin-1")
    cad: dict[str, dict] = {}
    reader = csv.reader(io.StringIO(raw), delimiter="|")
    for row in reader:
        if len(row) < 14:
            continue
        if row[0] not in ("R", "RP"):
            continue
        if row[0] == "RP":
            continue
        prop_class = row[13].strip()
        if not prop_class.startswith("A"):
            continue
        acct = row[2].strip().lstrip("0") or row[2].strip()
        cad[acct] = {
            "owner_name"    : row[6].strip(),
            "mail_address"  : row[7].strip(),
            "mail_citstate" : row[8].strip(),
            "mail_zip"      : row[9].strip(),
            "situs_address" : row[12].strip(),
        }
    print(f"  Loaded {len(cad):,} residential CAD records")
    return cad

JS_WAIT_FOR_ROWS = """
async (maxIter) => {
    for (let i = 0; i < maxIter; i++) {
        const rows = document.querySelectorAll('tr.search-row');
        if (rows.length > 0) return rows.length;
        await new Promise(r => setTimeout(r, 500));
    }
    return 0;
}
"""

async def scrape_clerk(doc_type: str, start_dt: datetime, end_dt: datetime,
                       page) -> list[dict]:
    s = start_dt.strftime("%m/%d/%Y")
    e = end_dt.strftime("%m/%d/%Y")
    url = (
        f"{BASE_URL}/search/index?"
        f"searchOpt=quickSearch"
        f"&recordType=official"
        f"&docType={doc_type}"
        f"&dateFrom={s}&dateTo={e}"
    )
    print(f"  Fetching {doc_type}  {s} -> {e}")
    await page.goto(url, timeout=120_000, wait_until="domcontentloaded")
    count = await page.evaluate(JS_WAIT_FOR_ROWS, 120)
    if count == 0:
        return []
    rows = await page.query_selector_all("tr.search-row")
    results = []
    for row in rows:
        cells = await row.query_selector_all("td")
        if len(cells) < 6:
            continue
        texts = [await c.inner_text() for c in cells]
        results.append({
            "doc_type"   : doc_type,
            "doc_number" : texts[0].strip(),
            "filed_date" : texts[1].strip(),
            "grantor"    : texts[2].strip(),
            "grantee"    : texts[3].strip(),
            "legal"      : texts[4].strip(),
            "book_page"  : texts[5].strip() if len(texts) > 5 else "",
        })
    return results

def build_output(clerk_rows: list[dict], cad: dict[str, dict]) -> list[dict]:
    seen  : set[str]   = set()
    leads : list[dict] = []
    for row in clerk_rows:
        doc_num = row["doc_number"]
        if doc_num in seen:
            continue
        grantor = row["grantor"]
        if not grantor or is_entity(grantor):
            continue
        cad_match = None
        for acct, info in cad.items():
            if fuzzy_match(info["owner_name"], grantor):
                cad_match = (acct, info)
                break
        seen.add(doc_num)
        lead = {
            "doc_number" : doc_num,
            "doc_type"   : row["doc_type"],
            "filed_date" : row["filed_date"],
            "grantor"    : grantor,
            "grantee"    : row["grantee"],
            "legal"      : row["legal"],
        }
        if cad_match:
            acct, info = cad_match
            lead.update({
                "account_num"   : acct,
                "owner_name"    : info["owner_name"],
                "situs_address" : info["situs_address"],
                "mail_address"  : info["mail_address"],
                "mail_citstate" : info["mail_citstate"],
                "mail_zip"      : info["mail_zip"],
            })
        leads.append(lead)
    return leads

async def main():
    end_dt   = datetime.now(timezone.utc)
    start_dt = end_dt - timedelta(days=LOOKBACK_DAYS)
    cad = load_cad()
    all_rows: list[dict] = []
    async with async_playwright() as pw:
        browser = await pw.chromium.launch(args=["--no-sandbox"])
        page    = await browser.new_page()
        for doc_type in KEEP_TYPES:
            try:
                rows = await scrape_clerk(doc_type, start_dt, end_dt, page)
                print(f"    -> {len(rows)} rows")
                all_rows.extend(rows)
            except Exception as exc:
                print(f"    x {doc_type}: {exc}")
        await browser.close()
    print(f"\nTotal clerk rows  : {len(all_rows)}")
    leads = build_output(all_rows, cad)
    print(f"Leads after filter: {len(leads)}")
    OUT_PATH.parent.mkdir(parents=True, exist_ok=True)
    OUT_PATH.write_text(json.dumps({
        "generated" : datetime.now(timezone.utc).isoformat(),
        "county"    : "Tarrant",
        "records"   : leads,
    }, indent=2))
    print(f"Wrote {len(leads)} leads -> {OUT_PATH}")

if __name__ == "__main__":
    asyncio.run(main())
