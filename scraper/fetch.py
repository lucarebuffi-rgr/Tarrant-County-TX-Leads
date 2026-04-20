#!/usr/bin/env python3
"""
Tarrant County TX – Motivated Seller Lead Scraper
"""

import asyncio
import csv
import io
import json
import logging
import re
import traceback
import zipfile
from datetime import datetime, timedelta
from difflib import SequenceMatcher
from pathlib import Path
from typing import Optional

try:
    from playwright.async_api import async_playwright
    HAS_PLAYWRIGHT = True
except ImportError:
    HAS_PLAYWRIGHT = False

import httpx

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s [%(levelname)s] %(message)s",
    datefmt="%Y-%m-%d %H:%M:%S",
)
log = logging.getLogger(__name__)

BASE_URL      = "https://tarrant.tx.publicsearch.us"
CAD_ZIP_URL   = (
    "https://www.tad.org/content/data-download/"
    "PropertyData_R_2025(Certified).ZIP"
)
LOOKBACK_DAYS   = 7
PAGE_LIMIT      = 250
REQUEST_TIMEOUT = 300

DOC_TYPES = {
    "LP":   ("pre_foreclosure", "Lis Pendens"),
    "FTL":  ("lien",            "Federal Tax Lien"),
    "STL":  ("lien",            "State Tax Lien"),
    "J":    ("judgment",        "Judgment"),
    "PROB": ("probate",         "Probate"),
    "AOH":  ("probate",         "Affidavit of Heirship"),
    "L":    ("lien",            "Lien"),
    "ML":   ("lien",            "Mechanics Lien"),
    "CL":   ("lien",            "Child Support Lien"),
    "DIV":  ("other",           "Divorce"),
}

GRANTEE_IS_OWNER = {"L", "CL", "FTL", "STL", "J", "ML"}

NAME_SUFFIXES = {"JR", "SR", "II", "III", "IV", "V", "ESQ", "TRUSTEE", "TR",
                 "ETAL", "ET", "AL", "ET AL", "ETUX", "ET UX", "ESTATE"}

ENTITY_FILTERS = (
    "LLC", "INC", "CORP", "LTD", "LP ", "L.P.", "TRUST", "ASSOC", "HOMEOWNERS",
    "STATE OF", "CITY OF", "COUNTY OF", "DISTRICT", "MUNICIPALITY", "DEPT ",
    "ISD", "UTILITY", "AUTHORITY", "COMMISSION", "FEDERAL", "NATIONAL BANK",
    "MORTGAGE", "FINANCIAL", "INVESTMENT", "PROPERTIES", "REALTY", "HOLDINGS",
    "PARTNERS", "GROUP", "SERVICES", "MANAGEMENT", "SOLUTIONS", "ENTERPRISES",
    "N/A", "UNKNOWN", "PUBLIC", "ATTY GEN", "ATTY/GEN", "FORT WORTH CITY",
    "CITY FORT WORTH", "TARRANT COUNTY", "ARLINGTON CITY"
)


def parse_date(raw: str) -> Optional[str]:
    for fmt in ("%m/%d/%Y", "%Y-%m-%d", "%m-%d-%Y", "%Y%m%d"):
        try:
            return datetime.strptime(raw.strip(), fmt).strftime("%Y-%m-%d")
        except ValueError:
            continue
    return None


def strip_suffixes(tokens: list) -> list:
    return [t for t in tokens if t not in NAME_SUFFIXES]


def name_variants(full: str) -> list:
    full = re.sub(r"[^\w\s]", "", full.strip().upper())
    tokens = strip_suffixes(full.split())
    if not tokens:
        return [full]
    variants = set()
    variants.add(" ".join(tokens))
    if len(tokens) < 2:
        return list(variants)
    last  = tokens[0]
    first = tokens[1] if len(tokens) > 1 else ""
    mid   = tokens[2] if len(tokens) > 2 else ""
    variants.add(f"{last} {first} {mid}".strip())
    variants.add(f"{last}, {first} {mid}".strip())
    variants.add(f"{last} {first}")
    variants.add(f"{last}, {first}")
    variants.add(f"{first} {last}")
    if mid:
        variants.add(f"{first} {mid} {last}")
        variants.add(f"{first} {last}")
        if len(mid) == 1:
            variants.add(f"{last} {first}")
    return [v for v in variants if v]


def normalize_for_fuzzy(name: str) -> tuple:
    name = re.sub(r"[^\w\s]", "", name.strip().upper())
    tokens = strip_suffixes(name.split())
    filtered = [t for t in tokens if len(t) > 1]
    if len(filtered) >= 2:
        tokens = filtered
    if not tokens:
        return ("", set())
    return tokens[0], set(tokens[1:])


def is_entity(name: str) -> bool:
    n = name.strip().upper()
    if not n or n in ("N/A", "NA", "UNKNOWN", "PUBLIC", ""):
        return True
    tokens = [t for t in re.sub(r"[^\w\s]", "", n).split() if len(t) > 1]
    if len(tokens) < 2:
        return True
    return any(x in n for x in ENTITY_FILTERS)


# ── CAD LOOKUP ────────────────────────────────────────────────────────────

def build_parcel_lookup() -> dict:
    lookup = {}
    log.info("Downloading TAD residential CAD data ...")
    try:
        resp = httpx.get(CAD_ZIP_URL, timeout=120, follow_redirects=True,
                         headers={"User-Agent": "Mozilla/5.0"})
        resp.raise_for_status()
        log.info(f"  Downloaded {len(resp.content)/1_048_576:.1f} MB")

        zf      = zipfile.ZipFile(io.BytesIO(resp.content))
        fname   = next(n for n in zf.namelist() if n.upper().endswith(".TXT"))
        raw     = zf.read(fname).decode("latin-1")

        total = 0
        reader = csv.reader(io.StringIO(raw), delimiter="|")
        for row in reader:
            if len(row) < 14:
                continue
            if row[0] == "RP":
                continue
            if row[0] != "R":
                continue
            prop_class = row[13].strip()
            if not prop_class.startswith("A"):
                continue
            owner_name = row[6].strip().upper()
            if not owner_name or is_entity(owner_name):
                continue
            situs      = row[12].strip()
            mail_addr  = row[7].strip()
            city_state = row[8].strip()
            mail_zip   = row[9].strip()
            mail_city  = city_state.split(",")[0].strip() if "," in city_state else city_state
            mail_state = city_state.split(",")[1].strip() if "," in city_state else "TX"
            parcel = {
                "prop_address": situs,
                "prop_city":    "Fort Worth",
                "prop_state":   "TX",
                "prop_zip":     "",
                "mail_address": mail_addr,
                "mail_city":    mail_city,
                "mail_state":   mail_state,
                "mail_zip":     mail_zip,
            }
            for variant in name_variants(owner_name):
                lookup[variant] = parcel
            total += 1
            if total % 50000 == 0:
                log.info(f"  Processed {total:,} parcels...")

        log.info(f"TAD lookup built: {len(lookup):,} name variants from {total:,} parcels")
    except Exception:
        log.error(f"CAD lookup error:\n{traceback.format_exc()}")
    return lookup


# ── TEXT BLOCK PARSER ─────────────────────────────────────────────────────

def parse_text_block(text: str, doc_code: str, cat: str, cat_label: str,
                     dt_from: str, dt_to: str) -> Optional[dict]:
    try:
        if not text:
            return None
        parts = [p.strip() for p in text.split("\t")]
        if len(parts) < 5:
            return None
        non_empty = [p for p in parts if p]
        if len(non_empty) < 3:
            return None
        grantor   = non_empty[0]
        grantee   = non_empty[1]
        filed_raw = ""
        doc_num   = ""
        legal     = ""
        for i, p in enumerate(non_empty):
            if re.match(r"\d{1,2}/\d{1,2}/\d{4}", p):
                filed_raw = p
                doc_num   = non_empty[i + 1] if i + 1 < len(non_empty) else ""
                legal     = non_empty[i + 3] if i + 3 < len(non_empty) else ""
                break
        if not grantor or not filed_raw:
            return None
        search_url = (f"{BASE_URL}/results?department=RP&_docTypes={doc_code}"
                      f"&recordedDateRange={dt_from},{dt_to}&searchType=advancedSearch")
        return {
            "doc_num":   doc_num,
            "doc_type":  doc_code,
            "cat":       cat,
            "cat_label": cat_label,
            "filed":     parse_date(filed_raw) or filed_raw,
            "grantor":   grantor,
            "grantee":   grantee,
            "legal":     legal,
            "amount":    None,
            "clerk_url": search_url,
            "_demo":     False,
        }
    except Exception:
        return None


# ── PLAYWRIGHT SCRAPER ────────────────────────────────────────────────────

JS_WAIT_FOR_ROWS = """
    async () => {
        for (let i = 0; i < 120; i++) {
            const rows = document.querySelectorAll('tbody tr');
            if (rows.length > 0) {
                const texts = [];
                rows.forEach(row => {
                    const cells = row.querySelectorAll('td');
                    if (cells.length >= 5) {
                        const parts = [];
                        cells.forEach(td => parts.push(td.innerText.trim()));
                        texts.push(parts.join('\\t'));
                    }
                });
                if (texts.length > 0) return texts;
            }
            await new Promise(r => setTimeout(r, 500));
        }
        return [];
    }
"""


async def scrape_doc_type(browser, doc_code: str, cat: str, cat_label: str,
                           dt_from: str, dt_to: str) -> list:
    records = []
    offset  = 0
    while True:
        url = (f"{BASE_URL}/results"
               f"?department=RP"
               f"&_docTypes={doc_code}"
               f"&recordedDateRange={dt_from},{dt_to}"
               f"&searchType=advancedSearch"
               f"&limit={PAGE_LIMIT}"
               f"&offset={offset}")
        log.info(f"  {doc_code} offset={offset} ...")
        context = await browser.new_context(
            user_agent="Mozilla/5.0 (X11; Linux x86_64) AppleWebKit/537.36 "
                       "(KHTML, like Gecko) Chrome/120.0.0.0 Safari/537.36"
        )
        page = await context.new_page()
        try:
            await page.goto(url, timeout=120_000)
            js_result = await page.evaluate(JS_WAIT_FOR_ROWS)
            log.info(f"    Got {len(js_result)} rows")
            for text in js_result:
                rec = parse_text_block(text, doc_code, cat, cat_label, dt_from, dt_to)
                if rec:
                    records.append(rec)
            if len(js_result) < PAGE_LIMIT:
                log.info(f"    Last page for {doc_code}")
                break
            offset += PAGE_LIMIT
        except Exception as e:
            log.warning(f"    Error scraping {doc_code} offset={offset}: {e}")
            break
        finally:
            await page.close()
            await context.close()
    log.info(f"  {doc_code} total: {len(records)} records")
    return records


async def scrape_all_playwright(date_from: str, date_to: str) -> list:
    if not HAS_PLAYWRIGHT:
        log.error("Playwright not available!")
        return []
    try:
        dt_from = datetime.strptime(date_from, "%m/%d/%Y").strftime("%Y%m%d")
        dt_to   = datetime.strptime(date_to,   "%m/%d/%Y").strftime("%Y%m%d")
    except Exception:
        dt_from = date_from.replace("/", "")
        dt_to   = date_to.replace("/", "")
    all_records = []
    async with async_playwright() as p:
        browser = await p.chromium.launch(headless=True)
        for doc_code, (cat, cat_label) in DOC_TYPES.items():
            recs = await scrape_doc_type(browser, doc_code, cat, cat_label, dt_from, dt_to)
            all_records.extend(recs)
        await browser.close()
    return all_records


# ── DEMO DATA ─────────────────────────────────────────────────────────────

def generate_demo_records(date_from: str, date_to: str) -> list:
    samples = [
        ("LP",  "pre_foreclosure", "Lis Pendens",          "SMITH ROBERT",          "ROCKET MORTGAGE LLC",  0),
        ("JUD", "judgment",        "Judgment",              "JONES MARY B",          "CAPITAL ONE NA",   87500),
        ("FTL", "lien",            "Federal Tax Lien",      "WILLIAMS DAVID",        "IRS",              45200),
        ("AJ",  "judgment",        "Abstract of Judgment",  "JOHNSON PATRICIA",      "CITIBANK NA",      18700),
        ("ML",  "lien",            "Mechanics Lien",        "BROWN MICHAEL",         "LONE STAR CONTR",  22000),
        ("PB",  "probate",         "Probate",               "ESTATE OF DAVIS JAMES", "TARRANT CO PROB",      0),
        ("STL", "lien",            "State Tax Lien",        "HENDERSON ROBERT",      "STATE OF TEXAS",    9800),
        ("CSL", "lien",            "Child Support Lien",    "RODRIGUEZ JUAN",        "ATTY/GEN",          5000),
        ("ALN", "lien",            "HOA Lien",              "THOMPSON SARAH",        "TARRANT HOA",       2100),
        ("AH",  "probate",         "Affidavit of Heirship", "GARCIA CARLOS",         "GARCIA MARIA",         0),
    ]
    base = datetime.strptime(date_from, "%m/%d/%Y")
    recs = []
    for i, (code, cat, cat_label, grantor, grantee, amt) in enumerate(samples):
        filed_dt = base + timedelta(days=i % LOOKBACK_DAYS)
        recs.append({
            "doc_num":   f"2026-DEMO-{i+1:04d}",
            "doc_type":  code,
            "cat":       cat,
            "cat_label": cat_label,
            "filed":     filed_dt.strftime("%Y-%m-%d"),
            "grantor":   grantor,
            "grantee":   grantee,
            "legal":     "DEMO RECORD",
            "amount":    float(amt) if amt else None,
            "clerk_url": f"{BASE_URL}/results?department=RP&_docTypes={code}&searchType=advancedSearch",
            "_demo":     True,
        })
    return recs


# ── ENRICHMENT ────────────────────────────────────────────────────────────

def enrich_with_parcel(records: list, lookup: dict) -> list:
    fuzzy_index = []
    seen = set()
    for variant, parcel in lookup.items():
        last, firsts = normalize_for_fuzzy(variant)
        key = (last, frozenset(firsts))
        if last and key not in seen:
            seen.add(key)
            fuzzy_index.append((last, firsts, parcel))

    matched = 0
    for rec in records:
        dtype  = rec.get("doc_type", "")
        owner  = (rec.get("grantee") if dtype in GRANTEE_IS_OWNER else rec.get("grantor") or "").upper().strip()
        parcel = None

        if is_entity(owner):
            rec.setdefault("prop_address", "")
            rec.setdefault("prop_city",    "")
            rec.setdefault("prop_state",   "TX")
            rec.setdefault("prop_zip",     "")
            rec.setdefault("mail_address", "")
            rec.setdefault("mail_city",    "")
            rec.setdefault("mail_state",   "TX")
            rec.setdefault("mail_zip",     "")
            continue

        for variant in name_variants(owner):
            parcel = lookup.get(variant)
            if parcel:
                break

        if not parcel and owner:
            o_last, o_firsts = normalize_for_fuzzy(owner)
            if o_last and o_firsts:
                for c_last, c_firsts, candidate in fuzzy_index:
                    if c_last != o_last:
                        continue
                    if not c_firsts:
                        continue
                    if o_firsts & c_firsts:
                        parcel = candidate
                        break
                    o_str = " ".join(sorted(o_firsts))
                    c_str = " ".join(sorted(c_firsts))
                    if o_str and c_str and SequenceMatcher(None, o_str, c_str).ratio() >= 0.85:
                        parcel = candidate
                        break

        if parcel:
            rec.update(parcel)
            matched += 1
        else:
            rec.setdefault("prop_address", "")
            rec.setdefault("prop_city",    "")
            rec.setdefault("prop_state",   "TX")
            rec.setdefault("prop_zip",     "")
            rec.setdefault("mail_address", "")
            rec.setdefault("mail_city",    "")
            rec.setdefault("mail_state",   "TX")
            rec.setdefault("mail_zip",     "")

    log.info(f"Parcel enrichment: {matched}/{len(records)} records matched")
    return records


# ── SCORING ───────────────────────────────────────────────────────────────

def score_record(rec: dict) -> tuple:
    score = 30
    flags = []
    dtype  = rec.get("doc_type", "")
    amount = rec.get("amount") or 0

    if dtype == "LP":                flags.append("Lis pendens")
    if dtype in ("FTL","STL","NLF"): flags.append("Tax lien")
    if dtype in ("JUD","AJ"):        flags.append("Judgment lien")
    if dtype in ("PB","AH"):         flags.append("Probate / estate")
    if dtype == "ML":                flags.append("Mechanic lien")
    if dtype in ("LC","ALN","ADL"):  flags.append("Lien")
    if dtype == "CSL":               flags.append("Child support lien")
    if dtype == "HL":                flags.append("Hospital lien")

    owner = rec.get("owner", "").upper()
    if any(x in owner for x in ("LLC", "INC", "CORP", "LTD", "LP ", "L.P.")):
        flags.append("LLC / corp owner")

    try:
        filed = datetime.strptime(rec.get("filed", ""), "%Y-%m-%d")
        if (datetime.today() - filed).days <= 14:
            flags.append("New this week")
    except Exception:
        pass

    has_addr = bool(rec.get("prop_address") or rec.get("mail_address"))
    score += 10 * len(flags)
    if "Lis pendens" in flags:      score += 20
    if "Probate / estate" in flags: score += 10
    if "Bankruptcy" in flags:       score += 15
    if amount and amount > 100_000: score += 15
    elif amount and amount > 50_000: score += 10
    if "New this week" in flags:    score += 5
    if has_addr:                    score += 5
    return min(score, 100), flags


# ── OUTPUT ────────────────────────────────────────────────────────────────

def build_output(raw_records: list, date_from: str, date_to: str) -> dict:
    out_records = []
    for raw in raw_records:
        try:
            dtype = raw.get("doc_type", "")
            if dtype in GRANTEE_IS_OWNER:
                owner   = raw.get("grantee", "")
                grantee = raw.get("grantor", "")
            else:
                owner   = raw.get("grantor", "")
                grantee = raw.get("grantee", "")

            score, flags = score_record({**raw, "owner": owner})

            out_records.append({
                "doc_num":      raw.get("doc_num", ""),
                "doc_type":     dtype,
                "filed":        raw.get("filed", ""),
                "cat":          raw.get("cat", "other"),
                "cat_label":    raw.get("cat_label", ""),
                "owner":        owner,
                "grantee":      grantee,
                "amount":       raw.get("amount"),
                "legal":        raw.get("legal", ""),
                "prop_address": raw.get("prop_address", ""),
                "prop_city":    raw.get("prop_city", ""),
                "prop_state":   raw.get("prop_state", "TX"),
                "prop_zip":     raw.get("prop_zip", ""),
                "mail_address": raw.get("mail_address", ""),
                "mail_city":    raw.get("mail_city", ""),
                "mail_state":   raw.get("mail_state", "TX"),
                "mail_zip":     raw.get("mail_zip", ""),
                "clerk_url":    raw.get("clerk_url", ""),
                "flags":        flags,
                "score":        score,
                "_demo":        raw.get("_demo", False),
            })
        except Exception:
            log.warning(f"Skipping: {traceback.format_exc()}")

    out_records = [r for r in out_records if r.get("prop_address") or r.get("mail_address")]
    out_records = [r for r in out_records if not is_entity(r.get("owner", ""))]
    out_records = [r for r in out_records if not any(
        x in (r.get("owner", "")).upper() for x in ENTITY_FILTERS
    )]
    out_records.sort(key=lambda r: (-r["score"], r.get("filed", "") or ""))
    with_address = sum(1 for r in out_records if r["prop_address"] or r["mail_address"])

    return {
        "fetched_at":   datetime.utcnow().isoformat() + "Z",
        "source":       "Tarrant County TX – PublicSearch",
        "date_range":   {"from": date_from, "to": date_to},
        "total":        len(out_records),
        "with_address": with_address,
        "records":      out_records,
    }


def save_output(data: dict):
    for path in ["dashboard/records.json", "data/records.json"]:
        p = Path(path)
        p.parent.mkdir(parents=True, exist_ok=True)
        p.write_text(json.dumps(data, indent=2))
        log.info(f"Saved {data['total']} records → {path}")


def export_ghl_csv(data: dict):
    fieldnames = [
        "First Name", "Last Name", "Mailing Address", "Mailing City",
        "Mailing State", "Mailing Zip", "Property Address", "Property City",
        "Property State", "Property Zip", "Lead Type", "Document Type",
        "Date Filed", "Document Number", "Amount/Debt Owed", "Seller Score",
        "Motivated Seller Flags", "Source", "Public Records URL",
    ]
    buf = io.StringIO()
    writer = csv.DictWriter(buf, fieldnames=fieldnames)
    writer.writeheader()
    for r in data["records"]:
        parts = (r.get("owner", "")).split()
        writer.writerow({
            "First Name":             parts[0] if parts else "",
            "Last Name":              " ".join(parts[1:]) if len(parts) > 1 else "",
            "Mailing Address":        r.get("mail_address", ""),
            "Mailing City":           r.get("mail_city", ""),
            "Mailing State":          r.get("mail_state", "TX"),
            "Mailing Zip":            r.get("mail_zip", ""),
            "Property Address":       r.get("prop_address", ""),
            "Property City":          r.get("prop_city", ""),
            "Property State":         r.get("prop_state", "TX"),
            "Property Zip":           r.get("prop_zip", ""),
            "Lead Type":              r.get("cat_label", ""),
            "Document Type":          r.get("doc_type", ""),
            "Date Filed":             r.get("filed", ""),
            "Document Number":        r.get("doc_num", ""),
            "Amount/Debt Owed":       str(r.get("amount", "") or ""),
            "Seller Score":           str(r.get("score", "")),
            "Motivated Seller Flags": "|".join(r.get("flags", [])),
            "Source":                 "Tarrant County TX",
            "Public Records URL":     r.get("clerk_url", ""),
        })
    Path("data/ghl_export.csv").write_text(buf.getvalue())
    log.info("GHL CSV saved")


# ── MAIN ──────────────────────────────────────────────────────────────────

async def main():
    today     = datetime.today()
    start     = today - timedelta(days=LOOKBACK_DAYS)
    date_from = start.strftime("%m/%d/%Y")
    date_to   = today.strftime("%m/%d/%Y")

    log.info("=== Tarrant County TX Lead Scraper ===")
    log.info(f"Date range: {date_from} → {date_to}")

    log.info("Building parcel lookup ...")
    parcel_lookup = build_parcel_lookup()
    log.info(f"  {len(parcel_lookup):,} name variants indexed")

    log.info("Scraping clerk records ...")
    raw_records = await scrape_all_playwright(date_from, date_to)
    log.info(f"Total raw records: {len(raw_records)}")

    if not raw_records:
        log.warning("No live records – using demo data")
        raw_records = generate_demo_records(date_from, date_to)

    raw_records = enrich_with_parcel(raw_records, parcel_lookup)
    data = build_output(raw_records, date_from, date_to)
    save_output(data)
    export_ghl_csv(data)
    log.info(f"Done. {data['total']} leads | {data['with_address']} with address")


if __name__ == "__main__":
    asyncio.run(main())
