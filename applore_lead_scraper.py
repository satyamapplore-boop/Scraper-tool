"""
Applore Technologies — European Lead Generation Scraper
========================================================
Targets: Mid-market (100-1000 employees) + VC-backed healthtech/fintech/B2B SaaS
Countries: UK, Netherlands, Germany, Sweden, Denmark, Norway, France, Spain
Contacts: CTO, CPO, VP Engineering, CEO (small cos), Head of Product

Sources used (no paid login required):
  1. Crunchbase public search pages  → company list
  2. LinkedIn public search          → decision-maker profiles
  3. Apollo.io public org pages      → contact email hints
  4. Hunter.io email guesser pattern → email construction

OUTPUT: leads.csv  (company, website, country, funding, sector, contact name,
                    title, LinkedIn URL, email guess, confidence)

SETUP (run once):
  pip install requests beautifulsoup4 selenium undetected-chromedriver \
              pandas tqdm fake-useragent python-dotenv

  Install Chrome + chromedriver matching your Chrome version.
  https://chromedriver.chromium.org/downloads

USAGE:
  python applore_lead_scraper.py

  The scraper is intentionally slow (random delays) to avoid blocks.
  A full run across all countries/sectors takes ~2-3 hours.
  You can interrupt at any time — progress is saved to leads_raw.json.
"""

import csv
import json
import os
import random
import re
import time
from datetime import datetime, timedelta
from pathlib import Path

import pandas as pd
import requests
from bs4 import BeautifulSoup
from dotenv import load_dotenv

load_dotenv()

try:
    import dns.resolver
    DNS_AVAILABLE = True
except ImportError:
    DNS_AVAILABLE = False
    print("[WARN] dnspython not found. MX checks disabled.")

# ── Optional: Selenium for JS-heavy pages ────────────────────────────────────
try:
    import undetected_chromedriver as uc
    from selenium.webdriver.common.by import By
    from selenium.webdriver.support import expected_conditions as EC
    from selenium.webdriver.support.ui import WebDriverWait
    SELENIUM_AVAILABLE = True
except ImportError:
    SELENIUM_AVAILABLE = False
    print("[WARN] undetected-chromedriver not found. Selenium scraping disabled.")
    print("       Install with: pip install undetected-chromedriver selenium")

# ─────────────────────────────────────────────────────────────────────────────
#  CONFIG
# ─────────────────────────────────────────────────────────────────────────────

OUTPUT_FILE    = "leads.csv"
PROGRESS_FILE  = "leads_raw.json"
CACHE_FILE     = "email_cache.json"
DELAY_MIN      = 1.0   # seconds between requests (accelerated)
DELAY_MAX      = 2.5
MAX_PAGES      = 5     # Crunchbase pages per query (25 results/page = 125 cos)

EMAILVALIDATR_API_KEY = os.getenv("EMAILVALIDATR_API_KEY", "")
global_use_fallback = False

def get_output_files():
    date_str = datetime.now().strftime("%Y-%m-%d")
    return f"leads_verified_{date_str}.csv", f"leads_rejected_{date_str}.csv"

TARGET_COUNTRIES = {
    "United Kingdom": "united-kingdom",
    "Netherlands":    "netherlands",
    "Germany":        "germany",
    "Sweden":         "sweden",
    "Denmark":        "denmark",
    "Norway":         "norway",
    "France":         "france",
    "Spain":          "spain",
}

TARGET_SECTORS = [
    "health-care",        # healthtech
    "financial-services", # fintech
    "fin-tech",
    "health-tech",
    "software",           # B2B SaaS
    "saas",
    "enterprise-software",
    "information-technology",
]

# Non-tech-native sectors where Applore acts as tech partner
NON_TECH_NATIVE_SECTORS = [
    "manufacturing", "industrials", "retail", "logistics",
    "supply-chain", "real-estate", "construction", "agriculture",
    "legal", "education", "media", "publishing", "hospitality",
    "food-and-beverage", "energy", "transportation", "healthcare",
    "professional-services", "accounting", "insurance",
]

# Pure-tech keywords to EXCLUDE when targeting non-tech-native cos
PURE_TECH_EXCLUDE_KEYWORDS = [
    "developer tools", "devtools", "infrastructure", "developer platform",
    "api", "cloud infrastructure", "cybersecurity", "data infrastructure",
    "open source", "mlops", "llm", "ai platform", "no-code", "low-code",
]

TARGET_TITLES = [
    "CTO", "Chief Technology Officer",
    "CPO", "Chief Product Officer",
    "VP Engineering", "VP of Engineering",
    "Head of Engineering", "Head of Product",
    "VP Product", "Co-Founder", "CEO", "Founder", "President",
    "Managing Director", "Chief Executive Officer", "COO", 
    "Chief Operating Officer", "CMO", "Chief Marketing Officer", 
    "VP Sales", "Head of Sales", "VP Business Development", 
    "Director of Engineering",
]

LINKEDIN_SEARCH_TITLES = [
    "Founder", "CEO", "CTO", "VP Sales", "CMO", "President"
]

EMPLOYEE_RANGE = "c_1_10,c_11_50,c_51_100,c_101_250,c_251_500"  # Target newer, smaller funded companies (1-500)
# Crunchbase employee filter codes:
#   c_1_10, c_11_50, c_51_100, c_101_250, c_251_500

HEADERS = {
    "User-Agent": (
        "Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) "
        "AppleWebKit/537.36 (KHTML, like Gecko) "
        "Chrome/122.0.0.0 Safari/537.36"
    ),
    "Accept-Language": "en-US,en;q=0.9",
    "Accept": "text/html,application/xhtml+xml,application/xml;q=0.9,*/*;q=0.8",
}

# ─────────────────────────────────────────────────────────────────────────────
#  HELPERS
# ─────────────────────────────────────────────────────────────────────────────

def sleep():
    t = random.uniform(DELAY_MIN, DELAY_MAX)
    print(f"    ⏳ sleeping {t:.1f}s...")
    time.sleep(t)


def get(url, session=None, retries=3):
    """HTTP GET with retry + exponential backoff."""
    requester = session or requests
    for attempt in range(retries):
        try:
            resp = requester.get(url, headers=HEADERS, timeout=15)
            if resp.status_code == 200:
                return resp
            if resp.status_code in [403, 404]:
                print(f"    ⚠️  HTTP {resp.status_code} for {url[:100]}... (Aborting retries)")
                return None
            if resp.status_code == 429:
                wait = 30 * (attempt + 1)
                print(f"    ⚠️  Rate limited. Waiting {wait}s...")
                time.sleep(wait)
            else:
                print(f"    ⚠️  HTTP {resp.status_code} for {url[:100]}...")
        except Exception as e:
            print(f"    ❌ Request error: {e}")
            
        # Only sleep if we actually need to retry
        time.sleep(1.5 * (attempt + 1))
    return None


def save_progress(data: list):
    with open(PROGRESS_FILE, "w") as f:
        json.dump(data, f, indent=2)
    print(f"  💾 Progress saved ({len(data)} records)")


def load_progress() -> list:
    if Path(PROGRESS_FILE).exists():
        with open(PROGRESS_FILE) as f:
            data = json.load(f)
        print(f"  ♻️  Resuming from {len(data)} saved records")
        return data
    return []

def load_email_cache() -> dict:
    if Path(CACHE_FILE).exists():
        try:
            with open(CACHE_FILE) as f:
                return json.load(f)
        except json.JSONDecodeError:
            return {}
    return {}

def save_email_cache(cache: dict):
    with open(CACHE_FILE, "w") as f:
        json.dump(cache, f, indent=2)


def guess_email(first: str, last: str, domain: str) -> tuple[str, str]:
    """
    Return (email_guess, confidence) using common corporate patterns.
    Confidence: high / medium / low
    """
    if not all([first, last, domain]):
        return "", "low"

    first  = first.lower().strip()
    last   = last.lower().strip()
    domain = domain.lower().strip().lstrip("www.").split("/")[0]

    patterns = [
        f"{first}.{last}@{domain}",        # john.smith@company.com  (most common)
        f"{first[0]}{last}@{domain}",       # jsmith@company.com
        f"{first}@{domain}",               # john@company.com
        f"{first}{last}@{domain}",         # johnsmith@company.com
        f"{last}.{first}@{domain}",        # smith.john@company.com
    ]

    # Return the first pattern (most likely) with confidence note
    return patterns[0], "medium"

def verify_email(email: str, cache: dict) -> dict:
    global global_use_fallback
    
    if not email:
        return {"valid": False, "reason": "empty", "skip": True, "confidence": "none"}
        
    # Check cache
    now = datetime.now()
    if email in cache:
        entry = cache[email]
        cached_time = datetime.fromisoformat(entry.get("timestamp", "2000-01-01T00:00:00"))
        if now - cached_time < timedelta(days=7):
            return entry.get("result")
            
    result = _do_verify_email(email)
    cache[email] = {
        "result": result,
        "timestamp": now.isoformat()
    }
    save_email_cache(cache)
    return result

def _do_verify_email(email: str) -> dict:
    global global_use_fallback
    
    # 1. Local syntax check
    pattern = re.compile(r"^[a-zA-Z0-9._%+-]+@[a-zA-Z0-9.-]+\.[a-zA-Z]{2,}$")
    if not pattern.match(email):
        return {"valid": False, "reason": "invalid_syntax", "skip": True, "confidence": "low"}
        
    domain = email.split("@")[1]
    
    # 2. MX Record Check
    if DNS_AVAILABLE:
        try:
            try:
                dns.resolver.resolve(domain, 'MX')
            except (dns.resolver.NoAnswer, dns.resolver.NXDOMAIN, dns.exception.Timeout):
                return {"valid": False, "reason": "no_mx", "skip": True, "confidence": "high"}
        except Exception:
            pass # Ignore other DNS errors and proceed
            
    # API Calls
    if not global_use_fallback:
        # 3. EmailValidatr API
        time.sleep(1) # Rate limit
        url = f"https://emailvalidatr.com/api/validate?email={email}&deep=true"
        headers = {"X-API-Key": EMAILVALIDATR_API_KEY}
        try:
            resp = requests.get(url, headers=headers, timeout=10)
            if resp.status_code == 429:
                global_use_fallback = True
            elif resp.status_code == 200:
                data = resp.json()
                res = data.get("result", {})
                validations = res.get("validations", {})
                is_valid = res.get("is_valid", False)
                disposable = validations.get("disposable", False)
                catch_all = validations.get("catch_all", False)
                
                if disposable:
                    return {"valid": False, "reason": "disposable", "skip": True, "confidence": "high"}
                if is_valid and not disposable:
                    return {"valid": True, "reason": "verified", "confidence": "high", "skip": False}
                if catch_all:
                    return {"valid": True, "reason": "catch_all", "confidence": "low", "skip": False}
                if not is_valid:
                    return {"valid": False, "reason": "invalid", "skip": True, "confidence": "high"}
        except requests.exceptions.RequestException:
            pass # Fall through to fallback
            
    # 4. Fallback API
    time.sleep(1) # Rate limit
    url = f"https://rapid-email-verifier.fly.dev/verify?email={email}"
    try:
        resp = requests.get(url, timeout=10)
        if resp.status_code == 200:
            data = resp.json()
            if data and data.get("result"):
                res_data = data.get("result")
                valid = res_data.get("valid", False)
                disposable = res_data.get("disposable", False)
                
                if disposable:
                    return {"valid": False, "reason": "disposable", "skip": True, "confidence": "high"}
                if valid and not disposable:
                    return {"valid": True, "reason": "verified_fallback", "confidence": "medium", "skip": False}
                if not valid:
                    return {"valid": False, "reason": "invalid_fallback", "skip": True, "confidence": "high"}
    except requests.exceptions.RequestException:
        pass
        
    # 5. Both APIs fail
    return {"valid": True, "reason": "unverified", "confidence": "low", "skip": False}


# ─────────────────────────────────────────────────────────────────────────────
#  SOURCE 1: CRUNCHBASE — Company Discovery
# ─────────────────────────────────────────────────────────────────────────────

def build_crunchbase_url(country_slug: str, sector_slug: str, page: int = 1) -> str:
    """
    Crunchbase Discover URL for companies.
    Filters: country, sector, employee count, has funding.
    """
    base = "https://www.crunchbase.com/discover/organization.companies"
    params = (
        f"?field_ids=identifier,short_description,location_identifiers,"
        f"num_employees_enum,funding_total,last_funding_type,website_url"
        f"&predefined_filter=funding"
        f"&location_identifiers={country_slug}"
        f"&category_groups_exact={sector_slug}"
        f"&num_employees_enum={EMPLOYEE_RANGE}"
        f"&page={page}"
    )
    return base + params


def scrape_crunchbase_companies(session) -> list:
    """
    Scrape Crunchbase for target companies.
    Returns list of dicts with company info.
    NOTE: Crunchbase heavily uses React/JS — we scrape their
    __NEXT_DATA__ JSON blob which is server-rendered.
    """
    companies = []
    seen_slugs = set()

    for country_name, country_slug in TARGET_COUNTRIES.items():
        for sector_slug in TARGET_SECTORS:
            print(f"\n  🔍 Crunchbase: {country_name} / {sector_slug}")
            for page in range(1, MAX_PAGES + 1):
                url = build_crunchbase_url(country_slug, sector_slug, page)
                resp = get(url, session)
                if not resp:
                    break

                soup = BeautifulSoup(resp.text, "html.parser")

                # Try to extract JSON from __NEXT_DATA__ script tag
                script = soup.find("script", {"id": "__NEXT_DATA__"})
                if script:
                    try:
                        data = json.loads(script.string)
                        entities = (
                            data.get("props", {})
                                .get("pageProps", {})
                                .get("bootstrapData", {})
                                .get("routing", {})
                                .get("searchResults", {})
                                .get("entities", [])
                        )
                        for entity in entities:
                            props = entity.get("properties", {})
                            slug  = props.get("identifier", {}).get("permalink", "")
                            if slug and slug not in seen_slugs:
                                seen_slugs.add(slug)
                                companies.append({
                                    "name":        props.get("identifier", {}).get("value", ""),
                                    "cb_slug":     slug,
                                    "description": props.get("short_description", ""),
                                    "country":     country_name,
                                    "sector":      sector_slug,
                                    "employees":   props.get("num_employees_enum", ""),
                                    "funding":     props.get("funding_total", {}).get("value_usd", ""),
                                    "last_round":  props.get("last_funding_type", ""),
                                    "website":     props.get("website_url", ""),
                                    "cb_url":      f"https://www.crunchbase.com/organization/{slug}",
                                })
                        print(f"    ✅ Page {page}: {len(entities)} companies found")
                        if len(entities) < 25:
                            break  # Last page
                    except (json.JSONDecodeError, KeyError) as e:
                        print(f"    ⚠️  JSON parse error: {e}")
                        # Fallback: try direct HTML parsing
                        cards = soup.select("[data-testid='component-entity-card']")
                        print(f"    ℹ️  HTML fallback: {len(cards)} cards found")

                sleep()

    print(f"\n  📦 Total companies discovered: {len(companies)}")
    return companies


# ─────────────────────────────────────────────────────────────────────────────
#  SOURCE 1.5: YCOMBINATOR — Open Startup Directory (No Blocks)
# ─────────────────────────────────────────────────────────────────────────────

def scrape_ycombinator_companies(session, target_count=None) -> list:
    """
    Scrape YCombinator Open API for startups.
    Bypasses Crunchbase blocks and finds targeted companies directly from YC.
    """
    companies = []
    seen_slugs = set()
    page = 0
    target_count = target_count or 999999
    
    print(f"\n  🔍 YCombinator: Extracting startups (unlimited)...")
    
    while len(companies) < target_count and page < 50:
        url = f"https://api.ycombinator.com/v0.1/companies?page={page}"
        resp = get(url, session)
        if not resp:
            break
            
        try:
            data = resp.json()
            api_companies = data.get("companies", [])
            if not api_companies:
                break
                
            for c in api_companies:
                slug = c.get("slug")
                if not slug or slug in seen_slugs:
                    continue

                industries_list = c.get("industries", [])
                industries_lower = [i.lower() for i in industries_list]
                desc_lower = (c.get("oneLiner", "") or "").lower()
                tags_lower = [t.lower() for t in c.get("tags", [])]

                # Keep only non-tech-native companies (traditional industries needing a tech partner)
                is_non_tech = any(
                    kw in " ".join(industries_lower + tags_lower + [desc_lower])
                    for kw in NON_TECH_NATIVE_SECTORS
                )
                is_pure_tech = any(
                    kw in " ".join(industries_lower + tags_lower + [desc_lower])
                    for kw in PURE_TECH_EXCLUDE_KEYWORDS
                )
                if not is_non_tech or is_pure_tech:
                    continue

                seen_slugs.add(slug)
                industries = ", ".join(industries_list)
                
                companies.append({
                    "name":        c.get("name", ""),
                    "cb_slug":     slug,
                    "description": c.get("oneLiner", ""),
                    "country":     c.get("location", "") or "Global",
                    "sector":      industries,
                    "employees":   str(c.get("teamSize", "")),
                    "funding":     "YC Backed",
                    "last_round":  c.get("batch", ""),
                    "website":     c.get("website", ""),
                    "cb_url":      c.get("url", ""),
                })
                
                if len(companies) >= target_count:
                    break
                        
            print(f"    ✅ Page {page}: Accumulated {len(companies)}/{target_count} companies...")
            page += 1
            sleep()
            
        except Exception as e:
            print(f"    ⚠️  JSON parse error on YC: {e}")
            break
            
    print(f"\n  📦 Total YC companies discovered: {len(companies)}")
    return companies


# ─────────────────────────────────────────────────────────────────────────────
#  SOURCE 2: EU-STARTUPS.COM — European Startup Directory
# ─────────────────────────────────────────────────────────────────────────────

def scrape_eu_startups(session) -> list:
    """
    Scrape eu-startups.com/directory/ for European companies.
    Filters for non-tech-native sectors matching Applore's target profile.
    """
    companies = []
    seen = set()
    base_url = "https://www.eu-startups.com/directory/"

    # Sector slugs available on eu-startups directory
    eu_startup_sectors = [
        "manufacturing", "health", "logistics", "retail", "energy",
        "agriculture", "construction", "legal", "education", "food",
        "real-estate", "transport", "insurance", "media",
    ]

    print(f"\n  🔍 EU-Startups: Scraping directory...")

    for sector in eu_startup_sectors:
        page = 1
        while page <= 10:
            url = f"{base_url}?wpbdp_sort=field-1&s=&cat=&sector={sector}&page={page}"
            resp = get(url, session)
            if not resp:
                break

            soup = BeautifulSoup(resp.text, "html.parser")
            listings = soup.select(".wpbdp-listing, .listing-item, article.type-wpbdp_listing")

            if not listings:
                # Try generic article/card selectors
                listings = soup.select("article, .company-card, .startup-card")

            if not listings:
                break

            for item in listings:
                name_el = item.select_one("h2 a, h3 a, .listing-title a, .entry-title a")
                if not name_el:
                    continue
                name = name_el.get_text(strip=True)
                if not name or name in seen:
                    continue

                link = name_el.get("href", "")
                desc_el = item.select_one(".listing-description, .excerpt, p")
                desc = desc_el.get_text(strip=True)[:200] if desc_el else ""
                country_el = item.select_one(".country, .location, [class*='country']")
                country = country_el.get_text(strip=True) if country_el else "Europe"

                seen.add(name)
                companies.append({
                    "name":        name,
                    "cb_slug":     link.rstrip("/").split("/")[-1],
                    "description": desc,
                    "country":     country,
                    "sector":      sector.title(),
                    "employees":   "",
                    "funding":     "",
                    "last_round":  "",
                    "website":     link,
                    "cb_url":      link,
                })

            print(f"    ✅ {sector} page {page}: {len(companies)} total so far")
            if len(listings) < 10:
                break
            page += 1
            sleep()

    print(f"\n  📦 Total EU-Startups companies: {len(companies)}")
    return companies


# ─────────────────────────────────────────────────────────────────────────────
#  SOURCE 3: F6S.COM — Global Startup Network (strong EU coverage)
# ─────────────────────────────────────────────────────────────────────────────

def scrape_f6s(session) -> list:
    """
    Scrape F6S.com for non-tech-native startups.
    F6S has strong European coverage and sector filtering.
    """
    companies = []
    seen = set()

    f6s_sectors = [
        "manufacturing", "healthcare", "logistics", "retail", "energy",
        "agriculture", "construction", "legaltech", "edtech", "food-drink",
        "real-estate", "transportation", "insurtech", "media-entertainment",
    ]

    target_countries_f6s = [
        "united-kingdom", "germany", "france", "netherlands",
        "sweden", "spain", "denmark", "norway",
    ]

    print(f"\n  🔍 F6S: Scraping startup directory...")

    for country in target_countries_f6s:
        for sector in f6s_sectors[:5]:  # Limit to top 5 sectors per country to avoid overload
            url = f"https://www.f6s.com/companies/{sector}?country={country}&sort=founded"
            resp = get(url, session)
            if not resp:
                continue

            soup = BeautifulSoup(resp.text, "html.parser")

            # F6S company cards
            cards = soup.select(".company-list-item, .startup-item, [class*='company-card']")
            if not cards:
                cards = soup.select("li.item, div.item")

            for card in cards:
                name_el = card.select_one("h2, h3, .name, a[href*='/company/']")
                if not name_el:
                    continue
                name = name_el.get_text(strip=True)
                if not name or name in seen or len(name) < 2:
                    continue

                link_el = card.select_one("a[href*='/company/'], a[href*='f6s.com']")
                link = link_el.get("href", "") if link_el else ""
                desc_el = card.select_one(".description, .tagline, p")
                desc = desc_el.get_text(strip=True)[:200] if desc_el else ""

                seen.add(name)
                companies.append({
                    "name":        name,
                    "cb_slug":     name.lower().replace(" ", "-"),
                    "description": desc,
                    "country":     country.replace("-", " ").title(),
                    "sector":      sector.replace("-", " ").title(),
                    "employees":   "",
                    "funding":     "",
                    "last_round":  "",
                    "website":     link,
                    "cb_url":      link,
                })

            if cards:
                print(f"    ✅ {country}/{sector}: {len(cards)} companies")
            sleep()

    print(f"\n  📦 Total F6S companies: {len(companies)}")
    return companies


# ─────────────────────────────────────────────────────────────────────────────
#  SOURCE 2: CRUNCHBASE — People at each company
# ─────────────────────────────────────────────────────────────────────────────

def scrape_crunchbase_people(company: dict, session) -> list:
    """
    Pull key contacts from a company's Crunchbase people tab.
    """
    slug = company.get("cb_slug", "")
    if not slug:
        return []

    url  = f"https://www.crunchbase.com/organization/{slug}/people"
    resp = get(url, session)
    if not resp:
        return []

    contacts = []
    soup = BeautifulSoup(resp.text, "html.parser")
    script = soup.find("script", {"id": "__NEXT_DATA__"})
    if not script:
        return []

    try:
        data    = json.loads(script.string)
        # Navigate the Crunchbase data tree to find people
        # Path varies by Crunchbase version — try multiple paths
        people_data = []
        try:
            people_data = (
                data["props"]["pageProps"]["bootstrapData"]
                    ["routing"]["currentPage"]["data"]["cards"]
                    .get("current_team_featured_order", {})
                    .get("cards", [])
            )
        except (KeyError, TypeError):
            pass

        if not people_data:
            # Alternative path
            try:
                sections = (
                    data["props"]["pageProps"]["bootstrapData"]
                        ["routing"]["currentPage"]["data"]["sections"]
                )
                for section in sections:
                    if "people" in str(section).lower():
                        people_data = section.get("cards", [])
                        break
            except (KeyError, TypeError):
                pass

        for person in people_data:
            props = person.get("properties", {}) or person
            title = props.get("title", "") or props.get("job_type", "")
            name  = props.get("name", "") or (
                props.get("identifier", {}) or {}
            ).get("value", "")

            # Only target decision-maker titles
            if any(t.lower() in title.lower() for t in TARGET_TITLES):
                li_url = props.get("linkedin", "") or ""
                contacts.append({
                    "contact_name":   name,
                    "title":          title,
                    "linkedin_url":   li_url,
                    "cb_person_url":  f"https://www.crunchbase.com/person/{props.get('identifier', {}).get('permalink', '')}",
                })

    except (json.JSONDecodeError, KeyError):
        pass

    return contacts


# ─────────────────────────────────────────────────────────────────────────────
#  SOURCE 3: GOOGLE Public Search -> LinkedIn (no login)
# ─────────────────────────────────────────────────────────────────────────────

def search_linkedin_contacts(company_name: str, session) -> list:
    """
    Search Google for LinkedIn public pages of decision-makers.
    """
    contacts = []
    for title_kw in LINKEDIN_SEARCH_TITLES:
        query    = f'site:linkedin.com/in "{company_name}" "{title_kw}"'
        url      = f"https://www.google.com/search?q={requests.utils.quote(query)}"
        resp     = get(url, session)
        if not resp:
            continue

        soup     = BeautifulSoup(resp.text, "html.parser")
        results  = soup.find_all("a")

        found_count = 0
        for a in results:
            href = a.get("href", "")
            if "linkedin.com/in/" in href and "google.com" not in href:
                if "/url?q=" in href:
                    href = href.split("/url?q=")[1].split("&")[0]
                
                # Extract text context for name parsing
                parent_text = a.find_parent("div").get_text(" ", strip=True) if a.find_parent("div") else a.get_text()
                
                name_match = re.search(rf"(.{{4,30}}?)\s*[-–|]\s*{re.escape(title_kw)}", parent_text, re.IGNORECASE)
                name = name_match.group(1).strip() if name_match else a.get_text().split("-")[0].strip()
                
                # Filter out raw URLs masking as names
                if "http" not in name and len(name) > 3:
                    contacts.append({
                        "contact_name": name,
                        "title":        title_kw,
                        "linkedin_url": requests.utils.unquote(href),
                        "cb_person_url": "",
                    })
                    found_count += 1
                    
            if found_count >= 2:
                break
                
        sleep()

    return contacts


# ─────────────────────────────────────────────────────────────────────────────
#  SOURCE 4: APOLLO.IO Public Org Pages
# ─────────────────────────────────────────────────────────────────────────────

def scrape_apollo_contacts(company_name: str, website: str, session) -> list:
    """
    Search Apollo.io's public company pages for contacts.
    Apollo shows partial data without login.
    """
    contacts = []
    domain   = website.replace("https://", "").replace("http://", "").split("/")[0]
    url      = f"https://app.apollo.io/#/companies?q_organization_domains[]={domain}"

    # Apollo is React SPA — try their search API endpoint instead
    api_url  = (
        f"https://api.apollo.io/v1/organizations/search"
        f"?q_organization_name={requests.utils.quote(company_name)}"
        f"&organization_locations[]=europe"
    )
    # Note: Apollo's public search returns limited results without API key
    resp = get(api_url, session)
    if resp and resp.status_code == 200:
        try:
            data  = resp.json()
            orgs  = data.get("organizations", [])
            for org in orgs[:1]:
                for person in org.get("people", [])[:5]:
                    title = person.get("title", "")
                    if any(t.lower() in title.lower() for t in TARGET_TITLES):
                        contacts.append({
                            "contact_name": person.get("name", ""),
                            "title":        title,
                            "linkedin_url": person.get("linkedin_url", ""),
                            "cb_person_url": "",
                            "email_source": "apollo",
                            "email":        person.get("email", ""),
                        })
        except Exception:
            pass

    return contacts


# ─────────────────────────────────────────────────────────────────────────────
#  SELENIUM SCRAPER (used when JS rendering needed)
# ─────────────────────────────────────────────────────────────────────────────

def init_selenium_driver():
    """Launch stealth Chrome driver."""
    if not SELENIUM_AVAILABLE:
        return None
    options = uc.ChromeOptions()
    options.add_argument("--no-sandbox")
    options.add_argument("--disable-blink-features=AutomationControlled")
    options.add_argument("--window-size=1440,900")
    # options.add_argument("--headless")  # Uncomment for headless
    driver = uc.Chrome(options=options)
    driver.implicitly_wait(10)
    return driver


def selenium_scrape_crunchbase(driver, url: str) -> dict:
    """Use Selenium to render Crunchbase pages that need JS."""
    try:
        driver.get(url)
        time.sleep(random.uniform(4, 7))
        # Wait for content
        WebDriverWait(driver, 15).until(
            EC.presence_of_element_located((By.TAG_NAME, "main"))
        )
        # Extract page source after JS render
        source = driver.page_source
        soup   = BeautifulSoup(source, "html.parser")
        script = soup.find("script", {"id": "__NEXT_DATA__"})
        if script:
            return json.loads(script.string)
    except Exception as e:
        print(f"    ⚠️  Selenium error: {e}")
    return {}


# ─────────────────────────────────────────────────────────────────────────────
#  MAIN PIPELINE
# ─────────────────────────────────────────────────────────────────────────────

def build_leads(companies: list, session) -> list:
    """
    For each company, find relevant contacts and build the lead record.
    """
    leads    = load_progress()
    seen_cos = {l["company_name"] for l in leads}

    for i, company in enumerate(companies):
        name = company.get("name", "")
        if not name or name in seen_cos:
            continue

        print(f"\n[{i+1}/{len(companies)}] 🏢 {name} ({company.get('country')})")

        # Find contacts via multiple sources
        contacts = []

        # Source A: YCombinator Native Founder Page
        yc_url = company.get("cb_url") if "ycombinator.com" in company.get("cb_url", "") else ""
        if yc_url:
            resp = get(yc_url, session)
            if resp:
                soup = BeautifulSoup(resp.text, "html.parser")
                seen_yc_names = set()
                # YC lists founders in font-bold utility classes
                for node in soup.find_all(class_="font-bold"):
                    found_name = node.text.strip()
                    if len(found_name.split()) >= 2 and len(found_name) < 30 and "Founder" not in found_name:
                        if found_name not in seen_yc_names:
                            seen_yc_names.add(found_name)
                            contacts.append({
                                "contact_name": found_name,
                                "title": "Founder",
                                "linkedin_url": "",
                                "cb_person_url": yc_url
                            })
        
        # Source B: Crunchbase people tab
        if not contacts:
            cb_people = scrape_crunchbase_people(company, session)
            contacts.extend(cb_people)
            sleep()

        # Source C: Google -> LinkedIn Search
        if not contacts:
            li_people = search_linkedin_contacts(name, session)
            contacts.extend(li_people)

        # Source C: Apollo public data
        if company.get("website"):
            apollo_people = scrape_apollo_contacts(name, company["website"], session)
            contacts.extend(apollo_people)
            sleep()

        # Deduplicate contacts
        seen_contacts = set()
        unique_contacts = []
        for c in contacts:
            key = c.get("linkedin_url") or c.get("contact_name", "")
            if key and key not in seen_contacts:
                seen_contacts.add(key)
                unique_contacts.append(c)

        if not unique_contacts:
            # Still add the company with blank contact fields
            unique_contacts = [{"contact_name": "", "title": "", "linkedin_url": "", "cb_person_url": ""}]

        website = company.get("website", "")
        domain  = website.replace("https://", "").replace("http://", "").split("/")[0] if website else ""

        for contact in unique_contacts:
            # Parse name for email guessing
            full_name = contact.get("contact_name", "")
            parts     = full_name.split()
            first     = parts[0] if parts else ""
            last      = parts[-1] if len(parts) > 1 else ""

            existing_email = contact.get("email", "")
            if existing_email:
                email_guess, confidence = existing_email, "high"
            else:
                email_guess, confidence = guess_email(first, last, domain)

            email_cache = load_email_cache()
            
            # Verify the email
            ver_result = verify_email(email_guess, email_cache) if email_guess else {"valid": False, "reason": "empty", "skip": True, "confidence": "none"}
            
            # Print to terminal
            if email_guess:
                if ver_result["skip"]:
                    print(f"    ❌ invalid   {email_guess} ({ver_result['reason']})")
                elif ver_result["reason"] == "catch_all":
                    print(f"    ⚠️  catch_all {email_guess} ({ver_result['confidence']})")
                elif "fallback" in ver_result["reason"]:
                    print(f"    🔁 fallback  {email_guess} ({ver_result['confidence']})")
                elif ver_result["valid"]:
                    print(f"    ✅ valid     {email_guess} ({ver_result['confidence']})")
            
            lead = {
                "company_name":    name,
                "website":         website,
                "country":         company.get("country", ""),
                "sector":          company.get("sector", ""),
                "employees":       company.get("employees", ""),
                "total_funding_usd": company.get("funding", ""),
                "last_round":      company.get("last_round", ""),
                "description":     company.get("description", ""),
                "crunchbase_url":  company.get("cb_url", ""),
                "contact_name":    full_name,
                "title":           contact.get("title", ""),
                "linkedin_url":    contact.get("linkedin_url", ""),
                "cb_person_url":   contact.get("cb_person_url", ""),
                "email_guess":     email_guess,
                "email_confidence": ver_result["confidence"],
                "email_verified":  "unknown" if "reason" in ver_result and ver_result["reason"] == "catch_all" else ver_result["valid"],
                "email_skip":      ver_result["skip"],
                "email_reason":    ver_result["reason"],
                "scraped_at":      datetime.now().isoformat(),
            }
            leads.append(lead)
            seen_cos.add(name)

        # Save progress instantly so dashboard updates live
        save_progress(leads)

    return leads


def export_csv(leads: list):
    """Write leads to CSV with clean formatting and segregation."""
    if not leads:
        print("No leads to export.")
        return

    df = pd.DataFrame(leads)

    # Clean up
    df = df.drop_duplicates(subset=["company_name", "linkedin_url"])
    df = df.sort_values(["country", "company_name"])

    # Reorder columns for readability — focused on company + person to reach out to
    col_order = [
        "company_name", "contact_name", "title",
        "email_guess", "email_confidence", "email_verified",
        "website", "sector", "country", "employees",
        "description", "last_round", "linkedin_url", "scraped_at",
    ]
    existing_cols = [c for c in col_order if c in df.columns]
    df = df[existing_cols]
    
    # Check cache hits/types for summary
    cache = load_email_cache()
    
    verified_file, rejected_file = get_output_files()
    
    df_verified = df[df["email_skip"] == False]
    df_rejected = df[df["email_skip"] == True]

    df_verified.to_csv(verified_file, index=False)
    df_rejected.to_csv(rejected_file, index=False)
    
    total_ver = len(df_verified)
    valid_ver = len(df_verified[df_verified["email_reason"].isin(["verified", "verified_fallback"])])
    catch_all = len(df_verified[df_verified["email_reason"] == "catch_all"])
    invalids = len(df_rejected)

    print(f"\n✅ Exported verified leads to {verified_file}")
    print(f"✅ Exported rejected leads to {rejected_file}")
    
    print(f"\n📈 VERIFICATION SUMMARY:")
    print(f"    Total verified: {total_ver}")
    print(f"    Valid (high confidence): {valid_ver}")
    print(f"    Catch-all (low confidence): {catch_all}")
    print(f"    Invalid (skipped): {invalids}")
    print(f"    Cache entries total: {len(cache)}")


# ─────────────────────────────────────────────────────────────────────────────
#  BONUS: QUICK-START — Verified Public Sources (no scraping needed)
# ─────────────────────────────────────────────────────────────────────────────

BONUS_SOURCES = """
╔══════════════════════════════════════════════════════════════╗
║  FREE LEAD DATABASES — No scraping required                  ║
╠══════════════════════════════════════════════════════════════╣
║  1. Crunchbase Free (25 exports/month)                       ║
║     crunchbase.com → Search → Export CSV                     ║
║                                                              ║
║  2. EU-Startups.com — directory of European startups         ║
║     eu-startups.com/category/startups                        ║
║                                                              ║
║  3. Dealroom.co — VC-backed European cos, free tier          ║
║     app.dealroom.co                                          ║
║                                                              ║
║  4. LinkedIn Sales Navigator CSV (if you have sub)           ║
║     Best for decision-maker targeting                        ║
║                                                              ║
║  5. Apollo.io Free (50 emails/month)                         ║
║     app.apollo.io → Search → Export                         ║
╚══════════════════════════════════════════════════════════════╝
"""


# ─────────────────────────────────────────────────────────────────────────────
#  ENTRY POINT
# ─────────────────────────────────────────────────────────────────────────────

def main():
    print("=" * 60)
    print("  Applore Technologies — European Lead Scraper")
    print("=" * 60)
    print(BONUS_SOURCES)

    session = requests.Session()
    session.headers.update(HEADERS)

    # ── STEP 1: Discover companies from all sources ─────────────
    print("\n📡 STEP 1: Discovering companies from multiple sources...")
    all_companies = []
    seen_names = set()

    def merge(new_cos, source_label):
        added = 0
        for c in new_cos:
            key = c.get("name", "").strip().lower()
            if key and key not in seen_names:
                seen_names.add(key)
                all_companies.append(c)
                added += 1
        print(f"  ✅ {source_label}: +{added} new companies (total: {len(all_companies)})")

    print("\n  [1/1] YCombinator API...")
    yc_cos = scrape_ycombinator_companies(session, target_count=None)
    merge(yc_cos, "YCombinator")

    companies = all_companies
    print(f"\n  🎯 Total unique companies across all sources: {len(companies)}")

    if not companies:
        print("  ⚠️  All sources returned 0 companies. Check network connectivity.")
        return

    # ── STEP 2: Build contact leads ─────────────────────────────
    print(f"\n👥 STEP 2: Finding contacts for {len(companies)} companies...")
    leads = build_leads(companies, session)

    # ── STEP 3: Export ───────────────────────────────────────────
    print("\n📊 STEP 3: Exporting to CSV...")
    export_csv(leads)

    print("\n🎯 NEXT STEPS:")
    print("  1. Open leads.csv and review quality")
    print("  2. Verify emails with https://hunter.io/email-verifier")
    print("  3. Use leads for LinkedIn outreach + cold email sequences")
    print("  4. Personalise each outreach using the 'description' column")


# ── CLI: load from a Crunchbase CSV export ────────────────────────────────────

def load_from_crunchbase_csv(filepath: str) -> list:
    """
    If you exported a CSV from Crunchbase manually, pass it here.
    Maps Crunchbase column names to our internal format.

    Usage: python applore_lead_scraper.py --from-csv crunchbase_export.csv
    """
    df = pd.read_csv(filepath)
    companies = []
    for _, row in df.iterrows():
        companies.append({
            "name":        row.get("Organization Name", ""),
            "cb_slug":     "",
            "description": row.get("Description", ""),
            "country":     row.get("HQ Location", ""),
            "sector":      row.get("Industries", ""),
            "employees":   row.get("Number of Employees", ""),
            "funding":     row.get("Total Funding Amount", ""),
            "last_round":  row.get("Last Funding Type", ""),
            "website":     row.get("Website", ""),
            "cb_url":      row.get("CB Rank (Company)", ""),
        })
    print(f"  📥 Loaded {len(companies)} companies from {filepath}")
    return companies


if __name__ == "__main__":
    import sys

    if "--from-csv" in sys.argv:
        idx      = sys.argv.index("--from-csv")
        filepath = sys.argv[idx + 1]
        session  = requests.Session()
        session.headers.update(HEADERS)
        companies = load_from_crunchbase_csv(filepath)
        leads     = build_leads(companies, session)
        export_csv(leads)
    else:
        main()
