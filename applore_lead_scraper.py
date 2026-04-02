"""
Applore Technologies — European Lead Generation Scraper (Enhanced)
==================================================================
Improvements: Config management, logging, retry logic, error handling, caching

Targets: Mid-market (100-1000 employees) + VC-backed healthtech/fintech/B2B SaaS
Countries: UK, Netherlands, Germany, Sweden, Denmark, Norway, France, Spain

Sources used (no paid login required):
  1. YCombinator API  → company list (most reliable)
  2. LinkedIn public search  → decision-maker profiles
  3. Apollo.io public org pages  → contact email hints

OUTPUT: leads_verified_YYYY-MM-DD.csv, leads_rejected_YYYY-MM-DD.csv

SETUP (run once):
  pip install requests beautifulsoup4 selenium undetected-chromedriver \
              pandas tqdm fake-useragent python-dotenv pyyaml dnspython

USAGE:
  python applore_lead_scraper.py

  The scraper is intentionally slow (random delays) to avoid blocks.
  A full run across all countries/sectors takes ~2-3 hours.
  You can interrupt at any time — progress is saved to leads_raw.json.

CONFIGURATION:
  Edit config.yaml to customize target countries, sectors, delays, etc.
"""

import csv
import json
import os
import random
import re
import sys
import time
from datetime import datetime, timedelta
from pathlib import Path

import pandas as pd
import requests
from bs4 import BeautifulSoup
from dotenv import load_dotenv

load_dotenv()

from utils import Config, ScraperLogger, get_timestamp, ensure_output_dir

logger = ScraperLogger("applore_scraper")
config = Config()

try:
    import dns.resolver
    DNS_AVAILABLE = True
except ImportError:
    DNS_AVAILABLE = False
    logger.warning("dnspython not found. MX checks disabled.")

try:
    import undetected_chromedriver as uc
    from selenium.webdriver.common.by import By
    from selenium.webdriver.support import expected_conditions as EC
    from selenium.webdriver.support.ui import WebDriverWait
    SELENIUM_AVAILABLE = True
except ImportError:
    SELENIUM_AVAILABLE = False
    logger.warning("undetected-chromedriver not found. Selenium scraping disabled.")

OUTPUT_FILE = "leads.csv"
PROGRESS_FILE = "leads_raw.json"
CACHE_FILE = "email_cache.json"

DELAY_MIN = config.get('delay_min', 1.0)
DELAY_MAX = config.get('delay_max', 2.5)
MAX_PAGES = config.get('max_pages', 5)
MAX_RETRIES = config.get('max_retries', 3)
RETRY_BACKOFF = config.get('retry_backoff', 1.5)
TIMEOUT = config.get('timeout', 15)

EMAILVALIDATR_API_KEY = os.getenv("EMAILVALIDATR_API_KEY", "")
global_use_fallback = False

def get_output_files():
    date_str = datetime.now().strftime("%Y-%m-%d")
    return f"leads_verified_{date_str}.csv", f"leads_rejected_{date_str}.csv"

TARGET_COUNTRIES = {
    "United Kingdom": "united-kingdom",
    "Netherlands": "netherlands",
    "Germany": "germany",
    "Sweden": "sweden",
    "Denmark": "denmark",
    "Norway": "norway",
    "France": "france",
    "Spain": "spain",
}

TARGET_SECTORS = config.get('target_sectors', [
    "health-care", "financial-services", "fin-tech", "health-tech",
    "software", "saas", "enterprise-software", "information-technology"
])

NON_TECH_NATIVE_SECTORS = config.get('non_tech_native_sectors', [
    "manufacturing", "industrials", "retail", "logistics",
    "supply-chain", "real-estate", "construction", "agriculture",
    "legal", "education", "media", "publishing", "hospitality",
    "food-and-beverage", "energy", "transportation", "healthcare",
    "professional-services", "accounting", "insurance"
])

PURE_TECH_EXCLUDE_KEYWORDS = [
    "developer tools", "devtools", "infrastructure", "developer platform",
    "api", "cloud infrastructure", "cybersecurity", "data infrastructure",
    "open source", "mlops", "llm", "ai platform", "no-code", "low-code"
]

TARGET_TITLES = config.get('target_titles', [
    "CTO", "Chief Technology Officer", "CPO", "Chief Product Officer",
    "VP Engineering", "VP of Engineering", "Head of Engineering",
    "Head of Product", "VP Product", "Co-Founder", "CEO", "Founder",
    "President", "Managing Director", "Chief Executive Officer", "COO",
    "Chief Operating Officer", "CMO", "Chief Marketing Officer",
    "VP Sales", "Head of Sales", "VP Business Development",
    "Director of Engineering"
])

LINKEDIN_SEARCH_TITLES = ["Founder", "CEO", "CTO", "VP Sales", "CMO", "President"]

EMPLOYEE_RANGE = config.get('employee_range', "c_1_10,c_11_50,c_51_100,c_101_250,c_251_500")

HEADERS = {
    "User-Agent": "Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/122.0.0.0 Safari/537.36",
    "Accept-Language": "en-US,en;q=0.9",
    "Accept": "text/html,application/xhtml+xml,application/xml;q=0.9,*/*;q=0.8"
}

def sleep():
    t = random.uniform(DELAY_MIN, DELAY_MAX)
    logger.debug(f"sleeping {t:.1f}s...")
    time.sleep(t)

def get(url, session=None, retries=MAX_RETRIES):
    requester = session or requests
    for attempt in range(retries):
        try:
            resp = requester.get(url, headers=HEADERS, timeout=TIMEOUT)
            if resp.status_code == 200:
                return resp
            if resp.status_code in [403, 404]:
                logger.warning(f"HTTP {resp.status_code} for {url[:100]}... (Aborting retries)")
                return None
            if resp.status_code == 429:
                wait = 30 * (attempt + 1)
                logger.warning(f"Rate limited. Waiting {wait}s...")
                time.sleep(wait)
            else:
                logger.warning(f"HTTP {resp.status_code} for {url[:100]}...")
        except requests.exceptions.Timeout:
            logger.error(f"Timeout for {url[:100]}...")
        except requests.exceptions.RequestException as e:
            logger.error(f"Request error: {e}")
        
        backoff = RETRY_BACKOFF * (attempt + 1)
        logger.info(f"Retry {attempt + 1}/{retries} after {backoff:.1f}s...")
        time.sleep(backoff)
    return None

def save_progress(data: list):
    try:
        with open(PROGRESS_FILE, "w", encoding="utf-8") as f:
            json.dump(data, f, indent=2, ensure_ascii=False)
        logger.debug(f"Progress saved ({len(data)} records)")
    except Exception as e:
        logger.error(f"Failed to save progress: {e}")

def load_progress() -> list:
    if Path(PROGRESS_FILE).exists():
        try:
            with open(PROGRESS_FILE, "r", encoding="utf-8") as f:
                data = json.load(f)
            logger.info(f"Resuming from {len(data)} saved records")
            return data
        except json.JSONDecodeError as e:
            logger.error(f"Failed to load progress: {e}")
    return []

def load_email_cache() -> dict:
    if Path(CACHE_FILE).exists():
        try:
            with open(CACHE_FILE, "r", encoding="utf-8") as f:
                return json.load(f)
        except (json.JSONDecodeError, IOError):
            return {}
    return {}

def save_email_cache(cache: dict):
    try:
        with open(CACHE_FILE, "w", encoding="utf-8") as f:
            json.dump(cache, f, indent=2)
    except IOError as e:
        logger.error(f"Failed to save email cache: {e}")

def guess_email(first: str, last: str, domain: str) -> tuple[str, str]:
    if not all([first, last, domain]):
        return "", "low"

    first = first.lower().strip()
    last = last.lower().strip()
    domain = domain.lower().strip().lstrip("www.").split("/")[0]

    patterns = [
        f"{first}.{last}@{domain}",
        f"{first[0]}{last}@{domain}",
        f"{first}@{domain}",
        f"{first}{last}@{domain}",
        f"{last}.{first}@{domain}",
    ]
    return patterns[0], "medium"

def verify_email(email: str, cache: dict) -> dict:
    global global_use_fallback
    
    if not email:
        return {"valid": False, "reason": "empty", "skip": True, "confidence": "none"}
    
    use_cache = config.get('email_verification.use_cache', True)
    cache_ttl = config.get('email_verification.cache_ttl_days', 7)
    
    if use_cache and email in cache:
        entry = cache[email]
        cached_time = datetime.fromisoformat(entry.get("timestamp", "2000-01-01T00:00:00"))
        if datetime.now() - cached_time < timedelta(days=cache_ttl):
            logger.debug(f"Cache hit for {email}")
            return entry.get("result")
    
    result = _do_verify_email(email)
    cache[email] = {
        "result": result,
        "timestamp": datetime.now().isoformat()
    }
    save_email_cache(cache)
    return result

def _do_verify_email(email: str) -> dict:
    global global_use_fallback
    
    pattern = re.compile(r"^[a-zA-Z0-9._%+-]+@[a-zA-Z0-9.-]+\.[a-zA-Z]{2,}$")
    if not pattern.match(email):
        return {"valid": False, "reason": "invalid_syntax", "skip": True, "confidence": "low"}
    
    domain = email.split("@")[1]
    
    if DNS_AVAILABLE:
        try:
            try:
                dns.resolver.resolve(domain, 'MX')
            except (dns.resolver.NoAnswer, dns.resolver.NXDOMAIN, dns.exception.Timeout):
                return {"valid": False, "reason": "no_mx", "skip": True, "confidence": "high"}
        except Exception:
            pass
    
    deep_verify = config.get('email_verification.deep_verify', True)
    
    if not global_use_fallback and deep_verify and EMAILVALIDATR_API_KEY:
        time.sleep(1)
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
        except requests.exceptions.RequestException as e:
            logger.warning(f"EmailValidatr API error: {e}")
    
    time.sleep(1)
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
    except requests.exceptions.RequestException as e:
        logger.warning(f"Fallback API error: {e}")
    
    return {"valid": True, "reason": "unverified", "confidence": "low", "skip": False}

def build_crunchbase_url(country_slug: str, sector_slug: str, page: int = 1) -> str:
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
    companies = []
    seen_slugs = set()

    for country_name, country_slug in TARGET_COUNTRIES.items():
        for sector_slug in TARGET_SECTORS:
            logger.info(f"Crunchbase: {country_name} / {sector_slug}")
            for page in range(1, MAX_PAGES + 1):
                url = build_crunchbase_url(country_slug, sector_slug, page)
                resp = get(url, session)
                if not resp:
                    break

                soup = BeautifulSoup(resp.text, "html.parser")
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
                            slug = props.get("identifier", {}).get("permalink", "")
                            if slug and slug not in seen_slugs:
                                seen_slugs.add(slug)
                                companies.append({
                                    "name": props.get("identifier", {}).get("value", ""),
                                    "cb_slug": slug,
                                    "description": props.get("short_description", ""),
                                    "country": country_name,
                                    "sector": sector_slug,
                                    "employees": props.get("num_employees_enum", ""),
                                    "funding": props.get("funding_total", {}).get("value_usd", ""),
                                    "last_round": props.get("last_funding_type", ""),
                                    "website": props.get("website_url", ""),
                                    "cb_url": f"https://www.crunchbase.com/organization/{slug}",
                                })
                        logger.debug(f"Page {page}: {len(entities)} companies found")
                        if len(entities) < 25:
                            break
                    except (json.JSONDecodeError, KeyError) as e:
                        logger.warning(f"JSON parse error: {e}")

                sleep()

    logger.info(f"Total companies discovered: {len(companies)}")
    return companies

def scrape_ycombinator_companies(session, target_count=None) -> list:
    companies = []
    seen_slugs = set()
    page = 0
    target_count = target_count or 999999
    
    logger.info("YCombinator: Extracting startups...")
    
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
                
                companies.append({
                    "name": c.get("name", ""),
                    "cb_slug": slug,
                    "description": c.get("oneLiner", ""),
                    "country": c.get("location", "") or "Global",
                    "sector": ", ".join(industries_list),
                    "employees": str(c.get("teamSize", "")),
                    "funding": "YC Backed",
                    "last_round": c.get("batch", ""),
                    "website": c.get("website", ""),
                    "cb_url": c.get("url", ""),
                })
                
                if len(companies) >= target_count:
                    break
                        
            logger.debug(f"Page {page}: {len(companies)}/{target_count}")
            page += 1
            sleep()
            
        except (json.JSONDecodeError, KeyError) as e:
            logger.warning(f"JSON parse error on YC: {e}")
            break
            
    logger.info(f"Total YC companies: {len(companies)}")
    return companies

def scrape_eu_startups(session) -> list:
    companies = []
    seen = set()
    base_url = "https://www.eu-startups.com/directory/"

    eu_startup_sectors = [
        "manufacturing", "health", "logistics", "retail", "energy",
        "agriculture", "construction", "legal", "education", "food",
        "real-estate", "transport", "insurance", "media",
    ]

    logger.info("EU-Startups: Scraping directory...")

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
                    "name": name,
                    "cb_slug": link.rstrip("/").split("/")[-1],
                    "description": desc,
                    "country": country,
                    "sector": sector.title(),
                    "employees": "",
                    "funding": "",
                    "last_round": "",
                    "website": link,
                    "cb_url": link,
                })

            logger.debug(f"{sector} page {page}: {len(companies)} total")
            if len(listings) < 10:
                break
            page += 1
            sleep()

    logger.info(f"Total EU-Startups: {len(companies)}")
    return companies

def scrape_f6s(session) -> list:
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

    logger.info("F6S: Scraping startup directory...")

    for country in target_countries_f6s:
        for sector in f6s_sectors[:5]:
            url = f"https://www.f6s.com/companies/{sector}?country={country}&sort=founded"
            resp = get(url, session)
            if not resp:
                continue

            soup = BeautifulSoup(resp.text, "html.parser")
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
                    "name": name,
                    "cb_slug": name.lower().replace(" ", "-"),
                    "description": desc,
                    "country": country.replace("-", " ").title(),
                    "sector": sector.replace("-", " ").title(),
                    "employees": "",
                    "funding": "",
                    "last_round": "",
                    "website": link,
                    "cb_url": link,
                })

            if cards:
                logger.debug(f"{country}/{sector}: {len(cards)} companies")
            sleep()

    logger.info(f"Total F6S companies: {len(companies)}")
    return companies

def scrape_crunchbase_people(company: dict, session) -> list:
    slug = company.get("cb_slug", "")
    if not slug:
        return []

    url = f"https://www.crunchbase.com/organization/{slug}/people"
    resp = get(url, session)
    if not resp:
        return []

    contacts = []
    soup = BeautifulSoup(resp.text, "html.parser")
    script = soup.find("script", {"id": "__NEXT_DATA__"})
    if not script:
        return []

    try:
        data = json.loads(script.string)
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

        for person in people_data:
            props = person.get("properties", {}) or person
            title = props.get("title", "") or props.get("job_type", "")
            name = props.get("name", "") or (
                props.get("identifier", {}) or {}
            ).get("value", "")

            if any(t.lower() in title.lower() for t in TARGET_TITLES):
                li_url = props.get("linkedin", "") or ""
                contacts.append({
                    "contact_name": name,
                    "title": title,
                    "linkedin_url": li_url,
                    "cb_person_url": f"https://www.crunchbase.com/person/{props.get('identifier', {}).get('permalink', '')}",
                })

    except (json.JSONDecodeError, KeyError):
        pass

    return contacts

def search_linkedin_contacts(company_name: str, session) -> list:
    contacts = []
    for title_kw in LINKEDIN_SEARCH_TITLES:
        query = f'site:linkedin.com/in "{company_name}" "{title_kw}"'
        url = f"https://www.google.com/search?q={requests.utils.quote(query)}"
        resp = get(url, session)
        if not resp:
            continue

        soup = BeautifulSoup(resp.text, "html.parser")
        results = soup.find_all("a")

        found_count = 0
        for a in results:
            href = a.get("href", "")
            if "linkedin.com/in/" in href and "google.com" not in href:
                if "/url?q=" in href:
                    href = href.split("/url?q=")[1].split("&")[0]
                
                parent_text = a.find_parent("div").get_text(" ", strip=True) if a.find_parent("div") else a.get_text()
                
                name_match = re.search(rf"(.{{4,30}}?)\s*[-–|]\s*{re.escape(title_kw)}", parent_text, re.IGNORECASE)
                name = name_match.group(1).strip() if name_match else a.get_text().split("-")[0].strip()
                
                if "http" not in name and len(name) > 3:
                    contacts.append({
                        "contact_name": name,
                        "title": title_kw,
                        "linkedin_url": requests.utils.unquote(href),
                        "cb_person_url": "",
                    })
                    found_count += 1
                    
            if found_count >= 2:
                break
                
        sleep()

    return contacts

def scrape_apollo_contacts(company_name: str, website: str, session) -> list:
    contacts = []
    domain = website.replace("https://", "").replace("http://", "").split("/")[0]
    
    api_url = (
        f"https://api.apollo.io/v1/organizations/search"
        f"?q_organization_name={requests.utils.quote(company_name)}"
        f"&organization_locations[]=europe"
    )
    resp = get(api_url, session)
    if resp and resp.status_code == 200:
        try:
            data = resp.json()
            orgs = data.get("organizations", [])
            for org in orgs[:1]:
                for person in org.get("people", [])[:5]:
                    title = person.get("title", "")
                    if any(t.lower() in title.lower() for t in TARGET_TITLES):
                        contacts.append({
                            "contact_name": person.get("name", ""),
                            "title": title,
                            "linkedin_url": person.get("linkedin_url", ""),
                            "cb_person_url": "",
                            "email_source": "apollo",
                            "email": person.get("email", ""),
                        })
        except Exception as e:
            logger.warning(f"Apollo API error: {e}")

    return contacts

def init_selenium_driver():
    if not SELENIUM_AVAILABLE:
        return None
    try:
        options = uc.ChromeOptions()
        options.add_argument("--no-sandbox")
        options.add_argument("--disable-blink-features=AutomationControlled")
        options.add_argument("--window-size=1440,900")
        driver = uc.Chrome(options=options)
        driver.implicitly_wait(10)
        return driver
    except Exception as e:
        logger.error(f"Selenium init failed: {e}")
        return None

def build_leads(companies: list, session) -> list:
    leads = load_progress()
    seen_cos = {l["company_name"] for l in leads}

    for i, company in enumerate(companies):
        name = company.get("name", "")
        if not name or name in seen_cos:
            continue

        logger.info(f"[{i+1}/{len(companies)}] {name} ({company.get('country')})")

        contacts = []

        yc_url = company.get("cb_url") if "ycombinator.com" in company.get("cb_url", "") else ""
        if yc_url:
            resp = get(yc_url, session)
            if resp:
                soup = BeautifulSoup(resp.text, "html.parser")
                seen_yc_names = set()
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
        
        if not contacts:
            cb_people = scrape_crunchbase_people(company, session)
            contacts.extend(cb_people)
            sleep()

        if not contacts:
            li_people = search_linkedin_contacts(name, session)
            contacts.extend(li_people)

        if company.get("website"):
            apollo_people = scrape_apollo_contacts(name, company["website"], session)
            contacts.extend(apollo_people)
            sleep()

        seen_contacts = set()
        unique_contacts = []
        for c in contacts:
            key = c.get("linkedin_url") or c.get("contact_name", "")
            if key and key not in seen_contacts:
                seen_contacts.add(key)
                unique_contacts.append(c)

        if not unique_contacts:
            unique_contacts = [{"contact_name": "", "title": "", "linkedin_url": "", "cb_person_url": ""}]

        website = company.get("website", "")
        domain = website.replace("https://", "").replace("http://", "").split("/")[0] if website else ""

        for contact in unique_contacts:
            full_name = contact.get("contact_name", "")
            parts = full_name.split()
            first = parts[0] if parts else ""
            last = parts[-1] if len(parts) > 1 else ""

            existing_email = contact.get("email", "")
            if existing_email:
                email_guess, confidence = existing_email, "high"
            else:
                email_guess, confidence = guess_email(first, last, domain)

            email_cache = load_email_cache()
            ver_result = verify_email(email_guess, email_cache) if email_guess else {"valid": False, "reason": "empty", "skip": True, "confidence": "none"}
            
            if email_guess:
                if ver_result["skip"]:
                    logger.debug(f"Invalid {email_guess} ({ver_result['reason']})")
                elif ver_result["valid"]:
                    logger.debug(f"Valid {email_guess} ({ver_result['confidence']})")
            
            lead = {
                "company_name": name,
                "website": website,
                "country": company.get("country", ""),
                "sector": company.get("sector", ""),
                "employees": company.get("employees", ""),
                "total_funding_usd": company.get("funding", ""),
                "last_round": company.get("last_round", ""),
                "description": company.get("description", ""),
                "crunchbase_url": company.get("cb_url", ""),
                "contact_name": full_name,
                "title": contact.get("title", ""),
                "linkedin_url": contact.get("linkedin_url", ""),
                "cb_person_url": contact.get("cb_person_url", ""),
                "email_guess": email_guess,
                "email_confidence": ver_result["confidence"],
                "email_verified": "unknown" if "reason" in ver_result and ver_result["reason"] == "catch_all" else ver_result["valid"],
                "email_skip": ver_result["skip"],
                "email_reason": ver_result["reason"],
                "scraped_at": datetime.now().isoformat(),
            }
            leads.append(lead)
            seen_cos.add(name)

        save_progress(leads)

    return leads

def export_csv(leads: list):
    if not leads:
        logger.warning("No leads to export.")
        return

    df = pd.DataFrame(leads)
    df = df.drop_duplicates(subset=["company_name", "linkedin_url"])
    df = df.sort_values(["country", "company_name"])

    col_order = [
        "company_name", "contact_name", "title",
        "email_guess", "email_confidence", "email_verified",
        "website", "sector", "country", "employees",
        "description", "last_round", "linkedin_url", "scraped_at",
    ]
    existing_cols = [c for c in col_order if c in df.columns]
    df = df[existing_cols]
    
    verified_file, rejected_file = get_output_files()
    
    df_verified = df[df["email_skip"] == False]
    df_rejected = df[df["email_skip"] == True]

    df_verified.to_csv(verified_file, index=False)
    df_rejected.to_csv(rejected_file, index=False)
    
    total_ver = len(df_verified)
    valid_ver = len(df_verified[df_verified["email_reason"].isin(["verified", "verified_fallback"])])
    catch_all = len(df_verified[df_verified["email_reason"] == "catch_all"])
    invalids = len(df_rejected)

    logger.info(f"Exported verified leads to {verified_file}")
    logger.info(f"Exported rejected leads to {rejected_file}")
    
    logger.info(f"Total verified: {total_ver}")
    logger.info(f"Valid (high confidence): {valid_ver}")
    logger.info(f"Catch-all (low confidence): {catch_all}")
    logger.info(f"Invalid (skipped): {invalids}")

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

def main():
    logger.info("=" * 60)
    logger.info("Applore Technologies — European Lead Scraper (Enhanced)")
    logger.info("=" * 60)
    logger.info(BONUS_SOURCES)

    session = requests.Session()
    session.headers.update(HEADERS)

    logger.info("STEP 1: Discovering companies from multiple sources...")
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
        logger.info(f"{source_label}: +{added} new companies (total: {len(all_companies)})")

    logger.info("YCombinator API...")
    yc_cos = scrape_ycombinator_companies(session, target_count=None)
    merge(yc_cos, "YCombinator")

    companies = all_companies
    logger.info(f"Total unique companies: {len(companies)}")

    if not companies:
        logger.warning("All sources returned 0 companies. Check network connectivity.")
        return

    logger.info(f"STEP 2: Finding contacts for {len(companies)} companies...")
    leads = build_leads(companies, session)

    logger.info("STEP 3: Exporting to CSV...")
    export_csv(leads)

    logger.info("NEXT STEPS:")
    logger.info("1. Open leads.csv and review quality")
    logger.info("2. Verify emails with https://hunter.io/email-verifier")
    logger.info("3. Use leads for LinkedIn outreach + cold email sequences")

def load_from_crunchbase_csv(filepath: str) -> list:
    df = pd.read_csv(filepath)
    companies = []
    for _, row in df.iterrows():
        companies.append({
            "name": row.get("Organization Name", ""),
            "cb_slug": "",
            "description": row.get("Description", ""),
            "country": row.get("HQ Location", ""),
            "sector": row.get("Industries", ""),
            "employees": row.get("Number of Employees", ""),
            "funding": row.get("Total Funding Amount", ""),
            "last_round": row.get("Last Funding Type", ""),
            "website": row.get("Website", ""),
            "cb_url": row.get("CB Rank (Company)", ""),
        })
    logger.info(f"Loaded {len(companies)} companies from {filepath}")
    return companies


if __name__ == "__main__":
    if "--from-csv" in sys.argv:
        idx = sys.argv.index("--from-csv")
        filepath = sys.argv[idx + 1]
        session = requests.Session()
        session.headers.update(HEADERS)
        companies = load_from_crunchbase_csv(filepath)
        leads = build_leads(companies, session)
        export_csv(leads)
    else:
        main()