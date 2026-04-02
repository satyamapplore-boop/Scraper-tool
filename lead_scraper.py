"""
Lead Scraper - With Founders & Exact Location
=============================================
"""

import csv
import json
import os
import time
import re
from datetime import datetime
from pathlib import Path

import requests
from bs4 import BeautifulSoup
from dotenv import load_dotenv

load_dotenv()

OUTPUT_FILE = "leads.csv"
PROGRESS_FILE = "leads_progress.json"

HEADERS = {
    "User-Agent": "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 Chrome/120.0.0.0 Safari/537.36",
    "Accept-Language": "en-US,en;q=0.9",
}

COUNTRY_PATTERNS = [
    (r"USA|US|America|United States", "USA"),
    (r"UK|United Kingdom|England|Scotland|Wales", "United Kingdom"),
    (r"Germany", "Germany"),
    (r"France", "France"),
    (r"Netherlands", "Netherlands"),
    (r"Spain", "Spain"),
    (r"Italy", "Italy"),
    (r"Canada", "Canada"),
    (r"India", "India"),
    (r"China", "China"),
    (r"Japan", "Japan"),
    (r"Australia", "Australia"),
    (r"Brazil", "Brazil"),
    (r"Ireland", "Ireland"),
    (r"Switzerland", "Switzerland"),
    (r"Singapore", "Singapore"),
    (r"Israel", "Israel"),
]

REGION_MAP = {
    "United States": "USA", "USA": "USA",
    "United Kingdom": "Europe", "England": "Europe", "Scotland": "Europe", "Wales": "Europe",
    "Germany": "Europe", "France": "Europe", "Netherlands": "Europe", "Spain": "Europe",
    "Italy": "Europe", "Belgium": "Europe", "Switzerland": "Europe", "Austria": "Europe",
    "Ireland": "Europe", "Portugal": "Europe", "Poland": "Europe", "Sweden": "Europe",
    "Denmark": "Europe", "Norway": "Europe", "Finland": "Europe",
    "Canada": "USA", "Mexico": "USA",
    "India": "Global", "China": "Global", "Japan": "Global", "Singapore": "Global",
    "Australia": "Global", "Brazil": "Global", "Israel": "Global",
}

INDUSTRY_MAP = {
    "manufacturing": "Manufacturing", "industrial": "Manufacturing", "industrials": "Manufacturing",
    "healthcare": "Healthcare", "health": "Healthcare", "medtech": "Healthcare", "biotech": "Healthcare", "pharma": "Healthcare",
    "finance": "Financial Services", "fintech": "Financial Services", "banking": "Financial Services", "insurance": "Financial Services",
    "retail": "Retail & E-commerce", "ecommerce": "Retail & E-commerce", "e-commerce": "Retail & E-commerce", "consumer": "Retail & E-commerce",
    "software": "Information Technology", "saas": "Information Technology", "it": "Information Technology", "tech": "Information Technology",
    "logistics": "Logistics & Supply Chain", "supply chain": "Logistics & Supply Chain", "transportation": "Logistics & Supply Chain",
    "real estate": "Real Estate & Construction", "construction": "Real Estate & Construction", "property": "Real Estate & Construction",
    "energy": "Energy & Utilities", "utilities": "Energy & Utilities", "clean tech": "Energy & Utilities", "sustainability": "Energy & Utilities",
    "education": "Education", "edtech": "Education", "learning": "Education",
    "consumer goods": "Consumer Goods", "fmcg": "Consumer Goods", "food": "Consumer Goods", "beverage": "Consumer Goods",
}

FIXED_INDUSTRIES = [
    "Manufacturing", "Healthcare", "Financial Services", "Retail & E-commerce",
    "Information Technology", "Logistics & Supply Chain", "Real Estate & Construction",
    "Energy & Utilities", "Education", "Consumer Goods"
]

COLUMNS = ["company_name", "contact_name", "title", "email", "email_verified", "website", "country", "region", "Sector", "linkedin_url", "scraped_at"]


def extract_country(location_str):
    if not location_str:
        return ""
    for pattern, country in COUNTRY_PATTERNS:
        if re.search(pattern, location_str, re.IGNORECASE):
            return country
    return ""


def get_region(country):
    if not country:
        return "Global"
    return REGION_MAP.get(country, "Global")


def map_industry(sector_str):
    if not sector_str:
        return ""
    sector_lower = sector_str.lower()
    for key, value in INDUSTRY_MAP.items():
        if key in sector_lower:
            return value
    return ""


def load_leads():
    if Path(PROGRESS_FILE).exists():
        try:
            with open(PROGRESS_FILE, "r", encoding="utf-8") as f:
                return json.load(f)
        except:
            return []
    return []


def save_leads(leads):
    with open(PROGRESS_FILE, "w", encoding="utf-8") as f:
        json.dump(leads, f, indent=2, ensure_ascii=False)


def save_lead(lead):
    leads = load_leads()
    key = (lead.get("company_name", "").lower(), lead.get("website", "").lower())
    existing_idx = None
    
    for i, l in enumerate(leads):
        if l.get("company_name", "").lower() == key[0] and l.get("website", "").lower() == key[1]:
            existing_idx = i
            break
    
    if existing_idx is not None:
        existing = leads[existing_idx]
        for col in COLUMNS:
            if lead.get(col) and not existing.get(col):
                existing[col] = lead[col]
        leads[existing_idx] = existing
    else:
        leads.append(lead)
    
    save_leads(leads)
    return len(leads)


def scrape_ycombinator(session):
    print("\n[YCombinator] Starting...")
    page = 0
    count = 0
    
    while page < 50:
        url = f"https://api.ycombinator.com/v0.1/companies?page={page}"
        try:
            resp = session.get(url, headers=HEADERS, timeout=15)
            if resp.status_code != 200:
                break
            data = resp.json()
            companies = data.get("companies", [])
            if not companies:
                break
                
            for c in companies:
                company_name = c.get("name", "")
                website = c.get("website", "")
                
                locations = c.get("locations", [])
                location = locations[0] if locations else ""
                country = extract_country(location)
                
                regions = c.get("regions", [])
                region = ""
                if "USA" in str(regions) or "America" in str(regions):
                    region = "USA"
                elif "Europe" in str(regions):
                    region = "Europe"
                else:
                    region = "Global"
                
                industries = c.get("industries", [])
                
                lead = {
                    "company_name": company_name,
                    "contact_name": "",
                    "title": "",
                    "email": "",
                    "email_verified": "FALSE",
                    "website": website,
                    "country": country,
                    "region": region,
                    "Sector": map_industry(", ".join(industries)),
                    "linkedin_url": "",
                    "scraped_at": datetime.now().isoformat()
                }
                
                if company_name:
                    save_lead(lead)
                    count += 1
                    if count % 100 == 0:
                        print(f"  YC: {count} companies...")
            
            page += 1
            time.sleep(1)
            
        except Exception as e:
            print(f"  Error: {e}")
            break
    
    print(f"[YCombinator] Got {count} companies with exact locations")
    return count


def scrape_yc_founders(session):
    print("\n[Finding Founders]...")
    leads = load_leads()
    updated = 0
    
    for i, lead in enumerate(leads[:100]):
        if lead.get("contact_name"):
            continue
        
        website = lead.get("website", "")
        if not website:
            continue
        
        try:
            if not website.startswith("http"):
                website = "https://" + website
            
            resp = session.get(website, headers=HEADERS, timeout=8)
            if resp.status_code == 200:
                soup = BeautifulSoup(resp.text, "html.parser")
                
                text_elements = soup.find_all(string=True)
                for text in text_elements:
                    text_str = str(text).strip()
                    if "founder" in text_str.lower():
                        parent = text.parent
                        if parent:
                            name = parent.get_text(strip=True)
                            name = re.sub(r'(Founder|Co-Founder|co-founder|CEO|CTO|CPO)', '', name, flags=re.IGNORECASE).strip()
                            if len(name.split()) >= 2 and 3 < len(name) < 35:
                                if not any(x in name.lower() for x in ["http", "www", ".com", "linkedin", "twitter"]):
                                    leads[i]["contact_name"] = name
                                    leads[i]["title"] = "Founder"
                                    updated += 1
                                    break
                        
        except:
            pass
        
        if i % 10 == 0:
            time.sleep(0.3)
    
    save_leads(leads)
    print(f"  Found {updated} founders")


def export_csv():
    leads = load_leads()
    if not leads:
        print("No leads to export.")
        return
    
    with open(OUTPUT_FILE, "w", newline="", encoding="utf-8") as f:
        writer = csv.DictWriter(f, fieldnames=COLUMNS)
        writer.writeheader()
        for lead in leads:
            row = {col: lead.get(col, "") for col in COLUMNS}
            writer.writerow(row)
    
    print(f"Exported {len(leads)} leads to {OUTPUT_FILE}")


def main():
    print("=" * 60)
    print("Lead Scraper - Exact Location + Founders")
    print("=" * 60)
    
    session = requests.Session()
    
    existing = len(load_leads())
    print(f"Existing: {existing}")
    
    if existing == 0:
        scrape_ycombinator(session)
    else:
        scrape_yc_founders(session)
    
    leads = load_leads()
    with_country = len([l for l in leads if l.get("country")])
    with_founders = len([l for l in leads if l.get("contact_name")])
    
    print(f"\nTotal: {len(leads)}")
    print(f"With country: {with_country}")
    print(f"With founders: {with_founders}")
    
    export_csv()
    print("\nDone!")


if __name__ == "__main__":
    main()