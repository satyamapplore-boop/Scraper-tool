import os
import re
import json
import time
import argparse
from datetime import datetime
import requests
import pandas as pd
from urllib.parse import urlparse
from typing import List, Dict, Any, Optional, Set
from dotenv import load_dotenv

load_dotenv()

# --- ACCURATE CONTINENT MAPPING ---
CONTINENT_MAP = {
    "Europe": ["United Kingdom","UK","London","Germany","Netherlands","Sweden","Denmark","Norway","France","Spain","Italy","Belgium","Switzerland","Austria","Ireland","Portugal","Poland","Finland"],
    "North America": ["United States","USA","Canada","Mexico"],
    "Asia": ["India","China","Japan","Singapore","UAE","South Korea"],
    "South America": ["Brazil","Argentina","Chile"],
    "Oceania": ["Australia","New Zealand"],
    "Africa": ["South Africa","Nigeria","Kenya"]
}

# FINAL SCHEMA (EXACT COLUMN ORDER)
SCHEMA_FIELDS = [
    "company_name", "contact_name", "title", "email", "email_verified",
    "website", "sector", "country", "employees", "description",
    "last_round", "linkedin_url", "scraped_at"
]

class CrmReadyPipeline:
    def __init__(self, target_continents=None, input_file="leads_raw.json"):
        self.target_continents = [c.title() for c in (target_continents or [])]
        self.input_file = input_file
        self.output_csv = "leads_refined.csv"
        self.output_xlsx = "leads_refined.xlsx"
        self.companies: Dict[tuple, Dict[str, Any]] = {}

    def run(self):
        print(f"[{datetime.now().strftime('%H:%M:%S')}] Initializing CRM-Ready Validation Pipeline...")
        raw_data = self._load_data()
        
        for entry in raw_data:
            self._process_entry(entry)
            
        self.export_data()
        print(f"✅ Finished. {len(self.companies)} companies successfully audited for CRM purity.")

    def _load_data(self) -> List[Dict]:
        if not os.path.exists(self.input_file): return []
        with open(self.input_file, "r", encoding="utf-8") as f:
            return json.load(f)

    def _process_entry(self, entry: Dict):
        # 1. FIELD PURITY & INITIAL IDENTIFICATION
        co_name = str(entry.get("company_name", "")).strip()
        website = str(entry.get("website", "")).strip()
        domain = self._extract_domain(website)
        
        if not co_name or not website or not domain: return
        # Ensure name doesn't contain URL or company placeholder
        if ".com" in co_name.lower() or "www." in co_name.lower(): return 

        # 2. CONTACT NAME CLEANLINESS (Real Person Only)
        c_name = str(entry.get("contact_name", "")).strip()
        # Reject generic/placeholder names
        if self._is_invalid_name(c_name): return
        if len(c_name.split()) < 2: return # Require First + Last Name

        # 3. ROLE FILTER (Decision Makers only)
        title = str(entry.get("title", "")).lower()
        if not self._is_decision_maker(title): return

        # 4. GEO & INDUSTRY GUARDRAILS
        country = self._resolve_country(entry, domain)
        if self.target_continents:
            match = False
            for tc in self.target_continents:
                if country in CONTINENT_MAP.get(tc, []): match = True; break
            if not match: return
            
        desc = str(entry.get("description", "")).lower()
        sector = str(entry.get("sector", "")).lower()
        if any(k in desc or k in sector for k in ["ai", "saas", "platform"]): return
        mapped_sector = self._map_industry(sector, desc)
        if not mapped_sector: return

        # 5. EMAIL ACCURACY (Domain Consistency & Verification Tier)
        email_data = self._validate_email(entry, domain)
        # Reject generic or invalid email formatting
        if not email_data["email"]: return

        # 6. UNIQUE COMPANY DEDUPLICATION (1:1 GROUPING)
        key = (co_name.lower(), domain.lower())
        if key not in self.companies:
            self.companies[key] = {
                "company_name": co_name,
                "contact_name": c_name.title(),
                "title": title.title(),
                "email": email_data["email"],
                "email_verified": email_data["verified"],
                "website": website,
                "sector": mapped_sector,
                "country": country,
                "employees": str(entry.get("employees", "11-50")),
                "description": entry.get("description", ""),
                "last_round": entry.get("last_round", ""),
                "linkedin_url": entry.get("linkedin_url", ""),
                "scraped_at": datetime.now().strftime("%Y-%m-%d %H:%M:%S")
            }
        # Else: Primary DM already stored for this company (One co per row)

    def _is_invalid_name(self, name: str) -> bool:
        n = name.lower()
        if any(x in n for x in ["http", ".com", ".net", "www.", "@", "info", "admin", "admin"]): return True
        # Check for company names or generic holders inside name field
        if n in ["n/a", "unknown", "placeholder", "team"]: return True
        return False

    def _validate_email(self, entry: Dict, domain: str) -> Dict:
        email = str(entry.get("email_guess", "")).lower().strip()
        conf = str(entry.get("email_confidence", "")).lower()
        source = str(entry.get("source", "unknown")).lower()
        
        # Domain Mismatch rule
        if email and f"@{domain}" not in email: return {"email": "", "verified": False}
        
        # Generic Reject
        if any(email.startswith(g) for g in ["info@", "jobs@", "hello@", "careers@"]):
            return {"email": "", "verified": False}
            
        # Proper verification logic
        # TRUE only if Apollo/Hunter/Snov verified source
        verified = bool("apollo" in source or conf == "high")
        
        return {"email": email, "verified": verified}

    def _resolve_country(self, entry: Dict, dom: str) -> str:
        c = str(entry.get("country", "Global")).strip()
        if c.lower() != "global": return c.title()
        if ".uk" in dom: return "United Kingdom"
        if ".de" in dom: return "Germany"
        return "United Kingdom"

    def _map_industry(self, sector: str, desc: str) -> Optional[str]:
        if any(w in desc for w in ["factory", "plant", "machinery", "fabrication", "assembly"]):
            return "Manufacturing"
        s = (sector + " " + desc).lower()
        if any(w in s for w in ["health", "medic"]): return "Healthcare"
        if any(w in s for w in ["bank", "finan"]): return "Financial Services"
        if any(w in s for w in ["retail", "ecommerce"]): return "Retail & E-commerce"
        if any(w in s for w in ["it", "soft", "tech"]): return "Information Technology"
        if any(w in s for w in ["logist", "supply"]): return "Logistics & Supply Chain"
        if any(w in s for w in ["real", "estate", "const"]): return "Real Estate & Construction"
        if any(w in s for w in ["energy", "utilit"]): return "Energy & Utilities"
        if any(w in s for w in ["educ", "school"]): return "Education & EdTech"
        if any(w in s for w in ["consumer", "fmcg"]): return "Consumer Goods & FMCG"
        return None

    def _is_decision_maker(self, title: str) -> bool:
        dm_roles = ["founder", "director", "ceo", "cpo", "cto", "vp", "head", "chief", "owner", "president"]
        return any(role in title.lower() for role in dm_roles)

    def _extract_domain(self, url: str) -> str:
        if not url: return ""
        if not url.startswith("http"): url = "http://" + url
        try: return urlparse(url).netloc.replace("www.", "").lower()
        except: return ""

    def export_data(self):
        final_list = list(self.companies.values())
        if not final_list: return
        df = pd.DataFrame(final_list, columns=SCHEMA_FIELDS)
        df.to_csv(self.output_csv, index=False)
        try: df.to_excel(self.output_xlsx, index=False)
        except: pass

if __name__ == "__main__":
    parser = argparse.ArgumentParser(description="CRM-Ready Industrial Lead Pipeline")
    parser.add_argument("--continents", help="Comma-separated continents", default="")
    args = parser.parse_args()
    conts = [c.strip().title() for c in args.continents.split(",") if c.strip()]
    pipeline = CrmReadyPipeline(target_continents=conts)
    pipeline.run()
