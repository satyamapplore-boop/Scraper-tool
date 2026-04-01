import csv
import json
import random
import time
import re
import hashlib
import argparse
import traceback
from datetime import datetime, timedelta, timezone

import requests
from bs4 import BeautifulSoup

# --- CONFIGURATION ---

SIGNALS_FILE = "signals.json"
OUTPUT_FILE = "leads_signals.csv"
USER_AGENT = "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/120.0.0.0 Safari/537.36"

# Signal point values
SCORES = {
    "new_cto_cpo": 40,
    "series_a_b_raised": 35,
    "new_cdo": 30,
    "digital_transformation": 25,
    "dev_jobs_posted": 20,
    "ma_announced": 15
}

# Outreach Templates
TEMPLATES = {
    "new_cto_cpo": "Subject: Congrats on the new role — quick thought on tech delivery\nHi {name}, saw the announcement about your new role at {company}. New tech leaders often want to move fast in the first 90 days. We're Applore — an Indian OPD firm that's shipped 500+ products for companies like ABB and Wipro. Worth a 20-min call? applore.in",
    "series_a_b_raised": "Subject: Scaling your product post-funding — Applore\nHi {name}, congrats on the funding round at {company}. When teams raise Series A/B the pressure to ship fast is real. We're Applore — full-stack OPD partner for European SaaS and healthtech companies. Open to a quick call? applore.in",
    "dev_jobs_posted": "Subject: Alternative to hiring a full dev team — Applore\nHi {name}, noticed {company} is hiring engineers. Many teams find it faster and cheaper to partner with an OPD firm. We handle full-stack delivery — AI/ML, mobile, SaaS. Worth a chat? applore.in",
    "digital_transformation": "Subject: Digital transformation partner — Applore\nHi {name}, saw the news about {company}'s transformation initiative. We specialise in helping legacy enterprises modernise with bespoke digital products. Clients like JK Tyres and Kohler have used us as a delivery partner. Worth a brief call? applore.in",
    "ma_announced": "Subject: Post-acquisition tech integration — Applore\nHi {name}, saw the news about {company}'s acquisition. M&A creates pressure to integrate and modernise tech stacks quickly. We're Applore — full-stack OPD partner. Happy to share how we've helped similar teams. applore.in"
}

# --- HTTP UTILS ---

def sleep_random():
    time.sleep(random.uniform(3.5, 8.0))

def safe_get(url):
    sleep_random()
    headers = {"User-Agent": USER_AGENT}
    try:
        resp = requests.get(url, headers=headers, timeout=15)
        if resp.status_code == 429:
            print(f"⚠️  HTTP 429 on {url}. Waiting 60s...")
            time.sleep(60)
            resp = requests.get(url, headers=headers, timeout=15)
            
        if resp.status_code == 200:
            return resp.content
        else:
            print(f"⚠️ HTTP {resp.status_code} for {url[:80]}")
            return None
    except Exception as e:
        print(f"❌ HTTP Error for {url[:80]}: {e}")
        return None

# --- STATE MANAGEMENT ---

def load_signals():
    try:
        with open(SIGNALS_FILE, "r") as f:
            data = json.load(f)
            # Filter to last 30 days
            cutoff = datetime.now(timezone.utc) - timedelta(days=30)
            valid = []
            for s in data:
                try:
                    dt = datetime.fromisoformat(s.get("detected_at", ""))
                    if dt > cutoff:
                        valid.append(s)
                except:
                    pass
            return valid
    except (FileNotFoundError, json.JSONDecodeError):
        return []

def save_signals(signals):
    with open(SIGNALS_FILE, "w") as f:
        json.dump(signals, f, indent=2)

def generate_uid(source_url, company_name):
    raw = f"{source_url}{company_name}".encode("utf-8")
    return hashlib.md5(raw).hexdigest()[:10]

# --- PARSERS ---

def parse_google_news():
    print("\\n📡 SOURCE 1: Google News RSS")
    queries = [
        '"new CTO" OR "appointed CTO" Europe technology',
        '"new CPO" OR "Chief Product Officer" startup Europe',
        '"Series A" OR "Series B" Europe software 2025',
        '"digital transformation" Europe enterprise',
        '"raises" million Europe SaaS OR healthtech OR fintech',
        '"acquires" OR "merger" technology Europe 2025',
        '"Chief Digital Officer" appointed Europe'
    ]
    
    new_signals = []
    for q in queries:
        url = f"https://news.google.com/rss/search?q={requests.utils.quote(q)}&hl=en-GB"
        content = safe_get(url)
        if not content: continue
        
        soup = BeautifulSoup(content, "html.parser")
        for item in soup.find_all("item")[:15]:
            title = item.title.text if item.title else ""
            link = item.link.text if item.link else ""
            desc = item.description.text if item.description else ""
            full_text = title + " " + desc
            
            # Determine signal type
            stype = None
            if "CTO" in full_text or "Chief Product Officer" in full_text or "CPO" in full_text:
                stype = "new_cto_cpo"
            elif "Chief Digital Officer" in full_text:
                stype = "new_cdo"
            elif "Series A" in full_text or "Series B" in full_text or "raises" in full_text.lower():
                stype = "series_a_b_raised"
            elif "digital transformation" in full_text.lower():
                stype = "digital_transformation"
            elif "acquire" in full_text.lower() or "merger" in full_text.lower():
                stype = "ma_announced"
                
            if not stype: continue
            
            # Extract company name
            company = ""
            if " raises " in title:
                company = title.split(" raises ")[0].strip()
            elif " secures " in title:
                company = title.split(" secures ")[0].strip()
            elif " closes " in title:
                company = title.split(" closes ")[0].strip()
            elif " appoints " in title.lower():
                company = title.lower().split(" appoints ")[0].strip().title()
            
            # Fallback company extraction (before hyphen)
            if not company and "-" in title:
                company = title.split("-")[0].strip()
                
            if company and len(company) > 2:
                new_signals.append({
                    "company": company.lower(),
                    "company_raw": company,
                    "signal_type": stype,
                    "source_url": link,
                    "headline": title,
                    "contact_name": "",
                    "contact_title": ""
                })
    return new_signals

def parse_ddg_jobs():
    print("\\n📡 SOURCE 2: LinkedIn Jobs via DDG")
    queries = [
        'site:linkedin.com/jobs "React Native" OR "Flutter" Europe',
        'site:linkedin.com/jobs "machine learning engineer" Europe startup',
        'site:linkedin.com/jobs "VP Engineering" Europe "Series A" OR "Series B"',
        'site:linkedin.com/jobs "Head of Product" Europe SaaS'
    ]
    
    new_signals = []
    for q in queries:
        url = f"https://html.duckduckgo.com/html/?q={requests.utils.quote(q)}"
        content = safe_get(url)
        if not content: continue
        
        soup = BeautifulSoup(content, "html.parser")
        for res in soup.select(".result"):
            a = res.select_one(".result__url")
            title_el = res.select_one(".result__title")
            if not a or not title_el: continue
            
            link = a.get("href", "")
            if "linkedin.com" not in link: continue
            
            title = title_el.text.strip()
            if " at " in title:
                company = title.split(" at ")[1].split("-")[0].strip()
                if company:
                    new_signals.append({
                        "company": company.lower(),
                        "company_raw": company,
                        "signal_type": "dev_jobs_posted",
                        "source_url": link,
                        "headline": title,
                        "contact_name": "",
                        "contact_title": ""
                    })
    return new_signals

def parse_ddg_leadership():
    print("\\n📡 SOURCE 3: New Leadership via DDG")
    queries = [
        '"joins as CTO" OR "appointed CTO" Europe startup 2025',
        '"new Chief Product Officer" Europe software company',
        '"appointed Chief Digital Officer" Europe enterprise'
    ]
    
    new_signals = []
    for q in queries:
        url = f"https://html.duckduckgo.com/html/?q={requests.utils.quote(q)}"
        content = safe_get(url)
        if not content: continue
        
        soup = BeautifulSoup(content, "html.parser")
        for res in soup.select(".result"):
            a = res.select_one(".result__url")
            snippet_el = res.select_one(".result__snippet")
            if not a or not snippet_el: continue
            
            link = a.get("href", "")
            snippet = snippet_el.text.strip()
            
            contact_name = ""
            company = ""
            stype = "new_cto_cpo"
            if "Chief Digital Officer" in snippet:
                stype = "new_cdo"
                
            # Regex extractors
            m1 = re.search(r"([A-Z][a-z]+ [A-Z][a-z]+).{1,20}joins ([A-Z][A-Za-z0-9]+)", snippet)
            m2 = re.search(r"([A-Z][A-Za-z0-9]+) appoints ([A-Z][a-z]+ [A-Z][a-z]+)", snippet)
            
            if m1:
                contact_name, company = m1.group(1), m1.group(2)
            elif m2:
                company, contact_name = m2.group(1), m2.group(2)
                
            if company and contact_name:
                new_signals.append({
                    "company": company.lower(),
                    "company_raw": company,
                    "signal_type": stype,
                    "source_url": link,
                    "headline": snippet,
                    "contact_name": contact_name,
                    "contact_title": "Executive"
                })
    return new_signals

def parse_ddg_techcrunch():
    print("\\n📡 SOURCE 4: TechCrunch Funding via DDG")
    queries = [
        'site:techcrunch.com "Series A" Europe SaaS 2025',
        'site:techcrunch.com "Series B" Europe healthtech fintech 2025',
        'site:techcrunch.com raises million Europe software 2025'
    ]
    
    new_signals = []
    for q in queries:
        url = f"https://html.duckduckgo.com/html/?q={requests.utils.quote(q)}"
        content = safe_get(url)
        if not content: continue
        
        soup = BeautifulSoup(content, "html.parser")
        for res in soup.select(".result"):
            a = res.select_one(".result__url")
            title_el = res.select_one(".result__title")
            if not a or not title_el: continue
            
            link = a.get("href", "")
            title = title_el.text.strip()
            
            company = ""
            if " raises " in title:
                company = title.split(" raises ")[0].strip()
            elif " secures " in title:
                company = title.split(" secures ")[0].strip()
            elif " closes " in title:
                company = title.split(" closes ")[0].strip()
                
            if company:
                new_signals.append({
                    "company": company.lower(),
                    "company_raw": company,
                    "signal_type": "series_a_b_raised",
                    "source_url": link,
                    "headline": title,
                    "contact_name": "",
                    "contact_title": ""
                })
    return new_signals

# --- ENRICHMENT ---

def enrich_companies(unique_companies):
    print(f"\\n🔎 ENRICHING {len(unique_companies)} companies for domains...")
    cache = {}
    for comp in unique_companies:
        q = f'"{comp}" official website'
        url = f"https://html.duckduckgo.com/html/?q={requests.utils.quote(q)}"
        content = safe_get(url)
        if not content: continue
        
        soup = BeautifulSoup(content, "html.parser")
        for a in soup.select(".result__url"):
            href = a.get("href", "")
            if any(x in href.lower() for x in ["linkedin.com", "twitter.com", "facebook.com", "crunchbase.com", "duckduckgo.com"]):
                continue
            
            domain = href.replace("https://", "").replace("http://", "").replace("www.", "").split("/")[0]
            if domain:
                cache[comp] = {"website": href, "domain": domain}
                break
    return cache

# --- ENGINE ---

def run_scraper():
    existing_signals = load_signals()
    seen_uids = {s["uid"] for s in existing_signals}
    
    new_found = []
    
    try:
        new_found.extend(parse_google_news())
        new_found.extend(parse_ddg_jobs())
        new_found.extend(parse_ddg_leadership())
        new_found.extend(parse_ddg_techcrunch())
    except Exception as e:
        print(f"❌ Unhandled exception during scrape: {e}")
        traceback.print_exc()
        
    # Deduplicate within new_found and against existing
    fresh_signals = []
    comp_names = set()
    for sig in new_found:
        uid = generate_uid(sig["source_url"], sig["company_raw"])
        if uid not in seen_uids:
            seen_uids.add(uid)
            sig["uid"] = uid
            sig["detected_at"] = datetime.now(timezone.utc).isoformat()
            fresh_signals.append(sig)
            comp_names.add(sig["company_raw"])
            print(f"[{sig['signal_type']}] {sig['company_raw']} — {sig['headline'][:60]}")
            
            # Save every 10 fresh signals
            if len(fresh_signals) % 10 == 0:
                save_signals(existing_signals + fresh_signals)
                
    # Enrichment
    enrichment_data = enrich_companies(list(comp_names))
    
    for sig in fresh_signals:
        comp = sig["company_raw"]
        edata = enrichment_data.get(comp, {})
        sig["website"] = edata.get("website", "")
        domain = edata.get("domain", "")
        
        email_guess = ""
        email_conf = "low"
        
        if domain and sig["contact_name"]:
            parts = sig["contact_name"].lower().split()
            if len(parts) >= 2:
                email_guess = f"{parts[0]}.{parts[-1]}@{domain}"
                email_conf = "medium"
                
        sig["email_guess"] = email_guess
        sig["email_confidence"] = email_conf
        
    all_signals = existing_signals + fresh_signals
    save_signals(all_signals)
    return all_signals

def digest(signals):
    # Grouping
    grouped = {}
    for s in signals:
        comp = s["company"]
        if comp not in grouped:
            grouped[comp] = {
                "company_raw": s["company_raw"],
                "signals": [],
                "website": "",
                "contact_name": "",
                "contact_title": "",
                "email_guess": "",
                "email_confidence": ""
            }
        grouped[comp]["signals"].append(s)
        
        if s.get("website"): grouped[comp]["website"] = s["website"]
        if s.get("contact_name"): 
            grouped[comp]["contact_name"] = s["contact_name"]
            grouped[comp]["contact_title"] = s["contact_title"]
        if s.get("email_guess"):
            grouped[comp]["email_guess"] = s["email_guess"]
            grouped[comp]["email_confidence"] = s["email_confidence"]

    # Scoring
    results = []
    for comp, data in grouped.items():
        types_seen = set()
        total_score = 0
        best_signal_type = "dev_jobs_posted"
        best_score = 0
        source_urls = []
        
        for sig in data["signals"]:
            stype = sig["signal_type"]
            types_seen.add(stype)
            score = SCORES.get(stype, 0)
            total_score += score
            source_urls.append(sig["source_url"])
            
            if score > best_score:
                best_score = score
                best_signal_type = stype
                
        stacking_bonus = max(0, len(types_seen) - 1) * 15
        final_score = total_score + stacking_bonus
        
        # Outreach
        template_str = TEMPLATES.get(best_signal_type, TEMPLATES["dev_jobs_posted"])
        cname = data["contact_name"] if data["contact_name"] else "[Name]"
        draft = template_str.replace("{name}", cname).replace("{company}", data["company_raw"])
        
        results.append({
            "company": data["company_raw"],
            "website": data["website"],
            "final_score": final_score,
            "signal_types": ", ".join(list(types_seen)),
            "contact_name": data["contact_name"],
            "contact_title": data["contact_title"],
            "email_guess": data["email_guess"],
            "email_confidence": data["email_confidence"],
            "source_urls": " | ".join(source_urls),
            "outreach_draft": draft,
            "detected_at": max(x["detected_at"] for x in data["signals"])
        })
        
    results.sort(key=lambda x: x["final_score"], reverse=True)
    
    # Export
    if results:
        keys = results[0].keys()
        with open(OUTPUT_FILE, "w", newline="") as f:
            writer = csv.DictWriter(f, fieldnames=keys)
            writer.writeheader()
            writer.writerows(results)
            
    print(f"\\n✅ Saved {len(results)} companies to {OUTPUT_FILE}")
    print("\\n🏆 TOP 10 COMPANIES:")
    for r in results[:10]:
        print(f"- [{r['final_score']} pts] {r['company']} ({r['signal_types']}) | Contact: {r['contact_name']} | Email: {r['email_guess']}")

# --- MAIN ---

if __name__ == "__main__":
    parser = argparse.ArgumentParser()
    parser.add_argument("--digest", action="store_true", help="Process saved signals without scraping")
    parser.add_argument("--from-date", type=str, help="Only include signals after YYYY-MM-DD")
    args = parser.parse_args()
    
    if args.digest:
        sigs = load_signals()
        if args.from_date:
            try:
                dt = datetime.fromisoformat(args.from_date).replace(tzinfo=timezone.utc)
                sigs = [s for s in sigs if datetime.fromisoformat(s["detected_at"]) >= dt]
            except ValueError:
                print("Invalid date format. Use YYYY-MM-DD")
        digest(sigs)
    else:
        print("🚀 Starting B2B Signal Scraper...")
        sigs = run_scraper()
        digest(sigs)
