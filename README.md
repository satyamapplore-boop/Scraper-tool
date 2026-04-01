# Scraper Tool

A high-precision lead generation and enrichment pipeline that captures real-world, non-AI companies.

## Features
- 7-layer validation engine for capturing real companies.
- Multi-source cross-verification (Google Places, OpenCorporates, YC, etc.).
- CRM-ready dataset extraction and enrichment.
- High-quality European and UK industrial leads data.

## Pipeline Components
- `pipeline.py`: Main lead scraper and enrichment logic.
- `applore_lead_scraper.py`: Core scraping functionalities.
- `signal_scraper.py`: Signal extraction and data enrichment.
- `dashboard.html`: Visualization of the pipeline status and results.

## Setup
1. Clone the repository.
2. Install dependencies: `pip install -r requirements.txt` (if applicable).
3. Set up environment variables in a `.env` file (see `.env.example`).
4. Run the pipeline: `python pipeline.py`.

## Built with
- Python
- HTML/CSS (Dashboard)
