@echo off
cd C:\Users\Satyam\Scraper-tool
start python -m http.server 9000
timeout /t 2 /nobreak > nul
start http://localhost:9000/dashboard.html