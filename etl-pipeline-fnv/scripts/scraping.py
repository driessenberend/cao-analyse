import os
import time
import hashlib
from collections import deque
from urllib.parse import urljoin, urlparse, urldefrag

import requests
from bs4 import BeautifulSoup

BASE = "https://www.fnv.nl"
START = "https://www.fnv.nl/cao-sector"
OUT_DIR = "data-raw"
os.makedirs(OUT_DIR, exist_ok=True)

HEADERS = {
    "User-Agent": "cao-scraper/1.0"
}

def is_cao_page(url: str) -> bool:
    p = urlparse(url)
    return (
        p.netloc == "www.fnv.nl"
        and p.path.startswith("/cao-sector/")
        and p.path.count("/") >= 2
    )

def fetch(url: str, session: requests.Session) -> str:
    r = session.get(url, headers=HEADERS, timeout=30)
    r.raise_for_status()
    return r.text

def find_download_je_cao_link(soup: BeautifulSoup) -> str | None:
    for a in soup.find_all("a"):
        text = (a.get_text() or "").strip().lower()
        if "download je cao" in text:
            href = a.get("href")
            if href:
                return href
    return None

def download_pdf(url: str, session: requests.Session):
    name = os.path.basename(urlparse(url).path)
    if not name.lower().endswith(".pdf"):
        name += ".pdf"

    path = os.path.join(OUT_DIR, name)
    if os.path.exists(path):
        return

    r = session.get(url, headers=HEADERS, stream=True, timeout=60)
    r.raise_for_status()

    with open(path, "wb") as f:
        for chunk in r.iter_content(1024 * 256):
            if chunk:
                f.write(chunk)

def main():
    session = requests.Session()
    visited = set()
    queue = deque([START])

    while queue:
        url = urldefrag(queue.popleft())[0]
        if url in visited:
            continue
        visited.add(url)

        if url != START and not is_cao_page(url):
            continue

        try:
            html = fetch(url, session)
        except Exception:
            continue

        soup = BeautifulSoup(html, "html.parser")

        # 1) probeer “Download je cao” te vinden
        link = find_download_je_cao_link(soup)
        if link:
            pdf_url = urljoin(BASE, link)
            print(f"[PDF] {pdf_url}")
            download_pdf(pdf_url, session)
            time.sleep(0.5)
            continue  # dit is een CAO-detailpagina; geen verdere crawl nodig

        # 2) anders: verzamel meer cao-sector links
        for a in soup.select("a[href]"):
            href = a.get("href")
            if not href:
                continue
            next_url = urljoin(BASE, href)
            if is_cao_page(next_url) and next_url not in visited:
                queue.append(next_url)

        time.sleep(0.3)

if __name__ == "__main__":
    main()