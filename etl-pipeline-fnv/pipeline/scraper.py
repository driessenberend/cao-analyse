from __future__ import annotations

import json
import time
from collections import deque
from pathlib import Path
from typing import Iterable
from urllib.parse import urljoin, urlparse, urldefrag

import requests
from bs4 import BeautifulSoup

from pipeline.config import ScrapeSettings

HEADERS = {
    "User-Agent": "cao-scraper/1.0",
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


def extract_cao_name(soup: BeautifulSoup) -> str:
    h1 = soup.find("h1")
    if h1 and h1.get_text(strip=True):
        return h1.get_text(strip=True)
    title = soup.find("title")
    if title and title.get_text(strip=True):
        return title.get_text(strip=True)
    return "Onbekende CAO"


def download_pdf(url: str, session: requests.Session, out_dir: Path) -> Path:
    name = Path(urlparse(url).path).name
    if not name.lower().endswith(".pdf"):
        name += ".pdf"

    path = out_dir / name
    if path.exists():
        return path

    r = session.get(url, headers=HEADERS, stream=True, timeout=60)
    r.raise_for_status()

    with path.open("wb") as handle:
        for chunk in r.iter_content(1024 * 256):
            if chunk:
                handle.write(chunk)
    return path


def load_manifest_urls(manifest_path: Path) -> set[str]:
    urls: set[str] = set()
    if not manifest_path.exists():
        return urls
    with manifest_path.open("r", encoding="utf-8") as handle:
        for line in handle:
            line = line.strip()
            if not line:
                continue
            try:
                obj = json.loads(line)
            except json.JSONDecodeError:
                continue
            url = obj.get("pdf_url")
            if url:
                urls.add(url)
    return urls


def append_manifest(records: Iterable[dict], manifest_path: Path) -> None:
    with manifest_path.open("a", encoding="utf-8") as handle:
        for record in records:
            handle.write(json.dumps(record, ensure_ascii=False) + "\n")


def run_scrape(settings: ScrapeSettings) -> None:
    settings.out_dir.mkdir(parents=True, exist_ok=True)
    settings.manifest_path.parent.mkdir(parents=True, exist_ok=True)
    session = requests.Session()
    visited = set()
    queue = deque([settings.start_url])
    seen_pdf_urls = load_manifest_urls(settings.manifest_path)

    while queue:
        url = urldefrag(queue.popleft())[0]
        if url in visited:
            continue
        visited.add(url)

        if url != settings.start_url and not is_cao_page(url):
            continue

        try:
            html = fetch(url, session)
        except Exception:
            continue

        soup = BeautifulSoup(html, "html.parser")

        link = find_download_je_cao_link(soup)
        if link:
            pdf_url = urljoin(settings.base_url, link)
            if pdf_url in seen_pdf_urls:
                continue
            print(f"[PDF] {pdf_url}")
            local_path = download_pdf(pdf_url, session, settings.out_dir)
            cao_name = extract_cao_name(soup)
            record = {
                "source_url": url,
                "pdf_url": pdf_url,
                "file_name": local_path.name,
                "cao_name": cao_name,
            }
            append_manifest([record], settings.manifest_path)
            seen_pdf_urls.add(pdf_url)
            time.sleep(0.5)
            continue

        for a in soup.select("a[href]"):
            href = a.get("href")
            if not href:
                continue
            next_url = urljoin(settings.base_url, href)
            if is_cao_page(next_url) and next_url not in visited:
                queue.append(next_url)

        time.sleep(0.3)
