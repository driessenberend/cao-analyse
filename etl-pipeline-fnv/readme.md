etl-pipeline fnv scraped alle cao's in pdf vorm van de https://www.fnv.nl/cao-sector, verwerkt deze naar chunks en embeddings, en laadt deze naar de supabase database. 

# Scrapen
Voor het scrapen gebruik ik hier beautifulsoup gebaseerd op de structuur en paden van de website van fnv. Het script `scripts/scraping.py` schrijft PDF’s weg naar `data-raw/` en legt metadata vast in `data-raw/manifest.jsonl`.

# Verwerken en laden
1) Upload PDF’s + metadata naar Supabase met `scripts/ingest_to_supabase.py`. Dit vult de tabel `cao_documents` en uploadt bestanden naar Storage.  
2) Vervolgens worden alle pdf's met behulp van een pdf-processor omgezet naar tekstformat, gechunked per 500 chars en in supabase geladen met de kolommen `cao_id`, `chunk_id`, `chunk_content`, `embedding` (OpenAI) en paginatievelden in `cao_chunks` via `scripts/main.py`.
