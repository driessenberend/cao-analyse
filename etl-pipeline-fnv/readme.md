etl-pipeline fnv scraped alle cao's in pdf vorm van de https://www.fnv.nl/cao-sector, verwerkt deze naar chunks en embeddings, en laadt deze naar de supabase database. 

# Scrapen
Voor het scrapen gebruik ik hier beautifulsoup gebaseerd op de structuur en paden van de website van fnv. 

# Verwerken en laden
PDF's worden gedownload naar de map data-raw. Vervolgens worden alle pdf's met behulp van een pdf-processer omgezet naar tekstformat, gechunked per 500 tokens en in supabase geladen met de kolommen 'cao-name', 'cao-id', 'chunk-id', 'chunk-content' en 'chunk-embeddings' (via de OpenAI API en opgeslagen in pgvector kolom in supabase). 
