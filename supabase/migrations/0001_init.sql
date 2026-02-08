-- Core extensions
create extension if not exists vector;
create extension if not exists pgcrypto;

-- CAO documents metadata (PDFs in Storage)
create table if not exists public.cao_documents (
    cao_id text primary key,
    cao_name text not null,
    source_url text,
    storage_bucket text not null,
    storage_path text not null,
    file_sha256 text,
    file_bytes bigint,
    ingested_at timestamptz default now(),
    processed_at timestamptz
);

-- CAO chunks with embeddings
create table if not exists public.cao_chunks (
    chunk_id text primary key,
    cao_id text not null references public.cao_documents(cao_id) on delete cascade,
    chunk_index integer not null,
    chunk_content text not null,
    embedding vector(1536),
    page_start integer,
    page_end integer,
    char_start integer,
    char_end integer
);

create index if not exists idx_cao_chunks_cao_id on public.cao_chunks(cao_id);
create index if not exists idx_cao_chunks_embedding on public.cao_chunks using ivfflat (embedding vector_cosine_ops);

-- Storage bucket for CAO PDFs
insert into storage.buckets (id, name, public)
values ('cao-pdfs', 'cao-pdfs', false)
on conflict (id) do nothing;

-- RPC for vector similarity search
create or replace function public.match_cao_chunks(
    query_embedding vector(1536),
    match_count int,
    filter_cao_id text default null
)
returns table (
    chunk_id text,
    cao_id text,
    chunk_index integer,
    chunk_content text,
    page_start integer,
    page_end integer,
    distance float
)
language sql
stable
as $$
    select
        c.chunk_id,
        c.cao_id,
        c.chunk_index,
        c.chunk_content,
        c.page_start,
        c.page_end,
        (c.embedding <=> query_embedding) as distance
    from public.cao_chunks c
    where filter_cao_id is null or c.cao_id = filter_cao_id
    order by c.embedding <=> query_embedding
    limit match_count;
$$;
