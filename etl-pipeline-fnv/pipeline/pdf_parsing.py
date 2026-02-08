# ----------------------------- pdf parsing -----------------------------

from __future__ import annotations

from typing import List, Optional, Tuple

import fitz  # PyMuPDF

def extract_text_with_page_map(pdf_bytes: bytes) -> Tuple[str, List[Tuple[int, int, int]]]:
    """
    Returns:
      - full_text: concatenated text
      - page_spans: list of (page_index, start_char, end_char) in full_text

    This allows you to store page_start/page_end per chunk cheaply.
    """
    doc = fitz.open(stream=pdf_bytes, filetype="pdf")
    parts: List[str] = []
    spans: List[Tuple[int, int, int]] = []
    cursor = 0

    for i, page in enumerate(doc):
        t = page.get_text("text") or ""
        parts.append(t)
        start = cursor
        cursor += len(t)
        end = cursor
        spans.append((i + 1, start, end))  # 1-based page number
    doc.close()

    full_text = "".join(parts).replace("\x00", " ")
    return full_text, spans


def chunk_text(text: str, chunk_chars: int) -> List[Tuple[int, int, str]]:
    """
    Returns list of (char_start, char_end, chunk_text).
    Character chunking is deterministic and simple.
    """
    text = text.strip()
    chunks: List[Tuple[int, int, str]] = []
    for i in range(0, len(text), chunk_chars):
        piece = text[i:i + chunk_chars].strip()
        if not piece:
            continue
        chunks.append((i, i + len(piece), piece))
    return chunks


def pages_for_chunk(page_spans: List[Tuple[int, int, int]], char_start: int, char_end: int) -> Tuple[Optional[int], Optional[int]]:
    """
    Map chunk [char_start, char_end) to page range.
    """
    p_start = None
    p_end = None
    for page_no, s, e in page_spans:
        if e <= char_start:
            continue
        if s >= char_end:
            break
        if p_start is None:
            p_start = page_no
        p_end = page_no
    return p_start, p_end
