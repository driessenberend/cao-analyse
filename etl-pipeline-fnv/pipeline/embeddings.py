# ----------------------------- openai embeddings -----------------------------

from __future__ import annotations

import logging
import time
from typing import List

from openai import OpenAI

log = logging.getLogger("embeddings")

def embed_texts(client: OpenAI, model: str, texts: List[str]) -> List[List[float]]:
    """
    Embeds a batch of texts. Includes minimal retry logic.
    """
    for attempt in range(1, 4):
        try:
            resp = client.embeddings.create(model=model, input=texts)
            return [d.embedding for d in resp.data]
        except Exception as e:
            if attempt == 3:
                raise
            backoff = 1.5 ** attempt
            log.warning("Embedding failed (attempt %d): %s; retry in %.1fs", attempt, e, backoff)
            time.sleep(backoff)
    raise RuntimeError("Unreachable")
