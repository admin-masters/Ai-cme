"""
Stage 3: Vector Indexing with Hierarchical Metadata and Reference Tracking
Enhanced version for hierarchical chunking strategy with proper URL handling
"""
from __future__ import annotations

import argparse
import io
import json
import logging
import re
import time
import warnings
from pathlib import Path
from urllib.parse import quote

import requests
import torch
from pdfminer.high_level import extract_text as pdf_extract_text
from transformers import AutoModel, AutoTokenizer

from cfg import (
    HF_MODEL,
    LOCAL_OUT,
    SEARCH_ADMIN_KEY,
    SEARCH_API_VERSION,
    SEARCH_ENDPOINT,
    VECTOR_INDEX_NAME,
)

# ───────────────────────── logging ────────────────────────────
logging.basicConfig(level=logging.INFO)
log = logging.getLogger("stage3")

warnings.filterwarnings("ignore", message="CropBox missing")

# ────────────────────── HF model & embed ──────────────────────
log.info("Loading PubMedBERT model: %s", HF_MODEL)
tok = AutoTokenizer.from_pretrained(HF_MODEL)
model = AutoModel.from_pretrained(HF_MODEL)
model.eval()
torch.set_grad_enabled(False)
MAX_POS = model.config.max_position_embeddings
log.info("Model loaded. Max positions: %d", MAX_POS)

def embed(text: str) -> list[float]:
    """Return a 768-dim PubMedBERT CLS embedding."""
    t = tok(text, return_tensors="pt", truncation=True,
            max_length=MAX_POS, padding=False)
    with torch.no_grad():
        return model(**t).last_hidden_state[:, 0, :].squeeze().tolist()

# ────────────────────── retry helpers ────────────────────────
MAX_RETRIES = 3
BASE_SLEEP = 2
TIMEOUT = (5, 60)

def _retry(fn, *args, **kwargs):
    """Retry function with exponential backoff"""
    for a in range(1, MAX_RETRIES + 1):
        try:
            return fn(*args, **kwargs)
        except Exception as e:
            if a == MAX_RETRIES:
                raise
            wait = BASE_SLEEP * (2 ** (a - 1))
            log.warning("Attempt %d/%d failed (%s) – retrying in %ds…",
                        a, MAX_RETRIES, e, wait)
            time.sleep(wait)

# ────────────────────── URL handling ──────────────────────────
_alnum_re = re.compile(r"[^0-9A-Za-z]+")

def _safe_id(raw: str) -> str:
    """Collapse non-alnum chars → '_' and hard-limit to 128 chars."""
    return _alnum_re.sub("_", raw)[:128]

def _encode_blob_url(url: str) -> str:
    """
    Properly encode blob URL to handle spaces and special characters.
    Only encodes the blob name part, not the entire URL.
    """
    if not url:
        return url
    
    try:
        parts = url.split('/')
        if len(parts) >= 4:  # https://account.blob.core.windows.net/container/blob
            blob_name = parts[-1]
            
            # Safe characters: letters, numbers, underscore, hyphen, period, parentheses
            safe_chars = '_.-()' 
            encoded_blob = quote(blob_name, safe=safe_chars)
            parts[-1] = encoded_blob
            
            if encoded_blob != blob_name:
                log.debug("URL encoded: '%s' → '%s'", blob_name, encoded_blob)
            
            return '/'.join(parts)
    except Exception as e:
        log.warning("URL encoding failed for %s: %s", url, e)
    return url

def _verify_blob_url(url: str) -> bool:
    """Test if blob URL is accessible with HEAD request."""
    if not url.strip():
        return False
    try:
        resp = requests.head(url, timeout=(5, 10))
        return resp.status_code == 200
    except Exception:
        return False

def _find_working_blob_url(original_url: str, chunk_id: str) -> str:
    """Try multiple URL variations to find one that works."""
    candidates = [original_url]
    
    # Try encoded version
    encoded = _encode_blob_url(original_url)
    if encoded != original_url:
        candidates.append(encoded)
    
    # Try building from chunk_id if original fails
    if chunk_id:
        try:
            base_url = '/'.join(original_url.split('/')[:-1])
            # Extract base name and number from chunk_id
            parts = chunk_id.split('_')
            if len(parts) >= 2 and parts[-1].startswith('part'):
                # Multi-part chunk: TOPIC_SUBTOPIC_partN
                base_name = '_'.join(parts[:-1])
                chunk_num = parts[-1].replace('part', '')
            else:
                # Single chunk
                base_name = chunk_id
                chunk_num = "0"
            
            canonical_name = f"{base_name}.pdf"
            canonical_url = f"{base_url}/{canonical_name}"
            candidates.append(canonical_url)
            candidates.append(_encode_blob_url(canonical_url))
        except Exception:
            pass
    
    # Test each candidate
    for url in candidates:
        if url and _verify_blob_url(url):
            log.debug("Working URL found: %s", url)
            return url
    
    # Return encoded original as fallback
    log.warning("No working URL found, using encoded original")
    return _encode_blob_url(original_url)

# ────────────────────── vector upload ──────────────────────────
HDRS = {"Content-Type": "application/json", "api-key": SEARCH_ADMIN_KEY}
MAX_CONTENT_CHARS = 16_384

def upload_vector(doc_id: str, content: str, emb: list[float], 
                  blob_url: str, metadata: dict) -> None:
    """
    Upload document vector to Azure Cognitive Search with hierarchical metadata.
    
    Args:
        doc_id: Unique document ID
        content: Text content for search
        emb: 768-dim embedding vector
        blob_url: URL to source blob
        metadata: Hierarchical metadata (topic, subtopic, references, etc.)
    """
    url = (f"{SEARCH_ENDPOINT}/indexes/{VECTOR_INDEX_NAME}/docs/index"
           f"?api-version={SEARCH_API_VERSION}")
    
    # Encode blob URL properly
    encoded_blob_url = _encode_blob_url(blob_url)
    references = metadata.get("references", [])
    # Azure Search has field size limits, so limit to ~20 references
    limited_refs = references[:20]
    print(metadata.get("sequence", "")[:50])
    # Build search document with hierarchical fields
    doc = {
        "@search.action": "upload",
        "id": _safe_id(doc_id),
        "content": content[:MAX_CONTENT_CHARS],
        "embedding": emb,
        "blob_url": encoded_blob_url,
        
        # Hierarchical navigation fields
        "topic": metadata.get("topic", "")[:100],
        "subtopic": metadata.get("subtopic", "")[:100],
        "sub_subtopic": metadata.get("sub_subtopic", "")[:100] if metadata.get("sub_subtopic") else "",
        "heading_path": metadata.get("heading_path", "")[:256],
        "sequence": metadata.get("sequence", "")[:50], 
        # Reference metadata
        "references": limited_refs,
        "reference_count": metadata.get("reference_count", 0),
        "has_references": len(metadata.get("references", [])) > 0,
        "has_guidelines": metadata.get("has_guidelines", False),
        
        # Chunk metadata
        "chunk_index": metadata.get("chunk_index", 0),
        "total_chunks": metadata.get("total_chunks", 1),
        "char_count": metadata.get("char_count", 0),
        
        # Source tracking
        "pdf_source": metadata.get("pdf_source", ""),
    }
    
    payload = {"value": [doc]}
    
    def _post():
        r = requests.post(url, headers=HDRS, json=payload, timeout=TIMEOUT)
        if not r.ok:
            raise RuntimeError(f"{r.status_code} – {r.text}")
        resp = r.json()
        bad = [v for v in resp.get("value", []) if not v.get("status", True)]
        if bad:
            raise RuntimeError(f"Search rejected docs: {bad}")
    
    _retry(_post)
    log.debug("Uploaded vector for %s with metadata: topic=%s, refs=%d",
              doc_id, metadata.get("topic", "?")[:30], 
              len(metadata.get("references", [])))

# ─────────────────────────── main ────────────────────────────
def main() -> None:
    parser = argparse.ArgumentParser()
    parser.add_argument("--manifest", default="manifest-2.json",
                        help="manifest-2.json relative to LOCAL_OUT")
    args = parser.parse_args()

    manifest_path: Path = LOCAL_OUT / args.manifest
    if not manifest_path.exists():
        log.error("Manifest not found: %s", manifest_path)
        return

    manifest = json.loads(manifest_path.read_text(encoding="utf-8"))
    log.info("Loaded manifest with %d chunks", len(manifest))
    
    uploaded = 0
    failed_urls = []
    skipped_empty = 0

    for i, rec in enumerate(manifest, 1):
        cid = rec["chunk_id"]
        log.info("[%d/%d] Processing: %s", i, len(manifest), cid)
        
        original_url = rec["blob_url"]

        # Find working blob URL with fallback strategies
        working_url = _find_working_blob_url(original_url, cid)
        if working_url != original_url:
            log.info("  Using corrected URL: %s", working_url)
            rec["blob_url"] = working_url
        elif not _verify_blob_url(working_url):
            log.error("  ✗ No working URL found")
            failed_urls.append((cid, original_url))
            continue

        # Download blob content
        try:
            resp = _retry(requests.get, rec["blob_url"], timeout=TIMEOUT)
        except Exception as e:
            log.error("  ✗ Download failed: %s", e)
            failed_urls.append((cid, rec["blob_url"]))
            continue

        # Extract text
        try:
            if rec["blob_url"].lower().endswith(".pdf"):
                text = pdf_extract_text(io.BytesIO(resp.content)) or ""
            else:
                text = resp.text or ""
        except Exception as e:
            log.error("  ✗ Text extraction failed: %s", e)
            continue

        if not text.strip():
            log.warning("  ⚠ Empty content, skipped")
            skipped_empty += 1
            continue

        # Generate embedding and upload with metadata
        try:
            log.debug("  Generating embedding...")
            vec = embed(text)
            
            log.debug("  Uploading to search index...")
            upload_vector(cid, text, vec, rec["blob_url"], rec)
            
            uploaded += 1
            log.info("  ✓ Uploaded (topic: %s, refs: %d)",
                     rec.get("topic", "?")[:30],
                     len(rec.get("references", [])))
        except Exception as e:
            log.error("  ✗ Vector upload failed: %s", e)
            failed_urls.append((cid, rec["blob_url"]))

    # Summary statistics
    log.info("="*70)
    log.info("Stage 3 Complete!")
    log.info("  Total chunks: %d", len(manifest))
    log.info("  Successfully uploaded: %d", uploaded)
    log.info("  Failed URLs: %d", len(failed_urls))
    log.info("  Skipped (empty): %d", skipped_empty)
    
    # Group by topic for analysis
    topic_counts = {}
    for rec in manifest:
        topic = rec.get("topic", "Unknown")
        topic_counts[topic] = topic_counts.get(topic, 0) + 1
    
    log.info("\nChunks per topic:")
    for topic, count in sorted(topic_counts.items(), key=lambda x: -x[1])[:10]:
        log.info("  %s: %d chunks", topic[:40], count)
    
    if failed_urls:
        log.warning("\n⚠ Failed URLs summary:")
        for cid, url in failed_urls[:10]:  # Show first 10
            log.warning("  %s: %s", cid, url)
        if len(failed_urls) > 10:
            log.warning("  ... and %d more failures", len(failed_urls) - 10)
    
    log.info("="*70)

if __name__ == "__main__":
    main()