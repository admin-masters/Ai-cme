"""
Stage 2: Hierarchical Meta-tagging with Reference-aware GPT Processing
Enhanced version for hierarchical chunking strategy
"""
from __future__ import annotations
import argparse, json, logging, re, secrets
from pathlib import Path
from typing import Any
from azure.storage.blob import BlobServiceClient, ContentSettings
from reportlab.lib.pagesizes import A4
from reportlab.pdfgen import canvas
from textwrap import wrap
from openai import AzureOpenAI
from io import BytesIO
import requests

from cfg import (BLOB_CONNECTION_STR, CONTAINER_CHUNKS, CONTAINER_META,
                 CONTAINER_MANIFESTS, AZURE_OAI_ENDPOINT, AZURE_OAI_KEY,
                 AZURE_OAI_DEPLOYMENT, AZURE_OAI_API_VER, LOCAL_OUT)
from helpers.string_helpers import normalise, safe_json
from helpers.taxonomy_loader import load_keywords

logging.basicConfig(level=logging.INFO)
blob = BlobServiceClient.from_connection_string(BLOB_CONNECTION_STR)
oai = AzureOpenAI(
    api_key       = AZURE_OAI_KEY,
    api_version   = AZURE_OAI_API_VER,
    azure_endpoint= AZURE_OAI_ENDPOINT
)

# Enhanced schema for hierarchical structure
SCHEMA_FIELDS = {
    "chunk_summary",
    "learning_objectives",
    "key_facts",
    "complexity",
    "tags_mesh",
    "tags_custom",
    "reference_quote",
    "chunk_heading",
    "document_key_topics",
    # NEW: Hierarchical context fields
    "topic",
    "subtopic",
    "sub_subtopic",
    "heading_path",
    "reference_count",
    "has_guidelines"
}

NUM_OR_GUIDELINE_RE = re.compile(r"\d|WHO|BNF|CDC|IAP|NCDC|ICMR|BMJ", re.I)
FALLBACK_FACT = "Evidence-based supportive care measures are essential"

MAX_TOKENS = 1600
MAX_RETRIES = 3

def write_small_pdf(title: str, body: str, out: Path):
    """Create PDF with hierarchical heading display"""
    c = canvas.Canvas(str(out), pagesize=A4)
    y = A4[1]-50
    
    # Title with hierarchy
    c.setFont("Helvetica-Bold", 14)
    for ln in wrap(title, 70):
        c.drawString(40, y, ln)
        y -= 18
    
    y -= 12
    c.setFont("Helvetica", 11)
    for ln in wrap(body, 95):
        if y < 50:
            break
        c.drawString(40, y, ln)
        y -= 14
    
    c.showPage()
    c.save()

def upload_blob(local: Path, container: str) -> str:
    """Upload blob with proper content type"""
    ct = "application/json" if local.suffix==".json" else "application/pdf"
    client = blob.get_container_client(container)
    try:
        client.create_container()
    except Exception:
        pass
    
    name = f"{local.stem}_{secrets.randbelow(1_000_000):06d}{local.suffix}"
    with local.open("rb") as fh:
        client.upload_blob(name, fh, overwrite=True,
                           content_settings=ContentSettings(content_type=ct))
    return f"https://{blob.account_name}.blob.core.windows.net/{container}/{name}"

def build_hierarchical_context(rec: dict) -> str:
    """Build context string with hierarchical information"""
    context_parts = []
    
    # Main hierarchy
    context_parts.append(f"TOPIC: {rec.get('topic', 'Unknown')}")
    context_parts.append(f"SUBTOPIC: {rec.get('subtopic', 'General')}")
    if rec.get('sub_subtopic'):
        context_parts.append(f"SUB-SUBTOPIC: {rec['sub_subtopic']}")
    
    # Chunk position
    if rec.get('total_chunks', 1) > 1:
        context_parts.append(
            f"Part {rec.get('chunk_index', 0) + 1} of {rec['total_chunks']}"
        )
    
    # References
    refs = rec.get('references', [])
    if refs:
        context_parts.append(f"\nREFERENCES ({len(refs)}):")
        for i, ref in enumerate(refs[:5], 1):  # Show first 5
            context_parts.append(f"{i}. {ref[:150]}...")  # Truncate long refs
        if len(refs) > 5:
            context_parts.append(f"... and {len(refs) - 5} more references")
    
    return "\n".join(context_parts)

def assess_complexity(rec: dict, content: str, refs: list) -> str:
    """
    Assess complexity based on hierarchy level and content characteristics
    
    Rules:
    - First level (topic only): Usually overview → "basic" or "intermediate"
    - Second level (subtopic): Usually detailed → "intermediate" or "advanced"
    - Third level (sub-subtopic): Usually very detailed → "advanced"
    - Has many references (>5): Likely more complex
    - Contains numeric data/guidelines: Likely more specific
    """
    level = 1
    if rec.get('subtopic'):
        level = 2
    if rec.get('sub_subtopic'):
        level = 3
    
    ref_count = len(refs)
    has_numbers = bool(NUM_OR_GUIDELINE_RE.search(content))
    
    # Heuristic scoring
    if level == 1 and ref_count <= 3:
        return "basic"
    elif level == 3 or ref_count > 8 or (has_numbers and ref_count > 5):
        return "advanced"
    else:
        return "intermediate"

def gpt_json(system: str, user: str, temp=0.0) -> str:
    """Call GPT in JSON-mode and return JSON string"""
    rsp = oai.chat.completions.create(
        model          = AZURE_OAI_DEPLOYMENT,
        temperature    = temp,
        max_tokens     = MAX_TOKENS,
        response_format= {"type": "json_object"},
        messages=[
            {"role":"system","content":system},
            {"role":"user"  ,"content":user}
        ]
    )
    return rsp.choices[0].message.content

def process_chunk(rec: dict, taxonomy: dict,
                  meta_out: Path, pdf_out: Path) -> tuple[str, dict]:
    """Process chunk with hierarchical awareness"""
    
    # Download chunk content
    r = requests.get(rec["blob_url"], timeout=30)
    if rec["blob_url"].lower().endswith(".pdf"):
        from pdfminer.high_level import extract_text
        txt = extract_text(BytesIO(r.content))
    else:
        txt = r.text
    
    # Build hierarchical context
    context = build_hierarchical_context(rec)
    
    # Enhanced GPT prompt with hierarchy
    prompt = f"""
You are analyzing a medical knowledge chunk with the following hierarchical context:

{context}

CHUNK CONTENT (truncated ≤2000 chars):
{txt[:2000]}

Return a valid JSON object with these EXACT keys:
- chunk_summary: Concise summary (≤50 words) that reflects the hierarchical position
- learning_objectives: List of 2-4 specific learning goals
- key_facts: List of 3-6 key facts (MUST include numeric data or guidelines if present)
- complexity: One of ["basic", "intermediate", "advanced"]
- tags_mesh: List of 3-8 MeSH-compatible medical terms
- tags_custom: List of 2-5 custom descriptive tags
- reference_quote: One notable quote from references (if available)
- document_key_topics: List of 2-4 main topics covered

IMPORTANT:
1. Use the hierarchical context (topic/subtopic/sub-subtopic) to guide your summary
2. If references are provided, incorporate their authority into your assessment
3. Extract specific numeric values, dosages, guidelines from the content
4. Never leave any field empty - provide best estimates
"""

    attempt = 0
    meta_json = None
    
    while attempt < MAX_RETRIES and meta_json is None:
        try:
            raw = gpt_json(
                "You are a medical knowledge-engineering assistant specializing in hierarchical document analysis.",
                prompt
            )
            meta_json = safe_json(raw) or {}
            meta_json = enforce_schema(meta_json, rec)
            
            # Self-healing: Check if summary is meaningful
            if _needs_fix(meta_json):
                fix_prompt = f"""
Chunk hierarchy: {rec.get('topic', '')} > {rec.get('subtopic', '')} > {rec.get('sub_subtopic', '')}

Chunk content:
{txt[:1200]}

Provide ONLY a concise, informative chunk_summary (≤50 words) in JSON:
{{"chunk_summary": "..."}}
"""
                try:
                    raw_fix = gpt_json(
                        "You are a scientific summarization assistant.",
                        fix_prompt,
                        temp=0.2
                    )
                    fix = safe_json(raw_fix) or {}
                    if fix.get("chunk_summary"):
                        meta_json["chunk_summary"] = fix["chunk_summary"]
                except Exception as e:
                    logging.warning("%s – re-prompt for summary failed (%s)",
                                    rec["chunk_id"], e)
            
            if not meta_json:
                raise ValueError("missing keys")
                
        except Exception as e:
            attempt += 1
            if attempt == MAX_RETRIES:
                logging.error("%s – GPT failed after %d retries (%s)",
                              rec["chunk_id"], attempt, e)
                # Fallback stub
                meta_json = create_fallback_meta(rec, txt)
    
    # QA: Ensure key_facts has numeric/guideline content
    kf = meta_json.get("key_facts", [])
    if not isinstance(kf, list):
        kf = [str(kf)] if kf is not None else []
    if not any(NUM_OR_GUIDELINE_RE.search(str(f)) for f in kf):
        kf.append(FALLBACK_FACT)
    meta_json["key_facts"] = kf
    
    # Override complexity with heuristic if GPT seems wrong
    refs = rec.get('references', [])
    heuristic_complexity = assess_complexity(rec, txt, refs)
    if meta_json.get("complexity") == "unknown":
        meta_json["complexity"] = heuristic_complexity
    
    # Write outputs
    meta_out.write_text(json.dumps(meta_json, indent=2), encoding="utf-8")
    write_small_pdf(rec["heading_path"], txt, pdf_out)
    
    url_json = upload_blob(meta_out, CONTAINER_META)
    upload_blob(pdf_out, CONTAINER_META)
    
    return url_json, meta_json

def _needs_fix(m: dict) -> bool:
    """Check if summary needs regeneration"""
    bad = ("tbd", "autogen failure", "", None, "unknown")
    summary = str(m.get("chunk_summary", "")).strip().lower()
    return summary in bad or len(summary) < 10

def create_fallback_meta(rec: dict, content: str) -> dict:
    """Create fallback metadata when GPT fails"""
    refs = rec.get('references', [])
    return {
        "chunk_heading":       rec.get("heading_path", "Unknown"),
        "chunk_summary":       f"Content about {rec.get('subtopic', 'medical topic')}",
        "learning_objectives": [f"Understand {rec.get('subtopic', 'this topic')}"],
        "key_facts":           [FALLBACK_FACT],
        "complexity":          assess_complexity(rec, content, refs),
        "tags_mesh":           [],
        "tags_custom":         [rec.get("topic", ""), rec.get("subtopic", "")],
        "reference_quote":     refs[0][:200] if refs else "",
        "document_key_topics": [rec.get("topic", ""), rec.get("subtopic", "")],
        # NEW: Hierarchical fields
        "topic":               rec.get("topic", ""),
        "subtopic":            rec.get("subtopic", ""),
        "sub_subtopic":        rec.get("sub_subtopic"),
        "heading_path":        rec.get("heading_path", ""),
        "reference_count":     len(refs),
        "has_guidelines":      bool(NUM_OR_GUIDELINE_RE.search(content)),
        "sequence": rec.get("sequence", "")
    }

REQ_KEYS = {
    "chunk_summary": "",
    "learning_objectives": [],
    "key_facts": [],
    "complexity": "intermediate",
    "tags_mesh": [],
    "tags_custom": [],
    "reference_quote": "",
    "chunk_heading": "",
    "document_key_topics": [],
    # NEW: Hierarchical fields
    "topic": "",
    "subtopic": "",
    "sub_subtopic": None,
    "heading_path": "",
    "reference_count": 0,
    "has_guidelines": False,
    "sequence": "" 
}

def enforce_schema(obj: dict, rec: dict) -> dict:
    out = obj.copy()
    
    # Fill missing fields with defaults
    for k, default in REQ_KEYS.items():
        v = out.get(k)
        if v in (None, "", [], {}):
            out[k] = default
    
    # Always populate hierarchical fields from manifest record
    out["topic"] = rec.get("topic", "")
    out["subtopic"] = rec.get("subtopic", "")
    out["sub_subtopic"] = rec.get("sub_subtopic")
    out["heading_path"] = rec.get("heading_path", "")
    out["chunk_heading"] = rec.get("heading_path", "")
    out["reference_count"] = len(rec.get("references", []))
    out["sequence"] = rec.get("sequence", "")  # NEW: Add this
    
    return out

def main():
    p = argparse.ArgumentParser()
    p.add_argument("--topic", required=True, help="Topic name for taxonomy loading")
    p.add_argument("--manifest", default="manifest-1.json", help="Input manifest")
    args = p.parse_args()

    taxonomy = load_keywords(args.topic)
    m1_path = LOCAL_OUT / args.manifest
    manifest1 = json.loads(m1_path.read_text(encoding="utf-8"))
    manifest2 = []

    meta_dir = LOCAL_OUT / "meta"
    meta_dir.mkdir(parents=True, exist_ok=True)
    
    logging.info("Starting Stage 2 with hierarchical chunking...")
    logging.info("Processing %d chunks", len(manifest1))
    
    for i, rec in enumerate(manifest1, 1):
        cid = rec['chunk_id']
        logging.info("[%d/%d] Processing: %s", i, len(manifest1), cid)
        
        meta_file = meta_dir / f"{cid}.json"
        pdf_file = meta_dir / f"{cid}.pdf"
        
        try:
            url, meta = process_chunk(rec, taxonomy, meta_file, pdf_file)
            manifest2.append({**rec, "meta_blob": url})
            logging.info("  ✓ Topic: %s | Subtopic: %s | Refs: %d",
                         rec.get('topic', '?')[:30],
                         rec.get('subtopic', '?')[:30],
                         len(rec.get('references', [])))
        except Exception as e:
            logging.error("  ✗ Failed: %s", e)
            # Create fallback
            stub = create_fallback_meta(rec, "")
            meta_file.write_text(json.dumps(stub, indent=2), encoding="utf-8")
            write_small_pdf(rec.get("heading_path", "Error"), "AUTOGEN FAILURE", pdf_file)
            url = upload_blob(meta_file, CONTAINER_META)
            upload_blob(pdf_file, CONTAINER_META)
            manifest2.append({**rec, "meta_blob": url})

    m2_path = LOCAL_OUT / "manifest-2.json"
    m2_path.write_text(json.dumps(manifest2, indent=2), encoding="utf-8")
    upload_blob(m2_path, CONTAINER_MANIFESTS)
    
    # Statistics
    topics = set(e.get("topic", "") for e in manifest2)
    subtopics = set((e.get("topic", ""), e.get("subtopic", "")) for e in manifest2)
    avg_refs = sum(len(e.get("references", [])) for e in manifest2) // max(len(manifest2), 1)
    
    logging.info("="*70)
    logging.info("Stage 2 Complete!")
    logging.info("  Total chunks: %d", len(manifest2))
    logging.info("  Unique topics: %d", len(topics))
    logging.info("  Unique subtopics: %d", len(subtopics))
    logging.info("  Avg references: %d", avg_refs)
    logging.info("  Manifest: %s", m2_path)
    logging.info("="*70)

if __name__ == "__main__":
    main()