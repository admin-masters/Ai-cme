# studyplan-pipeline/generateSubtopics/__init__.py - UPDATED FOR SUB-SUBTOPIC EXPANSION
from __future__ import annotations
import logging, os, json, re, unicodedata, uuid
from typing import List, Dict, Any

import azure.functions as func
import pyodbc

from openai import AzureOpenAI
from azure.storage.queue import QueueClient
from azure.core.credentials import AzureKeyCredential
from azure.search.documents import SearchClient
from pathlib import Path
from dotenv import load_dotenv

ENV_PATH = Path(__file__).resolve().parents[1] / ".env"
load_dotenv(dotenv_path=ENV_PATH)
# ───────────────────────── Azure/OpenAI config ─────────────────────────
AZURE_OAI_ENDPOINT = "https://azure-140709.openai.azure.com/"
AZURE_OAI_KEY = os.getenv("AZURE_OPENAI_KEY")
DEPLOYMENT = "gpt-4o"
AZURE_OAI_API_VERSION = "2024-02-15-preview"
SUBTOPIC_QUEUE_NAME = "subtopic-queue"
oai_client = AzureOpenAI(
    api_key=AZURE_OAI_KEY,
    azure_endpoint=AZURE_OAI_ENDPOINT,
    api_version=AZURE_OAI_API_VERSION,
)

# ───────────────────────── Azure Cognitive Search ─────────────────────────
SEARCH_ENDPOINT = "https://basic-rag-sandbox.search.windows.net"
SEARCH_ADMIN_KEY = "tuqRZ8A374Aw3wXKSTzOY6SEu6Ra8rOyhPgFEtcLpSAzSeBOByQL"
INDEX_NAME = "pubert-demo-new"
SEARCH_API_VERSION = "2025-05-01-preview"

search_cli = SearchClient(
    endpoint=SEARCH_ENDPOINT,
    index_name=INDEX_NAME,
    credential=AzureKeyCredential(SEARCH_ADMIN_KEY),
    api_version=SEARCH_API_VERSION,
)

# ───────────────────────── Configuration ─────────────────────────
MIN_SUBTOPICS = int(os.getenv("MIN_SUBTOPICS", "22"))
MAX_SUBTOPICS = int(os.getenv("MAX_SUBTOPICS", "40"))
COVERAGE_MIN_CHARS = int(os.getenv("COVERAGE_MIN_CHARS", "1200"))
BLOCK_ON_LOW_COVERAGE = os.getenv("BLOCK_ON_LOW_COVERAGE", "0") == "0"

# ───────────────────────── Helpers ─────────────────────────
_ADULT_BAN = re.compile(r"\b(pregnan\w*|lactat\w*|maternal|fetus)\b", re.I)
_VIGNETTE_PAT = re.compile(r"\b(vignett|vignettes|scenario|scenarios|case\s+vignette|case\s+stud(y|ies)|case\s+based)\b", re.I)
_SEQ_RE = re.compile(r"^(\d+)([a-zA-Z]?)(?:\.(\d+))?$")

def _norm(s: str) -> str:
    return unicodedata.normalize("NFKD", s or "").encode("ascii", "ignore").decode()

def _escape_odata(s: str) -> str:
    return (s or "").replace("'", "''")

def _letter_rank(ch: str) -> int:
    if not ch:
        return 0
    c = ch.lower()
    if 'a' <= c <= 'z':
        return ord(c) - ord('a') + 1
    return 0

def _sequence_key(seq: str) -> tuple:
    s = (seq or '').strip()
    m = _SEQ_RE.match(s)
    if not m:
        return (10**9, 10**9, 10**9, s)
    major = int(m.group(1))
    letter = _letter_rank(m.group(2) or '')
    minor = int(m.group(3) or 0)
    return (major, letter, minor, s)

def _search_all_index(*, search_text: str, **kwargs) -> list[dict]:
    out: list[dict] = []
    skip = 0
    top = int(kwargs.pop("top", 1000) or 1000)
    while True:
        results = search_cli.search(search_text=search_text, top=top, skip=skip, **kwargs)
        batch = list(results)
        if not batch:
            break
        out.extend(batch)
        if len(batch) < top:
            break
        skip += len(batch)
        if skip > 100000:
            break
    return out



# ───────────────────────── NEW: Expand Sub-Subtopics as Subtopics ─────────────────────────
def _expand_sub_subtopics_from_index(topic_name: str) -> tuple[str, list[dict], list[dict]]:
    """
    Fetch ALL sub-subtopics from index and treat each as an independent subtopic.
    
    Returns:
        (resolved_topic, expanded_subtopics, vignette_docs)
        
    expanded_subtopics structure:
    [
        {
            "subtopic": "Sub-subtopic title",  # The actual sub-subtopic
            "category": "Parent subtopic",      # The original subtopic becomes category
            "sequence": "1a.1",
            "coverage_chars": 1500,
            "seq_key": (1, 1, 1, "1a.1")
        },
        ...
    ]
    """
    resolved = topic_name
    
    # Fetch all documents with topic, subtopic, and sub_subtopic metadata
    docs = _search_all_index(
        search_text='*',
        filter=f"topic eq '{_escape_odata(resolved)}'",
        select=["subtopic", "sub_subtopic", "sequence", "char_count", "heading_path"],
        top=1000,
    )
    
    # Group by (subtopic, sub_subtopic) pairs
    by_sub_subsub: dict[tuple, dict] = {}
    vignette_subtopics: set[str] = set()
    
    for d in docs:
        main_subtopic = (d.get("subtopic") or "").strip()
        sub_subtopic = (d.get("sub_subtopic") or "").strip()
        
        if not main_subtopic:
            continue
            
        # Check for vignettes in either subtopic or sub-subtopic
        if _VIGNETTE_PAT.search(main_subtopic) or (sub_subtopic and _VIGNETTE_PAT.search(sub_subtopic)):
            vignette_subtopics.add(main_subtopic)
            if sub_subtopic:
                vignette_subtopics.add(sub_subtopic)
            continue
        
        seq = (d.get("sequence") or "").strip()
        cc = int(d.get("char_count") or 0)
        
        # KEY CHANGE: If sub_subtopic exists, use it as the subtopic with main_subtopic as category
        if sub_subtopic:
            key = (main_subtopic, sub_subtopic)
            
            if key not in by_sub_subsub:
                by_sub_subsub[key] = {
                    "subtopic": sub_subtopic,           # Sub-subtopic becomes the subtopic
                    "category": main_subtopic,           # Original subtopic becomes category
                    "sequence": seq,
                    "coverage_chars": cc,
                    "seq_key": _sequence_key(seq),
                }
            else:
                row = by_sub_subsub[key]
                row["coverage_chars"] += cc
                if _sequence_key(seq) < row["seq_key"]:
                    row["sequence"] = seq
                    row["seq_key"] = _sequence_key(seq)
        else:
            # No sub-subtopic: treat as regular subtopic with no category
            key = (main_subtopic, "")
            
            if key not in by_sub_subsub:
                by_sub_subsub[key] = {
                    "subtopic": main_subtopic,
                    "category": None,                    # No category for top-level subtopics
                    "sequence": seq,
                    "coverage_chars": cc,
                    "seq_key": _sequence_key(seq),
                }
            else:
                row = by_sub_subsub[key]
                row["coverage_chars"] += cc
                if _sequence_key(seq) < row["seq_key"]:
                    row["sequence"] = seq
                    row["seq_key"] = _sequence_key(seq)
    
    # Convert to list and sort
    outline = list(by_sub_subsub.values())
    outline.sort(key=lambda r: (r["seq_key"], r.get("category") or "", r["subtopic"].lower()))
    
    # Fetch vignette docs
    vignette_docs: list[dict] = []
    for vs in sorted(vignette_subtopics):
        vdocs = _search_all_index(
            search_text='*',
            filter=f"topic eq '{_escape_odata(resolved)}' and (subtopic eq '{_escape_odata(vs)}' or sub_subtopic eq '{_escape_odata(vs)}')",
            select=["id", "content", "sequence", "chunk_index", "heading_path", "subtopic", "sub_subtopic"],
            top=1000,
        )
        vignette_docs.extend(vdocs)
    
    logging.info("Expanded %d sub-subtopics from index for topic '%s'", len(outline), resolved)
    
    return resolved, outline, vignette_docs

# ───────────────────────── Coverage Estimation ─────────────────────────
def _estimate_coverage(topic_name: str, sub_title: str) -> int:
    """Estimate coverage for a subtopic by searching."""
    query = f"{topic_name} {sub_title}"
    try:
        results = search_cli.search(search_text=query, top=10)
        total = 0
        for doc in results:
            body = (doc.get("content") or "")
            total += len(body)
        return total
    except Exception:
        return 0

# ───────────────────────── Vignette Case Extraction (unchanged) ─────────────────────────
def _stitch_vignette_text(vignette_docs: list[dict], max_chars: int = 18000) -> str:
    if not vignette_docs:
        return ""
    
    def k(d: dict):
        return (_sequence_key(d.get("sequence") or ""), int(d.get("chunk_index") or 0), d.get("heading_path") or "")
    
    parts: list[str] = []
    used = 0
    for d in sorted(vignette_docs, key=k):
        c = (d.get("content") or "").strip()
        if not c:
            continue
        if used + len(c) > max_chars:
            parts.append(c[: max(0, max_chars - used)])
            break
        parts.append(c)
        used += len(c)
    
    return "\n\n".join(parts).strip()

def _extract_cases_gpt(topic: str, vignette_text: str) -> list[dict]:
    """Extract structured case studies from vignette sections."""
    if not vignette_text.strip():
        return []
    
    schema = {
        "cases": [
            {
                "case_title": "string",
                "vignette": "string",
                "learning_objective": "string"
            }
        ]
    }
    
    prompt = {
        "role": "user",
        "content": (
            "Extract DISTINCT paediatric case vignettes from the SOURCE text.\n"
            "- Do NOT invent facts; only restructure.\n"
            "- Keep each vignette 90-220 words, with age, setting, time course, key symptoms, focused exam, and <=2 objective data.\n"
            "- Do NOT include diagnosis or management in the vignette text.\n"
            "Return JSON only with schema: " + json.dumps(schema) + "\n\n"
            f"TOPIC: {topic}\n\nSOURCE:\n{vignette_text}\n"
        )
    }
    
    try:
        rsp = oai_client.chat.completions.create(
            model=DEPLOYMENT,
            messages=[{"role": "system", "content": "You are a medical editor. Return JSON only."}, prompt],
            temperature=0.2,
            max_tokens=1800,
            response_format={"type": "json_object"},
        )
        out = json.loads(rsp.choices[0].message.content)
        cases = out.get("cases") or []
        if not isinstance(cases, list):
            return []
        cleaned = []
        for c in cases:
            if not isinstance(c, dict):
                continue
            vign = (c.get("vignette") or "").strip()
            if len(vign) < 80:
                continue
            cleaned.append({
                "case_title": (c.get("case_title") or "").strip()[:255] or "Clinical case",
                "vignette": vign,
                "learning_objective": (c.get("learning_objective") or "").strip()[:255],
            })
        return cleaned
    except Exception:
        logging.exception("Case extraction failed")
        return []

def _assign_cases_to_subtopics_gpt(topic: str, subtopics: list[dict], cases: list[dict]) -> list[dict]:
    """Map each case to the most logical subtopic_id."""
    if not subtopics or not cases:
        return []
    
    schema = {
        "assignments": [
            {
                "case_index": 0,
                "subtopic_id": "uuid",
                "reason": "short"
            }
        ]
    }
    
    prompt = {
        "role": "user",
        "content": (
            "Assign each CASE to exactly one SUBTOPIC where it fits best pedagogically.\n"
            "If a case does not fit any, omit it (do not guess).\n"
            "Return JSON only with schema: " + json.dumps(schema) + "\n\n"
            f"TOPIC: {topic}\n\nSUBTOPICS (id,title,category):\n" + 
            json.dumps([{"subtopic_id": s["subtopic_id"], "title": s["title"], "category": s.get("category")} 
                       for s in subtopics], ensure_ascii=False) + "\n\n"
            f"CASES (indexed from 0):\n" + json.dumps(cases, ensure_ascii=False)
        )
    }
    
    try:
        rsp = oai_client.chat.completions.create(
            model=DEPLOYMENT,
            messages=[{"role": "system", "content": "Return JSON only."}, prompt],
            temperature=0.2,
            max_tokens=1400,
            response_format={"type": "json_object"},
        )
        out = json.loads(rsp.choices[0].message.content)
        assigns = out.get("assignments") or []
        if not isinstance(assigns, list):
            return []
        cleaned = []
        valid_ids = {s["subtopic_id"] for s in subtopics}
        for a in assigns:
            if not isinstance(a, dict):
                continue
            idx = a.get("case_index")
            sid = (a.get("subtopic_id") or "").strip()
            if not isinstance(idx, int) or idx < 0 or idx >= len(cases):
                continue
            if sid not in valid_ids:
                continue
            cleaned.append({"case_index": idx, "subtopic_id": sid, "reason": (a.get("reason") or "")[:200]})
        return cleaned
    except Exception:
        logging.exception("Case assignment failed")
        return []

def _ingest_vignette_cases(topic_id: str, topic_name: str, vignette_docs: list[dict], conn_str: str) -> int:
    """Extract vignette cases, map to subtopics, insert into cme.cases, and enqueue case-mcq-queue."""
    vign_text = _stitch_vignette_text(vignette_docs)
    cases = _extract_cases_gpt(topic_name, vign_text)
    if not cases:
        return 0
    
    with pyodbc.connect(conn_str) as conn:
        cur = conn.cursor()
        cur.execute("SELECT subtopic_id, title, category FROM cme.subtopics WHERE topic_id=? ORDER BY sequence_no", topic_id)
        subs = [{"subtopic_id": r.subtopic_id, "title": r.title, "category": r.category} for r in cur.fetchall()]
    
    assignments = _assign_cases_to_subtopics_gpt(topic_name, subs, cases)
    if not assignments:
        return 0
    
    inserted = 0
    q = None
    try:
        q = QueueClient.from_connection_string(os.environ["AzureWebJobsStorage"], "case-mcq-queue")
    except Exception:
        q = None
    
    with pyodbc.connect(conn_str) as conn:
        cur = conn.cursor()
        for a in assignments:
            c = cases[a["case_index"]]
            sid = a["subtopic_id"]
            title = c["case_title"]
            vign = c["vignette"]
            lo = c.get("learning_objective") or ""
            
            cur.execute("""
                IF NOT EXISTS (SELECT 1 FROM cme.cases WHERE subtopic_id=? AND vignette=?)
                BEGIN
                    INSERT INTO cme.cases (case_id, subtopic_id, title, vignette, word_count, learning_objective)
                    VALUES (NEWID(), ?, ?, ?, ?, ?)
                END
            """, sid, vign, sid, title, vign, len(re.findall(r"\b\w+\b", vign)), lo)
            
            cur.execute("SELECT TOP 1 case_id FROM cme.cases WHERE subtopic_id=? AND vignette=? ORDER BY created_utc DESC", sid, vign)
            row = cur.fetchone()
            if row:
                case_id = row.case_id
                inserted += 1
                cur.execute("""
                    UPDATE cme.subtopics
                    SET case_amenable=1,
                        case_status = CASE WHEN case_status IN ('verified','failed') THEN case_status ELSE 'pending' END
                    WHERE subtopic_id=?
                """, sid)
                if q:
                    try:
                        q.send_message(json.dumps({"case_id": case_id}))
                    except Exception:
                        pass
        
        conn.commit()
    
    return inserted

# ───────────────────────── Main Entry Point ─────────────────────────────
def main(msg: func.QueueMessage) -> None:
    logging.info("generateSubtopics triggered (SUB-SUBTOPIC EXPANSION MODE)")
    try:
        topic_id = json.loads(msg.get_body().decode())["topic_id"]
    except Exception:
        logging.error("Bad queue message - expected JSON with topic_id")
        return
    
    conn_str = "DRIVER={ODBC Driver 18 for SQL Server};SERVER=20.171.24.17;DATABASE=CME2;UID=new_root;PWD=japl@bJBYV77;Encrypt=no;TrustServerCertificate=yes;"
    
    # Fetch topic
    with pyodbc.connect(conn_str) as conn:
        cur = conn.cursor()
        cur.execute("SELECT topic_name FROM cme.topics WHERE topic_id = ?", topic_id)
        row = cur.fetchone()
        if not row:
            logging.error("Topic %s not found", topic_id)
            return
        topic_name = row.topic_name
        
        # Fetch existing placeholder subtopics
        cur.execute("""
        SELECT subtopic_id
        FROM cme.subtopics
        WHERE topic_id = ? AND status = 'queued'
        ORDER BY sequence_no
        """, topic_id)
        queued_sub_ids = [r.subtopic_id for r in cur.fetchall()]
    
    # Expand sub-subtopics from index
    resolved_topic, expanded_outline, vignette_docs = _expand_sub_subtopics_from_index(topic_name)
    
    # Update DB topic_name if resolved differently
    if resolved_topic and resolved_topic.strip() and resolved_topic.strip() != topic_name:
        with pyodbc.connect(conn_str) as conn:
            cur = conn.cursor()
            cur.execute("UPDATE cme.topics SET topic_name=? WHERE topic_id=?", resolved_topic, topic_id)
            conn.commit()
        topic_name = resolved_topic
    
    logging.info("Expanded outline size: %d items", len(expanded_outline))
    
    if not expanded_outline:
        logging.warning("No sub-subtopics found for topic '%s' - cannot proceed", topic_name)
        return
    
    # Update/insert subtopics with category information
    affected_ids: List[str] = []
    with pyodbc.connect(conn_str) as conn:
        cur = conn.cursor()
        
        # Update existing placeholders
        for i, item in enumerate(expanded_outline[:len(queued_sub_ids)]):
            sub_id = queued_sub_ids[i]
            cur.execute("""
                UPDATE cme.subtopics
                SET title = ?, 
                    category = ?,
                    status = 'refs_pending', 
                    sequence_no = ?
                WHERE subtopic_id = ?
            """, item["subtopic"], item.get("category"), i + 1, sub_id)
            affected_ids.append(sub_id)
        
        # Delete surplus placeholders
        for sub_id in queued_sub_ids[len(expanded_outline):]:
            cur.execute("DELETE FROM cme.subtopics WHERE subtopic_id = ?", sub_id)
        
        # Insert new subtopics if needed
        for seq, item in enumerate(expanded_outline[len(queued_sub_ids):], start=len(queued_sub_ids) + 1):
            new_id = str(uuid.uuid4())
            cur.execute("""
                INSERT INTO cme.subtopics
                (subtopic_id, topic_id, title, category, sequence_no, status)
                VALUES (?, ?, ?, ?, ?, 'refs_pending')
            """, new_id, topic_id, item["subtopic"], item.get("category"), seq)
            affected_ids.append(new_id)
        
        conn.commit()
    
    # Compute coverage and tag insufficiency
    with pyodbc.connect(conn_str) as conn:
        cur = conn.cursor()
        for i, sub_id in enumerate(affected_ids):
            cur.execute("SELECT title, topic_id, category FROM cme.subtopics WHERE subtopic_id=?", sub_id)
            row = cur.fetchone()
            if not row:
                continue
            title, t_id, category = row.title, row.topic_id, row.category
            
            # Use coverage from index if available
            score = expanded_outline[i]["coverage_chars"] if i < len(expanded_outline) else 0
            if score <= 0:
                score = _estimate_coverage(topic_name, title)
            
            status = 'ok' if score >= COVERAGE_MIN_CHARS else 'insufficient'
            note = None if status == 'ok' else f"Coverage < {COVERAGE_MIN_CHARS} chars in search corpus"
            
            cur.execute("""
                UPDATE cme.subtopics
                SET coverage_score = ?, content_status = ?, coverage_note = ?
                WHERE subtopic_id = ?
            """, score, status, note, sub_id)
            
            if status != 'ok':
                cur.execute("""
                    INSERT INTO cme.content_gaps (topic_id, subtopic_id, subtopic_title, coverage_score, reason)
                    VALUES (?, ?, ?, ?, ?)
                """, t_id, sub_id, title, score, note)
        
        conn.commit()
    
    # Ingest vignette cases
    try:
        if vignette_docs:
            n_cases = _ingest_vignette_cases(topic_id, topic_name, vignette_docs, conn_str)
            logging.info("Ingested %d case(s) from vignette sections", n_cases)
    except Exception:
        logging.exception("Vignette ingestion failed")
    
    # Enqueue subtopics
    try:
        to_enqueue = []
        skipped_count = 0
        
        with pyodbc.connect(conn_str) as conn:
            cur = conn.cursor()
            cur.execute("""
                SELECT subtopic_id, content_status
                FROM cme.subtopics
                WHERE topic_id = ?
                ORDER BY sequence_no
            """, topic_id)
            rows = cur.fetchall()
            
            for sid, content_status in rows:
                status = (content_status or "").strip().lower()
                if (not BLOCK_ON_LOW_COVERAGE) or status == "ok":
                    to_enqueue.append(sid)
                else:
                    skipped_count += 1
        
        if to_enqueue:
            qc = QueueClient.from_connection_string(
                os.environ["AzureWebJobsStorage"], 
                "subtopic-queue"
            )
            for sid in to_enqueue:
                qc.send_message(json.dumps({"subtopic_id": sid}))
            
            logging.info(
                "Enqueued %d subtopic(s) to subtopic-queue (skipped %d due to low coverage)",
                len(to_enqueue), skipped_count
            )
        else:
            logging.warning(
                "No subtopics enqueued for topic %s (all %d had insufficient coverage)",
                topic_id, skipped_count
            )
    
    except Exception:
        logging.exception("Failed to enqueue subtopics - continuing anyway")
    
    logging.info("Sub-topic expansion and enqueueing complete for %s (%d items)", 
                 topic_name, len(expanded_outline))

if __name__ == "__main__":
    main