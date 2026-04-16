# studyplan-pipeline/generateConcept/__init__.py - UPDATED FOR SUB-SUBTOPIC SUPPORT
from __future__ import annotations
import logging, os, json, textwrap, unicodedata, re
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
SEARCH_ENDPOINT = "https://basic-rag-sandbox.search.windows.net"
SEARCH_ADMIN_KEY = "tuqRZ8A374Aw3wXKSTzOY6SEu6Ra8rOyhPgFEtcLpSAzSeBOByQL"
INDEX_NAME = "pubert-demo-new"
SEARCH_API_VERSION = "2025-05-01-preview"
SEARCH_TOP_K = 12
MAX_CHARS = 4500
DUP_SIM_THRESHOLD = float(os.getenv("CONCEPT_DUP_SIM_THRESHOLD", "0.92"))
SUBTOK_MIN_HITS = int(os.getenv("SUBTOK_MIN_HITS", "2"))
conn = "DRIVER={ODBC Driver 18 for SQL Server};SERVER=20.171.24.17;DATABASE=CME2;UID=new_root;PWD=japl@bJBYV77;Encrypt=no;TrustServerCertificate=yes;"

search_cli = SearchClient(
    endpoint=os.environ.get("SEARCH_ENDPOINT", SEARCH_ENDPOINT),
    index_name=os.environ.get("SEARCH_INDEX", INDEX_NAME),
    credential=AzureKeyCredential(os.environ.get("SEARCH_ADMIN_KEY", SEARCH_ADMIN_KEY)),
    api_version=os.environ.get("SEARCH_API_VERSION", SEARCH_API_VERSION),
)

# ───────────────────────── Azure Search (hierarchical fetch) ─────────────────────────
_SEQ_RE = re.compile(r"^(\d+)([a-zA-Z]?)(?:\.(\d+))?$")

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

def _search_all(*, search_text: str, **kwargs) -> list[dict]:
    """Collect all results for a query (paginated by skip/top)."""
    out: list[dict] = []
    skip = 0
    top = int(kwargs.pop('top', 1000) or 1000)
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

def _fetch_index_docs(topic_name: str, sub_title: str, category: str = None) -> list[dict]:
    """
    Fetch ALL chunks for a subtopic from Azure Search.
    
    NEW: When category is provided, fetches from sub_subtopic field.
    When category is None, fetches from subtopic field (top-level).
    """
    select = [
        'id', 'content', 'topic', 'subtopic', 'sub_subtopic',
        'heading_path', 'sequence', 'chunk_index', 'total_chunks'
    ]
    
    # Build filter based on category
    if category:
        # This is a sub-subtopic: match sub_subtopic field
        filt = (f"topic eq '{_escape_odata(topic_name)}' and "
                f"sub_subtopic eq '{_escape_odata(sub_title)}'")
    else:
        # Top-level subtopic: match subtopic field
        filt = (f"topic eq '{_escape_odata(topic_name)}' and "
                f"subtopic eq '{_escape_odata(sub_title)}'")
    
    docs = _search_all(search_text='*', filter=filt, select=select, top=1000)
    
    # Fallback if no direct match
    if not docs:
        if category:
            # Try broader search with category
            filt2 = f"subtopic eq '{_escape_odata(category)}' and sub_subtopic eq '{_escape_odata(sub_title)}'"
        else:
            filt2 = f"subtopic eq '{_escape_odata(sub_title)}'"
        
        docs2 = _search_all(search_text=topic_name, filter=filt2, select=select, top=250)
        
        if docs2:
            freq: dict[str, int] = {}
            for d in docs2:
                t = (d.get('topic') or '').strip()
                if t:
                    freq[t] = freq.get(t, 0) + 1
            if freq:
                best_topic = max(freq.items(), key=lambda kv: kv[1])[0]
                if category:
                    filt3 = (f"topic eq '{_escape_odata(best_topic)}' and "
                            f"sub_subtopic eq '{_escape_odata(sub_title)}'")
                else:
                    filt3 = (f"topic eq '{_escape_odata(best_topic)}' and "
                            f"subtopic eq '{_escape_odata(sub_title)}'")
                docs = _search_all(search_text='*', filter=filt3, select=select, top=1000)
    
    # Stable ordering
    def doc_key(d: dict):
        return (
            _sequence_key(d.get('sequence') or ''),
            (d.get('heading_path') or ''),
            int(d.get('chunk_index') or 0),
            (d.get('id') or '')
        )
    docs.sort(key=doc_key)
    return docs

def _compose_concept_from_index(docs: list[dict], max_chars: int = MAX_CHARS) -> str:
    """
    Merge chunks into a single raw source string for GPT rewriting.
    
    Since we're now fetching specific sub-subtopics, we don't need to group
    by sub_subtopic anymore - all docs should be for the target sub-subtopic.
    """
    if not docs:
        return ''
    
    parts: list[str] = []
    for d in docs:
        content = (d.get('content') or '').strip()
        if content:
            parts.append(content)
    
    out = "\n\n".join(parts).strip()
    return out[:max_chars]

def _mark_insufficient(subtopic_id: str, reason: str = "Insufficient source text") -> None:
    with pyodbc.connect(conn) as sql:
        cur = sql.cursor()
        cur.execute("""
            UPDATE cme.subtopics
            SET status='concept_skipped', case_amenable=0, case_status='skipped'
            WHERE subtopic_id=?
        """, subtopic_id)
        sql.commit()
    logging.info("Subtopic %s marked skipped: %s", subtopic_id, reason)

# ── Relevance + duplicate helpers ─────────────────────────────
STOPWORDS = {"and","or","the","a","an","to","of","for","in","on","with","by","as","from","into","using","use","vs","vs."}

def _norm(txt: str) -> str:
    return re.sub(r"[^a-z0-9]+", " ", (txt or "").lower()).strip()

def _kw(txt: str) -> list[str]:
    return [t for t in _norm(txt).split() if len(t) >= 3 and t not in STOPWORDS]

def _has_min_hits(text: str, sub_title: str, min_hits: int) -> bool:
    needles = set(_kw(sub_title))
    min_hits = min(min_hits, max(1, len(needles)))
    h = " " + _norm(text) + " "
    return sum(1 for n in needles if f" {n} " in h) >= min_hits

def _shingles(text: str, n: int = 5) -> set[str]:
    toks = _norm(text).split()
    return {" ".join(toks[i:i+n]) for i in range(len(toks)-n+1)} if len(toks) >= n else set()

def _jaccard(a: set[str], b: set[str]) -> float:
    return (len(a & b) / len(a | b)) if a and b else 0.0

# ─────────────────── Azure OpenAI ────────────────────────────
AZURE_OAI_ENDPOINT = "https://azure-140709.openai.azure.com/"
AZURE_OAI_KEY = os.getenv("AZURE_OPENAI_KEY")
DEPLOYMENT = "gpt-4o"
AZURE_OAI_API_VERSION = "2024-02-15-preview"

oai = AzureOpenAI(
    api_key=AZURE_OAI_KEY,
    azure_endpoint=AZURE_OAI_ENDPOINT,
    api_version=AZURE_OAI_API_VERSION,
)

def _ascii_fold(txt: str) -> str:
    return unicodedata.normalize("NFKD", txt).encode("ascii", "ignore").decode()

def _make_outline(subtopic_title: str) -> str:
    k = (subtopic_title or "").lower()
    
    if any(w in k for w in ("triage", "admission", "escalat")):
        return ("Admission & escalation criteria (vitals, dehydration, neuro, bleed); "
                "initial labs; stabilization steps; thresholds for PICU; discharge & review triggers")
    
    if any(w in k for w in ("persistent", "relapse", "failure", "deferv", "day 3", "day 5")):
        return ("Expected defervescence timeline; when treatment failure is suspected; "
                "stepwise work‑up (cultures/sensitivity/imaging); switch/extend therapy; follow‑up")
    
    if any(w in k for w in ("carrier", "clearance", "food handler")):
        return ("When to suspect carriage; stool culture schedule & clearance criteria; "
                "household/school precautions; public‑health reporting")
    
    if any(w in k for w in ("household", "outbreak", "contact")):
        return ("Who to screen/test; prophylaxis/vaccination guidance; sanitation & food/water hygiene; "
                "return precautions; community/outbreak reporting")
    
    if "complication" in k:
        return ("Early vs late complications; red-flag warning signs; "
                "pathophysiology in brief; bedside monitoring & escalation; "
                "definitive management and follow-up")
    
    if "diagnos" in k:
        return ("Core clinical features; key differentials; definitive tests "
                "(with typical sensitivity/specificity where stated); "
                "sampling pitfalls; interpretation dos & don'ts")
    
    if "treat" in k or "therap" in k:
        return ("First-line regimen with exact doses and durations as stated; "
                "alternatives for allergy/intolerance; MDR/XDR protocols; "
                "monitoring & adverse effects")
    
    if "vaccin" in k or "immun" in k:
        return ("Licensed vaccines (India); schedules; efficacy & waning; "
                "contraindications; catch-up and special groups")
    
    if "epidemiolog" in k or "burden" in k:
        return ("Burden; transmission; risk factors; sanitation/prevention "
                "messages for caregivers")
    
    return ("Key facts specific to this sub-topic only; practical points "
            "for bedside decision-making")

def _looks_clipped(txt: str) -> bool:
    if txt and len(txt.strip()) < 400:
        return True
    
    t = txt.strip()
    if t[-1] not in ".!?":
        return True
    
    if re.search(r"(,\s*)?$", t[-3:]):
        return True
    
    if t.count("(") != t.count(")"):
        return True
    
    return False

# Case amenability assessment
CASE_AMENABLE_MIN_CONF = int(os.getenv("CASE_AMENABLE_MIN_CONF", "55"))
CASE_MAX_FRACTION = float(os.getenv("CASE_MAX_FRACTION", "0.5"))
CASE_BUDGET_STATUSES = ("pending", "ready", "verified")

def _coerce_confidence(x) -> int:
    try:
        if isinstance(x, (int, float)):
            val = float(x)
        elif isinstance(x, str):
            import re
            m = re.search(r"(\d+(?:\.\d+)?)", x)
            if not m:
                return 0
            val = float(m.group(1))
        else:
            return 0
        if 0 <= val <= 1:
            val *= 100.0
        val = int(round(val))
        return max(0, min(100, val))
    except Exception:
        return 0

def _case_budget_limits(cur, topic_id: str) -> tuple[int, int]:
    cur.execute("SELECT COUNT(*) FROM cme.subtopics WHERE topic_id=?", topic_id)
    total = cur.fetchone()[0] or 0
    cap = max(0, int(total * CASE_MAX_FRACTION))
    
    cur.execute("""
        SELECT COUNT(DISTINCT s.subtopic_id)
        FROM cme.subtopics s
        LEFT JOIN cme.cases cs ON cs.subtopic_id = s.subtopic_id
        WHERE s.topic_id=?
          AND (s.case_status IN ('ready','verified') OR cs.case_id IS NOT NULL)
    """, topic_id)
    pinned = cur.fetchone()[0] or 0
    return cap, pinned

def _rank_case_candidates_gpt(topic: str, items: list[dict], slots: int) -> list[str]:
    schema = {"pick": ["..."], "why": "short note"}
    ask = {
        "role": "user",
        "content": (
            "Select up to N items that gain the MOST from a clinical case vignette.\n"
            "Prioritise decision-impact (apply/interpret): triage/disposition thresholds; diagnostic "
            "approach & data interpretation; treatment-failure & escalation; complications recognition "
            "& rescue; imaging/procedure thresholds; nuanced counselling.\n"
            "Down-rank static science (pathophysiology), basic epidemiology, generic prevention/education "
            "unless the snippet shows concrete decision points.\n"
            f"N={slots}\nITEMS=" + json.dumps(items, ensure_ascii=False) + "\n\n"
            "Return JSON only as " + json.dumps(schema)
        ),
    }
    if slots <= 0 or not items:
        return []
    try:
        rsp = oai.chat.completions.create(
            model=DEPLOYMENT,
            messages=[{"role": "system", "content": "Return JSON only."}, ask],
            temperature=0.2, max_tokens=700, response_format={"type": "json_object"},
        )
        out = json.loads(rsp.choices[0].message.content)
        picks = [p for p in (out.get("pick") or []) if isinstance(p, str)]
        return picks[:slots]
    except Exception:
        return []

def _rebalance_case_budget(topic_id: str, topic_name: str) -> list[str]:
    promoted_now: list[str] = []
    with pyodbc.connect(conn) as sql:
        cur = sql.cursor()
        
        cap, pinned = _case_budget_limits(cur, topic_id)
        avail = max(0, cap - pinned)
        if avail <= 0:
            cur.execute("""
                UPDATE s
                SET s.case_status='candidate'
                FROM cme.subtopics s
                WHERE s.topic_id=?
                  AND s.case_status='pending'
                  AND NOT EXISTS (SELECT 1 FROM cme.cases cs WHERE cs.subtopic_id = s.subtopic_id)
            """, topic_id)
            sql.commit()
            return []
        
        cur.execute("""
            SELECT s.subtopic_id, s.title, s.category
            FROM cme.subtopics s
            WHERE s.topic_id=?
              AND s.case_amenable=1
              AND s.case_status IN ('candidate','pending')
              AND NOT EXISTS (SELECT 1 FROM cme.cases cs WHERE cs.subtopic_id = s.subtopic_id)
        """, topic_id)
        rows = cur.fetchall()
        pool = [{"id": r.subtopic_id, "title": r.title, "category": r.category} for r in rows]
        
        items = []
        for p in pool[:28]:
            cur.execute("""
                SELECT TOP 1 content FROM cme.concepts WHERE subtopic_id=? ORDER BY concept_id
            """, p["id"])
            crow = cur.fetchone()
            snippet = ((crow.content if crow else "") or "")[:350]
            items.append({"id": p["id"], "title": p["title"], "snippet": snippet})
        
        winners = set(_rank_case_candidates_gpt(topic_name, items, avail))
        
        for p in pool:
            sid = p["id"]
            cur.execute("SELECT case_status FROM cme.subtopics WHERE subtopic_id=?", sid)
            status = (cur.fetchone()[0] or "").lower()
            if sid in winners:
                if status == "candidate":
                    cur.execute("UPDATE cme.subtopics SET case_status='pending' WHERE subtopic_id=?", sid)
                    promoted_now.append(sid)
            else:
                if status == "pending":
                    cur.execute("UPDATE cme.subtopics SET case_status='candidate' WHERE subtopic_id=?", sid)
        
        sql.commit()
    return promoted_now

def _assess_case_amenable_gpt(topic: str, subtopic_title: str, concept_text: str) -> tuple[bool, int, dict]:
    ask = {
        "role": "user",
        "content": f"""
        Decide if a brief paediatric clinical case vignette would ADD learning value for this sub‑topic.
        Return ONLY a JSON object with these keys (no extra keys, no prose):
        "amenable": true|false,
        "confidence": integer 0–100 (NO "%" sign),
        "why": string ≤200 characters on learning gain/applicability,
        "suggested_case_focus": array of short strings

        Context
        ───────
        Topic: {topic}
        Sub‑topic: {subtopic_title}
        Concept (for context; do not invent new facts):
        {(concept_text or '')[:2500]}
        """.strip()
    }
    
    try:
        rsp = oai.chat.completions.create(
            model=DEPLOYMENT,
            messages=[
                {"role": "system", "content": "You are a paediatrics curriculum editor. Return JSON only."},
                ask
            ],
            temperature=0.2,
            max_tokens=400,
            response_format={"type": "json_object"},
        )
        data = json.loads(rsp.choices[0].message.content)
        amen = True if data.get("amenable") is True else False
        conf = _coerce_confidence(data.get("confidence", 0))
        return amen, conf, data
    except Exception:
        logging.exception("Case amenability check failed")
        return False, 0, {"amenable": False, "confidence": 0, "why": "AI call failed", "suggested_case_focus": []}

def _call_gpt(topic: str, subtopic: str, snippets: list[str], disambiguation_hint: str = "") -> str:
    joined = "\n".join(snippets)[:MAX_CHARS]
    outline = _make_outline(subtopic)
    
    disambig_instruction = ""
    if disambiguation_hint:
        disambig_instruction = f"\n• DISAMBIGUATION: {disambiguation_hint}\n"
    
    user = f"""
Rewrite the SOURCE into a single coherent paragraph (≈250–350 words)
for paediatric post-graduates. You MUST:
• Preserve every named threshold, dose, duration, sensitivity/specificity value, and timing window verbatim if present.
• Remove bullets/odd markers; write complete sentences only.
• Organise content as: {outline}.
• Stay strictly within the sub-topic "{subtopic}"; no off-topic drift.
• Keep the framing strictly paediatric; exclude pregnancy/lactation/adult-only contexts unless explicitly present in the sub-topic title.
• If a required element in the outline is not present in SOURCE, write "Not specified in source." Do not invent content.
• Do NOT invent facts not present in source.{disambig_instruction}
— SOURCE TEXT —
{joined}
— END SOURCE —
""".strip()
    
    rsp = oai.chat.completions.create(
        model=DEPLOYMENT,
        temperature=0.35,
        max_tokens=900,
        messages=[
            {"role": "system", "content": "You are an expert paediatric writer."},
            {"role": "user", "content": user},
        ],
    )
    
    return rsp.choices[0].message.content.strip()

# ─────────────────── Main entry ─────────────────────────────
def main(msg: func.QueueMessage) -> None:
    logging.info("generateConcept triggered (SUB-SUBTOPIC AWARE)")
    
    try:
        subtopic_id = json.loads(msg.get_body().decode())["subtopic_id"]
    except Exception:
        logging.error("Bad queue payload - expected {'subtopic_id': ...}")
        return
    
    # Fetch titles WITH category
    with pyodbc.connect(conn) as sql:
        cur = sql.cursor()
        cur.execute("""
            SELECT t.topic_name, s.title, s.topic_id, s.category
            FROM cme.subtopics AS s
            JOIN cme.topics AS t ON t.topic_id = s.topic_id
            WHERE s.subtopic_id = ?
        """, subtopic_id)
        row = cur.fetchone()
        if not row:
            logging.error("Sub-topic %s not found", subtopic_id)
            return
        topic_name, sub_title, topic_id, category = row
        
        cur.execute("SELECT COUNT(*) FROM cme.cases WHERE subtopic_id=?", subtopic_id)
        existing_case_count = cur.fetchone()[0] or 0
    
    # Pull raw content from Azure Search (category-aware)
    docs = _fetch_index_docs(topic_name, sub_title, category)
    raw_txt = _compose_concept_from_index(docs, max_chars=MAX_CHARS)
    
    MIN_SOURCE_CHARS = int(os.getenv("MIN_SOURCE_CHARS", "400"))
    SOFT_MIN_SOURCE_CHARS = int(os.getenv("SOFT_MIN_SOURCE_CHARS", "250"))
    
    if (not raw_txt or len(raw_txt) < SOFT_MIN_SOURCE_CHARS):
        reason = f"Source text < {SOFT_MIN_SOURCE_CHARS} chars (index fetch)"
        if category:
            reason += f" for sub-subtopic '{sub_title}' under category '{category}'"
        _mark_insufficient(subtopic_id, reason=reason)
        return
    
    # Ask GPT to rewrite
    paragraph = _call_gpt(topic_name, sub_title, [raw_txt])
    
    if _looks_clipped(paragraph):
        logging.warning("Concept looks clipped -> retrying once")
        paragraph = _call_gpt(
            topic_name,
            sub_title,
            [raw_txt + "\n\n(Ensure the rewrite ends with a complete sentence and no hanging lists.)"],
        )
    
    if not paragraph or len(paragraph) < 400:
        logging.error("GPT rewrite failed for %s", subtopic_id)
        _mark_insufficient(subtopic_id, reason="Model rewrite too short")
        return
    
    # Relevance lint
    if not _has_min_hits(paragraph, sub_title, SUBTOK_MIN_HITS):
        logging.warning("Concept failed relevance lint for %s", subtopic_id)
        _mark_insufficient(subtopic_id, reason="Low lexical overlap with sub-topic tokens")
        return
    
    # Near-duplicate guard with regeneration attempt
    dup_of_subtopic_id = None
    with pyodbc.connect(conn) as sql_dups:
        cur2 = sql_dups.cursor()
        cur2.execute("""
            SELECT s.subtopic_id, s.title, c.content
            FROM cme.concepts c
            JOIN cme.subtopics s ON s.subtopic_id = c.subtopic_id
            WHERE s.topic_id = ? AND s.subtopic_id <> ?
        """, topic_id, subtopic_id)
        
        target_fp = _shingles(paragraph, n=5)
        closest_siblings = []
        
        for sib_id, sib_title, sib_text in cur2.fetchall():
            sim = _jaccard(target_fp, _shingles(sib_text or "", n=5))
            if sim >= DUP_SIM_THRESHOLD:
                closest_siblings.append((sim, sib_id, sib_title))
        
        if closest_siblings:
            closest_siblings.sort(reverse=True, key=lambda x: x[0])
            top_siblings = closest_siblings[:2]
            
            logging.warning(
                "Concept near-duplicate detected (%.2f with '%s') -> attempting disambiguation",
                top_siblings[0][0], top_siblings[0][2]
            )
            
            sibling_titles = ", ".join([f"'{sib[2]}'" for sib in top_siblings])
            disambig_hint = (
                f"Avoid overlap with {sibling_titles}; emphasize the unique aspects specific to '{sub_title}'."
            )
            
            paragraph_v2 = _call_gpt(topic_name, sub_title, [raw_txt], disambiguation_hint=disambig_hint)
            
            target_fp_v2 = _shingles(paragraph_v2, n=5)
            still_duplicate = False
            for sim_orig, sib_id, sib_title in top_siblings:
                cur2.execute("SELECT content FROM cme.concepts WHERE subtopic_id=?", sib_id)
                sib_row = cur2.fetchone()
                if sib_row:
                    sim_v2 = _jaccard(target_fp_v2, _shingles(sib_row.content or "", n=5))
                    if sim_v2 >= DUP_SIM_THRESHOLD:
                        still_duplicate = True
                        dup_of_subtopic_id = sib_id
                        logging.warning(
                            "After disambiguation, still near-duplicate (%.2f) with '%s'",
                            sim_v2, sib_title
                        )
                        break
            
            paragraph = paragraph_v2
    
    # Insert concept + update status
    with pyodbc.connect(conn) as sql:
        cur = sql.cursor()
        
        if dup_of_subtopic_id:
            cur.execute("""
                INSERT INTO cme.concepts (concept_id, subtopic_id, content, token_count, coverage_note, created_utc)
                VALUES (NEWID(), ?, ?, 0, ?, SYSUTCDATETIME())
            """, subtopic_id, paragraph, f"dup_of:{dup_of_subtopic_id}")
        else:
            cur.execute("""
                INSERT INTO cme.concepts (concept_id, subtopic_id, content, token_count, created_utc)
                VALUES (NEWID(), ?, ?, 0, SYSUTCDATETIME())
            """, subtopic_id, paragraph)
        
        concept_text = paragraph
        
        # Case-amenability
        if existing_case_count > 0:
            cur.execute("""
                UPDATE cme.subtopics
                SET status='mcq_pending', case_amenable=1,
                    case_status = CASE
                        WHEN case_status IN ('ready','verified','failed','skipped') THEN case_status
                        ELSE 'pending'
                    END
                WHERE subtopic_id=?
            """, subtopic_id)
        else:
            amen_raw, conf, details = _assess_case_amenable_gpt(topic_name, sub_title, concept_text)
            amen = bool(amen_raw and (conf >= CASE_AMENABLE_MIN_CONF))
            
            if amen:
                cur.execute("""
                    UPDATE cme.subtopics
                    SET status='mcq_pending', case_amenable=1, case_status='candidate'
                    WHERE subtopic_id=?
                """, subtopic_id)
            else:
                cur.execute("""
                    UPDATE cme.subtopics
                    SET status='mcq_pending', case_amenable=0, case_status='skipped'
                    WHERE subtopic_id=?
                """, subtopic_id)
        
        sql.commit()
    
    # Rebalance case budget
    promoted = _rebalance_case_budget(topic_id, topic_name)
    
    # Queue next stages
    try:
        q = QueueClient.from_connection_string(os.environ["AzureWebJobsStorage"], "mcq-queue")
        q.send_message(json.dumps({"subtopic_id": subtopic_id}))
        
        if promoted:
            cq = QueueClient.from_connection_string(os.environ["AzureWebJobsStorage"], "case-queue")
            for sid in promoted:
                cq.send_message(json.dumps({"subtopic_id": sid}))
        
        log_msg = "Concept saved -> mcq_pending"
        if category:
            log_msg += f" (category: {category})"
        if dup_of_subtopic_id:
            log_msg += f"; marked duplicate of {dup_of_subtopic_id}"
        logging.info(log_msg)
    except Exception as e:
        logging.error("Could not queue next tasks: %s", e)