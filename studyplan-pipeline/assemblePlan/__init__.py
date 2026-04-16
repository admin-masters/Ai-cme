# ─────────────── Stage 5 – assemblePlan.py (WITH CATEGORY SUPPORT) ───────────────
from __future__ import annotations
import os, json, logging, datetime as dt, re
import azure.functions as func
import pyodbc
from azure.storage.queue import QueueClient
from pathlib import Path
from dotenv import load_dotenv

# Load environment variables from rag_pipeline/.env when running scripts directly.
ENV_PATH = Path(__file__).resolve().parents[1] / ".env"
load_dotenv(dotenv_path=ENV_PATH)
# ─────────────────────────────── config ────────────────────────────────────
DB_CONN = "DRIVER={ODBC Driver 18 for SQL Server};SERVER=20.171.24.17;DATABASE=CME2;UID=new_root;PWD=japl@bJBYV77;Encrypt=no;TrustServerCertificate=yes;"
QUEUE_CONN_STR = os.getenv("AzureWebJobsStorage")
PLAN_QUEUE_NAME = "plan-queue"
DONE_STATES = ("mcq_ready", "failed", "refs_missing")
VISIBILITY_DELAY = 120
EVAL_MIN_CONCEPT_CHARS = int(os.getenv("EVAL_MIN_CONCEPT_CHARS", "400"))
ASSEMBLE_DUP_SIM_THRESHOLD = float(os.getenv("ASSEMBLE_DUP_SIM_THRESHOLD", "0.90"))
EVAL_REQUIRE_MCQS = os.getenv("EVAL_REQUIRE_MCQS", "0") == "1"

# ────────────────────────── helper SQL -------------------------------------
SQL_SUBTOPIC_REFS = """
SELECT r.source_id, r.citation_link, r.excerpt
FROM cme.[references] r
JOIN cme.[subtopic_references] s ON s.reference_id = r.reference_id
WHERE s.subtopic_id = ?
"""

SQL_QUESTION_REFS = """
SELECT r.source_id, r.citation_link, r.excerpt
FROM cme.[references] r
JOIN cme.[question_references] q ON q.reference_id = r.reference_id
WHERE q.question_id = ?
"""

SQL_CHOICES = """
SELECT choice_index, choice_text, rationale
FROM cme.choices
WHERE question_id = ?
ORDER BY choice_index
"""

# ────────────────────────── utilities -------------------------------------
def _dictfetch(cur) -> list[dict]:
    cols = [c[0] for c in cur.description]
    return [dict(zip(cols, row)) for row in cur.fetchall()]

def _sentences(txt: str) -> list[str]:
    parts = re.split(r'(?<=[.!?])\s+', (txt or "").strip())
    return [p.strip() for p in parts if p.strip()]

# ── Near-duplicate detection ────────────────
STOPWORDS = {"and","or","the","a","an","to","of","for","in","on","with","by","as","from","into","using","use","vs","vs."}

def _norm(txt: str) -> str:
    return re.sub(r"[^a-z0-9]+", " ", (txt or "").lower()).strip()

def _fp5(txt: str) -> set[str]:
    toks = _norm(txt).split()
    return {" ".join(toks[i:i+5]) for i in range(len(toks)-4)} if len(toks) >= 5 else set()

def _jacc(a: set[str], b: set[str]) -> float:
    return (len(a & b) / len(a | b)) if a and b else 0.0

def _dedupe_near_duplicate_concepts(subs: list[dict], thr: float) -> None:
    """Annotate near-duplicate concepts without modifying content."""
    fps = []
    for s in subs:
        fp = _fp5(s.get("concept") or "")
        fps.append(fp)
    
    for i in range(len(subs)):
        for j in range(i+1, len(subs)):
            sim = _jacc(fps[i], fps[j])
            if sim >= thr:
                existing_note = subs[j].get("coverage_note", "")
                dup_note = f"Near‑duplicate of '{subs[i].get('subtopic_title')}' ({sim:.2f})"
                
                if existing_note:
                    subs[j]["coverage_note"] = f"{existing_note}; {dup_note}"
                else:
                    subs[j]["coverage_note"] = dup_note

def _title_allows_common_boiler(subtitle: str) -> bool:
    k = (subtitle or "").lower()
    return any(w in k for w in (
        "epidemiolog", "burden", "transmission", "prevention", "public", "sanitation"
    ))

def _dedupe_common_sentences(subs: list[dict]) -> None:
    """Remove high-frequency boilerplate sentences."""
    freq: dict[str, int] = {}
    sig = lambda s: re.sub(r"\W+", " ", s.lower()).strip()
    
    for sub in subs:
        for s in set(map(sig, _sentences(sub.get("concept", "")))):
            if s:
                freq[s] = freq.get(s, 0) + 1
    
    for sub in subs:
        if _title_allows_common_boiler(sub.get("subtopic_title", "")):
            continue
        
        kept = []
        for s in _sentences(sub.get("concept", "")):
            if freq.get(sig(s), 0) >= 3:
                continue
            kept.append(s)
        
        sub["concept"] = " ".join(kept) if kept else sub.get("concept", "")

def _all_subtopics_done(topic_id: str) -> bool:
    with pyodbc.connect(DB_CONN) as con:
        cur = con.cursor()
        cur.execute(f"""
            SELECT COUNT(*)
            FROM cme.subtopics
            WHERE topic_id = ?
            AND status NOT IN ({','.join('?'*len(DONE_STATES))});
        """, topic_id, *DONE_STATES)
        return cur.fetchone()[0] == 0

def _delay(msg: func.QueueMessage):
    QueueClient.from_connection_string(
        QUEUE_CONN_STR, PLAN_QUEUE_NAME
    ).update_message(
        msg.id, msg.pop_receipt,
        visibility_timeout=VISIBILITY_DELAY,
        content=msg.get_body().decode()
    )

def _all_done_with_cases(topic_id: str) -> bool:
    with pyodbc.connect(DB_CONN) as con:
        cur = con.cursor()
        cur.execute("""
            SELECT status, case_amenable, case_status
            FROM cme.subtopics WHERE topic_id=?""", topic_id)
        
        for status, amen, cstat in cur.fetchall():
            if status not in DONE_STATES:
                return False
            if amen and cstat not in ('verified', 'ready', 'failed', 'skipped'):
                return False
        return True

# ─────────────────── Build plan (flat, sorted by sequence_no) ──────────────────
def _build_plan(topic_id: str) -> tuple[str, list[dict]] | None:
    """
    Build the nested study-plan JSON as a flat subtopics list sorted by sequence_no.
    category is included as a plain field on each subtopic (like concept, title, etc.)

    Returns: (json_string, gaps_list) or None

    STRUCTURE:
    {
      "topic_id": "...",
      "topic_name": "...",
      "assembled_utc": "...",
      "subtopics": [
        {
          "subtopic_id": "...",
          "subtopic_title": "...",
          "sequence_no": 1,
          "category": "Some category name or null",
          "concept": "...",
          "references": [...],
          "questions": [...],
          "case_studies": [...],
          "content_status": "OK" or "Insufficient content",
          "coverage_note": "..."
        },
        ...
      ],
      "insufficient_subtopics": [...]
    }
    """
    with pyodbc.connect(DB_CONN) as con:
        cur = con.cursor()
        
        # Fetch topic
        cur.execute("SELECT topic_name FROM cme.topics WHERE topic_id = ?", topic_id)
        row = cur.fetchone()
        if not row:
            return None
        topic_name = row.topic_name
        
        # Fetch all subtopics sorted by sequence_no only
        cur.execute("""
            SELECT subtopic_id,
                   title AS subtopic_title,
                   sequence_no,
                   category
            FROM cme.subtopics
            WHERE topic_id = ?
            ORDER BY sequence_no, title;
        """, topic_id)
        
        subs = _dictfetch(cur)
        
        # Enrich each subtopic
        for sub in subs:
            sid = sub["subtopic_id"]
            
            # Concept text (latest by created_utc)
            try:
                cur.execute("""
                    SELECT TOP 1 content
                    FROM cme.concepts
                    WHERE subtopic_id = ?
                    ORDER BY created_utc DESC, concept_id
                """, sid)
            except Exception:
                cur.execute("""
                    SELECT TOP 1 content
                    FROM cme.concepts
                    WHERE subtopic_id = ?
                    ORDER BY concept_id
                """, sid)
            
            c_row = cur.fetchone()
            sub["concept"] = c_row.content if c_row else ""
            
            # Subtopic references
            cur.execute(SQL_SUBTOPIC_REFS, sid)
            sub["references"] = _dictfetch(cur)
            
            # Questions
            cur.execute("""
                SELECT q.question_id,
                       q.stem,
                       q.explanation,
                       q.correct_choice
                FROM cme.questions q
                WHERE q.subtopic_id = ?;
            """, sid)
            
            questions = _dictfetch(cur)
            
            for q in questions:
                qid = q["question_id"]
                
                # Choices
                cur.execute(SQL_CHOICES, qid)
                q["choices"] = _dictfetch(cur)
                
                # Resolve correct_choice_index
                wanted = (q["correct_choice"] or "").strip().lower()
                q["correct_choice_index"] = next(
                    (c["choice_index"]
                     for c in q["choices"]
                     if c["choice_text"].strip().lower() == wanted),
                    None
                )
                
                # Variants
                cur.execute("""
                    SELECT variant_no, stem, correct_choice_index
                    FROM cme.variants
                    WHERE question_id = ?
                    ORDER BY variant_no;
                """, qid)
                q["variants"] = _dictfetch(cur)
                
                # Question references
                cur.execute(SQL_QUESTION_REFS, qid)
                q["references"] = _dictfetch(cur)
            
            sub["questions"] = questions
            
            # Cases + case MCQs
            cur.execute("""
                SELECT case_id, title, vignette, learning_objective, word_count, verified
                FROM cme.cases WHERE subtopic_id = ? ORDER BY created_utc
            """, sid)
            
            cases = _dictfetch(cur)
            
            for cs in cases:
                cid = cs["case_id"]
                
                cur.execute("""
                    SELECT q.question_id, q.stem, q.explanation, q.correct_choice
                    FROM cme.questions q WHERE q.case_id = ?""", cid)
                
                cqs = _dictfetch(cur)
                
                for q in cqs:
                    qid = q["question_id"]
                    cur.execute("SELECT choice_index, choice_text, rationale FROM cme.choices WHERE question_id=? ORDER BY choice_index", qid)
                    q["choices"] = _dictfetch(cur)
                    cur.execute("SELECT variant_no, stem, correct_choice_index FROM cme.variants WHERE question_id=? ORDER BY variant_no", qid)
                    q["variants"] = _dictfetch(cur)
                
                cs["mcqs"] = cqs
            
            sub["case_studies"] = cases
            
            # Get DB status
            cur.execute("SELECT content_status, coverage_note FROM cme.subtopics WHERE subtopic_id=?", sid)
            status_row = cur.fetchone()
            db_status = ((status_row.content_status or "unknown") if status_row else "unknown").lower()
            db_note = (status_row.coverage_note if status_row else None)
            
            sub["coverage_note"] = db_note
        
        # Deduplication passes
        _dedupe_common_sentences(subs)
        _dedupe_near_duplicate_concepts(subs, ASSEMBLE_DUP_SIM_THRESHOLD)
        
        # Compute gaps and content status
        gaps = []
        for sub in subs:
            sid = sub["subtopic_id"]
            
            # Evidence-based insufficiency check
            if len((sub.get("concept") or "")) < EVAL_MIN_CONCEPT_CHARS:
                excerpts = [r.get("excerpt","") for r in sub.get("references", []) if r.get("excerpt")]
                if excerpts:
                    stitched = " ".join(excerpts)[:max(EVAL_MIN_CONCEPT_CHARS, 600)]
                    if len(stitched) >= EVAL_MIN_CONCEPT_CHARS:
                        sub["concept"] = stitched
            
            has_refs = len(sub.get("references", [])) > 0
            has_concept = len((sub.get("concept") or "")) >= EVAL_MIN_CONCEPT_CHARS
            has_mcqs = len(sub.get("questions", [])) > 0 if EVAL_REQUIRE_MCQS else True
            ev_ok = has_refs and has_concept and has_mcqs
            
            # Get DB status
            cur.execute("SELECT content_status FROM cme.subtopics WHERE subtopic_id=?", sid)
            status_row = cur.fetchone()
            db_status = ((status_row.content_status or "unknown") if status_row else "unknown").lower()
            
            content_status = "ok" if (db_status == "ok" and ev_ok) else ("insufficient" if not ev_ok else db_status)
            
            reasons = []
            if not has_refs:
                reasons.append("No references")
            if not has_concept:
                reasons.append("Short/missing concept")
            if not has_mcqs:
                reasons.append("No MCQs")
            
            # Combine notes
            coverage_note = sub.get("coverage_note", "")
            reason = coverage_note or ("; ".join(reasons) if reasons else None)
            
            sub["content_status"] = "OK" if content_status == "ok" else "Insufficient content"
            
            if content_status != "ok":
                gaps.append({
                    "subtopic_id": sid,
                    "title": sub.get("subtopic_title"),
                    "category": sub.get("category"),
                    "reason": reason or "Low coverage"
                })
        
        # Assemble final plan — flat subtopics list sorted by sequence_no
        # category is a plain field on each subtopic, same as concept, title, etc.
        plan = {
            "topic_id": topic_id,
            "topic_name": topic_name,
            "assembled_utc": dt.datetime.utcnow().isoformat(timespec="seconds"),
            "subtopics": subs,
            "insufficient_subtopics": gaps,
        }
        
        return json.dumps(plan, ensure_ascii=False), gaps

# ─────────────────── Azure Function entry point ───────────────────────────
def main(msg: func.QueueMessage):
    logging.info("assemblePlan triggered")
    
    try:
        topic_id = json.loads(msg.get_body().decode())["topic_id"]
    except Exception:
        logging.error("Bad queue payload – expected {'topic_id': …}")
        return
    
    built = _build_plan(topic_id)
    if built is None:
        logging.error("Cannot build plan for topic %s (missing data)", topic_id)
        return
    
    plan_json, gaps = built
    now = dt.datetime.utcnow()
    
    with pyodbc.connect(DB_CONN) as con:
        cur = con.cursor()
        cur.execute("""
            MERGE cme.study_plans AS tgt
            USING (SELECT ? AS tid) AS src
            ON tgt.topic_id = src.tid
            WHEN MATCHED THEN
                UPDATE SET assembled_utc = ?, plan_json = ?
            WHEN NOT MATCHED THEN
                INSERT (topic_id, assembled_utc, plan_json)
                VALUES (src.tid, ?, ?);
        """, topic_id, now, plan_json, now, plan_json)
        
        cur.execute("DELETE FROM cme.content_gaps WHERE topic_id = ?", topic_id)
        
        for g in gaps:
            cur.execute("""
                INSERT INTO cme.content_gaps (topic_id, subtopic_id, subtopic_title, coverage_score, reason)
                SELECT ?, s.subtopic_id, s.title, ISNULL(s.coverage_score, 0), ?
                FROM cme.subtopics s WHERE s.subtopic_id = ?;
            """, topic_id, g.get("reason"), g["subtopic_id"])
        
        con.commit()
    
    logging.info("✓ study_plan stored for %s (%d bytes, %d subtopics)",
                 topic_id, len(plan_json), len(json.loads(plan_json)["subtopics"]))