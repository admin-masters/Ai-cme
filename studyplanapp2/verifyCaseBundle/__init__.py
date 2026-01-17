# studyplan-pipeline/verifyCaseBundle/__init__.py
from __future__ import annotations
import json, logging, os, uuid
import azure.functions as func
import pyodbc
from openai import AzureOpenAI
from azure.storage.queue import QueueClient

DB = "DRIVER={ODBC Driver 18 for SQL Server};SERVER=20.171.24.17;DATABASE=CME2;UID=new_root;PWD=japl@bJBYV77;Encrypt=no;TrustServerCertificate=yes;"
AZURE_OAI_ENDPOINT = "https://azure1405.openai.azure.com/"
AZURE_OAI_KEY = "CzrrWvXbsmYcNguU1SqBpE9HDhhbfYsbkq3UedythCYCV9zNQ4mLJQQJ99BEACHYHv6XJ3w3AAABACOGiIPm"
DEPLOYMENT = "gpt-4o"
AZURE_OAI_API_VERSION = "2024-02-15-preview"

# ─────────────────── Azure-OpenAI client ────────────────────
oai = AzureOpenAI(
    api_key=AZURE_OAI_KEY,
    azure_endpoint=AZURE_OAI_ENDPOINT,
    api_version=AZURE_OAI_API_VERSION,
)

DEPLOYMENT = os.environ.get("DEPLOYMENT", "gpt-4o")

def main(msg: func.QueueMessage) -> None:
    logging.info("verifyCaseBundle triggered")
    try:
        case_id = json.loads(msg.get_body().decode())["case_id"]
    except Exception:
        logging.error("Bad payload")
        return
    
    with pyodbc.connect(DB) as con:
        cur = con.cursor()
        cur.execute("""
            SELECT cs.subtopic_id, t.topic_id, t.topic_name, s.title, cs.vignette
            FROM cme.cases cs
            JOIN cme.subtopics s ON s.subtopic_id = cs.subtopic_id
            JOIN cme.topics t ON t.topic_id = s.topic_id
            WHERE cs.case_id = ?""", case_id)
        row = cur.fetchone()
        if not row: 
            return
        
        sub_id, topic_id, topic_name, sub_title, vignette = row
        
        # verifyCaseBundle/__init__.py (safe patch)
        cur.execute("""
            SELECT TOP 1 content
            FROM cme.concepts
            WHERE subtopic_id=? ORDER BY concept_id
        """, sub_id)
        row = cur.fetchone()
        concept = (row.content if row else "")[:2500]
        
        # gather linked MCQs
        cur.execute("""
            SELECT q.question_id, q.stem, q.correct_choice
            FROM cme.questions q WHERE q.case_id = ?
        """, case_id)
        qs = cur.fetchall() or []  # ← guard: no MCQs yet
        
        bundle = []
        for q in qs:
            qid, stem, correct = q
            cur.execute("""
                SELECT choice_index, choice_text, ISNULL(rationale, '') AS rationale
                FROM cme.choices
                WHERE question_id=? ORDER BY choice_index
            """, qid)
            ch = [{"choice_index": r[0], "choice_text": r[1], "rationale": r[2]} for r in (cur.fetchall() or [])]
            bundle.append({"question_id": qid, "stem": stem, "correct_choice": correct, "choices": ch})
        
        ask = {
            "role": "user",
            "content": f"""
You are a paediatrics QA checker. Verify the CASE MCQs against the CASE and CONCEPT.
Return JSON:
{{
"verdict": "pass" | "fail",
"issues": [ "short bullet ..." ],
"suggested_fixes": [{{ "question_id": "uuid", "stem": "new (optional)", "rationales": ["...","...","...","..."] }}]
}}
Rules:
- Each MCQ must be answerable solely from CASE text; do not require external facts.
- Correct choice must be unambiguous; rationales must be consistent and non-contradictory.
- Flag if vignette length is outside 100–200 words or contains diagnosis/management spoilers.
CASE:
{vignette}
CONCEPT (guardrails only):
{concept}
MCQS:
{json.dumps(bundle, ensure_ascii=False)}
""".strip()
        }
        
        rsp = oai.chat.completions.create(
            model=DEPLOYMENT,
            messages=[{"role": "system", "content": "Return JSON only."}, ask],
            temperature=0.2, max_tokens=600, response_format={"type": "json_object"},
        )
        
        out = json.loads(rsp.choices[0].message.content)
        verdict = (out.get("verdict") or "fail").lower()
        
        with pyodbc.connect(DB) as con:
            cur = con.cursor()
            cur.execute("""
                INSERT INTO cme.qa_reviews (qa_id, entity_type, entity_id, status, issues, suggested_fix)
                VALUES (?, 'case', ?, ?, ?, ?)""",
                str(uuid.uuid4()), case_id, verdict, json.dumps(out.get("issues", []), ensure_ascii=False),
                json.dumps(out.get("suggested_fixes", []), ensure_ascii=False))
            
            cur.execute("""UPDATE cme.cases SET verified = ? , qa_summary = ? WHERE case_id = ?""",
                1 if verdict == "pass" else 0,
                json.dumps(out, ensure_ascii=False),
                case_id)
            
            # propagate aggregated subtopic case_status (supports multiple cases per subtopic)
            # 1) if any cases under this subtopic still lack MCQs -> keep pending
            cur.execute("""
                SELECT COUNT(*)
                FROM cme.cases cs
                WHERE cs.subtopic_id=?
                  AND NOT EXISTS (SELECT 1 FROM cme.questions q WHERE q.case_id = cs.case_id)
            """, sub_id)
            remaining = (cur.fetchone()[0] or 0)

            if remaining > 0:
                new_status = 'pending'
            else:
                # 2) all cases have MCQs; summarise verification state
                cur.execute("""
                    SELECT
                        SUM(CASE WHEN cs.verified=1 THEN 1 ELSE 0 END) AS pass_n,
                        SUM(CASE WHEN cs.verified=0 THEN 1 ELSE 0 END) AS fail_n,
                        SUM(CASE WHEN cs.verified IS NULL THEN 1 ELSE 0 END) AS null_n,
                        COUNT(*) AS total_n
                    FROM cme.cases cs
                    WHERE cs.subtopic_id=?
                """, sub_id)
                pass_n, fail_n, null_n, total_n = cur.fetchone()
                pass_n = pass_n or 0
                fail_n = fail_n or 0
                null_n = null_n or 0
                total_n = total_n or 0

                if fail_n > 0:
                    new_status = 'failed'
                elif total_n > 0 and pass_n == total_n:
                    new_status = 'verified'
                else:
                    # MCQs exist for all cases, but at least one case is not yet verified
                    new_status = 'ready'

            cur.execute("""UPDATE cme.subtopics SET case_status = ? WHERE subtopic_id = ?""", new_status, sub_id)
            con.commit()
        
        # nudge plan assembler
        try:
            q = QueueClient.from_connection_string(os.environ["AzureWebJobsStorage"], "plan-queue")
            q.send_message(json.dumps({"topic_id": topic_id}))
        except Exception: 
            pass