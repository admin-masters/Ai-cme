# studyplan-pipeline/generateCaseMcq/__init__.py
from __future__ import annotations
import json, logging, os, uuid, textwrap, random
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

def _save_case_mcq(cur, case_id: str, sub_id: str, block: dict):
    qid = str(uuid.uuid4())
    cur.execute("""
        INSERT INTO cme.questions (question_id, subtopic_id, case_id, stem, correct_choice, explanation)
        VALUES (?, ?, ?, ?, ?, ?)""",
        qid, sub_id, case_id, block["stem"],
        block["choices"][block["correct_index"]],
        block.get("explanation", ""))
    
    for i, choice in enumerate(block["choices"]):
        rationale = (block.get("rationales") or [""] * 4)[i]
        cur.execute("""
            INSERT INTO cme.choices (choice_id, question_id, choice_index, choice_text, rationale)
            VALUES (NEWID(), ?, ?, ?, ?)""", qid, i, choice, rationale)
    
    # reuse existing variants shape
    for v_no, vn in enumerate(("variant1", "variant2"), start=1):
        if vn in block and str(block[vn].get("stem", "")).strip():
            cur.execute("""
                INSERT INTO cme.variants (variant_id, question_id, variant_no, stem, correct_choice_index)
                VALUES (NEWID(), ?, ?, ?, ?)""",
                qid, v_no, block[vn]["stem"], block[vn]["correct_index"])
    
    # link question→same refs as sub-topic
    cur.execute("""
        INSERT INTO cme.question_references (question_id, reference_id)
        SELECT ?, reference_id FROM cme.subtopic_references WHERE subtopic_id=?""",
        qid, sub_id)

def main(msg: func.QueueMessage) -> None:
    logging.info("generateCaseMcq triggered")
    try:
        case_id = json.loads(msg.get_body().decode())["case_id"]
    except Exception:
        logging.error("Bad payload")
        return
    
    with pyodbc.connect(DB) as con:
        cur = con.cursor()
        cur.execute("""
            SELECT cs.subtopic_id, t.topic_name, s.title, cs.vignette
            FROM cme.cases cs
            JOIN cme.subtopics s ON s.subtopic_id = cs.subtopic_id
            JOIN cme.topics t ON t.topic_id = s.topic_id
            WHERE cs.case_id = ?""", case_id)
        row = cur.fetchone()
        if not row:
            logging.error("Case not found")
            return
        
        sub_id, topic, sub, vignette = row
        cur.execute("""SELECT TOP 1 content FROM cme.concepts WHERE subtopic_id=? ORDER BY concept_id""", sub_id)
        crow = cur.fetchone()
        concept = (crow.content if crow else "")[:2000]

        # If MCQs already exist for this case, avoid duplicating
        cur.execute("SELECT COUNT(*) FROM cme.questions WHERE case_id=?", case_id)
        existing_q = (cur.fetchone()[0] or 0)
        if existing_q > 0:
            # update subtopic case_status if all cases already have MCQs
            cur.execute("""
                SELECT COUNT(*)
                FROM cme.cases cs
                WHERE cs.subtopic_id=?
                  AND NOT EXISTS (SELECT 1 FROM cme.questions q WHERE q.case_id = cs.case_id)
            """, sub_id)
            remaining = (cur.fetchone()[0] or 0)
            if remaining == 0:
                cur.execute("""
                    UPDATE cme.subtopics
                    SET case_status = CASE
                        WHEN case_status IN ('verified','failed','skipped') THEN case_status
                        ELSE 'ready'
                    END
                    WHERE subtopic_id=?
                """, sub_id)
                con.commit()
            logging.info("Case MCQs already exist for %s (%d question(s)); skipping regeneration", case_id, existing_q)
            return
        
        schema = json.dumps({
            "mcqs": [{
                "stem": "string",
                "choices": ["string", "string", "string", "string"],
                "rationales": ["string", "string", "string", "string"],
                "correct_index": "int 0-3",
                "explanation": "string",
                "variant1": {"stem": "string", "correct_index": "int"},
                "variant2": {"stem": "string", "correct_index": "int"}
            }]
        }, indent=2)
        
        prompt = textwrap.dedent(f"""
            Create 1–2 single-best-answer MCQs *from the CASE ONLY* (no outside facts).
            Topic: {topic}
            Sub-topic: {sub}
            CASE
            ────
            {vignette}
            Rules
            ──
            1) 4 choices (≤6 words each) + 4 rationales (≤35 words each).
            2) If data are needed, they must be inferable from the case.
            3) Each item must be answerable and unambiguous.
            4) Provide 2 paraphrased stems (variant1, variant2). Correct choice may change.
            5) Prefer management/application questions (admit vs observe, next step on day‑5 persistent fever, rescue for complication) over single‑fact recall.
            Return JSON only, shape exactly:
            {schema}
            Concept (for guardrails; do not introduce new facts):
            {concept}
        """).strip()
        
        rsp = oai.chat.completions.create(
            model=DEPLOYMENT,
            messages=[{"role": "system", "content": "You are a paediatrics examiner."},
                     {"role": "user", "content": prompt}],
            temperature=0.45, max_tokens=900, response_format={"type": "json_object"},
        )
        
        data = json.loads(rsp.choices[0].message.content)
        blocks = data.get("mcqs") or []
        if not isinstance(blocks, list) or not blocks:
            logging.error("No MCQs returned for case %s", case_id)
            return
        
        # simple validation
        for i, b in enumerate(blocks, 1):
            assert len(b.get("choices", [])) == 4, f"MCQ {i}: need 4 choices"
            assert len(b.get("rationales", [])) == 4, f"MCQ {i}: need 4 rationales"
            assert b.get("correct_index") in (0, 1, 2, 3), f"MCQ {i}: bad correct_index"
            
            # shuffle choices to reduce patterning
            original = b["choices"][:]
            correct_text = original[b["correct_index"]]
            combined = list(zip(b["choices"], b["rationales"]))
            random.shuffle(combined)
            b["choices"], b["rationales"] = [c for c, _ in combined], [r for _, r in combined]
            b["correct_index"] = b["choices"].index(correct_text)
            
            # Remap variant indices to the new order using TEXT from pre-shuffle choices
            for vn in ("variant1", "variant2"):
                if vn in b and isinstance(b[vn], dict):
                    try:
                        old_idx = b[vn]["correct_index"]
                        # The correct text for the variant in the *original* order
                        variant_corr_text = original[old_idx]
                    except Exception:
                        # Fallback to stem's correct text if variant index was bad
                        variant_corr_text = correct_text
                    # New index in the shuffled order
                    b[vn]["correct_index"] = b["choices"].index(variant_corr_text)
        
        with pyodbc.connect(DB) as con:
            cur = con.cursor()
            for b in blocks:
                _save_case_mcq(cur, case_id, sub_id, b)
            # Update subtopic case_status only when ALL cases under this subtopic have MCQs
            cur.execute("""
                SELECT COUNT(*)
                FROM cme.cases cs
                WHERE cs.subtopic_id=?
                  AND NOT EXISTS (SELECT 1 FROM cme.questions q WHERE q.case_id = cs.case_id)
            """, sub_id)
            remaining = (cur.fetchone()[0] or 0)
            if remaining == 0:
                cur.execute("""
                    UPDATE cme.subtopics
                    SET case_status = CASE
                        WHEN case_status IN ('verified','failed','skipped') THEN case_status
                        ELSE 'ready'
                    END
                    WHERE subtopic_id=?
                """, sub_id)
            else:
                cur.execute("""
                    UPDATE cme.subtopics
                    SET case_status = CASE
                        WHEN case_status IN ('verified','failed','skipped') THEN case_status
                        ELSE 'pending'
                    END
                    WHERE subtopic_id=?
                """, sub_id)
            con.commit()
        
        # hand-off to verification
        try:
            q = QueueClient.from_connection_string(os.environ["AzureWebJobsStorage"], "verify-queue")
            q.send_message(json.dumps({"case_id": case_id}))
        except Exception as e:
            logging.error("Queue push failed: %s", e)