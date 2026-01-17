# studyplan-pipeline/generateCase/__init__.py
from __future__ import annotations
import json, logging, os, re, uuid
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

def _wc(s: str) -> int:
    return len(re.findall(r"\b\w+\b", s or ""))

def main(msg: func.QueueMessage) -> None:
    logging.info("generateCase triggered")
    
    try:
        subtopic_id = json.loads(msg.get_body().decode())["subtopic_id"]
    except Exception:
        logging.error("Bad queue payload – expected {'subtopic_id': ...}")
        return

    with pyodbc.connect(DB) as con:
        cur = con.cursor()
        cur.execute("SELECT case_status FROM cme.subtopics WHERE subtopic_id=?", subtopic_id)
        cstat = (cur.fetchone()[0] or "").lower()
        if cstat != "pending":
            logging.info("Case generation skipped: subtopic %s is no longer pending (status=%s)",
                        subtopic_id, cstat)
            return
        cur.execute("""
            SELECT t.topic_name, s.title, s.case_amenable
            FROM cme.subtopics s
            JOIN cme.topics t ON t.topic_id = s.topic_id
            WHERE s.subtopic_id = ?""", subtopic_id)
        
        row = cur.fetchone()
        if not row:
            logging.error("Subtopic not found")
            return
        
        topic, sub, caseable = row

        # Pull concept text as grounding (short)
        cur.execute("""
            SELECT TOP 1 content
            FROM cme.concepts
            WHERE subtopic_id=?
            ORDER BY concept_id""", subtopic_id)
        
        crow = cur.fetchone()
        concept = (crow.content if crow else "")[:1800]

        # If vignette cases are already ingested for this subtopic, do NOT generate a new case.
        # Instead, queue case-MCQ generation for any case rows missing MCQs.
        cur.execute("""
            SELECT cs.case_id
            FROM cme.cases cs
            WHERE cs.subtopic_id=?
              AND NOT EXISTS (SELECT 1 FROM cme.questions q WHERE q.case_id = cs.case_id)
            ORDER BY cs.created_utc
        """, subtopic_id)
        existing_missing = [r.case_id for r in (cur.fetchall() or [])]

        cur.execute("SELECT COUNT(*) FROM cme.cases WHERE subtopic_id=?", subtopic_id)
        existing_total = cur.fetchone()[0] or 0

    if existing_total > 0:
        logging.info("Subtopic %s already has %d case(s); skipping new case generation", subtopic_id, existing_total)
        if existing_missing:
            try:
                q = QueueClient.from_connection_string(os.environ["AzureWebJobsStorage"], "case-mcq-queue")
                for cid in existing_missing:
                    q.send_message(json.dumps({"case_id": cid}))
                logging.info("Queued %d existing case(s) for case-MCQ generation", len(existing_missing))
            except Exception as e:
                logging.error("Queue push failed for existing cases: %s", e)
        else:
            logging.info("All existing cases already have MCQs; nothing to do")
        return

    if not caseable:
        logging.info("Subtopic marked non-caseable; skipping")
        return

    prompt = f"""
Create ONE realistic clinical vignette for paediatrics under:
• Topic: {topic}
• Sub-topic: {sub}

Constraints:
• 100–200 words. Realistic India/LMIC context if relevant.
• Include age/setting, time course, key symptoms, focused exam, and 0–2 objective data (e.g., vitals or one key lab).
• Do NOT include the diagnosis or management in the vignette text.
• Must be answerable from the sub-topic's concept below (no new facts).
• Provide a short learning objective (≤20 words).
• Prefer situations that test triage/admission thresholds, persistent fever on day 3–5 of therapy, or acute complications (e.g., GI bleed, ileal perforation, encephalopathy) when relevant to the subtopic.

Return JSON with fields:
{{
    "title": "string",
    "vignette": "string",
    "learning_objective": "string"
}}

Concept (context only):
{concept}
""".strip()

    rsp = oai.chat.completions.create(
        model=DEPLOYMENT,
        messages=[{"role": "system", "content": "You are a paediatrics case writer."},
                 {"role": "user", "content": prompt}],
        temperature=0.5,
        max_tokens=400,
        response_format={"type": "json_object"},
    )

    data = json.loads(rsp.choices[0].message.content)
    title = (data.get("title") or "").strip()[:255] or sub
    vignette = (data.get("vignette") or "").strip()
    lo = (data.get("learning_objective") or "").strip()[:255]

    wc = _wc(vignette)
    if wc < 100 or wc > 220:
        logging.warning("Vignette word count %d out of range; proceeding but will be verified", wc)

    case_id = str(uuid.uuid4())

    with pyodbc.connect(DB) as con:
        cur = con.cursor()
        cur.execute("""
            INSERT INTO cme.cases
            (case_id, subtopic_id, title, vignette, word_count, learning_objective)
            VALUES (?, ?, ?, ?, ?, ?)""",
            case_id, subtopic_id, title, vignette, wc, lo)
        
        cur.execute("""
            UPDATE cme.subtopics
            SET case_status='pending'
            WHERE subtopic_id=?""", subtopic_id)
        
        con.commit()

    try:
        q = QueueClient.from_connection_string(
            os.environ["AzureWebJobsStorage"], "case-mcq-queue")
        q.send_message(json.dumps({"case_id": case_id}))
    except Exception as e:
        logging.error("Queue push failed: %s", e)