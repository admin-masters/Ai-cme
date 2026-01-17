from __future__ import annotations
import logging, os, uuid, json, re, unicodedata
from datetime import datetime as dt
import azure.functions as func  # ⬅ Azure-Functions runtime
import pyodbc  # ⬅ ODBC to Azure SQL

DB_CONN = "DRIVER={ODBC Driver 18 for SQL Server};SERVER=20.171.24.17;DATABASE=CME2;UID=new_root;PWD=japl@bJBYV77;Encrypt=no;TrustServerCertificate=yes;"

AZURE_OAI_KEY = "CzrrWvXbsmYcNguU1SqBpE9HDhhbfYsbkq3UedythCYCV9zNQ4mLJQQJ99BEACHYHv6XJ3w3AAABACOGiIPm"

# ─────────────────────────────── Taxonomy seeds ────────────────────────────
TAXONOMY = {
    "infectious": [
        "Burden & epidemiology",
        "Causative organism & pathogenesis",
        "Clinical presentation & red-flag signs",
        "Initial triage & admission/escalation criteria",
        "Investigations & diagnostic pitfalls",
        "Laboratory confirmation: blood/stool culture technique",
        "First-line antimicrobial therapy (pediatric dosing)",
        "MDR/XDR pediatric management protocols",
        "Persistent fever (day 3–5) & relapse algorithm",
        "Complications & their acute rescue",
        "Vaccines (TCV): indications & schedules",
        "Follow-up & defervescence expectations",
        "Carrier state: detection & clearance",
        "Household contacts & outbreak control",
        "Counselling on sanitation & prevention",
        "Differential diagnosis of pediatric febrile illness (malaria, dengue, leptospirosis, scrub typhus, UTI, appendicitis)"
    ],
    "chronic": [
        "Etiology & pathophysiology",
        "Diagnostic criteria",
        "Long-term pharmacologic management",
        "Acute exacerbation protocol",
        "Monitoring & follow-up",
        "Complications & comorbidities",
        "Transition to adult care",
        "Patient & family counselling"
    ],
    "growth": [
        "Normal milestones overview",
        "Assessment tools & charts",
        "Red flags for developmental delay",
        "Common causes of delay",
        "Early-intervention strategies",
        "Parental guidance & counselling"
    ],
    "nutrition": [
        "Physiologic nutrient requirements",
        "Breastfeeding & substitutes",
        "Complementary feeding schedule",
        "Micronutrient deficiencies",
        "Growth monitoring",
        "Counselling & government programmes"
    ],
    "emergency": [
        "Etiology / common triggers",
        "Pathophysiology",
        "Recognition & triage",
        "Initial stabilisation (ABCDE)",
        "Definitive management",
        "Complications & monitoring",
        "Discharge criteria & follow-up"
    ],
    "procedure": [
        "Indication",
        "Contra-indication",
        "Equipment checklist",
        "Step-by-step technique",
        "Complications",
        "Post-procedure care",
        "Counselling & consent"
    ]
}

FAMILY_KEYWORDS = {
    "infectious": r"(fever|virus|bacter|infect|vaccine|immuni|typhoid|measles|flu)",
    "growth": r"(growth|milestone|development|pubert)",
    "nutrition": r"(nutrition|feeding|diet|breast|formula|vitamin)",
    "emergency": r"(shock|status|burn|poison|arrest|seizure)",
    "procedure": r"(catheter|intubation|lumbar puncture|line |injection)",
}

# ─────────────────────────────── Helpers ───────────────────────────────────
def _norm(s: str) -> str:
    return unicodedata.normalize("NFKD", s).encode("ascii", "ignore").decode().lower()

def guess_family(topic: str) -> str:
    low = _norm(topic)
    for fam, pat in FAMILY_KEYWORDS.items():
        if re.search(pat, low):
            return fam
    return "chronic"

# ────────────────────────────── Main entry ─────────────────────────────────
def main(req: func.HttpRequest) -> func.HttpResponse:
    """
    POST /api/enqueueTopic?topic=Typhoid+Fever
    Body (optional JSON): { "topic": "Typhoid Fever" }
    Response 201:
    { "topic_id": "...", "seeded_subtopics": 9 }
    """
    logging.info("enqueueTopic called")
    
    topic_name = req.params.get("topic")
    if not topic_name:
        try:
            topic_name = req.get_json().get("topic")
        except Exception:
            pass
    
    if not topic_name:
        return func.HttpResponse(
            "Supply 'topic' as query-string or JSON.",
            status_code=400,
        )
    
    topic_id = str(uuid.uuid4())
    utc_now = dt.utcnow()
    family = guess_family(topic_name)
    placeholder_n = int(os.getenv("MIN_SUBTOPICS", "22"))
    seed_subs = [f"Placeholder {i}" for i in range(1, placeholder_n + 1)]
    
    # DB connection string stored **once** in App Settings / Key Vault
    try:
        with pyodbc.connect(DB_CONN) as conn:
            with conn.cursor() as cur:
                # ① insert topic
                cur.execute(
                    """
                    INSERT INTO cme.topics (topic_id, topic_name, created_utc)
                    VALUES (?, ?, ?)
                    """,
                    topic_id, topic_name, utc_now
                )
                
                # ② seed sub-topics in 'queued' state
                for seq, title in enumerate(seed_subs, start=1):
                    cur.execute(
                        """
                        INSERT INTO cme.subtopics
                        (subtopic_id, topic_id, title, sequence_no, status)
                        VALUES
                        (?, ?, ?, ?, 'queued')
                        """,
                        str(uuid.uuid4()), topic_id, title, seq
                    )
                conn.commit()
        
        from azure.storage.queue import QueueClient
        qc = QueueClient.from_connection_string(
            os.environ["AzureWebJobsStorage"],
            "topic-queue"
        )
        qc.send_message(json.dumps({"topic_id": topic_id}))
        
    except Exception as exc:
        logging.exception("DB insert failed")
        return func.HttpResponse(f"Database error: {exc}", status_code=500)
    
    payload = {"topic_id": topic_id, "seeded_subtopics": len(seed_subs)}
    return func.HttpResponse(
        json.dumps(payload),
        mimetype="application/json",
        status_code=201
    )