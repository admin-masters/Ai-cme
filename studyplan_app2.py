# studyplan_app.py  – LOCAL SQL · Azure Search RAG · Azure OpenAI ·
#                     interactive console demo with DB-backed references
###############################################################################
from __future__ import annotations
import os, json, logging, argparse, re, textwrap
import pyodbc, torch

# ── Azure OpenAI ────────────────────────────────────────────────────────────
from openai import AzureOpenAI
AZURE_OAI_ENDPOINT   = os.getenv("AZURE_OAI_ENDPOINT") or "https://azure1405.openai.azure.com/"
AZURE_OAI_KEY        = os.getenv("AZURE_OAI_KEY") or "CzrrWvXbsmYcNguU1SqBpE9HDhhbfYsbkq3UedythCYCV9zNQ4mLJQQJ99BEACHYHv6XJ3w3AAABACOGiIPm"
AZURE_OAI_DEPLOYMENT = os.getenv("AZURE_OAI_DEPLOYMENT") or "gpt-4o"
AZURE_OAI_API_VER    = os.getenv("AZURE_OAI_API_VER")    or "2024-02-15-preview"
oai = AzureOpenAI(api_key=AZURE_OAI_KEY,
                  azure_endpoint=AZURE_OAI_ENDPOINT,
                  api_version=AZURE_OAI_API_VER)

# ── Local SQL Server ───────────────────────────────────────────────────────
DB_CONN = (
    "Driver={ODBC Driver 18 for SQL Server};"
    "Server=localhost\\MSSQLSERVER03;"
    "Database=CME;"
    "Trusted_Connection=yes;"
    "Encrypt=no;TrustServerCertificate=yes"
)

# ── (optional) Azure Cognitive Search RAG client ───────────────────────────
from azure.core.credentials import AzureKeyCredential
from azure.search.documents import SearchClient
SEARCH_ENDPOINT      = "https://basic-rag-sandbox.search.windows.net"
SEARCH_ADMIN_KEY     = "tuqRZ8A374Aw3wXKSTzOY6SEu6Ra8rOyhPgFEtcLpSAzSeBOByQL"
SEARCH_INDEX_NAME    = os.getenv("SEARCH_INDEX_NAME")   or "pubert-demo-new"
SEARCH_API_VERSION   = os.getenv("SEARCH_API_VERSION")  or "2025-05-01-preview"
SEARCH_VECTOR_FIELD  = os.getenv("SEARCH_VECTOR_FIELD") or "embedding"

# ── local embedding model (only needed if RAG is used) ─────────────────────
from transformers import AutoTokenizer, AutoModel
tokenizer = AutoTokenizer.from_pretrained(
    "microsoft/BiomedNLP-PubMedBERT-base-uncased-abstract")
model     = AutoModel.from_pretrained(
    "microsoft/BiomedNLP-PubMedBERT-base-uncased-abstract")
model.eval()

###############################################################################
# 1) helpers – AOAI + JSON coercion
###############################################################################
def _safe_json(raw: str) -> dict | None:
    try:
        return json.loads(raw)
    except json.JSONDecodeError:
        m = re.search(r"\{.*\}", raw, re.S)
        return json.loads(m.group(0)) if m else None

def call_aoai(prompt: str,
              system_message: str = "You are a helpful medical tutor.") -> dict:
    rsp = oai.chat.completions.create(
        model           = AZURE_OAI_DEPLOYMENT,
        temperature     = 0.7,
        max_tokens      = 1024,
        response_format = {"type": "json_object"},
        messages=[{"role": "system", "content": system_message},
                  {"role": "user",   "content": prompt}]
    )
    data = _safe_json(rsp.choices[0].message.content)
    if data is None:
        raise ValueError("Azure OpenAI did not return JSON")
    return data

###############################################################################
# 2) (optional) RAG – unchanged
###############################################################################
class CognitiveSearchRagStore:
    def __init__(self, endpoint, key, index, vec_field, api_ver):
        self.cli = SearchClient(endpoint, index,
                                AzureKeyCredential(key),
                                api_version=api_ver)
        self.vec_field = vec_field
    def _embed(self, text: str) -> list[float]:
        with torch.no_grad():
            return model(**tokenizer(text, return_tensors="pt")
                         ).last_hidden_state[:, 0, :].squeeze(0).tolist()
    def get_content_for_topic(self, topic: str, k: int = 3) -> str | None:
        vec = self._embed(topic)
        res = self.cli.search(
            search_text=None,
            vectors=[{"value": vec, "fields": self.vec_field, "k": k}],
            top=k, include_total_count=False)
        docs = list(res)
        return docs[0]["content"] if docs else None

###############################################################################
# 3) study-plan store (unchanged)
###############################################################################
class PlanStoreSQL:
    def __init__(self, conn: str): self.conn = conn
    def _cx(self): return pyodbc.connect(self.conn, autocommit=False)
    def get_random_plan(self, topic_name: str) -> dict | None:
        with self._cx() as con:
            row = con.cursor().execute("""
SELECT TOP 1 plan_json
FROM   cme.study_plans sp
JOIN   cme.topics      t ON t.topic_id = sp.topic_id
WHERE  t.topic_name = ? ORDER BY NEWID();
""", topic_name).fetchone()
            return json.loads(row.plan_json) if row else None

###############################################################################
# 4) LearningScenarioAgent – variant / skip logic + DB-backed references
###############################################################################
class LearningScenarioAgent:
    def __init__(self, db_conn: str):
        self.db_conn = db_conn

    # ---------- reference helpers -----------------------------------------
    def _get_refs_for_subtopic(self, sub_id):
        sql = """
SELECT r.source_id, r.citation_link, r.excerpt
FROM   [cme].[references]          r
JOIN   cme.subtopic_references s ON s.reference_id = r.reference_id
WHERE  s.subtopic_id = ?
"""
        with pyodbc.connect(self.db_conn) as con:
            cur = con.cursor().execute(sql, sub_id)
            cols = [c[0] for c in cur.description]
            return [dict(zip(cols, row)) for row in cur.fetchall()]

    def _get_refs_for_question(self, q_id):
        sql = """
SELECT r.source_id, r.citation_link, r.excerpt
FROM   [cme].[references]          r
JOIN   cme.question_references q ON q.reference_id = r.reference_id
WHERE  q.question_id = ?
"""
        with pyodbc.connect(self.db_conn) as con:
            cur = con.cursor().execute(sql, q_id)
            cols = [c[0] for c in cur.description]
            return [dict(zip(cols, row)) for row in cur.fetchall()]

    # ---------- answer helper ---------------------------------------------
    def _correct_choice_index(self, q):
        if q.get("variants"):
            return q["variants"][0]["correct_choice_index"]

        qid = q["question_id"]
        with pyodbc.connect(self.db_conn) as con:
            row = con.cursor().execute(
                "SELECT correct_choice FROM cme.questions WHERE question_id = ?",
                qid).fetchone()

        if not row:
            return None
        correct_text = row.correct_choice.strip().lower()
        for c in q["choices"]:
            if c["choice_text"].strip().lower() == correct_text:
                return c["choice_index"]
        return None

    # ---------- interactive helpers ---------------------------------------
    @staticmethod
    def _print_refs(refs):
        if not refs:
            print("   [no references]")
            return
        for i, r in enumerate(refs, 1):
            link = r["citation_link"] or r["source_id"]
            excerpt = textwrap.shorten(r["excerpt"], 140, placeholder=" …")
            print(f"   {i}. {link}\n      {excerpt}")

    @staticmethod
    def _ask_variant(stem, choices):
        print("\n" + stem)
        for c in choices:
            label = chr(ord('A') + c["choice_index"])
            print(f"{label}) {c['choice_text']}")
        return input("Your answer (A/B/C/D – enter to skip): ").strip().upper()

    # ---------- main scenario runner --------------------------------------
    def run_scenario(self, plan, sess):
        print("\n=== Interactive Session Started ===")
        sess.update(correct=0, wrong=0, unanswered=0, answered=0)

        for s_no, sub in enumerate(plan.get("subtopics", []), 1):
            # <-  FIX: accept both 'concept' and 'concept_text'
            concept = (
                sub.get("concept") or
                sub.get("concept_text") or
                self._fetch_concept_from_rag(sub)
            )
            if not concept or concept.strip().lower() in ("", "[no concept]"):
                continue          # really no text → skip

            print(f"\n--- SUBTOPIC {s_no}: {sub['subtopic_title']} ---")
            print(concept + "\n")

            print("References:")
            self._print_refs(self._get_refs_for_subtopic(sub["subtopic_id"]))
            print()

            # ---------- questions ----------------------------------------
            for q in sub.get("questions", []):
                variants = list(q.get("variants", []))
                first_stem = q["stem"]

                while True:
                    stem_to_ask = variants.pop(0)["stem"] if variants else first_stem
                    ans = self._ask_variant(stem_to_ask, q["choices"])

                    if not ans:                     # skipped
                        if variants:
                            print("(Skip recorded – asking another variant)")
                            continue
                        print("Question skipped.")
                        sess["unanswered"] += 1
                        q_score = 0
                        break

                    idx = ord(ans) - ord('A')
                    correct_idx = self._correct_choice_index(q)

                    sess["answered"] += 1
                    if idx == correct_idx:
                        print("✅ Correct!")
                        sess["correct"] += 1
                        q_score = 1
                    else:
                        corr = chr(ord('A') + correct_idx) if correct_idx is not None else "?"
                        print(f"❌ Incorrect. Correct answer: {corr}")
                        print("Explanation:", q.get("explanation", "[none]"))
                        sess["wrong"] += 1
                        q_score = 0
                    break

                print("References for this question:")
                self._print_refs(self._get_refs_for_question(q["question_id"]))
                print(f"Score for this question: {q_score}/1\n")

        sess["scenario_complete"] = True
        print("=== Session Complete ===\n")
        return sess

    # ---------- RAG fallback ----------------------------------------------
    def _fetch_concept_from_rag(self, sub):
        try:
            rag = CognitiveSearchRagStore(
                SEARCH_ENDPOINT, SEARCH_ADMIN_KEY,
                SEARCH_INDEX_NAME, SEARCH_VECTOR_FIELD, SEARCH_API_VERSION
            )
            txt = rag.get_content_for_topic(sub['subtopic_title'], k=1)
            return txt or "[no concept]"
        except Exception:
            return "[no concept]"


###############################################################################
# 5) scoring + final summary
###############################################################################
class AssessmentAgent:
    def compute_scores(self, sess: dict) -> dict:
        total_attempted = sess["answered"] + sess["unanswered"]
        total_questions = max(1, total_attempted)          # avoid div-by-zero
        sess["score_pct"] = round(sess["correct"] / total_questions * 100, 1)
        return sess

class FinalAssessmentRecommendationsAgent:
    def generate_final_summary(self, sess: dict, plan: dict) -> dict:
        prompt = (
            f"Topic: {plan['topic_name']}\n"
            f"User Score: {sess.get('score_pct')}%\n\n"
            "Provide a JSON object with keys: summary_text, recommendations, references."
        )
        return call_aoai(prompt)

###############################################################################
# 6) Orchestrator
###############################################################################
class OrchestratorAgent:
    def __init__(self, store: PlanStoreSQL, db_conn: str):
        self.learning   = LearningScenarioAgent(db_conn)
        self.assessment = AssessmentAgent()
        self.final      = FinalAssessmentRecommendationsAgent()
        self.store      = store
        self.log        = logging.getLogger(__name__)

    def start_session(self, topic: str) -> dict:
        self.log.info("Starting session for '%s'", topic)
        plan = self.store.get_random_plan(topic)
        if not plan:
            raise ValueError(f"No study plan found for '{topic}'")
        sess: dict = {}
        sess = self.learning.run_scenario(plan, sess)
        sess = self.assessment.compute_scores(sess)
        return self.final.generate_final_summary(sess, plan)

###############################################################################
# 7) CLI
###############################################################################
def main():
    ap = argparse.ArgumentParser()
    ap.add_argument("--topic", required=True,
                    help="exact topic_name stored in cme.study_plans")
    args = ap.parse_args()
    logging.basicConfig(level=logging.INFO)
    try:
        summary = OrchestratorAgent(PlanStoreSQL(DB_CONN), DB_CONN)\
                 .start_session(args.topic)
        print(json.dumps(summary, indent=2, ensure_ascii=False))
    except Exception as exc:
        print("Cannot complete session:", exc)

if __name__ == "__main__":
    main()
