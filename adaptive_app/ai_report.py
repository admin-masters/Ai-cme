"""
ai_report.py – Rich mastery report via Azure OpenAI GPT‑4o
----------------------------------------------------------
Requires env vars:
AZURE_OPENAI_ENDPOINT, AZURE_OPENAI_KEY, AZURE_OPENAI_DEPLOYMENT
"""
from __future__ import annotations
import os, json, uuid
from collections import defaultdict
from datetime import datetime
from typing import Dict, Any, List, Tuple
from openai import AzureOpenAI
from sqlalchemy.orm import Session
from models import Attempt, Session as DbSession, Question
from services import load_plan

# ─── Azure client ────────────────────────────────────────────────────
client = AzureOpenAI( 
    api_key="CzrrWvXbsmYcNguU1SqBpE9HDhhbfYsbkq3UedythCYCV9zNQ4mLJQQJ99BEACHYHv6XJ3w3AAABACOGiIPm", 
    api_version="2024-02-15-preview", 
    azure_endpoint="https://azure1405.openai.azure.com/", 
) 
DEPLOYMENT = os.getenv("AZURE_OPENAI_DEPLOYMENT", "gpt-4o")

# ─── Build a *rich* JSON context ────────────────────────────────────
def build_context(db: Session, session_id: uuid.UUID) -> Dict[str, Any]:
    sess: DbSession | None = db.query(DbSession).get(session_id)
    if not sess:
        raise ValueError("Session not found")

    # Load study‑plan once
    plan = load_plan(sess.topic_id)
    q_meta_lookup = {}  # question_id → dict(meta)
    for st in plan["subtopics"]:
        for q in st["questions"]:
            q_meta_lookup[q["question_id"].lower()] = {
                "subtopic_title": st["subtopic_title"],
                "stem": q["stem"],
                "explanation": q["explanation"],
                "concept": st["concept"][:160] + "…"  # brief concept teaser
            }

    # Fetch attempts
    attempts: List[Attempt] = (
        db.query(Attempt)
        .filter(Attempt.session_id == session_id)
        .order_by(Attempt.ts_utc)
        .all()
    )

    # Summaries per sub‑topic + list each incorrect Q
    per_sub: Dict[str, Dict[str, Any]] = defaultdict(lambda: {
        "correct": 0, "total": 0, "incorrect_q": []
    })
    for a in attempts:
        meta = q_meta_lookup.get(str(a.question_id).lower())
        st = meta["subtopic_title"] if meta else "Unknown"
        rec = per_sub[st]
        rec["total"] += 1
        rec["correct"] += int(a.correct)
        if not a.correct and meta:
            rec["incorrect_q"].append({
                "stem": meta["stem"],
                "your_choice_index": a.chosen_index,
                "explanation": meta["explanation"]
            })

    return {
        "topic_name": plan["topic_name"],
        "session_uuid": str(session_id),
        "started_utc": str(sess.started_utc),
        "ended_utc": str(datetime.utcnow()),
        "overall": {
            "correct": sum(r["correct"] for r in per_sub.values()),
            "total": sum(r["total"] for r in per_sub.values()),
        },
        "per_subtopic": per_sub,  # dict keyed by readable sub‑topic title
    }

# ─── GPT‑4o call with a deeper, guided prompt ───────────────────────
def run_gpt_report(ctx: Dict[str, Any]) -> str:
    sys_prompt = (
        "You are a senior academic pediatrician creating a MASTER‑LEVEL "
        "feedback report.  Use precise clinical language, cite guideline‑level "
        "references *when provided in context*, and give actionable study advice."
    )

    user_prompt = f"""
JSON_DATA from an adaptive session is enclosed in triple backticks.
Return **markdown** that follows EXACTLY this template:

## Overall score
*Correct*: <n> / <N> (<percent>%)

## Strengths (by sub‑topic)
<bullet list, each "Subtopic – accuracy% – 1‑sentence praise">

## Knowledge gaps (top 3)
<for the three lowest‑scoring sub‑topics, 2‑sentence diagnostic – why likely struggling>

## Missed questions and explanations
| Sub‑topic | Question stem (abridged) | Your answer idx | Expert explanation (1‑2 lines) |
|-----------|--------------------------|-----------------|--------------------------------|
| … | … | … | … |

## Targeted next steps
1. Concrete action  (e.g. "Reread concept paragraph X", "Practice variant 2 again")
2. …
3. …

ONLY output the markdown.  Do not wrap it in code‑fences.

```json
{json.dumps(ctx, indent=2)}
"""

    resp = client.chat.completions.create(
        model=DEPLOYMENT,
        temperature=0.2,
        messages=[
            {"role": "system", "content": sys_prompt},
            {"role": "user", "content": user_prompt},
        ],
    )

    return resp.choices[0].message.content
# ai_report.py – add this alongside run_gpt_report()
from session_store import store
from services import load_plan

def build_context_from_live(topic_id, session_id):
    ls = store.get(session_id)
    if not ls:
        raise ValueError("No live attempts for this session")
    plan = load_plan(topic_id)
    # Build lookup: qid -> meta (subtopic title, stem/explanations)
    q_meta_lookup = {}
    for st in plan["subtopics"]:
        for q in st["questions"]:
            q_meta_lookup[q["question_id"].lower()] = {
                "subtopic_title": st["subtopic_title"],
                "stem": q["stem"],
                "explanation": q["explanation"]
            }
    # Aggregate
    per_sub = {}
    total = 0
    correct = 0
    missed_rows = []
    for a in ls.attempts:
        total += 1
        correct += int(a.correct)
        meta = q_meta_lookup.get(str(a.question_id).lower())
        st = meta["subtopic_title"] if meta else "Unknown"
        rec = per_sub.setdefault(st, {"correct": 0, "total": 0, "incorrect_q": []})
        rec["total"] += 1
        rec["correct"] += int(a.correct)
        if not a.correct and meta:
            rec["incorrect_q"].append({
                "stem": meta["stem"],
                "your_choice_index": a.chosen_index,
                "explanation": meta["explanation"]
            })
    return {
        "topic_name": plan["topic_name"],
        "session_uuid": str(session_id),
        "overall": {"correct": correct, "total": total},
        "per_subtopic": per_sub
    }
