from __future__ import annotations
from uuid import UUID
from random import choice
from datetime import datetime
from sqlalchemy.orm import Session
from crud import fetch_study_plan
from schemas import (
    StudyPlanOut, SubtopicOut, QuestionOut, ChoiceOut,
    SessionReport, AnswerIn, AnswerOut
)
from models import Question, Variant

import json, pathlib, functools
from typing import Dict, Any, List, Tuple, Optional
from functools import lru_cache
import uuid
# -----------------------------------------------------------------------
# Study‑plan assembly
# -----------------------------------------------------------------------
"""
services.py  –  JSON‑based study‑plan loader
Only two helpers are kept:  (1) list_topics()  (2) load_plan().
The rest of your FastAPI routes remain unchanged.
"""


# ---------------------------------------------------------------------
# Config – feel free to move this to settings.py
# ---------------------------------------------------------------------
_STUDYPLAN_DIR = pathlib.Path(__file__).parent / "studyplans"   # ./studyplans/*.json

# ---------------------------------------------------------------------
# Helpers
# ---------------------------------------------------------------------
def _plan_files():
    return _STUDYPLAN_DIR.glob("*_1.json")
def _uuid5_from(*parts: str) -> str:
    """
    Make a stable UUIDv5 from a tuple of strings.
    Deterministic across runs so resume/report remain consistent.
    """
    base = "/".join(str(p) for p in parts)
    return str(uuid.uuid5(uuid.NAMESPACE_URL, base))

def _infer_correct_index(q: Dict[str, Any]) -> int:
    """
    Ensure we always have a correct_choice_index for the main stem.
    Priority:
      1) q.correct_choice_index (if present)
      2) match q.correct_choice text against choices[*].(choice_text|text)
      3) fall back to first variant's correct_choice_index
      4) default 0
    """
    if "correct_choice_index" in q and q["correct_choice_index"] is not None:
        return int(q["correct_choice_index"])
    target = (q.get("correct_choice") or "").strip().lower()
    choices = q.get("choices") or []
    # unify choice_text vs text
    for c in choices:
        txt = (c.get("choice_text") or c.get("text") or "").strip().lower()
        if txt == target:
            return int(c.get("choice_index", 0))
    v0 = (q.get("variants") or [{}])[0]
    return int(v0.get("correct_choice_index", 0))

def _normalize_mcq(m: Dict[str, Any]) -> Dict[str, Any]:
    """
    Map a case-study MCQ to your existing QuestionOut shape.
    """
    # unify choices
    norm_choices = []
    for c in (m.get("choices") or []):
        norm_choices.append({
            "choice_index": int(c.get("choice_index", 0)),
            "choice_text": c.get("choice_text") or c.get("text") or "",
            "rationale": c.get("rationale") or ""
        })
    # unify variants
    norm_variants = []
    for v in (m.get("variants") or []):
        norm_variants.append({
            "variant_no": int(v.get("variant_no", len(norm_variants)+1)),
            "stem": v.get("stem") or m.get("stem") or "",
            "correct_choice_index": int(v.get("correct_choice_index", 0))
        })
    return {
        "question_id": m["question_id"],
        "stem": m.get("stem", ""),
        "explanation": m.get("explanation", ""),
        "correct_choice_index": _infer_correct_index({**m, "choices": norm_choices, "variants": norm_variants}),
        "choices": norm_choices,
        "variants": norm_variants,
        "references": m.get("references", [])
    }

def _expand_case_studies(plan: Dict[str, Any]) -> Dict[str, Any]:
    """
    For every subtopic, append each case_study as a virtual subtopic that follows it.
    """
    subs = plan.get("subtopics") or []
    out: List[Dict[str, Any]] = []
    for sub in subs:
        out.append(sub)
        cases = sub.get("case_studies") or []
        if not cases:
            continue
        for idx, cs in enumerate(cases, start=1):
            cs_sub = {
                "subtopic_id": _uuid5_from(str(sub.get("subtopic_id")), "case", str(cs.get("case_id"))),
                "subtopic_title": f"Case: {cs.get('title') or 'Case Study'}",
                "sequence_no": (sub.get("sequence_no") or 0),
                "concept": cs.get("vignette", ""),   # show vignette in the 'Concept' tab
                "references": cs.get("references") or sub.get("references") or [],
                "questions": [],
                "is_case": True,
                "case_id": cs.get("case_id"),
            }
            for m in (cs.get("mcqs") or []):
                cs_sub["questions"].append(_normalize_mcq(m))
            out.append(cs_sub)
    plan2 = dict(plan)
    plan2["subtopics"] = out
    return plan2

@functools.lru_cache(maxsize=128)
def _load_json(path: pathlib.Path) -> Dict[str, Any]:
    with path.open("r", encoding="utf-8") as fh:
        return json.load(fh)

def list_supertopics() -> List[str]:
    """Return all unique supertopics found in the studyplan files."""
    out = set()
    for file in _plan_files():
        try:
            data = _load_json(file)
            sup = (data.get("supertopic") or "Uncategorized").strip()
            if sup:
                out.add(sup)
        except Exception:
            continue
    return sorted(out, key=lambda s: s.lower())

def list_topics(supertopic: Optional[str] = None) -> List[dict]:
    """
    Scan the studyplans directory and return
    [{topic_id, topic_name, supertopic}, …], optionally filtered by supertopic.
    """
    out: List[dict] = []
    for file in _plan_files():
        try:
            data = _load_json(file)
            sup = (data.get("supertopic") or "Uncategorized").strip()
            if supertopic and sup.lower() != supertopic.lower():
                continue
            out.append({
                "topic_id": data["topic_id"],
                "topic_name": data["topic_name"],
                "supertopic": sup
            })
        except Exception:
            continue
    return sorted(out, key=lambda x: x["topic_name"].lower())

def load_plan(topic_id: UUID | str) -> Dict[str, Any] | None:
    """
    Fetch the pre‑assembled plan JSON for the topic.
    *topic_id* may be the real GUID or the human topic name.
    """
    # 1. quick filename match
    for file in _STUDYPLAN_DIR.glob("*_1.json"):
        data = _load_json(file)
        if str(topic_id).lower() in {str(data["topic_id"]).lower(), data["topic_name"].lower()}:
            # patch in a dummy completion percentage so the UI keeps working
            data.setdefault("percentage_complete", 0.0)
            return _expand_case_studies(data)
    return None
def _serialise_question(q: Question) -> QuestionOut:
    """
    Pick one variant at random (two exist) and serialise the question.
    """
    if not q.variants:
        # fallback to legacy stem
        v = Variant(variant_no=1, stem=q.stem, correct_choice_index=0)
    else:
        v = choice(q.variants)

    return QuestionOut(
        question_id=q.question_id,
        variant_no=v.variant_no,
        stem=v.stem,
        explanation=q.explanation,
        choices=[
            ChoiceOut(choice_index=c.choice_index, text=c.choice_text)
            for c in sorted(q.choices, key=lambda x: x.choice_index)
        ],
    )

# -----------------------------------------------------------------------
# Answer grading helpers
# -----------------------------------------------------------------------
def grade_answer_db(db: Session, question_id: UUID, variant_no: int, chosen_index: int) -> bool:
    v: Variant = (
        db.query(Variant)
        .filter(Variant.question_id == question_id, Variant.variant_no == variant_no)
        .one()
    )
    return chosen_index == v.correct_choice_index

def grade_answer(q: Question, variant_no: int, chosen_index: int) -> bool:
    """
    Adapter kept so that we don’t have to pass the whole DB session
    when the caller already fetched the variant row.
    """
    for v in q.variants:
        if v.variant_no == variant_no:
            return chosen_index == v.correct_choice_index
    raise ValueError("Variant not found")

# -----------------------------------------------------------------------
# Lightweight GPT‑style report placeholder
# -----------------------------------------------------------------------
def simple_report(score_pct: float) -> SessionReport:
    strong = ["Core concepts"] if score_pct >= 80 else []
    focus  = ["Review references"] if score_pct < 80 else []
    return SessionReport(
        session_id=UUID(int=0),  # Replace with real ID in assessment.py
        finished_utc=datetime.utcnow(),
        score_pct=score_pct,
        strong_areas=strong,
        focus_areas=focus,
    )

@lru_cache(maxsize=128)
def answer_key_for_topic(topic_id: str) -> Dict[Tuple[uuid.UUID, int], int]:
    """
    Returns {(question_uuid, variant_no): correct_choice_index, …}
    variant_no 0 == main stem
    """
    plan = load_plan(topic_id)
    key: Dict[Tuple[uuid.UUID, int], int] = {}

    for sub in plan["subtopics"]:
        for q in sub["questions"]:
            qid = uuid.UUID(q["question_id"])
            # main stem
            # main stem (be tolerant if correct_choice_index missing)
            cci = q.get("correct_choice_index")
            if cci is None:
                cci = _infer_correct_index(q)
            key[(qid, 0)] = int(cci)
            # variants
            for v in q.get("variants", []):
                key[(qid, v["variant_no"])] = v["correct_choice_index"]

    return key