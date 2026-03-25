"""
Very thin placeholder ‑ you will replace this with Azure OpenAI later.
"""
from uuid import UUID
from datetime import datetime
from sqlalchemy.orm import Session
from models import Session as DbSession, Question
from schemas import SessionReport
from services import grade_answer

def finalise_session(db: Session, session_id: UUID, answers: list[tuple[UUID, int, int]]) -> SessionReport:
    """
    answers = [(question_id, variant_no, chosen_index), …]
    """
    correct = 0
    total   = len(answers)

    for qid, vno, idx in answers:
        q: Question = db.query(Question).get(qid)
        if q and grade_answer(q, vno, idx):
            correct += 1

    pct = round(correct / total * 100, 1) if total else 0.0

    sess = db.query(DbSession).get(session_id)
    if sess:
        sess.ended_utc = datetime.utcnow()
        db.commit()

    return SessionReport(
        session_id=session_id,
        finished_utc=datetime.utcnow(),
        score_pct=pct,
        strong_areas=[],
        focus_areas=[],
    )