from uuid import UUID, uuid4
from datetime import datetime
from sqlalchemy.orm import Session, joinedload
from models import Topic, Session as DbSession

# -----------------------------------------------------------------------
# Study‑plan retrieval
# -----------------------------------------------------------------------
def fetch_study_plan(db: Session, topic_id: UUID) -> Topic | None:
    return (
        db.query(Topic)
        .filter(Topic.topic_id == topic_id)
        .options(
            joinedload(Topic.subtopics)
            .joinedload("concepts"),
            joinedload(Topic.subtopics)
            .joinedload("subtopic_refs")
            .joinedload("reference"),
            joinedload(Topic.subtopics)
            .joinedload("questions")
            .joinedload("choices"),
            joinedload(Topic.subtopics)
            .joinedload("questions")
            .joinedload("variants"),
        )
        .one_or_none()
    )

# -----------------------------------------------------------------------
# Session management helpers
# -----------------------------------------------------------------------
def start_session(db: Session, user_id: UUID, topic_id: UUID) -> UUID:
    sess = DbSession(session_id=uuid4(), user_id=user_id, topic_id=topic_id)
    db.add(sess)
    db.commit()
    db.refresh(sess)
    return sess.session_id

def close_session(db: Session, session_id: UUID, ended_utc: datetime) -> None:
    sess = db.query(DbSession).get(session_id)
    if sess:
        sess.ended_utc = ended_utc
        db.commit()
# crud.py  (new helper)
from session_store import store, AttemptRec
def record_attempt(db, *, session_id: UUID, subtopic_id: UUID, question_id: UUID,
                   variant_no: int, chosen_index: int, correct: bool):
    # No DB write. Keep only in-memory for the live session.
    rec = AttemptRec(
        subtopic_id=str(subtopic_id),
        question_id=str(question_id),
        variant_no=int(variant_no),
        chosen_index=int(chosen_index),
        correct=bool(correct),
        ts_utc=datetime.utcnow().isoformat()
    )
    store.append_attempt(session_id, rec)