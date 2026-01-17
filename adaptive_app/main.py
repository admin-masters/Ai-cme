from uuid import UUID, uuid4
from fastapi import FastAPI, Depends, HTTPException, status, Body, Request, Header
from sqlalchemy.orm import Session
from database import SessionLocal, Base, engine
from schemas import (
    StudyPlanOut, AnswerIn, AnswerOut,
    StartSessionIn, SessionReport, ReportOut,
    LaunchRequest, LaunchResponse, FinalResultResponse,
)
from datetime import datetime
from typing import Optional, Dict, Any
from services import list_topics, load_plan
from crud import start_session
from models import Question, Variant, Session, SessionSummary, User, Topic
from assessment import finalise_session
from crud import record_attempt
# main.py  (replace the placeholder route)
from ai_report import build_context, run_gpt_report
from services import list_topics, load_plan, answer_key_for_topic, list_supertopics
from session_store import store
import os
import time
import uuid
import hmac
import hashlib
import requests
from settings import settings
# Create tables if they do not exist (no‑op when already migrated)
Base.metadata.create_all(bind=engine)

app = FastAPI(
    title="Adaptive Learning API",
    version="1.0.0",
)

def get_db():
    db = SessionLocal()
    try:
        yield db
    finally:
        db.close()
# Shared secret used to validate launch payloads from the Education Platform.
# Set this in your environment: EDU_LAUNCH_SHARED_SECRET="super-long-random-string"
LAUNCH_SHARED_SECRET = os.getenv("EDU_LAUNCH_SHARED_SECRET", "change-me-in-prod")

# In-memory mapping: launch_session_id -> launch context
# This lets us later look up uid/email/return URLs when sending final results.
launch_sessions: Dict[str, Dict[str, Any]] = {}
def _verify_launch_signature(raw_body: bytes, signature_hex: str) -> bool:
    """
    Validate HMAC-SHA256 signature of the raw JSON payload.

    Education Platform must compute:
        sig = HMAC_SHA256(EDU_LAUNCH_SHARED_SECRET, raw_body_as_sent)
        send it as hex in header: X-Launch-Signature: <sig>

    We recompute and compare using compare_digest to avoid timing leaks.
    """
    if not LAUNCH_SHARED_SECRET:
        # If misconfigured, fail closed
        return False
    expected = hmac.new(
        LAUNCH_SHARED_SECRET.encode("utf-8"),
        raw_body,
        hashlib.sha256,
    ).hexdigest()
    return hmac.compare_digest(expected, signature_hex)
# main.py – near top, after app = FastAPI(...)

def _canonical_json_bytes(data: Dict[str, Any]) -> bytes:
    """
    Stable JSON representation for HMAC signing.
    Education Platform must use the same (sorted keys, compact separators).
    """
    return json.dumps(data, sort_keys=True, separators=(",", ":")).encode("utf-8")


def _compute_launch_signature(data: Dict[str, Any]) -> str:
    """
    HMAC-SHA256 over the canonical JSON using LAUNCH_SIGNING_SECRET.
    """
    secret = settings.launch_signing_secret
    if not secret:
        raise RuntimeError("LAUNCH_SIGNING_SECRET is not configured")
    return hmac.new(secret.encode("utf-8"), _canonical_json_bytes(data), hashlib.sha256).hexdigest()

# ------------------------ ENDPOINTS ---------------------------------- #

@app.get("/api/supertopics")
def api_supertopics():
    return list_supertopics()

# main.py

@app.get("/api/topics")
def api_topics(
    supertopic: Optional[str] = None,
    user_id: Optional[UUID] = None,
    db: Session = Depends(get_db),
):
    """
    List topics, optionally filtered by supertopic AND the user's credit balance.

    - If user_id is omitted: returns all topics (backwards compatible).
    - If user_id is provided: only topics where topic.credits <= user.credit_balance.
    """
    topics = list_topics(supertopic)  # [{topic_id, topic_name, supertopic}, …]

    if user_id is None:
        return topics

    user: User | None = db.query(User).get(user_id)
    if not user or user.credit_balance is None:
        # No credit info: return full list (or could return empty if you prefer)
        return topics

    allowed: list[dict] = []
    for t in topics:
        try:
            tid = UUID(str(t["topic_id"]))
        except Exception:
            # If topic_id can't be parsed, skip or treat as cost 1
            continue

        topic_row: Topic | None = db.query(Topic).get(tid)
        credits = topic_row.credits if topic_row and topic_row.credits is not None else 1

        if credits <= user.credit_balance:
            allowed.append(t)

    return allowed


from services import load_plan

# --- NEW: Lock status (live sessions + unfinished JSON files) ---
@app.get("/api/lock-status/{user_id}")
def api_lock_status(user_id: UUID):
    live = [str(s) for s in store.active_by_user(user_id)]
    unfinished = store.has_idle(user_id)
    return {"locked": bool(unfinished),"unfinished": unfinished}

# --- Harden existing /api/session against starting while locked ---
@app.post("/api/session", response_model=UUID, summary="Start a study session")
def api_start_session(payload: StartSessionIn, db: Session = Depends(get_db)):
    if store.is_locked(payload.user_id):
        # Block starting another topic while locked by live/unfinished session
        raise HTTPException(
            status_code=409,
            detail="You have an unfinished session. Please finish or terminate it before starting a new one."
        )
    sid = start_session(db, payload.user_id, payload.topic_id)
    store.ensure(sid, payload.user_id, payload.topic_id)  # keep live
    return sid

@app.post("/api/session/idle-save")
def api_idle_save(
    user_id: UUID = Body(...),
    topic_id: UUID = Body(...),
    session_id: UUID = Body(...),
    cursors: Dict[str, Any] = Body(default={}),
    db: Session = Depends(get_db)
):
    # Save current cursors too (for exact resume point)
    store.set_cursors(session_id, **cursors)
    # Persist snapshot to file-system
    f = store.save_idle_snapshot(session_id)

    # Mark session as abandoned and close it lightly
    s: Session = db.query(Session).get(session_id)
    if s:
        s.status = "abandoned"
        s.ended_utc = datetime.utcnow()
        s.last_activity_utc = s.ended_utc
        db.commit()

    # Remove from memory
    store.pop(session_id)
    return {"saved": True, "file": str(f)}

# --- check for unfinished sessions for a user ---
@app.get("/api/resume-status/{user_id}")
def api_resume_status(user_id: UUID):
    files = store.has_idle(user_id)  # [{"topic_id": "...", "file": "..."}]
    enriched = []
    for d in files:
        plan = load_plan(d["topic_id"])
        enriched.append({
            "topic_id": d["topic_id"],
            "topic_name": plan["topic_name"] if plan else "(Unknown topic)",
            "file": d["file"]
        })
    return {"unfinished": enriched}


# --- resume: fetch JSON snapshot for a given topic ---
@app.get("/api/resume/{user_id}/{topic_id}")
def api_resume(user_id: UUID, topic_id: UUID):
    data = store.load_idle(user_id, topic_id)
    return data or {}

# --- terminate an unfinished session: delete the JSON ---
@app.delete("/api/resume/{user_id}/{topic_id}")
def api_resume_delete(user_id: UUID, topic_id: UUID):
    ok = store.delete_idle(user_id, topic_id)
    return {"deleted": ok}

# --- Dashboard: list all attempted topics with status ---
@app.get("/api/dashboard/{user_id}")
def api_dashboard(user_id: UUID, db: Session = Depends(get_db)):
    q = (
        db.query(Session.session_id, Session.topic_id, Session.status, Session.started_utc, Session.ended_utc)
          .filter(Session.user_id == user_id)
          .order_by(Session.started_utc.desc())
          .all()
    )
    summaries = { str(r.session_id): r
                  for r in db.query(SessionSummary).filter(SessionSummary.user_id == user_id).all() }

    unfinished = { d["topic_id"] for d in store.has_idle(user_id) }

    out = []
    for s in q:
        sid = str(s.session_id)
        plan = load_plan(s.topic_id)
        row = {
            "session_id": sid,
            "topic_id": str(s.topic_id),
            "topic_name": plan["topic_name"] if plan else "Unknown topic",   # NEW
            "status": "not-completed" if str(s.topic_id).lower() in unfinished else s.status,
            "started_utc": s.started_utc,
            "ended_utc": s.ended_utc
        }
        if sid in summaries:
            sm = summaries[sid]
            row.update({
              "score_pct": float(sm.score_pct),
              "total_questions": sm.total_questions,
              "total_correct": sm.total_correct
            })
        out.append(row)
    return {"sessions": out}

@app.get("/api/lesson/{topic_id}/{user_id}", response_model=StudyPlanOut)
def api_get_plan(topic_id: UUID, user_id: UUID):
    plan = load_plan(topic_id)
    if not plan:
        raise HTTPException(status_code=404, detail="Topic not found")
    return plan


@app.post("/api/answer", response_model=AnswerOut)
def api_answer(ans: AnswerIn, db: Session = Depends(get_db)):
    """
    Grading is now 100 % in‑memory, sourced from the JSON answer‑key.
    """
    try:
        correct_idx = answer_key_for_topic(str(ans.topic_id))[(ans.question_id, ans.variant_no)]
    except KeyError:
        raise HTTPException(status_code=404, detail="Question / variant not found in answer‑key")

    correct = (ans.chosen_index == correct_idx)

    record_attempt(
        db,
        session_id   = ans.session_id,
        subtopic_id  = ans.subtopic_id,
        question_id  = ans.question_id,
        variant_no   = ans.variant_no,
        chosen_index = ans.chosen_index,
        correct      = correct,
    )
    return AnswerOut(correct=correct)
import json
# main.py
import json
from datetime import datetime
from fastapi import HTTPException

@app.get("/api/report/{session_id}", response_model=ReportOut)
def api_report(session_id: UUID, db: Session = Depends(get_db)):
    # 0) Load the session row
    s: Session = db.query(Session).get(session_id)
    if not s:
        raise HTTPException(status_code=404, detail="Session not found")

    # 1) If we already generated a report, just return it (no GPT call, no live store dependency)
    existing = db.query(SessionSummary).get(session_id)
    if existing and existing.report_markdown:
        print(ReportOut(markdown=existing.report_markdown))
        return ReportOut(markdown=existing.report_markdown)

    # 2) Try to build from the live in‑memory store (most recent session completions)
    try:
        from ai_report import build_context_from_live, run_gpt_report
        ctx = build_context_from_live(s.topic_id, session_id)
        built_from = "live"
    except Exception:
        # 3) Fallback: legacy path using DB attempts table (if present)
        try:
            from ai_report import build_context, run_gpt_report
            ctx = build_context(db, session_id)
            built_from = "legacy_db"
        except Exception:
            raise HTTPException(
                status_code=404,
                detail="No attempts found for this session (no live or legacy data)."
            )

    # 4) Generate markdown and UPSERT into session_summaries
    md = run_gpt_report(ctx)
    overall = ctx.get("overall", {"correct": 0, "total": 0})
    per_sub = ctx.get("per_subtopic", {})
    total = int(overall.get("total", 0))
    correct = int(overall.get("correct", 0))
    score = round((correct / total * 100.0), 1) if total else 0.0

    if existing:
        existing.total_questions = total
        existing.total_correct = correct
        existing.score_pct = score
        existing.per_subtopic_json = json.dumps(per_sub)
        existing.report_markdown = md
    else:
        db.add(SessionSummary(
            session_id=session_id,
            user_id=s.user_id,
            topic_id=s.topic_id,
            finished_utc=datetime.utcnow(),
            total_questions=total,
            total_correct=correct,
            score_pct=score,
            per_subtopic_json=json.dumps(per_sub),
            report_markdown=md
        ))

    # 5) Mark session completed and clear live store if it exists
    s.status = "completed"
    s.ended_utc = datetime.utcnow()
    s.last_activity_utc = s.ended_utc
    db.commit()
    try:
        store.pop(session_id)
        
    except Exception:
        pass

    store.delete_idle(s.user_id, s.topic_id)  # NEW: remove any snapshot for this session
    from main import api_session_final_result
    api_session_final_result(session_id, db)
    return ReportOut(markdown=md)
# main.py
from services import load_plan

@app.get("/api/resume-status/{user_id}")
def api_resume_status(user_id: UUID):
    items = store.has_idle(user_id)  # [{'topic_id': '...', 'file': '...'}, ...]
    for it in items:
        plan = load_plan(it["topic_id"])
        it["topic_name"] = plan["topic_name"] if plan else "Unknown topic"
    return {"unfinished": items}
# --- NEW: Persist a point-in-time snapshot, but keep the session live ---
@app.post("/api/session/snapshot")
def api_session_snapshot(
    user_id: UUID = Body(...),
    topic_id: UUID = Body(...),
    session_id: UUID = Body(...),
    cursors: Dict[str, Any] = Body(default={}),
):
    # Ensure the live container exists and update cursors
    store.ensure(session_id, user_id, topic_id)
    store.set_cursors(session_id, **cursors)
    # Write/update the JSON file on disk (do NOT close the session)
    f = store.save_idle_snapshot(session_id)
    return {"saved": True, "file": str(f)}
# main.py

@app.post("/api/launch-from-platform", response_model=LaunchResponse)
async def api_launch_from_platform(
    request: Request,
    db: Session = Depends(get_db),
):
    """
    Called by the Education Platform to initiate a session context.

    Body JSON:
    {
      "uid": <int>,                // Education Platform user id
      "email": "<user@example.com>",
      "credits": <int>,            // credit balance
      "return_url_post": "https://...",
      "return_url_get": "https://...",
      "iat": <unix seconds>,
      "exp": <unix seconds>
    }

    Header:
      X-Launch-Signature: HMAC-SHA256(canonical_json, LAUNCH_SIGNING_SECRET)

    Returns:
    {
      "status": "success",
      "user_id": "<UUID of adaptive app user>",
      "message": "Launch accepted"
    }

    The Education Platform should:
      1) Call this server-to-server.
      2) Use user_id from the response when redirecting the user to the UI.
         (e.g. https://adaptive-app/ui?user_id=<user_id>)
    """
    # 1) Read & parse body
    raw_body = await request.body()
    try:
        body_dict = json.loads(raw_body.decode("utf-8"))
    except Exception:
        raise HTTPException(status_code=400, detail="Invalid JSON payload")

    # 2) Verify signature
    provided_sig = request.headers.get("X-Launch-Signature")
    if not provided_sig:
        raise HTTPException(status_code=401, detail="Missing X-Launch-Signature header")

    try:
        expected_sig = _compute_launch_signature(body_dict)
        print(settings.launch_signing_secret)
        print(expected_sig, provided_sig)
    except RuntimeError as e:
        raise HTTPException(status_code=500, detail=str(e))

    if not hmac.compare_digest(provided_sig, expected_sig):
        raise HTTPException(status_code=401, detail="Invalid launch signature")

    # 3) Validate token times
    try:
        payload = LaunchRequest(**body_dict)
    except Exception as e:
        raise HTTPException(status_code=400, detail=f"Invalid launch payload: {e}")

    now = int(time.time())
    if payload.iat > now + 60:
        raise HTTPException(status_code=401, detail="Token issued in the future")
    if payload.exp < now:
        raise HTTPException(status_code=401, detail="Token expired")

    # 4) Find or create mapped user by integer platform id
    user = (
        db.query(User)
        .filter(User.platform_user_id == payload.uid)
        .one_or_none()
    )

    if not user:
        # Create new adaptive app user, mapped to the Education Platform uid
        user = User(
            user_id=uuid4(),
            platform_user_id=payload.uid,
            email=payload.email,
            display_name=payload.email.split("@")[0] if payload.email else None,
            credit_balance=payload.credits,
            return_url_post=str(payload.return_url_post),
            return_url_get=str(payload.return_url_get),
        )
        db.add(user)
    else:
        # Update email / credits / callback URLs on each launch
        user.email = payload.email
        user.credit_balance = payload.credits
        user.return_url_post = str(payload.return_url_post)
        user.return_url_get = str(payload.return_url_get)

    db.commit()
    db.refresh(user)

    return LaunchResponse(
        status="success",
        user_id=user.user_id,
        message="Launch accepted",
    )

# main.py

@app.post("/api/session/{session_id}/final-result", response_model=FinalResultResponse)
def api_session_final_result(
    session_id: UUID,
    db: Session = Depends(get_db),
):
    """
    Called by the Adaptive App (e.g. after generating the report) to send the
    final session result back to the Education Platform.

    It will:
      1) Load Session, User, Topic, SessionSummary from DB.
      2) Compute score, time_spent, and credits_earned.
      3) POST to user.return_url_post with JSON:
         {
           "uid": <int>,
           "email": "<str>",
           "module_id": "<topic_id>",
           "session_id": "<session_id>",
           "score": <number>,
           "time_spent": <number>,    // seconds
           "credits_earned": <number>,// negative of topic.credits
           "status": "<str>",
           "completed_at": "<ISO8601 datetime>"
         }
      4) Expect { "status": "received" } from the Education Platform.
    """
    sess: Session | None = db.query(Session).get(session_id)
    if not sess:
        raise HTTPException(status_code=404, detail="Session not found")

    user: User | None = sess.user
    if not user or user.platform_user_id is None:
        raise HTTPException(
            status_code=400,
            detail="Session user is not linked to an Education Platform user (platform_user_id missing)",
        )

    topic: Topic | None = db.query(Topic).get(sess.topic_id)
    if not topic:
        raise HTTPException(status_code=404, detail="Topic not found")

    summary: SessionSummary | None = db.query(SessionSummary).get(session_id)
    if not summary:
        # Usually generated by /api/report; insist on it so score is correct
        raise HTTPException(
            status_code=409,
            detail="Session summary not found. Generate report before sending final result.",
        )

    # Score as a plain float
    score = float(summary.score_pct)

    # Time spent in seconds (ended - started)
    if sess.started_utc and sess.ended_utc:
        delta = sess.ended_utc - sess.started_utc
    else:
        delta = datetime.utcnow() - (sess.started_utc or datetime.utcnow())
    time_spent_seconds = int(delta.total_seconds())

    # Credits rewarded: negative of topic credits (e.g. 1 -> -1)
    topic_credits = topic.credits if topic.credits is not None else 1
    credits_earned = -int(topic_credits)

    completed_at = (sess.ended_utc or summary.finished_utc or datetime.utcnow()).isoformat()

    callback_url = user.return_url_post
    if not callback_url:
        raise HTTPException(
            status_code=400,
            detail="No return_url_post stored for user. Ensure /api/launch-from-platform has been called.",
        )

    payload = {
        "uid": user.platform_user_id,
        "email": user.email,
        "module_id": str(topic.topic_id),          # module_id = topic_id (per requirement)
        "session_id": str(session_id),             # real session_id from /api/session
        "score": score,
        "time_spent": time_spent_seconds,
        "credits_earned": credits_earned,          # ALWAYS negative of topic.credits
        "status": sess.status or "completed",
        "completed_at": completed_at,
    }

    # POST to Education Platform
    try:
        import jwt, time

        secret = settings.launch_signing_secret  # same shared secret

        jwt_payload = {
            "uid": user.platform_user_id,
            "credits": user.credit_balance + credits_earned,
            "session_id": str(session_id),
            "exp": int(time.time()) + 300
        }

        token = jwt.encode(jwt_payload, secret, algorithm="HS256")

        callback = f"{user.return_url_post}?token={token}"

        resp = requests.post(callback, timeout=10)  # no json body

        print(resp)
    except requests.RequestException as exc:
        print(exc)
        raise HTTPException(
            status_code=502,
            detail=f"Error calling Education Platform callback URL: {exc}",
        )

    if resp.status_code >= 400:
        raise HTTPException(
            status_code=502,
            detail=f"Education Platform returned HTTP {resp.status_code}",
        )

    try:
        ack = resp.json()
    except ValueError:
        raise HTTPException(
            status_code=502,
            detail="Education Platform response is not valid JSON",
        )

    if not (ack.get("status") == "received" or ack.get("ok") is True):
        raise HTTPException(
            status_code=502,
            detail="Education Platform did not acknowledge the callback",
        )


    # Successful – return redirect_url for the UI to send the user back
    return FinalResultResponse(
        status="success",
        platform_status=ack.get("status", ""),
        redirect_url=user.return_url_get,
    )
