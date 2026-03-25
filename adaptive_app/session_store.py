# session_store.py
from __future__ import annotations
from dataclasses import dataclass, field, asdict
from datetime import datetime, timedelta
from uuid import UUID
from typing import Dict, List, Any
import json, pathlib

# Where to keep unfinished (idle) sessions on disk
_SESSIONS_FS = pathlib.Path(__file__).parent / "unfinished_sessions"
_SESSIONS_FS.mkdir(parents=True, exist_ok=True)

_IDLE = timedelta(minutes=5)

@dataclass
class AttemptRec:
    subtopic_id: str
    question_id: str
    variant_no: int
    chosen_index: int
    correct: bool
    ts_utc: str

@dataclass
class LiveSession:
    user_id: UUID
    topic_id: UUID
    attempts: List[AttemptRec] = field(default_factory=list)
    cursors: Dict[str, Any] = field(default_factory=dict)   # subIdx, mcqIdx, attemptIdx, view, tab, etc.
    last_activity_utc: datetime = field(default_factory=datetime.utcnow)

class SessionStore:
    def __init__(self):
        self._mem: Dict[UUID, LiveSession] = {}

    def touch(self, session_id: UUID):
        if session_id in self._mem:
            self._mem[session_id].last_activity_utc = datetime.utcnow()

    def ensure(self, session_id: UUID, user_id: UUID, topic_id: UUID):
        self._mem.setdefault(session_id, LiveSession(user_id=user_id, topic_id=topic_id))
        self.touch(session_id)

    def append_attempt(self, session_id: UUID, rec: AttemptRec):
        self._mem[session_id].attempts.append(rec)
        self.touch(session_id)

    def set_cursors(self, session_id: UUID, **kwargs):
        self._mem[session_id].cursors.update(kwargs)
        self.touch(session_id)

    def get(self, session_id: UUID) -> LiveSession | None:
        return self._mem.get(session_id)

    def pop(self, session_id: UUID) -> LiveSession | None:
        return self._mem.pop(session_id, None)

    # ----- File-system snapshots for idle sessions -----
    def _fname(self, user_id: UUID, topic_id: UUID) -> pathlib.Path:
        return _SESSIONS_FS / f"{str(user_id).lower()}_{str(topic_id).lower()}.json"

    def save_idle_snapshot(self, session_id: UUID) -> pathlib.Path:
        ls = self._mem.get(session_id)
        if not ls:
            raise ValueError("No live session to snapshot")
        payload = {
            "session_id": str(session_id),
            "user_id": str(ls.user_id),
            "topic_id": str(ls.topic_id),
            "attempts": [asdict(a) for a in ls.attempts],
            "cursors": ls.cursors,
            "saved_utc": datetime.utcnow().isoformat()
        }
        f = self._fname(ls.user_id, ls.topic_id)
        f.write_text(json.dumps(payload, ensure_ascii=False, indent=2), encoding="utf-8")
        return f

    def has_idle(self, user_id: UUID) -> List[dict]:
        out = []
        for f in _SESSIONS_FS.glob(f"{str(user_id).lower()}_*.json"):
            with f.open("r", encoding="utf-8") as fh:
                try:
                    j = json.load(fh)
                    out.append({"topic_id": j["topic_id"], "file": str(f)})
                except Exception:
                    continue
        return out
    def active_by_user(self, user_id: UUID) -> List[UUID]:
        out = []
        for sid, ls in self._mem.items():
            if str(ls.user_id).lower() == str(user_id).lower():
                out.append(sid)
        print(out)        
        return out

    # NEW: is the user locked by any unfinished/live session?
    def is_locked(self, user_id: UUID) -> bool:
        return bool(self.has_idle(user_id))
    
    def load_idle(self, user_id: UUID, topic_id: UUID) -> dict | None:
        f = self._fname(user_id, topic_id)
        if not f.exists():
            return None
        return json.loads(f.read_text(encoding="utf-8"))

    def delete_idle(self, user_id: UUID, topic_id: UUID) -> bool:
        f = self._fname(user_id, topic_id)
        if f.exists():
            f.unlink()
            return True
        return False

store = SessionStore()
