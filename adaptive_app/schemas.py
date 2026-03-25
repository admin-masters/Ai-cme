from typing import List, Literal, Optional
from uuid import UUID
from datetime import datetime
from pydantic import BaseModel, ConfigDict, Field, EmailStr, AnyHttpUrl

class ConfigExtraIgnore(BaseModel):
    """Base that silently ignores unknown keys from the JSON file."""
    model_config = ConfigDict(extra="ignore")
# ---------- Study‑plan payloads ----------
class ReferenceOut(ConfigExtraIgnore):
    source_id: str
    citation_link: str
    excerpt: str


class ChoiceOut(ConfigExtraIgnore):
    choice_index: int
    choice_text: str   
    rationale: str | None = None 
class VariantOut(ConfigExtraIgnore):
    variant_no: int
    stem: str
    correct_choice_index: int

class QuestionOut(ConfigExtraIgnore):
    question_id: UUID
    stem: str
    explanation: str
    correct_choice: str | None = None      # present in some plans
    correct_choice_index: int
    choices: List[ChoiceOut]
    variants: List[VariantOut]
    references: List[ReferenceOut]

class SubtopicOut(ConfigExtraIgnore):
    subtopic_id: UUID
    subtopic_title: str              # ← matches JSON key
    sequence_no: int | None = None
    concept: str
    references: List[ReferenceOut]
    questions: List[QuestionOut]
    is_case: bool = False

class StudyPlanOut(ConfigExtraIgnore):
    topic_id: UUID
    topic_name: str
    percentage_complete: float = Field(0, description="Placeholder")
    subtopics: List[SubtopicOut]
# ---------- Session / answers ----------
class StartSessionIn(BaseModel):
    user_id: UUID
    topic_id: UUID

class AnswerIn(BaseModel):
    session_id: UUID
    topic_id: UUID
    subtopic_id: UUID
    question_id: UUID
    variant_no: int
    chosen_index: int

class AnswerOut(BaseModel):
    correct: bool

class SessionReport(BaseModel):
    session_id: UUID
    finished_utc: datetime
    score_pct: float
    strong_areas: List[str]
    focus_areas: List[str]
    
class ReportOut(BaseModel):
    markdown: str
# ---------- Education Platform integration ----------

class LaunchRequest(BaseModel):
    """
    Payload sent from the Education Platform on launch.
    """
    uid: int                      # WordPress / platform user id (integer)
    email: EmailStr               # user email
    credits: int                  # current credit balance
    return_url_post: AnyHttpUrl   # callback URL for POSTing final result
    return_url_get: AnyHttpUrl    # URL to redirect user after completion
    iat: int                      # issued at (Unix seconds)
    exp: int                      # expiry (Unix seconds)


class LaunchResponse(BaseModel):
    """
    Response to the Education Platform launch.

    NOTE: per new requirement, we do NOT return session_id here.
    We return the Adaptive App's UUID user_id so the platform can
    stop using a hardcoded UUID and pass the correct one to the UI.
    """
    status: Literal["success"]
    user_id: UUID
    message: str


class FinalResultResponse(BaseModel):
    """
    Response to our *own* endpoint that sends the final result back
    to the Education Platform.
    """
    status: Literal["success"]
    platform_status: str
    redirect_url: AnyHttpUrl | None = None