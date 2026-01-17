"""
Minimal ORM layer matching the study-plan + progress schema.
Only the tables the adaptive-learning app needs are listed here.
If you later add exam-prep entities, extend this file and run Alembic.
"""
from uuid import uuid4
from sqlalchemy import (
    Column, String, Integer, Boolean, ForeignKey, DateTime, UniqueConstraint, TIMESTAMP, text, DECIMAL
)
from sqlalchemy.types import Text
from sqlalchemy.dialects.mssql import UNIQUEIDENTIFIER, TINYINT, NVARCHAR
from sqlalchemy.orm import relationship
from database import Base

class Topic(Base):
    __tablename__ = "topics"
    __table_args__ = {'schema': 'cme'}

    topic_id = Column(UNIQUEIDENTIFIER, primary_key=True, name='topic_id')
    topic_name = Column(NVARCHAR(255), nullable=False, name='topic_name')
    created_utc = Column(DateTime, nullable=False, name='created_utc',
                         server_default=text("SYSUTCDATETIME()"))
    schema_version = Column(TINYINT, nullable=False, name='schema_version',
                            server_default=text("1"))

    # NEW: credit cost for this topic (default 1)
    credits = Column(Integer, nullable=False, name='credits',
                     server_default=text("1"))

    # relationships
    subtopics = relationship("Subtopic", back_populates="topic", lazy="joined")
    study_plans = relationship("StudyPlan", back_populates="topic", lazy="select")


class Subtopic(Base):
    __tablename__ = "subtopics"
    __table_args__ = {'schema': 'cme'}
    
    subtopic_id = Column(UNIQUEIDENTIFIER, primary_key=True, name='subtopic_id')
    topic_id = Column(UNIQUEIDENTIFIER, ForeignKey("cme.topics.topic_id"), nullable=False, name='topic_id')
    title = Column(NVARCHAR(255), nullable=False, name='title')
    sequence_no = Column(Integer, nullable=True, name='sequence_no')
    status = Column(String(20), nullable=False, name='status')
    
    # relationships
    topic = relationship("Topic", back_populates="subtopics")
    concepts = relationship("Concept", back_populates="subtopic", lazy="select")
    questions = relationship("Question", back_populates="subtopic", lazy="select")
    subtopic_references = relationship("SubtopicReference", back_populates="subtopic", lazy="select")


class Concept(Base):
    __tablename__ = "concepts"
    __table_args__ = {'schema': 'cme'}
    
    concept_id = Column(UNIQUEIDENTIFIER, primary_key=True, name='concept_id')
    subtopic_id = Column(UNIQUEIDENTIFIER, ForeignKey("cme.subtopics.subtopic_id"), nullable=False, name='subtopic_id')
    content = Column(Text, nullable=False, name='content')
    token_count = Column(Integer, nullable=True, name='token_count')
    
    # relationships
    subtopic = relationship("Subtopic", back_populates="concepts")


class Reference(Base):
    __tablename__ = "references"
    __table_args__ = {'schema': 'cme'}
    
    reference_id = Column(UNIQUEIDENTIFIER, primary_key=True, name='reference_id')
    source_id = Column(NVARCHAR(128), nullable=False, name='source_id')
    citation_link = Column(NVARCHAR(512), nullable=True, name='citation_link')
    excerpt = Column(Text, nullable=False, name='excerpt')
    
    # relationships
    question_references = relationship("QuestionReference", back_populates="reference", lazy="select")
    subtopic_references = relationship("SubtopicReference", back_populates="reference", lazy="select")


class Question(Base):
    __tablename__ = "questions"
    __table_args__ = {'schema': 'cme'}
    
    question_id = Column(UNIQUEIDENTIFIER, primary_key=True, name='question_id')
    subtopic_id = Column(UNIQUEIDENTIFIER, ForeignKey("cme.subtopics.subtopic_id"), nullable=False, name='subtopic_id')
    stem = Column(Text, nullable=False, name='stem')
    explanation = Column(Text, nullable=False, name='explanation')
    correct_choice = Column(NVARCHAR(255), nullable=False, name='correct_choice', server_default=text("''"))
    
    # relationships
    subtopic = relationship("Subtopic", back_populates="questions")
    choices = relationship("Choice", back_populates="question", lazy="joined")
    question_references = relationship("QuestionReference", back_populates="question", lazy="select")
    variants = relationship("Variant", back_populates="question", lazy="select")


class Choice(Base):
    __tablename__ = "choices"
    __table_args__ = {'schema': 'cme'}
    
    question_id = Column(UNIQUEIDENTIFIER, ForeignKey("cme.questions.question_id"), primary_key=True, name='question_id')
    choice_index = Column(TINYINT, primary_key=True, name='choice_index')
    choice_text = Column(NVARCHAR(255), nullable=False, name='choice_text')
    choice_id = Column(UNIQUEIDENTIFIER, nullable=False, name='choice_id', server_default=text("NEWID()"))
    
    # relationships
    question = relationship("Question", back_populates="choices")


class Variant(Base):
    __tablename__ = "variants"
    __table_args__ = {'schema': 'cme'}
    
    variant_id = Column(UNIQUEIDENTIFIER, primary_key=True, name='variant_id')
    question_id = Column(UNIQUEIDENTIFIER, ForeignKey("cme.questions.question_id"), nullable=False, name='question_id')
    variant_no = Column(TINYINT, nullable=False, name='variant_no')
    stem = Column(Text, nullable=False, name='stem')
    correct_choice_index = Column(TINYINT, nullable=False, name='correct_choice_index')
    
    # relationships
    question = relationship("Question", back_populates="variants")


class QuestionReference(Base):
    __tablename__ = "question_references"
    __table_args__ = {'schema': 'cme'}
    
    question_id = Column(UNIQUEIDENTIFIER, ForeignKey("cme.questions.question_id"), primary_key=True, name='question_id')
    reference_id = Column(UNIQUEIDENTIFIER, ForeignKey("cme.references.reference_id"), primary_key=True, name='reference_id')
    
    # relationships
    question = relationship("Question", back_populates="question_references")
    reference = relationship("Reference", back_populates="question_references")


class SubtopicReference(Base):
    __tablename__ = "subtopic_references"
    __table_args__ = {'schema': 'cme'}
    
    subtopic_id = Column(UNIQUEIDENTIFIER, ForeignKey("cme.subtopics.subtopic_id"), primary_key=True, name='subtopic_id')
    reference_id = Column(UNIQUEIDENTIFIER, ForeignKey("cme.references.reference_id"), primary_key=True, name='reference_id')
    
    # relationships
    subtopic = relationship("Subtopic", back_populates="subtopic_references")
    reference = relationship("Reference", back_populates="subtopic_references")


class StudyPlan(Base):
    __tablename__ = "study_plans"
    __table_args__ = {'schema': 'cme'}
    
    topic_id = Column(UNIQUEIDENTIFIER, ForeignKey("cme.topics.topic_id"), primary_key=True, name='topic_id')
    assembled_utc = Column(DateTime, nullable=False, name='assembled_utc')
    plan_json = Column(Text, nullable=False, name='plan_json')
    
    # relationships
    topic = relationship("Topic", back_populates="study_plans")


class User(Base):
    __tablename__ = "users"
    __table_args__ = {"schema": "cme"}

    user_id = Column(UNIQUEIDENTIFIER, primary_key=True)
    email = Column(String(255), unique=True, nullable=False)
    display_name = Column(String(255))
    created_utc = Column(DateTime, nullable=False,
                         server_default=text("SYSUTCDATETIME()"))

    # NEW: integer ID from Education Platform (WordPress uid)
    platform_user_id = Column(Integer, unique=True, nullable=True)

    # NEW: last known credit balance from Education Platform
    credit_balance = Column(Integer, nullable=True)

    # NEW: last launch’s callback URLs
    return_url_post = Column(String(2048), nullable=True)
    return_url_get = Column(String(2048), nullable=True)

    sessions = relationship("Session", back_populates="user")

class Attempt(Base):
    __tablename__ = "attempts"
    __table_args__ = {"schema": "cme"}

    attempt_id      = Column(UNIQUEIDENTIFIER, primary_key=True, default=uuid4)
    session_id      = Column(UNIQUEIDENTIFIER, ForeignKey("cme.sessions.session_id"), nullable=False)
    subtopic_id     = Column(UNIQUEIDENTIFIER, nullable=False)
    question_id     = Column(UNIQUEIDENTIFIER, nullable=False)
    variant_no      = Column(TINYINT, nullable=False)
    chosen_index    = Column(TINYINT, nullable=False)
    correct         = Column(TINYINT, nullable=False)   # 1 = True, 0 = False
    ts_utc          = Column(DateTime, server_default=text("SYSUTCDATETIME()"))

class Session(Base):
    __tablename__ = "sessions"
    __table_args__ = {"schema": "cme"}
    
    session_id = Column(UNIQUEIDENTIFIER, primary_key=True)
    user_id = Column(UNIQUEIDENTIFIER, ForeignKey("cme.users.user_id"), nullable=False)
    topic_id = Column(UNIQUEIDENTIFIER, ForeignKey("cme.topics.topic_id"), nullable=False)
    started_utc = Column(DateTime, nullable=False, server_default=text("SYSUTCDATETIME()"))
    ended_utc = Column(DateTime)
    status = Column(String(16), nullable=False, server_default=text("'active'"))
    last_activity_utc = Column(DateTime, nullable=True)
    
    # ADD THIS LINE:
    user = relationship("User", back_populates="sessions")                                # NEW

class SessionSummary(Base):
    __tablename__ = "session_summaries"
    __table_args__ = {"schema": "cme"}

    session_id = Column(UNIQUEIDENTIFIER, primary_key=True)
    user_id = Column(UNIQUEIDENTIFIER, nullable=False)
    topic_id = Column(UNIQUEIDENTIFIER, nullable=False)
    finished_utc = Column(DateTime, nullable=False)
    total_questions = Column(Integer, nullable=False)
    total_correct = Column(Integer, nullable=False)
    score_pct = Column(DECIMAL(5,1), nullable=False)
    per_subtopic_json = Column(String)   # JSON as NVARCHAR(MAX)
    report_markdown = Column(String)     # MD as NVARCHAR(MAX)
    created_utc = Column(DateTime, nullable=False, server_default=text("SYSUTCDATETIME()"))
