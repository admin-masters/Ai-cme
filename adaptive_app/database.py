"""
Central DB engine & session factory.
Uses the DATABASE_URL you've already defined in settings.py
"""
from sqlalchemy import create_engine
from sqlalchemy.orm import sessionmaker, declarative_base
from settings import DATABASE_URL

engine = create_engine(
    DATABASE_URL,
    pool_pre_ping=True,
    connect_args={
        "fast_executemany": True,
    },
)

SessionLocal = sessionmaker(autocommit=False, autoflush=False, bind=engine)
Base = declarative_base()