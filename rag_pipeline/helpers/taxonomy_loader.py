"""
Merge the three synonym layers → single dict[str, list[str]]
Usage:
    from helpers.taxonomy_loader import load_keywords
    TAXONOMY_KEYWORDS = load_keywords("typhoid")   # topic slug
"""

from pathlib import Path
import json, functools

_TAX_DIR = Path(__file__).resolve().parent.parent / "taxonomy"

def _read_json(p: Path) -> dict:
    return json.loads(p.read_text(encoding="utf-8")) if p.exists() else {}

def _deep_merge(*dicts):
    out = {}
    for d in dicts:
        for k, v in d.items():
            out.setdefault(k, set()).update(v)
    # convert back to sorted list, remove duplicates / blanks
    return {k: sorted({w.strip().lower() for w in vs if w.strip()})
            for k, vs in out.items()}

def load_keywords(topic_slug: str) -> dict[str, list[str]]:
    base      = _read_json(_TAX_DIR / "base.json")
    shared    = _read_json(_TAX_DIR / "shared_synonyms.json")
    topicfile = _read_json(_TAX_DIR / f"{topic_slug}.json")
    return _deep_merge(base, shared, topicfile)