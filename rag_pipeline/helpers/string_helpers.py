import re, unicodedata
import json, re, logging
log = logging.getLogger(__name__)
__all__ = ["normalise", "token_overlap"]

def normalise(text: str) -> str:
    """Lower-case, strip accents, collapse whitespace."""
    txt = unicodedata.normalize("NFKD", text)
    txt = re.sub(r"\s+", " ", txt).lower().strip()
    return txt

def token_overlap(a: str, b: str, *, min_len: int = 4) -> bool:
    aset = {t for t in re.findall(r"[a-z]{%d,}" % min_len, normalise(a))}
    bset = {t for t in re.findall(r"[a-z]{%d,}" % min_len, normalise(b))}
    return bool(aset & bset)

def safe_json(text: str) -> dict | None:
    """
    Try strict JSON first; if that fails, use a { .. } regex rescue.
    Returns None if nothing decodable.
    """
    if not text:
        return None
    try:
        return json.loads(text)
    except Exception:
        m = re.search(r"\{[\s\S]*\}", text)
        if m:
            try:
                return json.loads(m.group(0))
            except Exception as e:
                log.warning("Regex-extracted JSON still invalid: %s", e)
    return None
   