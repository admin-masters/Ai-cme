# studyplan-pipeline/generateSubtopics/__init__.py
from __future__ import annotations
import logging, os, json, re, unicodedata, uuid
from typing import List, Dict, Any

import azure.functions as func
import pyodbc

from openai import AzureOpenAI
from azure.storage.queue import QueueClient
from azure.core.credentials import AzureKeyCredential
from azure.search.documents import SearchClient

# ───────────────────────── Azure/OpenAI config (env only) ─────────────────────────
AZURE_OAI_ENDPOINT = "https://azure1405.openai.azure.com/"
AZURE_OAI_KEY = "CzrrWvXbsmYcNguU1SqBpE9HDhhbfYsbkq3UedythCYCV9zNQ4mLJQQJ99BEACHYHv6XJ3w3AAABACOGiIPm"
DEPLOYMENT = "gpt-4o"
AZURE_OAI_API_VERSION = "2024-02-15-preview"

oai_client = AzureOpenAI(
    api_key=AZURE_OAI_KEY,
    azure_endpoint=AZURE_OAI_ENDPOINT,
    api_version=AZURE_OAI_API_VERSION,
)

# ───────────────────────── Azure Cognitive Search ─────────────────────────
SEARCH_ENDPOINT = "https://basic-rag-sandbox.search.windows.net"
SEARCH_ADMIN_KEY = "tuqRZ8A374Aw3wXKSTzOY6SEu6Ra8rOyhPgFEtcLpSAzSeBOByQL"
INDEX_NAME = "pubert-demo-new"
SEARCH_API_VERSION = "2025-05-01-preview"

search_cli = SearchClient(
    endpoint=SEARCH_ENDPOINT,
    index_name=INDEX_NAME,
    credential=AzureKeyCredential(SEARCH_ADMIN_KEY),
    api_version=SEARCH_API_VERSION,
)

# ───────────────────────── knobs (min/max; coverage) ─────────────────────
MIN_SUBTOPICS = int(os.getenv("MIN_SUBTOPICS", "22"))  # ← default 22
MAX_SUBTOPICS = int(os.getenv("MAX_SUBTOPICS", "40"))  # ← unchanged
COVERAGE_MIN_CHARS = int(os.getenv("COVERAGE_MIN_CHARS", "1200"))
BLOCK_ON_LOW_COVERAGE = os.getenv("BLOCK_ON_LOW_COVERAGE", "0") == "0"

AUDIENCE_DEFAULT = os.getenv(
    "AUDIENCE_DEFAULT",
    "Clinical practitioners and final-year medical students focusing on practical decision-making.",
)
OBJECTIVE_DEFAULT = os.getenv(
    "OBJECTIVE_DEFAULT",
    "Ensure practice-ready diagnosis, management, complications prevention, and counselling.",
)

# ───────────────────────── helpers: text / canon / dedupe ─────────────────
_ADULT_BAN = re.compile(r"\b(pregnan\w*|lactat\w*|maternal|fetus)\b", re.I)

def _norm(s: str) -> str:
    return unicodedata.normalize("NFKD", s or "").encode("ascii", "ignore").decode()

def _canon_title(t: str) -> str:
    s = _norm(t).lower()
    s = re.sub(r"\b(for\s+(paediatr(ic)?|pediatric)[^)]*)$", "", s).strip()
    s = re.sub(r"\b(in\s+children|in\s+paediatrics|p(a)ediatric)\b", "", s).strip()
    s = re.sub(r"\s+", " ", s)
    return s

def _dedupe_titles(titles: List[str]) -> List[str]:
    seen = set()
    out = []
    for t in titles:
        if not t or not str(t).strip():
            continue
        if _ADULT_BAN.search(t):  # global paeds guardrail
            continue
        k = _canon_title(t)
        if k and k not in seen:
            seen.add(k)
            out.append(t.strip())
    return out

# ───────────────────────── Search coverage (pre‑verify) ───────────────────
def _estimate_coverage(topic_name: str, sub_title: str) -> int:
    query = f"{topic_name} {sub_title}"
    try:
        results = search_cli.search(search_text=query, top=10)
        total = 0
        for doc in results:
            body = (doc.get("content") or "")
            total += len(body)
        return total
    except Exception:
        return 0

def _coverage_stats(topic: str, titles: List[str]) -> List[Dict[str, Any]]:
    return [{"title": t, "coverage_chars": _estimate_coverage(topic, t)} for t in titles]


# ───────────────────────── NEW: Outline from Azure Search index (hierarchical docs) ─────────────────────────
SUBTOPIC_SOURCE = os.getenv("SUBTOPIC_SOURCE", "index").lower()  # 'index' (default) or 'gpt'

_VIGNETTE_PAT = re.compile(r"\b(vignett|vignettes|case\s+vignette|case\s+stud(y|ies)|case\s+based)\b", re.I)
_SEQ_RE = re.compile(r"^(\d+)([a-zA-Z]?)(?:\.(\d+))?$")


def _escape_odata(s: str) -> str:
    return (s or "").replace("'", "''")


def _letter_rank(ch: str) -> int:
    if not ch:
        return 0
    c = ch.lower()
    if 'a' <= c <= 'z':
        return ord(c) - ord('a') + 1
    return 0


def _sequence_key(seq: str) -> tuple:
    s = (seq or '').strip()
    m = _SEQ_RE.match(s)
    if not m:
        return (10**9, 10**9, 10**9, s)
    major = int(m.group(1))
    letter = _letter_rank(m.group(2) or '')
    minor = int(m.group(3) or 0)
    return (major, letter, minor, s)


def _search_all_index(*, search_text: str, **kwargs) -> list[dict]:
    out: list[dict] = []
    skip = 0
    top = int(kwargs.pop("top", 1000) or 1000)
    while True:
        results = search_cli.search(search_text=search_text, top=top, skip=skip, **kwargs)
        batch = list(results)
        if not batch:
            break
        out.extend(batch)
        if len(batch) < top:
            break
        skip += len(batch)
        if skip > 100000:
            break
    return out


def _resolve_topic_in_index(topic_name: str) -> str:
    """Map DB topic_name to the most frequent matching index 'topic'."""
    try:
        docs = _search_all_index(search_text=topic_name, select=["topic"], top=50)
    except Exception:
        docs = []
    freq: dict[str, int] = {}
    for d in docs:
        t = (d.get("topic") or "").strip()
        if t:
            freq[t] = freq.get(t, 0) + 1
    if not freq:
        return topic_name
    return max(freq.items(), key=lambda kv: kv[1])[0]


def _outline_from_index(topic_name: str) -> tuple[str, list[dict], list[dict]]:
    """Return (resolved_topic, outline_rows, vignette_docs).

    outline_rows: [{subtopic, sequence, coverage_chars}]
    vignette_docs: index docs for vignette-only subtopics (include content for case extraction)
    """
    resolved = _resolve_topic_in_index(topic_name)

    # 1) enumerate all subtopics for resolved topic using metadata only
    docs = _search_all_index(
        search_text='*',
        filter=f"topic eq '{_escape_odata(resolved)}'",
        select=["subtopic", "sequence", "char_count"],
        top=1000,
    )

    by_sub: dict[str, dict] = {}
    vignette_subtopics: set[str] = set()

    for d in docs:
        st = (d.get("subtopic") or "").strip()
        if not st:
            continue
        seq = (d.get("sequence") or "").strip()
        cc = int(d.get("char_count") or 0)

        if _VIGNETTE_PAT.search(st):
            vignette_subtopics.add(st)
            continue

        row = by_sub.get(st)
        if not row:
            by_sub[st] = {
                "subtopic": st,
                "sequence": seq,
                "coverage_chars": cc,
                "seq_key": _sequence_key(seq),
            }
        else:
            row["coverage_chars"] += cc
            # keep earliest sequence by key
            if _sequence_key(seq) < row["seq_key"]:
                row["sequence"] = seq
                row["seq_key"] = _sequence_key(seq)

    outline = list(by_sub.values())
    outline.sort(key=lambda r: (r["seq_key"], r["subtopic"].lower()))

    # 2) fetch vignette docs with content (small subset)
    vignette_docs: list[dict] = []
    for vs in sorted(vignette_subtopics):
        vdocs = _search_all_index(
            search_text='*',
            filter=f"topic eq '{_escape_odata(resolved)}' and subtopic eq '{_escape_odata(vs)}'",
            select=["id", "content", "sequence", "chunk_index", "heading_path", "subtopic"],
            top=1000,
        )
        vignette_docs.extend(vdocs)

    return resolved, outline, vignette_docs


def _stitch_vignette_text(vignette_docs: list[dict], max_chars: int = 18000) -> str:
    if not vignette_docs:
        return ""

    def k(d: dict):
        return (_sequence_key(d.get("sequence") or ""), int(d.get("chunk_index") or 0), d.get("heading_path") or "")

    parts: list[str] = []
    used = 0
    for d in sorted(vignette_docs, key=k):
        c = (d.get("content") or "").strip()
        if not c:
            continue
        if used + len(c) > max_chars:
            parts.append(c[: max(0, max_chars - used)])
            break
        parts.append(c)
        used += len(c)

    return "\n\n".join(parts).strip()


def _extract_cases_gpt(topic: str, vignette_text: str) -> list[dict]:
    """Extract structured case studies from vignette sections."""
    if not vignette_text.strip():
        return []

    schema = {
        "cases": [
            {
                "case_title": "string",
                "vignette": "string",
                "learning_objective": "string"
            }
        ]
    }

    prompt = {
        "role": "user",
        "content": (
            "Extract DISTINCT paediatric case vignettes from the SOURCE text.\n"
            "- Do NOT invent facts; only restructure.\n"
            "- Keep each vignette 90-220 words, with age, setting, time course, key symptoms, focused exam, and <=2 objective data.\n"
            "- Do NOT include diagnosis or management in the vignette text.\n"
            "Return JSON only with schema: " + json.dumps(schema) + "\n\n"
            f"TOPIC: {topic}\n\nSOURCE:\n{vignette_text}\n"
        )
    }

    try:
        rsp = oai_client.chat.completions.create(
            model=DEPLOYMENT,
            messages=[{"role": "system", "content": "You are a medical editor. Return JSON only."}, prompt],
            temperature=0.2,
            max_tokens=1800,
            response_format={"type": "json_object"},
        )
        out = json.loads(rsp.choices[0].message.content)
        cases = out.get("cases") or []
        if not isinstance(cases, list):
            return []
        cleaned = []
        for c in cases:
            if not isinstance(c, dict):
                continue
            vign = (c.get("vignette") or "").strip()
            if len(vign) < 80:
                continue
            cleaned.append({
                "case_title": (c.get("case_title") or "").strip()[:255] or "Clinical case",
                "vignette": vign,
                "learning_objective": (c.get("learning_objective") or "").strip()[:255],
            })
        return cleaned
    except Exception:
        logging.exception("Case extraction failed")
        return []


def _assign_cases_to_subtopics_gpt(topic: str, subtopics: list[dict], cases: list[dict]) -> list[dict]:
    """Map each case to the most logical subtopic_id."""
    if not subtopics or not cases:
        return []

    schema = {
        "assignments": [
            {
                "case_index": 0,
                "subtopic_id": "uuid",
                "reason": "short"
            }
        ]
    }

    prompt = {
        "role": "user",
        "content": (
            "Assign each CASE to exactly one SUBTOPIC where it fits best pedagogically.\n"
            "If a case does not fit any, omit it (do not guess).\n"
            "Return JSON only with schema: " + json.dumps(schema) + "\n\n"
            f"TOPIC: {topic}\n\nSUBTOPICS (id,title):\n" + json.dumps(subtopics, ensure_ascii=False) + "\n\n"
            f"CASES (indexed from 0):\n" + json.dumps(cases, ensure_ascii=False)
        )
    }

    try:
        rsp = oai_client.chat.completions.create(
            model=DEPLOYMENT,
            messages=[{"role": "system", "content": "Return JSON only."}, prompt],
            temperature=0.2,
            max_tokens=1400,
            response_format={"type": "json_object"},
        )
        out = json.loads(rsp.choices[0].message.content)
        assigns = out.get("assignments") or []
        if not isinstance(assigns, list):
            return []
        cleaned = []
        valid_ids = {s["subtopic_id"] for s in subtopics}
        for a in assigns:
            if not isinstance(a, dict):
                continue
            idx = a.get("case_index")
            sid = (a.get("subtopic_id") or "").strip()
            if not isinstance(idx, int) or idx < 0 or idx >= len(cases):
                continue
            if sid not in valid_ids:
                continue
            cleaned.append({"case_index": idx, "subtopic_id": sid, "reason": (a.get("reason") or "")[:200]})
        return cleaned
    except Exception:
        logging.exception("Case assignment failed")
        return []


def _ingest_vignette_cases(topic_id: str, topic_name: str, vignette_docs: list[dict], conn_str: str) -> int:
    """Extract vignette cases, map to subtopics, insert into cme.cases, and enqueue case-mcq-queue."""
    vign_text = _stitch_vignette_text(vignette_docs)
    cases = _extract_cases_gpt(topic_name, vign_text)
    if not cases:
        return 0

    with pyodbc.connect(conn_str) as conn:
        cur = conn.cursor()
        cur.execute("SELECT subtopic_id, title FROM cme.subtopics WHERE topic_id=? ORDER BY sequence_no", topic_id)
        subs = [{"subtopic_id": r.subtopic_id, "title": r.title} for r in cur.fetchall()]

    assignments = _assign_cases_to_subtopics_gpt(topic_name, subs, cases)
    if not assignments:
        return 0

    inserted = 0
    q = None
    try:
        q = QueueClient.from_connection_string(os.environ["AzureWebJobsStorage"], "case-mcq-queue")
    except Exception:
        q = None

    with pyodbc.connect(conn_str) as conn:
        cur = conn.cursor()
        for a in assignments:
            c = cases[a["case_index"]]
            sid = a["subtopic_id"]
            title = c["case_title"]
            vign = c["vignette"]
            lo = c.get("learning_objective") or ""

            # de-dupe by exact vignette text under same subtopic
            cur.execute("""
                IF NOT EXISTS (SELECT 1 FROM cme.cases WHERE subtopic_id=? AND vignette=?)
                BEGIN
                    INSERT INTO cme.cases (case_id, subtopic_id, title, vignette, word_count, learning_objective)
                    VALUES (NEWID(), ?, ?, ?, ?, ?)
                END
            """, sid, vign, sid, title, vign, len(re.findall(r"\b\w+\b", vign)), lo)

            # find inserted case_id (or existing) for queueing
            cur.execute("SELECT TOP 1 case_id FROM cme.cases WHERE subtopic_id=? AND vignette=? ORDER BY created_utc DESC", sid, vign)
            row = cur.fetchone()
            if row:
                case_id = row.case_id
                inserted += 1
                # mark subtopic as case-bearing
                cur.execute("""
                    UPDATE cme.subtopics
                    SET case_amenable=1,
                        case_status = CASE WHEN case_status IN ('verified','failed') THEN case_status ELSE 'pending' END
                    WHERE subtopic_id=?
                """, sid)
                if q:
                    try:
                        q.send_message(json.dumps({"case_id": case_id}))
                    except Exception:
                        pass

        conn.commit()

    return inserted

# ───────────────────────── merge/top‑up (keep ≤MAX, ≥MIN) ─────────────────
def _coalesce_titles_gpt(topic: str, titles: List[str], max_n: int = MAX_SUBTOPICS) -> List[str]:
    """
    Ask GPT to merge closely related items to fit into <= max_n without losing scope.
    """
    prompt = {
        "role": "user",
        "content": (
            f"Merge or bundle closely related pediatric sub-topics for '{topic}' so that the final list "
            f"has at most {max_n} items. Combine only when defensible pedagogically and keep single‑purpose "
            f"clarity where clinically important. Return JSON {{\"subtopics\": [\"...\"]}}.\n"
            f"INPUT={json.dumps(titles, ensure_ascii=False)}"
        )
    }
    try:
        rsp = oai_client.chat.completions.create(
            model=DEPLOYMENT,
            messages=[{"role": "system", "content": "You are a curriculum editor. Return JSON only."}, prompt],
            temperature=0.2, max_tokens=800, response_format={"type": "json_object"},
        )
        data = json.loads(rsp.choices[0].message.content)
        merged = [t for t in (data.get("subtopics") or []) if str(t).strip()]
        return merged[:max_n]
    except Exception:
        # deterministic fallback below
        return []

def _coalesce_titles_heuristic(titles: List[str], max_n: int = MAX_SUBTOPICS) -> List[str]:
    """
    Greedy Jaccard merge of non-protected titles, forming 'A / B' labels.
    """
    protect = re.compile(
        r"(triage|admission|escalation|persistent|relapse|mdr|xdr|vaccine|carrier|"
        r"household|outbreak|follow[-]?up|defervesc|counsel)",
        re.I,
    )


    def normset(t: str) -> set[str]:
        s = re.sub(r"[^a-z0-9 ]+", " ", (_norm(t) or "").lower())
        return {w for w in s.split() if len(w) > 2}

    items = [{"t": t, "p": bool(protect.search(t))} for t in titles]
    while len(items) > max_n:
        best = None
        for i in range(len(items)):
            if items[i]["p"]:
                continue
            for j in range(i + 1, len(items)):
                if items[j]["p"]:
                    continue
                a, b = normset(items[i]["t"]), normset(items[j]["t"])
                if not a or not b:
                    continue
                jacc = len(a & b) / len(a | b)
                sub = items[i]["t"].lower() in items[j]["t"].lower() or items[j]["t"].lower() in items[i]["t"].lower()
                score = jacc + (0.15 if sub else 0.0)
                if (best is None) or (score > best[0]):
                    best = (score, i, j)
        if best is None:
            break
        _, i, j = best
        items[i]["t"] = f"{items[i]['t']} / {items[j]['t']}"
        items.pop(j)
    return [x["t"] for x in items][:max_n]

def _topup_titles_gpt(topic: str, titles: List[str], min_n: int = MIN_SUBTOPICS, rubric: Dict[str, Any] | None = None) -> List[str]:
    """
    Ask GPT to add missing but essential pediatric sub-topics (no duplicates) to reach >= min_n,
    guided by the rubric if available.
    """
    content = {
        "role": "user",
        "content": (
            f"Given this pediatric sub-topic list for '{topic}', add any missing essential items "
            f"so the total is at least {min_n}. Avoid duplicates and overly generic items. Prefer "
            f"decision/technique/criteria nodes. If a rubric is provided, cover all its dimensions.\n"
            f"Prefer adding concrete workflow nodes from the care‑pathway primitives above where "
            f"they are applicable to the topic kind (avoid local policy, brand names, or region‑specific details)."
            f"RUBRIC={json.dumps(rubric or {}, ensure_ascii=False)}\n"
            f"INPUT={json.dumps(titles, ensure_ascii=False)}"
        ),
    }
    try:
        rsp = oai_client.chat.completions.create(
            model=DEPLOYMENT,
            messages=[{"role": "system", "content": "Return JSON only as {\"subtopics\": [\"...\"]}."}, content],
            temperature=0.25, max_tokens=600, response_format={"type": "json_object"},
        )
        data = json.loads(rsp.choices[0].message.content)
        return [t for t in (data.get("subtopics") or []) if str(t).strip()]
    except Exception:
        return []

def _enforce_count(topic: str, titles: List[str], rubric: Dict[str, Any] | None = None) -> List[str]:
    """
    Ensure final list size MIN_SUBTOPICS..MAX_SUBTOPICS with logical merges or top-ups.
    """
    titles = [t for t in titles if str(t).strip()]
    if len(titles) > MAX_SUBTOPICS:
        merged = _coalesce_titles_gpt(topic, titles, MAX_SUBTOPICS) or _coalesce_titles_heuristic(titles, MAX_SUBTOPICS)
        titles = merged
    if len(titles) < MIN_SUBTOPICS:
        extra = _topup_titles_gpt(topic, titles, MIN_SUBTOPICS, rubric)
        titles = titles + [t for t in extra if t not in titles]
    return titles
# generateSubtopics/__init__.py

# generateSubtopics/__init__.py

def _strip_ellipses(s: str) -> str:
    # prevent literal '...' or '…' creeping into titles
    return re.sub(r'[.…]+$', '', (s or '').strip())

def _apply_verification(titles: List[str], verdict: Dict[str, Any]) -> List[str]:
    """
    Apply drops, merges, additions, and rewording from verifier output.
    Ensures merges *replace* originals (A,B) with a single "A / B", preserves order,
    and sanitizes titles (no ellipses).
    """
    drops = set((_canon_title(x) for x in (verdict.get("drop") or [])))
    keep = [t for t in titles if _canon_title(t) not in drops]

    # MERGES: replace A and B with "A / B"
    for pair in verdict.get("merge") or []:
        pair = [p for p in pair if p and str(p).strip()]
        if len(pair) < 2:
            continue
        a, b = pair[:2]
        canon_targets = {_canon_title(a), _canon_title(b)}
        # earliest index of A or B (if present)
        idxs = [i for i, t in enumerate(keep) if _canon_title(t) in canon_targets]
        insert_at = min(idxs) if idxs else len(keep)
        # drop existing A/B entries
        keep = [t for t in keep if _canon_title(t) not in canon_targets]
        keep.insert(insert_at, f"{a} / {b}")

    # REWORD
    for rw in (verdict.get("reword") or []):
        src = (rw.get("from") or "").strip()
        dst = (rw.get("to") or "").strip()
        if src and dst:
            try:
                idx = next(i for i, t in enumerate(keep) if _canon_title(t) == _canon_title(src))
                keep[idx] = dst
            except StopIteration:
                keep.append(dst)

    # MISSING additions
    for m in (verdict.get("missing") or []):
        if m and str(m).strip():
            keep.append(m)

    # sanitize & dedupe
    keep = [_strip_ellipses(t) for t in keep]
    return _dedupe_titles(keep)

# ───────────────────────── rubric / draft / verify ─────────────────────────


def _draft_subtopics(topic: str, rubric: Dict[str, Any], audience: str, objective: str) -> List[str]:
    """
    Over-generate 30–50 draft subtopics guided by the rubric (no hard-coded inclusions).
    """
    schema = "{\"subtopics\": [\"...\"]}"
    ask = {
        "role": "user",
        "content": (
            f"Design 30–50 concise, single‑purpose pediatric sub‑topics for '{topic}'. "
            f"Use this rubric to ensure breadth and avoid omissions: {json.dumps(rubric, ensure_ascii=False)}. "
            f"Audience: {audience}. Objective: {objective}. "
            "Prefer decision/technique/criteria/data‑interpretation nodes. Avoid duplicates, avoid trivial variants. "
            f"Return JSON only as {schema}."
            f"Where relevant for this topic kind, make sure the list includes practice‑critical, "
            f"decision‑oriented nodes such as: triage/admission/discharge criteria; time‑phase or "
            f"week‑of‑illness diagnostic algorithms (how work‑up changes over time); specimen "
            f"handling/volumes/pre‑treatment sampling; outpatient vs inpatient review plans; "
            f"non‑response/treatment‑failure and escalation algorithms; imaging/ procedure thresholds "
            f"for complications; contact/household management and return‑to‑school/day‑care advice; "
            f"follow‑up and expected time‑to‑improvement/defervescence; recurrence/relapse vs "
            f"reinfection distinctions; special populations; systems/implementation and psychosocial support. "
            f"Only include those that truly fit THIS topic kind; avoid disease‑specific details or local policies."
        )
    }
    rsp = oai_client.chat.completions.create(
        model=DEPLOYMENT,
        messages=[{"role": "system", "content": "You are a paediatrics curriculum designer. Return JSON only."}, ask],
        temperature=0.4, max_tokens=900, response_format={"type": "json_object"},
    )
    data = json.loads(rsp.choices[0].message.content)
    return [t for t in (data.get("subtopics") or []) if str(t).strip()]

# --- generateSubtopics/__init__.py  (replace _make_rubric) -----------------
def _make_rubric(topic_name: str, audience: str, objective: str) -> Dict[str, Any]:
    """
    Build a topic-kind rubric with neutral 'dimensions'.
    Each dimension has: name, why, required (bool), weight (1-5).
    """
    sys = {
        "role": "system",
        "content": (
            'Return JSON only as {"topic_kind":"...",'
            '"dimensions":[{"name":"...","why":"...","required":true|false,"weight":1-5}]}.'
        ),
    }
    user = {
        "role": "user",
        "content": f"""
Classify this paediatrics topic and list 8–12 universal coverage DIMENSIONS
(no example subtopics). Add fields:
- required=true for safety-critical / decision-centric dimensions for THIS topic kind (e.g.,
  triage/disposition, diagnostic approach & data interpretation, treatment protocols incl. step‑down,
  treatment‑failure/escalation/rescue, complications recognition & imaging/procedure thresholds,
  follow‑up & counselling, special populations). Mark them required ONLY where applicable.
- weight=1..5 indicating importance for the declared topic_kind.

Topic: {topic_name}
Audience: {audience}
Objective: {objective}
Output ONLY the JSON schema described above.
""".strip(),
    }
    try:
        rsp = oai_client.chat.completions.create(
            model=DEPLOYMENT, messages=[sys, user],
            temperature=0.3, max_tokens=700, response_format={"type": "json_object"},
        )
        out = json.loads(rsp.choices[0].message.content)
        out["topic_kind"] = (out.get("topic_kind") or "other").strip()
        dims = out.get("dimensions") or []
        # Defensive shaping
        cleaned = []
        for d in dims:
            if not isinstance(d, dict) or not d.get("name"):
                continue
            d["required"] = bool(d.get("required", False))
            try:
                w = int(d.get("weight", 3))
            except Exception:
                w = 3
            d["weight"] = max(1, min(5, w))
            cleaned.append(d)
        out["dimensions"] = cleaned
        return out
    except Exception:
        # Minimal neutral fallback
        return {
            "topic_kind": "other",
            "dimensions": [
                {"name": "clinical_workflow", "why": "practical decisions", "required": True, "weight": 5},
                {"name": "core_science", "why": "foundational", "required": False, "weight": 2},
                {"name": "safety_quality", "why": "prevent harm", "required": True, "weight": 4},
            ],
        }


# --- generateSubtopics/__init__.py  (replace _verify_subtopics) ------------
def _verify_subtopics(topic: str, rubric: Dict[str, Any], titles: List[str],
                      coverage: List[Dict[str, Any]], audience: str, objective: str) -> Dict[str, Any]:
    """
    Ask GPT to check completeness vs rubric + flag missing/merge/drop/edit.
    Coverage may guide merges between near-duplicates, but MUST NOT veto required dimensions.
    """
    schema = {
        "complete": True,
        "missing": ["..."],               # titles to add
        "drop": ["..."],                  # titles to drop
        "merge": [["...","..."]],         # pairs to merge
        "reword": [{"from": "...", "to": "..."}],
        "notes": "..."                    # optional free-text for debug
    }
    ask = {
        "role": "user",
        "content": (
            f"Verify completeness of this paediatric sub-topic list for '{topic}' against the rubric.\n"
            f"Audience: {audience}. Objective: {objective}.\n"
            "Rules:\n"
            "• Treat RUBRIC.dimensions where required=true as MUST-COVER for THIS topic kind—even if corpus coverage is low.\n"
            "• Use COVERAGE only to decide which near-duplicates to MERGE; do NOT drop required dimensions due to low coverage.\n"
            "• Prefer single-purpose, decision-centric nodes for triage/admission/escalation, treatment-failure, complications rescue.\n"
            "• Propose concise titles; keep ≤ MAX if needed by merging low-importance clusters (epi/burden; prevention/education; systems).\n"
            "Return JSON only in this schema: " + json.dumps(schema, ensure_ascii=False) + "\n\n"
            "RUBRIC=" + json.dumps(rubric, ensure_ascii=False) + "\n"
            "TITLES=" + json.dumps(titles, ensure_ascii=False) + "\n"
            "COVERAGE=" + json.dumps(coverage, ensure_ascii=False)
        ),
    }
    try:
        rsp = oai_client.chat.completions.create(
            model=DEPLOYMENT,
            messages=[{"role": "system", "content": "Return JSON only."}, ask],
            temperature=0.2, max_tokens=900, response_format={"type": "json_object"},
        )
        return json.loads(rsp.choices[0].message.content)
    except Exception:
        return {"complete": True, "missing": [], "drop": [], "merge": [], "reword": [], "notes": ""}


def _apply_verification(titles: List[str], verdict: Dict[str, Any]) -> List[str]:
    """
    Apply drops, merges, additions, and rewording from verifier output.
    """
    drops = set((_canon_title(x) for x in (verdict.get("drop") or [])))
    keep = [t for t in titles if _canon_title(t) not in drops]

    # merges: [["A","B"], ...]  -> "A / B"
    for pair in verdict.get("merge") or []:
        pair = [p for p in pair if p and str(p).strip()]
        if len(pair) >= 2:
            keep.append(" / ".join(pair[:2]))

    # reword
    for rw in (verdict.get("reword") or []):
        src = (rw.get("from") or "").strip()
        dst = (rw.get("to") or "").strip()
        if src and dst:
            try:
                idx = next(i for i, t in enumerate(keep) if _canon_title(t) == _canon_title(src))
                keep[idx] = dst
            except StopIteration:
                keep.append(dst)

    # missing additions
    for m in (verdict.get("missing") or []):
        if m and str(m).strip():
            keep.append(m)

    return _dedupe_titles(keep)

# ───────────────────────── main entry ───────────────────────────


# ───────────────────────── main entry ───────────────────────────
def main(msg: func.QueueMessage) -> None:
    logging.info("generateSubtopics triggered")
    try:
        topic_id = json.loads(msg.get_body().decode())["topic_id"]
    except Exception:
        logging.error("Bad queue message - expected JSON with topic_id")
        return

    conn_str = "DRIVER={ODBC Driver 18 for SQL Server};SERVER=20.171.24.17;DATABASE=CME2;UID=new_root;PWD=japl@bJBYV77;Encrypt=no;TrustServerCertificate=yes;"

    # B) fetch topic & queued placeholders
    with pyodbc.connect(conn_str) as conn:
        cur = conn.cursor()
        cur.execute("SELECT topic_name FROM cme.topics WHERE topic_id = ?", topic_id)
        row = cur.fetchone()
        if not row:
            logging.error("Topic %s not found", topic_id)
            return
        topic_name = row.topic_name

        cur.execute("""
        SELECT subtopic_id
        FROM cme.subtopics
        WHERE topic_id = ? AND status = 'queued'
        ORDER BY sequence_no
        """, topic_id)
        queued_sub_ids = [r.subtopic_id for r in cur.fetchall()]

    use_index = (SUBTOPIC_SOURCE != 'gpt')

    # C) Build outline
    vignette_docs: list[dict] = []
    cov_map: dict[str, int] = {}

    if use_index:
        resolved_topic, outline, vignette_docs = _outline_from_index(topic_name)
        titles = [r["subtopic"] for r in outline if r.get("subtopic")]
        cov_map = {r["subtopic"]: int(r.get("coverage_chars") or 0) for r in outline}

        # Align DB topic_name with index topic if they differ
        if resolved_topic and resolved_topic.strip() and resolved_topic.strip() != topic_name:
            with pyodbc.connect(conn_str) as conn:
                cur = conn.cursor()
                cur.execute("UPDATE cme.topics SET topic_name=? WHERE topic_id=?", resolved_topic, topic_id)
                conn.commit()
            topic_name = resolved_topic

        logging.info("Index outline size (excluding vignette sections): %d", len(titles))

        # If outline is empty, fall back to GPT to avoid stalling the pipeline
        if not titles:
            use_index = False

    if not use_index:
        # legacy GPT-driven outline (kept for fallback)
        audience = AUDIENCE_DEFAULT
        objective = OBJECTIVE_DEFAULT
        rubric = _make_rubric(topic_name, audience, objective)
        draft = _draft_subtopics(topic_name, rubric, audience, objective)
        draft = _dedupe_titles(draft)
        titles = _enforce_count(topic_name, draft, rubric)
        for _ in range(2):
            cov = _coverage_stats(topic_name, titles)
            verdict = _verify_subtopics(topic_name, rubric, titles, cov, audience, objective)
            if bool(verdict.get("complete", False)):
                break
            titles = _apply_verification(titles, verdict)
            titles = _dedupe_titles(titles)
            titles = _enforce_count(topic_name, titles, rubric)
        logging.info("Final outline size after verify/repair: %d", len(titles))

    # D) update placeholders, delete extras, insert additions
    affected_ids: List[str] = []
    with pyodbc.connect(conn_str) as conn:
        cur = conn.cursor()
        # Update existing placeholders up to available titles
        for i, sub_id in enumerate(queued_sub_ids[:len(titles)]):
            title = titles[i]
            cur.execute("""
                UPDATE cme.subtopics
                SET title = ?, status = 'refs_pending', sequence_no = ?
                WHERE subtopic_id = ?
            """, title, i + 1, sub_id)
            affected_ids.append(sub_id)

        # Delete surplus placeholders if any
        for sub_id in queued_sub_ids[len(titles):]:
            cur.execute("DELETE FROM cme.subtopics WHERE subtopic_id = ?", sub_id)

        # Insert additional subtopics if titles exceed placeholders
        for seq, title in enumerate(titles[len(queued_sub_ids):], start=len(queued_sub_ids) + 1):
            new_id = str(uuid.uuid4())
            cur.execute("""
                INSERT INTO cme.subtopics
                (subtopic_id, topic_id, title, sequence_no, status)
                VALUES (?, ?, ?, ?, 'refs_pending')
            """, new_id, topic_id, title, seq)
            affected_ids.append(new_id)

        conn.commit()

    # E) compute coverage & tag insufficiency
    with pyodbc.connect(conn_str) as conn:
        cur = conn.cursor()
        for sub_id in affected_ids:
            cur.execute("SELECT title, topic_id FROM cme.subtopics WHERE subtopic_id=?", sub_id)
            row = cur.fetchone()
            if not row:
                continue
            title, t_id = row.title, row.topic_id

            # Index-driven coverage when available; else, fallback search-estimate
            score = int(cov_map.get(title) or 0)
            if score <= 0:
                score = _estimate_coverage(topic_name, title)

            status = 'ok' if score >= COVERAGE_MIN_CHARS else 'insufficient'
            note = None if status == 'ok' else f"Coverage < {COVERAGE_MIN_CHARS} chars in search corpus"

            cur.execute("""
                UPDATE cme.subtopics
                SET coverage_score = ?, content_status = ?, coverage_note = ?
                WHERE subtopic_id = ?
            """, score, status, note, sub_id)

            if status != 'ok':
                cur.execute("""
                    INSERT INTO cme.content_gaps (topic_id, subtopic_id, subtopic_title, coverage_score, reason)
                    VALUES (?, ?, ?, ?, ?)
                """, t_id, sub_id, title, score, note)

        conn.commit()

    # F) NEW: Ingest vignette sections (if any) and queue case-MCQ generation
    try:
        if vignette_docs:
            n_cases = _ingest_vignette_cases(topic_id, topic_name, vignette_docs, conn_str)
            logging.info("Ingested %d case(s) from vignette sections", n_cases)
    except Exception:
        logging.exception("Vignette ingestion failed")

    logging.info("Sub-topic list for %s updated -> refs_pending", topic_name)
