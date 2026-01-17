from __future__ import annotations
import logging, os, json, textwrap, unicodedata, re
import azure.functions as func
import pyodbc
from openai import AzureOpenAI
from azure.storage.queue import QueueClient
from azure.core.credentials import AzureKeyCredential
from azure.search.documents import SearchClient

SEARCH_ENDPOINT = "https://basic-rag-sandbox.search.windows.net"
SEARCH_ADMIN_KEY = "tuqRZ8A374Aw3wXKSTzOY6SEu6Ra8rOyhPgFEtcLpSAzSeBOByQL"
INDEX_NAME = "pubert-demo-new"
SEARCH_API_VERSION = "2025-05-01-preview"
SEARCH_TOP_K = 12  # ↑ give GPT more material
MAX_CHARS = 4500  # ↑ allow ~1.5× more source text
DUP_SIM_THRESHOLD = float(os.getenv("CONCEPT_DUP_SIM_THRESHOLD", "0.92"))
SUBTOK_MIN_HITS   = int(os.getenv("SUBTOK_MIN_HITS", "2"))
conn = "DRIVER={ODBC Driver 18 for SQL Server};SERVER=20.171.24.17;DATABASE=CME2;UID=new_root;PWD=japl@bJBYV77;Encrypt=no;TrustServerCertificate=yes;"

search_cli = SearchClient(
    endpoint=os.environ.get("SEARCH_ENDPOINT", SEARCH_ENDPOINT),
    index_name=os.environ.get("SEARCH_INDEX", INDEX_NAME),
    credential=AzureKeyCredential(os.environ.get("SEARCH_ADMIN_KEY", SEARCH_ADMIN_KEY)),
    api_version=os.environ.get("SEARCH_API_VERSION", SEARCH_API_VERSION),
)




# ───────────────────────── Azure Search (hierarchical fetch) ─────────────────────────
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


def _search_all(*, search_text: str, **kwargs) -> list[dict]:
    """Collect all results for a query (paginated by skip/top)."""
    out: list[dict] = []
    skip = 0
    top = int(kwargs.pop('top', 1000) or 1000)
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


def _fetch_index_docs(topic_name: str, sub_title: str) -> list[dict]:
    """Fetch ALL chunks for (topic, subtopic) from Azure Search, including sub-subtopics."""
    select = [
        'id', 'content', 'topic', 'subtopic', 'sub_subtopic',
        'heading_path', 'sequence', 'chunk_index', 'total_chunks'
    ]
    filt = f"topic eq '{_escape_odata(topic_name)}' and subtopic eq '{_escape_odata(sub_title)}'"
    docs = _search_all(search_text='*', filter=filt, select=select, top=1000)

    # Fallback if DB topic_name differs from index casing or wording
    if not docs:
        filt2 = f"subtopic eq '{_escape_odata(sub_title)}'"
        docs2 = _search_all(search_text=topic_name, filter=filt2, select=select, top=250)
        if docs2:
            freq: dict[str, int] = {}
            for d in docs2:
                t = (d.get('topic') or '').strip()
                if t:
                    freq[t] = freq.get(t, 0) + 1
            if freq:
                best_topic = max(freq.items(), key=lambda kv: kv[1])[0]
                filt3 = f"topic eq '{_escape_odata(best_topic)}' and subtopic eq '{_escape_odata(sub_title)}'"
                docs = _search_all(search_text='*', filter=filt3, select=select, top=1000)

    # Stable ordering: sequence, heading_path, sub_subtopic, chunk_index
    def doc_key(d: dict):
        return (
            _sequence_key(d.get('sequence') or ''),
            (d.get('heading_path') or ''),
            (d.get('sub_subtopic') or ''),
            int(d.get('chunk_index') or 0),
            (d.get('id') or '')
        )
    docs.sort(key=doc_key)
    return docs


def _compose_concept_from_index(docs: list[dict], max_chars: int = MAX_CHARS) -> str:
    """Merge hierarchical chunks into a single raw source string for GPT rewriting."""
    if not docs:
        return ''

    # group by sub_subtopic (empty grouped first)
    grouped: dict[str, list[str]] = {}
    for d in docs:
        key = (d.get('sub_subtopic') or '').strip()
        grouped.setdefault(key, []).append((d.get('content') or '').strip())

    blocks: list[str] = []
    # Keep empty group first, then alpha
    keys = sorted(grouped.keys(), key=lambda k: (1 if k else 0, k.lower()))
    for k in keys:
        parts = [p for p in grouped[k] if p]
        if not parts:
            continue
        joined = "\n\n".join(parts)
        if k:
            blocks.append(f"SUB-SUBTOPIC: {k}\n{joined}")
        else:
            blocks.append(joined)

    out = "\n\n".join(blocks).strip()
    return out[:max_chars]
def _compose_concept(source_ids: list[str], max_chars=MAX_CHARS) -> str:
    parts = []
    for sid in source_ids[:12]:  # allow more chunks but same corpus
        try:
            doc = search_cli.get_document(sid)
            if (txt := (doc.get("content") or "").strip()):
                parts.append(txt)
        except Exception:
            pass
        if sum(len(p) for p in parts) >= max_chars:
            break
    return ("\n\n".join(parts))[:max_chars].strip()
def _mark_insufficient(subtopic_id: str, reason: str = "Insufficient source text") -> None:
    with pyodbc.connect(conn) as sql:
        cur = sql.cursor()
        cur.execute("""
            UPDATE cme.subtopics
            SET status='concept_skipped', case_amenable=0, case_status='skipped'
            WHERE subtopic_id=?
        """, subtopic_id)
        sql.commit()
    logging.info("Subtopic %s marked skipped: %s", subtopic_id, reason)
# ── NEW: generic relevance + duplicate helpers ─────────────────────────────
STOPWORDS = {"and","or","the","a","an","to","of","for","in","on","with","by","as","from","into","using","use","vs","vs."}
def _norm(txt: str) -> str:
    return re.sub(r"[^a-z0-9]+", " ", (txt or "").lower()).strip()
def _kw(txt: str) -> list[str]:
    return [t for t in _norm(txt).split() if len(t) >= 3 and t not in STOPWORDS]
def _has_min_hits(text: str, sub_title: str, min_hits: int) -> bool:
    needles = set(_kw(sub_title))
    min_hits = min(min_hits, max(1, len(needles)))
    h = " " + _norm(text) + " "
    return sum(1 for n in needles if f" {n} " in h) >= min_hits
def _shingles(text: str, n: int = 5) -> set[str]:
    toks = _norm(text).split()
    return {" ".join(toks[i:i+n]) for i in range(len(toks)-n+1)} if len(toks) >= n else set()
def _jaccard(a: set[str], b: set[str]) -> float:
    return (len(a & b) / len(a | b)) if a and b else 0.0

AZURE_OAI_ENDPOINT = "https://azure1405.openai.azure.com/"
AZURE_OAI_KEY = "CzrrWvXbsmYcNguU1SqBpE9HDhhbfYsbkq3UedythCYCV9zNQ4mLJQQJ99BEACHYHv6XJ3w3AAABACOGiIPm"
DEPLOYMENT = "gpt-4o"
AZURE_OAI_API_VERSION = "2024-02-15-preview"

# ─────────────────── Azure-OpenAI client ────────────────────
oai = AzureOpenAI(
    api_key=AZURE_OAI_KEY,
    azure_endpoint=AZURE_OAI_ENDPOINT,
    api_version=AZURE_OAI_API_VERSION,
)


# ─────────────────── Helpers ────────────────────────────────
def _ascii_fold(txt: str) -> str:  # ASCII-fold to help GPT tokens
    return unicodedata.normalize("NFKD", txt).encode("ascii", "ignore").decode()


def _make_outline(subtopic_title: str) -> str:
    k = (subtopic_title or "").lower()
    
    # infection-specific slots
    if any(w in k for w in ("triage", "admission", "escalat")):
        return ("Admission & escalation criteria (vitals, dehydration, neuro, bleed); "
                "initial labs; stabilization steps; thresholds for PICU; discharge & review triggers")
    
    if any(w in k for w in ("persistent", "relapse", "failure", "deferv", "day 3", "day 5")):
        return ("Expected defervescence timeline; when treatment failure is suspected; "
                "stepwise work‑up (cultures/sensitivity/imaging); switch/extend therapy; follow‑up")
    
    if any(w in k for w in ("carrier", "clearance", "food handler")):
        return ("When to suspect carriage; stool culture schedule & clearance criteria; "
                "household/school precautions; public‑health reporting")
    
    if any(w in k for w in ("household", "outbreak", "contact")):
        return ("Who to screen/test; prophylaxis/vaccination guidance; sanitation & food/water hygiene; "
                "return precautions; community/outbreak reporting")
    
    if "complication" in k:
        return ("Early vs late complications; red-flag warning signs; "
                "pathophysiology in brief; bedside monitoring & escalation; "
                "definitive management and follow-up")
    
    if "diagnos" in k:
        return ("Core clinical features; key differentials; definitive tests "
                "(with typical sensitivity/specificity where stated); "
                "sampling pitfalls; interpretation dos & don'ts")
    
    if "treat" in k or "therap" in k:
        return ("First-line regimen with exact doses and durations as stated; "
                "alternatives for allergy/intolerance; MDR/XDR protocols; "
                "monitoring & adverse effects")
    
    if "vaccin" in k or "immun" in k:
        return ("Licensed vaccines (India); schedules; efficacy & waning; "
                "contraindications; catch-up and special groups")
    
    if "epidemiolog" in k or "burden" in k:
        return ("Burden; transmission; risk factors; sanitation/prevention "
                "messages for caregivers")
    
    # generic fallback
    return ("Key facts specific to this sub-topic only; practical points "
            "for bedside decision-making")


def _looks_clipped(txt: str) -> bool:
    """Heuristic: last char must be terminal punctuation; no hanging list markers/parentheses."""
    if txt and len(txt.strip()) < 400:
        return True
    
    t = txt.strip()
    if t[-1] not in ".!?":
        return True
    
    # obvious truncation patterns
    if re.search(r"(,\s*)?$", t[-3:]):
        return True
    
    if t.count("(") != t.count(")"):
        return True
    
    return False

CASE_AMENABLE_MIN_CONF = int(os.getenv("CASE_AMENABLE_MIN_CONF", "55"))
CASE_MAX_FRACTION = float(os.getenv("CASE_MAX_FRACTION", "0.5"))  # ≤ half
CASE_BUDGET_STATUSES = ("pending", "ready", "verified")            # consume budget
CASE_REBALANCE_MAX_CANDIDATES = int(os.getenv("CASE_REBALANCE_MAX_CANDIDATES", "28"))

def _coerce_confidence(x) -> int:
    """
    Coerce model 'confidence' into an int 0–100.
    Accepts ints, floats, '79', '79%', or '0.79' (interpreted as 79).
    """
    try:
        if isinstance(x, (int, float)):
            val = float(x)
        elif isinstance(x, str):
            import re
            m = re.search(r"(\d+(?:\.\d+)?)", x)
            if not m:
                return 0
            val = float(m.group(1))
        else:
            return 0
        if 0 <= val <= 1:
            val *= 100.0
        val = int(round(val))
        return max(0, min(100, val))
    except Exception:
        return 0
    
def _case_budget_limits(cur, topic_id: str) -> tuple[int, int]:
    """Return (cap, pinned_used).

    Pinned means:
    - subtopics already marked ready/verified, OR
    - subtopics that already have at least one case row (e.g., vignette-ingested)

    This prevents the case-budget rebalance from demoting or re-selecting subtopics
    that already carry case content.
    """
    cur.execute("SELECT COUNT(*) FROM cme.subtopics WHERE topic_id=?", topic_id)
    total = cur.fetchone()[0] or 0
    cap = max(0, int(total * CASE_MAX_FRACTION))

    cur.execute("""
        SELECT COUNT(DISTINCT s.subtopic_id)
        FROM cme.subtopics s
        LEFT JOIN cme.cases cs ON cs.subtopic_id = s.subtopic_id
        WHERE s.topic_id=?
          AND (s.case_status IN ('ready','verified') OR cs.case_id IS NOT NULL)
    """, topic_id)
    pinned = cur.fetchone()[0] or 0
    return cap, pinned


def _rank_case_candidates_gpt(topic: str, items: list[dict], slots: int) -> list[str]:
    """
    items = [{"id": "...", "title": "...", "snippet": "first 350 chars of concept"}]
    Return list of subtopic_ids (length ≤ slots) in DESC priority.
    """
    schema = {"pick": ["..."], "why": "short note"}
    ask = {
        "role": "user",
        "content": (
            "Select up to N items that gain the MOST from a clinical case vignette.\n"
            "Prioritise decision-impact (apply/interpret): triage/disposition thresholds; diagnostic "
            "approach & data interpretation; treatment-failure & escalation; complications recognition "
            "& rescue; imaging/procedure thresholds; nuanced counselling.\n"
            "Down-rank static science (pathophysiology), basic epidemiology, generic prevention/education "
            "unless the snippet shows concrete decision points.\n"
            f"N={slots}\nITEMS=" + json.dumps(items, ensure_ascii=False) + "\n\n"
            "Return JSON only as " + json.dumps(schema)
        ),
    }
    if slots <= 0 or not items:
        return []
    try:
        rsp = oai.chat.completions.create(
            model=DEPLOYMENT,
            messages=[{"role": "system", "content": "Return JSON only."}, ask],
            temperature=0.2, max_tokens=700, response_format={"type": "json_object"},
        )
        out = json.loads(rsp.choices[0].message.content)
        picks = [p for p in (out.get("pick") or []) if isinstance(p, str)]
        return picks[:slots]
    except Exception:
        return []


def _rebalance_case_budget(topic_id: str, topic_name: str) -> list[str]:
    """Promote top-K amenable candidates to 'pending' based on ranked decision impact.

    IMPORTANT: subtopics that already have any case rows (e.g., vignette-ingested) are
    treated as pinned and are excluded from both promotion and demotion.

    Returns list of subtopic_ids that were just promoted (caller will queue them).
    """
    promoted_now: list[str] = []
    with pyodbc.connect(conn) as sql:
        cur = sql.cursor()

        cap, pinned = _case_budget_limits(cur, topic_id)
        avail = max(0, cap - pinned)
        if avail <= 0:
            # Demote any stray 'pending' that do not already have cases
            cur.execute("""
                UPDATE s
                SET s.case_status='candidate'
                FROM cme.subtopics s
                WHERE s.topic_id=?
                  AND s.case_status='pending'
                  AND NOT EXISTS (SELECT 1 FROM cme.cases cs WHERE cs.subtopic_id = s.subtopic_id)
            """, topic_id)
            sql.commit()
            return []

        # Candidate pool: amenable candidates/pending WITHOUT existing cases
        cur.execute("""
            SELECT s.subtopic_id, s.title
            FROM cme.subtopics s
            WHERE s.topic_id=?
              AND s.case_amenable=1
              AND s.case_status IN ('candidate','pending')
              AND NOT EXISTS (SELECT 1 FROM cme.cases cs WHERE cs.subtopic_id = s.subtopic_id)
        """, topic_id)
        rows = cur.fetchall()
        pool = [{"id": r.subtopic_id, "title": r.title} for r in rows]

        # Attach short concept snippets
        items = []
        for p in pool[:CASE_REBALANCE_MAX_CANDIDATES]:
            cur.execute("""
                SELECT TOP 1 content FROM cme.concepts WHERE subtopic_id=? ORDER BY concept_id
            """, p["id"])
            crow = cur.fetchone()
            snippet = ((crow.content if crow else "") or "")[:350]
            items.append({"id": p["id"], "title": p["title"], "snippet": snippet})

        winners = set(_rank_case_candidates_gpt(topic_name, items, avail))

        # Promote winners; demote losers (only those without existing cases)
        for p in pool:
            sid = p["id"]
            cur.execute("SELECT case_status FROM cme.subtopics WHERE subtopic_id=?", sid)
            status = (cur.fetchone()[0] or "").lower()
            if sid in winners:
                if status == "candidate":
                    cur.execute("UPDATE cme.subtopics SET case_status='pending' WHERE subtopic_id=?", sid)
                    promoted_now.append(sid)
            else:
                if status == "pending":
                    cur.execute("UPDATE cme.subtopics SET case_status='candidate' WHERE subtopic_id=?", sid)

        sql.commit()
    return promoted_now


def _case_budget_allows(cur, topic_id: str) -> tuple[bool, int, int]:
    """
    Return (allowed_now, used, cap) for the topic's case-study budget.
    used = count of subtopics already consuming the budget
    cap  = floor(total_subtopics * CASE_MAX_FRACTION)
    """
    # total subtopics for this topic
    cur.execute("SELECT COUNT(*) FROM cme.subtopics WHERE topic_id = ?", topic_id)
    total = cur.fetchone()[0] or 0
    cap = max(0, int(total * CASE_MAX_FRACTION))

    # how many already consuming budget (i.e., not skipped)
    placeholders = ",".join("?" * len(CASE_BUDGET_STATUSES))
    cur.execute(f"""
        SELECT COUNT(*)
        FROM cme.subtopics
        WHERE topic_id = ?
          AND case_status IN ({placeholders})
    """, topic_id, *CASE_BUDGET_STATUSES)
    used = cur.fetchone()[0] or 0

    return (used < cap), used, cap

def _assess_case_amenable_gpt(topic: str, subtopic_title: str, concept_text: str) -> tuple[bool, int, dict]:
    """
    Use the LLM to decide if a short clinical case vignette adds learning value
    for THIS subtopic + concept. Returns (amenable: bool, confidence: 0-100, raw_json: dict).

    Decision principle (model sees these rules):
    - TRUE if the subtopic benefits from applied reasoning: triage/disposition thresholds,
      algorithms, differential diagnosis, test interpretation, escalation/rescue, recognition of complications,
      dose/route adjustments, MDR/XDR branching, counselling with context, or any scenario where
      clinical data change the decision.
    - FALSE if static knowledge dominates: pure definitions, etiology/classification lists without actions,
      background epidemiology only, lab technique without patient context, product lists/schedules with no branching,
      admin/policy summaries, generic prevention messages without patient‑level decisions.
    - FALSE if concept text is too thin to support a meaningful, self‑contained vignette.
    """

    ask = {
        "role": "user",
        "content": f"""
        Decide if a brief paediatric clinical case vignette would ADD learning value for this sub‑topic.
        Return ONLY a JSON object with these keys (no extra keys, no prose):
        "amenable": true|false,
        "confidence": integer 0–100 (NO "%" sign),
        "why": string ≤200 characters on learning gain/applicability,
        "suggested_case_focus": array of short strings (e.g., ["triage thresholds","data interpretation"])

        Context
        ───────
        Topic: {topic}
        Sub‑topic: {subtopic_title}
        Concept (for context; do not invent new facts):
        {(concept_text or '')[:2500]}
        """.strip()
            }

    try:
        rsp = oai.chat.completions.create(
            model=DEPLOYMENT,
            messages=[
                {"role": "system", "content": "You are a paediatrics curriculum editor. Return JSON only."},
                ask
            ],
            temperature=0.2,
            max_tokens=400,
            response_format={"type": "json_object"},
        )
        data = json.loads(rsp.choices[0].message.content)
        amen = True if data.get("amenable") is True else False
        conf = _coerce_confidence(data.get("confidence", 0))
        return amen, conf, data
    except Exception:
        import logging
        logging.exception("Case amenability check failed")
        # Be conservative and skip.
        return False, 0, {"amenable": False, "confidence": 0, "why": "AI call failed", "suggested_case_focus": []}

def _call_gpt(topic: str, subtopic: str, snippets: list[str], disambiguation_hint: str = "") -> str:
    joined = "\n".join(snippets)[:MAX_CHARS]
    outline = _make_outline(subtopic)
    
    disambig_instruction = ""
    if disambiguation_hint:
        disambig_instruction = f"\n• DISAMBIGUATION: {disambiguation_hint}\n"
    
    user = f"""
Rewrite the SOURCE into a single coherent paragraph (≈250–350 words)
for paediatric post-graduates. You MUST:
• Preserve every named threshold, dose, duration, sensitivity/specificity value, and timing window verbatim if present.
• Remove bullets/odd markers; write complete sentences only.
• Organise content as: {outline}.
• Stay strictly within the sub-topic "{subtopic}"; no off-topic drift.
• Keep the framing strictly paediatric; exclude pregnancy/lactation/adult-only contexts unless explicitly present in the sub-topic title.
• If a required element in the outline is not present in SOURCE, write "Not specified in source." Do not invent content.
• Do NOT invent facts not present in source.{disambig_instruction}
— SOURCE TEXT —
{joined}
— END SOURCE —
""".strip()
    
    rsp = oai.chat.completions.create(
        model=DEPLOYMENT,
        temperature=0.35,
        max_tokens=900,
        messages=[
            {"role": "system", "content": "You are an expert paediatric writer."},
            {"role": "user", "content": user},
        ],
    )
    
    return rsp.choices[0].message.content.strip()

NEIGHBOR_WINDOW = int(os.getenv("NEIGHBOR_WINDOW", "1"))

def _expand_neighbors(source_ids: list[str]) -> list[str]:
    # collect ±1 neighbors of chunk-suffixed ids like "base_07"
    extra = []
    for sid in source_ids:
        m = re.match(r"^(.*?)[_\-](\d{2,})$", sid)
        if not m:
            continue
        base, idx = m.group(1), int(m.group(2))
        for d in range(-NEIGHBOR_WINDOW, NEIGHBOR_WINDOW + 1):
            if d == 0:
                continue
            j = idx + d
            if j >= 0:
                extra.append(f"{base}_{j:02d}")
    # keep originals first, then neighbors (dedup, preserve order)
    seen, out = set(), []
    for x in list(source_ids) + extra:
        if x not in seen:
            out.append(x); seen.add(x)
    return out

# ─────────────────── Main entry ─────────────────────────────
# ─────────────────── Main entry ─────────────────────────────
def main(msg: func.QueueMessage) -> None:
    logging.info("generateConcept triggered")

    try:
        subtopic_id = json.loads(msg.get_body().decode())["subtopic_id"]
    except Exception:
        logging.error("Bad queue payload - expected {'subtopic_id': ...}")
        return

    # 1) fetch titles
    with pyodbc.connect(conn) as sql:
        cur = sql.cursor()
        cur.execute("""
            SELECT t.topic_name, s.title, s.topic_id
            FROM cme.subtopics AS s
            JOIN cme.topics AS t ON t.topic_id = s.topic_id
            WHERE s.subtopic_id = ?
        """, subtopic_id)
        row = cur.fetchone()
        if not row:
            logging.error("Sub-topic %s not found", subtopic_id)
            return
        topic_name, sub_title, topic_id = row

        cur.execute("SELECT COUNT(*) FROM cme.cases WHERE subtopic_id=?", subtopic_id)
        existing_case_count = cur.fetchone()[0] or 0

    # 2) Pull raw hierarchical content from Azure Search by (topic, subtopic)
    docs = _fetch_index_docs(topic_name, sub_title)
    raw_txt = _compose_concept_from_index(docs, max_chars=MAX_CHARS)

    MIN_SOURCE_CHARS = int(os.getenv("MIN_SOURCE_CHARS", "400"))
    SOFT_MIN_SOURCE_CHARS = int(os.getenv("SOFT_MIN_SOURCE_CHARS", "250"))

    if (not raw_txt or len(raw_txt) < SOFT_MIN_SOURCE_CHARS):
        _mark_insufficient(subtopic_id, reason=f"Source text < {SOFT_MIN_SOURCE_CHARS} chars (index fetch)")
        return

    # 3) Ask GPT to rewrite cleanly
    paragraph = _call_gpt(topic_name, sub_title, [raw_txt])

    if _looks_clipped(paragraph):
        logging.warning("Concept looks clipped -> retrying once with finish instruction")
        paragraph = _call_gpt(
            topic_name,
            sub_title,
            [raw_txt + "\n\n(Ensure the rewrite ends with a complete sentence and no hanging lists.)"],
        )

    if not paragraph or len(paragraph) < 400:
        logging.error("GPT rewrite failed for %s", subtopic_id)
        _mark_insufficient(subtopic_id, reason="Model rewrite too short")
        return

    # 4) Relevance lint
    if not _has_min_hits(paragraph, sub_title, SUBTOK_MIN_HITS):
        logging.warning("Concept failed relevance lint for %s", subtopic_id)
        _mark_insufficient(subtopic_id, reason="Low lexical overlap with sub-topic tokens")
        return

    # 5) Near-duplicate guard within same topic with regeneration attempt
    dup_of_subtopic_id = None
    with pyodbc.connect(conn) as sql_dups:
        cur2 = sql_dups.cursor()
        cur2.execute("""
            SELECT s.subtopic_id, s.title, c.content
            FROM cme.concepts c
            JOIN cme.subtopics s ON s.subtopic_id = c.subtopic_id
            WHERE s.topic_id = ? AND s.subtopic_id <> ?
        """, topic_id, subtopic_id)

        target_fp = _shingles(paragraph, n=5)
        closest_siblings = []

        for sib_id, sib_title, sib_text in cur2.fetchall():
            sim = _jaccard(target_fp, _shingles(sib_text or "", n=5))
            if sim >= DUP_SIM_THRESHOLD:
                closest_siblings.append((sim, sib_id, sib_title))

        if closest_siblings:
            closest_siblings.sort(reverse=True, key=lambda x: x[0])
            top_siblings = closest_siblings[:2]

            logging.warning(
                "Concept near-duplicate detected (%.2f with '%s') -> attempting disambiguation",
                top_siblings[0][0], top_siblings[0][2]
            )

            sibling_titles = ", ".join([f"'{sib[2]}'" for sib in top_siblings])
            disambig_hint = (
                f"Avoid overlap with {sibling_titles}; emphasize the unique aspects specific to '{sub_title}'."
            )

            paragraph_v2 = _call_gpt(topic_name, sub_title, [raw_txt], disambiguation_hint=disambig_hint)

            target_fp_v2 = _shingles(paragraph_v2, n=5)
            still_duplicate = False
            for sim_orig, sib_id, sib_title in top_siblings:
                cur2.execute("SELECT content FROM cme.concepts WHERE subtopic_id=?", sib_id)
                sib_row = cur2.fetchone()
                if sib_row:
                    sim_v2 = _jaccard(target_fp_v2, _shingles(sib_row.content or "", n=5))
                    if sim_v2 >= DUP_SIM_THRESHOLD:
                        still_duplicate = True
                        dup_of_subtopic_id = sib_id
                        logging.warning(
                            "After disambiguation, still near-duplicate (%.2f) with '%s'",
                            sim_v2, sib_title
                        )
                        break

            paragraph = paragraph_v2

    # 6) insert + status update + next-queue(s)
    with pyodbc.connect(conn) as sql:
        cur = sql.cursor()

        if dup_of_subtopic_id:
            cur.execute("""
                INSERT INTO cme.concepts (concept_id, subtopic_id, content, token_count, coverage_note, created_utc)
                VALUES (NEWID(), ?, ?, 0, ?, SYSUTCDATETIME())
            """, subtopic_id, paragraph, f"dup_of:{dup_of_subtopic_id}")
        else:
            cur.execute("""
                INSERT INTO cme.concepts (concept_id, subtopic_id, content, token_count, created_utc)
                VALUES (NEWID(), ?, ?, 0, SYSUTCDATETIME())
            """, subtopic_id, paragraph)

        concept_text = paragraph

        # Case-amenability: preserve vignette-ingested cases
        if existing_case_count > 0:
            cur.execute("""
                UPDATE cme.subtopics
                SET status='mcq_pending', case_amenable=1,
                    case_status = CASE
                        WHEN case_status IN ('ready','verified','failed','skipped') THEN case_status
                        ELSE 'pending'
                    END
                WHERE subtopic_id=?
            """, subtopic_id)
        else:
            amen_raw, conf, details = _assess_case_amenable_gpt(topic_name, sub_title, concept_text)
            amen = bool(amen_raw and (conf >= CASE_AMENABLE_MIN_CONF))

            if amen:
                cur.execute("""
                    UPDATE cme.subtopics
                    SET status='mcq_pending', case_amenable=1, case_status='candidate'
                    WHERE subtopic_id=?
                """, subtopic_id)
            else:
                cur.execute("""
                    UPDATE cme.subtopics
                    SET status='mcq_pending', case_amenable=0, case_status='skipped'
                    WHERE subtopic_id=?
                """, subtopic_id)

        sql.commit()

    # 7) Rebalance case budget and queue messages
    promoted = _rebalance_case_budget(topic_id, topic_name)

    try:
        q = QueueClient.from_connection_string(os.environ["AzureWebJobsStorage"], "mcq-queue")
        q.send_message(json.dumps({"subtopic_id": subtopic_id}))

        if promoted:
            cq = QueueClient.from_connection_string(os.environ["AzureWebJobsStorage"], "case-queue")
            for sid in promoted:
                cq.send_message(json.dumps({"subtopic_id": sid}))

        if dup_of_subtopic_id:
            logging.info("Concept saved -> mcq_pending; marked duplicate of %s; case candidates rebalanced", dup_of_subtopic_id)
        else:
            logging.info("Concept saved -> mcq_pending; case candidates rebalanced")
    except Exception as e:
        logging.error("Could not queue next tasks: %s", e)
