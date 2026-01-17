from __future__ import annotations
import logging, os, json, uuid, hashlib, re
from textwrap import shorten

import azure.functions as func
import pyodbc
from azure.core.credentials import AzureKeyCredential
from azure.search.documents import SearchClient
from azure.storage.queue import QueueClient

# ─────────────────────────────── Azure Search
SEARCH_ENDPOINT = os.environ.get("SEARCH_ENDPOINT", "https://basic-rag-sandbox.search.windows.net")
SEARCH_ADMIN_KEY = os.environ.get("SEARCH_ADMIN_KEY", "tuqRZ8A374Aw3wXKSTzOY6SEu6Ra8rOyhPgFEtcLpSAzSeBOByQL")
INDEX_NAME = os.environ.get("SEARCH_INDEX", "pubert-demo-new")
SEARCH_API_VERSION = os.environ.get("SEARCH_API_VERSION", "2025-05-01-preview")

# ─────────────────────────────── SQL Server
DB_CONN = os.getenv("DB") or os.getenv("DB_CONN") or     "DRIVER={ODBC Driver 18 for SQL Server};SERVER=20.171.24.17;DATABASE=CME2;UID=new_root;PWD=japl@bJBYV77;Encrypt=no;TrustServerCertificate=yes;"

search_cli = SearchClient(
    endpoint=SEARCH_ENDPOINT,
    index_name=INDEX_NAME,
    credential=AzureKeyCredential(SEARCH_ADMIN_KEY),
    api_version=SEARCH_API_VERSION,
)

# ────────────────────────── helpers ──────────────────────────
_URL_MD = re.compile(r"\]\((https?://[^)\s]+)\)", re.I)
_URL_ANY = re.compile(r"(https?://[^\s\)]+)", re.I)

def _escape_odata(s: str) -> str:
    return (s or "").replace("'", "''")

def _clean_url(url: str) -> str:
    u = (url or "").strip()
    if not u:
        return ""
    # fix common spacing artefacts seen in your manifests (e.g., 'http s://')
    u = re.sub(r"(?i)\bhttp\s+s://", "https://", u)
    u = re.sub(r"(?i)\bhttps\s*://", "https://", u)
    u = re.sub(r"(?i)\bhttp\s*://", "http://", u)
    u = re.sub(r"\s+", "", u)
    return u

def _extract_url(ref: str) -> str:
    s = (ref or "").strip()
    if not s:
        return ""
    m = _URL_MD.search(s)
    if m:
        return _clean_url(m.group(1))
    m2 = _URL_ANY.search(s)
    if m2:
        return _clean_url(m2.group(1))
    return ""

def _search_all(*, search_text: str, **kwargs) -> list[dict]:
    out: list[dict] = []
    skip = 0
    page_size = int(kwargs.pop("top", 1000) or 1000)
    while True:
        results = search_cli.search(search_text=search_text, top=page_size, skip=skip, **kwargs)
        batch = list(results)
        if not batch:
            break
        out.extend(batch)
        if len(batch) < page_size:
            break
        skip += len(batch)
        if skip > 100000:
            break
    return out

def _fetch_index_references(topic_name: str, sub_title: str) -> list[str]:
    """Return unique bibliography reference strings from Azure Search index for topic+subtopic."""
    select = ["references", "topic", "subtopic"]
    filt = f"topic eq '{_escape_odata(topic_name)}' and subtopic eq '{_escape_odata(sub_title)}'"
    docs = _search_all(search_text="*", filter=filt, select=select, top=1000)

    # Fallback: if topic mismatch, try subtopic-only and pick the most common topic in results
    if not docs:
        filt2 = f"subtopic eq '{_escape_odata(sub_title)}'"
        docs2 = _search_all(search_text=topic_name, filter=filt2, select=select, top=200)
        if docs2:
            freq: dict[str, int] = {}
            for d in docs2:
                t = (d.get("topic") or "").strip()
                if t:
                    freq[t] = freq.get(t, 0) + 1
            if freq:
                best = max(freq.items(), key=lambda x: x[1])[0]
                filt3 = f"topic eq '{_escape_odata(best)}' and subtopic eq '{_escape_odata(sub_title)}'"
                docs = _search_all(search_text="*", filter=filt3, select=select, top=1000)

    seen: set[str] = set()
    out: list[str] = []
    for d in docs:
        for r in (d.get("references") or []):
            rs = (r or "").strip()
            if not rs:
                continue
            # normalise obvious whitespace artefacts to improve dedupe
            rs_norm = re.sub(r"\s+", " ", rs).strip()
            if rs_norm in seen:
                continue
            seen.add(rs_norm)
            out.append(rs_norm)
    return out

# ─────────────────────────────── Function entry point
def main(msg: func.QueueMessage) -> None:
    logging.info("harvestReferences triggered")

    try:
        subtopic_id = json.loads(msg.get_body().decode())["subtopic_id"]
    except (KeyError, json.JSONDecodeError):
        logging.error("Bad queue payload – expected {'subtopic_id': …}")
        return

    # ① Look-up sub-topic title + topic
    with pyodbc.connect(DB_CONN) as cx:
        cur = cx.cursor()
        cur.execute("""
            SELECT s.title, s.topic_id, t.topic_name
            FROM cme.subtopics AS s
            JOIN cme.topics AS t ON t.topic_id = s.topic_id
            WHERE s.subtopic_id = ?
        """, subtopic_id)
        row = cur.fetchone()
        if not row:
            logging.error("Sub-topic %s not found", subtopic_id)
            return
        sub_title, topic_id, topic_name = row.title, row.topic_id, row.topic_name

    # ② Fetch bibliography references from Azure Search index (NO chunk sources)
    ref_strings = _fetch_index_references(topic_name, sub_title)

    if not ref_strings:
        with pyodbc.connect(DB_CONN) as cx:
            cur = cx.cursor()
            cur.execute("""
                UPDATE cme.subtopics
                SET status = 'refs_missing',
                    coverage_note = COALESCE(coverage_note, 'No bibliography references found in index for topic+subtopic')
                WHERE subtopic_id = ?
            """, subtopic_id)
            cx.commit()
        logging.warning("No references found in index for topic='%s' subtopic='%s' → refs_missing", topic_name, sub_title)
        return

    refs: list[dict] = []
    for rs in ref_strings:
        source_id = "ref:" + hashlib.sha1(rs.encode("utf-8")).hexdigest()
        refs.append({
            "source_id": source_id,
            "citation_link": _extract_url(rs),
            "excerpt": shorten(rs, 400),
        })

    logging.info("Found %d bibliography reference(s) for '%s'", len(refs), sub_title)

    # ③ Persist into SQL Server
    with pyodbc.connect(DB_CONN) as cx:
        cur = cx.cursor()
        for ref in refs:
            # upsert into cme.references
            cur.execute("""
                IF NOT EXISTS (SELECT 1 FROM cme.[references] WHERE source_id = ?)
                    INSERT INTO cme.[references] (reference_id, source_id, citation_link, excerpt)
                    VALUES (?, ?, ?, ?);
            """, ref["source_id"],
                str(uuid.uuid4()), ref["source_id"], ref["citation_link"], ref["excerpt"])

            # link sub-topic → reference
            cur.execute("""
                IF NOT EXISTS (
                    SELECT 1 FROM cme.subtopic_references sr
                    JOIN cme.[references] r ON sr.reference_id = r.reference_id
                    WHERE sr.subtopic_id = ? AND r.source_id = ?
                )
                    INSERT INTO cme.subtopic_references (subtopic_id, reference_id)
                    SELECT ?, reference_id
                    FROM cme.[references]
                    WHERE source_id = ?;
            """, subtopic_id, ref["source_id"],
                subtopic_id, ref["source_id"])

        # update state
        cur.execute("""
            UPDATE cme.subtopics
            SET status = 'concept_pending'
            WHERE subtopic_id = ?
        """, subtopic_id)
        cx.commit()

    # ④ queue next stage
    queue = QueueClient.from_connection_string(os.environ["AzureWebJobsStorage"], "concept-queue")
    queue.send_message(json.dumps({"subtopic_id": subtopic_id}))

    logging.info("Sub-topic %s → concept_pending", subtopic_id)
