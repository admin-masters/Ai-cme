from __future__ import annotations
import os, json, logging
import azure.functions as func
import pyodbc
from azure.storage.queue import QueueClient

# same DB and storage settings as the rest of the app
DB_CONN = os.getenv("DB") or os.getenv("DB_CONN") or \
    "DRIVER={ODBC Driver 18 for SQL Server};SERVER=20.171.24.17;DATABASE=CME2;UID=new_root;PWD=japl@bJBYV77;Encrypt=no;TrustServerCertificate=yes;"

# mirror original gate: enqueue only 'ok' content when set
BLOCK_ON_LOW_COVERAGE = os.getenv("BLOCK_ON_LOW_COVERAGE", "1") == "1"
QUEUE_NAME = "subtopic-queue"

def main(req: func.HttpRequest) -> func.HttpResponse:
    """
    POST/GET /api/enqueueSubtopics?topic_id=<uuid>
    Body alt: { "topic_id": "..." }

    Fetch subtopics for topic_id in refs_pending and push them to subtopic-queue.
    If BLOCK_ON_LOW_COVERAGE=1 (default), only enqueue subtopics with content_status='ok'.
    """
    logging.info("enqueueSubtopics called")

    topic_id = req.params.get("topic_id")
    if not topic_id:
        try:
            body = req.get_json()
            topic_id = body.get("topic_id")
        except Exception:
            pass

    if not topic_id:
        return func.HttpResponse("Supply 'topic_id' as query-string or JSON.", status_code=400)

    try:
        to_enqueue = []
        skipped = []
        total = 0

        with pyodbc.connect(DB_CONN) as con:
            cur = con.cursor()
            cur.execute("""
                SELECT subtopic_id, content_status
                FROM cme.subtopics
                WHERE topic_id = ?
                ORDER BY sequence_no
            """, topic_id)
            rows = cur.fetchall()
            total = len(rows)
            for sid, content_status in rows:
                status = (content_status or "").strip().lower()
                if (not BLOCK_ON_LOW_COVERAGE) or status == "ok":
                    to_enqueue.append(sid)
                else:
                    skipped.append(sid)

        qc = QueueClient.from_connection_string(os.environ["AzureWebJobsStorage"], QUEUE_NAME)
        for sid in to_enqueue:
            qc.send_message(json.dumps({"subtopic_id": sid}))

        payload = {
            "topic_id": topic_id,
            "refs_pending": total,
            "queued": len(to_enqueue),
            "skipped_low_coverage": len(skipped),
            "queue": QUEUE_NAME
        }
        return func.HttpResponse(json.dumps(payload), mimetype="application/json", status_code=200)

    except Exception as exc:
        logging.exception("Failed to enqueue subtopics")
        return func.HttpResponse(f"Error: {exc}", status_code=500)
