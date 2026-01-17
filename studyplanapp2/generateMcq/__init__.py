# studyplan-pipeline/generateMcq/__init__.py ▶︎ v2 (2025-07-16)
from __future__ import annotations
import logging, os, json, uuid, re, unicodedata, textwrap, random
from typing import Any, Dict, List
import azure.functions as func
import pyodbc
from openai import AzureOpenAI
from azure.storage.queue import QueueClient

# ───────────────────── Azure/OpenAI config ─────────────────────
AZURE_OAI_ENDPOINT = "https://azure1405.openai.azure.com/"
AZURE_OAI_KEY = "CzrrWvXbsmYcNguU1SqBpE9HDhhbfYsbkq3UedythCYCV9zNQ4mLJQQJ99BEACHYHv6XJ3w3AAABACOGiIPm"
DEPLOYMENT = "gpt-4o"
AZURE_OAI_API_VERSION = "2024-02-15-preview"
oai = AzureOpenAI(
    api_key=AZURE_OAI_KEY,
    azure_endpoint=AZURE_OAI_ENDPOINT,
    api_version=AZURE_OAI_API_VERSION,
)
# === 1) KNOBS (add near other config constants) ===============================
MAX_MCQS_PER_SUBTOPIC = int(os.getenv("MAX_MCQS_PER_SUBTOPIC", "3"))
ENABLE_MCQ_PLANNING = os.getenv("ENABLE_MCQ_PLANNING", "1") == "1"

DB = "DRIVER={ODBC Driver 18 for SQL Server};SERVER=20.171.24.17;DATABASE=CME2;UID=new_root;PWD=japl@bJBYV77;Encrypt=no;TrustServerCertificate=yes;"

# ────────────────────────── helpers ────────────────────────────
def _norm(s: str) -> str:
    return unicodedata.normalize("NFKD", s).encode("ascii", "ignore").decode().lower()

def _word_overlap(a: str, b: str) -> bool:
    tok = lambda x: {w for w in re.findall(r"[A-Za-z0-9']+", _norm(x))}
    return bool(tok(a) & tok(b))

# NEW ─ fetch the rewritten concept paragraph (max 4 000 chars)
def _get_concept_text(cur, sub_id: str, limit: int = 4000) -> str:
    cur.execute("""
        SELECT TOP 1 content
        FROM cme.concepts
        WHERE subtopic_id = ?
        ORDER BY concept_id
    """, sub_id)
    row = cur.fetchone()
    return (row.content or "")[:limit] if row else ""
# === 2) MCQ PLANNER (add below helpers; above _target_mcq_count) ==============
def _plan_mcqs(topic: str, sub: str, concept: str, max_n: int = MAX_MCQS_PER_SUBTOPIC) -> dict:
    """
    Decide *which* MCQs matter and *why*, then recommend a count (1–3).
    Returns:
    {
      "recommendation": {"count": int, "reason": "str"},
      "blueprint": [{"focus":"...", "why":"...", "skill":"recall|apply|interpret", "priority": int}],
      "used_blueprint": [ ... top-N (<=max_n) ... ]
    }
    """
    schema = {
        "recommendation": {"count": 2, "reason": "text"},
        "blueprint": [
            {"focus": "what to test", "why": "why it matters",
             "skill": "recall|apply|interpret", "priority": 1}
        ]
    }

    prompt = textwrap.dedent(f"""
    You are planning single‑best‑answer MCQs for Indian paediatrics learners.
    Decide what to test for this sub‑topic and how many questions are warranted
    to learn it well (1–3). Base everything ONLY on the CONCEPT TEXT.

    Topic: {topic}
    Sub‑topic: {sub}

    CONCEPT TEXT
    ────────────
    {concept}
    ────────────

    Output strict JSON:
    {json.dumps(schema, ensure_ascii=False, indent=2)}

    Rules
    ──
    • 'count' must be 1, 2, or 3 and should reflect how many distinct, high‑value checks
      are needed (e.g., key criteria/algorithms, dose/duration thresholds, next‑step decisions).
    • 'blueprint' lists concrete MCQ intents in priority order:
        - focus: the specific competency to check (e.g., "criteria to admit", "next antibiotic
          on day‑5 persistent fever", "interpret blood culture timing/volume", "TCV schedule").
        - why: one short reason this is essential for practice/exams.
        - skill: recall | apply | interpret (Bloom-ish).
        - priority: 1 = highest.
    • If more than 3 good ideas exist, still list them all, but top‑prioritise.
    • No pregnancy/adult‑only contexts unless in sub‑topic title.
    """).strip()

    rsp = oai.chat.completions.create(
        model=DEPLOYMENT,
        messages=[{"role": "system", "content": "Return JSON only."},
                  {"role": "user", "content": prompt}],
        temperature=0.25,
        max_tokens=700,
        response_format={"type": "json_object"},
    )

    try:
        data = json.loads(rsp.choices[0].message.content)
    except Exception:
        return {"recommendation": {"count": 2, "reason": "fallback"},
                "blueprint": [], "used_blueprint": []}

    # Sanitise
    rec = data.get("recommendation") or {}
    cnt = rec.get("count", 2)
    try:
        cnt = int(cnt)
    except Exception:
        cnt = 2
    cnt = max(1, min(MAX_MCQS_PER_SUBTOPIC, cnt))

    bps = data.get("blueprint") or []
    # Order by explicit priority if present; else keep order
    def prio(x):
        try: return int(x.get("priority", 9999))
        except Exception: return 9999
    bps_sorted = sorted(
        [bp for bp in bps if isinstance(bp, dict) and bp.get("focus")],
        key=prio
    )
    used = bps_sorted[:cnt]

    return {
        "recommendation": {"count": cnt, "reason": rec.get("reason", "")},
        "blueprint": bps_sorted,
        "used_blueprint": used
    }

# === 3) FALLBACK COUNT (keep, but mark as fallback only) ======================
def _target_mcq_count(sub: str) -> int:
    """
    Fallback ONLY. If planning fails, fall back to keyword-based count.
    """
    k = (sub or "").lower()
    if any(w in k for w in ("complication", "diagnos", "treatment", "therap", "antimicrob", "mdr", "xdr")):
        return 3
    if any(w in k for w in ("immun", "vaccin", "follow", "counsel")):
        return 2
    if any(w in k for w in ("criteria", "definition", "overview")):
        return 1
    return 2


def _as_mcq_list(payload) -> list[dict]:
    """
    Accepts raw GPT payload (dict or list or None) and returns a list of MCQ dicts.
    Never returns None.
    """
    if isinstance(payload, dict):
        mcqs = payload.get("mcqs")
        return mcqs if isinstance(mcqs, list) else []
    if isinstance(payload, list):
        return payload
    return []

# ---------- GPT call -------------------------------------------------------
# === 4) GENERATOR: accept 'wanted' + 'blueprint' =============================
def _call_gpt_json(topic: str, sub: str, concept: str,
                   wanted: int | None = None,
                   blueprint: list[dict] | None = None) -> list[dict]:
    if wanted is None:
        wanted = _target_mcq_count(sub)  # fallback
    wanted = max(1, min(MAX_MCQS_PER_SUBTOPIC, int(wanted)))

    schema_txt = json.dumps({
        "mcqs": [{
            "stem": "string",
            "choices": ["string", "string", "string", "string"],
            "rationales": ["string", "string", "string", "string"],
            "correct_index": "int 0-3",
            "explanation": "string",
            "variant1": {"stem": "string", "correct_index": "int"},
            "variant2": {"stem": "string", "correct_index": "int"}
        }]
    }, indent=2)

    plan_snippet = ""
    if blueprint:
        plan_snippet = "\nBLUEPRINT (ordered; align MCQ 1→item 1, etc):\n" + \
                       json.dumps(blueprint, ensure_ascii=False)

    prompt = textwrap.dedent(f"""
    Create exactly {wanted} distinct single-best-answer MCQs for paediatrics.
    • Topic: {topic}
    • Sub-topic: {sub}
    Base EVERY item solely on CONCEPT TEXT below.{plan_snippet}

    CONCEPT TEXT
    ────────────
    {concept}
    ────────────

    Return ONLY valid JSON per this schema (no markdown):
    {schema_txt}

    Rules
    ──
    1) Each MCQ has exactly 4 answer choices (≤6 words each) and 4 matching rationales (≤35 words each).
    2) Include ≥1 higher‑order item (mini vignette or data interpretation) when the blueprint 'skill' is apply/interpret.
    3) Both "variant1" and "variant2" are paraphrases of the stem; reuse the same 4 choices; the correct_index may change.
    4) The explanation must quote the exact correct choice once and not mention distractors.
    5) Do NOT invent facts; all content must be answerable from CONCEPT TEXT.
    6) Paediatric guardrail: infants/children/adolescents only; no pregnancy/adult‑only contexts unless in sub‑topic title.
    """).strip()

    rsp = oai.chat.completions.create(
        model=DEPLOYMENT,
        messages=[{"role": "system", "content": "You are an exam-item generator."},
                  {"role": "user", "content": prompt}],
        temperature=0.45,
        max_tokens=900,
        response_format={"type": "json_object"},
    )

    try:
        data = json.loads(rsp.choices[0].message.content)
    except Exception:
        return []

    return _as_mcq_list(data)


def _ensure_variants(topic: str, sub: str, concept: str, block: dict) -> dict:
    """If variant1/variant2 missing or blank, ask GPT to supply them only."""
    missing = []
    for vn in ("variant1", "variant2"):
        if not block.get(vn) or not str(block[vn].get("stem", "")).strip():
            missing.append(vn)
    
    if not missing:
        return block
    
    choices = block["choices"]
    corr_text = choices[block["correct_index"]]
    
    ask = textwrap.dedent(f"""
        Create JSON with ONLY 'variant1' and 'variant2' keys.
        Each variant is a paraphrase of the original stem for the same sub-topic.
        Reuse these 4 choices verbatim: {json.dumps(choices, ensure_ascii=False)}.
        Set 'correct_index' in each variant based on the right choice text: {corr_text!r}.
        
        JSON shape strictly:
        {{
            "variant1": {{"stem":"...", "correct_index":int}},
            "variant2": {{"stem":"...", "correct_index":int}}
        }}
        
        Topic: {topic}
        Sub-topic: {sub}
        Concept text (for context only, do not add new facts):
        {concept[:1600]}
    """).strip()
    
    rsp = oai.chat.completions.create(
        model=DEPLOYMENT,
        messages=[{"role": "system", "content": "Return JSON only."},
                 {"role": "user", "content": ask}],
        temperature=0.3,
        max_tokens=250,
        response_format={"type": "json_object"},
    )
    
    add = json.loads(rsp.choices[0].message.content)
    for vn in ("variant1", "variant2"):
        if not block.get(vn) or not str(block[vn].get("stem", "")).strip():
            block[vn] = add.get(vn, block.get(vn, {"stem": block["stem"], "correct_index": block["correct_index"]}))
    
    return block

def _shuffle_choices(block: dict[str, Any]) -> None:
    """
    Shuffle choices *with* their rationales and remap correct_index
    for stem and variants using the correct answer TEXT.
    """
    import random
    # Defensive defaults
    choices = block.get("choices", [])[:]
    rationales = (block.get("rationales") or [""] * len(choices))[:]
    
    if len(rationales) != len(choices):
        # Pad/truncate so zipping is safe
        rationales = (rationales + [""] * len(choices))[:len(choices)]
    
    # Keep original list to look up correct texts
    original_choices = choices[:]
    stem_correct_text = original_choices[block["correct_index"]]
    
    # Shuffle pairs so rationales stay attached
    combined = list(zip(choices, rationales))
    random.shuffle(combined)
    block["choices"] = [c for c, _ in combined]
    block["rationales"] = [r for _, r in combined]
    
    # Stem: recompute by TEXT
    block["correct_index"] = block["choices"].index(stem_correct_text)
    
    # Variants: also recompute by TEXT (robust to any previous bad index)
    for vn in ("variant1", "variant2"):
        v = block.get(vn)
        if not v:
            continue
        try:
            corr_text_v = original_choices[v["correct_index"]]
        except Exception:
            # Fallback: assume same correct text as stem
            corr_text_v = stem_correct_text
        v["correct_index"] = block["choices"].index(corr_text_v)

# ---------- validation -----------------------------------------------------
def _validate_mcq(block: dict[str, Any], idx: int, concept: str, sub_title: str) -> str | None:
    """Return None if OK, else reason string."""
    if "stem" not in block or not block["stem"].strip():
        return f"MCQ {idx}: empty stem"
    
    if "choices" not in block or len(block["choices"]) != 4:
        return f"MCQ {idx}: choices ≠ 4"
    
    if any(not c.strip() for c in block["choices"]):
        return f"MCQ {idx}: blank choice"
    
    if "correct_index" not in block or block["correct_index"] not in range(4):
        return f"MCQ {idx}: bad correct_index"
    
    if "rationales" not in block or not isinstance(block["rationales"], list) or len(block["rationales"]) != 4:
        return f"MCQ {idx}: rationales ≠ 4"
    
    if any(not str(r).strip() for r in block["rationales"]):
        return f"MCQ {idx}: blank rationale"
    
    # (summary explanation optional; keep if provided)
    corr = block["choices"][block["correct_index"]]
    if not _word_overlap(corr, block["explanation"]):
        return f"MCQ {idx}: explanation missing correct choice"
    
    if not _word_overlap(corr, concept):
        return f"MCQ {idx}: correct answer not in concept"
    
    # simple adult/pregnancy drift guardrail
    if re.search(r"\b(pregnan\w*|lactat\w*|maternal|fetus)\b", block.get("stem", ""), re.I) and \
       "pregnan" not in (sub_title or "").lower():
        return f"MCQ {idx}: adult/pregnancy context not allowed"
    
    for vn in ("variant1", "variant2"):
        v = block.get(vn, {})
        if "stem" not in v or not v["stem"].strip():
            return f"MCQ {idx}: {vn} missing"
        if "correct_index" not in v or v["correct_index"] not in range(4):
            return f"MCQ {idx}: {vn} bad correct_index"
    
    rats = (block.get("rationales") or [""]*4)
    if re.search(r'^\s*Incorrect\b', rats[block["correct_index"]], flags=re.I):
        return f"MCQ {idx}: correct option rationale starts with 'Incorrect'"
    
    wrong_flags = [i for i, r in enumerate(rats) if i != block["correct_index"]
                   and re.search(r'^\s*Correct\b', r, flags=re.I)]
    if wrong_flags:
        return f"MCQ {idx}: distractor rationale(s) flagged 'Correct' at {wrong_flags}"
    
    return None

# ---------- DB persistence -------------------------------------------------
def _save_mcq(db_cur, sub_id: str, block: dict, order_no: int) -> None:
    """Insert one stem (+ its two variants) into DB."""
    qid = str(uuid.uuid4())
    
    # main question
    db_cur.execute(
        """
        INSERT INTO cme.questions
        (question_id, subtopic_id, stem, correct_choice, explanation)
        VALUES (?, ?, ?, ?, ?)
        """,
        qid,
        sub_id,
        block["stem"],
        block["choices"][block["correct_index"]],
        block["explanation"],
    )
    
    # answer choices + rationales
    for idx, txt in enumerate(block["choices"]):
        rationale = (block.get("rationales") or [""]*4)[idx]
        db_cur.execute(
            """
            INSERT INTO cme.choices
            (choice_id, question_id, choice_index, choice_text, rationale)
            VALUES (NEWID(), ?, ?, ?, ?)
            """,
            qid,
            idx,
            txt, rationale,
        )
    
    # variants 1 & 2
    for v_no, vn in enumerate(("variant1", "variant2"), start=1):
        v = block[vn]
        db_cur.execute(
            """
            INSERT INTO cme.variants
            (variant_id, question_id, variant_no, stem, correct_choice_index)
            VALUES (NEWID(), ?, ?, ?, ?)
            """,
            qid,
            v_no,
            v["stem"],
            v["correct_index"],
        )
    
    # reference linkage (reuse sub-topic refs)
    db_cur.execute(
        """
        INSERT INTO cme.question_references (question_id, reference_id)
        SELECT ?, reference_id
        FROM cme.subtopic_references
        WHERE subtopic_id = ?
        """,
        qid,
        sub_id,
    )

def _mark_failed(sub_id: str, reason: str) -> None:
    """
    Mark a subtopic as failed and log the reason.

    Args:
        sub_id: The subtopic ID that failed
        reason: The reason for failure
    """
    try:
        with pyodbc.connect(DB) as con:
            cur = con.cursor()

            # Update subtopic status to failed
            cur.execute(
                "UPDATE cme.subtopics SET status='failed' WHERE subtopic_id = ?",
                sub_id,
            )

            # Log the failure reason
            cur.execute(
                "INSERT INTO cme.fail_log (stage, entity_id, reason) VALUES ('mcq', ?, ?)",
                sub_id,
                reason,
            )

            con.commit()
            logging.error("Subtopic %s marked as failed: %s", sub_id, reason)

    except Exception as e:
        logging.error("Failed to mark subtopic %s as failed: %s", sub_id, str(e))

# ─────────────────────── main Azure Function ───────────────────────────────
# === 5) MAIN: call planner, persist plan (optional), then generate ===========
def main(msg: func.QueueMessage) -> None:
    logging.info("generateMcq triggered")

    try:
        sub_id = json.loads(msg.get_body().decode())["subtopic_id"]
    except Exception:
        logging.error("Bad queue message")
        return

    # -- 1. Pull titles -----------------------------------------------------
    with pyodbc.connect(DB) as con:
        cur = con.cursor()
        cur.execute(
            """
            SELECT t.topic_id, t.topic_name, s.title
            FROM cme.subtopics s
            JOIN cme.topics t ON t.topic_id = s.topic_id
            WHERE s.subtopic_id = ?
            """,
            sub_id,
        )
        row = cur.fetchone()
        if not row:
            logging.warning("Sub-topic %s not found (transient?) – requeue once", sub_id)
            try:
                q = QueueClient.from_connection_string(os.environ["AzureWebJobsStorage"], "mcq-queue")
                q.send_message(json.dumps({"subtopic_id": sub_id}))
            except Exception:
                pass
            return

        topic_id, topic, sub_title = row

    # -- pull the concept paragraph once -----------------------------------
    with pyodbc.connect(DB) as con:
        cur = con.cursor()
        concept_text = _get_concept_text(cur, sub_id)

    if not concept_text:
        logging.error("Concept missing for %s → refs_missing", sub_id)
        _mark_failed(sub_id, "concept missing")
        return

    # -- 2. PLAN (AI) -------------------------------------------------------
    plan = None
    blueprint = None
    wanted = None
    if ENABLE_MCQ_PLANNING:
        plan = _plan_mcqs(topic, sub_title, concept_text, MAX_MCQS_PER_SUBTOPIC)
        blueprint = plan.get("used_blueprint") or []
        wanted = len(blueprint) if blueprint else None

        # OPTIONAL: persist the plan for audit in existing qa_reviews table
        try:
            with pyodbc.connect(DB) as con:
                cur = con.cursor()
                cur.execute("""
                    INSERT INTO cme.qa_reviews (qa_id, entity_type, entity_id, status, issues, suggested_fix)
                    VALUES (?, 'mcq_plan', ?, ?, ?, ?)
                """,
                str(uuid.uuid4()), sub_id, 'ok',
                json.dumps([plan.get("recommendation", {}).get("reason", "")], ensure_ascii=False),
                json.dumps(plan, ensure_ascii=False))
                con.commit()
        except Exception as e:
            logging.warning("Could not persist MCQ plan for %s: %s", sub_id, e)

    # -- 3. GPT loop (max 4 attempts) --------------------------------------
    reason = ""
    blocks: List[Dict[str, Any]] = []

    for attempt in range(4):
        if attempt == 0 or reason:
            blocks = _call_gpt_json(topic, sub_title, concept_text, wanted=wanted, blueprint=blueprint)

        blocks = blocks or []

        if not blocks:
            logging.warning("MCQ generation returned 0 items for subtopic %s", sub_id)
            try:
                q = QueueClient.from_connection_string(os.environ["AzureWebJobsStorage"], "mcq-queue")
                q.send_message(json.dumps({"subtopic_id": sub_id}))
                logging.info("Requeued subtopic %s for MCQ generation (once).", sub_id)
            except Exception as e:
                logging.warning("Requeue failed for %s: %s", sub_id, e)

            try:
                with pyodbc.connect(DB) as con:
                    cur = con.cursor()
                    cur.execute("UPDATE cme.subtopics SET status='failed' WHERE subtopic_id = ?", sub_id)
                    cur.execute(
                        "INSERT INTO cme.fail_log (stage, entity_id, reason) VALUES ('mcq', ?, ?)",
                        sub_id, "no_mcqs_returned"
                    )
                    con.commit()
            except Exception as e:
                logging.error("Failed to mark subtopic %s failed: %s", sub_id, e)
            return

        # structural validation of all blocks
        fails = [
            r for i, b in enumerate(blocks or [], 1)
            if (r := _validate_mcq(b, i, concept_text, sub_title))
        ]

        if not fails:
            break

        reason = "; ".join(fails)
        logging.warning("MCQ validation failed (%s) – attempt %d/4", reason, attempt + 1)

    if reason:
        logging.error("MCQ generation FAILED → status=failed")
        with pyodbc.connect(DB) as con:
            cur = con.cursor()
            cur.execute("UPDATE cme.subtopics SET status='failed' WHERE subtopic_id = ?", sub_id)
            cur.execute(
                "INSERT INTO cme.fail_log (stage, entity_id, reason) VALUES ('mcq', ?, ?)",
                sub_id, reason or "unknown",
            )
            con.commit()
        return

    # cap to MAX as a last guard
    if len(blocks) > MAX_MCQS_PER_SUBTOPIC:
        blocks = blocks[:MAX_MCQS_PER_SUBTOPIC]

    # -- 4A. Final touch – randomise answer‑order ---------------------------
    for b in blocks or []:
        b = _ensure_variants(topic, sub_title, concept_text, b)
        _shuffle_choices(b)

    # -- 4B. Persist --------------------------------------------------------
    with pyodbc.connect(DB) as con:
        cur = con.cursor()
        for order_no, block in enumerate(blocks or [], 1):
            _save_mcq(cur, sub_id, block, order_no)

        cur.execute("UPDATE cme.subtopics SET status='mcq_ready' WHERE subtopic_id = ?", sub_id)
        con.commit()

    # -- 5. trigger plan assembler -----------------------------------------
    queue = QueueClient.from_connection_string(os.environ["AzureWebJobsStorage"], "plan-queue")
    queue.send_message(json.dumps({"topic_id": topic_id}))

    logging.info("✓ %d MCQ(s) saved for sub-topic %s → mcq_ready", len(blocks), sub_id)
    