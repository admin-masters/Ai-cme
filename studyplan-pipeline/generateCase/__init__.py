# studyplan-pipeline/generateCase/__init__.py - ENHANCED WITH VIGNETTE EXTRACTION
from __future__ import annotations
import json, logging, os, re, uuid
import azure.functions as func
import pyodbc
from openai import AzureOpenAI
from azure.storage.queue import QueueClient
from azure.core.credentials import AzureKeyCredential
from azure.search.documents import SearchClient

DB = "DRIVER={ODBC Driver 18 for SQL Server};SERVER=20.171.24.17;DATABASE=CME2;UID=new_root;PWD=japl@bJBYV77;Encrypt=no;TrustServerCertificate=yes;"

AZURE_OAI_ENDPOINT = "https://azure-140709.openai.azure.com/"
AZURE_OAI_KEY = os.getenv("AZURE_OPENAI_KEY")
DEPLOYMENT = "gpt-4o"
AZURE_OAI_API_VERSION = "2024-02-15-preview"

# Azure Search config for vignette extraction
SEARCH_ENDPOINT = "https://basic-rag-sandbox.search.windows.net"
SEARCH_ADMIN_KEY = "tuqRZ8A374Aw3wXKSTzOY6SEu6Ra8rOyhPgFEtcLpSAzSeBOByQL"
INDEX_NAME = "pubert-demo-new"
SEARCH_API_VERSION = "2025-05-01-preview"

# ─────────────────── Azure-OpenAI client ────────────────────
oai = AzureOpenAI(
    api_key=AZURE_OAI_KEY,
    azure_endpoint=AZURE_OAI_ENDPOINT,
    api_version=AZURE_OAI_API_VERSION,
)

search_cli = SearchClient(
    endpoint=SEARCH_ENDPOINT,
    index_name=INDEX_NAME,
    credential=AzureKeyCredential(SEARCH_ADMIN_KEY),
    api_version=SEARCH_API_VERSION,
)

# ───────────────────────── Configuration ─────────────────────────
MAX_CASES_PER_TOPIC = int(os.getenv("MAX_CASES_PER_TOPIC", "12"))  # Reduced from ~40
MIN_VIGNETTE_WORDS = int(os.getenv("MIN_VIGNETTE_WORDS", "90"))
MAX_VIGNETTE_WORDS = int(os.getenv("MAX_VIGNETTE_WORDS", "250"))
CASE_AMENABLE_MIN_CONF = int(os.getenv("CASE_AMENABLE_MIN_CONF", "60"))

# Pattern to detect case-based content
_CASE_PATTERN = re.compile(
    r'\b(scenario|vignette|case\s+study|presents?\s+with|examination\s+shows?|management)\b',
    re.I
)

def _wc(s: str) -> int:
    return len(re.findall(r"\b\w+\b", s or ""))

def _escape_odata(s: str) -> str:
    return (s or "").replace("'", "''")

# ───────────────────────── Search helpers ─────────────────────────
def _search_all(*, search_text: str, **kwargs) -> list[dict]:
    """Paginated search to get all results"""
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
        if skip > 10000:
            break
    return out

def _sequence_key(seq: str) -> tuple:
    """Parse sequence like '1a.2' into sortable tuple"""
    m = re.match(r"^(\d+)([a-zA-Z]?)(?:\.(\d+))?$", (seq or "").strip())
    if not m:
        return (10**9, 0, 0, seq or "")
    major = int(m.group(1))
    letter = ord(m.group(2).lower()) - ord('a') + 1 if m.group(2) else 0
    minor = int(m.group(3) or 0)
    return (major, letter, minor, seq)

# ───────────────────────── Vignette Extraction from Index ─────────────────────────
def _extract_vignettes_from_index(topic_name: str) -> list[dict]:
    """
    Extract rich vignettes from case-based sub-subtopics in the index.
    
    Returns list of:
    {
        "vignette_text": "...",
        "source_subtopic": "...",
        "source_sub_subtopic": "...",
        "word_count": 150,
        "sequence": "4a.1"
    }
    """
    # Search for case-based content (identified by sub-subtopic naming)
    select = ["content", "topic", "subtopic", "sub_subtopic", "sequence", "heading_path"]
    
    # Target case-based sections
    case_filter = (
        f"topic eq '{_escape_odata(topic_name)}' and "
        f"(contains(sub_subtopic, 'CASE') or contains(sub_subtopic, 'SCENARIO') or "
        f"contains(sub_subtopic, 'VIGNETTE'))"
    )
    
    docs = _search_all(search_text='*', filter=case_filter, select=select, top=1000)
    
    if not docs:
        logging.info("No explicit case sections found, searching for scenario content...")
        # Fallback: search all content for scenario keywords
        docs = _search_all(
            search_text=f'{topic_name} scenario vignette "presents with"',
            select=select,
            top=100
        )
    
    # Extract and clean vignettes
    vignettes = []
    seen_content = set()  # Deduplicate
    
    for d in docs:
        content = (d.get("content") or "").strip()
        if not content or len(content) < 200:  # Too short
            continue
        
        # Check for case-like content
        if not _CASE_PATTERN.search(content):
            continue
        
        # Normalize for dedup
        content_key = re.sub(r'\s+', ' ', content.lower())[:500]
        if content_key in seen_content:
            continue
        seen_content.add(content_key)
        
        wc = _wc(content)
        if wc < MIN_VIGNETTE_WORDS or wc > MAX_VIGNETTE_WORDS * 2:
            # Too short or way too long (will need splitting)
            continue
        
        vignettes.append({
            "vignette_text": content,
            "source_subtopic": d.get("subtopic", ""),
            "source_sub_subtopic": d.get("sub_subtopic", ""),
            "word_count": wc,
            "sequence": d.get("sequence", ""),
            "heading_path": d.get("heading_path", "")
        })
    
    # Sort by sequence
    vignettes.sort(key=lambda v: _sequence_key(v.get("sequence", "")))
    
    logging.info("Extracted %d raw vignettes from index for topic '%s'", 
                 len(vignettes), topic_name)
    
    return vignettes

def _refine_vignette_gpt(raw_vignette: str, topic: str, subtopic: str) -> dict:
    """
    Use GPT to refine a raw vignette into a clean, educational case.
    
    Returns:
    {
        "title": "...",
        "vignette": "...",  # 90-220 words
        "learning_objective": "...",
        "clinical_question": "..."  # Optional
    }
    """
    schema = {
        "title": "string (concise case title)",
        "vignette": "string (90-220 words)",
        "learning_objective": "string (<=100 chars)",
        "clinical_question": "string (optional teaching question)"
    }
    
    prompt = f"""
You are refining a pediatric clinical vignette for medical education.

TOPIC: {topic}
SUBTOPIC: {subtopic}

RAW SOURCE TEXT:
{raw_vignette[:1500]}

TASK:
Create a polished clinical vignette following these rules:

STRUCTURE (90-220 words):
1. Age, gender, setting (e.g., "A 6-year-old boy from an urban slum")
2. Time course and chief complaint
3. Key symptoms in chronological order
4. Focused physical examination findings
5. 0-2 objective data points (vitals, key lab values)

CONSTRAINTS:
- Do NOT include the diagnosis in the vignette
- Do NOT include management steps
- Keep it realistic for Indian/LMIC context
- Include specific clinical details (e.g., "fever for 8 days", "hepatomegaly 3 cm below costal margin")
- Must be answerable using information from the source

CLINICAL QUESTION (optional):
If the source suggests a decision point, add a focused question (e.g., 
"What is the most appropriate next step in management?" or
"Which investigation has the highest diagnostic yield?")

Return ONLY valid JSON matching this schema:
{json.dumps(schema, indent=2)}
"""
    
    try:
        rsp = oai.chat.completions.create(
            model=DEPLOYMENT,
            messages=[
                {"role": "system", "content": "You are a pediatrics case writer. Return JSON only."},
                {"role": "user", "content": prompt}
            ],
            temperature=0.4,
            max_tokens=600,
            response_format={"type": "json_object"},
        )
        
        data = json.loads(rsp.choices[0].message.content)
        
        # Validate
        vignette = (data.get("vignette") or "").strip()
        wc = _wc(vignette)
        
        if wc < MIN_VIGNETTE_WORDS or wc > MAX_VIGNETTE_WORDS:
            logging.warning("Refined vignette word count %d out of range, skipping", wc)
            return None
        
        return {
            "title": (data.get("title") or "").strip()[:255] or subtopic,
            "vignette": vignette,
            "learning_objective": (data.get("learning_objective") or "").strip()[:255],
            "clinical_question": (data.get("clinical_question") or "").strip()[:500]
        }
        
    except Exception as e:
        logging.error("Vignette refinement failed: %s", e)
        return None

# ───────────────────────── Selective Case Assignment ─────────────────────────
def _rank_subtopics_for_cases(topic_id: str, topic_name: str, 
                               max_cases: int = MAX_CASES_PER_TOPIC) -> list[str]:
    """
    Use GPT to rank subtopics by case-amenability and return top N subtopic_ids.
    
    This replaces the budget-based approach with intelligent selection.
    """
    with pyodbc.connect(DB) as con:
        cur = con.cursor()
        
        # Get all subtopics with their concepts
        cur.execute("""
            SELECT s.subtopic_id, s.title, s.category, c.content
            FROM cme.subtopics s
            LEFT JOIN cme.concepts c ON c.subtopic_id = s.subtopic_id
            WHERE s.topic_id = ?
            ORDER BY s.sequence_no
        """, topic_id)
        
        subtopics = []
        for row in cur.fetchall():
            concept_snippet = (row.content or "")[:400] if row.content else ""
            subtopics.append({
                "subtopic_id": row.subtopic_id,
                "title": row.title,
                "category": row.category,
                "snippet": concept_snippet
            })
    
    if not subtopics:
        return []
    
    # Ask GPT to rank
    schema = {
        "ranked_subtopics": [
            {
                "subtopic_id": "uuid",
                "rank": 1,
                "reason": "why this deserves a case"
            }
        ]
    }
    
    prompt = f"""
You are selecting which pediatric subtopics deserve rich clinical case vignettes.

TOPIC: {topic_name}
TARGET: Select TOP {max_cases} subtopics for case-based learning

CRITERIA (in priority order):
1. **Decision-intensive**: Triage, admission criteria, treatment-failure algorithms
2. **Data interpretation**: Lab values, imaging findings, clinical scoring
3. **Time-sensitive**: Complications recognition, emergency management
4. **Common pitfalls**: Misdiagnosis, delayed recognition
5. **Practical application**: Real-world scenarios requiring clinical judgment

DOWN-RANK:
- Pure definitions or classifications
- Simple recall facts (e.g., "what is typhoid?")
- Background epidemiology only
- Generic prevention messages
- Already case-bearing subtopics (if concept mentions "scenario" or "vignette")

SUBTOPICS ({len(subtopics)} total):
{json.dumps([{"id": s["subtopic_id"], "title": s["title"], "snippet": s["snippet"][:200]} 
             for s in subtopics], indent=2)}

Return JSON with TOP {max_cases} ranked by case-learning value:
{json.dumps(schema, indent=2)}
"""
    
    try:
        rsp = oai.chat.completions.create(
            model=DEPLOYMENT,
            messages=[
                {"role": "system", "content": "You are a medical education curriculum designer."},
                {"role": "user", "content": prompt}
            ],
            temperature=0.3,
            max_tokens=1200,
            response_format={"type": "json_object"},
        )
        
        data = json.loads(rsp.choices[0].message.content)
        ranked = data.get("ranked_subtopics", [])
        
        # Extract and validate IDs
        valid_ids = {s["subtopic_id"] for s in subtopics}
        selected = [
            r["subtopic_id"] 
            for r in ranked 
            if isinstance(r, dict) and r.get("subtopic_id") in valid_ids
        ][:max_cases]
        
        logging.info("Selected %d/%d subtopics for case generation", 
                     len(selected), len(subtopics))
        
        return selected
        
    except Exception as e:
        logging.error("Subtopic ranking failed: %s", e)
        # Fallback: select first N
        return [s["subtopic_id"] for s in subtopics[:max_cases]]

# ───────────────────────── Vignette-Subtopic Matching ─────────────────────────
def _match_vignettes_to_subtopics(topic_name: str, vignettes: list[dict], 
                                   subtopic_ids: list[str]) -> dict[str, dict]:
    """
    Match extracted vignettes to selected subtopics using semantic similarity.
    
    Returns: {subtopic_id: vignette_dict}
    """
    if not vignettes or not subtopic_ids:
        return {}
    
    # Get subtopic details
    with pyodbc.connect(DB) as con:
        cur = con.cursor()
        placeholders = ','.join('?' * len(subtopic_ids))
        cur.execute(f"""
            SELECT subtopic_id, title, category
            FROM cme.subtopics
            WHERE subtopic_id IN ({placeholders})
        """, *subtopic_ids)
        
        subtopics = [
            {"subtopic_id": r.subtopic_id, "title": r.title, "category": r.category}
            for r in cur.fetchall()
        ]
    
    # Use GPT to match
    schema = {
        "matches": [
            {
                "subtopic_id": "uuid",
                "vignette_index": 0,
                "confidence": 85,
                "reason": "why this is a good match"
            }
        ]
    }
    
    vignette_summaries = [
        {
            "index": i,
            "preview": v["vignette_text"][:300] + "...",
            "source": v.get("source_sub_subtopic") or v.get("source_subtopic")
        }
        for i, v in enumerate(vignettes)
    ]
    
    prompt = f"""
Match clinical vignettes to pediatric subtopics for {topic_name}.

SUBTOPICS NEEDING CASES:
{json.dumps([{"id": s["subtopic_id"], "title": s["title"]} for s in subtopics], indent=2)}

AVAILABLE VIGNETTES:
{json.dumps(vignette_summaries, indent=2)}

RULES:
1. Match based on clinical content alignment
2. Each subtopic gets AT MOST ONE vignette
3. Prioritize exact/close matches
4. confidence: 0-100 (only include if ≥60)
5. If no good match exists for a subtopic, omit it

Return JSON:
{json.dumps(schema, indent=2)}
"""
    
    try:
        rsp = oai.chat.completions.create(
            model=DEPLOYMENT,
            messages=[
                {"role": "system", "content": "You are matching clinical vignettes to learning objectives."},
                {"role": "user", "content": prompt}
            ],
            temperature=0.2,
            max_tokens=1000,
            response_format={"type": "json_object"},
        )
        
        data = json.loads(rsp.choices[0].message.content)
        matches_raw = data.get("matches", [])
        
        # Build mapping
        mapping = {}
        valid_ids = {s["subtopic_id"] for s in subtopics}
        
        for m in matches_raw:
            if not isinstance(m, dict):
                continue
            
            sid = m.get("subtopic_id")
            idx = m.get("vignette_index")
            conf = m.get("confidence", 0)
            
            if sid not in valid_ids:
                continue
            if not isinstance(idx, int) or idx < 0 or idx >= len(vignettes):
                continue
            if conf < 60:
                continue
            
            # Take first (highest confidence) match per subtopic
            if sid not in mapping:
                mapping[sid] = vignettes[idx]
        
        logging.info("Matched %d vignettes to subtopics", len(mapping))
        return mapping
        
    except Exception as e:
        logging.error("Vignette matching failed: %s", e)
        return {}

# ───────────────────────── Main Entry Point ─────────────────────────
def main(msg: func.QueueMessage) -> None:
    logging.info("generateCase triggered (ENHANCED WITH VIGNETTE EXTRACTION)")
    
    try:
        # This can be triggered per-topic instead of per-subtopic
        payload = json.loads(msg.get_body().decode())
        
        # Support both topic_id and subtopic_id triggers
        if "topic_id" in payload:
            topic_id = payload["topic_id"]
            mode = "topic"
        elif "subtopic_id" in payload:
            # Legacy: single subtopic mode (will still work but not optimal)
            subtopic_id = payload["subtopic_id"]
            mode = "subtopic"
        else:
            logging.error("Payload must have topic_id or subtopic_id")
            return
            
    except Exception:
        logging.error("Bad queue payload")
        return
    
    # ═══════════════════════════════════════════════════════════════
    # MODE 1: TOPIC-LEVEL (PREFERRED - generates rich cases strategically)
    # ═══════════════════════════════════════════════════════════════
    if mode == "topic":
        with pyodbc.connect(DB) as con:
            cur = con.cursor()
            cur.execute("SELECT topic_name FROM cme.topics WHERE topic_id=?", topic_id)
            row = cur.fetchone()
            if not row:
                logging.error("Topic %s not found", topic_id)
                return
            topic_name = row.topic_name
        
        logging.info("=== TOPIC-LEVEL CASE GENERATION: %s ===", topic_name)
        
        # Step 1: Extract vignettes from index
        vignettes = _extract_vignettes_from_index(topic_name)
        
        if not vignettes:
            logging.warning("No vignettes found in index for topic %s", topic_name)
            # Fall back to GPT generation for top subtopics
            vignettes = []
        
        # Step 2: Select top N subtopics that deserve cases
        selected_subtopics = _rank_subtopics_for_cases(
            topic_id, 
            topic_name, 
            max_cases=MAX_CASES_PER_TOPIC
        )
        
        if not selected_subtopics:
            logging.warning("No subtopics selected for cases in topic %s", topic_id)
            return
        
        # Step 3: Match vignettes to subtopics
        if vignettes:
            matches = _match_vignettes_to_subtopics(
                topic_name, 
                vignettes, 
                selected_subtopics
            )
        else:
            matches = {}
        
        # Step 4: Generate/refine cases
        generated_count = 0
        q = None
        try:
            q = QueueClient.from_connection_string(
                os.environ["AzureWebJobsStorage"], 
                "case-mcq-queue"
            )
        except Exception:
            pass
        
        for subtopic_id in selected_subtopics:
            # Get subtopic info
            with pyodbc.connect(DB) as con:
                cur = con.cursor()
                cur.execute("""
                    SELECT s.title, s.category, c.content
                    FROM cme.subtopics s
                    LEFT JOIN cme.concepts c ON c.subtopic_id = s.subtopic_id
                    WHERE s.subtopic_id = ?
                """, subtopic_id)
                row = cur.fetchone()
                if not row:
                    continue
                
                sub_title = row.title
                category = row.category
                concept = (row.content or "")[:1800]
            
            # Check if already has case
            with pyodbc.connect(DB) as con:
                cur = con.cursor()
                cur.execute("""
                    SELECT COUNT(*) FROM cme.cases WHERE subtopic_id=?
                """, subtopic_id)
                existing = cur.fetchone()[0] or 0
            
            if existing > 0:
                logging.info("Subtopic %s already has %d case(s), skipping", 
                             sub_title[:40], existing)
                continue
            
            # Generate case
            case_data = None
            
            if subtopic_id in matches:
                # Refine matched vignette
                raw_vignette = matches[subtopic_id]
                case_data = _refine_vignette_gpt(
                    raw_vignette["vignette_text"],
                    topic_name,
                    sub_title
                )
                if case_data:
                    logging.info("✓ Refined vignette for '%s'", sub_title[:40])
            
            if not case_data:
                # Generate new case from concept
                case_data = _generate_case_from_concept_gpt(
                    topic_name,
                    sub_title,
                    concept
                )
                if case_data:
                    logging.info("✓ Generated new case for '%s'", sub_title[:40])
            
            if not case_data:
                logging.warning("✗ Failed to create case for '%s'", sub_title[:40])
                continue
            
            # Validate word count
            wc = _wc(case_data["vignette"])
            if wc < MIN_VIGNETTE_WORDS or wc > MAX_VIGNETTE_WORDS:
                logging.warning("Case word count %d out of range, skipping", wc)
                continue
            
            # Insert into database
            case_id = str(uuid.uuid4())
            
            with pyodbc.connect(DB) as con:
                cur = con.cursor()
                cur.execute("""
                    INSERT INTO cme.cases
                    (case_id, subtopic_id, title, vignette, word_count, learning_objective)
                    VALUES (?, ?, ?, ?, ?, ?)
                """,
                case_id, subtopic_id, 
                case_data["title"], 
                case_data["vignette"], 
                wc,
                case_data.get("learning_objective", ""))
                
                cur.execute("""
                    UPDATE cme.subtopics
                    SET case_status='pending', case_amenable=1
                    WHERE subtopic_id=?
                """, subtopic_id)
                
                con.commit()
            
            # Queue for MCQ generation
            if q:
                try:
                    q.send_message(json.dumps({"case_id": case_id}))
                except Exception as e:
                    logging.error("Queue push failed: %s", e)
            
            generated_count += 1
        
        logging.info("=== TOPIC CASE GENERATION COMPLETE ===")
        logging.info("Generated %d/%d rich cases for topic %s", 
                     generated_count, MAX_CASES_PER_TOPIC, topic_name)
        return
    
    # ═══════════════════════════════════════════════════════════════
    # MODE 2: SUBTOPIC-LEVEL (LEGACY - single case generation)
    # ═══════════════════════════════════════════════════════════════
    else:  # mode == "subtopic"
        # Original single-subtopic logic (kept for compatibility)
        with pyodbc.connect(DB) as con:
            cur = con.cursor()
            cur.execute("SELECT case_status FROM cme.subtopics WHERE subtopic_id=?", subtopic_id)
            cstat = (cur.fetchone()[0] or "").lower()
            if cstat != "pending":
                logging.info("Subtopic %s is no longer pending (status=%s)", subtopic_id, cstat)
                return
            
            cur.execute("""
                SELECT t.topic_name, s.title, s.case_amenable
                FROM cme.subtopics s
                JOIN cme.topics t ON t.topic_id = s.topic_id
                WHERE s.subtopic_id = ?
            """, subtopic_id)
            
            row = cur.fetchone()
            if not row:
                logging.error("Subtopic not found")
                return
            
            topic, sub, caseable = row
            
            if not caseable:
                logging.info("Subtopic marked non-caseable; skipping")
                return
            
            # Get concept
            cur.execute("""
                SELECT TOP 1 content
                FROM cme.concepts
                WHERE subtopic_id=?
                ORDER BY concept_id
            """, subtopic_id)
            
            crow = cur.fetchone()
            concept = (crow.content if crow else "")[:1800]
        
        # Generate case
        case_data = _generate_case_from_concept_gpt(topic, sub, concept)
        
        if not case_data:
            logging.error("Failed to generate case for subtopic %s", subtopic_id)
            return
        
        wc = _wc(case_data["vignette"])
        case_id = str(uuid.uuid4())
        
        with pyodbc.connect(DB) as con:
            cur = con.cursor()
            cur.execute("""
                INSERT INTO cme.cases
                (case_id, subtopic_id, title, vignette, word_count, learning_objective)
                VALUES (?, ?, ?, ?, ?, ?)
            """,
            case_id, subtopic_id, 
            case_data["title"], 
            case_data["vignette"], 
            wc,
            case_data.get("learning_objective", ""))
            
            cur.execute("""
                UPDATE cme.subtopics
                SET case_status='pending'
                WHERE subtopic_id=?
            """, subtopic_id)
            
            con.commit()
        
        try:
            q = QueueClient.from_connection_string(
                os.environ["AzureWebJobsStorage"], 
                "case-mcq-queue"
            )
            q.send_message(json.dumps({"case_id": case_id}))
        except Exception as e:
            logging.error("Queue push failed: %s", e)

def _generate_case_from_concept_gpt(topic: str, subtopic: str, concept: str) -> dict:
    """Fallback: generate case from concept when no vignette available"""
    
    schema = {
        "title": "string",
        "vignette": "string (90-220 words)",
        "learning_objective": "string"
    }
    
    prompt = f"""
Create ONE realistic clinical vignette for paediatrics.

Topic: {topic}
Sub-topic: {subtopic}

Constraints:
• 100–200 words. Realistic India/LMIC context if relevant.
• Include age/setting, time course, key symptoms, focused exam, and 0–2 objective data.
• Do NOT include the diagnosis or management in the vignette text.
• Must be answerable from the sub-topic's concept below (no new facts).
• Prefer situations that test triage/admission thresholds, persistent fever algorithms, 
  or acute complications when relevant to the subtopic.

Concept (context only):
{concept}

Return JSON only:
{json.dumps(schema, indent=2)}
"""
    
    try:
        rsp = oai.chat.completions.create(
            model=DEPLOYMENT,
            messages=[
                {"role": "system", "content": "You are a paediatrics case writer."},
                {"role": "user", "content": prompt}
            ],
            temperature=0.5,
            max_tokens=500,
            response_format={"type": "json_object"},
        )
        
        data = json.loads(rsp.choices[0].message.content)
        return {
            "title": (data.get("title") or "").strip()[:255] or subtopic,
            "vignette": (data.get("vignette") or "").strip(),
            "learning_objective": (data.get("learning_objective") or "").strip()[:255]
        }
    except Exception as e:
        logging.error("Case generation from concept failed: %s", e)
        return None