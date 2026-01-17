"""
Stage 1: Hierarchical PDF Chunking with Clean Reference Extraction
Fixed version with proper URL handling and hierarchy preservation
"""
from __future__ import annotations
import argparse, json, logging, re
from pathlib import Path
from dataclasses import dataclass, asdict
from typing import List, Optional, Tuple

from azure.storage.blob import BlobServiceClient, ContentSettings
from pdfminer.high_level import extract_text
from reportlab.lib.pagesizes import A4
from reportlab.pdfgen import canvas
from textwrap import wrap

from cfg import (BLOB_CONNECTION_STR, CONTAINER_CHUNKS, CONTAINER_MANIFESTS, 
                 LOCAL_IN_PDF, LOCAL_OUT)

logging.basicConfig(level=logging.INFO)
blob = BlobServiceClient.from_connection_string(BLOB_CONNECTION_STR)

# Constants
MAX_CHUNK_CHARS = 2000
HEADING_SEPARATOR = ";"

@dataclass
class ChunkMetadata:
    """Metadata for each chunk with hierarchical context"""
    chunk_id: str
    pdf_source: str
    topic: str
    subtopic: str
    sub_subtopic: Optional[str]
    heading_path: str
    content: str  # Clean content WITHOUT references
    references: List[str]
    chunk_index: int
    total_chunks: int
    blob_url: str
    char_count: int
    sequence: str
    
    def to_dict(self):
        """Convert to dictionary for JSON serialization"""
        return {
            'chunk_id': self.chunk_id,
            'pdf_source': self.pdf_source,
            'topic': self.topic,
            'subtopic': self.subtopic,
            'sub_subtopic': self.sub_subtopic,
            'heading_path': self.heading_path,
            'references': self.references,
            'chunk_index': self.chunk_index,
            'total_chunks': self.total_chunks,
            'blob_url': self.blob_url,
            'char_count': self.char_count,
            'sequence': self.sequence
        }

def is_heading(line: str) -> bool:
    """
    Detect if line is a heading (ALL CAPS with semicolon separators)
    Example: "TYPHOID & ENTERIC FEVER;CLINICAL WORKFLOW;WHEN TO SUSPECT"
    """
    ln = line.strip()
    if not ln or HEADING_SEPARATOR not in ln:
        return False
    
    letters = "".join(ch for ch in ln if ch.isalpha())
    if not letters:
        return False
    
    uppercase_ratio = sum(ch.isupper() for ch in letters) / len(letters)
    return uppercase_ratio > 0.8

def extract_references_improved(text: str) -> Tuple[str, List[str]]:
    """
    Improved reference extraction with proper URL handling.
    Returns: (clean_content, deduplicated_references)
    """
    # Split on "References:" marker (case insensitive)
    ref_split_pattern = r'\n\s*References?\s*:\s*\n'
    parts = re.split(ref_split_pattern, text, flags=re.IGNORECASE)
    
    if len(parts) > 1:
        clean_content = parts[0].strip()
        ref_section = "\n".join(parts[1:])
    else:
        clean_content = text
        ref_section = ""
    
    refs = []
    
    # ==========================================
    # STRATEGY 1: Extract Complete URLs
    # ==========================================
    # Pattern: Start from http/https, continue until whitespace/newline/bracket
    # This prevents URL fragmentation
    
    # Find all complete URLs in both content and reference section
    # Pattern explanation:
    # - https?:// - protocol
    # - [^\s\]\)\n]+ - any non-whitespace/bracket/newline chars
    # - (?:\([^\)]*\))? - optional parentheses in URL (like Wikipedia URLs)
    # - [^\s\]\)\n]* - continue until whitespace
    url_pattern = r'https?://[^\s\]\)\n]+(?:\([^\)]*\))?[^\s\]\)\n]*'
    
    all_urls = re.findall(url_pattern, text)
    
    for url in all_urls:
        # Clean trailing punctuation that's not part of URL
        url = re.sub(r'[,;:\.]$', '', url)
        
        # Remove markdown link syntax if present
        url = re.sub(r'\]\([^\)]+\)$', '', url)
        
        # Only keep URLs longer than 20 chars (filter fragments)
        if len(url) > 20 and url.startswith('http'):
            refs.append(url)
    
    # ==========================================
    # STRATEGY 2: Extract Citation Patterns
    # ==========================================
    # Match patterns like "IAP Standard Treatment Guidelines 2022"
    citation_patterns = [
        r'\b(IAP|WHO|CDC|BMJ|NCBI|ICMR|NCDC|NTAGI|EUCAST|CLSI)\s+[^\n\.]{5,100}(?:\d{4}|Guidelines?|Position Paper)',
        r'(StatPearls|Cochrane|IDSP|ACVIP|MoHFW|SEFI)\s+[^\n\.]{5,100}',
    ]
    
    for pattern in citation_patterns:
        matches = re.findall(pattern, text, re.IGNORECASE)
        for match in matches:
            if isinstance(match, tuple):
                match = ' '.join(str(m) for m in match if m)
            citation_text = str(match).strip()
            if len(citation_text) > 10:  # Meaningful citations only
                refs.append(citation_text)
    
    # ==========================================
    # STRATEGY 3: Extract Reference List Items
    # ==========================================
    # Pattern for explicit reference lines:
    # "- Reference Title (Source, Year) — URL"
    if ref_section:
        ref_lines = ref_section.split('\n')
        current_ref = ""
        
        for line in ref_lines:
            line = line.strip()
            if not line:
                continue
            
            # Check if line starts with bullet/dash
            if re.match(r'^[-•▪‣●]\s*', line):
                # Save previous reference if exists
                if current_ref:
                    refs.append(current_ref.strip())
                # Start new reference (remove bullet)
                current_ref = re.sub(r'^[-•▪‣●]\s*', '', line)
            else:
                # Continuation of current reference
                current_ref += " " + line
        
        # Add last reference
        if current_ref:
            refs.append(current_ref.strip())
    
    # ==========================================
    # DEDUPLICATION
    # ==========================================
    # Remove duplicates while preserving order
    # Use normalized comparison (lowercase, strip whitespace)
    unique_refs = []
    seen_normalized = []  # List to track normalized versions
    
    for ref in refs:
        ref_clean = ref.strip()
        
        # Skip very short fragments
        if len(ref_clean) < 15:
            continue
        
        # Normalize for comparison
        ref_normalized = ref_clean.lower().strip()
        
        # Check if this is a substring of an already added reference
        # (prevents "https://example.com" and "https://example.com/page" duplicates)
        is_duplicate = False
        should_replace_idx = -1
        
        for idx, seen_ref in enumerate(seen_normalized):
            if ref_normalized == seen_ref:
                # Exact duplicate, skip
                is_duplicate = True
                break
            elif ref_normalized in seen_ref:
                # Current ref is substring of existing (shorter), skip current
                is_duplicate = True
                break
            elif seen_ref in ref_normalized:
                # Existing ref is substring of current (current is longer), replace
                should_replace_idx = idx
                break
        
        if should_replace_idx >= 0:
            # Replace shorter reference with longer one
            unique_refs[should_replace_idx] = ref_clean
            seen_normalized[should_replace_idx] = ref_normalized
        elif not is_duplicate:
            # Add new reference
            unique_refs.append(ref_clean)
            seen_normalized.append(ref_normalized)
    
    # Remove inline reference markers from content (##IAP##, ##AMR##, etc.)
    clean_content = re.sub(r'##([A-Z]+)##', r'\1', clean_content)
    
    return clean_content.strip(), unique_refs

def parse_heading(heading: str) -> Tuple[str, str, Optional[str]]:
    """
    Parse hierarchical heading into components.
    Returns: (topic, subtopic, sub_subtopic)
    """
    parts = [p.strip() for p in heading.split(HEADING_SEPARATOR)]
    
    topic = parts[0] if len(parts) > 0 else "Unknown"
    subtopic = parts[1] if len(parts) > 1 else "General"
    sub_subtopic = parts[2] if len(parts) > 2 else None
    
    return topic, subtopic, sub_subtopic

def create_safe_chunk_id(topic: str, subtopic: str, sub_subtopic: Optional[str], 
                        part_idx: int = -1) -> str:
    """
    Create a safe, readable chunk ID with proper length limits.
    Format: TOPIC_SUBTOPIC_SUBSUB or TOPIC_SUBTOPIC_SUBSUB_partN
    """
    def clean(s: str, maxlen: int) -> str:
        # Remove special chars, keep alphanumeric and spaces
        s = re.sub(r'[^A-Za-z0-9\s]', '', s)
        # Replace spaces with underscore
        s = re.sub(r'\s+', '_', s)
        # Truncate at word boundary if possible
        if len(s) > maxlen:
            s = s[:maxlen]
            last_underscore = s.rfind('_')
            if last_underscore > maxlen * 0.6:
                s = s[:last_underscore]
        return s.upper()
    
    # Allocate length budget
    topic_part = clean(topic, 35)
    subtopic_part = clean(subtopic, 35)
    
    if sub_subtopic:
        subsub_part = clean(sub_subtopic, 35)
        base_id = f"{topic_part}_{subtopic_part}_{subsub_part}"
    else:
        base_id = f"{topic_part}_{subtopic_part}"
    
    if part_idx >= 0:
        base_id += f"_part{part_idx}"
    
    return base_id

def split_large_content(content: str, max_chars: int) -> List[str]:
    """
    Split content into chunks respecting paragraph/sentence boundaries.
    """
    if len(content) <= max_chars:
        return [content]
    
    chunks = []
    paragraphs = content.split('\n\n')
    current_chunk = ""
    
    for para in paragraphs:
        para = para.strip()
        if not para:
            continue
        
        # If single paragraph exceeds limit, split by sentences
        if len(para) > max_chars:
            sentences = re.split(r'(?<=[.!?])\s+', para)
            
            for sent in sentences:
                if len(current_chunk) + len(sent) + 2 <= max_chars:
                    current_chunk += sent + " "
                else:
                    if current_chunk.strip():
                        chunks.append(current_chunk.strip())
                    current_chunk = sent + " "
        else:
            # Try to add paragraph to current chunk
            if len(current_chunk) + len(para) + 2 <= max_chars:
                current_chunk += para + "\n\n"
            else:
                if current_chunk.strip():
                    chunks.append(current_chunk.strip())
                current_chunk = para + "\n\n"
    
    if current_chunk.strip():
        chunks.append(current_chunk.strip())
    
    return chunks if chunks else [content[:max_chars]]

def merge_split_headings(lines: List[str]) -> List[str]:
    """
    Merge headings that were split across multiple lines by PDF extraction.
    
    Example:
    Line 1: "TYPHOID & ENTERIC FEVER;ANTIMICROBIAL RESISTANCE AND"
    Line 2: "STEWARDSHIP;PRACTICAL STEWARDSHIP ACTIONS"
    
    Should become:
    "TYPHOID & ENTERIC FEVER;ANTIMICROBIAL RESISTANCE AND STEWARDSHIP;PRACTICAL STEWARDSHIP ACTIONS"
    """
    merged = []
    i = 0
    
    while i < len(lines):
        line = lines[i].strip()
        
        if not line:
            i += 1
            continue
        
        # Check if this looks like a heading
        if is_heading(line):
            # Count semicolons to determine heading level
            semicolon_count = line.count(HEADING_SEPARATOR)
            
            # Look ahead to see if next line continues the heading
            if i + 1 < len(lines):
                next_line = lines[i + 1].strip()
                
                # Check if next line could be continuation:
                # 1. It has semicolons (is also a heading-like line)
                # 2. Current line seems incomplete (ends with partial word or has < 3 semicolons)
                if next_line and is_heading(next_line):
                    # Check if combining makes sense
                    combined = line + " " + next_line
                    
                    # If combined line has more semicolons or forms a more complete heading
                    combined_semicolons = combined.count(HEADING_SEPARATOR)
                    
                    if combined_semicolons > semicolon_count or semicolon_count < 2:
                        # This is likely a split heading, merge them
                        merged.append(combined)
                        i += 2  # Skip both lines
                        continue
            
            merged.append(line)
        else:
            merged.append(lines[i])
        
        i += 1
    
    return merged
def generate_sequence(topic: str, subtopic: str, sub_subtopic: Optional[str],
                     topic_counter: int, subtopic_counter: dict, 
                     sub_subtopic_counter: dict) -> str:
    """
    Generate hierarchical sequence number.
    Examples: "1", "1a", "1a1", "2", "2a", "2b1"
    """
    # Base topic number
    sequence = str(topic_counter)
    
    # Add subtopic letter (a, b, c, ...)
    if subtopic and topic in subtopic_counter:
        subtopic_num = subtopic_counter[topic]
        if subtopic_num > 0:
            # Convert 1->a, 2->b, 3->c, etc.
            letter = chr(96 + subtopic_num)  # 97 is 'a'
            sequence += letter
    
    # Add sub-subtopic number
    if sub_subtopic and (topic, subtopic) in sub_subtopic_counter:
        sub_num = sub_subtopic_counter[(topic, subtopic)]
        if sub_num > 0:
            sequence += str(sub_num)
    
    return sequence
def create_hierarchical_chunks(lines: List[str]) -> List[ChunkMetadata]:
    """
    Create hierarchical chunks from PDF lines with improved reference handling.
    """
    # First, merge any split headings
    lines = merge_split_headings(lines)
    
    chunks = []
    current_heading = None
    current_content_lines = []
    # NEW: Add sequence tracking variables
    topic_counter = 0
    subtopic_counter = {}  # {topic: counter}
    sub_subtopic_counter = {}  # {(topic, subtopic): counter}
    
    for line in lines:
        ln = line.strip()
        if not ln:
            continue
        
        if is_heading(ln):
            # Save previous section if exists
            if current_heading and current_content_lines:
                full_content = "\n".join(current_content_lines)
                clean_content, refs = extract_references_improved(full_content)
                
                topic, subtopic, sub_subtopic = parse_heading(current_heading)
                sequence = generate_sequence(
                    topic, subtopic, sub_subtopic,
                    topic_counter, subtopic_counter, sub_subtopic_counter
                )
                content_parts = split_large_content(clean_content, MAX_CHUNK_CHARS)
                
                for idx, part in enumerate(content_parts):
                    chunk_id = create_safe_chunk_id(
                        topic, subtopic, sub_subtopic, 
                        part_idx=idx if len(content_parts) > 1 else -1
                    )
                    part_sequence = sequence
                    if len(content_parts) > 1:
                        part_sequence = f"{sequence}.{idx + 1}"
                    chunks.append(ChunkMetadata(
                        chunk_id=chunk_id,
                        pdf_source="",  # Set later
                        topic=topic,
                        subtopic=subtopic,
                        sub_subtopic=sub_subtopic,
                        heading_path=current_heading,
                        content=part,
                        references=refs,  # Same refs for all parts of same section
                        chunk_index=idx,
                        total_chunks=len(content_parts),
                        blob_url="",
                        char_count=len(part),
                        sequence=part_sequence
                    ))
            
            # Start new section
            current_heading = ln
            current_content_lines = []
            
            topic, subtopic, sub_subtopic = parse_heading(ln)
            
            # Update topic counter
            if topic not in subtopic_counter:
                topic_counter += 1
                subtopic_counter[topic] = 0
            
            # Update subtopic counter
            if subtopic:
                subtopic_counter[topic] += 1
                if (topic, subtopic) not in sub_subtopic_counter:
                    sub_subtopic_counter[(topic, subtopic)] = 0
            
            # Update sub-subtopic counter
            if sub_subtopic:
                sub_subtopic_counter[(topic, subtopic)] += 1
        else:
            current_content_lines.append(line)
    
    # Save final section
    if current_heading and current_content_lines:
        full_content = "\n".join(current_content_lines)
        clean_content, refs = extract_references_improved(full_content)
        
        topic, subtopic, sub_subtopic = parse_heading(current_heading)
        content_parts = split_large_content(clean_content, MAX_CHUNK_CHARS)
        
        for idx, part in enumerate(content_parts):
            chunk_id = create_safe_chunk_id(
                topic, subtopic, sub_subtopic,
                part_idx=idx if len(content_parts) > 1 else -1
            )
            
            chunks.append(ChunkMetadata(
                chunk_id=chunk_id,
                pdf_source="",
                topic=topic,
                subtopic=subtopic,
                sub_subtopic=sub_subtopic,
                heading_path=current_heading,
                content=part,
                references=refs,
                chunk_index=idx,
                total_chunks=len(content_parts),
                blob_url="",
                char_count=len(part),
                sequence=part_sequence
            ))
    
    return chunks
def chunk_to_pdf(chunk: ChunkMetadata, out_path: Path) -> None:
    """Create PDF for a chunk with clean content and references"""
    c = canvas.Canvas(str(out_path), pagesize=A4)
    y = A4[1] - 50
    
    # Title
    c.setFont("Helvetica-Bold", 12)
    title_lines = wrap(chunk.heading_path, 80)
    for ln in title_lines[:3]:
        c.drawString(40, y, ln)
        y -= 16
    
    # Metadata
    y -= 10
    c.setFont("Helvetica-Oblique", 9)
    meta_text = f"Topic: {chunk.topic} | Subtopic: {chunk.subtopic}"
    if chunk.sub_subtopic:
        meta_text += f" | Sub-subtopic: {chunk.sub_subtopic[:40]}"
    if chunk.total_chunks > 1:
        meta_text += f" | Part {chunk.chunk_index + 1}/{chunk.total_chunks}"
    c.drawString(40, y, meta_text)
    y -= 20
    
    # Content
    c.setFont("Helvetica", 10)
    content_lines = wrap(chunk.content, 100)
    
    for ln in content_lines:
        if y < 150:  # Leave space for references
            c.showPage()
            y = A4[1] - 50
            c.setFont("Helvetica", 10)
        c.drawString(40, y, ln)
        y -= 13
    
    # References section
    if chunk.references:
        y -= 20
        if y < 200:
            c.showPage()
            y = A4[1] - 50
        
        c.setFont("Helvetica-Bold", 10)
        c.drawString(40, y, "References:")
        y -= 15
        
        c.setFont("Helvetica", 8)
        for i, ref in enumerate(chunk.references[:15], 1):  # Max 15 refs
            ref_text = f"{i}. {ref}"
            ref_lines = wrap(ref_text, 110)
            for ref_line in ref_lines:
                if y < 50:
                    c.showPage()
                    y = A4[1] - 50
                    c.setFont("Helvetica", 8)
                c.drawString(40, y, ref_line)
                y -= 11
    
    c.showPage()
    c.save()

def upload_blob(local: Path, container: str, name: str | None = None) -> str:
    """Upload file to Azure Blob Storage"""
    name = name or local.name
    ct = "application/pdf"
    client = blob.get_container_client(container)
    
    try:
        client.create_container()
    except Exception:
        pass
    
    with local.open("rb") as fh:
        client.upload_blob(name, fh, overwrite=True,
                          content_settings=ContentSettings(content_type=ct))
    
    return f"https://{blob.account_name}.blob.core.windows.net/{container}/{name}"

def process_pdf(pdf: Path, outdir: Path) -> List[dict]:
    """Process a single PDF and return manifest entries"""
    logging.info(f"Processing: {pdf.name}")
    
    text = extract_text(str(pdf)) or ""
    lines = text.splitlines()
    
    if not lines:
        logging.warning(f"{pdf.name} - empty, skipped")
        return []
    
    chunks = create_hierarchical_chunks(lines)
    
    if not chunks:
        logging.warning(f"{pdf.name} - no chunks created")
        return []
    
    manifest_entries = []
    
    for chunk in chunks:
        chunk.pdf_source = pdf.name
        
        fname = f"{chunk.chunk_id}.pdf"
        local = outdir / fname
        
        try:
            chunk_to_pdf(chunk, local)
            url = upload_blob(local, CONTAINER_CHUNKS, fname)
            chunk.blob_url = url
            
            manifest_entries.append(chunk.to_dict())
            
            logging.info(f"  ✓ {chunk.chunk_id} ({chunk.char_count} chars, {len(chunk.references)} refs)")
            
        except Exception as e:
            logging.error(f"  ✗ Failed to create chunk {chunk.chunk_id}: {e}")
    
    return manifest_entries

def main():
    parser = argparse.ArgumentParser()
    parser.add_argument("--in", default=str(LOCAL_IN_PDF),
                       help="folder containing PDFs")
    args = parser.parse_args()
    
    indir = Path(args.__dict__["in"])
    outdir = LOCAL_OUT / "chunks"
    outdir.mkdir(parents=True, exist_ok=True)
    
    all_manifest = []
    pdf_files = list(indir.glob("*.pdf"))
    
    logging.info(f"Found {len(pdf_files)} PDF files")
    
    for pdf in pdf_files:
        entries = process_pdf(pdf, outdir)
        all_manifest.extend(entries)
    
    # Write manifest
    m_path = LOCAL_OUT / "manifest-1.json"
    m_path.write_text(json.dumps(all_manifest, indent=2), encoding="utf-8")
    
    # Upload manifest
    upload_blob(m_path, CONTAINER_MANIFESTS, m_path.name)
    
    # Statistics
    topics = set(e["topic"] for e in all_manifest)
    subtopics = set((e["topic"], e["subtopic"]) for e in all_manifest)
    avg_chars = sum(e["char_count"] for e in all_manifest) // max(len(all_manifest), 1)
    total_refs = sum(len(e["references"]) for e in all_manifest)
    avg_refs = total_refs // max(len(all_manifest), 1)
    
    logging.info("="*70)
    logging.info(f"Stage 1 Complete!")
    logging.info(f"  Total chunks: {len(all_manifest)}")
    logging.info(f"  From PDFs: {len(pdf_files)}")
    logging.info(f"  Unique topics: {len(topics)}")
    logging.info(f"  Unique subtopics: {len(subtopics)}")
    logging.info(f"  Avg chunk size: {avg_chars} chars")
    logging.info(f"  Total references: {total_refs}")
    logging.info(f"  Avg references per chunk: {avg_refs}")
    logging.info(f"  Manifest: {m_path}")
    logging.info("="*70)

if __name__ == "__main__":
    main()