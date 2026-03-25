"""
heading_normalizer.py
─────────────────────
Fixes split headings in manually-integrated medical education PDFs.

The heading pattern is deterministic: all-caps lines containing semicolons,
ending with a semicolon. When a heading is split across lines by PDF
extraction, this script joins the fragments into one line — no LLM needed.

For every PDF in INPUT_DIR:
  1. Extract text with pdfminer
  2. Fix split headings with a regex-based merger (instant, free, reliable)
  3. Write the corrected text to a _fixed.txt and _fixed.pdf in OUTPUT_DIR

Requirements:
    pip install pdfminer.six reportlab tqdm
"""

from __future__ import annotations

import argparse
import logging
from pathlib import Path

from pdfminer.high_level import extract_text
from tqdm import tqdm

# ─── CONFIGURATION ────────────────────────────────────────────────────────────
INPUT_DIR  = Path("./input_pdfs")
OUTPUT_DIR = Path("./output_fixed")
# ──────────────────────────────────────────────────────────────────────────────

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s  %(levelname)-8s  %(message)s",
)
log = logging.getLogger(__name__)


# ─── HEADING DETECTION ────────────────────────────────────────────────────────

def _is_heading_fragment(line: str) -> bool:
    """
    Return True if a line looks like a heading or heading continuation:
      - Predominantly uppercase (>80% of letters)
      - Contains at least one semicolon
    Does NOT require the line to end with ';' — that's how we detect fragments.
    """
    stripped = line.strip()
    if not stripped or ";" not in stripped:
        return False
    letters = [c for c in stripped if c.isalpha()]
    if not letters:
        return False
    return sum(c.isupper() for c in letters) / len(letters) > 0.80


def _ends_heading(line: str) -> bool:
    """A heading is complete when its non-whitespace tail is a semicolon."""
    return line.rstrip().endswith(";")


# ─── CORE FIX ─────────────────────────────────────────────────────────────────

def fix_headings(text: str) -> str:
    """
    Join heading lines that were split across multiple lines by PDF extraction.

    Rules:
    - A heading fragment is an all-caps line containing ';' that does NOT end with ';'.
    - Keep accumulating subsequent all-caps+semicolon lines until we find one ending with ';'.
    - Join fragments with a single space.
    - Everything else (body text, references, URLs, blank lines) passes through unchanged.
    """
    lines = text.splitlines(keepends=True)
    out: list[str] = []
    i = 0

    while i < len(lines):
        line = lines[i]
        stripped = line.rstrip("\n\r")

        if _is_heading_fragment(stripped):
            # Accumulate until heading is complete (ends with ';')
            accumulated = stripped.rstrip()
            j = i + 1

            while not _ends_heading(accumulated) and j < len(lines):
                next_stripped = lines[j].rstrip("\n\r")

                # Only merge if next line also looks like a heading fragment
                next_letters = [c for c in next_stripped if c.isalpha()]
                next_is_caps = (
                    next_letters and
                    sum(c.isupper() for c in next_letters) / len(next_letters) > 0.80
                )

                if next_is_caps and next_stripped.strip():
                    accumulated = accumulated + " " + next_stripped.strip()
                    j += 1
                else:
                    # Body text — stop merging
                    break

            # Preserve line ending
            line_ending = "\n" if lines[j - 1].endswith("\n") else ""
            out.append(accumulated + line_ending)
            i = j

        else:
            out.append(line)
            i += 1

    return "".join(out)


# ─── PDF I/O ──────────────────────────────────────────────────────────────────

def extract_pdf_text(pdf_path: Path) -> str:
    log.info(f"  Extracting: {pdf_path.name}")
    text = extract_text(str(pdf_path)) or ""
    if not text.strip():
        log.warning("  No text extracted — skipping.")
    return text


def write_txt(text: str, out_path: Path) -> None:
    out_path.write_text(text, encoding="utf-8")
    log.info(f"  ✔  TXT → {out_path.name}")


def write_pdf_out(text: str, out_path: Path) -> None:
    from reportlab.lib.pagesizes import A4
    from reportlab.pdfgen import canvas
    from textwrap import wrap as tw

    c = canvas.Canvas(str(out_path), pagesize=A4)
    _, H = A4
    y = H - 50
    lh = 13
    c.setFont("Helvetica", 10)

    for raw_line in text.splitlines():
        for wl in (tw(raw_line, 100) if raw_line.strip() else [""]):
            if y < 50:
                c.showPage()
                y = H - 50
                c.setFont("Helvetica", 10)
            c.drawString(40, y, wl)
            y -= lh

    c.showPage()
    c.save()
    log.info(f"  ✔  PDF → {out_path.name}")


# ─── PIPELINE ─────────────────────────────────────────────────────────────────

def process_pdf(pdf_path: Path, output_dir: Path, make_pdf: bool) -> None:
    raw = extract_pdf_text(pdf_path)
    if not raw.strip():
        return

    fixed = fix_headings(raw)

    # Count merged fragments for logging
    merged = sum(
        1 for l in raw.splitlines()
        if _is_heading_fragment(l.rstrip()) and not _ends_heading(l.rstrip())
    )
    log.info(f"  Merged {merged} split heading fragment(s).")

    stem = pdf_path.stem
    write_txt(fixed, output_dir / f"{stem}_fixed.txt")

    if make_pdf:
        try:
            write_pdf_out(fixed, output_dir / f"{stem}_fixed.pdf")
        except Exception as exc:
            log.warning(f"  PDF write failed ({exc}) — TXT is still available.")


def main() -> None:
    parser = argparse.ArgumentParser(
        description="Fix split headings in medical education PDFs (no LLM, instant)."
    )
    parser.add_argument("--input",  default=str(INPUT_DIR), help="Folder of input PDFs")
    parser.add_argument("--output", default=str(OUTPUT_DIR), help="Folder for fixed outputs")
    parser.add_argument("--no-pdf", action="store_true", help="Skip PDF output; TXT only")
    args = parser.parse_args()

    in_dir  = Path(args.input)
    out_dir = Path(args.output)
    out_dir.mkdir(parents=True, exist_ok=True)

    pdfs = sorted(in_dir.glob("*.pdf"))
    if not pdfs:
        log.error(f"No PDFs found in {in_dir}")
        return

    log.info(f"Found {len(pdfs)} PDF(s) in {in_dir}")

    for pdf in tqdm(pdfs, desc="PDFs", unit="file"):
        log.info(f"Processing: {pdf.name}")
        process_pdf(pdf, out_dir, make_pdf=not args.no_pdf)

    log.info("=" * 60)
    log.info(f"Done. Fixed files are in: {out_dir}")


if __name__ == "__main__":
    main()