"""
Pipeline Orchestrator: Runs Stage 1 → Stage 2 → Stage 3 for each PDF sequentially.
Usage: python run_pipeline.py --in data/input_pdfs
"""
from __future__ import annotations
import argparse
import logging
import subprocess
import sys
import time
from pathlib import Path

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s [ORCHESTRATOR] %(levelname)s %(message)s"
)
log = logging.getLogger("orchestrator")

STAGE_BREAK_SECONDS = 10


def run_stage(cmd: list[str], stage_name: str, pdf_name: str) -> bool:
    """Run a stage command, streaming output directly to terminal. Returns True on success."""
    print(f"\n{'=' * 70}")
    print(f"  Starting {stage_name} for: {pdf_name}")
    print(f"  Command: {' '.join(cmd)}")
    print(f"{'=' * 70}\n")

    result = subprocess.run(cmd)  # stdout/stderr inherit from parent — fully live

    print(f"\n{'=' * 70}")
    if result.returncode != 0:
        print(f"  ✗ {stage_name} FAILED for {pdf_name} (exit code {result.returncode})")
        print(f"{'=' * 70}\n")
        return False

    print(f"  ✓ {stage_name} COMPLETE for {pdf_name}")
    print(f"{'=' * 70}\n")
    return True


def stage_break():
    print(f"\n--- Waiting {STAGE_BREAK_SECONDS}s before next stage ---\n")
    time.sleep(STAGE_BREAK_SECONDS)


def main():
    parser = argparse.ArgumentParser(description="Run full 3-stage pipeline per PDF topic")
    parser.add_argument("--in", default="data/input_pdfs", help="Folder containing PDFs")
    parser.add_argument("--skip-on-failure", action="store_true",
                        help="Skip to next PDF if a stage fails (default: stop)")
    args = parser.parse_args()

    indir = Path(args.__dict__["in"])
    if not indir.exists():
        log.error("Input directory not found: %s", indir)
        sys.exit(1)

    pdf_files = sorted(indir.glob("*.pdf"))
    if not pdf_files:
        log.error("No PDF files found in: %s", indir)
        sys.exit(1)

    print(f"\n{'█' * 70}")
    print(f"  PIPELINE START — {len(pdf_files)} document(s) to process")
    for p in pdf_files:
        print(f"    • {p.name}")
    print(f"{'█' * 70}\n")

    failed = []

    for i, pdf in enumerate(pdf_files, 1):
        doc_name = pdf.stem  # exact filename stem used as --topic

        print(f"\n{'█' * 70}")
        print(f"  DOCUMENT {i}/{len(pdf_files)}: {pdf.name}  (topic: {doc_name})")
        print(f"{'█' * 70}")

        # ── Stage 1 ──────────────────────────────────────────────────────────
        ok = run_stage(
            [sys.executable, "stage1_chunk_pdf.py", "--in", str(indir)],
            "Stage 1 — Chunking",
            pdf.name
        )
        if not ok:
            failed.append((pdf.name, "Stage 1"))
            if not args.skip_on_failure:
                log.error("Stopping. Use --skip-on-failure to continue past errors.")
                break
            continue

        stage_break()

        # ── Stage 2 ──────────────────────────────────────────────────────────
        ok = run_stage(
            [sys.executable, "stage2_meta_tag.py", "--topic", doc_name],
            "Stage 2 — Meta-tagging",
            pdf.name
        )
        if not ok:
            failed.append((pdf.name, "Stage 2"))
            if not args.skip_on_failure:
                log.error("Stopping. Use --skip-on-failure to continue past errors.")
                break
            continue

        stage_break()

        # ── Stage 3 ──────────────────────────────────────────────────────────
        ok = run_stage(
            [sys.executable, "stage3_vector_index.py"],
            "Stage 3 — Vector Indexing",
            pdf.name
        )
        if not ok:
            failed.append((pdf.name, "Stage 3"))
            if not args.skip_on_failure:
                log.error("Stopping. Use --skip-on-failure to continue past errors.")
                break
            continue

        stage_break()

        print(f"\n  ✓✓✓ All 3 stages complete for: {pdf.name}\n")

    # ── Final summary ─────────────────────────────────────────────────────────
    print(f"\n{'█' * 70}")
    print(f"  PIPELINE FINISHED")
    print(f"  Processed : {len(pdf_files)} document(s)")
    print(f"  Succeeded : {len(pdf_files) - len(failed)}")
    print(f"  Failed    : {len(failed)}")
    if failed:
        print(f"\n  Failures:")
        for name, stage in failed:
            print(f"    ✗  {name}  @  {stage}")
    print(f"{'█' * 70}\n")

    sys.exit(1 if failed else 0)


if __name__ == "__main__":
    main()