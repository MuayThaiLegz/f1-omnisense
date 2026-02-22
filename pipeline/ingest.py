"""Multi-source document ingestion pipeline.

Combines multiple data sources into a unified set of text chunks
ready for vectorstore embedding. Sources can include:

  1. PDF extractions (LLM-enhanced or native text)
  2. Structured data (CSV/JSON records)
  3. Reference tables (standards, lookup data)
  4. Deep extraction tracker output

Domain-agnostic: customize the loaders for your specific data sources.

Usage:
    from pipeline.ingest import create_pdf_chunks, rebuild_vectorstore

    # Create chunks from PDF extractions
    chunks = create_pdf_chunks(extraction_results)

    # Full rebuild with all sources
    rebuild_vectorstore(pdf_extractions=results, csv_dir=Path("data/"))
"""

from __future__ import annotations

import json
import re
from collections import defaultdict
from pathlib import Path

from langchain_text_splitters import RecursiveCharacterTextSplitter
from langchain_core.documents import Document


# ── Regex patterns for metadata enrichment ──────────────────────────────

RE_IDENTIFIER = re.compile(r"\b[A-Z]{1,4}\d{3,6}[A-Z]?\b")
RE_REFERENCE = re.compile(r"\b\d{2,3}-[A-Z]-\d{1,3}\b")


def _extract_metadata_from_text(text: str) -> dict:
    """Extract identifier tags from text content."""
    return {
        "identifiers": ",".join(sorted(set(RE_IDENTIFIER.findall(text)))),
        "references": ",".join(sorted(set(RE_REFERENCE.findall(text)))),
    }


def _safe_val(obj, *keys, default=""):
    """Safely navigate nested dicts/values. Returns default if any key fails."""
    for key in keys:
        if obj is None:
            return default
        if isinstance(obj, dict):
            obj = obj.get(key)
        else:
            return default
    if obj is None:
        return default
    return obj


# ── Source 1: PDF Extraction Results ─────────────────────────────────────

def create_pdf_chunks(
    extractions: list[dict],
    chunk_size: int = 1000,
    chunk_overlap: int = 200,
) -> list[Document]:
    """Create LangChain Documents from extracted PDFs.

    Each extraction dict should have:
      - full_text: str
      - filename: str
      - category: str (optional)
      - folder: str (optional)
    """
    splitter = RecursiveCharacterTextSplitter(
        chunk_size=chunk_size,
        chunk_overlap=chunk_overlap,
        separators=["\n\n", "\n", ". ", " ", ""],
    )
    chunks = []

    for doc in extractions:
        text = doc.get("full_text", "")
        if not text or len(text) < 50:
            continue

        category = doc.get("category", "general")
        filename = doc.get("filename", "unknown")
        folder = doc.get("folder", "unknown")

        texts = splitter.split_text(text)
        for i, chunk_text in enumerate(texts):
            meta = _extract_metadata_from_text(chunk_text)
            meta.update({
                "source_file": filename,
                "category": category,
                "folder": folder,
                "chunk_index": i,
                "total_chunks": len(texts),
                "data_type": "pdf_text",
            })
            chunks.append(Document(page_content=chunk_text, metadata=meta))

    return chunks


def create_table_chunks(tables: list[dict]) -> list[Document]:
    """Create searchable documents from extracted tables.

    Each table dict should have:
      - headers: list[str]
      - rows: list[list[str]]
      - source: str
      - page: int (optional)
    """
    table_docs = []

    for table in tables:
        headers = table.get("headers", [])
        rows = table.get("rows", [])
        source = table.get("source", "unknown")
        if not headers or not rows:
            continue

        text_parts = [f"Table from {source}, page {table.get('page', '?')}"]
        text_parts.append("Headers: " + " | ".join(str(h) for h in headers))
        for row in rows[:20]:
            text_parts.append(" | ".join(str(c) for c in row))

        table_docs.append(Document(
            page_content="\n".join(text_parts),
            metadata={
                "source_file": source,
                "category": "table_data",
                "page": table.get("page", 0),
                "num_rows": len(rows),
                "data_type": "table",
            },
        ))

    return table_docs


# ── Source 2: Structured Data (CSV/JSON) ─────────────────────────────────

def load_json_record_chunks(
    path: Path,
    category: str,
    text_formatter,
) -> list[Document]:
    """Load JSON records and convert to searchable chunks.

    Args:
        path: Path to JSON file containing a list of records.
        category: Category label for the vectorstore.
        text_formatter: Callable(record) -> str that formats a record as text.

    Returns:
        List of Document objects.
    """
    if not path.exists():
        print(f"  WARNING: {path.name} not found")
        return []

    records = json.loads(path.read_text())
    chunks = []

    for rec in records:
        text = text_formatter(rec)
        if not text or len(text) < 20:
            continue

        meta = _extract_metadata_from_text(text)
        meta.update({
            "source_file": path.name,
            "category": category,
            "data_type": "structured_record",
        })
        chunks.append(Document(page_content=text, metadata=meta))

    return chunks


def load_csv_chunks(
    path: Path,
    category: str,
    text_formatter=None,
) -> list[Document]:
    """Load CSV records and convert to searchable chunks.

    Args:
        path: Path to CSV file.
        category: Category label for the vectorstore.
        text_formatter: Optional callable(row_dict) -> str. If None,
                       uses default "key: value" formatting.

    Returns:
        List of Document objects.
    """
    import csv

    if not path.exists():
        print(f"  WARNING: {path.name} not found")
        return []

    chunks = []
    with open(path, newline="", encoding="utf-8-sig") as f:
        reader = csv.DictReader(f)
        for row in reader:
            if text_formatter:
                text = text_formatter(row)
            else:
                text = "\n".join(f"{k}: {v}" for k, v in row.items() if v)

            if not text or len(text) < 20:
                continue

            meta = _extract_metadata_from_text(text)
            meta.update({
                "source_file": path.name,
                "category": category,
                "data_type": "csv_record",
            })
            chunks.append(Document(page_content=text, metadata=meta))

    return chunks


# ── Source 3: Master Tracker (Deep Extraction) ────────────────────────────

def load_master_tracker_chunks(
    tracker_path: Path,
    category_map: dict[str, str | None] | None = None,
    min_fields: dict[str, int] | None = None,
) -> list[Document]:
    """Load items from the deep PDF extraction master tracker.

    Args:
        tracker_path: Path to master_tracker.json
        category_map: Maps tracker categories to vectorstore categories.
                     None values = skip that category.
        min_fields: Minimum non-null data fields per category to keep an item.
    """
    if not tracker_path.exists():
        print(f"  WARNING: master_tracker.json not found at {tracker_path}")
        return []

    cat_map = category_map or {}
    min_f = min_fields or {}

    tracker = json.loads(tracker_path.read_text())
    chunks = []
    skipped = 0

    for cat_name, cat_data in tracker.get("categories", {}).items():
        vs_category = cat_map.get(cat_name, cat_name)  # Default: use same name
        if vs_category is None:
            skipped += cat_data.get("total", 0)
            continue

        min_field_count = min_f.get(cat_name, 1)

        for item in cat_data.get("items", []):
            data = item.get("data", {})

            # Quality filter: count non-null, non-internal fields
            useful_fields = sum(
                1 for k, v in data.items()
                if not k.startswith("_") and v is not None
                and v != "" and v != [] and v != {}
            )
            if useful_fields < min_field_count:
                skipped += 1
                continue

            item_id = item.get("id", "unknown")
            pdfs = item.get("pdfs", [])

            text = _item_to_text(cat_name, item_id, data, pdfs)
            if len(text) < 30:
                skipped += 1
                continue

            meta = _extract_metadata_from_text(text)
            meta.update({
                "source_file": "master_tracker.json",
                "category": vs_category,
                "data_type": "deep_extraction",
                "tracker_category": cat_name,
                "item_id": item_id,
                "pdf_sources": ",".join(p[:60] for p in pdfs[:3]),
            })

            chunks.append(Document(page_content=text, metadata=meta))

    print(f"  Loaded {len(chunks)} items from master tracker ({skipped} filtered out)")
    return chunks


def _item_to_text(category: str, item_id: str, data: dict, pdfs: list) -> str:
    """Convert a master tracker item to searchable text."""
    parts = [f"[{category}] {item_id}"]
    pdf_short = ", ".join(p[:60] for p in (pdfs or [])[:3])
    if pdf_short:
        parts.append(f"Source: {pdf_short}")

    for key, val in data.items():
        if key.startswith("_") or val is None:
            continue
        if isinstance(val, list):
            if val and isinstance(val[0], list):
                # Table rows — format as text
                for row in val[:15]:
                    parts.append(" | ".join(str(c) for c in row))
            elif val:
                parts.append(f"{key}: {', '.join(str(v) for v in val[:20])}")
        elif isinstance(val, dict):
            sub = ", ".join(f"{k}={v}" for k, v in val.items() if v is not None)
            if sub:
                parts.append(f"{key}: {sub}")
        else:
            parts.append(f"{key}: {val}")

    return "\n".join(parts)


# ── Master Ingestion Pipeline ────────────────────────────────────────────

def build_all_chunks(
    pdf_extractions: list[dict] | None = None,
    tables: list[dict] | None = None,
    json_sources: list[tuple[Path, str, callable]] | None = None,
    csv_sources: list[tuple[Path, str, callable | None]] | None = None,
    tracker_path: Path | None = None,
    tracker_category_map: dict[str, str | None] | None = None,
    chunk_size: int = 1000,
    chunk_overlap: int = 200,
) -> list[Document]:
    """Build all chunks from multiple sources.

    Args:
        pdf_extractions: List of extraction result dicts (from vision_extract or pipeline).
        tables: List of table dicts with headers/rows.
        json_sources: List of (path, category, formatter) tuples.
        csv_sources: List of (path, category, formatter) tuples.
        tracker_path: Path to master_tracker.json for deep extraction results.
        tracker_category_map: Category mapping for tracker items.
        chunk_size: Text chunk size for PDF splitting.
        chunk_overlap: Overlap between chunks.

    Returns:
        Combined list of all Document chunks.
    """
    all_chunks = []

    # Source 1: PDFs
    if pdf_extractions:
        print("\n[Source 1] Creating PDF chunks...")
        pdf_chunks = create_pdf_chunks(pdf_extractions, chunk_size, chunk_overlap)
        print(f"  Created {len(pdf_chunks)} PDF text chunks")
        all_chunks.extend(pdf_chunks)

    # Source 1b: Tables
    if tables:
        table_chunks = create_table_chunks(tables)
        print(f"  Created {len(table_chunks)} table chunks")
        all_chunks.extend(table_chunks)

    # Source 2: Structured data
    if json_sources:
        print("\n[Source 2] Loading structured data...")
        for path, category, formatter in json_sources:
            chunks = load_json_record_chunks(path, category, formatter)
            print(f"  {path.name}: {len(chunks)} chunks")
            all_chunks.extend(chunks)

    if csv_sources:
        for path, category, formatter in csv_sources:
            chunks = load_csv_chunks(path, category, formatter)
            print(f"  {path.name}: {len(chunks)} chunks")
            all_chunks.extend(chunks)

    # Source 3: Master tracker
    if tracker_path:
        print("\n[Source 3] Loading deep extraction master tracker...")
        tracker_chunks = load_master_tracker_chunks(
            tracker_path, tracker_category_map
        )
        all_chunks.extend(tracker_chunks)

    print(f"\nTotal chunks: {len(all_chunks)}")
    return all_chunks
