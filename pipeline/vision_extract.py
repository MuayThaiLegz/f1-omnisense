"""Smart vision extraction for PDF pages with minimal native text.

Only runs expensive vision API calls on pages with <50 words of native text
(drawing/diagram pages). Text-heavy pages use native PyMuPDF text only.

This is a standalone extraction tool — separate from the multi-pass pipeline.
Use it for quick single-model extraction of all pages in a PDF.

Usage:
    from pipeline.vision_extract import extract_pdf_with_vision

    result = extract_pdf_with_vision(
        pdf_path=Path("document.pdf"),
        category="general",
        client=groq_client,
        model="meta-llama/llama-4-maverick-17b-128e-instruct",
    )
"""

from __future__ import annotations

import base64
import io
import json
import time
from pathlib import Path

import fitz  # PyMuPDF
from PIL import Image, ImageEnhance, ImageFilter

RENDER_ZOOM = 3
OVERLAP = 0.10
MAX_PIXELS = 30_000_000
NATIVE_TEXT_THRESHOLD = 50  # words — pages above this skip vision

# ── Default prompt (customize per domain) ─────────────────────────────────

DEFAULT_PROMPT = """\
You are analyzing a SECTION of a technical document.

CRITICAL: Read the ACTUAL text, numbers, and labels visible in the image. Do NOT invent or guess values. If something is illegible, skip it.

Extract ALL visible information including:
1. Text content: paragraphs, labels, notes, annotations, title blocks
2. Tables: preserve headers and row data exactly as shown
3. Specifications: dimensions, pressures, temperatures with units
4. Identifiers: equipment tags, reference codes, part numbers
5. Standards references: any standard codes mentioned

Return valid JSON:
{"text_content": "all readable text from this section",
 "tables": [{"headers": ["col1", "col2"], "rows": [["val1", "val2"]]}],
 "specifications": [{"type": "pressure", "value": "8.5", "unit": "bara"}],
 "equipment_tags": ["TAG1", "TAG2"],
 "pipe_refs": ["REF-1"],
 "kks_codes": ["CODE123"],
 "standards_refs": ["STANDARD 1"]}

Return ONLY valid JSON, no other text."""


# ── Utility functions ────────────────────────────────────────────────────

def _render_page(pdf_doc, page_idx: int) -> Image.Image:
    """Render a PDF page at high resolution and enhance."""
    page = pdf_doc[page_idx]
    mat = fitz.Matrix(RENDER_ZOOM, RENDER_ZOOM)
    pix = page.get_pixmap(matrix=mat)
    img = Image.frombytes("RGB", [pix.width, pix.height], pix.samples)
    img = ImageEnhance.Contrast(img).enhance(1.3)
    img = ImageEnhance.Sharpness(img).enhance(2.0)
    img = img.filter(ImageFilter.DETAIL)
    return img


def _split_quadrants(img: Image.Image) -> dict[str, Image.Image]:
    """Split image into 4 overlapping quadrants."""
    w, h = img.size
    return {
        "top_left": img.crop((0, 0, int(w * (0.5 + OVERLAP)), int(h * (0.5 + OVERLAP)))),
        "top_right": img.crop((int(w * (0.5 - OVERLAP)), 0, w, int(h * (0.5 + OVERLAP)))),
        "bottom_left": img.crop((0, int(h * (0.5 - OVERLAP)), int(w * (0.5 + OVERLAP)), h)),
        "bottom_right": img.crop((int(w * (0.5 - OVERLAP)), int(h * (0.5 - OVERLAP)), w, h)),
    }


def _img_to_b64(img: Image.Image) -> str:
    """Convert PIL image to base64, downscaling if over pixel limit."""
    px = img.size[0] * img.size[1]
    if px > MAX_PIXELS:
        scale = (MAX_PIXELS / px) ** 0.5
        img = img.resize((int(img.size[0] * scale), int(img.size[1] * scale)), Image.LANCZOS)
    buf = io.BytesIO()
    img.save(buf, format="PNG", optimize=True)
    return base64.b64encode(buf.getvalue()).decode()


def _extract_json(text: str) -> dict | None:
    """Extract JSON from response, handling markdown code blocks."""
    text = text.strip()
    if "```" in text:
        parts = text.split("```")
        for part in parts[1:]:
            cleaned = part.strip()
            if cleaned.startswith("json"):
                cleaned = cleaned[4:].strip()
            if cleaned.startswith("{"):
                try:
                    return json.loads(cleaned)
                except json.JSONDecodeError:
                    continue
    try:
        return json.loads(text)
    except json.JSONDecodeError:
        start = text.find("{")
        end = text.rfind("}") + 1
        if start >= 0 and end > start:
            try:
                return json.loads(text[start:end])
            except json.JSONDecodeError:
                return None
    return None


def _call_vision(client, model: str, img_b64: str, prompt: str) -> tuple[dict | None, float, int]:
    """Send image to Groq vision API and parse response."""
    t0 = time.time()
    try:
        resp = client.chat.completions.create(
            model=model,
            messages=[{
                "role": "user",
                "content": [
                    {"type": "text", "text": prompt},
                    {"type": "image_url", "image_url": {"url": f"data:image/png;base64,{img_b64}"}},
                ],
            }],
            temperature=0.05,
            max_tokens=4096,
        )
        elapsed = time.time() - t0
        tokens = resp.usage.completion_tokens if resp.usage else 0
        data = _extract_json(resp.choices[0].message.content)
        return data, elapsed, tokens
    except Exception as e:
        elapsed = time.time() - t0
        err_str = str(e)
        if "429" in err_str or "rate" in err_str.lower():
            print(f"    Rate limited, waiting 10s...")
            time.sleep(10)
        else:
            print(f"    API error: {e}")
        return None, elapsed, 0


def _vision_text_from_data(data: dict) -> str:
    """Convert structured vision JSON to readable text."""
    parts = []
    text_content = data.get("text_content", "")
    if text_content:
        parts.append(text_content)

    for table in data.get("tables", []):
        headers = table.get("headers", [])
        rows = table.get("rows", [])
        if headers:
            parts.append("Table: " + " | ".join(str(h) for h in headers))
        for row in rows[:20]:
            parts.append(" | ".join(str(c) for c in row))

    specs = data.get("specifications", [])
    if specs:
        spec_lines = []
        for s in specs:
            spec_lines.append(f"{s.get('type', '')}: {s.get('value', '')} {s.get('unit', '')}")
        parts.append("Specifications: " + "; ".join(spec_lines))

    tags = data.get("equipment_tags", [])
    if tags:
        parts.append("Equipment: " + ", ".join(tags))

    refs = data.get("pipe_refs", [])
    if refs:
        parts.append("References: " + ", ".join(refs))

    codes = data.get("kks_codes", [])
    if codes:
        parts.append("Codes: " + ", ".join(codes))

    stds = data.get("standards_refs", [])
    if stds:
        parts.append("Standards: " + ", ".join(stds))

    return "\n".join(parts)


# ── Main extraction ─────────────────────────────────────────────────────

def extract_pdf_with_vision(
    pdf_path: Path,
    category: str,
    client,
    model: str,
    prompt: str | None = None,
    rate_limit_sleep: float = 2.0,
) -> dict:
    """Extract all pages from a PDF using native text + optional LLM vision.

    Args:
        pdf_path: Path to the PDF.
        category: Document category label.
        client: Groq client instance (or compatible API client).
        model: Model ID to use for vision calls.
        prompt: Custom prompt. Uses DEFAULT_PROMPT if None.
        rate_limit_sleep: Sleep between API calls (seconds).

    Returns:
        Dict with pages, full_text, metadata, and stats.
    """
    doc = fitz.open(str(pdf_path))
    n_pages = doc.page_count
    extraction_prompt = prompt or DEFAULT_PROMPT

    pages = []
    total_api_calls = 0
    total_api_time = 0.0
    total_tokens = 0
    pages_with_vision = 0

    for page_idx in range(n_pages):
        native_text = doc[page_idx].get_text("text")
        word_count = len(native_text.split())

        page_data = {
            "page_num": page_idx + 1,
            "native_text": native_text,
            "vision_text": "",
            "combined_text": native_text,
            "vision_data": None,
            "used_vision": False,
            "native_word_count": word_count,
            "api_time_s": 0.0,
            "tokens": 0,
        }

        # Only run vision on pages with minimal native text
        if word_count < NATIVE_TEXT_THRESHOLD:
            img = _render_page(doc, page_idx)
            quadrants = _split_quadrants(img)
            quad_results = []
            page_time = 0.0
            page_tokens = 0

            for qname, qimg in quadrants.items():
                b64 = _img_to_b64(qimg)
                data, elapsed, tokens = _call_vision(client, model, b64, extraction_prompt)
                page_time += elapsed
                page_tokens += tokens
                total_api_calls += 1

                if data:
                    quad_results.append(data)

                time.sleep(rate_limit_sleep)

            # Merge quadrant results
            if quad_results:
                merged_data = _merge_vision_results(quad_results)
                vision_text = _vision_text_from_data(merged_data)
                page_data["vision_text"] = vision_text
                page_data["vision_data"] = merged_data
                page_data["combined_text"] = (native_text + "\n\n" + vision_text).strip()

            page_data["used_vision"] = True
            page_data["api_time_s"] = round(page_time, 1)
            page_data["tokens"] = page_tokens
            pages_with_vision += 1
            total_api_time += page_time
            total_tokens += page_tokens

            print(f"    Page {page_idx + 1}: VISION ({len(quad_results)}/4 OK, {page_time:.1f}s)")
        else:
            print(f"    Page {page_idx + 1}: native ({word_count} words)")

        pages.append(page_data)

    doc.close()

    full_text = "\n\n".join(p["combined_text"] for p in pages if p["combined_text"])

    return {
        "filename": pdf_path.name,
        "folder": pdf_path.parent.name,
        "category": category,
        "num_pages": n_pages,
        "pages": pages,
        "full_text": full_text,
        "metadata": {
            "native_word_count": sum(p["native_word_count"] for p in pages),
            "vision_word_count": sum(len(p["vision_text"].split()) for p in pages),
            "combined_word_count": len(full_text.split()),
        },
        "stats": {
            "pages_with_vision": pages_with_vision,
            "pages_native_only": n_pages - pages_with_vision,
            "total_api_calls": total_api_calls,
            "total_api_time_s": round(total_api_time, 1),
            "total_tokens": total_tokens,
        },
    }


def _merge_vision_results(results: list[dict]) -> dict:
    """Merge and deduplicate results from multiple quadrants."""
    merged = {
        "text_content": "",
        "tables": [],
        "specifications": [],
        "equipment_tags": [],
        "pipe_refs": [],
        "kks_codes": [],
        "standards_refs": [],
    }

    texts = []
    seen_specs = set()
    seen_tags = set()
    seen_refs = set()
    seen_kks = set()
    seen_stds = set()

    for data in results:
        if not data:
            continue

        tc = data.get("text_content", "")
        if tc:
            texts.append(tc)

        for table in data.get("tables", []):
            if table.get("headers") or table.get("rows"):
                merged["tables"].append(table)

        for spec in data.get("specifications", []):
            key = f"{spec.get('type', '')}:{spec.get('value', '')}:{spec.get('unit', '')}"
            if key not in seen_specs:
                seen_specs.add(key)
                merged["specifications"].append(spec)

        for tag in data.get("equipment_tags", []):
            tag = tag.strip()
            if tag and tag not in seen_tags:
                seen_tags.add(tag)
                merged["equipment_tags"].append(tag)

        for ref in data.get("pipe_refs", []):
            ref = ref.strip()
            if ref and ref not in seen_refs:
                seen_refs.add(ref)
                merged["pipe_refs"].append(ref)

        for kks in data.get("kks_codes", []):
            kks = kks.strip()
            if kks and kks not in seen_kks:
                seen_kks.add(kks)
                merged["kks_codes"].append(kks)

        for std in data.get("standards_refs", []):
            std = std.strip()
            if std and std not in seen_stds:
                seen_stds.add(std)
                merged["standards_refs"].append(std)

    merged["text_content"] = "\n".join(texts)

    return merged
