"""Async OCR service for zero-text PDF drawings.

Extracts text from pure CAD/engineering/technical drawings that have zero
native extractable text. Uses EasyOCR for text recognition with
bounding-box positions, and PyMuPDF for high-DPI page rendering.

Provides an async job pattern:
  - submit_ocr_job() → starts background thread
  - get_job_status() → poll for completion
  - get_result() → retrieve cached results

Usage:
    from pipeline.ocr import submit_ocr_job, get_job_status, get_result

    job = submit_ocr_job(Path("drawing.pdf"), "my_drawing")
    # ... poll get_job_status(job["job_id"]) ...
    result = get_result("my_drawing")
"""

import json
import tempfile
import threading
import time
import uuid
from pathlib import Path
from typing import Optional


class _NumpyEncoder(json.JSONEncoder):
    def default(self, obj):
        try:
            import numpy as np
            if isinstance(obj, (np.integer,)):
                return int(obj)
            if isinstance(obj, (np.floating,)):
                return float(obj)
            if isinstance(obj, np.ndarray):
                return obj.tolist()
        except ImportError:
            pass
        return super().default(obj)


# Default output directory (override via set_output_dir)
_output_dir: Path = Path(__file__).parent / "output" / "ocr"
_images_dir: Path = _output_dir / "page_images"

# In-memory job store
_jobs: dict[str, dict] = {}


def set_output_dir(output_dir: Path):
    """Configure where OCR results and images are saved."""
    global _output_dir, _images_dir
    _output_dir = output_dir
    _images_dir = output_dir / "page_images"


def _ensure_dirs():
    _output_dir.mkdir(parents=True, exist_ok=True)
    _images_dir.mkdir(parents=True, exist_ok=True)


def _get_cached_result(drawing_key: str) -> Optional[dict]:
    """Check if we already have OCR results for this drawing."""
    cache_path = _output_dir / f"{drawing_key}.json"
    if cache_path.exists():
        return json.loads(cache_path.read_text())
    return None


def _save_result(drawing_key: str, result: dict):
    """Cache OCR results to JSON."""
    _ensure_dirs()
    cache_path = _output_dir / f"{drawing_key}.json"
    cache_path.write_text(json.dumps(result, indent=2, ensure_ascii=False, cls=_NumpyEncoder))


def _run_ocr_extraction(pdf_path: Path, drawing_key: str, dpi: int = 200) -> dict:
    """Run OCR on a PDF and return structured text blocks with positions."""
    import fitz  # PyMuPDF

    result = {
        "drawing_key": drawing_key,
        "source_file": pdf_path.name,
        "source_folder": pdf_path.parent.name,
        "pages": [],
        "all_text": "",
        "text_block_count": 0,
        "metadata": {},
    }

    doc = fitz.open(str(pdf_path))
    result["metadata"]["num_pages"] = len(doc)
    result["metadata"]["page_sizes"] = []

    # Check native text first
    native_text = ""
    for page in doc:
        native_text += page.get_text("text")
    result["metadata"]["native_word_count"] = len(native_text.split())

    all_ocr_text = []

    for page_num in range(len(doc)):
        page = doc[page_num]
        page_width = page.rect.width
        page_height = page.rect.height
        result["metadata"]["page_sizes"].append({
            "width_pt": round(page_width, 1),
            "height_pt": round(page_height, 1),
        })

        # Render page at high DPI
        zoom = dpi / 72.0
        mat = fitz.Matrix(zoom, zoom)
        pix = page.get_pixmap(matrix=mat)

        # Save page image
        _ensure_dirs()
        img_name = f"{drawing_key}_page{page_num + 1}.png"
        img_path = _images_dir / img_name
        pix.save(str(img_path))

        # Run OCR
        try:
            import easyocr
            import numpy as np
            from PIL import Image

            img = Image.frombytes("RGB", [pix.width, pix.height], pix.samples)
            img_np = np.array(img)

            reader = easyocr.Reader(['en'], gpu=True, verbose=False)
            ocr_results = reader.readtext(img_np)

            text_blocks = []
            page_text_parts = []

            for bbox, text, confidence in ocr_results:
                if confidence < 0.15:
                    continue

                # Convert pixel coords back to PDF points
                x_min = min(p[0] for p in bbox) / zoom
                y_min = min(p[1] for p in bbox) / zoom
                x_max = max(p[0] for p in bbox) / zoom
                y_max = max(p[1] for p in bbox) / zoom

                block = {
                    "text": text,
                    "confidence": round(float(confidence), 3),
                    "bbox_pt": {
                        "x": round(x_min, 1),
                        "y": round(y_min, 1),
                        "width": round(x_max - x_min, 1),
                        "height": round(y_max - y_min, 1),
                    },
                    "bbox_px": {
                        "x": round(min(p[0] for p in bbox), 1),
                        "y": round(min(p[1] for p in bbox), 1),
                        "width": round(max(p[0] for p in bbox) - min(p[0] for p in bbox), 1),
                        "height": round(max(p[1] for p in bbox) - min(p[1] for p in bbox), 1),
                    },
                }
                text_blocks.append(block)
                page_text_parts.append(text)

            page_text = " ".join(page_text_parts)
            all_ocr_text.append(page_text)

            result["pages"].append({
                "page_number": page_num + 1,
                "image_file": img_name,
                "text_blocks": text_blocks,
                "text_block_count": len(text_blocks),
                "full_text": page_text,
                "word_count": len(page_text.split()),
            })

        except ImportError:
            result["pages"].append({
                "page_number": page_num + 1,
                "image_file": img_name,
                "text_blocks": [],
                "text_block_count": 0,
                "full_text": "",
                "word_count": 0,
                "error": "EasyOCR not installed. Install with: pip install easyocr",
            })
        except Exception as e:
            result["pages"].append({
                "page_number": page_num + 1,
                "image_file": img_name,
                "text_blocks": [],
                "text_block_count": 0,
                "full_text": "",
                "word_count": 0,
                "error": str(e),
            })

    doc.close()

    result["all_text"] = "\n\n".join(all_ocr_text)
    result["text_block_count"] = sum(p["text_block_count"] for p in result["pages"])
    result["metadata"]["total_word_count"] = len(result["all_text"].split())
    result["metadata"]["processed_at"] = time.strftime("%Y-%m-%dT%H:%M:%S")

    return result


def _worker(job_id: str, pdf_path: Path, drawing_key: str, dpi: int):
    """Background worker for OCR extraction."""
    _jobs[job_id]["status"] = "running"
    _jobs[job_id]["started_at"] = time.strftime("%Y-%m-%dT%H:%M:%S")

    try:
        result = _run_ocr_extraction(pdf_path, drawing_key, dpi)
        _save_result(drawing_key, result)
        _jobs[job_id]["status"] = "completed"
        _jobs[job_id]["result_summary"] = {
            "pages": len(result["pages"]),
            "text_blocks": result["text_block_count"],
            "words": result["metadata"]["total_word_count"],
        }
        _jobs[job_id]["completed_at"] = time.strftime("%Y-%m-%dT%H:%M:%S")
    except Exception as e:
        _jobs[job_id]["status"] = "failed"
        _jobs[job_id]["error"] = str(e)
        _jobs[job_id]["completed_at"] = time.strftime("%Y-%m-%dT%H:%M:%S")
    finally:
        # Clean up temp files from uploaded PDFs
        if pdf_path.exists() and str(pdf_path).startswith(tempfile.gettempdir()):
            try:
                pdf_path.unlink()
            except OSError:
                pass


def submit_ocr_job(
    pdf_path: Path,
    drawing_key: str,
    force: bool = False,
    dpi: int = 200,
) -> dict:
    """Submit an OCR extraction job for a PDF.

    Args:
        pdf_path: Path to the PDF file.
        drawing_key: Unique key for caching results.
        force: If True, re-extract even if cached.
        dpi: Render DPI (default 200).

    Returns:
        Dict with job_id and status.
    """
    if not pdf_path.exists():
        raise FileNotFoundError(f"PDF not found: {pdf_path}")

    # Check cache
    if not force:
        cached = _get_cached_result(drawing_key)
        if cached:
            return {
                "status": "cached",
                "drawing_key": drawing_key,
                "result_summary": {
                    "pages": len(cached["pages"]),
                    "text_blocks": cached["text_block_count"],
                    "words": cached["metadata"]["total_word_count"],
                },
            }

    job_id = str(uuid.uuid4())[:8]
    _jobs[job_id] = {
        "job_id": job_id,
        "drawing_key": drawing_key,
        "drawing_name": pdf_path.stem,
        "status": "queued",
        "submitted_at": time.strftime("%Y-%m-%dT%H:%M:%S"),
    }

    thread = threading.Thread(target=_worker, args=(job_id, pdf_path, drawing_key, dpi))
    thread.start()

    return {"job_id": job_id, "status": "queued", "drawing_key": drawing_key}


def get_job_status(job_id: str) -> Optional[dict]:
    """Get status of an OCR job by job ID."""
    return _jobs.get(job_id)


def get_result(drawing_key: str) -> Optional[dict]:
    """Get cached OCR result by drawing key."""
    return _get_cached_result(drawing_key)


def list_jobs() -> list[dict]:
    """List all submitted jobs."""
    return list(_jobs.values())


def run_ocr_sync(pdf_path: Path, drawing_key: str, dpi: int = 200) -> dict:
    """Run OCR synchronously (blocking) and cache the result."""
    result = _run_ocr_extraction(pdf_path, drawing_key, dpi)
    _save_result(drawing_key, result)
    return result
