"""PDF → multi-view image renderer with slicing, zooming, and augmentation.

Converts each PDF page into multiple image views:
  - Full page at 3x zoom
  - 4 overlapping quadrants (10% overlap)
  - Auto-detected zoom regions (tables, diagrams)

All images are cached — re-rendering is skipped if output exists and PDF is unchanged.
"""

import base64
import io
import json
import os
from dataclasses import dataclass, field
from pathlib import Path

import fitz  # PyMuPDF
import pdfplumber
from PIL import Image, ImageEnhance, ImageFilter

Image.MAX_IMAGE_PIXELS = 300_000_000  # Allow large drawings (default 178M)

# ── Configuration ────────────────────────────────────────────────────────

RENDER_ZOOM = 3          # 3x native resolution
OVERLAP = 0.10           # 10% quadrant overlap
MAX_PIXELS = 30_000_000  # Max pixels per image before downscaling
MIN_REGION_PX = 200      # Minimum zoom region dimension (pixels)
TABLE_ZOOM = 2.0         # Extra zoom factor for detected table regions
DIAGRAM_ZOOM = 1.5       # Extra zoom factor for detected diagram regions


# ── Data classes ─────────────────────────────────────────────────────────

@dataclass
class ZoomedRegion:
    """A cropped + zoomed region of interest from a page."""
    path: Path
    region_type: str  # "table" or "diagram"
    bbox: tuple[int, int, int, int]  # (x0, y0, x1, y1) in rendered pixels
    zoom_factor: float
    source_page: int


@dataclass
class PageViews:
    """All rendered views for a single PDF page."""
    page_num: int
    full: Path
    quadrants: dict[str, Path] = field(default_factory=dict)
    zoomed: list[ZoomedRegion] = field(default_factory=list)
    native_text: str = ""
    native_tables: list = field(default_factory=list)
    word_count: int = 0


# ── Image processing ────────────────────────────────────────────────────

def enhance_image(img: Image.Image) -> Image.Image:
    """Apply contrast, sharpness, and detail enhancement."""
    img = ImageEnhance.Contrast(img).enhance(1.3)
    img = ImageEnhance.Sharpness(img).enhance(2.0)
    img = img.filter(ImageFilter.DETAIL)
    return img


MAX_B64_BYTES = 4_500_000  # Stay under Claude's 5MB per-image limit
MAX_DIMENSION = 7900       # Claude max is 8000px per dimension — leave margin
CHUNK_OVERLAP = 0.05       # 5% overlap between chunks


def _encode_single(img: Image.Image) -> str:
    """Encode a single image to base64, progressively shrinking if over 5MB."""
    # Downscale if over pixel limit
    px = img.size[0] * img.size[1]
    if px > MAX_PIXELS:
        scale = (MAX_PIXELS / px) ** 0.5
        img = img.resize(
            (int(img.size[0] * scale), int(img.size[1] * scale)),
            Image.LANCZOS,
        )

    for _ in range(5):
        buf = io.BytesIO()
        img.save(buf, format="PNG", optimize=True)
        encoded = base64.b64encode(buf.getvalue()).decode()
        if len(encoded) <= MAX_B64_BYTES:
            return encoded
        img = img.resize(
            (int(img.size[0] * 0.7), int(img.size[1] * 0.7)),
            Image.LANCZOS,
        )

    buf = io.BytesIO()
    img.save(buf, format="PNG", optimize=True)
    return base64.b64encode(buf.getvalue()).decode()


def img_to_b64(img_or_path) -> str:
    """Convert PIL image or file path to base64 PNG string.

    For images within Claude's limits, returns a single base64 string.
    For oversized images, downscales to fit. Use img_to_b64_chunks()
    to split into multiple images instead.
    """
    if isinstance(img_or_path, (str, Path)):
        img = Image.open(img_or_path)
    else:
        img = img_or_path

    # Downscale if any dimension exceeds Claude's 8000px limit
    w, h = img.size
    if w > MAX_DIMENSION or h > MAX_DIMENSION:
        scale = min(MAX_DIMENSION / w, MAX_DIMENSION / h)
        img = img.resize((int(w * scale), int(h * scale)), Image.LANCZOS)

    return _encode_single(img)


def img_to_b64_chunks(img_or_path) -> list[str]:
    """Convert image to a list of base64 PNG strings.

    If the image fits within Claude's limits (8000px per dimension),
    returns a single-element list. Otherwise, splits into a grid of
    overlapping chunks that each fit within the limit.
    """
    if isinstance(img_or_path, (str, Path)):
        img = Image.open(img_or_path)
    else:
        img = img_or_path

    w, h = img.size

    # If it fits, just return one chunk
    if w <= MAX_DIMENSION and h <= MAX_DIMENSION:
        return [_encode_single(img)]

    # Calculate grid: how many chunks needed per axis
    cols = max(1, -(-w // MAX_DIMENSION))  # ceil division
    rows = max(1, -(-h // MAX_DIMENSION))

    # Chunk size with overlap
    chunk_w = int(w / cols * (1 + CHUNK_OVERLAP))
    chunk_h = int(h / rows * (1 + CHUNK_OVERLAP))
    # Clamp chunk dimensions to MAX_DIMENSION
    chunk_w = min(chunk_w, MAX_DIMENSION)
    chunk_h = min(chunk_h, MAX_DIMENSION)

    step_x = (w - chunk_w) / max(1, cols - 1) if cols > 1 else 0
    step_y = (h - chunk_h) / max(1, rows - 1) if rows > 1 else 0

    chunks = []
    for row in range(rows):
        for col in range(cols):
            x0 = int(col * step_x)
            y0 = int(row * step_y)
            x1 = min(x0 + chunk_w, w)
            y1 = min(y0 + chunk_h, h)
            crop = img.crop((x0, y0, x1, y1))
            chunks.append(_encode_single(crop))

    return chunks


def split_quadrants(
    img: Image.Image, overlap: float = OVERLAP
) -> dict[str, Image.Image]:
    """Split image into 4 overlapping quadrants."""
    w, h = img.size
    hw = int(w * (0.5 + overlap))
    hh = int(h * (0.5 + overlap))
    ow = int(w * (0.5 - overlap))
    oh = int(h * (0.5 - overlap))

    return {
        "q1_top_left": img.crop((0, 0, hw, hh)),
        "q2_top_right": img.crop((ow, 0, w, hh)),
        "q3_bottom_left": img.crop((0, oh, hw, h)),
        "q4_bottom_right": img.crop((ow, oh, w, h)),
    }


# ── Zoom region detection ───────────────────────────────────────────────

def _detect_table_regions(
    pdf_path: Path, page_num: int, rendered_w: int, rendered_h: int
) -> list[tuple[int, int, int, int]]:
    """Use pdfplumber to find table bounding boxes, scaled to rendered coords."""
    regions = []
    try:
        with pdfplumber.open(pdf_path) as pdf:
            if page_num >= len(pdf.pages):
                return regions
            page = pdf.pages[page_num]
            pw, ph = float(page.width), float(page.height)
            sx = rendered_w / pw
            sy = rendered_h / ph

            tables = page.find_tables()
            for tbl in tables:
                bbox = tbl.bbox  # (x0, y0, x1, y1) in PDF coords
                rx0 = max(0, int(bbox[0] * sx) - 10)
                ry0 = max(0, int(bbox[1] * sy) - 10)
                rx1 = min(rendered_w, int(bbox[2] * sx) + 10)
                ry1 = min(rendered_h, int(bbox[3] * sy) + 10)

                if (rx1 - rx0) >= MIN_REGION_PX and (ry1 - ry0) >= MIN_REGION_PX:
                    regions.append((rx0, ry0, rx1, ry1))
    except Exception:
        pass  # pdfplumber may fail on some pages
    return regions


def _extract_native_tables(pdf_path: Path, page_num: int) -> list[dict]:
    """Extract table data using pdfplumber."""
    tables_data = []
    try:
        with pdfplumber.open(pdf_path) as pdf:
            if page_num >= len(pdf.pages):
                return tables_data
            page = pdf.pages[page_num]
            for tbl in page.extract_tables():
                if tbl and len(tbl) > 1:
                    headers = [str(c or "").strip() for c in tbl[0]]
                    rows = [
                        [str(c or "").strip() for c in row]
                        for row in tbl[1:]
                        if any(c for c in row)
                    ]
                    if headers and rows:
                        tables_data.append({"headers": headers, "rows": rows})
    except Exception:
        pass
    return tables_data


def detect_and_zoom_regions(
    img: Image.Image,
    pdf_path: Path,
    page_num: int,
    output_dir: Path,
    stem: str,
) -> list[ZoomedRegion]:
    """Auto-detect tables/diagrams and create zoomed crops."""
    regions = []
    w, h = img.size

    # Table regions from pdfplumber
    table_bboxes = _detect_table_regions(pdf_path, page_num, w, h)
    for i, bbox in enumerate(table_bboxes[:3]):  # Max 3 zoom regions
        crop = img.crop(bbox)
        # Zoom in further
        new_w = int(crop.size[0] * TABLE_ZOOM)
        new_h = int(crop.size[1] * TABLE_ZOOM)
        zoomed = crop.resize((new_w, new_h), Image.LANCZOS)
        zoomed = enhance_image(zoomed)

        out_path = output_dir / f"{stem}_zoom_table_{i}.png"
        zoomed.save(out_path, optimize=True)
        regions.append(ZoomedRegion(
            path=out_path,
            region_type="table",
            bbox=bbox,
            zoom_factor=TABLE_ZOOM,
            source_page=page_num,
        ))

    return regions


# ── Page rendering ───────────────────────────────────────────────────────

def render_page(
    doc: fitz.Document,
    page_idx: int,
    pdf_path: Path,
    output_dir: Path,
) -> PageViews:
    """Render a single PDF page into multiple views."""
    stem = f"page_{page_idx + 1:03d}"
    output_dir.mkdir(parents=True, exist_ok=True)

    # 1. Render at high resolution
    page = doc[page_idx]
    mat = fitz.Matrix(RENDER_ZOOM, RENDER_ZOOM)
    pix = page.get_pixmap(matrix=mat)
    img = Image.frombytes("RGB", [pix.width, pix.height], pix.samples)

    # 2. Enhance
    img_enhanced = enhance_image(img)

    # 3. Save full page
    full_path = output_dir / f"{stem}_full.png"
    img_enhanced.save(full_path, optimize=True)

    # 4. Extract native text
    native_text = page.get_text("text") or ""
    word_count = len(native_text.split())

    # 5. Extract native tables
    native_tables = _extract_native_tables(pdf_path, page_idx)

    # 6. Split quadrants
    quads = split_quadrants(img_enhanced)
    quad_paths = {}
    for name, quad_img in quads.items():
        qpath = output_dir / f"{stem}_{name}.png"
        quad_img.save(qpath, optimize=True)
        quad_paths[name] = qpath

    # 7. Detect and zoom regions
    zoomed = detect_and_zoom_regions(
        img_enhanced, pdf_path, page_idx, output_dir, stem
    )

    return PageViews(
        page_num=page_idx + 1,
        full=full_path,
        quadrants=quad_paths,
        zoomed=zoomed,
        native_text=native_text,
        native_tables=native_tables,
        word_count=word_count,
    )


# ── PDF rendering ────────────────────────────────────────────────────────

def _pdf_stem(pdf_path: Path) -> str:
    """Create a filesystem-safe stem from a PDF filename."""
    stem = pdf_path.stem
    # Collapse whitespace and special chars
    safe = "".join(c if c.isalnum() or c in "-_" else "_" for c in stem)
    # Collapse runs of underscores
    while "__" in safe:
        safe = safe.replace("__", "_")
    return safe.strip("_")[:80]


def render_pdf(
    pdf_path: Path,
    output_base: Path,
    force: bool = False,
) -> list[PageViews]:
    """Render all pages of a PDF into multi-view images.

    Returns list of PageViews (one per page).
    Caches results — skips rendering if output exists and PDF is unchanged.
    """
    stem = _pdf_stem(pdf_path)
    output_dir = output_base / stem

    # Check cache
    manifest_path = output_dir / "_manifest.json"
    if not force and manifest_path.exists():
        manifest = json.loads(manifest_path.read_text())
        pdf_mtime = os.path.getmtime(pdf_path)
        if manifest.get("pdf_mtime") == pdf_mtime:
            # Rebuild PageViews from manifest
            pages = []
            for pm in manifest["pages"]:
                pv = PageViews(
                    page_num=pm["page_num"],
                    full=Path(pm["full"]),
                    quadrants={k: Path(v) for k, v in pm["quadrants"].items()},
                    zoomed=[
                        ZoomedRegion(
                            path=Path(z["path"]),
                            region_type=z["region_type"],
                            bbox=tuple(z["bbox"]),
                            zoom_factor=z["zoom_factor"],
                            source_page=z["source_page"],
                        )
                        for z in pm.get("zoomed", [])
                    ],
                    native_text=pm.get("native_text", ""),
                    native_tables=pm.get("native_tables", []),
                    word_count=pm.get("word_count", 0),
                )
                pages.append(pv)
            print(f"  [cached] {stem}: {len(pages)} pages")
            return pages

    output_dir.mkdir(parents=True, exist_ok=True)
    doc = fitz.open(str(pdf_path))
    total = len(doc)
    pages = []

    for i in range(total):
        pv = render_page(doc, i, pdf_path, output_dir)
        pages.append(pv)

    doc.close()

    # Save manifest for caching
    manifest = {
        "pdf_path": str(pdf_path),
        "pdf_mtime": os.path.getmtime(pdf_path),
        "stem": stem,
        "page_count": total,
        "pages": [
            {
                "page_num": pv.page_num,
                "full": str(pv.full),
                "quadrants": {k: str(v) for k, v in pv.quadrants.items()},
                "zoomed": [
                    {
                        "path": str(z.path),
                        "region_type": z.region_type,
                        "bbox": list(z.bbox),
                        "zoom_factor": z.zoom_factor,
                        "source_page": z.source_page,
                    }
                    for z in pv.zoomed
                ],
                "native_text": pv.native_text,
                "native_tables": pv.native_tables,
                "word_count": pv.word_count,
            }
            for pv in pages
        ],
    }
    manifest_path.write_text(json.dumps(manifest, indent=2))

    zoom_count = sum(len(pv.zoomed) for pv in pages)
    print(f"  [rendered] {stem}: {total} pages, "
          f"{total * 4} quadrants, {zoom_count} zoom regions")

    return pages


def discover_pdfs(base_dir: Path, skip_names: set[str] | None = None) -> list[Path]:
    """Find all PDF files recursively in a directory.

    Args:
        base_dir: Directory to search recursively.
        skip_names: Set of filenames (lowercased) to skip.
    """
    skip = skip_names or set()
    pdfs = sorted(base_dir.rglob("*.pdf")) + sorted(base_dir.rglob("*.PDF"))
    # Deduplicate (case-insensitive on some filesystems)
    seen = set()
    unique = []
    for p in pdfs:
        key = str(p).lower()
        if key not in seen and p.name.lower() not in skip:
            seen.add(key)
            unique.append(p)
    return unique
