"""RAG Chatbot server — F1 Knowledge Base Agent.

Queries MongoDB Atlas vector search for context, then generates
answers via Groq LLM (llama-3.3-70b-versatile).

Usage:
    python pipeline/chat_server.py          # Runs on port 8100
"""

from __future__ import annotations

import os
import sys
import json
import tempfile
from pathlib import Path

import numpy as np

from fastapi import FastAPI, UploadFile, File
from fastapi.middleware.cors import CORSMiddleware
from pydantic import BaseModel

# Ensure project root is on path
sys.path.insert(0, str(Path(__file__).parent.parent))

from dotenv import load_dotenv
load_dotenv(Path(__file__).parent.parent / ".env")

from groq import Groq
from pipeline.vectorstore import AtlasVectorStore
from pipeline.embeddings import NomicEmbedder
from pipeline.model_3d_server import router as model_3d_router, mount_3d_static

# ── Config ───────────────────────────────────────────────────────────────

GROQ_MODEL = os.getenv("GROQ_REASONING_MODEL", "llama-3.3-70b-versatile")
PORT = int(os.getenv("PORT", os.getenv("API_PORT", "8100")))

app = FastAPI(title="F1 OmniSense API")
app.add_middleware(
    CORSMiddleware,
    allow_origins=["*"],
    allow_methods=["*"],
    allow_headers=["*"],
)

# Mount 3D model generation routes
app.include_router(model_3d_router)
mount_3d_static(app)

# Lazy-init singletons
_groq: Groq | None = None
_vs: AtlasVectorStore | None = None
_embedder: NomicEmbedder | None = None
_clip_index: dict | None = None
_clip_embedder = None

CLIP_INDEX_PATH = Path(__file__).parent.parent / "f1data" / "McMedia" / "clip_index.json"


def get_groq() -> Groq:
    global _groq
    if _groq is None:
        _groq = Groq(api_key=os.getenv("GROQ_API_KEY"))
    return _groq


def get_vs() -> AtlasVectorStore:
    global _vs
    if _vs is None:
        _vs = AtlasVectorStore()
    return _vs


def get_embedder() -> NomicEmbedder:
    global _embedder
    if _embedder is None:
        _embedder = NomicEmbedder()
    return _embedder


def get_clip_index() -> dict:
    """Load pre-built CLIP index from disk."""
    global _clip_index
    if _clip_index is None:
        if not CLIP_INDEX_PATH.exists():
            raise FileNotFoundError(
                f"CLIP index not found. Run: python pipeline/clip_index.py"
            )
        with open(CLIP_INDEX_PATH) as f:
            _clip_index = json.load(f)
        # Pre-compute numpy arrays for fast search
        _clip_index["_image_vecs"] = np.array(
            [img["embedding"] for img in _clip_index["images"]]
        )
    return _clip_index


def get_clip_embedder():
    """Lazy-load CLIP embedder for query embedding."""
    global _clip_embedder
    if _clip_embedder is None:
        from pipeline.embeddings import CLIPEmbedder
        _clip_embedder = CLIPEmbedder()
    return _clip_embedder


# ── Models ───────────────────────────────────────────────────────────────

class ChatMessage(BaseModel):
    role: str  # "user" or "assistant"
    content: str


class ChatRequest(BaseModel):
    message: str
    history: list[ChatMessage] = []


class ChatResponse(BaseModel):
    answer: str
    sources: list[dict]


# ── System Prompt ────────────────────────────────────────────────────────

SYSTEM_PROMPT = """You are the F1 OmniSense Knowledge Agent — an expert on Formula 1 technical regulations, car specifications, equipment, and engineering standards.

You have access to a knowledge base extracted from FIA 2024 Technical Regulations and related engineering documents. When answering questions:

1. Use ONLY the provided context to answer. If the context doesn't contain enough information, say so clearly.
2. Cite specific regulation IDs (e.g., "Article 3.5.2") and page numbers when available.
3. Be precise with numerical values, units, and tolerances.
4. For dimensional specifications, always include the value and unit.
5. When discussing equipment, reference tags and types.
6. Keep answers concise but thorough. Use bullet points for lists.
7. If a question is ambiguous, briefly clarify what you're answering.

You are speaking with F1 engineers and technical staff — use appropriate technical language."""


# ── RAG Pipeline ─────────────────────────────────────────────────────────

def retrieve_context(query: str, k: int = 8) -> list[dict]:
    """Embed query and retrieve relevant documents from Atlas."""
    embedder = get_embedder()
    vs = get_vs()

    query_vec = embedder.embed([query])[0]
    docs = vs.similarity_search(query, k=k, query_embedding=query_vec)

    sources = []
    for doc in docs:
        sources.append({
            "content": doc.page_content,
            "data_type": doc.metadata.get("data_type", ""),
            "category": doc.metadata.get("category", ""),
            "source": doc.metadata.get("source", ""),
            "page": doc.metadata.get("page", 0),
        })
    return sources


def build_rag_prompt(query: str, sources: list[dict], history: list[ChatMessage]) -> list[dict]:
    """Build the full prompt with system, context, history, and user query."""
    context_parts = []
    for i, src in enumerate(sources, 1):
        context_parts.append(
            f"[{i}] ({src['data_type']}/{src['category']}) "
            f"Page {src['page']} — {src['source']}\n{src['content']}"
        )
    context_block = "\n\n".join(context_parts)

    messages = [{"role": "system", "content": SYSTEM_PROMPT}]

    # Add conversation history (last 10 messages)
    for msg in history[-10:]:
        messages.append({"role": msg.role, "content": msg.content})

    # User message with context
    user_prompt = f"""CONTEXT FROM KNOWLEDGE BASE:
{context_block}

USER QUESTION:
{query}

Answer based on the context above. Cite regulation IDs and page numbers where applicable."""

    messages.append({"role": "user", "content": user_prompt})
    return messages


# ── Endpoints ────────────────────────────────────────────────────────────

@app.post("/chat", response_model=ChatResponse)
def chat(req: ChatRequest):
    """RAG chat endpoint."""
    # 1. Retrieve context
    sources = retrieve_context(req.message, k=8)

    # 2. Build prompt
    messages = build_rag_prompt(req.message, sources, req.history)

    # 3. Generate answer via Groq
    groq = get_groq()
    completion = groq.chat.completions.create(
        model=GROQ_MODEL,
        messages=messages,
        temperature=0.3,
        max_tokens=2048,
    )

    answer = completion.choices[0].message.content

    return ChatResponse(answer=answer, sources=sources)


@app.get("/health")
def health():
    vs = get_vs()
    return {
        "status": "ok",
        "model": GROQ_MODEL,
        "documents": vs.count(),
        "services": ["knowledge_agent", "3d_model_gen"],
    }


# ── CLIP Visual Search ───────────────────────────────────────────────

@app.get("/visual-search")
def visual_search(q: str, k: int = 8):
    """Search images by text query using CLIP embeddings."""
    index = get_clip_index()
    clip = get_clip_embedder()

    # Embed text query into CLIP space
    query_vec = np.array(clip.embed_text(q))
    query_vec = query_vec / np.linalg.norm(query_vec)

    # Cosine similarity against all image embeddings
    image_vecs = index["_image_vecs"]
    image_norms = image_vecs / np.linalg.norm(image_vecs, axis=1, keepdims=True)
    similarities = query_vec @ image_norms.T

    # Top-k results
    top_indices = np.argsort(similarities)[::-1][:k]

    results = []
    for idx in top_indices:
        img = index["images"][int(idx)]
        results.append({
            "path": img["path"],
            "score": round(float(similarities[idx]), 4),
            "auto_tags": img["auto_tags"],
            "source_video": img["source_video"],
            "frame_index": img["frame_index"],
        })

    return {"query": q, "results": results}


@app.get("/visual-tags")
def visual_tags():
    """Return all images with their auto-tags for gallery filtering."""
    index = get_clip_index()

    images = []
    for img in index["images"]:
        images.append({
            "path": img["path"],
            "auto_tags": img["auto_tags"],
            "source_video": img["source_video"],
            "frame_index": img["frame_index"],
        })

    # Collect all unique tag labels with their max scores
    tag_summary: dict[str, float] = {}
    for img in index["images"]:
        for tag in img["auto_tags"]:
            label = tag["label"]
            if label not in tag_summary or tag["score"] > tag_summary[label]:
                tag_summary[label] = tag["score"]

    top_tags = sorted(tag_summary.items(), key=lambda x: -x[1])

    return {
        "images": images,
        "tags": [{"label": t[0], "max_score": round(t[1], 4)} for t in top_tags],
        "stats": index["stats"],
    }


# ── Document Upload & Ingestion ──────────────────────────────────────────

ALLOWED_EXTENSIONS = {".pdf", ".docx", ".txt", ".csv", ".json", ".md"}


def extract_text_from_pdf(file_path: str) -> str:
    """Extract text from PDF using PyMuPDF."""
    import fitz
    doc = fitz.open(file_path)
    pages = []
    for page in doc:
        pages.append(page.get_text())
    doc.close()
    return "\n\n".join(pages)


def extract_text_from_docx(file_path: str) -> str:
    """Extract text from DOCX using python-docx."""
    from docx import Document as DocxDocument
    doc = DocxDocument(file_path)
    return "\n\n".join(p.text for p in doc.paragraphs if p.text.strip())


def extract_text_from_plain(file_path: str) -> str:
    """Read plain text files (txt, csv, json, md)."""
    return Path(file_path).read_text(encoding="utf-8", errors="replace")


EXTRACTORS = {
    ".pdf": extract_text_from_pdf,
    ".docx": extract_text_from_docx,
    ".txt": extract_text_from_plain,
    ".csv": extract_text_from_plain,
    ".json": extract_text_from_plain,
    ".md": extract_text_from_plain,
}


@app.post("/upload")
async def upload_document(file: UploadFile = File(...)):
    """Upload a document, extract text, embed, and ingest into the knowledge base."""
    filename = file.filename or "unknown"
    ext = Path(filename).suffix.lower()

    if ext not in ALLOWED_EXTENSIONS:
        return {"filename": filename, "status": "error",
                "error": f"Unsupported format: {ext}. Allowed: {', '.join(sorted(ALLOWED_EXTENSIONS))}"}

    # Save to temp file
    with tempfile.NamedTemporaryFile(delete=False, suffix=ext) as tmp:
        content = await file.read()
        tmp.write(content)
        tmp_path = tmp.name

    try:
        # Extract text
        extractor = EXTRACTORS[ext]
        text = extractor(tmp_path)

        if not text.strip():
            return {"filename": filename, "status": "error", "error": "No text extracted from file"}

        # Chunk text
        from langchain_text_splitters import RecursiveCharacterTextSplitter
        splitter = RecursiveCharacterTextSplitter(chunk_size=500, chunk_overlap=50)
        chunks = splitter.split_text(text)

        if not chunks:
            return {"filename": filename, "status": "error", "error": "No chunks created"}

        # Build LangChain Documents
        from langchain_core.documents import Document
        docs = []
        for i, chunk in enumerate(chunks):
            docs.append(Document(
                page_content=chunk,
                metadata={
                    "data_type": "uploaded_document",
                    "category": "user_upload",
                    "source": filename,
                    "chunk": i + 1,
                    "total_chunks": len(chunks),
                },
            ))

        # Embed
        embedder = get_embedder()
        texts = [doc.page_content for doc in docs]
        embeddings: list[list[float]] = []
        for i in range(0, len(texts), 32):
            batch = texts[i:i + 32]
            embeddings.extend(embedder.embed(batch))

        # Upsert to Atlas
        vs = get_vs()
        count = vs.upsert_documents(docs, embeddings)

        return {
            "filename": filename,
            "status": "ok",
            "chunks": count,
            "text_length": len(text),
        }

    except Exception as e:
        return {"filename": filename, "status": "error", "error": str(e)}
    finally:
        os.unlink(tmp_path)


# ── Data API — serve MongoDB collections to frontend ─────────────────────

from pymongo import MongoClient as _MongoClient

_data_client: _MongoClient | None = None
_data_db = None

def get_data_db():
    global _data_client, _data_db
    if _data_db is None:
        _data_client = _MongoClient(os.getenv("MONGODB_URI", ""))
        _data_db = _data_client[os.getenv("MONGODB_DB", "McLaren_f1")]
    return _data_db

@app.get("/api/local/jolpica/race_results")
async def jolpica_race_results():
    db = get_data_db()
    docs = list(db["race_results"].find({}, {"_id": 0}))
    return docs

@app.get("/api/local/jolpica/driver_standings")
async def jolpica_driver_standings():
    db = get_data_db()
    races = list(db["race_results"].find({}, {"_id": 0}).sort([("season", 1), ("round", 1)]))
    # Build standings by aggregating points per driver per season
    from collections import defaultdict
    driver_data = defaultdict(lambda: {"points": 0, "wins": 0, "Driver": None, "Constructors": [], "season": ""})
    for race in races:
        season = race.get("season", "")
        for result in race.get("Results", []):
            driver = result.get("Driver", {})
            did = driver.get("driverId", "")
            key = f"{season}_{did}"
            d = driver_data[key]
            d["points"] += float(result.get("points", 0))
            if result.get("position") == "1":
                d["wins"] += 1
            d["Driver"] = driver
            d["season"] = season
            constructor = result.get("Constructor", {})
            if constructor and constructor not in d["Constructors"]:
                d["Constructors"] = [constructor]
    # Format as JolpicaDriverStanding[]
    standings = []
    for key, d in driver_data.items():
        if d["Driver"]:
            standings.append({
                "position": "0",
                "positionText": "0",
                "points": str(d["points"]),
                "wins": str(d["wins"]),
                "Driver": d["Driver"],
                "Constructors": d["Constructors"],
                "season": d["season"],
            })
    # Sort by season desc, points desc
    standings.sort(key=lambda x: (-int(x["season"] or "0"), -float(x["points"])))
    # Assign positions per season
    current_season = None
    pos = 0
    for s in standings:
        if s["season"] != current_season:
            current_season = s["season"]
            pos = 1
        s["position"] = str(pos)
        s["positionText"] = str(pos)
        pos += 1
    return standings

@app.get("/api/local/jolpica/constructor_standings")
async def jolpica_constructor_standings():
    db = get_data_db()
    races = list(db["race_results"].find({}, {"_id": 0}).sort([("season", 1), ("round", 1)]))
    from collections import defaultdict
    constructor_data = defaultdict(lambda: {"points": 0, "wins": 0, "Constructor": None, "season": ""})
    for race in races:
        season = race.get("season", "")
        for result in race.get("Results", []):
            constructor = result.get("Constructor", {})
            cid = constructor.get("constructorId", "")
            key = f"{season}_{cid}"
            d = constructor_data[key]
            d["points"] += float(result.get("points", 0))
            if result.get("position") == "1":
                d["wins"] += 1
            d["Constructor"] = constructor
            d["season"] = season
    standings = []
    for key, d in constructor_data.items():
        if d["Constructor"]:
            standings.append({
                "position": "0",
                "positionText": "0",
                "points": str(d["points"]),
                "wins": str(d["wins"]),
                "Constructor": d["Constructor"],
                "season": d["season"],
            })
    standings.sort(key=lambda x: (-int(x["season"] or "0"), -float(x["points"])))
    current_season = None
    pos = 0
    for s in standings:
        if s["season"] != current_season:
            current_season = s["season"]
            pos = 1
        s["position"] = str(pos)
        s["positionText"] = str(pos)
        pos += 1
    return standings

@app.get("/api/local/jolpica/qualifying")
async def jolpica_qualifying():
    return []

@app.get("/api/local/jolpica/circuits")
async def jolpica_circuits():
    return []

@app.get("/api/local/jolpica/pit_stops")
async def jolpica_pit_stops():
    return []

@app.get("/api/local/jolpica/lap_times")
async def jolpica_lap_times():
    return []

@app.get("/api/local/jolpica/drivers")
async def jolpica_drivers():
    return []

@app.get("/api/local/jolpica/seasons")
async def jolpica_seasons():
    return []

@app.get("/api/local/pipeline/anomaly")
async def pipeline_anomaly():
    db = get_data_db()
    snapshot = db["anomaly_scores_snapshot"].find_one({}, {"_id": 0})
    return snapshot or {}

@app.get("/api/local/pipeline/intelligence")
async def pipeline_intelligence():
    """Serve regulations, equipment, dimensions, materials from f1_knowledge."""
    db = get_data_db()
    rules = []
    equipment = []
    dimensional_data = []
    material_specs = []

    for doc in db["f1_knowledge"].find({}, {"_id": 0, "embedding": 0}):
        meta = doc.get("metadata", {})
        dt = meta.get("data_type", "")
        content = doc.get("page_content", "")

        if dt == "regulation":
            # Parse description from page_content
            desc_lines = content.split("\n")
            description = desc_lines[1] if len(desc_lines) > 1 else content
            rules.append({
                "id": meta.get("rule_id", ""),
                "category": meta.get("category", ""),
                "description": description,
                "value": None,
                "unit": None,
                "condition": None,
                "reference": None,
                "severity": meta.get("severity", "info"),
                "source_standard": meta.get("source", ""),
                "_source": meta.get("source", ""),
                "_page": meta.get("page", 0),
            })
        elif dt == "equipment":
            # Parse equipment fields from page_content
            lines = content.split("\n")
            eq_type = ""
            eq_desc = ""
            eq_location = None
            for line in lines:
                if line.startswith("Type: "):
                    eq_type = line[6:]
                elif line.startswith("Location: "):
                    eq_location = line[10:]
                elif not line.startswith("["):
                    eq_desc = line
            equipment.append({
                "tag": meta.get("tag", ""),
                "type": eq_type or meta.get("category", ""),
                "description": eq_desc,
                "kks": "",
                "specs": {},
                "location_description": eq_location,
                "_source": meta.get("source", ""),
                "_page": meta.get("page", 0),
            })
        elif dt == "dimension":
            # Parse dimensional data from page_content
            lines = content.split("\n")
            dimension_desc = lines[1] if len(lines) > 1 else ""
            value = None
            unit = ""
            for line in lines:
                if line.startswith("Value: "):
                    val_str = line[7:].strip()
                    parts = val_str.split()
                    try:
                        value = float(parts[0])
                        unit = " ".join(parts[1:]) if len(parts) > 1 else ""
                    except (ValueError, IndexError):
                        value = val_str
            dimensional_data.append({
                "component": meta.get("component", ""),
                "dimension": dimension_desc,
                "value": value,
                "unit": unit,
                "_source": meta.get("source", ""),
                "_page": meta.get("page", 0),
            })
        elif dt == "material":
            lines = content.split("\n")
            application = ""
            for line in lines:
                if line.startswith("Application: "):
                    application = line[13:]
            material_specs.append({
                "material": meta.get("material", ""),
                "application": application,
                "properties": {},
                "_source": meta.get("source", ""),
                "_page": meta.get("page", 0),
            })

    return {
        "documents": [],
        "rules": rules,
        "equipment": equipment,
        "dimensional_data": dimensional_data,
        "material_specs": material_specs,
        "stats": {
            "total_pages": 0,
            "total_rules": len(rules),
            "total_equipment": len(equipment),
            "total_dimensions": len(dimensional_data),
            "total_materials": len(material_specs),
            "total_tokens_in": 0,
            "total_tokens_out": 0,
            "total_cost_usd": 0,
            "total_latency_s": 0,
        },
    }

@app.get("/api/local/pipeline/gdino")
async def pipeline_gdino():
    return {}

@app.get("/api/local/pipeline/fused")
async def pipeline_fused():
    return {}

@app.get("/api/local/pipeline/minicpm")
async def pipeline_minicpm():
    return {}

@app.get("/api/local/pipeline/videomae")
async def pipeline_videomae():
    return {}

@app.get("/api/local/pipeline/timesformer")
async def pipeline_timesformer():
    return {}

@app.get("/api/local/openf1/{collection}")
async def openf1_data(collection: str):
    return []

def _aggregate_telemetry_summary(docs: list[dict]) -> list[dict]:
    """Aggregate raw telemetry docs into CarSummary format grouped by race."""
    from collections import defaultdict
    races = defaultdict(lambda: {
        "speeds": [], "rpms": [], "throttles": [],
        "brake_count": 0, "drs_count": 0, "total": 0,
        "compounds": set(),
    })
    for d in docs:
        race = d.get("Race", "Unknown")
        r = races[race]
        speed = d.get("Speed")
        if speed is not None:
            try:
                r["speeds"].append(float(speed))
            except (ValueError, TypeError):
                pass
        rpm = d.get("RPM")
        if rpm is not None:
            try:
                r["rpms"].append(float(rpm))
            except (ValueError, TypeError):
                pass
        throttle = d.get("Throttle")
        if throttle is not None:
            try:
                r["throttles"].append(float(throttle))
            except (ValueError, TypeError):
                pass
        brake = d.get("Brake")
        if brake is True or brake == "True" or brake == 1:
            r["brake_count"] += 1
        drs = d.get("DRS")
        try:
            if drs is not None and int(float(str(drs))) >= 10:
                r["drs_count"] += 1
        except (ValueError, TypeError):
            pass
        compound = d.get("Compound")
        if compound:
            r["compounds"].add(str(compound))
        r["total"] += 1

    summaries = []
    for race_name, r in races.items():
        if not r["speeds"]:
            continue
        n = r["total"] or 1
        # Extract short race name (remove "Grand Prix" suffix for display)
        short_name = race_name.replace(" Grand Prix", "")
        summaries.append({
            "race": short_name,
            "avgSpeed": round(sum(r["speeds"]) / len(r["speeds"]), 2) if r["speeds"] else 0,
            "topSpeed": round(max(r["speeds"]), 1) if r["speeds"] else 0,
            "avgRPM": round(sum(r["rpms"]) / len(r["rpms"])) if r["rpms"] else 0,
            "maxRPM": round(max(r["rpms"])) if r["rpms"] else 0,
            "avgThrottle": round(sum(r["throttles"]) / len(r["throttles"]), 2) if r["throttles"] else 0,
            "brakePct": round(r["brake_count"] / n * 100, 2),
            "drsPct": round(r["drs_count"] / n * 100, 2),
            "compounds": sorted(r["compounds"]),
            "samples": n,
        })
    summaries.sort(key=lambda x: x["race"])
    return summaries

@app.get("/api/local/mccar-summary/{year}/{driver}")
async def mccar_summary(year: str, driver: str):
    """Aggregate telemetry into CarSummary[] format grouped by race."""
    db = get_data_db()
    docs = list(db["telemetry"].find(
        {"Driver": driver, "Year": year},
        {"_id": 0, "Speed": 1, "RPM": 1, "Throttle": 1, "Brake": 1,
         "DRS": 1, "Compound": 1, "Race": 1}
    ))
    return _aggregate_telemetry_summary(docs)

@app.get("/api/local/mcdriver-summary/{year}/{driver}")
async def mcdriver_summary(year: str, driver: str):
    """Aggregate telemetry into RaceSummary[] format for driver biometrics."""
    db = get_data_db()
    docs = list(db["telemetry"].find(
        {"Driver": driver, "Year": year},
        {"_id": 0, "Speed": 1, "RPM": 1, "Throttle": 1, "Brake": 1,
         "DRS": 1, "Race": 1}
    ))
    from collections import defaultdict
    races = defaultdict(lambda: {"speeds": [], "rpms": [], "total": 0})
    for d in docs:
        race = d.get("Race", "Unknown")
        r = races[race]
        speed = d.get("Speed")
        if speed is not None:
            try:
                r["speeds"].append(float(speed))
            except (ValueError, TypeError):
                pass
        rpm = d.get("RPM")
        if rpm is not None:
            try:
                r["rpms"].append(float(rpm))
            except (ValueError, TypeError):
                pass
        r["total"] += 1
    summaries = []
    for race_name, r in races.items():
        if not r["speeds"]:
            continue
        short_name = race_name.replace(" Grand Prix", "")
        avg_speed = sum(r["speeds"]) / len(r["speeds"]) if r["speeds"] else 0
        # Simulate biometric data from telemetry intensity
        battle_intensity = min(100, round(avg_speed / 3.5, 1))
        summaries.append({
            "race": short_name,
            "avgHR": round(140 + battle_intensity * 0.3, 1),
            "peakHR": round(160 + battle_intensity * 0.4, 1),
            "avgTemp": 36.8,
            "battleIntensity": battle_intensity,
            "airTemp": 25.0,
            "trackTemp": 40.0,
            "samples": r["total"],
        })
    summaries.sort(key=lambda x: x["race"])
    return summaries

def _telemetry_to_csv(docs: list[dict]) -> str:
    """Convert telemetry documents to CSV string."""
    if not docs:
        return ""
    headers = ["Date", "RPM", "Speed", "nGear", "Throttle", "Brake", "DRS",
               "Source", "Time", "SessionTime", "Distance", "Driver", "Year",
               "Race", "LapNumber", "LapTime", "Compound", "TyreLife"]
    lines = [",".join(headers)]
    for d in docs:
        row = []
        for h in headers:
            val = d.get(h, "")
            # Escape commas in values
            s = str(val) if val is not None else ""
            if "," in s:
                s = f'"{s}"'
            row.append(s)
        lines.append(",".join(row))
    return "\n".join(lines)

@app.get("/api/local/mccar/{year}/{filename}")
async def mccar_csv(year: str, filename: str):
    """Serve telemetry as CSV for a specific race."""
    from starlette.responses import PlainTextResponse
    db = get_data_db()
    # filename like 2024_Abu_Dhabi_Grand_Prix_Race.csv
    source_file = filename.replace(".csv", "") + ".csv"
    docs = list(db["telemetry"].find(
        {"_source_file": source_file},
        {"_id": 0}
    ))
    return PlainTextResponse(_telemetry_to_csv(docs))

@app.get("/api/local/mcdriver/{year}/{filename}")
async def mcdriver_csv(year: str, filename: str):
    """Serve driver telemetry as CSV (same data, frontend filters by driver)."""
    from starlette.responses import PlainTextResponse
    db = get_data_db()
    source_file = filename.replace(".csv", "") + ".csv"
    docs = list(db["telemetry"].find(
        {"_source_file": source_file},
        {"_id": 0}
    ))
    return PlainTextResponse(_telemetry_to_csv(docs))

@app.get("/api/local/mcracecontext/{year}/tire_stints.csv")
async def mcracecontext_tire_stints(year: str):
    """Generate tire stints CSV from telemetry data."""
    from starlette.responses import PlainTextResponse
    db = get_data_db()
    # Aggregate stint data from telemetry
    pipeline_agg = [
        {"$match": {"Year": year}},
        {"$group": {
            "_id": {"Driver": "$Driver", "Race": "$Race", "Compound": "$Compound"},
            "start_lap": {"$min": "$LapNumber"},
            "end_lap": {"$max": "$LapNumber"},
            "tyre_life": {"$max": "$TyreLife"},
        }},
        {"$sort": {"_id.Race": 1, "_id.Driver": 1, "start_lap": 1}},
    ]
    results = list(db["telemetry"].aggregate(pipeline_agg))
    headers = ["Driver", "Race", "Compound", "StartLap", "EndLap", "TyreLife"]
    lines = [",".join(headers)]
    for r in results:
        ident = r["_id"]
        lines.append(",".join([
            str(ident.get("Driver", "")),
            str(ident.get("Race", "")),
            str(ident.get("Compound", "")),
            str(int(r.get("start_lap", 0))) if r.get("start_lap") else "0",
            str(int(r.get("end_lap", 0))) if r.get("end_lap") else "0",
            str(int(r.get("tyre_life", 0))) if r.get("tyre_life") else "0",
        ]))
    return PlainTextResponse("\n".join(lines))

@app.get("/api/local/mccsv/driver_career")
async def mccsv_driver_career():
    """Generate driver career CSV from race_results."""
    from starlette.responses import PlainTextResponse
    from collections import defaultdict
    db = get_data_db()
    races = list(db["race_results"].find(
        {"Results.Driver.code": {"$in": ["NOR", "PIA"]}},
        {"_id": 0}
    ))
    drivers = defaultdict(lambda: {
        "seasons": set(), "races": 0, "wins": 0, "podiums": 0,
        "poles": 0, "dnfs": 0, "total_points": 0, "best_finish": 99,
        "full_name": "", "nationality": "", "date_of_birth": "",
    })
    for race in races:
        for result in race.get("Results", []):
            drv = result.get("Driver", {})
            code = drv.get("code", "")
            if code not in ["NOR", "PIA"]:
                continue
            d = drivers[code]
            d["seasons"].add(race.get("season", ""))
            d["races"] += 1
            d["full_name"] = f'{drv.get("givenName", "")} {drv.get("familyName", "")}'
            d["nationality"] = drv.get("nationality", "")
            d["date_of_birth"] = drv.get("dateOfBirth", "")
            pos = result.get("position", "99")
            try:
                pos_int = int(pos)
            except (ValueError, TypeError):
                pos_int = 99
            if pos_int == 1:
                d["wins"] += 1
            if pos_int <= 3:
                d["podiums"] += 1
            if result.get("grid") == "1":
                d["poles"] += 1
            if "Finished" not in result.get("status", "Finished"):
                d["dnfs"] += 1
            d["total_points"] += float(result.get("points", 0))
            d["best_finish"] = min(d["best_finish"], pos_int)

    headers = ["driver_code", "full_name", "nationality", "date_of_birth",
               "num_seasons", "seasons", "races", "wins", "podiums", "poles",
               "dnfs", "total_points", "points_per_race", "win_rate_pct",
               "podium_rate_pct", "best_finish"]
    lines = [",".join(headers)]
    for code, d in drivers.items():
        races_n = d["races"] or 1
        lines.append(",".join([
            code, d["full_name"], d["nationality"], d["date_of_birth"],
            str(len(d["seasons"])), ";".join(sorted(d["seasons"])),
            str(d["races"]), str(d["wins"]), str(d["podiums"]), str(d["poles"]),
            str(d["dnfs"]), str(d["total_points"]),
            str(round(d["total_points"] / races_n, 2)),
            str(round(d["wins"] / races_n * 100, 2)),
            str(round(d["podiums"] / races_n * 100, 2)),
            str(d["best_finish"]),
        ]))
    return PlainTextResponse("\n".join(lines))

@app.get("/api/local/f1data/McResults/{year}/championship_drivers.csv")
async def mc_championship_drivers(year: str):
    """Generate championship drivers CSV from race_results."""
    from starlette.responses import PlainTextResponse
    from collections import defaultdict
    db = get_data_db()
    races = list(db["race_results"].find({"season": year}, {"_id": 0}))
    driver_pts = defaultdict(lambda: {"points": 0, "wins": 0, "name": "", "team": "", "nationality": ""})
    for race in races:
        for result in race.get("Results", []):
            drv = result.get("Driver", {})
            code = drv.get("code", drv.get("driverId", ""))
            d = driver_pts[code]
            d["points"] += float(result.get("points", 0))
            if result.get("position") == "1":
                d["wins"] += 1
            d["name"] = f'{drv.get("givenName", "")} {drv.get("familyName", "")}'
            d["nationality"] = drv.get("nationality", "")
            con = result.get("Constructor", {})
            if con:
                d["team"] = con.get("name", "")
    headers = ["position", "driver_code", "driver_name", "nationality", "team", "points", "wins"]
    lines = [",".join(headers)]
    sorted_drivers = sorted(driver_pts.items(), key=lambda x: -x[1]["points"])
    for pos, (code, d) in enumerate(sorted_drivers, 1):
        lines.append(",".join([str(pos), code, d["name"], d["nationality"], d["team"], str(d["points"]), str(d["wins"])]))
    return PlainTextResponse("\n".join(lines))

@app.get("/api/local/f1data/McResults/{year}/championship_teams.csv")
async def mc_championship_teams(year: str):
    """Generate championship teams CSV from race_results."""
    from starlette.responses import PlainTextResponse
    from collections import defaultdict
    db = get_data_db()
    races = list(db["race_results"].find({"season": year}, {"_id": 0}))
    team_pts = defaultdict(lambda: {"points": 0, "wins": 0, "name": ""})
    for race in races:
        for result in race.get("Results", []):
            con = result.get("Constructor", {})
            cid = con.get("constructorId", "")
            if not cid:
                continue
            t = team_pts[cid]
            t["points"] += float(result.get("points", 0))
            if result.get("position") == "1":
                t["wins"] += 1
            t["name"] = con.get("name", "")
    headers = ["position", "team_id", "team_name", "points", "wins"]
    lines = [",".join(headers)]
    sorted_teams = sorted(team_pts.items(), key=lambda x: -x[1]["points"])
    for pos, (tid, t) in enumerate(sorted_teams, 1):
        lines.append(",".join([str(pos), tid, t["name"], str(t["points"]), str(t["wins"])]))
    return PlainTextResponse("\n".join(lines))

@app.get("/api/local/f1data/McStrategy/{year}/pit_stops.csv")
async def mc_pit_stops(year: str):
    """Generate pit stops CSV — approximate from telemetry compound changes."""
    from starlette.responses import PlainTextResponse
    db = get_data_db()
    pipeline_agg = [
        {"$match": {"Year": year}},
        {"$sort": {"LapNumber": 1}},
        {"$group": {
            "_id": {"Driver": "$Driver", "Race": "$Race"},
            "compounds": {"$push": "$Compound"},
            "laps": {"$push": "$LapNumber"},
        }},
    ]
    results = list(db["telemetry"].aggregate(pipeline_agg))
    headers = ["Driver", "Race", "Lap", "Compound", "PitStopNumber"]
    lines = [",".join(headers)]
    for r in results:
        ident = r["_id"]
        compounds = r.get("compounds", [])
        laps = r.get("laps", [])
        prev_compound = None
        pit_num = 0
        for i, (comp, lap) in enumerate(zip(compounds, laps)):
            if comp != prev_compound and prev_compound is not None:
                pit_num += 1
                lines.append(",".join([
                    str(ident.get("Driver", "")),
                    str(ident.get("Race", "")),
                    str(int(lap)) if lap else "0",
                    str(comp),
                    str(pit_num),
                ]))
            prev_compound = comp
    return PlainTextResponse("\n".join(lines))

@app.get("/api/local/f1data/McResults/{year}/session_results.csv")
async def mc_session_results(year: str):
    """Generate session results CSV from race_results."""
    from starlette.responses import PlainTextResponse
    db = get_data_db()
    races = list(db["race_results"].find({"season": year}, {"_id": 0}).sort("round", 1))
    headers = ["Race", "Round", "Driver", "DriverCode", "Team", "Position", "Points", "Grid", "Status", "Laps"]
    lines = [",".join(headers)]
    for race in races:
        for result in race.get("Results", []):
            drv = result.get("Driver", {})
            con = result.get("Constructor", {})
            lines.append(",".join([
                race.get("raceName", ""),
                str(race.get("round", "")),
                f'{drv.get("givenName", "")} {drv.get("familyName", "")}',
                drv.get("code", ""),
                con.get("name", ""),
                result.get("position", ""),
                result.get("points", "0"),
                result.get("grid", ""),
                result.get("status", ""),
                result.get("laps", ""),
            ]))
    return PlainTextResponse("\n".join(lines))

@app.get("/api/local/f1data/McResults/{year}/overtakes.csv")
async def mc_overtakes(year: str):
    """Return empty overtakes CSV — no overtake data in MongoDB."""
    from starlette.responses import PlainTextResponse
    return PlainTextResponse("Driver,Race,Lap,Position,OvertakenDriver")

@app.get("/api/local/f1data/McResults/{year}/starting_grid.csv")
async def mc_starting_grid(year: str):
    """Generate starting grid CSV from race_results grid positions."""
    from starlette.responses import PlainTextResponse
    db = get_data_db()
    races = list(db["race_results"].find({"season": year}, {"_id": 0}).sort("round", 1))
    headers = ["Race", "Round", "Driver", "DriverCode", "Team", "GridPosition"]
    lines = [",".join(headers)]
    for race in races:
        for result in race.get("Results", []):
            drv = result.get("Driver", {})
            con = result.get("Constructor", {})
            lines.append(",".join([
                race.get("raceName", ""),
                str(race.get("round", "")),
                f'{drv.get("givenName", "")} {drv.get("familyName", "")}',
                drv.get("code", ""),
                con.get("name", ""),
                result.get("grid", ""),
            ]))
    return PlainTextResponse("\n".join(lines))

@app.get("/api/local/f1data/McRaceContext/{year}/tire_stints.csv")
async def mc_tire_stints(year: str):
    """Redirect to mcracecontext tire stints."""
    return await mcracecontext_tire_stints(year)

@app.get("/api/local/{path:path}")
async def local_catchall(path: str):
    """Catch-all for /api/local/ routes — return empty data."""
    if path.endswith(".csv"):
        from starlette.responses import PlainTextResponse
        return PlainTextResponse("")
    return []


# ── Run ──────────────────────────────────────────────────────────────────

if __name__ == "__main__":
    import uvicorn
    print(f"Starting F1 OmniSense API on port {PORT}")
    print(f"  Knowledge Agent: {GROQ_MODEL}")
    print(f"  3D Model Gen:   enabled")
    print(f"  Vector store:   MongoDB Atlas")
    uvicorn.run(app, host="0.0.0.0", port=PORT)
