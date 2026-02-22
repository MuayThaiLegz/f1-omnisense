"""Multi-model PDF extraction pipeline with consensus scoring.

Domain-agnostic extraction engine for any PDF corpus.

Core components:
  - renderer: PDF → multi-view images (full page, quadrants, zoom regions)
  - models: Ollama (edge) + Groq (cloud) vision clients
  - passes: Multi-pass extraction definitions (configurable per domain)
  - merge: Cross-model consensus merging
  - tracker: Master document tracker with export
  - embeddings: CLIP (image+text) + Nomic (text) dual embeddings
"""

from .embeddings import EmbeddingEngine
from .models import MultiModelAnalyzer, GroqVisionClient, GemmaClient, QwenVLClient
from .renderer import render_pdf, PageViews
from .tracker import MasterTracker

__all__ = [
    "EmbeddingEngine",
    "MultiModelAnalyzer",
    "GroqVisionClient",
    "GemmaClient",
    "QwenVLClient",
    "render_pdf",
    "PageViews",
    "MasterTracker",
]
