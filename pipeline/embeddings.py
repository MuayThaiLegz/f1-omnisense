"""Dual embedding pipeline: CLIP ViT-B/32 (image+text) + nomic-embed-text (text).

CLIP: Encodes both images and text into the same 512-dim vector space,
      enabling cross-modal search (find diagrams by text description).

Nomic: 768-dim text embeddings via sentence-transformers (HuggingFace),
       optimized for document search. No Ollama dependency.

Usage:
    from pipeline.embeddings import EmbeddingEngine

    engine = EmbeddingEngine()

    # Text embeddings (nomic)
    vecs = engine.embed_texts(["pipe routing algorithm", "ASME B31.1"])

    # Image embeddings (CLIP)
    vecs = engine.embed_images([Path("page1.png"), Path("diagram.png")])

    # Cross-modal: text query against image index
    query_vec = engine.embed_text_for_image_search("pump discharge nozzle")
"""

from __future__ import annotations

import json
from pathlib import Path


# ── Nomic text embeddings (via sentence-transformers / HuggingFace) ────

class NomicEmbedder:
    """Text embeddings using nomic-embed-text-v1.5 from HuggingFace.

    768-dim vectors, optimized for document/technical search.
    Downloads model on first use (~270MB), cached in ~/.cache/huggingface/.
    """

    MODEL_NAME = "nomic-ai/nomic-embed-text-v1.5"

    def __init__(self):
        try:
            from sentence_transformers import SentenceTransformer
        except ImportError:
            raise ValueError(
                "sentence-transformers not installed. Run:\n"
                "  pip install sentence-transformers"
            )
        self._model = SentenceTransformer(self.MODEL_NAME, trust_remote_code=True)
        print(f"  Nomic {self.MODEL_NAME}: loaded ({self._model.device})")

    def embed(self, texts: list[str]) -> list[list[float]]:
        """Embed a list of text strings into 768-dim vectors."""
        # nomic-embed-text expects "search_document: " or "search_query: " prefix
        # For general embedding, use "search_document: " prefix
        prefixed = [f"search_document: {t}" for t in texts]
        embeddings = self._model.encode(prefixed, normalize_embeddings=True)
        return embeddings.tolist()

    def embed_query(self, text: str) -> list[float]:
        """Embed a search query (uses 'search_query:' prefix for better retrieval)."""
        embeddings = self._model.encode(
            [f"search_query: {text}"], normalize_embeddings=True
        )
        return embeddings[0].tolist()

    def embed_one(self, text: str) -> list[float]:
        """Embed a single text string."""
        return self.embed([text])[0]


# ── CLIP image+text embeddings ──────────────────────────────────────────

class CLIPEmbedder:
    """Cross-modal embeddings using CLIP ViT-B/32.

    512-dim vectors for both images and text in the same space.
    Enables: "find me a diagram showing pump connections" → matching images.
    """

    def __init__(self):
        try:
            import open_clip
            import torch
            from PIL import Image
        except ImportError:
            raise ValueError(
                "CLIP dependencies not installed. Run:\n"
                "  pip install open-clip-torch pillow"
            )

        self._torch = torch
        self._Image = Image

        self.device = "cuda" if torch.cuda.is_available() else "cpu"
        self.model, _, self.preprocess = open_clip.create_model_and_transforms(
            "ViT-B-32", pretrained="laion2b_s34b_b79k"
        )
        self.tokenizer = open_clip.get_tokenizer("ViT-B-32")
        self.model = self.model.to(self.device).eval()

    def embed_images(self, image_paths: list[Path]) -> list[list[float]]:
        """Embed images into 512-dim CLIP vectors."""
        import torch

        vectors = []
        for path in image_paths:
            img = self._Image.open(path).convert("RGB")
            img_tensor = self.preprocess(img).unsqueeze(0).to(self.device)

            with torch.no_grad():
                features = self.model.encode_image(img_tensor)
                features = features / features.norm(dim=-1, keepdim=True)
                vectors.append(features[0].cpu().tolist())

        return vectors

    def embed_texts(self, texts: list[str]) -> list[list[float]]:
        """Embed text into 512-dim CLIP vectors (same space as images)."""
        import torch

        tokens = self.tokenizer(texts).to(self.device)
        with torch.no_grad():
            features = self.model.encode_text(tokens)
            features = features / features.norm(dim=-1, keepdim=True)

        return features.cpu().tolist()

    def embed_image(self, path: Path) -> list[float]:
        """Embed a single image."""
        return self.embed_images([path])[0]

    def embed_text(self, text: str) -> list[float]:
        """Embed a single text string (for image search)."""
        return self.embed_texts([text])[0]


# ── Unified engine ──────────────────────────────────────────────────────

class EmbeddingEngine:
    """Dual embedding engine combining nomic (text) + CLIP (image+text).

    Usage:
        engine = EmbeddingEngine()

        # Pure text search (nomic, 768-dim)
        vecs = engine.embed_texts(["equipment list", "piping class A1A"])

        # Image indexing (CLIP, 512-dim)
        vecs = engine.embed_images([Path("page1.png")])

        # Cross-modal text→image query (CLIP, 512-dim)
        query = engine.embed_text_for_image_search("pump nozzle diagram")
    """

    def __init__(self, enable_clip: bool = True, enable_nomic: bool = True):
        self.nomic = None
        self.clip = None

        if enable_nomic:
            try:
                self.nomic = NomicEmbedder()
                print("  Nomic-embed-text: ready (768-dim, text)")
            except (ValueError, Exception) as e:
                print(f"  Nomic: skipped ({e})")

        if enable_clip:
            try:
                self.clip = CLIPEmbedder()
                print(f"  CLIP ViT-B/32: ready (512-dim, image+text, {self.clip.device})")
            except (ValueError, Exception) as e:
                print(f"  CLIP: skipped ({e})")

    # ── Text embeddings (nomic) ─────────────────────────────────────────

    def embed_texts(self, texts: list[str]) -> list[list[float]]:
        """Embed texts using nomic (768-dim). For document chunk search."""
        if not self.nomic:
            raise RuntimeError("Nomic embedder not available")
        return self.nomic.embed(texts)

    def embed_text(self, text: str) -> list[float]:
        """Embed single text using nomic."""
        return self.embed_texts([text])[0]

    # ── Image embeddings (CLIP) ─────────────────────────────────────────

    def embed_images(self, paths: list[Path]) -> list[list[float]]:
        """Embed images using CLIP (512-dim). For visual indexing."""
        if not self.clip:
            raise RuntimeError("CLIP embedder not available")
        return self.clip.embed_images(paths)

    def embed_image(self, path: Path) -> list[float]:
        """Embed single image using CLIP."""
        return self.embed_images([path])[0]

    # ── Cross-modal (CLIP text for image search) ────────────────────────

    def embed_text_for_image_search(self, text: str) -> list[float]:
        """Embed text into CLIP space (512-dim) for image retrieval."""
        if not self.clip:
            raise RuntimeError("CLIP embedder not available")
        return self.clip.embed_text(text)

    def embed_texts_for_image_search(self, texts: list[str]) -> list[list[float]]:
        """Embed multiple texts into CLIP space for image retrieval."""
        if not self.clip:
            raise RuntimeError("CLIP embedder not available")
        return self.clip.embed_texts(texts)
