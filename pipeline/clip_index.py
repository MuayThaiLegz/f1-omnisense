"""Build CLIP visual search index + auto-tag all McMedia images.

Embeds every image in f1data/McMedia/ using CLIP ViT-B/32 (512-dim),
then computes cosine similarity against F1 category descriptions
to generate auto-tags per image.

Usage:
    python pipeline/clip_index.py
    python pipeline/clip_index.py --top-k 5   # Top-k tags per image (default 5)
"""

from __future__ import annotations

import argparse
import json
import re
import time
from pathlib import Path

import numpy as np

MEDIA_ROOT = Path(__file__).parent.parent / "f1data" / "McMedia"
OUTPUT_PATH = MEDIA_ROOT / "clip_index.json"

# F1-specific category descriptions for auto-tagging
F1_CATEGORIES = [
    "formula one car on track racing at high speed",
    "pit stop crew changing tires on a formula one car",
    "front wing close-up of a formula one car",
    "rain racing conditions on a wet formula one circuit",
    "cockpit dashboard view from inside formula one car",
    "tire compound rubber close-up",
    "rear wing and DRS mechanism on formula one car",
    "safety car leading the formula one field",
    "crash or collision between formula one cars",
    "podium celebration after a formula one race",
    "steering wheel controls inside formula one cockpit",
    "driver helmet visor close-up",
    "brake glow on formula one car during braking",
    "aerodynamic flow visualization on car body",
    "race start grid with multiple formula one cars",
    "overtaking maneuver between two formula one cars",
    "pit lane entry with formula one car",
    "exhaust and engine of a formula one power unit",
    "suspension geometry of a formula one car",
    "track surface kerbing and curbing on circuit",
]


def find_images() -> list[Path]:
    """Find all JPG/PNG images in McMedia subdirectories."""
    images = []
    for subdir in sorted(MEDIA_ROOT.iterdir()):
        if not subdir.is_dir():
            continue
        for img in sorted(subdir.glob("*.jpg")):
            if img.name.startswith("_temp"):
                continue
            images.append(img)
        for img in sorted(subdir.glob("*.png")):
            images.append(img)
    return images


def parse_image_meta(img_path: Path) -> dict:
    """Extract source video and frame index from filename."""
    name = img_path.stem
    # Pattern: {video_name}_frame{N}
    match = re.match(r"(.+)_frame(\d+)$", name)
    if match:
        return {"source_video": match.group(1), "frame_index": int(match.group(2))}
    return {"source_video": name, "frame_index": 0}


def cosine_similarity(a: np.ndarray, b: np.ndarray) -> np.ndarray:
    """Compute cosine similarity between vector a and matrix b."""
    a_norm = a / np.linalg.norm(a)
    b_norm = b / np.linalg.norm(b, axis=1, keepdims=True)
    return a_norm @ b_norm.T


def build_index(top_k: int = 5):
    """Main indexing pipeline."""
    import sys
    sys.path.insert(0, str(Path(__file__).parent.parent))

    from pipeline.embeddings import CLIPEmbedder

    print("=" * 60)
    print("  CLIP Visual Search Index Builder")
    print("=" * 60)

    # Find images
    images = find_images()
    print(f"\n  Found {len(images)} images in {MEDIA_ROOT.name}/")
    for img in images[:5]:
        print(f"    {img.relative_to(MEDIA_ROOT)}")
    if len(images) > 5:
        print(f"    ... and {len(images) - 5} more")

    if not images:
        print("\n  ERROR: No images found.")
        return

    # Init CLIP
    print("\n[1/3] Loading CLIP ViT-B/32...")
    t0 = time.time()
    clip = CLIPEmbedder()
    load_time = time.time() - t0
    print(f"  Model loaded in {load_time:.1f}s (device: {clip.device})")

    # Embed images
    print(f"\n[2/3] Embedding {len(images)} images...")
    t0 = time.time()
    image_embeddings = clip.embed_images(images)
    embed_time = time.time() - t0
    print(f"  Embedded in {embed_time:.1f}s ({len(images) / embed_time:.1f} img/sec)")

    image_vecs = np.array(image_embeddings)

    # Embed category descriptions
    print(f"\n[3/3] Auto-tagging against {len(F1_CATEGORIES)} F1 categories...")
    t0 = time.time()
    category_embeddings = clip.embed_texts(F1_CATEGORIES)
    cat_vecs = np.array(category_embeddings)

    # Compute similarities and build index
    index_data = {
        "images": [],
        "categories": F1_CATEGORIES,
        "category_embeddings": [v.tolist() for v in cat_vecs],
        "stats": {
            "total_images": len(images),
            "total_categories": len(F1_CATEGORIES),
            "embed_time_s": round(embed_time, 1),
            "model": "ViT-B-32",
            "embedding_dim": 512,
        },
    }

    for i, img_path in enumerate(images):
        # Cosine similarity against all categories
        sims = cosine_similarity(image_vecs[i], cat_vecs)
        # Flatten — could be (1, N) or (N,) depending on shapes
        sims = np.asarray(sims).flatten()

        # Top-k tags
        top_indices = np.argsort(sims)[::-1][:top_k]
        auto_tags = [
            {"label": F1_CATEGORIES[idx], "score": round(float(sims[idx]), 4)}
            for idx in top_indices
        ]

        meta = parse_image_meta(img_path)
        rel_path = str(img_path.relative_to(MEDIA_ROOT))

        index_data["images"].append({
            "path": rel_path,
            "embedding": image_vecs[i].tolist(),
            "auto_tags": auto_tags,
            "source_video": meta["source_video"],
            "frame_index": meta["frame_index"],
        })

    tag_time = time.time() - t0
    print(f"  Tagged in {tag_time:.1f}s")

    # Save
    with open(OUTPUT_PATH, "w") as f:
        json.dump(index_data, f)

    file_size = OUTPUT_PATH.stat().st_size / 1024
    print(f"\n  Output: {OUTPUT_PATH} ({file_size:.0f} KB)")
    print(f"  Images indexed: {len(index_data['images'])}")
    print(f"  Categories: {len(F1_CATEGORIES)}")

    # Print sample tags
    print(f"\n  Sample auto-tags:")
    for entry in index_data["images"][:3]:
        print(f"    {entry['path']}:")
        for tag in entry["auto_tags"][:3]:
            print(f"      {tag['score']:.3f}  {tag['label']}")

    print(f"\n  Done! CLIP index ready for visual search.")


if __name__ == "__main__":
    parser = argparse.ArgumentParser(description="Build CLIP visual search index")
    parser.add_argument("--top-k", type=int, default=5, help="Top-k tags per image")
    args = parser.parse_args()
    build_index(top_k=args.top_k)
