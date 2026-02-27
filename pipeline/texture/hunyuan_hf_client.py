"""Hunyuan3D client — generates 3D GLB models via HuggingFace Spaces.

Calls the official tencent/Hunyuan3D-2.1 Gradio Space on HuggingFace.
No local GPU required — HF provides the compute for free (queued).

Provides the same GenerationResult interface as hunyuan_client.py (GCP)
so the rest of the pipeline can use either interchangeably.
"""

import os
import shutil
import logging
import time
from pathlib import Path
from dataclasses import dataclass, field

logger = logging.getLogger(__name__)

DEFAULT_HF_SPACE = os.environ.get(
    "HUNYUAN_HF_SPACE", "tencent/Hunyuan3D-2.1"
)


@dataclass
class GenerationResult:
    """Result from a Hunyuan3D generation call."""
    glb_path: Path
    textured_glb_path: Path | None = None
    mesh_stats: dict = field(default_factory=dict)
    seed: int = 0
    raw: tuple = ()


def _extract_path(val) -> str:
    """Extract file path from Gradio result which may be a dict or string."""
    if isinstance(val, dict):
        return val.get("value", val.get("path", str(val)))
    return str(val)


class HunyuanHFClient:
    """HuggingFace Spaces client for Hunyuan3D generation.

    Supports two generation modes:
    - shape_generation: geometry only (faster)
    - generation_all: geometry + texture (slower)
    """

    def __init__(self, hf_space: str = DEFAULT_HF_SPACE):
        self.hf_space = hf_space
        self._client = None

    def _get_client(self):
        """Lazy-init the Gradio client."""
        if self._client is None:
            from gradio_client import Client
            logger.info("Connecting to HuggingFace Space: %s", self.hf_space)
            self._client = Client(self.hf_space)
        return self._client

    def check_health(self) -> bool:
        """Test HF Space connectivity."""
        try:
            client = self._get_client()
            logger.info("HuggingFace Space connected: %s", self.hf_space)
            return True
        except Exception as e:
            logger.error("HuggingFace Space unreachable: %s", e)
            return False

    def generate(self, image_path: str | Path, output_path: str | Path = None,
                 steps: int = 30, guidance_scale: float = 5.0,
                 seed: int = 1234, octree_resolution: int = 256,
                 remove_bg: bool = True, num_chunks: int = 8000,
                 randomize_seed: bool = False) -> GenerationResult:
        """Generate a 3D shape from an image (geometry only)."""
        from gradio_client import handle_file

        image_path = Path(image_path)
        if not image_path.exists():
            raise FileNotFoundError(f"Input image not found: {image_path}")

        client = self._get_client()
        start = time.time()

        logger.info("Submitting shape generation to %s", self.hf_space)
        result = client.predict(
            image=handle_file(str(image_path)),
            steps=int(steps),
            guidance_scale=float(guidance_scale),
            seed=int(seed),
            octree_resolution=int(octree_resolution),
            check_box_rembg=remove_bg,
            num_chunks=int(num_chunks),
            randomize_seed=randomize_seed,
            api_name="/shape_generation",
        )

        logger.info("Shape generation completed in %.1fs, result types: %s",
                     time.time() - start,
                     [type(r).__name__ for r in result])

        # Extract GLB path from result
        glb_src = _extract_path(result[0])
        stats = result[2] if len(result) > 2 else {}
        out_seed = int(float(str(result[3]))) if len(result) > 3 else seed

        # Copy to output path
        if output_path is None:
            output_path = Path(f"/tmp/hunyuan_hf_{int(time.time())}.glb")
        output_path = Path(output_path)
        output_path.parent.mkdir(parents=True, exist_ok=True)
        shutil.copy2(glb_src, str(output_path))

        logger.info("Saved GLB to %s (%d bytes)",
                     output_path, output_path.stat().st_size)

        return GenerationResult(
            glb_path=output_path,
            mesh_stats=stats if isinstance(stats, dict) else {},
            seed=out_seed,
            raw=result,
        )

    def generate_textured(self, image_path: str | Path,
                          output_shape_path: str | Path = None,
                          output_textured_path: str | Path = None,
                          steps: int = 30, guidance_scale: float = 5.0,
                          seed: int = 1234, octree_resolution: int = 256,
                          remove_bg: bool = True, num_chunks: int = 8000,
                          randomize_seed: bool = False) -> GenerationResult:
        """Generate a textured 3D model from an image (shape + texture)."""
        from gradio_client import handle_file

        image_path = Path(image_path)
        if not image_path.exists():
            raise FileNotFoundError(f"Input image not found: {image_path}")

        client = self._get_client()
        start = time.time()

        logger.info("Submitting textured generation to %s", self.hf_space)
        result = client.predict(
            image=handle_file(str(image_path)),
            steps=int(steps),
            guidance_scale=float(guidance_scale),
            seed=int(seed),
            octree_resolution=int(octree_resolution),
            check_box_rembg=remove_bg,
            num_chunks=int(num_chunks),
            randomize_seed=randomize_seed,
            api_name="/generation_all",
        )

        logger.info("Textured generation completed in %.1fs", time.time() - start)

        # Extract paths from result
        shape_src = _extract_path(result[0])
        textured_src = _extract_path(result[1])
        stats = result[3] if len(result) > 3 else {}
        out_seed = int(float(str(result[4]))) if len(result) > 4 else seed

        # Copy to output paths
        ts = int(time.time())
        if output_shape_path is None:
            output_shape_path = Path(f"/tmp/hunyuan_hf_{ts}_shape.glb")
        if output_textured_path is None:
            output_textured_path = Path(f"/tmp/hunyuan_hf_{ts}_textured.glb")

        output_shape_path = Path(output_shape_path)
        output_textured_path = Path(output_textured_path)
        output_shape_path.parent.mkdir(parents=True, exist_ok=True)
        output_textured_path.parent.mkdir(parents=True, exist_ok=True)

        shutil.copy2(shape_src, str(output_shape_path))
        shutil.copy2(textured_src, str(output_textured_path))

        logger.info("Saved shape to %s, textured to %s",
                     output_shape_path, output_textured_path)

        return GenerationResult(
            glb_path=output_shape_path,
            textured_glb_path=output_textured_path,
            mesh_stats=stats if isinstance(stats, dict) else {},
            seed=out_seed,
            raw=result,
        )


# Module-level singleton
_client = None


def get_hunyuan_hf_client(hf_space: str = DEFAULT_HF_SPACE) -> HunyuanHFClient:
    """Get or create the singleton HuggingFace Hunyuan3D client."""
    global _client
    if _client is None:
        _client = HunyuanHFClient(hf_space)
    return _client


if __name__ == "__main__":
    import sys

    logging.basicConfig(level=logging.INFO, format="%(levelname)s: %(message)s")

    client = HunyuanHFClient()

    if len(sys.argv) < 2:
        print("=== Hunyuan3D HuggingFace Space Check ===")
        print(f"Space: {DEFAULT_HF_SPACE}")
        healthy = client.check_health()
        print(f"Health: {'OK' if healthy else 'UNREACHABLE'}")
        sys.exit(0)

    image_path = sys.argv[1]
    output_path = sys.argv[2] if len(sys.argv) > 2 else "output.glb"
    mode = sys.argv[3] if len(sys.argv) > 3 else "shape"

    if mode == "textured":
        result = client.generate_textured(
            image_path,
            output_shape_path=output_path.replace(".glb", "_shape.glb"),
            output_textured_path=output_path,
        )
        print(f"Shape: {result.glb_path}")
        print(f"Textured: {result.textured_glb_path}")
    else:
        result = client.generate(image_path, output_path)
        print(f"Generated: {result.glb_path}")

    print(f"Mesh stats: {result.mesh_stats}")
    print(f"Seed: {result.seed}")
