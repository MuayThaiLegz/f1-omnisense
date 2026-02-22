"""Hunyuan3D client — generates 3D GLB models from images via Gradio API.

Connects to a Hunyuan3D server running at a Gradio endpoint.
Primary generation pathway for image-to-3D conversion.

Available endpoints (discovered via view_api()):
  /shape_generation  — shape only → (file, html, mesh_stats, seed)
  /generation_all    — shape + texture → (file, file, html, mesh_stats, seed)
  /on_export_click   — re-export as glb/obj/ply/stl with face reduction
"""

import shutil
import logging
from pathlib import Path
from dataclasses import dataclass, field

logger = logging.getLogger(__name__)

DEFAULT_API_URL = "http://34.86.192.78:8080/"


@dataclass
class GenerationResult:
    """Result from a Hunyuan3D generation call."""
    glb_path: Path
    textured_glb_path: Path | None = None
    mesh_stats: dict = field(default_factory=dict)
    seed: int = 0
    raw: tuple = ()


class HunyuanClient:
    """Wrapper around the Hunyuan3D Gradio server.

    Supports two generation modes:
    - shape_generation: geometry only (faster, ~30s)
    - generation_all: geometry + texture (slower, ~60-90s)
    """

    def __init__(self, api_url: str = DEFAULT_API_URL):
        self.api_url = api_url
        self._client = None

    def _get_client(self):
        """Lazy-init Gradio client (matches RAG singleton pattern)."""
        if self._client is None:
            from gradio_client import Client
            logger.info("Connecting to Hunyuan3D at %s", self.api_url)
            self._client = Client(self.api_url)
        return self._client

    def view_api(self) -> str:
        """Discover available API endpoints on the server."""
        client = self._get_client()
        return client.view_api(return_format="str")

    def check_health(self) -> bool:
        """Test server connectivity."""
        try:
            self._get_client()
            return True
        except Exception as e:
            logger.error("Hunyuan3D server unreachable: %s", e)
            return False

    def generate(self, image_path: str | Path, output_path: str | Path = None,
                 steps: int = 30, guidance_scale: float = 5.0,
                 seed: int = 1234, octree_resolution: int = 256,
                 remove_bg: bool = True, num_chunks: int = 8000,
                 randomize_seed: bool = False) -> GenerationResult:
        """Generate a 3D shape from an image (geometry only).

        Calls /shape_generation → returns (file, html, mesh_stats, seed).

        Args:
            image_path: Path to input PNG/JPG image.
            output_path: Where to save the GLB. If None, returns the temp path.
            steps: Inference steps (1-100). Default 30 (Turbo mode).
            guidance_scale: CFG guidance scale. Default 5.0.
            seed: Random seed (0-10M). Default 1234.
            octree_resolution: Mesh resolution 16-512. Default 256 (Standard).
            remove_bg: Remove background from input image. Default True.
            num_chunks: Processing chunks. Default 8000.
            randomize_seed: Randomize seed each call. Default False.

        Returns:
            GenerationResult with GLB path and metadata.
        """
        from gradio_client import handle_file

        image_path = Path(image_path)
        if not image_path.exists():
            raise FileNotFoundError(f"Input image not found: {image_path}")

        client = self._get_client()
        logger.info("Submitting %s to Hunyuan3D /shape_generation", image_path.name)

        result = client.predict(
            image=handle_file(str(image_path)),
            steps=float(steps),
            guidance_scale=float(guidance_scale),
            seed=float(seed),
            octree_resolution=float(octree_resolution),
            check_box_rembg=remove_bg,
            num_chunks=float(num_chunks),
            randomize_seed=randomize_seed,
            api_name="/shape_generation",
        )

        # Result: (file_path, html_str, mesh_stats_json, seed_float)
        glb_source = Path(result[0])
        mesh_stats = result[2] if len(result) > 2 else {}
        out_seed = int(result[3]) if len(result) > 3 else seed

        logger.info("Hunyuan3D shape returned: %s (seed=%d)", glb_source.name, out_seed)

        if output_path:
            output_path = Path(output_path)
            output_path.parent.mkdir(parents=True, exist_ok=True)
            shutil.copy2(str(glb_source), str(output_path))
            glb_source = output_path

        return GenerationResult(
            glb_path=glb_source,
            mesh_stats=mesh_stats if isinstance(mesh_stats, dict) else {},
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
        """Generate a textured 3D model from an image (shape + texture).

        Calls /generation_all → returns (file, file, html, mesh_stats, seed).
        Returns TWO GLBs: untextured shape and textured version.

        Args:
            image_path: Path to input PNG/JPG image.
            output_shape_path: Where to save the shape GLB.
            output_textured_path: Where to save the textured GLB.
            (other params same as generate())

        Returns:
            GenerationResult with both GLB paths.
        """
        from gradio_client import handle_file

        image_path = Path(image_path)
        if not image_path.exists():
            raise FileNotFoundError(f"Input image not found: {image_path}")

        client = self._get_client()
        logger.info("Submitting %s to Hunyuan3D /generation_all", image_path.name)

        result = client.predict(
            image=handle_file(str(image_path)),
            steps=float(steps),
            guidance_scale=float(guidance_scale),
            seed=float(seed),
            octree_resolution=float(octree_resolution),
            check_box_rembg=remove_bg,
            num_chunks=float(num_chunks),
            randomize_seed=randomize_seed,
            api_name="/generation_all",
        )

        # Result: (shape_file, textured_file, html_str, mesh_stats, seed)
        shape_source = Path(result[0])
        textured_source = Path(result[1])
        mesh_stats = result[3] if len(result) > 3 else {}
        out_seed = int(result[4]) if len(result) > 4 else seed

        logger.info("Hunyuan3D textured returned: shape=%s, textured=%s (seed=%d)",
                     shape_source.name, textured_source.name, out_seed)

        if output_shape_path:
            output_shape_path = Path(output_shape_path)
            output_shape_path.parent.mkdir(parents=True, exist_ok=True)
            shutil.copy2(str(shape_source), str(output_shape_path))
            shape_source = output_shape_path

        if output_textured_path:
            output_textured_path = Path(output_textured_path)
            output_textured_path.parent.mkdir(parents=True, exist_ok=True)
            shutil.copy2(str(textured_source), str(output_textured_path))
            textured_source = output_textured_path

        return GenerationResult(
            glb_path=shape_source,
            textured_glb_path=textured_source,
            mesh_stats=mesh_stats if isinstance(mesh_stats, dict) else {},
            seed=out_seed,
            raw=result,
        )


# Module-level singleton
_client = None


def get_hunyuan_client(api_url: str = DEFAULT_API_URL) -> HunyuanClient:
    """Get or create the singleton Hunyuan3D client."""
    global _client
    if _client is None:
        _client = HunyuanClient(api_url)
    return _client


if __name__ == "__main__":
    import sys

    logging.basicConfig(level=logging.INFO, format="%(levelname)s: %(message)s")

    client = HunyuanClient()

    if len(sys.argv) < 2:
        # Discovery mode — show available endpoints
        print("=== Hunyuan3D API Discovery ===")
        print(f"Server: {DEFAULT_API_URL}")
        healthy = client.check_health()
        print(f"Health: {'OK' if healthy else 'UNREACHABLE'}")
        if healthy:
            print("\n--- Available Endpoints ---")
            print(client.view_api())
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
