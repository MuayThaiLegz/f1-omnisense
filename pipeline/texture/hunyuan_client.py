"""Hunyuan3D client — generates 3D GLB models via REST API on GCP VM.

Connects to a lightweight REST proxy (rest_api.py) running on the same
VM as the Hunyuan3D Gradio server. Uses short HTTP requests (POST to
submit, GET to poll, GET to download) instead of long-lived websockets,
which avoids Railway/cloud NAT connection timeouts.

REST endpoints on the VM (port 5432):
  POST /generate     — upload image, returns job_id
  GET  /jobs/{id}    — poll status
  GET  /download/{id} — download GLB file
  GET  /health       — health check
"""

import os
import time
import shutil
import logging
import requests
from pathlib import Path
from dataclasses import dataclass, field

logger = logging.getLogger(__name__)

DEFAULT_REST_URL = os.environ.get(
    "HUNYUAN_REST_URL", "http://34.48.15.70:5432"
)

# How long to wait for generation to complete (seconds)
GENERATION_TIMEOUT = 300  # 5 minutes
POLL_INTERVAL = 5  # seconds between status checks


@dataclass
class GenerationResult:
    """Result from a Hunyuan3D generation call."""
    glb_path: Path
    textured_glb_path: Path | None = None
    mesh_stats: dict = field(default_factory=dict)
    seed: int = 0
    raw: tuple = ()


class HunyuanClient:
    """REST client for the Hunyuan3D generation proxy.

    Supports two generation modes:
    - shape_generation: geometry only (faster, ~30s)
    - generation_all: geometry + texture (slower, ~60-90s)
    """

    def __init__(self, rest_url: str = DEFAULT_REST_URL):
        self.rest_url = rest_url.rstrip("/")

    def check_health(self) -> bool:
        """Test server connectivity."""
        try:
            resp = requests.get(f"{self.rest_url}/health", timeout=10)
            resp.raise_for_status()
            logger.info("Hunyuan3D REST API healthy: %s", resp.json())
            return True
        except Exception as e:
            logger.error("Hunyuan3D REST API unreachable: %s", e)
            return False

    def _submit_and_wait(self, image_path: Path, textured: bool = False,
                         output_path: Path = None, **kwargs) -> dict:
        """Submit generation job and poll until complete.

        Args:
            image_path: Local image file to upload.
            textured: Whether to generate texture (shape+texture mode).
            output_path: Where to save the downloaded GLB.
            **kwargs: Generation parameters (steps, guidance_scale, etc.)

        Returns:
            Dict with job info including local glb_path.
        """
        # 1. Submit — short POST, returns immediately
        with open(image_path, "rb") as f:
            files = {"image": (image_path.name, f, "image/png")}
            data = {
                "steps": kwargs.get("steps", 30.0),
                "guidance_scale": kwargs.get("guidance_scale", 5.0),
                "seed": kwargs.get("seed", 1234.0),
                "octree_resolution": kwargs.get("octree_resolution", 256.0),
                "remove_bg": kwargs.get("remove_bg", True),
                "num_chunks": kwargs.get("num_chunks", 8000.0),
                "randomize_seed": kwargs.get("randomize_seed", False),
                "textured": textured,
            }
            logger.info("Submitting %s to Hunyuan3D REST API (textured=%s)",
                        image_path.name, textured)
            resp = requests.post(
                f"{self.rest_url}/generate",
                files=files,
                data=data,
                timeout=30,
            )
            resp.raise_for_status()
            job = resp.json()

        job_id = job["job_id"]
        logger.info("Hunyuan3D job submitted: %s", job_id)

        # 2. Poll — short GETs every few seconds
        start = time.time()
        while time.time() - start < GENERATION_TIMEOUT:
            time.sleep(POLL_INTERVAL)
            try:
                resp = requests.get(
                    f"{self.rest_url}/jobs/{job_id}",
                    timeout=10,
                )
                resp.raise_for_status()
                status = resp.json()
            except requests.RequestException as e:
                logger.warning("Poll error (will retry): %s", e)
                continue

            if status["status"] == "completed":
                logger.info("Hunyuan3D job %s completed in %.1fs",
                            job_id, time.time() - start)

                # 3. Download GLB — short GET
                if output_path is None:
                    output_path = Path(f"/tmp/hunyuan_{job_id}.glb")
                output_path.parent.mkdir(parents=True, exist_ok=True)

                dl_resp = requests.get(
                    f"{self.rest_url}/download/{job_id}",
                    timeout=60,
                    stream=True,
                )
                dl_resp.raise_for_status()
                with open(output_path, "wb") as f:
                    for chunk in dl_resp.iter_content(chunk_size=8192):
                        f.write(chunk)

                logger.info("Downloaded GLB to %s (%d bytes)",
                            output_path, output_path.stat().st_size)

                return {
                    "glb_path": output_path,
                    "seed": status.get("seed", 0),
                    "mesh_stats": status.get("mesh_stats", {}),
                    "elapsed": status.get("elapsed", 0),
                }

            elif status["status"] == "failed":
                raise RuntimeError(
                    f"Hunyuan3D job {job_id} failed: {status.get('error', 'unknown')}"
                )

            elapsed = time.time() - start
            logger.debug("Job %s: %s (%.0fs elapsed)", job_id,
                         status["status"], elapsed)

        raise TimeoutError(
            f"Hunyuan3D job {job_id} timed out after {GENERATION_TIMEOUT}s"
        )

    def generate(self, image_path: str | Path, output_path: str | Path = None,
                 steps: int = 30, guidance_scale: float = 5.0,
                 seed: int = 1234, octree_resolution: int = 256,
                 remove_bg: bool = True, num_chunks: int = 8000,
                 randomize_seed: bool = False) -> GenerationResult:
        """Generate a 3D shape from an image (geometry only)."""
        image_path = Path(image_path)
        if not image_path.exists():
            raise FileNotFoundError(f"Input image not found: {image_path}")

        out = Path(output_path) if output_path else None
        result = self._submit_and_wait(
            image_path, textured=False, output_path=out,
            steps=float(steps), guidance_scale=float(guidance_scale),
            seed=float(seed), octree_resolution=float(octree_resolution),
            remove_bg=remove_bg, num_chunks=float(num_chunks),
            randomize_seed=randomize_seed,
        )

        return GenerationResult(
            glb_path=result["glb_path"],
            mesh_stats=result.get("mesh_stats", {}),
            seed=result.get("seed", seed),
        )

    def generate_textured(self, image_path: str | Path,
                          output_shape_path: str | Path = None,
                          output_textured_path: str | Path = None,
                          steps: int = 30, guidance_scale: float = 5.0,
                          seed: int = 1234, octree_resolution: int = 256,
                          remove_bg: bool = True, num_chunks: int = 8000,
                          randomize_seed: bool = False) -> GenerationResult:
        """Generate a textured 3D model from an image (shape + texture)."""
        image_path = Path(image_path)
        if not image_path.exists():
            raise FileNotFoundError(f"Input image not found: {image_path}")

        out = Path(output_textured_path) if output_textured_path else None
        result = self._submit_and_wait(
            image_path, textured=True, output_path=out,
            steps=float(steps), guidance_scale=float(guidance_scale),
            seed=float(seed), octree_resolution=float(octree_resolution),
            remove_bg=remove_bg, num_chunks=float(num_chunks),
            randomize_seed=randomize_seed,
        )

        return GenerationResult(
            glb_path=result["glb_path"],
            textured_glb_path=result["glb_path"],
            mesh_stats=result.get("mesh_stats", {}),
            seed=result.get("seed", seed),
        )


# Module-level singleton
_client = None


def get_hunyuan_client(rest_url: str = DEFAULT_REST_URL) -> HunyuanClient:
    """Get or create the singleton Hunyuan3D client."""
    global _client
    if _client is None:
        _client = HunyuanClient(rest_url)
    return _client


if __name__ == "__main__":
    import sys

    logging.basicConfig(level=logging.INFO, format="%(levelname)s: %(message)s")

    client = HunyuanClient()

    if len(sys.argv) < 2:
        print("=== Hunyuan3D REST API Check ===")
        print(f"Server: {DEFAULT_REST_URL}")
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
