"""TRELLIS client — generates 3D GLB models from images via fal.ai API.

Uses Microsoft TRELLIS 2 (4B params) hosted on fal.ai for
image-to-3D conversion with PBR textures.

Pricing: ~$0.02 per generation on fal.ai.
API docs: https://fal.ai/models/fal-ai/trellis-2/api
"""

import os
import time
import base64
import logging
import requests
from pathlib import Path

logger = logging.getLogger(__name__)

FAL_BASE_URL = "https://queue.fal.run/fal-ai/trellis-2"


class TrellisClient:
    """Wrapper around TRELLIS 2 via fal.ai queue API."""

    def __init__(self, api_key: str = None):
        self.api_key = api_key or os.getenv("FAL_KEY", "")
        if not self.api_key:
            raise ValueError(
                "fal.ai API key not set. Add FAL_KEY to .env file. "
                "Get one at https://fal.ai/dashboard/keys"
            )
        self._headers = {
            "Authorization": f"Key {self.api_key}",
            "Content-Type": "application/json",
        }

    def create_task(self, image_path: str | Path,
                    ss_sampling_steps: int = 12,
                    ss_guidance_strength: float = 7.5,
                    slat_sampling_steps: int = 12,
                    slat_guidance_strength: float = 3.0,
                    mesh_simplify: float = 0.95,
                    texture_size: int = 1024,
                    seed: int = 42) -> str:
        """Submit an image-to-3D task to the fal.ai queue.

        Args:
            image_path: Path to input image.
            ss_sampling_steps: Structured latent sampling steps.
            ss_guidance_strength: Structured latent guidance.
            slat_sampling_steps: SLAT sampling steps.
            slat_guidance_strength: SLAT guidance.
            mesh_simplify: Mesh simplification ratio (0-1). 0.95 = simplify 95%.
            texture_size: Texture resolution (512, 1024, 2048).
            seed: Random seed.

        Returns:
            Request ID string.
        """
        image_path = Path(image_path)
        if not image_path.exists():
            raise FileNotFoundError(f"Input image not found: {image_path}")

        # Encode image as data URI
        suffix = image_path.suffix.lower().lstrip(".")
        mime = {"png": "image/png", "jpg": "image/jpeg", "jpeg": "image/jpeg",
                "webp": "image/webp"}.get(suffix, "image/png")
        with open(image_path, "rb") as f:
            b64 = base64.b64encode(f.read()).decode()
        data_uri = f"data:{mime};base64,{b64}"

        payload = {
            "image_url": data_uri,
            "ss_sampling_steps": ss_sampling_steps,
            "ss_guidance_strength": ss_guidance_strength,
            "slat_sampling_steps": slat_sampling_steps,
            "slat_guidance_strength": slat_guidance_strength,
            "mesh_simplify": mesh_simplify,
            "texture_size": texture_size,
            "seed": seed,
        }

        logger.info("Submitting %s to TRELLIS via fal.ai", image_path.name)
        resp = requests.post(
            FAL_BASE_URL,
            headers=self._headers,
            json=payload,
            timeout=30,
        )
        resp.raise_for_status()
        data = resp.json()
        request_id = data.get("request_id", "")
        logger.info("TRELLIS task submitted: %s", request_id)
        return request_id

    def poll_task(self, request_id: str) -> dict:
        """Poll task status.

        Returns dict with status and response_url when complete.
        """
        resp = requests.get(
            f"{FAL_BASE_URL}/requests/{request_id}/status",
            headers=self._headers,
            timeout=15,
        )
        resp.raise_for_status()
        return resp.json()

    def get_result(self, request_id: str) -> dict:
        """Get the completed task result."""
        resp = requests.get(
            f"{FAL_BASE_URL}/requests/{request_id}",
            headers=self._headers,
            timeout=30,
        )
        resp.raise_for_status()
        return resp.json()

    def download_glb(self, model_url: str, output_path: str | Path) -> Path:
        """Download GLB file from fal.ai CDN."""
        output_path = Path(output_path)
        output_path.parent.mkdir(parents=True, exist_ok=True)

        logger.info("Downloading GLB from TRELLIS/fal.ai...")
        resp = requests.get(model_url, timeout=120, stream=True)
        resp.raise_for_status()

        with open(output_path, "wb") as f:
            for chunk in resp.iter_content(chunk_size=8192):
                f.write(chunk)

        size_mb = output_path.stat().st_size / (1024 * 1024)
        logger.info("Saved GLB (%.1f MB) to %s", size_mb, output_path)
        return output_path

    def wait_and_download(self, request_id: str, output_path: str | Path,
                          timeout: int = 600, poll_interval: int = 5,
                          progress_callback=None) -> Path:
        """Poll until completion, then download the GLB.

        Args:
            request_id: fal.ai request ID.
            output_path: Where to save the GLB.
            timeout: Max wait time in seconds.
            poll_interval: Seconds between polls.
            progress_callback: Optional fn(status, progress_pct).

        Returns:
            Path to downloaded GLB.
        """
        start = time.time()

        while time.time() - start < timeout:
            status_data = self.poll_task(request_id)
            status = status_data.get("status", "unknown")

            # Map fal.ai statuses to progress
            progress_map = {"IN_QUEUE": 10, "IN_PROGRESS": 50, "COMPLETED": 100}
            progress = progress_map.get(status, 0)

            logger.info("TRELLIS task %s: %s", request_id[:12], status)
            if progress_callback:
                progress_callback(status, progress)

            if status == "COMPLETED":
                result = self.get_result(request_id)
                # fal.ai TRELLIS returns model in result.glb_file or result.model
                # fal.ai TRELLIS returns GLB in various keys
                glb_url = None
                for key in ("model_glb", "glb_file", "model", "glb", "mesh", "output"):
                    val = result.get(key)
                    if isinstance(val, dict) and "url" in val:
                        glb_url = val["url"]
                        break
                    elif isinstance(val, str) and val.startswith("http"):
                        glb_url = val
                        break
                if not glb_url:
                    raise RuntimeError(
                        f"TRELLIS task succeeded but no GLB URL found. "
                        f"Result keys: {list(result.keys())}"
                    )
                return self.download_glb(glb_url, output_path)

            if status == "FAILED":
                error = status_data.get("error", "Unknown error")
                raise RuntimeError(f"TRELLIS task failed: {error}")

            time.sleep(poll_interval)

        raise TimeoutError(f"TRELLIS task {request_id} timed out after {timeout}s")


# Module-level singleton
_client = None


def get_trellis_client(api_key: str = None) -> TrellisClient:
    """Get or create the singleton TRELLIS client."""
    global _client
    if _client is None:
        _client = TrellisClient(api_key)
    return _client


if __name__ == "__main__":
    import sys
    from dotenv import load_dotenv

    logging.basicConfig(level=logging.INFO, format="%(levelname)s: %(message)s")

    env_path = Path(__file__).resolve().parent.parent.parent / ".env"
    load_dotenv(env_path)

    if len(sys.argv) < 2:
        print("Usage: python -m pipeline.texture.trellis_client <image.png> [output.glb]")
        print("\nRequires FAL_KEY in .env file.")
        sys.exit(1)

    image_path = sys.argv[1]
    output_path = sys.argv[2] if len(sys.argv) > 2 else "trellis_output.glb"

    client = TrellisClient()
    request_id = client.create_task(image_path)
    print(f"Request ID: {request_id}")

    def on_progress(status, pct):
        print(f"  {status}: {pct}%")

    result = client.wait_and_download(request_id, output_path, progress_callback=on_progress)
    print(f"Generated: {result}")
