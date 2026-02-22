"""Meshy.ai client — generates 3D GLB models from images via REST API.

Used as the regeneration pathway when reviewing Hunyuan3D output
and wanting an alternative 3D model.
"""

import os
import time
import base64
import logging
import requests
from pathlib import Path

logger = logging.getLogger(__name__)

MESHY_BASE_URL = "https://api.meshy.ai"


class MeshyClient:
    """Wrapper around the Meshy.ai Image-to-3D REST API."""

    def __init__(self, api_key: str = None):
        self.api_key = api_key or os.getenv("MESHY_API_KEY", "")
        if not self.api_key:
            raise ValueError(
                "Meshy API key not set. Add MESHY_API_KEY to .env file."
            )
        self._headers = {"Authorization": f"Bearer {self.api_key}"}

    def create_task(self, image_path: str | Path,
                    texture_prompt: str = "",
                    enable_pbr: bool = True,
                    topology: str = "triangle",
                    target_polycount: int = 30000) -> str:
        """Create an image-to-3D task.

        Args:
            image_path: Path to input image (encoded as base64 data URI).
            texture_prompt: Optional text prompt for texture style.
            enable_pbr: Generate PBR material textures.
            topology: Mesh topology ('triangle' or 'quad').
            target_polycount: Target polygon count.

        Returns:
            Task ID string.
        """
        image_path = Path(image_path)
        if not image_path.exists():
            raise FileNotFoundError(f"Input image not found: {image_path}")

        # Encode image as base64 data URI
        suffix = image_path.suffix.lower().lstrip(".")
        mime = {"png": "image/png", "jpg": "image/jpeg", "jpeg": "image/jpeg"}.get(
            suffix, "image/png"
        )
        with open(image_path, "rb") as f:
            b64 = base64.b64encode(f.read()).decode()
        data_uri = f"data:{mime};base64,{b64}"

        payload = {
            "image_url": data_uri,
            "enable_pbr": enable_pbr,
            "topology": topology,
            "target_polycount": target_polycount,
        }
        if texture_prompt:
            payload["texture_prompt"] = texture_prompt

        logger.info("Creating Meshy task for %s", image_path.name)
        resp = requests.post(
            f"{MESHY_BASE_URL}/openapi/v1/image-to-3d",
            headers={**self._headers, "Content-Type": "application/json"},
            json=payload,
            timeout=30,
        )
        resp.raise_for_status()
        data = resp.json()
        task_id = data.get("result", data.get("id", ""))
        logger.info("Meshy task created: %s", task_id)
        return task_id

    def poll_task(self, task_id: str) -> dict:
        """Poll task status.

        Returns dict with keys: status, progress, model_urls, etc.
        Status values: PENDING, IN_PROGRESS, SUCCEEDED, FAILED, EXPIRED.
        """
        resp = requests.get(
            f"{MESHY_BASE_URL}/openapi/v1/image-to-3d/{task_id}",
            headers=self._headers,
            timeout=15,
        )
        resp.raise_for_status()
        return resp.json()

    def download_glb(self, model_url: str, output_path: str | Path) -> Path:
        """Download GLB file from Meshy CDN."""
        output_path = Path(output_path)
        output_path.parent.mkdir(parents=True, exist_ok=True)

        logger.info("Downloading GLB from Meshy...")
        resp = requests.get(model_url, timeout=120, stream=True)
        resp.raise_for_status()

        with open(output_path, "wb") as f:
            for chunk in resp.iter_content(chunk_size=8192):
                f.write(chunk)

        size_mb = output_path.stat().st_size / (1024 * 1024)
        logger.info("Saved GLB (%.1f MB) to %s", size_mb, output_path)
        return output_path

    def wait_and_download(self, task_id: str, output_path: str | Path,
                          timeout: int = 300, poll_interval: int = 5,
                          progress_callback=None) -> Path:
        """Poll until completion, then download the GLB.

        Args:
            task_id: Meshy task ID.
            output_path: Where to save the GLB.
            timeout: Max wait time in seconds.
            poll_interval: Seconds between polls.
            progress_callback: Optional fn(status, progress_pct) called each poll.

        Returns:
            Path to downloaded GLB.

        Raises:
            TimeoutError: If task doesn't complete within timeout.
            RuntimeError: If task fails.
        """
        start = time.time()

        while time.time() - start < timeout:
            status_data = self.poll_task(task_id)
            status = status_data.get("status", "UNKNOWN")
            progress = status_data.get("progress", 0)

            logger.info("Meshy task %s: %s (%d%%)", task_id, status, progress)
            if progress_callback:
                progress_callback(status, progress)

            if status == "SUCCEEDED":
                model_urls = status_data.get("model_urls", {})
                glb_url = model_urls.get("glb", "")
                if not glb_url:
                    raise RuntimeError("Meshy task succeeded but no GLB URL found")
                return self.download_glb(glb_url, output_path)

            if status in ("FAILED", "EXPIRED"):
                error = status_data.get("task_error", {}).get("message", "Unknown error")
                raise RuntimeError(f"Meshy task {status}: {error}")

            time.sleep(poll_interval)

        raise TimeoutError(f"Meshy task {task_id} timed out after {timeout}s")


# Module-level singleton
_client = None


def get_meshy_client(api_key: str = None) -> MeshyClient:
    """Get or create the singleton Meshy client."""
    global _client
    if _client is None:
        _client = MeshyClient(api_key)
    return _client


if __name__ == "__main__":
    import sys
    from dotenv import load_dotenv

    logging.basicConfig(level=logging.INFO, format="%(levelname)s: %(message)s")

    # Load .env from project root
    env_path = Path(__file__).resolve().parent.parent.parent / ".env"
    load_dotenv(env_path)

    if len(sys.argv) < 2:
        print("Usage: python -m backend.texture.meshy_client <image.png> [output.glb]")
        print("\nRequires MESHY_API_KEY in .env file.")
        sys.exit(1)

    image_path = sys.argv[1]
    output_path = sys.argv[2] if len(sys.argv) > 2 else "meshy_output.glb"

    client = MeshyClient()
    task_id = client.create_task(image_path)
    print(f"Task ID: {task_id}")

    def on_progress(status, pct):
        print(f"  {status}: {pct}%")

    result = client.wait_and_download(task_id, output_path, progress_callback=on_progress)
    print(f"Generated: {result}")
