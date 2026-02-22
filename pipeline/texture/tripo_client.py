"""Tripo AI client — generates 3D GLB models from images via REST API.

Uses Tripo3D v2.5 API for image-to-3D conversion with PBR textures.
Free tier: 300 credits/month (~6-10 models).

API docs: https://platform.tripo3d.ai/docs
"""

import os
import time
import logging
import requests
from pathlib import Path

logger = logging.getLogger(__name__)

TRIPO_BASE_URL = "https://api.tripo3d.ai/v2/openapi"


class TripoClient:
    """Wrapper around the Tripo3D Image-to-3D REST API."""

    def __init__(self, api_key: str = None):
        self.api_key = api_key or os.getenv("TRIPO_API_KEY", "")
        if not self.api_key:
            raise ValueError(
                "Tripo API key not set. Add TRIPO_API_KEY to .env file. "
                "Get one at https://platform.tripo3d.ai/api-keys"
            )
        self._headers = {"Authorization": f"Bearer {self.api_key}"}

    def upload_image(self, image_path: str | Path) -> str:
        """Upload image and return file token.

        Args:
            image_path: Path to input image (PNG, JPEG, WebP).

        Returns:
            Image token string for use in task creation.
        """
        image_path = Path(image_path)
        if not image_path.exists():
            raise FileNotFoundError(f"Input image not found: {image_path}")

        with open(image_path, "rb") as f:
            resp = requests.post(
                f"{TRIPO_BASE_URL}/upload/sts",
                headers=self._headers,
                files={"file": (image_path.name, f)},
                timeout=60,
            )
        resp.raise_for_status()
        data = resp.json()
        if data.get("code") != 0:
            raise RuntimeError(f"Tripo upload failed: {data}")

        token = data["data"]["image_token"]
        logger.info("Uploaded %s -> token %s", image_path.name, token[:20])
        return token

    def create_task(self, image_path: str | Path,
                    texture: bool = True,
                    pbr: bool = True,
                    model_version: str = "v2.5-20250123",
                    face_limit: int = None) -> str:
        """Create an image-to-3D task.

        Args:
            image_path: Path to input image.
            texture: Generate texture. Default True.
            pbr: Generate PBR maps. Default True.
            model_version: Tripo model version.
            face_limit: Max polygon count (optional).

        Returns:
            Task ID string.
        """
        image_token = self.upload_image(image_path)

        suffix = Path(image_path).suffix.lower().lstrip(".")
        file_type = {"png": "png", "jpg": "jpg", "jpeg": "jpg", "webp": "webp"}.get(
            suffix, "png"
        )

        payload = {
            "type": "image_to_model",
            "model_version": model_version,
            "file": {
                "type": file_type,
                "file_token": image_token,
            },
            "texture": texture,
            "pbr": pbr,
        }
        if face_limit:
            payload["face_limit"] = face_limit

        logger.info("Creating Tripo task for %s", Path(image_path).name)
        resp = requests.post(
            f"{TRIPO_BASE_URL}/task",
            headers={**self._headers, "Content-Type": "application/json"},
            json=payload,
            timeout=30,
        )
        resp.raise_for_status()
        data = resp.json()
        if data.get("code") != 0:
            raise RuntimeError(f"Tripo task creation failed: {data}")

        task_id = data["data"]["task_id"]
        logger.info("Tripo task created: %s", task_id)
        return task_id

    def poll_task(self, task_id: str) -> dict:
        """Poll task status.

        Returns dict with status, output URLs, etc.
        Status values: queued, running, success, failed.
        """
        resp = requests.get(
            f"{TRIPO_BASE_URL}/task/{task_id}",
            headers=self._headers,
            timeout=15,
        )
        resp.raise_for_status()
        data = resp.json()
        if data.get("code") != 0:
            raise RuntimeError(f"Tripo poll failed: {data}")
        return data["data"]

    def download_glb(self, model_url: str, output_path: str | Path) -> Path:
        """Download GLB file from Tripo CDN."""
        output_path = Path(output_path)
        output_path.parent.mkdir(parents=True, exist_ok=True)

        logger.info("Downloading GLB from Tripo...")
        resp = requests.get(model_url, timeout=120, stream=True)
        resp.raise_for_status()

        with open(output_path, "wb") as f:
            for chunk in resp.iter_content(chunk_size=8192):
                f.write(chunk)

        size_mb = output_path.stat().st_size / (1024 * 1024)
        logger.info("Saved GLB (%.1f MB) to %s", size_mb, output_path)
        return output_path

    def wait_and_download(self, task_id: str, output_path: str | Path,
                          timeout: int = 600, poll_interval: int = 5,
                          progress_callback=None) -> Path:
        """Poll until completion, then download the GLB.

        Args:
            task_id: Tripo task ID.
            output_path: Where to save the GLB.
            timeout: Max wait time in seconds.
            poll_interval: Seconds between polls.
            progress_callback: Optional fn(status, progress_pct).

        Returns:
            Path to downloaded GLB.
        """
        start = time.time()

        while time.time() - start < timeout:
            task = self.poll_task(task_id)
            status = task.get("status", "unknown")
            progress = task.get("progress", 0)

            logger.info("Tripo task %s: %s (%d%%)", task_id, status, progress)
            if progress_callback:
                progress_callback(status, progress)

            if status == "success":
                output = task.get("output", {})
                # Prefer PBR model if available
                glb_url = output.get("pbr_model") or output.get("model", "")
                if not glb_url:
                    raise RuntimeError("Tripo task succeeded but no GLB URL found")
                return self.download_glb(glb_url, output_path)

            if status == "failed":
                raise RuntimeError(f"Tripo task failed: {task}")

            time.sleep(poll_interval)

        raise TimeoutError(f"Tripo task {task_id} timed out after {timeout}s")


# Module-level singleton
_client = None


def get_tripo_client(api_key: str = None) -> TripoClient:
    """Get or create the singleton Tripo client."""
    global _client
    if _client is None:
        _client = TripoClient(api_key)
    return _client


if __name__ == "__main__":
    import sys
    from dotenv import load_dotenv

    logging.basicConfig(level=logging.INFO, format="%(levelname)s: %(message)s")

    env_path = Path(__file__).resolve().parent.parent.parent / ".env"
    load_dotenv(env_path)

    if len(sys.argv) < 2:
        print("Usage: python -m pipeline.texture.tripo_client <image.png> [output.glb]")
        print("\nRequires TRIPO_API_KEY in .env file.")
        sys.exit(1)

    image_path = sys.argv[1]
    output_path = sys.argv[2] if len(sys.argv) > 2 else "tripo_output.glb"

    client = TripoClient()
    task_id = client.create_task(image_path)
    print(f"Task ID: {task_id}")

    def on_progress(status, pct):
        print(f"  {status}: {pct}%")

    result = client.wait_and_download(task_id, output_path, progress_callback=on_progress)
    print(f"Generated: {result}")
