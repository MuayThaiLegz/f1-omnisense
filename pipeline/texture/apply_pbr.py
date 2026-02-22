"""Apply PBR material properties to GLB files.

Uses pygltflib to modify GLB material properties based on F1-themed
material presets (carbon fiber, papaya orange, titanium, etc.).
"""

import json
import logging
from datetime import datetime, timezone
from pathlib import Path

logger = logging.getLogger(__name__)

# F1-themed PBR material presets (metallic-roughness workflow, glTF 2.0)
MATERIAL_PBR: dict[str, dict] = {
    "carbon_fiber": {
        "baseColor": [0.15, 0.15, 0.15, 1.0],
        "metallic": 0.3,
        "roughness": 0.35,
        "display_name": "Carbon Fiber",
    },
    "papaya_orange": {
        "baseColor": [1.0, 0.5, 0.0, 1.0],
        "metallic": 0.7,
        "roughness": 0.25,
        "display_name": "McLaren Papaya",
    },
    "titanium": {
        "baseColor": [0.65, 0.65, 0.68, 1.0],
        "metallic": 0.95,
        "roughness": 0.2,
        "display_name": "Titanium",
    },
    "rubber": {
        "baseColor": [0.12, 0.12, 0.12, 1.0],
        "metallic": 0.0,
        "roughness": 0.9,
        "display_name": "Rubber/Tire",
    },
    "painted_blue": {
        "baseColor": [0.0, 0.2, 0.6, 1.0],
        "metallic": 0.7,
        "roughness": 0.25,
        "display_name": "Painted Blue",
    },
    "chrome": {
        "baseColor": [0.9, 0.9, 0.92, 1.0],
        "metallic": 1.0,
        "roughness": 0.05,
        "display_name": "Chrome",
    },
    "matte_black": {
        "baseColor": [0.08, 0.08, 0.08, 1.0],
        "metallic": 0.1,
        "roughness": 0.8,
        "display_name": "Matte Black",
    },
    "red_livery": {
        "baseColor": [0.8, 0.1, 0.1, 1.0],
        "metallic": 0.7,
        "roughness": 0.25,
        "display_name": "Red Livery",
    },
}


def apply_pbr_to_glb(
    input_glb: Path,
    output_glb: Path,
    preset: str = "carbon_fiber",
    pbr_overrides: dict | None = None,
) -> dict:
    """Apply PBR material to all meshes in a GLB file.

    Args:
        input_glb: Path to source GLB.
        output_glb: Path to write the modified GLB.
        preset: Material preset name (key into MATERIAL_PBR).
        pbr_overrides: Optional dict to override specific PBR values.

    Returns:
        Dict with applied material info and mesh count.
    """
    import pygltflib

    gltf = pygltflib.GLTF2().load(str(input_glb))

    pbr_values = MATERIAL_PBR.get(preset, MATERIAL_PBR["carbon_fiber"]).copy()
    if pbr_overrides:
        pbr_values.update(pbr_overrides)

    base_color = pbr_values["baseColor"]
    metallic = float(pbr_values["metallic"])
    roughness = float(pbr_values["roughness"])

    modified_count = 0

    if gltf.materials:
        for material in gltf.materials:
            if material.pbrMetallicRoughness is None:
                material.pbrMetallicRoughness = pygltflib.PbrMetallicRoughness()
            material.pbrMetallicRoughness.baseColorFactor = base_color
            material.pbrMetallicRoughness.metallicFactor = metallic
            material.pbrMetallicRoughness.roughnessFactor = roughness
            material.name = f"f1_{preset}"
            modified_count += 1
    else:
        new_mat = pygltflib.Material(
            pbrMetallicRoughness=pygltflib.PbrMetallicRoughness(
                baseColorFactor=base_color,
                metallicFactor=metallic,
                roughnessFactor=roughness,
            ),
            name=f"f1_{preset}",
        )
        gltf.materials.append(new_mat)
        mat_index = len(gltf.materials) - 1
        for mesh in gltf.meshes:
            for primitive in mesh.primitives:
                primitive.material = mat_index
        modified_count = 1

    gltf.save(str(output_glb))

    logger.info(
        "Applied PBR preset '%s' to %s (%d materials modified) -> %s",
        preset, input_glb.name, modified_count, output_glb.name,
    )

    return {
        "preset": preset,
        "pbr": {
            "baseColor": base_color,
            "metallic": metallic,
            "roughness": roughness,
        },
        "materials_modified": modified_count,
        "output_path": str(output_glb),
        "output_size_bytes": int(output_glb.stat().st_size),
    }


def apply_texture_to_model(
    model_name: str,
    preset: str = "carbon_fiber",
    models_dir: Path = None,
) -> dict:
    """Apply PBR material to a Hunyuan GLB for a model.

    Reads from output/3d_models/{model_name}/hunyuan.glb.
    Writes to output/3d_models/{model_name}/hunyuan_pbr.glb.

    Args:
        model_name: Model identifier.
        preset: Material preset name.
        models_dir: Override for models directory.

    Returns:
        Dict with applied material info.
    """
    if models_dir is None:
        models_dir = Path(__file__).resolve().parent.parent / "output" / "3d_models"

    model_dir = models_dir / model_name
    input_glb = model_dir / "hunyuan.glb"

    if not input_glb.exists():
        raise FileNotFoundError(f"No Hunyuan GLB for model: {model_name}")

    output_glb = model_dir / "hunyuan_pbr.glb"
    result = apply_pbr_to_glb(input_glb, output_glb, preset)

    # Update metadata.json
    meta_path = model_dir / "metadata.json"
    existing = {}
    if meta_path.exists():
        with open(meta_path) as f:
            existing = json.load(f)

    existing["pbr_applied"] = {
        "preset": preset,
        "pbr": result["pbr"],
        "materials_modified": result["materials_modified"],
        "output_glb": "hunyuan_pbr.glb",
        "output_size_bytes": result["output_size_bytes"],
        "applied_at": datetime.now(timezone.utc).isoformat(),
    }

    with open(meta_path, "w") as f:
        json.dump(existing, f, indent=2)

    result["model_name"] = model_name
    return result
