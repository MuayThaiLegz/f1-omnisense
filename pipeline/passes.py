"""5-pass extraction definitions for multi-model PDF analysis.

Each pass targets a specific aspect of the documents:
  1. Document Overview — type, title, revision, TOC, standards
  2. Equipment & Tags — identifiers, references, manufacturer data
  3. Specifications & Rules — design rules, materials, ratings, dimensions
  4. Tables & Data — complete table extraction with all rows/values
  5. Connections & Flow — component-to-component, flow paths, system boundaries

Customize the prompts below for your domain. The current prompts are
general-purpose templates suitable for engineering/technical documents.
"""

from __future__ import annotations

from dataclasses import dataclass, field
from pathlib import Path
from typing import TYPE_CHECKING

if TYPE_CHECKING:
    from .renderer import PageViews


# ── Pass definition dataclass ────────────────────────────────────────────

@dataclass
class PassDefinition:
    """Defines one extraction pass."""

    number: int
    name: str
    focus: str
    system_prompt: str
    user_prompt: str
    include_quadrants: bool = False
    include_zoomed: bool = False
    page_filter: str = "all"  # "all" | "tables_only" | "drawings_only"


# ── Image selection logic ────────────────────────────────────────────────

def select_images(views: PageViews, pass_def: PassDefinition) -> list[Path]:
    """Select which images to send for a given page + pass combination.

    Returns a list of image paths to include in the API call.
    """
    images: list[Path] = []

    # Full page is always included
    if views.full and views.full.exists():
        images.append(views.full)

    # Quadrants: include when pass requests them AND page is image-heavy
    if pass_def.include_quadrants and views.quadrants:
        is_image_heavy = views.word_count < 50
        # Always include quadrants for drawing pages or image-heavy pages
        if is_image_heavy or pass_def.page_filter == "drawings_only":
            for qpath in views.quadrants.values():
                if qpath.exists():
                    images.append(qpath)

    # Zoomed regions: include when pass requests them
    if pass_def.include_zoomed and views.zoomed:
        for region in views.zoomed[:3]:  # Max 3 zoom regions
            if region.path.exists():
                images.append(region.path)

    return images


def should_process_page(views: PageViews, pass_def: PassDefinition) -> bool:
    """Determine if a page should be processed for a given pass."""
    if pass_def.page_filter == "all":
        return True
    if pass_def.page_filter == "tables_only":
        return len(views.native_tables) > 0 or len(views.zoomed) > 0
    if pass_def.page_filter == "drawings_only":
        # Pages with few words and/or detected regions are likely drawings
        return views.word_count < 200 or len(views.zoomed) > 0
    return True


# ── System prompts ───────────────────────────────────────────────────────

_SYSTEM_BASE = (
    "You are an expert document analyst. You extract structured data from "
    "technical documents with extreme precision. "
    "Always respond with valid JSON only — no markdown, no commentary, no explanation. "
    "If a field is not found, use null. If a list is empty, use []. "
    "Extract EVERYTHING visible — do not summarize or omit details."
)

# ── Pass 1: Document Overview ────────────────────────────────────────────

PASS_1_OVERVIEW = PassDefinition(
    number=1,
    name="overview",
    focus="Document type, title, revision, date, scope, TOC, standards referenced",
    include_quadrants=False,
    include_zoomed=False,
    page_filter="all",
    system_prompt=_SYSTEM_BASE,
    user_prompt="""\
Analyze this document page and extract document-level metadata.

Return JSON with this exact structure:
{
  "page_number": <int>,
  "document_type": "<type classification>",
  "title": "<document title if visible, null otherwise>",
  "revision": "<revision number/letter if visible, null otherwise>",
  "date": "<date if visible, null otherwise>",
  "scope": "<brief scope description if identifiable>",
  "sections": [
    {"number": "<section number>", "title": "<section title>", "page_range": [<start>, <end>]}
  ],
  "standards_referenced": ["<standard code and title>"],
  "equipment_mentioned": ["<equipment tag or identifier>"],
  "key_topics": ["<topic>"],
  "has_drawings": <true/false>,
  "has_tables": <true/false>,
  "has_pid": <true/false>,
  "notes": "<any other relevant observations>"
}

Important:
- Extract ALL tags and references visible on this page.
- Include standard codes (e.g. ASME, ANSI, API, ISO, FIA, etc.)
- Include all identifiers, part numbers, and reference codes.""",
)

# ── Pass 2: Equipment & Tags ────────────────────────────────────────────

PASS_2_EQUIPMENT = PassDefinition(
    number=2,
    name="equipment",
    focus="Equipment tags, identifiers, references, component data",
    include_quadrants=True,
    include_zoomed=False,
    page_filter="all",
    system_prompt=_SYSTEM_BASE,
    user_prompt="""\
Extract ALL equipment identifiers, tags, references, and component data from this page.

Return JSON with this exact structure:
{
  "page_number": <int>,
  "equipment": [
    {
      "tag": "<equipment tag/identifier>",
      "type": "<equipment type>",
      "description": "<equipment description>",
      "kks": "<full identifier code if visible>",
      "pipe_refs": ["<connected references>"],
      "nozzles": [
        {"id": "<connection ID>", "nps": "<nominal size>", "service": "<function>"}
      ],
      "specs": {
        "manufacturer": "<manufacturer name>",
        "model": "<model number>",
        "material": "<material specification>",
        "rating": "<rating>",
        "capacity": "<capacity/flow rate>"
      },
      "location_description": "<where on the drawing/document>"
    }
  ],
  "pipe_references": [
    {
      "ref": "<reference identifier>",
      "nps": "<nominal size with units>",
      "class": "<class designation>",
      "service": "<service description>",
      "from_equipment": "<source equipment tag>",
      "to_equipment": "<destination equipment tag>",
      "medium": "<medium>"
    }
  ],
  "kks_codes": ["<all identifier codes found on this page>"],
  "instruments": [
    {
      "tag": "<instrument tag>",
      "type": "<type>",
      "description": "<what it measures/controls>"
    }
  ]
}

Important:
- Look for BOTH text labels AND drawing annotations
- Include ALL items, even partially visible ones — mark uncertain values with "?"
""",
)

# ── Pass 3: Specifications & Rules ──────────────────────────────────────

PASS_3_SPECS = PassDefinition(
    number=3,
    name="specifications",
    focus="Design rules, dimensions, materials, ratings, tolerances",
    include_quadrants=True,
    include_zoomed=True,
    page_filter="all",
    system_prompt=_SYSTEM_BASE,
    user_prompt="""\
Extract ALL specifications, design rules, material specifications, and dimensional data from this page.

Return JSON with this exact structure:
{
  "page_number": <int>,
  "rules": [
    {
      "id": "<rule identifier>",
      "category": "<category>",
      "description": "<what the rule specifies>",
      "value": <numeric value or null>,
      "unit": "<unit>",
      "condition": "<when this rule applies>",
      "reference": "<section/table/standard reference>",
      "severity": "<violation|warning|recommendation>",
      "source_standard": "<standard reference>"
    }
  ],
  "material_specs": [
    {
      "material": "<material designation>",
      "application": "<what it's used for>",
      "temp_range": "<temperature range>",
      "pressure_rating": "<pressure rating>",
      "notes": "<additional notes>"
    }
  ],
  "pressure_ratings": [
    {
      "class": "<class>",
      "temp_c": <temperature in Celsius>,
      "pressure_bar": <allowable pressure in bar>,
      "material_group": "<material group>"
    }
  ],
  "dimensional_data": [
    {
      "component": "<what component>",
      "dimension": "<what dimension>",
      "nps": "<nominal size if applicable>",
      "value": <numeric value>,
      "unit": "<unit>",
      "schedule": "<schedule if applicable>"
    }
  ],
  "insulation": [
    {
      "service": "<service type>",
      "temp_range": "<temperature range>",
      "thickness_mm": <thickness>,
      "material": "<material>",
      "jacket": "<jacket material>"
    }
  ]
}

Important:
- Extract EVERY numeric value with its unit
- Reference the source (section number, table number, standard)""",
)

# ── Pass 4: Tables & Data ───────────────────────────────────────────────

PASS_4_TABLES = PassDefinition(
    number=4,
    name="tables",
    focus="Complete table extraction — every header, row, value, and unit",
    include_quadrants=False,
    include_zoomed=True,
    page_filter="tables_only",
    system_prompt=_SYSTEM_BASE,
    user_prompt="""\
Extract EVERY table on this page with COMPLETE data — all headers, all rows, all values.

Return JSON with this exact structure:
{
  "page_number": <int>,
  "tables": [
    {
      "table_id": "<sequential ID, e.g. T1, T2>",
      "title": "<table title/caption>",
      "context": "<what section/topic this table belongs to>",
      "headers": ["<column header 1>", "<column header 2>"],
      "rows": [
        ["<cell value>", "<cell value>"],
        ["<cell value>", "<cell value>"]
      ],
      "units": {"<column_name>": "<unit>"},
      "merged_cells": ["<description of any merged cells>"],
      "notes": "<footnotes or notes below the table>",
      "row_count": <number of data rows>,
      "col_count": <number of columns>,
      "reference": "<standard or source reference if cited>"
    }
  ]
}

CRITICAL RULES:
- Extract EVERY row — do NOT summarize or skip rows
- Include merged header cells — describe spanning in merged_cells
- Preserve exact numeric values — do NOT round
- Include units for every column that has them
- If a cell is empty, use "" (empty string)
- If a cell spans multiple columns, repeat the value
- For multi-line cells, join with " | "
- Include table footnotes and reference notes
- Small tables in margins or side-notes count too
- Extract values like "1/2", "2 1/2" exactly as written (fractions)""",
)

# ── Pass 5: Connections & Flow ──────────────────────────────────────────

PASS_5_CONNECTIONS = PassDefinition(
    number=5,
    name="connections",
    focus="Component-to-component connections, flow paths, system boundaries",
    include_quadrants=True,
    include_zoomed=False,
    page_filter="drawings_only",
    system_prompt=_SYSTEM_BASE,
    user_prompt="""\
Extract ALL connections, flow paths, and system connectivity from this page.

Return JSON with this exact structure:
{
  "page_number": <int>,
  "connections": [
    {
      "from_equipment": "<source equipment tag>",
      "from_nozzle": "<source connection ID>",
      "to_equipment": "<destination equipment tag>",
      "to_nozzle": "<destination connection ID>",
      "pipe_ref": "<reference identifier>",
      "nps": "<nominal size>",
      "class": "<class>",
      "service": "<service description>",
      "flow_direction": "<from→to description>",
      "medium": "<medium>",
      "line_kks": "<line identifier code if visible>"
    }
  ],
  "system_boundaries": [
    {
      "system": "<system name>",
      "boundary_type": "<battery_limit|area_limit|unit_limit>",
      "location": "<where the boundary is>",
      "tag": "<boundary tag if any>"
    }
  ],
  "nozzle_connections": [
    {
      "equipment": "<equipment tag>",
      "nozzle": "<connection ID>",
      "pipe_ref": "<connected reference>",
      "service": "<inlet|outlet|drain|vent|bypass>",
      "nps": "<nominal size>",
      "elevation": "<elevation if noted>"
    }
  ],
  "flow_paths": [
    {
      "path": ["<equipment1>", "<equipment2>", "<equipment3>"],
      "service": "<service description>",
      "medium": "<medium>",
      "direction": "<description of flow direction>",
      "pipe_refs": ["<references along this path>"]
    }
  ],
  "valves_inline": [
    {
      "tag": "<valve tag>",
      "type": "<gate|globe|check|butterfly|ball|control>",
      "pipe_ref": "<on which reference>",
      "between": ["<upstream equipment>", "<downstream equipment>"],
      "nps": "<nominal size>",
      "function": "<isolation|regulation|check|etc.>"
    }
  ]
}

Important:
- Follow lines from equipment to equipment — trace the full path
- Note flow DIRECTION (arrows on diagrams)
- Identify all connections and their relationships
- Look for bypass lines and drain connections
- Valves inline between equipment are important for routing""",
)


# ── All passes registry ─────────────────────────────────────────────────

ALL_PASSES: list[PassDefinition] = [
    PASS_1_OVERVIEW,
    PASS_2_EQUIPMENT,
    PASS_3_SPECS,
    PASS_4_TABLES,
    PASS_5_CONNECTIONS,
]

PASS_BY_NUMBER: dict[int, PassDefinition] = {p.number: p for p in ALL_PASSES}
PASS_BY_NAME: dict[str, PassDefinition] = {p.name: p for p in ALL_PASSES}


def get_pass(identifier: int | str) -> PassDefinition:
    """Get a pass definition by number (1-5) or name."""
    if isinstance(identifier, int):
        if identifier not in PASS_BY_NUMBER:
            raise ValueError(f"Pass {identifier} not found. Valid: 1-5")
        return PASS_BY_NUMBER[identifier]
    if identifier not in PASS_BY_NAME:
        raise ValueError(f"Pass '{identifier}' not found. Valid: {list(PASS_BY_NAME)}")
    return PASS_BY_NAME[identifier]
