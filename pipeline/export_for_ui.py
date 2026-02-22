"""Aggregate pipeline extraction JSONs into a single intelligence.json for the UI."""

from __future__ import annotations

import json
from pathlib import Path

EXTRACTIONS_DIR = Path(__file__).parent / "output" / "extractions"
OUTPUT_PATH = Path(__file__).parent / "output" / "intelligence.json"


def _collect_items(pages: list[dict], key: str) -> list[dict]:
    """Collect all non-empty items from a specific key across pages."""
    items = []
    for page in pages:
        data = page.get("data")
        if not data:
            continue
        val = data.get(key)
        if isinstance(val, list):
            for item in val:
                if isinstance(item, dict):
                    item["_page"] = page.get("page", 0)
                items.append(item)
        elif isinstance(val, dict) and val:
            val["_page"] = page.get("page", 0)
            items.append(val)
    return items


def _dedup_rules(rules: list[dict]) -> list[dict]:
    """Deduplicate rules by id+category."""
    seen = set()
    out = []
    for r in rules:
        key = (r.get("id", ""), r.get("category", ""), (r.get("description") or "")[:80])
        if key not in seen:
            seen.add(key)
            out.append(r)
    return out


def _dedup_equipment(equipment: list[dict]) -> list[dict]:
    """Deduplicate equipment by tag."""
    seen = set()
    out = []
    for e in equipment:
        tag = e.get("tag", "")
        if tag and tag not in seen:
            seen.add(tag)
            out.append(e)
    return out


def export():
    intelligence: dict = {
        "documents": [],
        "rules": [],
        "equipment": [],
        "dimensional_data": [],
        "material_specs": [],
        "stats": {
            "total_pages": 0,
            "total_rules": 0,
            "total_equipment": 0,
            "total_dimensions": 0,
            "total_materials": 0,
            "total_tokens_in": 0,
            "total_tokens_out": 0,
            "total_cost_usd": 0.0,
            "total_latency_s": 0.0,
        },
    }

    if not EXTRACTIONS_DIR.exists():
        print("No extractions directory found.")
        return

    for pdf_dir in sorted(EXTRACTIONS_DIR.iterdir()):
        if not pdf_dir.is_dir():
            continue

        pdf_stem = pdf_dir.name
        doc_meta: dict = {
            "name": pdf_stem,
            "title": pdf_stem.replace("_", " "),
            "passes": [],
        }

        for json_file in sorted(pdf_dir.glob("*.json")):
            try:
                with open(json_file) as f:
                    data = json.load(f)
            except (json.JSONDecodeError, OSError):
                continue

            pages = data.get("pages", [])
            summary = data.get("summary", {})
            pass_num = data.get("pass_num", 0)
            pass_name = data.get("pass_name", "")

            doc_meta["passes"].append({
                "number": pass_num,
                "name": pass_name,
                "pages_processed": summary.get("pages_processed", 0),
                "items_found": summary.get("items_found", 0),
                "tokens_in": summary.get("total_tokens_in", 0),
                "tokens_out": summary.get("total_tokens_out", 0),
                "cost_usd": summary.get("total_cost_usd", 0),
                "latency_s": summary.get("total_latency_s", 0),
            })

            # Accumulate stats
            intelligence["stats"]["total_tokens_in"] += summary.get("total_tokens_in", 0)
            intelligence["stats"]["total_tokens_out"] += summary.get("total_tokens_out", 0)
            intelligence["stats"]["total_cost_usd"] += summary.get("total_cost_usd", 0)
            intelligence["stats"]["total_latency_s"] += summary.get("total_latency_s", 0)

            # Pass 1: overview — extract document metadata
            if pass_num == 1 and pages:
                first = pages[0].get("data", {})
                doc_meta["document_type"] = first.get("document_type", "")
                doc_meta["title"] = first.get("title", pdf_stem.replace("_", " "))
                doc_meta["revision"] = first.get("revision", "")
                doc_meta["date"] = first.get("date", "")
                doc_meta["total_pages"] = len(pages)
                intelligence["stats"]["total_pages"] += len(pages)

                # Collect all unique sections, standards, topics
                all_sections = []
                all_standards = set()
                all_topics = set()
                for p in pages:
                    d = p.get("data", {})
                    for s in d.get("sections", []):
                        all_sections.append(s)
                    for st in d.get("standards_referenced", []):
                        all_standards.add(st)
                    for t in d.get("key_topics", []):
                        all_topics.add(t)

                # Deduplicate sections by number
                seen_sections = set()
                unique_sections = []
                for s in all_sections:
                    num = s.get("number", "")
                    if num and num not in seen_sections:
                        seen_sections.add(num)
                        unique_sections.append(s)

                doc_meta["sections"] = unique_sections[:50]  # Cap for UI
                doc_meta["standards"] = sorted(all_standards)
                doc_meta["topics"] = sorted(all_topics)[:30]

            # Pass 2: equipment
            if pass_num == 2:
                equip = _collect_items(pages, "equipment")
                for e in equip:
                    e["_source"] = pdf_stem
                intelligence["equipment"].extend(equip)

            # Pass 3: specifications
            if pass_num == 3:
                rules = _collect_items(pages, "rules")
                for r in rules:
                    r["_source"] = pdf_stem
                intelligence["rules"].extend(rules)

                dims = _collect_items(pages, "dimensional_data")
                for d in dims:
                    d["_source"] = pdf_stem
                intelligence["dimensional_data"].extend(dims)

                mats = _collect_items(pages, "material_specs")
                for m in mats:
                    m["_source"] = pdf_stem
                intelligence["material_specs"].extend(mats)

        intelligence["documents"].append(doc_meta)

    # Deduplicate
    intelligence["rules"] = _dedup_rules(intelligence["rules"])
    intelligence["equipment"] = _dedup_equipment(intelligence["equipment"])

    # Update final counts
    intelligence["stats"]["total_rules"] = len(intelligence["rules"])
    intelligence["stats"]["total_equipment"] = len(intelligence["equipment"])
    intelligence["stats"]["total_dimensions"] = len(intelligence["dimensional_data"])
    intelligence["stats"]["total_materials"] = len(intelligence["material_specs"])
    intelligence["stats"]["total_cost_usd"] = round(intelligence["stats"]["total_cost_usd"], 4)
    intelligence["stats"]["total_latency_s"] = round(intelligence["stats"]["total_latency_s"], 1)

    # Write output
    OUTPUT_PATH.parent.mkdir(parents=True, exist_ok=True)
    with open(OUTPUT_PATH, "w") as f:
        json.dump(intelligence, f, indent=2, default=str)

    print(f"Exported intelligence.json:")
    print(f"  Documents: {len(intelligence['documents'])}")
    print(f"  Rules: {intelligence['stats']['total_rules']}")
    print(f"  Equipment: {intelligence['stats']['total_equipment']}")
    print(f"  Dimensions: {intelligence['stats']['total_dimensions']}")
    print(f"  Materials: {intelligence['stats']['total_materials']}")
    print(f"  Total pages: {intelligence['stats']['total_pages']}")
    print(f"  Total cost: ${intelligence['stats']['total_cost_usd']}")
    print(f"  Output: {OUTPUT_PATH}")


if __name__ == "__main__":
    export()
