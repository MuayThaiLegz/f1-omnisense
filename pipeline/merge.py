"""Cross-model result merging and consensus scoring.

Merges extraction results from two models (e.g. Gemma + Qwen) for the same
PDF + pass, computing agreement levels and flagging conflicts.

Model-agnostic: works with any two model names.
"""

from __future__ import annotations

import re
from difflib import SequenceMatcher


# ── Similarity helpers ───────────────────────────────────────────────────

def _normalize(text: str | None) -> str:
    """Normalize text for comparison: lowercase, strip, collapse whitespace."""
    if not text:
        return ""
    return re.sub(r"\s+", " ", str(text).strip().lower())


def _similarity(a: str | None, b: str | None) -> float:
    """String similarity ratio (0-1)."""
    na, nb = _normalize(a), _normalize(b)
    if not na and not nb:
        return 1.0
    if not na or not nb:
        return 0.0
    return SequenceMatcher(None, na, nb).ratio()


def _tag_key(tag: str | None) -> str:
    """Normalize equipment/pipe tag for matching (strip spaces, uppercase)."""
    if not tag:
        return ""
    return re.sub(r"[\s\-_/]", "", str(tag).upper())


# ── Per-pass merging ────────────────────────────────────────────────────

def merge_pass1_overview(model_a: dict | None, model_b: dict | None,
                         name_a: str = "model_a", name_b: str = "model_b") -> dict:
    """Merge document overview results."""
    if not model_a and not model_b:
        return {"merged": {}, "consensus": 0.0, "sources": []}

    a = model_a or {}
    b = model_b or {}

    merged = {}
    conflicts = []

    # Simple fields — prefer longer/more detailed value
    for field in ["document_type", "title", "revision", "date", "scope", "notes"]:
        av, bv = a.get(field), b.get(field)
        if av and bv:
            if _similarity(av, bv) > 0.7:
                merged[field] = av if len(str(av)) >= len(str(bv)) else bv
            else:
                merged[field] = av  # Prefer model_a for ambiguous cases
                conflicts.append({"field": field, name_a: av, name_b: bv})
        else:
            merged[field] = av or bv

    # List fields — union with dedup
    for field in ["standards_referenced", "equipment_mentioned", "key_topics"]:
        a_list = a.get(field, []) or []
        b_list = b.get(field, []) or []
        seen = set()
        combined = []
        for item in a_list + b_list:
            key = _normalize(item)
            if key and key not in seen:
                seen.add(key)
                combined.append(item)
        merged[field] = combined

    # Sections — merge by section number
    a_sections = {str(s.get("number") or ""): s for s in (a.get("sections") or [])}
    b_sections = {str(s.get("number") or ""): s for s in (b.get("sections") or [])}
    all_nums = set(a_sections) | set(b_sections)
    merged["sections"] = []
    for num in sorted(all_nums, key=lambda x: str(x or "")):
        merged["sections"].append(a_sections.get(num) or b_sections.get(num))

    # Boolean fields
    for field in ["has_drawings", "has_tables", "has_pid"]:
        av, bv = a.get(field), b.get(field)
        merged[field] = (av or False) or (bv or False)

    # Consensus score
    agreement_count = 0
    total_fields = 0
    for field in ["document_type", "title", "revision", "date"]:
        av, bv = a.get(field), b.get(field)
        if av or bv:
            total_fields += 1
            if av and bv and _similarity(av, bv) > 0.7:
                agreement_count += 1

    consensus = agreement_count / total_fields if total_fields > 0 else 0.0

    return {
        "merged": merged,
        "consensus": round(consensus, 2),
        "conflicts": conflicts,
        "sources": _build_sources(model_a, model_b, name_a, name_b),
    }


def merge_pass2_equipment(model_a: dict | None, model_b: dict | None,
                          name_a: str = "model_a", name_b: str = "model_b") -> dict:
    """Merge equipment & tags results."""
    if not model_a and not model_b:
        return {"merged": {}, "consensus": 0.0, "sources": []}

    a = model_a or {}
    b = model_b or {}

    # Merge equipment by tag
    a_equip = {_tag_key(e.get("tag")): e for e in (a.get("equipment") or [])}
    b_equip = {_tag_key(e.get("tag")): e for e in (b.get("equipment") or [])}

    all_tags = set(a_equip) | set(b_equip)
    merged_equipment = []
    agreement = 0
    total = 0

    for tag in sorted(all_tags):
        if not tag:
            continue
        ae = a_equip.get(tag)
        be = b_equip.get(tag)
        total += 1

        if ae and be:
            agreement += 1
            merged_e = _merge_dicts(ae, be)
            merged_e["_found_by"] = [name_a, name_b]
            merged_equipment.append(merged_e)
        else:
            item = ae or be
            item["_found_by"] = [name_a if ae else name_b]
            merged_equipment.append(item)

    # Merge pipe references by ref
    a_pipes = {_tag_key(p.get("ref")): p for p in (a.get("pipe_references") or [])}
    b_pipes = {_tag_key(p.get("ref")): p for p in (b.get("pipe_references") or [])}
    all_refs = set(a_pipes) | set(b_pipes)
    merged_pipes = []
    for ref in sorted(all_refs):
        if not ref:
            continue
        ap = a_pipes.get(ref)
        bp = b_pipes.get(ref)
        if ap and bp:
            merged_pipes.append(_merge_dicts(ap, bp))
        else:
            merged_pipes.append(ap or bp)

    # KKS codes — union
    a_kks = set(_tag_key(k) for k in (a.get("kks_codes") or []))
    b_kks = set(_tag_key(k) for k in (b.get("kks_codes") or []))
    all_kks = sorted(a_kks | b_kks)

    # Instruments — union by tag
    a_inst = {_tag_key(i.get("tag")): i for i in (a.get("instruments") or [])}
    b_inst = {_tag_key(i.get("tag")): i for i in (b.get("instruments") or [])}
    merged_inst = []
    for tag in sorted(set(a_inst) | set(b_inst)):
        if not tag:
            continue
        ai = a_inst.get(tag)
        bi = b_inst.get(tag)
        merged_inst.append(_merge_dicts(ai, bi) if ai and bi else (ai or bi))

    consensus = agreement / total if total > 0 else 0.0

    return {
        "merged": {
            "equipment": merged_equipment,
            "pipe_references": merged_pipes,
            "kks_codes": all_kks,
            "instruments": merged_inst,
        },
        "consensus": round(consensus, 2),
        "sources": _build_sources(model_a, model_b, name_a, name_b),
    }


def merge_pass3_specs(model_a: dict | None, model_b: dict | None,
                      name_a: str = "model_a", name_b: str = "model_b") -> dict:
    """Merge specifications & rules results."""
    if not model_a and not model_b:
        return {"merged": {}, "consensus": 0.0, "sources": []}

    a = model_a or {}
    b = model_b or {}

    # Merge rules by id
    a_rules = {_normalize(r.get("id")): r for r in (a.get("rules") or [])}
    b_rules = {_normalize(r.get("id")): r for r in (b.get("rules") or [])}
    all_ids = set(a_rules) | set(b_rules)
    merged_rules = []
    conflicts = []
    agreement = 0
    total = 0

    for rid in sorted(all_ids):
        if not rid:
            continue
        ar = a_rules.get(rid)
        br = b_rules.get(rid)
        total += 1

        if ar and br:
            av = ar.get("value")
            bv = br.get("value")
            if av is not None and bv is not None:
                if av == bv:
                    agreement += 1
                else:
                    conflicts.append({
                        "rule_id": rid,
                        f"{name_a}_value": av,
                        f"{name_b}_value": bv,
                        f"{name_a}_unit": ar.get("unit"),
                        f"{name_b}_unit": br.get("unit"),
                    })
            else:
                agreement += 1
            merged_rules.append(_merge_dicts(ar, br))
        else:
            merged_rules.append(ar or br)

    # Material specs — union by material name
    merged_materials = _merge_lists_by_key(
        a.get("material_specs", []), b.get("material_specs", []), "material"
    )

    # Pressure ratings — union
    merged_pressure = _merge_lists_by_key(
        a.get("pressure_ratings", []), b.get("pressure_ratings", []), "class"
    )

    # Dimensional data — union
    merged_dims = _merge_lists_by_key(
        a.get("dimensional_data", []), b.get("dimensional_data", []), "component"
    )

    # Insulation — union by service
    merged_insulation = _merge_lists_by_key(
        a.get("insulation", []), b.get("insulation", []), "service"
    )

    consensus = agreement / total if total > 0 else 0.0

    return {
        "merged": {
            "rules": merged_rules,
            "material_specs": merged_materials,
            "pressure_ratings": merged_pressure,
            "dimensional_data": merged_dims,
            "insulation": merged_insulation,
        },
        "consensus": round(consensus, 2),
        "conflicts": conflicts,
        "sources": _build_sources(model_a, model_b, name_a, name_b),
    }


def merge_pass4_tables(model_a: dict | None, model_b: dict | None,
                       name_a: str = "model_a", name_b: str = "model_b") -> dict:
    """Merge table extraction results."""
    if not model_a and not model_b:
        return {"merged": {}, "consensus": 0.0, "sources": []}

    a = model_a or {}
    b = model_b or {}

    a_tables = a.get("tables") or []
    b_tables = b.get("tables") or []

    # Match tables by title similarity
    merged_tables = []
    b_matched = set()
    agreement = 0
    total = 0

    for at in a_tables:
        total += 1
        best_match = None
        best_sim = 0.0
        for j, bt in enumerate(b_tables):
            if j in b_matched:
                continue
            sim = _similarity(at.get("title"), bt.get("title"))
            if sim > best_sim:
                best_sim = sim
                best_match = (j, bt)

        if best_match and best_sim > 0.5:
            j, bt = best_match
            b_matched.add(j)
            agreement += 1
            # Prefer table with more rows
            a_rows = len(at.get("rows", []))
            b_rows = len(bt.get("rows", []))
            winner = at if a_rows >= b_rows else bt
            winner["_found_by"] = [name_a, name_b]
            winner["_alt_row_count"] = min(a_rows, b_rows)
            merged_tables.append(winner)
        else:
            at["_found_by"] = [name_a]
            merged_tables.append(at)

    # Add unmatched model_b tables
    for j, bt in enumerate(b_tables):
        if j not in b_matched:
            total += 1
            bt["_found_by"] = [name_b]
            merged_tables.append(bt)

    consensus = agreement / total if total > 0 else 0.0

    return {
        "merged": {"tables": merged_tables},
        "consensus": round(consensus, 2),
        "sources": _build_sources(model_a, model_b, name_a, name_b),
    }


def merge_pass5_connections(model_a: dict | None, model_b: dict | None,
                            name_a: str = "model_a", name_b: str = "model_b") -> dict:
    """Merge connections & flow results."""
    if not model_a and not model_b:
        return {"merged": {}, "consensus": 0.0, "sources": []}

    a = model_a or {}
    b = model_b or {}

    # Connections — match by pipe_ref
    a_conns = a.get("connections") or []
    b_conns = b.get("connections") or []
    merged_conns = _merge_connections(a_conns, b_conns, name_a, name_b)

    # System boundaries — union
    merged_boundaries = _merge_lists_by_key(
        a.get("system_boundaries", []), b.get("system_boundaries", []), "system"
    )

    # Nozzle connections — match by equipment + nozzle
    a_nozzles = a.get("nozzle_connections") or []
    b_nozzles = b.get("nozzle_connections") or []
    a_noz_map = {}
    for n in a_nozzles:
        key = _tag_key(n.get("equipment", "")) + "_" + _tag_key(n.get("nozzle", ""))
        a_noz_map[key] = n
    b_noz_map = {}
    for n in b_nozzles:
        key = _tag_key(n.get("equipment", "")) + "_" + _tag_key(n.get("nozzle", ""))
        b_noz_map[key] = n

    merged_nozzles = []
    agreement = 0
    total = 0
    for key in sorted(set(a_noz_map) | set(b_noz_map)):
        an = a_noz_map.get(key)
        bn = b_noz_map.get(key)
        total += 1
        if an and bn:
            agreement += 1
            merged_nozzles.append(_merge_dicts(an, bn))
        else:
            merged_nozzles.append(an or bn)

    # Flow paths — union
    a_flows = a.get("flow_paths") or []
    b_flows = b.get("flow_paths") or []
    merged_flows = a_flows + [f for f in b_flows if f not in a_flows]

    # Inline valves — union by tag
    merged_valves = _merge_lists_by_key(
        a.get("valves_inline", []), b.get("valves_inline", []), "tag"
    )

    total = max(total, 1)
    consensus = agreement / total if total > 0 else 0.0

    return {
        "merged": {
            "connections": merged_conns,
            "system_boundaries": merged_boundaries,
            "nozzle_connections": merged_nozzles,
            "flow_paths": merged_flows,
            "valves_inline": merged_valves,
        },
        "consensus": round(consensus, 2),
        "sources": _build_sources(model_a, model_b, name_a, name_b),
    }


# ── Main merge dispatcher ──────────────────────────────────────────────

_MERGE_FUNCTIONS = {
    1: merge_pass1_overview,
    2: merge_pass2_equipment,
    3: merge_pass3_specs,
    4: merge_pass4_tables,
    5: merge_pass5_connections,
}


def merge_results(
    pass_num: int,
    model_a_result: dict | None,
    model_b_result: dict | None,
    name_a: str = "gemma",
    name_b: str = "qwen",
) -> dict:
    """Merge results from two models for the same PDF + pass.

    Args:
        pass_num: Pass number (1-5)
        model_a_result: Parsed JSON from first model (or None)
        model_b_result: Parsed JSON from second model (or None)
        name_a: Short name for first model
        name_b: Short name for second model

    Returns:
        {merged: {...}, consensus: float, conflicts: [...], sources: [...]}
    """
    merge_fn = _MERGE_FUNCTIONS.get(pass_num)
    if not merge_fn:
        raise ValueError(f"No merge function for pass {pass_num}")
    return merge_fn(model_a_result, model_b_result, name_a, name_b)


# ── Helper functions ────────────────────────────────────────────────────

def _build_sources(model_a: dict | None, model_b: dict | None,
                   name_a: str = "model_a", name_b: str = "model_b") -> list[str]:
    sources = []
    if model_a:
        sources.append(name_a)
    if model_b:
        sources.append(name_b)
    return sources


def _merge_dicts(a: dict | None, b: dict | None) -> dict:
    """Merge two dicts, preferring non-null/longer values from a."""
    if not a:
        return dict(b) if b else {}
    if not b:
        return dict(a)

    merged = {}
    for key in set(a) | set(b):
        av = a.get(key)
        bv = b.get(key)

        if av is None:
            merged[key] = bv
        elif bv is None:
            merged[key] = av
        elif isinstance(av, list) and isinstance(bv, list):
            merged[key] = av if len(av) >= len(bv) else bv
        elif isinstance(av, dict) and isinstance(bv, dict):
            merged[key] = _merge_dicts(av, bv)
        elif isinstance(av, str) and isinstance(bv, str):
            merged[key] = av if len(av) >= len(bv) else bv
        else:
            merged[key] = av  # Default: prefer first model

    return merged


def _merge_lists_by_key(
    list_a: list[dict], list_b: list[dict], key_field: str
) -> list[dict]:
    """Merge two lists of dicts by a key field."""
    a_map = {_normalize(item.get(key_field)): item for item in (list_a or [])}
    b_map = {_normalize(item.get(key_field)): item for item in (list_b or [])}

    merged = []
    for key in sorted(set(a_map) | set(b_map)):
        if not key:
            continue
        ai = a_map.get(key)
        bi = b_map.get(key)
        merged.append(_merge_dicts(ai, bi) if ai and bi else (ai or bi))
    return merged


def _merge_connections(a_conns: list[dict], b_conns: list[dict],
                       name_a: str = "model_a", name_b: str = "model_b") -> list[dict]:
    """Merge connection lists, matching by pipe_ref."""
    a_map = {}
    for conn in (a_conns or []):
        key = _tag_key(conn.get("pipe_ref", ""))
        if key:
            a_map[key] = conn

    b_map = {}
    for conn in (b_conns or []):
        key = _tag_key(conn.get("pipe_ref", ""))
        if key:
            b_map[key] = conn

    merged = []
    for key in sorted(set(a_map) | set(b_map)):
        ac = a_map.get(key)
        bc = b_map.get(key)
        if ac and bc:
            m = _merge_dicts(ac, bc)
            m["_found_by"] = [name_a, name_b]
            merged.append(m)
        else:
            item = ac or bc
            item["_found_by"] = [name_a if ac else name_b]
            merged.append(item)
    return merged
