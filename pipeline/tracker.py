"""Master document tracker — aggregates all extractions into a unified registry.

Collects items from all PDFs × passes × models, deduplicates, scores consensus,
and exports JSON + Markdown reports.
"""

from __future__ import annotations

import json
from collections import defaultdict
from datetime import datetime, timezone
from pathlib import Path


class MasterTracker:
    """Aggregates extraction results into a master registry."""

    def __init__(self):
        self.items: dict[str, dict[str, dict]] = defaultdict(dict)
        # items[category][item_id] = {data, sources, ...}
        self.metadata = {
            "generated": "",
            "pdfs_processed": 0,
            "total_pages": 0,
            "models_used": [],
            "total_api_calls": 0,
            "total_cost_usd": 0.0,
        }
        self._pdf_set: set[str] = set()
        self._model_set: set[str] = set()
        self._api_calls = 0
        self._total_cost = 0.0

    # ── Ingestion ───────────────────────────────────────────────────────

    def record_api_call(self, model: str, cost_usd: float):
        """Track an API call for metadata."""
        self._api_calls += 1
        self._total_cost += cost_usd
        self._model_set.add(model)

    def ingest_merged(self, pdf_stem: str, pass_num: int, merged: dict):
        """Ingest merged results from a single PDF + pass."""
        self._pdf_set.add(pdf_stem)
        data = merged.get("merged", {})
        consensus = merged.get("consensus", 0.0)
        sources = merged.get("sources", [])

        if pass_num == 1:
            self._ingest_overview(pdf_stem, data, consensus, sources)
        elif pass_num == 2:
            self._ingest_equipment(pdf_stem, data, consensus, sources)
        elif pass_num == 3:
            self._ingest_specs(pdf_stem, data, consensus, sources)
        elif pass_num == 4:
            self._ingest_tables(pdf_stem, data, consensus, sources)
        elif pass_num == 5:
            self._ingest_connections(pdf_stem, data, consensus, sources)

    def _ingest_overview(self, pdf: str, data: dict, consensus: float, sources: list):
        key = pdf
        self._upsert("documents", key, {
            "pdf": pdf,
            "document_type": data.get("document_type"),
            "title": data.get("title"),
            "revision": data.get("revision"),
            "date": data.get("date"),
            "scope": data.get("scope"),
            "has_drawings": data.get("has_drawings", False),
            "has_tables": data.get("has_tables", False),
            "has_pid": data.get("has_pid", False),
            "sections": data.get("sections", []),
            "key_topics": data.get("key_topics", []),
        }, pdf, 1, consensus, sources)

        # Track standards
        for std in data.get("standards_referenced", []):
            std_key = std.strip().upper()
            if std_key:
                self._upsert("standards", std_key, {
                    "standard": std,
                }, pdf, 1, consensus, sources)

        # Track equipment mentions
        for tag in data.get("equipment_mentioned", []):
            tag_key = _normalize_tag(tag)
            if tag_key:
                self._upsert("equipment", tag_key, {
                    "tag": tag,
                }, pdf, 1, consensus, sources)

    def _ingest_equipment(self, pdf: str, data: dict, consensus: float, sources: list):
        for equip in data.get("equipment", []):
            tag = equip.get("tag") or equip.get("kks", "")
            tag_key = _normalize_tag(tag)
            if not tag_key:
                continue
            self._upsert("equipment", tag_key, equip, pdf, 2, consensus, sources)

        for pipe in data.get("pipe_references", []):
            ref = pipe.get("ref", "")
            ref_key = _normalize_tag(ref)
            if ref_key:
                self._upsert("pipe_references", ref_key, pipe, pdf, 2, consensus, sources)

        for kks in data.get("kks_codes", []):
            kks_key = _normalize_tag(kks)
            if kks_key:
                self._upsert("kks_codes", kks_key, {"kks": kks}, pdf, 2, consensus, sources)

        for inst in data.get("instruments", []):
            tag = inst.get("tag", "")
            tag_key = _normalize_tag(tag)
            if tag_key:
                self._upsert("instruments", tag_key, inst, pdf, 2, consensus, sources)

    def _ingest_specs(self, pdf: str, data: dict, consensus: float, sources: list):
        for rule in data.get("rules", []):
            rid = rule.get("id", "")
            if rid:
                self._upsert("design_rules", rid.upper(), rule, pdf, 3, consensus, sources)

        for mat in data.get("material_specs", []):
            key = mat.get("material", "")
            if key:
                self._upsert("materials", key.upper(), mat, pdf, 3, consensus, sources)

        for pr in data.get("pressure_ratings", []):
            key = f"{pr.get('class', '')}_{pr.get('temp_c', '')}"
            self._upsert("pressure_ratings", key, pr, pdf, 3, consensus, sources)

        for dim in data.get("dimensional_data", []):
            key = f"{dim.get('component', '')}_{dim.get('dimension', '')}_{dim.get('nps', '')}"
            self._upsert("dimensions", key, dim, pdf, 3, consensus, sources)

        for ins in data.get("insulation", []):
            key = ins.get("service", "")
            if key:
                self._upsert("insulation", key.upper(), ins, pdf, 3, consensus, sources)

    def _ingest_tables(self, pdf: str, data: dict, consensus: float, sources: list):
        for table in data.get("tables", []):
            title = table.get("title", "")
            tid = table.get("table_id", "")
            key = f"{pdf}_{tid}_{title[:40]}" if tid else f"{pdf}_{title[:50]}"
            self._upsert("tables", key, table, pdf, 4, consensus, sources)

    def _ingest_connections(self, pdf: str, data: dict, consensus: float, sources: list):
        for conn in data.get("connections", []):
            ref = conn.get("pipe_ref", "")
            key = _normalize_tag(ref) if ref else f"{conn.get('from_equipment', '')}_{conn.get('to_equipment', '')}"
            if key:
                self._upsert("connections", key, conn, pdf, 5, consensus, sources)

        for noz in data.get("nozzle_connections", []):
            key = f"{_normalize_tag(noz.get('equipment', ''))}_{noz.get('nozzle', '')}"
            if key.strip("_"):
                self._upsert("nozzle_connections", key, noz, pdf, 5, consensus, sources)

        for flow in data.get("flow_paths", []):
            path_str = "→".join(flow.get("path", []))
            if path_str:
                self._upsert("flow_paths", path_str, flow, pdf, 5, consensus, sources)

        for valve in data.get("valves_inline", []):
            tag = valve.get("tag", "")
            tag_key = _normalize_tag(tag)
            if tag_key:
                self._upsert("valves_inline", tag_key, valve, pdf, 5, consensus, sources)

    def _upsert(
        self,
        category: str,
        item_id: str,
        data: dict,
        pdf: str,
        pass_num: int,
        consensus: float,
        sources: list,
    ):
        """Insert or update an item in the tracker."""
        if item_id not in self.items[category]:
            self.items[category][item_id] = {
                "data": data,
                "sources": [],
                "consensus_scores": [],
            }
        entry = self.items[category][item_id]
        # Merge data (newer overwrites, but keep existing non-null values)
        for k, v in data.items():
            if v is not None and (k not in entry["data"] or entry["data"][k] is None):
                entry["data"][k] = v
        entry["sources"].append({
            "pdf": pdf,
            "pass": pass_num,
            "models": sources,
            "consensus": consensus,
        })
        entry["consensus_scores"].append(consensus)

    # ── Export ──────────────────────────────────────────────────────────

    def finalize_metadata(self, total_pages: int = 0):
        """Update metadata before export."""
        self.metadata.update({
            "generated": datetime.now(timezone.utc).isoformat(),
            "pdfs_processed": len(self._pdf_set),
            "total_pages": total_pages,
            "models_used": sorted(self._model_set),
            "total_api_calls": self._api_calls,
            "total_cost_usd": round(self._total_cost, 2),
        })

    def export_json(self, path: Path):
        """Export master tracker as JSON."""
        output = {
            "metadata": self.metadata,
            "categories": {},
        }

        for category, items in sorted(self.items.items()):
            cat_items = []
            for item_id, entry in sorted(items.items()):
                scores = entry["consensus_scores"]
                avg_consensus = sum(scores) / len(scores) if scores else 0.0
                n_sources = len(entry["sources"])
                n_models = len(set(
                    m for s in entry["sources"] for m in s.get("models", [])
                ))
                status = (
                    "confirmed" if n_models >= 2 and avg_consensus > 0.5
                    else "single_source" if n_models == 1
                    else "low_consensus"
                )
                cat_items.append({
                    "id": item_id,
                    "status": status,
                    "consensus": round(avg_consensus, 2),
                    "source_count": n_sources,
                    "model_count": n_models,
                    "pdfs": sorted(set(s["pdf"] for s in entry["sources"])),
                    "data": entry["data"],
                })

            output["categories"][category] = {
                "total": len(cat_items),
                "confirmed": sum(1 for i in cat_items if i["status"] == "confirmed"),
                "single_source": sum(1 for i in cat_items if i["status"] == "single_source"),
                "items": cat_items,
            }

        path.parent.mkdir(parents=True, exist_ok=True)
        with open(path, "w") as f:
            json.dump(output, f, indent=2, default=str)

        total_items = sum(len(items) for items in self.items.values())
        print(f"  Master tracker: {total_items} items across {len(self.items)} categories → {path}")

    def export_markdown(self, path: Path):
        """Export extraction report as Markdown."""
        lines = []
        lines.append("# PDF Deep Extraction Report")
        lines.append(f"Generated: {self.metadata['generated'][:10]} | "
                     f"Models: {', '.join(self.metadata['models_used'])} | "
                     f"Cost: ${self.metadata['total_cost_usd']:.2f}")
        lines.append("")

        # Summary
        total_items = sum(len(items) for items in self.items.values())
        total_confirmed = sum(
            1 for items in self.items.values()
            for entry in items.values()
            if len(set(m for s in entry["sources"] for m in s.get("models", []))) >= 2
        )
        lines.append("## Summary")
        lines.append(f"- {self.metadata['pdfs_processed']} PDFs processed "
                     f"({self.metadata['total_pages']} pages)")
        lines.append(f"- {self.metadata['total_api_calls']} API calls")
        lines.append(f"- {total_items} items extracted across {len(self.items)} categories")
        if total_items > 0:
            lines.append(f"- {total_confirmed}/{total_items} confirmed by both models "
                         f"({100*total_confirmed/total_items:.0f}%)")
        lines.append("")

        # Category table
        lines.append("## Items by Category")
        lines.append("")
        lines.append("| Category | Total | Confirmed | Single Source |")
        lines.append("|----------|------:|----------:|-------------:|")
        for category in sorted(self.items):
            items = self.items[category]
            total = len(items)
            confirmed = sum(
                1 for entry in items.values()
                if len(set(m for s in entry["sources"] for m in s.get("models", []))) >= 2
            )
            single = total - confirmed
            lines.append(f"| {category} | {total} | {confirmed} | {single} |")
        lines.append("")

        # Per-PDF coverage
        pdf_stats: dict[str, dict[str, int]] = defaultdict(lambda: defaultdict(int))
        for category, items in self.items.items():
            for entry in items.values():
                for source in entry["sources"]:
                    pdf_stats[source["pdf"]][category] += 1

        if pdf_stats:
            lines.append("## Coverage by PDF")
            lines.append("")
            cats = sorted(self.items.keys())
            header = "| PDF | " + " | ".join(c[:12] for c in cats) + " | Total |"
            sep = "|-----|" + "|".join("-" * max(5, len(c[:12])) + ":" for c in cats) + "|------:|"
            lines.append(header)
            lines.append(sep)
            for pdf in sorted(pdf_stats):
                counts = [str(pdf_stats[pdf].get(c, 0)) for c in cats]
                total = sum(pdf_stats[pdf].values())
                lines.append(f"| {pdf[:30]} | " + " | ".join(counts) + f" | {total} |")
            lines.append("")

        # Equipment details
        if "equipment" in self.items:
            lines.append("## Equipment Registry")
            lines.append("")
            for item_id, entry in sorted(self.items["equipment"].items()):
                data = entry["data"]
                tag = data.get("tag", item_id)
                etype = data.get("type", "unknown")
                desc = data.get("description", "")
                n_models = len(set(
                    m for s in entry["sources"] for m in s.get("models", [])
                ))
                icon = "V" if n_models >= 2 else "?"
                pdfs = sorted(set(s["pdf"] for s in entry["sources"]))
                lines.append(f"- [{icon}] **{tag}** ({etype}) — {desc}")
                if pdfs:
                    lines.append(f"  Found in: {', '.join(pdfs)}")
            lines.append("")

        # Design rules
        if "design_rules" in self.items:
            lines.append("## Design Rules")
            lines.append("")
            for item_id, entry in sorted(self.items["design_rules"].items()):
                data = entry["data"]
                val = data.get("value")
                unit = data.get("unit", "")
                desc = data.get("description", "")
                ref = data.get("reference", "")
                val_str = f" = {val} {unit}" if val is not None else ""
                lines.append(f"- **{item_id}**{val_str} — {desc} [{ref}]")
            lines.append("")

        # Connections
        if "connections" in self.items:
            lines.append("## Pipe Connections")
            lines.append("")
            for item_id, entry in sorted(self.items["connections"].items()):
                data = entry["data"]
                fr = data.get("from_equipment", "?")
                to = data.get("to_equipment", "?")
                ref = data.get("pipe_ref", "")
                nps = data.get("nps", "")
                lines.append(f"- **{ref}** ({nps}): {fr} -> {to}")
            lines.append("")

        # Tables summary
        if "tables" in self.items:
            lines.append("## Tables Extracted")
            lines.append("")
            lines.append(f"Total: {len(self.items['tables'])} tables")
            lines.append("")
            for item_id, entry in sorted(self.items["tables"].items()):
                data = entry["data"]
                title = data.get("title", "Untitled")
                rows = data.get("row_count", len(data.get("rows", [])))
                cols = data.get("col_count", len(data.get("headers", [])))
                lines.append(f"- **{title}** ({rows} rows x {cols} cols)")
            lines.append("")

        # Footer
        lines.append("---")
        lines.append("Generated with multi-model extraction pipeline")
        lines.append("")

        path.parent.mkdir(parents=True, exist_ok=True)
        path.write_text("\n".join(lines))
        print(f"  Extraction report → {path}")


# ── Helpers ─────────────────────────────────────────────────────────────

def _normalize_tag(tag: str | None) -> str:
    """Normalize an equipment/pipe tag for deduplication."""
    if not tag:
        return ""
    import re
    return re.sub(r"[\s\-_/]", "", str(tag).upper())
