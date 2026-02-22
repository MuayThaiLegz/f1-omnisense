"""
Migrate data to McLaren_f1 database.

1. Copy f1_knowledge collection from cadai → McLaren_f1  (preserves vector index)
2. Populate telemetry collection from McCar CSV files
3. Populate race_results collection from jolpica JSON
4. Populate anomaly_scores collection from anomaly_scores.json

Usage:
    python pipeline/migrate_to_mclaren_f1.py              # run all
    python pipeline/migrate_to_mclaren_f1.py --only knowledge
    python pipeline/migrate_to_mclaren_f1.py --only telemetry
    python pipeline/migrate_to_mclaren_f1.py --only results
    python pipeline/migrate_to_mclaren_f1.py --only anomaly
"""

import os, sys, json, csv, argparse
from pathlib import Path
from dotenv import load_dotenv
from pymongo import MongoClient

ROOT = Path(__file__).resolve().parent.parent
load_dotenv(ROOT / ".env")

URI = os.environ["MONGODB_URI"]
OLD_DB = "cadai"
NEW_DB = os.environ.get("MONGODB_DB", "McLaren_f1")

client = MongoClient(URI)
old_db = client[OLD_DB]
new_db = client[NEW_DB]


# ── 1. Copy f1_knowledge ──────────────────────────────────────────

def migrate_knowledge():
    src = old_db["f1_knowledge"]
    dst = new_db["f1_knowledge"]

    count = src.count_documents({})
    if count == 0:
        print("  f1_knowledge: source collection is empty, skipping.")
        return

    existing = dst.count_documents({})
    if existing > 0:
        print(f"  f1_knowledge: destination already has {existing} docs — skipping.")
        print("  (drop McLaren_f1.f1_knowledge first if you want a fresh copy)")
        return

    print(f"  f1_knowledge: copying {count} docs from {OLD_DB} → {NEW_DB} ...")
    batch = []
    for doc in src.find():
        doc.pop("_id", None)
        batch.append(doc)
        if len(batch) >= 500:
            dst.insert_many(batch)
            batch.clear()
    if batch:
        dst.insert_many(batch)

    print(f"  f1_knowledge: {dst.count_documents({})} docs copied.")
    print("  NOTE: You must recreate the vector_index in Atlas UI or via")
    print("  AtlasVectorStore.ensure_vector_index() for RAG search to work.")


# ── 2. Telemetry from McCar CSVs ─────────────────────────────────

def migrate_telemetry():
    dst = new_db["telemetry"]
    existing = dst.count_documents({})
    if existing > 0:
        print(f"  telemetry: already has {existing} docs — skipping.")
        return

    car_dir = ROOT / "f1data" / "McCar" / "2024"
    bio_dir = ROOT / "f1data" / "McDriver" / "2024"

    if not car_dir.exists():
        print(f"  telemetry: {car_dir} not found, skipping.")
        return

    total = 0
    for csv_path in sorted(car_dir.glob("*.csv")):
        race_name = csv_path.stem  # e.g. 2024_Bahrain_Grand_Prix_Race
        batch = []
        with open(csv_path, newline="") as f:
            reader = csv.DictReader(f)
            for row in reader:
                # Convert numeric fields
                for key in ("RPM", "Speed", "Throttle", "Distance", "TyreLife", "LapNumber"):
                    if key in row and row[key]:
                        try:
                            row[key] = float(row[key])
                        except ValueError:
                            pass
                for key in ("nGear",):
                    if key in row and row[key]:
                        try:
                            row[key] = int(float(row[key]))
                        except ValueError:
                            pass
                if "Brake" in row:
                    row["Brake"] = row["Brake"] in ("True", "true", "1")
                if "DRS" in row:
                    try:
                        row["DRS"] = int(float(row["DRS"]))
                    except (ValueError, TypeError):
                        pass

                row["_source_file"] = csv_path.name
                batch.append(row)

                if len(batch) >= 2000:
                    dst.insert_many(batch)
                    total += len(batch)
                    batch.clear()

        if batch:
            dst.insert_many(batch)
            total += len(batch)

        print(f"    {csv_path.name}: inserted")

    # Also load biometrics if available
    if bio_dir.exists():
        for csv_path in sorted(bio_dir.glob("*_biometrics.csv")):
            batch = []
            with open(csv_path, newline="") as f:
                reader = csv.DictReader(f)
                for row in reader:
                    row["_source_file"] = csv_path.name
                    row["_data_type"] = "biometrics"
                    batch.append(row)
                    if len(batch) >= 2000:
                        dst.insert_many(batch)
                        total += len(batch)
                        batch.clear()
            if batch:
                dst.insert_many(batch)
                total += len(batch)
            print(f"    {csv_path.name}: inserted")

    print(f"  telemetry: {total} total docs inserted.")

    # Create useful indexes
    dst.create_index([("Driver", 1), ("Race", 1)])
    dst.create_index([("Race", 1), ("LapNumber", 1)])
    print("  telemetry: indexes created (Driver+Race, Race+LapNumber).")


# ── 3. Race results from jolpica JSON ────────────────────────────

def migrate_race_results():
    dst = new_db["race_results"]
    existing = dst.count_documents({})
    if existing > 0:
        print(f"  race_results: already has {existing} docs — skipping.")
        return

    json_path = ROOT / "data" / "other" / "jolpica" / "race_results.json"
    if not json_path.exists():
        print(f"  race_results: {json_path} not found, skipping.")
        return

    with open(json_path) as f:
        races = json.load(f)

    # Each race is a document with embedded results array
    batch = []
    for race in races:
        batch.append(race)
        if len(batch) >= 500:
            dst.insert_many(batch)
            batch.clear()
    if batch:
        dst.insert_many(batch)

    print(f"  race_results: {dst.count_documents({})} race documents inserted.")

    dst.create_index([("season", 1), ("round", 1)])
    dst.create_index("raceName")
    print("  race_results: indexes created (season+round, raceName).")


# ── 4. Anomaly scores from JSON ──────────────────────────────────

def migrate_anomaly_scores():
    dst = new_db["anomaly_scores"]
    existing = dst.count_documents({})
    if existing > 0:
        print(f"  anomaly_scores: already has {existing} docs — skipping.")
        return

    json_path = ROOT / "pipeline" / "output" / "anomaly_scores.json"
    if not json_path.exists():
        print(f"  anomaly_scores: {json_path} not found, skipping.")
        return

    with open(json_path) as f:
        data = json.load(f)

    # Store the whole blob as a single document (versioned snapshot)
    # Plus individual per-driver-per-race docs for querying
    docs = []
    for driver_data in data.get("drivers", []):
        driver = driver_data["driver"]
        code = driver_data["code"]
        number = driver_data["number"]
        overall_health = driver_data["overall_health"]

        for race_entry in driver_data.get("races", []):
            doc = {
                "driver": driver,
                "code": code,
                "number": number,
                "overall_health": overall_health,
                "race": race_entry["race"],
                "systems": race_entry["systems"],
            }
            docs.append(doc)

    if docs:
        dst.insert_many(docs)
    print(f"  anomaly_scores: {len(docs)} driver-race docs inserted.")

    # Also store the full snapshot for metadata
    new_db["anomaly_scores_snapshot"].drop()
    new_db["anomaly_scores_snapshot"].insert_one(data)
    print("  anomaly_scores_snapshot: full JSON stored.")

    dst.create_index([("code", 1), ("race", 1)])
    dst.create_index("number")
    print("  anomaly_scores: indexes created (code+race, number).")


# ── Main ──────────────────────────────────────────────────────────

TASKS = {
    "knowledge": ("f1_knowledge", migrate_knowledge),
    "telemetry": ("telemetry", migrate_telemetry),
    "results":   ("race_results", migrate_race_results),
    "anomaly":   ("anomaly_scores", migrate_anomaly_scores),
}

if __name__ == "__main__":
    parser = argparse.ArgumentParser(description="Migrate data to McLaren_f1 database")
    parser.add_argument("--only", choices=list(TASKS.keys()), help="Run only one migration")
    parser.add_argument("--drop", action="store_true", help="Drop target collections before migrating")
    args = parser.parse_args()

    print(f"Target database: {NEW_DB}")
    print(f"Source database: {OLD_DB} (for f1_knowledge copy)\n")

    if args.only:
        tasks = {args.only: TASKS[args.only]}
    else:
        tasks = TASKS

    if args.drop:
        for key, (coll_name, _) in tasks.items():
            print(f"  Dropping {NEW_DB}.{coll_name} ...")
            new_db[coll_name].drop()
        if "anomaly" in tasks:
            new_db["anomaly_scores_snapshot"].drop()
        print()

    for key, (coll_name, fn) in tasks.items():
        print(f"[{key}]")
        fn()
        print()

    # Summary
    print("── Summary ──")
    for coll_name in ["f1_knowledge", "telemetry", "race_results", "anomaly_scores"]:
        count = new_db[coll_name].count_documents({})
        print(f"  {NEW_DB}.{coll_name}: {count:,} docs")

    client.close()
    print("\nDone.")
