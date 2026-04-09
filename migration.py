"""
Migrate a Supabase Postgres hospital database to MongoDB.

Follows the 5-step RDBMS → Document Database conversion process:
  Step 1 – Each table becomes a MongoDB collection.
  Step 2 – Primary key becomes the document _id (e.g. patient_id → _id).
  Step 3 – All fields combined into a single self-contained document.
  Step 4 – Related rows embedded as nested arrays (no joins needed in Mongo).
  Step 5 – Documents inserted into their collections.

Collections produced:
  Patients     – one doc per patient, with physician_visits embedded and
                 an admission_ids reference array (admissions accessed separately)
  Admissions   – one doc per admission, with diagnoses, clinical_notes,
                 radiology_exams, surgical_procedures, and icu_ccu_stays embedded
                 (icu_ccu_stays further embed their transfers)
  IcdDictionary – standalone lookup table, composite _id {icd_code, icd_version}

Usage:
    python migration.py
"""

import os
from datetime import date, datetime, time
from decimal import Decimal
from pathlib import Path
from uuid import UUID

import psycopg
from psycopg.rows import dict_row
import pymongo


# ── .env loader ────────────────────────────────────────────────────────────────

def load_env_files():
    """Load key=value pairs from .env / .env.local if they exist."""
    for filename in (".env", ".env.local"):
        path = Path(filename)
        if not path.exists() or not path.is_file():
            continue
        for raw_line in path.read_text(encoding="utf-8").splitlines():
            line = raw_line.strip()
            if not line or line.startswith("#") or "=" not in line:
                continue
            key, value = line.split("=", 1)
            key   = key.strip()
            value = value.strip().strip('"').strip("'")
            if key and key not in os.environ:
                os.environ[key] = value


# ── Connection settings ────────────────────────────────────────────────────────

def get_postgres_config():
    database_url = os.getenv("DATABASE_URL") or os.getenv("URL") or os.getenv("PG_DSN")
    if database_url:
        if database_url.startswith("jdbc:"):
            database_url = database_url.removeprefix("jdbc:")
        return {"conninfo": database_url}

    return {
        "host":     os.getenv("SUPABASE_DB_HOST") or os.getenv("PG_HOST") or "db.pudtseovfzxuwltaphpp.supabase.co",
        "port":     int(os.getenv("SUPABASE_DB_PORT") or os.getenv("PG_PORT") or 5432),
        "dbname":   os.getenv("SUPABASE_DB_NAME") or os.getenv("PG_DB") or "postgres",
        "user":     os.getenv("SUPABASE_DB_USER") or os.getenv("PG_USER") or "postgres",
        "password": os.getenv("SUPABASE_DB_PASSWORD") or os.getenv("PG_PASSWORD") or "",
        "sslmode":  "require",
    }

def get_mongo_config():
    uri = os.getenv("MONGODB_URI") or os.getenv("MONGO_URI") or "mongodb://localhost:27017"
    db_name = os.getenv("MONGODB_DATABASE") or os.getenv("MONGO_DB") or "hospital"
    return uri, db_name


# ── Serialization helpers ──────────────────────────────────────────────────────

def serialize(value):
    """Convert Postgres types that MongoDB cannot store natively."""
    if isinstance(value, Decimal):
        return float(value)
    if isinstance(value, UUID):
        return str(value)
    if isinstance(value, date) and not isinstance(value, datetime):
        return datetime.combine(value, time.min)
    if isinstance(value, (bytes, bytearray)):
        return value.hex()
    if isinstance(value, list):
        return [serialize(v) for v in value]
    if isinstance(value, dict):
        return {k: serialize(v) for k, v in value.items()}
    return value

def serialize_row(row):
    return {k: serialize(v) for k, v in row.items()}


# ── Postgres helpers ───────────────────────────────────────────────────────────

def fetch_all(conn, query, params=()):
    """Run a query and return all rows as dicts."""
    with conn.cursor(row_factory=dict_row) as cur:
        cur.execute(query, params)
        return [serialize_row(r) for r in cur.fetchall()]


# ── Step 3 & 4: Build self-contained documents with embedded sub-documents ─────

def group_by(rows, key):
    """Index a list of row dicts by a given field for O(1) lookup."""
    index = {}
    for row in rows:
        index.setdefault(row[key], []).append(row)
    return index


def build_admission_documents(conn):
    """
    Step 1: admission table → Admissions collection (top-level, accessed by ID).

    Step 3 – Admission document structure:
    {
        _id: <admission_id>,            ← Step 2: PK becomes document key
        admission_id: ...,
        patient_id: ...,                ← reference back to patient
        ...,
        diagnoses:           [...],     ← Step 4: embedded sub-documents
        clinical_notes:      [...],
        radiology_exams:     [...],
        surgical_procedures: [...],
        icu_ccu_stays: [
            { ..., transfers: [...] }   ← transfers embedded inside each stay
        ]
    }
    """
    admissions     = fetch_all(conn, "SELECT * FROM public.admission")
    diagnoses      = fetch_all(conn, "SELECT * FROM public.diagnosis")
    clinical_notes = fetch_all(conn, "SELECT * FROM public.clinical_note")
    radiology_exams= fetch_all(conn, "SELECT * FROM public.radiology_exam")
    surgical_procs = fetch_all(conn, "SELECT * FROM public.surgical_procedure")
    icu_stays      = fetch_all(conn, "SELECT * FROM public.icu_ccu_stay")
    icu_transfers  = fetch_all(conn, "SELECT * FROM public.icu_ccu_transfers")

    diagnoses_by_admission = group_by(diagnoses,       "admission_id")
    notes_by_admission     = group_by(clinical_notes,  "admission_id")
    radiology_by_admission = group_by(radiology_exams, "admission_id")
    surgery_by_admission   = group_by(surgical_procs,  "admission_id")
    stays_by_admission     = group_by(icu_stays,       "admission_id")
    transfers_by_stay      = group_by(icu_transfers,   "stay_id")

    documents = []
    for adm in admissions:
        aid = adm["admission_id"]

        # Embed ICU stays, and inside each stay embed its transfers
        stays = []
        for stay in stays_by_admission.get(aid, []):
            stay["transfers"] = transfers_by_stay.get(stay["stay_id"], [])
            stays.append(stay)

        doc = adm.copy()
        doc["_id"]                  = aid
        doc["diagnoses"]            = diagnoses_by_admission.get(aid, [])
        doc["clinical_notes"]       = notes_by_admission.get(aid, [])
        doc["radiology_exams"]      = radiology_by_admission.get(aid, [])
        doc["surgical_procedures"]  = surgery_by_admission.get(aid, [])
        doc["icu_ccu_stays"]        = stays
        documents.append(doc)

    return documents


def build_patient_documents(conn, admission_docs):
    """
    Step 1: patient table → Patients collection.

    Step 3 – Patient document structure:
    {
        _id: <patient_id>,          ← Step 2: PK becomes document key
        patient_id: ...,
        first_name: ...,
        ...,
        admission_ids: [1, 4, 7],   ← Step 4: reference array (admissions are
                                       their own top-level collection)
        physician_visits: [...]     ← embedded (only accessed through patient)
    }
    """
    patients         = fetch_all(conn, "SELECT * FROM public.patient ORDER BY patient_id")
    physician_visits = fetch_all(conn, "SELECT * FROM public.physician_visit")

    visits_by_patient    = group_by(physician_visits, "patient_id")
    admission_ids_by_patient = group_by(
        [{"patient_id": d["patient_id"], "admission_id": d["admission_id"]} for d in admission_docs],
        "patient_id",
    )

    documents = []
    for patient in patients:
        pid = patient["patient_id"]
        doc = patient.copy()
        doc["_id"]              = pid
        doc["admission_ids"]    = [
            a["admission_id"] for a in admission_ids_by_patient.get(pid, [])
        ]
        doc["physician_visits"] = visits_by_patient.get(pid, [])
        documents.append(doc)

    return documents


def build_icd_dictionary_documents(conn):
    """
    icd_dictionary has a composite PK (icd_code, icd_version).
    Step 2: _id = {icd_code, icd_version} composite key.
    """
    rows = fetch_all(conn, "SELECT * FROM public.icd_dictionary")
    docs = []
    for row in rows:
        doc = row.copy()
        doc["_id"] = {"icd_code": row["icd_code"], "icd_version": row["icd_version"]}
        docs.append(doc)
    return docs


# ── Step 5: Insert documents into MongoDB collections ─────────────────────────

def insert_collection(mongo_db, collection_name, documents):
    if not documents:
        print(f"  {collection_name}: no documents to insert, skipping.")
        return
    mongo_db[collection_name].drop()
    mongo_db[collection_name].insert_many(documents)
    print(f"  {collection_name}: inserted {len(documents)} documents.")


# ── Main ───────────────────────────────────────────────────────────────────────

def main():
    load_env_files()

    postgres_config = get_postgres_config()
    mongo_uri, mongo_db_name = get_mongo_config()

    print("Connecting to Postgres...")
    with psycopg.connect(**postgres_config) as conn:
        print("Building documents...")

        # Step 1: admission table → Admissions collection (top-level, directly queryable)
        admission_docs = build_admission_documents(conn)

        # Step 1: patient table → Patients collection (references admissions by ID)
        patient_docs = build_patient_documents(conn, admission_docs)

        # Step 1: icd_dictionary table → IcdDictionary collection (standalone lookup)
        icd_docs = build_icd_dictionary_documents(conn)

    print("Connecting to MongoDB...")
    client   = pymongo.MongoClient(mongo_uri)
    mongo_db = client[mongo_db_name]

    # Step 5: Insert into collections
    print("Inserting documents...")
    insert_collection(mongo_db, "Admissions",    admission_docs)
    insert_collection(mongo_db, "Patients",      patient_docs)
    insert_collection(mongo_db, "IcdDictionary", icd_docs)

    client.close()
    print("Migration complete.")


if __name__ == "__main__":
    main()
