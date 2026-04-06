"""Supabase Postgres utilities for connectivity, health checks, and migration.

Features:
1) Loads environment from .env automatically.
2) Supports URL-based auth (DATABASE_URL/URL) and field-based auth vars.
3) Provides `ping`, `health`, and `migrate` commands.

Examples:
python connectToSupabase.py
python connectToSupabase.py health
python connectToSupabase.py migrate
"""

from __future__ import annotations

import argparse
import importlib
from datetime import date, datetime, time
from decimal import Decimal
import os
import sys
from pathlib import Path
from typing import Any
from uuid import UUID

import psycopg
from psycopg import sql
from psycopg.rows import dict_row


DEFAULT_HOST = "db.pudtseovfzxuwltaphpp.supabase.co"
DEFAULT_PORT = 5432
DEFAULT_DB = "postgres"
DEFAULT_USER = "postgres"
DEFAULT_MONGO_URI = "mongodb://localhost:27017"
DEFAULT_MONGO_DB = "supabase_mirror"


def load_env_files() -> None:
	"""Load key=value pairs from .env files if they exist."""
	for filename in (".env", ".env.local"):
		path = Path(filename)
		if not path.exists() or not path.is_file():
			continue

		for raw_line in path.read_text(encoding="utf-8").splitlines():
			line = raw_line.strip()
			if not line or line.startswith("#") or "=" not in line:
				continue

			key, value = line.split("=", 1)
			key = key.strip()
			value = value.strip().strip('"').strip("'")
			if key and key not in os.environ:
				os.environ[key] = value


def get_connection_config() -> dict[str, object]:
	host = os.getenv("SUPABASE_DB_HOST") or os.getenv("HOST") or DEFAULT_HOST
	port = int(os.getenv("SUPABASE_DB_PORT") or os.getenv("PORT") or DEFAULT_PORT)
	db_name = os.getenv("SUPABASE_DB_NAME") or os.getenv("DATABASE") or DEFAULT_DB
	user = os.getenv("SUPABASE_DB_USER") or os.getenv("USERNAME") or DEFAULT_USER
	password = os.getenv("SUPABASE_DB_PASSWORD") or os.getenv("PASSWORD")

	# Prefer explicit field-based credentials when present.
	if password:
		return {
			"host": host,
			"port": port,
			"dbname": db_name,
			"user": user,
			"password": password,
			"sslmode": "require",
		}

	database_url = os.getenv("DATABASE_URL") or os.getenv("URL")
	if database_url:
		if database_url.startswith("jdbc:"):
			database_url = database_url.removeprefix("jdbc:")
		return {"conninfo": database_url}

	raise RuntimeError(
		"Missing credentials. Set DATABASE_URL/URL or SUPABASE_DB_PASSWORD/PASSWORD."
	)


def test_connection(connection_config: dict[str, object]) -> None:
	# Passing structured fields avoids URL parsing issues with special chars in passwords.
	with psycopg.connect(**connection_config) as conn:
		with conn.cursor() as cur:
			cur.execute("SELECT current_database(), current_user, version();")
			db_name, user_name, version = cur.fetchone()

	print("Connected successfully.")
	print(f"Database: {db_name}")
	print(f"User: {user_name}")
	print(f"Version: {version}")


def health_check(connection_config: dict[str, object]) -> None:
	with psycopg.connect(**connection_config) as conn:
		with conn.cursor() as cur:
			cur.execute(
				"""
				SELECT
					current_database(),
					current_user,
					current_setting('server_version'),
					current_setting('TimeZone')
				"""
			)
			db_name, user_name, version, timezone = cur.fetchone()

			cur.execute(
				"""
				SELECT COUNT(*)
				FROM information_schema.tables
				WHERE table_schema NOT IN ('pg_catalog', 'information_schema')
				"""
			)
			user_table_count = cur.fetchone()[0]

			cur.execute(
				"""
				SELECT EXISTS (
					SELECT 1
					FROM pg_extension
					WHERE extname = 'pgcrypto'
				)
				"""
			)
			has_pgcrypto = cur.fetchone()[0]

	print("Health check passed.")
	print(f"Database: {db_name}")
	print(f"User: {user_name}")
	print(f"Server version: {version}")
	print(f"Timezone: {timezone}")
	print(f"User tables: {user_table_count}")
	print(f"pgcrypto installed: {has_pgcrypto}")


def get_mongo_config() -> tuple[str, str]:
	mongo_uri = os.getenv("MONGODB_URI") or DEFAULT_MONGO_URI
	mongo_db_name = os.getenv("MONGODB_DATABASE") or DEFAULT_MONGO_DB
	return mongo_uri, mongo_db_name


def get_user_tables(conn: psycopg.Connection, schema: str) -> list[str]:
	with conn.cursor() as cur:
		cur.execute(
			"""
			SELECT table_name
			FROM information_schema.tables
			WHERE table_schema = %s
			  AND table_type = 'BASE TABLE'
			ORDER BY table_name
			""",
			(schema,),
		)
		return [row[0] for row in cur.fetchall()]


def get_primary_key_columns(conn: psycopg.Connection, schema: str, table: str) -> list[str]:
	with conn.cursor() as cur:
		cur.execute(
			"""
			SELECT a.attname
			FROM pg_index i
			JOIN pg_class c ON c.oid = i.indrelid
			JOIN pg_namespace n ON n.oid = c.relnamespace
			JOIN pg_attribute a ON a.attrelid = c.oid AND a.attnum = ANY(i.indkey)
			WHERE i.indisprimary
			  AND n.nspname = %s
			  AND c.relname = %s
			ORDER BY array_position(i.indkey, a.attnum)
			""",
			(schema, table),
		)
		return [row[0] for row in cur.fetchall()]


def serialize_value(value: object) -> object:
	if isinstance(value, Decimal):
		return float(value)
	if isinstance(value, UUID):
		return str(value)
	if isinstance(value, date) and not isinstance(value, datetime):
		return datetime.combine(value, time.min)
	if isinstance(value, time):
		return value.isoformat()
	if isinstance(value, bytes):
		return value.hex()
	if isinstance(value, list):
		return [serialize_value(item) for item in value]
	if isinstance(value, dict):
		return {key: serialize_value(item) for key, item in value.items()}
	return value


def serialize_row(row: dict[str, object]) -> dict[str, object]:
	return {key: serialize_value(value) for key, value in row.items()}


def build_mongo_doc(
	row: dict[str, object],
	pk_columns: list[str],
	write_mode: str,
) -> dict[str, object]:
	doc = serialize_row(row)
	if write_mode == "upsert" and pk_columns:
		if len(pk_columns) == 1:
			doc["_id"] = doc.get(pk_columns[0])
		else:
			doc["_id"] = {col: doc.get(col) for col in pk_columns}
	return doc


def flush_batch(
	collection: Any,
	batch: list[dict[str, object]],
	pk_columns: list[str],
	write_mode: str,
) -> int:
	if not batch:
		return 0

	if write_mode == "upsert" and pk_columns:
		ops_written = 0
		for doc in batch:
			collection.replace_one({"_id": doc["_id"]}, doc, upsert=True)
			ops_written += 1
		return ops_written

	result = collection.insert_many(batch, ordered=False)
	return len(result.inserted_ids)


def migrate_table(
	conn: psycopg.Connection,
	mongo_db,
	schema: str,
	table: str,
	batch_size: int,
	truncate_collections: bool,
	write_mode: str,
) -> int:
	collection_name = table if schema == "public" else f"{schema}_{table}"
	collection = mongo_db[collection_name]

	if truncate_collections:
		collection.delete_many({})

	pk_columns = get_primary_key_columns(conn, schema, table)
	rows_copied = 0
	batch: list[dict[str, object]] = []

	with conn.cursor(row_factory=dict_row) as cur:
		query = sql.SQL("SELECT * FROM {}.{}").format(
			sql.Identifier(schema),
			sql.Identifier(table),
		)
		cur.execute(query)
		for row in cur:
			doc = build_mongo_doc(row, pk_columns, write_mode)
			batch.append(doc)
			if len(batch) >= batch_size:
				rows_copied += flush_batch(collection, batch, pk_columns, write_mode)
				batch.clear()

	rows_copied += flush_batch(collection, batch, pk_columns, write_mode)
	return rows_copied


def migrate_all_tables(
	connection_config: dict[str, object],
	schema: str,
	batch_size: int,
	truncate_collections: bool,
	write_mode: str,
) -> None:
	mongo_uri, mongo_db_name = get_mongo_config()
	pymongo_module = importlib.import_module("pymongo")
	mongo_client_class = getattr(pymongo_module, "MongoClient")

	with psycopg.connect(**connection_config) as conn:
		tables = get_user_tables(conn, schema)
		if not tables:
			print(f"No tables found in schema '{schema}'.")
			return

		with mongo_client_class(mongo_uri) as mongo_client:
			mongo_db = mongo_client[mongo_db_name]
			total_rows = 0
			for table in tables:
				rows_copied = migrate_table(
					conn=conn,
					mongo_db=mongo_db,
					schema=schema,
					table=table,
					batch_size=batch_size,
					truncate_collections=truncate_collections,
					write_mode=write_mode,
				)
				total_rows += rows_copied
				print(f"Migrated {rows_copied} rows from {schema}.{table} -> {mongo_db_name}.{table}")

	print(f"Migration complete. Total rows copied: {total_rows}")


def parse_args() -> argparse.Namespace:
	parser = argparse.ArgumentParser(description="Supabase Postgres helper script")
	parser.add_argument(
		"command",
		nargs="?",
		choices=("ping", "health", "migrate"),
		default="ping",
		help="ping (default), health checks, or migrate all tables to MongoDB",
	)
	parser.add_argument(
		"--schema",
		default="public",
		help="Postgres schema to migrate from (default: public)",
	)
	parser.add_argument(
		"--batch-size",
		type=int,
		default=1000,
		help="Rows per MongoDB write batch (default: 1000)",
	)
	parser.add_argument(
		"--no-truncate",
		action="store_true",
		help="Append to MongoDB collections instead of clearing them first",
	)
	parser.add_argument(
		"--write-mode",
		choices=("insert", "upsert"),
		default="insert",
		help="insert: fast append; upsert: idempotent when PK exists",
	)
	return parser.parse_args()


def main() -> int:
	try:
		load_env_files()
		args = parse_args()
		connection_config = get_connection_config()
		if args.command == "health":
			health_check(connection_config)
		elif args.command == "migrate":
			if args.batch_size <= 0:
				raise RuntimeError("--batch-size must be greater than 0")
			migrate_all_tables(
				connection_config=connection_config,
				schema=args.schema,
				batch_size=args.batch_size,
				truncate_collections=not args.no_truncate,
				write_mode=args.write_mode,
			)
		else:
			test_connection(connection_config)
		return 0
	except Exception as exc:  # noqa: BLE001 - report exact connection failure to CLI
		print(f"Connection failed: {exc}", file=sys.stderr)
		return 1


if __name__ == "__main__":
	raise SystemExit(main())
