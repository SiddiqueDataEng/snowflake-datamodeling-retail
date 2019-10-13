import os
from pathlib import Path
from typing import List

from src.config import get_snowflake_connection


SQL_DIR = Path(__file__).resolve().parents[2] / "sql"


def read_sql_files() -> List[Path]:
	files = sorted([p for p in SQL_DIR.glob("*.sql")])
	if not files:
		raise FileNotFoundError(f"No SQL files found in {SQL_DIR}")
	return files


def split_statements(sql_text: str) -> List[str]:
	lines = []
	for raw_line in sql_text.splitlines():
		line = raw_line.strip()
		if not line:
			continue
		if line.startswith("--"):
			continue
		lines.append(raw_line)
	clean = "\n".join(lines)
	parts = [p.strip() for p in clean.split(";")]
	return [p for p in parts if p]


def main():
	conn = get_snowflake_connection()
	try:
		with conn.cursor() as cur:
			for sql_file in read_sql_files():
				print(f"Applying {sql_file.name} ...")
				text = sql_file.read_text(encoding="utf-8")
				for stmt in split_statements(text):
					cur.execute(stmt)
					# print a dot per statement for progress
					print(".", end="")
				print(" done")
	finally:
		conn.close()


if __name__ == "__main__":
	main() 