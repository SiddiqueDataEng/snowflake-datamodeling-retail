import os
from pathlib import Path

import pandas as pd
from snowflake.connector.pandas_tools import write_pandas

from src.config import get_snowflake_connection


DATA_DIR = Path(__file__).resolve().parents[2] / "data"


def load_csv_to_table(cur, table: str, filename: str):
	file_path = DATA_DIR / filename
	if not file_path.exists():
		raise FileNotFoundError(f"Missing data file: {file_path}")
	df = pd.read_csv(file_path)
	# write_pandas handles chunking and staging under the hood
	success, nchunks, nrows, _ = write_pandas(cur.connection, df, table_name=table, schema="RAW")
	if not success:
		raise RuntimeError(f"write_pandas failed for {table}")
	return nrows


def main():
	conn = get_snowflake_connection()
	try:
		with conn.cursor() as cur:
			cur.execute("USE DATABASE DM_DEMO")
			cur.execute("USE SCHEMA RAW")
			rows = 0
			rows += load_csv_to_table(cur, "CUSTOMERS", "customers.csv")
			rows += load_csv_to_table(cur, "PRODUCTS", "products.csv")
			rows += load_csv_to_table(cur, "ORDERS", "orders.csv")
			rows += load_csv_to_table(cur, "ORDER_ITEMS", "order_items.csv")
			print(f"Loaded total rows: {rows}")
	finally:
		conn.close()


if __name__ == "__main__":
	main() 