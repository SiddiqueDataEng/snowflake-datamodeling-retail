import os
import random
import argparse
from datetime import datetime, timedelta

import pandas as pd
from faker import Faker

from src.config import get_mysql_engine


OUTPUT_DIR = os.path.join(os.path.dirname(__file__), "..", "..", "data")


def ensure_output_dir(path: str = OUTPUT_DIR) -> str:
	output_path = os.path.abspath(path)
	os.makedirs(output_path, exist_ok=True)
	return output_path


def generate_customers(num_customers: int, seed: int = 42) -> pd.DataFrame:
	fake = Faker()
	Faker.seed(seed)
	random.seed(seed)
	rows = []
	for customer_id in range(1, num_customers + 1):
		first = fake.first_name()
		last = fake.last_name()
		rows.append(
			{
				"CUSTOMER_ID": customer_id,
				"FIRST_NAME": first,
				"LAST_NAME": last,
				"EMAIL": f"{first}.{last}.{customer_id}@example.com".lower(),
				"PHONE": fake.phone_number(),
				"ADDRESS": fake.street_address().replace(",", ""),
				"CITY": fake.city().replace(",", ""),
				"STATE": fake.state_abbr(),
				"POSTAL_CODE": fake.postcode(),
				"COUNTRY": "USA",
				"CREATED_AT": fake.date_time_between(start_date="-2y", end_date="now"),
			}
		)
	return pd.DataFrame(rows)


def generate_products(num_products: int, seed: int = 43) -> pd.DataFrame:
	fake = Faker()
	Faker.seed(seed)
	random.seed(seed)
	categories = [
		"Electronics",
		"Home",
		"Garden",
		"Sports",
		"Clothing",
		"Toys",
	]
	rows = []
	for product_id in range(1, num_products + 1):
		price = round(random.uniform(5.0, 500.0), 2)
		cost = round(price * random.uniform(0.4, 0.8), 2)
		rows.append(
			{
				"PRODUCT_ID": product_id,
				"PRODUCT_NAME": fake.catch_phrase().replace(",", ""),
				"CATEGORY": random.choice(categories),
				"PRICE": price,
				"COST": cost,
				"ACTIVE": random.random() > 0.05,
				"CREATED_AT": fake.date_time_between(start_date="-2y", end_date="now"),
			}
		)
	return pd.DataFrame(rows)


def generate_orders(
	num_orders: int,
	max_items_per_order: int,
	num_customers: int,
	num_products: int,
	seed: int = 44,
):
	fake = Faker()
	Faker.seed(seed)
	random.seed(seed)
	orders = []
	order_items = []
	order_item_id = 1
	for order_id in range(1, num_orders + 1):
		customer_id = random.randint(1, num_customers)
		order_date = fake.date_time_between(start_date="-18m", end_date="now")
		status = random.choice(["NEW", "SHIPPED", "DELIVERED", "CANCELLED"])
		num_items = random.randint(1, max_items_per_order)
		order_total = 0.0
		for _ in range(num_items):
			product_id = random.randint(1, num_products)
			quantity = random.randint(1, 5)
			unit_price = round(random.uniform(5.0, 500.0), 2)
			extended = round(quantity * unit_price, 2)
			order_items.append(
				{
					"ORDER_ITEM_ID": order_item_id,
					"ORDER_ID": order_id,
					"PRODUCT_ID": product_id,
					"QUANTITY": quantity,
					"UNIT_PRICE": unit_price,
					"EXTENDED_PRICE": extended,
				}
			)
			order_item_id += 1
			order_total += extended
		orders.append(
			{
				"ORDER_ID": order_id,
				"CUSTOMER_ID": customer_id,
				"ORDER_DATE": order_date,
				"STATUS": status,
				"ORDER_TOTAL": round(order_total, 2),
			}
		)
	return pd.DataFrame(orders), pd.DataFrame(order_items)


def write_csv(df: pd.DataFrame, path: str):
	df.to_csv(path, index=False)


def write_json(df: pd.DataFrame, path: str):
	# JSON Lines, friendly for big-data ingestion
	df.to_json(path, orient="records", lines=True, date_format="iso")


def write_parquet(df: pd.DataFrame, path: str):
	# Uses pyarrow via pandas
	df.to_parquet(path, index=False)


def write_avro(df: pd.DataFrame, path: str):
	from fastavro import writer, parse_schema

	# Simple dynamic schema from pandas dtypes; Snowflake can infer types
	def map_dtype(name: str, dtype) -> dict:
		if pd.api.types.is_integer_dtype(dtype):
			return {"name": name, "type": ["null", "long"], "default": None}
		if pd.api.types.is_float_dtype(dtype):
			return {"name": name, "type": ["null", "double"], "default": None}
		if pd.api.types.is_bool_dtype(dtype):
			return {"name": name, "type": ["null", "boolean"], "default": None}
		# Timestamps and objects to string
		return {"name": name, "type": ["null", "string"], "default": None}

	fields = [map_dtype(col, df[col].dtype) for col in df.columns]
	schema = {"name": "record", "type": "record", "fields": fields}
	parsed = parse_schema(schema)
	# Convert timestamps to iso strings to be safe
	records = df.copy()
	for col in records.select_dtypes(include=["datetime64[ns]"]).columns:
		records[col] = records[col].dt.strftime("%Y-%m-%dT%H:%M:%S%z")
	records = records.where(pd.notnull(records), None)
	with open(path, "wb") as f:
		writer(f, parsed, records.to_dict(orient="records"))


def ensure_subdir(base_dir: str, name: str) -> str:
	d = os.path.join(base_dir, name)
	os.makedirs(d, exist_ok=True)
	return d


def load_to_mysql(
	customers: pd.DataFrame,
	products: pd.DataFrame,
	orders: pd.DataFrame,
	order_items: pd.DataFrame,
	chunksize: int = 20000,
):
	from sqlalchemy import types as satypes

	engine = get_mysql_engine()
	with engine.begin() as conn:
		# dtype hints for better MySQL column types
		customers.to_sql(
			"customers",
			conn,
			if_exists="append",
			index=False,
			chunksize=chunksize,
			dtype={
				"CUSTOMER_ID": satypes.Integer(),
				"FIRST_NAME": satypes.String(100),
				"LAST_NAME": satypes.String(100),
				"EMAIL": satypes.String(255),
				"PHONE": satypes.String(50),
				"ADDRESS": satypes.String(255),
				"CITY": satypes.String(100),
				"STATE": satypes.String(10),
				"POSTAL_CODE": satypes.String(20),
				"COUNTRY": satypes.String(50),
				"CREATED_AT": satypes.DateTime(),
			},
			method="multi",
		)
		products.to_sql(
			"products",
			conn,
			if_exists="append",
			index=False,
			chunksize=chunksize,
			dtype={
				"PRODUCT_ID": satypes.Integer(),
				"PRODUCT_NAME": satypes.String(255),
				"CATEGORY": satypes.String(50),
				"PRICE": satypes.Numeric(10, 2),
				"COST": satypes.Numeric(10, 2),
				"ACTIVE": satypes.Boolean(),
				"CREATED_AT": satypes.DateTime(),
			},
			method="multi",
		)
		orders.to_sql(
			"orders",
			conn,
			if_exists="append",
			index=False,
			chunksize=chunksize,
			dtype={
				"ORDER_ID": satypes.Integer(),
				"CUSTOMER_ID": satypes.Integer(),
				"ORDER_DATE": satypes.DateTime(),
				"STATUS": satypes.String(20),
				"ORDER_TOTAL": satypes.Numeric(12, 2),
			},
			method="multi",
		)
		order_items.to_sql(
			"order_items",
			conn,
			if_exists="append",
			index=False,
			chunksize=chunksize,
			dtype={
				"ORDER_ITEM_ID": satypes.Integer(),
				"ORDER_ID": satypes.Integer(),
				"PRODUCT_ID": satypes.Integer(),
				"QUANTITY": satypes.Integer(),
				"UNIT_PRICE": satypes.Numeric(10, 2),
				"EXTENDED_PRICE": satypes.Numeric(12, 2),
			},
			method="multi",
		)


def main():
	parser = argparse.ArgumentParser(description="Generate synthetic retail datasets")
	parser.add_argument("--num-customers", type=int, default=10000)
	parser.add_argument("--num-products", type=int, default=2000)
	parser.add_argument("--num-orders", type=int, default=50000)
	parser.add_argument("--max-items", type=int, default=5)
	parser.add_argument(
		"--formats",
		type=str,
		default="csv,json,parquet,avro",
		help="Comma-separated list of formats to write: csv,json,parquet,avro",
	)
	parser.add_argument("--output-dir", type=str, default=OUTPUT_DIR)
	parser.add_argument("--load-mysql", action="store_true")
	parser.add_argument("--mysql-chunk-size", type=int, default=20000)
	args = parser.parse_args()

	output_dir = ensure_output_dir(args.output_dir)
	formats = {f.strip().lower() for f in args.formats.split(",") if f.strip()}

	customers = generate_customers(args.num_customers)
	products = generate_products(args.num_products)
	orders, order_items = generate_orders(
		num_orders=args.num_orders,
		max_items_per_order=args.max_items,
		num_customers=len(customers),
		num_products=len(products),
	)

	# Write files in requested formats
	if "csv" in formats:
		csv_dir = ensure_subdir(output_dir, "csv")
		write_csv(customers, os.path.join(csv_dir, "customers.csv"))
		write_csv(products, os.path.join(csv_dir, "products.csv"))
		write_csv(orders, os.path.join(csv_dir, "orders.csv"))
		write_csv(order_items, os.path.join(csv_dir, "order_items.csv"))

	if "json" in formats:
		json_dir = ensure_subdir(output_dir, "json")
		write_json(customers, os.path.join(json_dir, "customers.jsonl"))
		write_json(products, os.path.join(json_dir, "products.jsonl"))
		write_json(orders, os.path.join(json_dir, "orders.jsonl"))
		write_json(order_items, os.path.join(json_dir, "order_items.jsonl"))

	if "parquet" in formats:
		pq_dir = ensure_subdir(output_dir, "parquet")
		write_parquet(customers, os.path.join(pq_dir, "customers.parquet"))
		write_parquet(products, os.path.join(pq_dir, "products.parquet"))
		write_parquet(orders, os.path.join(pq_dir, "orders.parquet"))
		write_parquet(order_items, os.path.join(pq_dir, "order_items.parquet"))

	if "avro" in formats:
		avro_dir = ensure_subdir(output_dir, "avro")
		write_avro(customers, os.path.join(avro_dir, "customers.avro"))
		write_avro(products, os.path.join(avro_dir, "products.avro"))
		write_avro(orders, os.path.join(avro_dir, "orders.avro"))
		write_avro(order_items, os.path.join(avro_dir, "order_items.avro"))

	print(f"Data generated in: {output_dir}")

	if args.load_mysql:
		print("Loading datasets into MySQL...")
		load_to_mysql(customers, products, orders, order_items, chunksize=args.mysql_chunk_size)
		print("MySQL load complete.")


if __name__ == "__main__":
	main()