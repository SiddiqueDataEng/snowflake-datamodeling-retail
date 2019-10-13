import os
from pathlib import Path
import streamlit as st
import pandas as pd
from sqlalchemy import text

from src.config import get_sqlalchemy_engine


st.set_page_config(page_title="Snowflake Retail Demo", layout="wide")

st.title("Snowflake Retail Data Explorer")

# Prefer Snowflake; fall back to local CSVs under data/ if credentials are missing
engine = None
source = "snowflake"
try:
	engine = get_sqlalchemy_engine()
except Exception:
	source = "local"


@st.cache_data(ttl=300)
def _fetch_from_snowflake(query: str) -> pd.DataFrame:
	with engine.begin() as conn:
		return pd.read_sql(text(query), conn)


@st.cache_data(ttl=300)
def _load_local_frames():
	data_dir = Path(__file__).resolve().parents[2] / "data"
	customers = pd.read_csv(data_dir / "customers.csv") if (data_dir / "customers.csv").exists() else pd.DataFrame()
	products = pd.read_csv(data_dir / "products.csv") if (data_dir / "products.csv").exists() else pd.DataFrame()
	orders = pd.read_csv(data_dir / "orders.csv") if (data_dir / "orders.csv").exists() else pd.DataFrame()
	order_items = pd.read_csv(data_dir / "order_items.csv") if (data_dir / "order_items.csv").exists() else pd.DataFrame()

	# Views
	dim_customer = pd.DataFrame()
	if not customers.empty:
		dim_customer = customers[[
			"CUSTOMER_ID",
			"FIRST_NAME",
			"LAST_NAME",
			"EMAIL",
			"PHONE",
			"CITY",
			"STATE",
			"COUNTRY",
			"CREATED_AT",
		]].rename(columns={"CREATED_AT": "CUSTOMER_CREATED_AT"})

	dim_product = pd.DataFrame()
	if not products.empty:
		dim_product = products[[
			"PRODUCT_ID",
			"PRODUCT_NAME",
			"CATEGORY",
			"PRICE",
			"COST",
			"ACTIVE",
			"CREATED_AT",
		]].rename(columns={"CREATED_AT": "PRODUCT_CREATED_AT"})

	fact_order = pd.DataFrame()
	if not order_items.empty and not orders.empty:
		tmp = order_items.merge(orders[["ORDER_ID", "CUSTOMER_ID", "ORDER_DATE", "STATUS"]], on="ORDER_ID", how="left")
		tmp["QUANTITY"] = tmp.apply(lambda r: 0 if r.get("STATUS") == "CANCELLED" else r.get("QUANTITY", 0), axis=1)
		tmp["SALES_AMOUNT"] = tmp.apply(lambda r: 0.0 if r.get("STATUS") == "CANCELLED" else r.get("EXTENDED_PRICE", 0.0), axis=1)
		fact_order = tmp[[
			"ORDER_ITEM_ID",
			"ORDER_ID",
			"CUSTOMER_ID",
			"PRODUCT_ID",
			"ORDER_DATE",
			"QUANTITY",
			"SALES_AMOUNT",
		]]

	sales_by_day = pd.DataFrame()
	if not fact_order.empty:
		sales_by_day = (
			fact_order.assign(ORDER_DAY=pd.to_datetime(fact_order["ORDER_DATE"]).dt.date)
			.groupby("ORDER_DAY", as_index=False)
			.agg(TOTAL_SALES=("SALES_AMOUNT", "sum"), TOTAL_QUANTITY=("QUANTITY", "sum"))
		)

	return {
		"DIM_CUSTOMER": dim_customer,
		"DIM_PRODUCT": dim_product,
		"FACT_ORDER": fact_order,
		"SALES_BY_DAY": sales_by_day,
	}


def fetch_df(query: str) -> pd.DataFrame:
	if engine is not None:
		return _fetch_from_snowflake(query)
	frames = _load_local_frames()
	upper = query.upper()
	if "DIM_CUSTOMER" in upper:
		return frames["DIM_CUSTOMER"]
	if "DIM_PRODUCT" in upper:
		return frames["DIM_PRODUCT"]
	if "SALES_BY_DAY" in upper:
		return frames["SALES_BY_DAY"]
	return frames["FACT_ORDER"]


st.sidebar.write("Data Source")
if source == "snowflake":
	st.sidebar.success("Connected to Snowflake")
else:
	st.sidebar.warning("Using local CSVs (no Snowflake connection)")


tab_customers, tab_orders, tab_products, tab_sales = st.tabs(["Customers", "Orders", "Products", "Sales"])

with tab_customers:
	df_customers = fetch_df("SELECT * FROM CORE.DIM_CUSTOMER ORDER BY CUSTOMER_ID LIMIT 500")
	st.subheader("Customers (sample)")
	st.dataframe(df_customers, use_container_width=True)

with tab_products:
	df_products = fetch_df("SELECT * FROM CORE.DIM_PRODUCT ORDER BY PRODUCT_ID LIMIT 500")
	st.subheader("Products (sample)")
	col1, col2 = st.columns(2)
	col1.metric("Products", len(df_products))
	if "CATEGORY" in df_products.columns and not df_products.empty:
		cat_counts = df_products["CATEGORY"].value_counts().reset_index()
		cat_counts.columns = ["CATEGORY", "COUNT"]
		col2.bar_chart(cat_counts, x="CATEGORY", y="COUNT")
	st.dataframe(df_products, use_container_width=True)

with tab_orders:
	df_orders = fetch_df("SELECT * FROM CORE.FACT_ORDER ORDER BY ORDER_DATE DESC LIMIT 1000")
	st.subheader("Orders (recent)")
	st.dataframe(df_orders, use_container_width=True)

with tab_sales:
	df_sales = fetch_df("SELECT * FROM CORE.SALES_BY_DAY ORDER BY ORDER_DAY")
	st.subheader("Sales by Day")
	if not df_sales.empty:
		st.line_chart(df_sales, x="ORDER_DAY", y="TOTAL_SALES")
		st.bar_chart(df_sales, x="ORDER_DAY", y="TOTAL_QUANTITY")
	st.dataframe(df_sales, use_container_width=True)