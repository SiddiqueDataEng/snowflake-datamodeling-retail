import os
from typing import Optional

from dotenv import load_dotenv
import snowflake.connector
from sqlalchemy import create_engine


load_dotenv()


def get_env(name: str, default: Optional[str] = None) -> str:
	value = os.getenv(name, default)
	if value is None or value == "":
		raise RuntimeError(f"Missing required environment variable: {name}")
	return value


def get_snowflake_connection():
	authenticator = os.getenv("SNOWFLAKE_AUTHENTICATOR")
	kwargs = {
		"account": get_env("SNOWFLAKE_ACCOUNT"),
		"user": get_env("SNOWFLAKE_USER"),
		"role": get_env("SNOWFLAKE_ROLE"),
		"warehouse": get_env("SNOWFLAKE_WAREHOUSE"),
		"database": get_env("SNOWFLAKE_DATABASE"),
		"schema": get_env("SNOWFLAKE_SCHEMA"),
	}
	if authenticator:
		kwargs["authenticator"] = authenticator
	else:
		kwargs["password"] = get_env("SNOWFLAKE_PASSWORD")
	conn = snowflake.connector.connect(**kwargs)
	return conn


def get_sqlalchemy_engine():
	account = get_env("SNOWFLAKE_ACCOUNT")
	user = get_env("SNOWFLAKE_USER")
	role = get_env("SNOWFLAKE_ROLE")
	warehouse = get_env("SNOWFLAKE_WAREHOUSE")
	database = get_env("SNOWFLAKE_DATABASE")
	schema = get_env("SNOWFLAKE_SCHEMA")
	authenticator = os.getenv("SNOWFLAKE_AUTHENTICATOR")
	if authenticator:
		conn_str = (
			f"snowflake://{user}@{account}/{database}/{schema}?role={role}&warehouse={warehouse}&authenticator={authenticator}"
		)
	else:
		password = get_env("SNOWFLAKE_PASSWORD")
		conn_str = (
			f"snowflake://{user}:{password}@{account}/{database}/{schema}?role={role}&warehouse={warehouse}"
		)
	engine = create_engine(conn_str)
	return engine


def get_mysql_engine(echo: bool = False):
	"""Create a SQLAlchemy engine for a local/remote MySQL instance using PyMySQL.

	Environment variables (see env.example):
	- MYSQL_HOST (default: 127.0.0.1)
	- MYSQL_PORT (default: 3306)
	- MYSQL_DATABASE (required)
	- MYSQL_USER (required)
	- MYSQL_PASSWORD (required)
	"""
	import urllib.parse as _urlparse

	host = os.getenv("MYSQL_HOST", "127.0.0.1")
	port = int(os.getenv("MYSQL_PORT", "3306"))
	database = get_env("MYSQL_DATABASE")
	user = get_env("MYSQL_USER")
	password = get_env("MYSQL_PASSWORD")
	password_escaped = _urlparse.quote_plus(password)
	conn_str = f"mysql+pymysql://{user}:{password_escaped}@{host}:{port}/{database}"
	engine = create_engine(
		conn_str,
		echo=echo,
		pool_pre_ping=True,
		pool_recycle=3600,
	)
	return engine