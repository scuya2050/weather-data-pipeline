from pyspark.sql import SparkSession
from pyspark.sql.types import *

import logging
import argparse

logger = logging.getLogger()
logger.setLevel(logging.INFO)

spark = SparkSession.builder.getOrCreate()

parser = argparse.ArgumentParser()
parser.add_argument("--catalog", required=True)
parser.add_argument("--schema", required=True)
parser.add_argument("--table", required=True)

args = parser.parse_args()

catalog = args.catalog
schema = args.schema
table = args.table

logger.info(f"Using catalog: {catalog}")
logger.info(f"Using schema: {schema}")
logger.info(f"Using table: {table}")

tb_hist = spark.sql(f"""
    WITH table_history AS (
        SELECT * FROM (DESCRIBE HISTORY {catalog}.{schema}.{table})
    )
    SELECT
        CAST(operationMetrics.numSkippedCorruptFiles AS INT)
    FROM table_history
    WHERE operation = 'COPY INTO'
          """)

skipped_files = tb_hist.first()[0]

if skipped_files == 0:
    logger.info(f"No corrupt files found in {catalog}.{schema}.{table}")
else:
    logger.info(f"Found {skipped_files} corrupt files in {catalog}.{schema}.{table}")
    assert skipped_files == 0, f"Found {skipped_files} corrupt files in {catalog}.{schema}.{table}"