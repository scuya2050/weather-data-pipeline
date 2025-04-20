from pyspark.sql import SparkSession
from pyspark.sql.types import *
from pyspark.sql.functions import lit, concat, col, lower

import logging
import argparse
from functools import reduce

logger = logging.getLogger()
logger.setLevel(logging.INFO)
# logging.getLogger("py4j").setLevel(logging.ERROR)

spark = SparkSession.builder.getOrCreate()


def load_country_codes(path, catalog, schema):
    df = spark.read.format("csv").option("header", "true").option("delimiter", ";").option("inferSchema", "false").load(path)
    df = df.withColumnRenamed("Country", "country")\
        .withColumnRenamed("Abbreviation", "abbreviation") \
        .select(["country", "abbreviation"])

    df.write.mode("overwrite")\
        .option("overwriteSchema", "true")\
        .format("delta")\
        .saveAsTable(f"{catalog}.{schema}.country_codes")


def read_peru(paths, catalog, schema):
    df = spark.read.format("csv").option("header", "true").option("delimiter", ";").option("inferSchema", "false").load(paths["Peru"])
    df = df.withColumnRenamed("Ubigeo", "district_code")\
        .withColumnRenamed("Distrito", "district") \
        .withColumnRenamed("Departamento", "region") \
        .withColumn("country", lit("Peru")) \
        .select(["district_code", "country", "region", "district"]) \
        .dropDuplicates(["country", "region", "district"])

    return df


def read_ecuador(paths, catalog, schema):
    df = spark.read.format("csv").option("header", "true").option("delimiter", ";").option("inferSchema", "false").load(paths["Ecuador"])
    df = df.withColumnRenamed("Código Parroquia", "district_code")\
        .withColumnRenamed("Parroquia", "district") \
        .withColumnRenamed("Provincia", "region") \
        .withColumn("country", lit("Ecuador")) \
        .select(["district_code", "country", "region", "district"]) \
        .dropDuplicates(["country", "region", "district"])

    return df


def load_countries(readers, paths, catalog, schema):
    dfs = [reader(paths, catalog, schema) for reader in readers]
    if len(dfs) > 1:
        df = reduce(lambda a, b: a.unionByName(b, allowMissingColumns=True), dfs)
    else:
        df = dfs[0]
    
    df.write.mode("overwrite")\
        .option("overwriteSchema", "true")\
        .format("delta")\
        .saveAsTable(f"{catalog}.{schema}.countries")


def process_locations(source_catalog, source_schema, target_catalog, target_schema):

    spark.sql(f"""
        CREATE TABLE IF NOT EXISTS {target_catalog}.{target_schema}.dim_locations (
            location_code       STRING PRIMARY KEY, 
            country             STRING NOT NULL, 
            region              STRING NOT NULL, 
            district            STRING NOT NULL,
            effective_from_date DATE,
            effective_to_date   DATE,
            current_flag        BOOLEAN,
            deleted_flag        BOOLEAN
        )
        """)
    
    country_codes_df = spark.read.table(f"{source_catalog}.{source_schema}.country_codes")
    countries_df = spark.read.table(f"{source_catalog}.{source_schema}.countries")
    
    df = country_codes_df.join(countries_df, "country")\
        .withColumn("location_code", concat(col("abbreviation"), lit("-"), col("district_code")))\
        .select("location_code", "country", "region", "district")\

    df.createOrReplaceTempView("tmp_locations")

    
    spark.sql(f"""
        MERGE INTO {target_catalog}.{target_schema}.dim_locations t
        USING tmp_locations s
            ON t.location_code = s.location_code
        WHEN MATCHED THEN
            UPDATE SET 
                t.country = s.country,
                t.region = s.region,
                t.district = s.district
        WHEN NOT MATCHED BY TARGET THEN
            INSERT 
                (location_code, country, region, district, effective_from_date, effective_to_date, current_flag, deleted_flag) 
                VALUES (s.location_code, s.country, s.region, s.district, '2000-01-01', NULL, true, false)
        WHEN NOT MATCHED BY SOURCE THEN
            UPDATE SET 
                t.current_flag = false,
                t.effective_to_date = current_date(),
                t.deleted_flag = true
        """)

if __name__ == "__main__":
    PATH_COUNTRY_CODES = r'/Volumes/weather/01_bronze/locations/country_alpha_3.csv'
    PATH_COUNTRIES = {
        "Peru": r'/Volumes/weather/01_bronze/locations/peru_locations.csv',
        "Ecuador": r'/Volumes/weather/01_bronze/locations/ecuador_locations.csv'
        }

    BRONZE_CATALOG = "weather"
    BRONZE_SCHEMA = "01_bronze"

    SILVER_CATALOG = "weather"
    SILVER_SCHEMA = "02_silver"

    READERS = [read_peru, read_ecuador]

    load_country_codes(PATH_COUNTRY_CODES, BRONZE_CATALOG, BRONZE_SCHEMA)
    load_countries(READERS, PATH_COUNTRIES, BRONZE_CATALOG, BRONZE_SCHEMA)
    process_locations(BRONZE_CATALOG, BRONZE_SCHEMA, SILVER_CATALOG, SILVER_SCHEMA)

  
