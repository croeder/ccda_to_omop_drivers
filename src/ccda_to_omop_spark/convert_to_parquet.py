
import datetime
import logging
import os
import pandas as pd
import pathlib

import numpy as np
from pyspark.sql import SparkSession
from pyspark.sql.types import (
    StructType, StructField,
    LongType, IntegerType, FloatType, DoubleType,
    StringType, DateType, TimestampType
)
from ccda_to_omop import layer_datasets as LD
from ccda_to_omop import value_transformations as VT
from ccda_to_omop import domain_dataframe_column_types
from ccda_to_omop import ddl
from ccda_to_omop.util import create_codemap_dict_from_csv
from process_directory import process_directory
from type_util import  coerce_dataframe


def convert_to_parquet(omop_dataset_dict):
    for (cfg_name, dataset) in omop_dataset_dict.items():
        omop_domain = ddl.config_to_domain_name_dict[cfg_name]
        print(f"DOING {cfg_name} {omop_domain}")
        if omop_domain in ddl.domain_name_to_table_name:
            omop_tablename = ddl.domain_name_to_table_name[omop_domain]
            if dataset is None or len(dataset) == 0:
                print(f"   SKIPPED {cfg_name}: empty dataset")
                continue
            sdf = None
            try:
                col_types = domain_dataframe_column_types.domain_dataframe_column_types[omop_tablename]
                dataset = coerce_dataframe(dataset, col_types)
                schema = build_spark_schema(col_types)
                # Align DataFrame column order to schema (Spark matches by position)
                schema_cols = [f.name for f in schema.fields]
                dataset = dataset.reindex(columns=schema_cols)
                sdf = spark.createDataFrame(dataset, schema=schema)
            except Exception as e:
                print(f"   ERROR converting parquet/{omop_domain}_parquet")
                print(f"   {e}")
            if sdf:
                try:
                    print(f"  WRITING parquet/{omop_domain}_parquet")
                    sdf.write.parquet(f"parquet/{omop_domain}_parquet", mode='overwrite')
                except Exception as e:
                    print(f"   ERROR writing parquet/{omop_domain}_parquet")
                    print(f"   {e}")
        else:
            print(f"   SKIPPED \"{omop_domain}\" not in domain_name_to_table_name")
    
