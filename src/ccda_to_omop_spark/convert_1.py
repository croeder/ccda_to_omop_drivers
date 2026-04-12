
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


def _numpy_dtype_to_spark(dtype):
    if dtype == np.int64:
        return LongType()
    elif dtype == np.int32:
        return IntegerType()
    elif dtype == np.float64:
        return DoubleType()
    elif dtype == np.float32:
        return FloatType()
    elif dtype == datetime.datetime:
        return TimestampType()
    elif dtype == datetime.date:
        return DateType()
    else:
        return StringType()


def build_spark_schema(dtype_dict):
    return StructType([
        StructField(col, _numpy_dtype_to_spark(dtype), True)
        for col, dtype in dtype_dict.items()
    ])


def _to_str_or_none(x):
    if _is_null(x):
        return None
    return str(x)


def _to_date_or_none(x):
    if _is_null(x):
        return None
    if isinstance(x, datetime.datetime):
        return x.date()
    if isinstance(x, datetime.date):
        return x
    try:
        result = pd.to_datetime(x)
        return None if pd.isnull(result) else result.date()
    except Exception:
        return None


def _to_datetime_or_none(x):
    if _is_null(x):
        return None
    if isinstance(x, datetime.datetime):
        return x
    if isinstance(x, datetime.date):
        return datetime.datetime.combine(x, datetime.time())
    try:
        result = pd.to_datetime(x)
        return None if pd.isnull(result) else result.to_pydatetime()
    except Exception:
        return None


def _is_null(x):
    """Return True if x should be treated as null."""
    if x is None:
        return True
    try:
        return pd.isnull(x)
    except Exception:
        return False


def _to_int_or_none(x):
    if _is_null(x):
        return None
    try:
        v = pd.to_numeric(x, errors='coerce')
        return None if pd.isnull(v) else int(v)
    except Exception:
        return None


def _to_float_or_none(x):
    if _is_null(x):
        return None
    try:
        v = pd.to_numeric(x, errors='coerce')
        return None if pd.isnull(v) else float(v)
    except Exception:
        return None


def _apply_object(series, fn):
    """Apply fn element-wise and return an object-dtype Series.
    Prevents pandas from converting [int, None] → float64."""
    return pd.array([fn(x) for x in series], dtype=object)


def coerce_dataframe(df, dtype_dict):
    """Coerce pandas DataFrame columns to Python native types so Spark can
    apply an explicit schema. All null values become Python None."""
    df = df.copy()
    for col, dtype in dtype_dict.items():
        if col not in df.columns:
            continue
        try:
            if dtype in (np.int64, np.int32):
                df[col] = _apply_object(df[col], _to_int_or_none)
            elif dtype in (np.float64, np.float32):
                df[col] = _apply_object(df[col], _to_float_or_none)
            elif dtype == datetime.datetime:
                df[col] = _apply_object(df[col], _to_datetime_or_none)
            elif dtype == datetime.date:
                df[col] = _apply_object(df[col], _to_date_or_none)
            elif dtype == str:
                df[col] = _apply_object(df[col], _to_str_or_none)
        except Exception as e:
            logger.warning(f"coerce_dataframe: col={col} dtype={dtype} error={e}")
    return df

spark = SparkSession.builder.getOrCreate()

logging.basicConfig(
        format='%(levelname)s: %(filename)s %(lineno)d %(message)s',
        level=logging.INFO
)
logger = logging.getLogger(__name__)

dir="/Users/croeder/git/CCDA-data/resources"

home=pathlib.Path(__file__).parent.parent.parent.resolve()
codemap_dict = create_codemap_dict_from_csv(f"{home}/resources/map.csv")
VT.set_codemap_dict(codemap_dict)

# LD.process_directory() returns None (designed for Foundry), so this local
# version returns the accumulated dict instead.
# Also writes per-file per-config CSVs to output/ for comparison.
def process_directory(directory_path, write_csv_flag, parse_config, csv_output_dir="output"):
    omop_dataset_dict = {}  # keyed by config names
    os.makedirs(csv_output_dir, exist_ok=True)

    only_files = [f for f in os.listdir(directory_path) if os.path.isfile(os.path.join(directory_path, f))]
    for file in only_files:
        if file.endswith(".xml"):
            new_data_dict = LD.process_file(os.path.join(directory_path, file), write_csv_flag, parse_config)
            for key in new_data_dict:
                if new_data_dict[key] is not None:
                    csv_path = os.path.join(csv_output_dir, f"{file}__{key}.csv")
                    new_data_dict[key].to_csv(csv_path, index=False, lineterminator='\r\n')
                if key in omop_dataset_dict and omop_dataset_dict[key] is not None:
                    if new_data_dict[key] is not None:
                        omop_dataset_dict[key] = pd.concat([omop_dataset_dict[key], new_data_dict[key]])
                else:
                    omop_dataset_dict[key] = new_data_dict[key]
                if new_data_dict[key] is not None:
                    logger.info(f"{file} {key} {len(omop_dataset_dict)} {omop_dataset_dict[key].shape} {new_data_dict[key].shape}")
                else:
                    logger.info(f"{file} {key} {len(omop_dataset_dict)} None / no data")
    return omop_dataset_dict

omop_dataset_dict = process_directory(dir, False, '')

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

