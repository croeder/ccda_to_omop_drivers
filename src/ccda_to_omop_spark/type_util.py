
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


logger = logging.getLogger(__name__)

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


