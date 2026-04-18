"""
convert_2.py — distributed CCDA→OMOP conversion on a Spark cluster.

Designed for a cluster with no shared storage (e.g. a Raspberry Pi cluster).
The driver reads all XML files and serialises their content into the RDD so
workers never need to reach back to the driver's filesystem. map.csv and
type_util.py are shipped to workers via sc.addFile / sc.addPyFile.
Each worker writes its own parquet files locally.

After all workers finish, the driver consolidates the per-file parquet files
in each OMOP domain subdirectory into a single combined parquet file at
OUTPUT_DIR/<table_name>.parquet, then removes the per-file subdirectories.

NOTE: ccda_to_omop cannot be shipped as a zip (sc.addPyFile) because
metadata/__init__.py uses os.listdir() to discover config files, which
does not work inside a zip archive. The package must be pip-installed
on every worker node before running:
    pip install /path/to/CCDA_OMOP_Conversion_Package
"""

import os
import shutil
import pathlib
from pyspark.sql import SparkSession

# ---- CONFIG ----
_HERE          = pathlib.Path(__file__).parent
_REPO_ROOT     = _HERE.parent.parent.parent.resolve()
_PKG_ROOT      = _REPO_ROOT / "CCDA_OMOP_Conversion_Package"

INPUT_DIR      = os.environ.get("CCDA_INPUT_DIR",  str(_PKG_ROOT / "resources"))
OUTPUT_DIR     = os.environ.get("CCDA_OUTPUT_DIR", "/tmp/parquet2")  # workers write locally
CODEMAP        = os.environ.get("CCDA_CODEMAP",    str(_PKG_ROOT / "resources" / "map.csv"))
TYPE_UTIL      = str(_HERE / "type_util.py")


# ---- WORKER FUNCTION ----
def run(item):
    """Process one CCDA XML file (passed as content) and write parquet locally.

    Runs on a Spark worker. All imports are inside the function so each worker
    initialises its own state independently.
    """
    import os
    import logging
    import tempfile
    from pyspark import SparkFiles
    from ccda_to_omop import layer_datasets as LD
    from ccda_to_omop import value_transformations as VT
    from ccda_to_omop import domain_dataframe_column_types, ddl
    from ccda_to_omop.util import create_codemap_dict_from_csv
    from type_util import coerce_dataframe, build_spark_schema

    logging.basicConfig(level=logging.WARNING)

    filename, xml_content = item

    # map.csv was shipped by the driver — resolve its local path on this worker
    codemap_path = SparkFiles.get("map.csv")
    codemap_dict = create_codemap_dict_from_csv(codemap_path)
    VT.set_codemap_dict(codemap_dict)

    results = []

    # Write XML content to a temp file so LD.process_file() can read it
    tmp = tempfile.NamedTemporaryFile(suffix=".xml", delete=False, mode="w", encoding="utf-8")
    try:
        tmp.write(xml_content)
        tmp.close()

        data_dict = LD.process_file(tmp.name, False, "")

        file_id = os.path.splitext(filename)[0]

        for cfg_name, dataset in data_dict.items():
            if dataset is None or len(dataset) == 0:
                continue

            omop_domain = ddl.config_to_domain_name_dict.get(cfg_name)
            if omop_domain is None:
                continue
            table_name = ddl.domain_name_to_table_name.get(omop_domain)
            if table_name is None:
                continue
            col_types = domain_dataframe_column_types.domain_dataframe_column_types.get(table_name)
            if col_types is None:
                continue

            dataset = coerce_dataframe(dataset, col_types)
            schema = build_spark_schema(col_types)
            schema_cols = [f.name for f in schema.fields]
            dataset = dataset.reindex(columns=schema_cols)

            out_dir = os.path.join(OUTPUT_DIR, table_name)
            os.makedirs(out_dir, exist_ok=True)
            dataset.to_parquet(os.path.join(out_dir, f"{file_id}.parquet"), index=False)
            results.append(f"OK: {file_id} -> {table_name}")

    except Exception as e:
        results.append(f"ERROR: {filename} :: {e}")
    finally:
        os.unlink(tmp.name)

    return results


# ---- DRIVER ----
def main():
    spark = SparkSession.builder.appName("ccda_to_omop").getOrCreate()
    sc = spark.sparkContext

    # Ship helper files to all workers
    sc.addPyFile(TYPE_UTIL)
    sc.addFile(CODEMAP)           # accessed on workers via SparkFiles.get("map.csv")

    # Driver reads XML content — workers never touch the driver filesystem
    items = []
    for fname in os.listdir(INPUT_DIR):
        if fname.endswith(".xml"):
            fpath = os.path.join(INPUT_DIR, fname)
            with open(fpath, encoding="utf-8") as f:
                items.append((fname, f.read()))

    if not items:
        raise ValueError(f"No CCDA XML files found in {INPUT_DIR}")

    rdd = sc.parallelize(items, numSlices=len(items))
    all_results = rdd.flatMap(run).collect()

    for r in all_results:
        print(r)

    # ---- CONSOLIDATION ----
    # Each domain subdirectory contains one parquet file per input XML file.
    # Read each subdirectory as a single Spark DataFrame and write it out as
    # one combined parquet file at OUTPUT_DIR/<table_name>.parquet, then
    # remove the per-file subdirectory.
    print("\nConsolidating per-file parquet files by OMOP domain...")
    for table_name in os.listdir(OUTPUT_DIR):
        table_dir = os.path.join(OUTPUT_DIR, table_name)
        if not os.path.isdir(table_dir):
            continue
        out_path = os.path.join(OUTPUT_DIR, f"{table_name}.parquet")
        try:
            df = spark.read.parquet(table_dir)
            df.coalesce(1).write.mode("overwrite").parquet(out_path)
            shutil.rmtree(table_dir)
            print(f"  consolidated {table_name} ({df.count()} rows) -> {out_path}")
        except Exception as e:
            print(f"  ERROR consolidating {table_name}: {e}")

    spark.stop()


if __name__ == "__main__":
    main()
