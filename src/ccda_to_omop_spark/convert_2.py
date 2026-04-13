from pyspark.sql import SparkSession
import os
from process_directory import process_dictory

# ---- CONFIG (keep this dead simple) ----
INPUT_DIR = "/Users/croeder/git/CCDA/tislab-clad/CCDA_OMOP_Conversion_Package/resources"
OUTPUT_DIR = "/Users/croeder/git/CCDA/tislab-clad/ccda_to_omop_drivers/src/ccda_to_omop_spark"

# ---- CORE WORK FUNCTION (must be top-level) ----
def run(file_path):
    # Import INSIDE the function so Spark workers can resolve it
    from ccda_to_omop.pipeline import process_ccda  # adjust to your actual import

    try:
        tables = process_ccda(file_path)

        file_id = os.path.basename(file_path)

        for table_name, df in tables.items():
            out_dir = f"{OUTPUT_DIR}/{table_name}"
            os.makedirs(out_dir, exist_ok=True)

            out_path = f"{out_dir}/{file_id}.parquet"
            df.to_parquet(out_path)

        return f"OK: {file_path}"

    except Exception as e:
        return f"ERROR: {file_path} :: {str(e)}"


# ---- DRIVER ----
def main():
    spark = SparkSession.builder.appName("ccda_to_omop").getOrCreate()
    sc = spark.sparkContext

    # Collect file list
    files = [
        os.path.join(INPUT_DIR, f)
        for f in os.listdir(INPUT_DIR)
        if f.endswith(".xml")
    ]

    if not files:
        raise ValueError("No CCDA files found")

    # Parallelize
    rdd = sc.parallelize(files, numSlices=len(files))

    # Run and collect results (important: not foreach)
    results = rdd.map(run).collect()

    # Print results (so failures are visible)
    for r in results:
        print(r)

    spark.stop()


if __name__ == "__main__":
    main()
