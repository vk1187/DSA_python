import os
import json
import logging
import time
from pyspark.sql import SparkSession
from pyspark.sql.functions import col, when


config_file = "../config/spark_config.json"
with open(config_file, "r") as f:
    spark_config = json.load(f)

# Ensure the log directory exists
log_dir = spark_config.get("spark.driver.log.path", "../logs/")
os.makedirs(log_dir, exist_ok=True)

# Initialize logging
log_file_path = os.path.join(log_dir, "pyspark_processing.log")
logging.basicConfig(filename=log_file_path, level=logging.INFO, 
                    format='%(asctime)s %(levelname)s: %(message)s')

# Log the start of the script
logging.info("Starting PySpark CSV processing script")
start_time = time.time()

# Initialize SparkSession with configurations
spark = SparkSession.builder \
    .appName(spark_config["appName"]) \
    .master(spark_config["master"]) \
    .config("spark.executor.instances", spark_config["spark.executor.instances"]) \
    .config("spark.executor.cores", spark_config["spark.executor.cores"]) \
    .config("spark.cores.max", spark_config["spark.cores.max"]) \
    .config("spark.executor.memory", spark_config["spark.executor.memory"]) \
    .config("spark.driver.memory", spark_config["spark.driver.memory"]) \
    .getOrCreate()

# Paths for input and output
input_folder = "../data/input/"
output_folder = "../data/output/"
os.makedirs(output_folder, exist_ok=True)

# Read all CSV files from input folder
csv_files = f"{input_folder}/*.csv"
df = spark.read.csv(csv_files, header=True, inferSchema=True)

# Log the number of files read
logging.info(f"Number of CSV files read: {df.count()}")

# Perform CRUD Operations
df = df.withColumn("seniority", when(col("age") >= 30, "Senior").otherwise("Junior"))
df = df.select("id", "name", "seniority", "salary")
df = df.withColumn("salary", when(col("seniority") == "Junior", col("salary") * 1.1).otherwise(col("salary")))
df = df.filter(col("salary") >= 50000)

# Log the number of records after filtering
logging.info(f"Number of records after filtering: {df.count()}")

# Write the processed data to the output folder
df.write.csv(output_folder, header=True, mode="overwrite")

# Log the output path
logging.info(f"Processed data saved to {output_folder}")

# Stop the Spark session
spark.stop()

# Calculate and log the execution time
end_time = time.time()
execution_time = end_time - start_time
logging.info(f"Execution time: {execution_time} seconds")
logging.info("PySpark CSV processing script completed")
