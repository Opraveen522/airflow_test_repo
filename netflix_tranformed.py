from pyspark.sql import SparkSession
from pyspark.sql.functions import col, upper

def transform_csv_to_uppercase(input_file_path, output_file_path):
    # Initialize Spark session
    spark = SparkSession.builder \
        .appName("CSV to Uppercase") \
        .getOrCreate()

    # Read CSV file into DataFrame
    df = spark.read.option("header", True).csv(input_file_path)

    # Apply transformation: Convert "description" column to uppercase
    df_transformed = df.withColumn("description", upper(col("description")))

    # Write transformed DataFrame to output location
    df_transformed.write.mode("overwrite").csv(output_file_path)

    # Stop Spark session
    spark.stop()

# Example usage of the function
if __name__ == "__main__":
    input_path = "netflix_titles.csv"
    output_path = "netflix_output.csv"
    transform_csv_to_uppercase(input_path, output_path)

