from pyspark.sql import SparkSession

# Set up the SparkSession
spark = SparkSession.builder.appName("covid_analysis").enableHiveSupport().getOrCreate()

# Load the total_cases_by_country data from Parquet format
df_cases = spark.read.parquet("s3://your-bucket-name/path/to/total-cases-by-country.parquet")

# Create a temporary view for querying
df_cases.createOrReplaceTempView("total_cases_by_country")

# Define the Hive table schema
hive_schema = "`location` STRING, `total_cases` LONG"

# Create the Hive table
hive_table_name = "covid_total_cases_by_country"
spark.sql(f"CREATE TABLE IF NOT EXISTS {hive_table_name} ({hive_schema}) STORED AS ORC")

# Insert the data from the DataFrame into the Hive table
spark.sql(f"INSERT INTO {hive_table_name} SELECT * FROM total_cases_by_country")

# Stop the SparkSession
spark.stop()
