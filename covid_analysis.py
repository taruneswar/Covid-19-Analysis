from pyspark.sql import SparkSession

# Set up the SparkSession
spark = SparkSession.builder.appName("covid_analysis").getOrCreate()

# Load the COVID-19 data from S3
df_covid = spark.read.csv("s3://covid-19/2023/covid-data.csv", header=True, inferSchema=True)

# Show the schema of the COVID-19 data
df_covid.printSchema()

# Perform some basic data cleaning
df_covid = df_covid.filter(df_covid["date"].isNotNull()) \
                   .filter(df_covid["location"].isNotNull()) \
                   .filter(df_covid["total_cases"].isNotNull()) \
                   .filter(df_covid["total_deaths"].isNotNull()) \
                   .na.fill(0)

# Register the DataFrame as a temporary view for querying
df_covid.createOrReplaceTempView("covid_data")

# Run some SQL queries on the COVID-19 data
total_cases_by_country = spark.sql("SELECT location, SUM(total_cases) AS total_cases FROM covid_data GROUP BY location ORDER BY total_cases DESC")
total_cases_by_country.show()

total_deaths_by_country = spark.sql("SELECT location, SUM(total_deaths) AS total_deaths FROM covid_data GROUP BY location ORDER BY total_deaths DESC")
total_deaths_by_country.show()

# Write the results back to S3 in Parquet format
total_cases_by_country.write.parquet("s3://covid-19/2023/output/total-cases-by-country.parquet")
total_deaths_by_country.write.parquet("s3://covid-19/2023/output/total-deaths-by-country.parquet")

# Stop the SparkSession
spark.stop()
