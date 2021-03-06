from pyspark.sql import SparkSession
from pyspark.sql.functions import col
import logging
import os
import sys

APP_NAME = "nextflix_shows_identification"
SOURCE_FILE = "tv-shows-netflix.csv"
LOGGER = logging.getLogger(APP_NAME)
logging.basicConfig(level=logging.INFO)


def get_parent_folder_path():
    return os.path.dirname(os.path.realpath(__file__))


def main_process(tmp_output_folder, df_limit=0):
    LOGGER.info("Initializing spark session")
    spark = SparkSession.builder \
        .master("local") \
        .appName(APP_NAME) \
        .getOrCreate()

    parent_script_folder = get_parent_folder_path()
    LOGGER.info(f"Parent script folder: {parent_script_folder}")

    LOGGER.info("Reading input file")
    input_file_path = parent_script_folder + "/sources/" + SOURCE_FILE
    netflix_shows = spark.read.csv(input_file_path, inferSchema=True, header=True)

    LOGGER.info("Get netflix_shows schema:")
    netflix_shows.printSchema()

    LOGGER.info("Show first 10 records in netflix_shows DF")
    netflix_shows.show(10, truncate=False)

    LOGGER.info("Sort by IMDB_Rating")
    netflix_shows = netflix_shows.sort(col("IMDB_Rating"), ascending=False)
    netflix_shows.show(10)

    LOGGER.info("Writing output to json file")
    output_path = get_parent_folder_path() + "/tmp_output/" + tmp_output_folder
    netflix_shows.limit(df_limit).coalesce(1).write.format('json').mode('overwrite').save(output_path)

    LOGGER.info(f"App {APP_NAME} completed.")


if __name__ == "__main__":
    output_folder = sys.argv[1]
    extraction_limit = int(sys.argv[2])
    main_process(output_folder, extraction_limit)
