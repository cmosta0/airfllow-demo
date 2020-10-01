from pyspark.sql import SparkSession
import logging
import os

APP_NAME = "read_random_words"

LOGGER = logging.getLogger(APP_NAME)
logging.basicConfig(level=logging.INFO)


def get_parent_folder_path():
    return os.path.dirname(os.path.realpath(__file__))


def main_process():
    LOGGER.info("Initializing spark session")
    spark = SparkSession.builder \
        .master("local") \
        .appName(APP_NAME) \
        .getOrCreate()

    parent_script_folder = get_parent_folder_path()
    LOGGER.info(f"Parent script folder: {parent_script_folder}")

    LOGGER.info("Reading input file")
    input_file_path = parent_script_folder + "/sources/random_words.txt"
    random_words_df = spark.read.text(input_file_path)

    LOGGER.info("Printing schema along with data for input data source")
    random_words_df.show(truncate=False)
    random_words_df.printSchema()

    LOGGER.info("Getting words from dataframe")
    words_rdd = random_words_df.rdd.flatMap(lambda x: x[0].split(" "))
    LOGGER.info("Sample processed data:")
    LOGGER.info(words_rdd.take(10))

    LOGGER.info("Calculating word counter (not best way with .count())")
    words_count = words_rdd.count()
    LOGGER.info(f"Total word count:::::: {words_count}")


if __name__ == "__main__":
    LOGGER.info(f"Starting {APP_NAME}")
    main_process()
