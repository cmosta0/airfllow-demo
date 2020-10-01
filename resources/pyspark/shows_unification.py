from pyspark.sql import SparkSession
import logging
import os
import sys

APP_NAME = "shows_unification"
LOGGER = logging.getLogger(APP_NAME)
logging.basicConfig(level=logging.INFO)


def get_parent_folder_path():
    return os.path.dirname(os.path.realpath(__file__))


def get_input_df(spark, input_dir, select_expr_list):
    parent_script_folder = get_parent_folder_path()
    LOGGER.info(f"Parent script folder: {parent_script_folder}")

    input_dir_path = parent_script_folder + "/tmp_output/" + input_dir + "/"
    LOGGER.info(f"Reading input file: {input_dir_path}")
    input_shows = spark.read.json(input_dir_path)

    LOGGER.info("Extracting df for unification")

    return input_shows.selectExpr(select_expr_list)


def main_process(netflix_dir, prime_dir, output_folder):
    LOGGER.info("Initializing spark session")
    spark = SparkSession.builder \
        .master("local") \
        .appName(APP_NAME) \
        .getOrCreate()

    netflix_select_expr = ["Titles as show_name", "IMDB_Rating as imdb_rating", "'Netflix' as source"]
    netflix_shows = get_input_df(spark, netflix_dir, netflix_select_expr)

    prime_select_expr = ["`Name of the show` as show_name", "`IMDb rating` as imdb_rating", "'Prime' as source"]
    prime_shows = get_input_df(spark, prime_dir, prime_select_expr)

    LOGGER.info("Netflix and prime unified DF's")

    netflix_shows.show(100, False)
    prime_shows.show(100, False)

    LOGGER.info("Merging data...")
    all_shows = netflix_shows.union(prime_shows)
    all_shows.show(100, False)

    absolute_output_dir = get_parent_folder_path() + "/tmp_output/" + output_folder + "/"
    all_shows.coalesce(1).write.option("header", "true").format('csv').mode('overwrite').save(absolute_output_dir)

    LOGGER.info(f"App {APP_NAME} completed.")


if __name__ == "__main__":
    LOGGER.info(f"Reading app args: {sys.argv}")
    netflix_input = sys.argv[1]
    prime_input = sys.argv[2]
    unified_shows_output = sys.argv[3]
    main_process(netflix_input, prime_input, unified_shows_output)
