import sys
from pyspark.sql import SparkSession
import spark.src.app.extract_job as ej


def run() -> None:
    """
    A simple function to create the spark session and triggers the jobs.
    """
    spark_session = SparkSession \
        .builder \
        .appName('transaction-management-app') \
        .getOrCreate()

    helpers_utils = ej.HelperUtils()
    config = helpers_utils.config_loader(sys.argv[1])
    ej.InsightJob(spark_session, ej.HelperUtils, config).run()


# trigger the insight jobs

if __name__ == '__main__':
    run()
