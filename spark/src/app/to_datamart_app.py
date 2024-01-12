import sys
from pyspark.sql import SparkSession
import spark.src.app.to_datamart_job as dj


def run() -> None:
    """
    A simple function to create the spark session and triggers the jobs.
    """
    spark_session = (SparkSession
                     .builder
                     .getOrCreate()
                     )

    helpers_utils = dj.HelperUtils()
    print(sys.argv[1])
    config = helpers_utils.config_loader(sys.argv[1])
    dj.ExtractJob(spark_session, dj.HelperUtils, config).run()


# trigger the trasnformations
if __name__ == '__main__':
    run()
