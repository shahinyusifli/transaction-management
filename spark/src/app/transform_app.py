import sys
from pyspark.sql import SparkSession
import spark.src.app.transform_job as tj


def run() -> None:
    """
    A simple function to create the spark session and triggers the jobs.
    """
    spark_session = (SparkSession
                     .builder
                     .getOrCreate()
                     )

    helpers_utils = tj.HelperUtils()
    print(sys.argv[1])
    config = helpers_utils.config_loader(sys.argv[1])
    tj.ExtractJob(spark_session, tj.HelperUtils, config).run()


# trigger the trasnformations
if __name__ == '__main__':
    run()
