from pyspark.sql.functions import udf, lower, col, to_date
from pyspark.sql.types import StringType, StructField, StructType, DateType, IntegerType
import json


class TransformationJob:
    """
    A Class to to run all the transformation jobs
    """

    def __init__(self, spark_session, helper_utils, config) -> None:
        """
        The default constructor
        :param spark_session: Spark Session instance
        :param helper_utils: Utils Class instance
        :param config: Config data
        """
        self.config = config
        self.source_path = config['csv_data']
        self.spark_session = spark_session
        self.logger = TransformationJob.__logger(spark_session)
        self.helper_utils = helper_utils

    def run(self) -> None:
        """
        Main transform class to run all jobs and save the data into jdbc sink (postgres)
        """
        self.logger.info('Running Transformation Job')

        

        def to_retrieve_data() -> None:
            self.df = self.spark_session.read \
                .format("jdbc") \
                .option("url", self.config['postgres_db'])\
                .option("dbtable", self.config['postgres_bronze'])\
                .option("user", self.config['postgres_user'])\
                .option("password", self.config["postgres_pwd"])\
                .load()

        def lowercase_transaction_type() -> None:
            """
            Lowercase the letters of the transaction_type coulmn.
            """
            self.logger.info('Running lowercase Job')
            self.df = self.df.withColumn("transaction_type", lower(col('country')))


        def to_date_format() -> None:
            """
            Taking a fixed format for the date column
            """
            self.logger.info('Running DATE Job')
            self.df = self.df.withColumn('date', to_date('date', 'dd/mm/yyyy'))
            self.df.show()

        def to_persist_data() -> None:
            """
            Data persist to use for later into postgres
            """
            self.logger.info('Running persistent Job')
            (self.df.write
             .format("jdbc")
             .option("url", self.config['postgres_url'])
             .option("dbtable", self.config['postgres_silver'])
             .option("user", self.config['postgres_user'])
             .option("password", self.config["postgres_pwd"])
             .mode("overwrite")
             .save())

        to_retrieve_data()
        lowercase_transaction_type()
        to_date_format()
        to_persist_data()

        self.logger.info('End running TransformationJob')

    @staticmethod
    def __logger(spark_session):
        """
        Logger method to get the logging
        :param spark_session: Spark Session
        :return: Logmanager instance
        """
        log4j_logger = spark_session.sparkContext._jvm.org.apache.log4j  # pylint: disable=W0212
        return log4j_logger.LogManager.getLogger(__name__)
    
class HelperUtils:
    """
    A helper class to provide some dependecy function to help the trasnform job
    """

    @staticmethod
    def config_loader(file_path: str) -> json:
        """
        A function to load config file
        """
        try:
            with open(file_path, 'r') as f:
                config = json.load(f)
        except IOError:
            print("Error: File does not appear to exist.")
            return 0
        return config