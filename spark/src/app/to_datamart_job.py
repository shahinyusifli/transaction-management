from pyspark.sql.functions import F
from pyspark.sql.window import Window
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
        self.spark_session = spark_session
        self.logger = TransformationJob.__logger(spark_session)
        self.helper_utils = helper_utils
        self.df = None

    def run(self) -> None:
        """
        Main transform class to run all jobs and save the data into jdbc sink (postgres)
        """
        self.logger.info('Running Transformation Job')

        

        def to_retrieve_data() -> None:
            self.df = self.spark_session.read \
                .format("jdbc") \
                .option("url", self.config['postgres_db'])\
                .option("dbtable", self.config['postgres_table'])\
                .option("user", self.config['postgres_user'])\
                .option("password", self.config["postgres_pwd"])\
                .load()

        def transaction_max_amount() -> None:
            """
            Lowercase the letters of the transaction_type coulmn.
            """
            self.logger.info('Running lowercase Job')
            transaction_df = self.df.withColumn("date", F.to_date("date"))

            # Group by 'user_id' and 'counterparty_id', calculate the total amount for each pair
            grouped_df = transaction_df.groupBy("user_id", "counterparty_id").agg(F.sum("amount").alias("total_amount"))

            # Use window function to rank the pairs based on total amount in descending order
            window_spec = Window.partitionBy("user_id").orderBy(F.col("total_amount").desc())

            ranked_df = grouped_df.withColumn("rank", F.row_number().over(window_spec))

            # Filter only the rows with rank 1 for each user
            result_df = ranked_df.filter("rank = 1")

            # Select the required columns and show the final DataFrame
            self.df = result_df.select("user_id", "counterparty_id", "total_amount")


        def to_persist_data() -> None:
            """
            Data persist to use for later into postgres
            """
            self.logger.info('Running persistent Job')
            (self.df.write
             .format("jdbc")
             .option("url", self.config['postgres_url'])
             .option("dbtable", self.config['postgres_gold'])
             .option("user", self.config['postgres_user'])
             .option("password", self.config["postgres_pwd"])
             .mode("overwrite")
             .save())

        to_retrieve_data()
        transaction_max_amount()
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