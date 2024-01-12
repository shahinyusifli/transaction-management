from pyspark.sql.types import StringType, StructField, StructType


class ExtractJob:
    """
    A Class to to run all the transformation jobs
    """

    def __init__(self, spark_session, helper_utils, config) -> None:
        """
        The default constructor
        :param spark_session: Spark Session instance
        :param helper_utils: Utils Class instance
        :param config: Config json
        """
        self.config = config
        self.spark_session = spark_session
        self.helper_utils = helper_utils
        self.logger = ExtractJob.__logger(spark_session)
        self.df = None

    def run(self) -> None:
        """
        Main transform class to run all jobs and save the data into absolute sink location
        """
        self.logger.info('Running actions Job')

        def read_data() -> None:
            csv_schema = StructType(
                [
                    StructField('user_id', StringType()),
                    StructField('account_id', StringType()),
                    StructField('counterparty_id', StringType()),
                    StructField('transaction_type', StringType()),
                    StructField('date', StringType()),
                    StructField('amount', StringType())
                ]
            )
            # read the input file
            self.df = self.spark_session.read.csv(self.source_path, schema=csv_schema)

        def to_persist_data() -> None:
            """
            Data persist to use for later into postgres
            """
            self.logger.info('Running persistent Job')
            (self.df.write
             .format("jdbc")
             .option("url", self.config['postgres_url'])
             .option("dbtable", self.config['postgres_bronze'])
             .option("user", self.config['postgres_user'])
             .option("password", self.config["postgres_pwd"])
             .mode("overwrite")
             .save())

        read_data()
        to_persist_data()

        self.logger.info('End running actions')

    @staticmethod
    def __logger(spark_session):
        """
        Logger method to get the logging
        :param spark_session: Spark Session
        :return: Logmanager instance
        """
        log4j_logger = spark_session.sparkContext._jvm.org.apache.log4j  # pylint: disable=W0212
        return log4j_logger.LogManager.getLogger(__name__)
