import pytest
from pyspark.sql import SparkSession
from pyspark.sql.functions import col
from spark.src.app.transform_job import TransformationJob, HelperUtils 

@pytest.fixture(scope="session")
def spark():
    return SparkSession.builder.master("local[2]").appName("test").getOrCreate()

def test_transaction_max_amount(spark, mocker):
    # Mock the logger to avoid actual log messages during testing
    mocker.patch("transformation_job.TransformationJob.__logger")

    # Mock the DataFrame for testing
    mock_df = mocker.Mock()
    mocker.patch.object(TransformationJob, 'df', mock_df)

    # Create an instance of HelperUtils to mock the config_loader
    helper_utils = HelperUtils()
    mocker.patch.object(HelperUtils, 'config_loader', return_value={'postgres_db': 'testdb', 'postgres_table': 'test_table',
                                                                    'postgres_user': 'testuser',
                                                                    'postgres_pwd': 'testpassword',
                                                                    'postgres_url': 'jdbc:postgresql://localhost:5432/testdb',
                                                                    'postgres_gold': 'test_gold'})

    # Create an instance of TransformationJob
    transform_job = TransformationJob(spark, helper_utils, {})

    # Mock the to_retrieve_data method to avoid actual database operations during testing
    mocker.patch.object(TransformationJob, 'to_retrieve_data')

    # Create a mock DataFrame for testing
    mock_data = [("user1", "counterparty1", 100.0), ("user1", "counterparty2", 150.0),
                 ("user2", "counterparty3", 120.0), ("user2", "counterparty4", 80.0)]
    mock_columns = ["user_id", "counterparty_id", "amount"]
    mock_df.createDataFrame(mock_data, mock_columns)

    # Set the mocked DataFrame for testing
    transform_job.df = mock_df

    # Run the transaction_max_amount method
    transform_job.transaction_max_amount()

    # Verify that the DataFrame transformations are applied correctly
    mock_df.withColumn.assert_called_once_with("date", col("date"))
    mock_df.groupBy.assert_called_once_with("user_id", "counterparty_id")
    mock_df.agg.assert_called_once_with(col("amount").alias("total_amount"))

