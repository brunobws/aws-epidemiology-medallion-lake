import pytest
from unittest.mock import Mock

try:
    from pyspark.sql import SparkSession
    from pyspark.sql.types import StructType, StructField, StringType, DoubleType, IntegerType
except ImportError:
    pytest.skip("PySpark is not installed, skipping Spark tests.", allow_module_level=True)

from aws.modules.pyspark_utils import Pyspark

@pytest.fixture(scope="session")
def spark():
    """Create a local Spark session for testing."""
    return SparkSession.builder \
        .appName("pytest-spark-testing") \
        .master("local[1]") \
        .getOrCreate()

@pytest.fixture
def pyspark_util(spark):
    """Instantiate the custom Pyspark utility module."""
    mock_logger = Mock()
    return Pyspark(
        job_name="test_job",
        spark=spark,
        env="dev",
        logger=mock_logger,
        trgt_tbl="test_tb_mock"
    )

def test_cast_df_trims_strings_and_casts_types(spark, pyspark_util):
    """
    Tests if cast_df correctly:
    1. Trims whitespaces from strings
    2. Casts strings to integers
    3. Converts European formatted floats (1.000,50) to standard double (1000.50)
    """
    schema = StructType([
        StructField("name", StringType(), True),
        StructField("age", StringType(), True),
        StructField("salary", StringType(), True)
    ])
    
    # Fake dataframe with dirty data
    data = [
        ("  Bruno  ", "30", "5000,50"),
        (" Alice ", "25", "4.000,00")
    ]
    df = spark.createDataFrame(data, schema)
    
    # Schema matching the DynamoDB config
    target_schema = {
        "name": "string",
        "age": "int",
        "salary": "double"
    }
    
    # Execute the method from our pyspark_utils module
    result_df = pyspark_util.cast_df(
        df=df,
        schema=target_schema,
        ext="csv"
    )
    
    results = result_df.collect()
    
    # Verify string trimming
    assert results[0]["name"] == "Bruno"
    assert results[1]["name"] == "Alice"
    
    # Verify int casting
    assert results[0]["age"] == 30
    assert results[1]["age"] == 25
    
    # Verify European double conversion
    assert results[0]["salary"] == 5000.50
    assert results[1]["salary"] == 4000.00


def test_cast_df_handles_date_casting(spark, pyspark_util):
    """Tests if cast_df converts string dates to native Spark DateTypes."""
    schema = StructType([
        StructField("raw_date", StringType(), True)
    ])
    data = [("2026-04-30",), ("2026-05-01",)]
    df = spark.createDataFrame(data, schema)
    
    target_schema = {
        "date_col": ["raw_date", "date", "yyyy-MM-dd"]
    }
    
    result_df = pyspark_util.cast_df(
        df=df,
        schema=target_schema,
        ext="csv"
    )
    
    results = result_df.collect()
    # The output column should be named 'date_col' and be a native python datetime.date
    assert results[0]["date_col"].strftime("%Y-%m-%d") == "2026-04-30"
    assert results[1]["date_col"].strftime("%Y-%m-%d") == "2026-05-01"
