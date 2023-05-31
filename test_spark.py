import logging
import os
import sys
import pytest
from pyspark.sql import SparkSession

def quiet_py4j():
    """Suppress spark logging for the test context."""
    logger = logging.getLogger('py4j')
    logger.setLevel(logging.WARN)


@pytest.fixture(scope="session")
def spark_session():
    """Fixture for creating a spark context."""
    os.environ['PYSPARK_PYTHON'] = sys.executable
    os.environ['PYSPARK_DRIVER_PYTHON'] = sys.executable
    import pyspark
    sc = pyspark.SparkContext.getOrCreate(pyspark.SparkConf().setMaster("local[*]"))
    spark = pyspark.sql.SparkSession(sc)

    quiet_py4j()
    return spark

def test_my_app(spark_session):
    spark_session()