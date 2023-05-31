from pyspark.sql.functions import stddev, mean, col
from pyspark.sql import DataFrame

from pyspark.sql.functions import udf, array, greatest, when, lit, expr
from pyspark.sql.types import IntegerType
import os
import sys

# starting spark session
os.environ['PYSPARK_PYTHON'] = sys.executable
os.environ['PYSPARK_DRIVER_PYTHON'] = sys.executable

import pyspark
sc = pyspark.SparkContext.getOrCreate(pyspark.SparkConf().setMaster("local[*]"))
spark = pyspark.sql.SparkSession(sc)
   
df = spark.read\
        .option("inferSchema", "true")\
        .option("header","true")\
        .option("escape", "\"")\
        .option("multiLine","true")\
        .format("csv")\
        .load("test.csv")\
        .persist()

cols_numbering = [(col.split('_')[-2], col.split('_')[-1]) for col in df.columns if 'feature' in col]

def test_standardization():
    from main import standardization
    for i,j in cols_numbering:
        input_column = "_".join(["feature_type",i,j])
        output_column = "_".join(["features_type",i,"stand",j])
        test_df = standardization(df, input_column, output_column)
        assert type(test_df) == DataFrame
        output_columns = int([col for col in test_df.columns if 'feature' in col].__len__())
        assert output_columns  == int(cols_numbering.__len__()) + 1
    
def test_max_feature_value():
    from main import max_feature_value
    input_columns = ["_".join(["feature_type",i,j]) for i,j in cols_numbering]
    test_df2 = max_feature_value(df, columns = input_columns)
    assert type(test_df2) == DataFrame
    assert test_df2.columns.__len__() == df.columns.__len__() + 2