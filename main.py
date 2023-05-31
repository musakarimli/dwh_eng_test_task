from pyspark.sql.functions import stddev, mean, col
from pyspark.sql import DataFrame

def standardization(df: DataFrame, input_column: str, output_column: str) -> DataFrame:
    df1 = df\
            .select(mean(f"{input_column}").alias(f"mean_{input_column}"), stddev(f"{input_column}").alias(f"stddev_{input_column}"))\
            .crossJoin(df)\
            .withColumn(f"{output_column}" , (col("feature_type_1_0") - col(f"mean_{input_column}")) / col(f"stddev_{input_column}"))\
            .drop(*[f"mean_{input_column}",f"stddev_{input_column}"])\
            .persist()
    return df1

from pyspark.sql.functions import udf, array, greatest, when, lit, expr
from pyspark.sql.types import IntegerType

def max_feature_value(df: DataFrame, columns: list) -> DataFrame:  
    df2 = df\
        .withColumn("max",greatest(*[col(x) for x in columns]))\
        .withColumn('max_feature_type_1_index', array(*[when(col(c) ==col('max'), lit(c)) for c in columns]))\
        .withColumn('max_feature_type_1_index', expr("filter(max_feature_type_1_index, x -> x is not null)"))\
        .withColumn('max_feature_type_1_index', col("max_feature_type_1_index")[0])\
        .persist()
    return df2

def main():
    import os
    import sys
    import logging
    logger = logging.getLogger('py4j')
    logger.setLevel(logging.INFO)

    # starting spark session
    os.environ['PYSPARK_PYTHON'] = sys.executable
    os.environ['PYSPARK_DRIVER_PYTHON'] = sys.executable

    import pyspark
    sc = pyspark.SparkContext.getOrCreate(pyspark.SparkConf().setMaster("local[*]"))
    spark = pyspark.sql.SparkSession(sc)
    logger.info(f"spark.version")

    # reading the file
    staticSchema = spark.read\
            .option("inferSchema", "true")\
            .option("header","true")\
            .option("escape", "\"")\
            .option("multiLine","true")\
            .format("csv")\
            .load("test.csv")\
            .limit(1000)\
            .schema

    # reading the file
    df = spark.read\
            .schema(staticSchema)\
            .option("header","true")\
            .option("escape", "\"")\
            .option("multiLine","true")\
            .format("csv")\
            .load("test.csv")\
            .persist()
            
    logger.info(df.columns)
            
    df = df.repartition(2 * 2 * 1) # (executors * cores * replicationFactor)
    cols_numbering = [(col.split('_')[-2], col.split('_')[-1]) for col in df.columns if 'feature' in col]



    for i,j in cols_numbering:
        input_column = "_".join(["feature_type",i,j])
        output_column = "_".join(["features_type",i,"stand",j])
        df = standardization(df, input_column, output_column)
        
    input_columns = ["_".join(["feature_type",i,j]) for i,j in cols_numbering]
    df = max_feature_value(df, columns = input_columns)
    
    df.persist()\
        .repartition(1)\
        .write\
        .mode('overwrite')\
        .format("csv")\
        .save("output/test_transformed")

if __name__ == '__main__':
    main()