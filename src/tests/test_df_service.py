import os
import sys
from pyspark.sql import SparkSession
import pytest
from dfservice import get_result_dataframe


os.environ['PYSPARK_PYTHON'] = sys.executable
os.environ['PYSPARK_DRIVER_PYTHON'] = sys.executable


def test_df_service():
    spark = SparkSession.builder.getOrCreate()
    df_product = spark.createDataFrame(
        [
            (1, "Product1"),
            (2, "Product2"),
            (3, "Product3"),
            (4, "Product4")
        ],
        ["id", "name"]
    )
    df_cat = spark.createDataFrame(
        [
            (1, "Cat1"),
            (2, "Cat2"),
            (3, "Cat3"),
        ],
        ["id", "name"]
    )
    df_product_cat = spark.createDataFrame(
        [
            (1, 1),
            (1, 2),
            (2, 1),
            (3, 2)
        ],
        ["product_id", "cat_id"]
    )

    df_result = spark.createDataFrame(
        [
            ["Product1", "Cat1"],
            ["Product1", "Cat2"],
            ["Product2", "Cat1"],
            ["Product3", "Cat2"],
            ["Product4", None],
        ],
        ["product_name", "cat_name"]
    )
    df_return = get_result_dataframe(df_product, df_cat, df_product_cat)
    assert len(df_return.columns) == len(df_result.columns)
    assert df_return.count() == df_result.count()
    df_return.show()
    df_diff = df_return.exceptAll(df_result)
    assert df_diff.count() == 0