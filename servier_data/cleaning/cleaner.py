from typing import List

from pyspark.sql import DataFrame
from pyspark.sql.functions import col, coalesce, to_date

import servier_data.transformation.transformations as T


def clean_dataset(df: DataFrame, column_list: List[str] = None, date_col: str = None, date_patterns: List[str] = None):
    """ Clean method : it allows to filter rows with on of given column is null and parse date based on given patterns

    :param df: dataframe to clean
    :param column_list: column list to consider in filter, if one of the given column is null, the row is filtred
    :param date_col: date column to parse
    :param date_patterns: List of pattern to consider
    :return: dataframe
    """
    trials = T.drop_null_values(df, column_list=column_list)
    if date_col is not None and len(date_patterns) > 0:
        trials = trials.withColumn("date", coalesce(*[to_date(col(date_col), pattern) for pattern in date_patterns]))
    return trials
