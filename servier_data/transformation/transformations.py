from typing import List, Dict

from pyspark.sql import DataFrame
from pyspark.sql.functions import upper, col, split, explode, struct, collect_set


def drop_null_values(df: DataFrame, column_list: List[str] = None):
    """ drop row if one column  of the given List has null value

    :param df : dataframe to filtre on
    :param column_list: column list to consider in filter, if one of the given column is null, the row is filtred
    :return: dataframe with none null values on specified column
    """
    return df.dropna(subset=column_list)


def make_join_column(df: DataFrame, input_col: str, output_col: str, split_delimiter: str = " "):
    """Tokenize given column on giver delimiter

    :param df : dataframe to operate on
    :param input_col : iput column to split
    :param output_col : the output column to store result
    :rtype: dataframe
    """
    return df.withColumn(output_col, upper(col(input_col))) \
        .withColumn(output_col, split(col(output_col), split_delimiter)) \
        .withColumn(output_col, explode(col(output_col)))


def link_df(left_df: DataFrame, right_df: DataFrame, out_col: str, right_col: str, left_col: str, join: str = 'inner',
            map_cols: Dict[str, str] = None):
    """make a join between two dataframe and construct struct from set of column defined by map_cols parameter

    :param left_df: left dataframe
    :param right_df: right dataframe
    :param out_col: column with in to set result
    :param right_col: right column in the join condition
    :param left_col: left column in the join condition
    :param join: join type
    :param map_cols: dico used to construct article object(column-alias)
    :return: dataframe
    """
    return left_df.join(right_df, left_df[left_col] == right_df[right_col], how=join) \
        .withColumn(out_col, _build_struct(map_cols)) \
        .drop(*[key for key in map_cols.keys()])


def aggregate_object(df: DataFrame, cols: List[str], out_col: str, agg_column: str):
    """ Build struct from given columns

    :param df: intput dataframe
    :param cols: columns to group by
    :param out_col: out put column name
    :param agg_column: columns used to build struct
    :return: dataframe
    """
    return df.groupby(cols).agg(collect_set(agg_column).alias(out_col))


def _build_struct(map_cols: Dict[str, str]):
    return struct([col(k) if map_cols[k] is None else col(k).alias(map_cols[k]) for k in map_cols.keys()])
