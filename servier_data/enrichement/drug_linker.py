from typing import Dict

from pyspark.sql import DataFrame
from pyspark.sql.functions import col, coalesce, to_date

import servier_data.transformation.transformations as T


class DrugLinker:

    def __init__(self, df_drugs: DataFrame, articles: DataFrame, col_mapping: Dict[str, str],
                 article_col_join: str, out_col_name: str):
        """
        Constructor of the DrugLinker clas
        :param df_drugs: drugs dataframe
        :param articles: article dataframe
        :param col_mapping: mapping of headers column
        :param article_col_join: column name on the article where to search drug name (if this column contain drugs the row will be link to the drug )
        :param out_col_name: column name of articles array()
        """
        self.df_drugs = df_drugs
        self.articles = articles
        self.col_mapping = col_mapping
        self.article_col_join = article_col_join
        self.out_col_name = out_col_name

    def transform(self):
        """
        Link drugs and article. it regroups all articles

        """
        trials = T.make_join_column(df=self.articles, input_col=self.article_col_join,
                                    output_col='intermediate_cols')

        result = T.link_df(trials, self.df_drugs, left_col='intermediate_cols', right_col='drug',
                           out_col='intermediate_cols', join='right_outer',
                           map_cols=self.col_mapping)

        return T.aggregate_object(result, ['drug'], self.out_col_name, 'intermediate_cols')
