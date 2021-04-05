from datetime import datetime

from servier_data.cleaning.cleaner import clean_dataset
from servier_data.session.spark_session import Session


class TestCleaner(object):
    """
    Class to test clean data set method
    """

    def test_drop_null_values_only(self):
        """ Filter row if one of column is Null

        """
        source_data = [(1, "title 1", "01/04/2020", "journal 1"),
                       (2, "title 2", "03/05/2021", None),
                       (3, "title 3", None, "journal 1"),
                       (4, "title 4", "10/02/2010", "journal 3")
                       ]
        source_df = Session.get_session().createDataFrame(
            source_data,
            ["id", "title", "date", "journal"]
        )

        actual_df = clean_dataset(source_df)
        expected_data = [(1, "title 1", "01/04/2020", "journal 1"),
                         (4, "title 4", "10/02/2010", "journal 3")
                         ]
        expected_df = Session.get_session().createDataFrame(
            expected_data,
            ["id", "title", "date", "journal"]
        )
        assert (expected_df.collect() == actual_df.collect())

    def test_drop_null_and_format_date(self):
        """ Filter row if one of column is Null and format date
        """
        source_data = [(1, "title 1", "01/04/2020", "journal 1"),
                       (2, "title 2", "03/05/2021", None),
                       (3, "title 3", "10 July 2018", "journal 1"),
                       (4, "title 4", "10/02/2010", "journal 3")
                       ]
        source_df = Session.get_session().createDataFrame(
            source_data,
            ["id", "title", "date", "journal"]
        )

        actual_df = clean_dataset(source_df, date_col="date", date_patterns=["dd/MM/yyyy", "d MMMM yyyy"])
        expected_data = [(1, "title 1", datetime.strptime("2020-04-01", "%Y-%m-%d").date(), "journal 1"),
                         (3, "title 3", datetime.strptime("2018-07-10", "%Y-%m-%d").date(), "journal 1"),
                         (4, "title 4", datetime.strptime("2010-02-10", "%Y-%m-%d").date(), "journal 3")
                         ]
        expected_df = Session.get_session().createDataFrame(
            expected_data,
            ["id", "title", "date", "journal"]
        )
        assert (expected_df.collect() == actual_df.collect())
