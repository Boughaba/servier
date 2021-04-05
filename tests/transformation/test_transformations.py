import servier_data.transformation.transformations as T
from servier_data.session.spark_session import Session


class TestTransformations(object):
    """ Test transformation defined in transformations module
    """

    def test_drop_null_values(self):
        """
        Drop row with on of the column contain null
        """
        source_data = [
            (1,
             "A 44-year-old man with erythema of the face diphenhydramine, neck, and chest, weakness, and palpitations",
             "01/01/2019", "Journal of emergency nursing"),
            (2,
             "An evaluation of benadryl, pyribenzamine, and other so-called diphenhydramine antihistaminic drugs in the treatment of allergy.",
             "01/01/2019", "Journal of emergency nursing"),
            (3, "Diphenhydramine hydrochloride helps symptoms of ciguatera fish poisoning.", None,
             "The Journal of pediatrics")
        ]
        source_df = Session.get_session().createDataFrame(
            source_data,
            ["id", "title", "date", "journal"]
        )
        actual_df = T.drop_null_values(source_df)
        expected_data = [
            (1,
             "A 44-year-old man with erythema of the face diphenhydramine, neck, and chest, weakness, and palpitations",
             "01/01/2019", "Journal of emergency nursing"),
            (2,
             "An evaluation of benadryl, pyribenzamine, and other so-called diphenhydramine antihistaminic drugs in the treatment of allergy.",
             "01/01/2019", "Journal of emergency nursing")
        ]
        expected_df = Session.get_session().createDataFrame(
            expected_data,
            ["id", "title", "date", "journal"]
        )
        assert (expected_df.collect() == actual_df.collect())

    def test_make_join_column(self):
        """ Build join column by split title column
        """
        source_data = [
            (1,
             "Diphenhydramine hydrochloride helps symptoms",
             "01/01/2019", "Journal of emergency nursing"),
            (2,
             "Appositional Tetracycline bone formation rates",
             "01/01/2019", "Journal of emergency nursing"),
        ]
        source_df = Session.get_session().createDataFrame(
            source_data,
            ["id", "title", "date", "journal"]
        )
        actual_df = T.make_join_column(source_df, input_col='title', output_col='tokens', split_delimiter=" ")
        expected_data = [
            (1, "Diphenhydramine hydrochloride helps symptoms", "01/01/2019", "Journal of emergency nursing",
             "DIPHENHYDRAMINE"),
            (1, "Diphenhydramine hydrochloride helps symptoms", "01/01/2019", "Journal of emergency nursing",
             "HYDROCHLORIDE"),
            (1, "Diphenhydramine hydrochloride helps symptoms", "01/01/2019", "Journal of emergency nursing", "HELPS"),
            (1, "Diphenhydramine hydrochloride helps symptoms", "01/01/2019", "Journal of emergency nursing",
             "SYMPTOMS"),
            (2, "Appositional Tetracycline bone formation rates", "01/01/2019", "Journal of emergency nursing",
             "APPOSITIONAL"),
            (2, "Appositional Tetracycline bone formation rates", "01/01/2019", "Journal of emergency nursing",
             "TETRACYCLINE"),
            (2, "Appositional Tetracycline bone formation rates", "01/01/2019", "Journal of emergency nursing", "BONE"),
            (2, "Appositional Tetracycline bone formation rates", "01/01/2019", "Journal of emergency nursing",
             "FORMATION"),
            (2, "Appositional Tetracycline bone formation rates", "01/01/2019", "Journal of emergency nursing", "RATES")
        ]
        expected_df = Session.get_session().createDataFrame(
            expected_data,
            ["id", "title", "date", "journal", "tokens"]
        )
        assert (expected_df.collect() == actual_df.collect())
