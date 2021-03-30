from pyspark.sql import SparkSession


class Session(object):
    _spark = None

    @staticmethod
    def get_session(master: str = 'local'):
        """ build single spark session if _spark is None otherwise return the existing sparkSession(_spark)
        :param master: master mode
        :return: sparkSession
        """
        if Session._spark is None:
            Session._spark = SparkSession.builder \
                .master(master) \
                .appName("servier-test") \
                .getOrCreate()

        return Session._spark
