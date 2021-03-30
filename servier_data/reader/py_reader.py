from pyspark.sql import SparkSession


def read_cvs(session: SparkSession, path: str, header: bool = True, delimiter: str = ","):
    """ Read csv format

    :param session: spark session
    :param path: path of file to load
    :param header: is file contains headers or  ot
    :param delimiter: columns delimiter
    :return: dataframe
    """
    return session.read.options(header=header, delimiter=delimiter) \
        .csv(path)
