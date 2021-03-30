def write_csv(df, path: str, header: bool = True, delimiter: str = ",", file_numbers: int = 1):
    """ Write dataframein csv format

    :param df: dataframe
    :param path: path to write in
    :param header: include header on files or not
    :param delimiter: columns delimiter
    :param file_numbers: number of output files, default 1
    :return:
    """
    return df.repartition(file_numbers).write.mode('overwrite').options(header=header, delimiter=delimiter) \
        .csv(path)


def write_json(df, path: str, file_numbers: int = 1):
    """Write dataframein json format

    :param df: dataframe
    :param path: path to write in
    :param file_numbers: number of output files, default 1
    :return:
    """
    return df.repartition(file_numbers).write.mode('overwrite').json(path)
