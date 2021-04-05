import operator
from typing import Dict, List

from servier_data.query.query import build_counters, get_file


def merge_dictionaries(dics: List[Dict]):
    print("Implementt this to merge result of multiple json files")
    pass


if __name__ == '__main__':
    files = get_file("out/merge2")
    dictionaries = []
    for fl in files:
        result = build_counters(fl)
        dictionaries.append(result)
        sort_dict = sorted(result.items(), key=operator.itemgetter(1), reverse=True)
    print(f'The top journal is : {sort_dict[0][0]}, with reference to {sort_dict[0][1]} drugs')
