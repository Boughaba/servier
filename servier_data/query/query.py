import fnmatch
import json
import os
from typing import Dict

DEFAULT_PATH = 'out/merge_test'


def get_file(path: str = DEFAULT_PATH):
    """ Get list of path files given a directory path

    :param path: directory path
    :return: list of list files path
    """
    files = []
    list_files = os.listdir(path)
    pattern = "*.json"
    for entry in list_files:
        if fnmatch.fnmatch(entry, pattern):
            files.append(path + "/" + entry)
    return files


def _update_journal_counters(counters: Dict[str, int], journals):
    for obj in journals:
        has_items = bool(obj)
        if has_items:
            journal = obj['journal']
            if journal in counters:
                counters[journal] += 1
            else:
                counters[journal] = 1


def build_counters(path: str) -> Dict[str, int]:
    """ Count the number of apparition for each journal in the given file.
    it merge counters on scientific articles and clinical med

    :param path: path file to operate on
    :return: Dictionary of journal name and their counters
    """
    journal_counter: Dict[str, int] = {}
    with open(path) as fp:
        lines = fp.readlines()
        for line in lines:
            json_line = json.loads(line)
            scientific_articles = json_line["scientific_article"]
            _update_journal_counters(counters=journal_counter, journals=scientific_articles)
            med_articles = json_line["med_article"]
            _update_journal_counters(counters=journal_counter, journals=med_articles)
    return journal_counter
