import argparse

import servier_data.cleaning.cleaner as C
import servier_data.reader.py_reader as R
import servier_data.reader.py_writer as W
from servier_data.enrichement.drug_linker import DrugLinker
from servier_data.session.spark_session import Session

DATE_PATTERNS = ["dd/MM/yyyy", "d MMMM yyyy", "yyyy-MM-dd"]


def parse_args():
    """ Parse argument: possible options are :
        --drug-path
        --clinical-trials-path
        --pubmed-path
    :return:
    """
    parser = argparse.ArgumentParser()
    parser.add_argument('--drug-path', required=True, type=str, help='drugs data files path')
    parser.add_argument('--clinical-trials-path', required=True, type=str, help='clinical trial data files path')
    parser.add_argument('--pubmed-path', required=True, type=str, help='pubmed data files path')
    parser.add_argument('--output-path', required=True, type=str, help='path to write the result')

    return parser.parse_args()


if __name__ == '__main__':
    # Load drugs existing on the given path
    args = parse_args()
    # for test we let sparkSession on local mode
    # for cluster(yarn, mesos) add parameter master='yarn' to get_session method
    spark = Session.get_session()
    df_drugs = R.read_cvs(session=spark, path=args.drug_path)
    clinical_trials = R.read_cvs(session=spark, path=args.clinical_trials_path)
    pubmed_df = R.read_cvs(session=spark, path=args.pubmed_path)

    clinical_trials = C.clean_dataset(clinical_trials, date_col='date', date_patterns=DATE_PATTERNS)
    pubmed_df = C.clean_dataset(pubmed_df, date_col='date', date_patterns=DATE_PATTERNS)
    clinic_drugs = DrugLinker(df_drugs, clinical_trials,
                              col_mapping={'id': 'id', 'scientific_title': 'title', 'date': 'date',
                                           'journal': 'journal'},
                              article_col_join='scientific_title', out_col_name='scientific_article')
    df_drugs_result = clinic_drugs.transform()

    pmd_drugs = DrugLinker(df_drugs, pubmed_df,
                           col_mapping={'id': 'id', 'title': 'title', 'date': 'date', 'journal': 'journal'},
                           article_col_join='title', out_col_name='med_article')
    df_pmd_result = pmd_drugs.transform()
    full_result = df_drugs_result.join(df_pmd_result, on='drug',
                                       how='full_outer')
    W.write_json(full_result, path=args.output_path)
