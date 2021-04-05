from servier_data.query.query import get_file, build_counters

CONTENT = "content"


class TestQuery(object):
    """
    Class to test query methods
    """

    def test_get_files(self, tmp_path):
        """ filter and get onlmy files with .json suffix

        :param tmp_path: pytest fixture (temporary directory)
        """
        d = tmp_path / "work"
        d.mkdir()
        txt_file = d / "hello.txt"
        json_file = d / "hello.json"
        svg_file = d / "hello.svg"
        txt_file.write_text(CONTENT)
        json_file.write_text(CONTENT)
        svg_file.write_text(CONTENT)
        files = get_file(str(d))
        expected = [str(d / "hello.json")]

        assert files == expected

    def test_build_counters(self, tmp_path):
        """ Build counter for each journal in the file.

        :param tmp_path: pytest fixture (temporary directory)
        """
        d = tmp_path / "work"
        d.mkdir()
        json_file = d / "hello.json"
        json_content = """{"drug":"ATROPINE","scientific_article":[{}],"med_article":[{}]}
{"drug":"BETAMETHASONE","scientific_article":[{"id":"NCT04153396","title":"Preemptive BETAMETHASONE","date":"2020-01-01","journal":"Hôpitaux Universitaires de Genève"}],"med_article":[{}]}
{"drug":"EPINEPHRINE","scientific_article":[{"id":"NCT04188184","title":"Preemptive EPINEPHRINE","date":"2020-04-27","journal":"Journal of emergency nursing"}],"med_article":[{"id":"7","title":"The High Cost of Epinephrine","date":"2020-02-01","journal":"The journal of allergy and clinical immunology. In practice"},{"id":"8","title":"Time to epinephrine treatment is associated with the risk of mortality in children who achieve sustained ROSC after traumatic out-of-hospital cardiac arrest.","date":"2020-03-01","journal":"The journal of allergy and clinical immunology. In practice"}]}
{"drug":"ISOPRENALINE","scientific_article":[{}],"med_article":[{}]}
{"drug":"TETRACYCLINE","scientific_article":[{}],"med_article":[{"id":"5","title":"Appositional Tetracycline bone formation rates in the Beagle.","date":"2020-01-02","journal":"American journal of veterinary research"},{"id":"4","title":"Tetracycline Resistance Patterns of Lactobacillus buchneri Group Strains.","date":"2020-01-01","journal":"Journal of food protection"},{"id":"6","title":"Rapid reacquisition of contextual fear following extinction in mice: effects of amount of extinction, tetracycline acute ethanol withdrawal, and ethanol intoxication.","date":"2020-01-01","journal":"Psychopharmacology"}]}
{"drug":"DIPHENHYDRAMINE","scientific_article":[{"id":"NCT04237091","title":"Feasibility of a Randomized Controlled Clinical Trial Comparing the Use of Cetirizine to Replace Diphenhydramine in the Prevention of Reactions Related to Paclitaxel","date":"2020-01-01","journal":"Journal of emergency nursing"},{"id":"NCT01967433","title":"Use of Diphenhydramine as an Adjunctive Sedative for Colonoscopy in Patients Chronically on Opioids","date":"2020-01-01","journal":"Journal of emergency nursing"},{"id":"NCT04189588","title":"Phase 2 Study IV QUZYTTIR™ (Cetirizine Hydrochloride Injection) vs V Diphenhydramine","date":"2020-01-01","journal":"Journal of emergency nursing"}],"med_article":[{"id":"3","title":"Diphenhydramine hydrochloride helps symptoms of ciguatera fish poisoning.","date":"2019-01-02","journal":"The Journal of pediatrics"},{"id":"2","title":"An evaluation of benadryl, pyribenzamine, and other so-called diphenhydramine antihistaminic drugs in the treatment of allergy.","date":"2019-01-01","journal":"Journal of emergency nursing"}]}
{"drug":"ETHANOL","scientific_article":[{}],"med_article":[{"id":"6","title":"Rapid reacquisition of contextual fear following extinction in mice: effects of amount of extinction, tetracycline acute ethanol withdrawal, and ethanol intoxication.","date":"2020-01-01","journal":"Psychopharmacology"}]}
"""
        json_file.write_text(json_content)
        result = build_counters(str(json_file))
        expected = {'Hôpitaux Universitaires de Genève': 1,
                    'Journal of food protection': 1, 'The journal of allergy and clinical immunology. In practice': 2,
                    'American journal of veterinary research': 1, 'Journal of emergency nursing': 5,
                    'Psychopharmacology': 2, 'The Journal of pediatrics': 1}
        assert result == expected
