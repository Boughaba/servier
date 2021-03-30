# Drugs usage graph(Servier drugs Linker)

This project is a __library__ that aims to provide a way to link drugs , clinic trails and publications.

It's mainly based on inputs CSV files with specific structure.

## Requirements
* Python>=3.7.5(Please download the appropriate according to your platform)
* asdf(refer to : https://asdf-vm.com/#/core-manage-asdf)  
* Link asdf with python (Running `asdf plugin add python`)
* Install python version(Running `asdf install python 3.8.0` & `asdf install python 3.7.5`)
* Tox(Running `pip install tox `)
* Tox(Running `pip install poetry`)
* activate virtualenv(on the project root Run `poetry shell`)


## Running tests
Simply run `tox` command from root directory:

```tox```

## Packaging

### Packaging Command :
```
poetry build
```

### run example
```
python main.py  --drug-path data/drugs.csv --clinical-trials-path data/clinical_trials.csv --pubmed-path data/pubmed.csv  --output-path out/merge2/
```
## Improvements
* Add more tests
* Add more cleaning rules
* Redesign spark session generator to have more configs
* *Flake8* integration(It could be done through *tox*)