# ccda_to_omop_drivers
# links and scripts to run the ccda_to_omop code from plain Python and Spark
# ccda_to_omop is a package on pypi

- need data from https://github.com/cladteam/CCDA-data
- need vocabularies from https://github.com/cladteam/CCDA_OMOP_Private
  - but it's a private repo, so  you'll have to get your own concept table from OMOP

Run here with:
python3  src/ccda_to_omop_simple/test.py -d <path-to>/CCDA-data/resources
