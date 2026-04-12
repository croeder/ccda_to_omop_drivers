
mkdir output > /dev/null
rm output/*

python3 src/ccda_to_omop_spark/convert_1.py -d ~/git/CCDA-data/resources  > convert_1.out 2> convert_1.err

