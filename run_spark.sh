
mkdir output > /dev/null
rm output/*

env/bin/python3 src/ccda_to_omop_spark/convert_1.py > convert_1.out 2> convert_1.err

