#!/usr/bin/env bash

python ../main.py -w params_1.json input.tsv output_1.tsv
python ../main.py -w params_2.json input.tsv output_2.tsv
python ../main.py -w params_2.json input.tsv input.tsv input_empty.tsv output_2_concat.tsv
python ../main.py -w params_3.json input.tsv output_3.tsv
python ../main.py -w params_4.json input.tsv output_4.tsv
python ../main.py -w params_5.json input.tsv output_5.tsv
python ../main.py -w params_6.json input.tsv output_6.tsv
python ../main.py -w params_7.json input.tsv output_7.tsv
python ../main.py -w params_1.json input_empty.tsv output_empty_1.tsv
python ../main.py -w params_2.json input_empty.tsv output_empty_2.tsv

