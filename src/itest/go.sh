#!/usr/bin/env bash

python ../main.py -w params_1.json input.tsv output_1.tsv
python ../main.py -w params_2.json input.tsv output_2.tsv
python ../main.py -w params_1.json input_empty.tsv output_empty_1.tsv
python ../main.py -w params_2.json input_empty.tsv output_empty_2.tsv
