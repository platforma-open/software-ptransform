#!/usr/bin/env python

import msgspec.json
import pandas as pd

import argparse

from workflow import Workflow

parser = argparse.ArgumentParser(
    description="Performs input table transformation according to the JSON workflow")
parser.add_argument("-w", "--workflow", help="workflow")
parser.add_argument("input", help="input tsv file")
parser.add_argument("output", help="output tsv file")

args = parser.parse_args()

data = pd.read_csv(args.input, sep='\t')
with open(args.workflow, "rb") as f:
    workflow = msgspec.json.decode(f.read(), type=Workflow)
result = workflow.apply(data)
result.to_csv(args.output, sep='\t', index=False)
