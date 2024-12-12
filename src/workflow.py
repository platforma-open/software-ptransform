from functools import reduce
from typing import Union

import msgspec
import pandas as pd

from aggregate import Aggregate, AggregateMulti
from filter import Filter

type AnyWorkflowStep = Union[Filter, Aggregate, AggregateMulti]


class Workflow(msgspec.Struct):
    steps: list[AnyWorkflowStep]

    def apply(self, data: pd.DataFrame) -> pd.DataFrame:
        for step in self.steps:
            data = step.apply(data)
        return data
