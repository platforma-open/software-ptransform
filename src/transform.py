import json
from typing import Union

import pandas as pd

from workflow_base import WorkflowStepBase

type AnyTransformation = Union[TransformationCombineColumnsAsJson]


class TransformationCombineColumnsAsJson(WorkflowStepBase, tag="combine_columns_as_json"):
    src: list[str]
    dst: str

    def apply(self, data: pd.DataFrame) -> pd.DataFrame:
        data_copy = data.copy(deep=False)
        src = self.src
        data_copy[self.dst] = data_copy.loc[:, src] \
            .apply(lambda x: json.dumps([x[c] for c in src], separators=(',', ':')), axis=1)
        # .astype('string')
        return data_copy
