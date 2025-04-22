import json
from typing import Union

import pandas as pd

from workflow_base import WorkflowStepBase
from custom_json_encoder import NumpyEncoder

type AnyTransformation = Union[TransformationCombineColumnsAsJson]


class TransformationCombineColumnsAsJson(WorkflowStepBase, tag="combine_columns_as_json"):
    src: list[str]
    dst: str

    def apply(self, data: pd.DataFrame) -> pd.DataFrame:
        data_copy = data.copy(deep=False)
        src = self.src
        data_copy[self.dst] = data_copy.loc[:, src] \
            .apply(lambda x: json.dumps([x[c] for c in src], separators=(',', ':'), cls=NumpyEncoder), axis=1)
        # .astype('string')
        return data_copy
