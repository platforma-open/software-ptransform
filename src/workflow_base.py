from abc import abstractmethod

import msgspec
import pandas as pd


class WorkflowStepBase(msgspec.Struct, tag_field="type", rename="camel"):
    @abstractmethod
    def apply(self, data: pd.DataFrame) -> pd.DataFrame:
        pass
