from abc import abstractmethod
from collections.abc import Collection
from typing import Union, cast

import msgspec
import pandas as pd

from workflow_base import WorkflowStepBase

type AnyFilter = Union[
    FilterEquals, FilterGreaterThan, FilterGreaterThanOrEqual, FilterLessThan, FilterLessThanOrEqual,
    FilterNot, FilterAnd, FilterOr]


class Filter(WorkflowStepBase, tag="filter"):
    predicate: AnyFilter

    def apply(self, data: pd.DataFrame) -> pd.DataFrame:
        pass


class FilterBase(msgspec.Struct, tag_field="type", rename="camel"):
    @abstractmethod
    def filter(self, table: pd.DataFrame) -> pd.Series:
        pass


class ColumnFilterBase(FilterBase):
    column: str

    def _column_data(self, table: pd.DataFrame) -> pd.Series:
        col = table[self.column]
        if col is None:
            raise ValueError("Column not found")
        return cast(pd.Series, col)


class FilterEquals(ColumnFilterBase, tag="eq"):
    value: Union[int, str]

    def filter(self, table: pd.DataFrame) -> pd.Series:
        return self._column_data(table).eq(self.value)


class FilterGreaterThan(ColumnFilterBase, tag="gt"):
    value: Union[int, str]

    def filter(self, table: pd.DataFrame) -> pd.Series:
        return self._column_data(table).gt(self.value)


class FilterGreaterThanOrEqual(ColumnFilterBase, tag="ge"):
    value: Union[int, str]

    def filter(self, table: pd.DataFrame) -> pd.Series:
        return self._column_data(table).ge(self.value)


class FilterLessThan(ColumnFilterBase, tag="lt"):
    value: Union[int, str]

    def filter(self, table: pd.DataFrame) -> pd.Series:
        return self._column_data(table).lt(self.value)


class FilterLessThanOrEqual(ColumnFilterBase, tag="le"):
    value: Union[int, str]

    def filter(self, table: pd.DataFrame) -> pd.Series:
        return self._column_data(table).le(self.value)


class FilterNot(FilterBase, tag="not"):
    operand: AnyFilter

    def filter(self, table: pd.DataFrame) -> pd.Series:
        return ~self.operand.filter(table)


class FilterAnd(FilterBase, tag="and"):
    operands: Collection[AnyFilter]

    def filter(self, table: pd.DataFrame) -> pd.Series:
        result: pd.Series | None = None
        for op in self.operands:
            if result is None:
                result = op.filter(table)
            else:
                result = result & op.filter(table)
        if result is None:
            raise ValueError("No operands")
        return result


class FilterOr(FilterBase, tag="or"):
    operands: Collection[AnyFilter]

    def filter(self, table: pd.DataFrame) -> pd.Series:
        result: pd.Series | None = None
        for op in self.operands:
            if result is None:
                result = op.filter(table)
            else:
                result = result | op.filter(table)
        if result is None:
            raise ValueError("No operands")
        return result
