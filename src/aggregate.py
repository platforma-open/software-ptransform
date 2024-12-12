from abc import ABCMeta, abstractmethod, ABC
from typing import Union, cast

import msgspec
import pandas as pd
from pandas.core.groupby import DataFrameGroupBy, SeriesGroupBy

from workflow_base import WorkflowStepBase


class AggregationBase(msgspec.Struct, tag_field="type", rename="camel"):
    dst: str

    @abstractmethod
    def aggregate(self, data: DataFrameGroupBy) -> pd.Series:
        pass


type AnyAggregation = Union[AggregationSum, AggregationFirst]

type AnyMultiAggregation = Union[MultiAggregationCumsum, MultiAggregationRank]


def _apply_aggregations(
        data: pd.DataFrame,
        group_by: list[str],
        aggregations: list[AggregationBase]) -> pd.DataFrame:
    results: list[pd.Series] = []
    g = data.groupby(group_by)
    for agg in aggregations:
        r = agg.aggregate(g)
        r.name = agg.dst
        results.append(r)
    return pd.concat(results, axis=1).reset_index()


class Aggregate(WorkflowStepBase, tag="aggregate"):
    group_by: list[str]
    aggregations: list[AnyAggregation]

    def apply(self, data: pd.DataFrame) -> pd.DataFrame:
        return _apply_aggregations(data, self.group_by, self.aggregations)


class AggregateMulti(WorkflowStepBase, tag="aggregate_multi"):
    group_by: list[str]
    aggregations: list[AnyMultiAggregation]

    def apply(self, data: pd.DataFrame) -> pd.DataFrame:
        results = data.copy()
        if len(data) == 0:
            for agg in self.aggregations:
                results[agg.dst] = []
            return results
        else:
            g = data.groupby(self.group_by)
            for agg in self.aggregations:
                results[agg.dst] = agg.aggregate(g)
            return results


class ColumnAggregationBase(AggregationBase):
    src: str

    def _column_data(self, data: DataFrameGroupBy) -> SeriesGroupBy:
        return cast(SeriesGroupBy, data[self.src])


class AggregationSum(ColumnAggregationBase, tag="sum"):
    def aggregate(self, data: DataFrameGroupBy) -> pd.Series:
        return self._column_data(data).sum()


class AggregationFirst(ColumnAggregationBase, tag="first"):
    def aggregate(self, data: DataFrameGroupBy) -> pd.Series:
        return self._column_data(data).first()


class MultiAggregationCumsum(ColumnAggregationBase, tag="cumsum"):
    def aggregate(self, data: DataFrameGroupBy) -> pd.Series:
        return self._column_data(data).cumsum()


class MultiAggregationRank(ColumnAggregationBase, tag="rank"):
    def aggregate(self, data: DataFrameGroupBy) -> pd.Series:
        return self._column_data(data).rank()
