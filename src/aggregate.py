from abc import abstractmethod
from typing import Union, cast, Optional

import msgspec
import pandas as pd
from pandas.core.groupby import DataFrameGroupBy, SeriesGroupBy

from workflow_base import WorkflowStepBase


class AggregationBase(msgspec.Struct, tag_field="type", rename="camel"):
    @abstractmethod
    def aggregate(self, grp_data: DataFrameGroupBy, data: pd.DataFrame, group_by: list[str]) -> list[
        pd.Series | pd.DataFrame]:
        pass


type AnyAggregation = Union[
    AggregationCount,
    AggregationMax, AggregationMin, AggregationMean, AggregationMedian, AggregationSum, AggregationFirst,
    AggregationMaxBy]

type AnyMultiAggregation = Union[MultiAggregationCumsum, MultiAggregationRank]


def _apply_aggregations(
        data: pd.DataFrame,
        group_by: list[str],
        aggregations: list[AggregationBase]) -> pd.DataFrame:
    results: list[pd.Series | pd.DataFrame] = []
    g = data.groupby(group_by)
    for agg in aggregations:
        r = agg.aggregate(g, data, group_by)
        results.extend(r)
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
                results[agg.dst] = agg.aggregate(g, data, 0)[0]
            return results


class ColumnAggregationBase(AggregationBase):
    src: str
    dst: str

    def _column_data(self, data: DataFrameGroupBy) -> SeriesGroupBy:
        return cast(SeriesGroupBy, data[self.src])

    def _aggregate_column(self, grp_data: DataFrameGroupBy, data) -> pd.Series:
        pass

    def aggregate(self, grp_data: DataFrameGroupBy, data: pd.DataFrame, group_by: list[str]) -> list[pd.Series]:
        s = self._aggregate_column(grp_data, data)
        s.name = self.dst
        return [s]


class AggregationMaxBy(AggregationBase, tag="max_by"):
    ranking_col: str
    pick_cols: Optional[list[tuple[str, str]]] = None

    def aggregate(self, grp_data: DataFrameGroupBy, data: pd.DataFrame, group_by: list[str]) -> list[pd.DataFrame]:
        idx = cast(SeriesGroupBy, grp_data[self.ranking_col]).idxmax()
        # result = list[pd.Series]()
        sliced_data = data.loc[idx, :]
        sliced_data.set_index(group_by, inplace=True)

        if self.pick_cols is None or len(self.pick_cols) == 0:
            return [sliced_data]
        else:
            sliced_data = sliced_data[[k for k, v in self.pick_cols]].copy(deep=False)
            sliced_data.columns = [v for k, v in self.pick_cols]
            return [sliced_data]


class AggregationMax(ColumnAggregationBase, tag="max"):
    def _aggregate_column(self, grp_data: DataFrameGroupBy, data: pd.DataFrame) -> pd.Series:
        return self._column_data(grp_data).max()


class AggregationMin(ColumnAggregationBase, tag="min"):
    def _aggregate_column(self, grp_data: DataFrameGroupBy, data: pd.DataFrame) -> pd.Series:
        return self._column_data(grp_data).min()


class AggregationMean(ColumnAggregationBase, tag="mean"):
    def _aggregate_column(self, grp_data: DataFrameGroupBy, data: pd.DataFrame) -> pd.Series:
        return self._column_data(grp_data).mean()


class AggregationMedian(ColumnAggregationBase, tag="median"):
    def _aggregate_column(self, grp_data: DataFrameGroupBy, data: pd.DataFrame) -> pd.Series:
        return self._column_data(grp_data).mean()


class AggregationSum(ColumnAggregationBase, tag="sum"):
    def _aggregate_column(self, grp_data: DataFrameGroupBy, data: pd.DataFrame) -> pd.Series:
        return self._column_data(grp_data).sum()


class AggregationFirst(ColumnAggregationBase, tag="first"):
    def _aggregate_column(self, grp_data: DataFrameGroupBy, data: pd.DataFrame) -> pd.Series:
        return self._column_data(grp_data).first()


class AggregationCount(ColumnAggregationBase, tag="count"):
    def _aggregate_column(self, grp_data: DataFrameGroupBy, data: pd.DataFrame) -> pd.Series:
        return self._column_data(grp_data).count()


class MultiAggregationCumsum(ColumnAggregationBase, tag="cumsum"):
    def _aggregate_column(self, grp_data: DataFrameGroupBy, data: pd.DataFrame) -> pd.Series:
        return self._column_data(grp_data).cumsum()


class MultiAggregationRank(ColumnAggregationBase, tag="rank"):
    def _aggregate_column(self, grp_data: DataFrameGroupBy, data: pd.DataFrame) -> pd.Series:
        return self._column_data(grp_data).rank()
