import unittest

import msgspec.json
import pandas as pd

from aggregate import Aggregate, AggregationSum, AggregationFirst, AggregateMulti, MultiAggregationCumsum, \
    MultiAggregationRank
from workflow import Workflow


class MyTestCase(unittest.TestCase):
    def test_simple_wf1(self):
        df = pd.DataFrame({'k1': ['a', 'a', 'a', 'a', 'b', 'b', 'b', 'b'],
                           'k2': ['a', 'a', 'b', 'b', 'a', 'a', 'b', 'b'],
                           'v1': [1., 2., 3., 4., 5., 6., 7., 8.], })
        wf = Workflow(
            [Aggregate(['k1'], [
                AggregationSum(src='v1', dst='v1_sum'),
                AggregationFirst(src='v1', dst='v1_first')])])

        serialized = msgspec.json.encode(wf)
        print(serialized)
        wf_deserialized = msgspec.json.decode(serialized, type=Workflow)

        result = wf_deserialized.apply(df)
        pd.testing.assert_frame_equal(
            result,
            pd.DataFrame({'k1': ['a', 'b'],
                          'v1_sum': [10., 26.],
                          'v1_first': [1., 5.], }))

    def test_simple_wf2(self):
        df = pd.DataFrame({'k1': ['a', 'a', 'a', 'a', 'b', 'b', 'b', 'b'],
                           'k2': ['a', 'a', 'b', 'b', 'a', 'a', 'b', 'b'],
                           'v1': [1., 2., 3., 4., 5., 6., 7., 8.], })
        wf = Workflow(
            [AggregateMulti(['k1'], [
                MultiAggregationCumsum(src='v1', dst='v1_cumsum'),
                MultiAggregationRank(src='v1', dst='v1_rank')])])

        serialized = msgspec.json.encode(wf)
        print(serialized)
        wf_deserialized = msgspec.json.decode(serialized, type=Workflow)

        result = wf_deserialized.apply(df)
        pd.testing.assert_frame_equal(
            result,
            pd.DataFrame({'k1': ['a', 'a', 'a', 'a', 'b', 'b', 'b', 'b'],
                          'k2': ['a', 'a', 'b', 'b', 'a', 'a', 'b', 'b'],
                          'v1': [1., 2., 3., 4., 5., 6., 7., 8.],
                          'v1_cumsum': [1., 3., 6., 10., 5., 11., 18., 26.],
                          'v1_rank': [1., 2., 3., 4., 1., 2., 3., 4.],
                          }))


if __name__ == '__main__':
    unittest.main()
