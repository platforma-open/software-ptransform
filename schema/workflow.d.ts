/**
 * Base interface for workflow steps, identified by a 'type' tag.
 */
export interface WorkflowStepBase {
  /** The type identifier for the workflow step. */
  type: string;
}

// --- Transformation Types ---

/**
 * Transformation step: Combines multiple source columns into a single destination column as a JSON array string.
 */
export interface TransformationCombineColumnsAsJson extends WorkflowStepBase {
  type: "combine_columns_as_json";
  /** List of source column names. */
  src: string[];
  /** Destination column name. */
  dst: string;
}

/**
 * Union type representing any possible transformation step.
 */
export type AnyTransformation = TransformationCombineColumnsAsJson;


// --- Filter Types ---

/**
 * Base interface for filter predicates, identified by a 'type' tag.
 */
export interface FilterBase {
  /** The type identifier for the filter predicate. */
  type: string;
}

/**
 * Base interface for filter predicates operating on a specific column.
 */
export interface ColumnFilterBase extends FilterBase {
  /** The column to apply the filter on. */
  column: string;
}

/**
 * Filter predicate: Checks if a column value equals the specified value.
 */
export interface FilterEquals extends ColumnFilterBase {
  type: "eq";
  /** The value to compare against. */
  value: number | string;
}

/**
 * Filter predicate: Checks if a column value is greater than the specified value.
 */
export interface FilterGreaterThan extends ColumnFilterBase {
  type: "gt";
  /** The value to compare against. */
  value: number | string;
}

/**
 * Filter predicate: Checks if a column value is greater than or equal to the specified value.
 */
export interface FilterGreaterThanOrEqual extends ColumnFilterBase {
  type: "ge";
  /** The value to compare against. */
  value: number | string;
}

/**
 * Filter predicate: Checks if a column value is less than the specified value.
 */
export interface FilterLessThan extends ColumnFilterBase {
  type: "lt";
  /** The value to compare against. */
  value: number | string;
}

/**
 * Filter predicate: Checks if a column value is less than or equal to the specified value.
 */
export interface FilterLessThanOrEqual extends ColumnFilterBase {
  type: "le";
  /** The value to compare against. */
  value: number | string;
}

/**
 * Filter predicate: Negates the result of its operand.
 */
export interface FilterNot extends FilterBase {
  type: "not";
  /** The filter predicate to negate. */
  operand: AnyFilter; // Recursive definition
}

/**
 * Filter predicate: Logical AND of multiple operands.
 */
export interface FilterAnd extends FilterBase {
  type: "and";
  /** List of filter predicates to combine with AND. */
  operands: AnyFilter[]; // Recursive definition
}

/**
 * Filter predicate: Logical OR of multiple operands.
 */
export interface FilterOr extends FilterBase {
  type: "or";
  /** List of filter predicates to combine with OR. */
  operands: AnyFilter[]; // Recursive definition
}

/**
 * Union type representing any possible filter predicate.
 */
export type AnyFilter =
  | FilterEquals
  | FilterGreaterThan
  | FilterGreaterThanOrEqual
  | FilterLessThan
  | FilterLessThanOrEqual
  | FilterNot
  | FilterAnd
  | FilterOr;

/**
 * Filter step: Applies a filter predicate to the data.
 */
export interface Filter extends WorkflowStepBase {
  type: "filter";
  /** The filter predicate to apply. */
  predicate: AnyFilter;
}


// --- Aggregation Types ---

/**
 * Base interface for aggregations, identified by a 'type' tag.
 */
export interface AggregationBase {
  /** The type identifier for the aggregation. */
  type: string;
}

/**
 * Base interface for aggregations operating on a specific source column and writing to a destination column.
 */
export interface ColumnAggregationBase extends AggregationBase {
  /** The source column for the aggregation. */
  src: string;
  /** The destination column for the result. */
  dst: string;
}

/**
 * Aggregation: Counts the number of non-null values in a group.
 */
export interface AggregationCount extends ColumnAggregationBase {
  type: "count";
}

/**
 * Aggregation: Finds the maximum value in a group.
 */
export interface AggregationMax extends ColumnAggregationBase {
  type: "max";
}

/**
 * Aggregation: Finds the minimum value in a group.
 */
export interface AggregationMin extends ColumnAggregationBase {
  type: "min";
}

/**
 * Aggregation: Calculates the mean (average) value in a group.
 */
export interface AggregationMean extends ColumnAggregationBase {
  type: "mean";
}

/**
 * Aggregation: Calculates the median value in a group.
 */
export interface AggregationMedian extends ColumnAggregationBase {
  type: "median";
}

/**
 * Aggregation: Calculates the sum of values in a group.
 */
export interface AggregationSum extends ColumnAggregationBase {
  type: "sum";
}

/**
 * Aggregation: Takes the first value encountered in a group.
 */
export interface AggregationFirst extends ColumnAggregationBase {
  type: "first";
}

/**
 * Aggregation: Selects rows based on the maximum value in a ranking column within each group.
 */
export interface AggregationMaxBy extends AggregationBase {
  type: "max_by";
  /** The column used for ranking to find the maximum. */
  ranking_col: string;
  /** Optional list of columns to pick from the selected rows, mapping source to destination names. If empty or null, all columns are kept. */
  pick_cols?: [string, string][];
}

/**
 * Union type representing any possible single-output aggregation.
 */
export type AnyAggregation =
  | AggregationCount
  | AggregationMax
  | AggregationMin
  | AggregationMean
  | AggregationMedian
  | AggregationSum
  | AggregationFirst
  | AggregationMaxBy;

/**
 * Aggregate step: Groups data and applies single-output aggregations.
 */
export interface Aggregate extends WorkflowStepBase {
  type: "aggregate";
  /** List of columns to group by. */
  group_by: string[];
  /** List of aggregations to apply. */
  aggregations: AnyAggregation[];
}


// --- Multi-Aggregation Types ---

/**
 * Base interface for multi-aggregations (those that typically preserve the original number of rows).
 */
export interface MultiAggregationBase extends AggregationBase {
    /** The source column for the aggregation. */
    src: string;
    /** The destination column for the result. */
    dst: string;
}

/**
 * Multi-Aggregation: Calculates the cumulative sum within each group.
 */
export interface MultiAggregationCumsum extends MultiAggregationBase {
  type: "cumsum";
}

/**
 * Multi-Aggregation: Calculates the rank within each group.
 */
export interface MultiAggregationRank extends MultiAggregationBase {
  type: "rank";
}

/**
 * Union type representing any possible multi-output aggregation (preserving row count).
 */
export type AnyMultiAggregation =
  | MultiAggregationCumsum
  | MultiAggregationRank;

/**
 * AggregateMulti step: Groups data and applies multi-output aggregations (preserving row count).
 */
export interface AggregateMulti extends WorkflowStepBase {
  type: "aggregate_multi";
  /** List of columns to group by. */
  group_by: string[];
  /** List of multi-aggregations to apply. */
  aggregations: AnyMultiAggregation[];
}


// --- Workflow Definition ---

/**
 * Union type representing any possible workflow step.
 */
export type AnyWorkflowStep =
  | Filter
  | Aggregate
  | AggregateMulti
  | AnyTransformation; // Using AnyTransformation directly as it's currently a single type union

/**
 * Represents a complete data processing workflow.
 */
export interface Workflow {
  /** A list of steps to be executed sequentially. */
  steps: AnyWorkflowStep[];
}
