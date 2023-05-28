# Load a plugin to allow typing
import typing as tp

# Load the python libraries
import numpy as np
import pandas as pd
# Load the TRAC runtime library
import tracdap.rt.api as trac

# Load the schemas library
from impairment import schemas as schemas
# Load a set of utils for handling TRAC schemas
from utils.utils_trac_schema import TracSchemaUtils

"""
A model that creates a data quality report on a dataset. 
"""


# TODO when dynamic schemas are available in the TRAC API then this will work for any dataset. Until then this model is
#  coded to a specific dataset's schema.
# TODO when spark models are available in the TRAC API then this need to be rewritten to work for larger datasets handled in spark.


class Wrapper(trac.TracModel):

    # Set the model parameters
    def define_parameters(self) -> tp.Dict[str, trac.ModelParameter]:
        return trac.declare_parameters(
        )

    # Set the model input datasets
    def define_inputs(self) -> tp.Dict[str, trac.ModelInputSchema]:

        data_schema = trac.load_schema(schemas, "mortgage_book_t0_schema.csv")

        return {"data": trac.ModelInputSchema(data_schema)}

    # Set the model output datasets
    def define_outputs(self) -> tp.Dict[str, trac.ModelOutputSchema]:

        data_quality_report_schema = [
            trac.F(field_name="field_name", field_type=trac.STRING, field_order=0, label="Name", categorical=True),
            trac.F(field_name="label", field_type=trac.STRING, field_order=1, label="Label", categorical=False),
            trac.F(field_name="field_type", field_type=trac.STRING, field_order=2, label="Field type", categorical=True),
            trac.F(field_name="field_order", field_type=trac.INTEGER, field_order=3, label="Field order"),
            trac.F(field_name="categorical", field_type=trac.BOOLEAN, field_order=4, label="Categorical"),
            trac.F(field_name="count_not_null", field_type=trac.FLOAT, field_order=5, label="Count of non-missing values"),
            trac.F(field_name="count_null", field_type=trac.FLOAT, field_order=6, label="Count of missing values"),
            trac.F(field_name="count_infinite", field_type=trac.FLOAT, field_order=7, label="Count of infinite values (numbers only"),
            trac.F(field_name="count_less_than_zero", field_type=trac.FLOAT, field_order=8, label="Count of negative values (numbers only)"),
            trac.F(field_name="count_equals_zero", field_type=trac.FLOAT, field_order=9, label="Count of zero values (numbers only)"),
            trac.F(field_name="count_more_than_zero", field_type=trac.FLOAT, field_order=10, label="Count of positive values (numbers only)"),
            trac.F(field_name="count_more_than_one", field_type=trac.FLOAT, field_order=11, label="Count greater than one (numbers only)"),
            trac.F(field_name="count_empty_string", field_type=trac.FLOAT, field_order=12, label="Count empty strings (strings only)"),
            trac.F(field_name="count_unique_values", field_type=trac.FLOAT, field_order=13, label="Count of unique value"),
            trac.F(field_name="sum", field_type=trac.FLOAT, field_order=18, label="Sum (numbers only)"),
            trac.F(field_name="minimum_number_value", field_type=trac.FLOAT, field_order=14, label="Minimum value (numbers & dates only)"),
            trac.F(field_name="maximum_number_value", field_type=trac.FLOAT, field_order=15, label="Maximum value (numbers & dates only)"),
            trac.F(field_name="mean_number_value", field_type=trac.FLOAT, field_order=17, label="Mean value (float/decimals only)"),
            trac.F(field_name="median_number_value", field_type=trac.FLOAT, field_order=16, label="Median value (integers only)"),
            trac.F(field_name="minimum_date_value", field_type=trac.DATE, field_order=14, label="Minimum value (numbers & dates only)"),
            trac.F(field_name="maximum_date_value", field_type=trac.DATE, field_order=15, label="Maximum value (numbers & dates only)"),
            trac.F(field_name="median_date_value", field_type=trac.DATE, field_order=16, label="Median value (integers only)"),
            trac.F(field_name="minimum_datetime_value", field_type=trac.DATETIME, field_order=14, label="Minimum value (numbers & dates only)"),
            trac.F(field_name="maximum_datetime_value", field_type=trac.DATETIME, field_order=15, label="Maximum value (numbers & dates only)"),
            trac.F(field_name="median_datetime_value", field_type=trac.DATETIME, field_order=16, label="Median value (integers only)")
        ]

        return {"data_quality_report": trac.define_output_table(data_quality_report_schema, label="Data quality report")}

    @staticmethod
    def extract_columns_into_single_statistic(df: pd.DataFrame, column_name_part: str) -> pd.Series:
        """
        A function used to create a new series for an aggregation statistic from the series available. If we have two numeric variables 'A' and 'B'
        then we will have 'sum_A' and 'sum_B' columns in the dataset. This function creates the 'sum' column which coalesces the values from the right
        columns. It uses the 'field_name column to determine which values to take.
        :param df: The dataFrame to coalesce the values from.
        :param column_name_part: The first part of the column to get the values for, in the example above this will be 'sum_'
        :return: A series with the coalesced values.
        """
        return df.apply(lambda row: pd.NA if (column_name_part + row["field_name"] not in df.columns) else row[column_name_part + row["field_name"]], axis=1)

    def run_model(self, ctx: trac.TracContext):

        # Load the input data
        data = ctx.get_pandas_table("data")

        # Load the schema for the input data
        # TODO this schema is fixed at the moment but when dynamic schemas are available in the
        #  API we will be able to define a schema at runtime
        data_schema = ctx.get_schema("data")

        # A dictionary of variable name lists broken down by type
        schema_breakdown = TracSchemaUtils.get_trac_schema_type_breakdown(data_schema)

        # Convert the schema to an equivalent dataFrame, used to add columns into the output dataset
        schema_as_data = TracSchemaUtils.convert_schema_into_dataframe(data_schema)

        #########################################################################################
        #                     FLAG STATISTICS FOR FLOAT/DECIMAL/INTEGER VARIABLES               #
        #########################################################################################

        numeric_flag_data_quality_report = pd.DataFrame()

        for variable in (schema_breakdown[trac.FLOAT] + schema_breakdown[trac.DECIMAL] + schema_breakdown[trac.INTEGER]):
            # This is just an empty version of the input data but the index and the size of this index is the same as
            # what's needed now, we recreate it for each variable being processed
            data_temp = data.drop(columns=data.columns)

            # Create the required true/false flag columns
            data_temp["count_not_null"] = (data[variable].notnull()) & (~data[variable].map(pd.isna))
            data_temp["count_null"] = (data[variable].isnull()) | (data[variable].map(pd.isna))
            data_temp["count_infinite"] = (data[variable] == np.inf) | (data[variable] == -1 * np.inf)
            data_temp["count_less_than_zero"] = data[variable] < 0
            data_temp["count_equals_zero"] = data[variable] == 0
            data_temp["count_more_than_zero"] = data[variable] > 0
            data_temp["count_more_than_one"] = data[variable] > 1

            # A dictionary of the flag based statistics that need a column to be calculated first before a statistic
            # can be calculated. While we could wrap some of these into numeric_aggregation_statistic we would need
            # to use lambda functions which would be slower.
            numeric_flag_statistics = {
                'count_not_null': 'sum',
                'count_null': 'sum',
                'count_infinite': 'sum',
                'count_less_than_zero': 'sum',
                'count_equals_zero': 'sum',
                'count_more_than_zero': 'sum',
                'count_more_than_one': 'sum'
            }

            # Calculate the numeric flag statistics against the new temporary dataset that
            # had the new required metrics calculated on it
            numeric_flag_data_quality_report[variable] = data_temp.agg(numeric_flag_statistics)

        #########################################################################################
        #                           FLAG STATISTICS FOR STRING VARIABLES                        #
        #########################################################################################

        string_flag_data_quality_report = pd.DataFrame()

        for variable in schema_breakdown[trac.STRING]:
            # This is just an empty version of the input data but the index and the size of this index is the same as
            # what's needed now, we recreate it for each variable being processed
            data_temp = data.drop(columns=data.columns)

            # Create the required true/false flag columns
            # Note we remove whitespace in 'count_empty_string'
            data_temp["count_not_null"] = data[variable].notnull()
            data_temp["count_null"] = data[variable].isnull()
            data_temp["count_empty_string"] = data[variable].str.strip() == ""

            # A dictionary of the flag based statistics that need a column to be calculated first before a statistic
            # can be calculated.
            string_flag_statistics = {
                'count_not_null': 'sum',
                'count_null': 'sum',
                'count_empty_string': 'sum'
            }

            # Calculate the numeric flag statistics against the new temporary dataset that
            # had the new required metrics calculated on it
            string_flag_data_quality_report[variable] = data_temp.agg(string_flag_statistics)

        #########################################################################################
        #                   FLAG STATISTICS FOR BOOLEAN/DATE/DATETIME VARIABLES                 #
        #########################################################################################

        boolean_and_date_flag_data_quality_report = pd.DataFrame()

        for variable in (schema_breakdown[trac.BOOLEAN] + schema_breakdown[trac.DATE] + schema_breakdown[trac.DATETIME]):
            # This is just an empty version of the input data but the index and the size of this index is the same as
            # what's needed now, we recreate it for each variable being processed
            data_temp = data.drop(columns=data.columns)

            # Create the required true/false flag columns
            data_temp["count_not_null"] = data[variable].notnull()
            data_temp["count_null"] = data[variable].isnull()

            # A dictionary of the flag based statistics that need a column to be calculated first before a statistic
            # can be calculated.
            boolean_and_date_flag_statistics = {
                'count_not_null': 'sum',
                'count_null': 'sum'
            }

            # Calculate the numeric flag statistics against the new temporary dataset that
            # had the new required metrics calculated on it
            boolean_and_date_flag_data_quality_report[variable] = data_temp.agg(boolean_and_date_flag_statistics)

        #########################################################################################
        #                       NUMERIC AGGREGATION DATA QUALITY METRICS                        #
        #########################################################################################

        # A dictionary of the aggregation statistics for standard numeric calculations e.g. sum
        # applied to all types of variables
        numeric_aggregation_statistics = dict()

        # Calculate the standard numeric statistics
        for column in data.columns:

            ctx.log().info("Adding aggregation statistics for %s", column)

            # nan values are valid float dtypes but with a value set to nan. These behave differently to null values.
            # It's really confusing to the user when they look at the report, and it's polluted by null, nan and inf,
            # so we remove these so that the aggregation stats are all presented on a common basis and relate to on;y
            # real numbers
            if column in schema_breakdown[trac.FLOAT] + schema_breakdown[trac.DECIMAL]:
                data[column] = data[column].map(lambda x: pd.NA if pd.isna(x) or (x == np.inf or x == -1 * np.inf) else x)

            if column in schema_breakdown[trac.FLOAT] + schema_breakdown[trac.DECIMAL]:

                # Mean is used for floats and decimals
                numeric_aggregation_statistics["mean_number_value_" + column] = (column, "mean")
                numeric_aggregation_statistics["sum_" + column] = (column, "sum")
                numeric_aggregation_statistics["minimum_number_value_" + column] = (column, "min")
                numeric_aggregation_statistics["maximum_number_value_" + column] = (column, "max")
                # Use the builtin unique count as this can handle columns with NA in them
                numeric_aggregation_statistics["count_unique_values_" + column] = (column, "nunique")

            elif column in schema_breakdown[trac.INTEGER]:

                # Median is used for integers
                numeric_aggregation_statistics["median_number_value_" + column] = (column, "median")
                numeric_aggregation_statistics["minimum_number_value_" + column] = (column, "min")
                numeric_aggregation_statistics["maximum_number_value_" + column] = (column, "max")
                # Use the builtin unique count as this can handle columns with NA in them
                numeric_aggregation_statistics["count_unique_values_" + column] = (column, "nunique")

            # Strings and booleans don't have all aggregation statistics applied as they can't be calculated
            elif column in schema_breakdown[trac.STRING] + schema_breakdown[trac.BOOLEAN]:

                numeric_aggregation_statistics["count_unique_values_" + column] = (column, "nunique")

            elif column in schema_breakdown[trac.DATE] + schema_breakdown[trac.DATETIME]:

                numeric_aggregation_statistics["count_unique_values_" + column] = (column, "nunique")
                numeric_aggregation_statistics["median_date_value_" + column] = (column, "median")
                numeric_aggregation_statistics["minimum_date_value_" + column] = (column, "min")
                numeric_aggregation_statistics["maximum_date_value_" + column] = (column, "max")

            elif column in schema_breakdown[trac.DATETIME] + schema_breakdown[trac.DATETIME]:

                numeric_aggregation_statistics["count_unique_values_" + column] = (column, "nunique")
                numeric_aggregation_statistics["median_datetime_value_" + column] = (column, "median")
                numeric_aggregation_statistics["minimum_datetime_value_" + column] = (column, "min")
                numeric_aggregation_statistics["maximum_datetime_value_" + column] = (column, "max")

        # Calculate the standard numeric statistics in one pass, this should be faster
        # Note the columns are the variable names and the index is the statistics with the column name appended, so
        # we transpose so the columns are the statistics, this makes the final output have a data independent schema
        # The ** is a clever way of deconstructing the numeric_aggregation_statistics dictionary into a series of
        # arguments to the agg function. This means we can name each statistic (although because these have to be
        # unique we have to append the variable name - which means we have a little extra work on the summary
        # statistic data to reduce it to a summary dataFrame)
        aggregation_data_quality_report = data.agg(**numeric_aggregation_statistics).T.reset_index(names="field_name")

        # This is a list of the final set of aggregation statistics we want to have on the final version of
        # 'aggregation_data_quality_report'.
        list_to_extract = ["count_unique_values", "sum", "minimum_number_value", "maximum_number_value", "mean_number_value", "median_number_value", "minimum_date_value", "maximum_date_value",
                           "median_date_value", "minimum_datetime_value", "maximum_datetime_value", "median_datetime_value"]

        # For each statistic we want to coalesce the values from across the columns (which are per variable)
        for item in list_to_extract:
            aggregation_data_quality_report[item] = Wrapper.extract_columns_into_single_statistic(aggregation_data_quality_report, item + "_")

        # Put back the index we had
        aggregation_data_quality_report = aggregation_data_quality_report.set_index(keys="field_name", drop=True, inplace=False)

        # Keep everything we need
        aggregation_data_quality_report = aggregation_data_quality_report[list_to_extract]

        # Make sure that the date column types are dates rather than datetime values
        aggregation_data_quality_report["minimum_date_value"] = aggregation_data_quality_report["minimum_date_value"].date.date()
        aggregation_data_quality_report["maximum_date_value"] = aggregation_data_quality_report["maximum_date_value"].date.date()
        aggregation_data_quality_report["median_date_value"] = aggregation_data_quality_report["median_date_value"].date.date()

        # Join all the flag metrics into a single dataFrame
        flag_data_quality_report = pd.concat([numeric_flag_data_quality_report.T, string_flag_data_quality_report.T, boolean_and_date_flag_data_quality_report.T], axis=0)

        # Merge the flag data quality results to the schema, if there are no flags configured for a particular colum in the
        # data then it's still in the output, just with no data quality information
        data_quality_report = pd.merge(schema_as_data, flag_data_quality_report, left_index=True, right_index=True, how="left")

        # Merge the flag and aggregation statistics together by index, so they are on the same row per variable
        data_quality_report = pd.merge(data_quality_report, aggregation_data_quality_report, left_index=True, right_index=True)

        # Output the dataset
        ctx.put_pandas_table("data_quality_report", data_quality_report)


if __name__ == "__main__":
    import tracdap.rt.launch as launch

    launch.launch_model(Wrapper, "config/data_quality/calculate_data_quality_metrics_python.yaml", "config/sys_config.yaml")
