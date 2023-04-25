import tracdap.rt.api as trac
import typing as tp
from testing_models import schemas as schemas
import datetime


# A model that has parameters that cover all the parameter types except maps with and without defaults. This can be used
# to test API requests to run this model.
class Wrapper(trac.TracModel):

    def define_parameters(self) -> tp.Dict[str, trac.ModelParameter]:
        string_list_type = trac.TypeDescriptor(trac.BasicType.ARRAY, arrayType=trac.TypeDescriptor(trac.BasicType.STRING))
        integer_list_type = trac.TypeDescriptor(trac.BasicType.ARRAY, arrayType=trac.TypeDescriptor(trac.BasicType.INTEGER))
        float_list_type = trac.TypeDescriptor(trac.BasicType.ARRAY, arrayType=trac.TypeDescriptor(trac.BasicType.FLOAT))
        date_list_type = trac.TypeDescriptor(trac.BasicType.ARRAY, arrayType=trac.TypeDescriptor(trac.BasicType.DATE))
        datetime_list_type = trac.TypeDescriptor(trac.BasicType.ARRAY, arrayType=trac.TypeDescriptor(trac.BasicType.DATETIME))

        return trac.define_parameters(

            trac.P("date_with_default", trac.DATE, label="Date with default", default_value=datetime.datetime(2022, 1, 1).date()),
            trac.P("date_without_default", trac.DATE, label="Date without default"),

            trac.P("string_with_default", trac.STRING, label="String with default", default_value="Test string"),
            trac.P("string_without_default", trac.STRING, label="String without default"),

            trac.P("integer_with_default", trac.INTEGER, label="Integer with default", default_value=1),
            trac.P("integer_without_default", trac.INTEGER, label="Integer without default"),

            trac.P("float_with_default", trac.FLOAT, label="Float with default", default_value=1.1),
            trac.P("float_without_default", trac.FLOAT, label="Float without default"),

            trac.P("decimal_with_default", trac.DECIMAL, label="Decimal with default", default_value=1.11),
            trac.P("decimal_without_default", trac.DECIMAL, label="Decimal without default"),

            trac.P("datetime_with_default", trac.DATETIME, label="Datetime with default", default_value=datetime.datetime(2022, 1, 1)),
            trac.P("datetime_without_default", trac.DATETIME, label="Datetime without default"),

            trac.P("boolean_with_default", trac.BOOLEAN, label="Boolean with default", default_value=True),
            trac.P("boolean_without_default", trac.BOOLEAN, label="Boolean without default"),

            # trac.P("date_list_with_default", date_list_type, label="List of dates with default",
            #        default_value=[datetime.datetime(2022, 1, 1).date(), datetime.datetime(2022, 2, 1).date(), datetime.datetime(2022, 3, 1).date()]),
            #trac.P("date_list_without_default", date_list_type, label="List of dates with without default"),

            #trac.P("string_list_with_default", string_list_type, label="List of strings with default", default_value=["a", "b", "c"]),
            trac.P("string_list_without_default", string_list_type, label="List of strings without default"),

            # trac.P("integer_list_with_default", integer_list_type, label="List of integers with default", default_value=[1, 2, 3]),
            # trac.P("integer_list_without_default", integer_list_type, label="List of integers without default"),

            # trac.P("float_list_with_default", float_list_type, label="List of floats with default", default_value=[1.1, 2.1, 3.1]),
            # trac.P("float_list_without_default", float_list_type, label="List of floats with without default"),

            # trac.P("decimal_list_with_default", float_list_type, label="List of decimals with default", default_value=[1.11,2.11, 3.11]),
            # trac.P("decimal_list_without_default", float_list_type, label="List of decimals with without default"),

            # trac.P("datetime_list_with_default", datetime_list_type, label="List of datetimes with default", default_value=[datetime.datetime(2022, 1, 1), datetime.datetime(2022, 2, 1), datetime.datetime(2022, 3, 1)]),
            # trac.P("datetime_list_without_default", datetime_list_type, label="List of datetimes with without default"),

            # trac.P("boolean_list_with_default", float_list_type, label="List of booleans with default", default_value=[True, False, True, False]),
            # trac.P("boolean_list_without_default", float_list_type, label="List of booleans with without default"),
        )

    def define_inputs(self) -> tp.Dict[str, trac.ModelInputSchema]:
        test_input_schema = trac.load_schema(schemas, "test_schema.csv")

        return {"test_input": trac.ModelInputSchema(test_input_schema)}

    def define_outputs(self) -> tp.Dict[str, trac.ModelOutputSchema]:
        test_output_schema = trac.load_schema(schemas, "test_schema.csv")

        return {"test_output": trac.ModelOutputSchema(test_output_schema)}

    def run_model(self, ctx: trac.TracContext):
        # Load the inputs
        test_input = ctx.get_pandas_table("test_input")

        # Load the parameters
        variables = [
            "date_with_default", "date_without_default",
            "string_with_default", "string_without_default",
            "integer_with_default", "integer_without_default",
            "float_with_default", "float_without_default",
            "decimal_with_default", "decimal_without_default",
            "datetime_with_default", "datetime_without_default",
            "boolean_with_default", "boolean_without_default",
        ]

        for variable in variables:
            loaded_value = ctx.get_parameter(variable)
            ctx.log().info(variable + ": %s", str(loaded_value))

        # Output the dataset
        ctx.put_pandas_table("test_output", test_input)


if __name__ == "__main__":
    import tracdap.rt.launch as launch

    launch.launch_model(Wrapper, "config/test_models/test_model_a.yaml", "config/sys_config.yaml")
