# Load a plugin to allow typing
import typing as tp

# Load the python libraries
import pandas as pd
# Import the TRAC runtime library
import tracdap.rt.api as trac


class TracSchemaUtils:

    @staticmethod
    def get_trac_fields_by_type(schema: trac.SchemaDefinition, basic_type: trac.BasicType) -> tp.List[str]:
        """
        A function that takes a TRAC schema and returns a list of the variables of a given basic type in it.
        :param schema: The TRAC schema to filter.
        :param basic_type: The TRAC basic type to look for.
        :return: A list of variables of the corresponding type.
        """

        filtered_fields = filter(lambda field: field.fieldType == basic_type, schema.table.fields)

        return list(map(lambda field: field.fieldName, filtered_fields))

    @staticmethod
    def get_trac_schema_type_breakdown(schema: trac.SchemaDefinition) -> tp.Dict[trac.BasicType, tp.List[str]]:
        """
        A function that takes a TRAC schema and returns a dictionary of lists, where the key is a
        TRAC type and the list is the corresponding variables in the schema.
        :return: A dictionary keyed by the TRAC type with lists of variable names as the properties.
        """

        type_breakdown = dict()
        for basic_type in trac.BasicType:
            print(basic_type)
            type_breakdown[basic_type] = TracSchemaUtils.get_trac_fields_by_type(schema, basic_type)

        return type_breakdown

    @staticmethod
    def convert_enum_basic_type_to_string(basic_type: trac.BasicType) -> str:
        """
        A function that converts a TRAC basic type (which is an integer enum) into
        a humanreadable string.
        :param basic_type: The basic type to convert.
        :return: The equivalent string
        """
        if basic_type == trac.BasicType.FLOAT:
            return "FLOAT"
        elif basic_type == trac.BasicType.INTEGER:
            return "INTEGER"
        elif basic_type == trac.BasicType.DECIMAL:
            return "DECIMAL"
        elif basic_type == trac.BasicType.DATE:
            return "DATE"
        elif basic_type == trac.BasicType.DATETIME:
            return "DATETIME"
        elif basic_type == trac.BasicType.STRING:
            return "STRING"
        elif basic_type == trac.BasicType.BOOLEAN:
            return "BOOLEAN"
        elif basic_type == trac.BasicType.MAP:
            return "MAP"
        elif basic_type == trac.BasicType.ARRAY:
            return "ARRAY"
        else:
            raise Exception("The supplied basic type is not valid")

    @staticmethod
    def convert_schema_into_dataframe(schema: trac.SchemaDefinition) -> pd.DataFrame:
        """
        A function that takes a TRAC schema and converts to a dataFrame with columns for each property.
        :param schema: The schema to convert.
        :return: The schema as a dataFrame.
        """
        # We need to take the schema fields and remap them into lists that can be used as the
        # columns in a dataFrame

        # The keys if the dictionary relate to the columns in the dataFrame
        dictionary_keys = tp.Literal["field_name", "field_type", "field_order", "label", "categorical"]

        # The blank structure that we will add to
        schema_dictionary: tp.Dict[dictionary_keys, tp.List[str | int]] = {
            "field_name": [],
            "field_type": [],
            "field_order": [],
            "label": [],
            "categorical": []
        }

        for field in schema.table.fields:

            schema_dictionary["field_name"].append(field.fieldName)
            schema_dictionary["field_order"].append(field.fieldOrder)

            # Default to the field name as the label if label is not set
            label = field.label
            if label is None:
                label = field.fieldName
            schema_dictionary["label"].append(label)

            # Default categorical to false if not set
            categorical = field.categorical
            if categorical is None:
                categorical = field.categorical
            schema_dictionary["categorical"].append(categorical)

            schema_dictionary["field_type"].append(TracSchemaUtils.convert_enum_basic_type_to_string(field.fieldType))

        # Put the lists in the dictionary into a dataFrame
        schema_as_df = pd.DataFrame({
            'field_name': schema_dictionary["field_name"],
            'label': schema_dictionary["label"],
            'field_type': schema_dictionary["field_type"],
            'field_order': schema_dictionary["field_order"],
            'categorical': schema_dictionary["categorical"]
        })

        schema_as_df['field_order'] = schema_as_df['field_order'].astype(int)

        # Add the field name as the index
        return schema_as_df.set_index(["field_name"], drop=False)