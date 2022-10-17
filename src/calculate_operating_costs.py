#  Copyright 2022 Accenture Global Solutions Limited
#
#  Licensed under the Apache License, Version 2.0 (the "License");
#  you may not use this file except in compliance with the License.
#  You may obtain a copy of the License at
#
#      http://www.apache.org/licenses/LICENSE-2.0
#
#  Unless required by applicable law or agreed to in writing, software
#  distributed under the License is distributed on an "AS IS" BASIS,
#  WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
#  See the License for the specific language governing permissions and
#  limitations under the License.

import typing as tp
# import pandas as pd
import tracdap.rt.api as trac
import schemas as schemas


class OperatingCostsDataModel(trac.TracModel):

    def define_parameters(self) -> tp.Dict[str, trac.ModelParameter]:
        return trac.define_parameters(
            # to do
            trac.P("expected_base_rate", trac.FLOAT,
                   label="expected base rate"),

            trac.P("expected_employee_cost_change", trac.FLOAT,
                   label="expected employee cost change")
        )

    def define_inputs(self) -> tp.Dict[str, trac.ModelInputSchema]:
        business_support_costs = trac.load_schema(schemas, "business_support_costs.csv")
        processing_costs = trac.load_schema(schemas, "processing_costs.csv")
        sales_and_marketing_costs = trac.load_schema(schemas, "sales_and_marketing_costs.csv")
        corporate_centre_costs = trac.load_schema(schemas, "corporate_centre_costs.csv")

        return {"business_support_costs": trac.ModelInputSchema(business_support_costs),
                "processing_costs": trac.ModelInputSchema(processing_costs),
                "sales_and_marketing_costs": trac.ModelInputSchema(sales_and_marketing_costs),
                "corporate_centre_costs": trac.ModelInputSchema(corporate_centre_costs)}

    def define_outputs(self) -> tp.Dict[str, trac.ModelOutputSchema]:
        operating_costs = trac.load_schema(schemas, "operating_costs.csv")
        return {"operating_costs": trac.ModelOutputSchema(operating_costs)}

    def run_model(self, ctx: trac.TracContext):
        ctx.log().info("Net interest margin model is running...")

        # expected_base_rate = ctx.get_parameter("expected_base_rate")
        # expected_employee_cost_change = ctx.get_parameter("expected_employee_cost_change")

        # business_support_costs = ctx.get_pandas_table("business_support_costs")
        processing_costs = ctx.get_pandas_table("processing_costs")
        # sales_and_marketing_costs = ctx.get_pandas_table("sales_and_marketing_costs")
        # corporate_centre_costs = ctx.get_pandas_table("corporate_centre_costs")

        operating_costs = processing_costs.rename(columns={"it_delivery_costs": "staff_costs",
                                                           "employee_costs": "other_expenses"})
        ctx.put_pandas_table("operating_costs", operating_costs)


if __name__ == "__main__":
    import tracdap.rt.launch as launch

    launch.launch_model(OperatingCostsDataModel, "config/operating_costs.yaml", "config/sys_config.yaml")
