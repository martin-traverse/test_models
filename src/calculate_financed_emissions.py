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
import src.schemas as schemas


class FinancedEmissionsDataModel(trac.TracModel):

    def define_parameters(self) -> tp.Dict[str, trac.ModelParameter]:
        return trac.define_parameters(

            trac.P("expected_base_rate", trac.FLOAT,
                   label="expected base rate"),

            trac.P("expected_employee_cost_change", trac.FLOAT,
                   label="expected employee cost change")
        )

    def define_inputs(self) -> tp.Dict[str, trac.ModelInputSchema]:
        earning_assets = trac.load_schema(schemas, "average_interest_earning_assets.csv")
        return {"average_interest_earning_assets": trac.ModelInputSchema(earning_assets)}

    def define_outputs(self) -> tp.Dict[str, trac.ModelOutputSchema]:
        emissions = trac.load_schema(schemas, "financed_emissions.csv")
        return {"financed_emissions": trac.ModelOutputSchema(emissions)}

    def run_model(self, ctx: trac.TracContext):
        ctx.log().info("Financed_emissions model is running...")

        # expected_base_rate = ctx.get_parameter("expected_base_rate")
        # expected_employee_cost_change = ctx.get_parameter("expected_employee_cost_change")

        earning_assets = ctx.get_pandas_table("average_interest_earning_assets")
        # dummy computations
        financed_emissions = earning_assets.rename(columns={"average_balance": "financed_emissions"})
        ctx.put_pandas_table("financed_emissions", financed_emissions)


if __name__ == "__main__":
    import tracdap.rt.launch as launch

    launch.launch_model(FinancedEmissionsDataModel, "config/financed_emissions.yaml", "config/sys_config.yaml")
