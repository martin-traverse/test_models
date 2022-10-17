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


class NetInterestIncomeDataModel(trac.TracModel):

    def define_parameters(self) -> tp.Dict[str, trac.ModelParameter]:

        return trac.define_parameters(

            trac.P("expected_base_rate", trac.FLOAT,
                   label="expected base rate"),

            trac.P("expected_employee_cost_change", trac.FLOAT,
                   label="expected employee cost change")
                   )

    def define_inputs(self) -> tp.Dict[str, trac.ModelInputSchema]:
        earning_assets = trac.load_schema(schemas, "average_interest_earning_assets.csv")
        net_interest_margin = trac.load_schema(schemas, "net_interest_margin.csv")
        return {"average_interest_earning_assets": trac.ModelInputSchema(earning_assets),
                "net_interest_margin": trac.ModelInputSchema(net_interest_margin)}

    def define_outputs(self) -> tp.Dict[str, trac.ModelOutputSchema]:
        net_interest_income = trac.load_schema(schemas, "net_interest_income.csv")
        return {"net_interest_income": trac.ModelOutputSchema(net_interest_income)}

    def run_model(self, ctx: trac.TracContext):
        ctx.log().info("Net Interest Income model is running...")

        # expected_base_rate = ctx.get_parameter("expected_base_rate")
        # expected_employee_cost_change = ctx.get_parameter("expected_employee_cost_change")

        # net_interest_margin = ctx.get_pandas_table("net_interest_margin")
        earning_assets = ctx.get_pandas_table("average_interest_earning_assets")
        
        # dummy computations
        net_interest_income = earning_assets.rename(columns={"average_balance":"net_interest_income"})
        ctx.put_pandas_table("net_interest_income", net_interest_income)


if __name__ == "__main__":
    import tracdap.rt.launch as launch
    launch.launch_model(NetInterestIncomeDataModel, "config/net_interest_income.yaml", "config/sys_config.yaml")
