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


class NetInterestMarginDataModel(trac.TracModel):

    def define_parameters(self) -> tp.Dict[str, trac.ModelParameter]:
        return trac.define_parameters(
            # to do
            trac.P("expected_base_rate", trac.FLOAT,
                   label="expected base rate"),

            trac.P("expected_employee_cost_change", trac.FLOAT,
                   label="expected employee cost change")
        )

    def define_inputs(self) -> tp.Dict[str, trac.ModelInputSchema]:
        interest_paid_assets = trac.load_schema(schemas, "average_interest_paid_on_assets.csv")
        interest_earned_assets = trac.load_schema(schemas, "average_interest_earned_on_assets.csv")
        return {"interest_paid_assets": trac.ModelInputSchema(interest_paid_assets),
                "interest_earned_assets": trac.ModelInputSchema(interest_earned_assets)}

    def define_outputs(self) -> tp.Dict[str, trac.ModelOutputSchema]:
        net_interest_margin = trac.load_schema(schemas, "net_interest_margin.csv")
        return {"net_interest_margin": trac.ModelOutputSchema(net_interest_margin)}

    def run_model(self, ctx: trac.TracContext):
        ctx.log().info("Net interest margin model is running...")

        # expected_base_rate = ctx.get_parameter("expected_base_rate")
        # expected_employee_cost_change = ctx.get_parameter("expected_employee_cost_change")

        interest_paid_assets = ctx.get_pandas_table("interest_paid_assets")
        # interest_earned_assets = ctx.get_pandas_table("interest_earned_assets")

        # dummy computations
        net_interest_margin = interest_paid_assets.rename(
            columns={"average_funding_interest_rate": "average_net_interest_margin"})
        ctx.put_pandas_table("net_interest_margin", net_interest_margin)


if __name__ == "__main__":
    import tracdap.rt.launch as launch

    launch.launch_model(NetInterestMarginDataModel, "config/net_interest_margin.yaml", "config/sys_config.yaml")
