#  Copyright 2020 Accenture Global Solutions Limited
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

import tracdap.rt.api as trac
import typing as tp
import random
import impairment.schemas as schemas


class CalculatePd(trac.TracModel):

    def define_parameters(self) -> tp.Dict[str, trac.ModelParameter]:
        return trac.declare_parameters(

            trac.P("first_forecast_month", trac.BasicType.DATE, label="First month of forecast",
                   default_value="2021-01-01"),
            trac.P("last_forecast_month", trac.BasicType.DATE, label="Last month of forecast",
                   default_value="2021-12-01"),
        )

    def define_inputs(self) -> tp.Dict[str, trac.ModelInputSchema]:
        economic_scenario_schema = trac.load_schema(schemas, "economic_scenario.csv")
        balance_forecast_schema = trac.load_schema(schemas, "balance_forecast.csv")

        return {"economic_scenario": trac.ModelInputSchema(economic_scenario_schema),
                "balance_forecast": trac.ModelInputSchema(balance_forecast_schema)}

    def define_outputs(self) -> tp.Dict[str, trac.ModelOutputSchema]:
        pd_forecast_schema = trac.load_schema(schemas, "pd_forecast.csv")

        return {"pd_forecast": trac.ModelOutputSchema(pd_forecast_schema)}

    def run_model(self, ctx: trac.TracContext):
        economic_scenario = ctx.get_pandas_table("economic_scenario")
        balance_forecast = ctx.get_pandas_table("balance_forecast")

        pd_forecast = balance_forecast.copy()

        pd_forecast["pd_12m"] = random.randint(2, 50) / 100000
        pd_forecast["pd_lifetime"] = pd_forecast["pd_12m"] * random.randint(10, 15) / 10

        # Output the dataset
        ctx.put_pandas_table("pd_forecast", pd_forecast)


if __name__ == "__main__":
    import tracdap.rt.launch as launch

    launch.launch_model(CalculatePd, "config/calculate_pd.yaml", "config/sys_config.yaml")
