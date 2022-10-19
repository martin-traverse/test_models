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
import datetime


class CalculateEad(trac.TracModel):

    def define_parameters(self) -> tp.Dict[str, trac.ModelParameter]:
        return trac.declare_parameters(

            trac.P("first_forecast_month", trac.BasicType.DATE, label="First month of forecast",
                   default_value=datetime.datetime(2022, 1, 1).date()),
            trac.P("last_forecast_month", trac.BasicType.DATE, label="Last month of forecast",
                   default_value=datetime.datetime(2025, 12, 1).date())
        )

    def define_inputs(self) -> tp.Dict[str, trac.ModelInputSchema]:
        ead_model_parameters_schema = trac.load_schema(schemas, "ead_model_parameters.csv")
        balance_forecast_schema = trac.load_schema(schemas, "balance_forecast.csv")

        return {"ead_model_parameters": trac.ModelInputSchema(ead_model_parameters_schema),
                "balance_forecast": trac.ModelInputSchema(balance_forecast_schema)}

    def define_outputs(self) -> tp.Dict[str, trac.ModelOutputSchema]:
        ead_forecast_schema = trac.load_schema(schemas, "ead_forecast.csv")

        return {"ead_forecast": trac.ModelOutputSchema(ead_forecast_schema)}

    def run_model(self, ctx: trac.TracContext):
        # ead_model_parameters = ctx.get_pandas_table("ead_model_parameters")
        balance_forecast = ctx.get_pandas_table("balance_forecast")

        ead_forecast = balance_forecast.copy()

        time_to_default_list = [0, 1, 2, 3]

        ead_forecast["time_to_default"] = random.choices(time_to_default_list, weights=(5, 10, 20, 50), k=len(ead_forecast))
        ead_forecast["ead"] = ead_forecast["balance"] + 400 + ead_forecast["balance"] * (
                    pow(1 + (0.05 / 12), ead_forecast["time_to_default"]) - 1)

        # Output the dataset
        ctx.put_pandas_table("ead_forecast", ead_forecast)


if __name__ == "__main__":
    import tracdap.rt.launch as launch

    launch.launch_model(CalculateEad, "config/calculate_ead.yaml", "config/sys_config.yaml")
