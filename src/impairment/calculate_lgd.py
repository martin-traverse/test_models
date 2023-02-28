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
from impairment import schemas as schemas
import datetime


class CalculateLgd(trac.TracModel):

    def define_parameters(self) -> tp.Dict[str, trac.ModelParameter]:
        return trac.declare_parameters(

            trac.P("first_forecast_month", trac.BasicType.DATE, label="First month of forecast",
                   default_value=datetime.datetime(2022, 1, 1).date()),
            trac.P("last_forecast_month", trac.BasicType.DATE, label="Last month of forecast",
                   default_value=datetime.datetime(2025, 12, 1).date())
        )

    def define_inputs(self) -> tp.Dict[str, trac.ModelInputSchema]:
        lgd_model_parameters_schema = trac.load_schema(schemas, "lgd_model_parameters_schema.csv")
        ead_forecast_schema = trac.load_schema(schemas, "ead_forecast_schema.csv")

        return {"lgd_model_parameters": trac.ModelInputSchema(lgd_model_parameters_schema),
                "ead_forecast": trac.ModelInputSchema(ead_forecast_schema)}

    def define_outputs(self) -> tp.Dict[str, trac.ModelOutputSchema]:
        lgd_forecast_schema = trac.load_schema(schemas, "lgd_forecast_schema.csv")

        return {"lgd_forecast": trac.ModelOutputSchema(lgd_forecast_schema)}

    @staticmethod
    def gauss(x):
        return max(0.0, min(0.8, random.gauss(0.2, 0.2)))

    def run_model(self, ctx: trac.TracContext):
        lgd_model_parameters = ctx.get_pandas_table("lgd_model_parameters")
        ead_forecast = ctx.get_pandas_table("ead_forecast")

        lgd_forecast = ead_forecast.copy()

        forced_sale_discount_list = [0.1, 0.2, 0.25, 0.3, 0.5]
        lgd_forecast["forced_sale_discount"] = random.choices(forced_sale_discount_list, weights=(50, 40, 20, 10, 5),
                                                              k=len(lgd_forecast))

        lgd_forecast["time_to_sale"] = lgd_forecast["time_to_default"] + 9

        lgd_forecast["lgd_12m"] = lgd_forecast.apply(CalculateLgd.gauss, axis=1)

        lgd_forecast["lgd_lifetime"] = lgd_forecast["lgd_12m"] * min(1.1, max(1.0, random.gauss(1, 1)))

        # Output the dataset
        ctx.put_pandas_table("lgd_forecast", lgd_forecast)


if __name__ == "__main__":
    import tracdap.rt.launch as launch

    launch.launch_model(CalculateLgd, "config/impairment/calculate_lgd.yaml", "config/sys_config.yaml")
