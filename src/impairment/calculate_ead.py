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

import calendar
import tracdap.rt.api as trac
import typing as tp
import impairment.schemas as schemas
import datetime
import pandas as pd


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
        mortgage_book_t0_schema = trac.load_schema(schemas, "mortgage_book_t0.csv")

        return {"ead_model_parameters": trac.ModelInputSchema(ead_model_parameters_schema),
                "balance_forecast": trac.ModelInputSchema(balance_forecast_schema),
                "mortgage_book_t0": trac.ModelInputSchema(mortgage_book_t0_schema)}

    def define_outputs(self) -> tp.Dict[str, trac.ModelOutputSchema]:
        ead_forecast_schema = trac.load_schema(schemas, "ead_forecast.csv")

        return {"ead_forecast": trac.ModelOutputSchema(ead_forecast_schema)}

    def run_model(self, ctx: trac.TracContext):
        first_forecast_month = ctx.get_parameter("first_forecast_month")
        last_forecast_month = ctx.get_parameter("last_forecast_month")

        last_day = calendar.monthrange(first_forecast_month.year, first_forecast_month.month)[1]
        first_forecast_month = first_forecast_month.replace(day=last_day)

        last_day = calendar.monthrange(last_forecast_month.year, last_forecast_month.month)[1]
        last_forecast_month = last_forecast_month.replace(day=last_day)

        date_list = pd.date_range(first_forecast_month, last_forecast_month, freq="M", inclusive="both", name="date")
        dates = date_list.to_series("date").to_frame("date").reset_index(drop=True)

        # ead_model_parameters = ctx.get_pandas_table("ead_model_parameters")
        balance_forecast = ctx.get_pandas_table("balance_forecast")
        mortgage_book_t0 = ctx.get_pandas_table("mortgage_book_t0")

        ead_forecast = mortgage_book_t0.drop("date", axis=1).join(dates, how="cross")
        print(ead_forecast)
        ead_forecast["time_to_default"] = 3 - ead_forecast["months_in_arrears"]
        ead_forecast["ead"] = ead_forecast["balance"] + 400 + ead_forecast["balance"] * (
                    pow(1 + (0.05 / 12), ead_forecast["time_to_default"]) - 1)

        print(ead_forecast)

        ead_forecast.drop(["pd_12m", "in_default"], axis=1, inplace=True)
        # Output the dataset
        ctx.put_pandas_table("ead_forecast", ead_forecast)


if __name__ == "__main__":
    import tracdap.rt.launch as launch

    launch.launch_model(CalculateEad, "config/calculate_ead.yaml", "config/sys_config.yaml")
