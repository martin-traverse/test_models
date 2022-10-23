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
import random
import impairment.schemas as schemas
import datetime
import pandas as pd


class CalculatePd(trac.TracModel):

    def define_parameters(self) -> tp.Dict[str, trac.ModelParameter]:
        return trac.declare_parameters(

            trac.P("first_forecast_month", trac.BasicType.DATE, label="First month of forecast",
                   default_value=datetime.datetime(2022, 1, 1).date()),
            trac.P("last_forecast_month", trac.BasicType.DATE, label="Last month of forecast",
                   default_value=datetime.datetime(2025, 12, 1).date())
        )

    def define_inputs(self) -> tp.Dict[str, trac.ModelInputSchema]:
        economic_scenario_schema = trac.load_schema(schemas, "economic_scenario.csv")
        mortgage_book_t0_schema = trac.load_schema(schemas, "mortgage_book_t0_schema.csv")

        return {"economic_scenario": trac.ModelInputSchema(economic_scenario_schema),
                "mortgage_book_t0": trac.ModelInputSchema(mortgage_book_t0_schema)}

    def define_outputs(self) -> tp.Dict[str, trac.ModelOutputSchema]:
        pd_forecast_schema = trac.load_schema(schemas, "pd_forecast.csv")

        return {"pd_forecast": trac.ModelOutputSchema(pd_forecast_schema)}

    def run_model(self, ctx: trac.TracContext):
        first_forecast_month = ctx.get_parameter("first_forecast_month")
        last_forecast_month = ctx.get_parameter("last_forecast_month")

        economic_scenario = ctx.get_pandas_table("economic_scenario")
        mortgage_book_t0 = ctx.get_pandas_table("mortgage_book_t0")

        last_day = calendar.monthrange(first_forecast_month.year, first_forecast_month.month)[1]
        first_forecast_month = first_forecast_month.replace(day=last_day)

        last_day = calendar.monthrange(last_forecast_month.year, last_forecast_month.month)[1]
        last_forecast_month = last_forecast_month.replace(day=last_day)

        date_list = pd.date_range(first_forecast_month, last_forecast_month, freq="M", inclusive="both", name="date")
        dates = date_list.to_series("date").to_frame("date").reset_index(drop=True)

        pd_forecast = mortgage_book_t0.drop("date", axis=1).join(dates, how="cross")

        pd_forecast["pd_lifetime"] = pd_forecast["pd_12m"] * random.randint(10, 15) / 10
        pd_forecast["pd_lifetime_max"] = 1
        pd_forecast["pd_lifetime"] = pd_forecast[['pd_lifetime', 'pd_lifetime_max']].min(axis=1)
        pd_forecast.drop(["pd_lifetime_max"], inplace=True, axis=1)
        pd_forecast = pd_forecast.drop(["valuation", "dtv", "balance", "monthly_repayment", "months_in_arrears", "in_default"], axis=1)

        # Output the dataset
        ctx.put_pandas_table("pd_forecast", pd_forecast)


if __name__ == "__main__":
    import tracdap.rt.launch as launch

    launch.launch_model(CalculatePd, "config/calculate_pd.yaml", "config/sys_config.yaml")
