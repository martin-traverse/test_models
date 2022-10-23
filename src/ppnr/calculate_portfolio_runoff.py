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
import tracdap.rt.api as trac
import ppnr.schemas as schemas
import datetime
import calendar
import pandas as pd


class PortfolioRunoffModel(trac.TracModel):

    def define_parameters(self) -> tp.Dict[str, trac.ModelParameter]:
        return trac.define_parameters(

            trac.P("base_rate_sensitivity_uplift", trac.FLOAT,
                   label="Base rate sensitivity uplift",
                   default_value=1.0),

            trac.P("first_forecast_month", trac.BasicType.DATE, label="First month of forecast",
                   default_value=datetime.datetime(2022, 1, 1).date()),

            trac.P("last_forecast_month", trac.BasicType.DATE, label="Last month of forecast",
                   default_value=datetime.datetime(2025, 12, 1).date())
        )

    def define_inputs(self) -> tp.Dict[str, trac.ModelInputSchema]:
        mortgage_book_t0_schema = trac.load_schema(schemas, "mortgage_book_t0_schema.csv")
        economic_scenario_schema = trac.load_schema(schemas, "economic_scenario_schema.csv")
        return {
            "mortgage_book_t0": trac.ModelInputSchema(mortgage_book_t0_schema),
            "economic_scenario": trac.ModelInputSchema(economic_scenario_schema)
        }

    def define_outputs(self) -> tp.Dict[str, trac.ModelOutputSchema]:
        portfolio_runoff_schema = trac.load_schema(schemas, "portfolio_runoff_schema.csv")
        return {"portfolio_runoff": trac.ModelOutputSchema(portfolio_runoff_schema)}

    def run_model(self, ctx: trac.TracContext):
        ctx.log().info("Model is running...")

        first_forecast_month = ctx.get_parameter("first_forecast_month")
        last_forecast_month = ctx.get_parameter("last_forecast_month")

        last_day = calendar.monthrange(first_forecast_month.year, first_forecast_month.month)[1]
        first_forecast_month = first_forecast_month.replace(day=last_day)

        last_day = calendar.monthrange(last_forecast_month.year, last_forecast_month.month)[1]
        last_forecast_month = last_forecast_month.replace(day=last_day)

        date_list = pd.date_range(first_forecast_month, last_forecast_month, freq="M", inclusive="both", name="date")
        dates = date_list.to_series("date").to_frame("date").reset_index(drop=True)

        mortgage_book_t0 = ctx.get_pandas_table("mortgage_book_t0")

        portfolio_runoff = mortgage_book_t0.drop("date", axis=1).join(dates, how="cross")

        group_by_list = ['business_line', "mortgage_type", "date"]

        portfolio_runoff = (portfolio_runoff.groupby(group_by_list, as_index=False)
                            .agg({'balance': 'sum', "monthly_repayment": "sum"})
                            .rename(
            columns={'balance': 'prepayment_balance', 'monthly_repayment': 'repayment_balance'}))

        portfolio_runoff["month_index"] = 1 + (portfolio_runoff["date"].dt.year - first_forecast_month.year) * 12 + \
                                          portfolio_runoff["date"].dt.month - first_forecast_month.month

        portfolio_runoff["prepayment_balance"] = portfolio_runoff["prepayment_balance"] - portfolio_runoff[
            "month_index"] * portfolio_runoff[
                                                     "repayment_balance"] * 0.02

        portfolio_runoff["runoff_balance"] = portfolio_runoff["prepayment_balance"] + portfolio_runoff[
            "repayment_balance"]

        ctx.put_pandas_table("portfolio_runoff", portfolio_runoff[
            ['business_line', 'mortgage_type', 'date', 'runoff_balance', 'prepayment_balance', 'repayment_balance']])


if __name__ == "__main__":
    import tracdap.rt.launch as launch

    launch.launch_model(PortfolioRunoffModel, "config/calculate_portfolio_runoff.yaml", "config/sys_config.yaml")
