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


class NewOriginationsModel(trac.TracModel):

    def define_parameters(self) -> tp.Dict[str, trac.ModelParameter]:
        return trac.define_parameters(

            trac.P("first_forecast_month", trac.BasicType.STRING, label="First month of forecast2",
                   default_value="YO"),

            trac.P("last_forecast_month", trac.BasicType.DATE, label="Last month of forecast",
                   default_value=datetime.datetime(2025, 12, 1).date()),

            trac.P("sek_to_eur_exchange_rate", trac.BasicType.FLOAT, label="SEK to EUR exchange rate",
                   default_value=0.09)
        )

    def define_inputs(self) -> tp.Dict[str, trac.ModelInputSchema]:
        market_scenario_schema = trac.load_schema(schemas, "new_originations_schema.csv")
        return {
            "market_scenario": trac.ModelInputSchema(market_scenario_schema)
        }

    def define_outputs(self) -> tp.Dict[str, trac.ModelOutputSchema]:
        new_originations_schema = trac.load_schema(schemas, "market_scenario_schema.csv")
        return {"new_originations": trac.ModelOutputSchema(new_originations_schema)}

    def run_model(self, ctx: trac.TracContext):
        ctx.log().info("Model is running...")

        first_forecast_month = ctx.get_parameter("first_forecast_month")
        last_forecast_month = ctx.get_parameter("last_forecast_month")
        sek_to_eur_exchange_rate = ctx.get_parameter("sek_to_eur_exchange_rate")

        first_forecast_month = first_forecast_month.replace(day=1)

        last_day = calendar.monthrange(last_forecast_month.year, last_forecast_month.month)[1]
        last_forecast_month = last_forecast_month.replace(day=last_day)

        new_originations = ctx.get_pandas_table("market_scenario")

        new_originations['new_lending'] = new_originations["mortgage_lending_to_households"] * new_originations["share_of_market"]

        new_originations["business_line"] = "Retail"
        new_originations['new_fixed_rate_interest_only_balance'] = new_originations["new_lending"] * new_originations["new_business_fixed_rate"] * new_originations["new_business_interest_only"] * sek_to_eur_exchange_rate
        new_originations['new_floating_rate_interest_only_balance'] = new_originations["new_lending"] * new_originations["new_business_floating_rate"] * new_originations["new_business_interest_only"] * sek_to_eur_exchange_rate
        new_originations['new_capped_rate_interest_only_balance'] = new_originations["new_lending"] * new_originations["new_business_capped_rate"] * new_originations["new_business_interest_only"] * sek_to_eur_exchange_rate
        new_originations['new_fixed_rate_amortising_balance'] = new_originations["new_lending"] * new_originations["new_business_fixed_rate"] * new_originations["new_business_amortising"] * sek_to_eur_exchange_rate
        new_originations['new_floating_rate_amortising_balance'] = new_originations["new_lending"] * new_originations["new_business_floating_rate"] * new_originations["new_business_amortising"] * sek_to_eur_exchange_rate
        new_originations['new_capped_rate_amortising_balance'] = new_originations["new_lending"] * new_originations["new_business_capped_rate"] * new_originations["new_business_amortising"] * sek_to_eur_exchange_rate

        new_originations = new_originations.loc[(new_originations['observation_date'].dt.date >= first_forecast_month) & (new_originations['observation_date'].dt.date <= last_forecast_month)]

        new_originations.rename(columns={"observation_date": "date"}, inplace=True)

        ctx.put_pandas_table("new_originations", new_originations[
            ['business_line', 'date', 'new_fixed_rate_interest_only_balance', 'new_floating_rate_interest_only_balance',
             'new_capped_rate_interest_only_balance', 'new_fixed_rate_amortising_balance',
             'new_floating_rate_amortising_balance', 'new_capped_rate_amortising_balance']])


if __name__ == "__main__":
    import tracdap.rt.launch as launch

    launch.launch_model(NewOriginationsModel, "config/calculate_new_originations.yaml", "config/sys_config.yaml")
