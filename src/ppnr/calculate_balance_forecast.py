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


class BalanceForecastModel(trac.TracModel):

    def define_parameters(self) -> tp.Dict[str, trac.ModelParameter]:
        return trac.define_parameters()

    def define_inputs(self) -> tp.Dict[str, trac.ModelInputSchema]:
        mortgage_book_t0_schema = trac.load_schema(schemas, "mortgage_book_t0_schema.csv")
        portfolio_runoff_schema = trac.load_schema(schemas, "portfolio_runoff_schema.csv")
        new_originations_schema = trac.load_schema(schemas, "new_originations_schema.csv")
        return {
            "mortgage_book_t0": trac.ModelInputSchema(mortgage_book_t0_schema),
            "portfolio_runoff": trac.ModelInputSchema(portfolio_runoff_schema),
            "new_originations": trac.ModelInputSchema(new_originations_schema)
        }

    def define_outputs(self) -> tp.Dict[str, trac.ModelOutputSchema]:
        balance_forecast_schema = trac.load_schema(schemas, "balance_forecast_schema.csv")
        return {
            "balance_forecast": trac.ModelOutputSchema(balance_forecast_schema),
            "financed_emissions": trac.ModelOutputSchema(balance_forecast_schema)
        }

    def run_model(self, ctx: trac.TracContext):
        ctx.log().info("Model is running...")

        mortgage_book_t0 = ctx.get_pandas_table("mortgage_book_t0")
        portfolio_runoff = ctx.get_pandas_table("portfolio_runoff")
        new_originations = ctx.get_pandas_table("new_originations")

        mortgage_book_t0 = mortgage_book_t0[['business_line', "mortgage_type", "date", "balance"]]
        mortgage_book_t0["date"] = mortgage_book_t0["date"].apply(lambda x: x.replace(day=1))
        portfolio_runoff["date"] = portfolio_runoff["date"].apply(lambda x: x.replace(day=1))
        new_originations["date"] = new_originations["date"].apply(lambda x: x.replace(day=1))

        balance_flows = portfolio_runoff.merge(new_originations, on=["date", 'business_line'], how="inner")
        balance_flows.loc[balance_flows['mortgage_type'] != "fixed rate", 'new_fixed_rate_interest_only_balance'] = 0
        balance_flows.loc[balance_flows['mortgage_type'] != "fixed rate", 'new_fixed_rate_amortising_balance'] = 0

        balance_flows.loc[balance_flows['mortgage_type'] != "capped rate", 'new_capped_rate_interest_only_balance'] = 0
        balance_flows.loc[balance_flows['mortgage_type'] != "capped rate", 'new_capped_rate_amortising_balance'] = 0

        balance_flows.loc[
            balance_flows['mortgage_type'] != "floating rate", 'new_floating_rate_interest_only_balance'] = 0
        balance_flows.loc[balance_flows['mortgage_type'] != "floating rate", 'new_floating_rate_amortising_balance'] = 0

        balance_flows["net_balance_flow"] = balance_flows["new_fixed_rate_interest_only_balance"] + balance_flows[
            "new_floating_rate_interest_only_balance"] + balance_flows["new_capped_rate_interest_only_balance"] + \
                                            balance_flows["new_fixed_rate_amortising_balance"] + balance_flows[
                                                "new_floating_rate_amortising_balance"] + balance_flows[
                                                "new_capped_rate_amortising_balance"] - (
                                                    balance_flows["repayment_balance"] / 1000000) - (
                                                    balance_flows["prepayment_balance"] / 1000000)

        balance_flows = balance_flows.sort_values(by=["mortgage_type", "date"], ascending=True)

        balance_flows["cumulative_net_balance_flow"] = balance_flows.groupby(["mortgage_type"])[
            "net_balance_flow"].cumsum()

        group_by_list = ["mortgage_type"]

        mortgage_book_t0 = (mortgage_book_t0.groupby(group_by_list, as_index=False).agg({'balance': 'sum'}).rename(
            columns={'balance': 'balance_at_start'}))

        balance_forecast = mortgage_book_t0.merge(balance_flows, on=['mortgage_type'], how="inner")

        balance_forecast["balance"] = balance_forecast["balance_at_start"] / 1000000 + balance_forecast[
            "cumulative_net_balance_flow"]

        balance_forecast = balance_forecast[
            ["date", "business_line", "mortgage_type", "net_balance_flow", "cumulative_net_balance_flow", "balance"]]

        ctx.put_pandas_table("balance_forecast", balance_forecast)
        ctx.put_pandas_table("financed_emissions", balance_forecast)


if __name__ == "__main__":
    import tracdap.rt.launch as launch

    launch.launch_model(BalanceForecastModel, "config/calculate_balance_forecast.yaml", "config/sys_config.yaml")
