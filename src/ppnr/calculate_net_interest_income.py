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


def calculate_net_interest_margin(interest_paid_assets, interest_earned_assets):
    average_funding_interest_rate = interest_paid_assets["average_funding_interest_rate"]
    average_earner_interest_rate = interest_earned_assets["average_earner_interest_rate"]
    net_interest_margin = interest_earned_assets.copy()
    net_interest_margin[
        "average_net_interest_margin"] = average_funding_interest_rate - average_earner_interest_rate + 0.25
    net_interest_margin = net_interest_margin.drop("average_earner_interest_rate", axis=1)
    return net_interest_margin


def calculate_net_interest_income(net_interest_margin, earning_assets):
    average_balance = earning_assets["balance"]
    average_net_interest_margin = net_interest_margin["average_net_interest_margin"]
    net_interest_income = net_interest_margin.copy()
    net_interest_income = net_interest_income.drop("average_net_interest_margin", axis=1)
    net_interest_income["net_interest_income"] = average_balance * average_net_interest_margin
    return net_interest_income


class NetInterestMarginDataModel(trac.TracModel):

    def define_parameters(self) -> tp.Dict[str, trac.ModelParameter]:
        return trac.define_parameters()

    def define_inputs(self) -> tp.Dict[str, trac.ModelInputSchema]:
        cost_of_funding_schema = trac.load_schema(schemas, "cost_of_funding_schema.csv")
        customer_rates_schema = trac.load_schema(schemas, "customer_rates_schema.csv")
        economic_scenario_schema = trac.load_schema(schemas, "economic_scenario_schema.csv")
        balance_forecast_schema = trac.load_schema(schemas, "balance_forecast_schema.csv")

        return {
            "cost_of_funding": trac.ModelInputSchema(cost_of_funding_schema),
            "customer_rates": trac.ModelInputSchema(customer_rates_schema),
            "economic_scenario": trac.ModelInputSchema(economic_scenario_schema),
            "balance_forecast": trac.ModelInputSchema(balance_forecast_schema),
        }

    def define_outputs(self) -> tp.Dict[str, trac.ModelOutputSchema]:
        net_interest_income_schema = trac.load_schema(schemas, "net_interest_income_schema.csv")
        return {"net_interest_income": trac.ModelOutputSchema(net_interest_income_schema)}

    def run_model(self, ctx: trac.TracContext):
        ctx.log().info("Net interest interest model is running...")

        cost_of_funding = ctx.get_pandas_table("cost_of_funding")
        customer_rates = ctx.get_pandas_table("customer_rates")
        balance_forecast = ctx.get_pandas_table("balance_forecast")

        # dummy computations
        net_interest_income = calculate_net_interest_margin(cost_of_funding, customer_rates)

        net_interest_income = net_interest_income[(net_interest_income['date'] == 2021) & (net_interest_income['region'].str.upper() == "SWEDEN")]

        net_interest_income = balance_forecast.merge(net_interest_income[["average_net_interest_margin"]], how="cross")

        net_interest_income["net_interest_income"] = 10000 * net_interest_income["balance"] * net_interest_income["average_net_interest_margin"]

        net_interest_income.drop(["net_balance_flow", "cumulative_net_balance_flow", "balance"], axis=1, inplace=True)

        ctx.put_pandas_table("net_interest_income", net_interest_income)


if __name__ == "__main__":
    import tracdap.rt.launch as launch

    launch.launch_model(NetInterestMarginDataModel, "config/calculate_net_interest_income.yaml",
                        "config/sys_config.yaml")
