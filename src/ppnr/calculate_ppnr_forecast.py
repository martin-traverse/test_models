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
import schemas as schemas


def calculate_operating_profit(non_interest_income, operating_costs, net_interest_income):
    operating_profit = net_interest_income.copy()
    operating_profit["non_interest_income"] = non_interest_income["non_interest_income"]
    operating_profit["staff_costs"] = operating_costs["staff_costs"]
    operating_profit["other_expenses"] = operating_costs["other_expenses"]
    operating_profit["total_operating_income"] = operating_profit["net_interest_income"] + operating_profit[
        "non_interest_income"]
    operating_profit["total_operating_expenses"] = operating_profit["staff_costs"] + operating_profit["other_expenses"]
    operating_profit["profit_before_loan_losses"] = operating_profit["total_operating_income"] + operating_profit[
        "total_operating_expenses"]
    return operating_profit


def calculate_operating_costs(corporate_centre_costs, sales_and_marketing_costs,
                              processing_costs, business_support_costs):
    operating_costs = processing_costs.copy()
    operating_costs["other_expenses"] = (corporate_centre_costs["it_compute_cost"] +
                                         corporate_centre_costs["it_infrastructure_cost"] +
                                         corporate_centre_costs["physical_infrastructure_cost"] +
                                         sales_and_marketing_costs["direct_marketing_campaign_costs"] +
                                         sales_and_marketing_costs["indirect_marketing_costs"] +
                                         sales_and_marketing_costs["sales_commissions_costs"] +
                                         processing_costs["it_delivery_costs"] +
                                         business_support_costs["consulting_cost"])

    operating_costs["staff_costs"] = (corporate_centre_costs["employee_costs"] +
                                      sales_and_marketing_costs["employee_costs"] +
                                      processing_costs["employee_costs"])

    drop_cols = ["it_delivery_costs", "employee_costs"]
    operating_costs = operating_costs.drop(drop_cols, axis=1)
    return operating_costs


class PpnrForecastModel(trac.TracModel):

    def define_parameters(self) -> tp.Dict[str, trac.ModelParameter]:
        return trac.define_parameters()

    def define_inputs(self) -> tp.Dict[str, trac.ModelInputSchema]:
        non_interest_income_schema = trac.load_schema(schemas, "non_interest_income_schema.csv")
        net_interest_income_schema = trac.load_schema(schemas, "net_interest_income_schema.csv")
        business_support_costs_schema = trac.load_schema(schemas, "business_support_costs_schema.csv")
        processing_costs_schema = trac.load_schema(schemas, "processing_costs_schema.csv")
        sales_and_marketing_costs_schema = trac.load_schema(schemas, "sales_and_marketing_costs_schema.csv")
        corporate_centre_costs_schema = trac.load_schema(schemas, "corporate_centre_costs_schema.csv")

        return {
            "non_interest_income": trac.ModelInputSchema(non_interest_income_schema),
            "net_interest_income": trac.ModelInputSchema(net_interest_income_schema),
            "business_support_costs": trac.ModelInputSchema(business_support_costs_schema),
            "processing_costs": trac.ModelInputSchema(processing_costs_schema),
            "sales_and_marketing_costs": trac.ModelInputSchema(sales_and_marketing_costs_schema),
            "corporate_centre_costs": trac.ModelInputSchema(corporate_centre_costs_schema)
        }

    def define_outputs(self) -> tp.Dict[str, trac.ModelOutputSchema]:
        ppnr_forecast_schema = trac.load_schema(schemas, "ppnr_forecast_schema.csv")

        return {
            "ppnr_forecast": trac.ModelOutputSchema(ppnr_forecast_schema)
        }

    def run_model(self, ctx: trac.TracContext):
        ctx.log().info("Operating costs profit model is running...")

        business_support_costs = ctx.get_pandas_table("business_support_costs")
        processing_costs = ctx.get_pandas_table("processing_costs")
        sales_and_marketing_costs = ctx.get_pandas_table("sales_and_marketing_costs")
        corporate_centre_costs = ctx.get_pandas_table("corporate_centre_costs")

        non_interest_income = ctx.get_pandas_table("non_interest_income")
        net_interest_income = ctx.get_pandas_table("net_interest_income")

        total_income = non_interest_income.merge(net_interest_income, on=["date", "business_line", "mortgage_type", "region"], how="inner")
        total_income["total_operating_income"] = total_income["net_interest_income"] + total_income["non_interest_income"]

        operating_costs = calculate_operating_costs(corporate_centre_costs, sales_and_marketing_costs,
                                                    processing_costs, business_support_costs)

        operating_costs["total_operating_expenses"] = operating_costs["other_expenses"] + operating_costs["staff_costs"]
        operating_costs = operating_costs[(operating_costs['date'] == 2021) & (operating_costs['region'].str.upper() == "SWEDEN")]

        ppnr_forecast = total_income.merge(operating_costs[["total_operating_expenses"]], how="cross")

        ppnr_forecast["total_operating_expenses"] = ppnr_forecast["total_operating_expenses"] * ppnr_forecast["balance"] / ppnr_forecast[
            "balance_across_segments"]

        ppnr_forecast["pre_provision_net_revenue"] = ppnr_forecast["total_operating_income"] + ppnr_forecast["total_operating_expenses"]

        ppnr_forecast.drop(["average_net_interest_margin", "balance", "balance_across_segments"], axis=1, inplace=True)

        ctx.put_pandas_table("ppnr_forecast", ppnr_forecast)


if __name__ == "__main__":
    import tracdap.rt.launch as launch

    launch.launch_model(PpnrForecastModel, "config/calculate_ppnr_forecast.yaml", "config/sys_config.yaml")
