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


def calculate_total_income(non_interest_income, net_interest_income):
    total_income = net_interest_income.copy()
    total_income["net_fee_commissions_income"] = non_interest_income["net_fee_commissions_income"]
    total_income["total_operating_income"] = total_income["net_interest_income"] + total_income[
        "net_fee_commissions_income"]
    return total_income


def calculate_operating_profit(non_interest_income, operating_costs, net_interest_income):
    operating_profit = net_interest_income.copy()
    operating_profit["net_fee_commissions_income"] = non_interest_income["net_fee_commissions_income"]
    operating_profit["staff_costs"] = operating_costs["staff_costs"]
    operating_profit["other_expenses"] = operating_costs["other_expenses"]
    operating_profit["total_operating_income"] = operating_profit["net_interest_income"] + operating_profit[
        "net_fee_commissions_income"]
    operating_profit["total_operating_expenses"] = operating_profit["staff_costs"] + operating_profit["staff_costs"]
    operating_profit["profit_before_loan_losses"] = operating_profit["total_operating_income"] + operating_profit[
        "total_operating_expenses"]
    return operating_profit


class OperatingCostsProfitModel(trac.TracModel):

    def define_parameters(self) -> tp.Dict[str, trac.ModelParameter]:
        return trac.define_parameters(

            trac.P("expected_base_rate", trac.FLOAT,
                   label="Expected base rate",
                   default_value=1.0),

            trac.P("expected_employee_cost_change", trac.FLOAT,
                   label="Expected employee cost change",
                   default_value=0.0)
        )

    def define_inputs(self) -> tp.Dict[str, trac.ModelInputSchema]:
        non_interest_income = trac.load_schema(schemas, "non_interest_income.csv")
        operating_costs = trac.load_schema(schemas, "operating_costs.csv")
        net_interest_income = trac.load_schema(schemas, "net_interest_income.csv")

        return {"non_interest_income": trac.ModelInputSchema(non_interest_income),
                "operating_costs": trac.ModelInputSchema(operating_costs),
                "net_interest_income": trac.ModelInputSchema(net_interest_income)}

    def define_outputs(self) -> tp.Dict[str, trac.ModelOutputSchema]:
        operating_profit = trac.load_schema(schemas, "operating_profit.csv")
        total_income = trac.load_schema(schemas, "total_income.csv")

        return {"operating_profit": trac.ModelOutputSchema(operating_profit),
                "total_income": trac.ModelOutputSchema(total_income)}

    def run_model(self, ctx: trac.TracContext):
        ctx.log().info("Operating costs profit model is running...")

        # expected_base_rate = ctx.get_parameter("expected_base_rate")
        # expected_employee_cost_change = ctx.get_parameter("expected_employee_cost_change")

        non_interest_income = ctx.get_pandas_table("non_interest_income")
        operating_costs = ctx.get_pandas_table("operating_costs")
        net_interest_income = ctx.get_pandas_table("net_interest_income")

        operating_profit = calculate_operating_profit(non_interest_income, operating_costs, net_interest_income)
        total_income = calculate_total_income(non_interest_income, net_interest_income)

        ctx.log().info(f"{total_income.head(1).T}")

        ctx.put_pandas_table("operating_profit", operating_profit)
        ctx.put_pandas_table("total_income", total_income)


if __name__ == "__main__":
    import tracdap.rt.launch as launch

    launch.launch_model(OperatingCostsProfitModel, "config/calculate_operating_profit.yaml", "config/sys_config.yaml")
