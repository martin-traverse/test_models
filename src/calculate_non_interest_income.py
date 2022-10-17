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
# import pandas as pd
import tracdap.rt.api as trac
import src.schemas as schemas


class NonInterestIncomeDataModel(trac.TracModel):

    def define_parameters(self) -> tp.Dict[str, trac.ModelParameter]:
        return trac.define_parameters(
            # to do
            trac.P("expected_base_rate", trac.FLOAT,
                   label="expected base rate"),

            trac.P("expected_employee_cost_change", trac.FLOAT,
                   label="expected employee cost change")
        )

    def define_inputs(self) -> tp.Dict[str, trac.ModelInputSchema]:
        investment_income = trac.load_schema(schemas, "investment_income.csv")
        fees_and_commissions_income = trac.load_schema(schemas, "fees_and_commissions_income.csv")
        return {"investment_income": trac.ModelInputSchema(investment_income),
                "fees_and_commissions_income": trac.ModelInputSchema(fees_and_commissions_income)}

    def define_outputs(self) -> tp.Dict[str, trac.ModelOutputSchema]:
        non_interest_income = trac.load_schema(schemas, "non_interest_income.csv")
        return {"non_interest_income": trac.ModelOutputSchema(non_interest_income)}

    def run_model(self, ctx: trac.TracContext):
        ctx.log().info("Net interest margin model is running...")

        # expected_base_rate = ctx.get_parameter("expected_base_rate")
        # expected_employee_cost_change = ctx.get_parameter("expected_employee_cost_change")

        # commissions_fees Net fee and commissions income

        # investment_income = ctx.get_pandas_table("investment_income")
        fees_and_commissions_income = ctx.get_pandas_table("fees_and_commissions_income")
        non_interest_income = fees_and_commissions_income.rename(
            columns={"commissions_fees": "net_fee_commissions_income"})
        ctx.put_pandas_table("non_interest_income", non_interest_income)


if __name__ == "__main__":
    import tracdap.rt.launch as launch

    launch.launch_model(NonInterestIncomeDataModel, "config/non_interest_income.yaml", "config/sys_config.yaml")
