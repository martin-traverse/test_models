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


def calculate_non_interest_income(fees_and_commission_income):
    commissions_fees = fees_and_commission_income["commissions_fees"]
    early_termination_fees = fees_and_commission_income["early_termination_early_payments"]
    delinquency_fees = fees_and_commission_income["delinquency_fees"]

    non_interest_income = fees_and_commission_income.copy()
    non_interest_income["net_fee_commissions_income"] = commissions_fees + early_termination_fees + delinquency_fees

    drop_cols = ["commissions_fees", "early_termination_early_payments", "delinquency_fees"]
    non_interest_income = non_interest_income.drop(drop_cols, axis=1)
    return non_interest_income


class NonInterestIncomeDataModel(trac.TracModel):

    def define_parameters(self) -> tp.Dict[str, trac.ModelParameter]:
        return trac.define_parameters(

            trac.P("expected_base_rate", trac.FLOAT,
                   label="Expected base rate",
                   default_value=1.0),

            trac.P("expected_employee_cost_change", trac.FLOAT,
                   label="Expected employee cost growth",
                   default_value=0.0)
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

        # investment_income = ctx.get_pandas_table("investment_income")
        fees_and_commissions_income = ctx.get_pandas_table("fees_and_commissions_income")

        non_interest_income = calculate_non_interest_income(fees_and_commissions_income)
        ctx.put_pandas_table("non_interest_income", non_interest_income)


if __name__ == "__main__":
    import tracdap.rt.launch as launch

    launch.launch_model(NonInterestIncomeDataModel, "config/calculate_non_interest_income.yaml",
                        "config/sys_config.yaml")
