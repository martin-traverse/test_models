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

import tracdap.rt.api as trac
import typing as tp


class BondPricingModel(trac.TracModel):

    def define_parameters(self) -> tp.Dict[str, trac.ModelParameter]:

        return trac.declare_parameters(

            trac.P("maximum_number_of_months", trac.BasicType.INTEGER, label="Maximum months to maturity"),
            trac.P("include_zero_coupon_bonds", trac.BasicType.BOOLEAN, label="Include zero coupon bonds in valuation")
        )

    def define_inputs(self) -> tp.Dict[str, trac.ModelInputSchema]:

        interest_rate_scenario = trac.declare_input_table(
            trac.F("OBSERVATION_DATE", trac.BasicType.DATE, label="Date", business_key=True, categorical=True, format_code="MONTH"),
            trac.F("INTEREST_RATE", trac.BasicType.FLOAT, label="Annual yield to maturity", format_code=",|.|2||%")
        )
        
        bond_portfolio = trac.declare_input_table(
            trac.F("OBSERVATION_DATE", trac.BasicType.DATE, label="Date", business_key=True, categorical=True, format_code="MONTH"),
            trac.F("COUPON_RATE", trac.BasicType.FLOAT, label="Annual coupon rate", format_code=",|.|2||%"),
            trac.F("COUPON_PAYMENTS_PER_YEAR", trac.BasicType.FLOAT, label="Number of coupon payments per year", format_code="|.|0||"),
            trac.F("MATURITY_DATE", trac.BasicType.DATE, label="Maturity Date", format_code="DAY"),
            trac.F("CURRENT_PRICE", trac.BasicType.FLOAT, label="Current market pricee", format_code=",|.|2|$|"),
            trac.F("FACE_VALUE", trac.BasicType.FLOAT, label="Face value", format_code=",|.|2|$|")
        )

        return {"interest_rate_scenario": interest_rate_scenario, "bond_portfolio": bond_portfolio}

    def define_outputs(self) -> tp.Dict[str, trac.ModelOutputSchema]:

        bond_portfolio_valuation = trac.declare_output_table(
            trac.F("OBSERVATION_DATE", trac.BasicType.DATE, label="Date", business_key=True, categorical=True, format_code="MONTH"),
            trac.F("COUPON_RATE", trac.BasicType.FLOAT, label="Annual coupon rate", format_code=",|.|2||%"),
            trac.F("COUPON_PAYMENTS_PER_YEAR", trac.BasicType.FLOAT, label="Number of coupon payments per year", format_code="|.|0||"),
            trac.F("MATURITY_DATE", trac.BasicType.DATE, label="Maturity Date", format_code="DAY"),
            trac.F("CURRENT_PRICE", trac.BasicType.FLOAT, label="Current market pricee", format_code=",|.|2|$|"),
            trac.F("FACE_VALUE", trac.BasicType.FLOAT, label="Face value", format_code=",|.|2|$|"),
            trac.F("BOND_VALUATION", trac.BasicType.FLOAT, label="Bond valuation", format_code=",|.|2|$|")
        )
        
        total_valuation = trac.declare_output_table(
            trac.F("OBSERVATION_DATE", trac.BasicType.DATE, label="Date", business_key=True, categorical=True, format_code="MONTH"),
            trac.F("BOND_VALUATION", trac.BasicType.FLOAT, label="Total bond valuation", format_code=",|.|2|$|")
        )

        return {"bond_portfolio_valuation": bond_portfolio_valuation, "total_valuation": total_valuation}

    def run_model(self, ctx: trac.TracContext):

        maximum_number_of_months = ctx.get_parameter("maximum_number_of_months")
        include_zero_coupon_bonds = ctx.get_parameter("include_zero_coupon_bonds")

        interest_rate_scenario = ctx.get_pandas_table("interest_rate_scenario")
        bond_portfolio = ctx.get_pandas_table("bond_portfolio")

        bond_portfolio_valuation = bond_portfolio.copy()
   
        # $ amount received each coupon payment
        bond_portfolio_valuation["PAYMENT_PER_PERIOD"] = bond_portfolio_valuation["FACE_VALUE"] * bond_portfolio_valuation["COUPON_RATE"] / bond_portfolio_valuation["COUPON_PAYMENTS_PER_YEAR"]
        
        # Discount coupon payments by yield to maturity
        bond_portfolio_valuation["PRESENT_VALUE_OF_PAYMENTS"] = bond_portfolio_valuation["PAYMENT_PER_PERIOD"] /(1.015)
        
        # Discount face value by yield to maturity
        bond_portfolio_valuation["PRESENT_VALUE_OF_FACE_VALUE"] = bond_portfolio_valuation["FACE_VALUE"] /(1.015)

        # Sum both discounted values as full value
        bond_portfolio_valuation["BOND_VALUATION"] = bond_portfolio_valuation["PRESENT_VALUE_OF_PAYMENTS"] + bond_portfolio_valuation["PRESENT_VALUE_OF_FACE_VALUE"]
        
        # Calculate the total valuation
        total_valuation = bond_portfolio_valuation.groupby(['OBSERVATION_DATE'])['BOND_VALUATION'].sum().reset_index()
        
        # Output the two datasets
        ctx.put_pandas_table("bond_portfolio_valuation", bond_portfolio_valuation)
        ctx.put_pandas_table("total_valuation", total_valuation)


if __name__ == "__main__":
    import trac.rt.launch as launch
    launch.launch_model(BondPricingModel, "bond_pricing.yaml", "../sys_config.yaml")
