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
import pandas as pd
import numpy as np
import datetime

# Set display options
pd.set_option("display.max.columns", None)

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
   
        # Convert dates
        bond_portfolio["MATURITY_DATE"]= pd.to_datetime(bond_portfolio["MATURITY_DATE"], errors='coerce', format = '%Y-%m-%d')
        bond_portfolio["OBSERVATION_DATE"]= pd.to_datetime(bond_portfolio["OBSERVATION_DATE"], errors='coerce', format = '%Y-%m-%d')
        
        # Calculate the number of payments remaining, ceiling set by user is applied
        bond_portfolio['MONTHS_TO_MATURITY'] = ((bond_portfolio.MATURITY_DATE - bond_portfolio.OBSERVATION_DATE)/np.timedelta64(1, 'M')).astype(int)
        bond_portfolio['MONTHS_TO_MATURITY'] = np.minimum(maximum_number_of_months, bond_portfolio['MONTHS_TO_MATURITY'])
        bond_portfolio['NUMBER_OF_PAYMENTS_LEFT'] = (np.floor(bond_portfolio['MONTHS_TO_MATURITY']/(12/bond_portfolio_valuation["COUPON_PAYMENTS_PER_YEAR"]))).astype(int)

        # $ amount received each coupon payment
        bond_portfolio_valuation["PAYMENT_PER_PERIOD"] = bond_portfolio_valuation["FACE_VALUE"] * bond_portfolio_valuation["COUPON_RATE"] / (100 * bond_portfolio_valuation["COUPON_PAYMENTS_PER_YEAR"])
        
        maximum_payments_left_across_whole_portfolio = bond_portfolio['NUMBER_OF_PAYMENTS_LEFT'].max()
        
        # The DCF to calculate for each payment
        bond_portfolio_valuation["PRESENT_VALUE_OF_PAYMENTS"] = 0
        bond_portfolio_valuation["PRESENT_VALUE_OF_FACE_VALUE"] = 0
        
        # Sum the discounted cash flow
        #for i in range(maximum_payments_left_across_whole_portfolio):
            
        # Discount coupon payments by yield to maturity
        bond_portfolio_valuation["PRESENT_VALUE_OF_PAYMENTS"] = bond_portfolio_valuation["PRESENT_VALUE_OF_PAYMENTS"] + (bond_portfolio_valuation["PAYMENT_PER_PERIOD"] / pow(1.015, 0+1))
            
        # Discount face value by yield to maturity
        bond_portfolio_valuation["PRESENT_VALUE_OF_FACE_VALUE"] = bond_portfolio_valuation["PRESENT_VALUE_OF_FACE_VALUE"] + (bond_portfolio_valuation["FACE_VALUE"] / pow(1.015, 0+1))
        
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
