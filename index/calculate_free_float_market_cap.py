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

# Set display options 2
pd.set_option("display.max.columns", None)

class CalculateFreeFloatMarketCap(trac.TracModel):

    def define_parameters(self) -> tp.Dict[str, trac.ModelParameter]:

        return trac.declare_parameters(
           trac.P("index_calculation_date", trac.BasicType.DATE, label="Date of index calculation"),
           trac.P("days_of_history", trac.BasicType.INTEGER, label="Days of history"),
           trac.P("advanced_logging", trac.BasicType.BOOLEAN, label="Advanced logging")
        )

    def define_inputs(self) -> tp.Dict[str, trac.ModelInputSchema]:

        company_data = trac.declare_input_table(
            trac.F("OBSERVATION_DATE", trac.BasicType.DATE, label="Date", format_code="MONTH"),
            trac.F("COMPANY_TICKER", trac.BasicType.STRING, label="Company ticker"),
            trac.F("COMPANY_NAME", trac.BasicType.STRING, label="Company name"),
            trac.F("SHARE_PRICE_AT_CLOSE", trac.BasicType.FLOAT, label="Share price at close", format_code="|.|4||"),
            trac.F("SHARE_PRICE_CURRENCY", trac.BasicType.STRING, label="Share price currency"),
            trac.F("FREE_FLOAT", trac.BasicType.INTEGER, label="Free float")
        )

        return {"company_data": company_data}

    def define_outputs(self) -> tp.Dict[str, trac.ModelOutputSchema]:

        free_float_market_cap = trac.declare_output_table(
            trac.F("OBSERVATION_DATE", trac.BasicType.DATE, label="Date", format_code="MONTH"),
            trac.F("FREE_FLOAT_MARKET_CAP", trac.BasicType.INTEGER, label="Index free float market cap")
        )

        return {"free_float_market_cap": free_float_market_cap}

    def run_model(self, ctx: trac.TracContext):
      
        company_data = ctx.get_pandas_table("company_data")
        
        company_data["TOTAL_FLOAT"] = company_data["SHARE_PRICE_AT_CLOSE"] *  company_data["FREE_FLOAT"]
      
        free_float_market_cap = company_data.groupby(['OBSERVATION_DATE'])['TOTAL_FLOAT'].agg('sum')
        
        # Output the dataset
        ctx.put_pandas_table("free_float_market_cap", free_float_market_cap)
      
if __name__ == "__main__":
    import trac.rt.launch as launch
    launch.launch_model(CalculateFreeFloatMarketCap, "calculate_free_float_market_cap.yaml", "../sys_config.yaml")
