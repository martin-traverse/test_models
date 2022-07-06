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

class CalculateMarketWeights(trac.TracModel):

    def define_parameters(self) -> tp.Dict[str, trac.ModelParameter]:

        return trac.declare_parameters(
           trac.P("index_calculation_date", trac.BasicType.DATE, label="Date of index calculation"),
           trac.P("days_of_history", trac.BasicType.INTEGER, label="Days of history"),
           trac.P("advanced_logging", trac.BasicType.BOOLEAN, label="Advanced logging")
        )

    def define_inputs(self) -> tp.Dict[str, trac.ModelInputSchema]:

        free_float_market_cap = trac.declare_input_table(
            trac.F("OBSERVATION_DATE", trac.BasicType.DATE, label="Date", format_code="MONTH"),
            trac.F("FREE_FLOAT_MARKET_CAP", trac.BasicType.INTEGER, label="Index free float market cap")
        )

        return {"free_float_market_cap": free_float_market_cap}

    def define_outputs(self) -> tp.Dict[str, trac.ModelOutputSchema]:

        market_weights = trac.declare_output_table(
            trac.F("OBSERVATION_DATE", trac.BasicType.DATE, label="Date", format_code="MONTH"),
            trac.F("FREE_FLOAT_MARKET_CAP", trac.BasicType.INTEGER, label="Index free float market cap"),
						trac.F("WEIGHT", trac.BasicType.INTEGER, label="Index weight", format_code="|.|2||%")
        )

        return {"free_float_market_cap": free_float_market_cap}

    def run_model(self, ctx: trac.TracContext):
      
        market_weights = ctx.get_pandas_table("free_float_market_cap")
				
				market_weights['SUM_OF_FREE_FLOAT_MARKET_CAP'] = market_weights["FREE_FLOAT_MARKET_CAP"].sum(axis=0)
        
        market_weights["WEIGHT"] = 100) * market_weights["FREE_FLOAT_MARKET_CAP"] /  market_weights["SUM_OF_FREE_FLOAT_MARKET_CAP"]
         
        # Output the dataset
        ctx.put_pandas_table("market_weights", market_weights)
      
if __name__ == "__main__":
    import trac.rt.launch as launch
    launch.launch_model(CalculateMarketWeights, "calculate_market_weights.yaml", "../sys_config.yaml")
