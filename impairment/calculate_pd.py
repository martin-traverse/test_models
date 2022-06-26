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

class CalculatePd(trac.TracModel):

    def define_parameters(self) -> tp.Dict[str, trac.ModelParameter]:

        return trac.declare_parameters(

            trac.P("first_forecast_day", trac.BasicType.DATE, label="First month for PD outcome"),
            trac.P("last_forecast_day", trac.BasicType.DATE, label="Last month for PD outcome"),
            trac.P("days_of_history", trac.BasicType.INTEGER, label="Days of performance history used for PD"),
            trac.P("advanced_logging", trac.BasicType.BOOLEAN, label="Advanced logging")
        )

    def define_inputs(self) -> tp.Dict[str, trac.ModelInputSchema]:

        economic_scenario = trac.declare_input_table(
            trac.F("OBSERVATION_DATE", trac.BasicType.DATE, label="Date", business_key=True, categorical=False, format_code="MONTH"),
            trac.F("UNEMPLOYMENT_RATE", trac.BasicType.FLOAT, label="Unemployment rate", format_code=",|.|2||%"),
            trac.F("BOE_BASE_RATE", trac.BasicType.FLOAT, label="Base rate (Bank of England)", format_code=",|.|2||%")
        )
        
        account_data = trac.declare_input_table(
            trac.F("OBSERVATION_DATE", trac.BasicType.DATE, label="Date", business_key=True, categorical=False, format_code="MONTH"),
            trac.F("BALANCE", trac.BasicType.FLOAT, label="Balance (drawn)", format_code=",|.|2|£|")
        )

        return {"economic_scenario": economic_scenario, "account_data": account_data}

    def define_outputs(self) -> tp.Dict[str, trac.ModelOutputSchema]:

        account_pd = trac.declare_output_table(
            trac.F("OBSERVATION_DATE", trac.BasicType.DATE, label="Date", business_key=True, categorical=False, format_code="MONTH"),
            trac.F("BALANCE", trac.BasicType.FLOAT, label="Balance (drawn)", format_code=",|.|2|£|"),
            trac.F("PD", trac.BasicType.FLOAT, label="Probability of default", format_code="|.|4||%")
        )

        return {"account_pd": account_pd}

    def run_model(self, ctx: trac.TracContext):
      
        economic_scenario = ctx.get_pandas_table("economic_scenario")
        account_data = ctx.get_pandas_table("account_data")
        
        account_pd = account_data.copy()
        
        account_pd["PD"] = 0.05
        
        # Output the dataset
        ctx.put_pandas_table("account_pd", account_pd)
      
if __name__ == "__main__":
    import trac.rt.launch as launch
    launch.launch_model(CalculatePd, "calculate_pd.yaml", "../sys_config.yaml")
